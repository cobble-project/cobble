//! Adaptive memtable type controller.
//!
//! Monitors read/write/scan access patterns and adaptively switches the memtable type to match
//! the observed access patterns. The controller is lock-free on the fast path and has no background
//! thread: operations increment counters and return an optional
//! [`SwitchDecision`] when a fast fallback is detected. Normal evaluation runs at a non-empty
//! memtable rotation, selecting the replacement type before its allocation. Fast fallback decisions
//! are applied and confirmed after the originating operation releases its active-memtable lock.
//!
//! Statistics are approximate: counters are drained separately, so concurrent operations can land
//! in different windows for the total and read/scan counts. Threshold checks use `>=` so batches
//! crossing a threshold are not missed; each completed evaluation starts a fresh window.
//!
//! See [`AdaptiveMemtableController`] for the full decision logic.

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};

use log::info;

use crate::config::MemtableType;

/// Main evaluation window size (operations).
const WINDOW_SIZE: u64 = 4096;

/// Consecutive main windows needed to specialize again after a confirmed rollback.
const REENTRY_WINDOWS: u8 = 3;

/// Minimum sample size for fast HASH rollback.
const HASH_FALLBACK_MIN_OPS: u64 = 64;

/// Minimum sample size for fast VEC rollback on any point read or range scan.
const VEC_FALLBACK_MIN_OPS: u64 = 16;

/// Encodes a concrete [`MemtableType`] (never `Adaptive`) as a `u8` for atomic storage.
fn type_to_u8(t: MemtableType) -> u8 {
    match t {
        MemtableType::Hash => 0,
        MemtableType::Skiplist => 1,
        MemtableType::Vec => 2,
        // Adaptive is never stored as the current type; map it to Skiplist defensively.
        MemtableType::Adaptive => 1,
    }
}

fn type_from_u8(v: u8) -> MemtableType {
    match v {
        0 => MemtableType::Hash,
        2 => MemtableType::Vec,
        _ => MemtableType::Skiplist,
    }
}

/// A decision returned by the controller when a window boundary is crossed.
///
/// Each decision carries an `epoch` and a `generation`. The epoch identifies the adaptive mode
/// session in which the decision was created: every `enable`/`disable` (manual pin or re-enable)
/// increments the epoch, so a decision from a previous session is automatically invalidated.
/// The `generation` is monotonically increasing across epochs. Only the controller's single
/// pending generation is valid at a time, which prevents stale decisions from overriding newer
/// ones or from a previous adaptive session leaking into a new one (no ABA).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SwitchDecision {
    pub(crate) target: MemtableType,
    pub(crate) flush_current: bool,
    /// The adaptive epoch in which this decision was generated.
    epoch: u64,
    /// Monotonically increasing generation within the epoch. Only the most recent decision is valid.
    generation: u64,
}

/// Monitors read/write/scan access patterns and adaptively switches the native memtable type to
/// match the observed access patterns.
///
/// # Switching rules
///
/// Normal evaluation runs only at a non-empty memtable rotation after at least 4096 operations.
/// Operations keep accumulating beyond 4096 until then. Pure writes select `Vec`; otherwise
/// at least 99% point reads among point reads plus range scans select `Hash`.
/// Writes do not dilute the scan ratio.
/// Other mixtures select `Skiplist`.
///
/// `Vec` falls back to `Skiplist` with a flush on any read/scan after 16 total operations.
/// `Hash` falls back with a flush below the 99% point-read ratio after 64 total operations.
/// Every operation can detect these fallbacks, including writes following a scan. Probes that
/// do not fall back leave the main window intact; completed normal evaluations drain it.
///
/// A confirmed fallback restarts the window after its forced rotation and enables a reentry
/// bias: specialization then requires three consecutive completed normal windows selecting the
/// same type. Unsuitable windows clear the run; a different candidate starts a new run. This bias
/// lasts until adaptive mode is toggled; initial specialization still needs only one full window.
///
/// # Fast path
///
/// Recording uses atomics only unless an actual fast-fallback signal is detected. The manager
/// evaluates full windows while rotating a non-empty table, before creating the next active table.
pub(crate) struct AdaptiveMemtableController {
    total_ops: AtomicU64,
    point_reads: AtomicU64,
    range_scans: AtomicU64,
    current_type: AtomicU8,
    /// Whether adaptive evaluation is active. When `false`, all `record_*` calls are no-ops and
    /// `confirm_switch` rejects everything. Toggled by `switch_memtable_type`: enabling on
    /// `Adaptive`, disabling on a concrete type.
    enabled: AtomicBool,
    /// Serializes mode transitions, evaluation and decision resolution, and protects the
    /// post-rollback reentry state. A decision belongs to the session supplying its statistics.
    eval_lock: Mutex<EvaluationState>,
    /// Monotonically increasing **epoch** - incremented on every mode transition (enable/disable).
    /// Never reset. A `SwitchDecision` carries the epoch it was created in; `validate_decision`
    /// rejects decisions from a stale epoch. This prevents ABA when adaptive mode is toggled.
    epoch: AtomicU64,
    /// Monotonically increasing generation for switch decisions. Never reset.
    decision_generation: AtomicU64,
    /// The generation of the currently in-flight (pending) decision, or 0 if none. Only one
    /// decision may be in-flight at a time: `evaluate` refuses to generate a new decision while a
    /// pending one exists. This prevents generation gaps that would permanently stall switching.
    /// The pending decision is cleared by `confirm_switch` (success) or `cancel_decision`
    /// (failure/rejection), and by `enable`/`disable` (mode toggle).
    pending_generation: AtomicU64,
    /// Test-only rendezvous point after a window is drained but before its decision is published.
    /// This lets the regression test deterministically prove that a mode transition cannot slip
    /// into that interval.
    #[cfg(test)]
    evaluation_hook: Mutex<Option<EvaluationHook>>,
}

#[derive(Default)]
struct EvaluationState {
    after_fallback: bool,
    reentry_candidate: Option<(MemtableType, u8)>,
}

#[cfg(test)]
#[derive(Clone)]
struct EvaluationHook {
    reached: std::sync::Arc<std::sync::Barrier>,
    resume: std::sync::Arc<std::sync::Barrier>,
}

impl AdaptiveMemtableController {
    /// Creates a controller with internal default thresholds.
    ///
    /// `enabled` should be `true` when the DB is opened with `memtable_type = Adaptive`; the
    /// manager toggles it via [`enable`](Self::enable) / [`disable`](Self::disable) when
    /// `switch_memtable_type` is called at runtime.
    ///
    /// `initial_type` is the concrete memtable type the DB opens with (the resolved type). The
    /// controller tracks this so that if adaptive mode is disabled and later re-enabled, it
    /// resumes from the correct type rather than defaulting to `Skiplist`.
    pub(crate) fn new(enabled: bool, initial_type: MemtableType) -> Self {
        Self {
            total_ops: AtomicU64::new(0),
            point_reads: AtomicU64::new(0),
            range_scans: AtomicU64::new(0),
            current_type: AtomicU8::new(type_to_u8(initial_type)),
            enabled: AtomicBool::new(enabled),
            eval_lock: Mutex::new(EvaluationState::default()),
            epoch: AtomicU64::new(0),
            decision_generation: AtomicU64::new(0),
            pending_generation: AtomicU64::new(0),
            #[cfg(test)]
            evaluation_hook: Mutex::new(None),
        }
    }

    /// Returns the concrete type the controller last switched to or was initialized with.
    pub(crate) fn current_type(&self) -> MemtableType {
        type_from_u8(self.current_type.load(Ordering::Relaxed))
    }

    /// Returns whether adaptive evaluation is currently active.
    pub(crate) fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    /// Enables adaptive evaluation, resetting counters/reentry bias and bumping the epoch so
    /// any in-flight decision from a previous (disabled) session is invalidated. Called when
    /// `switch_memtable_type(Adaptive)` is invoked. Generation is **not** reset - it remains
    /// monotonically increasing across epochs, so the epoch check alone prevents ABA.
    pub(crate) fn enable(&self) {
        // Keep the mode transition in the same critical section as decision creation. In
        // particular, an old evaluation must not drain its window, then acquire this new epoch
        // while publishing its decision.
        let mut state = self.eval_lock.lock().unwrap();
        *state = EvaluationState::default();
        self.reset_counters();
        self.pending_generation.store(0, Ordering::Relaxed);
        self.epoch.fetch_add(1, Ordering::Relaxed);
        self.enabled.store(true, Ordering::Relaxed);
        info!(
            "Adaptive memtable controller enabled (epoch={}, starting from {:?})",
            self.epoch.load(Ordering::Relaxed),
            self.current_type()
        );
    }

    /// Disables adaptive evaluation. Future `record_*` calls become no-ops. Called when
    /// `switch_memtable_type(concrete)` pins a specific type. The `current_type` is updated to
    /// `pinned_type` so that if adaptive mode is re-enabled later, evaluation resumes from the
    /// pinned type rather than a stale value. The epoch is bumped and any in-flight decision is
    /// cleared, together with the reentry bias and candidate run.
    pub(crate) fn disable(&self, pinned_type: MemtableType) {
        // See `enable`: mode transitions and evaluation share this lock so a decision's epoch
        // always describes the statistics used to make it.
        let mut state = self.eval_lock.lock().unwrap();
        *state = EvaluationState::default();
        self.enabled.store(false, Ordering::Relaxed);
        self.current_type
            .store(type_to_u8(pinned_type), Ordering::Relaxed);
        self.reset_counters();
        self.pending_generation.store(0, Ordering::Relaxed);
        self.epoch.fetch_add(1, Ordering::Relaxed);
        info!(
            "Adaptive memtable controller disabled (pinned to {:?}, epoch={})",
            pinned_type,
            self.epoch.load(Ordering::Relaxed)
        );
    }

    /// Clears statistics on mode transitions and confirmed fallbacks, not generations/epoch.
    fn reset_counters(&self) {
        self.total_ops.store(0, Ordering::Relaxed);
        self.point_reads.store(0, Ordering::Relaxed);
        self.range_scans.store(0, Ordering::Relaxed);
    }

    /// Validates whether a decision is still applicable **before** any side effects. Returns
    /// `false` if the controller is disabled, the decision's epoch doesn't match the current
    /// epoch (mode was toggled since the decision was generated), or the decision is not the
    /// currently pending one.
    ///
    /// This is called by the manager inside the transition lock **before** performing the
    /// physical switch, so a stale decision never mutates the memtable target.
    pub(crate) fn validate_decision(&self, decision: &SwitchDecision) -> bool {
        if !self.enabled.load(Ordering::Relaxed) {
            return false;
        }
        if decision.epoch != self.epoch.load(Ordering::Relaxed) {
            log::debug!(
                "Discarding adaptive decision from stale epoch (decision={}, current={})",
                decision.epoch,
                self.epoch.load(Ordering::Relaxed)
            );
            return false;
        }
        let pending = self.pending_generation.load(Ordering::Relaxed);
        if decision.generation != pending {
            log::debug!(
                "Discarding adaptive decision (gen={}, pending={}): not the in-flight decision",
                decision.generation,
                pending
            );
            return false;
        }
        true
    }

    /// Called by the caller after successfully performing the switch described by `decision`.
    /// Updates the controller's tracked type and clears the pending slot so the next window can
    /// generate a new decision. A confirmed rollback enables the three-window reentry bias.
    pub(crate) fn confirm_switch(&self, decision: &SwitchDecision) {
        let mut state = self.eval_lock.lock().unwrap();
        if !self.validate_decision(decision) {
            return;
        }
        if matches!(self.current_type(), MemtableType::Hash | MemtableType::Vec)
            && decision.target == MemtableType::Skiplist
        {
            state.after_fallback = true;
            // Restart at confirmation so the fallback's forced rotation and operations
            // collected while it was pending cannot satisfy the new full window.
            self.reset_counters();
        }
        state.reentry_candidate = None;
        self.current_type
            .store(type_to_u8(decision.target), Ordering::Relaxed);
        self.pending_generation.store(0, Ordering::Relaxed);
    }

    /// Cancels a pending decision without updating the type. Called when the physical switch
    /// fails or a decision is rejected. Clears the pending slot so the next window can generate
    /// a fresh decision and retry. Cancelling reentry also clears its candidate run, not its bias.
    pub(crate) fn cancel_decision(&self, decision: &SwitchDecision) {
        let mut state = self.eval_lock.lock().unwrap();
        // Only clear if this is still the pending decision (it may have been superseded by a
        // mode toggle, in which case pending is already 0 or belongs to a new epoch).
        let pending = self.pending_generation.load(Ordering::Relaxed);
        if pending == decision.generation && decision.epoch == self.epoch.load(Ordering::Relaxed) {
            self.pending_generation.store(0, Ordering::Relaxed);
            state.reentry_candidate = None;
        }
    }

    /// Records `count` writes. Returns a [`SwitchDecision`] only for a fast fallback;
    /// the caller performs the switch and calls
    /// [`confirm_switch`](Self::confirm_switch) on success.
    pub(crate) fn record_write(&self, count: u64) -> Option<SwitchDecision> {
        if count == 0 || !self.enabled.load(Ordering::Relaxed) {
            return None;
        }
        let n = self.total_ops.fetch_add(count, Ordering::Relaxed) + count;
        self.maybe_fallback(n)
    }

    /// Records `count` point reads. Returns a [`SwitchDecision`] if evaluation fires.
    pub(crate) fn record_point_read(&self, count: u64) -> Option<SwitchDecision> {
        if count == 0 || !self.enabled.load(Ordering::Relaxed) {
            return None;
        }
        self.point_reads.fetch_add(count, Ordering::Relaxed);
        let n = self.total_ops.fetch_add(count, Ordering::Relaxed) + count;
        self.maybe_fallback(n)
    }

    /// Records a range scan. Returns a [`SwitchDecision`] if evaluation fires.
    ///
    /// Scans are tracked separately from point reads. Writes never dilute their ratio.
    pub(crate) fn record_range_scan(&self) -> Option<SwitchDecision> {
        if !self.enabled.load(Ordering::Relaxed) {
            return None;
        }
        self.range_scans.fetch_add(1, Ordering::Relaxed);
        let n = self.total_ops.fetch_add(1, Ordering::Relaxed) + 1;
        self.maybe_fallback(n)
    }

    fn maybe_fallback(&self, n: u64) -> Option<SwitchDecision> {
        let prev = self.current_type();
        // Avoid acquiring eval_lock for every specialized operation past 16/64 when there is
        // no actual fallback signal. Every operation checks so a scan followed by writes can
        // still trigger HASH rollback at 64 total operations.
        if self.fast_fallback(prev, n) {
            return self.evaluate(false);
        }
        None
    }

    fn fast_fallback(&self, prev: MemtableType, n: u64) -> bool {
        match prev {
            MemtableType::Vec if n >= VEC_FALLBACK_MIN_OPS => {
                self.point_reads.load(Ordering::Relaxed) > 0
                    || self.range_scans.load(Ordering::Relaxed) > 0
            }
            MemtableType::Hash if n >= HASH_FALLBACK_MIN_OPS => {
                let scans = self.range_scans.load(Ordering::Relaxed);
                scans != 0 && !hash_read_ratio(self.point_reads.load(Ordering::Relaxed), scans)
            }
            _ => false,
        }
    }

    /// Evaluates a full normal window at a non-empty rotation. The manager applies and confirms
    /// any returned decision before creating the replacement table; no additional flush is needed.
    pub(crate) fn evaluate_on_rotation(&self) -> Option<SwitchDecision> {
        if self.total_ops.load(Ordering::Relaxed) < WINDOW_SIZE {
            return None;
        }
        self.evaluate(true)
    }

    fn evaluate(&self, normal_rotation: bool) -> Option<SwitchDecision> {
        // Another evaluator or mode transition owns the lock; leave statistics for a later call.
        let Ok(mut state) = self.eval_lock.try_lock() else {
            return None;
        };

        // The fast path checks `enabled` before it starts counting, but a concurrent manual
        // switch may have disabled adaptive mode while that operation was in flight. Re-check
        // under the shared transition/evaluation lock and capture the epoch before draining the
        // window. `enable` and `disable` cannot run until this decision is fully published.
        if !self.enabled.load(Ordering::Relaxed) {
            return None;
        }
        let epoch = self.epoch.load(Ordering::Relaxed);

        // If a decision is already pending (in-flight), do not generate a new one. This prevents
        // generation gaps: the pending decision will be confirmed or cancelled, after which the
        // next window can evaluate fresh. Counters are NOT reset here, so the pending window's
        // data carries forward until the next evaluation.
        if self.pending_generation.load(Ordering::Relaxed) != 0 {
            return None;
        }

        // Re-check after acquiring the lock: another evaluation may have drained the counters.
        let current_total = self.total_ops.load(Ordering::Relaxed);
        let prev = self.current_type();
        let normal = normal_rotation && current_total >= WINDOW_SIZE;
        let fallback = self.fast_fallback(prev, current_total);
        // Fast probes never drain a partial main window unless they actually roll back.
        if !normal && !fallback {
            return None;
        }

        // Use swap(0) to atomically read-and-reset each counter. total_ops is also swapped (not
        // store(0)) to avoid discarding ops written between the load above and the reset.
        let pr = self.point_reads.swap(0, Ordering::Relaxed);
        let rs = self.range_scans.swap(0, Ordering::Relaxed);
        let total = self.total_ops.swap(0, Ordering::Relaxed);
        // Concurrent operations may straddle these separate drains; statistics are approximate.

        // Once a fast fallback is detected, keep that decision even if concurrent point reads
        // change the ratio while draining. An early probe must never drain then specialize.
        let raw_decision = if fallback {
            RawDecision {
                target: MemtableType::Skiplist,
                flush_current: true,
            }
        } else {
            decide(prev, pr, rs)
        };

        if raw_decision.target == prev {
            state.reentry_candidate = None;
            return None;
        }
        if prev == MemtableType::Skiplist && state.after_fallback {
            let windows = match state.reentry_candidate {
                Some((target, windows)) if target == raw_decision.target => windows + 1,
                _ => 1,
            };
            state.reentry_candidate = Some((raw_decision.target, windows));
            if windows < REENTRY_WINDOWS {
                return None;
            }
        } else {
            state.reentry_candidate = None;
        }

        #[cfg(test)]
        self.run_evaluation_hook();

        // Assign a new generation and mark it as pending (in-flight). Only one decision may be
        // pending at a time: the eval_lock ensures only one thread reaches here concurrently, and
        // the pending check above blocks new decisions until the current one is resolved.
        let generation = self.decision_generation.fetch_add(1, Ordering::Relaxed) + 1;
        self.pending_generation.store(generation, Ordering::Relaxed);
        let decision = SwitchDecision {
            target: raw_decision.target,
            flush_current: raw_decision.flush_current,
            epoch,
            generation,
        };

        info!(
            "Adaptive memtable switch: {:?} -> {:?} (flush_current={}, gen={}, epoch={}, \
             window: pointReads={}, rangeScans={}, total={})",
            prev, decision.target, decision.flush_current, generation, epoch, pr, rs, total
        );
        // Do NOT update current_type here - the caller must confirm after performing the switch.
        Some(decision)
    }

    #[cfg(test)]
    fn set_evaluation_hook(&self, hook: Option<EvaluationHook>) {
        *self.evaluation_hook.lock().unwrap() = hook;
    }

    #[cfg(test)]
    fn run_evaluation_hook(&self) {
        let hook = self.evaluation_hook.lock().unwrap().clone();
        if let Some(hook) = hook {
            hook.reached.wait();
            hook.resume.wait();
        }
    }
}

/// Internal decision without generation, used by the pure `decide` function.
struct RawDecision {
    target: MemtableType,
    flush_current: bool,
}

/// Pure decision function for a non-empty window, without atomic state.
fn decide(prev: MemtableType, pr: u64, rs: u64) -> RawDecision {
    // Rule 1: pure writes -> Vec (non-disruptive).
    if pr == 0 && rs == 0 {
        return RawDecision {
            target: MemtableType::Vec,
            flush_current: false,
        };
    }

    // Rule 2: on Vec, any read or scan is poison -> rollback to Skiplist with flush.
    // Checked before HASH entry so that any reads on VEC roll back to SKIPLIST
    // rather than "entering" HASH (which would skip the flush and leave VEC's data in place).
    if prev == MemtableType::Vec {
        return RawDecision {
            target: MemtableType::Skiplist,
            flush_current: true,
        };
    }

    // Rule 3: below 99% point reads requires Skiplist; leaving Hash requires a flush.
    if !hash_read_ratio(pr, rs) {
        return RawDecision {
            target: MemtableType::Skiplist,
            flush_current: prev == MemtableType::Hash,
        };
    }

    // Rule 4: at least 99% point reads among reads/scans -> Hash, regardless of write count.
    RawDecision {
        target: MemtableType::Hash,
        flush_current: false,
    }
}

/// At least 99% point reads among reads/scans; u128 keeps the comparison exact and overflow-free.
fn hash_read_ratio(pr: u64, rs: u64) -> bool {
    (pr as u128) >= 99 * (rs as u128)
}

#[cfg(test)]
#[path = "../../tests/unit/memtable/adaptive.rs"]
mod tests;
