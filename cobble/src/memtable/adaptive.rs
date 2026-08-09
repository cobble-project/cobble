//! Adaptive memtable type controller.
//!
//! Monitors read/write/scan access patterns and adaptively switches the memtable type to match
//! the dominant workload. The controller is lock-free on the fast path and has no background
//! thread: every state operation increments a counter and returns an optional
//! [`SwitchDecision`] when a window boundary is crossed. The memtable manager applies and confirms
//! the decision after the originating operation releases its active-memtable lock.
//!
//! Statistics are approximate: the window boundary check uses `>=` rather than exact bitmask
//! equality so that batch operations crossing a boundary (e.g. `record_write(4097)`) are never
//! missed. A concurrent thread that adds ops between the boundary check and the counter reset
//! may have those ops discarded by the `swap(0)` - this is acceptable because the next window
//! will re-evaluate with fresh data.
//!
//! See [`AdaptiveMemtableController`] for the full decision logic.

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};

use log::info;

use crate::config::MemtableType;

/// Main evaluation window size (operations).
const WINDOW_SIZE: u64 = 4096;

/// Sensitive (fast rollback) window size, checked only on poison ops (scans on VEC/HASH).
const SENSITIVE_WINDOW_SIZE: u64 = 64;

/// Extra-sensitive window size, checked only for reads on VEC. VEC is the most performance-
/// fragile specialized type: any read activity should trigger a rollback as fast as possible.
const VEC_READ_SENSITIVE_WINDOW_SIZE: u64 = 16;

/// Point-read ratio at or above which (with zero scans) the controller switches to HASH.
/// 99%: 4051 point reads out of 4096 total ops in a window.
const READ_RATIO_THRESHOLD: f64 = 0.99;

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
/// match the dominant workload.
///
/// # Switching rules
///
/// The controller maintains a `current_type` (one of `Skiplist`, `Hash`, `Vec`) and evaluates
/// a window of operations when a boundary is crossed. The decision rules are evaluated in order:
///
/// 1. Pure writes (zero reads + zero scans) -> `Vec`, no flush.
/// 2. On `Vec` with any reads/scans -> `Skiplist`, flush (reads are poison on VEC).
/// 3. On `Hash` with any scans -> `Skiplist`, flush (scans are poison on HASH).
/// 4. Point-read ratio >= 99% and zero scans -> `Hash`, no flush.
/// 5. On `Hash` with no scans -> stay `Hash` (handles point reads + writes well).
/// 6. Otherwise (Skiplist, mixed) -> stay `Skiplist`.
///
/// VEC enters when a full window contains only writes (zero reads/scans). VEC exits on the first
/// window where any read or scan appears - the extra-sensitive 16-op window on VEC makes this
/// detection near-instant. HASH stays HASH as long as no scans appear, since HASH handles point
/// reads and writes well. A read-heavy window on VEC rolls back to SKIPLIST (with flush) rather
/// than "entering" HASH, because the flush is needed to migrate VEC's append-only data.
///
/// # Scans
///
/// Range scans are tracked in a separate `range_scans` counter used only as a boolean signal:
/// any scan (`rs > 0`) blocks HASH and VEC entry (neither supports efficient range scans). Scans
/// do not contribute to the point-read ratio denominator.
///
/// # Fast path vs slow path
///
/// - **SKIPLIST (fast path)**: all operations are not sensitive. Only the 4096-op window is
///   checked.
/// - **VEC (slow path for reads)**: point reads and range scans are poison. Poison ops
///   additionally check a smaller sensitive window for fast rollback.
/// - **HASH (slow path for scans)**: range scans are poison. Scans check the sensitive window.
///
/// Writes are never poison, so `record_write()` always uses the fast path (4096-op window only),
/// except on VEC where writes also check the 16-op window so evaluation fires frequently enough to
/// detect any read activity.
pub(crate) struct AdaptiveMemtableController {
    total_ops: AtomicU64,
    point_reads: AtomicU64,
    range_scans: AtomicU64,
    writes: AtomicU64,
    current_type: AtomicU8,
    /// Whether adaptive evaluation is active. When `false`, all `record_*` calls are no-ops and
    /// `confirm_switch` rejects everything. Toggled by `switch_memtable_type`: enabling on
    /// `Adaptive`, disabling on a concrete type.
    enabled: AtomicBool,
    /// Serializes mode transitions and evaluation so that a decision is always created from the
    /// same adaptive session that supplied its statistics.
    eval_lock: Mutex<()>,
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
            writes: AtomicU64::new(0),
            current_type: AtomicU8::new(type_to_u8(initial_type)),
            enabled: AtomicBool::new(enabled),
            eval_lock: Mutex::new(()),
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

    /// Enables adaptive evaluation, resetting counters and bumping the epoch so any in-flight
    /// decision from a previous (disabled) session is invalidated. Called when
    /// `switch_memtable_type(Adaptive)` is invoked. Generation is **not** reset - it remains
    /// monotonically increasing across epochs, so the epoch check alone prevents ABA.
    pub(crate) fn enable(&self) {
        // Keep the mode transition in the same critical section as decision creation. In
        // particular, an old evaluation must not drain its window, then acquire this new epoch
        // while publishing its decision.
        let _guard = self.eval_lock.lock().unwrap();
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
    /// cleared.
    pub(crate) fn disable(&self, pinned_type: MemtableType) {
        // See `enable`: mode transitions and evaluation share this lock so a decision's epoch
        // always describes the statistics used to make it.
        let _guard = self.eval_lock.lock().unwrap();
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

    /// Clears accumulated statistics (not generations/epoch). Used by `enable` and `disable` so
    /// transitions don't carry over stale window data.
    fn reset_counters(&self) {
        self.total_ops.store(0, Ordering::Relaxed);
        self.point_reads.store(0, Ordering::Relaxed);
        self.range_scans.store(0, Ordering::Relaxed);
        self.writes.store(0, Ordering::Relaxed);
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
    /// generate a new decision.
    pub(crate) fn confirm_switch(&self, decision: &SwitchDecision) {
        if !self.validate_decision(decision) {
            return;
        }
        self.current_type
            .store(type_to_u8(decision.target), Ordering::Relaxed);
        self.pending_generation.store(0, Ordering::Relaxed);
    }

    /// Cancels a pending decision without updating the type. Called when the physical switch
    /// fails or a decision is rejected. Clears the pending slot so the next window can generate
    /// a fresh decision and retry.
    pub(crate) fn cancel_decision(&self, decision: &SwitchDecision) {
        // Only clear if this is still the pending decision (it may have been superseded by a
        // mode toggle, in which case pending is already 0 or belongs to a new epoch).
        let pending = self.pending_generation.load(Ordering::Relaxed);
        if pending == decision.generation && decision.epoch == self.epoch.load(Ordering::Relaxed) {
            self.pending_generation.store(0, Ordering::Relaxed);
        }
    }

    /// Records `count` writes. Returns a [`SwitchDecision`] if a window boundary was crossed and
    /// the controller decided to switch types; the caller performs the switch and calls
    /// [`confirm_switch`](Self::confirm_switch) on success.
    pub(crate) fn record_write(&self, count: u64) -> Option<SwitchDecision> {
        if count == 0 || !self.enabled.load(Ordering::Relaxed) {
            return None;
        }
        self.writes.fetch_add(count, Ordering::Relaxed);
        let n = self.total_ops.fetch_add(count, Ordering::Relaxed) + count;
        // Use >= so batch ops crossing a boundary are never missed. On VEC, also check the
        // 16-op window so reads are detected quickly.
        let main_window = n >= WINDOW_SIZE;
        let sensitive =
            self.current_type() == MemtableType::Vec && n >= VEC_READ_SENSITIVE_WINDOW_SIZE;
        if main_window || sensitive {
            return self.evaluate(n);
        }
        None
    }

    /// Records `count` point reads. Returns a [`SwitchDecision`] if evaluation fires.
    pub(crate) fn record_point_read(&self, count: u64) -> Option<SwitchDecision> {
        if count == 0 || !self.enabled.load(Ordering::Relaxed) {
            return None;
        }
        self.point_reads.fetch_add(count, Ordering::Relaxed);
        let n = self.total_ops.fetch_add(count, Ordering::Relaxed) + count;
        let main_window = n >= WINDOW_SIZE;
        let sensitive =
            self.current_type() == MemtableType::Vec && n >= VEC_READ_SENSITIVE_WINDOW_SIZE;
        if main_window || sensitive {
            return self.evaluate(n);
        }
        None
    }

    /// Records a range scan. Returns a [`SwitchDecision`] if evaluation fires.
    ///
    /// Scans are tracked in a separate counter used only as a boolean signal (`rs > 0` blocks
    /// HASH/VEC entry). They do not contribute to the point-read counter or the ratio denominator.
    pub(crate) fn record_range_scan(&self) -> Option<SwitchDecision> {
        if !self.enabled.load(Ordering::Relaxed) {
            return None;
        }
        self.range_scans.fetch_add(1, Ordering::Relaxed);
        let n = self.total_ops.fetch_add(1, Ordering::Relaxed) + 1;
        let main_window = n >= WINDOW_SIZE;
        let sensitive = self.current_type() != MemtableType::Skiplist && n >= SENSITIVE_WINDOW_SIZE;
        if main_window || sensitive {
            return self.evaluate(n);
        }
        None
    }

    fn evaluate(&self, observed_total: u64) -> Option<SwitchDecision> {
        // Guard against concurrent evaluation: a failed try_lock means another thread is already
        // evaluating. That thread will reset the counters, so skipping is safe.
        let Ok(_guard) = self.eval_lock.try_lock() else {
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

        // Re-check the threshold after acquiring the lock: another thread may have already
        // evaluated and reset the counters. If the current total is below the threshold, skip.
        let current_total = self.total_ops.load(Ordering::Relaxed);
        if current_total < observed_total.min(WINDOW_SIZE) && current_total < WINDOW_SIZE {
            // Counters were reset by a concurrent evaluation; nothing to do.
            return None;
        }

        // Use swap(0) to atomically read-and-reset each counter. total_ops is also swapped (not
        // store(0)) to avoid discarding ops written between the load above and the reset.
        let pr = self.point_reads.swap(0, Ordering::Relaxed);
        let rs = self.range_scans.swap(0, Ordering::Relaxed);
        let wr = self.writes.swap(0, Ordering::Relaxed);
        let _total = self.total_ops.swap(0, Ordering::Relaxed);
        // Note: `total` may differ from pr+rs+wr due to concurrent writes between individual
        // swap calls. This is acceptable - the ratios are approximate by design.
        let sum = pr + rs + wr;
        if sum == 0 {
            return None;
        }

        let prev = self.current_type();
        let raw_decision = decide(prev, pr, rs, sum);

        if raw_decision.target == prev {
            return None;
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
             window: pointReads={}, rangeScans={}, writes={}, total={})",
            prev, decision.target, decision.flush_current, generation, epoch, pr, rs, wr, sum
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

/// Pure decision function. Extracted for testability without atomic state.
///
/// Rules (evaluated in order):
/// 1. Pure writes (zero reads + zero scans) -> Vec, no flush.
/// 2. On Vec with any reads/scans -> Skiplist, flush.
/// 3. On Hash with any scans -> Skiplist, flush.
/// 4. Point-read ratio >= 99% and zero scans -> Hash, no flush.
/// 5. On Hash with no scans -> stay Hash (handles point reads + writes well).
/// 6. Otherwise (Skiplist, mixed) -> stay Skiplist.
fn decide(prev: MemtableType, pr: u64, rs: u64, sum: u64) -> RawDecision {
    debug_assert!(sum > 0);

    // Rule 1: pure writes -> Vec (non-disruptive).
    if pr == 0 && rs == 0 {
        return RawDecision {
            target: MemtableType::Vec,
            flush_current: false,
        };
    }

    // Rule 2: on Vec, any read or scan is poison -> rollback to Skiplist with flush.
    // Checked before HASH entry so that a read-heavy window on VEC rolls back to SKIPLIST
    // rather than "entering" HASH (which would skip the flush and leave VEC's data in place).
    if prev == MemtableType::Vec && (pr + rs) > 0 {
        return RawDecision {
            target: MemtableType::Skiplist,
            flush_current: true,
        };
    }

    // Rule 3: on Hash, any scan is poison -> rollback to Skiplist with flush.
    if prev == MemtableType::Hash && rs > 0 {
        return RawDecision {
            target: MemtableType::Skiplist,
            flush_current: true,
        };
    }

    // Rule 4: point-read-dominant with no scans -> Hash (non-disruptive).
    // range_scans excluded from denominator (sum = pr + rs + wr, but rs > 0 is already handled
    // by rules 2-3; reaching here means rs == 0, so sum = pr + wr).
    if rs == 0 && (pr as f64) / (sum as f64) >= READ_RATIO_THRESHOLD {
        return RawDecision {
            target: MemtableType::Hash,
            flush_current: false,
        };
    }

    // Rule 5: on Hash with no scans, HASH handles point reads + writes -> stay.
    // Rule 6: on Skiplist with mixed workload -> stay (Skiplist handles everything).
    RawDecision {
        target: prev,
        flush_current: false,
    }
}

#[cfg(test)]
#[path = "../../tests/unit/memtable/adaptive.rs"]
mod tests;
