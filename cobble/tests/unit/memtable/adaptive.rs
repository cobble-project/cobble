use super::*;
use std::sync::{Arc, Barrier};
use std::thread;

// Helpers supply the non-empty rotation required by a normal window. Gate behavior itself
// is tested separately using record_write/record_point_read directly.
impl AdaptiveMemtableController {
    fn record_write_then_rotate(&self, count: u64) -> Option<SwitchDecision> {
        self.record_write(count)
            .or_else(|| self.evaluate_on_rotation())
    }

    fn record_point_read_then_rotate(&self, count: u64) -> Option<SwitchDecision> {
        self.record_point_read(count)
            .or_else(|| self.evaluate_on_rotation())
    }
}

fn controller() -> AdaptiveMemtableController {
    AdaptiveMemtableController::new(true, MemtableType::Skiplist)
}

/// Helper: record writes and confirm any resulting switch (auto-confirms in tests).
fn record_write_and_confirm(c: &AdaptiveMemtableController, count: u64) {
    if let Some(d) = c.record_write_then_rotate(count) {
        c.confirm_switch(&d);
    }
}

fn record_point_read_and_confirm(c: &AdaptiveMemtableController, count: u64) {
    if let Some(d) = c.record_point_read_then_rotate(count) {
        c.confirm_switch(&d);
    }
}

fn record_range_scan_and_confirm(c: &AdaptiveMemtableController) {
    if let Some(d) = c.record_range_scan().or_else(|| c.evaluate_on_rotation()) {
        c.confirm_switch(&d);
    }
}

fn controller_after_fallback(specialized: MemtableType) -> AdaptiveMemtableController {
    let c = controller();
    let decision = record_specialization_window(&c, specialized).unwrap();
    c.confirm_switch(&decision);
    let decision = record_fallback_window(&c);
    c.confirm_switch(&decision);
    assert_eq!(c.current_type(), MemtableType::Skiplist);
    c
}

fn record_fallback_window(c: &AdaptiveMemtableController) -> SwitchDecision {
    match c.current_type() {
        MemtableType::Hash => {
            for _ in 0..HASH_FALLBACK_MIN_OPS - 1 {
                assert!(c.record_range_scan().is_none());
            }
            c.record_range_scan().unwrap()
        }
        MemtableType::Vec => c.record_point_read(VEC_FALLBACK_MIN_OPS).unwrap(),
        _ => unreachable!(),
    }
}

fn record_specialization_window(
    c: &AdaptiveMemtableController,
    target: MemtableType,
) -> Option<SwitchDecision> {
    match target {
        MemtableType::Hash => c.record_point_read_then_rotate(WINDOW_SIZE),
        MemtableType::Vec => c.record_write_then_rotate(WINDOW_SIZE),
        _ => unreachable!(),
    }
}

#[test]
fn test_initial_type_is_skiplist() {
    let c = controller();
    assert_eq!(c.current_type(), MemtableType::Skiplist);
}

#[test]
fn test_normal_window_evaluates_only_at_rotation_after_threshold() {
    for flush_before_threshold in [false, true] {
        let c = controller();
        assert!(c.record_write(WINDOW_SIZE - 1).is_none());
        if flush_before_threshold {
            assert!(c.evaluate_on_rotation().is_none());
            assert_eq!(c.total_ops.load(Ordering::Relaxed), WINDOW_SIZE - 1);
        }
        assert!(c.record_write(1).is_none());
        assert!(c.record_write(WINDOW_SIZE).is_none());
        assert_eq!(c.total_ops.load(Ordering::Relaxed), WINDOW_SIZE * 2);
        assert_eq!(c.current_type(), MemtableType::Skiplist);
        let decision = c.evaluate_on_rotation().unwrap();
        assert_eq!(decision.target, MemtableType::Vec);
        c.confirm_switch(&decision);
        assert_eq!(c.total_ops.load(Ordering::Relaxed), 0);
    }
}

#[test]
fn test_fast_probes_preserve_full_window_and_hash_ratio_excludes_writes() {
    let c = AdaptiveMemtableController::new(true, MemtableType::Hash);
    assert!(c.record_point_read(99).is_none());
    assert!(c.record_range_scan().is_none()); // Exactly 99%: stay Hash.
    assert!(c.record_write(WINDOW_SIZE * 2).is_none()); // No rotation yet.
    assert_eq!(c.point_reads.load(Ordering::Relaxed), 99);
    assert_eq!(c.range_scans.load(Ordering::Relaxed), 1);
    assert_eq!(c.total_ops.load(Ordering::Relaxed), WINDOW_SIZE * 2 + 100);
    let fallback = c.record_range_scan().unwrap();
    assert_eq!(fallback.target, MemtableType::Skiplist);
    assert!(fallback.flush_current);

    let c = AdaptiveMemtableController::new(true, MemtableType::Hash);
    assert!(c.record_range_scan().is_none());
    assert!(c.record_range_scan().is_none());
    assert!(c.record_point_read(98).unwrap().flush_current); // 98%: rollback.

    let c = AdaptiveMemtableController::new(true, MemtableType::Hash);
    assert!(c.record_range_scan().is_none());
    assert!(c.record_write(HASH_FALLBACK_MIN_OPS - 2).is_none());
    assert!(c.record_write(1).unwrap().flush_current); // Writes cannot dilute a scan.

    let c = AdaptiveMemtableController::new(true, MemtableType::Vec);
    assert!(c.record_write(WINDOW_SIZE).is_none());
    assert_eq!(c.total_ops.load(Ordering::Relaxed), WINDOW_SIZE);
    assert!(c.record_point_read(1).unwrap().flush_current);
}

#[test]
fn test_confirmed_fallback_starts_fresh_window_after_its_forced_rotation() {
    for previous in [MemtableType::Hash, MemtableType::Vec] {
        let c = AdaptiveMemtableController::new(true, previous);
        let fallback = record_fallback_window(&c);
        // The manager's forced rotation and concurrent operations happen before confirmation.
        assert!(c.evaluate_on_rotation().is_none());
        assert!(c.record_write(WINDOW_SIZE).is_none());
        c.confirm_switch(&fallback);
        assert_eq!(c.total_ops.load(Ordering::Relaxed), 0);
        assert!(c.record_write(WINDOW_SIZE - 1).is_none());
        assert!(c.evaluate_on_rotation().is_none());
        assert!(c.eval_lock.lock().unwrap().reentry_candidate.is_none());
        assert!(c.record_write(1).is_none());
        assert!(c.evaluate_on_rotation().is_none());
        assert_eq!(
            c.eval_lock.lock().unwrap().reentry_candidate,
            Some((MemtableType::Vec, 1))
        );
    }
}

#[test]
fn test_vec_enter_on_pure_writes() {
    let c = controller();
    record_write_and_confirm(&c, WINDOW_SIZE);
    assert_eq!(c.current_type(), MemtableType::Vec);
}

#[test]
fn test_vec_exit_on_read_after_enter() {
    let c = controller();
    record_write_and_confirm(&c, WINDOW_SIZE);
    assert_eq!(c.current_type(), MemtableType::Vec);
    // One point read in the 16-op sensitive window triggers rollback, not Hash entry.
    assert!(c.record_point_read(1).is_none());
    record_write_and_confirm(&c, VEC_FALLBACK_MIN_OPS - 1);
    assert_eq!(c.current_type(), MemtableType::Skiplist);
}

#[test]
fn test_hash_enter_on_one_point_read_among_writes() {
    let c = controller();
    assert!(c.record_point_read(1).is_none());
    let decision = c.record_write_then_rotate(WINDOW_SIZE - 1).unwrap();
    assert_eq!(decision.target, MemtableType::Hash);
    assert!(!decision.flush_current);
    c.confirm_switch(&decision);
    assert_eq!(c.current_type(), MemtableType::Hash);
}

#[test]
fn test_hash_exit_on_scan() {
    let c = controller();
    record_point_read_and_confirm(&c, WINDOW_SIZE);
    assert_eq!(c.current_type(), MemtableType::Hash);
    // Scans on HASH are poison. Need to cross the 64-op sensitive window.
    for _ in 0..HASH_FALLBACK_MIN_OPS {
        record_range_scan_and_confirm(&c);
    }
    assert_eq!(c.current_type(), MemtableType::Skiplist);
}

#[test]
fn test_hash_stays_with_point_reads_and_writes_no_scans() {
    let c = controller();
    // Enter HASH first.
    record_point_read_and_confirm(&c, WINDOW_SIZE);
    assert_eq!(c.current_type(), MemtableType::Hash);
    // Next window: mixed point reads + writes, no scans -> HASH stays.
    record_point_read_and_confirm(&c, 2000);
    record_write_and_confirm(&c, WINDOW_SIZE - 2000);
    assert_eq!(c.current_type(), MemtableType::Hash);
}

#[test]
fn test_scan_ratio_controls_hash_entry() {
    for (reads, scans, expected) in [(99, 1, MemtableType::Hash), (98, 2, MemtableType::Skiplist)] {
        let c = controller();
        record_point_read_and_confirm(&c, reads);
        for _ in 0..scans {
            record_range_scan_and_confirm(&c);
        }
        record_write_and_confirm(&c, WINDOW_SIZE - reads - scans);
        assert_eq!(c.current_type(), expected);
    }
}

#[test]
fn test_record_zero_is_noop() {
    let c = controller();
    assert!(c.record_write(0).is_none());
    assert!(c.record_point_read(0).is_none());
    assert_eq!(c.current_type(), MemtableType::Skiplist);
}

// === Tests for P1 fix: cross-window batch operations ===

#[test]
fn test_record_write_batch_crosses_window() {
    // record_write(4097) crosses the 4096 boundary - must trigger evaluation.
    let c = controller();
    record_write_and_confirm(&c, WINDOW_SIZE + 1);
    assert_eq!(c.current_type(), MemtableType::Vec);
}

#[test]
fn test_record_point_read_batch_crosses_vec_sensitive_window() {
    // On VEC, record_point_read(17) crosses the 16-op boundary.
    let c = controller();
    record_write_and_confirm(&c, WINDOW_SIZE);
    assert_eq!(c.current_type(), MemtableType::Vec);
    record_point_read_and_confirm(&c, VEC_FALLBACK_MIN_OPS + 1);
    assert_eq!(c.current_type(), MemtableType::Skiplist);
}

#[test]
fn test_record_write_large_batch_crosses_multiple_windows() {
    // A very large batch (e.g. 8192 = 2 windows) should still trigger exactly one evaluation
    // (the eval_lock prevents double-eval) and switch to VEC.
    let c = controller();
    record_write_and_confirm(&c, WINDOW_SIZE * 2);
    assert_eq!(c.current_type(), MemtableType::Vec);
}

// === Scan and mixed-workload selection ===

#[test]
fn test_scan_blocks_vec_entry() {
    // A single scan in a window of otherwise pure writes should block VEC entry (rs > 0).
    let c = controller();
    record_write_and_confirm(&c, WINDOW_SIZE - 1);
    record_range_scan_and_confirm(&c);
    assert_eq!(c.current_type(), MemtableType::Skiplist);
}

// === Multi-transition tests ===

#[test]
fn test_multi_transition_skiplist_vec_skiplist_hash() {
    let c = controller();
    // Window 1: pure writes -> Vec.
    record_write_and_confirm(&c, WINDOW_SIZE);
    assert_eq!(c.current_type(), MemtableType::Vec);

    // Window 2: point reads -> Skiplist (flush, since reads are poison on Vec).
    record_point_read_and_confirm(&c, WINDOW_SIZE);
    assert_eq!(c.current_type(), MemtableType::Skiplist);

    // After rollback, three consecutive point-read windows are required to enter Hash.
    for _ in 0..REENTRY_WINDOWS {
        record_point_read_and_confirm(&c, WINDOW_SIZE);
    }
    assert_eq!(c.current_type(), MemtableType::Hash);

    // Next window: pure writes -> Vec (non-disruptive from Hash).
    record_write_and_confirm(&c, WINDOW_SIZE);
    assert_eq!(c.current_type(), MemtableType::Vec);
}

// === Confirmed rollback hysteresis ===

#[test]
fn test_reentry_requires_three_windows_after_hash_or_vec_fallback() {
    for previous in [MemtableType::Hash, MemtableType::Vec] {
        for target in [MemtableType::Hash, MemtableType::Vec] {
            let c = controller_after_fallback(previous);
            for _ in 0..REENTRY_WINDOWS - 1 {
                assert!(record_specialization_window(&c, target).is_none());
                assert_eq!(c.current_type(), MemtableType::Skiplist);
            }
            let decision = record_specialization_window(&c, target).unwrap();
            assert_eq!(decision.target, target);
            assert!(!decision.flush_current);
            assert_eq!(c.current_type(), MemtableType::Skiplist);
            c.confirm_switch(&decision);
            assert_eq!(c.current_type(), target);
        }
    }
}

#[test]
fn test_scan_resets_reentry_windows() {
    for target in [MemtableType::Hash, MemtableType::Vec] {
        let c = controller_after_fallback(MemtableType::Hash);
        for _ in 0..REENTRY_WINDOWS - 1 {
            assert!(record_specialization_window(&c, target).is_none());
        }
        assert!(c.record_range_scan().is_none());
        assert!(c.record_write_then_rotate(WINDOW_SIZE - 1).is_none());
        for _ in 0..REENTRY_WINDOWS - 1 {
            assert!(record_specialization_window(&c, target).is_none());
        }
        assert_eq!(
            record_specialization_window(&c, target).unwrap().target,
            target
        );
    }
}

#[test]
fn test_candidate_change_starts_a_new_reentry_run() {
    let c = controller_after_fallback(MemtableType::Vec);
    for target in [
        MemtableType::Vec,
        MemtableType::Vec,
        MemtableType::Hash,
        MemtableType::Hash,
        MemtableType::Vec,
        MemtableType::Vec,
    ] {
        assert!(record_specialization_window(&c, target).is_none());
    }
    assert_eq!(
        record_specialization_window(&c, MemtableType::Vec)
            .unwrap()
            .target,
        MemtableType::Vec
    );
}

#[test]
fn test_large_batch_counts_as_one_reentry_window() {
    let c = controller_after_fallback(MemtableType::Vec);
    assert!(c.record_write_then_rotate(WINDOW_SIZE * 10).is_none());
    assert!(c.record_write_then_rotate(WINDOW_SIZE).is_none());
    assert_eq!(
        c.record_write_then_rotate(WINDOW_SIZE).unwrap().target,
        MemtableType::Vec
    );
}

#[test]
fn test_partial_sensitive_window_does_not_count_after_fallback() {
    let c = controller_after_fallback(MemtableType::Vec);
    assert!(c.record_write(VEC_FALLBACK_MIN_OPS).is_none());
    // Simulate a delayed evaluation started while the controller still tracked Vec.
    assert!(c.evaluate(false).is_none());
    assert!(
        c.record_write_then_rotate(WINDOW_SIZE - VEC_FALLBACK_MIN_OPS)
            .is_none()
    );
    assert!(c.record_write_then_rotate(WINDOW_SIZE).is_none());
    assert_eq!(
        c.record_write_then_rotate(WINDOW_SIZE).unwrap().target,
        MemtableType::Vec
    );
}

#[test]
fn test_repeated_fallback_keeps_three_window_bias() {
    let c = controller_after_fallback(MemtableType::Vec);
    for _ in 0..REENTRY_WINDOWS {
        record_write_and_confirm(&c, WINDOW_SIZE);
    }
    record_point_read_and_confirm(&c, VEC_FALLBACK_MIN_OPS);
    assert_eq!(c.current_type(), MemtableType::Skiplist);
    for _ in 0..REENTRY_WINDOWS - 1 {
        assert!(c.record_write_then_rotate(WINDOW_SIZE).is_none());
    }
    assert_eq!(
        c.record_write_then_rotate(WINDOW_SIZE).unwrap().target,
        MemtableType::Vec
    );
}

#[test]
fn test_cancelled_fallback_does_not_activate_bias() {
    for initial in [MemtableType::Hash, MemtableType::Vec] {
        let c = AdaptiveMemtableController::new(true, initial);
        let decision = record_fallback_window(&c);
        c.cancel_decision(&decision);
        c.confirm_switch(&decision);
        assert_eq!(c.current_type(), initial);
        assert!(!c.eval_lock.lock().unwrap().after_fallback);
    }
}

#[test]
fn test_stale_epoch_fallback_does_not_activate_bias() {
    for initial in [MemtableType::Hash, MemtableType::Vec] {
        let c = AdaptiveMemtableController::new(true, initial);
        let stale = record_fallback_window(&c);
        c.disable(MemtableType::Skiplist);
        c.enable();
        c.confirm_switch(&stale);
        assert!(!c.validate_decision(&stale));
        assert_eq!(
            c.record_write_then_rotate(WINDOW_SIZE).unwrap().target,
            MemtableType::Vec
        );
    }
}

#[test]
fn test_cancelled_reentry_resets_run_without_clearing_bias() {
    let c = controller_after_fallback(MemtableType::Hash);
    for _ in 0..REENTRY_WINDOWS - 1 {
        assert!(c.record_point_read_then_rotate(WINDOW_SIZE).is_none());
    }
    let decision = c.record_point_read_then_rotate(WINDOW_SIZE).unwrap();
    assert!(c.record_write_then_rotate(WINDOW_SIZE * 10).is_none());
    c.cancel_decision(&decision);
    c.confirm_switch(&decision);
    assert_eq!(c.current_type(), MemtableType::Skiplist);
    // Statistics accumulated while pending form one new evaluation, not ten windows.
    assert!(c.record_write_then_rotate(1).is_none());
    assert!(c.record_write_then_rotate(WINDOW_SIZE).is_none());
    let fresh = c.record_write_then_rotate(WINDOW_SIZE).unwrap();
    c.cancel_decision(&decision);
    assert!(c.validate_decision(&fresh));
    c.confirm_switch(&fresh);
    assert_eq!(c.current_type(), MemtableType::Vec);
}

#[test]
fn test_mode_toggle_clears_reentry_bias_candidates_and_statistics() {
    let c = controller_after_fallback(MemtableType::Vec);
    assert!(c.record_write_then_rotate(WINDOW_SIZE).is_none());
    assert!(c.record_write(1).is_none());
    c.enable();
    // Enabling starts a fresh adaptive session, even without a preceding disable.
    assert!(c.record_point_read_then_rotate(WINDOW_SIZE - 1).is_none());
    let first = c.record_point_read_then_rotate(1).unwrap();
    assert_eq!(first.target, MemtableType::Hash);
    c.confirm_switch(&first);
    for _ in 0..HASH_FALLBACK_MIN_OPS {
        record_range_scan_and_confirm(&c);
    }
    assert!(c.record_point_read_then_rotate(WINDOW_SIZE).is_none());
    assert!(c.record_point_read_then_rotate(WINDOW_SIZE).is_none());
    let stale = c.record_point_read_then_rotate(WINDOW_SIZE).unwrap();
    c.disable(MemtableType::Skiplist);
    c.enable();
    c.confirm_switch(&stale);
    let fresh = c.record_write_then_rotate(WINDOW_SIZE).unwrap();
    c.cancel_decision(&stale);
    assert!(c.validate_decision(&fresh));
    c.confirm_switch(&fresh);
    assert_eq!(c.current_type(), MemtableType::Vec);
}

#[test]
fn test_hash_to_vec_via_pure_writes() {
    let c = controller();
    // Enter Hash via point reads.
    record_point_read_and_confirm(&c, WINDOW_SIZE);
    assert_eq!(c.current_type(), MemtableType::Hash);
    // Next window: pure writes -> Vec (non-disruptive, no flush).
    record_write_and_confirm(&c, WINDOW_SIZE);
    assert_eq!(c.current_type(), MemtableType::Vec);
}

// === Deferred vs flush path tests ===

#[test]
fn test_vec_enter_is_deferred_no_flush() {
    let c = controller();
    let decision = c.record_write_then_rotate(WINDOW_SIZE).unwrap();
    assert_eq!(decision.target, MemtableType::Vec);
    assert!(!decision.flush_current);
}

#[test]
fn test_hash_enter_is_deferred_no_flush() {
    let c = controller();
    let decision = c.record_point_read_then_rotate(WINDOW_SIZE).unwrap();
    assert_eq!(decision.target, MemtableType::Hash);
    assert!(!decision.flush_current);
}

#[test]
fn test_vec_exit_is_flush() {
    let c = controller();
    record_write_and_confirm(&c, WINDOW_SIZE);
    let decision = c.record_point_read(VEC_FALLBACK_MIN_OPS).unwrap();
    assert_eq!(decision.target, MemtableType::Skiplist);
    assert!(decision.flush_current);
}

#[test]
fn test_hash_exit_is_flush() {
    let c = controller();
    record_point_read_and_confirm(&c, WINDOW_SIZE);
    let mut decision = None;
    for _ in 0..HASH_FALLBACK_MIN_OPS {
        if let Some(d) = c.record_range_scan() {
            decision = Some(d);
            c.confirm_switch(&d);
            break;
        }
    }
    let decision = decision.expect("should have triggered evaluation");
    assert_eq!(decision.target, MemtableType::Skiplist);
    assert!(decision.flush_current);
}

// === Generation / stale decision tests ===

#[test]
fn test_pending_decision_blocks_new_evaluation() {
    // Only one decision may be in-flight at a time. While d1 is pending (not yet confirmed or
    // cancelled), a second window boundary does NOT produce a new decision. This prevents
    // generation gaps that would permanently stall switching.
    let c = controller();
    // Window 1: 4096 writes -> decide(Skiplist, 0, 0) -> Vec.
    let d1 = c.record_write_then_rotate(WINDOW_SIZE).unwrap();
    assert_eq!(d1.generation, 1);
    assert_eq!(d1.target, MemtableType::Vec);
    // Window 2: while d1 is pending, no new decision is generated.
    let d2 = c.record_point_read_then_rotate(WINDOW_SIZE);
    assert!(d2.is_none(), "no new decision while one is pending");
    // Confirm d1: pending slot is cleared, current_type advances to Vec.
    c.confirm_switch(&d1);
    assert_eq!(c.current_type(), MemtableType::Vec);
    // Now the next window can generate a fresh decision.
    let d3 = c.record_point_read(VEC_FALLBACK_MIN_OPS).unwrap();
    assert_eq!(d3.generation, 2);
    c.confirm_switch(&d3);
    assert_eq!(c.current_type(), MemtableType::Skiplist);
}

#[test]
fn test_cancel_decision_allows_retry() {
    // If a decision is cancelled (e.g. physical switch failed), the pending slot is cleared
    // and the next window can generate a fresh decision without a generation gap.
    let c = controller();
    let d1 = c.record_write_then_rotate(WINDOW_SIZE).unwrap();
    assert_eq!(d1.generation, 1);
    // Cancel d1 (simulates a failed switch). Pending is cleared.
    c.cancel_decision(&d1);
    assert_eq!(c.current_type(), MemtableType::Skiplist); // type unchanged
    // The next window can now generate a new decision. It gets gen=2 (generation is
    // monotonic, never reused).
    let d2 = c.record_write_then_rotate(WINDOW_SIZE).unwrap();
    assert_eq!(d2.generation, 2);
    c.confirm_switch(&d2);
    assert_eq!(c.current_type(), MemtableType::Vec);
}

#[test]
fn test_stale_decision_rejected_after_cancel() {
    // After d1 is cancelled, trying to apply it (stale) is rejected. Only the current
    // pending decision can be applied.
    let c = controller();
    let d1 = c.record_write_then_rotate(WINDOW_SIZE).unwrap();
    c.cancel_decision(&d1);
    // d1 is no longer pending -> validate rejects it.
    assert!(!c.validate_decision(&d1));
    c.confirm_switch(&d1);
    assert_eq!(c.current_type(), MemtableType::Skiplist); // unchanged
}

#[test]
fn test_disable_invalidates_in_flight_decision() {
    // A decision generated before disable() must be rejected after re-enable, because the
    // epoch changed. This prevents a stale in-flight decision from overriding a manual pin.
    let c = controller();
    // Enter Vec via pure writes, but don't confirm.
    let d1 = c.record_write_then_rotate(WINDOW_SIZE).unwrap();
    assert_eq!(d1.target, MemtableType::Vec);
    // Manual pin to Hash: disable bumps epoch, invalidating d1.
    c.disable(MemtableType::Hash);
    assert_eq!(c.current_type(), MemtableType::Hash);
    assert!(!c.is_enabled());
    // Re-enable: epoch bumps again. d1 is from epoch 0, current epoch is 2.
    c.enable();
    assert!(c.is_enabled());
    // d1 must be rejected by validate_decision (stale epoch).
    assert!(!c.validate_decision(&d1));
    // confirm_switch also rejects.
    c.confirm_switch(&d1);
    assert_eq!(c.current_type(), MemtableType::Hash); // unchanged
}

#[test]
fn test_validate_decision_rejects_when_disabled() {
    let c = controller();
    let d1 = c.record_write_then_rotate(WINDOW_SIZE).unwrap();
    c.disable(MemtableType::Skiplist);
    // Controller is disabled: validate must reject even a fresh-looking decision.
    assert!(!c.validate_decision(&d1));
}

#[test]
fn test_mode_toggle_cannot_relabel_an_in_progress_evaluation() {
    // Pause evaluation after it has drained an old session's statistics but before it
    // publishes a decision. A mode toggle must be unable to acquire `eval_lock` during that
    // interval; otherwise the decision could be stamped with the new epoch and be accepted.
    let c = Arc::new(controller());
    let reached = Arc::new(Barrier::new(2));
    let resume = Arc::new(Barrier::new(2));
    c.set_evaluation_hook(Some(EvaluationHook {
        reached: Arc::clone(&reached),
        resume: Arc::clone(&resume),
    }));
    let evaluator = {
        let c = Arc::clone(&c);
        thread::spawn(move || c.record_write_then_rotate(WINDOW_SIZE).unwrap())
    };
    reached.wait();

    // A mode transition uses this same lock, so it cannot begin while the old evaluation is
    // paused before publishing its decision. Checking the lock directly makes this proof
    // deterministic and independent of scheduler timing.
    assert!(
        c.eval_lock.try_lock().is_err(),
        "evaluation must hold eval_lock until its decision is fully published"
    );

    resume.wait();
    let decision = evaluator.join().unwrap();

    // Now complete a mode toggle. The decision was created with the old epoch and must not
    // become valid in the new adaptive session.
    c.disable(MemtableType::Hash);
    c.enable();

    assert_ne!(decision.epoch, c.epoch.load(Ordering::Relaxed));
    assert!(
        !c.validate_decision(&decision),
        "a decision from the old window must be rejected after the mode transition"
    );
    c.set_evaluation_hook(None);
}

#[test]
fn test_initial_type_from_constructor() {
    // When the DB opens with a concrete type (e.g. Hash), the controller should track it
    // so that re-enabling adaptive after a pin resumes from the right type.
    let c = AdaptiveMemtableController::new(false, MemtableType::Hash);
    assert_eq!(c.current_type(), MemtableType::Hash);
    assert!(!c.is_enabled());
    // Enable: resumes from Hash.
    c.enable();
    assert_eq!(c.current_type(), MemtableType::Hash);
    assert!(c.is_enabled());
}

// === Pure decide() function tests ===

#[test]
fn test_decide_access_presence_and_flush_flags() {
    use MemtableType::{Hash, Skiplist, Vec};

    let cases = [
        (Skiplist, 0, 0, Vec, false),
        (Skiplist, 1, 0, Hash, false),
        (Skiplist, 0, 1, Skiplist, false),
        (Skiplist, 1, 1, Skiplist, false),
        (Skiplist, 99, 1, Hash, false),
        (Skiplist, 98, 2, Skiplist, false),
        (Hash, 0, 0, Vec, false),
        (Hash, 1, 0, Hash, false),
        (Hash, 0, 1, Skiplist, true),
        (Hash, 1, 1, Skiplist, true),
        (Hash, 99, 1, Hash, false),
        (Hash, 98, 2, Skiplist, true),
        (Vec, 0, 0, Vec, false),
        (Vec, 1, 0, Skiplist, true),
        (Vec, 0, 1, Skiplist, true),
        (Vec, 1, 1, Skiplist, true),
    ];
    for (prev, point_reads, scans, target, flush_current) in cases {
        let d = decide(prev, point_reads, scans);
        assert_eq!(
            (d.target, d.flush_current),
            (target, flush_current),
            "prev={prev:?}, point_reads={point_reads}, scans={scans}"
        );
    }
}

// === Multi-thread test: concurrent evaluation does not stall ===

#[test]
fn test_concurrent_writers_do_not_stall_evaluation() {
    // Multiple threads writing concurrently should not cause evaluation to permanently stop.
    // After all threads finish, the next rotation should select Vec (all writes, zero reads).
    let c = Arc::new(controller());
    let mut handles = Vec::new();
    for _ in 0..4 {
        let c = Arc::clone(&c);
        handles.push(thread::spawn(move || {
            for _ in 0..1500 {
                if let Some(d) = c.record_write(1) {
                    c.confirm_switch(&d);
                }
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    // 6000 pure-write operations stay Skiplist until the next non-empty rotation.
    assert_eq!(c.current_type(), MemtableType::Skiplist);
    c.confirm_switch(&c.evaluate_on_rotation().unwrap());
    assert_eq!(c.current_type(), MemtableType::Vec);
}

#[test]
fn test_concurrent_mixed_ops_do_not_stall() {
    // Mix of writes and reads from multiple threads. After completion, clean windows
    // of pure writes should still trigger a Vec switch.
    let c = Arc::new(controller());
    let mut handles = Vec::new();
    for i in 0..4 {
        let c = Arc::clone(&c);
        handles.push(thread::spawn(move || {
            for _ in 0..1100 {
                if i % 2 == 0 {
                    if let Some(d) = c.record_write(1) {
                        c.confirm_switch(&d);
                    }
                } else {
                    if let Some(d) = c.record_point_read(1) {
                        c.confirm_switch(&d);
                    }
                }
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    // Now do clean windows of pure writes to verify evaluation still works. Multiple windows
    // are needed because residual counters from the concurrent phase may pollute the first
    // window; three subsequent pure-write windows also satisfy any rollback bias.
    for _ in 0..=REENTRY_WINDOWS {
        if let Some(d) = c.record_write_then_rotate(WINDOW_SIZE) {
            c.confirm_switch(&d);
        }
    }
    assert_eq!(c.current_type(), MemtableType::Vec);
}
