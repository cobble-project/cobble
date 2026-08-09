use super::*;
use std::sync::{Arc, Barrier};
use std::thread;

fn controller() -> AdaptiveMemtableController {
    AdaptiveMemtableController::new(true, MemtableType::Skiplist)
}

/// Helper: record writes and confirm any resulting switch (auto-confirms in tests).
fn record_write_and_confirm(c: &AdaptiveMemtableController, count: u64) {
    if let Some(d) = c.record_write(count) {
        c.confirm_switch(&d);
    }
}

fn record_point_read_and_confirm(c: &AdaptiveMemtableController, count: u64) {
    if let Some(d) = c.record_point_read(count) {
        c.confirm_switch(&d);
    }
}

fn record_range_scan_and_confirm(c: &AdaptiveMemtableController) {
    if let Some(d) = c.record_range_scan() {
        c.confirm_switch(&d);
    }
}

#[test]
fn test_initial_type_is_skiplist() {
    let c = controller();
    assert_eq!(c.current_type(), MemtableType::Skiplist);
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
    // A single point read on VEC triggers the 16-op sensitive window fast rollback.
    record_point_read_and_confirm(&c, VEC_READ_SENSITIVE_WINDOW_SIZE);
    assert_eq!(c.current_type(), MemtableType::Skiplist);
}

#[test]
fn test_hash_enter_on_point_reads() {
    let c = controller();
    record_point_read_and_confirm(&c, WINDOW_SIZE);
    assert_eq!(c.current_type(), MemtableType::Hash);
}

#[test]
fn test_hash_exit_on_scan() {
    let c = controller();
    record_point_read_and_confirm(&c, WINDOW_SIZE);
    assert_eq!(c.current_type(), MemtableType::Hash);
    // Scans on HASH are poison. Need to cross the 64-op sensitive window.
    for _ in 0..SENSITIVE_WINDOW_SIZE {
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
fn test_mixed_workload_stays_skiplist() {
    let c = controller();
    record_point_read_and_confirm(&c, 1000);
    record_range_scan_and_confirm(&c);
    record_write_and_confirm(&c, WINDOW_SIZE - 1001);
    assert_eq!(c.current_type(), MemtableType::Skiplist);
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
    record_point_read_and_confirm(&c, VEC_READ_SENSITIVE_WINDOW_SIZE + 1);
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

// === Tests for scan as boolean signal (SCAN_WEIGHT removed) ===

#[test]
fn test_scan_blocks_vec_entry() {
    // A single scan in a window of otherwise pure writes should block VEC entry (rs > 0).
    let c = controller();
    record_write_and_confirm(&c, WINDOW_SIZE - 1);
    record_range_scan_and_confirm(&c);
    // sum = pr(0) + rs(1) + wr(4095) = 4096. pr==0 && rs==0 is false -> not pure writes.
    // prev=Skiplist, not Vec -> rule 2 doesn't fire. rs > 0 but prev != Hash -> rule 3 doesn't fire.
    // pr=0, rs > 0 -> rule 4 requires rs==0 -> no. Rule 6: stay Skiplist.
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

    // Window 3: pure point reads -> Hash.
    record_point_read_and_confirm(&c, WINDOW_SIZE);
    assert_eq!(c.current_type(), MemtableType::Hash);

    // Window 4: pure writes -> Vec (non-disruptive from Hash).
    record_write_and_confirm(&c, WINDOW_SIZE);
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
    let decision = c.record_write(WINDOW_SIZE).unwrap();
    assert_eq!(decision.target, MemtableType::Vec);
    assert!(!decision.flush_current);
}

#[test]
fn test_hash_enter_is_deferred_no_flush() {
    let c = controller();
    let decision = c.record_point_read(WINDOW_SIZE).unwrap();
    assert_eq!(decision.target, MemtableType::Hash);
    assert!(!decision.flush_current);
}

#[test]
fn test_vec_exit_is_flush() {
    let c = controller();
    record_write_and_confirm(&c, WINDOW_SIZE);
    let decision = c.record_point_read(VEC_READ_SENSITIVE_WINDOW_SIZE).unwrap();
    assert_eq!(decision.target, MemtableType::Skiplist);
    assert!(decision.flush_current);
}

#[test]
fn test_hash_exit_is_flush() {
    let c = controller();
    record_point_read_and_confirm(&c, WINDOW_SIZE);
    let mut decision = None;
    for _ in 0..SENSITIVE_WINDOW_SIZE {
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
    // Window 1: 4096 writes -> decide(Skiplist, 0, 0, 4096) -> Vec.
    let d1 = c.record_write(WINDOW_SIZE).unwrap();
    assert_eq!(d1.generation, 1);
    assert_eq!(d1.target, MemtableType::Vec);
    // Window 2: while d1 is pending, no new decision is generated.
    let d2 = c.record_point_read(WINDOW_SIZE);
    assert!(d2.is_none(), "no new decision while one is pending");
    // Confirm d1: pending slot is cleared, current_type advances to Vec.
    c.confirm_switch(&d1);
    assert_eq!(c.current_type(), MemtableType::Vec);
    // Now the next window can generate a fresh decision.
    let d3 = c.record_point_read(VEC_READ_SENSITIVE_WINDOW_SIZE).unwrap();
    assert_eq!(d3.generation, 2);
    c.confirm_switch(&d3);
    assert_eq!(c.current_type(), MemtableType::Skiplist);
}

#[test]
fn test_cancel_decision_allows_retry() {
    // If a decision is cancelled (e.g. physical switch failed), the pending slot is cleared
    // and the next window can generate a fresh decision without a generation gap.
    let c = controller();
    let d1 = c.record_write(WINDOW_SIZE).unwrap();
    assert_eq!(d1.generation, 1);
    // Cancel d1 (simulates a failed switch). Pending is cleared.
    c.cancel_decision(&d1);
    assert_eq!(c.current_type(), MemtableType::Skiplist); // type unchanged
    // The next window can now generate a new decision. It gets gen=2 (generation is
    // monotonic, never reused).
    let d2 = c.record_write(WINDOW_SIZE).unwrap();
    assert_eq!(d2.generation, 2);
    c.confirm_switch(&d2);
    assert_eq!(c.current_type(), MemtableType::Vec);
}

#[test]
fn test_stale_decision_rejected_after_cancel() {
    // After d1 is cancelled, trying to apply it (stale) is rejected. Only the current
    // pending decision can be applied.
    let c = controller();
    let d1 = c.record_write(WINDOW_SIZE).unwrap();
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
    let d1 = c.record_write(WINDOW_SIZE).unwrap();
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
    let d1 = c.record_write(WINDOW_SIZE).unwrap();
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
        thread::spawn(move || c.record_write(WINDOW_SIZE).unwrap())
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
fn test_decide_pure_writes_enter_vec() {
    let d = decide(MemtableType::Skiplist, 0, 0, 4096);
    assert_eq!(d.target, MemtableType::Vec);
    assert!(!d.flush_current);
}

#[test]
fn test_decide_point_read_dominant_enter_hash() {
    let d = decide(MemtableType::Skiplist, 4096, 0, 4096);
    assert_eq!(d.target, MemtableType::Hash);
    assert!(!d.flush_current);
}

#[test]
fn test_decide_vec_exit_on_any_read() {
    let d = decide(MemtableType::Vec, 1, 0, 4096);
    assert_eq!(d.target, MemtableType::Skiplist);
    assert!(d.flush_current);
}

#[test]
fn test_decide_vec_exit_on_any_scan() {
    let d = decide(MemtableType::Vec, 0, 1, 4096);
    assert_eq!(d.target, MemtableType::Skiplist);
    assert!(d.flush_current);
}

#[test]
fn test_decide_vec_stays_pure_writes() {
    let d = decide(MemtableType::Vec, 0, 0, 4096);
    assert_eq!(d.target, MemtableType::Vec);
    assert!(!d.flush_current);
}

#[test]
fn test_decide_hash_exit_on_scan() {
    let d = decide(MemtableType::Hash, 4090, 1, 4096);
    assert_eq!(d.target, MemtableType::Skiplist);
    assert!(d.flush_current);
}

#[test]
fn test_decide_hash_stays_no_scans() {
    let d = decide(MemtableType::Hash, 2000, 0, 4096);
    assert_eq!(d.target, MemtableType::Hash);
    assert!(!d.flush_current);
}

#[test]
fn test_decide_mixed_stays_skiplist() {
    let d = decide(MemtableType::Skiplist, 1000, 10, 4096);
    assert_eq!(d.target, MemtableType::Skiplist);
    assert!(!d.flush_current);
}

// === Multi-thread test: concurrent evaluation does not stall ===

#[test]
fn test_concurrent_writers_do_not_stall_evaluation() {
    // Multiple threads writing concurrently should not cause evaluation to permanently stop.
    // After all threads finish, the type should be Vec (all writes, zero reads).
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
    // 4 * 1500 = 6000 ops total, which crossed the 4096 window at least once.
    // The type should be Vec (all writes, zero reads).
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
    // window; the second window will be purely writes.
    for _ in 0..3 {
        if let Some(d) = c.record_write(WINDOW_SIZE) {
            c.confirm_switch(&d);
        }
    }
    assert_eq!(c.current_type(), MemtableType::Vec);
}
