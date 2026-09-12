use super::AutoRefreshController;
use std::sync::atomic::Ordering;
use std::time::Duration;

#[test]
fn auto_refresh_checks_are_throttled_without_blocking_contenders() {
    let controller = AutoRefreshController::new(Some(Duration::from_secs(60)));
    assert!(!controller.due());

    controller.next_check_at.store(0, Ordering::Release);
    assert!(controller.due());
    controller.schedule_next_check();
    assert!(!controller.due());

    let _guard = controller.lock().unwrap();
    assert!(controller.try_lock().unwrap().is_none());
}
