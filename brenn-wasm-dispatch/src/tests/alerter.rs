//! `DispatcherAlerter` bridge unit test.

use super::*;

#[tokio::test]
async fn dispatcher_alerter_prefixes_title_and_maps_severity() {
    let (alert_dispatcher, captured, alert_handle) = make_capturing_alerter_with_severity();
    let alerter = DispatcherAlerter::new(alert_dispatcher, "my-component".to_string());

    alerter.alert(GuestAlertSeverity::Info, "guest-title", "guest-body");
    alerter.alert(GuestAlertSeverity::Warning, "warn-title", "warn-body");
    alerter.alert(GuestAlertSeverity::Critical, "crit-title", "crit-body");

    // Wait for the background alert task to drain.
    drop(alerter);
    alert_handle.await.unwrap();

    let alerts = captured.lock().unwrap();
    assert_eq!(alerts.len(), 3, "expected 3 alerts, got {}", alerts.len());

    // Info
    assert!(
        matches!(alerts[0].0, AlertSeverity::Info),
        "first alert must be Info"
    );
    assert_eq!(
        alerts[0].1, "WASM my-component: guest-title",
        "title must be host-prefixed"
    );
    assert_eq!(alerts[0].2, "guest-body");

    // Warning
    assert!(
        matches!(alerts[1].0, AlertSeverity::Warning),
        "second alert must be Warning"
    );
    assert_eq!(alerts[1].1, "WASM my-component: warn-title");
    assert_eq!(alerts[1].2, "warn-body");

    // Critical
    assert!(
        matches!(alerts[2].0, AlertSeverity::Critical),
        "third alert must be Critical"
    );
    assert_eq!(alerts[2].1, "WASM my-component: crit-title");
    assert_eq!(alerts[2].2, "crit-body");
}
