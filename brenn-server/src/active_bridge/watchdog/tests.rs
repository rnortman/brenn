#![cfg(test)]
//! Tests for the bridge-wedge watchdog. All fixtures are synthetic; no CC-trace
//! content is used.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use brenn_lib::config::WatchdogConfig;
use brenn_lib::obs::alerting::{
    AlertSeverity, make_capturing_alerter_with_severity, noop_alert_dispatcher,
};
use brenn_lib::ws_types::{CcState, WsServerMessage};
use tokio::sync::broadcast;

use super::Watchdog;
use crate::active_bridge::ActiveBridge;
use crate::active_bridge::CompactionPhase;
use crate::active_bridge::registry::ActiveBridges;
use crate::active_bridge::test_fixtures::TestBridgeConfig;
use crate::active_bridge::test_support::{
    drain_broadcast, install_dead_io_session, install_failing_session, make_bridge_no_loop,
    set_context_usage, set_waiting_for_idle,
};

/// Build a bridge (no event loop) and register it in `registry`.
async fn registered_bridge(
    registry: &ActiveBridges,
) -> (Arc<ActiveBridge>, broadcast::Receiver<WsServerMessage>) {
    let (bridge, _event_tx, _event_rx, broadcast_rx, _ad, _reg) = make_bridge_no_loop(
        "test",
        noop_alert_dispatcher().0,
        TestBridgeConfig {
            active_bridges: Some(registry.clone()),
            ..Default::default()
        },
    )
    .await;
    registry
        .insert(bridge.conversation_id, bridge.clone())
        .await;
    (bridge, broadcast_rx)
}

/// A `JoinHandle` for a task that has already finished.
async fn finished_handle() -> tokio::task::JoinHandle<()> {
    let handle = tokio::spawn(async {});
    while !handle.is_finished() {
        tokio::task::yield_now().await;
    }
    handle
}

/// A `JoinHandle` for a task that never finishes.
fn live_handle() -> tokio::task::JoinHandle<()> {
    tokio::spawn(std::future::pending::<()>())
}

#[tokio::test]
async fn watchdog_detects_dead_event_loop() {
    let registry = ActiveBridges::new();
    let (bridge, mut broadcast_rx) = registered_bridge(&registry).await;
    let cid = bridge.conversation_id;

    // Arrange a wedge: dead event loop, dirty compaction + context state, and a
    // session to reap.
    bridge.install_event_loop_handle(finished_handle().await);
    set_waiting_for_idle(&bridge).await;
    set_context_usage(&bridge, 80);
    install_failing_session(&bridge).await;

    let (ad, captured, drain_handle) = make_capturing_alerter_with_severity();
    let mut watchdog = Watchdog::new(WatchdogConfig::default(), registry.clone(), ad);
    watchdog.sweep().await;

    // Bridge deregistered, death handled, runtime state reset.
    assert!(
        registry.get(cid).await.is_none(),
        "wedged bridge must be deregistered"
    );
    assert!(bridge.died_handled(), "death must be marked handled");
    {
        let state = bridge.compaction.lock().await;
        assert!(
            matches!(state.phase, CompactionPhase::Normal),
            "compaction phase must reset to Normal"
        );
    }
    assert!(
        bridge.context_usage.lock().unwrap().is_none(),
        "context_usage must be nulled"
    );
    assert!(
        bridge.session.lock().await.is_none(),
        "session must be taken (reaped)"
    );

    // Error + error-status broadcasts fired.
    let msgs = drain_broadcast(&mut broadcast_rx);
    assert!(
        msgs.iter()
            .any(|m| matches!(m, WsServerMessage::Error { .. })),
        "an Error broadcast must be sent: {msgs:?}"
    );
    assert!(
        msgs.iter()
            .any(|m| matches!(m, WsServerMessage::Status { state } if *state == CcState::Error)),
        "an Error status broadcast must be sent: {msgs:?}"
    );

    // Exactly one Critical page naming the predicate.
    drop(watchdog);
    drain_handle.await.unwrap();
    let alerts = captured.lock().unwrap();
    assert_eq!(alerts.len(), 1, "exactly one alert expected: {alerts:?}");
    assert!(matches!(alerts[0].0, AlertSeverity::Critical));
    assert_eq!(alerts[0].1, "Bridge wedged");
    assert!(
        alerts[0].2.contains("dead event loop"),
        "alert body must name the predicate: {}",
        alerts[0].2
    );
}

#[tokio::test]
async fn watchdog_ignores_server_shutdown() {
    let registry = ActiveBridges::new();
    let (bridge, mut broadcast_rx) = registered_bridge(&registry).await;
    let cid = bridge.conversation_id;

    // Dead event loop, but a process-wide server shutdown is in progress: the
    // whole process is going down, so even the dead-loop predicate stands down.
    bridge.install_event_loop_handle(finished_handle().await);
    bridge.server_shutting_down.store(true, Ordering::SeqCst);

    let (ad, captured, drain_handle) = make_capturing_alerter_with_severity();
    let mut watchdog = Watchdog::new(WatchdogConfig::default(), registry.clone(), ad);
    watchdog.sweep().await;

    assert!(
        registry.get(cid).await.is_some(),
        "server shutdown must not deregister the bridge"
    );
    assert!(
        !bridge.died_handled(),
        "no death handling on server shutdown"
    );
    assert!(
        drain_broadcast(&mut broadcast_rx).is_empty(),
        "no broadcasts on server shutdown"
    );

    drop(watchdog);
    drain_handle.await.unwrap();
    assert!(
        captured.lock().unwrap().is_empty(),
        "no alert on server shutdown"
    );
}

/// A per-conversation idle-drain flag must NOT suppress the dead-event-loop
/// predicate: a deferred drain runs *from* the event loop, so once the loop is
/// dead the drain can never complete and the bridge is genuinely wedged.
#[tokio::test]
async fn watchdog_dead_loop_with_drain_flag_still_wedges() {
    let registry = ActiveBridges::new();
    let (bridge, _broadcast_rx) = registered_bridge(&registry).await;
    let cid = bridge.conversation_id;

    bridge.install_event_loop_handle(finished_handle().await);
    bridge.drain_on_idle.store(true, Ordering::SeqCst); // drain flagged, but loop is dead

    let (ad, captured, drain_handle) = make_capturing_alerter_with_severity();
    let mut watchdog = Watchdog::new(WatchdogConfig::default(), registry.clone(), ad);
    watchdog.sweep().await;

    assert!(
        registry.get(cid).await.is_none(),
        "a dead event loop with an un-completable deferred drain must still wedge"
    );
    assert!(bridge.died_handled());

    drop(watchdog);
    drain_handle.await.unwrap();
    let alerts = captured.lock().unwrap();
    assert_eq!(alerts.len(), 1);
    assert!(matches!(alerts[0].0, AlertSeverity::Critical));
    assert!(alerts[0].2.contains("dead event loop"));
}

#[tokio::test]
async fn watchdog_dead_session_busy_bridge_needs_grace() {
    let registry = ActiveBridges::new();
    let (bridge, _broadcast_rx) = registered_bridge(&registry).await;
    let cid = bridge.conversation_id;

    // Busy (cc_idle=false) with no session → cannot make progress. No event-loop
    // handle installed, so predicate 1 does not fire.
    bridge.cc_idle.store(false, Ordering::SeqCst);

    // Default grace = 60s / 30s sweep = 2 sweeps of separation. Firing at
    // `count > grace_sweeps` means the wedge must be observed on 3 sweeps (2
    // sweep-intervals ≈ 60s of real elapsed time) before the watchdog acts.
    assert_eq!(WatchdogConfig::default().grace_sweeps(), 2);

    let (ad, captured, drain_handle) = make_capturing_alerter_with_severity();
    let mut watchdog = Watchdog::new(WatchdogConfig::default(), registry.clone(), ad);

    // First two sweeps: within grace, no action.
    watchdog.sweep().await;
    assert!(
        registry.get(cid).await.is_some(),
        "first sweep must not act (grace)"
    );
    watchdog.sweep().await;
    assert!(
        registry.get(cid).await.is_some(),
        "second sweep must not act (grace not yet elapsed)"
    );

    // Third sweep: grace exceeded, wedge handled.
    watchdog.sweep().await;
    assert!(
        registry.get(cid).await.is_none(),
        "third sweep must deregister the wedged bridge"
    );
    assert!(bridge.died_handled());

    drop(watchdog);
    drain_handle.await.unwrap();
    let alerts = captured.lock().unwrap();
    assert_eq!(alerts.len(), 1);
    assert!(matches!(alerts[0].0, AlertSeverity::Critical));
    assert!(alerts[0].2.contains("dead session I/O"));
}

/// Predicate 2 via `!s.is_alive()`: a busy bridge whose session is present but
/// dead (reader flipped `alive` false) with a live event loop wedges after
/// grace. Uses a short grace so `grace_sweeps() == 1` (fires on the 2nd sweep).
#[tokio::test]
async fn watchdog_dead_session_not_alive_busy_bridge_wedges() {
    let registry = ActiveBridges::new();
    let (bridge, _broadcast_rx) = registered_bridge(&registry).await;
    let cid = bridge.conversation_id;

    bridge.cc_idle.store(false, Ordering::SeqCst);
    install_failing_session(&bridge).await; // is_alive() = true initially
    bridge
        .session
        .lock()
        .await
        .as_ref()
        .expect("session installed")
        .mark_dead_for_test(); // now is_alive() = false
    bridge.install_event_loop_handle(live_handle());

    let config = WatchdogConfig {
        sweep_interval_secs: 30,
        wedge_grace_secs: 1,
    };
    assert_eq!(config.grace_sweeps(), 1);

    let (ad, captured, drain_handle) = make_capturing_alerter_with_severity();
    let mut watchdog = Watchdog::new(config, registry.clone(), ad);

    watchdog.sweep().await;
    assert!(
        registry.get(cid).await.is_some(),
        "first sweep within grace"
    );
    watchdog.sweep().await;
    assert!(
        registry.get(cid).await.is_none(),
        "dead (not-alive) session with a busy bridge must wedge after grace"
    );
    assert!(bridge.died_handled());

    drop(watchdog);
    drain_handle.await.unwrap();
    let alerts = captured.lock().unwrap();
    assert_eq!(alerts.len(), 1);
    assert!(matches!(alerts[0].0, AlertSeverity::Critical));
    assert!(alerts[0].2.contains("dead session I/O"));
}

/// Predicate 2 via `!s.io_alive()`: the conv45 incident signature — the reader
/// task exited (io dead) while `alive` stays true. A busy bridge with a live
/// event loop must still wedge after grace.
#[tokio::test]
async fn watchdog_dead_io_busy_bridge_wedges() {
    let registry = ActiveBridges::new();
    let (bridge, _broadcast_rx) = registered_bridge(&registry).await;
    let cid = bridge.conversation_id;

    bridge.cc_idle.store(false, Ordering::SeqCst);
    install_dead_io_session(&bridge).await; // is_alive() = true, io_alive() = false
    bridge.install_event_loop_handle(live_handle());

    let config = WatchdogConfig {
        sweep_interval_secs: 30,
        wedge_grace_secs: 1,
    };
    assert_eq!(config.grace_sweeps(), 1);

    let (ad, captured, drain_handle) = make_capturing_alerter_with_severity();
    let mut watchdog = Watchdog::new(config, registry.clone(), ad);

    watchdog.sweep().await;
    assert!(
        registry.get(cid).await.is_some(),
        "first sweep within grace"
    );
    watchdog.sweep().await;
    assert!(
        registry.get(cid).await.is_none(),
        "dead-I/O busy bridge (reader exited, alive still true) must wedge after grace"
    );
    assert!(bridge.died_handled());

    drop(watchdog);
    drain_handle.await.unwrap();
    let alerts = captured.lock().unwrap();
    assert_eq!(alerts.len(), 1);
    assert!(matches!(alerts[0].0, AlertSeverity::Critical));
    assert!(alerts[0].2.contains("dead session I/O"));
}

#[tokio::test]
async fn watchdog_died_handled_suppresses_wedge() {
    let registry = ActiveBridges::new();
    let (bridge, _broadcast_rx) = registered_bridge(&registry).await;
    let cid = bridge.conversation_id;

    // Busy with no session (would otherwise wedge under predicate 2), but the
    // death has already been handled.
    bridge.cc_idle.store(false, Ordering::SeqCst);
    bridge.died_handled.store(true, Ordering::SeqCst);

    let (ad, captured, drain_handle) = make_capturing_alerter_with_severity();
    let mut watchdog = Watchdog::new(WatchdogConfig::default(), registry.clone(), ad);
    watchdog.sweep().await;
    watchdog.sweep().await;
    watchdog.sweep().await;

    assert!(
        registry.get(cid).await.is_some(),
        "died_handled must suppress the wedge"
    );

    drop(watchdog);
    drain_handle.await.unwrap();
    assert!(captured.lock().unwrap().is_empty());
}

#[tokio::test]
async fn watchdog_leaves_healthy_busy_bridge_alone() {
    let registry = ActiveBridges::new();
    let (bridge, mut broadcast_rx) = registered_bridge(&registry).await;
    let cid = bridge.conversation_id;

    // Busy, but with a live session and a live event loop — a long turn, not a
    // wedge.
    bridge.cc_idle.store(false, Ordering::SeqCst);
    install_failing_session(&bridge).await; // dummy: is_alive() = true, io_alive() = true
    bridge.install_event_loop_handle(live_handle());

    let (ad, captured, drain_handle) = make_capturing_alerter_with_severity();
    let mut watchdog = Watchdog::new(WatchdogConfig::default(), registry.clone(), ad);
    watchdog.sweep().await;
    watchdog.sweep().await;
    watchdog.sweep().await;

    assert!(
        registry.get(cid).await.is_some(),
        "healthy busy bridge must be left alone"
    );
    assert!(!bridge.died_handled());
    assert!(drain_broadcast(&mut broadcast_rx).is_empty());

    drop(watchdog);
    drain_handle.await.unwrap();
    assert!(captured.lock().unwrap().is_empty());
}
