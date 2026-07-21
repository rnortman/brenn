#![cfg(test)]
//! Shared test helpers used across active_bridge submodules' test blocks: bridge constructors with various service configurations, broadcast helpers, mount fixtures, and approval-request fixtures.

use std::sync::Arc;
use std::time::{Duration, Instant};

use brenn_cc::session::SessionEvent;
use brenn_cc::session::{ApprovalKind, ApprovalRequest};
use brenn_lib::config::PathMapper;

use crate::active_bridge::test_fixtures::TestBridgeConfig;
use brenn_lib::conversation;
use brenn_lib::obs::alerting::{AlertDispatcher, noop_alert_dispatcher};
use brenn_lib::ws_types::WsServerMessage;
use tokio::sync::broadcast::error::TryRecvError;
use tokio::sync::oneshot;
use tokio::sync::{broadcast, mpsc, watch};

use super::cc_event_loop::cc_event_loop;
use super::compaction::{CompactionPhase, ContextUsage};
use super::{ActiveBridge, ActiveBridges};

/// Core no-spawn bridge constructor: runs the shared 5-step init
/// (db, user, conversation, broadcast channel, mpsc channel) and returns the
/// full superset of handles. Callers destructure what they need and add extras.
///
/// When `cfg.active_bridges` is `None` (the default), a fresh `ActiveBridges::new()`
/// is minted and returned. When `cfg.active_bridges` is `Some(registry)`, that
/// caller-supplied registry is used, enabling two bridges to share one `ActiveBridges`
/// instance in a single test (e.g. the cross-bridge isolation test). The resolved
/// registry is always returned as the last tuple element regardless of which path
/// was taken.
///
/// Parts A and B share `TestBridgeConfig`/`inject_for_test_full` but **not**
/// this helper — the automation helpers (`test_new_with_automation*`) own their
/// own `broadcast::channel(16)` and do not call `make_bridge_no_loop`.
pub(in crate::active_bridge) async fn make_bridge_no_loop(
    app_slug: &str,
    alert_dispatcher: AlertDispatcher,
    mut cfg: TestBridgeConfig,
) -> (
    Arc<ActiveBridge>,
    mpsc::Sender<SessionEvent>,
    mpsc::Receiver<SessionEvent>,
    broadcast::Receiver<WsServerMessage>,
    AlertDispatcher,
    ActiveBridges,
) {
    let db = brenn_lib::db::init_db_memory();
    // Honor a caller-supplied shared registry; mint a fresh one if None.
    let active_bridges = cfg.active_bridges.take().unwrap_or_else(ActiveBridges::new);

    let (user_id, conv_id) = {
        let conn = db.lock().await;
        let uid = brenn_lib::auth::user::create_user(&conn, "testuser", "$argon2id$fake");
        let cid = conversation::create_conversation(&conn, uid, app_slug, false);
        (uid, cid)
    };

    let (broadcast_tx, broadcast_rx) = broadcast::channel(64);
    let (event_tx, event_rx) = mpsc::channel(64);
    // Pass the resolved registry directly via struct update — no mutation of cfg.
    // Clone the dispatcher onto the bridge; the original is still returned in the
    // tuple so callers (e.g. `test_bridge_with_dispatcher`) can wire a
    // `CountingAlerter` and assert on `&self`-handler-emitted security signals.
    let bridge = ActiveBridge::inject_for_test_full(
        user_id,
        conv_id,
        app_slug,
        db,
        broadcast_tx,
        alert_dispatcher.clone(),
        TestBridgeConfig {
            active_bridges: Some(active_bridges.clone()),
            ..cfg
        },
    );

    (
        bridge,
        event_tx,
        event_rx,
        broadcast_rx,
        alert_dispatcher,
        active_bridges,
    )
}

/// Helper: create a test bridge with event channels for testing event routing.
pub(in crate::active_bridge) async fn test_bridge() -> (
    Arc<ActiveBridge>,
    mpsc::Sender<SessionEvent>,
    broadcast::Receiver<WsServerMessage>,
    ActiveBridges,
) {
    let (alert_dispatcher, _handle) = noop_alert_dispatcher();
    test_bridge_with_dispatcher(alert_dispatcher).await
}

/// Core bridge constructor used by all test-bridge helpers.
///
/// Delegates init to `make_bridge_no_loop`, then spawns the event loop.
///
/// When `cfg.active_bridges` is `None` (the default for all current single-bridge
/// callers), a fresh per-bridge `ActiveBridges::new()` is minted. When it is
/// `Some(registry)`, the caller's registry is honored, enabling tests that need
/// two bridges in one shared registry. The resolved registry is always returned.
pub(in crate::active_bridge) async fn test_bridge_with_config(
    cfg: TestBridgeConfig,
    alert_dispatcher: AlertDispatcher,
) -> (
    Arc<ActiveBridge>,
    mpsc::Sender<SessionEvent>,
    broadcast::Receiver<WsServerMessage>,
    ActiveBridges,
) {
    let (bridge, event_tx, event_rx, broadcast_rx, alert_dispatcher, active_bridges) =
        make_bridge_no_loop("test", alert_dispatcher, cfg).await;
    tokio::spawn(cc_event_loop(event_rx, bridge.clone(), alert_dispatcher));
    (bridge, event_tx, broadcast_rx, active_bridges)
}

/// Variant of `test_bridge` that accepts a pre-built alert dispatcher so
/// tests can use a `CountingAlerter` to assert on alert dispatch.
pub(in crate::active_bridge) async fn test_bridge_with_dispatcher(
    alert_dispatcher: AlertDispatcher,
) -> (
    Arc<ActiveBridge>,
    mpsc::Sender<SessionEvent>,
    broadcast::Receiver<WsServerMessage>,
    ActiveBridges,
) {
    test_bridge_with_config(TestBridgeConfig::default(), alert_dispatcher).await
}

/// Receive the next broadcast message with a timeout.
pub(in crate::active_bridge) async fn recv_broadcast(
    rx: &mut broadcast::Receiver<WsServerMessage>,
) -> WsServerMessage {
    tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
        .await
        .expect("broadcast recv timed out")
        .expect("broadcast channel closed")
}

/// Subscribe to the event-loop epoch channel. Call BEFORE sending the event or
/// triggering the action under test. Await the returned receiver with
/// `await_fence` AFTER.
///
/// The two-step ordering is required: creating the fence after sending risks
/// the event loop advancing the epoch before `subscribe()` runs, causing
/// `changed()` to wait indefinitely for the next-next epoch.
///
/// **Failure mode:** If the fence is created after the action has already caused
/// an epoch increment (e.g., after consuming the action's broadcast with
/// `recv_broadcast`), `changed()` blocks until the next unrelated epoch increment
/// or the 2-second timeout — whichever comes first. Tests that consume a broadcast
/// before creating the fence are at risk of this: always create the fence before
/// sending the event, not after observing its effects.
pub(in crate::active_bridge) fn event_fence(bridge: &ActiveBridge) -> watch::Receiver<u64> {
    bridge.event_loop_epoch.subscribe()
}

/// Block until the event loop processes at least one event after the fence was
/// created. 2-second timeout matches `recv_broadcast`.
pub(in crate::active_bridge) async fn await_fence(mut rx: watch::Receiver<u64>) {
    tokio::time::timeout(std::time::Duration::from_secs(2), rx.changed())
        .await
        .expect("event loop did not process event within 2s")
        .expect("event loop epoch channel closed");
}

/// Block until the event loop has incremented the epoch `n` times past the
/// value when the fence was subscribed. Use when the action under test triggers
/// exactly `n` epoch increments (e.g. startup-drain + teardown = 2).
pub(in crate::active_bridge) async fn await_fence_n(mut rx: watch::Receiver<u64>, n: u64) {
    let target = *rx.borrow() + n;
    while *rx.borrow() < target {
        tokio::time::timeout(std::time::Duration::from_secs(2), rx.changed())
            .await
            .expect("event loop did not reach expected epoch within 2s")
            .expect("event loop epoch channel closed");
    }
}

/// Drain all available broadcast messages.
///
/// Panics on `Lagged` — a lagged receiver means the test channel is too small
/// or the code under test emitted unexpectedly many messages. Silently
/// swallowing lag would allow assertions to pass for the wrong reason.
pub(in crate::active_bridge) fn drain_broadcast(
    rx: &mut broadcast::Receiver<WsServerMessage>,
) -> Vec<WsServerMessage> {
    let mut msgs = Vec::new();
    loop {
        match rx.try_recv() {
            Ok(msg) => msgs.push(msg),
            Err(TryRecvError::Empty) => break,
            Err(TryRecvError::Lagged(n)) => panic!(
                "broadcast receiver lagged by {n} — test channel too small or drain emitted \
                 unexpectedly many messages"
            ),
            Err(TryRecvError::Closed) => break,
        }
    }
    msgs
}

/// Shut down a test event loop and drain its capturing alerter.
///
/// Drops both `AlertDispatcher` clones — the one held by `cc_event_loop` (released
/// when `event_tx` closes the loop) and the one stored on `bridge.alert_dispatcher`
/// — then awaits the drainer `handle`. Both clones must be gone before the alert
/// mpsc closes, otherwise `handle.await` blocks forever. Awaiting also flushes
/// pending broadcasts as a side effect of the loop completing, so callers may
/// `drain_broadcast` afterward.
pub(in crate::active_bridge) async fn drop_and_drain_alerts(
    event_tx: mpsc::Sender<SessionEvent>,
    bridge: Arc<ActiveBridge>,
    handle: tokio::task::JoinHandle<()>,
) {
    drop(event_tx);
    drop(bridge);
    handle.await.unwrap();
}

/// Helper: create a test bridge with a non-empty `allowed_users` list.
pub(in crate::active_bridge) async fn test_bridge_with_allowed_users(
    allowed_users: Vec<String>,
) -> (
    Arc<ActiveBridge>,
    mpsc::Sender<SessionEvent>,
    broadcast::Receiver<WsServerMessage>,
    ActiveBridges,
) {
    let (alert_dispatcher, _handle) = noop_alert_dispatcher();
    test_bridge_with_config(
        TestBridgeConfig {
            allowed_users,
            ..Default::default()
        },
        alert_dispatcher,
    )
    .await
}

/// Helper: create a shared test bridge (shared=true) for testing shared-bridge guards.
pub(in crate::active_bridge) async fn test_shared_bridge() -> (
    Arc<ActiveBridge>,
    mpsc::Sender<SessionEvent>,
    broadcast::Receiver<WsServerMessage>,
    ActiveBridges,
) {
    let (alert_dispatcher, _handle) = noop_alert_dispatcher();
    test_bridge_with_config(
        TestBridgeConfig {
            shared: true,
            ..Default::default()
        },
        alert_dispatcher,
    )
    .await
}

/// Overwrite `bridge.session` with `CcSession::dummy_for_test()`: alive flag = true,
/// send channel closed → all sends return `Err(CcError::SendFailed)`.
///
/// Composable: works after any bridge constructor. No broadcasts are emitted by the
/// overwrite, so a `broadcast::Receiver` subscribed before this call is safe for
/// post-action assertions.
pub(in crate::active_bridge) async fn install_failing_session(bridge: &Arc<ActiveBridge>) {
    let mut guard = bridge.session.lock().await;
    *guard = Some(brenn_cc::session::CcSession::dummy_for_test());
}

/// Overwrite `bridge.session` with a session whose I/O tasks are installed but
/// whose reader task has already finished: `is_alive() = true`, `io_alive() =
/// false`. Reproduces the conv45 incident signature — the reader exited via the
/// "consumer gone" branch without flipping `alive` — so watchdog tests can
/// exercise the `!io_alive()` wedge predicate.
pub(in crate::active_bridge) async fn install_dead_io_session(bridge: &Arc<ActiveBridge>) {
    let mut guard = bridge.session.lock().await;
    *guard = Some(brenn_cc::session::CcSession::dummy_with_dead_io_for_test().await);
}

/// Overwrite `bridge.session` with `CcSession::recording_for_test()`: alive flag = true,
/// live outgoing channel — sends succeed and the returned receiver captures every
/// `OutgoingEnvelope` delivered to the session. Access the `CcOutgoing` via `.msg`;
/// the `.ack` sender (if present) may be resolved manually in tests exercising the
/// flush-ack path.
///
/// Composable: works after any bridge constructor.
pub(in crate::active_bridge) async fn install_recording_session(
    bridge: &Arc<ActiveBridge>,
) -> tokio::sync::mpsc::Receiver<brenn_cc::session::OutgoingEnvelope> {
    let (session, rx) = brenn_cc::session::CcSession::recording_for_test();
    let mut guard = bridge.session.lock().await;
    *guard = Some(session);
    rx
}

/// Overwrite `bridge.session` with `CcSession::stalling_for_test()`: alive flag = true,
/// live outgoing channel (cap 64), `auto_ack = false` — acks are held hostage.
///
/// Returns `rx` that receives every `OutgoingEnvelope`. Envelopes for acked sends carry
/// `ack: Some(tx)`; the test manually fires `tx.send(Ok(()))` to unblock the awaiting
/// `persist_broadcast_send`, or fires `Err` to simulate a flush failure, or drops `tx`
/// to simulate the writer task exiting (RecvError → flush failure).
///
/// Use this in tests validating the lock-release-before-await property of §2.6: the
/// session lock is released after enqueue, so a second caller can proceed while the
/// first is blocked awaiting its ack.
pub(in crate::active_bridge) async fn install_stalling_session(
    bridge: &Arc<ActiveBridge>,
) -> tokio::sync::mpsc::Receiver<brenn_cc::session::OutgoingEnvelope> {
    let (session, rx) = brenn_cc::session::CcSession::stalling_for_test();
    let mut guard = bridge.session.lock().await;
    *guard = Some(session);
    rx
}

/// Helper: create a test bridge whose CC session is alive but immediately fails all sends.
///
/// Built on top of `test_bridge()`, then the session is overwritten with
/// `CcSession::dummy_for_test()` via `install_failing_session` (alive flag = true,
/// send channel closed → Err). The broadcast receiver is subscribed before the
/// session overwrite; no broadcasts are emitted during the overwrite, so it is safe
/// to use for post-action assertions.
pub(in crate::active_bridge) async fn test_bridge_with_failing_session() -> (
    Arc<ActiveBridge>,
    mpsc::Sender<SessionEvent>,
    broadcast::Receiver<WsServerMessage>,
    ActiveBridges,
) {
    let (bridge, event_tx, broadcast_rx, active_bridges) = test_bridge().await;
    install_failing_session(&bridge).await;
    (bridge, event_tx, broadcast_rx, active_bridges)
}

/// Helper: create a test bridge with cwd set (for artifact tests).
pub(in crate::active_bridge) async fn test_bridge_with_cwd(
    cwd: &str,
) -> (
    Arc<ActiveBridge>,
    mpsc::Sender<SessionEvent>,
    broadcast::Receiver<WsServerMessage>,
) {
    test_bridge_with_cwd_and_mounts(cwd, vec![]).await
}

/// Helper: create a test bridge with cwd and a list of repo mounts.
/// Delegates to `test_bridge_with_cwd_and_mounts_and_mapper` with `PathMapper::Identity`.
pub(in crate::active_bridge) async fn test_bridge_with_cwd_and_mounts(
    cwd: &str,
    mounts: Vec<brenn_lib::config::ResolvedMount>,
) -> (
    Arc<ActiveBridge>,
    mpsc::Sender<SessionEvent>,
    broadcast::Receiver<WsServerMessage>,
) {
    test_bridge_with_cwd_and_mounts_and_mapper(cwd, mounts, PathMapper::Identity).await
}

/// Helper: create a singleton test bridge.
pub(in crate::active_bridge) async fn test_bridge_singleton() -> (
    Arc<ActiveBridge>,
    mpsc::Sender<SessionEvent>,
    broadcast::Receiver<WsServerMessage>,
    ActiveBridges,
) {
    let (alert_dispatcher, _handle) = noop_alert_dispatcher();
    test_bridge_with_config(
        TestBridgeConfig {
            singleton: true,
            ..Default::default()
        },
        alert_dispatcher,
    )
    .await
}

// -----------------------------------------------------------------------
// Device virtual tool integration tests (design §4, §2.8)
// -----------------------------------------------------------------------

/// Helper: create a real device row + device_users membership and return the device_id.
pub(in crate::active_bridge) async fn create_test_device_for_user(
    db: &brenn_lib::db::Db,
    user_id: i64,
    user_agent: &str,
) -> i64 {
    let conn = db.lock().await;
    let resolved =
        brenn_lib::auth::device::resolve_or_create_device(&conn, None, user_id, user_agent);
    resolved.id
}

/// Build an `ApprovalRequest` for a `PostToolUse` of the given tool.
///
/// `tool_response` uses the canonical object form
/// `{"content": [{"type": "text", "text": "__NOOP__"}]}` so intercept modules
/// that call `is_noop_tool_response` / `warn_if_unexpected_tool_response` see a
/// well-formed response and don't emit spurious warnings.
///
/// Shared by `device.rs`, `export_usage.rs`, `messaging_intercept`,
/// `pwa_push_intercept`, `automation_intercept`, and `mqtt_intercept` test modules.
pub(crate) fn post_tool_use_req(tool_name: &str, tool_input: serde_json::Value) -> ApprovalRequest {
    let (tx, _rx) = oneshot::channel();
    ApprovalRequest {
        request_id: "req1".into(),
        kind: ApprovalKind::PostToolUse {
            callback_id: "cb1".into(),
            tool_name: tool_name.to_string(),
            tool_input,
            tool_use_id: "use1".to_string(),
            tool_response: serde_json::json!({"content": [{"type": "text", "text": "__NOOP__"}]}),
        },
        response_tx: tx,
    }
}

/// Build an `ApprovalRequest` for a `PreToolUse` of the given tool.
///
/// Fixture IDs: `req1` / `cb1` / `use1`. `tool_input` is `{}`.
///
/// Shared by `mqtt_intercept`, `automation_intercept`, and `pwa_push_intercept`
/// test modules.
pub(crate) fn pre_tool_use_req(tool_name: &str) -> ApprovalRequest {
    let (tx, _rx) = oneshot::channel();
    ApprovalRequest {
        request_id: "req1".into(),
        kind: ApprovalKind::PreToolUse {
            callback_id: "cb1".into(),
            tool_name: tool_name.to_string(),
            tool_input: serde_json::json!({}),
            tool_use_id: "use1".to_string(),
        },
        response_tx: tx,
    }
}

/// Panics if `msg` is not `CcOutgoing::User` or contains no `Text` block.
pub(in crate::active_bridge) fn user_text(envelope: &brenn_cc::session::OutgoingEnvelope) -> &str {
    match &envelope.msg {
        brenn_cc::protocol::CcOutgoing::User { message } => message
            .content
            .iter()
            .find_map(|b| match b {
                brenn_cc::protocol::outgoing::UserContentBlock::Text { text } => {
                    Some(text.as_str())
                }
                _ => None,
            })
            .expect("CcOutgoing::User must contain a Text block"),
        other => panic!("expected CcOutgoing::User, got: {other:?}"),
    }
}

/// Build an RW `ResolvedMount` with both a host and container path.
/// Used in containerized-bridge tests.
pub(in crate::active_bridge) fn mk_rw_mount_with_container(
    host_path: std::path::PathBuf,
    container_path: std::path::PathBuf,
) -> brenn_lib::config::ResolvedMount {
    brenn_lib::config::ResolvedMount {
        slug: "test-repo".to_string(),
        host_path,
        container_path: Some(container_path),
        access: brenn_lib::config::AccessLevel::ReadWrite,
        auto_pull: false,
        is_working_dir: false,
        primary: false,
    }
}

/// Variant of `test_bridge_with_cwd_and_mounts` that sets a custom `PathMapper`.
///
/// Delegates to `test_bridge_with_config` and records `cwd` via a post-construction
/// `conversation::set_init_metadata` call.
pub(in crate::active_bridge) async fn test_bridge_with_cwd_and_mounts_and_mapper(
    cwd: &str,
    mounts: Vec<brenn_lib::config::ResolvedMount>,
    path_mapper: PathMapper,
) -> (
    Arc<ActiveBridge>,
    mpsc::Sender<SessionEvent>,
    broadcast::Receiver<WsServerMessage>,
) {
    let (alert_dispatcher, _handle) = noop_alert_dispatcher();
    let (bridge, event_tx, broadcast_rx, _active_bridges) = test_bridge_with_config(
        TestBridgeConfig {
            mounts,
            path_mapper,
            ..Default::default()
        },
        alert_dispatcher,
    )
    .await;

    {
        let conn = bridge.db.lock().await;
        conversation::set_init_metadata(&conn, bridge.conversation_id, "sonnet", cwd);
    }

    (bridge, event_tx, broadcast_rx)
}

/// Set `bridge.context_usage` to a synthetic fill at `pct` percent
/// (`pct * 2000` tokens against a 200_000-token window).
///
/// Shared by the trigger, compaction, and watchdog test modules — the single
/// source of truth for the `pct → ContextUsage` derivation.
pub(in crate::active_bridge) fn set_context_usage(bridge: &ActiveBridge, pct: u8) {
    *bridge.context_usage.lock().expect("context_usage lock") = Some(ContextUsage {
        current_tokens: pct as u64 * 2000,
        max_tokens: 200_000,
        usage_pct: pct,
        checked_at: Instant::now(),
    });
}

/// Set `bridge.compaction` to `WaitingForIdle` with a long-running timer.
///
/// Shared by tests that need to assert on the WaitingForIdle cancellation postcondition
/// (`send_outgoing_cancels_waiting_for_idle`, `send_message_cancels_waiting_for_idle`,
/// `compaction_pre_tool_use_allowed_during_waiting_for_idle`, and any future additions).
/// Spawns a 9999-second sleep as the idle timer so the test runs without the timer firing.
pub(in crate::active_bridge) async fn set_waiting_for_idle(bridge: &ActiveBridge) {
    let mut state = bridge.compaction.lock().await;
    state.phase = CompactionPhase::WaitingForIdle;
    // Simulate a timer by spawning a long sleep.
    state.idle_timer = Some(tokio::spawn(async {
        tokio::time::sleep(Duration::from_secs(9999)).await;
    }));
}
