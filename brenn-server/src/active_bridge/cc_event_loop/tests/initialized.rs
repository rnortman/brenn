//! Initialized event-family tests: `SessionEvent::Initialized` handling
//! (`handle_initialized` — conversation metadata persistence, runtime tool
//! validation, permission-mode broadcast + alert dedup, CC version floor,
//! and seed-on-cache-miss behavior). Peeled out of `tests/mod.rs` per
//! design §2.4.

use super::super::super::ActiveBridges;
use super::super::super::test_support::{
    await_fence, drain_broadcast, drop_and_drain_alerts, event_fence, test_bridge,
    test_bridge_with_dispatcher,
};
use super::super::*;

use brenn_cc::session::SessionInfo;
use brenn_lib::obs::alerting::{
    AlertDispatcher, CountingAlerter, RateLimiter, make_capturing_alerter,
};
use brenn_lib::ws_types::PermissionModeValue;

use crate::active_bridge::test_fixtures::TestBridgeConfig;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::sync::broadcast;

#[tokio::test]
async fn initialized_updates_conversation_metadata() {
    let (bridge, event_tx, mut broadcast_rx, _ab) = test_bridge().await;

    let info = SessionInfo {
        session_id: "cc-sess-123".into(),
        tools: vec!["Read".into(), "Write".into()],
        model: "claude-sonnet-4-20250514".into(),
        cwd: "/home/user/project".into(),
        claude_code_version: Some("2.1.123".into()),
        mcp_servers: Vec::new(),
        permission_mode: Some(PermissionModeValue::Auto),
    };
    let fence = event_fence(&bridge);
    event_tx
        .send(SessionEvent::Initialized(info))
        .await
        .unwrap();

    await_fence(fence).await;

    // Initialized is informational except for the PermissionMode
    // broadcast (one per CC spawn, carrying the mode CC reported).
    let msgs = drain_broadcast(&mut broadcast_rx);
    assert_eq!(msgs.len(), 1, "expected only PermissionMode, got {msgs:?}");
    match &msgs[0] {
        WsServerMessage::PermissionMode { mode } => {
            assert_eq!(*mode, Some(PermissionModeValue::Auto))
        }
        other => panic!("expected PermissionMode, got {other:?}"),
    }

    let conv = {
        let conn = bridge.db.lock().await;
        conversation::get_conversation(&conn, bridge.conversation_id)
    };
    assert_eq!(conv.cc_session_id.as_deref(), Some("cc-sess-123"));
    assert_eq!(conv.model.as_deref(), Some("claude-sonnet-4-20250514"));
    assert_eq!(conv.cwd.as_deref(), Some("/home/user/project"));
}

// -----------------------------------------------------------------------
// Runtime tool validation (handle_initialized)
// -----------------------------------------------------------------------

#[tokio::test]
async fn initialized_with_unknown_tools_persists_metadata() {
    let (bridge, event_tx, _broadcast_rx, _ab) = test_bridge().await;

    // Send Initialized with a tool not in CC_KNOWN_TOOLS.
    let info = SessionInfo {
        session_id: "sess-unknown-tools".into(),
        tools: vec![
            "Read".into(),
            "BrandNewTool".into(),            // unknown
            "mcp__brenn__DisplayFile".into(), // mcp__ prefix — should be ignored
        ],
        model: "sonnet".into(),
        cwd: "/tmp".into(),
        claude_code_version: Some("2.1.123".into()),
        mcp_servers: Vec::new(),
        permission_mode: Some(PermissionModeValue::Auto),
    };
    let fence = event_fence(&bridge);
    event_tx
        .send(SessionEvent::Initialized(info))
        .await
        .unwrap();

    await_fence(fence).await;
    let conv = {
        let conn = bridge.db.lock().await;
        conversation::get_conversation(&conn, bridge.conversation_id)
    };
    assert_eq!(conv.cc_session_id.as_deref(), Some("sess-unknown-tools"));
}

#[tokio::test]
async fn initialized_with_all_known_tools_fires_no_alert() {
    // Regression guarding the fix for the #1 alert storm: when the CC
    // 2.1.112 init tools list arrives, every tool must be recognized
    // by CC_KNOWN_TOOLS and NO "Unknown CC tools detected" alert
    // should fire. If someone drops PushNotification or ScheduleWakeup
    // from the list, this test fails instead of the user getting
    // paged every wake.
    use std::sync::atomic::AtomicU32;
    let alert_count = Arc::new(AtomicU32::new(0));
    let (dispatcher, _h) = AlertDispatcher::new(
        CountingAlerter(alert_count.clone()),
        RateLimiter::new(10, 60),
    );
    let (bridge, event_tx, _broadcast_rx, _ab) = test_bridge_with_dispatcher(dispatcher).await;

    // Full CC 2.1.112 init tools array (from staging transcript
    // 2026-04-16). Brenn must recognize every entry here.
    let info = SessionInfo {
        session_id: "sess-known-tools".into(),
        tools: vec![
            "Task".into(),
            "AskUserQuestion".into(),
            "Bash".into(),
            "CronCreate".into(),
            "CronDelete".into(),
            "CronList".into(),
            "Edit".into(),
            "EnterPlanMode".into(),
            "EnterWorktree".into(),
            "ExitPlanMode".into(),
            "ExitWorktree".into(),
            "Glob".into(),
            "Grep".into(),
            "Monitor".into(),
            "NotebookEdit".into(),
            "PushNotification".into(),
            "Read".into(),
            "RemoteTrigger".into(),
            "ScheduleWakeup".into(),
            "Skill".into(),
            "TaskOutput".into(),
            "TaskStop".into(),
            "TodoWrite".into(),
            "ToolSearch".into(),
            "WebFetch".into(),
            "WebSearch".into(),
            "Write".into(),
            // mcp__* are filtered before the check, include a couple
            // to exercise that filter.
            "mcp__brenn__DisplayFile".into(),
            "mcp__graf__graf_lint".into(),
        ],
        model: "sonnet".into(),
        cwd: "/tmp".into(),
        claude_code_version: Some("2.1.123".into()),
        mcp_servers: Vec::new(),
        permission_mode: Some(PermissionModeValue::Auto),
    };
    let fence = event_fence(&bridge);
    event_tx
        .send(SessionEvent::Initialized(info))
        .await
        .unwrap();

    await_fence(fence).await;
    assert_eq!(
        alert_count.load(Ordering::SeqCst),
        0,
        "every CC 2.1.112 built-in must be in CC_KNOWN_TOOLS — no alert should fire"
    );
}

#[tokio::test]
async fn initialized_with_auto_permission_mode_fires_no_alert() {
    let (dispatcher, captured, handle) = make_capturing_alerter();
    let (bridge, event_tx, mut broadcast_rx, _ab) = test_bridge_with_dispatcher(dispatcher).await;

    let info = SessionInfo {
        session_id: "sess-perm-auto".into(),
        tools: vec!["Read".into()],
        model: "sonnet".into(),
        cwd: "/tmp".into(),
        claude_code_version: Some("2.1.123".into()),
        mcp_servers: Vec::new(),
        permission_mode: Some(PermissionModeValue::Auto),
    };
    event_tx
        .send(SessionEvent::Initialized(info))
        .await
        .unwrap();

    drop_and_drain_alerts(event_tx, bridge, handle).await;
    let msgs = drain_broadcast(&mut broadcast_rx);
    let perm_msgs: Vec<_> = msgs
        .iter()
        .filter_map(|m| match m {
            WsServerMessage::PermissionMode { mode } => Some(mode.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(perm_msgs.len(), 1, "expected exactly one PermissionMode");
    assert_eq!(perm_msgs[0], Some(PermissionModeValue::Auto));

    let captured = captured.lock().unwrap();
    let perm_alerts: Vec<_> = captured
        .iter()
        .filter(|(title, _)| title.starts_with("CC permission_mode"))
        .collect();
    assert!(
        perm_alerts.is_empty(),
        "no permission_mode alert should fire on auto, got {perm_alerts:?}"
    );
}

#[tokio::test]
async fn initialized_with_mismatched_permission_mode_alerts() {
    let (dispatcher, captured, handle) = make_capturing_alerter();
    let (bridge, event_tx, mut broadcast_rx, _ab) = test_bridge_with_dispatcher(dispatcher).await;

    let info = SessionInfo {
        session_id: "sess-perm-default".into(),
        tools: vec!["Read".into()],
        model: "sonnet".into(),
        cwd: "/tmp".into(),
        claude_code_version: Some("2.1.123".into()),
        mcp_servers: Vec::new(),
        permission_mode: Some(PermissionModeValue::Other("default".into())),
    };
    event_tx
        .send(SessionEvent::Initialized(info))
        .await
        .unwrap();

    drop_and_drain_alerts(event_tx, bridge, handle).await;
    let msgs = drain_broadcast(&mut broadcast_rx);
    let perm_msgs: Vec<_> = msgs
        .iter()
        .filter_map(|m| match m {
            WsServerMessage::PermissionMode { mode } => Some(mode.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(perm_msgs.len(), 1);
    assert_eq!(
        perm_msgs[0],
        Some(PermissionModeValue::Other("default".into()))
    );

    let captured = captured.lock().unwrap();
    let mismatch: Vec<_> = captured
        .iter()
        .filter(|(title, _)| title == "CC permission_mode mismatch")
        .collect();
    assert_eq!(
        mismatch.len(),
        1,
        "expected exactly one mismatch alert, got {captured:?}"
    );
}

#[tokio::test]
async fn initialized_with_missing_permission_mode_alerts() {
    let (dispatcher, captured, handle) = make_capturing_alerter();
    let (bridge, event_tx, mut broadcast_rx, _ab) = test_bridge_with_dispatcher(dispatcher).await;

    let info = SessionInfo {
        session_id: "sess-perm-missing".into(),
        tools: vec!["Read".into()],
        model: "sonnet".into(),
        cwd: "/tmp".into(),
        claude_code_version: Some("2.1.123".into()),
        mcp_servers: Vec::new(),
        permission_mode: None,
    };
    event_tx
        .send(SessionEvent::Initialized(info))
        .await
        .unwrap();

    drop_and_drain_alerts(event_tx, bridge, handle).await;
    let msgs = drain_broadcast(&mut broadcast_rx);
    let perm_msgs: Vec<_> = msgs
        .iter()
        .filter_map(|m| match m {
            WsServerMessage::PermissionMode { mode } => Some(mode.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(perm_msgs.len(), 1);
    assert!(perm_msgs[0].is_none());

    let captured = captured.lock().unwrap();
    let missing: Vec<_> = captured
        .iter()
        .filter(|(title, _)| title == "CC permission_mode missing from init")
        .collect();
    assert_eq!(
        missing.len(),
        1,
        "expected exactly one missing alert, got {captured:?}"
    );
}

#[tokio::test]
async fn initialized_with_cased_auto_permission_mode_alerts() {
    // Edge case 4 in the design: "Auto"/"AUTO" must be treated as a
    // mismatch — CC is a strict producer, no case-folding. Pins the
    // tripwire against a future refactor that adds `.to_lowercase()`.
    let (dispatcher, captured, handle) = make_capturing_alerter();
    let (bridge, event_tx, mut broadcast_rx, _ab) = test_bridge_with_dispatcher(dispatcher).await;

    let info = SessionInfo {
        session_id: "sess-perm-cased".into(),
        tools: vec!["Read".into()],
        model: "sonnet".into(),
        cwd: "/tmp".into(),
        claude_code_version: Some("2.1.123".into()),
        mcp_servers: Vec::new(),
        permission_mode: Some(PermissionModeValue::Other("Auto".into())),
    };
    event_tx
        .send(SessionEvent::Initialized(info))
        .await
        .unwrap();

    drop_and_drain_alerts(event_tx, bridge, handle).await;
    let msgs = drain_broadcast(&mut broadcast_rx);
    let perm_msgs: Vec<_> = msgs
        .iter()
        .filter_map(|m| match m {
            WsServerMessage::PermissionMode { mode } => Some(mode.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(perm_msgs.len(), 1);
    assert_eq!(
        perm_msgs[0],
        Some(PermissionModeValue::Other("Auto".into()))
    );

    let captured = captured.lock().unwrap();
    let mismatch: Vec<_> = captured
        .iter()
        .filter(|(title, _)| title == "CC permission_mode mismatch")
        .collect();
    assert_eq!(
        mismatch.len(),
        1,
        "expected one mismatch alert for cased 'Auto', got {captured:?}"
    );
}

#[tokio::test]
async fn repeated_missing_permission_mode_dedups_alert_but_rebroadcasts() {
    // Edge case 3 in the design: the once-per-process alert dedup must
    // engage at the handle_initialized call-site, and the broadcast must
    // fire on every init. A regression swapping the dedup key (e.g. to
    // `session_id`) or folding the broadcast into the match arms would
    // silently break this.
    let (dispatcher, captured, handle) = make_capturing_alerter();
    let (bridge, event_tx, mut broadcast_rx, _ab) = test_bridge_with_dispatcher(dispatcher).await;

    for session in ["sess-a", "sess-b"] {
        let info = SessionInfo {
            session_id: session.into(),
            tools: vec!["Read".into()],
            model: "sonnet".into(),
            cwd: "/tmp".into(),
            claude_code_version: Some("2.1.123".into()),
            mcp_servers: Vec::new(),
            permission_mode: None,
        };
        event_tx
            .send(SessionEvent::Initialized(info))
            .await
            .unwrap();
    }

    // Two Initialized events were sent above; the drain covers both iterations.
    drop_and_drain_alerts(event_tx, bridge, handle).await;
    let msgs = drain_broadcast(&mut broadcast_rx);
    let perm_msgs: Vec<_> = msgs
        .iter()
        .filter_map(|m| match m {
            WsServerMessage::PermissionMode { mode } => Some(mode.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        perm_msgs.len(),
        2,
        "broadcast must fire on every init, got {perm_msgs:?}"
    );
    assert!(perm_msgs.iter().all(|m| m.is_none()));

    let captured = captured.lock().unwrap();
    let missing: Vec<_> = captured
        .iter()
        .filter(|(title, _)| title == "CC permission_mode missing from init")
        .collect();
    assert_eq!(
        missing.len(),
        1,
        "alert must dedup across repeated inits, got {captured:?}"
    );
}

#[tokio::test]
async fn repeated_mismatched_permission_mode_dedups_same_value() {
    // Edge case 3: same `other` value across two inits pages once. A
    // different `other` value would page independently (different dedup
    // key); this test pins the same-value dedup path specifically.
    let (dispatcher, captured, handle) = make_capturing_alerter();
    let (bridge, event_tx, mut broadcast_rx, _ab) = test_bridge_with_dispatcher(dispatcher).await;

    for session in ["sess-c", "sess-d"] {
        let info = SessionInfo {
            session_id: session.into(),
            tools: vec!["Read".into()],
            model: "sonnet".into(),
            cwd: "/tmp".into(),
            claude_code_version: Some("2.1.123".into()),
            mcp_servers: Vec::new(),
            permission_mode: Some(PermissionModeValue::Other("default".into())),
        };
        event_tx
            .send(SessionEvent::Initialized(info))
            .await
            .unwrap();
    }

    drop_and_drain_alerts(event_tx, bridge, handle).await;
    let msgs = drain_broadcast(&mut broadcast_rx);
    let perm_msgs: Vec<_> = msgs
        .iter()
        .filter_map(|m| match m {
            WsServerMessage::PermissionMode { mode } => Some(mode.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(perm_msgs.len(), 2);

    let captured = captured.lock().unwrap();
    let mismatch: Vec<_> = captured
        .iter()
        .filter(|(title, _)| title == "CC permission_mode mismatch")
        .collect();
    assert_eq!(
        mismatch.len(),
        1,
        "same mismatched value must dedup across repeated inits, got {captured:?}"
    );
}

// -----------------------------------------------------------------------
// CC version floor tests
// -----------------------------------------------------------------------

/// CC reporting a version below the minimum must panic handle_initialized.
#[tokio::test]
#[should_panic(expected = "Brenn requires >= 2.1.123")]
async fn version_floor_panics_on_old_cc() {
    let db = brenn_lib::db::init_db_memory();
    let (tx, _rx) = broadcast::channel(64);
    let active_bridges = ActiveBridges::new();
    let (uid, conv_id) = {
        let conn = db.lock().await;
        let uid = brenn_lib::auth::user::create_user(&conn, "vfloor1", "$argon2id$fake");
        let cid = conversation::create_conversation(&conn, uid, "test", false);
        (uid, cid)
    };
    let bridge = ActiveBridge::inject_for_test_full(
        uid,
        conv_id,
        "test",
        db,
        tx,
        brenn_lib::obs::alerting::noop_alert_dispatcher().0,
        TestBridgeConfig {
            active_bridges: Some(active_bridges),
            singleton: true,
            ..Default::default()
        },
    );
    let (ad, _h) = brenn_lib::obs::alerting::noop_alert_dispatcher();
    let info = brenn_cc::session::SessionInfo {
        session_id: "s".into(),
        tools: vec![],
        model: "claude-sonnet-4-6".into(),
        cwd: "/".into(),
        claude_code_version: Some("2.1.111".into()), // below 2.1.123
        mcp_servers: vec![],
        permission_mode: Some(PermissionModeValue::Auto),
    };
    handle_initialized(&bridge, &info, &ad).await;
}

/// CC omitting `claude_code_version` must panic handle_initialized.
#[tokio::test]
#[should_panic(expected = "did not include claude_code_version")]
async fn version_floor_panics_on_missing_version() {
    let db = brenn_lib::db::init_db_memory();
    let (tx, _rx) = broadcast::channel(64);
    let active_bridges = ActiveBridges::new();
    let (uid, conv_id) = {
        let conn = db.lock().await;
        let uid = brenn_lib::auth::user::create_user(&conn, "vfloor2", "$argon2id$fake");
        let cid = conversation::create_conversation(&conn, uid, "test", false);
        (uid, cid)
    };
    let bridge = ActiveBridge::inject_for_test_full(
        uid,
        conv_id,
        "test",
        db,
        tx,
        brenn_lib::obs::alerting::noop_alert_dispatcher().0,
        TestBridgeConfig {
            active_bridges: Some(active_bridges),
            singleton: true,
            ..Default::default()
        },
    );
    let (ad, _h) = brenn_lib::obs::alerting::noop_alert_dispatcher();
    let info = brenn_cc::session::SessionInfo {
        session_id: "s".into(),
        tools: vec![],
        model: "claude-sonnet-4-6".into(),
        cwd: "/".into(),
        claude_code_version: None, // missing
        mcp_servers: vec![],
        permission_mode: Some(PermissionModeValue::Auto),
    };
    handle_initialized(&bridge, &info, &ad).await;
}

/// On a cache miss, `handle_initialized` must leave `seed_max_tokens` as
/// `None` — no hardcoded guess. Context-usage broadcasts are deferred
/// until the `result` frame provides the authoritative contextWindow value.
#[tokio::test]
async fn init_seed_none_on_cache_miss() {
    let db = brenn_lib::db::init_db_memory();
    let (tx, _rx) = broadcast::channel(64);
    let active_bridges = ActiveBridges::new();
    let (uid, conv_id) = {
        let conn = db.lock().await;
        let uid = brenn_lib::auth::user::create_user(&conn, "seed-miss", "$argon2id$fake");
        let cid = conversation::create_conversation(&conn, uid, "test", false);
        (uid, cid)
    };
    let bridge = ActiveBridge::inject_for_test_full(
        uid,
        conv_id,
        "test",
        db,
        tx,
        brenn_lib::obs::alerting::noop_alert_dispatcher().0,
        TestBridgeConfig {
            active_bridges: Some(active_bridges),
            singleton: true,
            ..Default::default()
        },
    );
    let (ad, _h) = brenn_lib::obs::alerting::noop_alert_dispatcher();
    let info = brenn_cc::session::SessionInfo {
        session_id: "s-miss".into(),
        tools: vec![],
        // Use a slug that is definitely not in the empty in-memory cache.
        model: "claude-sonnet-4-6-never-seen".into(),
        cwd: "/".into(),
        claude_code_version: Some("2.1.123".into()),
        mcp_servers: vec![],
        permission_mode: Some(PermissionModeValue::Auto),
    };
    handle_initialized(&bridge, &info, &ad).await;

    assert_eq!(
        *bridge.seed_max_tokens.lock().expect("lock"),
        None,
        "seed_max_tokens must be None on cache miss — no guessed default"
    );
}
