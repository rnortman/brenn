//! Status/compaction-lifecycle tests: `handle_status_change` (StatusChange)
//! and `handle_compact_boundary` (CompactBoundary), plus the TurnCompleted
//! Idle broadcast. Peeled out of `tests/mod.rs` per design §2.4.

use super::super::super::test_support::{
    await_fence, drain_broadcast, event_fence, recv_broadcast, test_bridge, test_bridge_singleton,
};
use super::super::*;
use brenn_lib::ws_types::CcState;

use brenn_cc::protocol::incoming::ResultMessage;

#[tokio::test]
async fn completed_sends_idle_status() {
    let (_bridge, event_tx, mut broadcast_rx, _ab) = test_bridge().await;

    let result = ResultMessage {
        subtype: Some("success".into()),
        duration_ms: Some(5000),
        duration_api_ms: Some(4000),
        is_error: Some(false),
        num_turns: Some(2),
        session_id: Some("sess-1".into()),
        total_cost_usd: Some(0.03),
        usage: None,
        result: None,
        stop_reason: Some("end_turn".into()),
        model_usage: None,
        origin: None,
        extra: serde_json::Value::Object(Default::default()),
    };
    event_tx
        .send(SessionEvent::TurnCompleted(result))
        .await
        .unwrap();

    // CostUsage is broadcast before Status::Idle.
    let msg1 = recv_broadcast(&mut broadcast_rx).await;
    assert!(
        matches!(msg1, WsServerMessage::CostUsage { .. }),
        "expected CostUsage first, got {msg1:?}"
    );
    let msg2 = recv_broadcast(&mut broadcast_rx).await;
    match &msg2 {
        WsServerMessage::Status { state } => assert_eq!(*state, CcState::Idle),
        other => panic!("expected Status Idle, got {other:?}"),
    }
}

#[tokio::test]
async fn status_change_compacting_broadcasts() {
    let (_bridge, event_tx, mut broadcast_rx, _ab) = test_bridge().await;

    event_tx
        .send(SessionEvent::StatusChange {
            status: Some("compacting".into()),
            compact_result: None,
        })
        .await
        .unwrap();

    let msg = recv_broadcast(&mut broadcast_rx).await;
    match &msg {
        WsServerMessage::Status { state } => assert_eq!(*state, CcState::Compacting),
        other => panic!("expected Status Compacting, got {other:?}"),
    }
}

#[tokio::test]
async fn status_change_null_without_compact_result_does_not_broadcast() {
    let (_bridge, event_tx, mut broadcast_rx, _ab) = test_bridge().await;

    // Status cleared without compact_result — non-compaction clear (e.g. after
    // `requesting`). Must NOT broadcast; handle_turn_completed handles Idle.
    event_tx
        .send(SessionEvent::StatusChange {
            status: None,
            compact_result: None,
        })
        .await
        .unwrap();

    let result = tokio::time::timeout(
        tokio::time::Duration::from_millis(100),
        recv_broadcast(&mut broadcast_rx),
    )
    .await;
    assert!(
        result.is_err(),
        "should not broadcast on status=null without compact_result"
    );
}

#[tokio::test]
async fn status_change_null_with_compact_result_success_broadcasts_idle() {
    let (bridge, event_tx, mut broadcast_rx, _ab) = test_bridge().await;

    // Put the compaction state machine into Compacting so we can verify reset.
    {
        let mut compaction = bridge.compaction.lock().await;
        compaction.phase = super::super::CompactionPhase::Compacting;
        compaction.compact_boundary_seen = true;
    }

    let fence = event_fence(&bridge);
    event_tx
        .send(SessionEvent::StatusChange {
            status: None,
            compact_result: Some("success".into()),
        })
        .await
        .unwrap();

    let msg = recv_broadcast(&mut broadcast_rx).await;
    match &msg {
        WsServerMessage::Status { state } => assert_eq!(*state, CcState::Idle),
        other => panic!("expected Status Idle, got {other:?}"),
    }

    // State machine must be reset so TurnCompleted proceeds normally.
    await_fence(fence).await;
    let compaction = bridge.compaction.lock().await;
    assert!(
        matches!(compaction.phase, super::super::CompactionPhase::Normal),
        "phase should be reset to Normal, got {:?}",
        compaction.phase
    );
    assert!(
        !compaction.compact_boundary_seen,
        "compact_boundary_seen should be reset to false"
    );
}

#[tokio::test]
async fn status_change_null_with_compact_result_failure_broadcasts_idle_and_resets_compaction() {
    let (bridge, event_tx, mut broadcast_rx, _ab) = test_bridge().await;

    // Put the compaction state machine into Compacting with boundary seen.
    {
        let mut compaction = bridge.compaction.lock().await;
        compaction.phase = super::super::CompactionPhase::Compacting;
        compaction.compact_boundary_seen = true;
    }

    let fence = event_fence(&bridge);
    event_tx
        .send(SessionEvent::StatusChange {
            status: None,
            compact_result: Some("failure".into()),
        })
        .await
        .unwrap();

    // Expect Status::Idle broadcast.
    let msg1 = recv_broadcast(&mut broadcast_rx).await;
    match &msg1 {
        WsServerMessage::Status { state } => assert_eq!(*state, CcState::Idle),
        other => panic!("expected Status Idle, got {other:?}"),
    }

    // Expect SystemMessageBroadcast with CompactionFailed category.
    let msg2 = recv_broadcast(&mut broadcast_rx).await;
    match &msg2 {
        WsServerMessage::SystemMessageBroadcast { category, .. } => {
            assert_eq!(
                *category,
                brenn_lib::ws_types::SystemMessageCategory::CompactionFailed,
                "expected CompactionFailed category"
            );
        }
        other => panic!("expected SystemMessageBroadcast, got {other:?}"),
    }

    // State machine must be reset.
    await_fence(fence).await;
    let compaction = bridge.compaction.lock().await;
    assert!(
        matches!(compaction.phase, super::super::CompactionPhase::Normal),
        "phase should be reset to Normal after failure"
    );
    assert!(
        !compaction.compact_boundary_seen,
        "compact_boundary_seen should be reset after failure"
    );
}

#[tokio::test]
async fn status_change_null_with_unknown_compact_result_broadcasts_idle() {
    let (bridge, event_tx, mut broadcast_rx, _ab) = test_bridge().await;

    // Must be in Compacting phase — phase check gates the handler.
    {
        let mut compaction = bridge.compaction.lock().await;
        compaction.phase = super::super::CompactionPhase::Compacting;
        compaction.compact_boundary_seen = true;
    }

    let fence = event_fence(&bridge);
    event_tx
        .send(SessionEvent::StatusChange {
            status: None,
            compact_result: Some("new_value".into()),
        })
        .await
        .unwrap();

    // Unknown compact_result treated defensively as end-of-compaction.
    let msg = recv_broadcast(&mut broadcast_rx).await;
    match &msg {
        WsServerMessage::Status { state } => assert_eq!(*state, CcState::Idle),
        other => panic!("expected Status Idle, got {other:?}"),
    }

    // State machine reset.
    await_fence(fence).await;
    let compaction = bridge.compaction.lock().await;
    assert!(
        matches!(compaction.phase, super::super::CompactionPhase::Normal),
        "phase should be Normal after unknown compact_result"
    );
    assert!(
        !compaction.compact_boundary_seen,
        "compact_boundary_seen should be reset after unknown compact_result"
    );
}

/// compact_result arriving on status:null while phase is Normal (not Compacting)
/// must NOT mutate state machine fields — it logs + alerts but does not reset phase
/// or broadcast Idle (protocol drift guard, correctness-3).
#[tokio::test]
async fn status_change_compact_result_outside_compacting_does_not_mutate() {
    let (bridge, event_tx, mut broadcast_rx, _ab) = test_bridge().await;

    // Leave phase at Normal (default from test_bridge).
    {
        let compaction = bridge.compaction.lock().await;
        assert!(
            matches!(compaction.phase, super::super::CompactionPhase::Normal),
            "precondition: phase is Normal"
        );
    }

    let fence = event_fence(&bridge);
    event_tx
        .send(SessionEvent::StatusChange {
            status: None,
            compact_result: Some("success".into()),
        })
        .await
        .unwrap();

    await_fence(fence).await;

    // Must NOT broadcast any message (no Idle, no state change).
    let extra = drain_broadcast(&mut broadcast_rx);
    assert!(
        extra.is_empty(),
        "must not broadcast when compact_result arrives outside Compacting phase"
    );

    // Phase must remain Normal; compact_boundary_seen must remain false.
    let compaction = bridge.compaction.lock().await;
    assert!(
        matches!(compaction.phase, super::super::CompactionPhase::Normal),
        "phase must not be mutated by out-of-phase compact_result"
    );
    assert!(
        !compaction.compact_boundary_seen,
        "compact_boundary_seen must not be mutated by out-of-phase compact_result"
    );
}

#[tokio::test]
async fn status_change_unknown_does_not_broadcast() {
    let (_bridge, event_tx, mut broadcast_rx, _ab) = test_bridge().await;

    // An unknown status (e.g., from a CC upgrade) should NOT broadcast
    // a state change — it logs a warning and fires an alert instead.
    event_tx
        .send(SessionEvent::StatusChange {
            status: Some("new_unknown_status".into()),
            compact_result: None,
        })
        .await
        .unwrap();

    let result = tokio::time::timeout(
        tokio::time::Duration::from_millis(100),
        recv_broadcast(&mut broadcast_rx),
    )
    .await;
    assert!(result.is_err(), "should not broadcast on unknown status");
}

#[tokio::test]
async fn status_change_requesting_does_not_broadcast() {
    let (_bridge, event_tx, mut broadcast_rx, _ab) = test_bridge().await;

    // CC 2.1.112+ emits {status: "requesting"} before every API call.
    // It's a recognized passthrough; Brenn already broadcasts Thinking
    // via set_cc_busy, so this must NOT trigger another broadcast and
    // must NOT go down the "unknown status" alert path.
    event_tx
        .send(SessionEvent::StatusChange {
            status: Some("requesting".into()),
            compact_result: None,
        })
        .await
        .unwrap();

    let result = tokio::time::timeout(
        tokio::time::Duration::from_millis(100),
        recv_broadcast(&mut broadcast_rx),
    )
    .await;
    assert!(
        result.is_err(),
        "requesting is recognized-passthrough; no broadcast"
    );
}

#[tokio::test]
async fn compact_boundary_persists_marker() {
    let (bridge, event_tx, _broadcast_rx, _ab) = test_bridge().await;

    let metadata = brenn_cc::protocol::incoming::CompactMetadata {
        trigger: Some("manual".into()),
        pre_tokens: Some(16807),
        extra: serde_json::Value::Object(Default::default()),
    };

    let fence = event_fence(&bridge);
    event_tx
        .send(SessionEvent::CompactBoundary {
            metadata: Some(metadata),
        })
        .await
        .unwrap();

    await_fence(fence).await;

    // Verify the compact boundary was persisted as a message.
    let conn = bridge.db.lock().await;
    let messages = conversation::get_messages(&conn, bridge.conversation_id);
    let compact_msg = messages
        .iter()
        .find(|m| m.msg_type == "compact_boundary")
        .expect("compact_boundary message should be persisted");
    let payload: serde_json::Value =
        serde_json::from_str(&compact_msg.payload).expect("payload should be JSON");
    assert_eq!(payload["type"], "compact_boundary");
    assert_eq!(payload["metadata"]["trigger"], "manual");
    assert_eq!(payload["metadata"]["pre_tokens"], 16807);
}

/// `CompactBoundary` event while phase is `Compacting` sets
/// `compact_boundary_seen = true`.
#[tokio::test]
async fn compact_boundary_sets_flag_during_compacting() {
    let (bridge, event_tx, _broadcast_rx, _ab) = test_bridge_singleton().await;
    bridge.compaction.lock().await.phase = CompactionPhase::Compacting;

    let fence = event_fence(&bridge);
    event_tx
        .send(SessionEvent::CompactBoundary { metadata: None })
        .await
        .unwrap();

    await_fence(fence).await;

    let state = bridge.compaction.lock().await;
    assert!(
        state.compact_boundary_seen,
        "compact_boundary_seen must be true after CompactBoundary during Compacting"
    );
}

/// `CompactBoundary` event while phase is NOT `Compacting` must not set
/// the flag (anomalous — ignored for flag purposes).
#[tokio::test]
async fn compact_boundary_ignored_when_not_compacting() {
    let (bridge, event_tx, _broadcast_rx, _ab) = test_bridge_singleton().await;
    // Phase defaults to Normal.

    let fence = event_fence(&bridge);
    event_tx
        .send(SessionEvent::CompactBoundary { metadata: None })
        .await
        .unwrap();

    await_fence(fence).await;

    let state = bridge.compaction.lock().await;
    assert!(
        !state.compact_boundary_seen,
        "compact_boundary_seen must remain false when CompactBoundary arrives outside Compacting"
    );
}
