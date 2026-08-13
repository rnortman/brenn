//! Drain-family tests: `drain_pending_events` startup queue/messaging drain —
//! ingress event queue draining (no-events noop, no-session park,
//! submit-then-drain roundtrip, singleton/non-singleton at loop start,
//! repo-sync staleness drop + collapse), messaging-pushes integration
//! (combined event+push delivery, send-failure parking, messaging-only
//! delivery), and the AC2/AC3 durability cases (real D1 broken-pipe window,
//! drain-path recovery after session-death flush failure).
//! Peeled out of `tests/mod.rs` per design §2.4; the shared helpers
//! (`enqueue_ingress`, `pending_ingress_count`, `bridge_with_unspawned_event_loop`,
//! `bridge_with_messenger_for_drain`, `seed_pending_push`) and
//! `DRAIN_TEST_CHANNEL_UUID` remain in `tests/mod.rs` as their single home,
//! reached here via `super::`.

use super::super::super::test_support::{
    await_fence, drain_broadcast, event_fence, test_bridge_singleton,
};
use super::super::*;
use super::{
    bridge_with_messenger_for_drain, bridge_with_messenger_for_drain_at_ceiling,
    bridge_with_unspawned_event_loop, enqueue_ingress, pending_ingress_count, seed_pending_push,
    seed_pending_push_from, seed_pending_push_with_impetus,
};

/// Whether the bridge's conversation is still owed anything on a bus channel —
/// its cursor position trails retention.
///
/// The drain's ack point is the advance it makes after a confirmed flush, so a
/// failed send leaves this `true`: at-least-once, with no per-subscriber row
/// involved. Takes no connection guard because it acquires its own.
async fn owed_on_the_bus(bridge: &ActiveBridge) -> bool {
    let messenger = bridge
        .messenger()
        .expect("the drain fixtures wire a messenger");
    !brenn_messaging::testutils::owed_everywhere(
        messenger.as_ref(),
        &brenn_lib::messaging::ParticipantId::for_conversation(bridge.conversation_id),
    )
    .await
    .is_empty()
}

// -----------------------------------------------------------------------
// Event queue drain tests
// -----------------------------------------------------------------------

#[tokio::test]
async fn drain_pending_events_no_events_is_noop() {
    let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge_singleton().await;

    // No events queued — drain should be a no-op (no error, no message).
    drain_pending_events(&bridge).await;

    let conn = bridge.db.lock().await;
    assert_eq!(pending_ingress_count(&conn, bridge.conversation_id), 0);
}

#[tokio::test]
async fn drain_pending_events_with_no_session_leaves_events_pending() {
    let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge_singleton().await;

    // Enqueue events.
    {
        let conn = bridge.db.lock().await;
        enqueue_ingress(
            &conn,
            bridge.conversation_id,
            "cron",
            "Morning briefing",
            r#"{"job":"morning"}"#,
        );
        enqueue_ingress(
            &conn,
            bridge.conversation_id,
            "discord",
            "Message from Bob",
            r#"{"text":"hi"}"#,
        );
    }

    // Test bridge has no real CC session, so send_system_message
    // will fail. Events should remain pending (at-least-once semantics).
    drain_pending_events(&bridge).await;

    let conn = bridge.db.lock().await;
    assert_eq!(
        pending_ingress_count(&conn, bridge.conversation_id),
        2,
        "events should stay pending after failed delivery"
    );
}

/// `submit_ingress` (via the bridge's `Messenger`) inserts a pending push;
/// `drain_pending_events` then delivers (or parks) it. On the test bridge
/// (no real CC session) delivery fails and the row stays pending — but the
/// handoff is exercised: `target_subscriber` matches `bridge.conversation_id`,
/// and no spurious rows for other conversations appear.
#[tokio::test]
async fn submit_ingress_then_drain_roundtrip() {
    // Use bridge_with_unspawned_event_loop (not test_bridge_singleton) because
    // it configures a messenger — test_bridge_singleton does not.
    let (bridge, _event_tx, _event_rx, _broadcast_rx, _alert_dispatcher, _active_bridges) =
        bridge_with_unspawned_event_loop(true).await;

    // submit_ingress via the bridge's messenger — full path including DB insert.
    let messenger = bridge
        .messenger
        .as_ref()
        .expect("test bridge must have a messenger configured");
    messenger
        .submit_ingress(
            bridge.conversation_id,
            "test",
            "cron",
            "roundtrip summary",
            "{}",
            brenn_lib::messaging::Urgency::Normal,
        )
        .await;

    // Confirm the row landed.
    {
        let conn = bridge.db.lock().await;
        assert_eq!(
            pending_ingress_count(&conn, bridge.conversation_id),
            1,
            "submit_ingress must have inserted one pending push"
        );
    }

    // Drain — the test bridge has no real CC session, so the send fails and
    // the push stays pending (at-least-once). The drain must not panic, must
    // not drop the row, and must not produce pushes for wrong subscribers.
    drain_pending_events(&bridge).await;

    let conn = bridge.db.lock().await;
    assert_eq!(
        pending_ingress_count(&conn, bridge.conversation_id),
        1,
        "push must remain pending after failed drain (no session)"
    );
    // No pushes for other conversations.
    let total_pending: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM messaging_pending_pushes pp \
                 JOIN messaging_messages m ON pp.message_id = m.id \
                 WHERE m.envelope_type = 'ingress' AND pp.delivered_at IS NULL",
            [],
            |r| r.get(0),
        )
        .expect("total pending query");
    assert_eq!(
        total_pending, 1,
        "no spurious pushes for other conversations"
    );
}

#[tokio::test]
async fn drain_runs_for_non_singleton_at_event_loop_start() {
    // Regression: the drain used to be singleton-gated. It was ungated
    // in the repo-sync design (docs/designs/repo-sync.md — "Drain ungate
    // to all apps (M7)"); the gate was a scope limiter, not a
    // correctness guard. Ensure non-singleton bridges now attempt the
    // drain at event-loop start. Test bridge has no real session, so
    // the send will fail and the event stays pending — but the send
    // attempt is observable as a SystemMessageBroadcast broadcast.
    let (bridge, _event_tx, event_rx, mut broadcast_rx, alert_dispatcher, _active_bridges) =
        bridge_with_unspawned_event_loop(false).await;

    {
        let conn = bridge.db.lock().await;
        enqueue_ingress(
            &conn,
            bridge.conversation_id,
            "cron",
            "Drain me (non-singleton)",
            "{}",
        );
    }

    let fence = event_fence(&bridge);
    tokio::spawn(cc_event_loop(event_rx, bridge.clone(), alert_dispatcher));

    await_fence(fence).await;

    let msgs = drain_broadcast(&mut broadcast_rx);
    let saw_system_broadcast = msgs
        .iter()
        .any(|m| matches!(m, WsServerMessage::SystemMessageBroadcast { .. }));
    assert!(
        saw_system_broadcast,
        "non-singleton drain should have attempted send (SystemMessageBroadcast), got {msgs:?}"
    );

    let conn = bridge.db.lock().await;
    assert_eq!(
        pending_ingress_count(&conn, bridge.conversation_id),
        1,
        "event stays pending after failed send (at-least-once)"
    );
}

#[tokio::test]
async fn drain_runs_for_singleton_at_event_loop_start() {
    // The test bridge has no real CC session, so the send fails and events
    // stay pending — but the attempt is observable as a SystemMessageBroadcast
    // that fires before the no-session check.
    let (bridge, _event_tx, event_rx, mut broadcast_rx, alert_dispatcher, _active_bridges) =
        bridge_with_unspawned_event_loop(true).await;

    {
        let conn = bridge.db.lock().await;
        enqueue_ingress(&conn, bridge.conversation_id, "cron", "Drain me", "{}");
    }

    let fence = event_fence(&bridge);
    tokio::spawn(cc_event_loop(event_rx, bridge.clone(), alert_dispatcher));

    await_fence(fence).await;

    let msgs = drain_broadcast(&mut broadcast_rx);
    let saw_system_broadcast = msgs
        .iter()
        .any(|m| matches!(m, WsServerMessage::SystemMessageBroadcast { .. }));
    assert!(
        saw_system_broadcast,
        "singleton drain should have attempted send (SystemMessageBroadcast), got {msgs:?}"
    );

    let conn = bridge.db.lock().await;
    assert_eq!(
        pending_ingress_count(&conn, bridge.conversation_id),
        1,
        "event should remain pending after failed drain"
    );
}

#[tokio::test]
async fn drain_drops_stale_repo_sync_events() {
    // Integration: verify the drain-time staleness wiring (design M4).
    // A conversation whose `updated_at` is older than the staleness cap
    // has its `repo_sync:*` rows silently marked delivered *without*
    // inject. Non-repo_sync rows from the same conversation are still
    // attempted.
    let (bridge, _event_tx, event_rx, _broadcast_rx, alert_dispatcher, _active_bridges) =
        bridge_with_unspawned_event_loop(false).await;

    // Force the staleness cap low so we can backdate the conversation
    // by a known amount without waiting.
    brenn_messaging::set_repo_sync_staleness_days(1);

    // Backdate the conversation's updated_at to 10 days ago.
    {
        let conn = bridge.db.lock().await;
        let backdate = (chrono::Utc::now() - chrono::Duration::days(10)).to_rfc3339();
        brenn_db::conversation::set_updated_at_for_test(&conn, bridge.conversation_id, &backdate);

        // Enqueue one repo_sync row and one cron row. Only the
        // repo_sync row should get dropped-by-staleness.
        enqueue_ingress(
            &conn,
            bridge.conversation_id,
            "repo_sync:pulled",
            "stale pulled",
            r#"{"kind":"pulled","slug":"life","oneline":["abc stale"]}"#,
        );
        enqueue_ingress(&conn, bridge.conversation_id, "cron", "fresh cron", "{}");
    }

    let fence = event_fence(&bridge);
    tokio::spawn(cc_event_loop(event_rx, bridge.clone(), alert_dispatcher));
    await_fence(fence).await;

    let conn = bridge.db.lock().await;
    // The repo_sync row is marked delivered at drain time (stale).
    // The cron row is sent — the bridge has no session so the send
    // fails and it stays pending.
    let subscriber_str = format!("conversation:{}", bridge.conversation_id);
    let mut pending_sources_stmt = conn
        .prepare(
            "SELECT m.ingress_source FROM messaging_pending_pushes pp \
                 JOIN messaging_messages m ON pp.message_id = m.id \
                 WHERE pp.target_subscriber = ?1 AND pp.delivered_at IS NULL",
        )
        .unwrap();
    let sources: Vec<String> = pending_sources_stmt
        .query_map(rusqlite::params![subscriber_str], |row| row.get(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    let source_refs: Vec<&str> = sources.iter().map(|s| s.as_str()).collect();
    assert!(
        !source_refs.contains(&"repo_sync:pulled"),
        "stale repo_sync row must be marked delivered at drain; got {source_refs:?}",
    );
    assert!(
        source_refs.contains(&"cron"),
        "non-repo_sync row must stay pending when send fails; got {source_refs:?}",
    );

    // The stale repo_sync row must be mark-delivered (delivered_at IS NOT NULL),
    // not silently deleted — mark-delivered lets the cleanup loop reap it.
    let stale_delivered_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM messaging_pending_pushes pp \
                 JOIN messaging_messages m ON pp.message_id = m.id \
                 WHERE pp.target_subscriber = ?1 \
                   AND m.ingress_source = 'repo_sync:pulled' \
                   AND pp.delivered_at IS NOT NULL",
            rusqlite::params![subscriber_str],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        stale_delivered_count, 1,
        "stale repo_sync push must be marked delivered (not deleted); got {stale_delivered_count}"
    );

    // Reset to default so other tests aren't affected by the low cap.
    brenn_messaging::set_repo_sync_staleness_days(7);
}

#[tokio::test]
async fn drain_collapses_multiple_repo_sync_pulled_events() {
    // Integration: two `repo_sync:pulled` rows for the same slug from
    // two cycles fold into a single synthesized `repo_sync:summary`
    // inject. Per design collapsing rules, we can observe the combined
    // event via the SystemMessageBroadcast rendered_html body.
    let (bridge, _event_tx, event_rx, mut broadcast_rx, alert_dispatcher, _active_bridges) =
        bridge_with_unspawned_event_loop(false).await;

    {
        let conn = bridge.db.lock().await;
        enqueue_ingress(
            &conn,
            bridge.conversation_id,
            "repo_sync:pulled",
            "first",
            r#"{"kind":"pulled","slug":"life","oneline":["aaa first"]}"#,
        );
        enqueue_ingress(
            &conn,
            bridge.conversation_id,
            "repo_sync:pulled",
            "second",
            r#"{"kind":"pulled","slug":"life","oneline":["bbb second"]}"#,
        );
    }

    let fence = event_fence(&bridge);
    tokio::spawn(cc_event_loop(event_rx, bridge.clone(), alert_dispatcher));
    await_fence(fence).await;

    // The drain emits one SystemMessageBroadcast carrying the synthesized
    // summary. Content checks: both sha prefixes should appear in the
    // rendered_html, and the source label should mention repo_sync:summary.
    let msgs = drain_broadcast(&mut broadcast_rx);
    let broadcasts: Vec<&WsServerMessage> = msgs
        .iter()
        .filter(|m| matches!(m, WsServerMessage::SystemMessageBroadcast { .. }))
        .collect();
    assert_eq!(
        broadcasts.len(),
        1,
        "drain should produce a single combined inject, got {}: {msgs:?}",
        broadcasts.len(),
    );
    let body = match &broadcasts[0] {
        WsServerMessage::SystemMessageBroadcast { rendered_html, .. } => rendered_html.clone(),
        _ => unreachable!(),
    };
    assert!(
        body.contains("aaa first") && body.contains("bbb second"),
        "collapsed summary should include both commit onelines; body: {body}"
    );
    assert!(
        body.contains("repo_sync:summary"),
        "collapsed summary should carry the summary source label; body: {body}"
    );
}

// -----------------------------------------------------------------------
// Messaging-drain integration tests
// -----------------------------------------------------------------------

/// Combined drain: one event + one pending push, session installed.
///
/// Asserts:
/// - Exactly one `SystemMessageBroadcast` whose `rendered_html` contains
///   content from both the event and the message.
/// - Exactly one `ToolUseSummary` with
///   `tool_name == MCP_MESSAGE_RECEIVED_PSEUDO_TOOL`.
/// - Ingress push row is marked delivered (`pending_ingress_count` returns 0).
/// - The conversation is owed nothing further on the bus: the drain advanced its
///   cursor past the message it just rendered.
#[tokio::test]
async fn drain_combined_events_and_messaging_marks_all_delivered() {
    let (bridge, mut broadcast_rx) = bridge_with_messenger_for_drain().await;

    {
        let conn = bridge.db.lock().await;
        // Use a sentinel in the payload (not the name) for the HTML assertion — the payload
        // is always included verbatim in the rendered JSON block, making the check robust to
        // event-name display formatting changes.
        enqueue_ingress(
            &conn,
            bridge.conversation_id,
            "cron",
            "combined-drain-cron-event",
            r#"{"key":"combined-drain-cron-sentinel"}"#,
        );
    }
    seed_pending_push(&bridge, "combined-drain-push-body").await;

    // Install recording session so send_system_message succeeds.
    let _cc_rx = bridge.install_recording_session_for_test().await;

    drain_pending_events(&bridge).await;

    let msgs = drain_broadcast(&mut broadcast_rx);

    // Exactly one SystemMessageBroadcast containing both sources.
    let system_broadcasts: Vec<&WsServerMessage> = msgs
        .iter()
        .filter(|m| matches!(m, WsServerMessage::SystemMessageBroadcast { .. }))
        .collect();
    assert_eq!(
        system_broadcasts.len(),
        1,
        "expected 1 SystemMessageBroadcast, got {}: {msgs:?}",
        system_broadcasts.len()
    );
    let rendered_html = match system_broadcasts[0] {
        WsServerMessage::SystemMessageBroadcast { rendered_html, .. } => rendered_html,
        m => unreachable!("filter guaranteed SystemMessageBroadcast; got {m:?}"),
    };
    assert!(
        rendered_html.contains("combined-drain-cron-sentinel"),
        "SystemMessageBroadcast must contain event payload sentinel; html: {rendered_html}"
    );
    assert!(
        rendered_html.contains("combined-drain-push-body"),
        "SystemMessageBroadcast must contain messaging push body; html: {rendered_html}"
    );

    // Exactly one ToolUseSummary for MCP_MESSAGE_RECEIVED_PSEUDO_TOOL.
    let tool_summaries: Vec<&WsServerMessage> = msgs
        .iter()
        .filter(|m| {
            matches!(
                m,
                WsServerMessage::ToolUseSummary { tool_name, .. }
                    if tool_name == brenn_render::tools::messaging::MCP_MESSAGE_RECEIVED_PSEUDO_TOOL
            )
        })
        .collect();
    assert_eq!(
        tool_summaries.len(),
        1,
        "expected 1 ToolUseSummary for MCP_MESSAGE_RECEIVED_PSEUDO_TOOL, got {}: {msgs:?}",
        tool_summaries.len()
    );

    // Ingress push row marked delivered (unified store).
    {
        let conn = bridge.db.lock().await;
        assert_eq!(
            pending_ingress_count(&conn, bridge.conversation_id),
            0,
            "ingress push row must be marked delivered after successful drain"
        );
    }

    // The bus half: the drain advanced the conversation's cursor past the
    // message it rendered, so nothing is owed on the channel any more.
    assert!(
        !owed_on_the_bus(&bridge).await,
        "the drain must advance the conversation's position past the delivered message"
    );
}

/// A bus batch consisting solely of the conversation's own utterances costs it
/// nothing: no CC turn, no broadcast — and the position still advances, so a
/// later wake finds nothing owed rather than the same batch forever.
///
/// This is the machinery loop's break: an operator-authored subscription on a
/// conversation's own record would otherwise inject each record event as a
/// system message, which is republished to the record, and round again.
#[tokio::test]
async fn a_self_echo_batch_advances_and_injects_nothing() {
    let (bridge, mut broadcast_rx) = bridge_with_messenger_for_drain().await;

    let me = brenn_lib::messaging::ParticipantId::for_conversation(bridge.conversation_id);
    seed_pending_push_from(&bridge, me.as_str(), "my own words, coming back").await;

    // A live session, so a send would succeed if the drain attempted one.
    let _cc_rx = bridge.install_recording_session_for_test().await;

    drain_pending_events(&bridge).await;

    let msgs = drain_broadcast(&mut broadcast_rx);
    assert!(
        !msgs
            .iter()
            .any(|m| matches!(m, WsServerMessage::SystemMessageBroadcast { .. })),
        "a conversation must not be told what it itself said; got {msgs:?}"
    );
    assert!(
        !owed_on_the_bus(&bridge).await,
        "the position must advance past a self-echo or every wake re-serves it"
    );
}

/// The other half of the filter: a batch mixing the conversation's own message
/// with a peer's delivers the peer's and only the peer's.
#[tokio::test]
async fn a_mixed_batch_delivers_only_what_the_conversation_did_not_say() {
    let (bridge, mut broadcast_rx) = bridge_with_messenger_for_drain().await;

    let me = brenn_lib::messaging::ParticipantId::for_conversation(bridge.conversation_id);
    seed_pending_push_from(&bridge, me.as_str(), "self-echo-body").await;
    seed_pending_push_from(&bridge, "conversation:99", "peer-body").await;

    let _cc_rx = bridge.install_recording_session_for_test().await;

    drain_pending_events(&bridge).await;

    let msgs = drain_broadcast(&mut broadcast_rx);
    let rendered: Vec<&String> = msgs
        .iter()
        .filter_map(|m| match m {
            WsServerMessage::SystemMessageBroadcast { rendered_html, .. } => Some(rendered_html),
            _ => None,
        })
        .collect();
    assert_eq!(rendered.len(), 1, "one injection for the batch: {msgs:?}");
    assert!(
        rendered[0].contains("peer-body"),
        "the peer's message must be delivered; html: {}",
        rendered[0]
    );
    assert!(
        !rendered[0].contains("self-echo-body"),
        "the conversation's own message must not be; html: {}",
        rendered[0]
    );
    assert!(!owed_on_the_bus(&bridge).await);
}

/// Send-failure path: one event + one pending push, no session installed.
///
/// Asserts:
/// - Event row is still pending.
/// - Push row is still pending.
/// - At least one `SystemMessageBroadcast` is emitted (persist_broadcast_send
///   fires before the no-session check).
/// - No `ToolUseSummary` for `MCP_MESSAGE_RECEIVED_PSEUDO_TOOL` (drain
///   returns early at the send-failure branch before reaching line 625).
#[tokio::test]
async fn drain_send_failure_leaves_messaging_pushes_pending() {
    let (bridge, mut broadcast_rx) = bridge_with_messenger_for_drain().await;

    {
        let conn = bridge.db.lock().await;
        enqueue_ingress(
            &conn,
            bridge.conversation_id,
            "cron",
            "send-failure-cron-event",
            "{}",
        );
    }
    seed_pending_push(&bridge, "send-failure-push-body").await;

    // No CC session installed — send_system_message will return Err.
    drain_pending_events(&bridge).await;

    let msgs = drain_broadcast(&mut broadcast_rx);

    // Exactly one SystemMessageBroadcast is emitted (persist_broadcast_send fires before
    // the no-session check; checking exactly 1 catches spurious double-broadcast regressions).
    let system_broadcast_count = msgs
        .iter()
        .filter(|m| matches!(m, WsServerMessage::SystemMessageBroadcast { .. }))
        .count();
    assert_eq!(
        system_broadcast_count, 1,
        "expected exactly 1 SystemMessageBroadcast before send failure; got {system_broadcast_count}: {msgs:?}"
    );

    // No ToolUseSummary — drain returns early before the dual-broadcast step.
    let has_tool_summary = msgs.iter().any(|m| {
        matches!(
            m,
            WsServerMessage::ToolUseSummary { tool_name, .. }
                if tool_name == brenn_render::tools::messaging::MCP_MESSAGE_RECEIVED_PSEUDO_TOOL
        )
    });
    assert!(
        !has_tool_summary,
        "ToolUseSummary must NOT be emitted after send failure; got {msgs:?}"
    );

    // Ingress row still pending.
    {
        let conn = bridge.db.lock().await;
        assert_eq!(
            pending_ingress_count(&conn, bridge.conversation_id),
            1,
            "ingress row must stay pending after failed drain"
        );
    }

    // And the bus half too: the position did not advance, so the conversation is
    // still owed the message.
    assert!(
        owed_on_the_bus(&bridge).await,
        "the position must not advance past a message the drain failed to send"
    );
}

/// Messaging-only drain: no events, one pending push, session installed.
///
/// Exercises the `(true, false)` branch of `render_combined_drain`
/// (messages-only rendering). Asserts:
/// - `SystemMessageBroadcast` is emitted.
/// - `ToolUseSummary` with `tool_name == MCP_MESSAGE_RECEIVED_PSEUDO_TOOL`
///   is emitted.
/// - Push row is marked delivered.
/// - Ingress push count is 0 (no ingress was seeded).
#[tokio::test]
async fn drain_messaging_only_delivers_without_events() {
    let (bridge, mut broadcast_rx) = bridge_with_messenger_for_drain().await;

    seed_pending_push(&bridge, "messaging-only-push-body").await;

    // Install recording session so send_system_message succeeds.
    let _cc_rx = bridge.install_recording_session_for_test().await;

    drain_pending_events(&bridge).await;

    let msgs = drain_broadcast(&mut broadcast_rx);

    // SystemMessageBroadcast emitted.
    let saw_system_broadcast = msgs
        .iter()
        .any(|m| matches!(m, WsServerMessage::SystemMessageBroadcast { .. }));
    assert!(
        saw_system_broadcast,
        "messaging-only drain must emit SystemMessageBroadcast; got {msgs:?}"
    );

    // ToolUseSummary for MCP_MESSAGE_RECEIVED_PSEUDO_TOOL emitted.
    let tool_summaries: Vec<&WsServerMessage> = msgs
        .iter()
        .filter(|m| {
            matches!(
                m,
                WsServerMessage::ToolUseSummary { tool_name, .. }
                    if tool_name == brenn_render::tools::messaging::MCP_MESSAGE_RECEIVED_PSEUDO_TOOL
            )
        })
        .collect();
    assert_eq!(
        tool_summaries.len(),
        1,
        "messaging-only drain must emit exactly 1 ToolUseSummary for \
             MCP_MESSAGE_RECEIVED_PSEUDO_TOOL; got {}: {msgs:?}",
        tool_summaries.len()
    );

    // The position advanced past the message the drain rendered.
    assert!(
        !owed_on_the_bus(&bridge).await,
        "the position must advance after a messaging-only drain"
    );

    let conn = bridge.db.lock().await;

    // No ingress events were seeded — still empty.
    assert_eq!(
        pending_ingress_count(&conn, bridge.conversation_id),
        0,
        "ingress push count must remain 0 for messaging-only drain"
    );
}

// -----------------------------------------------------------------------
// AC2 real D1 window test (test-1)
//
// Acceptance criterion 2: failure injected in the actual post-mpsc-enqueue /
// pre-flush window (between outgoing_tx.send() and write_all+flush in
// spawn_stdin_writer) must leave the push row delivered_at IS NULL.
//
// The previous `d1_window_flush_failure_leaves_row_undelivered` test injects
// failure at the `dispatch_row` / mock-router level, which is too high — it
// does not exercise the D1 window in spawn_stdin_writer at all.
//
// This test wires a real spawn_stdin_writer to a broken pipe (read end dropped
// immediately) and drives the full drain path: seed push row → broken-pipe
// writer → drain_pending_events fails → row stays delivered_at IS NULL.
// -----------------------------------------------------------------------

/// Acceptance 2 — real D1 window (flush failure in spawn_stdin_writer).
///
/// A messaging push row must stay `delivered_at IS NULL` when the actual
/// OS-pipe flush in `spawn_stdin_writer` fails (read end of pipe was dropped
/// before the write).
///
/// Test structure:
///   1. Create bridge with one pending push row.
///   2. Create stalling session + real `spawn_stdin_writer` on a broken
///      duplex pipe (read half dropped immediately).
///   3. Install the session, call `drain_pending_events`.
///   4. The writer tries to write to the broken pipe → fires Err ack.
///   5. `persist_broadcast_send` returns Err → `drain_pending_events` returns
///      early without marking the row delivered.
///   6. Assert push row is still `delivered_at IS NULL`.
#[tokio::test]
async fn d1_real_window_broken_pipe_leaves_push_row_undelivered() {
    use brenn_obs::transcript::TranscriptWriter;

    let (bridge, _broadcast_rx) = bridge_with_messenger_for_drain().await;
    seed_pending_push(&bridge, "d1-window-test-body").await;

    // Create a transcript writer backed by a temp dir (required by
    // spawn_stdin_writer; the writer logs every sent line there).
    let tmp_dir = tempfile::tempdir().expect("tempdir");
    let transcript = std::sync::Arc::new(
        TranscriptWriter::new(tmp_dir.path(), "d1-test.log").expect("TranscriptWriter::new"),
    );

    // Broken-pipe: create a duplex pair and immediately drop the read half.
    // The first write the writer task attempts will fail with an I/O error.
    let (write_half, read_half) = tokio::io::duplex(4096);
    drop(read_half);

    // Build a stalling session (auto_ack=false, cap-64) and hand the outgoing_rx
    // directly to spawn_stdin_writer. The session itself goes into the bridge.
    // We deliberately do NOT capture outgoing_rx here — the writer task owns it.
    let (session, outgoing_rx) = brenn_cc::session::CcSession::stalling_for_test();
    brenn_cc::session::tasks::spawn_stdin_writer(write_half, outgoing_rx, transcript);

    // Install the (stalling) session into the bridge.
    {
        let mut guard = bridge.session.lock().await;
        *guard = Some(session);
    }

    // Drain: send_system_message → persist_broadcast_send → send_message_acked
    // → writer picks up envelope → write to broken pipe → Err ack →
    // persist_broadcast_send returns Err → drain_pending_events returns early.
    drain_pending_events(&bridge).await;

    // The message must still be owed: the broken-pipe error left the position
    // where it was (at-least-once durability guarantee, D1).
    assert!(
        owed_on_the_bus(&bridge).await,
        "D1 real-window: the position must not advance past a flush failure"
    );
}

// -----------------------------------------------------------------------
// AC3 drain-path recovery test (test-2)
//
// Acceptance criterion 3: a push row left delivered_at IS NULL after a
// flush failure (mpsc-loss scenario) is recovered by drain_pending_events
// on the next session attach.
//
// The mpsc-loss scenario: message was enqueued into the mpsc buffer (outgoing_tx
// sent successfully), but the writer task died (session dropped) before
// write_all+flush — the ack receiver gets RecvError (ack_tx dropped), so
// persist_broadcast_send returns Err and the push row stays undelivered.
//
// After a new session attaches (simulating a Brenn restart / session restart)
// and drain_pending_events runs, the row must be delivered.
// -----------------------------------------------------------------------

/// Acceptance 3 — drain-path recovery after mpsc-loss.
///
/// A push row left `delivered_at IS NULL` because the session died mid-flight
/// (ack sender dropped → RecvError) must be picked up and delivered by
/// `drain_pending_events` when a new session attaches.
///
/// Test structure:
///   1. Create bridge with one pending push row.
///   2. Install stalling session; call drain_pending_events. It enqueues
///      the message and blocks awaiting the flush ack. Drop the session
///      (simulating the writer task exiting without firing the ack) — the
///      ack receiver gets RecvError → persist_broadcast_send returns Err →
///      drain_pending_events returns early, row stays undelivered.
///   3. Verify the row is still pending.
///   4. Install a working (recording) session. Run drain_pending_events again.
///   5. Assert the row is now marked delivered.
#[tokio::test]
async fn drain_recovers_push_row_left_undelivered_after_session_death() {
    let (bridge, _broadcast_rx) = bridge_with_messenger_for_drain().await;
    seed_pending_push(&bridge, "ac3-drain-recovery-body").await;

    // Pass 1: install stalling session; drain; drop the stalling rx (simulate
    // the writer task exiting without firing the ack → RecvError → Err).
    {
        let stalling_rx =
            super::super::super::test_support::install_stalling_session(&bridge).await;

        // Run drain in a separate task — it will block awaiting the flush ack.
        let bridge_clone = bridge.clone();
        let drain_task = tokio::spawn(async move {
            drain_pending_events(&bridge_clone).await;
        });

        // Wait briefly for drain to enqueue the message (it will block on ack_rx.await).
        // Then drop stalling_rx: the ack_tx in the queued envelope is dropped, so
        // ack_rx.await resolves with RecvError → persist_broadcast_send returns Err.
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        drop(stalling_rx);

        // The drain task must now unblock (RecvError resolves the ack await) and return.
        tokio::time::timeout(tokio::time::Duration::from_secs(2), drain_task)
            .await
            .expect("drain task must complete within 2s after ack_tx dropped")
            .expect("drain task must not panic");
    }

    // Verify the message is still owed after the failed drain.
    assert!(
        owed_on_the_bus(&bridge).await,
        "AC3 drain-path: the position must not advance past a session-death flush failure"
    );

    // Pass 2: install recording session (auto-ack → sends succeed), run drain again.
    let _cc_rx = bridge.install_recording_session_for_test().await;
    drain_pending_events(&bridge).await;

    // The position must now have advanced past it.
    assert!(
        !owed_on_the_bus(&bridge).await,
        "AC3 drain-path: the position must advance after a successful drain on a new session"
    );
}

/// The live wake path serves the conversation from its position, not from the
/// envelope the wake carried: one call renders every message the conversation is
/// owed, and a second call — the shape a per-message wake produces — renders
/// nothing, because the first advanced past them.
#[tokio::test]
async fn live_delivery_serves_the_backlog_once() {
    let (bridge, mut broadcast_rx) = bridge_with_messenger_for_drain().await;

    seed_pending_push(&bridge, "live-first").await;
    seed_pending_push(&bridge, "live-second").await;
    let _cc_rx = bridge.install_recording_session_for_test().await;

    crate::active_bridge::deliver_conversation_backlog(&bridge)
        .await
        .expect("recording session accepts the send");

    let msgs = drain_broadcast(&mut broadcast_rx);
    let broadcasts: Vec<&WsServerMessage> = msgs
        .iter()
        .filter(|m| matches!(m, WsServerMessage::SystemMessageBroadcast { .. }))
        .collect();
    assert_eq!(
        broadcasts.len(),
        1,
        "both owed messages ride one render; got {msgs:?}"
    );
    // The live path carries no dual ToolUseSummary — that is drain-path only.
    assert!(
        !msgs
            .iter()
            .any(|m| matches!(m, WsServerMessage::ToolUseSummary { .. })),
        "live delivery emits no ToolUseSummary; got {msgs:?}"
    );

    crate::active_bridge::deliver_conversation_backlog(&bridge)
        .await
        .expect("a second wake with nothing owed is not a failure");
    assert!(
        drain_broadcast(&mut broadcast_rx)
            .iter()
            .all(|m| !matches!(m, WsServerMessage::SystemMessageBroadcast { .. })),
        "the position moved past the batch, so the second wake renders nothing"
    );

    assert!(
        !owed_on_the_bus(&bridge).await,
        "the advance moved the position past both messages"
    );
}

// -----------------------------------------------------------------------
// The impetus pool: what an ambience injection costs, and what restores it
// -----------------------------------------------------------------------

/// The ceiling the bridge's app resolves — read from the bridge rather than
/// duplicated, so a fixture that changes it fails on the assertion that cares
/// instead of on an unrelated literal.
fn ceiling(bridge: &ActiveBridge) -> u32 {
    bridge.app_config_default_send_budget()
}

/// What the conversation's pool holds, or `None` where nothing has touched it.
async fn pool(bridge: &ActiveBridge) -> Option<u32> {
    let conn = bridge.db.lock().await;
    brenn_messaging_store::db::read_send_budget(&conn, bridge.conversation_id)
}

/// Put the pool at a known level — an exhausted conversation, or one with
/// exactly enough left to be worth counting.
async fn set_pool(bridge: &ActiveBridge, remaining: u32) {
    let conn = bridge.db.lock().await;
    brenn_messaging_store::db::reset_send_budget(&conn, bridge.conversation_id, remaining);
}

/// One injection is one CC turn, and one CC turn is one unit — however many
/// messages rode in it.
#[tokio::test]
async fn an_ambience_batch_draws_one_unit_whatever_its_size() {
    let (bridge, _broadcast_rx) = bridge_with_messenger_for_drain().await;
    set_pool(&bridge, 10).await;
    seed_pending_push(&bridge, "first").await;
    seed_pending_push(&bridge, "second").await;
    seed_pending_push(&bridge, "third").await;
    let _cc_rx = bridge.install_recording_session_for_test().await;

    drain_pending_events(&bridge).await;

    assert_eq!(
        pool(&bridge).await,
        Some(9),
        "three messages in one render is one turn and one unit"
    );
    assert!(!owed_on_the_bus(&bridge).await);
}

/// Ingress rows are not bus injections and do not draw: a render with no
/// envelopes in it leaves the pool alone.
#[tokio::test]
async fn an_events_only_drain_draws_nothing() {
    let (bridge, _broadcast_rx) = bridge_with_messenger_for_drain().await;
    set_pool(&bridge, 10).await;
    {
        let conn = bridge.db.lock().await;
        enqueue_ingress(&conn, bridge.conversation_id, "cron", "a cron poke", "{}");
    }
    let _cc_rx = bridge.install_recording_session_for_test().await;

    drain_pending_events(&bridge).await;

    assert_eq!(
        pool(&bridge).await,
        Some(10),
        "an ingress-only render is outside the pool's scope"
    );
}

/// Carried impetus restores the pool before the turn it pays for, so the batch
/// lands at the ceiling less its own unit.
#[tokio::test]
async fn a_batch_carrying_impetus_refills_the_pool_then_draws() {
    let (bridge, _broadcast_rx) = bridge_with_messenger_for_drain().await;
    set_pool(&bridge, 2).await;
    seed_pending_push(&bridge, "ordinary traffic").await;
    seed_pending_push_with_impetus(&bridge, "someone is actually here").await;
    let _cc_rx = bridge.install_recording_session_for_test().await;

    drain_pending_events(&bridge).await;

    assert_eq!(
        pool(&bridge).await,
        Some(ceiling(&bridge) - 1),
        "one impetus-bearing envelope refills the batch it rides in"
    );
}

/// An exhausted pool holds the bus batch: nothing is injected, the positions
/// stay owed, and the ingress half of the same drain still delivers.
#[tokio::test]
async fn at_zero_the_bus_batch_is_held_and_the_ingress_half_still_delivers() {
    let (bridge, mut broadcast_rx) = bridge_with_messenger_for_drain().await;
    set_pool(&bridge, 0).await;
    {
        let conn = bridge.db.lock().await;
        enqueue_ingress(&conn, bridge.conversation_id, "cron", "a cron poke", "{}");
    }
    seed_pending_push(&bridge, "held-bus-body").await;
    let _cc_rx = bridge.install_recording_session_for_test().await;

    drain_pending_events(&bridge).await;

    let rendered: Vec<String> = drain_broadcast(&mut broadcast_rx)
        .iter()
        .filter_map(|m| match m {
            WsServerMessage::SystemMessageBroadcast { rendered_html, .. } => {
                Some(rendered_html.clone())
            }
            _ => None,
        })
        .collect();
    assert_eq!(rendered.len(), 1, "the ingress half renders on its own");
    assert!(
        !rendered[0].contains("held-bus-body"),
        "the bus half is held, not injected; html: {}",
        rendered[0]
    );
    assert_eq!(pool(&bridge).await, Some(0), "a held batch draws nothing");
    assert!(
        owed_on_the_bus(&bridge).await,
        "a held batch leaves its positions owed so a refill can deliver it"
    );
    {
        let conn = bridge.db.lock().await;
        assert_eq!(
            pending_ingress_count(&conn, bridge.conversation_id),
            0,
            "the ingress half draws nothing and delivers regardless"
        );
    }
}

/// The live wake path holds too, and reports served-for-now so the wake record
/// retires instead of spinning.
#[tokio::test]
async fn at_zero_the_live_delivery_holds_and_still_reports_served() {
    let (bridge, mut broadcast_rx) = bridge_with_messenger_for_drain().await;
    set_pool(&bridge, 0).await;
    seed_pending_push(&bridge, "held-live-body").await;
    let _cc_rx = bridge.install_recording_session_for_test().await;

    crate::active_bridge::deliver_conversation_backlog(&bridge)
        .await
        .expect("a held batch is served for now, not a failure");

    assert!(
        !drain_broadcast(&mut broadcast_rx)
            .iter()
            .any(|m| matches!(m, WsServerMessage::SystemMessageBroadcast { .. })),
        "an exhausted conversation is told nothing"
    );
    assert!(owed_on_the_bus(&bridge).await, "the batch stays owed");
    assert_eq!(pool(&bridge).await, Some(0));
}

/// The revival hook: a refill delivers what the exhausted pool was holding,
/// riding the turn that revived the conversation rather than waiting for
/// unrelated bus traffic to wake it again.
#[tokio::test]
async fn a_refill_delivers_the_held_backlog() {
    let (bridge, mut broadcast_rx) = bridge_with_messenger_for_drain().await;
    set_pool(&bridge, 0).await;
    seed_pending_push(&bridge, "waiting-for-a-human").await;
    let _cc_rx = bridge.install_recording_session_for_test().await;

    crate::active_bridge::deliver_conversation_backlog(&bridge)
        .await
        .expect("held, not failed");
    assert!(owed_on_the_bus(&bridge).await);
    let _ = drain_broadcast(&mut broadcast_rx);

    bridge.refill_impetus_pool().await;

    let delivered: Vec<String> = drain_broadcast(&mut broadcast_rx)
        .iter()
        .filter_map(|m| match m {
            WsServerMessage::SystemMessageBroadcast { rendered_html, .. } => {
                Some(rendered_html.clone())
            }
            _ => None,
        })
        .collect();
    assert_eq!(delivered.len(), 1, "the held batch rides the refill");
    assert!(delivered[0].contains("waiting-for-a-human"));
    assert!(
        !owed_on_the_bus(&bridge).await,
        "delivering the held batch advances past it"
    );
    assert_eq!(
        pool(&bridge).await,
        Some(ceiling(&bridge) - 1),
        "the refill restored the pool and the delivered batch drew its unit"
    );
}

/// The filter runs before the pool, so a batch of nothing but the
/// conversation's own utterances advances even at zero — it costs no turn, so it
/// costs no unit, and leaving it owed would spin the wake pass forever.
#[tokio::test]
async fn a_self_echo_only_batch_advances_without_drawing_even_at_zero() {
    let (bridge, mut broadcast_rx) = bridge_with_messenger_for_drain().await;
    set_pool(&bridge, 0).await;
    let me = brenn_lib::messaging::ParticipantId::for_conversation(bridge.conversation_id);
    seed_pending_push_from(&bridge, me.as_str(), "my own words").await;
    let _cc_rx = bridge.install_recording_session_for_test().await;

    drain_pending_events(&bridge).await;

    assert!(
        !drain_broadcast(&mut broadcast_rx)
            .iter()
            .any(|m| matches!(m, WsServerMessage::SystemMessageBroadcast { .. })),
        "a self-echo is never injected"
    );
    assert!(
        !owed_on_the_bus(&bridge).await,
        "an exhausted pool must not turn a self-echo into a wake livelock"
    );
    assert_eq!(pool(&bridge).await, Some(0));
}

/// A handoff that fails is a tolerated transient: the batch stays owed and costs
/// nothing, so a dying bridge cannot burn the conversation's runway of real
/// turns on attempts. The retry after recovery draws exactly once.
#[tokio::test]
async fn a_failed_handoff_draws_nothing_and_the_retry_draws_once() {
    let (bridge, _broadcast_rx) = bridge_with_messenger_for_drain().await;
    set_pool(&bridge, 10).await;
    seed_pending_push(&bridge, "against-a-dead-bridge").await;

    // No session installed: send_system_message fails.
    drain_pending_events(&bridge).await;
    assert_eq!(
        pool(&bridge).await,
        Some(10),
        "an attempt that never reached the harness is not a turn"
    );
    assert!(owed_on_the_bus(&bridge).await);

    let _cc_rx = bridge.install_recording_session_for_test().await;
    drain_pending_events(&bridge).await;
    assert_eq!(
        pool(&bridge).await,
        Some(9),
        "the retry that landed draws once, not once per attempt"
    );
    assert!(!owed_on_the_bus(&bridge).await);
}

/// A pool nothing has touched holds the ceiling: the row is minted by the first
/// draw, not by the conversation.
#[tokio::test]
async fn a_conversation_that_has_never_drawn_holds_the_ceiling() {
    let (bridge, _broadcast_rx) = bridge_with_messenger_for_drain().await;
    assert_eq!(
        pool(&bridge).await,
        None,
        "nothing has touched the pool yet"
    );
    seed_pending_push(&bridge, "first-ever-traffic").await;
    let _cc_rx = bridge.install_recording_session_for_test().await;

    drain_pending_events(&bridge).await;

    assert_eq!(
        pool(&bridge).await,
        Some(ceiling(&bridge) - 1),
        "an untouched pool is a full one, and the first batch draws from the ceiling"
    );
}

/// A ceiling of zero makes the conversation attended-only: the bus provokes no
/// turn on it, and carried impetus is no exception — the reset it redeems
/// restores the pool to zero, which still cannot pay for a turn.
#[tokio::test]
async fn a_zero_ceiling_conversation_is_attended_only() {
    let (bridge, mut broadcast_rx) = bridge_with_messenger_for_drain_at_ceiling(0).await;
    let _cc_rx = bridge.install_recording_session_for_test().await;
    seed_pending_push(&bridge, "unattended-traffic").await;

    drain_pending_events(&bridge).await;

    assert!(
        !drain_broadcast(&mut broadcast_rx)
            .iter()
            .any(|m| matches!(m, WsServerMessage::SystemMessageBroadcast { .. })),
        "a zero ceiling injects nothing"
    );
    assert!(owed_on_the_bus(&bridge).await, "the batch is held");
    assert_eq!(pool(&bridge).await, None, "and nothing drew");

    seed_pending_push_with_impetus(&bridge, "someone-is-actually-here").await;
    drain_pending_events(&bridge).await;

    assert!(
        !drain_broadcast(&mut broadcast_rx)
            .iter()
            .any(|m| matches!(m, WsServerMessage::SystemMessageBroadcast { .. })),
        "a refill to zero is still zero"
    );
    assert!(owed_on_the_bus(&bridge).await, "the batch stays held");
    assert_eq!(
        pool(&bridge).await,
        Some(0),
        "the redemption reset the pool to its ceiling, and its ceiling is nothing"
    );

    // The attended door is the one that still works on such a conversation.
    let cc_err = legacy_websocket_turn(&bridge, "typed by a person").await;
    assert!(
        cc_err.is_none(),
        "the legacy door takes the turn: {cc_err:?}"
    );
}

/// The legacy websocket door is the attended surface: its turn restores the
/// pool to full and spends nothing — an attended turn does not pay out of the
/// allowance that attention just granted.
#[tokio::test]
async fn a_legacy_websocket_turn_refills_the_pool_without_drawing() {
    let (bridge, _broadcast_rx) = bridge_with_messenger_for_drain().await;
    set_pool(&bridge, 3).await;
    let _cc_rx = bridge.install_recording_session_for_test().await;

    let cc_err = legacy_websocket_turn(&bridge, "hello again").await;
    assert!(
        cc_err.is_none(),
        "the recording session takes it: {cc_err:?}"
    );

    assert_eq!(
        pool(&bridge).await,
        Some(ceiling(&bridge)),
        "the attended turn refills and draws nothing — ceiling less one would be a stray draw"
    );
}

/// A human turn through the legacy websocket delivers the backlog an exhausted
/// pool was holding, so a stalled conversation is unstalled by the same touch
/// that refilled it rather than by the next unrelated bus traffic.
#[tokio::test]
async fn a_legacy_websocket_turn_delivers_the_held_backlog() {
    let (bridge, mut broadcast_rx) = bridge_with_messenger_for_drain().await;
    set_pool(&bridge, 0).await;
    seed_pending_push(&bridge, "waiting-for-the-legacy-door").await;
    let _cc_rx = bridge.install_recording_session_for_test().await;

    crate::active_bridge::deliver_conversation_backlog(&bridge)
        .await
        .expect("held, not failed");
    assert!(owed_on_the_bus(&bridge).await, "the batch is held");
    let _ = drain_broadcast(&mut broadcast_rx);

    let cc_err = legacy_websocket_turn(&bridge, "are you still there").await;
    assert!(
        cc_err.is_none(),
        "the recording session takes it: {cc_err:?}"
    );

    let delivered: Vec<String> = drain_broadcast(&mut broadcast_rx)
        .iter()
        .filter_map(|m| match m {
            WsServerMessage::SystemMessageBroadcast { rendered_html, .. } => {
                Some(rendered_html.clone())
            }
            _ => None,
        })
        .collect();
    assert_eq!(delivered.len(), 1, "the held batch rides the attended turn");
    assert!(delivered[0].contains("waiting-for-the-legacy-door"));
    assert!(
        !owed_on_the_bus(&bridge).await,
        "delivering it advances past it"
    );
    assert_eq!(
        pool(&bridge).await,
        Some(ceiling(&bridge) - 1),
        "the turn refilled without drawing; the unit is the released batch's own"
    );
}

/// One human turn through the legacy websocket door.
/// `restores_impetus_pool` is the bit that makes it attended.
async fn legacy_websocket_turn(bridge: &Arc<ActiveBridge>, text: &str) -> Option<String> {
    crate::active_bridge::accept_user_send(
        bridge,
        crate::active_bridge::AcceptedSend {
            text,
            cc_text: text.to_string(),
            extra_blocks: Vec::new(),
            sender_user_id: bridge.user_id,
            sender_tz: None,
            sender_device_id: None,
            attachments: Vec::new(),
            selected_tasks: Vec::new(),
            origin: crate::active_bridge::SendOrigin::LegacyWs {
                username: "drain-test-user".to_string(),
                timestamp: "2026-01-01T00:00:00+00:00".to_string(),
            },
            interstitial: None,
            restores_impetus_pool: true,
        },
    )
    .await
}
