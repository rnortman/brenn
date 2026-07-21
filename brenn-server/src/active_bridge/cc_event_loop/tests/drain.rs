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
    bridge_with_messenger_for_drain, bridge_with_unspawned_event_loop, enqueue_ingress,
    pending_ingress_count, seed_pending_push,
};

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
    brenn_lib::messaging::set_repo_sync_staleness_days(1);

    // Backdate the conversation's updated_at to 10 days ago.
    {
        let conn = bridge.db.lock().await;
        let backdate = (chrono::Utc::now() - chrono::Duration::days(10)).to_rfc3339();
        brenn_lib::conversation::set_updated_at_for_test(&conn, bridge.conversation_id, &backdate);

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
    brenn_lib::messaging::set_repo_sync_staleness_days(7);
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
/// - Bus push row is marked delivered (`load_pending_pushes_for_drain` returns
///   empty).
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
                    if tool_name == crate::tools::messaging::MCP_MESSAGE_RECEIVED_PSEUDO_TOOL
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
    let conn = bridge.db.lock().await;
    assert_eq!(
        pending_ingress_count(&conn, bridge.conversation_id),
        0,
        "ingress push row must be marked delivered after successful drain"
    );

    // Bus push row also marked delivered (via the messaging path).
    let pushes = brenn_lib::messaging::db::load_pending_pushes_for_drain(
        &conn,
        &brenn_lib::messaging::ParticipantId::for_conversation(bridge.conversation_id),
    );
    assert!(
        pushes.is_empty(),
        "push row must be marked delivered after successful drain; got {pushes:?}"
    );
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
                if tool_name == crate::tools::messaging::MCP_MESSAGE_RECEIVED_PSEUDO_TOOL
        )
    });
    assert!(
        !has_tool_summary,
        "ToolUseSummary must NOT be emitted after send failure; got {msgs:?}"
    );

    // Ingress push row still pending.
    let conn = bridge.db.lock().await;
    assert_eq!(
        pending_ingress_count(&conn, bridge.conversation_id),
        1,
        "ingress push row must stay pending after failed drain"
    );

    // Both ingress push row and bus push row still pending.
    let pushes = brenn_lib::messaging::db::load_pending_pushes_for_drain(
        &conn,
        &brenn_lib::messaging::ParticipantId::for_conversation(bridge.conversation_id),
    );
    assert_eq!(
        pushes.len(),
        2,
        "both ingress and bus push rows must stay pending after failed drain; got {pushes:?}"
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
                    if tool_name == crate::tools::messaging::MCP_MESSAGE_RECEIVED_PSEUDO_TOOL
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

    let conn = bridge.db.lock().await;

    // Push row marked delivered.
    let pushes = brenn_lib::messaging::db::load_pending_pushes_for_drain(
        &conn,
        &brenn_lib::messaging::ParticipantId::for_conversation(bridge.conversation_id),
    );
    assert!(
        pushes.is_empty(),
        "push row must be marked delivered after messaging-only drain; got {pushes:?}"
    );

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
    use brenn_lib::obs::transcript::TranscriptWriter;

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

    // The push row must still be undelivered: the broken-pipe error left it
    // delivered_at IS NULL (at-least-once durability guarantee, D1).
    let conn = bridge.db.lock().await;
    let pushes = brenn_lib::messaging::db::load_pending_pushes_for_drain(
        &conn,
        &brenn_lib::messaging::ParticipantId::for_conversation(bridge.conversation_id),
    );
    assert!(
        !pushes.is_empty(),
        "D1 real-window: push row must stay delivered_at IS NULL after flush failure; \
             load_pending_pushes_for_drain returned empty (row was incorrectly marked delivered)"
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

    // Verify the row is still pending after the failed drain.
    {
        let conn = bridge.db.lock().await;
        let pushes = brenn_lib::messaging::db::load_pending_pushes_for_drain(
            &conn,
            &brenn_lib::messaging::ParticipantId::for_conversation(bridge.conversation_id),
        );
        assert!(
            !pushes.is_empty(),
            "AC3 drain-path: push row must stay pending after session-death flush failure; \
                 got empty (row was incorrectly marked delivered)"
        );
    }

    // Pass 2: install recording session (auto-ack → sends succeed), run drain again.
    let _cc_rx = bridge.install_recording_session_for_test().await;
    drain_pending_events(&bridge).await;

    // The row must now be delivered.
    let conn = bridge.db.lock().await;
    let pushes = brenn_lib::messaging::db::load_pending_pushes_for_drain(
        &conn,
        &brenn_lib::messaging::ParticipantId::for_conversation(bridge.conversation_id),
    );
    assert!(
        pushes.is_empty(),
        "AC3 drain-path: push row must be marked delivered after successful drain on new session; \
             got {pushes:?}"
    );
}
