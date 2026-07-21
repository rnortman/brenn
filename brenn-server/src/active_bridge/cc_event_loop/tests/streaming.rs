//! Streaming-family tests: `handle_stream_event` (StreamEvent) and
//! `handle_assistant_message` (AssistantMessage). Peeled out of `tests/mod.rs`
//! per design §2.4.

use super::super::super::test_support::{
    await_fence, drain_broadcast, event_fence, recv_broadcast, test_bridge,
};
use super::super::*;

use brenn_cc::protocol::incoming::{
    AssistantContent, AssistantMessage as CcAssistantMessage, ContentBlock, StreamEventMessage,
};

use crate::active_bridge::test_fixtures::TestBridgeConfig;
use tokio::sync::broadcast;

use super::super::super::ActiveBridges;

#[tokio::test]
async fn stream_event_produces_stream_token() {
    let (_bridge, event_tx, mut broadcast_rx, _ab) = test_bridge().await;

    let stream_evt = StreamEventMessage {
        uuid: "msg-1".into(),
        session_id: None,
        event: serde_json::json!({
            "type": "content_block_delta",
            "delta": { "type": "text_delta", "text": "Hello" }
        }),
    };
    event_tx
        .send(SessionEvent::StreamEvent(stream_evt))
        .await
        .unwrap();

    let msg = recv_broadcast(&mut broadcast_rx).await;
    match &msg {
        WsServerMessage::StreamToken { token } => assert_eq!(token, "Hello"),
        other => panic!("expected StreamToken, got {other:?}"),
    }
}

#[tokio::test]
async fn thinking_delta_produces_thinking_token() {
    let (_bridge, event_tx, mut broadcast_rx, _ab) = test_bridge().await;

    let stream_evt = StreamEventMessage {
        uuid: "msg-t".into(),
        session_id: None,
        event: serde_json::json!({
            "type": "content_block_delta",
            "delta": { "type": "thinking_delta", "thinking": "Let me think..." }
        }),
    };
    event_tx
        .send(SessionEvent::StreamEvent(stream_evt))
        .await
        .unwrap();

    let msg = recv_broadcast(&mut broadcast_rx).await;
    match &msg {
        WsServerMessage::ThinkingToken { token } => assert_eq!(token, "Let me think..."),
        other => panic!("expected ThinkingToken, got {other:?}"),
    }
}

#[tokio::test]
async fn assistant_message_extracts_text_and_thinking() {
    let (bridge, event_tx, mut broadcast_rx, _ab) = test_bridge().await;

    let assistant_msg = CcAssistantMessage {
        uuid: "msg-2".into(),
        parent_tool_use_id: None,
        message: AssistantContent {
            role: "assistant".into(),
            content: vec![
                ContentBlock::Thinking {
                    thinking: "let me think".into(),
                    signature: None,
                },
                ContentBlock::Text {
                    text: "Here's my answer".into(),
                },
                ContentBlock::ToolUse {
                    id: "t1".into(),
                    name: "Bash".into(),
                    input: serde_json::json!({}),
                },
            ],
            model: None,
            usage: None,
        },
    };
    let fence = event_fence(&bridge);
    event_tx
        .send(SessionEvent::AssistantMessage(assistant_msg))
        .await
        .unwrap();

    // Only AssistantMessage broadcasts; Status is not.
    let msg1 = recv_broadcast(&mut broadcast_rx).await;

    match &msg1 {
        WsServerMessage::AssistantMessage { content, .. } => {
            assert!(
                content.contains("<details"),
                "thinking should render as details element, got: {content}"
            );
            assert!(
                content.contains("let me think"),
                "thinking content should be present"
            );
            assert!(
                content.contains("Here&#x27;s my answer")
                    || content.contains("Here's my answer")
                    || content.contains("Here&rsquo;s my answer"),
                "text block should be present, got: {content}"
            );
            assert!(!content.contains("Bash"), "ToolUse should not appear");
        }
        other => panic!("expected AssistantMessage, got {other:?}"),
    }

    await_fence(fence).await;
    let extra = drain_broadcast(&mut broadcast_rx);
    assert!(
        extra.is_empty(),
        "AssistantMessage should not trigger further broadcasts, got: {extra:?}"
    );
}

/// After a genuine mid-session model switch, `handle_assistant_message`
/// must look up the new slug in `model_window_cache` and re-populate
/// `seed_max_tokens` with the cached value. This tests the caller-side
/// cache-lookup block in `handle_assistant_message` (cc_event_loop.rs:701-718),
/// which `slug_change_returns_new_slug_and_nulls_state` does not cover.
#[tokio::test]
async fn slug_change_caller_seeds_from_cache() {
    use brenn_cc::protocol::incoming::{AssistantContent, AssistantMessage as CcAssistantMessage};

    let db = brenn_lib::db::init_db_memory();
    let (tx, _rx) = broadcast::channel(64);
    let active_bridges = ActiveBridges::new();
    let (uid, conv_id) = {
        let conn = db.lock().await;
        let uid = brenn_lib::auth::user::create_user(&conn, "slug-cache-seed", "$argon2id$fake");
        let cid = conversation::create_conversation(&conn, uid, "test", false);
        // Pre-populate model_window_cache with the target slug.
        brenn_lib::model_window_cache::upsert(
            &conn,
            "claude-opus-4-7[1m]",
            1_000_000,
            Some("2.1.123"),
        );
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

    // Simulate the bridge being mid-session on a different model.
    *bridge.active_model_slug.lock().expect("lock") = Some("claude-sonnet-4-6".into());
    *bridge.seed_max_tokens.lock().expect("lock") = Some(200_000);

    // Process an assistant message from the new (switched-to) model.
    let switch_msg = CcAssistantMessage {
        message: AssistantContent {
            role: "assistant".into(),
            content: vec![],
            model: Some("claude-opus-4-7[1m]".into()),
            usage: None,
        },
        uuid: "u-cache-seed".into(),
        parent_tool_use_id: None,
    };
    handle_assistant_message(&bridge, &switch_msg, &ad).await;

    // After the message is processed, seed_max_tokens must reflect the
    // cached value for the new slug — not the old 200k seed.
    assert_eq!(
        *bridge.seed_max_tokens.lock().expect("lock"),
        Some(1_000_000),
        "seed_max_tokens must be re-populated from model_window_cache \
             after mid-session model switch to a cached slug"
    );
}

/// When a mid-session model switch targets a slug NOT in the cache,
/// `seed_max_tokens` must remain `None` (deferred until result frame).
/// Covers the `None =>` arm of the cache-lookup block in
/// `handle_assistant_message` (`cc_event_loop.rs:709-716`).
#[tokio::test]
async fn slug_change_caller_cache_miss_leaves_none() {
    use brenn_cc::protocol::incoming::{AssistantContent, AssistantMessage as CcAssistantMessage};

    let db = brenn_lib::db::init_db_memory();
    let (tx, _rx) = broadcast::channel(64);
    let active_bridges = ActiveBridges::new();
    let (uid, conv_id) = {
        let conn = db.lock().await;
        let uid = brenn_lib::auth::user::create_user(&conn, "slug-cache-miss", "$argon2id$fake");
        let cid = conversation::create_conversation(&conn, uid, "test", false);
        // Intentionally do NOT populate model_window_cache for the new slug.
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

    // Simulate the bridge being mid-session on a different model.
    *bridge.active_model_slug.lock().expect("lock") = Some("claude-sonnet-4-6".into());
    *bridge.seed_max_tokens.lock().expect("lock") = Some(200_000);

    // Process a switch message for a slug with no cache entry.
    let switch_msg = CcAssistantMessage {
        message: AssistantContent {
            role: "assistant".into(),
            content: vec![],
            model: Some("claude-never-seen-before".into()),
            usage: None,
        },
        uuid: "u-cache-miss".into(),
        parent_tool_use_id: None,
    };
    handle_assistant_message(&bridge, &switch_msg, &ad).await;

    // seed_max_tokens must be None — no cache entry for the new slug,
    // so broadcast deferred until the result frame.
    assert_eq!(
        *bridge.seed_max_tokens.lock().expect("lock"),
        None,
        "seed_max_tokens must be None when the new slug is not in the cache"
    );
}
