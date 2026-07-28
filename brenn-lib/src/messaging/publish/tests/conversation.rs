//! The conversation principal: a conversation publishing its own chat record and
//! token stream.
//!
//! What these pin is that the adapter is not privileged. It reaches its own
//! app's chat subtree because the derived policy says so, and it reaches nothing
//! else — the same two layers, in the same order, that gate every other
//! publisher.

use super::super::*;
use super::{CHAT_PREFIX, ChatFixture, test_app_config};
use crate::config::{ChatLeaf, LlmChatConfig, chat_address};
use crate::messaging::config::ResolvedMessagingConfig;
use crate::messaging::testutils::test_channel_entry;
use indexmap::IndexMap;
use std::sync::Arc;

/// The app owning the fixture's conversation.
const OWNER: &str = "pa-bob";
/// A second app, whose chat subtree the fixture's conversation must not reach.
const NEIGHBOR: &str = "pa-alice";
const CONVERSATION: i64 = 1;

/// An app whose policy carries nothing but the derived chat-tree authority —
/// the shape resolution produces for an app that authored no messaging config.
fn chat_app(slug: &str) -> crate::config::AppConfig {
    let mut cfg = test_app_config(
        slug,
        Some(ResolvedMessagingConfig {
            send_budget: 100,
            subscriptions: vec![],
        }),
        vec!["bob".to_string()],
    );
    cfg.policy = crate::access::AppPolicy::default();
    LlmChatConfig::default().grant_app_chat_tree(slug, &mut cfg.policy);
    cfg
}

fn addr(app: &str, leaf: ChatLeaf) -> String {
    chat_address(CHAT_PREFIX, app, leaf, CONVERSATION)
}

/// A messenger holding both apps' durable and ephemeral chat channels, plus one
/// ordinary channel outside the chat tree entirely.
async fn chat_messenger() -> Arc<Messenger> {
    let mut apps: IndexMap<String, crate::config::AppConfig> = IndexMap::new();
    apps.insert(OWNER.to_string(), chat_app(OWNER));
    apps.insert(NEIGHBOR.to_string(), chat_app(NEIGHBOR));
    apps.insert(
        "no-grants".to_string(),
        test_app_config("no-grants", None, vec!["bob".to_string()]),
    );

    ChatFixture {
        leaves: [OWNER, NEIGHBOR]
            .iter()
            .map(|app| ((*app).to_string(), CONVERSATION))
            .collect(),
        // Only the owner's conversation exists: the neighbor's subtree is here
        // to be refused, and a refusal is decided before any row is read.
        conversations: vec![(CONVERSATION, OWNER.to_string())],
        apps,
        extra_durable: vec![test_channel_entry("alerts.high", vec![])],
    }
    .build()
    .await
}

async fn send(m: &Messenger, app_slug: &str, addr: &str) -> PublishResult {
    m.publish_from_conversation(CONVERSATION, app_slug, addr, "body", Urgency::Normal)
        .await
}

#[tokio::test]
async fn the_record_and_the_stream_both_land() {
    let m = chat_messenger().await;

    for leaf in [ChatLeaf::Out, ChatLeaf::Stream] {
        let result = send(&m, OWNER, &addr(OWNER, leaf)).await;
        assert!(
            matches!(
                result,
                PublishResult::Ok {
                    remaining_budget: None,
                    ..
                }
            ),
            "{leaf:?} publish must be admitted with no conversation budget drawn, got {result:?}"
        );
    }
}

#[tokio::test]
async fn the_stored_sender_is_the_conversation_not_the_app() {
    let m = chat_messenger().await;
    assert!(matches!(
        send(&m, OWNER, &addr(OWNER, ChatLeaf::Out)).await,
        PublishResult::Ok { .. }
    ));

    let conn = m.db.lock().await;
    let sender: String = conn
        .query_row("SELECT sender FROM messaging_messages", [], |r| r.get(0))
        .unwrap();
    assert_eq!(sender, "conversation:1");
}

#[tokio::test]
async fn a_conversation_cannot_reach_another_apps_tree() {
    let m = chat_messenger().await;

    for leaf in [
        ChatLeaf::In,
        ChatLeaf::Out,
        ChatLeaf::Stream,
        ChatLeaf::Wake,
    ] {
        let target = addr(NEIGHBOR, leaf);
        let result = send(&m, OWNER, &target).await;
        assert!(
            matches!(result, PublishResult::AclDenied(ref a) if *a == target),
            "{target} must be denied, got {result:?}"
        );
    }
}

#[tokio::test]
async fn a_conversation_reaches_nothing_outside_the_chat_tree() {
    let m = chat_messenger().await;
    let result = send(&m, OWNER, "brenn:alerts.high").await;
    assert!(matches!(result, PublishResult::AclDenied(_)), "{result:?}");
}

#[tokio::test]
async fn without_the_derived_grant_there_is_no_bypass() {
    // The adapter's authority is entirely the owning app's policy. An app that
    // somehow resolved without the chat grants publishes nothing — this arm is
    // what makes "no ACL bypass anywhere" checkable rather than asserted.
    let m = chat_messenger().await;
    let result = m
        .publish_from_conversation(
            CONVERSATION,
            "no-grants",
            &addr(OWNER, ChatLeaf::Out),
            "body",
            Urgency::Normal,
        )
        .await;
    assert!(matches!(result, PublishResult::MissingSender), "{result:?}");
}

#[tokio::test]
async fn an_unknown_app_slug_is_missing_sender() {
    let m = chat_messenger().await;
    let result = send(&m, "nonexistent", &addr(OWNER, ChatLeaf::Out)).await;
    assert!(matches!(result, PublishResult::MissingSender), "{result:?}");
}

#[tokio::test]
async fn a_channel_that_was_never_provisioned_is_unknown() {
    // Conversation 99 has no channels, so its record has nowhere to land — the
    // directory, not the ACL, is what refuses it (the prefix grant covers every
    // id by construction).
    let m = chat_messenger().await;
    let target = chat_address("chat", OWNER, ChatLeaf::Out, 99);
    let result = m
        .publish_from_conversation(99, OWNER, &target, "body", Urgency::Normal)
        .await;
    assert!(
        matches!(result, PublishResult::UnknownChannel(ref a) if *a == target),
        "{result:?}"
    );
}
