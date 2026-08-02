//! The conversation principal: a conversation publishing its own chat record and
//! token stream.
//!
//! What these pin is that the adapter is not privileged. It reaches its own
//! app's chat subtree because the derived policy says so, and it reaches nothing
//! else — the same two layers, in the same order, that gate every other
//! publisher.

use super::super::*;
use super::{CHAT_PREFIX, ChatFixture, test_app_config};
use brenn_envelope::chat::{ChatLeaf, chat_address, chat_bare_name};

use crate::config::LlmChatConfig;
use crate::messaging::config::ResolvedMessagingConfig;
use crate::messaging::testutils::test_channel_entry;
use indexmap::IndexMap;
use std::sync::Arc;

/// The app owning the fixture's conversation.
const OWNER: &str = "pa-bob";
/// A second app, whose chat subtree the fixture's conversation must not reach.
const NEIGHBOR: &str = "pa-alice";
const CONVERSATION: i64 = 1;

/// An app that authored nothing — an empty (deny-everything) policy — carrying
/// the derived chat-tree authority on its harness policy alone. The shape
/// resolution produces for an app with no messaging config of its own.
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
    cfg.chat_harness_policy = LlmChatConfig::default().harness_policy(slug);
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
async fn the_authored_policy_is_not_what_authorizes_the_adapter() {
    // Every app in this fixture authored an empty, deny-everything policy; the
    // record and stream land anyway. Authority is the harness policy, and the
    // separation is the point of the arm rather than an incidental fixture
    // detail.
    let m = chat_messenger().await;
    assert!(
        !m.apps[OWNER].policy.allows_brenn_publish(&chat_bare_name(
            CHAT_PREFIX,
            OWNER,
            ChatLeaf::Out,
            CONVERSATION
        )),
        "the fixture's authored policy must reach nothing"
    );
    assert!(matches!(
        send(&m, OWNER, &addr(OWNER, ChatLeaf::Out)).await,
        PublishResult::Ok { .. }
    ));
}

#[tokio::test]
async fn the_apps_own_llm_cannot_publish_into_its_chat_tree() {
    // The `App` principal — the shape a `BrennSend` tool call takes — under the
    // same app, same channels, same messenger. It resolves the authored policy,
    // which carries no chat matcher, so its own command channel and its own
    // record are both out of reach.
    let m = chat_messenger().await;
    for leaf in [ChatLeaf::In, ChatLeaf::Out] {
        let target = addr(OWNER, leaf);
        let result = m
            .publish(
                PublishOrigin::Conversation { id: CONVERSATION },
                OWNER,
                &target,
                "body",
                Urgency::Normal,
                None,
                None,
                None,
            )
            .await;
        assert!(
            matches!(
                result,
                PublishResult::MissingSender | PublishResult::AclDenied(_)
            ),
            "{target} must be refused to the app principal, got {result:?}"
        );
    }
}

#[tokio::test]
async fn without_the_harness_policy_there_is_no_bypass() {
    // The adapter's authority is entirely the owning app's harness policy. An
    // app that somehow resolved without it publishes nothing — this arm is what
    // makes "no ACL bypass anywhere" checkable rather than asserted.
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
async fn the_chat_tree_is_outside_the_apps_listing() {
    // What the app's LLM can enumerate is what its authored policy covers. The
    // chat channels exist in the directory and are not among them.
    let m = chat_messenger().await;
    let listed: Vec<String> = m
        .list_accessible_channels(OWNER)
        .into_iter()
        .map(|c| c.address)
        .collect();
    assert!(
        listed.iter().all(|a| !a.contains("chat.app.")),
        "no chat channel may appear in the app's listing, got {listed:?}"
    );
}

#[tokio::test]
async fn the_conversation_subscriber_reads_under_the_harness_policy() {
    // The read gate and the wake gate both go through `targets.policy`, so this
    // one predicate is where the split between the two principals shows up on
    // the read side.
    let m = chat_messenger().await;
    let conversation = SubscriberEntryKind::ChatConversation {
        app_slug: OWNER.to_string(),
        conversation_id: CONVERSATION,
    };
    assert!(m.channel_access_allowed(&conversation, &addr(OWNER, ChatLeaf::In)));
    assert!(m.channel_access_allowed(&conversation, &addr(OWNER, ChatLeaf::Stream)));
    assert!(!m.channel_access_allowed(&conversation, &addr(NEIGHBOR, ChatLeaf::In)));
    assert!(!m.channel_access_allowed(&conversation, "brenn:alerts.high"));

    // The same app as an ordinary `App` subscriber resolves the authored policy
    // and reaches none of it.
    let app = SubscriberEntryKind::App(OWNER.to_string());
    assert!(!m.channel_access_allowed(&app, &addr(OWNER, ChatLeaf::Out)));
    assert!(!m.channel_access_allowed(&app, &addr(OWNER, ChatLeaf::Stream)));
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
