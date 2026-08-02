//! The peer roles the chat tree's shape exists to express, at the gate.
//!
//! With the conversation id as the terminal segment, every role is grantable at
//! both grains — one conversation and every conversation of an app —
//! independently per direction, using nothing but `Exact` and segment-boundary
//! `Prefix`. That is a property of authored operator policy, not of the derived
//! authority a conversation holds over its own subtree (`conversation.rs` covers
//! that one). Here each role is written as an operator would author it and then
//! run through the real publish ladder and the real read-time access gate.
//!
//! The two refusal shapes are both roles, not accidents: a role holding no
//! publish grant at all is refused at layer 1 (`MissingSender`), and a role
//! holding the grant but no matcher covering the target is refused at layer 2
//! (`AclDenied`). Between them the two layers are both exercised.

use super::super::*;
use super::{CHAT_PREFIX as PREFIX, ChatFixture, test_app_config};
use crate::access::acl::{AclSet, ChannelMatcher};
use crate::access::{AppCapability, AppPolicy};
use crate::messaging::SubscriberEntryKind;
use crate::messaging::config::ResolvedMessagingConfig;
use brenn_envelope::chat::{ChatLeaf, chat_address, chat_bare_name, chat_leaf_prefix};
use indexmap::IndexMap;
use std::sync::Arc;

/// The app whose conversations the peers below drive or watch.
const OWNER: &str = "pa-bob";
/// The one conversation a conversation-grain role is granted over.
const MINE: i64 = 1;
/// A sibling conversation of the same app: what a conversation-grain grant must
/// not reach and a fleet-grain one must.
const SIBLING: i64 = 2;

/// A peer app: the role it plays, the policy an operator would author for it,
/// and the conversation its own publishes are attributed to.
struct Peer {
    slug: &'static str,
    origin_conversation: i64,
    policy: AppPolicy,
}

fn exact(leaf: ChatLeaf, conversation_id: i64) -> ChannelMatcher {
    ChannelMatcher::Exact(chat_bare_name(PREFIX, OWNER, leaf, conversation_id))
}

fn fleet(leaf: ChatLeaf) -> ChannelMatcher {
    ChannelMatcher::Prefix(chat_leaf_prefix(PREFIX, OWNER, leaf))
}

fn policy(acls: AclSet, grants: &[AppCapability]) -> AppPolicy {
    let mut policy = AppPolicy::with_grants(grants);
    policy.acls = acls;
    policy
}

/// May chat with one conversation: publish its commands and its pre-warm, read
/// its record and its stream. Four `Exact` entries, one per direction per
/// delivery class.
fn driver() -> Peer {
    Peer {
        slug: "driver",
        origin_conversation: 11,
        policy: policy(
            AclSet {
                brenn_publish: vec![exact(ChatLeaf::In, MINE)],
                ephemeral_publish: vec![exact(ChatLeaf::Wake, MINE)],
                brenn_subscribe: vec![exact(ChatLeaf::Out, MINE)],
                ephemeral_subscribe: vec![exact(ChatLeaf::Stream, MINE)],
                ..AclSet::default()
            },
            &[
                AppCapability::MessagingPublish,
                AppCapability::EphemeralPublish,
                AppCapability::MessagingSubscribe,
                AppCapability::EphemeralSubscribe,
            ],
        ),
    }
}

/// May watch one conversation and nothing more: the driver's two subscribe
/// entries, and no publish authority of any kind.
fn observer() -> Peer {
    Peer {
        slug: "observer",
        origin_conversation: 12,
        policy: policy(
            AclSet {
                brenn_subscribe: vec![exact(ChatLeaf::Out, MINE)],
                ephemeral_subscribe: vec![exact(ChatLeaf::Stream, MINE)],
                ..AclSet::default()
            },
            &[
                AppCapability::MessagingSubscribe,
                AppCapability::EphemeralSubscribe,
            ],
        ),
    }
}

/// May drive every conversation of the app — the voice-gateway shape — without
/// being able to write any of their records.
fn fleet_driver() -> Peer {
    Peer {
        slug: "fleet-driver",
        origin_conversation: 13,
        policy: policy(
            AclSet {
                brenn_publish: vec![fleet(ChatLeaf::In)],
                ephemeral_publish: vec![fleet(ChatLeaf::Wake)],
                brenn_subscribe: vec![fleet(ChatLeaf::Out)],
                ephemeral_subscribe: vec![fleet(ChatLeaf::Stream)],
                ..AclSet::default()
            },
            &[
                AppCapability::MessagingPublish,
                AppCapability::EphemeralPublish,
                AppCapability::MessagingSubscribe,
                AppCapability::EphemeralSubscribe,
            ],
        ),
    }
}

fn addr(leaf: ChatLeaf, conversation_id: i64) -> String {
    chat_address(PREFIX, OWNER, leaf, conversation_id)
}

/// A messenger holding both conversations' four chat channels and the peers,
/// each with the policy its role authors.
async fn peer_messenger(peers: &[&Peer]) -> Arc<Messenger> {
    let mut apps: IndexMap<String, crate::config::AppConfig> = IndexMap::new();
    for peer in peers {
        let mut cfg = test_app_config(
            peer.slug,
            Some(ResolvedMessagingConfig {
                send_budget: 100,
                subscriptions: vec![],
            }),
            vec!["bob".to_string()],
        );
        cfg.policy = peer.policy.clone();
        apps.insert(peer.slug.to_string(), cfg);
    }

    ChatFixture {
        // Two conversations of the one owner: what a conversation-grain grant
        // must reach, and the sibling it must not.
        leaves: [MINE, SIBLING]
            .iter()
            .map(|id| (OWNER.to_string(), *id))
            .collect(),
        // A peer publishes as its own conversation, so each needs the row its
        // publish is attributed to.
        conversations: peers
            .iter()
            .map(|peer| (peer.origin_conversation, peer.slug.to_string()))
            .collect(),
        apps,
        extra_durable: vec![],
    }
    .build()
    .await
}

/// One publish by a peer, as its own LLM's bus tool would make it.
async fn peer_publish(m: &Messenger, peer: &Peer, addr: &str) -> PublishResult {
    m.publish(
        crate::messaging::PublishOrigin::Conversation {
            id: peer.origin_conversation,
        },
        peer.slug,
        addr,
        "body",
        Urgency::Normal,
        None,
        None,
        None,
    )
    .await
}

/// What the read-time gate answers a peer asking to see a channel.
fn peer_reads(m: &Messenger, peer: &Peer, addr: &str) -> bool {
    m.channel_access_allowed(&SubscriberEntryKind::App(peer.slug.to_string()), addr)
}

#[tokio::test]
async fn a_driver_drives_its_conversation_and_cannot_forge_its_record() {
    let driver = driver();
    let m = peer_messenger(&[&driver]).await;

    for leaf in [ChatLeaf::In, ChatLeaf::Wake] {
        let target = addr(leaf, MINE);
        let result = peer_publish(&m, &driver, &target).await;
        assert!(
            matches!(result, PublishResult::Ok { .. }),
            "a driver publishes {target}, got {result:?}"
        );
    }

    // The point of splitting the leaves: "may chat" is granted without "may
    // write the record", per delivery class.
    for leaf in [ChatLeaf::Out, ChatLeaf::Stream] {
        let target = addr(leaf, MINE);
        let result = peer_publish(&m, &driver, &target).await;
        assert!(
            matches!(result, PublishResult::AclDenied(ref a) if *a == target),
            "a driver must not write {target}, got {result:?}"
        );
    }

    assert!(peer_reads(&m, &driver, &addr(ChatLeaf::Out, MINE)));
    assert!(peer_reads(&m, &driver, &addr(ChatLeaf::Stream, MINE)));
}

#[tokio::test]
async fn a_conversation_grain_grant_stops_at_its_own_conversation() {
    // The id is the terminal segment, so an `Exact` entry names one
    // conversation and cannot spread to a sibling — the whole reason for the
    // segment order.
    let driver = driver();
    let m = peer_messenger(&[&driver]).await;

    let target = addr(ChatLeaf::In, SIBLING);
    let result = peer_publish(&m, &driver, &target).await;
    assert!(
        matches!(result, PublishResult::AclDenied(ref a) if *a == target),
        "{target} belongs to another conversation, got {result:?}"
    );
    assert!(!peer_reads(&m, &driver, &addr(ChatLeaf::Out, SIBLING)));
    assert!(!peer_reads(&m, &driver, &addr(ChatLeaf::Stream, SIBLING)));
}

#[tokio::test]
async fn an_observer_reads_the_record_and_cannot_drive_it() {
    let observer = observer();
    let m = peer_messenger(&[&observer]).await;

    assert!(peer_reads(&m, &observer, &addr(ChatLeaf::Out, MINE)));
    assert!(peer_reads(&m, &observer, &addr(ChatLeaf::Stream, MINE)));

    // No publish grant of either class, so the refusal is layer 1's: the
    // observer is not a publisher on this bus at all, never mind on which
    // channel.
    for leaf in [ChatLeaf::In, ChatLeaf::Wake] {
        let result = peer_publish(&m, &observer, &addr(leaf, MINE)).await;
        assert!(
            matches!(result, PublishResult::MissingSender),
            "an observer must not drive {leaf:?}, got {result:?}"
        );
    }
}

#[tokio::test]
async fn a_fleet_driver_reaches_every_conversation_on_the_leaves_it_holds() {
    let fleet = fleet_driver();
    let m = peer_messenger(&[&fleet]).await;

    for id in [MINE, SIBLING] {
        for leaf in [ChatLeaf::In, ChatLeaf::Wake] {
            let target = addr(leaf, id);
            let result = peer_publish(&m, &fleet, &target).await;
            assert!(
                matches!(result, PublishResult::Ok { .. }),
                "a fleet driver publishes {target}, got {result:?}"
            );
        }
        // Fleet grain widens the id, never the leaf: one entry cannot bundle
        // "may drive every conversation" with "may forge every record".
        for leaf in [ChatLeaf::Out, ChatLeaf::Stream] {
            let target = addr(leaf, id);
            let result = peer_publish(&m, &fleet, &target).await;
            assert!(
                matches!(result, PublishResult::AclDenied(ref a) if *a == target),
                "a fleet driver must not write {target}, got {result:?}"
            );
        }
        assert!(peer_reads(&m, &fleet, &addr(ChatLeaf::Out, id)));
        assert!(peer_reads(&m, &fleet, &addr(ChatLeaf::Stream, id)));
    }
}
