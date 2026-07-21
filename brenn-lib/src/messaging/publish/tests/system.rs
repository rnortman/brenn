//! `Messenger::publish_from_system` gate + happy-path tests, and the
//! `System(component)` subscriber fan-out.
//!
//! The system-substrate durable-publish entry runs the identical `publish_core`
//! gate sequence as `publish`, differing only in the layer-1 authority source
//! (`system_policies`, not `apps`) and the stored principal
//! (`system:<component>`). There is **no** ACL bypass — a system component
//! publishes only where its code-built policy authorizes. These pin the shared
//! gates for the system arm (MissingSender, AclDenied, BodyTooLarge) and that a
//! `System(component)` subscriber is a valid, ACL-gated durable push target.

use super::super::*;
use super::CountingRouter;
use crate::access::acl::ChannelMatcher;
use crate::access::{AppCapability, AppPolicy};
use crate::db::init_db_memory;
use crate::messaging::config::{Depth, MessagingGlobalConfig, NoiseLevel, ResolvedChannel, Sink};
use crate::messaging::db::upsert_channels;
use crate::messaging::{
    ChannelEntry, ChannelScheme, MessagingDirectory, ParticipantId, SubscriberEntry,
    SubscriberEntryKind, Urgency, WakeMin, WakeRouter, canonical_address,
};
use indexmap::IndexMap;
use std::sync::Arc;
use uuid::Uuid;

const PUBLISHER: &str = "tool-executor";
const RECEIVER: &str = "results-sink";

/// A system publish policy: `MessagingPublish` grant + one `brenn_publish`
/// matcher. `matcher` chooses the scope (`Prefix("")` = universal).
fn system_publish_policy(matcher: ChannelMatcher) -> AppPolicy {
    let mut p = AppPolicy::default();
    p.grants.insert(AppCapability::MessagingPublish);
    p.acls.brenn_publish.push(matcher);
    p
}

/// A universal `brenn_subscribe` delivery policy for the system receiver, so the
/// fan-out row is not dropped at the delivery-time ACL gate.
fn system_receiver_policy() -> AppPolicy {
    let mut p = AppPolicy::default();
    p.grants.insert(AppCapability::MessagingSubscribe);
    p.acls
        .brenn_subscribe
        .push(ChannelMatcher::Prefix(String::new()));
    p
}

/// Build a `Messenger` with one `brenn:` channel carrying a single
/// `System(RECEIVER)` subscriber (so a system publish fans out an inspectable
/// pending row that also exercises the `resolve_push_targets` System arm), the
/// given publisher policy installed for `PUBLISHER`, and a configurable body
/// cap. `apps` is empty — system components are not apps.
async fn build_system_publish_messenger(
    publisher_policy: AppPolicy,
    max_body_bytes: usize,
) -> (Arc<Messenger>, String) {
    let db = init_db_memory();
    let channel_addr = canonical_address("tool-results-sink-ch");
    let entry = ChannelEntry {
        uuid: Uuid::new_v4(),
        address: channel_addr.clone(),
        description: None,
        resolved_channel: ResolvedChannel {
            push_depth: Depth::Unbounded,
            retain_depth: Depth::Unbounded,
            standing_retain_depth: Depth::Unbounded,
            noise: NoiseLevel::Silent,
            sink: Sink::Drop,
            wake_min: WakeMin::Normal,
        },
        subscribers: vec![SubscriberEntry {
            kind: SubscriberEntryKind::System(RECEIVER.to_string()),
            push_depth: Depth::Unbounded,
            retain_depth: Depth::Unbounded,
            noise: NoiseLevel::Silent,
            wake_min: None,
        }],
        transport_type: ChannelScheme::Brenn,
        mount: None,
    };
    {
        let conn = db.lock().await;
        upsert_channels(&conn, std::slice::from_ref(&entry));
    }
    let directory = Arc::new(MessagingDirectory::with_entries(vec![entry]));
    let mut system_policies = std::collections::HashMap::new();
    system_policies.insert(PUBLISHER.to_string(), publisher_policy);
    system_policies.insert(RECEIVER.to_string(), system_receiver_policy());
    let messenger = Messenger::new(
        db,
        directory,
        Arc::from("test"),
        Arc::new(IndexMap::new()),
        Arc::new(CountingRouter::default()) as Arc<dyn WakeRouter>,
        MessagingGlobalConfig {
            max_body_bytes,
            ..Default::default()
        },
    )
    .with_subscriber_registrations(crate::messaging::testutils::system_registrations(
        system_policies,
    ));
    (messenger, channel_addr)
}

/// Happy path: a granted, in-ACL system publish inserts a durable row stamped
/// with the `system:<component>` sender, fans out to the `System` subscriber,
/// and returns `Ok` with **no** remaining budget (System origin has no send
/// budget). Proves the publish-side System arm, the `resolve_push_targets`
/// System arm, and the `subscriber_policy` System arm together.
#[tokio::test]
async fn publish_from_system_ok_stamps_system_sender_and_fans_out() {
    let (m, addr) = build_system_publish_messenger(
        system_publish_policy(ChannelMatcher::Prefix(String::new())),
        65_536,
    )
    .await;

    let result = m
        .publish_from_system(PUBLISHER, &addr, "hello", Urgency::Normal, None)
        .await;
    assert!(
        matches!(
            result,
            PublishResult::Ok {
                remaining_budget: None,
                ..
            }
        ),
        "system publish is System origin: Ok with no budget, got {result:?}"
    );

    // The row fanned out to the System subscriber (resolve_push_targets +
    // subscriber_policy System arms).
    let rows = m
        .load_pending_pushes(&ParticipantId::for_system(RECEIVER))
        .await;
    assert_eq!(rows.len(), 1, "one pending push for the system subscriber");

    // The stored sender is the backend-derived system principal.
    let conn = m.db().lock().await;
    let sender: String = conn
        .query_row("SELECT sender FROM messaging_messages", [], |r| r.get(0))
        .unwrap();
    assert_eq!(
        sender,
        format!("system:{PUBLISHER}"),
        "sender must be system:<component>"
    );
}

/// A `System` publish targets a reserved `/`-namespaced channel
/// (`brenn:tool-results/<slug>`), which the attacker-shape charset gate rejects
/// for `App` principals. The system arm derives the bare name by prefix-strip
/// (host-trust, like `publish_from_wasm`) so the tool executor can deliver
/// results to its `/`-namespaced inbox — while the layer-2 ACL still gates. A
/// regression guard on the shape-gate exemption.
#[tokio::test]
async fn publish_from_system_reaches_reserved_slash_channel() {
    let db = init_db_memory();
    // A `/`-namespaced address: bare name `tool-results/sync` contains `/`, which
    // `well_formed_name` rejects.
    let channel_addr = canonical_address("tool-results/sync");
    let entry = ChannelEntry {
        uuid: Uuid::new_v4(),
        address: channel_addr.clone(),
        description: None,
        resolved_channel: ResolvedChannel {
            push_depth: Depth::Unbounded,
            retain_depth: Depth::Unbounded,
            standing_retain_depth: Depth::Unbounded,
            noise: NoiseLevel::Silent,
            sink: Sink::Drop,
            wake_min: WakeMin::Normal,
        },
        subscribers: vec![],
        transport_type: ChannelScheme::Brenn,
        mount: None,
    };
    {
        let conn = db.lock().await;
        upsert_channels(&conn, std::slice::from_ref(&entry));
    }
    let directory = Arc::new(MessagingDirectory::with_entries(vec![entry]));
    let mut system_policies = std::collections::HashMap::new();
    // Publish scope covers the reserved `tool-results/` namespace.
    system_policies.insert(
        PUBLISHER.to_string(),
        system_publish_policy(ChannelMatcher::Prefix("tool-results/".to_string())),
    );
    let messenger = Messenger::new(
        db,
        directory,
        Arc::from("test"),
        Arc::new(IndexMap::new()),
        Arc::new(CountingRouter::default()) as Arc<dyn WakeRouter>,
        MessagingGlobalConfig::default(),
    )
    .with_subscriber_registrations(crate::messaging::testutils::system_registrations(
        system_policies,
    ));

    let result = messenger
        .publish_from_system(PUBLISHER, &channel_addr, "hi", Urgency::Normal, None)
        .await;
    assert!(
        matches!(result, PublishResult::Ok { .. }),
        "system publish to a reserved /-namespaced channel must succeed, got {result:?}"
    );

    // And the layer-2 ACL still gates: a scope not covering `tool-results/` denies.
    let mut denying = std::collections::HashMap::new();
    denying.insert(
        PUBLISHER.to_string(),
        system_publish_policy(ChannelMatcher::Exact("other".to_string())),
    );
    let db2 = init_db_memory();
    let entry2 = ChannelEntry {
        uuid: Uuid::new_v4(),
        address: channel_addr.clone(),
        description: None,
        resolved_channel: ResolvedChannel {
            push_depth: Depth::Unbounded,
            retain_depth: Depth::Unbounded,
            standing_retain_depth: Depth::Unbounded,
            noise: NoiseLevel::Silent,
            sink: Sink::Drop,
            wake_min: WakeMin::Normal,
        },
        subscribers: vec![],
        transport_type: ChannelScheme::Brenn,
        mount: None,
    };
    {
        let conn = db2.lock().await;
        upsert_channels(&conn, std::slice::from_ref(&entry2));
    }
    let messenger2 = Messenger::new(
        db2,
        Arc::new(MessagingDirectory::with_entries(vec![entry2])),
        Arc::from("test"),
        Arc::new(IndexMap::new()),
        Arc::new(CountingRouter::default()) as Arc<dyn WakeRouter>,
        MessagingGlobalConfig::default(),
    )
    .with_subscriber_registrations(crate::messaging::testutils::system_registrations(denying));
    let denied = messenger2
        .publish_from_system(PUBLISHER, &channel_addr, "hi", Urgency::Normal, None)
        .await;
    assert!(
        matches!(denied, PublishResult::AclDenied(_)),
        "reserved-namespace exemption must not bypass the ACL, got {denied:?}"
    );
}

/// Charset-gate narrowing: the System principal's shape-gate exemption is scoped
/// to reserved `/`-namespaced channels only. A system publish to an *ordinary*
/// channel whose bare name carries a `/` (not a reserved namespace) is rejected
/// by the full charset gate as `MalformedAddress`, exactly like any other
/// principal — the exemption does not widen with each migrated system publisher.
#[tokio::test]
async fn publish_from_system_ordinary_slash_channel_is_malformed() {
    let (m, _) = build_system_publish_messenger(
        system_publish_policy(ChannelMatcher::Prefix(String::new())),
        1_000,
    )
    .await;
    // `ordinary/thing` is not a reserved namespace (`tools`/`tool-results`), so
    // the charset gate applies and rejects the `/` before directory resolution.
    let result = m
        .publish_from_system(
            PUBLISHER,
            &canonical_address("ordinary/thing"),
            "hi",
            Urgency::Normal,
            None,
        )
        .await;
    assert!(
        matches!(result, PublishResult::MalformedAddress(_)),
        "system publish to an ordinary /-bearing channel must be MalformedAddress \
         (the exemption is reserved-namespace only), got {result:?}"
    );
}

/// Layer-1: a system component whose policy lacks `MessagingPublish` is
/// `MissingSender`, even with a covering `brenn_publish` matcher present (the
/// deny is the grant, not the ACL).
#[tokio::test]
async fn publish_from_system_missing_grant_is_missing_sender() {
    let mut policy = AppPolicy::default();
    policy
        .acls
        .brenn_publish
        .push(ChannelMatcher::Prefix(String::new()));
    let (m, addr) = build_system_publish_messenger(policy, 65_536).await;

    let result = m
        .publish_from_system(PUBLISHER, &addr, "hello", Urgency::Normal, None)
        .await;
    assert!(
        matches!(result, PublishResult::MissingSender),
        "no MessagingPublish grant is MissingSender, got {result:?}"
    );
}

/// An unknown system component (no `system_policies` entry) is `MissingSender` —
/// fail-closed, never silently admitted (no ACL bypass for the substrate).
#[tokio::test]
async fn publish_from_system_unknown_component_is_missing_sender() {
    let (m, addr) = build_system_publish_messenger(
        system_publish_policy(ChannelMatcher::Prefix(String::new())),
        65_536,
    )
    .await;

    let result = m
        .publish_from_system("ghost", &addr, "hello", Urgency::Normal, None)
        .await;
    assert!(
        matches!(result, PublishResult::MissingSender),
        "unknown system component is MissingSender, got {result:?}"
    );
}

/// Layer-2: a granted system component publishing to a channel outside its
/// `brenn_publish` matchers is `AclDenied` — the executor cannot escape its
/// code-built publish scope.
#[tokio::test]
async fn publish_from_system_out_of_acl_is_acl_denied() {
    let (m, addr) = build_system_publish_messenger(
        system_publish_policy(ChannelMatcher::Exact("some-other-channel".to_string())),
        65_536,
    )
    .await;

    let result = m
        .publish_from_system(PUBLISHER, &addr, "hello", Urgency::Normal, None)
        .await;
    assert!(
        matches!(result, PublishResult::AclDenied(_)),
        "channel outside brenn_publish scope is AclDenied, got {result:?}"
    );
}

/// The shared body-size gate fires on the system arm: a body over the cap is
/// `BodyTooLarge`.
#[tokio::test]
async fn publish_from_system_body_too_large() {
    let (m, addr) = build_system_publish_messenger(
        system_publish_policy(ChannelMatcher::Prefix(String::new())),
        4,
    )
    .await;

    let result = m
        .publish_from_system(PUBLISHER, &addr, "abcde", Urgency::Normal, None)
        .await;
    assert!(
        matches!(result, PublishResult::BodyTooLarge { len: 5, max: 4 }),
        "over-cap body is BodyTooLarge, got {result:?}"
    );
}
