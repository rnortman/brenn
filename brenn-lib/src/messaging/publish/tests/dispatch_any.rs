//! Tests for `Messenger::publish_any` — the entry-point scheme dispatch above
//! the durable and ephemeral pipelines.

use super::super::*;
use super::{CountingRouter, build_messenger, test_app_config};
use crate::access::AppCapability;
use crate::access::acl::ChannelMatcher;
use crate::db::init_db_memory;
use crate::messaging::config::{
    Depth, EphemeralChannelEntry, MessagingGlobalConfig, NoiseLevel, ResolvedChannel,
    ResolvedMessagingConfig, Sink,
};
use crate::messaging::testutils::ephemeral_channel_entry;
use crate::messaging::{
    ChannelEntry, ChannelScheme, EphemeralBus, MessagingDirectory, WakeMin, WakeRouter,
    canonical_address,
};
use indexmap::IndexMap;
use std::sync::Arc;

const SOURCE: &str = "test-source";

/// A resolvable durable channel so the `publish_any` durable arm gets past the
/// channel-resolve gate to the layer-1 sender gate.
const DURABLE_CHANNEL: &str = "durable-chan";

fn eph_entry(name: &str) -> EphemeralChannelEntry {
    ephemeral_channel_entry(name, 8, 16)
}

/// App with `EphemeralPublish` grant + an `ephemeral_publish` matcher for
/// `channel`, and **no** `MessagingPublish` grant — genuinely
/// ephemeral-publish-only, as its name claims. Built with `None` messaging config
/// so `test_app_config` does not stamp its default `MessagingPublish`/`brenn_*`
/// grants; the ephemeral publish path reads only `policy`, never `messaging`.
fn ephemeral_publisher(slug: &str, channel: &str) -> crate::config::AppConfig {
    let mut cfg = test_app_config(slug, None, vec!["bob".to_string()]);
    cfg.policy.grants.insert(AppCapability::EphemeralPublish);
    cfg.policy
        .acls
        .ephemeral_publish
        .push(ChannelMatcher::Exact(channel.to_string()));
    cfg
}

/// App present but without any ephemeral grant (durable-only).
fn durable_only(slug: &str) -> crate::config::AppConfig {
    test_app_config(
        slug,
        Some(ResolvedMessagingConfig {
            send_budget: 100,
            subscriptions: vec![],
        }),
        vec!["bob".to_string()],
    )
}

/// A durable channel entry with no subscribers — enough for the directory to
/// resolve a `brenn:` target; the durable arm short-circuits at the sender gate
/// before any DB work, so the empty subscriber set is never consulted.
fn durable_channel_entry(name: &str) -> ChannelEntry {
    ChannelEntry {
        uuid: Uuid::new_v4(),
        address: canonical_address(name),
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
    }
}

/// A `Messenger` whose directory holds one resolvable durable channel and a
/// one-channel ephemeral bus, for exercising both dispatch arms. The db is never
/// touched by either arm in these tests.
fn ephemeral_messenger() -> Arc<Messenger> {
    let db = init_db_memory();
    let directory = Arc::new(MessagingDirectory::with_entries(vec![
        durable_channel_entry(DURABLE_CHANNEL),
    ]));

    let mut apps: IndexMap<String, crate::config::AppConfig> = IndexMap::new();
    apps.insert(
        "eph-pub".to_string(),
        ephemeral_publisher("eph-pub", "protobar"),
    );
    apps.insert("dur-only".to_string(), durable_only("dur-only"));
    let apps = Arc::new(apps);

    let router = Arc::new(CountingRouter::default());
    let messenger = Messenger::new(
        db,
        directory,
        Arc::from(SOURCE),
        apps,
        router as Arc<dyn WakeRouter>,
        MessagingGlobalConfig::default(),
    );
    let bus = EphemeralBus::new(vec![eph_entry("protobar")], Arc::from(SOURCE), 1024);
    messenger.with_ephemeral_bus(bus)
}

// --- Durable routing ------------------------------------------------------

#[tokio::test]
async fn brenn_address_routes_durable() {
    // A `brenn:` target routes to `publish`; the wrapped outcome matches what
    // `publish` returns directly (same Ok address).
    let (messenger, _cuuid, bob_conv, _alice_conv, _router) = build_messenger(0).await;
    let addr = canonical_address("pa-alice");

    let direct = messenger
        .publish(
            crate::messaging::PublishOrigin::Conversation { id: bob_conv },
            "pa-bob",
            &addr,
            "hi",
            Urgency::Normal,
            None,
            None,
            None,
        )
        .await;
    let PublishResult::Ok { address: da, .. } = direct else {
        panic!("expected direct publish Ok, got {direct:?}");
    };

    let any = messenger
        .publish_any(
            crate::messaging::PublishOrigin::Conversation { id: bob_conv },
            "pa-bob",
            &addr,
            "hi",
            Urgency::Normal,
            None,
            None,
            None,
        )
        .await;
    let AnyPublishResult::Durable(PublishResult::Ok { address: aa, .. }) = any else {
        panic!("expected Durable Ok, got {any:?}");
    };
    assert_eq!(da, aa);
    assert_eq!(aa, addr);
}

#[tokio::test]
async fn unknown_scheme_routes_durable_malformed() {
    // A scheme that is neither `ephemeral:` nor a resolvable `brenn:` channel
    // falls through to `publish`, which reports it as `MalformedAddress`.
    let (messenger, _cuuid, bob_conv, _alice_conv, _router) = build_messenger(0).await;
    let any = messenger
        .publish_any(
            crate::messaging::PublishOrigin::Conversation { id: bob_conv },
            "pa-bob",
            "weird:thing",
            "hi",
            Urgency::Normal,
            None,
            None,
            None,
        )
        .await;
    assert!(
        matches!(
            any,
            AnyPublishResult::Durable(PublishResult::MalformedAddress(ref a)) if a == "weird:thing"
        ),
        "got {any:?}",
    );
}

#[tokio::test]
async fn brenn_target_from_ephemeral_only_sender_is_durable_missing_sender() {
    // An `EphemeralPublish`-only app (no `MessagingPublish`) targeting a `brenn:`
    // address: `publish_any` routes to the durable arm, resolves the channel, then
    // fails the layer-1 `MessagingPublish` sender gate → `MissingSender`. This is
    // the mirror of `ephemeral_slug_without_grant_is_missing_sender`. This path is
    // tool-reachable: `BrennSend` is offered to ephemeral-only apps.
    let messenger = ephemeral_messenger();
    let addr = canonical_address(DURABLE_CHANNEL);
    let any = messenger
        .publish_any(
            crate::messaging::PublishOrigin::Conversation { id: 0 },
            "eph-pub",
            &addr,
            "hi",
            Urgency::Normal,
            None,
            None,
            None,
        )
        .await;
    assert!(
        matches!(any, AnyPublishResult::Durable(PublishResult::MissingSender)),
        "got {any:?}",
    );
}

// --- Ephemeral routing ----------------------------------------------------

#[tokio::test]
async fn ephemeral_address_routes_to_bus() {
    let messenger = ephemeral_messenger();
    let any = messenger
        .publish_any(
            crate::messaging::PublishOrigin::Conversation { id: 0 },
            "eph-pub",
            "ephemeral:protobar",
            "hi",
            Urgency::Normal,
            None,
            None,
            None,
        )
        .await;
    let AnyPublishResult::Ephemeral(EphemeralPublishResult::Ok { address, seq, .. }) = any else {
        panic!("expected Ephemeral Ok, got {any:?}");
    };
    assert_eq!(address, "ephemeral:protobar");
    assert_eq!(seq, 1);
    assert_eq!(messenger.ephemeral_bus().publish_count("protobar"), 1);
}

#[tokio::test]
async fn ephemeral_unknown_slug_is_missing_sender() {
    let messenger = ephemeral_messenger();
    let any = messenger
        .publish_any(
            crate::messaging::PublishOrigin::Conversation { id: 0 },
            "nope",
            "ephemeral:protobar",
            "hi",
            Urgency::Normal,
            None,
            None,
            None,
        )
        .await;
    assert!(
        matches!(
            any,
            AnyPublishResult::Ephemeral(EphemeralPublishResult::MissingSender)
        ),
        "got {any:?}",
    );
}

#[tokio::test]
async fn ephemeral_slug_without_grant_is_missing_sender() {
    // `dur-only` exists but holds no `EphemeralPublish` grant → layer-1 miss.
    let messenger = ephemeral_messenger();
    let any = messenger
        .publish_any(
            crate::messaging::PublishOrigin::Conversation { id: 0 },
            "dur-only",
            "ephemeral:protobar",
            "hi",
            Urgency::Normal,
            None,
            None,
            None,
        )
        .await;
    assert!(
        matches!(
            any,
            AnyPublishResult::Ephemeral(EphemeralPublishResult::MissingSender)
        ),
        "got {any:?}",
    );
}

#[tokio::test]
async fn ephemeral_reply_to_is_unsupported_option() {
    let messenger = ephemeral_messenger();
    let any = messenger
        .publish_any(
            crate::messaging::PublishOrigin::Conversation { id: 0 },
            "eph-pub",
            "ephemeral:protobar",
            "hi",
            Urgency::Normal,
            Some("brenn:whatever"),
            None,
            None,
        )
        .await;
    assert!(
        matches!(
            any,
            AnyPublishResult::Ephemeral(EphemeralPublishResult::UnsupportedOption { field })
                if field == "reply_to"
        ),
        "got {any:?}",
    );
}

#[tokio::test]
async fn ephemeral_deliver_after_is_unsupported_option() {
    let messenger = ephemeral_messenger();
    let any = messenger
        .publish_any(
            crate::messaging::PublishOrigin::Conversation { id: 0 },
            "eph-pub",
            "ephemeral:protobar",
            "hi",
            Urgency::Normal,
            None,
            Some(chrono::Utc::now()),
            None,
        )
        .await;
    assert!(
        matches!(
            any,
            AnyPublishResult::Ephemeral(EphemeralPublishResult::UnsupportedOption { field })
                if field == "deliver_after"
        ),
        "got {any:?}",
    );
}

#[tokio::test]
async fn ephemeral_delivery_deadline_is_unsupported_option() {
    let messenger = ephemeral_messenger();
    let any = messenger
        .publish_any(
            crate::messaging::PublishOrigin::Conversation { id: 0 },
            "eph-pub",
            "ephemeral:protobar",
            "hi",
            Urgency::Normal,
            None,
            None,
            Some(chrono::Utc::now()),
        )
        .await;
    assert!(
        matches!(
            any,
            AnyPublishResult::Ephemeral(EphemeralPublishResult::UnsupportedOption { field })
                if field == "delivery_deadline"
        ),
        "got {any:?}",
    );
}
