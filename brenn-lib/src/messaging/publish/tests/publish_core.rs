//! Core `publish()` gate + happy-path tests (design §"Tests: fanned into
//! `publish/tests/` by family": `publish/tests/publish_core.rs`).
//!
//! These exercise the front-door `Messenger::publish` path: the unknown-channel
//! / missing-sender / messaging-disabled / body-too-large / budget gates, the
//! off-stack-dispatch invariant (R1: publish inserts a pending row but never
//! delivers or eager-wakes inline), the deferred `deliver_after` behaviour, the
//! correctness-2 deferred-eager-wake regression guard, and the AC1 stored-sender
//! shape.
//!
//! Production items (`PublishResult`, `Urgency`, `Messenger`) are reached via
//! `use super::super::*;` (directly from `publish/mod.rs`); the cross-family
//! shared fixtures (`build_messenger`, `test_app_config`, `CountingRouter`) are
//! declared `pub(super)` in `tests/mod.rs` and pulled in by the named
//! `use super::{…};` below. `build_messenger` is shared with the `overflow`
//! family (`unbounded_push_depth_no_overflow`), so per design §"Tests: fanned…"
//! it lives in the harness rather than here.

use super::super::*;
use super::{CountingRouter, build_messenger, test_app_config};
use crate::access::{AppCapability, AppPolicy, acl::ChannelMatcher};
use crate::messaging::config::{Depth, NoiseLevel, ResolvedChannel, ResolvedMessagingConfig, Sink};
use crate::messaging::db::upsert_channels;
use crate::messaging::{
    ChannelEntry, ChannelScheme, MessagingDirectory, MessagingGlobalConfig, ParticipantId,
    SubscriberEntry, SubscriberEntryKind, Urgency, WakeMin, WakeRouter, canonical_address, db,
};
use chrono::Utc;
use indexmap::IndexMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;

#[tokio::test]
async fn publish_unknown_channel_returns_error() {
    let (m, _, _, _, _) = build_messenger(0).await;
    let result = m
        .publish(
            crate::messaging::PublishOrigin::Conversation { id: 1 },
            "pa-bob",
            "brenn:nope",
            "hi",
            Urgency::Low,
            None,
            None,
            None,
        )
        .await;
    assert!(matches!(result, PublishResult::UnknownChannel(_)));
}

#[tokio::test]
async fn publish_missing_sender_returns_error() {
    let (mut m, _, _, _, _) = build_messenger(0).await;
    // Replace pa-bob's app config without messaging. Clear the grants too:
    // messaging_enabled() reads the policy, so dropping only the `messaging`
    // field would leave the app still authorized.
    let new_apps = {
        let mut a = (*m.apps).clone();
        let pa_bob = a.get_mut("pa-bob").unwrap();
        pa_bob.messaging = None;
        pa_bob.policy = crate::access::AppPolicy::default();
        Arc::new(a)
    };
    Arc::get_mut(&mut m).unwrap().apps = new_apps;
    let result = m
        .publish(
            crate::messaging::PublishOrigin::Conversation { id: 1 },
            "pa-bob",
            &canonical_address("pa-alice"),
            "hi",
            Urgency::Low,
            None,
            None,
            None,
        )
        .await;
    assert!(matches!(result, PublishResult::MissingSender));
}

#[tokio::test]
async fn publish_messaging_disabled_returns_missing_sender() {
    // `[app.messaging]` block present but the app holds no messaging grant —
    // post-Phase-0, messaging_enabled() reads the policy, so a present-but-
    // ungranted block does not authorize (the new equivalent of the old
    // `enabled = false`). The gate must deny.
    let (mut m, _, _, _, _) = build_messenger(0).await;
    let new_apps = {
        let mut a = (*m.apps).clone();
        let pa_bob = a.get_mut("pa-bob").unwrap();
        pa_bob.messaging = Some(ResolvedMessagingConfig {
            send_budget: 100,
            subscriptions: vec![],
        });
        pa_bob.policy = crate::access::AppPolicy::default();
        Arc::new(a)
    };
    Arc::get_mut(&mut m).unwrap().apps = new_apps;
    let result = m
        .publish(
            crate::messaging::PublishOrigin::Conversation { id: 1 },
            "pa-bob",
            &canonical_address("pa-alice"),
            "hi",
            Urgency::Low,
            None,
            None,
            None,
        )
        .await;
    assert!(matches!(result, PublishResult::MissingSender));
}

#[tokio::test]
async fn publish_with_grant_but_no_messaging_block_uses_global_default_budget() {
    // Post-Phase-0 a sender may hold `MessagingPublish` with **no**
    // `[app.messaging]` block. `messaging_enabled()` then passes on the grant,
    // and the budget is read via `messaging_send_budget()`, which falls back to
    // the global default (`messaging_default_send_budget`, 100 in the fixture)
    // rather than panicking on the old `.messaging.expect(...)`. This pins that
    // fallback end-to-end through the publish gate (test review test-2).
    let (mut m, _, _, _, _) = build_messenger(1).await;
    let new_apps = {
        let mut a = (*m.apps).clone();
        let pa_bob = a.get_mut("pa-bob").unwrap();
        pa_bob.messaging = None;
        pa_bob.policy = crate::access::AppPolicy::default();
        pa_bob
            .policy
            .grants
            .insert(crate::access::AppCapability::MessagingPublish);
        // Phase-2 Seam A also requires a covering `brenn_publish` matcher; stamp a
        // universal one so this test continues to exercise the budget fallback,
        // not the new ACL gate.
        pa_bob
            .policy
            .acls
            .brenn_publish
            .push(crate::access::acl::ChannelMatcher::Prefix(String::new()));
        Arc::new(a)
    };
    Arc::get_mut(&mut m).unwrap().apps = new_apps;
    let result = m
        .publish(
            crate::messaging::PublishOrigin::Conversation { id: 1 },
            "pa-bob",
            &canonical_address("pa-alice"),
            "hi",
            Urgency::Low,
            None,
            None,
            None,
        )
        .await;
    // First publish on a fresh row using the fallback budget (global default
    // 100) ⇒ 99 remaining. Confirms the fallback resolves to the global default,
    // not 0 and not a panic.
    assert!(
        matches!(
            result,
            PublishResult::Ok {
                remaining_budget: Some(99),
                ..
            }
        ),
        "expected Ok with remaining_budget 99, got {result:?}"
    );
}

#[tokio::test]
async fn publish_too_large_body_rejected_without_budget_consumption() {
    // The body-size check must run before any budget decrement: an
    // oversized body must leave the remaining budget unchanged. We
    // force a visible budget by publishing once successfully first
    // (decrementing the row from 100 → 99), then assert the oversized
    // publish leaves it unchanged at 99.
    let (mut m, _, _, _, _) = build_messenger(1).await;
    // Decrement the budget once to make the row visible at 99.
    let prep = m
        .publish(
            crate::messaging::PublishOrigin::Conversation { id: 1 },
            "pa-bob",
            &canonical_address("pa-alice"),
            "ok",
            Urgency::Low,
            None,
            None,
            None,
        )
        .await;
    assert!(matches!(
        prep,
        PublishResult::Ok {
            remaining_budget: Some(99),
            ..
        }
    ));
    // Now lower max_body_bytes and publish an oversized body.
    Arc::get_mut(&mut m).unwrap().defaults = MessagingGlobalConfig {
        default_send_budget: 100,
        max_body_bytes: 10,
        ..MessagingGlobalConfig::default()
    };
    let result = m
        .publish(
            crate::messaging::PublishOrigin::Conversation { id: 1 },
            "pa-bob",
            &canonical_address("pa-alice"),
            "this body is way too long",
            Urgency::Low,
            None,
            None,
            None,
        )
        .await;
    assert!(matches!(result, PublishResult::BodyTooLarge { .. }));
    // Budget unchanged at 99 — oversized body was rejected
    // pre-decrement.
    let conn = m.db.lock().await;
    let remaining = db::read_send_budget(&conn, 1);
    assert_eq!(remaining, Some(99));
}

/// publish inserts the push row; no inline deliver or eager-wake (R1: off-stack dispatch).
#[tokio::test]
async fn publish_inserts_pending_row_no_inline_dispatch() {
    let (m, _, _, sub_conv, router) = build_messenger(1).await;
    let result = m
        .publish(
            crate::messaging::PublishOrigin::Conversation { id: 1 },
            "pa-bob",
            &canonical_address("pa-alice"),
            "hi",
            Urgency::Normal,
            None,
            None,
            None,
        )
        .await;
    assert!(matches!(result, PublishResult::Ok { .. }));
    // No inline deliver or eager-wake — dispatcher handles it later.
    let deliveries = router.deliveries.lock().await;
    assert_eq!(
        deliveries.len(),
        0,
        "publish must not call deliver inline — dispatch is off-stack (R1)"
    );
    drop(deliveries);
    assert_eq!(
        router.eager_wakes.load(Ordering::SeqCst),
        0,
        "publish must not call spawn_eager_wake inline — dispatch is off-stack (R1)"
    );
    // Pending row exists for the subscriber.
    let sub = ParticipantId::for_conversation(sub_conv);
    let rows = m.load_pending_pushes(&sub).await;
    assert_eq!(rows.len(), 1, "push row must exist");
}

#[tokio::test]
async fn publish_sleeping_bridge_with_none_wake_no_inline_wake() {
    let (m, _, _, _, router) = build_messenger(0).await;
    let _ = m
        .publish(
            crate::messaging::PublishOrigin::Conversation { id: 1 },
            "pa-bob",
            &canonical_address("pa-alice"),
            "hi",
            Urgency::Low,
            None,
            None,
            None,
        )
        .await;
    // Off-stack dispatch: no inline eager-wake regardless of wake_kind.
    assert_eq!(router.eager_wakes.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn publish_sleeping_bridge_with_none_wake_does_not_wake() {
    let (m, _, _, _, router) = build_messenger(0).await;
    let _ = m
        .publish(
            crate::messaging::PublishOrigin::Conversation { id: 1 },
            "pa-bob",
            &canonical_address("pa-alice"),
            "hi",
            Urgency::Low,
            None,
            None,
            None,
        )
        .await;
    assert_eq!(router.eager_wakes.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn publish_with_future_deliver_after_does_not_dispatch_now() {
    let (m, _, _, _, router) = build_messenger(1).await;
    let result = m
        .publish(
            crate::messaging::PublishOrigin::Conversation { id: 1 },
            "pa-bob",
            &canonical_address("pa-alice"),
            "hi",
            Urgency::Normal,
            None,
            Some(Utc::now() + chrono::Duration::seconds(60)),
            None,
        )
        .await;
    assert!(matches!(result, PublishResult::Ok { .. }));
    let deliveries = router.deliveries.lock().await;
    assert!(deliveries.is_empty());
}

/// A deferred publish holds no claim, and the claim its release mints carries
/// eager_wake=1 for a qualifying urgency (correctness-2 regression guard).
///
/// eager_wake=0 on that claim would leave it permanently invisible to the
/// dispatcher after release — silent deferred delivery loss. Resolving the wake
/// per released message is what keeps the mint honest: the batch carries
/// whatever urgencies were parked, not one urgency decided at park time.
#[tokio::test]
async fn released_deferred_claim_carries_eager_wake_for_qualifying_urgency() {
    let (m, _, _, sub_conv, _router) = build_messenger(1).await;
    let result = m
        .publish(
            crate::messaging::PublishOrigin::Conversation { id: 1 },
            "pa-bob",
            &canonical_address("pa-alice"),
            "hi",
            Urgency::Normal, // meets WakeMin::Normal threshold
            None,
            Some(Utc::now() + chrono::Duration::seconds(60)),
            None,
        )
        .await;
    assert!(matches!(result, PublishResult::Ok { .. }), "{result:?}");

    let sub = ParticipantId::for_conversation(sub_conv);
    async fn claim_wakes(m: &crate::messaging::Messenger, sub: &ParticipantId) -> Vec<bool> {
        let conn = m.db.lock().await;
        let mut stmt = conn
            .prepare(
                "SELECT pp.eager_wake FROM messaging_pending_pushes pp \
                 WHERE pp.target_subscriber = ?1 AND pp.delivered_at IS NULL",
            )
            .unwrap();
        let values: Vec<bool> = stmt
            .query_map(rusqlite::params![sub.as_str()], |r| r.get::<_, bool>(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        values
    }
    assert!(
        claim_wakes(&m, &sub).await.is_empty(),
        "a parked message is owed to nobody, so it holds no claim"
    );

    m.release_due_messages(Utc::now() + chrono::Duration::seconds(61))
        .await;
    let eager_wake_values = claim_wakes(&m, &sub).await;

    assert_eq!(
        eager_wake_values.len(),
        1,
        "release mints exactly one claim for the subscriber attached now"
    );
    assert!(
        eager_wake_values[0],
        "deferred row with qualifying urgency must have eager_wake=1; \
         eager_wake=0 would make the row invisible to dispatcher after release (correctness-2)"
    );
}

/// One release pass carries whatever urgencies were parked, so the wake is
/// resolved per message against the target's threshold — not once for the batch.
/// A threshold dropped on the way to the release would wake the fixture's
/// `WakeMin::Normal` subscriber for the low-urgency message too, which is
/// exactly the cost the threshold exists to prevent.
#[tokio::test]
async fn a_released_batch_resolves_each_messages_wake_against_the_threshold() {
    let (m, _, _, sub_conv, _router) = build_messenger(1).await;
    let release_at = Utc::now() + chrono::Duration::seconds(60);
    for (body, urgency) in [("quiet", Urgency::Low), ("loud", Urgency::Normal)] {
        let result = m
            .publish(
                crate::messaging::PublishOrigin::Conversation { id: 1 },
                "pa-bob",
                &canonical_address("pa-alice"),
                body,
                urgency,
                None,
                Some(release_at),
                None,
            )
            .await;
        assert!(matches!(result, PublishResult::Ok { .. }), "{result:?}");
    }

    m.release_due_messages(release_at + chrono::Duration::seconds(1))
        .await;

    let sub = ParticipantId::for_conversation(sub_conv);
    let wakes: Vec<(String, bool)> = {
        let conn = m.db.lock().await;
        let mut stmt = conn
            .prepare(
                "SELECT m.body, pp.eager_wake FROM messaging_pending_pushes pp \
                 JOIN messaging_messages m ON m.id = pp.message_id \
                 WHERE pp.target_subscriber = ?1 ORDER BY pp.id",
            )
            .unwrap();
        stmt.query_map(rusqlite::params![sub.as_str()], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
    };
    assert_eq!(
        wakes,
        vec![("quiet".to_string(), false), ("loud".to_string(), true)],
        "the below-threshold message waits for a natural wake; the qualifying one wakes"
    );
}

/// The fold-0 context feed is gated on the message holding a retention position,
/// and feeds that position rather than the row id.
///
/// Both halves are load-bearing and neither leaves evidence behind — the feed
/// writes no row. A parked message fed early hands an attached surface a message
/// before its release instant; a feed carrying the row id mints a wire cursor in
/// the wrong numbering domain, which the next resume compares against retention
/// positions. The park consumes a row id and no position, so the two diverge and
/// the assertion can tell them apart.
#[tokio::test]
async fn the_fold0_context_feed_skips_a_parked_message_and_feeds_its_retention_position() {
    let mut surface_policy = crate::access::AppPolicy::default();
    surface_policy
        .grants
        .insert(crate::access::AppCapability::MessagingSubscribe);
    surface_policy
        .acls
        .brenn_subscribe
        .push(crate::access::acl::ChannelMatcher::Prefix(String::new()));
    let (m, _, _, _, router) = super::build_messenger_with(
        1,
        vec![SubscriberEntry {
            kind: SubscriberEntryKind::Surface {
                slug: "deskbar".to_string(),
                instance: None,
            },
            push_depth: Depth::Bounded(0),
            retain_depth: Depth::Unbounded,
            noise: NoiseLevel::Silent,
            wake_min: None,
        }],
        std::collections::HashMap::from([("deskbar".to_string(), surface_policy)]),
    )
    .await;

    let publish = async |body: &str, deliver_after| {
        m.publish(
            crate::messaging::PublishOrigin::Conversation { id: 1 },
            "pa-bob",
            &canonical_address("pa-alice"),
            body,
            Urgency::Normal,
            None,
            deliver_after,
            None,
        )
        .await
    };

    // The parked message takes a row id and no retention position.
    let parked = publish("later", Some(Utc::now() + chrono::Duration::seconds(60))).await;
    assert!(matches!(parked, PublishResult::Ok { .. }), "{parked:?}");
    assert!(
        router.contexts.lock().await.is_empty(),
        "a parked message is not observable to a depth-0 feed before its release"
    );

    let live = publish("now", None).await;
    assert!(matches!(live, PublishResult::Ok { .. }), "{live:?}");
    assert_eq!(
        *router.contexts.lock().await,
        vec![("now".to_string(), 1)],
        "the feed carries the message's retention position, not its row id (2 here)"
    );
}

/// A surface policy authorizing delivery on any `brenn:` channel, and the
/// `Surface` subscriber it covers.
fn surface_subscriber(
    push_depth: Depth,
) -> (
    SubscriberEntry,
    std::collections::HashMap<String, AppPolicy>,
) {
    let mut policy = AppPolicy::default();
    policy.grants.insert(AppCapability::MessagingSubscribe);
    policy
        .acls
        .brenn_subscribe
        .push(ChannelMatcher::Prefix(String::new()));
    (
        SubscriberEntry {
            kind: SubscriberEntryKind::Surface {
                slug: "deskbar".to_string(),
                instance: None,
            },
            push_depth,
            retain_depth: Depth::Unbounded,
            noise: NoiseLevel::Silent,
            wake_min: None,
        },
        std::collections::HashMap::from([("deskbar".to_string(), policy)]),
    )
}

/// A push-enabled surface subscriber is served the committed envelope at the
/// commit, row-lessly, exactly as a fold-0 one is — a surface holds no position,
/// so nothing that walks positions can name it and the fan-out is its whole
/// delivery trigger. It goes through `deliver` rather than `deliver_context`:
/// its session can resume the suffix it misses, which the fold-0 feed cannot.
#[tokio::test]
async fn a_push_enabled_surface_subscriber_is_fanned_out_at_commit() {
    let (subscriber, policies) = surface_subscriber(Depth::Bounded(8));
    let (m, _, _, _, router) = super::build_messenger_with(1, vec![subscriber], policies).await;

    let published = m
        .publish(
            crate::messaging::PublishOrigin::Conversation { id: 1 },
            "pa-bob",
            &canonical_address("pa-alice"),
            "now",
            Urgency::Normal,
            None,
            None,
            None,
        )
        .await;
    assert!(
        matches!(published, PublishResult::Ok { .. }),
        "{published:?}"
    );

    let deliveries = router.deliveries.lock().await;
    let surface: Vec<_> = deliveries
        .iter()
        .filter(|(sub, _)| sub.as_str().starts_with("surface:"))
        .collect();
    assert_eq!(
        surface.len(),
        1,
        "one fan-out to the surface: {deliveries:?}"
    );
    assert!(surface[0].1.contains("now"));
    assert!(
        router.contexts.lock().await.is_empty(),
        "a push-enabled subscription is not a fold-0 context feed"
    );
}

/// A parked message reaches a surface when it enters retention, which is at the
/// release sweep — not at the publish that scheduled it, and not never. The
/// release is the parked message's commit, so it is also its fan-out.
#[tokio::test]
async fn a_released_message_is_fanned_out_to_surfaces_by_the_release_sweep() {
    let (subscriber, policies) = surface_subscriber(Depth::Bounded(8));
    let (m, _, _, _, router) = super::build_messenger_with(1, vec![subscriber], policies).await;

    let release_at = Utc::now() + chrono::Duration::seconds(60);
    let parked = m
        .publish(
            crate::messaging::PublishOrigin::Conversation { id: 1 },
            "pa-bob",
            &canonical_address("pa-alice"),
            "later",
            Urgency::Normal,
            None,
            Some(release_at),
            None,
        )
        .await;
    assert!(matches!(parked, PublishResult::Ok { .. }), "{parked:?}");
    assert!(
        router.deliveries.lock().await.is_empty(),
        "a parked message is observable to nobody before its release"
    );

    let sweep = m
        .release_due_messages(release_at + chrono::Duration::seconds(1))
        .await;
    assert_eq!(sweep.released, 1);
    let deliveries = router.deliveries.lock().await;
    let surface: Vec<_> = deliveries
        .iter()
        .filter(|(sub, _)| sub.as_str().starts_with("surface:"))
        .collect();
    assert_eq!(
        surface.len(),
        1,
        "the release fans out once: {deliveries:?}"
    );
    assert!(surface[0].1.contains("later"));
}

/// A `deliver_after` that is already past schedules nothing: the message enters
/// retention now, with claims, like any immediate publish. The park decision is
/// made once — a message claimed but holding no retention position is a state no
/// reader can serve.
#[tokio::test]
async fn a_past_deliver_after_publishes_immediately_with_a_retention_position() {
    let (m, _, _, sub_conv, _router) = build_messenger(1).await;
    let result = m
        .publish(
            crate::messaging::PublishOrigin::Conversation { id: 1 },
            "pa-bob",
            &canonical_address("pa-alice"),
            "already due",
            Urgency::Normal,
            None,
            Some(Utc::now() - chrono::Duration::seconds(5)),
            None,
        )
        .await;
    assert!(matches!(result, PublishResult::Ok { .. }), "{result:?}");

    let (deliver_after, retained_seq): (Option<String>, Option<i64>) = {
        let conn = m.db.lock().await;
        conn.query_row(
            "SELECT deliver_after, retained_seq FROM messaging_messages",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap()
    };
    assert!(
        deliver_after.is_none(),
        "a past release time is no schedule at all, got {deliver_after:?}"
    );
    assert!(
        retained_seq.is_some(),
        "an unparked message holds the retention position its claims are served against"
    );
    assert_eq!(
        m.load_pending_pushes(&ParticipantId::for_conversation(sub_conv))
            .await
            .len(),
        1,
        "the subscriber is owed it now"
    );
}

#[tokio::test]
async fn publish_budget_exhaustion() {
    let (mut m, _, _, _, _) = build_messenger(1).await;
    // Lower the global default to 1, then override the app's resolved
    // messaging budget.
    let new_apps = {
        let mut a = (*m.apps).clone();
        a.get_mut("pa-bob")
            .unwrap()
            .messaging
            .as_mut()
            .unwrap()
            .send_budget = 1;
        Arc::new(a)
    };
    Arc::get_mut(&mut m).unwrap().apps = new_apps;

    // First publish ok.
    let r = m
        .publish(
            crate::messaging::PublishOrigin::Conversation { id: 1 },
            "pa-bob",
            &canonical_address("pa-alice"),
            "1",
            Urgency::Low,
            None,
            None,
            None,
        )
        .await;
    assert!(matches!(
        r,
        PublishResult::Ok {
            remaining_budget: Some(0),
            ..
        }
    ));
    // Second publish exhausted.
    let r = m
        .publish(
            crate::messaging::PublishOrigin::Conversation { id: 1 },
            "pa-bob",
            &canonical_address("pa-alice"),
            "2",
            Urgency::Low,
            None,
            None,
            None,
        )
        .await;
    assert!(matches!(r, PublishResult::BudgetExhausted));
}

/// AC1: bus publish stamps `app:<slug>@<server>` as the stored sender value.
/// Regression guard: a revert to stamping msg_cfg.sender or any other value
/// would not be caught by tests that only check the PublishResult variant.
#[tokio::test]
async fn bus_publish_stores_structured_sender_in_db() {
    let (m, _, _, _, _) = build_messenger(0).await;
    let channel_addr = canonical_address("pa-alice");
    let result = m
        .publish(
            crate::messaging::PublishOrigin::Conversation { id: 1 },
            "pa-bob",
            &channel_addr,
            "hello",
            Urgency::Low,
            None,
            None,
            None,
        )
        .await;
    assert!(
        matches!(result, PublishResult::Ok { .. }),
        "expected Ok, got {result:?}"
    );
    // Verify the stored sender column: source is "test-source" (build_messenger default).
    let conn = m.db.lock().await;
    let stored_sender: String = conn
        .query_row(
            "SELECT sender FROM messaging_messages WHERE envelope_type = 'brenn' ORDER BY id DESC LIMIT 1",
            [],
            |row| row.get(0),
        )
        .expect("must find the published row");
    assert_eq!(
        stored_sender, "app:pa-bob@test-source",
        "bus publish must stamp app:<slug>@<server> as sender"
    );
}

// --- Phase-2 Seam A: per-channel `brenn_publish` ACL at the bus publish gate
//     (design §2.2 / §4 "Seam A — bus publish enforcement"). `pa-bob` publishes
//     to `brenn:pa-alice`; we vary `pa-bob`'s policy to drive each gate arm.

/// Replace `pa-bob`'s resolved policy with `policy`, leaving the rest of the
/// messenger fixture intact.
fn set_bob_policy(m: &mut Arc<Messenger>, policy: crate::access::AppPolicy) {
    let new_apps = {
        let mut a = (*m.apps).clone();
        a.get_mut("pa-bob").unwrap().policy = policy;
        Arc::new(a)
    };
    Arc::get_mut(m).unwrap().apps = new_apps;
}

/// Grant held but the target `brenn:` channel is not covered by any
/// `brenn_publish` matcher ⇒ `AclDenied` (layer-2 deny-by-default), not
/// `MissingSender`.
#[tokio::test]
async fn publish_acl_denied_when_channel_not_in_brenn_publish() {
    let (mut m, _, _, _, _) = build_messenger(1).await;
    // MessagingPublish granted, but the only matcher covers a *different*
    // channel, so `brenn:pa-alice` is out of scope.
    let mut p = crate::access::AppPolicy::default();
    p.grants
        .insert(crate::access::AppCapability::MessagingPublish);
    p.acls
        .brenn_publish
        .push(crate::access::acl::ChannelMatcher::Exact(
            "other".to_string(),
        ));
    set_bob_policy(&mut m, p);

    let result = m
        .publish(
            crate::messaging::PublishOrigin::Conversation { id: 1 },
            "pa-bob",
            &canonical_address("pa-alice"),
            "hi",
            Urgency::Low,
            None,
            None,
            None,
        )
        .await;
    match result {
        PublishResult::AclDenied(addr) => {
            assert_eq!(addr, canonical_address("pa-alice"), "carries the address");
        }
        other => panic!("expected AclDenied, got {other:?}"),
    }
}

/// The publish/subscribe split (§2.5): an app granted only `MessagingSubscribe`
/// can no longer publish — the gate returns `MissingSender` (layer-1 grant
/// absence), even though `messaging_enabled()` is still `true` for it.
#[tokio::test]
async fn publish_subscribe_only_app_is_missing_sender() {
    let (mut m, _, _, _, _) = build_messenger(1).await;
    // Subscribe-only: MessagingSubscribe but NOT MessagingPublish. A covering
    // brenn_publish matcher is present to prove the deny is on the grant, not the
    // ACL.
    let mut p = crate::access::AppPolicy::default();
    p.grants
        .insert(crate::access::AppCapability::MessagingSubscribe);
    p.acls
        .brenn_publish
        .push(crate::access::acl::ChannelMatcher::Prefix(String::new()));
    // Sanity: still a messaging participant.
    assert!(
        {
            let mut a = (*m.apps).clone();
            a.get_mut("pa-bob").unwrap().policy = p.clone();
            a.get("pa-bob").unwrap().messaging_enabled()
        },
        "subscribe-only app must still read as messaging_enabled()"
    );
    set_bob_policy(&mut m, p);

    let result = m
        .publish(
            crate::messaging::PublishOrigin::Conversation { id: 1 },
            "pa-bob",
            &canonical_address("pa-alice"),
            "hi",
            Urgency::Low,
            None,
            None,
            None,
        )
        .await;
    assert!(
        matches!(result, PublishResult::MissingSender),
        "subscribe-only app must be MissingSender at the publish gate, got {result:?}"
    );
}

/// Grant + a covering matcher both hold ⇒ `Ok`.
#[tokio::test]
async fn publish_allowed_with_grant_and_covering_matcher() {
    let (mut m, _, _, _, _) = build_messenger(1).await;
    let mut p = crate::access::AppPolicy::default();
    p.grants
        .insert(crate::access::AppCapability::MessagingPublish);
    p.acls
        .brenn_publish
        .push(crate::access::acl::ChannelMatcher::Exact(
            "pa-alice".to_string(),
        ));
    set_bob_policy(&mut m, p);

    let result = m
        .publish(
            crate::messaging::PublishOrigin::Conversation { id: 1 },
            "pa-bob",
            &canonical_address("pa-alice"),
            "hi",
            Urgency::Low,
            None,
            None,
            None,
        )
        .await;
    assert!(
        matches!(result, PublishResult::Ok { .. }),
        "grant + covering matcher must publish Ok, got {result:?}"
    );
}

/// An ACL-denied publish consumes **no** budget: the gate runs before the budget
/// decrement (§2.2 "validate before budget"). We prime the budget row to 99 with
/// a successful publish, tighten the policy out of scope, then assert the denied
/// publish leaves the row unchanged at 99.
#[tokio::test]
async fn publish_acl_denied_consumes_no_budget() {
    let (mut m, _, _, _, _) = build_messenger(1).await;
    // Prime: one successful publish (universal brenn_publish from the fixture)
    // drops the row 100 → 99.
    let prep = m
        .publish(
            crate::messaging::PublishOrigin::Conversation { id: 1 },
            "pa-bob",
            &canonical_address("pa-alice"),
            "ok",
            Urgency::Low,
            None,
            None,
            None,
        )
        .await;
    assert!(matches!(
        prep,
        PublishResult::Ok {
            remaining_budget: Some(99),
            ..
        }
    ));
    // Tighten: grant held but matcher no longer covers pa-alice.
    let mut p = crate::access::AppPolicy::default();
    p.grants
        .insert(crate::access::AppCapability::MessagingPublish);
    p.acls
        .brenn_publish
        .push(crate::access::acl::ChannelMatcher::Exact(
            "other".to_string(),
        ));
    set_bob_policy(&mut m, p);

    let denied = m
        .publish(
            crate::messaging::PublishOrigin::Conversation { id: 1 },
            "pa-bob",
            &canonical_address("pa-alice"),
            "denied",
            Urgency::Low,
            None,
            None,
            None,
        )
        .await;
    assert!(matches!(denied, PublishResult::AclDenied(_)));
    // Budget unchanged at 99 — the ACL deny ran pre-decrement.
    let conn = m.db.lock().await;
    let remaining = db::read_send_budget(&conn, 1);
    assert_eq!(
        remaining,
        Some(99),
        "ACL-denied publish must not consume budget"
    );
}

// The System-origin publisher contract (budget-skip, sender-grant, ACL, and
// body-size gates) is exercised through `publish_from_system` in
// `publish/tests/system.rs`; the App entry point now asserts its origin is not
// `System`, so those contracts no longer route through this `publish` arm.

// --- reply_to visibility gate: a `reply_to` is resolved only
//     after passing a visibility check (publish allowlist ∪ delivery scope), so
//     an out-of-visibility `reply_to` fails identically whether or not the
//     channel exists — closing the success/failure existence oracle.

/// A no-subscriber durable channel entry (enough for the directory to resolve a
/// `brenn:` target; the denial arms return before any subscriber is consulted).
fn bare_channel(name: &str) -> ChannelEntry {
    ChannelEntry {
        uuid: Uuid::new_v4(),
        address: canonical_address(name),
        description: None,
        resolved_channel: ResolvedChannel {
            send_rate: Default::default(),
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

/// A `Messenger` whose directory holds `brenn:pub-target` (sender publishes here)
/// and `brenn:secret` (resolvable, but outside the sender's publish AND delivery
/// scope). `pa-bob` may publish only to `pub-target` and may receive delivery
/// only on `subonly` (which is deliberately absent from the directory). Channels
/// are upserted so a completing `Ok` publish satisfies the message-row FKs.
async fn reply_to_gate_messenger() -> Arc<Messenger> {
    let db = crate::db::init_db_memory();
    let entries = vec![bare_channel("pub-target"), bare_channel("secret")];
    {
        let conn = db.lock().await;
        upsert_channels(&conn, &entries);
        // Seed the user + conversation the `Conversation { id: 1 }` origin's
        // send-budget row FKs onto, so an `Ok` publish lands (the visibility gate
        // these tests exercise is origin-independent).
        conn.execute(
            "INSERT INTO users (id, username, password_hash, created_at) \
                 VALUES (1, 'bob', 'h', '2024-01-01')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO conversations (id, user_id, status, app_slug, created_at, updated_at) \
                 VALUES (1, 1, 'active', 'pa-bob', '2024-01-01', '2024-01-01')",
            [],
        )
        .unwrap();
    }
    let directory = Arc::new(MessagingDirectory::with_entries(entries));

    let mut policy = AppPolicy::default();
    policy.grants.insert(AppCapability::MessagingPublish);
    policy.grants.insert(AppCapability::MessagingSubscribe);
    policy
        .acls
        .brenn_publish
        .push(ChannelMatcher::Exact("pub-target".to_string()));
    policy
        .acls
        .brenn_subscribe
        .push(ChannelMatcher::Exact("subonly".to_string()));
    let mut cfg = test_app_config(
        "pa-bob",
        Some(ResolvedMessagingConfig {
            send_budget: 100,
            subscriptions: vec![],
        }),
        vec![],
    );
    cfg.policy = policy;
    let mut apps: IndexMap<String, crate::config::AppConfig> = IndexMap::new();
    apps.insert("pa-bob".to_string(), cfg);

    let router = Arc::new(CountingRouter::default());
    Messenger::new(
        db,
        directory,
        Arc::from("test-source"),
        Arc::new(apps),
        router as Arc<dyn WakeRouter>,
        MessagingGlobalConfig::default(),
    )
}

/// A `reply_to` inside the publish allowlist that resolves publishes `Ok`.
#[tokio::test]
async fn reply_to_in_publish_scope_existing_is_ok() {
    let m = reply_to_gate_messenger().await;
    let r = m
        .publish(
            crate::messaging::PublishOrigin::Conversation { id: 1 },
            "pa-bob",
            &canonical_address("pub-target"),
            "hi",
            Urgency::Low,
            Some(&canonical_address("pub-target")),
            None,
            None,
        )
        .await;
    assert!(matches!(r, PublishResult::Ok { .. }), "{r:?}");
}

/// The oracle-closing property: an out-of-visibility `reply_to` yields the SAME
/// `AclDenied` variant whether the channel exists (`secret`, in the directory)
/// or not (`ghost`, absent) — the success/failure bit reveals no existence
/// information for channels outside the sender's scope.
#[tokio::test]
async fn reply_to_out_of_visibility_is_acl_denied_regardless_of_existence() {
    let m = reply_to_gate_messenger().await;
    let existing = m
        .publish(
            crate::messaging::PublishOrigin::Conversation { id: 1 },
            "pa-bob",
            &canonical_address("pub-target"),
            "hi",
            Urgency::Low,
            Some(&canonical_address("secret")),
            None,
            None,
        )
        .await;
    let missing = m
        .publish(
            crate::messaging::PublishOrigin::Conversation { id: 1 },
            "pa-bob",
            &canonical_address("pub-target"),
            "hi",
            Urgency::Low,
            Some(&canonical_address("ghost")),
            None,
            None,
        )
        .await;
    assert!(
        matches!(existing, PublishResult::AclDenied(ref a) if a == &canonical_address("secret")),
        "existing-but-out-of-scope reply_to must be AclDenied, got {existing:?}"
    );
    assert!(
        matches!(missing, PublishResult::AclDenied(ref a) if a == &canonical_address("ghost")),
        "missing-and-out-of-scope reply_to must be AclDenied, got {missing:?}"
    );
}

/// A `reply_to` inside the delivery (subscribe) scope but absent from the
/// directory surfaces as `UnknownChannel` — the sender legitimately sees the
/// existence bit for channels within its own scope.
#[tokio::test]
async fn reply_to_in_subscribe_scope_but_unresolved_is_unknown_channel() {
    let m = reply_to_gate_messenger().await;
    let r = m
        .publish(
            crate::messaging::PublishOrigin::Conversation { id: 1 },
            "pa-bob",
            &canonical_address("pub-target"),
            "hi",
            Urgency::Low,
            Some(&canonical_address("subonly")),
            None,
            None,
        )
        .await;
    assert!(
        matches!(r, PublishResult::UnknownChannel(ref a) if a == &canonical_address("subonly")),
        "in-scope-but-unresolved reply_to must be UnknownChannel, got {r:?}"
    );
}

/// A malformed `reply_to` still fails shape validation before the visibility
/// gate — `MalformedAddress`, not `AclDenied`.
#[tokio::test]
async fn reply_to_malformed_is_malformed_address() {
    let m = reply_to_gate_messenger().await;
    let r = m
        .publish(
            crate::messaging::PublishOrigin::Conversation { id: 1 },
            "pa-bob",
            &canonical_address("pub-target"),
            "hi",
            Urgency::Low,
            Some("brenn:bad name"),
            None,
            None,
        )
        .await;
    assert!(
        matches!(r, PublishResult::MalformedAddress(ref a) if a == "brenn:bad name"),
        "malformed reply_to must be MalformedAddress, got {r:?}"
    );
}

/// `signal_kind` / `denied_address` map each `PublishResult` variant to the
/// security-log tag and echoed address the intercept signal derives — the
/// non-denial arms (`Ok`, `BudgetExhausted`) emit no signal.
#[test]
fn signal_kind_and_denied_address_cover_every_variant() {
    use crate::obs::security::DenialKind;

    let ok = PublishResult::Ok {
        message_id: Uuid::new_v4(),
        address: canonical_address("x").to_string(),
        remaining_budget: Some(1),
    };
    assert_eq!(ok.signal_kind(), None);
    assert_eq!(ok.denied_address(), None);
    assert_eq!(PublishResult::BudgetExhausted.signal_kind(), None);
    assert_eq!(PublishResult::BudgetExhausted.denied_address(), None);

    let unknown = PublishResult::UnknownChannel("brenn:a".to_string());
    assert_eq!(unknown.signal_kind(), Some(DenialKind::UnknownChannel));
    assert_eq!(unknown.denied_address(), Some("brenn:a"));

    let malformed = PublishResult::MalformedAddress("brenn:b".to_string());
    assert_eq!(malformed.signal_kind(), Some(DenialKind::MalformedAddress));
    assert_eq!(malformed.denied_address(), Some("brenn:b"));

    let acl = PublishResult::AclDenied("brenn:c".to_string());
    assert_eq!(acl.signal_kind(), Some(DenialKind::AclDenied));
    assert_eq!(acl.denied_address(), Some("brenn:c"));

    assert_eq!(
        PublishResult::MissingSender.signal_kind(),
        Some(DenialKind::MissingSender)
    );
    assert_eq!(PublishResult::MissingSender.denied_address(), None);

    let too_large = PublishResult::BodyTooLarge { len: 9, max: 8 };
    assert_eq!(too_large.signal_kind(), Some(DenialKind::BodyTooLarge));
    assert_eq!(too_large.denied_address(), None);
}
