//! `Messenger::publish_from_surface` gate + happy-path tests.
//!
//! The surface durable-publish entry runs the identical `publish_core` gate
//! sequence as `publish`, differing only in the layer-1 authority source
//! (`surface_policies`, not `apps`) and the stored principal (`surface:<slug>`).
//! These pin that the shared gates fire for the surface arm (MissingSender on a
//! missing grant, AclDenied out of ACL scope, BodyTooLarge over the cap) and
//! that a successful publish stamps the `surface:<slug>` sender with no
//! per-conversation remaining budget (System origin), and that the per-surface
//! send budget (R3) bounds durable surface publishes and is reconnect-resistant.
//! Session-arm outcome mapping (the panic invariants) and the full
//! parked/drained integration live elsewhere.

use super::super::*;
use super::CountingRouter;
use crate::access::acl::ChannelMatcher;
use crate::access::{AppCapability, AppPolicy};
use crate::db::init_db_memory;
use crate::messaging::config::{
    Depth, MessagingGlobalConfig, NoiseLevel, ResolvedChannel, Sink, SurfacePrincipalBudgets,
    SurfaceSendBudget,
};
use crate::messaging::db::upsert_channels;
use crate::messaging::{
    ChannelEntry, ChannelScheme, MessagingDirectory, ParticipantId, SubscriberEntry,
    SubscriberEntryKind, Urgency, WakeMin, WakeRouter, canonical_address,
};
use indexmap::IndexMap;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

/// One surface's principal set at the default budget: the kernel grain plus
/// `instances`, the shape `ResolvedSurface::principal_send_budgets` produces for
/// a surface whose components declare no override.
fn default_principals(instances: &[&str]) -> SurfacePrincipalBudgets {
    std::iter::once((None, SurfaceSendBudget::default()))
        .chain(
            instances
                .iter()
                .map(|i| (Some(i.to_string()), SurfaceSendBudget::default())),
        )
        .collect()
}

/// A surface publish policy: `MessagingPublish` grant + one `brenn_publish`
/// matcher. `matcher` chooses the scope (`Prefix("")` = universal).
fn surface_publish_policy(matcher: ChannelMatcher) -> AppPolicy {
    let mut p = AppPolicy::default();
    p.grants.insert(AppCapability::MessagingPublish);
    p.acls.brenn_publish.push(matcher);
    p
}

/// A universal `brenn_subscribe` delivery policy for the Wasm receiver, so the
/// fan-out row is not dropped at the delivery-time ACL gate.
fn wasm_receiver_policy() -> AppPolicy {
    let mut p = AppPolicy::default();
    p.grants.insert(AppCapability::MessagingSubscribe);
    p.acls
        .brenn_subscribe
        .push(ChannelMatcher::Prefix(String::new()));
    p
}

/// The single `brenn:` channel every surface-publish fixture publishes onto,
/// with the given `subscribers` (a Wasm receiver for the fan-out tests, none for
/// the budget tests). Fixed unbounded/silent config; the tests exercise the
/// publish gate, not channel economics.
fn surface_channel_entry(subscribers: Vec<SubscriberEntry>) -> ChannelEntry {
    ChannelEntry {
        uuid: Uuid::new_v4(),
        address: canonical_address("surface-out-ch"),
        description: None,
        resolved_channel: ResolvedChannel {
            push_depth: Depth::Unbounded,
            retain_depth: Depth::Unbounded,
            standing_retain_depth: Depth::Unbounded,
            noise: NoiseLevel::Silent,
            sink: Sink::Drop,
            wake_min: WakeMin::Normal,
        },
        subscribers,
        transport_type: ChannelScheme::Brenn,
        mount: None,
    }
}

/// The component instances every fixture surface declares. Two of them, because
/// the property most of this suite exists to pin — one principal's flood does
/// not touch another's — needs a sibling to be visible at all.
const FIXTURE_INSTANCES: [&str; 2] = ["clock", "todos"];

/// The shared spine of the surface-publish fixtures: persist `entry`, install the
/// given Wasm + surface policies and full send budgets for every surface
/// principal, and return the `Messenger` plus the channel address. Each caller
/// assembles the channel entry (subscribers or none) and policy shapes it needs,
/// so the `Messenger::new` → registration → budget-install sequence has one home
/// and a new builder step lands in both suites at once. The budgeted principals
/// are derived from the surface policies, so every registered surface is bounded
/// at both grains — its kernel identity and each declared component instance (an
/// unbudgeted principal is the panic path, tested separately).
///
/// `principals` is the principal set — with each one's budget — handed to the
/// installer for every surface, in the shape
/// `ResolvedSurface::principal_send_budgets` produces. It is a parameter rather
/// than always the defaults over [`FIXTURE_INSTANCES`] so a test can drive the
/// installer with a set of its own shape, or with a declared override's
/// parameters.
async fn assemble_surface_messenger(
    entry: ChannelEntry,
    wasm_policies: std::collections::HashMap<String, AppPolicy>,
    surface_policies: std::collections::HashMap<String, AppPolicy>,
    max_body_bytes: usize,
    principals: &[(Option<String>, SurfaceSendBudget)],
) -> (Arc<Messenger>, String) {
    let db = init_db_memory();
    let channel_addr = entry.address.clone();
    {
        let conn = db.lock().await;
        upsert_channels(&conn, std::slice::from_ref(&entry));
    }
    let budget_principals: Vec<(String, SurfacePrincipalBudgets)> = surface_policies
        .keys()
        .cloned()
        .map(|slug| (slug, principals.to_vec()))
        .collect();
    let directory = Arc::new(MessagingDirectory::with_entries(vec![entry]));
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
    .with_subscriber_registrations(crate::messaging::testutils::wasm_registrations(
        wasm_policies,
    ))
    .with_subscriber_registrations(crate::messaging::testutils::surface_registrations(
        surface_policies,
    ))
    .with_surface_send_budgets(budget_principals);
    (messenger, channel_addr)
}

/// Build a `Messenger` with one `brenn:` channel carrying a single Wasm
/// subscriber (so a surface publish fans out an inspectable pending row), the
/// given surface policy installed for `surface_slug`, and a configurable body
/// cap.
async fn build_surface_publish_messenger(
    surface_slug: &str,
    surface_policy: AppPolicy,
    max_body_bytes: usize,
) -> (Arc<Messenger>, String) {
    let receiver_slug = "surface-out-receiver";
    let entry = surface_channel_entry(vec![SubscriberEntry {
        kind: SubscriberEntryKind::Wasm(receiver_slug.to_string()),
        push_depth: Depth::Unbounded,
        retain_depth: Depth::Unbounded,
        noise: NoiseLevel::Silent,
        wake_min: None,
    }]);
    let mut wasm_policies = std::collections::HashMap::new();
    wasm_policies.insert(receiver_slug.to_string(), wasm_receiver_policy());
    let mut surface_policies = std::collections::HashMap::new();
    surface_policies.insert(surface_slug.to_string(), surface_policy);
    assemble_surface_messenger(
        entry,
        wasm_policies,
        surface_policies,
        max_body_bytes,
        &default_principals(&FIXTURE_INSTANCES),
    )
    .await
}

/// Happy path: a granted, in-ACL surface publish inserts a durable row stamped
/// with the `surface:<slug>` sender, fans out to the subscriber, and returns
/// `Ok` with **no** remaining budget (System origin has no send budget).
#[tokio::test]
async fn publish_from_surface_ok_stamps_surface_sender_no_budget() {
    let (m, addr) = build_surface_publish_messenger(
        "durabar",
        surface_publish_policy(ChannelMatcher::Prefix(String::new())),
        65_536,
    )
    .await;

    let result = m
        .publish_from_surface("durabar", None, &addr, "hello", Urgency::Normal)
        .await;
    assert!(
        matches!(
            result,
            PublishResult::Ok {
                remaining_budget: None,
                ..
            }
        ),
        "surface publish is System origin: Ok with no budget, got {result:?}"
    );

    // The row fanned out to the Wasm subscriber.
    let rows = m
        .load_pending_pushes(&ParticipantId::for_wasm("surface-out-receiver"))
        .await;
    assert_eq!(rows.len(), 1, "one pending push for the subscriber");

    // The stored sender is the backend-derived surface principal.
    let conn = m.db().lock().await;
    let sender: String = conn
        .query_row("SELECT sender FROM messaging_messages", [], |r| r.get(0))
        .unwrap();
    assert_eq!(sender, "surface:durabar", "sender must be surface:<slug>");
}

/// Layer-1: a surface whose policy lacks `MessagingPublish` is `MissingSender`,
/// even with a covering `brenn_publish` matcher present (the deny is the grant,
/// not the ACL) — mirrors the app/System-origin grant gate.
#[tokio::test]
async fn publish_from_surface_missing_grant_is_missing_sender() {
    let mut policy = AppPolicy::default();
    // Covering matcher present, but no MessagingPublish grant.
    policy
        .acls
        .brenn_publish
        .push(ChannelMatcher::Prefix(String::new()));
    let (m, addr) = build_surface_publish_messenger("durabar", policy, 65_536).await;

    let result = m
        .publish_from_surface("durabar", None, &addr, "hello", Urgency::Normal)
        .await;
    assert!(
        matches!(result, PublishResult::MissingSender),
        "no MessagingPublish grant is MissingSender, got {result:?}"
    );
}

/// An unknown surface slug (no `surface_policies` entry) is `MissingSender` —
/// fail-closed, never silently admitted.
#[tokio::test]
async fn publish_from_surface_unknown_slug_is_missing_sender() {
    let (m, addr) = build_surface_publish_messenger(
        "durabar",
        surface_publish_policy(ChannelMatcher::Prefix(String::new())),
        65_536,
    )
    .await;

    let result = m
        .publish_from_surface("ghost", None, &addr, "hello", Urgency::Normal)
        .await;
    assert!(
        matches!(result, PublishResult::MissingSender),
        "unknown surface slug is MissingSender, got {result:?}"
    );
}

/// Layer-2: a granted surface publishing to a channel outside its
/// `brenn_publish` matchers is `AclDenied`.
#[tokio::test]
async fn publish_from_surface_out_of_acl_is_acl_denied() {
    let (m, addr) = build_surface_publish_messenger(
        "durabar",
        surface_publish_policy(ChannelMatcher::Exact("some-other-channel".to_string())),
        65_536,
    )
    .await;

    let result = m
        .publish_from_surface("durabar", None, &addr, "hello", Urgency::Normal)
        .await;
    assert!(
        matches!(result, PublishResult::AclDenied(_)),
        "channel outside brenn_publish scope is AclDenied, got {result:?}"
    );
}

/// The shared body-size gate fires on the surface arm: a body over the cap is
/// `BodyTooLarge` (an outcome, never a panic — the session maps it straight
/// through).
#[tokio::test]
async fn publish_from_surface_body_too_large() {
    let (m, addr) = build_surface_publish_messenger(
        "durabar",
        surface_publish_policy(ChannelMatcher::Prefix(String::new())),
        4,
    )
    .await;

    let result = m
        .publish_from_surface("durabar", None, &addr, "abcde", Urgency::Normal)
        .await;
    assert!(
        matches!(result, PublishResult::BodyTooLarge { len: 5, max: 4 }),
        "over-cap body is BodyTooLarge, got {result:?}"
    );
}

/// Build a `Messenger` whose one `brenn:` channel has no subscribers (the send
/// budget is checked before fan-out, so a subscriber is irrelevant here), with a
/// universal-publish surface policy + a full send budget installed for each of
/// `slugs`. Lets a test drive per-slug budget independence.
async fn build_multi_surface_publish_messenger(slugs: &[&str]) -> (Arc<Messenger>, String) {
    build_multi_surface_publish_messenger_with_instances(slugs, &FIXTURE_INSTANCES).await
}

/// [`build_multi_surface_publish_messenger`] with the budgeted instance list
/// spelled out, for the tests that care about which principals exist.
async fn build_multi_surface_publish_messenger_with_instances(
    slugs: &[&str],
    instances: &[&str],
) -> (Arc<Messenger>, String) {
    build_multi_surface_publish_messenger_with_principals(slugs, &default_principals(instances))
        .await
}

/// [`build_multi_surface_publish_messenger`] with each principal's budget spelled
/// out, for the tests that care about the *parameters* a principal is metered at
/// rather than which principals exist.
async fn build_multi_surface_publish_messenger_with_principals(
    slugs: &[&str],
    principals: &[(Option<String>, SurfaceSendBudget)],
) -> (Arc<Messenger>, String) {
    let mut surface_policies = std::collections::HashMap::new();
    for slug in slugs {
        surface_policies.insert(
            slug.to_string(),
            surface_publish_policy(ChannelMatcher::Prefix(String::new())),
        );
    }
    assemble_surface_messenger(
        surface_channel_entry(vec![]),
        std::collections::HashMap::new(),
        surface_policies,
        65_536,
        principals,
    )
    .await
}

/// One surface principal's send budget admits exactly `SURFACE_SEND_BURST`
/// durable publishes, then denies with `BudgetExhausted` — the R3 bound at the
/// gate.
#[tokio::test]
async fn surface_send_budget_admits_burst_then_exhausts() {
    let (m, addr) = build_multi_surface_publish_messenger(&["durabar"]).await;
    for i in 0..SURFACE_SEND_BURST {
        let r = m
            .publish_from_surface("durabar", None, &addr, "x", Urgency::Normal)
            .await;
        assert!(
            matches!(r, PublishResult::Ok { .. }),
            "publish {i} within burst should be Ok, got {r:?}"
        );
    }
    let r = m
        .publish_from_surface("durabar", None, &addr, "x", Urgency::Normal)
        .await;
    assert!(
        matches!(r, PublishResult::BudgetExhausted),
        "publish past the burst is BudgetExhausted, got {r:?}"
    );
}

/// A declared per-instance burst override is what meters that instance, and it
/// meters *only* it: `clock` at a burst of 2 is exhausted by its third publish,
/// while its sibling and the kernel — both at the default — are untouched.
///
/// The behavioural half of the override knob. Its parameters travel config →
/// boot resolution → the installer → the bucket, and this is the only place that
/// whole path is observable: a resolution test can only prove the number was
/// *resolved*, not that the bucket was built from it. The burst is deliberately
/// far below `SURFACE_SEND_BURST` so the assertion fails if the override is
/// dropped anywhere on the path and the default is silently used instead.
#[tokio::test]
async fn a_declared_send_burst_override_meters_only_its_own_instance() {
    let (m, addr) = build_multi_surface_publish_messenger_with_principals(
        &["durabar"],
        &[
            (None, SurfaceSendBudget::default()),
            (
                Some("clock".to_string()),
                SurfaceSendBudget {
                    burst: 2,
                    ..SurfaceSendBudget::default()
                },
            ),
            (Some("todos".to_string()), SurfaceSendBudget::default()),
        ],
    )
    .await;

    for i in 0..2 {
        let r = m
            .publish_from_surface("durabar", Some("clock"), &addr, "x", Urgency::Normal)
            .await;
        assert!(
            matches!(r, PublishResult::Ok { .. }),
            "publish {i} within the declared burst of 2 should be Ok, got {r:?}"
        );
    }
    let r = m
        .publish_from_surface("durabar", Some("clock"), &addr, "x", Urgency::Normal)
        .await;
    assert!(
        matches!(r, PublishResult::BudgetExhausted),
        "the third publish is past the declared burst of 2, got {r:?}"
    );

    // The sibling and the kernel keep the default burst — the override scoped to
    // the principal that declared it, not to the surface.
    for who in [Some("todos"), None] {
        for i in 0..SURFACE_SEND_BURST {
            let r = m
                .publish_from_surface("durabar", who, &addr, "x", Urgency::Normal)
                .await;
            assert!(
                matches!(r, PublishResult::Ok { .. }),
                "{who:?} publish {i} should still be within its own default burst, got {r:?}"
            );
        }
    }
}

/// A declared per-instance **refill** override reaches the bucket: `clock` at a
/// burst of 1 and a refill of 1s is exhausted by its second publish, and a third
/// succeeds once one refill interval has actually passed.
///
/// The `send_refill_secs` twin of the burst test above, and it exists for the
/// same reason: the resolution test proves only that the number was resolved,
/// leaving `TokenBucket::new(budget.burst, budget.refill, 1)` free to substitute
/// the `SURFACE_SEND_REFILL` default for `budget.refill` with the whole suite
/// green — an operator's tuning silently ignored in production. Nothing else
/// waits for a refill.
///
/// The sleep is real wall clock: `TokenBucket` reads `Instant::now()` directly
/// and offers no clock seam, so a second is the cheapest honest observation.
/// 1.1s buys margin over the 1s interval; overshooting under parallel test load
/// is harmless (more whole intervals only refill more, capped at `burst`), and
/// `sleep` cannot return early — so the test can only fail if the refill the
/// bucket used was not the declared one.
#[tokio::test]
async fn a_declared_send_refill_override_reaches_the_bucket() {
    let (m, addr) = build_multi_surface_publish_messenger_with_principals(
        &["durabar"],
        &[
            (None, SurfaceSendBudget::default()),
            (
                Some("clock".to_string()),
                SurfaceSendBudget {
                    burst: 1,
                    refill: Duration::from_secs(1),
                },
            ),
        ],
    )
    .await;

    let publish = || m.publish_from_surface("durabar", Some("clock"), &addr, "x", Urgency::Normal);

    assert!(
        matches!(publish().await, PublishResult::Ok { .. }),
        "the first publish spends the declared burst of 1"
    );
    let r = publish().await;
    assert!(
        matches!(r, PublishResult::BudgetExhausted),
        "the second publish is past the burst and no interval has elapsed, got {r:?}"
    );

    tokio::time::sleep(Duration::from_millis(1_100)).await;

    let r = publish().await;
    assert!(
        matches!(r, PublishResult::Ok { .. }),
        "one declared 1s refill interval has passed, so a token is back; a bucket \
         built with the {SURFACE_SEND_REFILL:?} default would still be exhausted, got {r:?}"
    );
}

/// Platform telemetry (`publish_from_surface_platform`) is exempt from the
/// send budget: after the budget is fully drained (so an ordinary
/// surface publish is `BudgetExhausted`), a platform publish still succeeds. The
/// exemption skips only the budget step — every other gate applies, so a platform
/// publish outside the surface's ACL is still `AclDenied`.
#[tokio::test]
async fn platform_publish_is_send_budget_exempt() {
    let (m, addr) = build_multi_surface_publish_messenger(&["durabar"]).await;
    // Drain the budget with ordinary surface publishes.
    for _ in 0..SURFACE_SEND_BURST {
        let _ = m
            .publish_from_surface("durabar", None, &addr, "x", Urgency::Normal)
            .await;
    }
    assert!(
        matches!(
            m.publish_from_surface("durabar", None, &addr, "x", Urgency::Normal)
                .await,
            PublishResult::BudgetExhausted
        ),
        "budget must be drained"
    );
    // The platform path still publishes despite the drained budget.
    let r = m
        .publish_from_surface_platform("durabar", &addr, "telemetry", Urgency::Normal)
        .await;
    assert!(
        matches!(r, PublishResult::Ok { .. }),
        "platform telemetry is send-budget exempt, got {r:?}"
    );
    // The exemption skips only the budget: an out-of-ACL platform publish is
    // still denied by the normal gates.
    let denied = m
        .publish_from_surface_platform("durabar", "brenn:not-in-acl", "x", Urgency::Normal)
        .await;
    assert!(
        matches!(
            denied,
            PublishResult::UnknownChannel(_) | PublishResult::AclDenied(_)
        ),
        "platform publish still passes every non-budget gate, got {denied:?}"
    );
}

/// The budget is keyed by principal and is process-lifetime, so a "reconnected"
/// session (another publish under the same principal on the same `Messenger`)
/// inherits the drained budget rather than a fresh one.
#[tokio::test]
async fn surface_send_budget_survives_reconnect() {
    let (m, addr) = build_multi_surface_publish_messenger(&["durabar"]).await;
    for _ in 0..SURFACE_SEND_BURST {
        let _ = m
            .publish_from_surface("durabar", None, &addr, "x", Urgency::Normal)
            .await;
    }
    // A later publish (a reconnected session would land here) is still denied —
    // the drained budget was not reset by anything connection-scoped.
    let r = m
        .publish_from_surface("durabar", None, &addr, "x", Urgency::Normal)
        .await;
    assert!(
        matches!(r, PublishResult::BudgetExhausted),
        "the drained budget must persist across sessions, got {r:?}"
    );
}

/// Budgets are independent per slug: exhausting one surface's budget leaves
/// another surface's budget full.
#[tokio::test]
async fn surface_send_budgets_are_per_slug_independent() {
    let (m, addr) = build_multi_surface_publish_messenger(&["durabar", "kitchen"]).await;
    // Drain durabar's budget completely (burst + one denied).
    for _ in 0..SURFACE_SEND_BURST {
        let _ = m
            .publish_from_surface("durabar", None, &addr, "x", Urgency::Normal)
            .await;
    }
    assert!(matches!(
        m.publish_from_surface("durabar", None, &addr, "x", Urgency::Normal)
            .await,
        PublishResult::BudgetExhausted
    ));
    // kitchen's budget is untouched.
    let r = m
        .publish_from_surface("kitchen", None, &addr, "x", Urgency::Normal)
        .await;
    assert!(
        matches!(r, PublishResult::Ok { .. }),
        "a sibling surface's budget is independent, got {r:?}"
    );
}

/// A component publish is stored under the derived sub-identity
/// `surface:<slug>#<kind>`, not the bare surface: attribution lands on the
/// component that acted.
#[tokio::test]
async fn component_publish_stamps_sub_identity_sender() {
    let (m, addr) = build_surface_publish_messenger(
        "durabar",
        surface_publish_policy(ChannelMatcher::Prefix(String::new())),
        65_536,
    )
    .await;

    let result = m
        .publish_from_surface("durabar", Some("clock"), &addr, "hello", Urgency::Normal)
        .await;
    assert!(matches!(result, PublishResult::Ok { .. }), "got {result:?}");

    let conn = m.db().lock().await;
    let sender: String = conn
        .query_row("SELECT sender FROM messaging_messages", [], |r| r.get(0))
        .unwrap();
    assert_eq!(
        sender, "surface:durabar#clock",
        "a component publish is attributed to the component, not its surface"
    );
}

/// The point of the whole increment: a runaway component exhausts *its own*
/// limit. Draining one kind's bucket leaves its sibling kind and the surface's
/// own kernel identity able to publish — under the old per-surface keying, all
/// three shared one bucket and the first one to run away silenced the rest.
#[tokio::test]
async fn component_budgets_are_blast_radius_scoped() {
    let (m, addr) = build_multi_surface_publish_messenger(&["durabar"]).await;
    // Drain the `clock` component's bucket completely.
    for _ in 0..SURFACE_SEND_BURST {
        let _ = m
            .publish_from_surface("durabar", Some("clock"), &addr, "x", Urgency::Normal)
            .await;
    }
    assert!(
        matches!(
            m.publish_from_surface("durabar", Some("clock"), &addr, "x", Urgency::Normal)
                .await,
            PublishResult::BudgetExhausted
        ),
        "clock's own budget must be drained"
    );

    // The sibling kind is untouched.
    let sibling = m
        .publish_from_surface("durabar", Some("todos"), &addr, "x", Urgency::Normal)
        .await;
    assert!(
        matches!(sibling, PublishResult::Ok { .. }),
        "a sibling component's budget is independent, got {sibling:?}"
    );

    // And so is the surface's own kernel identity — the grain that carries the
    // kernel's error reports, which a runaway component must never silence.
    let kernel = m
        .publish_from_surface("durabar", None, &addr, "x", Urgency::Normal)
        .await;
    assert!(
        matches!(kernel, PublishResult::Ok { .. }),
        "the kernel's own budget is independent of any component's, got {kernel:?}"
    );
}

/// A repeated *instance* is a boot wiring bug and panics. Boot resolution
/// already proves instances unique within a surface (it asserts on a duplicate
/// `[[surface.component]]` instance id), so the installer seeing one means the
/// two sides disagree about the declaration set — the exact disagreement that
/// would leave a live principal unbudgeted.
///
/// Driven straight at the installer: the fixture builders take an instance list
/// verbatim, so this is the one shape a caller can hand it that boot cannot.
#[tokio::test]
#[should_panic(expected = "duplicate budget for surface \"durabar\" principal Some(\"clock\")")]
async fn a_repeated_instance_panics() {
    let _ = build_multi_surface_publish_messenger_with_instances(&["durabar"], &["clock", "clock"])
        .await;
}

/// A repeated *slug* is a boot wiring bug and panics, for the same reason as a
/// repeated instance: the same surface resolved twice is nonsense.
///
/// Driven straight at the installer rather than through the fixture builders:
/// their policy map is keyed by slug, so it folds a repeated slug away before
/// the installer could ever see one.
#[tokio::test]
#[should_panic(expected = "duplicate budget for surface")]
async fn a_repeated_slug_panics() {
    let messenger = Messenger::new(
        init_db_memory(),
        Arc::new(MessagingDirectory::with_entries(vec![])),
        Arc::from("test"),
        Arc::new(IndexMap::new()),
        Arc::new(CountingRouter::default()) as Arc<dyn WakeRouter>,
        MessagingGlobalConfig::default(),
    );
    let _ = messenger.with_surface_send_budgets([
        ("durabar".to_string(), default_principals(&["clock"])),
        ("durabar".to_string(), default_principals(&["clock"])),
    ]);
}

/// The same instance name on two different surfaces is two principals: blast
/// radius is scoped per `(slug, instance)`, not per instance name globally.
#[tokio::test]
async fn same_instance_name_on_two_surfaces_are_distinct_principals() {
    let (m, addr) = build_multi_surface_publish_messenger(&["durabar", "kitchen"]).await;
    for _ in 0..SURFACE_SEND_BURST {
        let _ = m
            .publish_from_surface("durabar", Some("clock"), &addr, "x", Urgency::Normal)
            .await;
    }
    assert!(matches!(
        m.publish_from_surface("durabar", Some("clock"), &addr, "x", Urgency::Normal)
            .await,
        PublishResult::BudgetExhausted
    ));
    let r = m
        .publish_from_surface("kitchen", Some("clock"), &addr, "x", Urgency::Normal)
        .await;
    assert!(
        matches!(r, PublishResult::Ok { .. }),
        "the same instance name on another surface is its own principal, got {r:?}"
    );
}

/// **The grain correction, at the gate.** Two instances of one kind are two
/// principals with two buckets: draining one leaves its sibling-of-kind free to
/// publish. Twelve `agenda` instances are twelve people's agendas — one wedged
/// instance must not silence the other eleven.
///
/// This is the behavioural inverse of what the kind-level grain enforced, so it
/// is the test that fails if the identity ever folds back to the kind: with a
/// shared bucket the sibling's publish would be `BudgetExhausted`.
#[tokio::test]
async fn sibling_instances_of_one_kind_get_independent_buckets() {
    let (m, addr) = build_multi_surface_publish_messenger_with_instances(
        &["durabar"],
        // The shape boot produces for a surface declaring two `agenda` instances.
        &["agenda-alice", "agenda-bob"],
    )
    .await;
    for _ in 0..SURFACE_SEND_BURST {
        let _ = m
            .publish_from_surface("durabar", Some("agenda-alice"), &addr, "x", Urgency::Normal)
            .await;
    }
    assert!(
        matches!(
            m.publish_from_surface("durabar", Some("agenda-alice"), &addr, "x", Urgency::Normal)
                .await,
            PublishResult::BudgetExhausted
        ),
        "the drained instance must be exhausted"
    );
    let sibling = m
        .publish_from_surface("durabar", Some("agenda-bob"), &addr, "x", Urgency::Normal)
        .await;
    assert!(
        matches!(sibling, PublishResult::Ok { .. }),
        "a sibling instance of the same kind has its own bucket, got {sibling:?}"
    );
}

/// A component instance with no installed budget is a broken boot invariant,
/// exactly like an unbudgeted surface: the gate panics rather than admit an
/// unbounded publisher. Pins that the finer keying did not open a hole where an
/// unrecognised instance silently skips the budget.
#[tokio::test]
#[should_panic(expected = "has no send budget")]
async fn unbudgeted_component_instance_panics() {
    let (m, addr) = build_multi_surface_publish_messenger(&["durabar"]).await;
    let _ = m
        .publish_from_surface(
            "durabar",
            Some("never-declared"),
            &addr,
            "x",
            Urgency::Normal,
        )
        .await;
}

/// A registered surface with no installed send budget is a broken boot
/// invariant: the gate panics rather than admit an unbounded publisher.
#[tokio::test]
#[should_panic(expected = "has no send budget")]
async fn surface_send_budget_missing_panics() {
    // Register a surface subscriber but install no budget for it.
    let (m, addr) = build_multi_surface_publish_messenger(&[]).await;
    let m = m.with_subscriber_registrations(crate::messaging::testutils::surface_registrations({
        let mut p = std::collections::HashMap::new();
        p.insert(
            "ghost".to_string(),
            surface_publish_policy(ChannelMatcher::Prefix(String::new())),
        );
        p
    }));
    let _ = m
        .publish_from_surface("ghost", None, &addr, "x", Urgency::Normal)
        .await;
}

// ---------------------------------------------------------------------------
// `publish_batch_from_surface` — one activation's flush
// ---------------------------------------------------------------------------

/// Batch entries onto the fixture channel, one per body, each at `urgency`,
/// stamped in call order as the session handler stamps a real flush (strictly
/// increasing, one pass across the whole batch).
fn batch<'a>(
    addr: &'a str,
    bodies: &'a [&'a str],
    urgency: Urgency,
) -> Vec<SurfaceBatchPublish<'a>> {
    bodies
        .iter()
        .enumerate()
        .map(|(i, body)| SurfaceBatchPublish {
            channel_address: addr,
            body,
            urgency,
            publish_ts_ns: stamp(i),
        })
        .collect()
}

/// The `i`th call-order stamp of a test flush. Anchored at a fixed instant
/// rather than `now` so a test's expected order is readable in its assertions.
fn stamp(i: usize) -> i64 {
    1_700_000_000_000_000_000 + i as i64
}

/// Every stored body, oldest first by publish timestamp — the order a subscriber
/// reads the batch in.
async fn stored_bodies(m: &Messenger) -> Vec<String> {
    let conn = m.db().lock().await;
    let mut stmt = conn
        .prepare("SELECT body FROM messaging_messages ORDER BY publish_ts_ns")
        .unwrap();
    stmt.query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

/// The happy path: every entry lands, in call order, stamped with the
/// **instance** sub-identity — the grain the batch names, not the bare surface.
#[tokio::test]
async fn publish_batch_from_surface_commits_every_entry_in_call_order() {
    let (m, addr) = build_surface_publish_messenger(
        "durabar",
        surface_publish_policy(ChannelMatcher::Prefix(String::new())),
        65_536,
    )
    .await;

    m.publish_batch_from_surface(
        "durabar",
        "clock",
        &batch(&addr, &["a", "b", "c"], Urgency::Normal),
    )
    .await;

    assert_eq!(
        stored_bodies(&m).await,
        vec!["a", "b", "c"],
        "the batch commits in call order"
    );
    let conn = m.db().lock().await;
    let senders: Vec<String> = conn
        .prepare("SELECT DISTINCT sender FROM messaging_messages")
        .unwrap()
        .query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert_eq!(
        senders,
        vec!["surface:durabar#clock"],
        "every row carries the instance sub-identity"
    );
}

/// Per-entry urgency is preserved: the caller resolved override-else-default per
/// entry, and the batch must not flatten the batch to one rung.
#[tokio::test]
async fn publish_batch_from_surface_preserves_per_entry_urgency() {
    let (m, addr) = build_surface_publish_messenger(
        "durabar",
        surface_publish_policy(ChannelMatcher::Prefix(String::new())),
        65_536,
    )
    .await;

    m.publish_batch_from_surface(
        "durabar",
        "clock",
        &[
            SurfaceBatchPublish {
                channel_address: &addr,
                body: "low",
                urgency: Urgency::Low,
                publish_ts_ns: stamp(0),
            },
            SurfaceBatchPublish {
                channel_address: &addr,
                body: "high",
                urgency: Urgency::High,
                publish_ts_ns: stamp(1),
            },
        ],
    )
    .await;

    let conn = m.db().lock().await;
    let rows: Vec<(String, String)> = conn
        .prepare("SELECT body, urgency FROM messaging_messages ORDER BY publish_ts_ns")
        .unwrap()
        .query_map([], |r| Ok((r.get(0).unwrap(), r.get(1).unwrap())))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert_eq!(
        rows,
        vec![
            ("low".to_string(), Urgency::Low.as_str().to_string()),
            ("high".to_string(), Urgency::High.as_str().to_string()),
        ],
        "each entry keeps the urgency the caller resolved for it"
    );
}

/// **The atomicity property.** A failure part-way through the batch — here an
/// entry naming a channel outside the directory, the broken-boot-invariant panic
/// — must leave **zero** rows, not the prefix that had already been inserted. The
/// activation released these publishes together; half of them is a state no
/// component asked for.
#[tokio::test]
async fn a_mid_batch_failure_leaves_zero_rows() {
    let (m, addr) = build_surface_publish_messenger(
        "durabar",
        surface_publish_policy(ChannelMatcher::Prefix(String::new())),
        65_536,
    )
    .await;

    // The panic unwinds through the `Transaction` drop guard; a spawned task
    // isolates it so this test can inspect the DB afterwards.
    let m2 = Arc::clone(&m);
    let addr2 = addr.clone();
    let joined = tokio::spawn(async move {
        let entries = vec![
            SurfaceBatchPublish {
                channel_address: &addr2,
                body: "first",
                urgency: Urgency::Normal,
                publish_ts_ns: stamp(0),
            },
            SurfaceBatchPublish {
                channel_address: "brenn:not-in-the-directory",
                body: "doomed",
                urgency: Urgency::Normal,
                publish_ts_ns: stamp(1),
            },
        ];
        m2.publish_batch_from_surface("durabar", "clock", &entries)
            .await;
    })
    .await;
    assert!(
        joined.is_err(),
        "an entry outside the directory is a broken boot invariant and panics"
    );

    assert!(
        stored_bodies(&m).await.is_empty(),
        "the first entry rolled back with the batch — all-or-nothing"
    );
}

/// The batch entry point does **not** draw the send budget: the caller draws once
/// for the whole batch, because a per-entry draw could admit a prefix of an atomic
/// flush and refuse its tail. Pins that the two are not both drawing (which would
/// double-charge every batch).
#[tokio::test]
async fn publish_batch_from_surface_does_not_draw_the_send_budget() {
    let (m, addr) = build_multi_surface_publish_messenger(&["durabar"]).await;

    // Far more entries than the burst; if the entry point drew per row it would
    // exhaust the bucket mid-batch.
    let bodies: Vec<&str> = vec!["x"; SURFACE_SEND_BURST as usize + 5];
    m.publish_batch_from_surface("durabar", "clock", &batch(&addr, &bodies, Urgency::Normal))
        .await;
    assert_eq!(
        stored_bodies(&m).await.len(),
        bodies.len(),
        "every entry committed — the entry point consulted no budget"
    );

    // And the instance's bucket is untouched: a single publish still lands.
    let r = m
        .publish_from_surface("durabar", Some("clock"), &addr, "x", Urgency::Normal)
        .await;
    assert!(
        matches!(r, PublishResult::Ok { .. }),
        "the batch drew nothing, so the bucket is still full, got {r:?}"
    );
}

/// Sufficiency, end to end at the budget: a batch wider than the burst is
/// **refused** and costs nothing — no debt is minted, so the instance's ordinary
/// publishes keep working — and a batch the balance covers whole is admitted and
/// spends exactly its own width. The refusal is the honest answer for a
/// mis-sized bucket; boot's sizing invariant is what keeps a *conforming* flush
/// from ever being the batch that is refused.
#[tokio::test(start_paused = true)]
async fn an_oversized_batch_draw_is_refused_and_mints_no_debt() {
    let (m, addr) = build_multi_surface_publish_messenger(&["durabar"]).await;

    assert_eq!(
        m.draw_surface_send_budget_for_batch("durabar", "clock", SURFACE_SEND_BURST + 5),
        SurfaceSendVerdict::Denied,
        "a batch wider than the burst cannot be covered, so it is refused"
    );

    // The refusal deducted nothing: the instance's single publishes are
    // unaffected, which is exactly what a debt would have blocked.
    let r = m
        .publish_from_surface("durabar", Some("clock"), &addr, "x", Urgency::Normal)
        .await;
    assert!(
        matches!(r, PublishResult::Ok { .. }),
        "a refused batch minted no debt, got {r:?}"
    );

    // And the balance is still there for a batch that fits: burst - 1 already
    // spent above, so the rest of the bucket draws whole.
    assert_eq!(
        m.draw_surface_send_budget_for_batch("durabar", "clock", SURFACE_SEND_BURST - 1),
        SurfaceSendVerdict::Admitted,
        "a draw the balance covers exactly is admitted"
    );
    assert_eq!(
        m.draw_surface_send_budget_for_batch("durabar", "clock", 1),
        SurfaceSendVerdict::Denied,
        "and it spent exactly its own width — nothing is left"
    );

    // A sibling instance is untouched: the bucket is per-principal.
    let sibling = m
        .publish_from_surface("durabar", Some("todos"), &addr, "x", Urgency::Normal)
        .await;
    assert!(
        matches!(sibling, PublishResult::Ok { .. }),
        "the sibling's bucket never saw the batch, got {sibling:?}"
    );

    // Refill restores admission at the rate the interval sets.
    tokio::time::advance(SURFACE_SEND_REFILL).await;
    assert_eq!(
        m.draw_surface_send_budget_for_batch("durabar", "clock", 1),
        SurfaceSendVerdict::Admitted,
        "one interval refills one publish's worth"
    );
}

/// A maximal conforming flush — `MAX_PUBLISHES_PER_ACTIVATION` entries — is
/// admitted whole from a full default bucket, every time. This is the sizing
/// invariant's whole point, executable: the default burst *is* the cap, so the
/// widest batch a conforming kernel can send fits exactly.
#[tokio::test(start_paused = true)]
async fn a_maximal_conforming_flush_is_admitted_from_a_full_bucket() {
    let (m, _addr) = build_multi_surface_publish_messenger(&["durabar"]).await;

    let cap = u32::try_from(brenn_budget::MAX_PUBLISHES_PER_ACTIVATION).unwrap();
    assert_eq!(
        m.draw_surface_send_budget_for_batch("durabar", "clock", cap),
        SurfaceSendVerdict::Admitted,
        "a full default bucket admits exactly one maximal conforming flush"
    );
}

/// A batch draw for an instance with no installed budget is the same broken boot
/// invariant as a single publish's: panic rather than admit an unbounded
/// publisher. The batch path must not be the hole in the backstop.
#[test]
#[should_panic(expected = "has no send budget")]
fn a_batch_draw_for_an_unbudgeted_instance_panics() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let (m, _addr) = build_multi_surface_publish_messenger(&["durabar"]).await;
        let _ = m.draw_surface_send_budget_for_batch("durabar", "never-declared", 3);
    });
}
