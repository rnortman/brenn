//! Unit tests for `resolve_surfaces`,
//! plus the directory-level `validate_static_subscriptions_deliverable`
//! surface- and system-participant-coverage checks.

use super::test_fixtures::{
    brenn_entry_with, dir_of, make_brenn_dir, minimal_surface_raw, surface_sub_raw,
};
use super::*;
use brenn_lib::config::AppConfig;
use brenn_lib::messaging::config::{
    DEFAULT_SURFACE_PUBLISH_BURST, DEFAULT_SURFACE_PUBLISH_PER_SEC, ResolvedComponent,
    ResolvedLocalChannel, SurfaceSendBudget,
};
use brenn_lib::messaging::{EPHEMERAL_SENDER_BURST, EPHEMERAL_SENDER_REFILL_AMOUNT, Urgency};

/// Global messaging defaults for resolution tests: the stock defaults with a
/// bounded `default_push_depth`, which is what an operator hosting a surface must
/// set (a surface binding's port queue is page memory, so resolution rejects the
/// stock `Unbounded`). Tests that exercise that rejection pass their own globals.
fn test_globals() -> brenn_lib::messaging::config::MessagingGlobalConfig {
    brenn_lib::messaging::config::MessagingGlobalConfig {
        default_push_depth: brenn_lib::messaging::config::Depth::Bounded(8),
        ..Default::default()
    }
}

/// A one-channel directory carrying a single `Surface("deskbar")` subscriber
/// on `brenn:surface-boot`. Used by both surface deliverability tests below.
fn surface_boot_directory() -> messaging::MessagingDirectory {
    use brenn_lib::messaging::config::{Depth, NoiseLevel, ResolvedChannel, Sink};
    use brenn_lib::messaging::{
        ChannelEntry, ChannelScheme, MessagingDirectory, SubscriberEntry, SubscriberEntryKind,
    };
    let entry = ChannelEntry {
        uuid: uuid::Uuid::new_v4(),
        address: brenn_lib::messaging::canonical_address("surface-boot"),
        description: None,
        resolved_channel: ResolvedChannel {
            push_depth: Depth::Unbounded,
            retain_depth: Depth::Unbounded,
            standing_retain_depth: Depth::Unbounded,
            noise: NoiseLevel::Silent,
            sink: Sink::Drop,
            wake_min: brenn_lib::messaging::WakeMin::Normal,
        },
        subscribers: vec![SubscriberEntry {
            kind: SubscriberEntryKind::Surface {
                slug: "deskbar".to_string(),
                instance: None,
            },
            push_depth: Depth::Unbounded,
            retain_depth: Depth::Unbounded,
            noise: NoiseLevel::Silent,
            wake_min: None,
        }],
        transport_type: ChannelScheme::Brenn,
        mount: None,
    };
    MessagingDirectory::with_entries(vec![entry])
}

/// Build a `ResolvedSurface` named `deskbar` carrying `policy`; the other
/// fields are inert for the deliverability check.
fn surface_with_policy(policy: brenn_lib::access::AppPolicy) -> ResolvedSurface {
    crate::test_support::surface::SurfaceFixture::new("deskbar", "protobar")
        .policy(policy)
        .build()
}

/// The boot subscription-coverage check treats a `Surface` subscriber exactly
/// like App/Wasm: a Surface whose resolved surface policy covers the channel is
/// deliverable, so the validator accepts it without panic.
#[test]
fn validate_static_subscriptions_surface_covered_passes() {
    use brenn_lib::access::raw::ChannelMatcherRaw;
    use brenn_lib::messaging::config::SurfaceGrant;
    use indexmap::IndexMap as IM;

    let directory = surface_boot_directory();
    let apps: IM<String, AppConfig> = IM::new();
    let policy = brenn_lib::access::resolve::build_surface_policy(
        "deskbar",
        [SurfaceGrant::Subscribe],
        &[ChannelMatcherRaw::Exact("surface-boot".to_string())],
        &[],
        &[],
        &[],
    );
    // No panic: the surface policy authorizes delivery on brenn:surface-boot.
    validate_static_subscriptions_deliverable(
        &directory,
        &apps,
        &[],
        &[surface_with_policy(policy)],
        &[],
    );
}

/// A `Surface` subscriber whose resolved surface policy does not cover its
/// channel (empty `subscribe_acl`, so no covering matcher) is a dead
/// subscription — boot refuses to start, byte-for-byte the App/Wasm floor
/// parity behavior.
#[test]
#[should_panic(expected = "can never deliver on")]
fn validate_static_subscriptions_surface_uncovered_panics() {
    use brenn_lib::messaging::config::SurfaceGrant;
    use indexmap::IndexMap as IM;

    let directory = surface_boot_directory();
    let apps: IM<String, AppConfig> = IM::new();
    // Subscribe grant but no ACL matcher ⇒ allows_channel_access is false.
    let policy = brenn_lib::access::resolve::build_surface_policy(
        "deskbar",
        [SurfaceGrant::Subscribe],
        &[],
        &[],
        &[],
        &[],
    );
    validate_static_subscriptions_deliverable(
        &directory,
        &apps,
        &[],
        &[surface_with_policy(policy)],
        &[],
    );
}

/// A one-channel directory carrying a single `System("relay")` subscriber on
/// `brenn:system-boot`, as `fold_spec_subscriptions` would produce. Used by
/// both system deliverability tests below.
fn system_boot_directory() -> messaging::MessagingDirectory {
    use brenn_lib::messaging::config::{Depth, NoiseLevel, ResolvedChannel, Sink};
    use brenn_lib::messaging::{
        ChannelEntry, ChannelScheme, MessagingDirectory, SubscriberEntry, SubscriberEntryKind,
    };
    let entry = ChannelEntry {
        uuid: uuid::Uuid::new_v4(),
        address: brenn_lib::messaging::canonical_address("system-boot"),
        description: None,
        resolved_channel: ResolvedChannel {
            push_depth: Depth::Unbounded,
            retain_depth: Depth::Unbounded,
            standing_retain_depth: Depth::Unbounded,
            noise: NoiseLevel::Silent,
            sink: Sink::Drop,
            wake_min: brenn_lib::messaging::WakeMin::Normal,
        },
        subscribers: vec![SubscriberEntry {
            kind: SubscriberEntryKind::System("relay".to_string()),
            push_depth: Depth::Unbounded,
            retain_depth: Depth::Unbounded,
            noise: NoiseLevel::Silent,
            wake_min: None,
        }],
        transport_type: ChannelScheme::Brenn,
        mount: None,
    };
    MessagingDirectory::with_entries(vec![entry])
}

/// A `SystemParticipantSpec` named `relay` carrying `policy`.
fn system_spec_with_policy(
    policy: brenn_lib::access::AppPolicy,
) -> brenn_lib::messaging::system::SystemParticipantSpec {
    brenn_lib::messaging::system::SystemParticipantSpec {
        component: "relay",
        policy,
        subscriptions: vec!["brenn:system-boot".to_string()],
    }
}

/// A code-built policy with `MessagingSubscribe` and one exact `brenn_subscribe`
/// matcher on `channel`.
fn system_subscribe_policy(channel: &str) -> brenn_lib::access::AppPolicy {
    use brenn_lib::access::acl::{AclSet, ChannelMatcher};
    use brenn_lib::access::{AppCapability, AppPolicy, GrantSet};
    let mut grants = GrantSet::default();
    grants.insert(AppCapability::MessagingSubscribe);
    let mut acls = AclSet::default();
    acls.brenn_subscribe
        .push(ChannelMatcher::Exact(channel.to_string()));
    AppPolicy {
        grants,
        acls,
        tool_grants: std::collections::BTreeMap::new(),
    }
}

/// A `System` subscriber whose spec policy covers its subscribed channel passes
/// the deliverability check — the former skip arm is gone; system participants
/// are validated like every other static subscriber.
#[test]
fn validate_static_subscriptions_system_covered_passes() {
    use indexmap::IndexMap as IM;
    let directory = system_boot_directory();
    let apps: IM<String, AppConfig> = IM::new();
    validate_static_subscriptions_deliverable(
        &directory,
        &apps,
        &[],
        &[],
        &[system_spec_with_policy(system_subscribe_policy(
            "system-boot",
        ))],
    );
}

/// A `System` subscriber whose code-built policy cannot deliver on its own
/// subscribed channel is a host wiring bug — boot refuses to start (the old
/// skip arm's test inverse).
#[test]
#[should_panic(expected = "can never deliver on")]
fn validate_static_subscriptions_system_uncovered_panics() {
    use indexmap::IndexMap as IM;
    let directory = system_boot_directory();
    let apps: IM<String, AppConfig> = IM::new();
    // Policy covers a different channel, so allows_channel_access is false.
    validate_static_subscriptions_deliverable(
        &directory,
        &apps,
        &[],
        &[],
        &[system_spec_with_policy(system_subscribe_policy(
            "elsewhere",
        ))],
    );
}

// --- resolve_surfaces fixtures ---

/// An `EphemeralChannelEntry` whose channel rung is transparent to
/// `test_globals` — a bounded `push_depth` matching the test global default, so a
/// binding that states no `push_depth` resolves binding → channel → global to a
/// legal page-queue depth exactly as `test_globals` intends.
fn ephem(name: &str) -> brenn_lib::messaging::config::EphemeralChannelEntry {
    use brenn_lib::messaging::config::{Depth, EphemeralChannelEntry, NoiseLevel};
    EphemeralChannelEntry {
        uuid: brenn_lib::messaging::ephemeral_channel_uuid_from_name(name),
        name: name.to_string(),
        push_depth: Depth::Bounded(8),
        retain_depth: 1,
        noise: NoiseLevel::Silent,
        capacity: 16,
    }
}

/// Like [`ephem`] but with an explicit channel-rung `push_depth`, `retain_depth`,
/// and `noise` — for the binding → channel → global inheritance tests.
fn ephem_with(
    name: &str,
    push_depth: brenn_lib::messaging::config::Depth,
    retain_depth: u64,
    noise: brenn_lib::messaging::config::NoiseLevel,
) -> brenn_lib::messaging::config::EphemeralChannelEntry {
    brenn_lib::messaging::config::EphemeralChannelEntry {
        uuid: brenn_lib::messaging::ephemeral_channel_uuid_from_name(name),
        name: name.to_string(),
        push_depth,
        retain_depth,
        noise,
        capacity: 16,
    }
}

/// A valid `[[surface]]` raw block: one bound component (`protobar`) with an
/// ephemeral input + a brenn output, one presentational component (`sidecar`,
/// no bindings — pins the allowed "component with no ports" case), the required
/// `chrome` singleton, and grants + ACLs that cover both bindings.
fn valid_surface_raw() -> brenn_lib::messaging::config::SurfaceConfigRaw {
    use brenn_lib::access::raw::ChannelMatcherRaw;
    use brenn_lib::messaging::config::{
        SurfaceComponentRaw, SurfaceConfigRaw, SurfaceGrant, SurfaceOutputRaw,
    };
    SurfaceConfigRaw {
        grants: vec![SurfaceGrant::EphemeralSubscribe, SurfaceGrant::Publish],
        publish_acl: vec![ChannelMatcherRaw::Exact("alerts".to_string())],
        ephemeral_subscribe_acl: vec![ChannelMatcherRaw::Exact("protobar-demo".to_string())],
        components: vec![
            SurfaceComponentRaw {
                kind: "protobar".to_string(),
                instance: None,
                abi: "dom".to_string(),
                send_burst: None,
                send_refill_secs: None,
                parked_batch_depth: None,
                config: None,
                chrome: false,
            },
            SurfaceComponentRaw {
                kind: "sidecar".to_string(),
                instance: None,
                abi: "dom".to_string(),
                send_burst: None,
                send_refill_secs: None,
                parked_batch_depth: None,
                config: None,
                chrome: false,
            },
            SurfaceComponentRaw {
                kind: "chrome".to_string(),
                instance: None,
                abi: "dom".to_string(),
                send_burst: None,
                send_refill_secs: None,
                parked_batch_depth: None,
                config: None,
                chrome: true,
            },
        ],
        subscriptions: vec![surface_sub_raw(
            "ephemeral:protobar-demo",
            "protobar",
            "messages",
        )],
        outputs: vec![SurfaceOutputRaw {
            instance: "protobar".to_string(),
            port: "out".to_string(),
            channel: "brenn:alerts".to_string(),
            urgency: None,
            publish_per_activation: None,
            publish_capacity: None,
        }],
        ..minimal_surface_raw()
    }
}

/// A directory holding `brenn:alerts` and an ephemeral set holding
/// `protobar-demo` — the channels the valid surface binds.
fn surface_dir_and_ephem() -> (
    MessagingDirectory,
    Vec<brenn_lib::messaging::config::EphemeralChannelEntry>,
) {
    let (dir, _addr) = make_brenn_dir("brenn:alerts");
    (dir, vec![ephem("protobar-demo")])
}

/// A surface with one `brenn:alerts` durable subscription, granted Subscribe
/// plus a covering `subscribe_acl`. The bound `brenn:alerts` channel comes
/// from `make_brenn_dir` (push/retain Unbounded, noise Silent, wake_min
/// Normal), so the subscription sets explicit **bounded** push/retain depths —
/// a durable surface binding whose resolved depths are unbounded is a boot
/// panic (the per-subscribe replay must be bounded).
fn durable_surface_raw() -> brenn_lib::messaging::config::SurfaceConfigRaw {
    use brenn_lib::access::raw::ChannelMatcherRaw;
    use brenn_lib::messaging::config::{
        Depth, SurfaceConfigRaw, SurfaceGrant, SurfaceSubscriptionRaw,
    };
    SurfaceConfigRaw {
        grants: vec![SurfaceGrant::Subscribe],
        subscribe_acl: vec![ChannelMatcherRaw::Exact("alerts".to_string())],
        subscriptions: vec![SurfaceSubscriptionRaw {
            push_depth: Some(Depth::Bounded(8)),
            retain_depth: Some(Depth::Bounded(4)),
            ..surface_sub_raw("brenn:alerts", "protobar", "messages")
        }],
        ..minimal_surface_raw()
    }
}

/// A declared component instance whose `instance` id is stated explicitly, so a
/// test can add a *sibling* of an existing kind — the multi-instance shape the
/// per-instance principal exists for.
fn component_raw(instance: &str) -> brenn_lib::messaging::config::SurfaceComponentRaw {
    brenn_lib::messaging::config::SurfaceComponentRaw {
        kind: "agenda".to_string(),
        instance: Some(instance.to_string()),
        abi: "dom".to_string(),
        send_burst: None,
        send_refill_secs: None,
        parked_batch_depth: None,
        config: None,
        chrome: false,
    }
}

/// A `MessagingDirectory` holding one `brenn:` channel with **bounded** push
/// and retain depths — the inheritance target for a durable surface binding
/// that leaves its own knobs unset (unbounded channel defaults are rejected at
/// boot, so the inherit test cannot use `make_brenn_dir`).
fn make_bounded_brenn_dir(chan_addr: &str) -> MessagingDirectory {
    dir_of(vec![brenn_entry_with(
        chan_addr,
        Depth::Bounded(16),
        Depth::Bounded(8),
        NoiseLevel::Silent,
    )])
}

// --- resolve_surfaces tests ---

/// The operator's per-output `urgency` survives boot resolution.
///
/// The one link in the config knob's chain nothing else pins: every other
/// fixture leaves `urgency` unset and every downstream assertion expects
/// `Normal`, so replacing the resolution with a bare `Urgency::Normal` constant
/// passes them all — the knob would be silently dead, config accepted and
/// parsed and then thrown away. `High` (not `Normal`) is the point: at `Normal`
/// the assertion cannot tell resolution from a hard-coded constant.
#[test]
fn a_configured_output_urgency_resolves_onto_the_output() {
    let (dir, ephem_chs) = surface_dir_and_ephem();
    let mut raw = valid_surface_raw();
    raw.outputs[0].urgency = Some(Urgency::High);
    let resolved = resolve_surfaces(&[raw], &dir, &ephem_chs, &test_globals());
    assert_eq!(resolved[0].outputs[0].default_urgency, Urgency::High);
}

/// The `chrome` flag on a `[[surface.component]]` survives boot resolution onto
/// its `ResolvedComponent`, so the server can advertise the chrome instance.
/// Default false leaves siblings unmarked; setting it on one carries through.
#[test]
fn the_chrome_flag_resolves_onto_its_component() {
    let (dir, ephem_chs) = surface_dir_and_ephem();
    let raw = valid_surface_raw();
    let resolved = resolve_surfaces(&[raw], &dir, &ephem_chs, &test_globals());
    // The fixture's chrome component is the third one; the two application
    // components must resolve unmarked.
    assert!(
        resolved[0].components[2].chrome,
        "chrome flag lost in resolution"
    );
    assert!(
        !resolved[0].components[0].chrome && !resolved[0].components[1].chrome,
        "sibling wrongly marked chrome"
    );
}

/// The unset twin: no configured urgency falls to `normal`, the same one-step
/// port → global ladder `[[wasm_consumer]] [[output]]` uses.
#[test]
fn an_unset_output_urgency_falls_back_to_normal() {
    let (dir, ephem_chs) = surface_dir_and_ephem();
    let raw = valid_surface_raw();
    assert_eq!(raw.outputs[0].urgency, None, "fixture leaves it unset");
    let resolved = resolve_surfaces(&[raw], &dir, &ephem_chs, &test_globals());
    assert_eq!(resolved[0].outputs[0].default_urgency, Urgency::Normal);
}

/// Every rung of the ladder round-trips, not just the two the other tests
/// happen to name — an `unwrap_or` that mapped some rung to a neighbour would
/// otherwise survive.
#[test]
fn every_urgency_rung_resolves_onto_the_output() {
    let (dir, ephem_chs) = surface_dir_and_ephem();
    for urgency in Urgency::ALL {
        let mut raw = valid_surface_raw();
        raw.outputs[0].urgency = Some(urgency);
        let resolved = resolve_surfaces(&[raw], &dir, &ephem_chs, &test_globals());
        assert_eq!(
            resolved[0].outputs[0].default_urgency, urgency,
            "output urgency {urgency:?} must survive resolution"
        );
    }
}

/// Happy path: bindings resolve, the presentational component is allowed, and
/// the carried policy enforces the same two-factor decision the coverage
/// checks used.
#[test]
fn surface_resolves_happy_path() {
    let (dir, ephem_chs) = surface_dir_and_ephem();
    let resolved = resolve_surfaces(&[valid_surface_raw()], &dir, &ephem_chs, &test_globals());
    assert_eq!(resolved.len(), 1);
    let s = &resolved[0];
    assert_eq!(s.slug, "deskbar");
    assert_eq!(
        s.components,
        vec![
            ResolvedComponent {
                instance: "protobar".to_string(),
                kind: "protobar".to_string(),
                abi: brenn_surface_proto::Abi::Dom,
                send_budget: SurfaceSendBudget::default(),
                parked_batch_depth: 8,
                config: Default::default(),
                chrome: false,
            },
            ResolvedComponent {
                instance: "sidecar".to_string(),
                kind: "sidecar".to_string(),
                abi: brenn_surface_proto::Abi::Dom,
                send_budget: SurfaceSendBudget::default(),
                parked_batch_depth: 8,
                config: Default::default(),
                chrome: false,
            },
            ResolvedComponent {
                instance: "chrome".to_string(),
                kind: "chrome".to_string(),
                abi: brenn_surface_proto::Abi::Dom,
                send_budget: SurfaceSendBudget::default(),
                parked_batch_depth: 8,
                config: Default::default(),
                chrome: true,
            },
        ]
    );
    assert_eq!(s.subscriptions.len(), 1);
    assert_eq!(
        s.subscriptions[0].channel_address,
        "ephemeral:protobar-demo"
    );
    assert_eq!(s.subscriptions[0].instance, "protobar");
    assert_eq!(s.subscriptions[0].port, "messages");
    assert_eq!(s.outputs.len(), 1);
    assert_eq!(s.outputs[0].channel_address, "brenn:alerts");
    assert_eq!(s.outputs[0].instance, "protobar");
    assert_eq!(s.outputs[0].port, "out");
    // Policy carried; it must authorize exactly what the bindings need.
    assert!(s.policy.allows_channel_access("ephemeral:protobar-demo"));
    assert!(s.policy.allows_brenn_publish("alerts"));
    // Access + budget defaults when the fields are unset.
    assert!(s.allowed_users.is_empty());
    assert!(s.user_has_access("anyone"));
    assert_eq!(s.publish_burst, DEFAULT_SURFACE_PUBLISH_BURST);
    assert_eq!(s.publish_per_sec, DEFAULT_SURFACE_PUBLISH_PER_SEC);
}

/// Injection adds the substrate error-reporting grant to every surface policy:
/// `MessagingPublish` + a covering `brenn_publish` matcher on the bare error
/// channel, leaving the surface's own publish coverage intact.
#[test]
fn inject_surface_error_grant_adds_covering_publish_acl() {
    let (dir, ephem_chs) = surface_dir_and_ephem();
    let mut resolved = resolve_surfaces(&[valid_surface_raw()], &dir, &ephem_chs, &test_globals());
    // Before injection the surface has a publish grant (for brenn:alerts) but no
    // matcher covering the error channel, so it cannot publish there.
    assert!(!resolved[0].policy.allows_brenn_publish("surface-errors"));

    inject_surface_error_grant(&mut resolved, "surface-errors");

    let s = &resolved[0];
    assert!(
        s.policy.allows_brenn_publish("surface-errors"),
        "injected grant + exact ACL must authorize the error channel"
    );
    // The surface's own publish coverage is untouched.
    assert!(s.policy.allows_brenn_publish("alerts"));
}

/// Injection adds the surface self-description telemetry grant: a
/// `MessagingPublish` grant plus exact `brenn_publish` matchers on the surface's
/// own geometry and status channels only, leaving its own publish coverage intact.
#[test]
fn inject_surface_geometry_status_grants_adds_covering_publish_acls() {
    let (dir, ephem_chs) = surface_dir_and_ephem();
    let mut resolved = resolve_surfaces(&[valid_surface_raw()], &dir, &ephem_chs, &test_globals());
    let slug = resolved[0].slug.clone();
    assert!(
        !resolved[0]
            .policy
            .allows_brenn_publish(&format!("surface.surface.{slug}.geometry"))
    );

    inject_surface_geometry_status_grants(&mut resolved, "surface");

    let s = &resolved[0];
    assert!(
        s.policy
            .allows_brenn_publish(&format!("surface.surface.{slug}.geometry")),
        "injected grant must authorize the geometry channel"
    );
    assert!(
        s.policy
            .allows_brenn_publish(&format!("surface.surface.{slug}.status")),
        "injected grant must authorize the status channel"
    );
    // Scoped to exactly its own two channels, not a foreign surface's.
    assert!(
        !s.policy
            .allows_brenn_publish("surface.surface.other.geometry")
    );
    // The surface's own publish coverage is untouched.
    assert!(s.policy.allows_brenn_publish("alerts"));
}

#[test]
fn surface_access_and_budgets_carry_through() {
    let (dir, ephem_chs) = surface_dir_and_ephem();
    let mut raw = valid_surface_raw();
    raw.allowed_users = vec!["alice".to_string()];
    raw.publish_burst = Some(120);
    raw.publish_per_sec = Some(5);
    let resolved = resolve_surfaces(&[raw], &dir, &ephem_chs, &test_globals());
    let s = &resolved[0];
    assert_eq!(s.allowed_users, vec!["alice".to_string()]);
    assert!(s.user_has_access("alice"));
    assert!(!s.user_has_access("intruder"));
    assert_eq!(s.publish_burst, 120);
    assert_eq!(s.publish_per_sec, 5);
}

/// A `brenn:` binding resolves into `durable_subscriptions`, inheriting the
/// channel's push/retain/noise/wake defaults when the knobs are unset. The
/// `SurfaceBinding` list still carries it too (it serves the `Welcome`
/// payload), but ephemeral bindings never enter `durable_subscriptions`.
#[test]
fn surface_durable_subscription_inherits_channel_defaults() {
    use brenn_lib::messaging::config::{Depth, NoiseLevel};
    let dir = make_bounded_brenn_dir("brenn:alerts");
    // Clear the fixture's explicit depths so the resolution inherits the
    // channel's (bounded) push/retain/noise/wake defaults.
    let mut raw = durable_surface_raw();
    raw.subscriptions[0].push_depth = None;
    raw.subscriptions[0].retain_depth = None;
    raw.subscriptions[0].noise = None;
    raw.subscriptions[0].wake_min = None;
    let resolved = resolve_surfaces(&[raw], &dir, &[], &test_globals());
    let s = &resolved[0];
    assert_eq!(
        s.subscriptions.len(),
        1,
        "SurfaceBinding carries the binding"
    );
    assert_eq!(s.durable_subscriptions.len(), 1);
    let ds = &s.durable_subscriptions[0].subscription;
    assert_eq!(ds.channel_address, "brenn:alerts");
    assert_eq!(ds.push_depth, Depth::Bounded(16));
    assert_eq!(ds.retain_depth, Depth::Bounded(8));
    assert_eq!(ds.noise, NoiseLevel::Silent);
    assert_eq!(ds.wake_min, brenn_lib::messaging::WakeMin::Normal);
}

/// A durable binding whose resolved `push_depth` is unbounded (here: inherited
/// from an unbounded channel default) is a boot panic — a surface projection's
/// per-subscribe replay must be bounded.
#[test]
#[should_panic(expected = "needs a bounded push_depth")]
fn surface_durable_push_depth_unbounded_panics() {
    use brenn_lib::messaging::config::Depth;
    let (dir, _addr) = make_brenn_dir("brenn:alerts");
    let mut raw = durable_surface_raw();
    // Leave push_depth to inherit the unbounded channel default; keep retain
    // bounded so the push check is what trips.
    raw.subscriptions[0].push_depth = None;
    raw.subscriptions[0].retain_depth = Some(Depth::Bounded(4));
    resolve_surfaces(&[raw], &dir, &[], &test_globals());
}

/// A durable binding whose resolved `retain_depth` is unbounded (inherited from
/// an unbounded channel default) is a boot panic — the per-subscribe replay
/// must be bounded.
#[test]
#[should_panic(expected = "needs a bounded retain_depth")]
fn surface_durable_retain_depth_unbounded_panics() {
    use brenn_lib::messaging::config::Depth;
    let (dir, _addr) = make_brenn_dir("brenn:alerts");
    let mut raw = durable_surface_raw();
    raw.subscriptions[0].push_depth = Some(Depth::Bounded(8));
    raw.subscriptions[0].retain_depth = None;
    resolve_surfaces(&[raw], &dir, &[], &test_globals());
}

/// Explicit per-subscription durable knobs override the channel defaults.
#[test]
fn surface_durable_subscription_explicit_knobs_carry() {
    use brenn_lib::messaging::config::SurfaceGrant;
    use brenn_lib::messaging::config::{Depth, NoiseLevel};
    let (dir, _addr) = make_brenn_dir("brenn:alerts");
    let mut raw = durable_surface_raw();
    raw.subscriptions[0].push_depth = Some(Depth::Bounded(4));
    raw.subscriptions[0].retain_depth = Some(Depth::Bounded(2));
    raw.subscriptions[0].noise = Some(NoiseLevel::Alarm);
    // `alarm` alerts on overflow, so the surface must hold the alert grant.
    raw.grants.push(SurfaceGrant::Alert);
    let resolved = resolve_surfaces(&[raw], &dir, &[], &test_globals());
    let ds = &resolved[0].durable_subscriptions[0].subscription;
    assert_eq!(ds.push_depth, Depth::Bounded(4));
    assert_eq!(ds.retain_depth, Depth::Bounded(2));
    assert_eq!(ds.noise, NoiseLevel::Alarm);
    // The per-binding resolved noise is held on the `SurfaceBinding` too,
    // class-uniform — the same value the durable subscriber entry carries.
    assert_eq!(resolved[0].subscriptions[0].noise, NoiseLevel::Alarm);
}

/// An explicit `wake_min` on a durable surface subscription is a config error:
/// surfaces are always delivered eagerly, so the knob does nothing (design §5).
#[test]
#[should_panic(expected = "always delivered eagerly")]
fn surface_durable_subscription_explicit_wake_min_panics() {
    use brenn_lib::messaging::config::Depth;
    let (dir, _addr) = make_brenn_dir("brenn:alerts");
    let mut raw = durable_surface_raw();
    raw.subscriptions[0].push_depth = Some(Depth::Bounded(4));
    raw.subscriptions[0].retain_depth = Some(Depth::Bounded(2));
    raw.subscriptions[0].wake_min = Some(brenn_lib::messaging::WakeMin::Never);
    resolve_surfaces(&[raw], &dir, &[], &test_globals());
}

/// `push_depth` is not a durable knob: every class puts a page queue in front of
/// the port, so an ephemeral binding's value resolves onto its binding.
#[test]
fn surface_ephemeral_binding_honours_push_depth() {
    use brenn_lib::messaging::config::Depth;
    let (dir, ephem_chs) = surface_dir_and_ephem();
    let mut raw = valid_surface_raw();
    raw.subscriptions[0].push_depth = Some(Depth::Bounded(3));
    let resolved = resolve_surfaces(&[raw], &dir, &ephem_chs, &test_globals());
    assert_eq!(resolved[0].subscriptions[0].push_depth, 3);
}

/// An unset ephemeral binding inherits the `[[ephemeral_channel]]` rung's
/// `push_depth` — binding → channel → global, class-uniform with `brenn:`. The
/// `ephem` fixture's channel rung is 8.
#[test]
fn surface_ephemeral_binding_inherits_the_channel_push_depth() {
    let (dir, ephem_chs) = surface_dir_and_ephem();
    let resolved = resolve_surfaces(&[valid_surface_raw()], &dir, &ephem_chs, &test_globals());
    assert_eq!(resolved[0].subscriptions[0].push_depth, 8);
}

/// A distinct channel-rung `push_depth` (5, not the global 8) reaches an unset
/// binding — the middle rung is real, not a stand-in for the global.
#[test]
fn surface_ephemeral_binding_inherits_a_distinct_channel_push_depth() {
    use brenn_lib::messaging::config::{Depth, NoiseLevel};
    let (dir, _addr) = make_brenn_dir("brenn:alerts");
    let ephem_chs = vec![ephem_with(
        "protobar-demo",
        Depth::Bounded(5),
        1,
        NoiseLevel::Silent,
    )];
    let resolved = resolve_surfaces(&[valid_surface_raw()], &dir, &ephem_chs, &test_globals());
    assert_eq!(resolved[0].subscriptions[0].push_depth, 5);
}

/// An ephemeral binding whose whole ladder resolves unbounded — channel rung
/// `Unbounded`, binding and global unset — is the class-uniform page-queue panic:
/// the port queue is page memory and cannot be a tab that grows until it dies.
#[test]
#[should_panic(expected = "must resolve to a bounded push_depth")]
fn surface_ephemeral_binding_with_an_unbounded_resolved_push_depth_panics() {
    use brenn_lib::messaging::config::{Depth, NoiseLevel};
    let (dir, _addr) = make_brenn_dir("brenn:alerts");
    let ephem_chs = vec![ephem_with(
        "protobar-demo",
        Depth::Unbounded,
        1,
        NoiseLevel::Silent,
    )];
    resolve_surfaces(
        &[valid_surface_raw()],
        &dir,
        &ephem_chs,
        &Default::default(),
    );
}

/// Depth 0 is the bus's sampled/context-only port, and it is legal on every
/// surface binding: every component rides activations, so every depth-0 port has
/// a window to be read as context on. The binding declares retained context, so
/// it carries something.
#[test]
fn surface_ephemeral_binding_push_depth_zero_resolves() {
    use brenn_lib::messaging::config::{Depth, SurfaceSubscriptionRaw};
    let (dir, ephem_chs) = surface_dir_and_ephem();
    let mut raw = valid_surface_raw();
    // A sibling triggering port on the same instance: depth-0 ports are read as
    // context when some *other* port activates, so an instance needs one.
    raw.subscriptions.push(SurfaceSubscriptionRaw {
        push_depth: Some(Depth::Bounded(0)),
        retain_depth: Some(Depth::Bounded(4)),
        ..surface_sub_raw("ephemeral:protobar-demo", "protobar", "context")
    });
    let resolved = resolve_surfaces(&[raw], &dir, &ephem_chs, &test_globals());
    let ctx = resolved[0]
        .subscriptions
        .iter()
        .find(|b| b.port == "context")
        .expect("the depth-0 binding resolved");
    assert_eq!(ctx.push_depth, 0);
    assert_eq!(ctx.retain_depth, 4);
}

/// `retain_depth` on an ephemeral binding is meaningful: it is the depth of the
/// page's own context window on that subscription, and the binding's own value
/// overrides the channel rung.
#[test]
fn surface_ephemeral_binding_retain_depth_resolves() {
    use brenn_lib::messaging::config::Depth;
    let (dir, ephem_chs) = surface_dir_and_ephem();
    let mut raw = valid_surface_raw();
    raw.subscriptions[0].retain_depth = Some(Depth::Bounded(3));
    let resolved = resolve_surfaces(&[raw], &dir, &ephem_chs, &test_globals());
    assert_eq!(resolved[0].subscriptions[0].retain_depth, 3);
}

/// An ephemeral binding that states no `retain_depth` inherits the
/// `[[ephemeral_channel]]` rung — binding → channel → global, class-uniform with
/// `brenn:`. The `ephem` fixture's channel rung is 1.
#[test]
fn surface_ephemeral_binding_retain_depth_inherits_the_channel_rung() {
    use brenn_lib::messaging::config::{Depth, NoiseLevel};
    let (dir, _addr) = make_brenn_dir("brenn:alerts");
    let ephem_chs = vec![ephem_with(
        "protobar-demo",
        Depth::Bounded(8),
        6,
        NoiseLevel::Silent,
    )];
    let raw = valid_surface_raw();
    assert!(raw.subscriptions[0].retain_depth.is_none());
    let resolved = resolve_surfaces(&[raw], &dir, &ephem_chs, &test_globals());
    assert_eq!(resolved[0].subscriptions[0].retain_depth, 6);
}

/// An unbounded `retain_depth` is a named panic on every class: the retained
/// ring is page memory, so "unbounded" is a tab that grows until it dies.
#[test]
#[should_panic(expected = "retained context ring lives in page memory")]
fn surface_ephemeral_binding_unbounded_retain_depth_panics() {
    use brenn_lib::messaging::config::Depth;
    let (dir, ephem_chs) = surface_dir_and_ephem();
    let mut raw = valid_surface_raw();
    raw.subscriptions[0].retain_depth = Some(Depth::Unbounded);
    resolve_surfaces(&[raw], &dir, &ephem_chs, &test_globals());
}

/// `noise` on an ephemeral binding no longer panics — the class fork is gone. It
/// resolves binding → channel → global and is held on the binding, unread until
/// the surface noise ladder lands, exactly as a durable binding's noise is.
#[test]
fn surface_noise_on_ephemeral_binding_resolves_not_panics() {
    use brenn_lib::messaging::NoiseLevel;
    let (dir, ephem_chs) = surface_dir_and_ephem();
    let mut raw = valid_surface_raw();
    raw.subscriptions[0].noise = Some(NoiseLevel::Metered);
    let resolved = resolve_surfaces(&[raw], &dir, &ephem_chs, &test_globals());
    assert_eq!(resolved[0].subscriptions[0].noise, NoiseLevel::Metered);
}

/// An unset ephemeral binding inherits the channel rung's `noise` — the middle
/// rung is real. The `ephem_with` fixture's rung is `Alarm`.
#[test]
fn surface_ephemeral_binding_inherits_the_channel_noise() {
    use brenn_lib::messaging::config::SurfaceGrant;
    use brenn_lib::messaging::config::{Depth, NoiseLevel};
    let (dir, _addr) = make_brenn_dir("brenn:alerts");
    let ephem_chs = vec![ephem_with(
        "protobar-demo",
        Depth::Bounded(8),
        1,
        NoiseLevel::Alarm,
    )];
    // The inherited `alarm` alerts on overflow, so the surface must hold the
    // alert grant.
    let mut raw = valid_surface_raw();
    raw.grants.push(SurfaceGrant::Alert);
    let resolved = resolve_surfaces(&[raw], &dir, &ephem_chs, &test_globals());
    assert_eq!(resolved[0].subscriptions[0].noise, NoiseLevel::Alarm);
}

/// `wake_min` is rejected on an ephemeral binding by the one class-blind
/// rejection every surface binding meets — surfaces are always delivered eagerly.
/// The same text answers `brenn:`, `ephemeral:`, and `local:` alike.
#[test]
#[should_panic(expected = "always delivered eagerly")]
fn surface_wake_min_on_ephemeral_binding_panics() {
    use brenn_lib::messaging::WakeMin;
    let (dir, ephem_chs) = surface_dir_and_ephem();
    let mut raw = valid_surface_raw();
    raw.subscriptions[0].wake_min = Some(WakeMin::Normal);
    resolve_surfaces(&[raw], &dir, &ephem_chs, &test_globals());
}

/// A `local:` binding's context depth is its read into the router's ring. An
/// operator-declared channel takes the binding's own number.
#[test]
fn surface_local_binding_retain_depth_resolves() {
    use brenn_lib::messaging::config::Depth;
    let mut raw = local_surface_raw();
    raw.subscriptions[0].retain_depth = Some(Depth::Bounded(4));
    let resolved = resolve_surfaces(&[raw], &dir_of(vec![]), &[], &test_globals());
    assert_eq!(resolved[0].subscriptions[0].retain_depth, 4);
    // The ring the binding reads is folded from the same number.
    assert_eq!(resolved[0].local_channels[0].ring_depth, 4);
}

/// A `local:` binding that states nothing reads 1, not 0 — where this class
/// parts from the others' default, and must: the router keeps a floor-1 ring for
/// every local channel whether or not a binding asks for it, so a 0 default
/// would leave the binding blind to a ring the kernel is already filling.
#[test]
fn surface_local_binding_retain_depth_defaults_to_the_rings_floor() {
    let raw = local_surface_raw();
    assert!(raw.subscriptions[0].retain_depth.is_none());
    let resolved = resolve_surfaces(&[raw], &dir_of(vec![]), &[], &test_globals());
    assert_eq!(resolved[0].subscriptions[0].retain_depth, 1);
    assert_eq!(resolved[0].local_channels[0].ring_depth, 1);
}

/// A `local:` binding's noise joins the ladder: binding → global (no channel
/// rung on this class). An explicit rung resolves onto the binding rather than
/// being rejected — the relaxation that lands with the surface noise ladder.
#[test]
fn surface_local_binding_explicit_noise_resolves() {
    use brenn_lib::messaging::config::{NoiseLevel, SurfaceGrant};
    let mut raw = local_surface_raw();
    raw.subscriptions[0].noise = Some(NoiseLevel::Alarm);
    // `alarm` alerts on overflow, so the surface must hold the alert grant — even
    // on a `local:` binding, whose kernel-side overflow still rides the plane.
    raw.grants.push(SurfaceGrant::Alert);
    let resolved = resolve_surfaces(&[raw], &dir_of(vec![]), &[], &test_globals());
    assert_eq!(resolved[0].subscriptions[0].noise, NoiseLevel::Alarm);
}

/// `alarm`/`fatal` on a surface binding alert on overflow, so the surface must
/// hold the alert grant — a loud binding without it is dead config (the kernel's
/// overflow alert would be denied at the plane). Named boot panic.
#[test]
#[should_panic(expected = "requires the surface's `alert` grant")]
fn surface_loud_noise_without_alert_grant_panics() {
    use brenn_lib::messaging::config::NoiseLevel;
    let mut raw = local_surface_raw();
    raw.subscriptions[0].noise = Some(NoiseLevel::Alarm);
    resolve_surfaces(&[raw], &dir_of(vec![]), &[], &test_globals());
}

/// The grant gate is stated as covering `fatal` too, but it reaches the assert
/// only because `NoiseLevel: Ord` places `Fatal` above `Alarm` — a cross-crate
/// declaration-order assumption. Pinned directly so reordering the ladder cannot
/// let `fatal` boot as dead config.
#[test]
#[should_panic(expected = "requires the surface's `alert` grant")]
fn surface_fatal_noise_without_alert_grant_panics() {
    use brenn_lib::messaging::config::NoiseLevel;
    let mut raw = local_surface_raw();
    raw.subscriptions[0].noise = Some(NoiseLevel::Fatal);
    resolve_surfaces(&[raw], &dir_of(vec![]), &[], &test_globals());
}

/// `metered` and `silent` do not alert, so they need no grant — the check gates
/// the loud rungs only.
#[test]
fn surface_metered_noise_needs_no_alert_grant() {
    use brenn_lib::messaging::config::NoiseLevel;
    let mut raw = local_surface_raw();
    raw.subscriptions[0].noise = Some(NoiseLevel::Metered);
    let resolved = resolve_surfaces(&[raw], &dir_of(vec![]), &[], &test_globals());
    assert_eq!(resolved[0].subscriptions[0].noise, NoiseLevel::Metered);
}

/// A `local:` binding that states no noise inherits the global default (`Silent`
/// in the test globals) — binding → global with no channel rung.
#[test]
fn surface_local_binding_noise_defaults_to_global() {
    use brenn_lib::messaging::config::NoiseLevel;
    let raw = local_surface_raw();
    assert!(raw.subscriptions[0].noise.is_none());
    let resolved = resolve_surfaces(&[raw], &dir_of(vec![]), &[], &test_globals());
    assert_eq!(resolved[0].subscriptions[0].noise, NoiseLevel::Silent);
}

/// A reserved control plane's depth is contract-fixed, and a binding on one
/// reads exactly that — the depth-1 planes replay their last value to whoever
/// attaches, which is the whole point of the late-attach handoff.
#[test]
fn surface_reserved_plane_binding_reads_the_contract_fixed_depth() {
    let mut raw = local_surface_raw();
    raw.subscriptions[0] = surface_sub_raw("local:brenn/theme", "protobar", "theme-in");
    let resolved = resolve_surfaces(&[raw], &dir_of(vec![]), &[], &test_globals());
    let fixed = brenn_surface_proto::reserved_local_channel("local:brenn/theme")
        .expect("theme is a reserved plane")
        .ring_depth;
    assert_eq!(resolved[0].subscriptions[0].retain_depth, fixed);
}

/// An output's sink budget resolves with the backend's spelling, semantics, and
/// defaults, to millitokens. The kernel enforces these numbers; the server
/// resolves them once and advertises them.
#[test]
fn surface_output_budget_resolves_to_millitokens() {
    let (dir, ephem_chs) = surface_dir_and_ephem();
    let mut raw = valid_surface_raw();
    raw.outputs[0].publish_per_activation = Some(2.5);
    raw.outputs[0].publish_capacity = Some(0.5);
    let resolved = resolve_surfaces(&[raw], &dir, &ephem_chs, &test_globals());
    assert_eq!(resolved[0].outputs[0].budget.fill_mt, 2500);
    assert_eq!(resolved[0].outputs[0].budget.capacity_mt, 500);
}

/// Unstated budget knobs take the same defaults `[[wasm_consumer.output]]`
/// takes: one publish per activation, one carried over.
#[test]
fn surface_output_budget_defaults_match_the_backend_knobs() {
    use brenn_lib::messaging::config::{
        DEFAULT_WASM_PUBLISH_CAPACITY, DEFAULT_WASM_PUBLISH_PER_ACTIVATION, MILLITOKENS_PER_PUBLISH,
    };
    let (dir, ephem_chs) = surface_dir_and_ephem();
    let raw = valid_surface_raw();
    assert!(raw.outputs[0].publish_per_activation.is_none());
    let resolved = resolve_surfaces(&[raw], &dir, &ephem_chs, &test_globals());
    assert_eq!(
        resolved[0].outputs[0].budget.fill_mt,
        (DEFAULT_WASM_PUBLISH_PER_ACTIVATION * MILLITOKENS_PER_PUBLISH as f64) as u64,
    );
    assert_eq!(
        resolved[0].outputs[0].budget.capacity_mt,
        (DEFAULT_WASM_PUBLISH_CAPACITY * MILLITOKENS_PER_PUBLISH as f64) as u64,
    );
}

/// `0` fill is legal and means purely input-driven — the sink publishes only
/// what its inputs grant it. Distinct from a sink that may never publish.
#[test]
fn surface_output_zero_fill_is_input_driven_not_rejected() {
    let (dir, ephem_chs) = surface_dir_and_ephem();
    let mut raw = valid_surface_raw();
    raw.outputs[0].publish_per_activation = Some(0.0);
    let resolved = resolve_surfaces(&[raw], &dir, &ephem_chs, &test_globals());
    assert_eq!(resolved[0].outputs[0].budget.fill_mt, 0);
}

/// The shared resolver's validation reaches the surface block: a knob that would
/// round to 0 millitokens is rejected rather than silently disabling the sink.
#[test]
#[should_panic(expected = "would round to 0")]
fn surface_output_budget_rounding_to_zero_panics() {
    let (dir, ephem_chs) = surface_dir_and_ephem();
    let mut raw = valid_surface_raw();
    raw.outputs[0].publish_per_activation = Some(0.0001);
    resolve_surfaces(&[raw], &dir, &ephem_chs, &test_globals());
}

/// …and a non-finite knob, on the same shared resolver.
#[test]
#[should_panic(expected = "must be finite")]
fn surface_output_budget_nan_panics() {
    let (dir, ephem_chs) = surface_dir_and_ephem();
    let mut raw = valid_surface_raw();
    raw.outputs[0].publish_capacity = Some(f64::NAN);
    resolve_surfaces(&[raw], &dir, &ephem_chs, &test_globals());
}

/// An unstated `parked_batch_depth` takes the stated default.
#[test]
fn surface_parked_batch_depth_defaults() {
    use brenn_lib::messaging::config::DEFAULT_PARKED_BATCH_DEPTH;
    let (dir, ephem_chs) = surface_dir_and_ephem();
    let raw = valid_surface_raw();
    assert!(raw.components[0].parked_batch_depth.is_none());
    let resolved = resolve_surfaces(&[raw], &dir, &ephem_chs, &test_globals());
    assert_eq!(
        resolved[0].components[0].parked_batch_depth,
        DEFAULT_PARKED_BATCH_DEPTH
    );
}

/// A stated `parked_batch_depth` overrides the default, per instance.
#[test]
fn surface_parked_batch_depth_override_resolves() {
    use brenn_lib::messaging::config::Depth;
    let (dir, ephem_chs) = surface_dir_and_ephem();
    let mut raw = valid_surface_raw();
    raw.components[0].parked_batch_depth = Some(Depth::Bounded(3));
    let resolved = resolve_surfaces(&[raw], &dir, &ephem_chs, &test_globals());
    assert_eq!(resolved[0].components[0].parked_batch_depth, 3);
    // Per instance: the sibling that stated nothing is untouched.
    assert_eq!(resolved[0].components[1].parked_batch_depth, 8);
}

/// Depth 0 drops every flush an activation makes while the link is down — dead
/// config, not a bound.
#[test]
#[should_panic(expected = "parked_batch_depth = 0")]
fn surface_parked_batch_depth_zero_panics() {
    use brenn_lib::messaging::config::Depth;
    let (dir, ephem_chs) = surface_dir_and_ephem();
    let mut raw = valid_surface_raw();
    raw.components[0].parked_batch_depth = Some(Depth::Bounded(0));
    resolve_surfaces(&[raw], &dir, &ephem_chs, &test_globals());
}

/// Unbounded is a page queue that grows for the length of the outage.
#[test]
#[should_panic(expected = "parked_batch_depth = \"unbounded\"")]
fn surface_parked_batch_depth_unbounded_panics() {
    use brenn_lib::messaging::config::Depth;
    let (dir, ephem_chs) = surface_dir_and_ephem();
    let mut raw = valid_surface_raw();
    raw.components[0].parked_batch_depth = Some(Depth::Unbounded);
    resolve_surfaces(&[raw], &dir, &ephem_chs, &test_globals());
}

/// A durable binding's page-queue depth is the same number its subscription
/// resolved (binding → channel → global): one operator knob, applied at the
/// server's push rows and at the page's queue, so the two cannot drift.
#[test]
fn surface_durable_binding_carries_its_subscriptions_push_depth() {
    use brenn_lib::messaging::config::Depth;
    let (dir, _addr) = make_brenn_dir("brenn:alerts");
    let mut raw = durable_surface_raw();
    raw.subscriptions[0].push_depth = Some(Depth::Bounded(5));
    let resolved = resolve_surfaces(&[raw], &dir, &[], &test_globals());
    assert_eq!(resolved[0].subscriptions[0].push_depth, 5);
    assert_eq!(
        resolved[0].durable_subscriptions[0].subscription.push_depth,
        Depth::Bounded(5)
    );
}

/// One instance binding one durable channel on two ports is the only genuinely
/// shared surface subscription: one `ResolvedSubscription`, whose window folds
/// by **max** over the bindings' declared depths — while each port's page queue
/// keeps its own binding's number. Both bindings state a depth: neither is
/// ignored and neither wins by position.
#[test]
fn surface_one_instance_two_ports_share_a_subscription_folded_by_max() {
    use brenn_lib::messaging::config::Depth;
    let dir = make_bounded_brenn_dir("brenn:alerts");
    let mut raw = durable_surface_raw();
    raw.subscriptions[0].push_depth = Some(Depth::Bounded(6));
    raw.subscriptions[0].retain_depth = Some(Depth::Bounded(2));
    let mut alt = surface_sub_raw("brenn:alerts", "protobar", "alt");
    alt.push_depth = Some(Depth::Bounded(9));
    alt.retain_depth = Some(Depth::Bounded(5));
    raw.subscriptions.push(alt);
    let resolved = resolve_surfaces(&[raw], &dir, &[], &test_globals());

    // Each port queue takes its own binding's depth — the queue is the port's.
    assert_eq!(resolved[0].subscriptions[0].push_depth, 6);
    assert_eq!(resolved[0].subscriptions[1].push_depth, 9);

    // One subscription, its window covering the hungriest port on it.
    assert_eq!(resolved[0].durable_subscriptions.len(), 1);
    let ds = &resolved[0].durable_subscriptions[0];
    assert_eq!(ds.instance, "protobar");
    assert_eq!(ds.subscription.push_depth, Depth::Bounded(9));
    // `retain_depth` folds by max alongside it — both are capacities on the one
    // shared window. Declared differently per binding so the fold is observable:
    // first-binding-wins would resolve 2 and shrink the hungrier port's replay
    // window to whichever block the operator happened to write first.
    assert_eq!(ds.subscription.retain_depth, Depth::Bounded(5));
}

/// The max-fold is order-independent: the same two bindings declared in the
/// other order resolve the same window. This is the property that makes the rule
/// not positional — the defect the first-binding-wins rule had.
#[test]
fn surface_shared_subscription_max_fold_is_order_independent() {
    use brenn_lib::messaging::config::Depth;
    // Both folded knobs, so neither can be positional. The two are varied in
    // opposite directions within each call, so a fold that read the wrong
    // binding for one of them could not pass by luck.
    let fold = |first: (u64, u64), second: (u64, u64)| {
        let dir = make_bounded_brenn_dir("brenn:alerts");
        let mut raw = durable_surface_raw();
        raw.subscriptions[0].push_depth = Some(Depth::Bounded(first.0));
        raw.subscriptions[0].retain_depth = Some(Depth::Bounded(first.1));
        let mut alt = surface_sub_raw("brenn:alerts", "protobar", "alt");
        alt.push_depth = Some(Depth::Bounded(second.0));
        alt.retain_depth = Some(Depth::Bounded(second.1));
        raw.subscriptions.push(alt);
        let resolved = resolve_surfaces(&[raw], &dir, &[], &test_globals());
        let ds = &resolved[0].durable_subscriptions[0].subscription;
        (ds.push_depth, ds.retain_depth)
    };
    let expected = (Depth::Bounded(9), Depth::Bounded(5));
    assert_eq!(fold((6, 5), (9, 2)), expected);
    assert_eq!(fold((9, 2), (6, 5)), expected);
}

/// `noise` is the one shared-subscription knob that does **not** fold: it is a
/// policy, not a capacity (`fatal` alters control flow), so two bindings of one
/// (instance, channel) declaring different loudness is an operator
/// contradiction about one event, and the answer is fail-fast rather than
/// picking a winner. Order-independent — it fires on the set disagreeing — so it
/// is not the positional semantics D-12 forbids.
#[test]
#[should_panic(expected = "conflicting noise")]
fn surface_shared_subscription_conflicting_noise_panics() {
    use brenn_lib::messaging::config::{Depth, NoiseLevel, SurfaceGrant};
    let dir = make_bounded_brenn_dir("brenn:alerts");
    let mut raw = durable_surface_raw();
    raw.grants.push(SurfaceGrant::Alert);
    raw.subscriptions[0].push_depth = Some(Depth::Bounded(6));
    raw.subscriptions[0].noise = Some(NoiseLevel::Alarm);
    let mut alt = surface_sub_raw("brenn:alerts", "protobar", "alt");
    alt.push_depth = Some(Depth::Bounded(9));
    alt.noise = Some(NoiseLevel::Silent);
    raw.subscriptions.push(alt);
    resolve_surfaces(&[raw], &dir, &[], &test_globals());
}

/// The passing twin: agreeing bindings resolve, and the agreed loudness carries
/// onto the shared subscription. Without this, the assert above could be
/// "fixed" by making it unconditional — a rule that rejects every shared
/// subscription would pass the panic test just as well.
#[test]
fn surface_shared_subscription_agreeing_noise_carries() {
    use brenn_lib::messaging::config::{Depth, NoiseLevel, SurfaceGrant};
    let dir = make_bounded_brenn_dir("brenn:alerts");
    let mut raw = durable_surface_raw();
    raw.grants.push(SurfaceGrant::Alert);
    raw.subscriptions[0].push_depth = Some(Depth::Bounded(6));
    raw.subscriptions[0].noise = Some(NoiseLevel::Alarm);
    let mut alt = surface_sub_raw("brenn:alerts", "protobar", "alt");
    alt.push_depth = Some(Depth::Bounded(9));
    alt.noise = Some(NoiseLevel::Alarm);
    raw.subscriptions.push(alt);
    let resolved = resolve_surfaces(&[raw], &dir, &[], &test_globals());

    assert_eq!(resolved[0].durable_subscriptions.len(), 1);
    let ds = &resolved[0].durable_subscriptions[0];
    assert_eq!(ds.subscription.noise, NoiseLevel::Alarm);
}

/// Two *different* instances on one channel are two principals: two
/// subscriptions, two windows, two cursors — the same shape two `[[app]]`
/// blocks on one channel produce. Nothing is shared, so nothing folds.
#[test]
fn surface_sibling_instances_on_one_channel_are_two_subscriptions() {
    use brenn_lib::messaging::config::Depth;
    let dir = make_bounded_brenn_dir("brenn:alerts");
    let mut raw = durable_surface_raw();
    raw.subscriptions[0].push_depth = Some(Depth::Bounded(6));
    raw.components.push(component_raw("agenda-bob"));
    let mut bob = surface_sub_raw("brenn:alerts", "agenda-bob", "in");
    bob.push_depth = Some(Depth::Bounded(9));
    raw.subscriptions.push(bob);
    let resolved = resolve_surfaces(&[raw], &dir, &[], &test_globals());

    assert_eq!(
        resolved[0].durable_subscriptions.len(),
        2,
        "each instance owns its own subscription"
    );
    let mut by_instance: Vec<(&str, Depth)> = resolved[0]
        .durable_subscriptions
        .iter()
        .map(|d| (d.instance.as_str(), d.subscription.push_depth))
        .collect();
    by_instance.sort();
    assert_eq!(
        by_instance,
        vec![
            ("agenda-bob", Depth::Bounded(9)),
            ("protobar", Depth::Bounded(6)),
        ],
        "each keeps its own declared depth — sibling windows never fold together"
    );
}

/// Depth 0 on a durable binding takes the same rule as every other class: legal,
/// sampled/context-only. The maxim's answer — the classes differ only in
/// persistence, never in what config is expressible.
#[test]
fn surface_durable_push_depth_zero_resolves() {
    use brenn_lib::messaging::config::{Depth, SurfaceSubscriptionRaw};
    let (dir, _addr) = make_brenn_dir("brenn:alerts");
    let mut raw = durable_surface_raw();
    raw.subscriptions.push(SurfaceSubscriptionRaw {
        push_depth: Some(Depth::Bounded(0)),
        retain_depth: Some(Depth::Bounded(4)),
        ..surface_sub_raw("brenn:alerts", "protobar", "context")
    });
    let resolved = resolve_surfaces(&[raw], &dir, &[], &test_globals());
    let ctx = resolved[0]
        .subscriptions
        .iter()
        .find(|b| b.port == "context")
        .expect("the depth-0 binding resolved");
    assert_eq!(ctx.push_depth, 0);
    assert_eq!(ctx.retain_depth, 4);
}

/// An explicit non-default skin resolves through.
#[test]
fn surface_skin_carries_through() {
    let (dir, ephem_chs) = surface_dir_and_ephem();
    let mut raw = valid_surface_raw();
    raw.skin = Some("foundry".to_string());
    let resolved = resolve_surfaces(&[raw], &dir, &ephem_chs, &test_globals());
    assert_eq!(resolved[0].skin, "foundry");
}

/// An unknown skin name is dead config — the page handler would link a
/// nonexistent stylesheet — so boot panics.
#[test]
#[should_panic(expected = "unknown skin")]
fn surface_unknown_skin_panics() {
    let (dir, ephem_chs) = surface_dir_and_ephem();
    let mut raw = valid_surface_raw();
    raw.skin = Some("nonesuch".to_string());
    resolve_surfaces(&[raw], &dir, &ephem_chs, &test_globals());
}

#[test]
#[should_panic(expected = "synchronous startup-attach bound")]
fn surface_subscription_count_over_startup_attach_bound_panics() {
    let (dir, ephem_chs) = surface_dir_and_ephem();
    let mut raw = valid_surface_raw();
    // One binding per distinct port on the covered ephemeral channel, one
    // past the shell's synchronous startup-attach bound.
    raw.subscriptions = (0..=brenn_surface_proto::MAX_SURFACE_SUBSCRIPTION_BINDINGS)
        .map(|i| surface_sub_raw("ephemeral:protobar-demo", "protobar", &format!("p{i}")))
        .collect();
    resolve_surfaces(&[raw], &dir, &ephem_chs, &test_globals());
}

#[test]
#[should_panic(expected = "allowed_users entry must be non-empty")]
fn surface_empty_allowed_user_panics() {
    let (dir, ephem_chs) = surface_dir_and_ephem();
    let mut raw = valid_surface_raw();
    raw.allowed_users = vec!["".to_string()];
    resolve_surfaces(&[raw], &dir, &ephem_chs, &test_globals());
}

#[test]
#[should_panic(expected = "duplicate allowed_users entry")]
fn surface_duplicate_allowed_user_panics() {
    let (dir, ephem_chs) = surface_dir_and_ephem();
    let mut raw = valid_surface_raw();
    raw.allowed_users = vec!["alice".to_string(), "alice".to_string()];
    resolve_surfaces(&[raw], &dir, &ephem_chs, &test_globals());
}

#[test]
#[should_panic(expected = "publish_burst must be >= 1")]
fn surface_zero_publish_burst_panics() {
    let (dir, ephem_chs) = surface_dir_and_ephem();
    let mut raw = valid_surface_raw();
    raw.publish_burst = Some(0);
    resolve_surfaces(&[raw], &dir, &ephem_chs, &test_globals());
}

#[test]
#[should_panic(expected = "publish_per_sec must be >= 1")]
fn surface_zero_publish_per_sec_panics() {
    let (dir, ephem_chs) = surface_dir_and_ephem();
    let mut raw = valid_surface_raw();
    raw.publish_per_sec = Some(0);
    resolve_surfaces(&[raw], &dir, &ephem_chs, &test_globals());
}

/// At-ceiling budgets (exactly the bus per-sender constants) resolve: equal
/// sizes are safe because both buckets start full, so the connection bucket
/// trips no later than the bus gate.
#[test]
fn surface_at_ceiling_publish_budgets_resolve() {
    let (dir, ephem_chs) = surface_dir_and_ephem();
    let mut raw = valid_surface_raw();
    raw.publish_burst = Some(EPHEMERAL_SENDER_BURST);
    raw.publish_per_sec = Some(EPHEMERAL_SENDER_REFILL_AMOUNT);
    let resolved = resolve_surfaces(&[raw], &dir, &ephem_chs, &test_globals());
    assert_eq!(resolved[0].publish_burst, EPHEMERAL_SENDER_BURST);
    assert_eq!(resolved[0].publish_per_sec, EPHEMERAL_SENDER_REFILL_AMOUNT);
}

#[test]
#[should_panic(expected = "exceeds the bus per-sender burst")]
fn surface_over_ceiling_publish_burst_panics() {
    let (dir, ephem_chs) = surface_dir_and_ephem();
    let mut raw = valid_surface_raw();
    raw.publish_burst = Some(EPHEMERAL_SENDER_BURST + 1);
    resolve_surfaces(&[raw], &dir, &ephem_chs, &test_globals());
}

#[test]
#[should_panic(expected = "exceeds the bus per-sender refill")]
fn surface_over_ceiling_publish_per_sec_panics() {
    let (dir, ephem_chs) = surface_dir_and_ephem();
    let mut raw = valid_surface_raw();
    raw.publish_per_sec = Some(EPHEMERAL_SENDER_REFILL_AMOUNT + 1);
    resolve_surfaces(&[raw], &dir, &ephem_chs, &test_globals());
}

#[test]
#[should_panic(expected = "must consist of RFC 3986 unreserved")]
fn surface_bad_slug_charset_panics() {
    let (dir, ephem_chs) = surface_dir_and_ephem();
    let mut raw = valid_surface_raw();
    raw.slug = "desk:bar".to_string();
    resolve_surfaces(&[raw], &dir, &ephem_chs, &test_globals());
}

#[test]
#[should_panic(expected = "duplicate [[surface]] slug")]
fn surface_duplicate_slug_panics() {
    let (dir, ephem_chs) = surface_dir_and_ephem();
    resolve_surfaces(
        &[valid_surface_raw(), valid_surface_raw()],
        &dir,
        &ephem_chs,
        &test_globals(),
    );
}

// A second component of an already-present kind is fine (one wasm module, N
// elements) — what must be unique is the *instance* id. Pushing another
// `protobar` with a defaulted instance collides on instance "protobar".
#[test]
#[should_panic(expected = "duplicate component instance")]
fn surface_duplicate_component_instance_panics() {
    use brenn_lib::messaging::config::SurfaceComponentRaw;
    let (dir, ephem_chs) = surface_dir_and_ephem();
    let mut raw = valid_surface_raw();
    raw.components.push(SurfaceComponentRaw {
        kind: "protobar".to_string(),
        instance: None,
        abi: "dom".to_string(),
        send_burst: None,
        send_refill_secs: None,
        parked_batch_depth: None,
        config: None,
        chrome: false,
    });
    resolve_surfaces(&[raw], &dir, &ephem_chs, &test_globals());
}

/// Exactly one component per surface must carry `chrome = true`. The valid
/// fixture declares one, so it resolves — the exactly-one leg of the singleton
/// invariant.
#[test]
fn surface_exactly_one_chrome_resolves() {
    let (dir, ephem_chs) = surface_dir_and_ephem();
    let resolved = resolve_surfaces(&[valid_surface_raw()], &dir, &ephem_chs, &test_globals());
    let chrome: Vec<&str> = resolved[0]
        .components
        .iter()
        .filter(|c| c.chrome)
        .map(|c| c.instance.as_str())
        .collect();
    assert_eq!(chrome, vec!["chrome"], "exactly one chrome component");
}

/// Zero `chrome = true` components is a boot panic: the surface has no
/// privileged renderer and the wire's `chrome_instance` cannot be filled.
#[test]
#[should_panic(expected = "declares 0 components with `chrome = true`")]
fn surface_zero_chrome_panics() {
    let (dir, ephem_chs) = surface_dir_and_ephem();
    let mut raw = valid_surface_raw();
    for comp in &mut raw.components {
        comp.chrome = false;
    }
    resolve_surfaces(&[raw], &dir, &ephem_chs, &test_globals());
}

/// Two `chrome = true` components is a boot panic: the designation is ambiguous
/// and the singleton `chrome_instance` wire field unrepresentable-right.
#[test]
#[should_panic(expected = "declares 2 components with `chrome = true`")]
fn surface_two_chrome_panics() {
    let (dir, ephem_chs) = surface_dir_and_ephem();
    let mut raw = valid_surface_raw();
    // The fixture already carries one chrome; mark a second.
    raw.components[0].chrome = true;
    resolve_surfaces(&[raw], &dir, &ephem_chs, &test_globals());
}

/// A second component sharing a kind but with a distinct explicit instance id
/// resolves cleanly — the multi-instance case the whole cut exists for.
#[test]
fn surface_two_instances_of_one_kind_ok() {
    use brenn_lib::messaging::config::SurfaceComponentRaw;
    let (dir, ephem_chs) = surface_dir_and_ephem();
    let mut raw = valid_surface_raw();
    raw.components.push(SurfaceComponentRaw {
        kind: "protobar".to_string(),
        instance: Some("p2".to_string()),
        abi: "dom".to_string(),
        send_burst: None,
        send_refill_secs: None,
        parked_batch_depth: None,
        config: None,
        chrome: false,
    });
    let resolved = resolve_surfaces(&[raw], &dir, &ephem_chs, &test_globals());
    let kinds: Vec<&str> = resolved[0]
        .components
        .iter()
        .map(|c| c.kind.as_str())
        .collect();
    assert_eq!(kinds, vec!["protobar", "sidecar", "chrome", "protobar"]);
    let instances: Vec<&str> = resolved[0]
        .components
        .iter()
        .map(|c| c.instance.as_str())
        .collect();
    assert_eq!(instances, vec!["protobar", "sidecar", "chrome", "p2"]);
}

/// A component declaring neither budget knob resolves to the defaults — the
/// per-surface constants, now bounding one instance rather than the whole page.
#[test]
fn surface_component_send_budget_defaults_when_undeclared() {
    use brenn_lib::messaging::config::SurfaceSendBudget;
    let (dir, ephem_chs) = surface_dir_and_ephem();
    let resolved = resolve_surfaces(&[valid_surface_raw()], &dir, &ephem_chs, &test_globals());
    for comp in &resolved[0].components {
        assert_eq!(comp.send_budget, SurfaceSendBudget::default());
    }
    assert_eq!(
        SurfaceSendBudget::default().burst,
        brenn_lib::messaging::publish::SURFACE_SEND_BURST,
        "the default burst is the constant it replaces at the finer grain",
    );
    assert_eq!(
        SurfaceSendBudget::default().refill,
        brenn_lib::messaging::publish::SURFACE_SEND_REFILL,
    );
}

/// Each knob overrides independently: a component may retune its burst, its
/// refill, or both, and an unstated knob keeps its default rather than dragging
/// the other one along.
#[test]
fn surface_component_send_budget_knobs_override_independently() {
    use brenn_lib::messaging::config::SurfaceSendBudget;
    let (dir, ephem_chs) = surface_dir_and_ephem();
    let mut raw = valid_surface_raw();
    raw.components[0].send_burst = Some(300);
    raw.components[1].send_refill_secs = Some(90);
    let resolved = resolve_surfaces(&[raw], &dir, &ephem_chs, &test_globals());
    let default = SurfaceSendBudget::default();
    assert_eq!(
        resolved[0].components[0].send_budget,
        SurfaceSendBudget {
            burst: 300,
            refill: default.refill,
        },
    );
    assert_eq!(
        resolved[0].components[1].send_budget,
        SurfaceSendBudget {
            burst: default.burst,
            refill: std::time::Duration::from_secs(90),
        },
    );
}

/// Sibling instances of one kind carry their own budgets: the override is the
/// *instance's*, matching the principal it meters.
#[test]
fn surface_sibling_instances_carry_their_own_send_budgets() {
    let (dir, ephem_chs) = surface_dir_and_ephem();
    let mut raw = valid_surface_raw();
    raw.components
        .push(brenn_lib::messaging::config::SurfaceComponentRaw {
            kind: "protobar".to_string(),
            instance: Some("p2".to_string()),
            abi: "dom".to_string(),
            send_burst: Some(512),
            send_refill_secs: None,
            parked_batch_depth: None,
            config: None,
            chrome: false,
        });
    let resolved = resolve_surfaces(&[raw], &dir, &ephem_chs, &test_globals());
    let budgets: Vec<(&str, u32)> = resolved[0]
        .components
        .iter()
        .map(|c| (c.instance.as_str(), c.send_budget.burst))
        .collect();
    assert_eq!(
        budgets,
        vec![
            (
                "protobar",
                brenn_lib::messaging::publish::SURFACE_SEND_BURST
            ),
            ("sidecar", brenn_lib::messaging::publish::SURFACE_SEND_BURST),
            ("chrome", brenn_lib::messaging::publish::SURFACE_SEND_BURST),
            ("p2", 512),
        ],
    );
}

/// `principal_send_budgets` is what boot installs from: the kernel grain at the
/// defaults, then every declared instance with its own resolved parameters. It
/// must cover exactly `principals()` — a drift leaves a live principal
/// unbudgeted, which the publish gate panics on.
#[test]
fn surface_principal_send_budgets_cover_every_principal() {
    use brenn_lib::messaging::config::SurfaceSendBudget;
    let (dir, ephem_chs) = surface_dir_and_ephem();
    let mut raw = valid_surface_raw();
    raw.components[0].send_burst = Some(400);
    let resolved = resolve_surfaces(&[raw], &dir, &ephem_chs, &test_globals());
    let budgeted: Vec<(Option<String>, SurfaceSendBudget)> =
        resolved[0].principal_send_budgets().collect();
    let principals: Vec<Option<String>> = resolved[0].principals().collect();
    assert_eq!(
        budgeted.iter().map(|(p, _)| p.clone()).collect::<Vec<_>>(),
        principals,
    );
    assert_eq!(budgeted[0].1, SurfaceSendBudget::default(), "kernel grain");
    assert_eq!(budgeted[1].1.burst, 400, "the declared instance's override");
}

#[test]
#[should_panic(expected = "sets send_burst = 0, which admits no publish at all")]
fn surface_component_zero_send_burst_panics() {
    let (dir, ephem_chs) = surface_dir_and_ephem();
    let mut raw = valid_surface_raw();
    raw.components[0].send_burst = Some(0);
    resolve_surfaces(&[raw], &dir, &ephem_chs, &test_globals());
}

/// The sizing invariant: a declared burst below the per-activation cap is a
/// boot panic naming both numbers. This bucket is drawn whole against a flush's
/// entries, so a burst under the cap refuses every maximal conforming flush —
/// a backstop that binds on truthful traffic is mis-sized by definition.
#[test]
#[should_panic(expected = "burst of 255, below the 256-publish per-activation cap")]
fn surface_component_send_burst_below_the_activation_cap_panics() {
    let (dir, ephem_chs) = surface_dir_and_ephem();
    let mut raw = valid_surface_raw();
    raw.components[0].send_burst =
        Some(u32::try_from(brenn_budget::MAX_PUBLISHES_PER_ACTIVATION).unwrap() - 1);
    resolve_surfaces(&[raw], &dir, &ephem_chs, &test_globals());
}

/// The default satisfies the invariant it is asserted against — the value every
/// component gets without declaring anything, and the only value the kernel
/// grain can ever have (it has no override knob). A compile-time assert pins
/// the constant; this pins that *resolution* of an undeclared component yields
/// it and survives the boot assert.
#[test]
fn surface_component_default_send_burst_covers_a_maximal_flush() {
    let (dir, ephem_chs) = surface_dir_and_ephem();
    let resolved = resolve_surfaces(&[valid_surface_raw()], &dir, &ephem_chs, &test_globals());
    assert!(
        resolved[0].components[0].send_budget.burst as usize
            >= brenn_budget::MAX_PUBLISHES_PER_ACTIVATION,
        "the default burst must admit one maximal conforming flush"
    );
}

#[test]
#[should_panic(expected = "sets send_refill_secs = 0, which is a budget that never refills")]
fn surface_component_zero_send_refill_panics() {
    let (dir, ephem_chs) = surface_dir_and_ephem();
    let mut raw = valid_surface_raw();
    raw.components[0].send_refill_secs = Some(0);
    resolve_surfaces(&[raw], &dir, &ephem_chs, &test_globals());
}

#[test]
#[should_panic(expected = "is not declared as a [[surface.component]]")]
fn surface_unknown_component_panics() {
    let (dir, ephem_chs) = surface_dir_and_ephem();
    let mut raw = valid_surface_raw();
    raw.subscriptions[0].instance = "ghost".to_string();
    resolve_surfaces(&[raw], &dir, &ephem_chs, &test_globals());
}

#[test]
#[should_panic(expected = "port name")]
fn surface_bad_port_charset_panics() {
    let (dir, ephem_chs) = surface_dir_and_ephem();
    let mut raw = valid_surface_raw();
    raw.subscriptions[0].port = "bad:port".to_string();
    resolve_surfaces(&[raw], &dir, &ephem_chs, &test_globals());
}

#[test]
#[should_panic(expected = "must be a brenn:, ephemeral:, or local: address")]
fn surface_wrong_scheme_channel_panics() {
    let (dir, ephem_chs) = surface_dir_and_ephem();
    let mut raw = valid_surface_raw();
    raw.subscriptions[0].channel = "mqtt:client:topic".to_string();
    resolve_surfaces(&[raw], &dir, &ephem_chs, &test_globals());
}

#[test]
#[should_panic(expected = "names no declared [[ephemeral_channel]]")]
fn surface_unknown_ephemeral_channel_panics() {
    let (dir, _ephem_chs) = surface_dir_and_ephem();
    // Empty ephemeral set → the ephemeral:protobar-demo subscription has no
    // backing channel.
    resolve_surfaces(&[valid_surface_raw()], &dir, &[], &test_globals());
}

#[test]
#[should_panic(expected = "is not a known brenn: channel")]
fn surface_unknown_brenn_channel_panics() {
    // Directory lacks brenn:alerts (the output's channel).
    let (dir, _addr) = make_brenn_dir("brenn:other");
    let ephem_chs = vec![ephem("protobar-demo")];
    resolve_surfaces(&[valid_surface_raw()], &dir, &ephem_chs, &test_globals());
}

#[test]
#[should_panic(expected = "duplicate subscription binding")]
fn surface_duplicate_subscription_port_panics() {
    let (dir, ephem_chs) = surface_dir_and_ephem();
    let mut raw = valid_surface_raw();
    raw.subscriptions.push(surface_sub_raw(
        "ephemeral:protobar-demo",
        "protobar",
        "messages",
    ));
    resolve_surfaces(&[raw], &dir, &ephem_chs, &test_globals());
}

#[test]
#[should_panic(expected = "does not authorize delivery there")]
fn surface_subscription_not_covered_panics() {
    use brenn_lib::messaging::config::SurfaceGrant;
    let (dir, ephem_chs) = surface_dir_and_ephem();
    let mut raw = valid_surface_raw();
    // Drop EphemeralSubscribe → the ephemeral sub's matcher is present but the
    // grant is gone, so allows_channel_access denies it (dead config).
    raw.grants = vec![SurfaceGrant::Publish];
    resolve_surfaces(&[raw], &dir, &ephem_chs, &test_globals());
}

#[test]
#[should_panic(expected = "does not authorize publishing there")]
fn surface_output_not_covered_panics() {
    use brenn_lib::messaging::config::SurfaceGrant;
    let (dir, ephem_chs) = surface_dir_and_ephem();
    let mut raw = valid_surface_raw();
    // Drop Publish → the brenn output's matcher is present but the grant is
    // gone, so allows_brenn_publish denies it. Coverage is asserted by the
    // post-pass (lifted out of resolve_surfaces so the error-report grant can be
    // injected first), so drive that here.
    raw.grants = vec![SurfaceGrant::EphemeralSubscribe];
    let resolved = resolve_surfaces(&[raw], &dir, &ephem_chs, &test_globals());
    assert_output_bindings_covered(&resolved);
}

/// The design's many-writer shape: an operator binds a `[[surface.output]]` to
/// the configured error channel carrying *no* explicit `publish_acl` for it, and
/// relies on the substrate grant. Coverage must pass once the grant is injected —
/// the exact ordering the post-pass restores (before this fix, resolve_surfaces
/// panicked before the injection ran).
#[test]
fn surface_output_to_error_channel_covered_by_injected_grant() {
    use super::test_fixtures::brenn_entry;
    use brenn_lib::messaging::config::{SurfaceGrant, SurfaceOutputRaw};
    // Directory carries both the surface's own output channel (brenn:alerts) and
    // the error channel it binds.
    let dir = dir_of(vec![
        brenn_entry("brenn:alerts"),
        brenn_entry("brenn:surface-errors"),
    ]);
    let ephem_chs = vec![ephem("protobar-demo")];
    let mut raw = valid_surface_raw();
    // Only the surface's own publish coverage (brenn:alerts) is operator-granted;
    // the error channel has no publish_acl entry — it must be covered solely by
    // the injected substrate grant.
    raw.grants = vec![SurfaceGrant::EphemeralSubscribe, SurfaceGrant::Publish];
    raw.outputs.push(SurfaceOutputRaw {
        instance: "protobar".to_string(),
        port: "errors".to_string(),
        channel: "brenn:surface-errors".to_string(),
        urgency: None,
        publish_per_activation: None,
        publish_capacity: None,
    });
    let mut resolved = resolve_surfaces(&[raw], &dir, &ephem_chs, &test_globals());
    inject_surface_error_grant(&mut resolved, "surface-errors");
    // Must not panic: the injected grant covers the error-channel output.
    assert_output_bindings_covered(&resolved);
}

#[test]
#[should_panic(expected = "declares no [[surface.component]] blocks")]
fn surface_zero_components_panics() {
    let (dir, ephem_chs) = surface_dir_and_ephem();
    let mut raw = valid_surface_raw();
    raw.components = vec![];
    raw.subscriptions = vec![];
    raw.outputs = vec![];
    resolve_surfaces(&[raw], &dir, &ephem_chs, &test_globals());
}

/// The output-side duplicate-`(component, port)` check is a separate code path
/// from the subscription side; pin it independently.
#[test]
#[should_panic(expected = "duplicate output binding")]
fn surface_duplicate_output_port_panics() {
    use brenn_lib::messaging::config::SurfaceOutputRaw;
    let (dir, ephem_chs) = surface_dir_and_ephem();
    let mut raw = valid_surface_raw();
    raw.outputs.push(SurfaceOutputRaw {
        instance: "protobar".to_string(),
        port: "out".to_string(),
        channel: "brenn:alerts".to_string(),
        urgency: None,
        publish_per_activation: None,
        publish_capacity: None,
    });
    resolve_surfaces(&[raw], &dir, &ephem_chs, &test_globals());
}

#[test]
#[should_panic(expected = "kind must be non-empty")]
fn surface_empty_component_kind_panics() {
    let (dir, ephem_chs) = surface_dir_and_ephem();
    let mut raw = valid_surface_raw();
    raw.components[0].kind = String::new();
    resolve_surfaces(&[raw], &dir, &ephem_chs, &test_globals());
}

#[test]
#[should_panic(expected = "component kind")]
fn surface_bad_component_kind_charset_panics() {
    let (dir, ephem_chs) = surface_dir_and_ephem();
    let mut raw = valid_surface_raw();
    raw.components[0].kind = "bad:kind".to_string();
    resolve_surfaces(&[raw], &dir, &ephem_chs, &test_globals());
}

/// Kinds the general unreserved charset (`A-Za-z0-9._~-`) permitted but the
/// tightened `^[a-z0-9][a-z0-9-]*$` rule rejects: uppercase, `_`, `.`, `~`,
/// and a leading `-`. Each must now panic at boot.
#[test]
#[should_panic(expected = "component kind")]
fn surface_uppercase_component_kind_panics() {
    let (dir, ephem_chs) = surface_dir_and_ephem();
    let mut raw = valid_surface_raw();
    raw.components[0].kind = "Protobar".to_string();
    resolve_surfaces(&[raw], &dir, &ephem_chs, &test_globals());
}

#[test]
#[should_panic(expected = "component kind")]
fn surface_underscore_component_kind_panics() {
    let (dir, ephem_chs) = surface_dir_and_ephem();
    let mut raw = valid_surface_raw();
    raw.components[0].kind = "echo_stub".to_string();
    resolve_surfaces(&[raw], &dir, &ephem_chs, &test_globals());
}

#[test]
#[should_panic(expected = "component kind")]
fn surface_leading_hyphen_component_kind_panics() {
    let (dir, ephem_chs) = surface_dir_and_ephem();
    let mut raw = valid_surface_raw();
    raw.components[0].kind = "-protobar".to_string();
    resolve_surfaces(&[raw], &dir, &ephem_chs, &test_globals());
}

/// An `[[ephemeral_channel]]` no surface binding references is allowed
/// (a deliberate staged decision). Pins that no over-eager
/// "every channel must be read" cross-check is ever added.
#[test]
fn surface_unreferenced_ephemeral_channel_allowed() {
    let (dir, _ephem_chs) = surface_dir_and_ephem();
    let ephem_chs = vec![ephem("protobar-demo"), ephem("unused")];
    let resolved = resolve_surfaces(&[valid_surface_raw()], &dir, &ephem_chs, &test_globals());
    assert_eq!(resolved.len(), 1);
    assert_eq!(resolved[0].subscriptions.len(), 1);
}

// --- `local:` bindings ---

/// A surface binding one operator-declared `local:` channel in each direction,
/// plus one reserved control plane. Deliberately grants nothing beyond what the
/// reserved binding needs: page-local traffic never reaches the bus, so no
/// transport grant or ACL can (or should) authorize it.
fn local_surface_raw() -> brenn_lib::messaging::config::SurfaceConfigRaw {
    use brenn_lib::messaging::config::{SurfaceConfigRaw, SurfaceOutputRaw};
    SurfaceConfigRaw {
        subscriptions: vec![surface_sub_raw("local:page-bus", "protobar", "in")],
        outputs: vec![
            SurfaceOutputRaw {
                instance: "protobar".to_string(),
                port: "out".to_string(),
                channel: "local:page-bus".to_string(),
                urgency: None,
                publish_per_activation: None,
                publish_capacity: None,
            },
            SurfaceOutputRaw {
                instance: "protobar".to_string(),
                port: "theme-out".to_string(),
                channel: "local:brenn/theme".to_string(),
                urgency: None,
                publish_per_activation: None,
                publish_capacity: None,
            },
        ],
        ..minimal_surface_raw()
    }
}

/// A `local:` binding resolves with no `[[channel]]` block, no
/// directory entry, and no ACL coverage — the per-surface binding *is* the
/// declaration. The empty directory here is the assertion: nothing about a local
/// channel is looked up server-side.
#[test]
fn local_bindings_resolve_without_a_directory_entry_or_acl() {
    let resolved = resolve_surfaces(
        &[local_surface_raw()],
        &dir_of(vec![]),
        &[],
        &test_globals(),
    );
    let s = &resolved[0];
    assert_eq!(s.subscriptions.len(), 1);
    assert_eq!(s.subscriptions[0].channel_address, "local:page-bus");
    assert_eq!(s.outputs.len(), 2);
    // Both directions of the operator channel collapse to one router channel,
    // and the reserved plane joins it. Order is first-binding order:
    // `local:page-bus` is bound by the subscription first.
    assert_eq!(
        s.local_channels,
        vec![
            ResolvedLocalChannel {
                address: "local:page-bus".to_string(),
                ring_depth: 1,
            },
            ResolvedLocalChannel {
                address: "local:brenn/theme".to_string(),
                ring_depth: 1,
            },
        ]
    );
    // The surface's policy authorizes nothing on the bus, which is the point: a
    // local binding needs no grant because the server carries no local traffic.
    assert!(!s.policy.allows_channel_access("local:page-bus"));
}

/// Ring depth is the **max** over the channel's declared bindings, not the
/// first or the last — a channel read by two ports retains enough for the
/// hungriest of them.
#[test]
fn local_ring_depth_is_the_max_over_bindings() {
    use brenn_lib::messaging::config::{Depth, SurfaceSubscriptionRaw};
    let mut raw = local_surface_raw();
    raw.subscriptions = vec![
        SurfaceSubscriptionRaw {
            retain_depth: Some(Depth::Bounded(3)),
            ..surface_sub_raw("local:page-bus", "protobar", "in")
        },
        SurfaceSubscriptionRaw {
            retain_depth: Some(Depth::Bounded(9)),
            ..surface_sub_raw("local:page-bus", "protobar", "in2")
        },
        SurfaceSubscriptionRaw {
            retain_depth: Some(Depth::Bounded(5)),
            ..surface_sub_raw("local:page-bus", "protobar", "in3")
        },
    ];
    let resolved = resolve_surfaces(&[raw], &dir_of(vec![]), &[], &test_globals());
    assert_eq!(resolved[0].local_channels[0].ring_depth, 9);
}

/// The floor: `retain_depth = 0` resolves to 1, not 0. A depth-0 ring would
/// silently break the late-attach handoff the local class exists for.
#[test]
fn local_ring_depth_floors_at_one() {
    use brenn_lib::messaging::config::{Depth, SurfaceSubscriptionRaw};
    let mut raw = local_surface_raw();
    raw.subscriptions = vec![SurfaceSubscriptionRaw {
        retain_depth: Some(Depth::Bounded(0)),
        ..surface_sub_raw("local:page-bus", "protobar", "in")
    }];
    let resolved = resolve_surfaces(&[raw], &dir_of(vec![]), &[], &test_globals());
    assert_eq!(resolved[0].local_channels[0].ring_depth, 1);
}

/// A local channel bound only as an output still reaches the router: its
/// subscribers may be bound on a *later* config edit, and more importantly the
/// router cannot route a channel it was never told about.
#[test]
fn local_output_only_channel_is_declared_to_the_router() {
    let mut raw = local_surface_raw();
    raw.subscriptions = vec![];
    let resolved = resolve_surfaces(&[raw], &dir_of(vec![]), &[], &test_globals());
    let addrs: Vec<&str> = resolved[0]
        .local_channels
        .iter()
        .map(|c| c.address.as_str())
        .collect();
    assert_eq!(addrs, vec!["local:page-bus", "local:brenn/theme"]);
}

/// The ring lives in page memory. "Unbounded" here is not a retention policy,
/// it is a tab that grows until it dies.
#[test]
#[should_panic(expected = "retained ring lives in page memory")]
fn local_unbounded_retain_depth_panics() {
    use brenn_lib::messaging::config::{Depth, SurfaceSubscriptionRaw};
    let mut raw = local_surface_raw();
    raw.subscriptions = vec![SurfaceSubscriptionRaw {
        retain_depth: Some(Depth::Unbounded),
        ..surface_sub_raw("local:page-bus", "protobar", "in")
    }];
    resolve_surfaces(&[raw], &dir_of(vec![]), &[], &test_globals());
}

/// A local binding's `push_depth` is honoured: the router's ports are queues
/// like any other, and the binding is the only rung a local channel has.
#[test]
fn local_binding_honours_push_depth() {
    use brenn_lib::messaging::config::{Depth, SurfaceSubscriptionRaw};
    let mut raw = local_surface_raw();
    raw.subscriptions = vec![SurfaceSubscriptionRaw {
        push_depth: Some(Depth::Bounded(2)),
        ..surface_sub_raw("local:page-bus", "protobar", "in")
    }];
    let resolved = resolve_surfaces(&[raw], &dir_of(vec![]), &[], &test_globals());
    assert_eq!(resolved[0].subscriptions[0].push_depth, 2);
    // The ring is a separate knob on a separate axis: `push_depth` bounds the
    // port's queue, `retain_depth` the channel's replay ring. Setting one must
    // not move the other.
    assert_eq!(resolved[0].local_channels[0].ring_depth, 1);
}

/// A local binding has no `[[channel]]` rung, so an unset depth inherits the
/// global default directly.
#[test]
fn local_binding_inherits_the_global_push_depth() {
    let resolved = resolve_surfaces(
        &[local_surface_raw()],
        &dir_of(vec![]),
        &[],
        &test_globals(),
    );
    assert_eq!(resolved[0].subscriptions[0].push_depth, 8);
}

/// `wake_min` on a `local:` binding is rejected by the one class-blind
/// rejection every surface binding meets — surfaces are always delivered eagerly.
#[test]
#[should_panic(expected = "always delivered eagerly")]
fn local_wake_min_panics() {
    use brenn_lib::messaging::config::SurfaceSubscriptionRaw;
    let mut raw = local_surface_raw();
    raw.subscriptions = vec![SurfaceSubscriptionRaw {
        wake_min: Some(brenn_lib::messaging::WakeMin::High),
        ..surface_sub_raw("local:page-bus", "protobar", "in")
    }];
    resolve_surfaces(&[raw], &dir_of(vec![]), &[], &test_globals());
}

/// Reserved channels carry contract-fixed depths, so an override is not merely
/// redundant — it is a lie the operator would believe.
#[test]
#[should_panic(expected = "contract-fixed ring depths")]
fn local_retain_depth_on_a_reserved_channel_panics() {
    use brenn_lib::messaging::config::{Depth, SurfaceSubscriptionRaw};
    let mut raw = local_surface_raw();
    raw.subscriptions = vec![SurfaceSubscriptionRaw {
        retain_depth: Some(Depth::Bounded(4)),
        ..surface_sub_raw("local:brenn/theme", "protobar", "theme-in")
    }];
    resolve_surfaces(&[raw], &dir_of(vec![]), &[], &test_globals());
}

/// The contract-fixed depths, pinned end-to-end through resolution: the four
/// control planes replay their last value (what makes a late-attaching chrome
/// gap-free); toast replays nothing (a stale toast is a resurfaced past event).
#[test]
fn reserved_local_channels_carry_their_contract_fixed_ring_depths() {
    use brenn_lib::messaging::config::SurfaceGrant;
    let mut raw = local_surface_raw();
    raw.grants = vec![SurfaceGrant::Takeover];
    raw.outputs = vec![];
    raw.subscriptions = vec![
        surface_sub_raw("local:brenn/theme", "protobar", "p1"),
        surface_sub_raw("local:brenn/takeover", "protobar", "p2"),
        surface_sub_raw("local:brenn/link-state", "protobar", "p3"),
        surface_sub_raw("local:brenn/surface-state", "protobar", "p4"),
        surface_sub_raw("local:brenn/toast", "protobar", "p5"),
    ];
    let resolved = resolve_surfaces(&[raw], &dir_of(vec![]), &[], &test_globals());
    let depths: Vec<(&str, u64)> = resolved[0]
        .local_channels
        .iter()
        .map(|c| (c.address.as_str(), c.ring_depth))
        .collect();
    assert_eq!(
        depths,
        vec![
            ("local:brenn/theme", 1),
            ("local:brenn/takeover", 1),
            ("local:brenn/link-state", 1),
            ("local:brenn/surface-state", 1),
            ("local:brenn/toast", 0),
        ]
    );
}

/// Capability-as-binding: the takeover grant gates the *wiring*, replacing v0's
/// runtime DOM-event gate.
#[test]
#[should_panic(expected = "requires the surface's `takeover` grant")]
fn local_takeover_binding_without_the_grant_panics() {
    let mut raw = local_surface_raw();
    raw.subscriptions = vec![surface_sub_raw("local:brenn/takeover", "protobar", "t")];
    resolve_surfaces(&[raw], &dir_of(vec![]), &[], &test_globals());
}

/// ...and with the grant it resolves. Pins that the check reads the grant
/// rather than rejecting the channel outright.
#[test]
fn local_takeover_binding_with_the_grant_resolves() {
    use brenn_lib::messaging::config::SurfaceGrant;
    let mut raw = local_surface_raw();
    raw.grants = vec![SurfaceGrant::Takeover];
    raw.subscriptions = vec![surface_sub_raw("local:brenn/takeover", "protobar", "t")];
    raw.outputs = vec![];
    let resolved = resolve_surfaces(&[raw], &dir_of(vec![]), &[], &test_globals());
    assert_eq!(
        resolved[0].local_channels[0].address,
        "local:brenn/takeover"
    );
}

/// The kernel owns the state-reporting planes; a component output there would
/// forge them.
#[test]
#[should_panic(expected = "kernel-publish-only")]
fn local_output_to_a_kernel_only_plane_panics() {
    use brenn_lib::messaging::config::SurfaceOutputRaw;
    let mut raw = local_surface_raw();
    raw.subscriptions = vec![];
    raw.outputs = vec![SurfaceOutputRaw {
        instance: "protobar".to_string(),
        port: "out".to_string(),
        channel: "local:brenn/link-state".to_string(),
        urgency: None,
        publish_per_activation: None,
        publish_capacity: None,
    }];
    resolve_surfaces(&[raw], &dir_of(vec![]), &[], &test_globals());
}

/// ...but subscribing one is ordinary: that is how chrome renders the banner.
#[test]
fn local_subscription_to_a_kernel_only_plane_resolves() {
    let mut raw = local_surface_raw();
    raw.subscriptions = vec![surface_sub_raw("local:brenn/link-state", "protobar", "ls")];
    raw.outputs = vec![];
    let resolved = resolve_surfaces(&[raw], &dir_of(vec![]), &[], &test_globals());
    assert_eq!(
        resolved[0].local_channels[0].address,
        "local:brenn/link-state"
    );
}

/// A typo'd control plane must not degrade into an ordinary operator channel
/// that silently routes to nobody. The reserved namespace is closed vocabulary.
#[test]
#[should_panic(expected = "names no control channel the contract defines")]
fn local_undefined_reserved_channel_panics() {
    let mut raw = local_surface_raw();
    raw.subscriptions = vec![surface_sub_raw("local:brenn/nonesuch", "protobar", "in")];
    resolve_surfaces(&[raw], &dir_of(vec![]), &[], &test_globals());
}

/// The reservation rests on the charset: `/` is outside the operator set, so an
/// operator-declared local name can never collide with a reserved one. This
/// pins the enforcement (the boot check), where the messaging-crate test pins
/// the property (`/` is unrepresentable).
#[test]
#[should_panic(expected = "RFC 3986 unreserved characters only")]
fn local_operator_channel_with_a_slash_panics() {
    let mut raw = local_surface_raw();
    raw.subscriptions = vec![surface_sub_raw("local:mine/own", "protobar", "in")];
    resolve_surfaces(&[raw], &dir_of(vec![]), &[], &test_globals());
}

#[test]
#[should_panic(expected = "names an empty local channel")]
fn local_empty_channel_name_panics() {
    let mut raw = local_surface_raw();
    raw.subscriptions = vec![surface_sub_raw("local:", "protobar", "in")];
    resolve_surfaces(&[raw], &dir_of(vec![]), &[], &test_globals());
}

/// A surface whose one component's ABI is `abi`, ready to resolve.
fn surface_with_abi(abi: &str) -> brenn_lib::messaging::config::SurfaceConfigRaw {
    let mut raw = minimal_surface_raw();
    raw.components[0].abi = abi.to_string();
    raw
}

#[test]
fn component_abi_dom_resolves() {
    let resolved = resolve_surfaces(
        &[surface_with_abi("dom")],
        &dir_of(vec![]),
        &[],
        &test_globals(),
    );
    // The declared component carries its resolved ABI to the page: the shell is
    // told what it is loading rather than inferring it from the kind.
    assert_eq!(resolved[0].components[0].abi, brenn_surface_proto::Abi::Dom);
}

#[test]
fn component_abi_processor_resolves() {
    // Headless component-model hosting: the kernel loads a jco-transpiled
    // `brenn:processor` artifact, so the ABI resolves and rides `Welcome` beside
    // `dom`. What the artifact may import is a separate, asset-level check.
    let resolved = resolve_surfaces(
        &[surface_with_abi("processor")],
        &dir_of(vec![]),
        &[],
        &test_globals(),
    );
    assert_eq!(
        resolved[0].components[0].abi,
        brenn_surface_proto::Abi::Processor
    );
}

#[test]
#[should_panic(expected = "reserved but not yet supported")]
fn component_abi_dom_ts_panics() {
    // The reserved-but-unbuilt ABIs are admitted by the contract's name set and
    // refused by the loader, so a config written early keeps its meaning.
    resolve_surfaces(
        &[surface_with_abi("dom-ts")],
        &dir_of(vec![]),
        &[],
        &test_globals(),
    );
}

#[test]
#[should_panic(expected = "reserved but not yet supported")]
fn component_abi_html_panics() {
    resolve_surfaces(
        &[surface_with_abi("html")],
        &dir_of(vec![]),
        &[],
        &test_globals(),
    );
}

#[test]
#[should_panic(expected = "names no component ABI")]
fn component_abi_unknown_panics() {
    // Distinct from the reserved case on purpose: an unknown string is a typo,
    // or config written against something that does not exist, and the operator
    // is told which mistake they made.
    resolve_surfaces(
        &[surface_with_abi("wasm")],
        &dir_of(vec![]),
        &[],
        &test_globals(),
    );
}

#[test]
#[should_panic(expected = "names no component ABI")]
fn component_abi_is_case_sensitive() {
    // The ABI set is a fixed vocabulary, not a string the resolver normalizes:
    // accepting "DOM" would mean the config had two spellings of one value.
    resolve_surfaces(
        &[surface_with_abi("DOM")],
        &dir_of(vec![]),
        &[],
        &test_globals(),
    );
}

// --- depth-0: the bus's rule set, one set for every ABI and class ---
//
// Every surface component rides activations, so depth 0 always has a window to
// be read as context on. What remains are the bus's own two rules: no `noise`
// without a push window, and no port that is neither triggering nor
// context-carrying.

// --- the processor config map ---

/// Turn the fixture's first component into a `processor` carrying `config`, and
/// resolve. Returns the resolved surfaces so a caller can read the map back.
fn resolve_with_component_config(
    config: Option<std::collections::BTreeMap<String, String>>,
    abi: &str,
) -> Vec<brenn_lib::messaging::config::ResolvedSurface> {
    let (dir, ephem_chs) = surface_dir_and_ephem();
    let mut raw = valid_surface_raw();
    raw.components[0].abi = abi.to_string();
    raw.components[0].config = config;
    resolve_surfaces(&[raw], &dir, &ephem_chs, &test_globals())
}

/// A `processor` component's declared map resolves through onto its
/// `ResolvedComponent`, which is what rides `Welcome` as `ComponentEntry.config`.
#[test]
fn processor_component_config_resolves() {
    let map = std::collections::BTreeMap::from([
        ("horizon-days".to_string(), "30".to_string()),
        ("mode".to_string(), "digest".to_string()),
    ]);
    let resolved = resolve_with_component_config(Some(map.clone()), "processor");
    assert_eq!(resolved[0].components[0].config, map);
}

/// No `config` table means the empty map — an operator need not write
/// `config = {}` to say "nothing to configure".
#[test]
fn absent_component_config_resolves_empty() {
    let resolved = resolve_with_component_config(None, "processor");
    assert!(resolved[0].components[0].config.is_empty());
}

/// Only a `processor` is handed a `config` import, so a map on any other ABI has
/// no reader — a dead declaration, which this project treats as a config error
/// rather than a silent no-op.
#[test]
#[should_panic(expected = "would have no reader")]
fn component_config_on_a_dom_component_panics() {
    let map = std::collections::BTreeMap::from([("k".to_string(), "v".to_string())]);
    resolve_with_component_config(Some(map), "dom");
}

/// `brenn.` is the host-reserved key namespace (`processor.wit`); an operator
/// key there is a collision-in-waiting or a typo, never intent.
#[test]
#[should_panic(expected = "host-reserved namespace")]
fn component_config_brenn_prefixed_key_panics() {
    let map = std::collections::BTreeMap::from([("brenn.instance".to_string(), "x".to_string())]);
    resolve_with_component_config(Some(map), "processor");
}

/// A depth-0 port with retained context is the bus's ordinary
/// sampled/context-only port: legal, and the retained context is the point of it.
#[test]
fn a_binding_at_depth_zero_with_context_is_legal() {
    surfaces::assert_page_queue_deliverable(
        "deskbar",
        "durable subscription",
        "brenn:alerts",
        0,
        4,
        None,
    );
}

/// `noise` is a policy over push overflow. A depth-0 port has no push window, so
/// no overflow is expressible and the setting has no referent — the same arm
/// `resolve_wasm_consumers` fires on its own subscriptions.
#[test]
#[should_panic(expected = "no push-overflow events are possible")]
fn noise_on_a_binding_at_depth_zero_panics() {
    surfaces::assert_page_queue_deliverable(
        "deskbar",
        "durable subscription",
        "brenn:alerts",
        0,
        4,
        Some(NoiseLevel::Metered),
    );
}

/// Neither triggering nor context-carrying: a dead port on every ABI and class.
#[test]
#[should_panic(expected = "never carries context (dead config)")]
fn a_binding_with_no_push_and_no_context_panics() {
    surfaces::assert_page_queue_deliverable(
        "deskbar",
        "ephemeral subscription",
        "ephemeral:ticks",
        0,
        0,
        None,
    );
}

/// Every arm below the depth-0 gate is about depth 0 alone: a triggering binding
/// is free to declare noise (it has a window to overflow) and no context at all.
#[test]
fn a_triggering_binding_may_declare_noise_and_no_context() {
    surfaces::assert_page_queue_deliverable(
        "deskbar",
        "durable subscription",
        "brenn:alerts",
        8,
        0,
        Some(NoiseLevel::Metered),
    );
}

/// An instance every one of whose ports is context-only can never activate, so
/// its context windows are never read — dead config at the instance grain, the
/// check `resolve_wasm_consumers` makes at its own principal's.
#[test]
#[should_panic(expected = "input binding(s), all with push_depth = 0")]
fn an_instance_whose_every_binding_is_context_only_panics() {
    surfaces::assert_instance_can_activate("deskbar", "protobar", &[0, 0]);
}

/// One triggering port is enough: the depth-0 siblings window as context on the
/// activations it mints.
#[test]
fn an_instance_with_one_triggering_binding_can_activate() {
    surfaces::assert_instance_can_activate("deskbar", "protobar", &[0, 1, 0]);
}

/// A component with no input bindings is not judged: a purely presentational
/// component is live config, and has been since before activations existed.
#[test]
fn an_instance_with_no_input_bindings_is_not_judged() {
    surfaces::assert_instance_can_activate("deskbar", "protobar", &[]);
}
