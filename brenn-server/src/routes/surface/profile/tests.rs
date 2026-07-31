use brenn_lib::access::AppPolicy;
use brenn_lib::messaging::config::{Depth, ResolvedSurface};
use brenn_surface_schema::LOCAL_THEME_CHANNEL;

use super::*;
use crate::routes::surface::SurfaceRuntime;
use crate::routes::surface::test_fixtures::TEST_MAX_BODY_BYTES;
use crate::test_support::surface::{SurfaceFixture, description_params, shape_only_messenger};

const ERROR_CHANNEL: &str = "brenn:surface-errors";

/// The fixture's per-connection publish bucket. Deliberately not
/// `DEFAULT_SURFACE_PUBLISH_BURST`/`_PER_SEC`, so an assertion about the
/// lowering can tell the operator's numbers from a default substituted for them.
const FIXTURE_PUBLISH_BURST: u32 = 7;
const FIXTURE_PUBLISH_PER_SEC: u32 = 3;

/// A surface with two components: chrome, which binds one durable channel and
/// publishes on two ports (one of them sharing a channel with its other port),
/// and a headless processor that binds the same durable channel at wider depths
/// and publishes nowhere. Enough shape for the fold, the per-attribution sets,
/// and the parked-view dedupe to have something to say.
fn two_component_surface() -> ResolvedSurface {
    SurfaceFixture::new("deskbar", "chrome")
        .publish_rate(FIXTURE_PUBLISH_BURST, FIXTURE_PUBLISH_PER_SEC)
        .processor("mode", "mode-clock", Default::default())
        .subscribe_at_depths("brenn:shared", "chrome", "content", 4, 2)
        .subscribe_at_depths("brenn:shared", "mode", "content", 2, 9)
        .subscribe_at_depths("ephemeral:solo", "chrome", "ticks", 8, 0)
        .subscribe(LOCAL_THEME_CHANNEL, "chrome", "theme")
        .output("brenn:chrome-out", "chrome", "out")
        .output("brenn:chrome-out", "chrome", "alt")
        .output("ephemeral:chrome-eph", "chrome", "eph")
        .output(LOCAL_THEME_CHANNEL, "chrome", "theme-out")
        .build()
}

fn runtime(resolved: ResolvedSurface) -> SurfaceRuntime {
    SurfaceRuntime::build(
        resolved,
        Some(shape_only_messenger()),
        TEST_MAX_BODY_BYTES,
        description_params(),
    )
}

fn profile() -> SurfaceProfile {
    runtime(two_component_surface()).profile
}

/// **One channel, one subscription, both knobs at their widest.** Two instances
/// bind `brenn:shared` at different depths; the attachment sees the channel once,
/// so what arrives must satisfy the hungriest of them — the max of each knob
/// independently, not the pair belonging to whichever instance won.
#[test]
fn per_channel_facts_fold_by_max_across_instances() {
    assert_eq!(
        profile().subscribable("brenn:shared"),
        Some(SubscriptionFacts {
            push_depth: 4,
            retain_depth: 9,
        })
    );
}

/// A channel one instance alone binds folds to that instance's own depths.
#[test]
fn a_single_binding_folds_to_itself() {
    assert_eq!(
        profile().subscribable("ephemeral:solo"),
        Some(SubscriptionFacts {
            push_depth: 8,
            retain_depth: 0,
        })
    );
}

/// **The config channel is subscribable without an operator writing it.** The
/// surface cannot be configured without reading its own bindings document, so the
/// right is substrate, and the document is latest-wins state: one row is the whole
/// window.
#[test]
fn the_config_channel_is_subscribable_at_depth_one() {
    let rt = runtime(two_component_surface());
    assert_eq!(
        rt.profile.subscribable(&rt.description.config_channel),
        Some(SubscriptionFacts {
            push_depth: 1,
            retain_depth: 1,
        })
    );
}

/// A channel this surface binds nowhere is not subscribable — and neither is one
/// it binds page-locally. Page-local traffic never crosses the wire, so its
/// channels are as unbound as a stranger's as far as an attachment is concerned.
#[test]
fn unbound_and_page_local_channels_are_alike_unsubscribable() {
    let profile = profile();
    assert_eq!(profile.subscribable("brenn:somebody-elses"), None);
    assert_eq!(profile.subscribable(LOCAL_THEME_CHANNEL), None);
}

/// **An attribution publishes onto its own channels and nowhere else.** The
/// operator wired chrome's ports; the processor declares no output, and a
/// declared attribution with no outputs may publish nothing at all.
#[test]
fn each_attribution_publishes_only_its_own_channels() {
    let profile = profile();
    assert!(profile.publishable(Some("chrome"), "brenn:chrome-out"));
    assert!(profile.publishable(Some("chrome"), "ephemeral:chrome-eph"));
    assert!(!profile.publishable(Some("mode"), "brenn:chrome-out"));
    assert!(!profile.publishable(Some("chrome"), "brenn:somebody-elses"));
    // A page-local output is the page router's business and never reaches the
    // bus, so it is not publishable over the wire either.
    assert!(!profile.publishable(Some("chrome"), LOCAL_THEME_CHANNEL));
}

/// An undeclared attribution has no set of its own, so it publishes nothing —
/// including onto a channel some declared sibling may write.
#[test]
fn an_undeclared_attribution_publishes_nothing() {
    assert!(!profile().publishable(Some("ghost"), "brenn:chrome-out"));
}

/// **The telemetry pair belongs to the bare identity alone.** Boot's single-writer
/// sweep keeps every other principal off these channels; this set is what keeps
/// the surface's own components off them, so the documents have exactly one
/// author at runtime as well as at boot.
#[test]
fn the_telemetry_pair_is_the_bare_identitys_alone() {
    let rt = runtime(two_component_surface());
    for channel in [
        &rt.description.geometry_channel,
        &rt.description.status_channel,
    ] {
        assert!(rt.profile.publishable(None, channel));
        assert!(!rt.profile.publishable(Some("chrome"), channel));
        assert!(!rt.profile.publishable(Some("mode"), channel));
    }
}

/// The bare identity's set is the platform's, not a superset of every
/// component's: the kernel publishes telemetry and its own reports, not a
/// component's output.
#[test]
fn the_bare_identity_does_not_inherit_component_outputs() {
    assert!(!profile().publishable(None, "brenn:chrome-out"));
}

/// **The error channel widens every set.** It is many-writer by design — a
/// component's report carries that component's sender and the kernel's carries
/// the bare identity — so binding it admits every declared attribution and the
/// attacher itself.
#[test]
fn the_error_channel_admits_every_attribution() {
    let mut profile = profile();
    assert!(!profile.publishable(None, ERROR_CHANNEL));
    profile.bind_error_channel(ERROR_CHANNEL);
    assert!(profile.publishable(None, ERROR_CHANNEL));
    assert!(profile.publishable(Some("chrome"), ERROR_CHANNEL));
    assert!(profile.publishable(Some("mode"), ERROR_CHANNEL));
    // Still not a way in for an attribution the operator never wrote.
    assert!(!profile.publishable(Some("ghost"), ERROR_CHANNEL));
}

/// **The error channel is the one channel a publish may fail on without killing
/// the process.** Everywhere else boot proved reachability and coverage, so a
/// refusal is a broken server; on the shell's own diagnostics path a refusal must
/// survive as a log line instead. A surface with no error channel configured has
/// no diagnostics posture at all.
#[test]
fn only_the_error_channel_carries_the_diagnostics_posture() {
    let mut profile = profile();
    assert_eq!(
        profile.publish_posture(ERROR_CHANNEL),
        PublishPosture::Invariant,
        "unbound, the error address is just another channel"
    );

    profile.bind_error_channel(ERROR_CHANNEL);
    assert_eq!(
        profile.publish_posture(ERROR_CHANNEL),
        PublishPosture::Diagnostic
    );
    assert_eq!(
        profile.publish_posture("brenn:chrome-out"),
        PublishPosture::Invariant
    );
    assert_eq!(
        profile.publish_posture("ephemeral:chrome-eph"),
        PublishPosture::Invariant
    );
}

/// **Minting is admission, not spelling.** The absent attribution is the bare
/// identity, a declared one is its own sub-identity, and anything else mints
/// nothing — the caller's cue that the frame is a violation rather than a
/// publish under a fallback identity.
#[test]
fn attribution_admission_mints_only_declared_identities() {
    let profile = profile();
    assert_eq!(
        profile
            .admit_attribution(None)
            .map(|p| p.as_str().to_string()),
        Some("surface:deskbar".to_string())
    );
    assert_eq!(
        profile
            .admit_attribution(Some("mode"))
            .map(|p| p.as_str().to_string()),
        Some("surface:deskbar#mode".to_string())
    );
    assert_eq!(profile.admit_attribution(Some("ghost")), None);
}

/// An attribution carrying the characters the identity grammar reserves is
/// refused by the declared-set check, which runs before any minting — the
/// minting guards panic on such a string, and this one came off the wire.
#[test]
fn a_malformed_attribution_is_refused_before_minting() {
    assert_eq!(profile().admit_attribution(Some("surface:x#y")), None);
    assert_eq!(profile().admit_attribution(Some("")), None);
}

/// The budget scope is the surface slug; the attribution half of a bucket key is
/// the caller's, so a sub-identity's retry loop drains only its own bucket.
#[test]
fn the_send_budget_scope_is_the_slug() {
    assert_eq!(profile().send_budget_scope(), "deskbar");
}

/// The route's capacity policy reaches the registry through the profile, at the
/// compiled-in surface caps. Asserted against the constants rather than the
/// numbers so the test pins the wiring, not a value the operator may retune.
#[test]
fn the_session_caps_are_the_surface_caps() {
    let caps = profile().session_caps();
    assert_eq!(caps.per_attacher, MAX_SESSIONS_PER_SURFACE);
    assert_eq!(caps.per_account, MAX_SESSIONS_PER_USER_PER_SURFACE);
}

/// **The per-connection publish bucket is the operator's own numbers.** The
/// fixture tunes both knobs away from the config defaults, so a lowering that
/// substituted a default — leaving an operator who tightened the bucket with the
/// stock one on every attachment — fails here.
#[test]
fn the_publish_rate_is_the_operators_own_numbers_not_a_default() {
    use brenn_lib::messaging::config::{
        DEFAULT_SURFACE_PUBLISH_BURST, DEFAULT_SURFACE_PUBLISH_PER_SEC,
    };
    assert_ne!(FIXTURE_PUBLISH_BURST, DEFAULT_SURFACE_PUBLISH_BURST);
    assert_ne!(FIXTURE_PUBLISH_PER_SEC, DEFAULT_SURFACE_PUBLISH_PER_SEC);
    assert_eq!(
        profile().publish_rate(),
        PublishRate {
            burst: FIXTURE_PUBLISH_BURST,
            per_sec: FIXTURE_PUBLISH_PER_SEC,
        }
    );
}

/// A surface whose operator-written policy grants the alert plane.
fn alert_granted_surface() -> ResolvedSurface {
    let mut policy = AppPolicy::default();
    policy.grants.insert(AppCapability::SurfaceAlert);
    SurfaceFixture::new("deskbar", "chrome")
        .policy(policy)
        .build()
}

/// **The alert grant is lowered, both ways.** An ungranted surface answers
/// `false` and a granted one `true`: a lowering stuck at `false` leaves a surface
/// the operator granted paging rights unable to page *and* — since the attachment
/// treats an ungranted `Alert` as a violation — killed and fail2ban-flagged every
/// time its shell tries.
#[test]
fn the_alert_grant_is_lowered_from_the_resolved_policy() {
    assert!(
        !profile().alert_granted(),
        "the default policy grants no alert"
    );
    let granted = runtime(alert_granted_surface());
    assert!(granted.profile.alert_granted());
    assert_agrees_with_port_maps(&granted);
}

/// The burst must cover a boot-valid maximum-size surface's first-connect
/// reconcile — one `Subscribe` per bound channel in one burst — or a legitimate
/// connect becomes a violation. Asserted against the binding maximum, so the
/// property survives a retune of either number.
#[test]
fn the_subscribe_burst_covers_a_maximum_size_reconcile() {
    let burst = profile().subscribe_burst();
    let max_bindings = brenn_surface_schema::MAX_SURFACE_SUBSCRIPTION_BINDINGS as u32;
    assert!(
        burst >= max_bindings,
        "a burst of {burst} refuses a {max_bindings}-binding surface's reconcile"
    );
    // Plus one full detach/re-attach cycle of that surface.
    assert_eq!(burst, 3 * max_bindings);
}

/// **Two ports on one channel share one parked set.** The mirror is cut at
/// `(attribution, channel)`, so chrome's two ports onto `brenn:chrome-out` seed
/// once; page-local outputs, which the backend parks nothing on, are absent
/// entirely.
#[test]
fn parked_view_targets_dedupe_shared_channels_and_exclude_local() {
    assert_eq!(
        profile().deferred_view_targets(),
        &[
            DeferredTarget {
                channel: "brenn:chrome-out".to_string(),
                attribution: Some("chrome".to_string()),
            },
            DeferredTarget {
                channel: "ephemeral:chrome-eph".to_string(),
                attribution: Some("chrome".to_string()),
            },
        ]
    );
}

/// Lower `resolved` against a well-formed surface's description runtime — the
/// build path on its own, for the guards that fire before a `SurfaceRuntime`
/// exists. The runtime path reaches its own copy of the depth guard first, so a
/// test about *this* one must call the builder directly.
fn build_profile(resolved: &ResolvedSurface) -> SurfaceProfile {
    let description = runtime(two_component_surface()).description;
    SurfaceProfile::build(resolved, &description)
}

/// **An unbounded wire depth is boot's bug, not a runtime shrug.** The number
/// becomes a replay clamp; accepting `Unbounded` here would serve a subscription
/// no config can ask for.
#[test]
#[should_panic(expected = "brenn:shared resolves an unbounded push_depth")]
fn an_unbounded_wire_push_depth_is_a_boot_panic() {
    let mut resolved = two_component_surface();
    resolved.wire_subscriptions[0].subscription.push_depth = Depth::Unbounded;
    build_profile(&resolved);
}

/// The retain twin, which also proves the guard names the knob it read rather
/// than interpolating whichever one the caller passed first.
#[test]
#[should_panic(expected = "brenn:shared resolves an unbounded retain_depth")]
fn an_unbounded_wire_retain_depth_is_a_boot_panic() {
    let mut resolved = two_component_surface();
    resolved.wire_subscriptions[0].subscription.retain_depth = Depth::Unbounded;
    build_profile(&resolved);
}

/// An output naming an instance the component set does not carry has no
/// attribution to be published under, so the lowering refuses it rather than
/// silently minting a key for it.
#[test]
#[should_panic(expected = "absent from the resolved component set")]
fn an_output_naming_an_undeclared_instance_is_a_boot_panic() {
    let resolved = SurfaceFixture::new("deskbar", "chrome")
        .output("brenn:ghost-out", "ghost", "out")
        .build();
    build_profile(&resolved);
}

/// The two lowerings of one surface's authority agree on the config every other
/// test here is written against, error channel bound and all.
#[test]
fn the_cross_check_passes_on_a_resolved_surface() {
    let mut rt = runtime(two_component_surface());
    rt.profile.bind_error_channel(ERROR_CHANNEL);
    rt.output_ports.insert(
        (
            brenn_surface_contract::ERROR_REPORT_INSTANCE.to_string(),
            brenn_surface_contract::ERROR_REPORT_PORT.to_string(),
        ),
        crate::routes::surface::OutputPort {
            address: ERROR_CHANNEL.to_string(),
            default_urgency: brenn_lib::messaging::Urgency::Normal,
        },
    );
    assert_agrees_with_port_maps(&rt);
}

/// A bound output the profile knows nothing about is exactly the drift the
/// cross-check exists to kill: the session would dispatch the publish and the
/// attachment-grain authority would refuse it.
#[test]
#[should_panic(expected = "is not publishable by its own attribution")]
fn the_cross_check_catches_an_output_the_profile_lacks() {
    let mut rt = runtime(two_component_surface());
    rt.output_ports.insert(
        ("chrome".to_string(), "smuggled".to_string()),
        crate::routes::surface::OutputPort {
            address: "brenn:smuggled".to_string(),
            default_urgency: brenn_lib::messaging::Urgency::Normal,
        },
    );
    assert_agrees_with_port_maps(&rt);
}

/// The subscribe direction of the same drift: a per-instance subscription the
/// per-channel fold does not cover would be a channel the session admits and the
/// attachment cannot see.
#[test]
#[should_panic(expected = "cover different channels")]
fn the_cross_check_catches_a_subscription_the_profile_lacks() {
    let mut rt = runtime(two_component_surface());
    rt.subscription_channels.insert(
        crate::routes::surface::SubKey {
            instance: "chrome".to_string(),
            channel: "brenn:smuggled".to_string(),
        },
        SubscriptionFacts {
            push_depth: 1,
            retain_depth: 1,
        },
    );
    assert_agrees_with_port_maps(&rt);
}
