//! Unit tests for `resolve_wasm_consumers`: slug dedup/reservation, ACL
//! passthrough, activation pacing, noise/pull-only guards, dead-port/consumer/
//! sink checks, port-name validation, grant cross-validation, store validation,
//! config passthrough, and publish-budget knob resolution.

use super::test_fixtures::{
    brenn_entry, brenn_entry_with, dir_of, make_brenn_dir, minimal_wasm_consumer,
    minimal_wasm_consumer_raw, no_auto, out_raw, resolve, sub_raw,
};
use super::wasm::{DEFAULT_ACTIVATION_BURST, DEFAULT_ACTIVATION_MIN_PERIOD};
use super::*;
use brenn_lib::messaging::ComponentGrant;
use brenn_lib::messaging::config::{
    Depth, WasmConsumerConfigRaw, WasmConsumerOutputRaw, WasmConsumerSubscriptionRaw,
    WasmSinkBudget,
};
use std::time::Duration;

/// Single-channel directory with the given resolved noise.
fn make_dir_with_noise(chan_addr: &str, noise: NoiseLevel) -> (MessagingDirectory, String) {
    (
        dir_of(vec![brenn_entry_with(
            chan_addr,
            Depth::Unbounded,
            Depth::Unbounded,
            noise,
        )]),
        chan_addr.to_string(),
    )
}

/// Single-channel directory with the given noise and channel push_depth.
fn make_dir_with_noise_and_push_depth(
    chan_addr: &str,
    noise: NoiseLevel,
    push_depth: Depth,
) -> (MessagingDirectory, String) {
    (
        dir_of(vec![brenn_entry_with(
            chan_addr,
            push_depth,
            Depth::Unbounded,
            noise,
        )]),
        chan_addr.to_string(),
    )
}

// --- Identity collision ---

/// Duplicate `wasm:` slug across two `[[wasm_consumer]]` blocks panics at
/// bootstrap.
#[test]
#[should_panic(expected = "duplicate [[wasm_consumer]] slug")]
fn duplicate_wasm_slug_panics_at_bootstrap() {
    let (dir, chan_addr) = make_brenn_dir("brenn:dedup-test");
    let raw = vec![
        minimal_wasm_consumer_raw("duplicate", "/tmp/a.wasm", &chan_addr),
        minimal_wasm_consumer_raw("duplicate", "/tmp/b.wasm", &chan_addr),
    ];
    resolve(&raw, &dir);
}

/// The class's spec hash reaches the resolved consumer intact.
///
/// This is the only hop carrying it from the lowered config to the value
/// `run_server` hands the package verifier, and every other fixture in the tree
/// leaves the field empty — so without this the clone is only ever exercised
/// over the empty string. A hash that arrived wrong would panic on the deploy
/// target, for every consumer at once.
#[test]
fn a_consumers_spec_hash_reaches_the_resolved_consumer() {
    let (dir, chan_addr) = make_brenn_dir("brenn:spec-hash-passthrough");
    let mut raw = minimal_wasm_consumer_raw("hashed", "/tmp/a.wasm", &chan_addr);
    let hash = "a".repeat(64);
    raw.spec_sha256 = hash.clone();

    let resolved = resolve(&[raw], &dir);
    assert_eq!(resolved[0].spec_sha256, hash);
}

/// Non-empty `subscribe_acl`/`publish_acl` pass through `resolve_wasm_consumers`
/// without panicking and the consumer is resolved. The ACL entries are resolved
/// into the policy by `build_wasm_policy`; this test pins that the resolver
/// *accepts* non-empty ACL input rather than only ever seeing empty lists, so it
/// serves as a smoke-check that resolution with non-empty ACL matchers succeeds
/// without panicking. (The policy is carried but not yet enforced.)
#[test]
fn wasm_consumer_resolves_with_non_empty_acl() {
    use brenn_lib::access::raw::ChannelMatcherRaw;
    let (dir, chan_addr) = make_brenn_dir("brenn:acl-passthrough");
    let mut raw = minimal_wasm_consumer_raw("acl-consumer", "/tmp/a.wasm", &chan_addr);
    raw.subscribe_acl = vec![ChannelMatcherRaw::Exact("inbox".to_string())];
    raw.publish_acl = vec![ChannelMatcherRaw::Prefix("events.".to_string())];
    // A non-empty publish_acl requires the `ports` grant — without it the
    // matchers are dead (MessagingPublish absent) and resolution panics.
    raw.grants = vec![ComponentGrant::Ports];

    let resolved = resolve(&[raw], &dir);
    assert_eq!(
        resolved.len(),
        1,
        "consumer with non-empty ACL must resolve"
    );
    // Pin the policy content at the integration layer so a regression that
    // swaps the subscribe_acl/publish_acl arguments to build_wasm_policy, or
    // discards the policy before assigning it to ResolvedWasmConsumer.policy,
    // is caught here and not only in the brenn-lib unit tests.
    use brenn_lib::access::acl::ChannelMatcher;
    let c = &resolved[0];
    assert_eq!(
        c.policy.acls.brenn_subscribe,
        vec![ChannelMatcher::Exact("inbox".to_string())]
    );
    assert_eq!(
        c.policy.acls.brenn_publish,
        vec![ChannelMatcher::Prefix("events.".to_string())]
    );
    assert!(c.policy.acls.mqtt_subscribe.is_empty());
    assert!(c.policy.acls.mqtt_publish.is_empty());
    assert!(c.policy.acls.webhook.is_empty());
}

/// A WASM consumer subscribing to an `ephemeral:` channel resolves the input
/// when `ephemeral_subscribe_acl` covers it.
#[test]
fn wasm_consumer_resolves_ephemeral_input_with_covering_acl() {
    use brenn_lib::access::raw::ChannelMatcherRaw;
    let entry = brenn_messaging::testutils::ephemeral_channel_entry("sensors", 8);
    let addr = entry.address.clone();
    let dir = dir_of(vec![entry]);
    let mut raw = minimal_wasm_consumer_raw("watcher", "/tmp/a.wasm", &addr);
    raw.ephemeral_subscribe_acl = vec![ChannelMatcherRaw::Exact("sensors".to_string())];

    let resolved = resolve(&[raw], &dir);
    assert_eq!(resolved.len(), 1);
    assert_eq!(resolved[0].inputs.len(), 1);
    assert_eq!(resolved[0].inputs[0].sub.channel_address, addr);
    assert!(resolved[0].policy.allows_ephemeral_delivery("sensors"));
}

/// An `ephemeral:` input with no covering `ephemeral_subscribe_acl` is
/// silently-dead config — ring delivery is authorized only at boot — so it must
/// fail fast rather than boot into a subscription that could never receive.
#[test]
#[should_panic(expected = "not covered by ephemeral_subscribe_acl")]
fn wasm_consumer_ephemeral_input_without_acl_panics() {
    let entry = brenn_messaging::testutils::ephemeral_channel_entry("sensors", 8);
    let addr = entry.address.clone();
    let dir = dir_of(vec![entry]);
    let raw = minimal_wasm_consumer_raw("watcher", "/tmp/a.wasm", &addr);
    resolve(&[raw], &dir);
}

/// A WASM consumer subscribing to a confined `local:` channel resolves the input
/// when `local_subscribe_acl` covers it — the mirror of the ephemeral case.
#[test]
fn wasm_consumer_resolves_local_input_with_covering_acl() {
    use brenn_lib::access::raw::ChannelMatcherRaw;
    let entry = brenn_messaging::testutils::local_channel_entry("scratch", 8);
    let addr = entry.address.clone();
    let dir = dir_of(vec![entry]);
    let mut raw = minimal_wasm_consumer_raw("watcher", "/tmp/a.wasm", &addr);
    raw.local_subscribe_acl = vec![ChannelMatcherRaw::Exact("scratch".to_string())];

    let resolved = resolve(&[raw], &dir);
    assert_eq!(resolved.len(), 1);
    assert_eq!(resolved[0].inputs.len(), 1);
    assert_eq!(resolved[0].inputs[0].sub.channel_address, addr);
    assert!(resolved[0].policy.allows_local_delivery("scratch"));
}

/// A `local:` input with no covering `local_subscribe_acl` is silently-dead
/// config — ring delivery is authorized only at boot — so it must fail fast.
#[test]
#[should_panic(expected = "not covered by local_subscribe_acl")]
fn wasm_consumer_local_input_without_acl_panics() {
    let entry = brenn_messaging::testutils::local_channel_entry("scratch", 8);
    let addr = entry.address.clone();
    let dir = dir_of(vec![entry]);
    let raw = minimal_wasm_consumer_raw("watcher", "/tmp/a.wasm", &addr);
    resolve(&[raw], &dir);
}

/// A `[[wasm_consumer.tool_grant]]` table resolves into the consumer's
/// `AppPolicy.tool_grants`, the same map an LLM app carries.
#[test]
fn wasm_consumer_resolves_tool_grants_into_policy() {
    use brenn_lib::tools::config::ToolGrantRaw;
    let (dir, chan_addr) = make_brenn_dir("brenn:tool-grant");
    let mut raw = minimal_wasm_consumer_raw("puller", "/tmp/a.wasm", &chan_addr);
    raw.tool_grants = vec![ToolGrantRaw {
        tool: "git-repo-pull".to_string(),
        acl: vec![
            [("repo".to_string(), "brenn".to_string())]
                .into_iter()
                .collect(),
        ],
        rate_limit: None,
    }];

    let resolved = resolve(&[raw], &dir);
    let grant = resolved[0]
        .policy
        .tool_grants
        .get("git-repo-pull")
        .expect("tool grant resolved into policy");
    let allowed: std::collections::BTreeMap<String, String> =
        [("repo".to_string(), "brenn".to_string())]
            .into_iter()
            .collect();
    assert!(grant.acl_allows(&allowed));
    let denied: std::collections::BTreeMap<String, String> =
        [("repo".to_string(), "pfin".to_string())]
            .into_iter()
            .collect();
    assert!(!grant.acl_allows(&denied));
}

/// A consumer with no `tool_grant` table has an empty `tool_grants` map (so no
/// `Tools` capability is derived at load).
#[test]
fn wasm_consumer_without_tool_grants_has_empty_map() {
    let (dir, chan_addr) = make_brenn_dir("brenn:no-tool-grant");
    let raw = minimal_wasm_consumer_raw("plain", "/tmp/a.wasm", &chan_addr);
    let resolved = resolve(&[raw], &dir);
    assert!(resolved[0].policy.tool_grants.is_empty());
}

// --- Activation pacing ---

/// Absent activation-pacing fields resolve to the hardcoded defaults: burst 60,
/// min_period 1 s. There is no `[wasm]` global fallback for these — the
/// per-consumer knob (or the default) is the whole surface.
#[test]
fn activation_pacing_absent_resolves_to_defaults() {
    let (dir, chan_addr) = make_brenn_dir("brenn:pacing-default");
    let raw = minimal_wasm_consumer_raw("pacing-default", "/tmp/a.wasm", &chan_addr);
    assert!(raw.activation_burst.is_none());
    assert!(raw.activation_min_period_ms.is_none());
    let resolved = resolve(&[raw], &dir);
    let pacing = resolved[0].activation_pacing;
    assert_eq!(pacing.burst, DEFAULT_ACTIVATION_BURST);
    assert_eq!(pacing.min_period, DEFAULT_ACTIVATION_MIN_PERIOD);
}

/// Present activation-pacing fields override the defaults and are carried
/// through onto `ResolvedWasmConsumer.activation_pacing`.
#[test]
fn activation_pacing_present_overrides_defaults() {
    let (dir, chan_addr) = make_brenn_dir("brenn:pacing-override");
    let mut raw = minimal_wasm_consumer_raw("pacing-override", "/tmp/a.wasm", &chan_addr);
    raw.activation_burst = Some(5);
    raw.activation_min_period_ms = Some(250);
    let resolved = resolve(&[raw], &dir);
    let pacing = resolved[0].activation_pacing;
    assert_eq!(pacing.burst, 5);
    assert_eq!(pacing.min_period, Duration::from_millis(250));
}

/// `activation_burst = 0` is a fail-fast bootstrap panic naming the slug — a
/// zero-capacity bucket can never admit an activation.
#[test]
#[should_panic(expected = "activation_burst must be >= 1")]
fn activation_burst_zero_panics() {
    let (dir, chan_addr) = make_brenn_dir("brenn:pacing-zero-burst");
    let mut raw = minimal_wasm_consumer_raw("pacing-zero-burst", "/tmp/a.wasm", &chan_addr);
    raw.activation_burst = Some(0);
    resolve(&[raw], &dir);
}

/// `activation_min_period_ms = 0` is a fail-fast bootstrap panic naming the
/// slug — rejected at the config layer rather than deferred to
/// `TokenBucket::new`'s zero-interval panic.
#[test]
#[should_panic(expected = "activation_min_period_ms must be >= 1")]
fn activation_min_period_zero_panics() {
    let (dir, chan_addr) = make_brenn_dir("brenn:pacing-zero-period");
    let mut raw = minimal_wasm_consumer_raw("pacing-zero-period", "/tmp/a.wasm", &chan_addr);
    raw.activation_min_period_ms = Some(0);
    resolve(&[raw], &dir);
}

// --- Noise inheritance and pull-only guard ---

/// A WASM subscriber with no per-sub noise override inherits the channel noise,
/// not the global default.
#[test]
fn wasm_consumer_inherits_channel_noise() {
    let (dir, chan_addr) = make_dir_with_noise("brenn:inherit-test", NoiseLevel::Alarm);
    let raw = vec![WasmConsumerConfigRaw {
        slug: "consumer-a".to_string(),
        // Deliberately unlike the slug: the package name is the only thing
        // that tells boot which installed component to resolve, and a
        // resolution that substituted the slug would look plausible.
        package: "processor-demo".to_string(),
        subscriptions: vec![sub_raw(&chan_addr, "in")],
        ..minimal_wasm_consumer()
    }];
    let result = resolve(&raw, &dir);
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].package, "processor-demo");
    assert_eq!(result[0].inputs.len(), 1);
    assert_eq!(result[0].inputs[0].sub.noise, NoiseLevel::Alarm);
}

/// Explicit per-sub noise takes precedence over channel noise.
#[test]
fn wasm_consumer_explicit_noise_overrides_channel() {
    let (dir, chan_addr) = make_dir_with_noise("brenn:override-test", NoiseLevel::Alarm);
    let raw = vec![WasmConsumerConfigRaw {
        slug: "consumer-b".to_string(),
        package: "b".to_string(),
        subscriptions: vec![WasmConsumerSubscriptionRaw {
            push_depth: Some(Depth::Bounded(5)),
            noise: Some(NoiseLevel::Metered),
            ..sub_raw(&chan_addr, "in")
        }],
        ..minimal_wasm_consumer()
    }];
    let result = resolve(&raw, &dir);
    assert_eq!(result[0].inputs[0].sub.noise, NoiseLevel::Metered);
}

/// Explicit noise + resolved push_depth = 0 → panic at bootstrap.
#[test]
#[should_panic(expected = "push_depth = 0 (pull-only)")]
fn wasm_consumer_explicit_noise_on_pull_only_panics() {
    let (dir, chan_addr) = make_dir_with_noise("brenn:pullonly-panic-test", NoiseLevel::Silent);
    let raw = vec![WasmConsumerConfigRaw {
        slug: "consumer-c".to_string(),
        package: "c".to_string(),
        subscriptions: vec![WasmConsumerSubscriptionRaw {
            push_depth: Some(Depth::Bounded(0)),
            noise: Some(NoiseLevel::Alarm),
            ..sub_raw(&chan_addr, "in")
        }],
        ..minimal_wasm_consumer()
    }];
    resolve(&raw, &dir);
}

/// Explicit `fatal` noise on a WASM subscription is rejected at boot — fatal is
/// the surface-only kill rung and the backend overflow path has no kill.
#[test]
#[should_panic(expected = "fatal is surface-only")]
fn wasm_consumer_explicit_fatal_noise_panics() {
    let (dir, chan_addr) = make_dir_with_noise("brenn:fatal-explicit", NoiseLevel::Silent);
    let raw = vec![WasmConsumerConfigRaw {
        slug: "consumer-fatal-x".to_string(),
        package: "fx".to_string(),
        subscriptions: vec![WasmConsumerSubscriptionRaw {
            push_depth: Some(Depth::Bounded(4)),
            noise: Some(NoiseLevel::Fatal),
            ..sub_raw(&chan_addr, "in")
        }],
        ..minimal_wasm_consumer()
    }];
    resolve(&raw, &dir);
}

/// `fatal` inherited from the channel (no per-sub override) is rejected at boot
/// too — the resolved noise is what matters, not where it came from.
#[test]
#[should_panic(expected = "fatal is surface-only")]
fn wasm_consumer_inherited_fatal_noise_panics() {
    let (dir, chan_addr) = make_dir_with_noise("brenn:fatal-inherited", NoiseLevel::Fatal);
    let raw = vec![WasmConsumerConfigRaw {
        slug: "consumer-fatal-i".to_string(),
        package: "fi".to_string(),
        subscriptions: vec![sub_raw(&chan_addr, "in")],
        ..minimal_wasm_consumer()
    }];
    resolve(&raw, &dir);
}

/// Any explicit `wake_min` on a WASM subscription is a config error, even on a
/// push-enabled sub — WASM consumers are always delivered eagerly, so the knob
/// does nothing; the error points at `push_depth = 0` as the honest pull-only
/// alternative.
#[test]
#[should_panic(expected = "always delivered eagerly")]
fn wasm_consumer_explicit_wake_min_panics() {
    let (dir, chan_addr) = make_dir_with_noise("brenn:wakemin-panic-test", NoiseLevel::Silent);
    let raw = vec![WasmConsumerConfigRaw {
        slug: "consumer-wm".to_string(),
        package: "wm".to_string(),
        subscriptions: vec![WasmConsumerSubscriptionRaw {
            push_depth: Some(Depth::Bounded(5)),
            wake_min: Some(brenn_lib::messaging::WakeMin::High),
            ..sub_raw(&chan_addr, "in")
        }],
        ..minimal_wasm_consumer()
    }];
    resolve(&raw, &dir);
}

/// A consumer whose single subscription has push_depth=0 (sampled/context-only)
/// can never activate → panic.
///
/// Replaces the old `wasm_consumer_inherited_noise_on_pull_only_ok` test: that
/// test asserted no-panic on a pull-only sub, but the multi-port design
/// explicitly makes all-sampled-only consumers dead config.
#[test]
#[should_panic(expected = "can never activate")]
fn wasm_consumer_all_sampled_inputs_panics() {
    let (dir, chan_addr) = make_dir_with_noise("brenn:pullonly-dead-test", NoiseLevel::Alarm);
    let raw = vec![WasmConsumerConfigRaw {
        slug: "consumer-d".to_string(),
        package: "d".to_string(),
        subscriptions: vec![WasmConsumerSubscriptionRaw {
            push_depth: Some(Depth::Bounded(0)),
            // retain_depth inherits channel's Unbounded — sampled port, not dead-port.
            ..sub_raw(&chan_addr, "in")
        }],
        ..minimal_wasm_consumer()
    }];
    // Panics: the single input has push_depth=0 → consumer can never activate.
    resolve(&raw, &dir);
}

/// push_depth=0 AND retain_depth=0 → dead port, can never trigger and never
/// contributes context.
#[test]
#[should_panic(expected = "can never trigger")]
fn wasm_consumer_dead_port_both_depths_zero_panics() {
    let (dir, chan_addr) = make_dir_with_noise("brenn:dead-port-test", NoiseLevel::Alarm);
    let raw = vec![WasmConsumerConfigRaw {
        slug: "consumer-dead-port".to_string(),
        package: "dp".to_string(),
        subscriptions: vec![WasmConsumerSubscriptionRaw {
            push_depth: Some(Depth::Bounded(0)),
            retain_depth: Some(Depth::Bounded(0)),
            ..sub_raw(&chan_addr, "in")
        }],
        ..minimal_wasm_consumer()
    }];
    resolve(&raw, &dir);
}

/// Explicit noise + channel-inherited push_depth = 0 → panic.
#[test]
#[should_panic(expected = "push_depth = 0 (pull-only)")]
fn wasm_consumer_explicit_noise_on_channel_pull_only_panics() {
    let (dir, chan_addr) = make_dir_with_noise_and_push_depth(
        "brenn:ch-pullonly-panic-test",
        NoiseLevel::Silent,
        Depth::Bounded(0),
    );
    let raw = vec![WasmConsumerConfigRaw {
        slug: "consumer-e".to_string(),
        package: "e".to_string(),
        subscriptions: vec![WasmConsumerSubscriptionRaw {
            noise: Some(NoiseLevel::Alarm),
            ..sub_raw(&chan_addr, "in")
        }],
        ..minimal_wasm_consumer()
    }];
    resolve(&raw, &dir);
}

// --- Port name validation ---

/// Duplicate port names (input/input) panic at bootstrap.
#[test]
#[should_panic(expected = "duplicate port name")]
fn duplicate_input_port_names_panic() {
    let dir = dir_of(vec![brenn_entry("brenn:ch1"), brenn_entry("brenn:ch2")]);
    let raw = vec![WasmConsumerConfigRaw {
        slug: "dup-in".to_string(),
        package: "a".to_string(),
        subscriptions: vec![
            sub_raw("brenn:ch1", "same-port"),
            sub_raw("brenn:ch2", "same-port"), // duplicate
        ],
        ..minimal_wasm_consumer()
    }];
    resolve(&raw, &dir);
}

/// Duplicate port names (input/output) panic at bootstrap.
#[test]
#[should_panic(expected = "duplicate port name")]
fn input_output_port_name_collision_panics() {
    let dir = dir_of(vec![
        brenn_entry("brenn:in-ch"),
        brenn_entry("brenn:out-ch"),
    ]);
    let raw = vec![WasmConsumerConfigRaw {
        slug: "dup-io".to_string(),
        package: "a".to_string(),
        subscriptions: vec![sub_raw("brenn:in-ch", "shared-port")],
        outputs: vec![out_raw("shared-port", "brenn:out-ch")], // collides with input port
        ..minimal_wasm_consumer()
    }];
    resolve(&raw, &dir);
}

/// One subscription and one output naming the *same* channel are one
/// bidirectional port, not a collision.
#[test]
fn input_output_port_name_on_one_channel_is_a_bidirectional_port() {
    let dir = dir_of(vec![brenn_entry("brenn:both-ch")]);
    let raw = vec![WasmConsumerConfigRaw {
        slug: "io-pair".to_string(),
        package: "a".to_string(),
        grants: vec![ComponentGrant::Ports],
        publish_acl: vec![brenn_lib::access::raw::ChannelMatcherRaw::Exact(
            "both-ch".to_string(),
        )],
        subscriptions: vec![sub_raw("brenn:both-ch", "total")],
        outputs: vec![out_raw("total", "brenn:both-ch")],
        ..minimal_wasm_consumer()
    }];
    let resolved = resolve(&raw, &dir);
    assert_eq!(resolved.len(), 1);
    let c = &resolved[0];
    assert_eq!(c.inputs.len(), 1);
    assert_eq!(c.inputs[0].port, "total");
    assert_eq!(c.outputs.len(), 1);
    assert_eq!(c.outputs[0].port, "total");
    assert_eq!(
        c.inputs[0].sub.channel_address,
        c.outputs[0].channel_address
    );
}

/// A third binding on that name is still a collision — the exception admits one
/// input and one output, not an unbounded set.
#[test]
#[should_panic(expected = "duplicate port name")]
fn third_binding_on_a_bidirectional_port_name_panics() {
    let dir = dir_of(vec![brenn_entry("brenn:both-ch")]);
    let raw = vec![WasmConsumerConfigRaw {
        slug: "io-pair-plus".to_string(),
        package: "a".to_string(),
        subscriptions: vec![sub_raw("brenn:both-ch", "total")],
        outputs: vec![
            out_raw("total", "brenn:both-ch"),
            out_raw("total", "brenn:both-ch"),
        ],
        ..minimal_wasm_consumer()
    }];
    resolve(&raw, &dir);
}

/// A config input port literally named `tool-results` collides with the
/// synthetic async-tool-result inbox port folded in for a consumer holding an
/// async tool grant; it must be rejected at resolve time.
#[test]
#[should_panic(expected = "is reserved for the async tool-result inbox")]
fn reserved_tool_results_input_port_panics() {
    let dir = dir_of(vec![brenn_entry("brenn:ch1")]);
    let raw = vec![WasmConsumerConfigRaw {
        slug: "reserved-in".to_string(),
        package: "a".to_string(),
        subscriptions: vec![sub_raw(
            "brenn:ch1",
            brenn_tool_registry::bus_wiring::TOOL_RESULT_INPUT_PORT,
        )],
        ..minimal_wasm_consumer()
    }];
    resolve(&raw, &dir);
}

/// The reserved `tool-results` name is rejected on the output side too.
#[test]
#[should_panic(expected = "is reserved for the async tool-result inbox")]
fn reserved_tool_results_output_port_panics() {
    let dir = dir_of(vec![
        brenn_entry("brenn:in-ch"),
        brenn_entry("brenn:out-ch"),
    ]);
    let raw = vec![WasmConsumerConfigRaw {
        slug: "reserved-out".to_string(),
        package: "a".to_string(),
        subscriptions: vec![sub_raw("brenn:in-ch", "in")],
        outputs: vec![out_raw(
            brenn_tool_registry::bus_wiring::TOOL_RESULT_INPUT_PORT,
            "brenn:out-ch",
        )],
        ..minimal_wasm_consumer()
    }];
    resolve(&raw, &dir);
}

/// Output channel must be a pub/sub scheme; an egress scheme (webhook:) panics.
#[test]
#[should_panic(expected = "must be a pub/sub address")]
fn egress_scheme_output_channel_panics() {
    let wh = ChannelEntry {
        uuid: webhook_channel_uuid_from_slug("wh"),
        transport_type: ChannelScheme::Webhook,
        ..brenn_entry("webhook:wh")
    };
    let dir = dir_of(vec![brenn_entry("brenn:in-ch"), wh]);
    let raw = vec![WasmConsumerConfigRaw {
        slug: "bad-out".to_string(),
        package: "a".to_string(),
        subscriptions: vec![sub_raw("brenn:in-ch", "in")],
        outputs: vec![out_raw("out", "webhook:wh")], // must panic
        ..minimal_wasm_consumer()
    }];
    resolve(&raw, &dir);
}

/// An `ephemeral:` output resolves when the consumer carries a covering
/// `ephemeral_publish_acl`.
#[test]
fn ephemeral_output_resolves_with_ephemeral_publish_acl() {
    let in_e = brenn_entry("brenn:in-ch");
    let eph = ChannelEntry {
        uuid: uuid::Uuid::new_v4(),
        transport_type: ChannelScheme::Ephemeral,
        ..brenn_entry("ephemeral:eph-out")
    };
    let dir = dir_of(vec![in_e, eph]);
    let raw = vec![WasmConsumerConfigRaw {
        slug: "eph-pub".to_string(),
        package: "a".to_string(),
        grants: vec![ComponentGrant::Ports],
        ephemeral_publish_acl: vec![brenn_lib::access::raw::ChannelMatcherRaw::Exact(
            "eph-out".to_string(),
        )],
        subscriptions: vec![sub_raw("brenn:in-ch", "in")],
        outputs: vec![out_raw("out", "ephemeral:eph-out")],
        ..minimal_wasm_consumer()
    }];
    let resolved = resolve(&raw, &dir);
    let c = &resolved[0];
    assert_eq!(c.outputs[0].channel_address, "ephemeral:eph-out");
    assert!(
        c.policy.allows_ephemeral_publish("eph-out"),
        "the resolved policy must authorize the bound ephemeral output"
    );
    assert!(
        !c.policy.allows_ephemeral_publish("other"),
        "authorization is scoped to the authored matcher"
    );
}

/// A bound `ephemeral:` output with no `ephemeral_publish_acl` panics at boot —
/// under deny-by-default it could never publish to its own bound channel.
#[test]
#[should_panic(expected = "ephemeral_publish_acl is empty")]
fn ephemeral_output_without_acl_panics() {
    let eph = ChannelEntry {
        uuid: uuid::Uuid::new_v4(),
        transport_type: ChannelScheme::Ephemeral,
        ..brenn_entry("ephemeral:eph-out")
    };
    let dir = dir_of(vec![brenn_entry("brenn:in-ch"), eph]);
    let raw = vec![WasmConsumerConfigRaw {
        slug: "eph-noacl".to_string(),
        package: "a".to_string(),
        grants: vec![ComponentGrant::Ports],
        subscriptions: vec![sub_raw("brenn:in-ch", "in")],
        outputs: vec![out_raw("out", "ephemeral:eph-out")], // must panic
        ..minimal_wasm_consumer()
    }];
    resolve(&raw, &dir);
}

/// A `local:` output resolves when the consumer carries a covering
/// `local_publish_acl`.
#[test]
fn local_output_resolves_with_local_publish_acl() {
    let in_e = brenn_entry("brenn:in-ch");
    let loc = ChannelEntry {
        uuid: uuid::Uuid::new_v4(),
        transport_type: ChannelScheme::Local,
        ..brenn_entry("local:loc-out")
    };
    let dir = dir_of(vec![in_e, loc]);
    let raw = vec![WasmConsumerConfigRaw {
        slug: "loc-pub".to_string(),
        package: "a".to_string(),
        grants: vec![ComponentGrant::Ports],
        local_publish_acl: vec![brenn_lib::access::raw::ChannelMatcherRaw::Exact(
            "loc-out".to_string(),
        )],
        subscriptions: vec![sub_raw("brenn:in-ch", "in")],
        outputs: vec![out_raw("out", "local:loc-out")],
        ..minimal_wasm_consumer()
    }];
    let resolved = resolve(&raw, &dir);
    let c = &resolved[0];
    assert_eq!(c.outputs[0].channel_address, "local:loc-out");
    assert!(
        c.policy.allows_local_publish("loc-out"),
        "the resolved policy must authorize the bound local output"
    );
    assert!(
        !c.policy.allows_local_publish("other"),
        "authorization is scoped to the authored matcher"
    );
}

/// A bound `local:` output with no `local_publish_acl` panics at boot —
/// under deny-by-default it could never publish to its own bound channel.
#[test]
#[should_panic(expected = "local_publish_acl is empty")]
fn local_output_without_acl_panics() {
    let loc = ChannelEntry {
        uuid: uuid::Uuid::new_v4(),
        transport_type: ChannelScheme::Local,
        ..brenn_entry("local:loc-out")
    };
    let dir = dir_of(vec![brenn_entry("brenn:in-ch"), loc]);
    let raw = vec![WasmConsumerConfigRaw {
        slug: "loc-noacl".to_string(),
        package: "a".to_string(),
        grants: vec![ComponentGrant::Ports],
        subscriptions: vec![sub_raw("brenn:in-ch", "in")],
        outputs: vec![out_raw("out", "local:loc-out")], // must panic
        ..minimal_wasm_consumer()
    }];
    resolve(&raw, &dir);
}

/// Outputs-without-inputs is dead config → panic.
#[test]
#[should_panic(expected = "has output port(s) but no subscriptions")]
fn outputs_without_inputs_panics() {
    let (dir, out_addr) = make_brenn_dir("brenn:out-only");
    let raw = vec![WasmConsumerConfigRaw {
        slug: "dead-config".to_string(),
        package: "a".to_string(),
        subscriptions: vec![], // no inputs
        outputs: vec![out_raw("out", &out_addr)],
        ..minimal_wasm_consumer()
    }];
    resolve(&raw, &dir);
}

/// The class's declared out-port vocabulary reaches the resolved consumer
/// whole: the bound port and the ports the deployer left unwired alike.
///
/// The unwired half is the point. Resolution drops an unbound port from
/// `outputs`, so before this the host had no way to tell a port its author
/// declared and its deployer left unheard from a name no specification
/// mentions.
#[test]
fn a_resolved_consumer_carries_the_ports_its_class_declares_but_binds_none_of() {
    let dir = dir_of(vec![
        brenn_entry("brenn:in-ch"),
        brenn_entry("brenn:out-ch"),
    ]);
    let raw = vec![WasmConsumerConfigRaw {
        slug: "fan".to_string(),
        package: "fan".to_string(),
        grants: vec![ComponentGrant::Ports],
        publish_acl: vec![brenn_lib::access::raw::ChannelMatcherRaw::Exact(
            "out-ch".to_string(),
        )],
        subscriptions: vec![sub_raw("brenn:in-ch", "inbound")],
        outputs: vec![out_raw("digest", "brenn:out-ch")],
        declared_out_ports: vec![
            "digest".to_string(),
            "tick".to_string(),
            "unheard".to_string(),
        ],
        ..minimal_wasm_consumer()
    }];

    let consumer = &resolve(&raw, &dir)[0];
    assert_eq!(
        consumer
            .declared_out_ports
            .iter()
            .cloned()
            .collect::<Vec<_>>(),
        ["digest", "tick", "unheard"],
    );
    assert_eq!(
        consumer.outputs.len(),
        1,
        "only the bound port has a sink; the other two are vocabulary and nothing else",
    );
}

/// A bound output the class does not declare is the two halves of lowering
/// disagreeing about which class the instance is an instance of — the compiler
/// refuses the binding, so reaching resolution with one means something above
/// is broken. Refused by name rather than resolved into a sink the host would
/// later refuse to publish on.
#[test]
#[should_panic(expected = "is bound but is not in the class's declared out-port vocabulary")]
fn a_bound_output_outside_the_declared_vocabulary_panics() {
    let dir = dir_of(vec![
        brenn_entry("brenn:in-ch"),
        brenn_entry("brenn:out-ch"),
    ]);
    let raw = vec![WasmConsumerConfigRaw {
        slug: "fan".to_string(),
        package: "fan".to_string(),
        grants: vec![ComponentGrant::Ports],
        publish_acl: vec![brenn_lib::access::raw::ChannelMatcherRaw::Exact(
            "out-ch".to_string(),
        )],
        subscriptions: vec![sub_raw("brenn:in-ch", "inbound")],
        outputs: vec![out_raw("digest", "brenn:out-ch")],
        declared_out_ports: vec!["unheard".to_string()],
        ..minimal_wasm_consumer()
    }];
    resolve(&raw, &dir);
}

/// A declared name is held to the charset every port name is held to: it is
/// what the host compares a guest's publish against, and what a trap
/// diagnostic prints.
#[test]
#[should_panic(expected = "declared out-port name \"bad port\" must consist of")]
fn a_declared_out_port_outside_the_charset_panics() {
    let dir = dir_of(vec![brenn_entry("brenn:in-ch")]);
    let raw = vec![WasmConsumerConfigRaw {
        slug: "fan".to_string(),
        package: "fan".to_string(),
        grants: vec![ComponentGrant::Ports],
        subscriptions: vec![sub_raw("brenn:in-ch", "inbound")],
        declared_out_ports: vec!["bad port".to_string()],
        ..minimal_wasm_consumer()
    }];
    resolve(&raw, &dir);
}

/// A vocabulary that names one port twice is a lowered set that is not a set.
/// Refused here rather than silently deduplicated: the two halves of lowering
/// disagree, and which one is right is not this code's guess to make.
#[test]
#[should_panic(expected = "declared out-port name \"digest\" appears twice")]
fn a_declared_out_port_named_twice_panics() {
    let dir = dir_of(vec![brenn_entry("brenn:in-ch")]);
    let raw = vec![WasmConsumerConfigRaw {
        slug: "fan".to_string(),
        package: "fan".to_string(),
        grants: vec![ComponentGrant::Ports],
        subscriptions: vec![sub_raw("brenn:in-ch", "inbound")],
        declared_out_ports: vec!["digest".to_string(), "digest".to_string()],
        ..minimal_wasm_consumer()
    }];
    resolve(&raw, &dir);
}

#[test]
#[should_panic(expected = "declared out-port name must be non-empty")]
fn an_empty_declared_out_port_name_panics() {
    let dir = dir_of(vec![brenn_entry("brenn:in-ch")]);
    let raw = vec![WasmConsumerConfigRaw {
        slug: "fan".to_string(),
        package: "fan".to_string(),
        grants: vec![ComponentGrant::Ports],
        subscriptions: vec![sub_raw("brenn:in-ch", "inbound")],
        declared_out_ports: vec![String::new()],
        ..minimal_wasm_consumer()
    }];
    resolve(&raw, &dir);
}

/// `ResolvedWasmConsumer` carries correct port name, channel_uuid, and
/// channel_address for an input and an output.
#[test]
fn resolved_consumer_carries_port_and_channel_info() {
    let in_e = brenn_entry("brenn:in-ch");
    let out_e = brenn_entry("brenn:out-ch");
    let in_uuid = in_e.uuid;
    let out_uuid = out_e.uuid;
    let dir = dir_of(vec![in_e, out_e]);
    let raw = vec![WasmConsumerConfigRaw {
        slug: "my-consumer".to_string(),
        package: "a".to_string(),
        grants: vec![ComponentGrant::Ports],
        // Bound output port requires a covering publish_acl; without it
        // resolution panics (bound-ports + empty publish_acl).
        publish_acl: vec![brenn_lib::access::raw::ChannelMatcherRaw::Exact(
            "out-ch".to_string(),
        )],
        subscriptions: vec![sub_raw("brenn:in-ch", "data-in")],
        outputs: vec![out_raw("data-out", "brenn:out-ch")],
        ..minimal_wasm_consumer()
    }];

    let result = resolve(&raw, &dir);
    assert_eq!(result.len(), 1);
    let c = &result[0];
    assert_eq!(c.slug, "my-consumer");
    assert_eq!(c.inputs.len(), 1);
    assert_eq!(c.inputs[0].port, "data-in");
    assert_eq!(c.inputs[0].sub.channel_uuid, in_uuid);
    assert_eq!(c.inputs[0].sub.channel_address, "brenn:in-ch");
    assert_eq!(c.outputs.len(), 1);
    assert_eq!(c.outputs[0].port, "data-out");
    assert_eq!(c.outputs[0].channel_uuid, out_uuid);
    assert_eq!(c.outputs[0].channel_address, "brenn:out-ch");
    // Grants must round-trip through resolution.
    assert_eq!(
        c.grants,
        std::collections::BTreeSet::from([ComponentGrant::Ports]),
        "resolved grants must match configured grants"
    );
}

/// Bound output port(s) + empty `publish_acl` ⇒ bootstrap panic naming the
/// slug. Under the deny-by-default publish gate such a component could never
/// publish to its own operator-authored bound channels, so resolution refuses
/// it at startup rather than letting every publish silently deny at runtime.
#[test]
#[should_panic(expected = "bound brenn: output port but publish_acl is empty")]
fn bound_output_with_empty_publish_acl_panics() {
    let dir = dir_of(vec![
        brenn_entry("brenn:bound-in"),
        brenn_entry("brenn:bound-out"),
    ]);
    // publish_acl empty (from minimal) + bound output below → must panic.
    let raw = vec![WasmConsumerConfigRaw {
        slug: "bound-empty-acl".to_string(),
        package: "a".to_string(),
        grants: vec![ComponentGrant::Ports],
        subscriptions: vec![sub_raw("brenn:bound-in", "in")],
        outputs: vec![out_raw("out", "brenn:bound-out")],
        ..minimal_wasm_consumer()
    }];
    resolve(&raw, &dir);
}

/// A `Ports`-granted consumer with **no** bound output ports + empty
/// `publish_acl` is a legitimate intermediate state and resolves WITHOUT
/// panicking. Pins that the bound-ports panic does not fire for the
/// holds-the-grant-for-a-future-binding case.
#[test]
fn ports_grant_no_outputs_empty_publish_acl_resolves() {
    let (dir, chan_addr) = make_brenn_dir("brenn:no-outputs-ch");
    let raw = vec![WasmConsumerConfigRaw {
        slug: "ports-no-outputs".to_string(),
        package: "a".to_string(),
        grants: vec![ComponentGrant::Ports],
        subscriptions: vec![sub_raw(&chan_addr, "in")],
        ..minimal_wasm_consumer() // empty publish_acl, no bound outputs → must not panic
    }];
    let resolved = resolve(&raw, &dir);
    assert_eq!(
        resolved.len(),
        1,
        "Ports grant with no bound outputs + empty publish_acl must resolve"
    );
    assert!(resolved[0].outputs.is_empty(), "no output ports resolved");
}

/// Non-empty `publish_acl` without the `ports` grant ⇒ bootstrap panic. The
/// `build_wasm_policy` mapping derives `MessagingPublish` only from `Ports`, so
/// without that grant the authored matchers can never authorize any publish —
/// dead config. Fail-fast at resolution rather than silently dropping the ACL
/// (which would surface as an unexplained NotPermitted once outputs are added).
#[test]
#[should_panic(expected = "a publish ACL has matcher(s) but \"ports\" is not in grants")]
fn non_empty_publish_acl_without_ports_grant_panics() {
    use brenn_lib::access::raw::ChannelMatcherRaw;
    let (dir, chan_addr) = make_brenn_dir("brenn:dead-acl-ch");
    let raw = vec![WasmConsumerConfigRaw {
        slug: "dead-publish-acl".to_string(),
        package: "a".to_string(),
        publish_acl: vec![ChannelMatcherRaw::Exact("events".to_string())], // non-empty
        // no Ports grant → matchers can never authorize a publish.
        subscriptions: vec![sub_raw(&chan_addr, "in")],
        ..minimal_wasm_consumer()
    }];
    resolve(&raw, &dir);
}

/// Duplicate output port names (output/output collision) panic at bootstrap.
#[test]
#[should_panic(expected = "duplicate port name")]
fn duplicate_output_port_names_panic() {
    let dir = dir_of(vec![
        brenn_entry("brenn:in-ch"),
        brenn_entry("brenn:out-ch1"),
        brenn_entry("brenn:out-ch2"),
    ]);
    let raw = vec![WasmConsumerConfigRaw {
        slug: "dup-out".to_string(),
        package: "a".to_string(),
        subscriptions: vec![sub_raw("brenn:in-ch", "in")],
        outputs: vec![
            out_raw("same-out", "brenn:out-ch1"),
            out_raw("same-out", "brenn:out-ch2"), // duplicate
        ],
        ..minimal_wasm_consumer()
    }];
    resolve(&raw, &dir);
}

/// Empty port name panics at bootstrap.
#[test]
#[should_panic(expected = "non-empty")]
fn empty_port_name_panics() {
    let (dir, chan_addr) = make_brenn_dir("brenn:empty-port-ch");
    let raw = vec![WasmConsumerConfigRaw {
        slug: "empty-port".to_string(),
        package: "a".to_string(),
        subscriptions: vec![sub_raw(&chan_addr, "")], // empty → must panic
        ..minimal_wasm_consumer()
    }];
    resolve(&raw, &dir);
}

/// Port name containing a reserved character panics at bootstrap.
#[test]
#[should_panic(expected = "unreserved")]
fn reserved_char_port_name_panics() {
    let (dir, chan_addr) = make_brenn_dir("brenn:reserved-port-ch");
    let raw = vec![WasmConsumerConfigRaw {
        slug: "reserved-port".to_string(),
        package: "a".to_string(),
        subscriptions: vec![sub_raw(&chan_addr, "port:x")], // colon is reserved → must panic
        ..minimal_wasm_consumer()
    }];
    resolve(&raw, &dir);
}

/// Missing store_path parent directory panics at bootstrap.
#[test]
#[should_panic(expected = "parent directory does not exist")]
fn missing_store_path_parent_panics() {
    let (dir, chan_addr) = make_brenn_dir("brenn:no-parent-ch");
    let raw = vec![WasmConsumerConfigRaw {
        slug: "no-parent".to_string(),
        package: "a".to_string(),
        grants: vec![ComponentGrant::Store],
        store_path: Some(std::path::PathBuf::from(
            "/nonexistent_dir_xyz_brenn_test/store.sqlite",
        )),
        subscriptions: vec![sub_raw(&chan_addr, "in")],
        ..minimal_wasm_consumer()
    }];
    resolve(&raw, &dir);
}

// --- Config passthrough ---

/// `resolve_wasm_consumers` carries the resolved config map through to
/// `ResolvedWasmConsumer`. A `[[wasm_consumer]]` with a config table sees the
/// correct string-valued entries; one without sees an empty map.
#[test]
fn wasm_consumer_config_carried_through() {
    let (dir, chan_addr) = make_brenn_dir("brenn:config-test");

    let mut config_table = toml::Table::new();
    config_table.insert(
        "mode".to_string(),
        toml::Value::String("strict".to_string()),
    );
    config_table.insert("max-entries".to_string(), toml::Value::Integer(512));

    let raw = vec![WasmConsumerConfigRaw {
        slug: "cfg-consumer".to_string(),
        package: "a".to_string(),
        grants: vec![ComponentGrant::Config],
        config: Some(config_table),
        subscriptions: vec![sub_raw(&chan_addr, "in")],
        ..minimal_wasm_consumer()
    }];

    let result = resolve(&raw, &dir);
    assert_eq!(result.len(), 1);
    let cfg = &result[0].config;
    assert_eq!(cfg.get("mode").map(String::as_str), Some("strict"));
    assert_eq!(cfg.get("max-entries").map(String::as_str), Some("512"));
    assert_eq!(cfg.len(), 2);
    // Grants must round-trip through resolution.
    assert_eq!(
        result[0].grants,
        std::collections::BTreeSet::from([ComponentGrant::Config]),
        "resolved grants must match configured grants"
    );
}

/// A consumer without a config table gets an empty config map.
#[test]
fn wasm_consumer_absent_config_gives_empty_map() {
    let (dir, chan_addr) = make_brenn_dir("brenn:no-config-test");
    let raw = vec![minimal_wasm_consumer_raw(
        "no-cfg",
        "/tmp/a.wasm",
        &chan_addr,
    )];
    let result = resolve(&raw, &dir);
    assert!(
        result[0].config.is_empty(),
        "absent config table must yield empty map"
    );
}

// --- Grant resolution cross-validation panics ---

/// Duplicate entry in `grants` list panics naming the slug.
#[test]
#[should_panic(expected = "duplicate grant")]
fn duplicate_grant_entry_panics() {
    let (dir, chan_addr) = make_brenn_dir("brenn:dup-grant-test");
    let raw = vec![WasmConsumerConfigRaw {
        slug: "dup-grant".to_string(),
        package: "a".to_string(),
        grants: vec![ComponentGrant::Ports, ComponentGrant::Ports], // duplicate
        subscriptions: vec![sub_raw(&chan_addr, "in")],
        ..minimal_wasm_consumer()
    }];
    resolve(&raw, &dir);
}

/// The placement-legality table at the top-level end: `takeover` is a page
/// capability and a backend consumer has no page. Refused here rather than at
/// the linker mapping, which knows only that the word names no WIT interface.
#[test]
#[should_panic(expected = "a top-level consumer has no page")]
fn a_page_only_grant_on_a_consumer_panics() {
    let (dir, chan_addr) = make_brenn_dir("brenn:page-grant-test");
    let raw = vec![WasmConsumerConfigRaw {
        slug: "page-grant".to_string(),
        package: "a".to_string(),
        grants: vec![ComponentGrant::Takeover],
        subscriptions: vec![sub_raw(&chan_addr, "in")],
        ..minimal_wasm_consumer()
    }];
    resolve(&raw, &dir);
}

/// `outputs` non-empty but `ports` not in grants → panic naming slug.
#[test]
#[should_panic(expected = "\"ports\" is not in grants")]
fn outputs_without_ports_grant_panics() {
    // Both channels must be in the same directory so output resolution succeeds
    // and the Ports-grant check (which comes after output resolution) fires.
    let dir = dir_of(vec![
        brenn_entry("brenn:out-no-ports-in"),
        brenn_entry("brenn:out-no-ports-out"),
    ]);
    let raw = vec![WasmConsumerConfigRaw {
        slug: "out-no-ports".to_string(),
        package: "a".to_string(),
        grants: vec![], // Ports absent
        subscriptions: vec![sub_raw("brenn:out-no-ports-in", "in")],
        outputs: vec![out_raw("out", "brenn:out-no-ports-out")],
        ..minimal_wasm_consumer()
    }];
    resolve(&raw, &dir);
}

/// `[wasm_consumer.config]` table present but `config` not granted → panic
/// naming slug.
#[test]
#[should_panic(expected = "\"config\" is not in grants")]
fn config_table_without_config_grant_panics() {
    let (dir, chan_addr) = make_brenn_dir("brenn:cfg-no-grant-test");
    let mut config_table = toml::Table::new();
    config_table.insert("key".to_string(), toml::Value::String("val".to_string()));
    let raw = vec![WasmConsumerConfigRaw {
        slug: "cfg-no-grant".to_string(),
        package: "a".to_string(),
        config: Some(config_table), // Config grant absent
        subscriptions: vec![sub_raw(&chan_addr, "in")],
        ..minimal_wasm_consumer()
    }];
    resolve(&raw, &dir);
}

/// `store_path` set but `store` not granted → panic naming slug.
#[test]
#[should_panic(expected = "store_path is set but \"store\" is not in grants")]
fn store_path_without_store_grant_panics() {
    let (dir, chan_addr) = make_brenn_dir("brenn:store-path-no-grant-test");
    let raw = vec![WasmConsumerConfigRaw {
        slug: "store-path-no-grant".to_string(),
        package: "a".to_string(),
        store_path: Some(std::path::PathBuf::from("/nonexistent/x.sqlite")), // Store grant absent; path never read (grant check precedes store validation)
        subscriptions: vec![sub_raw(&chan_addr, "in")],
        ..minimal_wasm_consumer()
    }];
    resolve(&raw, &dir);
}

/// `store_size_limit` set but `store` not granted → panic naming slug.
#[test]
#[should_panic(expected = "store_size_limit is set but \"store\" is not in grants")]
fn store_size_limit_without_store_grant_panics() {
    let (dir, chan_addr) = make_brenn_dir("brenn:store-limit-no-grant-test");
    let raw = vec![WasmConsumerConfigRaw {
        slug: "store-limit-no-grant".to_string(),
        package: "a".to_string(),
        store_size_limit: Some("32MiB".to_string()), // Store grant absent
        subscriptions: vec![sub_raw(&chan_addr, "in")],
        ..minimal_wasm_consumer()
    }];
    resolve(&raw, &dir);
}

/// `store` granted but `store_path` absent → panic naming slug.
#[test]
#[should_panic(expected = "\"store\" is in grants but store_path is not set")]
fn store_grant_without_store_path_panics() {
    let (dir, chan_addr) = make_brenn_dir("brenn:store-grant-no-path-test");
    let raw = vec![WasmConsumerConfigRaw {
        slug: "store-grant-no-path".to_string(),
        package: "a".to_string(),
        grants: vec![ComponentGrant::Store],
        store_path: None, // absent — panic
        subscriptions: vec![sub_raw(&chan_addr, "in")],
        ..minimal_wasm_consumer()
    }];
    resolve(&raw, &dir);
}

// --- Publish token-bucket knob resolution ---

/// Build an `IndexMap` of declared MQTT clients from bare slugs, mirroring the
/// registry `resolve_wasm_consumers` cross-checks `mqtt_publish` matchers against.
fn declared_clients(slugs: &[&str]) -> IndexMap<String, brenn_lib::mqtt::config::MqttClientConfig> {
    let raw: Vec<_> = slugs
        .iter()
        .map(|s| brenn_lib::mqtt::config::MqttClientConfigRaw::minimal(s, "mqtts://127.0.0.1:1"))
        .collect();
    brenn_lib::mqtt::config::resolve_clients(&raw)
}

/// A one-subscription, one-output consumer with the `ports` grant and a publish
/// ACL covering the bound channel — the baseline the budget-knob tests mutate.
/// `chan` is the bare channel name (its `brenn:` address is bound to both the
/// input and the output port).
fn budget_consumer(chan: &str) -> WasmConsumerConfigRaw {
    use brenn_lib::access::raw::ChannelMatcherRaw;
    let addr = format!("brenn:{chan}");
    WasmConsumerConfigRaw {
        grants: vec![ComponentGrant::Ports],
        subscriptions: vec![sub_raw(&addr, "in")],
        outputs: vec![out_raw("out", &addr)],
        publish_acl: vec![ChannelMatcherRaw::Exact(chan.to_string())],
        ..minimal_wasm_consumer()
    }
    .implying_its_vocabulary()
}

/// Two-channel directory: one triggering (push) input and one pull-only
/// (`push_depth = 0`) context input on a distinct channel, so the dead-consumer
/// and dead-port checks pass and the pull-only-specific budget logic can be
/// exercised.
fn make_two_brenn_dirs(a: &str, b: &str) -> MessagingDirectory {
    dir_of(vec![brenn_entry(a), brenn_entry(b)])
}

#[test]
fn wasm_publish_knobs_absent_resolve_to_defaults() {
    let (dir, _) = make_brenn_dir("brenn:budget-defaults");
    let resolved = resolve(&[budget_consumer("budget-defaults")], &dir);
    let c = &resolved[0];
    assert_eq!(
        c.inputs[0].amplification_mt, 1000,
        "default amplification 1.0"
    );
    assert_eq!(
        c.outputs[0].budget,
        WasmSinkBudget {
            fill_mt: 1000,
            capacity_mt: 1000,
        },
        "default fill/capacity 1.0",
    );
}

#[test]
fn wasm_publish_knobs_explicit_convert_to_millitokens() {
    let (dir, _) = make_brenn_dir("brenn:budget-explicit");
    let mut raw = budget_consumer("budget-explicit");
    raw.subscriptions[0].amplification = Some(0.1);
    raw.outputs[0].publish_per_activation = Some(2.5);
    raw.outputs[0].publish_capacity = Some(0.0);
    let resolved = resolve(&[raw], &dir);
    let c = &resolved[0];
    assert_eq!(c.inputs[0].amplification_mt, 100, "0.1 -> 100 mt (exact)");
    assert_eq!(
        c.outputs[0].budget,
        WasmSinkBudget {
            fill_mt: 2500,
            capacity_mt: 0,
        },
    );
}

#[test]
fn wasm_mqtt_sink_defaults_from_publish_acl() {
    use brenn_lib::access::raw::MqttClientMatcherRaw;
    let (dir, _) = make_brenn_dir("brenn:mqtt-default-sink");
    let mut raw = budget_consumer("mqtt-default-sink");
    raw.grants = vec![ComponentGrant::Ports, ComponentGrant::Mqtt];
    raw.mqtt_publish_acl = vec![MqttClientMatcherRaw {
        client: "home".to_string(),
    }];
    let resolved = resolve_wasm_consumers(
        &[raw],
        &dir,
        "64MiB",
        &declared_clients(&["home"]),
        &no_auto(),
    );
    assert_eq!(
        resolved[0].mqtt_sinks.get("home"),
        Some(&WasmSinkBudget {
            fill_mt: 1000,
            capacity_mt: 1000,
        }),
        "an ACL-allowed client gets a default-budget sink",
    );
}

#[test]
fn wasm_mqtt_output_override_honored() {
    use brenn_lib::access::raw::MqttClientMatcherRaw;
    use brenn_lib::messaging::config::WasmConsumerMqttOutputRaw;
    let (dir, _) = make_brenn_dir("brenn:mqtt-override");
    let mut raw = budget_consumer("mqtt-override");
    raw.grants = vec![ComponentGrant::Ports, ComponentGrant::Mqtt];
    raw.mqtt_publish_acl = vec![MqttClientMatcherRaw {
        client: "home".to_string(),
    }];
    raw.mqtt_outputs = vec![WasmConsumerMqttOutputRaw {
        client: "home".to_string(),
        publish_per_activation: Some(5.0),
        publish_capacity: Some(3.0),
    }];
    let resolved = resolve_wasm_consumers(
        &[raw],
        &dir,
        "64MiB",
        &declared_clients(&["home"]),
        &no_auto(),
    );
    assert_eq!(
        resolved[0].mqtt_sinks.get("home"),
        Some(&WasmSinkBudget {
            fill_mt: 5000,
            capacity_mt: 3000,
        }),
    );
}

#[test]
#[should_panic(expected = "must be finite and >= 0")]
fn wasm_publish_knob_nan_panics() {
    let (dir, _) = make_brenn_dir("brenn:knob-nan");
    let mut raw = budget_consumer("knob-nan");
    raw.subscriptions[0].amplification = Some(f64::NAN);
    resolve(&[raw], &dir);
}

#[test]
#[should_panic(expected = "must be finite and >= 0")]
fn wasm_publish_knob_negative_panics() {
    let (dir, _) = make_brenn_dir("brenn:knob-neg");
    let mut raw = budget_consumer("knob-neg");
    raw.outputs[0].publish_per_activation = Some(-1.0);
    resolve(&[raw], &dir);
}

#[test]
#[should_panic(expected = "exceeds the maximum")]
fn wasm_publish_knob_above_ceiling_panics() {
    let (dir, _) = make_brenn_dir("brenn:knob-big");
    let mut raw = budget_consumer("knob-big");
    raw.outputs[0].publish_capacity = Some(1_000_000.5);
    resolve(&[raw], &dir);
}

#[test]
#[should_panic(expected = "would round to 0")]
fn wasm_publish_knob_subthreshold_panics() {
    let (dir, _) = make_brenn_dir("brenn:knob-tiny");
    let mut raw = budget_consumer("knob-tiny");
    raw.subscriptions[0].amplification = Some(0.0005);
    resolve(&[raw], &dir);
}

#[test]
#[should_panic(expected = "this sink can never publish")]
fn wasm_dead_sink_zero_fill_zero_amplification_panics() {
    let (dir, _) = make_brenn_dir("brenn:dead-sink");
    let mut raw = budget_consumer("dead-sink");
    raw.subscriptions[0].amplification = Some(0.0);
    raw.outputs[0].publish_per_activation = Some(0.0);
    resolve(&[raw], &dir);
}

/// An explicit `amplification` on a pull-only (`push_depth = 0`) subscription is
/// meaningless — a pull-only input never produces new envelopes so it can never
/// grant a token — and is rejected at boot, mirroring the noise/wake_min precedent.
#[test]
#[should_panic(expected = "amplification configured but push_depth = 0")]
fn wasm_explicit_amplification_on_pull_only_input_panics() {
    use brenn_lib::access::raw::ChannelMatcherRaw;
    let dir = make_two_brenn_dirs("brenn:push-in", "brenn:pull-in");
    let raw = WasmConsumerConfigRaw {
        grants: vec![ComponentGrant::Ports],
        subscriptions: vec![
            WasmConsumerSubscriptionRaw {
                push_depth: Some(Depth::Bounded(1)),
                ..sub_raw("brenn:push-in", "push")
            },
            WasmConsumerSubscriptionRaw {
                push_depth: Some(Depth::Bounded(0)),
                amplification: Some(1.0),
                ..sub_raw("brenn:pull-in", "pull")
            },
        ],
        outputs: vec![out_raw("out", "brenn:push-in")],
        publish_acl: vec![ChannelMatcherRaw::Exact("push-in".to_string())],
        ..minimal_wasm_consumer()
    };
    resolve(&[raw], &dir);
}

/// A pull-only input with the default (unset ⇒ 1.0) amplification must NOT count
/// as keeping a fill-0 sink alive: it can never produce new envelopes, so the
/// sink is dead and boot must fail. Regression gate for the dead-sink check being
/// fooled by a pull-only input's inert amplification.
#[test]
#[should_panic(expected = "this sink can never publish")]
fn wasm_dead_sink_pull_only_amplification_does_not_rescue() {
    use brenn_lib::access::raw::ChannelMatcherRaw;
    let dir = make_two_brenn_dirs("brenn:push-in2", "brenn:pull-in2");
    let raw = WasmConsumerConfigRaw {
        grants: vec![ComponentGrant::Ports],
        subscriptions: vec![
            WasmConsumerSubscriptionRaw {
                push_depth: Some(Depth::Bounded(1)),
                amplification: Some(0.0),
                ..sub_raw("brenn:push-in2", "push")
            },
            WasmConsumerSubscriptionRaw {
                push_depth: Some(Depth::Bounded(0)),
                // amplification defaults to 1.0, but pull-only ⇒ inert.
                ..sub_raw("brenn:pull-in2", "pull")
            },
        ],
        outputs: vec![WasmConsumerOutputRaw {
            publish_per_activation: Some(0.0),
            ..out_raw("out", "brenn:push-in2")
        }],
        publish_acl: vec![ChannelMatcherRaw::Exact("push-in2".to_string())],
        ..minimal_wasm_consumer()
    };
    resolve(&[raw], &dir);
}

#[test]
#[should_panic(expected = "not covered by mqtt_publish_acl")]
fn wasm_mqtt_output_unlisted_client_panics() {
    use brenn_lib::access::raw::MqttClientMatcherRaw;
    use brenn_lib::messaging::config::WasmConsumerMqttOutputRaw;
    let (dir, _) = make_brenn_dir("brenn:mqtt-unlisted");
    let mut raw = budget_consumer("mqtt-unlisted");
    raw.grants = vec![ComponentGrant::Ports, ComponentGrant::Mqtt];
    raw.mqtt_publish_acl = vec![MqttClientMatcherRaw {
        client: "home".to_string(),
    }];
    raw.mqtt_outputs = vec![WasmConsumerMqttOutputRaw {
        client: "away".to_string(),
        publish_per_activation: None,
        publish_capacity: None,
    }];
    resolve_wasm_consumers(
        &[raw],
        &dir,
        "64MiB",
        &declared_clients(&["home", "away"]),
        &no_auto(),
    );
}

#[test]
#[should_panic(expected = "duplicate [[mqtt_output]] block")]
fn wasm_mqtt_output_duplicate_client_panics() {
    use brenn_lib::access::raw::MqttClientMatcherRaw;
    use brenn_lib::messaging::config::WasmConsumerMqttOutputRaw;
    let (dir, _) = make_brenn_dir("brenn:mqtt-dup");
    let mut raw = budget_consumer("mqtt-dup");
    raw.grants = vec![ComponentGrant::Ports, ComponentGrant::Mqtt];
    raw.mqtt_publish_acl = vec![MqttClientMatcherRaw {
        client: "home".to_string(),
    }];
    raw.mqtt_outputs = vec![
        WasmConsumerMqttOutputRaw {
            client: "home".to_string(),
            publish_per_activation: Some(2.0),
            publish_capacity: None,
        },
        WasmConsumerMqttOutputRaw {
            client: "home".to_string(),
            publish_per_activation: Some(3.0),
            publish_capacity: None,
        },
    ];
    resolve_wasm_consumers(
        &[raw],
        &dir,
        "64MiB",
        &declared_clients(&["home"]),
        &no_auto(),
    );
}

// --- Free ports and the auto namespace ---

/// A channel-less subscription is a free port; with no `link` binding it,
/// nothing ever supplies a channel — dead config, same posture as an output
/// port on a consumer that never activates.
#[test]
#[should_panic(expected = "declares no channel and no link binds it")]
fn free_input_port_with_no_link_panics() {
    let (dir, chan_addr) = make_brenn_dir("brenn:free-in");
    let mut raw = minimal_wasm_consumer_raw("orphan", "/tmp/a.wasm", &chan_addr);
    raw.subscriptions.push(WasmConsumerSubscriptionRaw {
        channel: None,
        ..sub_raw(&chan_addr, "loose")
    });
    resolve(&raw_vec(raw), &dir);
}

/// Same for an output port: the free-port contract is direction-blind.
#[test]
#[should_panic(expected = "declares no channel and no link binds it")]
fn free_output_port_with_no_link_panics() {
    let (dir, chan_addr) = make_brenn_dir("brenn:free-out");
    let mut raw = minimal_wasm_consumer_raw("orphan-out", "/tmp/a.wasm", &chan_addr);
    raw.grants = vec![ComponentGrant::Ports];
    raw.outputs.push(WasmConsumerOutputRaw {
        channel: None,
        ..out_raw("loose", &chan_addr)
    });
    resolve(&raw_vec(raw), &dir);
}

/// An operator writing a hand-computed `auto.<cid>` on a binding is rejected: an
/// auto channel's endpoint set is its ACL, so it cannot be joined by address.
#[test]
#[should_panic(expected = "reserved auto namespace")]
fn operator_written_auto_address_on_input_panics() {
    let (dir, chan_addr) = make_brenn_dir("brenn:auto-ref");
    let mut raw = minimal_wasm_consumer_raw("snooper", "/tmp/a.wasm", &chan_addr);
    raw.subscriptions.push(sub_raw(
        "local:auto.9c1f4a4e-6d38-4a2e-9f1a-2f7c0d5b8e31",
        "peek",
    ));
    resolve(&raw_vec(raw), &dir);
}

/// The `auto` namespace is reserved on the pub/sub schemes, which are the only
/// ones an auto channel is ever placed on. An ingress endpoint slug wears its own
/// charset — `.` included — so `webhook:auto.github` is an ordinary webhook
/// channel whose name happens to start with a word this machinery uses.
#[test]
fn an_ingress_channel_named_auto_resolves() {
    let wh = ChannelEntry {
        uuid: webhook_channel_uuid_from_slug("auto.github"),
        transport_type: ChannelScheme::Webhook,
        ..brenn_entry("webhook:auto.github")
    };
    let dir = dir_of(vec![wh]);
    let raw = vec![WasmConsumerConfigRaw {
        slug: "hooks".to_string(),
        package: "a".to_string(),
        subscriptions: vec![sub_raw("webhook:auto.github", "in")],
        webhook_acl: vec![brenn_lib::access::raw::WebhookMatcherRaw {
            endpoint: "auto.github".to_string(),
        }],
        ..minimal_wasm_consumer()
    }];
    let resolved = resolve(&raw, &dir);
    assert_eq!(
        resolved[0].inputs[0].sub.channel_address,
        "webhook:auto.github"
    );
}

/// `pwa_push:` is an egress-only scheme; subscribing through it is refused.
#[test]
#[should_panic(expected = "names a scheme a consumer does not subscribe through")]
fn pwa_push_input_channel_panics() {
    let push = ChannelEntry {
        uuid: uuid::Uuid::new_v4(),
        transport_type: ChannelScheme::PwaPush,
        ..brenn_entry("pwa_push:phone")
    };
    let dir = dir_of(vec![push]);
    let raw = vec![WasmConsumerConfigRaw {
        slug: "pusher".to_string(),
        package: "a".to_string(),
        subscriptions: vec![sub_raw("pwa_push:phone", "in")],
        ..minimal_wasm_consumer()
    }];
    resolve(&raw, &dir);
}

/// An address with no recognized scheme prefix is refused.
#[test]
#[should_panic(expected = "carries no recognized scheme prefix")]
fn scheme_less_input_channel_panics() {
    let bare = ChannelEntry {
        uuid: uuid::Uuid::new_v4(),
        ..brenn_entry("noscheme")
    };
    let dir = dir_of(vec![bare]);
    let raw = vec![WasmConsumerConfigRaw {
        slug: "bare".to_string(),
        package: "a".to_string(),
        subscriptions: vec![sub_raw("noscheme", "in")],
        ..minimal_wasm_consumer()
    }];
    resolve(&raw, &dir);
}

/// The other half of the admit list: an `mqtt:` ingress port is a scheme a
/// consumer does subscribe through.
#[test]
fn an_mqtt_input_channel_resolves() {
    let topic = ChannelEntry {
        uuid: brenn_lib::messaging::mqtt_channel_uuid_from_address("mqtt:home:sensors/temp"),
        transport_type: ChannelScheme::Mqtt,
        ..brenn_entry("mqtt:home:sensors/temp")
    };
    let dir = dir_of(vec![topic]);
    let raw = vec![WasmConsumerConfigRaw {
        slug: "sensors".to_string(),
        package: "a".to_string(),
        subscriptions: vec![sub_raw("mqtt:home:sensors/temp", "in")],
        mqtt_subscribe_acl: vec![brenn_lib::access::raw::MqttSubMatcherRaw {
            client: "home".to_string(),
            topic_filter: "sensors/#".to_string(),
        }],
        grants: vec![ComponentGrant::Ports, ComponentGrant::Mqtt],
        ..minimal_wasm_consumer()
    }];
    let resolved = resolve_wasm_consumers(
        &raw,
        &dir,
        "64MiB",
        &declared_clients(&["home"]),
        &no_auto(),
    );
    assert_eq!(
        resolved[0].inputs[0].sub.channel_address,
        "mqtt:home:sensors/temp"
    );
}

/// The reservation is segment-bounded on binding addresses too: a channel whose
/// name merely starts with `auto` is an ordinary channel.
#[test]
fn auto_namespace_sibling_address_resolves() {
    let (dir, chan_addr) = make_brenn_dir("brenn:autobahn");
    let raw = minimal_wasm_consumer_raw("sibling", "/tmp/a.wasm", &chan_addr);
    let resolved = resolve(&raw_vec(raw), &dir);
    assert_eq!(resolved[0].inputs[0].sub.channel_address, chan_addr);
}

/// One-consumer slice for the resolver.
fn raw_vec(raw: WasmConsumerConfigRaw) -> Vec<WasmConsumerConfigRaw> {
    vec![raw]
}
