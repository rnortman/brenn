//! Bootstrap-time derivations that connect the async tool substrate to the
//! messaging bus: the programmatic `brenn:tools/<tool>` request channels,
//! `brenn:tool-results/<slug>` inboxes and providers' `brenn:tool-requests/<slug>`
//! inboxes, the `system:tool-executor` publish policy, and the per-consumer bus
//! grants derived from a tool grant or a provided tool.
//!
//! These are pure functions over resolved config/policy types so the delicate
//! `build_messaging` assembly can call them and they can be unit-tested in
//! isolation. Nothing here touches the DB or the directory directly — the caller
//! folds the returned `ChannelEntry`s and `ResolvedSubscription`s into the same
//! finalize/rebuild path every other channel and subscription flows through.

use brenn_envelope::addressing::{
    PROVIDER_CONCURRENCY_CEILING, TOOL_REQUEST_INPUT_PORT, TOOL_REQUESTS_NAMESPACE,
    TOOL_RESULT_INPUT_PORT, TOOL_RESULTS_NAMESPACE, TOOLS_NAMESPACE,
};
use brenn_envelope::grants::AppCapability;
use brenn_lib::access::acl::{AclSet, ChannelMatcher};
use brenn_lib::access::{AppPolicy, GrantSet};
use brenn_lib::messaging::config::{
    Depth, MILLITOKENS_PER_PUBLISH, MessagingGlobalConfig, NoiseLevel, ProvidedToolDecl,
    ResolvedChannel, ResolvedSubscription, SendRate, SystemChannelTuning, WINDOW_DEPTH_CEILING,
    WasmInputPort, resolve_system_channel, resolve_wasm_window_depth,
};
use brenn_lib::messaging::{
    ChannelEntry, ChannelScheme, canonical_address, tool_channel_uuid_from_address,
};

use super::executor::TOOL_EXECUTOR_COMPONENT;
use super::registry::ToolRegistry;

/// Bare (prefix-less) channel name of a tool's request channel.
pub fn request_channel_name(tool: &str) -> String {
    format!("{TOOLS_NAMESPACE}{tool}")
}

/// Bare (prefix-less) channel name of a consumer's result inbox.
pub fn result_inbox_name(slug: &str) -> String {
    format!("{TOOL_RESULTS_NAMESPACE}{slug}")
}

/// Bare (prefix-less) channel name of a providing consumer's request inbox.
pub fn request_inbox_name(slug: &str) -> String {
    format!("{TOOL_REQUESTS_NAMESPACE}{slug}")
}

/// A tool-substrate channel minted by the system: durable `brenn:`, its uuid
/// derived from its address, with no description and no subscriber pre-set.
fn system_entry(address: String, resolved_channel: ResolvedChannel) -> ChannelEntry {
    ChannelEntry {
        uuid: tool_channel_uuid_from_address(&address),
        address,
        description: None,
        resolved_channel,
        subscribers: vec![],
        transport_type: ChannelScheme::Brenn,
        mount: None,
    }
}

/// The `brenn:tools/<tool>` request channel for one async tool. The
/// `system:tool-executor` subscriber is not pre-set here: it is folded in from
/// the executor's [`SystemParticipantSpec`] subscriptions
/// (`fold_spec_subscriptions`), like every system subscription.
///
/// Its `ResolvedChannel` comes from the one system-channel resolver, the same
/// one webhook and MQTT ingress channels go through. A tool channel has no
/// declaring `[[channel]]` block for the same reason they do not — the tool
/// substrate owns the `tools/` namespace — but an operator may still tune its
/// depths with a `[[channel]]` block addressing it.
pub fn request_channel_entry(
    tool: &str,
    tuning: &SystemChannelTuning,
    defaults: &MessagingGlobalConfig,
) -> ChannelEntry {
    let address = canonical_address(&request_channel_name(tool));
    let resolved_channel = resolve_system_channel(&address, tuning, defaults);
    system_entry(address, resolved_channel)
}

/// The send rate of the tool executor's publishes onto both tool inboxes,
/// `brenn:tool-results/<slug>` and `brenn:tool-requests/<slug>`, in place of
/// the operator's `default_send_rate`.
///
/// The executor is the only publisher on both families, and each publish is
/// one per call already admitted upstream (the caller's send bucket, the
/// per-grant limiter, the permits). A refusal here would lose an answer or
/// refuse an admitted call, so this rate must exceed any admitted inflow; the
/// ring's depth, not the rate, bounds memory.
///
/// A channel the substrate publishes on takes this rate; a channel a component
/// publishes on never does.
pub const TOOL_INBOX_SEND_RATE: SendRate = SendRate {
    burst: 100_000,
    refill_interval_secs: 1,
    refill: 10_000,
};

/// The `brenn:tool-results/<slug>` inbox channel for one consumer, with no
/// subscriber pre-set: the consumer's `Wasm(slug)` subscription is folded in
/// through the normal wasm-subscription path (see [`inbox_subscription`]),
/// exactly like a configured wasm subscription.
///
/// Depths, `noise`, `wake_min` and `sink` resolve from the operator's tuning
/// and defaults; the send rate is [`TOOL_INBOX_SEND_RATE`], and a tuning block
/// carrying `send_rate` on this family is refused when the table is built.
pub fn result_inbox_entry(
    slug: &str,
    tuning: &SystemChannelTuning,
    defaults: &MessagingGlobalConfig,
) -> ChannelEntry {
    let address = canonical_address(&result_inbox_name(slug));
    let mut resolved_channel = resolve_system_channel(&address, tuning, defaults);
    resolved_channel.send_rate = TOOL_INBOX_SEND_RATE;
    system_entry(address, resolved_channel)
}

/// The synthetic `ResolvedSubscription` for a consumer's own result inbox, folded
/// into the consumer's directory + DB subscriptions so a result publish reaches it
/// as an ordinary wasm delivery.
///
/// Both depth rungs are `ch`'s `retain_depth`: the consumer is owed exactly
/// what its inbox retains, and a deeper subscriber would pin the channel
/// against reaping. `noise` and `wake_min` are the channel's; `push_depth`
/// is not read.
pub fn inbox_subscription(slug: &str, ch: &ResolvedChannel) -> ResolvedSubscription {
    let address = canonical_address(&result_inbox_name(slug));
    let context = format!(
        "the tool-result inbox {address:?} of [[wasm_consumer]] {slug:?}, whose rungs both take \
         the inbox channel's retain_depth,"
    );
    ResolvedSubscription {
        channel_uuid: tool_channel_uuid_from_address(&address),
        channel_address: address,
        // The inbox is a WASM input port like any other, so its depths take the
        // shared window ceiling: `unbounded` tuning resolves to the ceiling and
        // anything above it is refused here rather than capped at the window
        // read. Each call is labelled with the rung it resolves, so the two
        // refusals are told apart in a log.
        push_depth: resolve_wasm_window_depth(ch.retain_depth, "push_depth", &context),
        retain_depth: resolve_wasm_window_depth(ch.retain_depth, "retain_depth", &context),
        noise: ch.noise,
        wake_min: ch.wake_min,
    }
}

/// The consumer's own result inbox as a triggering `WasmInputPort`. Folded into
/// the consumer's `inputs` so a delivered result both activates the consumer and
/// survives the drain's residue reconciliation, which retires pending rows whose
/// channel is not a current input (`load_activation_snapshot`). Shares its
/// `ResolvedSubscription` with [`inbox_subscription`]; the default publish
/// amplification matches an ordinary input port.
pub fn inbox_input_port(slug: &str, ch: &ResolvedChannel) -> WasmInputPort {
    WasmInputPort {
        port: TOOL_RESULT_INPUT_PORT.to_string(),
        sub: inbox_subscription(slug, ch),
        amplification_mt: MILLITOKENS_PER_PUBLISH,
    }
}

// A request inbox's depth is at most the provider ceiling and takes no window
// ceiling check of its own, so the ceiling must sit within the window a host
// reads.
const _: () = assert!(
    PROVIDER_CONCURRENCY_CEILING <= WINDOW_DEPTH_CEILING,
    "a provider's request inbox must fit the window one activation reads"
);

/// The depth N of a providing consumer's request inbox: the sum of its
/// provided tools' `max_concurrency`, at least one.
///
/// # Panics
///
/// When the sum exceeds [`PROVIDER_CONCURRENCY_CEILING`]: the config language
/// refuses such a class and `resolve_wasm_consumers` refuses such a hand-built
/// consumer, so a sum above it here is a host bug.
pub fn request_inbox_depth(slug: &str, provides: &[ProvidedToolDecl]) -> u64 {
    let sum: u64 = provides
        .iter()
        .map(|tool| u64::from(tool.max_concurrency))
        .sum();
    assert!(
        sum <= PROVIDER_CONCURRENCY_CEILING,
        "BUG: [[wasm_consumer]] {slug:?} reached the request-inbox fold with \
         Σ max_concurrency = {sum}, above PROVIDER_CONCURRENCY_CEILING = \
         {PROVIDER_CONCURRENCY_CEILING}; resolution refuses that sum",
    );
    sum.max(1)
}

/// The `brenn:tool-requests/<slug>` request inbox of a consumer that provides
/// tools: durable, `noise = metered`, send rate [`TOOL_INBOX_SEND_RATE`],
/// and `depth` deep — retain depth, standing depth and the channel's push
/// template all at `depth`
/// ([`request_inbox_depth`]). No subscriber is pre-set: the consumer's own is
/// folded in through [`request_inbox_subscription`].
///
/// No operator tuning applies: a tuning block on this family is refused, so the
/// depth is the declaration's, and neither the noise nor the send rate is the
/// operator's global default.
///
/// What the depth guarantees rests on three premises: every live request holds
/// one of its tool's permits from the moment its pending record is minted until
/// that record ends (completion, failure, expiry or a dropped waiter); only the
/// tool executor publishes on this address; and the depth and both rungs of the
/// consumer's subscription are N = max(Σ `max_concurrency`, 1). Under them, a
/// live request is pushed out of the window before the consumer sees it only
/// when a permit is freed with no consumer activation in between, which happens
/// two ways: (a) a request published later reaches its deadline — possible
/// seconds after its publish when it waited on its permit for most of its
/// deadline, or when it is a nested provided call admitted with a served
/// deadline nearer than its own batch's; (b) a waiter is dropped before its
/// call completes. Otherwise every live request is served as new. A request
/// already answered can always be pushed out, and that loses nothing. A live
/// request pushed out is never served; its waiter answers `internal` at its
/// deadline and nothing runs for it. That is a designed consequence of the
/// ceiling, not a misconfiguration, so the drop is metered rather than alarmed:
/// the per-tool ceiling alert is the signal that a provider is not keeping up.
pub fn request_inbox_entry(
    slug: &str,
    depth: u64,
    defaults: &MessagingGlobalConfig,
) -> ChannelEntry {
    let address = canonical_address(&request_inbox_name(slug));
    let mut resolved_channel =
        resolve_system_channel(&address, &SystemChannelTuning::default(), defaults);
    resolved_channel.push_depth = Depth::Bounded(depth);
    resolved_channel.retain_depth = Depth::Bounded(depth);
    resolved_channel.standing_retain_depth = Depth::Bounded(depth);
    resolved_channel.noise = NoiseLevel::Metered;
    resolved_channel.send_rate = TOOL_INBOX_SEND_RATE;
    system_entry(address, resolved_channel)
}

/// The providing consumer's subscription to its own request inbox: both rungs
/// at the inbox's retain depth, so every unseen request still in the window is
/// handed over as new and none as context. `noise` and `wake_min` are the
/// channel's.
pub fn request_inbox_subscription(slug: &str, ch: &ResolvedChannel) -> ResolvedSubscription {
    let address = canonical_address(&request_inbox_name(slug));
    ResolvedSubscription {
        channel_uuid: tool_channel_uuid_from_address(&address),
        channel_address: address,
        push_depth: ch.retain_depth,
        retain_depth: ch.retain_depth,
        noise: ch.noise,
        wake_min: ch.wake_min,
    }
}

/// The providing consumer's request inbox as the triggering `tool-requests`
/// input port, so a delivered request activates it. Shares its subscription
/// with [`request_inbox_subscription`].
pub fn request_input_port(slug: &str, ch: &ResolvedChannel) -> WasmInputPort {
    WasmInputPort {
        port: TOOL_REQUEST_INPUT_PORT.to_string(),
        sub: request_inbox_subscription(slug, ch),
        amplification_mt: MILLITOKENS_PER_PUBLISH,
    }
}

/// Derive a providing consumer's bus grants: the `MessagingSubscribe`
/// transport grant and an exact `brenn_subscribe` matcher on its own request
/// inbox, so the delivery gate admits its requests. No publish authority: a
/// provider answers through `provides.complete`, never on the bus.
pub fn derive_provided_bus_grants(policy: &mut AppPolicy, slug: &str) {
    policy.grants.insert(AppCapability::MessagingSubscribe);
    policy
        .acls
        .brenn_subscribe
        .push(ChannelMatcher::Exact(request_inbox_name(slug)));
}

/// The async-class tool names a consumer's resolved tool grants address. Fast
/// tools take no bus channel; only async grants derive an inbox and bus grants.
pub fn consumer_async_tools(registry: &ToolRegistry, policy: &AppPolicy) -> Vec<&'static str> {
    policy
        .tool_grants
        .keys()
        .filter_map(|name| match registry.get(name) {
            Some(super::tool::RegisteredTool::Async(a)) => Some(a.descriptor().name),
            _ => None,
        })
        .collect()
}

/// Derive the async-tool bus grants into a consumer's policy: the
/// `MessagingSubscribe` transport grant + a `brenn_subscribe` matcher on the
/// consumer's own inbox (so the delivery gate admits its results), and publish
/// visibility of each granted async tool's request channel. These are never
/// written in config; the tool grant is their authorization signal, so the
/// transport grants do not depend on a non-empty `subscribe_acl`/`publish_acl`.
pub fn derive_async_tool_bus_grants(policy: &mut AppPolicy, slug: &str, async_tools: &[&str]) {
    policy.grants.insert(AppCapability::MessagingSubscribe);
    policy
        .acls
        .brenn_subscribe
        .push(ChannelMatcher::Exact(result_inbox_name(slug)));
    policy.grants.insert(AppCapability::MessagingPublish);
    for tool in async_tools {
        policy
            .acls
            .brenn_publish
            .push(ChannelMatcher::Exact(request_channel_name(tool)));
    }
}

/// The executor's [`SystemParticipantSpec`]: the `system:tool-executor`
/// participant with its code-built policy and one static subscription per
/// registered async tool's request channel. Bootstrap derives the executor's
/// registry entry, directory subscriber entries, deliverability validation,
/// and parked-notify delivery binding from this one declaration.
pub fn tool_executor_spec(
    async_tools: &[&'static str],
) -> brenn_messaging::system::SystemParticipantSpec {
    brenn_messaging::system::SystemParticipantSpec {
        component: TOOL_EXECUTOR_COMPONENT,
        policy: tool_executor_system_policy(),
        subscriptions: async_tools
            .iter()
            .map(|tool| canonical_address(&request_channel_name(tool)))
            .collect(),
    }
}

/// The bootstrap-built `system:tool-executor` policy: subscribe on every
/// `brenn:tools/*` request channel (to receive requests), publish on
/// `brenn:tool-results/*` (to deliver results) and on `brenn:tool-requests/*`
/// (to hand a provided tool's call to its provider) — nothing else. Built in code, not
/// config, because the executor is substrate, not an operator participant.
pub fn tool_executor_system_policy() -> AppPolicy {
    let mut grants = GrantSet::default();
    grants.insert(AppCapability::MessagingSubscribe);
    grants.insert(AppCapability::MessagingPublish);
    let mut acls = AclSet::default();
    acls.brenn_subscribe
        .push(ChannelMatcher::Prefix(TOOLS_NAMESPACE.to_string()));
    acls.brenn_publish
        .push(ChannelMatcher::Prefix(TOOL_RESULTS_NAMESPACE.to_string()));
    acls.brenn_publish
        .push(ChannelMatcher::Prefix(TOOL_REQUESTS_NAMESPACE.to_string()));
    AppPolicy {
        grants,
        acls,
        tool_grants: std::collections::BTreeMap::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::descriptor::{Idempotency, ToolClass, ToolDescriptor};
    use crate::tool::{AsyncTool, FastTool, RegisteredTool, ToolCtx};
    use brenn_lib::messaging::config::{
        ChannelConfigRaw, Depth, NoiseLevel, build_system_channel_tuning,
    };
    use brenn_lib::messaging::directory::WakeMin;
    use brenn_lib::tools::AclClause;
    use serde_json::{Value, json};
    use std::sync::Arc;

    struct AsyncStub(ToolDescriptor);
    #[async_trait::async_trait]
    impl AsyncTool for AsyncStub {
        fn descriptor(&self) -> &ToolDescriptor {
            &self.0
        }
        fn check_acl(
            &self,
            _a: &Value,
            _c: &[AclClause],
        ) -> Result<(), crate::descriptor::AclDenied> {
            Ok(())
        }
        async fn execute(
            &self,
            _c: &ToolCtx,
            _a: Value,
        ) -> Result<Value, crate::descriptor::ToolError> {
            Ok(json!({}))
        }
    }
    struct FastStub(ToolDescriptor);
    impl FastTool for FastStub {
        fn descriptor(&self) -> &ToolDescriptor {
            &self.0
        }
        fn check_acl(
            &self,
            _a: &Value,
            _c: &[AclClause],
        ) -> Result<(), crate::descriptor::AclDenied> {
            Ok(())
        }
        fn execute(&self, _c: &ToolCtx, _a: Value) -> Result<Value, crate::descriptor::ToolError> {
            Ok(json!({}))
        }
    }

    fn desc(name: &'static str, mcp: &'static str, class: ToolClass) -> ToolDescriptor {
        ToolDescriptor {
            name,
            mcp_name: mcp,
            description: "stub",
            input_schema: json!({ "type": "object" }),
            class,
            acl_keys: &[],
            idempotency: Idempotency::Natural,
            auto_approve: true,
            user_binding: crate::UserBinding::None,
        }
    }

    fn registry() -> ToolRegistry {
        ToolRegistry::new(vec![
            RegisteredTool::Async(Arc::new(AsyncStub(desc(
                "apull",
                "mcp__brenn__APull",
                ToolClass::Async { max_concurrency: 2 },
            )))),
            RegisteredTool::Fast(Arc::new(FastStub(desc(
                "afast",
                "mcp__brenn__AFast",
                ToolClass::Fast {
                    budget: std::time::Duration::from_millis(5),
                },
            )))),
        ])
    }

    fn policy_with_grants(tools: &[&str]) -> AppPolicy {
        let mut tool_grants = std::collections::BTreeMap::new();
        for t in tools {
            tool_grants.insert(
                t.to_string(),
                brenn_lib::tools::ResolvedToolGrant {
                    acl: vec![],
                    rate_limit: None,
                },
            );
        }
        AppPolicy {
            grants: GrantSet::default(),
            acls: AclSet::default(),
            tool_grants,
        }
    }

    #[test]
    fn consumer_async_tools_filters_to_async_class() {
        let reg = registry();
        // A grant on the async tool + the fast tool: only the async one is a bus tool.
        let policy = policy_with_grants(&["apull", "afast"]);
        let names = consumer_async_tools(&reg, &policy);
        assert_eq!(names, vec!["apull"]);
    }

    #[test]
    fn derived_grants_admit_own_inbox_and_request_channel() {
        let mut policy = policy_with_grants(&["apull"]);
        derive_async_tool_bus_grants(&mut policy, "sync", &["apull"]);
        // Delivery of its own result inbox is authorized (transport grant + matcher).
        assert!(policy.allows_channel_access("brenn:tool-results/sync"));
        // A different consumer's inbox is not.
        assert!(!policy.allows_channel_access("brenn:tool-results/other"));
        // Publish visibility of the request channel.
        assert!(policy.allows_brenn_publish("tools/apull"));
        assert!(!policy.allows_brenn_publish("tools/other"));
    }

    #[test]
    fn executor_policy_receives_requests_and_publishes_results_and_provider_requests_only() {
        let policy = tool_executor_system_policy();
        // Subscribe scope covers every request channel.
        assert!(policy.allows_channel_access("brenn:tools/apull"));
        assert!(policy.allows_channel_access("brenn:tools/git-repo-pull"));
        // Publish scope is exactly the result inboxes and the providers'
        // request inboxes.
        assert!(policy.allows_brenn_publish("tool-results/sync"));
        assert!(policy.allows_brenn_publish("tool-requests/gmail"));
        assert!(!policy.allows_brenn_publish("tools/apull"));
        assert!(!policy.allows_channel_access("brenn:tool-requests/gmail"));
    }

    #[test]
    fn channel_entries_carry_stable_addresses_and_subscribers() {
        let defaults = MessagingGlobalConfig::default();
        let req = request_channel_entry("apull", &SystemChannelTuning::default(), &defaults);
        assert_eq!(req.address, "brenn:tools/apull");
        assert_eq!(
            req.uuid,
            tool_channel_uuid_from_address("brenn:tools/apull")
        );
        // The executor subscriber is folded in from the spec, not pre-set here.
        assert!(req.subscribers.is_empty());
        let inbox = result_inbox_entry("sync", &SystemChannelTuning::default(), &defaults);
        assert_eq!(inbox.address, "brenn:tool-results/sync");
        assert!(inbox.subscribers.is_empty());
        let sub = inbox_subscription("sync", &inbox.resolved_channel);
        assert_eq!(sub.channel_uuid, inbox.uuid);
        assert_eq!(sub.push_depth, inbox.resolved_channel.retain_depth);
        assert_eq!(sub.retain_depth, inbox.resolved_channel.retain_depth);
    }

    /// A tuning block on an inbox address, with the two depths the subscription
    /// could take set apart so neither can pass by coincidence. Standing is the
    /// ceiling on both and so matches the larger.
    fn inbox_tuning(
        address: &str,
        noise: Option<NoiseLevel>,
        wake_min: Option<WakeMin>,
    ) -> SystemChannelTuning {
        let raw = ChannelConfigRaw {
            push_depth: Some(Depth::Bounded(100)),
            retain_depth: Some(Depth::Bounded(20)),
            standing_retain_depth: Some(Depth::Bounded(100)),
            noise,
            wake_min,
            ..ChannelConfigRaw::minimal(address)
        };
        build_system_channel_tuning(&[raw], &MessagingGlobalConfig::default())
    }

    /// The folded-in inbox subscription inherits the tuning block's `noise` and
    /// `wake_min` — the whole ladder a configured subscription stating neither
    /// gets — and falls back to the globals when the block states nothing. The
    /// tuned `wake_min` is the one the defaults do not already carry, so a
    /// hard-coded rung cannot pass this.
    #[test]
    fn the_inbox_subscription_inherits_its_channels_noise_and_wake_min() {
        let defaults = MessagingGlobalConfig::default();
        assert_ne!(defaults.default_wake_min, WakeMin::High);
        let tuned = inbox_tuning(
            "brenn:tool-results/sync",
            Some(NoiseLevel::Alarm),
            Some(WakeMin::High),
        );
        let inbox = result_inbox_entry("sync", &tuned, &defaults);
        let sub = inbox_subscription("sync", &inbox.resolved_channel);
        assert_eq!(sub.noise, NoiseLevel::Alarm);
        assert_eq!(sub.wake_min, WakeMin::High);

        let untuned = result_inbox_entry("sync", &SystemChannelTuning::default(), &defaults);
        let sub = inbox_subscription("sync", &untuned.resolved_channel);
        assert_eq!(sub.noise, defaults.default_noise);
        assert_eq!(sub.wake_min, defaults.default_wake_min);
    }

    /// The block's `push_depth` is the channel-level inheritance template only.
    /// Both rungs of the inbox subscription are its `retain_depth`.
    #[test]
    fn the_inbox_subscriptions_rungs_are_both_the_blocks_retain_depth() {
        let tuned = inbox_tuning("brenn:tool-results/sync", None, None);
        let inbox = result_inbox_entry("sync", &tuned, &MessagingGlobalConfig::default());
        assert_eq!(inbox.resolved_channel.push_depth, Depth::Bounded(100));
        let sub = inbox_subscription("sync", &inbox.resolved_channel);
        assert_eq!(sub.push_depth, Depth::Bounded(20));
        assert_eq!(sub.retain_depth, Depth::Bounded(20));
    }

    /// The inbox's tuning at `retain_depth`, for the ceiling cases. Its standing
    /// depth is unbounded so the ceiling under test is the window one and not a
    /// channel-level bound in front of it.
    fn inbox_tuning_at_retain(address: &str, retain_depth: Depth) -> SystemChannelTuning {
        let raw = ChannelConfigRaw {
            push_depth: Some(Depth::Bounded(100)),
            retain_depth: Some(retain_depth),
            standing_retain_depth: Some(Depth::Unbounded),
            noise: None,
            wake_min: None,
            ..ChannelConfigRaw::minimal(address)
        };
        build_system_channel_tuning(&[raw], &MessagingGlobalConfig::default())
    }

    /// The inbox is a WASM input port, so an `unbounded` tuning resolves to the
    /// shared window ceiling here rather than reaching a host as `Unbounded`.
    #[test]
    fn an_unbounded_inbox_tuning_resolves_both_rungs_to_the_ceiling() {
        let tuned = inbox_tuning_at_retain("brenn:tool-results/sync", Depth::Unbounded);
        let inbox = result_inbox_entry("sync", &tuned, &MessagingGlobalConfig::default());
        let sub = inbox_subscription("sync", &inbox.resolved_channel);
        assert_eq!(
            sub.push_depth,
            Depth::Bounded(brenn_activation::WINDOW_DEPTH_CEILING)
        );
        assert_eq!(
            sub.retain_depth,
            Depth::Bounded(brenn_activation::WINDOW_DEPTH_CEILING)
        );
    }

    /// And a tuning above that ceiling is refused at boot, naming the inbox —
    /// never capped on the way to the window read.
    #[test]
    #[should_panic(expected = "the tool-result inbox")]
    fn an_inbox_tuning_above_the_ceiling_panics() {
        let tuned = inbox_tuning_at_retain(
            "brenn:tool-results/sync",
            Depth::Bounded(brenn_activation::WINDOW_DEPTH_CEILING + 1),
        );
        let inbox = result_inbox_entry("sync", &tuned, &MessagingGlobalConfig::default());
        let _ = inbox_subscription("sync", &inbox.resolved_channel);
    }

    /// A tool request channel stays reapable once the executor is folded onto
    /// it: the subscriber sits at the channel's window, so the reap frontier is
    /// the operator's standing number rather than `None`. A pinned frontier is
    /// what let executed requests accumulate forever and be re-executed by a
    /// re-minted cursor.
    #[test]
    fn the_folded_executor_leaves_its_request_channel_reapable() {
        let defaults = MessagingGlobalConfig::default();
        let mut entries = vec![request_channel_entry(
            "apull",
            &SystemChannelTuning::default(),
            &defaults,
        )];
        let window = entries[0].resolved_channel.retain_depth;
        brenn_messaging::system::fold_spec_subscriptions(
            &mut entries,
            &[tool_executor_spec(&["apull"])],
        );
        let sub = &entries[0].subscribers[0];
        assert_eq!(sub.push_depth, window);
        assert_eq!(sub.retain_depth, window);
        assert!(
            matches!(window, Depth::Bounded(_)),
            "the family default is bounded: {window:?}"
        );
        let Depth::Bounded(n) = entries[0].resolved_channel.standing_retain_depth else {
            panic!("the family default standing depth is bounded");
        };
        assert_eq!(entries[0].reap_frontier(), Some(n));
    }

    #[test]
    fn tool_executor_spec_subscribes_to_each_request_channel() {
        let spec = tool_executor_spec(&["apull", "git-repo-pull"]);
        assert_eq!(spec.component, TOOL_EXECUTOR_COMPONENT);
        assert_eq!(
            spec.subscriptions,
            vec!["brenn:tools/apull", "brenn:tools/git-repo-pull"]
        );
        // The spec's policy is the executor policy: it can receive on every
        // subscription it declares (the boot deliverability invariant).
        for address in &spec.subscriptions {
            assert!(spec.policy.allows_channel_access(address));
        }
    }

    fn provided(max_concurrency: u32) -> ProvidedToolDecl {
        ProvidedToolDecl {
            name: "echo-for-user".to_string(),
            mcp_name: "EchoForUser".to_string(),
            description: "Echo.".to_string(),
            input_schema: json!({ "type": "object" }),
            max_concurrency,
            auto_approve: true,
        }
    }

    /// N is the summed concurrency of the provider's tools, at least one.
    #[test]
    fn the_request_inbox_depth_is_the_summed_concurrency() {
        assert_eq!(
            request_inbox_depth("prov", &[provided(2), provided(30)]),
            32
        );
        assert_eq!(request_inbox_depth("prov", &[provided(3)]), 3);
        assert_eq!(request_inbox_depth("prov", &[]), 1);
    }

    #[test]
    #[should_panic(
        expected = "BUG: [[wasm_consumer]] \"prov\" reached the request-inbox fold \
                               with Σ max_concurrency = 33"
    )]
    fn a_request_inbox_sum_above_the_ceiling_is_a_bug() {
        request_inbox_depth("prov", &[provided(32), provided(1)]);
    }

    /// Depth, standing depth and both subscription rungs are N, and the rung is
    /// `metered` whatever the global default — here `alarm`.
    #[test]
    fn the_request_inbox_is_n_deep_and_metered() {
        let defaults = MessagingGlobalConfig {
            default_noise: NoiseLevel::Alarm,
            default_send_rate: SendRate {
                burst: 10,
                refill_interval_secs: 60,
                refill: 1,
            },
            ..MessagingGlobalConfig::default()
        };
        let inbox = request_inbox_entry("prov", 5, &defaults);
        assert_eq!(inbox.address, "brenn:tool-requests/prov");
        assert_eq!(
            inbox.uuid,
            tool_channel_uuid_from_address("brenn:tool-requests/prov")
        );
        assert_eq!(inbox.transport_type, ChannelScheme::Brenn);
        assert!(inbox.subscribers.is_empty());
        let ch = &inbox.resolved_channel;
        assert_eq!(ch.push_depth, Depth::Bounded(5));
        assert_eq!(ch.retain_depth, Depth::Bounded(5));
        assert_eq!(ch.standing_retain_depth, Depth::Bounded(5));
        assert_eq!(ch.noise, NoiseLevel::Metered);
        assert_eq!(ch.send_rate, TOOL_INBOX_SEND_RATE);

        let port = request_input_port("prov", ch);
        assert_eq!(port.port, TOOL_REQUEST_INPUT_PORT);
        assert_eq!(port.amplification_mt, MILLITOKENS_PER_PUBLISH);
        assert_eq!(port.sub.channel_address, "brenn:tool-requests/prov");
        assert_eq!(port.sub.channel_uuid, inbox.uuid);
        assert_eq!(port.sub.push_depth, Depth::Bounded(5));
        assert_eq!(port.sub.retain_depth, Depth::Bounded(5));
        assert_eq!(port.sub.noise, NoiseLevel::Metered);
        assert_eq!(port.sub.wake_min, defaults.default_wake_min);
    }

    /// The result inbox takes the host's send rate whatever the global default
    /// — here a restrictive one — while everything else still resolves from
    /// the defaults: depths as the system resolver gives them, and the global
    /// `noise`.
    #[test]
    fn the_result_inbox_takes_the_host_send_rate() {
        let restrictive = SendRate {
            burst: 10,
            refill_interval_secs: 60,
            refill: 1,
        };
        let defaults = MessagingGlobalConfig {
            default_noise: NoiseLevel::Alarm,
            default_send_rate: restrictive,
            ..MessagingGlobalConfig::default()
        };
        let empty = SystemChannelTuning::default();
        let inbox = result_inbox_entry("sync", &empty, &defaults);
        let ch = &inbox.resolved_channel;
        assert_eq!(ch.send_rate, TOOL_INBOX_SEND_RATE);
        assert_ne!(ch.send_rate, restrictive);
        let resolved = resolve_system_channel("brenn:tool-results/sync", &empty, &defaults);
        assert_eq!(ch.push_depth, resolved.push_depth);
        assert_eq!(ch.retain_depth, resolved.retain_depth);
        assert_eq!(ch.standing_retain_depth, resolved.standing_retain_depth);
        assert_eq!(ch.noise, NoiseLevel::Alarm);
        assert_eq!(ch.wake_min, defaults.default_wake_min);
    }

    /// A provider may receive on its own request inbox and on nothing else,
    /// and gains no publish authority at all.
    #[test]
    fn provided_grants_subscribe_to_the_own_request_inbox_only() {
        let mut policy = policy_with_grants(&[]);
        derive_provided_bus_grants(&mut policy, "prov");
        assert!(policy.allows_channel_access("brenn:tool-requests/prov"));
        assert!(!policy.allows_channel_access("brenn:tool-requests/other"));
        assert!(!policy.allows_channel_access("brenn:tool-results/prov"));
        assert!(!policy.grants.has(AppCapability::MessagingPublish));
        assert!(policy.acls.brenn_publish.is_empty());
    }

    #[test]
    fn inbox_input_port_is_triggering_on_the_own_inbox_channel() {
        let inbox = result_inbox_entry(
            "sync",
            &inbox_tuning("brenn:tool-results/sync", None, None),
            &MessagingGlobalConfig::default(),
        );
        let port = inbox_input_port("sync", &inbox.resolved_channel);
        assert_eq!(port.port, TOOL_RESULT_INPUT_PORT);
        assert_eq!(port.sub.channel_address, "brenn:tool-results/sync");
        // A triggering (push_depth > 0) port so a delivered result activates the
        // consumer and is not treated as sampled/context-only.
        assert_eq!(port.sub.push_depth, Depth::Bounded(20));
        // Same channel identity as the folded synthetic subscription.
        assert_eq!(
            port.sub.channel_uuid,
            inbox_subscription("sync", &inbox.resolved_channel).channel_uuid
        );
    }
}
