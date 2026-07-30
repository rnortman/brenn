//! Bootstrap-time derivations that connect the async tool substrate to the
//! messaging bus: the programmatic `brenn:tools/<tool>` request channels and
//! `brenn:tool-results/<slug>` inboxes, the `system:tool-executor` publish
//! policy, and the per-consumer async bus grants derived from a tool grant.
//!
//! These are pure functions over resolved config/policy types so the delicate
//! `build_messaging` assembly can call them and they can be unit-tested in
//! isolation. Nothing here touches the DB or the directory directly — the caller
//! folds the returned `ChannelEntry`s and `ResolvedSubscription`s into the same
//! finalize/rebuild path every other channel and subscription flows through.

use brenn_lib::access::acl::{AclSet, ChannelMatcher};
use brenn_lib::access::{AppCapability, AppPolicy, GrantSet};
use brenn_lib::messaging::config::{
    Depth, MILLITOKENS_PER_PUBLISH, MessagingGlobalConfig, NoiseLevel, ResolvedSubscription,
    SystemChannelTuning, WasmInputPort, resolve_system_channel,
};
use brenn_lib::messaging::{
    ChannelEntry, ChannelScheme, WakeMin, canonical_address, tool_channel_uuid_from_address,
};

use super::executor::TOOL_EXECUTOR_COMPONENT;
use super::registry::ToolRegistry;

/// Reserved namespace of the async-tool request channels (`brenn:tools/<tool>`).
pub const TOOLS_NAMESPACE: &str = "tools/";
/// Reserved namespace of the per-consumer result inboxes
/// (`brenn:tool-results/<slug>`).
pub const TOOL_RESULTS_NAMESPACE: &str = "tool-results/";

/// Bare (prefix-less) channel name of a tool's request channel.
pub fn request_channel_name(tool: &str) -> String {
    format!("{TOOLS_NAMESPACE}{tool}")
}

/// Bare (prefix-less) channel name of a consumer's result inbox.
pub fn result_inbox_name(slug: &str) -> String {
    format!("{TOOL_RESULTS_NAMESPACE}{slug}")
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

/// The `brenn:tool-results/<slug>` inbox channel for one consumer, with no
/// subscriber pre-set: the consumer's `Wasm(slug)` subscription is folded in
/// through the normal wasm-subscription path (see [`inbox_subscription`]),
/// exactly like a configured wasm subscription.
pub fn result_inbox_entry(
    slug: &str,
    tuning: &SystemChannelTuning,
    defaults: &MessagingGlobalConfig,
) -> ChannelEntry {
    let address = canonical_address(&result_inbox_name(slug));
    let resolved_channel = resolve_system_channel(&address, tuning, defaults);
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

/// The synthetic `ResolvedSubscription` for a consumer's own result inbox, folded
/// into the consumer's directory + DB subscriptions so a result publish reaches it
/// as an ordinary wasm delivery.
///
/// `window` is the inbox channel's own `retain_depth`, as
/// [`result_inbox_entry`] resolved it: the consumer is owed exactly what its
/// inbox retains, and a deeper subscriber would pin the channel against
/// reaping and take the sizing decision away from the operator's
/// `[[channel]]` block.
pub fn inbox_subscription(slug: &str, window: Depth) -> ResolvedSubscription {
    let address = canonical_address(&result_inbox_name(slug));
    ResolvedSubscription {
        channel_uuid: tool_channel_uuid_from_address(&address),
        channel_address: address,
        push_depth: window,
        retain_depth: window,
        noise: NoiseLevel::Silent,
        wake_min: WakeMin::Normal,
    }
}

/// Logical input port the consumer's result inbox is delivered on. Reserved; the
/// guest reads async tool-call results as activations on this port.
pub const TOOL_RESULT_INPUT_PORT: &str = "tool-results";

/// The consumer's own result inbox as a triggering `WasmInputPort`. Folded into
/// the consumer's `inputs` so a delivered result both activates the consumer and
/// survives the drain's residue reconciliation, which retires pending rows whose
/// channel is not a current input (`load_activation_snapshot`). Shares its
/// `ResolvedSubscription` with [`inbox_subscription`]; the default publish
/// amplification matches an ordinary input port.
pub fn inbox_input_port(slug: &str, window: Depth) -> WasmInputPort {
    WasmInputPort {
        port: TOOL_RESULT_INPUT_PORT.to_string(),
        sub: inbox_subscription(slug, window),
        amplification_mt: MILLITOKENS_PER_PUBLISH,
    }
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
) -> brenn_lib::messaging::system::SystemParticipantSpec {
    brenn_lib::messaging::system::SystemParticipantSpec {
        component: TOOL_EXECUTOR_COMPONENT,
        policy: tool_executor_system_policy(),
        subscriptions: async_tools
            .iter()
            .map(|tool| canonical_address(&request_channel_name(tool)))
            .collect(),
    }
}

/// The bootstrap-built `system:tool-executor` policy: subscribe on every
/// `brenn:tools/*` request channel (to receive requests) and publish on exactly
/// `brenn:tool-results/*` (to deliver results) — nothing else. Built in code, not
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
    AppPolicy {
        grants,
        acls,
        tool_grants: std::collections::BTreeMap::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tool_registry::descriptor::{Idempotency, ToolClass, ToolDescriptor};
    use crate::tool_registry::tool::{AsyncTool, FastTool, RegisteredTool, ToolCtx};
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
        ) -> Result<(), crate::tool_registry::descriptor::AclDenied> {
            Ok(())
        }
        async fn execute(
            &self,
            _c: &ToolCtx,
            _a: Value,
        ) -> Result<Value, crate::tool_registry::descriptor::ToolError> {
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
        ) -> Result<(), crate::tool_registry::descriptor::AclDenied> {
            Ok(())
        }
        fn execute(
            &self,
            _c: &ToolCtx,
            _a: Value,
        ) -> Result<Value, crate::tool_registry::descriptor::ToolError> {
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
    fn executor_policy_receives_requests_and_publishes_results_only() {
        let policy = tool_executor_system_policy();
        // Subscribe scope covers every request channel.
        assert!(policy.allows_channel_access("brenn:tools/apull"));
        assert!(policy.allows_channel_access("brenn:tools/git-repo-pull"));
        // Publish scope is exactly the result inboxes.
        assert!(policy.allows_brenn_publish("tool-results/sync"));
        assert!(!policy.allows_brenn_publish("tools/apull"));
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
        let sub = inbox_subscription("sync", inbox.resolved_channel.retain_depth);
        assert_eq!(sub.channel_uuid, inbox.uuid);
        assert_eq!(sub.push_depth, inbox.resolved_channel.retain_depth);
        assert_eq!(sub.retain_depth, inbox.resolved_channel.retain_depth);
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
        brenn_lib::messaging::system::fold_spec_subscriptions(
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

    #[test]
    fn inbox_input_port_is_triggering_on_the_own_inbox_channel() {
        let port = inbox_input_port("sync", Depth::Bounded(16));
        assert_eq!(port.port, TOOL_RESULT_INPUT_PORT);
        assert_eq!(port.sub.channel_address, "brenn:tool-results/sync");
        // A triggering (push_depth > 0) port so a delivered result activates the
        // consumer and is not treated as sampled/context-only.
        assert_eq!(port.sub.push_depth, Depth::Bounded(16));
        // Same channel identity as the folded synthetic subscription.
        assert_eq!(
            port.sub.channel_uuid,
            inbox_subscription("sync", Depth::Bounded(16)).channel_uuid
        );
    }
}
