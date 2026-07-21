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
    Depth, MILLITOKENS_PER_PUBLISH, MessagingGlobalConfig, NoiseLevel, ResolvedChannel,
    ResolvedSubscription, WasmInputPort,
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

/// Build the `ResolvedChannel` for a programmatic tool channel from the global
/// messaging defaults, mirroring how webhook/mqtt channels inherit them. The
/// channel-level depths follow those operator defaults; the durability guarantee
/// that a request survives until the executor dequeues it (and a result until its
/// consumer drains it) is carried by the subscriber entries below, whose
/// push/retain depths are `Depth::Unbounded` (`eager_subscriber`,
/// `inbox_subscription`/`inbox_input_port`) — not by these channel-level values.
fn tool_resolved_channel(defaults: &MessagingGlobalConfig) -> ResolvedChannel {
    ResolvedChannel {
        push_depth: defaults.default_push_depth,
        retain_depth: defaults.default_retain_depth,
        standing_retain_depth: defaults.default_standing_retain_depth,
        noise: defaults.default_noise,
        sink: defaults.default_sink,
        wake_min: defaults.default_wake_min,
    }
}

/// The `brenn:tools/<tool>` request channel for one async tool. The
/// `system:tool-executor` subscriber is not pre-set here: it is folded in from
/// the executor's [`SystemParticipantSpec`] subscriptions
/// (`fold_spec_subscriptions`), like every system subscription. It is a
/// programmatic (non-config) subscriber, so it lives only in the directory —
/// no `messaging_subscriptions` row is written for it.
pub fn request_channel_entry(tool: &str, defaults: &MessagingGlobalConfig) -> ChannelEntry {
    let address = canonical_address(&request_channel_name(tool));
    ChannelEntry {
        uuid: tool_channel_uuid_from_address(&address),
        address,
        description: None,
        resolved_channel: tool_resolved_channel(defaults),
        subscribers: vec![],
        transport_type: ChannelScheme::Brenn,
        mount: None,
    }
}

/// The `brenn:tool-results/<slug>` inbox channel for one consumer, with no
/// subscriber pre-set: the consumer's `Wasm(slug)` subscription is folded in
/// through the normal wasm-subscription path (see [`inbox_subscription`]) so it
/// is written to both the directory and `messaging_subscriptions`, exactly like a
/// configured wasm subscription.
pub fn result_inbox_entry(slug: &str, defaults: &MessagingGlobalConfig) -> ChannelEntry {
    let address = canonical_address(&result_inbox_name(slug));
    ChannelEntry {
        uuid: tool_channel_uuid_from_address(&address),
        address,
        description: None,
        resolved_channel: tool_resolved_channel(defaults),
        subscribers: vec![],
        transport_type: ChannelScheme::Brenn,
        mount: None,
    }
}

/// The synthetic `ResolvedSubscription` for a consumer's own result inbox, folded
/// into the consumer's directory + DB subscriptions so a result publish reaches it
/// as an ordinary wasm delivery.
pub fn inbox_subscription(slug: &str) -> ResolvedSubscription {
    let address = canonical_address(&result_inbox_name(slug));
    ResolvedSubscription {
        channel_uuid: tool_channel_uuid_from_address(&address),
        channel_address: address,
        push_depth: Depth::Unbounded,
        retain_depth: Depth::Unbounded,
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
pub fn inbox_input_port(slug: &str) -> WasmInputPort {
    WasmInputPort {
        port: TOOL_RESULT_INPUT_PORT.to_string(),
        sub: inbox_subscription(slug),
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
        let req = request_channel_entry("apull", &defaults);
        assert_eq!(req.address, "brenn:tools/apull");
        assert_eq!(
            req.uuid,
            tool_channel_uuid_from_address("brenn:tools/apull")
        );
        // The executor subscriber is folded in from the spec, not pre-set here.
        assert!(req.subscribers.is_empty());
        let inbox = result_inbox_entry("sync", &defaults);
        assert_eq!(inbox.address, "brenn:tool-results/sync");
        assert!(inbox.subscribers.is_empty());
        let sub = inbox_subscription("sync");
        assert_eq!(sub.channel_uuid, inbox.uuid);
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
        let port = inbox_input_port("sync");
        assert_eq!(port.port, TOOL_RESULT_INPUT_PORT);
        assert_eq!(port.sub.channel_address, "brenn:tool-results/sync");
        // A triggering (push_depth > 0) port so a delivered result activates the
        // consumer and is not treated as sampled/context-only.
        assert!(matches!(port.sub.push_depth, Depth::Unbounded));
        // Same channel identity as the folded synthetic subscription.
        assert_eq!(
            port.sub.channel_uuid,
            inbox_subscription("sync").channel_uuid
        );
    }
}
