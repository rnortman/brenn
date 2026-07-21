//! The real `brenn_wasm::ToolHost` over the native `ToolRegistry`.
//!
//! `brenn-wasm` cannot depend on `brenn-lib`/`brenn-server`, so the guest-facing
//! `tools` WIT interface calls back through the `ToolHost` seam for everything
//! that needs a native type: grant lookup, ACL-against-args, class dispatch, the
//! fast-tool time budget, and the async request-envelope resolution. One host is
//! built per WASM consumer holding ≥1 tool grant (the `Tools` capability is
//! derived + linked iff the map is non-empty), capturing that consumer's resolved
//! `tool_grants` and its `wasm:<slug>` caller identity.
//!
//! Fast calls execute synchronously here and return their result. Async calls are
//! validated and resolved into a `QueuedToolRequest` (channel/reply_to/body); the
//! guest buffers it and the dispatch layer flushes it transactionally.

use std::collections::BTreeMap;
use std::sync::Arc;

use brenn_lib::messaging::{ParticipantId, canonical_address};
use brenn_lib::obs::alerting::AlertDispatcher;
use brenn_lib::tools::ResolvedToolGrant;
use brenn_wasm::{QueuedToolRequest, ToolCallError, ToolHost};
use serde_json::json;

use super::bus_wiring;
use super::descriptor::ToolError;
use super::registry::ToolRegistry;
use super::tool::{RegisteredTool, ToolCtx};

/// A per-consumer tool host: the shared registry plus this consumer's resolved
/// grants and `wasm:<slug>` identity.
pub struct WasmToolHost {
    registry: Arc<ToolRegistry>,
    /// This consumer's resolved tool grants, keyed by canonical tool name. A tool
    /// absent from this map is ungranted for this caller (→ `NotGranted`).
    tool_grants: BTreeMap<String, ResolvedToolGrant>,
    /// The consumer slug (`<slug>`), used to name the result inbox `reply_to`.
    slug: String,
    /// The caller principal (`wasm:<slug>`), keyed into the body's `caller` field
    /// and the rate-limit bucket.
    caller: ParticipantId,
    alert: AlertDispatcher,
}

impl WasmToolHost {
    /// Build the host for a WASM consumer. `slug` must be a valid wasm slug (it
    /// already is — the consumer was resolved with it).
    pub fn new(
        registry: Arc<ToolRegistry>,
        tool_grants: BTreeMap<String, ResolvedToolGrant>,
        slug: String,
        alert: AlertDispatcher,
    ) -> Self {
        let caller = ParticipantId::for_wasm(&slug);
        Self {
            registry,
            tool_grants,
            slug,
            caller,
            alert,
        }
    }
}

/// Map the native tool error onto the crate-boundary seam error. Same variants;
/// the WIT layer maps this again to the guest-visible `tool-error`.
impl From<ToolError> for ToolCallError {
    fn from(e: ToolError) -> Self {
        match e {
            ToolError::NotGranted => ToolCallError::NotGranted,
            ToolError::Denied(r) => ToolCallError::Denied(r),
            ToolError::InvalidArgs(d) => ToolCallError::InvalidArgs(d),
            ToolError::RateLimited => ToolCallError::RateLimited,
            ToolError::WrongClass => ToolCallError::WrongClass,
            ToolError::Internal(t) => ToolCallError::Internal(t),
        }
    }
}

impl ToolHost for WasmToolHost {
    fn fast_call(&self, tool: &str, args_json: &str) -> Result<String, ToolCallError> {
        // Ungranted (or unknown) tool is one indistinguishable error (oracle
        // closure): a guest probing names learns nothing about what exists.
        let grant = self
            .tool_grants
            .get(tool)
            .ok_or(ToolCallError::NotGranted)?
            .clone();
        let args: serde_json::Value = serde_json::from_str(args_json)
            .map_err(|e| ToolCallError::InvalidArgs(e.to_string()))?;
        let ctx = ToolCtx {
            caller: self.caller.clone(),
            grant,
            acting_conversation_id: None,
        };
        // `invoke_fast` emits the unified invocation log (tool, caller, class,
        // duration, outcome) on every path, so nothing is logged here.
        let value = self.registry.invoke_fast(tool, &ctx, args, &self.alert)?;
        serde_json::to_string(&value).map_err(|e| ToolCallError::Internal(e.to_string()))
    }

    fn queue_async(
        &self,
        tool: &str,
        args_json: &str,
        call_id: &str,
    ) -> Result<QueuedToolRequest, ToolCallError> {
        let grant = self
            .tool_grants
            .get(tool)
            .ok_or(ToolCallError::NotGranted)?;
        // Resolve the tool and confirm its class. A grant for a fast tool reached
        // via `call-async` is a class mismatch (the class is the tool's, never the
        // caller's). An unregistered but somehow-granted tool is `NotGranted`.
        let registered = self.registry.get(tool).ok_or(ToolCallError::NotGranted)?;
        let async_tool = match registered {
            RegisteredTool::Async(t) => t,
            RegisteredTool::Fast(_) => return Err(ToolCallError::WrongClass),
        };
        let args: serde_json::Value = serde_json::from_str(args_json)
            .map_err(|e| ToolCallError::InvalidArgs(e.to_string()))?;
        // ACL-against-args at call time so caller mistakes fail fast. The executor
        // re-checks at dequeue (config may change between publish and execution).
        async_tool
            .check_acl(&args, &grant.acl)
            .map_err(|d| ToolCallError::Denied(d.resource))?;
        // Canonical name from the descriptor (the caller passed it, but the
        // descriptor is the source of truth for the channel name).
        let name = async_tool.descriptor().name;
        let body = json!({
            "v": 1,
            "tool": name,
            "call_id": call_id,
            "caller": self.caller.as_str(),
            "args": args,
        });
        // Derive the request channel and reply inbox through the shared bus-wiring
        // helpers (+ `canonical_address`) that the bootstrap and executor policy
        // own, so the reserved namespace is spelled in exactly one place.
        Ok(QueuedToolRequest {
            channel: canonical_address(&bus_wiring::request_channel_name(name)),
            reply_to: canonical_address(&bus_wiring::result_inbox_name(&self.slug)),
            body_json: serde_json::to_string(&body)
                .map_err(|e| ToolCallError::Internal(e.to_string()))?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tool_registry::descriptor::{
        AclDenied, DEFAULT_FAST_BUDGET, Idempotency, ToolClass, ToolDescriptor,
    };
    use crate::tool_registry::tool::{AsyncTool, FastTool};
    use brenn_lib::obs::alerting::noop_alert_dispatcher;
    use brenn_lib::tools::AclClause;

    /// A fast tool that echoes its args back and rejects any repo not in the ACL.
    struct EchoFast;
    impl FastTool for EchoFast {
        fn descriptor(&self) -> &ToolDescriptor {
            static D: std::sync::OnceLock<ToolDescriptor> = std::sync::OnceLock::new();
            D.get_or_init(|| ToolDescriptor {
                name: "echo",
                mcp_name: "mcp__brenn__Echo",
                description: "echo",
                input_schema: json!({ "type": "object" }),
                class: ToolClass::Fast {
                    budget: DEFAULT_FAST_BUDGET,
                },
                acl_keys: &["repo"],
                idempotency: Idempotency::Natural,
                auto_approve: true,
            })
        }
        fn check_acl(&self, args: &serde_json::Value, acl: &[AclClause]) -> Result<(), AclDenied> {
            crate::tool_registry::testutil::repo_acl_check(args, acl)
        }
        fn execute(
            &self,
            _ctx: &ToolCtx,
            args: serde_json::Value,
        ) -> Result<serde_json::Value, ToolError> {
            Ok(json!({ "echoed": args }))
        }
    }

    /// A trivial async tool with a `repo` ACL key.
    struct StubAsync;
    #[async_trait::async_trait]
    impl AsyncTool for StubAsync {
        fn descriptor(&self) -> &ToolDescriptor {
            static D: std::sync::OnceLock<ToolDescriptor> = std::sync::OnceLock::new();
            D.get_or_init(|| ToolDescriptor {
                name: "apull",
                mcp_name: "mcp__brenn__APull",
                description: "apull",
                input_schema: json!({ "type": "object" }),
                class: ToolClass::Async { max_concurrency: 2 },
                acl_keys: &["repo"],
                idempotency: Idempotency::Natural,
                auto_approve: true,
            })
        }
        fn check_acl(&self, args: &serde_json::Value, acl: &[AclClause]) -> Result<(), AclDenied> {
            crate::tool_registry::testutil::repo_acl_check(args, acl)
        }
        async fn execute(
            &self,
            _ctx: &ToolCtx,
            _args: serde_json::Value,
        ) -> Result<serde_json::Value, ToolError> {
            Ok(json!({}))
        }
    }

    use crate::tool_registry::testutil::{clause, grant};

    fn registry() -> Arc<ToolRegistry> {
        Arc::new(ToolRegistry::new(vec![
            RegisteredTool::Fast(Arc::new(EchoFast)),
            RegisteredTool::Async(Arc::new(StubAsync)),
        ]))
    }

    fn host(grants: BTreeMap<String, ResolvedToolGrant>) -> WasmToolHost {
        let (alert, _h) = noop_alert_dispatcher();
        WasmToolHost::new(registry(), grants, "sync".to_string(), alert)
    }

    #[tokio::test]
    async fn fast_call_happy_path_echoes_result() {
        let mut grants = BTreeMap::new();
        grants.insert(
            "echo".to_string(),
            grant(vec![clause(&[("repo", "brenn")])]),
        );
        let h = host(grants);
        let out = h
            .fast_call("echo", r#"{"repo":"brenn"}"#)
            .expect("granted fast call succeeds");
        let v: serde_json::Value = serde_json::from_str(&out).unwrap();
        assert_eq!(v["echoed"]["repo"], "brenn");
    }

    #[tokio::test]
    async fn fast_call_ungranted_is_not_granted() {
        // Empty grant map: even a registered tool is ungranted for this caller.
        let h = host(BTreeMap::new());
        assert_eq!(
            h.fast_call("echo", r#"{"repo":"brenn"}"#),
            Err(ToolCallError::NotGranted)
        );
    }

    #[tokio::test]
    async fn fast_call_unknown_tool_is_not_granted() {
        // A grant naming a tool the registry does not know is still NotGranted
        // (indistinguishable from ungranted — oracle closure). Registry config
        // validation rejects such a grant at startup; this guards the runtime path.
        let mut grants = BTreeMap::new();
        grants.insert("ghost".to_string(), grant(vec![]));
        let h = host(grants);
        assert_eq!(
            h.fast_call("ghost", r#"{}"#),
            Err(ToolCallError::NotGranted)
        );
    }

    #[tokio::test]
    async fn fast_call_on_async_tool_is_wrong_class() {
        let mut grants = BTreeMap::new();
        grants.insert(
            "apull".to_string(),
            grant(vec![clause(&[("repo", "brenn")])]),
        );
        let h = host(grants);
        assert_eq!(
            h.fast_call("apull", r#"{"repo":"brenn"}"#),
            Err(ToolCallError::WrongClass)
        );
    }

    #[tokio::test]
    async fn fast_call_acl_miss_is_denied() {
        let mut grants = BTreeMap::new();
        grants.insert(
            "echo".to_string(),
            grant(vec![clause(&[("repo", "brenn")])]),
        );
        let h = host(grants);
        assert_eq!(
            h.fast_call("echo", r#"{"repo":"pfin"}"#),
            Err(ToolCallError::Denied("pfin".to_string()))
        );
    }

    #[tokio::test]
    async fn fast_call_malformed_args_is_invalid_args() {
        let mut grants = BTreeMap::new();
        grants.insert("echo".to_string(), grant(vec![]));
        let h = host(grants);
        assert!(matches!(
            h.fast_call("echo", "not json"),
            Err(ToolCallError::InvalidArgs(_))
        ));
    }

    #[tokio::test]
    async fn queue_async_resolves_envelope() {
        let mut grants = BTreeMap::new();
        grants.insert(
            "apull".to_string(),
            grant(vec![clause(&[("repo", "brenn")])]),
        );
        let h = host(grants);
        let req = h
            .queue_async("apull", r#"{"repo":"brenn"}"#, "call-1")
            .expect("granted async call resolves");
        assert_eq!(req.channel, "brenn:tools/apull");
        assert_eq!(req.reply_to, "brenn:tool-results/sync");
        let body: serde_json::Value = serde_json::from_str(&req.body_json).unwrap();
        assert_eq!(body["v"], 1);
        assert_eq!(body["tool"], "apull");
        assert_eq!(body["call_id"], "call-1");
        assert_eq!(body["caller"], "wasm:sync");
        assert_eq!(body["args"]["repo"], "brenn");
    }

    #[tokio::test]
    async fn queue_async_on_fast_tool_is_wrong_class() {
        let mut grants = BTreeMap::new();
        grants.insert(
            "echo".to_string(),
            grant(vec![clause(&[("repo", "brenn")])]),
        );
        let h = host(grants);
        assert_eq!(
            h.queue_async("echo", r#"{"repo":"brenn"}"#, "c"),
            Err(ToolCallError::WrongClass)
        );
    }

    #[tokio::test]
    async fn queue_async_acl_miss_is_denied() {
        let mut grants = BTreeMap::new();
        grants.insert(
            "apull".to_string(),
            grant(vec![clause(&[("repo", "brenn")])]),
        );
        let h = host(grants);
        assert_eq!(
            h.queue_async("apull", r#"{"repo":"graf"}"#, "c"),
            Err(ToolCallError::Denied("graf".to_string()))
        );
    }

    #[tokio::test]
    async fn queue_async_ungranted_is_not_granted() {
        let h = host(BTreeMap::new());
        assert_eq!(
            h.queue_async("apull", r#"{"repo":"brenn"}"#, "c"),
            Err(ToolCallError::NotGranted)
        );
    }

    #[tokio::test]
    async fn queue_async_malformed_args_is_invalid_args() {
        let mut grants = BTreeMap::new();
        grants.insert("apull".to_string(), grant(vec![]));
        let h = host(grants);
        assert!(matches!(
            h.queue_async("apull", "}{", "c"),
            Err(ToolCallError::InvalidArgs(_))
        ));
    }
}
