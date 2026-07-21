//! The tool registry: the built-once, immutable table of registered tools plus
//! the shared rate limiter and per-tool concurrency semaphores.
//!
//! Built at bootstrap, then `validate_config` runs against resolved config
//! before serving. Registration and validation are both fail-fast (panic) — a
//! duplicate tool, an unsupported idempotency mode, an over-budget fast tool, or
//! a grant naming an unknown tool/ACL key are all operator/wiring bugs that must
//! stop startup, never degrade at runtime.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use brenn_lib::config::AppConfig;
use brenn_lib::messaging::ParticipantId;
use brenn_lib::obs::alerting::{AlertDispatcher, AlertSeverity};
use brenn_lib::tools::{ResolvedRateLimit, ResolvedToolGrant};
use indexmap::IndexMap;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::{info, warn};

use super::descriptor::{Idempotency, MAX_FAST_BUDGET, ToolClass, ToolError};
use super::rate_limit::RateLimiter;
use super::tool::{RegisteredTool, ToolCtx};

/// The immutable tool table. Lookup is by canonical name or MCP name; async
/// tools additionally carry a global concurrency semaphore.
pub struct ToolRegistry {
    /// Registered tools keyed by canonical name.
    tools: HashMap<&'static str, RegisteredTool>,
    /// Reverse index from MCP name to canonical name (the LLM adapter's entry).
    by_mcp_name: HashMap<&'static str, &'static str>,
    /// Per-async-tool global concurrency bound; `execute` acquires a permit on
    /// every path (executor and LLM).
    semaphores: HashMap<&'static str, Arc<Semaphore>>,
    /// Shared per-`(participant, tool)` rate limiter.
    rate_limiter: RateLimiter,
}

impl ToolRegistry {
    /// Build the registry from the given tools.
    ///
    /// # Panics
    ///
    /// Panics (fail-fast at startup) on: a duplicate canonical name or MCP
    /// name; a `RequiresKey` tool (dedupe table not built this cycle — see
    /// `TODO(tool-registry-idempotency-dedupe)`); a fast tool whose declared
    /// budget exceeds `MAX_FAST_BUDGET`; or an async tool with
    /// `max_concurrency == 0`.
    pub fn new(tools: Vec<RegisteredTool>) -> Self {
        let mut map: HashMap<&'static str, RegisteredTool> = HashMap::new();
        let mut by_mcp_name: HashMap<&'static str, &'static str> = HashMap::new();
        let mut semaphores: HashMap<&'static str, Arc<Semaphore>> = HashMap::new();

        for tool in tools {
            let desc = tool.descriptor();
            // Copy the static fields out before moving `tool` into the map.
            let name = desc.name;
            let mcp_name = desc.mcp_name;
            let class = desc.class;
            let idempotency = desc.idempotency;

            assert!(
                matches!(idempotency, Idempotency::Natural),
                "tool {name:?}: RequiresKey idempotency is not supported this cycle \
                 (dedupe table deferred; see TODO(tool-registry-idempotency-dedupe))",
            );
            // The `RegisteredTool` variant and the descriptor's class must agree.
            // Without this check a `Fast` tool declaring an `Async` class (or
            // vice versa) passes registration — even receives a semaphore — and
            // only panics when first invoked, at request time. Fail at startup.
            assert!(
                matches!(
                    (&tool, class),
                    (RegisteredTool::Fast(_), ToolClass::Fast { .. })
                        | (RegisteredTool::Async(_), ToolClass::Async { .. })
                ),
                "tool {name:?}: RegisteredTool variant disagrees with descriptor class {class:?}",
            );
            match class {
                ToolClass::Fast { budget } => {
                    assert!(
                        budget <= MAX_FAST_BUDGET,
                        "tool {name:?}: declared fast budget {budget:?} exceeds the {MAX_FAST_BUDGET:?} cap \
                         (a tool wanting more is not fast)",
                    );
                }
                ToolClass::Async { max_concurrency } => {
                    assert!(
                        max_concurrency >= 1,
                        "tool {name:?}: async max_concurrency must be >= 1",
                    );
                    semaphores.insert(name, Arc::new(Semaphore::new(max_concurrency)));
                }
            }

            assert!(
                by_mcp_name.insert(mcp_name, name).is_none(),
                "duplicate tool mcp_name: {mcp_name:?}",
            );
            assert!(
                map.insert(name, tool).is_none(),
                "duplicate tool name: {name:?}",
            );
        }

        Self {
            tools: map,
            by_mcp_name,
            semaphores,
            rate_limiter: RateLimiter::default(),
        }
    }

    /// Look up a tool by its canonical name.
    pub fn get(&self, name: &str) -> Option<&RegisteredTool> {
        self.tools.get(name)
    }

    /// Look up a tool by its MCP name (the LLM adapter's entry point).
    pub fn get_by_mcp_name(&self, mcp_name: &str) -> Option<&RegisteredTool> {
        self.by_mcp_name
            .get(mcp_name)
            .and_then(|name| self.tools.get(name))
    }

    /// The global concurrency semaphore for an async tool, if registered.
    pub fn concurrency(&self, name: &str) -> Option<&Arc<Semaphore>> {
        self.semaphores.get(name)
    }

    /// Canonical names of every registered async-class tool. The tool-bus
    /// bootstrap creates one `brenn:tools/<name>` request channel per entry.
    pub fn async_tool_names(&self) -> Vec<&'static str> {
        self.tools
            .values()
            .filter_map(|t| match t {
                RegisteredTool::Async(a) => Some(a.descriptor().name),
                RegisteredTool::Fast(_) => None,
            })
            .collect()
    }

    /// The shared rate limiter.
    pub fn rate_limiter(&self) -> &RateLimiter {
        &self.rate_limiter
    }

    /// Validate every app's resolved tool grants against the registry.
    ///
    /// WASM-consumer grants live on their resolved policy, not in this `apps`
    /// map, so they are validated separately (via [`ToolRegistry::validate_grants`])
    /// at the component-load site where the resolved consumers are in scope.
    ///
    /// # Panics
    ///
    /// Panics on any grant that names an unknown tool, uses an ACL key the tool
    /// does not declare, or omits ACL clauses on a tool that takes an ACL.
    pub fn validate_config(&self, apps: &IndexMap<String, AppConfig>) {
        for (slug, app) in apps.iter() {
            self.validate_grants(&format!("app {slug:?}"), &app.policy.tool_grants);
        }
    }

    /// Validate one participant's resolved grants against the registry.
    ///
    /// # Panics
    ///
    /// See [`ToolRegistry::validate_config`].
    pub fn validate_grants(
        &self,
        owner: &str,
        grants: &std::collections::BTreeMap<String, ResolvedToolGrant>,
    ) {
        for (tool_name, grant) in grants {
            let registered = self
                .tools
                .get(tool_name.as_str())
                .unwrap_or_else(|| panic!("{owner}: tool_grant names unknown tool {tool_name:?}"));
            let desc = registered.descriptor();
            if !desc.acl_keys.is_empty() {
                assert!(
                    !grant.acl.is_empty(),
                    "{owner}: tool_grant for {tool_name:?} must supply at least one ACL clause",
                );
            }
            for clause in &grant.acl {
                for key in clause.keys() {
                    assert!(
                        desc.acl_keys.contains(&key),
                        "{owner}: tool_grant for {tool_name:?} uses unknown ACL key {key:?} \
                         (tool declares {:?})",
                        desc.acl_keys,
                    );
                }
            }
        }
    }

    /// Run a fast-class tool synchronously: resolve the tool by canonical name,
    /// confirm it is fast, ACL-check `args` against the grant, take a rate token
    /// (empty bucket → immediate [`ToolError::RateLimited`] — a sync call cannot
    /// wait), then time `execute` and alert on a budget overrun (a tool bug; the
    /// call still returns its result).
    ///
    /// Shared by the LLM adapter (`invoke_for_llm`'s fast arm) and the WASM
    /// `ToolHost` fast path — one guard, both caller kinds. Does not emit the
    /// per-invocation info log; the caller owns that.
    ///
    /// An unknown name → `NotGranted` (oracle closure); a name resolving to an
    /// async tool → `WrongClass`.
    pub fn invoke_fast(
        &self,
        name: &str,
        ctx: &ToolCtx,
        args: serde_json::Value,
        alert: &AlertDispatcher,
    ) -> Result<serde_json::Value, ToolError> {
        let tool = self.get(name).ok_or(ToolError::NotGranted)?;
        let t = match tool {
            RegisteredTool::Fast(t) => t,
            RegisteredTool::Async(_) => return Err(ToolError::WrongClass),
        };
        let budget = match t.descriptor().class {
            ToolClass::Fast { budget } => budget,
            ToolClass::Async { .. } => unreachable!("fast registered tool has async class"),
        };
        t.check_acl(&args, &ctx.grant.acl)?;
        if !self
            .rate_limiter
            .try_take(ctx.caller.as_str(), name, ctx.grant.rate_limit)
        {
            return Err(ToolError::RateLimited);
        }
        let started = Instant::now();
        let result = t.execute(ctx, args);
        let elapsed = started.elapsed();
        if elapsed > budget {
            warn!(
                tool = %name, ?elapsed, ?budget,
                "fast tool exceeded its declared budget (tool bug)"
            );
            alert.alert_once_per_process(
                AlertSeverity::Warning,
                format!("Fast tool {name:?} exceeded its budget"),
                &format!("tool:{name}:fast-budget-exceeded"),
                format!(
                    "Fast tool {name:?} took {elapsed:?} (budget {budget:?}). \
                     A fast tool must be effectively non-blocking; this is a tool bug."
                ),
            );
        }
        // The unified invocation line (design §3.7: tool, caller, class, duration,
        // outcome) lives here, on the one path every fast call — LLM and WASM —
        // runs through, so a failed fast call is logged with its duration too.
        info!(
            tool = %name,
            caller = %ctx.caller.as_str(),
            class = "fast",
            ?elapsed,
            outcome = super::descriptor::outcome_label(&result),
            "tool invocation",
        );
        result
    }

    /// Admit an async invocation: the shared guard both caller kinds run through
    /// (the bus executor and `invoke_for_llm`). Reserve a rate token and wait for
    /// it to accrue (delay-not-drop; a sustained wait fires the throttle alert),
    /// then acquire the tool's global concurrency permit. The returned permit must
    /// be held across `execute` so LLM- and bus-originated executions are bounded
    /// together.
    ///
    /// # Panics
    ///
    /// Panics if `name` has no concurrency semaphore — every async tool is
    /// registered with one, so a missing semaphore is an impossible state and a
    /// wiring bug that must crash.
    pub async fn admit_async(
        &self,
        caller: &str,
        name: &str,
        rate_limit: Option<ResolvedRateLimit>,
        alert: &AlertDispatcher,
    ) -> OwnedSemaphorePermit {
        let wait = self.rate_limiter.reserve(caller, name, rate_limit);
        if !wait.is_zero() {
            alert.alert_once_per_process(
                AlertSeverity::Warning,
                format!("Async tool {name:?} is being rate-throttled"),
                &format!("tool:{name}:{caller}:rate_throttled"),
                format!(
                    "Async tool {name:?} calls from {caller} are being delayed by the rate \
                     limiter (waited {wait:?}). Sustained throttling indicates the caller is \
                     exceeding its configured tool budget."
                ),
            );
            tokio::time::sleep(wait).await;
        }
        self.semaphores
            .get(name)
            .unwrap_or_else(|| panic!("BUG: async tool {name:?} has no concurrency semaphore"))
            .clone()
            .acquire_owned()
            .await
            .expect("tool concurrency semaphore never closed")
    }

    /// Invoke a tool directly for the LLM path (the PostToolUse adapter).
    ///
    /// This is the hook-driven request/response bridge: async tools are awaited
    /// in place through the same rate-limit bucket, ACL check, and per-tool
    /// concurrency semaphore the bus executor uses — not round-tripped through
    /// the bus. One tool object, one grant table, one guard, both caller kinds.
    ///
    /// The caller must already have confirmed the grant (the adapter checks it
    /// at PreToolUse and re-reads it here); `grant` is that resolved grant.
    /// Returns the tool's raw output value or a typed [`ToolError`].
    pub async fn invoke_for_llm(
        &self,
        mcp_name: &str,
        caller: ParticipantId,
        grant: ResolvedToolGrant,
        acting_conversation_id: Option<i64>,
        args: serde_json::Value,
        alert: &AlertDispatcher,
    ) -> Result<serde_json::Value, ToolError> {
        // Unknown tool is indistinguishable from ungranted (oracle closure).
        let tool = self
            .get_by_mcp_name(mcp_name)
            .ok_or(ToolError::NotGranted)?
            .clone();
        let name = tool.descriptor().name;
        let rate_limit = grant.rate_limit;
        let ctx = ToolCtx {
            caller,
            grant: grant.clone(),
            acting_conversation_id,
        };

        let invoke_started = Instant::now();

        let result = match tool {
            // Fast class: the shared sync guard (ACL, immediate rate check, budget
            // timing/alert). A sync call cannot wait, so an empty bucket errors.
            // `invoke_fast` emits the invocation log line itself.
            RegisteredTool::Fast(_) => return self.invoke_fast(name, &ctx, args, alert),
            RegisteredTool::Async(t) => {
                if let Err(denied) = t.check_acl(&args, &grant.acl) {
                    Err(denied.into())
                } else {
                    // Async class: delay-not-drop rate admission then the tool's
                    // global concurrency permit — the shared guard. The 300s
                    // PostToolUse timeout is the ceiling. Held across `execute`.
                    let _permit = self
                        .admit_async(ctx.caller.as_str(), name, rate_limit, alert)
                        .await;
                    t.execute(&ctx, args).await
                }
            }
        };

        info!(
            tool = %name,
            caller = %ctx.caller.as_str(),
            class = "async",
            elapsed = ?invoke_started.elapsed(),
            outcome = super::descriptor::outcome_label(&result),
            "tool invocation",
        );
        result
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::time::Duration;

    use brenn_lib::obs::alerting::make_capturing_alerter;
    use brenn_lib::tools::{AclClause, ResolvedRateLimit, ResolvedToolGrant};
    use serde_json::json;

    use super::*;
    use crate::tool_registry::descriptor::{
        AclDenied, DEFAULT_FAST_BUDGET, ToolDescriptor, ToolError,
    };
    use crate::tool_registry::tool::{AsyncTool, FastTool, ToolCtx};

    /// A minimal fast tool for registry tests. Its descriptor is fully
    /// configurable so each panic path can be provoked.
    struct StubTool {
        descriptor: ToolDescriptor,
    }

    impl FastTool for StubTool {
        fn descriptor(&self) -> &ToolDescriptor {
            &self.descriptor
        }
        fn check_acl(
            &self,
            _args: &serde_json::Value,
            _acl: &[AclClause],
        ) -> Result<(), AclDenied> {
            Ok(())
        }
        fn execute(
            &self,
            _ctx: &ToolCtx,
            _args: serde_json::Value,
        ) -> Result<serde_json::Value, ToolError> {
            Ok(json!({}))
        }
    }

    fn descriptor(
        name: &'static str,
        mcp_name: &'static str,
        class: ToolClass,
        acl_keys: &'static [&'static str],
        idempotency: Idempotency,
    ) -> ToolDescriptor {
        ToolDescriptor {
            name,
            mcp_name,
            description: "stub",
            input_schema: json!({ "type": "object" }),
            class,
            acl_keys,
            idempotency,
            auto_approve: true,
        }
    }

    fn fast(
        name: &'static str,
        mcp_name: &'static str,
        acl_keys: &'static [&'static str],
    ) -> RegisteredTool {
        RegisteredTool::Fast(Arc::new(StubTool {
            descriptor: descriptor(
                name,
                mcp_name,
                ToolClass::Fast {
                    budget: DEFAULT_FAST_BUDGET,
                },
                acl_keys,
                Idempotency::Natural,
            ),
        }))
    }

    use crate::tool_registry::testutil::{clause, grant};

    #[test]
    fn lookup_by_name_and_mcp_name() {
        let reg = ToolRegistry::new(vec![fast("stub", "mcp__brenn__Stub", &["repo"])]);
        assert!(reg.get("stub").is_some());
        assert!(reg.get_by_mcp_name("mcp__brenn__Stub").is_some());
        assert!(reg.get("missing").is_none());
        assert!(reg.get_by_mcp_name("mcp__brenn__Missing").is_none());
    }

    #[test]
    fn async_tool_gets_a_concurrency_semaphore() {
        struct AsyncStub(ToolDescriptor);
        #[async_trait::async_trait]
        impl crate::tool_registry::tool::AsyncTool for AsyncStub {
            fn descriptor(&self) -> &ToolDescriptor {
                &self.0
            }
            fn check_acl(
                &self,
                _args: &serde_json::Value,
                _acl: &[AclClause],
            ) -> Result<(), AclDenied> {
                Ok(())
            }
            async fn execute(
                &self,
                _ctx: &ToolCtx,
                _args: serde_json::Value,
            ) -> Result<serde_json::Value, ToolError> {
                Ok(json!({}))
            }
        }
        let reg = ToolRegistry::new(vec![RegisteredTool::Async(Arc::new(AsyncStub(
            descriptor(
                "apull",
                "mcp__brenn__APull",
                ToolClass::Async { max_concurrency: 4 },
                &["repo"],
                Idempotency::Natural,
            ),
        )))]);
        let sem = reg.concurrency("apull").expect("async semaphore present");
        assert_eq!(sem.available_permits(), 4);
        // Fast tools carry no semaphore.
        assert!(reg.concurrency("stub").is_none());
    }

    #[test]
    #[should_panic(expected = "duplicate tool name")]
    fn duplicate_name_panics() {
        ToolRegistry::new(vec![
            fast("stub", "mcp__brenn__StubA", &["repo"]),
            fast("stub", "mcp__brenn__StubB", &["repo"]),
        ]);
    }

    #[test]
    #[should_panic(expected = "duplicate tool mcp_name")]
    fn duplicate_mcp_name_panics() {
        ToolRegistry::new(vec![
            fast("stub-a", "mcp__brenn__Stub", &["repo"]),
            fast("stub-b", "mcp__brenn__Stub", &["repo"]),
        ]);
    }

    #[test]
    #[should_panic(expected = "TODO(tool-registry-idempotency-dedupe)")]
    fn requires_key_registration_panics() {
        let tool = RegisteredTool::Fast(Arc::new(StubTool {
            descriptor: descriptor(
                "keyed",
                "mcp__brenn__Keyed",
                ToolClass::Fast {
                    budget: DEFAULT_FAST_BUDGET,
                },
                &[],
                Idempotency::RequiresKey,
            ),
        }));
        ToolRegistry::new(vec![tool]);
    }

    #[test]
    #[should_panic(expected = "variant disagrees with descriptor class")]
    fn variant_class_disagreement_panics() {
        // A `RegisteredTool::Fast` wrapping a descriptor with an `Async` class
        // must fail at registration, not at first invocation.
        let tool = RegisteredTool::Fast(Arc::new(StubTool {
            descriptor: descriptor(
                "mismatch",
                "mcp__brenn__Mismatch",
                ToolClass::Async { max_concurrency: 2 },
                &[],
                Idempotency::Natural,
            ),
        }));
        ToolRegistry::new(vec![tool]);
    }

    #[test]
    #[should_panic(expected = "exceeds the")]
    fn over_budget_fast_tool_panics() {
        let tool = RegisteredTool::Fast(Arc::new(StubTool {
            descriptor: descriptor(
                "slow",
                "mcp__brenn__Slow",
                ToolClass::Fast {
                    budget: std::time::Duration::from_millis(51),
                },
                &[],
                Idempotency::Natural,
            ),
        }));
        ToolRegistry::new(vec![tool]);
    }

    #[test]
    fn validate_grants_accepts_known_tool_and_keys() {
        let reg = ToolRegistry::new(vec![fast("stub", "mcp__brenn__Stub", &["repo"])]);
        let mut grants = BTreeMap::new();
        grants.insert(
            "stub".to_string(),
            grant(vec![clause(&[("repo", "brenn")])]),
        );
        reg.validate_grants("app \"pfin\"", &grants);
    }

    #[test]
    #[should_panic(expected = "unknown tool")]
    fn validate_grants_rejects_unknown_tool() {
        let reg = ToolRegistry::new(vec![fast("stub", "mcp__brenn__Stub", &["repo"])]);
        let mut grants = BTreeMap::new();
        grants.insert(
            "nope".to_string(),
            grant(vec![clause(&[("repo", "brenn")])]),
        );
        reg.validate_grants("app \"pfin\"", &grants);
    }

    #[test]
    #[should_panic(expected = "unknown ACL key")]
    fn validate_grants_rejects_unknown_acl_key() {
        let reg = ToolRegistry::new(vec![fast("stub", "mcp__brenn__Stub", &["repo"])]);
        let mut grants = BTreeMap::new();
        grants.insert(
            "stub".to_string(),
            grant(vec![clause(&[("branch", "main")])]),
        );
        reg.validate_grants("app \"pfin\"", &grants);
    }

    #[test]
    #[should_panic(expected = "at least one ACL clause")]
    fn validate_grants_rejects_aclless_grant_on_acld_tool() {
        let reg = ToolRegistry::new(vec![fast("stub", "mcp__brenn__Stub", &["repo"])]);
        let mut grants = BTreeMap::new();
        grants.insert("stub".to_string(), grant(vec![]));
        reg.validate_grants("app \"pfin\"", &grants);
    }

    #[test]
    fn validate_grants_allows_aclless_grant_on_no_acl_tool() {
        // A tool with no ACL keys accepts a clause-less grant.
        let reg = ToolRegistry::new(vec![fast("noacl", "mcp__brenn__NoAcl", &[])]);
        let mut grants = BTreeMap::new();
        grants.insert("noacl".to_string(), grant(vec![]));
        reg.validate_grants("app \"pfin\"", &grants);
    }

    /// A fast tool whose `execute` blocks for a configured duration, so a test
    /// can drive the budget-overrun alert on the fast path.
    struct SlowFastTool {
        descriptor: ToolDescriptor,
        sleep: Duration,
    }

    impl FastTool for SlowFastTool {
        fn descriptor(&self) -> &ToolDescriptor {
            &self.descriptor
        }
        fn check_acl(
            &self,
            _args: &serde_json::Value,
            _acl: &[AclClause],
        ) -> Result<(), AclDenied> {
            Ok(())
        }
        fn execute(
            &self,
            _ctx: &ToolCtx,
            _args: serde_json::Value,
        ) -> Result<serde_json::Value, ToolError> {
            std::thread::sleep(self.sleep);
            Ok(json!({ "ok": true }))
        }
    }

    /// A trivial async tool whose `execute` returns immediately — enough to
    /// exercise the async admission path (rate-limit reserve, throttle alert,
    /// concurrency permit) in `invoke_for_llm`.
    struct AsyncStubTool {
        descriptor: ToolDescriptor,
    }

    #[async_trait::async_trait]
    impl AsyncTool for AsyncStubTool {
        fn descriptor(&self) -> &ToolDescriptor {
            &self.descriptor
        }
        fn check_acl(
            &self,
            _args: &serde_json::Value,
            _acl: &[AclClause],
        ) -> Result<(), AclDenied> {
            Ok(())
        }
        async fn execute(
            &self,
            _ctx: &ToolCtx,
            _args: serde_json::Value,
        ) -> Result<serde_json::Value, ToolError> {
            Ok(json!({ "ok": true }))
        }
    }

    fn caller() -> ParticipantId {
        ParticipantId::for_app("adapt", "srv")
    }

    fn rate_limit(burst: u32, per_min: u32) -> Option<ResolvedRateLimit> {
        Some(ResolvedRateLimit {
            burst,
            sustained_per_minute: per_min,
        })
    }

    #[tokio::test]
    async fn invoke_for_llm_fast_rate_limited_on_empty_bucket() {
        // The fast path's rate-limit wiring: a single-token bucket admits the
        // first call and immediately rejects the second (a sync call cannot wait).
        let reg = ToolRegistry::new(vec![fast("fastrl", "mcp__brenn__FastRl", &[])]);
        let (alert, _h) = brenn_lib::obs::alerting::noop_alert_dispatcher();
        let g = ResolvedToolGrant {
            acl: vec![],
            rate_limit: rate_limit(1, 60),
        };
        let first = reg
            .invoke_for_llm(
                "mcp__brenn__FastRl",
                caller(),
                g.clone(),
                None,
                json!({}),
                &alert,
            )
            .await;
        assert!(first.is_ok(), "first fast call should pass: {first:?}");
        let second = reg
            .invoke_for_llm("mcp__brenn__FastRl", caller(), g, None, json!({}), &alert)
            .await;
        assert!(
            matches!(second, Err(ToolError::RateLimited)),
            "empty bucket must yield RateLimited, got {second:?}",
        );
    }

    #[tokio::test]
    async fn invoke_for_llm_fast_budget_overrun_alerts_but_returns_ok() {
        // A fast tool overrunning its budget is a tool bug: the call still
        // succeeds, but the fast-budget-exceeded alert fires.
        let tool = RegisteredTool::Fast(Arc::new(SlowFastTool {
            descriptor: descriptor(
                "slowfast",
                "mcp__brenn__SlowFast",
                ToolClass::Fast {
                    budget: DEFAULT_FAST_BUDGET,
                },
                &[],
                Idempotency::Natural,
            ),
            sleep: DEFAULT_FAST_BUDGET + Duration::from_millis(20),
        }));
        let reg = ToolRegistry::new(vec![tool]);
        let (alert, captured, handle) = make_capturing_alerter();
        let g = ResolvedToolGrant {
            acl: vec![],
            rate_limit: None,
        };
        let result = reg
            .invoke_for_llm("mcp__brenn__SlowFast", caller(), g, None, json!({}), &alert)
            .await;
        assert!(
            result.is_ok(),
            "over-budget fast call still returns Ok: {result:?}"
        );
        alert.flush().await;
        drop(alert);
        handle.await.unwrap();
        let captured = captured.lock().unwrap();
        assert!(
            captured
                .iter()
                .any(|(title, _)| title.contains("exceeded its budget")),
            "budget-overrun alert should fire: {captured:?}",
        );
    }

    #[tokio::test]
    async fn invoke_for_llm_async_throttles_and_alerts() {
        // The async path's delay-not-drop wiring: the second call over a
        // single-token bucket is delayed until a token accrues, and a sustained
        // throttle fires the rate-throttle alert. High refill keeps the delay
        // small so the test stays fast.
        let tool = RegisteredTool::Async(Arc::new(AsyncStubTool {
            descriptor: descriptor(
                "athrottle",
                "mcp__brenn__AThrottle",
                ToolClass::Async { max_concurrency: 4 },
                &[],
                Idempotency::Natural,
            ),
        }));
        let reg = ToolRegistry::new(vec![tool]);
        let (alert, captured, handle) = make_capturing_alerter();
        let g = ResolvedToolGrant {
            acl: vec![],
            rate_limit: rate_limit(1, 6000),
        };
        reg.invoke_for_llm(
            "mcp__brenn__AThrottle",
            caller(),
            g.clone(),
            None,
            json!({}),
            &alert,
        )
        .await
        .unwrap();
        let started = Instant::now();
        reg.invoke_for_llm(
            "mcp__brenn__AThrottle",
            caller(),
            g,
            None,
            json!({}),
            &alert,
        )
        .await
        .unwrap();
        let elapsed = started.elapsed();
        assert!(
            elapsed >= Duration::from_millis(5),
            "second async call should be delayed by the rate limiter, waited {elapsed:?}",
        );
        alert.flush().await;
        drop(alert);
        handle.await.unwrap();
        let captured = captured.lock().unwrap();
        assert!(
            captured
                .iter()
                .any(|(title, _)| title.contains("rate-throttled")),
            "sustained throttle should fire the rate-throttle alert: {captured:?}",
        );
    }
}
