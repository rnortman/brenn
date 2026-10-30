//! The tool registry: the built-once, immutable table of registered tools plus
//! the shared rate limiter and per-tool concurrency semaphores.
//!
//! Built at bootstrap, then `validate_config` runs against resolved config
//! before serving. Registration and validation are both fail-fast (panic) — a
//! duplicate tool, an unsupported idempotency mode, an over-budget fast tool, or
//! a grant naming an unknown tool/ACL key are all operator/wiring bugs that must
//! stop startup, never degrade at runtime.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use brenn_lib::config::AppConfig;
use brenn_lib::messaging::ParticipantId;
use brenn_lib::tools::{ResolvedRateLimit, ResolvedToolGrant};
use brenn_obs::alerting::{AlertDispatcher, AlertSeverity};
use indexmap::IndexMap;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::{info, warn};

use super::descriptor::{
    Idempotency, MAX_FAST_BUDGET, PROVIDED_TOOL_CEILING, ToolClass, ToolError, UserBinding,
};
use super::provided::ProvidedTool;
use super::rate_limit::RateLimiter;
use super::tool::{ActingUser, AsyncTool, RegisteredTool, ToolCtx};

/// The `internal` detail of a provided tool's call whose ceiling passed while
/// it waited for admission ([`ToolRegistry::admit_async_by`]).
pub const PROVIDED_ADMISSION_CEILING_DETAIL: &str = "provider ceiling reached before admission";

/// The participant whose grants are being validated, carrying the facts the
/// user-binding refusals depend on.
#[derive(Debug, Clone, Copy)]
pub enum GrantOwner<'a> {
    /// An LLM app. Its conversations' calls carry the conversation owner as
    /// acting user.
    App {
        slug: &'a str,
        /// Shared conversations: the typist need not be the owner.
        multiuser: bool,
        /// The agent runs in a container rather than as Brenn's uid.
        has_container: bool,
    },
    /// A WASM consumer. Its calls carry no acting user unless the executor
    /// stamps one from a pending provided-tool call it serves.
    WasmConsumer { slug: &'a str },
}

impl<'a> GrantOwner<'a> {
    /// The owner of an app's grants, read from its resolved config.
    pub fn for_app(slug: &'a str, app: &AppConfig) -> Self {
        GrantOwner::App {
            slug,
            multiuser: app.multiuser,
            has_container: app.container_spawn.is_some(),
        }
    }
}

impl std::fmt::Display for GrantOwner<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GrantOwner::App { slug, .. } => write!(f, "app {slug:?}"),
            GrantOwner::WasmConsumer { slug } => write!(f, "wasm consumer {slug:?}"),
        }
    }
}

/// Why `owner` may not hold a grant for a tool with `binding`, or `None` when
/// it may.
fn user_binding_refusal(owner: &GrantOwner<'_>, binding: UserBinding) -> Option<&'static str> {
    match (binding, owner) {
        (UserBinding::None, _) => None,
        (
            UserBinding::ActingUser | UserBinding::ActingOrLentUser,
            GrantOwner::App {
                multiuser: true, ..
            },
        ) => Some(
            "the tool acts as a user and the app is multiuser (a shared conversation's \
             typist need not be its owner)",
        ),
        (
            UserBinding::ActingUser | UserBinding::ActingOrLentUser,
            GrantOwner::App {
                has_container: false,
                ..
            },
        ) => Some(
            "the tool acts as a user and the app has no container (a bare agent runs as \
             Brenn's uid and could read the user's credentials)",
        ),
        (UserBinding::ActingUser | UserBinding::ActingOrLentUser, GrantOwner::App { .. }) => None,
        (UserBinding::ActingUser, GrantOwner::WasmConsumer { .. }) => {
            Some("the tool acts as the acting user and a wasm consumer's calls carry none")
        }
        (UserBinding::ActingOrLentUser, GrantOwner::WasmConsumer { .. }) => None,
    }
}

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
    /// Canonical names of the tools a WASM instance provides. Their calls carry
    /// a deadline from admission on.
    provided: HashSet<&'static str>,
    /// Shared per-`(participant, tool)` rate limiter.
    rate_limiter: RateLimiter,
}

impl ToolRegistry {
    /// Build the registry from the given tools.
    ///
    /// # Panics
    ///
    /// Panics (fail-fast at startup) on: a duplicate canonical name or MCP
    /// name; a `RequiresKey` tool (dedupe table not built this cycle, and it
    /// must cover replay from a re-created executor position as well as a
    /// retrying caller — see `TODO(tool-registry-idempotency-dedupe)`); a fast
    /// tool whose declared budget exceeds `MAX_FAST_BUDGET`; or an async tool
    /// with `max_concurrency == 0`.
    pub fn new(tools: Vec<RegisteredTool>) -> Self {
        Self::with_provided(tools, Vec::new())
    }

    /// Build the registry from native tools and the tools WASM instances
    /// provide, natives first.
    ///
    /// # Panics
    ///
    /// As [`ToolRegistry::new`]; a provided tool whose name or MCP name is
    /// already taken panics naming both sources.
    pub fn with_provided(native: Vec<RegisteredTool>, provided: Vec<Arc<ProvidedTool>>) -> Self {
        // Who holds each name and each MCP name so far, so a provided tool's
        // collision names both sides. `None` is a native tool.
        let mut names: HashMap<&'static str, Option<&str>> = HashMap::new();
        let mut mcp_names: HashMap<&'static str, Option<&str>> = HashMap::new();
        for tool in &native {
            let desc = tool.descriptor();
            names.insert(desc.name, None);
            mcp_names.insert(desc.mcp_name, None);
        }
        let mut provided_names = HashSet::new();
        for tool in &provided {
            let desc = tool.descriptor();
            let slug = tool.instance();
            for (what, key, held) in [
                ("tool", desc.name, &mut names),
                ("tool mcp_name", desc.mcp_name, &mut mcp_names),
            ] {
                match held.insert(key, Some(slug)) {
                    None => {}
                    Some(None) => panic!(
                        "{what} {key:?} is provided by [[wasm_consumer]] {slug:?} and registered \
                         natively"
                    ),
                    Some(Some(other)) => panic!(
                        "{what} {key:?} is provided by [[wasm_consumer]] {other:?} and by {slug:?}"
                    ),
                }
            }
            provided_names.insert(desc.name);
        }

        let tools = native
            .into_iter()
            .chain(provided.into_iter().map(|t| RegisteredTool::Async(t as _)));
        let mut registry = Self::build(tools);
        registry.provided = provided_names;
        registry
    }

    fn build(tools: impl Iterator<Item = RegisteredTool>) -> Self {
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
            provided: HashSet::new(),
            rate_limiter: RateLimiter::default(),
        }
    }

    /// Whether `name` is a tool a WASM instance provides.
    pub fn is_provided(&self, name: &str) -> bool {
        self.provided.contains(name)
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
    /// does not declare, omits ACL clauses on a tool that takes an ACL, holds a
    /// clause the tool's `validate_clause` refuses, or grants a user-bound tool
    /// to an owner that may not hold it (see [`UserBinding`]).
    pub fn validate_config(&self, apps: &IndexMap<String, AppConfig>) {
        for (slug, app) in apps.iter() {
            self.validate_grants(GrantOwner::for_app(slug, app), &app.policy.tool_grants);
        }
    }

    /// Validate one participant's resolved grants against the registry.
    ///
    /// # Panics
    ///
    /// See [`ToolRegistry::validate_config`].
    pub fn validate_grants(
        &self,
        owner: GrantOwner<'_>,
        grants: &std::collections::BTreeMap<String, ResolvedToolGrant>,
    ) {
        for (tool_name, grant) in grants {
            let registered = self
                .tools
                .get(tool_name.as_str())
                .unwrap_or_else(|| panic!("{owner}: tool_grant names unknown tool {tool_name:?}"));
            let desc = registered.descriptor();
            if let Some(reason) = user_binding_refusal(&owner, desc.user_binding) {
                panic!("{owner}: tool_grant for {tool_name:?} is refused: {reason}");
            }
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
                if let Err(reason) = registered.validate_clause(clause) {
                    panic!(
                        "{owner}: tool_grant for {tool_name:?} has an invalid ACL clause: {reason}"
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
        // The unified invocation line — tool, caller, class, duration, outcome —
        // lives here, on the one path every fast call (LLM and WASM) runs
        // through, so a failed fast call is logged with its duration too.
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

    /// [`ToolRegistry::admit_async`] bounded by `deadline`, the admission rule
    /// of a provided tool's call: when the deadline passes first it logs and
    /// answers `Internal(`[`PROVIDED_ADMISSION_CEILING_DETAIL`]`)`, so the call
    /// is never handed to its provider. A cancelled admission keeps its
    /// rate-limit reservation (over-counting against the caller's own budget,
    /// never under) and gives up its place in the semaphore queue.
    pub async fn admit_async_by(
        &self,
        deadline: tokio::time::Instant,
        caller: &str,
        name: &str,
        rate_limit: Option<ResolvedRateLimit>,
        alert: &AlertDispatcher,
    ) -> Result<OwnedSemaphorePermit, ToolError> {
        match tokio::time::timeout_at(deadline, self.admit_async(caller, name, rate_limit, alert))
            .await
        {
            Ok(permit) => Ok(permit),
            Err(_elapsed) => {
                warn!(
                    tool = %name, %caller,
                    "provided tool call reached its ceiling waiting for admission; answered internal",
                );
                Err(ToolError::Internal(
                    PROVIDED_ADMISSION_CEILING_DETAIL.to_string(),
                ))
            }
        }
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
    /// `acting_user` is the conversation's owner, passed through to the tool.
    /// Returns the tool's raw output value or a typed [`ToolError`].
    #[allow(clippy::too_many_arguments)]
    pub async fn invoke_for_llm(
        &self,
        mcp_name: &str,
        caller: ParticipantId,
        grant: ResolvedToolGrant,
        acting_conversation_id: Option<i64>,
        acting_user: Option<ActingUser>,
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
        let mut ctx = ToolCtx {
            caller,
            grant: grant.clone(),
            acting_conversation_id,
            acting_user,
            deadline: None,
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
                } else if self.is_provided(name) {
                    // A provided tool: one deadline from here bounds admission
                    // and the provider's answer together, so a call parked
                    // behind a busy permit is answered inside the ceiling and
                    // never handed to its provider late.
                    let deadline = tokio::time::Instant::now() + PROVIDED_TOOL_CEILING;
                    ctx.deadline = Some(deadline);
                    match self
                        .admit_async_by(deadline, ctx.caller.as_str(), name, rate_limit, alert)
                        .await
                    {
                        Ok(_permit) => t.execute(&ctx, args).await,
                        Err(e) => Err(e),
                    }
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

    use brenn_lib::tools::{AclClause, ResolvedRateLimit, ResolvedToolGrant};
    use brenn_obs::alerting::make_capturing_alerter;
    use serde_json::json;

    use super::*;
    use crate::descriptor::{AclDenied, DEFAULT_FAST_BUDGET, ToolDescriptor, ToolError};
    use crate::tool::{AsyncTool, FastTool, ToolCtx};

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
            user_binding: UserBinding::None,
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

    use crate::testutil::{clause, grant};

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
        impl crate::tool::AsyncTool for AsyncStub {
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
        reg.validate_grants(app_owner(), &grants);
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
        reg.validate_grants(app_owner(), &grants);
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
        reg.validate_grants(app_owner(), &grants);
    }

    #[test]
    #[should_panic(expected = "at least one ACL clause")]
    fn validate_grants_rejects_aclless_grant_on_acld_tool() {
        let reg = ToolRegistry::new(vec![fast("stub", "mcp__brenn__Stub", &["repo"])]);
        let mut grants = BTreeMap::new();
        grants.insert("stub".to_string(), grant(vec![]));
        reg.validate_grants(app_owner(), &grants);
    }

    #[test]
    fn validate_grants_allows_aclless_grant_on_no_acl_tool() {
        // A tool with no ACL keys accepts a clause-less grant.
        let reg = ToolRegistry::new(vec![fast("noacl", "mcp__brenn__NoAcl", &[])]);
        let mut grants = BTreeMap::new();
        grants.insert("noacl".to_string(), grant(vec![]));
        reg.validate_grants(app_owner(), &grants);
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
        let (alert, _h) = brenn_obs::alerting::noop_alert_dispatcher();
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
                None,
                json!({}),
                &alert,
            )
            .await;
        assert!(first.is_ok(), "first fast call should pass: {first:?}");
        let second = reg
            .invoke_for_llm(
                "mcp__brenn__FastRl",
                caller(),
                g,
                None,
                None,
                json!({}),
                &alert,
            )
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
            .invoke_for_llm(
                "mcp__brenn__SlowFast",
                caller(),
                g,
                None,
                None,
                json!({}),
                &alert,
            )
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

    fn app_owner() -> GrantOwner<'static> {
        GrantOwner::App {
            slug: "pfin",
            multiuser: false,
            has_container: true,
        }
    }

    /// An async tool with a configurable user binding that refuses any clause
    /// whose `path` is `"bad"`, and returns the acting user it was handed.
    struct BoundTool {
        descriptor: ToolDescriptor,
    }

    #[async_trait::async_trait]
    impl AsyncTool for BoundTool {
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
        fn validate_clause(&self, clause: &AclClause) -> Result<(), String> {
            match clause.iter().find(|(k, _)| *k == "path") {
                Some((_, "bad")) => Err("path \"bad\" is not a template".to_string()),
                _ => Ok(()),
            }
        }
        async fn execute(
            &self,
            ctx: &ToolCtx,
            _args: serde_json::Value,
        ) -> Result<serde_json::Value, ToolError> {
            let user = ctx.require_acting_user(self.descriptor.name);
            Ok(json!({ "id": user.id, "username": user.username }))
        }
    }

    fn bound(name: &'static str, binding: UserBinding) -> RegisteredTool {
        let mcp_name: &'static str = Box::leak(format!("mcp__brenn__{name}").into_boxed_str());
        let mut d = descriptor(
            name,
            mcp_name,
            ToolClass::Async { max_concurrency: 2 },
            &["provider", "path"],
            Idempotency::Natural,
        );
        d.user_binding = binding;
        RegisteredTool::Async(Arc::new(BoundTool { descriptor: d }))
    }

    fn one_grant(tool: &str, pairs: &[(&str, &str)]) -> BTreeMap<String, ResolvedToolGrant> {
        BTreeMap::from([(tool.to_string(), grant(vec![clause(pairs)]))])
    }

    fn bound_registry() -> ToolRegistry {
        ToolRegistry::new(vec![
            bound("acting", UserBinding::ActingUser),
            bound("lent", UserBinding::ActingOrLentUser),
            fast("unbound", "mcp__brenn__Unbound", &["provider"]),
        ])
    }

    #[test]
    fn user_bound_tools_admitted_on_a_single_user_container_app() {
        let reg = bound_registry();
        for tool in ["acting", "lent", "unbound"] {
            reg.validate_grants(app_owner(), &one_grant(tool, &[("provider", "google")]));
        }
    }

    #[test]
    #[should_panic(
        expected = "app \"pfin\": tool_grant for \"acting\" is refused: the tool acts as a user and the app is multiuser"
    )]
    fn acting_user_tool_refused_on_multiuser_app() {
        let owner = GrantOwner::App {
            slug: "pfin",
            multiuser: true,
            has_container: true,
        };
        bound_registry().validate_grants(owner, &one_grant("acting", &[("provider", "google")]));
    }

    #[test]
    #[should_panic(
        expected = "tool_grant for \"lent\" is refused: the tool acts as a user and the app is multiuser"
    )]
    fn lent_user_tool_refused_on_multiuser_app() {
        let owner = GrantOwner::App {
            slug: "pfin",
            multiuser: true,
            has_container: true,
        };
        bound_registry().validate_grants(owner, &one_grant("lent", &[("provider", "google")]));
    }

    #[test]
    #[should_panic(
        expected = "tool_grant for \"lent\" is refused: the tool acts as a user and the app has no container"
    )]
    fn user_bound_tool_refused_on_bare_app() {
        let owner = GrantOwner::App {
            slug: "pfin",
            multiuser: false,
            has_container: false,
        };
        bound_registry().validate_grants(owner, &one_grant("lent", &[("provider", "google")]));
    }

    #[test]
    fn unbound_tool_admitted_on_multiuser_bare_app() {
        let owner = GrantOwner::App {
            slug: "pfin",
            multiuser: true,
            has_container: false,
        };
        bound_registry().validate_grants(owner, &one_grant("unbound", &[("provider", "google")]));
    }

    #[test]
    #[should_panic(
        expected = "wasm consumer \"sync\": tool_grant for \"acting\" is refused: the tool acts as the acting user and a wasm consumer's calls carry none"
    )]
    fn acting_user_tool_refused_on_wasm_consumer() {
        bound_registry().validate_grants(
            GrantOwner::WasmConsumer { slug: "sync" },
            &one_grant("acting", &[("provider", "google")]),
        );
    }

    #[test]
    fn lent_user_tool_admitted_on_wasm_consumer() {
        bound_registry().validate_grants(
            GrantOwner::WasmConsumer { slug: "sync" },
            &one_grant("lent", &[("provider", "google")]),
        );
    }

    #[test]
    #[should_panic(
        expected = "tool_grant for \"lent\" has an invalid ACL clause: path \"bad\" is not a template"
    )]
    fn validate_clause_refusal_stops_validation() {
        bound_registry().validate_grants(
            app_owner(),
            &one_grant("lent", &[("provider", "google"), ("path", "bad")]),
        );
    }

    #[test]
    fn validate_clause_default_admits_every_clause() {
        let reg = bound_registry();
        let unbound = reg.get("unbound").unwrap();
        assert_eq!(
            unbound.validate_clause(&clause(&[("provider", "bad")])),
            Ok(())
        );
    }

    #[test]
    fn grant_owner_for_app_reads_multiuser_and_container() {
        let mut app = brenn_lib::config::test_app_config("pfin");
        app.multiuser = true;
        let owner = GrantOwner::for_app("pfin", &app);
        assert!(matches!(
            owner,
            GrantOwner::App {
                slug: "pfin",
                multiuser: true,
                has_container: false,
            }
        ));
    }

    #[test]
    #[should_panic(expected = "app \"shared\": tool_grant for \"acting\" is refused")]
    fn validate_config_reads_each_app_as_its_grant_owner() {
        let mut solo = brenn_lib::config::test_app_config("solo");
        solo.container_spawn = None;
        std::sync::Arc::make_mut(&mut solo.policy).tool_grants =
            one_grant("unbound", &[("provider", "google")]);
        let mut shared = brenn_lib::config::test_app_config("shared");
        shared.multiuser = true;
        std::sync::Arc::make_mut(&mut shared.policy).tool_grants =
            one_grant("acting", &[("provider", "google")]);
        let apps = IndexMap::from([("solo".to_string(), solo), ("shared".to_string(), shared)]);
        bound_registry().validate_config(&apps);
    }

    #[tokio::test]
    async fn invoke_for_llm_passes_the_acting_user_through() {
        let reg = bound_registry();
        let (alert, _h) = brenn_obs::alerting::noop_alert_dispatcher();
        let user = ActingUser {
            id: 7,
            username: "alice".to_string(),
        };
        let out = reg
            .invoke_for_llm(
                "mcp__brenn__acting",
                caller(),
                grant(vec![clause(&[("provider", "google")])]),
                Some(3),
                Some(user),
                json!({}),
                &alert,
            )
            .await
            .expect("acting-user call succeeds");
        assert_eq!(out, json!({ "id": 7, "username": "alice" }));
    }

    #[tokio::test]
    #[should_panic(expected = "BUG: tool \"acting\" requires an acting user")]
    async fn acting_user_tool_without_acting_user_panics() {
        let reg = bound_registry();
        let (alert, _h) = brenn_obs::alerting::noop_alert_dispatcher();
        let _ = reg
            .invoke_for_llm(
                "mcp__brenn__acting",
                caller(),
                grant(vec![clause(&[("provider", "google")])]),
                None,
                None,
                json!({}),
                &alert,
            )
            .await;
    }

    // --- provided tools ---

    use crate::provided::tests::{PROVIDER, decl, messenger, published_id, requests};
    use crate::provided::{ProvidedTool, ProviderRuntime};

    fn provided(
        slug: &str,
        decl: &brenn_lib::messaging::config::ProvidedToolDecl,
    ) -> Arc<ProvidedTool> {
        let (alert, _drain) = brenn_obs::alerting::noop_alert_dispatcher();
        Arc::new(ProvidedTool::new(slug, decl, ProviderRuntime::new(alert)))
    }

    fn renamed(name: &str, mcp_name: &str) -> brenn_lib::messaging::config::ProvidedToolDecl {
        brenn_lib::messaging::config::ProvidedToolDecl {
            name: name.to_string(),
            mcp_name: mcp_name.to_string(),
            ..decl(1)
        }
    }

    /// Provided tools sit beside the native ones, async-class with their
    /// declared concurrency, and only they are marked provided.
    #[tokio::test]
    async fn provided_tools_register_beside_native_ones() {
        let reg = ToolRegistry::with_provided(
            vec![fast("stub", "mcp__brenn__Stub", &["repo"])],
            vec![provided(PROVIDER, &decl(2))],
        );
        assert!(reg.is_provided("echo-for-user"));
        assert!(!reg.is_provided("stub"));
        assert!(!reg.is_provided("absent"));
        assert!(matches!(
            reg.get_by_mcp_name("mcp__brenn__EchoForUser"),
            Some(RegisteredTool::Async(_))
        ));
        assert_eq!(
            reg.concurrency("echo-for-user")
                .unwrap()
                .available_permits(),
            2
        );
        assert_eq!(reg.async_tool_names(), vec!["echo-for-user"]);
        assert!(!ToolRegistry::new(vec![]).is_provided("echo-for-user"));
    }

    #[tokio::test]
    #[should_panic(
        expected = "tool \"stub\" is provided by [[wasm_consumer]] \"prov\" and registered natively"
    )]
    async fn a_provided_tool_taking_a_native_name_panics_naming_both() {
        ToolRegistry::with_provided(
            vec![fast("stub", "mcp__brenn__Stub", &[])],
            vec![provided(PROVIDER, &renamed("stub", "Other"))],
        );
    }

    #[tokio::test]
    #[should_panic(
        expected = "tool mcp_name \"mcp__brenn__Stub\" is provided by [[wasm_consumer]] \"prov\" \
                    and registered natively"
    )]
    async fn a_provided_tool_taking_a_native_mcp_name_panics_naming_both() {
        ToolRegistry::with_provided(
            vec![fast("stub", "mcp__brenn__Stub", &[])],
            vec![provided(PROVIDER, &renamed("other", "Stub"))],
        );
    }

    #[tokio::test]
    #[should_panic(
        expected = "tool \"echo-for-user\" is provided by [[wasm_consumer]] \"gmail\" and by \
                    \"calendar\""
    )]
    async fn two_providers_of_one_name_panic_naming_both() {
        ToolRegistry::with_provided(
            vec![],
            vec![
                provided("gmail", &decl(1)),
                provided("calendar", &renamed("echo-for-user", "Other")),
            ],
        );
    }

    #[tokio::test]
    #[should_panic(
        expected = "tool mcp_name \"mcp__brenn__EchoForUser\" is provided by [[wasm_consumer]] \
                    \"gmail\" and by \"calendar\""
    )]
    async fn two_providers_of_one_mcp_name_panic_naming_both() {
        ToolRegistry::with_provided(
            vec![],
            vec![
                provided("gmail", &decl(1)),
                provided("calendar", &renamed("other", "EchoForUser")),
            ],
        );
    }

    /// The LLM path stamps a provided call with a deadline, hands it to the
    /// provider and returns the provider's answer, holding the tool's permit
    /// for the whole wait.
    #[tokio::test]
    async fn invoke_for_llm_runs_a_provided_tool_to_its_answer() {
        let (alert, _drain) = brenn_obs::alerting::noop_alert_dispatcher();
        let runtime = ProviderRuntime::new(alert.clone());
        let messenger = messenger().await;
        runtime.attach(messenger.clone());
        let reg = Arc::new(ToolRegistry::with_provided(
            vec![],
            vec![Arc::new(ProvidedTool::new(
                PROVIDER,
                &decl(1),
                runtime.clone(),
            ))],
        ));

        let call = tokio::spawn({
            let reg = reg.clone();
            let alert = alert.clone();
            async move {
                reg.invoke_for_llm(
                    "mcp__brenn__EchoForUser",
                    caller(),
                    grant(vec![]),
                    Some(3),
                    None,
                    json!({ "q": "x" }),
                    &alert,
                )
                .await
            }
        });
        let id = published_id(&messenger).await;
        assert_eq!(
            reg.concurrency("echo-for-user")
                .unwrap()
                .available_permits(),
            0,
            "the permit is held across the provider's wait"
        );
        runtime
            .complete(PROVIDER, &id, Ok(json!({ "echoed": "x" })))
            .unwrap();
        assert_eq!(call.await.unwrap(), Ok(json!({ "echoed": "x" })));
        assert_eq!(
            reg.concurrency("echo-for-user")
                .unwrap()
                .available_permits(),
            1
        );
    }

    /// A call parked behind a busy permit is answered inside the ceiling and
    /// never handed to its provider: admission and the provider's wait share
    /// one deadline.
    #[tokio::test(start_paused = true)]
    async fn a_provided_call_parked_past_the_ceiling_is_answered_without_a_request() {
        let (alert, _drain) = brenn_obs::alerting::noop_alert_dispatcher();
        let runtime = ProviderRuntime::new(alert.clone());
        let messenger = messenger().await;
        runtime.attach(messenger.clone());
        let reg = ToolRegistry::with_provided(
            vec![],
            vec![Arc::new(ProvidedTool::new(PROVIDER, &decl(1), runtime))],
        );
        let _held = reg
            .concurrency("echo-for-user")
            .unwrap()
            .clone()
            .acquire_owned()
            .await
            .unwrap();

        let started = tokio::time::Instant::now();
        let out = reg
            .invoke_for_llm(
                "mcp__brenn__EchoForUser",
                caller(),
                grant(vec![]),
                Some(3),
                None,
                json!({}),
                &alert,
            )
            .await;
        assert_eq!(
            out,
            Err(ToolError::Internal(
                PROVIDED_ADMISSION_CEILING_DETAIL.to_string()
            ))
        );
        assert_eq!(started.elapsed(), PROVIDED_TOOL_CEILING);
        assert!(
            requests(&messenger).await.is_empty(),
            "nothing was published"
        );
    }

    /// `admit_async_by` gives up at its deadline and admits before it.
    #[tokio::test(start_paused = true)]
    async fn admit_async_by_is_bounded_by_its_deadline() {
        let (alert, _drain) = brenn_obs::alerting::noop_alert_dispatcher();
        let reg = ToolRegistry::with_provided(vec![], vec![provided(PROVIDER, &decl(1))]);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let first = reg
            .admit_async_by(deadline, "c", "echo-for-user", None, &alert)
            .await
            .expect("a free permit admits");
        assert!(matches!(
            reg.admit_async_by(deadline, "c", "echo-for-user", None, &alert)
                .await,
            Err(ToolError::Internal(detail)) if detail == PROVIDED_ADMISSION_CEILING_DETAIL
        ));
        assert_eq!(tokio::time::Instant::now(), deadline);
        drop(first);
        let later = tokio::time::Instant::now() + Duration::from_secs(5);
        assert!(
            reg.admit_async_by(later, "c", "echo-for-user", None, &alert)
                .await
                .is_ok()
        );
    }
}
