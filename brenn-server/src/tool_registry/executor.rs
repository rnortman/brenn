//! The `ToolExecutor`: the native `system:` subscriber that turns an async tool
//! call on the bus into an execution and a result activation.
//!
//! A WASM consumer's `call-async` becomes a durable message on
//! `brenn:tools/<tool>` carrying a `reply_to` of `brenn:tool-results/<slug>`
//! (Slice-B increments 5/6a/6b). The executor subscribes as the single
//! `system:tool-executor` principal, drains those requests, re-checks the
//! caller's grant + ACL against the current config, admits through the per-tool
//! rate limiter and concurrency semaphore, runs the tool, and publishes a v1
//! result envelope back to the caller's inbox through the gated
//! `publish_from_system` path — no ACL bypass.
//!
//! The drain loop is the shared [`SystemInbox`] park/wake shape: a startup
//! sweep then a `Notify`-driven loop, **ack-at-dequeue** (each loaded row is
//! marked delivered before any execution, at-most-once). A dequeued batch is
//! grouped by request channel (one channel per tool) and each group drains on
//! its own serialized task: for every request it acquires the tool's
//! rate-limit + concurrency admission *before* spawning the `execute` future,
//! so parked (awaiting-permit) work is bounded to one in-flight admission per
//! tool and a slow pull never blocks a different tool. Only the `execute`
//! future is spawned, holding the permit until it returns; per-tool
//! concurrency is bounded by the registry semaphore and per-resource
//! dogpiling by the tool's own guard.
//!
//! Bootstrap wires this subsystem in `bootstrap/` from the executor's
//! `SystemParticipantSpec` (`bus_wiring::tool_executor_spec`): the
//! `brenn:tools/<tool>` request channels and `brenn:tool-results/<slug>`
//! inboxes, the `system:tool-executor` registration, the parked-notify
//! delivery binding, and the drain task. The per-caller grant table and the
//! channel/policy derivations live in `super::bus_wiring`.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Instant;

use brenn_lib::messaging::system::SystemInbox;
use brenn_lib::messaging::{
    MessageEnvelope, Messenger, ParticipantId, PublishResult, SubscriberKind, Urgency,
};
use brenn_lib::obs::alerting::{AlertDispatcher, AlertSeverity};
use brenn_lib::tools::ResolvedToolGrant;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tracing::{info, warn};

use super::descriptor::{MAX_ASYNC_RESULT_BYTES, ToolError, outcome_label};
use super::registry::ToolRegistry;
use super::tool::{RegisteredTool, ToolCtx};

/// The system component name the executor publishes results under. Its
/// code-built policy (bootstrap) grants publish on exactly `brenn:tool-results/*`.
pub const TOOL_EXECUTOR_COMPONENT: &str = "tool-executor";

/// Per-caller resolved tool grants, keyed by the caller's `ParticipantId` string
/// (`wasm:<slug>`) then by canonical tool name. Built at bootstrap from the
/// resolved WASM consumers; the executor re-checks each dequeued request against
/// it because config may have changed between the request publish and its
/// execution (belt-and-suspenders: the publish-side grant check already ran).
pub type ToolCallerGrants = HashMap<String, BTreeMap<String, ResolvedToolGrant>>;

/// The v1 async tool-call request body published to `brenn:tools/<tool>`. Extra
/// fields (`v`, `idempotency_key`) are ignored — only what the executor acts on
/// is captured here.
#[derive(Debug, Deserialize)]
struct ToolRequest {
    tool: String,
    call_id: String,
    caller: String,
    args: Value,
}

/// The native async-tool executor. One instance drains all `brenn:tools/<tool>`
/// request channels through the single `system:tool-executor` subscriber.
pub struct ToolExecutor {
    /// The shared system drain loop (startup sweep + `Notify`-driven passes,
    /// ack-at-dequeue) over the `system:tool-executor` subscriber.
    inbox: SystemInbox,
    messenger: Arc<Messenger>,
    registry: Arc<ToolRegistry>,
    caller_grants: Arc<ToolCallerGrants>,
    alert: AlertDispatcher,
}

impl ToolExecutor {
    /// Build the executor over the shared messenger, registry, and per-caller
    /// grant table. `notify` is the same handle registered with the wake router
    /// as the executor's parked-notify delivery binding.
    pub fn new(
        messenger: Arc<Messenger>,
        registry: Arc<ToolRegistry>,
        caller_grants: Arc<ToolCallerGrants>,
        alert: AlertDispatcher,
        notify: Arc<Notify>,
    ) -> Self {
        Self {
            inbox: SystemInbox::new(TOOL_EXECUTOR_COMPONENT, messenger.clone(), notify),
            messenger,
            registry,
            caller_grants,
            alert,
        }
    }

    /// Spawn the drain loop as a process-lifetime task. Panics propagate through
    /// the global panic hook (same policy as the WASM dispatch task); no
    /// per-task supervision.
    pub fn spawn(self) -> JoinHandle<()> {
        tokio::spawn(async move {
            let Self {
                inbox,
                messenger,
                registry,
                caller_grants,
                alert,
            } = self;
            inbox
                .run(move |batch| {
                    let messenger = messenger.clone();
                    let registry = registry.clone();
                    let caller_grants = caller_grants.clone();
                    let alert = alert.clone();
                    async move {
                        process_batch(&messenger, &registry, &caller_grants, &alert, batch).await;
                    }
                })
                .await;
        })
    }

    /// One drain step: dequeue the executor's pending request rows through the
    /// inbox (ack-at-dequeue — advance before execute, at-most-once), then
    /// process the batch. Tests drive this method directly and observe
    /// completed delivery on return; `spawn`'s loop is the same dequeue +
    /// process pass driven by the inbox.
    #[cfg(test)]
    pub(crate) async fn drain_step(&self) {
        let batch = self.inbox.dequeue_batch().await;
        process_batch(
            &self.messenger,
            &self.registry,
            &self.caller_grants,
            &self.alert,
            batch,
        )
        .await;
    }
}

/// Process one dequeued (already-acked) batch: group by request channel and
/// drain each tool's requests on its own serialized task.
///
/// Per-tool task: for each request it runs admission (`admit_async`: rate
/// delay + the tool's concurrency permit) *before* spawning the `execute`
/// future, so at most one admission per tool is ever parked awaiting a permit
/// and only bounded `execute` futures (≤ the tool's `max_concurrency`) are
/// spawned. Awaits every spawned execution before returning, so the drain
/// loop only advances to the next batch once this one is fully delivered —
/// keeping the acked-but-unexecuted crash window bounded to a single batch.
/// (A crash mid-batch loses the un-executed requests: at-most-once, accepted;
/// the repo-sync poller backstops git-repo-pull.)
async fn process_batch(
    messenger: &Arc<Messenger>,
    registry: &Arc<ToolRegistry>,
    caller_grants: &Arc<ToolCallerGrants>,
    alert: &AlertDispatcher,
    rows: Vec<(i64, MessageEnvelope)>,
) {
    if rows.is_empty() {
        return;
    }

    // Group by request channel (one `brenn:tools/<tool>` channel per tool) so
    // each tool drains independently: admission on one tool never head-of-line
    // blocks another.
    let mut by_tool: HashMap<String, Vec<(i64, MessageEnvelope)>> = HashMap::new();
    for (push_id, env) in rows {
        by_tool
            .entry(env.channel.clone())
            .or_default()
            .push((push_id, env));
    }

    let mut tool_tasks = Vec::with_capacity(by_tool.len());
    for (_channel, tool_rows) in by_tool {
        let messenger = messenger.clone();
        let registry = registry.clone();
        let caller_grants = caller_grants.clone();
        let alert = alert.clone();
        tool_tasks.push(tokio::spawn(async move {
            // Serial admission-before-spawn for this tool's requests; collect
            // the spawned execute futures and await them so the drain step
            // observes full delivery.
            let mut executions = Vec::new();
            for (push_id, env) in tool_rows {
                if let Some(handle) =
                    admit_and_dispatch(&messenger, &registry, &caller_grants, &alert, push_id, env)
                        .await
                {
                    executions.push(handle);
                }
            }
            for handle in executions {
                join_swallowing_alerted_panic(handle).await;
            }
        }));
    }
    for task in tool_tasks {
        join_swallowing_alerted_panic(task).await;
    }
}

/// Await a spawned task, tolerating a panic that the global panic hook has
/// already logged + Critical-alerted. A tool `execute` (or an admission
/// invariant panic) that unwinds is surfaced once by the hook; the `JoinError`
/// here is that same panic re-surfaced, so the drain keeps going rather than
/// tearing down the executor over one already-reported failure — the same
/// fire-and-forget policy the dispatch tasks follow. A cancellation
/// (`JoinError::is_cancelled`) can only happen if the runtime is shutting down.
async fn join_swallowing_alerted_panic(handle: JoinHandle<()>) {
    if let Err(join_err) = handle.await {
        debug_assert!(
            join_err.is_panic() || join_err.is_cancelled(),
            "unexpected JoinError variant"
        );
    }
}

/// Admit one dequeued request and dispatch its execution: parse the body,
/// re-check grant and ACL, then run admission (rate limit plus concurrency
/// permit) *inline* — the caller invokes this serially per tool, so at most one
/// admission per tool is ever parked awaiting a permit. On success the tool's
/// `execute` future is spawned holding the permit and its `JoinHandle` is
/// returned (so the per-tool drain can await delivery); every rejection path
/// delivers its typed-error result inline and returns `None` (nothing spawned).
async fn admit_and_dispatch(
    messenger: &Arc<Messenger>,
    registry: &Arc<ToolRegistry>,
    caller_grants: &Arc<ToolCallerGrants>,
    alert: &AlertDispatcher,
    push_id: i64,
    env: MessageEnvelope,
) -> Option<JoinHandle<()>> {
    // The reply target is the request envelope's `reply_to`, already the full
    // canonical `brenn:<inbox>` address (the stored channel address carries its
    // scheme). A request with no reply_to has nowhere to send its result — a
    // caller/host bug; the side effect still runs but the result is dropped with
    // a warning.
    let reply_addr = env.reply_to.clone();

    let request: ToolRequest = match serde_json::from_str(&env.body) {
        Ok(r) => r,
        Err(e) => {
            // Malformed body from an internal caller — a bug (post-auth bus
            // traffic, so not fail2ban signal). Alert and best-effort return an
            // error result so the caller can correlate the failure by call_id.
            warn!(push_id, error = %e, "tool executor: malformed request body");
            alert.alert_once_per_process(
                AlertSeverity::Warning,
                "Tool executor received a malformed request".to_string(),
                "tool-executor:malformed-request",
                format!(
                    "A tool request body failed to parse ({e}). An internal caller produced \
                     garbage; this is a bug, not attacker traffic."
                ),
            );
            if let Some(addr) = reply_addr {
                let call_id = serde_json::from_str::<Value>(&env.body)
                    .ok()
                    .and_then(|v| v.get("call_id").and_then(Value::as_str).map(String::from))
                    .unwrap_or_default();
                let body = build_result_body(
                    "unknown",
                    &call_id,
                    &Err(ToolError::InvalidArgs("malformed request body".to_string())),
                    alert,
                );
                publish_result(messenger, &addr, &body).await;
            }
            return None;
        }
    };

    let ToolRequest {
        tool,
        call_id,
        caller,
        args,
    } = request;

    // Defense in depth: every authority decision below keys off the body's
    // `caller`, but the envelope `sender` is host-stamped at flush
    // (`publish_from_wasm`). They must agree; a mismatch means a publisher named a
    // `caller` it is not — deny (never execute, never leak, never trust the body).
    // Unreachable on the current publish path (the only writer to `brenn:tools/*`
    // is the host-built async request whose body caller == its stamped sender), so
    // this collapses the cross-cutting non-forgeability argument to one local check.
    if env.sender != caller {
        warn!(
            push_id, sender = %env.sender, %caller, %tool,
            "tool executor: request caller does not match the host-stamped envelope sender; denying"
        );
        finish(
            messenger,
            &reply_addr,
            &tool,
            &call_id,
            Err(ToolError::NotGranted),
            alert,
        )
        .await;
        return None;
    }

    // Re-check the caller's grant for this tool against the current config.
    let grant = match caller_grants.get(&caller).and_then(|g| g.get(&tool)) {
        Some(g) => g.clone(),
        None => {
            warn!(push_id, %caller, %tool, "tool executor: caller no longer granted this tool");
            finish(
                messenger,
                &reply_addr,
                &tool,
                &call_id,
                Err(ToolError::NotGranted),
                alert,
            )
            .await;
            return None;
        }
    };

    // The tool must still be a registered async tool. A fast or unregistered tool
    // reaching here is a config/wiring anomaly; the caller learns a typed error.
    let async_tool = match registry.get(&tool) {
        Some(RegisteredTool::Async(t)) => t.clone(),
        Some(RegisteredTool::Fast(_)) => {
            warn!(push_id, %tool, "tool executor: request names a fast tool");
            finish(
                messenger,
                &reply_addr,
                &tool,
                &call_id,
                Err(ToolError::WrongClass),
                alert,
            )
            .await;
            return None;
        }
        None => {
            warn!(push_id, %tool, "tool executor: request names an unregistered tool");
            finish(
                messenger,
                &reply_addr,
                &tool,
                &call_id,
                Err(ToolError::NotGranted),
                alert,
            )
            .await;
            return None;
        }
    };

    // Re-check the ACL against the args (config may have tightened since publish).
    if let Err(denied) = async_tool.check_acl(&args, &grant.acl) {
        finish(
            messenger,
            &reply_addr,
            &tool,
            &call_id,
            Err(ToolError::Denied(denied.resource)),
            alert,
        )
        .await;
        return None;
    }

    // Only WASM consumers issue async bus tool calls this cycle; the caller
    // passed the grant lookup above, so it is a configured wasm participant. The
    // scheme is classified through `ParticipantId::kind` rather than a re-spelled
    // `wasm:` prefix. Checked before admission so a bogus caller never holds a
    // permit.
    let caller_pid = ParticipantId::from_stored(caller.clone());
    match caller_pid.kind() {
        SubscriberKind::Wasm(_) => {}
        other => panic!(
            "BUG: tool executor caller {caller:?} is a {other:?}, not a wasm participant \
             (only wasm callers issue async tool calls this cycle)"
        ),
    }

    // Rate-limit admission (delay-not-drop) then the tool's global concurrency
    // permit — the shared guard both caller kinds run through (see
    // `ToolRegistry::admit_async`). Acquired *before* the spawn and moved into it,
    // held until `execute` returns; because this runs serially per tool, only one
    // admission per tool is ever parked here awaiting a permit.
    let permit = registry
        .admit_async(&caller, &tool, grant.rate_limit, alert)
        .await;

    let ctx = ToolCtx {
        caller: caller_pid,
        grant,
        // Bus/executor calls carry no acting conversation (no self-notification
        // suppression); only LLM-originated calls do.
        acting_conversation_id: None,
    };

    // Spawn only the `execute` future (holding the permit) so a long pull never
    // stalls the per-tool admission loop.
    let messenger = messenger.clone();
    let alert = alert.clone();
    Some(tokio::spawn(async move {
        let started = Instant::now();
        let result = async_tool.execute(&ctx, args).await;
        info!(
            tool = %tool,
            caller = %caller,
            class = "async",
            elapsed = ?started.elapsed(),
            outcome = outcome_label(&result),
            "tool invocation (bus)",
        );
        finish(&messenger, &reply_addr, &tool, &call_id, result, &alert).await;
        drop(permit);
    }))
}

/// Build the result envelope and publish it to `reply_addr`, or warn-and-drop
/// when the request carried no reply target.
async fn finish(
    messenger: &Messenger,
    reply_addr: &Option<String>,
    tool: &str,
    call_id: &str,
    result: Result<Value, ToolError>,
    alert: &AlertDispatcher,
) {
    match reply_addr {
        Some(addr) => {
            let body = build_result_body(tool, call_id, &result, alert);
            publish_result(messenger, addr, &body).await;
        }
        None => {
            warn!(%tool, %call_id, "tool executor: request has no reply_to; result dropped");
        }
    }
}

/// Publish a result body to the caller's inbox through the gated system-publish
/// path. An unresolved reply target (consumer removed from config between request
/// and result) is the one deliberate no-alert drop; any other publish failure is
/// a host-wiring bug and panics (the executor's system policy must permit
/// `brenn:tool-results/*`).
async fn publish_result(messenger: &Messenger, reply_addr: &str, body: &str) {
    match messenger
        .publish_from_system(
            TOOL_EXECUTOR_COMPONENT,
            reply_addr,
            body,
            Urgency::Normal,
            None,
        )
        .await
    {
        PublishResult::Ok { .. } => {}
        PublishResult::UnknownChannel(addr) => {
            warn!(
                reply_to = %addr,
                "tool executor: result reply target no longer resolves; dropping result \
                 (caller removed from config)"
            );
        }
        other => panic!(
            "tool executor: publishing a result to {reply_addr:?} failed unexpectedly ({other:?}) \
             — host-wiring invariant violated: the {TOOL_EXECUTOR_COMPONENT} system policy must \
             grant publish on brenn:tool-results/*"
        ),
    }
}

/// Serialize the v1 result envelope, capping the payload. An over-cap result is a
/// tool bug: it alerts and the body is replaced with an `internal` error result.
fn build_result_body(
    tool: &str,
    call_id: &str,
    result: &Result<Value, ToolError>,
    alert: &AlertDispatcher,
) -> String {
    let body = json!({
        "v": 1,
        "tool": tool,
        "call_id": call_id,
        "outcome": outcome_json(result),
    });
    let s = serde_json::to_string(&body).expect("result envelope serializes");
    if s.len() > MAX_ASYNC_RESULT_BYTES {
        warn!(%tool, len = s.len(), cap = MAX_ASYNC_RESULT_BYTES, "tool executor: result over cap (tool bug)");
        alert.alert_once_per_process(
            AlertSeverity::Warning,
            format!("Tool {tool:?} produced an over-cap result"),
            &format!("tool:{tool}:result-cap-exceeded"),
            format!(
                "Tool {tool:?} produced a {} byte result (cap {MAX_ASYNC_RESULT_BYTES}). \
                 A tool result exceeding the cap is a tool bug.",
                s.len(),
            ),
        );
        let capped = json!({
            "v": 1,
            "tool": tool,
            "call_id": call_id,
            "outcome": { "err": { "kind": "internal", "detail": "result exceeded size cap" } },
        });
        return serde_json::to_string(&capped).expect("capped result envelope serializes");
    }
    s
}

/// The `outcome` field of the result envelope: `{ok: <value>}` on success,
/// `{err: {kind, detail}}` on a typed error. The `kind`/`detail` tokens come
/// from `ToolError` itself so the wire vocabulary matches the log vocabulary.
fn outcome_json(result: &Result<Value, ToolError>) -> Value {
    match result {
        Ok(v) => json!({ "ok": v }),
        Err(e) => json!({ "err": { "kind": e.kind_str(), "detail": e.detail() } }),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use async_trait::async_trait;
    use brenn_lib::access::acl::ChannelMatcher;
    use brenn_lib::access::{AppCapability, AppPolicy, GrantSet};
    use brenn_lib::db::init_db_memory;
    use brenn_lib::messaging::config::MessagingGlobalConfig;
    use brenn_lib::messaging::db::{
        PendingPushInsert, insert_message_with_pushes, upsert_channels, utc_to_ns,
    };
    use brenn_lib::messaging::query::NoopWakeRouter;
    use brenn_lib::messaging::testutils::test_channel_entry;
    use brenn_lib::messaging::{
        ChannelScheme, IngressOrBus, MessagingDirectory, SubscriberEntry, SubscriberEntryKind,
        WakeMin, WakeRouter, config::Depth, config::NoiseLevel,
    };
    use brenn_lib::obs::alerting::{make_capturing_alerter, noop_alert_dispatcher};
    use brenn_lib::tools::AclClause;
    use chrono::Utc;
    use indexmap::IndexMap;

    use super::*;
    use crate::tool_registry::descriptor::{AclDenied, Idempotency, ToolClass, ToolDescriptor};
    use crate::tool_registry::tool::{AsyncTool, FastTool};

    const CALLER: &str = "wasm:sync";
    const CALLER_SLUG: &str = "sync";

    /// A stub async tool whose `execute` echoes its args and rejects any `repo`
    /// arg outside the ACL. `max_concurrency` and a shared in-flight probe make it
    /// double as the concurrency-bound fixture.
    struct StubAsync {
        descriptor: ToolDescriptor,
        /// Live count of concurrent `execute` calls and the max observed — the
        /// concurrency test asserts the max never exceeds `max_concurrency`.
        in_flight: Arc<AtomicUsize>,
        max_in_flight: Arc<AtomicUsize>,
        /// Per-call sleep so overlapping calls actually overlap in time.
        hold: Duration,
    }

    impl StubAsync {
        fn new(max_concurrency: usize, hold: Duration) -> Self {
            Self {
                descriptor: ToolDescriptor {
                    name: "apull",
                    mcp_name: "mcp__brenn__APull",
                    description: "stub async",
                    input_schema: json!({ "type": "object" }),
                    class: ToolClass::Async { max_concurrency },
                    acl_keys: &["repo"],
                    idempotency: Idempotency::Natural,
                    auto_approve: true,
                },
                in_flight: Arc::new(AtomicUsize::new(0)),
                max_in_flight: Arc::new(AtomicUsize::new(0)),
                hold,
            }
        }
    }

    #[async_trait]
    impl AsyncTool for StubAsync {
        fn descriptor(&self) -> &ToolDescriptor {
            &self.descriptor
        }
        fn check_acl(&self, args: &Value, acl: &[AclClause]) -> Result<(), AclDenied> {
            crate::tool_registry::testutil::repo_acl_check(args, acl)
        }
        async fn execute(&self, _ctx: &ToolCtx, args: Value) -> Result<Value, ToolError> {
            let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_in_flight.fetch_max(now, Ordering::SeqCst);
            if !self.hold.is_zero() {
                tokio::time::sleep(self.hold).await;
            }
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            Ok(json!({ "echoed": args }))
        }
    }

    /// A fast tool registered under the async request name, used to prove the
    /// executor rejects a request that resolves to the wrong class.
    struct StubFast(ToolDescriptor);
    impl StubFast {
        fn apull() -> Self {
            Self(ToolDescriptor {
                name: "apull",
                mcp_name: "mcp__brenn__APull",
                description: "stub fast",
                input_schema: json!({ "type": "object" }),
                class: ToolClass::Fast {
                    budget: Duration::from_millis(5),
                },
                acl_keys: &["repo"],
                idempotency: Idempotency::Natural,
                auto_approve: true,
            })
        }
    }
    impl FastTool for StubFast {
        fn descriptor(&self) -> &ToolDescriptor {
            &self.0
        }
        fn check_acl(&self, args: &Value, acl: &[AclClause]) -> Result<(), AclDenied> {
            crate::tool_registry::testutil::repo_acl_check(args, acl)
        }
        fn execute(&self, _ctx: &ToolCtx, args: Value) -> Result<Value, ToolError> {
            Ok(args)
        }
    }

    use crate::tool_registry::testutil::{clause, grant};

    fn sub(kind: SubscriberEntryKind) -> SubscriberEntry {
        // Only `UrgencyGated` (App) subscribers carry a wake threshold.
        let wake_min = matches!(kind, SubscriberEntryKind::App(_)).then_some(WakeMin::Normal);
        SubscriberEntry {
            kind,
            push_depth: Depth::Unbounded,
            retain_depth: Depth::Unbounded,
            noise: NoiseLevel::Silent,
            wake_min,
        }
    }

    /// `MessagingPublish` + a `brenn_publish` prefix matcher policy.
    fn publish_policy(prefix: &str) -> AppPolicy {
        let mut grants = GrantSet::default();
        grants.insert(AppCapability::MessagingPublish);
        let mut acls = brenn_lib::access::acl::AclSet::default();
        acls.brenn_publish
            .push(ChannelMatcher::Prefix(prefix.to_string()));
        AppPolicy {
            grants,
            acls,
            tool_grants: BTreeMap::new(),
        }
    }

    /// `MessagingSubscribe` + a `brenn_subscribe` prefix matcher policy (delivery).
    fn subscribe_policy(prefix: &str) -> AppPolicy {
        let mut grants = GrantSet::default();
        grants.insert(AppCapability::MessagingSubscribe);
        let mut acls = brenn_lib::access::acl::AclSet::default();
        acls.brenn_subscribe
            .push(ChannelMatcher::Prefix(prefix.to_string()));
        AppPolicy {
            grants,
            acls,
            tool_grants: BTreeMap::new(),
        }
    }

    /// Full test harness: an in-memory messenger with a `tools/apull` request
    /// channel (executor subscriber) and a `tool-results/sync` inbox (wasm
    /// subscriber), the executor's system policy, and the caller's wasm policy.
    struct Harness {
        messenger: Arc<Messenger>,
        tools_uuid: uuid::Uuid,
        results_uuid: uuid::Uuid,
    }

    async fn harness() -> Harness {
        let db = init_db_memory();

        let tools_ch = test_channel_entry(
            "tools/apull",
            vec![sub(SubscriberEntryKind::System(
                TOOL_EXECUTOR_COMPONENT.to_string(),
            ))],
        );
        let results_ch = test_channel_entry(
            "tool-results/sync",
            vec![sub(SubscriberEntryKind::Wasm(CALLER_SLUG.to_string()))],
        );
        let tools_uuid = tools_ch.uuid;
        let results_uuid = results_ch.uuid;

        {
            let conn = db.lock().await;
            upsert_channels(&conn, &[tools_ch.clone(), results_ch.clone()]);
        }

        let directory = Arc::new(MessagingDirectory::with_entries(vec![tools_ch, results_ch]));
        let messenger = Messenger::new(
            db,
            directory,
            Arc::from("test"),
            Arc::new(IndexMap::new()),
            Arc::new(NoopWakeRouter) as Arc<dyn WakeRouter>,
            MessagingGlobalConfig::default(),
        );
        let mut wasm_policies = HashMap::new();
        wasm_policies.insert(CALLER_SLUG.to_string(), subscribe_policy("tool-results/"));
        let mut system_policies = HashMap::new();
        system_policies.insert(
            TOOL_EXECUTOR_COMPONENT.to_string(),
            publish_policy("tool-results/"),
        );
        let messenger = messenger
            .with_subscriber_registrations(brenn_lib::messaging::testutils::wasm_registrations(
                wasm_policies,
            ))
            .with_subscriber_registrations(brenn_lib::messaging::testutils::system_registrations(
                system_policies,
            ));

        Harness {
            messenger,
            tools_uuid,
            results_uuid,
        }
    }

    /// Insert one request pending-push row for the executor on `tools/apull`,
    /// with `reply_to` set to the `tool-results/sync` inbox.
    async fn insert_request(h: &Harness, body: &str, reply_to_uuid: Option<uuid::Uuid>) -> i64 {
        let conn = h.messenger.db().lock().await;
        let push = PendingPushInsert {
            target_subscriber: ParticipantId::for_system(TOOL_EXECUTOR_COMPONENT),
            target_app_slug: TOOL_EXECUTOR_COMPONENT.to_string(),
            eager_wake: true,
            release_after: None,
            delivery_deadline: None,
        };
        let msg = insert_message_with_pushes(
            &conn,
            h.tools_uuid,
            "test",
            CALLER,
            body,
            Urgency::Normal,
            ChannelScheme::Brenn,
            reply_to_uuid,
            None,
            None,
            utc_to_ns(Utc::now()),
            &[push],
        );
        msg.push_ids[0]
    }

    fn request_body(call_id: &str, repo: &str) -> String {
        json!({
            "v": 1,
            "tool": "apull",
            "call_id": call_id,
            "caller": CALLER,
            "args": { "repo": repo },
        })
        .to_string()
    }

    fn caller_grants(acl: Vec<AclClause>) -> Arc<ToolCallerGrants> {
        let mut per_caller = BTreeMap::new();
        per_caller.insert("apull".to_string(), grant(acl));
        let mut map: ToolCallerGrants = HashMap::new();
        map.insert(CALLER.to_string(), per_caller);
        Arc::new(map)
    }

    fn registry(tool: StubAsync) -> Arc<ToolRegistry> {
        Arc::new(ToolRegistry::new(vec![RegisteredTool::Async(Arc::new(
            tool,
        ))]))
    }

    fn executor(
        h: &Harness,
        reg: Arc<ToolRegistry>,
        grants: Arc<ToolCallerGrants>,
        alert: AlertDispatcher,
    ) -> ToolExecutor {
        ToolExecutor::new(
            h.messenger.clone(),
            reg,
            grants,
            alert,
            Arc::new(Notify::new()),
        )
    }

    async fn drain_and_join(exec: &ToolExecutor) {
        // `drain_step` awaits every spawned execution before returning, so a
        // single call fully drains and delivers the batch.
        exec.drain_step().await;
    }

    /// The one result row on the caller's inbox, decoded as JSON.
    async fn read_result(h: &Harness) -> Value {
        let rows = h
            .messenger
            .load_pending_pushes(&ParticipantId::for_wasm(CALLER_SLUG))
            .await;
        assert_eq!(rows.len(), 1, "exactly one result row on the caller inbox");
        match &rows[0].1 {
            IngressOrBus::Bus(env) => serde_json::from_str(&env.body).expect("result body is JSON"),
            other => panic!("expected a bus result row, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn drains_executes_and_delivers_result_and_acks_request() {
        let h = harness().await;
        let push_id = insert_request(&h, &request_body("c1", "brenn"), Some(h.results_uuid)).await;
        let (alert, _hg) = noop_alert_dispatcher();
        let exec = executor(
            &h,
            registry(StubAsync::new(4, Duration::ZERO)),
            caller_grants(vec![clause(&[("repo", "brenn")])]),
            alert,
        );

        drain_and_join(&exec).await;

        // Request row acked (ack-at-dequeue).
        let remaining = h
            .messenger
            .load_pending_pushes(&ParticipantId::for_system(TOOL_EXECUTOR_COMPONENT))
            .await;
        assert!(
            remaining.is_empty(),
            "request row must be acked, got {remaining:?}"
        );
        let _ = push_id;

        // Result delivered to the caller inbox with the ok outcome + echoed args.
        let result = read_result(&h).await;
        assert_eq!(result["v"], 1);
        assert_eq!(result["tool"], "apull");
        assert_eq!(result["call_id"], "c1");
        assert_eq!(result["outcome"]["ok"]["echoed"]["repo"], "brenn");
    }

    #[tokio::test]
    async fn revoked_grant_denies_at_dequeue() {
        let h = harness().await;
        insert_request(&h, &request_body("c2", "brenn"), Some(h.results_uuid)).await;
        let (alert, _hg) = noop_alert_dispatcher();
        // Empty caller-grants map: the caller is no longer granted anything.
        let exec = executor(
            &h,
            registry(StubAsync::new(4, Duration::ZERO)),
            Arc::new(HashMap::new()),
            alert,
        );

        drain_and_join(&exec).await;

        let result = read_result(&h).await;
        assert_eq!(result["call_id"], "c2");
        assert_eq!(result["outcome"]["err"]["kind"], "not_granted");
    }

    #[tokio::test]
    async fn acl_miss_denies_and_names_resource() {
        let h = harness().await;
        insert_request(&h, &request_body("c3", "pfin"), Some(h.results_uuid)).await;
        let (alert, _hg) = noop_alert_dispatcher();
        // Grant admits only "brenn"; the request names "pfin".
        let exec = executor(
            &h,
            registry(StubAsync::new(4, Duration::ZERO)),
            caller_grants(vec![clause(&[("repo", "brenn")])]),
            alert,
        );

        drain_and_join(&exec).await;

        let result = read_result(&h).await;
        assert_eq!(result["outcome"]["err"]["kind"], "denied");
        assert_eq!(result["outcome"]["err"]["detail"], "pfin");
    }

    #[tokio::test]
    async fn malformed_request_alerts_and_returns_error_result() {
        let h = harness().await;
        // A body that parses as JSON (so call_id survives) but is missing the
        // required `tool`/`caller` fields → ToolRequest deserialize fails.
        let bad = json!({ "v": 1, "call_id": "c4" }).to_string();
        insert_request(&h, &bad, Some(h.results_uuid)).await;
        let (alert, captured, handle) = make_capturing_alerter();
        let exec = executor(
            &h,
            registry(StubAsync::new(4, Duration::ZERO)),
            caller_grants(vec![clause(&[("repo", "brenn")])]),
            alert.clone(),
        );

        drain_and_join(&exec).await;

        let result = read_result(&h).await;
        assert_eq!(result["call_id"], "c4");
        assert_eq!(result["outcome"]["err"]["kind"], "invalid_args");

        // The executor holds an `AlertDispatcher` clone; the capturing drainer's
        // handle only completes once every clone is dropped, so release the
        // executor before awaiting it (drop protocol from `make_capturing_alerter`).
        drop(exec);
        alert.flush().await;
        drop(alert);
        handle.await.unwrap();
        let captured = captured.lock().unwrap();
        assert!(
            captured
                .iter()
                .any(|(t, _)| t.contains("malformed request")),
            "malformed-request alert should fire: {captured:?}",
        );
    }

    #[tokio::test]
    async fn missing_reply_target_warns_and_drops_without_panic() {
        let h = harness().await;
        let (alert, _hg) = noop_alert_dispatcher();
        // Publishing a result to an address absent from the directory yields
        // UnknownChannel → warn+drop, never a panic (the caller-removed case).
        publish_result(
            &h.messenger,
            "brenn:tool-results/ghost",
            &build_result_body("apull", "c5", &Ok(json!({})), &alert),
        )
        .await;
        // Nothing landed on any inbox; the drop is silent-but-logged.
        let rows = h
            .messenger
            .load_pending_pushes(&ParticipantId::for_wasm(CALLER_SLUG))
            .await;
        assert!(rows.is_empty(), "no result row for a dropped reply target");
    }

    #[tokio::test]
    async fn concurrency_semaphore_bounds_overlapping_executions() {
        let h = harness().await;
        // Two requests, a tool with max_concurrency = 1 and a real hold: without
        // the semaphore both execute futures would overlap (max_in_flight = 2).
        insert_request(&h, &request_body("a", "brenn"), Some(h.results_uuid)).await;
        insert_request(&h, &request_body("b", "brenn"), Some(h.results_uuid)).await;
        let tool = StubAsync::new(1, Duration::from_millis(80));
        let max_in_flight = tool.max_in_flight.clone();
        let (alert, _hg) = noop_alert_dispatcher();
        let exec = executor(
            &h,
            registry(tool),
            caller_grants(vec![clause(&[("repo", "brenn")])]),
            alert,
        );

        drain_and_join(&exec).await;

        assert_eq!(
            max_in_flight.load(Ordering::SeqCst),
            1,
            "max_concurrency = 1 must serialize the two executions",
        );
        // Both results delivered.
        let rows = h
            .messenger
            .load_pending_pushes(&ParticipantId::for_wasm(CALLER_SLUG))
            .await;
        assert_eq!(rows.len(), 2, "both results delivered to the caller inbox");
    }

    #[tokio::test]
    async fn startup_sweep_executes_a_preexisting_pending_row() {
        // The run-loop's startup sweep is `drain_step` before the first wake; a
        // row already pending at construction is executed. Driving `drain_step`
        // directly (as the sweep does) proves the crash-recovery path.
        let h = harness().await;
        insert_request(&h, &request_body("sweep", "brenn"), Some(h.results_uuid)).await;
        let (alert, _hg) = noop_alert_dispatcher();
        let exec = executor(
            &h,
            registry(StubAsync::new(4, Duration::ZERO)),
            caller_grants(vec![clause(&[("repo", "brenn")])]),
            alert,
        );

        drain_and_join(&exec).await;

        let result = read_result(&h).await;
        assert_eq!(result["call_id"], "sweep");
        assert_eq!(result["outcome"]["ok"]["echoed"]["repo"], "brenn");
    }

    #[tokio::test]
    async fn no_reply_to_still_executes_and_drops_result() {
        // A request with no reply_to: the side effect runs, the result is dropped.
        let h = harness().await;
        let max_in_flight = {
            let tool = StubAsync::new(4, Duration::ZERO);
            // `max_in_flight` is fetch_max'd on entry to `execute` and never
            // decremented, so it reads >= 1 iff `execute` was actually entered.
            // (`in_flight` alone is incremented then decremented inside `execute`,
            // so it reads 0 whether the tool ran or was skipped — a vacuous probe.)
            let probe = tool.max_in_flight.clone();
            insert_request(&h, &request_body("c6", "brenn"), None).await;
            let (alert, _hg) = noop_alert_dispatcher();
            let exec = executor(
                &h,
                registry(tool),
                caller_grants(vec![clause(&[("repo", "brenn")])]),
                alert,
            );
            drain_and_join(&exec).await;
            probe
        };
        // No result row anywhere.
        let rows = h
            .messenger
            .load_pending_pushes(&ParticipantId::for_wasm(CALLER_SLUG))
            .await;
        assert!(rows.is_empty(), "no result row when reply_to is absent");
        // The side effect still ran despite the missing reply target.
        assert!(
            max_in_flight.load(Ordering::SeqCst) >= 1,
            "the tool must still execute when reply_to is absent",
        );
    }

    #[tokio::test]
    async fn build_result_body_over_cap_substitutes_internal_and_alerts() {
        // A tool returning a result over `MAX_ASYNC_RESULT_BYTES` is a tool bug:
        // the body is replaced with an `internal`/"exceeded size cap" envelope and
        // the cap alert fires.
        let (alert, captured, handle) = make_capturing_alerter();
        let big = "x".repeat(MAX_ASYNC_RESULT_BYTES + 1);
        let body = build_result_body("apull", "c7", &Ok(json!({ "blob": big })), &alert);
        let v: Value = serde_json::from_str(&body).expect("capped body is JSON");
        assert_eq!(v["call_id"], "c7");
        assert_eq!(v["outcome"]["err"]["kind"], "internal");
        assert_eq!(v["outcome"]["err"]["detail"], "result exceeded size cap");
        assert!(
            body.len() <= MAX_ASYNC_RESULT_BYTES,
            "the substituted envelope must be within the cap"
        );

        alert.flush().await;
        drop(alert);
        handle.await.unwrap();
        let captured = captured.lock().unwrap();
        assert!(
            captured.iter().any(|(t, _)| t.contains("over-cap result")),
            "result-cap alert should fire: {captured:?}",
        );
    }

    #[tokio::test]
    async fn request_naming_fast_tool_returns_wrong_class() {
        // The caller is granted, but the registry resolves the name to a *fast*
        // tool: the executor must return `wrong_class`, never execute.
        let h = harness().await;
        insert_request(&h, &request_body("c8", "brenn"), Some(h.results_uuid)).await;
        let (alert, _hg) = noop_alert_dispatcher();
        let reg = Arc::new(ToolRegistry::new(vec![RegisteredTool::Fast(Arc::new(
            StubFast::apull(),
        ))]));
        let exec = executor(
            &h,
            reg,
            caller_grants(vec![clause(&[("repo", "brenn")])]),
            alert,
        );

        drain_and_join(&exec).await;

        let result = read_result(&h).await;
        assert_eq!(result["call_id"], "c8");
        assert_eq!(result["outcome"]["err"]["kind"], "wrong_class");
    }

    #[tokio::test]
    async fn request_naming_unregistered_tool_returns_not_granted() {
        // The caller holds a grant for a tool absent from the registry (a config/
        // wiring anomaly): the executor returns `not_granted` (oracle closure).
        let h = harness().await;
        let body = json!({
            "v": 1,
            "tool": "ghost",
            "call_id": "c9",
            "caller": CALLER,
            "args": { "repo": "brenn" },
        })
        .to_string();
        insert_request(&h, &body, Some(h.results_uuid)).await;
        let (alert, _hg) = noop_alert_dispatcher();
        // Grant keyed on the ghost tool so the grant lookup passes; the registry
        // only knows "apull", so `registry.get("ghost")` is None.
        let mut per_caller = BTreeMap::new();
        per_caller.insert(
            "ghost".to_string(),
            grant(vec![clause(&[("repo", "brenn")])]),
        );
        let mut map: ToolCallerGrants = HashMap::new();
        map.insert(CALLER.to_string(), per_caller);
        let exec = executor(
            &h,
            registry(StubAsync::new(4, Duration::ZERO)),
            Arc::new(map),
            alert,
        );

        drain_and_join(&exec).await;

        let result = read_result(&h).await;
        assert_eq!(result["call_id"], "c9");
        assert_eq!(result["outcome"]["err"]["kind"], "not_granted");
    }

    #[tokio::test]
    async fn result_row_activates_on_tool_results_port_and_is_not_retired() {
        use crate::tool_registry::bus_wiring::{TOOL_RESULT_INPUT_PORT, inbox_input_port};

        // Publish a result to the caller's inbox exactly as the executor does.
        let h = harness().await;
        let (alert, _hg) = noop_alert_dispatcher();
        publish_result(
            &h.messenger,
            "brenn:tool-results/sync",
            &build_result_body(
                "apull",
                "r1",
                &Ok(json!({ "echoed": { "repo": "brenn" } })),
                &alert,
            ),
        )
        .await;

        // Drain the caller's inbox the way the wasm dispatch loop does: an
        // activation snapshot whose inputs include the consumer's own inbox port
        // (`inbox_input_port`). Because that port is a *current* input, the row
        // activates the consumer on the `tool-results` port rather than being
        // retired as residue.
        let caller = ParticipantId::for_wasm(CALLER_SLUG);
        let snapshot = h
            .messenger
            .load_activation_snapshot(&caller, &[inbox_input_port(CALLER_SLUG)])
            .await
            .expect("a pending inbox row on a triggering input port yields an activation");
        let port = snapshot
            .iter()
            .find(|p| p.port == TOOL_RESULT_INPUT_PORT)
            .expect("the tool-results port is present in the activation");
        assert_eq!(port.channel_address, "brenn:tool-results/sync");
        assert_eq!(port.new_rows.len(), 1, "the one result row is delivered");
        let body: Value =
            serde_json::from_str(&port.new_rows[0].1.body).expect("result body is JSON");
        assert_eq!(body["call_id"], "r1");
        assert_eq!(body["outcome"]["ok"]["echoed"]["repo"], "brenn");
    }

    #[tokio::test]
    async fn result_row_is_retired_when_inbox_port_absent_from_inputs() {
        // The mirror of the property above: if the inbox channel is *not* a
        // current input, the row is residue and is retired (marked delivered) with
        // no activation. This pins why folding `inbox_input_port` into the
        // consumer's `inputs` is load-bearing — without it every result is eaten.
        let h = harness().await;
        let (alert, _hg) = noop_alert_dispatcher();
        publish_result(
            &h.messenger,
            "brenn:tool-results/sync",
            &build_result_body("apull", "r2", &Ok(json!({})), &alert),
        )
        .await;

        let caller = ParticipantId::for_wasm(CALLER_SLUG);
        // No inputs ⇒ the inbox channel matches no port ⇒ the row is retired.
        let snapshot = h.messenger.load_activation_snapshot(&caller, &[]).await;
        assert!(
            snapshot.is_none(),
            "with no matching input port the result row must not activate",
        );
        // Retired means acked: a second drain sees nothing pending.
        let remaining = h.messenger.load_pending_pushes(&caller).await;
        assert!(
            remaining.is_empty(),
            "the residue row must be retired (marked delivered), got {remaining:?}",
        );
    }
}
