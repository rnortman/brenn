//! The `ToolExecutor`: the native `system:` subscriber that turns an async tool
//! call on the bus into an execution and a result activation.
//!
//! A WASM consumer's `call-async` becomes a durable message on
//! `brenn:tools/<tool>` carrying a `reply_to` of `brenn:tool-results/<slug>`.
//! The executor subscribes as the single `system:tool-executor` principal,
//! drains those requests, re-checks the caller's grant + ACL against the
//! current config, admits through the per-tool
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
//! tool per batch and a slow pull never blocks a different tool. Only the
//! `execute` future is spawned, holding the permit until it returns; per-tool
//! concurrency is bounded by the registry semaphore and per-resource
//! dogpiling by the tool's own guard.
//!
//! The drain awaits delivery of every tool whose execution does not depend on
//! the drain, and of no other. Two regimes follow:
//!
//! - **Native tools.** Admission before spawn, one parked admission per tool,
//!   and the drain awaits the whole batch before dequeuing the next, so the
//!   acked-but-unexecuted crash window is one batch.
//! - **Provided tools.** The same per-tool task, but detached: the drain does
//!   not await it. A provided tool's `execute` completes only after its
//!   provider's own bus calls are executed by this same drain, so awaiting the
//!   execution — or an admission parked behind permits those executions hold —
//!   would stall the drain until the call's deadline. Every request of a batch
//!   carries one deadline, the batch's dequeue instant plus
//!   [`PROVIDED_TOOL_CEILING`]; an admission still parked then is answered
//!   `internal` with nothing handed to the provider, and an execution still
//!   waiting then is answered by the tool itself. The detached tasks alive for
//!   one provided tool are therefore at most the batches dequeued within one
//!   ceiling.
//!
//! A request carrying `serving` is a provider's nested call on behalf of the
//! pending provided-tool call it names. Its acting user is read from that
//! call's [`ProviderRuntime`] record after the permit is acquired and just
//! before `execute`, so a call whose record went away while it was parked is
//! denied and never runs; the result echoes `serving` back to the provider.
//! No body field ever names a user. A nested call to a provided tool takes the
//! earlier of its batch's deadline and the served call's, so a chain of
//! provided calls, however long or cyclic, ends by the deadline of the call
//! that started it.
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

use brenn_envelope::addressing::TOOLS_NAMESPACE;
use brenn_lib::messaging::{
    MessageEnvelope, ParticipantId, SubscriberKind, Urgency, canonical_address,
};
use brenn_lib::tools::ResolvedToolGrant;
use brenn_messaging::system::SystemInbox;
use brenn_messaging::{Messenger, PublishResult};
use brenn_messaging_store::store::MessageSeq;
use brenn_obs::alerting::{AlertDispatcher, AlertSeverity};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tracing::{info, warn};

use super::bus_wiring::request_channel_name;
use super::descriptor::{MAX_ASYNC_RESULT_BYTES, PROVIDED_TOOL_CEILING, ToolError, outcome_label};
use super::provided::ProviderRuntime;
use super::registry::ToolRegistry;
use super::tool::{RegisteredTool, ToolCtx};

/// The system component name the executor publishes results under. Its
/// code-built policy (bootstrap) grants publish on exactly `brenn:tool-results/*`.
pub const TOOL_EXECUTOR_COMPONENT: &str = "tool-executor";

/// Per-caller resolved tool grants, keyed by the caller's `ParticipantId`
/// string (`wasm:<slug>`) then by canonical tool name.
///
/// The value the messaging plan derives from the document's consumers and that
/// [`ToolCallerGrants`] is built from and edited to.
pub type CallerGrantTable = HashMap<String, BTreeMap<String, ResolvedToolGrant>>;

/// Per-caller resolved tool grants, keyed by the caller's `ParticipantId` string
/// (`wasm:<slug>`) then by canonical tool name. Built at bootstrap from the
/// resolved WASM consumers and edited as consumers join and leave; the executor
/// re-checks each dequeued request against it because the grants may have
/// changed between the request publish and its execution (belt-and-suspenders:
/// the publish-side grant check already ran).
///
/// Behind an `RwLock` because the table outlives the set of callers it describes
/// and the executor reads it per request; the read answers with an owned grant
/// so no lock is held across a tool execution.
#[derive(Default)]
pub struct ToolCallerGrants {
    inner: std::sync::RwLock<CallerGrantTable>,
}

impl std::fmt::Debug for ToolCallerGrants {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ToolCallerGrants")
            .field("callers", &self.read().len())
            .finish()
    }
}

impl ToolCallerGrants {
    /// The table as it stands at boot: one entry per caller with a non-empty
    /// grant set.
    pub fn new(grants: CallerGrantTable) -> Self {
        Self {
            inner: std::sync::RwLock::new(grants),
        }
    }

    /// The table as it stands, copied out.
    ///
    /// For a test reader that has to compare one process's authorization table
    /// with another's. Behind `testutils` because holding the whole table is
    /// exactly what this type's shape refuses a production caller: the lock is
    /// taken per grant and never held across an execution, and a request path
    /// that copied the table out would answer from a snapshot rather than from
    /// the grants as they stand.
    #[cfg(any(test, feature = "testutils"))]
    pub fn snapshot(&self) -> CallerGrantTable {
        self.read().clone()
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, CallerGrantTable> {
        self.inner.read().expect("tool caller grants lock poisoned")
    }

    /// Install one caller's whole grant set, replacing whatever it held. A
    /// caller with no grants is removed rather than stored empty, so presence in
    /// the table means "may address some tool".
    pub fn set_caller(&self, caller: String, grants: BTreeMap<String, ResolvedToolGrant>) {
        let mut inner = self
            .inner
            .write()
            .expect("tool caller grants lock poisoned");
        if grants.is_empty() {
            inner.remove(&caller);
        } else {
            inner.insert(caller, grants);
        }
    }

    /// Drop one caller's grants entirely. Idempotent: a caller that held none is
    /// absent either way.
    pub fn remove_caller(&self, caller: &str) {
        self.inner
            .write()
            .expect("tool caller grants lock poisoned")
            .remove(caller);
    }

    /// `caller`'s resolved grant for `tool` as the table stands now, or `None`
    /// if the caller may not address it. Owned, so the lock is released before
    /// the grant is acted on.
    pub fn grant(&self, caller: &str, tool: &str) -> Option<ResolvedToolGrant> {
        self.read().get(caller).and_then(|g| g.get(tool)).cloned()
    }
}

/// The v1 async tool-call request body published to `brenn:tools/<tool>`:
/// written by [`WasmToolHost`](crate::WasmToolHost) and read here, so the two
/// sides share one spelling. Unknown fields are ignored on read; `v` is
/// required, and its value is not checked: the one writer stamps 1 and ships
/// with this reader. `serving` is
/// present on a provider's nested call: the pending provided-tool call it acts
/// for.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ToolRequest {
    pub(crate) v: u32,
    pub(crate) tool: String,
    pub(crate) call_id: String,
    pub(crate) caller: String,
    pub(crate) args: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) serving: Option<String>,
}

/// The handles every batch's tasks share: the bus, the tools, the grants, the
/// pending provided-tool calls, and the alert dispatcher.
#[derive(Clone)]
struct ExecutorShared {
    messenger: Arc<Messenger>,
    registry: Arc<ToolRegistry>,
    caller_grants: Arc<ToolCallerGrants>,
    provider: Arc<ProviderRuntime>,
    alert: AlertDispatcher,
}

/// What one dequeued batch's tasks share: the executor's handles and the
/// instant the batch was dequeued.
#[derive(Clone)]
struct BatchCtx {
    shared: ExecutorShared,
    dequeued: tokio::time::Instant,
}

impl ExecutorShared {
    /// The context of a batch dequeued now.
    fn batch_ctx(&self) -> BatchCtx {
        BatchCtx {
            shared: self.clone(),
            dequeued: tokio::time::Instant::now(),
        }
    }
}

/// The native async-tool executor. One instance drains all `brenn:tools/<tool>`
/// request channels through the single `system:tool-executor` subscriber.
pub struct ToolExecutor {
    /// The shared system drain loop (startup sweep + `Notify`-driven passes,
    /// ack-at-dequeue) over the `system:tool-executor` subscriber.
    inbox: SystemInbox,
    shared: ExecutorShared,
}

impl ToolExecutor {
    /// Build the executor over the shared messenger, registry, per-caller
    /// grant table, and the process's pending provided-tool calls. `notify` is
    /// the same handle registered with the wake router as the executor's
    /// parked-notify delivery binding.
    pub fn new(
        messenger: Arc<Messenger>,
        registry: Arc<ToolRegistry>,
        caller_grants: Arc<ToolCallerGrants>,
        provider: Arc<ProviderRuntime>,
        alert: AlertDispatcher,
        notify: Arc<Notify>,
    ) -> Self {
        Self {
            inbox: SystemInbox::new(TOOL_EXECUTOR_COMPONENT, messenger.clone(), notify),
            shared: ExecutorShared {
                messenger,
                registry,
                caller_grants,
                provider,
                alert,
            },
        }
    }

    /// Spawn the drain loop as a process-lifetime task. Panics propagate through
    /// the global panic hook (same policy as the WASM dispatch task); no
    /// per-task supervision.
    pub fn spawn(self) -> JoinHandle<()> {
        tokio::spawn(async move {
            let Self { inbox, shared } = self;
            inbox
                .run(move |batch| {
                    // Called with each batch as it is dequeued: the batch's
                    // provided-tool deadline runs from here.
                    let cx = shared.batch_ctx();
                    async move {
                        process_batch(cx, batch).await;
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
    #[cfg(any(test, feature = "testutils"))]
    pub async fn drain_step(&self) {
        let batch = self.inbox.dequeue_batch().await;
        let cx = self.shared.batch_ctx();
        process_batch(cx, batch).await;
    }
}

/// Process one dequeued (already-acked) batch: group by request channel and
/// drain each tool's requests on its own serialized task.
///
/// Per-tool task: for each request it runs admission (rate delay + the tool's
/// concurrency permit) *before* spawning the `execute` future, so at most one
/// admission per tool is ever parked awaiting a permit and only bounded
/// `execute` futures (≤ the tool's `max_concurrency`) are spawned; the task
/// awaits its own executions. For a native tool this function awaits the
/// task, so the drain loop only advances to the next batch once this one is
/// fully delivered — keeping the acked-but-unexecuted crash window bounded to
/// a single batch. (A crash mid-batch loses the un-executed requests:
/// at-most-once, accepted; the repo-sync poller backstops git-repo-pull.) A
/// provided tool's task is detached and bounded by the batch's deadline
/// instead (module doc).
async fn process_batch(cx: BatchCtx, rows: Vec<(MessageSeq, MessageEnvelope)>) {
    if rows.is_empty() {
        return;
    }

    // Group by request channel (one `brenn:tools/<tool>` channel per tool) so
    // each tool drains independently: admission on one tool never head-of-line
    // blocks another.
    let mut by_tool: HashMap<String, Vec<(MessageSeq, MessageEnvelope)>> = HashMap::new();
    for (seq, env) in rows {
        by_tool
            .entry(env.channel.clone())
            .or_default()
            .push((seq, env));
    }

    let tools_prefix = canonical_address(TOOLS_NAMESPACE);
    let mut tool_tasks = Vec::with_capacity(by_tool.len());
    for (channel, tool_rows) in by_tool {
        let provided = channel
            .strip_prefix(tools_prefix.as_str())
            .is_some_and(|tool| cx.shared.registry.is_provided(tool));
        let cx = cx.clone();
        let task = tokio::spawn(async move {
            let mut executions = Vec::new();
            for (seq, env) in tool_rows {
                if let Some(handle) = admit_and_dispatch(&cx, seq, env).await {
                    executions.push(handle);
                }
            }
            for handle in executions {
                join_swallowing_alerted_panic(handle).await;
            }
        });
        if provided {
            // Detached: its executions depend on this drain (module doc).
            drop(task);
        } else {
            tool_tasks.push(task);
        }
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

/// Where one request's result goes and what it echoes: the caller's inbox (or
/// nowhere, for a request with no `reply_to`), the tool and `call_id`, and the
/// `serving` id of a nested provider call.
struct Reply {
    addr: Option<String>,
    tool: String,
    call_id: String,
    serving: Option<String>,
}

impl Reply {
    /// Build the result envelope and publish it to the reply target, or
    /// warn-and-drop when the request carried none.
    async fn send(
        &self,
        messenger: &Messenger,
        result: Result<Value, ToolError>,
        alert: &AlertDispatcher,
    ) {
        let Self {
            addr,
            tool,
            call_id,
            serving,
        } = self;
        match addr {
            Some(addr) => {
                let body = build_result_body(tool, call_id, serving.as_deref(), &result, alert);
                publish_result(messenger, addr, &body).await;
            }
            None => {
                warn!(%tool, %call_id, "tool executor: request has no reply_to; result dropped");
            }
        }
    }
}

/// Admit one dequeued request and dispatch its execution: parse the body,
/// re-check grant and ACL, then run admission (rate limit plus concurrency
/// permit) *inline* — the caller invokes this serially per tool, so at most one
/// admission per tool is ever parked awaiting a permit. A provided tool's
/// admission is bounded by the batch's deadline. On success the tool's
/// `execute` future is spawned holding the permit and its `JoinHandle` is
/// returned (so the per-tool drain can await delivery); every rejection path
/// delivers its typed-error result inline and returns `None` (nothing spawned).
async fn admit_and_dispatch(
    cx: &BatchCtx,
    seq: MessageSeq,
    env: MessageEnvelope,
) -> Option<JoinHandle<()>> {
    let BatchCtx {
        shared:
            ExecutorShared {
                messenger,
                registry,
                caller_grants,
                provider,
                alert,
            },
        dequeued,
    } = cx;
    let seq = seq.0;
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
            warn!(seq, error = %e, "tool executor: malformed request body");
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
                    None,
                    &Err(ToolError::InvalidArgs("malformed request body".to_string())),
                    alert,
                );
                publish_result(messenger, &addr, &body).await;
            }
            return None;
        }
    };

    let ToolRequest {
        v: _,
        tool,
        call_id,
        caller,
        args,
        serving,
    } = request;
    let reply = Reply {
        addr: reply_addr,
        tool: tool.clone(),
        call_id,
        serving,
    };

    // `process_batch` chose this request's regime (awaited or detached) from
    // its channel; admission and execution key off the body's `tool`. The one
    // writer publishes each request on its own tool's channel, so a mismatch is
    // a host bug that would run a provided execution inside the awaited drain.
    assert!(
        env.channel == canonical_address(&request_channel_name(&tool)),
        "BUG: tool executor: request {seq} on {:?} names tool {tool:?} in its body; every \
         request is published on its own tool's channel",
        env.channel,
    );

    // Defense in depth: every authority decision below keys off the body's
    // `caller`, but the envelope `sender` is host-stamped at flush
    // (`publish_from_wasm`). They must agree; a mismatch means a publisher named a
    // `caller` it is not — deny (never execute, never leak, never trust the body).
    // Unreachable on the current publish path (the only writer to `brenn:tools/*`
    // is the host-built async request whose body caller == its stamped sender), so
    // this collapses the cross-cutting non-forgeability argument to one local check.
    if env.sender != caller {
        warn!(
            seq, sender = %env.sender, %caller, %tool,
            "tool executor: request caller does not match the host-stamped envelope sender; denying"
        );
        reply
            .send(messenger, Err(ToolError::NotGranted), alert)
            .await;
        return None;
    }

    // Re-check the caller's grant for this tool against the current config.
    let grant = match caller_grants.grant(&caller, &tool) {
        Some(g) => g,
        None => {
            warn!(seq, %caller, %tool, "tool executor: caller no longer granted this tool");
            reply
                .send(messenger, Err(ToolError::NotGranted), alert)
                .await;
            return None;
        }
    };

    // The tool must still be a registered async tool. A fast or unregistered tool
    // reaching here is a config/wiring anomaly; the caller learns a typed error.
    let async_tool = match registry.get(&tool) {
        Some(RegisteredTool::Async(t)) => t.clone(),
        Some(RegisteredTool::Fast(_)) => {
            warn!(seq, %tool, "tool executor: request names a fast tool");
            reply
                .send(messenger, Err(ToolError::WrongClass), alert)
                .await;
            return None;
        }
        None => {
            warn!(seq, %tool, "tool executor: request names an unregistered tool");
            reply
                .send(messenger, Err(ToolError::NotGranted), alert)
                .await;
            return None;
        }
    };

    // Re-check the ACL against the args (config may have tightened since publish).
    if let Err(denied) = async_tool.check_acl(&args, &grant.acl) {
        reply
            .send(messenger, Err(ToolError::Denied(denied.resource)), alert)
            .await;
        return None;
    }

    // Only WASM consumers issue async bus tool calls this cycle; the caller
    // passed the grant lookup above, so it is a configured wasm participant. The
    // scheme is classified through `ParticipantId::kind` rather than a re-spelled
    // `wasm:` prefix. Checked before admission so a bogus caller never holds a
    // permit.
    let caller_pid = ParticipantId::from_stored(caller.clone());
    let caller_slug = match caller_pid.kind() {
        SubscriberKind::Wasm(slug) => slug,
        other => panic!(
            "BUG: tool executor caller {caller:?} is a {other:?}, not a wasm participant \
             (only wasm callers issue async tool calls this cycle)"
        ),
    };

    // Rate-limit admission (delay-not-drop) then the tool's global concurrency
    // permit — the shared guard both caller kinds run through (see
    // `ToolRegistry::admit_async`). Acquired *before* the spawn and moved into it,
    // held until `execute` returns; because this runs serially per tool, only one
    // admission per tool is ever parked here awaiting a permit. A provided tool's
    // admission and execution share the batch's deadline.
    let deadline = registry
        .is_provided(&tool)
        .then(|| *dequeued + PROVIDED_TOOL_CEILING);
    let permit = match deadline {
        None => {
            registry
                .admit_async(&caller, &tool, grant.rate_limit, alert)
                .await
        }
        Some(deadline) => match registry
            .admit_async_by(deadline, &caller, &tool, grant.rate_limit, alert)
            .await
        {
            Ok(permit) => permit,
            Err(e) => {
                reply.send(messenger, Err(e), alert).await;
                return None;
            }
        },
    };

    // A nested provider call acts for the user of the call it serves, read from
    // that call's record now, after the permit, so a record that went away
    // while this request was parked denies it. A nested call to a provided
    // tool also ends no later than the call it serves, so a chain of provided
    // calls never outlives the call that started it.
    let (acting_user, deadline) = match reply.serving.as_deref() {
        None => (None, deadline),
        Some(serving) => match provider.serving(&caller_slug, serving) {
            Some(served) => (
                served.acting_user,
                deadline.map(|own| own.min(served.deadline)),
            ),
            None => {
                drop(permit);
                warn!(seq, %caller, %tool, serving, "tool executor: serving id is not pending for this caller");
                reply
                    .send(
                        messenger,
                        Err(ToolError::Denied(
                            "serving id is not pending for this caller".to_string(),
                        )),
                        alert,
                    )
                    .await;
                return None;
            }
        },
    };

    let ctx = ToolCtx {
        caller: caller_pid,
        grant,
        // Bus/executor calls carry no acting conversation (no self-notification
        // suppression); only LLM-originated calls do.
        acting_conversation_id: None,
        acting_user,
        deadline,
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
        reply.send(&messenger, result, &alert).await;
        drop(permit);
    }))
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
        PublishResult::RateLimited => panic!(
            "BUG: tool executor: the send rate refused a result to {reply_addr:?} — the executor \
             publishes at most one result per request it dequeues from a brenn:tools/<tool> \
             channel, whose rate the caller's own send bucket there already bounded, and the \
             inbox's send rate is the host's TOOL_INBOX_SEND_RATE, set past any such inflow; \
             this is a host bug, not an operator setting"
        ),
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
    serving: Option<&str>,
    result: &Result<Value, ToolError>,
    alert: &AlertDispatcher,
) -> String {
    let envelope = |outcome: Value| {
        let mut body = json!({
            "v": 1,
            "tool": tool,
            "call_id": call_id,
            "outcome": outcome,
        });
        if let Some(serving) = serving {
            body["serving"] = json!(serving);
        }
        serde_json::to_string(&body).expect("result envelope serializes")
    };
    let s = envelope(outcome_json(result));
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
        return envelope(
            json!({ "err": { "kind": "internal", "detail": "result exceeded size cap" } }),
        );
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
    use brenn_envelope::grants::AppCapability;
    use brenn_lib::access::acl::ChannelMatcher;
    use brenn_lib::access::{AppPolicy, GrantSet};
    use brenn_lib::messaging::config::MessagingGlobalConfig;
    use brenn_lib::messaging::{
        ChannelScheme, MessagingDirectory, SubscriberEntry, SubscriberEntryKind, WakeMin,
    };
    use brenn_lib::tools::AclClause;
    use brenn_messaging::query::NoopWakeRouter;
    use brenn_messaging::testutils::test_channel_entry;
    use brenn_messaging::{MountDebt, WakeRouter, config::Depth, config::NoiseLevel};
    use brenn_messaging_store::db::{insert_message, upsert_channels, utc_to_ns};
    use brenn_obs::alerting::{make_capturing_alerter, noop_alert_dispatcher};
    use chrono::Utc;
    use indexmap::IndexMap;

    use super::*;
    use crate::descriptor::{AclDenied, Idempotency, ToolClass, ToolDescriptor};
    use crate::provided::ProvidedTool;
    use crate::provided::tests::{PROVIDER, decl};
    use crate::registry::PROVIDED_ADMISSION_CEILING_DETAIL;
    use crate::tool::{ActingUser, AsyncTool, FastTool};

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
        /// The acting user of every `execute` call, in call order.
        seen_users: Arc<std::sync::Mutex<Vec<Option<ActingUser>>>>,
        /// When set, every `execute` waits for a permit here before answering.
        gate: Option<Arc<tokio::sync::Semaphore>>,
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
                    user_binding: crate::UserBinding::None,
                },
                in_flight: Arc::new(AtomicUsize::new(0)),
                max_in_flight: Arc::new(AtomicUsize::new(0)),
                hold,
                seen_users: Arc::default(),
                gate: None,
            }
        }

        /// Hold every `execute` until `gate` is given a permit.
        fn gated(mut self, gate: &Arc<tokio::sync::Semaphore>) -> Self {
            self.gate = Some(gate.clone());
            self
        }
    }

    #[async_trait]
    impl AsyncTool for StubAsync {
        fn descriptor(&self) -> &ToolDescriptor {
            &self.descriptor
        }
        fn check_acl(&self, args: &Value, acl: &[AclClause]) -> Result<(), AclDenied> {
            crate::testutil::repo_acl_check(args, acl)
        }
        async fn execute(&self, ctx: &ToolCtx, args: Value) -> Result<Value, ToolError> {
            self.seen_users
                .lock()
                .unwrap()
                .push(ctx.acting_user.clone());
            let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_in_flight.fetch_max(now, Ordering::SeqCst);
            if !self.hold.is_zero() {
                tokio::time::sleep(self.hold).await;
            }
            if let Some(gate) = &self.gate {
                let _open = gate.acquire().await.expect("the gate is never closed");
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
                user_binding: crate::UserBinding::None,
            })
        }
    }
    impl FastTool for StubFast {
        fn descriptor(&self) -> &ToolDescriptor {
            &self.0
        }
        fn check_acl(&self, args: &Value, acl: &[AclClause]) -> Result<(), AclDenied> {
            crate::testutil::repo_acl_check(args, acl)
        }
        fn execute(&self, _ctx: &ToolCtx, args: Value) -> Result<Value, ToolError> {
            Ok(args)
        }
    }

    use crate::testutil::{clause, grant};

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
    /// channel, the provided tool's `tools/echo-for-user` and an unregistered
    /// tool's `tools/ghost` (executor subscriber), a `tool-results/sync` inbox
    /// (caller subscriber), the provider's `tool-requests/prov` inbox (provider
    /// subscriber), the executor's system policy, the two wasm policies, and a
    /// provider runtime attached to the messenger.
    struct Harness {
        messenger: Arc<Messenger>,
        tools_uuid: uuid::Uuid,
        provided_uuid: uuid::Uuid,
        ghost_uuid: uuid::Uuid,
        results_uuid: uuid::Uuid,
        provider: Arc<ProviderRuntime>,
    }

    async fn harness() -> Harness {
        let db = brenn_messaging_store::db::init_db_memory();

        let tools_ch = test_channel_entry(
            "tools/apull",
            vec![sub(SubscriberEntryKind::System(
                TOOL_EXECUTOR_COMPONENT.to_string(),
            ))],
        );
        let provided_ch = test_channel_entry(
            "tools/echo-for-user",
            vec![sub(SubscriberEntryKind::System(
                TOOL_EXECUTOR_COMPONENT.to_string(),
            ))],
        );
        let results_ch = test_channel_entry(
            "tool-results/sync",
            vec![sub(SubscriberEntryKind::Wasm(CALLER_SLUG.to_string()))],
        );
        let requests_ch = test_channel_entry(
            &crate::bus_wiring::request_inbox_name(PROVIDER),
            vec![sub(SubscriberEntryKind::Wasm(PROVIDER.to_string()))],
        );
        let ghost_ch = test_channel_entry(
            "tools/ghost",
            vec![sub(SubscriberEntryKind::System(
                TOOL_EXECUTOR_COMPONENT.to_string(),
            ))],
        );
        let tools_uuid = tools_ch.uuid;
        let provided_uuid = provided_ch.uuid;
        let ghost_uuid = ghost_ch.uuid;
        let results_uuid = results_ch.uuid;
        let channels = vec![tools_ch, provided_ch, results_ch, requests_ch, ghost_ch];

        {
            let conn = db.lock().await;
            upsert_channels(&conn, &channels);
        }

        let directory = Arc::new(MessagingDirectory::with_entries(channels.clone()));
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
        wasm_policies.insert(PROVIDER.to_string(), subscribe_policy("tool-requests/"));
        // The production policy, not a hand-rolled one: the executor's delivery
        // read is gated on the same subscribe ACL boot builds for it, so a
        // fixture policy that omitted the `tools/` half would be denied its own
        // request channel.
        let mut system_policies = HashMap::new();
        system_policies.insert(
            TOOL_EXECUTOR_COMPONENT.to_string(),
            crate::bus_wiring::tool_executor_system_policy(),
        );
        let messenger = messenger
            .with_subscriber_registrations(brenn_messaging::testutils::wasm_registrations(
                wasm_policies,
            ))
            .with_subscriber_registrations(brenn_messaging::testutils::system_registrations(
                system_policies,
            ));
        // The caller and the provider read their inboxes through a position,
        // so each holds one from before anything lands.
        for (entry, slug) in [(&channels[2], CALLER_SLUG), (&channels[3], PROVIDER)] {
            brenn_messaging::testutils::attach_wasm_port(
                &messenger,
                entry,
                slug,
                &ParticipantId::for_wasm(slug),
                Depth::Unbounded,
            )
            .await;
        }
        // The executor reads its request channels through a position too,
        // primed at head before any request is inserted.
        for entry in [&channels[0], &channels[1], &channels[4]] {
            messenger
                .attach_subscriber(
                    &entry.address,
                    TOOL_EXECUTOR_COMPONENT,
                    &ParticipantId::for_system(TOOL_EXECUTOR_COMPONENT),
                    Depth::Unbounded,
                )
                .await;
        }
        let (alert, _drain) = noop_alert_dispatcher();
        let provider = ProviderRuntime::new(alert);
        provider.attach(messenger.clone());

        Harness {
            messenger,
            tools_uuid,
            provided_uuid,
            ghost_uuid,
            results_uuid,
            provider,
        }
    }

    /// Commit one request onto `tools/apull` with `reply_to` set to the
    /// `tool-results/sync` inbox. The executor's attached position is what makes
    /// it owed; a commit writes nothing per subscriber.
    async fn insert_request(h: &Harness, body: &str, reply_to_uuid: Option<uuid::Uuid>) {
        insert_request_on(h, h.tools_uuid, body, reply_to_uuid).await;
    }

    /// Commit one request onto the request channel `channel`.
    async fn insert_request_on(
        h: &Harness,
        channel: uuid::Uuid,
        body: &str,
        reply_to_uuid: Option<uuid::Uuid>,
    ) {
        let conn = h.messenger.db().lock().await;
        insert_message(
            &conn,
            channel,
            "test",
            CALLER,
            body,
            Urgency::Normal,
            ChannelScheme::Brenn,
            reply_to_uuid,
            None,
            None,
            None,
            utc_to_ns(Utc::now()),
        );
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
        Arc::new(ToolCallerGrants::new(HashMap::from([(
            CALLER.to_string(),
            per_caller,
        )])))
    }

    /// The grant table is editable while the executor holds it: a caller's set
    /// is installed and dropped whole, and a read answers with what stands now.
    /// A caller with no grants is absent rather than stored empty, so presence
    /// means "may address some tool".
    #[test]
    fn caller_grants_are_installed_and_dropped_whole() {
        let grants = ToolCallerGrants::default();
        assert!(grants.grant(CALLER, "apull").is_none());

        grants.set_caller(
            CALLER.to_string(),
            BTreeMap::from([(
                "apull".to_string(),
                grant(vec![clause(&[("repo", "brenn")])]),
            )]),
        );
        assert!(grants.grant(CALLER, "apull").is_some());
        assert!(
            grants.grant(CALLER, "other").is_none(),
            "a tool the caller was not granted"
        );
        assert!(
            grants.grant("wasm:stranger", "apull").is_none(),
            "a caller the table does not name"
        );

        // Replacing the set is not merging it: the old tool is gone.
        grants.set_caller(
            CALLER.to_string(),
            BTreeMap::from([(
                "other".to_string(),
                grant(vec![clause(&[("repo", "brenn")])]),
            )]),
        );
        assert!(grants.grant(CALLER, "apull").is_none());
        assert!(grants.grant(CALLER, "other").is_some());

        // An empty set removes the caller; removing again is a no-op.
        grants.set_caller(CALLER.to_string(), BTreeMap::new());
        assert!(grants.grant(CALLER, "other").is_none());
        grants.remove_caller(CALLER);
        assert!(grants.grant(CALLER, "other").is_none());
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
            h.provider.clone(),
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
        let rows = brenn_messaging::testutils::owed_everywhere(
            &h.messenger,
            &ParticipantId::for_wasm(CALLER_SLUG),
        )
        .await;
        assert_eq!(rows.len(), 1, "exactly one result row on the caller inbox");
        serde_json::from_str(&rows[0].1.body).expect("result body is JSON")
    }

    #[tokio::test]
    async fn drains_executes_and_delivers_result_and_acks_request() {
        let h = harness().await;
        insert_request(&h, &request_body("c1", "brenn"), Some(h.results_uuid)).await;
        let (alert, _hg) = noop_alert_dispatcher();
        let exec = executor(
            &h,
            registry(StubAsync::new(4, Duration::ZERO)),
            caller_grants(vec![clause(&[("repo", "brenn")])]),
            alert,
        );

        drain_and_join(&exec).await;

        // Request row acked (ack-at-dequeue).
        let remaining = brenn_messaging::testutils::owed_everywhere(
            &h.messenger,
            &ParticipantId::for_system(TOOL_EXECUTOR_COMPONENT),
        )
        .await;
        assert!(
            remaining.is_empty(),
            "the executor's position must be advanced past the request, got {remaining:?}"
        );

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
            Arc::new(ToolCallerGrants::default()),
            alert,
        );

        drain_and_join(&exec).await;

        let result = read_result(&h).await;
        assert_eq!(result["call_id"], "c2");
        assert_eq!(result["outcome"]["err"]["kind"], "not_granted");
    }

    /// The reason the grant table is behind a lock: a request published while
    /// its caller was granted, dequeued after the grant was withdrawn. The
    /// re-check at dequeue is the only thing between a retired consumer's
    /// in-flight request and an execution it is no longer authorized for, so
    /// the stub must never be reached.
    #[tokio::test]
    async fn a_grant_withdrawn_between_publish_and_dequeue_denies() {
        let h = harness().await;
        let grants = caller_grants(vec![clause(&[("repo", "brenn")])]);
        assert!(grants.grant(CALLER, "apull").is_some());
        insert_request(&h, &request_body("c2b", "brenn"), Some(h.results_uuid)).await;

        let (alert, _hg) = noop_alert_dispatcher();
        let stub = StubAsync::new(4, Duration::ZERO);
        let reached = Arc::clone(&stub.max_in_flight);
        let exec = executor(&h, registry(stub), Arc::clone(&grants), alert);

        grants.remove_caller(CALLER);
        drain_and_join(&exec).await;

        let result = read_result(&h).await;
        assert_eq!(result["call_id"], "c2b");
        assert_eq!(result["outcome"]["err"]["kind"], "not_granted");
        assert_eq!(
            reached.load(Ordering::SeqCst),
            0,
            "the tool must never have been invoked"
        );
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

    /// A body without `v` is malformed: the request is answered `invalid_args`
    /// and never executed.
    #[tokio::test]
    async fn a_request_without_a_version_is_malformed() {
        let h = harness().await;
        let mut body: Value = serde_json::from_str(&request_body("c5", "brenn")).unwrap();
        body.as_object_mut().unwrap().remove("v");
        insert_request(&h, &body.to_string(), Some(h.results_uuid)).await;
        let (alert, _hg) = noop_alert_dispatcher();
        let tool = StubAsync::new(4, Duration::ZERO);
        let seen = tool.seen_users.clone();
        let exec = executor(
            &h,
            registry(tool),
            caller_grants(vec![clause(&[("repo", "brenn")])]),
            alert,
        );

        drain_and_join(&exec).await;

        let result = read_result(&h).await;
        assert_eq!(result["call_id"], "c5");
        assert_eq!(result["outcome"]["err"]["kind"], "invalid_args");
        assert_eq!(result["outcome"]["err"]["detail"], "malformed request body");
        assert!(seen.lock().unwrap().is_empty(), "the tool must not run");
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
            &build_result_body("apull", "c5", None, &Ok(json!({})), &alert),
        )
        .await;
        // Nothing landed on any inbox; the drop is silent-but-logged.
        let rows = brenn_messaging::testutils::owed_everywhere(
            &h.messenger,
            &ParticipantId::for_wasm(CALLER_SLUG),
        )
        .await;
        assert!(rows.is_empty(), "no result row for a dropped reply target");
    }

    /// A result inbox's send rate is the host's and set past any inflow, so a
    /// refusal is a host bug: a hand-built inbox with a one-token rate makes
    /// the second result panic, naming the invariant rather than the policy.
    #[tokio::test]
    #[should_panic(expected = "BUG: tool executor: the send rate refused a result to \
                               \"brenn:tool-results/sync\"")]
    async fn a_rate_refused_result_is_a_host_bug() {
        use brenn_lib::messaging::config::SendRate;

        let db = brenn_messaging_store::db::init_db_memory();
        let mut inbox = test_channel_entry("tool-results/sync", vec![]);
        inbox.resolved_channel.send_rate = SendRate {
            burst: 1,
            refill_interval_secs: 3600,
            refill: 1,
        };
        {
            let conn = db.lock().await;
            upsert_channels(&conn, std::slice::from_ref(&inbox));
        }
        let messenger = Messenger::new(
            db,
            Arc::new(MessagingDirectory::with_entries(vec![inbox])),
            Arc::from("test"),
            Arc::new(IndexMap::new()),
            Arc::new(NoopWakeRouter) as Arc<dyn WakeRouter>,
            MessagingGlobalConfig::default(),
        )
        .with_subscriber_registrations(brenn_messaging::testutils::system_registrations(
            HashMap::from([(
                TOOL_EXECUTOR_COMPONENT.to_string(),
                crate::bus_wiring::tool_executor_system_policy(),
            )]),
        ));
        let (alert, _hg) = noop_alert_dispatcher();
        let body = build_result_body("apull", "r1", None, &Ok(json!({})), &alert);
        publish_result(&messenger, "brenn:tool-results/sync", &body).await;
        publish_result(&messenger, "brenn:tool-results/sync", &body).await;
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
        let rows = brenn_messaging::testutils::owed_everywhere(
            &h.messenger,
            &ParticipantId::for_wasm(CALLER_SLUG),
        )
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
        let rows = brenn_messaging::testutils::owed_everywhere(
            &h.messenger,
            &ParticipantId::for_wasm(CALLER_SLUG),
        )
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
        let body = build_result_body("apull", "c7", None, &Ok(json!({ "blob": big })), &alert);
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
    async fn envelope_around_a_result_at_the_tool_budget_is_under_the_cap() {
        // A tool bounding its serialized value to
        // `MAX_ASYNC_RESULT_BYTES - ENVELOPE_MARGIN` never trips the over-cap
        // substitution, even under a maximal call id and a long tool name.
        use crate::descriptor::ENVELOPE_MARGIN;
        let budget = MAX_ASYNC_RESULT_BYTES - ENVELOPE_MARGIN;
        let skeleton = serde_json::to_string(&json!({ "body": "" })).unwrap().len();
        let value = json!({ "body": "x".repeat(budget - skeleton) });
        assert_eq!(serde_json::to_string(&value).unwrap().len(), budget);

        let (alert, captured, handle) = make_capturing_alerter();
        let call_id = "c".repeat(128);
        let tool = "a-tool-name-of-generous-length-for-the-envelope-test";
        let body = build_result_body(tool, &call_id, None, &Ok(value.clone()), &alert);
        assert!(
            body.len() <= MAX_ASYNC_RESULT_BYTES,
            "envelope {} over cap",
            body.len()
        );
        let v: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(
            v["outcome"]["ok"], value,
            "the value is delivered, not substituted"
        );

        alert.flush().await;
        drop(alert);
        handle.await.unwrap();
        assert!(captured.lock().unwrap().is_empty(), "no over-cap alert");
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
        insert_request_on(&h, h.ghost_uuid, &body, Some(h.results_uuid)).await;
        let (alert, _hg) = noop_alert_dispatcher();
        // Grant keyed on the ghost tool so the grant lookup passes; the registry
        // only knows "apull", so `registry.get("ghost")` is None.
        let mut per_caller = BTreeMap::new();
        per_caller.insert(
            "ghost".to_string(),
            grant(vec![clause(&[("repo", "brenn")])]),
        );
        let exec = executor(
            &h,
            registry(StubAsync::new(4, Duration::ZERO)),
            Arc::new(ToolCallerGrants::new(HashMap::from([(
                CALLER.to_string(),
                per_caller,
            )]))),
            alert,
        );

        drain_and_join(&exec).await;

        let result = read_result(&h).await;
        assert_eq!(result["call_id"], "c9");
        assert_eq!(result["outcome"]["err"]["kind"], "not_granted");
    }

    /// Dequeue the executor's one pending request and admit it on its own
    /// task, returning the message it panicked with.
    async fn admission_panic(exec: &ToolExecutor) -> String {
        let mut batch = exec.inbox.dequeue_batch().await;
        assert_eq!(batch.len(), 1, "one pending request");
        let (seq, env) = batch.pop().expect("one pending request");
        let cx = exec.shared.batch_ctx();
        let joined =
            tokio::spawn(async move { admit_and_dispatch(&cx, seq, env).await.is_some() }).await;
        *joined
            .expect_err("admission panics")
            .into_panic()
            .downcast::<String>()
            .expect("a formatted panic message")
    }

    /// A request whose body names a tool other than its channel's panics
    /// before admission: the tool never runs and nothing answers.
    #[tokio::test]
    async fn a_request_naming_another_channels_tool_panics_before_admission() {
        let h = harness().await;
        insert_request_on(
            &h,
            h.ghost_uuid,
            &request_body("c1", "brenn"),
            Some(h.results_uuid),
        )
        .await;
        let (alert, _hg) = noop_alert_dispatcher();
        let stub = StubAsync::new(4, Duration::ZERO);
        let seen = stub.seen_users.clone();
        let exec = executor(&h, registry(stub), caller_grants(vec![]), alert);

        let message = admission_panic(&exec).await;

        assert!(message.contains("names tool \"apull\""), "{message}");
        assert!(seen.lock().unwrap().is_empty(), "the tool never ran");
        assert!(results(&h).await.is_empty(), "nothing answers");
    }

    /// A provided tool's request on a native tool's channel would run in the
    /// awaited regime and hold the drain; it panics before admission and never
    /// reaches the provider.
    #[tokio::test]
    async fn a_provided_request_on_a_native_channel_panics_before_admission() {
        let h = harness().await;
        insert_request(&h, &provided_body("c1"), Some(h.results_uuid)).await;
        let (alert, _hg) = noop_alert_dispatcher();
        let reg = registry_with_provided(&h, StubAsync::new(4, Duration::ZERO), 1);
        let exec = executor(&h, reg, grants_with_provided(), alert);

        let message = admission_panic(&exec).await;

        assert!(
            message.contains("names tool \"echo-for-user\""),
            "{message}"
        );
        assert!(
            crate::provided::tests::requests(&h.messenger)
                .await
                .is_empty(),
            "nothing reaches the provider"
        );
        assert!(results(&h).await.is_empty(), "nothing answers");
    }

    #[tokio::test]
    async fn result_row_activates_on_tool_results_port_and_is_not_retired() {
        use crate::bus_wiring::inbox_input_port;
        use brenn_envelope::addressing::TOOL_RESULT_INPUT_PORT;
        use brenn_lib::messaging::config::{ResolvedChannel, SendRate, Sink};

        // Publish a result to the caller's inbox exactly as the executor does.
        let h = harness().await;
        let (alert, _hg) = noop_alert_dispatcher();
        publish_result(
            &h.messenger,
            "brenn:tool-results/sync",
            &build_result_body(
                "apull",
                "r1",
                None,
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
        // Nothing here tunes the inbox; an unbounded window keeps every pending
        // row in the snapshot.
        let unbounded_inbox = ResolvedChannel {
            push_depth: Depth::Unbounded,
            retain_depth: Depth::Unbounded,
            standing_retain_depth: Depth::Unbounded,
            noise: NoiseLevel::Silent,
            sink: Sink::Drop,
            wake_min: WakeMin::Normal,
            send_rate: SendRate::default(),
        };
        let (_wake, snapshot) = h
            .messenger
            .load_activation_snapshot(
                &caller,
                &[inbox_input_port(CALLER_SLUG, &unbounded_inbox)],
                MountDebt::Settled,
            )
            .await
            .expect("a pending inbox row on a triggering input port yields an activation");
        let port = snapshot
            .iter()
            .find(|p| p.port == TOOL_RESULT_INPUT_PORT)
            .expect("the tool-results port is present in the activation");
        assert_eq!(port.channel_address, "brenn:tool-results/sync");
        assert_eq!(port.new_len(), 1, "the one result message is delivered");
        let body: Value =
            serde_json::from_str(&port.new_entries()[0].1.body).expect("result body is JSON");
        assert_eq!(body["call_id"], "r1");
        assert_eq!(body["outcome"]["ok"]["echoed"]["repo"], "brenn");
    }

    fn alice() -> ActingUser {
        ActingUser {
            id: 1,
            username: "alice".to_string(),
        }
    }

    /// `request_body` for a provider's nested call serving `serving`.
    fn serving_body(call_id: &str, serving: &str) -> String {
        let mut body: Value = serde_json::from_str(&request_body(call_id, "brenn")).unwrap();
        body["serving"] = json!(serving);
        body.to_string()
    }

    fn provided_body(call_id: &str) -> String {
        json!({
            "v": 1,
            "tool": "echo-for-user",
            "call_id": call_id,
            "caller": CALLER,
            "args": { "q": call_id },
        })
        .to_string()
    }

    /// The stub `apull` beside the provided `echo-for-user` of `PROVIDER`.
    fn registry_with_provided(
        h: &Harness,
        stub: StubAsync,
        max_concurrency: u32,
    ) -> Arc<ToolRegistry> {
        Arc::new(ToolRegistry::with_provided(
            vec![RegisteredTool::Async(Arc::new(stub))],
            vec![Arc::new(ProvidedTool::new(
                PROVIDER,
                &decl(max_concurrency),
                h.provider.clone(),
            ))],
        ))
    }

    /// The caller granted both `apull` (for `repo = brenn`) and `echo-for-user`.
    fn grants_with_provided() -> Arc<ToolCallerGrants> {
        let acl = vec![clause(&[("repo", "brenn")])];
        Arc::new(ToolCallerGrants::new(HashMap::from([(
            CALLER.to_string(),
            BTreeMap::from([
                ("apull".to_string(), grant(acl.clone())),
                ("echo-for-user".to_string(), grant(acl)),
            ]),
        )])))
    }

    /// Every result row on the caller's inbox so far, decoded.
    async fn results(h: &Harness) -> Vec<Value> {
        brenn_messaging::testutils::owed_everywhere(
            &h.messenger,
            &ParticipantId::for_wasm(CALLER_SLUG),
        )
        .await
        .iter()
        .map(|(_, env)| serde_json::from_str(&env.body).expect("result body is JSON"))
        .collect()
    }

    /// The caller's results once there are `n`, yielding to the detached tasks
    /// that produce them.
    async fn results_once(h: &Harness, n: usize) -> Vec<Value> {
        for _ in 0..1000 {
            let rows = results(h).await;
            if rows.len() >= n {
                assert_eq!(rows.len(), n, "exactly {n} results: {rows:?}");
                return rows;
            }
            tokio::task::yield_now().await;
        }
        panic!("fewer than {n} results arrived: {:?}", results(h).await);
    }

    /// A nested call acts for the user on the record of the call it serves,
    /// not for no one, and its result names the call it serves; a plain call's
    /// result carries no `serving`.
    #[tokio::test]
    async fn a_serving_request_acts_for_the_pending_calls_user_and_echoes_serving() {
        let h = harness().await;
        let outer = h.provider.begin(
            CALLER_SLUG,
            "echo-for-user",
            &crate::provided::tests::pending_ctx(Some(alice())),
        );
        insert_request(
            &h,
            &serving_body("n1", outer.id().as_str()),
            Some(h.results_uuid),
        )
        .await;
        insert_request(&h, &request_body("p1", "brenn"), Some(h.results_uuid)).await;
        let (alert, _hg) = noop_alert_dispatcher();
        let stub = StubAsync::new(4, Duration::ZERO);
        let seen = Arc::clone(&stub.seen_users);
        let exec = executor(
            &h,
            registry(stub),
            caller_grants(vec![clause(&[("repo", "brenn")])]),
            alert,
        );

        drain_and_join(&exec).await;

        let rows = results(&h).await;
        let nested = rows
            .iter()
            .find(|r| r["call_id"] == "n1")
            .expect("nested result");
        assert_eq!(nested["outcome"]["ok"]["echoed"]["repo"], "brenn");
        assert_eq!(nested["serving"], outer.id().as_str());
        let plain = rows
            .iter()
            .find(|r| r["call_id"] == "p1")
            .expect("plain result");
        assert!(
            plain.get("serving").is_none(),
            "a plain call echoes no serving: {plain}"
        );
        let mut seen = seen.lock().unwrap().clone();
        seen.sort_by_key(|u| u.as_ref().map(|u| u.id));
        assert_eq!(seen, vec![None, Some(alice())]);
    }

    /// An id pending for a different instance is, to this caller, not pending:
    /// the nested call is denied and its tool never runs.
    #[tokio::test]
    async fn a_serving_id_not_pending_for_the_caller_is_denied() {
        let h = harness().await;
        let theirs = h.provider.begin(
            "other",
            "echo-for-user",
            &crate::provided::tests::pending_ctx(Some(alice())),
        );
        insert_request(
            &h,
            &serving_body("n2", theirs.id().as_str()),
            Some(h.results_uuid),
        )
        .await;
        let (alert, _hg) = noop_alert_dispatcher();
        let stub = StubAsync::new(4, Duration::ZERO);
        let seen = Arc::clone(&stub.seen_users);
        let exec = executor(
            &h,
            registry(stub),
            caller_grants(vec![clause(&[("repo", "brenn")])]),
            alert,
        );

        drain_and_join(&exec).await;

        let result = read_result(&h).await;
        assert_eq!(result["outcome"]["err"]["kind"], "denied");
        assert_eq!(
            result["outcome"]["err"]["detail"],
            "serving id is not pending for this caller"
        );
        assert_eq!(result["serving"], theirs.id().as_str());
        assert!(seen.lock().unwrap().is_empty(), "the tool never ran");
    }

    /// The user is read after the permit: a nested call parked behind a busy
    /// permit whose served call ends meanwhile is denied when the permit frees,
    /// and its tool never runs.
    #[tokio::test]
    async fn a_nested_call_whose_record_ends_while_parked_is_denied_without_running() {
        let h = harness().await;
        let outer = h.provider.begin(
            CALLER_SLUG,
            "echo-for-user",
            &crate::provided::tests::pending_ctx(Some(alice())),
        );
        insert_request(&h, &request_body("busy", "brenn"), Some(h.results_uuid)).await;
        insert_request(
            &h,
            &serving_body("parked", outer.id().as_str()),
            Some(h.results_uuid),
        )
        .await;
        let (alert, _hg) = noop_alert_dispatcher();
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let stub = StubAsync::new(1, Duration::ZERO).gated(&gate);
        let seen = Arc::clone(&stub.seen_users);
        let exec = Arc::new(executor(
            &h,
            registry(stub),
            caller_grants(vec![clause(&[("repo", "brenn")])]),
            alert,
        ));

        let drain = tokio::spawn({
            let exec = Arc::clone(&exec);
            async move { exec.drain_step().await }
        });
        // `busy` holds the one permit inside `execute`; `parked` waits for it.
        while seen.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
        drop(outer);
        gate.add_permits(1);
        drain.await.unwrap();

        let rows = results(&h).await;
        let parked = rows
            .iter()
            .find(|r| r["call_id"] == "parked")
            .expect("parked result");
        assert_eq!(parked["outcome"]["err"]["kind"], "denied");
        assert_eq!(seen.lock().unwrap().len(), 1, "only `busy` ran");
    }

    /// The drain does not wait on a provided tool's execution: with a provided
    /// call outstanding, the next batch's native request runs and answers, and
    /// the provided call answers when its provider completes it.
    #[tokio::test]
    async fn a_provided_execution_does_not_hold_the_drain() {
        let h = harness().await;
        insert_request_on(
            &h,
            h.provided_uuid,
            &provided_body("prov1"),
            Some(h.results_uuid),
        )
        .await;
        let (alert, _hg) = noop_alert_dispatcher();
        let reg = registry_with_provided(&h, StubAsync::new(4, Duration::ZERO), 1);
        let exec = executor(&h, reg, grants_with_provided(), alert);

        tokio::time::timeout(Duration::from_secs(5), exec.drain_step())
            .await
            .expect("the drain returns with the provided call outstanding");
        let id = crate::provided::tests::published_id(&h.messenger).await;
        assert!(
            results(&h).await.is_empty(),
            "the provided call is still pending"
        );

        insert_request(&h, &request_body("native1", "brenn"), Some(h.results_uuid)).await;
        exec.drain_step().await;
        let rows = results(&h).await;
        assert_eq!(rows.len(), 1, "{rows:?}");
        assert_eq!(rows[0]["call_id"], "native1");

        h.provider
            .complete(PROVIDER, &id, Ok(json!({ "answered": true })))
            .expect("the provided call is pending");
        let rows = results_once(&h, 2).await;
        let provided = rows
            .iter()
            .find(|r| r["call_id"] == "prov1")
            .expect("provided result");
        assert_eq!(provided["outcome"]["ok"]["answered"], true);
        assert!(provided.get("serving").is_none());
    }

    /// A nested call to a provided tool ends with the call it serves: served
    /// with ten seconds left, it is answered at that call's deadline, not its
    /// own batch's, and its own record is gone with it.
    #[tokio::test(start_paused = true)]
    async fn a_nested_provided_call_ends_by_the_deadline_of_the_call_it_serves() {
        let h = harness().await;
        let served = h.provider.begin(
            CALLER_SLUG,
            "echo-for-user",
            &crate::provided::tests::ctx(
                Some(alice()),
                Some(tokio::time::Instant::now() + Duration::from_secs(10)),
            ),
        );
        let mut body: Value = serde_json::from_str(&provided_body("nested")).unwrap();
        body["serving"] = json!(served.id().as_str());
        insert_request_on(&h, h.provided_uuid, &body.to_string(), Some(h.results_uuid)).await;
        let (alert, _hg) = noop_alert_dispatcher();
        let reg = registry_with_provided(&h, StubAsync::new(4, Duration::ZERO), 1);
        let exec = executor(&h, reg, grants_with_provided(), alert);

        exec.drain_step().await;
        let nested = crate::provided::tests::published_id(&h.messenger).await;
        assert_eq!(
            h.provider.acting_user_for(PROVIDER, &nested),
            Some(Some(alice()))
        );
        tokio::time::sleep(Duration::from_secs(9)).await;
        assert!(results(&h).await.is_empty(), "nothing answers before 10 s");
        tokio::time::sleep(Duration::from_secs(2)).await;

        let rows = results_once(&h, 1).await;
        assert_eq!(
            rows[0]["outcome"]["err"]["detail"],
            "provider did not complete within the ceiling"
        );
        assert_eq!(rows[0]["serving"], served.id().as_str());
        assert_eq!(h.provider.acting_user_for(PROVIDER, &nested), None);
    }

    /// One deadline per batch, from its dequeue: with the provider silent and
    /// one permit, the executing request is answered by the tool at the
    /// ceiling and the parked one is answered by admission at the same
    /// instant, never handed to the provider.
    #[tokio::test(start_paused = true)]
    async fn a_provided_batchs_parked_admission_expires_at_the_batch_deadline() {
        let h = harness().await;
        insert_request_on(
            &h,
            h.provided_uuid,
            &provided_body("first"),
            Some(h.results_uuid),
        )
        .await;
        insert_request_on(
            &h,
            h.provided_uuid,
            &provided_body("second"),
            Some(h.results_uuid),
        )
        .await;
        let (alert, _hg) = noop_alert_dispatcher();
        let reg = registry_with_provided(&h, StubAsync::new(4, Duration::ZERO), 1);
        let exec = executor(&h, reg, grants_with_provided(), alert);

        exec.drain_step().await;
        tokio::time::sleep(PROVIDED_TOOL_CEILING - Duration::from_secs(1)).await;
        assert!(
            results(&h).await.is_empty(),
            "nothing answers before the deadline"
        );
        tokio::time::sleep(Duration::from_secs(2)).await;

        let rows = results_once(&h, 2).await;
        let detail = |call_id: &str| {
            rows.iter()
                .find(|r| r["call_id"] == call_id)
                .expect("a result")["outcome"]["err"]["detail"]
                .clone()
        };
        assert_eq!(
            detail("first"),
            "provider did not complete within the ceiling"
        );
        assert_eq!(detail("second"), PROVIDED_ADMISSION_CEILING_DETAIL);
        assert_eq!(
            crate::provided::tests::requests(&h.messenger).await.len(),
            1,
            "the parked request never reached the provider"
        );
    }
}
