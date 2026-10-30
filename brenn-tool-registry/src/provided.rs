//! Component-provided tools: a registry tool whose `execute` hands the call to
//! a WASM instance and awaits that instance's answer.
//!
//! [`ProvidedTool`] is an ordinary [`AsyncTool`], so the bus executor and
//! `invoke_for_llm` admit it, stamp it and log it exactly as any async tool.
//! Its `execute` records the call in the process's [`ProviderRuntime`] under a
//! host-minted [`PendingId`], publishes the request to the providing instance's
//! own inbox `brenn:tool-requests/<slug>`, and awaits a completion until the
//! call's deadline. The request body names no caller and no user: the acting
//! user stays on the host-side record, where a nested call made on the
//! request's behalf reads it.
//!
//! A pending record lives exactly as long as the `execute` future that minted
//! it: the [`PendingGuard`] owns the record and removes it on drop, whether the
//! call completed, failed, reached its deadline, or was cancelled by its
//! caller. No id outlives its waiter.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use async_trait::async_trait;
use brenn_lib::mcp_tool_names::MCP_BRENN_PREFIX;
use brenn_lib::messaging::config::ProvidedToolDecl;
use brenn_lib::messaging::{ParticipantId, Urgency, canonical_address};
use brenn_lib::tools::AclClause;
use brenn_messaging::{Messenger, PublishResult};
use brenn_obs::alerting::{AlertDispatcher, AlertSeverity};
use rand::RngExt;
use serde_json::{Value, json};
use tokio::sync::oneshot;
use tokio::time::Instant;
use tracing::{debug, info};

use super::bus_wiring::request_inbox_name;
use super::descriptor::{
    AclDenied, ENVELOPE_MARGIN, Idempotency, MAX_ASYNC_RESULT_BYTES, ToolClass, ToolDescriptor,
    ToolError, UserBinding,
};
use super::executor::TOOL_EXECUTOR_COMPONENT;
use super::tool::{ActingUser, AsyncTool, ToolCtx};

/// The largest serialized `ok` value a provider may complete with: the async
/// result cap less the executor's envelope margin, so a completed value never
/// trips the executor's over-cap substitution.
pub const PROVIDED_RESULT_BUDGET: usize = MAX_ASYNC_RESULT_BYTES - ENVELOPE_MARGIN;

/// A pending provided-tool call's id: 16 CSPRNG bytes as 32 lowercase hex
/// characters. Host-minted and never derived from anything a caller chose.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PendingId(String);

impl PendingId {
    fn mint() -> Self {
        let mut bytes = [0u8; 16];
        rand::rng().fill(&mut bytes);
        Self(hex::encode(bytes))
    }

    /// The id as it travels in a request body.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for PendingId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Why a completion was not accepted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompleteError {
    /// The id is not pending for this instance: completed, failed, expired,
    /// abandoned by its caller, from before a restart, or another instance's.
    /// Indistinguishable on purpose.
    NotPending,
    /// The outcome is one a provider may not give — an admission error kind,
    /// or an `ok` value or error detail over [`PROVIDED_RESULT_BUDGET`]. The id
    /// stays pending.
    InvalidOutcome(String),
}

struct PendingCall {
    /// The providing instance's consumer slug.
    instance: String,
    tool: &'static str,
    caller: ParticipantId,
    acting_user: Option<ActingUser>,
    /// The call's deadline; the waiter answers for it then at the latest.
    deadline: Instant,
    completion: oneshot::Sender<Result<Value, ToolError>>,
}

/// What a nested call serving a pending call inherits from it: the acting
/// user and the deadline by which the served call is answered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Serving {
    pub acting_user: Option<ActingUser>,
    pub deadline: Instant,
}

/// The process's table of pending provided-tool calls, shared by every
/// [`ProvidedTool`], every provider host and the executor.
pub struct ProviderRuntime {
    pending: Mutex<HashMap<String, PendingCall>>,
    messenger: OnceLock<Arc<Messenger>>,
    alert: AlertDispatcher,
}

impl ProviderRuntime {
    /// An empty table. The messenger is attached once messaging is built.
    pub fn new(alert: AlertDispatcher) -> Arc<Self> {
        Arc::new(Self {
            pending: Mutex::new(HashMap::new()),
            messenger: OnceLock::new(),
            alert,
        })
    }

    /// Attach the messenger requests are published through.
    ///
    /// # Panics
    ///
    /// Panics if a messenger is already attached: there is one per process.
    pub fn attach(&self, messenger: Arc<Messenger>) {
        if self.messenger.set(messenger).is_err() {
            panic!("BUG: the provider runtime's messenger is attached twice");
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, PendingCall>> {
        self.pending
            .lock()
            .expect("provider runtime pending table lock poisoned")
    }

    /// Record a call to `tool` provided by `instance` under a fresh id. The
    /// record is removed when the returned guard drops, if nothing removed it
    /// first.
    ///
    /// # Panics
    ///
    /// Panics if `ctx` carries no deadline: every provided call is admitted
    /// with one.
    pub fn begin(
        self: &Arc<Self>,
        instance: &str,
        tool: &'static str,
        ctx: &ToolCtx,
    ) -> PendingGuard {
        let deadline = ctx.deadline.unwrap_or_else(|| {
            panic!("BUG: a call to provided tool {tool:?} began without a deadline")
        });
        let id = PendingId::mint();
        let (completion, rx) = oneshot::channel();
        let previous = self.lock().insert(
            id.0.clone(),
            PendingCall {
                instance: instance.to_string(),
                tool,
                caller: ctx.caller.clone(),
                acting_user: ctx.acting_user.clone(),
                deadline,
                completion,
            },
        );
        assert!(previous.is_none(), "BUG: a minted pending id {id} collided");
        PendingGuard {
            id,
            runtime: self.clone(),
            rx,
        }
    }

    /// Answer the call `id` with `outcome`, iff it is pending for `instance`.
    /// The record is consumed: a second completion is `NotPending`.
    ///
    /// An `outcome` a provider may not give leaves the record pending and is
    /// refused as `InvalidOutcome`, so the provider can answer again.
    pub fn complete(
        &self,
        instance: &str,
        id: &str,
        outcome: Result<Value, ToolError>,
    ) -> Result<(), CompleteError> {
        let mut pending = self.lock();
        match pending.get(id) {
            Some(record) if record.instance == instance => {}
            _ => return Err(CompleteError::NotPending),
        }
        provider_outcome_admissible(&outcome).map_err(CompleteError::InvalidOutcome)?;
        let record = pending.remove(id).expect("record present under the lock");
        drop(pending);
        deliver(id, record, outcome);
        Ok(())
    }

    /// The acting user of the call `id` when it is pending for `instance`:
    /// `None` when it is not, `Some(None)` when it is and carries no user.
    pub fn acting_user_for(&self, instance: &str, id: &str) -> Option<Option<ActingUser>> {
        self.serving(instance, id)
            .map(|serving| serving.acting_user)
    }

    /// What a nested call serving `id` inherits, when `id` is pending for
    /// `instance`.
    pub fn serving(&self, instance: &str, id: &str) -> Option<Serving> {
        self.lock()
            .get(id)
            .filter(|record| record.instance == instance)
            .map(|record| Serving {
                acting_user: record.acting_user.clone(),
                deadline: record.deadline,
            })
    }

    /// Fail every listed id still pending for `instance` with
    /// `Internal(reason)`. Ids already answered, or another instance's, are
    /// left alone.
    pub fn fail_delivered(&self, instance: &str, ids: &[String], reason: &str) {
        let mut failed = Vec::new();
        {
            let mut pending = self.lock();
            for id in ids {
                if pending
                    .get(id.as_str())
                    .is_some_and(|record| record.instance == instance)
                {
                    let record = pending.remove(id.as_str()).expect("present under the lock");
                    failed.push((id, record));
                }
            }
        }
        for (id, record) in failed {
            deliver(id, record, Err(ToolError::Internal(reason.to_string())));
        }
    }

    fn messenger(&self) -> &Arc<Messenger> {
        self.messenger.get().expect(
            "BUG: a provided tool executed before the provider runtime's messenger was attached",
        )
    }
}

/// Send `outcome` to the record's waiter. The waiter may have gone between the
/// record's removal and this send (its guard found nothing to remove); the
/// answer then has no one to reach, which is the caller's own choice.
fn deliver(id: &str, record: PendingCall, outcome: Result<Value, ToolError>) {
    let PendingCall {
        instance,
        tool,
        caller,
        completion,
        ..
    } = record;
    if completion.send(outcome).is_err() {
        debug!(%instance, %tool, caller = %caller.as_str(), id, "provided tool: waiter gone before its answer arrived");
    }
}

/// Whether a provider may answer with `outcome`: the error kinds a tool's own
/// `execute` produces, with the answer — the `ok` value or the error's detail,
/// serialized — within [`PROVIDED_RESULT_BUDGET`].
fn provider_outcome_admissible(outcome: &Result<Value, ToolError>) -> Result<(), String> {
    let (what, len) = match outcome {
        Ok(value) => (
            "result",
            serde_json::to_string(value)
                .expect("a JSON value serializes")
                .len(),
        ),
        Err(
            ToolError::Denied(detail)
            | ToolError::InvalidArgs(detail)
            | ToolError::Internal(detail),
        ) => (
            "error detail",
            serde_json::to_string(detail)
                .expect("a string serializes")
                .len(),
        ),
        Err(e @ (ToolError::NotGranted | ToolError::RateLimited | ToolError::WrongClass)) => {
            return Err(format!(
                "error kind {:?} is the host's admission answer, not a provider's",
                e.kind_str()
            ));
        }
    };
    if len > PROVIDED_RESULT_BUDGET {
        return Err(format!(
            "the {what} is {len} bytes; a provided tool's answer may be at most \
             {PROVIDED_RESULT_BUDGET}"
        ));
    }
    Ok(())
}

/// The one owner of a pending record. Dropping it removes the record if a
/// completion or a delivered failure has not.
pub struct PendingGuard {
    id: PendingId,
    runtime: Arc<ProviderRuntime>,
    rx: oneshot::Receiver<Result<Value, ToolError>>,
}

impl PendingGuard {
    /// The id this guard's record is pending under.
    pub fn id(&self) -> &PendingId {
        &self.id
    }
}

impl Drop for PendingGuard {
    fn drop(&mut self) {
        self.runtime.lock().remove(self.id.as_str());
    }
}

/// A tool a WASM instance provides, registered beside the native tools.
pub struct ProvidedTool {
    descriptor: ToolDescriptor,
    instance: String,
    runtime: Arc<ProviderRuntime>,
}

impl ProvidedTool {
    /// The tool `decl` declares, provided by the consumer `instance`.
    ///
    /// The descriptor's strings are leaked: the registry is built once per
    /// process and a provided tool changes only across a restart, so they live
    /// exactly as long as the registry that holds them.
    pub fn new(instance: &str, decl: &ProvidedToolDecl, runtime: Arc<ProviderRuntime>) -> Self {
        let leak = |s: String| -> &'static str { Box::leak(s.into_boxed_str()) };
        Self {
            descriptor: ToolDescriptor {
                name: leak(decl.name.clone()),
                mcp_name: leak(format!("{MCP_BRENN_PREFIX}{}", decl.mcp_name)),
                description: leak(decl.description.clone()),
                input_schema: decl.input_schema.clone(),
                class: ToolClass::Async {
                    max_concurrency: usize::try_from(decl.max_concurrency)
                        .expect("a u32 fits a usize"),
                },
                acl_keys: &[],
                idempotency: Idempotency::Natural,
                auto_approve: decl.auto_approve,
                user_binding: UserBinding::ActingOrLentUser,
            },
            instance: instance.to_string(),
            runtime,
        }
    }

    /// The consumer slug of the instance that provides this tool.
    pub fn instance(&self) -> &str {
        &self.instance
    }
}

#[async_trait]
impl AsyncTool for ProvidedTool {
    fn descriptor(&self) -> &ToolDescriptor {
        &self.descriptor
    }

    fn check_acl(&self, _args: &Value, _acl: &[AclClause]) -> Result<(), AclDenied> {
        Ok(())
    }

    async fn execute(&self, ctx: &ToolCtx, args: Value) -> Result<Value, ToolError> {
        let name = self.descriptor.name;
        let deadline = ctx
            .deadline
            .unwrap_or_else(|| panic!("BUG: provided tool {name:?} admitted without a deadline"));
        let started = Instant::now();
        let mut guard = self.runtime.begin(&self.instance, name, ctx);

        let address = canonical_address(&request_inbox_name(&self.instance));
        let body =
            json!({ "v": 1, "id": guard.id().as_str(), "tool": name, "args": args }).to_string();
        match self
            .runtime
            .messenger()
            .publish_from_system(
                TOOL_EXECUTOR_COMPONENT,
                &address,
                &body,
                Urgency::Normal,
                None,
            )
            .await
        {
            PublishResult::Ok { .. } => {}
            // The caller's arguments made the request larger than the bus
            // carries. Dropping the guard on return ends the record.
            PublishResult::BodyTooLarge { len, max } => {
                return Err(ToolError::InvalidArgs(format!(
                    "the request to the provider is {len} bytes, over the bus body limit of \
                     {max} bytes"
                )));
            }
            // The executor's send-rate bucket on this inbox is empty: a limit,
            // answered as one.
            PublishResult::RateLimited => return Err(ToolError::RateLimited),
            other @ (PublishResult::UnknownChannel(_)
            | PublishResult::MalformedAddress(_)
            | PublishResult::MissingSender
            | PublishResult::AclDenied(_)
            | PublishResult::BudgetExhausted
            | PublishResult::DeferredQuotaExceeded { .. }
            | PublishResult::ImpetusUnauthorized) => panic!(
                "provided tool {name:?}: publishing its request to {address:?} failed ({other:?}) \
                 — host-wiring invariant violated: a providing instance's request inbox exists \
                 for the process's lifetime, the {TOOL_EXECUTOR_COMPONENT} system policy \
                 grants publish on it, and a system publish with no impetus, no reply_to and \
                 no deliver_after draws no budget and no deferred quota"
            ),
        }

        let result = match tokio::time::timeout_at(deadline, &mut guard.rx).await {
            Ok(Ok(outcome)) => outcome,
            Ok(Err(_)) => panic!(
                "BUG: provided tool {name:?}: the pending record of {} was removed without an answer \
                 while its guard was alive",
                guard.id()
            ),
            Err(_elapsed) => {
                self.runtime.alert.alert_once_per_process(
                    AlertSeverity::Warning,
                    format!("Provided tool {name:?} did not complete within its ceiling"),
                    &format!("tool:{name}:provider-ceiling"),
                    format!(
                        "A call to tool {name:?}, provided by [[wasm_consumer]] {:?}, reached its \
                         deadline before the provider completed it. The caller was answered \
                         `internal`.",
                        self.instance
                    ),
                );
                Err(ToolError::Internal(
                    "provider did not complete within the ceiling".to_string(),
                ))
            }
        };
        info!(
            tool = %name,
            instance = %self.instance,
            id = %guard.id(),
            elapsed = ?started.elapsed(),
            "tool invocation (provided)",
        );
        result
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::{BTreeMap, HashMap};
    use std::time::Duration;

    use brenn_envelope::grants::AppCapability;
    use brenn_lib::access::acl::{AclSet, ChannelMatcher};
    use brenn_lib::access::{AppPolicy, GrantSet};
    use brenn_lib::messaging::config::{Depth, MessagingGlobalConfig, NoiseLevel, SendRate};
    use brenn_lib::messaging::{MessagingDirectory, SubscriberEntry, SubscriberEntryKind};
    use brenn_lib::tools::ResolvedToolGrant;
    use brenn_messaging::WakeRouter;
    use brenn_messaging::query::NoopWakeRouter;
    use brenn_messaging::testutils::test_channel_entry;
    use brenn_obs::alerting::{make_capturing_alerter, noop_alert_dispatcher};
    use indexmap::IndexMap;

    use super::*;

    pub(crate) const PROVIDER: &str = "prov";

    /// A declaration of `echo-for-user` with the given concurrency.
    pub(crate) fn decl(max_concurrency: u32) -> ProvidedToolDecl {
        ProvidedToolDecl {
            name: "echo-for-user".to_string(),
            mcp_name: "EchoForUser".to_string(),
            description: "Echo, for a user.".to_string(),
            input_schema: json!({ "type": "object" }),
            max_concurrency,
            auto_approve: false,
        }
    }

    fn alice() -> ActingUser {
        ActingUser {
            id: 1,
            username: "alice".to_string(),
        }
    }

    pub(crate) fn ctx(acting_user: Option<ActingUser>, deadline: Option<Instant>) -> ToolCtx {
        ToolCtx {
            caller: ParticipantId::for_app("pa", "test-origin"),
            grant: ResolvedToolGrant {
                acl: vec![],
                rate_limit: None,
            },
            acting_conversation_id: Some(7),
            acting_user,
            deadline,
        }
    }

    /// `ctx` for a call admitted now, with a full ceiling ahead of it.
    pub(crate) fn pending_ctx(acting_user: Option<ActingUser>) -> ToolCtx {
        ctx(
            acting_user,
            Some(Instant::now() + crate::descriptor::PROVIDED_TOOL_CEILING),
        )
    }

    /// An in-memory messenger holding the provider's request inbox, read by the
    /// provider through a position held from before any request lands, and the
    /// executor's production system policy.
    pub(crate) async fn messenger() -> Arc<Messenger> {
        messenger_with(SendRate::default()).await
    }

    /// [`messenger`] with `send_rate` set on the request inbox's entry.
    async fn messenger_with(send_rate: SendRate) -> Arc<Messenger> {
        let db = brenn_messaging_store::db::init_db_memory();
        let mut inbox = test_channel_entry(
            &request_inbox_name(PROVIDER),
            vec![SubscriberEntry {
                kind: SubscriberEntryKind::Wasm(PROVIDER.to_string()),
                push_depth: Depth::Unbounded,
                retain_depth: Depth::Unbounded,
                noise: NoiseLevel::Silent,
                wake_min: None,
            }],
        );
        inbox.resolved_channel.send_rate = send_rate;
        {
            let conn = db.lock().await;
            brenn_messaging_store::db::upsert_channels(&conn, std::slice::from_ref(&inbox));
        }
        let messenger = Messenger::new(
            db,
            Arc::new(MessagingDirectory::with_entries(vec![inbox.clone()])),
            Arc::from("test"),
            Arc::new(IndexMap::new()),
            Arc::new(NoopWakeRouter) as Arc<dyn WakeRouter>,
            MessagingGlobalConfig::default(),
        );
        let mut grants = GrantSet::default();
        grants.insert(AppCapability::MessagingSubscribe);
        let mut acls = AclSet::default();
        acls.brenn_subscribe
            .push(ChannelMatcher::Exact(request_inbox_name(PROVIDER)));
        let provider_policy = AppPolicy {
            grants,
            acls,
            tool_grants: BTreeMap::new(),
        };
        let messenger = messenger
            .with_subscriber_registrations(brenn_messaging::testutils::wasm_registrations(
                HashMap::from([(PROVIDER.to_string(), provider_policy)]),
            ))
            .with_subscriber_registrations(brenn_messaging::testutils::system_registrations(
                HashMap::from([(
                    TOOL_EXECUTOR_COMPONENT.to_string(),
                    crate::bus_wiring::tool_executor_system_policy(),
                )]),
            ));
        brenn_messaging::testutils::attach_wasm_port(
            &messenger,
            &inbox,
            PROVIDER,
            &ParticipantId::for_wasm(PROVIDER),
            Depth::Unbounded,
        )
        .await;
        messenger
    }

    /// Every request published to the provider's inbox so far, decoded.
    pub(crate) async fn requests(messenger: &Messenger) -> Vec<Value> {
        brenn_messaging::testutils::owed_everywhere(messenger, &ParticipantId::for_wasm(PROVIDER))
            .await
            .iter()
            .map(|(_, env)| serde_json::from_str(&env.body).expect("request body is JSON"))
            .collect()
    }

    /// The id of the one request on the inbox, once it has been published.
    pub(crate) async fn published_id(messenger: &Messenger) -> String {
        for _ in 0..1000 {
            let published = requests(messenger).await;
            if let Some(request) = published.first() {
                assert_eq!(published.len(), 1, "one request: {published:?}");
                return request["id"].as_str().expect("id is a string").to_string();
            }
            tokio::task::yield_now().await;
        }
        panic!("no request was published");
    }

    fn runtime() -> Arc<ProviderRuntime> {
        let (alert, _drain) = noop_alert_dispatcher();
        ProviderRuntime::new(alert)
    }

    /// A completion reaches the waiter of its own instance's record and
    /// consumes it; the user is readable only by the instance the record is
    /// pending for, and another instance's completion is indistinguishable from
    /// an unknown id.
    #[tokio::test]
    async fn a_record_answers_only_its_own_instance_once() {
        let rt = runtime();
        let mut guard = rt.begin(PROVIDER, "echo-for-user", &pending_ctx(Some(alice())));
        let id = guard.id().as_str().to_string();
        assert_eq!(id.len(), 32);
        assert!(id.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')));

        assert_eq!(rt.acting_user_for(PROVIDER, &id), Some(Some(alice())));
        assert_eq!(rt.acting_user_for("other", &id), None);
        assert_eq!(
            rt.complete("other", &id, Ok(json!(1))),
            Err(CompleteError::NotPending)
        );
        assert_eq!(
            rt.complete(PROVIDER, "0".repeat(32).as_str(), Ok(json!(1))),
            Err(CompleteError::NotPending)
        );

        assert_eq!(rt.complete(PROVIDER, &id, Ok(json!({ "a": 1 }))), Ok(()));
        assert_eq!((&mut guard.rx).await.unwrap(), Ok(json!({ "a": 1 })));
        assert_eq!(rt.acting_user_for(PROVIDER, &id), None);
        assert_eq!(
            rt.complete(PROVIDER, &id, Ok(json!(2))),
            Err(CompleteError::NotPending)
        );
    }

    /// Two calls mint two ids, and a record with no acting user is still
    /// pending: `Some(None)`, not `None`.
    #[tokio::test]
    async fn ids_are_distinct_and_a_userless_record_is_pending() {
        let rt = runtime();
        let a = rt.begin(PROVIDER, "echo-for-user", &pending_ctx(None));
        let b = rt.begin(PROVIDER, "echo-for-user", &pending_ctx(None));
        assert_ne!(a.id(), b.id());
        assert_eq!(rt.acting_user_for(PROVIDER, a.id().as_str()), Some(None));
    }

    /// Dropping the guard is what ends a record, however the call ended.
    #[tokio::test]
    async fn dropping_the_guard_removes_the_record() {
        let rt = runtime();
        let guard = rt.begin(PROVIDER, "echo-for-user", &pending_ctx(Some(alice())));
        let id = guard.id().as_str().to_string();
        drop(guard);
        assert_eq!(rt.acting_user_for(PROVIDER, &id), None);
        assert_eq!(
            rt.complete(PROVIDER, &id, Ok(json!(1))),
            Err(CompleteError::NotPending)
        );
    }

    /// An outcome a provider may not give is refused and leaves the record
    /// pending, so the provider can answer again.
    #[tokio::test]
    async fn an_inadmissible_outcome_leaves_the_record_pending() {
        let rt = runtime();
        let mut guard = rt.begin(PROVIDER, "echo-for-user", &pending_ctx(None));
        let id = guard.id().as_str().to_string();

        for refused in [
            ToolError::NotGranted,
            ToolError::RateLimited,
            ToolError::WrongClass,
        ] {
            let kind = refused.kind_str();
            match rt.complete(PROVIDER, &id, Err(refused)) {
                Err(CompleteError::InvalidOutcome(detail)) => {
                    assert!(detail.contains(kind), "{detail}");
                }
                other => panic!("{kind}: {other:?}"),
            }
        }
        let over = "x".repeat(PROVIDED_RESULT_BUDGET);
        match rt.complete(PROVIDER, &id, Ok(json!(over))) {
            Err(CompleteError::InvalidOutcome(detail)) => {
                assert!(
                    detail.contains(&PROVIDED_RESULT_BUDGET.to_string()),
                    "{detail}"
                );
            }
            other => panic!("over budget: {other:?}"),
        }
        // An error's detail is the provider's text too, and is bounded alike.
        for over in [
            ToolError::Denied("x".repeat(PROVIDED_RESULT_BUDGET)),
            ToolError::InvalidArgs("x".repeat(PROVIDED_RESULT_BUDGET)),
            ToolError::Internal("x".repeat(PROVIDED_RESULT_BUDGET)),
        ] {
            let kind = over.kind_str();
            match rt.complete(PROVIDER, &id, Err(over)) {
                Err(CompleteError::InvalidOutcome(detail)) => {
                    assert!(detail.contains("error detail"), "{kind}: {detail}");
                    assert!(
                        detail.contains(&PROVIDED_RESULT_BUDGET.to_string()),
                        "{kind}: {detail}"
                    );
                }
                other => panic!("{kind} over budget: {other:?}"),
            }
        }
        // A value exactly at the budget is accepted (two bytes of quotes).
        let at = "x".repeat(PROVIDED_RESULT_BUDGET - 2);
        assert_eq!(rt.complete(PROVIDER, &id, Ok(json!(at.clone()))), Ok(()));
        assert_eq!((&mut guard.rx).await.unwrap(), Ok(json!(at)));
    }

    /// Every error kind a tool's own `execute` produces is a provider's to give.
    #[tokio::test]
    async fn a_provider_may_answer_with_an_execution_error() {
        let rt = runtime();
        for answer in [
            ToolError::Denied("no".to_string()),
            ToolError::InvalidArgs("bad".to_string()),
            ToolError::Internal("broke".to_string()),
        ] {
            let mut guard = rt.begin(PROVIDER, "echo-for-user", &pending_ctx(None));
            let id = guard.id().as_str().to_string();
            assert_eq!(rt.complete(PROVIDER, &id, Err(answer.clone())), Ok(()));
            assert_eq!((&mut guard.rx).await.unwrap(), Err(answer));
        }
    }

    /// A failed delivery answers the listed ids still pending for the instance
    /// with `internal`, and leaves completed ids, unknown ids and another
    /// instance's ids alone.
    #[tokio::test]
    async fn fail_delivered_answers_only_this_instances_pending_ids() {
        let rt = runtime();
        let mut live = rt.begin(PROVIDER, "echo-for-user", &pending_ctx(None));
        let mut done = rt.begin(PROVIDER, "echo-for-user", &pending_ctx(None));
        let foreign = rt.begin("other", "echo-for-user", &pending_ctx(None));
        let done_id = done.id().as_str().to_string();
        rt.complete(PROVIDER, &done_id, Ok(json!("answered")))
            .unwrap();

        rt.fail_delivered(
            PROVIDER,
            &[
                live.id().as_str().to_string(),
                done_id,
                foreign.id().as_str().to_string(),
                "f".repeat(32),
            ],
            "provider activation failed: trap",
        );

        assert_eq!(
            rt.acting_user_for(PROVIDER, live.id().as_str()),
            None,
            "the failed record is gone while its guard still holds"
        );
        assert_eq!(
            (&mut live.rx).await.unwrap(),
            Err(ToolError::Internal(
                "provider activation failed: trap".to_string()
            ))
        );
        assert_eq!((&mut done.rx).await.unwrap(), Ok(json!("answered")));
        assert_eq!(
            rt.acting_user_for("other", foreign.id().as_str()),
            Some(None)
        );
    }

    /// The descriptor is the declaration's, with the MCP prefix the registry
    /// owns and the fixed fields every provided tool carries.
    #[tokio::test]
    async fn the_descriptor_is_the_declarations() {
        let tool = ProvidedTool::new(PROVIDER, &decl(3), runtime());
        let d = tool.descriptor();
        assert_eq!(d.name, "echo-for-user");
        assert_eq!(d.mcp_name, "mcp__brenn__EchoForUser");
        assert_eq!(d.description, "Echo, for a user.");
        assert_eq!(d.input_schema, json!({ "type": "object" }));
        assert_eq!(d.class, ToolClass::Async { max_concurrency: 3 });
        assert!(d.acl_keys.is_empty());
        assert_eq!(d.idempotency, Idempotency::Natural);
        assert!(!d.auto_approve);
        assert_eq!(d.user_binding, UserBinding::ActingOrLentUser);
        assert_eq!(tool.instance(), PROVIDER);
        assert_eq!(tool.check_acl(&json!({ "anything": 1 }), &[]), Ok(()));
    }

    /// `execute` hands the request to the provider's inbox — id, tool and args,
    /// and nothing naming the caller or the user — and returns the provider's
    /// answer.
    #[tokio::test]
    async fn execute_publishes_the_request_and_returns_the_completion() {
        let rt = runtime();
        let messenger = messenger().await;
        rt.attach(messenger.clone());
        let tool = Arc::new(ProvidedTool::new(PROVIDER, &decl(1), rt.clone()));

        let deadline = Instant::now() + Duration::from_secs(30);
        let call = tokio::spawn({
            let tool = tool.clone();
            async move {
                tool.execute(&ctx(Some(alice()), Some(deadline)), json!({ "q": "x" }))
                    .await
            }
        });
        let id = published_id(&messenger).await;
        let request = &requests(&messenger).await[0];
        assert_eq!(
            request,
            &json!({ "v": 1, "id": id, "tool": "echo-for-user", "args": { "q": "x" } })
        );
        assert_eq!(rt.acting_user_for(PROVIDER, &id), Some(Some(alice())));

        rt.complete(PROVIDER, &id, Ok(json!({ "echoed": "x" })))
            .unwrap();
        assert_eq!(call.await.unwrap(), Ok(json!({ "echoed": "x" })));
        assert_eq!(rt.acting_user_for(PROVIDER, &id), None);
    }

    /// A provider that never answers is answered for at the deadline: the
    /// caller gets `internal`, the record is gone, a late completion is not
    /// pending, and the ceiling alert names the tool.
    #[tokio::test]
    async fn execute_answers_internal_at_the_deadline() {
        let (alert, captured, drain) = make_capturing_alerter();
        let rt = ProviderRuntime::new(alert);
        let messenger = messenger().await;
        rt.attach(messenger.clone());
        let tool = ProvidedTool::new(PROVIDER, &decl(1), rt.clone());

        let deadline = Instant::now() + Duration::from_millis(100);
        let result = tool.execute(&ctx(None, Some(deadline)), json!({})).await;
        assert_eq!(
            result,
            Err(ToolError::Internal(
                "provider did not complete within the ceiling".to_string()
            ))
        );
        assert!(Instant::now() >= deadline);
        let id = requests(&messenger).await[0]["id"]
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(rt.acting_user_for(PROVIDER, &id), None);
        assert_eq!(
            rt.complete(PROVIDER, &id, Ok(json!(1))),
            Err(CompleteError::NotPending)
        );

        drop(tool);
        drop(rt);
        drain.await.unwrap();
        let captured = captured.lock().unwrap();
        assert_eq!(captured.len(), 1, "{captured:?}");
        assert!(captured[0].0.contains("echo-for-user"), "{captured:?}");
    }

    /// Arguments that make the request larger than the bus carries are the
    /// caller's to fix: `invalid_args` naming the sizes, nothing published and
    /// no record left behind.
    #[tokio::test]
    async fn execute_answers_invalid_args_for_a_request_over_the_body_limit() {
        let rt = runtime();
        let messenger = messenger().await;
        rt.attach(messenger.clone());
        let tool = ProvidedTool::new(PROVIDER, &decl(1), rt.clone());

        let args = json!({ "q": "x".repeat(70_000) });
        let result = tool.execute(&pending_ctx(Some(alice())), args).await;
        let Err(ToolError::InvalidArgs(detail)) = result else {
            panic!("expected invalid_args, got {result:?}");
        };
        assert!(
            detail.contains("over the bus body limit of 65536 bytes"),
            "{detail:?}"
        );
        assert!(requests(&messenger).await.is_empty());
        assert!(
            rt.pending.lock().unwrap().is_empty(),
            "the refused call leaves no record"
        );
    }

    /// A send-rate refusal on the inbox's entry is answered to the caller as
    /// a limit, not a host fault; the refused call leaves no record and the
    /// calls admitted before it are unaffected.
    #[tokio::test]
    async fn execute_answers_rate_limited_when_the_inbox_send_rate_runs_out() {
        let rt = runtime();
        let messenger = messenger_with(SendRate {
            burst: 1,
            refill_interval_secs: 3600,
            refill: 1,
        })
        .await;
        rt.attach(messenger.clone());
        let tool = Arc::new(ProvidedTool::new(PROVIDER, &decl(2), rt.clone()));

        let first = tokio::spawn({
            let tool = tool.clone();
            async move { tool.execute(&pending_ctx(None), json!({})).await }
        });
        let id = published_id(&messenger).await;

        let second = tool.execute(&pending_ctx(None), json!({})).await;
        assert_eq!(second, Err(ToolError::RateLimited));
        assert_eq!(requests(&messenger).await.len(), 1);
        assert_eq!(
            rt.pending.lock().unwrap().len(),
            1,
            "only the published call is pending"
        );

        rt.complete(PROVIDER, &id, Ok(json!("done"))).unwrap();
        assert_eq!(first.await.unwrap(), Ok(json!("done")));
    }

    /// Dropping the `execute` future mid-wait — a caller's own timeout, an
    /// aborted task — ends the record with it.
    #[tokio::test]
    async fn dropping_execute_mid_wait_ends_the_record() {
        let rt = runtime();
        let messenger = messenger().await;
        rt.attach(messenger.clone());
        let tool = Arc::new(ProvidedTool::new(PROVIDER, &decl(1), rt.clone()));

        let deadline = Instant::now() + Duration::from_secs(30);
        let call = tokio::spawn({
            let tool = tool.clone();
            async move {
                tool.execute(&ctx(Some(alice()), Some(deadline)), json!({}))
                    .await
            }
        });
        let id = published_id(&messenger).await;
        assert_eq!(rt.acting_user_for(PROVIDER, &id), Some(Some(alice())));
        call.abort();
        assert!(call.await.unwrap_err().is_cancelled());
        assert_eq!(rt.acting_user_for(PROVIDER, &id), None);
        assert_eq!(
            rt.complete(PROVIDER, &id, Ok(json!(1))),
            Err(CompleteError::NotPending)
        );
    }

    /// Every call site that admits a provided tool sets its deadline; one that
    /// does not is a bypassed admission.
    #[tokio::test]
    #[should_panic(expected = "admitted without a deadline")]
    async fn execute_without_a_deadline_panics() {
        let tool = ProvidedTool::new(PROVIDER, &decl(1), runtime());
        tool.execute(&ctx(None, None), json!({}))
            .await
            .expect("execute panics before answering");
    }

    #[tokio::test]
    #[should_panic(expected = "attached twice")]
    async fn attaching_a_second_messenger_panics() {
        let rt = runtime();
        let messenger = messenger().await;
        rt.attach(messenger.clone());
        rt.attach(messenger);
    }
}
