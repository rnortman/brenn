//! A tool a compiled component provides, end to end.
//!
//! The `processor-provider-test` fixture provides `echo-for-user`. A caller —
//! an app conversation through `invoke_for_llm`, or the `processor-tool-test`
//! component through `call-async` — reaches it through the one registry; the
//! request lands on the provider's `brenn:tool-requests/<slug>` inbox; the
//! guest serves it through a real `WasmProvidesHost` over the process's
//! `ProviderRuntime`, acting for the caller's user through `call-async-for`
//! where its mode says so; and the caller receives the provider's completion.
//! The guest and the executor are stepped by hand; the conversation's call
//! runs on a spawned task.

use super::*;

use std::collections::{BTreeMap, HashMap};
use std::path::Path;

use brenn_envelope::addressing::TOOL_REQUEST_INPUT_PORT;
use brenn_lib::messaging::config::{ProvidedToolDecl, SystemChannelTuning};
use brenn_lib::tools::ResolvedToolGrant;
use brenn_messaging::{Messenger, MountDebt};
use brenn_oauth::testutil;
use brenn_tool_registry::bus_wiring::{
    inbox_input_port, request_channel_entry, request_inbox_depth, request_inbox_entry,
    request_inbox_subscription, request_input_port, tool_executor_spec,
    tool_executor_system_policy,
};
use brenn_tool_registry::executor::TOOL_EXECUTOR_COMPONENT;
use brenn_tool_registry::testutil::{clause, grant};
use brenn_tool_registry::{
    ActingUser, GrantOwner, ProvidedTool, ProviderRuntime, ToolCallerGrants, ToolError,
    ToolExecutor, ToolRegistry, WasmProvidesHost, WasmToolHost,
};
use brenn_wasm::{ProvidesCallError, ProvidesHost, ToolCallError};
use serde_json::{Value, json};
use tokio::task::JoinHandle;

const PROVIDER_WASM: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../brenn-wasm/target/components/brenn_processor_provider_test.wasm"
);

/// The providing instance.
const PROVIDER: &str = "prov";
/// The component that calls the provided tool through `call-async`.
const CALLER: &str = "agenda";
/// The provided tool's MCP name, as an agent sees it.
const MCP_NAME: &str = "mcp__brenn__EchoForUser";

/// The fixture's one `provide` statement, as its spec declares it.
fn echo_for_user() -> ProvidedToolDecl {
    ProvidedToolDecl {
        name: "echo-for-user".to_string(),
        mcp_name: "EchoForUser".to_string(),
        description: "Test fixture: answer by the `mode` argument, for the caller's user."
            .to_string(),
        input_schema: json!({ "type": "object" }),
        max_concurrency: 2,
        auto_approve: true,
    }
}

/// The provider's request inbox with the provider's own subscription on it,
/// at the depth `request_inbox_depth` gives it.
fn request_inbox_with_own_subscriber(defaults: &MessagingGlobalConfig) -> ChannelEntry {
    let depth = request_inbox_depth(PROVIDER, &[echo_for_user()]);
    super::with_own_wasm_subscriber(
        PROVIDER,
        request_inbox_entry(PROVIDER, depth, defaults),
        request_inbox_subscription,
    )
}

/// The provider, its callers' bus and registry, and the OAuth stub its nested
/// calls reach.
struct Rig {
    messenger: Arc<Messenger>,
    registry: Arc<ToolRegistry>,
    runtime: Arc<ProviderRuntime>,
    executor: ToolExecutor,
    alert: brenn_obs::alerting::AlertDispatcher,
    provider: WasmConsumerConfig,
    provider_sub: ParticipantId,
    /// The provider's tool grants, which its nested calls are admitted by.
    provider_grants: BTreeMap<String, ResolvedToolGrant>,
    caller: WasmConsumerConfig,
    caller_sub: ParticipantId,
    trigger: ChannelEntry,
    stub: testutil::Stub,
    /// The stub's `host:port`, as an `api-request` names it.
    host: String,
    _vault: tempfile::TempDir,
}

/// Stand up the rig. `provider_acl` is the provider's grant on `api-request`,
/// none when empty; `caller_args` is what the calling component passes `echo-for-user`.
async fn rig(provider_acl: Vec<&[(&str, &str)]>, caller_args: Value) -> Rig {
    let (alert, _alerts) = noop_alert_dispatcher();
    let (base, stub) = testutil::start_stub().await;
    stub.lock().unwrap().api_body = br#"{"items":[]}"#.to_vec();
    let host = base
        .strip_prefix("http://")
        .expect("the stub is plain http")
        .to_string();
    let (service, vault) = testutil::service(vec![testutil::provider_config("stub", &base)]);
    // Alice's credential, and the same account connected under bob: a call
    // acting for alice must send alice's.
    let mut alices = testutil::credential("stub", "alice@example.com", "RT-alice");
    alices.access_token = "AT-alice".to_string();
    service.vault().store("alice", &alices).await;
    let mut bobs = testutil::credential("stub", "alice@example.com", "RT-bob");
    bobs.access_token = "AT-bob".to_string();
    service.vault().store("bob", &bobs).await;

    let runtime = ProviderRuntime::new(alert.clone());
    let registry = Arc::new(ToolRegistry::with_provided(
        brenn_oauth::tools::registered(
            Some(&service),
            BTreeMap::from([testutil::key_provider_config("maps", &base)]),
        ),
        vec![Arc::new(ProvidedTool::new(
            PROVIDER,
            &echo_for_user(),
            runtime.clone(),
        ))],
    ));

    let defaults = MessagingGlobalConfig::default();
    let tuning = SystemChannelTuning::default();
    let requests = request_inbox_with_own_subscriber(&defaults);
    let (provider_results, provider_results_ch) =
        super::inbox_entry_with_own_subscriber(PROVIDER, &tuning, &defaults);
    let trigger =
        (*testutils::wasm_channel_entry(CALLER, "trigger", Depth::Unbounded, Depth::Unbounded))
            .clone();
    let (caller_results, caller_results_ch) =
        super::inbox_entry_with_own_subscriber(CALLER, &tuning, &defaults);
    let mut entries = vec![
        request_channel_entry("api-request", &tuning, &defaults),
        request_channel_entry("echo-for-user", &tuning, &defaults),
        requests.clone(),
        provider_results.clone(),
        trigger.clone(),
        caller_results.clone(),
    ];
    brenn_messaging::system::fold_spec_subscriptions(
        &mut entries,
        &[tool_executor_spec(&["api-request", "echo-for-user"])],
    );
    let db = init_db_memory_server_slice();
    {
        let conn = db.lock().await;
        upsert_channels(&conn, &entries);
    }
    let messenger = Messenger::new(
        db,
        Arc::new(MessagingDirectory::with_entries(entries.clone())),
        Arc::from("test"),
        Arc::new(IndexMap::new()),
        Arc::new(NoopWakeRouter) as Arc<dyn WakeRouter>,
        defaults,
    )
    .with_subscriber_registrations(testutils::wasm_registrations(wasm_policies_from_entries(
        &entries,
    )))
    .with_subscriber_registrations(testutils::system_registrations(HashMap::from([(
        TOOL_EXECUTOR_COMPONENT.to_string(),
        tool_executor_system_policy(),
    )])));
    runtime.attach(messenger.clone());
    let provider_sub = ParticipantId::for_wasm(PROVIDER);
    let caller_sub = ParticipantId::for_wasm(CALLER);
    super::attach_input_ports(
        &messenger,
        PROVIDER,
        &provider_sub,
        &[
            (&requests, requests.resolved_channel.retain_depth),
            (&provider_results, Depth::Unbounded),
        ],
    )
    .await;
    super::attach_input_ports(
        &messenger,
        CALLER,
        &caller_sub,
        &[
            (&trigger, Depth::Unbounded),
            (&caller_results, Depth::Unbounded),
        ],
    )
    .await;
    super::attach_tool_executor(&messenger).await;

    // A provider whose test makes no nested call holds no grant for one.
    let provider_grants = if provider_acl.is_empty() {
        BTreeMap::new()
    } else {
        BTreeMap::from([(
            "api-request".to_string(),
            grant(provider_acl.into_iter().map(clause).collect()),
        )])
    };
    registry.validate_grants(
        GrantOwner::WasmConsumer { slug: PROVIDER },
        &provider_grants,
    );
    let caller_grants = BTreeMap::from([("echo-for-user".to_string(), grant(vec![]))]);
    registry.validate_grants(GrantOwner::WasmConsumer { slug: CALLER }, &caller_grants);

    let provider_tools = Arc::new(WasmToolHost::new(
        registry.clone(),
        provider_grants.clone(),
        PROVIDER.to_string(),
        alert.clone(),
    ));
    let provider_component = Arc::new(ProcessorComponent::load(ProcessorLoadSpec {
        input_amplification_mt: HashMap::from([
            (TOOL_REQUEST_INPUT_PORT.to_string(), 1000),
            ("tool-results".to_string(), 1000),
        ]),
        grants: [ComponentGrant::Tools, ComponentGrant::Provides]
            .into_iter()
            .collect(),
        alerter: noop_proc_alerter(),
        tool_host: Some(provider_tools.clone()),
        provides_host: Some(Arc::new(WasmProvidesHost::new(
            runtime.clone(),
            provider_tools,
        ))),
        ..ProcessorLoadSpec::minimal(Path::new(PROVIDER_WASM), PROVIDER)
    }));
    let provider = WasmConsumerConfig {
        slug: PROVIDER.to_string(),
        component: provider_component,
        notify: Arc::new(Notify::new()),
        messenger: messenger.clone(),
        alert_dispatcher: alert.clone(),
        inputs: vec![
            request_input_port(PROVIDER, &requests.resolved_channel),
            inbox_input_port(PROVIDER, &provider_results_ch),
        ],
        outputs: vec![],
        sync_ports: std::collections::BTreeSet::new(),
        activation_pacing: unthrottled_pacing(),
        provider: Some(runtime.clone()),
    };

    let caller_component = Arc::new(ProcessorComponent::load(ProcessorLoadSpec {
        input_amplification_mt: HashMap::from([
            ("in".to_string(), 1000),
            ("tool-results".to_string(), 1000),
        ]),
        config: HashMap::from([
            ("tool".to_string(), "echo-for-user".to_string()),
            ("args".to_string(), caller_args.to_string()),
        ]),
        grants: [
            ComponentGrant::Ports,
            ComponentGrant::Tools,
            ComponentGrant::Config,
        ]
        .into_iter()
        .collect(),
        alerter: noop_proc_alerter(),
        tool_host: Some(Arc::new(WasmToolHost::new(
            registry.clone(),
            caller_grants.clone(),
            CALLER.to_string(),
            alert.clone(),
        ))),
        ..ProcessorLoadSpec::minimal(Path::new(super::tool_e2e::TOOL_WASM), CALLER)
    }));
    let caller = WasmConsumerConfig {
        slug: CALLER.to_string(),
        component: caller_component,
        notify: Arc::new(Notify::new()),
        messenger: messenger.clone(),
        alert_dispatcher: alert.clone(),
        inputs: vec![
            WasmInputPort {
                port: "in".to_string(),
                sub: ResolvedSubscription {
                    channel_uuid: trigger.uuid,
                    channel_address: trigger.address.clone(),
                    push_depth: Depth::Bounded(WINDOW_DEPTH_CEILING),
                    retain_depth: Depth::Bounded(WINDOW_DEPTH_CEILING),
                    noise: NoiseLevel::Silent,
                    wake_min: WakeMin::Normal,
                },
                amplification_mt: 1000,
            },
            inbox_input_port(CALLER, &caller_results_ch),
        ],
        outputs: vec![],
        sync_ports: std::collections::BTreeSet::new(),
        activation_pacing: unthrottled_pacing(),
        provider: None,
    };

    let executor = ToolExecutor::new(
        messenger.clone(),
        registry.clone(),
        Arc::new(ToolCallerGrants::new(HashMap::from([
            (provider_sub.as_str().to_string(), provider_grants.clone()),
            (caller_sub.as_str().to_string(), caller_grants),
        ]))),
        runtime.clone(),
        alert.clone(),
        Arc::new(Notify::new()),
    );

    Rig {
        messenger,
        registry,
        runtime,
        executor,
        alert,
        provider,
        provider_sub,
        provider_grants,
        caller,
        caller_sub,
        trigger,
        stub,
        host,
        _vault: vault,
    }
}

fn alice() -> ActingUser {
    ActingUser {
        id: 1,
        username: "alice".to_string(),
    }
}

impl Rig {
    /// An app conversation acting for alice calls the provided tool, on a
    /// task of its own: it waits for the provider as an agent's call does.
    fn call_for_alice(&self, args: Value) -> JoinHandle<Result<Value, ToolError>> {
        let registry = self.registry.clone();
        let alert = self.alert.clone();
        tokio::spawn(async move {
            registry
                .invoke_for_llm(
                    MCP_NAME,
                    ParticipantId::for_app("pa", "test-origin"),
                    grant(vec![]),
                    Some(7),
                    Some(alice()),
                    args,
                    &alert,
                )
                .await
        })
    }

    /// The bodies owed to `sub`, once there are `n` of them.
    async fn owed(&self, sub: &ParticipantId, n: usize) -> Vec<Value> {
        for _ in 0..1000 {
            let rows = testutils::owed_everywhere(&self.messenger, sub).await;
            if rows.len() >= n {
                assert_eq!(rows.len(), n, "{sub:?} is owed more than expected");
                return rows
                    .iter()
                    .map(|(_, env)| serde_json::from_str(&env.body).expect("a JSON body"))
                    .collect();
            }
            tokio::task::yield_now().await;
        }
        panic!("{sub:?} was never owed {n} rows");
    }

    /// Wait for one request on the provider's inbox and return its id.
    async fn requested_id(&self) -> String {
        let requests = self.owed(&self.provider_sub, 1).await;
        requests[0]["id"]
            .as_str()
            .expect("a request carries its id")
            .to_string()
    }

    /// The tool executor, as a subscriber to the request channels.
    fn executor_sub(&self) -> ParticipantId {
        ParticipantId::for_system(TOOL_EXECUTOR_COMPONENT)
    }

    /// Activate the provider on whatever it is owed.
    async fn serve(&self) {
        drain_step(&self.provider, &self.provider_sub, MountDebt::Settled).await;
    }

    /// A `provides` host over the same table for another instance, granted
    /// what the provider is granted.
    fn host_for(&self, slug: &str) -> WasmProvidesHost {
        WasmProvidesHost::new(
            self.runtime.clone(),
            Arc::new(WasmToolHost::new(
                self.registry.clone(),
                self.provider_grants.clone(),
                slug.to_string(),
                self.alert.clone(),
            )),
        )
    }
}

/// A conversation's call is served by the provider acting for the
/// conversation's user: the nested `api-request` sends alice's OAuth
/// credential, under a clause naming any user, and the API's answer is the
/// conversation's result.
#[tokio::test]
async fn an_app_call_acts_for_its_user_through_the_provider() {
    let r = rig(
        vec![&[
            ("provider", "stub"),
            ("user", "*"),
            ("account", "alice@example.com"),
            ("method", "GET"),
            ("host", "*"),
            ("path", "/calendar/v3/calendars/*/events"),
        ]],
        json!({}),
    )
    .await;
    let call = r.call_for_alice(json!({
        "mode": "api",
        "request": {
            "provider": "stub",
            "account": "alice@example.com",
            "method": "GET",
            "host": r.host,
            "path": "/calendar/v3/calendars/primary/events",
        },
    }));
    let id = r.requested_id().await;

    // The provider's request activation issues the nested call for alice.
    r.serve().await;
    let nested = r.owed(&r.executor_sub(), 1).await;
    assert_eq!(nested[0]["tool"], "api-request");
    assert_eq!(nested[0]["serving"], id.as_str());
    assert!(nested[0].get("user").is_none() && nested[0].get("acting_user").is_none());

    // The executor runs it stamped with the pending call's user.
    r.executor.drain_step().await;
    {
        let seen = &r.stub.lock().unwrap().api_seen;
        assert_eq!(seen.len(), 1, "exactly one request reached the API");
        assert_eq!(
            seen[0].headers.get("authorization").unwrap(),
            "Bearer AT-alice"
        );
    }
    let results = r.owed(&r.provider_sub, 1).await;
    assert_eq!(results[0]["serving"], id.as_str());

    // The provider's result activation completes the conversation's call.
    r.serve().await;
    let answer = call.await.unwrap().expect("the provider answered");
    assert_eq!(answer["status"], 200, "{answer}");
    assert_eq!(answer["body"], r#"{"items":[]}"#);
    assert!(r.runtime.serving(PROVIDER, &id).is_none());
}

/// A stamped call writes: the nested `POST` to a key provider is admitted by
/// the provider's `method = "POST"` clause because it acts for alice, where
/// the same call with no acting user only reads.
#[tokio::test]
async fn a_stamped_nested_call_writes_with_a_key() {
    let r = rig(
        vec![&[
            ("provider", "maps"),
            ("method", "POST"),
            ("path", "/v1/places:search"),
        ]],
        json!({}),
    )
    .await;
    let call = r.call_for_alice(json!({
        "mode": "api",
        "request": {
            "provider": "maps",
            "method": "POST",
            "host": r.host,
            "path": "/v1/places:search",
            "body": "{}",
        },
    }));
    r.requested_id().await;
    r.serve().await;
    r.executor.drain_step().await;
    r.serve().await;

    let answer = call.await.unwrap().expect("the provider answered");
    assert_eq!(answer["status"], 200, "{answer}");
    let seen = &r.stub.lock().unwrap().api_seen;
    assert_eq!(seen.len(), 1, "exactly one request reached the API");
    assert_eq!(seen[0].method, "POST");
    assert_eq!(seen[0].headers.get("x-api-key").unwrap(), testutil::KEY);
}

/// A component's `call-async` to the provided tool is answered on the
/// component's own result inbox, through the executor, and the answer carries
/// no `serving`: the component serves nobody.
#[tokio::test]
async fn a_components_call_is_answered_on_its_result_inbox() {
    let args = json!({ "mode": "echo", "n": 1 });
    let r = rig(vec![], args.clone()).await;
    testutils::insert_bus_message(&r.messenger, &r.trigger, "go", ChannelScheme::Brenn).await;
    drain_step(&r.caller, &r.caller_sub, MountDebt::Settled).await;

    // The executor admits the call and hands it to the provider without
    // waiting for the answer.
    r.executor.drain_step().await;
    r.requested_id().await;
    r.serve().await;

    let results = r.owed(&r.caller_sub, 1).await;
    assert_eq!(results[0]["tool"], "echo-for-user");
    assert_eq!(results[0]["call_id"], "call-1");
    assert_eq!(results[0]["outcome"]["ok"], args);
    assert!(
        results[0].get("serving").is_none(),
        "a plain call's result names no request it serves: {}",
        results[0]
    );
}

/// Completing an id the host never minted is `not-pending`, and leaves the
/// real request to be answered.
#[tokio::test]
async fn a_made_up_id_is_not_pending() {
    let r = rig(vec![], json!({})).await;
    let call = r.call_for_alice(json!({ "mode": "stale" }));
    r.requested_id().await;
    r.serve().await;
    let answer = call.await.unwrap().expect("the provider answered");
    let drew = answer["stale"]
        .as_str()
        .expect("the fixture names what it drew");
    assert!(drew.contains("not-pending"), "{drew}");
}

/// A provider activation that fails answers the request it was handed at
/// once, not at the ceiling.
#[tokio::test]
async fn a_failed_provider_activation_answers_its_caller_at_once() {
    let r = rig(vec![], json!({})).await;
    let call = r.call_for_alice(json!({ "mode": "fail" }));
    r.requested_id().await;
    r.serve().await;
    let answer = tokio::time::timeout(std::time::Duration::from_secs(10), call)
        .await
        .expect("answered well inside the ceiling")
        .unwrap();
    let Err(ToolError::Internal(detail)) = answer else {
        panic!("expected an internal answer, got {answer:?}");
    };
    assert!(
        detail.starts_with("provider activation failed") && detail.contains("fail mode"),
        "{detail}"
    );
}

/// An id pending for one instance can neither be completed nor served by
/// another: both are refused as an unknown id would be, and the instance it
/// belongs to still answers it.
#[tokio::test]
async fn another_instance_cannot_complete_or_act_for_a_request() {
    let r = rig(vec![], json!({})).await;
    let call = r.call_for_alice(json!({ "mode": "echo" }));
    let id = r.requested_id().await;

    let other = r.host_for("other");
    let nested = json!({ "provider": "stub", "method": "GET", "host": r.host, "path": "/" });
    assert!(matches!(
        other.queue_async_for(&id, "api-request", &nested.to_string(), "c1"),
        Err(ToolCallError::Denied(_))
    ));
    assert_eq!(
        other.complete(&id, r#"{"ok": "hijacked"}"#),
        Err(ProvidesCallError::NotPending)
    );

    r.serve().await;
    let answer = call.await.unwrap().expect("the provider answered");
    assert_eq!(answer["mode"], "echo");
}
