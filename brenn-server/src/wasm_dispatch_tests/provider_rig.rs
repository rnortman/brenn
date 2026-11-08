//! The shared rig for a compiled component that provides tools, end to end.
//!
//! One registry holds the provider's tools beside the native ones it is
//! given; the provider's `brenn:tool-requests/<slug>` and
//! `brenn:tool-results/<slug>` inboxes, the request channel of every tool and
//! the tool executor's subscriptions are on one bus; the provider runs behind a
//! real `WasmProvidesHost` over the process's `ProviderRuntime`. A rig may also
//! hold a calling component, `processor-tool-test`, that calls one provided
//! tool through `call-async` when its trigger channel is written. Each suite
//! keeps its own stub and fixtures beside this.

use super::*;

use std::collections::{BTreeMap, HashMap};
use std::path::Path;

use brenn_envelope::addressing::TOOL_REQUEST_INPUT_PORT;
use brenn_lib::messaging::config::{ProvidedToolDecl, SystemChannelTuning};
use brenn_lib::tools::ResolvedToolGrant;
use brenn_messaging::{Messenger, MountDebt};
use brenn_tool_registry::bus_wiring::{
    inbox_input_port, request_channel_entry, request_inbox_depth, request_inbox_entry,
    request_inbox_subscription, request_input_port, tool_executor_spec,
    tool_executor_system_policy,
};
use brenn_tool_registry::executor::TOOL_EXECUTOR_COMPONENT;
use brenn_tool_registry::testutil::grant;
use brenn_tool_registry::{
    ActingUser, GrantOwner, ProvidedTool, ProviderRuntime, RegisteredTool, ToolCallerGrants,
    ToolError, ToolExecutor, ToolRegistry, WasmProvidesHost, WasmToolHost,
};
use serde_json::Value;
use tokio::task::JoinHandle;

/// What a provider rig is built from.
pub(super) struct ProviderSetup<'a> {
    /// The providing instance's slug.
    pub slug: &'a str,
    /// The provider's `provide` statements.
    pub decls: Vec<ProvidedToolDecl>,
    /// The native tools the registry holds beside the provided ones.
    pub native: Vec<RegisteredTool>,
    /// The provider's tool grants, which its nested calls are admitted by.
    pub grants: BTreeMap<String, ResolvedToolGrant>,
    /// The provider's component: its artifact, `config`, store and capability
    /// grants. The rig sets its hosts, alerter and input amplification.
    pub component: ProcessorLoadSpec<'a>,
    /// A component that calls one provided tool, if the suite needs one.
    pub caller: Option<CallerSetup<'a>>,
}

/// A `processor-tool-test` instance that calls `tool` with `args` through
/// `call-async` each time its trigger channel is written.
pub(super) struct CallerSetup<'a> {
    pub slug: &'a str,
    pub tool: &'a str,
    pub args: Value,
}

/// The calling component and the channel that sets it off.
pub(super) struct Caller {
    pub component: WasmConsumerConfig,
    pub sub: ParticipantId,
    pub trigger: ChannelEntry,
}

/// The provider, its callers' registry and bus, and the tool executor.
pub(super) struct ProviderRig {
    pub messenger: Arc<Messenger>,
    pub registry: Arc<ToolRegistry>,
    pub runtime: Arc<ProviderRuntime>,
    pub executor: ToolExecutor,
    pub alert: brenn_obs::alerting::AlertDispatcher,
    pub provider: WasmConsumerConfig,
    pub provider_sub: ParticipantId,
    /// The provider's tool grants, which its nested calls are admitted by.
    pub provider_grants: BTreeMap<String, ResolvedToolGrant>,
    /// The provider's result inbox, for a case that delivers a result itself.
    pub provider_results: ChannelEntry,
    caller: Option<Caller>,
}

pub(super) fn alice() -> ActingUser {
    ActingUser {
        id: 1,
        username: "alice".to_string(),
    }
}

impl ProviderRig {
    /// Stand up the bus, the registry, the provider, the caller if any, and
    /// the executor.
    pub async fn build(setup: ProviderSetup<'_>) -> ProviderRig {
        let ProviderSetup {
            slug,
            decls,
            native,
            grants: provider_grants,
            component,
            caller,
        } = setup;
        let (alert, _alerts) = noop_alert_dispatcher();
        let runtime = ProviderRuntime::new(alert.clone());
        let registry = Arc::new(ToolRegistry::with_provided(
            native,
            decls
                .iter()
                .map(|decl| Arc::new(ProvidedTool::new(slug, decl, runtime.clone())))
                .collect(),
        ));

        let defaults = MessagingGlobalConfig::default();
        let tuning = SystemChannelTuning::default();
        let requests = super::with_own_wasm_subscriber(
            slug,
            request_inbox_entry(slug, request_inbox_depth(slug, &decls), &defaults),
            request_inbox_subscription,
        );
        let (provider_results, provider_results_ch) =
            super::inbox_entry_with_own_subscriber(slug, &tuning, &defaults);
        let tool_names = registry.async_tool_names();
        let mut entries: Vec<ChannelEntry> = tool_names
            .iter()
            .map(|name| request_channel_entry(name, &tuning, &defaults))
            .collect();
        entries.extend([requests.clone(), provider_results.clone()]);
        let caller_inboxes = caller.as_ref().map(|caller| {
            let trigger = (*testutils::wasm_channel_entry(
                caller.slug,
                "trigger",
                Depth::Unbounded,
                Depth::Unbounded,
            ))
            .clone();
            let (results, results_ch) =
                super::inbox_entry_with_own_subscriber(caller.slug, &tuning, &defaults);
            entries.extend([trigger.clone(), results.clone()]);
            (trigger, results, results_ch)
        });
        brenn_messaging::system::fold_spec_subscriptions(
            &mut entries,
            &[tool_executor_spec(&tool_names)],
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
        let provider_sub = ParticipantId::for_wasm(slug);
        super::attach_input_ports(
            &messenger,
            slug,
            &provider_sub,
            &[
                (&requests, requests.resolved_channel.retain_depth),
                (&provider_results, Depth::Unbounded),
            ],
        )
        .await;
        if let (Some(caller), Some((trigger, results, _))) = (&caller, &caller_inboxes) {
            super::attach_input_ports(
                &messenger,
                caller.slug,
                &ParticipantId::for_wasm(caller.slug),
                &[(trigger, Depth::Unbounded), (results, Depth::Unbounded)],
            )
            .await;
        }
        super::attach_tool_executor(&messenger).await;

        registry.validate_grants(GrantOwner::WasmConsumer { slug }, &provider_grants);
        let provider_tools = Arc::new(WasmToolHost::new(
            registry.clone(),
            provider_grants.clone(),
            slug.to_string(),
            alert.clone(),
        ));
        let provider_component = Arc::new(ProcessorComponent::load(ProcessorLoadSpec {
            input_amplification_mt: HashMap::from([
                (TOOL_REQUEST_INPUT_PORT.to_string(), 1000),
                ("tool-results".to_string(), 1000),
            ]),
            alerter: noop_proc_alerter(),
            tool_host: Some(provider_tools.clone()),
            provides_host: Some(Arc::new(WasmProvidesHost::new(
                runtime.clone(),
                provider_tools,
            ))),
            ..component
        }));
        let provider = WasmConsumerConfig {
            slug: slug.to_string(),
            component: provider_component,
            notify: Arc::new(Notify::new()),
            messenger: messenger.clone(),
            alert_dispatcher: alert.clone(),
            inputs: vec![
                request_input_port(slug, &requests.resolved_channel),
                inbox_input_port(slug, &provider_results_ch),
            ],
            outputs: vec![],
            sync_ports: std::collections::BTreeSet::new(),
            activation_pacing: unthrottled_pacing(),
            provider: Some(runtime.clone()),
        };

        let mut caller_grants =
            HashMap::from([(provider_sub.as_str().to_string(), provider_grants.clone())]);
        let caller = caller
            .zip(caller_inboxes)
            .map(|(setup, (trigger, _, results_ch))| {
                let grants = BTreeMap::from([(setup.tool.to_string(), grant(vec![]))]);
                registry.validate_grants(GrantOwner::WasmConsumer { slug: setup.slug }, &grants);
                let sub = ParticipantId::for_wasm(setup.slug);
                caller_grants.insert(sub.as_str().to_string(), grants.clone());
                let component = Arc::new(ProcessorComponent::load(ProcessorLoadSpec {
                    input_amplification_mt: HashMap::from([
                        ("in".to_string(), 1000),
                        ("tool-results".to_string(), 1000),
                    ]),
                    config: HashMap::from([
                        ("tool".to_string(), setup.tool.to_string()),
                        ("args".to_string(), setup.args.to_string()),
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
                        grants,
                        setup.slug.to_string(),
                        alert.clone(),
                    ))),
                    ..ProcessorLoadSpec::minimal(Path::new(super::tool_e2e::TOOL_WASM), setup.slug)
                }));
                let component = WasmConsumerConfig {
                    slug: setup.slug.to_string(),
                    component,
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
                        inbox_input_port(setup.slug, &results_ch),
                    ],
                    outputs: vec![],
                    sync_ports: std::collections::BTreeSet::new(),
                    activation_pacing: unthrottled_pacing(),
                    provider: None,
                };
                Caller {
                    component,
                    sub,
                    trigger,
                }
            });

        let executor = ToolExecutor::new(
            messenger.clone(),
            registry.clone(),
            Arc::new(ToolCallerGrants::new(caller_grants)),
            runtime.clone(),
            alert.clone(),
            Arc::new(Notify::new()),
        );

        ProviderRig {
            messenger,
            registry,
            runtime,
            executor,
            alert,
            provider,
            provider_sub,
            provider_grants,
            provider_results,
            caller,
        }
    }

    /// The calling component; panics on a rig built without one.
    pub fn caller(&self) -> &Caller {
        self.caller
            .as_ref()
            .expect("the rig was built with a caller")
    }

    /// An app conversation acting for alice calls the tool `mcp_name`, on a
    /// task of its own: it waits for the provider as an agent's call does.
    pub fn call_for_alice(
        &self,
        mcp_name: &str,
        args: Value,
    ) -> JoinHandle<Result<Value, ToolError>> {
        let registry = self.registry.clone();
        let alert = self.alert.clone();
        let tool = format!("mcp__brenn__{mcp_name}");
        tokio::spawn(async move {
            registry
                .invoke_for_llm(
                    &tool,
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
    pub async fn owed(&self, sub: &ParticipantId, n: usize) -> Vec<Value> {
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
    pub async fn requested_id(&self) -> String {
        let requests = self.owed(&self.provider_sub, 1).await;
        requests[0]["id"]
            .as_str()
            .expect("a request carries its id")
            .to_string()
    }

    /// The tool executor, as a subscriber to the request channels.
    pub fn executor_sub(&self) -> ParticipantId {
        ParticipantId::for_system(TOOL_EXECUTOR_COMPONENT)
    }

    /// Activate the provider on whatever it is owed.
    pub async fn serve(&self) {
        drain_step(&self.provider, &self.provider_sub, MountDebt::Settled).await;
    }
}
