//! Guest-compiled async tool call, end to end.
//!
//! A real compiled WASM consumer (`processor-tool-test`) holding a tool grant
//! calls `call-async` with the tool and arguments its config names; the request
//! rides the transactional flush onto `brenn:tools/<tool>`; the `ToolExecutor`
//! dequeues it and runs the real tool's `execute`; the executor publishes the v1
//! result to the consumer's `brenn:tool-results/<slug>` inbox; and a second drain
//! activates the guest on its `tool-results` port, where it forwards the result
//! to "out" for observation.
//!
//! Two tools run the whole chain: `git-repo-pull` against a fixture git clone
//! one commit behind its remote (so the ff-only pull reports `Advanced`), and
//! `api-request` with a lent credential against a stub API server. These tests
//! exercise the chain through a compiled guest — the `WasmToolHost` seam, the
//! transactional request flush, the executor drain, the real tool `execute`, and
//! result-as-activation delivery — rather than any single stage in isolation.

use super::*;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;

use brenn_lib::messaging::config::SystemChannelTuning;
use brenn_lib::tools::ResolvedToolGrant;
use brenn_messaging::{Messenger, MountDebt};
use tokio::sync::Mutex;
use uuid::Uuid;

use brenn_git::sync::CloneInfo;
use brenn_tool_registry::bus_wiring::{
    inbox_input_port, request_channel_entry, tool_executor_spec, tool_executor_system_policy,
};
use brenn_tool_registry::executor::TOOL_EXECUTOR_COMPONENT;
use brenn_tool_registry::testutil::{clause, grant};
use brenn_tool_registry::{
    GitRepoPullTool, GrantOwner, RegisteredTool, ToolCallerGrants, ToolExecutor, ToolRegistry,
    WasmToolHost,
};
use git_fixture::scratch::scratch_remote_and_clone_behind_by_one;

const TOOL_WASM: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../brenn-wasm/target/components/brenn_processor_tool_test.wasm"
);

/// Publish amplification map for the two input ports this fixture binds: the
/// trigger ("in") and the async result inbox ("tool-results"). A window port
/// absent from the map is a host invariant violation (panics), so both must be
/// present even though "tool-results" is empty on the trigger activation.
fn tool_amp_map() -> HashMap<String, u64> {
    HashMap::from([
        ("in".to_string(), 1000u64),
        ("tool-results".to_string(), 1000u64),
    ])
}

/// The one async call the guest makes: the tool, its JSON arguments (handed to
/// the guest as its `tool` and `args` config), and the consumer's grant for it.
struct GuestCall {
    tool: &'static str,
    args: String,
    grant: ResolvedToolGrant,
}

/// The `git-repo-pull` call on the fixture clone `testclone`.
fn git_repo_pull_call() -> GuestCall {
    GuestCall {
        tool: "git-repo-pull",
        args: r#"{"repos":["testclone"]}"#.to_string(),
        grant: grant(vec![clause(&[("repo", "testclone")])]),
    }
}

/// The shared guest + bus wiring the tool e2e tests stand up.
struct ToolHarness {
    messenger: Arc<Messenger>,
    cfg: WasmConsumerConfig,
    trigger: ChannelEntry,
    guest_sub: ParticipantId,
    executor_sub: ParticipantId,
    /// The consumer's resolved tool grants, keyed by canonical tool name — the
    /// same map the guest's `WasmToolHost` holds. Reused verbatim by the caller
    /// that also drives the executor so the call-time and execute-time gates are
    /// one map, not two independent copies that can drift.
    tool_grants: BTreeMap<String, ResolvedToolGrant>,
}

/// Build the trigger, the request channel of `call.tool`, and the consumer's
/// result inbox (with its own Wasm subscriber), plus any `extra_channels` the
/// caller reads; a `Messenger` carrying the channel-derived subscribe policies
/// and — when `with_executor_policy` — the executor's code-built system policy;
/// and a `WasmConsumerConfig` whose guest binds the trigger and its inbox
/// through a real `WasmToolHost` over `registry`, holding `call.grant`.
async fn tool_harness(
    slug: &str,
    registry: Arc<ToolRegistry>,
    call: &GuestCall,
    extra_channels: Vec<ChannelEntry>,
    output_ports: HashMap<String, brenn_wasm::OutputPortSpec>,
    with_executor_policy: bool,
) -> ToolHarness {
    let defaults = MessagingGlobalConfig::default();
    let trigger =
        (*testutils::wasm_channel_entry(slug, "trigger", Depth::Unbounded, Depth::Unbounded))
            .clone();
    let request_ch = request_channel_entry(call.tool, &SystemChannelTuning::default(), &defaults);
    // The inbox: the bus-wiring entry (stable v5 uuid, matches `inbox_input_port`)
    // plus the consumer's own Wasm subscriber, which is what gives the consumer a
    // position on its own result inbox (and what `wasm_policies_from_entries`
    // derives its covering policy from).
    let (inbox, inbox_channel) =
        super::inbox_entry_with_own_subscriber(slug, &SystemChannelTuning::default(), &defaults);

    let inbox_ch = inbox.clone();
    let mut all_entries = vec![trigger.clone(), request_ch, inbox];
    all_entries.extend(extra_channels);
    // Fold the executor's spec subscription into the request channel, exactly
    // as bootstrap does.
    brenn_messaging::system::fold_spec_subscriptions(
        &mut all_entries,
        &[tool_executor_spec(&[call.tool])],
    );

    let db = init_db_memory_server_slice();
    {
        let conn = db.lock().await;
        upsert_channels(&conn, &all_entries);
    }
    let directory = Arc::new(MessagingDirectory::with_entries(all_entries.clone()));

    // The consumer/out-reader subscribe policies come from the channel subscriber
    // entries; the executor's publish/subscribe scope is the code-built system policy.
    let messenger = Messenger::new(
        db,
        directory,
        Arc::from("test"),
        Arc::new(IndexMap::new()),
        Arc::new(NoopWakeRouter) as Arc<dyn WakeRouter>,
        MessagingGlobalConfig::default(),
    )
    .with_subscriber_registrations(brenn_messaging::testutils::wasm_registrations(
        wasm_policies_from_entries(&all_entries),
    ));
    let messenger: Arc<Messenger> = if with_executor_policy {
        let mut system_policies = HashMap::new();
        system_policies.insert(
            TOOL_EXECUTOR_COMPONENT.to_string(),
            tool_executor_system_policy(),
        );
        messenger.with_subscriber_registrations(brenn_messaging::testutils::system_registrations(
            system_policies,
        ))
    } else {
        messenger
    };
    super::attach_input_ports(
        &messenger,
        slug,
        &ParticipantId::for_wasm(slug),
        &[(&trigger, Depth::Unbounded), (&inbox_ch, Depth::Unbounded)],
    )
    .await;
    super::attach_tool_executor(&messenger).await;

    // A detached noop alert task; dropping its JoinHandle leaves the task running.
    let (alert_dispatcher, _alert_handle) = noop_alert_dispatcher();
    let mut tool_grants = BTreeMap::new();
    tool_grants.insert(call.tool.to_string(), call.grant.clone());
    // The plan build's check on a consumer's grants, run on the same map.
    registry.validate_grants(GrantOwner::WasmConsumer { slug }, &tool_grants);
    let tool_host: brenn_wasm::ToolHostFn = Arc::new(WasmToolHost::new(
        Arc::clone(&registry),
        tool_grants.clone(),
        slug.to_string(),
        alert_dispatcher.clone(),
    ));

    let component = Arc::new(ProcessorComponent::load(ProcessorLoadSpec {
        declared_out_ports: output_ports.keys().cloned().collect(),
        output_ports,
        input_amplification_mt: tool_amp_map(),
        config: HashMap::from([
            ("tool".to_string(), call.tool.to_string()),
            ("args".to_string(), call.args.clone()),
        ]),
        grants: [
            ComponentGrant::Ports,
            ComponentGrant::Tools,
            ComponentGrant::Config,
        ]
        .into_iter()
        .collect(),
        alerter: noop_proc_alerter(),
        tool_host: Some(tool_host),
        ..ProcessorLoadSpec::minimal(Path::new(TOOL_WASM), slug)
    }));
    let cfg = WasmConsumerConfig {
        slug: slug.to_string(),
        component,
        notify: Arc::new(Notify::new()),
        messenger: Arc::clone(&messenger),
        alert_dispatcher,
        // Two triggering input ports: the external trigger and the consumer's own
        // result inbox. Folding the inbox into `inputs` (as bootstrap does via
        // `inbox_input_port`) is what makes a delivered result activate the guest
        // instead of being retired as residue.
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
            inbox_input_port(slug, &inbox_channel),
        ],
        outputs: vec![],
        sync_ports: std::collections::BTreeSet::new(),
        activation_pacing: unthrottled_pacing(),
        provider: None,
    };

    ToolHarness {
        messenger,
        cfg,
        trigger,
        guest_sub: ParticipantId::for_wasm(slug),
        executor_sub: ParticipantId::for_system(TOOL_EXECUTOR_COMPONENT),
        tool_grants,
    }
}

/// An output channel `brenn:<slug>:out` with its own reader subscriber, and the
/// guest's port map binding "out" to it. Returns the channel, the port map and
/// the reader's slug.
fn out_channel(
    slug: &str,
) -> (
    ChannelEntry,
    HashMap<String, brenn_wasm::OutputPortSpec>,
    String,
) {
    let out_reader_slug = format!("{slug}-out-reader");
    let out_addr = format!("brenn:{slug}:out");
    let out_ch = ChannelEntry {
        uuid: Uuid::new_v4(),
        address: out_addr.clone(),
        description: None,
        resolved_channel: ResolvedChannel {
            send_rate: Default::default(),
            push_depth: Depth::Unbounded,
            retain_depth: Depth::Unbounded,
            standing_retain_depth: Depth::Unbounded,
            noise: NoiseLevel::Silent,
            sink: Sink::Drop,
            wake_min: WakeMin::Normal,
        },
        subscribers: vec![SubscriberEntry {
            kind: SubscriberEntryKind::Wasm(out_reader_slug.clone()),
            push_depth: Depth::Unbounded,
            retain_depth: Depth::Unbounded,
            noise: NoiseLevel::Silent,
            wake_min: None,
        }],
        transport_type: ChannelScheme::Brenn,
        mount: None,
    };
    let output_ports = HashMap::from([("out".to_string(), test_out_spec(out_addr))]);
    (out_ch, output_ports, out_reader_slug)
}

/// Give the out channel's reader a position, or reading it would see an empty
/// window rather than the guest's forwarded result.
async fn attach_out_reader(harness: &ToolHarness, slug: &str, reader_slug: &str) -> ParticipantId {
    let out_sub = ParticipantId::for_wasm(reader_slug);
    let out_entry = harness
        .messenger
        .directory()
        .resolve(&format!("brenn:{slug}:out"))
        .expect("the out channel is registered");
    super::attach_input_ports(
        &harness.messenger,
        reader_slug,
        &out_sub,
        &[(out_entry.as_ref(), Depth::Unbounded)],
    )
    .await;
    out_sub
}

/// Deliver a trigger and drain the guest once: it fires `call-async`, flushed on
/// Ok. Asserts the trigger is acked and exactly one request reached the executor.
async fn trigger_one_call(harness: &ToolHarness) {
    testutils::insert_bus_message(
        &harness.messenger,
        &harness.trigger,
        "go",
        ChannelScheme::Brenn,
    )
    .await;
    drain_step(&harness.cfg, &harness.guest_sub, MountDebt::Settled).await;

    assert!(
        brenn_messaging::testutils::owed_everywhere(&harness.messenger, &harness.guest_sub)
            .await
            .is_empty(),
        "trigger row must be acked after the guest activation"
    );
    let requests =
        brenn_messaging::testutils::owed_everywhere(&harness.messenger, &harness.executor_sub)
            .await;
    assert_eq!(
        requests.len(),
        1,
        "exactly one async tool request flushed to the executor inbox"
    );
}

/// Drain the executor once against the same grant map the guest's host holds,
/// so the call-time and execute-time gates cannot drift; returns the one result
/// delivered to the consumer's inbox.
async fn execute_and_read_result(
    harness: &ToolHarness,
    registry: &Arc<ToolRegistry>,
) -> serde_json::Value {
    let caller_grants = Arc::new(ToolCallerGrants::new(HashMap::from([(
        harness.guest_sub.as_str().to_string(),
        harness.tool_grants.clone(),
    )])));
    let (exec_alert, _exec_alert_handle) = noop_alert_dispatcher();
    let executor = ToolExecutor::new(
        Arc::clone(&harness.messenger),
        Arc::clone(registry),
        caller_grants,
        brenn_tool_registry::ProviderRuntime::new(exec_alert.clone()),
        exec_alert,
        Arc::new(Notify::new()),
    );
    executor.drain_step().await;

    let inbox_rows =
        brenn_messaging::testutils::owed_everywhere(&harness.messenger, &harness.guest_sub).await;
    assert_eq!(
        inbox_rows.len(),
        1,
        "exactly one result row on the consumer inbox"
    );
    serde_json::from_str(&inbox_rows[0].1.body).expect("result body is JSON")
}

/// Drain the guest on its result activation and return the v1 result it
/// forwarded to "out" (the `body` of the verbatim inbox envelope).
async fn forwarded_result(harness: &ToolHarness, out_sub: &ParticipantId) -> serde_json::Value {
    drain_step(&harness.cfg, &harness.guest_sub, MountDebt::Settled).await;
    let out_rows = brenn_messaging::testutils::owed_everywhere(&harness.messenger, out_sub).await;
    assert_eq!(
        out_rows.len(),
        1,
        "the guest must forward exactly one result to out (proving it was activated)"
    );
    let forwarded_envelope: serde_json::Value =
        serde_json::from_str(&out_rows[0].1.body).expect("out body is an envelope JSON");
    serde_json::from_str(
        forwarded_envelope["body"]
            .as_str()
            .expect("envelope has a body string"),
    )
    .expect("forwarded result body is JSON")
}

#[tokio::test]
async fn guest_async_tool_call_pulls_fixture_and_delivers_advanced_result() {
    let slug = "sync";
    let remote = "ssh://example/testclone.git";

    // A fixture clone one commit behind its remote → the ff-only pull advances.
    let (_remote_dir, clone) = scratch_remote_and_clone_behind_by_one();
    let clones = Arc::new(HashMap::from([(
        "testclone".to_string(),
        CloneInfo {
            slug: "testclone".to_string(),
            host_path: clone.path().to_path_buf(),
            remote: remote.to_string(),
            sync_enabled: true,
            consumer_apps: HashSet::new(),
            primary_apps: HashSet::new(),
        },
    )]));
    let remote_locks = Arc::new(HashMap::from([(
        remote.to_string(),
        Arc::new(Mutex::new(())),
    )]));

    // One shared registry: the guest's `WasmToolHost` resolves the async request
    // against it, and the executor runs the tool from it.
    let registry = Arc::new(ToolRegistry::new(vec![RegisteredTool::Async(Arc::new(
        GitRepoPullTool::new(clones, remote_locks, None),
    ))]));

    let (out_ch, output_ports, out_reader_slug) = out_channel(slug);
    // The executor publishes results here, so the harness installs its system policy.
    let harness = tool_harness(
        slug,
        Arc::clone(&registry),
        &git_repo_pull_call(),
        vec![out_ch],
        output_ports,
        true,
    )
    .await;
    let out_sub = attach_out_reader(&harness, slug, &out_reader_slug).await;

    // --- Step 1: the guest fires `call-async`, flushed on Ok.
    trigger_one_call(&harness).await;

    // --- Step 2: the executor pulls the fixture and publishes the result to the
    // consumer's inbox with an Advanced outcome, correlated by the guest's `call-1`.
    let result = execute_and_read_result(&harness, &registry).await;
    assert_eq!(result["call_id"], "call-1");
    assert_eq!(result["tool"], "git-repo-pull");
    assert_eq!(
        result["outcome"]["ok"]["repos"][0]["advanced"], true,
        "the ff-only pull of the behind-by-one clone must report advanced; got {result}"
    );

    // --- Step 3: the result activates the guest on its `tool-results` port; the
    // guest forwards the result envelope to "out".
    let forwarded = forwarded_result(&harness, &out_sub).await;
    assert_eq!(forwarded["call_id"], "call-1");
    assert_eq!(
        forwarded["outcome"]["ok"]["repos"][0]["advanced"], true,
        "the guest-forwarded result must carry the Advanced outcome"
    );
}

/// A consumer with no acting user reads through `api-request` with the
/// credential its own clause lends it: the clause names the user and account,
/// the request's bearer is that stored credential's access token, the path is
/// admitted by the clause's segment template, and the API's answer comes back
/// to the guest on `tool-results`.
#[tokio::test]
async fn guest_api_request_reads_with_a_lent_credential() {
    use brenn_oauth::testutil;

    let slug = "agenda";
    let (base, stub) = testutil::start_stub().await;
    stub.lock().unwrap().api_body = br#"{"items":[]}"#.to_vec();
    let (service, _vault_dir) = testutil::service(vec![testutil::provider_config("stub", &base)]);
    // The lent credential, and the same account connected under another user:
    // the lent call must read alice's file by its direct path.
    let mut lent = testutil::credential("stub", "alice@example.com", "RT-alice");
    lent.access_token = "AT-alice".to_string();
    service.vault().store("alice", &lent).await;
    let mut other = testutil::credential("stub", "alice@example.com", "RT-bob");
    other.access_token = "AT-bob".to_string();
    service.vault().store("bob", &other).await;
    let registry = Arc::new(ToolRegistry::new(brenn_oauth::tools::registered(
        Some(&service),
        std::collections::BTreeMap::new(),
    )));

    let host = base
        .strip_prefix("http://")
        .expect("the stub is plain http");
    let call = GuestCall {
        tool: "api-request",
        args: serde_json::json!({
            "provider": "stub",
            "account": "alice@example.com",
            "method": "GET",
            "host": host,
            "path": "/calendar/v3/calendars/primary/events",
            "query": {"maxResults": "5"},
        })
        .to_string(),
        grant: grant(vec![clause(&[
            ("provider", "stub"),
            ("user", "alice"),
            ("account", "alice@example.com"),
            ("method", "GET"),
            ("host", host),
            ("path", "/calendar/v3/calendars/*/events"),
        ])]),
    };

    let (out_ch, output_ports, out_reader_slug) = out_channel(slug);
    let harness = tool_harness(
        slug,
        Arc::clone(&registry),
        &call,
        vec![out_ch],
        output_ports,
        true,
    )
    .await;
    let out_sub = attach_out_reader(&harness, slug, &out_reader_slug).await;

    trigger_one_call(&harness).await;
    let result = execute_and_read_result(&harness, &registry).await;
    assert_eq!(result["call_id"], "call-1");
    assert_eq!(result["tool"], "api-request");
    let ok = &result["outcome"]["ok"];
    assert_eq!(ok["status"], 200, "got {result}");
    assert_eq!(ok["body"], r#"{"items":[]}"#);
    assert_eq!(ok["truncated"], false);

    {
        let seen = &stub.lock().unwrap().api_seen;
        assert_eq!(seen.len(), 1, "exactly one request reached the API");
        assert_eq!(seen[0].method, "GET");
        assert_eq!(
            seen[0].path_and_query,
            "/calendar/v3/calendars/primary/events?maxResults=5"
        );
        assert_eq!(
            seen[0].headers.get("authorization").unwrap(),
            "Bearer AT-alice"
        );
    }

    let forwarded = forwarded_result(&harness, &out_sub).await;
    assert_eq!(forwarded["call_id"], "call-1");
    assert_eq!(forwarded["outcome"]["ok"]["status"], 200);
}

/// Trap-discard guarantee, guest level: a trap discards the requests buffered
/// during the activation. A compiled guest calls `call-async` and then aborts the
/// activation; the transactional flush must discard the buffered request so that
/// nothing reaches the executor's request inbox. This is the guest-compiled
/// counterpart to the unit-level proof that `take_ok_publishes` is reached only
/// on `Ok` — it exercises the whole flush path through a real component.
#[tokio::test]
async fn guest_trap_after_call_async_discards_the_buffered_request() {
    let slug = "sync";

    // The tool never runs in this test, but the registry must still hold it so
    // the guest's `WasmToolHost` resolves the async request at call time.
    let registry = Arc::new(brenn_tool_registry::testutil::git_repo_pull_only());

    // No outputs and no executor policy: the trap must drop the request before
    // anything reaches the executor at all.
    let harness = tool_harness(
        slug,
        registry,
        &git_repo_pull_call(),
        vec![],
        std::collections::HashMap::new(),
        false,
    )
    .await;
    let guest_sub = harness.guest_sub.clone();
    let executor_sub = harness.executor_sub.clone();

    // The trigger body carries the marker: the guest buffers the async call, then
    // returns Err — the activation fails and the flush must drop the request.
    testutils::insert_bus_message(
        &harness.messenger,
        &harness.trigger,
        "TRAP_AFTER_CALL",
        ChannelScheme::Brenn,
    )
    .await;
    drain_step(&harness.cfg, &guest_sub, MountDebt::Settled).await;

    // The activation actually ran: the trigger row is acked (drain acks at
    // activation start, before invoking the guest), so an empty guest inbox
    // means the drain assembled and fired the activation rather than the trigger
    // wiring silently regressing to a no-op.
    assert!(
        brenn_messaging::testutils::owed_everywhere(&harness.messenger, &guest_sub)
            .await
            .is_empty(),
        "the trigger row must be acked, proving the guest activation fired"
    );

    // The guest reached the *deliberate* trap after buffering the call — not an
    // early `call-async` error before buffering. Both surface as `receive`
    // returning Err (an empty executor inbox either way), so without this the
    // discard assertion below would pass vacuously if `call-async` ever failed
    // at call time. The quarantine diagnostic pins that `call-async` returned Ok
    // and the request really was buffered before the abort.
    let (outcome, diagnostic): (String, String) = {
        let conn = harness.messenger.db().lock().await;
        conn.query_row(
            "SELECT outcome, diagnostic FROM messaging_wasm_consume_failures \
             WHERE subscriber = ?1",
            rusqlite::params![guest_sub.as_str()],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .expect("exactly one quarantine row for the trapped guest activation")
    };
    assert_eq!(outcome, "err", "the guest returned Err, not a wasm trap");
    assert!(
        diagnostic.contains("intentional trap after call-async"),
        "the guest must have reached the deliberate post-buffer trap (proving \
         call-async returned Ok and buffered), not failed at call time; got: {diagnostic}"
    );

    // No request reached the executor's inbox: the trapped activation fired no
    // tool call, so the transactional flush discarded the buffered request.
    let requests =
        brenn_messaging::testutils::owed_everywhere(&harness.messenger, &executor_sub).await;
    assert!(
        requests.is_empty(),
        "a trapped activation must flush no tool request; got {} on the executor inbox",
        requests.len()
    );
}
