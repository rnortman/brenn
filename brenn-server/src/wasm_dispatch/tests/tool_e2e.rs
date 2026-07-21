//! Guest-compiled async tool call, end to end (design §6 "End-to-end").
//!
//! The cycle-2 consumption path proven in cycle 1: a real compiled WASM consumer
//! (`processor-tool-test`) holding a `git-repo-pull` grant calls `call-async`;
//! the request rides the transactional flush onto `brenn:tools/git-repo-pull`;
//! the `ToolExecutor` dequeues it and runs `GitRepoPullTool::execute` against a
//! fixture git clone that is one commit behind its remote (so the ff-only pull
//! reports `Advanced`); the executor publishes the v1 result to the consumer's
//! `brenn:tool-results/sync` inbox; and a second drain activates the guest on its
//! `tool-results` port, where it forwards the result to "out" for observation.
//!
//! This is the one test that exercises the whole chain through a compiled guest —
//! the `WasmToolHost` seam, the transactional request flush, the executor drain,
//! the real tool `execute`, and result-as-activation delivery — rather than any
//! single stage in isolation.

use super::*;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;

use brenn_lib::messaging::Messenger;
use brenn_lib::tools::ResolvedToolGrant;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::repo_sync::CloneInfo;
use crate::repo_sync::test_git_fixtures::scratch_remote_and_clone_behind_by_one;
use crate::tool_registry::bus_wiring::{
    inbox_input_port, request_channel_entry, result_inbox_entry, tool_executor_spec,
    tool_executor_system_policy,
};
use crate::tool_registry::executor::TOOL_EXECUTOR_COMPONENT;
use crate::tool_registry::testutil::{clause, grant};
use crate::tool_registry::{
    GitRepoPullTool, RegisteredTool, ToolCallerGrants, ToolExecutor, ToolRegistry, WasmToolHost,
};

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

/// The shared guest + bus wiring both tool e2e tests stand up.
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

/// Build the trigger, the `git-repo-pull` request channel, and the consumer's
/// result inbox (with its own Wasm subscriber), plus any `extra_channels` the
/// caller reads; a `Messenger` carrying the channel-derived subscribe policies
/// and — when `with_executor_policy` — the executor's code-built system policy;
/// and a `WasmConsumerConfig` whose guest binds the trigger and its inbox
/// through a real `WasmToolHost` over `registry`.
async fn tool_harness(
    slug: &str,
    registry: Arc<ToolRegistry>,
    extra_channels: Vec<ChannelEntry>,
    output_ports: HashMap<String, brenn_wasm::OutputPortSpec>,
    with_executor_policy: bool,
) -> ToolHarness {
    let defaults = MessagingGlobalConfig::default();
    let trigger =
        (*testutils::wasm_channel_entry(slug, "trigger", Depth::Unbounded, Depth::Unbounded))
            .clone();
    let request_ch = request_channel_entry("git-repo-pull", &defaults);
    // The inbox: the bus-wiring entry (stable v5 uuid, matches `inbox_input_port`)
    // plus the consumer's own Wasm subscriber, so `wasm_policies_from_entries`
    // grants it delivery of its results and `resolve_push_targets` can push to it.
    let mut inbox = result_inbox_entry(slug, &defaults);
    inbox.subscribers.push(SubscriberEntry {
        kind: SubscriberEntryKind::Wasm(slug.to_string()),
        push_depth: Depth::Unbounded,
        retain_depth: Depth::Unbounded,
        noise: NoiseLevel::Silent,
        wake_min: None,
    });

    let mut all_entries = vec![trigger.clone(), request_ch, inbox];
    all_entries.extend(extra_channels);
    // Fold the executor's spec subscription into the request channel, exactly
    // as bootstrap does.
    brenn_lib::messaging::system::fold_spec_subscriptions(
        &mut all_entries,
        &[tool_executor_spec(&["git-repo-pull"])],
    );

    let db = init_db_memory();
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
    .with_subscriber_registrations(brenn_lib::messaging::testutils::wasm_registrations(
        wasm_policies_from_entries(&all_entries),
    ));
    let messenger: Arc<Messenger> = if with_executor_policy {
        let mut system_policies = HashMap::new();
        system_policies.insert(
            TOOL_EXECUTOR_COMPONENT.to_string(),
            tool_executor_system_policy(),
        );
        messenger.with_subscriber_registrations(
            brenn_lib::messaging::testutils::system_registrations(system_policies),
        )
    } else {
        messenger
    };

    // A detached noop alert task; dropping its JoinHandle leaves the task running.
    let (alert_dispatcher, _alert_handle) = noop_alert_dispatcher();
    let mut tool_grants = BTreeMap::new();
    tool_grants.insert(
        "git-repo-pull".to_string(),
        grant(vec![clause(&[("repo", "testclone")])]),
    );
    let tool_host: brenn_wasm::ToolHostFn = Arc::new(WasmToolHost::new(
        Arc::clone(&registry),
        tool_grants.clone(),
        slug.to_string(),
        alert_dispatcher.clone(),
    ));

    let component = Arc::new(ProcessorComponent::load(ProcessorLoadSpec {
        component_path: Path::new(TOOL_WASM),
        slug,
        output_ports,
        input_amplification_mt: tool_amp_map(),
        mqtt_sinks: std::collections::HashMap::new(),
        config: std::collections::HashMap::new(),
        grants: [Capability::Ports, Capability::Tools].into_iter().collect(),
        store_path: None,
        max_page_count: DEFAULT_MAX_PAGE_COUNT,
        max_payload_bytes: 1024 * 1024,
        alerter: noop_proc_alerter(),
        output_acl: allow_all(),
        mqtt_publish: None,
        tool_host: Some(tool_host),
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
                    push_depth: Depth::Unbounded,
                    retain_depth: Depth::Unbounded,
                    noise: NoiseLevel::Silent,
                    wake_min: WakeMin::Normal,
                },
                amplification_mt: 1000,
            },
            inbox_input_port(slug),
        ],
        activation_pacing: unthrottled_pacing(),
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

    // The one extra channel beyond the harness trio: an output the test reads,
    // with its own reader subscriber.
    let out_reader_slug = format!("{slug}-out-reader");
    let out_addr = format!("brenn:{slug}:out");
    let out_ch = ChannelEntry {
        uuid: Uuid::new_v4(),
        address: out_addr.clone(),
        description: None,
        resolved_channel: ResolvedChannel {
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

    let mut output_ports = std::collections::HashMap::new();
    output_ports.insert("out".to_string(), test_out_spec(out_addr.clone()));

    // The executor publishes results here, so the harness installs its system policy.
    let harness = tool_harness(
        slug,
        Arc::clone(&registry),
        vec![out_ch],
        output_ports,
        true,
    )
    .await;
    let guest_sub = harness.guest_sub.clone();
    let executor_sub = harness.executor_sub.clone();
    let out_sub = ParticipantId::for_wasm(&out_reader_slug);

    // --- Step 1: deliver a trigger; the guest fires `call-async`, flushed on Ok.
    testutils::insert_wasm_push(
        &harness.messenger,
        &harness.trigger,
        &guest_sub,
        "go",
        ChannelScheme::Brenn,
    )
    .await;
    let mut last_seen = HashMap::new();
    drain_step(&harness.cfg, &guest_sub, &mut last_seen).await;

    // The trigger row is acked; a request now sits on the executor's inbox.
    assert!(
        harness
            .messenger
            .load_pending_pushes(&guest_sub)
            .await
            .is_empty(),
        "trigger row must be acked after the guest activation"
    );
    let requests = harness.messenger.load_pending_pushes(&executor_sub).await;
    assert_eq!(
        requests.len(),
        1,
        "exactly one async tool request flushed to the executor inbox"
    );

    // --- Step 2: the executor drains the request, pulls the fixture, publishes
    // the result to the consumer's inbox. It runs against the same grant map the
    // guest's host holds, so the call-time and execute-time gates cannot drift.
    let caller_grants: Arc<ToolCallerGrants> = {
        let mut map: ToolCallerGrants = HashMap::new();
        map.insert(guest_sub.as_str().to_string(), harness.tool_grants.clone());
        Arc::new(map)
    };
    let (exec_alert, _exec_alert_handle) = noop_alert_dispatcher();
    let executor = ToolExecutor::new(
        Arc::clone(&harness.messenger),
        Arc::clone(&registry),
        caller_grants,
        exec_alert,
        Arc::new(Notify::new()),
    );
    executor.drain_step().await;

    // The result is delivered to the consumer's inbox with an Advanced outcome,
    // correlated by the guest's `call-1`.
    let inbox_rows = harness.messenger.load_pending_pushes(&guest_sub).await;
    assert_eq!(
        inbox_rows.len(),
        1,
        "exactly one result row on the consumer inbox"
    );
    let result: serde_json::Value = match &inbox_rows[0].1 {
        brenn_lib::messaging::IngressOrBus::Bus(env) => {
            serde_json::from_str(&env.body).expect("result body is JSON")
        }
        other => panic!("expected a bus result row, got {other:?}"),
    };
    assert_eq!(result["call_id"], "call-1");
    assert_eq!(result["tool"], "git-repo-pull");
    assert_eq!(
        result["outcome"]["ok"]["repos"][0]["advanced"], true,
        "the ff-only pull of the behind-by-one clone must report advanced; got {result}"
    );

    // --- Step 3: the result activates the guest on its `tool-results` port; the
    // guest forwards the result envelope to "out".
    drain_step(&harness.cfg, &guest_sub, &mut last_seen).await;
    let out_rows = harness.messenger.load_pending_pushes(&out_sub).await;
    assert_eq!(
        out_rows.len(),
        1,
        "the guest must forward exactly one result to out (proving it was activated)"
    );
    // The forwarded "out" body is the verbatim inbox envelope-json; its `body`
    // field is the v1 result the guest received.
    let forwarded_envelope: serde_json::Value = match &out_rows[0].1 {
        brenn_lib::messaging::IngressOrBus::Bus(env) => {
            serde_json::from_str(&env.body).expect("out body is an envelope JSON")
        }
        other => panic!("expected a bus row on out, got {other:?}"),
    };
    let forwarded_result: serde_json::Value = serde_json::from_str(
        forwarded_envelope["body"]
            .as_str()
            .expect("envelope has a body string"),
    )
    .expect("forwarded result body is JSON");
    assert_eq!(forwarded_result["call_id"], "call-1");
    assert_eq!(
        forwarded_result["outcome"]["ok"]["repos"][0]["advanced"], true,
        "the guest-forwarded result must carry the Advanced outcome"
    );
}

/// Trap-discard guarantee, guest level (design §6 "Async path": "trap-discards-
/// requests guarantee"). A compiled guest calls `call-async` and then aborts the
/// activation; the transactional flush must discard the buffered request so that
/// nothing reaches the executor's request inbox. This is the guest-compiled
/// counterpart to the unit-level proof that `take_ok_publishes` is reached only
/// on `Ok` — it exercises the whole flush path through a real component.
#[tokio::test]
async fn guest_trap_after_call_async_discards_the_buffered_request() {
    let slug = "sync";

    // The tool never runs in this test, but the registry must still hold it so
    // the guest's `WasmToolHost` resolves the async request at call time.
    let clones = Arc::new(HashMap::new());
    let remote_locks = Arc::new(HashMap::new());
    let registry = Arc::new(ToolRegistry::new(vec![RegisteredTool::Async(Arc::new(
        GitRepoPullTool::new(clones, remote_locks, None),
    ))]));

    // No outputs and no executor policy: the trap must drop the request before
    // anything reaches the executor at all.
    let harness = tool_harness(
        slug,
        registry,
        vec![],
        std::collections::HashMap::new(),
        false,
    )
    .await;
    let guest_sub = harness.guest_sub.clone();
    let executor_sub = harness.executor_sub.clone();

    // The trigger body carries the marker: the guest buffers the async call, then
    // returns Err — the activation fails and the flush must drop the request.
    testutils::insert_wasm_push(
        &harness.messenger,
        &harness.trigger,
        &guest_sub,
        "TRAP_AFTER_CALL",
        ChannelScheme::Brenn,
    )
    .await;
    drain_step(&harness.cfg, &guest_sub, &mut HashMap::new()).await;

    // The activation actually ran: the trigger row is acked (drain acks at
    // activation start, before invoking the guest), so an empty guest inbox
    // means the drain assembled and fired the activation rather than the trigger
    // wiring silently regressing to a no-op.
    assert!(
        harness
            .messenger
            .load_pending_pushes(&guest_sub)
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
    let requests = harness.messenger.load_pending_pushes(&executor_sub).await;
    assert!(
        requests.is_empty(),
        "a trapped activation must flush no tool request; got {} on the executor inbox",
        requests.len()
    );
}
