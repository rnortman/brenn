//! `git-sync-consumer` guest tests.
//!
//! Drives the real compiled `brenn_git_sync_consumer.wasm` through the full
//! wasm_dispatch + Messenger path. The consumer has two input ports:
//! `push-events` (normalized forge push events from `git-forge-parser`) and
//! `tool-results` (the derived async tool-result inbox), and one output port
//! `outcomes`. On a push event it matches configured slugs by remote and fires
//! `call-async("git-repo-pull", …, "pull-<seq>")`; on a result it logs/alerts
//! per repo outcome and republishes an outcome event.
//!
//! `push_event_matches_and_pulls_fixture` is the consumer-half end-to-end: a
//! real push event → real `call-async` → the executor pulls a fixture clone →
//! the result activates the consumer → an outcome event lands on the outcomes
//! channel. The remaining tests exercise the individual result branches and the
//! misconfig quarantine in isolation.

use super::*;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use std::sync::Mutex as StdMutex;

use brenn_lib::messaging::Messenger;
use brenn_lib::messaging::config::NoiseLevel;
use brenn_lib::tools::ResolvedToolGrant;
use tokio::sync::Mutex;

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

const CONSUMER_WASM: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../brenn-wasm/target/components/brenn_git_sync_consumer.wasm"
);

/// The fixture clone slug + its remote URL, shared by the config map, the tool
/// grant ACL, and the clone index.
const SLUG: &str = "testclone";
const REMOTE: &str = "ssh://example/testclone.git";

/// A `ProcessorAlerter` that records the guest's `alert` grant emissions so a
/// test can assert which lanes fired.
#[derive(Clone)]
struct CapturingProcAlerter {
    events: Arc<StdMutex<Vec<(String, String, String)>>>,
}
impl ProcessorAlerter for CapturingProcAlerter {
    fn alert(&self, severity: GuestAlertSeverity, title: &str, body: &str) {
        self.events.lock().unwrap().push((
            format!("{severity:?}"),
            title.to_string(),
            body.to_string(),
        ));
    }
}

/// Everything a consumer test drives: the guest config + bus wiring, plus the
/// handles to inspect what the guest did.
struct ConsumerHarness {
    messenger: Arc<Messenger>,
    cfg: WasmConsumerConfig,
    /// The `brenn:git-repo-sync` push-event channel; the test inserts events here.
    push_ch: ChannelEntry,
    /// The consumer's result inbox; the branch tests insert synthetic results here.
    inbox_ch: ChannelEntry,
    outcomes_addr: String,
    guest_sub: ParticipantId,
    executor_sub: ParticipantId,
    tool_grants: BTreeMap<String, ResolvedToolGrant>,
    alerts: Arc<StdMutex<Vec<(String, String, String)>>>,
    _store: tempfile::NamedTempFile,
}

/// The default valid config map: one configured slug whose remote matches the
/// fixture, so a push carrying `REMOTE` matches exactly `SLUG`.
fn valid_config() -> HashMap<String, String> {
    HashMap::from([
        ("repo_slugs".to_string(), SLUG.to_string()),
        (format!("remote:{SLUG}"), REMOTE.to_string()),
    ])
}

/// Stand up the consumer over `registry`, with `config` and a grant ACL naming
/// `grant_repos`. `with_executor_policy` installs the executor's system policy so
/// a co-driven `ToolExecutor` may publish results.
async fn consumer_harness(
    slug: &str,
    registry: Arc<ToolRegistry>,
    config: HashMap<String, String>,
    grant_repos: &[&str],
    with_executor_policy: bool,
) -> ConsumerHarness {
    let defaults = MessagingGlobalConfig::default();

    // push-events input channel (the parser's output), subscribed by the consumer.
    let push_ch = brenn_channel("brenn:git-repo-sync", slug);
    // outcomes output channel, read by a separate reader.
    let outcomes_reader_slug = format!("{slug}-outcomes-reader");
    let outcomes_addr = "brenn:git-repo-sync-outcomes".to_string();
    let outcomes_ch = brenn_channel(&outcomes_addr, &outcomes_reader_slug);

    let request_ch = request_channel_entry("git-repo-pull", &defaults);
    let mut inbox = result_inbox_entry(slug, &defaults);
    inbox.subscribers.push(SubscriberEntry {
        kind: SubscriberEntryKind::Wasm(slug.to_string()),
        push_depth: Depth::Unbounded,
        retain_depth: Depth::Unbounded,
        noise: NoiseLevel::Silent,
        wake_min: None,
    });

    let mut all_entries = vec![
        push_ch.clone(),
        outcomes_ch.clone(),
        request_ch,
        inbox.clone(),
    ];
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

    let (alert_dispatcher, _alert_handle) = noop_alert_dispatcher();
    let mut tool_grants = BTreeMap::new();
    tool_grants.insert(
        "git-repo-pull".to_string(),
        grant(grant_repos.iter().map(|r| clause(&[("repo", r)])).collect()),
    );
    let tool_host: brenn_wasm::ToolHostFn = Arc::new(WasmToolHost::new(
        Arc::clone(&registry),
        tool_grants.clone(),
        slug.to_string(),
        alert_dispatcher.clone(),
    ));

    let alerts = Arc::new(StdMutex::new(Vec::new()));
    let store = tempfile::NamedTempFile::new().unwrap();

    let mut output_ports = HashMap::new();
    output_ports.insert("outcomes".to_string(), test_out_spec(outcomes_addr.clone()));

    let mut amp = HashMap::new();
    amp.insert("push-events".to_string(), 1000u64);
    amp.insert("tool-results".to_string(), 1000u64);

    let component = Arc::new(ProcessorComponent::load(ProcessorLoadSpec {
        component_path: Path::new(CONSUMER_WASM),
        slug,
        output_ports,
        input_amplification_mt: amp,
        mqtt_sinks: HashMap::new(),
        config,
        grants: [
            Capability::Ports,
            Capability::Store,
            Capability::Log,
            Capability::Alert,
            Capability::Config,
            Capability::Tools,
        ]
        .into_iter()
        .collect(),
        store_path: Some(store.path()),
        max_page_count: DEFAULT_MAX_PAGE_COUNT,
        max_payload_bytes: 1024 * 1024,
        alerter: Arc::new(CapturingProcAlerter {
            events: Arc::clone(&alerts),
        }),
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
        inputs: vec![
            WasmInputPort {
                port: "push-events".to_string(),
                sub: ResolvedSubscription {
                    channel_uuid: push_ch.uuid,
                    channel_address: push_ch.address.clone(),
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

    ConsumerHarness {
        messenger,
        cfg,
        push_ch,
        inbox_ch: inbox,
        outcomes_addr,
        guest_sub: ParticipantId::for_wasm(slug),
        executor_sub: ParticipantId::for_system(TOOL_EXECUTOR_COMPONENT),
        tool_grants,
        alerts,
        _store: store,
    }
}

/// A normalized push event body (what `git-forge-parser` publishes).
fn push_event(remotes: &[&str]) -> String {
    serde_json::json!({
        "v": 1,
        "event": "push",
        "forge": "forgejo",
        "endpoint": "git-forgejo",
        "remotes": remotes,
        "received_at": "2026-07-12T00:00:00Z",
    })
    .to_string()
}

/// A v1 tool-result envelope with an `ok.repos` array.
fn ok_result(call_id: &str, repos: serde_json::Value) -> String {
    serde_json::json!({
        "v": 1,
        "tool": "git-repo-pull",
        "call_id": call_id,
        "outcome": { "ok": { "repos": repos } },
    })
    .to_string()
}

/// Count quarantine rows for a subscriber.
async fn failure_count(messenger: &Messenger, sub: &ParticipantId) -> i64 {
    let conn = messenger.db().lock().await;
    conn.query_row(
        "SELECT COUNT(*) FROM messaging_wasm_consume_failures WHERE subscriber = ?1",
        rusqlite::params![sub.as_str()],
        |r| r.get(0),
    )
    .expect("query failures")
}

/// A registry holding `git-repo-pull` over the fixture clone (behind by one, so
/// the ff-only pull advances). Returns the registry plus the remote temp dir the
/// caller must keep alive.
fn fixture_registry() -> (Arc<ToolRegistry>, tempfile::TempDir, tempfile::TempDir) {
    let (remote_dir, clone) = scratch_remote_and_clone_behind_by_one();
    let clones = Arc::new(HashMap::from([(
        SLUG.to_string(),
        CloneInfo {
            slug: SLUG.to_string(),
            host_path: clone.path().to_path_buf(),
            remote: REMOTE.to_string(),
            sync_enabled: true,
            consumer_apps: HashSet::new(),
            primary_apps: HashSet::new(),
        },
    )]));
    let remote_locks = Arc::new(HashMap::from([(
        REMOTE.to_string(),
        Arc::new(Mutex::new(())),
    )]));
    let registry = Arc::new(ToolRegistry::new(vec![RegisteredTool::Async(Arc::new(
        GitRepoPullTool::new(clones, remote_locks, None),
    ))]));
    (registry, remote_dir, clone)
}

/// An empty-clone registry: the tool resolves at call time but never runs.
fn empty_registry() -> Arc<ToolRegistry> {
    Arc::new(ToolRegistry::new(vec![RegisteredTool::Async(Arc::new(
        GitRepoPullTool::new(Arc::new(HashMap::new()), Arc::new(HashMap::new()), None),
    ))]))
}

#[tokio::test]
async fn push_event_matches_and_pulls_fixture_to_outcome() {
    let slug = "gsc-e2e";
    let (registry, _remote, _clone) = fixture_registry();
    let harness =
        consumer_harness(slug, Arc::clone(&registry), valid_config(), &[SLUG], true).await;
    let guest_sub = harness.guest_sub.clone();
    let executor_sub = harness.executor_sub.clone();

    // Step 1: deliver a push whose remote matches the configured slug.
    testutils::insert_wasm_push(
        &harness.messenger,
        &harness.push_ch,
        &guest_sub,
        &push_event(&[REMOTE]),
        ChannelScheme::Brenn,
    )
    .await;
    let mut last_seen = HashMap::new();
    drain_step(&harness.cfg, &guest_sub, &mut last_seen).await;

    // The guest fired exactly one async call for the matched slug.
    let requests = harness.messenger.load_pending_pushes(&executor_sub).await;
    assert_eq!(requests.len(), 1, "one git-repo-pull request expected");
    let req: serde_json::Value = match &requests[0].1 {
        brenn_lib::messaging::IngressOrBus::Bus(env) => serde_json::from_str(&env.body).unwrap(),
        other => panic!("expected bus request, got {other:?}"),
    };
    assert_eq!(req["call_id"], "pull-1");
    assert_eq!(req["args"]["repos"][0], SLUG);

    // Step 2: the executor pulls the fixture and publishes the result.
    let caller_grants: Arc<ToolCallerGrants> = {
        let mut map: ToolCallerGrants = HashMap::new();
        map.insert(guest_sub.as_str().to_string(), harness.tool_grants.clone());
        Arc::new(map)
    };
    let (exec_alert, _h) = noop_alert_dispatcher();
    let executor = ToolExecutor::new(
        Arc::clone(&harness.messenger),
        registry,
        caller_grants,
        exec_alert,
        Arc::new(Notify::new()),
    );
    executor.drain_step().await;

    // Step 3: the result activates the consumer, which publishes the outcome.
    drain_step(&harness.cfg, &guest_sub, &mut last_seen).await;
    let outcome = read_latest(&harness.messenger, &harness.outcomes_addr)
        .await
        .expect("outcome event published");
    assert_eq!(outcome["v"], 1);
    assert_eq!(outcome["call_id"], "pull-1");
    assert_eq!(
        outcome["repos"][0]["advanced"], true,
        "the ff-only pull of the behind-by-one clone must report advanced; got {outcome}"
    );
    assert_eq!(outcome["repos"][0]["slug"], SLUG);
}

#[tokio::test]
async fn push_event_no_match_fires_no_call() {
    let slug = "gsc-nomatch";
    // Executor policy installed so the inbox-empty assertion is meaningful (a real
    // zero, not a zero from an unrecognized executor participant).
    let harness = consumer_harness(slug, empty_registry(), valid_config(), &[SLUG], true).await;
    let guest_sub = harness.guest_sub.clone();

    testutils::insert_wasm_push(
        &harness.messenger,
        &harness.push_ch,
        &guest_sub,
        &push_event(&["ssh://example/unconfigured.git"]),
        ChannelScheme::Brenn,
    )
    .await;
    drain_step(&harness.cfg, &guest_sub, &mut HashMap::new()).await;

    assert!(
        harness
            .messenger
            .load_pending_pushes(&harness.executor_sub)
            .await
            .is_empty(),
        "a push for an unconfigured remote must fire no tool call"
    );
    assert_eq!(failure_count(&harness.messenger, &guest_sub).await, 0);
}

#[tokio::test]
async fn call_id_sequence_is_monotonic_across_activations() {
    let slug = "gsc-seq";
    let harness = consumer_harness(slug, empty_registry(), valid_config(), &[SLUG], true).await;
    let guest_sub = harness.guest_sub.clone();
    let mut last_seen = HashMap::new();

    for _ in 0..2 {
        testutils::insert_wasm_push(
            &harness.messenger,
            &harness.push_ch,
            &guest_sub,
            &push_event(&[REMOTE]),
            ChannelScheme::Brenn,
        )
        .await;
        drain_step(&harness.cfg, &guest_sub, &mut last_seen).await;
    }

    let requests = harness
        .messenger
        .load_pending_pushes(&harness.executor_sub)
        .await;
    assert_eq!(requests.len(), 2, "two activations, two requests");
    let call_ids: Vec<String> = requests
        .iter()
        .map(|(_, row)| match row {
            brenn_lib::messaging::IngressOrBus::Bus(env) => {
                let v: serde_json::Value = serde_json::from_str(&env.body).unwrap();
                v["call_id"].as_str().unwrap().to_string()
            }
            other => panic!("expected bus request, got {other:?}"),
        })
        .collect();
    assert!(
        call_ids.contains(&"pull-1".to_string()) && call_ids.contains(&"pull-2".to_string()),
        "the store-backed counter must advance: {call_ids:?}"
    );
}

/// Feed a synthetic result and drain the consumer's result activation.
async fn drive_result(harness: &ConsumerHarness, result_body: &str) {
    testutils::insert_wasm_push(
        &harness.messenger,
        &harness.inbox_ch,
        &harness.guest_sub,
        result_body,
        ChannelScheme::Brenn,
    )
    .await;
    drain_step(&harness.cfg, &harness.guest_sub, &mut HashMap::new()).await;
}

#[tokio::test]
async fn transient_error_warns_but_still_publishes_outcome() {
    let slug = "gsc-transient";
    let harness = consumer_harness(slug, empty_registry(), valid_config(), &[SLUG], false).await;

    let repos = serde_json::json!([
        { "slug": SLUG, "ok": false, "error_type": "transient", "error": "connection refused" }
    ]);
    drive_result(&harness, &ok_result("pull-7", repos)).await;

    // Transient is a warn-log lane, not an alert.
    assert!(
        harness.alerts.lock().unwrap().is_empty(),
        "transient errors must not alert"
    );
    let outcome = read_latest(&harness.messenger, &harness.outcomes_addr)
        .await
        .expect("ok outcome still publishes");
    assert_eq!(outcome["call_id"], "pull-7");
}

#[tokio::test]
async fn auth_and_unknown_errors_alert_and_publish() {
    let slug = "gsc-auth";
    let harness = consumer_harness(slug, empty_registry(), valid_config(), &[SLUG], false).await;

    let repos = serde_json::json!([
        { "slug": SLUG, "ok": false, "error_type": "auth", "error": "Permission denied", "detail": "publickey" },
        { "slug": "other", "ok": false, "error_type": "unknown", "error": "no clone configured" }
    ]);
    drive_result(&harness, &ok_result("pull-8", repos)).await;

    assert_eq!(
        harness.alerts.lock().unwrap().len(),
        2,
        "auth + unknown both alert (human-actionable)"
    );
    assert!(
        read_latest(&harness.messenger, &harness.outcomes_addr)
            .await
            .is_some(),
        "ok outcome publishes even when some repos failed"
    );
}

#[tokio::test]
async fn outcome_err_alerts_and_publishes_nothing() {
    let slug = "gsc-err";
    let harness = consumer_harness(slug, empty_registry(), valid_config(), &[SLUG], false).await;

    let result = serde_json::json!({
        "v": 1,
        "tool": "git-repo-pull",
        "call_id": "pull-9",
        "outcome": { "err": { "kind": "not-granted", "detail": "grant revoked" } },
    })
    .to_string();
    drive_result(&harness, &result).await;

    assert_eq!(
        harness.alerts.lock().unwrap().len(),
        1,
        "an outcome.err must alert"
    );
    assert!(
        read_latest(&harness.messenger, &harness.outcomes_addr)
            .await
            .is_none(),
        "an outcome.err must publish no outcome event"
    );
}

#[tokio::test]
async fn missing_remote_config_quarantines() {
    let slug = "gsc-misconfig";
    // repo_slugs lists a slug with no matching remote:<slug> key.
    let config = HashMap::from([("repo_slugs".to_string(), format!("{SLUG},missing"))]);
    let harness = consumer_harness(slug, empty_registry(), config, &[SLUG], false).await;
    let guest_sub = harness.guest_sub.clone();

    testutils::insert_wasm_push(
        &harness.messenger,
        &harness.push_ch,
        &guest_sub,
        &push_event(&[REMOTE]),
        ChannelScheme::Brenn,
    )
    .await;
    drain_step(&harness.cfg, &guest_sub, &mut HashMap::new()).await;

    assert_eq!(
        failure_count(&harness.messenger, &guest_sub).await,
        1,
        "a repo_slugs entry with no remote:<slug> key must quarantine"
    );
}

#[tokio::test]
async fn empty_remote_config_quarantines() {
    let slug = "gsc-emptyremote";
    // repo_slugs lists a slug whose remote:<slug> value is whitespace-only — a
    // dead map entry that could never match any event. Fail fast rather than
    // silently sync nothing.
    let config = HashMap::from([
        ("repo_slugs".to_string(), SLUG.to_string()),
        (format!("remote:{SLUG}"), "   ".to_string()),
    ]);
    let harness = consumer_harness(slug, empty_registry(), config, &[SLUG], false).await;
    let guest_sub = harness.guest_sub.clone();

    testutils::insert_wasm_push(
        &harness.messenger,
        &harness.push_ch,
        &guest_sub,
        &push_event(&[REMOTE]),
        ChannelScheme::Brenn,
    )
    .await;
    drain_step(&harness.cfg, &guest_sub, &mut HashMap::new()).await;

    assert_eq!(
        failure_count(&harness.messenger, &guest_sub).await,
        1,
        "an empty remote:<slug> value must quarantine"
    );
}

#[tokio::test]
async fn duplicate_slug_config_quarantines() {
    let slug = "gsc-dupslug";
    // The same slug listed twice in repo_slugs is an operator misconfig; fail
    // fast rather than issue a doubled pull.
    let config = HashMap::from([
        ("repo_slugs".to_string(), format!("{SLUG},{SLUG}")),
        (format!("remote:{SLUG}"), REMOTE.to_string()),
    ]);
    let harness = consumer_harness(slug, empty_registry(), config, &[SLUG], false).await;
    let guest_sub = harness.guest_sub.clone();

    testutils::insert_wasm_push(
        &harness.messenger,
        &harness.push_ch,
        &guest_sub,
        &push_event(&[REMOTE]),
        ChannelScheme::Brenn,
    )
    .await;
    drain_step(&harness.cfg, &guest_sub, &mut HashMap::new()).await;

    assert_eq!(
        failure_count(&harness.messenger, &guest_sub).await,
        1,
        "a duplicate slug in repo_slugs must quarantine"
    );
}

#[tokio::test]
async fn denied_tool_call_quarantines() {
    let slug = "gsc-denied";
    // The slug is configured (map resolves fine) but the tool grant ACL names a
    // different repo, so the `call-async` for the matched slug is denied at call
    // time → the guest returns Err → the activation quarantines. (An empty ACL
    // would admit all, so the ACL must name a non-matching repo to force denial.)
    // A regression that swallowed the denial (Ok + silent drop) instead of
    // quarantining would flip failure_count to 0.
    let harness = consumer_harness(slug, empty_registry(), valid_config(), &["other"], true).await;
    let guest_sub = harness.guest_sub.clone();

    testutils::insert_wasm_push(
        &harness.messenger,
        &harness.push_ch,
        &guest_sub,
        &push_event(&[REMOTE]),
        ChannelScheme::Brenn,
    )
    .await;
    drain_step(&harness.cfg, &guest_sub, &mut HashMap::new()).await;

    assert_eq!(
        failure_count(&harness.messenger, &guest_sub).await,
        1,
        "a denied tool call must quarantine, not drop silently"
    );
    assert!(
        harness
            .messenger
            .load_pending_pushes(&harness.executor_sub)
            .await
            .is_empty(),
        "a denied call must place no request on the executor inbox"
    );
}

#[tokio::test]
async fn unknown_push_event_schema_version_quarantines() {
    let slug = "gsc-badver";
    let harness = consumer_harness(slug, empty_registry(), valid_config(), &[SLUG], true).await;
    let guest_sub = harness.guest_sub.clone();

    // A push event carrying an incompatible schema version must quarantine, not
    // be silently accepted.
    let body = serde_json::json!({
        "v": 2,
        "event": "push",
        "remotes": [REMOTE],
    })
    .to_string();
    testutils::insert_wasm_push(
        &harness.messenger,
        &harness.push_ch,
        &guest_sub,
        &body,
        ChannelScheme::Brenn,
    )
    .await;
    drain_step(&harness.cfg, &guest_sub, &mut HashMap::new()).await;

    assert_eq!(
        failure_count(&harness.messenger, &guest_sub).await,
        1,
        "an unknown push-event schema version must quarantine"
    );
    assert!(
        harness
            .messenger
            .load_pending_pushes(&harness.executor_sub)
            .await
            .is_empty(),
        "an unknown schema version must fire no tool call"
    );
}
