//! Full cross-component git-webhook pipeline, end to end.
//!
//! This is the one test that drives the entire pipeline through both real
//! compiled guests on a single `Messenger`, entered from a real HMAC-signed HTTP
//! POST:
//!
//! ```text
//! HTTP POST (valid forge HMAC)
//!   → receive handler + verify_request        (native ingress + signature)
//!   → WebhookEventRouterImpl::deliver_inbound  (webhook:git-<forge>, durable)
//!   → git-forge-parser guest                   (filter push, extract remotes)
//!   → brenn:git-repo-sync                       (normalized push event)
//!   → git-sync-consumer guest                  (match slug, call-async)
//!   → brenn:tools/git-repo-pull                 (async tool request)
//!   → ToolExecutor + GitRepoPullTool::execute  (ff-only pull of a fixture clone)
//!   → git-sync-consumer result activation      (advanced → outcome publish)
//!   → brenn:git-repo-sync-outcomes              (advanced:true)
//! ```
//!
//! The parser and consumer halves are each covered in isolation by
//! `git_forge_parser` and `git_sync_consumer`; this test proves they compose on
//! one bus, wired the way boot resolution wires them, and that the HMAC/HTTP
//! ingress feeds the guest chain. Run once per forge signature format (Forgejo
//! bare-hex `X-Gitea-Signature`, GitHub `sha256=<hex>` `X-Hub-Signature-256`).

use super::*;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::net::SocketAddr;
use std::path::Path;

use axum::Router;
use axum::body::Body;
use axum::extract::DefaultBodyLimit;
use axum::extract::connect_info::MockConnectInfo;
use axum::http::{Request, StatusCode};
use axum::middleware as axum_mw;
use axum::routing::post;
use brenn_lib::messaging::config::{NoiseLevel, ResolvedChannel, Sink};
use brenn_lib::messaging::{Messenger, Urgency};
use brenn_lib::tools::ResolvedToolGrant;
use brenn_lib::webhook::config::{ResolvedWebhookEndpoint, WebhookOwner};
use brenn_lib::webhook::service::WebhookService;
use brenn_lib::webhook::signature::{
    HexFormat, SignatureAlgorithm, SignatureScheme, hmac_sha256_hex,
};
use tokio::sync::Mutex;
use tower::ServiceExt;

use crate::client_ip::{TrustedProxyHops, resolve_client_ip};
use crate::repo_sync::CloneInfo;
use crate::repo_sync::test_git_fixtures::scratch_remote_and_clone_behind_by_one;
use crate::routes::webhooks::inbound::{EndpointSlug, receive};
use crate::tool_registry::bus_wiring::{
    inbox_input_port, request_channel_entry, result_inbox_entry, tool_executor_spec,
    tool_executor_system_policy,
};
use crate::tool_registry::executor::TOOL_EXECUTOR_COMPONENT;
use crate::tool_registry::testutil::{clause, grant};
use crate::tool_registry::{
    GitRepoPullTool, RegisteredTool, ToolCallerGrants, ToolExecutor, ToolRegistry, WasmToolHost,
};
use crate::webhook_router::WebhookEventRouterImpl;

const PARSER_WASM: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../brenn-wasm/target/components/brenn_git_forge_parser.wasm"
);
const CONSUMER_WASM: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../brenn-wasm/target/components/brenn_git_sync_consumer.wasm"
);

const PARSER_SLUG: &str = "git-forge-parser";
const CONSUMER_SLUG: &str = "git-sync-consumer";

/// The fixture clone slug + its remote URL, shared by the forge payload, the
/// consumer config map, the tool grant ACL, and the clone index.
const SLUG: &str = "testclone";
const REMOTE: &str = "ssh://example/testclone.git";

const FORGEJO_SECRET: &[u8] = b"forgejo-shared-secret";
const GITHUB_SECRET: &[u8] = b"github-shared-secret";

/// Which forge a pipeline run drives: selects the endpoint, its signature
/// format + credential header, the event header the parser reads, and the
/// secret the POST is signed with.
#[derive(Clone, Copy)]
struct Forge {
    endpoint_slug: &'static str,
    sig_header: &'static str,
    event_header: &'static str,
    format: HexFormat,
    secret: &'static [u8],
}

const FORGEJO: Forge = Forge {
    endpoint_slug: "git-forgejo",
    sig_header: "x-gitea-signature",
    event_header: "x-forgejo-event",
    format: HexFormat::Hex,
    secret: FORGEJO_SECRET,
};

const GITHUB: Forge = Forge {
    endpoint_slug: "git-github",
    sig_header: "x-hub-signature-256",
    event_header: "x-github-event",
    format: HexFormat::Sha256Hex,
    secret: GITHUB_SECRET,
};

/// Everything a pipeline run needs to keep alive plus the handles it drives.
struct Pipeline {
    messenger: Arc<Messenger>,
    axum_state: crate::state::AppState,
    parser_cfg: WasmConsumerConfig,
    consumer_cfg: WasmConsumerConfig,
    executor: ToolExecutor,
    parser_sub: ParticipantId,
    consumer_sub: ParticipantId,
    executor_sub: ParticipantId,
    // Kept alive for the pipeline's duration.
    _registry: Arc<ToolRegistry>,
    _remote_dir: tempfile::TempDir,
    _clone_dir: tempfile::TempDir,
    _consumer_store: tempfile::NamedTempFile,
}

/// A `webhook:<slug>` channel entry (stable slug-derived uuid, matching the
/// endpoint the router resolves) whose sole subscriber is the parser.
fn webhook_channel(endpoint_slug: &str) -> ChannelEntry {
    let address = format!("{WEBHOOK_ADDRESS_PREFIX}{endpoint_slug}");
    ChannelEntry {
        uuid: webhook_channel_uuid_from_slug(endpoint_slug),
        address,
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
            kind: SubscriberEntryKind::Wasm(PARSER_SLUG.to_string()),
            push_depth: Depth::Unbounded,
            retain_depth: Depth::Unbounded,
            noise: NoiseLevel::Silent,
            wake_min: None,
        }],
        transport_type: ChannelScheme::Webhook,
        mount: Some(format!("/webhooks/{endpoint_slug}")),
    }
}

/// A single-key `HmacRawBody` endpoint owned by the parser WASM consumer.
fn forge_endpoint(forge: Forge) -> Arc<ResolvedWebhookEndpoint> {
    let mut keys = HashMap::new();
    keys.insert("primary".to_string(), forge.secret.to_vec());
    Arc::new(ResolvedWebhookEndpoint {
        slug: forge.endpoint_slug.to_string(),
        mount: format!("/webhooks/{}", forge.endpoint_slug),
        description: None,
        transport_ceiling_bytes: 65536,
        content_type: "application/json".to_string(),
        scheme: SignatureScheme::HmacRawBody {
            algorithm: SignatureAlgorithm::HmacSha256,
            header: forge.sig_header.parse().unwrap(),
            format: forge.format,
            key_id_header: None,
            keys,
        },
        owner: WebhookOwner::Wasm(Arc::from(PARSER_SLUG)),
        urgency: Urgency::Normal,
        replay_protection: None,
    })
}

/// Format the signature header value a forge would send for `body`.
fn sign(forge: Forge, body: &[u8]) -> String {
    let hex = hmac_sha256_hex(forge.secret, body);
    match forge.format {
        HexFormat::Hex => hex,
        HexFormat::Sha256Hex => format!("sha256={hex}"),
        other => panic!("unexpected forge format {other:?}"),
    }
}

/// A forge push payload carrying `REMOTE` as the repository ssh url — the shape
/// both parsers accept (`repository.ssh_url`).
fn push_payload() -> String {
    serde_json::json!({ "repository": { "ssh_url": REMOTE } }).to_string()
}

/// Stand up the whole pipeline on one messenger: both forge endpoints (owned by
/// the parser), the two pipeline channels, the tool request channel and consumer
/// inbox, both compiled guests, and a `ToolExecutor` over a behind-by-one fixture
/// clone.
async fn build_pipeline() -> Pipeline {
    let defaults = MessagingGlobalConfig::default();

    // --- Channels: webhook ingress (parser-subscribed), pipeline, tool bus. ---
    let forgejo_wh = webhook_channel(FORGEJO.endpoint_slug);
    let github_wh = webhook_channel(GITHUB.endpoint_slug);
    let sync_ch = brenn_channel("brenn:git-repo-sync", CONSUMER_SLUG);
    let outcomes_addr = "brenn:git-repo-sync-outcomes".to_string();
    let outcomes_ch = brenn_channel(&outcomes_addr, "git-sync-outcomes-reader");
    let request_ch = request_channel_entry("git-repo-pull", &defaults);
    let mut inbox = result_inbox_entry(CONSUMER_SLUG, &defaults);
    inbox.subscribers.push(SubscriberEntry {
        kind: SubscriberEntryKind::Wasm(CONSUMER_SLUG.to_string()),
        push_depth: Depth::Unbounded,
        retain_depth: Depth::Unbounded,
        noise: NoiseLevel::Silent,
        wake_min: None,
    });

    let mut all_entries = vec![
        forgejo_wh.clone(),
        github_wh.clone(),
        sync_ch.clone(),
        outcomes_ch,
        request_ch,
        inbox,
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

    let mut system_policies = HashMap::new();
    system_policies.insert(
        TOOL_EXECUTOR_COMPONENT.to_string(),
        tool_executor_system_policy(),
    );
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
    ))
    .with_subscriber_registrations(brenn_lib::messaging::testutils::system_registrations(
        system_policies,
    ));

    // --- Tool registry over a fixture clone one commit behind its remote. ---
    let (remote_dir, clone_dir) = scratch_remote_and_clone_behind_by_one();
    let clones = Arc::new(HashMap::from([(
        SLUG.to_string(),
        CloneInfo {
            slug: SLUG.to_string(),
            host_path: clone_dir.path().to_path_buf(),
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

    // --- Parser guest: two forge input ports → push-events output. ---
    let parser_sub = ParticipantId::for_wasm(PARSER_SLUG);
    let (parser_alert, _parser_alert_handle) = noop_alert_dispatcher();
    let mut parser_amp = HashMap::new();
    parser_amp.insert("forgejo".to_string(), 1000u64);
    parser_amp.insert("github".to_string(), 1000u64);
    let mut parser_outputs = HashMap::new();
    parser_outputs.insert(
        "push-events".to_string(),
        test_out_spec("brenn:git-repo-sync".to_string()),
    );
    let parser_component = Arc::new(ProcessorComponent::load(ProcessorLoadSpec {
        component_path: Path::new(PARSER_WASM),
        slug: PARSER_SLUG,
        output_ports: parser_outputs,
        input_amplification_mt: parser_amp,
        mqtt_sinks: HashMap::new(),
        config: HashMap::new(),
        grants: [Capability::Ports, Capability::Log].into_iter().collect(),
        store_path: None,
        max_page_count: DEFAULT_MAX_PAGE_COUNT,
        max_payload_bytes: 1024 * 1024,
        alerter: noop_proc_alerter(),
        output_acl: allow_all(),
        mqtt_publish: None,
        tool_host: None,
    }));
    let parser_cfg = WasmConsumerConfig {
        slug: PARSER_SLUG.to_string(),
        component: parser_component,
        notify: Arc::new(Notify::new()),
        messenger: Arc::clone(&messenger),
        alert_dispatcher: parser_alert,
        inputs: vec![
            WasmInputPort {
                port: "forgejo".to_string(),
                sub: ResolvedSubscription {
                    channel_uuid: forgejo_wh.uuid,
                    channel_address: forgejo_wh.address.clone(),
                    push_depth: Depth::Unbounded,
                    retain_depth: Depth::Unbounded,
                    noise: NoiseLevel::Silent,
                    wake_min: WakeMin::Normal,
                },
                amplification_mt: 1000,
            },
            WasmInputPort {
                port: "github".to_string(),
                sub: ResolvedSubscription {
                    channel_uuid: github_wh.uuid,
                    channel_address: github_wh.address.clone(),
                    push_depth: Depth::Unbounded,
                    retain_depth: Depth::Unbounded,
                    noise: NoiseLevel::Silent,
                    wake_min: WakeMin::Normal,
                },
                amplification_mt: 1000,
            },
        ],
        activation_pacing: unthrottled_pacing(),
    };

    // --- Consumer guest: push-events + tool-results inbox → outcomes output. ---
    let consumer_sub = ParticipantId::for_wasm(CONSUMER_SLUG);
    let (consumer_alert, _consumer_alert_handle) = noop_alert_dispatcher();
    let mut tool_grants: BTreeMap<String, ResolvedToolGrant> = BTreeMap::new();
    tool_grants.insert(
        "git-repo-pull".to_string(),
        grant(vec![clause(&[("repo", SLUG)])]),
    );
    let tool_host: brenn_wasm::ToolHostFn = Arc::new(WasmToolHost::new(
        Arc::clone(&registry),
        tool_grants.clone(),
        CONSUMER_SLUG.to_string(),
        consumer_alert.clone(),
    ));
    let consumer_config = HashMap::from([
        ("repo_slugs".to_string(), SLUG.to_string()),
        (format!("remote:{SLUG}"), REMOTE.to_string()),
    ]);
    let consumer_store = tempfile::NamedTempFile::new().unwrap();
    let mut consumer_amp = HashMap::new();
    consumer_amp.insert("push-events".to_string(), 1000u64);
    consumer_amp.insert("tool-results".to_string(), 1000u64);
    let mut consumer_outputs = HashMap::new();
    consumer_outputs.insert("outcomes".to_string(), test_out_spec(outcomes_addr.clone()));
    let consumer_component = Arc::new(ProcessorComponent::load(ProcessorLoadSpec {
        component_path: Path::new(CONSUMER_WASM),
        slug: CONSUMER_SLUG,
        output_ports: consumer_outputs,
        input_amplification_mt: consumer_amp,
        mqtt_sinks: HashMap::new(),
        config: consumer_config,
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
        store_path: Some(consumer_store.path()),
        max_page_count: DEFAULT_MAX_PAGE_COUNT,
        max_payload_bytes: 1024 * 1024,
        alerter: noop_proc_alerter(),
        output_acl: allow_all(),
        mqtt_publish: None,
        tool_host: Some(tool_host),
    }));
    let consumer_cfg = WasmConsumerConfig {
        slug: CONSUMER_SLUG.to_string(),
        component: consumer_component,
        notify: Arc::new(Notify::new()),
        messenger: Arc::clone(&messenger),
        alert_dispatcher: consumer_alert,
        inputs: vec![
            WasmInputPort {
                port: "push-events".to_string(),
                sub: ResolvedSubscription {
                    channel_uuid: sync_ch.uuid,
                    channel_address: sync_ch.address.clone(),
                    push_depth: Depth::Unbounded,
                    retain_depth: Depth::Unbounded,
                    noise: NoiseLevel::Silent,
                    wake_min: WakeMin::Normal,
                },
                amplification_mt: 1000,
            },
            inbox_input_port(CONSUMER_SLUG),
        ],
        activation_pacing: unthrottled_pacing(),
    };

    // --- Tool executor over the same messenger + registry + consumer grant. ---
    let executor_sub = ParticipantId::for_system(TOOL_EXECUTOR_COMPONENT);
    let caller_grants: Arc<ToolCallerGrants> = {
        let mut map: ToolCallerGrants = HashMap::new();
        map.insert(consumer_sub.as_str().to_string(), tool_grants.clone());
        Arc::new(map)
    };
    let (exec_alert, _exec_alert_handle) = noop_alert_dispatcher();
    let executor = ToolExecutor::new(
        Arc::clone(&messenger),
        Arc::clone(&registry),
        caller_grants,
        exec_alert,
        Arc::new(Notify::new()),
    );

    // --- AppState + real WebhookEventRouterImpl for the HTTP ingress. ---
    let svc = WebhookService::new(vec![
        (FORGEJO.endpoint_slug.to_string(), forge_endpoint(FORGEJO)),
        (GITHUB.endpoint_slug.to_string(), forge_endpoint(GITHUB)),
    ]);
    let mut state = crate::state::AppState::for_test(messenger.db().clone(), None);
    state.messenger = Some(Arc::clone(&messenger));
    state.webhook = Some(svc.clone());
    let axum_state = state.clone();

    let real_router = Arc::new(WebhookEventRouterImpl::new());
    svc.set_router(
        Arc::clone(&real_router) as Arc<dyn brenn_lib::webhook::service::WebhookEventRouter>
    );
    real_router.set_state(state);

    Pipeline {
        messenger,
        axum_state,
        parser_cfg,
        consumer_cfg,
        executor,
        parser_sub,
        consumer_sub,
        executor_sub,
        _registry: registry,
        _remote_dir: remote_dir,
        _clone_dir: clone_dir,
        _consumer_store: consumer_store,
    }
}

/// Build the axum router serving `forge`'s mount, backed by the pipeline's
/// shared `AppState`. A fresh router per request (oneshot consumes it).
fn axum_router(pipeline: &Pipeline, forge: Forge) -> Router {
    Router::new()
        .route(
            &format!("/webhooks/{}", forge.endpoint_slug),
            post(receive).layer(
                tower::ServiceBuilder::new()
                    .layer(axum::Extension(EndpointSlug(
                        forge.endpoint_slug.to_string(),
                    )))
                    .layer(DefaultBodyLimit::max(65536)),
            ),
        )
        .with_state(pipeline.axum_state.clone())
        .layer(axum_mw::from_fn(resolve_client_ip))
        .layer(axum::Extension(TrustedProxyHops(0)))
        .layer(MockConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9999))))
}

/// Drive the whole chain for `forge` and return the terminal outcome event on
/// `brenn:git-repo-sync-outcomes`.
async fn run_pipeline(forge: Forge) -> serde_json::Value {
    let pipeline = build_pipeline().await;

    // --- Step 1: real HMAC-signed HTTP POST → 204, envelope on webhook:<slug>. ---
    let body = push_payload();
    let sig = sign(forge, body.as_bytes());
    let req = Request::builder()
        .method("POST")
        .uri(format!("/webhooks/{}", forge.endpoint_slug))
        .header("content-type", "application/json")
        .header(forge.sig_header, sig)
        .header(forge.event_header, "push")
        .body(Body::from(body))
        .unwrap();
    let resp = axum_router(&pipeline, forge).oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::NO_CONTENT,
        "a valid signed forge push must be accepted (204)"
    );

    // --- Step 2: parser activation → normalized push event on git-repo-sync. ---
    let mut parser_seen = HashMap::new();
    drain_step(&pipeline.parser_cfg, &pipeline.parser_sub, &mut parser_seen).await;
    let event = read_latest(&pipeline.messenger, "brenn:git-repo-sync")
        .await
        .expect("parser must publish a normalized push event");
    assert_eq!(event["event"], "push");
    assert_eq!(
        event["remotes"][0], REMOTE,
        "the fixture remote must survive normalization; got {event}"
    );

    // --- Step 3: consumer push activation → one call-async on the tool bus. ---
    let mut consumer_seen = HashMap::new();
    drain_step(
        &pipeline.consumer_cfg,
        &pipeline.consumer_sub,
        &mut consumer_seen,
    )
    .await;
    let requests = pipeline
        .messenger
        .load_pending_pushes(&pipeline.executor_sub)
        .await;
    assert_eq!(
        requests.len(),
        1,
        "the consumer must fire exactly one git-repo-pull request"
    );

    // --- Step 4: executor pulls the fixture and publishes the result. ---
    pipeline.executor.drain_step().await;

    // --- Step 5: consumer result activation → outcome event published. ---
    drain_step(
        &pipeline.consumer_cfg,
        &pipeline.consumer_sub,
        &mut consumer_seen,
    )
    .await;

    read_latest(&pipeline.messenger, "brenn:git-repo-sync-outcomes")
        .await
        .expect("consumer must publish an outcome event")
}

#[tokio::test]
async fn forgejo_push_flows_end_to_end_to_advanced_outcome() {
    let outcome = run_pipeline(FORGEJO).await;
    assert_eq!(outcome["v"], 1);
    assert_eq!(outcome["call_id"], "pull-1");
    assert_eq!(outcome["repos"][0]["slug"], SLUG);
    assert_eq!(
        outcome["repos"][0]["advanced"], true,
        "the ff-only pull of the behind-by-one clone must advance; got {outcome}"
    );
}

#[tokio::test]
async fn github_push_flows_end_to_end_to_advanced_outcome() {
    let outcome = run_pipeline(GITHUB).await;
    assert_eq!(outcome["v"], 1);
    assert_eq!(outcome["call_id"], "pull-1");
    assert_eq!(outcome["repos"][0]["slug"], SLUG);
    assert_eq!(
        outcome["repos"][0]["advanced"], true,
        "the ff-only pull of the behind-by-one clone must advance; got {outcome}"
    );
}
