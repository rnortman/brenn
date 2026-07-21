//! `git-forge-parser` guest tests.
//!
//! Drives the real compiled `brenn_git_forge_parser.wasm` through the full
//! wasm_dispatch + Messenger path. The parser has two input ports (`forgejo`,
//! `github`) bound to webhook channels and one output port (`push-events`); it
//! republishes a normalized push event or drops (info/warn log) per the design.
//!
//! Input messages carry a serialized `WebhookEnvelope` as their body — exactly
//! the shape `deliver_inbound` publishes onto `webhook:<slug>`.

use super::*;

use brenn_lib::messaging::config::{NoiseLevel, ResolvedChannel, Sink};
use uuid::Uuid;

const GIT_FORGE_PARSER_WASM: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../brenn-wasm/target/components/brenn_git_forge_parser.wasm"
);

/// Build a parser setup: two webhook input channels (ports `forgejo` and
/// `github`) and one `brenn:` output channel (port `push-events`) with a reader
/// subscriber. Grants Ports + Log.
///
/// Returns `(messenger, forgejo_in, github_in, out_entry, out_sub, wasm_sub,
///           cfg, alert_handle, store_db)`.
#[allow(clippy::type_complexity)]
async fn build_parser_setup(
    slug: &str,
) -> (
    Arc<brenn_lib::messaging::Messenger>,
    Arc<ChannelEntry>,
    Arc<ChannelEntry>,
    Arc<ChannelEntry>,
    ParticipantId,
    ParticipantId,
    WasmConsumerConfig,
    tokio::task::JoinHandle<()>,
    tempfile::NamedTempFile,
) {
    let db = init_db_memory();
    let wasm_sub = ParticipantId::for_wasm(slug);
    let out_sub_slug = format!("{slug}-out-reader");
    let out_sub = ParticipantId::for_wasm(&out_sub_slug);

    // Two webhook input channels.
    let forgejo_addr = format!("{WEBHOOK_ADDRESS_PREFIX}{slug}-forgejo");
    let github_addr = format!("{WEBHOOK_ADDRESS_PREFIX}{slug}-github");
    let webhook_in = |addr: &str, sub_uuid: &str| ChannelEntry {
        uuid: webhook_channel_uuid_from_slug(sub_uuid),
        address: addr.to_string(),
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
            kind: SubscriberEntryKind::Wasm(slug.to_string()),
            push_depth: Depth::Unbounded,
            retain_depth: Depth::Unbounded,
            noise: NoiseLevel::Silent,
            wake_min: None,
        }],
        transport_type: ChannelScheme::Webhook,
        mount: Some(format!("/webhooks/{slug}-forgejo")),
    };
    let forgejo_entry = webhook_in(&forgejo_addr, &format!("{slug}-forgejo"));
    let github_entry = webhook_in(&github_addr, &format!("{slug}-github"));

    // Output channel.
    let out_addr = format!("brenn:{slug}:push-events");
    let out_entry_raw = ChannelEntry {
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
            kind: SubscriberEntryKind::Wasm(out_sub_slug.clone()),
            push_depth: Depth::Unbounded,
            retain_depth: Depth::Unbounded,
            noise: NoiseLevel::Silent,
            wake_min: None,
        }],
        transport_type: ChannelScheme::Brenn,
        mount: None,
    };

    let all_entries = vec![
        forgejo_entry.clone(),
        github_entry.clone(),
        out_entry_raw.clone(),
    ];
    {
        let conn = db.lock().await;
        upsert_channels(&conn, &all_entries);
    }
    let directory = Arc::new(MessagingDirectory::with_entries(all_entries.clone()));
    let router = Arc::new(NoopWakeRouter);
    let messenger = brenn_lib::messaging::Messenger::new(
        db,
        directory,
        Arc::from("test"),
        Arc::new(IndexMap::new()),
        router as Arc<dyn WakeRouter>,
        MessagingGlobalConfig::default(),
    )
    .with_subscriber_registrations(brenn_lib::messaging::testutils::wasm_registrations(
        wasm_policies_from_entries(&all_entries),
    ));

    let (alert_dispatcher, alert_handle) = noop_alert_dispatcher();
    let store_db = tempfile::NamedTempFile::new().unwrap();

    let mut amp = std::collections::HashMap::new();
    amp.insert("forgejo".to_string(), 1000u64);
    amp.insert("github".to_string(), 1000u64);

    let mut output_ports = std::collections::HashMap::new();
    output_ports.insert("push-events".to_string(), test_out_spec(out_addr));

    let component = Arc::new(ProcessorComponent::load(ProcessorLoadSpec {
        component_path: std::path::Path::new(GIT_FORGE_PARSER_WASM),
        slug,
        output_ports,
        input_amplification_mt: amp,
        mqtt_sinks: std::collections::HashMap::new(),
        config: std::collections::HashMap::new(),
        grants: [Capability::Ports, Capability::Log].into_iter().collect(),
        store_path: None,
        max_page_count: DEFAULT_MAX_PAGE_COUNT,
        max_payload_bytes: 1024 * 1024,
        alerter: noop_proc_alerter(),
        output_acl: allow_all(),
        mqtt_publish: None,
        tool_host: None,
    }));
    let notify = Arc::new(Notify::new());
    let cfg = WasmConsumerConfig {
        slug: slug.to_string(),
        component,
        notify,
        messenger: Arc::clone(&messenger),
        alert_dispatcher,
        inputs: vec![
            WasmInputPort {
                port: "forgejo".to_string(),
                sub: ResolvedSubscription {
                    channel_uuid: forgejo_entry.uuid,
                    channel_address: forgejo_entry.address.clone(),
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
                    channel_uuid: github_entry.uuid,
                    channel_address: github_entry.address.clone(),
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

    (
        messenger,
        Arc::new(forgejo_entry),
        Arc::new(github_entry),
        Arc::new(out_entry_raw),
        out_sub,
        wasm_sub,
        cfg,
        alert_handle,
        store_db,
    )
}

/// Build a `WebhookEnvelope` JSON body with the given headers, forge payload
/// body, and endpoint slug — the message body the parser reads.
fn webhook_body(headers: &[(&str, &str)], forge_body: &str, endpoint: &str) -> String {
    let env = WebhookEnvelope {
        headers: headers
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        key_id: "test-key".to_string(),
        client_ip: "203.0.113.7".to_string(),
        received_at: Utc::now(),
        body: forge_body.to_string(),
        endpoint_slug: endpoint.to_string(),
    };
    serde_json::to_string(&env).expect("serialize webhook envelope")
}

fn forgejo_payload(ssh: Option<&str>, clone: Option<&str>) -> String {
    let mut repo = serde_json::Map::new();
    if let Some(s) = ssh {
        repo.insert("ssh_url".to_string(), serde_json::json!(s));
    }
    if let Some(c) = clone {
        repo.insert("clone_url".to_string(), serde_json::json!(c));
    }
    serde_json::json!({ "repository": repo }).to_string()
}

#[tokio::test]
async fn forgejo_push_emits_event_both_remotes_ssh_first() {
    let slug = "gfp-forgejo-push";
    let (messenger, forgejo_in, _github_in, out_entry, out_sub, wasm_sub, cfg, _ah, _db) =
        build_parser_setup(slug).await;

    let payload = forgejo_payload(
        Some("ssh://git@forge/rn/brenn.git"),
        Some("https://forge/rn/brenn.git"),
    );
    let body = webhook_body(&[("x-forgejo-event", "push")], &payload, "git-forgejo");
    testutils::insert_wasm_push(
        &messenger,
        &forgejo_in,
        &wasm_sub,
        &body,
        ChannelScheme::Webhook,
    )
    .await;

    let mut last_seen = HashMap::new();
    drain_step(&cfg, &wasm_sub, &mut last_seen).await;

    let out_rows = messenger.load_pending_pushes(&out_sub).await;
    assert_eq!(out_rows.len(), 1, "exactly one push event published");
    let event = read_latest(&messenger, &out_entry.address)
        .await
        .expect("event present");
    assert_eq!(event["v"].as_u64(), Some(1));
    assert_eq!(event["event"].as_str(), Some("push"));
    assert_eq!(event["forge"].as_str(), Some("forgejo"));
    assert_eq!(event["endpoint"].as_str(), Some("git-forgejo"));
    let remotes = event["remotes"].as_array().expect("remotes array");
    assert_eq!(
        remotes.len(),
        2,
        "both remotes present, ssh first: {remotes:?}"
    );
    assert_eq!(remotes[0].as_str(), Some("ssh://git@forge/rn/brenn.git"));
    assert_eq!(remotes[1].as_str(), Some("https://forge/rn/brenn.git"));
    assert!(
        event["received_at"].as_str().is_some(),
        "received_at present"
    );
}

#[tokio::test]
async fn github_push_sets_forge_github() {
    let slug = "gfp-github-push";
    let (messenger, _forgejo_in, github_in, out_entry, out_sub, wasm_sub, cfg, _ah, _db) =
        build_parser_setup(slug).await;

    let payload = forgejo_payload(Some("git@github.com:rn/brenn.git"), None);
    let body = webhook_body(&[("x-github-event", "push")], &payload, "git-github");
    testutils::insert_wasm_push(
        &messenger,
        &github_in,
        &wasm_sub,
        &body,
        ChannelScheme::Webhook,
    )
    .await;

    let mut last_seen = HashMap::new();
    drain_step(&cfg, &wasm_sub, &mut last_seen).await;

    assert_eq!(messenger.load_pending_pushes(&out_sub).await.len(), 1);
    let event = read_latest(&messenger, &out_entry.address)
        .await
        .expect("event present");
    assert_eq!(event["forge"].as_str(), Some("github"));
    let remotes = event["remotes"].as_array().expect("remotes array");
    assert_eq!(remotes.len(), 1);
    assert_eq!(remotes[0].as_str(), Some("git@github.com:rn/brenn.git"));
}

#[tokio::test]
async fn forgejo_port_falls_back_to_gitea_event_header() {
    let slug = "gfp-gitea-fallback";
    let (messenger, forgejo_in, _github_in, _out_entry, out_sub, wasm_sub, cfg, _ah, _db) =
        build_parser_setup(slug).await;

    // Only the legacy X-Gitea-Event header is present.
    let payload = forgejo_payload(Some("ssh://git@forge/rn/pfin.git"), None);
    let body = webhook_body(&[("x-gitea-event", "push")], &payload, "git-forgejo");
    testutils::insert_wasm_push(
        &messenger,
        &forgejo_in,
        &wasm_sub,
        &body,
        ChannelScheme::Webhook,
    )
    .await;

    let mut last_seen = HashMap::new();
    drain_step(&cfg, &wasm_sub, &mut last_seen).await;

    assert_eq!(
        messenger.load_pending_pushes(&out_sub).await.len(),
        1,
        "gitea-event fallback must yield a push event"
    );
}

#[tokio::test]
async fn non_push_event_dropped_no_publish() {
    let slug = "gfp-non-push";
    let (messenger, forgejo_in, _github_in, _out_entry, out_sub, wasm_sub, cfg, _ah, _db) =
        build_parser_setup(slug).await;

    let payload = forgejo_payload(Some("ssh://git@forge/rn/brenn.git"), None);
    let body = webhook_body(&[("x-forgejo-event", "issues")], &payload, "git-forgejo");
    testutils::insert_wasm_push(
        &messenger,
        &forgejo_in,
        &wasm_sub,
        &body,
        ChannelScheme::Webhook,
    )
    .await;

    let mut last_seen = HashMap::new();
    drain_step(&cfg, &wasm_sub, &mut last_seen).await;

    // Input row acked, nothing published.
    assert!(messenger.load_pending_pushes(&wasm_sub).await.is_empty());
    assert!(
        messenger.load_pending_pushes(&out_sub).await.is_empty(),
        "non-push event must not publish"
    );
}

#[tokio::test]
async fn missing_event_header_dropped_no_publish() {
    let slug = "gfp-no-header";
    let (messenger, forgejo_in, _github_in, _out_entry, out_sub, wasm_sub, cfg, _ah, _db) =
        build_parser_setup(slug).await;

    let payload = forgejo_payload(Some("ssh://git@forge/rn/brenn.git"), None);
    let body = webhook_body(&[("x-some-other", "value")], &payload, "git-forgejo");
    testutils::insert_wasm_push(
        &messenger,
        &forgejo_in,
        &wasm_sub,
        &body,
        ChannelScheme::Webhook,
    )
    .await;

    let mut last_seen = HashMap::new();
    drain_step(&cfg, &wasm_sub, &mut last_seen).await;

    // Input row acked, nothing published — a warn-drop, distinct from a
    // quarantine (which also publishes nothing but records a failure).
    assert!(
        messenger.load_pending_pushes(&wasm_sub).await.is_empty(),
        "missing event header must ack its input row"
    );
    assert!(
        messenger.load_pending_pushes(&out_sub).await.is_empty(),
        "missing event header must not publish"
    );
    let failures: i64 = {
        let conn = messenger.db().lock().await;
        conn.query_row(
            "SELECT COUNT(*) FROM messaging_wasm_consume_failures WHERE subscriber = ?1",
            rusqlite::params![wasm_sub.as_str()],
            |r| r.get(0),
        )
        .expect("query failures")
    };
    assert_eq!(
        failures, 0,
        "missing event header is a warn-drop, not quarantine"
    );
}

#[tokio::test]
async fn malformed_payload_json_dropped_no_publish() {
    let slug = "gfp-bad-payload";
    let (messenger, forgejo_in, _github_in, _out_entry, out_sub, wasm_sub, cfg, _ah, _db) =
        build_parser_setup(slug).await;

    // Valid envelope + push header, but the forge body is not JSON.
    let body = webhook_body(
        &[("x-forgejo-event", "push")],
        "not json at all",
        "git-forgejo",
    );
    testutils::insert_wasm_push(
        &messenger,
        &forgejo_in,
        &wasm_sub,
        &body,
        ChannelScheme::Webhook,
    )
    .await;

    let mut last_seen = HashMap::new();
    drain_step(&cfg, &wasm_sub, &mut last_seen).await;

    assert!(
        messenger.load_pending_pushes(&out_sub).await.is_empty(),
        "malformed payload must not publish"
    );
    // A malformed forge payload is a warn-log drop, not quarantine.
    let failures: i64 = {
        let conn = messenger.db().lock().await;
        conn.query_row(
            "SELECT COUNT(*) FROM messaging_wasm_consume_failures WHERE subscriber = ?1",
            rusqlite::params![wasm_sub.as_str()],
            |r| r.get(0),
        )
        .expect("query failures")
    };
    assert_eq!(failures, 0, "warn-drop must not create a failure record");
}

#[tokio::test]
async fn ssh_only_and_clone_only_payloads() {
    let slug = "gfp-single-url";
    let (messenger, forgejo_in, _github_in, out_entry, out_sub, wasm_sub, cfg, _ah, _db) =
        build_parser_setup(slug).await;

    // clone-only.
    let payload = forgejo_payload(None, Some("https://forge/rn/graf.git"));
    let body = webhook_body(&[("x-forgejo-event", "push")], &payload, "git-forgejo");
    testutils::insert_wasm_push(
        &messenger,
        &forgejo_in,
        &wasm_sub,
        &body,
        ChannelScheme::Webhook,
    )
    .await;

    let mut last_seen = HashMap::new();
    drain_step(&cfg, &wasm_sub, &mut last_seen).await;

    assert_eq!(messenger.load_pending_pushes(&out_sub).await.len(), 1);
    let event = read_latest(&messenger, &out_entry.address)
        .await
        .expect("event present");
    let remotes = event["remotes"].as_array().expect("remotes array");
    assert_eq!(remotes.len(), 1);
    assert_eq!(remotes[0].as_str(), Some("https://forge/rn/graf.git"));
}

#[tokio::test]
async fn duplicate_ssh_clone_deduped() {
    let slug = "gfp-dedupe";
    let (messenger, forgejo_in, _github_in, out_entry, out_sub, wasm_sub, cfg, _ah, _db) =
        build_parser_setup(slug).await;

    let same = "ssh://git@forge/rn/brenn.git";
    let payload = forgejo_payload(Some(same), Some(same));
    let body = webhook_body(&[("x-forgejo-event", "push")], &payload, "git-forgejo");
    testutils::insert_wasm_push(
        &messenger,
        &forgejo_in,
        &wasm_sub,
        &body,
        ChannelScheme::Webhook,
    )
    .await;

    let mut last_seen = HashMap::new();
    drain_step(&cfg, &wasm_sub, &mut last_seen).await;

    let event = read_latest(&messenger, &out_entry.address)
        .await
        .expect("event present");
    let remotes = event["remotes"].as_array().expect("remotes array");
    assert_eq!(remotes.len(), 1, "identical ssh/clone deduped to one");
    let _ = out_sub;
}

#[tokio::test]
async fn malformed_envelope_quarantines() {
    let slug = "gfp-bad-envelope";
    let (messenger, forgejo_in, _github_in, _out_entry, out_sub, wasm_sub, cfg, alert_handle, _db) =
        build_parser_setup(slug).await;

    // Body is not a valid WebhookEnvelope at all → receive-error (quarantine).
    testutils::insert_wasm_push(
        &messenger,
        &forgejo_in,
        &wasm_sub,
        "{\"garbage\":true}",
        ChannelScheme::Webhook,
    )
    .await;

    let mut last_seen = HashMap::new();
    drain_step(&cfg, &wasm_sub, &mut last_seen).await;

    // Row acked (at-most-once), nothing published, failure recorded.
    assert!(messenger.load_pending_pushes(&wasm_sub).await.is_empty());
    assert!(messenger.load_pending_pushes(&out_sub).await.is_empty());
    let failures: i64 = {
        let conn = messenger.db().lock().await;
        conn.query_row(
            "SELECT COUNT(*) FROM messaging_wasm_consume_failures WHERE subscriber = ?1",
            rusqlite::params![wasm_sub.as_str()],
            |r| r.get(0),
        )
        .expect("query failures")
    };
    assert_eq!(failures, 1, "malformed envelope must quarantine");
    drop(cfg);
    let _ = alert_handle.await;
}
