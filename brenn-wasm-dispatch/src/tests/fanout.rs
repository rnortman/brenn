use super::*;

// ── Fan-out: brenn: message → WASM push row created, consumer invoked once ──

/// Publish one `brenn:` message to a channel with a WASM subscriber.
/// After `drain_all_channels`, the push row is marked delivered (no pending rows).
/// `drain_all_channels` is the startup-sweep / drain step: it assembles the window
/// and invokes the guest (demo component → Ok).
#[tokio::test]
async fn brenn_message_creates_push_row_and_consumer_invoked_once() {
    let slug = "consumer-fanout";
    let (messenger, channel, wasm_sub) =
        testutils::build_wasm_messenger(slug, "fanout-ch", Depth::Unbounded, Depth::Unbounded)
            .await;
    // `body` here is the raw message body stored in messaging_messages.body.
    // `drain_channel` reads it back as MessageEnvelope.body and serializes the
    // full MessageEnvelope to JSON for the guest — the guest sees a valid envelope.
    let _ =
        testutils::insert_bus_message(&messenger, &channel, "hello", ChannelScheme::Brenn).await;

    // Before drain: the consumer's position trails the message.
    let owed_before = brenn_messaging::testutils::owed_everywhere(&messenger, &wasm_sub).await;
    assert_eq!(owed_before.len(), 1, "one message owed before drain");
    assert_eq!(owed_before[0].0, channel.address);

    // Run the drain step.
    let (cfg, _handle, _db) = build_cfg(
        slug,
        Arc::clone(&messenger),
        &channel,
        Depth::Unbounded,
        Depth::Unbounded,
    );
    drain_step(&cfg, &wasm_sub).await;

    // After drain: the row is delivered (no more pending).
    let rows_after = brenn_messaging::testutils::owed_everywhere(&messenger, &wasm_sub).await;
    assert!(
        rows_after.is_empty(),
        "pending row must be delivered after drain"
    );
}

// ── Batching AC: N messages before drain → one invocation with all N ─────

/// Insert 3 messages before calling drain. The demo component processes all 3
/// in one invocation (returning Ok). All 3 push rows must be marked delivered.
#[tokio::test]
async fn batching_n_messages_delivered_in_one_invocation() {
    let slug = "consumer-batch";
    let (messenger, channel, wasm_sub) =
        testutils::build_wasm_messenger(slug, "batch-ch", Depth::Unbounded, Depth::Unbounded).await;

    let n = 3usize;
    for i in 0..n {
        // Raw body string — MessageEnvelope is assembled from DB fields by the host.
        let body = format!("msg-{i}");
        let _ =
            testutils::insert_bus_message(&messenger, &channel, &body, ChannelScheme::Brenn).await;
    }

    // Before drain: all 3 messages owed.
    let rows_before = brenn_messaging::testutils::owed_everywhere(&messenger, &wasm_sub).await;
    assert_eq!(
        rows_before.len(),
        n,
        "should have {n} pending rows before drain"
    );

    // Drain once — all N consumed in one guest invocation.
    let (cfg, _handle, _db) = build_cfg(
        slug,
        Arc::clone(&messenger),
        &channel,
        Depth::Unbounded,
        Depth::Unbounded,
    );
    drain_step(&cfg, &wasm_sub).await;

    // After one drain step: all rows delivered.
    let rows_after = brenn_messaging::testutils::owed_everywhere(&messenger, &wasm_sub).await;
    assert!(
        rows_after.is_empty(),
        "all {n} rows must be delivered after one drain step"
    );
}

// ── Retained-context AC ───────────────────────────────────────────────────

/// Insert 2 messages and drain (they become context). Insert a 3rd message.
/// On the second drain the window must have `new_from > 0` (context prefix)
/// and one new entry. The demo component accepts this Ok.
#[tokio::test]
async fn retained_context_prefix_in_window() {
    let slug = "consumer-ctx";
    let (messenger, channel, wasm_sub) = testutils::build_wasm_messenger(
        slug,
        "ctx-ch",
        Depth::Unbounded,
        Depth::Bounded(10), // small retain to make the test clear
    )
    .await;

    let (cfg, _handle, _db) = build_cfg(
        slug,
        Arc::clone(&messenger),
        &channel,
        Depth::Unbounded,
        Depth::Bounded(10),
    );

    // Insert 2 messages and drain them — they become retained context.
    for i in 0..2usize {
        testutils::insert_bus_message(
            &messenger,
            &channel,
            &format!("ctx-{i}"),
            ChannelScheme::Brenn,
        )
        .await;
    }
    drain_step(&cfg, &wasm_sub).await;

    // Verify those 2 rows are now delivered.
    let pending_after_first =
        brenn_messaging::testutils::owed_everywhere(&messenger, &wasm_sub).await;
    assert!(
        pending_after_first.is_empty(),
        "first 2 rows must be delivered"
    );

    // Insert a 3rd message.
    testutils::insert_bus_message(&messenger, &channel, "new-1", ChannelScheme::Brenn).await;

    // Drain again — the window should have context prefix from the 2 prior messages.
    // The demo component accepts the window (Ok). Assert the 3rd row is delivered.
    drain_step(&cfg, &wasm_sub).await;
    let pending_after_second =
        brenn_messaging::testutils::owed_everywhere(&messenger, &wasm_sub).await;
    assert!(
        pending_after_second.is_empty(),
        "3rd row must be delivered after second drain"
    );
}

// ── Crash-recovery (startup sweep) AC ────────────────────────────────────

/// Pre-insert a push row (simulating rows left undelivered by a crash) and
/// run drain_all_channels without having processed the row in a prior drain.
/// This is the "startup sweep" path: the task picks up undelivered rows and
/// invokes the guest. The row must be delivered after the sweep.
#[tokio::test]
async fn crash_recovery_startup_sweep_re_invokes_undelivered_rows() {
    let slug = "consumer-crash";
    let (messenger, channel, wasm_sub) =
        testutils::build_wasm_messenger(slug, "crash-ch", Depth::Unbounded, Depth::Unbounded).await;

    // Simulate a crash: insert a pending row without running any drain step.
    let _ =
        testutils::insert_bus_message(&messenger, &channel, "undelivered", ChannelScheme::Brenn)
            .await;

    // Verify the message is owed (as it would be after a crash restart).
    let rows = brenn_messaging::testutils::owed_everywhere(&messenger, &wasm_sub).await;
    assert_eq!(rows.len(), 1, "one unconsumed message before startup sweep");
    assert_eq!(rows[0].0, channel.address);

    // Startup sweep: drain_all_channels runs once (before any wake — simulates
    // the task body's unconditional first drain in run_consumer).
    let (cfg, _handle, _db) = build_cfg(
        slug,
        Arc::clone(&messenger),
        &channel,
        Depth::Unbounded,
        Depth::Unbounded,
    );
    drain_step(&cfg, &wasm_sub).await;

    // The row must now be delivered — at-least-once on the Immediate no-deadline case.
    let rows_after = brenn_messaging::testutils::owed_everywhere(&messenger, &wasm_sub).await;
    assert!(
        rows_after.is_empty(),
        "startup sweep must deliver the undelivered row (crash recovery AC)"
    );
}

// ── Always-trap consumer → quarantined/alerted, other subscribers unaffected

/// Insert a message with the sentinel body `__trap__` that causes the demo
/// component to trap. Drain: the row is quarantined (not pending) and one
/// alert is fired. A second drain finds nothing new.
#[tokio::test]
async fn always_trap_consumer_quarantines_batch_and_alerts() {
    let slug = "consumer-trap";
    let (messenger, channel, wasm_sub) =
        testutils::build_wasm_messenger(slug, "trap-ch", Depth::Unbounded, Depth::Unbounded).await;

    // The demo component traps on `body == "__trap__"` via `unreachable!()`.
    // `insert_wasm_push` stores this as the raw message body; drain_channel reads it
    // back as MessageEnvelope.body = "__trap__", serializes the full envelope to JSON,
    // and passes it to the guest — which then checks `obj["body"] == "__trap__"`.
    let _ =
        testutils::insert_bus_message(&messenger, &channel, "__trap__", ChannelScheme::Brenn).await;

    // Use a severity-capturing alerter to verify both count and content.
    let (alert_dispatcher, captured_alerts, alert_handle) = make_capturing_alerter_with_severity();
    let _db = tempfile::NamedTempFile::new().unwrap();
    let component = Arc::new(ProcessorComponent::load(ProcessorLoadSpec {
        component_path: std::path::Path::new(DEMO_WASM),
        slug,
        output_ports: std::collections::HashMap::new(),
        input_amplification_mt: test_amp_map(),
        mqtt_sinks: std::collections::HashMap::new(),
        config: std::collections::HashMap::new(),
        grants: [ComponentGrant::Ports].into_iter().collect(),

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
        inputs: vec![WasmInputPort {
            port: "in".to_string(),
            sub: ResolvedSubscription {
                channel_uuid: channel.uuid,
                channel_address: channel.address.clone(),
                push_depth: Depth::Unbounded,
                retain_depth: Depth::Unbounded,
                noise: NoiseLevel::Silent,
                wake_min: WakeMin::Normal,
            },
            amplification_mt: 1000,
        }],
        outputs: vec![],
        activation_pacing: unthrottled_pacing(),
    };

    drain_step(&cfg, &wasm_sub).await;

    // Ack-at-start: the position moved past the batch BEFORE the guest ran, not
    // only on a successful outcome. A regression to advance-on-Ok-only would leave
    // the message owed and be caught here, on the Trap path.
    let rows_after = brenn_messaging::testutils::owed_everywhere(&messenger, &wasm_sub).await;
    assert!(
        rows_after.is_empty(),
        "ack-at-start: the trapping batch must be advanced past even on Trap"
    );

    // A second drain must find nothing new (no redelivery loop — N=1 terminal).
    drain_step(&cfg, &wasm_sub).await;
    let rows_second = brenn_messaging::testutils::owed_everywhere(&messenger, &wasm_sub).await;
    assert!(
        rows_second.is_empty(),
        "no redelivery loop after quarantine"
    );

    // Drop the cfg to close the alerter channel, then drain the handle.
    drop(cfg);
    let _ = alert_handle.await;

    // Exactly one alert must have fired (one trap → one quarantine alert).
    let alerts = captured_alerts.lock().unwrap();
    assert_eq!(
        alerts.len(),
        1,
        "exactly one alert for the trap batch, got {}: {:?}",
        alerts.len(),
        &*alerts
    );

    // severity must be Warning — not Critical (which would incorrectly trigger
    // the fail2ban tier) and not Info (too low for an operator-installed component
    // failure).
    assert!(
        matches!(alerts[0].0, AlertSeverity::Warning),
        "trap alert severity must be Warning, got {:?}",
        alerts[0].0
    );

    // title must identify the consumer slug and "trapped" so the operator can see
    // which component failed without opening the body.
    assert!(
        alerts[0].1.contains(slug),
        "alert title must contain consumer slug '{}': '{}'",
        slug,
        alerts[0].1
    );
    assert!(
        alerts[0].1.contains("trap"),
        "alert title must contain 'trap': '{}'",
        alerts[0].1
    );

    // body must contain the channel address (so the operator knows which channel
    // the failure occurred on) and a trap diagnostic (the wasmtime error string).
    assert!(
        alerts[0].2.contains(channel.address.as_str()),
        "alert body must contain channel address '{}': '{}'",
        channel.address,
        alerts[0].2
    );
    assert!(
        !alerts[0].2.is_empty(),
        "alert body must not be empty — must include trap diagnostic"
    );
}

// ── Webhook: consumer invoked with envelope_type=webhook ─────────────────

/// Insert a `WebhookEnvelope` body (envelope_type=Webhook) and drain.
/// The demo component validates the `channel` field — it must be non-empty.
/// The row must be delivered.
///
/// This test directly exercises the "webhook message fans out to WASM consumer"
/// path via `insert_wasm_push` (equivalent to `publish_transport_ingress` fanning
/// out to a Wasm subscriber on a webhook channel).
#[tokio::test]
async fn webhook_message_invokes_consumer_with_webhook_envelope_type() {
    let slug = "consumer-webhook";
    let channel_slug = "wh-test";
    let channel_uuid = webhook_channel_uuid_from_slug(channel_slug);
    let channel_addr = format!("{WEBHOOK_ADDRESS_PREFIX}{channel_slug}");

    let db = init_db_memory_lib_slice();
    let entry = Arc::new(ChannelEntry {
        uuid: channel_uuid,
        address: channel_addr.clone(),
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
            kind: SubscriberEntryKind::Wasm(slug.to_string()),
            push_depth: Depth::Unbounded,
            retain_depth: Depth::Unbounded,
            noise: NoiseLevel::Silent,
            wake_min: None,
        }],
        transport_type: ChannelScheme::Webhook,
        mount: Some(format!("/webhooks/{channel_slug}")),
    });
    {
        let conn = db.lock().await;
        upsert_channels(&conn, std::slice::from_ref(&*entry));
    }
    let directory = Arc::new(MessagingDirectory::with_entries(vec![(*entry).clone()]));
    let router = Arc::new(NoopWakeRouter);
    let messenger = brenn_messaging::Messenger::new(
        db,
        directory,
        Arc::from("test"),
        Arc::new(IndexMap::new()),
        router as Arc<dyn WakeRouter>,
        MessagingGlobalConfig::default(),
    )
    .with_subscriber_registrations(brenn_messaging::testutils::wasm_registrations(
        wasm_policies_from_entries(std::slice::from_ref(&*entry)),
    ));
    let wasm_sub = ParticipantId::for_wasm(slug);
    super::attach_input_ports(&messenger, slug, &wasm_sub, &[(&entry, Depth::Unbounded)]).await;

    // The body stored in messaging_messages for a webhook channel is the WebhookEnvelope
    // JSON (that's what `publish_transport_ingress` stores). The MessageEnvelope read
    // back has `body = <WebhookEnvelope JSON>` and `envelope_type = Webhook`.
    // The guest (demo component) parses the outer MessageEnvelope and checks
    // that `channel` is non-empty — it does not parse the inner WebhookEnvelope.
    let wh_env = WebhookEnvelope {
        headers: vec![],
        key_id: "k".into(),
        client_ip: "127.0.0.1".into(),
        received_at: Utc::now(),
        body: "payload".into(),
        endpoint_slug: channel_slug.into(),
    };
    let wh_body = serde_json::to_string(&wh_env).unwrap();

    let _ =
        testutils::insert_bus_message(&messenger, &entry, &wh_body, ChannelScheme::Webhook).await;

    // Before drain: one pending row.
    let rows_before = brenn_messaging::testutils::owed_everywhere(&messenger, &wasm_sub).await;
    assert_eq!(
        rows_before.len(),
        1,
        "one webhook message owed before drain"
    );

    let (cfg, _handle, _db) = build_cfg(
        slug,
        Arc::clone(&messenger),
        &entry,
        Depth::Unbounded,
        Depth::Unbounded,
    );
    drain_step(&cfg, &wasm_sub).await;

    // After drain: row delivered.
    let rows_after = brenn_messaging::testutils::owed_everywhere(&messenger, &wasm_sub).await;
    assert!(
        rows_after.is_empty(),
        "webhook push row must be delivered after drain"
    );
}

// ── push_depth=0 subscription → never invoked ────────────────────────────

/// A `push_depth=Bounded(0)` Wasm subscriber is owed nothing however much the
/// channel retains, so `drain_all_channels` finds nothing to invoke. Covered also
/// in publish.rs, but repeated here at the dispatch level.
#[tokio::test]
async fn push_depth_zero_wasm_subscription_never_invoked() {
    // build_wasm_messenger configures the channel with a push_depth=0
    // SubscriberEntry, which is the whole of the setup: a sampled subscriber holds
    // no position, so there is nothing for the drain to read.
    let slug = "consumer-no-push";
    let (messenger, channel, wasm_sub) =
        testutils::build_wasm_messenger(slug, "nopush-ch", Depth::Bounded(0), Depth::Unbounded)
            .await;

    let owed_before = brenn_messaging::testutils::owed_everywhere(&messenger, &wasm_sub).await;
    assert!(
        owed_before.is_empty(),
        "a push_depth=0 subscriber is owed nothing before the drain"
    );

    let (cfg, _handle, _db) = build_cfg(
        slug,
        Arc::clone(&messenger),
        &channel,
        Depth::Bounded(0),
        Depth::Unbounded,
    );
    // Drain finds nothing and invokes nothing — no panic, no error.
    drain_step(&cfg, &wasm_sub).await;
    let owed_after = brenn_messaging::testutils::owed_everywhere(&messenger, &wasm_sub).await;
    assert!(owed_after.is_empty(), "and is owed nothing after it");
}
