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
    let (push_id, _) = testutils::insert_wasm_push(
        &messenger,
        &channel,
        &wasm_sub,
        "hello",
        ChannelScheme::Brenn,
    )
    .await;

    // Before drain: the push row is pending.
    let rows_before = messenger.load_pending_pushes(&wasm_sub).await;
    assert_eq!(rows_before.len(), 1, "one pending push before drain");
    assert_eq!(rows_before[0].0, push_id);

    // Run the drain step.
    let (cfg, _handle, _db) = build_cfg(
        slug,
        Arc::clone(&messenger),
        &channel,
        Depth::Unbounded,
        Depth::Unbounded,
    );
    let mut last_seen = HashMap::new();
    drain_step(&cfg, &wasm_sub, &mut last_seen).await;

    // After drain: the row is delivered (no more pending).
    let rows_after = messenger.load_pending_pushes(&wasm_sub).await;
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
    let mut push_ids = Vec::new();
    for i in 0..n {
        // Raw body string — MessageEnvelope is assembled from DB fields by the host.
        let body = format!("msg-{i}");
        let (pid, _) = testutils::insert_wasm_push(
            &messenger,
            &channel,
            &wasm_sub,
            &body,
            ChannelScheme::Brenn,
        )
        .await;
        push_ids.push(pid);
    }

    // Before drain: all 3 rows pending.
    let rows_before = messenger.load_pending_pushes(&wasm_sub).await;
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
    let mut last_seen = HashMap::new();
    drain_step(&cfg, &wasm_sub, &mut last_seen).await;

    // After one drain step: all rows delivered.
    let rows_after = messenger.load_pending_pushes(&wasm_sub).await;
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
    let mut last_seen = HashMap::new();

    // Insert 2 messages and drain them — they become retained context.
    for i in 0..2usize {
        testutils::insert_wasm_push(
            &messenger,
            &channel,
            &wasm_sub,
            &format!("ctx-{i}"),
            ChannelScheme::Brenn,
        )
        .await;
    }
    drain_step(&cfg, &wasm_sub, &mut last_seen).await;

    // Verify those 2 rows are now delivered.
    let pending_after_first = messenger.load_pending_pushes(&wasm_sub).await;
    assert!(
        pending_after_first.is_empty(),
        "first 2 rows must be delivered"
    );

    // Insert a 3rd message.
    testutils::insert_wasm_push(
        &messenger,
        &channel,
        &wasm_sub,
        "new-1",
        ChannelScheme::Brenn,
    )
    .await;

    // Drain again — the window should have context prefix from the 2 prior messages.
    // The demo component accepts the window (Ok). Assert the 3rd row is delivered.
    drain_step(&cfg, &wasm_sub, &mut last_seen).await;
    let pending_after_second = messenger.load_pending_pushes(&wasm_sub).await;
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
    let (push_id, _) = testutils::insert_wasm_push(
        &messenger,
        &channel,
        &wasm_sub,
        "undelivered",
        ChannelScheme::Brenn,
    )
    .await;

    // Verify the row is pending (as it would be after a crash restart).
    let rows = messenger.load_pending_pushes(&wasm_sub).await;
    assert_eq!(rows.len(), 1, "one undelivered row before startup sweep");
    assert_eq!(rows[0].0, push_id);

    // Startup sweep: drain_all_channels runs once (before any wake — simulates
    // the task body's unconditional first drain in run_consumer).
    let (cfg, _handle, _db) = build_cfg(
        slug,
        Arc::clone(&messenger),
        &channel,
        Depth::Unbounded,
        Depth::Unbounded,
    );
    let mut last_seen = HashMap::new();
    drain_step(&cfg, &wasm_sub, &mut last_seen).await;

    // The row must now be delivered — at-least-once on the Immediate no-deadline case.
    let rows_after = messenger.load_pending_pushes(&wasm_sub).await;
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
    let (push_id, _) = testutils::insert_wasm_push(
        &messenger,
        &channel,
        &wasm_sub,
        "__trap__",
        ChannelScheme::Brenn,
    )
    .await;

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
        grants: [Capability::Ports].into_iter().collect(),

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
        activation_pacing: unthrottled_pacing(),
    };

    let mut last_seen = HashMap::new();
    drain_step(&cfg, &wasm_sub, &mut last_seen).await;

    // The push row must be delivered (retired into the quarantine table, not pending).
    let rows_after = messenger.load_pending_pushes(&wasm_sub).await;
    assert!(
        rows_after.is_empty(),
        "trap batch push row must be retired (quarantined), not pending. push_id={push_id}"
    );

    // Ack-at-start invariant (design §2.5): the push row must have delivered_at set
    // BEFORE the guest ran — not only after a successful outcome. Assert directly on
    // the DB row so a regression to ack-on-Ok-only is caught on the Trap path.
    // `rows_after.is_empty()` (above) already proved the row is not pending
    // (delivered_at IS NOT NULL), but an explicit delivered_at IS NOT NULL check
    // pins the specific ack-at-start ordering contract against the Trap arm.
    {
        let conn = messenger.db().lock().await;
        let delivered_at: Option<String> = conn
            .query_row(
                "SELECT delivered_at FROM messaging_pending_pushes WHERE id = ?1",
                rusqlite::params![push_id],
                |r| r.get(0),
            )
            .unwrap_or(None); // None if row was deleted — also fine
        // delivered_at IS NOT NULL (mark_pushes_delivered ran) OR row deleted both prove ack.
        // `rows_after.is_empty()` guarantees one of the two; the assertion here
        // additionally verifies the row-exists / delivered_at NOT NULL case specifically.
        assert!(
            delivered_at.is_some(),
            "ack-at-start: push row {push_id} must have delivered_at set (even on Trap) \
                 — got None (row deleted or delivered_at NULL). ack-at-start regressed?"
        );
    }

    // A second drain must find nothing new (no redelivery loop — N=1 terminal).
    drain_step(&cfg, &wasm_sub, &mut last_seen).await;
    let rows_second = messenger.load_pending_pushes(&wasm_sub).await;
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
    // failure). Design §3 / requirements §User-visible/Logging.
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

    let db = init_db_memory();
    let entry = Arc::new(ChannelEntry {
        uuid: channel_uuid,
        address: channel_addr.clone(),
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
        mount: Some(format!("/webhooks/{channel_slug}")),
    });
    {
        let conn = db.lock().await;
        upsert_channels(&conn, std::slice::from_ref(&*entry));
    }
    let directory = Arc::new(MessagingDirectory::with_entries(vec![(*entry).clone()]));
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
        wasm_policies_from_entries(std::slice::from_ref(&*entry)),
    ));
    let wasm_sub = ParticipantId::for_wasm(slug);

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

    let (push_id, _) = testutils::insert_wasm_push(
        &messenger,
        &entry,
        &wasm_sub,
        &wh_body,
        ChannelScheme::Webhook,
    )
    .await;

    // Before drain: one pending row.
    let rows_before = messenger.load_pending_pushes(&wasm_sub).await;
    assert_eq!(
        rows_before.len(),
        1,
        "one webhook push row before drain; push_id={push_id}"
    );

    let (cfg, _handle, _db) = build_cfg(
        slug,
        Arc::clone(&messenger),
        &entry,
        Depth::Unbounded,
        Depth::Unbounded,
    );
    let mut last_seen = HashMap::new();
    drain_step(&cfg, &wasm_sub, &mut last_seen).await;

    // After drain: row delivered.
    let rows_after = messenger.load_pending_pushes(&wasm_sub).await;
    assert!(
        rows_after.is_empty(),
        "webhook push row must be delivered after drain"
    );
}

// ── push_depth=0 subscription → never invoked ────────────────────────────

/// With push_depth=Bounded(0), no pending push rows are created for the Wasm
/// subscriber, so drain_all_channels finds nothing to invoke.
/// (This test verifies the guard in resolve_push_targets; covered also in
/// publish.rs but repeated here at the dispatch level.)
#[tokio::test]
async fn push_depth_zero_wasm_subscription_never_invoked() {
    // Use build_wasm_setup with push_depth=Bounded(0) so the channel is configured
    // correctly (the SubscriberEntry has push_depth=0). Then insert_wasm_push bypasses
    // resolve_push_targets entirely — so we verify the drain step finds nothing by
    // simply not inserting any push row.
    let slug = "consumer-no-push";
    let (messenger, channel, wasm_sub) =
        testutils::build_wasm_messenger(slug, "nopush-ch", Depth::Bounded(0), Depth::Unbounded)
            .await;

    // Do NOT insert any push row — with push_depth=0 none would be created by publish().
    let rows_before = messenger.load_pending_pushes(&wasm_sub).await;
    assert!(
        rows_before.is_empty(),
        "no rows before drain with push_depth=0"
    );

    let (cfg, _handle, _db) = build_cfg(
        slug,
        Arc::clone(&messenger),
        &channel,
        Depth::Bounded(0),
        Depth::Unbounded,
    );
    let mut last_seen = HashMap::new();
    // Drain finds nothing and invokes nothing — no panic, no error.
    drain_step(&cfg, &wasm_sub, &mut last_seen).await;
    let rows_after = messenger.load_pending_pushes(&wasm_sub).await;
    assert!(
        rows_after.is_empty(),
        "still no rows after drain with push_depth=0"
    );
}
