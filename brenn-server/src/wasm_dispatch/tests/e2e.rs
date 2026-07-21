//! End-to-end publish-path + chaining-wake family.

use super::*;

/// Design §4 "end-to-end demo (behavior 10)": webhook envelope on subscribed
/// channel → drain → second subscriber on the bound `brenn:` channel holds one
/// pending push row with `sender == "wasm:<slug>"`, `body == inner webhook body`,
/// `envelope_type == brenn`, `wake == immediate`.
#[tokio::test]
async fn end_to_end_demo_webhook_to_brenn_output() {
    let slug = "e2e-demo";
    let out_slug = "e2e-out-sub";
    let (messenger, in_entry, out_entry, wasm_sub, out_sub, cfg, _alert_handle, _db) =
        build_two_channel_setup(slug, out_slug).await;

    // Insert a webhook envelope into the input channel.
    let inner_body = "hello-from-webhook-e2e";
    let wh_env = WebhookEnvelope {
        headers: vec![],
        key_id: "k".into(),
        client_ip: "127.0.0.1".into(),
        received_at: Utc::now(),
        body: inner_body.into(),
        endpoint_slug: "e2e-in".into(),
    };
    let wh_body = serde_json::to_string(&wh_env).unwrap();
    testutils::insert_wasm_push(
        &messenger,
        &in_entry,
        &wasm_sub,
        &wh_body,
        ChannelScheme::Webhook,
    )
    .await;

    // Drain: demo component extracts inner body and publishes to "out" port.
    let mut last_seen = HashMap::new();
    drain_step(&cfg, &wasm_sub, &mut last_seen).await;

    // Verify the WASM push row is consumed.
    let in_rows_after = messenger.load_pending_pushes(&wasm_sub).await;
    assert!(
        in_rows_after.is_empty(),
        "WASM input push row must be delivered"
    );

    // Verify the output row on the brenn: channel.
    let out_rows = messenger.load_pending_pushes(&out_sub).await;
    assert_eq!(
        out_rows.len(),
        1,
        "exactly one output row for the second subscriber"
    );

    // Verify sender, body, envelope_type, and wake on the published message.
    {
        let conn = messenger.db().lock().await;
        let expected_sender = format!("wasm:{slug}");
        let (actual_sender, actual_body, actual_envelope_type, actual_wake): (
            String,
            String,
            String,
            String,
        ) = conn
            .query_row(
                "SELECT m.sender, m.body, m.envelope_type, m.urgency \
                 FROM messaging_messages m \
                 JOIN messaging_channels c ON c.uuid = m.channel_uuid \
                 WHERE c.address = ?1 \
                 ORDER BY m.publish_ts_ns DESC LIMIT 1",
                rusqlite::params![out_entry.address.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .expect("query output channel message");
        assert_eq!(
            actual_sender, expected_sender,
            "sender must be wasm:<slug>, got {actual_sender:?}"
        );
        assert_eq!(
            actual_body, inner_body,
            "body must be the inner webhook body, got {actual_body:?}"
        );
        assert_eq!(
            actual_envelope_type, "brenn",
            "envelope_type must be brenn, got {actual_envelope_type:?}"
        );
        assert_eq!(
            actual_wake, "normal",
            "urgency must be normal (port default_urgency = Normal in this test), got {actual_wake:?}"
        );
    }
}

/// Design §4 "all-or-nothing": activation with one publishable webhook envelope
/// + one `__trap__` sentinel → guest traps → nothing published on the output channel.
#[tokio::test]
async fn all_or_nothing_trap_after_publish_discards_output() {
    let slug = "e2e-all-or-nothing";
    let out_slug = "e2e-aon-out-sub";
    let (messenger, in_entry, out_entry, wasm_sub, out_sub, cfg, _alert_handle, _db) =
        build_two_channel_setup(slug, out_slug).await;

    // Insert two rows: first a webhook (would publish), then a sentinel (traps).
    let wh_env = WebhookEnvelope {
        headers: vec![],
        key_id: "k".into(),
        client_ip: "127.0.0.1".into(),
        received_at: Utc::now(),
        body: "some-payload".into(),
        endpoint_slug: "e2e-in".into(),
    };
    let wh_body = serde_json::to_string(&wh_env).unwrap();
    testutils::insert_wasm_push(
        &messenger,
        &in_entry,
        &wasm_sub,
        &wh_body,
        ChannelScheme::Webhook,
    )
    .await;
    // The sentinel causes trap. Because both rows arrive in the same activation
    // window, the trap discards the buffered publish from the first envelope.
    testutils::insert_wasm_push(
        &messenger,
        &in_entry,
        &wasm_sub,
        "__trap__", // MessageEnvelope.body == "__trap__" → guest traps
        ChannelScheme::Brenn,
    )
    .await;

    let mut last_seen = HashMap::new();
    drain_step(&cfg, &wasm_sub, &mut last_seen).await;

    // All input rows must be acked (delivered), but output channel must have no rows.
    let in_rows_after = messenger.load_pending_pushes(&wasm_sub).await;
    assert!(
        in_rows_after.is_empty(),
        "WASM input rows must be acked despite trap"
    );

    let out_rows = messenger.load_pending_pushes(&out_sub).await;
    assert!(
        out_rows.is_empty(),
        "no rows must be published to output channel when trap discards buffer; \
         out_entry={}",
        out_entry.address
    );
}

/// Ack-at-start on Err path: a guest returning Err leaves the push row delivered
/// (not pending). Design §2.5 / slop-2: the ack happens before the guest runs, so
/// an Err outcome leaves the row acked exactly like a Trap — no redelivery.
///
/// Setup: webhook channel with no output port bound → demo returns processing-failed
/// (Err) on the `publish("out", …)` call. The push row must be delivered (gone from
/// pending) even though the guest returned Err, confirming at-most-once semantics.
#[tokio::test]
async fn err_outcome_acks_push_row_at_activation_start() {
    let slug = "ack-err-path";
    let channel_slug = "ack-err-ch";
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

    // Insert a webhook envelope. The demo calls publish("out", …); with no output
    // port bound, this returns NotPermitted, causing the guest to return Err.
    let wh_env = WebhookEnvelope {
        headers: vec![],
        key_id: "k".into(),
        client_ip: "127.0.0.1".into(),
        received_at: Utc::now(),
        body: "some-payload".into(),
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

    // Build component with no output ports → publish("out", …) → NotPermitted → Err.
    let _db = tempfile::NamedTempFile::new().unwrap();
    let component = Arc::new(ProcessorComponent::load(ProcessorLoadSpec {
        component_path: std::path::Path::new(DEMO_WASM),
        slug,
        output_ports: std::collections::HashMap::new(), // no "out" bound → NotPermitted
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
    let (alert_dispatcher, alert_handle) = noop_alert_dispatcher();
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
                channel_uuid,
                channel_address: channel_addr,
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

    // Ack-at-start: the push row must be delivered even though the guest returned Err.
    let rows_after = messenger.load_pending_pushes(&wasm_sub).await;
    assert!(
        rows_after.is_empty(),
        "push row must be acked (delivered) even after Err outcome — \
         ack happens before guest runs (at-most-once); push_id={push_id}"
    );

    // A second drain must find nothing (no redelivery).
    drain_step(&cfg, &wasm_sub, &mut last_seen).await;
    let rows_second = messenger.load_pending_pushes(&wasm_sub).await;
    assert!(
        rows_second.is_empty(),
        "no redelivery after Err — at-most-once semantics"
    );

    drop(cfg);
    let _ = alert_handle.await;
}

/// Design §4 "call-order flush": N publishes in one activation appear with
/// strictly increasing `publish_ts_ns` in call order (guaranteed by the
/// monotonic `max(prev+1, now)` assignment, §2.3).
#[tokio::test]
async fn call_order_flush_monotonic_timestamps() {
    let slug = "e2e-order";
    let out_slug = "e2e-order-out-sub";
    let (messenger, in_entry, _out_entry, wasm_sub, _out_sub, cfg, _alert_handle, _db) =
        build_two_channel_setup(slug, out_slug).await;

    // Insert 3 webhook envelopes — each causes one publish in call order.
    let n = 3usize;
    for i in 0..n {
        let wh_env = WebhookEnvelope {
            headers: vec![],
            key_id: "k".into(),
            client_ip: "127.0.0.1".into(),
            received_at: Utc::now(),
            body: format!("payload-{i}"),
            endpoint_slug: "e2e-in".into(),
        };
        let wh_body = serde_json::to_string(&wh_env).unwrap();
        testutils::insert_wasm_push(
            &messenger,
            &in_entry,
            &wasm_sub,
            &wh_body,
            ChannelScheme::Webhook,
        )
        .await;
    }

    let mut last_seen = HashMap::new();
    drain_step(&cfg, &wasm_sub, &mut last_seen).await;

    // Query the 3 messages on the output channel ordered by publish_ts_ns ASC.
    let ts_list: Vec<i64> = {
        let conn = messenger.db().lock().await;
        let mut stmt = conn
            .prepare(
                "SELECT m.publish_ts_ns \
                 FROM messaging_messages m \
                 JOIN messaging_channels c ON c.uuid = m.channel_uuid \
                 WHERE c.address = 'brenn:e2e-out' \
                 ORDER BY m.publish_ts_ns ASC",
            )
            .expect("prepare ts query");
        stmt.query_map([], |row| row.get(0))
            .expect("query ts list")
            .map(|r| r.expect("read ts"))
            .collect()
    };

    assert_eq!(
        ts_list.len(),
        n,
        "expected {n} published messages, got {}",
        ts_list.len()
    );
    // Strictly increasing: each ts must be strictly greater than the previous.
    for window in ts_list.windows(2) {
        assert!(
            window[1] > window[0],
            "publish_ts_ns must be strictly increasing in call order: \
             ts[i]={} ts[i+1]={}",
            window[0],
            window[1]
        );
    }
}

/// Design §4 "chaining wake": a published row for a second WASM subscriber is
/// an `Immediate`-wake WASM push row. When the dispatcher processes it via
/// `dispatch_row`, `spawn_eager_wake` is called for the downstream subscriber.
///
/// Test structure: drain → verify the output push row is `Immediate`-wake →
/// call `dispatch_row` with a capturing router → assert `spawn_eager_wake` fires.
/// This exercises the full pipeline: `publish_from_wasm` lands the row, then
/// the dispatcher step that the background task would run.
#[tokio::test]
async fn chaining_wake_dispatch_row_fires_eager_wake_for_downstream_subscriber() {
    use brenn_lib::messaging::dispatcher;

    let slug = "e2e-chain";
    let out_slug = "e2e-chain-out";
    let (messenger, in_entry, _out_entry, wasm_sub, out_sub, cfg, _alert_handle, _db) =
        build_two_channel_setup(slug, out_slug).await;

    // Insert a webhook envelope.
    let wh_env = WebhookEnvelope {
        headers: vec![],
        key_id: "k".into(),
        client_ip: "127.0.0.1".into(),
        received_at: Utc::now(),
        body: "chained-payload".into(),
        endpoint_slug: "e2e-in".into(),
    };
    let wh_body = serde_json::to_string(&wh_env).unwrap();
    testutils::insert_wasm_push(
        &messenger,
        &in_entry,
        &wasm_sub,
        &wh_body,
        ChannelScheme::Webhook,
    )
    .await;

    // Drain: demo publishes to output channel → publish_from_wasm inserts push row
    // with wake=Immediate for the downstream subscriber.
    let mut last_seen = HashMap::new();
    drain_step(&cfg, &wasm_sub, &mut last_seen).await;

    // The output push row must be present and Immediate-wake.
    let out_pending = messenger.load_pending_pushes(&out_sub).await;
    assert_eq!(
        out_pending.len(),
        1,
        "exactly one pending push for downstream subscriber after drain"
    );
    let out_push_id = out_pending[0].0;

    // Load the push row for dispatch_row.
    let push_row = {
        let conn = messenger.db().lock().await;
        brenn_lib::messaging::db::load_pushes_by_ids(&conn, &[out_push_id])
            .into_iter()
            .next()
            .expect("output push row must exist")
    };
    assert!(
        push_row.eager_wake,
        "output push row must have eager_wake=true for eager-wake routing"
    );

    // Build a capturing wake router and call dispatch_row directly —
    // simulating the one step the background dispatcher task would run.
    use std::sync::Mutex;
    struct CapturingWakeRouter {
        woken: Mutex<Vec<String>>,
    }
    #[async_trait::async_trait]
    impl WakeRouter for CapturingWakeRouter {
        async fn deliver(
            &self,
            _: &brenn_lib::messaging::SubscriberEntryKind,
            _: &ParticipantId,
            _: &brenn_lib::messaging::MessageEnvelope,
            _push_id: i64,
            _seq: i64,
        ) -> Result<bool, String> {
            Ok(false)
        }
        async fn deliver_ingress(
            &self,
            _: &brenn_lib::messaging::SubscriberEntryKind,
            _: &ParticipantId,
            _: &brenn_lib::messaging::ingress::Event,
        ) -> Result<bool, String> {
            Ok(false)
        }
        fn spawn_eager_wake(
            &self,
            _: &brenn_lib::messaging::SubscriberEntryKind,
            subscriber: &ParticipantId,
        ) {
            self.woken
                .lock()
                .unwrap()
                .push(subscriber.as_str().to_string());
        }
        fn delivery_shape(
            &self,
            key: &brenn_lib::messaging::SubscriberEntryKind,
        ) -> brenn_lib::messaging::DeliveryShape {
            brenn_lib::messaging::default_delivery_shape(key)
        }
        fn alarm(&self, _: &str, _: &ParticipantId) {}
    }

    let capturing_router = CapturingWakeRouter {
        woken: Mutex::new(Vec::new()),
    };
    dispatcher::dispatch_row(&capturing_router, &push_row, false, false).await;

    let woken = capturing_router.woken.lock().unwrap();
    assert!(
        woken.iter().any(|s| s == out_sub.as_str()),
        "dispatch_row must call spawn_eager_wake for the downstream WASM subscriber {:?}; \
         got woken={woken:?}",
        out_sub.as_str()
    );
}
