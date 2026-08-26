//! End-to-end publish-path + chaining-wake family.

use brenn_wasm_dispatch::tests::*;
use brenn_wasm_dispatch::{WasmConsumerConfig, apply_deferred_ops, drain_step};
use std::collections::HashMap;

use brenn_lib::access::test_fixtures::delivery_policy_for_addresses;
use brenn_lib::messaging::{Urgency, WebhookEnvelope};
use brenn_messaging::WasmPublish;
use brenn_wasm::{ProcessorComponent, ProcessorDeferredOp};

use chrono::Utc;

/// End-to-end demo: webhook envelope on subscribed
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
    testutils::insert_bus_message(&messenger, &in_entry, &wh_body, ChannelScheme::Webhook).await;

    // Drain: demo component extracts inner body and publishes to "out" port.
    drain_step(&cfg, &wasm_sub).await;

    // Verify the WASM push row is consumed.
    let in_rows_after = brenn_messaging::testutils::owed_everywhere(&messenger, &wasm_sub).await;
    assert!(
        in_rows_after.is_empty(),
        "WASM input push row must be delivered"
    );

    // Verify the output row on the brenn: channel.
    let out_rows = brenn_messaging::testutils::owed_everywhere(&messenger, &out_sub).await;
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

/// All-or-nothing: activation with one publishable webhook envelope
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
    testutils::insert_bus_message(&messenger, &in_entry, &wh_body, ChannelScheme::Webhook).await;
    // The sentinel causes trap. Because both rows arrive in the same activation
    // window, the trap discards the buffered publish from the first envelope.
    testutils::insert_bus_message(
        &messenger,
        &in_entry,
        "__trap__", // MessageEnvelope.body == "__trap__" → guest traps
        ChannelScheme::Brenn,
    )
    .await;

    drain_step(&cfg, &wasm_sub).await;

    // All input rows must be acked (delivered), but output channel must have no rows.
    let in_rows_after = brenn_messaging::testutils::owed_everywhere(&messenger, &wasm_sub).await;
    assert!(
        in_rows_after.is_empty(),
        "WASM input rows must be acked despite trap"
    );

    let out_rows = brenn_messaging::testutils::owed_everywhere(&messenger, &out_sub).await;
    assert!(
        out_rows.is_empty(),
        "no rows must be published to output channel when trap discards buffer; \
         out_entry={}",
        out_entry.address
    );
}

/// Ack-at-start on Err path: a guest returning Err leaves the push row delivered
/// (not pending). The ack happens before the guest runs, so
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
    brenn_wasm_dispatch::tests::attach_input_ports(
        &messenger,
        slug,
        &wasm_sub,
        &[(&entry, Depth::Unbounded)],
    )
    .await;

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
    let _ =
        testutils::insert_bus_message(&messenger, &entry, &wh_body, ChannelScheme::Webhook).await;

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
        outputs: vec![],
        activation_pacing: unthrottled_pacing(),
    };

    drain_step(&cfg, &wasm_sub).await;

    // Ack-at-start: the position moved past the batch even though the guest
    // returned Err — the advance runs before the guest (at-most-once).
    let rows_after = brenn_messaging::testutils::owed_everywhere(&messenger, &wasm_sub).await;
    assert!(
        rows_after.is_empty(),
        "the batch must be advanced past even after an Err outcome"
    );

    // A second drain must find nothing (no redelivery).
    drain_step(&cfg, &wasm_sub).await;
    let rows_second = brenn_messaging::testutils::owed_everywhere(&messenger, &wasm_sub).await;
    assert!(
        rows_second.is_empty(),
        "no redelivery after Err — at-most-once semantics"
    );

    drop(cfg);
    let _ = alert_handle.await;
}

/// Call-order flush: N publishes in one activation appear with strictly
/// increasing `publish_ts_ns` in call order, which the monotonic
/// `max(prev + 1, now)` assignment guarantees.
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
        testutils::insert_bus_message(&messenger, &in_entry, &wh_body, ChannelScheme::Webhook)
            .await;
    }

    drain_step(&cfg, &wasm_sub).await;

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

/// Chaining wake: a row one component publishes for a second WASM subscriber is
/// an eager-wake push row, and the wake walk calls `spawn_eager_wake` for that
/// downstream subscriber.
///
/// Test structure: drain → verify the output push row is eager-wake → run the
/// wake walk against a capturing router → assert `spawn_eager_wake` fires.
#[tokio::test]
async fn chaining_wake_store_walk_fires_eager_wake_for_downstream_subscriber() {
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
    testutils::insert_bus_message(&messenger, &in_entry, &wh_body, ChannelScheme::Webhook).await;

    // Drain: demo publishes to output channel → publish_from_wasm inserts push row
    // with wake=Immediate for the downstream subscriber.
    drain_step(&cfg, &wasm_sub).await;

    // The downstream subscriber's position now trails the published output.
    let out_pending = brenn_messaging::testutils::owed_everywhere(&messenger, &out_sub).await;
    assert_eq!(
        out_pending.len(),
        1,
        "exactly one message owed to the downstream subscriber after drain"
    );

    // Build a capturing wake router and run the wake pass — the one step the
    // background dispatcher task would run for a trailing position.
    use std::sync::Mutex;
    struct CapturingWakeRouter {
        woken: Mutex<Vec<String>>,
    }
    #[async_trait::async_trait]
    impl WakeRouter for CapturingWakeRouter {
        async fn deliver(
            &self,
            _: &brenn_lib::messaging::SubscriberEntryKind,
            _envelope: &std::sync::Arc<brenn_lib::messaging::MessageEnvelope>,
            _retained_seq: i64,
        ) -> Result<bool, String> {
            Ok(false)
        }
        async fn deliver_ingress(
            &self,
            _: &brenn_lib::messaging::SubscriberEntryKind,
            _: &ParticipantId,
            _: &brenn_messaging_store::ingress::Event,
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
        ) -> brenn_messaging::DeliveryShape {
            brenn_messaging::default_delivery_shape(key)
        }
        fn alarm(&self, _: &str, _: &ParticipantId, _: u64) {}
    }

    // Drive the wake through a Messenger whose router captures wakes (the
    // fixture's router is a noop).
    let capturing_router = Arc::new(CapturingWakeRouter {
        woken: Mutex::new(Vec::new()),
    });
    let walker = brenn_messaging::Messenger::new(
        messenger.db().clone(),
        Arc::clone(messenger.directory()),
        Arc::from("test"),
        Arc::new(IndexMap::new()),
        Arc::clone(&capturing_router) as Arc<dyn WakeRouter>,
        brenn_lib::messaging::config::MessagingGlobalConfig::default(),
    )
    // The walk's ACL gate reads the registration, so the stand-in messenger
    // needs the same one the fixture built or the wake it is here to observe is
    // denied before it is decided.
    .with_subscriber_registrations(brenn_messaging::testutils::wasm_registrations(
        wasm_policies_from_entries(
            &messenger
                .directory()
                .list()
                .iter()
                .map(|entry| ChannelEntry::clone(entry))
                .collect::<Vec<_>>(),
        ),
    ));
    walker.wake_owed_subscribers(Utc::now()).await;

    let woken = capturing_router.woken.lock().unwrap();
    assert!(
        woken.iter().any(|s| s == out_sub.as_str()),
        "the store walk must call spawn_eager_wake for the downstream WASM subscriber {:?}; \
         got woken={woken:?}",
        out_sub.as_str()
    );
}

/// `publish_deferred` end-to-end: the guest computes an absolute
/// `deliver_after` from the host-stamped `now`, and the message parks rather
/// than committing immediately. A parked row with a future release time proves
/// the host stamp was live (the guest traps on a missing `now`).
#[tokio::test]
async fn guest_publish_deferred_parks_with_a_host_stamped_now() {
    let slug = "e2e-defer";
    let out_slug = "e2e-defer-out-sub";
    let (messenger, in_entry, _out_entry, wasm_sub, out_sub, cfg, _alert_handle, _db) =
        build_two_channel_setup(slug, out_slug).await;

    let wh_env = WebhookEnvelope {
        headers: vec![],
        key_id: "k".into(),
        client_ip: "127.0.0.1".into(),
        received_at: Utc::now(),
        body: "__defer__".into(),
        endpoint_slug: "e2e-in".into(),
    };
    let wh_body = serde_json::to_string(&wh_env).unwrap();
    testutils::insert_bus_message(&messenger, &in_entry, &wh_body, ChannelScheme::Webhook).await;

    let before = Utc::now();
    drain_step(&cfg, &wasm_sub).await;

    let in_rows_after = brenn_messaging::testutils::owed_everywhere(&messenger, &wasm_sub).await;
    assert!(in_rows_after.is_empty(), "WASM input row must be delivered");

    {
        let conn = messenger.db().lock().await;
        let (body, deliver_after): (String, Option<String>) = conn
            .query_row(
                "SELECT m.body, m.deliver_after \
                 FROM messaging_messages m \
                 JOIN messaging_channels c ON c.uuid = m.channel_uuid \
                 WHERE c.address = 'brenn:e2e-out' \
                 ORDER BY m.publish_ts_ns DESC LIMIT 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("the deferred self-publish produced a parked message row");
        assert_eq!(body, "deferred-payload");
        let da = deliver_after.expect("a parked message row carries deliver_after");
        let parsed = chrono::DateTime::parse_from_rfc3339(&da)
            .map(|d| d.with_timezone(&Utc))
            .unwrap_or_else(|_| panic!("deliver_after {da:?} is not a parseable timestamp"));
        assert!(
            parsed > before,
            "deliver_after {parsed} must be after drain start {before} — proves the guest \
             computed it from a real host-stamped now",
        );
    }

    let out_rows = brenn_messaging::testutils::owed_everywhere(&messenger, &out_sub).await;
    assert!(
        out_rows.is_empty(),
        "a parked deferred publish delivers nothing before its release time",
    );
}

/// The output-port deferred view, end-to-end: after the guest schedules a
/// deferred self-publish, a later activation carries a deferred-window for that
/// output port, and the guest reads its own parked message back — payload,
/// `deliver_after`, and index-ordered — proving `drain_step` builds the view
/// from the store and lowers it across the WIT boundary.
#[tokio::test]
async fn output_port_deferred_view_reflects_the_guests_own_parked_message() {
    let slug = "e2e-view";
    let out_slug = "e2e-view-out-sub";
    let (messenger, in_entry, _out_entry, wasm_sub, _out_sub, cfg, _alert_handle, _db) =
        build_two_channel_setup(slug, out_slug).await;

    // First activation: __defer__ parks a message on the "out" channel.
    let defer_env = WebhookEnvelope {
        headers: vec![],
        key_id: "k".into(),
        client_ip: "127.0.0.1".into(),
        received_at: Utc::now(),
        body: "__defer__".into(),
        endpoint_slug: "e2e-in".into(),
    };
    testutils::insert_bus_message(
        &messenger,
        &in_entry,
        &serde_json::to_string(&defer_env).unwrap(),
        ChannelScheme::Webhook,
    )
    .await;
    drain_step(&cfg, &wasm_sub).await;

    // Read the parked message's deliver_after so we can assert the guest saw it.
    let parked_da: String = {
        let conn = messenger.db().lock().await;
        conn.query_row(
            "SELECT m.deliver_after FROM messaging_messages m \
             JOIN messaging_channels c ON c.uuid = m.channel_uuid \
             WHERE c.address = 'brenn:e2e-out' AND m.deliver_after IS NOT NULL \
             ORDER BY m.publish_ts_ns DESC LIMIT 1",
            [],
            |row| row.get(0),
        )
        .expect("the deferred self-publish produced a parked row")
    };
    let parked_ms = chrono::DateTime::parse_from_rfc3339(&parked_da)
        .unwrap()
        .with_timezone(&Utc)
        .timestamp_millis();

    // Second activation: __viewcount__ makes the guest read its deferred view
    // for port "out" and publish a summary of it immediately.
    let view_env = WebhookEnvelope {
        headers: vec![],
        key_id: "k".into(),
        client_ip: "127.0.0.1".into(),
        received_at: Utc::now(),
        body: "__viewcount__".into(),
        endpoint_slug: "e2e-in".into(),
    };
    testutils::insert_bus_message(
        &messenger,
        &in_entry,
        &serde_json::to_string(&view_env).unwrap(),
        ChannelScheme::Webhook,
    )
    .await;
    drain_step(&cfg, &wasm_sub).await;

    // The immediate summary the guest published reports its own parked message.
    let summary: String = {
        let conn = messenger.db().lock().await;
        conn.query_row(
            "SELECT m.body FROM messaging_messages m \
             JOIN messaging_channels c ON c.uuid = m.channel_uuid \
             WHERE c.address = 'brenn:e2e-out' AND m.deliver_after IS NULL \
             ORDER BY m.publish_ts_ns DESC LIMIT 1",
            [],
            |row| row.get(0),
        )
        .expect("the __viewcount__ activation published an immediate summary")
    };
    assert_eq!(
        summary,
        format!("view=1 first=deferred-payload at={parked_ms}"),
        "the guest's deferred view carries its own parked payload and release time"
    );
}

/// The output-port `defer-cancel`, end-to-end: after the guest schedules a
/// deferred self-publish, a later `__cancel__` activation cancels it by its view
/// index. The buffered cancel is applied at flush against the identity captured in
/// the activation snapshot, so the parked row is gone afterward and never releases.
#[tokio::test]
async fn output_port_defer_cancel_removes_the_guests_own_parked_message() {
    let slug = "e2e-cancel";
    let out_slug = "e2e-cancel-out-sub";
    let (messenger, in_entry, _out_entry, wasm_sub, _out_sub, cfg, _alert_handle, _db) =
        build_two_channel_setup(slug, out_slug).await;

    // First activation: __defer__ parks a message on the "out" channel.
    let defer_env = WebhookEnvelope {
        headers: vec![],
        key_id: "k".into(),
        client_ip: "127.0.0.1".into(),
        received_at: Utc::now(),
        body: "__defer__".into(),
        endpoint_slug: "e2e-in".into(),
    };
    testutils::insert_bus_message(
        &messenger,
        &in_entry,
        &serde_json::to_string(&defer_env).unwrap(),
        ChannelScheme::Webhook,
    )
    .await;
    drain_step(&cfg, &wasm_sub).await;

    let parked_before: i64 = {
        let conn = messenger.db().lock().await;
        conn.query_row(
            "SELECT COUNT(*) FROM messaging_messages m \
             JOIN messaging_channels c ON c.uuid = m.channel_uuid \
             WHERE c.address = 'brenn:e2e-out' AND m.deliver_after IS NOT NULL",
            [],
            |row| row.get(0),
        )
        .unwrap()
    };
    assert_eq!(parked_before, 1, "the deferred self-publish parked one row");

    // Second activation: __cancel__ cancels the parked message by its view index.
    let cancel_env = WebhookEnvelope {
        headers: vec![],
        key_id: "k".into(),
        client_ip: "127.0.0.1".into(),
        received_at: Utc::now(),
        body: "__cancel__".into(),
        endpoint_slug: "e2e-in".into(),
    };
    testutils::insert_bus_message(
        &messenger,
        &in_entry,
        &serde_json::to_string(&cancel_env).unwrap(),
        ChannelScheme::Webhook,
    )
    .await;
    drain_step(&cfg, &wasm_sub).await;

    let parked_after: i64 = {
        let conn = messenger.db().lock().await;
        conn.query_row(
            "SELECT COUNT(*) FROM messaging_messages m \
             JOIN messaging_channels c ON c.uuid = m.channel_uuid \
             WHERE c.address = 'brenn:e2e-out' AND m.deliver_after IS NOT NULL",
            [],
            |row| row.get(0),
        )
        .unwrap()
    };
    assert_eq!(
        parked_after, 0,
        "the buffered defer-cancel erased the parked message at flush"
    );
}

/// The output-port `defer-edit`, end-to-end: after the guest schedules a
/// deferred self-publish (release at `now + 60s`), a later `__reschedule__`
/// activation edits it by its view index to `now + 1h`. The parked row
/// survives (not cancelled) with its release pushed further out.
#[tokio::test]
async fn output_port_defer_edit_reschedules_the_guests_own_parked_message() {
    let slug = "e2e-edit";
    let out_slug = "e2e-edit-out-sub";
    let (messenger, in_entry, _out_entry, wasm_sub, _out_sub, cfg, _alert_handle, _db) =
        build_two_channel_setup(slug, out_slug).await;

    // First activation: __defer__ parks a message on the "out" channel at now+60s.
    let defer_env = WebhookEnvelope {
        headers: vec![],
        key_id: "k".into(),
        client_ip: "127.0.0.1".into(),
        received_at: Utc::now(),
        body: "__defer__".into(),
        endpoint_slug: "e2e-in".into(),
    };
    testutils::insert_bus_message(
        &messenger,
        &in_entry,
        &serde_json::to_string(&defer_env).unwrap(),
        ChannelScheme::Webhook,
    )
    .await;
    drain_step(&cfg, &wasm_sub).await;

    let before_ms: i64 = {
        let conn = messenger.db().lock().await;
        conn.query_row(
            "SELECT m.deliver_after FROM messaging_messages m \
             JOIN messaging_channels c ON c.uuid = m.channel_uuid \
             WHERE c.address = 'brenn:e2e-out' AND m.deliver_after IS NOT NULL \
             ORDER BY m.publish_ts_ns DESC LIMIT 1",
            [],
            |row| {
                let da: String = row.get(0)?;
                Ok(chrono::DateTime::parse_from_rfc3339(&da)
                    .unwrap()
                    .with_timezone(&Utc)
                    .timestamp_millis())
            },
        )
        .expect("the deferred self-publish produced a parked row")
    };

    // Second activation: __reschedule__ edits the parked message's release to
    // now + 1h by its view index.
    let edit_env = WebhookEnvelope {
        headers: vec![],
        key_id: "k".into(),
        client_ip: "127.0.0.1".into(),
        received_at: Utc::now(),
        body: "__reschedule__".into(),
        endpoint_slug: "e2e-in".into(),
    };
    testutils::insert_bus_message(
        &messenger,
        &in_entry,
        &serde_json::to_string(&edit_env).unwrap(),
        ChannelScheme::Webhook,
    )
    .await;
    drain_step(&cfg, &wasm_sub).await;

    let (after_ms, still_parked): (i64, i64) = {
        let conn = messenger.db().lock().await;
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM messaging_messages m \
                 JOIN messaging_channels c ON c.uuid = m.channel_uuid \
                 WHERE c.address = 'brenn:e2e-out' AND m.deliver_after IS NOT NULL",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let da: String = conn
            .query_row(
                "SELECT m.deliver_after FROM messaging_messages m \
                 JOIN messaging_channels c ON c.uuid = m.channel_uuid \
                 WHERE c.address = 'brenn:e2e-out' AND m.deliver_after IS NOT NULL \
                 ORDER BY m.publish_ts_ns DESC LIMIT 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let ms = chrono::DateTime::parse_from_rfc3339(&da)
            .unwrap()
            .with_timezone(&Utc)
            .timestamp_millis();
        (ms, count)
    };

    assert_eq!(
        still_parked, 1,
        "the edit rescheduled the one parked message, it did not cancel or re-park"
    );
    assert!(
        after_ms > before_ms,
        "the defer-edit pushed the release further out: {after_ms} !> {before_ms}"
    );
}

/// The store reports a cross-sender id instead of panicking, so the judgement
/// belongs to each caller. This host's judgement: every id it passes came from a
/// snapshot it built from that component's own sender-scoped view, so a
/// cross-sender hit means the snapshot and the store disagree — a broken
/// invariant, not a runtime condition.
///
/// Driven directly rather than through a guest, because the WIT gives a guest no
/// way to name a message outside its own window; only a host bug can.
#[tokio::test]
#[should_panic(expected = "parked by another sender")]
async fn a_deferred_op_naming_another_senders_parked_message_panics() {
    let slug = "e2e-foreign";
    let (messenger, _in_entry, _out_entry, _wasm_sub, _out_sub, cfg, _alert_handle, _db) =
        build_two_channel_setup(slug, "e2e-foreign-out-sub").await;

    let other = "e2e-foreign-other";
    messenger
        .publish_from_wasm(
            other,
            &[WasmPublish {
                channel_address: "brenn:e2e-out",
                body: "not-yours",
                urgency: Urgency::Normal,
                reply_to: None,
                deliver_after: Some(Utc::now() + chrono::Duration::seconds(60)),
            }],
        )
        .await;
    let now = Utc::now();
    let foreign = messenger
        .deferred_view_for_sender(
            "brenn:e2e-out",
            ParticipantId::for_wasm(other).as_str(),
            now,
        )
        .await;
    let uuid = foreign
        .first()
        .expect("the other component's publish parked one message")
        .message_uuid();

    apply_deferred_ops(
        &cfg,
        ParticipantId::for_wasm(slug).as_str(),
        &HashMap::from([("out".to_string(), vec![uuid])]),
        &[ProcessorDeferredOp::Cancel {
            port: "out".to_string(),
            index: 0,
        }],
        now,
    )
    .await;
}

/// A `wasm:<slug>` consumer with one ring-backed (`ephemeral:`) input, its
/// cursor attached at the ring head.
async fn build_ring_backed_consumer(
    slug: &str,
    channel_name: &str,
) -> (
    Arc<brenn_messaging::Messenger>,
    ChannelEntry,
    Arc<brenn_messaging_store::store::RingStore>,
    ParticipantId,
    WasmConsumerConfig,
    tokio::task::JoinHandle<()>,
) {
    use brenn_messaging_store::store::RingStores;

    let entry = testutils::ephemeral_channel_entry(channel_name, 8);
    let uuid = entry.uuid;

    let db = init_db_memory_lib_slice();
    let wasm_sub = ParticipantId::for_wasm(slug);
    let directory = Arc::new(MessagingDirectory::with_entries(vec![entry.clone()]));
    let ring_stores = Arc::new(RingStores::build(std::slice::from_ref(&entry)));
    let messenger = brenn_messaging::Messenger::new(
        db,
        directory,
        Arc::from("test"),
        Arc::new(IndexMap::new()),
        Arc::new(NoopWakeRouter) as Arc<dyn WakeRouter>,
        MessagingGlobalConfig::default(),
    )
    .with_ring_stores(Arc::clone(&ring_stores))
    .with_subscriber_registrations(brenn_messaging::testutils::wasm_registrations(
        std::collections::HashMap::from([(
            slug.to_string(),
            delivery_policy_for_addresses([entry.address.as_str()]),
        )]),
    ));

    messenger.attach_ring_subscriber(&uuid, &wasm_sub, u64::MAX);
    let ring = ring_stores.get(&uuid).expect("registered ring store");

    let (alert_dispatcher, alert_handle) = noop_alert_dispatcher();
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
    let cfg = WasmConsumerConfig {
        slug: slug.to_string(),
        component,
        notify: Arc::new(Notify::new()),
        messenger: Arc::clone(&messenger),
        alert_dispatcher,
        inputs: vec![WasmInputPort {
            port: "in".to_string(),
            sub: ResolvedSubscription {
                channel_uuid: uuid,
                channel_address: entry.address.clone(),
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

    (messenger, entry, ring, wasm_sub, cfg, alert_handle)
}

/// Append `body` to a ring-backed channel as an ordinary publisher would.
async fn append_ring_message(
    messenger: &brenn_messaging::Messenger,
    entry: &ChannelEntry,
    body: String,
) {
    use brenn_lib::messaging::Urgency;
    use brenn_messaging_store::store::NewMessage;

    messenger
        .store_for(entry)
        .append(NewMessage {
            source: "test".to_string(),
            sender: "publisher".to_string(),
            body,
            urgency: Urgency::Normal,
            envelope_type: ChannelScheme::Ephemeral,
            reply_to: None,
            delivery_deadline: None,
            impetus: None,
            publish_ts_ns: Utc::now().timestamp_nanos_opt().unwrap(),
        })
        .await;
}

/// A consumer whose two input ports span both store classes: one durable
/// (`brenn:`) and one ring-backed (`ephemeral:`). Returns
/// `(messenger, durable_entry, ring_entry, ring_store, wasm_sub, cfg, alert_handle)`.
#[allow(clippy::type_complexity)]
async fn build_mixed_class_consumer(
    slug: &str,
) -> (
    Arc<brenn_messaging::Messenger>,
    Arc<ChannelEntry>,
    ChannelEntry,
    Arc<brenn_messaging_store::store::RingStore>,
    ParticipantId,
    WasmConsumerConfig,
    tokio::task::JoinHandle<()>,
) {
    use brenn_messaging_store::store::RingStores;

    let durable = testutils::wasm_channel_entry(
        slug,
        &format!("{slug}-durable"),
        Depth::Unbounded,
        Depth::Unbounded,
    );
    let ring_entry = testutils::ephemeral_channel_entry(&format!("{slug}-ring"), 8);

    let db = init_db_memory_lib_slice();
    let wasm_sub = ParticipantId::for_wasm(slug);
    {
        let conn = db.lock().await;
        upsert_channels(&conn, std::slice::from_ref(durable.as_ref()));
    }
    let directory = Arc::new(MessagingDirectory::with_entries(vec![
        (*durable).clone(),
        ring_entry.clone(),
    ]));
    let ring_stores = Arc::new(RingStores::build(std::slice::from_ref(&ring_entry)));
    let messenger = brenn_messaging::Messenger::new(
        db,
        directory,
        Arc::from("test"),
        Arc::new(IndexMap::new()),
        Arc::new(NoopWakeRouter) as Arc<dyn WakeRouter>,
        MessagingGlobalConfig::default(),
    )
    .with_ring_stores(Arc::clone(&ring_stores))
    .with_subscriber_registrations(brenn_messaging::testutils::wasm_registrations(
        std::collections::HashMap::from([(
            slug.to_string(),
            delivery_policy_for_addresses([durable.address.as_str(), ring_entry.address.as_str()]),
        )]),
    ));

    messenger.attach_ring_subscriber(&ring_entry.uuid, &wasm_sub, u64::MAX);
    brenn_wasm_dispatch::tests::attach_input_ports(
        &messenger,
        slug,
        &wasm_sub,
        &[(durable.as_ref(), Depth::Unbounded)],
    )
    .await;
    let ring = ring_stores
        .get(&ring_entry.uuid)
        .expect("registered ring store");

    let (alert_dispatcher, alert_handle) = noop_alert_dispatcher();
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
    let port = |name: &str, entry_uuid, address: String| WasmInputPort {
        port: name.to_string(),
        sub: ResolvedSubscription {
            channel_uuid: entry_uuid,
            channel_address: address,
            push_depth: Depth::Unbounded,
            retain_depth: Depth::Unbounded,
            noise: NoiseLevel::Silent,
            wake_min: WakeMin::Normal,
        },
        amplification_mt: 1000,
    };
    let cfg = WasmConsumerConfig {
        slug: slug.to_string(),
        component,
        notify: Arc::new(Notify::new()),
        messenger: Arc::clone(&messenger),
        alert_dispatcher,
        inputs: vec![
            port("in0", durable.uuid, durable.address.clone()),
            port("in1", ring_entry.uuid, ring_entry.address.clone()),
        ],
        outputs: vec![],
        activation_pacing: unthrottled_pacing(),
    };

    (
        messenger,
        durable,
        ring_entry,
        ring,
        wasm_sub,
        cfg,
        alert_handle,
    )
}

/// One activation spanning both store classes: the settle set carries a
/// `messaging_pending_pushes` rowid for the durable port and nothing for the
/// cursor-tracked one, and each half must be settled by the store that issued
/// it. Routing a claim id into the ring would jump its cursor to a rowid-sized
/// position — skipping every retained message and charging the span as drops —
/// while the durable claim stayed pending and redelivered forever.
#[tokio::test]
async fn a_mixed_class_activation_settles_each_port_in_its_own_domain() {
    let (messenger, durable, ring_entry, ring, wasm_sub, cfg, _alert_handle) =
        build_mixed_class_consumer("e2e-mixed").await;

    let _ =
        testutils::insert_bus_message(&messenger, &durable, "durable-body", ChannelScheme::Brenn)
            .await;
    append_ring_message(&messenger, &ring_entry, "ring-body".to_string()).await;
    assert!(
        ring.has_deliverable(&wasm_sub),
        "the consumer is owed the ring message before draining"
    );

    drain_step(&cfg, &wasm_sub).await;

    assert!(
        brenn_messaging::testutils::owed_everywhere(&messenger, &wasm_sub)
            .await
            .is_empty(),
        "the durable port's position advanced past its one message"
    );

    assert!(
        !ring.has_deliverable(&wasm_sub),
        "the ring cursor advanced past its one message"
    );
    // The next message lands exactly on the cursor: a jump to a foreign id
    // domain would have left it far above the ring, and this window would serve
    // nothing while reporting the skipped span as drops.
    append_ring_message(&messenger, &ring_entry, "ring-next".to_string()).await;
    let window = ring
        .window(&wasm_sub, 8, 0)
        .expect("the case attached this subscriber");
    assert_eq!(
        window.entries.len() - window.new_from,
        1,
        "the follow-up message is the only new one"
    );
    let (through, seen_floor) = window.advance_span().expect("a served window advances");
    assert_eq!(
        ring.advance(&wasm_sub, through, seen_floor)
            .expect("the case attached this subscriber")
            .dropped,
        0,
        "a cursor advanced by exactly one loses nothing in between"
    );
}

/// Ring-backed (`ephemeral:`) input through `drain_step` end to end.
/// Ephemeral rows carry no push id (the snapshot-time cursor take is their
/// ack), so this is also the regression guard for the triggering-rows
/// invariant: an ephemeral-only activation must not trip the debug assert.
#[tokio::test]
async fn drain_step_consumes_a_ring_backed_ephemeral_input() {
    let (messenger, entry, ring, wasm_sub, cfg, _alert_handle) =
        build_ring_backed_consumer("e2e-ring", "e2e-ring-in").await;

    append_ring_message(
        &messenger,
        &entry,
        serde_json::json!({ "channel": "ephemeral:e2e-ring-in" }).to_string(),
    )
    .await;
    assert!(
        ring.has_deliverable(&wasm_sub),
        "the consumer is owed the appended ring message before draining"
    );

    // The drain must consume the ring-triggered activation without panicking
    // (an ephemeral-only trigger delivers rows with no push ids).
    drain_step(&cfg, &wasm_sub).await;

    assert!(
        !ring.has_deliverable(&wasm_sub),
        "the drain's snapshot-time take advanced the cursor past the message"
    );
}

/// A trapping guest on a ring-backed input quarantines the batch. The port's
/// rows carry no claim ids — the cursor take was their ack — so the quarantine
/// record names none and retires none, and the failure path must not read the
/// ring's own positions as claim rowids.
#[tokio::test]
async fn ring_backed_trap_quarantines_without_claim_ids() {
    let (messenger, entry, ring, wasm_sub, cfg, _alert_handle) =
        build_ring_backed_consumer("e2e-ring-trap", "e2e-ring-trap-in").await;

    append_ring_message(&messenger, &entry, "__trap__".to_string()).await;

    drain_step(&cfg, &wasm_sub).await;

    assert!(
        !ring.has_deliverable(&wasm_sub),
        "the trapped batch was acked by the snapshot-time take, not redelivered"
    );

    let conn = messenger.db().lock().await;
    let (rows, batch_seq_span): (i64, String) = conn
        .query_row(
            "SELECT COUNT(*), COALESCE(MIN(batch_seq_span), '') \
             FROM messaging_wasm_consume_failures WHERE channel = ?1",
            rusqlite::params![entry.address.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("query wasm_consume_failures");
    assert_eq!(
        rows, 1,
        "the trapped ring activation wrote one quarantine row"
    );
    assert_eq!(
        batch_seq_span, "1-1",
        "the quarantine row names the one retention seq the batch spanned"
    );
}

/// The trigger channel every auto-channel case fires. The demo component
/// requires a webhook-typed envelope to produce output, so this channel carries
/// that shape. The uuid is drawn once so a restart case can reboot the same
/// config.
fn trigger_channel() -> brenn_lib::messaging::config::ChannelConfigRaw {
    brenn_lib::messaging::config::ChannelConfigRaw {
        send_rate: None,
        uuid: Some(uuid::Uuid::new_v4().to_string()),
        address: Some("brenn:e2e-trigger".to_string()),
        address_prefix: None,
        description: None,
        push_depth: Some(brenn_lib::messaging::config::Depth::Unbounded),
        retain_depth: Some(brenn_lib::messaging::config::Depth::Unbounded),
        standing_retain_depth: Some(brenn_lib::messaging::config::Depth::Unbounded),
        noise: None,
        sink: None,
        wake_min: None,
    }
}

/// A `[[wasm_consumer]]` running the demo guest and woken by the trigger
/// channel. Callers add the output side under test — a free output port, an
/// io_port — via struct update.
fn demo_consumer_raw(slug: &str) -> brenn_lib::messaging::config::WasmConsumerConfigRaw {
    use brenn_lib::access::raw::ChannelMatcherRaw;
    use brenn_lib::messaging::ComponentGrant;
    use brenn_lib::messaging::config::{WasmConsumerConfigRaw, WasmConsumerSubscriptionRaw};

    WasmConsumerConfigRaw {
        slug: slug.to_string(),
        component_path: std::path::PathBuf::from(DEMO_WASM),
        grants: vec![ComponentGrant::Ports],
        subscriptions: vec![WasmConsumerSubscriptionRaw {
            channel: Some("brenn:e2e-trigger".to_string()),
            port: "in".to_string(),
            push_depth: None,
            retain_depth: None,
            noise: None,
            wake_min: None,
            amplification: None,
        }],
        subscribe_acl: vec![ChannelMatcherRaw::Exact("e2e-trigger".to_string())],
        ..brenn_messaging_boot::test_fixtures::minimal_wasm_consumer()
    }
}

use brenn_messaging_boot::test_fixtures::io_port_raw;

/// A free input port: no channel of its own, tuned here, bound by a `link`.
fn free_input_raw(port: &str) -> brenn_lib::messaging::config::WasmConsumerSubscriptionRaw {
    brenn_lib::messaging::config::WasmConsumerSubscriptionRaw {
        channel: None,
        port: port.to_string(),
        push_depth: Some(Depth::Bounded(4)),
        retain_depth: Some(Depth::Bounded(4)),
        noise: None,
        wake_min: None,
        amplification: None,
    }
}

/// Boot the real messaging layer over `config` and load the demo guest against
/// every resolved consumer, through the same lowering production boot uses.
/// Hand-building the load spec would test the fixture, not the lowering pass.
async fn boot_dispatch(
    config: &brenn_lib::config::BrennConfig,
    db: brenn_db::Db,
    apps: &Arc<IndexMap<String, brenn_lib::config::AppConfig>>,
) -> (
    brenn_messaging_boot::MessagingResult,
    Vec<WasmConsumerConfig>,
    tokio::task::JoinHandle<()>,
) {
    use crate::{ConsumerLoadParts, lower_consumer_load_parts};

    let (alert_dispatcher, alert_handle) = noop_alert_dispatcher();
    let result = brenn_messaging_boot::test_fixtures::boot_messaging_with(
        config,
        db,
        apps,
        alert_dispatcher.clone(),
        "brenn://test",
    )
    .await;

    let messenger = result
        .messenger
        .clone()
        .expect("a config with a wasm consumer wires a messenger");
    let configs = result
        .wasm_consumers
        .iter()
        .map(|consumer| {
            let ConsumerLoadParts {
                output_ports,
                input_amplification_mt,
                mqtt_sinks,
                grants,
                output_acl,
            } = lower_consumer_load_parts(consumer);
            let component = Arc::new(ProcessorComponent::load(ProcessorLoadSpec {
                component_path: &consumer.component_path,
                slug: &consumer.slug,
                output_ports,
                input_amplification_mt,
                mqtt_sinks,
                config: consumer.config.clone(),
                grants,
                store_path: None,
                max_page_count: consumer.max_page_count,
                max_payload_bytes: config.messaging.max_body_bytes,
                alerter: noop_proc_alerter(),
                output_acl,
                mqtt_publish: None,
                tool_host: None,
            }));
            WasmConsumerConfig {
                slug: consumer.slug.clone(),
                component,
                notify: Arc::new(Notify::new()),
                messenger: Arc::clone(&messenger),
                alert_dispatcher: alert_dispatcher.clone(),
                inputs: consumer.inputs.clone(),
                outputs: consumer.outputs.clone(),
                activation_pacing: unthrottled_pacing(),
            }
        })
        .collect();

    (result, configs, alert_handle)
}

/// No apps: the default periphery for a case that only wires WASM consumers.
fn no_apps() -> Arc<IndexMap<String, brenn_lib::config::AppConfig>> {
    Arc::new(IndexMap::new())
}

/// Insert one webhook-typed envelope carrying `body` on the trigger channel.
async fn fire_trigger(messenger: &brenn_messaging::Messenger, body: &str) {
    let entry = messenger
        .directory()
        .resolve("brenn:e2e-trigger")
        .expect("the trigger channel is declared in every auto-channel config");
    let envelope = WebhookEnvelope {
        headers: vec![],
        key_id: "k".into(),
        client_ip: "127.0.0.1".into(),
        received_at: Utc::now(),
        body: body.into(),
        endpoint_slug: "e2e-trigger".into(),
    };
    testutils::insert_bus_message(
        messenger,
        &entry,
        &serde_json::to_string(&envelope).unwrap(),
        ChannelScheme::Webhook,
    )
    .await;
}

/// Everything `subscriber` is owed, as `(channel address, body)` pairs.
async fn owed_pairs(
    messenger: &brenn_messaging::Messenger,
    subscriber: &ParticipantId,
) -> Vec<(String, String)> {
    brenn_messaging::testutils::owed_everywhere(messenger, subscriber)
        .await
        .into_iter()
        .map(|(address, envelope)| (address, envelope.body.clone()))
        .collect()
}

/// The timer idiom, end to end and structural. The config declares one io_port
/// and nothing else — no `[[channel]]` block for it, no `link`, no ACL
/// entry — and the guest's own `publish-deferred` parks on the channel the
/// lowering pass placed for that port. When it releases, the port that scheduled
/// it is the port that receives it: the self-loop cannot be miswired, because
/// there is only one channel to be wired to.
#[tokio::test]
async fn an_io_port_timer_loop_delivers_the_guests_own_deferred_wake() {
    use brenn_lib::config::BrennConfig;
    use brenn_lib::messaging::config::WasmConsumerConfigRaw;

    let config = BrennConfig {
        channels: vec![trigger_channel()],
        wasm_consumers: vec![WasmConsumerConfigRaw {
            io_ports: vec![io_port_raw(
                "out",
                None,
                Depth::Bounded(4),
                Depth::Bounded(4),
            )],
            ..demo_consumer_raw("ticker")
        }],
        ..BrennConfig::default()
    };

    let (result, cfgs, _alert_handle) =
        boot_dispatch(&config, init_db_memory_lib_slice(), &no_apps()).await;
    let messenger = result.messenger.clone().unwrap();
    let cfg = &cfgs[0];
    let subscriber = ParticipantId::for_wasm("ticker");

    let auto_address = cfg.outputs[0].channel_address.clone();
    assert!(
        auto_address.starts_with("local:auto."),
        "an io_port with no channel of its own rides an anonymous non-transportable \
         channel, got {auto_address:?}"
    );

    fire_trigger(&messenger, "__defer__").await;
    drain_step(cfg, &subscriber).await;

    assert!(
        owed_pairs(&messenger, &subscriber).await.is_empty(),
        "a parked schedule is owed to nobody before it releases"
    );

    let swept = messenger
        .release_due_messages(Utc::now() + chrono::Duration::minutes(2))
        .await;
    assert_eq!(swept.released, 1, "the guest's own schedule came due");

    assert_eq!(
        owed_pairs(&messenger, &subscriber).await,
        vec![(auto_address, "deferred-payload".to_string())],
        "the released wake is owed to the same port that scheduled it"
    );

    drain_step(cfg, &subscriber).await;
    assert!(
        owed_pairs(&messenger, &subscriber).await.is_empty(),
        "the component consumed its own wake"
    );
}

/// Two consumers and one `link`: the producer's output port and the consumer's
/// input port are both free — no channel is named anywhere in the config, and
/// neither declaration carries an ACL entry for the wire. The producer's publish
/// through the real guest is what the second component is activated on, payload
/// intact.
#[tokio::test]
async fn a_link_carries_one_components_publish_into_anothers_activation() {
    let (result, cfgs, _alert_handle) =
        boot_dispatch(&link_config(), init_db_memory_lib_slice(), &no_apps()).await;
    let messenger = result.messenger.clone().unwrap();
    let producer_sub = ParticipantId::for_wasm("producer");
    let reader_sub = ParticipantId::for_wasm("reader");

    let auto_address = cfgs[0].outputs[0].channel_address.clone();
    assert!(
        auto_address.starts_with("local:auto."),
        "an all-backend connection lowers to an anonymous server-ring channel, \
         got {auto_address:?}"
    );
    assert_eq!(
        cfgs[1].inputs[0].sub.channel_address, auto_address,
        "both endpoints of the connection resolve to the one channel it created"
    );

    fire_trigger(&messenger, "hand-off-payload").await;
    drain_step(&cfgs[0], &producer_sub).await;

    assert_eq!(
        owed_pairs(&messenger, &reader_sub).await,
        vec![(auto_address, "hand-off-payload".to_string())],
        "the connection delivered the producer's publish to the reader, body intact"
    );

    drain_step(&cfgs[1], &reader_sub).await;
    assert!(
        owed_pairs(&messenger, &reader_sub).await.is_empty(),
        "the reader's activation consumed the hand-off"
    );
}

/// A producer whose free output port and a reader whose free input port are
/// wired by one `link`, plus the trigger channel that drives the producer's
/// guest.
fn link_config() -> brenn_lib::config::BrennConfig {
    use brenn_lib::config::BrennConfig;
    use brenn_lib::messaging::ComponentGrant;
    use brenn_lib::messaging::config::{
        Depth, LinkConfigRaw, LinkEndpointRaw, LinkHostRaw, WasmConsumerConfigRaw,
        WasmConsumerOutputRaw,
    };

    let producer = WasmConsumerConfigRaw {
        outputs: vec![WasmConsumerOutputRaw {
            port: "out".to_string(),
            channel: None,
            urgency: None,
            publish_per_activation: None,
            publish_capacity: None,
        }],
        ..demo_consumer_raw("producer")
    };
    let reader = WasmConsumerConfigRaw {
        slug: "reader".to_string(),
        component_path: std::path::PathBuf::from(DEMO_WASM),
        grants: vec![ComponentGrant::Ports],
        subscriptions: vec![free_input_raw("in")],
        ..brenn_messaging_boot::test_fixtures::minimal_wasm_consumer()
    };
    BrennConfig {
        channels: vec![trigger_channel()],
        wasm_consumers: vec![producer, reader],
        links: vec![LinkConfigRaw {
            link: "hand-off".to_string(),
            description: None,
            endpoints: vec![
                LinkEndpointRaw {
                    host: LinkHostRaw::Wasm {
                        slug: "producer".to_string(),
                    },
                    port: "out".to_string(),
                    publishes: true,
                    subscribes: false,
                    io_port: false,
                    push_depth: None,
                    retain_depth: None,
                },
                LinkEndpointRaw {
                    host: LinkHostRaw::Wasm {
                        slug: "reader".to_string(),
                    },
                    port: "in".to_string(),
                    publishes: false,
                    subscribes: true,
                    io_port: false,
                    push_depth: Some(Depth::Bounded(4)),
                    retain_depth: Some(Depth::Bounded(4)),
                },
            ],
        }],
        ..BrennConfig::default()
    }
}

/// Auto-injected grants reach exactly the endpoints of the link. A bystander app
/// that holds the transport grant and a `local_publish` allowlist of its own is
/// still denied on the anonymous address: the link is the authorization signal,
/// and nothing else in the config can express reach into it (an ACL matcher
/// naming the `auto` namespace is a boot panic).
#[tokio::test]
async fn a_third_party_publishing_to_an_anonymous_auto_channel_is_denied() {
    use brenn_envelope::grants::AppCapability;
    use brenn_lib::access::acl::ChannelMatcher;
    use brenn_lib::messaging::Urgency;
    use brenn_messaging::publish::{PublishOrigin, PublishResult};

    let mut bystander =
        brenn_server::test_support::app_config::default_test_app_config("graf", "Graf Test");
    bystander.policy.grants.insert(AppCapability::LocalPublish);
    bystander
        .policy
        .acls
        .local_publish
        .push(ChannelMatcher::Exact("graf.scratch".to_string()));
    let mut apps = IndexMap::new();
    apps.insert("graf".to_string(), bystander);

    let (result, cfgs, _alert_handle) =
        boot_dispatch(&link_config(), init_db_memory_lib_slice(), &Arc::new(apps)).await;
    let messenger = result.messenger.clone().unwrap();
    let auto_address = cfgs[0].outputs[0].channel_address.clone();
    let bare = auto_address.strip_prefix("local:").unwrap();

    // The endpoint's own reach, for contrast: the same address the bystander is
    // denied on is granted to the component the link bound.
    assert!(
        result.wasm_consumers[0].policy.allows_local_publish(bare),
        "the publishing endpoint holds the injected grant on its own link"
    );

    let denied = messenger
        .publish(
            PublishOrigin::Conversation { id: 1 },
            "graf",
            &auto_address,
            "intruder",
            Urgency::Normal,
            None,
            None,
            None,
        )
        .await;
    assert!(
        matches!(&denied, PublishResult::AclDenied(address) if address == &auto_address),
        "a non-endpoint principal is denied on the anonymous address, got {denied:?}"
    );

    assert!(
        owed_pairs(&messenger, &ParticipantId::for_wasm("reader"))
            .await
            .is_empty(),
        "a denied publish put no message on the channel"
    );
}

/// Naming an io_port's channel `brenn:` is the one config line that buys
/// durability, and this is what it buys: a schedule the guest parked before a
/// restart is still parked after one, and the boot on the other side re-derives
/// the same channel identity, so the release wakes the same port.
#[tokio::test]
async fn a_durable_named_io_port_channel_carries_a_schedule_across_a_restart() {
    use brenn_lib::config::BrennConfig;
    use brenn_lib::messaging::config::WasmConsumerConfigRaw;

    let config = BrennConfig {
        channels: vec![trigger_channel()],
        wasm_consumers: vec![WasmConsumerConfigRaw {
            io_ports: vec![io_port_raw(
                "out",
                Some("brenn:ticker.timer"),
                Depth::Bounded(4),
                Depth::Bounded(4),
            )],
            ..demo_consumer_raw("ticker")
        }],
        ..BrennConfig::default()
    };
    let db = init_db_memory_lib_slice();
    let subscriber = ParticipantId::for_wasm("ticker");

    // Boot 1: the guest schedules its own wake a minute out.
    {
        let (result, cfgs, _alert_handle) = boot_dispatch(&config, db.clone(), &no_apps()).await;
        let messenger = result.messenger.clone().unwrap();
        assert_eq!(cfgs[0].outputs[0].channel_address, "brenn:ticker.timer");

        fire_trigger(&messenger, "__defer__").await;
        drain_step(&cfgs[0], &subscriber).await;

        let parked: i64 = {
            let conn = messenger.db().lock().await;
            conn.query_row(
                "SELECT COUNT(*) FROM messaging_messages m \
                 JOIN messaging_channels c ON c.uuid = m.channel_uuid \
                 WHERE c.address = 'brenn:ticker.timer' AND m.deliver_after IS NOT NULL",
                [],
                |row| row.get(0),
            )
            .unwrap()
        };
        assert_eq!(parked, 1, "the schedule is a durable parked row");
    }

    // Boot 2: a restart over the same store. Nothing carried across in memory —
    // the channel identity is re-derived from the name.
    let (result, cfgs, _alert_handle) = boot_dispatch(&config, db.clone(), &no_apps()).await;
    let messenger = result.messenger.clone().unwrap();
    let timer_input = cfgs[0]
        .inputs
        .iter()
        .find(|input| input.port == "out")
        .expect("the io_port's input half is a resolved input port");
    assert_eq!(
        timer_input.sub.channel_uuid,
        brenn_lib::messaging::durable_auto_channel_uuid("ticker.timer"),
        "the boot after the restart names the same durable row"
    );

    let swept = messenger
        .release_due_messages(Utc::now() + chrono::Duration::minutes(2))
        .await;
    assert_eq!(
        swept.released, 1,
        "the parked schedule survived the restart"
    );
    assert_eq!(
        owed_pairs(&messenger, &subscriber).await,
        vec![(
            "brenn:ticker.timer".to_string(),
            "deferred-payload".to_string()
        )],
        "the released wake is owed to the io_port that scheduled it, one process ago"
    );

    drain_step(&cfgs[0], &subscriber).await;
    assert!(
        owed_pairs(&messenger, &subscriber).await.is_empty(),
        "the component consumed the wake it scheduled before the restart"
    );
}

/// Renaming a durable auto channel with no explicit `uuid` re-keys it: the
/// identity is derived from the name, so the new name is a new row and the old
/// one is left behind for the operator to delete. That is the existing posture
/// for a uuid that leaves config — pinned here so the trade-off cannot drift
/// silently.
#[tokio::test]
async fn renaming_a_durable_auto_channel_writes_a_fresh_row() {
    use brenn_lib::config::BrennConfig;
    use brenn_lib::messaging::config::WasmConsumerConfigRaw;

    let trigger = trigger_channel();
    let config_for = |address: &str| BrennConfig {
        channels: vec![trigger.clone()],
        wasm_consumers: vec![WasmConsumerConfigRaw {
            io_ports: vec![io_port_raw(
                "out",
                Some(address),
                Depth::Bounded(4),
                Depth::Bounded(4),
            )],
            ..demo_consumer_raw("ticker")
        }],
        ..BrennConfig::default()
    };
    let db = init_db_memory_lib_slice();

    let _ = boot_dispatch(&config_for("brenn:ticker.timer"), db.clone(), &no_apps()).await;
    let (result, _cfgs, _alert_handle) =
        boot_dispatch(&config_for("brenn:ticker.timer2"), db.clone(), &no_apps()).await;
    let messenger = result.messenger.clone().unwrap();

    let rows: Vec<(String, Vec<u8>)> = {
        let conn = messenger.db().lock().await;
        let mut stmt = conn
            .prepare(
                "SELECT address, uuid FROM messaging_channels \
                 WHERE address LIKE 'brenn:ticker.timer%' ORDER BY address",
            )
            .unwrap();
        stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .map(|r| r.unwrap())
            .collect()
    };
    let addresses: Vec<&str> = rows.iter().map(|(address, _)| address.as_str()).collect();
    assert_eq!(
        addresses,
        vec!["brenn:ticker.timer", "brenn:ticker.timer2"],
        "the rename left the old row behind rather than moving it"
    );
    for (address, uuid) in &rows {
        let bare = address.strip_prefix("brenn:").unwrap();
        assert_eq!(
            uuid::Uuid::from_slice(uuid).unwrap(),
            brenn_lib::messaging::durable_auto_channel_uuid(bare),
            "each row is keyed by the uuid derived from its own name"
        );
    }
}
