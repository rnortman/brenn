//! Multi-port activation family (processor-multiport / processor-dual fixtures),
//! plus the activation-scoped single-port trap test.

use super::*;

/// §5 test 6: a trap row on channel[0] with a normal row on channel[1] in the
/// same activation → one invocation, both rows acked at activation start, failure
/// records for BOTH channels (activation-scoped quarantine), exactly ONE alert,
/// nothing published. Activation-scoped quarantine is deliberate — design §2.4.
#[tokio::test]
async fn activation_scoped_failure_quarantines_all_ports_and_fires_one_alert() {
    let slug = "mc-activation-fail";
    let (messenger, channels, wasm_sub, cfg, alert_handle, _db) =
        build_multi_channel_setup(slug, &["act-trap-ch", "act-ok-ch"]).await;

    // channel[0] gets a trap row; channel[1] gets a normal row.
    // Both arrive in the same activation (multi-port snapshot).
    let (trap_push_id, _) = testutils::insert_wasm_push(
        &messenger,
        &channels[0],
        &wasm_sub,
        "__trap__",
        ChannelScheme::Brenn,
    )
    .await;
    let (ok_push_id, _) = testutils::insert_wasm_push(
        &messenger,
        &channels[1],
        &wasm_sub,
        "ok-body",
        ChannelScheme::Brenn,
    )
    .await;

    // Override with a capturing alerter to assert alert count.
    use brenn_lib::obs::alerting::make_capturing_alerter_with_severity;
    let (alert_dispatcher, captured_alerts, cap_handle) = make_capturing_alerter_with_severity();
    let _db2 = tempfile::NamedTempFile::new().unwrap();
    let component2 = Arc::new(ProcessorComponent::load(ProcessorLoadSpec {
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
    let notify2 = Arc::new(tokio::sync::Notify::new());
    let cfg2 = WasmConsumerConfig {
        slug: slug.to_string(),
        component: component2,
        notify: notify2,
        messenger: Arc::clone(&messenger),
        alert_dispatcher,
        inputs: cfg.inputs.clone(),
        activation_pacing: unthrottled_pacing(),
    };

    let mut last_seen = HashMap::new();
    drain_step(&cfg2, &wasm_sub, &mut last_seen).await;

    // Both push rows must be acked (ack-at-start — neither remains pending).
    let remaining = messenger.load_pending_pushes(&wasm_sub).await;
    assert!(
        remaining.is_empty(),
        "both push rows must be acked after drain; remaining={remaining:?} \
         trap_push_id={trap_push_id} ok_push_id={ok_push_id}"
    );

    // Ack-at-start direct-DB check (test-1): delivered_at must be set on BOTH push rows
    // after drain regardless of Trap outcome. A regression where mark_pushes_delivered
    // was moved inside record_wasm_activation_failure (post-guest) would pass the
    // load_pending_pushes check but fail here.
    {
        let conn = messenger.db().lock().await;
        for (push_id, label) in [(trap_push_id, "trap"), (ok_push_id, "ok")] {
            let delivered_at: Option<String> = conn
                .query_row(
                    "SELECT delivered_at FROM messaging_pending_pushes WHERE id = ?1",
                    rusqlite::params![push_id],
                    |r| r.get(0),
                )
                .unwrap_or(None);
            assert!(
                delivered_at.is_some(),
                "ack-at-start: push row {push_id} ({label}) must have delivered_at set \
                 (mark_pushes_delivered must run before guest) — got None"
            );
        }
    }

    // channel[0]'s row must appear in the failures table (trap triggered quarantine).
    let trap_failure_count: i64 = {
        let conn = messenger.db().lock().await;
        conn.query_row(
            "SELECT COUNT(*) FROM messaging_wasm_consume_failures \
             WHERE subscriber = ?1 AND channel = ?2",
            rusqlite::params![wasm_sub.as_str(), channels[0].address.as_str()],
            |row| row.get(0),
        )
        .expect("query failure count for trap channel")
    };
    assert_eq!(
        trap_failure_count, 1,
        "trap channel must have exactly one failure record"
    );

    // channel[1]'s row must ALSO appear in the failures table (activation-scoped).
    let ok_failure_count: i64 = {
        let conn = messenger.db().lock().await;
        conn.query_row(
            "SELECT COUNT(*) FROM messaging_wasm_consume_failures \
             WHERE subscriber = ?1 AND channel = ?2",
            rusqlite::params![wasm_sub.as_str(), channels[1].address.as_str()],
            |row| row.get(0),
        )
        .expect("query failure count for ok channel")
    };
    assert_eq!(
        ok_failure_count, 1,
        "ok channel must ALSO have a failure record — activation-scoped quarantine"
    );

    drop(cfg2);
    let _ = cap_handle.await;

    // Exactly ONE alert must fire for the whole activation (not one per channel).
    // Clone to drop the MutexGuard before the next await.
    let alert_count = {
        let alerts = captured_alerts.lock().unwrap();
        let count = alerts.len();
        let summary = format!("{:?}", &*alerts);
        (count, summary)
    };
    assert_eq!(
        alert_count.0, 1,
        "exactly one alert for the trapped activation, got {}: {}",
        alert_count.0, alert_count.1
    );
    drop(cfg);
    let _ = alert_handle.await;
}

/// §5 test 1 (combined activation): 2 triggering channels both pending →
/// exactly ONE drain activation → exactly ONE summary message in the output
/// channel; the summary has 2 entries in cfg.inputs order with correct
/// `len`/`new_from` values.
///
/// Uses the `processor-multiport` fixture which publishes one summary JSON
/// per activation to port "out", making activation count directly assertable.
#[tokio::test]
async fn multiport_combined_activation_two_triggering_channels() {
    let slug = "mp-combined";
    let (messenger, in_entries, out_entry, out_sub, wasm_sub, cfg, _alert_handle, _store_db) =
        build_multiport_setup(slug, &["mp-comb-ch-a", "mp-comb-ch-b"]).await;

    // Insert one row on each input channel.
    testutils::insert_wasm_push(
        &messenger,
        &in_entries[0],
        &wasm_sub,
        "body-a",
        ChannelScheme::Brenn,
    )
    .await;
    testutils::insert_wasm_push(
        &messenger,
        &in_entries[1],
        &wasm_sub,
        "body-b",
        ChannelScheme::Brenn,
    )
    .await;

    // Drain — must invoke the guest exactly once.
    let mut last_seen = HashMap::new();
    drain_step(&cfg, &wasm_sub, &mut last_seen).await;

    // Input push rows must be acked.
    let remaining = messenger.load_pending_pushes(&wasm_sub).await;
    assert!(
        remaining.is_empty(),
        "all input push rows must be delivered after drain"
    );

    // Exactly one summary message in the output channel.
    let out_rows = messenger.load_pending_pushes(&out_sub).await;
    assert_eq!(
        out_rows.len(),
        1,
        "exactly one summary message (one activation); got {}",
        out_rows.len()
    );

    // Parse the summary from the output message body.
    let summary_body = {
        let conn = messenger.db().lock().await;
        conn.query_row(
            "SELECT m.body \
             FROM messaging_messages m \
             JOIN messaging_channels c ON c.uuid = m.channel_uuid \
             WHERE c.address = ?1 \
             ORDER BY m.publish_ts_ns ASC LIMIT 1",
            rusqlite::params![out_entry.address.as_str()],
            |row| row.get::<_, String>(0),
        )
        .expect("output message must exist")
    };

    let summary: serde_json::Value =
        serde_json::from_str(&summary_body).expect("summary must be valid JSON");
    let arr = summary.as_array().expect("summary must be a JSON array");

    assert_eq!(
        arr.len(),
        2,
        "summary must have 2 entries (one per input port)"
    );

    // Port order must match cfg.inputs order: "in0" then "in1".
    assert_eq!(
        arr[0]["port"].as_str(),
        Some("in0"),
        "first entry must be port in0"
    );
    assert_eq!(
        arr[1]["port"].as_str(),
        Some("in1"),
        "second entry must be port in1"
    );

    // Each port window has 1 new message (len=1, new_from=0).
    assert_eq!(arr[0]["len"].as_u64(), Some(1), "in0 len must be 1");
    assert_eq!(
        arr[0]["new_from"].as_u64(),
        Some(0),
        "in0 new_from must be 0"
    );
    assert_eq!(arr[1]["len"].as_u64(), Some(1), "in1 len must be 1");
    assert_eq!(
        arr[1]["new_from"].as_u64(),
        Some(0),
        "in1 new_from must be 0"
    );

    // No drops on fresh consumer.
    assert_eq!(arr[0]["dropped"].as_u64(), Some(0), "in0 dropped must be 0");
    assert_eq!(arr[1]["dropped"].as_u64(), Some(0), "in1 dropped must be 0");
}

/// §5 test 2 (sampled port as pure context): triggering port (in0,
/// push_depth=Unbounded) + sampled port (in1, push_depth=Bounded(0),
/// retain_depth=Unbounded) with retained messages on in1.
///
/// Expected: one activation; summary has 2 entries; in0 is a new-message
/// window (`new_from=0, len=1`); in1 is pure context (`new_from==len>0`).
#[tokio::test]
async fn multiport_sampled_port_included_as_pure_context() {
    let slug = "mp-sampled";
    let (messenger, in_entries, out_entry, out_sub, wasm_sub, cfg, _alert_handle, _store_db) =
        build_multiport_setup_with_depths(
            slug,
            &[
                ("mp-samp-ch-trigger", Depth::Unbounded, Depth::Unbounded),
                ("mp-samp-ch-sampled", Depth::Bounded(0), Depth::Unbounded),
            ],
        )
        .await;

    // Insert 2 retained-only messages on the sampled channel (in1).
    // These have no push rows so they appear only as retained context.
    testutils::insert_retain_only(
        &messenger,
        &in_entries[1],
        "sampled-msg-1",
        ChannelScheme::Brenn,
    )
    .await;
    testutils::insert_retain_only(
        &messenger,
        &in_entries[1],
        "sampled-msg-2",
        ChannelScheme::Brenn,
    )
    .await;

    // Insert one triggering push row on in0.
    testutils::insert_wasm_push(
        &messenger,
        &in_entries[0],
        &wasm_sub,
        "trigger-msg",
        ChannelScheme::Brenn,
    )
    .await;

    // Drain — must invoke the guest exactly once.
    let mut last_seen = HashMap::new();
    drain_step(&cfg, &wasm_sub, &mut last_seen).await;

    // in0 push row must be acked; no push rows existed for in1.
    let remaining = messenger.load_pending_pushes(&wasm_sub).await;
    assert!(
        remaining.is_empty(),
        "all push rows must be delivered after drain"
    );

    // Exactly one summary message in the output channel.
    let out_rows = messenger.load_pending_pushes(&out_sub).await;
    assert_eq!(
        out_rows.len(),
        1,
        "exactly one summary message (one activation); got {}",
        out_rows.len()
    );

    // Parse the summary.
    let summary_body = {
        let conn = messenger.db().lock().await;
        conn.query_row(
            "SELECT m.body \
             FROM messaging_messages m \
             JOIN messaging_channels c ON c.uuid = m.channel_uuid \
             WHERE c.address = ?1 \
             ORDER BY m.publish_ts_ns ASC LIMIT 1",
            rusqlite::params![out_entry.address.as_str()],
            |row| row.get::<_, String>(0),
        )
        .expect("output message must exist")
    };

    let summary: serde_json::Value =
        serde_json::from_str(&summary_body).expect("summary must be valid JSON");
    let arr = summary.as_array().expect("summary must be a JSON array");

    assert_eq!(
        arr.len(),
        2,
        "summary must have 2 entries (one per bound input port)"
    );

    // Port order: in0 (triggering), then in1 (sampled).
    assert_eq!(
        arr[0]["port"].as_str(),
        Some("in0"),
        "first entry must be port in0"
    );
    assert_eq!(
        arr[1]["port"].as_str(),
        Some("in1"),
        "second entry must be port in1"
    );

    // in0: new message window — len=1, new_from=0.
    assert_eq!(arr[0]["len"].as_u64(), Some(1), "in0 len must be 1");
    assert_eq!(
        arr[0]["new_from"].as_u64(),
        Some(0),
        "in0 new_from must be 0 (new messages)"
    );

    // in1: pure context window — new_from == len > 0 (2 retained messages).
    let in1_len = arr[1]["len"].as_u64().expect("in1 len must be present");
    let in1_new_from = arr[1]["new_from"]
        .as_u64()
        .expect("in1 new_from must be present");
    assert!(
        in1_len > 0,
        "in1 must have retained context messages (got len=0)"
    );
    assert_eq!(
        in1_new_from, in1_len,
        "in1 must be pure context (new_from == len); got new_from={in1_new_from}, len={in1_len}"
    );

    // No drops.
    assert_eq!(arr[0]["dropped"].as_u64(), Some(0), "in0 dropped must be 0");
    assert_eq!(arr[1]["dropped"].as_u64(), Some(0), "in1 dropped must be 0");
}

/// §5 test 3 (sampled-only traffic, no activation): only the sampled channel
/// (push_depth=0) has messages (inserted as retained context only). No push rows
/// exist so `drain_step` returns without invoking the guest and produces no output.
#[tokio::test]
async fn multiport_sampled_only_no_push_rows_no_activation() {
    let slug = "mp-sampled-only";
    let (messenger, in_entries, _out_entry, out_sub, wasm_sub, cfg, _alert_handle, _store_db) =
        build_multiport_setup_with_depths(
            slug,
            &[
                ("mp-so-ch-sampled", Depth::Bounded(0), Depth::Unbounded),
                ("mp-so-ch-trigger", Depth::Unbounded, Depth::Unbounded),
            ],
        )
        .await;

    // Insert retained-only messages on the sampled channel — no push rows created.
    testutils::insert_retain_only(
        &messenger,
        &in_entries[0],
        "sampled-only-msg",
        ChannelScheme::Brenn,
    )
    .await;

    // Do NOT insert any push row on the triggering channel.
    let push_rows_before = messenger.load_pending_pushes(&wasm_sub).await;
    assert!(
        push_rows_before.is_empty(),
        "no push rows before drain (only retained-only messages exist)"
    );

    // Drain: no triggering port has pending rows → no activation → no output.
    let mut last_seen = HashMap::new();
    drain_step(&cfg, &wasm_sub, &mut last_seen).await;

    // Still no push rows (nothing acked, nothing created).
    let push_rows_after = messenger.load_pending_pushes(&wasm_sub).await;
    assert!(
        push_rows_after.is_empty(),
        "still no push rows after drain — sampled-only traffic must not activate"
    );

    // No summary message in the output channel (no invocation occurred).
    let out_rows = messenger.load_pending_pushes(&out_sub).await;
    assert!(
        out_rows.is_empty(),
        "no output message must be produced when only sampled messages exist"
    );
}

/// §5 test 4 (triggering port without rows gets context window): rows pending on
/// in0 only; in1 (triggering, retain_depth=Unbounded) has retained history but
/// no pending push rows this step → in1 appears as pure-context window.
///
/// Setup: drain 2 messages on in1 first (they become retained context). Then
/// insert a push row on in0 only. Drain → one activation; in0 has `new_from=0`
/// (new messages); in1 has `new_from==len>0` (pure context).
#[tokio::test]
async fn multiport_triggering_port_without_rows_appears_as_context_window() {
    let slug = "mp-ctx-window";
    let (messenger, in_entries, out_entry, out_sub, wasm_sub, cfg, _alert_handle, _store_db) =
        build_multiport_setup_with_depths(
            slug,
            &[
                ("mp-ctx-ch-a", Depth::Unbounded, Depth::Unbounded),
                ("mp-ctx-ch-b", Depth::Unbounded, Depth::Unbounded),
            ],
        )
        .await;

    // Insert 2 push rows on in1 and drain them — they become retained context.
    for i in 0..2usize {
        testutils::insert_wasm_push(
            &messenger,
            &in_entries[1],
            &wasm_sub,
            &format!("in1-ctx-{i}"),
            ChannelScheme::Brenn,
        )
        .await;
    }
    let mut last_seen = HashMap::new();
    drain_step(&cfg, &wasm_sub, &mut last_seen).await;
    // First drain consumed in1 rows as a triggering activation (in1 had rows, in0 didn't).
    // After drain, in1 messages are now retained context. Discard the output of this drain.
    {
        let conn = messenger.db().lock().await;
        conn.execute(
            "UPDATE messaging_pending_pushes SET delivered_at = datetime('now') \
             WHERE delivered_at IS NULL",
            [],
        )
        .expect("mark all output pushes delivered");
    }

    // Now insert a push row on in0 only — in1 has no new push rows.
    testutils::insert_wasm_push(
        &messenger,
        &in_entries[0],
        &wasm_sub,
        "in0-new-msg",
        ChannelScheme::Brenn,
    )
    .await;

    // Drain again: in0 has a push row (triggering), in1 has retained context only.
    drain_step(&cfg, &wasm_sub, &mut last_seen).await;

    // Exactly one summary in the output channel for this drain.
    let out_rows = messenger.load_pending_pushes(&out_sub).await;
    assert_eq!(
        out_rows.len(),
        1,
        "exactly one summary from the second drain; got {}",
        out_rows.len()
    );

    let summary_body = {
        let conn = messenger.db().lock().await;
        // Get the most recent message on the output channel.
        conn.query_row(
            "SELECT m.body \
             FROM messaging_messages m \
             JOIN messaging_channels c ON c.uuid = m.channel_uuid \
             WHERE c.address = ?1 \
             ORDER BY m.publish_ts_ns DESC LIMIT 1",
            rusqlite::params![out_entry.address.as_str()],
            |row| row.get::<_, String>(0),
        )
        .expect("output message must exist")
    };

    let summary: serde_json::Value =
        serde_json::from_str(&summary_body).expect("summary must be valid JSON");
    let arr = summary.as_array().expect("summary must be a JSON array");
    assert_eq!(arr.len(), 2, "summary must have 2 entries");

    // in0: new messages window — new_from=0.
    assert_eq!(
        arr[0]["port"].as_str(),
        Some("in0"),
        "first entry must be port in0"
    );
    assert_eq!(
        arr[0]["new_from"].as_u64(),
        Some(0),
        "in0 must have new messages (new_from=0)"
    );
    assert!(
        arr[0]["len"].as_u64().unwrap_or(0) > 0,
        "in0 must have at least 1 envelope"
    );

    // in1: pure-context window — new_from == len > 0.
    assert_eq!(
        arr[1]["port"].as_str(),
        Some("in1"),
        "second entry must be port in1"
    );
    let in1_len = arr[1]["len"].as_u64().expect("in1 len must be present");
    let in1_new_from = arr[1]["new_from"]
        .as_u64()
        .expect("in1 new_from must be present");
    assert!(
        in1_len > 0,
        "in1 must have retained context (len > 0); got len=0"
    );
    assert_eq!(
        in1_new_from, in1_len,
        "in1 must be pure context (new_from==len); new_from={in1_new_from}, len={in1_len}"
    );
}

/// §5 test 7 (all-or-nothing across ports): in0 has a normal row (would publish
/// to output), in1 has a `__trap__` row → single invocation → trap → no output
/// on any bound output channel. Both push rows are acked (at-most-once).
#[tokio::test]
async fn multiport_all_or_nothing_trap_discards_output_from_all_ports() {
    let slug = "mp-aon-trap";
    let (messenger, in_entries, out_entry, out_sub, wasm_sub, cfg, _alert_handle, _store_db) =
        build_multiport_setup(slug, &["mp-aon-ch-a", "mp-aon-ch-b"]).await;

    // in0: normal message — the fixture would publish a summary on Ok.
    testutils::insert_wasm_push(
        &messenger,
        &in_entries[0],
        &wasm_sub,
        "normal-body",
        ChannelScheme::Brenn,
    )
    .await;
    // in1: trap sentinel — causes the fixture to trap, discarding any buffered publish.
    testutils::insert_wasm_push(
        &messenger,
        &in_entries[1],
        &wasm_sub,
        "__trap__",
        ChannelScheme::Brenn,
    )
    .await;

    let mut last_seen = HashMap::new();
    drain_step(&cfg, &wasm_sub, &mut last_seen).await;

    // Both push rows must be acked (ack-at-start, at-most-once).
    let remaining = messenger.load_pending_pushes(&wasm_sub).await;
    assert!(
        remaining.is_empty(),
        "both push rows must be acked after trap drain"
    );

    // No output: trap discards the buffered summary publish.
    let out_rows = messenger.load_pending_pushes(&out_sub).await;
    assert!(
        out_rows.is_empty(),
        "no output must be published when trap fires; out_entry={}",
        out_entry.address
    );
}

/// §5 test-2 (Err arm on multi-port): 2-port activation where the guest returns
/// Err (not Trap) — both channels get failure records, no output is published,
/// both push rows are acked. Exercises the Err arm of drain_step under multi-port
/// (the Trap arm is covered by multiport_all_or_nothing_trap_discards_output_from_all_ports).
#[tokio::test]
async fn multiport_err_outcome_quarantines_both_channels() {
    let slug = "mp-err";
    let (messenger, in_entries, out_entry, out_sub, wasm_sub, cfg, alert_handle, _store_db) =
        build_multiport_setup(slug, &["mp-err-ch-a", "mp-err-ch-b"]).await;

    // Override with a capturing alerter.
    use brenn_lib::obs::alerting::make_capturing_alerter_with_severity;
    let (alert_dispatcher, captured_alerts, cap_handle) = make_capturing_alerter_with_severity();
    let _db2 = tempfile::NamedTempFile::new().unwrap();
    let component2 = Arc::new(ProcessorComponent::load(ProcessorLoadSpec {
        component_path: std::path::Path::new(MULTIPORT_WASM),
        slug,
        output_ports: {
            let mut m = std::collections::HashMap::new();
            m.insert(
                "out".to_string(),
                test_out_spec(cfg.inputs[0].sub.channel_address.clone()),
            );
            m
        },
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
    let notify2 = Arc::new(tokio::sync::Notify::new());
    let cfg2 = WasmConsumerConfig {
        slug: slug.to_string(),
        component: component2,
        notify: notify2,
        messenger: Arc::clone(&messenger),
        alert_dispatcher,
        inputs: cfg.inputs.clone(),
        activation_pacing: unthrottled_pacing(),
    };

    // in0: normal body; in1: __err__ sentinel → Err returned.
    let (push_a, _) = testutils::insert_wasm_push(
        &messenger,
        &in_entries[0],
        &wasm_sub,
        "normal-body",
        ChannelScheme::Brenn,
    )
    .await;
    let (push_b, _) = testutils::insert_wasm_push(
        &messenger,
        &in_entries[1],
        &wasm_sub,
        "__err__",
        ChannelScheme::Brenn,
    )
    .await;

    let mut last_seen = HashMap::new();
    drain_step(&cfg2, &wasm_sub, &mut last_seen).await;

    // Both push rows must be acked (ack-at-start).
    let remaining = messenger.load_pending_pushes(&wasm_sub).await;
    assert!(
        remaining.is_empty(),
        "both push rows must be acked after Err; push_a={push_a} push_b={push_b}"
    );

    // Both channels must have failure records (activation-scoped quarantine under Err).
    for (ch, label) in [(&in_entries[0], "in0"), (&in_entries[1], "in1")] {
        let count: i64 = {
            let conn = messenger.db().lock().await;
            conn.query_row(
                "SELECT COUNT(*) FROM messaging_wasm_consume_failures \
                 WHERE subscriber = ?1 AND channel = ?2",
                rusqlite::params![wasm_sub.as_str(), ch.address.as_str()],
                |row| row.get(0),
            )
            .expect("query failure count")
        };
        assert_eq!(
            count, 1,
            "{label} must have exactly one failure record after Err"
        );
    }

    // No output published.
    let out_rows = messenger.load_pending_pushes(&out_sub).await;
    assert!(
        out_rows.is_empty(),
        "no output must be published when Err fires; out_entry={}",
        out_entry.address
    );

    drop(cfg2);
    drop(cfg);
    let _ = cap_handle.await;
    let _ = alert_handle.await;

    // Exactly one alert must fire (one Err → one activation-scoped alert).
    let alert_count = {
        let alerts = captured_alerts.lock().unwrap();
        alerts.len()
    };
    assert_eq!(alert_count, 1, "exactly one alert for Err activation");
}

/// §5 test 8 (drop reporting under multi-port): overflow on in0, rows also
/// pending on in1 → one activation; in0's window reports `dropped > 0` exactly
/// once; next activation reports 0 for both.
///
/// Uses `testutils::inject_drop` to simulate overflow without requiring a full
/// app-level publish path (which needs noise=Metered and an app config).
#[tokio::test]
async fn multiport_drop_reporting_exactly_once() {
    let slug = "mp-drop";
    let (messenger, in_entries, out_entry, out_sub, wasm_sub, cfg, _alert_handle, _store_db) =
        build_multiport_setup(slug, &["mp-drop-ch-a", "mp-drop-ch-b"]).await;

    // Insert one push row on each input channel.
    testutils::insert_wasm_push(
        &messenger,
        &in_entries[0],
        &wasm_sub,
        "body-a",
        ChannelScheme::Brenn,
    )
    .await;
    testutils::insert_wasm_push(
        &messenger,
        &in_entries[1],
        &wasm_sub,
        "body-b",
        ChannelScheme::Brenn,
    )
    .await;

    // Simulate overflow on in0: inject a drop count of 2.
    testutils::inject_drop(&messenger, &in_entries[0].address, &wasm_sub, 2);
    assert_eq!(
        messenger.drop_counter(&in_entries[0].address, &wasm_sub),
        2,
        "in0 drop counter must be 2 before drain"
    );
    assert_eq!(
        messenger.drop_counter(&in_entries[1].address, &wasm_sub),
        0,
        "in1 drop counter must be 0 before drain"
    );

    // Drain: one activation with both ports; in0 reports dropped=2, in1 reports 0.
    let mut last_seen = HashMap::new();
    drain_step(&cfg, &wasm_sub, &mut last_seen).await;

    // Parse the first summary.
    let summary1_body = {
        let conn = messenger.db().lock().await;
        conn.query_row(
            "SELECT m.body \
             FROM messaging_messages m \
             JOIN messaging_channels c ON c.uuid = m.channel_uuid \
             WHERE c.address = ?1 \
             ORDER BY m.publish_ts_ns ASC LIMIT 1",
            rusqlite::params![out_entry.address.as_str()],
            |row| row.get::<_, String>(0),
        )
        .expect("first summary must exist")
    };
    let s1: serde_json::Value =
        serde_json::from_str(&summary1_body).expect("summary1 must be valid JSON");
    let a1 = s1.as_array().expect("summary1 must be array");
    assert_eq!(a1.len(), 2, "summary1 must have 2 entries");
    // in0 must report dropped=2.
    assert_eq!(
        a1[0]["port"].as_str(),
        Some("in0"),
        "first entry must be in0"
    );
    assert_eq!(
        a1[0]["dropped"].as_u64(),
        Some(2),
        "in0 must report dropped=2 in the first activation"
    );
    // in1 must report dropped=0.
    assert_eq!(
        a1[1]["port"].as_str(),
        Some("in1"),
        "second entry must be in1"
    );
    assert_eq!(
        a1[1]["dropped"].as_u64(),
        Some(0),
        "in1 must report dropped=0 in the first activation"
    );

    // Mark out_sub's pending rows delivered so we can detect new output in the second
    // drain. Scoped to out_sub only to avoid accidentally clearing wasm_sub input rows
    // (test-5: a blanket UPDATE would mask partial-ack bugs on the input side).
    {
        let conn = messenger.db().lock().await;
        conn.execute(
            "UPDATE messaging_pending_pushes SET delivered_at = datetime('now') \
             WHERE target_subscriber = ?1 AND delivered_at IS NULL",
            rusqlite::params![out_sub.as_str()],
        )
        .expect("mark out_sub pending delivered");
    }

    // Second drain: insert new rows on both channels; no new overflow.
    testutils::insert_wasm_push(
        &messenger,
        &in_entries[0],
        &wasm_sub,
        "body-a2",
        ChannelScheme::Brenn,
    )
    .await;
    testutils::insert_wasm_push(
        &messenger,
        &in_entries[1],
        &wasm_sub,
        "body-b2",
        ChannelScheme::Brenn,
    )
    .await;

    drain_step(&cfg, &wasm_sub, &mut last_seen).await;

    // Parse the second summary (most recently published).
    let summary2_body = {
        let conn = messenger.db().lock().await;
        conn.query_row(
            "SELECT m.body \
             FROM messaging_messages m \
             JOIN messaging_channels c ON c.uuid = m.channel_uuid \
             WHERE c.address = ?1 \
             ORDER BY m.publish_ts_ns DESC LIMIT 1",
            rusqlite::params![out_entry.address.as_str()],
            |row| row.get::<_, String>(0),
        )
        .expect("second summary must exist")
    };
    let s2: serde_json::Value =
        serde_json::from_str(&summary2_body).expect("summary2 must be valid JSON");
    let a2 = s2.as_array().expect("summary2 must be array");
    assert_eq!(a2.len(), 2, "summary2 must have 2 entries");
    // Both must report dropped=0 on the second activation.
    assert_eq!(
        a2[0]["dropped"].as_u64(),
        Some(0),
        "in0 must report dropped=0 on second activation (delta advanced)"
    );
    assert_eq!(
        a2[1]["dropped"].as_u64(),
        Some(0),
        "in1 must report dropped=0 on second activation"
    );
}

/// §5 test 10 (retain_depth=0 triggering port): the port has push_depth=Unbounded
/// (triggering) but retain_depth=Bounded(0) — window must have `new_from=0`
/// (new messages), no context prefix, and no context in the summary.
#[tokio::test]
async fn multiport_retain_depth_zero_triggering_port_no_context() {
    let slug = "mp-retain0";
    let (messenger, in_entries, out_entry, out_sub, wasm_sub, cfg, _alert_handle, _store_db) =
        build_multiport_setup_with_depths(
            slug,
            &[
                ("mp-r0-ch-a", Depth::Unbounded, Depth::Bounded(0)),
                ("mp-r0-ch-b", Depth::Unbounded, Depth::Unbounded),
            ],
        )
        .await;

    // Insert push rows on both channels.
    testutils::insert_wasm_push(
        &messenger,
        &in_entries[0],
        &wasm_sub,
        "no-retain-body",
        ChannelScheme::Brenn,
    )
    .await;
    testutils::insert_wasm_push(
        &messenger,
        &in_entries[1],
        &wasm_sub,
        "normal-body",
        ChannelScheme::Brenn,
    )
    .await;

    let mut last_seen = HashMap::new();
    drain_step(&cfg, &wasm_sub, &mut last_seen).await;

    // One summary in output.
    let out_rows = messenger.load_pending_pushes(&out_sub).await;
    assert_eq!(out_rows.len(), 1, "exactly one summary");

    let summary_body = {
        let conn = messenger.db().lock().await;
        conn.query_row(
            "SELECT m.body \
             FROM messaging_messages m \
             JOIN messaging_channels c ON c.uuid = m.channel_uuid \
             WHERE c.address = ?1 \
             ORDER BY m.publish_ts_ns DESC LIMIT 1",
            rusqlite::params![out_entry.address.as_str()],
            |row| row.get::<_, String>(0),
        )
        .expect("summary must exist")
    };
    let summary: serde_json::Value =
        serde_json::from_str(&summary_body).expect("summary must be valid JSON");
    let arr = summary.as_array().expect("summary must be array");
    assert_eq!(arr.len(), 2, "summary must have 2 entries");

    // in0: retain_depth=0 → new_from=0 (new messages only), len=1 (the new message),
    // no context prefix.
    assert_eq!(
        arr[0]["port"].as_str(),
        Some("in0"),
        "first entry must be in0"
    );
    assert_eq!(
        arr[0]["len"].as_u64(),
        Some(1),
        "in0 must have len=1 (one new message)"
    );
    assert_eq!(
        arr[0]["new_from"].as_u64(),
        Some(0),
        "in0 new_from must be 0 (no context, all messages are new)"
    );

    // in1: retain_depth=Unbounded, also just one new message.
    assert_eq!(
        arr[1]["port"].as_str(),
        Some("in1"),
        "second entry must be in1"
    );
    assert_eq!(arr[1]["len"].as_u64(), Some(1), "in1 must have len=1");
    assert_eq!(
        arr[1]["new_from"].as_u64(),
        Some(0),
        "in1 new_from must be 0"
    );
}

/// §5 test 13 (processor-dual via genuine 2-window activation): set up
/// `processor-dual` with 2 input channels and 2 output channels ("out1", "out2").
/// A message on each input triggers one activation → dual publishes each new
/// envelope body to both "out1" and "out2".
///
/// This exercises per-port publish resolution under a multi-port input activation:
/// each port name must independently resolve to its own channel address.
#[tokio::test]
async fn processor_dual_multi_port_activation_per_port_publish_resolution() {
    const DUAL_WASM: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../brenn-wasm/target/components/brenn_processor_dual.wasm"
    );

    use brenn_lib::messaging::config::MessagingGlobalConfig;
    use brenn_lib::messaging::config::{NoiseLevel, ResolvedChannel, Sink};
    use brenn_lib::messaging::{
        ChannelScheme, MessagingDirectory, SubscriberEntry, SubscriberEntryKind, WakeMin,
    };
    use uuid::Uuid;

    let slug = "mp-dual";
    let db = init_db_memory();
    let wasm_sub = ParticipantId::for_wasm(slug);
    let out1_sub_slug = format!("{slug}-out1");
    let out2_sub_slug = format!("{slug}-out2");
    let out1_sub = ParticipantId::for_wasm(&out1_sub_slug);
    let out2_sub = ParticipantId::for_wasm(&out2_sub_slug);

    let in0_entry =
        (*testutils::wasm_channel_entry(slug, "mp-dual-in0", Depth::Unbounded, Depth::Unbounded))
            .clone();
    let in1_entry =
        (*testutils::wasm_channel_entry(slug, "mp-dual-in1", Depth::Unbounded, Depth::Unbounded))
            .clone();

    let out1_addr = format!("brenn:{slug}:out1");
    let out2_addr = format!("brenn:{slug}:out2");
    let out1_entry = ChannelEntry {
        uuid: Uuid::new_v4(),
        address: out1_addr.clone(),
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
            kind: SubscriberEntryKind::Wasm(out1_sub_slug.clone()),
            push_depth: Depth::Unbounded,
            retain_depth: Depth::Unbounded,
            noise: NoiseLevel::Silent,
            wake_min: None,
        }],
        transport_type: ChannelScheme::Brenn,
        mount: None,
    };
    let out2_entry = ChannelEntry {
        uuid: Uuid::new_v4(),
        address: out2_addr.clone(),
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
            kind: SubscriberEntryKind::Wasm(out2_sub_slug.clone()),
            push_depth: Depth::Unbounded,
            retain_depth: Depth::Unbounded,
            noise: NoiseLevel::Silent,
            wake_min: None,
        }],
        transport_type: ChannelScheme::Brenn,
        mount: None,
    };

    let all_entries = vec![
        in0_entry.clone(),
        in1_entry.clone(),
        out1_entry.clone(),
        out2_entry.clone(),
    ];
    {
        let conn = db.lock().await;
        upsert_channels(&conn, &all_entries);
    }
    // Delivery-time ACL gate (design §2.2 Point A): the out1/out2 WASM subscribers
    // must hold a covering policy or `resolve_push_targets` denies them.
    let wasm_policies = wasm_policies_from_entries(&all_entries);
    let directory = Arc::new(MessagingDirectory::with_entries(all_entries));
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
        wasm_policies,
    ));

    let in0_arc = Arc::new(in0_entry.clone());
    let in1_arc = Arc::new(in1_entry.clone());

    let mut output_ports = std::collections::HashMap::new();
    output_ports.insert("out1".to_string(), test_out_spec(out1_addr.clone()));
    output_ports.insert("out2".to_string(), test_out_spec(out2_addr.clone()));

    let _store_db = tempfile::NamedTempFile::new().unwrap();
    let component = Arc::new(ProcessorComponent::load(ProcessorLoadSpec {
        component_path: std::path::Path::new(DUAL_WASM),
        slug,
        output_ports,
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

    let (alert_dispatcher, _alert_handle) = noop_alert_dispatcher();
    let notify = Arc::new(Notify::new());
    let cfg = WasmConsumerConfig {
        slug: slug.to_string(),
        component,
        notify,
        messenger: Arc::clone(&messenger),
        alert_dispatcher,
        inputs: vec![
            WasmInputPort {
                port: "in0".to_string(),
                sub: ResolvedSubscription {
                    channel_uuid: in0_arc.uuid,
                    channel_address: in0_arc.address.clone(),
                    push_depth: Depth::Unbounded,
                    retain_depth: Depth::Unbounded,
                    noise: NoiseLevel::Silent,
                    wake_min: WakeMin::Normal,
                },
                amplification_mt: 1000,
            },
            WasmInputPort {
                port: "in1".to_string(),
                sub: ResolvedSubscription {
                    channel_uuid: in1_arc.uuid,
                    channel_address: in1_arc.address.clone(),
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

    // Insert one message on each input channel.
    testutils::insert_wasm_push(
        &messenger,
        &in0_arc,
        &wasm_sub,
        "msg-a",
        ChannelScheme::Brenn,
    )
    .await;
    testutils::insert_wasm_push(
        &messenger,
        &in1_arc,
        &wasm_sub,
        "msg-b",
        ChannelScheme::Brenn,
    )
    .await;

    // Drain: one activation with 2 input windows.
    let mut last_seen = HashMap::new();
    drain_step(&cfg, &wasm_sub, &mut last_seen).await;

    // Both input push rows must be acked.
    let remaining = messenger.load_pending_pushes(&wasm_sub).await;
    assert!(
        remaining.is_empty(),
        "all input push rows must be acked after drain"
    );

    // processor-dual publishes each new envelope to both out1 and out2.
    // 2 input windows × 1 new envelope each = 2 publishes to out1, 2 to out2.
    let out1_rows = messenger.load_pending_pushes(&out1_sub).await;
    let out2_rows = messenger.load_pending_pushes(&out2_sub).await;
    assert_eq!(
        out1_rows.len(),
        2,
        "out1 must have 2 pending pushes (one per input envelope)"
    );
    assert_eq!(
        out2_rows.len(),
        2,
        "out2 must have 2 pending pushes (one per input envelope)"
    );
}

/// §5 test 14 (config-residue reconciliation): insert a pending push row for
/// (a) a channel not in cfg.inputs (subscription removed) and (b) a channel
/// whose input has push_depth=Bounded(0) (push_depth lowered, old rows remain).
///
/// After drain: both rows are retired (delivered_at set), no guest was invoked,
/// no failure record created. The triggering input in cfg.inputs is empty so
/// the snapshot returns None and the drain produces no output.
#[tokio::test]
async fn multiport_config_residue_retired_no_activation() {
    let slug = "mp-residue";

    // Build a multiport setup with one triggering input (in0) and one sampled
    // input (in1, push_depth=0). We'll inject residue rows directly into the DB.
    let (messenger, in_entries, _out_entry, _out_sub, wasm_sub, cfg, _alert_handle, _store_db) =
        build_multiport_setup_with_depths(
            slug,
            &[
                ("mp-res-ch-trigger", Depth::Unbounded, Depth::Unbounded),
                ("mp-res-ch-sampled", Depth::Bounded(0), Depth::Unbounded),
            ],
        )
        .await;

    // Case (a): insert a push row for a channel that is NOT in cfg.inputs.
    // Build a ghost channel entry using upsert_channels (proper schema insert),
    // then insert a push row for the wasm subscriber on it.
    let ghost_entry =
        (*testutils::wasm_channel_entry(slug, "mp-res-ghost", Depth::Unbounded, Depth::Unbounded))
            .clone();
    {
        let conn = messenger.db().lock().await;
        brenn_lib::messaging::db::upsert_channels(&conn, std::slice::from_ref(&ghost_entry));
        brenn_lib::messaging::db::insert_message_with_pushes(
            &conn,
            ghost_entry.uuid,
            "test",
            "test-sender",
            "ghost-body",
            brenn_lib::messaging::Urgency::Normal,
            ChannelScheme::Brenn,
            None,
            None,
            None,
            brenn_lib::messaging::db::utc_to_ns(chrono::Utc::now()),
            &[brenn_lib::messaging::db::PendingPushInsert {
                target_subscriber: wasm_sub.clone(),
                target_app_slug: String::new(),
                eager_wake: true,
                release_after: None,
                delivery_deadline: None,
            }],
        );
    }

    // Case (b): insert a push row for the sampled channel (push_depth=Bounded(0)).
    // This simulates old rows from before the push_depth was lowered to 0.
    testutils::insert_wasm_push(
        &messenger,
        &in_entries[1],
        &wasm_sub,
        "residue-sampled-body",
        ChannelScheme::Brenn,
    )
    .await;

    // Verify we have 2 pending rows (ghost + sampled residue).
    let rows_before = messenger.load_pending_pushes(&wasm_sub).await;
    assert_eq!(
        rows_before.len(),
        2,
        "must have 2 pending residue rows before drain"
    );

    // Drain: both residue rows must be retired; no triggering input has valid rows
    // so the snapshot returns None → no activation → no output.
    let scan_before = messenger.pending_bus_pushes_scan_count();
    let mut last_seen = HashMap::new();
    drain_step(&cfg, &wasm_sub, &mut last_seen).await;
    let scan_after = messenger.pending_bus_pushes_scan_count();

    // Exactly one snapshot call must have occurred (test-4: confirms load_activation_snapshot
    // was called once and returned None, i.e. the guest was NOT invoked).
    assert_eq!(
        scan_after - scan_before,
        1,
        "exactly one load_activation_snapshot call per drain_step; delta={}",
        scan_after - scan_before,
    );

    // All residue rows must now be retired (delivered_at set).
    let rows_after = messenger.load_pending_pushes(&wasm_sub).await;
    assert!(
        rows_after.is_empty(),
        "both residue rows must be retired after drain; remaining={rows_after:?}"
    );

    // No failure records must exist for these rows — residue retirement is not a failure.
    let failure_count: i64 = {
        let conn = messenger.db().lock().await;
        conn.query_row(
            "SELECT COUNT(*) FROM messaging_wasm_consume_failures WHERE subscriber = ?1",
            rusqlite::params![wasm_sub.as_str()],
            |row| row.get(0),
        )
        .expect("query failure count")
    };
    assert_eq!(
        failure_count, 0,
        "residue retirement must not create failure records"
    );
}
