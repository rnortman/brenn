//! Sync calls across the two component contracts: a request and a reply are
//! bodies, and each end reads only what its contract carries.
//!
//! A callee is a real 0.1.0 fixture (`processor-multiport`) or a real 0.2.0
//! one (`processor-bytes-test`), and is judged against its own contract only:
//! the request names no caller. A 0.1.0 caller (`processor-call-test`) is the
//! end that cannot read a non-text reply.

use super::*;

use brenn_activation::sync::SyncRefusal;
use brenn_lib::messaging::MessageBody;
use brenn_wasm::ProcessorContract;

const BYTES_WASM: &str = "brenn-wasm/target/components/brenn_processor_bytes_test.wasm";
const CALL_TEST_WASM: &str = "brenn-wasm/target/components/brenn_processor_call_test.wasm";

/// Bytes that are not UTF-8 (a JPEG start-of-image marker).
const NOT_UTF8: &[u8] = &[0xff, 0xd8, 0xff, 0x00];

fn body(content_type: Option<&str>, bytes: &[u8]) -> MessageBody {
    MessageBody::from_parts(content_type, bytes).expect("a test body is well-formed")
}

/// Send one request into a config's own drain and read the answer.
async fn call(
    cfg: &WasmConsumerConfig,
    subscriber: &ParticipantId,
    body: MessageBody,
) -> SyncAnswer {
    let (reply, answer) = tokio::sync::oneshot::channel();
    drain_step_sync(
        cfg,
        subscriber,
        SyncRequest {
            port: "ask".to_string(),
            body,
            chain: vec![],
            reply,
        },
    )
    .await;
    answer.await.expect("the drain answers its caller")
}

/// The 0.2.0 bytes fixture as `slug`, its `report` port bound as `output_ports`
/// says. A sync request carrying a content type is echoed back as the reply;
/// an untyped one is the fixture's commands.
fn bytes_component(
    slug: &str,
    output_ports: std::collections::HashMap<String, brenn_wasm::OutputPortSpec>,
) -> Arc<ProcessorComponent> {
    let mut input_amplification_mt = test_amp_map();
    input_amplification_mt.extend(
        ["feed", "state", "timer"]
            .into_iter()
            .map(|p| (p.to_string(), 1_000)),
    );
    let component = Arc::new(ProcessorComponent::load(ProcessorLoadSpec {
        declared_out_ports: ["out", "report", "state", "timer"]
            .into_iter()
            .map(String::from)
            .collect(),
        output_ports,
        input_amplification_mt,
        grants: [ComponentGrant::Ports, ComponentGrant::Calls]
            .into_iter()
            .collect(),
        alerter: noop_proc_alerter(),
        declared_call_ports: ["peer".to_string()].into_iter().collect(),
        sync_caller: Some(Arc::new(|_, _, _, _| SyncAnswer::Ok(None))),
        ..ProcessorLoadSpec::minimal(std::path::Path::new(BYTES_WASM), slug)
    }));
    assert_eq!(component.contract(), ProcessorContract::V0_2);
    component
}

/// `cfg` serving the 0.2.0 bytes fixture on its `ask` port, with no bound
/// inputs or outputs.
fn bytes_callee(mut cfg: WasmConsumerConfig) -> WasmConsumerConfig {
    cfg.component = bytes_component(&cfg.slug, std::collections::HashMap::new());
    cfg.inputs = vec![];
    cfg.outputs = vec![];
    cfg.sync_ports = ["ask".to_string()].into_iter().collect();
    cfg
}

/// A 0.1.0 callee cannot read a non-text request, so it is refused before
/// anything advances: the callee does not run, its input is still owed, and
/// nothing reaches its output.
#[tokio::test]
async fn a_non_text_request_to_a_0_1_callee_is_refused_and_the_callee_does_not_run() {
    let slug = "sync-text-only-callee";
    let (messenger, in_entries, _out_entry, out_sub, wasm_sub, mut cfg, _alerts, _db) =
        build_multiport_setup(slug, &["sync-text-only-a"]).await;
    cfg.sync_ports = ["ask".to_string()].into_iter().collect();
    assert_eq!(cfg.component.contract(), ProcessorContract::V0_1);
    testutils::insert_bus_message(&messenger, &in_entries[0], "queued", ChannelScheme::Brenn).await;

    let answer = call(&cfg, &wasm_sub, body(Some("image/png"), NOT_UTF8)).await;

    assert_eq!(answer, SyncAnswer::Refused(SyncRefusal::TextOnlyTarget));
    assert_eq!(
        testutils::owed_everywhere(&messenger, &wasm_sub)
            .await
            .len(),
        1,
        "the callee's input was not advanced over"
    );
    assert!(
        testutils::owed_everywhere(&messenger, &out_sub)
            .await
            .is_empty(),
        "the callee published nothing"
    );
}

/// Text is text whatever its type: a typed text request from a 0.2.0 caller
/// reaches a 0.1.0 callee as its text, and its text reply comes back.
#[tokio::test]
async fn a_typed_text_request_from_a_0_2_caller_reaches_a_0_1_callee_as_its_text() {
    let slug = "sync-typed-text-to-0-1";
    let (_messenger, _in, _out_entry, _out_sub, wasm_sub, mut cfg, _alerts, _db) =
        build_multiport_setup(slug, &["sync-typed-text-a"]).await;
    cfg.sync_ports = ["ask".to_string()].into_iter().collect();

    let answer = call(&cfg, &wasm_sub, body(Some("text/markdown"), b"__reply__")).await;

    let SyncAnswer::Ok(Some(reply)) = answer else {
        panic!("the 0.1.0 callee read its text request and answered, got {answer:?}");
    };
    assert_eq!(reply.content_type(), None, "a 0.1.0 reply is untyped text");
    assert!(
        reply
            .as_text()
            .is_some_and(|t| t.contains("\"port\":\"ask\"")),
        "the reply is the callee's summary: {reply:?}"
    );
}

/// Between two 0.2.0 ends a request and a reply cross as bytes with their
/// content type.
#[tokio::test]
async fn a_non_text_request_and_reply_cross_between_0_2_ends_byte_exact() {
    let slug = "sync-bytes-both-ends";
    let (_messenger, _in, _out_entry, _out_sub, wasm_sub, cfg, _alerts, _db) =
        build_multiport_setup(slug, &["sync-bytes-a"]).await;
    let cfg = bytes_callee(cfg);

    let request = body(Some("image/png"), NOT_UTF8);
    let answer = call(&cfg, &wasm_sub, request.clone()).await;

    assert_eq!(
        answer,
        SyncAnswer::Ok(Some(request)),
        "the callee was handed the bytes and echoed them"
    );
}

/// The callee's reply is judged against its own contract, whoever asks. A
/// 0.2.0 callee holding a new message on its own input when a call lands
/// answers with a non-text body and ends ok: the reply comes back byte-exact,
/// its report for that input is published, and nothing is recorded as failed
/// or alerted.
#[tokio::test]
async fn a_non_text_reply_ends_the_callee_ok_with_its_inputs_served() {
    let slug = "sync-bytes-reply-with-input";
    let (messenger, in_entries, out_entry, out_sub, wasm_sub, mut cfg, alert_handle, _db) =
        build_multiport_setup(slug, &["sync-bytes-reply-a"]).await;
    let mut output_ports = std::collections::HashMap::new();
    output_ports.insert(
        "report".to_string(),
        test_out_spec(out_entry.address.clone()),
    );
    cfg.component = bytes_component(slug, output_ports);
    cfg.sync_ports = ["ask".to_string()].into_iter().collect();
    let (alert_dispatcher, captured_alerts, cap_handle) = make_capturing_alerter_with_severity();
    cfg.alert_dispatcher = alert_dispatcher;
    testutils::insert_bus_message(&messenger, &in_entries[0], "waiting", ChannelScheme::Brenn)
        .await;

    let answer = call(
        &cfg,
        &wasm_sub,
        MessageBody::text("reply image/png ffd8ff00"),
    )
    .await;

    assert_eq!(
        answer,
        SyncAnswer::Ok(Some(body(Some("image/png"), NOT_UTF8))),
        "the non-text reply is the callee's answer"
    );
    assert!(
        testutils::owed_everywhere(&messenger, &wasm_sub)
            .await
            .is_empty(),
        "the callee's input was served"
    );
    let published = testutils::owed_everywhere(&messenger, &out_sub).await;
    assert_eq!(
        published.len(),
        1,
        "the callee's report flushed: {published:?}"
    );
    let report = published[0]
        .1
        .body
        .as_text()
        .expect("the report is JSON text")
        .to_string();
    assert!(
        report.contains(&format!("\"hex\":\"{}\"", hex(b"waiting"))),
        "the report names the waiting input: {report}"
    );
    assert!(
        report.contains("\"sync\":\"ask\""),
        "the report is the sync activation's: {report}"
    );
    let failures: i64 = {
        let conn = messenger.db().lock().await;
        conn.query_row(
            "SELECT COUNT(*) FROM messaging_wasm_consume_failures WHERE subscriber = ?1",
            rusqlite::params![wasm_sub.as_str()],
            |row| row.get(0),
        )
        .expect("count failure rows")
    };
    assert_eq!(failures, 0, "no activation failure is recorded");

    drop(cfg);
    let _ = cap_handle.await;
    let _ = alert_handle.await;
    assert!(
        captured_alerts.lock().unwrap().is_empty(),
        "no alert is raised"
    );
}

/// The 0.1.0 caller is the end that cannot read a non-text reply, so its own
/// activation traps: one alert naming it, its delivered input recorded as
/// failed by a trap, and nothing it buffered published.
#[tokio::test]
async fn a_non_text_reply_traps_the_0_1_caller() {
    let slug = "sync-bytes-0-1-caller";
    let (messenger, in_entries, out_entry, out_sub, wasm_sub, mut cfg, alert_handle, _db) =
        build_multiport_setup(slug, &["sync-bytes-0-1-caller-a"]).await;
    let mut output_ports = std::collections::HashMap::new();
    output_ports.insert("out".to_string(), test_out_spec(out_entry.address.clone()));
    let mut calls = std::collections::HashMap::new();
    calls.insert(
        "ask".to_string(),
        brenn_wasm::ProcessorCallTarget {
            target_slug: "thumbnailer".to_string(),
            target_port: "shrink".to_string(),
        },
    );
    let reply = body(Some("image/png"), NOT_UTF8);
    cfg.component = Arc::new(ProcessorComponent::load(ProcessorLoadSpec {
        declared_out_ports: output_ports.keys().cloned().collect(),
        output_ports,
        input_amplification_mt: test_amp_map(),
        grants: [ComponentGrant::Ports, ComponentGrant::Calls]
            .into_iter()
            .collect(),
        alerter: noop_proc_alerter(),
        declared_call_ports: ["ask".to_string()].into_iter().collect(),
        calls,
        sync_caller: Some(Arc::new(move |_, _, _, _| {
            SyncAnswer::Ok(Some(reply.clone()))
        })),
        ..ProcessorLoadSpec::minimal(std::path::Path::new(CALL_TEST_WASM), slug)
    }));
    assert_eq!(cfg.component.contract(), ProcessorContract::V0_1);
    let (alert_dispatcher, captured_alerts, cap_handle) = make_capturing_alerter_with_severity();
    cfg.alert_dispatcher = alert_dispatcher;
    testutils::insert_bus_message(&messenger, &in_entries[0], "call:x", ChannelScheme::Brenn).await;

    drain_step(&cfg, &wasm_sub, MountDebt::Settled).await;

    assert!(
        testutils::owed_everywhere(&messenger, &out_sub)
            .await
            .is_empty(),
        "the trapped caller published nothing"
    );
    let outcomes: Vec<String> = {
        let conn = messenger.db().lock().await;
        let mut stmt = conn
            .prepare(
                "SELECT outcome FROM messaging_wasm_consume_failures \
                 WHERE subscriber = ?1 AND channel = ?2",
            )
            .expect("prepare failure query");
        stmt.query_map(
            rusqlite::params![wasm_sub.as_str(), in_entries[0].address.as_str()],
            |row| row.get(0),
        )
        .expect("query failure rows")
        .collect::<Result<_, _>>()
        .expect("read failure rows")
    };
    assert_eq!(outcomes, ["trap"], "the delivered message failed by a trap");

    drop(cfg);
    let _ = cap_handle.await;
    let _ = alert_handle.await;
    let alerts = captured_alerts.lock().unwrap();
    assert_eq!(alerts.len(), 1, "exactly one alert: {alerts:?}");
    let (_, title, detail) = &alerts[0];
    assert!(
        title.contains(slug) && title.contains("trapped"),
        "the alert names the caller: {title}"
    );
    assert!(
        detail.contains("brenn:processor@0.1.0") && detail.contains("thumbnailer"),
        "the trap says why and names the peer: {detail}"
    );
}

/// Lower-case hex, as the bytes fixture reports bodies.
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
