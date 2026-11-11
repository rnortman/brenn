//! Bodies that are not text, end to end through the dispatcher: 0.2.0
//! consumers (`processor-bytes-test`) on channels declared `non_text`, beside a
//! 0.1.0 one (`processor-dual`) where the channel is text-only.
//!
//! Each consumer is driven by text commands on its `in` channel and reports
//! what it was handed as JSON on `report`, which a reader subscribed to every
//! report channel reads back.

use super::*;

use brenn_lib::messaging::MessageBody;
use brenn_lib::messaging::config::{WasmOutputPort, WasmSinkBudget};
use brenn_wasm::ProcessorContract;

const BYTES_WASM: &str = "brenn-wasm/target/components/brenn_processor_bytes_test.wasm";
const DUAL_WASM: &str = "brenn-wasm/target/components/brenn_processor_dual.wasm";

/// The participant subscribed to every report channel.
const READER: &str = "nt-report-reader";

/// Bytes that are not UTF-8 (a JPEG start-of-image marker) and their hex.
const NOT_UTF8: &[u8] = &[0xff, 0xd8, 0xff, 0x00];
const NOT_UTF8_HEX: &str = "ffd8ff00";

fn body(content_type: Option<&str>, bytes: &[u8]) -> MessageBody {
    MessageBody::from_parts(content_type, bytes).expect("a test body is well-formed")
}

/// A `brenn:` channel at `address` whose WASM subscribers are `subscribers`,
/// each with its push and retain depth.
fn durable(address: &str, non_text: bool, subscribers: &[(&str, Depth, Depth)]) -> ChannelEntry {
    let mut entry = brenn_channel(address, READER);
    entry.resolved_channel.non_text = non_text;
    entry.subscribers = subscribers
        .iter()
        .map(|(slug, push_depth, retain_depth)| SubscriberEntry {
            kind: SubscriberEntryKind::Wasm(slug.to_string()),
            push_depth: *push_depth,
            retain_depth: *retain_depth,
            noise: NoiseLevel::Silent,
            wake_min: None,
        })
        .collect();
    entry
}

/// A report channel read by [`READER`].
fn report_channel(address: &str) -> ChannelEntry {
    durable(
        address,
        false,
        &[(READER, Depth::Unbounded, Depth::Unbounded)],
    )
}

/// A messenger over `entries`, every push-enabled subscriber attached at head.
async fn messenger_over(entries: &[ChannelEntry]) -> Arc<brenn_messaging::Messenger> {
    let db = init_db_memory_lib_slice();
    {
        let conn = db.lock().await;
        upsert_channels(&conn, entries);
    }
    let rings: Vec<ChannelEntry> = entries
        .iter()
        .filter(|e| !e.capabilities().durable)
        .cloned()
        .collect();
    let messenger = brenn_messaging::Messenger::new(
        db,
        Arc::new(MessagingDirectory::with_entries(entries.to_vec())),
        Arc::from("test"),
        Arc::new(IndexMap::new()),
        Arc::new(NoopWakeRouter) as Arc<dyn WakeRouter>,
        MessagingGlobalConfig::default(),
    )
    .with_ring_stores(Arc::new(brenn_messaging_store::store::RingStores::build(
        &rings,
    )))
    .with_subscriber_registrations(testutils::wasm_registrations(wasm_policies_from_entries(
        entries,
    )));
    for entry in entries {
        for sub in &entry.subscribers {
            let SubscriberEntryKind::Wasm(slug) = &sub.kind else {
                continue;
            };
            testutils::attach_wasm_port(
                &messenger,
                entry,
                slug,
                &ParticipantId::for_wasm(slug),
                sub.push_depth,
            )
            .await;
        }
    }
    messenger
}

/// `slug`'s input port reading `entry`, at the depths `entry` registers for it.
fn input(slug: &str, port: &str, entry: &ChannelEntry) -> WasmInputPort {
    let sub = entry
        .subscribers
        .iter()
        .find(|s| s.kind == SubscriberEntryKind::Wasm(slug.to_string()))
        .unwrap_or_else(|| panic!("{} registers no subscriber {slug}", entry.address));
    WasmInputPort {
        port: port.to_string(),
        sub: ResolvedSubscription {
            channel_uuid: entry.uuid,
            channel_address: entry.address.clone(),
            push_depth: resolve_wasm_window_depth(sub.push_depth, "push_depth", slug),
            retain_depth: resolve_wasm_window_depth(sub.retain_depth, "retain_depth", slug),
            noise: NoiseLevel::Silent,
            wake_min: WakeMin::Normal,
        },
        amplification_mt: 1000,
        non_text: entry.resolved_channel.non_text,
    }
}

/// An output port bound to `entry`, as the consumer config carries it.
fn output(port: &str, entry: &ChannelEntry) -> WasmOutputPort {
    WasmOutputPort {
        port: port.to_string(),
        channel_uuid: entry.uuid,
        channel_address: entry.address.clone(),
        default_urgency: brenn_lib::messaging::Urgency::Normal,
        budget: WasmSinkBudget {
            fill_mt: 1_000_000_000,
            capacity_mt: 1_000_000_000,
        },
        non_text: entry.resolved_channel.non_text,
    }
}

/// The consumer config for `component` with these ports.
fn consumer(
    slug: &str,
    component: Arc<ProcessorComponent>,
    messenger: &Arc<brenn_messaging::Messenger>,
    inputs: Vec<WasmInputPort>,
    outputs: Vec<WasmOutputPort>,
) -> WasmConsumerConfig {
    WasmConsumerConfig {
        slug: slug.to_string(),
        component,
        notify: Arc::new(Notify::new()),
        messenger: Arc::clone(messenger),
        alert_dispatcher: noop_alert_dispatcher().0,
        inputs,
        outputs,
        sync_ports: std::collections::BTreeSet::new(),
        activation_pacing: unthrottled_pacing(),
        provider: None,
    }
}

/// The output-port specs the component is loaded with, one per bound output.
fn output_specs(
    outputs: &[WasmOutputPort],
) -> std::collections::HashMap<String, brenn_wasm::OutputPortSpec> {
    outputs
        .iter()
        .map(|o| {
            (
                o.port.clone(),
                brenn_wasm::OutputPortSpec {
                    non_text: o.non_text,
                    ..test_out_spec(o.channel_address.clone())
                },
            )
        })
        .collect()
}

/// A consumer running the 0.2.0 bytes fixture.
fn bytes_consumer(
    slug: &str,
    messenger: &Arc<brenn_messaging::Messenger>,
    inputs: Vec<WasmInputPort>,
    outputs: Vec<WasmOutputPort>,
) -> WasmConsumerConfig {
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
        output_ports: output_specs(&outputs),
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
    consumer(slug, component, messenger, inputs, outputs)
}

/// One activation of `cfg`, over whatever it is owed.
async fn activate(cfg: &WasmConsumerConfig) {
    drain_step(cfg, &ParticipantId::for_wasm(&cfg.slug), MountDebt::Settled).await;
}

/// Hand `entry`'s subscribers one command message.
async fn command(messenger: &brenn_messaging::Messenger, entry: &ChannelEntry, text: &str) {
    testutils::insert_bus_message(messenger, entry, text, ChannelScheme::Brenn).await;
}

/// Every report published on `address` since the last read, parsed; the
/// reader is then advanced past them.
async fn reports(messenger: &brenn_messaging::Messenger, address: &str) -> Vec<serde_json::Value> {
    let reader = ParticipantId::for_wasm(READER);
    let reports = testutils::owed_everywhere(messenger, &reader)
        .await
        .into_iter()
        .filter(|(channel, _)| channel == address)
        .map(|(_, envelope)| {
            assert_eq!(envelope.body.content_type(), Some("application/json"));
            serde_json::from_str(envelope.body.as_text().expect("a report is text"))
                .expect("a report is JSON")
        })
        .collect();
    testutils::consume_owed(messenger, address, &reader).await;
    reports
}

/// The one report published on `address` since the last read.
async fn one_report(messenger: &brenn_messaging::Messenger, address: &str) -> serde_json::Value {
    let mut all = reports(messenger, address).await;
    assert_eq!(all.len(), 1, "one activation, one report: {all:?}");
    all.remove(0)
}

/// The envelopes a report shows on `port`'s window.
fn window<'a>(report: &'a serde_json::Value, port: &str) -> &'a Vec<serde_json::Value> {
    report["windows"]
        .as_array()
        .expect("a report lists its windows")
        .iter()
        .find(|w| w["port"] == port)
        .unwrap_or_else(|| panic!("no {port} window in {report}"))["envelopes"]
        .as_array()
        .expect("a window lists its envelopes")
}

/// The entries a report shows in `port`'s deferred view.
fn deferred<'a>(report: &'a serde_json::Value, port: &str) -> &'a Vec<serde_json::Value> {
    report["deferred"]
        .as_array()
        .expect("a report lists its deferred view")
        .iter()
        .find(|w| w["port"] == port)
        .unwrap_or_else(|| panic!("no {port} deferred window in {report}"))["entries"]
        .as_array()
        .expect("a deferred window lists its entries")
}

fn results(report: &serde_json::Value) -> Vec<&str> {
    report["results"]
        .as_array()
        .expect("a report lists its results")
        .iter()
        .map(|r| r.as_str().expect("a result is a word"))
        .collect()
}

/// Two 0.2.0 consumers share a `non_text` channel: what one publishes, the
/// other is handed with its content type and every byte, and the store holds
/// exactly that body between them.
#[tokio::test]
async fn two_0_2_consumers_carry_a_non_text_body_byte_exact() {
    let unbounded = (Depth::Unbounded, Depth::Unbounded);
    let cmd = durable(
        "brenn:nt-camera-cmd",
        false,
        &[("nt-camera", unbounded.0, unbounded.1)],
    );
    let frames = durable(
        "brenn:nt-frames",
        true,
        &[("nt-viewer", unbounded.0, unbounded.1)],
    );
    let camera_report = report_channel("brenn:nt-camera-report");
    let viewer_report = report_channel("brenn:nt-viewer-report");
    let messenger = messenger_over(&[
        cmd.clone(),
        frames.clone(),
        camera_report.clone(),
        viewer_report.clone(),
    ])
    .await;
    let camera = bytes_consumer(
        "nt-camera",
        &messenger,
        vec![input("nt-camera", "in", &cmd)],
        vec![output("out", &frames), output("report", &camera_report)],
    );
    let viewer = bytes_consumer(
        "nt-viewer",
        &messenger,
        vec![input("nt-viewer", "feed", &frames)],
        vec![output("report", &viewer_report)],
    );

    command(
        &messenger,
        &cmd,
        &format!("publish out image/jpeg {NOT_UTF8_HEX}"),
    )
    .await;
    activate(&camera).await;

    assert_eq!(
        results(&one_report(&messenger, &camera_report.address).await),
        ["ok"]
    );
    let owed = testutils::owed_everywhere(&messenger, &ParticipantId::for_wasm("nt-viewer")).await;
    assert_eq!(owed.len(), 1, "the frame is on the channel: {owed:?}");
    assert_eq!(owed[0].0, frames.address);
    assert_eq!(owed[0].1.body, body(Some("image/jpeg"), NOT_UTF8));

    activate(&viewer).await;

    let report = one_report(&messenger, &viewer_report.address).await;
    let feed = window(&report, "feed");
    assert_eq!(feed.len(), 1, "{report}");
    assert_eq!(feed[0]["content_type"], "image/jpeg");
    assert_eq!(feed[0]["hex"], NOT_UTF8_HEX);
    assert_eq!(feed[0]["header"]["channel"], frames.address.as_str());
    assert_eq!(
        feed[0]["header"]["sender"],
        ParticipantId::for_wasm("nt-camera").as_str()
    );
    assert!(
        feed[0]["header"].get("body").is_none(),
        "the header carries no body: {report}"
    );
}

/// A component keeps binary state on a free `io` port
/// declared `non_text`, sampled with a window of one, and reads back the last
/// body it wrote, byte-exact, at its next activation.
#[tokio::test]
async fn a_component_reads_its_own_non_text_state_back_on_a_free_io() {
    let slug = "nt-keeper";
    let cmd = durable(
        "brenn:nt-keeper-cmd",
        false,
        &[(slug, Depth::Unbounded, Depth::Unbounded)],
    );
    let mut state = testutils::declared_non_text(testutils::local_channel_entry("nt-state", 1));
    state.subscribers = vec![SubscriberEntry {
        kind: SubscriberEntryKind::Wasm(slug.to_string()),
        push_depth: Depth::Bounded(0),
        retain_depth: Depth::Bounded(1),
        noise: NoiseLevel::Silent,
        wake_min: None,
    }];
    let report = report_channel("brenn:nt-keeper-report");
    let messenger = messenger_over(&[cmd.clone(), state.clone(), report.clone()]).await;
    let keeper = bytes_consumer(
        slug,
        &messenger,
        vec![input(slug, "in", &cmd), input(slug, "state", &state)],
        vec![output("state", &state), output("report", &report)],
    );

    command(
        &messenger,
        &cmd,
        "publish state application/octet-stream 00ff01",
    )
    .await;
    activate(&keeper).await;
    let first = one_report(&messenger, &report.address).await;
    assert_eq!(results(&first), ["ok"]);
    assert!(window(&first, "state").is_empty(), "no state yet: {first}");

    command(&messenger, &cmd, "publish state image/png 89504e47").await;
    activate(&keeper).await;
    let second = one_report(&messenger, &report.address).await;
    assert_eq!(results(&second), ["ok"]);
    let held = window(&second, "state");
    assert_eq!(held.len(), 1, "{second}");
    assert_eq!(held[0]["content_type"], "application/octet-stream");
    assert_eq!(held[0]["hex"], "00ff01");

    command(&messenger, &cmd, "").await;
    activate(&keeper).await;
    let third = one_report(&messenger, &report.address).await;
    let held = window(&third, "state");
    assert_eq!(held.len(), 1, "a window of one holds the latest: {third}");
    assert_eq!(held[0]["content_type"], "image/png");
    assert_eq!(held[0]["hex"], "89504e47");
}

/// A non-text body parked on a `non_text` channel comes back byte-exact in
/// the publisher's deferred view, and so does the body an edit replaces it
/// with; the release time stays where it was put.
#[tokio::test]
async fn a_parked_non_text_body_and_its_edit_come_back_byte_exact_in_the_deferred_view() {
    let slug = "nt-scheduler";
    let cmd = durable(
        "brenn:nt-scheduler-cmd",
        false,
        &[(slug, Depth::Unbounded, Depth::Unbounded)],
    );
    let timer = durable("brenn:nt-timer", true, &[]);
    let report = report_channel("brenn:nt-scheduler-report");
    let messenger = messenger_over(&[cmd.clone(), timer.clone(), report.clone()]).await;
    let scheduler = bytes_consumer(
        slug,
        &messenger,
        vec![input(slug, "in", &cmd)],
        vec![output("timer", &timer), output("report", &report)],
    );
    // Whole seconds: the durable store keeps a release time to the second.
    let at = u64::try_from((chrono::Utc::now() + chrono::Duration::hours(1)).timestamp())
        .expect("now is after the epoch")
        * 1000;

    command(
        &messenger,
        &cmd,
        &format!("defer timer image/png 89504e47 {at}"),
    )
    .await;
    activate(&scheduler).await;
    let parked = one_report(&messenger, &report.address).await;
    assert_eq!(results(&parked), ["ok"]);
    assert!(
        deferred(&parked, "timer").is_empty(),
        "the view predates the park"
    );

    command(
        &messenger,
        &cmd,
        "edit timer 0 application/octet-stream ff00fe",
    )
    .await;
    activate(&scheduler).await;
    let edited = one_report(&messenger, &report.address).await;
    assert_eq!(results(&edited), ["ok"]);
    let view = deferred(&edited, "timer");
    assert_eq!(view.len(), 1, "{edited}");
    assert_eq!(view[0]["index"], 0);
    assert_eq!(view[0]["content_type"], "image/png");
    assert_eq!(view[0]["hex"], "89504e47");
    assert_eq!(view[0]["deliver_after"], at);

    command(&messenger, &cmd, "").await;
    activate(&scheduler).await;
    let after = one_report(&messenger, &report.address).await;
    let view = deferred(&after, "timer");
    assert_eq!(view.len(), 1, "{after}");
    assert_eq!(view[0]["content_type"], "application/octet-stream");
    assert_eq!(view[0]["hex"], "ff00fe");
    assert_eq!(view[0]["deliver_after"], at);

    let stored = messenger
        .deferred_view_for_sender(&timer.address, ParticipantId::for_wasm(slug).as_str())
        .await;
    assert_eq!(stored.len(), 1);
    assert_eq!(
        stored[0].envelope.body,
        body(Some("application/octet-stream"), &[0xff, 0x00, 0xfe])
    );
}

/// On a text-only channel a 0.2.0 consumer is handed a `text/markdown` body
/// with its type, and a 0.1.0 consumer beside it the same text with none.
#[tokio::test]
async fn a_typed_text_body_reaches_a_0_2_reader_typed_and_a_0_1_reader_as_its_text() {
    let unbounded = (Depth::Unbounded, Depth::Unbounded);
    let cmd = durable(
        "brenn:nt-author-cmd",
        false,
        &[("nt-author", unbounded.0, unbounded.1)],
    );
    let notes = durable(
        "brenn:nt-notes",
        false,
        &[
            ("nt-reader-new", unbounded.0, unbounded.1),
            ("nt-reader-old", unbounded.0, unbounded.1),
        ],
    );
    let author_report = report_channel("brenn:nt-author-report");
    let new_report = report_channel("brenn:nt-reader-new-report");
    let old_out = report_channel("brenn:nt-reader-old-out");
    let messenger = messenger_over(&[
        cmd.clone(),
        notes.clone(),
        author_report.clone(),
        new_report.clone(),
        old_out.clone(),
    ])
    .await;
    let author = bytes_consumer(
        "nt-author",
        &messenger,
        vec![input("nt-author", "in", &cmd)],
        vec![output("out", &notes), output("report", &author_report)],
    );
    let new_reader = bytes_consumer(
        "nt-reader-new",
        &messenger,
        vec![input("nt-reader-new", "feed", &notes)],
        vec![output("report", &new_report)],
    );
    let old_outputs = vec![output("out1", &old_out), output("out2", &old_out)];
    let old_component = Arc::new(ProcessorComponent::load(ProcessorLoadSpec {
        declared_out_ports: ["out1", "out2"].into_iter().map(String::from).collect(),
        output_ports: output_specs(&old_outputs),
        input_amplification_mt: test_amp_map(),
        grants: [ComponentGrant::Ports].into_iter().collect(),
        alerter: noop_proc_alerter(),
        ..ProcessorLoadSpec::minimal(std::path::Path::new(DUAL_WASM), "nt-reader-old")
    }));
    assert_eq!(old_component.contract(), ProcessorContract::V0_1);
    let old_reader = consumer(
        "nt-reader-old",
        old_component,
        &messenger,
        vec![input("nt-reader-old", "in", &notes)],
        old_outputs,
    );

    // `# hi`
    command(&messenger, &cmd, "publish out text/markdown 23206869").await;
    activate(&author).await;
    assert_eq!(
        results(&one_report(&messenger, &author_report.address).await),
        ["ok"]
    );

    activate(&new_reader).await;
    let report = one_report(&messenger, &new_report.address).await;
    let feed = window(&report, "feed");
    assert_eq!(feed.len(), 1, "{report}");
    assert_eq!(feed[0]["content_type"], "text/markdown");
    assert_eq!(feed[0]["hex"], "23206869");

    activate(&old_reader).await;
    // The dual fixture republishes each envelope-json it was handed, once per
    // output port; both ports are bound to one channel.
    let reader = ParticipantId::for_wasm(READER);
    let echoed: Vec<serde_json::Value> = testutils::owed_everywhere(&messenger, &reader)
        .await
        .into_iter()
        .filter(|(channel, _)| *channel == old_out.address)
        .map(|(_, envelope)| {
            serde_json::from_str(envelope.body.as_text().expect("the echo is text"))
                .expect("the echo is the envelope-json the guest was handed")
        })
        .collect();
    assert_eq!(echoed.len(), 2, "{echoed:?}");
    for envelope in &echoed {
        assert_eq!(envelope["body"], "# hi");
        assert_eq!(envelope["channel"], notes.address.as_str());
        assert!(
            envelope.get("content_type").is_none(),
            "the 0.1.0 envelope carries no type: {envelope}"
        );
    }
}

/// A non-text publish to a text-only channel is refused at the door: the
/// guest is told `text-only-channel`, its activation still ends ok, and the
/// channel holds nothing.
#[tokio::test]
async fn a_refused_non_text_publish_leaves_the_channel_empty() {
    let slug = "nt-misdeclared";
    let cmd = durable(
        "brenn:nt-misdeclared-cmd",
        false,
        &[(slug, Depth::Unbounded, Depth::Unbounded)],
    );
    let notes = durable(
        "brenn:nt-plain",
        false,
        &[(READER, Depth::Unbounded, Depth::Unbounded)],
    );
    let report = report_channel("brenn:nt-misdeclared-report");
    let messenger = messenger_over(&[cmd.clone(), notes.clone(), report.clone()]).await;
    let publisher = bytes_consumer(
        slug,
        &messenger,
        vec![input(slug, "in", &cmd)],
        vec![output("out", &notes), output("report", &report)],
    );

    command(
        &messenger,
        &cmd,
        &format!("publish out image/jpeg {NOT_UTF8_HEX}"),
    )
    .await;
    activate(&publisher).await;

    assert_eq!(
        results(&one_report(&messenger, &report.address).await),
        ["text-only-channel"]
    );
    assert!(
        reports(&messenger, &notes.address).await.is_empty(),
        "nothing is owed on the text-only channel"
    );
    let held = messenger
        .store_for_address(&notes.address)
        .replay_from(None, Depth::Bounded(512))
        .await
        .messages;
    assert!(held.is_empty(), "the channel holds nothing: {held:?}");
}
