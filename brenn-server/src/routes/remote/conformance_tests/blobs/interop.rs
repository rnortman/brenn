//! The remote door and the component door on one messenger.
//!
//! A blob a remote uploads is opened and read by a component through
//! `blobs.open`, and a blob the component makes is opened and read by the
//! remote. The component is `processor-blobs-test`, stepped with `drain_step`:
//! each message on its script channel is a script, and its report lists what each `blobs` call returned.

use brenn_messaging::MountDebt;
use brenn_wasm_dispatch::tests as dispatch;
use dispatch::{
    ComponentGrant, NoiseLevel, ParticipantId, ProcessorComponent, ProcessorLoadSpec,
    WasmConsumerConfig, fnv1a64,
};
use tokio::sync::Notify;

use super::*;
use crate::messaging_router::DeliveryBinding;

/// The component instance.
const FIXTURE: &str = "rb-blobs";

/// The channel the component reads its scripts from.
const SCRIPT: &str = "brenn:rb-blobs-in";

/// The channel the component reports on. Nothing subscribes; the test reads
/// the latest row.
const REPORT: &str = "brenn:rb-blobs-report";

const BLOBS_WASM: &str = "brenn-wasm/target/components/brenn_processor_blobs_test.wasm";

/// Over one remote chunk, and inside one component read and one write.
const SIZE: usize = 300_000;

/// The script, report and stream channels, with the component subscribed to
/// its script channel and to `stream.7`.
fn interop_channels() -> Vec<ChannelEntry> {
    let mut channels = blob_channels();
    let stream = channels
        .iter_mut()
        .find(|entry| entry.address == STREAM_7)
        .expect("the fleet carries stream.7");
    stream.subscribers.push(SubscriberEntry {
        kind: SubscriberEntryKind::Wasm(FIXTURE.to_string()),
        push_depth: Depth::Unbounded,
        retain_depth: Depth::Unbounded,
        noise: NoiseLevel::Silent,
        wake_min: None,
    });
    let mut report = dispatch::brenn_channel(REPORT, FIXTURE);
    report.subscribers.clear();
    channels.extend([dispatch::brenn_channel(SCRIPT, FIXTURE), report]);
    channels
}

fn entry<'a>(channels: &'a [ChannelEntry], address: &str) -> &'a ChannelEntry {
    channels
        .iter()
        .find(|entry| entry.address == address)
        .unwrap_or_else(|| panic!("no channel {address}"))
}

/// The component, granted `blobs` over the messenger's store: scripts on
/// `in`, `stream.7` on `fa`, publishing on `a` (`stream.7`) and `report`.
fn fixture(messenger: &Arc<Messenger>, channels: &[ChannelEntry]) -> WasmConsumerConfig {
    let inputs = vec![
        dispatch::input(FIXTURE, "in", entry(channels, SCRIPT)),
        dispatch::input(FIXTURE, "fa", entry(channels, STREAM_7)),
    ];
    let outputs = vec![
        dispatch::output("report", entry(channels, REPORT)),
        dispatch::output("a", entry(channels, STREAM_7)),
    ];
    let mut input_amplification_mt = dispatch::test_amp_map();
    input_amplification_mt.insert("fa".to_string(), 1000);
    let component = Arc::new(ProcessorComponent::load(ProcessorLoadSpec {
        declared_out_ports: ["report", "a"].into_iter().map(String::from).collect(),
        output_ports: dispatch::output_specs(&outputs),
        input_amplification_mt,
        grants: [
            ComponentGrant::Ports,
            ComponentGrant::Log,
            ComponentGrant::Blobs,
        ]
        .into_iter()
        .collect(),
        alerter: dispatch::noop_proc_alerter(),
        blob_host: Some(
            brenn_wasm_dispatch::blob_host_over(messenger).expect("the messenger has a store"),
        ),
        ..ProcessorLoadSpec::minimal(std::path::Path::new(BLOBS_WASM), FIXTURE)
    }));
    dispatch::consumer(FIXTURE, component, messenger, inputs, outputs)
}

/// Hand the component `script` and step it once; the report's result words.
async fn run(b: &BlobRig, cfg: &WasmConsumerConfig, script: &str) -> Vec<String> {
    publish_attached(&b.messenger, HOST, SCRIPT, script, &[], None).await;
    brenn_wasm_dispatch::drain_step(cfg, &ParticipantId::for_wasm(FIXTURE), MountDebt::Settled)
        .await;
    let report = dispatch::read_latest(&b.messenger, REPORT)
        .await
        .expect("the component reported");
    report["results"]
        .as_array()
        .expect("a report lists its results")
        .iter()
        .map(|r| r.as_str().expect("a result is a word").to_string())
        .collect()
}

/// **The two doors interoperate.** A remote's upload is read by a component,
/// and a component's blob by a remote, byte for byte, with no security event.
#[tokio::test]
async fn a_remote_and_a_component_each_read_the_blob_the_other_made() {
    let db = crate::test_support::init_db_memory();
    let channels = interop_channels();
    let registrations =
        dispatch::testutils::wasm_registrations(dispatch::wasm_policies_from_entries(&channels));
    let mut b = blob_rig_over(
        &db,
        loopback,
        VersionRange::exactly(6),
        TEST_BLOB_LIMITS,
        channels.clone(),
        move |m, router| {
            router.register_delivery_binding(
                SubscriberEntryKind::Wasm(FIXTURE.to_string()),
                DeliveryBinding::ParkedNotify(Arc::new(Notify::new())),
            );
            m.with_subscriber_registrations(registrations)
        },
    )
    .await;
    let me = ParticipantId::for_wasm(FIXTURE);
    for address in [SCRIPT, STREAM_7] {
        dispatch::testutils::attach_wasm_port(
            &b.messenger,
            entry(&channels, address),
            FIXTURE,
            &me,
            Depth::Unbounded,
        )
        .await;
    }
    let cfg = fixture(&b.messenger, &channels);

    let mut reader = b.rig.sibling(VersionRange::exactly(6));
    b.rig.client.attach().await;
    reader.attach().await;
    reader
        .subscribe(STREAM_7, DEPTHS, ResumePolicy::Cursorless)
        .await;

    // Remote to component.
    let data = bytes_of(SIZE, 7);
    let handle = b
        .rig
        .client
        .upload("image/jpeg", &data)
        .await
        .expect("the upload seals");
    assert_eq!(
        b.rig
            .client
            .publish(attaching(STREAM_7, "frame", handle))
            .await,
        PublishOutcome::Ok
    );
    b.rig.client.close_blob(handle).await;
    let uploaded = reader.next_delivery(STREAM_7).await;
    assert!(uploaded.attachments.get(&name("frame")).is_some());

    let all = format!("all:{SIZE}:{:016x}", fnv1a64(&data));
    assert_eq!(
        run(&b, &cfg, "list fa:0\nopen fa:0 frame\nread-all 0").await,
        [
            format!("frame/{SIZE}/image/jpeg"),
            "blob:0".to_string(),
            all.clone(),
        ],
        "the component opens the remote's attachment and reads every byte"
    );

    // Component to remote. The fixture's `fill` pattern is `bytes_of`'s at
    // seed 7.
    assert_eq!(
        run(
            &b,
            &cfg,
            &format!("create image/png\nfill 0 0 {SIZE}\nseal 0\npublish a made doc=0"),
        )
        .await,
        ["writer:0", "ok", "blob:0", "ok"]
    );
    let made = reader.next_delivery(STREAM_7).await;
    let reference = made
        .attachments
        .get(&name("doc"))
        .expect("the delivery names the component's attachment");
    assert_eq!(
        (reference.size, reference.content_type.as_str()),
        (SIZE as u64, "image/png")
    );
    let opened = reader
        .open(STREAM_7, made.message_id, "doc")
        .await
        .expect("the remote opens the component's attachment");
    assert_eq!(
        reader.read(opened.handle, 0, opened.size).await,
        Ok(data),
        "the remote reads every byte the component wrote"
    );
    reader.close_blob(opened.handle).await;

    assert!(reader.is_attached());
    assert!(b.rig.harness.captured().await.is_empty());
    reader.close().await;
    b.rig.client.close().await;
}
