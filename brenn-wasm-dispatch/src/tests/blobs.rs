//! The `blobs` interface under a real `Messenger`: `processor-blobs-test`
//! consumers make, read, forward and park blobs on a `brenn:` and an
//! `ephemeral:` channel, and a second consumer reads back what the first
//! published.
//!
//! Every new text message on a consumer's `in` channel is a script the fixture
//! runs; its report lists what each call returned. The blob seam is the
//! production one over the messenger, wrapped with one addition for the
//! eviction cases: a hook run once, right after the first `retain`, while the
//! guest still holds the handle it is about to receive.
//!
//! `measure` is the forwarding measurement; `//brenn-wasm-dispatch:blob_forward_measure`
//! runs it and `docs/blob-store-timings.md` records a run.

use super::non_text::{
    READER, activate, attach_subscribers, report_channel, results, unattached_messenger,
};
use super::*;

use brenn_lib::messaging::config::WasmOutputPort;
use brenn_messaging::Messenger;
use brenn_messaging::testutils::{
    TempBlobDir, covering_system_publisher, install_blob_store_at, publish_attached, seal_blob,
};
use brenn_messaging_store::blob::{BlobError, BlobHold, BlobUsage, BlobWriter};
use brenn_wasm::BlobHost;
use std::sync::Mutex;

const BLOBS_WASM: &str = "brenn-wasm/target/components/brenn_processor_blobs_test.wasm";

/// The host component that publishes scripts and seeded attachments.
const SYSTEM: &str = "bl-system";

/// What a hook does inside the guest's `open`, after the store took its hold.
type Hook = Box<dyn FnOnce(&Messenger, &tokio::runtime::Handle) + Send>;

/// The production seam over `messenger` ([`crate::blob_host_over`]), with a
/// hook run once after the first `retain`.
struct HookedSeam {
    inner: brenn_wasm::BlobHostFn,
    messenger: Arc<Messenger>,
    handle: tokio::runtime::Handle,
    after_retain: Mutex<Option<Hook>>,
}

impl BlobHost for HookedSeam {
    fn retain(&self, id: &brenn_envelope::BlobId) -> Option<BlobHold> {
        let held = self.inner.retain(id);
        let hook = self.after_retain.lock().expect("hook lock").take();
        if let Some(hook) = hook {
            hook(&self.messenger, &self.handle);
        }
        held
    }

    fn create(&self, content_type: &str) -> Result<BlobWriter, BlobError> {
        self.inner.create(content_type)
    }
}

/// Where a channel keeps its messages.
#[derive(Clone, Copy, Debug)]
enum Kept {
    /// `brenn:`, with this retain depth (and so this deferred cap).
    Durable(Depth),
    /// `ephemeral:`, retaining this many.
    Ring(u64),
}

/// A channel `name` on `kept`, read by each WASM subscriber in `readers`.
fn channel(name: &str, kept: Kept, readers: &[&str]) -> ChannelEntry {
    let subscribers = readers
        .iter()
        .map(|slug| SubscriberEntry {
            kind: SubscriberEntryKind::Wasm(slug.to_string()),
            push_depth: Depth::Unbounded,
            retain_depth: Depth::Unbounded,
            noise: NoiseLevel::Silent,
            wake_min: None,
        })
        .collect();
    let mut entry = match kept {
        Kept::Durable(retain) => {
            let mut entry = brenn_channel(&format!("brenn:{name}"), "unused");
            entry.resolved_channel.retain_depth = retain;
            entry.resolved_channel.standing_retain_depth = retain;
            entry
        }
        Kept::Ring(depth) => testutils::ephemeral_channel_entry(name, depth),
    };
    entry.subscribers = subscribers;
    entry
}

/// A messenger over `entries` with a blob store, every subscriber attached at
/// head, and the seam over it.
struct Rig {
    messenger: Arc<Messenger>,
    seam: Arc<HookedSeam>,
    blobs: TempBlobDir,
}

async fn rig(entries: &[ChannelEntry]) -> Rig {
    let blobs = TempBlobDir::new();
    let messenger = install_blob_store_at(
        unattached_messenger(entries, covering_system_publisher(SYSTEM)).await,
        blobs.path(),
    )
    .await;
    let messenger = attach_subscribers(messenger, entries).await;
    let seam = Arc::new(HookedSeam {
        inner: crate::blob_host_over(&messenger).expect("the messenger has a blob store"),
        messenger: Arc::clone(&messenger),
        handle: tokio::runtime::Handle::current(),
        after_retain: Mutex::new(None),
    });
    Rig {
        messenger,
        seam,
        blobs,
    }
}

/// One fixture consumer: its script channel, its report channel and its
/// config.
struct Fixture {
    script: ChannelEntry,
    report: ChannelEntry,
    cfg: WasmConsumerConfig,
}

impl Rig {
    /// The fixture as `slug`, running scripts from `script`, reading each
    /// `(port, channel)` of `feeds`, publishing on each `(port, channel)` of
    /// `outs` and reporting on `report`.
    fn fixture(
        &self,
        slug: &str,
        script: &ChannelEntry,
        report: &ChannelEntry,
        feeds: &[(&str, &ChannelEntry)],
        outs: &[(&str, &ChannelEntry)],
    ) -> Fixture {
        let mut inputs = vec![input(slug, "in", script)];
        inputs.extend(feeds.iter().map(|(port, entry)| input(slug, port, entry)));
        let mut outputs: Vec<WasmOutputPort> = vec![output("report", report)];
        outputs.extend(outs.iter().map(|(port, entry)| output(port, entry)));
        let mut input_amplification_mt = test_amp_map();
        input_amplification_mt.extend(feeds.iter().map(|(port, _)| (port.to_string(), 1_000)));
        let seam: brenn_wasm::BlobHostFn = Arc::clone(&self.seam) as _;
        let component = Arc::new(ProcessorComponent::load(ProcessorLoadSpec {
            declared_out_ports: ["report", "a", "b"].into_iter().map(String::from).collect(),
            output_ports: output_specs(&outputs),
            input_amplification_mt,
            grants: [
                ComponentGrant::Ports,
                ComponentGrant::Log,
                ComponentGrant::Blobs,
            ]
            .into_iter()
            .collect(),
            alerter: noop_proc_alerter(),
            blob_host: Some(seam),
            ..ProcessorLoadSpec::minimal(std::path::Path::new(BLOBS_WASM), slug)
        }));
        Fixture {
            script: script.clone(),
            report: report.clone(),
            cfg: consumer(slug, component, &self.messenger, inputs, outputs),
        }
    }

    /// Publish `body` on `entry` from the system component, naming `holds`.
    async fn publish(&self, entry: &ChannelEntry, body: &str, holds: &[(&str, &BlobHold)]) {
        publish_attached(&self.messenger, SYSTEM, &entry.address, body, holds, None).await;
    }

    /// Hand `fixture` `script` and activate it once; the report's result words.
    async fn run(&self, fixture: &Fixture, script: &str) -> Vec<String> {
        self.run_with(fixture, script, &[]).await
    }

    /// As [`Rig::run`], the script message naming `holds`.
    async fn run_with(
        &self,
        fixture: &Fixture,
        script: &str,
        holds: &[(&str, &BlobHold)],
    ) -> Vec<String> {
        self.publish(&fixture.script, script, holds).await;
        activate(&fixture.cfg).await;
        let report = one_report(&self.messenger, &fixture.report.address).await;
        results(&report).into_iter().map(String::from).collect()
    }

    fn usage(&self) -> BlobUsage {
        self.messenger
            .blob_store()
            .expect("the messenger has a blob store")
            .usage()
    }

    /// Run `hook` inside the next `open`, after the store took its hold.
    fn after_next_retain(&self, hook: Hook) {
        let previous = self
            .seam
            .after_retain
            .lock()
            .expect("hook lock")
            .replace(hook);
        assert!(previous.is_none(), "one hook at a time");
    }
}

const NOTHING: BlobUsage = BlobUsage {
    sealed: 0,
    unsealed: 0,
    charged_bytes: 0,
};

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Epoch milliseconds an hour from now: a `deliver-after` no test reaches
/// without a release.
fn an_hour_on() -> (u64, chrono::DateTime<chrono::Utc>) {
    let at = chrono::Utc::now() + chrono::Duration::hours(1);
    (
        u64::try_from(at.timestamp_millis()).expect("after the epoch"),
        at,
    )
}

/// Every report published on `address` since the last read, parsed; the
/// reader is then advanced past them. The fixture publishes its reports
/// untyped.
async fn reports(messenger: &Messenger, address: &str) -> Vec<serde_json::Value> {
    let reports = owed_on(messenger, READER, address)
        .await
        .iter()
        .map(|envelope| {
            serde_json::from_str(envelope.body.as_text().expect("a report is text"))
                .expect("a report is JSON")
        })
        .collect();
    testutils::consume_owed(messenger, address, &ParticipantId::for_wasm(READER)).await;
    reports
}

/// The one report published on `address` since the last read.
async fn one_report(messenger: &Messenger, address: &str) -> serde_json::Value {
    let mut all = reports(messenger, address).await;
    assert_eq!(all.len(), 1, "one activation, one report: {all:?}");
    all.remove(0)
}

/// Every envelope `slug` is owed on `address`.
async fn owed_on(
    messenger: &Messenger,
    slug: &str,
    address: &str,
) -> Vec<Arc<brenn_envelope::MessageEnvelope>> {
    testutils::owed_everywhere(messenger, &ParticipantId::for_wasm(slug))
        .await
        .into_iter()
        .filter(|(channel, _)| channel == address)
        .map(|(_, envelope)| envelope)
        .collect()
}

/// The id of attachment `name` on `envelope`.
fn attachment_id(envelope: &brenn_envelope::MessageEnvelope, name: &str) -> brenn_envelope::BlobId {
    envelope
        .attachments
        .get(&brenn_envelope::AttachmentName::new(name).expect("a test attachment name"))
        .unwrap_or_else(|| panic!("no attachment {name} on {}", envelope.message_id))
        .id
        .clone()
}

/// A component makes a blob by out-of-order, overlapping writes, seals it and
/// publishes it on a `brenn:` and an `ephemeral:` channel; a second component
/// opens it from each and reads back every byte.
#[tokio::test]
async fn a_made_blob_is_published_on_either_store_and_read_back_downstream() {
    let (maker, reader) = ("bl-made-p", "bl-made-q");
    let p_in = channel("bl-made-p-in", Kept::Durable(Depth::Unbounded), &[maker]);
    let q_in = channel("bl-made-q-in", Kept::Durable(Depth::Unbounded), &[reader]);
    let a = channel("bl-made-a", Kept::Durable(Depth::Unbounded), &[reader]);
    let b = channel("bl-made-b", Kept::Ring(4), &[reader]);
    let (p_report, q_report) = (
        report_channel("brenn:bl-made-p-report"),
        report_channel("brenn:bl-made-q-report"),
    );
    let rig = rig(&[
        p_in.clone(),
        q_in.clone(),
        a.clone(),
        b.clone(),
        p_report.clone(),
        q_report.clone(),
    ])
    .await;
    let p = rig.fixture(maker, &p_in, &p_report, &[], &[("a", &a), ("b", &b)]);
    let q = rig.fixture(reader, &q_in, &q_report, &[("fa", &a), ("fb", &b)], &[]);

    let made = rig
        .run(
            &p,
            "create text/x-test\n\
             write 0 3 646566\n\
             write 0 0 61626364\n\
             seal 0\n\
             publish a made doc=0\n\
             publish b made doc=0",
        )
        .await;
    assert_eq!(made, ["writer:0", "ok", "ok", "blob:0", "ok", "ok"]);
    let usage = rig.usage();
    assert_eq!(
        (usage.sealed, usage.unsealed, usage.charged_bytes),
        (1, 0, 6)
    );

    let read = rig
        .run(
            &q,
            "list fa:0\n\
             open fa:0 doc\n\
             read 0 0 6\n\
             list fb:0\n\
             open fb:0 doc\n\
             read 1 0 6",
        )
        .await;
    let bytes = format!("hex:{}", hex(b"abcdef"));
    assert_eq!(
        read,
        [
            "doc/6/text/x-test",
            "blob:0",
            bytes.as_str(),
            "doc/6/text/x-test",
            "blob:1",
            bytes.as_str(),
        ]
    );
}

/// A received attachment republished under another name on another channel
/// is the same blob: the id is unchanged and the store charges nothing more.
/// It outlives the first message and goes with the second.
#[tokio::test]
async fn a_forwarded_attachment_is_the_same_blob_and_lives_until_its_last_message_goes() {
    let (forwarder, tap) = ("bl-fwd-p", "bl-fwd-tap");
    let p_in = channel("bl-fwd-p-in", Kept::Ring(1), &[forwarder]);
    let b = channel("bl-fwd-b", Kept::Ring(1), &[tap]);
    let p_report = report_channel("brenn:bl-fwd-p-report");
    let rig = rig(&[p_in.clone(), b.clone(), p_report.clone()]).await;
    let p = rig.fixture(forwarder, &p_in, &p_report, &[], &[("b", &b)]);

    let photo = seal_blob(&rig.messenger, &[7u8; 1000]);
    let id = photo.reference().id;
    let charged = rig.usage().charged_bytes;
    let forwarded = rig
        .run_with(
            &p,
            "open self photo\n\
             publish b fwd copy=0",
            &[("photo", &photo)],
        )
        .await;
    drop(photo);
    assert_eq!(forwarded, ["blob:0", "ok"]);
    assert_eq!(
        rig.usage().charged_bytes,
        charged,
        "forwarding charges nothing"
    );
    assert_eq!(rig.usage().sealed, 1);

    let on_b = owed_on(&rig.messenger, tap, &b.address).await;
    assert_eq!(on_b.len(), 1);
    assert_eq!(attachment_id(&on_b[0], "copy"), id, "the same blob");
    drop(on_b);

    rig.publish(&p_in, "evicts the first", &[]).await;
    assert!(rig.blobs.holds_file(&id), "the forwarded message holds it");
    rig.publish(&b, "evicts the second", &[]).await;
    assert!(!rig.blobs.holds_file(&id), "nothing holds it");
    assert_eq!(rig.usage(), NOTHING);
}

/// A handle holds its blob when the last message naming it is evicted while
/// the guest still has it open: the read after the eviction gets every byte,
/// and the blob goes when the handle drops — at once on a ring, at the next
/// reap on a durable channel.
#[tokio::test]
async fn a_handle_keeps_its_blob_across_the_eviction_of_its_last_message_on_either_store() {
    for kept in [Kept::Durable(Depth::Unbounded), Kept::Ring(1)] {
        let durable = matches!(kept, Kept::Durable(_));
        let tag = if durable { "durable" } else { "ring" };
        let slug = format!("bl-evict-{tag}");
        let p_in = channel(
            &format!("bl-evict-{tag}-in"),
            Kept::Durable(Depth::Unbounded),
            &[slug.as_str()],
        );
        let feed = channel(&format!("bl-evict-{tag}-feed"), kept, &[slug.as_str()]);
        let p_report = report_channel(&format!("brenn:bl-evict-{tag}-report"));
        let rig = rig(&[p_in.clone(), feed.clone(), p_report.clone()]).await;
        let p = rig.fixture(&slug, &p_in, &p_report, &[("fa", &feed)], &[]);

        let doc = seal_blob(&rig.messenger, b"still here");
        let id = doc.reference().id;
        rig.publish(&feed, "carries doc", &[("doc", &doc)]).await;
        drop(doc);

        let (feed_entry, dir) = (feed.clone(), rig.blobs.path().to_path_buf());
        let evicted_id = id.clone();
        rig.after_next_retain(Box::new(move |messenger, handle| {
            handle.block_on(async {
                if durable {
                    let conn = messenger.db().lock().await;
                    let eviction = brenn_messaging_store::db::bus_gc_evict_channel(
                        &conn,
                        feed_entry.uuid,
                        &feed_entry.address,
                        feed_entry.transport_type,
                        0,
                        feed_entry.resolved_channel.sink,
                        None,
                    );
                    assert_eq!(eviction.messages_evicted, 1, "{}", feed_entry.address);
                    drop(conn);
                    assert_eq!(messenger.reap_blobs().await.blobs, 0, "the handle holds it");
                } else {
                    publish_attached(messenger, SYSTEM, &feed_entry.address, "evicts", &[], None)
                        .await;
                }
            });
            assert!(
                brenn_messaging_store::testutils::holds_file(&dir, &evicted_id),
                "the handle holds it"
            );
        }));

        let read = rig.run(&p, "open fa:0 doc\nread 0 0 10").await;
        assert_eq!(
            read,
            ["blob:0".to_string(), format!("hex:{}", hex(b"still here"))],
            "{tag}"
        );
        if durable {
            assert!(rig.blobs.holds_file(&id), "the reap deletes it");
            assert_eq!(rig.messenger.reap_blobs().await.blobs, 1);
        }
        assert!(!rig.blobs.holds_file(&id), "{tag}");
        assert_eq!(rig.usage(), NOTHING, "{tag}");
    }
}

/// A `deliver-after` publish with an attachment holds its blob while parked
/// on either store, and the released message hands it to a reader.
#[tokio::test]
async fn a_parked_attachment_is_held_and_delivered_at_release_on_either_store() {
    let (maker, reader) = ("bl-park-p", "bl-park-q");
    let p_in = channel("bl-park-p-in", Kept::Durable(Depth::Unbounded), &[maker]);
    let q_in = channel("bl-park-q-in", Kept::Durable(Depth::Unbounded), &[reader]);
    let a = channel("bl-park-a", Kept::Durable(Depth::Unbounded), &[reader]);
    let b = channel("bl-park-b", Kept::Ring(4), &[reader]);
    let (p_report, q_report) = (
        report_channel("brenn:bl-park-p-report"),
        report_channel("brenn:bl-park-q-report"),
    );
    let rig = rig(&[
        p_in.clone(),
        q_in.clone(),
        a.clone(),
        b.clone(),
        p_report.clone(),
        q_report.clone(),
    ])
    .await;
    let p = rig.fixture(maker, &p_in, &p_report, &[], &[("a", &a), ("b", &b)]);
    let q = rig.fixture(reader, &q_in, &q_report, &[("fa", &a), ("fb", &b)], &[]);

    let (at_ms, at) = an_hour_on();
    let parked = rig
        .run(
            &p,
            &format!(
                "create text/x-test\n\
                 write 0 0 {}\n\
                 seal 0\n\
                 publish a later doc=0 @{at_ms}\n\
                 publish b later doc=0 @{at_ms}\n\
                 drop 0",
                hex(b"parked")
            ),
        )
        .await;
    assert_eq!(parked, ["writer:0", "ok", "blob:0", "ok", "ok", "ok"]);
    assert_eq!(rig.usage().sealed, 1, "the parked messages hold it");
    assert_eq!(rig.messenger.reap_blobs().await.blobs, 0);
    assert_eq!(
        rig.usage().sealed,
        1,
        "a parked row holds it through the reap"
    );

    rig.messenger
        .release_due_messages(at + chrono::Duration::seconds(1))
        .await;
    let read = rig
        .run(&q, "open fa:0 doc\nread 0 0 6\nopen fb:0 doc\nread 1 0 6")
        .await;
    let bytes = format!("hex:{}", hex(b"parked"));
    assert_eq!(read, ["blob:0", bytes.as_str(), "blob:1", bytes.as_str()]);
    assert_eq!(rig.messenger.reap_blobs().await.blobs, 0);
    assert_eq!(rig.usage().sealed, 1, "the released messages hold it");
}

/// The fixture's script making one blob per entry of `ports` and parking a
/// message naming it on that port.
fn park_one_each(ports: &[&str], at_ms: u64) -> String {
    let mut script = String::new();
    for (n, _) in ports.iter().enumerate() {
        script.push_str(&format!(
            "create text/x-test\nwrite {n} 0 {n:02x}\nseal {n}\n"
        ));
    }
    for (n, port) in ports.iter().enumerate() {
        script.push_str(&format!("publish {port} later doc={n} @{at_ms}\n"));
    }
    script
}

/// The result words of [`park_one_each`] when every call succeeds.
fn parked_each(ports: &[&str]) -> Vec<String> {
    let mut words: Vec<String> = (0..ports.len())
        .flat_map(|n| [format!("writer:{n}"), "ok".to_string(), format!("blob:{n}")])
        .collect();
    words.extend(ports.iter().map(|_| "ok".to_string()));
    words
}

/// A component's `defer-cancel` of a parked message deletes the blob only it
/// named: on a ring at once, on a durable channel by the reap the cancel runs.
#[tokio::test]
async fn a_cancelled_park_deletes_its_blob_on_either_store() {
    let maker = "bl-cancel-p";
    let p_in = channel("bl-cancel-p-in", Kept::Durable(Depth::Unbounded), &[maker]);
    let a = channel("bl-cancel-a", Kept::Durable(Depth::Unbounded), &[]);
    let b = channel("bl-cancel-b", Kept::Ring(4), &[]);
    let p_report = report_channel("brenn:bl-cancel-p-report");
    let rig = rig(&[p_in.clone(), a.clone(), b.clone(), p_report.clone()]).await;
    let p = rig.fixture(maker, &p_in, &p_report, &[], &[("a", &a), ("b", &b)]);

    let (at_ms, _) = an_hour_on();
    let ports = ["a", "b"];
    assert_eq!(
        rig.run(&p, &park_one_each(&ports, at_ms)).await,
        parked_each(&ports)
    );
    assert_eq!(rig.usage().sealed, 2);

    assert_eq!(
        rig.run(&p, "defer-cancel a 0\ndefer-cancel b 0").await,
        ["ok", "ok"]
    );
    assert_eq!(rig.usage(), NOTHING, "both cancels deleted their blobs");
}

/// A park the channel's deferred cap refuses at the flush deletes the blob
/// only it named, on either store; the park beside it keeps its own.
#[tokio::test]
async fn a_park_refused_at_the_cap_deletes_its_blob_on_either_store() {
    let maker = "bl-cap-p";
    let p_in = channel("bl-cap-p-in", Kept::Durable(Depth::Unbounded), &[maker]);
    let a = channel("bl-cap-a", Kept::Durable(Depth::Bounded(1)), &[]);
    let b = channel("bl-cap-b", Kept::Ring(1), &[]);
    let p_report = report_channel("brenn:bl-cap-p-report");
    let rig = rig(&[p_in.clone(), a.clone(), b.clone(), p_report.clone()]).await;
    let p = rig.fixture(maker, &p_in, &p_report, &[], &[("a", &a), ("b", &b)]);

    let (at_ms, _) = an_hour_on();
    let ports = ["a", "a", "b", "b"];
    assert_eq!(
        rig.run(&p, &park_one_each(&ports, at_ms)).await,
        parked_each(&ports),
        "the guest is told nothing: the cap refuses at the flush"
    );
    assert_eq!(rig.usage().sealed, 2, "one park kept on each channel");
    assert_eq!(rig.messenger.reap_blobs().await.blobs, 0);
    assert_eq!(rig.usage().sealed, 2);
}

/// Bytes of the measured attachment.
const FORWARD_BYTES: usize = 100 * 1024;

/// `iterations` activations of a forwarder: each is handed a message on a
/// ring carrying a fresh 100 KiB attachment, reads it in full and republishes
/// it by reference on a second ring. Returns the time the activations took;
/// asserts every report, and that the store holds nothing once both rings
/// have moved on.
async fn forward_loop(iterations: usize) -> std::time::Duration {
    let forwarder = "bl-measure-p";
    // Every channel admits each iteration's publish and the closing one
    // without meeting its send rate.
    let unmetered = |mut entry: ChannelEntry| {
        entry.resolved_channel.send_rate = brenn_lib::messaging::config::SendRate {
            burst: u32::try_from(iterations + 1).expect("a small iteration count"),
            refill_interval_secs: 1,
            refill: 1,
        };
        entry
    };
    let p_in = unmetered(channel("bl-measure-in", Kept::Ring(1), &[forwarder]));
    let b = unmetered(channel("bl-measure-b", Kept::Ring(1), &[]));
    let p_report = unmetered(report_channel("brenn:bl-measure-report"));
    let rig = rig(&[p_in.clone(), b.clone(), p_report.clone()]).await;
    let p = rig.fixture(forwarder, &p_in, &p_report, &[], &[("b", &b)]);
    let bytes: Vec<u8> = (0..FORWARD_BYTES).map(|i| (i % 251) as u8).collect();
    let script = "open self blob\nread-all 0\npublish b fwd copy=0";

    let start = std::time::Instant::now();
    for _ in 0..iterations {
        let blob = seal_blob(&rig.messenger, &bytes);
        rig.publish(&p_in, script, &[("blob", &blob)]).await;
        drop(blob);
        activate(&p.cfg).await;
    }
    let elapsed = start.elapsed();

    let all = format!("all:{FORWARD_BYTES}:{:016x}", fnv1a64(&bytes));
    let reports = reports(&rig.messenger, &p_report.address).await;
    assert_eq!(reports.len(), iterations);
    for report in &reports {
        assert_eq!(results(report), ["blob:0", all.as_str(), "ok"]);
    }
    rig.publish(&p_in, "evicts the last script", &[]).await;
    rig.publish(&b, "evicts the last forward", &[]).await;
    assert_eq!(rig.usage(), NOTHING);
    elapsed
}

#[tokio::test]
async fn ten_forwards_leave_the_store_empty() {
    forward_loop(10).await;
}

#[tokio::test]
#[ignore = "a measurement: run by //brenn-wasm-dispatch:blob_forward_measure"]
async fn measure() {
    const ITERATIONS: usize = 10_000;
    let elapsed = forward_loop(ITERATIONS).await;
    println!(
        "blob forward: {ITERATIONS} activations of {FORWARD_BYTES} bytes in {:.3} s, {:.0} activations/s",
        elapsed.as_secs_f64(),
        ITERATIONS as f64 / elapsed.as_secs_f64()
    );
}
