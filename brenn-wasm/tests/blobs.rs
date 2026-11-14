// The `blobs` interface on the backend host, through the SDK-built
// `processor-blobs-test` fixture over a real `BlobStore`.
//
// Each test hands the fixture activations built by hand: every new message on
// `in` is a script whose commands the fixture runs, and its report lists what
// each call returned. Messages that carry attachments are built here from
// blobs the test made in the store, as a channel would deliver them.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::Path;
use std::sync::Arc;

use brenn_activation::Activation;
use brenn_envelope::grants::ComponentGrant;
use brenn_envelope::{AttachmentName, Attachments, BlobId, MessageBody, MessageEnvelope};
use brenn_messaging_store::blob::{
    BlobError, BlobHold, BlobLimits, BlobStore, BlobUsage, BlobWriter, Retain,
};
use brenn_wasm::{
    BlobHost, HostActivation, PROCESSOR_MAX_BLOB_CALLS_PER_ACTIVATION, PROCESSOR_MAX_BLOB_HANDLES,
    PROCESSOR_MAX_BLOB_READ_BYTES, PROCESSOR_MAX_BLOB_WRITE_BYTES, ProcessorComponent,
    ProcessorLoadSpec, ProcessorOutcome, ProcessorPublish,
};
use serde_json::Value;
use tracing_test::traced_test;

mod common;

/// The seam over one in-memory store, as the bootstrap builds it over the
/// messenger's.
struct StoreSeam(Arc<BlobStore>);

impl BlobHost for StoreSeam {
    fn retain(&self, id: &BlobId) -> Option<BlobHold> {
        match self.0.retain(id) {
            Retain::Held(hold) => Some(hold),
            Retain::NotFound => None,
            Retain::Unsettled => panic!("a store with no database holds no settled blob"),
        }
    }

    fn create(&self, content_type: &str) -> Result<BlobWriter, BlobError> {
        self.0.create(content_type)
    }
}

/// A store over a fresh directory and the fixture loaded against it.
struct Rig {
    dir: tempfile::TempDir,
    store: Arc<BlobStore>,
    component: ProcessorComponent,
}

const GENEROUS: BlobLimits = BlobLimits {
    max_blob_bytes: 8 * 1024 * 1024,
    budget_bytes: 64 * 1024 * 1024,
};

/// `blobs.publish` without `ports` is a unit test of the host: this fixture
/// publishes its report through `ports`, so it cannot load without the word.
fn rig(limits: BlobLimits) -> Rig {
    let dir = tempfile::tempdir().expect("blob directory");
    let store = BlobStore::for_tests(dir.path(), limits);
    let grants: BTreeSet<ComponentGrant> = [
        ComponentGrant::Ports,
        ComponentGrant::Log,
        ComponentGrant::Blobs,
    ]
    .into();
    let component = ProcessorComponent::load(ProcessorLoadSpec {
        output_ports: HashMap::from([
            ("report".to_string(), common::out_spec("brenn:report")),
            ("a".to_string(), common::out_spec("brenn:a")),
            ("b".to_string(), common::out_spec("brenn:b")),
        ]),
        declared_out_ports: ["report", "a", "b"].into_iter().map(String::from).collect(),
        input_amplification_mt: common::amp_in(),
        grants,
        max_payload_bytes: 1024 * 1024,
        blob_host: Some(Arc::new(StoreSeam(Arc::clone(&store)))),
        ..ProcessorLoadSpec::minimal(
            &common::artifact_path("brenn_processor_blobs_test"),
            "blobs-test",
        )
    });
    Rig {
        dir,
        store,
        component,
    }
}

impl Rig {
    /// A sealed blob holding `bytes`.
    fn blob(&self, content_type: &str, bytes: &[u8]) -> BlobHold {
        let mut writer = self.store.create(content_type).expect("create");
        writer.write(0, bytes).expect("write");
        writer.seal().expect("seal")
    }

    fn usage(&self) -> BlobUsage {
        self.store.usage()
    }

    /// The files in the blob directory.
    fn files(&self) -> usize {
        files(self.dir.path())
    }

    fn handle(&self, activation: HostActivation) -> ProcessorOutcome {
        self.component.handle(activation)
    }
}

fn files(dir: &Path) -> usize {
    std::fs::read_dir(dir).expect("read blob directory").count()
}

/// A message on `in` whose body is `script`, carrying `attachments`.
fn script_message(
    id: &str,
    script: &str,
    attachments: &[(&str, &BlobHold)],
) -> Arc<MessageEnvelope> {
    let mut envelope =
        brenn_envelope::testutils::fixture_envelope("brenn:in", id, MessageBody::text(script));
    envelope.attachments = Attachments::from_map(
        attachments
            .iter()
            .map(|(name, hold)| {
                (
                    AttachmentName::new(*name).expect("a test attachment name"),
                    hold.reference(),
                )
            })
            .collect::<BTreeMap<_, _>>(),
    );
    Arc::new(envelope)
}

/// An activation whose `in` window holds `context` then `new`.
fn activation(
    context: Vec<Arc<MessageEnvelope>>,
    new: Vec<Arc<MessageEnvelope>>,
) -> HostActivation {
    let new_from = u32::try_from(context.len()).expect("a small window");
    let mut envelopes = context;
    envelopes.extend(new);
    Activation {
        ports: vec![common::window("in", envelopes, new_from)],
        deferred: vec![],
        now: Some(1_000),
        sync: None,
    }
}

/// An activation of one new script message.
fn run(script: &str, attachments: &[(&str, &BlobHold)]) -> HostActivation {
    activation(vec![], vec![script_message("01", script, attachments)])
}

/// The publishes of an ok outcome other than the report, and the report's
/// result words.
fn ok(outcome: ProcessorOutcome) -> (Vec<ProcessorPublish>, Vec<String>) {
    let ProcessorOutcome::Ok { mut publishes, .. } = outcome else {
        panic!("expected an ok outcome, got {outcome:?}");
    };
    let report = publishes.pop().expect("the report is the last publish");
    assert_eq!(report.port, "report", "the report is the last publish");
    let report: Value =
        serde_json::from_str(report.body.as_text().expect("text report")).expect("JSON report");
    let words = common::results(&report)
        .into_iter()
        .map(String::from)
        .collect();
    (publishes, words)
}

fn words(outcome: ProcessorOutcome) -> Vec<String> {
    ok(outcome).1
}

const NOTHING: BlobUsage = BlobUsage {
    sealed: 0,
    unsealed: 0,
    charged_bytes: 0,
};

#[test]
fn an_attachment_is_read_by_range() {
    let rig = rig(GENEROUS);
    let x = rig.blob("text/plain", b"0123456789");
    let y = rig.blob("application/octet-stream", &[0xde, 0xad, 0xbe]);
    // An offset whose end passes `u64::MAX` is out of range, not wrapped.
    let script = "limits\nlist self\nopen self x\nread 0 2 4\nread 0 8 3\nread 0 0 1048577\n\
                  read 0 10 0\nread 0 4 0\nsize 0\ntype 0\nopen self y\nread 1 0 3\n\
                  read 0 18446744073709551615 1\nread 0 18446744073709551615 0";
    let got = words(rig.handle(run(script, &[("x", &x), ("y", &y)])));
    assert_eq!(
        got,
        [
            format!("limits:{PROCESSOR_MAX_BLOB_READ_BYTES}:{PROCESSOR_MAX_BLOB_WRITE_BYTES}"),
            "x/10/text/plain,y/3/application/octet-stream".to_string(),
            "blob:0".to_string(),
            format!("hex:{}", common::hex(b"2345")),
            "out-of-range".to_string(),
            "too-large".to_string(),
            "hex:".to_string(),
            "hex:".to_string(),
            "10".to_string(),
            "text/plain".to_string(),
            "blob:1".to_string(),
            "hex:deadbe".to_string(),
            "out-of-range".to_string(),
            "out-of-range".to_string(),
        ]
    );
}

#[test]
fn writes_in_any_order_seal_once_the_gap_is_filled() {
    let rig = rig(GENEROUS);
    let script = "create application/octet-stream\nwrite 0 4 aabb\nwrite 0 2 ffff\nseal 0\n\
                  write 0 0 0011\nwrite 0 2 2233\nseal 0\nread 0 0 6\nsize 0\ntype 0";
    let got = words(rig.handle(run(script, &[])));
    assert_eq!(
        got,
        [
            "writer:0",
            "ok",
            "ok",
            "gap:0:2",
            "ok",
            "ok",
            "blob:0",
            "hex:00112233aabb",
            "6",
            "application/octet-stream",
        ]
    );
}

#[test]
fn an_empty_blob_seals() {
    let rig = rig(GENEROUS);
    let got = words(rig.handle(run("create text/plain\nseal 0\nsize 0\nread 0 0 0", &[])));
    assert_eq!(got, ["writer:0", "blob:0", "0", "hex:"]);
}

#[test]
fn an_invalid_content_type_is_refused() {
    let rig = rig(GENEROUS);
    let got = words(rig.handle(run("create notatype", &[])));
    assert_eq!(got, ["invalid-content-type"]);
    assert_eq!(rig.usage(), NOTHING);
}

#[test]
fn the_per_blob_cap_and_the_store_budget_refuse_a_write() {
    let rig = rig(BlobLimits {
        max_blob_bytes: 16,
        budget_bytes: 24,
    });
    // The per-blob cap: the extent may reach 16 and not pass it.
    let got = words(rig.handle(run(
        "create text/plain\nfill 0 0 16\nfill 0 16 1\nfill 0 8 9",
        &[],
    )));
    assert_eq!(got, ["writer:0", "ok", "blob-too-large", "blob-too-large"]);

    // The budget: two writers' extents together may reach 24 and not pass it.
    let got = words(rig.handle(run(
        "create text/plain\ncreate text/plain\nfill 0 0 16\nfill 1 0 9\nfill 1 0 8",
        &[],
    )));
    assert_eq!(
        got,
        ["writer:0", "writer:1", "ok", "budget-exhausted", "ok"]
    );
    assert_eq!(
        rig.usage(),
        NOTHING,
        "unsealed writers are deleted at the end"
    );
}

#[test]
#[traced_test]
fn store_refusals_are_reported_once_per_activation() {
    let rig = rig(BlobLimits {
        max_blob_bytes: 16,
        budget_bytes: 24,
    });
    // A guest retrying against a full store: every refusal is counted into
    // one post-activation line.
    let got = words(rig.handle(run(
        "create text/plain\ncreate text/plain\nfill 0 0 16\nrepeat 100 fill 1 0 9",
        &[],
    )));
    assert_eq!(got, ["writer:0", "writer:1", "ok", "budget-exhausted*100"]);
    assert!(logs_contain("BudgetExhausted: 100"));
    logs_assert(|lines: &[&str]| {
        let n = lines
            .iter()
            .filter(|l| l.contains("wasm blobs: calls refused by the blob store"))
            .count();
        if n == 1 {
            Ok(())
        } else {
            Err(format!("expected one store-refusal line, got {n}"))
        }
    });
}

#[test]
fn the_fragment_bound_refuses_a_write() {
    let rig = rig(GENEROUS);
    let mut script = String::from("create application/octet-stream\n");
    for i in 0..=brenn_messaging_store::blob::MAX_WRITTEN_RANGES {
        script.push_str(&format!("write 0 {} 00\n", i * 2));
    }
    let got = words(rig.handle(run(&script, &[])));
    assert_eq!(
        got.len(),
        brenn_messaging_store::blob::MAX_WRITTEN_RANGES + 2
    );
    assert!(
        got[1..=brenn_messaging_store::blob::MAX_WRITTEN_RANGES]
            .iter()
            .all(|w| w == "ok")
    );
    assert_eq!(got.last().map(String::as_str), Some("too-fragmented"));
}

#[test]
#[traced_test]
fn the_read_and_write_size_bounds_refuse_and_are_reported() {
    let rig = rig(GENEROUS);
    let x = rig.blob("text/plain", b"abc");
    let script = format!(
        "open self x\nread 0 0 {}\ncreate text/plain\nfill 0 0 {}\nfill 0 0 {}",
        PROCESSOR_MAX_BLOB_READ_BYTES + 1,
        PROCESSOR_MAX_BLOB_WRITE_BYTES + 1,
        PROCESSOR_MAX_BLOB_WRITE_BYTES,
    );
    let got = words(rig.handle(run(&script, &[("x", &x)])));
    assert_eq!(got, ["blob:0", "too-large", "writer:0", "too-large", "ok"]);
    assert!(logs_contain(
        "wasm blobs: calls refused at a per-activation bound"
    ));
    assert!(logs_contain("ReadSize: 1"));
    assert!(logs_contain("WriteSize: 1"));
}

#[test]
#[traced_test]
fn the_handle_bound_counts_live_handles() {
    let rig = rig(GENEROUS);
    let x = rig.blob("text/plain", b"abc");
    let limit = PROCESSOR_MAX_BLOB_HANDLES;
    // Every slot taken, one more refused at open and at create; a drop frees
    // one, and a seal takes no second slot for the writer it spends.
    let script = format!(
        "repeat {} open self x\ncreate text/plain\ndrop 0\ncreate text/plain\nseal 0\nopen self x",
        limit + 1
    );
    let got = words(rig.handle(run(&script, &[("x", &x)])));
    assert_eq!(
        got,
        [
            format!("blob*{limit},quota-exceeded*1"),
            "quota-exceeded".to_string(),
            "ok".to_string(),
            "writer:0".to_string(),
            format!("blob:{limit}"),
            "quota-exceeded".to_string(),
        ]
    );
    assert!(logs_contain("Handles: 3"));
}

#[test]
#[traced_test]
fn the_call_bound_refuses_every_blob_call_past_it() {
    let rig = rig(GENEROUS);
    let x = rig.blob("text/plain", b"abc");
    let limit = PROCESSOR_MAX_BLOB_CALLS_PER_ACTIVATION;
    // `open` is the first call; `size` is not charged.
    let script = format!(
        "open self x\nrepeat {} read 0 0 1\nsize 0\ncreate text/plain",
        limit + 1
    );
    let got = words(rig.handle(run(&script, &[("x", &x)])));
    assert_eq!(
        got,
        [
            "blob:0".to_string(),
            format!("hex:61*{},quota-exceeded*2", limit - 1),
            "3".to_string(),
            "quota-exceeded".to_string(),
        ]
    );
    assert!(logs_contain("Calls: 3"));
}

#[test]
#[traced_test]
fn the_bytes_written_bound_charges_bytes_passed_not_extent() {
    let rig = rig(GENEROUS);
    let mib = PROCESSOR_MAX_BLOB_WRITE_BYTES;
    let times = brenn_wasm::PROCESSOR_MAX_BLOB_BYTES_WRITTEN_PER_ACTIVATION / u64::from(mib);
    // The same megabyte rewritten: the extent stays one megabyte.
    let script = format!(
        "create application/octet-stream\nrepeat {} fill 0 0 {mib}",
        times + 1
    );
    let got = words(rig.handle(run(&script, &[])));
    assert_eq!(
        got,
        [
            "writer:0".to_string(),
            format!("ok*{times},quota-exceeded*1")
        ]
    );
    assert!(logs_contain("BytesWritten: 1"));
    assert_eq!(rig.usage(), NOTHING);
}

/// Only a raw-binding caller reaches a spent writer: the SDK's `seal`
/// consumes it. The call traps; the host does not panic.
#[test]
fn a_call_on_a_spent_writer_traps() {
    let rig = rig(GENEROUS);
    for call in ["write", "seal"] {
        let outcome = rig.handle(run(&format!("spent {call}"), &[]));
        let ProcessorOutcome::Trap(message) = &outcome else {
            panic!("expected a trap, got {outcome:?}");
        };
        assert!(
            message.contains(&format!("blobs.writer.{call} on a writer already sealed")),
            "{message}"
        );
        assert_eq!(rig.usage(), NOTHING);
        assert_eq!(rig.files(), 0);
    }
}

/// The sealed blob took the writer's slot under the handle bound, so dropping
/// the spent writer frees nothing: one slot stays taken.
#[test]
fn a_dropped_spent_writer_frees_no_slot() {
    let rig = rig(GENEROUS);
    let limit = PROCESSOR_MAX_BLOB_HANDLES;
    let script = format!("spent drop\nrepeat {limit} create text/plain");
    let got = words(rig.handle(run(&script, &[])));
    assert_eq!(
        got,
        [
            "blob:0".to_string(),
            format!("writer*{},quota-exceeded*1", limit - 1)
        ]
    );
}

/// No function of the interface takes a blob id, so an id in a body or a
/// header names nothing: the only way to a blob is `(port, index, name)` of a
/// message this activation delivered, or a blob it made.
#[test]
fn only_an_attachment_of_a_delivered_message_opens() {
    let rig = rig(GENEROUS);
    let x = rig.blob("text/plain", b"abc");
    let script = "open-raw nosuch 0 x\nopen-raw in 1 x\nopen self y\nopen-raw in 0 x";
    let got = words(rig.handle(run(script, &[("x", &x)])));
    assert_eq!(
        got,
        ["not-permitted", "not-permitted", "not-permitted", "blob:0"]
    );
}

#[test]
fn an_attachment_is_unreachable_once_its_message_left_the_window() {
    let rig = rig(GENEROUS);
    let x = rig.blob("text/plain", b"abc");
    let first = script_message("01", "open self x\nsize 0", &[("x", &x)]);
    assert_eq!(
        words(rig.handle(activation(vec![], vec![first]))),
        ["blob:0", "3"]
    );
    // The window moved on; the blob is alive under the test's own hold.
    let second = script_message("02", "open-raw in 0 x\nopen-raw in 1 x", &[]);
    assert_eq!(
        words(rig.handle(activation(vec![], vec![second]))),
        ["not-permitted", "not-permitted"]
    );
    assert_eq!(rig.usage().sealed, 1);
}

#[test]
fn a_context_message_opens_by_its_window_index() {
    let rig = rig(GENEROUS);
    let x = rig.blob("text/plain", b"abc");
    let context = script_message("01", "", &[("x", &x)]);
    let new = script_message("02", "open in:0 x\nread 0 0 3\nopen self x", &[]);
    assert_eq!(
        words(rig.handle(activation(vec![context], vec![new]))),
        ["blob:0", "hex:616263", "not-permitted"]
    );
}

#[test]
fn err_after_create_seal_and_publish_deletes_everything() {
    let rig = rig(GENEROUS);
    let script = "create text/plain\nwrite 0 0 6869\nseal 0\npublish a hi doc=0\nfail";
    let outcome = rig.handle(run(script, &[]));
    assert!(matches!(outcome, ProcessorOutcome::Err(_)), "{outcome:?}");
    assert_eq!(rig.usage(), NOTHING);
    assert_eq!(rig.files(), 0);
}

#[test]
fn a_trap_after_create_seal_and_publish_deletes_everything() {
    let rig = rig(GENEROUS);
    let script = "create text/plain\nwrite 0 0 6869\nseal 0\npublish a hi doc=0\n\
                  create text/plain\nwrite 1 0 00\ntrap";
    let outcome = rig.handle(run(script, &[]));
    assert!(matches!(outcome, ProcessorOutcome::Trap(_)), "{outcome:?}");
    assert_eq!(rig.usage(), NOTHING);
    assert_eq!(rig.files(), 0);
}

#[test]
fn at_ok_only_what_a_publish_names_survives() {
    let rig = rig(GENEROUS);
    // A sealed blob nothing names, an unsealed writer, and one published blob.
    let script = "create text/plain\nwrite 0 0 01\nseal 0\ncreate text/plain\nwrite 1 0 02\n\
                  create text/plain\nwrite 2 0 0304\nseal 2\npublish a hi doc=1";
    let (publishes, got) = ok(rig.handle(run(script, &[])));
    assert_eq!(
        got,
        [
            "writer:0", "ok", "blob:0", "writer:1", "ok", "writer:2", "ok", "blob:1", "ok"
        ]
    );
    assert_eq!(
        rig.usage(),
        BlobUsage {
            sealed: 1,
            unsealed: 0,
            charged_bytes: 2,
        },
        "only the published blob is held, by the buffered publish"
    );
    assert_eq!(rig.files(), 1);
    let [publish] = <[ProcessorPublish; 1]>::try_from(publishes).expect("one publish");
    let (name, reference) = publish
        .attachments
        .refs()
        .iter()
        .next()
        .expect("one attachment");
    assert_eq!(name.as_str(), "doc");
    assert_eq!(reference.size, 2);
    assert_eq!(reference.content_type, "text/plain");
    drop(publish);
    assert_eq!(rig.usage(), NOTHING, "the flush's hold was the last");
    assert_eq!(rig.files(), 0);
}

#[test]
fn a_reference_whose_blob_has_no_holder_is_not_found() {
    let rig = rig(GENEROUS);
    let gone = rig.blob("text/plain", b"abc");
    let message = script_message("01", "open self x\nlist self", &[("x", &gone)]);
    drop(gone);
    assert_eq!(rig.usage(), NOTHING);
    assert_eq!(
        words(rig.handle(activation(vec![], vec![message]))),
        ["not-found", "x/3/text/plain"]
    );
}

#[test]
fn forwarding_holds_the_same_blob_and_copies_nothing() {
    let rig = rig(GENEROUS);
    let x = rig.blob("text/plain", b"abcdef");
    let before = rig.usage();
    let script = "open self x\npublish b fwd copy=0 again=0";
    let (publishes, got) = ok(rig.handle(run(script, &[("x", &x)])));
    assert_eq!(got, ["blob:0", "ok"]);
    assert_eq!(rig.usage(), before, "no byte charged for a forward");
    let [publish] = <[ProcessorPublish; 1]>::try_from(publishes).expect("one publish");
    assert_eq!(publish.channel_address, "brenn:b");
    let refs: Vec<_> = publish.attachments.refs().iter().collect();
    assert_eq!(refs.len(), 2, "the same blob under two names");
    for (_, reference) in refs {
        assert_eq!(reference.id, *x.id());
    }
    drop(x);
    assert_eq!(rig.usage().sealed, 1, "the publish holds it");
    drop(publish);
    assert_eq!(rig.usage(), NOTHING);
}

#[test]
fn publish_refuses_bad_attachment_lists_by_variant() {
    let rig = rig(GENEROUS);
    let x = rig.blob("text/plain", b"abc");
    let many: String = (0..=brenn_envelope::MAX_ATTACHMENTS_PER_MESSAGE)
        .map(|i| format!(" n{i}=0"))
        .collect();
    let script = format!(
        "open self x\npublish a hi -bad=0\npublish a hi d=0 d=0\npublish a hi{many}\n\
         publish nosuchport hi d=0"
    );
    let outcome = rig.handle(run(&script, &[("x", &x)]));
    // An undeclared port traps, as `ports.publish` does.
    assert!(matches!(outcome, ProcessorOutcome::Trap(_)), "{outcome:?}");

    let script =
        format!("open self x\npublish a hi -bad=0\npublish a hi d=0 d=0\npublish a hi{many}");
    let (publishes, got) = ok(rig.handle(run(&script, &[("x", &x)])));
    assert!(publishes.is_empty());
    assert_eq!(
        got,
        [
            "blob:0",
            "invalid-attachment-name",
            "duplicate-attachment-name",
            "too-many-attachments",
        ]
    );
}

#[test]
fn ports_and_blobs_publishes_keep_call_order() {
    let rig = rig(GENEROUS);
    let x = rig.blob("text/plain", b"abc");
    let script = "open self x\npublish-plain a p1\npublish a p2 doc=0\n\
                  publish-deferred a p3 5000\npublish b p4 doc=0 @6000\npublish-plain b p5";
    let (publishes, got) = ok(rig.handle(run(script, &[("x", &x)])));
    assert_eq!(got, ["blob:0", "ok", "ok", "ok", "ok", "ok"]);
    let seen: Vec<_> = publishes
        .iter()
        .map(|p| {
            (
                p.port.as_str(),
                p.body.as_text().expect("text"),
                p.deliver_after,
                p.attachments.refs().len(),
            )
        })
        .collect();
    assert_eq!(
        seen,
        [
            ("a", "p1", None, 0),
            ("a", "p2", None, 1),
            ("a", "p3", Some(5000), 0),
            ("b", "p4", Some(6000), 1),
            ("b", "p5", None, 0),
        ]
    );
}
