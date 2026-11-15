//! `blobs.publish` into the buffer: the check order, one test per adjacent
//! pair, the door, the per-flush blob bounds, the batch charge, and call order
//! shared with `ports.publish`.

use brenn_attach_proto::BatchEntry;
use brenn_envelope::{
    AttachmentName, Attachments, BlobId, BlobRef, ChannelScheme, MAX_ATTACHMENTS_PER_MESSAGE,
    MessageEnvelope,
};

use crate::blob_scope::{CacheView, largest_made_blob};
use crate::fetch::Cached;
use crate::page_blobs::PageBlobs;
use crate::page_bytes::MadeLedger;

use super::*;

const WIRE: &str = "brenn:site.bar.out";
const LOCAL: &str = "local:page/notes";
const UNWIRED: &str = "spare";
const MAX_BODY: u64 = 4_096;

fn spec(channel: &str) -> OutputSpec {
    OutputSpec {
        channel: channel.to_string(),
        default_urgency: Urgency::Normal,
        non_text: false,
    }
}

/// `out` on a wire channel, `notes` on a page-local one, and [`UNWIRED`].
fn buffer() -> PublishBuffer {
    buffer_capped(MAX_BODY)
}

/// [`buffer`] with a body cap of `max_body`.
fn buffer_capped(max_body: u64) -> PublishBuffer {
    let outputs = HashMap::from([
        ("out".to_string(), spec(WIRE)),
        ("notes".to_string(), spec(LOCAL)),
    ]);
    let sink_mt = HashMap::from([
        ("out".to_string(), 1_000_000),
        ("notes".to_string(), 1_000_000),
    ]);
    let vocabulary = ["out", "notes", UNWIRED]
        .into_iter()
        .map(str::to_string)
        .collect();
    PublishBuffer::new(
        "maker",
        outputs,
        Arc::new(vocabulary),
        sink_mt,
        max_body,
        HashMap::new(),
    )
}

fn door(upload: bool) -> BlobFacts {
    BlobFacts {
        max_blob_bytes: 64 << 20,
        max_chunk_bytes: 256 << 10,
        max_handles: 64,
        max_bytes_held: 256 << 20,
        max_inflight_reads: 4,
        upload,
    }
}

fn text(body: &str) -> GuestBody {
    GuestBody::Text(body.to_string())
}

/// A body that does not build: untyped and not UTF-8.
fn malformed() -> GuestBody {
    GuestBody::Parts {
        content_type: None,
        bytes: vec![0xff],
    }
}

/// A well-formed body that is not text.
fn binary() -> GuestBody {
    GuestBody::Parts {
        content_type: Some("application/octet-stream".to_string()),
        bytes: vec![0x00, 0xff],
    }
}

struct Absent;

impl CacheView for Absent {
    fn cached(&self, _channel: &str, _message_id: Uuid, _name: &AttachmentName) -> Cached<'_> {
        Cached::Absent
    }
}

/// A scope with a wire window naming one backend blob, and the ledger its page
/// bytes are counted in.
struct Maker {
    scope: BlobScope,
    ledger: MadeLedger,
}

impl Maker {
    fn new() -> Self {
        let mut scope = BlobScope::empty();
        let envelope = MessageEnvelope {
            message_id: Uuid::from_u128(1),
            source: "test".into(),
            channel: "brenn:site.bar.in".into(),
            sender: "app:cam".into(),
            publish_ts: chrono::DateTime::from_timestamp(0, 0).expect("a representable instant"),
            body: "m".into(),
            reply_to: None,
            delivery_deadline: None,
            deliver_after: None,
            impetus: None,
            urgency: Urgency::Normal,
            envelope_type: ChannelScheme::Brenn,
            transport: None,
            attachments: Attachments::from_map(
                [(
                    AttachmentName::new("photo").unwrap(),
                    BlobRef {
                        id: BlobId::new("0123abcd".to_string()),
                        size: 1_000,
                        content_type: "image/png".to_string(),
                    },
                )]
                .into_iter()
                .collect(),
            ),
        };
        scope.add_window("in", "brenn:site.bar.in".into(), &[envelope]);
        Self {
            scope,
            ledger: MadeLedger::new(),
        }
    }

    /// A sealed blob of `len` bytes.
    fn made(&mut self, len: usize) -> u32 {
        let writer = self
            .scope
            .create(&self.ledger, "application/octet-stream")
            .unwrap();
        self.scope
            .write(writer, 0, &vec![7; len], largest_made_blob(None))
            .unwrap()
            .unwrap();
        self.scope.seal(writer).unwrap().unwrap()
    }

    /// The backend blob the wire window delivered.
    fn forwarded(&mut self) -> u32 {
        self.scope
            .open(&Absent, &PageBlobs::new(), "in", 0, "photo")
            .unwrap()
    }

    /// An open writer.
    fn writer(&mut self) -> u32 {
        self.scope.create(&self.ledger, "text/plain").unwrap()
    }
}

fn named(pairs: &[(&str, u32)]) -> Vec<(String, u32)> {
    pairs.iter().map(|&(n, h)| (n.to_string(), h)).collect()
}

/// Publish `body` from `port` with `attachments` under `door`.
fn publish(
    buf: &mut PublishBuffer,
    maker: &Maker,
    door: Option<&BlobFacts>,
    port: &str,
    body: GuestBody,
    attachments: &[(&str, u32)],
) -> Result<Result<(), BlobsPublishFault>, UnknownHandle> {
    buf.publish_attached(
        &maker.scope,
        GrantVerdict::Held,
        door,
        port,
        body,
        &named(attachments),
        None,
        None,
    )
}

fn refused(error: BlobsPublishError) -> Result<Result<(), BlobsPublishFault>, UnknownHandle> {
    Ok(Err(PortFault::Refused(error)))
}

fn plain(error: PublishError) -> Result<Result<(), BlobsPublishFault>, UnknownHandle> {
    refused(BlobsPublishError::Plain(error))
}

#[test]
fn an_accepted_publish_holds_its_blobs_under_their_names_in_call_order() {
    let mut maker = Maker::new();
    let blob = maker.made(3);
    let forwarded = maker.forwarded();
    let mut buf = buffer();
    let up = door(true);
    assert_eq!(
        publish(
            &mut buf,
            &maker,
            Some(&up),
            "out",
            text("x"),
            &[("b", blob), ("a", forwarded), ("again", blob)]
        ),
        Ok(Ok(()))
    );
    let entry = &buf.take().publishes[0];
    let names: Vec<&str> = entry.attachments.iter().map(|a| a.name.as_str()).collect();
    assert_eq!(names, ["b", "a", "again"]);
    assert_eq!(&entry.attachments[0].blob, maker.scope.blob(blob).unwrap());
    assert_eq!(
        entry.attachments[0].blob, entry.attachments[2].blob,
        "one blob, two names"
    );
    assert_eq!(
        &entry.attachments[1].blob,
        maker.scope.blob(forwarded).unwrap()
    );
}

#[test]
fn ports_and_blobs_publishes_share_one_call_order() {
    let mut maker = Maker::new();
    let blob = maker.made(1);
    let mut buf = buffer();
    let up = door(true);
    buf.publish("notes", text("1")).unwrap();
    publish(
        &mut buf,
        &maker,
        Some(&up),
        "notes",
        text("2"),
        &[("a", blob)],
    )
    .unwrap()
    .unwrap();
    buf.publish("out", text("3")).unwrap();
    publish(
        &mut buf,
        &maker,
        Some(&up),
        "out",
        text("4"),
        &[("a", blob)],
    )
    .unwrap()
    .unwrap();
    publish(&mut buf, &maker, Some(&up), "notes", text("5"), &[])
        .unwrap()
        .unwrap();
    let order: Vec<(String, usize)> = buf
        .take()
        .publishes
        .iter()
        .map(|p| (p.body.as_text().unwrap().to_string(), p.attachments.len()))
        .collect();
    assert_eq!(
        order,
        [
            ("1".to_string(), 0),
            ("2".to_string(), 1),
            ("3".to_string(), 0),
            ("4".to_string(), 1),
            ("5".to_string(), 0)
        ]
    );
}

#[test]
fn the_call_budget_is_charged_before_the_port_is_judged() {
    let maker = Maker::new();
    let mut buf = buffer();
    while buf.admit_call(text("x")).is_ok() {}
    assert_eq!(
        publish(&mut buf, &maker, None, "ghost", text("x"), &[]),
        plain(PublishError::QuotaExceeded)
    );
}

/// Publish as a caller the core found missing a grant, under a door that
/// uploads, so no refusal below the grant could come from the door.
fn ungranted(
    buf: &mut PublishBuffer,
    maker: &Maker,
    port: &str,
    body: GuestBody,
    attachments: &[(&str, u32)],
) -> Result<Result<(), BlobsPublishFault>, UnknownHandle> {
    buf.publish_attached(
        &maker.scope,
        GrantVerdict::Missing,
        Some(&door(true)),
        port,
        body,
        &named(attachments),
        None,
        None,
    )
}

#[test]
fn an_ungranted_call_is_charged_before_its_grant_is_answered() {
    let mut maker = Maker::new();
    let blob = maker.made(1);
    let mut buf = buffer();
    for call in 0..brenn_budget::MAX_PUBLISH_CALLS_PER_ACTIVATION {
        assert_eq!(
            ungranted(&mut buf, &maker, "out", text("x"), &[("a", blob)]),
            plain(PublishError::NotPermitted),
            "call {call}"
        );
    }
    for _ in 0..2 {
        assert_eq!(
            ungranted(&mut buf, &maker, "out", text("x"), &[("a", blob)]),
            plain(PublishError::QuotaExceeded)
        );
    }
    assert_eq!(
        buf.publish("out", text("x")),
        Err(PortFault::Refused(PublishError::QuotaExceeded)),
        "the refusals spent the count `ports.publish` draws on"
    );
    assert_eq!(
        publish(
            &mut buf,
            &maker,
            Some(&door(true)),
            "out",
            text("x"),
            &[("a", blob)]
        ),
        plain(PublishError::QuotaExceeded),
        "and the count a granted `blobs.publish` draws on"
    );
    assert!(buf.take().publishes.is_empty());
}

#[test]
fn ungranted_refusals_bring_the_shared_call_ceiling_nearer() {
    let mut maker = Maker::new();
    let blob = maker.made(1);
    let mut buf = buffer();
    let refusals = 10;
    for _ in 0..refusals {
        assert_eq!(
            ungranted(&mut buf, &maker, "out", text("x"), &[("a", blob)]),
            plain(PublishError::NotPermitted)
        );
    }
    assert_eq!(
        buf.publish("out", text("x")),
        Ok(()),
        "short of the ceiling"
    );
    // Calls refused for their body spend a call and buffer nothing, so the
    // ceiling is reached by count alone.
    for call in 0..brenn_budget::MAX_PUBLISH_CALLS_PER_ACTIVATION - refusals - 1 {
        assert_eq!(
            buf.publish("out", binary()),
            Err(PortFault::Refused(PublishError::TextOnlyChannel)),
            "call {call}"
        );
    }
    assert_eq!(
        buf.publish("out", binary()),
        Err(PortFault::Refused(PublishError::QuotaExceeded)),
        "the ceiling, {refusals} calls sooner"
    );
    assert_eq!(buf.take().publishes.len(), 1);
}

#[test]
fn an_ungranted_call_is_not_permitted_before_its_port_is_judged() {
    let mut maker = Maker::new();
    let blob = maker.made(1);
    let mut buf = buffer();
    assert_eq!(
        ungranted(&mut buf, &maker, "ghost", text("x"), &[("a", blob)]),
        plain(PublishError::NotPermitted),
        "an undeclared port does not trap"
    );
    assert_eq!(
        ungranted(&mut buf, &maker, UNWIRED, text("x"), &[("a", blob)]),
        plain(PublishError::NotPermitted),
        "a declared, unwired port is not answered `ok`"
    );
    assert_eq!(buf.dropped(), 0);
    assert!(buf.take().publishes.is_empty());
}

#[test]
fn an_ungranted_call_is_not_permitted_whatever_its_arguments() {
    let mut maker = Maker::new();
    let blob = maker.made(1);
    let writer = maker.writer();
    let mut buf = buffer();
    for port in ["out", "notes"] {
        assert_eq!(
            ungranted(&mut buf, &maker, port, malformed(), &[("a", blob)]),
            plain(PublishError::NotPermitted),
            "a malformed body on {port}"
        );
        assert_eq!(
            ungranted(&mut buf, &maker, port, text("x"), &[("!", blob)]),
            plain(PublishError::NotPermitted),
            "a bad name on {port}"
        );
        assert_eq!(
            ungranted(
                &mut buf,
                &maker,
                port,
                text("x"),
                &[("a", blob), ("a", blob)]
            ),
            plain(PublishError::NotPermitted),
            "a duplicate name on {port}"
        );
        for handle in [99, writer] {
            assert_eq!(
                ungranted(&mut buf, &maker, port, text("x"), &[("a", handle)]),
                plain(PublishError::NotPermitted),
                "handle {handle}, which the scope does not hold as a blob, on {port}"
            );
        }
    }
    assert!(buf.take().publishes.is_empty());
}

/// A grant judgement that refused, standing in for the core's report.
const MISSING: Result<(), &str> = Err("ungranted");
const HELD: Result<(), &str> = Ok(());

/// `answer` as the export sees it: the buffer's answer, in flight.
fn in_flight(answer: Result<Result<(), BlobsPublishFault>, UnknownHandle>) -> BlobsPublishAnswer {
    Some(answer.map(|admitted| admitted.map(|()| Admission::Buffered)))
}

#[test]
fn the_verdict_handed_to_the_buffer_is_held_only_when_no_grant_is_refused() {
    assert_eq!(GrantVerdict::of(&HELD), GrantVerdict::Held);
    assert_eq!(GrantVerdict::of(&MISSING), GrantVerdict::Missing);
}

#[test]
fn a_missing_grant_is_reported_when_answered_not_permitted() {
    assert_eq!(
        ungranted_report("p", &MISSING, &in_flight(plain(PublishError::NotPermitted))),
        Some(&"ungranted")
    );
}

#[test]
fn a_missing_grant_is_reported_when_no_activation_is_in_flight() {
    assert_eq!(ungranted_report("p", &MISSING, &None), Some(&"ungranted"));
}

#[test]
fn a_missing_grant_is_not_reported_when_answered_quota_exceeded() {
    assert_eq!(
        ungranted_report(
            "p",
            &MISSING,
            &in_flight(plain(PublishError::QuotaExceeded))
        ),
        None
    );
}

#[test]
fn a_held_grant_reports_nothing_whatever_the_answer() {
    let answers = [
        None,
        in_flight(Ok(Ok(()))),
        Some(Ok(Ok(Admission::Dropped))),
        in_flight(plain(PublishError::NotPermitted)),
        in_flight(plain(PublishError::QuotaExceeded)),
        in_flight(plain(PublishError::InvalidBody("x".to_string()))),
        in_flight(Ok(Err(PortFault::Undeclared("ghost".to_string())))),
        in_flight(Err(UnknownHandle(7))),
    ];
    for answer in &answers {
        assert_eq!(ungranted_report("p", &HELD, answer), None, "{answer:?}");
    }
}

#[test]
#[should_panic(expected = "ungranted blobs.publish")]
fn a_missing_grant_answered_ok_panics() {
    ungranted_report("p", &MISSING, &in_flight(Ok(Ok(()))));
}

#[test]
#[should_panic(expected = "ungranted blobs.publish")]
fn a_missing_grant_answered_with_another_refusal_panics() {
    ungranted_report(
        "p",
        &MISSING,
        &in_flight(plain(PublishError::InvalidBody("x".to_string()))),
    );
}

#[test]
#[should_panic(expected = "ungranted blobs.publish")]
fn a_missing_grant_answered_with_an_unknown_handle_panics() {
    ungranted_report("p", &MISSING, &in_flight(Err(UnknownHandle(7))));
}

#[test]
fn an_ungranted_caller_is_reported_until_the_call_ceiling_and_not_after() {
    let mut maker = Maker::new();
    let blob = maker.made(1);
    let mut buf = buffer();
    let mut reports = 0;
    for _ in 0..brenn_budget::MAX_PUBLISH_CALLS_PER_ACTIVATION + 2 {
        let answer = in_flight(buf.publish_attached(
            &maker.scope,
            GrantVerdict::of(&MISSING),
            Some(&door(true)),
            "out",
            text("x"),
            &named(&[("a", blob)]),
            None,
            None,
        ));
        if ungranted_report("p", &MISSING, &answer).is_some() {
            reports += 1;
        }
    }
    assert_eq!(reports, brenn_budget::MAX_PUBLISH_CALLS_PER_ACTIVATION);
}

#[test]
fn an_undeclared_port_traps_before_the_door_is_asked() {
    let mut maker = Maker::new();
    let blob = maker.made(1);
    let mut buf = buffer();
    assert!(matches!(
        publish(&mut buf, &maker, None, "ghost", text("x"), &[("a", blob)]),
        Ok(Err(PortFault::Undeclared(_)))
    ));
}

#[test]
fn a_wire_bound_attachment_publish_needs_a_door_that_uploads() {
    let mut maker = Maker::new();
    let blob = maker.made(1);
    let forwarded = maker.forwarded();
    let mut buf = buffer();
    let read_only = door(false);
    for door in [None, Some(&read_only)] {
        for handle in [blob, forwarded] {
            assert_eq!(
                publish(&mut buf, &maker, door, "out", text("x"), &[("a", handle)]),
                plain(PublishError::NotPermitted)
            );
        }
        assert_eq!(
            publish(&mut buf, &maker, door, "out", text("x"), &[]),
            Ok(Ok(())),
            "a plain publish needs no door"
        );
        assert_eq!(
            publish(&mut buf, &maker, door, "notes", text("x"), &[("a", blob)]),
            Ok(Ok(())),
            "a page-local attachment needs no door"
        );
    }
}

#[test]
fn the_door_is_asked_before_the_body() {
    let mut maker = Maker::new();
    let blob = maker.made(1);
    let mut buf = buffer();
    assert_eq!(
        publish(&mut buf, &maker, None, "out", malformed(), &[("a", blob)]),
        plain(PublishError::NotPermitted)
    );
}

#[test]
fn the_body_is_judged_before_the_attachments() {
    let mut maker = Maker::new();
    let blob = maker.made(1);
    let mut buf = buffer();
    let up = door(true);
    assert!(matches!(
        publish(
            &mut buf,
            &maker,
            Some(&up),
            "out",
            malformed(),
            &[("!", blob)]
        ),
        Ok(Err(PortFault::Refused(BlobsPublishError::Plain(
            PublishError::InvalidBody(_)
        ))))
    ));
    assert_eq!(
        publish(&mut buf, &maker, Some(&up), "out", binary(), &[("!", blob)]),
        plain(PublishError::TextOnlyChannel),
        "carriage before the names"
    );
}

#[test]
fn the_attachment_names_are_judged_count_then_grammar_then_duplicates() {
    let mut maker = Maker::new();
    let blob = maker.made(1);
    let mut buf = buffer();
    let up = door(true);
    let too_many: Vec<(String, u32)> = (0..=MAX_ATTACHMENTS_PER_MESSAGE)
        .map(|_| ("!".to_string(), blob))
        .collect();
    assert_eq!(
        buf.publish_attached(
            &maker.scope,
            GrantVerdict::Held,
            Some(&up),
            "out",
            text("x"),
            &too_many,
            None,
            None
        ),
        refused(BlobsPublishError::TooManyAttachments)
    );
    assert!(matches!(
        publish(
            &mut buf,
            &maker,
            Some(&up),
            "out",
            text("x"),
            &[("!", blob), ("!", blob)]
        ),
        Ok(Err(PortFault::Refused(
            BlobsPublishError::InvalidAttachmentName(_)
        )))
    ));
    assert_eq!(
        publish(
            &mut buf,
            &maker,
            Some(&up),
            "out",
            text("x"),
            &[("a", blob), ("a", blob)]
        ),
        refused(BlobsPublishError::DuplicateAttachmentName("a".to_string()))
    );
}

#[test]
fn the_names_are_judged_before_the_handles() {
    let maker = Maker::new();
    let mut buf = buffer();
    let up = door(true);
    assert!(matches!(
        publish(&mut buf, &maker, Some(&up), "out", text("x"), &[("!", 99)]),
        Ok(Err(PortFault::Refused(
            BlobsPublishError::InvalidAttachmentName(_)
        )))
    ));
}

#[test]
fn a_handle_that_is_not_a_blob_traps_before_the_size_cap() {
    let mut maker = Maker::new();
    let writer = maker.writer();
    let mut buf = buffer();
    let up = door(true);
    let oversize = text(&"x".repeat(MAX_BODY as usize + 1));
    assert_eq!(
        publish(&mut buf, &maker, Some(&up), "out", oversize, &[("a", 99)]),
        Err(UnknownHandle(99))
    );
    assert_eq!(
        publish(
            &mut buf,
            &maker,
            Some(&up),
            "out",
            text("x"),
            &[("a", writer)]
        ),
        Err(UnknownHandle(writer)),
        "a writer is not a blob"
    );
    let sealed_from = maker.writer();
    maker.scope.seal(sealed_from).unwrap().unwrap();
    assert_eq!(
        publish(
            &mut buf,
            &maker,
            Some(&up),
            "notes",
            text("x"),
            &[("a", sealed_from)]
        ),
        Err(UnknownHandle(sealed_from)),
        "nor is a spent writer"
    );
}

#[test]
fn the_body_cap_answers_before_the_per_flush_blob_bounds() {
    let mut maker = Maker::new();
    let blob = maker.made(10);
    let mut buf = buffer();
    let tight = BlobFacts {
        max_bytes_held: 1,
        ..door(true)
    };
    let oversize = text(&"x".repeat(MAX_BODY as usize + 1));
    assert!(matches!(
        publish(
            &mut buf,
            &maker,
            Some(&tight),
            "out",
            oversize,
            &[("a", blob)]
        ),
        Ok(Err(PortFault::Refused(BlobsPublishError::Plain(
            PublishError::InvalidPayload(_)
        ))))
    ));
    assert_eq!(
        publish(
            &mut buf,
            &maker,
            Some(&tight),
            "out",
            text("x"),
            &[("a", blob)]
        ),
        plain(PublishError::QuotaExceeded)
    );
}

#[test]
fn the_flushs_distinct_blobs_are_bounded_by_the_doors_handles() {
    let mut maker = Maker::new();
    let first = maker.made(1);
    let second = maker.made(1);
    let third = maker.made(1);
    let mut buf = buffer();
    let two = BlobFacts {
        max_handles: 2,
        ..door(true)
    };
    assert_eq!(
        publish(
            &mut buf,
            &maker,
            Some(&two),
            "out",
            text("1"),
            &[("a", first)]
        ),
        Ok(Ok(()))
    );
    assert_eq!(
        publish(
            &mut buf,
            &maker,
            Some(&two),
            "out",
            text("2"),
            &[("a", second), ("b", first)]
        ),
        Ok(Ok(())),
        "a blob already named counts once"
    );
    assert_eq!(
        publish(
            &mut buf,
            &maker,
            Some(&two),
            "out",
            text("3"),
            &[("a", third)]
        ),
        plain(PublishError::QuotaExceeded)
    );
    assert_eq!(
        publish(
            &mut buf,
            &maker,
            Some(&two),
            "notes",
            text("4"),
            &[("a", third)]
        ),
        Ok(Ok(())),
        "a page-local publish is not part of the wire flush"
    );
}

#[test]
fn the_flushs_distinct_blobs_are_bounded_by_the_page_without_a_door_bound() {
    let mut maker = Maker::new();
    let handles: Vec<u32> = (0..=SURFACE_MAX_FLUSH_BLOBS)
        .map(|_| maker.made(1))
        .collect();
    let mut buf = buffer();
    let up = door(true);
    for (i, &handle) in handles.iter().enumerate() {
        let answer = publish(
            &mut buf,
            &maker,
            Some(&up),
            "out",
            text("x"),
            &[("a", handle)],
        );
        if i < SURFACE_MAX_FLUSH_BLOBS {
            assert_eq!(answer, Ok(Ok(())), "blob {i}");
        } else {
            assert_eq!(answer, plain(PublishError::QuotaExceeded));
        }
    }
}

#[test]
fn the_flushs_blob_bytes_are_bounded_by_the_doors_bytes_held() {
    let mut maker = Maker::new();
    let made = maker.made(600);
    let forwarded = maker.forwarded();
    let mut buf = buffer();
    let held = BlobFacts {
        max_bytes_held: 1_599,
        ..door(true)
    };
    assert_eq!(
        publish(
            &mut buf,
            &maker,
            Some(&held),
            "out",
            text("x"),
            &[("a", made)]
        ),
        Ok(Ok(()))
    );
    assert_eq!(
        publish(
            &mut buf,
            &maker,
            Some(&held),
            "out",
            text("x"),
            &[("a", forwarded)]
        ),
        plain(PublishError::QuotaExceeded),
        "600 + 1000 bytes pass 1599"
    );
}

#[test]
fn the_batch_charge_equals_the_codecs_for_entries_with_attachments() {
    let mut maker = Maker::new();
    let blob = maker.made(1);
    let mut buf = buffer();
    let up = door(true);
    publish(
        &mut buf,
        &maker,
        Some(&up),
        "out",
        text("one"),
        &[("photo", blob), ("x", blob)],
    )
    .unwrap()
    .unwrap();
    buf.publish("out", text("two")).unwrap();
    publish(
        &mut buf,
        &maker,
        Some(&up),
        "notes",
        text("local"),
        &[("photo", blob)],
    )
    .unwrap()
    .unwrap();
    let charged = buf.batch_charge();
    let entries: Vec<BatchEntry> = buf
        .take()
        .publishes
        .into_iter()
        .filter(|p| channel_is_transportable(&p.channel))
        .map(|p| {
            let attachments = p
                .attachments
                .iter()
                .enumerate()
                .map(|(i, a)| PublishAttachment::new(a.name.clone(), BlobHandle(i as u64)))
                .collect();
            BatchEntry::new(p.channel, p.body, p.urgency).attachments(attachments)
        })
        .collect();
    assert_eq!(
        charged,
        Codec::negotiated(SURFACE_ATTACH_VERSION).batch_charged_bytes(&entries, &[])
    );
}

#[test]
fn an_attachment_overrunning_the_batch_budget_is_quota_exceeded() {
    let mut maker = Maker::new();
    let blob = maker.made(1);
    let mut buf = buffer_capped(1_024);
    let up = door(true);
    let plain_charge = wire_charge(WIRE, Some(&MessageBody::from("x".to_string())));
    let room = 1_024 - plain_charge;
    // Each attachment charges its name plus the codec's overhead, so enough
    // of them overrun a budget the body alone fits.
    let per = attachments_charge(&[BufferedAttachment {
        name: AttachmentName::new("a0").unwrap(),
        blob: maker.scope.blob(blob).unwrap().clone(),
    }]);
    let count = room / per + 1;
    assert!(
        count <= MAX_ATTACHMENTS_PER_MESSAGE,
        "the fixture's cap is small enough"
    );
    let attachments: Vec<(String, u32)> = (0..count).map(|i| (format!("a{i}"), blob)).collect();
    assert_eq!(
        buf.publish_attached(
            &maker.scope,
            GrantVerdict::Held,
            Some(&up),
            "out",
            text("x"),
            &attachments,
            None,
            None
        ),
        plain(PublishError::QuotaExceeded)
    );
}

#[test]
fn an_unwired_port_checks_the_names_and_handles_and_drops_the_message() {
    let mut maker = Maker::new();
    let blob = maker.made(1);
    let mut buf = buffer();
    assert_eq!(
        publish(&mut buf, &maker, None, UNWIRED, text("x"), &[("a", blob)]),
        Ok(Ok(())),
        "no door is asked of a port with no channel"
    );
    assert_eq!(buf.dropped(), 1);
    assert_eq!(
        publish(
            &mut buf,
            &maker,
            None,
            UNWIRED,
            text("x"),
            &[("a", blob), ("a", blob)]
        ),
        refused(BlobsPublishError::DuplicateAttachmentName("a".to_string()))
    );
    assert_eq!(
        publish(&mut buf, &maker, None, UNWIRED, text("x"), &[("a", 99)]),
        Err(UnknownHandle(99))
    );
    assert!(buf.take().publishes.is_empty());
}
