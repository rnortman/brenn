//! An attachment flush through whole turns: staged by the completion that made
//! it, uploaded, handed to the outbox, answered, lost, and carried across a
//! detach, with the frames and announcements each step leaves.

use std::collections::{BTreeMap, HashMap};

use brenn_attach_client::Millis;
use brenn_attach_client::blobs::{OpenRefusal, UploadRefusal};
use brenn_attach_client::conn::{AttachmentFacts, ConnEvent, DetachReason};
use brenn_attach_client::router::MessageStamp;
use brenn_attach_proto::{
    BlobCreateOutcome, BlobFacts, BlobHandle, BlobOpenOutcome, BlobSealOutcome, ClientFrame,
    PublishBatchOutcome, ServerFrame,
};
use brenn_envelope::grants::ComponentGrant;
use brenn_envelope::{
    AttachmentName, Attachments, BlobId, BlobRef, ChannelScheme, GuestBody, MessageEnvelope,
    Urgency,
};
use brenn_surface_schema::bindings::BindingsDocument;
use brenn_surface_schema::{LOCAL_TOAST_CHANNEL, LogLevel};
use uuid::Uuid;

use brenn_surface_contract::ActivationError;

use crate::activation::ActivationOutcome;
use crate::blob_scope::{Blob, BlobScope, CacheView, Remote, largest_made_blob};
use crate::fetch::Cached;
use crate::outward::Completed;
use crate::page::SurfacePage;
use crate::page_blobs::PageBlobs;
use crate::page_bytes::MadeLedger;
use crate::publish_buffer::{OutputSpec, PublishBuffer};
use crate::session::{Effect, Event, Reactions};
use crate::test_support::bindings as fixtures;
use crate::test_support::bindings::output;
use crate::test_support::pages;
use crate::turn::{Input, on_input};
use crate::uploads::{LossReason, Lost};

const CONFIG: &str = "ephemeral:site.surface.bar.bindings";
const OUT: &str = "ephemeral:site.bar.out";
const OTHER: &str = "ephemeral:site.bar.other";
const ERRORS: &str = "brenn:site.errors";
const NOTES: &str = "local:app/notes";
/// A wire channel whose messages carry the attachments `p1` forwards.
const ORIGIN: &str = "ephemeral:site.bar.in";
const EPOCH: Uuid = Uuid::from_u128(0x0b10);
const NOW: Millis = Millis(1_000);
const NOW_MS: u64 = 1_000;

fn door() -> BlobFacts {
    BlobFacts {
        max_blob_bytes: 1 << 20,
        max_chunk_bytes: 64 * 1024,
        max_handles: 64,
        max_bytes_held: 4 << 20,
        max_inflight_reads: 4,
        upload: true,
    }
}

fn facts() -> AttachmentFacts {
    AttachmentFacts {
        blobs: Some(door()),
        ..pages::facts()
    }
}

/// `p1` makes blobs, writes `OUT` and the page-local `NOTES`, and reads `ORIGIN`
/// on `feed`; `p2` also holds `blobs`, so the server's door uploads whatever
/// `p1` is granted. `p1_blobs` and `p1_out` vary what the document says of `p1`.
fn doc(p1_blobs: bool, p1_out: &str) -> BindingsDocument {
    doc_feeding(p1_blobs, p1_out, true)
}

/// [`doc`], with `p1`'s `feed` binding on `ORIGIN` only when `feeds`.
fn doc_feeding(p1_blobs: bool, p1_out: &str, feeds: bool) -> BindingsDocument {
    let p1_grants: &[ComponentGrant] = if p1_blobs {
        &[ComponentGrant::Ports, ComponentGrant::Blobs]
    } else {
        &[ComponentGrant::Ports]
    };
    let mut doc = fixtures::doc(
        vec![
            fixtures::component_with_grants("p1", "protobar", p1_grants),
            fixtures::component_with_grants(
                "p2",
                "protobar",
                &[ComponentGrant::Ports, ComponentGrant::Blobs],
            ),
            fixtures::component(fixtures::CHROME),
        ],
        if feeds {
            vec![fixtures::subscription("p1", "feed", ORIGIN, 0, 1)]
        } else {
            vec![]
        },
        vec![
            output("p1", "out", p1_out),
            output("p1", "notes", NOTES),
            output("p2", "out", OTHER),
        ],
        vec![fixtures::local(NOTES, 1)],
    );
    doc.platform.error_channel = Some(ERRORS.to_string());
    doc.platform.error_report_floor = Some(LogLevel::Debug);
    doc
}

fn page() -> SurfacePage {
    pages::configured_page(
        CONFIG,
        EPOCH,
        facts(),
        &["p1", "p2", fixtures::CHROME],
        &doc(true, OUT),
        NOW,
    )
}

fn feed(page: &mut SurfacePage, input: Input) -> Vec<Effect> {
    on_input(page, input, NOW, NOW_MS)
}

fn frames(effects: &[Effect]) -> Vec<&ClientFrame> {
    effects
        .iter()
        .filter_map(|effect| match effect {
            Effect::SendFrame(frame) => Some(frame),
            _ => None,
        })
        .collect()
}

fn lost(effects: &[Effect]) -> Vec<LossReason> {
    effects
        .iter()
        .filter_map(|effect| match effect {
            Effect::EmitEvent(Event::FlushLost(lost)) => Some(lost.reason),
            _ => None,
        })
        .collect()
}

fn toasts(effects: &[Effect]) -> usize {
    effects
        .iter()
        .filter(|effect| {
            matches!(effect, Effect::PublishControl { channel, .. } if channel == LOCAL_TOAST_CHANNEL)
        })
        .count()
}

fn is_upload_or_batch(frame: &ClientFrame) -> bool {
    matches!(
        frame,
        ClientFrame::BlobCreate { .. }
            | ClientFrame::BlobWrite { .. }
            | ClientFrame::BlobSeal { .. }
            | ClientFrame::BlobOpen { .. }
            | ClientFrame::PublishBatch { .. }
    )
}

/// `p1`'s buffer, seeded for its wiring under the page's document.
fn buffer(page: &SurfacePage, instance: &str) -> PublishBuffer {
    let bindings = page.bindings().expect("the fixture page is configured");
    let mut outputs = HashMap::new();
    let mut sink_mt = HashMap::new();
    for binding in bindings.outputs_of(instance) {
        outputs.insert(
            binding.port.clone(),
            OutputSpec {
                channel: binding.channel.clone(),
                default_urgency: binding.urgency,
                non_text: bindings.carries_non_text(&binding.channel),
            },
        );
        sink_mt.insert(binding.port.clone(), 1_000_000);
    }
    PublishBuffer::new(
        instance,
        outputs,
        bindings.declared_out_ports(instance),
        sink_mt,
        4_096,
        HashMap::new(),
    )
}

/// `p1`'s buffer holding one publish on `out` naming a blob of `len` bytes made
/// in its activation, counted in `ledger`.
fn attachment_buffer(page: &SurfacePage, ledger: &MadeLedger, len: usize) -> PublishBuffer {
    let mut scope = BlobScope::empty();
    let writer = scope
        .create(ledger, "application/octet-stream")
        .expect("room for the blob");
    let bytes: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
    scope
        .write(writer, 0, &bytes, largest_made_blob(Some(&door())))
        .expect("a live writer")
        .expect("the write fits");
    let blob = scope.seal(writer).expect("a live writer").expect("no gap");
    let mut buffer = buffer(page, "p1");
    buffer
        .publish_attached(
            &scope,
            crate::publish_buffer::GrantVerdict::Held,
            Some(&door()),
            "out",
            GuestBody::Text("with a blob".to_string()),
            &[("pic".to_string(), blob), ("again".to_string(), blob)],
            None,
            None,
        )
        .expect("the handle is the activation's")
        .expect("the publish is admitted");
    buffer
}

fn plain_buffer(page: &SurfacePage) -> PublishBuffer {
    let mut buffer = buffer(page, "p1");
    buffer
        .publish("out", GuestBody::Text("plain".to_string()))
        .expect("bound");
    buffer
}

fn complete(
    page: &mut SurfacePage,
    buffer: PublishBuffer,
    outcome: ActivationOutcome<GuestBody>,
) -> Vec<Effect> {
    complete_as(page, "p1", buffer, outcome)
}

/// [`complete`] for `instance`'s activation.
fn complete_as(
    page: &mut SurfacePage,
    instance: &str,
    buffer: PublishBuffer,
    outcome: ActivationOutcome<GuestBody>,
) -> Vec<Effect> {
    let generation = page
        .registrations
        .generation(instance)
        .expect("the instance is mounted");
    let stamps = (0..buffer.len())
        .map(|i| MessageStamp {
            message_id: Uuid::from_u128(0x5000 + i as u128),
            publish_ts: chrono::DateTime::from_timestamp(0, 0).expect("an instant"),
        })
        .collect();
    feed(
        page,
        Input::ActivationDone(Box::new(Completed {
            instance: instance.to_string(),
            generation,
            outcome,
            buffer,
            stamps,
        })),
    )
}

/// [`complete`] with a buffer `make` builds from the page.
fn complete_with(
    page: &mut SurfacePage,
    make: impl FnOnce(&SurfacePage) -> PublishBuffer,
    outcome: ActivationOutcome<GuestBody>,
) -> Vec<Effect> {
    let buffer = make(page);
    complete(page, buffer, outcome)
}

/// Every chunk of every upload in progress, each upload drained in turn as
/// the runner drains one.
fn drain(page: &mut SurfacePage) -> Vec<ClientFrame> {
    let mut sent = Vec::new();
    let SurfacePage { stage, plane, .. } = page;
    for handle in stage.uploading_handles() {
        while let Some(frames) = stage.next_chunk_frames(handle, plane) {
            sent.extend(frames);
        }
    }
    sent
}

fn created(handle: BlobHandle, outcome: BlobCreateOutcome) -> Input {
    Input::Frame(ServerFrame::BlobCreateResult { handle, outcome })
}

fn sealed(handle: BlobHandle) -> Input {
    Input::Frame(ServerFrame::BlobSealResult {
        handle,
        outcome: BlobSealOutcome::Ok,
    })
}

/// Complete an attachment publish of `len` bytes and upload it until the
/// batch is handed over; answers the handle and the batch's correlation.
fn to_handed(page: &mut SurfacePage, ledger: &MadeLedger, len: usize) -> (BlobHandle, u64) {
    let buffer = attachment_buffer(page, ledger, len);
    let effects = complete(page, buffer, ActivationOutcome::Ok(None));
    let [ClientFrame::BlobCreate { handle, size, .. }] = frames(&effects)[..] else {
        panic!("one create for one blob named twice: {effects:?}")
    };
    assert_eq!(*size, len as u64);
    let create = effects
        .iter()
        .position(|effect| matches!(effect, Effect::SendFrame(ClientFrame::BlobCreate { .. })))
        .expect("the create");
    assert_eq!(effects.get(create + 1), Some(&Effect::DrainUpload(*handle)));
    let handle = *handle;
    let chunks = drain(page);
    let written: u64 = chunks
        .iter()
        .map(|frame| match frame {
            ClientFrame::BlobWrite { bytes, .. } => bytes.len() as u64,
            _ => 0,
        })
        .sum();
    assert_eq!(written, len as u64);
    assert!(matches!(chunks.last(), Some(ClientFrame::BlobSeal { .. })));
    assert!(frames(&feed(page, created(handle, BlobCreateOutcome::Ok))).is_empty());
    let effects = feed(page, sealed(handle));
    let [
        ClientFrame::PublishBatch {
            correlation,
            publishes,
            ..
        },
    ] = frames(&effects)[..]
    else {
        panic!("the batch goes out once the blob is sealed: {effects:?}")
    };
    let named: Vec<(String, BlobHandle)> = publishes[0]
        .attachments
        .iter()
        .map(|attachment| (attachment.name.as_str().to_string(), attachment.handle))
        .collect();
    assert_eq!(
        named,
        vec![("pic".to_string(), handle), ("again".to_string(), handle)]
    );
    (handle, *correlation)
}

#[test]
fn a_made_blob_is_uploaded_then_named_by_the_batch_and_closed_once_applied() {
    let mut page = page();
    let ledger = MadeLedger::new();
    let (handle, correlation) = to_handed(&mut page, &ledger, 100_000);
    assert_eq!(
        ledger.held(),
        100_000,
        "the stage holds the bytes until applied"
    );

    let effects = feed(
        &mut page,
        Input::Frame(ServerFrame::PublishBatchResult {
            correlation,
            outcome: PublishBatchOutcome::Ok,
        }),
    );

    assert_eq!(frames(&effects), vec![&ClientFrame::BlobClose { handle }]);
    assert_eq!(ledger.held(), 0);
    assert_eq!(page.plane.handles_in_use(), 0);
    assert!(!page.stage.holds("p1"));
}

#[test]
fn a_refused_create_loses_the_flush_announces_it_and_the_next_publish_goes_out() {
    let mut page = page();
    let ledger = MadeLedger::new();
    let effects = complete_with(
        &mut page,
        |page| attachment_buffer(page, &ledger, 10),
        ActivationOutcome::Ok(None),
    );
    let [ClientFrame::BlobCreate { handle, .. }] = frames(&effects)[..] else {
        panic!("one create: {effects:?}")
    };
    let handle = *handle;
    drain(&mut page);

    let effects = feed(
        &mut page,
        created(handle, BlobCreateOutcome::BudgetExhausted),
    );

    assert_eq!(
        lost(&effects),
        vec![LossReason::Upload(
            brenn_attach_client::blobs::UploadRefusal::BudgetExhausted
        )]
    );
    assert_eq!(toasts(&effects), 1);
    assert!(
        !frames(&effects)
            .iter()
            .any(|frame| matches!(frame, ClientFrame::PublishBatch { .. }))
    );
    assert_eq!(ledger.held(), 0);
    assert!(!page.stage.holds("p1"));

    let effects = complete_with(&mut page, plain_buffer, ActivationOutcome::Ok(None));
    assert!(matches!(
        frames(&effects)[..],
        [ClientFrame::PublishBatch { .. }]
    ));
}

#[test]
fn an_activation_that_made_and_published_a_blob_and_then_failed_sends_nothing() {
    for outcome in [
        ActivationOutcome::Err(ActivationError {
            message: "no".to_string(),
        }),
        ActivationOutcome::Trap("boom".to_string()),
    ] {
        let mut page = page();
        let ledger = MadeLedger::new();
        let buffer = attachment_buffer(&page, &ledger, 64);
        let effects = complete(&mut page, buffer, outcome);
        assert!(!frames(&effects).into_iter().any(is_upload_or_batch));
        assert!(drain(&mut page).is_empty());
        assert!(!page.stage.holds("p1"));
        assert_eq!(page.plane.handles_in_use(), 0);
        assert_eq!(ledger.held(), 0);
        // Nothing reaches the wire later either.
        let effects = feed(&mut page, Input::RetryDue);
        assert!(!frames(&effects).into_iter().any(is_upload_or_batch));
    }
}

/// Detach, then attach again and put `document` in force, folding phase 2
/// through the session as an inbound document would be.
fn reattach(page: &mut SurfacePage, document: &BindingsDocument) -> Vec<Effect> {
    reattach_to(page, document, facts())
}

/// [`reattach`] to an attachment advertising `facts`.
fn reattach_to(
    page: &mut SurfacePage,
    document: &BindingsDocument,
    facts: AttachmentFacts,
) -> Vec<Effect> {
    let mut reactions = Reactions::new();
    reactions.conn_event(page, ConnEvent::Attached(facts));
    page.subs
        .on_subscribe_result(CONFIG, brenn_attach_proto::SubscribeOutcome::Ok, 1, None)
        .expect("the config channel is pending");
    let configured = page
        .apply_config(&document.to_body(), NOW)
        .expect("the document applies");
    reactions.configured(page, configured, NOW, NOW_MS);
    reactions.end_turn(page);
    reactions.into_effects()
}

fn detach(page: &mut SurfacePage) -> Vec<Effect> {
    feed(
        page,
        Input::Conn(ConnEvent::Detached {
            reason: DetachReason::LivenessTimeout,
        }),
    )
}

#[test]
fn a_batch_on_the_wire_at_a_detach_is_lost_never_resent_and_counted_after_reattach() {
    let mut page = page();
    let ledger = MadeLedger::new();
    to_handed(&mut page, &ledger, 10);

    let effects = detach(&mut page);
    assert_eq!(lost(&effects), vec![LossReason::Unanswered]);
    assert_eq!(toasts(&effects), 1);
    assert_eq!(ledger.held(), 0);

    let effects = reattach(&mut page, &doc(true, OUT));
    let sent = frames(&effects);
    assert!(!sent.iter().any(|frame| is_upload_or_batch(frame)));
    let reports: Vec<&ClientFrame> = sent
        .into_iter()
        .filter(|frame| matches!(frame, ClientFrame::Publish { channel, .. } if channel == ERRORS))
        .collect();
    assert_eq!(reports.len(), 1, "the count is reported once: {effects:?}");
    assert!(page.stage.take_unanswered().is_empty());
}

#[test]
fn an_upload_interrupted_by_a_detach_is_uploaded_again_after_reattach() {
    let mut page = page();
    let ledger = MadeLedger::new();
    let effects = complete_with(
        &mut page,
        |page| attachment_buffer(page, &ledger, 10),
        ActivationOutcome::Ok(None),
    );
    assert!(matches!(
        frames(&effects)[..],
        [ClientFrame::BlobCreate { .. }]
    ));
    let effects = detach(&mut page);
    assert!(lost(&effects).is_empty());
    assert_eq!(page.plane.handles_in_use(), 0);
    assert_eq!(ledger.held(), 10);

    let effects = reattach(&mut page, &doc(true, OUT));
    assert!(
        frames(&effects)
            .iter()
            .any(|frame| matches!(frame, ClientFrame::BlobCreate { size: 10, .. }))
    );
    assert!(
        effects
            .iter()
            .any(|effect| matches!(effect, Effect::DrainUpload(_)))
    );
}

/// A flush staged while detached, ahead of a plain one, meets `document` at
/// the next attachment.
fn staged_while_detached(document: &BindingsDocument) -> (Vec<Effect>, MadeLedger) {
    let mut page = page();
    detach(&mut page);
    let ledger = MadeLedger::new();
    let effects = complete_with(
        &mut page,
        |page| attachment_buffer(page, &ledger, 10),
        ActivationOutcome::Ok(None),
    );
    assert!(frames(&effects).is_empty());
    complete_with(&mut page, plain_buffer, ActivationOutcome::Ok(None));
    assert!(page.stage.holds("p1"));
    (reattach(&mut page, document), ledger)
}

#[test]
fn a_held_flush_the_next_document_no_longer_lets_its_instance_attach_is_lost_unsent() {
    // (a) p1 lost `blobs` while p2 keeps the door uploading; (b) p1 keeps
    // `blobs` but `OUT` is no longer its output.
    for (document, keeps_out) in [(doc(false, OUT), true), (doc(true, OTHER), false)] {
        let (effects, ledger) = staged_while_detached(&document);
        let sent = frames(&effects);
        assert!(!sent.iter().any(|frame| matches!(
            frame,
            ClientFrame::BlobCreate { .. } | ClientFrame::BlobOpen { .. }
        )));
        assert_eq!(ledger.held(), 0);
        let batches = sent
            .iter()
            .filter(|frame| matches!(frame, ClientFrame::PublishBatch { .. }))
            .count();
        if keeps_out {
            // The plain flush behind it still has its channel, and is sent.
            assert_eq!(lost(&effects), vec![LossReason::Revalidation]);
            assert_eq!(batches, 1, "{effects:?}");
        } else {
            // `OUT` is no longer p1's at all, so the plain flush goes too.
            assert_eq!(
                lost(&effects),
                vec![LossReason::Revalidation, LossReason::Revalidation]
            );
            assert_eq!(batches, 0, "{effects:?}");
        }
    }
}

#[test]
fn a_held_flush_the_unchanged_document_admits_is_uploaded() {
    let (effects, _ledger) = staged_while_detached(&doc(true, OUT));
    assert!(lost(&effects).is_empty());
    assert!(
        frames(&effects)
            .iter()
            .any(|frame| matches!(frame, ClientFrame::BlobCreate { .. }))
    );
}

/// The event announcing `instance`'s flush on `OUT` lost for `reason`.
fn flush_lost(instance: &str, reason: LossReason) -> Event {
    Event::FlushLost(Lost {
        instance: instance.to_string(),
        channels: vec![OUT.to_string()],
        reason,
    })
}

/// A core connected under the fixture document: its reports can reach the
/// error channel.
fn connected_core() -> crate::logic::KernelCore {
    let mut core = crate::logic::KernelCore::new();
    core.on_event(&connected_event());
    core
}

fn connected_event() -> Event {
    Event::Connected {
        bindings: Box::new(doc(true, OUT)),
        participant_id: "surface:bar".to_string(),
        session_id: "sess-1".to_string(),
        max_body_bytes: 65_536,
        alert_granted: false,
    }
}

/// The level and text of the one report `actions` holds, about `instance`.
fn report_of(instance: &str, actions: Vec<crate::logic::KernelAction>) -> (LogLevel, String) {
    match &actions[..] {
        [
            crate::logic::KernelAction::Report {
                level,
                subject,
                message,
            },
        ] => {
            assert_eq!(subject.as_deref(), Some(instance));
            (*level, message.clone())
        }
        other => panic!("one report: {other:?}"),
    }
}

#[test]
fn a_lost_flush_is_reported_at_warn_once_per_instance_and_reason() {
    let mut core = connected_core();
    let mut level = |instance: &str, reason: LossReason| {
        report_of(instance, core.on_event(&flush_lost(instance, reason))).0
    };
    assert_eq!(level("p1", LossReason::Revalidation), LogLevel::Warn);
    assert_eq!(level("p1", LossReason::Revalidation), LogLevel::Debug);
    assert_eq!(level("p1", LossReason::Overflow), LogLevel::Warn);
    // Another instance's first loss of the same kind is its own first.
    assert_eq!(level("p2", LossReason::Revalidation), LogLevel::Warn);
    assert_eq!(level("p2", LossReason::Revalidation), LogLevel::Debug);
}

#[test]
fn a_loss_while_detached_does_not_demote_the_first_one_attached() {
    let mut core = connected_core();
    core.on_event(&Event::Disconnected {
        reason: DetachReason::LivenessTimeout,
    });
    let overflow = flush_lost("p1", LossReason::Overflow);
    for _ in 0..2 {
        let (level, message) = report_of("p1", core.on_event(&overflow));
        assert_eq!(level, LogLevel::Warn);
        assert!(
            !message.contains("debug"),
            "nothing is demoted yet: {message}"
        );
    }

    core.on_event(&connected_event());
    let (level, message) = report_of("p1", core.on_event(&overflow));
    assert_eq!(level, LogLevel::Warn);
    assert!(message.contains("later losses of this kind are logged at debug"));
    assert_eq!(report_of("p1", core.on_event(&overflow)).0, LogLevel::Debug);
}

#[test]
fn losses_of_one_kind_with_different_numbers_are_one_kind_for_the_report() {
    let mut core = connected_core();
    let level = |actions: Vec<crate::logic::KernelAction>| match &actions[..] {
        [crate::logic::KernelAction::Report { level, .. }] => *level,
        other => panic!("one report: {other:?}"),
    };
    let incomplete = |offset| LossReason::Upload(UploadRefusal::Incomplete { offset, len: 1 });
    let too_large = |size| LossReason::Upload(UploadRefusal::BlobTooLarge { size, max: 1 });
    let bytes_held = |size| LossReason::Open(OpenRefusal::BytesHeldLimit { size });
    for (first, later) in [
        (incomplete(0), incomplete(4_096)),
        (too_large(10), too_large(20)),
        (bytes_held(10), bytes_held(20)),
    ] {
        assert_eq!(
            level(core.on_event(&flush_lost("p1", first))),
            LogLevel::Warn
        );
        assert_eq!(
            level(core.on_event(&flush_lost("p1", later))),
            LogLevel::Debug,
            "{later:?} repeats the kind of {first:?}"
        );
    }
    // The refusal inside is part of the kind.
    assert_eq!(
        level(core.on_event(&flush_lost(
            "p1",
            LossReason::Upload(UploadRefusal::StorageFailed)
        ))),
        LogLevel::Warn
    );
    assert_eq!(
        level(core.on_event(&flush_lost(
            "p1",
            LossReason::Open(OpenRefusal::HandleLimit)
        ))),
        LogLevel::Warn
    );
}

/// Put `document` in force on the attached page, as a later document on the
/// config channel is.
fn apply_document(page: &mut SurfacePage, document: &BindingsDocument) -> Vec<Effect> {
    let mut reactions = Reactions::new();
    let configured = page
        .apply_config(&document.to_body(), NOW)
        .expect("the document applies");
    reactions.configured(page, configured, NOW, NOW_MS);
    reactions.end_turn(page);
    reactions.into_effects()
}

/// The handle of the blob `create` answered `p1`'s completion with.
fn the_create(effects: &[Effect]) -> BlobHandle {
    let [ClientFrame::BlobCreate { handle, .. }] = frames(effects)[..] else {
        panic!("one create: {effects:?}")
    };
    *handle
}

fn batches(effects: &[Effect]) -> usize {
    frames(effects)
        .iter()
        .filter(|frame| matches!(frame, ClientFrame::PublishBatch { .. }))
        .count()
}

/// A cache holding no bytes, as a page that fetched nothing.
struct NoBytes;

impl CacheView for NoBytes {
    fn cached(&self, _channel: &str, _message_id: Uuid, _name: &AttachmentName) -> Cached<'_> {
        Cached::Absent
    }
}

fn attachment_name(name: &str) -> AttachmentName {
    AttachmentName::new(name).expect("a fixture attachment name")
}

/// The backend reference a message on `ORIGIN` delivered as `photo`.
fn origin_ref() -> BlobRef {
    BlobRef {
        id: BlobId::new("0123abcd".to_string()),
        size: 3,
        content_type: "image/png".to_string(),
    }
}

const ORIGIN_MESSAGE: Uuid = Uuid::from_u128(9);

fn origin() -> Blob {
    Blob::Remote(Remote {
        channel: ORIGIN.to_string(),
        message_id: ORIGIN_MESSAGE,
        name: attachment_name("photo"),
        blob: origin_ref(),
    })
}

/// A message on `channel` naming `blob` as `name`.
fn envelope(
    channel: &str,
    scheme: ChannelScheme,
    message_id: Uuid,
    name: &str,
    blob: &BlobRef,
) -> MessageEnvelope {
    MessageEnvelope {
        message_id,
        source: "test".into(),
        channel: channel.into(),
        sender: "app:page#p2".into(),
        publish_ts: chrono::DateTime::from_timestamp(0, 0).expect("an instant"),
        body: String::from("m").into(),
        reply_to: None,
        delivery_deadline: None,
        deliver_after: None,
        impetus: None,
        urgency: Urgency::Normal,
        envelope_type: scheme,
        transport: None,
        attachments: Attachments::from_map(BTreeMap::from([(attachment_name(name), blob.clone())])),
    }
}

/// A blob of `bytes` made in `scope`, counted in `ledger`; answers its handle.
fn made_in(scope: &mut BlobScope, ledger: &MadeLedger, bytes: &[u8]) -> u32 {
    let writer = scope
        .create(ledger, "application/octet-stream")
        .expect("room for the blob");
    scope
        .write(writer, 0, bytes, largest_made_blob(Some(&door())))
        .expect("a live writer")
        .expect("the write fits");
    scope.seal(writer).expect("a live writer").expect("no gap")
}

/// `p1`'s buffer holding one publish on `port` naming `handle` of `scope` as `a`.
fn named_buffer(page: &SurfacePage, scope: &BlobScope, port: &str, handle: u32) -> PublishBuffer {
    let mut buffer = buffer(page, "p1");
    buffer
        .publish_attached(
            scope,
            crate::publish_buffer::GrantVerdict::Held,
            Some(&door()),
            port,
            GuestBody::Text("named".to_string()),
            &[("a".to_string(), handle)],
            None,
            None,
        )
        .expect("the handle is the activation's")
        .expect("the publish is admitted");
    buffer
}

/// `p1`'s buffer forwarding to `OUT` the attachment `photo` of a message on
/// `ORIGIN` in its window.
fn forward_buffer(page: &SurfacePage) -> PublishBuffer {
    let mut scope = BlobScope::empty();
    scope.add_window(
        "feed",
        ORIGIN.to_string(),
        &[envelope(
            ORIGIN,
            ChannelScheme::Ephemeral,
            ORIGIN_MESSAGE,
            "photo",
            &origin_ref(),
        )],
    );
    let handle = scope
        .open(&NoBytes, &PageBlobs::new(), "feed", 0, "photo")
        .expect("a delivered attachment opens");
    named_buffer(page, &scope, "out", handle)
}

#[test]
fn a_second_document_that_fails_an_uploading_flush_closes_its_handle_and_sends_no_batch() {
    let mut page = page();
    let ledger = MadeLedger::new();
    let effects = complete_with(
        &mut page,
        |page| attachment_buffer(page, &ledger, 10),
        ActivationOutcome::Ok(None),
    );
    let handle = the_create(&effects);

    let effects = apply_document(&mut page, &doc(false, OUT));

    assert_eq!(lost(&effects), vec![LossReason::Revalidation]);
    assert_eq!(toasts(&effects), 1);
    assert!(frames(&effects).contains(&&ClientFrame::BlobClose { handle }));
    assert_eq!(batches(&effects), 0);
    assert!(drain(&mut page).is_empty());
    assert!(!page.stage.holds("p1"));
    assert_eq!(page.plane.handles_in_use(), 0);
    assert_eq!(ledger.held(), 0);
}

#[test]
fn a_forward_whose_message_left_the_servers_window_is_lost_and_the_next_publish_goes_out() {
    let mut page = page();
    let effects = complete_with(&mut page, forward_buffer, ActivationOutcome::Ok(None));
    let [
        ClientFrame::BlobOpen {
            handle,
            channel,
            message_id,
            name,
        },
    ] = frames(&effects)[..]
    else {
        panic!("one open of the origin: {effects:?}")
    };
    assert_eq!(
        (channel.as_str(), *message_id, name.as_str()),
        (ORIGIN, ORIGIN_MESSAGE, "photo")
    );
    let handle = *handle;

    let effects = feed(
        &mut page,
        Input::Frame(ServerFrame::BlobOpenResult {
            handle,
            outcome: BlobOpenOutcome::NotRetained,
        }),
    );

    assert_eq!(
        lost(&effects),
        vec![LossReason::Open(OpenRefusal::NotRetained)]
    );
    assert_eq!(toasts(&effects), 1);
    assert_eq!(batches(&effects), 0);
    assert!(!page.stage.holds("p1"));
    assert_eq!(page.plane.handles_in_use(), 0);

    let effects = complete_with(&mut page, plain_buffer, ActivationOutcome::Ok(None));
    assert_eq!(batches(&effects), 1);
}

#[test]
fn a_held_forward_whose_origin_the_next_document_no_longer_subscribes_is_lost_unopened() {
    for feeds in [true, false] {
        let mut page = page();
        detach(&mut page);
        let effects = complete_with(&mut page, forward_buffer, ActivationOutcome::Ok(None));
        assert!(frames(&effects).is_empty());
        complete_with(&mut page, plain_buffer, ActivationOutcome::Ok(None));

        let effects = reattach(&mut page, &doc_feeding(true, OUT, feeds));

        let opens = frames(&effects)
            .iter()
            .filter(|frame| matches!(frame, ClientFrame::BlobOpen { .. }))
            .count();
        if feeds {
            assert_eq!(opens, 1, "the origin is still subscribed: {effects:?}");
            assert!(lost(&effects).is_empty());
        } else {
            // The server answers an open there with a violation, so none is sent.
            assert_eq!(opens, 0, "{effects:?}");
            assert_eq!(lost(&effects), vec![LossReason::Revalidation]);
            assert_eq!(toasts(&effects), 1);
            assert_eq!(batches(&effects), 1, "the plain flush behind it is sent");
            assert!(!page.stage.holds("p1"));
            assert_eq!(page.plane.handles_in_use(), 0);
        }
    }
}

#[test]
fn a_reattach_to_a_door_too_small_for_a_staged_flush_loses_it_and_sends_the_one_behind() {
    let mut page = page();
    detach(&mut page);
    let ledger = MadeLedger::new();
    complete_with(
        &mut page,
        |page| attachment_buffer(page, &ledger, 10),
        ActivationOutcome::Ok(None),
    );
    complete_with(&mut page, plain_buffer, ActivationOutcome::Ok(None));
    let smaller = AttachmentFacts {
        blobs: Some(BlobFacts {
            max_blob_bytes: 5,
            ..door()
        }),
        ..pages::facts()
    };

    let effects = reattach_to(&mut page, &doc(true, OUT), smaller);

    assert_eq!(lost(&effects), vec![LossReason::DoorTooSmall]);
    assert_eq!(toasts(&effects), 1);
    assert!(
        !frames(&effects)
            .iter()
            .any(|frame| matches!(frame, ClientFrame::BlobCreate { .. }))
    );
    assert_eq!(batches(&effects), 1, "the plain flush behind it is sent");
    assert_eq!(ledger.held(), 0);
}

#[test]
fn over_its_depth_a_detached_page_loses_its_oldest_staged_flushes_with_a_toast_each() {
    let mut page = page();
    detach(&mut page);
    let ledger = MadeLedger::new();
    let mut losses = Vec::new();
    let mut toasted = 0;
    // The fixture's `parked_batch_depth` is 2.
    for _ in 0..4 {
        let effects = complete_with(
            &mut page,
            |page| attachment_buffer(page, &ledger, 10),
            ActivationOutcome::Ok(None),
        );
        losses.extend(lost(&effects));
        toasted += toasts(&effects);
    }
    assert_eq!(losses, vec![LossReason::Overflow, LossReason::Overflow]);
    assert_eq!(toasted, 2);
    assert_eq!(ledger.held(), 20, "the two kept flushes hold their bytes");
}

#[test]
fn an_instance_unmounted_mid_upload_closes_its_handle_and_frees_its_bytes() {
    let mut page = page();
    let ledger = MadeLedger::new();
    let effects = complete_with(
        &mut page,
        |page| attachment_buffer(page, &ledger, 10),
        ActivationOutcome::Ok(None),
    );
    let handle = the_create(&effects);

    let effects = feed(
        &mut page,
        Input::ActivationDeregistered {
            instance: "p1".to_string(),
        },
    );

    assert!(frames(&effects).contains(&&ClientFrame::BlobClose { handle }));
    assert_eq!(batches(&effects), 0);
    assert!(!page.stage.holds("p1"));
    assert_eq!(page.plane.handles_in_use(), 0);
    assert_eq!(ledger.held(), 0);
}

#[test]
fn a_rate_limited_batch_parked_at_a_detach_returns_to_staged_and_is_uploaded_again() {
    let mut page = page();
    let ledger = MadeLedger::new();
    let (handle, correlation) = to_handed(&mut page, &ledger, 10);
    let effects = feed(
        &mut page,
        Input::Frame(ServerFrame::PublishBatchResult {
            correlation,
            outcome: PublishBatchOutcome::RateLimited,
        }),
    );
    assert!(lost(&effects).is_empty());
    assert!(
        !frames(&effects).contains(&&ClientFrame::BlobClose { handle }),
        "the handle stays held while the outbox retries"
    );

    let effects = detach(&mut page);
    assert!(lost(&effects).is_empty());
    assert_eq!(
        toasts(&effects),
        0,
        "the flush survives, so no drop is announced"
    );
    assert!(page.stage.holds("p1"));
    assert_eq!(ledger.held(), 10);

    let effects = reattach(&mut page, &doc(true, OUT));
    assert!(
        frames(&effects)
            .iter()
            .any(|frame| matches!(frame, ClientFrame::BlobCreate { size: 10, .. }))
    );
}

#[test]
fn page_local_attachments_published_to_the_wire_are_uploaded_or_forwarded_by_kind() {
    let ledger = MadeLedger::new();
    let mut maker = BlobScope::empty();
    let made = made_in(&mut maker, &ledger, b"page");
    let made = maker.blob(made).expect("sealed").clone();
    drop(maker);
    let mut table = PageBlobs::new();
    let page_ref = table.commit(&made);
    let remote_ref = table.commit(&origin());
    let mut scope = BlobScope::empty();
    scope.add_window(
        "notes_in",
        NOTES.to_string(),
        &[
            envelope(
                NOTES,
                ChannelScheme::Local,
                Uuid::from_u128(1),
                "a",
                &page_ref,
            ),
            envelope(
                NOTES,
                ChannelScheme::Local,
                Uuid::from_u128(2),
                "a",
                &remote_ref,
            ),
        ],
    );

    // Page bytes a page-local message holds are uploaded from the page's copy.
    {
        let mut page = page();
        let bytes = scope
            .open(&NoBytes, &table, "notes_in", 0, "a")
            .expect("the table holds it");
        let effects = complete_with(
            &mut page,
            |page| named_buffer(page, &scope, "out", bytes),
            ActivationOutcome::Ok(None),
        );
        assert!(matches!(
            frames(&effects)[..],
            [ClientFrame::BlobCreate { size: 4, .. }]
        ));
        assert!(drain(&mut page).iter().any(
            |frame| matches!(frame, ClientFrame::BlobWrite { bytes, .. } if bytes.as_slice() == b"page")
        ));
    }

    // A backend reference a page-local message carries is forwarded by its origin.
    {
        let mut page = page();
        let reference = scope
            .open(&NoBytes, &table, "notes_in", 1, "a")
            .expect("the table holds it");
        let effects = complete_with(
            &mut page,
            |page| named_buffer(page, &scope, "out", reference),
            ActivationOutcome::Ok(None),
        );
        let [
            ClientFrame::BlobOpen {
                channel,
                message_id,
                name,
                ..
            },
        ] = frames(&effects)[..]
        else {
            panic!("one open of the origin, nothing written: {effects:?}")
        };
        assert_eq!(
            (channel.as_str(), *message_id, name.as_str()),
            (ORIGIN, ORIGIN_MESSAGE, "photo")
        );
        assert!(drain(&mut page).is_empty());
    }
}

#[test]
fn page_bytes_a_page_local_message_holds_are_freed_when_a_document_removes_its_channel() {
    let mut page = page();
    let ledger = MadeLedger::new();
    let effects = complete_with(
        &mut page,
        |page| {
            let mut scope = BlobScope::empty();
            let blob = made_in(&mut scope, &ledger, b"pattern");
            named_buffer(page, &scope, "notes", blob)
        },
        ActivationOutcome::Ok(None),
    );
    assert!(!frames(&effects).into_iter().any(is_upload_or_batch));
    assert_eq!(ledger.held(), 7, "the retained message holds the bytes");

    apply_document(&mut page, &doc(true, OUT));
    assert_eq!(ledger.held(), 7, "an unchanged document keeps them");

    let mut without = doc(true, OUT);
    without.outputs.retain(|output| output.channel != NOTES);
    without.local_channels.clear();
    apply_document(&mut page, &without);
    assert_eq!(ledger.held(), 0);
}

/// Perform `effects` in the runner's order: a frame is sent where it stands
/// in the queue and a drain sends its upload's chunks where it stands. After
/// every chunk, `between` is given the page and what it answers is appended
/// to the queue, as the runner folds an answer that arrives mid-drain.
fn perform(
    page: &mut SurfacePage,
    effects: Vec<Effect>,
    mut between: impl FnMut(&mut SurfacePage) -> Vec<Effect>,
) -> Vec<ClientFrame> {
    let mut queue: std::collections::VecDeque<Effect> = effects.into();
    let mut sent = Vec::new();
    while let Some(effect) = queue.pop_front() {
        match effect {
            Effect::SendFrame(frame) => sent.push(frame),
            Effect::DrainUpload(handle) => loop {
                let chunk = {
                    let SurfacePage { stage, plane, .. } = &mut *page;
                    stage.next_chunk_frames(handle, plane)
                };
                let Some(chunk) = chunk else {
                    break;
                };
                sent.extend(chunk);
                queue.extend(between(page));
            },
            _ => {}
        }
    }
    sent
}

/// Assert every `BlobWrite` and `BlobSeal` in `sent` follows the `BlobCreate`
/// of its handle, as the server requires; answers the created handles in order.
fn creates_lead_their_chunks(sent: &[ClientFrame]) -> Vec<BlobHandle> {
    let mut created = Vec::new();
    for frame in sent {
        match frame {
            ClientFrame::BlobCreate { handle, .. } => created.push(*handle),
            ClientFrame::BlobWrite { handle, .. } | ClientFrame::BlobSeal { handle, .. } => {
                assert!(
                    created.contains(handle),
                    "a chunk of handle {} went out before its create: {sent:?}",
                    handle.0
                );
            }
            _ => {}
        }
    }
    created
}

/// `instance`'s buffer holding one publish on `out` naming one blob of each of
/// `lens`, made in its activation and counted in `ledger`.
fn made_buffer(
    page: &SurfacePage,
    instance: &str,
    ledger: &MadeLedger,
    lens: &[usize],
) -> PublishBuffer {
    let mut scope = BlobScope::empty();
    let named: Vec<(String, u32)> = lens
        .iter()
        .enumerate()
        .map(|(i, len)| {
            let bytes: Vec<u8> = (0..*len).map(|b| ((b + i) % 251) as u8).collect();
            (format!("b{i}"), made_in(&mut scope, ledger, &bytes))
        })
        .collect();
    let mut buffer = buffer(page, instance);
    buffer
        .publish_attached(
            &scope,
            crate::publish_buffer::GrantVerdict::Held,
            Some(&door()),
            "out",
            GuestBody::Text("made".to_string()),
            &named,
            None,
            None,
        )
        .expect("the handles are the activation's")
        .expect("the publish is admitted");
    buffer
}

/// Bytes of `BlobWrite`s per handle, in `sent`.
fn written(sent: &[ClientFrame]) -> BTreeMap<u64, usize> {
    let mut written = BTreeMap::new();
    for frame in sent {
        if let ClientFrame::BlobWrite { handle, bytes, .. } = frame {
            *written.entry(handle.0).or_default() += bytes.len();
        }
    }
    written
}

#[test]
fn two_blobs_of_one_flush_each_have_their_create_on_the_wire_before_their_chunks() {
    let mut page = page();
    let ledger = MadeLedger::new();
    let effects = complete_with(
        &mut page,
        |page| made_buffer(page, "p1", &ledger, &[100_000, 70_000]),
        ActivationOutcome::Ok(None),
    );

    let sent = perform(&mut page, effects, |_| Vec::new());

    let created = creates_lead_their_chunks(&sent);
    assert_eq!(created.len(), 2, "{sent:?}");
    assert_eq!(
        written(&sent),
        BTreeMap::from([(created[0].0, 100_000), (created[1].0, 70_000)])
    );
}

#[test]
fn two_instances_whose_uploads_start_in_one_pass_each_send_their_create_first() {
    let mut page = page();
    detach(&mut page);
    let ledger = MadeLedger::new();
    complete_with(
        &mut page,
        |page| made_buffer(page, "p1", &ledger, &[100_000]),
        ActivationOutcome::Ok(None),
    );
    let buffer = made_buffer(&page, "p2", &ledger, &[70_000]);
    complete_as(&mut page, "p2", buffer, ActivationOutcome::Ok(None));

    let effects = reattach(&mut page, &doc(true, OUT));
    let sent = perform(&mut page, effects, |_| Vec::new());

    let created = creates_lead_their_chunks(&sent);
    assert_eq!(created.len(), 2, "{sent:?}");
    assert_eq!(written(&sent).values().sum::<usize>(), 170_000);
}

#[test]
fn an_upload_started_by_an_answer_mid_drain_sends_its_create_before_its_chunks() {
    let mut page = page();
    let ledger = MadeLedger::new();
    // p2's plain flush is on the wire, so its attachment flush waits for an
    // idle outbox.
    let mut plain = buffer(&page, "p2");
    plain
        .publish("out", GuestBody::Text("plain".to_string()))
        .expect("bound");
    let effects = complete_as(&mut page, "p2", plain, ActivationOutcome::Ok(None));
    let [ClientFrame::PublishBatch { correlation, .. }] = frames(&effects)[..] else {
        panic!("p2's plain flush goes out: {effects:?}")
    };
    let correlation = *correlation;
    let buffer = made_buffer(&page, "p2", &ledger, &[70_000]);
    let effects = complete_as(&mut page, "p2", buffer, ActivationOutcome::Ok(None));
    assert!(frames(&effects).is_empty(), "p2 waits: {effects:?}");

    let effects = complete_with(
        &mut page,
        |page| made_buffer(page, "p1", &ledger, &[200_000]),
        ActivationOutcome::Ok(None),
    );
    // The answer that frees p2's outbox arrives after p1's first chunk.
    let mut answer = Some(Input::Frame(ServerFrame::PublishBatchResult {
        correlation,
        outcome: PublishBatchOutcome::Ok,
    }));
    let sent = perform(&mut page, effects, |page| match answer.take() {
        Some(input) => feed(page, input),
        None => Vec::new(),
    });

    let created = creates_lead_their_chunks(&sent);
    assert_eq!(created.len(), 2, "{sent:?}");
    assert_eq!(
        written(&sent),
        BTreeMap::from([(created[0].0, 200_000), (created[1].0, 70_000)])
    );
}
