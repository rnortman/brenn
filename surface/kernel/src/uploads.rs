//! The stage: attachment flushes held per instance, outside the outbox, until
//! every blob they name is held on the live attachment.
//!
//! The outbox names blob handles, and a handle lives only as long as the
//! attachment that minted it. So a flush that names attachments waits here,
//! naming page bytes and backend references rather than handles, and its
//! handles are minted only once the instance's outbox is idle: then the blobs
//! are uploaded or opened, and when every handle is answered `Sealed` or
//! `Opened` the batch is composed with them and handed to the outbox, which
//! finds the wire free.
//!
//! **Order.** An instance's flushes reach the wire in completion order. A plain
//! flush completed while the instance has nothing staged goes straight to the
//! outbox. An attachment flush is staged, and every later flush of that
//! instance is staged behind it. A plain flush at the head is handed over and
//! leaves; an attachment flush at the head is staged, then uploading, then
//! handed, and nothing behind it moves until it is finished — answered `Ok` or
//! lost.
//!
//! **Waiting, not pipelining.** The batch is composed only after every handle
//! is answered, so a refused create or open is known with its reason before a
//! doomed batch is sent, and a detach mid-upload leaves a flush the server
//! certainly never applied, which is uploaded again from nothing.
//!
//! **Lost, whole.** What only the server can refuse loses the flush: its
//! entries and ops together, never a message without its attachment. Losses
//! are answered as data; announcing them is the caller's.
//!
//! Nothing here reads a clock or sends a frame.

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, VecDeque};

use brenn_attach_client::blobs::{BlobEvent, OpenRefusal, UploadRefusal};
use brenn_attach_client::publish::FlushBatch;
use brenn_attach_proto::{
    BatchDeferredOp, BatchEntry, BlobFacts, BlobHandle, ClientFrame, PublishAttachment,
    PublishBatchOutcome,
};
use brenn_envelope::{MessageBody, Urgency};

use crate::blob_plane::{PagePlane, PlaneTag, UploadId};
use crate::blob_scope::{Blob, blobs_bytes, distinct_blobs, door_holds};
use crate::publish_buffer::BufferedAttachment;

/// One wire-bound publish of a staged flush, its attachments still blobs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagedEntry {
    pub channel: String,
    pub body: MessageBody,
    pub urgency: Urgency,
    pub deliver_after: Option<u64>,
    pub attachments: Vec<BufferedAttachment>,
}

/// One `ok` activation's wire batch, as the stage holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagedFlush {
    pub entries: Vec<StagedEntry>,
    pub ops: Vec<BatchDeferredOp>,
}

impl StagedFlush {
    pub fn names_attachments(&self) -> bool {
        self.entries
            .iter()
            .any(|entry| !entry.attachments.is_empty())
    }

    /// The distinct blobs the flush names, in first-naming order.
    fn blobs(&self) -> Vec<Blob> {
        distinct_blobs(
            self.entries
                .iter()
                .flat_map(|entry| &entry.attachments)
                .map(|attachment| &attachment.blob),
        )
        .into_iter()
        .cloned()
        .collect()
    }

    /// The channel each backend reference it forwards was delivered on, once
    /// per attachment naming one.
    pub fn origins(&self) -> impl Iterator<Item = &str> {
        self.entries
            .iter()
            .flat_map(|entry| &entry.attachments)
            .filter_map(|attachment| match &attachment.blob {
                Blob::Remote(remote) => Some(remote.channel.as_str()),
                Blob::Bytes(_) => None,
            })
    }

    /// The channels its entries name, in entry order, each once.
    pub fn channels(&self) -> Vec<String> {
        let mut channels: Vec<String> = Vec::new();
        for entry in &self.entries {
            if !channels.contains(&entry.channel) {
                channels.push(entry.channel.clone());
            }
        }
        channels
    }

    /// The batch frame's lists, each attachment naming the handle `handle_of`
    /// answers for its blob.
    pub(crate) fn batch(&self, handle_of: impl Fn(&Blob) -> BlobHandle) -> FlushBatch {
        FlushBatch {
            entries: self
                .entries
                .iter()
                .map(|entry| {
                    let mut wire =
                        BatchEntry::new(entry.channel.clone(), entry.body.clone(), entry.urgency)
                            .attachments(
                                entry
                                    .attachments
                                    .iter()
                                    .map(|attachment| {
                                        PublishAttachment::new(
                                            attachment.name.clone(),
                                            handle_of(&attachment.blob),
                                        )
                                    })
                                    .collect(),
                            );
                    if let Some(at) = entry.deliver_after {
                        wire = wire.deliver_after(at);
                    }
                    wire
                })
                .collect(),
            ops: self.ops.clone(),
        }
    }

    /// The batch of a flush that names no attachment.
    ///
    /// # Panics
    ///
    /// If it names one.
    fn plain_batch(&self) -> FlushBatch {
        self.batch(|blob| {
            panic!(
                "surface client: a plain flush names {} bytes of attachment",
                blob.size()
            )
        })
    }
}

/// Why a staged flush was lost.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LossReason {
    /// The server refused a create.
    Upload(UploadRefusal),
    /// The server refused an open of a forwarded attachment.
    Open(OpenRefusal),
    /// The live attachment's blob door can never take the flush: no door, a
    /// door without upload, a made blob over `max_blob_bytes`, more blobs than
    /// `max_handles`, or more bytes than `max_bytes_held`.
    DoorTooSmall,
    /// The flush no longer passes the checks a held flush is judged by before
    /// it moves.
    Revalidation,
    /// The instance's stage was over its depth.
    Overflow,
    /// The batch was on the wire, unanswered, when the attachment ended. The
    /// server may have applied it; it is never sent again.
    Unanswered,
}

impl LossReason {
    /// The reason with the numbers a refusal carries left out: two losses of
    /// one kind at different offsets or sizes are the same kind.
    pub fn kind(&self) -> LossKind {
        LossKind {
            reason: std::mem::discriminant(self),
            upload: match self {
                LossReason::Upload(refusal) => Some(std::mem::discriminant(refusal)),
                _ => None,
            },
            open: match self {
                LossReason::Open(refusal) => Some(std::mem::discriminant(refusal)),
                _ => None,
            },
        }
    }
}

/// A [`LossReason`] without its payloads, one per variant of it and of the
/// refusal it wraps. See [`LossReason::kind`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LossKind {
    reason: std::mem::Discriminant<LossReason>,
    upload: Option<std::mem::Discriminant<UploadRefusal>>,
    open: Option<std::mem::Discriminant<OpenRefusal>>,
}

/// One staged flush that will never reach the wire.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Lost {
    pub instance: String,
    /// The channels its entries named.
    pub channels: Vec<String>,
    pub reason: LossReason,
}

/// What a stage operation produced.
#[derive(Debug, Default, PartialEq)]
pub struct StageStep {
    /// Frames to send, in order: creates and opens. An upload's chunks are not
    /// here; [`Stage::next_chunk_frames`] composes them one at a time, behind
    /// the upload's create.
    pub frames: Vec<ClientFrame>,
    /// Flushes for the instance's outbox, in order.
    pub handed: Vec<(String, FlushBatch)>,
    pub lost: Vec<Lost>,
}

/// One handle of an uploading flush.
#[derive(Debug)]
struct Upload {
    id: UploadId,
    blob: Blob,
    handle: BlobHandle,
    held: bool,
}

#[derive(Debug)]
enum Phase {
    Staged,
    Uploading(Vec<Upload>),
    /// The batch is in the outbox, its handles held.
    Handed(Vec<Upload>),
}

#[derive(Debug)]
struct Held {
    flush: StagedFlush,
    phase: Phase,
    /// The flush passed the gate's `survives` under the contract now in
    /// force. That contract changes only when a bindings document is applied,
    /// and [`Stage::revalidate`] judges every held flush then, so a head
    /// waiting for room is not judged again on every pass.
    vetted: bool,
}

impl Held {
    fn handle_of(uploads: &[Upload], blob: &Blob) -> BlobHandle {
        uploads
            .iter()
            .find(|upload| upload.blob == *blob)
            .map(|upload| upload.handle)
            .expect("surface client: every blob of an uploading flush has a handle")
    }
}

/// Whether a flush of the distinct `blobs`, `bytes` together, can ever fit
/// `door`, whatever else the session holds. A made blob must also fit the
/// door's `max_blob_bytes`: the page checked it at `write` against the door
/// it knew then, which may be a previous attachment's.
fn ever_fits(door: Option<BlobFacts>, blobs: &[Blob], bytes: Option<u64>) -> bool {
    let Some(door) = door.filter(|door| door.upload) else {
        return false;
    };
    let made_fit = blobs.iter().all(|blob| match blob {
        Blob::Bytes(bytes) => bytes.size() <= door.max_blob_bytes,
        Blob::Remote(_) => true,
    });
    made_fit && door_holds(&door, blobs.len(), bytes)
}

/// What the stage asks of the page before it moves a flush: whether the
/// instance's outbox is idle, and whether a held flush still passes the checks
/// a flush is judged by before it is offered the wire.
pub trait StageGate {
    /// Nothing parked and nothing unanswered in `instance`'s outbox.
    fn idle(&self, instance: &str) -> bool;
    /// Whether `flush` of `instance`'s may still go to the wire.
    fn survives(&self, instance: &str, flush: &StagedFlush) -> bool;
}

/// Every outbox idle or none, and every flush passing, for tests that script
/// the outbox alone.
#[cfg(test)]
#[derive(Clone, Copy)]
pub(crate) struct Idle(pub bool);

#[cfg(test)]
impl StageGate for Idle {
    fn idle(&self, _: &str) -> bool {
        self.0
    }

    fn survives(&self, _: &str, _: &StagedFlush) -> bool {
        true
    }
}

/// The page's stage, one queue per instance.
#[derive(Debug, Default)]
pub struct Stage {
    queues: BTreeMap<String, VecDeque<Held>>,
    /// Which instance each live upload id belongs to.
    owners: BTreeMap<UploadId, String>,
    next_upload: u64,
    /// Whether flushes may start: an attachment is live and its bindings
    /// document applied.
    ready: bool,
    /// The instance whose head waits for room. It is tried first on every
    /// pass, and no other upload starts while it waits.
    room_waiter: Option<String>,
    /// Flushes lost, per instance, lifetime.
    lost: BTreeMap<String, u64>,
    /// Flushes lost on the wire at a detach, per instance, not yet reported.
    unanswered: BTreeMap<String, u64>,
}

impl Stage {
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether `instance` holds anything here.
    pub fn holds(&self, instance: &str) -> bool {
        self.queues
            .get(instance)
            .is_some_and(|queue| !queue.is_empty())
    }

    /// Flushes `instance` lost here, lifetime.
    pub fn lost_count(&self, instance: &str) -> u64 {
        self.lost.get(instance).copied().unwrap_or(0)
    }

    /// Take the per-instance counts of flushes lost on the wire at a detach,
    /// for the report made once the next attachment is configured.
    pub fn take_unanswered(&mut self) -> BTreeMap<String, u64> {
        std::mem::take(&mut self.unanswered)
    }

    /// Accept one completed activation's wire batch for `instance`, whose
    /// outbox holds `depth` flushes.
    ///
    /// A plain flush with nothing staged ahead of it is handed straight back
    /// for the outbox. Anything else is staged; with more than `depth` flushes
    /// not yet uploading, the oldest of them is lost. An uploading or handed
    /// head is not counted, as an outbox does not count its flush on the wire.
    pub fn push(
        &mut self,
        instance: &str,
        flush: StagedFlush,
        depth: u64,
        plane: &mut PagePlane,
        gate: impl StageGate,
    ) -> StageStep {
        let mut step = StageStep::default();
        if !self.holds(instance) && !flush.names_attachments() {
            step.handed
                .push((instance.to_string(), flush.plain_batch()));
            return step;
        }
        let queue = self.queues.entry(instance.to_string()).or_default();
        queue.push_back(Held {
            flush,
            phase: Phase::Staged,
            vetted: false,
        });
        let staged = |queue: &VecDeque<Held>| {
            queue
                .iter()
                .filter(|held| matches!(held.phase, Phase::Staged))
                .count() as u64
        };
        let mut overflowed = Vec::new();
        while staged(queue) > depth {
            let index = queue
                .iter()
                .position(|held| matches!(held.phase, Phase::Staged))
                .expect("a queue over its depth holds a staged flush");
            overflowed.push(queue.remove(index).expect("the index came from the queue"));
        }
        for held in overflowed {
            step.lost
                .push(self.lose(instance, &held.flush, LossReason::Overflow));
        }
        step.absorb(self.advance(plane, gate));
        step
    }

    /// The live attachment's bindings document is applied: flushes may start.
    pub fn on_ready(&mut self, plane: &mut PagePlane, gate: impl StageGate) -> StageStep {
        self.ready = true;
        self.advance(plane, gate)
    }

    /// Lose every flush not yet handed that `survives` refuses, closing the
    /// handles of one that was uploading. Run when a bindings document is
    /// applied, before [`on_ready`](Self::on_ready).
    pub fn revalidate(
        &mut self,
        plane: &mut PagePlane,
        survives: impl Fn(&str, &StagedFlush) -> bool,
    ) -> StageStep {
        let mut step = StageStep::default();
        let instances: Vec<String> = self.queues.keys().cloned().collect();
        for instance in instances {
            let queue = self
                .queues
                .remove(&instance)
                .expect("the key came from the map");
            let mut kept = VecDeque::new();
            for mut held in queue {
                if matches!(held.phase, Phase::Handed(_)) {
                    kept.push_back(held);
                    continue;
                }
                if survives(&instance, &held.flush) {
                    held.vetted = true;
                    kept.push_back(held);
                    continue;
                }
                if let Phase::Uploading(uploads) = &held.phase {
                    step.frames.extend(self.close_all(plane, uploads));
                }
                step.lost
                    .push(self.lose(&instance, &held.flush, LossReason::Revalidation));
            }
            self.queues.insert(instance, kept);
        }
        step
    }

    /// Move every instance's head as far as it goes: hand plain heads over,
    /// lose a head the door can never take, and start a staged head when the
    /// stage is ready, the instance's outbox is idle and the plane has room for
    /// all of it.
    ///
    /// While a head waits for room alone, it is tried first on the next pass,
    /// no other upload starts and the plane holds new fetches back, so what is
    /// in flight drains and the upload is not starved.
    pub fn advance(&mut self, plane: &mut PagePlane, gate: impl StageGate) -> StageStep {
        let mut step = StageStep::default();
        let waiter = self.room_waiter.take();
        let mut instances: Vec<String> = waiter
            .iter()
            .filter(|instance| self.queues.contains_key(*instance))
            .cloned()
            .collect();
        instances.extend(
            self.queues
                .keys()
                .filter(|instance| waiter.as_ref() != Some(*instance))
                .cloned(),
        );
        for instance in instances {
            // A plain flush handed over in this pass is not in the outbox yet,
            // so the outbox's idleness says nothing about it: the instance's
            // next upload waits for that flush's answer.
            let mut handed_plain = false;
            while let Some(head) = self.queues.get(&instance).and_then(VecDeque::front) {
                if !matches!(head.phase, Phase::Staged) {
                    break;
                }
                if !head.flush.names_attachments() {
                    let held = self.pop(&instance);
                    step.handed
                        .push((instance.clone(), held.flush.plain_batch()));
                    handed_plain = true;
                    continue;
                }
                if !self.ready || handed_plain {
                    break;
                }
                let blobs = head.flush.blobs();
                let bytes = blobs_bytes(&blobs);
                if !ever_fits(plane.door(), &blobs, bytes) {
                    let held = self.pop(&instance);
                    step.lost
                        .push(self.lose(&instance, &held.flush, LossReason::DoorTooSmall));
                    continue;
                }
                let bytes = bytes.expect("a flush the door holds has a byte total");
                if !gate.idle(&instance) {
                    break;
                }
                if self.room_waiter.is_some() {
                    break;
                }
                if !head.vetted {
                    if !gate.survives(&instance, &head.flush) {
                        let held = self.pop(&instance);
                        step.lost
                            .push(self.lose(&instance, &held.flush, LossReason::Revalidation));
                        continue;
                    }
                    self.queues
                        .get_mut(&instance)
                        .and_then(VecDeque::front_mut)
                        .expect("the head was just read")
                        .vetted = true;
                }
                if !plane.has_room(blobs.len() as u64, bytes) {
                    self.room_waiter = Some(instance.clone());
                    break;
                }
                let uploads = self.start(&instance, plane, blobs, &mut step);
                self.queues
                    .get_mut(&instance)
                    .and_then(VecDeque::front_mut)
                    .expect("the head was just read")
                    .phase = Phase::Uploading(uploads);
                break;
            }
        }
        self.queues.retain(|_, queue| !queue.is_empty());
        plane.hold_fetches(self.room_waiter.is_some());
        step
    }

    /// Mint one handle per distinct blob: a create for page bytes, an open of
    /// the place of delivery for a backend reference.
    fn start(
        &mut self,
        instance: &str,
        plane: &mut PagePlane,
        blobs: Vec<Blob>,
        step: &mut StageStep,
    ) -> Vec<Upload> {
        let mut uploads = Vec::new();
        for blob in blobs {
            let id = UploadId(self.next_upload);
            self.next_upload += 1;
            self.owners.insert(id, instance.to_string());
            let handle = match &blob {
                Blob::Bytes(bytes) => {
                    let (handle, frames) = plane
                        .create(id, bytes.content_type(), bytes.size())
                        .unwrap_or_else(|refusal| {
                            panic!(
                                "surface client: a create the stage checked against the door \
                                 was refused locally: {refusal:?}"
                            )
                        });
                    step.frames.extend(frames);
                    handle
                }
                Blob::Remote(remote) => {
                    let (handle, frame) = plane
                        .open(
                            PlaneTag::Upload(id),
                            &remote.channel,
                            remote.message_id,
                            remote.name.clone(),
                            remote.blob.size,
                        )
                        .unwrap_or_else(|refusal| {
                            panic!(
                                "surface client: an open the stage checked against the door was \
                                 refused locally: {refusal:?}"
                            )
                        });
                    step.frames.push(frame);
                    handle
                }
            };
            uploads.push(Upload {
                id,
                blob,
                handle,
                held: false,
            });
        }
        uploads
    }

    /// The frames of the next chunk of the upload of `handle`, or `None` when
    /// it wants none or is no longer in progress — refused, closed, or
    /// forgotten at a detach. The caller sends them and asks again until
    /// `None`, so the page holds a blob once plus one chunk.
    ///
    /// Only the named upload is asked: its create was sent before this, and
    /// an upload started meanwhile has its own create still queued.
    pub fn next_chunk_frames(
        &self,
        handle: BlobHandle,
        plane: &mut PagePlane,
    ) -> Option<Vec<ClientFrame>> {
        let bytes = self.queues.values().find_map(|queue| {
            let Phase::Uploading(uploads) = &queue.front()?.phase else {
                return None;
            };
            uploads
                .iter()
                .find(|upload| upload.handle == handle)
                .and_then(|upload| match &upload.blob {
                    Blob::Bytes(bytes) => Some(bytes),
                    Blob::Remote(_) => None,
                })
        })?;
        let range = plane.next_chunk(handle)?;
        let start = usize::try_from(range.offset)
            .expect("surface client: a page-made blob's offset fits usize");
        let chunk = bytes.bytes()[start..start + range.len].to_vec();
        Some(plane.write_chunk(handle, chunk))
    }

    /// The handles of every upload in progress, for tests that drain them all.
    #[cfg(test)]
    pub(crate) fn uploading_handles(&self) -> Vec<BlobHandle> {
        self.queues
            .values()
            .filter_map(|queue| match &queue.front()?.phase {
                Phase::Uploading(uploads) => Some(uploads.iter().map(|upload| upload.handle)),
                _ => None,
            })
            .flatten()
            .collect()
    }

    /// Settle the stage's share of one blob answer.
    ///
    /// `Err` is a peer the page cannot reconcile: `NoSuchAttachment` for a
    /// reference the server itself delivered, or an `Opened` whose size or
    /// content type contradicts that reference.
    pub fn on_events(
        &mut self,
        plane: &mut PagePlane,
        events: Vec<BlobEvent<UploadId>>,
        gate: impl StageGate,
    ) -> Result<StageStep, String> {
        let mut step = StageStep::default();
        for event in events {
            match event {
                BlobEvent::Sealed { tag, .. } => self.on_held(tag, &mut step),
                BlobEvent::Opened {
                    tag,
                    size,
                    content_type,
                    ..
                } => {
                    if let Some(Blob::Remote(remote)) = self.blob_of(tag)
                        && (remote.blob.size != size || remote.blob.content_type != content_type)
                    {
                        return Err(format!(
                            "BlobOpenResult for forwarded attachment {} of message {} on {} \
                             answered {size} bytes of {content_type:?}, and the delivery named \
                             {} bytes of {:?}",
                            remote.name,
                            remote.message_id,
                            remote.channel,
                            remote.blob.size,
                            remote.blob.content_type
                        ));
                    }
                    self.on_held(tag, &mut step);
                }
                // The stage's own close, or a detach the stage acts on itself.
                BlobEvent::UploadRefused {
                    reason: UploadRefusal::Closed | UploadRefusal::Detached,
                    ..
                }
                | BlobEvent::OpenRefused {
                    reason: OpenRefusal::Detached,
                    ..
                } => {}
                BlobEvent::OpenRefused {
                    tag,
                    reason: OpenRefusal::NoSuchAttachment,
                    ..
                } => {
                    let detail = match self.blob_of(tag) {
                        Some(Blob::Remote(remote)) => format!(
                            "attachment {} of message {} on {}",
                            remote.name, remote.message_id, remote.channel
                        ),
                        _ => format!("upload {tag:?}"),
                    };
                    return Err(format!(
                        "BlobOpenResult NoSuchAttachment for forwarded {detail}, which the \
                         server's own delivery named"
                    ));
                }
                BlobEvent::UploadRefused { tag, reason, .. } => {
                    self.on_refused(plane, tag, LossReason::Upload(reason), &mut step);
                }
                BlobEvent::OpenRefused { tag, reason, .. } => {
                    self.on_refused(plane, tag, LossReason::Open(reason), &mut step);
                }
                BlobEvent::Chunk { .. }
                | BlobEvent::ReadDone { .. }
                | BlobEvent::ReadRefused { .. } => {
                    unreachable!("surface client: the plane hands the stage no read event")
                }
            }
        }
        step.absorb(self.advance(plane, gate));
        Ok(step)
    }

    /// The outbox settled a batch of `instance`'s. When the head is handed the
    /// answer is its own: an instance's outbox holds nothing else while its
    /// attachment flush is there. `Ok` finishes it; a `RateLimited` refusal
    /// leaves it in the outbox to be retried.
    ///
    /// # Panics
    ///
    /// On `TextOnlyChannel` or `AttachmentNotHeld` for a handed head: every
    /// handle it names was answered held on this session.
    pub fn on_batch_answer(
        &mut self,
        instance: &str,
        outcome: PublishBatchOutcome,
        plane: &mut PagePlane,
        gate: impl StageGate,
    ) -> StageStep {
        let handed = self
            .queues
            .get(instance)
            .and_then(VecDeque::front)
            .is_some_and(|held| matches!(held.phase, Phase::Handed(_)));
        if handed {
            match outcome {
                PublishBatchOutcome::Ok => {
                    let held = self.pop(instance);
                    self.forget(&held);
                }
                PublishBatchOutcome::RateLimited => {}
                PublishBatchOutcome::TextOnlyChannel | PublishBatchOutcome::AttachmentNotHeld => {
                    panic!(
                        "surface client: {instance}'s attachment flush answered {outcome:?} after \
                         the server answered every handle it names held"
                    )
                }
            }
        }
        self.advance(plane, gate)
    }

    /// The attachment ended. An uploading head returns to staged, to be
    /// uploaded again from nothing. A handed head the outbox dropped as parked
    /// — its name is taken out of `dropped` — returns to staged too: it was
    /// refused, so it was not applied. A handed head still on the wire is lost
    /// and counted for the report after the next attachment.
    pub fn on_detached(&mut self, dropped: &mut Vec<String>) -> StageStep {
        self.ready = false;
        self.room_waiter = None;
        let mut step = StageStep::default();
        let instances: Vec<String> = self.queues.keys().cloned().collect();
        for instance in instances {
            let Some(head) = self.queues.get_mut(&instance).and_then(VecDeque::front_mut) else {
                continue;
            };
            let (uploads, on_the_wire) = match std::mem::replace(&mut head.phase, Phase::Staged) {
                Phase::Staged => continue,
                Phase::Uploading(uploads) => (uploads, false),
                Phase::Handed(uploads) => match dropped.iter().position(|name| *name == instance) {
                    Some(index) => {
                        dropped.remove(index);
                        (uploads, false)
                    }
                    None => (uploads, true),
                },
            };
            for upload in uploads {
                self.owners.remove(&upload.id);
            }
            if on_the_wire {
                let held = self.pop(&instance);
                step.lost
                    .push(self.lose(&instance, &held.flush, LossReason::Unanswered));
                *self.unanswered.entry(instance.clone()).or_default() += 1;
            }
        }
        self.queues.retain(|_, queue| !queue.is_empty());
        step
    }

    /// Throw away everything `instance` holds here: it was killed, or its
    /// outbox closed. Answers the frames that close its handles and how many
    /// flushes died. A handed head's handles are the outbox's to release.
    pub fn discard(&mut self, instance: &str, plane: &mut PagePlane) -> (Vec<ClientFrame>, u64) {
        let Some(queue) = self.queues.remove(instance) else {
            return (Vec::new(), 0);
        };
        let mut frames = Vec::new();
        let count = queue.len() as u64;
        for held in queue {
            match &held.phase {
                Phase::Uploading(uploads) => frames.extend(self.close_all(plane, uploads)),
                Phase::Handed(_) | Phase::Staged => self.forget(&held),
            }
        }
        (frames, count)
    }

    fn on_held(&mut self, id: UploadId, step: &mut StageStep) {
        let Some(instance) = self.owners.get(&id).cloned() else {
            return;
        };
        let head = self
            .queues
            .get_mut(&instance)
            .and_then(VecDeque::front_mut)
            .expect("surface client: a live upload's instance has a head");
        let Phase::Uploading(uploads) = &mut head.phase else {
            panic!("surface client: upload {id:?} answered for a head that is not uploading")
        };
        uploads
            .iter_mut()
            .find(|upload| upload.id == id)
            .expect("surface client: a live upload is its head's")
            .held = true;
        if uploads.iter().all(|upload| upload.held) {
            let Phase::Uploading(uploads) = std::mem::replace(&mut head.phase, Phase::Staged)
            else {
                unreachable!("matched above")
            };
            let batch = head.flush.batch(|blob| Held::handle_of(&uploads, blob));
            head.phase = Phase::Handed(uploads);
            step.handed.push((instance, batch));
        }
    }

    fn on_refused(
        &mut self,
        plane: &mut PagePlane,
        id: UploadId,
        reason: LossReason,
        step: &mut StageStep,
    ) {
        let Some(instance) = self.owners.get(&id).cloned() else {
            return;
        };
        let held = self.pop(&instance);
        let Phase::Uploading(uploads) = &held.phase else {
            panic!("surface client: upload {id:?} refused for a head that is not uploading")
        };
        step.frames.extend(self.close_all(plane, uploads));
        step.lost.push(self.lose(&instance, &held.flush, reason));
    }

    /// Close every handle of an uploading flush and forget its ids.
    fn close_all(&mut self, plane: &mut PagePlane, uploads: &[Upload]) -> Vec<ClientFrame> {
        let mut frames = Vec::new();
        for upload in uploads {
            self.owners.remove(&upload.id);
            frames.extend(plane.close(upload.handle));
        }
        frames
    }

    fn forget(&mut self, held: &Held) {
        if let Phase::Uploading(uploads) | Phase::Handed(uploads) = &held.phase {
            for upload in uploads {
                self.owners.remove(&upload.id);
            }
        }
    }

    fn blob_of(&self, id: UploadId) -> Option<&Blob> {
        let instance = self.owners.get(&id)?;
        match &self.queues.get(instance)?.front()?.phase {
            Phase::Uploading(uploads) | Phase::Handed(uploads) => uploads
                .iter()
                .find(|upload| upload.id == id)
                .map(|upload| &upload.blob),
            Phase::Staged => None,
        }
    }

    fn pop(&mut self, instance: &str) -> Held {
        self.queues
            .get_mut(instance)
            .and_then(VecDeque::pop_front)
            .expect("surface client: the instance's head was just read")
    }

    fn lose(&mut self, instance: &str, flush: &StagedFlush, reason: LossReason) -> Lost {
        *self.lost.entry(instance.to_string()).or_default() += 1;
        Lost {
            instance: instance.to_string(),
            channels: flush.channels(),
            reason,
        }
    }
}

impl StageStep {
    /// Append a later step of the same turn.
    pub fn absorb(&mut self, other: StageStep) {
        self.frames.extend(other.frames);
        self.handed.extend(other.handed);
        self.lost.extend(other.lost);
    }
}
