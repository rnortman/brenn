//! One activation's blob handle table: what it may open of the attachments in
//! its windows, the writers it made, and the blobs it holds.
//!
//! A component names an attachment only by `(port, index, name)` in the windows
//! its own activation was assembled with. The scope is built from those windows
//! before the entry is called, so a message that arrives or leaves while the
//! entry runs changes nothing a component can name; it is dropped with the
//! activation's in-flight frame, so no handle outlives the activation.
//!
//! Writers and blobs share one number space, the page's ([`HandleNumbers`]):
//! a number is minted once in the page's life, so a handle a component carries
//! out of its activation names nothing in a later one and traps there. A blob
//! is either page bytes ([`Blob::Bytes`], made on the page)
//! or a backend reference ([`Blob::Remote`], an attachment the page was
//! delivered, known by the place it was delivered).
//!
//! `open` reads no byte and touches no network. On a wire-bound port it yields
//! the backend reference the delivery carried. On a page-local port it yields
//! what the page's blob table ([`crate::page_blobs`]) holds for the
//! attachment's id: page bytes, or a backend reference. A backend reference's
//! bytes are read out of the page's attachment cache ([`crate::fetch`]) at
//! `read`, and an attachment whose bytes are not on the page answers
//! `unavailable`.
//!
//! Built on both targets so its authority rules are tested natively; only the
//! browser's exports call it.

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use brenn_attach_proto::BlobFacts;
use brenn_envelope::{AttachmentName, BlobRef, MessageEnvelope, is_local_channel};
use uuid::Uuid;

use crate::activation::PageActivation;
use crate::bindings::AppliedBindings;
use crate::fetch::{Cached, Fetch};
use crate::page_blobs::PageBlobs;
use crate::page_bytes::{BufferRefusal, MadeLedger, PAGE_MADE_BLOB_BYTES, PageBytes, WriteBuffer};

/// The largest single read a page serves, in bytes. `limits` reports it.
pub const SURFACE_MAX_READ_BYTES: u32 = 1024 * 1024;

/// The largest single write a page serves, in bytes. `limits` reports it.
pub const SURFACE_MAX_WRITE_BYTES: u32 = 1024 * 1024;

/// The most bytes one activation's writes carry, together.
pub const SURFACE_MAX_BYTES_WRITTEN_PER_ACTIVATION: u64 = 64 << 20;

/// The most handles one activation holds at once, writers and blobs together.
pub const SURFACE_MAX_HANDLES: usize = 64;

/// The largest blob the page makes: [`PAGE_MADE_BLOB_BYTES`], or the blob
/// door's `max_blob_bytes` when the last `Welcome` advertised a door with a
/// smaller one.
pub fn largest_made_blob(door: Option<&BlobFacts>) -> u64 {
    door.map_or(PAGE_MADE_BLOB_BYTES, |door| {
        door.max_blob_bytes.min(PAGE_MADE_BLOB_BYTES)
    })
}

/// The page's attachment cache as a scope reads it.
pub trait CacheView {
    fn cached(&self, channel: &str, message_id: Uuid, name: &AttachmentName) -> Cached<'_>;
}

impl CacheView for Fetch {
    fn cached(&self, channel: &str, message_id: Uuid, name: &AttachmentName) -> Cached<'_> {
        Fetch::cached(self, channel, message_id, name)
    }
}

/// `open-error`, as the scope answers it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenRefusal {
    NotPermitted,
    NotFound,
    QuotaExceeded,
}

impl OpenRefusal {
    /// The WIT variant's name.
    pub fn tag(self) -> &'static str {
        match self {
            OpenRefusal::NotPermitted => "not-permitted",
            OpenRefusal::NotFound => "not-found",
            OpenRefusal::QuotaExceeded => "quota-exceeded",
        }
    }
}

/// `read-error`, as the scope answers it. A page answers neither
/// `storage-failed` nor `quota-exceeded`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadRefusal {
    OutOfRange,
    TooLarge,
    Unavailable,
}

impl ReadRefusal {
    /// The WIT variant's name.
    pub fn tag(self) -> &'static str {
        match self {
            ReadRefusal::OutOfRange => "out-of-range",
            ReadRefusal::TooLarge => "too-large",
            ReadRefusal::Unavailable => "unavailable",
        }
    }
}

/// `create-error`, as the scope answers it. A page never answers
/// `storage-failed`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CreateRefusal {
    /// The content type is not one a body may carry; the string says why.
    InvalidContentType(String),
    /// The page already holds [`PAGE_MADE_BLOB_BYTES`] of blobs it made.
    BudgetExhausted,
    /// The activation holds [`SURFACE_MAX_HANDLES`].
    QuotaExceeded,
}

impl CreateRefusal {
    /// The WIT variant's name.
    pub fn tag(&self) -> &'static str {
        match self {
            CreateRefusal::InvalidContentType(_) => "invalid-content-type",
            CreateRefusal::BudgetExhausted => "budget-exhausted",
            CreateRefusal::QuotaExceeded => "quota-exceeded",
        }
    }
}

/// `write-error`, as the scope answers it, in the order the scope checks. A
/// page never answers `storage-failed`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteRefusal {
    /// Longer than [`SURFACE_MAX_WRITE_BYTES`].
    TooLarge,
    /// The activation's writes would pass
    /// [`SURFACE_MAX_BYTES_WRITTEN_PER_ACTIVATION`].
    QuotaExceeded,
    /// The write would end past [`largest_made_blob`].
    BlobTooLarge,
    /// The page's page-made bytes would pass [`PAGE_MADE_BLOB_BYTES`].
    BudgetExhausted,
    /// The writer would track more than
    /// [`crate::page_bytes::SURFACE_MAX_WRITTEN_RANGES`] disjoint ranges.
    TooFragmented,
}

impl WriteRefusal {
    /// The WIT variant's name.
    pub fn tag(self) -> &'static str {
        match self {
            WriteRefusal::TooLarge => "too-large",
            WriteRefusal::QuotaExceeded => "quota-exceeded",
            WriteRefusal::BlobTooLarge => "blob-too-large",
            WriteRefusal::BudgetExhausted => "budget-exhausted",
            WriteRefusal::TooFragmented => "too-fragmented",
        }
    }
}

impl From<BufferRefusal> for WriteRefusal {
    fn from(refusal: BufferRefusal) -> Self {
        match refusal {
            BufferRefusal::BlobTooLarge => WriteRefusal::BlobTooLarge,
            BufferRefusal::BudgetExhausted => WriteRefusal::BudgetExhausted,
            BufferRefusal::TooFragmented => WriteRefusal::TooFragmented,
        }
    }
}

/// `seal-error`'s `gap`: the first range below the highest byte written that
/// no write covered. A page never answers `quota-exceeded` to a seal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Gap {
    pub offset: u64,
    pub len: u64,
}

/// A blob handle the scope does not hold: never minted, already dropped, or a
/// writer's number.
///
/// The glue maps a guest's handle to the object it was given, so a component
/// cannot produce one; it is a glue fault and the caller traps.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnknownHandle(pub u32);

/// A writer call the guest may not make; the caller traps.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriterFault {
    /// No writer by that number: never minted, dropped, or a blob's number. A
    /// glue fault.
    Unknown(u32),
    /// `write` or `seal` on a writer already sealed: the component breaking
    /// the interface.
    Spent(u32),
}

/// One message of a window, as far as opening its attachments goes.
#[derive(Debug, Clone)]
struct ScopeMessage {
    message_id: Uuid,
    attachments: BTreeMap<AttachmentName, BlobRef>,
}

/// One input port's window.
#[derive(Debug, Clone)]
struct ScopePort {
    /// The channel the port is bound to: the cache's key, which is the channel
    /// that delivered the message.
    channel: String,
    messages: Vec<ScopeMessage>,
}

/// A blob in the backend store, known by the place the page was delivered it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Remote {
    /// The channel that delivered the message: the cache's key.
    pub channel: String,
    pub message_id: Uuid,
    pub name: AttachmentName,
    /// The reference the delivery carried.
    pub blob: BlobRef,
}

/// A blob a handle holds.
///
/// Equal when it is the same blob: the same page bytes buffer, not merely equal
/// bytes, or a backend reference from the same place of delivery.
#[derive(Debug, Clone)]
pub enum Blob {
    /// Bytes the page made; they are in page memory.
    Bytes(PageBytes),
    /// A backend reference; the page may or may not hold its bytes.
    Remote(Remote),
}

impl PartialEq for Blob {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Blob::Bytes(a), Blob::Bytes(b)) => a.same_blob(b),
            (Blob::Remote(a), Blob::Remote(b)) => {
                a.channel == b.channel && a.message_id == b.message_id && a.name == b.name
            }
            (Blob::Bytes(_), Blob::Remote(_)) | (Blob::Remote(_), Blob::Bytes(_)) => false,
        }
    }
}

impl Eq for Blob {}

impl Blob {
    pub fn size(&self) -> u64 {
        match self {
            Blob::Bytes(bytes) => bytes.size(),
            Blob::Remote(remote) => remote.blob.size,
        }
    }

    pub fn content_type(&self) -> &str {
        match self {
            Blob::Bytes(bytes) => bytes.content_type(),
            Blob::Remote(remote) => &remote.blob.content_type,
        }
    }
}

/// `blobs` with every repeat of the same blob taken out, in first-naming
/// order: the blobs a wire flush holds one server handle each for.
pub fn distinct_blobs<'a>(blobs: impl IntoIterator<Item = &'a Blob>) -> Vec<&'a Blob> {
    let mut distinct: Vec<&Blob> = Vec::new();
    for blob in blobs {
        if !distinct.contains(&blob) {
            distinct.push(blob);
        }
    }
    distinct
}

/// The bytes of `blobs` together, `None` past `u64`.
pub fn blobs_bytes<'a>(blobs: impl IntoIterator<Item = &'a Blob>) -> Option<u64> {
    blobs
        .into_iter()
        .try_fold(0u64, |sum, blob| sum.checked_add(blob.size()))
}

/// Whether one session of `door` may hold `count` distinct blobs of `bytes`
/// together (`None`: past `u64`): no more handles than its `max_handles`, and
/// the bytes within `max_bytes_held` by the server's own rule. Each caller
/// adds the bounds of its own moment.
pub fn door_holds(door: &BlobFacts, count: usize, bytes: Option<u64>) -> bool {
    u64::try_from(count).is_ok_and(|count| count <= u64::from(door.max_handles))
        && bytes.is_some_and(|bytes| door.admits_bytes_held(0, bytes))
}

/// What one handle number names.
#[derive(Debug)]
enum Slot {
    Writer(WriteBuffer),
    /// A sealed writer. Not counted under the handle bound: its slot passed to
    /// the blob it sealed into.
    Spent,
    Blob(Blob),
}

/// The page's source of blob handle numbers, shared by every scope the page
/// builds. Clones count from one counter.
#[derive(Debug, Clone, Default)]
pub struct HandleNumbers(Arc<AtomicU32>);

impl HandleNumbers {
    pub fn new() -> Self {
        Self::default()
    }

    /// A number this source has never answered, from 1.
    ///
    /// # Panics
    ///
    /// After 2^32 - 1 numbers: a later one would repeat an earlier one.
    fn next(&self) -> u32 {
        let previous = self
            .0
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
            .expect("surface kernel: the page minted 2^32 blob handles");
        previous + 1
    }
}

/// One activation's attachments and handle table.
#[derive(Debug, Default)]
pub struct BlobScope {
    ports: BTreeMap<String, ScopePort>,
    handles: BTreeMap<u32, Slot>,
    numbers: HandleNumbers,
    /// Bytes this activation's writes carried, refused writes excluded.
    bytes_written: u64,
}

impl BlobScope {
    /// A scope naming nothing, minting from a source of its own.
    pub fn empty() -> Self {
        Self::default()
    }

    /// A scope naming nothing, minting from `numbers`.
    pub fn numbered(numbers: &HandleNumbers) -> Self {
        Self {
            numbers: numbers.clone(),
            ..Self::default()
        }
    }

    /// The scope of `instance`'s activation: every input port window, with the
    /// channel `bindings` binds the port to, minting from the page's `numbers`.
    /// Names nothing when the instance is not granted `blobs`.
    ///
    /// A sync-call activation's request window is not in it: the request is
    /// page-local, bound to no channel, and names no attachment the page could
    /// have fetched, so `open` on the sync port is `not-permitted`.
    ///
    /// # Panics
    ///
    /// If any other window names a port `bindings` does not bind for
    /// `instance`: the activation was assembled from these bindings in the same
    /// stretch.
    pub fn of(
        bindings: &AppliedBindings,
        instance: &str,
        activation: &PageActivation,
        numbers: &HandleNumbers,
    ) -> Self {
        let mut scope = Self::numbered(numbers);
        if !bindings.instance_granted(instance, brenn_envelope::grants::ComponentGrant::Blobs) {
            return scope;
        }
        for window in &activation.ports {
            if activation.sync.as_deref() == Some(window.port.as_str()) {
                continue;
            }
            let channel = bindings
                .inputs_of(instance)
                .find(|binding| binding.port == window.port)
                .map(|binding| binding.channel.clone())
                .unwrap_or_else(|| {
                    panic!(
                        "surface kernel: {instance:?}'s activation windows port {:?}, which the \
                         bindings in force do not bind",
                        window.port
                    )
                });
            scope.add_window(&window.port, channel, &window.envelopes);
        }
        scope
    }

    /// Add one port's window.
    pub fn add_window(&mut self, port: &str, channel: String, envelopes: &[MessageEnvelope]) {
        let messages = envelopes
            .iter()
            .map(|envelope| ScopeMessage {
                message_id: envelope.message_id,
                attachments: envelope
                    .attachments
                    .iter()
                    .map(|(name, blob)| (name.clone(), blob.clone()))
                    .collect(),
            })
            .collect();
        self.ports
            .insert(port.to_string(), ScopePort { channel, messages });
    }

    /// How many handles count under [`SURFACE_MAX_HANDLES`]: unsealed
    /// writers and blobs.
    pub fn live_handles(&self) -> usize {
        self.handles
            .values()
            .filter(|slot| !matches!(slot, Slot::Spent))
            .count()
    }

    /// Bytes this activation's accepted writes carried.
    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }

    /// A fresh handle number for `slot`.
    fn mint(&mut self, slot: Slot) -> u32 {
        // Never reused in the page's life, so a stale number names nothing.
        let handle = self.numbers.next();
        self.handles.insert(handle, slot);
        handle
    }

    /// Open attachment `name` of the message at `index` in `port`'s window.
    ///
    /// On a wire-bound port, succeeds whatever the cache holds of it, except
    /// that the server answered the message has left the channel's window:
    /// reading bytes is `read`'s. On a page-local port, yields what `local`
    /// holds for the attachment's id, and `not-found` when it holds nothing —
    /// the message left the page-local window after the windows were read.
    pub fn open(
        &mut self,
        cache: &impl CacheView,
        local: &PageBlobs,
        port: &str,
        index: u32,
        name: &str,
    ) -> Result<u32, OpenRefusal> {
        let port = self.ports.get(port).ok_or(OpenRefusal::NotPermitted)?;
        let message = usize::try_from(index)
            .ok()
            .and_then(|index| port.messages.get(index))
            .ok_or(OpenRefusal::NotPermitted)?;
        let name = AttachmentName::new(name).map_err(|_| OpenRefusal::NotPermitted)?;
        let blob = message
            .attachments
            .get(&name)
            .ok_or(OpenRefusal::NotPermitted)?;
        if is_local_channel(&port.channel) {
            let held = local.get(&blob.id).ok_or(OpenRefusal::NotFound)?.clone();
            if self.live_handles() >= SURFACE_MAX_HANDLES {
                return Err(OpenRefusal::QuotaExceeded);
            }
            return Ok(self.mint(Slot::Blob(held)));
        }
        if matches!(
            cache.cached(&port.channel, message.message_id, &name),
            Cached::Gone
        ) {
            return Err(OpenRefusal::NotFound);
        }
        if self.live_handles() >= SURFACE_MAX_HANDLES {
            return Err(OpenRefusal::QuotaExceeded);
        }
        let remote = Remote {
            channel: port.channel.clone(),
            message_id: message.message_id,
            name,
            blob: blob.clone(),
        };
        Ok(self.mint(Slot::Blob(Blob::Remote(remote))))
    }

    /// A new, empty writer of `content_type`, counted into `ledger`.
    ///
    /// Checked in order: the handle bound, the content type, the page's
    /// budget for the bytes it made.
    pub fn create(
        &mut self,
        ledger: &MadeLedger,
        content_type: &str,
    ) -> Result<u32, CreateRefusal> {
        if self.live_handles() >= SURFACE_MAX_HANDLES {
            return Err(CreateRefusal::QuotaExceeded);
        }
        brenn_envelope::check_content_type(content_type)
            .map_err(|e| CreateRefusal::InvalidContentType(e.to_string()))?;
        if !ledger.has_room() {
            return Err(CreateRefusal::BudgetExhausted);
        }
        let buffer = WriteBuffer::new(ledger, content_type.to_owned());
        Ok(self.mint(Slot::Writer(buffer)))
    }

    /// Write `bytes` at `offset` of `writer`, the blob ending no further than
    /// `largest_blob` ([`largest_made_blob`]). A refusal changes nothing.
    ///
    /// Checked in order: the write's size, the activation's bytes written,
    /// then the buffer's own rules — the largest blob, the page's budget, the
    /// written-range bound.
    pub fn write(
        &mut self,
        writer: u32,
        offset: u64,
        bytes: &[u8],
        largest_blob: u64,
    ) -> Result<Result<(), WriteRefusal>, WriterFault> {
        let written = self.bytes_written;
        let buffer = self.live_writer(writer)?;
        if bytes.len() > SURFACE_MAX_WRITE_BYTES as usize {
            return Ok(Err(WriteRefusal::TooLarge));
        }
        let len = bytes.len() as u64;
        if written + len > SURFACE_MAX_BYTES_WRITTEN_PER_ACTIVATION {
            return Ok(Err(WriteRefusal::QuotaExceeded));
        }
        if let Err(refusal) = buffer.write(offset, bytes, largest_blob) {
            return Ok(Err(refusal.into()));
        }
        self.bytes_written = written + len;
        Ok(Ok(()))
    }

    /// Seal `writer` into a blob under a new handle, the writer spent; or the
    /// first gap, the writer still open. The writer's slot under the handle
    /// bound passes to the blob, so a seal never trips the bound.
    pub fn seal(&mut self, writer: u32) -> Result<Result<u32, Gap>, WriterFault> {
        self.live_writer(writer)?;
        let Some(Slot::Writer(buffer)) = self.handles.insert(writer, Slot::Spent) else {
            unreachable!("checked live above");
        };
        match buffer.seal() {
            Ok(bytes) => Ok(Ok(self.mint(Slot::Blob(Blob::Bytes(bytes))))),
            Err((buffer, gap)) => {
                self.handles.insert(writer, Slot::Writer(buffer));
                Ok(Err(Gap {
                    offset: gap.start,
                    len: gap.end - gap.start,
                }))
            }
        }
    }

    /// A guest's drop of a writer: an unsealed writer's buffer is freed, a
    /// spent writer frees nothing.
    pub fn drop_writer(&mut self, writer: u32) -> Result<(), WriterFault> {
        match self.handles.get(&writer) {
            Some(Slot::Writer(_) | Slot::Spent) => {
                self.handles.remove(&writer);
                Ok(())
            }
            Some(Slot::Blob(_)) | None => Err(WriterFault::Unknown(writer)),
        }
    }

    /// The blob `handle` holds.
    pub fn blob(&self, handle: u32) -> Result<&Blob, UnknownHandle> {
        match self.handles.get(&handle) {
            Some(Slot::Blob(blob)) => Ok(blob),
            Some(Slot::Writer(_) | Slot::Spent) | None => Err(UnknownHandle(handle)),
        }
    }

    /// The blob's size.
    pub fn size(&self, handle: u32) -> Result<u64, UnknownHandle> {
        Ok(self.blob(handle)?.size())
    }

    /// The blob's content type.
    pub fn content_type(&self, handle: u32) -> Result<&str, UnknownHandle> {
        Ok(self.blob(handle)?.content_type())
    }

    /// Exactly the bytes `[offset, offset + len)`: page bytes always, a
    /// backend reference's if the page holds them.
    pub fn read(
        &self,
        cache: &impl CacheView,
        handle: u32,
        offset: u64,
        len: u32,
    ) -> Result<Result<Vec<u8>, ReadRefusal>, UnknownHandle> {
        let blob = self.blob(handle)?;
        if len > SURFACE_MAX_READ_BYTES {
            return Ok(Err(ReadRefusal::TooLarge));
        }
        let Some(end) = offset.checked_add(u64::from(len)) else {
            return Ok(Err(ReadRefusal::OutOfRange));
        };
        if end > blob.size() {
            return Ok(Err(ReadRefusal::OutOfRange));
        }
        let held = match blob {
            Blob::Bytes(bytes) => bytes.bytes(),
            Blob::Remote(remote) => {
                match cache.cached(&remote.channel, remote.message_id, &remote.name) {
                    // Relies on the cache holding only bytes of the
                    // reference's size, so the range is inside them.
                    Cached::Bytes(bytes) => bytes,
                    Cached::Gone | Cached::Absent => return Ok(Err(ReadRefusal::Unavailable)),
                }
            }
        };
        let start = usize::try_from(offset).expect("offset is within a held blob");
        let end = usize::try_from(end).expect("end is within a held blob");
        Ok(Ok(held[start..end].to_vec()))
    }

    /// A guest's drop of a blob: frees the slot and the scope's share of the
    /// blob.
    pub fn drop_handle(&mut self, handle: u32) -> Result<(), UnknownHandle> {
        self.blob(handle)?;
        self.handles.remove(&handle);
        Ok(())
    }

    fn live_writer(&mut self, writer: u32) -> Result<&mut WriteBuffer, WriterFault> {
        match self.handles.get_mut(&writer) {
            Some(Slot::Writer(buffer)) => Ok(buffer),
            Some(Slot::Spent) => Err(WriterFault::Spent(writer)),
            Some(Slot::Blob(_)) | None => Err(WriterFault::Unknown(writer)),
        }
    }
}
