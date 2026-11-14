//! The backend host side of `brenn:processor/blobs@0.3.0`.
//!
//! A component names a blob only by a resource handle: a [`BlobHandle`] it got
//! from `open`, for an attachment of a message in one of its activation's
//! input port windows, or from `writer.seal`, for a blob it made. Each handle
//! is a [`BlobHold`], so a blob with an open handle is not deleted under it,
//! and every handle lives in the activation's `ResourceTable`, which drops with
//! the activation. Nothing here cleans up at the end of an activation: what a
//! published message names is held by the publish buffer's entry, and
//! everything else lets go when the store drops.
//!
//! `open` looks an attachment up by `(port, index, name)` in a table the host
//! built from the envelopes it delivered; no function takes a blob id.

use std::sync::Arc;

use brenn_envelope::{AttachmentName, BlobId, BlobRef, MAX_ATTACHMENTS_PER_MESSAGE};
use brenn_messaging_store::blob::{BlobError, BlobHold, BlobWriter, HeldAttachments};
use tracing::error;
use wasmtime::component::Resource;

use crate::processor_v0_2_bindings::brenn::processor0_2_0::bus as bus_v0_2;
use crate::processor_v0_3_bindings::brenn::processor0_3_0::blobs as wit;
use crate::{
    PROCESSOR_MAX_BLOB_BYTES_WRITTEN_PER_ACTIVATION, PROCESSOR_MAX_BLOB_CALLS_PER_ACTIVATION,
    PROCESSOR_MAX_BLOB_HANDLES, PROCESSOR_MAX_BLOB_READ_BYTES, PROCESSOR_MAX_BLOB_WRITE_BYTES,
    ProcessorData, ProcessorUrgency, PublishRefusal,
};

/// The blob store as a component's activation reaches it.
///
/// `Some` on `ProcessorLoadSpec` iff the component holds the `Blobs` grant,
/// asserted at load. The store's own types cross the seam as themselves.
///
/// Contract: neither method may panic on guest-supplied input. `create`
/// answers a malformed content type with [`BlobError::InvalidContentType`].
pub trait BlobHost: Send + Sync {
    /// A hold on `id` if anything still holds it.
    fn retain(&self, id: &BlobId) -> Option<BlobHold>;
    /// Start a new blob.
    fn create(&self, content_type: &str) -> Result<BlobWriter, BlobError>;
}

/// Host seam for the `blobs` interface. `None` when the component lacks the
/// `Blobs` grant; the interface is then unlinked and its host functions
/// unreachable.
pub type BlobHostFn = Arc<dyn BlobHost>;

/// The host value behind a `blob` resource: a hold on a sealed blob.
pub struct BlobHandle(BlobHold);

/// The host value behind a `writer` resource: the writer, or `None` once it
/// has been sealed and is spent.
pub struct WriterHandle(Option<BlobWriter>);

/// A per-activation blob bound a call ran into; each is counted and reported
/// once, after the activation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum BlobBound {
    Calls,
    Handles,
    ReadSize,
    WriteSize,
    BytesWritten,
}

/// A blob call the store refused; each is counted and reported once, after
/// the activation, so a guest retrying against a full store or a failing disk
/// writes one line per activation and not one per call.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum StoreRefusal {
    BudgetExhausted,
    StorageFailed,
}

/// Each input port's window as the envelopes the activation was handed,
/// shared rather than copied and index-aligned: their attachments are the only
/// blobs `open` can name.
pub(crate) type HandedAttachments =
    std::collections::HashMap<String, Vec<Arc<brenn_envelope::MessageEnvelope>>>;

/// One activation's `blobs` state.
pub(crate) struct ActivationBlobs {
    /// `None` when the component lacks the `Blobs` grant; the host fns are
    /// then unreachable.
    pub(crate) host: Option<BlobHostFn>,
    /// Whether the component holds the `Ports` grant, which `blobs.publish`
    /// needs as `ports.publish` does.
    ports_granted: bool,
    /// What `open` can name. Filled before the guest runs, and only when
    /// `host` is present.
    pub(crate) handed: HandedAttachments,
    /// Live `blob` and `writer` handles.
    handles: usize,
    /// Calls of `open`, `read`, `create`, `write` and `seal`, refused or not.
    calls: usize,
    /// Bytes accepted by writes.
    bytes_written: u64,
    /// Calls refused at each bound; drained into one warn after the
    /// activation, so an operator sees a bound being hit even when the guest
    /// swallows the refusal.
    pub(crate) refusals: std::collections::BTreeMap<BlobBound, u32>,
    /// Calls the store refused, by kind; drained into one warn after the
    /// activation.
    pub(crate) store_refusals: std::collections::BTreeMap<StoreRefusal, u32>,
}

impl ActivationBlobs {
    pub(crate) fn new(host: Option<BlobHostFn>, ports_granted: bool) -> Self {
        Self {
            host,
            ports_granted,
            handed: HandedAttachments::new(),
            handles: 0,
            calls: 0,
            bytes_written: 0,
            refusals: std::collections::BTreeMap::new(),
            store_refusals: std::collections::BTreeMap::new(),
        }
    }
}

/// `BlobError` from `BlobHold::read` as the guest reads it.
///
/// # Panics
///
/// On a variant a read through a held handle cannot draw: the host checks the
/// range against the handle's size before calling the store.
fn read_error(err: BlobError) -> wit::ReadError {
    match err {
        BlobError::Io(e) => wit::ReadError::StorageFailed(storage_failed(&e)),
        BlobError::InvalidContentType(_)
        | BlobError::TooLarge { .. }
        | BlobError::BudgetExhausted { .. }
        | BlobError::TooFragmented
        | BlobError::NotFound
        | BlobError::OutOfRange { .. } => {
            panic!("host invariant: blobs.read on a held blob drew {err:?}")
        }
    }
}

/// `BlobError` from `BlobHost::create` as the guest reads it.
///
/// # Panics
///
/// On a variant creating a blob cannot draw.
fn create_error(err: BlobError) -> wit::CreateError {
    match err {
        BlobError::InvalidContentType(e) => wit::CreateError::InvalidContentType(e.to_string()),
        BlobError::BudgetExhausted { .. } => wit::CreateError::BudgetExhausted,
        BlobError::Io(e) => wit::CreateError::StorageFailed(storage_failed(&e)),
        BlobError::TooLarge { .. }
        | BlobError::TooFragmented
        | BlobError::NotFound
        | BlobError::OutOfRange { .. } => {
            panic!("host invariant: blobs.create drew {err:?}")
        }
    }
}

/// `BlobError` from `BlobWriter::write` as the guest reads it.
///
/// # Panics
///
/// On a variant a write cannot draw.
fn write_error(err: BlobError) -> wit::WriteError {
    match err {
        BlobError::TooLarge { .. } => wit::WriteError::BlobTooLarge,
        BlobError::BudgetExhausted { .. } => wit::WriteError::BudgetExhausted,
        BlobError::TooFragmented => wit::WriteError::TooFragmented,
        BlobError::Io(e) => wit::WriteError::StorageFailed(storage_failed(&e)),
        BlobError::InvalidContentType(_) | BlobError::NotFound | BlobError::OutOfRange { .. } => {
            panic!("host invariant: blobs.writer.write drew {err:?}")
        }
    }
}

/// The diagnostic a guest is handed for a filesystem failure.
fn storage_failed(e: &std::io::Error) -> String {
    format!("blob I/O: {e}")
}

impl ProcessorData {
    fn blob_host(&self) -> &BlobHostFn {
        self.blob
            .host
            .as_ref()
            .expect("blobs host fn invoked without a blob_host (Blobs grant linked but seam unset)")
    }

    /// Count one call of `open`, `read`, `create`, `write` or `seal` against
    /// the activation's ceiling; `false` once it is passed.
    fn charge_blob_call(&mut self) -> bool {
        self.blob.calls += 1;
        if self.blob.calls > PROCESSOR_MAX_BLOB_CALLS_PER_ACTIVATION {
            self.refuse_at(BlobBound::Calls);
            return false;
        }
        true
    }

    fn refuse_at(&mut self, bound: BlobBound) {
        *self.blob.refusals.entry(bound).or_insert(0) += 1;
    }

    /// Count a store refusal a guest may swallow. The activation's first
    /// filesystem failure is logged at `error` with its I/O error; the rest,
    /// and every budget refusal, reach the log only as the post-activation
    /// counts.
    pub(crate) fn note_store_error(&mut self, call: &str, err: &BlobError) {
        let kind = match err {
            BlobError::BudgetExhausted { .. } => StoreRefusal::BudgetExhausted,
            BlobError::Io(e) => {
                if !self
                    .blob
                    .store_refusals
                    .contains_key(&StoreRefusal::StorageFailed)
                {
                    error!(slug = %self.slug, call, err = %e, "wasm blobs: storage failed");
                }
                StoreRefusal::StorageFailed
            }
            BlobError::InvalidContentType(_)
            | BlobError::TooLarge { .. }
            | BlobError::TooFragmented
            | BlobError::NotFound
            | BlobError::OutOfRange { .. } => return,
        };
        *self.blob.store_refusals.entry(kind).or_insert(0) += 1;
    }

    /// Whether another live handle would pass the activation's bound.
    fn handles_full(&mut self) -> bool {
        if self.blob.handles >= PROCESSOR_MAX_BLOB_HANDLES {
            self.refuse_at(BlobBound::Handles);
            return true;
        }
        false
    }

    fn handle(&self, blob: &Resource<BlobHandle>) -> &BlobHold {
        &self
            .resource_table
            .get(blob)
            .unwrap_or_else(|e| panic!("blob resource invalid: {e}"))
            .0
    }

    /// The reference `(port, index, name)` names among the attachments this
    /// activation was handed, if any.
    fn handed_ref(&self, port: &str, index: u32, name: &str) -> Option<&BlobRef> {
        let envelope = self.blob.handed.get(port)?.get(index as usize)?;
        envelope
            .attachments
            .iter()
            .find(|(n, _)| n.as_str() == name)
            .map(|(_, r)| r)
    }

    /// The attachments of a publish, validated in the order the interface
    /// states and each holding its blob.
    pub(crate) fn held_attachments(
        &self,
        attachments: &[(String, Resource<BlobHandle>)],
    ) -> Result<HeldAttachments, PublishRefusal> {
        if attachments.len() > MAX_ATTACHMENTS_PER_MESSAGE {
            return Err(PublishRefusal::TooManyAttachments);
        }
        let mut names: Vec<AttachmentName> = Vec::with_capacity(attachments.len());
        for (name, _) in attachments {
            let name = AttachmentName::new(name.as_str())
                .map_err(|e| PublishRefusal::InvalidAttachmentName(e.to_string()))?;
            if names.contains(&name) {
                return Err(PublishRefusal::DuplicateAttachmentName(name.to_string()));
            }
            names.push(name);
        }
        let entries = names
            .into_iter()
            .zip(attachments)
            .map(|(name, (_, blob))| (name, self.handle(blob).clone()));
        Ok(HeldAttachments::new(entries).expect("count and names checked above"))
    }
}

impl wit::Host for ProcessorData {
    fn limits(&mut self) -> wasmtime::Result<wit::BlobLimits> {
        Ok(wit::BlobLimits {
            max_read_bytes: PROCESSOR_MAX_BLOB_READ_BYTES,
            max_write_bytes: PROCESSOR_MAX_BLOB_WRITE_BYTES,
        })
    }

    fn open(
        &mut self,
        port: String,
        index: u32,
        name: String,
    ) -> wasmtime::Result<Result<Resource<BlobHandle>, wit::OpenError>> {
        if !self.charge_blob_call() {
            return Ok(Err(wit::OpenError::QuotaExceeded));
        }
        let Some(reference) = self.handed_ref(&port, index, &name).cloned() else {
            return Ok(Err(wit::OpenError::NotPermitted));
        };
        if self.handles_full() {
            return Ok(Err(wit::OpenError::QuotaExceeded));
        }
        let Some(hold) = self.blob_host().retain(&reference.id) else {
            return Ok(Err(wit::OpenError::NotFound));
        };
        assert_eq!(
            hold.reference(),
            reference,
            "host invariant: the store's record of a delivered blob differs from the message's"
        );
        let handle = self.resource_table.push(BlobHandle(hold))?;
        self.blob.handles += 1;
        Ok(Ok(handle))
    }

    fn create(
        &mut self,
        content_type: String,
    ) -> wasmtime::Result<Result<Resource<WriterHandle>, wit::CreateError>> {
        if !self.charge_blob_call() || self.handles_full() {
            return Ok(Err(wit::CreateError::QuotaExceeded));
        }
        let writer = match self.blob_host().create(&content_type) {
            Ok(writer) => writer,
            Err(e) => {
                self.note_store_error("create", &e);
                return Ok(Err(create_error(e)));
            }
        };
        let handle = self.resource_table.push(WriterHandle(Some(writer)))?;
        self.blob.handles += 1;
        Ok(Ok(handle))
    }

    fn publish(
        &mut self,
        port: String,
        body: bus_v0_2::Body,
        attachments: Vec<(String, Resource<BlobHandle>)>,
        options: wit::PublishOptions,
    ) -> wasmtime::Result<Result<(), wit::PublishError>> {
        if !self.blob.ports_granted {
            // The call is charged as `ports.publish` charges it, so a component
            // without `ports` cannot loop on this for free.
            if let Err(refusal) = self.gate.charge_call() {
                return Ok(Err(crate::publish_error(refusal).into_blobs()));
            }
            return Ok(Err(wit::PublishError::NotPermitted));
        }
        let urgency = options.urgency.map(|u| match u {
            wit::Urgency::VeryLow => ProcessorUrgency::VeryLow,
            wit::Urgency::Low => ProcessorUrgency::Low,
            wit::Urgency::Normal => ProcessorUrgency::Normal,
            wit::Urgency::High => ProcessorUrgency::High,
        });
        Ok(self
            .do_publish(
                port,
                body.into(),
                urgency,
                options.deliver_after,
                &attachments,
            )?
            .map_err(PublishRefusal::into_blobs))
    }
}

impl wit::HostBlob for ProcessorData {
    fn size(&mut self, blob: Resource<BlobHandle>) -> wasmtime::Result<u64> {
        Ok(self.handle(&blob).reference().size)
    }

    fn content_type(&mut self, blob: Resource<BlobHandle>) -> wasmtime::Result<String> {
        Ok(self.handle(&blob).reference().content_type)
    }

    fn read(
        &mut self,
        blob: Resource<BlobHandle>,
        offset: u64,
        len: u32,
    ) -> wasmtime::Result<Result<Vec<u8>, wit::ReadError>> {
        if !self.charge_blob_call() {
            return Ok(Err(wit::ReadError::QuotaExceeded));
        }
        if len > PROCESSOR_MAX_BLOB_READ_BYTES {
            self.refuse_at(BlobBound::ReadSize);
            return Ok(Err(wit::ReadError::TooLarge));
        }
        let hold = self.handle(&blob);
        let size = hold.reference().size;
        if offset
            .checked_add(u64::from(len))
            .is_none_or(|end| end > size)
        {
            return Ok(Err(wit::ReadError::OutOfRange));
        }
        match hold.read(offset, len as usize) {
            Ok(bytes) => Ok(Ok(bytes)),
            Err(e) => {
                self.note_store_error("read", &e);
                Ok(Err(read_error(e)))
            }
        }
    }

    fn drop(&mut self, blob: Resource<BlobHandle>) -> wasmtime::Result<()> {
        self.resource_table.delete(blob)?;
        self.blob.handles -= 1;
        Ok(())
    }
}

impl wit::HostWriter for ProcessorData {
    fn write(
        &mut self,
        writer: Resource<WriterHandle>,
        offset: u64,
        bytes: Vec<u8>,
    ) -> wasmtime::Result<Result<(), wit::WriteError>> {
        self.live_writer(&writer, "write")?;
        if !self.charge_blob_call() {
            return Ok(Err(wit::WriteError::QuotaExceeded));
        }
        if bytes.len() > PROCESSOR_MAX_BLOB_WRITE_BYTES as usize {
            self.refuse_at(BlobBound::WriteSize);
            return Ok(Err(wit::WriteError::TooLarge));
        }
        let len = bytes.len() as u64;
        if self.blob.bytes_written + len > PROCESSOR_MAX_BLOB_BYTES_WRITTEN_PER_ACTIVATION {
            self.refuse_at(BlobBound::BytesWritten);
            return Ok(Err(wit::WriteError::QuotaExceeded));
        }
        let w = self
            .resource_table
            .get_mut(&writer)?
            .0
            .as_mut()
            .expect("checked live above");
        match w.write(offset, &bytes) {
            Ok(()) => {
                self.blob.bytes_written += len;
                Ok(Ok(()))
            }
            Err(e) => {
                self.note_store_error("write", &e);
                Ok(Err(write_error(e)))
            }
        }
    }

    fn seal(
        &mut self,
        writer: Resource<WriterHandle>,
    ) -> wasmtime::Result<Result<Resource<BlobHandle>, wit::SealError>> {
        self.live_writer(&writer, "seal")?;
        if !self.charge_blob_call() {
            return Ok(Err(wit::SealError::QuotaExceeded));
        }
        let w = self
            .resource_table
            .get_mut(&writer)?
            .0
            .take()
            .expect("checked live above");
        match w.seal() {
            // The writer's slot under the handle bound passes to the blob: a
            // spent writer is not counted, so a seal never trips the bound.
            Ok(hold) => Ok(Ok(self.resource_table.push(BlobHandle(hold))?)),
            Err(refused) => {
                let gap = wit::Gap {
                    offset: refused.gap.start,
                    len: refused.gap.end - refused.gap.start,
                };
                self.resource_table.get_mut(&writer)?.0 = Some(refused.writer);
                Ok(Err(wit::SealError::Gap(gap)))
            }
        }
    }

    fn drop(&mut self, writer: Resource<WriterHandle>) -> wasmtime::Result<()> {
        if self.resource_table.delete(writer)?.0.is_some() {
            self.blob.handles -= 1;
        }
        Ok(())
    }
}

impl ProcessorData {
    /// Trap unless `writer` is still unsealed: a call on a spent writer is a
    /// component breaking the interface.
    fn live_writer(&self, writer: &Resource<WriterHandle>, call: &str) -> wasmtime::Result<()> {
        if self.resource_table.get(writer)?.0.is_none() {
            return Err(wasmtime::Error::msg(format!(
                "processor {}: blobs.writer.{call} on a writer already sealed",
                self.slug
            )));
        }
        Ok(())
    }
}

impl PublishRefusal {
    pub(crate) fn into_blobs(self) -> wit::PublishError {
        match self {
            Self::NotPermitted => wit::PublishError::NotPermitted,
            Self::InvalidPayload(why) => wit::PublishError::InvalidPayload(why),
            Self::InvalidBody(why) => wit::PublishError::InvalidBody(why),
            Self::TextOnlyChannel => wit::PublishError::TextOnlyChannel,
            Self::QuotaExceeded => wit::PublishError::QuotaExceeded,
            Self::InvalidAttachmentName(why) => wit::PublishError::InvalidAttachmentName(why),
            Self::DuplicateAttachmentName(name) => wit::PublishError::DuplicateAttachmentName(name),
            Self::TooManyAttachments => wit::PublishError::TooManyAttachments,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use brenn_envelope::BodyError;

    fn io() -> BlobError {
        BlobError::Io(std::io::Error::other("disk full"))
    }

    #[test]
    fn io_is_storage_failed_on_every_call() {
        assert!(matches!(
            read_error(io()),
            wit::ReadError::StorageFailed(s) if s.contains("disk full")
        ));
        assert!(matches!(
            create_error(io()),
            wit::CreateError::StorageFailed(s) if s.contains("disk full")
        ));
        assert!(matches!(
            write_error(io()),
            wit::WriteError::StorageFailed(s) if s.contains("disk full")
        ));
    }

    #[test]
    fn create_maps_content_type_and_budget() {
        assert!(matches!(
            create_error(BlobError::InvalidContentType(
                BodyError::InvalidContentType {
                    content_type: String::new(),
                    reason: "empty",
                }
            )),
            wit::CreateError::InvalidContentType(_)
        ));
        assert!(matches!(
            create_error(BlobError::BudgetExhausted { budget: 1 }),
            wit::CreateError::BudgetExhausted
        ));
    }

    #[test]
    fn write_maps_the_stores_bounds() {
        assert!(matches!(
            write_error(BlobError::TooLarge { max: 1 }),
            wit::WriteError::BlobTooLarge
        ));
        assert!(matches!(
            write_error(BlobError::BudgetExhausted { budget: 1 }),
            wit::WriteError::BudgetExhausted
        ));
        assert!(matches!(
            write_error(BlobError::TooFragmented),
            wit::WriteError::TooFragmented
        ));
    }

    #[test]
    #[should_panic(expected = "blobs.read on a held blob drew NotFound")]
    fn read_panics_on_not_found() {
        read_error(BlobError::NotFound);
    }

    #[test]
    #[should_panic(expected = "blobs.read on a held blob drew OutOfRange")]
    fn read_panics_on_out_of_range() {
        read_error(BlobError::OutOfRange {
            offset: 0,
            len: 1,
            size: 0,
        });
    }

    #[test]
    #[should_panic(expected = "blobs.create drew TooFragmented")]
    fn create_panics_on_a_write_variant() {
        create_error(BlobError::TooFragmented);
    }

    #[test]
    #[should_panic(expected = "blobs.writer.write drew NotFound")]
    fn write_panics_on_not_found() {
        write_error(BlobError::NotFound);
    }
}
