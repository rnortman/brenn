//! The three `blobs` tool-call forms against recording seams over a real
//! blob store: their grants, their check order, and who holds each blob.

use std::sync::{Arc, Mutex};

use brenn_envelope::BlobId;
use brenn_messaging_store::blob::{
    BlobError, BlobHold, BlobLimits, BlobStore, BlobWriter, HeldAttachments, Retain,
};
use wasmtime::component::Resource;

use super::*;
use crate::blobs::{BlobHandle, BlobHost};
use crate::processor_store_host_tests::make_tool_data;
use crate::processor_v0_3_bindings::brenn::processor0_3_0::blobs::{
    self as wit, Host as BlobsHost, HostBlob, HostWriter,
};

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

/// A `ToolHost` whose one async tool takes attachments iff `accepts`, and
/// which records the flag of every call that reaches it.
struct QueueSeam {
    accepts: bool,
    calls: Mutex<Vec<bool>>,
}

impl QueueSeam {
    fn new(accepts: bool) -> Arc<Self> {
        Arc::new(Self {
            accepts,
            calls: Mutex::new(Vec::new()),
        })
    }

    fn resolve(
        &self,
        tool: &str,
        with_attachments: bool,
    ) -> Result<QueuedToolRequest, ToolCallError> {
        self.calls.lock().unwrap().push(with_attachments);
        if with_attachments && !self.accepts {
            return Err(ToolCallError::AttachmentsNotAccepted);
        }
        Ok(QueuedToolRequest {
            channel: format!("brenn:tools/{tool}"),
            reply_to: "brenn:tool-results/test-slug".to_string(),
            body_json: "{\"v\":1}".to_string(),
        })
    }

    fn calls(&self) -> Vec<bool> {
        self.calls.lock().unwrap().clone()
    }
}

impl ToolHost for QueueSeam {
    fn fast_call(&self, _tool: &str, _args_json: &str) -> Result<String, ToolCallError> {
        Err(ToolCallError::WrongClass)
    }

    fn queue_async(
        &self,
        tool: &str,
        _args_json: &str,
        _call_id: &str,
        with_attachments: bool,
    ) -> Result<QueuedToolRequest, ToolCallError> {
        self.resolve(tool, with_attachments)
    }
}

/// A `ProvidesHost` with one pending id that keeps every completion it
/// accepts, holds included, and resolves nested calls as [`QueueSeam`].
struct ProvidesSeam {
    queue: Arc<QueueSeam>,
    completed: Mutex<Vec<(String, HeldAttachments)>>,
}

impl ProvidesSeam {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            queue: QueueSeam::new(true),
            completed: Mutex::new(Vec::new()),
        })
    }
}

impl ProvidesHost for ProvidesSeam {
    fn complete(
        &self,
        id: &str,
        _outcome_json: &str,
        attachments: HeldAttachments,
    ) -> Result<(), ProvidesCallError> {
        if id != "pending" {
            return Err(ProvidesCallError::NotPending);
        }
        self.completed
            .lock()
            .unwrap()
            .push((id.to_string(), attachments));
        Ok(())
    }

    fn queue_async_for(
        &self,
        _serving: &str,
        tool: &str,
        _args_json: &str,
        _call_id: &str,
        with_attachments: bool,
    ) -> Result<QueuedToolRequest, ToolCallError> {
        self.queue.resolve(tool, with_attachments)
    }
}

/// Fields drop in order: the holds in `data` before the directory their
/// files are in.
struct Rig {
    data: ProcessorData,
    store: Arc<BlobStore>,
    _dir: tempfile::TempDir,
}

/// `ProcessorData` with the `blobs` seam over a fresh store, the given tool
/// and provides seams, and no other.
fn rig(tool_host: Option<ToolHostFn>, provides_host: Option<ProvidesHostFn>) -> Rig {
    let dir = tempfile::tempdir().expect("blob directory");
    let store = BlobStore::for_tests(
        dir.path(),
        BlobLimits {
            max_blob_bytes: 1024 * 1024,
            budget_bytes: 8 * 1024 * 1024,
        },
    );
    let mut data = make_tool_data(QueueSeam::new(false));
    data.tool_host = tool_host;
    data.provides_host = provides_host;
    data.blob = blobs::ActivationBlobs::new(Some(Arc::new(StoreSeam(Arc::clone(&store)))), false);
    Rig {
        _dir: dir,
        store,
        data,
    }
}

impl Rig {
    /// A blob the guest made and sealed, by its handle.
    fn sealed(&mut self, bytes: &[u8]) -> Resource<BlobHandle> {
        let writer = BlobsHost::create(&mut self.data, "application/octet-stream".to_string())
            .expect("no trap")
            .expect("created");
        let w = Resource::<blobs::WriterHandle>::new_borrow(writer.rep());
        HostWriter::write(&mut self.data, w, 0, bytes.to_vec())
            .expect("no trap")
            .expect("written");
        let w = Resource::<blobs::WriterHandle>::new_borrow(writer.rep());
        let blob = HostWriter::seal(&mut self.data, w)
            .expect("no trap")
            .expect("sealed");
        HostWriter::drop(&mut self.data, writer).expect("dropped");
        blob
    }

    fn sealed_count(&self) -> usize {
        self.store.usage().sealed
    }
}

fn borrow(handle: &Resource<BlobHandle>) -> Resource<BlobHandle> {
    Resource::new_borrow(handle.rep())
}

fn named(entries: &[(&str, &Resource<BlobHandle>)]) -> Vec<(String, Resource<BlobHandle>)> {
    entries
        .iter()
        .map(|(name, handle)| (name.to_string(), borrow(handle)))
        .collect()
}

fn call(
    data: &mut ProcessorData,
    args_json: &str,
    attachments: Vec<(String, Resource<BlobHandle>)>,
) -> Result<(), wit::CallError> {
    BlobsHost::call_async(
        data,
        "echo".to_string(),
        args_json.to_string(),
        "c1".to_string(),
        attachments,
    )
    .expect("no trap")
}

/// A queued request holds its blobs from the call: the guest drops its
/// handle, the flush carries the attachment under the name given, and the
/// blob is deleted only when the flushed request lets go.
#[test]
fn a_queued_request_holds_its_blobs_into_the_flush() {
    let seam = QueueSeam::new(true);
    let mut rig = rig(Some(seam.clone()), None);
    let blob = rig.sealed(b"hello");
    call(&mut rig.data, "{}", named(&[("a", &blob), ("b", &blob)])).expect("queued");
    assert_eq!(seam.calls(), vec![true]);
    HostBlob::drop(&mut rig.data, blob).expect("dropped");
    assert_eq!(rig.sealed_count(), 1, "the buffered request holds the blob");

    let publishes = rig.data.take_ok_publishes();
    assert_eq!(publishes.len(), 1);
    assert_eq!(publishes[0].channel_address, "brenn:tools/echo");
    let refs = publishes[0].attachments.refs();
    let names: Vec<&str> = refs.iter().map(|(n, _)| n.as_str()).collect();
    assert_eq!(names, ["a", "b"]);
    let ids: Vec<_> = refs.iter().map(|(_, r)| r.id.clone()).collect();
    assert_eq!(ids[0], ids[1], "one blob under two names");
    assert!(refs.iter().all(|(_, r)| r.size == 5));
    assert_eq!(publishes[0].attachments.holds().len(), 1);
    drop(publishes);
    assert_eq!(rig.sealed_count(), 0);
}

/// An activation that does not return `ok` never flushes: its buffered
/// request and the holds in it drop with the store.
#[test]
fn a_failed_activation_drops_the_requests_holds() {
    let mut rig = rig(Some(QueueSeam::new(true)), None);
    let blob = rig.sealed(b"hello");
    call(&mut rig.data, "{}", named(&[("a", &blob)])).expect("queued");
    HostBlob::drop(&mut rig.data, blob).expect("dropped");
    assert_eq!(rig.sealed_count(), 1);
    let Rig { store, data, _dir } = rig;
    drop(data);
    assert_eq!(store.usage().sealed, 0);
}

/// An empty list is the plain form: the seam is told no attachments and the
/// request carries none.
#[test]
fn an_empty_list_behaves_as_the_plain_form() {
    let seam = QueueSeam::new(false);
    let mut rig = rig(Some(seam.clone()), None);
    call(&mut rig.data, "{}", Vec::new()).expect("queued");
    assert_eq!(seam.calls(), vec![false]);
    let publishes = rig.data.take_ok_publishes();
    assert!(publishes[0].attachments.is_empty());
}

/// Without the tool host `call-async` is `not-granted`, charged against the
/// call cap, before its attachments are judged.
#[test]
fn call_async_without_tools_is_not_granted_before_the_attachments() {
    let mut rig = rig(None, None);
    let blob = rig.sealed(b"x");
    let bad = named(&[("no good", &blob)]);
    assert!(matches!(
        call(&mut rig.data, "{}", bad),
        Err(wit::CallError::Plain(ToolWitError::NotGranted))
    ));
    assert_eq!(rig.data.tool_call_count, 1);
    rig.data.tool_call_count = PROCESSOR_MAX_FAST_TOOL_CALLS_PER_ACTIVATION;
    assert!(matches!(
        call(&mut rig.data, "{}", Vec::new()),
        Err(wit::CallError::Plain(ToolWitError::Internal(t))) if t == "call-count-exceeded"
    ));
}

/// Each attachment bound draws its variant, after the `args-json` cap and
/// before the seam.
#[test]
fn the_attachment_bounds_draw_their_variants_before_the_seam() {
    let seam = QueueSeam::new(true);
    let mut rig = rig(Some(seam.clone()), None);
    let blob = rig.sealed(b"x");

    let names: Vec<String> = (0..=brenn_envelope::MAX_ATTACHMENTS_PER_MESSAGE)
        .map(|i| format!("a{i}"))
        .collect();
    let too_many: Vec<(&str, &Resource<BlobHandle>)> =
        names.iter().map(|n| (n.as_str(), &blob)).collect();
    assert!(matches!(
        call(&mut rig.data, "{}", named(&too_many)),
        Err(wit::CallError::TooManyAttachments)
    ));
    assert!(matches!(
        call(&mut rig.data, "{}", named(&[("-bad", &blob)])),
        Err(wit::CallError::InvalidAttachmentName(_))
    ));
    assert!(matches!(
        call(&mut rig.data, "{}", named(&[("a", &blob), ("a", &blob)])),
        Err(wit::CallError::DuplicateAttachmentName(n)) if n == "a"
    ));
    let big = "x".repeat(PROCESSOR_MAX_TOOL_ARGS_BYTES + 1);
    assert!(matches!(
        call(&mut rig.data, &big, named(&[("-bad", &blob)])),
        Err(wit::CallError::Plain(ToolWitError::InvalidArgs(_)))
    ));
    assert!(seam.calls().is_empty(), "no refused call reached the seam");
    assert!(rig.data.tool_request_buffer.is_empty());
}

/// A tool that takes no attachments refuses them at the seam, after its own
/// checks; nothing is buffered.
#[test]
fn a_tool_that_takes_none_is_attachments_not_accepted() {
    let seam = QueueSeam::new(false);
    let mut rig = rig(Some(seam.clone()), None);
    let blob = rig.sealed(b"x");
    assert!(matches!(
        call(&mut rig.data, "{}", named(&[("a", &blob)])),
        Err(wit::CallError::AttachmentsNotAccepted)
    ));
    assert_eq!(seam.calls(), vec![true]);
    assert!(rig.data.tool_request_buffer.is_empty());
}

/// `call-async-for` needs `provides` and not `tools`: with the provides host
/// alone it queues with its holds; with the tool host alone it is
/// `not-granted`.
#[test]
fn call_async_for_needs_provides_and_not_tools() {
    let provides = ProvidesSeam::new();
    let mut rig = rig(None, Some(provides.clone()));
    let blob = rig.sealed(b"x");
    BlobsHost::call_async_for(
        &mut rig.data,
        "pending".to_string(),
        "echo".to_string(),
        "{}".to_string(),
        "c1".to_string(),
        named(&[("a", &blob)]),
    )
    .expect("no trap")
    .expect("queued");
    assert_eq!(provides.queue.calls(), vec![true]);
    let publishes = rig.data.take_ok_publishes();
    assert_eq!(publishes[0].attachments.refs().len(), 1);

    let mut rig = self::rig(Some(QueueSeam::new(true)), None);
    let blob = rig.sealed(b"x");
    assert!(matches!(
        BlobsHost::call_async_for(
            &mut rig.data,
            "pending".to_string(),
            "echo".to_string(),
            "{}".to_string(),
            "c1".to_string(),
            named(&[("a", &blob)]),
        )
        .expect("no trap"),
        Err(wit::CallError::Plain(ToolWitError::NotGranted))
    ));
}

fn complete(
    data: &mut ProcessorData,
    id: &str,
    attachments: Vec<(String, Resource<BlobHandle>)>,
) -> wasmtime::Result<Result<(), wit::CompleteError>> {
    BlobsHost::complete(data, id.to_string(), "{\"ok\":1}".to_string(), attachments)
}

/// A completion's holds reach the seam at the call and outlive the
/// activation: the blob the guest made survives the store's drop, held by
/// the completion alone.
#[test]
fn a_completions_holds_survive_the_activation() {
    let provides = ProvidesSeam::new();
    let mut rig = rig(None, Some(provides.clone()));
    let blob = rig.sealed(b"answer");
    complete(&mut rig.data, "pending", named(&[("out", &blob)]))
        .expect("no trap")
        .expect("completed");
    let Rig { store, data, _dir } = rig;
    drop(data);
    assert_eq!(store.usage().sealed, 1, "the completion holds the blob");
    let completed = std::mem::take(&mut *provides.completed.lock().unwrap());
    assert_eq!(completed.len(), 1);
    assert_eq!(completed[0].1.refs().len(), 1);
    drop(completed);
    assert_eq!(store.usage().sealed, 0);
}

/// `complete` without `provides` is `not-permitted`, charged against the
/// completion cap; its attachment bounds draw their variants; `not-pending`
/// passes through; and the call past the cap traps.
#[test]
fn complete_refusals_and_its_cap() {
    let mut rig = rig(None, None);
    let blob = rig.sealed(b"x");
    assert!(matches!(
        complete(&mut rig.data, "pending", named(&[("-bad", &blob)])).expect("no trap"),
        Err(wit::CompleteError::NotPermitted)
    ));
    assert_eq!(rig.data.complete_call_count, 1);

    let provides = ProvidesSeam::new();
    let mut rig = self::rig(None, Some(provides.clone()));
    let blob = rig.sealed(b"x");
    assert!(matches!(
        complete(&mut rig.data, "pending", named(&[("-bad", &blob)])).expect("no trap"),
        Err(wit::CompleteError::InvalidAttachmentName(_))
    ));
    assert!(matches!(
        complete(
            &mut rig.data,
            "pending",
            named(&[("a", &blob), ("a", &blob)])
        )
        .expect("no trap"),
        Err(wit::CompleteError::DuplicateAttachmentName(n)) if n == "a"
    ));
    assert!(matches!(
        complete(&mut rig.data, "gone", named(&[("a", &blob)])).expect("no trap"),
        Err(wit::CompleteError::Plain(CompleteWitError::NotPending))
    ));
    let over = "x".repeat(PROCESSOR_MAX_COMPLETE_OUTCOME_BYTES + 1);
    assert!(matches!(
        BlobsHost::complete(
            &mut rig.data,
            "pending".to_string(),
            over,
            named(&[("-bad", &blob)])
        )
        .expect("no trap"),
        Err(wit::CompleteError::Plain(CompleteWitError::InvalidOutcome(
            _
        )))
    ));
    assert!(provides.completed.lock().unwrap().is_empty());

    rig.data.complete_call_count = PROCESSOR_MAX_COMPLETE_CALLS_PER_ACTIVATION;
    let trap = complete(&mut rig.data, "pending", Vec::new()).expect_err("traps");
    assert!(
        trap.to_string()
            .contains("provides.complete calls in one activation")
    );
}
