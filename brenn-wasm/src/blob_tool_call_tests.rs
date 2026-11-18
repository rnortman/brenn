//! The three `blobs` tool-call forms and `open-request` against recording
//! seams over a real blob store: their grants, their check order, and who
//! holds each blob.

use std::sync::{Arc, Mutex};

use brenn_envelope::{AttachmentName, BlobId};
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

/// A `ProvidesHost` with one pending id, `"pending"`, whose request holds
/// the attachments it is given, that keeps every completion it accepts, holds
/// included, counts the attachment lookups that reach it, and resolves nested
/// calls as [`QueueSeam`].
struct ProvidesSeam {
    queue: Arc<QueueSeam>,
    completed: Mutex<Vec<(String, HeldAttachments)>>,
    request: Mutex<HeldAttachments>,
    lookups: Mutex<usize>,
}

impl ProvidesSeam {
    fn new() -> Arc<Self> {
        Self::holding(HeldAttachments::none())
    }

    fn holding(request: HeldAttachments) -> Arc<Self> {
        Arc::new(Self {
            queue: QueueSeam::new(true),
            completed: Mutex::new(Vec::new()),
            request: Mutex::new(request),
            lookups: Mutex::new(0),
        })
    }

    /// The request ends: its holds go.
    fn end_request(&self) {
        *self.request.lock().unwrap() = HeldAttachments::none();
    }

    fn lookups(&self) -> usize {
        *self.lookups.lock().unwrap()
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

    fn request_attachment(
        &self,
        id: &str,
        name: &str,
    ) -> Result<BlobHold, RequestAttachmentRefusal> {
        *self.lookups.lock().unwrap() += 1;
        if id != "pending" {
            return Err(RequestAttachmentRefusal::NotPending);
        }
        let name = AttachmentName::new(name).map_err(|_| RequestAttachmentRefusal::NotAttached)?;
        self.request
            .lock()
            .unwrap()
            .hold(&name)
            .cloned()
            .ok_or(RequestAttachmentRefusal::NotAttached)
    }

    fn is_pending(&self, id: &str) -> bool {
        id == "pending"
    }

    fn pending_load(&self, _new_requests: &[&MessageBody]) -> PendingLoad {
        PendingLoad {
            body_bytes: 0,
            attachments: self
                .request
                .lock()
                .unwrap()
                .holds()
                .iter()
                .map(BlobHold::reference)
                .collect(),
        }
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

/// A request's attachments as the host holds them: `bytes` in one blob of
/// type `text/plain`, under each of `names`, made in `store` and held only by
/// the value returned.
fn request_holding(store: &Arc<BlobStore>, bytes: &[u8], names: &[&str]) -> HeldAttachments {
    let mut writer = store.create("text/plain").expect("created");
    writer.write(0, bytes).expect("written");
    let hold = writer.seal().expect("sealed");
    HeldAttachments::new(
        names
            .iter()
            .map(|n| (AttachmentName::new(*n).expect("a name"), hold.clone())),
    )
    .expect("within bounds")
}

fn open_request(
    data: &mut ProcessorData,
    id: &str,
    name: &str,
) -> Result<Resource<BlobHandle>, wit::OpenRequestError> {
    BlobsHost::open_request(data, id.to_string(), name.to_string()).expect("no trap")
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

/// Without `provides` every `open-request` is `not-permitted` and charged as
/// a blob call; once the blob-call budget is spent the answer is
/// `quota-exceeded`, ahead of the grant.
#[test]
fn open_request_without_provides_is_not_permitted_and_charged() {
    let mut rig = rig(None, None);
    for _ in 0..PROCESSOR_MAX_BLOB_CALLS_PER_ACTIVATION {
        assert!(matches!(
            open_request(&mut rig.data, "pending", "a"),
            Err(wit::OpenRequestError::NotPermitted)
        ));
    }
    assert!(matches!(
        open_request(&mut rig.data, "pending", "a"),
        Err(wit::OpenRequestError::QuotaExceeded)
    ));
}

/// With `provides`: the blob-call budget is judged first, then the handle
/// bound, which a full table meets without asking the seam; then the id, then
/// the name.
#[test]
fn open_request_checks_run_in_the_stated_order() {
    let dir = tempfile::tempdir().expect("blob directory");
    let store = BlobStore::for_tests(
        dir.path(),
        BlobLimits {
            max_blob_bytes: 1024,
            budget_bytes: 1024 * 1024,
        },
    );
    let provides = ProvidesSeam::holding(request_holding(&store, b"x", &["a"]));
    let mut rig = rig(None, Some(provides.clone()));

    assert!(matches!(
        open_request(&mut rig.data, "gone", "a"),
        Err(wit::OpenRequestError::NotPending)
    ));
    assert!(matches!(
        open_request(&mut rig.data, "pending", "b"),
        Err(wit::OpenRequestError::NotAttached)
    ));
    assert!(matches!(
        open_request(&mut rig.data, "pending", "not a name"),
        Err(wit::OpenRequestError::NotAttached)
    ));
    assert!(matches!(
        open_request(&mut rig.data, "gone", "b"),
        Err(wit::OpenRequestError::NotPending)
    ));
    assert_eq!(provides.lookups(), 4);

    let mut handles = Vec::new();
    for _ in 0..PROCESSOR_MAX_BLOB_HANDLES {
        handles.push(open_request(&mut rig.data, "pending", "a").expect("opened"));
    }
    assert_eq!(provides.lookups(), 4 + PROCESSOR_MAX_BLOB_HANDLES);
    assert!(matches!(
        open_request(&mut rig.data, "gone", "a"),
        Err(wit::OpenRequestError::QuotaExceeded)
    ));
    assert_eq!(
        provides.lookups(),
        4 + PROCESSOR_MAX_BLOB_HANDLES,
        "a full handle table refuses before the seam is asked"
    );
    HostBlob::drop(&mut rig.data, handles.pop().expect("a handle")).expect("dropped");
    handles.push(open_request(&mut rig.data, "pending", "a").expect("a freed slot opens"));
    for handle in handles {
        HostBlob::drop(&mut rig.data, handle).expect("dropped");
    }

    // Spend what is left of the blob-call budget: with the handle table empty,
    // the next call is refused before the seam is asked.
    let spent = 4 + PROCESSOR_MAX_BLOB_HANDLES + 2;
    for _ in spent..PROCESSOR_MAX_BLOB_CALLS_PER_ACTIVATION {
        let _ = open_request(&mut rig.data, "gone", "a");
    }
    let asked = provides.lookups();
    assert!(matches!(
        open_request(&mut rig.data, "pending", "a"),
        Err(wit::OpenRequestError::QuotaExceeded)
    ));
    assert_eq!(provides.lookups(), asked);
}

/// A handle from `open-request` reads the request's blob under either of its
/// names, holds it after the request ends, and lets it go when dropped.
#[test]
fn an_open_request_handle_reads_and_holds_the_requests_blob() {
    let dir = tempfile::tempdir().expect("blob directory");
    let store = BlobStore::for_tests(
        dir.path(),
        BlobLimits {
            max_blob_bytes: 1024,
            budget_bytes: 1024 * 1024,
        },
    );
    let provides = ProvidesSeam::holding(request_holding(&store, b"hello", &["a", "b"]));
    let mut rig = rig(None, Some(provides.clone()));

    let blob = open_request(&mut rig.data, "pending", "b").expect("opened");
    assert_eq!(
        HostBlob::size(&mut rig.data, borrow(&blob)).expect("no trap"),
        5
    );
    assert_eq!(
        HostBlob::content_type(&mut rig.data, borrow(&blob)).expect("no trap"),
        "text/plain"
    );
    let bytes = HostBlob::read(&mut rig.data, borrow(&blob), 0, 5)
        .expect("no trap")
        .expect("read");
    assert_eq!(bytes, b"hello");

    provides.end_request();
    assert_eq!(store.usage().sealed, 1, "the handle holds the blob");
    assert!(matches!(
        open_request(&mut rig.data, "pending", "a"),
        Err(wit::OpenRequestError::NotAttached)
    ));
    HostBlob::drop(&mut rig.data, blob).expect("dropped");
    assert_eq!(store.usage().sealed, 0);
}

/// A request's attachment forwarded through `call-async-for`: the queued
/// nested request holds the blob after the handle is dropped and the request
/// has ended, and lets it go when the buffer drops.
#[test]
fn a_forwarded_request_attachment_outlives_the_request() {
    let dir = tempfile::tempdir().expect("blob directory");
    let store = BlobStore::for_tests(
        dir.path(),
        BlobLimits {
            max_blob_bytes: 1024,
            budget_bytes: 1024 * 1024,
        },
    );
    let provides = ProvidesSeam::holding(request_holding(&store, b"body", &["file"]));
    let mut rig = rig(None, Some(provides.clone()));

    let blob = open_request(&mut rig.data, "pending", "file").expect("opened");
    BlobsHost::call_async_for(
        &mut rig.data,
        "pending".to_string(),
        "echo".to_string(),
        "{}".to_string(),
        "c1".to_string(),
        named(&[("body", &blob)]),
    )
    .expect("no trap")
    .expect("queued");
    HostBlob::drop(&mut rig.data, blob).expect("dropped");
    provides.end_request();
    assert_eq!(store.usage().sealed, 1, "the queued request holds the blob");

    let publishes = rig.data.take_ok_publishes();
    assert_eq!(publishes.len(), 1);
    let refs = publishes[0].attachments.refs();
    assert_eq!(refs.len(), 1);
    assert_eq!(refs.iter().next().expect("one").1.size, 4);
    drop(publishes);
    assert_eq!(store.usage().sealed, 0);
}
