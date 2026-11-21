//! `brenn-provider-kit`'s activation loop, served through the `brenn-guest`
//! SDK.
//!
//! A provider component's `Processor::receive` reads its settings and calls
//! [`serve`] or [`serve_with_blobs`] with its [`Logic`]; everything else — the
//! sweep, the results, the backlog, the new requests, the call budget — is the
//! kit's loop, run against one store transaction that commits when the loop
//! returns `Ok`, so the nested calls it buffered flush against committed
//! state. An `Err` drops the transaction, which rolls back every row the
//! activation wrote.
//!
//! The component's class needs the `provides`, `store` and `log` grants and a
//! `tool-results` inbox; nested calls reach `api-request`, or the other tool a
//! call names, on behalf of the request's user. A completion the host no longer holds is dropped.
//!
//! The entry point is the choice of grant. [`serve`] serves the loop no
//! blobs: its [`Blobs`] answers `Unavailable` from every method and it
//! reaches no function of the SDK's `blobs` module, so a component served by
//! it imports no `blobs` interface and needs no `blobs` grant; a call whose
//! body the component builds fails its request `internal`, naming
//! [`serve_with_blobs`]. [`serve_with_blobs`] serves the SDK's `blobs`
//! interface, so a component served by it imports `blobs` and its class
//! requires the grant.

use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;

use brenn_guest::provides::{
    self, OutcomeErrorKind, PendingId, RequestAttachmentError, drop_not_pending,
};
use brenn_guest::serde_json::Value;
use brenn_guest::tools::ToolResult;
use brenn_guest::{Activation, Attachment, Error, blobs, log, store};
use brenn_provider_kit::{
    Attached, Blob, BlobLimits, Blobs, CreateError, Delivered, ErrorKind, Host, Input, Level,
    Logic, Nested, NestedError, OpenError, ReadError, Request, Rows, SealError, Unavailable,
    WriteError, Writer,
};

/// Serve one activation of a provider built on `brenn-provider-kit`, with no
/// blobs: one store transaction, the kit's loop, commit.
pub fn serve<L: Logic>(activation: &Activation, settings: &L::Settings) -> Result<(), Error> {
    let results = activation.tool_results()?;
    run::<L, _>(activation, settings, &results, NoBlobs)
}

/// [`serve`], with the SDK's `blobs` interface behind the loop's [`Blobs`]:
/// a step opens its request's and its answer's attachments, reads and builds
/// blobs, forwards a request attachment as a nested call's body, builds a
/// nested call's body (`Logic::body`) and completes with attachments. The
/// component's class requires `blobs`.
pub fn serve_with_blobs<L: Logic>(
    activation: &Activation,
    settings: &L::Settings,
) -> Result<(), Error> {
    let results = activation.tool_results()?;
    let blobs = SdkBlobs {
        results: &results,
        table: RefCell::new(BTreeMap::new()),
        next: Cell::new(0),
    };
    run::<L, _>(activation, settings, &results, blobs)
}

fn run<L: Logic, B: Served>(
    activation: &Activation,
    settings: &L::Settings,
    results: &[ToolResult],
    blobs: B,
) -> Result<(), Error> {
    let now_ms = activation
        .now()
        .expect("backend activation carries a host-stamped now");
    let requests = activation
        .tool_requests()?
        .into_iter()
        .map(|request| Request {
            id: request.id.as_str().to_owned(),
            tool: request.tool,
            args: request.args,
            attachments: attached(&request.attachments),
        })
        .collect();
    let results = results
        .iter()
        .map(|result| Delivered {
            call_id: result.call_id.clone(),
            serving: result.serving.as_ref().map(|id| id.as_str().to_owned()),
            outcome: result.outcome.clone().map_err(|error| NestedError {
                kind: error.kind,
                detail: error.detail,
            }),
            attachments: attached(&result.attachments),
        })
        .collect();
    let tx = store::begin()?;
    brenn_provider_kit::serve::<_, L>(
        &GuestHost { tx: &tx, blobs },
        settings,
        Input {
            now_ms,
            requests,
            results,
        },
    )?;
    tx.commit()
}

/// A message's attachments as the kit names them.
fn attached(attachments: &[Attachment]) -> Vec<Attached> {
    attachments
        .iter()
        .map(|attachment| Attached {
            name: attachment.name().as_str().to_owned(),
            size: attachment.size(),
            content_type: attachment.content_type().to_owned(),
        })
        .collect()
}

/// The [`Blobs`] an entry point serves, and the two `provides` calls whose
/// form depends on it.
trait Served: Blobs {
    fn call(
        &self,
        id: &PendingId,
        call: &Nested,
        call_id: &str,
        body: Option<(&str, &Blob)>,
    ) -> Result<(), String>;

    fn complete(
        &self,
        id: &PendingId,
        value: &Value,
        attachments: &[(String, Blob)],
    ) -> Result<(), Error>;
}

/// The kit's [`Host`] over one store transaction, the SDK's `provides` and
/// `log` calls, and the entry point's [`Served`].
struct GuestHost<'a, B> {
    tx: &'a store::Transaction,
    blobs: B,
}

impl<B: Blobs> Blobs for GuestHost<'_, B> {
    fn limits(&self) -> Result<BlobLimits, Unavailable> {
        self.blobs.limits()
    }

    fn open_request(&self, id: &str, name: &str) -> Result<Blob, OpenError> {
        self.blobs.open_request(id, name)
    }

    fn open_result(&self, call_id: &str, name: &str) -> Result<Blob, OpenError> {
        self.blobs.open_result(call_id, name)
    }

    fn read(&self, blob: &Blob, offset: u64, len: u32) -> Result<Vec<u8>, ReadError> {
        self.blobs.read(blob, offset, len)
    }

    fn create(&self, content_type: &str) -> Result<Writer, CreateError> {
        self.blobs.create(content_type)
    }

    fn write(&self, writer: &Writer, offset: u64, bytes: &[u8]) -> Result<(), WriteError> {
        self.blobs.write(writer, offset, bytes)
    }

    fn seal(&self, writer: Writer) -> Result<Blob, SealError> {
        self.blobs.seal(writer)
    }

    fn release(&self) {
        self.blobs.release()
    }
}

impl<B: Served> Host for GuestHost<'_, B> {
    type Error = Error;

    fn fault(detail: String) -> Error {
        Error::failed(detail)
    }

    fn get(&self, ns: &str, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        self.tx.get(ns, key)
    }

    fn put(&self, ns: &str, key: &[u8], value: &[u8]) -> Result<(), Error> {
        self.tx.put(ns, key, value)
    }

    fn delete(&self, ns: &str, key: &[u8]) -> Result<(), Error> {
        self.tx.delete(ns, key)
    }

    fn scan(&self, ns: &str) -> Result<Rows, Error> {
        self.tx.scan(ns, b"", None, 0)
    }

    fn call(
        &self,
        id: &str,
        call: &Nested,
        call_id: &str,
        body: Option<(&str, &Blob)>,
    ) -> Result<(), String> {
        self.blobs.call(&pending(id), call, call_id, body)
    }

    fn complete(
        &self,
        id: &str,
        value: &Value,
        attachments: &[(String, Blob)],
    ) -> Result<(), Error> {
        self.blobs.complete(&pending(id), value, attachments)
    }

    fn complete_err(&self, id: &str, kind: ErrorKind, detail: &str) -> Result<(), Error> {
        drop_not_pending(provides::complete_err(
            &pending(id),
            outcome_kind(kind),
            detail,
        ))
    }

    fn pending(&self, id: &str) -> bool {
        provides::pending(&pending(id))
    }

    fn log(&self, level: Level, line: String) {
        match level {
            Level::Debug => log::debug(line),
            Level::Info => log::info(line),
            Level::Warn => log::warn(line),
        }
    }
}

/// No blobs: a [`Blob`] or [`Writer`] is never handed out, so neither a body
/// attachment nor a completion's attachment reaches [`Served`].
struct NoBlobs;

impl Blobs for NoBlobs {
    fn limits(&self) -> Result<BlobLimits, Unavailable> {
        Err(Unavailable)
    }

    fn open_request(&self, _: &str, _: &str) -> Result<Blob, OpenError> {
        Err(OpenError::Unavailable)
    }

    fn open_result(&self, _: &str, _: &str) -> Result<Blob, OpenError> {
        Err(OpenError::Unavailable)
    }

    fn read(&self, _: &Blob, _: u64, _: u32) -> Result<Vec<u8>, ReadError> {
        Err(ReadError::Unavailable)
    }

    fn create(&self, _: &str) -> Result<Writer, CreateError> {
        Err(CreateError::Unavailable)
    }

    fn write(&self, _: &Writer, _: u64, _: &[u8]) -> Result<(), WriteError> {
        Err(WriteError::Unavailable)
    }

    fn seal(&self, _: Writer) -> Result<Blob, SealError> {
        Err(SealError::Unavailable)
    }

    fn release(&self) {}
}

impl Served for NoBlobs {
    fn call(
        &self,
        id: &PendingId,
        call: &Nested,
        call_id: &str,
        body: Option<(&str, &Blob)>,
    ) -> Result<(), String> {
        if body.is_some() {
            return Err(brenn_provider_kit::Failure::from(Unavailable).detail);
        }
        call_async_for(id, call, call_id)
    }

    fn complete(
        &self,
        id: &PendingId,
        value: &Value,
        attachments: &[(String, Blob)],
    ) -> Result<(), Error> {
        if !attachments.is_empty() {
            return Err(Error::failed(
                brenn_provider_kit::Failure::from(Unavailable).detail,
            ));
        }
        drop_not_pending(provides::complete(id, value))
    }
}

/// `provides::call_async_for` of `call`'s arguments to its tool, with no
/// attachment.
fn call_async_for(id: &PendingId, call: &Nested, call_id: &str) -> Result<(), String> {
    match call {
        Nested::Api(api) => provides::call_async_for(id, call.tool(), api, call_id),
        Nested::Tool(tool) => provides::call_async_for(id, call.tool(), &tool.args, call_id),
    }
    .map_err(|refused| refused.to_string())
}

/// One handle of a step's table.
enum Entry {
    Blob(blobs::Blob),
    Writer(blobs::Writer),
}

/// The SDK's `blobs` interface, by slot. Slot numbers are never reused within
/// the activation, so a handle kept past its step's [`Blobs::release`] finds
/// no entry rather than another step's blob.
struct SdkBlobs<'a> {
    /// The results delivered new, for [`Blobs::open_result`].
    results: &'a [ToolResult],
    table: RefCell<BTreeMap<u32, Entry>>,
    next: Cell<u32>,
}

impl SdkBlobs<'_> {
    fn insert(&self, entry: Entry) -> u32 {
        let slot = self.next.get();
        self.next
            .set(slot.checked_add(1).expect("fewer than 2^32 blob handles"));
        self.table.borrow_mut().insert(slot, entry);
        slot
    }

    fn hold(&self, raw: blobs::Blob) -> Blob {
        let (size, content_type) = (raw.size(), raw.content_type());
        Blob::new(self.insert(Entry::Blob(raw)), size, content_type)
    }

    /// The SDK blob behind `blob`, for `use`. A slot that holds none is a
    /// handle used after its step ended — the component's fault.
    fn with_blob<T>(&self, blob: &Blob, use_: impl FnOnce(&blobs::Blob) -> T) -> T {
        match self.table.borrow().get(&blob.slot()) {
            Some(Entry::Blob(raw)) => use_(raw),
            _ => panic!("blob handle {} used after its step ended", blob.slot()),
        }
    }

    /// The SDK blobs behind `attachments`, by name, for one call.
    fn with_blobs<T>(
        &self,
        attachments: &[(&str, &Blob)],
        use_: impl FnOnce(&[(&str, &blobs::Blob)]) -> T,
    ) -> T {
        let table = self.table.borrow();
        let raw: Vec<(&str, &blobs::Blob)> = attachments
            .iter()
            .map(|(name, blob)| match table.get(&blob.slot()) {
                Some(Entry::Blob(raw)) => (*name, raw),
                _ => panic!("blob handle {} used after its step ended", blob.slot()),
            })
            .collect();
        use_(&raw)
    }
}

impl Blobs for SdkBlobs<'_> {
    fn limits(&self) -> Result<BlobLimits, Unavailable> {
        let limits = blobs::limits();
        Ok(BlobLimits {
            max_read_bytes: limits.max_read_bytes,
            max_write_bytes: limits.max_write_bytes,
        })
    }

    fn open_request(&self, id: &str, name: &str) -> Result<Blob, OpenError> {
        match provides::open_request_attachment(&pending(id), name) {
            Ok(raw) => Ok(self.hold(raw)),
            Err(RequestAttachmentError::NotPending) => Err(OpenError::NotPending),
            Err(RequestAttachmentError::NotAttached) => Err(OpenError::NotAttached),
            Err(RequestAttachmentError::QuotaExceeded) => Err(OpenError::Quota),
            // The kit completes through `provides`, so the component holds
            // the grant; a host that says otherwise has broken that premise.
            Err(RequestAttachmentError::NotPermitted) => {
                panic!("the host refuses open-request for lack of `provides`")
            }
        }
    }

    fn open_result(&self, call_id: &str, name: &str) -> Result<Blob, OpenError> {
        let Some(result) = self.results.iter().find(|result| result.call_id == call_id) else {
            return Err(OpenError::NotAttached);
        };
        result
            .open(name)
            .map(|raw| self.hold(raw))
            .map_err(open_error)
    }

    fn read(&self, blob: &Blob, offset: u64, len: u32) -> Result<Vec<u8>, ReadError> {
        self.with_blob(blob, |raw| raw.read(offset, len))
            .map_err(|error| match error {
                blobs::ReadError::OutOfRange => ReadError::OutOfRange,
                blobs::ReadError::TooLarge => ReadError::TooLarge,
                blobs::ReadError::StorageFailed(detail) => ReadError::Storage(detail),
                blobs::ReadError::QuotaExceeded => ReadError::Quota,
                blobs::ReadError::Unavailable => ReadError::Unavailable,
            })
    }

    fn create(&self, content_type: &str) -> Result<Writer, CreateError> {
        match blobs::create(content_type) {
            Ok(raw) => Ok(Writer::new(self.insert(Entry::Writer(raw)))),
            Err(blobs::CreateError::InvalidContentType(detail)) => {
                Err(CreateError::InvalidContentType(detail))
            }
            Err(blobs::CreateError::BudgetExhausted) => Err(CreateError::BudgetExhausted),
            Err(blobs::CreateError::StorageFailed(detail)) => Err(CreateError::Storage(detail)),
            Err(blobs::CreateError::QuotaExceeded) => Err(CreateError::Quota),
        }
    }

    fn write(&self, writer: &Writer, offset: u64, bytes: &[u8]) -> Result<(), WriteError> {
        let table = self.table.borrow();
        let Some(Entry::Writer(raw)) = table.get(&writer.slot()) else {
            panic!("writer handle {} used after its step ended", writer.slot());
        };
        raw.write(offset, bytes).map_err(|error| match error {
            blobs::WriteError::TooLarge => WriteError::TooLarge,
            blobs::WriteError::BlobTooLarge => WriteError::BlobTooLarge,
            blobs::WriteError::BudgetExhausted => WriteError::BudgetExhausted,
            blobs::WriteError::TooFragmented => WriteError::TooFragmented,
            blobs::WriteError::StorageFailed(detail) => WriteError::Storage(detail),
            blobs::WriteError::QuotaExceeded => WriteError::Quota,
        })
    }

    /// A refused seal drops the SDK writer, which deletes its bytes: the
    /// kit's [`Writer`] is consumed either way.
    fn seal(&self, writer: Writer) -> Result<Blob, SealError> {
        let Some(Entry::Writer(raw)) = self.table.borrow_mut().remove(&writer.slot()) else {
            panic!("writer handle {} used after its step ended", writer.slot());
        };
        match raw.seal() {
            Ok(sealed) => Ok(self.hold(sealed)),
            Err(refused) => Err(match refused.error {
                blobs::SealError::Gap(gap) => SealError::Gap {
                    offset: gap.offset,
                    len: gap.len,
                },
                blobs::SealError::QuotaExceeded => SealError::Quota,
            }),
        }
    }

    fn release(&self) {
        self.table.borrow_mut().clear();
    }
}

impl Served for SdkBlobs<'_> {
    /// The call message holds its body blob from this call on, so the step's
    /// handle may be released before the activation flushes.
    fn call(
        &self,
        id: &PendingId,
        call: &Nested,
        call_id: &str,
        body: Option<(&str, &Blob)>,
    ) -> Result<(), String> {
        let Some(body) = body else {
            return call_async_for(id, call, call_id);
        };
        let Nested::Api(api) = call else {
            panic!("the kit gave a call to {} a body", call.tool());
        };
        self.with_blobs(&[body], |attachments| {
            provides::call_async_for_with_attachments(id, call.tool(), api, call_id, attachments)
        })
        .map_err(|refused| Error::from(refused).to_string())
    }

    fn complete(
        &self,
        id: &PendingId,
        value: &Value,
        attachments: &[(String, Blob)],
    ) -> Result<(), Error> {
        if attachments.is_empty() {
            return drop_not_pending(provides::complete(id, value));
        }
        let named: Vec<(&str, &Blob)> = attachments
            .iter()
            .map(|(name, blob)| (name.as_str(), blob))
            .collect();
        drop_not_pending(
            self.with_blobs(&named, |attachments| {
                provides::complete_with_attachments(id, value, attachments)
            })
            .map_err(Error::from),
        )
    }
}

fn open_error(error: blobs::OpenError) -> OpenError {
    match error {
        // The kit opens only a name the message's header lists.
        blobs::OpenError::NotPermitted => OpenError::NotAttached,
        blobs::OpenError::NotFound => OpenError::NotFound,
        blobs::OpenError::QuotaExceeded => OpenError::Quota,
    }
}

fn pending(id: &str) -> PendingId {
    PendingId::from(id.to_owned())
}

fn outcome_kind(kind: ErrorKind) -> OutcomeErrorKind {
    match kind {
        ErrorKind::Denied => OutcomeErrorKind::Denied,
        ErrorKind::InvalidArgs => OutcomeErrorKind::InvalidArgs,
        ErrorKind::Internal => OutcomeErrorKind::Internal,
    }
}
