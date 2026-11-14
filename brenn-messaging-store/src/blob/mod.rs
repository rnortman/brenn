//! The blob store: immutable byte objects that messages name by reference.
//!
//! A blob is one file, `<dir>/<id>`, written in place by positional writes
//! through a [`BlobWriter`] and sealed once every byte below its extent has
//! been written. A sealed blob never changes. It has no name, no listing and no
//! lifetime of its own: it lives while something holds it and its bytes are
//! deleted when nothing does.
//!
//! A blob has two kinds of holder. An in-memory holder is a [`BlobHold`], a
//! counted handle: host code has one from [`BlobWriter::seal`] or from
//! redeeming a reference, and a clone is one more hold. A durable holder is a
//! row of `messaging_messages` that names the blob, counted in the database in
//! the transaction that inserts or deletes the row. A blob's bytes are deleted
//! only when both counts are zero.
//!
//! Each index entry is in one of three states. *Volatile*: no durable insert
//! has ever named it; when its last hold drops its entry is removed, its file
//! unlinked and its charge returned, there and then. *Adopting*: a durable
//! insert named it and has not confirmed its commit. *Durable*: a committed row
//! named it at some point. An *Adopting* or *Durable* blob is unredeemable once
//! it has neither a hold nor a row naming it, and its bytes go at the next
//! [`BlobStore::reap`].
//!
//! The index is a `std::sync::Mutex` that is a leaf lock: nothing else is
//! acquired while it is held, so it may be taken under a ring's mutex or the
//! database mutex. File I/O never runs under it.
//!
//! # Bounds
//!
//! A blob's *extent* is the highest `offset + len` written to it, or the size
//! [`BlobWriter::reserve`] raised it to if that is higher. Each blob's
//! extent is capped at [`BlobLimits::max_blob_bytes`], and the store's charged
//! total — the sum of every live blob's extent, sealed or not — at
//! [`BlobLimits::budget_bytes`]. A write that would pass either is refused
//! before any byte is written; nothing is evicted to make room and nothing
//! waits. A one-byte write at a far offset is charged for everything below it.
//! A store opened over a budget lowered below what it holds also refuses
//! creates until deletions bring its total under.
//!
//! # Mutability
//!
//! Blobs are immutable once sealed, and nothing here forecloses a mutable kind:
//! the id is random rather than derived from content, a reference carries no
//! digest, and a write after seal would be the same positional write with the
//! seal check lifted. Three choices lean on immutability, and in-place mutation
//! would have to answer each: `size` and `content_type` are copied into every
//! reference (a mutable blob keeps a fixed extent, or `size` means "at
//! publish"); bytes are synced once, when a durable message first names them
//! (a sync rule for writes after that); and reads take no lock (a rule for a
//! read overlapping a concurrent write).

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io;
use std::ops::Range;
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::LazyLock;
use std::sync::{Arc, Mutex, MutexGuard};

use brenn_envelope::{
    AttachmentName, Attachments, BlobId, BlobRef, BodyError, MAX_ATTACHMENTS_PER_MESSAGE,
    check_content_type,
};
use rand::RngExt;

mod durable;
pub use durable::ReapReport;
pub(crate) use durable::adopt_in_tx;

#[cfg(test)]
mod tests;

/// The most disjoint written ranges a [`BlobWriter`] tracks before refusing a
/// write as [`BlobError::TooFragmented`].
pub const MAX_WRITTEN_RANGES: usize = 1024;

/// The store's bounds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlobLimits {
    /// Cap on one blob's extent.
    pub max_blob_bytes: u64,
    /// Cap on the store's charged total.
    pub budget_bytes: u64,
}

/// What the store holds: counts and bytes only, never ids.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlobUsage {
    pub sealed: usize,
    pub unsealed: usize,
    pub charged_bytes: u64,
}

#[derive(Debug)]
pub enum BlobError {
    InvalidContentType(BodyError),
    /// The write would take the blob's extent past `max`.
    TooLarge {
        max: u64,
    },
    /// The write would take the store's charged total past `budget`.
    BudgetExhausted {
        budget: u64,
    },
    /// The write would leave more than [`MAX_WRITTEN_RANGES`] disjoint ranges.
    TooFragmented,
    NotFound,
    OutOfRange {
        offset: u64,
        len: usize,
        size: u64,
    },
    Io(io::Error),
}

impl fmt::Display for BlobError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BlobError::InvalidContentType(e) => write!(f, "blob content type: {e}"),
            BlobError::TooLarge { max } => write!(f, "blob would exceed {max} bytes"),
            BlobError::BudgetExhausted { budget } => {
                write!(f, "blob store budget of {budget} bytes exhausted")
            }
            BlobError::TooFragmented => write!(
                f,
                "blob write would leave more than {MAX_WRITTEN_RANGES} disjoint written ranges"
            ),
            BlobError::NotFound => f.write_str("blob not found"),
            BlobError::OutOfRange { offset, len, size } => write!(
                f,
                "read of {len} bytes at offset {offset} passes the blob's size of {size}"
            ),
            BlobError::Io(e) => write!(f, "blob I/O: {e}"),
        }
    }
}

impl std::error::Error for BlobError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            BlobError::InvalidContentType(e) => Some(e),
            BlobError::Io(e) => Some(e),
            _ => None,
        }
    }
}

/// The answer to [`BlobStore::retain`].
#[derive(Debug)]
pub enum Retain {
    Held(BlobHold),
    NotFound,
    /// The blob is sealed and has no in-memory hold, so whether any holder
    /// exists is a fact only the database has: ask
    /// [`BlobStore::retain_settled`] with the database lock held.
    Unsettled,
}

/// A store of blobs under one directory.
pub struct BlobStore {
    dir: PathBuf,
    limits: BlobLimits,
    /// The generation of the database this store reconciled against at open;
    /// `None` for a store with no database, which can adopt nothing.
    generation: Option<uuid::Uuid>,
    /// An exclusive `flock` on `dir`, held for the store's lifetime so no
    /// second store opens, and sweeps, the same directory; `None` for a store
    /// built without [`BlobStore::open`].
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "held for its lock, released when the store drops")
    )]
    dir_lock: Option<std::fs::File>,
    index: Mutex<Index>,
}

#[derive(Default)]
struct Index {
    entries: HashMap<BlobId, Entry>,
    /// The sum of every entry's `extent`.
    charged: u64,
    /// *Adopting* entries whose last hold dropped — the inserts that named
    /// them rolled back — for the next reap to settle against the database.
    /// Every id here is indexed.
    unheld_adopting: HashSet<BlobId>,
}

struct Entry {
    /// For a sealed blob, its size; for an unsealed one, its writer's extent.
    extent: u64,
    content_type: String,
    /// `None` while the blob is unsealed; then the number of live holds.
    holds: Option<u64>,
    life: Life,
    /// Whether the file and its directory have been synced since seal.
    synced: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Life {
    /// No durable insert has named the blob.
    Volatile,
    /// A durable insert named it and has not confirmed its commit.
    Adopting,
    /// A committed durable row named it.
    Durable,
}

impl fmt::Debug for BlobStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BlobStore")
            .field("dir", &self.dir)
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

impl BlobStore {
    /// Release the directory lock while the store stays in memory, as the
    /// death of its process would: for a test that reopens the directory over
    /// a store it leaks to stand for a crash.
    #[cfg(test)]
    pub(crate) fn release_dir_lock(&self) {
        if let Some(lock) = &self.dir_lock {
            lock.unlock().expect("unlock blob directory");
        }
    }

    /// A store over `dir`, which must exist, with an empty index, no database
    /// and no directory lock: it reads and writes no owner record, and a blob
    /// it holds can never be adopted.
    #[cfg(any(test, feature = "testutils"))]
    pub fn for_tests(dir: &Path, limits: BlobLimits) -> Arc<Self> {
        Arc::new(BlobStore {
            dir: dir.to_owned(),
            limits,
            generation: None,
            dir_lock: None,
            index: Mutex::new(Index::default()),
        })
    }

    pub fn limits(&self) -> BlobLimits {
        self.limits
    }

    /// Start a new blob. Its id is exposed by no accessor until it is sealed.
    ///
    /// Refused `BudgetExhausted` while the charged total is above the budget,
    /// which only a store opened over a lowered budget can be. Writes never
    /// take the total above the budget, so a create this check admits is not
    /// overtaken by one.
    pub fn create(self: &Arc<Self>, content_type: &str) -> Result<BlobWriter, BlobError> {
        check_content_type(content_type).map_err(BlobError::InvalidContentType)?;
        {
            let index = self.lock();
            if index.charged > self.limits.budget_bytes {
                // `debug`: the refusal is returned, and the caller reports it
                // at its own rate.
                tracing::debug!(
                    charged = index.charged,
                    budget = self.limits.budget_bytes,
                    "blob store above its budget; create refused"
                );
                return Err(BlobError::BudgetExhausted {
                    budget: self.limits.budget_bytes,
                });
            }
        }
        let id = mint_id();
        let path = self.path_of(&id);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&path)
            .map_err(BlobError::Io)?;
        let previous = self.lock().entries.insert(
            id.clone(),
            Entry {
                extent: 0,
                content_type: content_type.to_owned(),
                holds: None,
                life: Life::Volatile,
                synced: false,
            },
        );
        assert!(previous.is_none(), "blob id {id} minted twice");
        Ok(BlobWriter {
            store: Arc::clone(self),
            id,
            file,
            extent: 0,
            written: Vec::new(),
            sealed: false,
        })
    }

    /// Take one more hold on a sealed blob that an in-memory hold already
    /// holds. Reads the index only.
    ///
    /// `NotFound` for an id the store has no entry for and for an unsealed
    /// blob, so only its producer can name a blob before it is sealed.
    /// `Unsettled` for a sealed blob with no in-memory hold, which only an
    /// *Adopting* or *Durable* entry can be.
    pub fn retain(self: &Arc<Self>, id: &BlobId) -> Retain {
        let mut index = self.lock();
        let Some(entry) = index.entries.get_mut(id) else {
            return Retain::NotFound;
        };
        match entry.holds {
            None => return Retain::NotFound,
            Some(0) => {
                assert_ne!(
                    entry.life,
                    Life::Volatile,
                    "volatile blob {id} indexed with no holder"
                );
                return Retain::Unsettled;
            }
            Some(_) => {}
        }
        Retain::Held(self.take_hold(id, entry))
    }

    /// Count one more hold on the sealed blob `id`, whose `entry` the caller
    /// looked up under the index lock and checked its own preconditions on,
    /// and return it.
    fn take_hold(self: &Arc<Self>, id: &BlobId, entry: &mut Entry) -> BlobHold {
        let holds = entry
            .holds
            .as_mut()
            .unwrap_or_else(|| panic!("taking a hold on blob {id}, which is not sealed"));
        *holds += 1;
        BlobHold {
            store: Arc::clone(self),
            reference: BlobRef {
                id: id.clone(),
                size: entry.extent,
                content_type: entry.content_type.clone(),
            },
        }
    }

    pub fn usage(&self) -> BlobUsage {
        let index = self.lock();
        let sealed = index.entries.values().filter(|e| e.holds.is_some()).count();
        BlobUsage {
            sealed,
            unsealed: index.entries.len() - sealed,
            charged_bytes: index.charged,
        }
    }

    fn lock(&self) -> MutexGuard<'_, Index> {
        self.index.lock().expect("blob index mutex poisoned")
    }

    fn path_of(&self, id: &BlobId) -> PathBuf {
        self.dir.join(id.as_str())
    }

    /// Remove `id`'s entry, return its charge, and unlink its file.
    fn delete(&self, id: &BlobId) {
        remove_entry(&mut self.lock(), id);
        unlink(&self.path_of(id));
    }

    /// Move `id`'s charge from `from` to `to`, refusing if either bound would
    /// be passed. Changes nothing on refusal.
    fn recharge(&self, id: &BlobId, from: u64, to: u64) -> Result<(), BlobError> {
        let mut index = self.lock();
        if to > from {
            let charged = index.charged - from + to;
            if charged > self.limits.budget_bytes {
                // `debug`: the refusal is returned, and the caller reports it
                // at its own rate.
                tracing::debug!(
                    charged = index.charged,
                    requested = to - from,
                    budget = self.limits.budget_bytes,
                    "blob store budget exhausted"
                );
                return Err(BlobError::BudgetExhausted {
                    budget: self.limits.budget_bytes,
                });
            }
        }
        index.charged = index.charged - from + to;
        let entry = index
            .entries
            .get_mut(id)
            .unwrap_or_else(|| panic!("charging blob {id}, which has no index entry"));
        assert_eq!(entry.extent, from, "blob {id} charge out of step");
        entry.extent = to;
        Ok(())
    }
}

/// Remove `id`'s entry and return its charge. The caller unlinks the file
/// once the index lock is released.
fn remove_entry(index: &mut Index, id: &BlobId) {
    let entry = index
        .entries
        .remove(id)
        .unwrap_or_else(|| panic!("deleting blob {id}, which has no index entry"));
    index.charged = index
        .charged
        .checked_sub(entry.extent)
        .expect("blob store charge underflow");
}

/// A fresh id: 16 random bytes as 32 lowercase hex characters.
fn mint_id() -> BlobId {
    let mut bytes = [0u8; 16];
    rand::rng().fill(&mut bytes[..]);
    BlobId::new(format!("{:032x}", u128::from_be_bytes(bytes)))
}

/// Whether `name` has the form [`mint_id`] gives an id, and so names a blob
/// file in the store's directory.
fn is_minted_id(name: &str) -> bool {
    name.len() == 32 && name.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

fn unlink(path: &Path) {
    if let Err(e) = std::fs::remove_file(path) {
        panic!("unlinking blob file {}: {e}", path.display());
    }
}

/// A blob being written. Dropping it unsealed deletes its file and returns
/// its charge.
pub struct BlobWriter {
    store: Arc<BlobStore>,
    id: BlobId,
    file: File,
    extent: u64,
    /// Written ranges, sorted, disjoint and non-adjacent. A write that fails
    /// with an I/O error can split one range in two, leaving one more than
    /// [`MAX_WRITTEN_RANGES`].
    written: Vec<Range<u64>>,
    sealed: bool,
}

impl fmt::Debug for BlobWriter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BlobWriter")
            .field("extent", &self.extent)
            .field("written", &self.written)
            .finish_non_exhaustive()
    }
}

/// [`BlobWriter::seal`] refused: `gap` is the first range below the extent
/// that no write covered. The writer is returned to be filled and sealed again.
#[derive(Debug)]
pub struct SealRefused {
    pub writer: BlobWriter,
    pub gap: Range<u64>,
}

impl BlobWriter {
    /// The highest `offset + len` written so far, or the reserved size if
    /// that is higher.
    pub fn extent(&self) -> u64 {
        self.extent
    }

    /// Write `bytes` at `offset`, over anything already written there.
    ///
    /// Refused before any byte is written if it would pass a bound or leave
    /// too many disjoint ranges; a refused write changes nothing. An empty
    /// write changes nothing.
    ///
    /// A write that fails with [`BlobError::Io`] may have landed some of its
    /// bytes. The file is cut back to the extent it had before the write and
    /// the failed range counts as unwritten, so the extent and the charge are
    /// unchanged and [`BlobWriter::seal`] refuses until that range is written
    /// again.
    pub fn write(&mut self, offset: u64, bytes: &[u8]) -> Result<(), BlobError> {
        if bytes.is_empty() {
            return Ok(());
        }
        let max = self.store.limits.max_blob_bytes;
        let end = offset
            .checked_add(bytes.len() as u64)
            .ok_or(BlobError::TooLarge { max })?;
        if end > max {
            return Err(BlobError::TooLarge { max });
        }
        let merged = merge_range(&self.written, offset..end);
        if merged.len() > MAX_WRITTEN_RANGES {
            return Err(BlobError::TooFragmented);
        }
        let new_extent = self.extent.max(end);
        self.store.recharge(&self.id, self.extent, new_extent)?;
        if let Err(e) = self.file.write_all_at(bytes, offset) {
            self.undo_failed_write(offset..end, new_extent, &e);
            return Err(BlobError::Io(e));
        }
        self.extent = new_extent;
        self.written = merged;
        Ok(())
    }

    /// Raise the extent to `size` and charge for it now, writing nothing, so
    /// a producer that knows the blob's size is refused for space before its
    /// first byte rather than part-way through. A `size` at or below the
    /// extent changes nothing.
    ///
    /// Refused `TooLarge` or `BudgetExhausted` as [`BlobWriter::write`] would
    /// refuse a write ending at `size`; a refusal changes nothing.
    /// [`BlobWriter::seal`] still refuses until every byte below the extent
    /// has been written.
    pub fn reserve(&mut self, size: u64) -> Result<(), BlobError> {
        if size <= self.extent {
            return Ok(());
        }
        let max = self.store.limits.max_blob_bytes;
        if size > max {
            return Err(BlobError::TooLarge { max });
        }
        self.store.recharge(&self.id, self.extent, size)?;
        self.extent = size;
        Ok(())
    }

    /// After a write of `range` failed with `cause`, with `new_extent`
    /// charged for it: cut the file back to the extent, count `range` as
    /// unwritten, and return the charge.
    fn undo_failed_write(&mut self, range: Range<u64>, new_extent: u64, cause: &io::Error) {
        if new_extent > self.extent {
            self.file.set_len(self.extent).unwrap_or_else(|e| {
                panic!(
                    "cutting blob {} back to {} bytes after a failed write ({cause}): {e}",
                    self.id, self.extent
                )
            });
        }
        self.written = remove_range(&self.written, range);
        self.store
            .recharge(&self.id, new_extent, self.extent)
            .expect("returning a charge cannot pass a bound");
    }

    /// Seal the blob if every byte below its extent has been written, making
    /// it immutable and nameable and returning the first hold on it.
    pub fn seal(mut self) -> Result<BlobHold, SealRefused> {
        if let Some(gap) = first_gap(&self.written, self.extent) {
            return Err(SealRefused { writer: self, gap });
        }
        let hold = {
            let mut index = self.store.lock();
            let entry = index
                .entries
                .get_mut(&self.id)
                .unwrap_or_else(|| panic!("sealing blob {}, which has no index entry", self.id));
            assert_eq!(
                entry.extent, self.extent,
                "blob {} charge out of step",
                self.id
            );
            assert!(entry.holds.is_none(), "blob {} sealed twice", self.id);
            entry.holds = Some(0);
            self.store.take_hold(&self.id, entry)
        };
        self.sealed = true;
        Ok(hold)
    }
}

impl Drop for BlobWriter {
    fn drop(&mut self) {
        if !self.sealed {
            self.store.delete(&self.id);
        }
    }
}

/// `written` with `new` added, merging overlapping and adjacent ranges.
fn merge_range(written: &[Range<u64>], new: Range<u64>) -> Vec<Range<u64>> {
    let mut out = Vec::with_capacity(written.len() + 1);
    let mut pending = new;
    let mut placed = false;
    for r in written {
        if placed || r.end < pending.start {
            out.push(r.clone());
        } else if pending.end < r.start {
            out.push(pending.clone());
            out.push(r.clone());
            placed = true;
        } else {
            pending = pending.start.min(r.start)..pending.end.max(r.end);
        }
    }
    if !placed {
        out.push(pending);
    }
    out
}

/// `written` with `gone` taken out of it.
fn remove_range(written: &[Range<u64>], gone: Range<u64>) -> Vec<Range<u64>> {
    let mut out = Vec::with_capacity(written.len() + 1);
    for r in written {
        if r.end <= gone.start || gone.end <= r.start {
            out.push(r.clone());
            continue;
        }
        if r.start < gone.start {
            out.push(r.start..gone.start);
        }
        if gone.end < r.end {
            out.push(gone.end..r.end);
        }
    }
    out
}

/// The first range below `extent` that `written` does not cover.
fn first_gap(written: &[Range<u64>], extent: u64) -> Option<Range<u64>> {
    let mut covered_to = 0;
    for r in written {
        if r.start > covered_to {
            return Some(covered_to..r.start);
        }
        covered_to = r.end;
    }
    (covered_to < extent).then_some(covered_to..extent)
}

/// A counted hold on a sealed blob. While any hold exists the blob's bytes
/// stay; a clone is one more hold.
pub struct BlobHold {
    store: Arc<BlobStore>,
    reference: BlobRef,
}

impl fmt::Debug for BlobHold {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BlobHold")
            .field("reference", &self.reference)
            .finish_non_exhaustive()
    }
}

impl BlobHold {
    /// The blob's reference, as the store recorded it at seal.
    pub fn reference(&self) -> BlobRef {
        self.reference.clone()
    }

    pub fn id(&self) -> &BlobId {
        &self.reference.id
    }

    /// Exactly `len` bytes from `offset`.
    ///
    /// # Panics
    ///
    /// If the file is missing or shorter than the blob's size: nothing writes
    /// a sealed blob, so its file was removed or cut behind the store.
    pub fn read(&self, offset: u64, len: usize) -> Result<Vec<u8>, BlobError> {
        self.check_range(offset, len)?;
        let mut buf = vec![0u8; len];
        self.read_into(offset, &mut buf)?;
        Ok(buf)
    }

    /// Exactly `buf.len()` bytes from `offset`, into `buf`: a reader of many
    /// pieces reuses one buffer.
    ///
    /// # Panics
    ///
    /// As [`BlobHold::read`].
    pub fn read_into(&self, offset: u64, buf: &mut [u8]) -> Result<(), BlobError> {
        let len = buf.len();
        self.check_range(offset, len)?;
        let size = self.reference.size;
        let path = self.store.path_of(&self.reference.id);
        let file = match File::open(&path) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                panic!("held blob's file {} is missing", path.display())
            }
            Err(e) => return Err(BlobError::Io(e)),
        };
        match file.read_exact_at(buf, offset) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => panic!(
                "held blob's file {} ends inside the read of {len} bytes at offset {offset}; \
                 the blob's size is {size}",
                path.display()
            ),
            Err(e) => Err(BlobError::Io(e)),
        }
    }

    fn check_range(&self, offset: u64, len: usize) -> Result<(), BlobError> {
        let size = self.reference.size;
        let in_range = offset
            .checked_add(len as u64)
            .is_some_and(|end| end <= size);
        if in_range {
            Ok(())
        } else {
            Err(BlobError::OutOfRange { offset, len, size })
        }
    }
}

impl Clone for BlobHold {
    fn clone(&self) -> Self {
        let id = &self.reference.id;
        let mut index = self.store.lock();
        let entry = index
            .entries
            .get_mut(id)
            .unwrap_or_else(|| panic!("cloning a hold on blob {id}, which is not indexed"));
        self.store.take_hold(id, entry)
    }
}

impl Drop for BlobHold {
    fn drop(&mut self) {
        let id = &self.reference.id;
        {
            let mut index = self.store.lock();
            let holds = index
                .entries
                .get_mut(id)
                .and_then(|e| e.holds.as_mut())
                .unwrap_or_else(|| {
                    panic!("dropping a hold on blob {id}, which is not indexed as sealed")
                });
            *holds -= 1;
            if *holds > 0 {
                return;
            }
            match index.entries.get(id).expect("entry looked up above").life {
                Life::Volatile => remove_entry(&mut index, id),
                Life::Adopting => {
                    index.unheld_adopting.insert(id.clone());
                    return;
                }
                Life::Durable => return,
            }
        }
        unlink(&self.store.path_of(id));
    }
}

/// Why [`HeldAttachments::new`] refused.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AttachmentError {
    /// More than [`MAX_ATTACHMENTS_PER_MESSAGE`] attachments.
    TooMany {
        count: usize,
    },
    DuplicateName(AttachmentName),
}

impl fmt::Display for AttachmentError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AttachmentError::TooMany { count } => write!(
                f,
                "{count} attachments; a message carries at most {MAX_ATTACHMENTS_PER_MESSAGE}"
            ),
            AttachmentError::DuplicateName(name) => {
                write!(f, "attachment name {name:?} given twice")
            }
        }
    }
}

impl std::error::Error for AttachmentError {}

/// A message's attachments with a hold on every blob they name: names that
/// passed [`AttachmentName::new`], a count within the bound, and references
/// taken from the store's own record of each blob.
#[derive(Clone, Debug, Default)]
pub struct HeldAttachments {
    refs: Attachments,
    /// One hold per distinct blob, however many names give it.
    holds: Vec<BlobHold>,
}

impl HeldAttachments {
    pub fn new(
        entries: impl IntoIterator<Item = (AttachmentName, BlobHold)>,
    ) -> Result<Self, AttachmentError> {
        let entries: Vec<(AttachmentName, BlobHold)> = entries.into_iter().collect();
        if entries.len() > MAX_ATTACHMENTS_PER_MESSAGE {
            return Err(AttachmentError::TooMany {
                count: entries.len(),
            });
        }
        let mut map = std::collections::BTreeMap::new();
        let mut holds: Vec<BlobHold> = Vec::new();
        for (name, hold) in entries {
            if map.contains_key(&name) {
                return Err(AttachmentError::DuplicateName(name));
            }
            map.insert(name, hold.reference());
            if !holds.iter().any(|h| h.id() == hold.id()) {
                holds.push(hold);
            }
        }
        Ok(HeldAttachments {
            refs: Attachments::from_map(map),
            holds,
        })
    }

    pub fn none() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.refs.is_empty()
    }

    pub fn refs(&self) -> &Attachments {
        &self.refs
    }

    /// One hold per distinct blob named.
    pub fn holds(&self) -> &[BlobHold] {
        &self.holds
    }

    /// The references and the holds, for a store that keeps the holds beside
    /// a message naming the references.
    pub fn into_parts(self) -> (Attachments, BlobHolds) {
        let holds = if self.holds.is_empty() {
            no_holds()
        } else {
            BlobHolds::from(self.holds)
        };
        (self.refs, holds)
    }
}

/// Holds shared by every copy of one stored message: cloning shares them, and
/// they are released when the last copy drops.
pub type BlobHolds = Arc<[BlobHold]>;

static NO_HOLDS: LazyLock<BlobHolds> = LazyLock::new(|| BlobHolds::from([]));

/// No holds, for a message that names no blob: one shared empty set, so a
/// message without attachments allocates nothing for it.
pub fn no_holds() -> BlobHolds {
    Arc::clone(&NO_HOLDS)
}

/// Panic unless every blob `refs` names is among `holds`: a stored message
/// never names a blob it does not hold. `what` names the message in the
/// panic.
pub fn assert_held(refs: &Attachments, holds: &[BlobHold], what: fmt::Arguments<'_>) {
    for (name, reference) in refs.iter() {
        assert!(
            holds.iter().any(|h| h.id() == &reference.id),
            "{what}: attachment {name} names blob {}, which the message does not hold",
            reference.id
        );
    }
}
