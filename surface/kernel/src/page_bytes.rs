//! Blob bytes a page made, and the ledger that counts them against
//! [`PAGE_MADE_BLOB_BYTES`].
//!
//! Page-made bytes are the only copy: unlike fetched bytes, nothing may drop
//! them to make room. Every buffer debits the page's [`MadeLedger`] for its
//! length when it grows and credits it when it is freed, so the ledger's total
//! is exactly the page-made bytes alive in memory, counted once however many
//! holders share a sealed buffer.
//!
//! Built on both targets so the rules are tested natively.

#[cfg(test)]
mod tests;

use std::fmt;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use brenn_envelope::WrittenRanges;

pub use brenn_surface_schema::PAGE_MADE_BLOB_BYTES;

/// The most disjoint written ranges one writer tracks.
pub const SURFACE_MAX_WRITTEN_RANGES: usize = 1024;

/// The page's count of page-made bytes alive. A clone counts into the same
/// total.
#[derive(Clone, Default)]
pub struct MadeLedger(Arc<AtomicU64>);

impl fmt::Debug for MadeLedger {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("MadeLedger").field(&self.held()).finish()
    }
}

impl MadeLedger {
    pub fn new() -> Self {
        Self::default()
    }

    /// Page-made bytes alive now.
    pub fn held(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }

    /// Whether the page holds less than [`PAGE_MADE_BLOB_BYTES`], so a new
    /// writer may be made.
    pub fn has_room(&self) -> bool {
        self.held() < PAGE_MADE_BLOB_BYTES
    }

    /// Whether `more` bytes fit under [`PAGE_MADE_BLOB_BYTES`].
    fn admits(&self, more: u64) -> bool {
        self.held()
            .checked_add(more)
            .is_some_and(|total| total <= PAGE_MADE_BLOB_BYTES)
    }

    fn debit(&self, bytes: u64) {
        let before = self.0.fetch_add(bytes, Ordering::Relaxed);
        assert!(
            before.checked_add(bytes).is_some(),
            "surface kernel: page-made byte ledger overflowed"
        );
    }

    fn credit(&self, bytes: u64) {
        let before = self.0.fetch_sub(bytes, Ordering::Relaxed);
        assert!(
            before >= bytes,
            "surface kernel: page-made byte ledger credited {bytes} with {before} held"
        );
    }
}

/// One buffer's share of the ledger, returned when the buffer is freed.
struct Charge {
    ledger: MadeLedger,
    bytes: u64,
}

impl Charge {
    fn new(ledger: &MadeLedger) -> Self {
        Self {
            ledger: ledger.clone(),
            bytes: 0,
        }
    }

    fn grow_to(&mut self, bytes: u64) {
        assert!(bytes >= self.bytes, "a page-made buffer never shrinks");
        self.ledger.debit(bytes - self.bytes);
        self.bytes = bytes;
    }
}

impl Drop for Charge {
    fn drop(&mut self) {
        self.ledger.credit(self.bytes);
    }
}

/// Why a write changed nothing, past the activation's own bounds, which the
/// scope checks first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BufferRefusal {
    /// The write would end past the page's largest blob.
    BlobTooLarge,
    /// Growing the buffer would take the page past [`PAGE_MADE_BLOB_BYTES`].
    BudgetExhausted,
    /// The write would leave more than [`SURFACE_MAX_WRITTEN_RANGES`]
    /// disjoint written ranges.
    TooFragmented,
}

/// An unsealed blob: a buffer as long as the highest byte written, and the
/// ranges written so far.
pub struct WriteBuffer {
    content_type: String,
    bytes: Vec<u8>,
    written: WrittenRanges,
    charge: Charge,
}

impl fmt::Debug for WriteBuffer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WriteBuffer")
            .field("content_type", &self.content_type)
            .field("len", &self.bytes.len())
            .field("written", &self.written)
            .finish()
    }
}

impl WriteBuffer {
    /// An empty buffer counted into `ledger`. The content type is checked by
    /// the caller.
    pub fn new(ledger: &MadeLedger, content_type: String) -> Self {
        Self {
            content_type,
            bytes: Vec::new(),
            written: WrittenRanges::new(),
            charge: Charge::new(ledger),
        }
    }

    /// The buffer's length: the highest byte written.
    pub fn len(&self) -> u64 {
        self.bytes.len() as u64
    }

    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    /// Write `data` at `offset`, over anything written there. A refusal
    /// changes nothing. An empty write changes nothing and is never refused,
    /// as the backend store's.
    pub fn write(
        &mut self,
        offset: u64,
        data: &[u8],
        largest_blob: u64,
    ) -> Result<(), BufferRefusal> {
        if data.is_empty() {
            return Ok(());
        }
        let end = offset
            .checked_add(data.len() as u64)
            .ok_or(BufferRefusal::BlobTooLarge)?;
        if end > largest_blob {
            return Err(BufferRefusal::BlobTooLarge);
        }
        let new_len = self.len().max(end);
        if !self.charge.ledger.admits(new_len - self.len()) {
            return Err(BufferRefusal::BudgetExhausted);
        }
        let merged = self.written.with(offset..end);
        if merged.len() > SURFACE_MAX_WRITTEN_RANGES {
            return Err(BufferRefusal::TooFragmented);
        }
        let new_len_usize =
            usize::try_from(new_len).expect("a page-made blob is within the address space");
        if new_len_usize > self.bytes.len() {
            self.charge.grow_to(new_len);
            self.bytes.resize(new_len_usize, 0);
        }
        let start = usize::try_from(offset).expect("offset is below the buffer's length");
        self.bytes[start..start + data.len()].copy_from_slice(data);
        self.written = merged;
        Ok(())
    }

    /// The first range below the buffer's length no write covered.
    pub fn first_gap(&self) -> Option<Range<u64>> {
        self.written.first_gap(self.len())
    }

    /// The sealed blob, its charge passed on, or the buffer back with the first
    /// gap.
    pub fn seal(self) -> Result<PageBytes, (Self, Range<u64>)> {
        if let Some(gap) = self.first_gap() {
            return Err((self, gap));
        }
        Ok(PageBytes(Arc::new(Sealed {
            content_type: self.content_type,
            bytes: self.bytes,
            _charge: self.charge,
        })))
    }
}

struct Sealed {
    content_type: String,
    bytes: Vec<u8>,
    _charge: Charge,
}

/// A sealed page-made blob. Immutable and shared: a clone is one more holder
/// of the same bytes, and the bytes are freed, and credited to the ledger,
/// when the last holder lets go.
#[derive(Clone)]
pub struct PageBytes(Arc<Sealed>);

impl fmt::Debug for PageBytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PageBytes")
            .field("content_type", &self.0.content_type)
            .field("size", &self.size())
            .finish()
    }
}

impl PageBytes {
    pub fn size(&self) -> u64 {
        self.0.bytes.len() as u64
    }

    pub fn content_type(&self) -> &str {
        &self.0.content_type
    }

    pub fn bytes(&self) -> &[u8] {
        &self.0.bytes
    }

    /// Whether `self` and `other` are the same blob, not merely equal bytes.
    pub fn same_blob(&self, other: &PageBytes) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}
