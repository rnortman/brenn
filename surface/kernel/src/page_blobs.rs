//! The page's blob table: what each attachment of a page-local message names.
//!
//! A page-local (`local:`) message never leaves the page, so the blobs it names
//! are the page's to hold. An attachment there is either page bytes the page
//! made, under an id the page mints, or a backend reference, under the
//! backend's own id with the place the page was delivered it. The envelope
//! carries the [`BlobRef`]; this table carries what the id stands for.
//!
//! An entry lives while a retained or parked message of a page-local store
//! names its id. [`PageBlobs::sweep`] removes every other entry, and a page
//! bytes buffer is freed when its last holder lets go. Nothing here is uploaded
//! or persisted.

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet};

use brenn_envelope::{BlobId, BlobRef, is_local_channel};

use crate::blob_scope::Blob;
use crate::registry::SurfaceStores;

/// The prefix of a page-minted blob id. The backend mints 32 hex digits, so the
/// two never collide.
const PAGE_ID_PREFIX: &str = "page-";

/// What each attachment of the page's page-local messages names.
#[derive(Debug, Default)]
pub struct PageBlobs {
    entries: BTreeMap<BlobId, Blob>,
    /// The last page id minted; never reused within the page.
    minted: u64,
}

impl PageBlobs {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Record `blob` for one attachment of a page-local message and answer the
    /// reference its envelope carries.
    ///
    /// Page bytes are shared, not copied, under a fresh page id. A backend
    /// reference is recorded under the backend's own id, replacing an older
    /// origin for that id.
    pub fn commit(&mut self, blob: &Blob) -> BlobRef {
        match blob {
            Blob::Bytes(bytes) => {
                self.minted = self
                    .minted
                    .checked_add(1)
                    .expect("surface kernel: the page minted 2^64 blob ids");
                let id = BlobId::new(format!("{PAGE_ID_PREFIX}{}", self.minted));
                let reference = BlobRef {
                    id: id.clone(),
                    size: bytes.size(),
                    content_type: bytes.content_type().to_owned(),
                };
                self.entries.insert(id, blob.clone());
                reference
            }
            Blob::Remote(remote) => {
                // TODO(correctness-page-blob-table-last-origin-wins): every
                // message naming this id resolves through the newest origin,
                // whose window may be shorter than the one it was committed
                // with.
                self.entries.insert(remote.blob.id.clone(), blob.clone());
                remote.blob.clone()
            }
        }
    }

    /// What `id` names, or `None` once no page-local message names it.
    pub fn get(&self, id: &BlobId) -> Option<&Blob> {
        self.entries.get(id)
    }

    /// Remove every entry no retained or parked message of a page-local store
    /// names.
    pub fn sweep(&mut self, stores: &SurfaceStores) {
        if self.entries.is_empty() {
            return;
        }
        let mut named: BTreeSet<&BlobId> = BTreeSet::new();
        for channel in stores
            .channels()
            .filter(|channel| is_local_channel(channel))
        {
            let store = stores
                .get(channel)
                .expect("a channel the collection lists is held");
            let envelopes = store
                .retained()
                .map(|(envelope, _)| envelope)
                .chain(store.parked().map(|(envelope, _)| envelope));
            for envelope in envelopes {
                named.extend(envelope.attachments.iter().map(|(_, blob)| &blob.id));
            }
        }
        self.entries.retain(|id, _| named.contains(id));
    }
}
