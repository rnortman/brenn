//! The page's one blob plane and what each of its live handles holds.
//!
//! A session's blob table is one table on the server: the handles the page
//! opens to fetch attachments and the ones it creates or opens to upload a
//! flush count against the same `max_handles` and `max_bytes_held`. So the page
//! keeps one [`BlobPlane`], tagged by which of its two users asked, and this
//! owner keeps the size behind every live handle — the declared size of a
//! create, the delivered reference's size of an open — so "how much of the
//! session's table is in use" has one answer both users ask before starting
//! anything.
//!
//! An answer is settled here once and its events are split by tag: a fetch's go
//! to the fetcher, an upload's to the stage. A `Chunk` carries no tag and is the
//! fetcher's, since only the fetcher reads.

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;

use brenn_attach_client::blobs::{
    BlobEvent, BlobPlane, ChunkRange, CreateError, OpenError, ReadError, ReadId,
};
use brenn_attach_client::conn::AttachmentFacts;
use brenn_attach_proto::{AttachmentName, BlobFacts, BlobHandle, ClientFrame, ServerFrame};
use uuid::Uuid;

use crate::fetch::FetchId;

/// Names one upload handle of a staged flush. Never reused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct UploadId(pub u64);

/// Which of the page's two users a plane operation belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlaneTag {
    Fetch(FetchId),
    Upload(UploadId),
}

/// One blob answer, settled and split between the two users.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct PlaneAnswer {
    /// Frames the plane composed in answer, in order.
    pub frames: Vec<ClientFrame>,
    pub fetch: Vec<BlobEvent<FetchId>>,
    pub upload: Vec<BlobEvent<UploadId>>,
}

/// The page's blob plane, its live door, and the size behind each live handle.
#[derive(Default)]
pub struct PagePlane {
    plane: BlobPlane<PlaneTag>,
    /// The live attachment's blob door; `None` while detached or doorless.
    door: Option<BlobFacts>,
    /// Every handle minted on this attachment and not yet given up — closed,
    /// refused, or forgotten at a detach — with what it counts against
    /// `max_bytes_held`.
    sizes: BTreeMap<BlobHandle, u64>,
    /// Whether the stage has a flush waiting for room alone; while it has, no
    /// new fetch starts.
    fetches_held: bool,
}

impl PagePlane {
    pub fn new() -> Self {
        Self::default()
    }

    /// A new attachment is live; its blob door is `facts.blobs`.
    pub fn on_attached(&mut self, facts: &AttachmentFacts) {
        self.plane.on_attached(facts);
        self.door = facts.blobs;
    }

    /// The attachment ended: every handle is gone, answered or not.
    ///
    /// The plane's `Detached` events are not handed on. Each user resets what
    /// it had in flight on the detach itself, because the plane reports
    /// nothing for a handle it already answered `Sealed` or `Opened`.
    pub fn on_detached(&mut self) {
        self.plane.on_detached();
        self.door = None;
        self.sizes.clear();
        self.fetches_held = false;
    }

    /// Hold new fetches back, or let them start again.
    pub fn hold_fetches(&mut self, held: bool) {
        self.fetches_held = held;
    }

    /// Whether new fetches are held back for an upload waiting for room.
    pub fn fetches_held(&self) -> bool {
        self.fetches_held
    }

    /// The live attachment's blob door.
    pub fn door(&self) -> Option<BlobFacts> {
        self.door
    }

    /// Live handles, of both users.
    pub fn handles_in_use(&self) -> u64 {
        self.sizes.len() as u64
    }

    /// What the live handles of both users count against `max_bytes_held`.
    pub fn bytes_in_use(&self) -> u64 {
        self.sizes
            .values()
            .fold(0, |sum, size| sum.saturating_add(*size))
    }

    /// Whether `handles` more handles holding `bytes` more bytes fit the live
    /// door beside what is in use. `false` with no door.
    pub fn has_room(&self, handles: u64, bytes: u64) -> bool {
        self.door.is_some_and(|door| {
            self.handles_in_use()
                .checked_add(handles)
                .is_some_and(|total| total <= u64::from(door.max_handles))
                && door.admits_bytes_held(self.bytes_in_use(), bytes)
        })
    }

    /// Open attachment `name` of `message_id` on `channel`, whose delivered
    /// reference says it is `size` bytes.
    ///
    /// # Panics
    ///
    /// With no blob door, as [`BlobPlane::open`].
    pub fn open(
        &mut self,
        tag: PlaneTag,
        channel: &str,
        message_id: Uuid,
        name: AttachmentName,
        size: u64,
    ) -> Result<(BlobHandle, ClientFrame), OpenError> {
        let (handle, frame) = self.plane.open(tag, channel, message_id, name)?;
        self.sizes.insert(handle, size);
        Ok((handle, frame))
    }

    /// Start an upload of `size` bytes of `content_type`.
    ///
    /// # Panics
    ///
    /// With no blob door, or one without `upload`, as [`BlobPlane::create`].
    pub fn create(
        &mut self,
        id: UploadId,
        content_type: &str,
        size: u64,
    ) -> Result<(BlobHandle, Vec<ClientFrame>), CreateError> {
        let (handle, frames) = self
            .plane
            .create(PlaneTag::Upload(id), content_type, size)?;
        self.sizes.insert(handle, size);
        Ok((handle, frames))
    }

    /// See [`BlobPlane::next_chunk`].
    pub fn next_chunk(&self, handle: BlobHandle) -> Option<ChunkRange> {
        self.plane.next_chunk(handle)
    }

    /// See [`BlobPlane::write_chunk`].
    pub fn write_chunk(&mut self, handle: BlobHandle, bytes: Vec<u8>) -> Vec<ClientFrame> {
        self.plane.write_chunk(handle, bytes)
    }

    /// Read `[offset, offset + len)` of a held handle for the fetch `fetch`.
    pub fn read(
        &mut self,
        handle: BlobHandle,
        fetch: FetchId,
        offset: u64,
        len: u64,
    ) -> Result<(ReadId, Vec<ClientFrame>), ReadError> {
        self.plane.read(handle, PlaneTag::Fetch(fetch), offset, len)
    }

    /// Give `handle` up. Answers the `BlobClose` to send, if any.
    ///
    /// The events a close raises — an upload or a read ended `Closed` — are
    /// the closer's own doing and are not handed on: the user that closes a
    /// handle has already forgotten what it was for.
    pub fn close(&mut self, handle: BlobHandle) -> Vec<ClientFrame> {
        self.sizes.remove(&handle);
        self.plane.close(handle).frames
    }

    /// Settle one blob answer and split its events by tag. `Err` is an answer
    /// the plane is not owed, fatal on the connection.
    pub fn on_frame(&mut self, frame: ServerFrame) -> Result<PlaneAnswer, String> {
        let step = self.plane.on_frame(frame)?;
        let mut answer = PlaneAnswer {
            frames: step.frames,
            ..PlaneAnswer::default()
        };
        for event in step.events {
            if let BlobEvent::UploadRefused { handle, .. } | BlobEvent::OpenRefused { handle, .. } =
                &event
            {
                self.sizes.remove(handle);
            }
            route(event, &mut answer);
        }
        Ok(answer)
    }
}

/// Hand one event to the user its tag names.
fn route(event: BlobEvent<PlaneTag>, answer: &mut PlaneAnswer) {
    let fetch = |tag: PlaneTag| match tag {
        PlaneTag::Fetch(id) => id,
        PlaneTag::Upload(id) => {
            panic!("surface client: a read event tagged for upload {id:?}; the stage reads nothing")
        }
    };
    match event {
        BlobEvent::Sealed { tag, handle } => match tag {
            PlaneTag::Upload(tag) => answer.upload.push(BlobEvent::Sealed { tag, handle }),
            PlaneTag::Fetch(id) => {
                panic!("surface client: a seal answered fetch {id:?}, which creates nothing")
            }
        },
        BlobEvent::UploadRefused {
            tag,
            handle,
            reason,
        } => match tag {
            PlaneTag::Upload(tag) => answer.upload.push(BlobEvent::UploadRefused {
                tag,
                handle,
                reason,
            }),
            PlaneTag::Fetch(id) => {
                panic!(
                    "surface client: an upload refusal answered fetch {id:?}, which creates nothing"
                )
            }
        },
        BlobEvent::Opened {
            tag,
            handle,
            size,
            content_type,
        } => match tag {
            PlaneTag::Fetch(tag) => answer.fetch.push(BlobEvent::Opened {
                tag,
                handle,
                size,
                content_type,
            }),
            PlaneTag::Upload(tag) => answer.upload.push(BlobEvent::Opened {
                tag,
                handle,
                size,
                content_type,
            }),
        },
        BlobEvent::OpenRefused {
            tag,
            handle,
            reason,
        } => match tag {
            PlaneTag::Fetch(tag) => answer.fetch.push(BlobEvent::OpenRefused {
                tag,
                handle,
                reason,
            }),
            PlaneTag::Upload(tag) => answer.upload.push(BlobEvent::OpenRefused {
                tag,
                handle,
                reason,
            }),
        },
        BlobEvent::Chunk {
            read,
            handle,
            offset,
            bytes,
        } => answer.fetch.push(BlobEvent::Chunk {
            read,
            handle,
            offset,
            bytes,
        }),
        BlobEvent::ReadDone { read, tag, handle } => answer.fetch.push(BlobEvent::ReadDone {
            read,
            tag: fetch(tag),
            handle,
        }),
        BlobEvent::ReadRefused {
            read,
            tag,
            handle,
            reason,
        } => answer.fetch.push(BlobEvent::ReadRefused {
            read,
            tag: fetch(tag),
            handle,
            reason,
        }),
    }
}
