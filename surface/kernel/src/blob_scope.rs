//! What one activation may open of the attachments in its windows, and the
//! handles it holds.
//!
//! A component names an attachment only by `(port, index, name)` in the windows
//! its own activation was assembled with. The scope is built from those windows
//! before the entry is called, so a message that arrives or leaves while the
//! entry runs changes nothing a component can name; it is dropped with the
//! activation's in-flight frame, so no handle outlives the activation.
//!
//! `open` reads no byte and touches no network. Bytes are read out of the page's
//! attachment cache ([`crate::fetch`]) at `read`, and an attachment whose bytes
//! are not on the page answers `unavailable`.
//!
//! Built on both targets so its authority rules are tested natively; only the
//! browser's exports call it.

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;

use brenn_envelope::{AttachmentName, BlobRef, MessageEnvelope};
use uuid::Uuid;

use crate::activation::PageActivation;
use crate::bindings::AppliedBindings;
use crate::fetch::{Cached, Fetch};

/// The largest single read a page serves, in bytes. `limits` reports it.
pub const SURFACE_MAX_READ_BYTES: u32 = 1024 * 1024;

/// The largest single write a page serves: it accepts none.
pub const SURFACE_MAX_WRITE_BYTES: u32 = 0;

/// The most handles one activation holds at once.
pub const SURFACE_MAX_HANDLES: usize = 64;

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

/// A handle the scope does not hold: never minted, or already dropped.
///
/// The glue maps a guest's handle to the object it was given, so a component
/// cannot produce one; it is a glue fault and the caller traps.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnknownHandle(pub u32);

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

/// One open handle: the attachment it names.
#[derive(Debug, Clone)]
struct Opened {
    channel: String,
    message_id: Uuid,
    name: AttachmentName,
    size: u64,
    content_type: String,
}

/// One activation's attachments and handle table.
#[derive(Debug, Clone, Default)]
pub struct BlobScope {
    ports: BTreeMap<String, ScopePort>,
    handles: BTreeMap<u32, Opened>,
    next_handle: u32,
}

impl BlobScope {
    /// The scope of an instance not granted `blobs`: it names nothing.
    pub fn empty() -> Self {
        Self::default()
    }

    /// The scope of `instance`'s activation: every input port window, with the
    /// channel `bindings` binds the port to. Empty when the instance is not
    /// granted `blobs`.
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
    pub fn of(bindings: &AppliedBindings, instance: &str, activation: &PageActivation) -> Self {
        if !bindings.instance_granted(instance, brenn_envelope::grants::ComponentGrant::Blobs) {
            return Self::empty();
        }
        let mut scope = Self::empty();
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

    /// How many handles are open.
    pub fn live_handles(&self) -> usize {
        self.handles.len()
    }

    /// Open attachment `name` of the message at `index` in `port`'s window.
    ///
    /// Succeeds whatever the cache holds of it, except that the server answered
    /// the message has left the channel's window: reading bytes is `read`'s.
    pub fn open(
        &mut self,
        cache: &impl CacheView,
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
        if matches!(
            cache.cached(&port.channel, message.message_id, &name),
            Cached::Gone
        ) {
            return Err(OpenRefusal::NotFound);
        }
        if self.handles.len() >= SURFACE_MAX_HANDLES {
            return Err(OpenRefusal::QuotaExceeded);
        }
        let opened = Opened {
            channel: port.channel.clone(),
            message_id: message.message_id,
            name,
            size: blob.size,
            content_type: blob.content_type.clone(),
        };
        // Never reused within the activation, so a stale number names nothing.
        self.next_handle = self
            .next_handle
            .checked_add(1)
            .expect("surface kernel: an activation opened 2^32 blob handles");
        self.handles.insert(self.next_handle, opened);
        Ok(self.next_handle)
    }

    /// The attachment's size, from its reference.
    pub fn size(&self, handle: u32) -> Result<u64, UnknownHandle> {
        Ok(self.opened(handle)?.size)
    }

    /// The attachment's content type, from its reference.
    pub fn content_type(&self, handle: u32) -> Result<&str, UnknownHandle> {
        Ok(&self.opened(handle)?.content_type)
    }

    /// Exactly the bytes `[offset, offset + len)`, if the page holds them.
    pub fn read(
        &self,
        cache: &impl CacheView,
        handle: u32,
        offset: u64,
        len: u32,
    ) -> Result<Result<Vec<u8>, ReadRefusal>, UnknownHandle> {
        let opened = self.opened(handle)?;
        if len > SURFACE_MAX_READ_BYTES {
            return Ok(Err(ReadRefusal::TooLarge));
        }
        let Some(end) = offset.checked_add(u64::from(len)) else {
            return Ok(Err(ReadRefusal::OutOfRange));
        };
        if end > opened.size {
            return Ok(Err(ReadRefusal::OutOfRange));
        }
        Ok(
            match cache.cached(&opened.channel, opened.message_id, &opened.name) {
                Cached::Bytes(bytes) => {
                    // The cache checked the bytes against the reference's size
                    // when the fetch ended, so the range is inside them.
                    let start = usize::try_from(offset).expect("offset is within a cached blob");
                    let end = usize::try_from(end).expect("end is within a cached blob");
                    Ok(bytes[start..end].to_vec())
                }
                Cached::Gone | Cached::Absent => Err(ReadRefusal::Unavailable),
            },
        )
    }

    /// Free the handle's slot.
    pub fn drop_handle(&mut self, handle: u32) -> Result<(), UnknownHandle> {
        self.handles
            .remove(&handle)
            .map(|_| ())
            .ok_or(UnknownHandle(handle))
    }

    fn opened(&self, handle: u32) -> Result<&Opened, UnknownHandle> {
        self.handles.get(&handle).ok_or(UnknownHandle(handle))
    }
}
