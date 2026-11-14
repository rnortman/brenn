//! Admitting a part of a retained message — its body or one of its
//! attachments — named by channel and message id.
//! [`Messenger::admit_retained_part`] is the one function every door that
//! names a message's part calls: a conversation through
//! [`Messenger::admit_conversation_name`], a remote attacher through its blob
//! frames. It never asks who is naming; each caller has already settled
//! authority over the channel.
//!
//! A part is admitted when the channel is in the directory now, the message
//! is retained in the channel's own window — the whole ring, or a durable
//! channel's standing depth — and not parked, and the message carries the
//! name. An attachment's hold is taken under the store's own lock, so the
//! blob cannot lose its last holder between the check and the hold. On the
//! tool substrate's channels an attachment is refused unless the caller
//! admits it.

use uuid::Uuid;

use brenn_messaging_store::blob::{BlobHold, Retain};
use brenn_messaging_store::store::RingNamed;

use crate::config::{Depth, SystemChannelFamily};
use crate::{AttachmentName, BlobRef, MessageBody, MessageEnvelope, Messenger};

/// What part of a message is named.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NamedPart {
    Attachment(AttachmentName),
    Body,
}

/// What an admitted name gives.
#[derive(Debug)]
pub enum AdmittedPart {
    /// One hold on the named blob, which keeps its bytes whatever then
    /// happens to the message.
    Attachment(BlobHold),
    Body(MessageBody),
}

/// Whether an attachment may be named on a channel of the tool substrate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ToolChannelAttachments {
    Refused,
    Admitted,
}

/// Why [`Messenger::admit_retained_part`] refused a name.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PartRefusal {
    /// An attachment was named on a tool-substrate channel and the caller
    /// passed [`ToolChannelAttachments::Refused`]. Decided by the channel
    /// alone, before the message is looked at.
    ToolChannelAttachment,
    /// The channel is not in the directory now.
    ChannelGone,
    /// The message is not retained in its channel's window: evicted, past
    /// the window, on another channel, or parked.
    NotRetained,
    /// The message does not carry the name; `carried` is what it does carry.
    NoSuchAttachment { carried: Vec<AttachmentName> },
}

impl Messenger {
    /// Admit `part` of the message `message_id` on `channel` and hand over
    /// what it names.
    ///
    /// In order: an attachment named on a tool-substrate channel is refused
    /// when `tool_channel_attachments` is [`ToolChannelAttachments::Refused`];
    /// the directory resolves the channel; the message is retained in the
    /// channel's window and not parked; the message carries the name.
    ///
    /// # Panics
    ///
    /// If a retained message names a blob the store cannot hold or holds
    /// under a different reference, or the directory lists a non-durable
    /// channel with no retention store.
    pub async fn admit_retained_part(
        &self,
        channel: &str,
        message_id: Uuid,
        part: &NamedPart,
        tool_channel_attachments: ToolChannelAttachments,
    ) -> Result<AdmittedPart, PartRefusal> {
        let name = match part {
            NamedPart::Attachment(name) => Some(name),
            NamedPart::Body => None,
        };
        // TODO(tool-channel-attachment-watch-authority): no authority lets a
        // reader of the tool substrate's channels name their attachments.
        if tool_channel_attachments == ToolChannelAttachments::Refused
            && name.is_some()
            && SystemChannelFamily::of(channel) == Some(SystemChannelFamily::Tool)
        {
            return Err(PartRefusal::ToolChannelAttachment);
        }
        let entry = self
            .directory
            .resolve(channel)
            .ok_or(PartRefusal::ChannelGone)?;
        if !entry.capabilities().durable {
            let ring = self.ring_stores.get(&entry.uuid).unwrap_or_else(|| {
                panic!(
                    "messaging: non-durable channel {channel:?} is in the directory but has no \
                     retention store"
                )
            });
            return match ring.retained_named(message_id, name) {
                RingNamed::NotRetained => Err(PartRefusal::NotRetained),
                RingNamed::Body(envelope) => Ok(AdmittedPart::Body(envelope.body.clone())),
                RingNamed::NoSuchAttachment(envelope) => Err(no_such_attachment(&envelope)),
                RingNamed::Attachment(envelope, hold) => {
                    let name = name.expect("an attachment answers only a name");
                    assert_holds_reference(&envelope, name, &hold);
                    Ok(AdmittedPart::Attachment(hold))
                }
            };
        }

        let conn = self.db.lock().await;
        let Some(row) = crate::db::retained_message_by_uuid(&conn, message_id) else {
            return Err(PartRefusal::NotRetained);
        };
        if row.channel_uuid != entry.uuid {
            return Err(PartRefusal::NotRetained);
        }
        if let Depth::Bounded(depth) = entry.resolved_channel.standing_retain_depth {
            let newer =
                crate::db::channel_retained_count_after_seq(&conn, entry.uuid, row.retained_seq);
            let depth = i64::try_from(depth).unwrap_or(i64::MAX);
            if newer >= depth {
                return Err(PartRefusal::NotRetained);
            }
        }
        let envelope = row.envelope;
        let Some(name) = name else {
            return Ok(AdmittedPart::Body(envelope.body));
        };
        let Some(reference) = envelope.attachments.get(name) else {
            return Err(no_such_attachment(&envelope));
        };
        let blobs = self.blob_store.as_ref().unwrap_or_else(|| {
            panic!(
                "messaging: retained message {message_id} on {channel} names blob {} and this \
                 host has no blob store",
                reference.id
            )
        });
        let hold = match blobs.retain(&reference.id) {
            Retain::Held(hold) => hold,
            Retain::NotFound => panic!(
                "messaging: retained message {message_id} on {channel} names blob {}, which the \
                 store does not hold",
                reference.id
            ),
            Retain::Unsettled => blobs
                .retain_settled(&reference.id, &conn)
                .unwrap_or_else(|e| {
                    panic!(
                        "messaging: retained message {message_id} on {channel} names blob {}, \
                         which the store cannot hold: {e}",
                        reference.id
                    )
                }),
        };
        drop(conn);
        assert_holds_reference(&envelope, name, &hold);
        Ok(AdmittedPart::Attachment(hold))
    }
}

fn no_such_attachment(envelope: &MessageEnvelope) -> PartRefusal {
    PartRefusal::NoSuchAttachment {
        carried: envelope
            .attachments
            .iter()
            .map(|(name, _)| name.clone())
            .collect(),
    }
}

/// Panic unless `hold` is on the blob `envelope` names as `name`, as the
/// store recorded it.
fn assert_holds_reference(envelope: &MessageEnvelope, name: &AttachmentName, hold: &BlobHold) {
    let named: &BlobRef = envelope.attachments.get(name).unwrap_or_else(|| {
        panic!(
            "message {} was admitted for attachment {name}, which it does not carry",
            envelope.message_id
        )
    });
    assert_eq!(
        &hold.reference(),
        named,
        "message {} names attachment {name} with a reference that differs from the store's",
        envelope.message_id
    );
}

#[cfg(test)]
mod tests;
