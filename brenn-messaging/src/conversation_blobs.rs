//! How a conversation names a blob: as an attachment of a message the host
//! handed it, or as that message's body. A conversation never handles a blob
//! id; [`Messenger::admit_conversation_name`] is the one function that turns a
//! name into a hold or a body for it.
//!
//! The host keeps, per conversation, the bus messages it has handed that
//! conversation — the read record. A name is admitted only for a message in
//! the record that is still retained, in its channel's own window, on a
//! channel the conversation's app may read. Both conditions are asked of that
//! one message.
//!
//! The record is in memory and lost with the server process. It is bounded
//! by dropping, never by waiting: [`READ_RECORD_MESSAGES`] messages per
//! conversation, oldest dropped first, and [`READ_RECORD_CONVERSATIONS`]
//! conversations, the least recently fed dropped whole. A dropped entry costs
//! the conversation a re-read and never causes a wrong admission.

use std::fmt;
use std::sync::{Arc, Mutex};

use indexmap::IndexMap;
use tracing::{debug, warn};
use uuid::Uuid;

use brenn_messaging_store::blob::{BlobHold, Retain};
use brenn_messaging_store::store::RingNamed;

use crate::config::Depth;
use crate::{AttachmentName, BlobRef, MessageBody, MessageEnvelope, Messenger};

/// The most messages the read record keeps for one conversation.
pub const READ_RECORD_MESSAGES: usize = 1024;

/// The most conversations the read record keeps.
pub const READ_RECORD_CONVERSATIONS: usize = 256;

/// What part of a message a conversation names.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NamedPart {
    Attachment(AttachmentName),
    Body,
}

/// Why a name was refused.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NameRefusal {
    /// The message is not in the conversation's read record.
    NotRead,
    /// The app may not read the message's channel now, or the channel is
    /// gone.
    ChannelNotReadable { channel: String },
    /// The message is not retained in its channel's window: evicted, past the
    /// window, or parked. `attachment` is whether an attachment was named
    /// rather than the body.
    NotRetained { channel: String, attachment: bool },
    /// The message does not carry the name; `carried` is what it does carry.
    NoSuchAttachment { carried: Vec<AttachmentName> },
}

impl NameRefusal {
    /// The refusal's kind as a tool's error JSON spells it.
    pub fn reason(&self) -> &'static str {
        match self {
            NameRefusal::NotRead => "not_read",
            NameRefusal::ChannelNotReadable { .. } => "channel_not_readable",
            NameRefusal::NotRetained { .. } => "not_retained",
            NameRefusal::NoSuchAttachment { .. } => "no_such_attachment",
        }
    }
}

impl fmt::Display for NameRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NameRefusal::NotRead => f.write_str(
                "this conversation has not been shown that message since it started, since its \
                 last compaction, since a user last sent it a slash command, or since the server \
                 restarted; read it again with MessageChannelGet and retry",
            ),
            NameRefusal::ChannelNotReadable { channel } => {
                write!(f, "this app may not read channel {channel} now")
            }
            NameRefusal::NotRetained {
                channel,
                attachment: true,
            } => write!(
                f,
                "the message is not retained on {channel} now; another message carrying the same \
                 attachment, if this conversation can read one, names it"
            ),
            NameRefusal::NotRetained {
                channel,
                attachment: false,
            } => write!(f, "the message is not retained on {channel} now"),
            NameRefusal::NoSuchAttachment { carried } if carried.is_empty() => {
                f.write_str("the message carries no attachments")
            }
            NameRefusal::NoSuchAttachment { carried } => {
                let names: Vec<&str> = carried.iter().map(AttachmentName::as_str).collect();
                write!(
                    f,
                    "the message carries no attachment of that name; it carries: {}",
                    names.join(", ")
                )
            }
        }
    }
}

impl std::error::Error for NameRefusal {}

/// What an admitted name gives.
#[derive(Debug)]
pub enum AdmittedPart {
    /// One hold on the named blob, which keeps its bytes whatever then
    /// happens to the message.
    Attachment(BlobHold),
    Body(MessageBody),
}

/// Per conversation id, the bus messages the host has handed that
/// conversation, each as `message id → channel address`. Both maps are
/// ordered oldest first: a conversation moves to the back each time it is
/// fed, and a message each time it is recorded. A run of messages on one
/// channel within one batch shares one copy of its address.
#[derive(Debug, Default)]
pub(crate) struct ConversationReads {
    by_conversation: Mutex<IndexMap<i64, IndexMap<Uuid, Arc<str>>>>,
}

impl ConversationReads {
    fn record<'a>(&self, conversation_id: i64, reads: impl IntoIterator<Item = (Uuid, &'a str)>) {
        let mut reads = reads.into_iter().peekable();
        if reads.peek().is_none() {
            return;
        }
        let mut all = self.lock();
        let mut record = all.shift_remove(&conversation_id).unwrap_or_default();
        let mut shared: Option<Arc<str>> = None;
        for (message_id, address) in reads {
            let address = match shared.take() {
                Some(last) if *last == *address => last,
                _ => Arc::from(address),
            };
            shared = Some(address.clone());
            record.shift_remove(&message_id);
            record.insert(message_id, address);
            if record.len() > READ_RECORD_MESSAGES {
                record.shift_remove_index(0);
            }
        }
        all.insert(conversation_id, record);
        if all.len() > READ_RECORD_CONVERSATIONS {
            all.shift_remove_index(0);
        }
    }

    fn channel_of(&self, conversation_id: i64, message_id: Uuid) -> Option<String> {
        self.lock()
            .get(&conversation_id)
            .and_then(|record| record.get(&message_id))
            .map(|address| address.to_string())
    }

    fn forget(&self, conversation_id: i64) {
        self.lock().shift_remove(&conversation_id);
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, IndexMap<i64, IndexMap<Uuid, Arc<str>>>> {
        self.by_conversation
            .lock()
            .expect("conversation read record poisoned")
    }
}

impl Messenger {
    /// Record that every envelope in `envelopes` was handed to the
    /// conversation `conversation_id`.
    pub fn note_conversation_read<'a>(
        &self,
        conversation_id: i64,
        envelopes: impl IntoIterator<Item = &'a MessageEnvelope>,
    ) {
        self.conversation_reads.record(
            conversation_id,
            envelopes
                .into_iter()
                .map(|env| (env.message_id, env.channel.as_str())),
        );
    }

    /// Record that the conversation `conversation_id` published
    /// `message_id` onto `address`.
    pub fn note_conversation_sent(&self, conversation_id: i64, message_id: Uuid, address: &str) {
        self.conversation_reads
            .record(conversation_id, [(message_id, address)]);
    }

    /// Clear the conversation's read record: its context no longer holds what
    /// the host handed it.
    // TODO(blob-handle-integrity): a `--resume` answered with a fresh session,
    // and a slash command acted on after messages handed over mid-turn, reset
    // the context without a call here.
    pub fn forget_conversation_reads(&self, conversation_id: i64) {
        self.conversation_reads.forget(conversation_id);
    }

    /// Whether the conversation's read record holds `message_id`.
    #[cfg(any(test, feature = "testutils"))]
    pub fn conversation_has_read(&self, conversation_id: i64, message_id: Uuid) -> bool {
        self.conversation_reads
            .channel_of(conversation_id, message_id)
            .is_some()
    }

    /// Admit the conversation's name for `part` of `message_id`, for the app
    /// `app_slug`, and hand over what it names.
    ///
    /// In order: the message is in the conversation's read record; the app's
    /// policy allows access to its channel and the directory resolves it;
    /// the message is retained in the channel's window — the whole ring, or a
    /// durable channel's standing depth — and not parked; the message carries
    /// the name. An attachment's hold is taken under the store's own lock, so
    /// the blob cannot lose its last holder between the check and the hold.
    ///
    /// `not_read` and `channel_not_readable` are logged at `warn`: a name the
    /// host never handed over is a mis-copied id or an injected one.
    ///
    /// # Panics
    ///
    /// If `app_slug` is not a registered app, or a retained message names a
    /// blob the store cannot hold or holds under a different reference.
    // TODO(conversation-blob-name-per-message): both conditions are asked of
    // one message, so an evicted message's blob is refused while another
    // retained, readable message names it.
    pub async fn admit_conversation_name(
        &self,
        conversation_id: i64,
        app_slug: &str,
        message_id: Uuid,
        part: &NamedPart,
    ) -> Result<AdmittedPart, NameRefusal> {
        let admitted = self
            .admit_named(conversation_id, app_slug, message_id, part)
            .await;
        if let Err(refusal) = &admitted {
            match refusal {
                NameRefusal::NotRead | NameRefusal::ChannelNotReadable { .. } => warn!(
                    app = %app_slug,
                    conversation_id,
                    %message_id,
                    reason = refusal.reason(),
                    "conversation named a message it may not name"
                ),
                NameRefusal::NotRetained { .. } | NameRefusal::NoSuchAttachment { .. } => debug!(
                    app = %app_slug,
                    conversation_id,
                    %message_id,
                    reason = refusal.reason(),
                    "conversation name refused"
                ),
            }
        }
        admitted
    }

    async fn admit_named(
        &self,
        conversation_id: i64,
        app_slug: &str,
        message_id: Uuid,
        part: &NamedPart,
    ) -> Result<AdmittedPart, NameRefusal> {
        let channel = self
            .conversation_reads
            .channel_of(conversation_id, message_id)
            .ok_or(NameRefusal::NotRead)?;
        let policy = self.app_policy(app_slug).unwrap_or_else(|| {
            panic!(
                "Messenger::admit_conversation_name: app {app_slug:?} is not a registered app — \
                 host wiring bug (the app slug is host-plumbed from the bridge, never tool input)"
            )
        });
        let not_readable = || NameRefusal::ChannelNotReadable {
            channel: channel.clone(),
        };
        if !policy.allows_channel_access(&channel) {
            return Err(not_readable());
        }
        let entry = self.directory.resolve(&channel).ok_or_else(not_readable)?;
        let name = match part {
            NamedPart::Attachment(name) => Some(name),
            NamedPart::Body => None,
        };
        let not_retained = || NameRefusal::NotRetained {
            channel: channel.clone(),
            attachment: name.is_some(),
        };
        if !entry.capabilities().durable {
            let ring = self.ring_stores.get(&entry.uuid).unwrap_or_else(|| {
                panic!(
                    "messaging: non-durable channel {channel:?} is in the directory but has no \
                     retention store"
                )
            });
            return match ring.retained_named(message_id, name) {
                RingNamed::NotRetained => Err(not_retained()),
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
            return Err(not_retained());
        };
        if row.channel_uuid != entry.uuid {
            return Err(not_retained());
        }
        if let Depth::Bounded(depth) = entry.resolved_channel.standing_retain_depth {
            let newer =
                crate::db::channel_retained_count_after_seq(&conn, entry.uuid, row.retained_seq);
            let depth = i64::try_from(depth).unwrap_or(i64::MAX);
            if newer >= depth {
                return Err(not_retained());
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

fn no_such_attachment(envelope: &MessageEnvelope) -> NameRefusal {
    NameRefusal::NoSuchAttachment {
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
