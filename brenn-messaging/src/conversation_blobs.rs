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
//! A tool result that carries attachments to a conversation is published to
//! the app's result inbox by [`Messenger::hand_tool_result`], which records it
//! for the calling conversation alone. No policy reaches that inbox, so the
//! record is the only way in. On the tool substrate's other channels a
//! policy shows a call's messages, and a conversation names their bodies but
//! never their attachments, which are for the call's parties.
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

use brenn_messaging_store::blob::HeldAttachments;

pub use crate::retained_part::{AdmittedPart, NamedPart};
use crate::retained_part::{PartRefusal, ToolChannelAttachments};
use crate::{
    AttachmentName, Attachments, MessageBody, MessageEnvelope, Messenger, PublishResult, Urgency,
    app_tool_result_inbox_address,
};

/// The most messages the read record keeps for one conversation.
pub const READ_RECORD_MESSAGES: usize = 1024;

/// The most conversations the read record keeps.
pub const READ_RECORD_CONVERSATIONS: usize = 256;

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
    /// The message is on a channel of the tool substrate other than the app's
    /// own result inbox. A tool call's attachments are for the call's parties.
    ToolChannelAttachment { channel: String },
}

impl NameRefusal {
    /// The refusal's kind as a tool's error JSON spells it.
    pub fn reason(&self) -> &'static str {
        match self {
            NameRefusal::NotRead => "not_read",
            NameRefusal::ChannelNotReadable { .. } => "channel_not_readable",
            NameRefusal::NotRetained { .. } => "not_retained",
            NameRefusal::NoSuchAttachment { .. } => "no_such_attachment",
            NameRefusal::ToolChannelAttachment { .. } => "tool_channel_attachment",
        }
    }
}

impl fmt::Display for NameRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NameRefusal::NotRead => f.write_str(
                "this conversation has not been shown that message since it started, since its \
                 last compaction, since a user last sent it a slash command, or since the server \
                 restarted; read it again with MessageChannelGet and retry, or, for a tool \
                 result, call the tool again",
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
            NameRefusal::ToolChannelAttachment { channel } => write!(
                f,
                "the attachments of a message on {channel} cannot be named: a tool call's \
                 attachments are for the tool and its caller, and a conversation that reads the \
                 call's messages is shown their names, sizes and content types only"
            ),
        }
    }
}

impl std::error::Error for NameRefusal {}

/// A tool result's attachments, published to the app's result inbox and
/// recorded for the conversation that made the call.
#[derive(Debug)]
pub struct HandedToolResult {
    /// The inbox message the conversation names the attachments by.
    pub message_id: Uuid,
    /// The references the message carries, for rendering.
    pub attachments: Attachments,
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

    /// Publish a tool result's attachments to `app_slug`'s result inbox as the
    /// system principal `component`, and record the message in the
    /// conversation's read record.
    ///
    /// The body is `{"v":1,"tool":"<tool>"}`: the conversation receives the
    /// tool's value in place, and the message exists to hold the attachments.
    /// Publishing and recording are one function so that no caller can do the
    /// first without the second. A refused publish is returned unrecorded, and
    /// `attachments`' holds drop with it.
    pub async fn hand_tool_result(
        &self,
        component: &str,
        app_slug: &str,
        conversation_id: i64,
        tool: &str,
        attachments: HeldAttachments,
    ) -> Result<HandedToolResult, PublishResult> {
        let address = app_tool_result_inbox_address(app_slug);
        let references = attachments.refs().clone();
        let tool = serde_json::to_string(tool).expect("a string serializes");
        let body = format!(r#"{{"v":1,"tool":{tool}}}"#);
        match self
            .publish_from_system_with_attachments(
                component,
                &address,
                MessageBody::text(body),
                Urgency::Normal,
                None,
                None,
                attachments,
            )
            .await
        {
            PublishResult::Ok { message_id, .. } => {
                self.conversation_reads
                    .record(conversation_id, [(message_id, address.as_str())]);
                Ok(HandedToolResult {
                    message_id,
                    attachments: references,
                })
            }
            refused => Err(refused),
        }
    }

    /// Admit the conversation's name for `part` of `message_id`, for the app
    /// `app_slug`, and hand over what it names.
    ///
    /// In order: the message is in the conversation's read record; the app's
    /// policy allows access to its channel; an attachment is not named on a
    /// tool-substrate channel other than the app's own result inbox; the
    /// directory resolves the channel; the message is retained in the
    /// channel's window — the whole ring, or a durable channel's standing
    /// depth — and not parked; the message carries the name. An attachment's
    /// hold is taken under the store's own lock, so the blob cannot lose its
    /// last holder between the check and the hold.
    ///
    /// `not_read`, `channel_not_readable` and `tool_channel_attachment` are
    /// logged at `warn`: a name the host never handed over is a mis-copied id
    /// or an injected one, and a tool call's attachment named by a
    /// conversation that only watched the call is a reach for another
    /// principal's blob.
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
                NameRefusal::NotRead
                | NameRefusal::ChannelNotReadable { .. }
                | NameRefusal::ToolChannelAttachment { .. } => warn!(
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
        let attachment = matches!(part, NamedPart::Attachment(_));
        // On the tool substrate's channels a call's attachments are for its
        // parties. A conversation names one only on its own app's result
        // inbox, which no policy covers and the record alone opens: only
        // `hand_tool_result` records its messages, each for the conversation
        // that made the call. A policy over the other three namespaces is a
        // watch of the traffic, and names bodies only.
        let own_inbox = channel == app_tool_result_inbox_address(app_slug);
        if !own_inbox && !policy.allows_channel_access(&channel) {
            return Err(NameRefusal::ChannelNotReadable { channel });
        }
        let tool_channel_attachments = if own_inbox {
            ToolChannelAttachments::Admitted
        } else {
            ToolChannelAttachments::Refused
        };
        self.admit_retained_part(&channel, message_id, part, tool_channel_attachments)
            .await
            .map_err(|refusal| match refusal {
                PartRefusal::ToolChannelAttachment => NameRefusal::ToolChannelAttachment {
                    channel: channel.clone(),
                },
                PartRefusal::ChannelGone => NameRefusal::ChannelNotReadable {
                    channel: channel.clone(),
                },
                PartRefusal::NotRetained => NameRefusal::NotRetained {
                    channel: channel.clone(),
                    attachment,
                },
                PartRefusal::NoSuchAttachment { carried } => {
                    NameRefusal::NoSuchAttachment { carried }
                }
            })
    }
}

#[cfg(test)]
mod tests;
