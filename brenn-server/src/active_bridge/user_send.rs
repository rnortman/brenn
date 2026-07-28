//! Accepting a user turn, from whichever door it arrived by.
//!
//! Persist the row, echo it to every attached browser, restore the impetus pool
//! if this door is the attended one, hand the text to CC. That sequence is
//! identical for a message typed
//! into the legacy websocket and one published on the conversation's command
//! channel; only the attribution and the CC prefix differ, and both are the
//! caller's to compute. Nothing here touches connection state, so the bus
//! adapter can call it with no browser in sight.

use std::sync::Arc;

use brenn_lib::ws_types::{AttachmentMeta, SelectedTask, WsServerMessage};

use super::ActiveBridge;

/// Who spoke, and how the record should say so.
#[derive(Debug, Clone)]
pub(crate) enum SendOrigin {
    /// Typed into a browser session. The echo carries the username but no bus
    /// attribution.
    LegacyWs {
        username: String,
        /// The sender's local time, already formatted — the browser renders it
        /// verbatim.
        timestamp: String,
    },
    /// Published on the conversation's command channel. The echo carries the
    /// publishing participant, which tells the chat adapter that the record
    /// already has this message and the broadcast is browser-only.
    Bus {
        /// `ParticipantId` string of the peer that published the command.
        sender: String,
        timestamp: String,
    },
}

impl SendOrigin {
    /// The name a browser shows beside the bubble. A bus peer has no username,
    /// so its participant id stands in.
    fn display_name(&self) -> &str {
        match self {
            Self::LegacyWs { username, .. } => username,
            Self::Bus { sender, .. } => sender,
        }
    }

    fn timestamp(&self) -> &str {
        match self {
            Self::LegacyWs { timestamp, .. } | Self::Bus { timestamp, .. } => timestamp,
        }
    }

    fn bus_sender(&self) -> Option<String> {
        match self {
            Self::LegacyWs { .. } => None,
            Self::Bus { sender, .. } => Some(sender.clone()),
        }
    }
}

/// A system message that lands between the echo and the CC send.
///
/// It is persisted and shown *after* the user's own bubble, but its text rides
/// in the same NDJSON envelope as the message rather than reaching CC on its
/// own — so the subprocess never sees it dangling without the message it
/// accompanies. The caller puts that text in
/// [`AcceptedSend::extra_blocks`].
pub(crate) struct Interstitial {
    pub render: crate::system_message::SystemMessageRender,
    /// `None` attributes the row to the conversation owner.
    pub attribute_to_user_id: Option<i64>,
}

/// One accepted user turn, with everything the two doors compute differently
/// already computed.
pub(crate) struct AcceptedSend<'a> {
    /// Raw text as the sender wrote it: what the DB row and the echo carry.
    pub text: &'a str,
    /// What CC receives — the raw text plus whatever prefix and attachment
    /// notices the caller's door adds.
    pub cc_text: String,
    /// Extra CC content blocks delivered in the same NDJSON envelope as the
    /// message, so a partial failure cannot separate them from it.
    pub extra_blocks: Vec<String>,
    /// Whose row this is in `messages.sender_user_id`. A bus send is attributed
    /// to the conversation owner, which is the same default a system message
    /// uses.
    pub sender_user_id: i64,
    pub sender_tz: Option<&'a str>,
    pub sender_device_id: Option<i64>,
    /// Attachment rows to write against the new message, and their metadata for
    /// the echo. Always empty for a bus send.
    pub attachments: Vec<crate::routes::upload::ResolvedAttachment>,
    /// Task context chips, a legacy-websocket concept the bus protocol does not
    /// carry.
    pub selected_tasks: Vec<SelectedTask>,
    pub origin: SendOrigin,
    /// A system message to land after the echo and before the CC send.
    pub interstitial: Option<Interstitial>,
    /// Whether this turn restores the conversation's impetus pool.
    ///
    /// True for the legacy websocket door, which *is* the attended surface: a
    /// person typed this, so the pool that bounds unattended activity goes back
    /// to full and whatever an exhausted pool was holding rides the same turn.
    /// It refills and never draws — an attended turn does not spend the
    /// allowance that attention just granted.
    ///
    /// False for a bus command, which restores the pool only on carried impetus
    /// and does that for itself, and false where the deployment has no bus.
    pub restores_impetus_pool: bool,
}

/// Persist the turn, echo it, restore the impetus pool, and hand it to CC.
///
/// Returns the CC send error, if any. The row and the echo are already
/// committed at that point and are not rolled back — the message happened; only
/// its delivery to the subprocess failed, and the caller reports that on its own
/// door.
pub(crate) async fn accept_user_send(
    bridge: &Arc<ActiveBridge>,
    send: AcceptedSend<'_>,
) -> Option<String> {
    let attachment_metas: Vec<AttachmentMeta> =
        send.attachments.iter().map(|r| r.to_meta()).collect();
    let attachments = send.attachments;

    let (_msg_id, echo_db_seq) = bridge
        .persist_user_message_with_attachments(
            send.text,
            send.sender_user_id,
            send.sender_tz,
            send.sender_device_id,
            |msg_id| {
                attachments
                    .into_iter()
                    .map(|r| brenn_lib::conversation::StoredAttachment {
                        upload_id: r.upload_id.to_string(),
                        message_id: msg_id,
                        filename: r.filename,
                        media_type: r.media_type,
                        size: r.size,
                        disk_filename: r.disk_filename,
                    })
                    .collect()
            },
        )
        .await;

    // seq lets the frontend deduplicate this live broadcast against a
    // concurrent history replay.
    bridge.broadcast_user_echo(WsServerMessage::UserMessageEcho {
        text: send.text.to_string(),
        username: send.origin.display_name().to_string(),
        timestamp: send.origin.timestamp().to_string(),
        attachments: attachment_metas,
        selected_tasks: send.selected_tasks,
        seq: Some(echo_db_seq),
        bus_sender: send.origin.bus_sender(),
    });

    if send.restores_impetus_pool {
        bridge.refill_impetus_pool().await;
    }

    if let Some(interstitial) = send.interstitial {
        bridge
            .persist_and_broadcast_system_message(
                interstitial.render,
                interstitial.attribute_to_user_id,
            )
            .await;
    }

    if send.extra_blocks.is_empty() {
        bridge.send_message(&send.cc_text).await.err()
    } else {
        let msg = brenn_cc::protocol::user_message_with_context(&send.cc_text, &send.extra_blocks);
        bridge.send_outgoing(msg).await.err()
    }
}
