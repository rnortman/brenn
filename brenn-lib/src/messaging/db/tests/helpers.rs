use crate::db::format_ts_for_db;
use crate::messaging::config::{Depth, NoiseLevel, ResolvedChannel, Sink};
use crate::messaging::db::*;
use crate::messaging::{
    ChannelEntry, ChannelScheme, MessagingDirectory, Urgency, WakeMin, canonical_address,
};
use chrono::{DateTime, Utc};
use rusqlite::Connection;
use uuid::Uuid;

pub(super) fn default_resolved_channel() -> ResolvedChannel {
    ResolvedChannel {
        push_depth: Depth::Unbounded,
        retain_depth: Depth::Unbounded,
        standing_retain_depth: Depth::Unbounded,
        noise: NoiseLevel::Silent,
        sink: Sink::Drop,
        wake_min: WakeMin::Normal,
    }
}

pub(super) fn make_directory() -> (MessagingDirectory, Uuid) {
    let uuid = Uuid::new_v4();
    let dir = MessagingDirectory::with_entries(vec![ChannelEntry {
        uuid,
        address: canonical_address("test"),
        description: None,
        resolved_channel: default_resolved_channel(),
        subscribers: vec![],
        transport_type: ChannelScheme::Brenn,
        mount: None,
    }]);
    (dir, uuid)
}

/// Helper: insert a message with N push rows (one per conversation_id provided).
/// Returns (message internal id, message uuid).
#[allow(clippy::too_many_arguments)]
pub(super) fn insert_msg(
    conn: &Connection,
    channel_uuid: Uuid,
    sender: &str,
    body: &str,
    deliver_after: Option<DateTime<Utc>>,
    conv_ids: &[i64],
) -> (i64, Uuid) {
    let ns = utc_to_ns(Utc::now());
    let inserted = insert_message_with_pushes(
        conn,
        channel_uuid,
        "src",
        sender,
        body,
        Urgency::Low, // Low is the §2.7 mapping for old 'none' (no eager wake at default wake_min=Normal)
        crate::messaging::ChannelScheme::Brenn,
        None,
        None,
        deliver_after,
        ns,
        &conv_ids
            .iter()
            .map(|&cid| PendingPushInsert {
                target_subscriber: ParticipantId::for_conversation(cid),
                target_app_slug: "app".to_string(),
                eager_wake: false,
                release_after: deliver_after.filter(|da| *da > Utc::now()),
                delivery_deadline: None,
            })
            .collect::<Vec<_>>(),
    );
    (inserted.id, inserted.uuid)
}

pub(super) fn mark_push_delivered(conn: &Connection, message_id: i64, conv_id: i64) {
    let now = format_ts_for_db(Utc::now());
    let subscriber = ParticipantId::for_conversation(conv_id);
    conn.execute(
        "UPDATE messaging_pending_pushes SET delivered_at = ?1
         WHERE message_id = ?2 AND target_subscriber = ?3 AND delivered_at IS NULL",
        rusqlite::params![now, message_id, subscriber.as_str()],
    )
    .unwrap();
}
