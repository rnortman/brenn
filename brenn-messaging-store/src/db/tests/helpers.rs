use crate::db::*;
use brenn_lib::messaging::config::{Depth, NoiseLevel, ResolvedChannel, Sink};
use brenn_lib::messaging::{
    ChannelEntry, ChannelScheme, MessagingDirectory, Urgency, WakeMin, canonical_address,
};
use chrono::{DateTime, Utc};
use rusqlite::Connection;
use uuid::Uuid;

pub(super) fn default_resolved_channel() -> ResolvedChannel {
    ResolvedChannel {
        send_rate: Default::default(),
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

/// Helper: insert one message row. Returns (message internal id, message uuid).
///
/// `deliver_after` parks it: a parked message holds no retention position, which
/// is the whole of what hides it from every read.
pub(super) fn insert_msg(
    conn: &Connection,
    channel_uuid: Uuid,
    sender: &str,
    body: &str,
    deliver_after: Option<DateTime<Utc>>,
) -> (i64, Uuid) {
    let ns = utc_to_ns(Utc::now());
    let inserted = insert_message(
        conn,
        channel_uuid,
        "src",
        sender,
        body,
        Urgency::Low, // Low is the mapping for old 'none' (no eager wake at default wake_min=Normal)
        brenn_lib::messaging::ChannelScheme::Brenn,
        None,
        None,
        deliver_after,
        None,
        ns,
    );
    (inserted.id, inserted.uuid)
}
