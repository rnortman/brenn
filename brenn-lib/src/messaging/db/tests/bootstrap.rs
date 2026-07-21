use super::helpers::*;
use crate::db::init_db_memory;
use crate::messaging::canonical_address;
use crate::messaging::db::*;
use crate::messaging::{ChannelEntry, ChannelScheme, webhook_channel_uuid_from_slug};
use uuid::Uuid;

#[test]
fn upsert_channels_idempotent_and_renames() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    let uuid = Uuid::new_v4();
    let entry = ChannelEntry {
        uuid,
        address: canonical_address("first"),
        description: Some("v1".to_string()),
        resolved_channel: default_resolved_channel(),
        subscribers: vec![],
        transport_type: ChannelScheme::Brenn,
        mount: None,
    };
    upsert_channels(&conn, std::slice::from_ref(&entry));

    // Rename (same UUID, new address).
    let renamed = ChannelEntry {
        uuid,
        address: canonical_address("renamed"),
        description: Some("v2".to_string()),
        resolved_channel: default_resolved_channel(),
        subscribers: vec![],
        transport_type: ChannelScheme::Brenn,
        mount: None,
    };
    upsert_channels(&conn, &[renamed]);

    let (addr, desc): (String, String) = conn
        .query_row(
            "SELECT address, description FROM messaging_channels WHERE uuid = ?1",
            rusqlite::params![uuid.as_bytes().to_vec()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(addr, canonical_address("renamed"));
    assert_eq!(desc, "v2");
}

/// `upsert_channels` persists `transport_type='webhook'` for a webhook channel,
/// and the persisted value survives a re-upsert (idempotent).
#[test]
fn upsert_channels_persists_webhook_transport_type() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    let slug = "my-endpoint";
    let uuid = webhook_channel_uuid_from_slug(slug);
    let entry = ChannelEntry {
        uuid,
        address: format!("webhook:{slug}"),
        description: None,
        resolved_channel: default_resolved_channel(),
        subscribers: vec![],
        transport_type: ChannelScheme::Webhook,
        mount: Some("/webhooks/my-endpoint".to_string()),
    };
    upsert_channels(&conn, std::slice::from_ref(&entry));

    let transport_type: String = conn
        .query_row(
            "SELECT transport_type FROM messaging_channels WHERE uuid = ?1",
            rusqlite::params![uuid.as_bytes().to_vec()],
            |row| row.get(0),
        )
        .expect("row must exist after upsert");
    assert_eq!(transport_type, "webhook");

    // Re-upsert is idempotent.
    upsert_channels(&conn, &[entry]);
    let count: i64 = conn
        .query_row(
            "SELECT count(*) FROM messaging_channels WHERE uuid = ?1",
            rusqlite::params![uuid.as_bytes().to_vec()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(count, 1, "re-upsert must not duplicate the row");
}
