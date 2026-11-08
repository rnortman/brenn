//! The per-user location channels, `brenn:device-location.<username>`, and the
//! `system:device-location` participant that writes them.
//!
//! One durable channel per registered user carries every fix and sharing
//! change that user's devices report. Nothing is created on publish, so each
//! user's channel is minted by the server: from the users table when the
//! process plans its messaging, and at registration for a user created after
//! that. Both paths derive the same entry from the username, so they agree on
//! the row.
//!
//! The writer is publish-only and code-built: it holds `MessagingPublish` on
//! the family prefix and nothing else, and reads nothing from config, so it is
//! identical on every reload.

use brenn_envelope::grants::AppCapability;
use brenn_lib::access::AppPolicy;
use brenn_lib::access::acl::ChannelMatcher;
use rusqlite::Connection;

use crate::config::{
    DEVICE_LOCATION_SEND_RATE, MessagingGlobalConfig, SystemChannelTuning, resolve_system_channel,
};
use crate::db::upsert_channels;
use crate::system::SystemParticipantSpec;
use crate::{
    ChannelEntry, ChannelScheme, DEVICE_LOCATION_PREFIX, Messenger, device_location_address,
    device_location_channel_uuid,
};

/// System-participant component name; the identity is
/// `system:device-location`.
pub const DEVICE_LOCATION_COMPONENT: &str = "device-location";

/// The `system:device-location` participant: publish on every user's location
/// channel, subscribe on nothing.
///
/// The matcher is the bare family prefix, which ends at the `.` boundary and so
/// reaches `device-location.<username>` and nothing beside it.
pub fn device_location_spec() -> SystemParticipantSpec {
    let mut policy = AppPolicy::default();
    policy.grants.insert(AppCapability::MessagingPublish);
    policy
        .acls
        .brenn_publish
        .push(ChannelMatcher::Prefix(DEVICE_LOCATION_PREFIX.to_string()));
    SystemParticipantSpec {
        component: DEVICE_LOCATION_COMPONENT,
        policy,
        subscriptions: vec![],
    }
}

/// The directory entry for `username`'s location channel: durable `brenn:`,
/// its uuid derived from the address, depths from the system-channel resolver,
/// the send rate [`DEVICE_LOCATION_SEND_RATE`], no subscriber pre-set.
pub fn device_location_entry(
    username: &str,
    tuning: &SystemChannelTuning,
    defaults: &MessagingGlobalConfig,
) -> ChannelEntry {
    let address = device_location_address(username);
    let mut resolved_channel = resolve_system_channel(&address, tuning, defaults);
    resolved_channel.send_rate = DEVICE_LOCATION_SEND_RATE;
    ChannelEntry {
        uuid: device_location_channel_uuid(&address),
        resolved_channel,
        description: Some(format!(
            "Location fixes and sharing state reported by {username}'s devices"
        )),
        address,
        subscribers: vec![],
        transport_type: ChannelScheme::Brenn,
        mount: None,
    }
}

impl Messenger {
    /// Give `username` its location channel, idempotently.
    ///
    /// Called where a user is created; users that existed when messaging was
    /// planned already have theirs. A directory that already answers for the
    /// address means the row is there too, and the call returns the existing
    /// entry without touching the database.
    ///
    /// The caller supplies the connection because user creation already holds
    /// one; taking it here would deadlock against the caller's guard.
    pub fn provision_device_location_channel(
        &self,
        conn: &Connection,
        username: &str,
    ) -> ChannelEntry {
        let entry = device_location_entry(username, &self.system_channel_tuning, &self.defaults);
        if let Some(existing) = self.directory.resolve(&entry.address) {
            return (*existing).clone();
        }
        upsert_channels(conn, std::slice::from_ref(&entry));
        self.directory.add_channel(entry.clone());
        entry
    }
}

/// A `Messenger` over `db` with `system:device-location` registered as boot
/// registers it and a location channel provisioned for each of `usernames`.
///
/// `db` must carry the messaging tables.
#[cfg(any(test, feature = "testutils"))]
pub async fn device_location_messenger(
    db: brenn_db::Db,
    usernames: &[&str],
) -> std::sync::Arc<Messenger> {
    device_location_messenger_at_rate(db, usernames, DEVICE_LOCATION_SEND_RATE).await
}

/// [`device_location_messenger`], with every user's channel at `send_rate`
/// instead of the host's: for a test that needs the rate to refuse a publish.
#[cfg(any(test, feature = "testutils"))]
pub async fn device_location_messenger_at_rate(
    db: brenn_db::Db,
    usernames: &[&str],
    send_rate: crate::config::SendRate,
) -> std::sync::Arc<Messenger> {
    let messenger = crate::testutils::empty_directory_messenger_over(db.clone(), "test")
        .with_subscriber_registrations(crate::system::registrations_from_specs(&[
            device_location_spec(),
        ]));
    let conn = db.lock().await;
    for username in usernames {
        let mut entry = device_location_entry(
            username,
            &messenger.system_channel_tuning,
            &messenger.defaults,
        );
        entry.resolved_channel.send_rate = send_rate;
        upsert_channels(&conn, std::slice::from_ref(&entry));
        messenger.directory.add_channel(entry);
    }
    drop(conn);
    messenger
}

/// Insert `body` on `username`'s location channel as `sender`, stamped
/// `publish_ts`, bypassing the publish gates: for a test that needs a message
/// of a given age, or one from a participant the writer's policy would not be.
#[cfg(any(test, feature = "testutils"))]
pub async fn insert_device_location_message(
    messenger: &Messenger,
    username: &str,
    sender: &str,
    body: &str,
    publish_ts: chrono::DateTime<chrono::Utc>,
) {
    let address = device_location_address(username);
    let entry = messenger
        .directory
        .resolve(&address)
        .unwrap_or_else(|| panic!("no location channel {address:?}"));
    crate::testutils::insert_bus_message_from(
        messenger,
        &entry,
        sender,
        body,
        ChannelScheme::Brenn,
        crate::db::utc_to_ns(publish_ts),
    )
    .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutils::empty_directory_messenger;

    fn bare_covered(spec: &SystemParticipantSpec, bare: &str) -> bool {
        spec.policy
            .acls
            .brenn_publish
            .iter()
            .any(|m| m.matches(bare))
    }

    #[test]
    fn the_writer_publishes_on_the_family_and_nothing_beside_it() {
        let spec = device_location_spec();
        assert_eq!(spec.component, DEVICE_LOCATION_COMPONENT);
        assert!(spec.subscriptions.is_empty());
        assert!(spec.policy.grants.has(AppCapability::MessagingPublish));
        assert!(!spec.policy.grants.has(AppCapability::MessagingSubscribe));
        assert!(bare_covered(&spec, "device-location.alice"));
        assert!(!bare_covered(&spec, "device-locationx"));
        assert!(!bare_covered(&spec, "device-location"));
        assert!(!bare_covered(&spec, "tools/device-location.alice"));
        assert!(!bare_covered(&spec, "alice.status"));
    }

    #[test]
    fn the_spec_reads_nothing_from_config() {
        assert_eq!(device_location_spec(), device_location_spec());
    }

    #[test]
    fn the_entry_is_the_users_durable_channel_at_the_family_default() {
        let entry = device_location_entry(
            "alice",
            &SystemChannelTuning::default(),
            &MessagingGlobalConfig::default(),
        );
        assert_eq!(entry.address, "brenn:device-location.alice");
        assert_eq!(
            entry.uuid,
            device_location_channel_uuid("brenn:device-location.alice")
        );
        assert_eq!(entry.transport_type, ChannelScheme::Brenn);
        assert!(entry.capabilities().durable);
        assert!(entry.subscribers.is_empty());
        assert_eq!(
            entry.resolved_channel.retain_depth,
            crate::config::DEVICE_LOCATION_DEFAULT_RETAIN_DEPTH
        );
    }

    /// The operator's `default_send_rate` does not reach the family: a stop
    /// shares the bucket with the user's fixes, so the rate is the host's.
    #[test]
    fn the_entry_takes_the_host_send_rate() {
        let restrictive = crate::config::SendRate {
            burst: 1,
            refill_interval_secs: 3600,
            refill: 1,
        };
        let defaults = MessagingGlobalConfig {
            default_send_rate: restrictive,
            ..MessagingGlobalConfig::default()
        };
        let entry = device_location_entry("alice", &SystemChannelTuning::default(), &defaults);
        assert_eq!(entry.resolved_channel.send_rate, DEVICE_LOCATION_SEND_RATE);
    }

    #[tokio::test]
    async fn provisioning_adds_the_row_and_the_entry_once() {
        let m = empty_directory_messenger("test");
        let address = device_location_address("alice");
        assert!(m.directory().resolve(&address).is_none());

        let conn = m.db.lock().await;
        let first = m.provision_device_location_channel(&conn, "alice");
        let second = m.provision_device_location_channel(&conn, "alice");
        assert_eq!(first.uuid, second.uuid);
        let rows: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM messaging_channels WHERE address = ?1",
                [&address],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(rows, 1);
        drop(conn);

        let resolved = m
            .directory()
            .resolve(&address)
            .expect("provisioning adds the directory entry");
        assert_eq!(resolved.uuid, first.uuid);
        assert!(
            m.directory()
                .resolve(&device_location_address("bob"))
                .is_none()
        );
    }
}
