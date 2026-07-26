use rusqlite::{Connection, OptionalExtension};

use crate::db::format_ts_for_db;
use chrono::Utc;

use super::super::{
    ChannelEntry, ChannelScheme,
    config::{Depth, MessagingGlobalConfig, NoiseLevel, ResolvedChannel},
};
use uuid::Uuid;

/// Upsert all configured channels into `messaging_channels`. UUIDs not
/// present in config are kept (so renamed channels keep their history);
/// operators delete obsolete channels manually if desired.
pub fn upsert_channels(conn: &Connection, entries: &[ChannelEntry]) {
    let now = format_ts_for_db(Utc::now());
    for entry in entries {
        let uuid_bytes = entry.uuid.as_bytes().to_vec();
        // Try insert; on UNIQUE address conflict we surface the panic.
        // First try INSERT OR IGNORE keyed by uuid (PK), then UPDATE
        // address/description if the row already existed.
        let transport_type_str = entry.transport_type.as_str();
        // The UPDATE below never touches resume_epoch, so an existing channel
        // keeps the epoch it was born with (the epoch dies only with the row).
        conn.execute(
            "INSERT OR IGNORE INTO messaging_channels \
             (uuid, address, description, transport_type, created_at, resume_epoch)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params![
                uuid_bytes,
                &entry.address,
                &entry.description,
                transport_type_str,
                &now,
                Uuid::new_v4().as_bytes().to_vec(),
            ],
        )
        .unwrap_or_else(|e| {
            panic!(
                "messaging: failed to upsert channel {:?} (UUID conflict?): {e}",
                entry.address,
            )
        });
        conn.execute(
            "UPDATE messaging_channels \
             SET address = ?2, description = ?3, transport_type = ?4 WHERE uuid = ?1",
            rusqlite::params![
                uuid_bytes,
                &entry.address,
                &entry.description,
                transport_type_str,
            ],
        )
        .unwrap_or_else(|e| {
            panic!(
                "messaging: failed to update channel {:?} \
                 (likely an address collision with another UUID): {e}",
                entry.address,
            )
        });
    }
}

/// Load the channels for the given UUIDs from `messaging_channels`, decoded into
/// [`ChannelEntry`] values (design §2.1, §2.3).
///
/// **Scoped, not full-table.** The caller passes exactly the distinct
/// `channel_uuid`s referenced by the surviving durable dynamic-subscription rows,
/// so this reconstructs only those channels — never every row in
/// `messaging_channels`. Orphan channels (runtime-created channels whose only/last
/// dynamic subscription was torn down, deleting its durable row) are by
/// construction not referenced by any surviving row and so are never requested,
/// never materialized, and add no per-orphan runtime memory (design §2.1 "Why not
/// load every channel").
///
/// Each returned entry has empty `subscribers` and `mount = None` (a reconstructed
/// channel is inert until the merge attaches subscribers — design §2.1 "Channel
/// address ≠ subscription"), and a `resolved_channel` stamped from `defaults` —
/// the same global messaging defaults the runtime `subscribe_dynamic` writer
/// (`subscribe.rs`) and the static `mqtt:`/webhook entry builders
/// (`bootstrap/messaging.rs`) use, so a reconstructed channel resolves identically
/// to the one created at runtime.
///
/// `transport_type` decodes via [`ChannelScheme::parse`]; an unparseable value is
/// host-written corruption and panics (CLAUDE.md BETTER DEAD THAN WRONG, consistent with
/// the `db/dynamic.rs` decoders — this read runs on host-written startup state,
/// not attacker-influenceable inbound traffic). A requested UUID that is not
/// present in `messaging_channels` simply yields no entry; the caller's merge then
/// classifies its durable row as genuine config drift (`dropped`), the existing
/// correct behavior.
pub fn load_channels_by_uuids(
    conn: &Connection,
    uuids: &[Uuid],
    defaults: &MessagingGlobalConfig,
) -> Vec<ChannelEntry> {
    let mut entries = Vec::new();
    let mut stmt = conn
        .prepare(
            "SELECT uuid, address, description, transport_type \
             FROM messaging_channels WHERE uuid = ?1",
        )
        .expect("messaging: prepare load_channels_by_uuids");
    let resolved_channel = ResolvedChannel {
        send_rate: Default::default(),
        push_depth: defaults.default_push_depth,
        retain_depth: defaults.default_retain_depth,
        standing_retain_depth: defaults.default_standing_retain_depth,
        noise: defaults.default_noise,
        sink: defaults.default_sink,
        wake_min: defaults.default_wake_min,
    };
    for uuid in uuids {
        let entry = stmt
            .query_row(rusqlite::params![uuid.as_bytes().to_vec()], |row| {
                let address: String = row.get(1)?;
                let description: Option<String> = row.get(2)?;
                let transport_type_s: String = row.get(3)?;
                Ok((address, description, transport_type_s))
            })
            .optional()
            .unwrap_or_else(|e| {
                // Include the UUID so a startup panic is self-diagnosing
                // (errhandling-4): the operator can tell exactly which channel
                // row failed to load.
                panic!("messaging: query load_channels_by_uuids for uuid={uuid}: {e}")
            });
        if let Some((address, description, transport_type_s)) = entry {
            let transport_type = ChannelScheme::parse(&transport_type_s).unwrap_or_else(|| {
                panic!(
                    "messaging: malformed channel transport_type {transport_type_s:?} \
                     for {address:?} in DB"
                )
            });
            entries.push(ChannelEntry {
                uuid: *uuid,
                address,
                description,
                resolved_channel: resolved_channel.clone(),
                subscribers: Vec::new(),
                transport_type,
                mount: None,
            });
        }
    }
    entries
}

/// Encode a `Depth` as its SQL wire form: integer string for `Bounded(n)`,
/// `"unbounded"` for `Unbounded`.
///
/// `pub(super)` so the runtime dynamic-subscription writer (`db/dynamic.rs`) can
/// share this single encoder rather than duplicating it (the read side's
/// `depth_from_sql` is its inverse).
pub(super) fn depth_to_sql(d: Depth) -> String {
    match d {
        Depth::Bounded(n) => n.to_string(),
        Depth::Unbounded => "unbounded".to_string(),
    }
}

/// Encode a `NoiseLevel` as its SQL wire form.
///
/// `pub(super)` for the same single-encoder reason as [`depth_to_sql`].
pub(super) fn noise_to_sql(n: NoiseLevel) -> &'static str {
    match n {
        NoiseLevel::Silent => "silent",
        NoiseLevel::Metered => "metered",
        NoiseLevel::Alarm => "alarm",
        NoiseLevel::Fatal => "fatal",
    }
}

/// Prune durable dynamic-subscription rows that the boot merge dropped (their
/// channel is gone from config, or a static sub now overrides them) from
/// `messaging_dynamic_subscriptions`, keyed by `(channel_uuid, app_slug)`
/// (design §2.1 "Boot merge" / "Mirror collision policy").
///
/// Removing them from the durable truth ensures the same conflict does not recur
/// on the next boot.
pub fn prune_dropped_dynamic_subscriptions(conn: &Connection, dropped: &[(Uuid, String)]) {
    for (channel_uuid, app_slug) in dropped {
        conn.execute(
            "DELETE FROM messaging_dynamic_subscriptions \
             WHERE channel_uuid = ?1 AND app_slug = ?2",
            rusqlite::params![channel_uuid.as_bytes().to_vec(), app_slug],
        )
        .expect("messaging: prune dropped dynamic subscription");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messaging::config::{MessagingGlobalConfig, ResolvedChannel, Sink};
    use crate::messaging::db::run_messaging_migrations;
    use crate::messaging::{ChannelScheme, WakeMin};

    fn test_conn() -> Connection {
        let conn = Connection::open_in_memory().expect("open in-memory db");
        conn.execute_batch("CREATE TABLE conversations (id INTEGER PRIMARY KEY);")
            .expect("create conversations stub");
        run_messaging_migrations(&conn);
        conn
    }

    /// Seed `messaging_channels` with one channel so the subscription FKs resolve.
    fn seed_channel(conn: &Connection, uuid: Uuid, address: &str) {
        let entry = ChannelEntry {
            uuid,
            address: address.to_string(),
            description: None,
            transport_type: ChannelScheme::Brenn,
            resolved_channel: ResolvedChannel {
                send_rate: Default::default(),
                push_depth: Depth::Unbounded,
                retain_depth: Depth::Unbounded,
                standing_retain_depth: Depth::Unbounded,
                noise: NoiseLevel::Silent,
                sink: Sink::Drop,
                wake_min: WakeMin::Normal,
            },
            subscribers: Vec::new(),
            mount: None,
        };
        upsert_channels(conn, std::slice::from_ref(&entry));
    }

    /// Pruning a dropped `(channel, app)` key removes exactly that durable
    /// dynamic-subscription row, leaving others intact.
    #[test]
    fn prune_removes_only_named_dropped_rows() {
        let conn = test_conn();
        let uuid = Uuid::new_v4();
        seed_channel(&conn, uuid, "heartbeat");
        for app in ["graf", "pfin"] {
            conn.execute(
                "INSERT INTO messaging_dynamic_subscriptions \
                 (channel_uuid, app_slug, push_depth, retain_depth, noise, wake_min, qos, created_at) \
                 VALUES (?1, ?2, '0', '1', 'silent', 'normal', NULL, '2026-06-20T00:00:00Z')",
                rusqlite::params![uuid.as_bytes().to_vec(), app],
            )
            .expect("seed dynamic row");
        }

        prune_dropped_dynamic_subscriptions(&conn, &[(uuid, "graf".to_string())]);

        let remaining: Vec<String> = {
            let mut stmt = conn
                .prepare("SELECT app_slug FROM messaging_dynamic_subscriptions ORDER BY app_slug")
                .expect("prepare");
            let rows = stmt
                .query_map([], |r| r.get::<_, String>(0))
                .expect("query");
            rows.map(|r| r.expect("row")).collect()
        };
        assert_eq!(remaining, vec!["pfin".to_string()], "only graf pruned");
    }

    // --- load_channels_by_uuids ---

    /// Seed `messaging_channels` with one channel of a given transport so the
    /// reconstruction read has a row to decode.
    fn seed_channel_typed(
        conn: &Connection,
        uuid: Uuid,
        address: &str,
        description: Option<&str>,
        transport: ChannelScheme,
    ) {
        let entry = ChannelEntry {
            uuid,
            address: address.to_string(),
            description: description.map(str::to_string),
            transport_type: transport,
            resolved_channel: ResolvedChannel {
                send_rate: Default::default(),
                push_depth: Depth::Unbounded,
                retain_depth: Depth::Unbounded,
                standing_retain_depth: Depth::Unbounded,
                noise: NoiseLevel::Silent,
                sink: Sink::Drop,
                wake_min: WakeMin::Normal,
            },
            subscribers: Vec::new(),
            mount: None,
        };
        upsert_channels(conn, std::slice::from_ref(&entry));
    }

    /// A written channel round-trips (uuid/address/description/transport_type)
    /// when its UUID is requested, with a `resolved_channel` stamped from the
    /// passed global defaults, empty subscribers, and `mount = None`.
    #[test]
    fn load_channels_by_uuids_round_trips_requested_channel() {
        let conn = test_conn();
        let uuid = Uuid::new_v4();
        seed_channel_typed(
            &conn,
            uuid,
            "mqtt:home:sensors/temp",
            Some("temp sensor"),
            ChannelScheme::Mqtt,
        );
        let defaults = MessagingGlobalConfig {
            default_push_depth: Depth::Bounded(7),
            default_noise: NoiseLevel::Metered,
            default_wake_min: WakeMin::High,
            ..MessagingGlobalConfig::default()
        };

        let loaded = load_channels_by_uuids(&conn, &[uuid], &defaults);

        assert_eq!(loaded.len(), 1);
        let ch = &loaded[0];
        assert_eq!(ch.uuid, uuid);
        assert_eq!(ch.address, "mqtt:home:sensors/temp");
        assert_eq!(ch.description.as_deref(), Some("temp sensor"));
        assert_eq!(ch.transport_type, ChannelScheme::Mqtt);
        assert!(ch.subscribers.is_empty(), "reconstructed channel is inert");
        assert!(ch.mount.is_none());
        // resolved_channel stamped from the supplied global defaults.
        assert_eq!(ch.resolved_channel.push_depth, Depth::Bounded(7));
        assert_eq!(ch.resolved_channel.noise, NoiseLevel::Metered);
        assert_eq!(ch.resolved_channel.wake_min, WakeMin::High);
    }

    /// A UUID not in the request set is not returned — the scoped load reads only
    /// the requested channels, never the whole table (the orphan-exclusion
    /// invariant: an unreferenced channel is never reconstructed). A requested but
    /// absent UUID likewise yields no entry (genuine config-drift case).
    #[test]
    fn load_channels_by_uuids_returns_only_requested() {
        let conn = test_conn();
        let requested = Uuid::new_v4();
        let other = Uuid::new_v4();
        let absent = Uuid::new_v4();
        seed_channel_typed(&conn, requested, "heartbeat", None, ChannelScheme::Brenn);
        // `other` exists in the table but is NOT requested (e.g. an orphan).
        seed_channel_typed(&conn, other, "webhook:hook", None, ChannelScheme::Webhook);

        let loaded = load_channels_by_uuids(
            &conn,
            &[requested, absent],
            &MessagingGlobalConfig::default(),
        );

        assert_eq!(loaded.len(), 1, "only the present, requested channel loads");
        assert_eq!(loaded[0].uuid, requested);
        assert!(
            loaded.iter().all(|c| c.uuid != other),
            "an unrequested channel is never materialized (orphan exclusion)"
        );
    }

    /// A corrupt `transport_type` is host-written corruption → panic (BETTER
    /// DEAD THAN WRONG; this boot read runs on host startup state, not inbound
    /// traffic).
    #[test]
    #[should_panic(expected = "malformed channel transport_type")]
    fn load_channels_by_uuids_panics_on_corrupt_transport_type() {
        let conn = test_conn();
        let uuid = Uuid::new_v4();
        conn.execute(
            "INSERT INTO messaging_channels \
             (uuid, address, description, transport_type, created_at, resume_epoch) \
             VALUES (?1, 'heartbeat', NULL, 'garbage', '2026-06-20T00:00:00Z', \
                     X'00000000000000000000000000000001')",
            rusqlite::params![uuid.as_bytes().to_vec()],
        )
        .expect("seed corrupt channel row");

        let _ = load_channels_by_uuids(&conn, &[uuid], &MessagingGlobalConfig::default());
    }

    /// A channel row with `transport_type = 'ingress'` panics at directory load:
    /// `ingress` is a storage-only row-kind, never an address scheme, so a
    /// channel carrying it is host-state corruption (ingress rows are
    /// channel-less). Stricter than tolerating it and mislabeling later.
    #[test]
    #[should_panic(expected = "malformed channel transport_type")]
    fn load_channels_by_uuids_panics_on_ingress_transport_type() {
        let conn = test_conn();
        let uuid = Uuid::new_v4();
        conn.execute(
            "INSERT INTO messaging_channels \
             (uuid, address, description, transport_type, created_at, resume_epoch) \
             VALUES (?1, 'heartbeat', NULL, 'ingress', '2026-06-20T00:00:00Z', \
                     X'00000000000000000000000000000001')",
            rusqlite::params![uuid.as_bytes().to_vec()],
        )
        .expect("seed ingress channel row");

        let _ = load_channels_by_uuids(&conn, &[uuid], &MessagingGlobalConfig::default());
    }
}
