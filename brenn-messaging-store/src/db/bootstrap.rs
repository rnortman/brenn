use rusqlite::{Connection, OptionalExtension};

use brenn_db::format_ts_for_db;
use chrono::Utc;

use brenn_lib::messaging::{
    ChannelEntry, ChannelScheme,
    config::{
        Depth, MessagingGlobalConfig, NoiseLevel, SystemChannelFamily, SystemChannelTuning,
        resolve_system_channel,
    },
};
use uuid::Uuid;

/// Upsert all configured channels into `messaging_channels`. UUIDs not
/// present in config are kept (so renamed channels keep their history);
/// operators delete obsolete channels manually if desired.
pub fn upsert_channels(conn: &Connection, entries: &[ChannelEntry]) {
    let now = format_ts_for_db(Utc::now());
    for entry in entries {
        let uuid_bytes = entry.uuid.as_bytes().to_vec();
        // INSERT OR IGNORE keyed by uuid (the PK), then UPDATE the mutable
        // columns for the row that already existed. The UPDATE never touches
        // resume_epoch, so an existing channel keeps the epoch it was born with
        // (the epoch dies only with the row).
        let transport_type_str = entry.transport_type.as_str();
        let inserted = conn
            .execute(
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
        let updated = conn
            .execute(
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
        // `INSERT OR IGNORE` skips a row violating *any* constraint, the UNIQUE
        // on `address` as readily as the PK on `uuid`, and the UPDATE then
        // matches nothing. Untouched, that leaves a channel the directory holds
        // with no row of its own, and the failure surfaces as a foreign-key
        // error on its first publish. Refuse the config here instead.
        assert!(
            inserted > 0 || updated > 0,
            "messaging: channel {:?} could not be written under uuid {} — its address \
             already belongs to another channel row. Delete that row to reuse the address.",
            entry.address,
            entry.uuid,
        );
    }
}

/// What one [`load_channels_by_uuids`] pass found: the channels it reconstructed
/// and the extant rows it declined to reconstruct.
///
/// The two halves answer different questions for the caller's merge. `entries`
/// are channels the directory can now resolve, so their durable dynamic
/// subscriptions fold normally. `skipped` names the requested rows this loader
/// declined to reconstruct — every non-system address, whether or not a
/// `[[channel]]` block still declares it, since a declared channel is already in
/// the directory and reconstructing it would only compete. Membership therefore
/// means "this loader could not build it", not "nothing can size it": the merge
/// asks the directory first and consults this report only when the directory has
/// no answer, and then classifies the row dormant rather than as drift, so a
/// removed or commented-out block does not destroy durable user state.
#[derive(Debug, Default, Clone)]
pub struct ChannelReconstruction {
    /// Reconstructed channels, ready to fold into the boot directory.
    pub entries: Vec<ChannelEntry>,
    /// `(uuid, address)` for each requested row present in `messaging_channels`
    /// but not reconstructible. A requested UUID with no row at all appears in
    /// neither half.
    pub skipped: Vec<(Uuid, String)>,
}

/// Load the channels for the given UUIDs from `messaging_channels`, decoded into
/// [`ChannelEntry`] values, alongside a report of the rows that were present but
/// not reconstructible.
///
/// **Scoped, not full-table.** The caller passes exactly the distinct
/// `channel_uuid`s referenced by the surviving durable dynamic-subscription rows,
/// so this reconstructs only those channels — never every row in
/// `messaging_channels`. Orphan channels (runtime-created channels whose only/last
/// dynamic subscription was torn down, deleting its durable row) are by
/// construction not referenced by any surviving row and so are never requested,
/// never materialized, and add no per-orphan runtime memory.
///
/// Each returned entry has empty `subscribers` and `mount = None` (a reconstructed
/// channel is inert until the merge attaches subscribers), and a `resolved_channel`
/// from [`resolve_system_channel`] keyed on the row's own address — every site
/// that creates or reconstructs a system-minted channel calls this same function,
/// so a reconstructed channel resolves identically to one minted at runtime.
///
/// `transport_type` decodes via [`ChannelScheme::parse`]; an unparseable value is
/// host-written corruption and panics (CLAUDE.md BETTER DEAD THAN WRONG, consistent with
/// the `db/dynamic.rs` decoders — this read runs on host-written startup state,
/// not attacker-influenceable inbound traffic). A requested UUID that is not
/// present in `messaging_channels` yields neither an entry nor a skip report
/// entry; the caller's merge then classifies its durable row as genuine config
/// drift (`dropped`).
pub fn load_channels_by_uuids(
    conn: &Connection,
    uuids: &[Uuid],
    tuning: &SystemChannelTuning,
    defaults: &MessagingGlobalConfig,
) -> ChannelReconstruction {
    let mut loaded = ChannelReconstruction::default();
    let mut stmt = conn
        .prepare(
            "SELECT uuid, address, description, transport_type \
             FROM messaging_channels WHERE uuid = ?1",
        )
        .expect("messaging: prepare load_channels_by_uuids");
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
            // Only system-minted channels are reconstructible from a row alone,
            // because only they have a family default to resolve against. An
            // operator-declared address gets its depths from its `[[channel]]`
            // block or not at all: if the block is still there the directory
            // already holds the channel and this entry would be discarded, and
            // if it is gone there is no source of truth to invent one from. A
            // conversation's chat channels are the third case — runtime-created
            // but sized by chat provisioning, which registers them into the
            // directory before the merge asks about their rows. Either way the
            // row itself is still here, so it goes in the skip report: the
            // channel exists and is merely undeclared, which the merge treats as
            // dormant rather than as drift.
            if SystemChannelFamily::of(&address).is_none() {
                loaded.skipped.push((*uuid, address));
                continue;
            }
            let resolved_channel = resolve_system_channel(&address, tuning, defaults);
            loaded.entries.push(ChannelEntry {
                uuid: *uuid,
                address,
                description,
                resolved_channel,
                subscribers: Vec::new(),
                transport_type,
                mount: None,
            });
        }
    }
    loaded
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
/// channel's row is gone from `messaging_channels`, or a static sub now
/// overrides them) from `messaging_dynamic_subscriptions`, keyed by
/// `(channel_uuid, app_slug)`.
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
    use crate::db::run_messaging_migrations;
    use brenn_lib::messaging::config::{
        INGRESS_DEFAULT_RETAIN_DEPTH, MessagingGlobalConfig, ResolvedChannel,
        SYSTEM_CHANNEL_DEFAULT_PUSH_DEPTH, Sink,
    };
    use brenn_lib::messaging::{ChannelScheme, WakeMin};

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

    /// A second uuid claiming an address a row already holds is refused. Nothing
    /// in SQL objects on its own: `INSERT OR IGNORE` skips the row over the
    /// UNIQUE address as readily as over a duplicate uuid, and the UPDATE then
    /// matches no row — leaving a channel the directory holds with no row of its
    /// own, and a foreign-key failure on its first publish.
    #[test]
    #[should_panic(expected = "already belongs to another channel row")]
    fn upsert_channels_refuses_a_new_uuid_for_a_taken_address() {
        let conn = test_conn();
        seed_channel(&conn, Uuid::new_v4(), "reused");
        seed_channel(&conn, Uuid::new_v4(), "reused");
    }

    /// The same address under the same uuid is the ordinary re-boot: the insert
    /// is ignored and the update rewrites the mutable columns.
    #[test]
    fn upsert_channels_is_idempotent_under_one_uuid() {
        let conn = test_conn();
        let uuid = Uuid::new_v4();
        seed_channel(&conn, uuid, "steady");
        seed_channel(&conn, uuid, "steady");
        let rows: i64 = conn
            .query_row("SELECT COUNT(*) FROM messaging_channels", [], |r| r.get(0))
            .expect("count channels");
        assert_eq!(rows, 1, "one uuid, one row");
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
    /// when its UUID is requested, with the `resolved_channel` its address
    /// resolves to — the ingress family default over the passed global
    /// defaults — empty subscribers, and `mount = None`.
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
            default_noise: NoiseLevel::Metered,
            default_wake_min: WakeMin::High,
            ..MessagingGlobalConfig::default()
        };

        let loaded =
            load_channels_by_uuids(&conn, &[uuid], &SystemChannelTuning::default(), &defaults);

        assert_eq!(loaded.entries.len(), 1);
        assert!(loaded.skipped.is_empty(), "a minted family reconstructs");
        let ch = &loaded.entries[0];
        assert_eq!(ch.uuid, uuid);
        assert_eq!(ch.address, "mqtt:home:sensors/temp");
        assert_eq!(ch.description.as_deref(), Some("temp sensor"));
        assert_eq!(ch.transport_type, ChannelScheme::Mqtt);
        assert!(ch.subscribers.is_empty(), "reconstructed channel is inert");
        assert!(ch.mount.is_none());
        // Depths are the ingress family's bounded default; the rest follows
        // the globals.
        assert_eq!(
            ch.resolved_channel.push_depth,
            SYSTEM_CHANNEL_DEFAULT_PUSH_DEPTH
        );
        assert_eq!(
            ch.resolved_channel.retain_depth,
            INGRESS_DEFAULT_RETAIN_DEPTH
        );
        assert_eq!(
            ch.resolved_channel.standing_retain_depth,
            INGRESS_DEFAULT_RETAIN_DEPTH
        );
        assert_eq!(ch.resolved_channel.noise, NoiseLevel::Metered);
        assert_eq!(ch.resolved_channel.wake_min, WakeMin::High);
    }

    /// A reconstructed row resolves through the same pure function of (address,
    /// config) the runtime minter calls, so a tuning block reaches the
    /// DB-reconstructed twin exactly as it reaches the runtime-minted channel.
    /// The row carries no depths of its own; the config is re-read each boot.
    #[test]
    fn a_reconstructed_row_resolves_identically_to_the_runtime_minted_channel() {
        use brenn_lib::messaging::config::{
            ChannelConfigRaw, build_system_channel_tuning, resolve_system_channel,
        };

        let conn = test_conn();
        let uuid = Uuid::new_v4();
        let address = "mqtt:home:sensors/temp";
        seed_channel_typed(&conn, uuid, address, None, ChannelScheme::Mqtt);
        let defaults = MessagingGlobalConfig::default();
        let tuning = build_system_channel_tuning(
            &[ChannelConfigRaw {
                send_rate: None,
                uuid: None,
                address: None,
                address_prefix: Some("mqtt:home:".to_string()),
                description: None,
                push_depth: Some(Depth::Bounded(3)),
                retain_depth: Some(Depth::Bounded(42)),
                standing_retain_depth: Some(Depth::Bounded(42)),
                noise: None,
                sink: None,
                wake_min: None,
            }],
            &defaults,
        );

        let loaded = load_channels_by_uuids(&conn, &[uuid], &tuning, &defaults);

        assert_eq!(loaded.entries.len(), 1);
        let entry = &loaded.entries[0];
        assert_eq!(
            entry.resolved_channel.retain_depth,
            Depth::Bounded(42),
            "the prefix tuning block reaches the reconstructed row",
        );
        let minted = resolve_system_channel(address, &tuning, &defaults);
        assert_eq!(entry.resolved_channel.push_depth, minted.push_depth);
        assert_eq!(entry.resolved_channel.retain_depth, minted.retain_depth);
        assert_eq!(
            entry.resolved_channel.standing_retain_depth,
            minted.standing_retain_depth
        );
    }

    /// An operator-declared address in the table is not reconstructible: its
    /// depths live in its `[[channel]]` block, so a row for a block that is gone
    /// yields no entry. It comes back in the skip report instead — the channel
    /// exists, it is merely undeclared, and the merge holds its subscriptions
    /// dormant rather than pruning them.
    #[test]
    fn an_operator_declared_row_is_not_reconstructed() {
        let conn = test_conn();
        let uuid = Uuid::new_v4();
        seed_channel_typed(&conn, uuid, "heartbeat", None, ChannelScheme::Brenn);

        let loaded = load_channels_by_uuids(
            &conn,
            &[uuid],
            &SystemChannelTuning::default(),
            &MessagingGlobalConfig::default(),
        );

        assert!(loaded.entries.is_empty());
        assert_eq!(loaded.skipped, vec![(uuid, "heartbeat".to_string())]);
    }

    /// A conversation's chat leaf is the other skip-report class: runtime-created
    /// under `brenn:`, sized by chat provisioning rather than by any block or
    /// family default. The report keys on reconstructibility, not on family.
    #[test]
    fn a_chat_leaf_row_is_reported_as_unreconstructible() {
        let conn = test_conn();
        let uuid = Uuid::new_v4();
        let address = "chat/host/out/7";
        seed_channel_typed(&conn, uuid, address, None, ChannelScheme::Brenn);

        let loaded = load_channels_by_uuids(
            &conn,
            &[uuid],
            &SystemChannelTuning::default(),
            &MessagingGlobalConfig::default(),
        );

        assert!(loaded.entries.is_empty());
        assert_eq!(loaded.skipped, vec![(uuid, address.to_string())]);
    }

    /// A UUID not in the request set is not returned — the scoped load reads only
    /// the requested channels, never the whole table (the orphan-exclusion
    /// invariant: an unreferenced channel is never reconstructed). A requested but
    /// absent UUID yields no entry and no skip-report line either, which is what
    /// makes it the genuine config-drift case.
    #[test]
    fn load_channels_by_uuids_returns_only_requested() {
        let conn = test_conn();
        let requested = Uuid::new_v4();
        let other = Uuid::new_v4();
        let absent = Uuid::new_v4();
        seed_channel_typed(
            &conn,
            requested,
            "webhook:requested",
            None,
            ChannelScheme::Webhook,
        );
        // `other` exists in the table but is NOT requested (e.g. an orphan).
        seed_channel_typed(&conn, other, "webhook:hook", None, ChannelScheme::Webhook);

        let loaded = load_channels_by_uuids(
            &conn,
            &[requested, absent],
            &SystemChannelTuning::default(),
            &MessagingGlobalConfig::default(),
        );

        assert_eq!(
            loaded.entries.len(),
            1,
            "only the present, requested channel loads"
        );
        assert_eq!(loaded.entries[0].uuid, requested);
        assert!(
            loaded.entries.iter().all(|c| c.uuid != other),
            "an unrequested channel is never materialized (orphan exclusion)"
        );
        assert!(
            loaded.skipped.is_empty(),
            "an absent uuid is drift, not an undeclared channel"
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

        let _ = load_channels_by_uuids(
            &conn,
            &[uuid],
            &SystemChannelTuning::default(),
            &MessagingGlobalConfig::default(),
        );
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

        let _ = load_channels_by_uuids(
            &conn,
            &[uuid],
            &SystemChannelTuning::default(),
            &MessagingGlobalConfig::default(),
        );
    }
}
