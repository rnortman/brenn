//! Read side of the durable dynamic-subscription store.
//!
//! `messaging_dynamic_subscriptions` is the durable truth for runtime-created
//! subscriptions, and is NOT truncated and rebuilt at boot the way the
//! config-derived static registrations are. This module loads its rows into
//! typed Rust values and carries the runtime subscribe/unsubscribe writes; the
//! boot merge folds the rows into the directory.

use rusqlite::{Connection, OptionalExtension};
use uuid::Uuid;

use super::super::WakeMin;
use super::super::config::{Depth, NoiseLevel};
use super::bootstrap::{depth_to_sql, noise_to_sql};

/// One row of `messaging_dynamic_subscriptions`, decoded into typed values:
/// the resolved subscription parameters (`channel_uuid`/`app_slug`/`push_depth`/
/// `retain_depth`/`noise`/`wake_min`) plus the MQTT-only `qos` (`None` for
/// `brenn:`/`webhook:`) and the `created_at` timestamp.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DynamicSubscriptionRow {
    pub channel_uuid: Uuid,
    pub app_slug: String,
    pub push_depth: Depth,
    pub retain_depth: Depth,
    pub noise: NoiseLevel,
    pub wake_min: WakeMin,
    /// MQTT SUBSCRIBE QoS (0/1/2). `None` for non-MQTT transports.
    pub qos: Option<u8>,
    /// RFC3339 creation timestamp (DB wire form), as stored.
    pub created_at: String,
}

/// Decode a `Depth` from its SQL wire form (the inverse of `depth_to_sql` in
/// `bootstrap.rs`): `"unbounded"` → `Unbounded`, otherwise a non-negative
/// integer → `Bounded(n)`. The value is one we wrote ourselves; a malformed
/// value is a host bug (corrupt DB / writer bug), so panic (CLAUDE.md BETTER
/// DEAD THAN WRONG).
pub(super) fn depth_from_sql(s: &str) -> Depth {
    if s == "unbounded" {
        Depth::Unbounded
    } else {
        let n: u64 = s
            .parse()
            .unwrap_or_else(|e| panic!("messaging: malformed depth {s:?} in DB: {e}"));
        Depth::Bounded(n)
    }
}

/// Decode a `NoiseLevel` from its SQL wire form (the inverse of `noise_to_sql`
/// in `bootstrap.rs`). A malformed value is a host bug → panic.
fn noise_from_sql(s: &str) -> NoiseLevel {
    match s {
        "silent" => NoiseLevel::Silent,
        "metered" => NoiseLevel::Metered,
        "alarm" => NoiseLevel::Alarm,
        other => panic!("messaging: malformed dynamic-subscription noise {other:?} in DB"),
    }
}

/// Load all rows from `messaging_dynamic_subscriptions`, decoded into
/// [`DynamicSubscriptionRow`] values.
///
/// Used by the boot merge to fold durable dynamic subs back into the directory.
/// Returns rows in `app_slug, channel_uuid` order for determinism. Any decode
/// failure on a value we wrote ourselves is a host bug and panics.
pub fn load_dynamic_subscriptions(conn: &Connection) -> Vec<DynamicSubscriptionRow> {
    let mut stmt = conn
        .prepare(
            "SELECT channel_uuid, app_slug, push_depth, retain_depth, noise, wake_min, \
                    qos, created_at \
             FROM messaging_dynamic_subscriptions \
             ORDER BY app_slug, channel_uuid",
        )
        .expect("messaging: prepare load_dynamic_subscriptions");
    let rows = stmt
        .query_map([], |row| {
            let uuid_bytes: Vec<u8> = row.get(0)?;
            let app_slug: String = row.get(1)?;
            let push_depth_s: String = row.get(2)?;
            let retain_depth_s: String = row.get(3)?;
            let noise_s: String = row.get(4)?;
            let wake_min_s: String = row.get(5)?;
            let qos: Option<u8> = row.get(6)?;
            let created_at: String = row.get(7)?;
            Ok(DynamicSubscriptionRow {
                channel_uuid: Uuid::from_slice(&uuid_bytes).unwrap_or_else(|e| {
                    panic!(
                        "messaging: malformed dynamic-subscription channel_uuid \
                         ({} bytes) in DB: {e}",
                        uuid_bytes.len()
                    )
                }),
                app_slug,
                push_depth: depth_from_sql(&push_depth_s),
                retain_depth: depth_from_sql(&retain_depth_s),
                noise: noise_from_sql(&noise_s),
                wake_min: WakeMin::parse(&wake_min_s).unwrap_or_else(|| {
                    panic!(
                        "messaging: malformed dynamic-subscription wake_min {wake_min_s:?} in DB"
                    )
                }),
                qos,
                created_at,
            })
        })
        .expect("messaging: query load_dynamic_subscriptions");
    rows.map(|r| r.expect("messaging: read dynamic-subscription row"))
        .collect()
}

/// Point-lookup a single dynamic-subscription row by its `(channel_uuid, app_slug)`
/// primary key, decoded into a [`DynamicSubscriptionRow`].
///
/// Hits the PK index directly (`WHERE channel_uuid = ?1 AND app_slug = ?2`), so the
/// cost is O(1) regardless of the total dynamic-subscription population — unlike
/// [`load_dynamic_subscriptions`], which scans + decodes the whole table. Used by
/// the runtime re-subscribe check (`subscribe_dynamic`), reached on every
/// idempotent re-subscribe and "differs" rejection, so it must not pay a full-table
/// cost under the shared DB lock. Returns `None` if no row exists for that key. Any
/// decode failure on a value we wrote ourselves is a host bug and panics (same as
/// [`load_dynamic_subscriptions`]).
pub fn load_dynamic_subscription_for(
    conn: &Connection,
    channel_uuid: Uuid,
    app_slug: &str,
) -> Option<DynamicSubscriptionRow> {
    conn.query_row(
        "SELECT channel_uuid, app_slug, push_depth, retain_depth, noise, wake_min, \
                qos, created_at \
         FROM messaging_dynamic_subscriptions \
         WHERE channel_uuid = ?1 AND app_slug = ?2",
        rusqlite::params![channel_uuid.as_bytes().to_vec(), app_slug],
        |row| {
            let uuid_bytes: Vec<u8> = row.get(0)?;
            let app_slug: String = row.get(1)?;
            let push_depth_s: String = row.get(2)?;
            let retain_depth_s: String = row.get(3)?;
            let noise_s: String = row.get(4)?;
            let wake_min_s: String = row.get(5)?;
            let qos: Option<u8> = row.get(6)?;
            let created_at: String = row.get(7)?;
            Ok(DynamicSubscriptionRow {
                channel_uuid: Uuid::from_slice(&uuid_bytes).unwrap_or_else(|e| {
                    panic!(
                        "messaging: malformed dynamic-subscription channel_uuid \
                         ({} bytes) in DB: {e}",
                        uuid_bytes.len()
                    )
                }),
                app_slug,
                push_depth: depth_from_sql(&push_depth_s),
                retain_depth: depth_from_sql(&retain_depth_s),
                noise: noise_from_sql(&noise_s),
                wake_min: WakeMin::parse(&wake_min_s).unwrap_or_else(|| {
                    panic!(
                        "messaging: malformed dynamic-subscription wake_min {wake_min_s:?} in DB"
                    )
                }),
                qos,
                created_at,
            })
        },
    )
    .optional()
    .expect("messaging: query load_dynamic_subscription_for")
}

/// Persist a newly-created (runtime) dynamic subscription: write the durable row
/// into `messaging_dynamic_subscriptions`.
///
/// The caller guarantees the `(channel_uuid, app_slug)` PK does not pre-exist.
/// `subscribe_dynamic` re-establishes that guarantee before reaching here: it
/// returns early on an existing directory subscriber (re-subscribe is
/// identity-only) and probes the durable table for a *dormant* (unfolded)
/// dynamic row — rejecting with `DormantSubscriptionExists` rather than colliding.
/// So this is a plain `INSERT` — a PK collision here is a caller/host bug and
/// panics.
///
/// The probe and this INSERT are separate `db` lock acquisitions, so the
/// guarantee rests on `subscribe_dynamic` holding its dynamic-subscribe gate
/// across both: intercepts serialize per *conversation*, not per app, so an app
/// with several live conversations can issue two same-key subscribes at once and
/// without that gate both would pass the probe and the loser would panic here on
/// attacker-shaped input.
pub fn insert_dynamic_subscription(conn: &Connection, row: &DynamicSubscriptionRow) {
    conn.execute(
        "INSERT INTO messaging_dynamic_subscriptions \
         (channel_uuid, app_slug, push_depth, retain_depth, noise, wake_min, qos, created_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        rusqlite::params![
            row.channel_uuid.as_bytes().to_vec(),
            &row.app_slug,
            depth_to_sql(row.push_depth),
            depth_to_sql(row.retain_depth),
            noise_to_sql(row.noise),
            row.wake_min.as_str(),
            row.qos,
            &row.created_at,
        ],
    )
    .expect("messaging: insert durable dynamic subscription");
}

/// Remove a dynamic subscription at runtime: delete the durable
/// `messaging_dynamic_subscriptions` row keyed by `(channel_uuid, app_slug)` —
/// the inverse of [`insert_dynamic_subscription`].
///
/// Returns `true` if a durable dynamic row was removed, `false` if none existed
/// for that `(channel, app)` — the caller (the unsubscribe tool) turns `false`
/// into a tool error ("no dynamic subscription to remove"). A static TOML sub
/// has no durable dynamic row and is unreachable here, so this can never remove
/// a static subscription.
pub fn delete_dynamic_subscription(conn: &Connection, channel_uuid: Uuid, app_slug: &str) -> bool {
    conn.execute(
        "DELETE FROM messaging_dynamic_subscriptions \
         WHERE channel_uuid = ?1 AND app_slug = ?2",
        rusqlite::params![channel_uuid.as_bytes().to_vec(), app_slug],
    )
    .expect("messaging: delete durable dynamic subscription")
        > 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messaging::config::{Depth, NoiseLevel, ResolvedChannel, Sink};
    use crate::messaging::db::run_messaging_migrations;
    use crate::messaging::db::upsert_channels;
    use crate::messaging::{ChannelEntry, ChannelScheme};

    fn test_conn() -> Connection {
        let conn = Connection::open_in_memory().expect("open in-memory db");
        // messaging_dynamic_subscriptions has an FK to messaging_channels, and
        // conversations/messaging_send_budget reference it too — run the full
        // messaging migration so the FK targets exist.
        conn.execute_batch("CREATE TABLE conversations (id INTEGER PRIMARY KEY);")
            .expect("create conversations stub");
        run_messaging_migrations(&conn);
        conn
    }

    fn seed_channel(conn: &Connection, uuid: Uuid, address: &str, envelope: ChannelScheme) {
        let entry = ChannelEntry {
            uuid,
            address: address.to_string(),
            description: None,
            transport_type: envelope,
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

    /// `params` is `[push_depth, retain_depth, noise, wake_min]` in SQL wire form
    /// (bundled into one slice so the helper stays under clippy's arg limit).
    fn insert_dyn_row(
        conn: &Connection,
        uuid: Uuid,
        app: &str,
        params: [&str; 4],
        qos: Option<u8>,
    ) {
        let [push, retain, noise, wake] = params;
        conn.execute(
            "INSERT INTO messaging_dynamic_subscriptions \
             (channel_uuid, app_slug, push_depth, retain_depth, noise, wake_min, qos, created_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            rusqlite::params![
                uuid.as_bytes().to_vec(),
                app,
                push,
                retain,
                noise,
                wake,
                qos,
                "2026-06-20T00:00:00Z",
            ],
        )
        .expect("insert dynamic-subscription row");
    }

    #[test]
    fn empty_table_loads_no_rows() {
        let conn = test_conn();
        assert!(load_dynamic_subscriptions(&conn).is_empty());
    }

    #[test]
    fn loads_and_decodes_mqtt_and_brenn_rows() {
        let conn = test_conn();
        let mqtt_uuid = Uuid::new_v4();
        let brenn_uuid = Uuid::new_v4();
        seed_channel(
            &conn,
            mqtt_uuid,
            "mqtt:home:sensors/temp",
            ChannelScheme::Mqtt,
        );
        seed_channel(&conn, brenn_uuid, "heartbeat", ChannelScheme::Brenn);
        // mqtt: pull-only with qos; brenn: push-enabled, unbounded, no qos.
        insert_dyn_row(
            &conn,
            mqtt_uuid,
            "graf",
            ["0", "5", "silent", "normal"],
            Some(1),
        );
        insert_dyn_row(
            &conn,
            brenn_uuid,
            "pfin",
            ["unbounded", "unbounded", "alarm", "high"],
            None,
        );

        let rows = load_dynamic_subscriptions(&conn);
        assert_eq!(rows.len(), 2);
        // ORDER BY app_slug: "graf" before "pfin".
        let graf = &rows[0];
        assert_eq!(graf.app_slug, "graf");
        assert_eq!(graf.channel_uuid, mqtt_uuid);
        assert_eq!(graf.push_depth, Depth::Bounded(0));
        assert_eq!(graf.retain_depth, Depth::Bounded(5));
        assert_eq!(graf.noise, NoiseLevel::Silent);
        assert_eq!(graf.wake_min, WakeMin::Normal);
        assert_eq!(graf.qos, Some(1));

        let pfin = &rows[1];
        assert_eq!(pfin.app_slug, "pfin");
        assert_eq!(pfin.channel_uuid, brenn_uuid);
        assert_eq!(pfin.push_depth, Depth::Unbounded);
        assert_eq!(pfin.retain_depth, Depth::Unbounded);
        assert_eq!(pfin.noise, NoiseLevel::Alarm);
        assert_eq!(pfin.wake_min, WakeMin::High);
        assert_eq!(pfin.qos, None);
    }

    #[test]
    #[should_panic(expected = "malformed depth")]
    fn malformed_depth_panics() {
        let conn = test_conn();
        let uuid = Uuid::new_v4();
        seed_channel(&conn, uuid, "heartbeat", ChannelScheme::Brenn);
        insert_dyn_row(
            &conn,
            uuid,
            "graf",
            ["garbage", "5", "silent", "normal"],
            None,
        );
        let _ = load_dynamic_subscriptions(&conn);
    }

    fn row(
        uuid: Uuid,
        app: &str,
        push: Depth,
        retain: Depth,
        qos: Option<u8>,
    ) -> DynamicSubscriptionRow {
        DynamicSubscriptionRow {
            channel_uuid: uuid,
            app_slug: app.to_string(),
            push_depth: push,
            retain_depth: retain,
            noise: NoiseLevel::Metered,
            wake_min: WakeMin::High,
            qos,
            created_at: "2026-06-20T00:00:00Z".to_string(),
        }
    }

    /// A runtime insert writes the durable dynamic-table row, MQTT `qos` and
    /// all, and it decodes back as written.
    #[test]
    fn insert_writes_the_durable_row() {
        let conn = test_conn();
        let uuid = Uuid::new_v4();
        seed_channel(&conn, uuid, "mqtt:home:sensors/temp", ChannelScheme::Mqtt);

        insert_dynamic_subscription(
            &conn,
            &row(uuid, "graf", Depth::Bounded(3), Depth::Bounded(5), Some(2)),
        );

        let durable = load_dynamic_subscriptions(&conn);
        assert_eq!(durable.len(), 1);
        assert_eq!(durable[0].app_slug, "graf");
        assert_eq!(durable[0].push_depth, Depth::Bounded(3));
        assert_eq!(durable[0].retain_depth, Depth::Bounded(5));
        assert_eq!(durable[0].qos, Some(2));
    }

    /// A runtime delete removes the durable row and reports `true`; deleting a
    /// non-existent `(channel, app)` reports `false` (the unsubscribe tool turns
    /// that into a tool error).
    #[test]
    fn delete_removes_the_row_and_reports_match() {
        let conn = test_conn();
        let uuid = Uuid::new_v4();
        seed_channel(&conn, uuid, "heartbeat", ChannelScheme::Brenn);
        insert_dynamic_subscription(
            &conn,
            &row(uuid, "pfin", Depth::Bounded(0), Depth::Bounded(1), None),
        );

        assert!(
            delete_dynamic_subscription(&conn, uuid, "pfin"),
            "removing the existing sub reports true"
        );
        assert!(
            load_dynamic_subscriptions(&conn).is_empty(),
            "durable row gone"
        );

        assert!(
            !delete_dynamic_subscription(&conn, uuid, "pfin"),
            "second delete (nothing to remove) reports false"
        );
    }

    /// Delete by `(channel, app)` PK leaves another app's subscriber on the same
    /// channel intact — the unsubscribe is scoped to the owning app's rows only.
    #[test]
    fn delete_leaves_other_apps_intact() {
        let conn = test_conn();
        let uuid = Uuid::new_v4();
        seed_channel(&conn, uuid, "heartbeat", ChannelScheme::Brenn);
        insert_dynamic_subscription(
            &conn,
            &row(uuid, "graf", Depth::Bounded(0), Depth::Bounded(1), None),
        );
        insert_dynamic_subscription(
            &conn,
            &row(uuid, "pfin", Depth::Bounded(0), Depth::Bounded(1), None),
        );

        assert!(delete_dynamic_subscription(&conn, uuid, "graf"));

        let remaining = load_dynamic_subscriptions(&conn);
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].app_slug, "pfin", "other app's sub survives");
    }
}
