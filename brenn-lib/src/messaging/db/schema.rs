//! Cluster 1: Schema DDL for the messaging subsystem.

use rusqlite::Connection;

/// Messaging migration entry point: create every messaging table in its current
/// shape.
///
/// Idempotent — pure `CREATE ... IF NOT EXISTS` DDL, safe to run on every boot
/// (it is, via `crate::db::run_migrations`). Creates, in FK-dependency order:
/// `messaging_channels`, `messaging_messages` (+ FTS + triggers),
/// `messaging_subscriptions`, `messaging_dynamic_subscriptions`,
/// `messaging_send_budget`, `messaging_pending_pushes`, and
/// `messaging_wasm_consume_failures`.
///
/// No structural migrations are currently registered; the marked slot below is
/// where future column/table migrations are added behind `column_exists` /
/// table-presence guards as the schema evolves.
///
/// After the DDL, [`assert_messaging_schema_current`] fails the boot fast if the
/// DB predates the urgency redesign (pre-2026-06); no migration path exists for
/// such DBs, so the guard aborts with an actionable message rather than letting a
/// raw SQLite error kill the dispatcher later. [`normalize_released_messages`]
/// then converges the one data shape an earlier release path could leave behind.
pub fn run_messaging_migrations(conn: &Connection) {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS messaging_channels (
            uuid            BLOB PRIMARY KEY,
            address         TEXT NOT NULL UNIQUE,
            description     TEXT,
            created_at      TEXT NOT NULL,
            transport_type  TEXT NOT NULL DEFAULT 'brenn',
            -- Durable resume domain: the epoch is minted with the row and dies
            -- only with it; last_retained_seq is the per-channel dense-sequence
            -- allocator and the persisted high-water.
            resume_epoch        BLOB NOT NULL,
            last_retained_seq   INTEGER NOT NULL DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_messaging_channels_address
            ON messaging_channels(address);

        CREATE TABLE IF NOT EXISTS messaging_messages (
            id                  INTEGER PRIMARY KEY,
            uuid                BLOB NOT NULL UNIQUE,
            channel_uuid        BLOB REFERENCES messaging_channels(uuid),
            source              TEXT NOT NULL,
            sender              TEXT NOT NULL,
            body                TEXT NOT NULL,
            urgency             TEXT NOT NULL CHECK(urgency IN ('very-low','low','normal','high')),
            reply_to_uuid       BLOB REFERENCES messaging_channels(uuid),
            delivery_deadline   TEXT,
            deliver_after       TEXT,
            publish_ts_ns       INTEGER NOT NULL,
            created_at          TEXT NOT NULL,
            envelope_type       TEXT NOT NULL DEFAULT 'brenn',
            ingress_source      TEXT,
            ingress_summary     TEXT,
            -- Dense per-channel retention-order sequence, assigned when the row
            -- enters retention (append commit, or release for a parked row);
            -- NULL while parked. Carries retention order, which the rowid cannot.
            retained_seq        INTEGER,
            -- Non-brenn envelope rows must never have deliver_after or delivery_deadline;
            -- those are bus-only dispatch fields. This constraint makes the
            -- invariant machine-enforced rather than convention-enforced.
            CHECK(envelope_type = 'brenn' OR (deliver_after IS NULL AND delivery_deadline IS NULL))
        );
        CREATE INDEX IF NOT EXISTS idx_messaging_messages_channel_ts
            ON messaging_messages(channel_uuid, publish_ts_ns DESC);
        CREATE INDEX IF NOT EXISTS idx_messaging_messages_deliver_after
            ON messaging_messages(deliver_after)
            WHERE deliver_after IS NOT NULL;

        CREATE VIRTUAL TABLE IF NOT EXISTS messaging_messages_fts USING fts5(
            body,
            content='messaging_messages',
            content_rowid='id'
        );
        CREATE TRIGGER IF NOT EXISTS messaging_messages_ai
            AFTER INSERT ON messaging_messages BEGIN
                INSERT INTO messaging_messages_fts(rowid, body) VALUES (new.id, new.body);
            END;
        CREATE TRIGGER IF NOT EXISTS messaging_messages_ad
            AFTER DELETE ON messaging_messages BEGIN
                INSERT INTO messaging_messages_fts(messaging_messages_fts, rowid, body)
                VALUES ('delete', old.id, old.body);
            END;
        CREATE TRIGGER IF NOT EXISTS messaging_messages_au
            AFTER UPDATE OF body ON messaging_messages BEGIN
                INSERT INTO messaging_messages_fts(messaging_messages_fts, rowid, body)
                VALUES ('delete', old.id, old.body);
                INSERT INTO messaging_messages_fts(rowid, body) VALUES (new.id, new.body);
            END;

        CREATE TABLE IF NOT EXISTS messaging_subscriptions (
            channel_uuid   BLOB NOT NULL REFERENCES messaging_channels(uuid),
            app_slug       TEXT NOT NULL,
            push_depth     TEXT NOT NULL,
            retain_depth   TEXT NOT NULL,
            noise          TEXT NOT NULL CHECK(noise IN ('silent','metered','alarm')),
            wake_min       TEXT NOT NULL CHECK(wake_min IN ('very-low','low','normal','high','never')),
            PRIMARY KEY (channel_uuid, app_slug)
        );
        CREATE INDEX IF NOT EXISTS idx_messaging_subscriptions_app
            ON messaging_subscriptions(app_slug);

        -- Durable dynamic (runtime-created) subscriptions (design §2.1).
        -- Structurally parallel to messaging_subscriptions, plus a transport-
        -- specific MQTT `qos` column (NULL for brenn:/webhook:) and a
        -- created_at timestamp. CRUCIALLY this table is NOT touched by
        -- rebuild_subscriptions and NOT truncated at boot: it is the durable
        -- truth for dynamic subs, with its own independent lifecycle. The boot
        -- merge folds these rows into the directory and mirrors them into
        -- messaging_subscriptions; runtime subscribe/unsubscribe writes here.
        CREATE TABLE IF NOT EXISTS messaging_dynamic_subscriptions (
            channel_uuid   BLOB NOT NULL REFERENCES messaging_channels(uuid),
            app_slug       TEXT NOT NULL,
            push_depth     TEXT NOT NULL,
            retain_depth   TEXT NOT NULL,
            noise          TEXT NOT NULL CHECK(noise IN ('silent','metered','alarm')),
            wake_min       TEXT NOT NULL CHECK(wake_min IN ('very-low','low','normal','high','never')),
            qos            INTEGER CHECK(qos IN (0,1,2)),
            created_at     TEXT NOT NULL,
            PRIMARY KEY (channel_uuid, app_slug)
        );
        CREATE INDEX IF NOT EXISTS idx_messaging_dynamic_subscriptions_app
            ON messaging_dynamic_subscriptions(app_slug);

        CREATE TABLE IF NOT EXISTS messaging_send_budget (
            conversation_id  INTEGER PRIMARY KEY REFERENCES conversations(id),
            remaining        INTEGER NOT NULL,
            last_reset_at    TEXT NOT NULL
        );
        ",
    )
    .expect("failed to run messaging supporting migrations");

    // messaging_pending_pushes must be created after messaging_messages above,
    // since it declares a REFERENCES messaging_messages(id) FK.
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS messaging_pending_pushes (
            id                INTEGER PRIMARY KEY,
            message_id        INTEGER NOT NULL REFERENCES messaging_messages(id),
            target_subscriber TEXT NOT NULL,
            target_app_slug   TEXT NOT NULL,
            eager_wake        INTEGER NOT NULL CHECK(eager_wake IN (0,1)),
            delivery_deadline TEXT,
            release_after     TEXT,
            delivered_at      TEXT,
            confirm_pending   INTEGER NOT NULL DEFAULT 0 CHECK(confirm_pending IN (0,1)),
            created_at        TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_messaging_pending_pushes_undelivered
            ON messaging_pending_pushes(target_subscriber)
            WHERE delivered_at IS NULL AND release_after IS NULL;
        CREATE INDEX IF NOT EXISTS idx_messaging_pending_pushes_deadline
            ON messaging_pending_pushes(delivery_deadline)
            WHERE delivered_at IS NULL AND delivery_deadline IS NOT NULL;
        CREATE INDEX IF NOT EXISTS idx_messaging_pending_pushes_release
            ON messaging_pending_pushes(release_after)
            WHERE delivered_at IS NULL AND release_after IS NOT NULL;
        CREATE INDEX IF NOT EXISTS idx_messaging_pending_pushes_message_id
            ON messaging_pending_pushes(message_id);
        ",
    )
    .expect("failed to run messaging pending_pushes DDL");

    // Structural migrations are added here behind column_exists / table-presence
    // guards as the schema evolves. The DDL above (and the wasm_consume_failures
    // DDL below) produces the current schema directly on a fresh or already-current
    // DB; a live DB from before a column was added is brought current here. The
    // schema-currency guard runs at the end of this function
    // (assert_messaging_schema_current).
    //
    // confirm_pending: the below-water ack channel's tentative-delivery stamp. A
    // pre-existing DB's pending_pushes table lacks it; add it with the same default
    // the CREATE TABLE declares so old rows read as not-tentative.
    if !crate::db::column_exists(conn, "messaging_pending_pushes", "confirm_pending") {
        conn.execute_batch(
            "ALTER TABLE messaging_pending_pushes
                ADD COLUMN confirm_pending INTEGER NOT NULL DEFAULT 0
                CHECK(confirm_pending IN (0,1));",
        )
        .expect("failed to add messaging_pending_pushes.confirm_pending column");
    }

    if !crate::db::column_exists(conn, "messaging_messages", "retained_seq") {
        conn.execute_batch("ALTER TABLE messaging_messages ADD COLUMN retained_seq INTEGER;")
            .expect("failed to add messaging_messages.retained_seq column");
    }
    if !crate::db::column_exists(conn, "messaging_channels", "last_retained_seq") {
        conn.execute_batch(
            "ALTER TABLE messaging_channels
                ADD COLUMN last_retained_seq INTEGER NOT NULL DEFAULT 0;",
        )
        .expect("failed to add messaging_channels.last_retained_seq column");
    }
    // SQLite rejects a NOT NULL ADD COLUMN without a default; the empty
    // placeholder is replaced by backfill_resume_epochs below.
    if !crate::db::column_exists(conn, "messaging_channels", "resume_epoch") {
        conn.execute_batch(
            "ALTER TABLE messaging_channels
                ADD COLUMN resume_epoch BLOB NOT NULL DEFAULT x'';",
        )
        .expect("failed to add messaging_channels.resume_epoch column");
    }
    // Outside the ALTER guard: the ALTER commits on its own, so a crash between
    // it and the backfill would otherwise strand the placeholder forever with no
    // path left to repair it. Idempotent and cheap — one scan of a table with one
    // row per channel, matching nothing once the epochs are real.
    backfill_resume_epochs(conn);

    // Must follow the ALTER that adds retained_seq on a legacy DB.
    conn.execute_batch(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_messaging_messages_retained_seq
            ON messaging_messages(channel_uuid, retained_seq)
            WHERE retained_seq IS NOT NULL;",
    )
    .expect("failed to create idx_messaging_messages_retained_seq");

    // WASM consumer failure quarantine table.
    // One row per failed batch; used as the durable audit/replay surface for
    // per-batch dispositions that did not complete successfully.
    // CREATE IF NOT EXISTS is idempotent — safe on fresh and existing DBs.
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS messaging_wasm_consume_failures (
            id               INTEGER PRIMARY KEY,
            channel          TEXT NOT NULL,
            subscriber       TEXT NOT NULL,
            first_message_id TEXT NOT NULL,
            last_message_id  TEXT NOT NULL,
            batch_push_ids   TEXT NOT NULL,
            outcome          TEXT NOT NULL CHECK(outcome IN ('err','trap')),
            diagnostic       TEXT NOT NULL,
            failed_at        TEXT NOT NULL,
            UNIQUE(subscriber, last_message_id)
        );
        CREATE INDEX IF NOT EXISTS idx_wasm_consume_failures_subscriber
            ON messaging_wasm_consume_failures(subscriber, channel);
        ",
    )
    .expect("failed to run messaging_wasm_consume_failures migration");

    // The store's durable identity (generation UUID + per-boot incarnation).
    // Created here so a fresh or restored DB carries the row from first boot; the
    // incarnation is bumped once per messenger boot in `Messenger::new`.
    super::store_identity::ensure_store_identity(conn);

    assert_messaging_schema_current(conn);

    // Dispatcher global-scan index. Its partial predicate references eager_wake, a
    // post-urgency-redesign column, so it is created only after the schema-currency
    // guard: on a legacy DB the guard aborts with an actionable message first, rather
    // than this DDL failing with a raw "no such column: eager_wake" error. The predicate
    // matches the dispatcher scan's WHERE so the planner can qualify this partial index;
    // parked no-deadline rows (eager_wake=0) fail the predicate and are never indexed.
    conn.execute_batch(
        "
        CREATE INDEX IF NOT EXISTS idx_messaging_pending_pushes_dispatchable
            ON messaging_pending_pushes(delivery_deadline)
            WHERE delivered_at IS NULL AND release_after IS NULL
              AND (eager_wake = 1 OR delivery_deadline IS NOT NULL);
        ",
    )
    .expect("failed to create idx_messaging_pending_pushes_dispatchable");

    // Backfill first: it numbers the rows that were already retained before this
    // boot, in rowid order. Normalizing afterwards releases the rows that came
    // due while the process was down, which take their sequences above those —
    // release order, so retention order is consistent across restarts.
    backfill_retained_seqs(conn);

    normalize_released_messages(conn);
}

/// Replace the migration's placeholder `resume_epoch` on every channel row with
/// a fresh UUID.
///
/// Runs once, in the ALTER path that added the column with a `DEFAULT ''`
/// placeholder. Every channel row is minted its own epoch — including
/// egress-only `pwa_push:` rows, which are ordinary channel rows here.
fn backfill_resume_epochs(conn: &Connection) {
    let uuids: Vec<Vec<u8>> = conn
        .prepare("SELECT uuid FROM messaging_channels WHERE resume_epoch = x''")
        .expect("prepare resume_epoch backfill scan")
        .query_map([], |row| row.get(0))
        .expect("query resume_epoch backfill scan")
        .map(|r| r.expect("read channel uuid for resume_epoch backfill"))
        .collect();
    for uuid in uuids {
        conn.execute(
            "UPDATE messaging_channels SET resume_epoch = ?2 WHERE uuid = ?1",
            rusqlite::params![uuid, uuid::Uuid::new_v4().as_bytes().to_vec()],
        )
        .expect("backfill resume_epoch");
    }
}

/// Assign `retained_seq` to every retained (`deliver_after IS NULL`) row that
/// lacks one, per channel in rowid order.
///
/// Idempotent: rows that already carry a seq are untouched. Rows still parked
/// (`deliver_after IS NOT NULL`) stay unsequenced until release. Ingress rows
/// (`channel_uuid IS NULL`) are excluded.
///
/// Runs at every boot but does work only where a channel has unsequenced
/// retained rows, so the cost is a per-channel block allocation plus one cached
/// assignment statement per row — paid once, on the deploy that adds the column.
fn backfill_retained_seqs(conn: &Connection) {
    let channels: Vec<Vec<u8>> = conn
        .prepare(
            "SELECT DISTINCT channel_uuid FROM messaging_messages
             WHERE channel_uuid IS NOT NULL
               AND deliver_after IS NULL
               AND retained_seq IS NULL",
        )
        .expect("prepare retained_seq backfill channel scan")
        .query_map([], |row| row.get(0))
        .expect("query retained_seq backfill channel scan")
        .map(|r| r.expect("read channel uuid for retained_seq backfill"))
        .collect();

    for channel in channels {
        let ids: Vec<i64> = conn
            .prepare(
                "SELECT id FROM messaging_messages
                 WHERE channel_uuid = ?1 AND deliver_after IS NULL AND retained_seq IS NULL
                 ORDER BY id ASC",
            )
            .expect("prepare retained_seq backfill row scan")
            .query_map(rusqlite::params![channel], |row| row.get(0))
            .expect("query retained_seq backfill row scan")
            .map(|r| r.expect("read message id for retained_seq backfill"))
            .collect();
        let channel_uuid = uuid::Uuid::from_slice(&channel)
            .expect("messaging: malformed channel uuid in retained_seq backfill");
        super::bus::assign_retained_seqs(conn, channel_uuid, &ids);
    }
}

/// Release the messages that came due with nothing left holding them: clear the
/// message-row half of the deferral hold and mint each one a retention sequence.
///
/// A message is parked while `messaging_messages.deliver_after` is set, and
/// released when a release pass clears it along with its push rows'
/// `release_after`. A message whose push rows were all released (or that never
/// had any) but whose own hold still stands would never appear in a retention
/// read and would occupy a deferred-cap slot forever.
///
/// The predicate is what makes this safe to run at every boot: a message still
/// holding an undelivered, unreleased push row is genuinely parked and is left
/// alone, and a future `deliver_after` is left alone.
///
/// Sequences are minted per channel in release order (`deliver_after` ascending,
/// rowid breaking ties), so retention order is consistent regardless of whether
/// a release occurs during uptime or downtime.
fn normalize_released_messages(conn: &Connection) {
    const RELEASABLE: &str = "deliver_after IS NOT NULL
         AND deliver_after <= ?1
         AND NOT EXISTS (
             SELECT 1 FROM messaging_pending_pushes pp
             WHERE pp.message_id = messaging_messages.id
               AND pp.delivered_at IS NULL
               AND pp.release_after IS NOT NULL
         )";
    let now = crate::db::format_ts_for_db(chrono::Utc::now());

    // Read the release order before clearing the column that carries it.
    let mut stmt = conn
        .prepare(&format!(
            "SELECT channel_uuid, id FROM messaging_messages
             WHERE {RELEASABLE} AND channel_uuid IS NOT NULL
             ORDER BY deliver_after ASC, id ASC"
        ))
        .expect("prepare boot release-order scan");
    let mut grouped: Vec<(uuid::Uuid, Vec<i64>)> = Vec::new();
    let rows = stmt
        .query_map(rusqlite::params![now], |row| {
            Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, i64>(1)?))
        })
        .expect("query boot release-order scan");
    for r in rows {
        let (channel, id) = r.expect("read boot release-order row");
        let channel_uuid = uuid::Uuid::from_slice(&channel)
            .expect("messaging: malformed channel uuid on a boot-released row");
        match grouped.iter_mut().find(|(uuid, _)| *uuid == channel_uuid) {
            Some((_, ids)) => ids.push(id),
            None => grouped.push((channel_uuid, vec![id])),
        }
    }
    drop(stmt);

    conn.execute(
        &format!("UPDATE messaging_messages SET deliver_after = NULL WHERE {RELEASABLE}"),
        rusqlite::params![now],
    )
    .expect("failed to normalize released messaging_messages rows");

    for (channel_uuid, ids) in grouped {
        super::bus::assign_retained_seqs(conn, channel_uuid, &ids);
    }
}

/// Fail fast if the messaging schema predates the urgency redesign (pre-2026-06).
///
/// The `CREATE TABLE IF NOT EXISTS` DDL above never alters a pre-existing table,
/// so a DB carrying the legacy (pre-urgency-redesign) table shapes boots past
/// migrations and then breaks later with a raw SQLite error — e.g. the
/// dispatcher scan selecting `pp.eager_wake` panics inside its background task,
/// leaving the server half-alive with durable delivery silently dead. No
/// migration path exists for such a legacy DB, so it is unsupported. This guard
/// turns the latent breakage into an actionable abort at boot.
///
/// It is a tripwire for the known removed-migration floor, not general schema
/// validation: the sentinel list is frozen at that floor. Future post-hoc
/// columns get real guarded migrations in the reserved slot and must not be
/// added here.
fn assert_messaging_schema_current(conn: &Connection) {
    const SENTINELS: &[(&str, &str)] = &[
        ("messaging_pending_pushes", "eager_wake"),
        ("messaging_messages", "urgency"),
        ("messaging_subscriptions", "wake_min"),
    ];
    for (table, column) in SENTINELS {
        if !crate::db::column_exists(conn, table, column) {
            panic!(
                "messaging DB schema is out of date: {table}.{column} is missing. \
                 This database predates the urgency redesign (pre-2026-06) and its \
                 migrations were removed; automatic migration is not supported. \
                 Restore from a current backup, or delete the database file if its \
                 contents are disposable (dev databases)."
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::db::{column_exists, init_db_memory};
    use crate::messaging::ChannelScheme;

    // -----------------------------------------------------------------------
    // Smoke test
    // -----------------------------------------------------------------------

    /// The convergence pass clears the message-row hold only where nothing is
    /// still waiting on it: a due message whose parked push rows were released
    /// without it, never a message that is genuinely still parked.
    #[test]
    fn boot_normalizes_only_the_messages_nothing_still_holds() {
        use chrono::{Duration, Utc};
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let past = crate::db::format_ts_for_db(Utc::now() - Duration::hours(1));
        let future = crate::db::format_ts_for_db(Utc::now() + Duration::hours(1));
        conn.execute_batch(&format!(
            "INSERT INTO messaging_messages
                 (id, uuid, source, sender, body, urgency, deliver_after, publish_ts_ns, created_at)
             VALUES (1, X'01', 's', 'a', 'stale', 'normal', '{past}', 0, '{past}'),
                    (2, X'02', 's', 'a', 'held', 'normal', '{past}', 0, '{past}'),
                    (3, X'03', 's', 'a', 'future', 'normal', '{future}', 0, '{past}');
             INSERT INTO messaging_pending_pushes
                 (message_id, target_subscriber, target_app_slug, eager_wake, release_after,
                  created_at)
             VALUES (2, 'conv:1', 'app', 1, '{past}', '{past}');"
        ))
        .expect("seed rows");

        super::normalize_released_messages(&conn);

        let parked: Vec<i64> = conn
            .prepare(
                "SELECT id FROM messaging_messages WHERE deliver_after IS NOT NULL ORDER BY id",
            )
            .expect("prepare")
            .query_map([], |row| row.get(0))
            .expect("query")
            .map(|r| r.expect("read"))
            .collect();
        assert_eq!(
            parked,
            vec![2, 3],
            "only the message whose pushes were released without it is converged"
        );
    }

    /// Messages that came due while the process was down are released at boot
    /// and take their retention sequences in release order, not rowid order —
    /// the same order a live release pass and the ring both use.
    #[test]
    fn boot_release_sequences_in_release_order_not_rowid_order() {
        use chrono::{Duration, Utc};
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let ch = uuid::Uuid::new_v4();
        let older = crate::db::format_ts_for_db(Utc::now() - Duration::hours(2));
        let newer = crate::db::format_ts_for_db(Utc::now() - Duration::hours(1));
        conn.execute_batch(&format!(
            "INSERT INTO messaging_channels (uuid, address, created_at, resume_epoch)
             VALUES (X'{ch}', 'brenn:down', 'now', X'{ep}');
             INSERT INTO messaging_messages
                 (id, uuid, channel_uuid, source, sender, body, urgency, deliver_after,
                  publish_ts_ns, created_at)
             VALUES (1, X'{u1}', X'{ch}', 's', 'a', 'late-release', 'normal', '{newer}', 0, 'now'),
                    (2, X'{u2}', X'{ch}', 's', 'a', 'early-release', 'normal', '{older}', 0,
                     'now');",
            ch = ch.simple(),
            ep = uuid::Uuid::new_v4().simple(),
            u1 = uuid::Uuid::new_v4().simple(),
            u2 = uuid::Uuid::new_v4().simple(),
        ))
        .expect("seed rows");

        super::normalize_released_messages(&conn);

        let seq = |id: i64| -> Option<i64> {
            conn.query_row(
                "SELECT retained_seq FROM messaging_messages WHERE id = ?1",
                rusqlite::params![id],
                |r| r.get(0),
            )
            .expect("read retained_seq")
        };
        assert_eq!(
            seq(2),
            Some(1),
            "the earlier release time is retained first"
        );
        assert_eq!(seq(1), Some(2), "the later release time is retained second");
    }

    #[test]
    fn migrations_create_messaging_tables() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        for table in &[
            "messaging_channels",
            "messaging_messages",
            "messaging_messages_fts",
            "messaging_subscriptions",
            "messaging_dynamic_subscriptions",
            "messaging_pending_pushes",
            "messaging_send_budget",
            "messaging_wasm_consume_failures",
        ] {
            conn.execute(&format!("SELECT * FROM {table} WHERE 0"), [])
                .unwrap_or_else(|e| panic!("table {table} not created: {e}"));
        }
    }

    /// The `confirm_pending` ALTER path, driven on a synthesized pre-column DB:
    /// the column is added, a row written before it existed reads as
    /// not-tentative (the default the CREATE TABLE declares), and a second
    /// migration pass over the now-current schema is inert.
    #[test]
    fn confirm_pending_migrates_onto_a_pre_column_store() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        // Synthesize the old schema: drop the current table and rebuild it
        // without `confirm_pending`, then seed a row as the old code would.
        conn.execute_batch(
            "DROP TABLE messaging_pending_pushes;
             CREATE TABLE messaging_pending_pushes (
                id                INTEGER PRIMARY KEY,
                message_id        INTEGER NOT NULL REFERENCES messaging_messages(id),
                target_subscriber TEXT NOT NULL,
                target_app_slug   TEXT NOT NULL,
                eager_wake        INTEGER NOT NULL CHECK(eager_wake IN (0,1)),
                delivery_deadline TEXT,
                release_after     TEXT,
                delivered_at      TEXT,
                created_at        TEXT NOT NULL
             );",
        )
        .unwrap();
        assert!(!column_exists(
            &conn,
            "messaging_pending_pushes",
            "confirm_pending"
        ));
        // The row's message FK is irrelevant to the ALTER under test and seeding a
        // real message would drag in a channel row too, so the constraint is
        // suspended for this one insert.
        conn.execute_batch(
            "PRAGMA foreign_keys=OFF;
             INSERT INTO messaging_pending_pushes
                (id, message_id, target_subscriber, target_app_slug, eager_wake, created_at)
             VALUES (1, 1, 'sub', 'slug', 1, 'now');
             PRAGMA foreign_keys=ON;",
        )
        .unwrap();

        super::run_messaging_migrations(&conn);
        assert!(
            column_exists(&conn, "messaging_pending_pushes", "confirm_pending"),
            "the ALTER path adds the column to a pre-existing store"
        );
        let flag: i64 = conn
            .query_row(
                "SELECT confirm_pending FROM messaging_pending_pushes WHERE id = 1",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            flag, 0,
            "a row written before the column reads not-tentative"
        );

        // Idempotent: re-running over the current schema neither errors nor
        // rewrites the row.
        super::run_messaging_migrations(&conn);
        let flag_after: i64 = conn
            .query_row(
                "SELECT confirm_pending FROM messaging_pending_pushes WHERE id = 1",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(flag_after, 0, "the second pass leaves the row untouched");
    }

    /// The durable-resume migration on a pre-column DB: the three columns are
    /// added, every retained (`deliver_after IS NULL`) row is assigned
    /// `retained_seq` per channel in rowid order, `last_retained_seq` records the
    /// high-water, each channel is minted a distinct real `resume_epoch`, and a
    /// parked row stays unsequenced until its first release.
    #[test]
    fn resume_migration_backfills_seq_epoch_and_high_water() {
        let conn = rusqlite::Connection::open_in_memory().expect("open in-memory db");
        // FKs off so hand-written channel/message rows need no full DDL.
        conn.execute_batch(
            "PRAGMA foreign_keys=OFF;
             CREATE TABLE messaging_channels (
                uuid BLOB PRIMARY KEY, address TEXT NOT NULL UNIQUE, description TEXT,
                created_at TEXT NOT NULL, transport_type TEXT NOT NULL DEFAULT 'brenn'
             );
             CREATE TABLE messaging_messages (
                id INTEGER PRIMARY KEY, uuid BLOB NOT NULL UNIQUE, channel_uuid BLOB,
                source TEXT NOT NULL, sender TEXT NOT NULL, body TEXT NOT NULL,
                urgency TEXT NOT NULL, deliver_after TEXT, publish_ts_ns INTEGER NOT NULL,
                created_at TEXT NOT NULL
             );",
        )
        .expect("seed pre-migration schema");

        let a = uuid::Uuid::new_v4();
        let b = uuid::Uuid::new_v4();
        let future = crate::db::format_ts_for_db(chrono::Utc::now() + chrono::Duration::hours(1));
        conn.execute_batch(&format!(
            "INSERT INTO messaging_channels (uuid, address, created_at)
             VALUES (X'{a}', 'brenn:a', 'now'), (X'{b}', 'brenn:b', 'now');
             INSERT INTO messaging_messages
                 (id, uuid, channel_uuid, source, sender, body, urgency, deliver_after,
                  publish_ts_ns, created_at)
             VALUES (1, X'01', X'{a}', 's', 'x', 'm1', 'low', NULL, 0, 'now'),
                    (2, X'02', X'{a}', 's', 'x', 'm2', 'low', NULL, 0, 'now'),
                    (3, X'03', X'{a}', 's', 'x', 'p',  'low', '{future}', 0, 'now'),
                    (4, X'04', X'{b}', 's', 'x', 'n1', 'low', NULL, 0, 'now');",
            a = a.simple(),
            b = b.simple(),
        ))
        .expect("seed pre-migration rows");

        super::run_messaging_migrations(&conn);

        let seq = |id: i64| -> Option<i64> {
            conn.query_row(
                "SELECT retained_seq FROM messaging_messages WHERE id = ?1",
                rusqlite::params![id],
                |r| r.get(0),
            )
            .expect("read retained_seq")
        };
        assert_eq!(seq(1), Some(1), "channel A oldest gets seq 1");
        assert_eq!(seq(2), Some(2), "channel A next gets seq 2");
        assert_eq!(seq(3), None, "the parked row stays unsequenced");
        assert_eq!(seq(4), Some(1), "channel B numbers independently from 1");

        let hw = |ch: uuid::Uuid| -> i64 {
            conn.query_row(
                "SELECT last_retained_seq FROM messaging_channels WHERE uuid = ?1",
                rusqlite::params![ch.as_bytes().to_vec()],
                |r| r.get(0),
            )
            .expect("read last_retained_seq")
        };
        assert_eq!(hw(a), 2);
        assert_eq!(hw(b), 1);

        let epoch = |ch: uuid::Uuid| -> Vec<u8> {
            conn.query_row(
                "SELECT resume_epoch FROM messaging_channels WHERE uuid = ?1",
                rusqlite::params![ch.as_bytes().to_vec()],
                |r| r.get(0),
            )
            .expect("read resume_epoch")
        };
        let (ea, eb) = (epoch(a), epoch(b));
        assert!(!ea.is_empty() && !eb.is_empty(), "placeholder was replaced");
        assert_ne!(ea, eb, "each channel is minted its own epoch");
        assert!(
            uuid::Uuid::from_slice(&ea).is_ok(),
            "the minted epoch is a real UUID"
        );

        // A second pass over the now-current schema is inert: the backfills run
        // on every boot and must not renumber or re-mint anything.
        super::run_messaging_migrations(&conn);
        assert_eq!(seq(1), Some(1));
        assert_eq!(seq(2), Some(2));
        assert_eq!(seq(3), None);
        assert_eq!(seq(4), Some(1));
        assert_eq!(hw(a), 2);
        assert_eq!(hw(b), 1);
        assert_eq!(epoch(a), ea, "an already-minted epoch is never re-minted");
    }

    /// The deploy-day path continues past the migration: the first append after
    /// it must take the sequence above the backfilled high-water, and a resume
    /// against the migrated store must answer per the resume rules. A block
    /// allocator that left `last_retained_seq` lagging the seqs it wrote would
    /// pass the column assertions above and then collide on the unique index
    /// with the first message published after the upgrade.
    #[tokio::test]
    async fn a_migrated_store_appends_and_resumes() {
        use crate::messaging::store::{ResumeCursor, RetentionStore};

        let conn = rusqlite::Connection::open_in_memory().expect("open in-memory db");
        conn.execute_batch(
            "PRAGMA foreign_keys=OFF;
             CREATE TABLE messaging_channels (
                uuid BLOB PRIMARY KEY, address TEXT NOT NULL UNIQUE, description TEXT,
                created_at TEXT NOT NULL, transport_type TEXT NOT NULL DEFAULT 'brenn'
             );
             CREATE TABLE messaging_messages (
                id INTEGER PRIMARY KEY, uuid BLOB NOT NULL UNIQUE, channel_uuid BLOB,
                source TEXT NOT NULL, sender TEXT NOT NULL, body TEXT NOT NULL,
                urgency TEXT NOT NULL, reply_to_uuid BLOB, delivery_deadline TEXT,
                deliver_after TEXT, publish_ts_ns INTEGER NOT NULL, created_at TEXT NOT NULL,
                envelope_type TEXT NOT NULL DEFAULT 'brenn', ingress_source TEXT,
                ingress_summary TEXT
             );",
        )
        .expect("seed pre-migration schema");

        let a = uuid::Uuid::new_v4();
        conn.execute_batch(&format!(
            "INSERT INTO messaging_channels (uuid, address, created_at)
             VALUES (X'{a}', 'brenn:a', 'now');
             INSERT INTO messaging_messages
                 (id, uuid, channel_uuid, source, sender, body, urgency, deliver_after,
                  publish_ts_ns, created_at)
             VALUES (1, X'{m1}', X'{a}', 's', 'x', 'm1', 'low', NULL, 0, 'now'),
                    (2, X'{m2}', X'{a}', 's', 'x', 'm2', 'low', NULL, 0, 'now');",
            a = a.simple(),
            m1 = uuid::Uuid::new_v4().simple(),
            m2 = uuid::Uuid::new_v4().simple(),
        ))
        .expect("seed pre-migration rows");

        super::run_messaging_migrations(&conn);

        let db: crate::db::Db = std::sync::Arc::new(tokio::sync::Mutex::new(conn));
        let store = crate::messaging::store::DbStore::new(
            db.clone(),
            a,
            "brenn:a",
            crate::messaging::config::Depth::Unbounded,
            std::sync::Arc::new(crate::messaging::store::TargetResolver::unsubscribed()),
        );

        // The first post-migration append continues the backfilled numbering.
        let committed = store
            .append(crate::messaging::store::NewMessage {
                source: "s".to_string(),
                sender: "x".to_string(),
                body: "m3".to_string(),
                urgency: crate::messaging::Urgency::Low,
                envelope_type: crate::messaging::ChannelScheme::Brenn,
                reply_to_uuid: None,
                delivery_deadline: None,
                publish_ts_ns: 0,
            })
            .await;
        assert_eq!(
            committed.committed.seq.0, 3,
            "the append takes the next seq"
        );

        let epoch = store
            .replay_from(None, crate::messaging::config::Depth::Unbounded)
            .await
            .epoch;
        let cursor = |seq| ResumeCursor { epoch, seq };

        let up_to_date = store
            .replay_from(Some(cursor(3)), crate::messaging::config::Depth::Unbounded)
            .await;
        assert_eq!(
            up_to_date.decision,
            brenn_queue::ReplayDecision::UpToDate,
            "a cursor at the high-water over migrated data is up to date"
        );

        let trailing = store
            .replay_from(Some(cursor(1)), crate::messaging::config::Depth::Unbounded)
            .await;
        assert_eq!(trailing.decision, brenn_queue::ReplayDecision::Exact);
        let bodies: Vec<String> = trailing
            .messages
            .iter()
            .map(|m| m.message.body.clone())
            .collect();
        assert_eq!(bodies, ["m2", "m3"], "migrated rows replay in seq order");
    }

    // -----------------------------------------------------------------------
    // envelope_type column codec
    // -----------------------------------------------------------------------

    /// The `messaging_messages.envelope_type` column codec pins the canonical
    /// stored strings, including the storage-only `ingress` row-kind alongside
    /// the bus scheme tags. Round-trip coverage of every variant lives on the
    /// codec itself (`super::envelope_column`); this asserts the DB-facing
    /// column strings the schema depends on.
    #[test]
    fn envelope_type_column_strings() {
        use super::super::EnvelopeTypeColumn;
        assert_eq!(
            EnvelopeTypeColumn::Bus(ChannelScheme::Brenn).as_str(),
            "brenn"
        );
        assert_eq!(
            EnvelopeTypeColumn::Bus(ChannelScheme::Webhook).as_str(),
            "webhook"
        );
        assert_eq!(
            EnvelopeTypeColumn::Bus(ChannelScheme::Mqtt).as_str(),
            "mqtt"
        );
        assert_eq!(EnvelopeTypeColumn::Ingress.as_str(), "ingress");
        assert!(EnvelopeTypeColumn::parse("bogus").is_none());
    }

    // -----------------------------------------------------------------------
    // Current-schema column assertions
    // -----------------------------------------------------------------------

    /// On a fresh DB, the DDL path produces the current schema: each messaging
    /// table has its current columns and none of the legacy columns the deleted
    /// migrations used to carry. This single test pins the post-simplification
    /// schema, replacing the scattered fresh-DB assertions from the removed
    /// migration tests.
    #[test]
    fn fresh_schema_has_current_columns() {
        let db = init_db_memory();
        let conn = db.blocking_lock();

        // messaging_messages: current ingress/urgency columns present, legacy absent.
        for col in &[
            "urgency",
            "envelope_type",
            "ingress_source",
            "ingress_summary",
            "retained_seq",
        ] {
            assert!(
                column_exists(&conn, "messaging_messages", col),
                "messaging_messages missing current column {col}"
            );
        }
        for col in &["wake_kind", "kind"] {
            assert!(
                !column_exists(&conn, "messaging_messages", col),
                "messaging_messages still has legacy column {col}"
            );
        }

        // messaging_channels: transport_type + durable-resume columns present.
        for col in &["transport_type", "resume_epoch", "last_retained_seq"] {
            assert!(
                column_exists(&conn, "messaging_channels", col),
                "messaging_channels missing current column {col}"
            );
        }

        // messaging_subscriptions: depth/wake model present, legacy kind absent.
        for col in &["push_depth", "retain_depth", "noise", "wake_min"] {
            assert!(
                column_exists(&conn, "messaging_subscriptions", col),
                "messaging_subscriptions missing current column {col}"
            );
        }
        assert!(
            !column_exists(&conn, "messaging_subscriptions", "kind"),
            "messaging_subscriptions still has legacy column kind"
        );

        // messaging_dynamic_subscriptions: same depth/wake model as the static
        // mirror, plus the MQTT qos column and created_at (design §2.1).
        for col in &[
            "channel_uuid",
            "app_slug",
            "push_depth",
            "retain_depth",
            "noise",
            "wake_min",
            "qos",
            "created_at",
        ] {
            assert!(
                column_exists(&conn, "messaging_dynamic_subscriptions", col),
                "messaging_dynamic_subscriptions missing current column {col}"
            );
        }

        // messaging_pending_pushes: eager_wake present, legacy wake_kind absent.
        assert!(
            column_exists(&conn, "messaging_pending_pushes", "eager_wake"),
            "messaging_pending_pushes missing eager_wake"
        );
        assert!(
            !column_exists(&conn, "messaging_pending_pushes", "wake_kind"),
            "messaging_pending_pushes still has legacy column wake_kind"
        );

        // messaging_wasm_consume_failures: design's test plan requires this
        // table's columns be pinned so a typo in its DDL fails here rather than
        // passing silently.
        for col in &[
            "channel",
            "subscriber",
            "first_message_id",
            "last_message_id",
            "batch_push_ids",
            "outcome",
            "diagnostic",
            "failed_at",
        ] {
            assert!(
                column_exists(&conn, "messaging_wasm_consume_failures", col),
                "messaging_wasm_consume_failures missing current column {col}"
            );
        }

        // messaging_send_budget: current columns present.
        for col in &["remaining", "last_reset_at"] {
            assert!(
                column_exists(&conn, "messaging_send_budget", col),
                "messaging_send_budget missing current column {col}"
            );
        }
    }

    // -----------------------------------------------------------------------
    // FTS trigger smoke test
    // -----------------------------------------------------------------------

    /// The three FTS triggers (after-insert, after-delete, after-update-of-body)
    /// are pure DDL with no migration; nothing else in this module exercises
    /// their bodies. Insert/update/delete a message and assert the FTS shadow
    /// table tracks the body so a typo in any trigger body fails here.
    #[test]
    fn fts_triggers_track_message_body() {
        let db = init_db_memory();
        let conn = db.blocking_lock();

        let fts_matches = |needle: &str| -> i64 {
            conn.query_row(
                "SELECT COUNT(*) FROM messaging_messages_fts WHERE body MATCH ?1",
                rusqlite::params![needle],
                |r| r.get(0),
            )
            .expect("query fts")
        };

        // after-insert: body becomes searchable.
        conn.execute(
            "INSERT INTO messaging_messages
                (uuid, source, sender, body, urgency, publish_ts_ns, created_at)
             VALUES (X'00', 'test', 'tester', 'alpha', 'normal', 0, '2026-01-01T00:00:00Z')",
            [],
        )
        .expect("insert message");
        assert_eq!(
            fts_matches("alpha"),
            1,
            "after-insert trigger did not index body"
        );

        // after-update-of-body: old term gone, new term present.
        conn.execute(
            "UPDATE messaging_messages SET body = 'bravo' WHERE uuid = X'00'",
            [],
        )
        .expect("update message body");
        assert_eq!(
            fts_matches("alpha"),
            0,
            "after-update trigger did not remove old body"
        );
        assert_eq!(
            fts_matches("bravo"),
            1,
            "after-update trigger did not index new body"
        );

        // after-delete: term removed from FTS.
        conn.execute("DELETE FROM messaging_messages WHERE uuid = X'00'", [])
            .expect("delete message");
        assert_eq!(
            fts_matches("bravo"),
            0,
            "after-delete trigger did not remove body"
        );
    }

    // -----------------------------------------------------------------------
    // Schema-currency guard
    // -----------------------------------------------------------------------

    /// A DB whose `messaging_pending_pushes` predates `eager_wake` (the legacy
    /// `wake_kind` shape) trips the guard. `CREATE TABLE IF NOT EXISTS` leaves
    /// the pre-existing legacy table untouched, so the sentinel is missing and
    /// boot aborts with an actionable message instead of a later raw SQLite
    /// error in the dispatcher.
    #[test]
    #[should_panic(expected = "messaging_pending_pushes.eager_wake is missing")]
    fn legacy_pending_pushes_trips_guard() {
        let conn = rusqlite::Connection::open_in_memory().expect("open in-memory db");
        // Legacy shape (pre-urgency-redesign): wake_kind instead of eager_wake.
        // Creating it before messaging_messages exists is fine — SQLite does not
        // validate FK targets at DDL time.
        conn.execute_batch(
            "CREATE TABLE messaging_pending_pushes (
                id                INTEGER PRIMARY KEY,
                message_id        INTEGER NOT NULL REFERENCES messaging_messages(id),
                target_subscriber TEXT NOT NULL,
                target_app_slug   TEXT NOT NULL,
                wake_kind         TEXT NOT NULL CHECK(wake_kind IN ('none','immediate')),
                delivery_deadline TEXT,
                release_after     TEXT,
                delivered_at      TEXT,
                created_at        TEXT NOT NULL
            );",
        )
        .expect("create legacy pending_pushes");
        super::run_messaging_migrations(&conn);
    }

    /// A DB whose `messaging_messages` predates `urgency` (the legacy
    /// `wake_kind` shape) trips the guard on the messages sentinel.
    #[test]
    #[should_panic(expected = "messaging_messages.urgency is missing")]
    fn legacy_messages_trips_guard() {
        let conn = rusqlite::Connection::open_in_memory().expect("open in-memory db");
        // Legacy shape: wake_kind where the current schema has urgency. Other
        // columns match the current table so the current DDL's indexes and FTS
        // triggers (which reference deliver_after, body, id) create cleanly and
        // the guard is what trips, on the urgency sentinel.
        conn.execute_batch(
            "CREATE TABLE messaging_messages (
                id                  INTEGER PRIMARY KEY,
                uuid                BLOB NOT NULL UNIQUE,
                channel_uuid        BLOB,
                source              TEXT NOT NULL,
                sender              TEXT NOT NULL,
                body                TEXT NOT NULL,
                wake_kind           TEXT NOT NULL CHECK(wake_kind IN ('none','immediate')),
                reply_to_uuid       BLOB,
                delivery_deadline   TEXT,
                deliver_after       TEXT,
                publish_ts_ns       INTEGER NOT NULL,
                created_at          TEXT NOT NULL
            );",
        )
        .expect("create legacy messages");
        super::run_messaging_migrations(&conn);
    }
}
