//! DAOs over the `device_locations` table.
//!
//! The DDL and the unenroll-time deletion live in `brenn_db::device_locations`;
//! these are the reads and writes the WebSocket handlers and the tool make.
//! Every function takes a connection the caller has locked and panics on an SQL
//! error. Device and user always come from the authenticated caller, never
//! from a client message.

use brenn_db::auth::device::{DeviceUser, device_user_columns, device_user_from_row};
use rusqlite::{Connection, OptionalExtension as _};

use crate::validate::ValidFix;

/// A stored fix with the `device_users` row of its pair, as the tool answers it.
#[derive(Debug, Clone)]
pub struct StoredFix {
    /// The `(device, user)` row the fix belongs to: zone override and stored
    /// browser zone included.
    pub device_user: DeviceUser,
    /// The user's name for the device: their assigned slug, else the guessed one.
    pub display_slug: String,
    pub latitude: f64,
    pub longitude: f64,
    pub accuracy_m: f64,
    /// Server receipt time, Unix ms.
    pub reported_at_ms: i64,
}

/// Store `fix` as the pair's current fix, creating the row or replacing it.
/// The path that turns sharing on.
pub fn upsert_fix(
    conn: &Connection,
    device_id: i64,
    user_id: i64,
    fix: &ValidFix,
    reported_at_ms: i64,
) {
    conn.execute(
        "INSERT INTO device_locations
             (device_id, user_id, latitude, longitude, accuracy_m, reported_at_ms)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)
         ON CONFLICT(device_id, user_id) DO UPDATE SET
             latitude = excluded.latitude,
             longitude = excluded.longitude,
             accuracy_m = excluded.accuracy_m,
             reported_at_ms = excluded.reported_at_ms",
        rusqlite::params![
            device_id,
            user_id,
            fix.latitude(),
            fix.longitude(),
            fix.accuracy_m(),
            reported_at_ms
        ],
    )
    .expect("device_locations: upsert_fix");
}

/// Replace the pair's fix only if one is stored; `true` when a row changed.
/// A pair with no row stays without one, so a periodic report cannot turn
/// sharing back on.
pub fn update_fix(
    conn: &Connection,
    device_id: i64,
    user_id: i64,
    fix: &ValidFix,
    reported_at_ms: i64,
) -> bool {
    let rows = conn
        .execute(
            "UPDATE device_locations
             SET latitude = ?3, longitude = ?4, accuracy_m = ?5, reported_at_ms = ?6
             WHERE device_id = ?1 AND user_id = ?2",
            rusqlite::params![
                device_id,
                user_id,
                fix.latitude(),
                fix.longitude(),
                fix.accuracy_m(),
                reported_at_ms
            ],
        )
        .expect("device_locations: update_fix");
    // The primary key is (device_id, user_id): at most one row matches.
    assert!(rows <= 1, "update_fix: {rows} rows for one primary key");
    rows == 1
}

/// Delete the pair's fix. 0 or 1 rows; no panic on 0.
pub fn delete_fix(conn: &Connection, device_id: i64, user_id: i64) {
    conn.execute(
        "DELETE FROM device_locations WHERE device_id = ?1 AND user_id = ?2",
        rusqlite::params![device_id, user_id],
    )
    .expect("device_locations: delete_fix");
}

/// Whether the pair has a stored fix, i.e. shares location.
pub fn fix_exists(conn: &Connection, device_id: i64, user_id: i64) -> bool {
    conn.query_row(
        "SELECT 1 FROM device_locations WHERE device_id = ?1 AND user_id = ?2",
        rusqlite::params![device_id, user_id],
        |_| Ok(()),
    )
    .optional()
    .expect("device_locations: fix_exists")
    .is_some()
}

/// Every stored fix of `user_id` on an enrolled device, most recent first.
pub fn list_fixes_for_user(conn: &Connection, user_id: i64) -> Vec<StoredFix> {
    let sql = format!(
        "SELECT {du_cols},
                COALESCE(du.assigned_slug, d.guessed_slug),
                l.latitude, l.longitude, l.accuracy_m, l.reported_at_ms
         FROM device_locations l
         JOIN device_users du ON du.device_id = l.device_id AND du.user_id = l.user_id
         JOIN active_devices d ON d.id = l.device_id
         WHERE l.user_id = ?1
         ORDER BY l.reported_at_ms DESC, l.device_id",
        du_cols = device_user_columns("du"),
    );
    let mut stmt = conn
        .prepare(&sql)
        .expect("device_locations: list_fixes_for_user prepare");
    stmt.query_map(rusqlite::params![user_id], |row| {
        Ok(StoredFix {
            device_user: device_user_from_row(row, 0)?,
            display_slug: row.get(7)?,
            latitude: row.get(8)?,
            longitude: row.get(9)?,
            accuracy_m: row.get(10)?,
            reported_at_ms: row.get(11)?,
        })
    })
    .expect("device_locations: list_fixes_for_user query")
    .collect::<Result<Vec<_>, _>>()
    .expect("device_locations: list_fixes_for_user row")
}

/// An in-memory database with every `brenn-db` table, seeded for this crate's
/// tests.
///
/// Users: alice (1), bob (2). Devices: laptop (1), phone (2), tablet (3).
/// `device_users`: (laptop, alice), (phone, alice), (laptop, bob),
/// (tablet, alice).
#[cfg(test)]
pub(crate) fn seeded_db() -> brenn_db::Db {
    let conn = brenn_db::open_connection_memory();
    brenn_db::run_migrations(&conn);
    let now = brenn_db::format_ts_for_db(chrono::Utc::now());
    conn.execute_batch(&format!(
        "
        INSERT INTO users (id, username, password_hash, created_at)
            VALUES (1, 'alice', 'hash', '{now}'), (2, 'bob', 'hash', '{now}');
        INSERT INTO devices (id, token, guessed_slug, last_seen_at, created_at)
            VALUES (1, 'tok1', 'laptop', '{now}', '{now}'),
                   (2, 'tok2', 'phone', '{now}', '{now}'),
                   (3, 'tok3', 'tablet', '{now}', '{now}');
        INSERT INTO device_users (device_id, user_id, first_seen_at, last_seen_at)
            VALUES (1, 1, '{now}', '{now}'), (2, 1, '{now}', '{now}'),
                   (1, 2, '{now}', '{now}'), (3, 1, '{now}', '{now}');
        "
    ))
    .expect("seed test rows");
    brenn_db::into_db(conn)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::validate::fix;
    use brenn_db::auth::device::{load_device_user, set_browser_tz, set_tz_override};

    const ALICE: i64 = 1;
    const BOB: i64 = 2;
    const LAPTOP: i64 = 1;
    const PHONE: i64 = 2;
    const TABLET: i64 = 3;

    fn stored(conn: &Connection, device_id: i64, user_id: i64) -> Option<(f64, f64, f64, i64)> {
        conn.query_row(
            "SELECT latitude, longitude, accuracy_m, reported_at_ms FROM device_locations
             WHERE device_id = ?1 AND user_id = ?2",
            rusqlite::params![device_id, user_id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .optional()
        .unwrap()
    }

    #[test]
    fn upsert_creates_then_replaces() {
        let db = seeded_db();
        let conn = db.blocking_lock();
        upsert_fix(&conn, PHONE, ALICE, &fix(1.0, 2.0, 3.0), 100);
        assert_eq!(stored(&conn, PHONE, ALICE), Some((1.0, 2.0, 3.0, 100)));
        upsert_fix(&conn, PHONE, ALICE, &fix(4.0, 5.0, 6.0), 200);
        assert_eq!(stored(&conn, PHONE, ALICE), Some((4.0, 5.0, 6.0, 200)));
        let n: i64 = conn
            .query_row("SELECT COUNT(*) FROM device_locations", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 1, "latest wins; no history row");
    }

    #[test]
    fn update_fix_changes_only_an_existing_row() {
        let db = seeded_db();
        let conn = db.blocking_lock();
        assert!(!update_fix(&conn, PHONE, ALICE, &fix(1.0, 2.0, 3.0), 100));
        assert_eq!(stored(&conn, PHONE, ALICE), None, "no row is created");

        upsert_fix(&conn, PHONE, ALICE, &fix(1.0, 2.0, 3.0), 100);
        assert!(update_fix(&conn, PHONE, ALICE, &fix(7.0, 8.0, 9.0), 300));
        assert_eq!(stored(&conn, PHONE, ALICE), Some((7.0, 8.0, 9.0, 300)));
        assert_eq!(stored(&conn, LAPTOP, ALICE), None);
    }

    #[test]
    fn fix_exists_follows_upsert_and_delete() {
        let db = seeded_db();
        let conn = db.blocking_lock();
        assert!(!fix_exists(&conn, PHONE, ALICE));
        upsert_fix(&conn, PHONE, ALICE, &fix(1.0, 2.0, 3.0), 100);
        upsert_fix(&conn, LAPTOP, BOB, &fix(1.0, 2.0, 3.0), 100);
        assert!(fix_exists(&conn, PHONE, ALICE));
        delete_fix(&conn, PHONE, ALICE);
        assert!(!fix_exists(&conn, PHONE, ALICE));
        assert!(fix_exists(&conn, LAPTOP, BOB), "another pair is untouched");
        // A second delete is a no-op, not a panic.
        delete_fix(&conn, PHONE, ALICE);
    }

    #[test]
    fn list_is_the_users_rows_most_recent_first_with_display_slugs() {
        let db = seeded_db();
        let conn = db.blocking_lock();
        conn.execute(
            "UPDATE device_users SET assigned_slug = 'pocket' WHERE device_id = ?1 AND user_id = ?2",
            rusqlite::params![PHONE, ALICE],
        )
        .unwrap();
        upsert_fix(&conn, LAPTOP, ALICE, &fix(1.0, 2.0, 3.0), 100);
        upsert_fix(&conn, PHONE, ALICE, &fix(4.0, 5.0, 6.0), 300);
        upsert_fix(&conn, LAPTOP, BOB, &fix(9.0, 9.0, 9.0), 500);

        let fixes = list_fixes_for_user(&conn, ALICE);
        let got: Vec<(&str, f64, i64)> = fixes
            .iter()
            .map(|f| (f.display_slug.as_str(), f.latitude, f.reported_at_ms))
            .collect();
        assert_eq!(got, vec![("pocket", 4.0, 300), ("laptop", 1.0, 100)]);
        assert_eq!(fixes[0].longitude, 5.0);
        assert_eq!(fixes[0].accuracy_m, 6.0);

        let bobs = list_fixes_for_user(&conn, BOB);
        assert_eq!(bobs.len(), 1);
        assert_eq!(bobs[0].device_user.user_id, BOB);
    }

    #[test]
    fn listed_device_user_matches_load_device_user() {
        let db = seeded_db();
        let conn = db.blocking_lock();
        set_tz_override(&conn, PHONE, ALICE, Some("Asia/Tokyo"), Some(4_000_000_000));
        set_browser_tz(&conn, PHONE, ALICE, Some("Europe/Berlin"));
        brenn_db::auth::device::touch_slug_prompted_at(&conn, PHONE, ALICE);
        upsert_fix(&conn, PHONE, ALICE, &fix(1.0, 2.0, 3.0), 100);

        let listed = &list_fixes_for_user(&conn, ALICE)[0].device_user;
        let loaded = load_device_user(&conn, PHONE, ALICE);
        assert_eq!(listed.device_id, loaded.device_id);
        assert_eq!(listed.user_id, loaded.user_id);
        assert_eq!(listed.assigned_slug, loaded.assigned_slug);
        assert_eq!(listed.tz_override.as_deref(), Some("Asia/Tokyo"));
        assert_eq!(listed.tz_override, loaded.tz_override);
        assert_eq!(listed.tz_override_expires_at, loaded.tz_override_expires_at);
        assert_eq!(listed.browser_tz.as_deref(), Some("Europe/Berlin"));
        assert_eq!(listed.browser_tz, loaded.browser_tz);
        assert!(listed.slug_prompted_at.is_some());
        assert_eq!(listed.slug_prompted_at, loaded.slug_prompted_at);
    }

    #[test]
    fn list_omits_an_unenrolled_device() {
        let db = seeded_db();
        let conn = db.blocking_lock();
        upsert_fix(&conn, PHONE, ALICE, &fix(1.0, 2.0, 3.0), 100);
        upsert_fix(&conn, TABLET, ALICE, &fix(1.0, 2.0, 3.0), 200);
        // Mark the tablet unenrolled without the unenroll transaction, which
        // would delete its row: the view alone must keep it out.
        conn.execute(
            "UPDATE devices SET unenrolled_at = 1 WHERE id = ?1",
            rusqlite::params![TABLET],
        )
        .unwrap();
        let slugs: Vec<String> = list_fixes_for_user(&conn, ALICE)
            .into_iter()
            .map(|f| f.display_slug)
            .collect();
        assert_eq!(slugs, vec!["phone".to_string()]);
    }
}
