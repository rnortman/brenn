//! The one table read the tool makes: the acting user's enrolled devices.
//!
//! Positions live on the user's location channel ([`crate::channel`]); this is
//! the join that decides which of the channel's devices the tool answers and
//! under what name. The caller holds the lock; an SQL error panics.

use brenn_db::auth::device::{DeviceUser, device_user_columns, device_user_from_row};
use rusqlite::Connection;

/// One of the user's devices as the tool answers it.
#[derive(Debug, Clone)]
pub struct UserDevice {
    /// The `(device, user)` row: the zone override included.
    pub device_user: DeviceUser,
    /// The user's name for the device: their assigned slug, else the guessed one.
    pub display_slug: String,
}

/// Every device paired with `user_id` that is still enrolled, by device id.
pub fn list_user_devices(conn: &Connection, user_id: i64) -> Vec<UserDevice> {
    let sql = format!(
        "SELECT {du_cols}, COALESCE(du.assigned_slug, d.guessed_slug)
         FROM device_users du
         JOIN active_devices d ON d.id = du.device_id
         WHERE du.user_id = ?1
         ORDER BY du.device_id",
        du_cols = device_user_columns("du"),
    );
    let mut stmt = conn
        .prepare(&sql)
        .expect("device-location: list_user_devices prepare");
    stmt.query_map(rusqlite::params![user_id], |row| {
        Ok(UserDevice {
            device_user: device_user_from_row(row, 0)?,
            display_slug: row.get(6)?,
        })
    })
    .expect("device-location: list_user_devices query")
    .collect::<Result<Vec<_>, _>>()
    .expect("device-location: list_user_devices row")
}

/// An in-memory database with the messaging slice, seeded for this crate's
/// tests.
///
/// Users: alice (1), bob (2). Devices: laptop (1), phone (2), tablet (3).
/// `device_users`: (laptop, alice), (phone, alice), (laptop, bob),
/// (tablet, alice).
#[cfg(test)]
pub(crate) fn seeded_db() -> brenn_db::Db {
    let conn = brenn_db::open_connection_memory();
    brenn_messaging::slice::run_slice_migrations(&conn);
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
    use brenn_db::auth::device::{load_device_user, set_tz_override};

    const ALICE: i64 = 1;
    const BOB: i64 = 2;
    const PHONE: i64 = 2;
    const TABLET: i64 = 3;

    #[test]
    fn lists_the_users_enrolled_devices_with_display_slugs() {
        let db = seeded_db();
        let conn = db.blocking_lock();
        conn.execute(
            "UPDATE device_users SET assigned_slug = 'pocket' WHERE device_id = ?1 AND user_id = ?2",
            rusqlite::params![PHONE, ALICE],
        )
        .unwrap();
        conn.execute(
            "UPDATE devices SET unenrolled_at = 1 WHERE id = ?1",
            rusqlite::params![TABLET],
        )
        .unwrap();
        let slugs: Vec<(i64, String)> = list_user_devices(&conn, ALICE)
            .into_iter()
            .map(|d| (d.device_user.device_id, d.display_slug))
            .collect();
        assert_eq!(slugs, vec![(1, "laptop".into()), (PHONE, "pocket".into())]);
        let bobs = list_user_devices(&conn, BOB);
        assert_eq!(bobs.len(), 1);
        assert_eq!(bobs[0].device_user.user_id, BOB);
    }

    #[test]
    fn the_listed_row_matches_load_device_user() {
        let db = seeded_db();
        let conn = db.blocking_lock();
        set_tz_override(&conn, PHONE, ALICE, Some("Asia/Tokyo"), Some(4_000_000_000));
        brenn_db::auth::device::touch_slug_prompted_at(&conn, PHONE, ALICE);
        let listed = list_user_devices(&conn, ALICE)
            .into_iter()
            .find(|d| d.device_user.device_id == PHONE)
            .unwrap()
            .device_user;
        let loaded = load_device_user(&conn, PHONE, ALICE);
        assert_eq!(listed.tz_override.as_deref(), Some("Asia/Tokyo"));
        assert_eq!(listed.tz_override, loaded.tz_override);
        assert_eq!(listed.tz_override_expires_at, loaded.tz_override_expires_at);
        assert!(listed.slug_prompted_at.is_some());
        assert_eq!(listed.slug_prompted_at, loaded.slug_prompted_at);
        assert_eq!(listed.assigned_slug, loaded.assigned_slug);
    }
}
