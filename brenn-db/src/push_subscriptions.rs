//! The `pwa_push_subscriptions` table: its DDL and the device-lifecycle
//! deletion that runs inside the unenroll transaction.
//!
//! [`crate::auth::device::unenroll_device`] deletes this table's rows inside
//! the unenroll transaction — the atomicity invariant forbids splitting the
//! two writes, so the DDL must live in this crate alongside the deletion.

use rusqlite::Connection;

/// Run the `pwa_push_subscriptions` table migration. Idempotent.
pub fn run_push_subscription_migrations(conn: &Connection) {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS pwa_push_subscriptions (
            id              INTEGER PRIMARY KEY,
            device_id       INTEGER NOT NULL REFERENCES devices(id),
            user_id         INTEGER NOT NULL REFERENCES users(id),
            endpoint        TEXT NOT NULL,
            p256dh_b64url   TEXT NOT NULL,
            auth_b64url     TEXT NOT NULL,
            created_at      TEXT NOT NULL,
            last_used_at    TEXT NOT NULL,
            UNIQUE (device_id, user_id)
        );
        CREATE INDEX IF NOT EXISTS idx_pwa_push_subscriptions_user
            ON pwa_push_subscriptions(user_id);
        ",
    )
    .expect("failed to run pwa_push migrations");
}

/// Delete all subscription rows for a given `device_id`.
///
/// Used by `unenroll_device` to atomically clean up all push subscriptions
/// for a device as part of the unenroll transaction. 0..N rows affected;
/// no panic on 0 (idempotent if called twice or for a device with no subs).
pub fn delete_all_subscriptions_for_device(conn: &Connection, device_id: i64) {
    conn.execute(
        "DELETE FROM pwa_push_subscriptions WHERE device_id = ?1",
        rusqlite::params![device_id],
    )
    .expect("pwa_push: delete_all_subscriptions_for_device");
}
