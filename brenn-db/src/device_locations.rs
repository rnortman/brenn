//! The `device_locations` table: its DDL and the device-lifecycle deletion
//! that runs inside the unenroll transaction.
//!
//! One row per `(device, user)`: the latest position fix the PWA reported
//! from that device for that user, stamped with the server's receipt time.
//! No history is kept. [`crate::auth::device::unenroll_device`] deletes this
//! table's rows inside the unenroll transaction — the atomicity invariant
//! forbids splitting the two writes, so the DDL lives in this crate alongside
//! the deletion. The read and write DAOs live with the tool that answers the
//! fix.

use rusqlite::Connection;

/// Run the `device_locations` table migration. Idempotent.
pub fn run_device_location_migrations(conn: &Connection) {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS device_locations (
            device_id       INTEGER NOT NULL REFERENCES devices(id),
            user_id         INTEGER NOT NULL REFERENCES users(id),
            latitude        REAL    NOT NULL,
            longitude       REAL    NOT NULL,
            accuracy_m      REAL    NOT NULL,
            -- Server receipt time, Unix ms; the fix's age counts from here.
            reported_at_ms  INTEGER NOT NULL,
            PRIMARY KEY (device_id, user_id)
        );
        CREATE INDEX IF NOT EXISTS idx_device_locations_user
            ON device_locations(user_id);
        ",
    )
    .expect("failed to run device_locations migrations");
}

/// Delete every location row for a given `device_id`, for every user.
///
/// Used by `unenroll_device` inside the unenroll transaction. 0..N rows
/// affected; no panic on 0.
pub fn delete_all_locations_for_device(conn: &Connection, device_id: i64) {
    conn.execute(
        "DELETE FROM device_locations WHERE device_id = ?1",
        rusqlite::params![device_id],
    )
    .expect("device_locations: delete_all_locations_for_device");
}
