//! `brenn config-status --db <PATH>`: the retained reload outcome, as JSON.
//!
//! The running process publishes every boot and every reload outcome onto
//! `brenn:config.status`, retained. This reads that row out of the durable
//! store and prints it — which is how an installer that has just asked for a
//! reload learns whether the document it installed is the one running, without
//! parsing the journal and without a second state store: the retained channel
//! *is* the state.
//!
//! Read-only and out of process, so it runs while the server holds the
//! database. WAL admits one writer and any number of readers, and this
//! connection never writes.

use std::path::Path;

use brenn_lib::messaging::config::Depth;
use brenn_messaging_store::db::{channel_uuid_by_address, load_channel_retained_tail};
use rusqlite::Connection;

/// Exit status when the status channel holds nothing to print: the facility is
/// undeclared, or this database was never booted against.
///
/// Two rather than one, because an installer polls this in a loop and has to
/// tell "no outcome yet" apart from "this invocation was wrong".
pub const CONFIG_STATUS_ABSENT: u8 = 2;

/// What the store had to say about the retained reload outcome.
#[derive(Debug, PartialEq, Eq)]
pub enum RetainedStatus {
    /// The retained body, verbatim.
    Body(String),
    /// No retained message; the string describes which case.
    Absent(String),
    /// The store could not be read at all.
    Error(String),
}

/// Print the retained body of `brenn:config.status` and return the process's
/// exit status: 0 with a body, [`CONFIG_STATUS_ABSENT`] without one, 1 when the
/// database could not be read as a brenn store at all.
pub fn run_config_status(db: &Path) -> u8 {
    match retained_status(db) {
        RetainedStatus::Body(body) => {
            println!("{body}");
            0
        }
        RetainedStatus::Absent(message) => {
            eprintln!("{message}");
            CONFIG_STATUS_ABSENT
        }
        RetainedStatus::Error(message) => {
            eprintln!("{message}");
            1
        }
    }
}

/// Read the newest retained message on `brenn:config.status`.
pub fn retained_status(db: &Path) -> RetainedStatus {
    let conn = match open_read_only(db) {
        Ok(conn) => conn,
        Err(message) => return RetainedStatus::Error(message),
    };

    let uuid = match channel_uuid_by_address(&conn, brenn_messaging::config_reload::STATUS_ADDRESS)
    {
        Ok(Some(uuid)) => uuid,
        Ok(None) => {
            return RetainedStatus::Absent(format!(
                "nothing to report: no channel {} in {}. The reload facility is \
                 declared by the document (a ConfigReload stamp), and the channel row \
                 is written the first time a process boots against this database.",
                brenn_messaging::config_reload::STATUS_ADDRESS,
                db.display(),
            ));
        }
        Err(e) => {
            return RetainedStatus::Error(format!(
                "Error: cannot read the channel table in {}: {e}. --db must name a \
                 brenn messaging store.",
                db.display(),
            ));
        }
    };

    let tail = load_channel_retained_tail(&conn, uuid, Depth::Bounded(1));
    match tail.last() {
        Some((_, envelope)) => RetainedStatus::Body(envelope.body.clone()),
        None => RetainedStatus::Absent(format!(
            "nothing to report: {} exists but retains no message. A process \
             publishes its outcome at boot, so this is a channel declared since \
             the last start.",
            brenn_messaging::config_reload::STATUS_ADDRESS,
        )),
    }
}

/// A reader's connection to the store, and the two ways it can fail said in
/// the operator's terms.
///
/// The open is the existence check: read-only without `SQLITE_OPEN_CREATE`
/// fails on a missing file, so asking the filesystem first would only add a
/// window in which the answer changes between the two calls.
fn open_read_only(db: &Path) -> Result<Connection, String> {
    brenn_db::open_connection_read_only(db).map_err(|e| match cant_open(&e) {
        true => format!(
            "Error: no database at {}. --db names the store the server was \
             started against (DB_PATH in the deploy conf).",
            db.display(),
        ),
        false => format!("Error: cannot open {} read-only: {e}", db.display()),
    })
}

/// Whether sqlite refused the open because there is nothing there to open —
/// which is the operator pointing `--db` at the wrong path, and the one arm
/// worth its own sentence.
fn cant_open(e: &rusqlite::Error) -> bool {
    matches!(
        e,
        rusqlite::Error::SqliteFailure(failure, _)
            if failure.code == rusqlite::ErrorCode::CannotOpen
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    use brenn_messaging::config_reload::STATUS_ADDRESS;
    use uuid::Uuid;

    /// A store with the messaging tables and the given bodies retained, oldest
    /// first, on the status channel. Built with SQL rather than through a booted
    /// messenger: what this tool reads is two tables, and a fixture that boots
    /// a process to write them would be testing the process.
    fn store(dir: &std::path::Path, bodies: &[&str]) -> std::path::PathBuf {
        let path = dir.join("brenn.db");
        {
            let conn = brenn_db::open_connection(&path);
            brenn_messaging_store::db::run_slice_migrations(&conn);
            let uuid = Uuid::new_v4();
            conn.execute(
                "INSERT INTO messaging_channels (uuid, address, created_at, resume_epoch)
                 VALUES (?1, ?2, '2026-09-06T00:00:00Z', ?3)",
                rusqlite::params![
                    uuid.as_bytes().to_vec(),
                    STATUS_ADDRESS,
                    Uuid::new_v4().as_bytes().to_vec()
                ],
            )
            .expect("insert channel");
            for (seq, body) in bodies.iter().enumerate() {
                let seq = i64::try_from(seq).expect("seq") + 1;
                conn.execute(
                    "INSERT INTO messaging_messages
                       (uuid, channel_uuid, source, sender, body, urgency, publish_ts_ns,
                        created_at, retained_seq)
                     VALUES (?1, ?2, 'system', 'system:config-reload', ?3, 'normal', ?4,
                             '2026-09-06T00:00:00Z', ?4)",
                    rusqlite::params![
                        Uuid::new_v4().as_bytes().to_vec(),
                        uuid.as_bytes().to_vec(),
                        body,
                        seq
                    ],
                )
                .expect("insert message");
            }
        }
        path
    }

    #[test]
    fn a_retained_body_is_printed_and_exits_zero() {
        let dir = tempfile::tempdir().unwrap();
        let path = store(dir.path(), &[r#"{"outcome":"applied"}"#]);
        assert_eq!(
            retained_status(&path),
            RetainedStatus::Body(r#"{"outcome":"applied"}"#.to_string())
        );
        assert_eq!(run_config_status(&path), 0);
    }

    /// The installer polls this to learn whether *its* reload landed, so the
    /// one message that may be reported is the newest.
    #[test]
    fn the_newest_retained_body_is_the_one_reported() {
        let dir = tempfile::tempdir().unwrap();
        let path = store(
            dir.path(),
            &[r#"{"outcome":"booted"}"#, r#"{"outcome":"applied"}"#],
        );
        assert_eq!(
            retained_status(&path),
            RetainedStatus::Body(r#"{"outcome":"applied"}"#.to_string())
        );
    }

    #[test]
    fn a_channel_with_no_retained_message_is_absent() {
        let dir = tempfile::tempdir().unwrap();
        let path = store(dir.path(), &[]);
        assert_eq!(run_config_status(&path), CONFIG_STATUS_ABSENT);
    }

    #[test]
    fn a_store_without_the_channel_is_absent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("brenn.db");
        brenn_messaging_store::db::run_slice_migrations(&brenn_db::open_connection(&path));
        assert_eq!(run_config_status(&path), CONFIG_STATUS_ABSENT);
    }

    /// A path that is not a database at all is a wrong invocation, not an
    /// absent outcome: an installer that treated the two alike would poll a
    /// typo for ninety seconds.
    #[test]
    fn a_file_that_is_not_a_store_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("not.db");
        std::fs::write(&path, b"not a database").unwrap();
        assert_eq!(run_config_status(&path), 1);
    }

    #[test]
    fn a_missing_database_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(run_config_status(&dir.path().join("absent.db")), 1);
    }

    /// The reader never writes, so it works on a database it has no permission
    /// to change — and, more to the point, cannot change one it does. Asserted
    /// on the connection rather than on the file's length, which most in-place
    /// writes leave alone.
    #[test]
    fn the_reader_cannot_write_the_store() {
        let dir = tempfile::tempdir().unwrap();
        let path = store(dir.path(), &[r#"{"outcome":"booted"}"#]);
        let conn = brenn_db::open_connection_read_only(&path).expect("open read-only");
        conn.execute("DELETE FROM messaging_channels", [])
            .expect_err("a read-only connection must refuse a write");
        drop(conn);
        assert_eq!(run_config_status(&path), 0);
    }
}
