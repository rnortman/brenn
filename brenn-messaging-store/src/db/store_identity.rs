//! The messaging store's durable identity: a per-DB generation UUID and a
//! per-boot incarnation counter.
//!
//! Both live in a single-row table (`id = 1`). The **generation** is minted once,
//! when the store is first created, and survives every server restart — it dies
//! only with the store itself (a wipe or a replacement mints a new one). The
//! **incarnation** is a monotone counter bumped exactly once per messenger boot;
//! within one store's history it only moves forward, so a value *above* the
//! store's current one can only come from a cursor minted under a boot the store
//! never counted — i.e. the store travelled backwards (restored from backup).
//!
//! These are the durable analogue of the ephemeral bus's per-boot epoch: the
//! ephemeral epoch is a single per-boot UUID because a memory store dies with
//! every boot; the durable epoch is the pair `(generation, incarnation)` because
//! a disk store can additionally be replaced or travel backwards. That is the one
//! difference the two classes are allowed to have — the store persists, so its
//! epoch must too.

use rusqlite::Connection;
use uuid::Uuid;

/// The messaging store's durable identity, read from the single-row identity
/// table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoreIdentity {
    /// Per-DB UUID, minted once at store creation, stable across restarts.
    pub generation: Uuid,
    /// Monotone counter, bumped once per messenger boot.
    pub incarnation: i64,
}

/// Create the identity table and mint the generation row if absent. Idempotent:
/// `CREATE ... IF NOT EXISTS` plus `INSERT OR IGNORE` leave an existing store's
/// generation untouched. Called once, from the schema migration; every read and
/// bump afterwards assumes the row exists and panics if it does not.
pub fn ensure_store_identity(conn: &Connection) {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS messaging_store_identity (
            id           INTEGER PRIMARY KEY CHECK(id = 1),
            generation   TEXT NOT NULL,
            incarnation  INTEGER NOT NULL
        );
        ",
    )
    .expect("failed to create messaging_store_identity table");
    conn.execute(
        "INSERT OR IGNORE INTO messaging_store_identity (id, generation, incarnation) \
         VALUES (1, ?1, 0)",
        rusqlite::params![Uuid::new_v4().to_string()],
    )
    .expect("failed to mint messaging_store_identity generation");
}

/// Read the store's current identity. The migration
/// ([`ensure_store_identity`]) guarantees the row on any booted store; a missing
/// table or row here is a skipped-migration bug and panics rather than being
/// silently repaired (which would mint a fresh generation and invalidate every
/// live cursor).
pub fn read_store_identity(conn: &Connection) -> StoreIdentity {
    conn.query_row(
        "SELECT generation, incarnation FROM messaging_store_identity WHERE id = 1",
        [],
        |row| {
            let generation: String = row.get(0)?;
            let incarnation: i64 = row.get(1)?;
            Ok((generation, incarnation))
        },
    )
    .map(|(generation, incarnation)| StoreIdentity {
        generation: Uuid::parse_str(&generation)
            .expect("messaging_store_identity.generation is a valid UUID"),
        incarnation,
    })
    .expect("messaging: read_store_identity")
}

/// Bump the incarnation once (per messenger boot) and return the new identity.
/// The generation is untouched; only the counter moves, always forward.
pub fn bump_incarnation(conn: &Connection) -> StoreIdentity {
    conn.execute(
        "UPDATE messaging_store_identity SET incarnation = incarnation + 1 WHERE id = 1",
        [],
    )
    .expect("messaging: bump_incarnation");
    read_store_identity(conn)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn conn() -> Connection {
        Connection::open_in_memory().unwrap()
    }

    #[test]
    fn generation_is_stable_and_incarnation_starts_at_zero() {
        let c = conn();
        ensure_store_identity(&c);
        let first = read_store_identity(&c);
        assert_eq!(first.incarnation, 0);
        let again = read_store_identity(&c);
        assert_eq!(first.generation, again.generation);
        assert_eq!(again.incarnation, 0);
    }

    #[test]
    fn bump_moves_incarnation_forward_and_keeps_generation() {
        let c = conn();
        ensure_store_identity(&c);
        let base = read_store_identity(&c);
        let bumped = bump_incarnation(&c);
        assert_eq!(bumped.generation, base.generation);
        assert_eq!(bumped.incarnation, 1);
        let again = bump_incarnation(&c);
        assert_eq!(again.incarnation, 2);
    }

    /// Reading a store whose identity row was never minted panics rather than
    /// silently repairing itself: a self-repair would mint a fresh generation and
    /// invalidate every live cursor, answering "the store was replaced" to pages
    /// whose store is intact.
    #[test]
    #[should_panic(expected = "read_store_identity")]
    fn reading_a_store_with_no_identity_row_panics() {
        let c = conn();
        ensure_store_identity(&c);
        c.execute("DELETE FROM messaging_store_identity", [])
            .unwrap();
        let _ = read_store_identity(&c);
    }

    /// Ensuring an already-minted store is a no-op: the generation and the
    /// incarnation both survive, so a re-run migration never re-anchors the store.
    #[test]
    fn ensure_is_idempotent_over_an_existing_store() {
        let c = conn();
        ensure_store_identity(&c);
        bump_incarnation(&c);
        let before = read_store_identity(&c);
        ensure_store_identity(&c);
        assert_eq!(read_store_identity(&c), before);
    }
}
