//! The durable half of a blob's lifetime: adoption by a durable message row,
//! redemption against the database, the reap, and the reconciliation at open,
//! with the directory's owner record.
//!
//! Every function here that takes a `Connection` runs with the database lock
//! held by its caller. The database is always read before the index lock is
//! taken, so the index stays a leaf lock.

use std::collections::{BTreeMap, HashMap};
use std::fs::{self, DirBuilder, File};
use std::os::unix::fs::DirBuilderExt;
use std::path::Path;
use std::sync::{Arc, Mutex};

use rusqlite::{Connection, OptionalExtension};
use uuid::Uuid;

use brenn_envelope::BlobId;

use super::{
    BlobError, BlobHold, BlobLimits, BlobStore, Entry, HeldAttachments, Index, Life, is_minted_id,
    unlink,
};

/// What one [`BlobStore::reap`] deleted.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReapReport {
    pub blobs: usize,
    pub bytes: u64,
}

/// The `PRAGMA synchronous` value of `FULL`.
const SYNCHRONOUS_FULL: i64 = 2;

/// Where an operator reads how to start a host that refuses.
const RESTORE_DOC: &str = "docs/message-bus.md, the blob section's backup and restore paragraph";

impl BlobStore {
    /// Open the store over `dir`, reconciling the database's blob records with
    /// the directory, and return it with every surviving blob indexed
    /// *Durable*.
    ///
    /// Creates `dir` with mode 0700 if absent. Checks the directory's `owner`
    /// record against the database's store generation, and claims a directory
    /// that holds no record and no blob file by writing one. Then deletes every
    /// blob row no message names, with its file, and every file no row names:
    /// every blob only ring messages named, every unsealed blob and every
    /// sealed blob never published before the restart.
    ///
    /// # Panics
    ///
    /// Refuses to start, naming the blob and the messages, when `PRAGMA
    /// synchronous` is below `FULL`; when another open store, in this process
    /// or another, holds `dir`'s lock; when `dir` holds an entry that is not a
    /// regular file named by 32 lowercase hex characters, its `owner` record,
    /// or a regular file `owner.tmp`; when the `owner` record names another
    /// database's generation, is not a regular file holding one generation
    /// line, or is absent beside blob files; when a message names
    /// a blob that has no reference row or no blob row; when a referenced blob
    /// has no file or a file of the wrong length; or when a blob's `refs`
    /// disagrees with its reference rows or a reference row names a message
    /// that does not exist.
    pub fn open(dir: &Path, limits: BlobLimits, conn: &Connection) -> Arc<BlobStore> {
        assert!(
            conn.is_autocommit(),
            "BlobStore::open called with a transaction open"
        );
        let synchronous: i64 = conn
            .query_row("PRAGMA synchronous", [], |row| row.get(0))
            .expect("read PRAGMA synchronous");
        assert!(
            synchronous >= SYNCHRONOUS_FULL,
            "refusing to start: PRAGMA synchronous is {synchronous}, below FULL; the blob \
             store deletes files only after the row deletion naming them is durable"
        );

        DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(dir)
            .unwrap_or_else(|e| panic!("creating blob directory {}: {e}", dir.display()));
        let dir_lock = lock_dir(dir);
        let listing = list_dir(dir);
        let generation = crate::db::read_store_identity(conn).generation;
        check_or_claim_owner(dir, &listing, generation);
        let files = listing.blobs;

        check_message_references(conn);
        check_reference_counts(conn);
        let rows = load_blob_rows(conn);
        for row in rows.values().filter(|row| row.refs > 0) {
            let id = BlobId::new(row.id.clone());
            match files.get(&row.id) {
                None => refuse(conn, &id, "has no file in the blob directory".to_owned()),
                Some(&len) if len != row.size => refuse(
                    conn,
                    &id,
                    format!("has a file of {len} bytes; its row says {}", row.size),
                ),
                Some(_) => {}
            }
        }

        // The sweep. Rows are deleted before files are unlinked, so a crash
        // between the two leaves files with no row, which the next open removes.
        let unreferenced: Vec<&String> = rows
            .values()
            .filter(|row| row.refs == 0)
            .map(|row| &row.id)
            .collect();
        if !unreferenced.is_empty() {
            let tx = conn
                .unchecked_transaction()
                .expect("blob sweep: begin transaction");
            for id in &unreferenced {
                tx.execute(
                    "DELETE FROM messaging_blobs WHERE id = ?1 AND refs = 0",
                    [id],
                )
                .expect("blob sweep: delete unreferenced row");
            }
            tx.commit().expect("blob sweep: commit");
        }
        let rows_swept = unreferenced.len();
        let mut index = Index::default();
        let mut files_unlinked = 0usize;
        for (name, _) in files {
            match rows.get(&name) {
                Some(row) if row.refs > 0 => {}
                _ => {
                    unlink(&dir.join(&name));
                    files_unlinked += 1;
                }
            }
        }
        for row in rows.into_values().filter(|row| row.refs > 0) {
            index.charged += row.size;
            index.entries.insert(
                BlobId::new(row.id),
                Entry {
                    extent: row.size,
                    content_type: row.content_type,
                    holds: Some(0),
                    life: Life::Durable,
                    synced: true,
                },
            );
        }
        tracing::info!(
            dir = %dir.display(),
            rows_swept,
            files_unlinked,
            blobs_loaded = index.entries.len(),
            charged_bytes = index.charged,
            "blob store opened"
        );
        if index.charged > limits.budget_bytes {
            tracing::warn!(
                charged = index.charged,
                budget = limits.budget_bytes,
                "blob store opened above its budget; creates and extending writes are refused \
                 until deletions bring it under"
            );
        }

        Arc::new(BlobStore {
            dir: dir.to_owned(),
            limits,
            generation: Some(generation),
            dir_lock: Some(dir_lock),
            index: Mutex::new(index),
        })
    }

    /// Redeem `id` against the database: a hold if the blob has an in-memory
    /// hold or a durable row names it, `NotFound` otherwise.
    ///
    /// Called with the database lock held, after [`BlobStore::retain`]
    /// answered `Unsettled`. Inserts, deletes and the reap all run under the
    /// same lock, so the count read here cannot move before the hold is taken.
    pub fn retain_settled(
        self: &Arc<Self>,
        id: &BlobId,
        conn: &Connection,
    ) -> Result<BlobHold, BlobError> {
        let refs: Option<i64> = conn
            .query_row(
                "SELECT refs FROM messaging_blobs WHERE id = ?1",
                [id.as_str()],
                |row| row.get(0),
            )
            .optional()
            .expect("retain_settled: read refs");
        let mut index = self.lock();
        let Some(entry) = index.entries.get_mut(id) else {
            assert!(refs.is_none(), "blob row {id} has no index entry");
            return Err(BlobError::NotFound);
        };
        let Some(holds) = entry.holds.as_mut() else {
            return Err(BlobError::NotFound);
        };
        if *holds == 0 && refs.unwrap_or(0) == 0 {
            return Err(BlobError::NotFound);
        }
        Ok(self.take_hold(id, entry))
    }

    /// Delete every blob that neither an in-memory hold nor a durable row
    /// holds, in one transaction of its own: rows first, committed, then the
    /// files, then their charge.
    ///
    /// Candidates are the rows with `refs = 0` and every *Adopting* entry
    /// whose last hold dropped (an insert that rolled back). A candidate a row
    /// still names is marked *Durable* and kept.
    ///
    /// # Panics
    ///
    /// If `conn` has a transaction open: files would be unlinked before that
    /// transaction commits, and a rollback would restore rows naming them.
    pub fn reap(&self, conn: &Connection) -> ReapReport {
        assert!(
            conn.is_autocommit(),
            "BlobStore::reap called with a transaction open"
        );
        // Each candidate with its `refs`, `None` for a blob with no row.
        let mut counted: BTreeMap<BlobId, Option<i64>> = {
            let mut stmt = conn
                .prepare_cached("SELECT id FROM messaging_blobs WHERE refs = 0")
                .expect("reap: prepare candidates");
            stmt.query_map([], |row| row.get::<_, String>(0))
                .expect("reap: query candidates")
                .map(|r| (BlobId::new(r.expect("reap: candidate row")), Some(0)))
                .collect()
        };
        let unheld_adopting = std::mem::take(&mut self.lock().unheld_adopting);
        for id in unheld_adopting {
            if counted.contains_key(&id) {
                continue;
            }
            let refs = conn
                .query_row(
                    "SELECT refs FROM messaging_blobs WHERE id = ?1",
                    [id.as_str()],
                    |row| row.get(0),
                )
                .optional()
                .expect("reap: read refs");
            counted.insert(id, refs);
        }
        if counted.is_empty() {
            return ReapReport::default();
        }

        let mut doomed: Vec<(BlobId, u64, bool)> = Vec::new();
        {
            let mut index = self.lock();
            for (id, refs) in counted {
                let entry = index
                    .entries
                    .get_mut(&id)
                    .unwrap_or_else(|| panic!("blob row {id} has no index entry"));
                assert_ne!(
                    entry.life,
                    Life::Volatile,
                    "volatile blob {id} has a database row"
                );
                let holds = entry
                    .holds
                    .unwrap_or_else(|| panic!("unsealed blob {id} has a database row"));
                if holds > 0 {
                    continue;
                }
                if refs.is_some_and(|refs| refs > 0) {
                    entry.life = Life::Durable;
                    continue;
                }
                let extent = entry.extent;
                index.entries.remove(&id);
                index.unheld_adopting.remove(&id);
                doomed.push((id, extent, refs.is_some()));
            }
        }
        if doomed.is_empty() {
            return ReapReport::default();
        }

        let tx = conn
            .unchecked_transaction()
            .expect("reap: begin transaction");
        for (id, _, has_row) in &doomed {
            if *has_row {
                let deleted = tx
                    .execute(
                        "DELETE FROM messaging_blobs WHERE id = ?1 AND refs = 0",
                        [id.as_str()],
                    )
                    .expect("reap: delete row");
                assert_eq!(deleted, 1, "reap: blob row {id} changed under the lock");
            }
        }
        tx.commit().expect("reap: commit");

        let mut report = ReapReport::default();
        for (id, extent, _) in &doomed {
            unlink(&self.path_of(id));
            report.blobs += 1;
            report.bytes += extent;
        }
        let mut index = self.lock();
        index.charged = index
            .charged
            .checked_sub(report.bytes)
            .expect("blob store charge underflow");
        drop(index);
        tracing::info!(
            blobs = report.blobs,
            bytes = report.bytes,
            "blob reap deleted blobs nothing holds"
        );
        report
    }
}

impl HeldAttachments {
    /// Make every blob named here durable on disk: sync each blob's file not
    /// yet synced, then the directory once. Called before the database lock is
    /// taken for the insert that names them.
    ///
    /// A blob is marked synced only once its directory has been synced too,
    /// so a concurrent call for the same blob repeats both syncs rather than
    /// letting its insert name a file whose directory entry may not be on
    /// disk.
    ///
    /// # Panics
    ///
    /// On a failed sync. The publish this runs in has no refusal for a
    /// storage failure, as the database commit of the same publish has none;
    /// after a failed sync the bytes' state on disk is unknown, so neither
    /// retrying the sync nor writing the row that names them is sound.
    // TODO(blob-durable-sync-on-async-task): every caller runs this on an async
    // task, which stops for each sync.
    pub fn make_durable(&self) {
        let mut dirs: Vec<std::path::PathBuf> = Vec::new();
        let mut synced: Vec<&BlobHold> = Vec::new();
        for hold in self.holds() {
            let store = &hold.store;
            if store
                .lock()
                .entries
                .get(hold.id())
                .is_some_and(|e| e.synced)
            {
                continue;
            }
            let path = store.path_of(hold.id());
            File::open(&path)
                .and_then(|f| f.sync_all())
                .unwrap_or_else(|e| panic!("syncing blob file {}: {e}", path.display()));
            if !dirs.contains(&store.dir) {
                dirs.push(store.dir.clone());
            }
            synced.push(hold);
        }
        for dir in dirs {
            File::open(&dir)
                .and_then(|f| f.sync_all())
                .unwrap_or_else(|e| panic!("syncing blob directory {}: {e}", dir.display()));
        }
        for hold in synced {
            hold.store
                .lock()
                .entries
                .get_mut(hold.id())
                .expect("held blob is indexed")
                .synced = true;
        }
    }

    /// Mark every blob named here *Durable*, after the transaction that adopted
    /// them has committed.
    pub fn confirm_adopted(&self) {
        for hold in self.holds() {
            let mut index = hold.store.lock();
            let entry = index
                .entries
                .get_mut(hold.id())
                .expect("held blob is indexed");
            assert_ne!(
                entry.life,
                Life::Volatile,
                "confirming blob {} that no insert adopted",
                hold.id()
            );
            entry.life = Life::Durable;
        }
    }
}

/// Adopt `attachments` for message row `message_id`, in the caller's
/// transaction: mark each blob *Adopting*, write its row if it has none, and
/// write one reference row and one `refs` increment per distinct blob.
///
/// # Panics
///
/// If a blob was not synced by [`HeldAttachments::make_durable`], or if a
/// hold's store did not open against this database.
pub(crate) fn adopt_in_tx(conn: &Connection, message_id: i64, attachments: &HeldAttachments) {
    if attachments.is_empty() {
        return;
    }
    let generation = crate::db::read_store_identity(conn).generation;
    for hold in attachments.holds() {
        let id = hold.id();
        assert_eq!(
            hold.store.generation,
            Some(generation),
            "blob {id} is held by a store that did not open against this database"
        );
        {
            let mut index = hold.store.lock();
            let entry = index.entries.get_mut(id).expect("held blob is indexed");
            assert!(entry.synced, "blob {id} adopted before it was synced");
            if entry.life == Life::Volatile {
                entry.life = Life::Adopting;
            }
        }
        let reference = hold.reference();
        conn.execute(
            "INSERT INTO messaging_blobs (id, size, content_type) VALUES (?1, ?2, ?3)
             ON CONFLICT(id) DO NOTHING",
            rusqlite::params![
                id.as_str(),
                i64::try_from(reference.size).expect("blob size fits in i64"),
                reference.content_type
            ],
        )
        .expect("adopt: insert blob row");
        conn.execute(
            "INSERT INTO messaging_blob_refs (message_id, blob_id) VALUES (?1, ?2)",
            rusqlite::params![message_id, id.as_str()],
        )
        .expect("adopt: insert reference row");
        conn.execute(
            "UPDATE messaging_blobs SET refs = refs + 1 WHERE id = ?1",
            [id.as_str()],
        )
        .expect("adopt: count reference");
    }
}

struct BlobRow {
    id: String,
    size: u64,
    content_type: String,
    refs: i64,
}

fn load_blob_rows(conn: &Connection) -> BTreeMap<String, BlobRow> {
    let mut stmt = conn
        .prepare("SELECT id, size, content_type, refs FROM messaging_blobs")
        .expect("blob open: prepare rows");
    stmt.query_map([], |row| {
        let size: i64 = row.get(1)?;
        Ok(BlobRow {
            id: row.get(0)?,
            size: u64::try_from(size).expect("blob open: negative size"),
            content_type: row.get(2)?,
            refs: row.get(3)?,
        })
    })
    .expect("blob open: query rows")
    .map(|r| {
        let row = r.expect("blob open: read row");
        (row.id.clone(), row)
    })
    .collect()
}

/// Take an exclusive, non-blocking `flock` on `dir` itself, which adds no
/// entry to it. The open sweep deletes every file its database does not name,
/// so a second store over a directory a live store holds would delete that
/// store's blobs: refused instead.
fn lock_dir(dir: &Path) -> File {
    let lock = File::open(dir)
        .unwrap_or_else(|e| panic!("opening blob directory {} to lock it: {e}", dir.display()));
    match lock.try_lock() {
        Ok(()) => lock,
        Err(std::fs::TryLockError::WouldBlock) => panic!(
            "refusing to start: blob directory {} is held by another open blob store; two \
             hosts, or two stores, must not share a blob directory",
            dir.display()
        ),
        Err(std::fs::TryLockError::Error(e)) => {
            panic!("locking blob directory {}: {e}", dir.display())
        }
    }
}

/// The record in a blob directory naming the database that owns it: the
/// database's store generation, hyphenated lowercase, and a newline.
const OWNER_FILE: &str = "owner";

/// The name the owner record is written under before it is renamed into place.
const OWNER_TMP_FILE: &str = "owner.tmp";

/// The longest owner record read: one generation line is 37 bytes.
const OWNER_MAX_BYTES: u64 = 64;

/// What a blob directory holds.
struct DirListing {
    /// Every blob file, by name, with its length.
    blobs: HashMap<String, u64>,
    /// `Some(true)` for a regular file `owner`, `Some(false)` for an `owner`
    /// of any other type, `None` without one.
    owner: Option<bool>,
    /// Whether a regular file `owner.tmp` is present.
    owner_tmp: bool,
}

/// List `dir`: its blob files, its owner record and the record's temporary.
///
/// # Panics
///
/// On an entry that is none of those three — a regular file named as a blob
/// id, an entry named `owner`, a regular file named `owner.tmp`: the sweep
/// deletes files, and it deletes nothing it does not recognise.
fn list_dir(dir: &Path) -> DirListing {
    let entries = fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("reading blob directory {}: {e}", dir.display()));
    let mut listing = DirListing {
        blobs: HashMap::new(),
        owner: None,
        owner_tmp: false,
    };
    for entry in entries {
        let entry =
            entry.unwrap_or_else(|e| panic!("reading blob directory {}: {e}", dir.display()));
        let path = entry.path();
        let meta = fs::symlink_metadata(&path)
            .unwrap_or_else(|e| panic!("reading {}: {e}", path.display()));
        let is_file = meta.file_type().is_file();
        match entry.file_name().to_str() {
            Some(OWNER_FILE) => listing.owner = Some(is_file),
            Some(OWNER_TMP_FILE) if is_file => listing.owner_tmp = true,
            Some(name) if is_file && is_minted_id(name) => {
                listing.blobs.insert(name.to_owned(), meta.len());
            }
            _ => panic!(
                "refusing to start: the blob directory holds {}, which is not a blob file \
                 or its owner record; move it out of the directory",
                path.display()
            ),
        }
    }
    listing
}

/// Refuse unless `dir`'s owner record names `generation`; claim a directory
/// with no record and no blob file. Unlinks a leftover `owner.tmp` on both
/// paths that go on and touches nothing on a refusal.
///
/// # Panics
///
/// On a record naming another generation, on a record that is not a regular
/// file holding one generation line, and on blob files with no record: in
/// each case the host cannot tell that the files are this database's, and the
/// open sweep would delete them.
fn check_or_claim_owner(dir: &Path, listing: &DirListing, generation: Uuid) {
    let owner = dir.join(OWNER_FILE);
    match listing.owner {
        Some(true) => {
            let recorded = read_owner(&owner).unwrap_or_else(|| {
                panic!(
                    "refusing to start: the blob directory's owner record {} does not hold one \
                     store generation; if this database owns the directory, write its \
                     generation back: sqlite3 <database> \"SELECT generation FROM \
                     messaging_store_identity\" > {}; see {RESTORE_DOC}",
                    owner.display(),
                    owner.display()
                )
            });
            if recorded != generation {
                panic!(
                    "refusing to start: the blob directory {} belongs to the database of store \
                     generation {recorded}, and this database's generation is {generation}; \
                     its files are another store's: remove the directory, or point \
                     messaging blob_dir elsewhere, for this database to claim a new one; \
                     see {RESTORE_DOC}",
                    dir.display()
                );
            }
        }
        Some(false) => panic!(
            "refusing to start: the blob directory's owner record {} is not a regular file; \
             see {RESTORE_DOC}",
            owner.display()
        ),
        None if !listing.blobs.is_empty() => panic!(
            "refusing to start: the blob directory {} holds {} blob files and no owner \
             record, so the host cannot tell which database they belong to; if it is this \
             one, write its generation: sqlite3 <database> \"SELECT generation FROM \
             messaging_store_identity\" > {}; otherwise move the directory away; see \
             {RESTORE_DOC}",
            dir.display(),
            listing.blobs.len(),
            owner.display()
        ),
        None => {
            if listing.owner_tmp {
                unlink(&dir.join(OWNER_TMP_FILE));
            }
            claim(dir, generation);
            tracing::info!(dir = %dir.display(), %generation, "blob directory claimed");
            return;
        }
    }
    if listing.owner_tmp {
        unlink(&dir.join(OWNER_TMP_FILE));
    }
}

/// The generation an owner record holds, `None` when it does not hold exactly
/// one generation line.
fn read_owner(path: &Path) -> Option<Uuid> {
    let file =
        File::open(path).unwrap_or_else(|e| panic!("opening owner record {}: {e}", path.display()));
    let mut bytes = Vec::new();
    std::io::Read::read_to_end(
        &mut std::io::Read::take(&file, OWNER_MAX_BYTES + 1),
        &mut bytes,
    )
    .unwrap_or_else(|e| panic!("reading owner record {}: {e}", path.display()));
    let line = std::str::from_utf8(&bytes).ok()?.strip_suffix('\n')?;
    let generation = Uuid::parse_str(line).ok()?;
    (generation.hyphenated().to_string() == line).then_some(generation)
}

/// Write `dir`'s owner record so that `owner` is absent or whole at every
/// instant: write `owner.tmp`, sync it, rename it into place, sync the
/// directory.
fn claim(dir: &Path, generation: Uuid) {
    use std::io::Write as _;
    use std::os::unix::fs::OpenOptionsExt as _;
    let tmp = dir.join(OWNER_TMP_FILE);
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&tmp)
        .unwrap_or_else(|e| panic!("creating {}: {e}", tmp.display()));
    file.write_all(format!("{generation}\n").as_bytes())
        .and_then(|()| file.sync_all())
        .unwrap_or_else(|e| panic!("writing {}: {e}", tmp.display()));
    drop(file);
    let owner = dir.join(OWNER_FILE);
    fs::rename(&tmp, &owner)
        .unwrap_or_else(|e| panic!("renaming {} to {}: {e}", tmp.display(), owner.display()));
    File::open(dir)
        .and_then(|f| f.sync_all())
        .unwrap_or_else(|e| panic!("syncing blob directory {}: {e}", dir.display()));
}

/// Refuse unless every blob a message's `attachments` names has a reference row and
/// a blob row.
fn check_message_references(conn: &Connection) {
    let mut stmt = conn
        .prepare(
            "SELECT id, uuid, attachments FROM messaging_messages WHERE attachments IS NOT NULL",
        )
        .expect("blob open: prepare messages");
    let rows: Vec<(i64, Vec<u8>, String)> = stmt
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        .expect("blob open: query messages")
        .map(|r| r.expect("blob open: read message"))
        .collect();
    for (message_id, uuid, text) in rows {
        let uuid = Uuid::from_slice(&uuid).expect("blob open: message uuid");
        let attachments = crate::db::parse_attachments(&text).unwrap_or_else(|e| {
            panic!("refusing to start: message {uuid} has attachments that do not parse: {e}")
        });
        for (_, reference) in attachments.iter() {
            let id = reference.id.as_str();
            let has_ref: bool = conn
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM messaging_blob_refs
                                   WHERE message_id = ?1 AND blob_id = ?2)",
                    rusqlite::params![message_id, id],
                    |row| row.get(0),
                )
                .expect("blob open: read reference row");
            let has_blob: bool = conn
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM messaging_blobs WHERE id = ?1)",
                    [id],
                    |row| row.get(0),
                )
                .expect("blob open: read blob row");
            if !has_ref || !has_blob {
                panic!(
                    "refusing to start: message {uuid} names blob {id}, which has no {}; \
                     see {RESTORE_DOC}",
                    if has_ref { "blob row" } else { "reference row" }
                );
            }
        }
    }
}

/// Refuse unless every blob's `refs` equals its reference rows, and every reference
/// row names a message that exists.
fn check_reference_counts(conn: &Connection) {
    let wrong: Option<(String, i64, i64)> = conn
        .query_row(
            "SELECT b.id, b.refs,
                    (SELECT COUNT(*) FROM messaging_blob_refs r WHERE r.blob_id = b.id) AS n
             FROM messaging_blobs b WHERE b.refs != n LIMIT 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .expect("blob open: check counts");
    if let Some((id, refs, n)) = wrong {
        panic!(
            "refusing to start: blob {id} records {refs} references and {n} reference rows \
             name it; see {RESTORE_DOC}"
        );
    }
    let orphan: Option<(i64, String)> = conn
        .query_row(
            "SELECT r.message_id, r.blob_id FROM messaging_blob_refs r
             WHERE NOT EXISTS (SELECT 1 FROM messaging_messages m WHERE m.id = r.message_id)
             LIMIT 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .expect("blob open: check reference rows");
    if let Some((message_id, id)) = orphan {
        panic!(
            "refusing to start: a reference row names blob {id} for message row {message_id}, \
             which does not exist; see {RESTORE_DOC}"
        );
    }
}

/// Refuse to start over a referenced blob whose file is missing or the wrong
/// length, naming the messages that name it.
fn refuse(conn: &Connection, id: &BlobId, what: String) -> ! {
    let mut stmt = conn
        .prepare(
            "SELECT m.uuid FROM messaging_blob_refs r
             JOIN messaging_messages m ON m.id = r.message_id
             WHERE r.blob_id = ?1 ORDER BY m.id",
        )
        .expect("blob open: prepare naming messages");
    let messages: Vec<String> = stmt
        .query_map([id.as_str()], |row| row.get::<_, Vec<u8>>(0))
        .expect("blob open: query naming messages")
        .map(|r| {
            Uuid::from_slice(&r.expect("blob open: naming message"))
                .expect("blob open: message uuid")
                .to_string()
        })
        .collect();
    panic!(
        "refusing to start: blob {id} {what}, and messages [{}] name it; delete those message \
         rows to start anyway; see {RESTORE_DOC}",
        messages.join(", ")
    );
}
