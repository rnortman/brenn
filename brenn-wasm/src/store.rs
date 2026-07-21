// SQLite-backed KV store implementation for the WASM host capability.
//
// The component-model contract is KV+tx (see WIT store interface). SQLite is
// the backend. The component never sees SQL; it sees typed get/put/delete/scan
// operations inside explicit transactions.
//
// Naming: the SQLite-backed type is KvStore (not Store) to avoid colliding with
// wasmtime::Store, which is imported in lib.rs.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

use brenn_common::PAGE_SIZE;
use rusqlite::Connection;
use wasmtime::component::{Resource, ResourceTable};

use crate::StoreError;

/// Default `max_page_count` for test-only callers that need a generous cap.
///
/// Derived from a 64 MiB ceiling (matching `WasmConfig`'s default). Test
/// helpers pass this value so existing store tests are unaffected by the
/// required `max_page_count` parameter on `KvStore::open`.
pub const DEFAULT_MAX_PAGE_COUNT: u32 = (64 * 1024 * 1024 / PAGE_SIZE) as u32; // 16384

// Process-global guard: each store file may be open by at most one KvStore.
// Enforces the "one file per ReplayComponent" invariant in code, not just prose.
static OPEN_PATHS: Mutex<Option<HashSet<PathBuf>>> = Mutex::new(None);

fn register_path(path: PathBuf) {
    let already_open = {
        let mut guard = OPEN_PATHS
            .lock()
            .unwrap_or_else(|e| panic!("OPEN_PATHS mutex poisoned: {e}"));
        let set = guard.get_or_insert_with(HashSet::new);
        // Return the path to use in the panic message outside the lock so the
        // MutexGuard is dropped before the panic fires. A panic while holding
        // a MutexGuard poisons the mutex, which would break all subsequent
        // callers for the process lifetime.
        if set.insert(path.clone()) {
            None
        } else {
            Some(path)
        }
    };
    if let Some(dup) = already_open {
        panic!(
            "KvStore path already open: {}. Each ReplayComponent must use a distinct store file.",
            dup.display()
        );
    }
}

fn unregister_path(path: &Path) {
    let mut guard = OPEN_PATHS
        .lock()
        .unwrap_or_else(|e| panic!("OPEN_PATHS mutex poisoned: {e}"));
    if let Some(set) = guard.as_mut() {
        set.remove(path);
    }
}

/// SQLite-backed KV store for one ReplayComponent.
///
/// One file, one connection, one mutex. Iter-2 has at most one in-flight
/// check call per ReplayComponent (test harness is sequential).
///
/// std::sync::Mutex (not tokio::sync::Mutex) because all bindgen host traits
/// are sync: no .await ever happens while the lock is held. Iter-3 that wires
/// this into an async webhook handler will use spawn_blocking or equivalent.
pub struct KvStore {
    // pub(crate) rather than pub: callers outside this crate must go through
    // the typed KvStore helpers (tx_get, tx_put, etc.) or the WIT host-trait
    // impls in lib.rs. Direct field access bypasses the tx_active discipline.
    pub(crate) conn: Mutex<Connection>,
    /// Nested-begin guard and leak detector. Set in begin, cleared in
    /// commit / rollback ONLY. HostTransaction::drop reads this flag: still
    /// set when drop fires = transaction leaked by the component.
    pub(crate) tx_active: AtomicBool,
    /// Set to `true` when `tx_put` detects `SQLITE_FULL` (quota exceeded).
    /// Read-and-cleared by `ReplayComponent::check` to drive the phone alert.
    /// `AtomicBool` (not guarded by conn mutex) so the flag can be read
    /// outside a lock after the check call completes.
    pub(crate) quota_hit: AtomicBool,
    /// Set to `true` when `tx_put` detects `SQLITE_FULL`. SQLite automatically
    /// rolls back the statement (and sometimes the whole transaction) on
    /// `SQLITE_FULL`, so a subsequent `ROLLBACK` may return an error about
    /// "no transaction is active". This flag lets the `rollback` host-trait
    /// method suppress that specific case without string-matching the error
    /// message. Cleared in `clear_tx` alongside `tx_active`.
    pub(crate) auto_rolled_back: AtomicBool,
    /// The `PRAGMA max_page_count` value applied at open; stored for structured
    /// log output on quota-hit events.
    pub(crate) max_page_count: u32,
    path: PathBuf,
}

impl KvStore {
    /// Open (or create) the SQLite file at `path` with a host-enforced page
    /// cap of `max_page_count`.
    ///
    /// Creates the store file if it does not yet exist (the canonical creation
    /// site — `resolve_replay_protection` only validates the parent directory).
    /// Registers `path` in the process-global open-paths set; panics if the
    /// path is already open (enforces "one file per ReplayComponent").
    /// Panics on any I/O or DDL failure (CLAUDE.md fail-fast).
    ///
    /// `max_page_count` is applied via `PRAGMA max_page_count` immediately after
    /// the standard pragmas. It is a per-connection setting (not persisted in the
    /// db file) and must be re-applied every open. Because `KvStore` holds a
    /// single long-lived `Connection` opened once per process, setting it once
    /// here is correct.
    pub fn open(path: &Path, max_page_count: u32) -> Arc<Self> {
        // Path dedup uses string equality. `resolve_replay_protection` in
        // config.rs uses `std::path::absolute` to normalize the store_path
        // (joins relative paths against cwd; collapses lone `.` segments) before
        // passing it here, covering the common `./foo.db` vs `/cwd/foo.db` case.
        // NOTE: `absolute` does NOT resolve symlinks or collapse `..` traversing
        // symlinks, so two config entries aliasing the same physical file via a
        // symlink or a `..`-through-symlink path will both pass this dedup check.
        // Operators must not alias store paths via symlinks. This is acceptable
        // because store_path comes from operator-controlled TOML config, not
        // untrusted input (closes `store-path-canon`).
        //
        // IMPORTANT: register_path is called AFTER a successful open+DDL so
        // that a panic during setup does not leave a permanent stale entry in
        // OPEN_PATHS. KvStore::Drop will unregister the path, but Drop only
        // runs once Arc::new(KvStore{...}) has executed; if we register before
        // construction and construction panics, no Drop runs and the path is
        // permanently locked for this process. Register last, not first.
        let reg_path = path.to_path_buf();

        let conn = Connection::open(path)
            .unwrap_or_else(|e| panic!("failed to open KV store at {}: {e}", path.display()));

        conn.pragma_update(None, "journal_mode", "WAL")
            .unwrap_or_else(|e| panic!("failed to set WAL mode on KV store: {e}"));
        conn.pragma_update(None, "foreign_keys", "ON")
            .unwrap_or_else(|e| panic!("failed to enable foreign keys on KV store: {e}"));
        conn.pragma_update(None, "busy_timeout", 5000)
            .unwrap_or_else(|e| panic!("failed to set busy_timeout on KV store: {e}"));
        conn.pragma_update(None, "max_page_count", max_page_count)
            .unwrap_or_else(|e| panic!("failed to set max_page_count on KV store: {e}"));

        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS kv (
                namespace TEXT NOT NULL,
                key       BLOB NOT NULL,
                value     BLOB NOT NULL,
                PRIMARY KEY (namespace, key)
            );",
        )
        .unwrap_or_else(|e| panic!("failed to initialize KV store schema: {e}"));

        // Register AFTER all setup succeeds. Drop will unregister.
        register_path(reg_path.clone());
        Arc::new(KvStore {
            conn: Mutex::new(conn),
            tx_active: AtomicBool::new(false),
            quota_hit: AtomicBool::new(false),
            auto_rolled_back: AtomicBool::new(false),
            max_page_count,
            path: reg_path,
        })
    }

    /// Lock the connection, panicking if the mutex is poisoned.
    ///
    /// Centralizes the `lock().unwrap_or_else(|e| panic!(...))` pattern that
    /// appears at every store-op call site. All SQL must go through this
    /// method so the panic message and semantics are consistent.
    pub(crate) fn lock_conn(&self) -> std::sync::MutexGuard<'_, Connection> {
        self.conn
            .lock()
            .unwrap_or_else(|e| panic!("KvStore mutex poisoned ({}): {e}", self.path.display()))
    }

    /// CAS tx_active false->true. Returns true on success (was not set).
    pub fn try_begin_tx(&self) -> bool {
        self.tx_active
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }

    /// Clear tx_active and auto_rolled_back.
    pub fn clear_tx(&self) {
        self.tx_active.store(false, Ordering::SeqCst);
        self.auto_rolled_back.store(false, Ordering::SeqCst);
    }

    pub fn is_tx_active(&self) -> bool {
        self.tx_active.load(Ordering::SeqCst)
    }

    /// Path this store was opened at (canonicalized at open time by caller).
    /// Used for structured log context in commit-failure events.
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    /// Scan a namespace outside a transaction, for test assertions only.
    ///
    /// Uses SQLite's implicit autocommit mode — safe for read-only assertions
    /// outside a BEGIN/COMMIT. This is test-only; production paths go through
    /// the WIT host-impl (begin → scan → commit).
    #[doc(hidden)]
    pub fn scan_for_testing(&self, namespace: &str) -> KvPairs {
        let conn = self.lock_conn();
        scan_inner(&conn, namespace, b"", None, 0)
            .unwrap_or_else(|e| panic!("scan_for_testing({namespace}) failed: {e}"))
    }

    /// Insert or replace a key-value pair outside a transaction, for test seeding only.
    ///
    /// Uses SQLite's implicit autocommit mode (INSERT OR REPLACE). This is test-only;
    /// production paths go through the WIT host-impl (begin → put → commit). Use this
    /// to seed large namespaces without going through the wasm component, avoiding
    /// the O(N²) cost of running prune on every seeding call.
    #[doc(hidden)]
    pub fn put_for_testing(&self, namespace: &str, key: &[u8], value: &[u8]) {
        let conn = self.lock_conn();
        conn.execute(
            "INSERT OR REPLACE INTO kv (namespace, key, value) VALUES (?1, ?2, ?3)",
            rusqlite::params![namespace, key, value],
        )
        .unwrap_or_else(|e| panic!("put_for_testing({namespace}) failed: {e}"));
    }

    /// Pre-fill the store to the max_page_count cap by inserting rows until
    /// `SQLITE_FULL` fires. Returns the number of rows successfully inserted.
    ///
    /// Uses 4-byte big-endian integer keys with a large value (~3900 bytes) so
    /// each page holds only one row. This ensures all pages are fully packed
    /// after the fill, and the very next INSERT from any namespace will require
    /// a new page allocation and fire SQLITE_FULL.
    ///
    /// Uses `BEGIN IMMEDIATE` / `COMMIT` per row to exercise the same path as
    /// production code.
    ///
    /// Test-only; used to seed a cap-hit scenario without going through the WASM
    /// component (which would hit the guest-side CAP or validation before the
    /// host quota fires in many configurations).
    #[doc(hidden)]
    pub fn fill_to_cap_for_testing(&self) -> u32 {
        let mut inserted = 0u32;
        loop {
            // Begin IMMEDIATE exactly as the production path does.
            self.try_begin_tx();
            {
                let conn = self.lock_conn();
                if begin_immediate(&conn).is_err() {
                    // BEGIN IMMEDIATE itself failed — store already at cap or
                    // some other error. Stop filling.
                    self.clear_tx();
                    break;
                }
            }
            let key = inserted.to_be_bytes();
            // ~3900 bytes per row so each 4096-byte page holds exactly one row.
            // This ensures all pages are fully packed and the next INSERT requires
            // a new page, which fires SQLITE_FULL reliably.
            let value = vec![0xABu8; 3900];
            let result = tx_put(self, "fill-test", &key, &value);
            if result.is_ok() {
                let conn = self.lock_conn();
                commit_tx(&conn).unwrap_or_else(|e| {
                    // Commit failed; roll back best-effort (already in error state).
                    rollback_tx(&conn).unwrap_or_else(|re| {
                        tracing::warn!("fill_to_cap rollback after failed commit: {re}");
                    });
                    panic!("fill_to_cap: commit failed unexpectedly: {e}")
                });
                self.clear_tx();
                inserted += 1;
            } else {
                // tx_put failed (QuotaExceeded or other error). Roll back and stop.
                // SQLite auto-rolls back on SQLITE_FULL; the auto_rolled_back flag
                // is set in tx_put, so we can use the structured check here instead
                // of string-matching the error message.
                let already_rolled_back = self.auto_rolled_back.load(Ordering::SeqCst);
                let conn = self.lock_conn();
                match rollback_tx(&conn) {
                    Ok(()) => {}
                    Err(_) if already_rolled_back => {
                        // Expected: SQLite already rolled back on SQLITE_FULL.
                    }
                    Err(e) => panic!("fill_to_cap rollback failed unexpectedly: {e}"),
                }
                self.clear_tx();
                break;
            }
        }
        inserted
    }
}

impl Drop for KvStore {
    fn drop(&mut self) {
        unregister_path(&self.path);
    }
}

/// Live transaction. Held in the wasmtime ResourceTable. The component sees
/// only a Resource<Transaction> handle.
///
/// No rusqlite::Transaction (which borrows Connection and forces a
/// self-referential struct). Each method re-locks kv_store.conn and issues raw
/// SQL. Per-method re-locking is correct because the WIT contract serializes
/// all transaction operations on a single resource handle, and the nested-begin
/// guard prevents a second tx from interleaving.
pub struct Transaction {
    pub(crate) kv_store: Arc<KvStore>,
}

// --- Helpers called from the Host/HostTransaction impls in lib.rs ---

/// Map a rusqlite error to a WIT StoreError::Backend.
pub(crate) fn rusqlite_err_to_store_error(e: rusqlite::Error) -> StoreError {
    StoreError::Backend(format!("{e}"))
}

/// Map a write-path rusqlite error: `SQLITE_FULL` → `StoreError::QuotaExceeded`,
/// everything else → `StoreError::Backend`. Used only on the write path (`tx_put`);
/// reads and deletes cannot hit the cap.
///
/// Uses `sqlite_error_code()` (primary code) rather than checking the extended
/// code. `SQLITE_FULL` has no distinct extended subcode today, but primary-code
/// matching stays correct if SQLite ever introduces one.
pub(crate) fn map_write_error(e: rusqlite::Error) -> StoreError {
    if e.sqlite_error_code() == Some(rusqlite::ErrorCode::DiskFull) {
        return StoreError::QuotaExceeded;
    }
    rusqlite_err_to_store_error(e)
}

pub(crate) fn begin_immediate(conn: &Connection) -> Result<(), rusqlite::Error> {
    conn.execute_batch("BEGIN IMMEDIATE")
}

pub(crate) fn commit_tx(conn: &Connection) -> Result<(), rusqlite::Error> {
    conn.execute_batch("COMMIT")
}

pub(crate) fn rollback_tx(conn: &Connection) -> Result<(), rusqlite::Error> {
    conn.execute_batch("ROLLBACK")
}

/// get within a live transaction.
pub(crate) fn tx_get(
    kv_store: &KvStore,
    namespace: &str,
    key: &[u8],
) -> Result<Option<Vec<u8>>, StoreError> {
    let conn = kv_store.lock_conn();
    let result = conn.query_row(
        "SELECT value FROM kv WHERE namespace = ?1 AND key = ?2",
        rusqlite::params![namespace, key],
        |row| row.get::<_, Vec<u8>>(0),
    );
    match result {
        Ok(v) => Ok(Some(v)),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
        Err(e) => Err(rusqlite_err_to_store_error(e)),
    }
}

pub(crate) fn tx_put(
    kv_store: &KvStore,
    namespace: &str,
    key: &[u8],
    value: &[u8],
) -> Result<(), StoreError> {
    let conn = kv_store.lock_conn();
    let result = conn.execute(
        "INSERT OR REPLACE INTO kv (namespace, key, value) VALUES (?1, ?2, ?3)",
        rusqlite::params![namespace, key, value],
    );
    match result {
        Ok(_) => Ok(()),
        Err(e) => {
            let store_error = map_write_error(e);
            if matches!(store_error, StoreError::QuotaExceeded) {
                // Set quota_hit flag before returning so ReplayComponent::check
                // can read-and-clear it to drive the phone alert (§2.E layer 2).
                kv_store.quota_hit.store(true, Ordering::SeqCst);
                // Set auto_rolled_back: SQLite implicitly rolls back the
                // transaction on SQLITE_FULL, so the subsequent ROLLBACK from
                // the guest's rollback() call may return "no transaction is
                // active". The rollback host-trait method checks this flag to
                // suppress that expected non-error rather than string-matching
                // the error message.
                kv_store.auto_rolled_back.store(true, Ordering::SeqCst);
                tracing::warn!(
                    store_quota_exceeded = true,
                    store_path = %kv_store.path().display(),
                    max_page_count = kv_store.max_page_count,
                    "WASM store quota exceeded (SQLITE_FULL); write rejected"
                );
            }
            Err(store_error)
        }
    }
}

pub(crate) fn tx_delete(kv_store: &KvStore, namespace: &str, key: &[u8]) -> Result<(), StoreError> {
    let conn = kv_store.lock_conn();
    conn.execute(
        "DELETE FROM kv WHERE namespace = ?1 AND key = ?2",
        rusqlite::params![namespace, key],
    )
    .map(|_| ())
    .map_err(rusqlite_err_to_store_error)
}

pub type KvPairs = Vec<(Vec<u8>, Vec<u8>)>;

/// Host-side upper bound on scan result size.
///
/// Applied regardless of the caller's `limit` argument. `limit = 0` ("unlimited"
/// per WIT contract — subject to a host-side maximum the component must not rely
/// on) is clamped to this value; any explicit limit is also capped here.
///
/// 4096 = 4× the 1024 abuse-cap (`NONCE_CAP_N`), providing headroom for lazy
/// slack (TTL-expired entries not yet swept). If a namespace grows past this,
/// the algorithm cannot make sound decisions; the component traps.
///
/// `limit=0` is silently clamped to 4096; the component receives no signal
/// that the result was truncated. If this cap is ever raised above the
/// component's 4096 guard, the component would accept a truncated scan as
/// authoritative (replay protection degrades silently). Consider returning a
/// tagged result or error when the clamp fires, or document the contract in
/// replay.wit and assert both sides reference the same constant.
pub const MAX_SCAN_LIMIT: u32 = 4096;

pub(crate) fn tx_scan(
    kv_store: &KvStore,
    namespace: &str,
    start: &[u8],
    end: Option<&[u8]>,
    limit: u32,
) -> Result<KvPairs, StoreError> {
    let conn = kv_store.lock_conn();
    scan_inner(&conn, namespace, start, end, limit).map_err(rusqlite_err_to_store_error)
}

fn scan_inner(
    conn: &Connection,
    namespace: &str,
    start: &[u8],
    end: Option<&[u8]>,
    limit: u32,
) -> rusqlite::Result<KvPairs> {
    // limit=0 means "unlimited" (subject to host-side MAX_SCAN_LIMIT cap).
    // Any explicit limit is also capped at MAX_SCAN_LIMIT. SQLite LIMIT -1 is
    // the standard "no limit" sentinel; we convert after clamping.
    let clamped: u32 = match limit {
        0 => MAX_SCAN_LIMIT,
        n => n.min(MAX_SCAN_LIMIT),
    };
    let effective_limit: i64 = clamped as i64;

    match end {
        None => {
            let mut stmt = conn.prepare_cached(
                "SELECT key, value FROM kv WHERE namespace = ?1 AND key >= ?2 ORDER BY key ASC LIMIT ?3",
            )?;
            stmt.query_map(
                rusqlite::params![namespace, start, effective_limit],
                |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?)),
            )
            .and_then(|rows| rows.collect())
        }
        Some(end_key) => {
            let mut stmt = conn.prepare_cached(
                "SELECT key, value FROM kv WHERE namespace = ?1 AND key >= ?2 AND key < ?3 ORDER BY key ASC LIMIT ?4",
            )?;
            stmt.query_map(
                rusqlite::params![namespace, start, end_key, effective_limit],
                |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?)),
            )
            .and_then(|rows| rows.collect())
        }
    }
}

// ── Shared Host/HostTransaction helpers ──────────────────────────────────────
//
// Free functions that implement the non-trivial `store::Host` / `HostTransaction`
// method bodies shared between the `replay` and `processor` WIT worlds. Both worlds'
// generated `StoreError` types have identical variants; both impls delegate here and
// apply thin conversions at the call site.
//
// Each function takes `(&mut ResourceTable, &Arc<KvStore>)` and returns `StoreError`
// from the replay world (re-exported as `crate::StoreError`). Processor callers
// map the result via `replay_to_processor_store_error` or direct variant mapping.
//
// The `slug` parameter carries the component identifier for warn/error log context.
// The replay world passes `""` here: the endpoint slug exists on `ReplayComponent`
// (H1046) but is not threaded into `StoreData`, so it is unavailable at these call sites.
// Threading it through was left out of scope by H1046 (design §2.7-E).

/// Shared `begin` body: nested-begin guard, `BEGIN IMMEDIATE`, push resource.
///
/// Returns the new `Resource<Transaction>` on success, or a `StoreError::Backend`
/// if the store already has an active transaction or if `BEGIN IMMEDIATE` fails.
///
/// On `BEGIN IMMEDIATE` failure: clears `tx_active` and returns
/// `StoreError::Backend`. Callers may add world-specific logging before converting
/// to their own `StoreError` type (e.g. processor adds slug/store_path context).
pub(crate) fn host_begin(
    resource_table: &mut ResourceTable,
    kv: &Arc<KvStore>,
) -> Result<Resource<Transaction>, StoreError> {
    if !kv.try_begin_tx() {
        // INVARIANT: do NOT clear tx_active here. The first transaction is still
        // live; clearing the flag would make the first tx's rollback() call attempt
        // ROLLBACK on a connection with no active transaction, panicking. The
        // nested-begin caller is expected to rollback _tx1 explicitly after
        // receiving this error.
        return Err(StoreError::Backend(
            "nested transaction not supported".to_string(),
        ));
    }
    let conn = kv.lock_conn();
    match begin_immediate(&conn) {
        Ok(()) => {
            drop(conn);
            let tx = Transaction {
                kv_store: Arc::clone(kv),
            };
            let res = resource_table
                .push(tx)
                .unwrap_or_else(|e| panic!("ResourceTable::push failed: {e}"));
            Ok(res)
        }
        Err(e) => {
            kv.clear_tx();
            Err(rusqlite_err_to_store_error(e))
        }
    }
}

/// Shared `commit` body: double-use guard, COMMIT, ROLLBACK-on-failure.
///
/// The `tx` handle is obtained with `get` (not `delete`) so the resource lives
/// until the guest calls `drop`. `tx_active = false` after commit signals
/// `HostTransaction::drop` that the transaction completed normally.
///
/// On COMMIT failure: attempts ROLLBACK. If ROLLBACK succeeds, clears `tx_active`
/// and returns `StoreError::Backend`. If ROLLBACK also fails, panics (store
/// corrupted — CLAUDE.md fail-fast). Logs the failure with store_path context.
pub(crate) fn host_commit(
    resource_table: &mut ResourceTable,
    kv_tx: &Resource<Transaction>,
    slug: &str,
) -> Result<(), StoreError> {
    let t = resource_table
        .get(kv_tx)
        .unwrap_or_else(|e| panic!("Transaction resource invalid in commit: {e}"));
    let kv = Arc::clone(&t.kv_store);
    // Double-use guard: if tx_active is false, a prior commit/rollback already
    // completed on this handle. Return an error to surface as a guest-level
    // StoreError rather than the misleading "store corrupted" panic.
    if !kv.is_tx_active() {
        return Err(StoreError::Backend(
            "commit called on already-completed transaction".to_string(),
        ));
    }
    let conn = kv.lock_conn();
    match commit_tx(&conn) {
        Ok(()) => {
            kv.clear_tx();
            Ok(())
        }
        Err(commit_err) => match rollback_tx(&conn) {
            Ok(()) => {
                kv.clear_tx();
                tracing::error!(
                    slug = slug,
                    store_path = %kv.path().display(),
                    commit_err = %commit_err,
                    "WASM store COMMIT failed; ROLLBACK succeeded — surfacing as StoreError::Backend"
                );
                Err(rusqlite_err_to_store_error(commit_err))
            }
            Err(rollback_err) => {
                panic!(
                    "COMMIT failed ({commit_err}) and subsequent ROLLBACK also failed \
                     ({rollback_err}) — store corrupted"
                );
            }
        },
    }
}

/// Shared `rollback` body: completed-tx guard, auto-rollback detection, ROLLBACK.
///
/// If `tx_active` is already false (prior commit or rollback completed), logs a
/// warning (guest logic bug) and returns — a no-op is safe here. Otherwise issues
/// ROLLBACK and clears `tx_active`.
pub(crate) fn host_rollback(
    resource_table: &mut ResourceTable,
    kv_tx: &Resource<Transaction>,
    slug: &str,
) {
    let t = resource_table
        .get(kv_tx)
        .unwrap_or_else(|e| panic!("Transaction resource invalid in rollback: {e}"));
    let kv = Arc::clone(&t.kv_store);
    if !kv.is_tx_active() {
        tracing::warn!(
            slug = slug,
            "wasm store: rollback called on already-completed transaction — \
             guest logic bug (double-rollback or rollback-after-commit)"
        );
        return;
    }
    let conn = kv.lock_conn();
    let already_rolled_back = kv.auto_rolled_back.load(Ordering::SeqCst);
    match rollback_tx(&conn) {
        Ok(()) => {}
        Err(_) if already_rolled_back => {
            // Expected: SQLite already rolled back implicitly on SQLITE_FULL.
        }
        Err(e) => panic!("ROLLBACK failed: {e}"),
    }
    kv.clear_tx();
}

/// Shared `drop` body: resource delete, leaked-tx detection and rollback.
///
/// Returns `wasmtime::Result<()>`. If the transaction is still active when the
/// guest drops the handle (leaked transaction), performs a best-effort ROLLBACK
/// and returns `Err` — wasmtime treats a dtor returning `Err` as a trap.
/// Replay world: trap collapses to panic (`ReplayComponent::check`).
/// Processor world: trap is contained (alerted by caller).
pub(crate) fn host_drop(
    resource_table: &mut ResourceTable,
    tx: Resource<Transaction>,
) -> wasmtime::Result<()> {
    let t = resource_table
        .delete(tx)
        .unwrap_or_else(|e| panic!("ResourceTable::delete failed in drop: {e}"));
    let kv = &t.kv_store;
    if kv.is_tx_active() {
        // tx_active still set = commit/rollback was never called — guest leaked.
        // Best-effort ROLLBACK to release the SQLite write lock.
        let conn = kv.lock_conn();
        rollback_tx(&conn).unwrap_or_else(|e| {
            panic!(
                "best-effort ROLLBACK in leaked-tx drop also failed ({e}) — store may be corrupted"
            );
        });
        kv.clear_tx();
        return Err(wasmtime::Error::msg(
            "transaction leaked without commit/rollback — component bug",
        ));
    }
    // tx_active is false = commit or rollback already ran. Normal drop.
    // INVARIANT: drop-after-commit is safe — commit clears tx_active, so this
    // branch is always reached for a properly committed transaction. Fixture
    // code (processor-store-rt) relies on this: WIT-generated bindings take
    // `&self` for commit (not `self`), so the resource is not consumed and
    // its Drop impl fires when the binding goes out of scope. That drop reaches
    // here and returns Ok(()) — no double-free, no error.
    Ok(())
}

// ── KvStore unit tests ────────────────────────────────────────────────────────
//
// Tests for get/put/delete/scan/rollback semantics using the internal store API.
// Moved here from brenn-wasm/tests/store_round_trip.rs to allow pub(crate) access.
// Fault-test component tests (leaked_transaction_traps, component_trap_is_distinguished_from_typed_error)
// remain in the integration test file because they require ReplayComponent from lib.rs.

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    fn open_store() -> (NamedTempFile, Arc<KvStore>) {
        let db = NamedTempFile::new().unwrap();
        let kv = KvStore::open(db.path(), DEFAULT_MAX_PAGE_COUNT);
        (db, kv)
    }

    /// Begin a transaction, run `f`, commit, clear flag.
    ///
    /// Contract: `f` MUST NOT return without the store being in a committable
    /// state. In practice this means `f` must panic (not return `Err`) on store
    /// errors — use `unwrap()` / `unwrap_or_else(|e| panic!(...))` inside `f`,
    /// never `?` or returning `Err`. If `f` returns while a failed tx_put has
    /// left the transaction in a bad state, the unconditional `commit_tx` will
    /// either silently succeed (committing unintended state) or panic with a
    /// misleading error. For fallible puts, use `try_tx_put` instead.
    fn with_tx<R>(kv: &Arc<KvStore>, f: impl FnOnce() -> R) -> R {
        kv.try_begin_tx();
        begin_immediate(&kv.lock_conn()).unwrap();
        let result = f();
        commit_tx(&kv.lock_conn()).unwrap();
        kv.clear_tx();
        result
    }

    #[test]
    fn write_then_read() {
        let (_db, kv) = open_store();
        with_tx(&kv, || tx_put(&kv, "ns", b"key", b"val").unwrap());
        let result = with_tx(&kv, || tx_get(&kv, "ns", b"key").unwrap());
        assert_eq!(result, Some(b"val".to_vec()));
    }

    #[test]
    fn read_missing_key_is_miss() {
        let (_db, kv) = open_store();
        let result = with_tx(&kv, || tx_get(&kv, "ns", b"nonexistent").unwrap());
        assert_eq!(result, None);
    }

    #[test]
    fn delete_then_read_is_miss() {
        let (_db, kv) = open_store();
        with_tx(&kv, || tx_put(&kv, "ns", b"k", b"v").unwrap());
        with_tx(&kv, || tx_delete(&kv, "ns", b"k").unwrap());
        let result = with_tx(&kv, || tx_get(&kv, "ns", b"k").unwrap());
        assert_eq!(result, None);
    }

    #[test]
    fn rollback_discards_write() {
        let (_db, kv) = open_store();
        kv.try_begin_tx();
        begin_immediate(&kv.lock_conn()).unwrap();
        tx_put(&kv, "ns", b"k", b"v").unwrap();
        rollback_tx(&kv.lock_conn()).unwrap();
        kv.clear_tx();

        let result = with_tx(&kv, || tx_get(&kv, "ns", b"k").unwrap());
        assert_eq!(result, None, "rollback must discard the write");
    }

    /// A ROLLBACK on one tx must not disturb a prior committed write.
    #[test]
    fn rollback_does_not_erase_prior_committed_write() {
        let (_db, kv) = open_store();
        // Commit key A.
        with_tx(&kv, || tx_put(&kv, "ns", b"a", b"va").unwrap());
        // Rollback key B.
        kv.try_begin_tx();
        begin_immediate(&kv.lock_conn()).unwrap();
        tx_put(&kv, "ns", b"b", b"vb").unwrap();
        rollback_tx(&kv.lock_conn()).unwrap();
        kv.clear_tx();
        // A must still be present; B must be absent.
        let a = with_tx(&kv, || tx_get(&kv, "ns", b"a").unwrap());
        let b = with_tx(&kv, || tx_get(&kv, "ns", b"b").unwrap());
        assert_eq!(
            a,
            Some(b"va".to_vec()),
            "committed key A must survive rollback of B"
        );
        assert_eq!(b, None, "rolled-back key B must be absent");
    }

    #[test]
    fn scan_returns_ordered_range() {
        let (_db, kv) = open_store();
        for (k, v) in [("a", "va"), ("b", "vb"), ("c", "vc")] {
            with_tx(&kv, || {
                tx_put(&kv, "ns", k.as_bytes(), v.as_bytes()).unwrap()
            });
        }
        let pairs = with_tx(&kv, || tx_scan(&kv, "ns", b"a", None, 2).unwrap());
        assert_eq!(pairs.len(), 2);
        assert_eq!(pairs[0], (b"a".to_vec(), b"va".to_vec()));
        assert_eq!(pairs[1], (b"b".to_vec(), b"vb".to_vec()));
    }

    #[test]
    fn scan_unlimited_returns_all_keys() {
        let (_db, kv) = open_store();
        for (k, v) in [("a", "va"), ("b", "vb"), ("c", "vc")] {
            with_tx(&kv, || {
                tx_put(&kv, "ns", k.as_bytes(), v.as_bytes()).unwrap()
            });
        }
        let pairs = with_tx(&kv, || tx_scan(&kv, "ns", b"a", None, 0).unwrap());
        assert_eq!(pairs.len(), 3, "limit=0 should return all 3 keys");
    }

    #[test]
    fn scan_with_end_boundary() {
        let (_db, kv) = open_store();
        for (k, v) in [("a", "va"), ("b", "vb"), ("c", "vc")] {
            with_tx(&kv, || {
                tx_put(&kv, "ns", k.as_bytes(), v.as_bytes()).unwrap()
            });
        }
        // end="b" means key < "b": only "a" returned.
        let pairs = with_tx(&kv, || tx_scan(&kv, "ns", b"a", Some(b"b"), 0).unwrap());
        assert_eq!(pairs.len(), 1, "end boundary must exclude keys >= end");
        assert_eq!(pairs[0].0, b"a");
    }

    #[test]
    fn persistence_across_kvstore_reopen() {
        let db = NamedTempFile::new().unwrap();
        let path = db.path().to_path_buf();
        {
            let kv = KvStore::open(&path, DEFAULT_MAX_PAGE_COUNT);
            with_tx(&kv, || tx_put(&kv, "persist", b"key", b"hello").unwrap());
            // kv dropped here, unregisters path
        }
        {
            let kv = KvStore::open(&path, DEFAULT_MAX_PAGE_COUNT);
            let result = with_tx(&kv, || tx_get(&kv, "persist", b"key").unwrap());
            assert_eq!(
                result,
                Some(b"hello".to_vec()),
                "value must persist after reopen"
            );
        }
    }

    #[test]
    fn nested_begin_returns_store_error() {
        let (_db, kv) = open_store();
        assert!(kv.try_begin_tx(), "first try_begin_tx must succeed");
        begin_immediate(&kv.lock_conn()).unwrap();
        let nested_ok = kv.try_begin_tx();
        assert!(!nested_ok, "nested try_begin_tx must return false");
        rollback_tx(&kv.lock_conn()).unwrap();
        kv.clear_tx();
    }

    // ── Quota / max_page_count tests ─────────────────────────────────────────────
    //
    // TODO(quota-statement-vs-commit): This block is the empirical gate for the
    // design's load-bearing assumption that SQLITE_FULL fires at the INSERT
    // (tx_put / statement execution), NOT at COMMIT, under WAL + BEGIN IMMEDIATE.
    // The whole §2.C "write path only" detection design rests on this.
    //
    // If `quota_fires_at_statement_not_commit` fails (QuotaExceeded does NOT come
    // from tx_put but instead some other error surfaces), the §2.C detection
    // approach is wrong and map_write_error must also cover the commit path before
    // proceeding. See design §5 GO/NO-GO GATE and §2.C commit-path asymmetry.

    /// Open a store with a tiny cap (exactly 16 pages = 64 KiB — the minimum floor).
    fn open_tiny_store() -> (NamedTempFile, Arc<KvStore>) {
        let db = NamedTempFile::new().unwrap();
        // 16 pages = 64 KiB; just enough for schema + WAL header + one page of data.
        let tiny_cap: u32 = 16;
        let kv = KvStore::open(db.path(), tiny_cap);
        (db, kv)
    }

    /// Helper: try tx_put without panicking; returns the StoreError on failure.
    fn try_tx_put(kv: &Arc<KvStore>, ns: &str, key: &[u8], value: &[u8]) -> Result<(), StoreError> {
        kv.try_begin_tx();
        begin_immediate(&kv.lock_conn()).unwrap();
        let result = tx_put(kv, ns, key, value);
        if result.is_ok() {
            commit_tx(&kv.lock_conn()).unwrap();
        } else {
            // SQLite auto-rolls back on SQLITE_FULL; use the structured flag
            // (same mechanism as the production rollback host-trait method).
            let already_rolled_back = kv.auto_rolled_back.load(Ordering::SeqCst);
            match rollback_tx(&kv.lock_conn()) {
                Ok(()) => {}
                Err(_) if already_rolled_back => {
                    // Expected: SQLite already rolled back on SQLITE_FULL.
                }
                Err(e) => panic!("try_tx_put rollback failed unexpectedly: {e}"),
            }
        }
        kv.clear_tx();
        result
    }

    /// GO/NO-GO GATE (TODO(quota-statement-vs-commit)): filling the store to cap
    /// must produce `QuotaExceeded` from `tx_put` (statement), not from `commit`.
    ///
    /// Empirically validates that `PRAGMA max_page_count` under WAL + BEGIN IMMEDIATE
    /// causes `SQLITE_FULL` at INSERT execution, not at COMMIT. If this test passes,
    /// the §2.C "write path only" map_write_error detection is correct.
    #[test]
    fn quota_fires_at_statement_not_commit() {
        let (_db, kv) = open_tiny_store();

        // Write progressively larger entries until we hit QuotaExceeded.
        let mut hit_quota = false;
        for i in 0u32..500 {
            let key = i.to_be_bytes();
            // 256-byte value to fill up pages quickly.
            let value = vec![0xABu8; 256];
            match try_tx_put(&kv, "ns", &key, &value) {
                Ok(()) => {}
                Err(StoreError::QuotaExceeded) => {
                    hit_quota = true;
                    break;
                }
                Err(e) => panic!("unexpected store error while filling to cap: {e}"),
            }
        }
        assert!(
            hit_quota,
            "expected QuotaExceeded from tx_put before 500 entries at 256 bytes each in a 64 KiB store"
        );
        // quota_hit flag must be set.
        assert!(
            kv.quota_hit.load(Ordering::SeqCst),
            "quota_hit flag must be set after QuotaExceeded"
        );
        // AC-2: page_count must not exceed max_page_count after quota hit.
        // This validates that the pragma was actually enforced by SQLite, not
        // silently ignored. If the pragma name were wrong or the connection were
        // incorrect, the INSERT would fail for a different reason, but
        // page_count might quietly exceed the configured cap.
        let conn = kv.lock_conn();
        let page_count: u32 = conn
            .pragma_query_value(None, "page_count", |r| r.get(0))
            .unwrap();
        assert!(
            page_count <= 16,
            "page_count {page_count} must not exceed max_page_count=16 after quota hit"
        );
    }

    /// AC-7 layer 1: `tx_put` must emit a `warn!` with `store_quota_exceeded=true`
    /// when SQLITE_FULL fires. Validates that the structured log field is present
    /// so monitoring pipelines that key on `store_quota_exceeded=true` receive the
    /// signal even if the phone-alert path is unavailable.
    #[test]
    #[tracing_test::traced_test]
    fn quota_exceeded_emits_structured_warn_log() {
        let (_db, kv) = open_tiny_store();

        // Fill until QuotaExceeded.
        for i in 0u32..500 {
            let key = i.to_be_bytes();
            let value = vec![0xABu8; 256];
            match try_tx_put(&kv, "ns", &key, &value) {
                Ok(()) => {}
                Err(StoreError::QuotaExceeded) => break,
                Err(e) => panic!("unexpected store error: {e}"),
            }
        }

        // The structured field `store_quota_exceeded=true` must appear in the
        // captured log output at warn level.
        assert!(
            logs_contain("store_quota_exceeded"),
            "tx_put must emit a log containing 'store_quota_exceeded' on SQLITE_FULL"
        );
        assert!(
            logs_contain("WASM store quota exceeded (SQLITE_FULL)"),
            "tx_put must emit the expected warn message on SQLITE_FULL"
        );
    }

    /// After a rejected over-cap write, reads of previously committed data succeed (AC-3).
    #[test]
    fn read_succeeds_after_quota_rejection() {
        let (_db, kv) = open_tiny_store();

        // Write some data until quota is hit.
        let mut last_ok_key: Option<u32> = None;
        for i in 0u32..500 {
            let key = i.to_be_bytes();
            let value = vec![0xBBu8; 256];
            match try_tx_put(&kv, "ns", &key, &value) {
                Ok(()) => {
                    last_ok_key = Some(i);
                }
                Err(StoreError::QuotaExceeded) => break,
                Err(e) => panic!("unexpected store error: {e}"),
            }
        }

        let committed_key = last_ok_key
            .expect("expected at least one successful write before quota hit")
            .to_be_bytes();

        // Read the last committed key — must succeed.
        let result = with_tx(&kv, || tx_get(&kv, "ns", &committed_key).unwrap());
        assert!(
            result.is_some(),
            "previously committed key must be readable after quota rejection"
        );
    }

    /// DELETE at cap under WAL + BEGIN IMMEDIATE must not fail with SQLITE_FULL (AC-4).
    #[test]
    fn delete_at_cap_does_not_fail() {
        let (_db, kv) = open_tiny_store();

        // Fill to quota.
        let mut last_ok_key: Option<u32> = None;
        for i in 0u32..500 {
            let key = i.to_be_bytes();
            let value = vec![0xCCu8; 256];
            match try_tx_put(&kv, "ns", &key, &value) {
                Ok(()) => {
                    last_ok_key = Some(i);
                }
                Err(StoreError::QuotaExceeded) => break,
                Err(e) => panic!("unexpected store error: {e}"),
            }
        }

        let key_to_delete = last_ok_key
            .expect("expected at least one write before cap")
            .to_be_bytes();

        // DELETE at cap must not return SQLITE_FULL.
        with_tx(&kv, || {
            tx_delete(&kv, "ns", &key_to_delete)
                .unwrap_or_else(|e| panic!("DELETE at cap must not fail; got: {e}"))
        });
    }

    /// Prune-then-insert in one transaction at cap must commit (AC-4 recovery).
    ///
    /// Uses a 128-page (512 KiB) cap so the btree has enough room for the freelist
    /// reuse mechanism to work. The 16-page minimum-floor cap is too tight for
    /// freelist reuse because SQLite may need to split pages when the btree root
    /// is at capacity even after a delete frees one leaf — at 128 pages there is
    /// enough internal structure headroom for the reuse path to function.
    #[test]
    fn prune_then_insert_at_cap_commits() {
        let db = NamedTempFile::new().unwrap();
        // 128 pages = 512 KiB: large enough for freelist-reuse to work,
        // small enough that the test fills to cap quickly.
        let kv = KvStore::open(db.path(), 128);

        // Write until quota hit, tracking all committed keys.
        let mut committed_keys: Vec<[u8; 4]> = Vec::new();
        for i in 0u32..2000 {
            let key = i.to_be_bytes();
            let value = vec![0xDDu8; 256];
            match try_tx_put(&kv, "ns", &key, &value) {
                Ok(()) => committed_keys.push(key),
                Err(StoreError::QuotaExceeded) => break,
                Err(e) => panic!("unexpected store error: {e}"),
            }
        }
        assert!(
            committed_keys.len() >= 2,
            "need at least two committed keys to prune and reinsert"
        );

        // Prune-then-insert in ONE transaction: delete the majority of keys so that
        // entire btree pages are freed into the freelist; then insert a new one.
        // Deleting only a handful of rows may not free a complete btree leaf page.
        // We delete ~80% of committed keys to ensure whole pages are freed.
        let delete_count = committed_keys.len() * 4 / 5;
        let keys_to_delete: Vec<_> = committed_keys[..delete_count].to_vec();
        let new_key = 0xFFFF_FFFFu32.to_be_bytes();
        let new_value = vec![0xEEu8; 256];

        // This must commit without QuotaExceeded.
        kv.try_begin_tx();
        begin_immediate(&kv.lock_conn()).unwrap();
        for key_to_delete in &keys_to_delete {
            tx_delete(&kv, "ns", key_to_delete).expect("DELETE in prune-then-insert must not fail");
        }
        tx_put(&kv, "ns", &new_key, &new_value)
            .expect("INSERT after multi-prune in same tx must not fail with QuotaExceeded");
        commit_tx(&kv.lock_conn()).unwrap();
        kv.clear_tx();
    }
}
