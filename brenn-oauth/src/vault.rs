//! The credential vault: one file per `(provider, Brenn user, account)`.
//!
//! Layout: `<vault_dir>/<provider>/<username>/<account>.json`, directories
//! 0700 and files 0600, all owned by the server's uid. Boot validated the root
//! (mode, owner, disjoint from every agent-visible path); this module creates
//! only the directories beneath it. A file is written to a fresh temporary
//! name and renamed over its target, so a reader sees the old credential or
//! the new one and never a torn one.
//!
//! Vault I/O failing under a running server is a changed host, not a
//! configuration error: every failure panics naming the path.

use std::collections::HashMap;
use std::io::Write;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use rand::RngExt;
use serde::{Deserialize, Serialize};

/// The only file format version this build reads or writes.
const FORMAT_VERSION: u32 = 1;

/// Characters left as-is in a path component: alphanumerics and `@ . - _`, so
/// an account like `alice@example.com` stays readable in a listing.
const COMPONENT: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'@')
    .remove(b'.')
    .remove(b'-')
    .remove(b'_');

/// One stored credential, exactly as the file holds it.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Credential {
    pub v: u32,
    pub provider: String,
    pub account: String,
    pub refresh_token: String,
    pub access_token: String,
    pub expires_at: DateTime<Utc>,
    /// The scopes the provider granted, which may be fewer than requested.
    pub scopes: Vec<String>,
    pub obtained_at: DateTime<Utc>,
}

impl std::fmt::Debug for Credential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Credential")
            .field("provider", &self.provider)
            .field("account", &self.account)
            .field("expires_at", &self.expires_at)
            .field("scopes", &self.scopes)
            .field("obtained_at", &self.obtained_at)
            .finish_non_exhaustive()
    }
}

impl Credential {
    /// A credential in the current format version.
    pub fn new(
        provider: &str,
        account: &str,
        refresh_token: String,
        access_token: String,
        expires_at: DateTime<Utc>,
        scopes: Vec<String>,
        obtained_at: DateTime<Utc>,
    ) -> Self {
        Self {
            v: FORMAT_VERSION,
            provider: provider.to_string(),
            account: account.to_string(),
            refresh_token,
            access_token,
            expires_at,
            scopes,
            obtained_at,
        }
    }
}

/// The per-credential locks, keyed by credential path.
type LockTable = Arc<Mutex<HashMap<PathBuf, Arc<tokio::sync::Mutex<()>>>>>;

/// Exclusive hold of one credential file, from [`Vault::lock`]; released on
/// drop.
pub struct CredentialGuard {
    held: tokio::sync::OwnedMutexGuard<()>,
    table: LockTable,
    path: PathBuf,
    username: String,
}

impl CredentialGuard {
    /// The credential file this guard holds.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for CredentialGuard {
    /// Removes the path's lock from the table when nobody else holds or awaits
    /// it, so the table holds only credentials in use. Every handle on a lock
    /// is cloned from the table under the table's mutex, which is held here:
    /// a count of two (the table's and this guard's) means no other caller
    /// has one, and none can take one before the entry is gone.
    fn drop(&mut self) {
        let mut table = self.table.lock().expect("vault lock table poisoned");
        if Arc::strong_count(tokio::sync::OwnedMutexGuard::mutex(&self.held)) == 2 {
            table.remove(&self.path);
        }
    }
}

/// The vault root and the per-credential locks every write is made under.
///
/// Its file I/O runs on the blocking pool, never on an async worker; a panic
/// there is resumed in the caller.
pub struct Vault {
    root: PathBuf,
    locks: LockTable,
}

impl Vault {
    /// A vault over `root`, which boot has already validated.
    pub fn new(root: PathBuf) -> Self {
        Self {
            root,
            locks: LockTable::default(),
        }
    }

    /// The directory holding `username`'s credentials for `provider`.
    pub fn user_dir(&self, provider: &str, username: &str) -> PathBuf {
        user_dir(&self.root, provider, username)
    }

    /// The file holding one credential.
    pub fn credential_path(&self, provider: &str, username: &str, account: &str) -> PathBuf {
        credential_path(&self.root, provider, username, account)
    }

    /// Wait for exclusive hold of one credential file. Every write is made
    /// under it, so a refresh that reads, calls the provider and writes back
    /// cannot overwrite a reconnect, and concurrent callers needing a refresh
    /// perform one.
    pub async fn lock(&self, provider: &str, username: &str, account: &str) -> CredentialGuard {
        let path = self.credential_path(provider, username, account);
        let mutex = self
            .locks
            .lock()
            .expect("vault lock table poisoned")
            .entry(path.clone())
            .or_default()
            .clone();
        CredentialGuard {
            held: mutex.lock_owned().await,
            table: self.locks.clone(),
            path,
            username: username.to_string(),
        }
    }

    /// Store `credential` for `username` under its lock, replacing any
    /// earlier one for the same account.
    pub async fn store(&self, username: &str, credential: &Credential) {
        let guard = self
            .lock(&credential.provider, username, &credential.account)
            .await;
        self.write(&guard, credential).await;
    }

    /// Write `credential` to the file `guard` holds, replacing its contents.
    ///
    /// # Panics
    ///
    /// `guard` holds another credential's file, or any filesystem failure,
    /// naming the path.
    pub async fn write(&self, guard: &CredentialGuard, credential: &Credential) {
        let username = guard.username.as_str();
        let path = self.credential_path(&credential.provider, username, &credential.account);
        assert!(
            path == guard.path,
            "vault: write to {} under the lock of {}",
            path.display(),
            guard.path.display(),
        );
        let dir = self.user_dir(&credential.provider, username);
        let body = serde_json::to_vec_pretty(credential).expect("a credential serializes");
        blocking(move || write_file(&dir, &path, &body)).await;
    }

    /// The credential stored at `path`, or `None` when there is none,
    /// including when `path`'s name is too long for the filesystem to hold.
    ///
    /// # Panics
    ///
    /// The file exists but cannot be read, or does not parse as this format
    /// version: a bug or a hand edit, either way not a credential to use.
    pub async fn read(&self, path: &Path) -> Option<Credential> {
        let path = path.to_path_buf();
        blocking(move || read_file(&path)).await
    }

    /// Every credential `username` holds for `provider`, ordered by account.
    ///
    /// # Panics
    ///
    /// The directory cannot be listed, a credential cannot be read (as
    /// [`Vault::read`]), or a file's contents name another provider or account
    /// than its path: a hand edit, not a credential to report.
    pub async fn list(&self, provider: &str, username: &str) -> Vec<Credential> {
        let (root, provider, username) = (
            self.root.clone(),
            provider.to_string(),
            username.to_string(),
        );
        blocking(move || list_dir(&root, &provider, &username)).await
    }

    /// The accounts `username` has connected for `provider`, in order.
    ///
    /// # Panics
    ///
    /// As [`Vault::list`].
    pub async fn accounts(&self, provider: &str, username: &str) -> Vec<String> {
        self.list(provider, username)
            .await
            .into_iter()
            .map(|c| c.account)
            .collect()
    }

    /// Delete the credential file `guard` holds. Returns whether there was
    /// one.
    ///
    /// # Panics
    ///
    /// Any filesystem failure other than the file being absent, naming the
    /// path.
    pub async fn remove(&self, guard: &CredentialGuard) -> bool {
        let path = guard.path.clone();
        blocking(move || match std::fs::remove_file(&path) {
            Ok(()) => true,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => false,
            Err(e) => panic!("vault: cannot remove {}: {e}", path.display()),
        })
        .await
    }

    /// How many credential paths have a lock in the table.
    #[cfg(test)]
    fn locked_paths(&self) -> usize {
        self.locks.lock().unwrap().len()
    }
}

/// Run `f` on the blocking pool; a panic in it is resumed here.
async fn blocking<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> T {
    match tokio::task::spawn_blocking(f).await {
        Ok(value) => value,
        Err(e) => match e.try_into_panic() {
            Ok(panic) => std::panic::resume_unwind(panic),
            Err(e) => panic!("vault: blocking I/O task did not complete: {e}"),
        },
    }
}

fn user_dir(root: &Path, provider: &str, username: &str) -> PathBuf {
    root.join(component(provider)).join(component(username))
}

fn credential_path(root: &Path, provider: &str, username: &str, account: &str) -> PathBuf {
    user_dir(root, provider, username)
        .join(format!("{}.json", utf8_percent_encode(account, COMPONENT)))
}

/// Write `body` to a fresh temporary beside `path`, then rename it over `path`.
fn write_file(dir: &Path, path: &Path, body: &[u8]) {
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(dir)
        .unwrap_or_else(|e| panic!("vault: cannot create {}: {e}", dir.display()));
    let mut suffix = [0u8; 8];
    rand::rng().fill(&mut suffix);
    let tmp = path.with_extension(format!("json.tmp-{}", hex::encode(suffix)));
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&tmp)
        .unwrap_or_else(|e| panic!("vault: cannot create {}: {e}", tmp.display()));
    file.write_all(body)
        .and_then(|()| file.sync_all())
        .unwrap_or_else(|e| panic!("vault: cannot write {}: {e}", tmp.display()));
    drop(file);
    std::fs::rename(&tmp, path).unwrap_or_else(|e| {
        panic!(
            "vault: cannot rename {} to {}: {e}",
            tmp.display(),
            path.display()
        )
    });
}

fn read_file(path: &Path) -> Option<Credential> {
    let text = match std::fs::read(path) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return None,
        // The account comes from a caller's argument. A file name too long for
        // the filesystem was never written, so it names no stored credential.
        Err(e) if e.kind() == std::io::ErrorKind::InvalidFilename => return None,
        Err(e) => panic!("vault: cannot read {}: {e}", path.display()),
    };
    let credential: Credential = serde_json::from_slice(&text)
        .unwrap_or_else(|e| panic!("vault: {} is malformed: {e}", path.display()));
    assert!(
        credential.v == FORMAT_VERSION,
        "vault: {} has format version {}, this build reads {FORMAT_VERSION}",
        path.display(),
        credential.v,
    );
    Some(credential)
}

fn list_dir(root: &Path, provider: &str, username: &str) -> Vec<Credential> {
    let dir = user_dir(root, provider, username);
    let entries = match std::fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Vec::new(),
        Err(e) => panic!("vault: cannot list {}: {e}", dir.display()),
    };
    let mut credentials = Vec::new();
    for entry in entries {
        let path = entry
            .unwrap_or_else(|e| panic!("vault: cannot list {}: {e}", dir.display()))
            .path();
        // Temporaries end in `.json.tmp-<hex>` and are skipped here.
        if path.extension().is_none_or(|ext| ext != "json") {
            continue;
        }
        let Some(credential) = read_file(&path) else {
            // Removed between the listing and the read.
            continue;
        };
        assert!(
            credential.provider == provider
                && credential_path(root, provider, username, &credential.account) == path,
            "vault: {} holds a credential for {:?} at {:?}",
            path.display(),
            credential.account,
            credential.provider,
        );
        credentials.push(credential);
    }
    credentials.sort_by(|a, b| a.account.cmp(&b.account));
    credentials
}

/// One path component: percent-encoded, and never `.` or `..`.
fn component(name: &str) -> String {
    let encoded = utf8_percent_encode(name, COMPONENT).to_string();
    assert!(
        !encoded.is_empty() && encoded != "." && encoded != "..",
        "vault: {name:?} cannot name a directory"
    );
    encoded
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;

    use super::*;

    fn credential(account: &str, refresh: &str) -> Credential {
        let now = DateTime::parse_from_rfc3339("2026-10-28T22:10:00Z")
            .unwrap()
            .with_timezone(&Utc);
        Credential::new(
            "google",
            account,
            refresh.to_string(),
            "access".to_string(),
            now + chrono::Duration::hours(1),
            vec!["openid".to_string()],
            now,
        )
    }

    fn mode(path: &Path) -> u32 {
        std::fs::metadata(path).unwrap().permissions().mode() & 0o777
    }

    #[tokio::test]
    async fn write_then_read_round_trips_with_private_modes() {
        let tmp = tempfile::tempdir().unwrap();
        let vault = Vault::new(tmp.path().to_path_buf());
        let cred = credential("alice@example.com", "r1");
        vault.store("alice", &cred).await;

        let path = vault.credential_path("google", "alice", "alice@example.com");
        assert_eq!(
            path,
            tmp.path().join("google/alice/alice@example.com.json"),
            "an ordinary address stays readable"
        );
        assert_eq!(vault.read(&path).await, Some(cred));
        assert_eq!(mode(&path), 0o600);
        assert_eq!(mode(&tmp.path().join("google")), 0o700);
        assert_eq!(mode(&tmp.path().join("google/alice")), 0o700);
    }

    #[tokio::test]
    async fn write_replaces_atomically_and_leaves_no_temporary() {
        let tmp = tempfile::tempdir().unwrap();
        let vault = Vault::new(tmp.path().to_path_buf());
        vault
            .store("alice", &credential("alice@example.com", "r1"))
            .await;
        vault
            .store("alice", &credential("alice@example.com", "r2"))
            .await;

        let dir = tmp.path().join("google/alice");
        let names: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        assert_eq!(names, vec!["alice@example.com.json".to_string()]);
        let path = vault.credential_path("google", "alice", "alice@example.com");
        assert_eq!(vault.read(&path).await.unwrap().refresh_token, "r2");
    }

    #[test]
    fn account_and_user_are_percent_encoded_components() {
        let tmp = tempfile::tempdir().unwrap();
        let vault = Vault::new(tmp.path().to_path_buf());
        assert_eq!(
            vault.credential_path("google", "alice", "a/b c"),
            tmp.path().join("google/alice/a%2Fb%20c.json")
        );
        assert_eq!(
            vault.credential_path("google", "alice", ".."),
            tmp.path().join("google/alice/...json"),
            "the suffix keeps `..` a file name"
        );
        assert_eq!(
            vault.user_dir("google", "a/b"),
            tmp.path().join("google/a%2Fb")
        );
    }

    #[test]
    #[should_panic(expected = "cannot name a directory")]
    fn a_dot_dot_username_panics() {
        Vault::new(PathBuf::from("/v")).user_dir("google", "..");
    }

    #[tokio::test]
    async fn same_account_under_two_users_is_two_files() {
        let tmp = tempfile::tempdir().unwrap();
        let vault = Vault::new(tmp.path().to_path_buf());
        vault
            .store("alice", &credential("shared@example.com", "ra"))
            .await;
        vault
            .store("bob", &credential("shared@example.com", "rb"))
            .await;
        let a = vault.credential_path("google", "alice", "shared@example.com");
        let b = vault.credential_path("google", "bob", "shared@example.com");
        assert_ne!(a, b);
        assert_eq!(vault.read(&a).await.unwrap().refresh_token, "ra");
        assert_eq!(vault.read(&b).await.unwrap().refresh_token, "rb");
    }

    #[tokio::test]
    async fn missing_file_reads_none() {
        let tmp = tempfile::tempdir().unwrap();
        let vault = Vault::new(tmp.path().to_path_buf());
        let path = vault.credential_path("google", "alice", "nobody@example.com");
        assert_eq!(vault.read(&path).await, None);
    }

    #[tokio::test]
    async fn an_account_too_long_for_a_file_name_reads_none() {
        let tmp = tempfile::tempdir().unwrap();
        let vault = Vault::new(tmp.path().to_path_buf());
        vault
            .store("alice", &credential("alice@example.com", "r1"))
            .await;
        // Each `+` encodes to three bytes: 300 bytes, past any NAME_MAX.
        let path = vault.credential_path("google", "alice", &"+".repeat(100));
        let err = std::fs::read(&path).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidFilename, "{err}");
        assert_eq!(vault.read(&path).await, None);
    }

    #[tokio::test]
    #[should_panic(expected = "is malformed")]
    async fn unknown_field_panics() {
        let tmp = tempfile::tempdir().unwrap();
        let vault = Vault::new(tmp.path().to_path_buf());
        vault
            .store("alice", &credential("alice@example.com", "r1"))
            .await;
        let path = vault.credential_path("google", "alice", "alice@example.com");
        let mut value: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        value["extra"] = serde_json::json!(1);
        std::fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();
        vault.read(&path).await;
    }

    #[tokio::test]
    #[should_panic(expected = "format version 2")]
    async fn other_version_panics() {
        let tmp = tempfile::tempdir().unwrap();
        let vault = Vault::new(tmp.path().to_path_buf());
        let mut cred = credential("alice@example.com", "r1");
        cred.v = 2;
        vault.store("alice", &cred).await;
        vault
            .read(&vault.credential_path("google", "alice", "alice@example.com"))
            .await;
    }

    #[test]
    fn debug_omits_tokens() {
        let rendered = format!("{:?}", credential("alice@example.com", "secret-refresh"));
        assert!(!rendered.contains("secret-refresh"));
        assert!(!rendered.contains("access"));
    }

    /// A store waits for whoever holds the credential (a refresh in flight)
    /// and lands after it; another credential's lock is independent.
    #[tokio::test]
    async fn a_store_waits_for_the_credentials_holder() {
        let tmp = tempfile::tempdir().unwrap();
        let vault = Vault::new(tmp.path().to_path_buf());
        let held = vault.lock("google", "alice", "a@example.com").await;
        vault
            .write(&held, &credential("a@example.com", "refreshed"))
            .await;

        let wait = std::time::Duration::from_millis(50);
        let other = tokio::time::timeout(wait, vault.lock("google", "alice", "b@example.com"));
        assert!(other.await.is_ok(), "another credential is not blocked");

        let reconnect = credential("a@example.com", "reconnected");
        let mut store = std::pin::pin!(vault.store("alice", &reconnect));
        assert!(
            tokio::time::timeout(wait, store.as_mut()).await.is_err(),
            "the store must wait for the holder"
        );
        drop(held);
        store.await;
        let path = vault.credential_path("google", "alice", "a@example.com");
        assert_eq!(
            vault.read(&path).await.unwrap().refresh_token,
            "reconnected"
        );
    }

    #[tokio::test]
    async fn list_returns_the_users_credentials_for_the_provider() {
        let tmp = tempfile::tempdir().unwrap();
        let vault = Vault::new(tmp.path().to_path_buf());
        assert!(
            vault.list("google", "alice").await.is_empty(),
            "no directory yet"
        );

        vault
            .store("alice", &credential("b@example.com", "r"))
            .await;
        vault
            .store("alice", &credential("a b@example.com", "r"))
            .await;
        vault
            .store("bob", &credential("bob@example.com", "r"))
            .await;
        let mut other = credential("a@example.com", "r");
        other.provider = "other".to_string();
        vault.store("alice", &other).await;
        // A leftover temporary is not a credential.
        std::fs::write(
            tmp.path().join("google/alice/b@example.com.json.tmp-00"),
            b"partial",
        )
        .unwrap();

        let accounts: Vec<_> = vault
            .list("google", "alice")
            .await
            .into_iter()
            .map(|c| c.account)
            .collect();
        assert_eq!(accounts, vec!["a b@example.com", "b@example.com"]);
    }

    #[tokio::test]
    #[should_panic(expected = "holds a credential for")]
    async fn list_panics_on_a_file_under_another_accounts_name() {
        let tmp = tempfile::tempdir().unwrap();
        let vault = Vault::new(tmp.path().to_path_buf());
        vault
            .store("alice", &credential("a@example.com", "r"))
            .await;
        std::fs::rename(
            vault.credential_path("google", "alice", "a@example.com"),
            vault.credential_path("google", "alice", "z@example.com"),
        )
        .unwrap();
        vault.list("google", "alice").await;
    }

    #[tokio::test]
    async fn remove_deletes_the_held_file_once() {
        let tmp = tempfile::tempdir().unwrap();
        let vault = Vault::new(tmp.path().to_path_buf());
        vault
            .store("alice", &credential("a@example.com", "r"))
            .await;
        let guard = vault.lock("google", "alice", "a@example.com").await;
        assert!(vault.remove(&guard).await);
        assert!(!vault.remove(&guard).await, "already gone");
        assert_eq!(vault.read(guard.path()).await, None);
    }

    #[tokio::test]
    #[should_panic(expected = "under the lock of")]
    async fn a_write_under_another_credentials_lock_panics() {
        let tmp = tempfile::tempdir().unwrap();
        let vault = Vault::new(tmp.path().to_path_buf());
        let guard = vault.lock("google", "alice", "a@example.com").await;
        vault.write(&guard, &credential("b@example.com", "r")).await;
    }

    /// A lock leaves the table when its last holder or waiter is done, so
    /// names that never held a credential leave nothing behind; a waiter
    /// keeps it, and still waits.
    #[tokio::test]
    async fn the_lock_table_keeps_only_locks_in_use() {
        let tmp = tempfile::tempdir().unwrap();
        let vault = Vault::new(tmp.path().to_path_buf());
        for n in 0..3 {
            let guard = vault
                .lock("google", "alice", &format!("invented-{n}@example.com"))
                .await;
            assert_eq!(vault.read(guard.path()).await, None);
        }
        assert_eq!(vault.locked_paths(), 0);

        let held = vault.lock("google", "alice", "a@example.com").await;
        assert_eq!(vault.locked_paths(), 1);
        let mut waiter = std::pin::pin!(vault.lock("google", "alice", "a@example.com"));
        let wait = std::time::Duration::from_millis(50);
        assert!(tokio::time::timeout(wait, waiter.as_mut()).await.is_err());
        drop(held);
        assert_eq!(vault.locked_paths(), 1, "the waiter's lock stays");
        let second = waiter.await;
        assert_eq!(vault.locked_paths(), 1);
        drop(second);
        assert_eq!(vault.locked_paths(), 0);
    }

    #[tokio::test]
    async fn accounts_names_the_users_connected_accounts() {
        let tmp = tempfile::tempdir().unwrap();
        let vault = Vault::new(tmp.path().to_path_buf());
        assert!(vault.accounts("google", "alice").await.is_empty());
        vault
            .store("alice", &credential("b@example.com", "r"))
            .await;
        vault
            .store("alice", &credential("a@example.com", "r"))
            .await;
        assert_eq!(
            vault.accounts("google", "alice").await,
            ["a@example.com", "b@example.com"]
        );
    }
}
