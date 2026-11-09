//! The swappable key-provider table.
//!
//! `api-request` looks a key provider up per call through this type, so a
//! reload can install the candidate's resolved providers by swapping the map
//! whole and have the next call send the new keys.
//!
//! Readers take a snapshot per operation: [`ApiKeyTable::load`] clones one
//! `Arc` out from under a read lock and releases it. The snapshot is owned, so
//! it may be held across an `await`; the lock never is. A request that began
//! under the old map completes with the key it resolved there.

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use super::ApiKeyProviderConfig;

/// The resolved key providers `api-request` reads, by name. Swapped whole on a
/// reload; readers take a snapshot per operation and never hold the lock
/// across an await.
#[derive(Clone)]
pub struct ApiKeyTable(Arc<RwLock<Arc<BTreeMap<String, ApiKeyProviderConfig>>>>);

impl ApiKeyTable {
    pub fn new(providers: Arc<BTreeMap<String, ApiKeyProviderConfig>>) -> Self {
        Self(Arc::new(RwLock::new(providers)))
    }

    /// A table holding no provider.
    pub fn empty() -> Self {
        Self::new(Arc::new(BTreeMap::new()))
    }

    /// The current map. Cheap: one `Arc` clone under a read lock.
    ///
    /// Panics if a writer panicked while holding the lock — a poisoned table is
    /// a map nobody can vouch for, and it decides which key a request carries.
    pub fn load(&self) -> Arc<BTreeMap<String, ApiKeyProviderConfig>> {
        Arc::clone(&self.0.read().expect("BUG: ApiKeyTable lock poisoned"))
    }

    /// Install a new map, visible to every subsequent [`Self::load`].
    pub fn store(&self, providers: Arc<BTreeMap<String, ApiKeyProviderConfig>>) {
        *self.0.write().expect("BUG: ApiKeyTable lock poisoned") = providers;
    }
}

/// The provider names only: a key never reaches debug output.
impl std::fmt::Debug for ApiKeyTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ApiKeyTable")
            .field("providers", &self.load().keys().collect::<Vec<_>>())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;
    use std::path::Path;

    use super::*;
    use crate::config::test_api_key_provider_raw;
    use crate::config::{ApiKeySourceRaw, BrennConfig, resolve_api_key_providers};

    /// One deployment-key provider per name, each key `k3y-<name>`.
    fn map(dir: &Path, names: &[&str]) -> Arc<BTreeMap<String, ApiKeyProviderConfig>> {
        let mut config = BrennConfig::default();
        for name in names {
            let key_file = dir.join(name);
            std::fs::write(&key_file, format!("k3y-{name}")).unwrap();
            std::fs::set_permissions(&key_file, std::fs::Permissions::from_mode(0o600)).unwrap();
            let mut raw = test_api_key_provider_raw("https://api.example.com");
            raw.source = ApiKeySourceRaw::Deployment { key_file };
            config.api_key_providers.insert((*name).to_string(), raw);
        }
        Arc::new(resolve_api_key_providers(&config))
    }

    #[test]
    fn load_after_store_returns_the_new_map() {
        let tmp = tempfile::tempdir().unwrap();
        let table = ApiKeyTable::new(map(tmp.path(), &["maps"]));
        assert!(table.load().contains_key("maps"));

        let next = map(tmp.path(), &["maps", "mail"]);
        table.store(Arc::clone(&next));

        let seen = table.load();
        assert!(seen.contains_key("mail"));
        assert!(Arc::ptr_eq(&seen, &next));
    }

    #[test]
    fn a_snapshot_taken_before_a_store_is_unaffected_by_it() {
        let tmp = tempfile::tempdir().unwrap();
        let table = ApiKeyTable::new(map(tmp.path(), &["maps"]));
        let before = table.load();
        table.store(map(tmp.path(), &["maps", "mail"]));
        assert_eq!(before.len(), 1);
        assert_eq!(table.load().len(), 2);
    }

    #[test]
    fn clones_share_one_table() {
        let tmp = tempfile::tempdir().unwrap();
        let table = ApiKeyTable::new(map(tmp.path(), &["maps"]));
        let clone = table.clone();
        clone.store(map(tmp.path(), &["maps", "mail"]));
        assert_eq!(table.load().len(), 2);
    }

    #[test]
    fn empty_table_holds_no_provider() {
        assert!(ApiKeyTable::empty().load().is_empty());
    }

    #[test]
    fn debug_output_names_providers_and_never_a_key() {
        let tmp = tempfile::tempdir().unwrap();
        let shown = format!("{:?}", ApiKeyTable::new(map(tmp.path(), &["maps"])));
        assert!(shown.contains("maps"), "{shown}");
        assert!(!shown.contains("k3y"), "{shown}");
    }
}
