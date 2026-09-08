//! The swappable agent registry.
//!
//! Every gate that decides on an agent's authority — publish, delivery, query,
//! listing, dynamic subscribe, MQTT ingress, automation fire, push — looks the
//! agent up per call. This type is the one place that lookup goes through, so a
//! reload can install a new resolved map by swapping it whole and have every
//! gate decide on the new one from that instant.
//!
//! Readers take a snapshot per operation: [`AppTable::load`] clones one `Arc`
//! out from under a read lock and releases it. The snapshot is owned, so it may
//! be held across an `await`; the lock never is. An operation that began under
//! the old map completes under it and the next begins under the new one, which
//! is what a restart between the two operations would have produced.

use std::sync::{Arc, RwLock};

use indexmap::IndexMap;

use super::AppConfig;

/// The resolved agent registry every gate reads. Swapped whole on a reload;
/// readers take a snapshot per operation and never hold the lock across an
/// await.
#[derive(Clone)]
pub struct AppTable(Arc<RwLock<Versioned>>);

/// The map and the count of swaps that produced it, under one lock so a reader
/// cannot pair one map with another's generation.
struct Versioned {
    apps: Arc<IndexMap<String, AppConfig>>,
    generation: u64,
}

impl AppTable {
    pub fn new(apps: Arc<IndexMap<String, AppConfig>>) -> Self {
        Self(Arc::new(RwLock::new(Versioned {
            apps,
            generation: 0,
        })))
    }

    /// An empty table, for tests and for hosts with no agents.
    pub fn empty() -> Self {
        Self::new(Arc::new(IndexMap::new()))
    }

    /// The current map. Cheap: one `Arc` clone under a read lock.
    ///
    /// Panics if a writer panicked while holding the lock — a poisoned table is
    /// a map nobody can vouch for, and the gates that read it decide who may
    /// publish.
    pub fn load(&self) -> Arc<IndexMap<String, AppConfig>> {
        self.load_versioned().0
    }

    /// The current map and the generation it was installed at, read together.
    ///
    /// A process spawned from a map records that generation and is condemned
    /// when it registers against a table that has moved past it: the spawn
    /// reads the map, builds a process from it over seconds, and a swap in that
    /// window would otherwise leave a live process nobody condemns.
    pub fn load_versioned(&self) -> (Arc<IndexMap<String, AppConfig>>, u64) {
        let held = self.0.read().expect("BUG: AppTable lock poisoned");
        (Arc::clone(&held.apps), held.generation)
    }

    /// How many maps this table has been given. Boot's is generation 0 and
    /// every reload's swap is one more.
    pub fn generation(&self) -> u64 {
        self.0
            .read()
            .expect("BUG: AppTable lock poisoned")
            .generation
    }

    /// Install a new map, visible to every subsequent [`Self::load`].
    pub fn store(&self, apps: Arc<IndexMap<String, AppConfig>>) {
        let mut held = self.0.write().expect("BUG: AppTable lock poisoned");
        held.apps = apps;
        held.generation += 1;
    }

    /// One agent, held against the snapshot this call takes. `None` when the
    /// table has no such slug.
    pub fn get(&self, slug: &str) -> Option<AppRef> {
        let (apps, generation) = self.load_versioned();
        let index = apps.get_index_of(slug)?;
        Some(AppRef {
            apps,
            index,
            generation,
        })
    }

    /// Whether two handles name the same table, for host-wiring asserts.
    pub fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

/// One agent, borrowed out of a snapshot of the table.
///
/// Holds the snapshot it was taken from, so the borrow is of an owned map and
/// stays valid across an `await` and across a reload's swap: the operation that
/// took it completes on the agent it read, and the next operation takes a fresh
/// one. Derefs to the agent, so a caller reads its fields as it always did.
/// The slug is resolved to a position once, at construction: the snapshot is
/// owned and immutable, so the position is stable for the life of the `AppRef`
/// and a field read is an index rather than a hash.
pub struct AppRef {
    apps: Arc<IndexMap<String, AppConfig>>,
    index: usize,
    generation: u64,
}

impl AppRef {
    /// The table generation this agent was read at, for a spawn that has to
    /// record which map it built its process from.
    pub fn generation(&self) -> u64 {
        self.generation
    }
}

impl std::ops::Deref for AppRef {
    type Target = AppConfig;

    fn deref(&self) -> &AppConfig {
        self.apps
            .get_index(self.index)
            .expect("BUG: AppRef outlived its own snapshot's entry")
            .1
    }
}

impl std::fmt::Debug for AppRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&**self, f)
    }
}

/// A bare map becomes a table nobody else holds — a table no reload will ever
/// swap. That is what a fixture wants and what a running host must never build,
/// so the conversion exists only where fixtures are compiled: production reaches
/// a table by cloning the one handle its boot constructed.
#[cfg(any(test, feature = "testutils"))]
impl From<Arc<IndexMap<String, AppConfig>>> for AppTable {
    fn from(apps: Arc<IndexMap<String, AppConfig>>) -> Self {
        Self::new(apps)
    }
}

impl std::fmt::Debug for AppTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AppTable")
            .field("apps", &self.load().len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::test_fixtures::test_app_config;

    fn map(slugs: &[&str]) -> Arc<IndexMap<String, AppConfig>> {
        let mut m = IndexMap::new();
        for slug in slugs {
            m.insert((*slug).to_string(), test_app_config(slug));
        }
        Arc::new(m)
    }

    #[test]
    fn load_after_store_returns_the_new_map() {
        let table = AppTable::new(map(&["home"]));
        assert!(table.load().contains_key("home"));

        let next = map(&["home", "assistant"]);
        table.store(Arc::clone(&next));

        let seen = table.load();
        assert!(seen.contains_key("assistant"));
        assert!(Arc::ptr_eq(&seen, &next));
    }

    #[test]
    fn a_snapshot_taken_before_a_store_is_unaffected_by_it() {
        let table = AppTable::new(map(&["home"]));
        let before = table.load();
        table.store(map(&["home", "assistant"]));
        assert_eq!(before.len(), 1);
        assert_eq!(table.load().len(), 2);
    }

    #[test]
    fn clones_share_one_table() {
        let table = AppTable::new(map(&["home"]));
        let clone = table.clone();
        assert!(table.ptr_eq(&clone));
        clone.store(map(&["home", "assistant"]));
        assert_eq!(table.load().len(), 2);
    }

    #[test]
    fn a_store_bumps_the_generation_and_a_read_pairs_it_with_its_map() {
        let table = AppTable::new(map(&["home"]));
        assert_eq!(table.generation(), 0);
        assert_eq!(table.get("home").expect("agent present").generation(), 0);

        table.store(map(&["home", "assistant"]));
        assert_eq!(table.generation(), 1);
        let (apps, generation) = table.load_versioned();
        assert_eq!(generation, 1);
        assert_eq!(apps.len(), 2);
        assert_eq!(
            table.get("assistant").expect("agent present").generation(),
            1
        );
    }

    #[test]
    fn empty_table_holds_no_apps() {
        assert!(AppTable::empty().load().is_empty());
    }
}
