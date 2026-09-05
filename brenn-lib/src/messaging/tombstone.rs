//! A subscriber-keyed table whose departures leave a mark.
//!
//! Two invariants have to hold together on every runtime lookup of a
//! subscriber's wiring. A key that was **never** registered is a host-wiring bug
//! and panics — that is what keeps a subscriber the boot cross-check missed from
//! silently never being woken. A key that **left** is not a bug: the channel does
//! not care who is subscribed, and a publish already in flight holds a directory
//! snapshot that still names the departed subscriber. Telling the two apart is
//! what the tombstone is for.
//!
//! So a retirement moves the key from the live map into the retired set rather
//! than dropping it, and a registration under the same key clears the
//! tombstone — a subscriber replaced under its old slug is simply live again.
//!
//! **Tombstones are kept for the life of the process, deliberately.** The set is
//! bounded by the number of distinct keys ever retired, which is a count of the
//! subscriber slugs an operator has deployed and taken away, and each entry is
//! one `SubscriberEntryKind`. Reclaiming them would mean knowing that no work
//! predating a retirement is still in flight anywhere — and getting that wrong
//! turns a dropped wake into a panic in a healthy process, which is a far worse
//! trade than the bytes.

use std::collections::{HashMap, HashSet};
use std::sync::RwLock;

use super::SubscriberEntryKind;

/// What a lookup found. `Unknown` is a host-wiring bug at every call site;
/// `Retired` is an ordinary race with a departure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lookup<T> {
    /// The key is registered, carrying this value.
    Live(T),
    /// The key was registered and has since left.
    Retired,
    /// The key was never registered.
    Unknown,
}

impl<T> Lookup<T> {
    /// The value if the key is live, discarding the reason it is not.
    pub fn live(self) -> Option<T> {
        match self {
            Lookup::Live(value) => Some(value),
            Lookup::Retired | Lookup::Unknown => None,
        }
    }
}

/// The live table and the tombstones beside it, behind one lock.
struct Tables<V> {
    live: HashMap<SubscriberEntryKind, V>,
    retired: HashSet<SubscriberEntryKind>,
}

/// A registry of per-subscriber wiring that is written when a subscriber joins
/// or leaves and read on the publish and wake paths.
///
/// `what` names the table in its own panics, so one shared implementation still
/// tells an operator which of a process's registries was misused.
pub struct TombstonedRegistry<V> {
    what: &'static str,
    tables: RwLock<Tables<V>>,
}

impl<V> TombstonedRegistry<V> {
    /// An empty registry.
    pub fn new(what: &'static str) -> Self {
        Self::with_live(what, HashMap::new())
    }

    /// A registry holding an initial live table and no tombstones.
    pub fn with_live(what: &'static str, live: HashMap<SubscriberEntryKind, V>) -> Self {
        Self {
            what,
            tables: RwLock::new(Tables {
                live,
                retired: HashSet::new(),
            }),
        }
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, Tables<V>> {
        self.tables
            .read()
            .unwrap_or_else(|_| panic!("{} registry lock poisoned", self.what))
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, Tables<V>> {
        self.tables
            .write()
            .unwrap_or_else(|_| panic!("{} registry lock poisoned", self.what))
    }

    /// Register one key, clearing any tombstone it carried.
    ///
    /// # Panics
    ///
    /// If the key is already live. Two subscribers behind one key is a wiring
    /// bug, and whichever of them a lookup answered with would be arbitrary.
    pub fn register(&self, key: SubscriberEntryKind, value: V) {
        let mut tables = self.write();
        tables.retired.remove(&key);
        let prev = tables.live.insert(key.clone(), value);
        assert!(
            prev.is_none(),
            "{}: duplicate registration for {key:?} — wiring bug",
            self.what
        );
    }

    /// Register a batch, on the same terms as [`Self::register`].
    pub fn register_all(&self, entries: HashMap<SubscriberEntryKind, V>) {
        for (key, value) in entries {
            self.register(key, value);
        }
    }

    /// Retire one key: it leaves the live table and becomes a tombstone, so a
    /// lookup racing the departure answers `Retired` rather than panicking.
    ///
    /// # Panics
    ///
    /// If the key is not live. Retiring what was never registered, or retiring
    /// twice, is a wiring bug.
    pub fn retire(&self, key: &SubscriberEntryKind) {
        let mut tables = self.write();
        let prev = tables.live.remove(key);
        assert!(
            prev.is_some(),
            "{}: retire of unregistered {key:?} — wiring bug",
            self.what
        );
        tables.retired.insert(key.clone());
    }

    /// Whether `key` holds a tombstone: registered once, retired since, and not
    /// registered again.
    pub fn is_retired(&self, key: &SubscriberEntryKind) -> bool {
        self.read().retired.contains(key)
    }

    /// Whether `key` is live right now.
    pub fn is_live(&self, key: &SubscriberEntryKind) -> bool {
        self.read().live.contains_key(key)
    }

    /// Look `key` up, projecting the live value through `f` under the read lock
    /// so the caller reads nothing out of the guard.
    pub fn map<T>(&self, key: &SubscriberEntryKind, f: impl FnOnce(&V) -> T) -> Lookup<T> {
        let tables = self.read();
        match tables.live.get(key) {
            Some(value) => Lookup::Live(f(value)),
            None if tables.retired.contains(key) => Lookup::Retired,
            None => Lookup::Unknown,
        }
    }

    /// How many keys are live and how many are retired.
    pub fn counts(&self) -> (usize, usize) {
        let tables = self.read();
        (tables.live.len(), tables.retired.len())
    }
}

impl<V: Clone> TombstonedRegistry<V> {
    /// Look `key` up, cloning the live value out from behind the lock.
    pub fn get(&self, key: &SubscriberEntryKind) -> Lookup<V> {
        self.map(key, Clone::clone)
    }
}

impl<V> std::fmt::Debug for TombstonedRegistry<V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (live, retired) = self.counts();
        f.debug_struct("TombstonedRegistry")
            .field("what", &self.what)
            .field("live", &live)
            .field("retired", &retired)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(slug: &str) -> SubscriberEntryKind {
        SubscriberEntryKind::Wasm(slug.to_string())
    }

    #[test]
    fn a_never_registered_key_is_unknown_and_a_retired_one_is_not() {
        let registry: TombstonedRegistry<u8> = TombstonedRegistry::new("test");
        assert_eq!(registry.get(&key("ghost")), Lookup::Unknown);
        assert!(!registry.is_retired(&key("ghost")));

        registry.register(key("live"), 7);
        assert_eq!(registry.get(&key("live")), Lookup::Live(7));
        assert!(registry.is_live(&key("live")));

        registry.retire(&key("live"));
        assert_eq!(registry.get(&key("live")), Lookup::Retired);
        assert!(registry.is_retired(&key("live")));
        assert!(!registry.is_live(&key("live")));
        assert_eq!(registry.counts(), (0, 1));
    }

    #[test]
    fn re_registration_clears_the_tombstone() {
        let registry = TombstonedRegistry::new("test");
        registry.register(key("x"), 1);
        registry.retire(&key("x"));
        registry.register(key("x"), 2);
        assert_eq!(registry.get(&key("x")), Lookup::Live(2));
        assert!(!registry.is_retired(&key("x")));
        assert_eq!(registry.counts(), (1, 0));
    }

    #[test]
    fn map_projects_without_cloning() {
        let registry = TombstonedRegistry::new("test");
        registry.register(key("x"), String::from("payload"));
        assert_eq!(registry.map(&key("x"), String::len), Lookup::Live(7));
        assert_eq!(registry.map(&key("y"), String::len), Lookup::Unknown);
    }

    #[test]
    #[should_panic(expected = "duplicate registration for")]
    fn registering_a_live_key_twice_panics() {
        let registry = TombstonedRegistry::new("test");
        registry.register(key("x"), 1);
        registry.register(key("x"), 2);
    }

    #[test]
    #[should_panic(expected = "retire of unregistered")]
    fn retiring_a_key_that_was_never_live_panics() {
        let registry: TombstonedRegistry<u8> = TombstonedRegistry::new("test");
        registry.retire(&key("ghost"));
    }

    #[test]
    #[should_panic(expected = "retire of unregistered")]
    fn retiring_twice_panics() {
        let registry = TombstonedRegistry::new("test");
        registry.register(key("x"), 1);
        registry.retire(&key("x"));
        registry.retire(&key("x"));
    }
}
