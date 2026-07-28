//! The process's in-memory retention stores, one per non-durable channel.
//!
//! Most non-durable channels in a process are created together at boot and all
//! of them die together at shutdown, so their stores share one incarnation
//! epoch and are held here for the lifetime of the process. A durable channel
//! needs no entry: its retention lives in the database, so its store is a
//! handle that can be minted on demand from the channel entry.
//!
//! This is the store half of the unified registry: the directory says which
//! channels exist, and this says where each non-durable one's messages sit.
//!
//! The set is not fixed at boot. A conversation's token stream and pre-warm
//! channels come and go with the conversation, so [`RingStores::register`] and
//! [`RingStores::deregister`] mutate it at runtime under the same
//! `RwLock`-behind-`Arc` shape the directory uses: readers take a brief read
//! lock and leave with a cloned `Arc<RingStore>`, so a publisher that resolved
//! a store before a concurrent mutation keeps operating on its handle and the
//! mutation applies to the next resolve.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use uuid::Uuid;

use crate::messaging::ChannelEntry;

use super::RingStore;

/// The mutable index pair, held together so they cannot disagree.
#[derive(Debug, Default)]
struct StoresInner {
    by_uuid: HashMap<Uuid, Arc<RingStore>>,
    by_address: HashMap<String, Uuid>,
}

/// The ring-backed stores of one process, keyed by channel UUID.
#[derive(Debug)]
pub struct RingStores {
    /// Stamped on every store built here. A resume carrying a different epoch
    /// is a guaranteed gap, which is how restart loss becomes visible on every
    /// non-durable channel at once.
    epoch: Uuid,
    /// Live-fan-out ring size given to every store, including one registered
    /// after boot — a runtime-registered channel must behave like a declared
    /// one, and a test that shrank the capacity means it for the whole process.
    fan_out_capacity: u32,
    inner: RwLock<StoresInner>,
}

impl RingStores {
    /// A process with no non-durable channels.
    ///
    /// Still carries an epoch: a store set that gains no channels is
    /// indistinguishable from one that has none declared, and both are one
    /// incarnation.
    pub fn empty() -> Self {
        Self {
            epoch: Uuid::new_v4(),
            fan_out_capacity: super::ring::RING_FAN_OUT_CAPACITY,
            inner: RwLock::new(StoresInner::default()),
        }
    }

    /// One store per non-durable entry, all sharing a freshly minted epoch.
    ///
    /// # Panics
    ///
    /// If an entry is durable — a durable channel's retention belongs in the
    /// database, and giving it a ring here would lose its data at restart while
    /// its configuration promised otherwise. Also if two entries name the same
    /// channel.
    pub fn build(entries: &[ChannelEntry]) -> Self {
        Self::build_with_fan_out_capacity(entries, super::ring::RING_FAN_OUT_CAPACITY)
    }

    /// The same registry with a chosen live-fan-out ring size on every store.
    ///
    /// Production always takes the default; a test that wants to overrun a
    /// consumer's fan-out ring builds a small one here rather than committing
    /// hundreds of messages.
    pub fn build_with_fan_out_capacity(entries: &[ChannelEntry], fan_out_capacity: u32) -> Self {
        let stores = Self {
            epoch: Uuid::new_v4(),
            fan_out_capacity,
            inner: RwLock::new(StoresInner::default()),
        };
        for entry in entries {
            let fresh = stores.register(entry);
            assert!(
                fresh,
                "messaging stores: channel {:?} declared twice",
                entry.address,
            );
        }
        stores
    }

    /// The incarnation every store here carries.
    pub fn epoch(&self) -> Uuid {
        self.epoch
    }

    /// Give a non-durable channel a store, if it has none yet.
    ///
    /// Returns `true` when a store was built, `false` when one was already
    /// registered for this UUID — the idempotence a re-provisioned conversation
    /// depends on. Re-registration deliberately keeps the existing store rather
    /// than swapping in an empty one: the messages already in the ring are the
    /// channel's contents, and a subscriber holding a position into them must
    /// not have that position invalidated by a redundant provisioning call.
    ///
    /// # Panics
    ///
    /// If the entry is durable (its retention belongs in the database), or if
    /// its address is already indexed under a different UUID — two channels
    /// answering to one name is a host bug either way.
    pub fn register(&self, entry: &ChannelEntry) -> bool {
        assert!(
            !entry.capabilities().durable,
            "messaging stores: channel {:?} is durable and must not be ring-backed",
            entry.address,
        );
        let mut inner = self.inner.write().expect("ring stores lock poisoned");
        if inner.by_uuid.contains_key(&entry.uuid) {
            return false;
        }
        if let Some(other) = inner.by_address.get(&entry.address) {
            panic!(
                "messaging stores: address {:?} is already registered under uuid {other} \
                 (registering {})",
                entry.address, entry.uuid,
            );
        }
        let store = Arc::new(RingStore::with_fan_out_capacity(
            entry.uuid,
            entry.address.clone(),
            entry.resolved_channel.retain_depth,
            self.epoch,
            self.fan_out_capacity,
        ));
        inner.by_address.insert(entry.address.clone(), entry.uuid);
        inner.by_uuid.insert(entry.uuid, store);
        true
    }

    /// Drop a channel's store, returning `true` if there was one.
    ///
    /// The store's messages go with it — that is the point: a non-durable
    /// channel's contents have nowhere else to live, so tearing down the
    /// channel must tear down the ring rather than orphan it.
    pub fn deregister(&self, channel_uuid: &Uuid) -> bool {
        let mut inner = self.inner.write().expect("ring stores lock poisoned");
        let Some(store) = inner.by_uuid.remove(channel_uuid) else {
            return false;
        };
        inner.by_address.remove(store.address());
        true
    }

    /// The store for a channel UUID, or `None` if the channel is durable or
    /// unknown.
    pub fn get(&self, channel_uuid: &Uuid) -> Option<Arc<RingStore>> {
        let inner = self.inner.read().expect("ring stores lock poisoned");
        inner.by_uuid.get(channel_uuid).cloned()
    }

    /// The store for a canonical scheme-prefixed address.
    pub fn get_by_address(&self, address: &str) -> Option<Arc<RingStore>> {
        let inner = self.inner.read().expect("ring stores lock poisoned");
        inner.by_uuid.get(inner.by_address.get(address)?).cloned()
    }

    /// Every store, in unspecified order.
    pub fn stores(&self) -> Vec<Arc<RingStore>> {
        let inner = self.inner.read().expect("ring stores lock poisoned");
        inner.by_uuid.values().cloned().collect()
    }

    /// The number of stores in the registry.
    pub fn len(&self) -> usize {
        self.inner
            .read()
            .expect("ring stores lock poisoned")
            .by_uuid
            .len()
    }

    /// True when the registry holds no stores.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use brenn_envelope::ChannelScheme;

    use crate::messaging::config::{Depth, NoiseLevel, ResolvedChannel, Sink};
    use crate::messaging::{WakeMin, nondurable_channel_uuid};

    fn entry(address: &str) -> ChannelEntry {
        let (scheme, name) = ChannelScheme::split(address).expect("test address carries a scheme");
        ChannelEntry {
            uuid: nondurable_channel_uuid(scheme, name),
            address: address.to_string(),
            description: None,
            resolved_channel: ResolvedChannel {
                send_rate: Default::default(),
                push_depth: Depth::Bounded(4),
                retain_depth: Depth::Bounded(8),
                standing_retain_depth: Depth::Bounded(8),
                noise: NoiseLevel::Metered,
                sink: Sink::Drop,
                wake_min: WakeMin::Normal,
            },
            subscribers: vec![],
            transport_type: scheme,
            mount: None,
        }
    }

    fn durable_entry() -> ChannelEntry {
        let mut e = entry("ephemeral:x");
        e.address = "brenn:x".to_string();
        e.transport_type = ChannelScheme::Brenn;
        e
    }

    #[test]
    fn every_store_carries_the_one_epoch() {
        let stores = RingStores::build(&[entry("ephemeral:a"), entry("local:b")]);
        assert_eq!(stores.len(), 2);
        for store in stores.stores() {
            assert_eq!(store.epoch(), stores.epoch());
        }
    }

    #[test]
    fn lookup_by_uuid_and_address_agree() {
        let a = entry("ephemeral:a");
        let stores = RingStores::build(&[a.clone(), entry("local:b")]);
        let by_uuid = stores.get(&a.uuid).expect("registered");
        let by_address = stores.get_by_address(&a.address).expect("registered");
        assert!(Arc::ptr_eq(&by_uuid, &by_address));
        assert_eq!(by_uuid.address(), "ephemeral:a");
        assert!(stores.get_by_address("ephemeral:nope").is_none());
    }

    #[test]
    fn the_same_name_under_two_schemes_is_two_stores() {
        let stores = RingStores::build(&[entry("ephemeral:dup"), entry("local:dup")]);
        assert_eq!(stores.len(), 2);
        let eph = stores.get_by_address("ephemeral:dup").expect("registered");
        let loc = stores.get_by_address("local:dup").expect("registered");
        assert!(!Arc::ptr_eq(&eph, &loc));
        assert!(eph.capabilities().transportable);
        assert!(!loc.capabilities().transportable);
    }

    #[test]
    #[should_panic(expected = "is durable and must not be ring-backed")]
    fn a_durable_entry_is_rejected() {
        RingStores::build(&[durable_entry()]);
    }

    #[test]
    #[should_panic(expected = "declared twice")]
    fn a_repeated_channel_is_rejected() {
        RingStores::build(&[entry("ephemeral:a"), entry("ephemeral:a")]);
    }

    /// A store registered after boot is indistinguishable from a declared one:
    /// same epoch, resolvable both ways, and its retain depth is its own.
    #[test]
    fn a_runtime_registration_joins_the_same_incarnation() {
        let stores = RingStores::build(&[entry("ephemeral:declared")]);
        let late = entry("ephemeral:late");

        assert!(stores.register(&late), "a fresh channel is registered");
        assert_eq!(stores.len(), 2);

        let store = stores.get(&late.uuid).expect("registered");
        assert_eq!(store.epoch(), stores.epoch());
        assert!(Arc::ptr_eq(
            &store,
            &stores.get_by_address("ephemeral:late").expect("registered"),
        ));
    }

    /// Re-registering keeps the ring that is already there. A conversation
    /// re-provisioned while live must not have its stream emptied underneath a
    /// subscriber's position.
    #[test]
    fn re_registering_keeps_the_existing_store() {
        let stores = RingStores::empty();
        let e = entry("ephemeral:a");
        assert!(stores.register(&e));
        let first = stores.get(&e.uuid).expect("registered");

        assert!(!stores.register(&e), "a second register is a no-op");
        assert_eq!(stores.len(), 1);
        assert!(Arc::ptr_eq(
            &first,
            &stores.get(&e.uuid).expect("still there")
        ));
    }

    /// Deregistration drops both indexes, and the address is free to be
    /// registered again afterwards.
    #[test]
    fn deregistration_frees_the_uuid_and_the_address() {
        let stores = RingStores::empty();
        let e = entry("ephemeral:a");
        stores.register(&e);

        assert!(stores.deregister(&e.uuid));
        assert!(stores.is_empty());
        assert!(stores.get(&e.uuid).is_none());
        assert!(stores.get_by_address("ephemeral:a").is_none());

        assert!(
            !stores.deregister(&e.uuid),
            "a second removal finds nothing"
        );
        assert!(stores.register(&e), "the name is free again");
    }

    #[test]
    #[should_panic(expected = "already registered under uuid")]
    fn two_uuids_may_not_share_one_address() {
        let stores = RingStores::empty();
        let e = entry("ephemeral:a");
        stores.register(&e);
        let mut impostor = e.clone();
        impostor.uuid = Uuid::new_v4();
        stores.register(&impostor);
    }

    #[test]
    fn an_empty_registry_still_has_an_epoch() {
        let stores = RingStores::empty();
        assert!(stores.is_empty());
        assert_ne!(stores.epoch(), Uuid::nil());
    }
}
