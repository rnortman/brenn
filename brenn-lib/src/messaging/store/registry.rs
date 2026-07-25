//! The process's in-memory retention stores, one per non-durable channel.
//!
//! Every non-durable channel in a process is created together at boot and dies
//! together at shutdown, so their stores are built in one pass under one
//! incarnation epoch and held here for the lifetime of the process. A durable
//! channel needs no entry: its retention lives in the database, so its store is
//! a handle that can be minted on demand from the channel entry.
//!
//! This is the store half of the unified registry: the directory says which
//! channels exist, and this says where each non-durable one's messages sit.

use std::collections::HashMap;
use std::sync::Arc;

use uuid::Uuid;

use crate::messaging::ChannelEntry;

use super::RingStore;

/// The ring-backed stores of one process, keyed by channel UUID.
#[derive(Debug)]
pub struct RingStores {
    /// Stamped on every store built here. A resume carrying a different epoch
    /// is a guaranteed gap, which is how restart loss becomes visible on every
    /// non-durable channel at once.
    epoch: Uuid,
    by_uuid: HashMap<Uuid, Arc<RingStore>>,
    by_address: HashMap<String, Uuid>,
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
            by_uuid: HashMap::new(),
            by_address: HashMap::new(),
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
        let epoch = Uuid::new_v4();
        let mut by_uuid = HashMap::with_capacity(entries.len());
        let mut by_address = HashMap::with_capacity(entries.len());
        for entry in entries {
            assert!(
                !entry.capabilities().durable,
                "messaging stores: channel {:?} is durable and must not be ring-backed",
                entry.address,
            );
            let store = Arc::new(RingStore::with_fan_out_capacity(
                entry.uuid,
                entry.address.clone(),
                entry.resolved_channel.retain_depth,
                epoch,
                fan_out_capacity,
            ));
            let displaced = by_uuid.insert(entry.uuid, store);
            assert!(
                displaced.is_none(),
                "messaging stores: channel {:?} declared twice",
                entry.address,
            );
            by_address.insert(entry.address.clone(), entry.uuid);
        }
        Self {
            epoch,
            by_uuid,
            by_address,
        }
    }

    /// The incarnation every store here carries.
    pub fn epoch(&self) -> Uuid {
        self.epoch
    }

    /// The store for a channel UUID, or `None` if the channel is durable or
    /// unknown.
    pub fn get(&self, channel_uuid: &Uuid) -> Option<&Arc<RingStore>> {
        self.by_uuid.get(channel_uuid)
    }

    /// The store for a canonical scheme-prefixed address.
    pub fn get_by_address(&self, address: &str) -> Option<&Arc<RingStore>> {
        self.by_uuid.get(self.by_address.get(address)?)
    }

    /// Every store, in unspecified order.
    pub fn iter(&self) -> impl Iterator<Item = &Arc<RingStore>> {
        self.by_uuid.values()
    }

    /// The number of stores in the registry.
    pub fn len(&self) -> usize {
        self.by_uuid.len()
    }

    /// True when the registry holds no stores.
    pub fn is_empty(&self) -> bool {
        self.by_uuid.is_empty()
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
        for store in stores.iter() {
            assert_eq!(store.epoch(), stores.epoch());
        }
    }

    #[test]
    fn lookup_by_uuid_and_address_agree() {
        let a = entry("ephemeral:a");
        let stores = RingStores::build(&[a.clone(), entry("local:b")]);
        let by_uuid = stores.get(&a.uuid).expect("registered");
        let by_address = stores.get_by_address(&a.address).expect("registered");
        assert!(Arc::ptr_eq(by_uuid, by_address));
        assert_eq!(by_uuid.address(), "ephemeral:a");
        assert!(stores.get_by_address("ephemeral:nope").is_none());
    }

    #[test]
    fn the_same_name_under_two_schemes_is_two_stores() {
        let stores = RingStores::build(&[entry("ephemeral:dup"), entry("local:dup")]);
        assert_eq!(stores.len(), 2);
        let eph = stores.get_by_address("ephemeral:dup").expect("registered");
        let loc = stores.get_by_address("local:dup").expect("registered");
        assert!(!Arc::ptr_eq(eph, loc));
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

    #[test]
    fn an_empty_registry_still_has_an_epoch() {
        let stores = RingStores::empty();
        assert!(stores.is_empty());
        assert_ne!(stores.epoch(), Uuid::nil());
    }
}
