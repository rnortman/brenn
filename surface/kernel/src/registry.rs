//! The surface's registration table, and the two reconcile passes that put the
//! wiring in force.
//!
//! A bindings document says which channels exist, how deep the page keeps them,
//! and which component instance reads which of them through which port. A page
//! at runtime holds three things that must agree with it: the channel stores it
//! retains, the reader positions inside them, and the wire subscriptions it
//! holds open. Reconciling is making those three agree, and it happens at
//! exactly two moments — a bindings document arrives, or an instance registers
//! or leaves.
//!
//! Two passes, in that order, because the second depends on the first:
//!
//! 1. [`reconcile_stores`] — the channels. Every channel the document names has
//!    a store at the depth the document resolves for it; every channel it no
//!    longer names loses one.
//! 2. [`Registrations::reconcile`] — the readers and the wire. Every input
//!    binding of every registered instance holds a position in its channel's
//!    store and a reference on that channel's subscription; a binding that
//!    vanished holds neither.
//!
//! Both are idempotent, which is what lets a registration and an attachment
//! call the same code without either knowing about the other, and what makes
//! running the whole thing again on every reconnect cost nothing when the
//! wiring did not change.
//!
//! The subscriber is the **binding**, not the instance: two ports of one
//! instance on one channel read at their own depths and lag independently,
//! exactly as two backend applications on one channel would. That is what
//! [`BindingKey`] names, and it is the only thing the attacher-generic store
//! knows about its readers.

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;

use brenn_attach_client::store::ChannelStores;
use brenn_attach_client::subs::{ResumePolicy, Subscriptions};
use brenn_attach_proto::ClientFrame;
use brenn_queue::CursorOverflow;
use brenn_surface_schema::{RESERVED_LOCAL_CHANNELS, reserved_local_channel};
use uuid::Uuid;

use crate::bindings::{AppliedBindings, channel_is_transportable};

/// One input binding as a reader of its channel's store: the instance whose
/// position it is, and the port it reads through.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct BindingKey {
    pub instance: String,
    pub port: String,
}

impl BindingKey {
    pub fn new(instance: &str, port: &str) -> Self {
        Self {
            instance: instance.to_string(),
            port: port.to_string(),
        }
    }
}

/// The page's channel stores, read by binding.
pub type SurfaceStores = ChannelStores<BindingKey>;

/// The page's stores at their first instant: the reserved control planes and
/// nothing else.
///
/// The reserved planes exist before any attachment — they are contract-defined,
/// so their depths come from the contract rather than from a peer that has not
/// answered yet, and an unbound plane has no binding windows to fold in. Seeding
/// them here is what "auto-bound by the kernel" means, and [`reconcile_stores`]
/// never drops one: no document declares them into existence, so none can
/// un-declare them.
pub fn new_stores(epoch: Uuid) -> SurfaceStores {
    let mut stores = SurfaceStores::new(epoch);
    for plane in RESERVED_LOCAL_CHANNELS {
        stores.ensure(plane.address, plane.ring_depth);
    }
    stores
}

/// What a store reconcile cost the page, for the caller's loudness ladder and
/// counters.
#[derive(Debug, Default)]
pub struct StoreReconcile {
    /// Per channel, the reader positions a shrink retired out from under. A
    /// depth the operator lowered is as accountable a cause of loss as a burst,
    /// and the ladder must see it either way. Address-ordered.
    pub retired: Vec<(String, Vec<CursorOverflow<BindingKey>>)>,
    /// Per channel, the identity that parked each schedule the page dropped with
    /// the channel's store — one entry per parked message, so a caller charging
    /// a counter per lost schedule reads it straight off. Address-ordered.
    ///
    /// A dropped schedule is the only account of a timer a component believes it
    /// set, so it is never silent. Only confined channels can appear: a
    /// transportable channel's deferral authority is the peer, so a discarded
    /// mirror holds no schedule to lose.
    pub lost_schedules: Vec<(String, Vec<String>)>,
}

/// Bring the page's channel stores into line with the wiring now in force.
///
/// Surviving stores are **preserved**, contents, positions and seq counter
/// intact: a store is exactly what a post-reconnect window's context is read
/// from, and discarding one here would manufacture a loss the link never caused.
/// A surviving store is retuned in place, which takes effect at each reader's
/// next window. A store no surviving channel names is dropped — nothing can
/// route on it again — and what its deferred set was holding is reported.
///
/// The depth is the document's resolved one: the fold over the channel's
/// bindings of `max(push_depth, retain_depth)`, floored by a declared local
/// channel's ring depth and by a reserved plane's contract depth. Both halves of
/// the per-binding max are load-bearing — `retain_depth` is what a binding reads
/// as context and `push_depth` is what it can be handed as new — and the store
/// is the only thing holding either.
pub fn reconcile_stores(bindings: &AppliedBindings, stores: &mut SurfaceStores) -> StoreReconcile {
    let dropped = stores.retain(|channel| {
        bindings.store_depth(channel).is_some() || reserved_local_channel(channel).is_some()
    });
    let mut lost_schedules = Vec::new();
    for (channel, store) in dropped {
        let senders: Vec<String> = store.parked_senders().map(str::to_string).collect();
        if senders.is_empty() {
            continue;
        }
        tracing::warn!(
            %channel,
            schedules = senders.len(),
            "surface client: this bindings document no longer declares the channel, dropping the \
             schedules parked on it"
        );
        lost_schedules.push((channel, senders));
    }
    let mut retired = Vec::new();
    for (channel, depth) in bindings.store_depths() {
        let overflow = stores.ensure(channel, depth);
        if !overflow.is_empty() {
            retired.push((channel.to_string(), overflow));
        }
    }
    StoreReconcile {
        retired,
        lost_schedules,
    }
}

/// One registered instance's place in the wiring.
#[derive(Debug)]
struct Registration {
    /// Which registration under this instance id this is. An instance
    /// deregistered and registered again is a *different* mount with the same
    /// spelling — its own positions, its own scheduler state — so work minted for
    /// the previous one must not be applied to it.
    generation: u64,
    /// The transportable channels this instance holds a subscription reference
    /// on, one entry **per input binding** — so two ports of one instance on one
    /// channel hold two references on the one subscription they share.
    ///
    /// A registered instance is a subscriber like any other, and nothing else
    /// opens its subscriptions. Depth-0 bindings are included: a depth-0 binding
    /// still sees its channel, and on a transportable channel seeing it means
    /// subscribing to it. Confined channels are absent — they have no
    /// subscription, no refcount and no resume cursor, because no peer is in the
    /// loop.
    subs: Vec<String>,
    /// Whether the instance is terminal (a trap, or a `fatal`-rung overflow).
    /// Delivery stops and its positions are dropped; its subscription references
    /// are not, because those belong to the registration and it is still
    /// registered.
    failed: bool,
}

impl Registration {
    fn new(generation: u64) -> Self {
        Self {
            generation,
            subs: Vec::new(),
            failed: false,
        }
    }
}

/// Which component instances are registered for delivery, and what each one
/// holds because of it.
///
/// Ordered by instance id, so every pass over the table emits its frames in one
/// deterministic order and a page with several instances reconciles
/// reproducibly.
#[derive(Debug, Default)]
pub struct Registrations {
    entries: BTreeMap<String, Registration>,
    /// Stamped onto each entry at registration, so two registrations under one
    /// instance id are distinguishable for as long as the page lives.
    next_generation: u64,
}

impl Registrations {
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether `instance` is registered for delivery.
    pub fn is_registered(&self, instance: &str) -> bool {
        self.entries.contains_key(instance)
    }

    /// Whether `instance` is terminal. `false` for an unregistered instance,
    /// which is owed nothing either way.
    pub fn is_failed(&self, instance: &str) -> bool {
        self.entries.get(instance).is_some_and(|r| r.failed)
    }

    /// Which registration under `instance` is in force, or `None` when none is.
    ///
    /// What a pass holding work minted for a mount checks before applying it: the
    /// id alone would name a successor registration that mount's work says nothing
    /// about.
    pub fn generation(&self, instance: &str) -> Option<u64> {
        self.entries.get(instance).map(|r| r.generation)
    }

    /// Every registered instance, in id order.
    pub fn instances(&self) -> impl Iterator<Item = &str> {
        self.entries.keys().map(String::as_str)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Register an instance for delivery and give it its positions and
    /// subscription references.
    ///
    /// `wiring` is `None` before the first bindings document of the page, where
    /// there is no table to reconcile against and nothing to do: the document,
    /// when it lands, reconciles this instance in with everything else.
    ///
    /// # Panics
    ///
    /// If the instance is already registered. The second registration would
    /// silently orphan the first entry's positions, so this is the fail-fast
    /// backstop behind the caller's own registration gate.
    pub fn register(
        &mut self,
        instance: &str,
        wiring: Option<&AppliedBindings>,
        stores: &mut SurfaceStores,
        subs: &mut Subscriptions,
    ) -> Vec<ClientFrame> {
        let generation = self.next_generation;
        self.next_generation += 1;
        let prior = self
            .entries
            .insert(instance.to_string(), Registration::new(generation));
        assert!(
            prior.is_none(),
            "surface client: instance {instance:?} registered twice"
        );
        // Positions and references both come from the bindings, so the reconcile
        // that runs at every document is exactly the work a registration needs.
        match wiring {
            Some(bindings) => self.reconcile(bindings, stores, subs),
            None => Vec::new(),
        }
    }

    /// Withdraw an instance's registration.
    ///
    /// Its positions go with it — nothing will consume what they were owed — and
    /// so does every subscription reference it held, which closes any
    /// subscription it was the last holder of. The stores themselves stay: a
    /// channel does not stop retaining because one of its readers left, and a
    /// re-registration reads the same retained history a reconnect would have
    /// kept.
    ///
    /// # Panics
    ///
    /// If the instance is not registered, exactly as deregistering an unknown
    /// port is a caller bug.
    pub fn deregister(
        &mut self,
        instance: &str,
        stores: &mut SurfaceStores,
        subs: &mut Subscriptions,
    ) -> Vec<ClientFrame> {
        let reg = self.entries.remove(instance).unwrap_or_else(|| {
            panic!("surface client: deregistration of unregistered instance {instance:?}")
        });
        stores.detach_matching(|reader| reader.instance == instance);
        reg.subs
            .iter()
            .flat_map(|channel| subs.release(channel))
            .collect()
    }

    /// Mark an instance terminal and strip every position it held, answering
    /// whether this is the transition (so a caller reports a failure once).
    ///
    /// Its channels' stores are untouched: a channel does not stop retaining
    /// because one of its readers died, and the remaining readers are owed
    /// exactly what they were before. Its subscription references stay too — the
    /// instance is still registered, and a terminal instance releasing its
    /// siblings' channel would be a live binding losing its delivery.
    ///
    /// # Panics
    ///
    /// If the instance is not registered.
    pub fn fail(&mut self, instance: &str, stores: &mut SurfaceStores) -> bool {
        let reg = self.entries.get_mut(instance).unwrap_or_else(|| {
            panic!("surface client: failing unregistered instance {instance:?}")
        });
        if reg.failed {
            return false;
        }
        reg.failed = true;
        stores.detach_matching(|reader| reader.instance == instance);
        true
    }

    /// Rebuild every registered instance's positions and subscription references
    /// against the wiring now in force.
    ///
    /// A position is per binding and lives in the store its channel resolves to,
    /// so the binding table defines the set: a binding that vanished loses its
    /// position (nothing can deliver to a port that no longer exists), a port
    /// rebound to a different channel is detached from the old channel's store
    /// and attached fresh to the new one (the old channel's history is stale
    /// under the new binding), and a surviving one keeps its position with its
    /// push depth retuned. A dropped position takes its undelivered drop count
    /// with it: the count describes losses on a binding that no longer exists.
    /// An instance whose bindings all vanish simply stops being activated — it
    /// is not failed and not deregistered, because the operator un-wired it and
    /// that is not the component's fault.
    ///
    /// A position coming into existence is **primed** behind the retained tail,
    /// capped at its `push_depth`, on both channel classes: attach is a delivery
    /// point, so a component that binds after a publish still receives it as
    /// new. Surviving positions are never re-primed, which is what keeps running
    /// this at every attachment idempotent. A depth-0 binding holds no position
    /// — that is the mechanism of "never activates me", not an optimization —
    /// but it does take a subscription reference.
    ///
    /// References are diffed rather than dropped and retaken. Releasing a
    /// surviving reference to zero would discard the subscription's resume
    /// cursor, so the next attachment would subscribe from scratch and replay
    /// the retained window — manufacturing exactly the duplicate delivery the
    /// store's dedup exists to prevent.
    ///
    /// # Panics
    ///
    /// If a bound channel has no store. [`reconcile_stores`] creates one for
    /// every channel the document names and runs first, so reaching this means
    /// the caller ran the passes out of order.
    pub fn reconcile(
        &mut self,
        bindings: &AppliedBindings,
        stores: &mut SurfaceStores,
        subs: &mut Subscriptions,
    ) -> Vec<ClientFrame> {
        // A channel whose fold changed cannot be re-stated while references are
        // held — one open subscription has one statement — so every reference on
        // it is released and retaken below, which closes the subscription and
        // opens it afresh at the new depths. A channel whose `Subscribe` is still
        // unanswered is restated the same way, and the plane enacts it when the
        // result lands. Only a changed document can produce this, and a changed
        // document reloads the page; the restatement is what keeps the reconcile
        // total in the meantime rather than a panic.
        let restate: Vec<String> = subs
            .held_channels()
            .into_iter()
            .filter(|channel| {
                bindings
                    .wire_depths(channel)
                    .is_some_and(|folded| subs.depths(channel) != Some(folded))
            })
            .map(str::to_string)
            .collect();
        let mut release: Vec<String> = Vec::new();
        let mut acquire: Vec<String> = Vec::new();
        let instances: Vec<String> = self.entries.keys().cloned().collect();
        for instance in &instances {
            let failed = self.is_failed(instance);
            let mut positions: Vec<(String, BindingKey, u64)> = Vec::new();
            let mut wanted: Vec<String> = Vec::new();
            for b in bindings.inputs_of(instance) {
                if channel_is_transportable(&b.channel) {
                    wanted.push(b.channel.clone());
                }
                positions.push((
                    b.channel.clone(),
                    BindingKey::new(instance, &b.port),
                    b.push_depth,
                ));
            }
            let channels: Vec<String> = stores.channels().map(str::to_string).collect();
            for channel in channels {
                let store = stores
                    .get_mut(&channel)
                    .expect("surface client: the channel came from this collection");
                let stale: Vec<BindingKey> = store
                    .readers()
                    .filter(|held| held.instance == *instance)
                    .filter(|held| {
                        failed
                            || !positions
                                .iter()
                                .any(|(on, key, _)| on == &channel && key == *held)
                    })
                    .cloned()
                    .collect();
                for key in stale {
                    store.detach(&key);
                }
            }
            if !failed {
                for (channel, key, push_depth) in positions {
                    stores
                        .get_mut(&channel)
                        .unwrap_or_else(|| {
                            panic!(
                                "surface client: bound channel {channel:?} has no store — the \
                                 store reconcile runs first"
                            )
                        })
                        .attach(key, push_depth);
                }
            }
            // Multiset diff against the references this instance currently
            // holds: what is left over is released, what was not matched is
            // acquired, and everything matched is untouched.
            let entry = self
                .entries
                .get_mut(instance)
                .expect("surface client: the instance came from this table");
            let mut held = std::mem::replace(&mut entry.subs, wanted.clone());
            for channel in wanted {
                match held
                    .iter()
                    .position(|c| *c == channel)
                    .filter(|_| !restate.contains(&channel))
                {
                    Some(pos) => {
                        held.remove(pos);
                    }
                    None => acquire.push(channel),
                }
            }
            release.extend(held);
        }
        // Every release first, so a channel being restated is closed before it is
        // opened again and its `Subscribe` carries the new depths.
        let mut frames: Vec<ClientFrame> = release
            .iter()
            .flat_map(|channel| subs.release(channel))
            .collect();
        for channel in acquire {
            let depths = bindings
                .wire_depths(&channel)
                .expect("surface client: a bound transportable channel states wire depths");
            frames.extend(subs.acquire(&channel, depths, ResumePolicy::Resume));
        }
        frames
    }
}
