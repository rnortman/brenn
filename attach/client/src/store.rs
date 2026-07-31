//! Channel-keyed retention on the attacher side: what this attachment keeps of
//! each channel, where each local subscriber stands in it, and the window one
//! activation is served from.
//!
//! An attacher holds a channel by exactly the model the backend holds it by: a
//! bounded drop-oldest retained window with one cursor per local subscriber.
//! That model is [`brenn_queue::RingCore`], the same type the backend's own
//! store is built from, so a subscriber's delivery semantics do not change with
//! its hosting. What this module adds is what only an attacher has: the wire's
//! re-presentation of messages a store already holds, and the drops the peer
//! reports from upstream of the attachment.
//!
//! # One store per channel
//!
//! [`ChannelStores`] is keyed by channel address, and one key covers both
//! classes. A transportable channel has exactly one wire subscription per
//! attachment ([`crate::subs`]), so every local subscriber on it reads that one
//! store through its own cursor; a confined channel never had a subscription to
//! scope it to in the first place. Nothing here classifies an address — an
//! embedder that hosts confined channels puts their stores in the same map.
//!
//! # The subscriber key
//!
//! `K` is the embedder's: whatever names *one reader of one channel*. It is
//! deliberately opaque here — this layer knows only that two distinct keys read
//! at their own depths and lag independently, exactly as two backend
//! applications on one channel would. An embedder whose readers are finer than
//! its registrants (several ports of one component on one channel, say) keys by
//! the finer grain, and its coarser operations reach them through
//! [`ChannelStores::detach_matching`] and
//! [`ChannelStores::any_deliverable`].
//!
//! # Depths are the reader's, the store's size is the fold
//!
//! A store has one size, and every reader states its own two depths at each
//! serve. The embedder sizes the store to cover them — the fold over its
//! readers of `max(push_depth, retain_depth)`, floored by whatever the channel
//! itself declares — because a store shallower than a reader's push window
//! would silently cap that reader's delivery.

use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;

use brenn_envelope::MessageEnvelope;
use brenn_queue::RingCore;
use uuid::Uuid;

pub use brenn_queue::{Attached, CursorOverflow};

use crate::subs::SubscriptionDepths;

/// One reader's activation view of a channel, with its position already moved
/// past it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServedWindow {
    /// The window, oldest first: retained context followed by what is new to
    /// this reader.
    pub envelopes: Vec<MessageEnvelope>,
    /// Index into `envelopes` of the first new one; equal to `envelopes.len()`
    /// when the window is pure context.
    pub new_from: usize,
    /// Everything this reader lost since its previous serve — messages the
    /// window could not present, plus the peer's own upstream count. The figure
    /// to report to the reader.
    pub dropped: u64,
    /// The part of `dropped` nothing has accounted for yet: the still-retained
    /// span this serve stepped over, plus the peer's upstream count. A loss an
    /// eviction already reported is excluded, so an embedder's loudness ladder
    /// acting on this figure never enacts one span twice.
    pub counted: u64,
}

/// One channel's attacher-side state: what it retains, where each reader stands
/// in it, and what the peer said was lost before it ever arrived.
pub struct ChannelStore<K> {
    core: RingCore<MessageEnvelope, Uuid, K>,
    /// Per-reader accumulator of drops the peer reported on this channel, not
    /// yet handed to a serve. Loss upstream of the attachment is only reportable
    /// by the peer, so it cannot be derived from cursor arithmetic the way local
    /// loss is; it is added to what the reader's advance reports and drained at
    /// the same moment. Stays empty on a channel with no wire above it.
    server_drops: HashMap<K, u64>,
}

impl<K: Eq + Hash + Clone> ChannelStore<K> {
    /// A store retaining `depth` messages, stamped with `epoch`.
    ///
    /// The epoch identifies this attacher's incarnation: it changes only when
    /// the attacher restarts, which is also the only event that discards a
    /// store.
    pub fn new(epoch: Uuid, depth: u64) -> Self {
        Self {
            core: RingCore::new(epoch, depth),
            server_drops: HashMap::new(),
        }
    }

    /// Retune the retained window, trimming in place if it shrank. What the
    /// store already holds is still honest history.
    ///
    /// A shrink retires messages a lagging position was still owed, so it
    /// reports them exactly as an arrival's eviction does — an embedder lowering
    /// depths is as accountable a cause of loss as a burst, and a loudness
    /// ladder must see it either way.
    pub fn retune(&mut self, depth: u64) -> Vec<CursorOverflow<K>> {
        self.core.set_depth(depth)
    }

    /// The resolved retained depth.
    pub fn depth(&self) -> u64 {
        self.core.ring().depth()
    }

    /// The epoch this store's positions are stamped with.
    pub fn epoch(&self) -> Uuid {
        self.core.ring().epoch()
    }

    /// Take one delivered envelope into retention, reporting every reader the
    /// entry pushed retention past.
    ///
    /// **Idempotent by `message_id`:** an envelope the retained window already
    /// holds is not taken again, and nothing is charged for it. The wire
    /// legitimately re-presents envelopes a store already holds; without the
    /// check a window's context could carry the same message twice.
    ///
    /// The membership scan is over at most `depth` entries, a configured bound
    /// on attacher memory. A message re-presented after eviction is taken as
    /// fresh, which is all a scan of the retained window can say.
    pub fn insert(&mut self, envelope: MessageEnvelope) -> Vec<CursorOverflow<K>> {
        if self.holds(&envelope) {
            return Vec::new();
        }
        self.core.append(envelope).overflow
    }

    /// Take a locally minted envelope into retention.
    ///
    /// Asserts it is not already held: the identity is a fresh v4 uuid the
    /// embedder just minted, so a collision is an embedder bug, and taking the
    /// dedup path would silently drop a publish.
    pub fn append_minted(&mut self, envelope: MessageEnvelope) -> Vec<CursorOverflow<K>> {
        assert!(
            !self.holds(&envelope),
            "attach client: freshly minted envelope {} is already retained on {}",
            envelope.message_id,
            envelope.channel
        );
        self.core.append(envelope).overflow
    }

    fn holds(&self, envelope: &MessageEnvelope) -> bool {
        self.core
            .ring()
            .iter()
            .any(|e| e.message.message_id == envelope.message_id)
    }

    /// Give a reader a position on this channel, or retune the one it holds.
    ///
    /// Primed from the retained tail, capped at `push_depth`: attach is a
    /// delivery point, so the store's newest `min(push_depth, tail)` are new to
    /// a reader coming into existence, it wakes on them, and the priming charges
    /// it nothing. A reader that already holds a position is not re-primed —
    /// which is what lets an embedder run its whole reconcile at every
    /// reconnect.
    ///
    /// A sampled (`push_depth = 0`) reader holds no position: it is never
    /// delivered to, so a position kept for it would be one every eviction
    /// charges and no window can serve.
    pub fn attach(&mut self, reader: K, push_depth: u64) -> Attached {
        self.core.attach(reader, push_depth)
    }

    /// Drop one reader's position and its undelivered peer-reported drops. The
    /// messages stay retained for whoever else reads them.
    pub fn detach(&mut self, reader: &K) {
        self.core.detach(reader);
        self.server_drops.remove(reader);
    }

    /// Every reader holding a position here.
    pub fn readers(&self) -> impl Iterator<Item = &K> {
        self.core.cursors().keys()
    }

    /// Whether this reader is owed something the store still holds — the wake
    /// question. `false` for a sampled or unattached reader, which is owed
    /// nothing by definition.
    pub fn has_deliverable(&self, reader: &K) -> bool {
        self.core.has_deliverable(reader)
    }

    /// Whether any reader the predicate names is owed something — the wake
    /// question at a coarser grain than the key.
    ///
    /// Asked off the positions rather than off the embedder's own table because
    /// a driver asks it on every turn: reading the positions the store already
    /// holds builds no lookup key. The positions are the same set the embedder's
    /// table names — one comes into existence with the other.
    pub fn any_deliverable(&self, mut names: impl FnMut(&K) -> bool) -> bool {
        let ring = self.core.ring();
        self.core
            .cursors()
            .iter()
            .any(|(reader, cursor)| names(reader) && cursor.has_deliverable(ring))
    }

    /// Cut this reader's activation window and move its position past it.
    ///
    /// The window is the most recent `max(push_depth, retain_depth)` retained
    /// messages with the boundary where this reader's unseen ones begin, and
    /// `depths` is the reader's own pair — not the wire subscription's, which is
    /// the fold across every reader of the channel.
    ///
    /// Assembly and advance are one operation because the cursor **advances
    /// before the reader runs**: what an activation saw is behind the reader's
    /// position whatever the activation then does with it, and retention is its
    /// only recovery. A window that served nothing, and a sampled reader (which
    /// holds no position), advance nothing. The peer-reported drops are drained
    /// here too, alongside the advance whose figures they join, so one serve
    /// reports each loss exactly once. The two are disjoint by construction:
    /// upstream loss never reached this store, so no cursor subtraction can
    /// count it, and local loss is entirely cursor arithmetic.
    ///
    /// `None` for a push-enabled reader holding no position: there is no window
    /// to cut, and inventing one would serve messages nothing will ever advance
    /// over. A sampled reader is always served, as pure context.
    pub fn serve(&mut self, reader: &K, depths: SubscriptionDepths) -> Option<ServedWindow> {
        let window = self
            .core
            .window(reader, depths.push_depth, depths.retain_depth)?;
        let advance = window
            .advance_span()
            .and_then(|(through, seen_floor)| self.core.advance(reader, through, seen_floor));
        let from_peer = self.server_drops.remove(reader).unwrap_or(0);
        let new_from = window.new_from;
        let envelopes: Vec<MessageEnvelope> = window
            .entries
            .into_iter()
            .map(|entry| entry.message)
            .collect();
        Some(ServedWindow {
            envelopes,
            new_from,
            dropped: advance.map_or(0, |a| a.dropped) + from_peer,
            counted: advance.map_or(0, |a| a.noise_charge) + from_peer,
        })
    }

    /// Count `n` messages the peer reported dropped on this channel before they
    /// ever reached the attachment.
    ///
    /// Every reader holding a position takes the **full** count: each of them
    /// missed exactly those messages, since the channel's window rolled past the
    /// one position the attachment holds upstream. A sampled reader holds no
    /// position and takes nothing — it is never delivered to, so it is never
    /// reported against.
    pub fn count_server_drops(&mut self, n: u64) {
        if n == 0 {
            return;
        }
        let held: Vec<K> = self.core.cursors().keys().cloned().collect();
        for reader in held {
            let entry = self.server_drops.entry(reader).or_insert(0);
            *entry = entry.saturating_add(n);
        }
    }

    /// Every retained message with the seq it holds, oldest first — what the
    /// store actually kept, which is the one question a window cannot answer
    /// without being told a depth to trust.
    pub fn retained(&self) -> impl Iterator<Item = (&MessageEnvelope, u64)> {
        self.core.ring().iter().map(|e| (&e.message, e.seq))
    }
}

/// Every channel this attachment retains, keyed by channel address.
///
/// The embedder drives it: [`ensure`](ChannelStores::ensure) and
/// [`retain`](ChannelStores::retain) as its configuration resolves what channels
/// exist and how deep they are, then the per-store operations for delivery,
/// readers and windows.
pub struct ChannelStores<K> {
    /// Stamped into every store this collection creates, so an attacher's whole
    /// retention carries one incarnation identity.
    epoch: Uuid,
    /// Ordered by address, so an embedder's telemetry over the stores does not
    /// depend on hash order.
    stores: BTreeMap<String, ChannelStore<K>>,
}

impl<K: Eq + Hash + Clone> ChannelStores<K> {
    /// An empty collection whose stores will carry `epoch`.
    pub fn new(epoch: Uuid) -> Self {
        Self {
            epoch,
            stores: BTreeMap::new(),
        }
    }

    /// Create `channel`'s store at `depth`, or retune the one that exists.
    ///
    /// A surviving store is retuned in place rather than replaced: its contents,
    /// positions and seq counter are what a window's context is read from, and
    /// discarding them at a reconcile would manufacture a loss nothing caused.
    /// The retune takes effect at each reader's next window, and reports what a
    /// shrink retired.
    pub fn ensure(&mut self, channel: &str, depth: u64) -> Vec<CursorOverflow<K>> {
        let epoch = self.epoch;
        self.stores
            .entry(channel.to_string())
            .or_insert_with(|| ChannelStore::new(epoch, depth))
            .retune(depth)
    }

    /// Drop every store the predicate does not keep, handing them back in
    /// address order.
    ///
    /// They are returned rather than discarded because a store going away may
    /// owe an account of what went with it — parked schedules, a reader's
    /// unread tail — and what that account is worth is the embedder's question.
    pub fn retain(&mut self, keep: impl Fn(&str) -> bool) -> Vec<(String, ChannelStore<K>)> {
        let dropped: Vec<String> = self
            .stores
            .keys()
            .filter(|channel| !keep(channel))
            .cloned()
            .collect();
        dropped
            .into_iter()
            .map(|channel| {
                let store = self
                    .stores
                    .remove(&channel)
                    .expect("attach client: the key came from this map");
                (channel, store)
            })
            .collect()
    }

    /// This channel's store, or `None` when the attachment retains no such
    /// channel.
    pub fn get(&self, channel: &str) -> Option<&ChannelStore<K>> {
        self.stores.get(channel)
    }

    /// This channel's store for mutation — delivery intake, reader positions,
    /// and windows all go through it.
    pub fn get_mut(&mut self, channel: &str) -> Option<&mut ChannelStore<K>> {
        self.stores.get_mut(channel)
    }

    /// Every retained channel, in address order.
    pub fn channels(&self) -> impl Iterator<Item = &str> {
        self.stores.keys().map(String::as_str)
    }

    /// Drop the positions of every reader the predicate names, across every
    /// channel — a registrant's failure or deregistration, where nothing will
    /// ever consume what it was owed.
    ///
    /// The stores themselves are untouched: a channel does not stop retaining
    /// because one of its readers went away.
    pub fn detach_matching(&mut self, names: impl Fn(&K) -> bool) {
        for store in self.stores.values_mut() {
            let held: Vec<K> = store
                .readers()
                .filter(|reader| names(reader))
                .cloned()
                .collect();
            for reader in held {
                store.detach(&reader);
            }
        }
    }

    /// Whether any reader the predicate names is owed something on any channel —
    /// the wake question a driver asks per turn.
    pub fn any_deliverable(&self, names: impl Fn(&K) -> bool + Copy) -> bool {
        self.stores
            .values()
            .any(|store| store.any_deliverable(names))
    }
}

#[cfg(test)]
mod tests;
