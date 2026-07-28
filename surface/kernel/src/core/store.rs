//! One channel's page-side retention, and the key that names it.
//!
//! The page holds channels by exactly the model the backend holds them by: a
//! bounded drop-oldest retained window, one cursor per binding reading it, and
//! (later) a set of parked messages. That model is
//! [`brenn_queue::RingCore`], shared with the backend's own store, so a
//! component's delivery semantics do not change with its hosting. What lives
//! here is only what the page adds: the wire's re-presentation of messages a
//! store already holds, the drops the server reports from upstream of the page,
//! and the key that says which store a channel's retention is kept in.
//!
//! # One structure, both classes
//!
//! Retention is a channel property, so a confined channel and a transportable
//! one retain by one rule and one implementation. Only the *key* differs, and
//! only because retention authority does:
//!
//! - **Confined** (`local:`): the page is the authority. One store per channel,
//!   shared by every bound instance — there is no subscription to scope it to.
//! - **Wire** (transportable): the backend is the authority and feeds the page
//!   per subscription. `(instance, channel)` is the subscription's identity end
//!   to end — its own resume cursor, its own replay stream, its own drop
//!   attribution — and the cursors the server keeps are opaque to the page, so
//!   the page has no channel-global order two subscriptions' replays could be
//!   merged into. The per-subscription store is the channel model's private
//!   retained view, not a second channel.

use std::collections::HashMap;

use brenn_envelope::MessageEnvelope;
use brenn_queue::{
    Advance, Attached, CursorOverflow, Deferred, DeferredId, OwnedDeferred, Priming, QuotaExceeded,
    ReleaseReport, ReleaseTime, RingCore, Window,
};
use brenn_surface_proto::channel_capabilities;
use uuid::Uuid;

use super::SubKey;

/// One input binding as a subscriber on its channel's store: the instance whose
/// position it is, and the port it reads through.
///
/// The binding, not the instance, is the subscriber: two ports of one instance
/// on one channel read at their own depths and lag independently, exactly as two
/// backend applications on one channel would.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct BindingKey {
    pub instance: String,
    pub port: String,
}

impl BindingKey {
    pub(crate) fn new(instance: &str, port: &str) -> Self {
        Self {
            instance: instance.to_string(),
            port: port.to_string(),
        }
    }
}

/// Which store holds a channel's retention for the page.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum StoreKey {
    /// A confined channel: the page is the retention authority, so the channel
    /// address alone names the store and every bound instance shares it.
    Confined(String),
    /// A transportable channel: the backend is the authority and the wire feeds
    /// the page one subscription at a time.
    ///
    /// What such a store holds is the subscription's, and lives exactly as long:
    /// it is discarded with the resume token when the subscription's last
    /// reference goes, so a re-acquired subscription mirrors its own replay from
    /// empty rather than shadowing it with the previous one's.
    Wire(SubKey),
}

/// Whether this channel's messages cross the page/backend boundary.
///
/// The kernel's single derivation of the only channel-class distinction its
/// delivery paths are allowed to make. Every channel the kernel routes came
/// from boot-validated bindings or the reserved table, so an address that
/// classifies as nothing here is a kernel bug rather than a tolerated input.
pub(crate) fn channel_is_transportable(channel: &str) -> bool {
    channel_capabilities(channel)
        .unwrap_or_else(|| {
            panic!("surface client: unclassifiable channel address reached the kernel: {channel:?}")
        })
        .transportable
}

/// The store `instance`'s binding on `channel` reads: the transportability
/// branch, and the only one there is.
pub(crate) fn store_key(channel: &str, instance: &str) -> StoreKey {
    if channel_is_transportable(channel) {
        StoreKey::Wire(SubKey::for_instance(instance, channel))
    } else {
        StoreKey::Confined(channel.to_string())
    }
}

/// What to do to one message a component has parked.
///
/// One type shared between acceptance (buffer) and application (store): the
/// identity the op names is resolved once, at buffer time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DeferOp {
    Cancel,
    Edit {
        body: Option<String>,
        deliver_after: Option<ReleaseTime>,
    },
}

/// What became of a control op.
///
/// Three outcomes because the two failures mean opposite things about the caller.
/// [`Self::NotParked`] is the benign race every conforming component can lose: the
/// message released between the snapshot it read and the flush. A
/// [`Self::WrongSender`] is not a race at all — the identity came from a
/// sender-scoped view, so it can only mean the page built one wrong.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DeferOpOutcome {
    Applied,
    NotParked,
    WrongSender { owner: String },
}

/// One channel's page-side state: what it retains, where each binding is in it,
/// and what the server said was lost before it ever got here.
pub(crate) struct SurfaceChannelStore {
    core: RingCore<MessageEnvelope, Uuid, BindingKey>,
    /// Per-binding accumulator of drops the server reported on this
    /// subscription, not yet handed to an activation. Loss upstream of the page
    /// is only reportable by the server, so it cannot be derived from cursor
    /// arithmetic the way page-side loss is; it is added to what the binding's
    /// advance reports and drained at the same moment. Stays empty on a
    /// confined store, which has no upstream.
    server_drops: HashMap<BindingKey, u64>,
}

impl SurfaceChannelStore {
    /// A store retaining `depth` messages, stamped with the page's epoch.
    ///
    /// A store has one size, and `depth` is it: the fold of the channel's
    /// declared depth with every binding's `max(push_depth, retain_depth)`. What
    /// the store holds under that depth is the channel's history on the page,
    /// all of it — there is no second, smaller number it retains messages while
    /// disavowing.
    ///
    /// The epoch is the page load: it changes only when the page reloads, which
    /// is also the only event that discards a store.
    pub(crate) fn new(epoch: Uuid, depth: u64) -> Self {
        Self {
            core: RingCore::new(epoch, depth),
            server_drops: HashMap::new(),
        }
    }

    /// Retune the retained window, trimming in place if it shrank. What the
    /// store already holds is still honest history.
    ///
    /// A shrink retires messages a lagging position was still owed, so it reports
    /// them exactly as an arrival's eviction does — an operator lowering depths is
    /// as accountable a cause of loss as a burst, and the ladder must see it
    /// either way.
    pub(crate) fn retune(&mut self, depth: u64) -> Vec<CursorOverflow<BindingKey>> {
        self.core.set_depth(depth)
    }

    /// Take one delivered envelope into retention, reporting every binding the
    /// entry pushed retention past.
    ///
    /// **Idempotent by `message_id`:** an envelope the retained window already
    /// holds is not taken again, and nothing is charged for it. This is
    /// load-bearing rather than hygiene, and it is the page's own concern rather
    /// than the shared model's: several wire paths legitimately re-present
    /// envelopes a store already holds (a durable fresh-attach replay, a fresh
    /// replay after a gap past retention, an ephemeral epoch-change replay),
    /// while the backend structurally cannot — it mints a fresh identity at
    /// append. Without the check one window's context could carry the same
    /// message twice, a shape the backend's context read can never produce, so
    /// the two hostings would disagree about what "seen" means.
    ///
    /// The membership scan is over at most `depth` entries, a config bound on
    /// page memory. A message re-presented after eviction is taken as fresh,
    /// which is all a scan of the retained window can say.
    pub(crate) fn insert(&mut self, envelope: MessageEnvelope) -> Vec<CursorOverflow<BindingKey>> {
        if self.holds(&envelope) {
            return Vec::new();
        }
        self.core.append(envelope).overflow
    }

    /// Take a page-minted envelope into retention.
    ///
    /// Asserts it is not already held: the identity is a fresh v4 uuid the
    /// router just minted, so a collision is a kernel bug, and taking the
    /// dedup path would silently drop a publish.
    pub(crate) fn append_minted(
        &mut self,
        envelope: MessageEnvelope,
    ) -> Vec<CursorOverflow<BindingKey>> {
        assert!(
            !self.holds(&envelope),
            "surface client: freshly minted envelope {} is already retained on {}",
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

    /// Give a binding a position on this channel, or retune the one it holds.
    ///
    /// Primed from the retained tail, capped at `push_depth`: attach is a
    /// delivery point on both classes, so the store's newest
    /// `min(push_depth, tail)` are new to a binding coming into existence, it
    /// wakes on them, and the priming charges it nothing.
    ///
    /// On a confined channel that is the whole of the late-attach handoff. On a
    /// wire channel the dominant path is a fresh subscription, whose mirror is
    /// empty at attach — the server's replay then arrives as ordinary unseen
    /// messages — so this governs an attach to a mirror that survived: a
    /// subscription the instance keeps another reference to, where no fresh
    /// `Subscribe` will replay and the mirror's tail is the only catch-up there
    /// is.
    ///
    /// A sampled (`push_depth = 0`) binding holds no position: it is never
    /// delivered to, so a position kept for it would be one every eviction
    /// charges and no window can serve.
    pub(crate) fn attach(&mut self, binding: BindingKey, push_depth: u64) -> Attached {
        self.core.attach(binding, push_depth, Priming::Retained)
    }

    /// Drop one binding's position and its undelivered server-drop count. The
    /// messages stay retained for whoever else reads them.
    pub(crate) fn detach(&mut self, binding: &BindingKey) {
        self.core.detach(binding);
        self.server_drops.remove(binding);
    }

    /// Drop every position `instance` holds here — instance failure and
    /// deregistration, where nothing will ever consume what it was owed.
    pub(crate) fn detach_instance(&mut self, instance: &str) {
        let held: Vec<BindingKey> = self
            .core
            .cursors()
            .keys()
            .filter(|b| b.instance == instance)
            .cloned()
            .collect();
        for binding in held {
            self.detach(&binding);
        }
    }

    /// Every binding holding a position here.
    pub(crate) fn bindings(&self) -> impl Iterator<Item = &BindingKey> {
        self.core.cursors().keys()
    }

    /// Whether any position `instance` holds here is owed something the store
    /// still holds — the wake question. `false` for an instance whose bindings
    /// here are all sampled or unattached, which are owed nothing by definition.
    ///
    /// Asked at instance grain, and off the positions rather than off the binding
    /// table, because the driver's select loop asks it on every turn: reading the
    /// positions the store already holds classifies no channel address and builds
    /// no lookup key, where asking per binding costs both. The positions are the
    /// same set the binding table names — one comes into existence with the other
    /// — so the two readings agree.
    pub(crate) fn has_deliverable_for_instance(&self, instance: &str) -> bool {
        let ring = self.core.ring();
        self.core
            .cursors()
            .iter()
            .any(|(binding, cursor)| binding.instance == instance && cursor.has_deliverable(ring))
    }

    /// This binding's activation view: the most recent
    /// `max(push_depth, retain_depth)` retained messages with the boundary where
    /// its unseen ones begin.
    pub(crate) fn window(
        &mut self,
        binding: &BindingKey,
        push_depth: u64,
        retain_depth: u64,
    ) -> Option<Window<MessageEnvelope>> {
        self.core.window(binding, push_depth, retain_depth)
    }

    /// Move this binding past the window it was just served and report what it
    /// never saw.
    pub(crate) fn advance(
        &mut self,
        binding: &BindingKey,
        through: u64,
        seen_floor: u64,
    ) -> Option<Advance> {
        self.core.advance(binding, through, seen_floor)
    }

    /// Hold `envelope` out of retention until `release_at`, under the channel's
    /// deferred cap.
    ///
    /// A parked message is in no position's owed set and no window, so nothing is
    /// woken and nothing is charged; it enters retention at [`Self::release_due`]
    /// as an ordinary arrival. `sender` is the authorization key — the identity
    /// the envelope itself carries, so a later view or edit scoped to a sender
    /// reaches exactly what that sender parked.
    ///
    /// The cap is the store's depth (a channel holds at most as much parked
    /// future as retained past), which for a confined channel is the folded
    /// delivery capacity: a page whose bindings ask for deep push windows may
    /// park correspondingly more.
    pub(crate) fn park(
        &mut self,
        sender: &str,
        envelope: MessageEnvelope,
        release_at: ReleaseTime,
    ) -> Result<DeferredId, QuotaExceeded> {
        self.core.park(sender, envelope, release_at)
    }

    /// When this channel's next parked message comes due, or `None` when nothing
    /// is parked — the deadline a release timer arms from.
    pub(crate) fn next_release(&self) -> Option<ReleaseTime> {
        self.core.next_release()
    }

    /// Take every message due at or before `now` into retention, in release
    /// order, reporting what the batch retired.
    ///
    /// Each released message takes a fresh tail seq and charges exactly as a
    /// fresh arrival does, because to every position reading this channel that is
    /// what it is.
    pub(crate) fn release_due(
        &mut self,
        now: ReleaseTime,
    ) -> ReleaseReport<MessageEnvelope, BindingKey> {
        self.core.release_due(now)
    }

    /// One sender's messages still parked at `now`, soonest release first — the
    /// deferred view an activation carries for an output port on this channel.
    ///
    /// Still parked is exactly `release_at > now`: an entry whose time has come
    /// is out of the view before the release pass takes it, since there is
    /// nothing left to cancel or edit.
    pub(crate) fn deferred_for_sender<'a>(
        &'a self,
        sender: &'a str,
        now: ReleaseTime,
    ) -> impl Iterator<Item = &'a Deferred<MessageEnvelope>> {
        self.core.deferred_for_sender(sender, now)
    }

    /// The identity that parked each message still held here, across senders.
    ///
    /// The one read of the deferred set that is not sender-scoped, because its
    /// caller is not a component: a store about to be discarded owes an account
    /// of every schedule going with it, whoever set it.
    pub(crate) fn parked_senders(&self) -> impl Iterator<Item = &str> {
        self.core.deferred_at(0).map(|e| e.sender.as_str())
    }

    /// What became of a control op against one parked message.
    pub(crate) fn apply_defer_op(
        &mut self,
        sender: &str,
        message_id: Uuid,
        op: DeferOp,
    ) -> DeferOpOutcome {
        // Cutoff 0: parked means "still in the set", and the release pass is the
        // only thing that takes an entry out of it. A component names a message by
        // an index into the view it was handed, which already excluded everything
        // due at that instant, so a cutoff here could only narrow what it can reach
        // — never widen it.
        let (id, replacement) =
            match self
                .core
                .owned_deferred(sender, |m| m.message_id == message_id, 0)
            {
                OwnedDeferred::Owned(id, entry) => (
                    id,
                    // The body edit is a field rewrite the shared core cannot do for
                    // itself: it holds an opaque payload, and which part of one is "the
                    // message the component wrote" is the page's knowledge. The
                    // envelope's own `deliver_after` stays `None` — a schedule is the
                    // channel's, held in its deferred set until it releases.
                    match &op {
                        DeferOp::Cancel => None,
                        DeferOp::Edit { body, .. } => body.clone().map(|body| MessageEnvelope {
                            body,
                            ..entry.message.clone()
                        }),
                    },
                ),
                OwnedDeferred::NotFound => return DeferOpOutcome::NotParked,
                OwnedDeferred::WrongSender { owner } => {
                    return DeferOpOutcome::WrongSender {
                        owner: owner.to_string(),
                    };
                }
            };
        // Both delegations are infallible: the resolution above found the entry
        // under this very `&mut self`, and nothing between the two can release it.
        match op {
            DeferOp::Cancel => {
                self.core
                    .cancel_deferred(id)
                    .expect("surface client: the entry just resolved is still parked");
            }
            DeferOp::Edit { deliver_after, .. } => self
                .core
                .edit_deferred(id, replacement, deliver_after)
                .expect("surface client: the entry just resolved is still parked"),
        }
        DeferOpOutcome::Applied
    }

    /// Count `n` messages the server reported dropped on this subscription
    /// before they ever reached the page.
    ///
    /// Every binding holding a position takes the **full** count: each of them
    /// missed those messages. A sampled binding holds no position and takes
    /// nothing — it is never delivered to, so it is never reported against.
    pub(crate) fn count_server_drops(&mut self, n: u64) {
        if n == 0 {
            return;
        }
        let held: Vec<BindingKey> = self.core.cursors().keys().cloned().collect();
        for binding in held {
            let entry = self.server_drops.entry(binding).or_insert(0);
            *entry = entry.saturating_add(n);
        }
    }

    /// Take this binding's undelivered server-reported drops, zeroing them.
    ///
    /// Drained at assembly, alongside the advance whose figures it is added to.
    /// The two are disjoint by construction: server-side loss never reached this
    /// store, so no page-side subtraction can count it, and page-side loss is
    /// entirely cursor arithmetic.
    pub(crate) fn take_server_drops(&mut self, binding: &BindingKey) -> u64 {
        self.server_drops.remove(binding).unwrap_or(0)
    }

    /// The page epoch this store's positions are stamped with.
    #[cfg(all(test, not(target_arch = "wasm32")))]
    pub(crate) fn epoch(&self) -> Uuid {
        self.core.ring().epoch()
    }

    /// The resolved retained depth. Test-only: nothing in the kernel asks a
    /// store how deep it is — the store enforces its own bound and every reader
    /// states the depth it wants. The fold that computes it is worth asserting
    /// directly, though.
    #[cfg(all(test, not(target_arch = "wasm32")))]
    pub(crate) fn depth(&self) -> u64 {
        self.core.ring().depth()
    }

    /// Every message parked here across senders with its release time, soonest
    /// first. Test-only: the kernel reads a schedule only through a sender-scoped
    /// view, but a test playing several senders needs to see the whole set.
    #[cfg(all(test, not(target_arch = "wasm32")))]
    pub(crate) fn parked(&self) -> impl Iterator<Item = (&MessageEnvelope, ReleaseTime)> {
        self.core.deferred_at(0).map(|e| (&e.message, e.release_at))
    }

    /// Every retained message with the seq it holds, oldest first — what the
    /// store actually kept, which is the one question a window cannot answer
    /// without being told a depth to trust.
    #[cfg(all(test, not(target_arch = "wasm32")))]
    pub(crate) fn retained(&self) -> impl Iterator<Item = (&MessageEnvelope, u64)> {
        self.core.ring().iter().map(|e| (&e.message, e.seq))
    }
}
