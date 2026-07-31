//! The confined router: the attacher's own pub/sub, for channels that never
//! cross the wire.
//!
//! A confined (`local:`) channel has no wire state and no peer. The attacher is
//! its whole authority: nothing is subscribed, nothing is published upstream,
//! and no server ever sees an envelope on it. This module is what stands in for
//! the server on those channels — it mints the envelope, appends it to the
//! channel's store, and by that single append wakes every reader bound to the
//! channel. Confined delivery therefore keeps working with the attachment down,
//! which is the point of the class.
//!
//! # What is mechanics and what is policy
//!
//! Everything here is mechanics: identity minting, envelope composition,
//! retention, and the loss one append caused. The rules of a *particular*
//! confined channel — which ones exist, who may write them, whether a body is
//! acceptable as written, and what a message on one means — are the embedder's,
//! injected as a [`PlanePolicy`]. The router calls that policy and enacts its
//! answer; it holds no table of channels and knows no plane by name.
//!
//! # Identity
//!
//! Two grains, exactly as on the wire: the attacher's bare principal, or a
//! sub-identity within it ([`Origin`]). The router derives the sender from its
//! own configuration and the origin its caller resolved — never from anything
//! the publisher said — because it is the only party in a position to attribute
//! confined traffic at all. A publisher names a destination; it never names
//! itself.
//!
//! # Retention lives in the embedder's stores
//!
//! The router is handed the [`ChannelStores`] it routes into rather than owning
//! them: an attacher holds one collection of stores across both channel classes
//! (see [`crate::store`]), and a confined channel's retention is an ordinary
//! entry in it. What the router adds is the authority to *append* — on a wire
//! channel only a `Deliver` may.
//!
//! # Deferral, on this side of the wire
//!
//! The attacher is a confined channel's whole authority, so it is also the
//! authority for what is *scheduled* onto one: a publish carrying a release
//! time still ahead of its mint is parked in the channel's own deferred set
//! ([`LocalRouter::route`]), the embedder's timer brings it back
//! ([`LocalRouter::release_wakeup`], [`LocalRouter::release_due`]), and a
//! publisher reads and edits its own schedule through
//! [`LocalRouter::parked_for`] and [`LocalRouter::apply_op`]. The same three
//! questions on a transportable channel are the peer's, and the attacher only
//! mirrors its answers ([`crate::publish::DeferredViews`]) — which is why the
//! two halves are answered in the same entry shape.

use brenn_attach_proto::DeferredViewEntry;
use brenn_envelope::{
    ChannelScheme, MessageEnvelope, Urgency, is_local_channel, surface_sub_identity,
};
use brenn_queue::{CursorOverflow, QuotaExceeded, ReleaseTime};
use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::store::{ChannelStore, ChannelStores, DeferOp, DeferOpOutcome};
use crate::transport::clock::epoch_ms;

/// The identity and wall-clock time of one message the attacher is about to
/// mint, read by the driver and handed to the sans-I/O layers.
///
/// Separate from the publish itself because this layer reads neither a clock nor
/// an entropy source: both are I/O, and the layer that decides *whether* a
/// publish is confined is the one that must already hold the stamp when it finds
/// out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MessageStamp {
    /// A fresh v4 UUID for the envelope's `message_id`. Uniqueness is the whole
    /// requirement.
    pub message_id: Uuid,
    /// Wall-clock publish time. Never used for ordering — a wall clock steps —
    /// confined ordering is the store's dense per-channel seq.
    pub publish_ts: DateTime<Utc>,
}

/// Who is publishing on a confined channel, at the two identity grains an
/// attachment has.
///
/// Carried as a typed origin rather than a composed sender string so a plane
/// policy can do more with it than concatenate: a plane that stamps the
/// publisher into the body needs the sub-identity's own name, which a sender
/// string has already fused away.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Origin<'a> {
    /// The attacher itself, publishing on nobody's behalf.
    Attacher,
    /// A sub-identity within the attacher — the same opaque string a wire
    /// publish carries as its `attribution`.
    Sub(&'a str),
}

/// What a plane's guard made of one body.
pub enum GuardedBody {
    /// The body to carry onto the channel — rewritten where the plane rewrites
    /// it, the caller's own bytes everywhere else.
    Carry(String),
    /// Refused, with the reason to report. Nothing reaches the channel.
    Refused(String),
}

/// The embedder's rules for its own confined channels.
///
/// Three questions, at three different moments: may this origin write here at
/// all, is this particular body acceptable (and as written?), and what has just
/// become visible. The router asks them in that order and does nothing else with
/// a channel's meaning.
pub trait PlanePolicy {
    /// Whether `origin` may publish on `channel` at all — a structural rule
    /// about the plane, not about the message.
    ///
    /// The router treats a `false` here as a bug in the layer that composed the
    /// publish, not as a runtime refusal: which writers a confined channel
    /// admits is configuration, resolvable before any publish exists, so a
    /// publish that reaches the router from an inadmissible origin means the
    /// embedder's own gate did not run. A rule that depends on the *message*
    /// belongs in [`guard`](Self::guard).
    fn admits(&self, channel: &str, origin: Origin<'_>) -> bool;

    /// This plane's rules for one body about to become a message on it.
    ///
    /// Called on **every** path that puts a body onto a confined channel, so a
    /// plane's rules cannot be walked around by choosing a different one.
    /// Default: carry every body through, which is the right answer for a plane
    /// whose payload is nobody's contract but its readers'.
    fn guard(&self, channel: &str, origin: Origin<'_>, body: String) -> GuardedBody {
        let _ = (channel, origin);
        GuardedBody::Carry(body)
    }

    /// A message has reached `channel` and its readers can see it.
    ///
    /// Called where the message becomes observable rather than where it was
    /// minted, so an embedder tracking a plane's state records only what a reader
    /// could actually have read. Default: nothing — most planes carry no state
    /// the attacher itself reasons about.
    fn observe(&mut self, envelope: &MessageEnvelope) {
        let _ = envelope;
    }
}

/// One publish onto a confined channel, as its caller hands it over.
pub struct RouteRequest<'a> {
    pub channel: &'a str,
    pub origin: Origin<'a>,
    pub body: String,
    pub stamp: MessageStamp,
    /// The publisher's override, else whatever default the embedder resolved for
    /// this destination.
    ///
    /// Inert for waking — confined delivery wakes every reader on every arrival,
    /// since the append *is* the delivery — and carried anyway: the field exists
    /// on the envelope, so it should report what the publisher and the operator
    /// said rather than a value a reader would mistake for one of them.
    pub urgency: Urgency,
    /// When this message is to reach the channel, if not now.
    ///
    /// A time already at or behind the stamp publishes immediately, which is the
    /// contract every host in this system gives a release time in the past. A
    /// time still ahead parks the message in the channel's deferred set until a
    /// release pass takes it.
    pub deliver_after: Option<ReleaseTime>,
}

/// What the router did with one publish.
pub enum RouteOutcome<K> {
    /// Minted, retained, and visible to every reader on the channel.
    ///
    /// The overflow is what the append pushed retention past, per reader — the
    /// loss the embedder's loudness ladder acts on.
    Routed { overflow: Vec<CursorOverflow<K>> },
    /// Minted and held in the channel's deferred set until its release time.
    ///
    /// Nothing is retained, nothing is woken, and no reader can see it yet: a
    /// parked message is not on the channel. The release pass the embedder's
    /// timer drives is what puts it there.
    Parked { release_at: ReleaseTime },
    /// The channel's deferred set was already at its cap, so the schedule was
    /// dropped: nothing is parked and nothing will ever be delivered.
    ///
    /// Normal operation rather than an error — a full set refuses new work
    /// instead of silently cancelling work already scheduled — but never
    /// silent: the embedder counts it against whoever over-scheduled.
    ScheduleDropped { cap: u64 },
    /// A plane guard refused the body. Nothing was minted, nothing was retained,
    /// and no reader saw anything; the reason is the guard's own.
    Refused { reason: String },
    /// The attachment has no identity yet, so there is nobody to attribute the
    /// message to.
    ///
    /// The principal is a fact of the attachment ([`crate::conn::AttachmentFacts`]),
    /// so this is the window before the first one completes. Reported rather than
    /// panicked: an embedder with confined planes of its own may legitimately try
    /// to state one early, and an unattributable envelope is precisely what the
    /// identity model exists to prevent.
    NoIdentity,
}

/// One confined channel's release pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReleasedChannel<K> {
    pub channel: String,
    /// What entered retention, in release order. Each is an ordinary arrival on
    /// the channel from every reader's point of view, and every reader bound to
    /// it has been woken by it.
    pub released: Vec<MessageEnvelope>,
    /// What the batch pushed retention past, per reader, merged across it — the
    /// loss the embedder's loudness ladder acts on.
    pub overflow: Vec<CursorOverflow<K>>,
}

/// What to do with the release timer. Absent — `None` from
/// [`LocalRouter::release_wakeup`] — leaves it exactly as it is.
///
/// A distinct vocabulary from [`crate::publish::TimerChange`] because the
/// currencies are: a release deadline is wall-clock epoch milliseconds, the one
/// currency a schedule can be stated in across a restart, where every other
/// deadline in this crate is the driver's monotonic [`crate::Millis`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReleaseTimer {
    /// Fire when this wall-clock instant arrives.
    Arm(ReleaseTime),
    /// Cancel the armed deadline; nothing is parked anywhere.
    Disarm,
}

/// One control op against a message the caller already parked.
pub struct DeferOpRequest<'a> {
    pub channel: &'a str,
    /// Whose schedule is being changed. The op reaches only what this origin's
    /// own sender parked.
    pub origin: Origin<'a>,
    /// The identity the origin's own view of its schedule carried.
    pub message_id: Uuid,
    pub op: DeferOp,
    /// The wall-clock instant the op is judged at, in the currency a release time
    /// is stated in. The same cutoff [`LocalRouter::parked_for`] answers against,
    /// so an op reaches exactly what the schedule showed.
    pub now: ReleaseTime,
}

/// What the router made of one control op.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeferOpAnswer {
    Applied,
    /// The message is no longer parked — released or already cancelled. The
    /// benign race any publisher can lose between reading its schedule and
    /// acting on it.
    NotParked,
    /// A plane guard refused an edit's replacement body. The parked message is
    /// unchanged, schedule included.
    Refused {
        reason: String,
    },
}

/// The router for an attacher's confined channels.
///
/// Owns the embedder's [`PlanePolicy`], the attacher's principal, and the
/// release deadline it last stated, and nothing else: the stores it routes into
/// are passed to each call, so an embedder can hold its stores and its router as
/// separate fields of one struct.
pub struct LocalRouter<P> {
    policy: P,
    /// The attacher's principal, from the current attachment's `Welcome`. Kept
    /// across a detach — the confined planes and their readers outlive any one
    /// attachment, and the identity that attributes them does not change with
    /// the transport.
    principal: Option<String>,
    /// The release deadline the embedder was last told to hold, so
    /// [`Self::release_wakeup`] speaks only when it moves.
    release_armed: Option<ReleaseTime>,
}

impl<P: PlanePolicy> LocalRouter<P> {
    pub fn new(policy: P) -> Self {
        Self {
            policy,
            principal: None,
            release_armed: None,
        }
    }

    /// Adopt the principal this attachment was welcomed under.
    pub fn set_principal(&mut self, principal: String) {
        assert!(
            !principal.is_empty(),
            "attach client: the confined router was given an empty principal"
        );
        self.principal = Some(principal);
    }

    /// The principal confined traffic is attributed to, or `None` before the
    /// first attachment.
    pub fn principal(&self) -> Option<&str> {
        self.principal.as_deref()
    }

    /// The current plane policy.
    pub fn policy(&self) -> &P {
        &self.policy
    }

    /// The policy, for the embedder to reconfigure as its own configuration
    /// resolves.
    pub fn policy_mut(&mut self) -> &mut P {
        &mut self.policy
    }

    /// The sender a publish from `origin` carries: the bare principal, or the
    /// sub-identity `<principal>#<sub>`.
    ///
    /// Derived from the router's own state, never from anything the publisher
    /// supplied: the router is the sole identity authority on confined channels.
    fn sender(&self, principal: &str, origin: Origin<'_>) -> String {
        match origin {
            Origin::Attacher => principal.to_string(),
            Origin::Sub(sub) => surface_sub_identity(principal, sub),
        }
    }

    /// Mint one confined envelope and either retain it — by which single append
    /// every reader bound to the channel is woken — or park it until the release
    /// time it carries.
    ///
    /// The single point every confined publish passes through, so the plane
    /// policy's guard cannot be skipped by choosing a caller. A parked message
    /// is guarded here, when it is minted, and not again when it releases:
    /// what a plane admits is decided about the body its publisher wrote, at the
    /// moment it wrote it.
    ///
    /// # Panics
    ///
    /// If the channel is not confined (the wire owns retention on every other
    /// class), if the policy does not admit the origin (the embedder's own gate
    /// did not run), or if the channel has no store (a routable confined channel
    /// is one the embedder created a store for).
    pub fn route<K: Eq + std::hash::Hash + Clone>(
        &mut self,
        stores: &mut ChannelStores<K>,
        req: RouteRequest<'_>,
    ) -> RouteOutcome<K> {
        let RouteRequest {
            channel,
            origin,
            body,
            stamp,
            urgency,
            deliver_after,
        } = req;
        assert!(
            is_local_channel(channel),
            "attach client: the confined router was handed {channel}, which is not a confined \
             channel"
        );
        assert!(
            self.policy.admits(channel, origin),
            "attach client: {origin:?} does not publish on {channel}"
        );
        let Some(principal) = self.principal.clone() else {
            return RouteOutcome::NoIdentity;
        };
        let body = match self.policy.guard(channel, origin, body) {
            GuardedBody::Carry(body) => body,
            GuardedBody::Refused(reason) => return RouteOutcome::Refused { reason },
        };
        let envelope = MessageEnvelope {
            message_id: stamp.message_id,
            // The attacher produced this message; its own identity is the only
            // honest source, since no peer origin reaches a confined channel.
            source: principal.clone(),
            channel: channel.to_string(),
            sender: self.sender(&principal, origin),
            publish_ts: stamp.publish_ts,
            body,
            reply_to: None,
            delivery_deadline: None,
            deliver_after: None,
            // A confined publish carries no user-interaction authority: the
            // publisher named a destination, and nothing inside an attacher is in
            // a position to assert a gesture on the operator's behalf.
            impetus: None,
            urgency,
            envelope_type: ChannelScheme::Local,
        };
        if let Some(release_at) = deliver_after.filter(|at| *at > epoch_ms(stamp.publish_ts)) {
            let sender = envelope.sender.clone();
            return match store_for(stores, channel).park(&sender, envelope, release_at) {
                Ok(_) => RouteOutcome::Parked { release_at },
                Err(QuotaExceeded { cap }) => RouteOutcome::ScheduleDropped { cap },
            };
        }
        // Observed before the append only because the append consumes the
        // envelope: this is the moment it reaches its readers either way, and
        // nothing between the two can fail. A parked message is observed at its
        // release instead — the moment a reader could first see it.
        self.policy.observe(&envelope);
        let overflow = store_for(stores, channel).append_minted(envelope);
        RouteOutcome::Routed { overflow }
    }

    /// State the release deadline, when the soonest parked message moved.
    ///
    /// Answered against the whole store collection, so a park, a release pass, a
    /// cancel, an edit and a discarded store all reach the timer through one
    /// call the embedder makes after every input — which is the one arrangement
    /// that cannot be forgotten at a new site the way per-site arming can.
    ///
    /// `None` means the armed deadline is still the right one; the embedder
    /// leaves its timer alone.
    pub fn release_wakeup<K: Eq + std::hash::Hash + Clone>(
        &mut self,
        stores: &ChannelStores<K>,
    ) -> Option<ReleaseTimer> {
        let next = stores.next_release();
        if next == self.release_armed {
            return None;
        }
        self.release_armed = next;
        Some(match next {
            Some(at) => ReleaseTimer::Arm(at),
            None => ReleaseTimer::Disarm,
        })
    }

    /// Take every confined channel's due parked messages into retention.
    ///
    /// Each release is an ordinary arrival on its channel — a fresh tail seq,
    /// the same eviction charges, every bound reader woken by it — exactly as an
    /// immediate confined publish is. A release is also the first moment a
    /// parked message is observable, so it is where the plane policy observes
    /// it.
    ///
    /// A channel with nothing due is absent from the answer rather than present
    /// and empty.
    pub fn release_due<K: Eq + std::hash::Hash + Clone>(
        &mut self,
        stores: &mut ChannelStores<K>,
        now: ReleaseTime,
    ) -> Vec<ReleasedChannel<K>> {
        stores
            .release_due(now)
            .into_iter()
            .map(|(channel, report)| {
                let released: Vec<MessageEnvelope> = report
                    .released
                    .into_iter()
                    .map(|retained| retained.message)
                    .collect();
                for envelope in &released {
                    self.policy.observe(envelope);
                }
                ReleasedChannel {
                    channel,
                    released,
                    overflow: report.overflow,
                }
            })
            .collect()
    }

    /// What `origin` has parked on `channel` and still could act on at `now`,
    /// soonest release first.
    ///
    /// Scoped to the origin's own sender — the same identity the router stamps
    /// on its publishes — so a channel two sub-identities park on shows each of
    /// them only its own schedule. Answered in the entry shape the peer uses for
    /// a transportable channel's set ([`crate::publish::DeferredViews`]), so an
    /// embedder composing one publisher's whole schedule reads both halves
    /// alike.
    ///
    /// Empty before the attachment has an identity: nothing can be parked under
    /// a sender that does not exist yet.
    ///
    /// # Panics
    ///
    /// On a channel that is not confined, or that the embedder hosts no store
    /// for — the [`Self::route`] rules, for the same reasons.
    pub fn parked_for<K: Eq + std::hash::Hash + Clone>(
        &self,
        stores: &ChannelStores<K>,
        channel: &str,
        origin: Origin<'_>,
        now: ReleaseTime,
    ) -> Vec<DeferredViewEntry> {
        assert!(
            is_local_channel(channel),
            "attach client: the confined router was asked for {channel}'s schedule, which is not a \
             confined channel"
        );
        let Some(principal) = self.principal.as_deref() else {
            return Vec::new();
        };
        let sender = self.sender(principal, origin);
        store_ref(stores, channel)
            .deferred_for_sender(&sender, now)
            .map(|parked| DeferredViewEntry {
                message_id: parked.message.message_id,
                // The body, not the envelope: what a publisher gets back is what
                // it handed over, on either channel class.
                body: parked.message.body.clone(),
                deliver_after: parked.release_at,
            })
            .collect()
    }

    /// Cancel or edit one message `origin` parked on a confined channel.
    ///
    /// An edit's replacement body runs the plane's guard before it is written,
    /// exactly as a publish's body does: an edit is a second way to state a body
    /// on a plane, so a guard it skipped would police only half of them. A
    /// refused edit changes nothing, schedule included.
    ///
    /// Nothing is woken: a schedule changing is invisible until it releases.
    ///
    /// Reaches only what is still parked at `req.now` — the cutoff
    /// [`Self::parked_for`] answers against. A message whose release time has
    /// arrived is [`DeferOpAnswer::NotParked`] even before the sweep takes it,
    /// which is both what the schedule showed and what the peer answers for the
    /// same op on a channel that crosses the wire.
    ///
    /// # Panics
    ///
    /// On the [`Self::route`] rules (unconfined channel, inadmissible origin,
    /// unhosted channel), and on an entry parked by a *different* sender: the
    /// identity came from a view this very router scoped to `origin`, so a
    /// cross-sender hit means the embedder resolved it against the wrong one.
    pub fn apply_op<K: Eq + std::hash::Hash + Clone>(
        &mut self,
        stores: &mut ChannelStores<K>,
        req: DeferOpRequest<'_>,
    ) -> DeferOpAnswer {
        let DeferOpRequest {
            channel,
            origin,
            message_id,
            op,
            now,
        } = req;
        assert!(
            is_local_channel(channel),
            "attach client: the confined router was handed an op on {channel}, which is not a \
             confined channel"
        );
        assert!(
            self.policy.admits(channel, origin),
            "attach client: {origin:?} does not publish on {channel}"
        );
        let Some(principal) = self.principal.clone() else {
            // Nothing is parked under an identity that does not exist yet, so
            // the op names nothing — the same answer the release race gets.
            return DeferOpAnswer::NotParked;
        };
        let op = match op {
            DeferOp::Edit {
                body: Some(body),
                deliver_after,
            } => match self.policy.guard(channel, origin, body) {
                GuardedBody::Carry(body) => DeferOp::Edit {
                    body: Some(body),
                    deliver_after,
                },
                GuardedBody::Refused(reason) => return DeferOpAnswer::Refused { reason },
            },
            op => op,
        };
        let sender = self.sender(&principal, origin);
        match store_for(stores, channel).apply_defer_op(&sender, message_id, op, now) {
            DeferOpOutcome::Applied => DeferOpAnswer::Applied,
            DeferOpOutcome::NotParked => DeferOpAnswer::NotParked,
            DeferOpOutcome::WrongSender { owner } => panic!(
                "attach client: {sender} named message {message_id} on {channel}, parked by \
                 {owner} — the schedule this router showed {sender} carried an id it does not own"
            ),
        }
    }
}

/// The store hosting a confined channel the router is about to write.
///
/// A missing one is an embedder bug rather than a runtime condition: a routable
/// confined channel is one the embedder created a store for, and the router has
/// no authority to invent retention for a channel nobody declared.
fn store_for<'a, K: Eq + std::hash::Hash + Clone>(
    stores: &'a mut ChannelStores<K>,
    channel: &str,
) -> &'a mut ChannelStore<K> {
    stores.get_mut(channel).unwrap_or_else(|| unhosted(channel))
}

/// [`store_for`] for the paths that only read one — a schedule is answered from
/// the store without writing it.
fn store_ref<'a, K: Eq + std::hash::Hash + Clone>(
    stores: &'a ChannelStores<K>,
    channel: &str,
) -> &'a ChannelStore<K> {
    stores.get(channel).unwrap_or_else(|| unhosted(channel))
}

/// The one wording of the missing-store panic, so the two borrow flavors of the
/// lookup cannot drift into two diagnoses of one bug.
fn unhosted(channel: &str) -> ! {
    panic!("attach client: no store hosts the confined channel {channel}")
}

#[cfg(test)]
mod tests;
