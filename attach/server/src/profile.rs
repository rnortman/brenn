//! The application-profile seam: what an attachment session asks its route.
//!
//! The session handles frames; the profile answers authority questions about
//! them. The split is what keeps instances, ports, components, and pixels out of
//! the transport: the session asks "may this attacher subscribe this channel",
//! never "which component binds it", and the profile — boot-built from the
//! route's own resolved config — is the only thing that knows the difference.
//!
//! Every answer here is boot-resolved, so a profile is immutable for the life of
//! the process and shared by every session of its attacher.

use brenn_lib::messaging::config::Depth;
use brenn_lib::messaging::{AttachScope, ParticipantId, SubscriberEntry};
use brenn_messaging::MissingChannelPosture;

use super::registry::SessionCaps;

/// What one subscription is, at the grain the session delivers it: the two
/// standard bus subscription knobs, folded across everything behind the channel.
///
/// Two knobs, not one flattening of them: `push_depth` is what wakes the
/// attacher, `retain_depth` is what it can see, and everything the session needs
/// is derived from the pair rather than stored beside it. The fold happens
/// profile-side, because what is behind a channel — one component's binding, six
/// components' bindings, a daemon's own bookkeeping — is exactly what the
/// transport does not know.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SubscriptionFacts {
    /// The subscription's folded push depth — the push window behind it.
    pub push_depth: u64,
    /// The subscription's folded retain depth — the attacher's retained context
    /// ring behind it.
    pub retain_depth: u64,
}

impl SubscriptionFacts {
    /// Whether this subscription has a push window at all.
    ///
    /// A subscription whose fold is 0 is a *context feed*: its rows still reach
    /// the attacher — they are its retained-ring diet, and `retain_depth` bounds
    /// attacher memory, not the wire — but no push window exists behind them, so
    /// there is no overflow for `Deliver.dropped` to account.
    pub(crate) fn push_enabled(self) -> bool {
        self.push_depth >= 1
    }

    /// How far back a subscribe or a drain reads into the channel's retention.
    ///
    /// The max of the two depths, because that is what the subscription is owed:
    /// a subscriber with `push = 8, retain = 2` that missed 8 rows would have
    /// received all 8 had it stayed connected, so the drain may ask for all 8. A
    /// clamp below the wider knob would starve the push window on recovery.
    ///
    /// The clamp is a request, not a promise: if the store retains fewer rows
    /// than the clamp asks for, the shortfall is reported as `dropped` —
    /// bounded loss, exactly as the bus prescribes. Both depths are bounded and
    /// at least one is non-zero — a subscription that neither wakes nor sees is
    /// never constructed — so this is always a bounded, non-zero window.
    pub(crate) fn replay_clamp(self) -> Depth {
        Depth::Bounded(self.push_depth.max(self.retain_depth))
    }
}

/// One channel whose parked-message mirror an attachment is seeded with, and the
/// sub-identity whose parked set it holds.
///
/// A parked set is per-sender, and an attacher's sub-identities are distinct
/// senders, so the mirror is cut at `(attribution, channel)` — not at the
/// attacher grain, which would merge two components' schedules into one view
/// neither of them owns.
/// Ordered by channel first: the seeding sequence walks the attacher's channels,
/// and the sub-identities on any one of them follow together.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct DeferredTarget {
    /// Full scheme-qualified channel address the set is parked on.
    pub channel: String,
    /// The sub-identity whose parked set this mirrors, or `None` for the
    /// attacher's own bare identity.
    pub attribution: Option<String>,
}

/// What a publish outcome the boot invariants exclude means on one channel.
///
/// The transport knows the outcomes; only the route knows what each one costs.
/// A channel boot proved reachable, existent, and policy-covered cannot honestly
/// refuse a publish, so a refusal there says the server disagrees with itself —
/// but the same refusal on the channel an attacher reports its *own* failures to
/// must not take the process down, because that path is attacker-sendable by
/// construction and killing the server over its own diagnostics inverts
/// priorities.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublishPosture {
    /// Boot proved this channel reachable and policy-covered, so an
    /// invariant-excluded refusal is a broken server and the process dies rather
    /// than let a publish silently fail.
    Invariant,
    /// A diagnostics channel. The same refusals are logged loud with the body
    /// preserved and answered `Failed`, and a success emits an audit record
    /// correlating the report to the account and session its body cannot carry.
    Diagnostic,
}

/// The per-connection publish token bucket's shape.
///
/// A struct, not two adjacent `u32` params, because a burst and a refill rate
/// transpose silently: `(120, 5)` and `(5, 120)` are both plausible numbers and
/// only one of them is a rate limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublishRate {
    /// Tokens the bucket starts full with — how many publishes back to back an
    /// attachment may make before the refill governs.
    pub burst: u32,
    /// Tokens refilled per second under sustained load.
    pub per_sec: u32,
}

/// The authority half of an attachment, boot-built by the route that owns the
/// attacher.
///
/// Deliberately small and deliberately total: every method answers from
/// boot-resolved data with no I/O and no clock, so the session can consult it on
/// the frame path without awaiting anything. A `None`/`false` answer is the
/// session's violation signal — the profile never distinguishes "no such
/// channel" from "not yours", because that distinction is an existence oracle.
pub trait AttachProfile: Send + Sync {
    /// The attacher's bare principal — the identity it acts as when it names no
    /// sub-identity.
    fn attacher(&self) -> &ParticipantId;

    /// The facts delivery turns on for `channel`, or `None` if this attacher may
    /// not subscribe it.
    fn subscribable(&self, channel: &str) -> Option<SubscriptionFacts>;

    /// Whether `attribution` (`None` = the attacher itself) may publish onto
    /// `channel`.
    fn publishable(&self, attribution: Option<&str>, channel: &str) -> bool;

    /// The principal a publish under `attribution` is stamped with, or `None` if
    /// this attacher declares no such sub-identity.
    ///
    /// The attacher supplies a value its operator must have written, never an
    /// identity it spells: minting happens here, from the declared set, so an
    /// unknown attribution is refused rather than silently demoted to the bare
    /// identity — which is how a non-conforming client would launder a
    /// sub-identity's traffic onto the attacher's own budget.
    fn admit_attribution(&self, attribution: Option<&str>) -> Option<ParticipantId>;

    /// What a publish refusal the boot invariants exclude means on `channel` —
    /// see [`PublishPosture`]. Answered per channel because one attacher can own
    /// both kinds at once.
    fn publish_posture(&self, channel: &str) -> PublishPosture;

    /// Which route this attacher came through and which of that route's blocks
    /// it is — the pair every publish-side gate keys on. Combined with the
    /// attribution the caller already holds it names the principal, so a
    /// sub-identity's retry loop drains only its own budget, and a surface and a
    /// remote of the same slug never share one.
    fn attach_scope(&self) -> AttachScope<'_>;

    /// What a batch entry naming a channel the server cannot publish onto means
    /// for this attacher — see [`MissingChannelPosture`].
    ///
    /// Attacher-level, unlike [`AttachProfile::publish_posture`]: a flush is
    /// admitted or refused whole, so the question is about where this route's
    /// targets come from, not about which one an entry named. A boot-declared
    /// output set answers `Invariant`; a matcher-granted, runtime-provisioned one
    /// answers `Race`.
    fn missing_channel_posture(&self) -> MissingChannelPosture;

    /// Every `(attribution, channel)` whose parked-message mirror a fresh
    /// attachment is seeded with, deduped and in a stable order so the seeding
    /// sequence is the same on every attach.
    fn deferred_view_targets(&self) -> &[DeferredTarget];

    /// How many `Subscribe`/`Unsubscribe` frames this attacher's connection
    /// bucket admits back to back before the one-token-per-second refill governs.
    ///
    /// Route policy, not transport policy: the burst that a *correct* attacher
    /// produces on connect is the size of its own subscription set, which only
    /// the route knows. A number below it would turn an honest attacher's
    /// first-connect reconcile into a deterministic connect → violation →
    /// fail2ban loop.
    fn subscribe_burst(&self) -> u32;

    /// The per-connection publish bucket this attacher's connections start with.
    ///
    /// Route policy for the same reason [`AttachProfile::subscribe_burst`] is:
    /// what a *correct* attacher publishes back to back is a property of what it
    /// runs, which only the route knows, and the operator tunes it per attacher.
    /// The bucket is per connection and trips ahead of the bus-level per-sender
    /// gate, so it bounds one socket rather than one principal.
    fn publish_rate(&self) -> PublishRate;

    /// Whether this attacher's policy grants the alert plane, advertised in
    /// `Welcome` and enforced on every `Alert` frame.
    ///
    /// A grant, so it belongs to the route that resolved the attacher's policy:
    /// the transport knows what an alert *is* — a generic paging frame that
    /// reaches the operator without touching the bus it may be reporting on — but
    /// not who is allowed to raise one. Deny-by-default: an attacher whose route
    /// answers `false` is told so at attach time, and a frame that arrives anyway
    /// is a violation.
    fn alert_granted(&self) -> bool;

    /// How many concurrent attachments this attacher admits, in total and per
    /// account.
    ///
    /// The caps belong to the profile because what an over-cap attempt *costs*
    /// is the route's judgement: a browser tab beyond the cap is a user with too
    /// many tabs and is answered `503` with no security event, where a daemon
    /// reconnecting into a full slot may deserve a different posture entirely.
    /// The registry only enforces the numbers.
    fn session_caps(&self) -> SessionCaps;

    /// How many subscriptions one attachment of this attacher may hold at once.
    ///
    /// Route policy for the same reason the two burst knobs are, and load-
    /// bearing for exactly one shape of attacher: a profile that answers
    /// [`AttachProfile::subscribable`] from a *matcher* admits every channel
    /// under a prefix, so without a stated cap the per-session subscription
    /// bookkeeping is bounded only by how many channels the operator's prefix
    /// ever matches. A profile whose subscribable set is a finite boot-declared
    /// map answers that set's size, where the cap is unreachable by
    /// construction and costs nothing.
    ///
    /// Over-cap is a violation, not an outcome: a correct attacher knows its own
    /// subscription set and the operator sized the cap for it.
    fn max_active_subscriptions(&self) -> usize;

    /// The directory subscriber entry this attacher needs on `channel` in order
    /// to be delivered to, or `None` when the route's entries are all
    /// boot-declared.
    ///
    /// The delivery fan-out reads the channel's subscriber list, so an attacher
    /// with no entry on a channel receives nothing however legal its
    /// subscription. A surface's entries are folded from its declared bindings
    /// at boot and this answers `None`; an attacher whose channels come into
    /// being at runtime has nothing to fold from and answers the entry its own
    /// ACL ceilings describe — never the client-stated depths, so two sessions
    /// of one attacher mint the same entry and a re-subscribe is idempotent.
    ///
    /// Pure and total like the rest of the trait: the *depths* are the profile's
    /// answer, and clamping them against what the channel actually retains is
    /// the caller's, because only the caller holds the channel.
    fn runtime_entry(&self, channel: &str) -> Option<SubscriberEntry>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_replay_clamp_is_the_max_of_the_two_depths() {
        let clamp = |push, retain| {
            SubscriptionFacts {
                push_depth: push,
                retain_depth: retain,
            }
            .replay_clamp()
        };
        assert_eq!(clamp(8, 0), Depth::Bounded(8));
        assert_eq!(clamp(0, 4), Depth::Bounded(4));
        assert_eq!(clamp(2, 9), Depth::Bounded(9));
        assert_eq!(clamp(9, 2), Depth::Bounded(9));
    }

    /// `push_enabled` is the push depth's own question: a fold-0 subscription is
    /// a context feed however deep its retained ring is.
    #[test]
    fn push_enabled_reads_the_push_depth_alone() {
        let facts = |push, retain| SubscriptionFacts {
            push_depth: push,
            retain_depth: retain,
        };
        assert!(facts(1, 0).push_enabled());
        assert!(!facts(0, 4).push_enabled());
    }
}
