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

use brenn_lib::messaging::ParticipantId;
use brenn_lib::messaging::config::Depth;

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
    /// The sub-identity whose parked set this mirrors.
    pub attribution: String,
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

    /// The scope half of this attacher's send-budget key. The other half is the
    /// attribution the caller already holds, so a budget bucket is
    /// `(scope, attribution)` and a sub-identity's retry loop drains only its
    /// own.
    fn send_budget_scope(&self) -> &str;

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

    /// How many concurrent attachments this attacher admits, in total and per
    /// account.
    ///
    /// The caps belong to the profile because what an over-cap attempt *costs*
    /// is the route's judgement: a browser tab beyond the cap is a user with too
    /// many tabs and is answered `503` with no security event, where a daemon
    /// reconnecting into a full slot may deserve a different posture entirely.
    /// The registry only enforces the numbers.
    fn session_caps(&self) -> SessionCaps;
}
