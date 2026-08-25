//! Brenn attachment protocol wire types.
//!
//! The frames an **attacher** — anything that extends Brenn's message bus over a
//! websocket — speaks: a symmetric version handshake, subscribe/unsubscribe,
//! publish and one-activation atomic batches, delivery with cursors and gap
//! accounting, deferred-message views, heartbeat, and alert. Everything is
//! addressed by **channel**. The vocabulary names no application concept —
//! no component, port, instance, DOM, or pixel — so every attacher speaks the
//! same protocol and its application layer rides the same bus as ordinary
//! messages on ordinary channels.
//!
//! Kept free of I/O, tokio, and host-only dependencies so a wasm attacher links
//! it cleanly: the dependencies are `serde`, `uuid`, and `brenn-envelope`.
//!
//! Serde representation: the two frame enums are `#[serde(tag = "type")]`; inner
//! enums (outcomes, [`GapReason`], [`DeferredOpKind`]) are
//! `#[serde(tag = "kind")]`. Variant names are PascalCase. JSON text frames.
//!
//! **Schemas are strict.** Every frame and payload type carries
//! `deny_unknown_fields`: an unknown field inside a known variant is a protocol
//! violation, as is an unknown `type`/`kind` tag. [`negotiate`] settles which
//! schema is in force before any other frame is sent, so both ends know exactly
//! what they are parsing and leniency could only mask a bug. Any schema change
//! bumps the transport version. The one boundary: a variant with no fields has
//! no field list for serde's internally-tagged representation to check trailing
//! keys against, so junk beside such a tag parses — there is nothing there an
//! extra key could be a misspelling of.
//!
//! Bus semantics the frames carry, none of them re-invented here: cursors are
//! server-minted, opaque and client-held; nothing consumes and nothing acks;
//! there is no backpressure; overflow drops oldest and is reported as
//! [`DeliverRow::dropped`]; `push_depth` and `retain_depth` are the two
//! independent knobs a subscription is defined by. See `docs/message-bus.md`.

use brenn_envelope::MessageEnvelope;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// The RFC 8030 urgency ladder, re-exported from the carrier crate.
///
/// Named in this crate's own surface ([`ClientFrame::Publish::urgency`],
/// [`BatchEntry::urgency`]) so an attacher client reaches it through the
/// protocol contract it already depends on rather than taking a direct
/// dependency on `brenn-envelope` for one enum.
pub use brenn_envelope::Urgency;

/// The inclusive range of transport versions one end of an attachment speaks.
///
/// A range rather than a single number because the two ends deploy
/// independently: each states everything it can speak and [`negotiate`] picks
/// the highest both hold. `min > max` states an empty range, which never
/// negotiates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VersionRange {
    pub min: u32,
    pub max: u32,
}

impl VersionRange {
    /// The range holding exactly one version.
    pub const fn exactly(version: u32) -> Self {
        Self {
            min: version,
            max: version,
        }
    }
}

/// The transport versions this build speaks.
///
/// One version, and no support window is promised: until a consumer exists that
/// deploys out of lockstep with the backend, this is an internal protocol under
/// the project's no-backward-compat default, and any schema change bumps the
/// number on both ends at once. The negotiation *mechanism* exists from the
/// start regardless, because it is the one thing that cannot be added later
/// without breaking the frozen [`ClientFrame::Hello`] shell.
pub const SUPPORTED_VERSIONS: VersionRange = VersionRange::exactly(4);

/// The version two ends agree on, or `None` when their ranges do not overlap.
///
/// The highest version both can speak: `min(a.max, b.max)`, valid only when it
/// is at least `max(a.min, b.min)`. Symmetric — each end computes it
/// independently from the two `Hello` frames and reaches the same answer, which
/// is why an incompatibility needs no refusal frame: both sides already know,
/// and both close.
///
/// An empty range (`min > max`) never overlaps anything, so a malformed peer
/// range falls out as `None` rather than needing its own check.
pub fn negotiate(a: VersionRange, b: VersionRange) -> Option<u32> {
    let agreed = a.max.min(b.max);
    (agreed >= a.min.max(b.min)).then_some(agreed)
}

// ---------------------------------------------------------------------------
// Client → server frames
// ---------------------------------------------------------------------------

/// Client → server frame.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum ClientFrame {
    /// The attacher's half of the symmetric version handshake, sent as its first
    /// frame without waiting for the server's.
    ///
    /// **This frame's shape is frozen.** It is the one schema that must parse
    /// under every version, in both directions, because it is what decides which
    /// version is in force; a field added to it in some later version would be
    /// unparseable to the very peer the handshake exists to reach. Version-gated
    /// growth belongs in [`ServerFrame::Welcome`] and beyond.
    Hello {
        versions: VersionRange,
        /// A free-form build identifier, for logs only. Never parsed and never
        /// branched on: compatibility is [`negotiate`]'s answer, not a string
        /// comparison.
        ident: String,
    },
    /// Open a subscription on `channel`, stating both subscription knobs
    /// explicitly.
    ///
    /// One subscription per channel per attachment: a second `Subscribe` on a
    /// channel already subscribed replaces nothing and is a protocol violation.
    /// Fan-out to whatever the attacher's application layer binds to the channel
    /// is the client's own business — the wire delivers each message once.
    Subscribe {
        channel: String,
        /// What wakes the attacher: how many of the most recent unseen messages
        /// one activation is handed, with pending activations coalescing into
        /// one. `0` means this subscription never activates on new messages.
        push_depth: u64,
        /// What the attacher can see: the private window of most-recent messages
        /// it may read. Does not cause activation.
        retain_depth: u64,
        /// The last [`DeliverRow::cursor`] this attacher accepted on this
        /// channel, or `None` to start from the channel's retained tail.
        ///
        /// The server holds no per-attacher position, so resuming is entirely
        /// the client's claim; the server answers it with a
        /// [`SubscribeResult`](ServerFrame::SubscribeResult) that carries a
        /// [`GapInfo`] when the claim could not be covered.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        resume: Option<Cursor>,
    },
    /// Close the subscription on `channel`. Naming a channel with no live
    /// subscription on this attachment is a protocol violation.
    Unsubscribe { channel: String },
    /// Publish one message to `channel`, immediately.
    ///
    /// Deferral rides [`ClientFrame::PublishBatch`] only: parking a message
    /// belongs to the atomic-flush path, where the parked-set mirror
    /// ([`ServerFrame::DeferredView`]) and its control ops live.
    Publish {
        channel: String,
        /// Which sub-identity of the attached principal is sending, or `None`
        /// for the attacher itself.
        ///
        /// Opaque to the transport: an attribution is a string the *server's*
        /// configuration must already name, never an identity the client
        /// spells. The server validates it against the declared set, mints the
        /// envelope sender from it, and meters the send against that
        /// sub-identity's own budget — which is what keeps one noisy
        /// sub-identity from spending its neighbours' allowance. An
        /// undeclared attribution is a protocol violation.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        attribution: Option<String>,
        body: String,
        /// Sender intent about how hard the bus should work to wake a
        /// subscriber. Always concrete on the wire: a channel's configured
        /// default is resolved by the client, which needs it anyway to stamp
        /// the envelopes it routes without a server.
        urgency: Urgency,
        /// Routes the eventual [`ServerFrame::PublishResult`] back, or `None` to
        /// ask for no answer.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        correlation: Option<u64>,
    },
    /// One activation's whole flush, atomically.
    ///
    /// The transport for the activation model's flush-on-ok rule: publishes are
    /// buffered by the client during a handler and, iff it returns ok, arrive
    /// here as one frame. The server applies the durable entries in **one
    /// transaction** (all-or-nothing, in call order) and fans the ephemeral ones
    /// out at the same point, judging every `deliver_after` against a single
    /// read of its clock.
    ///
    /// Contract: call order is preserved within each delivery class; cross-class
    /// relative ordering is not guaranteed.
    ///
    /// **Legality is protocol law**, stated by [`check_batch_legality`]: a
    /// bounded op count and a bounded charge of channel and body bytes, both
    /// checked by the client before it sends and by the server after it parses.
    /// The law is what makes [`max_client_frame_bytes`] able to bound every
    /// legal frame, so a conforming attacher can never compose a flush its peer
    /// reads as tampering.
    ///
    /// **Every per-entry error is violation-grade**, unlike the single
    /// [`Publish`](ClientFrame::Publish) whose oversize body is an outcome. The
    /// client is the primary enforcer here — it checks each entry's channel,
    /// body size, and the per-activation caps at buffer time and answers its own
    /// caller — so an entry arriving broken means a non-compliant attacher.
    PublishBatch {
        /// The sub-identity whose activation produced this flush; `None` for the
        /// attacher itself. Validated and metered exactly as
        /// [`Publish::attribution`](ClientFrame::Publish) is.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        attribution: Option<String>,
        /// Routes the eventual [`ServerFrame::PublishBatchResult`] back.
        /// Required, unlike `Publish.correlation`: a batch is only ever produced
        /// by a flush, which always wants to know whether the server took it.
        correlation: u64,
        /// The activation's buffered publishes, in call order. Bounded by
        /// `brenn_budget::MAX_PUBLISHES_PER_ACTIVATION`; a longer batch is a
        /// violation, as is one over the whole-batch law.
        publishes: Vec<BatchEntry>,
        /// The activation's buffered control ops against messages this sender
        /// already parked, in call order. Applied **before** `publishes`: an op
        /// names a message an earlier activation parked, so applying it first
        /// keeps this activation's own publishes out of its way. Bounded by the
        /// same per-activation cap as `publishes`.
        ///
        /// A flush carrying only ops is a whole batch, so a `PublishBatch` is
        /// empty only when both lists are.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        deferred_ops: Vec<BatchDeferredOp>,
    },
    /// Raise an operator alert.
    ///
    /// Deliberately not a channel publish. Alerting exists to page an operator
    /// when subsystems are broken, so it must not depend on the machinery it
    /// reports on: this frame reaches the in-process alert dispatcher directly,
    /// touching no channel, no ACL resolution, and no store write. It is
    /// attacher-generic — nothing about `(severity, title, body)` is specific to
    /// any application layer — and it is grant-gated deny-by-default: the server
    /// advertises the grant in [`ServerFrame::Welcome`], a conforming client
    /// suppresses ungranted alerts itself, and an alert from an ungranted
    /// attacher is a protocol violation.
    Alert {
        /// Which sub-identity of the attached principal is paging, or `None` for
        /// the attacher itself.
        ///
        /// Opaque to the transport and validated exactly as
        /// [`Publish::attribution`](ClientFrame::Publish) is: the server admits
        /// it against the declared set and then judges it against that
        /// sub-identity's own alert right, so a sub-identity that may not page
        /// cannot page under its neighbours' or its attacher's name. An
        /// undeclared attribution is a protocol violation.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        attribution: Option<String>,
        severity: AlertSeverity,
        title: String,
        body: String,
    },
}

/// One buffered publish inside a [`ClientFrame::PublishBatch`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BatchEntry {
    pub channel: String,
    pub body: String,
    /// Sender intent, concrete as on [`ClientFrame::Publish`].
    pub urgency: Urgency,
    /// The requested release time, epoch milliseconds UTC; `None` for an
    /// ordinary publish.
    ///
    /// The server is the deferral authority — it holds the channel's retention,
    /// and a durable schedule must outlive the attacher — so the client states
    /// the time and the server decides park-vs-immediate against its own clock.
    /// A value in the past commits immediately.
    ///
    /// A value no timestamp can carry is a violation, not an outcome: the client
    /// refuses one at buffer time, so an unrepresentable time here is a batch no
    /// conforming client produced.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deliver_after: Option<u64>,
}

/// One buffered control op inside a [`ClientFrame::PublishBatch`]: what an
/// activation did to a message it had already parked.
///
/// The server is the parked set's authority — it holds the retention, and a
/// durable schedule must outlive the attacher — so the client cannot apply the
/// op itself. It states which message and what to do, and the server applies it
/// under the same sub-identity the batch's publishes use.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BatchDeferredOp {
    pub channel: String,
    /// The parked message's identity, taken from the
    /// [`ServerFrame::DeferredView`] the client holds, so the id is one the
    /// server minted.
    ///
    /// An id parked by a *different* sender is a protocol violation, not an
    /// outcome: a conforming client can only name what a sender-scoped view
    /// showed it. A message that simply released between the snapshot and this
    /// frame is the benign race instead, which the server logs and counts.
    pub message_id: Uuid,
    pub op: DeferredOpKind,
}

/// What a [`BatchDeferredOp`] does to the message it names.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", deny_unknown_fields)]
pub enum DeferredOpKind {
    /// Drop the schedule: the message never publishes.
    Cancel,
    /// Rewrite the message's body, its release time, or both; `None` leaves that
    /// half alone.
    ///
    /// An oversize `body` is a violation for the same reason a
    /// [`BatchEntry::body`] over the cap is. A `deliver_after` already in the
    /// past does not publish here: the edit lands and the server's next release
    /// pass takes it.
    Edit {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        body: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        deliver_after: Option<u64>,
    },
}

/// Severity of a [`ClientFrame::Alert`]. Must stay 1:1 with the WASM WIT
/// `alert.severity` enum and the native `AlertSeverity`; serialized lowercase
/// to match that shared vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AlertSeverity {
    Info,
    Warning,
    Critical,
}

impl AlertSeverity {
    /// The [`AlertSeverity`] for a lowercase wire string (`"info"`/`"warning"`/
    /// `"critical"`), or `None` for anything else — the inverse of the
    /// serde-lowercase serialization. Parses an untrusted severity string an
    /// application layer supplies before it reaches this typed frame; an
    /// unrecognized string is a malformed alert, dropped rather than coerced to
    /// a severity.
    pub fn from_wire_str(s: &str) -> Option<Self> {
        match s {
            "info" => Some(Self::Info),
            "warning" => Some(Self::Warning),
            "critical" => Some(Self::Critical),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Server → client frames
// ---------------------------------------------------------------------------

/// Server → client frame.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum ServerFrame {
    /// The server's half of the symmetric version handshake, sent immediately on
    /// upgrade without waiting for the client's. Frozen shell, exactly as
    /// [`ClientFrame::Hello`] is.
    Hello {
        versions: VersionRange,
        /// A free-form build identifier, for logs only. Never parsed.
        ident: String,
    },
    /// Sent once, after a successful negotiation: the transport contract of
    /// *this* attachment.
    ///
    /// Every field is a fact about the connection itself. Application
    /// configuration is deliberately absent — it is state, and state on this bus
    /// is a retained channel, which the attacher subscribes to like anything
    /// else.
    Welcome {
        /// The negotiated transport version, echoed so the client can assert the
        /// two ends reached the same answer.
        version: u32,
        /// The principal this attachment speaks as, before any attribution.
        participant_id: String,
        /// This attachment's server-minted id, so the attacher can self-attribute
        /// the documents it authors.
        session_id: String,
        /// Idle-heartbeat interval advertisement: the cadence of
        /// [`ServerFrame::Heartbeat`], against which the client arms its
        /// inbound-silence liveness rule.
        heartbeat_secs: u32,
        /// The server's publish-body cap, for client-side pre-validation.
        max_body_bytes: u64,
        /// The server's websocket read cap. Advertised rather than derived
        /// client-side so the number the server actually enforces is the number
        /// the client honours; [`max_client_frame_bytes`] is how the server gets
        /// it from `max_body_bytes`.
        max_frame_bytes: u64,
        /// Whether this attachment's policy grants the alert plane. A conforming
        /// client suppresses [`ClientFrame::Alert`] when this is false, so an
        /// ungranted alert reaches the server only from a non-conforming client.
        /// The attacher learns its rights from the server and never guesses.
        alert_granted: bool,
    },
    /// Idle-liveness signal, server → client only: a browser websocket cannot
    /// observe protocol-level pings, so liveness is an application frame and the
    /// rule is client-side inbound silence.
    Heartbeat,
    /// The answer to one [`ClientFrame::Subscribe`].
    SubscribeResult {
        channel: String,
        outcome: SubscribeOutcome,
        /// How many retained messages this subscribe is about to replay.
        replay_count: u32,
        /// Present when replay could not cover the requested `resume` point.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        gap: Option<GapInfo>,
    },
    /// One delivery pass on `channel`: every row the server produced in that
    /// pass, oldest first.
    ///
    /// **One frame is one delivery point.** A subscribe replay, a catch-up
    /// drain and a single live row are each one pass, so a multi-row frame is
    /// one delivery point by construction and an attacher that windows what a
    /// frame delivered sees the pass as one arrival rather than as N. Several
    /// frames remain legal — they are simply several delivery points, to be
    /// emitted knowingly.
    ///
    /// Single-target: the wire carries a message once per (attachment,
    /// channel), and whatever the attacher binds to that channel is the
    /// attacher's own fan-out. The per-row `seq`, `cursor` and `dropped` are
    /// therefore facts of this attachment's one stream on this channel.
    ///
    /// `rows` is never empty: a pass with nothing to deliver writes no frame.
    Deliver {
        channel: String,
        rows: Vec<DeliverRow>,
    },
    /// The answer to one [`ClientFrame::Publish`] that asked for one.
    PublishResult {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        correlation: Option<u64>,
        outcome: PublishOutcome,
    },
    /// The answer to one [`ClientFrame::PublishBatch`].
    PublishBatchResult {
        correlation: u64,
        outcome: PublishBatchOutcome,
    },
    /// The server's view of what one sub-identity has parked on one channel,
    /// soonest release first.
    ///
    /// A **full snapshot**: it replaces whatever the client held for
    /// `(channel, attribution)`, so it is idempotent and last-writer-wins. The
    /// client cannot maintain this itself — durable parked entries outlive the
    /// attachment, releases happen on the server's clock, and every attachment of
    /// one principal shares its parked set — so the authority pushes it.
    ///
    /// Emitted to every attachment of the principal whenever the parked set
    /// changes, and once per nonempty set after [`Welcome`](ServerFrame::Welcome).
    /// The client clears every mirror at `Welcome`, so a set with no frame is
    /// empty.
    DeferredView {
        channel: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        attribution: Option<String>,
        entries: Vec<DeferredViewEntry>,
    },
}

/// One delivered envelope inside a [`ServerFrame::Deliver`] pass.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeliverRow {
    pub envelope: MessageEnvelope,
    /// A delivery-time span sequence, assigned at socket-write time and strictly
    /// increasing per subscription-span (a span starts at each
    /// [`SubscribeResult`](ServerFrame::SubscribeResult), the counter restarting
    /// at 1), across rows within a pass as well as across passes, for replay and
    /// live rows alike. It exists solely for the client's continuity check on a
    /// peer it must not trust blindly; a non-increasing `seq` is a fatal protocol
    /// error.
    pub seq: u64,
    /// The resume token for this channel as of this row. The client stores the
    /// latest accepted one — the last row of the last accepted frame — and echoes
    /// it verbatim on its next [`Subscribe`](ClientFrame::Subscribe); it never
    /// interprets it.
    pub cursor: Cursor,
    /// Messages lost on this channel since the previous delivery on this
    /// attachment — the channel's window rolled past the cursor. `0` = none. The
    /// loss belongs to the subscription rather than to a message, so it rides the
    /// first row that follows it and the rest of the pass carries `0`; a nonzero
    /// count on any row but the first is a fatal protocol error, as a
    /// non-increasing `seq` is.
    pub dropped: u64,
}

/// One parked message in a [`ServerFrame::DeferredView`].
///
/// Carries `message_id` where an activation's deferred entry carries a snapshot
/// index: the id is the identity both ends know a parked message by, and the
/// index is per-snapshot. `deliver_after` is the release time, epoch
/// milliseconds UTC.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeferredViewEntry {
    pub message_id: Uuid,
    pub body: String,
    pub deliver_after: u64,
}

/// An opaque per-channel resume token the attacher stores and echoes verbatim,
/// never interprets.
///
/// A [`Deliver`](ServerFrame::Deliver) carries the latest one; the client keeps
/// it per channel and presents it as
/// [`Subscribe::resume`](ClientFrame::Subscribe) on the next subscribe. Only the
/// server mints and reads its contents, and one encoding serves every channel
/// class — the classes differ in what a restart destroys, not in how a position
/// is named. The inner string is **private** and the type has **no accessor and
/// no constructor**: the sanctioned server-side access is a serde round-trip
/// only (build via `serde_json::from_value::<Cursor>(Value::String(s))`, read
/// via matching `serde_json::to_value(&cursor)` for a `Value::String`).
///
/// Interpretation code lives in the server, never here: this crate links into
/// wasm attachers, so any code in it — even never-called interpretation code —
/// executes on an untrusted client. Moving a class branch into another crate
/// changes which file holds the branch and nothing about where it runs; that
/// crate-laundering pattern is forbidden. The rule's jurisdiction is *where code
/// executes*, not which crate names it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Cursor(String);

/// Result of a [`ClientFrame::Subscribe`].
///
/// A subscribe the attacher was never entitled to make is a protocol violation
/// that kills the connection rather than a wire outcome — including one on a
/// channel outside its grants, which answers identically whether or not the
/// channel exists, so the wire is no existence oracle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
pub enum SubscribeOutcome {
    Ok,
    /// The channel is inside this attacher's grants but does not exist right
    /// now: a profile whose channels are provisioned at runtime raced their
    /// coming and going.
    ///
    /// Non-fatal and disclosing nothing an operator did not authorize — the
    /// grant that admits the address is the operator's own statement that this
    /// attacher may know whether it is there. The subscription is not opened:
    /// nothing replays, no span starts, and the client is free to ask again
    /// when whatever names the channel says it exists. A profile whose channels
    /// are all provisioned before it accepts a connection never produces this,
    /// and a client of such a profile treats it as a broken peer.
    Unavailable,
}

/// Result of a [`ClientFrame::Publish`].
///
/// Every variant here is reachable by a *conforming* client under load or a
/// race; anything a conforming client cannot reach is a violation, not an
/// outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
pub enum PublishOutcome {
    Ok,
    /// The sender's server-side budget refused the publish. Never a violation:
    /// the client is the primary limiter and this backstop is only reached by an
    /// attacher out-running it.
    RateLimited,
    BodyTooLarge {
        len: u64,
        max: u64,
    },
    /// The server accepted the frame but the publish failed on a path the
    /// server's profile declares non-fatal — where panicking on the failure
    /// would be worse than reporting it, because the failing publish is itself
    /// the error-reporting path.
    Failed,
}

/// Result of a [`ClientFrame::PublishBatch`].
///
/// Two variants, deliberately not a reuse of [`PublishOutcome`]: the single
/// publish's other outcomes are violation-grade here (`BodyTooLarge` — the
/// client gates bodies at buffer time) or impossible (`Failed` — the
/// error-report backstop is not a batch path), so reusing that enum would
/// advertise arms this frame can never carry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
pub enum PublishBatchOutcome {
    /// Applied: durable entries committed in one transaction, ephemeral entries
    /// fanned out, all in call order.
    Ok,
    /// The sender's server-side budget refused the batch. The honest outcome when
    /// the two budget tiers disagree — never a violation and never a kill.
    RateLimited,
}

/// A gap in the replay window, attached to a
/// [`SubscribeResult`](ServerFrame::SubscribeResult) when replay could not cover
/// the client's requested resume point.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GapInfo {
    pub reason: GapReason,
}

/// Why replay was gapped.
///
/// Deliberately has no `ResumeAhead`: a matching epoch with a position the server
/// never assigned is impossible for an honest client, so the transport treats it
/// as a protocol violation rather than a wire gap.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
pub enum GapReason {
    EpochChanged,
    /// Resume could not be covered from the retained window: the requested
    /// position predates the oldest retained message, or the per-subscriber
    /// retain clamp truncated the re-send set. Class-neutral, and conservative —
    /// a false "may have missed" is honest; a false negative is not.
    BeyondRetained,
}

/// Generous allowance for everything in a [`ClientFrame::Publish`] besides the
/// body: JSON keys, type/kind tags, the channel address, the attribution, and
/// the correlation. All of them are orders of magnitude under this allowance;
/// the slack is what lets the frame cap be derived from `max_body_bytes` alone
/// rather than tracking each field.
pub const PUBLISH_FRAME_OVERHEAD_BYTES: usize = 8 * 1024;

/// Worst-case JSON string expansion: one source byte serializes as at most six
/// (`\u00XX` for a control character).
const JSON_ESCAPE_EXPANSION: usize = 6;

/// How many operations one [`ClientFrame::PublishBatch`] may carry — publishes
/// and control ops together.
///
/// Wide enough to admit the widest flush an activation can buffer: the
/// per-activation caps allow `brenn_budget::MAX_PUBLISHES_PER_ACTIVATION`
/// publishes *and* the same number of control ops, so at twice that number the
/// count half of the law never refuses a flush the buffer-time budget accepted.
/// The number is spelled here rather than imported because this crate links into
/// wasm attachers on three dependencies; the server's batch handler holds the
/// tripwire that keeps the two in step.
pub const MAX_BATCH_OPS: usize = 512;

/// Per-operation allowance in the frame-cap derivation: the JSON keys, the
/// braces and commas, the type and kind tags, an urgency, a release timestamp
/// and a message uuid of one batch operation.
///
/// The two unbounded fields of an operation — its channel address and its body —
/// are deliberately not in here. They are charged against the batch's own byte
/// budget by [`batch_charged_bytes`] instead, which is what makes the derivation
/// below a bound rather than an estimate.
pub const BATCH_OP_OVERHEAD_BYTES: usize = 256;

/// Why a [`ClientFrame::PublishBatch`] is not a legal frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchIllegal {
    /// More publishes and control ops than [`MAX_BATCH_OPS`] together.
    TooManyOps { ops: usize, cap: usize },
    /// More charged bytes than the attachment's body cap.
    TooManyBytes { bytes: usize, cap: usize },
}

impl std::fmt::Display for BatchIllegal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TooManyOps { ops, cap } => {
                write!(f, "{ops} batch operations, over the cap of {cap}")
            }
            Self::TooManyBytes { bytes, cap } => write!(
                f,
                "{bytes} charged batch bytes, over the {cap}-byte body cap"
            ),
        }
    }
}

/// What a batch's two lists charge against the body cap: every variable-length
/// string they carry, which is each operation's channel address plus each body
/// it publishes or writes.
///
/// A batch is charged as a whole rather than per entry because it travels as one
/// frame: what has to be bounded is the frame, and one budget spent across the
/// operations bounds it whatever the mix. Charging the channel alongside the
/// body is what closes the derivation — an address is client-composed and has no
/// length of its own, so a rule that ignored it would bound nothing.
pub fn batch_charged_bytes(publishes: &[BatchEntry], deferred_ops: &[BatchDeferredOp]) -> usize {
    let entries: usize = publishes
        .iter()
        .map(|entry| entry.channel.len() + entry.body.len())
        .sum();
    let ops: usize = deferred_ops
        .iter()
        .map(|op| {
            op.channel.len()
                + match &op.op {
                    DeferredOpKind::Cancel => 0,
                    DeferredOpKind::Edit { body, .. } => body.as_ref().map_or(0, String::len),
                }
        })
        .sum();
    entries + ops
}

/// Whether these two lists compose a legal [`ClientFrame::PublishBatch`] on an
/// attachment whose body cap is `max_body_bytes`.
///
/// Protocol law, enforced at both ends of the same wire: the client checks it
/// before composing the frame — an illegal batch is an embedder bug, since the
/// buffer-time gates exist to prevent one — and the server checks it after
/// parsing, where an illegal-but-parseable batch is a protocol violation.
///
/// The two clauses are what [`max_client_frame_bytes`] is derived from, so a
/// legal batch is by construction a frame the peer will read.
pub fn check_batch_legality(
    publishes: &[BatchEntry],
    deferred_ops: &[BatchDeferredOp],
    max_body_bytes: usize,
) -> Result<(), BatchIllegal> {
    let ops = publishes.len() + deferred_ops.len();
    if ops > MAX_BATCH_OPS {
        return Err(BatchIllegal::TooManyOps {
            ops,
            cap: MAX_BATCH_OPS,
        });
    }
    let bytes = batch_charged_bytes(publishes, deferred_ops);
    if bytes > max_body_bytes {
        return Err(BatchIllegal::TooManyBytes {
            bytes,
            cap: max_body_bytes,
        });
    }
    Ok(())
}

/// The websocket read cap, derived — not a fixed constant. `max_body_bytes` is
/// operator config, so a fixed frame cap could contradict a legal config; and
/// worst-case JSON string escaping expands one body byte to six (`\u00XX` for
/// control characters), so even a default-config-legal body needs ~6x headroom.
/// The server computes this from its config and advertises the result as
/// [`Welcome::max_frame_bytes`].
///
/// The derivation covers the worst legal frame of either publishing shape,
/// because [`check_batch_legality`] gives a batch the same byte budget a single
/// [`ClientFrame::Publish`] gets: the escaped budget, plus a fixed allowance per
/// batch operation for the fields no budget charges, plus the base overhead a
/// single publish is already sized with. A batch admitting more than that is not
/// a wider frame but an illegal one, refused at both ends.
///
/// [`Welcome::max_frame_bytes`]: ServerFrame::Welcome
pub fn max_client_frame_bytes(max_body_bytes: usize) -> usize {
    // Checked, not wrapping: this value gates a fail2ban decision (an oversized
    // frame is a protocol violation), and it must equal the number a 32-bit wasm
    // client honours. An operator `max_body_bytes` large enough to overflow is a
    // config contradiction — fail fast rather than derive a wrong cap.
    max_body_bytes
        .checked_mul(JSON_ESCAPE_EXPANSION)
        .and_then(|scaled| scaled.checked_add(MAX_BATCH_OPS * BATCH_OP_OVERHEAD_BYTES))
        .and_then(|scaled| scaled.checked_add(PUBLISH_FRAME_OVERHEAD_BYTES))
        .expect("max_body_bytes too large: WS frame-cap derivation overflowed usize")
}

/// [`ClientFrame::Alert`] `title` cap. Client-enforced by truncation; a longer
/// title reaching the server is a violation.
pub const MAX_ALERT_TITLE_BYTES: usize = 256;

/// [`ClientFrame::Alert`] `body` cap. Client-enforced by truncation, like
/// [`MAX_ALERT_TITLE_BYTES`].
pub const MAX_ALERT_BODY_BYTES: usize = 4 * 1024;

#[cfg(test)]
mod tests {
    use super::*;
    use brenn_envelope::ChannelScheme;
    use chrono::{DateTime, Utc};
    use serde_json::{Value, json};

    fn sample_cursor() -> Cursor {
        serde_json::from_value(json!("opaque-token-7")).unwrap()
    }

    fn sample_envelope() -> MessageEnvelope {
        MessageEnvelope {
            message_id: Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap(),
            source: "src".to_string(),
            channel: "ephemeral:demo".to_string(),
            sender: "surface:deskbar".to_string(),
            publish_ts: DateTime::<Utc>::from_timestamp(1_700_000_000, 0).unwrap(),
            body: "hello".to_string(),
            reply_to: None,
            delivery_deadline: None,
            deliver_after: None,
            impetus: None,
            urgency: Urgency::Normal,
            envelope_type: ChannelScheme::Ephemeral,
        }
    }

    /// Golden-plus-round-trip: a frame cannot be pinned without also proving it
    /// survives the trip.
    fn pin_client(frame: &ClientFrame, golden: Value) {
        assert_eq!(serde_json::to_value(frame).unwrap(), golden);
        assert_eq!(
            &serde_json::from_value::<ClientFrame>(golden).unwrap(),
            frame
        );
    }

    fn pin_server(frame: &ServerFrame, golden: Value) {
        assert_eq!(serde_json::to_value(frame).unwrap(), golden);
        assert_eq!(
            &serde_json::from_value::<ServerFrame>(golden).unwrap(),
            frame
        );
    }

    /// The negotiation table, including the rows a hand-written implementation
    /// gets wrong: touching ranges, and a malformed peer range that must not
    /// negotiate to anything.
    #[test]
    fn negotiation_table() {
        let r = |min, max| VersionRange { min, max };
        for (a, b, expected) in [
            // Identical single-version ranges: the common case today.
            (r(1, 1), r(1, 1), Some(1)),
            // Overlapping: the highest both speak wins.
            (r(1, 3), r(2, 5), Some(3)),
            (r(2, 5), r(1, 3), Some(3)),
            (r(1, 9), r(4, 4), Some(4)),
            // Touching at exactly one version.
            (r(1, 2), r(2, 7), Some(2)),
            // Disjoint in either direction.
            (r(1, 1), r(2, 2), None),
            (r(2, 2), r(1, 1), None),
            (r(1, 3), r(7, 9), None),
            // A malformed peer range is empty and overlaps nothing.
            (r(5, 1), r(1, 9), None),
            (r(1, 9), r(5, 1), None),
            (r(5, 1), r(5, 1), None),
        ] {
            assert_eq!(negotiate(a, b), expected, "negotiate({a:?}, {b:?})");
        }
    }

    /// Each end computes the agreement independently from the two `Hello`s, which
    /// is what lets an incompatibility close with no refusal frame.
    #[test]
    fn negotiation_is_symmetric() {
        let ranges = [(1, 1), (1, 3), (2, 5), (4, 4), (7, 9), (5, 1)];
        for a in ranges {
            for b in ranges {
                let a = VersionRange { min: a.0, max: a.1 };
                let b = VersionRange { min: b.0, max: b.1 };
                assert_eq!(negotiate(a, b), negotiate(b, a), "{a:?} vs {b:?}");
            }
        }
    }

    /// The tripwire for the crate's own rule: any schema change bumps the number
    /// on both ends at once.
    ///
    /// Every other assertion in the tree states the version symbolically, which
    /// is right for churn and leaves nothing holding the number itself. Without
    /// this, a breaking frame change that forgets the bump ships a server
    /// speaking one v2 and a cached page speaking another, and negotiation — the
    /// mechanism built to catch exactly that — reports agreement. Editing the
    /// literal below is the acknowledgement that both ends moved together.
    #[test]
    fn the_wire_version_is_pinned_to_this_frame_shape() {
        assert_eq!(SUPPORTED_VERSIONS, VersionRange::exactly(4));
    }

    #[test]
    fn this_build_negotiates_with_itself() {
        assert_eq!(
            negotiate(SUPPORTED_VERSIONS, SUPPORTED_VERSIONS),
            Some(SUPPORTED_VERSIONS.max)
        );
    }

    #[test]
    fn client_hello_is_pinned() {
        pin_client(
            &ClientFrame::Hello {
                versions: VersionRange { min: 1, max: 1 },
                ident: "attach-client/0.1.0".to_string(),
            },
            json!({
                "type": "Hello",
                "versions": {"min": 1, "max": 1},
                "ident": "attach-client/0.1.0",
            }),
        );
    }

    #[test]
    fn client_subscribe_is_pinned() {
        pin_client(
            &ClientFrame::Subscribe {
                channel: "ephemeral:demo".to_string(),
                push_depth: 5,
                retain_depth: 20,
                resume: Some(sample_cursor()),
            },
            json!({
                "type": "Subscribe",
                "channel": "ephemeral:demo",
                "push_depth": 5,
                "retain_depth": 20,
                "resume": "opaque-token-7",
            }),
        );
    }

    /// A first subscribe omits `resume` rather than sending null: absent is the
    /// claim "I hold no position", and a client should not have to distinguish it
    /// from an explicit null.
    #[test]
    fn client_subscribe_omits_an_absent_resume() {
        pin_client(
            &ClientFrame::Subscribe {
                channel: "brenn:orders".to_string(),
                push_depth: 0,
                retain_depth: 1,
                resume: None,
            },
            json!({
                "type": "Subscribe",
                "channel": "brenn:orders",
                "push_depth": 0,
                "retain_depth": 1,
            }),
        );
    }

    #[test]
    fn client_unsubscribe_is_pinned() {
        pin_client(
            &ClientFrame::Unsubscribe {
                channel: "ephemeral:demo".to_string(),
            },
            json!({"type": "Unsubscribe", "channel": "ephemeral:demo"}),
        );
    }

    #[test]
    fn client_publish_is_pinned() {
        pin_client(
            &ClientFrame::Publish {
                channel: "brenn:orders".to_string(),
                attribution: Some("ticker".to_string()),
                body: "{}".to_string(),
                urgency: Urgency::High,
                correlation: Some(9),
            },
            json!({
                "type": "Publish",
                "channel": "brenn:orders",
                "attribution": "ticker",
                "body": "{}",
                "urgency": "high",
                "correlation": 9,
            }),
        );
    }

    /// The attacher's own identity is the absence of an attribution, not a
    /// reserved string: nothing a client could spell may name a principal.
    #[test]
    fn client_publish_omits_an_absent_attribution() {
        pin_client(
            &ClientFrame::Publish {
                channel: "ephemeral:status".to_string(),
                attribution: None,
                body: "{}".to_string(),
                urgency: Urgency::Normal,
                correlation: None,
            },
            json!({
                "type": "Publish",
                "channel": "ephemeral:status",
                "body": "{}",
                "urgency": "normal",
            }),
        );
    }

    #[test]
    fn client_publish_batch_is_pinned() {
        pin_client(
            &ClientFrame::PublishBatch {
                attribution: Some("ticker".to_string()),
                correlation: 4,
                publishes: vec![
                    BatchEntry {
                        channel: "brenn:orders".to_string(),
                        body: "now".to_string(),
                        urgency: Urgency::Normal,
                        deliver_after: None,
                    },
                    BatchEntry {
                        channel: "brenn:orders".to_string(),
                        body: "later".to_string(),
                        urgency: Urgency::Low,
                        deliver_after: Some(1_700_000_000_000),
                    },
                ],
                deferred_ops: vec![
                    BatchDeferredOp {
                        channel: "brenn:orders".to_string(),
                        message_id: Uuid::parse_str("00000000-0000-0000-0000-00000000000a")
                            .unwrap(),
                        op: DeferredOpKind::Cancel,
                    },
                    BatchDeferredOp {
                        channel: "brenn:orders".to_string(),
                        message_id: Uuid::parse_str("00000000-0000-0000-0000-00000000000b")
                            .unwrap(),
                        op: DeferredOpKind::Edit {
                            body: Some("revised".to_string()),
                            deliver_after: None,
                        },
                    },
                ],
            },
            json!({
                "type": "PublishBatch",
                "attribution": "ticker",
                "correlation": 4,
                "publishes": [
                    {"channel": "brenn:orders", "body": "now", "urgency": "normal"},
                    {
                        "channel": "brenn:orders",
                        "body": "later",
                        "urgency": "low",
                        "deliver_after": 1_700_000_000_000u64,
                    },
                ],
                "deferred_ops": [
                    {
                        "channel": "brenn:orders",
                        "message_id": "00000000-0000-0000-0000-00000000000a",
                        "op": {"kind": "cancel"},
                    },
                    {
                        "channel": "brenn:orders",
                        "message_id": "00000000-0000-0000-0000-00000000000b",
                        "op": {"kind": "edit", "body": "revised"},
                    },
                ],
            }),
        );
    }

    /// An ops-only flush is a whole batch, and a publishes-only flush omits the
    /// ops list entirely.
    #[test]
    fn client_publish_batch_omits_an_empty_op_list() {
        pin_client(
            &ClientFrame::PublishBatch {
                attribution: None,
                correlation: 1,
                publishes: vec![BatchEntry {
                    channel: "ephemeral:demo".to_string(),
                    body: "x".to_string(),
                    urgency: Urgency::VeryLow,
                    deliver_after: None,
                }],
                deferred_ops: vec![],
            },
            json!({
                "type": "PublishBatch",
                "correlation": 1,
                "publishes": [
                    {"channel": "ephemeral:demo", "body": "x", "urgency": "very-low"},
                ],
            }),
        );
    }

    #[test]
    fn client_alert_is_pinned() {
        pin_client(
            &ClientFrame::Alert {
                attribution: None,
                severity: AlertSeverity::Critical,
                title: "chrome died".to_string(),
                body: "the mount plan failed".to_string(),
            },
            json!({
                "type": "Alert",
                "severity": "critical",
                "title": "chrome died",
                "body": "the mount plan failed",
            }),
        );
    }

    #[test]
    fn an_attributed_alert_carries_its_sub_identity() {
        pin_client(
            &ClientFrame::Alert {
                attribution: Some("meeting".to_string()),
                severity: AlertSeverity::Warning,
                title: "camera unreachable".to_string(),
                body: "no frames for 30s".to_string(),
            },
            json!({
                "type": "Alert",
                "attribution": "meeting",
                "severity": "warning",
                "title": "camera unreachable",
                "body": "no frames for 30s",
            }),
        );
    }

    #[test]
    fn alert_severity_wire_strings_round_trip() {
        for (severity, wire) in [
            (AlertSeverity::Info, "info"),
            (AlertSeverity::Warning, "warning"),
            (AlertSeverity::Critical, "critical"),
        ] {
            assert_eq!(serde_json::to_value(severity).unwrap(), json!(wire));
            assert_eq!(AlertSeverity::from_wire_str(wire), Some(severity));
        }
        assert_eq!(AlertSeverity::from_wire_str("fatal"), None);
    }

    #[test]
    fn server_hello_is_pinned() {
        pin_server(
            &ServerFrame::Hello {
                versions: VersionRange { min: 1, max: 1 },
                ident: "brenn-server/0.15.0".to_string(),
            },
            json!({
                "type": "Hello",
                "versions": {"min": 1, "max": 1},
                "ident": "brenn-server/0.15.0",
            }),
        );
    }

    /// The whole `Welcome`, field by field: this is the transport contract, and
    /// every field on it is a fact of the attachment rather than application
    /// config. A field that is not such a fact does not belong here.
    #[test]
    fn server_welcome_is_pinned() {
        pin_server(
            &ServerFrame::Welcome {
                version: 1,
                participant_id: "surface:deskbar".to_string(),
                session_id: "sess-1".to_string(),
                heartbeat_secs: 20,
                max_body_bytes: 65_536,
                max_frame_bytes: 532_480,
                alert_granted: true,
            },
            json!({
                "type": "Welcome",
                "version": 1,
                "participant_id": "surface:deskbar",
                "session_id": "sess-1",
                "heartbeat_secs": 20,
                "max_body_bytes": 65_536,
                "max_frame_bytes": 532_480,
                "alert_granted": true,
            }),
        );
    }

    #[test]
    fn server_heartbeat_is_pinned() {
        pin_server(&ServerFrame::Heartbeat, json!({"type": "Heartbeat"}));
    }

    #[test]
    fn server_subscribe_result_is_pinned() {
        pin_server(
            &ServerFrame::SubscribeResult {
                channel: "ephemeral:demo".to_string(),
                outcome: SubscribeOutcome::Ok,
                replay_count: 3,
                gap: Some(GapInfo {
                    reason: GapReason::BeyondRetained,
                }),
            },
            json!({
                "type": "SubscribeResult",
                "channel": "ephemeral:demo",
                "outcome": {"kind": "Ok"},
                "replay_count": 3,
                "gap": {"reason": {"kind": "BeyondRetained"}},
            }),
        );
        pin_server(
            &ServerFrame::SubscribeResult {
                channel: "ephemeral:demo".to_string(),
                outcome: SubscribeOutcome::Ok,
                replay_count: 0,
                gap: None,
            },
            json!({
                "type": "SubscribeResult",
                "channel": "ephemeral:demo",
                "outcome": {"kind": "Ok"},
                "replay_count": 0,
            }),
        );
        pin_server(
            &ServerFrame::SubscribeResult {
                channel: "brenn:chat.app.home.out.42".to_string(),
                outcome: SubscribeOutcome::Unavailable,
                replay_count: 0,
                gap: None,
            },
            json!({
                "type": "SubscribeResult",
                "channel": "brenn:chat.app.home.out.42",
                "outcome": {"kind": "Unavailable"},
                "replay_count": 0,
            }),
        );
    }

    /// Per-channel, single-target: the position facts sit on each row, not on a
    /// list of targets, because the wire carries a message once per attachment
    /// and fan-out is the attacher's own business.
    #[test]
    fn server_deliver_is_pinned() {
        pin_server(
            &ServerFrame::Deliver {
                channel: "ephemeral:demo".to_string(),
                rows: vec![DeliverRow {
                    envelope: sample_envelope(),
                    seq: 12,
                    cursor: sample_cursor(),
                    dropped: 2,
                }],
            },
            json!({
                "type": "Deliver",
                "channel": "ephemeral:demo",
                "rows": [{
                    "envelope": {
                        "message_id": "00000000-0000-0000-0000-000000000001",
                        "source": "src",
                        "channel": "ephemeral:demo",
                        "sender": "surface:deskbar",
                        "publish_ts": "2023-11-14T22:13:20Z",
                        "body": "hello",
                        "urgency": "normal",
                        "envelope_type": "ephemeral",
                    },
                    "seq": 12,
                    "cursor": "opaque-token-7",
                    "dropped": 2,
                }],
            }),
        );
    }

    /// A whole catch-up pass in one frame: the rows are oldest first, their
    /// `seq`s ascend across the pass, and the loss rides only the row that
    /// follows it.
    #[test]
    fn a_multi_row_deliver_pass_is_pinned() {
        pin_server(
            &ServerFrame::Deliver {
                channel: "ephemeral:demo".to_string(),
                rows: vec![
                    DeliverRow {
                        envelope: sample_envelope(),
                        seq: 1,
                        cursor: sample_cursor(),
                        dropped: 3,
                    },
                    DeliverRow {
                        envelope: sample_envelope(),
                        seq: 2,
                        cursor: sample_cursor(),
                        dropped: 0,
                    },
                ],
            },
            json!({
                "type": "Deliver",
                "channel": "ephemeral:demo",
                "rows": [
                    {
                        "envelope": {
                            "message_id": "00000000-0000-0000-0000-000000000001",
                            "source": "src",
                            "channel": "ephemeral:demo",
                            "sender": "surface:deskbar",
                            "publish_ts": "2023-11-14T22:13:20Z",
                            "body": "hello",
                            "urgency": "normal",
                            "envelope_type": "ephemeral",
                        },
                        "seq": 1,
                        "cursor": "opaque-token-7",
                        "dropped": 3,
                    },
                    {
                        "envelope": {
                            "message_id": "00000000-0000-0000-0000-000000000001",
                            "source": "src",
                            "channel": "ephemeral:demo",
                            "sender": "surface:deskbar",
                            "publish_ts": "2023-11-14T22:13:20Z",
                            "body": "hello",
                            "urgency": "normal",
                            "envelope_type": "ephemeral",
                        },
                        "seq": 2,
                        "cursor": "opaque-token-7",
                        "dropped": 0,
                    },
                ],
            }),
        );
    }

    /// A row is as strict as a frame: an unknown key inside one is a protocol
    /// violation, not something to skip past.
    #[test]
    fn an_unknown_key_in_a_deliver_row_is_refused() {
        let json = json!({
            "type": "Deliver",
            "channel": "ephemeral:demo",
            "rows": [{
                "envelope": {
                    "message_id": "00000000-0000-0000-0000-000000000001",
                    "source": "src",
                    "channel": "ephemeral:demo",
                    "sender": "surface:deskbar",
                    "publish_ts": "2023-11-14T22:13:20Z",
                    "body": "hello",
                    "urgency": "normal",
                    "envelope_type": "ephemeral",
                },
                "seq": 12,
                "cursor": "opaque-token-7",
                "dropped": 0,
                "targets": [],
            }],
        });
        assert!(serde_json::from_value::<ServerFrame>(json).is_err());
    }

    #[test]
    fn server_publish_result_is_pinned() {
        pin_server(
            &ServerFrame::PublishResult {
                correlation: Some(9),
                outcome: PublishOutcome::Ok,
            },
            json!({
                "type": "PublishResult",
                "correlation": 9,
                "outcome": {"kind": "Ok"},
            }),
        );
        pin_server(
            &ServerFrame::PublishResult {
                correlation: None,
                outcome: PublishOutcome::BodyTooLarge { len: 100, max: 64 },
            },
            json!({
                "type": "PublishResult",
                "outcome": {"kind": "BodyTooLarge", "len": 100, "max": 64},
            }),
        );
    }

    #[test]
    fn server_publish_batch_result_is_pinned() {
        pin_server(
            &ServerFrame::PublishBatchResult {
                correlation: 4,
                outcome: PublishBatchOutcome::RateLimited,
            },
            json!({
                "type": "PublishBatchResult",
                "correlation": 4,
                "outcome": {"kind": "RateLimited"},
            }),
        );
    }

    #[test]
    fn server_deferred_view_is_pinned() {
        pin_server(
            &ServerFrame::DeferredView {
                channel: "brenn:orders".to_string(),
                attribution: Some("ticker".to_string()),
                entries: vec![DeferredViewEntry {
                    message_id: Uuid::parse_str("00000000-0000-0000-0000-00000000000a").unwrap(),
                    body: "parked".to_string(),
                    deliver_after: 1_700_000_000_000,
                }],
            },
            json!({
                "type": "DeferredView",
                "channel": "brenn:orders",
                "attribution": "ticker",
                "entries": [{
                    "message_id": "00000000-0000-0000-0000-00000000000a",
                    "body": "parked",
                    "deliver_after": 1_700_000_000_000u64,
                }],
            }),
        );
    }

    /// An empty view is the "nothing parked" snapshot, and it is a frame the
    /// server does send — a set that emptied must be told, or the client's mirror
    /// keeps showing a schedule that is gone.
    #[test]
    fn server_deferred_view_carries_an_empty_set() {
        pin_server(
            &ServerFrame::DeferredView {
                channel: "brenn:orders".to_string(),
                attribution: None,
                entries: vec![],
            },
            json!({
                "type": "DeferredView",
                "channel": "brenn:orders",
                "entries": [],
            }),
        );
    }

    /// An unknown field inside a known variant does not parse. Negotiation has
    /// already settled which schema is in force, so an extra field is a
    /// non-conforming peer, not a newer one.
    #[test]
    fn an_unknown_field_in_a_known_variant_is_rejected() {
        let err = serde_json::from_str::<ClientFrame>(
            r#"{"type":"Unsubscribe","channel":"ephemeral:demo","extra":1}"#,
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("unknown field"), "{err}");

        let err = serde_json::from_str::<ServerFrame>(
            r#"{"type":"SubscribeResult","channel":"ephemeral:demo",
                "outcome":{"kind":"Ok"},"replay_count":0,"instance":"ticker"}"#,
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("unknown field"), "{err}");
    }

    /// The boundary of that strictness, pinned so it is a known shape rather than
    /// a surprise: a variant with **no fields** has nothing an extra key could be
    /// a misspelling of, and serde's internally-tagged representation carries no
    /// field list to check trailing keys against. Junk beside such a tag parses.
    /// Every variant that has fields is strict, which is the whole of the
    /// misnamed-field class this crate is defending against.
    #[test]
    fn a_fieldless_variant_tolerates_trailing_keys() {
        assert_eq!(
            serde_json::from_str::<ServerFrame>(r#"{"type":"Heartbeat","tick":1}"#).unwrap(),
            ServerFrame::Heartbeat
        );
    }

    /// Strictness reaches nested payloads too, not just the frame's own fields.
    #[test]
    fn an_unknown_field_in_a_nested_payload_is_rejected() {
        let err = serde_json::from_str::<ClientFrame>(
            r#"{"type":"PublishBatch","correlation":1,"publishes":[
                 {"channel":"ephemeral:demo","body":"x","urgency":"normal","port":"out"}]}"#,
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("unknown field"), "{err}");
    }

    #[test]
    fn an_unknown_frame_type_is_rejected() {
        for raw in [
            r#"{"type":"Geometry","width":800,"height":600}"#,
            r#"{"type":"Status"}"#,
        ] {
            let err = serde_json::from_str::<ClientFrame>(raw)
                .unwrap_err()
                .to_string();
            assert!(err.contains("unknown variant"), "{raw}: {err}");
        }
        let err = serde_json::from_str::<ServerFrame>(r#"{"type":"Bindings"}"#)
            .unwrap_err()
            .to_string();
        assert!(err.contains("unknown variant"), "{err}");
    }

    #[test]
    fn an_unknown_inner_kind_is_rejected() {
        let err = serde_json::from_str::<ServerFrame>(
            r#"{"type":"PublishResult","outcome":{"kind":"Nope"}}"#,
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("unknown variant"), "{err}");
    }

    /// A missing required field is rejected, so the absent-means-default rule is
    /// confined to the fields that declare it.
    #[test]
    fn a_missing_required_field_is_rejected() {
        let err = serde_json::from_str::<ClientFrame>(
            r#"{"type":"Subscribe","channel":"ephemeral:demo","push_depth":1}"#,
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("retain_depth"), "{err}");
    }

    /// A cursor rides the wire as a bare JSON string with no interior shape the
    /// client can see. The server's encoding lives server-side and can grow with
    /// no wire change; that only holds while the transported form stays a string.
    #[test]
    fn a_cursor_is_a_transparent_string() {
        let c = sample_cursor();
        assert_eq!(serde_json::to_value(&c).unwrap(), json!("opaque-token-7"));
        assert!(matches!(serde_json::to_value(&c), Ok(Value::String(_))));
    }

    #[test]
    fn the_frame_cap_clears_the_worst_case_escaping_of_a_legal_body() {
        assert_eq!(
            max_client_frame_bytes(64 * 1024),
            64 * 1024 * 6 + MAX_BATCH_OPS * BATCH_OP_OVERHEAD_BYTES + PUBLISH_FRAME_OVERHEAD_BYTES
        );
    }

    #[test]
    #[should_panic(expected = "WS frame-cap derivation overflowed")]
    fn an_unrepresentable_body_cap_panics_rather_than_deriving_a_wrong_frame_cap() {
        max_client_frame_bytes(usize::MAX);
    }

    /// One batch entry charging `charge` bytes across its channel and body, both
    /// filled with the control character that escapes six-for-one.
    fn worst_case_entry(charge: usize) -> BatchEntry {
        let channel = "\u{1}".repeat(charge / 2);
        BatchEntry {
            channel,
            body: "\u{1}".repeat(charge - charge / 2),
            urgency: Urgency::VeryLow,
            deliver_after: Some(u64::MAX),
        }
    }

    /// One control op charging `charge` bytes, in its widest shape: an edit that
    /// rewrites both halves, so it carries a body, a release time and a uuid.
    fn worst_case_op(charge: usize) -> BatchDeferredOp {
        BatchDeferredOp {
            channel: "\u{1}".repeat(charge / 2),
            message_id: Uuid::parse_str("ffffffff-ffff-ffff-ffff-ffffffffffff").unwrap(),
            op: DeferredOpKind::Edit {
                body: Some("\u{1}".repeat(charge - charge / 2)),
                deliver_after: Some(u64::MAX),
            },
        }
    }

    /// The law and the derivation are one contract: the widest batch legality
    /// admits must serialize under the cap the peer reads with. Anything else
    /// puts a conforming attacher back where the two contracts contradicted —
    /// composing a legal flush its peer takes for tampering.
    ///
    /// The op mix is swept as well as the body cap, because the two halves of the
    /// derivation answer to different inputs. The cap varies the *charged* bytes,
    /// which the escape allowance covers; the mix varies the *fixed* bytes, which
    /// `BATCH_OP_OVERHEAD_BYTES` covers — and the two op shapes do not weigh the
    /// same there, since an edit carries a uuid, a kind tag and a release time an
    /// entry does not. All-ops is the maximizing shape, so a fixture that only
    /// ever ran the even mix would leave the overhead constant free to shrink
    /// under the widest legal batch.
    #[test]
    fn the_frame_cap_bounds_the_worst_legal_batch() {
        for max_body_bytes in [1_024_usize, 64 * 1024, 1024 * 1024] {
            let charge = max_body_bytes / MAX_BATCH_OPS;
            for entry_count in [MAX_BATCH_OPS, MAX_BATCH_OPS / 2, 0] {
                let op_count = MAX_BATCH_OPS - entry_count;
                let publishes: Vec<BatchEntry> =
                    (0..entry_count).map(|_| worst_case_entry(charge)).collect();
                let deferred_ops: Vec<BatchDeferredOp> =
                    (0..op_count).map(|_| worst_case_op(charge)).collect();
                check_batch_legality(&publishes, &deferred_ops, max_body_bytes)
                    .expect("the batch is built to the law");
                let frame = ClientFrame::PublishBatch {
                    attribution: Some("instance-that-a-document-declared".to_string()),
                    correlation: u64::MAX,
                    publishes,
                    deferred_ops,
                };
                let bytes = serde_json::to_string(&frame).unwrap().len();
                let cap = max_client_frame_bytes(max_body_bytes);
                assert!(
                    bytes <= cap,
                    "{entry_count} entries + {op_count} ops at a {max_body_bytes}-byte cap \
                     serialize to {bytes} bytes, over the {cap}-byte cap"
                );
            }
        }
    }

    /// Both clauses, at and one past their boundaries.
    #[test]
    fn batch_legality_holds_at_its_boundaries() {
        let entry = |charge| worst_case_entry(charge);
        // Charged bytes: the budget is the body cap, spent across the batch.
        assert_eq!(check_batch_legality(&[entry(64)], &[], 64), Ok(()));
        assert_eq!(
            check_batch_legality(&[entry(64), entry(1)], &[], 64),
            Err(BatchIllegal::TooManyBytes { bytes: 65, cap: 64 })
        );
        // A channel address is charged like a body: it is client-composed, so a
        // rule that ignored it would bound nothing.
        assert_eq!(
            batch_charged_bytes(
                &[BatchEntry {
                    channel: "ephemeral:x".to_string(),
                    body: "hello".to_string(),
                    urgency: Urgency::Normal,
                    deliver_after: None,
                }],
                &[]
            ),
            "ephemeral:x".len() + "hello".len()
        );
        // A cancel op charges its channel and nothing else.
        assert_eq!(
            batch_charged_bytes(
                &[],
                &[BatchDeferredOp {
                    channel: "ephemeral:x".to_string(),
                    message_id: Uuid::nil(),
                    op: DeferredOpKind::Cancel,
                }]
            ),
            "ephemeral:x".len()
        );
        // Op count: publishes and ops share one ceiling, because they share one
        // frame.
        let empty = worst_case_entry(0);
        let full: Vec<BatchEntry> = (0..MAX_BATCH_OPS).map(|_| empty.clone()).collect();
        assert_eq!(check_batch_legality(&full, &[], 1), Ok(()));
        assert_eq!(
            check_batch_legality(&full[..MAX_BATCH_OPS - 1], &[worst_case_op(0)], 1),
            Ok(())
        );
        assert_eq!(
            check_batch_legality(&full, &[worst_case_op(0)], 1),
            Err(BatchIllegal::TooManyOps {
                ops: MAX_BATCH_OPS + 1,
                cap: MAX_BATCH_OPS,
            })
        );
    }
}
