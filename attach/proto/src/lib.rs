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
//! it cleanly: the dependencies are `serde`, `serde_json`, `uuid`, and
//! `brenn-envelope`.
//!
//! The frames are the in-memory vocabulary, not a wire schema: every body in
//! them is a [`MessageBody`], and none of them is serializable. A frame reaches
//! the websocket only through the [`Codec`] of the version the two ends
//! negotiated; [`wire`](mod@wire) holds each version's schema and encoding and
//! the frozen `Hello` shell both ends read before any version is in force.
//!
//! **Schemas are strict.** In every version an unknown field inside a known
//! frame or payload is a protocol violation, as is an unknown `type`/`kind`
//! tag. [`negotiate`] settles which schema is in force before any other frame
//! is sent, so both ends know exactly what they are parsing and leniency could
//! only mask a bug. A frame malformed for its session's version is a violation
//! at the server and fatal at the client; nothing is ever decoded a second way.
//!
//! # The versions this build speaks
//!
//! [`SUPPORTED_VERSIONS`] holds two, and one server serves both at once: each
//! route states the range it serves, and each client the range it offers.
//!
//! - **v4.** Every frame is one JSON text message and every body a JSON
//!   string; a delivered envelope is written in the text shape, without its
//!   content type. v4 can carry only text: a typed body from a client, a
//!   non-text body in either direction, and the `TextOnlyChannel` outcomes are
//!   frames it cannot write, and its encoder panics on them. A binary message
//!   is a violation. The schema is in the private `wire::v4` module, kept
//!   frame for frame as it was before v5 existed.
//! - **v5.** `Publish`, `PublishBatch`, `Deliver` and `DeferredView` travel as
//!   binary messages — always, even with no body in them — and every other
//!   frame as JSON text with v4's schema, the two publish outcomes gaining
//!   `TextOnlyChannel` (refused as a non-text body to a text-only channel).
//!   A binary message is a big-endian `u32` header length `H`, then `H`
//!   bytes of strict UTF-8 JSON (the frame, each body replaced by a
//!   descriptor `{"len":N,"content_type":"…"}`), then the bodies' raw bytes
//!   concatenated in frame order. A delivered row's
//!   envelope is `{"header":{…},"body":{…}}`. Every body is carried, with its
//!   content type. A text message of a binary kind, a binary message of a text
//!   kind, and any disagreement between a header and the bytes after it are
//!   malformed; decoding a peer's bytes never panics. The batch law charges
//!   bodies raw, as they travel, so the frame cap is the body cap plus fixed
//!   allowances (204,800 bytes at a 64 KiB body cap, against v4's 532,480).
//!   The layout in full is the private `wire::v5` module's documentation.
//!
//! The batch law and the frame cap belong to a version: [`Codec`] states both,
//! and [`max_transport_frame_bytes`] is what a route sets as its websocket read
//! cap before any version is agreed.
//!
//! # How the protocol grows
//!
//! Any schema change bumps the transport version; no frame kind is reserved
//! and no capability list exists beside the version range. A version is one
//! module under `wire/` holding its schema and its conversions to and from the
//! frames here, plus one arm in each version-dependent [`Codec`] method.
//!
//! The one place *data* may grow without a bump is the `header` object of a v5
//! delivered envelope: it is the only non-strict object in the protocol, and a
//! reader ignores keys it does not know. That is where references to blob
//! attachments are expected to appear. v5 itself carries no blob transfer; a
//! later version is expected to add it.
//!
//! Bus semantics the frames carry, none of them re-invented here: cursors are
//! server-minted, opaque and client-held; nothing consumes and nothing acks;
//! there is no backpressure; overflow drops oldest and is reported as
//! [`DeliverRow::dropped`]; `push_depth` and `retain_depth` are the two
//! independent knobs a subscription is defined by. See `docs/message-bus.md`.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub use brenn_envelope::{BodyError, MessageBody, MessageEnvelope};

pub mod wire;

pub use wire::{
    Codec, DecodeError, HelloRead, WireMessage, encode_client_hello, encode_server_hello,
    max_transport_frame_bytes, read_client_hello, read_server_hello,
};

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

    /// Whether this range holds `version`.
    pub const fn contains(self, version: u32) -> bool {
        self.min <= version && version <= self.max
    }

    /// Whether `inner` holds at least one version and every version it holds
    /// is one this range holds: what a range offered or served must satisfy
    /// against [`SUPPORTED_VERSIONS`]. An empty `inner` is never contained.
    pub const fn contains_range(self, inner: VersionRange) -> bool {
        inner.min <= inner.max && self.min <= inner.min && inner.max <= self.max
    }
}

/// The transport versions this build speaks: 4, where every frame is JSON text
/// and every body a string, and 5, where the body-carrying frames are binary
/// and a body is bytes with its content type.
///
/// A consumer exists that deploys out of lockstep with the backend — a remote
/// daemon binding the client crate by pin — so the server holds more than one
/// version at once. Each route serves a range within this one, chosen by the
/// server, and a daemon reaches v5 only on a route whose range holds it; the
/// surface route serves the version the page speaks. No length is stated for
/// v4's service and no release that removes it is named.
///
/// Any schema change still bumps the version. The negotiation mechanism is the
/// one thing that cannot be added later without breaking the frozen
/// [`ClientFrame::Hello`] shell.
// TODO(attach-v4-beside-v5): how long v4 is served beside v5, and when it goes.
pub const SUPPORTED_VERSIONS: VersionRange = VersionRange { min: 4, max: 5 };

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

/// Whether `version`'s frames can carry a body that is not text.
///
/// True from v5, which writes a body as its bytes; false for v4, which writes
/// it as a string. A server answers a subscribe to a channel declared
/// `non_text` [`SubscribeOutcome::Unavailable`] for a session on a version
/// this is false for, so that nothing it cannot represent is ever delivered
/// to it.
///
/// # Panics
///
/// On a version outside [`SUPPORTED_VERSIONS`], as [`Codec::negotiated`].
pub fn carries_non_text_bodies(version: u32) -> bool {
    Codec::negotiated(version).carries_non_text_bodies()
}

// ---------------------------------------------------------------------------
// Client → server frames
// ---------------------------------------------------------------------------

/// Client → server frame.
#[derive(Debug, Clone, PartialEq, Eq)]
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
        attribution: Option<String>,
        /// What the negotiated version can write is [`Codec::carries`]'s
        /// answer; anything else panics at the encoder.
        body: MessageBody,
        /// Sender intent about how hard the bus should work to wake a
        /// subscriber. Always concrete on the wire: a channel's configured
        /// default is resolved by the client, which needs it anyway to stamp
        /// the envelopes it routes without a server.
        urgency: Urgency,
        /// Routes the eventual [`ServerFrame::PublishResult`] back, or `None` to
        /// ask for no answer.
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
    /// **Legality is protocol law**, stated by [`Codec::check_batch_legality`]: a
    /// bounded op count and a bounded charge of channel and body bytes, both
    /// checked by the client before it sends and by the server after it parses.
    /// The law is what makes [`Codec::max_client_frame_bytes`] able to bound every
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
        attribution: Option<String>,
        severity: AlertSeverity,
        title: String,
        body: String,
    },
}

/// One buffered publish inside a [`ClientFrame::PublishBatch`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchEntry {
    pub channel: String,
    pub body: MessageBody,
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
    pub deliver_after: Option<u64>,
}

impl BatchEntry {
    /// An ordinary publish of `body` onto `channel`. The constructor, rather
    /// than a struct literal, is what keeps building an entry source-compatible
    /// when the type gains a field.
    pub fn new(channel: impl Into<String>, body: MessageBody, urgency: Urgency) -> Self {
        Self {
            channel: channel.into(),
            body,
            urgency,
            deliver_after: None,
        }
    }

    /// The entry parked until `deliver_after`, epoch milliseconds UTC.
    pub fn deliver_after(mut self, deliver_after: u64) -> Self {
        self.deliver_after = Some(deliver_after);
        self
    }
}

/// One buffered control op inside a [`ClientFrame::PublishBatch`]: what an
/// activation did to a message it had already parked.
///
/// The server is the parked set's authority — it holds the retention, and a
/// durable schedule must outlive the attacher — so the client cannot apply the
/// op itself. It states which message and what to do, and the server applies it
/// under the same sub-identity the batch's publishes use.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchDeferredOp {
    pub channel: String,
    /// The parked message's identity, taken from the
    /// [`ServerFrame::DeferredView`] the client holds, so the id is one the
    /// server minted.
    ///
    /// An id parked by a *different* sender is a protocol violation, not an
    /// outcome: a conforming client can only name what a sender-scoped view
    /// showed it. A message past its release instant at flush is the benign
    /// no-op instead, which the server logs and counts — and the view shows an
    /// entry whose instant has passed, so a client that names one gets that
    /// no-op rather than an applied op. Read `deliver_after` before acting: an
    /// entry already due is not cancellable or editable.
    pub message_id: Uuid,
    pub op: DeferredOpKind,
}

/// What a [`BatchDeferredOp`] does to the message it names.
#[derive(Debug, Clone, PartialEq, Eq)]
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
        body: Option<MessageBody>,
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
#[derive(Debug, Clone, PartialEq, Eq)]
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
        /// the client honours; [`Codec::max_client_frame_bytes`] is how the server gets
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
    ///
    /// Every entry the sender parked that no release pass has taken is here,
    /// including one whose `deliver_after` has already passed, carried with
    /// that past instant. So an empty set means nothing is standing, at every
    /// instant — and an entry in the set is not necessarily actionable: a
    /// [`BatchDeferredOp`] naming a due one is a no-op. A client that treats
    /// every entry as cancellable must read `deliver_after`.
    DeferredView {
        channel: String,
        attribution: Option<String>,
        entries: Vec<DeferredViewEntry>,
    },
}

/// One delivered envelope inside a [`ServerFrame::Deliver`] pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliverRow {
    /// The delivered envelope, body and all. A version that writes bodies as
    /// strings writes a typed text body as its text, and cannot write a
    /// non-text one ([`carries_non_text_bodies`]).
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeferredViewEntry {
    pub message_id: Uuid,
    pub body: MessageBody,
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
    /// Or the channel exists and cannot be served to this protocol version: it
    /// is declared to carry bodies that are not text, and the session's
    /// version carries only text ([`carries_non_text_bodies`]).
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
    /// The body is not text and the channel is declared to carry only text.
    /// Nothing was stored. Only a version that carries non-text bodies
    /// ([`carries_non_text_bodies`]) can write this outcome, since only such a
    /// version can send such a body.
    TextOnlyChannel,
}

/// Result of a [`ClientFrame::PublishBatch`].
///
/// Two variants, deliberately not a reuse of [`PublishOutcome`]: the single
/// publish's other outcomes are violation-grade here (`BodyTooLarge` — the
/// client gates bodies at buffer time) or impossible (`Failed` — the
/// error-report backstop is not a batch path), so reusing that enum would
/// advertise arms this frame can never carry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublishBatchOutcome {
    /// Applied: durable entries committed in one transaction, ephemeral entries
    /// fanned out, all in call order.
    Ok,
    /// The sender's server-side budget refused the batch. The honest outcome when
    /// the two budget tiers disagree — never a violation and never a kill.
    RateLimited,
    /// An entry or edit holds a body that is not text, bound for a channel
    /// declared to carry only text. Nothing of the batch was applied. Only a
    /// version that carries non-text bodies can write this outcome.
    TextOnlyChannel,
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

/// How many operations one [`ClientFrame::PublishBatch`] may carry — publishes
/// and control ops together.
///
/// Wide enough to admit the widest flush an activation can buffer: the
/// per-activation caps allow `brenn_budget::MAX_PUBLISHES_PER_ACTIVATION`
/// publishes *and* the same number of control ops, so at twice that number the
/// count half of the law never refuses a flush the buffer-time budget accepted.
/// The number is spelled here rather than imported because this crate links into
/// wasm attachers on four dependencies; the server's batch handler holds the
/// tripwire that keeps the two in step.
pub const MAX_BATCH_OPS: usize = 512;

/// Per-operation allowance in the frame-cap derivation: the JSON keys, the
/// braces and commas, the type and kind tags, an urgency, a release timestamp
/// and a message uuid of one batch operation.
///
/// The two unbounded fields of an operation — its channel address and its body —
/// are deliberately not in here. They are charged against the batch's own byte
/// budget by [`Codec::batch_charged_bytes`] instead, which is what makes the
/// frame-cap derivation a bound rather than an estimate.
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

/// [`ClientFrame::Alert`] `title` cap. Client-enforced by truncation; a longer
/// title reaching the server is a violation.
pub const MAX_ALERT_TITLE_BYTES: usize = 256;

/// [`ClientFrame::Alert`] `body` cap. Client-enforced by truncation, like
/// [`MAX_ALERT_TITLE_BYTES`].
pub const MAX_ALERT_BODY_BYTES: usize = 4 * 1024;

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    fn sample_cursor() -> Cursor {
        serde_json::from_value(json!("opaque-token-7")).unwrap()
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
        assert_eq!(SUPPORTED_VERSIONS, VersionRange { min: 4, max: 5 });
    }

    #[test]
    fn only_v5_carries_non_text_bodies() {
        assert!(!carries_non_text_bodies(4));
        assert!(carries_non_text_bodies(5));
    }

    #[test]
    fn contains_table() {
        let range = VersionRange { min: 4, max: 5 };
        assert!(!range.contains(3));
        assert!(range.contains(4));
        assert!(range.contains(5));
        assert!(!range.contains(6));
        assert!(!VersionRange { min: 5, max: 4 }.contains(4));
    }

    #[test]
    fn contains_range_table() {
        let r = |min, max| VersionRange { min, max };
        for (outer, inner, expected) in [
            (r(4, 5), r(4, 5), true),
            (r(4, 5), r(4, 4), true),
            (r(4, 5), r(5, 5), true),
            (r(4, 5), r(3, 5), false),
            (r(4, 5), r(4, 6), false),
            (r(4, 5), r(6, 9), false),
            (r(4, 4), r(4, 5), false),
            // An empty range is never contained, and an empty outer range
            // holds nothing.
            (r(4, 5), r(5, 4), false),
            (r(4, 5), r(9, 1), false),
            (r(5, 4), r(4, 4), false),
            (r(5, 4), r(9, 1), false),
        ] {
            assert_eq!(
                outer.contains_range(inner),
                expected,
                "{outer:?}.contains_range({inner:?})"
            );
        }
    }

    #[test]
    fn this_build_negotiates_with_itself() {
        assert_eq!(
            negotiate(SUPPORTED_VERSIONS, SUPPORTED_VERSIONS),
            Some(SUPPORTED_VERSIONS.max)
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

    /// A cursor rides the wire as a bare JSON string with no interior shape the
    /// client can see. The server's encoding lives server-side and can grow with
    /// no wire change; that only holds while the transported form stays a string.
    #[test]
    fn a_cursor_is_a_transparent_string() {
        let c = sample_cursor();
        assert_eq!(serde_json::to_value(&c).unwrap(), json!("opaque-token-7"));
        assert!(matches!(serde_json::to_value(&c), Ok(Value::String(_))));
    }
}
