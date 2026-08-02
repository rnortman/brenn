//! Chat-over-pubsub: the channel-name grammar of a conversation's channel
//! family, and the JSON bodies carried inside
//! [`MessageEnvelope::body`](crate::MessageEnvelope) on those channels.
//!
//! This is an **external contract**. Out-of-tree WASM components and non-browser
//! gateways speak it, so evolution within `v = 1` is additive only: new optional
//! fields and new event types. A breaking change bumps
//! [`CHAT_PROTOCOL_VERSION`].
//!
//! `docs/chat-protocol.md` is the normative specification, written for a peer
//! author who reads no Rust; this module is its implementation. **A vocabulary
//! change updates both in the same commit** — the test at the bottom of this
//! file derives every wire tag from the types below and fails when one of them
//! is missing from that document.
//!
//! Addresses first: every name in the family is minted by [`chat_bare_name`] or
//! [`chat_roster_bare_name`] and nothing else, so provisioning, ACL authoring,
//! the adapter, and an out-of-process peer cannot drift on the shape. The
//! grammar lives here rather than beside the server's config types because a
//! peer that composes a conversation address needs it and must not need the
//! host.
//!
//! Then three body vocabularies, one per direction:
//!
//! - [`ChatCommand`] — peer → conversation, on the durable `…in.<id>` channel.
//! - [`ChatEvent`] — conversation → peers, on the durable `…out.<id>` channel;
//!   the conversation record.
//! - [`ChatStreamEvent`] — conversation → peers, on the ephemeral
//!   `…stream.<id>` channel; loss-tolerant decoration over the record.
//!
//! …plus [`ChatRoster`], the per-app state snapshot on the `roster` channel,
//! which is what tells a peer which conversations exist to address at all.
//!
//! Every body is a JSON object of the form `{"v": 1, "type": "<variant>", …}`
//! (the roster carries no `type` — it is one shape, not a vocabulary).
//! [`encode`] stamps the version; [`decode`] checks it.
//!
//! Consumers tolerate the future: [`ChatEvent`] and [`ChatStreamEvent`] both
//! carry an `Unknown` catch-all so an event type added later deserializes
//! instead of failing. [`ChatCommand`] deliberately has none — an unrecognized
//! command is a rejection, not a shrug.
//!
//! All text is raw text as the model produced it (markdown, code, prose). No
//! HTML crosses this wire.

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::ChannelScheme;

/// Protocol version stamped on every chat body as the `v` field.
pub const CHAT_PROTOCOL_VERSION: u32 = 1;

/// Prefix of the `sender` string on a [`ChatEvent::UserMessage`] echoing input
/// that arrived over the legacy websocket rather than the bus.
///
/// The full form is `legacy-ws:<username>`. It is deliberately *not* a
/// `ParticipantId` — a browser session is not a bus participant — so a consumer
/// that parses senders as participant ids will not mistake one for the other.
pub const LEGACY_WS_SENDER: &str = "legacy-ws:";

/// Compose the `sender` string for input that arrived over the legacy
/// websocket: `legacy-ws:<username>`.
pub fn legacy_ws_sender(username: &str) -> String {
    format!("{LEGACY_WS_SENDER}{username}")
}

/// Why a chat body could not be turned into a message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChatDecodeError {
    /// The body is not parseable JSON.
    NotJson(String),
    /// The body parsed but is not a JSON object.
    NotAnObject,
    /// No `v` field, or its value is not a non-negative integer.
    MissingVersion,
    /// The `v` field names a protocol version this build does not implement.
    UnsupportedVersion(u64),
    /// The body is a versioned JSON object but does not match the target
    /// vocabulary (unknown `type`, missing or ill-typed field).
    Payload(String),
}

impl core::fmt::Display for ChatDecodeError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::NotJson(e) => write!(f, "body is not JSON: {e}"),
            Self::NotAnObject => write!(f, "body is not a JSON object"),
            Self::MissingVersion => write!(f, "body has no integer `v` field"),
            Self::UnsupportedVersion(v) => write!(
                f,
                "body declares protocol version {v}, this build implements {CHAT_PROTOCOL_VERSION}"
            ),
            Self::Payload(e) => write!(f, "body does not match the message schema: {e}"),
        }
    }
}

impl std::error::Error for ChatDecodeError {}

/// Serialize a chat message into an envelope body, stamping
/// `"v": CHAT_PROTOCOL_VERSION`.
///
/// # Panics
///
/// Panics if `message` does not serialize to a JSON object. Every type in this
/// module does; a caller passing something else is a programming error.
pub fn encode<T: Serialize>(message: &T) -> String {
    #[derive(Serialize)]
    struct Versioned<'a, T> {
        v: u32,
        #[serde(flatten)]
        message: &'a T,
    }

    serde_json::to_string(&Versioned {
        v: CHAT_PROTOCOL_VERSION,
        message,
    })
    .expect("chat message serializes to a JSON object")
}

/// Parse an envelope body into a chat message, requiring a `v` field equal to
/// [`CHAT_PROTOCOL_VERSION`].
///
/// Unknown fields are ignored (forward compatibility). Whether an unknown
/// `type` is tolerated is a property of `T`: [`ChatEvent`] and
/// [`ChatStreamEvent`] absorb it as their `Unknown` variant, [`ChatCommand`]
/// rejects it as [`ChatDecodeError::Payload`].
pub fn decode<T: DeserializeOwned>(body: &str) -> Result<T, ChatDecodeError> {
    let value: serde_json::Value =
        serde_json::from_str(body).map_err(|e| ChatDecodeError::NotJson(e.to_string()))?;
    let object = value.as_object().ok_or(ChatDecodeError::NotAnObject)?;
    let version = object
        .get("v")
        .and_then(serde_json::Value::as_u64)
        .ok_or(ChatDecodeError::MissingVersion)?;
    if version != u64::from(CHAT_PROTOCOL_VERSION) {
        return Err(ChatDecodeError::UnsupportedVersion(version));
    }
    serde_json::from_value(value).map_err(|e| ChatDecodeError::Payload(e.to_string()))
}

// ── Channel addresses ──────────────────────────────────────────────────────

/// Segment separator in a chat channel name.
pub const CHAT_SEGMENT_SEP: char = '.';

/// Literal second segment of every chat channel name. It reserves root-level
/// room beside the per-app subtree for future siblings under the prefix, which
/// would otherwise collide with an app slug.
pub const CHAT_APP_SEGMENT: &str = "app";

/// Terminal segment of an app's roster channel, in the same position a
/// [`ChatLeaf`] occupies. It carries no conversation id after it, which is what
/// keeps it outside every fleet prefix: a fleet grant is
/// `<prefix>.app.<slug>.<leaf>.` with the trailing separator, and this name ends
/// where that prefix would continue.
const CHAT_ROSTER_SEGMENT: &str = "roster";

/// The traffic leaf of a chat channel name — the segment between the owning
/// app's slug and the conversation id.
///
/// The conversation id is the terminal segment because it is the only segment
/// minted at runtime. That ordering is what lets an exact matcher name one
/// conversation and a segment-boundary prefix name every conversation of an
/// app, per leaf, with no wildcard matcher: a grant may cover "may drive every
/// conversation of this app" without also covering "may forge its record".
///
/// The cost is that a conversation owns no single subtree of its own — its
/// channels are siblings under the per-leaf subtrees.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChatLeaf {
    /// Commands from peers to the conversation.
    In,
    /// The conversation record: messages, status, errors, telemetry.
    Out,
    /// Token batches.
    Stream,
    /// Pre-warm signal: bodies are ignored, the message's existence is the
    /// signal.
    Wake,
    /// Tool-call permission flow. **Reserved, unbuilt** — the name is fixed so
    /// the grammar cannot collide once the flow exists, and so "may chat" and
    /// "may approve tool calls" are separately grantable today.
    Approvals,
}

impl ChatLeaf {
    /// Every leaf. The single source enumerating callers use, so a new leaf
    /// cannot be added and silently skipped by a hand-listed set.
    pub const ALL: [ChatLeaf; 5] = [
        Self::In,
        Self::Out,
        Self::Stream,
        Self::Wake,
        Self::Approvals,
    ];

    /// The leaf's name segment.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::In => "in",
            Self::Out => "out",
            Self::Stream => "stream",
            Self::Wake => "wake",
            Self::Approvals => "approvals",
        }
    }

    /// The address scheme this leaf's traffic rides. Durable where the content
    /// is the record and must survive restart; ephemeral where loss is
    /// preferable to a persistence round trip.
    pub fn scheme(self) -> ChannelScheme {
        match self {
            Self::In | Self::Out | Self::Approvals => ChannelScheme::Brenn,
            Self::Stream | Self::Wake => ChannelScheme::Ephemeral,
        }
    }
}

/// `<prefix>.app.<app_slug>.<leaf>.<conversation_id>` — the sole derivation of a
/// chat channel name.
pub fn chat_bare_name(
    prefix: &str,
    app_slug: &str,
    leaf: ChatLeaf,
    conversation_id: i64,
) -> String {
    let leaf = leaf.as_str();
    format!(
        "{prefix}{CHAT_SEGMENT_SEP}{CHAT_APP_SEGMENT}{CHAT_SEGMENT_SEP}{app_slug}{CHAT_SEGMENT_SEP}{leaf}{CHAT_SEGMENT_SEP}{conversation_id}"
    )
}

/// [`chat_bare_name`] with its leaf's address scheme, e.g.
/// `brenn:chat.app.<slug>.in.<id>`.
pub fn chat_address(prefix: &str, app_slug: &str, leaf: ChatLeaf, conversation_id: i64) -> String {
    format!(
        "{}{}",
        leaf.scheme().prefix(),
        chat_bare_name(prefix, app_slug, leaf, conversation_id)
    )
}

/// `<prefix>.app.<app_slug>.` — the segment-boundary prefix covering every chat
/// channel of one app, across leaves and conversations.
pub fn chat_app_prefix(prefix: &str, app_slug: &str) -> String {
    format!(
        "{prefix}{CHAT_SEGMENT_SEP}{CHAT_APP_SEGMENT}{CHAT_SEGMENT_SEP}{app_slug}{CHAT_SEGMENT_SEP}"
    )
}

/// `<prefix>.app.<app_slug>.<leaf>.` — the segment-boundary prefix covering one
/// leaf of every conversation of one app. The fleet-grain grant: it reaches
/// every conversation on that leaf and no other leaf.
pub fn chat_leaf_prefix(prefix: &str, app_slug: &str, leaf: ChatLeaf) -> String {
    let leaf = leaf.as_str();
    format!(
        "{prefix}{CHAT_SEGMENT_SEP}{CHAT_APP_SEGMENT}{CHAT_SEGMENT_SEP}{app_slug}{CHAT_SEGMENT_SEP}{leaf}{CHAT_SEGMENT_SEP}"
    )
}

/// `<prefix>.app.<app_slug>.roster` — the app's conversation roster.
///
/// One per app, not per conversation: the roster is what a peer reads to learn
/// which conversation ids exist, so it cannot be addressed by one.
pub fn chat_roster_bare_name(prefix: &str, app_slug: &str) -> String {
    format!(
        "{prefix}{CHAT_SEGMENT_SEP}{CHAT_APP_SEGMENT}{CHAT_SEGMENT_SEP}{app_slug}{CHAT_SEGMENT_SEP}{CHAT_ROSTER_SEGMENT}"
    )
}

/// [`chat_roster_bare_name`] as a `brenn:` address. The roster is durable: it is
/// the state a peer reconciles against after any outage, so it has to survive
/// one.
pub fn chat_roster_address(prefix: &str, app_slug: &str) -> String {
    format!(
        "{}{}",
        ChannelScheme::Brenn.prefix(),
        chat_roster_bare_name(prefix, app_slug)
    )
}

/// The body of an app's roster channel: every conversation of that app that
/// exists right now.
///
/// **State, not facts.** Each snapshot subsumes the one before it, so a peer
/// that was away reconciles against the newest rather than replaying what it
/// missed, and the channel retains a shallow window. A conversation is
/// addressable from the moment it appears here; one that has vanished is gone,
/// and a subscribe to its leaves answers "unavailable" rather than failing.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ChatRoster {
    /// The app's conversations, ascending by id — an order that is a function of
    /// the set alone, so identical state serializes to identical bytes.
    pub conversations: Vec<RosterConversation>,
}

/// One conversation in a [`ChatRoster`]. A struct rather than a bare id so
/// metadata (title, status, last activity) is an additive field later.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RosterConversation {
    /// The conversation id that terminates each of its channel addresses.
    pub id: i64,
}

/// Reference to a file already uploaded out of band, naming it for a
/// [`ChatCommand::Send`].
///
/// The upload id is acquired via HTTP; only the reference travels on the bus.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AttachmentRef {
    /// UUID minted by the upload endpoint. Parsed as a UUID at the handling
    /// boundary, not during deserialization, so a malformed value produces a
    /// specific rejection rather than a generic parse failure.
    pub upload_id: String,
}

/// Metadata for a file attached to an accepted user message, carried on the
/// echo so a consumer can render the attachment without a lookup.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AttachmentMeta {
    pub upload_id: String,
    pub filename: String,
    pub media_type: String,
    pub size: u64,
}

/// A model the conversation may be switched to, as reported by the harness.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ModelInfo {
    /// The alias to pass in [`ChatCommand::SetModel`] or
    /// [`ChatCommand::Send::model`] (e.g. `"default"`, `"sonnet"`).
    pub value: String,
    /// Human-readable name.
    pub display_name: String,
    /// Short description.
    pub description: String,
}

/// Live state of the conversation's harness.
///
/// Distinct from the conversation's permanent record status: this is the
/// transient state of the session, not of the stored conversation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CcState {
    /// Nothing running; ready for input.
    Idle,
    /// The subprocess is spawning but not yet ready.
    Connecting,
    /// The model is processing.
    Thinking,
    /// Blocked on a tool-use approval.
    AwaitingApproval,
    /// Context compaction in progress.
    Compacting,
    /// Something went wrong.
    Error,
}

/// Origin tag on a [`ChatEvent::SystemMessage`]. Consumers use it to decide
/// prominence; the text is the payload either way.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SystemMessageCategory {
    /// Intra-Brenn messages delivered into the conversation as context.
    MessagesReceived,
    /// Event-queue drain.
    EventDrain,
    /// Soft nudge that context is filling.
    CompactionReminder,
    /// Hard compaction trigger.
    CompactionHardTrigger,
    /// Idle-timer compaction prompt.
    CompactionIdlePrompt,
    /// Idle hooks fired.
    IdleHook,
    /// Compaction the user asked for.
    CompactionUserRequest,
    /// A UI tool error reported back to the model.
    UiError,
    /// Reminder that the device slug is unassigned.
    DeviceSlugReminder,
    /// A knowledge-base subprocess query failed.
    GrafError,
    /// A compaction attempt failed.
    CompactionFailed,
    /// A user-triggered viewport/layout snapshot.
    DebugSnapshot,
}

/// Which stream a token batch belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TokenKind {
    /// Visible assistant text.
    Text,
    /// Reasoning content.
    Thinking,
}

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

/// A peer's request to a conversation, published on its durable `in` channel.
///
/// Attribution is the envelope's `sender`, never a payload field: peers cannot
/// forge each other. `correlation` is a peer-chosen opaque string echoed back on
/// the [`ChatEvent`]s the command produces; peers that do not care omit it. One
/// event carries it per outcome — [`ChatEvent::UserMessage`] for `send`,
/// [`ChatEvent::ModelChanged`] for `set_model`, [`ChatEvent::Ack`] for `stop`
/// and `compact`, [`ChatEvent::Error`] for a refusal or a failure. Most commands
/// have exactly one outcome; a `send` naming a `model` has two, and reports each.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ChatCommand {
    /// Submit user text. Handed to the harness, which injects it at the end of
    /// the current tool-use round, or immediately when idle — there is no
    /// busy-gate and no separate "steer" verb, because injection timing is the
    /// only semantics either could have.
    Send {
        text: String,
        /// Sticky model alias, applied to this message and onward. An alias the
        /// server does not know is rejected with [`ChatEvent::Error`].
        #[serde(default, skip_serializing_if = "Option::is_none")]
        model: Option<String>,
        /// Files to attach.
        ///
        /// TODO(chat-bus-attachments): upload ids resolve through a per-user
        /// registry and a bus sender has no user mapping, so a bus `send`
        /// naming attachments is rejected whole.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        attachments: Vec<AttachmentRef>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        correlation: Option<String>,
    },
    /// Interrupt generation gracefully: the harness finishes with a result
    /// rather than being killed. Idempotent — stopping an idle conversation is
    /// an acknowledged no-op.
    Stop {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        correlation: Option<String>,
    },
    /// Change the sticky model without sending text. Same alias validation as
    /// [`ChatCommand::Send::model`].
    SetModel {
        model: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        correlation: Option<String>,
    },
    /// Ask the harness to compact its context.
    Compact {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        correlation: Option<String>,
    },
}

// ---------------------------------------------------------------------------
// Events
// ---------------------------------------------------------------------------

/// The conversation record, published on its durable `out` channel.
///
/// History is the channel's retained window — ordering and sequence come from
/// the envelope, never from these payloads.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ChatEvent {
    /// An accepted [`ChatCommand::Send`], echoed exactly once regardless of
    /// which door it arrived through.
    UserMessage {
        text: String,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        attachments: Vec<AttachmentMeta>,
        /// Bus-originated input carries the command envelope's `sender`;
        /// legacy-websocket input carries [`legacy_ws_sender`]'s form.
        sender: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        correlation: Option<String>,
    },
    /// A completed assistant message. Authoritative over anything seen on the
    /// stream channel.
    AssistantMessage {
        text: String,
        /// Server-minted opaque id shared with this message's
        /// [`ChatStreamEvent::Tokens`] batches.
        turn: String,
    },
    /// A Brenn-generated message in the conversation thread.
    SystemMessage {
        text: String,
        category: SystemMessageCategory,
    },
    /// The harness changed state.
    Status { state: CcState },
    /// A command that was rejected or did not take (with the command's
    /// `correlation`) or a conversation-level failure (without one).
    Error {
        message: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        correlation: Option<String>,
    },
    /// A command that has no other outcome event to carry its correlation —
    /// `stop` and `compact`. An acknowledged no-op (stopping an idle
    /// conversation) is the same ack as an interrupt that reached the harness:
    /// either way the verb is done.
    Ack {
        /// The command verb acknowledged, as it appears in the command's
        /// `type` field.
        command: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        correlation: Option<String>,
    },
    /// The effective sticky model changed.
    ModelChanged {
        model: String,
        /// The `correlation` of the command that changed it, when it came from
        /// one.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        correlation: Option<String>,
    },
    /// The model list the conversation accepts. Published on adapter start and
    /// whenever the list changes; the retained window makes it visible to a
    /// subscriber that arrives later.
    Models { available: Vec<ModelInfo> },
    /// A completed tool use, summarized as raw text.
    ToolUse { tool_name: String, summary: String },
    /// Context-window telemetry, emitted after each internal context check.
    ContextUsage {
        /// Fraction of the context window in use, 0-100.
        usage_pct: u8,
        current_tokens: u64,
        max_tokens: u64,
        /// Percentage at which the warning stage fires.
        reminder_pct: u8,
        /// Percentage at which the danger stage fires.
        red_pct: u8,
        /// Absolute token count at which the warning stage fires, when
        /// configured; overrides nothing, fires in addition to `reminder_pct`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reminder_tokens: Option<u64>,
        /// Absolute token count at which the danger stage fires, when
        /// configured.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        red_tokens: Option<u64>,
    },
    /// Cost telemetry, emitted after each turn completes.
    CostUsage {
        /// Cost of the turn just finished.
        last_turn_usd: f64,
        /// Cumulative session cost since the last compaction.
        since_last_compaction_usd: f64,
        /// Sum across every conversation this server ran in the last 24 wall
        /// hours.
        last_24h_usd: f64,
    },
    /// An event type this build does not implement. Produced by [`decode`];
    /// never published.
    #[serde(other)]
    Unknown,
}

/// Token traffic, published on the conversation's ephemeral `stream` channel.
///
/// Loss is expected and unrecovered: a consumer that its position bookkeeping
/// tells it dropped messages discards the partial for that `turn` and waits for
/// the durable [`ChatEvent::AssistantMessage`]. There is no retransmit — durable
/// is truth, the stream is decoration.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ChatStreamEvent {
    /// A batch of tokens.
    Tokens {
        text: String,
        kind: TokenKind,
        /// Matches the `turn` on the [`ChatEvent::AssistantMessage`] this batch
        /// is building toward.
        turn: String,
    },
    /// An event type this build does not implement. Produced by [`decode`];
    /// never published.
    #[serde(other)]
    Unknown,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip_command(command: ChatCommand) {
        let body = encode(&command);
        let parsed: ChatCommand = decode(&body).expect("encoded command decodes");
        assert_eq!(parsed, command);
    }

    fn round_trip_event(event: ChatEvent) {
        let body = encode(&event);
        let parsed: ChatEvent = decode(&body).expect("encoded event decodes");
        assert_eq!(parsed, event);
    }

    #[test]
    fn names_match_the_pinned_grammar() {
        assert_eq!(
            chat_bare_name("chat", "alice", ChatLeaf::In, 42),
            "chat.app.alice.in.42"
        );
        assert_eq!(
            chat_bare_name("chat", "alice", ChatLeaf::Out, 42),
            "chat.app.alice.out.42"
        );
        assert_eq!(
            chat_bare_name("chat", "alice", ChatLeaf::Stream, 42),
            "chat.app.alice.stream.42"
        );
        assert_eq!(
            chat_bare_name("chat", "alice", ChatLeaf::Wake, 42),
            "chat.app.alice.wake.42"
        );
        assert_eq!(
            chat_bare_name("chat", "alice", ChatLeaf::Approvals, 42),
            "chat.app.alice.approvals.42"
        );
        assert_eq!(
            chat_bare_name("talk", "alice", ChatLeaf::In, 42),
            "talk.app.alice.in.42",
            "the configured prefix roots the tree"
        );
    }

    #[test]
    fn addresses_carry_their_leafs_scheme() {
        assert_eq!(
            chat_address("chat", "alice", ChatLeaf::In, 42),
            "brenn:chat.app.alice.in.42"
        );
        assert_eq!(
            chat_address("chat", "alice", ChatLeaf::Out, 42),
            "brenn:chat.app.alice.out.42"
        );
        assert_eq!(
            chat_address("chat", "alice", ChatLeaf::Approvals, 42),
            "brenn:chat.app.alice.approvals.42"
        );
        assert_eq!(
            chat_address("chat", "alice", ChatLeaf::Stream, 42),
            "ephemeral:chat.app.alice.stream.42"
        );
        assert_eq!(
            chat_address("chat", "alice", ChatLeaf::Wake, 42),
            "ephemeral:chat.app.alice.wake.42"
        );
    }

    #[test]
    fn the_roster_is_a_durable_per_app_address() {
        assert_eq!(
            chat_roster_bare_name("chat", "alice"),
            "chat.app.alice.roster"
        );
        assert_eq!(
            chat_roster_address("chat", "alice"),
            "brenn:chat.app.alice.roster"
        );
        assert_eq!(
            chat_roster_bare_name("talk", "alice"),
            "talk.app.alice.roster"
        );
    }

    /// The roster sits in the leaf position, so a leaf that ever spelled itself
    /// `roster` would collide with it — and the collision would be silent, two
    /// derivations answering one address.
    #[test]
    fn no_leaf_is_named_roster() {
        for leaf in ChatLeaf::ALL {
            assert_ne!(leaf.as_str(), CHAT_ROSTER_SEGMENT);
            assert_ne!(
                chat_bare_name("chat", "alice", leaf, 42),
                chat_roster_bare_name("chat", "alice"),
            );
        }
    }

    /// A fleet grant is a prefix that continues past the leaf into the
    /// conversation id; the roster ends at the leaf position, so no fleet grant
    /// reaches it. Nothing but the app-wide prefix does.
    #[test]
    fn no_fleet_prefix_reaches_the_roster() {
        let roster = chat_roster_bare_name("chat", "alice");
        for leaf in ChatLeaf::ALL {
            let fleet = chat_leaf_prefix("chat", "alice", leaf);
            assert!(
                !roster.starts_with(&fleet),
                "{fleet} must not cover {roster}"
            );
        }
        assert!(roster.starts_with(&chat_app_prefix("chat", "alice")));
    }

    #[test]
    fn the_roster_body_is_versioned_and_ordered() {
        let roster = ChatRoster {
            conversations: vec![RosterConversation { id: 7 }, RosterConversation { id: 42 }],
        };
        assert_eq!(
            encode(&roster),
            r#"{"v":1,"conversations":[{"id":7},{"id":42}]}"#
        );
        let parsed: ChatRoster = decode(&encode(&roster)).expect("roster decodes");
        assert_eq!(parsed, roster);

        let empty = ChatRoster {
            conversations: Vec::new(),
        };
        assert_eq!(encode(&empty), r#"{"v":1,"conversations":[]}"#);
    }

    #[test]
    fn every_command_round_trips() {
        round_trip_command(ChatCommand::Send {
            text: "hello".to_string(),
            model: None,
            attachments: Vec::new(),
            correlation: None,
        });
        round_trip_command(ChatCommand::Send {
            text: "look at this".to_string(),
            model: Some("sonnet".to_string()),
            attachments: vec![AttachmentRef {
                upload_id: "b0a1c2d3-0000-4000-8000-000000000001".to_string(),
            }],
            correlation: Some("alice-42".to_string()),
        });
        round_trip_command(ChatCommand::Stop { correlation: None });
        round_trip_command(ChatCommand::Stop {
            correlation: Some("alice-43".to_string()),
        });
        round_trip_command(ChatCommand::SetModel {
            model: "opus".to_string(),
            correlation: None,
        });
        round_trip_command(ChatCommand::Compact {
            correlation: Some("alice-44".to_string()),
        });
    }

    #[test]
    fn every_event_round_trips() {
        round_trip_event(ChatEvent::UserMessage {
            text: "hello".to_string(),
            attachments: vec![AttachmentMeta {
                upload_id: "b0a1c2d3-0000-4000-8000-000000000002".to_string(),
                filename: "notes.md".to_string(),
                media_type: "text/markdown".to_string(),
                size: 17,
            }],
            sender: legacy_ws_sender("alice"),
            correlation: Some("alice-42".to_string()),
        });
        round_trip_event(ChatEvent::AssistantMessage {
            text: "# heading\n\nbody".to_string(),
            turn: "turn-1".to_string(),
        });
        round_trip_event(ChatEvent::SystemMessage {
            text: "compaction finished".to_string(),
            category: SystemMessageCategory::CompactionUserRequest,
        });
        round_trip_event(ChatEvent::Status {
            state: CcState::Thinking,
        });
        round_trip_event(ChatEvent::Error {
            message: "unknown model alias".to_string(),
            correlation: Some("alice-42".to_string()),
        });
        round_trip_event(ChatEvent::Ack {
            command: "stop".to_string(),
            correlation: Some("alice-42".to_string()),
        });
        round_trip_event(ChatEvent::ModelChanged {
            model: "sonnet".to_string(),
            correlation: Some("alice-43".to_string()),
        });
        round_trip_event(ChatEvent::Models {
            available: vec![ModelInfo {
                value: "sonnet".to_string(),
                display_name: "Sonnet".to_string(),
                description: "Best for everyday tasks".to_string(),
            }],
        });
        round_trip_event(ChatEvent::ToolUse {
            tool_name: "Read".to_string(),
            summary: "read 3 files".to_string(),
        });
        round_trip_event(ChatEvent::ContextUsage {
            usage_pct: 42,
            current_tokens: 84_000,
            max_tokens: 200_000,
            reminder_pct: 70,
            red_pct: 90,
            reminder_tokens: Some(150_000),
            red_tokens: None,
        });
        round_trip_event(ChatEvent::CostUsage {
            last_turn_usd: 0.031,
            since_last_compaction_usd: 1.25,
            last_24h_usd: 9.5,
        });
    }

    #[test]
    fn every_state_and_category_round_trips() {
        for state in [
            CcState::Idle,
            CcState::Connecting,
            CcState::Thinking,
            CcState::AwaitingApproval,
            CcState::Compacting,
            CcState::Error,
        ] {
            round_trip_event(ChatEvent::Status { state });
        }
        for category in [
            SystemMessageCategory::MessagesReceived,
            SystemMessageCategory::EventDrain,
            SystemMessageCategory::CompactionReminder,
            SystemMessageCategory::CompactionHardTrigger,
            SystemMessageCategory::CompactionIdlePrompt,
            SystemMessageCategory::IdleHook,
            SystemMessageCategory::CompactionUserRequest,
            SystemMessageCategory::UiError,
            SystemMessageCategory::DeviceSlugReminder,
            SystemMessageCategory::GrafError,
            SystemMessageCategory::CompactionFailed,
            SystemMessageCategory::DebugSnapshot,
        ] {
            round_trip_event(ChatEvent::SystemMessage {
                text: "note".to_string(),
                category,
            });
        }
    }

    #[test]
    fn every_stream_event_round_trips() {
        for kind in [TokenKind::Text, TokenKind::Thinking] {
            let event = ChatStreamEvent::Tokens {
                text: "par".to_string(),
                kind,
                turn: "turn-1".to_string(),
            };
            let body = encode(&event);
            let parsed: ChatStreamEvent = decode(&body).expect("encoded stream event decodes");
            assert_eq!(parsed, event);
        }
    }

    #[test]
    fn encoded_body_carries_version_and_snake_case_tag() {
        let body = encode(&ChatCommand::SetModel {
            model: "sonnet".to_string(),
            correlation: None,
        });
        let value: serde_json::Value = serde_json::from_str(&body).expect("body is JSON");
        assert_eq!(value["v"], serde_json::json!(CHAT_PROTOCOL_VERSION));
        assert_eq!(value["type"], serde_json::json!("set_model"));
        assert_eq!(value["model"], serde_json::json!("sonnet"));
        // Absent optionals are omitted rather than serialized as null, so an
        // additive future field cannot be confused with a present-but-empty one.
        assert!(value.get("correlation").is_none());
    }

    #[test]
    fn consumers_tolerate_unknown_event_types_and_fields() {
        let body = r#"{"v":1,"type":"telepathy","strength":11}"#;
        let parsed: ChatEvent = decode(body).expect("unknown event type is absorbed");
        assert_eq!(parsed, ChatEvent::Unknown);

        let body = r#"{"v":1,"type":"tokens","text":"par","kind":"text","turn":"t1","hue":"blue"}"#;
        let parsed: ChatStreamEvent = decode(body).expect("unknown stream event type is absorbed");
        assert_eq!(
            parsed,
            ChatStreamEvent::Tokens {
                text: "par".to_string(),
                kind: TokenKind::Text,
                turn: "t1".to_string(),
            }
        );

        let body = r#"{"v":1,"type":"model_changed","model":"sonnet","reason":"user"}"#;
        let parsed: ChatEvent = decode(body).expect("unknown field is ignored");
        assert_eq!(
            parsed,
            ChatEvent::ModelChanged {
                model: "sonnet".to_string(),
                correlation: None,
            }
        );
    }

    #[test]
    fn unknown_command_type_is_rejected() {
        let body = r#"{"v":1,"type":"telepathy","strength":11}"#;
        let err = decode::<ChatCommand>(body).expect_err("unknown command type is a rejection");
        assert!(matches!(err, ChatDecodeError::Payload(_)), "got {err:?}");
    }

    #[test]
    fn commands_tolerate_unknown_fields_and_absent_optionals() {
        let body = r#"{"v":1,"type":"send","text":"hi","priority":"urgent"}"#;
        let parsed: ChatCommand = decode(body).expect("unknown field is ignored");
        assert_eq!(
            parsed,
            ChatCommand::Send {
                text: "hi".to_string(),
                model: None,
                attachments: Vec::new(),
                correlation: None,
            }
        );
    }

    #[test]
    fn version_and_framing_failures_are_distinguished() {
        assert!(matches!(
            decode::<ChatCommand>("not json"),
            Err(ChatDecodeError::NotJson(_))
        ));
        assert_eq!(
            decode::<ChatCommand>("[1,2,3]"),
            Err(ChatDecodeError::NotAnObject)
        );
        assert_eq!(
            decode::<ChatCommand>(r#"{"type":"compact"}"#),
            Err(ChatDecodeError::MissingVersion)
        );
        assert_eq!(
            decode::<ChatCommand>(r#"{"v":"1","type":"compact"}"#),
            Err(ChatDecodeError::MissingVersion)
        );
        assert_eq!(
            decode::<ChatCommand>(r#"{"v":2,"type":"compact"}"#),
            Err(ChatDecodeError::UnsupportedVersion(2))
        );
        assert_eq!(
            decode::<ChatEvent>(r#"{"v":2,"type":"status","state":"idle"}"#),
            Err(ChatDecodeError::UnsupportedVersion(2)),
            "a bumped version is not absorbed by the unknown-event catch-all"
        );
    }

    #[test]
    fn missing_required_field_is_a_payload_error() {
        let err = decode::<ChatCommand>(r#"{"v":1,"type":"set_model"}"#)
            .expect_err("a required field is required");
        assert!(matches!(err, ChatDecodeError::Payload(_)), "got {err:?}");
    }

    #[test]
    fn legacy_sender_form_is_not_a_participant_id() {
        assert_eq!(legacy_ws_sender("alice"), "legacy-ws:alice");
    }

    /// Register one chat vocabulary: each published variant's pattern, its wire
    /// tag, and a sample value of that variant, in a single declaration.
    ///
    /// The generated match is exhaustive, so adding a variant to
    /// [`ChatCommand`], [`ChatEvent`] or [`ChatStreamEvent`] stops this file
    /// compiling — and the registration that fixes the compile carries the
    /// sample with it, so [`every_wire_tag`] cannot silently omit the new tag
    /// and [`the_document_describes_every_wire_tag`] keeps failing until
    /// `docs/chat-protocol.md` describes it.
    ///
    /// `never_published` variants are decode artifacts: [`decode`] produces them
    /// for bodies it does not recognize and nothing publishes them, so they are
    /// not wire tags and have nothing to document.
    macro_rules! wire_vocabulary {
        (
            $ty:ty, $tag_fn:ident, $samples_fn:ident,
            published { $( $pattern:pat => $tag:literal, sample $sample:expr ; )+ }
            never_published { $( $artifact:pat ),* $(,)? }
        ) => {
            fn $tag_fn(value: &$ty) -> &'static str {
                match value {
                    $( $pattern => $tag, )+
                    $( $artifact => panic!(concat!(
                        stringify!($ty),
                        " decode artifacts are never published tags"
                    )), )*
                }
            }

            fn $samples_fn() -> Vec<$ty> {
                vec![$( $sample ),+]
            }
        };
    }

    wire_vocabulary! {
        ChatCommand, tag_of_command, command_samples,
        published {
            ChatCommand::Send { .. } => "send", sample ChatCommand::Send {
                text: "hi".to_string(),
                model: None,
                attachments: Vec::new(),
                correlation: None,
            };
            ChatCommand::Stop { .. } => "stop", sample ChatCommand::Stop { correlation: None };
            ChatCommand::SetModel { .. } => "set_model", sample ChatCommand::SetModel {
                model: "sonnet".to_string(),
                correlation: None,
            };
            ChatCommand::Compact { .. } => "compact", sample ChatCommand::Compact {
                correlation: None,
            };
        }
        never_published {}
    }

    wire_vocabulary! {
        ChatEvent, tag_of_event, event_samples,
        published {
            ChatEvent::UserMessage { .. } => "user_message", sample ChatEvent::UserMessage {
                text: "hi".to_string(),
                attachments: Vec::new(),
                sender: legacy_ws_sender("alice"),
                correlation: None,
            };
            ChatEvent::AssistantMessage { .. } => "assistant_message",
                sample ChatEvent::AssistantMessage {
                    text: "hello".to_string(),
                    turn: "t1".to_string(),
                };
            ChatEvent::SystemMessage { .. } => "system_message", sample ChatEvent::SystemMessage {
                text: "note".to_string(),
                category: SystemMessageCategory::EventDrain,
            };
            ChatEvent::Status { .. } => "status", sample ChatEvent::Status {
                state: CcState::Idle,
            };
            ChatEvent::Error { .. } => "error", sample ChatEvent::Error {
                message: "no".to_string(),
                correlation: None,
            };
            ChatEvent::Ack { .. } => "ack", sample ChatEvent::Ack {
                command: "stop".to_string(),
                correlation: None,
            };
            ChatEvent::ModelChanged { .. } => "model_changed", sample ChatEvent::ModelChanged {
                model: "sonnet".to_string(),
                correlation: None,
            };
            ChatEvent::Models { .. } => "models", sample ChatEvent::Models {
                available: Vec::new(),
            };
            ChatEvent::ToolUse { .. } => "tool_use", sample ChatEvent::ToolUse {
                tool_name: "Read".to_string(),
                summary: "read a file".to_string(),
            };
            ChatEvent::ContextUsage { .. } => "context_usage", sample ChatEvent::ContextUsage {
                usage_pct: 1,
                current_tokens: 1,
                max_tokens: 2,
                reminder_pct: 70,
                red_pct: 90,
                reminder_tokens: None,
                red_tokens: None,
            };
            ChatEvent::CostUsage { .. } => "cost_usage", sample ChatEvent::CostUsage {
                last_turn_usd: 0.0,
                since_last_compaction_usd: 0.0,
                last_24h_usd: 0.0,
            };
        }
        never_published { ChatEvent::Unknown }
    }

    wire_vocabulary! {
        ChatStreamEvent, tag_of_stream_event, stream_event_samples,
        published {
            ChatStreamEvent::Tokens { .. } => "tokens", sample ChatStreamEvent::Tokens {
                text: "par".to_string(),
                kind: TokenKind::Text,
                turn: "t1".to_string(),
            };
        }
        never_published { ChatStreamEvent::Unknown }
    }

    /// Every tag a build can put on the wire, each cross-checked against what
    /// serde actually serializes — so the registered literals cannot drift from
    /// the wire form either.
    fn every_wire_tag() -> Vec<&'static str> {
        fn serialized_tag<T: Serialize>(value: &T) -> String {
            let body = encode(value);
            let parsed: serde_json::Value = serde_json::from_str(&body).expect("body is JSON");
            parsed["type"]
                .as_str()
                .expect("body carries a type")
                .to_string()
        }

        let mut tags = Vec::new();

        for command in &command_samples() {
            let tag = tag_of_command(command);
            assert_eq!(serialized_tag(command), tag);
            tags.push(tag);
        }
        for event in &event_samples() {
            let tag = tag_of_event(event);
            assert_eq!(serialized_tag(event), tag);
            tags.push(tag);
        }
        for event in &stream_event_samples() {
            let tag = tag_of_stream_event(event);
            assert_eq!(serialized_tag(event), tag);
            tags.push(tag);
        }

        tags
    }

    /// The normative spec lives beside the code and must describe every tag the
    /// code can emit. Crude on purpose: it turns "added a variant, forgot the
    /// doc" into a red test.
    ///
    /// The match is on the tag's own section heading, not on the tag appearing
    /// somewhere in the prose: these are ordinary words (`send`, `error`,
    /// `status`, `models`), and a check the running text can satisfy by accident
    /// is a check a new undocumented variant walks straight through.
    #[test]
    fn the_document_describes_every_wire_tag() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("docs/chat-protocol.md");
        let spec = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("{} is readable: {e}", path.display()));
        let headings: Vec<&str> = spec.lines().map(str::trim_end).collect();

        let missing: Vec<&str> = every_wire_tag()
            .into_iter()
            .filter(|tag| {
                let heading = format!("### `{tag}`");
                !headings.contains(&heading.as_str())
            })
            .collect();
        assert!(
            missing.is_empty(),
            "docs/chat-protocol.md has no `### `<tag>`` section for {missing:?} — the \
             vocabulary and its normative spec change in the same commit"
        );
    }
}
