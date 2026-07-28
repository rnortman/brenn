//! Chat-over-pubsub payload vocabulary: the JSON bodies carried inside
//! [`MessageEnvelope::body`](crate::MessageEnvelope) on a conversation's chat
//! channels.
//!
//! This is an **external contract**. Out-of-tree WASM components and non-browser
//! gateways speak it, so evolution within `v = 1` is additive only: new optional
//! fields and new event types. A breaking change bumps
//! [`CHAT_PROTOCOL_VERSION`].
//!
//! Three vocabularies, one per direction:
//!
//! - [`ChatCommand`] — peer → conversation, on the durable `…in.<id>` channel.
//! - [`ChatEvent`] — conversation → peers, on the durable `…out.<id>` channel;
//!   the conversation record.
//! - [`ChatStreamEvent`] — conversation → peers, on the ephemeral
//!   `…stream.<id>` channel; loss-tolerant decoration over the record.
//!
//! Every body is a JSON object of the form `{"v": 1, "type": "<variant>", …}`.
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
}
