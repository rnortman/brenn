//! Brenn message envelope wire types.
//!
//! This crate holds the types that form the external contract between the Brenn
//! host and WASM guest components. It is intentionally kept free of host-only
//! dependencies (tokio, sqlite, etc.) so that guest components may depend on it
//! directly when compiled to `wasm32-unknown-unknown`.
//!
//! The canonical definition lives here; `brenn-lib` re-exports everything from
//! this crate at the same paths so existing host callers are unaffected.
//!
//! Feature discipline: `chrono` is included with `default-features = false` and
//! only the `serde` + `alloc` features. Accidental feature creep (e.g. `clock`
//! or `wasmbind`) would fail loudly at compile time on `wasm32-unknown-unknown`.

use chrono::{DateTime, Utc};
use uuid::Uuid;

pub mod chat;

// ---------------------------------------------------------------------------
// Address scheme
// ---------------------------------------------------------------------------

// This section holds the bus **address-prefix** vocabulary (`"brenn:"`,
// `"ephemeral:"`, …). The prefixes carry a trailing colon; the bare type tags
// (`ChannelScheme::as_str`) do not. `ChannelScheme::split` (below) is the sole
// classifier of an address by its prefix; `ChannelScheme::capabilities` derives
// the two capability axes from it.

/// Transport prefix for Brenn-internal channel addresses.
///
/// Non-`brenn:` prefixes are rejected at the API boundary as unknown
/// transports; bare names without a prefix are also rejected.
pub const BRENN_ADDRESS_PREFIX: &str = "brenn:";

/// Transport prefix for webhook channel addresses.
pub const WEBHOOK_ADDRESS_PREFIX: &str = "webhook:";

/// Transport prefix for MQTT channel addresses.
pub const MQTT_ADDRESS_PREFIX: &str = "mqtt:";

/// Transport prefix for ephemeral (non-persistent) channel addresses.
pub const EPHEMERAL_ADDRESS_PREFIX: &str = "ephemeral:";

/// Transport prefix for PWA push (egress-only) target addresses.
pub const PWA_PUSH_ADDRESS_PREFIX: &str = "pwa_push:";

/// Transport prefix for page-local channel addresses.
///
/// `local:` traffic never crosses the wire: the surface kernel's router is its
/// sole source of truth. A `local:` address reaching any server ingress path is
/// a broken invariant or a protocol violation, decided per site.
pub const LOCAL_ADDRESS_PREFIX: &str = "local:";

/// Separator between a surface's slug and a component instance in the
/// per-component sub-identity `surface:<slug>#<instance>`. Outside the slug and
/// instance charsets, so the form is unambiguous to split.
pub const SURFACE_SUB_IDENTITY_SEP: char = '#';

/// Compose a surface component's sub-identity from a surface participant id and
/// a component instance: `surface:kitchen` + `agenda-alice` →
/// `surface:kitchen#agenda-alice`.
///
/// The one place the sub-identity wire syntax is minted. Two parties derive it
/// independently — the server from the instance its boot-resolved declaration
/// set admits, the page-local router from its own port wiring — and they must
/// agree: a page-stamped envelope and a server-stamped one carrying different
/// grammars would misattribute on whichever side split them, the exact failure
/// this identity grain exists to prevent. Callers are responsible for validating
/// the halves; this composes, it does not police.
pub fn surface_sub_identity(participant: &str, instance: &str) -> String {
    format!("{participant}{SURFACE_SUB_IDENTITY_SEP}{instance}")
}

// ---------------------------------------------------------------------------
// ChannelScheme — the One True Enum
// ---------------------------------------------------------------------------

/// The single classifier for a bus channel address by its transport prefix.
///
/// `split` is the only function in the codebase that inspects an address prefix
/// to decide "which scheme is this". Every multi-scheme authorization or
/// dispatch decision matches exhaustively on this enum, so a new transport
/// cannot be added without the compiler flagging each site that must handle it.
///
/// The bare type tags (`as_str`) are byte-identical to the strings stored in
/// `messaging_messages.envelope_type` / `messaging_channels.transport_type` and
/// to the serialized listing tags; the string-pinning test guards that.
///
/// # The message-class division
///
/// Pick the class by blast radius and durability need, not by convenience. The
/// address prefix *is* the contract: it tells a reader the blast radius without
/// consulting any subscriber table.
///
/// - **`brenn:` — durable state.** Persisted before delivery, server-assigned
///   seq, recoverable via the retained window. **Never push high-frequency data
///   over durable channels.**
/// - **`ephemeral:` — cross-boundary, high-frequency, loss-tolerant.** Retained
///   in an in-memory ring; server-mediated, so it crosses the wire (the
///   shape for backend producer → surface consumer traffic, e.g. token
///   streaming).
/// - **`local:` — page-local.** Never crosses the wire; the surface kernel's
///   router is the sole source of truth, so there is no echo problem, no dual
///   position assignment, and delivery works with the link down. Permanently
///   unreachable from outside the page: reaching a `local:` channel externally
///   takes an explicit bridge component (subscribe `brenn:`, republish
///   `local:`).
/// - **`mqtt:` / `webhook:` — ingress transports.** Durable-class, but not
///   surface-bindable.
/// - **`pwa_push:` — egress-only.** No capabilities: nothing on the bus ever
///   receives from it (see [`ChannelScheme::capabilities`]).
///
/// Two further classes exist that are deliberately *not* `ChannelScheme`
/// variants, named here so the whole class space is visible from one place:
///
/// - **DOM events** are invisible transport *under* ports, never a vocabulary.
///   Components see messages on named ports and nothing else; a headless
///   instance uses direct import calls for the same vocabulary.
/// - **`sync`** is request/reply — its own port class, not a delivery flavor:
///   no queue, no retention, no position, no gap/replay semantics. It shares
///   almost nothing with the schemes above beyond the vocabulary (named ports,
///   envelopes, the publish error triple), which is why it is a binding-level
///   property rather than an address scheme. Reserved, not built.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChannelScheme {
    Brenn,
    Ephemeral,
    Local,
    Mqtt,
    Webhook,
    PwaPush,
}

/// The two axes on which channels differ at all, carried on a channel's
/// directory entry and chosen once, at registration, from its address scheme.
///
/// Everything else about a channel — bounded drop-oldest retention, replay,
/// typed gaps, deferral, the noise ladder — is identical across schemes, so it
/// belongs to the substrate rather than to a per-scheme branch. These two
/// capabilities are what remains: where retained data lives, and how far a
/// message may travel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChannelCapabilities {
    /// Retained data survives a process restart or crash.
    pub durable: bool,
    /// Messages may cross the process (backend) or page (surface) boundary.
    /// A non-transportable channel is never handed to a wire serializer or an
    /// egress adapter.
    pub transportable: bool,
}

impl ChannelCapabilities {
    /// Persisted and free to cross the wire — `brenn:`, `mqtt:`, `webhook:`.
    pub const DURABLE_TRANSPORTABLE: Self = Self {
        durable: true,
        transportable: true,
    };
    /// In memory only, but free to cross the wire — `ephemeral:`.
    pub const TRANSPORTABLE: Self = Self {
        durable: false,
        transportable: true,
    };
    /// In memory only and confined to the instance that hosts it — `local:`.
    pub const CONFINED: Self = Self {
        durable: false,
        transportable: false,
    };
}

impl ChannelScheme {
    /// Every variant. The single source enumerating tests use, so a new scheme
    /// cannot be added and silently skipped by a hand-listed test case set.
    pub const ALL: [ChannelScheme; 6] = [
        Self::Brenn,
        Self::Ephemeral,
        Self::Local,
        Self::Mqtt,
        Self::Webhook,
        Self::PwaPush,
    ];

    /// Splits a channel address into its scheme and the scheme-local remainder
    /// (the text after the prefix colon). `None` for any address that carries
    /// no recognized prefix — callers deny or reject. The one place address
    /// prefixes are inspected to classify a scheme.
    pub fn split(address: &str) -> Option<(ChannelScheme, &str)> {
        if let Some(rest) = address.strip_prefix(BRENN_ADDRESS_PREFIX) {
            Some((ChannelScheme::Brenn, rest))
        } else if let Some(rest) = address.strip_prefix(EPHEMERAL_ADDRESS_PREFIX) {
            Some((ChannelScheme::Ephemeral, rest))
        } else if let Some(rest) = address.strip_prefix(LOCAL_ADDRESS_PREFIX) {
            Some((ChannelScheme::Local, rest))
        } else if let Some(rest) = address.strip_prefix(MQTT_ADDRESS_PREFIX) {
            Some((ChannelScheme::Mqtt, rest))
        } else if let Some(rest) = address.strip_prefix(WEBHOOK_ADDRESS_PREFIX) {
            Some((ChannelScheme::Webhook, rest))
        } else {
            address
                .strip_prefix(PWA_PUSH_ADDRESS_PREFIX)
                .map(|rest| (ChannelScheme::PwaPush, rest))
        }
    }

    /// The scheme `address` carries, discarding the remainder. Thin wrapper over
    /// `split`.
    pub fn of(address: &str) -> Option<ChannelScheme> {
        ChannelScheme::split(address).map(|(scheme, _)| scheme)
    }

    /// The address prefix (with trailing colon) this scheme's addresses carry.
    pub fn prefix(self) -> &'static str {
        match self {
            ChannelScheme::Brenn => BRENN_ADDRESS_PREFIX,
            ChannelScheme::Ephemeral => EPHEMERAL_ADDRESS_PREFIX,
            ChannelScheme::Local => LOCAL_ADDRESS_PREFIX,
            ChannelScheme::Mqtt => MQTT_ADDRESS_PREFIX,
            ChannelScheme::Webhook => WEBHOOK_ADDRESS_PREFIX,
            ChannelScheme::PwaPush => PWA_PUSH_ADDRESS_PREFIX,
        }
    }

    /// The bare type tag (no colon) — the DB column value and serialized
    /// listing/envelope tag. Equals the serde snake_case form (pinned by test).
    pub fn as_str(self) -> &'static str {
        match self {
            ChannelScheme::Brenn => "brenn",
            ChannelScheme::Ephemeral => "ephemeral",
            ChannelScheme::Local => "local",
            ChannelScheme::Mqtt => "mqtt",
            ChannelScheme::Webhook => "webhook",
            ChannelScheme::PwaPush => "pwa_push",
        }
    }

    /// Inverse of `as_str`. `None` on unknown tags.
    ///
    /// Named `parse` rather than `from_str` to avoid confusion with
    /// `std::str::FromStr::from_str` (clippy::should_implement_trait).
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "brenn" => Some(ChannelScheme::Brenn),
            "ephemeral" => Some(ChannelScheme::Ephemeral),
            "local" => Some(ChannelScheme::Local),
            "mqtt" => Some(ChannelScheme::Mqtt),
            "webhook" => Some(ChannelScheme::Webhook),
            "pwa_push" => Some(ChannelScheme::PwaPush),
            _ => None,
        }
    }

    /// The capability set channels of this scheme carry, or `None` for a scheme
    /// that names no pub/sub channel.
    ///
    /// `None` means egress-only, not "unclassifiable": `pwa_push:` addresses
    /// name a send target, carry no persisted row, and have no subscriber, so
    /// it holds no retention and answers neither capability question. Callers
    /// that reach this with a `pwa_push:` address are asking a question the
    /// address cannot answer.
    ///
    /// Orthogonal to surface-bindability: `mqtt:`/`webhook:` are durable and
    /// transportable but do not bind to a surface, which is the surface's own
    /// predicate to answer.
    ///
    /// Callers must use the returned capabilities rather than re-deriving them
    /// from the scheme at each site.
    pub fn capabilities(self) -> Option<ChannelCapabilities> {
        match self {
            // Ingress transports persist their rows exactly as brenn: does.
            ChannelScheme::Brenn | ChannelScheme::Mqtt | ChannelScheme::Webhook => {
                Some(ChannelCapabilities::DURABLE_TRANSPORTABLE)
            }
            ChannelScheme::Ephemeral => Some(ChannelCapabilities::TRANSPORTABLE),
            ChannelScheme::Local => Some(ChannelCapabilities::CONFINED),
            ChannelScheme::PwaPush => None,
        }
    }
}

/// The capability set the channel at `address` carries, or `None` when the
/// address carries no recognized prefix or names an egress-only target
/// (`pwa_push:`).
///
/// The address-string spelling of [`ChannelScheme::capabilities`], for the many
/// callers that hold an address rather than a scheme. Every retention, delivery
/// and publish path branches on the capabilities read from here, never on a
/// scheme name — one derivation, so a scheme addition has one call shape to
/// find.
pub fn channel_capabilities(address: &str) -> Option<ChannelCapabilities> {
    ChannelScheme::of(address)?.capabilities()
}

/// Whether `address` names a confined channel — one whose messages never leave
/// the host that holds them (`local:`).
///
/// The single spelling of "does this stay put?", shared by every host of a
/// confined realm: a page's router, a future daemon's, and the server code that
/// excludes confined bindings from every wire map. Derived from the address's
/// capabilities rather than its scheme name, because confinement is the
/// property being asked about.
///
/// A transportable scheme, an unrecognized prefix, and an egress-only target
/// all answer `false`: this is a positive identification, not a
/// classification, so it never panics and never speaks for the other classes.
pub fn is_local_channel(address: &str) -> bool {
    channel_capabilities(address).is_some_and(|caps| !caps.transportable)
}

// ---------------------------------------------------------------------------
// WebhookEnvelope
// ---------------------------------------------------------------------------

/// Transport-typed envelope for webhook ingress messages.
///
/// Stored as the `body` JSON of a `messaging_messages` row with
/// `envelope_type = 'webhook'`. The `body` field carries the raw request
/// body as opaque bytes-as-UTF-8-string; the bus never parses it.
///
/// Credential-bearing header values are masked to `"[redacted]"` at
/// envelope construction time (after signature verification) so the
/// LLM-visible JSON never carries live secrets.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WebhookEnvelope {
    /// HTTP request headers as ordered key-value pairs (header names lowercased).
    /// Credential-bearing header values are replaced with `"[redacted]"`.
    pub headers: Vec<(String, String)>,
    /// Identifier from the `VerifiedRequest` — not secret; kept verbatim.
    pub key_id: String,
    /// Client IP address rendered to string.
    pub client_ip: String,
    /// Timestamp when the request was received at the HTTP handler.
    pub received_at: DateTime<Utc>,
    /// Raw request body, opaque UTF-8 string; the bus never parses this.
    pub body: String,
    /// The originating webhook endpoint slug (`webhook:<slug>` identity).
    pub endpoint_slug: String,
}

// ---------------------------------------------------------------------------
// MqttEnvelope
// ---------------------------------------------------------------------------

/// Inner `payload` body for an inbound MQTT message stored in an
/// [`MqttEnvelope`].
///
/// `#[serde(untagged)]` because the wire format carries no `type`/`kind`
/// discriminant — the body serializes as the inner struct's fields directly
/// (matching the existing MQTT payload shape): a `Text { text }` form or a
/// `Binary { binary_placeholder, content_type }` placeholder form.
///
/// Binary payloads are represented as a placeholder only:
/// `{ binary_placeholder: true, content_type }`. The raw bytes are **not**
/// carried — they never reach the LLM/browser, preserving the prompt-injection
/// trust boundary (design §2.2).
///
/// `content_type` in the `Binary` variant has no
/// `#[serde(skip_serializing_if)]`: the key is always emitted (as `null` when
/// absent), matching the borrowed view's wire shape. Fields are declared
/// alphabetically (`binary_placeholder`, `content_type`) so untagged
/// serialization is byte-compatible with the `serde_json::Map` (sorted-key)
/// output of the borrowed view.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum MqttPayloadBody {
    Text {
        text: String,
    },
    Binary {
        // alphabetical: binary_placeholder, content_type
        binary_placeholder: bool,
        content_type: Option<String>,
    },
}

/// Transport-typed envelope for MQTT ingress messages.
///
/// Stored as the `body` JSON of a `messaging_messages` row with
/// `envelope_type = 'mqtt'`. This is the owned host↔guest contract; the
/// binary-crate router constructs it from the inbound `InboundPayload`
/// (design §2.2).
///
/// The struct is additive JSON: adding fields later leaves older stored rows
/// valid (they simply lack the field). Deferred fields (MQTT retain flag,
/// MQTT v5 user properties) are TODO candidates, not in v1.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct MqttEnvelope {
    /// The declared client this arrived on — provenance and the ACL boundary.
    /// The `<client>` segment of the channel address `mqtt:<client>:<topic>`.
    pub client_slug: String,
    /// The **actual** published topic, not the subscribed filter.
    pub topic: String,
    /// Text vs. binary-placeholder payload. Raw bytes are never carried.
    pub payload: MqttPayloadBody,
    /// Host receive timestamp.
    pub received_at: DateTime<Utc>,
    /// Delivery QoS of the inbound PUBLISH (the QoS at which the broker
    /// actually delivered this message — may differ from the QoS the bridge
    /// requested at subscribe time).
    pub qos: u8,
}

// ---------------------------------------------------------------------------
// Urgency
// ---------------------------------------------------------------------------

/// Per-message urgency intent set by the sender (design §2.1).
///
/// Ordered low-to-high: `VeryLow < Low < Normal < High`. The `derive(Ord)`
/// is load-bearing — `WakeMin::wakes` relies on `>=` comparison.
///
/// Wire/DB/TOML/LLM strings: `"very-low"`, `"low"`, `"normal"`, `"high"` —
/// identical to RFC 8030 §5.3, so `pwa_push` egress is a pass-through.
///
/// Migration mapping from the legacy binary wake flag:
/// - old `Immediate` → `Normal`
/// - old `None`      → `Low`
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "kebab-case")]
pub enum Urgency {
    VeryLow,
    Low,
    Normal,
    High,
}

impl Urgency {
    /// Every variant in ascending `rank()` order (`VeryLow`..=`High`); each
    /// variant's index equals its `rank()`. The single source downstream code
    /// uses to size per-level arrays and enumerate levels, so a level set is
    /// never re-listed (and never silently drifts) at a call site.
    pub const ALL: [Urgency; 4] = [Self::VeryLow, Self::Low, Self::Normal, Self::High];

    /// Wire/DB/TOML string representation (kebab-case; matches RFC 8030 §5.3).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::VeryLow => "very-low",
            Self::Low => "low",
            Self::Normal => "normal",
            Self::High => "high",
        }
    }

    /// Parse from a wire/DB/TOML string. Returns `None` on unknown values.
    /// Named `parse` rather than `from_str` to avoid confusion with
    /// `std::str::FromStr::from_str` (clippy::should_implement_trait).
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "very-low" => Some(Self::VeryLow),
            "low" => Some(Self::Low),
            "normal" => Some(Self::Normal),
            "high" => Some(Self::High),
            _ => None,
        }
    }

    /// Integer rank for SQL comparisons. `VeryLow=0, Low=1, Normal=2, High=3`.
    /// SQL CASE expressions that rank urgency must stay in sync with this mapping.
    pub fn rank(self) -> i64 {
        match self {
            Self::VeryLow => 0,
            Self::Low => 1,
            Self::Normal => 2,
            Self::High => 3,
        }
    }
}

// ---------------------------------------------------------------------------
// Epoch-millisecond times
// ---------------------------------------------------------------------------

/// A caller-supplied epoch-millisecond UTC time as the instant the envelope
/// family carries, or `None` for a value no `DateTime<Utc>` can represent.
///
/// The single definition of that boundary, shared by every gate that admits a
/// guest- or client-supplied time. The representable range is far narrower than
/// `u64`, and a value outside it collapses downstream into an immediate publish —
/// silently turning a schedule into a now — so the refusals on both sides of the
/// wire must agree on exactly one bound. Chrono is that bound's authority; this
/// function is where the whole system asks it.
pub fn utc_from_epoch_ms(ms: u64) -> Option<DateTime<Utc>> {
    i64::try_from(ms)
        .ok()
        .and_then(DateTime::<Utc>::from_timestamp_millis)
}

// ---------------------------------------------------------------------------
// Impetus
// ---------------------------------------------------------------------------

/// Publish-time-checked evidence that live user interaction produced this
/// message. Most messages carry none.
///
/// Carrying it requires the `mint_impetus` capability: the publish gate refuses
/// a claim the publisher's policy does not justify, so impetus on a stored
/// message is proof it was authorized when it was published, and a redeeming
/// consumer needs no second check.
///
/// Scheme-agnostic and inert by default — it means nothing until a consumer
/// defines a redemption for it.
///
/// Externally tagged with one v1 variant, so a future form (a fixed-amount
/// grant, say) is an additive variant that leaves this one untouched.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Impetus {
    /// The redeeming consumer may treat this message as the product of live user
    /// interaction and restore the allowance it governs to full.
    Replenish,
}

impl Impetus {
    /// Wire/DB string representation. The wire form is serde's (`"replenish"`);
    /// this is the same string, for the DB column and for log lines.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Replenish => "replenish",
        }
    }

    /// Parse from a wire/DB string. Returns `None` on unknown values. Named
    /// `parse` rather than `from_str` for the same reason as [`Urgency::parse`].
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "replenish" => Some(Self::Replenish),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// MessageEnvelope
// ---------------------------------------------------------------------------

/// Canonical message envelope used by `MessageQueryChannel` output and the
/// LLM-facing batch formatter.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct MessageEnvelope {
    pub message_id: Uuid,
    pub source: String,
    /// `brenn:<name>` form.
    pub channel: String,
    pub sender: String,
    pub publish_ts: DateTime<Utc>,
    pub body: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reply_to: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delivery_deadline: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deliver_after: Option<DateTime<Utc>>,
    /// Carried user-interaction authority, or `None` — which is what nearly
    /// every message carries. Skip-serialized when absent so an impetus-free
    /// envelope carries no `impetus` key on the wire.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub impetus: Option<Impetus>,
    /// Sender-assigned urgency intent. Stored in `messaging_messages.urgency`.
    /// Controls per-subscriber `eager_wake` (via `WakeMin::wakes`) at insert time.
    /// LLM-visible via serde kebab-case (`"very-low"` / `"low"` / `"normal"` / `"high"`).
    pub urgency: Urgency,
    /// Stored transport type of the channel this message was published on.
    /// Authoritative for heading selection in `format.rs`.
    /// Additive to the LLM-facing JSON; the LLM is the maximally-adaptable
    /// consumer.
    pub envelope_type: ChannelScheme,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};
    use uuid::Uuid;

    fn make_envelope(urgency: Urgency, envelope_type: ChannelScheme) -> MessageEnvelope {
        MessageEnvelope {
            message_id: Uuid::nil(),
            source: "test-source".to_string(),
            channel: "brenn:test".to_string(),
            sender: "test-sender".to_string(),
            publish_ts: DateTime::<Utc>::from_timestamp(1_700_000_000, 0).unwrap(),
            body: "hello".to_string(),
            reply_to: None,
            delivery_deadline: None,
            deliver_after: None,
            impetus: None,
            urgency,
            envelope_type,
        }
    }

    // ── Epoch-millisecond times ───────────────────────────────────────────

    #[test]
    fn epoch_ms_conversion_refuses_what_chrono_cannot_carry() {
        assert_eq!(
            utc_from_epoch_ms(1_700_000_000_123),
            DateTime::<Utc>::from_timestamp_millis(1_700_000_000_123)
        );
        assert_eq!(
            utc_from_epoch_ms(0),
            DateTime::<Utc>::from_timestamp_millis(0)
        );
        assert_eq!(utc_from_epoch_ms(u64::MAX), None);
        // Inside i64 but outside chrono's own range: the reason the check cannot
        // stop at the integer cast.
        assert_eq!(utc_from_epoch_ms(i64::MAX as u64), None);
    }

    // ── Address scheme ────────────────────────────────────────────────────

    /// Golden pin on the four prefix string values. A prefix rename or a
    /// scheme-set change becomes a conscious, test-visible edit at the single
    /// source of truth.
    #[test]
    fn address_prefix_values() {
        assert_eq!(BRENN_ADDRESS_PREFIX, "brenn:");
        assert_eq!(WEBHOOK_ADDRESS_PREFIX, "webhook:");
        assert_eq!(MQTT_ADDRESS_PREFIX, "mqtt:");
        assert_eq!(EPHEMERAL_ADDRESS_PREFIX, "ephemeral:");
    }

    // ── ChannelScheme ─────────────────────────────────────────────────────

    #[test]
    fn channel_scheme_pwa_push_prefix_value() {
        assert_eq!(PWA_PUSH_ADDRESS_PREFIX, "pwa_push:");
    }

    #[test]
    fn channel_scheme_split_all_prefixes() {
        assert_eq!(
            ChannelScheme::split("brenn:foo"),
            Some((ChannelScheme::Brenn, "foo"))
        );
        assert_eq!(
            ChannelScheme::split("ephemeral:foo"),
            Some((ChannelScheme::Ephemeral, "foo"))
        );
        assert_eq!(
            ChannelScheme::split("mqtt:client:topic"),
            Some((ChannelScheme::Mqtt, "client:topic"))
        );
        assert_eq!(
            ChannelScheme::split("webhook:my-hook"),
            Some((ChannelScheme::Webhook, "my-hook"))
        );
        assert_eq!(
            ChannelScheme::split("pwa_push:alice@device"),
            Some((ChannelScheme::PwaPush, "alice@device"))
        );
        assert_eq!(
            ChannelScheme::split("local:brenn/theme"),
            Some((ChannelScheme::Local, "brenn/theme"))
        );
    }

    #[test]
    fn channel_scheme_split_empty_remainder() {
        assert_eq!(
            ChannelScheme::split("brenn:"),
            Some((ChannelScheme::Brenn, ""))
        );
    }

    #[test]
    fn channel_scheme_split_unrecognized() {
        // `ingress:` is not an address scheme — no `ingress:` address exists.
        assert_eq!(ChannelScheme::split("ingress:foo"), None);
        assert_eq!(ChannelScheme::split("bare"), None);
        assert_eq!(ChannelScheme::split(""), None);
        assert_eq!(ChannelScheme::split("brenn"), None);
        assert_eq!(ChannelScheme::split("garbage:x"), None);
    }

    #[test]
    fn channel_scheme_of_discards_remainder() {
        assert_eq!(
            ChannelScheme::of("mqtt:client:topic"),
            Some(ChannelScheme::Mqtt)
        );
        assert_eq!(ChannelScheme::of("bare"), None);
    }

    /// The wire/DB stability guarantee: `as_str` == serde snake_case form ==
    /// documented tag, and `parse` inverts `as_str`. Drift becomes a test
    /// failure rather than silent corruption of stored strings.
    ///
    /// The tag list is pinned literally (not derived from `as_str`) — deriving
    /// it would make the test tautological, since a rename would move both
    /// sides together. `ALL` guards the *set*: adding a scheme without pinning
    /// its tag fails the length assert.
    #[test]
    fn channel_scheme_string_pinning() {
        let cases = [
            (ChannelScheme::Brenn, "brenn"),
            (ChannelScheme::Ephemeral, "ephemeral"),
            (ChannelScheme::Local, "local"),
            (ChannelScheme::Mqtt, "mqtt"),
            (ChannelScheme::Webhook, "webhook"),
            (ChannelScheme::PwaPush, "pwa_push"),
        ];
        assert_eq!(
            cases.len(),
            ChannelScheme::ALL.len(),
            "a scheme was added without pinning its wire/DB tag here"
        );
        for (scheme, tag) in cases {
            assert_eq!(scheme.as_str(), tag);
            assert_eq!(serde_json::to_value(scheme).unwrap(), json!(tag));
            assert_eq!(ChannelScheme::parse(tag), Some(scheme));
        }
        assert_eq!(ChannelScheme::parse("ingress"), None);
        assert_eq!(ChannelScheme::parse("unknown"), None);
    }

    /// Every scheme round-trips address → (scheme, remainder) through its own
    /// prefix, and `prefix` stays `as_str` + colon. Driven off `ALL` so a new
    /// scheme is covered the moment it is declared.
    #[test]
    fn channel_scheme_prefix_round_trips_for_every_scheme() {
        for scheme in ChannelScheme::ALL {
            assert_eq!(scheme.prefix(), format!("{}:", scheme.as_str()));
            let address = format!("{}some/remainder", scheme.prefix());
            assert_eq!(
                ChannelScheme::split(&address),
                Some((scheme, "some/remainder")),
                "{scheme:?} does not round-trip through its own prefix"
            );
        }
    }

    // ── ChannelCapabilities ───────────────────────────────────────────────

    /// The scheme → capability mapping, pinned per scheme rather than derived:
    /// it is the whole of the class-dependent behaviour, so it changes only by
    /// a visible edit here.
    #[test]
    fn capabilities_by_scheme() {
        assert_eq!(
            ChannelScheme::Brenn.capabilities(),
            Some(ChannelCapabilities::DURABLE_TRANSPORTABLE)
        );
        assert_eq!(
            ChannelScheme::Mqtt.capabilities(),
            Some(ChannelCapabilities::DURABLE_TRANSPORTABLE)
        );
        assert_eq!(
            ChannelScheme::Webhook.capabilities(),
            Some(ChannelCapabilities::DURABLE_TRANSPORTABLE)
        );
        assert_eq!(
            ChannelScheme::Ephemeral.capabilities(),
            Some(ChannelCapabilities::TRANSPORTABLE)
        );
        assert_eq!(
            ChannelScheme::Local.capabilities(),
            Some(ChannelCapabilities::CONFINED)
        );
        assert_eq!(ChannelScheme::PwaPush.capabilities(), None);
    }

    /// `local:` is the only scheme a wire serializer or egress adapter may never
    /// see, and it is confined by capability rather than by a check at each
    /// adapter.
    #[test]
    fn local_is_the_only_confined_scheme() {
        for scheme in ChannelScheme::ALL {
            let Some(caps) = scheme.capabilities() else {
                continue;
            };
            assert_eq!(
                !caps.transportable,
                scheme == ChannelScheme::Local,
                "{scheme:?}: local: is the only non-transportable channel scheme"
            );
        }
    }

    /// `pwa_push:` is the only scheme that answers no capability question at
    /// all — driven off `ALL` so a new scheme must decide rather than inherit.
    #[test]
    fn pwa_push_is_the_only_scheme_without_capabilities() {
        for scheme in ChannelScheme::ALL {
            assert_eq!(
                scheme.capabilities().is_none(),
                scheme == ChannelScheme::PwaPush,
                "{scheme:?}: pwa_push: is the only egress-only scheme"
            );
        }
    }

    /// The address-string helper answers exactly what the scheme it classifies
    /// answers, for every scheme, and `None` for an address carrying no
    /// recognized prefix.
    #[test]
    fn channel_capabilities_follows_the_address_scheme() {
        for scheme in ChannelScheme::ALL {
            let address = format!("{}some/name", scheme.prefix());
            assert_eq!(
                channel_capabilities(&address),
                scheme.capabilities(),
                "{scheme:?}: address helper disagrees with its scheme"
            );
        }
        assert_eq!(channel_capabilities("bare"), None);
        assert_eq!(channel_capabilities(""), None);
    }

    /// `is_local_channel` is the confined-capability reading of an address, not
    /// a scheme-name check.
    ///
    /// Pinned to literals rather than re-derived from `channel_capabilities`:
    /// re-deriving the expectation from the same helper the function calls
    /// would assert only that the two agree, which a wrong reading of
    /// capabilities satisfies as well as the right one.
    #[test]
    fn is_local_channel_tracks_confinement() {
        for (address, expected) in [
            ("brenn:orders", false),
            ("ephemeral:protobar", false),
            ("local:brenn/theme", true),
            ("mqtt:topic", false),
            ("webhook:hook", false),
            ("pwa_push:target", false),
            ("bare", false),
        ] {
            assert_eq!(
                is_local_channel(address),
                expected,
                "{address}: unexpected confinement reading"
            );
        }
    }

    // ── Urgency ───────────────────────────────────────────────────────────

    #[test]
    fn urgency_as_str() {
        assert_eq!(Urgency::VeryLow.as_str(), "very-low");
        assert_eq!(Urgency::Low.as_str(), "low");
        assert_eq!(Urgency::Normal.as_str(), "normal");
        assert_eq!(Urgency::High.as_str(), "high");
    }

    #[test]
    fn urgency_parse() {
        assert_eq!(Urgency::parse("very-low"), Some(Urgency::VeryLow));
        assert_eq!(Urgency::parse("low"), Some(Urgency::Low));
        assert_eq!(Urgency::parse("normal"), Some(Urgency::Normal));
        assert_eq!(Urgency::parse("high"), Some(Urgency::High));
        assert_eq!(Urgency::parse("unknown"), None);
    }

    #[test]
    fn urgency_ordering() {
        assert!(Urgency::VeryLow < Urgency::Low);
        assert!(Urgency::Low < Urgency::Normal);
        assert!(Urgency::Normal < Urgency::High);
    }

    #[test]
    fn urgency_rank() {
        assert_eq!(Urgency::VeryLow.rank(), 0);
        assert_eq!(Urgency::Low.rank(), 1);
        assert_eq!(Urgency::Normal.rank(), 2);
        assert_eq!(Urgency::High.rank(), 3);
    }

    #[test]
    fn urgency_serde_roundtrip() {
        for u in [
            Urgency::VeryLow,
            Urgency::Low,
            Urgency::Normal,
            Urgency::High,
        ] {
            let s = serde_json::to_string(&u).unwrap();
            let u2: Urgency = serde_json::from_str(&s).unwrap();
            assert_eq!(u, u2);
        }
    }

    // ── MessageEnvelope serde ─────────────────────────────────────────────

    #[test]
    fn message_envelope_serde_roundtrip_minimal() {
        let env = make_envelope(Urgency::Normal, ChannelScheme::Brenn);
        let s = serde_json::to_string(&env).unwrap();
        let env2: MessageEnvelope = serde_json::from_str(&s).unwrap();
        assert_eq!(env.message_id, env2.message_id);
        assert_eq!(env.urgency, env2.urgency);
        assert_eq!(env.envelope_type, env2.envelope_type);
    }

    #[test]
    fn message_envelope_serde_roundtrip_all_optional() {
        let mut env = make_envelope(Urgency::High, ChannelScheme::Webhook);
        env.reply_to = Some("reply-id".to_string());
        env.delivery_deadline = Some(DateTime::<Utc>::from_timestamp(1_700_001_000, 0).unwrap());
        env.deliver_after = Some(DateTime::<Utc>::from_timestamp(1_699_999_000, 0).unwrap());
        let s = serde_json::to_string(&env).unwrap();
        let env2: MessageEnvelope = serde_json::from_str(&s).unwrap();
        assert_eq!(env.reply_to, env2.reply_to);
        assert_eq!(env.delivery_deadline, env2.delivery_deadline);
        assert_eq!(env.deliver_after, env2.deliver_after);
    }

    #[test]
    fn message_envelope_optional_fields_absent_when_none() {
        let env = make_envelope(Urgency::Normal, ChannelScheme::Brenn);
        let v: Value = serde_json::to_value(&env).unwrap();
        assert!(v.get("reply_to").is_none(), "reply_to should be absent");
        assert!(
            v.get("delivery_deadline").is_none(),
            "delivery_deadline should be absent"
        );
        assert!(
            v.get("deliver_after").is_none(),
            "deliver_after should be absent"
        );
    }

    // Golden-JSON fixture: pins the exact wire shape including the `urgency` field.
    #[test]
    fn message_envelope_golden_json() {
        let env = MessageEnvelope {
            message_id: Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap(),
            source: "src".to_string(),
            channel: "brenn:chan".to_string(),
            sender: "alice".to_string(),
            publish_ts: DateTime::<Utc>::from_timestamp(1_700_000_000, 0).unwrap(),
            body: "test body".to_string(),
            reply_to: None,
            delivery_deadline: None,
            deliver_after: None,
            impetus: None,
            urgency: Urgency::Normal,
            envelope_type: ChannelScheme::Brenn,
        };
        let v: Value = serde_json::to_value(&env).unwrap();
        assert_eq!(
            v["message_id"],
            json!("00000000-0000-0000-0000-000000000001")
        );
        assert_eq!(v["urgency"], json!("normal"));
        assert_eq!(v["envelope_type"], json!("brenn"));
        assert_eq!(v["channel"], json!("brenn:chan"));
        // Confirm optional fields are absent in golden shape
        assert!(v.get("reply_to").is_none());
    }

    // ── Impetus ───────────────────────────────────────────────────────────

    /// The wire form out-of-tree envelope consumers parse. A rename here is a
    /// protocol break, so the literal is pinned rather than derived.
    #[test]
    fn impetus_wire_form_is_replenish() {
        assert_eq!(
            serde_json::to_value(Impetus::Replenish).unwrap(),
            json!("replenish")
        );
        assert_eq!(Impetus::Replenish.as_str(), "replenish");
        assert_eq!(Impetus::parse("replenish"), Some(Impetus::Replenish));
        assert_eq!(Impetus::parse("Replenish"), None);
        assert_eq!(Impetus::parse(""), None);
    }

    #[test]
    fn message_envelope_carries_impetus_through_a_roundtrip() {
        let mut env = make_envelope(Urgency::Normal, ChannelScheme::Brenn);
        env.impetus = Some(Impetus::Replenish);
        let v: Value = serde_json::to_value(&env).unwrap();
        assert_eq!(v["impetus"], json!("replenish"));
        let env2: MessageEnvelope = serde_json::from_value(v).unwrap();
        assert_eq!(env2.impetus, Some(Impetus::Replenish));
    }

    /// An impetus-free envelope carries no `impetus` key — strict deserializers
    /// that do not know the field continue to accept the wire form.
    #[test]
    fn an_impetus_free_envelope_carries_no_impetus_key() {
        let env = make_envelope(Urgency::Normal, ChannelScheme::Brenn);
        let v: Value = serde_json::to_value(&env).unwrap();
        assert!(v.get("impetus").is_none(), "impetus should be absent");
        // And the absent key decodes back to None without a `default` attribute.
        let env2: MessageEnvelope = serde_json::from_value(v).unwrap();
        assert_eq!(env2.impetus, None);
    }

    // ── WebhookEnvelope serde ─────────────────────────────────────────────

    #[test]
    fn webhook_envelope_serde_roundtrip() {
        let env = WebhookEnvelope {
            headers: vec![
                ("content-type".to_string(), "application/json".to_string()),
                ("authorization".to_string(), "[redacted]".to_string()),
            ],
            key_id: "key-abc".to_string(),
            client_ip: "1.2.3.4".to_string(),
            received_at: DateTime::<Utc>::from_timestamp(1_700_000_000, 0).unwrap(),
            body: r#"{"foo":"bar"}"#.to_string(),
            endpoint_slug: "my-hook".to_string(),
        };
        let s = serde_json::to_string(&env).unwrap();
        let env2: WebhookEnvelope = serde_json::from_str(&s).unwrap();
        assert_eq!(env.key_id, env2.key_id);
        assert_eq!(env.client_ip, env2.client_ip);
        assert_eq!(env.body, env2.body);
        assert_eq!(env.endpoint_slug, env2.endpoint_slug);
        assert_eq!(env.headers, env2.headers);
    }

    // ── MqttEnvelope serde ────────────────────────────────────────────────

    #[test]
    fn mqtt_envelope_text_serde_roundtrip() {
        let env = MqttEnvelope {
            client_slug: "homeassistant".to_string(),
            topic: "home/kitchen/state".to_string(),
            payload: MqttPayloadBody::Text {
                text: "22.5".to_string(),
            },
            received_at: DateTime::<Utc>::from_timestamp(1_700_000_000, 0).unwrap(),
            qos: 1,
        };
        let s = serde_json::to_string(&env).unwrap();
        let env2: MqttEnvelope = serde_json::from_str(&s).unwrap();
        assert_eq!(env, env2);
    }

    #[test]
    fn mqtt_envelope_binary_placeholder_serde_roundtrip() {
        let env = MqttEnvelope {
            client_slug: "homeassistant".to_string(),
            topic: "home/camera/frame".to_string(),
            payload: MqttPayloadBody::Binary {
                binary_placeholder: true,
                content_type: Some("image/jpeg".to_string()),
            },
            received_at: DateTime::<Utc>::from_timestamp(1_700_000_000, 0).unwrap(),
            qos: 0,
        };
        let s = serde_json::to_string(&env).unwrap();
        let env2: MqttEnvelope = serde_json::from_str(&s).unwrap();
        assert_eq!(env, env2);

        // Binary placeholder with absent content_type round-trips too, and the
        // key is emitted as `null` (not omitted) — matches the borrowed view's
        // wire shape.
        let env_nct = MqttEnvelope {
            payload: MqttPayloadBody::Binary {
                binary_placeholder: true,
                content_type: None,
            },
            ..env.clone()
        };
        let s_nct = serde_json::to_string(&env_nct).unwrap();
        assert!(
            s_nct.contains(r#""content_type":null"#),
            "content_type must serialize as null, not omitted: {s_nct}"
        );
        let env_nct2: MqttEnvelope = serde_json::from_str(&s_nct).unwrap();
        assert_eq!(env_nct, env_nct2);
    }

    /// Golden-JSON fixture pinning the on-wire shape (parallel to the webhook
    /// envelope's wire contract). Untagged payload serializes its inner fields
    /// directly with no wrapper.
    #[test]
    fn mqtt_envelope_golden_wire_shape() {
        let env = MqttEnvelope {
            client_slug: "homeassistant".to_string(),
            topic: "home/kitchen/state".to_string(),
            payload: MqttPayloadBody::Text {
                text: "22.5".to_string(),
            },
            received_at: DateTime::<Utc>::from_timestamp(1_700_000_000, 0).unwrap(),
            qos: 1,
        };
        let v = serde_json::to_value(&env).unwrap();
        assert_eq!(v["client_slug"], json!("homeassistant"));
        assert_eq!(v["topic"], json!("home/kitchen/state"));
        // Untagged Text variant: `payload` is `{ "text": ... }`, no discriminant.
        assert_eq!(v["payload"], json!({ "text": "22.5" }));
        assert_eq!(v["received_at"], json!("2023-11-14T22:13:20Z"));
        assert_eq!(v["qos"], json!(1));
    }
}
