//! Messaging config types and validation.
//!
//! Wired into `BrennConfig` via:
//! - top-level `[[channel]]` arrays → `Vec<ChannelConfigRaw>`
//! - top-level `[messaging]` section → `MessagingGlobalConfig`
//! - per-app `[app.messaging]` → `MessagingConfigRaw`
//!
//! Validation (uniqueness, charset, push-target invariants) lives in
//! free functions below — `crate::config::validate_and_resolve` calls
//! them after the rest of the app config has resolved.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use serde::de::{self, Deserializer, Visitor};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{
    ChannelEntry, ChannelScheme, MessagingDirectory, SubscriberEntryKind, WakeMin,
    is_reserved_channel_name, is_unreserved_char, nondurable_channel_uuid, publish,
};
use crate::config::AppConfigRaw;

// ---------------------------------------------------------------------------
// Depth / NoiseLevel / Sink types (design §2.1)
// ---------------------------------------------------------------------------

/// A retention depth value.
///
/// `Bounded(n)` = exactly n most-recent messages; `Unbounded` = the legacy ∞
/// behavior. An *omitted* TOML key is represented as `Option::None` at the
/// raw-config layer (meaning "inherit"), NOT as a `Depth` variant — the
/// distinction between "unbounded" and "inherit" is carried by `Option`, the
/// distinction between bounded/unbounded by this enum.
///
/// The variant order below is **load-bearing** for the derived `PartialOrd`/`Ord`:
/// the derive ranks by declaration order, so `Bounded` before `Unbounded` yields
/// the semantically correct total order — `Bounded(a) < Bounded(b)` iff `a < b`,
/// and every `Bounded(_) < Unbounded` (a bounded window is shallower than the
/// infinite one). Do not reorder the variants.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Depth {
    Bounded(u64),
    Unbounded,
}

/// Custom deserializer: accepts a non-negative integer (→ `Bounded(n)`) or the
/// string `"unbounded"` (→ `Unbounded`). Anything else is a deserialize error.
struct DepthVisitor;

impl<'de> Visitor<'de> for DepthVisitor {
    type Value = Depth;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a non-negative integer or the string \"unbounded\"")
    }

    fn visit_u64<E: de::Error>(self, v: u64) -> Result<Depth, E> {
        Ok(Depth::Bounded(v))
    }

    fn visit_i64<E: de::Error>(self, v: i64) -> Result<Depth, E> {
        if v < 0 {
            Err(E::custom(format!(
                "depth must be a non-negative integer, got {v}"
            )))
        } else {
            Ok(Depth::Bounded(v as u64))
        }
    }

    fn visit_str<E: de::Error>(self, v: &str) -> Result<Depth, E> {
        if v == "unbounded" {
            Ok(Depth::Unbounded)
        } else {
            Err(E::custom(format!(
                "expected a non-negative integer or \"unbounded\", got {v:?}"
            )))
        }
    }
}

impl<'de> Deserialize<'de> for Depth {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        d.deserialize_any(DepthVisitor)
    }
}

impl Serialize for Depth {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            Depth::Bounded(n) => s.serialize_u64(*n),
            Depth::Unbounded => s.serialize_str("unbounded"),
        }
    }
}

impl Depth {
    /// Returns true if this depth is push-enabled (> 0 or Unbounded).
    pub fn is_push_enabled(self) -> bool {
        match self {
            Depth::Bounded(0) => false,
            Depth::Bounded(_) | Depth::Unbounded => true,
        }
    }

    /// The tighter of two depths: `Unbounded` yields to any bound, and two
    /// bounds yield the smaller.
    pub fn narrowed_by(self, other: Depth) -> Depth {
        match (self, other) {
            (Depth::Unbounded, narrower) => narrower,
            (wider, Depth::Unbounded) => wider,
            (Depth::Bounded(a), Depth::Bounded(b)) => Depth::Bounded(a.min(b)),
        }
    }

    /// The looser of two depths — the one that covers both. `Unbounded`
    /// dominates: it is "no cap", and a cap that bounds one declaration's need
    /// cannot be the cap of a declaration that named none. Two bounds yield the
    /// larger.
    pub fn widened_by(self, other: Depth) -> Depth {
        match (self, other) {
            (Depth::Bounded(a), Depth::Bounded(b)) => Depth::Bounded(a.max(b)),
            _ => Depth::Unbounded,
        }
    }

    /// Collapses this depth to a concrete count bounded by `max`: `Unbounded`
    /// becomes `max`, a bounded depth is capped at `max`.
    pub fn clamped_to(self, max: u64) -> u64 {
        match self {
            Depth::Unbounded => max,
            Depth::Bounded(n) => n.min(max),
        }
    }
}

/// Noise level for push_depth-overflow events (per-subscriber).
///
/// The rungs are a monotone loudness ladder — each does everything the rung
/// below it does and more: `metered` counts; `alarm` counts and alerts; `fatal`
/// counts, alerts, and kills the instance. Declaration order is the ladder
/// order, so `Silent < Metered < Alarm < Fatal` and "at least this loud" reads
/// as a comparison.
///
/// `Fatal` is enacted only on the surface (kernel-side), never on the backend
/// overflow path: a backend subscription that resolves to `Fatal` is rejected
/// where its noise resolves ([`resolve_subscription_params`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum NoiseLevel {
    /// No signal on overflow.
    Silent,
    /// Increment a per-channel/per-subscriber drop counter.
    Metered,
    /// Increment the counter and fire an alert (superset of metered).
    Alarm,
    /// Everything `alarm` does, plus kill the overflowing instance. Surface-only
    /// (kernel-enacted); never valid on a backend subscription.
    Fatal,
}

impl NoiseLevel {
    /// Parse from a wire/DB/TOML string. Returns `None` on unknown values.
    ///
    /// Mirrors [`crate::messaging::WakeMin::parse`] — the sister per-subscription
    /// enum — so the `MessageSubscribe` intercept decodes both optional enum
    /// fields the same way instead of carrying a private one-off `parse_noise`.
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "silent" => Some(Self::Silent),
            "metered" => Some(Self::Metered),
            "alarm" => Some(Self::Alarm),
            "fatal" => Some(Self::Fatal),
            _ => None,
        }
    }
}

/// Eviction sink for a channel (per-channel / global only, never per-subscriber).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Sink {
    /// Delete bodies from the hot store.
    Drop,
    /// Append bodies to a JSONL file, then delete from the hot store.
    Archive,
}

/// Per-`(sender, channel)` send-rate gate: the token bucket every publish draws
/// one token from, on every scheme.
///
/// The grain is one bucket per publishing principal per channel. A sender's
/// aggregate budget is therefore (this rate × the channels its ACLs let it
/// publish to), which is bounded because no publisher can mint a channel to
/// widen its budget: channels come from operator config or from the server's
/// own provisioning, and no publish reaches a creation path.
#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct SendRate {
    /// Publishes admitted before refill matters.
    pub burst: u32,
    /// Seconds between refills.
    pub refill_interval_secs: u64,
    /// Tokens returned per interval — the sustained rate.
    pub refill: u32,
}

impl Default for SendRate {
    fn default() -> Self {
        // Far above any legitimate sustained rate while bounding a runaway
        // publisher — including an untrusted out-of-tree component.
        Self {
            burst: 240,
            refill_interval_secs: 1,
            refill: 30,
        }
    }
}

impl SendRate {
    /// Panics if any field would produce a nonsensical or deny-all bucket.
    /// `context` names the channel address or global-default key in the panic
    /// message. Must be called at boot — `bucket()` is built lazily, so an
    /// invalid rate would panic mid-publish instead.
    pub fn validate(&self, context: &str) {
        assert!(
            self.refill_interval_secs >= 1,
            "config: {context} send_rate.refill_interval_secs must be >= 1 \
             (a zero interval has no meaning and divides by zero on refill)",
        );
        assert!(
            self.burst >= 1,
            "config: {context} send_rate.burst must be >= 1 \
             (a zero burst admits no publish — a silent deny-all)",
        );
        assert!(
            self.refill >= 1,
            "config: {context} send_rate.refill must be >= 1 \
             (a zero refill never replenishes — a burst-then-deny-all)",
        );
    }

    /// A fresh bucket enforcing this rate.
    pub fn bucket(&self) -> crate::token_bucket::TokenBucket {
        crate::token_bucket::TokenBucket::new(
            self.burst,
            std::time::Duration::from_secs(self.refill_interval_secs),
            self.refill,
        )
    }
}

// ---------------------------------------------------------------------------
// Raw config types
// ---------------------------------------------------------------------------

/// Top-level `[[channel]]` block — one table for every pub/sub scheme, in
/// either of two roles decided by the address it names (see
/// [`channel_block_role`]).
///
/// **Declaring role.** The `address`'s scheme selects the channel's
/// capabilities: `brenn:` (or a bare address, which canonicalizes to `brenn:`)
/// is durable and transportable, `ephemeral:` is transportable only, `local:` is
/// neither. Class-uniform knobs — `push_depth`, `retain_depth`, `noise`,
/// `wake_min` — are valid on every scheme. `uuid`, `standing_retain_depth`, and
/// `sink` are durable-only: a non-durable channel has no DB row to name, no
/// reaper frontier to hold, and nothing to archive.
///
/// **Tuning role.** An address under `mqtt:`, `webhook:`, `brenn:tools/` or
/// `brenn:tool-results/` names a channel the system mints for itself. The block
/// mints nothing; it supplies that channel's depths and knobs, overriding the
/// in-code family defaults. `uuid` and `description` are forbidden there —
/// synthesis owns identity and description — and all three depths are required.
/// A tuning block may be keyed by `address_prefix` instead, standing for a whole
/// family of dynamically named channels.
#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct ChannelConfigRaw {
    /// UUID v4 in canonical hyphenated form. Globally unique across `[[channel]]`.
    /// Required on a durable channel (it names the DB row); rejected on a
    /// non-durable one, whose identity is a deterministic UUIDv5 of its address.
    pub uuid: Option<String>,
    /// Channel address, optionally scheme-qualified (`ephemeral:foo`,
    /// `local:foo`, `brenn:foo`, or bare `foo` ⇒ `brenn:foo`). The part after
    /// the scheme must match `^[A-Za-z0-9._~-]+$`.
    ///
    /// Exactly one of `address` and `address_prefix` must be set.
    pub address: Option<String>,
    /// Tuning-role key covering every system-minted address that starts with
    /// this byte prefix. Must end at a segment boundary (`/`, `.`, or the
    /// `mqtt:<client>:` colon) so it cannot reach past the family it names.
    ///
    /// Exactly one of `address` and `address_prefix` must be set.
    pub address_prefix: Option<String>,
    pub description: Option<String>,
    /// Per-channel push depth. Required: a depth is a sizing decision the
    /// operator makes for this channel, and there is no rung above it to fall
    /// back to.
    pub push_depth: Option<Depth>,
    /// Per-channel retain depth. Required. Must be bounded on a non-durable
    /// channel — its retention is process memory.
    pub retain_depth: Option<Depth>,
    /// Subscriber-independent retained buffer depth. Durable-only, and required
    /// there: a ring's standing buffer *is* its retained window, so a
    /// non-durable channel has no third number to state.
    pub standing_retain_depth: Option<Depth>,
    /// Noise level for push-overflow on this channel. `None` ⇒ inherit.
    pub noise: Option<NoiseLevel>,
    /// Eviction sink. `None` ⇒ inherit from global default. Durable-only.
    pub sink: Option<Sink>,
    /// Per-channel wake-min policy. `None` ⇒ inherit from global default.
    pub wake_min: Option<WakeMin>,
    /// Per-`(sender, channel)` send-rate gate. `None` ⇒ inherit from global
    /// default. Class-uniform: every scheme is rate-gated.
    pub send_rate: Option<SendRate>,
}

/// Top-level `[[connection]]` block — an **auto channel** declared by the ports
/// it wires together instead of by a `[[channel]]` block.
///
/// An auto channel comes into existence because ports are connected. It has no
/// `[[channel]]` block and no operator-written ACL entries: the transport grants
/// and channel matchers its endpoints need are injected at boot from this
/// declaration, because the connection *is* the operator's authorization signal.
/// Channel-level tuning is not available: every depth is the fold over the
/// subscribing endpoints' own declared depths, and `sink`, `send_rate`,
/// `wake_min` and channel-level `noise` come from the `[messaging]` defaults. A
/// channel that needs any of those stated for itself has outgrown auto
/// declaration; write the `[[channel]]` block.
///
/// ```toml
/// [[connection]]
/// endpoints = ["wasm:etl/batch-out", "wasm:indexer/batch-in"]
/// ```
///
/// **Direction is inferred, never stated.** An endpoint naming a port declared in
/// a `subscriptions` list subscribes; one naming a port in an `outputs` list
/// publishes; one naming an `io_port` does both.
///
/// **Anonymous by default.** With `channel` absent the channel's bare name is
/// `auto.<cid>`, where `cid` is a UUIDv5 of the sorted endpoint set, and its
/// scheme is decided from where the endpoints live: all-backend ⇒ `local:`,
/// all-one-surface ⇒ page-local `local:`, anything spanning the wire ⇒
/// `ephemeral:`. Adding an endpoint changes the address; nothing outlives that,
/// because an anonymous channel is never durable.
#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct ConnectionConfigRaw {
    /// The ports this connection wires together. Two reference forms:
    /// `wasm:<slug>/<port>` for a port on a `[[wasm_consumer]]`, and
    /// `surface:<surface-slug>#<instance>/<port>` for a port on a
    /// surface-hosted component (the prefix before `/<port>` is exactly the
    /// component's participant id, so endpoint refs grep against logs and cursor
    /// rows).
    ///
    /// Every referenced port must be *free*: declared without a `channel` of its
    /// own, and referenced by exactly one connection. The endpoint set must
    /// contain at least one publisher and at least one subscriber, and must not
    /// be a single `io_port` (which needs no connection to loop back to itself).
    pub endpoints: Vec<String>,
    /// Absent ⇒ anonymous. Present ⇒ a **named** auto channel at this full
    /// scheme-qualified address, where the scheme picks the capability:
    /// `brenn:` is durable (the reason to name one at all), `ephemeral:` is
    /// transportable-only and shared across sessions, `local:` is neither and is
    /// legal only when every endpoint lives on one side of the wire.
    ///
    /// The name wears `[[channel]]`-grade validation and may not fall in a
    /// reserved namespace. Colliding with a `[[channel]]` block, a
    /// webhook/mqtt-derived channel, or another connection's or io_port's name is
    /// a boot panic: that overlap is the seam through which auto-injected ACLs
    /// could reach a channel other parties legitimately use.
    ///
    /// Naming a channel is what lets a third party bind it with ordinary
    /// bindings and ordinary ACLs — but for `local:` that reach stops at the
    /// realm boundary. An all-backend `local:` channel is a server ring only
    /// backend bindings join; an all-one-surface one is a page ring only that
    /// surface's bindings join. Crossing the two is a boot panic, not a silent
    /// dead binding. Wiring a page to the backend takes `ephemeral:`.
    pub channel: Option<String>,
    /// UUID v4 in canonical hyphenated form, naming the DB row. Legal only on a
    /// durable (`brenn:`) named channel.
    ///
    /// Absent, a durable auto channel's identity is derived from its bare name —
    /// so renaming it re-keys the DB row (fresh resume epoch; old row kept
    /// orphaned until manually deleted). Writing this field is the
    /// rename-stability opt-out.
    pub uuid: Option<String>,
    /// Lands in the channel's directory description. Absent, a description is
    /// generated from the endpoint set, so a uuid-named row in a listing or log
    /// still explains itself.
    pub description: Option<String>,
}

/// `[messaging]` section.
#[derive(Debug, Deserialize, Clone)]
#[serde(default, deny_unknown_fields)]
pub struct MessagingGlobalConfig {
    /// Default per-conversation send budget. Overridable per-app via
    /// `[app.messaging.send_budget]`. Default 100.
    pub default_send_budget: u32,
    /// Maximum body length, bytes. Default 64 KiB. Sends exceeding this
    /// return an error tool result and consume no budget.
    pub max_body_bytes: usize,
    /// Global default noise level. Default `Silent`.
    pub default_noise: NoiseLevel,
    /// Global default sink. Default `Drop`.
    pub default_sink: Sink,
    /// Directory/file path for JSONL archive output. Required iff any resolved
    /// channel has `sink = Archive`.
    pub archive_path: Option<PathBuf>,
    /// Global default wake-min threshold for **urgency-gated** subscribers (LLM
    /// conversations), whose eager wake spawns a subprocess and is therefore gated
    /// by message urgency. It has no effect on eager subscribers (WASM consumers,
    /// surface sessions, system participants), which are always delivered eagerly
    /// regardless of this value. Default `Normal` (migration parity: a
    /// `Normal`-urgency message wakes a `Normal` conversation subscriber, matching
    /// old `Immediate` behavior; a `Low`-urgency message parks, matching old `None`).
    pub default_wake_min: WakeMin,
    /// Global default per-`(sender, channel)` send rate, overridable per
    /// `[[channel]]`.
    pub default_send_rate: SendRate,
}

impl Default for MessagingGlobalConfig {
    fn default() -> Self {
        Self {
            default_send_budget: 100,
            max_body_bytes: 65_536,
            default_noise: NoiseLevel::Silent,
            default_sink: Sink::Drop,
            archive_path: None,
            // Migration parity: Normal means old-Immediate still wakes, old-None parks.
            default_wake_min: WakeMin::Normal,
            default_send_rate: SendRate::default(),
        }
    }
}

/// Per-app `[app.messaging]` block (raw form).
#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct MessagingConfigRaw {
    // NOTE: the legacy `enabled` authorization boolean was removed (access-control
    // design §2.5.1 / §8 decision-2). Authorization is now decided by the app's
    // `AppPolicy` (`messaging_enabled()` reads `MessagingPublish`/`MessagingSubscribe`
    // grants). Because this struct carries `#[serde(deny_unknown_fields)]`, a stale
    // config that still sets `enabled = true/false` under `[app.messaging]` now
    // fails to parse with a precise error — the intended migration-forcing.
    /// Channel addresses (with `brenn:` prefix) this app subscribes to.
    #[serde(default)]
    pub subscribe: Vec<MessagingSubscriptionRaw>,
    /// Per-conversation send budget reset on each user chat message.
    /// Defaults to the global `[messaging].default_send_budget`.
    pub send_budget: Option<u32>,
}

/// Per-subscription TOML block (`[[app.messaging.subscribe]]`).
///
/// `standing_retain_depth` and `sink` are per-channel/global only; this struct
/// has `deny_unknown_fields`, so an attempt to set them here produces a
/// deserialize error automatically.
#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct MessagingSubscriptionRaw {
    /// Channel address, e.g. `brenn:my-channel`.
    pub channel: String,
    /// Per-subscription push depth. `None` ⇒ inherit the channel's rung.
    pub push_depth: Option<Depth>,
    /// Per-subscription retain depth. `None` ⇒ inherit the channel's rung.
    pub retain_depth: Option<Depth>,
    /// Per-subscription noise level for push overflow. `None` ⇒ inherit.
    /// Hard config error if set on a pull-only (`push_depth = 0`) subscription.
    pub noise: Option<NoiseLevel>,
    /// Per-subscription wake-min policy. `None` ⇒ inherit the channel's rung.
    /// Hard config error if set on a pull-only (`push_depth = 0`) subscription
    /// (no push rows exist; the policy is meaningless).
    pub wake_min: Option<WakeMin>,
}

/// Grantable capability interface names for WASM processor consumers (operator-facing, stable).
///
/// Each variant corresponds to one WIT interface in `world processor`. A grant
/// selects whether that interface's host functions are linked for a given component.
/// Deny-by-default: only listed grants are linked; all others are absent from the linker.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WasmGrant {
    /// `brenn:processor/ports` — output-port publish.
    Ports,
    /// `brenn:processor/store` — KV store (also requires `store_path` in config).
    Store,
    /// `brenn:processor/log` — structured logging.
    Log,
    /// `brenn:processor/alert` — phone/operator alerting.
    Alert,
    /// `brenn:processor/config` — read-only operator config.
    Config,
    /// `brenn:processor/mqtt` — synchronous direct-to-broker MQTT publish.
    Mqtt,
}

/// Grantable transport rights for a `[[surface]]` bus participant (operator-facing).
///
/// Unlike `WasmGrant` (whose tokens name WIT interfaces, deriving
/// `MessagingSubscribe` implicitly from `subscribe_acl` presence because no
/// grant token maps to it — see `build_wasm_policy`), a surface's grant
/// vocabulary names the four transport rights *directly*, one per delivery
/// class × direction. This follows the deny-by-default sketch
/// (`grants = ["ephemeral_subscribe"]`): with an explicit token for every
/// right there is no missing-grant gap to paper over with derivation, and
/// deny-by-default reads straight off the config.
///
/// Serde `snake_case` so the multi-word variants author as
/// `ephemeral_subscribe`/`ephemeral_publish`, matching the
/// `AppCapability` tokens they map onto.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SurfaceGrant {
    /// Durable (`brenn:`) delivery to the surface. Maps to `MessagingSubscribe`.
    Subscribe,
    /// Durable (`brenn:`) publish from the surface. Maps to `MessagingPublish`.
    Publish,
    /// Ephemeral (`ephemeral:`) delivery to the surface. Maps to `EphemeralSubscribe`.
    EphemeralSubscribe,
    /// Ephemeral (`ephemeral:`) publish from the surface. Maps to `EphemeralPublish`.
    EphemeralPublish,
    /// Alert (phone/operator paging) emission from the surface. Maps to
    /// `SurfaceAlert`. Deny-by-default: without this grant a surface has no
    /// alert plane. Authoring token `"alert"` — the same string operators
    /// already write for a WASM consumer's alert grant.
    Alert,
    /// Takeover (fullscreen overlay) emission from the surface. Maps to
    /// `SurfaceTakeover`. Deny-by-default: without this grant the shell drops a
    /// component's takeover request and never pushes an overlay. Authoring token
    /// `"takeover"`.
    Takeover,
}

/// Top-level `[[wasm_consumer]]` block.
///
/// Declares a WASM processing component as a bus subscriber. The component
/// at `component_path` is loaded at bootstrap; a missing or unloadable
/// component is a fail-fast bootstrap panic (config is host-authored).
#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct WasmConsumerConfigRaw {
    /// Globally unique slug; becomes `wasm:<slug>` as the participant identity.
    /// Charset: `[A-Za-z0-9._~-]+`, no `:` or `@`.
    pub slug: String,
    /// Path to the WASM component artifact (must exist at startup).
    pub component_path: std::path::PathBuf,
    /// Capability interfaces to link for this component (deny-by-default).
    /// Required — no default. The operator states intent explicitly; a missing
    /// field is a serde error at load time. Empty list = zero-capability consumer.
    pub grants: Vec<WasmGrant>,
    /// Path to the per-component SQLite KV store. Required iff `"store"` is in
    /// `grants`; must be absent otherwise.
    pub store_path: Option<std::path::PathBuf>,
    /// Per-consumer store size limit override. Human-readable binary size
    /// string (e.g. `"64MiB"`). `None` ⇒ use `[wasm].store_size_limit` global default.
    /// Must be absent when `"store"` is not in `grants`.
    pub store_size_limit: Option<String>,
    /// Channel subscriptions for this component.
    #[serde(default, rename = "subscription")]
    pub subscriptions: Vec<WasmConsumerSubscriptionRaw>,
    /// Output port bindings for this component.
    #[serde(default, rename = "output")]
    pub outputs: Vec<WasmConsumerOutputRaw>,
    /// Combined input+output port declarations for this component — the
    /// self-loop, made structural. See [`WasmConsumerIoPortRaw`].
    #[serde(default, rename = "io_port")]
    pub io_ports: Vec<WasmConsumerIoPortRaw>,
    /// Layer-2 subscribe ACL: channel matchers narrowing which `brenn:`
    /// channels this component may hold a (static) subscription to.
    /// Flat top-level `Vec`, matching the existing flat `grants` authoring
    /// convention (authoring-shape asymmetry vs. the LLM `[app.acl.*]` sub-table is
    /// deliberate — both resolve into the same `AppPolicy`). A non-empty
    /// `subscribe_acl` derives the `MessagingSubscribe` transport grant and IS
    /// enforced at delivery time over `Wasm` subscribers; an empty list means
    /// the consumer holds no subscribe
    /// authorization (deny-by-default at delivery). This list narrows `brenn:`
    /// subscriptions only; `webhook:` and `mqtt:` subscriptions are narrowed by
    /// `webhook_acl` / `mqtt_subscribe_acl` respectively.
    #[serde(default)]
    pub subscribe_acl: Vec<crate::access::raw::ChannelMatcherRaw>,
    /// Layer-2 ephemeral subscribe ACL: channel matchers narrowing which
    /// `ephemeral:` channels this component may hold a (static) subscription to.
    /// Same flat shape as `subscribe_acl`, scoped to the `ephemeral:` scheme
    /// (matchers are scheme-stripped names). Non-empty derives the
    /// `EphemeralSubscribe` transport grant; empty means deny-by-default. A
    /// ring-backed input's authorization is decided here at boot — ring delivery
    /// reads the subscriber cursor directly and never re-runs a delivery ACL gate.
    #[serde(default)]
    pub ephemeral_subscribe_acl: Vec<crate::access::raw::ChannelMatcherRaw>,
    /// `local:` (confined) channels this component may hold a (static) input on.
    /// Same flat shape as `ephemeral_subscribe_acl`, scoped to the `local:`
    /// scheme (matchers are scheme-stripped names). Non-empty derives the
    /// `LocalSubscribe` transport grant; empty means deny-by-default. Like the
    /// ephemeral case, a confined input's authorization is decided here at boot —
    /// ring delivery reads the subscriber cursor directly and never re-runs a
    /// delivery ACL gate.
    #[serde(default)]
    pub local_subscribe_acl: Vec<crate::access::raw::ChannelMatcherRaw>,
    /// Layer-2 publish ACL: channel matchers narrowing which `brenn:` channels this
    /// component's output ports may publish to. Same flat-`Vec` shape as
    /// `subscribe_acl`; deny-by-default when empty.
    #[serde(default)]
    pub publish_acl: Vec<crate::access::raw::ChannelMatcherRaw>,
    /// Layer-2 ephemeral publish ACL: channel matchers narrowing which
    /// `ephemeral:` channels this component's output ports may publish to. Same
    /// shape as `publish_acl`, scoped to the `ephemeral:` scheme (matchers are
    /// scheme-stripped names). Non-empty derives the `EphemeralPublish` capability;
    /// empty means deny-by-default.
    #[serde(default)]
    pub ephemeral_publish_acl: Vec<crate::access::raw::ChannelMatcherRaw>,
    /// Layer-2 local publish ACL: channel matchers narrowing which `local:`
    /// (confined) channels this component's output ports may publish to. Same
    /// shape as `ephemeral_publish_acl`, scoped to the `local:` scheme. Non-empty
    /// derives the `LocalPublish` capability; empty means deny-by-default.
    #[serde(default)]
    pub local_publish_acl: Vec<crate::access::raw::ChannelMatcherRaw>,
    /// Layer-2 MQTT publish ACL: per-client allowlist narrowing which
    /// `[[mqtt_client]]` slugs this component's `mqtt-publish` host call may target
    /// (mqtt-egress-unify design §2.5). Reuses the **same** `client`-keyed
    /// `MqttClientMatcherRaw` matcher as the LLM `[[app.acl.mqtt_publish]]` block,
    /// resolving into `AppPolicy.acls.mqtt_publish` exactly as the LLM side does;
    /// the guest addresses MQTT egress by client slug (design §2.4) and the ACL is
    /// the attenuation boundary. Same flat top-level `Vec` authoring convention as
    /// `subscribe_acl`/`publish_acl` (the table-nesting asymmetry vs. the LLM
    /// `[app.acl.mqtt_publish]` sub-table is deliberate; both resolve into the same
    /// `AppPolicy`, design §2.5). A non-empty list derives no grant on its own — the
    /// `"mqtt"` grant must be authored explicitly in `grants`; an empty list means
    /// the consumer holds no MQTT-publish authorization (deny-by-default).
    #[serde(default)]
    pub mqtt_publish_acl: Vec<crate::access::raw::MqttClientMatcherRaw>,
    /// Layer-2 MQTT *subscribe* ACL: `(client, topic_filter)` matchers narrowing
    /// which inbound `mqtt:` channels this component may hold a (static)
    /// subscription to. Reuses the **same** `MqttSubMatcherRaw` matcher as the LLM
    /// `[[app.acl.mqtt_subscribe]]` block, resolving into `AppPolicy.acls.mqtt_subscribe`
    /// exactly as the LLM side does; coverage is filter-subset per `mqtt_match`
    /// (a matcher's `topic_filter` must be a superset of the subscribed filter).
    /// Same flat top-level `Vec` authoring convention as `subscribe_acl`. A
    /// non-empty list derives the `MqttSubscribe` transport grant (there is no
    /// `WasmGrant` for inbound subscribe, mirroring `subscribe_acl`'s
    /// `MessagingSubscribe` derivation); an empty list means the consumer holds no
    /// MQTT-subscribe authorization (deny-by-default at delivery).
    #[serde(default)]
    pub mqtt_subscribe_acl: Vec<crate::access::raw::MqttSubMatcherRaw>,
    /// Layer-2 webhook subscribe ACL: endpoint-slug matchers narrowing which
    /// inbound `webhook:` channels this component may hold a (static) subscription
    /// to. Reuses the **same** `WebhookMatcherRaw` matcher as the LLM
    /// `[[app.acl.webhook]]` block, resolving into `AppPolicy.acls.webhook`; the
    /// matcher `endpoint` is a scheme-stripped slug matched exactly against the
    /// subscribed `webhook:<endpoint>` channel. Same flat top-level `Vec` authoring
    /// convention as `subscribe_acl`. Unqualified (no direction suffix) because
    /// webhooks are inbound-only, matching the LLM side's unqualified `webhook` ACL.
    /// A non-empty list derives the `Webhook` transport grant (no `WasmGrant` for
    /// inbound webhook, mirroring `subscribe_acl`); an empty list means the consumer
    /// holds no webhook-subscribe authorization (deny-by-default at delivery).
    #[serde(default)]
    pub webhook_acl: Vec<crate::access::raw::WebhookMatcherRaw>,
    /// Operator-supplied config map for this component (`[wasm_consumer.config]`).
    /// Values must be strings, integers, or booleans; floats, datetimes, arrays,
    /// and nested tables are rejected at load time. `None` when the sub-table is
    /// absent (equivalent to an empty table).
    #[serde(default)]
    pub config: Option<toml::Table>,
    /// Activation pacing burst — the token-bucket capacity in *activations*
    /// (mqtt-wasm-republish-pacing design §3.1). Up to this many activations may
    /// run back-to-back after idle before the sustained gate applies. `None` ⇒
    /// `DEFAULT_ACTIVATION_BURST`. Rejected at resolve time when `< 1`. Unlike
    /// `store_size_limit`, there is no `[wasm]`-table global fallback — the
    /// per-consumer knob (or the hardcoded default) is the whole surface (design
    /// §3.1 deliberate deviation).
    pub activation_burst: Option<u32>,
    /// Activation pacing minimum period in milliseconds — one activation is
    /// admitted per this interval under sustained load (bucket refill interval,
    /// design §3.1). `None` ⇒ `DEFAULT_ACTIVATION_MIN_PERIOD`. Rejected at resolve
    /// time when `< 1` (a zero interval would panic in `TokenBucket::new`; we
    /// reject it at the config layer where the message names the slug).
    pub activation_min_period_ms: Option<u64>,
    /// Per-MQTT-client egress budget overrides (`[[wasm_consumer.mqtt_output]]`).
    /// One sink exists per `mqtt_publish_acl`-allowed client regardless of these
    /// blocks; a block only overrides that sink's budget knobs. A block naming a
    /// client outside `mqtt_publish_acl`, or a duplicate `client`, is a boot panic.
    #[serde(default, rename = "mqtt_output")]
    pub mqtt_outputs: Vec<WasmConsumerMqttOutputRaw>,
    /// Tool grants for this component (`[[wasm_consumer.tool_grant]]`). Identical
    /// table shape as `[[app.tool_grant]]` — one grant vocabulary, both
    /// participant kinds. Each authorizes addressing a registry tool, optionally
    /// narrowed by an `acl` and throttled by `rate_limit`. Absent ⇒ no tool
    /// authorization (deny-by-default).
    #[serde(default, rename = "tool_grant")]
    pub tool_grants: Vec<crate::tools::config::ToolGrantRaw>,
}

#[cfg(test)]
impl WasmConsumerConfigRaw {
    /// Minimal raw consumer subscribing (port `in`) to each of `channels`, with
    /// everything else defaulted/empty. Shared across this crate's test modules
    /// so a new field on this `deny_unknown_fields` struct does not fan out into
    /// every hand-written literal here. `#[cfg(test)]` items are not visible to
    /// dependent crates, so brenn-server keeps its own equivalent fixture.
    pub fn minimal(slug: &str, component_path: std::path::PathBuf, channels: &[&str]) -> Self {
        WasmConsumerConfigRaw {
            slug: slug.to_string(),
            component_path,
            grants: vec![],
            store_path: None,
            store_size_limit: None,
            subscriptions: channels
                .iter()
                .map(|channel| WasmConsumerSubscriptionRaw {
                    channel: Some(channel.to_string()),
                    port: "in".to_string(),
                    push_depth: None,
                    retain_depth: None,
                    noise: None,
                    wake_min: None,
                    amplification: None,
                })
                .collect(),
            outputs: vec![],
            io_ports: vec![],
            subscribe_acl: vec![],
            ephemeral_subscribe_acl: vec![],
            local_subscribe_acl: vec![],
            publish_acl: vec![],
            ephemeral_publish_acl: vec![],
            local_publish_acl: vec![],
            mqtt_publish_acl: vec![],
            mqtt_subscribe_acl: vec![],
            webhook_acl: vec![],
            config: None,
            activation_burst: None,
            activation_min_period_ms: None,
            mqtt_outputs: vec![],
            tool_grants: vec![],
        }
    }
}

/// Resolved per-consumer activation pacing (mqtt-wasm-republish-pacing design
/// §3.2). Carried on `ResolvedWasmConsumer` (defaults already applied) and copied
/// through to the off-loop dispatch task, which builds a `TokenBucket` from it.
/// Sustained activation rate is one per `min_period`; up to `burst` activations
/// may run back-to-back after idle.
#[derive(Debug, Clone, Copy)]
pub struct ActivationPacing {
    /// Token-bucket capacity in activations (`>= 1`, validated at resolve).
    pub burst: u32,
    /// Refill interval — one activation admitted per `min_period` under sustained
    /// load (`>= 1ms`, validated at resolve).
    pub min_period: Duration,
}

/// Per-subscription block inside `[[wasm_consumer]]`.
///
/// Reuses the same depth-inheritance ladder as app messaging subscriptions
/// (`push_depth`/`retain_depth` optional, inherit channel → global).
/// `noise` controls push-overflow alarm behavior.
#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct WasmConsumerSubscriptionRaw {
    /// Channel address, e.g. `brenn:my-channel` or `webhook:my-endpoint`.
    ///
    /// `None` makes this a **free port**: the binding declares the port and its
    /// tuning, and exactly one `[[connection]]` must reference it to supply the
    /// channel. A free port bound by no connection is dead config and a boot
    /// panic. Addresses in the reserved `auto` namespace are rejected here — an
    /// anonymous auto channel is reachable only through the declarations that
    /// created it.
    #[serde(default)]
    pub channel: Option<String>,
    /// Logical input port name presented to the guest. Required — no host default.
    /// Must be non-empty and consist of RFC 3986 unreserved characters.
    pub port: String,
    /// Per-subscription push depth. `None` ⇒ inherit (channel → global).
    pub push_depth: Option<Depth>,
    /// Per-subscription retain depth. `None` ⇒ inherit (channel → global).
    pub retain_depth: Option<Depth>,
    /// Per-subscription noise level for push overflow. `None` ⇒ inherit.
    pub noise: Option<NoiseLevel>,
    /// Per-subscription wake-min policy. `None` ⇒ inherit (channel → global).
    /// Hard config error if set on a pull-only (`push_depth = 0`) subscription.
    pub wake_min: Option<WakeMin>,
    /// Per-input publish amplification factor: how many publish tokens each *new*
    /// envelope arriving on this input grants to every egress sink's bucket. `None`
    /// ⇒ `DEFAULT_WASM_INPUT_AMPLIFICATION` (1.0). Must be finite and `>= 0` when
    /// present; `< 1.0` (e.g. `0.1` for "publish once per 10 inputs") is expressly
    /// supported via millitoken fixed point. Retained context envelopes contribute
    /// nothing — only newly-delivered envelopes grant tokens.
    pub amplification: Option<f64>,
}

/// Per-output binding block inside `[[wasm_consumer]]`.
///
/// Binds a logical output port name to a bus channel address. The component
/// may call `publish(port, payload)` with this port name to send a message
/// on the bound channel.
#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct WasmConsumerOutputRaw {
    /// Logical output port name. Must be non-empty and unreserved-charset.
    pub port: String,
    /// Target channel address.
    ///
    /// `None` makes this a **free port**, bound by exactly one `[[connection]]`;
    /// see [`WasmConsumerSubscriptionRaw::channel`].
    #[serde(default)]
    pub channel: Option<String>,
    /// Default urgency for messages published on this output port (sub → port →
    /// `normal`). Guests may override per-message via `publish-with-urgency`.
    pub urgency: Option<super::Urgency>,
    /// Token-bucket fill per activation for this output sink. `None` ⇒
    /// `DEFAULT_WASM_PUBLISH_PER_ACTIVATION` (1.0). `0` = purely input-driven
    /// (only the per-input amplification grant feeds this sink). Must be finite and
    /// `>= 0` when present.
    pub publish_per_activation: Option<f64>,
    /// Max tokens carried over between activations for this output sink (the bucket
    /// capacity clamp applied at the *start* of the next activation). `None` ⇒
    /// `DEFAULT_WASM_PUBLISH_CAPACITY` (1.0). Must be finite and `>= 0` when present.
    pub publish_capacity: Option<f64>,
}

/// A combined input+output port on a `[[wasm_consumer]]`
/// (`[[wasm_consumer.io_port]]`).
///
/// One port name, registered once, resolving to a `WasmInputPort` **and** a
/// `WasmOutputPort` on the *same* channel. That is the whole point: whatever
/// channel serves the port serves both directions as a unit, so "I see my own
/// publishes here" is structural rather than an operator convention that fails
/// silently when the two halves are wired to different channels. This is the
/// sanctioned shape for the timer idiom — `publish-deferred` on the port, woken
/// by one's own message when it releases.
///
/// The consumer's `grants` must still include `"ports"`: that token gates whether
/// the publish interface is linked into the component at all, so it is not ACL
/// boilerplate this block can absorb.
///
/// ```toml
/// [[wasm_consumer.io_port]]
/// port = "timer"
/// push_depth = 2
/// retain_depth = 8
/// ```
///
/// With no `channel` and no `[[connection]]` naming it, the port gets its own
/// anonymous non-transportable channel — no channel-level config at all: no
/// `[[channel]]` block, no ACLs. The port's depths are not optional though: the
/// channel's every depth folds from `max(push_depth, retain_depth)` over its
/// subscribing ports, so both must be written here, and both must be bounded on
/// a non-durable channel or boot refuses. Give it a `channel` to
/// make it a named auto channel (`brenn:` for schedules that survive restart),
/// or list it in a `[[connection]]` to let other components see and feed the
/// same traffic; setting both is a boot panic.
#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct WasmConsumerIoPortRaw {
    /// Logical port name presented to the guest for both directions. Must be
    /// non-empty, unreserved-charset, and distinct from every other port name on
    /// the consumer — this block is the only sanctioned way one name serves both
    /// directions, because it is what carries the same-channel guarantee.
    pub port: String,
    /// Absent ⇒ the port's own anonymous non-transportable channel. Present ⇒ a
    /// named auto channel at this full scheme-qualified address. Must be absent
    /// when a `[[connection]]` lists this port.
    ///
    /// A `local:` name here is a *server* ring: only backend bindings join it.
    /// `local:` namespaces are private per realm, so a surface binding on the
    /// same bare name is an unrelated page ring, by design — the two exchange no
    /// message and neither knows the other exists. `ephemeral:` is what reaches
    /// a page.
    #[serde(default)]
    pub channel: Option<String>,
    /// Push depth for the input half. Required: an io_port is a subscribing
    /// endpoint on an auto channel, and the channel's depths are folded from
    /// exactly these numbers.
    pub push_depth: Option<Depth>,
    /// Retain depth for the input half. Required, same reason as
    /// [`Self::push_depth`], and on a non-durable channel it must be bounded.
    ///
    /// On an auto channel this value also *sets* the channel's own
    /// `retain_depth` (folded as the max over subscribing endpoints of
    /// `max(push_depth, retain_depth)`, floor 1) — and a channel's `retain_depth`
    /// caps its channel-wide deferred set. So this knob bounds how many
    /// `deliver_after` schedules the port may hold outstanding: a component
    /// juggling K parked wakes must declare at least K here, on top of its actual
    /// retention need. Over-cap flushes are dropped and host-logged only; the
    /// per-port `deferred-window` lets a careful component notice.
    pub retain_depth: Option<Depth>,
    /// Noise level for push overflow on the input half. `None` ⇒ inherit.
    pub noise: Option<NoiseLevel>,
    /// Per-input publish amplification for the input half. `None` ⇒
    /// [`DEFAULT_WASM_INPUT_AMPLIFICATION`]. Same semantics as
    /// [`WasmConsumerSubscriptionRaw::amplification`].
    pub amplification: Option<f64>,
    /// Default urgency for messages published on the output half. Same semantics
    /// as [`WasmConsumerOutputRaw::urgency`].
    pub urgency: Option<super::Urgency>,
    /// Token-bucket fill per activation for the output half. `None` ⇒
    /// [`DEFAULT_WASM_PUBLISH_PER_ACTIVATION`].
    pub publish_per_activation: Option<f64>,
    /// Max tokens carried between activations for the output half. `None` ⇒
    /// [`DEFAULT_WASM_PUBLISH_CAPACITY`].
    pub publish_capacity: Option<f64>,
}

/// Per-MQTT-client egress budget override block inside `[[wasm_consumer]]`
/// (`[[wasm_consumer.mqtt_output]]`).
///
/// One MQTT egress sink exists per `[[mqtt_client]]` slug the component's
/// `mqtt_publish_acl` allows; ACL-allowed clients without a block get the default
/// budget. This block only *overrides* the two per-sink knobs for one client — its
/// presence is not what authorizes egress (that is the `mqtt_publish_acl` + `mqtt`
/// grant). `client` must name a client covered by `mqtt_publish_acl`; a block for
/// an unlisted client, or a duplicate `client`, is a boot panic (dead config).
#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct WasmConsumerMqttOutputRaw {
    /// MQTT client slug this override applies to. Must be covered by
    /// `mqtt_publish_acl` (validated at resolution).
    pub client: String,
    /// Token-bucket fill per activation for this MQTT sink. `None` ⇒
    /// `DEFAULT_WASM_PUBLISH_PER_ACTIVATION` (1.0). Same semantics as the output
    /// port knob of the same name.
    pub publish_per_activation: Option<f64>,
    /// Max tokens carried over between activations for this MQTT sink. `None` ⇒
    /// `DEFAULT_WASM_PUBLISH_CAPACITY` (1.0). Same semantics as the output port knob.
    pub publish_capacity: Option<f64>,
}

/// Default per-activation publish-bucket fill (tokens) for a WASM egress sink when
/// `publish_per_activation` is unset. Conservative: one publish per activation.
pub const DEFAULT_WASM_PUBLISH_PER_ACTIVATION: f64 = 1.0;
/// Default publish-bucket carryover capacity (tokens) for a WASM egress sink when
/// `publish_capacity` is unset. Conservative: at most one token accumulates idle.
pub const DEFAULT_WASM_PUBLISH_CAPACITY: f64 = 1.0;
/// Default per-input publish amplification factor when `amplification` is unset.
/// One publish token per new envelope — captures the 1:1 republisher case.
pub const DEFAULT_WASM_INPUT_AMPLIFICATION: f64 = 1.0;
/// Fixed-point scale for publish tokens: all `f64` budget knobs resolve to integer
/// millitokens (`value * 1000`, rounded to nearest) so attenuation is exact and the
/// runtime hot path is integer-only saturating arithmetic. One publish costs
/// `MILLITOKENS_PER_PUBLISH`.
///
/// Re-exported from `brenn-budget`, which every host that spends these tokens
/// reads. Resolved budgets cross crate boundaries as raw millitoken `u64`s, so
/// one definition is the only way the scale cannot drift.
pub use brenn_budget::MILLITOKENS_PER_PUBLISH;
/// Resolve-time sanity ceiling on any `f64` budget knob (tokens). Keeps millitoken
/// math far from `u64` saturation; a value above this is a boot panic.
pub const MAX_WASM_PUBLISH_KNOB: f64 = 1_000_000.0;

/// Default per-connection publish burst (tokens) when `publish_burst` is unset.
/// Sits far under the bus-level per-sender gate so the connection bucket trips
/// first and the bus bucket stays defense in depth.
pub const DEFAULT_SURFACE_PUBLISH_BURST: u32 = 60;
/// Default per-connection sustained publish refill (tokens/sec) when
/// `publish_per_sec` is unset.
pub const DEFAULT_SURFACE_PUBLISH_PER_SEC: u32 = 1;

/// Top-level `[[surface]]` block.
///
/// Declares a browser surface as an ACL-bounded bus participant, following the
/// `[[wasm_consumer]]` precedent: operator-authored slug, explicit `grants`
/// (no default — intent is stated, mirroring `WasmConsumerConfigRaw::grants`),
/// four optional ACL matcher lists, and nested component/subscription/output
/// blocks. `deny_unknown_fields` on every struct closes the door on typos.
///
/// `allowed_users` is the surface access check (empty/absent = any
/// authenticated user, `AppConfig::user_has_access` semantics); `publish_burst`
/// / `publish_per_sec` are the per-connection publish token-bucket caps
/// (absent = the `DEFAULT_SURFACE_PUBLISH_*` constants).
///
/// This defines and parses these types; boot-time resolution + cross-validation
/// (`resolve_surfaces`) is done separately.
#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct SurfaceConfigRaw {
    /// Globally unique slug; becomes `surface:<slug>` as the participant identity.
    /// Charset enforced at resolution: `[A-Za-z0-9._~-]+`, no `:`/`@`/`#`.
    pub slug: String,
    /// Transport rights for this surface (deny-by-default). Required — no default;
    /// the operator states intent explicitly, exactly like `[[wasm_consumer]]`.
    pub grants: Vec<SurfaceGrant>,
    /// Durable (`brenn:`) subscribe ACL — bare channel names, no scheme.
    #[serde(default)]
    pub subscribe_acl: Vec<crate::access::raw::ChannelMatcherRaw>,
    /// Durable (`brenn:`) publish ACL — bare channel names, no scheme.
    #[serde(default)]
    pub publish_acl: Vec<crate::access::raw::ChannelMatcherRaw>,
    /// Ephemeral (`ephemeral:`) subscribe ACL — bare channel names, no scheme.
    #[serde(default)]
    pub ephemeral_subscribe_acl: Vec<crate::access::raw::ChannelMatcherRaw>,
    /// Ephemeral (`ephemeral:`) publish ACL — bare channel names, no scheme.
    #[serde(default)]
    pub ephemeral_publish_acl: Vec<crate::access::raw::ChannelMatcherRaw>,
    /// Component modules to mount on this surface (`[[surface.component]]`).
    #[serde(default, rename = "component")]
    pub components: Vec<SurfaceComponentRaw>,
    /// Static channel→port input bindings (`[[surface.subscription]]`).
    #[serde(default, rename = "subscription")]
    pub subscriptions: Vec<SurfaceSubscriptionRaw>,
    /// Static port→channel output bindings (`[[surface.output]]`).
    #[serde(default, rename = "output")]
    pub outputs: Vec<SurfaceOutputRaw>,
    /// Combined input+output port declarations (`[[surface.io_port]]`) — the
    /// self-loop, made structural. See [`SurfaceIoPortRaw`].
    #[serde(default, rename = "io_port")]
    pub io_ports: Vec<SurfaceIoPortRaw>,
    /// Skin (CSS pack + vendored fonts) this surface wears. Absent ⇒ `"bench"`.
    /// Validated at resolution against the compiled-in skin registry; an unknown
    /// name is a boot panic.
    #[serde(default)]
    pub skin: Option<String>,
    /// Usernames permitted to attach. Empty/absent = any authenticated user
    /// (mirrors `AppConfig::user_has_access`). Resolution rejects empty strings
    /// and duplicates.
    #[serde(default)]
    pub allowed_users: Vec<String>,
    /// Per-connection publish burst (tokens). Absent =
    /// `DEFAULT_SURFACE_PUBLISH_BURST`. Resolution rejects `0` and any value
    /// above the bus per-sender burst (`EPHEMERAL_SENDER_BURST`): the
    /// per-connection bucket must trip no later than the bus gate, so the
    /// documented "connection bucket trips first" layering cannot invert. That
    /// layering is per-connection only — all sessions of a surface share the one
    /// `surface:<slug>` bus participant and its single gate (shared-fate).
    #[serde(default)]
    pub publish_burst: Option<u32>,
    /// Per-connection sustained publish refill (tokens/sec). Absent =
    /// `DEFAULT_SURFACE_PUBLISH_PER_SEC`. Resolution rejects `0` and any value
    /// above the bus per-sender refill (`EPHEMERAL_SENDER_REFILL_AMOUNT`/s), for
    /// the same layering reason as `publish_burst`.
    #[serde(default)]
    pub publish_per_sec: Option<u32>,
}

/// A component module to mount on a surface (`[[surface.component]]`).
#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct SurfaceComponentRaw {
    /// Component module kind to mount. Must match `^[a-z0-9][a-z0-9-]*$` — the
    /// kind becomes a custom-element name (`brenn-<kind>`) and module filename.
    /// Several instances may share one kind (one wasm module, N elements).
    pub kind: String,
    /// Instance id: the routing/mount key that bindings reference. Absent ⇒
    /// defaults to `kind` (single-instance ergonomics). Must match the same
    /// charset as `kind` and be unique within the surface (enforced at
    /// resolution).
    #[serde(default)]
    pub instance: Option<String>,
    /// Which artifact shape backs this instance (`"dom"`, `"processor"`,
    /// `"dom-ts"`, `"html"`) — a build/loading fact the page must not have to
    /// guess, never a statement about what the component may reach. Required:
    /// the operator states the artifact shape explicitly, exactly as they state
    /// grants, because a default would silently pick a toolchain for them.
    /// Resolution rejects any value the shell cannot load.
    pub abi: String,
    /// Override for this instance's durable send-budget burst: how many
    /// publishes it may make back-to-back before the refill rate binds. Absent ⇒
    /// [`publish::SURFACE_SEND_BURST`].
    #[serde(default)]
    pub send_burst: Option<u32>,
    /// Override for this instance's durable send-budget refill interval, in
    /// seconds: one publish's worth of budget returns per interval. Absent ⇒
    /// [`publish::SURFACE_SEND_REFILL`].
    #[serde(default)]
    pub send_refill_secs: Option<u64>,
    /// How many of this instance's activation flushes the kernel parks while the
    /// link is down, before the oldest is dropped. Absent ⇒
    /// [`DEFAULT_PARKED_BATCH_DEPTH`].
    ///
    /// Activations continue while disconnected (page-local delivery and timers
    /// need no websocket), so their flushes are a queue like every other and
    /// take the same overflow model: drop-oldest, counted. What drops is a
    /// **whole batch** — one activation's flush is atomic, so it goes whole or
    /// not at all.
    ///
    /// Resolution rejects `0` (an instance whose every offline flush is dropped
    /// on arrival is dead config, not a bound) and unbounded (the parked queue
    /// is page memory; "unbounded" is a tab that grows for the length of the
    /// outage). This knob is also what bounds the reconnect burst — cap ×
    /// per-activation quota, rather than an outage-length backlog the
    /// server-side bucket would mass-reject anyway.
    #[serde(default)]
    pub parked_batch_depth: Option<Depth>,
    /// Marks this instance as the surface's chrome component: the singleton that
    /// owns layout/theme/takeover/banner/toast rendering and that the kernel
    /// treats specially (connect-indicator handoff, death-is-fatal). Exactly one
    /// component per surface must set it: resolution panics (naming the surface
    /// and the offending count) on zero or two-or-more, so the singleton
    /// invariant is enforced at boot.
    /// Default false keeps the flag opt-in and out-of-tree-chrome first-class —
    /// the designation is this flag, not the kind string.
    #[serde(default)]
    pub chrome: bool,
    /// Static key/value configuration handed to a `processor` instance and read
    /// through its `config` import. Absent ⇒ empty map.
    ///
    /// Processor ABI only: a `config` table on any other ABI is a config-time
    /// panic, because nothing would ever read it. Keys must not start with
    /// `brenn.`, which is the host-reserved namespace.
    ///
    /// **Confidentiality:** this map is carried in the surface's bindings
    /// document, a retained message on the surface's ephemeral config channel.
    /// It is therefore readable by every authenticated page session of the
    /// surface *and* by any principal the operator grants an ephemeral-subscribe
    /// matcher covering that channel — read access is ordinary deny-by-default
    /// ACL policy, with no further guard. It is operator configuration, not a
    /// secret store — never place credentials or secrets in it.
    #[serde(default)]
    pub config: Option<BTreeMap<String, String>>,
}

/// Default parked-batch depth when `parked_batch_depth` is unset: eight
/// activation flushes held per instance across a disconnect.
///
/// Sized for the case the bound exists for — a component that keeps working
/// through a brief outage and wants its work to land on reconnect — not for
/// riding out a long one. Deep enough that a handful of activations during a
/// reconnect blip survive; shallow enough that the reconnect burst stays a
/// burst.
pub const DEFAULT_PARKED_BATCH_DEPTH: u64 = 8;

/// One surface component instance's durable send budget: a burst capacity that
/// refills one publish per interval, per principal.
///
/// The knob an operator tunes per `[[surface.component]]`, and the parameters
/// boot hands the instance's token bucket. The default is the pair of constants
/// this replaces at the finer grain ([`publish::SURFACE_SEND_BURST`] /
/// [`publish::SURFACE_SEND_REFILL`]), which were sized for one surface's whole
/// traffic and now bound one instance of it.
///
/// This is deliberately *not* the backend's `WasmSinkBudget` shape. That budget
/// is per-sink millitokens filled per activation, with input-amplification
/// grants; a surface's is a flat wall-clock refill, because the server does not
/// run the activations it would meter. What both hostings preserve is the
/// property — blast-radius scoping to one principal — not the mechanism.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SurfaceSendBudget {
    /// Bucket capacity in publishes; the bucket starts full.
    pub burst: u32,
    /// Wall-clock interval per one publish of refill. Never zero.
    pub refill: std::time::Duration,
}

impl Default for SurfaceSendBudget {
    fn default() -> Self {
        Self {
            burst: publish::SURFACE_SEND_BURST,
            refill: publish::SURFACE_SEND_REFILL,
        }
    }
}

/// One surface's principals, each with its resolved send budget: `None` is the
/// surface's kernel grain, `Some(instance)` a declared component instance.
///
/// Produced by [`ResolvedSurface::principal_send_budgets`] and consumed by
/// `Messenger::with_surface_send_budgets`. Named because the shape travels
/// between boot and the installer and appears in every fixture that stands one
/// up — one name means a reader learns the pair grain once.
pub type SurfacePrincipalBudgets = Vec<(Option<String>, SurfaceSendBudget)>;

/// A static input binding on a surface (`[[surface.subscription]]`).
///
/// `channel` is a **full scheme-qualified address** (`ephemeral:protobar-demo`,
/// `brenn:alerts.high`) — the scheme selects the delivery class, unlike the
/// bare-name ACL matcher values.
#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct SurfaceSubscriptionRaw {
    /// Full scheme-qualified channel address to subscribe.
    ///
    /// `None` makes this a **free port**: exactly one `[[connection]]` must
    /// reference it (as `surface:<slug>#<instance>/<port>`) to supply the
    /// channel. Bound by no connection, it is dead config and a boot panic.
    #[serde(default)]
    pub channel: Option<String>,
    /// Declared component instance receiving deliveries on this binding.
    pub instance: String,
    /// Logical input port name presented to the component.
    pub port: String,
    /// This binding's queue depth. `None` ⇒ inherit the channel's own rung on
    /// `brenn:`/`ephemeral:`. Required on `local:`, which has no `[[channel]]`
    /// block to read a rung from.
    ///
    /// Applies to **every** delivery class, because every class puts a bounded
    /// queue in front of the port: the page's per-port fan-out queue. A `brenn:`
    /// binding's value governs the server-side push-row depth too — one knob,
    /// applied at each queue on the binding's path.
    ///
    /// Must resolve bounded and `>= 1` on any class. Unbounded is rejected: the
    /// page queue lives in browser memory, so "unbounded" is a tab that grows
    /// until it dies, not a policy. Zero is rejected because surfaces have no
    /// pull API, so a pull-only binding could never deliver.
    pub push_depth: Option<Depth>,
    /// Per-subscription retain depth. `None` ⇒ inherit the channel's rung on
    /// `brenn:`; on `local:` it gives the page ring's floor of 1, the one
    /// structural default the bus keeps (the kernel holds that ring whether or
    /// not a binding asks).
    ///
    /// Durable (`brenn:`) and `local:` bindings only: on `brenn:` it is the
    /// server's retained-replay window, on `local:` it feeds the page-local
    /// channel's ring depth. An `ephemeral:` binding has no per-binding
    /// retention to speak of (the ephemeral bus's retained ring is the
    /// channel's own `retain_depth`), so setting it there is a boot panic.
    pub retain_depth: Option<Depth>,
    /// Per-subscription noise level for push overflow. `None` ⇒ inherit.
    /// Durable (`brenn:`) bindings only in this build — the surface-side noise
    /// ladder does not exist yet, so setting it on an `ephemeral:`/`local:`
    /// binding is a boot panic rather than a silently-ignored knob.
    pub noise: Option<NoiseLevel>,
    /// Rejected: surface subscriptions are always delivered eagerly, so `wake_min`
    /// has no meaning here. The field exists only so an explicit setting produces a
    /// clear config error (rather than a generic unknown-field error) pointing the
    /// operator away from a knob that would do nothing. Setting it to any value is a
    /// boot config error.
    pub wake_min: Option<WakeMin>,
}

/// A static output binding on a surface (`[[surface.output]]`).
///
/// `channel` is a **full scheme-qualified address**, as in
/// `SurfaceSubscriptionRaw`.
#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct SurfaceOutputRaw {
    /// Declared component instance publishing on this binding.
    pub instance: String,
    /// Logical output port name the component publishes to.
    pub port: String,
    /// Full scheme-qualified channel address the port publishes onto.
    ///
    /// `None` makes this a **free port**, bound by exactly one `[[connection]]`;
    /// see [`SurfaceSubscriptionRaw::channel`].
    #[serde(default)]
    pub channel: Option<String>,
    /// Default urgency for messages published on this port (port → `normal`),
    /// mirroring `[[wasm_consumer]] [[output]] urgency`. Components override it
    /// per-message on the publish call; absent there, this applies.
    ///
    /// A surface has no `[[channel]]`-level urgency rung to inherit from — the
    /// backend's own ladder for this knob is port → global default — so the
    /// resolution is one step.
    pub urgency: Option<super::Urgency>,
    /// Token-bucket fill per activation for this output sink. `None` ⇒
    /// [`DEFAULT_WASM_PUBLISH_PER_ACTIVATION`] (1.0). `0` = purely input-driven
    /// (only the per-input amplification grant feeds this sink). Must be finite
    /// and `>= 0` when present.
    ///
    /// Same knob, spelling, and semantics as `[[wasm_consumer.output]]`'s: the
    /// kernel is the host that mints this component's activations, so it runs
    /// the backend host's budget model over them rather than mirroring a foreign
    /// bucket. A component moved between hostings keeps its budget vocabulary.
    #[serde(default)]
    pub publish_per_activation: Option<f64>,
    /// Max tokens carried over between activations for this output sink (the
    /// bucket capacity clamp applied at the *start* of the next activation).
    /// `None` ⇒ [`DEFAULT_WASM_PUBLISH_CAPACITY`] (1.0). Must be finite and
    /// `>= 0` when present. Same knob as `[[wasm_consumer.output]]`'s.
    #[serde(default)]
    pub publish_capacity: Option<f64>,
}

/// A combined input+output port on a surface-hosted component
/// (`[[surface.io_port]]`).
///
/// The surface twin of [`WasmConsumerIoPortRaw`]: one port name resolving to one
/// input binding and one output binding on the *same* channel, so a component
/// sees its own publishes there by construction.
///
/// The default (no `channel`, no `[[connection]]`) is a **page-local** channel —
/// per-session, browser-side, never on the server ring — riding the same
/// declared-by-bindings `local:` machinery as any `local:` surface binding.
///
/// The timer idiom runs on these: a component parks its next tick on its own
/// io_port with a deferred publish, and the tick arrives as an ordinary message
/// on the port's input half. The activation carries the instant to compute the
/// release time from, and the standing tick can be cancelled or edited from a
/// later activation.
#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct SurfaceIoPortRaw {
    /// Declared component instance owning both halves of this port.
    pub instance: String,
    /// Logical port name presented to the component for both directions. Must be
    /// distinct from every other port name the instance declares in either
    /// direction.
    pub port: String,
    /// Absent ⇒ the port's own anonymous page-local channel. Present ⇒ a named
    /// auto channel at this full scheme-qualified address. Must be absent when a
    /// `[[connection]]` lists this port.
    ///
    /// A `local:` name here stays page-local: other bindings *on this surface*
    /// join the one page ring, and nothing on the server can. Reaching the
    /// backend takes `ephemeral:` (shared across this surface's sessions) or
    /// `brenn:` (durable as well).
    #[serde(default)]
    pub channel: Option<String>,
    /// Queue depth for the input half. Required, in either realm: the port
    /// queue lives in page memory and must resolve bounded, and in the server
    /// realm — a named `ephemeral:`/`brenn:` address, or membership in a
    /// wire-spanning `[[connection]]` — it also feeds the channel's depth fold.
    pub push_depth: Option<Depth>,
    /// Retain depth for the input half. Required, and in the server realm it
    /// feeds the auto channel's depths via the same fold as
    /// [`WasmConsumerIoPortRaw::retain_depth`], where an unbounded value
    /// refuses to boot on a non-durable channel. Page-local there is no server
    /// entry and no fold — the number is this port's own page ring.
    pub retain_depth: Option<Depth>,
    /// Noise level for push overflow on the input half. `None` ⇒ inherit. Same
    /// class restrictions as [`SurfaceSubscriptionRaw::noise`].
    pub noise: Option<NoiseLevel>,
    /// Default urgency for messages published on the output half. Same semantics
    /// as [`SurfaceOutputRaw::urgency`].
    pub urgency: Option<super::Urgency>,
    /// Token-bucket fill per activation for the output half. `None` ⇒
    /// [`DEFAULT_WASM_PUBLISH_PER_ACTIVATION`].
    #[serde(default)]
    pub publish_per_activation: Option<f64>,
    /// Max tokens carried between activations for the output half. `None` ⇒
    /// [`DEFAULT_WASM_PUBLISH_CAPACITY`].
    #[serde(default)]
    pub publish_capacity: Option<f64>,
}

// ---------------------------------------------------------------------------
// Resolved config
// ---------------------------------------------------------------------------

/// Resolved per-app messaging config, attached to `AppConfig`.
///
/// The legacy `enabled` authorization boolean was removed alongside
/// `MessagingConfigRaw::enabled` (access-control design §2.5.1 / §8 decision-2):
/// messaging authorization is now decided by the app's `AppPolicy`
/// (`AppConfig::messaging_enabled()`), not by a field on this struct.
#[derive(Debug, Clone)]
pub struct ResolvedMessagingConfig {
    pub send_budget: u32,
    pub subscriptions: Vec<ResolvedSubscription>,
}

/// Fully-resolved per-subscription config (inheritance already applied).
#[derive(Debug, Clone)]
pub struct ResolvedSubscription {
    pub channel_uuid: Uuid,
    /// Canonical `brenn:...` form.
    pub channel_address: String,
    /// Resolved push depth (sub → channel → global).
    pub push_depth: Depth,
    /// Resolved retain depth (sub → channel → global).
    pub retain_depth: Depth,
    /// Resolved noise level (sub → channel → global).
    pub noise: NoiseLevel,
    /// Resolved wake-min policy (sub → channel → global).
    ///
    /// Determines at which urgency level this subscriber is eagerly woken.
    /// `WakeMin::Never` = rows park until the subscriber's next natural drain.
    /// Only meaningful when `push_depth > 0`; on pull-only subscriptions this
    /// field is present (for config inheritance simplicity) but `insert_pushes`
    /// never creates push rows for them, so `wake_min` has no effect.
    pub wake_min: WakeMin,
}

impl ResolvedSubscription {
    /// True iff this subscription is push-enabled (push_depth > 0 or Unbounded).
    pub fn is_push_enabled(&self) -> bool {
        self.push_depth.is_push_enabled()
    }
}

/// Fully-resolved per-channel config (held on `ChannelEntry`).
///
/// Deliberately has no `Default`: there is no defensible depth to default to.
/// Every channel's depths come from an explicit `[[channel]]` statement, a
/// documented structural derivation (the auto-channel fold), or a bounded
/// per-family constant for a system-minted channel — never from a blanket
/// fallback that would size retention without anyone deciding to.
#[derive(Debug, Clone)]
pub struct ResolvedChannel {
    /// Channel-level default push depth (used as subscriber-inheritance template).
    pub push_depth: Depth,
    /// Channel-level default retain depth.
    pub retain_depth: Depth,
    /// Subscriber-independent retained buffer.
    pub standing_retain_depth: Depth,
    /// Noise default for this channel.
    pub noise: NoiseLevel,
    /// Eviction sink for this channel.
    pub sink: Sink,
    /// Channel-level wake-min default (used as subscriber-inheritance template).
    pub wake_min: WakeMin,
    /// Per-`(sender, this channel)` send-rate gate applied to every publish.
    pub send_rate: SendRate,
}

/// Resolved millitoken budget knobs for one WASM egress sink (output port or MQTT
/// client). `f64` config knobs are converted to integer millitokens at resolve
/// time (`MILLITOKENS_PER_PUBLISH` scale) so runtime enforcement is integer-only.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WasmSinkBudget {
    /// Bucket fill per activation, in millitokens.
    pub fill_mt: u64,
    /// Max carryover between activations, in millitokens (clamp applied at the
    /// start of the next activation).
    pub capacity_mt: u64,
}

/// Resolved per-input port for a WASM consumer.
#[derive(Debug, Clone)]
pub struct WasmInputPort {
    /// Logical input port name presented to the guest (from config).
    pub port: String,
    /// Fully resolved subscription.
    pub sub: ResolvedSubscription,
    /// Publish amplification factor in millitokens: each new envelope on this input
    /// grants this many millitokens to every egress sink's bucket
    /// (`MILLITOKENS_PER_PUBLISH` scale). Resolved from `amplification` (default 1.0
    /// ⇒ 1000).
    pub amplification_mt: u64,
}

/// Resolved per-output port for a WASM consumer.
#[derive(Debug, Clone)]
pub struct WasmOutputPort {
    /// Logical output port name.
    pub port: String,
    /// Resolved channel UUID for dispatch use.
    pub channel_uuid: uuid::Uuid,
    /// Canonical channel address (e.g. `brenn:my-output`).
    pub channel_address: String,
    /// Default urgency for messages published on this port (from config, default `Normal`).
    /// Guests may override per-message via `publish-with-urgency`.
    pub default_urgency: super::Urgency,
    /// Resolved per-sink publish token-bucket budget for this output port
    /// (`publish_per_activation` / `publish_capacity`, converted to millitokens).
    pub budget: WasmSinkBudget,
}

/// Fully resolved `[[wasm_consumer]]` block, ready for use by bootstrap and dispatch.
#[derive(Debug, Clone)]
pub struct ResolvedWasmConsumer {
    pub slug: String,
    pub component_path: PathBuf,
    /// Granted capability interfaces for this component (deny-by-default).
    /// Determines which host functions are linked at component load time.
    pub grants: BTreeSet<WasmGrant>,
    /// Path to the per-component SQLite KV store. `Some` iff `Store` is in
    /// `grants` (config layer enforces the invariant).
    pub store_path: Option<PathBuf>,
    /// Maximum SQLite page count (computed from size limit). Always present
    /// (computed from global default even when `store_path` is `None`), but
    /// unused when no store is linked.
    pub max_page_count: u32,
    pub inputs: Vec<WasmInputPort>,
    pub outputs: Vec<WasmOutputPort>,
    /// Operator-supplied config map for this component (from `[wasm_consumer.config]`).
    /// Empty map when no config table is present.
    pub config: std::collections::HashMap<String, String>,
    /// Resolved access-control policy (grants + ACLs) for this component, built
    /// from its `grants` + `subscribe_acl`/`publish_acl` config. Unused in
    /// Phase 0-1 (WASM enforcement is Phase 3); built now so the policy model
    /// spans both app kinds. See `crate::access::AppPolicy`.
    pub policy: crate::access::AppPolicy,
    /// Per-component activation pacing (defaults applied). Always present; the
    /// off-loop dispatch task builds its `TokenBucket` from this
    /// (mqtt-wasm-republish-pacing design §3.2).
    pub activation_pacing: ActivationPacing,
    /// Resolved MQTT egress sink budgets, keyed by `[[mqtt_client]]` slug. One entry
    /// per client the component's `mqtt_publish_acl` allows; empty when the consumer
    /// has no MQTT publish ACL. `[[wasm_consumer.mqtt_output]]` blocks override the
    /// per-client budget; ACL-allowed clients without a block get the defaults.
    pub mqtt_sinks: std::collections::HashMap<String, WasmSinkBudget>,
}

/// Fully resolved `[[surface]]` block, carried for later consumers.
///
/// Populated by `resolve_surfaces` after boot-time
/// cross-validation. Carried on `MessagingResult` alongside `wasm_consumers`.
/// One resolved mounted component instance: its routing/mount `instance` id and
/// the component `kind` that backs it. Several instances may share a kind (one
/// wasm module, N elements).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedComponent {
    /// Routing/mount key that bindings reference. Defaults to `kind` when the
    /// config omits it.
    pub instance: String,
    /// Component module kind: the custom-element tag and wasm module.
    pub kind: String,
    /// Resolved artifact shape, validated at boot against the ABIs the shell can
    /// actually load. Carried to the page in the bindings document.
    pub abi: brenn_surface_schema::Abi,
    /// This instance's durable send budget: its own declared override, or the
    /// defaults. Server-side only — the page is told nothing about it, because
    /// the server is the authority and a mirrored bucket has no reader yet.
    pub send_budget: SurfaceSendBudget,
    /// How many activation flushes the kernel parks for this instance while the
    /// link is down before dropping the oldest whole batch. Bounded and `>= 1`.
    /// Carried to the page in the bindings document: the parked queue is the
    /// kernel's, so unlike `send_budget` this number has a page-side enforcer.
    pub parked_batch_depth: u64,
    /// Whether this instance is the surface's chrome component. Resolved from the
    /// component's `chrome` flag; the server names the chrome instance to the
    /// page in the bindings document's `chrome_instance`.
    pub chrome: bool,
    /// This instance's static config map, served to the component through its
    /// `config` import. Empty for every non-`processor` ABI.
    ///
    /// **Confidentiality:** carried in the surface's retained bindings
    /// document — operator configuration only, never secrets.
    pub config: BTreeMap<String, String>,
}

/// A resolved `local:` channel: a page-local pub/sub channel the surface's own
/// router owns end-to-end.
///
/// Local channels are declared *per surface* — they name page-local wiring, not
/// directory entries, so they have no `[[channel]]` block and the server's
/// channel directory never learns of them. The bindings in
/// `ResolvedSurface.subscriptions`/`outputs` *are* the declaration; this carries
/// the per-channel parameters derived from them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedLocalChannel {
    /// Full `local:` channel address.
    pub address: String,
    /// Retained-ring depth for the page-local router: how many most-recent
    /// messages it replays to a port on attach. Bounded — the ring is page
    /// memory.
    pub ring_depth: u64,
}

#[derive(Debug, Clone)]
pub struct ResolvedSurface {
    /// `surface:<slug>` participant identity source.
    pub slug: String,
    /// Skin (CSS pack + vendored fonts) this surface wears; resolved default is
    /// `"bench"`, validated against the compiled-in registry.
    pub skin: String,
    /// Declared component instances, in declaration order.
    pub components: Vec<ResolvedComponent>,
    /// Resolved input bindings (channel → component/port). Serves both delivery
    /// classes and the bindings document.
    pub subscriptions: Vec<SurfaceBinding>,
    /// Resolved **transportable** input subscriptions, one per (instance,
    /// channel) pair the surface's `brenn:` and `ephemeral:` bindings name.
    /// `local:` bindings never appear here — that traffic never crosses the
    /// wire.
    pub wire_subscriptions: Vec<ResolvedSurfaceSubscription>,
    /// Every distinct `local:` channel this surface's bindings name, with the
    /// ring depth resolved from them. Deduped, in first-binding order. Carried
    /// to the client in the bindings document: the page-local router is the sole
    /// source of truth for this traffic, so these channels exist nowhere else
    /// server-side.
    pub local_channels: Vec<ResolvedLocalChannel>,
    /// Resolved output bindings (component/port → channel), each carrying its
    /// resolved default publish urgency.
    pub outputs: Vec<SurfaceOutput>,
    /// Resolved access-control policy (grants + ACLs) for this surface,
    /// built via `build_surface_policy`.
    pub policy: crate::access::AppPolicy,
    /// Usernames permitted to attach. Empty = any authenticated user.
    pub allowed_users: Vec<String>,
    /// Per-connection publish token-bucket burst (tokens), defaults applied.
    pub publish_burst: u32,
    /// Per-connection publish token-bucket sustained refill (tokens/sec),
    /// defaults applied.
    pub publish_per_sec: u32,
}

impl ResolvedSurface {
    /// Whether `username` may attach to this surface. Empty `allowed_users`
    /// admits any authenticated user (same semantics as `AppConfig`).
    pub fn user_has_access(&self, username: &str) -> bool {
        self.allowed_users.is_empty() || self.allowed_users.iter().any(|u| u == username)
    }

    /// Every principal's send budget: the kernel grain (`None`, always the
    /// defaults — it has no declaration to override them) followed by one per
    /// declared component instance, in declaration order.
    ///
    /// The single authority for the principal set. Boot installs buckets from
    /// exactly this, so the budget map covers the same principals the
    /// sub-identity derivation admits — the invariant the publish gate panics on
    /// a miss to protect. Subscriber registrations and delivery routes are cut
    /// at the surface rather than the principal (a component's authority is the
    /// surface's), so this is the one set that has to be enumerated.
    pub fn principal_send_budgets(
        &self,
    ) -> impl Iterator<Item = (Option<String>, SurfaceSendBudget)> + '_ {
        std::iter::once((None, SurfaceSendBudget::default())).chain(
            self.components
                .iter()
                .map(|c| (Some(c.instance.clone()), c.send_budget)),
        )
    }
}

/// One transportable surface subscription and the component instance that
/// declared it.
///
/// The instance is the *binding's* grain: a component's bindings resolve one
/// subscription per (instance, channel), each with its own resolved depths and
/// noise.
#[derive(Debug, Clone)]
pub struct ResolvedSurfaceSubscription {
    /// The component instance that declared this binding. Every surface
    /// subscription is an instance's; the bare `surface:<slug>` grain is
    /// publisher-only.
    pub instance: String,
    /// The resolved depth/noise/wake inheritance for this binding's
    /// subscription to this channel.
    pub subscription: ResolvedSubscription,
}

/// A resolved static surface input binding (channel → component/port).
#[derive(Debug, Clone)]
pub struct SurfaceBinding {
    /// Full scheme-qualified channel address (`ephemeral:`/`brenn:`/`local:`).
    pub channel_address: String,
    /// Declared component instance on this surface.
    pub instance: String,
    /// Logical port name on that instance.
    pub port: String,
    /// Resolved queue depth for this binding's page-side port queue: how many
    /// undelivered messages it holds before overflow policy applies. Bounded and
    /// `>= 1` on every class — resolution rejects anything else, since the queue
    /// is page memory.
    ///
    /// On a `brenn:` binding this is the same number the channel's
    /// `ResolvedSubscription` carries, so the server's push-row depth and the
    /// page's queue depth are one operator knob rather than two that can drift.
    pub push_depth: u64,
    /// Resolved context-window depth for this binding: how many of the
    /// subscription's most-recent messages precede `new_from` in the port's
    /// window. Bounded on every class — the ring is page memory.
    ///
    /// Per binding, not per subscription: two ports of one instance on one
    /// channel share a subscription (whose ring folds by max) but each windows
    /// at its own depth. Resolution is class-uniform binding → channel → global:
    /// `brenn:` and `ephemeral:` inherit from their `[[channel]]` block; `local:`
    /// channels have no surface-visible channel block and collapse to
    /// binding → global.
    pub retain_depth: u64,
    /// Resolved push-overflow noise level for this binding, class-uniform
    /// binding → channel → global. Held for the surface-side noise ladder that
    /// lands in a later phase — no surface path consumes it yet, exactly as the
    /// durable subscriber entry already carries an unread `noise`.
    pub noise: NoiseLevel,
}

/// A resolved static surface output binding (component/port → channel).
///
/// Distinct from `SurfaceBinding` because an output carries a knob an input has
/// no meaning for: the port's default publish urgency. Urgency is a property of
/// *sending* — it tells the bus how hard to work to wake a subscriber — so
/// there is nothing for an input binding to say about it.
#[derive(Debug, Clone)]
pub struct SurfaceOutput {
    /// Full scheme-qualified channel address (`ephemeral:`/`brenn:`/`local:`).
    pub channel_address: String,
    /// Declared component instance on this surface.
    pub instance: String,
    /// Logical port name on that instance.
    pub port: String,
    /// Resolved default urgency for publishes on this port (port → `normal`).
    /// A component's per-message override wins over it.
    pub default_urgency: super::Urgency,
    /// This sink's resolved per-activation token bucket, in millitokens.
    ///
    /// Enforced by the kernel, not the server: the kernel mints the activations
    /// this bucket refills per, so it is the only party that can meter them. The
    /// server resolves the numbers and states them in the bindings document — the
    /// kernel
    /// enforces resolved values and never re-derives config. The server's own
    /// per-instance send bucket ([`SurfaceSendBudget`]) is a separate,
    /// wall-clock tier behind this one.
    pub budget: brenn_budget::SinkBudget,
}

// ---------------------------------------------------------------------------
// Channel directory + per-app validation
// ---------------------------------------------------------------------------

/// Validate top-level `[[channel]]` blocks and build the directory of
/// channel entries (without subscribers — those are filled in after apps
/// resolve).
///
/// How a `[[channel]]` block is keyed: exactly one of the two.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChannelBlockKey<'a> {
    Address(&'a str),
    Prefix(&'a str),
}

/// The one classifier both passes over the `[[channel]]` table run, so a block
/// cannot be skipped by each of them in turn.
///
/// # Panics
///
/// If the block sets neither or both of `address` and `address_prefix`.
fn channel_block_key(ch: &ChannelConfigRaw) -> ChannelBlockKey<'_> {
    match (ch.address.as_deref(), ch.address_prefix.as_deref()) {
        (Some(address), None) => ChannelBlockKey::Address(address),
        (None, Some(prefix)) => ChannelBlockKey::Prefix(prefix),
        (Some(address), Some(prefix)) => panic!(
            "config: [[channel]] sets both address {address:?} and address_prefix \
             {prefix:?} — a block is keyed by one or the other",
        ),
        (None, None) => panic!(
            "config: [[channel]] block sets neither address nor address_prefix — a block \
             must name the channel it declares or the family it tunes",
        ),
    }
}

/// `standing_retain_depth` is the ceiling on every depth stated about a
/// channel: no `push_depth` or `retain_depth` anywhere — the channel's own
/// rungs, a static subscriber's, a dynamic subscriber's — may exceed it.
///
/// It is what the reaper keeps ([`ChannelEntry::reap_frontier`]), so a deeper
/// depth elsewhere would either be a promise the disk cannot keep or force the
/// effective retention above what the operator wrote. One number, in one place,
/// is the whole retention story for a channel. Raising a subscriber's reach
/// means raising the channel's standing depth — deliberately, in the block that
/// records the sizing decision.
///
/// The exception is the page realm: a `local:` binding has no server directory
/// entry to check against, and its queue is browser memory the kernel bounds
/// with its own contract-fixed rings.
///
/// # Panics
///
/// If either rung exceeds `standing`. `label` names the offending block.
fn assert_rungs_within_standing(push: Depth, retain: Depth, standing: Depth, label: &str) {
    assert!(
        push <= standing,
        "config: {label} push_depth {push:?} exceeds standing_retain_depth {standing:?} — \
         standing is the ceiling on every depth stated about a channel; raise \
         standing_retain_depth or lower push_depth",
    );
    assert!(
        retain <= standing,
        "config: {label} retain_depth {retain:?} exceeds standing_retain_depth {standing:?} — \
         standing is the ceiling on every depth stated about a channel; raise \
         standing_retain_depth or lower retain_depth",
    );
}

/// The channel-block form of [`assert_rungs_within_standing`].
fn assert_depths_within_standing(resolved: &ResolvedChannel, label: &str) {
    assert_rungs_within_standing(
        resolved.push_depth,
        resolved.retain_depth,
        resolved.standing_retain_depth,
        label,
    );
}

/// Check every subscriber in the assembled directory against its channel's
/// standing depth — the second half of the ceiling invariant
/// ([`assert_rungs_within_standing`] covers the channel's own rungs).
///
/// Runs at boot once all subscriber sources have contributed: config-authored
/// app/wasm/surface subscriptions, folded system-participant subscriptions, and
/// the auto-channel synthesis. Dynamic durable rows are not here — they merge
/// later and an over-ceiling one is classified dormant rather than fatal, since
/// it is runtime state an operator may have invalidated by tightening standing.
///
/// # Panics
///
/// Naming channel and subscriber, if any subscriber's `push_depth` or
/// `retain_depth` exceeds the channel's `standing_retain_depth`.
pub fn validate_subscriber_depth_ceilings(directory: &MessagingDirectory) {
    for entry in directory.list() {
        let standing = entry.resolved_channel.standing_retain_depth;
        for sub in &entry.subscribers {
            for (field, depth) in [
                ("push_depth", sub.push_depth),
                ("retain_depth", sub.retain_depth),
            ] {
                assert!(
                    depth <= standing,
                    "config: subscriber {:?} on channel {:?} has {field} {depth:?} exceeding \
                     the channel's standing_retain_depth {standing:?} — standing is the \
                     ceiling on every depth stated about a channel. Raise the channel's \
                     standing_retain_depth or lower the subscriber's depth. Refusing to \
                     start (fail-fast on invalid config).",
                    sub.kind,
                    entry.address,
                );
            }
        }
    }
}

/// One table, every pub/sub scheme: the address's scheme picks the channel's
/// capabilities and with them which knobs are meaningful. Durable and
/// non-durable entries come back in one vec, in declaration order; the caller
/// partitions them by [`ChannelEntry::capabilities`].
///
/// Blocks in the **tuning** role — addressed under `mqtt:`, `webhook:`, or the
/// tool namespaces, or keyed by `address_prefix` — mint no entry and are skipped
/// here; [`build_system_channel_tuning`] consumes them.
///
/// # Panics
///
/// - duplicate UUIDs or duplicate canonical addresses
/// - malformed or missing UUID on a durable channel; `uuid` present on a
///   non-durable one
/// - a `pwa_push:` address (an egress adapter, not a pub/sub channel) or an
///   `auto` address (owned by the auto-channel machinery)
/// - address fails RFC 3986 unreserved charset (`is_unreserved_char`)
/// - a missing `push_depth` or `retain_depth`, or a missing
///   `standing_retain_depth` on a durable channel
/// - `standing_retain_depth` or `sink` on a non-durable channel
/// - `retain_depth` of `Unbounded` on a non-durable channel
pub fn build_channel_entries(
    raw_channels: &[ChannelConfigRaw],
    defaults: &MessagingGlobalConfig,
) -> Vec<ChannelEntry> {
    // The global send rate is inherited by every channel that omits a
    // per-channel override, so it is validated once here, unconditionally.
    defaults
        .default_send_rate
        .validate("[messaging].default_send_rate");

    let mut seen_uuids = HashSet::new();
    let mut seen_addresses = HashSet::new();
    let mut entries = Vec::with_capacity(raw_channels.len());

    for ch in raw_channels {
        let ChannelBlockKey::Address(raw_address) = channel_block_key(ch) else {
            continue;
        };
        if channel_block_role(raw_address) == ChannelBlockRole::Tuning {
            continue;
        }
        let (scheme, name) = split_channel_address(raw_address);
        assert!(
            !name.is_empty(),
            "config: [[channel]] address {raw_address:?} must name a channel after its scheme",
        );
        assert!(
            name.chars().all(is_unreserved_char),
            "config: [[channel]] address {raw_address:?} must consist of RFC 3986 \
             unreserved characters only (A-Za-z0-9._~-) after its scheme",
        );
        assert!(
            !is_reserved_channel_name(name),
            "config: [[channel]] address {raw_address:?} is in a reserved namespace \
             (auto is owned by the auto-channel machinery; the tool namespaces exist \
             only on brenn:, where a block addressing one tunes it instead)",
        );

        let capabilities = scheme
            .capabilities()
            .expect("split_channel_address admits only pub/sub schemes");
        let canonical = format!("{}{name}", scheme.prefix());
        assert!(
            seen_addresses.insert(canonical.clone()),
            "config: duplicate [[channel]] address {canonical:?}",
        );

        let uuid = if capabilities.durable {
            let raw_uuid = ch.uuid.as_deref().unwrap_or_else(|| {
                panic!(
                    "config: [[channel]] {canonical:?} requires a uuid \
                     (it names the channel's DB row)"
                )
            });
            Uuid::parse_str(raw_uuid).unwrap_or_else(|e| {
                panic!("config: [[channel]] uuid {raw_uuid:?} is not a valid UUID: {e}")
            })
        } else {
            assert!(
                ch.uuid.is_none(),
                "config: [[channel]] {canonical:?} must not set uuid — a non-durable \
                 channel has no DB row and derives its identity from its address",
            );
            nondurable_channel_uuid(scheme, name)
        };
        assert!(
            seen_uuids.insert(uuid),
            "config: duplicate [[channel]] uuid {uuid} (address {canonical:?})",
        );

        // Class-uniform: every scheme states its own depths and inherits
        // channel → global for noise and wake_min. A depth has no global rung —
        // sizing the window is the decision this block exists to record.
        let push_depth = ch.push_depth.unwrap_or_else(|| {
            panic!(
                "config: [[channel]] {canonical:?} requires push_depth — how many unseen \
                 messages one activation hands over is a sizing decision, not a default"
            )
        });
        let retain_depth = ch.retain_depth.unwrap_or_else(|| {
            panic!(
                "config: [[channel]] {canonical:?} requires retain_depth — the retained \
                 window is sized for the outage it must survive, not defaulted"
            )
        });
        let noise = ch.noise.unwrap_or(defaults.default_noise);
        let wake_min = ch.wake_min.unwrap_or(defaults.default_wake_min);
        let send_rate = ch.send_rate.unwrap_or(defaults.default_send_rate);
        send_rate.validate(&format!("[[channel]] {canonical:?}"));

        let (standing_retain_depth, sink) = if capabilities.durable {
            (
                ch.standing_retain_depth.unwrap_or_else(|| {
                    panic!(
                        "config: [[channel]] {canonical:?} requires standing_retain_depth — \
                         it is the reaper's disk frontier and bounds what the channel keeps \
                         for subscribers that do not exist yet"
                    )
                }),
                ch.sink.unwrap_or(defaults.default_sink),
            )
        } else {
            assert!(
                ch.standing_retain_depth.is_none(),
                "config: [[channel]] {canonical:?} must not set standing_retain_depth — \
                 it is the durable reaper's frontier; a non-durable channel's retention \
                 is retain_depth alone",
            );
            assert!(
                ch.sink.is_none(),
                "config: [[channel]] {canonical:?} must not set sink — a non-durable \
                 channel evicts from memory and has nothing to archive",
            );
            assert!(
                retain_depth != Depth::Unbounded,
                "config: [[channel]] {canonical:?} retain_depth must be bounded — \
                 non-durable retention is process memory; give it a number",
            );
            // The standing buffer is the retained window itself: there is no
            // separate subscriber-independent store off-disk.
            (retain_depth, Sink::Drop)
        };

        let resolved = ResolvedChannel {
            push_depth,
            retain_depth,
            standing_retain_depth,
            noise,
            sink,
            wake_min,
            send_rate,
        };
        assert_depths_within_standing(&resolved, &format!("[[channel]] {canonical:?}"));

        // Fail fast if archive sink configured but no archive_path set.
        if resolved.sink == Sink::Archive && defaults.archive_path.is_none() {
            panic!(
                "config: [[channel]] {canonical:?} has sink = \"archive\" but \
                 [messaging].archive_path is not set",
            );
        }

        entries.push(ChannelEntry {
            uuid,
            address: canonical,
            description: ch.description.clone(),
            resolved_channel: resolved,
            subscribers: vec![],
            transport_type: scheme,
            mount: None,
        });
    }

    entries
}

/// Split a `[[channel]]` address into its scheme and bare name, defaulting a
/// bare address to `brenn:`.
///
/// # Panics
///
/// On a scheme that declares nothing: `pwa_push:` is an egress adapter, not a
/// pub/sub channel. `mqtt:`/`webhook:` addresses never reach here — they are
/// tuning blocks, filtered out by the caller.
fn split_channel_address(address: &str) -> (ChannelScheme, &str) {
    match ChannelScheme::split(address) {
        Some((
            scheme @ (ChannelScheme::Brenn | ChannelScheme::Ephemeral | ChannelScheme::Local),
            name,
        )) => (scheme, name),
        Some((scheme, _)) => panic!(
            "config: [[channel]] address {address:?} uses scheme {:?}, which declares \
             nothing — pwa_push: is an egress adapter, not a pub/sub channel",
            scheme.prefix(),
        ),
        None => {
            assert!(
                !address.contains(':'),
                "config: [[channel]] address {address:?} has an unrecognized scheme",
            );
            (ChannelScheme::Brenn, address)
        }
    }
}

/// Inheritance rung for subscription-param resolution.
///
/// `resolve_subscription_params` resolves each omitted (`None`) raw param against
/// the matching field here. Every caller fills it from the target channel's
/// `ResolvedChannel`, so the ladder is always sub → channel: a `[[channel]]`
/// block for the operator-declared schemes, and [`resolve_system_channel`] for
/// the system-minted ones (`mqtt:`, `webhook:`, tool channels), which no
/// `[[channel]]` block mints.
#[derive(Debug, Clone, Copy)]
pub struct SubscriptionParamDefaults {
    pub push_depth: Depth,
    pub retain_depth: Depth,
    pub noise: NoiseLevel,
    pub wake_min: WakeMin,
}

impl SubscriptionParamDefaults {
    /// Rung built from a channel's resolved config (`brenn:`/`webhook:`).
    pub fn from_channel(ch: &ResolvedChannel) -> Self {
        Self {
            push_depth: ch.push_depth,
            retain_depth: ch.retain_depth,
            noise: ch.noise,
            wake_min: ch.wake_min,
        }
    }
}

// ---------------------------------------------------------------------------
// System-minted channels: family defaults and `[[channel]]` tuning
// ---------------------------------------------------------------------------

/// Push depth every system-minted channel family defaults to.
///
/// The channel-level push rung is near-inert on these families: MQTT and webhook
/// app subscriptions state their own depths, and a runtime dynamic subscribe
/// states both. One is the honest floor for what is left.
pub const SYSTEM_CHANNEL_DEFAULT_PUSH_DEPTH: Depth = Depth::Bounded(1);

/// Retained window webhook and MQTT ingress channels default to.
///
/// Ingress channels are fact channels: at their arrival rates a hundred messages
/// is a horizon of days to months, which is what sizing for the outage you
/// intend to survive means here.
pub const INGRESS_DEFAULT_RETAIN_DEPTH: Depth = Depth::Bounded(100);

/// Retained window the async-tool request and result channels default to.
///
/// Their executor and consumers are in-process and eager, so the window covers a
/// burst arriving while the executor is busy, not a multi-day outage.
pub const TOOL_CHANNEL_DEFAULT_RETAIN_DEPTH: Depth = Depth::Bounded(16);

/// Is `name` (a scheme-stripped channel name) inside a tool namespace the
/// substrate actually mints into?
///
/// Narrower than [`crate::tools::is_reserved_channel`], which also reserves the
/// `.` boundary form (`tools.mine`) against squatting. Only the `/` form names a
/// channel that exists, so only it can be tuned; the rest stays rejected.
fn in_a_tool_namespace(name: &str) -> bool {
    crate::tools::RESERVED_CHANNEL_SEGMENTS
        .iter()
        .any(|seg| name.starts_with(&format!("{seg}/")))
}

/// The families of channel the system mints for itself, each with its own
/// bounded default window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SystemChannelFamily {
    /// `webhook:<slug>` and `mqtt:<client>:<topic>` — inbound facts from outside.
    Ingress,
    /// `brenn:tools/<tool>` and `brenn:tool-results/<slug>` — the async-tool
    /// request/result substrate.
    Tool,
}

impl SystemChannelFamily {
    /// The family this address belongs to, or `None` if the address is an
    /// operator-declared channel rather than a system-minted one.
    pub fn of(address: &str) -> Option<Self> {
        match ChannelScheme::split(address) {
            Some((ChannelScheme::Mqtt | ChannelScheme::Webhook, _)) => Some(Self::Ingress),
            Some((ChannelScheme::Brenn, name)) if in_a_tool_namespace(name) => Some(Self::Tool),
            Some(_) => None,
            // A bare (scheme-less) address canonicalizes to `brenn:`.
            None if in_a_tool_namespace(address) => Some(Self::Tool),
            None => None,
        }
    }

    /// The family's default retained window, which is also its default standing
    /// buffer — every system-minted family is durable, and the operator's
    /// standing number is the one the reaper reads.
    pub fn default_retain_depth(self) -> Depth {
        match self {
            Self::Ingress => INGRESS_DEFAULT_RETAIN_DEPTH,
            Self::Tool => TOOL_CHANNEL_DEFAULT_RETAIN_DEPTH,
        }
    }
}

/// Which role a `[[channel]]` block plays.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelBlockRole {
    /// Mints a `ChannelEntry` for an operator-owned `brenn:`/`ephemeral:`/`local:`
    /// channel.
    Declaring,
    /// Supplies depths and knobs for channels the system mints. Mints nothing.
    Tuning,
}

/// The role a block keyed by this exact address plays. An address in a
/// system-minted family tunes; anything else declares.
pub fn channel_block_role(address: &str) -> ChannelBlockRole {
    match SystemChannelFamily::of(address) {
        Some(_) => ChannelBlockRole::Tuning,
        None => ChannelBlockRole::Declaring,
    }
}

/// One block's worth of tuning: the three required depths plus the optional
/// knobs, which fall back to the `[messaging]` globals exactly as a declaring
/// block's do.
#[derive(Debug, Clone)]
struct SystemChannelTuningEntry {
    push_depth: Depth,
    retain_depth: Depth,
    standing_retain_depth: Depth,
    noise: Option<NoiseLevel>,
    sink: Option<Sink>,
    wake_min: Option<WakeMin>,
    send_rate: Option<SendRate>,
}

/// The operator's tuning table for system-minted channels, keyed by exact
/// address or by address prefix.
///
/// Exact `webhook:`/`brenn:tools/`/`brenn:tool-results/` blocks are boot-checked
/// against the endpoints, tools and grants that exist, so a typo cannot silently
/// tune nothing. Exact `mqtt:` blocks and *every* prefix block are deliberately
/// not checked: the MQTT channel population is open-ended (runtime subscribes
/// mint channels long after boot), and a prefix is a standing rule for a family
/// whose membership is dynamic.
#[derive(Debug, Clone, Default)]
pub struct SystemChannelTuning {
    exact: BTreeMap<String, SystemChannelTuningEntry>,
    /// Prefix blocks, longest first, so the first match is the winner.
    prefixes: Vec<(String, SystemChannelTuningEntry)>,
}

impl SystemChannelTuning {
    /// The exact addresses this table tunes, for the caller that boot-checks
    /// them against the endpoints/tools/grants that actually exist.
    pub fn exact_addresses(&self) -> impl Iterator<Item = &str> {
        self.exact.keys().map(String::as_str)
    }

    fn lookup(&self, address: &str) -> Option<&SystemChannelTuningEntry> {
        if let Some(entry) = self.exact.get(address) {
            return Some(entry);
        }
        self.prefixes
            .iter()
            .find(|(p, _)| address.starts_with(p.as_str()))
            .map(|(_, e)| e)
    }
}

/// Does `prefix` end at a segment boundary of a channel address?
///
/// The same narrowing the ACL resolution boundary applies to an operator-written
/// `Prefix` matcher: without it `webhook:git` would reach `webhook:github`.
/// `mqtt:` adds one boundary the ACL side does not have — the colon that closes
/// the client segment (`mqtt:home:`).
fn ends_at_segment_boundary(prefix: &str) -> bool {
    prefix.ends_with('/') || prefix.ends_with('.') || prefix.ends_with(':')
}

/// Build the tuning table from the `[[channel]]` blocks that play the tuning
/// role. Declaring blocks are skipped here and consumed by
/// [`build_channel_entries`], which skips the tuning ones — both passes classify
/// through [`channel_block_role`], so no block can fall between them.
///
/// Keys enter the table canonicalized ([`canonical_tuning_key`]), so a block
/// addressing `tools/pull` tunes the same channel as one addressing
/// `brenn:tools/pull` — and the two spellings collide as duplicates.
///
/// # Panics
///
/// - a block setting neither or both of `address` / `address_prefix`
/// - a tuning block carrying `uuid` or `description`
/// - a tuning block missing any of the three depths, or stating
///   `retain_depth = 0`
/// - an exact `mqtt:` address that is not a well-formed
///   `mqtt:<client>:<topic-filter>`
/// - an `address_prefix` that is empty, names no system-minted family, or does
///   not end at a segment boundary
/// - a duplicate exact address or a duplicate prefix
/// - `sink = "archive"` (stated or inherited) with no `[messaging].archive_path`
pub fn build_system_channel_tuning(
    raw_channels: &[ChannelConfigRaw],
    defaults: &MessagingGlobalConfig,
) -> SystemChannelTuning {
    let mut tuning = SystemChannelTuning::default();

    for ch in raw_channels {
        let key = channel_block_key(ch);
        let (label, entry_key) = match key {
            ChannelBlockKey::Address(address) => {
                if channel_block_role(address) == ChannelBlockRole::Declaring {
                    continue;
                }
                let canonical = canonical_tuning_key(address);
                // `mqtt:` exact blocks are exempt from the boot existence check
                // (the population is open-ended), so their *shape* is the only
                // thing that can be checked at all: an address the mint path
                // could never produce would match nothing, ever, which is the
                // silent no-op the existence check exists to prevent.
                if ChannelScheme::of(&canonical) == Some(ChannelScheme::Mqtt) {
                    let parsed = crate::mqtt::address::parse_mqtt_address(&canonical)
                        .unwrap_or_else(|e| {
                            panic!(
                                "config: [[channel]] address {address:?} is not a well-formed \
                                 mqtt:<client>:<topic> address, so no minted channel can ever \
                                 carry it: {e}"
                            )
                        });
                    crate::mqtt::address::validate_topic_filter_str(&parsed.topic).unwrap_or_else(
                        |detail| {
                            panic!(
                                "config: [[channel]] address {address:?} is not a valid MQTT \
                                 topic filter, so no minted channel can ever carry it: {detail}"
                            )
                        },
                    );
                }
                (format!("{address:?}"), canonical)
            }
            ChannelBlockKey::Prefix(prefix) => {
                assert!(
                    !prefix.is_empty(),
                    "config: [[channel]] address_prefix must not be empty",
                );
                assert!(
                    ends_at_segment_boundary(prefix),
                    "config: [[channel]] address_prefix {prefix:?} must end at a segment \
                     boundary ('/', '.' or the mqtt client colon) — a bare byte prefix \
                     would reach past the family it names",
                );
                let canonical = canonical_tuning_key(prefix);
                // A prefix ending at a segment boundary is itself the shortest
                // address of the family, so `of()` classifies it directly.
                assert!(
                    SystemChannelFamily::of(&canonical).is_some(),
                    "config: [[channel]] address_prefix {prefix:?} names no system-minted \
                     family — prefixes tune mqtt:, webhook:, brenn:tools/ and \
                     brenn:tool-results/ channels, and an operator-declared channel takes \
                     its own [[channel]] block",
                );
                (format!("prefix {prefix:?}"), canonical)
            }
        };

        assert!(
            ch.uuid.is_none(),
            "config: [[channel]] {label} tunes a system-minted channel and must not set \
             uuid — those channels derive a deterministic UUID from their address, and an \
             operator-supplied one could only disagree",
        );
        assert!(
            ch.description.is_none(),
            "config: [[channel]] {label} tunes a system-minted channel and must not set \
             description — the endpoint/tool that mints the channel owns it",
        );

        let depth = |field: &str, value: Option<Depth>| -> Depth {
            value.unwrap_or_else(|| {
                panic!(
                    "config: [[channel]] {label} requires {field} — a system-minted channel \
                     has a bounded in-code default, and a block that tunes it states every \
                     depth rather than inheriting some of them"
                )
            })
        };
        let entry = SystemChannelTuningEntry {
            push_depth: depth("push_depth", ch.push_depth),
            retain_depth: depth("retain_depth", ch.retain_depth),
            standing_retain_depth: depth("standing_retain_depth", ch.standing_retain_depth),
            noise: ch.noise,
            sink: ch.sink,
            wake_min: ch.wake_min,
            send_rate: ch.send_rate,
        };
        assert_rungs_within_standing(
            entry.push_depth,
            entry.retain_depth,
            entry.standing_retain_depth,
            &format!("[[channel]] {label}"),
        );
        // A system-minted channel's retained window is also what its system
        // participants subscribe at (the tool executor's depths are the
        // channel's `retain_depth`), so a window of zero mints a channel that
        // retains nothing and a subscriber with no position at all — a state
        // whose only symptom is a much later "no position for system
        // subscriber" panic blaming the host. Refuse it here, naming the block.
        assert!(
            entry.retain_depth != Depth::Bounded(0),
            "config: [[channel]] {label} sets retain_depth = 0 — a system-minted channel \
             retains a window sized for the outage it must survive, and its system \
             participants subscribe at that window; zero leaves them with no position",
        );
        if let Some(rate) = entry.send_rate {
            rate.validate(&format!("[[channel]] {label}"));
        }
        // An archive sink with no archive_path panics in the hourly GC pass;
        // refuse at load time.
        if entry.sink.unwrap_or(defaults.default_sink) == Sink::Archive
            && defaults.archive_path.is_none()
        {
            panic!(
                "config: [[channel]] {label} has sink = \"archive\" but \
                 [messaging].archive_path is not set",
            );
        }

        match key {
            ChannelBlockKey::Address(_) => {
                assert!(
                    tuning.exact.insert(entry_key.clone(), entry).is_none(),
                    "config: duplicate [[channel]] address {entry_key:?} — a bare address \
                     names the same channel as its brenn:-qualified spelling",
                );
            }
            ChannelBlockKey::Prefix(_) => {
                assert!(
                    !tuning.prefixes.iter().any(|(p, _)| *p == entry_key),
                    "config: duplicate [[channel]] address_prefix {entry_key:?}",
                );
                tuning.prefixes.push((entry_key, entry));
            }
        }
    }

    // Longest first: two distinct prefixes of equal length cannot both match one
    // address, so ordering by descending length makes first-match total.
    tuning
        .prefixes
        .sort_by_key(|(prefix, _)| std::cmp::Reverse(prefix.len()));
    tuning
}

/// Canonicalize a tuning key to the spelling minted channels carry: a key
/// without a scheme becomes `brenn:<key>`. Minted addresses are always
/// canonical, so a bare key would match nothing without this.
fn canonical_tuning_key(key: &str) -> String {
    match ChannelScheme::of(key) {
        Some(_) => key.to_string(),
        None => crate::messaging::canonical_address(key),
    }
}

/// The `ResolvedChannel` a system-minted channel takes: webhook and MQTT
/// ingress, the async-tool request/result channels, and the rows a restart
/// reconstructs for any of them.
///
/// Nothing in the operator's config *declares* these channels — synthesis owns
/// their creation, identity and description — so their depths come from a
/// bounded per-family default, overridable by a `[[channel]]` tuning block.
/// Resolution is exact block, else the longest matching prefix block, else the
/// family default.
///
/// The window is what message-bus.md's fact-channel discipline actually asks
/// for: retention sized to the outage the deployment intends to survive, with
/// the sizing decision in front of the operator instead of pinned out of reach.
/// Nothing about a `ResolvedChannel` is persisted, so a retune takes effect on
/// the next restart with no migration.
///
/// One pure function of (address, config) serves every call site, which is what
/// keeps a channel reconstructed from its DB row resolving identically to the
/// one created at runtime.
///
/// # Panics
///
/// If `address` names no system-minted family — that is a host bug, not operator
/// config: an operator-declared channel resolves through `build_channel_entries`.
pub fn resolve_system_channel(
    address: &str,
    tuning: &SystemChannelTuning,
    defaults: &MessagingGlobalConfig,
) -> ResolvedChannel {
    let family = SystemChannelFamily::of(address).unwrap_or_else(|| {
        panic!(
            "messaging: resolve_system_channel called for {address:?}, which is not a \
             system-minted address"
        )
    });
    let tuned = tuning.lookup(address);
    let default_retain = family.default_retain_depth();
    ResolvedChannel {
        push_depth: tuned.map_or(SYSTEM_CHANNEL_DEFAULT_PUSH_DEPTH, |t| t.push_depth),
        retain_depth: tuned.map_or(default_retain, |t| t.retain_depth),
        standing_retain_depth: tuned.map_or(default_retain, |t| t.standing_retain_depth),
        noise: tuned
            .and_then(|t| t.noise)
            .unwrap_or(defaults.default_noise),
        sink: tuned.and_then(|t| t.sink).unwrap_or(defaults.default_sink),
        wake_min: tuned
            .and_then(|t| t.wake_min)
            .unwrap_or(defaults.default_wake_min),
        send_rate: tuned
            .and_then(|t| t.send_rate)
            .unwrap_or(defaults.default_send_rate),
    }
}

/// Raw (pre-inheritance) subscription params handed to `resolve_subscription_params`.
/// `None` means "inherit from the rung". `channel_uuid`/`channel_address` identify
/// the target channel for the resulting `ResolvedSubscription`.
#[derive(Debug, Clone)]
pub struct RawSubscriptionParams {
    pub channel_uuid: Uuid,
    pub channel_address: String,
    pub push_depth: Option<Depth>,
    pub retain_depth: Option<Depth>,
    pub noise: Option<NoiseLevel>,
    pub wake_min: Option<WakeMin>,
}

/// Error from `resolve_subscription_params`.
///
/// At boot these conditions are operator-config violations and the caller
/// `.expect()`s the result (preserving today's fail-fast `panic!`). At runtime the
/// same conditions are bad *tool* input and the caller maps the `Err` to a
/// tool-facing message (a misconfigured tool call is LLM/attacker-shaped input, not
/// a host bug — CLAUDE.md "panic on host bug, error on bad input").
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubscribeError {
    /// `noise` explicitly set on a pull-only (`push_depth = 0`) subscription.
    NoiseOnPullOnly { channel_address: String },
    /// `wake_min` explicitly set on a pull-only (`push_depth = 0`) subscription.
    WakeMinOnPullOnly { channel_address: String },
    /// A push-enabled (`push_depth > 0`) subscription on a non-`singleton` app.
    PushEnabledRequiresSingleton { channel_address: String },
    /// A push-enabled subscription on an app without exactly one `allowed_users`.
    PushEnabledRequiresSingleUser {
        channel_address: String,
        allowed_users: usize,
    },
    /// `noise = "fatal"` resolved on a backend subscription. `fatal` is the
    /// surface-only kill rung (kernel-enacted); it has no referent on the backend
    /// overflow path, so a backend subscription that resolves to it — directly or
    /// by inheriting a `fatal` channel/global default — is rejected. A `fatal`
    /// channel default is legal as long as no backend subscription inherits it.
    ///
    /// This variant covers app/mqtt/webhook subscriptions. WASM consumer
    /// subscriptions reject `fatal` separately at boot; the two sites enforce
    /// the same rule and must stay in step.
    FatalNoise { channel_address: String },
}

impl std::fmt::Display for SubscribeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SubscribeError::NoiseOnPullOnly { channel_address } => write!(
                f,
                "subscription on channel {channel_address:?} has noise configured but \
                 push_depth = 0 (pull-only) — no push-overflow events are possible; \
                 remove the noise setting or set push_depth > 0"
            ),
            SubscribeError::WakeMinOnPullOnly { channel_address } => write!(
                f,
                "subscription on channel {channel_address:?} has wake_min configured but \
                 push_depth = 0 (pull-only) — no push rows exist so wake_min is \
                 meaningless; remove the wake_min setting or set push_depth > 0"
            ),
            SubscribeError::PushEnabledRequiresSingleton { channel_address } => write!(
                f,
                "subscription with push_depth > 0 on channel {channel_address:?} requires \
                 `singleton = true` (push delivery needs a unique conversation target). \
                 Set push_depth = 0 for a pull-only multi-user subscription."
            ),
            SubscribeError::PushEnabledRequiresSingleUser {
                channel_address,
                allowed_users,
            } => write!(
                f,
                "subscription with push_depth > 0 on channel {channel_address:?} requires \
                 exactly one `allowed_users` entry (got {allowed_users}). Push delivery \
                 needs a unique authorized user."
            ),
            SubscribeError::FatalNoise { channel_address } => write!(
                f,
                "subscription on channel {channel_address:?} resolves noise = \"fatal\", but \
                 `fatal` is the surface-only kill rung — it has no meaning on a backend \
                 subscription. Set noise to silent/metered/alarm, or (if this is inherited) \
                 stop this subscription from inheriting a `fatal` channel/global default."
            ),
        }
    }
}

impl std::error::Error for SubscribeError {}

/// Resolve one subscription's params via sub → rung inheritance and enforce the
/// push-enabled invariants. Single source of truth shared by the boot path
/// (`resolve_app_messaging`, `resolve_app_mqtt_subscriptions`) and the runtime
/// dynamic-subscribe tool (later increment).
///
/// `singleton`/`allowed_users` come from the owning app's config; they gate the
/// push-enabled invariants. The `noise`/`wake_min` pull-only checks read the *raw*
/// presence (so an inherited value on a pull-only sub is fine but an explicit one
/// is an error).
///
/// Returns `Err` (never panics) on any invariant violation: the boot caller
/// `.expect()`s it; the tool caller maps it to a tool error.
pub fn resolve_subscription_params(
    raw: &RawSubscriptionParams,
    rung: &SubscriptionParamDefaults,
    singleton: bool,
    allowed_users: usize,
) -> Result<ResolvedSubscription, SubscribeError> {
    let resolved_push_depth = raw.push_depth.unwrap_or(rung.push_depth);
    // Both depths pass through verbatim. The standing-depth ceiling is checked
    // where the channel's standing depth is in hand: over the assembled
    // directory at boot (`validate_subscriber_depth_ceilings`) and in
    // `Messenger::subscribe_dynamic` for a runtime subscribe, which owes the
    // caller a typed refusal rather than a panic.
    let resolved_retain_depth = raw.retain_depth.unwrap_or(rung.retain_depth);

    // Noise: check raw presence BEFORE collapsing into inheritance, so an
    // explicitly-set noise on a pull-only sub is an error but an inherited
    // noise on a pull-only sub is not.
    if raw.noise.is_some() && resolved_push_depth == Depth::Bounded(0) {
        return Err(SubscribeError::NoiseOnPullOnly {
            channel_address: raw.channel_address.clone(),
        });
    }
    let resolved_noise = raw.noise.unwrap_or(rung.noise);

    // `fatal` is the surface-only kill rung; a backend subscription can never
    // enact it (the backend overflow path has no kill). Reject it here for
    // app/mqtt/webhook subscriptions; WASM consumers reject `fatal` separately
    // at boot — the two sites must stay in step.
    if resolved_noise == NoiseLevel::Fatal {
        return Err(SubscribeError::FatalNoise {
            channel_address: raw.channel_address.clone(),
        });
    }

    // wake_min: same pattern as noise.
    if raw.wake_min.is_some() && resolved_push_depth == Depth::Bounded(0) {
        return Err(SubscribeError::WakeMinOnPullOnly {
            channel_address: raw.channel_address.clone(),
        });
    }
    let resolved_wake_min = raw.wake_min.unwrap_or(rung.wake_min);

    // Push-enabled ⇒ singleton + exactly one allowed_user.
    if resolved_push_depth.is_push_enabled() {
        if !singleton {
            return Err(SubscribeError::PushEnabledRequiresSingleton {
                channel_address: raw.channel_address.clone(),
            });
        }
        if allowed_users != 1 {
            return Err(SubscribeError::PushEnabledRequiresSingleUser {
                channel_address: raw.channel_address.clone(),
                allowed_users,
            });
        }
    }

    Ok(ResolvedSubscription {
        channel_uuid: raw.channel_uuid,
        channel_address: raw.channel_address.clone(),
        push_depth: resolved_push_depth,
        retain_depth: resolved_retain_depth,
        noise: resolved_noise,
        wake_min: resolved_wake_min,
    })
}

/// Validate and resolve per-app messaging config, returning the resolved form.
///
/// Validation:
/// - subscribed channels exist (by canonical address)
/// - push-enabled subscriptions require `singleton == true` and
///   exactly one `allowed_users` entry
/// - `noise` on a pull-only (`push_depth=0`) subscription is a hard config error
/// - `send_budget` (if set) is >= 1
///
/// # Panics
///
/// On any of the above violations.
pub fn resolve_app_messaging(
    raw_app: &AppConfigRaw,
    raw: &MessagingConfigRaw,
    defaults: &MessagingGlobalConfig,
    directory: &MessagingDirectory,
) -> ResolvedMessagingConfig {
    if let Some(b) = raw.send_budget {
        assert!(
            b >= 1,
            "app {:?}: messaging.send_budget ({b}) must be >= 1",
            raw_app.slug,
        );
    }
    let send_budget = raw.send_budget.unwrap_or(defaults.default_send_budget);

    let mut resolved_subs = Vec::with_capacity(raw.subscribe.len());
    let mut seen_addresses = HashSet::new();
    for sub in &raw.subscribe {
        let entry = directory.resolve(&sub.channel).unwrap_or_else(|| {
            panic!(
                "app {:?}: messaging.subscribe.channel {:?} is not a known [[channel]] address",
                raw_app.slug, sub.channel,
            )
        });
        assert!(
            seen_addresses.insert(entry.address.clone()),
            "app {:?}: duplicate messaging.subscribe entry for {:?}",
            raw_app.slug,
            entry.address,
        );

        // Three-level inheritance (sub → channel → global) + push-enabled invariants,
        // via the shared resolver. At boot a violation is an operator-config error, so
        // `.expect()` preserves today's fail-fast `panic!`. The channel rung already
        // folds the channel layer over global (`ResolvedChannel`), so the resolver's
        // sub → rung ladder is the full sub → channel → global chain.
        let raw_params = RawSubscriptionParams {
            channel_uuid: entry.uuid,
            channel_address: entry.address.clone(),
            push_depth: sub.push_depth,
            retain_depth: sub.retain_depth,
            noise: sub.noise,
            wake_min: sub.wake_min,
        };
        let rung = SubscriptionParamDefaults::from_channel(&entry.resolved_channel);
        let resolved = resolve_subscription_params(
            &raw_params,
            &rung,
            raw_app.singleton,
            raw_app.allowed_users.len(),
        )
        .unwrap_or_else(|e| panic!("app {:?}: messaging.subscribe: {e}", raw_app.slug));
        resolved_subs.push(resolved);
    }

    ResolvedMessagingConfig {
        send_budget,
        subscriptions: resolved_subs,
    }
}

/// After all apps (and WASM consumers) have resolved, walk their messaging
/// configs to populate `subscribers` on each `ChannelEntry`. Returns a fresh
/// `MessagingDirectory` ready to wrap in `Arc`.
///
/// `apps_with_messaging` are added first (in declaration order), then
/// `wasm_consumers` (in declaration order). Both sets may subscribe to the
/// same channel; the subscriber list preserves that ordering.
///
/// WASM consumers whose `channel` address does not match any entry in
/// `entries` are silently skipped here — the caller is responsible for
/// panicking on missing channels at bootstrap before calling this function.
/// Surface subscriptions do **not** get that treatment: a missing channel there
/// panics, because the silent skip's failure mode is a component that resolves
/// a subscription, receives no push rows, and reports nothing — silent denial,
/// indistinguishable at runtime from an idle channel.
///
/// `surfaces` supplies `(surface_slug, wire_subscriptions)` in declaration
/// order, appended after the WASM consumers. A surface contributes exactly one
/// entry per channel it binds, whatever the component bindings behind it: an
/// attachment holds one subscription per channel, so the declared bindings fold
/// into one entry rather than each minting its own.
pub fn finalize_directory_with_subscribers(
    mut entries: Vec<ChannelEntry>,
    apps_with_messaging: &[(String, ResolvedMessagingConfig)],
    wasm_consumers: &[(String, Vec<ResolvedSubscription>)],
    surfaces: &[(String, Vec<ResolvedSurfaceSubscription>)],
) -> MessagingDirectory {
    // For each entry, collect subscribers in the order apps appear in
    // `apps_with_messaging` (which is itself the IndexMap declaration order).
    let mut by_uuid: std::collections::HashMap<Uuid, &mut ChannelEntry> =
        std::collections::HashMap::new();
    for entry in entries.iter_mut() {
        by_uuid.insert(entry.uuid, entry);
    }
    // Append one subscriber kind's static bindings. `kind` is the tuple-variant
    // constructor (`App`/`Wasm`/`Surface`), so a fourth kind is one call.
    // `gated` = the kind is `UrgencyGated` (App). Only those subscribers carry a
    // wake threshold; `Eager` kinds (Wasm/Surface) store `None`.
    let mut append_kind = |slug: &str,
                           subs: &[ResolvedSubscription],
                           kind: fn(String) -> SubscriberEntryKind,
                           gated: bool| {
        for sub in subs {
            if let Some(entry) = by_uuid.get_mut(&sub.channel_uuid) {
                entry.subscribers.push(crate::messaging::SubscriberEntry {
                    kind: kind(slug.to_string()),
                    push_depth: sub.push_depth,
                    retain_depth: sub.retain_depth,
                    noise: sub.noise,
                    wake_min: gated.then_some(sub.wake_min),
                });
            }
        }
    };
    for (slug, msg) in apps_with_messaging {
        append_kind(slug, &msg.subscriptions, SubscriberEntryKind::App, true);
    }
    for (slug, subs) in wasm_consumers {
        append_kind(slug, subs, SubscriberEntryKind::Wasm, false);
    }
    // Surfaces do not go through `append_kind`: N components of one surface may
    // bind the same channel, and the directory carries one entry per (surface,
    // channel) — one attachment holds one subscription per channel, so a second
    // entry would be a second server-side push window feeding the same socket.
    // The declared bindings fold into it: depths by max (the widest window any
    // component asked for is the one that must arrive), noise by max under the
    // clamp below.
    for (slug, subs) in surfaces {
        let mut folded: std::collections::HashMap<Uuid, usize> = std::collections::HashMap::new();
        for sub in subs {
            // A surface subscription resolved against a channel the directory
            // does not hold means the two build paths disagree about what
            // exists. Skipping would drop the surface's subscriber entry, and
            // the entry is what every delivery decision reads: no live fan-out
            // would name the surface on that channel, so it would receive
            // nothing forever with no signal at boot or runtime. Host-state
            // corruption — panic.
            let entry = by_uuid
                .get_mut(&sub.subscription.channel_uuid)
                .unwrap_or_else(|| {
                    panic!(
                        "surface {slug:?} instance {:?}: subscription on channel {} ({}) resolved \
                         against a channel absent from the directory — the resolver and the \
                         directory disagree about which channels exist",
                        sub.instance,
                        sub.subscription.channel_address,
                        sub.subscription.channel_uuid,
                    )
                });
            // Clamp a surface subscription's overflow noise to `Metered`: the
            // shared overflow path's `Alarm`/`Fatal` arms must never fire for a
            // surface. The full resolved rung is still carried in the bindings
            // document for the kernel; this clamp covers only the server's own
            // push window.
            let noise = sub.subscription.noise.min(NoiseLevel::Metered);
            match folded.get(&sub.subscription.channel_uuid) {
                Some(&at) => {
                    let existing = &mut entry.subscribers[at];
                    existing.push_depth = existing.push_depth.max(sub.subscription.push_depth);
                    existing.retain_depth =
                        existing.retain_depth.max(sub.subscription.retain_depth);
                    existing.noise = existing.noise.max(noise);
                }
                None => {
                    folded.insert(sub.subscription.channel_uuid, entry.subscribers.len());
                    entry.subscribers.push(crate::messaging::SubscriberEntry {
                        kind: SubscriberEntryKind::Surface(slug.clone()),
                        push_depth: sub.subscription.push_depth,
                        retain_depth: sub.subscription.retain_depth,
                        noise,
                        // Surfaces are `Eager`, like the wasm loop above.
                        wake_min: None,
                    });
                }
            }
        }
    }
    MessagingDirectory::with_entries(entries)
}

/// Fold durable dynamic-subscription rows into an already-finalized directory
/// (design §2.1 boot merge).
///
/// Runs *after* [`finalize_directory_with_subscribers`] has populated the
/// directory with the static (TOML) and WASM subscribers. Each row in `rows` (loaded
/// from `messaging_dynamic_subscriptions`, the durable truth that boot does NOT
/// truncate) is folded onto its channel as an `App(app_slug)` subscriber via the
/// directory's copy-on-write [`MessagingDirectory::add_subscriber`] — the same
/// directory mechanism static subs use, so dynamic subscribers become visible to
/// the hot paths exactly as static ones.
///
/// Two rows are **dropped with a `warn` log**, not panicked — both are durable
/// user state that the operator's config has since overridden, not host bugs:
/// - **Channel gone from `messaging_channels` too**: the directory cannot answer
///   for it and neither can the table, which is the documented manual-cleanup
///   path (an operator deleting a channel row retires it for good).
/// - **Static collision:** the `(channel, app)` already carries a static `App`
///   subscriber. Static config wins; the dynamic row is dropped.
///
/// A channel the directory cannot answer for whose row is still in
/// `messaging_channels` is a different case and is classified **revoked** below:
/// the `[[channel]]` block was removed, renamed, or commented out, which is a
/// config change the operator may revert.
///
/// `unreconstructible` is the skip report from
/// [`load_channels_by_uuids`](crate::messaging::db::load_channels_by_uuids): uuid
/// → address for every requested row that loader declined to reconstruct. It
/// routinely names channels that *are* declared — the loader skips every
/// non-system address, and a declared one is already in the directory — so the
/// directory is consulted **first** and this report only when the directory has
/// no answer. That order is what keeps a declared channel's rows folding live;
/// reading the report first would hold every one of them dormant. Within the
/// directory-miss branch, membership is the whole test — no channel family is
/// special-cased.
///
/// The dynamic rows carry already-resolved param values (resolved at creation
/// time and stored verbatim), so they are folded as-is — there is no inheritance
/// to re-apply.
///
/// Returns a [`DynamicMergeOutcome`] partitioning the input rows: `kept` are the
/// rows folded into the directory, which is the whole of what surviving them
/// means — the boot path writes nothing for them; `dropped` are the `(channel_uuid, app_slug)` keys the
/// boot path prunes from `messaging_dynamic_subscriptions` so the same conflict
/// does not recur next boot; `revoked` are the rows the current config no longer
/// stands behind — the app's resolved `AppPolicy` no longer authorizes delivery
/// on the channel, one of the row's depths exceeds the channel's current
/// `standing_retain_depth` (the operator tightened standing below the granted
/// depth), or the channel is no longer declared while its row survives. Each
/// comes back as a [`DormantSubscription`] carrying the channel address this
/// function resolved for it. These are
/// **neither folded nor pruned**: they lie dormant in the durable table so the
/// subscription resumes if the operator re-grants the ACL, raises standing back,
/// or redeclares the channel. This step itself mutates only the in-memory
/// directory; the mirror insert and durable-table prune are performed by the
/// caller.
///
/// `app_policy` resolves an app slug → its current resolved `AppPolicy`. The
/// dynamic rows always fold an `App(slug)` subscriber, so the merge gate only
/// needs the per-app policy view (not WASM). A row whose slug has no resolvable
/// policy is treated as **revoked** (fail-closed: an app with no policy cannot
/// authorize delivery), not dropped — the durable row survives in case the
/// policy is restored. The check is `AppPolicy::allows_channel_access(channel.address)`
/// — the same delivery-authorization decision the runtime fan-out / dispatcher
/// floor use, so the boot gate is identical to the delivery gate.
pub fn merge_dynamic_subscriptions<'p>(
    directory: &MessagingDirectory,
    rows: &[crate::messaging::db::DynamicSubscriptionRow],
    unreconstructible: &HashMap<Uuid, String>,
    app_policy: &dyn Fn(&str) -> Option<&'p crate::access::AppPolicy>,
) -> DynamicMergeOutcome {
    let mut outcome = DynamicMergeOutcome::default();
    for row in rows {
        let Some(entry) = directory.by_uuid(&row.channel_uuid) else {
            // If the channel's row is still in `messaging_channels` it exists
            // and is merely undeclared — a block removed, renamed, or commented
            // out, which is a config change the operator may revert, and pruning
            // would silently destroy durable user state on it. The row lies
            // dormant instead, the same treatment a revoked ACL or a tightened
            // standing depth gets. Deleting the channel's row is the retirement
            // path, and that is the case that still drops.
            if let Some(address) = unreconstructible.get(&row.channel_uuid) {
                tracing::warn!(
                    channel_uuid = %row.channel_uuid,
                    channel = %address,
                    app = %row.app_slug,
                    "merge_dynamic_subscriptions: dynamic subscription dormant — the \
                     channel row exists but no `[[channel]]` block declares it; \
                     durable row retained (not pruned), dormant until the channel \
                     is redeclared",
                );
                outcome.revoked.push(DormantSubscription {
                    channel_uuid: row.channel_uuid,
                    app_slug: row.app_slug.clone(),
                    channel_address: address.clone(),
                });
                continue;
            }
            tracing::warn!(
                channel_uuid = %row.channel_uuid,
                app = %row.app_slug,
                "merge_dynamic_subscriptions: dropping dynamic subscription for a \
                 channel whose row is gone from messaging_channels",
            );
            outcome
                .dropped
                .push((row.channel_uuid, row.app_slug.clone()));
            continue;
        };
        let static_collision = entry
            .subscribers
            .iter()
            .any(|s| matches!(&s.kind, SubscriberEntryKind::App(slug) if slug == &row.app_slug));
        if static_collision {
            tracing::warn!(
                channel = %entry.address,
                app = %row.app_slug,
                "merge_dynamic_subscriptions: dropping dynamic subscription that \
                 collides with a static subscription on the same (channel, app); \
                 static config wins",
            );
            outcome
                .dropped
                .push((row.channel_uuid, row.app_slug.clone()));
            continue;
        }
        // Delivery-time ACL gate at boot. A revoked-ACL row must
        // NOT be folded (no subscriber, no broker re-SUBSCRIBE for mqtt) and must
        // NOT be pruned (the operator may re-grant; pruning would silently destroy
        // durable user state on a policy change). A missing policy fails closed —
        // treated as revoked, not dropped, for the same non-destructive reason.
        let allowed = app_policy(&row.app_slug)
            .is_some_and(|policy| policy.allows_channel_access(&entry.address));
        if !allowed {
            tracing::warn!(
                channel = %entry.address,
                app = %row.app_slug,
                "merge_dynamic_subscriptions: dynamic subscription revoked — the \
                 app's resolved policy no longer authorizes delivery on this \
                 channel; durable row retained (not pruned), subscription dormant \
                 until the ACL is re-granted",
            );
            outcome.revoked.push(DormantSubscription {
                channel_uuid: row.channel_uuid,
                app_slug: row.app_slug.clone(),
                channel_address: entry.address.clone(),
            });
            continue;
        }
        // Depth conformance gate: a durable row whose push_depth or retain_depth
        // exceeds the channel's *current* standing_retain_depth is no longer
        // live-valid — the operator tightened standing below what this dynamic sub
        // was granted. Classify it `revoked` (dormant), exactly like the ACL gate
        // above: warn, neither folded (no over-standing subscriber is
        // re-established) nor pruned (durable user state invalidated by a config
        // change the operator may revert — pruning would destroy it silently). The
        // runtime gate (Messenger::subscribe_dynamic) rejects new over-standing
        // subs; this covers a row that predates a standing tightening.
        let standing = entry.resolved_channel.standing_retain_depth;
        if let Some((field, granted)) = [
            ("push_depth", row.push_depth),
            ("retain_depth", row.retain_depth),
        ]
        .into_iter()
        .find(|(_, depth)| *depth > standing)
        {
            tracing::warn!(
                channel = %entry.address,
                app = %row.app_slug,
                field,
                granted = ?granted,
                standing = ?standing,
                "merge_dynamic_subscriptions: dynamic subscription revoked — its \
                 depth exceeds the channel's current standing_retain_depth; \
                 durable row retained (not pruned), subscription dormant until the \
                 operator raises standing or the app re-subscribes with a \
                 conforming depth",
            );
            outcome.revoked.push(DormantSubscription {
                channel_uuid: row.channel_uuid,
                app_slug: row.app_slug.clone(),
                channel_address: entry.address.clone(),
            });
            continue;
        }
        let applied = directory.add_subscriber(
            &row.channel_uuid,
            crate::messaging::SubscriberEntry {
                kind: SubscriberEntryKind::App(row.app_slug.clone()),
                push_depth: row.push_depth,
                retain_depth: row.retain_depth,
                noise: row.noise,
                wake_min: Some(row.wake_min),
            },
        );
        // `by_uuid` resolved the entry above and we hold no other writer between
        // that read and this add; a missing channel here would be a host bug.
        assert!(
            applied,
            "merge_dynamic_subscriptions: channel {} vanished mid-merge",
            row.channel_uuid,
        );
        outcome.kept.push(row.clone());
    }
    outcome
}

/// A durable dynamic subscription the boot merge held back: kept in its table,
/// kept out of the directory, waiting for the config change behind it to be
/// reverted.
///
/// Carries the channel address because the merge is the single point that
/// resolves it for every dormant class; consumers would otherwise each have
/// to re-derive it from two sources.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DormantSubscription {
    pub channel_uuid: Uuid,
    pub app_slug: String,
    pub channel_address: String,
}

/// Result of [`merge_dynamic_subscriptions`]: which durable dynamic rows were
/// folded into the directory (`kept`), dropped (`dropped`), or revoked
/// (`revoked`).
///
/// The boot path folds `kept` into the directory and nowhere else, and uses
/// `dropped` to prune the now-overridden rows from
/// `messaging_dynamic_subscriptions` (so the conflict does not recur next boot).
/// `revoked` rows are folded into **neither** — they are not added to the
/// directory and are **not** pruned: the durable row is deliberately retained so
/// the subscription resumes if the config change behind it is reverted.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct DynamicMergeOutcome {
    /// Rows folded into the directory (to be mirrored into the static table).
    pub kept: Vec<crate::messaging::db::DynamicSubscriptionRow>,
    /// `(channel_uuid, app_slug)` keys of rows dropped at merge (a channel whose
    /// row is gone from `messaging_channels`, or a static collision) — to be
    /// pruned from `messaging_dynamic_subscriptions`.
    pub dropped: Vec<(Uuid, String)>,
    /// Rows the current config no longer stands behind: the app's resolved
    /// `AppPolicy` no longer authorizes delivery (revoked ACL or missing policy),
    /// one of the row's depths exceeds the channel's current
    /// `standing_retain_depth`, or the channel's `[[channel]]` block is gone
    /// while its row survives. NOT folded, NOT pruned — retained dormant in the
    /// durable table until the ACL is re-granted, standing is raised back, or the
    /// channel is redeclared.
    pub revoked: Vec<DormantSubscription>,
}

/// Convenience: build the channel directory wrapped in an `Arc`. The
/// caller is expected to pass the result to `Messenger::new`.
pub fn build_directory_arc(entries: Vec<ChannelEntry>) -> Arc<MessagingDirectory> {
    Arc::new(MessagingDirectory::with_entries(entries))
}

/// Build the runtime `MessagingDirectory` from configured channels, apps'
/// resolved messaging configs, and WASM consumer subscriptions.
///
/// This is the function the binary crate calls at startup. The
/// `apps_in_decl_order` argument supplies `(app_slug, ResolvedMessagingConfig)`
/// pairs in declaration order so subscriber lists on each entry retain the
/// app order shown in `MessageListChannels` output.
///
/// `wasm_consumers_in_decl_order` supplies `(slug, Vec<ResolvedSubscription>)`
/// for each `[[wasm_consumer]]` in config order. Pass an empty slice when no
/// WASM consumers are configured.
pub fn build_runtime_directory(
    raw_channels: &[ChannelConfigRaw],
    apps_in_decl_order: &[(String, ResolvedMessagingConfig)],
    wasm_consumers_in_decl_order: &[(String, Vec<ResolvedSubscription>)],
    defaults: &MessagingGlobalConfig,
) -> MessagingDirectory {
    let entries = build_channel_entries(raw_channels, defaults);
    finalize_directory_with_subscribers(
        entries,
        apps_in_decl_order,
        wasm_consumers_in_decl_order,
        &[],
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn global_defaults() -> MessagingGlobalConfig {
        MessagingGlobalConfig::default()
    }

    fn raw_channel(uuid: &str, address: &str) -> ChannelConfigRaw {
        ChannelConfigRaw {
            send_rate: None,
            uuid: Some(uuid.to_string()),
            address: Some(address.to_string()),
            address_prefix: None,
            description: None,
            push_depth: Some(Depth::Bounded(4)),
            retain_depth: Some(Depth::Bounded(4)),
            standing_retain_depth: Some(Depth::Bounded(4)),
            noise: None,
            sink: None,
            wake_min: None,
        }
    }

    #[test]
    fn build_channel_entries_round_trip() {
        let entries = build_channel_entries(
            &[
                raw_channel("1f6c6e3a-1d6e-4f7c-9b6a-12cb7e4a8d32", "pa-alice"),
                raw_channel("fe2f8b96-8b1c-4a44-a7c1-1ce1d76aa65d", "pa-bob"),
            ],
            &global_defaults(),
        );
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].address, "brenn:pa-alice");
        assert_eq!(entries[1].address, "brenn:pa-bob");
    }

    #[test]
    #[should_panic(expected = "duplicate [[channel]] uuid")]
    fn duplicate_uuid_panics() {
        build_channel_entries(
            &[
                raw_channel("1f6c6e3a-1d6e-4f7c-9b6a-12cb7e4a8d32", "a"),
                raw_channel("1f6c6e3a-1d6e-4f7c-9b6a-12cb7e4a8d32", "b"),
            ],
            &global_defaults(),
        );
    }

    #[test]
    #[should_panic(expected = "duplicate [[channel]] address")]
    fn duplicate_address_panics() {
        build_channel_entries(
            &[
                raw_channel("1f6c6e3a-1d6e-4f7c-9b6a-12cb7e4a8d32", "shared"),
                raw_channel("fe2f8b96-8b1c-4a44-a7c1-1ce1d76aa65d", "shared"),
            ],
            &global_defaults(),
        );
    }

    #[test]
    #[should_panic(expected = "is in a reserved namespace")]
    fn reserved_tool_namespace_channel_panics() {
        // A user channel squatting on the tool substrate's `.` boundary form is
        // rejected at load. The `/` form is not a squat but the address of a
        // channel the substrate mints, so a block naming it tunes instead.
        build_channel_entries(
            &[raw_channel(
                "1f6c6e3a-1d6e-4f7c-9b6a-12cb7e4a8d32",
                "tools.mine",
            )],
            &global_defaults(),
        );
    }

    // -----------------------------------------------------------------------
    // Non-durable `[[channel]]` blocks
    // -----------------------------------------------------------------------

    fn raw_nondurable(address: &str) -> ChannelConfigRaw {
        ChannelConfigRaw {
            send_rate: None,
            uuid: None,
            address: Some(address.to_string()),
            address_prefix: None,
            description: None,
            push_depth: Some(Depth::Bounded(2)),
            retain_depth: Some(Depth::Bounded(2)),
            standing_retain_depth: None,
            noise: None,
            sink: None,
            wake_min: None,
        }
    }

    #[test]
    fn channel_parses_bare_and_scheme_qualified() {
        let full: ChannelConfigRaw = toml::from_str(
            "address = \"ephemeral:protobar-demo\"\nretain_depth = 1\nnoise = \"metered\"\n",
        )
        .expect("scheme-qualified [[channel]] must parse");
        assert_eq!(full.address.as_deref(), Some("ephemeral:protobar-demo"));
        assert_eq!(full.retain_depth, Some(Depth::Bounded(1)));
        assert_eq!(full.uuid, None);

        let bare: ChannelConfigRaw =
            toml::from_str("uuid = \"1f6c6e3a-1d6e-4f7c-9b6a-12cb7e4a8d32\"\naddress = \"bare\"\n")
                .expect("bare [[channel]] must parse");
        assert_eq!(bare.address.as_deref(), Some("bare"));
        assert_eq!(bare.retain_depth, None);
    }

    /// The retired `[[ephemeral_channel]]` table is a boot panic, not a
    /// silently-ignored block: the config struct no longer carries the field and
    /// `deny_unknown_fields` rejects it.
    #[test]
    fn retired_ephemeral_channel_table_is_rejected() {
        let result: Result<crate::config::BrennConfig, _> =
            toml::from_str("[[ephemeral_channel]]\nname = \"dev-stub\"\nretain_depth = 1\n");
        let err = result.expect_err("[[ephemeral_channel]] must not parse");
        assert!(
            err.to_string().contains("ephemeral_channel"),
            "the rejection must name the retired table, got: {err}",
        );
    }

    #[test]
    fn channel_rejects_unknown_field() {
        let result: Result<ChannelConfigRaw, _> =
            toml::from_str("address = \"ephemeral:x\"\nbogus = 1\n");
        assert!(
            result.is_err(),
            "deny_unknown_fields must reject an unknown key"
        );
    }

    /// A config that still sets a removed depth default fails to parse rather
    /// than being ignored: an operator who wrote a number and got silence would
    /// believe their channels were sized.
    #[test]
    fn messaging_globals_reject_a_depth_default() {
        for key in [
            "default_push_depth",
            "default_retain_depth",
            "default_standing_retain_depth",
        ] {
            let result: Result<MessagingGlobalConfig, _> = toml::from_str(&format!("{key} = 8\n"));
            let err = result.expect_err("a removed depth default must not parse");
            assert!(
                err.to_string().contains(key),
                "the error must name the offending key; got: {err}"
            );
        }
    }

    #[test]
    fn nondurable_channel_applies_class_uniform_defaults() {
        let defaults = global_defaults();
        let entries = build_channel_entries(
            &[
                raw_nondurable("ephemeral:bare"),
                raw_nondurable("local:bare"),
            ],
            &defaults,
        );
        assert_eq!(entries.len(), 2);
        let eph = &entries[0];
        assert_eq!(eph.address, "ephemeral:bare");
        assert_eq!(eph.transport_type, ChannelScheme::Ephemeral);
        assert!(!eph.capabilities().durable);
        assert!(eph.capabilities().transportable);
        // Depths are the block's own on every scheme; noise/wake_min inherit the
        // global rung on every scheme.
        assert_eq!(eph.resolved_channel.push_depth, Depth::Bounded(2));
        assert_eq!(eph.resolved_channel.noise, defaults.default_noise);
        assert_eq!(eph.resolved_channel.wake_min, defaults.default_wake_min);
        // The retained window is the standing buffer: there is no separate
        // subscriber-independent store off-disk.
        assert_eq!(
            eph.resolved_channel.standing_retain_depth,
            eph.resolved_channel.retain_depth
        );
        assert_eq!(eph.resolved_channel.sink, Sink::Drop);
        assert_eq!(
            eph.uuid,
            super::super::ephemeral_channel_uuid_from_name("bare")
        );

        // `local:` is the same name in a distinct address space with a distinct
        // identity, and carries neither capability.
        let local = &entries[1];
        assert_eq!(local.address, "local:bare");
        assert!(!local.capabilities().durable);
        assert!(!local.capabilities().transportable);
        assert_ne!(local.uuid, eph.uuid);
    }

    #[test]
    fn nondurable_channel_resolves_explicit_values() {
        let entries = build_channel_entries(
            &[ChannelConfigRaw {
                send_rate: None,
                uuid: None,
                address: Some("ephemeral:keep-four".to_string()),
                address_prefix: None,
                description: None,
                push_depth: Some(Depth::Bounded(2)),
                retain_depth: Some(Depth::Bounded(4)),
                standing_retain_depth: None,
                noise: Some(NoiseLevel::Metered),
                sink: None,
                wake_min: None,
            }],
            &global_defaults(),
        );
        assert_eq!(entries[0].resolved_channel.push_depth, Depth::Bounded(2));
        assert_eq!(entries[0].resolved_channel.retain_depth, Depth::Bounded(4));
        assert_eq!(entries[0].resolved_channel.noise, NoiseLevel::Metered);
    }

    #[test]
    #[should_panic(expected = "duplicate [[channel]] address")]
    fn nondurable_duplicate_address_panics() {
        build_channel_entries(
            &[
                raw_nondurable("ephemeral:dup"),
                raw_nondurable("ephemeral:dup"),
            ],
            &global_defaults(),
        );
    }

    /// The same bare name under two schemes is two distinct channels, not a
    /// duplicate — the scheme is part of the address.
    #[test]
    fn same_name_under_two_schemes_is_two_channels() {
        let entries = build_channel_entries(
            &[
                raw_nondurable("ephemeral:twin"),
                raw_nondurable("local:twin"),
                raw_channel("1f6c6e3a-1d6e-4f7c-9b6a-12cb7e4a8d32", "twin"),
            ],
            &global_defaults(),
        );
        assert_eq!(entries.len(), 3);
    }

    #[test]
    #[should_panic(expected = "must name a channel after its scheme")]
    fn nondurable_empty_name_panics() {
        build_channel_entries(&[raw_nondurable("ephemeral:")], &global_defaults());
    }

    #[test]
    #[should_panic(expected = "RFC 3986")]
    fn nondurable_name_bad_charset_panics() {
        build_channel_entries(&[raw_nondurable("ephemeral:has space")], &global_defaults());
    }

    #[test]
    #[should_panic(expected = "retain_depth must be bounded")]
    fn nondurable_unbounded_retain_panics() {
        let mut raw = raw_nondurable("ephemeral:unbounded");
        raw.retain_depth = Some(Depth::Unbounded);
        build_channel_entries(&[raw], &global_defaults());
    }

    /// Every `[[channel]]` states its own depths; there is no rung under them.
    #[test]
    #[should_panic(expected = "requires retain_depth")]
    fn channel_without_retain_depth_panics() {
        let mut raw = raw_nondurable("ephemeral:no-retain");
        raw.retain_depth = None;
        build_channel_entries(&[raw], &global_defaults());
    }

    #[test]
    #[should_panic(expected = "requires push_depth")]
    fn channel_without_push_depth_panics() {
        let mut raw = raw_nondurable("ephemeral:no-push");
        raw.push_depth = None;
        build_channel_entries(&[raw], &global_defaults());
    }

    /// The standing buffer is the durable reaper's frontier, so a durable
    /// channel states it too — and only a durable one has a third number.
    #[test]
    #[should_panic(expected = "requires standing_retain_depth")]
    fn durable_channel_without_standing_retain_depth_panics() {
        let mut raw = raw_channel("1f6c6e3a-1d6e-4f7c-9b6a-12cb7e4a8d32", "no-standing");
        raw.standing_retain_depth = None;
        build_channel_entries(&[raw], &global_defaults());
    }

    /// The standing buffer is the ceiling on every depth stated about the
    /// channel, starting with the channel's own two rungs.
    #[test]
    #[should_panic(expected = "push_depth Bounded(9) exceeds standing_retain_depth Bounded(4)")]
    fn a_channel_push_rung_above_standing_panics() {
        let mut raw = raw_channel("1f6c6e3a-1d6e-4f7c-9b6a-12cb7e4a8d32", "over-push");
        raw.push_depth = Some(Depth::Bounded(9));
        build_channel_entries(&[raw], &global_defaults());
    }

    #[test]
    #[should_panic(expected = "retain_depth Unbounded exceeds standing_retain_depth Bounded(4)")]
    fn a_channel_retain_rung_above_standing_panics() {
        let mut raw = raw_channel("1f6c6e3a-1d6e-4f7c-9b6a-12cb7e4a8d32", "over-retain");
        raw.retain_depth = Some(Depth::Unbounded);
        build_channel_entries(&[raw], &global_defaults());
    }

    /// Equality is within the ceiling, and an explicitly-unbounded standing
    /// admits unbounded rungs — the operator asked for it in so many words.
    #[test]
    fn rungs_at_the_ceiling_and_under_an_unbounded_one_are_fine() {
        let mut at = raw_channel("1f6c6e3a-1d6e-4f7c-9b6a-12cb7e4a8d32", "at-ceiling");
        at.push_depth = Some(Depth::Bounded(4));
        at.retain_depth = Some(Depth::Bounded(4));
        let mut open = raw_channel("2f6c6e3a-1d6e-4f7c-9b6a-12cb7e4a8d32", "no-ceiling");
        open.push_depth = Some(Depth::Unbounded);
        open.retain_depth = Some(Depth::Unbounded);
        open.standing_retain_depth = Some(Depth::Unbounded);
        let entries = build_channel_entries(&[at, open], &global_defaults());
        assert_eq!(entries.len(), 2);
    }

    /// The non-durable arm of the ceiling. A non-durable block states no
    /// standing depth — `retain_depth` *is* the ceiling — so `push_depth` above
    /// it is a boot panic for every `ephemeral:` and `local:` channel. Without
    /// this, a regression that stopped folding retain into standing would
    /// silently unbind the ceiling on the whole non-durable half and
    /// `reap_frontier` would answer `None` for all of them.
    #[test]
    #[should_panic(expected = "push_depth Bounded(8) exceeds standing_retain_depth Bounded(1)")]
    fn a_nondurable_push_rung_above_its_retained_window_panics() {
        let mut raw = raw_nondurable("ephemeral:tight");
        raw.push_depth = Some(Depth::Bounded(8));
        raw.retain_depth = Some(Depth::Bounded(1));
        build_channel_entries(&[raw], &global_defaults());
    }

    /// The positive half: push within the window resolves, and the resulting
    /// standing depth is the window itself.
    #[test]
    fn a_nondurable_channels_standing_depth_is_its_retained_window() {
        let mut raw = raw_nondurable("ephemeral:sized");
        raw.push_depth = Some(Depth::Bounded(2));
        raw.retain_depth = Some(Depth::Bounded(8));
        let entries = build_channel_entries(&[raw], &global_defaults());
        assert_eq!(entries.len(), 1);
        assert_eq!(
            entries[0].resolved_channel.standing_retain_depth,
            Depth::Bounded(8),
            "a non-durable channel's standing buffer is its retained window",
        );
    }

    /// A tuning block is held to the same ceiling as a declaring one.
    #[test]
    #[should_panic(
        expected = "retain_depth Bounded(200) exceeds standing_retain_depth Bounded(16)"
    )]
    fn a_tuning_block_above_its_own_standing_panics() {
        let mut raw = raw_nondurable("brenn:tools/apull");
        raw.uuid = None;
        raw.push_depth = Some(Depth::Bounded(1));
        raw.retain_depth = Some(Depth::Bounded(200));
        raw.standing_retain_depth = Some(Depth::Bounded(16));
        tuning_of(&[raw]);
    }

    /// The directory-wide half of the ceiling: a subscriber over its channel's
    /// standing depth refuses to boot, naming channel and subscriber.
    #[test]
    #[should_panic(expected = "has push_depth Bounded(7) exceeding")]
    fn a_subscriber_push_depth_above_standing_refuses_to_boot() {
        let mut entry = build_channel_entries(
            &[raw_channel("1f6c6e3a-1d6e-4f7c-9b6a-12cb7e4a8d32", "ch")],
            &global_defaults(),
        )
        .pop()
        .expect("one entry");
        entry.subscribers.push(crate::messaging::SubscriberEntry {
            kind: SubscriberEntryKind::App("greedy".to_string()),
            push_depth: Depth::Bounded(7),
            retain_depth: Depth::Bounded(1),
            noise: NoiseLevel::Silent,
            wake_min: Some(WakeMin::Normal),
        });
        validate_subscriber_depth_ceilings(&MessagingDirectory::with_entries(vec![entry]));
    }

    #[test]
    #[should_panic(expected = "has retain_depth Unbounded exceeding")]
    fn a_subscriber_retain_depth_above_standing_refuses_to_boot() {
        let mut entry = build_channel_entries(
            &[raw_channel("1f6c6e3a-1d6e-4f7c-9b6a-12cb7e4a8d32", "ch")],
            &global_defaults(),
        )
        .pop()
        .expect("one entry");
        entry.subscribers.push(crate::messaging::SubscriberEntry {
            kind: SubscriberEntryKind::Wasm("greedy".to_string()),
            push_depth: Depth::Bounded(1),
            retain_depth: Depth::Unbounded,
            noise: NoiseLevel::Silent,
            wake_min: Some(WakeMin::Normal),
        });
        validate_subscriber_depth_ceilings(&MessagingDirectory::with_entries(vec![entry]));
    }

    /// The panic names the channel and the subscriber so the operator knows
    /// which block to edit.
    #[test]
    fn the_ceiling_panic_names_the_channel_and_the_subscriber() {
        let mut entry = build_channel_entries(
            &[raw_channel("1f6c6e3a-1d6e-4f7c-9b6a-12cb7e4a8d32", "ch")],
            &global_defaults(),
        )
        .pop()
        .expect("one entry");
        entry.subscribers.push(crate::messaging::SubscriberEntry {
            kind: SubscriberEntryKind::App("greedy".to_string()),
            push_depth: Depth::Bounded(7),
            retain_depth: Depth::Bounded(1),
            noise: NoiseLevel::Silent,
            wake_min: Some(WakeMin::Normal),
        });
        let dir = MessagingDirectory::with_entries(vec![entry]);
        let err = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            validate_subscriber_depth_ceilings(&dir)
        }))
        .expect_err("the over-ceiling subscriber refuses to boot");
        let msg = err
            .downcast_ref::<String>()
            .expect("panic payload is a String");
        assert!(msg.contains("brenn:ch"), "names the channel: {msg}");
        assert!(msg.contains("greedy"), "names the subscriber: {msg}");
    }

    /// Subscribers at or under the ceiling pass, and an unbounded standing
    /// admits an unbounded subscriber.
    #[test]
    fn subscribers_within_the_ceiling_boot() {
        let mut bounded = build_channel_entries(
            &[raw_channel("1f6c6e3a-1d6e-4f7c-9b6a-12cb7e4a8d32", "ch")],
            &global_defaults(),
        )
        .pop()
        .expect("one entry");
        bounded.subscribers.push(crate::messaging::SubscriberEntry {
            kind: SubscriberEntryKind::App("exact".to_string()),
            push_depth: Depth::Bounded(4),
            retain_depth: Depth::Bounded(4),
            noise: NoiseLevel::Silent,
            wake_min: Some(WakeMin::Normal),
        });
        let mut open = raw_channel("2f6c6e3a-1d6e-4f7c-9b6a-12cb7e4a8d32", "open");
        open.standing_retain_depth = Some(Depth::Unbounded);
        let mut open = build_channel_entries(&[open], &global_defaults())
            .pop()
            .expect("one entry");
        open.subscribers.push(crate::messaging::SubscriberEntry {
            kind: SubscriberEntryKind::App("deep".to_string()),
            push_depth: Depth::Unbounded,
            retain_depth: Depth::Unbounded,
            noise: NoiseLevel::Silent,
            wake_min: Some(WakeMin::Normal),
        });
        validate_subscriber_depth_ceilings(&MessagingDirectory::with_entries(vec![bounded, open]));
    }

    #[test]
    #[should_panic(expected = "requires a uuid")]
    fn durable_channel_without_uuid_panics() {
        let raw = raw_nondurable("brenn:needs-uuid");
        build_channel_entries(&[raw], &global_defaults());
    }

    #[test]
    #[should_panic(expected = "must not set uuid")]
    fn nondurable_channel_with_uuid_panics() {
        let mut raw = raw_nondurable("ephemeral:has-uuid");
        raw.uuid = Some("1f6c6e3a-1d6e-4f7c-9b6a-12cb7e4a8d32".to_string());
        build_channel_entries(&[raw], &global_defaults());
    }

    /// A zero refill interval is rejected at boot (it would panic lazily on
    /// first publish).
    #[test]
    #[should_panic(expected = "send_rate.refill_interval_secs must be >= 1")]
    fn channel_zero_refill_interval_send_rate_panics() {
        let mut raw = raw_nondurable("ephemeral:zero-interval");
        raw.retain_depth = Some(Depth::Bounded(4));
        raw.send_rate = Some(SendRate {
            burst: 10,
            refill_interval_secs: 0,
            refill: 10,
        });
        build_channel_entries(&[raw], &global_defaults());
    }

    /// A zero burst is a silent deny-all — rejected at boot.
    #[test]
    #[should_panic(expected = "send_rate.burst must be >= 1")]
    fn channel_zero_burst_send_rate_panics() {
        let mut raw = raw_nondurable("ephemeral:zero-burst");
        raw.retain_depth = Some(Depth::Bounded(4));
        raw.send_rate = Some(SendRate {
            burst: 0,
            refill_interval_secs: 1,
            refill: 10,
        });
        build_channel_entries(&[raw], &global_defaults());
    }

    /// An invalid `default_send_rate` is caught at boot even with no
    /// `[[channel]]` blocks.
    #[test]
    #[should_panic(expected = "[messaging].default_send_rate send_rate.refill must be >= 1")]
    fn zero_refill_default_send_rate_panics_without_channels() {
        let mut defaults = global_defaults();
        defaults.default_send_rate = SendRate {
            burst: 10,
            refill_interval_secs: 1,
            refill: 0,
        };
        build_channel_entries(&[], &defaults);
    }

    #[test]
    #[should_panic(expected = "must not set standing_retain_depth")]
    fn nondurable_channel_with_standing_retain_panics() {
        let mut raw = raw_nondurable("ephemeral:standing");
        raw.standing_retain_depth = Some(Depth::Bounded(4));
        build_channel_entries(&[raw], &global_defaults());
    }

    #[test]
    #[should_panic(expected = "must not set sink")]
    fn nondurable_channel_with_sink_panics() {
        let mut raw = raw_nondurable("ephemeral:sunk");
        raw.sink = Some(Sink::Drop);
        build_channel_entries(&[raw], &global_defaults());
    }

    #[test]
    #[should_panic(expected = "declares nothing")]
    fn pwa_push_scheme_in_channel_table_panics() {
        build_channel_entries(&[raw_nondurable("pwa_push:device")], &global_defaults());
    }

    // -----------------------------------------------------------------------
    // System-minted channels: family defaults and `[[channel]]` tuning
    // -----------------------------------------------------------------------

    /// A tuning block keyed by exact address, with the three required depths.
    fn tuning_block(address: &str, push: u64, retain: u64, standing: u64) -> ChannelConfigRaw {
        ChannelConfigRaw {
            send_rate: None,
            uuid: None,
            address: Some(address.to_string()),
            address_prefix: None,
            description: None,
            push_depth: Some(Depth::Bounded(push)),
            retain_depth: Some(Depth::Bounded(retain)),
            standing_retain_depth: Some(Depth::Bounded(standing)),
            noise: None,
            sink: None,
            wake_min: None,
        }
    }

    /// A tuning block keyed by prefix.
    fn tuning_prefix(prefix: &str, push: u64, retain: u64, standing: u64) -> ChannelConfigRaw {
        ChannelConfigRaw {
            address: None,
            address_prefix: Some(prefix.to_string()),
            ..tuning_block("webhook:placeholder", push, retain, standing)
        }
    }

    /// Build the tuning table against the stock globals.
    fn tuning_of(raw: &[ChannelConfigRaw]) -> SystemChannelTuning {
        build_system_channel_tuning(raw, &global_defaults())
    }

    /// With no tuning block, each family takes its own bounded in-code default
    /// and the non-depth knobs follow the `[messaging]` globals.
    #[test]
    fn an_untuned_system_channel_takes_its_family_default() {
        let tuning = SystemChannelTuning::default();
        let defaults = MessagingGlobalConfig {
            default_noise: NoiseLevel::Metered,
            default_wake_min: WakeMin::High,
            ..global_defaults()
        };

        for ingress in ["webhook:github", "mqtt:home:sensors/temp"] {
            let ch = resolve_system_channel(ingress, &tuning, &defaults);
            assert_eq!(
                ch.push_depth, SYSTEM_CHANNEL_DEFAULT_PUSH_DEPTH,
                "{ingress}"
            );
            assert_eq!(ch.retain_depth, INGRESS_DEFAULT_RETAIN_DEPTH, "{ingress}");
            assert_eq!(
                ch.standing_retain_depth, INGRESS_DEFAULT_RETAIN_DEPTH,
                "{ingress}"
            );
            assert_eq!(ch.noise, NoiseLevel::Metered, "{ingress}");
            assert_eq!(ch.wake_min, WakeMin::High, "{ingress}");
        }
        for tool in ["brenn:tools/git-repo-pull", "brenn:tool-results/sync"] {
            let ch = resolve_system_channel(tool, &tuning, &defaults);
            assert_eq!(ch.push_depth, SYSTEM_CHANNEL_DEFAULT_PUSH_DEPTH, "{tool}");
            assert_eq!(ch.retain_depth, TOOL_CHANNEL_DEFAULT_RETAIN_DEPTH, "{tool}");
            assert_eq!(
                ch.standing_retain_depth, TOOL_CHANNEL_DEFAULT_RETAIN_DEPTH,
                "{tool}"
            );
        }
        // Nothing a family default states is unbounded.
        for address in ["webhook:github", "brenn:tools/git-repo-pull"] {
            let ch = resolve_system_channel(address, &tuning, &defaults);
            assert_ne!(ch.retain_depth, Depth::Unbounded, "{address}");
            assert_ne!(ch.standing_retain_depth, Depth::Unbounded, "{address}");
        }
    }

    /// A tuning block mints nothing: the declaring pass skips it, so an
    /// operator tuning `webhook:hook` does not conjure a second channel.
    #[test]
    fn a_tuning_block_mints_no_channel_entry() {
        let raw = vec![
            raw_channel("1f6c6e3a-1d6e-4f7c-9b6a-12cb7e4a8d32", "pa-alice"),
            tuning_block("webhook:hook", 1, 8, 8),
            tuning_block("mqtt:home:sensors/temp", 1, 8, 8),
            tuning_block("brenn:tools/git-repo-pull", 1, 8, 8),
            tuning_prefix("webhook:", 1, 8, 8),
        ];
        let entries = build_channel_entries(&raw, &global_defaults());
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].address, "brenn:pa-alice");
        // And the tuning pass claims exactly the blocks the declaring pass left.
        let tuning = tuning_of(&raw);
        let mut exact: Vec<&str> = tuning.exact_addresses().collect();
        exact.sort_unstable();
        assert_eq!(
            exact,
            vec![
                "brenn:tools/git-repo-pull",
                "mqtt:home:sensors/temp",
                "webhook:hook",
            ]
        );
    }

    /// Precedence: an exact block beats a prefix block beats the family default.
    #[test]
    fn resolution_is_exact_then_prefix_then_family_default() {
        let tuning = tuning_of(&[
            tuning_prefix("webhook:", 1, 20, 20),
            tuning_block("webhook:github", 2, 30, 30),
        ]);
        let defaults = global_defaults();

        let exact = resolve_system_channel("webhook:github", &tuning, &defaults);
        assert_eq!(exact.retain_depth, Depth::Bounded(30));
        assert_eq!(exact.push_depth, Depth::Bounded(2));

        let by_prefix = resolve_system_channel("webhook:gitlab", &tuning, &defaults);
        assert_eq!(by_prefix.retain_depth, Depth::Bounded(20));

        let untouched = resolve_system_channel("brenn:tools/apull", &tuning, &defaults);
        assert_eq!(untouched.retain_depth, TOOL_CHANNEL_DEFAULT_RETAIN_DEPTH);
    }

    /// Two prefixes both matching an address: the longer one wins, whatever the
    /// declaration order.
    #[test]
    fn the_longest_matching_prefix_wins() {
        let tuning = tuning_of(&[
            tuning_prefix("mqtt:home:sensors/", 1, 40, 40),
            tuning_prefix("mqtt:", 1, 5, 5),
        ]);
        let defaults = global_defaults();
        assert_eq!(
            resolve_system_channel("mqtt:home:sensors/temp", &tuning, &defaults).retain_depth,
            Depth::Bounded(40),
        );
        assert_eq!(
            resolve_system_channel("mqtt:home:lights/kitchen", &tuning, &defaults).retain_depth,
            Depth::Bounded(5),
        );
    }

    /// A prefix block matching nothing is legal — it is a standing rule for a
    /// family whose membership is dynamic.
    #[test]
    fn a_prefix_matching_nothing_is_legal() {
        let tuning = tuning_of(&[tuning_prefix("mqtt:absent:", 1, 40, 40)]);
        assert_eq!(
            resolve_system_channel("mqtt:home:temp", &tuning, &global_defaults()).retain_depth,
            INGRESS_DEFAULT_RETAIN_DEPTH,
        );
    }

    /// An operator asking for unbounded retention in so many words gets it —
    /// the one route to `Unbounded` a system channel has.
    #[test]
    fn a_tuning_block_may_state_unbounded() {
        let mut block = tuning_block("webhook:archive", 1, 8, 8);
        block.retain_depth = Some(Depth::Unbounded);
        block.standing_retain_depth = Some(Depth::Unbounded);
        let tuning = tuning_of(&[block]);
        let ch = resolve_system_channel("webhook:archive", &tuning, &global_defaults());
        assert_eq!(ch.retain_depth, Depth::Unbounded);
        assert_eq!(ch.standing_retain_depth, Depth::Unbounded);
    }

    /// A tuning block's optional knobs override the globals; omitted ones
    /// inherit exactly as a declaring block's do.
    #[test]
    fn a_tuning_block_overrides_the_globals_it_states() {
        let rate = SendRate {
            burst: 7,
            refill_interval_secs: 1,
            refill: 3,
        };
        let mut block = tuning_block("webhook:hook", 1, 8, 8);
        block.noise = Some(NoiseLevel::Alarm);
        block.sink = Some(Sink::Archive);
        block.send_rate = Some(rate);
        let defaults = MessagingGlobalConfig {
            default_noise: NoiseLevel::Metered,
            default_wake_min: WakeMin::High,
            archive_path: Some(std::path::PathBuf::from("/tmp/archive")),
            ..global_defaults()
        };
        let tuning = build_system_channel_tuning(&[block], &defaults);
        let ch = resolve_system_channel("webhook:hook", &tuning, &defaults);
        assert_eq!(ch.noise, NoiseLevel::Alarm);
        assert_eq!(ch.sink, Sink::Archive);
        assert_eq!(ch.send_rate, rate);
        assert_eq!(ch.wake_min, WakeMin::High);
    }

    /// A tuning block is the one place an operator reaches `sink` on a
    /// system-minted channel. Without the pairing check here, the panic lands
    /// in the hourly GC pass.
    #[test]
    #[should_panic(expected = "archive_path is not set")]
    fn a_tuning_block_archiving_with_nowhere_to_archive_panics() {
        let mut block = tuning_block("webhook:hook", 1, 8, 8);
        block.sink = Some(Sink::Archive);
        build_system_channel_tuning(&[block], &global_defaults());
    }

    /// A system-minted channel that retains nothing leaves its system
    /// participants with no position at all, and the only symptom is a much
    /// later panic blaming the host. Refused at load, naming the block.
    #[test]
    #[should_panic(expected = "sets retain_depth = 0")]
    fn a_tuning_block_with_a_zero_window_panics() {
        tuning_of(&[tuning_block("brenn:tools/apull", 0, 0, 0)]);
    }

    /// An exact `mqtt:` key is exempt from the existence check, so its shape is
    /// the only thing checkable — and a spelling no mint path could produce
    /// would tune nothing, forever, which is the silent no-op the existence
    /// check exists to prevent.
    #[test]
    #[should_panic(expected = "not a well-formed mqtt:<client>:<topic> address")]
    fn an_exact_mqtt_key_that_names_no_possible_channel_panics() {
        tuning_of(&[tuning_block("mqtt:home/sensors/temp", 1, 8, 8)]);
    }

    /// Same for a key whose topic is not a legal filter.
    #[test]
    #[should_panic(expected = "not a valid MQTT topic filter")]
    fn an_exact_mqtt_key_with_a_bad_wildcard_panics() {
        tuning_of(&[tuning_block("mqtt:home:sensors/#/temp", 1, 8, 8)]);
    }

    /// The well-formed spelling still tunes the channel the mint path derives.
    #[test]
    fn an_exact_mqtt_key_reaches_the_minted_address() {
        let address = "mqtt:home:sensors/+/temp";
        let tuning = tuning_of(&[tuning_block(address, 1, 8, 8)]);
        let ch = resolve_system_channel(address, &tuning, &global_defaults());
        assert_eq!(ch.retain_depth, Depth::Bounded(8));
    }

    #[test]
    #[should_panic(expected = "must not set uuid")]
    fn a_tuning_block_with_a_uuid_panics() {
        let mut block = tuning_block("webhook:hook", 1, 8, 8);
        block.uuid = Some("1f6c6e3a-1d6e-4f7c-9b6a-12cb7e4a8d32".to_string());
        tuning_of(&[block]);
    }

    #[test]
    #[should_panic(expected = "must not set description")]
    fn a_tuning_block_with_a_description_panics() {
        let mut block = tuning_block("webhook:hook", 1, 8, 8);
        block.description = Some("mine now".to_string());
        tuning_of(&[block]);
    }

    #[test]
    #[should_panic(expected = "requires standing_retain_depth")]
    fn a_tuning_block_without_every_depth_panics() {
        let mut block = tuning_block("webhook:hook", 1, 8, 8);
        block.standing_retain_depth = None;
        tuning_of(&[block]);
    }

    #[test]
    #[should_panic(expected = "duplicate [[channel]] address")]
    fn duplicate_exact_tuning_blocks_panic() {
        tuning_of(&[
            tuning_block("webhook:hook", 1, 8, 8),
            tuning_block("webhook:hook", 1, 9, 9),
        ]);
    }

    #[test]
    #[should_panic(expected = "duplicate [[channel]] address_prefix")]
    fn duplicate_tuning_prefixes_panic() {
        tuning_of(&[
            tuning_prefix("webhook:", 1, 8, 8),
            tuning_prefix("webhook:", 1, 9, 9),
        ]);
    }

    #[test]
    #[should_panic(expected = "must end at a segment boundary")]
    fn a_prefix_that_stops_mid_segment_panics() {
        tuning_of(&[tuning_prefix("webhook:git", 1, 8, 8)]);
    }

    #[test]
    #[should_panic(expected = "names no system-minted family")]
    fn a_prefix_outside_the_system_families_panics() {
        tuning_of(&[tuning_prefix("brenn:alerts/", 1, 8, 8)]);
    }

    /// A tool namespace exists on `brenn:` only, so a prefix carrying another
    /// scheme names no family — it could byte-match no minted address, and the
    /// operator who wrote it meant the tool channels.
    #[test]
    #[should_panic(expected = "names no system-minted family")]
    fn a_tool_prefix_on_another_scheme_panics() {
        tuning_of(&[tuning_prefix("ephemeral:tools/", 1, 8, 8)]);
    }

    /// Bare is the house spelling for a `brenn:` channel, and minted tool
    /// addresses are canonical: a bare tuning key reaches the channel it names
    /// rather than tuning nothing.
    #[test]
    fn a_bare_tuning_key_reaches_the_minted_channel() {
        let tuning = tuning_of(&[
            tuning_block("tools/apull", 1, 4, 4),
            tuning_prefix("tool-results/", 1, 6, 6),
        ]);
        let defaults = global_defaults();
        assert_eq!(
            tuning.exact_addresses().collect::<Vec<_>>(),
            vec!["brenn:tools/apull"],
        );
        assert_eq!(
            resolve_system_channel("brenn:tools/apull", &tuning, &defaults).retain_depth,
            Depth::Bounded(4),
        );
        assert_eq!(
            resolve_system_channel("brenn:tool-results/sync", &tuning, &defaults).retain_depth,
            Depth::Bounded(6),
        );
    }

    /// The two spellings of one address are one key, so a config stating both is
    /// a duplicate rather than two blocks that silently disagree.
    #[test]
    #[should_panic(expected = "duplicate [[channel]] address")]
    fn the_bare_and_qualified_spellings_of_one_address_collide() {
        tuning_of(&[
            tuning_block("tools/apull", 1, 4, 4),
            tuning_block("brenn:tools/apull", 1, 8, 8),
        ]);
    }

    #[test]
    #[should_panic(expected = "sets both address")]
    fn a_block_keyed_twice_panics() {
        let mut block = tuning_block("webhook:hook", 1, 8, 8);
        block.address_prefix = Some("webhook:".to_string());
        tuning_of(&[block]);
    }

    #[test]
    #[should_panic(expected = "sets neither address nor address_prefix")]
    fn a_block_keyed_by_nothing_panics() {
        let mut block = tuning_block("webhook:hook", 1, 8, 8);
        block.address = None;
        tuning_of(&[block]);
    }

    /// The `.` boundary form of a tool namespace is a squat, not an address the
    /// substrate mints, so it stays a declaring block (and stays rejected).
    #[test]
    fn only_the_slash_form_of_a_tool_namespace_is_system_minted() {
        assert_eq!(
            SystemChannelFamily::of("brenn:tools/apull"),
            Some(SystemChannelFamily::Tool)
        );
        assert_eq!(
            SystemChannelFamily::of("tool-results/sync"),
            Some(SystemChannelFamily::Tool)
        );
        assert_eq!(SystemChannelFamily::of("brenn:tools.mine"), None);
        assert_eq!(SystemChannelFamily::of("brenn:toolsmith"), None);
        assert_eq!(SystemChannelFamily::of("ephemeral:tools/apull"), None);
        assert_eq!(
            channel_block_role("brenn:alerts"),
            ChannelBlockRole::Declaring
        );
        assert_eq!(channel_block_role("webhook:hook"), ChannelBlockRole::Tuning);
    }

    /// A misspelled scheme must not register as a `brenn:` channel whose name
    /// happens to contain a colon.
    #[test]
    #[should_panic(expected = "unrecognized scheme")]
    fn misspelled_scheme_in_channel_table_panics() {
        build_channel_entries(&[raw_nondurable("ephmeral:room")], &global_defaults());
    }

    // -----------------------------------------------------------------------
    // [[surface]] raw config types (parsing only; resolution and
    // boot cross-validation are done separately)
    // -----------------------------------------------------------------------

    /// A fully-populated `[[surface]]` block — every ACL list, all four grant
    /// tokens, and the nested component/subscription/output arrays — parses,
    /// with scheme-qualified binding channels and bare-name ACL matchers.
    #[test]
    fn surface_parses_full() {
        use crate::access::raw::ChannelMatcherRaw;
        let toml_str = r#"
slug = "deskbar"
grants = ["subscribe", "publish", "ephemeral_subscribe", "ephemeral_publish"]
subscribe_acl = [{ exact = "alerts.high" }]
publish_acl = [{ prefix = "cmd." }]
ephemeral_subscribe_acl = [{ exact = "protobar-demo" }]
ephemeral_publish_acl = [{ exact = "protobar-demo" }]
allowed_users = ["alice"]
publish_burst = 120
publish_per_sec = 5
skin = "foundry"

[[component]]
kind = "protobar"
abi = "dom"

[[subscription]]
channel = "ephemeral:protobar-demo"
instance = "protobar"
port = "messages"

[[output]]
instance = "protobar"
port = "out"
channel = "brenn:alerts.high"
"#;
        let raw: SurfaceConfigRaw = toml::from_str(toml_str).expect("full [[surface]] must parse");
        assert_eq!(raw.slug, "deskbar");
        assert_eq!(
            raw.grants,
            vec![
                SurfaceGrant::Subscribe,
                SurfaceGrant::Publish,
                SurfaceGrant::EphemeralSubscribe,
                SurfaceGrant::EphemeralPublish,
            ]
        );
        // ACL matchers are bare names, no scheme.
        assert!(matches!(
            raw.subscribe_acl.as_slice(),
            [ChannelMatcherRaw::Exact(e)] if e == "alerts.high"
        ));
        assert!(matches!(
            raw.publish_acl.as_slice(),
            [ChannelMatcherRaw::Prefix(p)] if p == "cmd."
        ));
        assert!(matches!(
            raw.ephemeral_subscribe_acl.as_slice(),
            [ChannelMatcherRaw::Exact(e)] if e == "protobar-demo"
        ));
        assert!(matches!(
            raw.ephemeral_publish_acl.as_slice(),
            [ChannelMatcherRaw::Exact(e)] if e == "protobar-demo"
        ));
        assert_eq!(raw.components.len(), 1);
        assert_eq!(raw.components[0].kind, "protobar");
        assert_eq!(raw.components[0].instance, None);
        // subscription.channel is scheme-qualified.
        assert_eq!(raw.subscriptions.len(), 1);
        assert_eq!(
            raw.subscriptions[0].channel.as_deref(),
            Some("ephemeral:protobar-demo")
        );
        assert_eq!(raw.subscriptions[0].instance, "protobar");
        assert_eq!(raw.subscriptions[0].port, "messages");
        assert_eq!(raw.outputs.len(), 1);
        assert_eq!(raw.outputs[0].channel.as_deref(), Some("brenn:alerts.high"));
        assert_eq!(raw.outputs[0].instance, "protobar");
        assert_eq!(raw.outputs[0].port, "out");
        // Access check + publish-budget caps parse from TOML.
        assert_eq!(raw.allowed_users, vec!["alice".to_string()]);
        assert_eq!(raw.publish_burst, Some(120));
        assert_eq!(raw.publish_per_sec, Some(5));
        // Skin parses.
        assert_eq!(raw.skin.as_deref(), Some("foundry"));
    }

    /// Minimal `[[surface]]`: only slug + grants; every ACL list and nested
    /// array defaults to empty (`#[serde(default)]`).
    #[test]
    fn surface_parses_minimal() {
        let raw: SurfaceConfigRaw = toml::from_str("slug = \"bare\"\ngrants = []\n")
            .expect("minimal [[surface]] must parse");
        assert_eq!(raw.slug, "bare");
        assert!(raw.grants.is_empty());
        assert!(raw.subscribe_acl.is_empty());
        assert!(raw.publish_acl.is_empty());
        assert!(raw.ephemeral_subscribe_acl.is_empty());
        assert!(raw.ephemeral_publish_acl.is_empty());
        assert!(raw.components.is_empty());
        assert!(raw.subscriptions.is_empty());
        assert!(raw.outputs.is_empty());
        assert!(raw.io_ports.is_empty());
        // Access check + publish-budget caps default when omitted.
        assert!(raw.allowed_users.is_empty());
        assert!(raw.publish_burst.is_none());
        assert!(raw.publish_per_sec.is_none());
        // Skin absent when omitted.
        assert!(raw.skin.is_none());
    }

    /// `grants` is required (no `#[serde(default)]`, like `[[wasm_consumer]]`):
    /// omitting it is a serde error, not a silent zero-grant surface.
    #[test]
    fn surface_missing_grants_rejected() {
        let result: Result<SurfaceConfigRaw, _> = toml::from_str("slug = \"x\"\n");
        assert!(
            result.is_err(),
            "missing grants field must be a serde error"
        );
    }

    /// A full `[[connection]]`: endpoint refs in both forms, plus the named
    /// durable channel and its rename-stability uuid.
    #[test]
    fn connection_parses_full() {
        let toml_str = r#"
endpoints = ["wasm:etl/batch-out", "wasm:indexer/batch-in", "surface:bench#monitor/batch-tap"]
channel = "brenn:etl.batches"
uuid = "9c1f4a4e-6d38-4a2e-9f1a-2f7c0d5b8e31"
description = "ETL batch hand-off"
"#;
        let raw: ConnectionConfigRaw =
            toml::from_str(toml_str).expect("full [[connection]] must parse");
        assert_eq!(
            raw.endpoints,
            vec![
                "wasm:etl/batch-out".to_string(),
                "wasm:indexer/batch-in".to_string(),
                "surface:bench#monitor/batch-tap".to_string(),
            ]
        );
        assert_eq!(raw.channel.as_deref(), Some("brenn:etl.batches"));
        assert_eq!(
            raw.uuid.as_deref(),
            Some("9c1f4a4e-6d38-4a2e-9f1a-2f7c0d5b8e31")
        );
        assert_eq!(raw.description.as_deref(), Some("ETL batch hand-off"));
    }

    /// The anonymous spelling: endpoints alone, all optional fields absent.
    #[test]
    fn connection_parses_anonymous() {
        let raw: ConnectionConfigRaw =
            toml::from_str("endpoints = [\"wasm:a/out\", \"wasm:b/in\"]\n")
                .expect("anonymous [[connection]] must parse");
        assert_eq!(raw.endpoints.len(), 2);
        assert!(raw.channel.is_none());
        assert!(raw.uuid.is_none());
        assert!(raw.description.is_none());
    }

    /// `endpoints` is required and unknown keys are rejected: a connection with
    /// no endpoints declares nothing, and a typo'd knob would silently do
    /// nothing.
    #[test]
    fn connection_rejects_missing_endpoints_and_unknown_fields() {
        let missing: Result<ConnectionConfigRaw, _> = toml::from_str("channel = \"brenn:x\"\n");
        assert!(missing.is_err(), "endpoints must be required");
        let unknown: Result<ConnectionConfigRaw, _> =
            toml::from_str("endpoints = [\"wasm:a/out\"]\nretain_depth = 4\n");
        assert!(
            unknown.is_err(),
            "channel-level tuning is not available on a [[connection]]"
        );
    }

    /// A `[[wasm_consumer.io_port]]` carries both directions' tuning under one
    /// port name, and nests inside `[[wasm_consumer]]` as `io_port`.
    #[test]
    fn wasm_consumer_io_port_parses() {
        let toml_str = r#"
slug = "etl"
component_path = "/opt/brenn/etl.wasm"
grants = ["ports"]

[[io_port]]
port = "timer"
push_depth = 2
retain_depth = 8
noise = "metered"
amplification = 0.5
urgency = "low"
publish_per_activation = 2.0
publish_capacity = 4.0
channel = "brenn:etl.timer"
"#;
        let raw: WasmConsumerConfigRaw =
            toml::from_str(toml_str).expect("[[wasm_consumer.io_port]] must parse");
        assert_eq!(raw.io_ports.len(), 1);
        let io = &raw.io_ports[0];
        assert_eq!(io.port, "timer");
        assert_eq!(io.push_depth, Some(Depth::Bounded(2)));
        assert_eq!(io.retain_depth, Some(Depth::Bounded(8)));
        assert_eq!(io.noise, Some(NoiseLevel::Metered));
        assert_eq!(io.amplification, Some(0.5));
        assert_eq!(io.urgency, Some(crate::messaging::Urgency::Low));
        assert_eq!(io.publish_per_activation, Some(2.0));
        assert_eq!(io.publish_capacity, Some(4.0));
        assert_eq!(io.channel.as_deref(), Some("brenn:etl.timer"));
    }

    /// The zero-config spelling: a port name and nothing else. Also pins that
    /// `wake_min` is rejected — it has no referent on a WASM subscription.
    #[test]
    fn wasm_consumer_io_port_minimal_and_rejects_wake_min() {
        let raw: WasmConsumerIoPortRaw =
            toml::from_str("port = \"timer\"\n").expect("bare io_port must parse");
        assert_eq!(raw.port, "timer");
        assert!(raw.channel.is_none());
        assert!(raw.push_depth.is_none());
        assert!(raw.retain_depth.is_none());
        let with_wake: Result<WasmConsumerIoPortRaw, _> =
            toml::from_str("port = \"timer\"\nwake_min = \"normal\"\n");
        assert!(with_wake.is_err(), "wake_min must not be accepted");
    }

    /// A `[[surface.io_port]]` names the owning instance and nests inside
    /// `[[surface]]` as `io_port`; with no channel it is the page-local default.
    #[test]
    fn surface_io_port_parses() {
        let toml_str = r#"
slug = "bench"
grants = []

[[io_port]]
instance = "monitor"
port = "tick"
push_depth = 4
retain_depth = 4
urgency = "normal"
publish_capacity = 3.0
"#;
        let raw: SurfaceConfigRaw =
            toml::from_str(toml_str).expect("[[surface.io_port]] must parse");
        assert_eq!(raw.io_ports.len(), 1);
        let io = &raw.io_ports[0];
        assert_eq!(io.instance, "monitor");
        assert_eq!(io.port, "tick");
        assert_eq!(io.push_depth, Some(Depth::Bounded(4)));
        assert_eq!(io.retain_depth, Some(Depth::Bounded(4)));
        assert_eq!(io.urgency, Some(crate::messaging::Urgency::Normal));
        assert_eq!(io.publish_capacity, Some(3.0));
        assert!(io.channel.is_none());
    }

    /// A binding with no `channel` is a free port: the field is optional on all
    /// four binding blocks.
    #[test]
    fn bindings_parse_as_free_ports() {
        let sub: WasmConsumerSubscriptionRaw =
            toml::from_str("port = \"in\"\n").expect("channel-less wasm subscription must parse");
        assert!(sub.channel.is_none());
        let out: WasmConsumerOutputRaw =
            toml::from_str("port = \"out\"\n").expect("channel-less wasm output must parse");
        assert!(out.channel.is_none());
        let ssub: SurfaceSubscriptionRaw =
            toml::from_str("instance = \"monitor\"\nport = \"in\"\n")
                .expect("channel-less surface subscription must parse");
        assert!(ssub.channel.is_none());
        let sout: SurfaceOutputRaw = toml::from_str("instance = \"monitor\"\nport = \"out\"\n")
            .expect("channel-less surface output must parse");
        assert!(sout.channel.is_none());
    }

    /// The `"alert"` grant token parses to `SurfaceGrant::Alert` — the same
    /// authoring string operators write for a WASM consumer's alert grant.
    #[test]
    fn surface_alert_grant_parses() {
        let raw: SurfaceConfigRaw = toml::from_str("slug = \"deskbar\"\ngrants = [\"alert\"]\n")
            .expect("[[surface]] with alert grant must parse");
        assert_eq!(raw.grants, vec![SurfaceGrant::Alert]);
    }

    /// The `"takeover"` grant token parses to `SurfaceGrant::Takeover`.
    #[test]
    fn surface_takeover_grant_parses() {
        let raw: SurfaceConfigRaw = toml::from_str("slug = \"deskbar\"\ngrants = [\"takeover\"]\n")
            .expect("[[surface]] with takeover grant must parse");
        assert_eq!(raw.grants, vec![SurfaceGrant::Takeover]);
    }

    /// An unknown grant token is rejected by serde.
    #[test]
    fn surface_unknown_grant_rejected() {
        let result: Result<SurfaceConfigRaw, _> =
            toml::from_str("slug = \"x\"\ngrants = [\"not-a-grant\"]\n");
        assert!(result.is_err(), "unknown grant token must be rejected");
    }

    /// `deny_unknown_fields` on `SurfaceConfigRaw` rejects a top-level typo — a
    /// misspelled ACL key would otherwise silently be a deny-by-default over-narrow.
    #[test]
    fn surface_rejects_unknown_field() {
        let result: Result<SurfaceConfigRaw, _> =
            toml::from_str("slug = \"x\"\ngrants = []\nbogus = 1\n");
        assert!(
            result.is_err(),
            "deny_unknown_fields must reject an unknown key"
        );
    }

    /// `deny_unknown_fields` on `[[surface.component]]`.
    #[test]
    fn surface_component_rejects_unknown_field() {
        let result: Result<SurfaceConfigRaw, _> = toml::from_str(
            "slug = \"x\"\ngrants = []\n\n[[component]]\nkind = \"protobar\"\nabi = \"dom\"\nbogus = 1\n",
        );
        assert!(
            result.is_err(),
            "deny_unknown_fields must reject an unknown component key"
        );
    }

    /// `abi` is required, with no default.
    #[test]
    fn surface_component_requires_abi() {
        // A defaulted `abi` would silently pick an artifact shape for the
        // operator; the field states which toolchain built the module, and no
        // one but the operator knows that. Pinned because "just default it to
        // dom" is the obvious ergonomic temptation.
        let result: Result<SurfaceConfigRaw, _> =
            toml::from_str("slug = \"x\"\ngrants = []\n\n[[component]]\nkind = \"protobar\"\n");
        assert!(result.is_err(), "abi must be a required component key");
    }

    /// `deny_unknown_fields` on `[[surface.subscription]]`.
    #[test]
    fn surface_subscription_rejects_unknown_field() {
        let result: Result<SurfaceConfigRaw, _> = toml::from_str(
            "slug = \"x\"\ngrants = []\n\n[[subscription]]\nchannel = \"ephemeral:c\"\ncomponent = \"p\"\nport = \"m\"\nbogus = 1\n",
        );
        assert!(
            result.is_err(),
            "deny_unknown_fields must reject an unknown subscription key"
        );
    }

    /// `deny_unknown_fields` on `[[surface.output]]`.
    #[test]
    fn surface_output_rejects_unknown_field() {
        let result: Result<SurfaceConfigRaw, _> = toml::from_str(
            "slug = \"x\"\ngrants = []\n\n[[output]]\ncomponent = \"p\"\nport = \"o\"\nchannel = \"brenn:c\"\nbogus = 1\n",
        );
        assert!(
            result.is_err(),
            "deny_unknown_fields must reject an unknown output key"
        );
    }

    #[test]
    #[should_panic(expected = "is not a valid UUID")]
    fn malformed_uuid_panics() {
        build_channel_entries(&[raw_channel("not-a-uuid", "ok")], &global_defaults());
    }

    /// A scheme-qualified `brenn:` address is legal and canonicalizes to the
    /// same channel a bare address does; a second colon is not part of any
    /// pub/sub address and fails the charset check.
    #[test]
    fn brenn_prefixed_address_canonicalizes() {
        let entries = build_channel_entries(
            &[raw_channel(
                "1f6c6e3a-1d6e-4f7c-9b6a-12cb7e4a8d32",
                "brenn:explicit",
            )],
            &global_defaults(),
        );
        assert_eq!(entries[0].address, "brenn:explicit");
        assert_eq!(entries[0].transport_type, ChannelScheme::Brenn);
    }

    #[test]
    #[should_panic(expected = "unreserved characters only")]
    fn address_containing_nested_colon_panics() {
        build_channel_entries(
            &[raw_channel(
                "1f6c6e3a-1d6e-4f7c-9b6a-12cb7e4a8d32",
                "brenn:nested:more",
            )],
            &global_defaults(),
        );
    }

    #[test]
    #[should_panic(expected = "unreserved characters only")]
    fn address_with_invalid_charset_panics() {
        build_channel_entries(
            &[raw_channel(
                "1f6c6e3a-1d6e-4f7c-9b6a-12cb7e4a8d32",
                "has space",
            )],
            &global_defaults(),
        );
    }

    // -----------------------------------------------------------------------
    // Depth deserialization tests
    // -----------------------------------------------------------------------

    #[test]
    fn depth_deserializes_integer_to_bounded() {
        let d: Depth = toml::from_str("x = 5\n")
            .map(|v: toml::Table| toml::Value::try_into(v["x"].clone()).unwrap())
            .unwrap();
        assert_eq!(d, Depth::Bounded(5));
    }

    #[test]
    fn depth_zero_is_bounded_zero() {
        let d: Depth = toml::from_str("x = 0\n")
            .map(|v: toml::Table| toml::Value::try_into(v["x"].clone()).unwrap())
            .unwrap();
        assert_eq!(d, Depth::Bounded(0));
    }

    #[test]
    fn depth_deserializes_unbounded_string() {
        let d: Depth = toml::from_str("x = \"unbounded\"\n")
            .map(|v: toml::Table| toml::Value::try_into(v["x"].clone()).unwrap())
            .unwrap();
        assert_eq!(d, Depth::Unbounded);
    }

    #[test]
    fn depth_rejects_negative_integer() {
        // Parse directly into a struct that uses Depth, so the outer TOML parse
        // itself must fail — there is no two-step that could silently succeed.
        #[derive(serde::Deserialize)]
        struct Wrapper {
            #[allow(dead_code)]
            x: Depth,
        }
        let result: Result<Wrapper, _> = toml::from_str("x = -1\n");
        assert!(
            result.is_err(),
            "negative depth must be a deserialize error"
        );
    }

    #[test]
    fn depth_rejects_unknown_string() {
        #[derive(serde::Deserialize)]
        struct Wrapper {
            #[allow(dead_code)]
            x: Depth,
        }
        let result: Result<Wrapper, _> = toml::from_str("x = \"inf\"\n");
        assert!(
            result.is_err(),
            "unknown string depth must be a deserialize error"
        );
    }

    #[test]
    fn depth_is_push_enabled() {
        assert!(!Depth::Bounded(0).is_push_enabled());
        assert!(Depth::Bounded(1).is_push_enabled());
        assert!(Depth::Bounded(100).is_push_enabled());
        assert!(Depth::Unbounded.is_push_enabled());
    }

    /// The derived `Ord` ranks depths by "how deep a retention window": deeper
    /// `Bounded` values are greater, and every `Bounded(_)` is less than
    /// `Unbounded`. Pins the load-bearing variant order the dynamic-path cap
    /// relies on (`resolved.retain_depth > standing` must mean "deeper than").
    #[test]
    fn depth_ordering_ranks_by_retention_depth() {
        assert!(Depth::Bounded(1) < Depth::Bounded(2));
        assert!(Depth::Bounded(2) > Depth::Bounded(1));
        assert!(Depth::Bounded(u64::MAX) < Depth::Unbounded);
        assert!(Depth::Bounded(0) < Depth::Unbounded);
        assert_eq!(Depth::Unbounded, Depth::Unbounded);
        assert!(Depth::Unbounded <= Depth::Unbounded);
        assert!(Depth::Bounded(5) <= Depth::Bounded(5));
    }

    /// Pinned in both orders: a naive numeric `max` rewrite produces a bound
    /// winning over `Unbounded`, silently narrowing every caller's window.
    #[test]
    fn depth_widened_by_takes_the_looser_and_unbounded_dominates() {
        for (a, b, want) in [
            (Depth::Bounded(2), Depth::Bounded(5), Depth::Bounded(5)),
            (Depth::Bounded(5), Depth::Bounded(2), Depth::Bounded(5)),
            (Depth::Bounded(3), Depth::Bounded(3), Depth::Bounded(3)),
            (Depth::Bounded(0), Depth::Bounded(0), Depth::Bounded(0)),
            (Depth::Bounded(2), Depth::Unbounded, Depth::Unbounded),
            (Depth::Unbounded, Depth::Bounded(2), Depth::Unbounded),
            (Depth::Unbounded, Depth::Unbounded, Depth::Unbounded),
        ] {
            assert_eq!(a.widened_by(b), want, "{a:?}.widened_by({b:?})");
        }
    }

    #[test]
    fn depth_narrowed_by_takes_the_tighter_and_a_bound_wins() {
        for (a, b, want) in [
            (Depth::Bounded(2), Depth::Bounded(5), Depth::Bounded(2)),
            (Depth::Bounded(5), Depth::Bounded(2), Depth::Bounded(2)),
            (Depth::Bounded(2), Depth::Unbounded, Depth::Bounded(2)),
            (Depth::Unbounded, Depth::Bounded(2), Depth::Bounded(2)),
            (Depth::Unbounded, Depth::Unbounded, Depth::Unbounded),
        ] {
            assert_eq!(a.narrowed_by(b), want, "{a:?}.narrowed_by({b:?})");
        }
    }

    // -----------------------------------------------------------------------
    // Inheritance tests
    // -----------------------------------------------------------------------

    #[test]
    fn channel_takes_its_own_depths_and_inherits_the_rest() {
        let entries = build_channel_entries(
            &[raw_channel("1f6c6e3a-1d6e-4f7c-9b6a-12cb7e4a8d32", "ch")],
            &global_defaults(),
        );
        let rc = &entries[0].resolved_channel;
        assert_eq!(rc.push_depth, Depth::Bounded(4));
        assert_eq!(rc.retain_depth, Depth::Bounded(4));
        assert_eq!(rc.standing_retain_depth, Depth::Bounded(4));
        assert_eq!(rc.noise, NoiseLevel::Silent);
        assert_eq!(rc.sink, Sink::Drop);
    }

    #[test]
    fn channel_states_its_own_push_depth() {
        let mut ch = raw_channel("1f6c6e3a-1d6e-4f7c-9b6a-12cb7e4a8d32", "ch");
        ch.push_depth = Some(Depth::Bounded(10));
        ch.standing_retain_depth = Some(Depth::Bounded(10));
        let entries = build_channel_entries(&[ch], &global_defaults());
        assert_eq!(entries[0].resolved_channel.push_depth, Depth::Bounded(10));
    }

    // -----------------------------------------------------------------------
    // Shared resolver (`resolve_subscription_params`) tests
    // -----------------------------------------------------------------------

    fn raw_params(push_depth: Option<Depth>) -> RawSubscriptionParams {
        RawSubscriptionParams {
            channel_uuid: Uuid::nil(),
            channel_address: "brenn:ch".to_string(),
            push_depth,
            retain_depth: None,
            noise: None,
            wake_min: None,
        }
    }

    /// Rung a system-minted channel presents — an untuned webhook channel's
    /// family default over the global non-depth defaults.
    fn default_rung() -> SubscriptionParamDefaults {
        SubscriptionParamDefaults::from_channel(&resolve_system_channel(
            "webhook:probe",
            &SystemChannelTuning::default(),
            &global_defaults(),
        ))
    }

    #[test]
    fn resolver_ok_for_valid_pull_only() {
        // push_depth = 0 (pull-only) on a non-singleton, multi-user app: valid.
        let raw = raw_params(Some(Depth::Bounded(0)));
        let resolved = resolve_subscription_params(&raw, &default_rung(), false, 3)
            .expect("pull-only sub on any app must resolve");
        assert_eq!(resolved.push_depth, Depth::Bounded(0));
        // Omitted noise/retain/wake inherit from the rung: the ingress family's
        // window, and the global non-depth defaults.
        assert_eq!(resolved.retain_depth, INGRESS_DEFAULT_RETAIN_DEPTH);
        assert_eq!(resolved.noise, NoiseLevel::Silent);
    }

    #[test]
    fn resolver_inherits_omitted_params_from_rung() {
        let rung = SubscriptionParamDefaults {
            push_depth: Depth::Bounded(7),
            retain_depth: Depth::Bounded(9),
            noise: NoiseLevel::Metered,
            wake_min: WakeMin::Normal,
        };
        // All raw params None ⇒ all inherit the rung.
        let resolved = resolve_subscription_params(&raw_params(None), &rung, true, 1)
            .expect("inheriting sub must resolve");
        assert_eq!(resolved.push_depth, Depth::Bounded(7));
        assert_eq!(resolved.retain_depth, Depth::Bounded(9));
        assert_eq!(resolved.noise, NoiseLevel::Metered);
        assert_eq!(resolved.wake_min, WakeMin::Normal);
    }

    #[test]
    fn resolver_err_fatal_noise_set_directly() {
        // `fatal` is surface-only; a backend subscription that sets it directly
        // is rejected (the boot caller turns this into a config-time panic; the
        // dynamic caller returns it as a tool error).
        let mut raw = raw_params(Some(Depth::Bounded(5)));
        raw.noise = Some(NoiseLevel::Fatal);
        let err = resolve_subscription_params(&raw, &default_rung(), true, 1)
            .expect_err("fatal noise on a backend subscription must be rejected");
        assert_eq!(
            err,
            SubscribeError::FatalNoise {
                channel_address: "brenn:ch".to_string(),
            }
        );
    }

    #[test]
    fn resolver_err_fatal_noise_inherited() {
        // Inheriting a `fatal` rung (channel/global default) is rejected the same
        // way — the check is on the resolved value, so both directly-set and
        // inherited are caught.
        let rung = SubscriptionParamDefaults {
            push_depth: Depth::Bounded(5),
            retain_depth: Depth::Unbounded,
            noise: NoiseLevel::Fatal,
            wake_min: WakeMin::Normal,
        };
        let err = resolve_subscription_params(&raw_params(None), &rung, true, 1)
            .expect_err("inherited fatal noise must be rejected");
        assert_eq!(
            err,
            SubscribeError::FatalNoise {
                channel_address: "brenn:ch".to_string(),
            }
        );
    }

    #[test]
    fn resolver_err_push_enabled_on_non_singleton() {
        let raw = raw_params(Some(Depth::Unbounded));
        let err = resolve_subscription_params(&raw, &default_rung(), false, 1)
            .expect_err("push-enabled on non-singleton must error");
        assert_eq!(
            err,
            SubscribeError::PushEnabledRequiresSingleton {
                channel_address: "brenn:ch".to_string(),
            }
        );
    }

    #[test]
    fn resolver_err_push_enabled_on_multi_user() {
        let raw = raw_params(Some(Depth::Bounded(5)));
        let err = resolve_subscription_params(&raw, &default_rung(), true, 2)
            .expect_err("push-enabled on multi-user must error");
        assert_eq!(
            err,
            SubscribeError::PushEnabledRequiresSingleUser {
                channel_address: "brenn:ch".to_string(),
                allowed_users: 2,
            }
        );
    }

    #[test]
    fn resolver_err_push_enabled_zero_allowed_users() {
        let raw = raw_params(Some(Depth::Bounded(5)));
        let err = resolve_subscription_params(&raw, &default_rung(), true, 0)
            .expect_err("push-enabled with zero allowed_users must error");
        assert_eq!(
            err,
            SubscribeError::PushEnabledRequiresSingleUser {
                channel_address: "brenn:ch".to_string(),
                allowed_users: 0,
            }
        );
    }

    #[test]
    fn resolver_err_explicit_noise_on_pull_only() {
        let mut raw = raw_params(Some(Depth::Bounded(0)));
        raw.noise = Some(NoiseLevel::Alarm);
        let err = resolve_subscription_params(&raw, &default_rung(), false, 1)
            .expect_err("explicit noise on pull-only must error");
        assert_eq!(
            err,
            SubscribeError::NoiseOnPullOnly {
                channel_address: "brenn:ch".to_string(),
            }
        );
    }

    #[test]
    fn resolver_err_explicit_wake_min_on_pull_only() {
        let mut raw = raw_params(Some(Depth::Bounded(0)));
        raw.wake_min = Some(WakeMin::Normal);
        let err = resolve_subscription_params(&raw, &default_rung(), false, 1)
            .expect_err("explicit wake_min on pull-only must error");
        assert_eq!(
            err,
            SubscribeError::WakeMinOnPullOnly {
                channel_address: "brenn:ch".to_string(),
            }
        );
    }

    #[test]
    fn resolver_inherited_noise_on_pull_only_is_ok() {
        // Pull-only with an *inherited* (non-explicit) noise from the rung is fine —
        // only an *explicit* noise on a pull-only sub is an error.
        let rung = SubscriptionParamDefaults {
            push_depth: Depth::Bounded(0),
            retain_depth: Depth::Unbounded,
            noise: NoiseLevel::Alarm,
            wake_min: WakeMin::Normal,
        };
        let resolved = resolve_subscription_params(&raw_params(None), &rung, false, 1)
            .expect("inherited noise on pull-only must resolve");
        assert_eq!(resolved.push_depth, Depth::Bounded(0));
        assert_eq!(resolved.noise, NoiseLevel::Alarm);
    }

    // -----------------------------------------------------------------------
    // Subscription resolution + validation tests
    // -----------------------------------------------------------------------

    /// Build a minimal `AppConfigRaw` for testing `resolve_app_messaging`.
    fn minimal_raw_app(
        slug: &str,
        singleton: bool,
        allowed_users: Vec<String>,
    ) -> crate::config::AppConfigRaw {
        crate::config::AppConfigRaw {
            slug: slug.to_string(),
            singleton,
            allowed_users,
            ..Default::default()
        }
    }

    /// Build a directory containing a single test channel, deep enough that a
    /// subscriber's depth is what these tests are measuring rather than the
    /// channel's standing cap.
    fn directory_with_one_channel() -> (MessagingDirectory, String) {
        let mut ch = raw_channel("1f6c6e3a-1d6e-4f7c-9b6a-12cb7e4a8d32", "ch");
        ch.push_depth = Some(Depth::Bounded(16));
        ch.retain_depth = Some(Depth::Bounded(16));
        ch.standing_retain_depth = Some(Depth::Bounded(16));
        let entries = build_channel_entries(&[ch], &global_defaults());
        let address = entries[0].address.clone();
        (MessagingDirectory::with_entries(entries), address)
    }

    #[test]
    fn subscription_inherits_the_channel_rung() {
        let (dir, address) = directory_with_one_channel();
        let raw_app = minimal_raw_app("single", true, vec!["alice".to_string()]);
        let raw_msg = MessagingConfigRaw {
            subscribe: vec![MessagingSubscriptionRaw {
                channel: address,
                push_depth: None,
                retain_depth: None,
                noise: None,
                wake_min: None,
            }],
            send_budget: None,
        };
        let resolved = resolve_app_messaging(&raw_app, &raw_msg, &global_defaults(), &dir);
        let sub = &resolved.subscriptions[0];
        // Both depths come off the channel; noise comes off the global rung the
        // channel itself inherited.
        assert_eq!(sub.push_depth, Depth::Bounded(16));
        assert_eq!(sub.retain_depth, Depth::Bounded(16));
        assert_eq!(sub.noise, NoiseLevel::Silent);
    }

    /// The whole ladder: a subscription that leaves push_depth unset takes the
    /// channel's stated number.
    #[test]
    fn subscription_inherits_the_channels_stated_push_depth() {
        let mut ch = raw_channel("1f6c6e3a-1d6e-4f7c-9b6a-12cb7e4a8d32", "ch");
        ch.push_depth = Some(Depth::Bounded(5));
        ch.standing_retain_depth = Some(Depth::Bounded(5));
        let entries = build_channel_entries(&[ch], &global_defaults());
        let dir = MessagingDirectory::with_entries(entries);
        let address = dir.resolve("brenn:ch").unwrap().address.clone();

        let raw_app = minimal_raw_app("single", true, vec!["alice".to_string()]);
        let raw_msg = MessagingConfigRaw {
            subscribe: vec![MessagingSubscriptionRaw {
                channel: address,
                push_depth: None, // inherit from channel (Bounded(5)), not global (Unbounded)
                retain_depth: None,
                noise: None,
                wake_min: None,
            }],
            send_budget: None,
        };
        let resolved = resolve_app_messaging(&raw_app, &raw_msg, &global_defaults(), &dir);
        assert_eq!(
            resolved.subscriptions[0].push_depth,
            Depth::Bounded(5),
            "sub should inherit channel override (Bounded(5)), not global default (Unbounded)"
        );
    }

    #[test]
    fn subscription_overrides_push_depth() {
        let (dir, address) = directory_with_one_channel();
        let raw_app = minimal_raw_app("single", true, vec!["alice".to_string()]);
        let raw_msg = MessagingConfigRaw {
            subscribe: vec![MessagingSubscriptionRaw {
                channel: address,
                push_depth: Some(Depth::Bounded(5)),
                retain_depth: None,
                noise: None,
                wake_min: None,
            }],
            send_budget: None,
        };
        let resolved = resolve_app_messaging(&raw_app, &raw_msg, &global_defaults(), &dir);
        assert_eq!(resolved.subscriptions[0].push_depth, Depth::Bounded(5));
    }

    #[test]
    #[should_panic(expected = "requires `singleton = true`")]
    fn push_enabled_subscription_on_non_singleton_panics() {
        let (dir, address) = directory_with_one_channel();
        let raw = minimal_raw_app("nonsingle", false, vec!["alice".to_string()]);
        let raw_msg = MessagingConfigRaw {
            subscribe: vec![MessagingSubscriptionRaw {
                channel: address,
                push_depth: Some(Depth::Unbounded),
                retain_depth: None,
                noise: None,
                wake_min: None,
            }],
            send_budget: None,
        };
        let _ = resolve_app_messaging(&raw, &raw_msg, &global_defaults(), &dir);
    }

    #[test]
    #[should_panic(expected = "exactly one `allowed_users` entry")]
    fn push_enabled_subscription_singleton_zero_allowed_users_panics() {
        let (dir, address) = directory_with_one_channel();
        let raw = minimal_raw_app("single0", true, vec![]);
        let raw_msg = MessagingConfigRaw {
            subscribe: vec![MessagingSubscriptionRaw {
                channel: address,
                push_depth: Some(Depth::Unbounded),
                retain_depth: None,
                noise: None,
                wake_min: None,
            }],
            send_budget: None,
        };
        let _ = resolve_app_messaging(&raw, &raw_msg, &global_defaults(), &dir);
    }

    #[test]
    #[should_panic(expected = "exactly one `allowed_users` entry")]
    fn push_enabled_subscription_singleton_two_allowed_users_panics() {
        let (dir, address) = directory_with_one_channel();
        let raw = minimal_raw_app(
            "single2",
            true,
            vec!["alice".to_string(), "bob".to_string()],
        );
        let raw_msg = MessagingConfigRaw {
            subscribe: vec![MessagingSubscriptionRaw {
                channel: address,
                push_depth: Some(Depth::Unbounded),
                retain_depth: None,
                noise: None,
                wake_min: None,
            }],
            send_budget: None,
        };
        let _ = resolve_app_messaging(&raw, &raw_msg, &global_defaults(), &dir);
    }

    #[test]
    #[should_panic(expected = "noise configured")]
    fn noise_on_pull_only_subscription_panics() {
        let (dir, address) = directory_with_one_channel();
        // Subscription explicitly sets push_depth=0 (pull-only); setting noise on it must panic.
        let raw = minimal_raw_app("myapp", true, vec!["alice".to_string()]);
        let raw_msg = MessagingConfigRaw {
            subscribe: vec![MessagingSubscriptionRaw {
                channel: address,
                push_depth: Some(Depth::Bounded(0)),
                retain_depth: None,
                noise: Some(NoiseLevel::Metered),
                wake_min: None,
            }],
            send_budget: None,
        };
        let _ = resolve_app_messaging(&raw, &raw_msg, &global_defaults(), &dir);
    }

    #[test]
    fn inherited_noise_on_pull_only_subscription_is_ok() {
        // Even if the global noise default were non-silent, a pull-only sub that
        // doesn't *explicitly* set noise should not panic. Build a custom global
        // with non-silent default.
        let mut defaults = global_defaults();
        defaults.default_noise = NoiseLevel::Metered;
        // Channel has default (Unbounded) push depth. Subscription sets push_depth=0 (pull-only).
        let (dir, address) = {
            let entries = build_channel_entries(
                &[raw_channel("1f6c6e3a-1d6e-4f7c-9b6a-12cb7e4a8d32", "ch")],
                &defaults,
            );
            let addr = entries[0].address.clone();
            (MessagingDirectory::with_entries(entries), addr)
        };
        let raw = minimal_raw_app("myapp", true, vec!["alice".to_string()]);
        let raw_msg = MessagingConfigRaw {
            subscribe: vec![MessagingSubscriptionRaw {
                channel: address,
                push_depth: Some(Depth::Bounded(0)),
                retain_depth: None,
                noise: None, // inherited, NOT explicitly set
                wake_min: None,
            }],
            send_budget: None,
        };
        // Must not panic — inherited noise on pull-only is OK.
        let resolved = resolve_app_messaging(&raw, &raw_msg, &defaults, &dir);
        // The noise was inherited, but since pull-only we don't error.
        assert_eq!(resolved.subscriptions[0].push_depth, Depth::Bounded(0));
    }

    #[test]
    #[should_panic(expected = "archive_path is not set")]
    fn archive_sink_without_archive_path_panics() {
        let mut ch = raw_channel("1f6c6e3a-1d6e-4f7c-9b6a-12cb7e4a8d32", "ch");
        ch.sink = Some(Sink::Archive);
        let mut defaults = global_defaults();
        defaults.archive_path = None;
        build_channel_entries(&[ch], &defaults);
    }

    /// If `default_sink = Archive` (global) and `archive_path = None`, any channel that
    /// inherits the global sink must panic — not just channels with an explicit override.
    #[test]
    #[should_panic(expected = "archive_path is not set")]
    fn global_archive_sink_without_archive_path_panics() {
        let ch = raw_channel("1f6c6e3a-1d6e-4f7c-9b6a-12cb7e4a8d32", "ch");
        // No explicit ch.sink — inherits global default_sink = Archive.
        let mut defaults = global_defaults();
        defaults.default_sink = Sink::Archive;
        defaults.archive_path = None;
        build_channel_entries(&[ch], &defaults);
    }

    #[test]
    fn archive_sink_with_archive_path_ok() {
        let mut ch = raw_channel("1f6c6e3a-1d6e-4f7c-9b6a-12cb7e4a8d32", "ch");
        ch.sink = Some(Sink::Archive);
        let mut defaults = global_defaults();
        defaults.archive_path = Some(PathBuf::from("/tmp/archive.jsonl"));
        let entries = build_channel_entries(&[ch], &defaults);
        assert_eq!(entries[0].resolved_channel.sink, Sink::Archive);
    }

    /// Migration-forcing (access-control design §2.5.1 / §6.6 / §8 decision-2):
    /// the legacy `[app.messaging].enabled` authorization boolean was removed.
    /// Because `MessagingConfigRaw` carries `#[serde(deny_unknown_fields)]`, a
    /// stale config that still sets `enabled` must now FAIL to parse with a
    /// precise error naming the offending field — forcing the operator to migrate
    /// to the explicit `grants` surface rather than silently parsing.
    #[test]
    fn legacy_enabled_field_is_rejected_as_unknown_field() {
        let toml_with_legacy_enabled = r#"enabled = true"#;
        let result: Result<MessagingConfigRaw, _> = toml::from_str(toml_with_legacy_enabled);
        assert!(
            result.is_err(),
            "legacy [app.messaging].enabled must be rejected by deny_unknown_fields"
        );
        let err_str = result.unwrap_err().to_string();
        assert!(
            err_str.contains("enabled"),
            "error message should name the offending field; got: {err_str}"
        );
    }

    /// `sender` key is no longer accepted in `[app.messaging]` (removed in
    /// the participant-identity slice — design §2.3/AC5). Startup with a
    /// config still carrying `sender` must fail-fast via serde deny_unknown_fields.
    #[test]
    fn sender_key_is_rejected_as_unknown_field() {
        let toml_with_legacy_sender = r#"sender = "my-app""#;
        let result: Result<MessagingConfigRaw, _> = toml::from_str(toml_with_legacy_sender);
        assert!(
            result.is_err(),
            "legacy sender key must be rejected by deny_unknown_fields"
        );
        let err_str = result.unwrap_err().to_string();
        assert!(
            err_str.contains("sender"),
            "error message should name the offending field; got: {err_str}"
        );
    }

    /// An absent `send_budget` resolves to the global default through resolve.
    /// (Formerly asserted on the removed `enabled` boolean; messaging
    /// authorization is now carried by the app's `AppPolicy`, not this struct —
    /// access-control §2.5.1.)
    #[test]
    fn absent_send_budget_resolves_to_global_default() {
        let (dir, _) = directory_with_one_channel();
        let raw_app = minimal_raw_app("myapp", false, vec![]);
        let raw_msg = MessagingConfigRaw {
            subscribe: vec![],
            send_budget: None,
        };
        let defaults = global_defaults();
        let resolved = resolve_app_messaging(&raw_app, &raw_msg, &defaults, &dir);
        assert_eq!(
            resolved.send_budget, defaults.default_send_budget,
            "absent send_budget must inherit the global default"
        );
    }

    /// Pull-only subscription (push_depth=0) doesn't trigger push-enabled constraints.
    #[test]
    fn pull_only_subscription_does_not_require_singleton() {
        let (dir, address) = directory_with_one_channel();
        let raw = minimal_raw_app("multi", false, vec![]);
        let raw_msg = MessagingConfigRaw {
            subscribe: vec![MessagingSubscriptionRaw {
                channel: address,
                push_depth: Some(Depth::Bounded(0)),
                retain_depth: None,
                noise: None,
                wake_min: None,
            }],
            send_budget: None,
        };
        // Must not panic: pull-only doesn't require singleton.
        let resolved = resolve_app_messaging(&raw, &raw_msg, &global_defaults(), &dir);
        assert!(!resolved.subscriptions[0].is_push_enabled());
    }

    // -----------------------------------------------------------------------
    // Subscriber-set wiring (design §6 "Subscriber-set wiring")
    // -----------------------------------------------------------------------

    /// `finalize_directory_with_subscribers` places a `Wasm(slug)` entry on a
    /// `brenn:` channel when a WASM consumer subscription is passed in.
    #[test]
    fn wasm_consumer_placed_on_brenn_channel() {
        use crate::messaging::{ChannelScheme, SubscriberEntryKind};
        use uuid::Uuid;

        let chan_uuid = Uuid::new_v4();
        let entry = crate::messaging::ChannelEntry {
            uuid: chan_uuid,
            address: "brenn:my-channel".to_string(),
            description: None,
            resolved_channel: ResolvedChannel {
                send_rate: Default::default(),
                push_depth: Depth::Unbounded,
                retain_depth: Depth::Unbounded,
                standing_retain_depth: Depth::Unbounded,
                noise: NoiseLevel::Silent,
                sink: Sink::Drop,
                wake_min: WakeMin::Normal,
            },
            subscribers: vec![],
            transport_type: ChannelScheme::Brenn,
            mount: None,
        };
        let wasm_subs = vec![(
            "my-consumer".to_string(),
            vec![ResolvedSubscription {
                channel_uuid: chan_uuid,
                channel_address: "brenn:my-channel".to_string(),
                push_depth: Depth::Unbounded,
                retain_depth: Depth::Unbounded,
                noise: NoiseLevel::Alarm,
                wake_min: WakeMin::Normal,
            }],
        )];
        let dir = finalize_directory_with_subscribers(vec![entry], &[], &wasm_subs, &[]);
        let chan = dir.by_uuid(&chan_uuid).expect("channel must be present");
        assert_eq!(chan.subscribers.len(), 1);
        assert!(
            matches!(&chan.subscribers[0].kind, SubscriberEntryKind::Wasm(s) if s == "my-consumer"),
            "subscriber kind must be Wasm(my-consumer)"
        );
        // Noise must be threaded through (not silently clamped to Silent).
        assert_eq!(
            chan.subscribers[0].noise,
            NoiseLevel::Alarm,
            "noise must be Alarm as configured, not silently clamped to Silent"
        );
    }

    /// `finalize_directory_with_subscribers` places a `Surface(slug)` entry on a
    /// `brenn:` channel when a surface durable subscription is passed in, threading
    /// the resolved depth/noise/wake — structurally identical to the wasm loop.
    #[test]
    fn surface_placed_on_brenn_channel() {
        use crate::messaging::{ChannelScheme, SubscriberEntryKind};
        use uuid::Uuid;

        let chan_uuid = Uuid::new_v4();
        let entry = crate::messaging::ChannelEntry {
            uuid: chan_uuid,
            address: "brenn:alerts".to_string(),
            description: None,
            resolved_channel: ResolvedChannel {
                send_rate: Default::default(),
                push_depth: Depth::Unbounded,
                retain_depth: Depth::Unbounded,
                standing_retain_depth: Depth::Unbounded,
                noise: NoiseLevel::Silent,
                sink: Sink::Drop,
                wake_min: WakeMin::Normal,
            },
            subscribers: vec![],
            transport_type: ChannelScheme::Brenn,
            mount: None,
        };
        let surface_subs = vec![(
            "deskbar".to_string(),
            vec![ResolvedSurfaceSubscription {
                instance: "agenda-alice".to_string(),
                subscription: ResolvedSubscription {
                    channel_uuid: chan_uuid,
                    channel_address: "brenn:alerts".to_string(),
                    push_depth: Depth::Bounded(8),
                    retain_depth: Depth::Bounded(2),
                    noise: NoiseLevel::Alarm,
                    wake_min: WakeMin::Never,
                },
            }],
        )];
        let dir = finalize_directory_with_subscribers(vec![entry], &[], &[], &surface_subs);
        let chan = dir.by_uuid(&chan_uuid).expect("channel must be present");
        assert_eq!(chan.subscribers.len(), 1);
        assert_eq!(
            chan.subscribers[0].kind,
            SubscriberEntryKind::Surface("deskbar".to_string()),
            "the directory entry is cut at the surface, not the binding's instance"
        );
        assert_eq!(chan.subscribers[0].push_depth, Depth::Bounded(8));
        // The server-side push window is clamped to `Metered` for a surface:
        // `Alarm`/`Fatal` must not reach the server's shared overflow path.
        // The full rung is still carried in the bindings document for the kernel.
        assert_eq!(chan.subscribers[0].noise, NoiseLevel::Metered);
        // Surface subscribers are `Eager`, so the directory carries no threshold.
        assert_eq!(chan.subscribers[0].wake_min, None);
    }

    /// The surface push-window noise clamp is `min(resolved, Metered)` for every
    /// louder rung: `Fatal` lands as `Metered` too, never reaching the shared
    /// overflow path's fatal panic server-side.
    #[test]
    fn surface_push_window_noise_clamps_fatal_to_metered() {
        use uuid::Uuid;
        let chan_uuid = Uuid::new_v4();
        let entry = crate::messaging::ChannelEntry {
            uuid: chan_uuid,
            address: "brenn:alerts".to_string(),
            description: None,
            resolved_channel: ResolvedChannel {
                send_rate: Default::default(),
                push_depth: Depth::Unbounded,
                retain_depth: Depth::Unbounded,
                standing_retain_depth: Depth::Unbounded,
                noise: NoiseLevel::Silent,
                sink: Sink::Drop,
                wake_min: WakeMin::Normal,
            },
            subscribers: vec![],
            transport_type: crate::messaging::ChannelScheme::Brenn,
            mount: None,
        };
        let surface_subs = vec![(
            "deskbar".to_string(),
            vec![ResolvedSurfaceSubscription {
                instance: "agenda-alice".to_string(),
                subscription: ResolvedSubscription {
                    channel_uuid: chan_uuid,
                    channel_address: "brenn:alerts".to_string(),
                    push_depth: Depth::Bounded(8),
                    retain_depth: Depth::Bounded(2),
                    noise: NoiseLevel::Fatal,
                    wake_min: WakeMin::Never,
                },
            }],
        )];
        let dir = finalize_directory_with_subscribers(vec![entry], &[], &[], &surface_subs);
        let chan = dir.by_uuid(&chan_uuid).expect("channel must be present");
        assert_eq!(chan.subscribers[0].noise, NoiseLevel::Metered);
    }

    /// Two components of one surface binding the same channel fold into the one
    /// entry the surface holds there, with each knob taking the widest value any
    /// binding asked for. A second entry would be a second server-side push
    /// window feeding the one socket the page attaches with.
    #[test]
    fn sibling_bindings_on_one_channel_fold_into_one_entry() {
        use crate::messaging::SubscriberEntryKind;
        use uuid::Uuid;
        let chan_uuid = Uuid::new_v4();
        let other_uuid = Uuid::new_v4();
        let entries = vec![
            surface_fold_channel(chan_uuid, "brenn:alerts"),
            surface_fold_channel(other_uuid, "brenn:ticker"),
        ];
        let binding = |uuid, address: &str, instance: &str, push, retain, noise| {
            ResolvedSurfaceSubscription {
                instance: instance.to_string(),
                subscription: ResolvedSubscription {
                    channel_uuid: uuid,
                    channel_address: address.to_string(),
                    push_depth: Depth::Bounded(push),
                    retain_depth: Depth::Bounded(retain),
                    noise,
                    wake_min: WakeMin::Never,
                },
            }
        };
        let surface_subs = vec![(
            "deskbar".to_string(),
            vec![
                binding(
                    chan_uuid,
                    "brenn:alerts",
                    "agenda",
                    2,
                    9,
                    NoiseLevel::Silent,
                ),
                binding(chan_uuid, "brenn:alerts", "ticker", 8, 1, NoiseLevel::Alarm),
                binding(
                    other_uuid,
                    "brenn:ticker",
                    "ticker",
                    3,
                    3,
                    NoiseLevel::Silent,
                ),
            ],
        )];
        let dir = finalize_directory_with_subscribers(entries, &[], &[], &surface_subs);

        let chan = dir.by_uuid(&chan_uuid).expect("channel must be present");
        assert_eq!(
            chan.subscribers.len(),
            1,
            "two bindings on one channel are one directory entry"
        );
        assert_eq!(
            chan.subscribers[0].kind,
            SubscriberEntryKind::Surface("deskbar".to_string())
        );
        assert_eq!(chan.subscribers[0].push_depth, Depth::Bounded(8));
        assert_eq!(chan.subscribers[0].retain_depth, Depth::Bounded(9));
        assert_eq!(chan.subscribers[0].noise, NoiseLevel::Metered);

        // The fold is per channel, not per surface: a sibling channel keeps its
        // own entry with its own depths.
        let other = dir.by_uuid(&other_uuid).expect("channel must be present");
        assert_eq!(other.subscribers.len(), 1);
        assert_eq!(other.subscribers[0].push_depth, Depth::Bounded(3));
    }

    /// A bare channel entry for the fold tests above: no subscribers, everything
    /// else at its widest so the resolved binding depths are what land.
    #[cfg(test)]
    fn surface_fold_channel(uuid: uuid::Uuid, address: &str) -> crate::messaging::ChannelEntry {
        crate::messaging::ChannelEntry {
            uuid,
            address: address.to_string(),
            description: None,
            resolved_channel: ResolvedChannel {
                send_rate: Default::default(),
                push_depth: Depth::Unbounded,
                retain_depth: Depth::Unbounded,
                standing_retain_depth: Depth::Unbounded,
                noise: NoiseLevel::Silent,
                sink: Sink::Drop,
                wake_min: WakeMin::Normal,
            },
            subscribers: vec![],
            transport_type: crate::messaging::ChannelScheme::Brenn,
            mount: None,
        }
    }

    /// `finalize_directory_with_subscribers` places a `Wasm(slug)` entry on a
    /// `webhook:` channel (transport-agnostic wiring — same function, same path).
    #[test]
    fn wasm_consumer_placed_on_webhook_channel() {
        use crate::messaging::{
            ChannelScheme, SubscriberEntryKind, webhook_channel_uuid_from_slug,
        };

        let slug = "my-endpoint";
        let chan_uuid = webhook_channel_uuid_from_slug(slug);
        let chan_address = format!("webhook:{slug}");
        let entry = crate::messaging::ChannelEntry {
            uuid: chan_uuid,
            address: chan_address.clone(),
            description: None,
            resolved_channel: ResolvedChannel {
                send_rate: Default::default(),
                push_depth: Depth::Unbounded,
                retain_depth: Depth::Unbounded,
                standing_retain_depth: Depth::Unbounded,
                noise: NoiseLevel::Silent,
                sink: Sink::Drop,
                wake_min: WakeMin::Normal,
            },
            subscribers: vec![],
            transport_type: ChannelScheme::Webhook,
            mount: Some(format!("/webhooks/{slug}")),
        };
        let wasm_subs = vec![(
            "wh-consumer".to_string(),
            vec![ResolvedSubscription {
                channel_uuid: chan_uuid,
                channel_address: chan_address.clone(),
                push_depth: Depth::Bounded(10),
                retain_depth: Depth::Bounded(5),
                noise: NoiseLevel::Metered,
                wake_min: WakeMin::Normal,
            }],
        )];
        let dir = finalize_directory_with_subscribers(vec![entry], &[], &wasm_subs, &[]);
        let chan = dir
            .by_uuid(&chan_uuid)
            .expect("webhook channel must be present");
        assert_eq!(chan.subscribers.len(), 1);
        assert!(
            matches!(&chan.subscribers[0].kind, SubscriberEntryKind::Wasm(s) if s == "wh-consumer"),
            "subscriber kind must be Wasm(wh-consumer)"
        );
        assert_eq!(chan.subscribers[0].push_depth, Depth::Bounded(10));
        assert_eq!(chan.subscribers[0].noise, NoiseLevel::Metered);
    }

    // -----------------------------------------------------------------------
    // wake_min inheritance (urgency-redesign §2.3)
    // -----------------------------------------------------------------------

    /// Global default `wake_min` is `Normal` (migration parity).
    #[test]
    fn global_default_wake_min_is_normal() {
        assert_eq!(global_defaults().default_wake_min, WakeMin::Normal);
    }

    /// `channel` with no explicit `wake_min` inherits the global default.
    #[test]
    fn channel_inherits_global_wake_min_default() {
        let entries = build_channel_entries(
            &[raw_channel("1f6c6e3a-1d6e-4f7c-9b6a-12cb7e4a8d32", "ch")],
            &global_defaults(),
        );
        assert_eq!(entries[0].resolved_channel.wake_min, WakeMin::Normal);
    }

    /// `channel` with an explicit `wake_min` overrides the global default.
    #[test]
    fn channel_overrides_global_wake_min() {
        let mut ch = raw_channel("1f6c6e3a-1d6e-4f7c-9b6a-12cb7e4a8d32", "ch");
        ch.wake_min = Some(WakeMin::High);
        let entries = build_channel_entries(&[ch], &global_defaults());
        assert_eq!(entries[0].resolved_channel.wake_min, WakeMin::High);
    }

    /// Subscription with no explicit `wake_min` inherits from channel (which inherits global).
    #[test]
    fn subscription_inherits_channel_wake_min_via_global() {
        let (dir, address) = directory_with_one_channel();
        let raw_app = minimal_raw_app("single", true, vec!["alice".to_string()]);
        let raw_msg = MessagingConfigRaw {
            subscribe: vec![MessagingSubscriptionRaw {
                channel: address,
                push_depth: None,
                retain_depth: None,
                noise: None,
                wake_min: None, // inherits channel → global (Normal)
            }],
            send_budget: None,
        };
        let resolved = resolve_app_messaging(&raw_app, &raw_msg, &global_defaults(), &dir);
        assert_eq!(resolved.subscriptions[0].wake_min, WakeMin::Normal);
    }

    /// Three-level inheritance: sub leaves `wake_min=None`, channel sets `High`,
    /// so sub resolves to `High` — not the global `Normal`.
    #[test]
    fn subscription_inherits_channel_wake_min_override_not_global() {
        let mut ch = raw_channel("1f6c6e3a-1d6e-4f7c-9b6a-12cb7e4a8d32", "ch");
        ch.wake_min = Some(WakeMin::High);
        let entries = build_channel_entries(&[ch], &global_defaults());
        let dir = MessagingDirectory::with_entries(entries);
        let address = dir.resolve("brenn:ch").unwrap().address.clone();

        let raw_app = minimal_raw_app("single", true, vec!["alice".to_string()]);
        let raw_msg = MessagingConfigRaw {
            subscribe: vec![MessagingSubscriptionRaw {
                channel: address,
                push_depth: None,
                retain_depth: None,
                noise: None,
                wake_min: None, // inherits channel (High), not global (Normal)
            }],
            send_budget: None,
        };
        let resolved = resolve_app_messaging(&raw_app, &raw_msg, &global_defaults(), &dir);
        assert_eq!(
            resolved.subscriptions[0].wake_min,
            WakeMin::High,
            "sub must inherit channel override (High), not global default (Normal)"
        );
    }

    /// Subscription can override `wake_min` explicitly.
    #[test]
    fn subscription_overrides_wake_min() {
        let (dir, address) = directory_with_one_channel();
        let raw_app = minimal_raw_app("single", true, vec!["alice".to_string()]);
        let raw_msg = MessagingConfigRaw {
            subscribe: vec![MessagingSubscriptionRaw {
                channel: address,
                push_depth: None,
                retain_depth: None,
                noise: None,
                wake_min: Some(WakeMin::Never),
            }],
            send_budget: None,
        };
        let resolved = resolve_app_messaging(&raw_app, &raw_msg, &global_defaults(), &dir);
        assert_eq!(resolved.subscriptions[0].wake_min, WakeMin::Never);
    }

    /// Explicit `wake_min` on a pull-only (`push_depth = 0`) subscription panics.
    #[test]
    #[should_panic(expected = "wake_min configured")]
    fn explicit_wake_min_on_pull_only_subscription_panics() {
        let (dir, address) = directory_with_one_channel();
        let raw = minimal_raw_app("myapp", true, vec!["alice".to_string()]);
        let raw_msg = MessagingConfigRaw {
            subscribe: vec![MessagingSubscriptionRaw {
                channel: address,
                push_depth: Some(Depth::Bounded(0)),
                retain_depth: None,
                noise: None,
                wake_min: Some(WakeMin::High), // explicit on pull-only → panic
            }],
            send_budget: None,
        };
        let _ = resolve_app_messaging(&raw, &raw_msg, &global_defaults(), &dir);
    }

    /// Inherited `wake_min` on a pull-only subscription is OK (no panic).
    #[test]
    fn inherited_wake_min_on_pull_only_subscription_is_ok() {
        let (dir, address) = directory_with_one_channel();
        let raw = minimal_raw_app("myapp", true, vec!["alice".to_string()]);
        let raw_msg = MessagingConfigRaw {
            subscribe: vec![MessagingSubscriptionRaw {
                channel: address,
                push_depth: Some(Depth::Bounded(0)),
                retain_depth: None,
                noise: None,
                wake_min: None, // inherited, NOT explicitly set — must not panic
            }],
            send_budget: None,
        };
        let resolved = resolve_app_messaging(&raw, &raw_msg, &global_defaults(), &dir);
        assert_eq!(resolved.subscriptions[0].push_depth, Depth::Bounded(0));
    }

    // -----------------------------------------------------------------------
    // WasmGrant TOML parsing (§8 item 12)
    // -----------------------------------------------------------------------

    /// `grants` parses all five known names from a TOML `[[wasm_consumer]]` block.
    #[test]
    fn wasm_grant_all_names_parse() {
        let toml_str = r#"
slug = "test"
component_path = "/tmp/test.wasm"
grants = ["ports", "store", "log", "alert", "config"]
store_path = "/tmp/test.sqlite"
"#;
        let raw: WasmConsumerConfigRaw = toml::from_str(toml_str).unwrap();
        assert_eq!(raw.grants.len(), 5);
        assert!(raw.grants.contains(&WasmGrant::Ports));
        assert!(raw.grants.contains(&WasmGrant::Store));
        assert!(raw.grants.contains(&WasmGrant::Log));
        assert!(raw.grants.contains(&WasmGrant::Alert));
        assert!(raw.grants.contains(&WasmGrant::Config));
    }

    /// An unknown grant name is rejected by serde.
    #[test]
    fn wasm_grant_unknown_name_rejected() {
        let toml_str = r#"
slug = "test"
component_path = "/tmp/test.wasm"
grants = ["not-a-real-grant"]
"#;
        let result: Result<WasmConsumerConfigRaw, _> = toml::from_str(toml_str);
        assert!(
            result.is_err(),
            "unknown grant name must be rejected by serde"
        );
    }

    /// Missing `grants` field is a serde error (required, no default).
    #[test]
    fn wasm_grant_missing_field_rejected() {
        let toml_str = r#"
slug = "test"
component_path = "/tmp/test.wasm"
"#;
        let result: Result<WasmConsumerConfigRaw, _> = toml::from_str(toml_str);
        assert!(
            result.is_err(),
            "missing grants field must be a serde error"
        );
    }

    /// Empty grants list parses successfully (degenerate zero-capability consumer).
    #[test]
    fn wasm_grant_empty_list_parses() {
        let toml_str = r#"
slug = "test"
component_path = "/tmp/test.wasm"
grants = []
"#;
        let raw: WasmConsumerConfigRaw = toml::from_str(toml_str).unwrap();
        assert!(raw.grants.is_empty());
    }

    // -----------------------------------------------------------------------
    // WASM ACL raw fields: subscribe_acl / publish_acl (design §2.5.1)
    // -----------------------------------------------------------------------

    /// The flat top-level `subscribe_acl` / `publish_acl` channel-matcher fields
    /// parse from a `[[wasm_consumer]]` block, each entry deserializing into the
    /// shared `ChannelMatcherRaw` (`{ exact = ... }` xor `{ prefix = ... }`).
    #[test]
    fn wasm_acl_fields_parse_channel_matchers() {
        use crate::access::raw::ChannelMatcherRaw;
        let toml_str = r#"
slug = "test"
component_path = "/tmp/test.wasm"
grants = ["ports"]

[[subscribe_acl]]
exact = "inbox"

[[subscribe_acl]]
prefix = "events."

[[publish_acl]]
exact = "outbox"
"#;
        let raw: WasmConsumerConfigRaw = toml::from_str(toml_str).unwrap();
        assert!(matches!(
            raw.subscribe_acl.as_slice(),
            [ChannelMatcherRaw::Exact(e), ChannelMatcherRaw::Prefix(p)]
                if e == "inbox" && p == "events."
        ));
        assert!(matches!(
            raw.publish_acl.as_slice(),
            [ChannelMatcherRaw::Exact(e)] if e == "outbox"
        ));
    }

    /// Absent ACL fields default to empty lists (`#[serde(default)]`): a component
    /// that authors no ACL block still parses, with deny-by-default empty lists.
    #[test]
    fn wasm_acl_fields_default_empty() {
        let toml_str = r#"
slug = "test"
component_path = "/tmp/test.wasm"
grants = []
"#;
        let raw: WasmConsumerConfigRaw = toml::from_str(toml_str).unwrap();
        assert!(raw.subscribe_acl.is_empty());
        assert!(raw.publish_acl.is_empty());
    }

    /// `deny_unknown_fields` on `WasmConsumerConfigRaw` is preserved after adding
    /// the ACL fields: a misspelled top-level key still fails to parse rather than
    /// being silently dropped (a silent over-grant if it were an ACL typo).
    #[test]
    fn wasm_acl_unknown_field_rejected() {
        let toml_str = r#"
slug = "test"
component_path = "/tmp/test.wasm"
grants = []

[[subscribe_acls]]
exact = "inbox"
"#;
        let result: Result<WasmConsumerConfigRaw, _> = toml::from_str(toml_str);
        assert!(
            result.is_err(),
            "a misspelled ACL field (subscribe_acls) must not parse"
        );
    }

    /// A `[[subscribe_acl]]` entry with neither `exact` nor `prefix` must fail to
    /// parse on the WASM authoring surface, mirroring the LLM-side coverage in
    /// `access/raw.rs` — the externally-tagged `ChannelMatcherRaw` requires exactly
    /// one variant key, and that invariant must hold through `WasmConsumerConfigRaw`
    /// directly (not only through the `AppAclRaw` parse-path wrapper).
    #[test]
    fn wasm_acl_matcher_neither_key_rejected() {
        let toml_str = r#"
slug = "test"
component_path = "/tmp/test.wasm"
grants = []

[[subscribe_acl]]
"#;
        let result: Result<WasmConsumerConfigRaw, _> = toml::from_str(toml_str);
        assert!(
            result.is_err(),
            "a WASM ACL matcher with no kind (neither exact nor prefix) must not parse"
        );
    }

    /// A `[[subscribe_acl]]` entry with both `exact` and `prefix` must fail to parse
    /// on the WASM authoring surface (a matcher is one kind, not two —
    /// `deny_unknown_fields` on `ChannelMatcherRaw` reinforces this).
    #[test]
    fn wasm_acl_matcher_both_keys_rejected() {
        let toml_str = r#"
slug = "test"
component_path = "/tmp/test.wasm"
grants = []

[[subscribe_acl]]
exact = "inbox"
prefix = "events."
"#;
        let result: Result<WasmConsumerConfigRaw, _> = toml::from_str(toml_str);
        assert!(
            result.is_err(),
            "a WASM ACL matcher with two kinds (exact and prefix) must not parse"
        );
    }

    /// The flat top-level `mqtt_subscribe_acl` / `webhook_acl` fields parse from a
    /// `[[wasm_consumer]]` block, each entry deserializing into the shared LLM-side
    /// matcher type (`MqttSubMatcherRaw` / `WebhookMatcherRaw`).
    #[test]
    fn wasm_subscribe_acl_fields_parse() {
        let toml_str = r#"
slug = "test"
component_path = "/tmp/test.wasm"
grants = []

[[mqtt_subscribe_acl]]
client = "home"
topic_filter = "sensors/+/temp"

[[webhook_acl]]
endpoint = "push-alice"
"#;
        let raw: WasmConsumerConfigRaw = toml::from_str(toml_str).unwrap();
        assert_eq!(raw.mqtt_subscribe_acl.len(), 1);
        assert_eq!(raw.mqtt_subscribe_acl[0].client, "home");
        assert_eq!(raw.mqtt_subscribe_acl[0].topic_filter, "sensors/+/temp");
        assert_eq!(raw.webhook_acl.len(), 1);
        assert_eq!(raw.webhook_acl[0].endpoint, "push-alice");
    }

    /// Absent `mqtt_subscribe_acl` / `webhook_acl` default to empty lists
    /// (`#[serde(default)]`): deny-by-default with no grant derived.
    #[test]
    fn wasm_subscribe_acl_fields_default_empty() {
        let toml_str = r#"
slug = "test"
component_path = "/tmp/test.wasm"
grants = []
"#;
        let raw: WasmConsumerConfigRaw = toml::from_str(toml_str).unwrap();
        assert!(raw.mqtt_subscribe_acl.is_empty());
        assert!(raw.webhook_acl.is_empty());
    }

    /// `finalize_directory_with_subscribers` threads an `App` (UrgencyGated)
    /// subscription's `wake_min` onto its `SubscriberEntry` as `Some`.
    #[test]
    fn wake_min_threaded_into_subscriber_entry() {
        use crate::messaging::{ChannelScheme, SubscriberEntryKind};
        use uuid::Uuid;

        let chan_uuid = Uuid::new_v4();
        let entry = crate::messaging::ChannelEntry {
            uuid: chan_uuid,
            address: "brenn:ch".to_string(),
            description: None,
            resolved_channel: ResolvedChannel {
                send_rate: Default::default(),
                push_depth: Depth::Unbounded,
                retain_depth: Depth::Unbounded,
                standing_retain_depth: Depth::Unbounded,
                noise: NoiseLevel::Silent,
                sink: Sink::Drop,
                wake_min: WakeMin::Normal,
            },
            subscribers: vec![],
            transport_type: ChannelScheme::Brenn,
            mount: None,
        };
        let apps = vec![(
            "pfin".to_string(),
            ResolvedMessagingConfig {
                send_budget: 100,
                subscriptions: vec![ResolvedSubscription {
                    channel_uuid: chan_uuid,
                    channel_address: "brenn:ch".to_string(),
                    push_depth: Depth::Unbounded,
                    retain_depth: Depth::Unbounded,
                    noise: NoiseLevel::Silent,
                    wake_min: WakeMin::Never, // explicitly non-default
                }],
            },
        )];
        let dir = finalize_directory_with_subscribers(vec![entry], &apps, &[], &[]);
        let chan = dir.by_uuid(&chan_uuid).expect("channel must be present");
        assert_eq!(chan.subscribers.len(), 1);
        assert!(
            matches!(&chan.subscribers[0].kind, SubscriberEntryKind::App(_)),
            "subscriber must be App"
        );
        assert_eq!(
            chan.subscribers[0].wake_min,
            Some(WakeMin::Never),
            "wake_min must be threaded through finalize_directory_with_subscribers"
        );
    }

    // -----------------------------------------------------------------------
    // Dynamic-subscription boot merge (design §2.1)
    // -----------------------------------------------------------------------

    fn dyn_row(
        channel_uuid: Uuid,
        app: &str,
        push: Depth,
        retain: Depth,
    ) -> crate::messaging::db::DynamicSubscriptionRow {
        crate::messaging::db::DynamicSubscriptionRow {
            channel_uuid,
            app_slug: app.to_string(),
            push_depth: push,
            retain_depth: retain,
            noise: NoiseLevel::Silent,
            wake_min: WakeMin::Normal,
            qos: None,
            created_at: "2026-06-20T00:00:00Z".to_string(),
        }
    }

    /// A `brenn` `AppPolicy` that authorizes delivery on `brenn:<channel>`:
    /// `MessagingSubscribe` grant + a covering `brenn_subscribe` matcher (the
    /// `<channel>` part of the address, without the `brenn:` prefix). This is the
    /// delivery-holding authorization the boot ACL gate checks (no
    /// `DynamicSubscribe` — the merge gate uses `allows_channel_access`, design §2.2).
    fn covering_brenn_policy(channel: &str) -> crate::access::AppPolicy {
        // Shared `test_support` constructor (reuse-2); `Exact` scopes it to the one
        // channel this merge test covers.
        crate::messaging::test_support::brenn_delivery_policy(
            crate::access::acl::ChannelMatcher::Exact(channel.to_string()),
        )
    }

    /// A policy view that returns the same covering policy for every slug. Used by
    /// the merge tests whose rows must fold (`kept`) — the ACL gate must pass so
    /// the test exercises the kept/dropped/collision paths, not the revoke path.
    fn permit_all_policy() -> crate::access::AppPolicy {
        covering_brenn_policy("ch")
    }

    /// Run `merge_dynamic_subscriptions` with a single policy applied to every
    /// slug and an empty skip report (nothing undeclared-but-extant). Wrapping
    /// the `&dyn Fn` view in a named function gives the closure's returned borrow
    /// an explicit (non-`'static`) lifetime tied to `policy`, which
    /// inline-closure type inference otherwise over-constrains to `'static`.
    fn merge_with_policy(
        dir: &MessagingDirectory,
        rows: &[crate::messaging::db::DynamicSubscriptionRow],
        policy: &crate::access::AppPolicy,
    ) -> DynamicMergeOutcome {
        merge_dynamic_subscriptions(dir, rows, &HashMap::new(), &|_| Some(policy))
    }

    /// As `merge_with_policy` but with a skip report naming channels whose rows
    /// exist while no block declares them.
    fn merge_with_skipped(
        dir: &MessagingDirectory,
        rows: &[crate::messaging::db::DynamicSubscriptionRow],
        skipped: &HashMap<Uuid, String>,
        policy: &crate::access::AppPolicy,
    ) -> DynamicMergeOutcome {
        merge_dynamic_subscriptions(dir, rows, skipped, &|_| Some(policy))
    }

    /// The dormant registration the merge reports for `row` when its channel
    /// sits at `address`.
    fn dormant_of(
        row: &crate::messaging::db::DynamicSubscriptionRow,
        address: &str,
    ) -> DormantSubscription {
        DormantSubscription {
            channel_uuid: row.channel_uuid,
            app_slug: row.app_slug.clone(),
            channel_address: address.to_string(),
        }
    }

    /// As `merge_with_policy` but with no policy for any slug (every row
    /// fail-closed → revoked).
    fn merge_with_no_policy(
        dir: &MessagingDirectory,
        rows: &[crate::messaging::db::DynamicSubscriptionRow],
    ) -> DynamicMergeOutcome {
        merge_dynamic_subscriptions(dir, rows, &HashMap::new(), &|_| None)
    }

    /// A dynamic row is folded onto its channel as an `App(slug)` subscriber.
    #[test]
    fn merge_folds_dynamic_row_onto_existing_channel() {
        use crate::messaging::SubscriberEntryKind;

        let (dir, _addr) = directory_with_one_channel();
        let chan_uuid = dir.list()[0].uuid;
        let rows = vec![dyn_row(
            chan_uuid,
            "graf",
            Depth::Bounded(0),
            Depth::Bounded(5),
        )];

        let outcome = merge_with_policy(&dir, &rows, &permit_all_policy());
        // Folded row is reported as kept (for mirroring), none dropped.
        assert_eq!(outcome.kept, rows, "folded row reported as kept");
        assert!(outcome.dropped.is_empty(), "nothing dropped");
        assert!(outcome.revoked.is_empty(), "nothing revoked");

        let chan = dir.by_uuid(&chan_uuid).expect("channel present");
        assert_eq!(chan.subscribers.len(), 1);
        assert!(
            matches!(&chan.subscribers[0].kind, SubscriberEntryKind::App(s) if s == "graf"),
            "dynamic row must fold as App(graf)"
        );
        assert_eq!(chan.subscribers[0].push_depth, Depth::Bounded(0));
        assert_eq!(chan.subscribers[0].retain_depth, Depth::Bounded(5));
    }

    /// A dynamic row whose channel row is gone from `messaging_channels` too is
    /// dropped (no panic) — the operator deleted the channel outright, which is
    /// the documented retirement path.
    #[test]
    fn merge_drops_row_for_absent_channel() {
        let (dir, _addr) = directory_with_one_channel();
        let known = dir.list()[0].uuid;
        let absent = Uuid::new_v4();
        let rows = vec![dyn_row(
            absent,
            "graf",
            Depth::Bounded(0),
            Depth::Bounded(1),
        )];

        // Must not panic; the only channel keeps its (empty) subscriber set.
        let outcome = merge_with_policy(&dir, &rows, &permit_all_policy());
        assert!(dir.by_uuid(&absent).is_none(), "absent channel not created");
        assert!(
            dir.by_uuid(&known)
                .expect("known channel")
                .subscribers
                .is_empty(),
            "no subscriber added anywhere",
        );
        // The absent-channel row is reported dropped (for durable-table prune),
        // not kept (it must not be mirrored).
        assert!(outcome.kept.is_empty(), "nothing kept");
        assert_eq!(
            outcome.dropped,
            vec![(absent, "graf".to_string())],
            "absent-channel row reported dropped",
        );
        assert!(outcome.revoked.is_empty(), "nothing revoked");
    }

    /// The same row, but the channel's own row is still in `messaging_channels` —
    /// the `[[channel]]` block was removed, renamed, or commented out. That is a
    /// config change the operator may revert, so the subscription goes dormant
    /// instead of being destroyed.
    #[test]
    fn merge_holds_a_row_dormant_when_only_the_block_is_gone() {
        let (dir, _addr) = directory_with_one_channel();
        let undeclared_uuid = Uuid::new_v4();
        let rows = vec![dyn_row(
            undeclared_uuid,
            "graf",
            Depth::Bounded(0),
            Depth::Bounded(1),
        )];
        let skipped: HashMap<Uuid, String> = [(undeclared_uuid, "retired".to_string())]
            .into_iter()
            .collect();

        let outcome = merge_with_skipped(&dir, &rows, &skipped, &permit_all_policy());

        assert!(
            outcome.kept.is_empty(),
            "an undeclared channel folds nothing"
        );
        assert!(
            outcome.dropped.is_empty(),
            "a revertible edit must not destroy the durable row",
        );
        assert_eq!(
            outcome.revoked,
            vec![dormant_of(&rows[0], "retired")],
            "the row is retained, dormant, under the address the skip report gave",
        );
        assert!(
            dir.by_uuid(&undeclared_uuid).is_none(),
            "the channel is not conjured into the directory",
        );
    }

    /// The dormant arm keys on skip-report membership alone. A chat leaf — a
    /// `brenn:` address outside every system family — classifies exactly like an
    /// operator-declared one, with no family special case in the merge.
    #[test]
    fn merge_holds_a_chat_leaf_row_dormant_by_the_same_rule() {
        let (dir, _addr) = directory_with_one_channel();
        let leaf_uuid = Uuid::new_v4();
        let rows = vec![dyn_row(
            leaf_uuid,
            "graf",
            Depth::Bounded(0),
            Depth::Bounded(1),
        )];
        let skipped: HashMap<Uuid, String> = [(leaf_uuid, "chat/host/out/7".to_string())]
            .into_iter()
            .collect();

        let outcome = merge_with_skipped(&dir, &rows, &skipped, &permit_all_policy());

        assert_eq!(
            outcome.revoked,
            vec![dormant_of(&rows[0], "chat/host/out/7")]
        );
        assert!(outcome.dropped.is_empty());
    }

    /// The directory answers first; skip-report membership is consulted only
    /// when it cannot. The report names every non-system address the loader
    /// declined to reconstruct, declared ones included, so on an ordinary boot a
    /// live channel's uuid sits in both places at once — and the row must still
    /// fold. Reading the report first would put every operator-declared
    /// subscription to sleep on every boot while its row sat undisturbed.
    #[test]
    fn merge_folds_a_row_whose_channel_is_in_the_directory_and_the_skip_report() {
        use crate::messaging::SubscriberEntryKind;

        let (dir, _addr) = directory_with_one_channel();
        let chan_uuid = dir.list()[0].uuid;
        let rows = vec![dyn_row(
            chan_uuid,
            "graf",
            Depth::Bounded(0),
            Depth::Bounded(5),
        )];
        let skipped: HashMap<Uuid, String> = [(chan_uuid, dir.list()[0].address.clone())]
            .into_iter()
            .collect();

        let outcome = merge_with_skipped(&dir, &rows, &skipped, &permit_all_policy());

        assert_eq!(
            outcome.kept, rows,
            "a channel the directory answers for folds live however the skip report reads",
        );
        assert!(outcome.revoked.is_empty(), "nothing dormant");
        assert!(outcome.dropped.is_empty(), "nothing dropped");
        let chan = dir.by_uuid(&chan_uuid).expect("channel present");
        assert!(
            matches!(&chan.subscribers[0].kind, SubscriberEntryKind::App(s) if s == "graf"),
            "the subscriber must reach the channel",
        );
    }

    /// A dynamic row colliding with a static sub on the same (channel, app) is
    /// dropped — static wins — leaving exactly the static subscriber.
    #[test]
    fn merge_drops_row_colliding_with_static_sub() {
        use crate::messaging::SubscriberEntryKind;

        let entries = build_channel_entries(
            &[raw_channel("1f6c6e3a-1d6e-4f7c-9b6a-12cb7e4a8d32", "ch")],
            &global_defaults(),
        );
        let chan_uuid = entries[0].uuid;
        // Static App(graf) subscriber already present (push-enabled).
        let static_subs = vec![(
            "graf".to_string(),
            ResolvedMessagingConfig {
                send_budget: 1,
                subscriptions: vec![ResolvedSubscription {
                    channel_uuid: chan_uuid,
                    channel_address: entries[0].address.clone(),
                    push_depth: Depth::Bounded(10),
                    retain_depth: Depth::Bounded(10),
                    noise: NoiseLevel::Silent,
                    wake_min: WakeMin::Normal,
                }],
            },
        )];
        let dir = finalize_directory_with_subscribers(entries, &static_subs, &[], &[]);

        // Dynamic row for the SAME (channel, graf) with different params.
        let rows = vec![dyn_row(
            chan_uuid,
            "graf",
            Depth::Bounded(0),
            Depth::Bounded(1),
        )];
        let outcome = merge_with_policy(&dir, &rows, &permit_all_policy());
        // Static collision → row reported dropped (prune from durable table), not
        // kept (must not be mirrored — the static row already mirrors it).
        assert!(outcome.kept.is_empty(), "nothing kept");
        assert!(outcome.revoked.is_empty(), "nothing revoked");
        assert_eq!(
            outcome.dropped,
            vec![(chan_uuid, "graf".to_string())],
            "colliding row reported dropped",
        );

        let chan = dir.by_uuid(&chan_uuid).expect("channel present");
        assert_eq!(chan.subscribers.len(), 1, "static wins, dynamic dropped");
        assert!(matches!(&chan.subscribers[0].kind, SubscriberEntryKind::App(s) if s == "graf"),);
        // The surviving subscriber is the static one (Bounded(10)), not the
        // dynamic row's Bounded(0).
        assert_eq!(
            chan.subscribers[0].push_depth,
            Depth::Bounded(10),
            "static params survive, not the dropped dynamic row's",
        );
    }

    /// A dynamic row for a *different* app on a channel that already has a
    /// static sub for another app is folded (no collision), and the existing
    /// subscriber is preserved.
    #[test]
    fn merge_folds_alongside_other_app_static_sub() {
        use crate::messaging::SubscriberEntryKind;

        let entries = build_channel_entries(
            &[raw_channel("1f6c6e3a-1d6e-4f7c-9b6a-12cb7e4a8d32", "ch")],
            &global_defaults(),
        );
        let chan_uuid = entries[0].uuid;
        let static_subs = vec![(
            "pfin".to_string(),
            ResolvedMessagingConfig {
                send_budget: 1,
                subscriptions: vec![ResolvedSubscription {
                    channel_uuid: chan_uuid,
                    channel_address: entries[0].address.clone(),
                    push_depth: Depth::Bounded(0),
                    retain_depth: Depth::Bounded(3),
                    noise: NoiseLevel::Silent,
                    wake_min: WakeMin::Normal,
                }],
            },
        )];
        let dir = finalize_directory_with_subscribers(entries, &static_subs, &[], &[]);

        let rows = vec![dyn_row(
            chan_uuid,
            "graf",
            Depth::Bounded(0),
            Depth::Bounded(1),
        )];
        let outcome = merge_with_policy(&dir, &rows, &permit_all_policy());
        // Different-app row folds alongside the other-app static sub: kept (to be
        // mirrored), nothing dropped.
        assert_eq!(outcome.kept, rows, "folded row reported as kept");
        assert!(outcome.dropped.is_empty(), "nothing dropped");
        assert!(outcome.revoked.is_empty(), "nothing revoked");

        let chan = dir.by_uuid(&chan_uuid).expect("channel present");
        assert_eq!(chan.subscribers.len(), 2, "both apps present");
        assert!(
            chan.subscribers
                .iter()
                .any(|s| matches!(&s.kind, SubscriberEntryKind::App(x) if x == "pfin")),
            "static pfin sub preserved",
        );
        assert!(
            chan.subscribers
                .iter()
                .any(|s| matches!(&s.kind, SubscriberEntryKind::App(x) if x == "graf")),
            "dynamic graf sub folded",
        );
    }

    /// A dynamic row whose app's policy no longer covers the channel is classified
    /// `revoked` (design §2.2): NOT folded into the directory, NOT reported as
    /// `dropped` (so the boot caller does not prune it — the durable row survives
    /// dormant). The channel itself remains present but with no subscriber.
    #[test]
    fn merge_revokes_row_when_policy_does_not_cover_channel() {
        use crate::access::{AppCapability, AppPolicy};

        let (dir, address) = directory_with_one_channel();
        let chan_uuid = dir.list()[0].uuid;
        let rows = vec![dyn_row(
            chan_uuid,
            "graf",
            Depth::Bounded(0),
            Depth::Bounded(5),
        )];

        // Policy holds the transport grant but NO covering `brenn_subscribe`
        // matcher (the operator removed the ACL): delivery is not authorized.
        let mut p = AppPolicy::default();
        p.grants.insert(AppCapability::MessagingSubscribe);
        let outcome = merge_with_policy(&dir, &rows, &p);

        assert!(outcome.kept.is_empty(), "revoked row must not be kept");
        assert!(
            outcome.dropped.is_empty(),
            "revoked row must not be dropped (must not be pruned)"
        );
        assert_eq!(
            outcome.revoked,
            vec![dormant_of(&rows[0], &address)],
            "row reported revoked",
        );

        let chan = dir.by_uuid(&chan_uuid).expect("channel still present");
        assert!(
            chan.subscribers.is_empty(),
            "revoked row must not be folded as a subscriber"
        );
    }

    /// A dynamic row whose app has no resolvable policy at all is classified
    /// `revoked` (fail-closed), not `dropped` — the durable row is retained in case
    /// the policy is restored, rather than destroyed.
    #[test]
    fn merge_revokes_row_when_policy_is_missing() {
        let (dir, address) = directory_with_one_channel();
        let chan_uuid = dir.list()[0].uuid;
        let rows = vec![dyn_row(
            chan_uuid,
            "graf",
            Depth::Bounded(0),
            Depth::Bounded(5),
        )];

        // No policy for any slug → fail-closed → revoked (not dropped).
        let outcome = merge_with_no_policy(&dir, &rows);

        assert!(outcome.kept.is_empty(), "missing-policy row not kept");
        assert!(
            outcome.dropped.is_empty(),
            "missing-policy row not dropped (must not be pruned)"
        );
        assert_eq!(
            outcome.revoked,
            vec![dormant_of(&rows[0], &address)],
            "missing-policy row reported revoked",
        );
    }

    /// The same row that revokes under a non-covering policy is `kept` once the
    /// policy covers the channel — pinning that the gate is the only difference and
    /// the row resumes when the ACL is re-granted (design §2.2 "revoked → kept").
    #[test]
    fn merge_keeps_row_when_policy_covers_channel() {
        let (dir, _addr) = directory_with_one_channel();
        let chan_uuid = dir.list()[0].uuid;
        let rows = vec![dyn_row(
            chan_uuid,
            "graf",
            Depth::Bounded(0),
            Depth::Bounded(5),
        )];

        let outcome = merge_with_policy(&dir, &rows, &covering_brenn_policy("ch"));

        assert_eq!(outcome.kept, rows, "covering-policy row kept");
        assert!(outcome.revoked.is_empty(), "nothing revoked");
        assert!(outcome.dropped.is_empty(), "nothing dropped");
        let chan = dir.by_uuid(&chan_uuid).expect("channel present");
        assert_eq!(chan.subscribers.len(), 1, "row folded as subscriber");
    }

    /// Boot-merge retain-depth conformance: on a channel with a
    /// bounded `standing_retain_depth`, an over-standing durable row is `revoked`
    /// (dormant — not folded, not pruned) while a conforming row on the same
    /// channel is `kept` and folded. The ACL is intact (covering policy), so only
    /// the retain-depth gate distinguishes them.
    #[test]
    fn merge_revokes_over_standing_row_keeps_conforming() {
        use crate::messaging::SubscriberEntryKind;

        // Channel with a bounded standing depth of 2; its own rungs sit within it.
        let mut raw = raw_channel("1f6c6e3a-1d6e-4f7c-9b6a-12cb7e4a8d32", "ch");
        raw.push_depth = Some(Depth::Bounded(2));
        raw.retain_depth = Some(Depth::Bounded(2));
        raw.standing_retain_depth = Some(Depth::Bounded(2));
        let entries = build_channel_entries(&[raw], &global_defaults());
        let chan_uuid = entries[0].uuid;
        let address = entries[0].address.clone();
        let dir = MessagingDirectory::with_entries(entries);

        // Over-standing row (retain 5 > standing 2) and a conforming row (retain 2
        // == standing 2) from two different apps on the same channel.
        let over = dyn_row(chan_uuid, "deep", Depth::Bounded(0), Depth::Bounded(5));
        let conforming = dyn_row(chan_uuid, "ok", Depth::Bounded(0), Depth::Bounded(2));
        let rows = vec![over.clone(), conforming.clone()];

        let outcome = merge_with_policy(&dir, &rows, &covering_brenn_policy("ch"));

        assert_eq!(
            outcome.revoked,
            vec![dormant_of(&over, &address)],
            "over-standing row revoked",
        );
        assert_eq!(outcome.kept, vec![conforming], "conforming row kept");
        assert!(
            outcome.dropped.is_empty(),
            "over-standing row not pruned (dormant, revertible)"
        );
        // Only the conforming subscriber is folded.
        let chan = dir.by_uuid(&chan_uuid).expect("channel present");
        assert_eq!(chan.subscribers.len(), 1, "only conforming row folded");
        assert!(
            matches!(&chan.subscribers[0].kind, SubscriberEntryKind::App(s) if s == "ok"),
            "folded subscriber is the conforming app"
        );
    }

    /// The boot-merge depth gate covers `push_depth` as well: a stored row whose
    /// push depth sits above the channel's current standing depth is `revoked`
    /// (dormant), not folded and not pruned, exactly like the retain case.
    #[test]
    fn merge_revokes_a_row_whose_push_depth_exceeds_standing() {
        use crate::messaging::SubscriberEntryKind;

        let mut raw = raw_channel("1f6c6e3a-1d6e-4f7c-9b6a-12cb7e4a8d32", "ch");
        raw.push_depth = Some(Depth::Bounded(2));
        raw.retain_depth = Some(Depth::Bounded(2));
        raw.standing_retain_depth = Some(Depth::Bounded(2));
        let entries = build_channel_entries(&[raw], &global_defaults());
        let chan_uuid = entries[0].uuid;
        let address = entries[0].address.clone();
        let dir = MessagingDirectory::with_entries(entries);

        // Both rows conform on retain; only the push depth separates them.
        let over = dyn_row(chan_uuid, "loud", Depth::Bounded(5), Depth::Bounded(2));
        let conforming = dyn_row(chan_uuid, "ok", Depth::Bounded(2), Depth::Bounded(2));
        let rows = vec![over.clone(), conforming.clone()];

        let outcome = merge_with_policy(&dir, &rows, &covering_brenn_policy("ch"));

        assert_eq!(
            outcome.revoked,
            vec![dormant_of(&over, &address)],
            "over-standing push row revoked",
        );
        assert_eq!(outcome.kept, vec![conforming], "conforming row kept");
        assert!(
            outcome.dropped.is_empty(),
            "over-standing row not pruned (dormant, revertible)"
        );
        let chan = dir.by_uuid(&chan_uuid).expect("channel present");
        assert_eq!(chan.subscribers.len(), 1, "only conforming row folded");
        assert!(
            matches!(&chan.subscribers[0].kind, SubscriberEntryKind::App(s) if s == "ok"),
            "folded subscriber is the conforming app"
        );
    }

    /// A runtime-created channel that is **not in config** — folded into the
    /// directory by the boot reconstruction (Fix 1, via `add_channel`) — is `kept`
    /// by the merge, not `dropped`. This pins the boot-fold→merge contract at the
    /// unit level (test-1): the merge classifies a row `kept` purely on the channel
    /// being present in the directory (`by_uuid`), regardless of how it got there.
    /// If the boot fold and merge were reordered (merge-before-fold), this channel
    /// would be absent and the row would wrongly `drop` — caught here, not only by
    /// the bootstrap integration test.
    #[test]
    fn merge_keeps_runtime_created_channel_folded_into_directory() {
        use crate::messaging::ChannelEntry;
        // Start from a directory WITHOUT the channel (config knows nothing of it).
        let dir = MessagingDirectory::with_entries(vec![]);
        // Simulate the boot reconstruction folding a DB-only channel via the same
        // `add_channel` the boot path uses.
        let chan_uuid = Uuid::new_v4();
        let entry = ChannelEntry {
            uuid: chan_uuid,
            address: "brenn:reconstructed".to_string(),
            description: None,
            resolved_channel: ResolvedChannel {
                send_rate: Default::default(),
                push_depth: Depth::Unbounded,
                retain_depth: Depth::Unbounded,
                standing_retain_depth: Depth::Unbounded,
                noise: NoiseLevel::Silent,
                sink: Sink::Drop,
                wake_min: WakeMin::Normal,
            },
            subscribers: Vec::new(),
            transport_type: ChannelScheme::Brenn,
            mount: None,
        };
        assert!(dir.by_uuid(&chan_uuid).is_none(), "channel absent pre-fold");
        dir.add_channel(entry);

        let rows = vec![dyn_row(
            chan_uuid,
            "graf",
            Depth::Bounded(0),
            Depth::Bounded(5),
        )];
        let outcome = merge_with_policy(&dir, &rows, &covering_brenn_policy("reconstructed"));

        assert_eq!(
            outcome.kept, rows,
            "reconstructed-channel row kept, not dropped"
        );
        assert!(outcome.dropped.is_empty(), "nothing dropped");
        assert!(outcome.revoked.is_empty(), "nothing revoked");
        let chan = dir
            .by_uuid(&chan_uuid)
            .expect("reconstructed channel present");
        assert_eq!(chan.subscribers.len(), 1, "row folded as subscriber");
    }
}
