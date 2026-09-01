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

pub use brenn_envelope::channel_model::ChannelBlockRole;
use brenn_envelope::channel_model::{
    ChannelDepthKey, TUNING_DURABILITY_IGNORED, depth_required, sink_admitted, standing_admitted,
};
use serde::de::{self, Deserializer, Visitor};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{
    AttachGrant, ChannelEntry, ChannelScheme, ComponentGrant, MessagingDirectory,
    SubscriberEntryKind, WakeMin, canonicalize_channel_address, ends_at_tuning_boundary,
    in_a_tool_namespace, is_reserved_channel_name, is_unreserved_char, nondurable_channel_uuid,
    tuning_boundary_list,
};
use crate::config::AppConfigRaw;

// ---------------------------------------------------------------------------
// Depth / NoiseLevel / Sink types
// ---------------------------------------------------------------------------

/// A retention depth value.
///
/// `Bounded(n)` = exactly n most-recent messages; `Unbounded` = the legacy ∞
/// behavior. An *unstated* key is represented as `Option::None` at the
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
///
/// Still a live code path: used by `parse_depth_field` (`brenn-server`) to
/// decode `MessageSubscribe` depth arguments on the LLM tool surface.
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
    /// Parse from a wire/DB string. Returns `None` on unknown values.
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
#[derive(Debug, Clone, PartialEq)]
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

/// A `link` declaration — an **auto channel** named by nothing, brought into
/// existence by the ports bound to it.
///
/// A link has no address, no `channel` declaration and no operator-written ACL
/// entries: the transport grants and channel matchers its endpoints need are
/// injected at boot from this declaration, because binding a port to the link
/// *is* the operator's authorization signal. Channel-level tuning is not
/// available: every depth is the fold over the subscribing endpoints' own
/// declared depths, and `sink`, `send_rate`, `wake_min` and channel-level
/// `noise` come from the `messaging` defaults. A channel that needs any of those
/// stated for itself has outgrown a link; declare the channel.
///
/// **Anonymous, always.** The channel's bare name is `auto.<cid>`, where `cid`
/// is a UUIDv5 of the sorted endpoint set, and its scheme is decided from where
/// the endpoints live: all-backend ⇒ `local:`, all-one-surface ⇒ page-local
/// `local:`, anything spanning the wire ⇒ `ephemeral:`. Adding an endpoint
/// changes the address; nothing outlives that, because a link's channel is never
/// durable.
#[derive(Debug, Clone, PartialEq)]
pub struct LinkConfigRaw {
    /// The handle the link was declared under, for boot messages. Not an
    /// address: nothing resolves a channel through this text.
    pub link: String,
    /// Lands in the channel's directory description. Absent, a description is
    /// generated from the endpoint set, so a uuid-named row in a listing or log
    /// still explains itself.
    pub description: Option<String>,
    /// The ports bound to this link. Each must name a port its host declares,
    /// with the roles that declaration gives it, and must be *free* — declared
    /// with no `channel` of its own. The direction-carrying shape below is what
    /// spares boot a reverse lookup over endpoint strings; that the port is
    /// there at all is still checked, because an endpoint nobody declared would
    /// carry real authority to a port that reads and writes nothing.
    ///
    /// The endpoint set must contain at least one publisher and at least one
    /// subscriber, and must not be a single io_port (which needs no link to loop
    /// back to itself).
    pub endpoints: Vec<LinkEndpointRaw>,
}

/// One port bound to a link, with the roles and depths its binding gave it.
///
/// Direction and depths are carried, not inferred: the binding that named the
/// link is the same statement that declared the port's tuning, so one lowering
/// site fills both and nothing downstream re-derives a role by searching the
/// subscription and output lists for a port name.
#[derive(Debug, Clone, PartialEq)]
pub struct LinkEndpointRaw {
    pub host: LinkHostRaw,
    pub port: String,
    pub publishes: bool,
    pub subscribes: bool,
    /// Whether the port is an io_port — both roles by declaration, and already
    /// entitled to a channel of its own.
    pub io_port: bool,
    /// The subscribing half's window. Required on a subscribing endpoint, and
    /// equal to what the port's own declaration states: a link's retention is
    /// folded from these numbers while the subscriber's cursor window comes
    /// from the declaration, so two answers would size two different windows.
    /// Absent on an endpoint that does not subscribe.
    pub push_depth: Option<Depth>,
    /// The subscribing half's retained window. Same rules as `push_depth`.
    pub retain_depth: Option<Depth>,
}

/// Which host a link endpoint's port lives on.
#[derive(Debug, Clone, PartialEq)]
pub enum LinkHostRaw {
    /// A port on a backend `wasm_consumer`.
    Wasm { slug: String },
    /// A port on a surface-hosted component instance.
    Surface { slug: String, instance: String },
}

/// `[messaging]` section.
#[derive(Debug, Clone, PartialEq)]
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
#[derive(Debug, Clone, PartialEq)]
pub struct MessagingConfigRaw {
    // NOTE: the legacy `enabled` authorization boolean was removed. Authorization
    // is now decided by the app's `AppPolicy` (`messaging_enabled()` reads
    // `MessagingPublish`/`MessagingSubscribe` grants). No vocabulary admits an
    // `enabled` word, so a stale config naming one is refused at that word — the
    // intended migration-forcing.
    /// Channel addresses (with `brenn:` prefix) this app subscribes to.
    pub subscribe: Vec<MessagingSubscriptionRaw>,
    /// Per-conversation send budget reset on each user chat message.
    /// Defaults to the global `[messaging].default_send_budget`.
    pub send_budget: Option<u32>,
}

/// One agent subscription's per-subscription tuning.
///
/// `standing_retain_depth` and `sink` are per-channel/global only; a
/// subscription tail that states either is refused at that key.
#[derive(Debug, Clone, PartialEq)]
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

/// Top-level `[[wasm_consumer]]` block.
///
/// Declares a WASM processing component as a bus subscriber. The installed
/// package named by `package` is resolved and loaded at bootstrap; a missing or
/// unloadable component is a fail-fast bootstrap panic (config is
/// host-authored).
#[derive(Debug, Clone, PartialEq)]
pub struct WasmConsumerConfigRaw {
    /// Globally unique slug; becomes `wasm:<slug>` as the participant identity.
    /// Charset: `[A-Za-z0-9._~-]+`, no `:` or `@`.
    pub slug: String,
    /// The installed component package the artifact is resolved from — the
    /// packaged module the instance's class was declared in. Not a body key:
    /// the class's fact, carried through lowering.
    pub package: String,
    /// Lowercase hex SHA-256 of the spec file this instance's class was
    /// declared in. Not a body key — the class's fact, carried through lowering
    /// so boot can bind it to the spec packaged beside the artifact.
    pub spec_sha256: String,
    /// Every port name this instance's class declares with direction `out` or
    /// `io`, sorted and duplicate-free — the complete vocabulary of names the
    /// component may legally publish to. Not a body key: the class's fact,
    /// carried through lowering so the host can tell a declared-but-unwired
    /// port (publish drops) from a name the specification never declared (the
    /// activation traps).
    pub declared_out_ports: Vec<String>,
    /// Capability interfaces to link for this component (deny-by-default).
    /// Required — no default. The operator states intent explicitly; an unstated
    /// `grants` is refused. Empty list = zero-capability consumer.
    pub grants: Vec<ComponentGrant>,
    /// Path to the per-component SQLite KV store. Required iff `"store"` is in
    /// `grants`; must be absent otherwise.
    pub store_path: Option<std::path::PathBuf>,
    /// Per-consumer store size limit override. Human-readable binary size
    /// string (e.g. `"64MiB"`). `None` ⇒ use `[wasm].store_size_limit` global default.
    /// Must be absent when `"store"` is not in `grants`.
    pub store_size_limit: Option<String>,
    /// Channel subscriptions for this component.
    pub subscriptions: Vec<WasmConsumerSubscriptionRaw>,
    /// Output port bindings for this component.
    pub outputs: Vec<WasmConsumerOutputRaw>,
    /// Combined input+output port declarations for this component — the
    /// self-loop, made structural. See [`WasmConsumerIoPortRaw`].
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
    pub subscribe_acl: Vec<crate::access::raw::ChannelMatcherRaw>,
    /// Layer-2 ephemeral subscribe ACL: channel matchers narrowing which
    /// `ephemeral:` channels this component may hold a (static) subscription to.
    /// Same flat shape as `subscribe_acl`, scoped to the `ephemeral:` scheme
    /// (matchers are scheme-stripped names). Non-empty derives the
    /// `EphemeralSubscribe` transport grant; empty means deny-by-default. A
    /// ring-backed input's authorization is decided here at boot — ring delivery
    /// reads the subscriber cursor directly and never re-runs a delivery ACL gate.
    pub ephemeral_subscribe_acl: Vec<crate::access::raw::ChannelMatcherRaw>,
    /// `local:` (confined) channels this component may hold a (static) input on.
    /// Same flat shape as `ephemeral_subscribe_acl`, scoped to the `local:`
    /// scheme (matchers are scheme-stripped names). Non-empty derives the
    /// `LocalSubscribe` transport grant; empty means deny-by-default. Like the
    /// ephemeral case, a confined input's authorization is decided here at boot —
    /// ring delivery reads the subscriber cursor directly and never re-runs a
    /// delivery ACL gate.
    pub local_subscribe_acl: Vec<crate::access::raw::ChannelMatcherRaw>,
    /// Layer-2 publish ACL: channel matchers narrowing which `brenn:` channels this
    /// component's output ports may publish to. Same flat-`Vec` shape as
    /// `subscribe_acl`; deny-by-default when empty.
    pub publish_acl: Vec<crate::access::raw::ChannelMatcherRaw>,
    /// Layer-2 ephemeral publish ACL: channel matchers narrowing which
    /// `ephemeral:` channels this component's output ports may publish to. Same
    /// shape as `publish_acl`, scoped to the `ephemeral:` scheme (matchers are
    /// scheme-stripped names). Non-empty derives the `EphemeralPublish` capability;
    /// empty means deny-by-default.
    pub ephemeral_publish_acl: Vec<crate::access::raw::ChannelMatcherRaw>,
    /// Layer-2 local publish ACL: channel matchers narrowing which `local:`
    /// (confined) channels this component's output ports may publish to. Same
    /// shape as `ephemeral_publish_acl`, scoped to the `local:` scheme. Non-empty
    /// derives the `LocalPublish` capability; empty means deny-by-default.
    pub local_publish_acl: Vec<crate::access::raw::ChannelMatcherRaw>,
    /// Layer-2 MQTT publish ACL: per-client allowlist narrowing which
    /// `[[mqtt_client]]` slugs this component's `mqtt-publish` host call may
    /// target. Reuses the **same** `client`-keyed `MqttClientMatcherRaw`
    /// matcher as the LLM `[[app.acl.mqtt_publish]]` block, resolving into
    /// `AppPolicy.acls.mqtt_publish` exactly as the LLM side does; the guest
    /// addresses MQTT egress by client slug and the ACL is the attenuation
    /// boundary. Same flat top-level `Vec` authoring convention as
    /// `subscribe_acl`/`publish_acl` (the table-nesting asymmetry vs. the LLM
    /// `[app.acl.mqtt_publish]` sub-table is deliberate; both resolve into the
    /// same `AppPolicy`). A non-empty list derives no grant on its own — the
    /// `"mqtt"` grant must be authored explicitly in `grants`; an empty list
    /// means the consumer holds no MQTT-publish authorization (deny-by-default).
    pub mqtt_publish_acl: Vec<crate::access::raw::MqttClientMatcherRaw>,
    /// Layer-2 MQTT *subscribe* ACL: `(client, topic_filter)` matchers narrowing
    /// which inbound `mqtt:` channels this component may hold a (static)
    /// subscription to. Reuses the **same** `MqttSubMatcherRaw` matcher as the LLM
    /// `[[app.acl.mqtt_subscribe]]` block, resolving into `AppPolicy.acls.mqtt_subscribe`
    /// exactly as the LLM side does; coverage is filter-subset per `mqtt_match`
    /// (a matcher's `topic_filter` must be a superset of the subscribed filter).
    /// Same flat top-level `Vec` authoring convention as `subscribe_acl`. A
    /// non-empty list derives the `MqttSubscribe` transport grant (there is no
    /// `ComponentGrant` for inbound subscribe, mirroring `subscribe_acl`'s
    /// `MessagingSubscribe` derivation); an empty list means the consumer holds no
    /// MQTT-subscribe authorization (deny-by-default at delivery).
    pub mqtt_subscribe_acl: Vec<crate::access::raw::MqttSubMatcherRaw>,
    /// Layer-2 webhook subscribe ACL: endpoint-slug matchers narrowing which
    /// inbound `webhook:` channels this component may hold a (static) subscription
    /// to. Reuses the **same** `WebhookMatcherRaw` matcher as the LLM
    /// `[[app.acl.webhook]]` block, resolving into `AppPolicy.acls.webhook`; the
    /// matcher `endpoint` is a scheme-stripped slug matched exactly against the
    /// subscribed `webhook:<endpoint>` channel. Same flat top-level `Vec` authoring
    /// convention as `subscribe_acl`. Unqualified (no direction suffix) because
    /// webhooks are inbound-only, matching the LLM side's unqualified `webhook` ACL.
    /// A non-empty list derives the `Webhook` transport grant (no `ComponentGrant` for
    /// inbound webhook, mirroring `subscribe_acl`); an empty list means the consumer
    /// holds no webhook-subscribe authorization (deny-by-default at delivery).
    pub webhook_acl: Vec<crate::access::raw::WebhookMatcherRaw>,
    /// Operator-supplied config map for this component (`[wasm_consumer.config]`).
    /// Values must be strings, integers, or booleans; floats, datetimes, arrays,
    /// and nested tables are rejected at load time. `None` when the sub-table is
    /// absent (equivalent to an empty table).
    pub config: Option<toml::Table>,
    /// Activation pacing burst — the token-bucket capacity in *activations*.
    /// Up to this many activations may run back-to-back after idle before the
    /// sustained gate applies. `None` ⇒ `DEFAULT_ACTIVATION_BURST`. Rejected
    /// at resolve time when `< 1`. Unlike `store_size_limit`, there is no
    /// `[wasm]`-table global fallback — the per-consumer knob (or the hardcoded
    /// default) is the whole surface.
    pub activation_burst: Option<u32>,
    /// Activation pacing minimum period in milliseconds — one activation is
    /// admitted per this interval under sustained load (bucket refill interval).
    /// `None` ⇒ `DEFAULT_ACTIVATION_MIN_PERIOD`. Rejected at resolve
    /// time when `< 1` (a zero interval would panic in `TokenBucket::new`; we
    /// reject it at the config layer where the message names the slug).
    pub activation_min_period_ms: Option<u64>,
    /// Per-MQTT-client egress budget overrides (`[[wasm_consumer.mqtt_output]]`).
    /// One sink exists per `mqtt_publish_acl`-allowed client regardless of these
    /// blocks; a block only overrides that sink's budget knobs. A block naming a
    /// client outside `mqtt_publish_acl`, or a duplicate `client`, is a boot panic.
    pub mqtt_outputs: Vec<WasmConsumerMqttOutputRaw>,
    /// Tool grants for this component (`[[wasm_consumer.tool_grant]]`). Identical
    /// table shape as `[[app.tool_grant]]` — one grant vocabulary, both
    /// participant kinds. Each authorizes addressing a registry tool, optionally
    /// narrowed by an `acl` and throttled by `rate_limit`. Absent ⇒ no tool
    /// authorization (deny-by-default).
    pub tool_grants: Vec<crate::tools::config::ToolGrantRaw>,
}

#[cfg(any(test, feature = "testutils"))]
impl WasmConsumerConfigRaw {
    /// Minimal raw consumer subscribing (port `in`) to each of `channels`, with
    /// everything else defaulted/empty. Shared across this crate's test modules
    /// and the boot crates above it so a new field on this struct lands in one
    /// place instead of every hand-written literal.
    pub fn minimal(slug: &str, package: &str, channels: &[&str]) -> Self {
        WasmConsumerConfigRaw {
            slug: slug.to_string(),
            package: package.to_string(),
            spec_sha256: String::new(),
            declared_out_ports: vec![],
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

    /// The same consumer, carrying the outbound vocabulary its own bindings
    /// imply — every `[[output]]` port plus every `[[io_port]]` name.
    ///
    /// [`imply_out_port_vocabulary`] states the rule, including the escape for
    /// a fixture that has something to say about the vocabulary itself.
    pub fn implying_its_vocabulary(mut self) -> Self {
        let ports: Vec<&str> = self
            .outputs
            .iter()
            .map(|out| out.port.as_str())
            .chain(self.io_ports.iter().map(|io| io.port.as_str()))
            .collect();
        imply_out_port_vocabulary(&mut self.declared_out_ports, ports.into_iter());
        self
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
#[derive(Debug, Clone, PartialEq)]
pub struct WasmConsumerSubscriptionRaw {
    /// Channel address, e.g. `brenn:my-channel` or `webhook:my-endpoint`.
    ///
    /// `None` makes this a **free port**: the binding declares the port and its
    /// tuning, and exactly one `link` must bind it to supply the channel. A free
    /// port bound by no link is dead config and a boot panic. Addresses in the
    /// reserved `auto` namespace are rejected here — an anonymous auto channel is
    /// reachable only through the declarations that created it.
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
#[derive(Debug, Clone, PartialEq)]
pub struct WasmConsumerOutputRaw {
    /// Logical output port name. Must be non-empty and unreserved-charset.
    pub port: String,
    /// Target channel address.
    ///
    /// `None` makes this a **free port**, bound by exactly one `link`; see
    /// [`WasmConsumerSubscriptionRaw::channel`].
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
/// ```text
/// io timer { push_depth = 2; retain_depth = 8; }
/// ```
///
/// With no `channel` naming it, the port gets its own anonymous
/// non-transportable channel — no channel-level config at all: no `channel`
/// declaration, no ACLs. The port's depths are not optional though: the
/// channel's every depth folds from `max(push_depth, retain_depth)` over its
/// subscribing ports, so both must be written here, and both must be bounded on
/// a non-durable channel or boot refuses. Give it a `channel` to
/// make it a named auto channel (`brenn:` for schedules that survive restart)
/// that other components can see and feed.
#[derive(Debug, Clone, PartialEq)]
pub struct WasmConsumerIoPortRaw {
    /// Logical port name presented to the guest for both directions. Must be
    /// non-empty, unreserved-charset, and distinct from every other port name on
    /// the consumer — this block is the only sanctioned way one name serves both
    /// directions, because it is what carries the same-channel guarantee.
    pub port: String,
    /// Absent ⇒ the port's own anonymous non-transportable channel. Present ⇒ a
    /// named auto channel at this full scheme-qualified address. Must be absent
    /// when a `link` binds this port.
    ///
    /// A `local:` name here is a *server* ring: only backend bindings join it.
    /// `local:` namespaces are private per realm, so a surface binding on the
    /// same bare name is an unrelated page ring, by design — the two exchange no
    /// message and neither knows the other exists. `ephemeral:` is what reaches
    /// a page.
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
#[derive(Debug, Clone, PartialEq)]
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
/// blocks.
///
/// `allowed_users` is the surface access check (empty/absent = any
/// authenticated user, `AppConfig::user_has_access` semantics); `publish_burst`
/// / `publish_per_sec` are the per-connection publish token-bucket caps
/// (absent = the `DEFAULT_SURFACE_PUBLISH_*` constants).
///
/// This defines and parses these types; boot-time resolution + cross-validation
/// (`resolve_surfaces`) is done separately.
#[derive(Debug, Clone, PartialEq)]
pub struct SurfaceConfigRaw {
    /// Globally unique slug; becomes `surface:<slug>` as the participant identity.
    /// Charset enforced at resolution: `[A-Za-z0-9._~-]+`, no `:`/`@`/`#`.
    pub slug: String,
    /// Transport rights for this surface (deny-by-default). Required — no default;
    /// the operator states intent explicitly, exactly like `[[wasm_consumer]]`.
    pub grants: Vec<AttachGrant>,
    /// Durable (`brenn:`) subscribe ACL — bare channel names, no scheme.
    pub subscribe_acl: Vec<crate::access::raw::ChannelMatcherRaw>,
    /// Durable (`brenn:`) publish ACL — bare channel names, no scheme.
    pub publish_acl: Vec<crate::access::raw::ChannelMatcherRaw>,
    /// Ephemeral (`ephemeral:`) subscribe ACL — bare channel names, no scheme.
    pub ephemeral_subscribe_acl: Vec<crate::access::raw::ChannelMatcherRaw>,
    /// Ephemeral (`ephemeral:`) publish ACL — bare channel names, no scheme.
    pub ephemeral_publish_acl: Vec<crate::access::raw::ChannelMatcherRaw>,
    /// Component modules to mount on this surface (`[[surface.component]]`).
    pub components: Vec<SurfaceComponentRaw>,
    /// Static channel→port input bindings (`[[surface.subscription]]`).
    pub subscriptions: Vec<SurfaceSubscriptionRaw>,
    /// Static port→channel output bindings (`[[surface.output]]`).
    pub outputs: Vec<SurfaceOutputRaw>,
    /// Combined input+output port declarations (`[[surface.io_port]]`) — the
    /// self-loop, made structural. See [`SurfaceIoPortRaw`].
    pub io_ports: Vec<SurfaceIoPortRaw>,
    /// Skin (CSS pack + vendored fonts) this surface wears. Absent ⇒ `"bench"`.
    /// Validated at resolution against the compiled-in skin registry; an unknown
    /// name is a boot panic.
    pub skin: Option<String>,
    /// Usernames permitted to attach. Empty/absent = any authenticated user
    /// (mirrors `AppConfig::user_has_access`). Resolution rejects empty strings
    /// and duplicates.
    pub allowed_users: Vec<String>,
    /// Per-connection publish burst (tokens). Absent =
    /// `DEFAULT_SURFACE_PUBLISH_BURST`. Resolution rejects `0` and any value
    /// above the bus per-sender burst (`EPHEMERAL_SENDER_BURST`): the
    /// per-connection bucket must trip no later than the bus gate, so the
    /// documented "connection bucket trips first" layering cannot invert. That
    /// layering is per-connection only — all sessions of a surface share the one
    /// `surface:<slug>` bus participant and its single gate (shared-fate).
    pub publish_burst: Option<u32>,
    /// Per-connection sustained publish refill (tokens/sec). Absent =
    /// `DEFAULT_SURFACE_PUBLISH_PER_SEC`. Resolution rejects `0` and any value
    /// above the bus per-sender refill (`EPHEMERAL_SENDER_REFILL_AMOUNT`/s), for
    /// the same layering reason as `publish_burst`.
    pub publish_per_sec: Option<u32>,
}

/// A component module to mount on a surface (`[[surface.component]]`).
#[derive(Debug, Clone, PartialEq)]
pub struct SurfaceComponentRaw {
    /// The component kind that names the artifact the page instantiates. Must
    /// match `^[a-z0-9][a-z0-9-]*$` — the kind is a directory name and a URL
    /// path segment under `processor/<kind>/`, and the stem of the kind's
    /// documentation sidecars. Several instances may share one kind (one
    /// transpiled tree, N instances).
    pub kind: String,
    /// Instance id: the routing/mount key that bindings reference. Absent ⇒
    /// defaults to `kind` (single-instance ergonomics). Must match the same
    /// charset as `kind` and be unique within the surface (enforced at
    /// resolution).
    pub instance: Option<String>,
    /// Lowercase hex SHA-256 of the spec file this instance's class was
    /// declared in. Not a body key — the class's fact, carried through lowering
    /// so boot can bind it to the spec packaged in the dist tree.
    pub spec_sha256: String,
    /// Every port name this instance's class declares with direction `out` or
    /// `io`, sorted and duplicate-free — the complete vocabulary of names the
    /// component may legally publish to. Not a body key: the class's fact,
    /// carried through lowering so the kernel can tell a declared-but-unwired
    /// port (publish drops) from a name the specification never declared (the
    /// activation traps).
    pub declared_out_ports: Vec<String>,
    /// Override for this instance's durable send-budget burst: how many
    /// publishes it may make back-to-back before the refill rate binds. Absent ⇒
    /// [`SURFACE_SEND_BURST`].
    pub send_burst: Option<u32>,
    /// Override for this instance's durable send-budget refill interval, in
    /// seconds: one publish's worth of budget returns per interval. Absent ⇒
    /// [`SURFACE_SEND_REFILL`].
    pub send_refill_secs: Option<u64>,
    /// The capability interfaces this instance is given, deny-by-default. Its
    /// own, not its surface's: a surface's grants are the transport rights the
    /// backend admits it over the wire, and these contain one component within
    /// the page it runs in.
    pub grants: Vec<ComponentGrant>,
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
    pub parked_batch_depth: Option<Depth>,
    /// Marks this instance as the surface's chrome component: the singleton that
    /// owns layout/theme/takeover/banner/toast rendering and that the kernel
    /// treats specially (connect-indicator handoff, death-is-fatal). Exactly one
    /// component per surface must set it: resolution panics (naming the surface
    /// and the offending count) on zero or two-or-more, so the singleton
    /// invariant is enforced at boot.
    /// Default false keeps the flag opt-in and out-of-tree-chrome first-class —
    /// the designation is this flag, not the kind string.
    pub chrome: bool,
    /// Static key/value configuration handed to this instance, read through its
    /// `config` capability. Absent ⇒ empty map.
    ///
    /// Readable only when the instance holds the `config` grant — resolution
    /// requires both a map and the grant; either alone is dead config. Keys must
    /// not start with `brenn.`, which is the host-reserved namespace.
    ///
    /// **Confidentiality:** this map is carried in the surface's bindings
    /// document, a retained message on the surface's ephemeral config channel.
    /// It is therefore readable by every authenticated page session of the
    /// surface *and* by any principal the operator grants an ephemeral-subscribe
    /// matcher covering that channel — read access is ordinary deny-by-default
    /// ACL policy, with no further guard. It is operator configuration, not a
    /// secret store — never place credentials or secrets in it.
    pub config: Option<BTreeMap<String, String>>,
}

#[cfg(any(test, feature = "testutils"))]
impl SurfaceComponentRaw {
    /// Minimal raw component of `kind`, with a defaulted instance id, no
    /// grants, no overrides and an empty specification hash. Shared across this
    /// crate's test modules and the boot crates above it so a new field on this
    /// struct lands in one place instead of every hand-written literal; a test
    /// that is *about* a field overrides that one field and nothing else.
    pub fn minimal(kind: &str) -> Self {
        SurfaceComponentRaw {
            kind: kind.to_string(),
            instance: None,
            spec_sha256: String::new(),
            declared_out_ports: vec![],
            send_burst: None,
            send_refill_secs: None,
            grants: vec![],
            parked_batch_depth: None,
            chrome: false,
            config: None,
        }
    }
}

/// Give a fixture the declared out-port vocabulary its own bindings imply,
/// unless it states one itself.
///
/// The one statement of the rule every fixture fold in the tree rides on.
/// Resolution refuses a bound output that is not in the declared vocabulary,
/// and the ordinary case is a class whose declared out ports are exactly the
/// ones its instance binds — so this is how a fixture with nothing to say about
/// the vocabulary says it. The escape is the load-bearing half: a fixture whose
/// subject *is* the vocabulary — an unwired optional port, a bound port no
/// class declares — states the field and is left untouched, which is the only
/// reason those fixtures test what their names say.
///
/// `ports` is the fixture's own outbound bindings for the component in
/// question, in any order and with repeats allowed.
#[cfg(any(test, feature = "testutils"))]
pub fn imply_out_port_vocabulary<'a>(
    declared: &mut Vec<String>,
    ports: impl Iterator<Item = &'a str>,
) {
    if !declared.is_empty() {
        return;
    }
    let mut implied: Vec<String> = ports.map(str::to_string).collect();
    implied.sort();
    implied.dedup();
    *declared = implied;
}

#[cfg(any(test, feature = "testutils"))]
impl SurfaceConfigRaw {
    /// The same surface, each of its components carrying the outbound
    /// vocabulary the surface's own bindings imply for it — every
    /// `[[surface.output]]` and `[[surface.io_port]]` naming that instance.
    ///
    /// The per-component counterpart of
    /// [`WasmConsumerConfigRaw::implying_its_vocabulary`], over the same rule in
    /// [`imply_out_port_vocabulary`].
    pub fn implying_component_vocabularies(mut self) -> Self {
        let outputs = &self.outputs;
        let io_ports = &self.io_ports;
        for component in &mut self.components {
            let instance = component.instance.as_deref().unwrap_or(&component.kind);
            let ports = outputs
                .iter()
                .filter(|out| out.instance == instance)
                .map(|out| out.port.as_str())
                .chain(
                    io_ports
                        .iter()
                        .filter(|io| io.instance == instance)
                        .map(|io| io.port.as_str()),
                );
            let ports: Vec<&str> = ports.collect();
            imply_out_port_vocabulary(&mut component.declared_out_ports, ports.into_iter());
        }
        self
    }
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

/// Equal to `brenn_budget::MAX_PUBLISHES_PER_ACTIVATION`, so a full bucket
/// admits exactly one maximal conforming activation flush. That constraint — not
/// the number — is the contract: this bucket is a backstop drawn in
/// whole-publish units against a flush's entries, and a backstop sized below the
/// flush it backstops would refuse truthful traffic. Boot asserts it (see
/// `resolve_send_budget` in the server's surface bootstrap). Sustained
/// throughput is governed by [`SURFACE_SEND_REFILL`], which is the knob that
/// means "rate".
pub const SURFACE_SEND_BURST: u32 = 256;

/// The default's half of the sizing invariant, at compile time.
///
/// Boot asserts every *resolved* burst, which covers this one too — but the
/// default is the value every surface gets without stating anything, including
/// the kernel grain, which has no override knob to state. A default that
/// violates the invariant should not compile, let alone reach a boot.
const _: () = assert!(
    SURFACE_SEND_BURST as usize >= brenn_budget::MAX_PUBLISHES_PER_ACTIVATION,
    "SURFACE_SEND_BURST must cover a maximal conforming activation flush \
     (MAX_PUBLISHES_PER_ACTIVATION)"
);

/// One durable-send token refilled per this interval, per surface principal
/// (steady-state 4/min) — far above any legitimate sustained rate while
/// bounding an attacker.
///
/// The surface's bare identity (no `[[surface.component]]` override) runs at
/// this rate. An operator sizing `status_interval_secs` is therefore sizing
/// against this refill; a cadence faster than it outruns the budget once the
/// burst is spent.
pub const SURFACE_SEND_REFILL: Duration = Duration::from_secs(15);

/// One attach principal's durable send budget: a burst capacity that refills one
/// publish per interval.
///
/// The knob an operator tunes per `[[surface.component]]`, and the parameters
/// boot hands the principal's token bucket. Every attach-route principal gets
/// one — a surface's kernel grain, each of its declared component instances, and
/// each `[[remote]]`, which takes the default. The default is the pair of
/// constants this replaces at the finer grain ([`SURFACE_SEND_BURST`] /
/// [`SURFACE_SEND_REFILL`]), which were sized for one surface's whole
/// traffic and now bound one principal of it.
///
/// This is deliberately *not* the backend's `WasmSinkBudget` shape. That budget
/// is per-sink millitokens filled per activation, with input-amplification
/// grants; an attacher's is a flat wall-clock refill, because the server does not
/// run the activations it would meter. What both hostings preserve is the
/// property — blast-radius scoping to one principal — not the mechanism.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AttachSendBudget {
    /// Bucket capacity in publishes; the bucket starts full.
    pub burst: u32,
    /// Wall-clock interval per one publish of refill. Never zero.
    pub refill: std::time::Duration,
}

impl Default for AttachSendBudget {
    fn default() -> Self {
        Self {
            burst: SURFACE_SEND_BURST,
            refill: SURFACE_SEND_REFILL,
        }
    }
}

/// One attacher's principals, each with its resolved send budget: `None` is the
/// attacher's own bare grain, `Some(instance)` a declared component instance. A
/// remote has only the bare grain; a surface has both.
///
/// Produced by [`ResolvedSurface::principal_send_budgets`] and folded into the
/// flat, principal-keyed list `Messenger::with_attach_send_budgets` installs.
/// Named because the shape travels between boot and the installer and appears in
/// every fixture that stands one up — one name means a reader learns the pair
/// grain once.
pub type AttachPrincipalBudgets = Vec<(Option<String>, AttachSendBudget)>;

/// A static input binding on a surface (`[[surface.subscription]]`).
///
/// `channel` is a **full scheme-qualified address** (`ephemeral:protobar-demo`,
/// `brenn:alerts.high`) — the scheme selects the delivery class, unlike the
/// bare-name ACL matcher values.
#[derive(Debug, Clone, PartialEq)]
pub struct SurfaceSubscriptionRaw {
    /// Full scheme-qualified channel address to subscribe.
    ///
    /// `None` makes this a **free port**: exactly one `link` must bind it to
    /// supply the channel. Bound by no link, it is dead config and a boot
    /// panic.
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
#[derive(Debug, Clone, PartialEq)]
pub struct SurfaceOutputRaw {
    /// Declared component instance publishing on this binding.
    pub instance: String,
    /// Logical output port name the component publishes to.
    pub port: String,
    /// Full scheme-qualified channel address the port publishes onto.
    ///
    /// `None` makes this a **free port**, bound by exactly one `link`; see
    /// [`SurfaceSubscriptionRaw::channel`].
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
    pub publish_per_activation: Option<f64>,
    /// Max tokens carried over between activations for this output sink (the
    /// bucket capacity clamp applied at the *start* of the next activation).
    /// `None` ⇒ [`DEFAULT_WASM_PUBLISH_CAPACITY`] (1.0). Must be finite and
    /// `>= 0` when present. Same knob as `[[wasm_consumer.output]]`'s.
    pub publish_capacity: Option<f64>,
}

/// A combined input+output port on a surface-hosted component
/// (`[[surface.io_port]]`).
///
/// The surface twin of [`WasmConsumerIoPortRaw`]: one port name resolving to one
/// input binding and one output binding on the *same* channel, so a component
/// sees its own publishes there by construction.
///
/// The default (no `channel`, no `link`) is a **page-local** channel —
/// per-session, browser-side, never on the server ring — riding the same
/// declared-by-bindings `local:` machinery as any `local:` surface binding.
///
/// The timer idiom runs on these: a component parks its next tick on its own
/// io_port with a deferred publish, and the tick arrives as an ordinary message
/// on the port's input half. The activation carries the instant to compute the
/// release time from, and the standing tick can be cancelled or edited from a
/// later activation.
#[derive(Debug, Clone, PartialEq)]
pub struct SurfaceIoPortRaw {
    /// Declared component instance owning both halves of this port.
    pub instance: String,
    /// Logical port name presented to the component for both directions. Must be
    /// distinct from every other port name the instance declares in either
    /// direction.
    pub port: String,
    /// Absent ⇒ the port's own anonymous page-local channel. Present ⇒ a named
    /// auto channel at this full scheme-qualified address. Must be absent when a
    /// `link` binds this port.
    ///
    /// A `local:` name here stays page-local: other bindings *on this surface*
    /// join the one page ring, and nothing on the server can. Reaching the
    /// backend takes `ephemeral:` (shared across this surface's sessions) or
    /// `brenn:` (durable as well).
    pub channel: Option<String>,
    /// Queue depth for the input half. Required, in either realm: the port
    /// queue lives in page memory and must resolve bounded, and in the server
    /// realm — a named `ephemeral:`/`brenn:` address, or membership in a
    /// wire-spanning `link` — it also feeds the channel's depth fold.
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
    pub publish_per_activation: Option<f64>,
    /// Max tokens carried between activations for the output half. `None` ⇒
    /// [`DEFAULT_WASM_PUBLISH_CAPACITY`].
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
    /// The installed component package the artifact is resolved from.
    pub package: String,
    /// Lowercase hex SHA-256 of the spec file this consumer's class was
    /// declared in — what boot compares against the record in that package.
    pub spec_sha256: String,
    /// The complete vocabulary of port names this consumer's class declares
    /// outbound (`out` and `io` directions). A publish to a bound name in this
    /// set delivers; a publish to an unbound name in it drops; a publish to a
    /// name outside it contradicts the specification the artifact is hash-bound
    /// to, and traps the activation.
    pub declared_out_ports: BTreeSet<String>,
    /// Granted capability interfaces for this component (deny-by-default).
    /// Determines which host functions are linked at component load time.
    pub grants: BTreeSet<ComponentGrant>,
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
/// One resolved component instance: its routing `instance` id and the component
/// `kind` that backs it. Several instances may share a kind — one compiled
/// module, N instantiations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedComponent {
    /// Routing/mount key that bindings reference. Defaults to `kind` when the
    /// config omits it.
    pub instance: String,
    /// Component module kind: the name of the wasm component the page loads.
    pub kind: String,
    /// Lowercase hex SHA-256 of the spec file this instance's class was declared
    /// in. Must match the hash in the record packaged with this kind's artifacts.
    pub spec_sha256: String,
    /// The complete vocabulary of port names this instance's class declares
    /// outbound (`out` and `io` directions). Carried to the page in the
    /// bindings document: a publish to a bound name delivers, one to an unbound
    /// declared name drops, and one to a name outside the set traps the
    /// instance.
    pub declared_out_ports: BTreeSet<String>,
    /// This instance's durable send budget: its own declared override, or the
    /// defaults. Server-side only — the page is told nothing about it, because
    /// the server is the authority and a mirrored bucket has no reader yet.
    pub send_budget: AttachSendBudget,
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
    /// `config` import. Empty unless the instance declares one.
    ///
    /// **Confidentiality:** carried in the surface's retained bindings
    /// document — operator configuration only, never secrets.
    pub config: BTreeMap<String, String>,
    /// The capability interfaces this instance is given, deny-by-default and
    /// deduplicated. Its own, not its surface's: the surface's grants are the
    /// transport rights the backend admits it over the wire, these contain one
    /// component within the page. Carried to the page in the bindings document,
    /// where the kernel is the runtime enforcer.
    pub grants: BTreeSet<ComponentGrant>,
}

#[cfg(any(test, feature = "testutils"))]
impl ResolvedComponent {
    /// Minimal resolved component: the identity triple, default budgets, no
    /// grants, no config, not chrome, and an empty specification hash. Shared
    /// across this crate's test modules and the crates above it so a new field
    /// on this struct lands in one place instead of every hand-written literal;
    /// a test that is *about* a field overrides that one field and nothing else.
    ///
    /// The empty hash is what a fixture that never runs asset validation wants —
    /// there is nothing installed for it to bind to. A fixture that does run it
    /// overrides the hash with the one its tree packages.
    pub fn minimal(instance: &str, kind: &str) -> Self {
        ResolvedComponent {
            instance: instance.to_string(),
            kind: kind.to_string(),
            spec_sha256: String::new(),
            declared_out_ports: BTreeSet::new(),
            send_budget: AttachSendBudget::default(),
            parked_batch_depth: DEFAULT_PARKED_BATCH_DEPTH,
            chrome: false,
            config: BTreeMap::new(),
            grants: BTreeSet::new(),
        }
    }
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
    ) -> impl Iterator<Item = (Option<String>, AttachSendBudget)> + '_ {
        std::iter::once((None, AttachSendBudget::default())).chain(
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
    /// per-instance send bucket ([`AttachSendBudget`]) is a separate,
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
        // Only a depth this block's role and durability require is fetched here;
        // the ones it may omit are computed from the ones it stated.
        let stated = |key: ChannelDepthKey, value: Option<Depth>, why: &str| -> Depth {
            match value {
                Some(depth) => depth,
                None if depth_required(key, ChannelBlockRole::Declaring, capabilities.durable) => {
                    panic!(
                        "config: [[channel]] {canonical:?} requires {} — {why}",
                        key.word(),
                    )
                }
                None => unreachable!(
                    "{} is not required of this block, so nothing fetches it",
                    key.word(),
                ),
            }
        };
        let push_depth = stated(
            ChannelDepthKey::PushDepth,
            ch.push_depth,
            "how many unseen messages one activation hands over is a sizing decision, not a \
             default",
        );
        let retain_depth = stated(
            ChannelDepthKey::RetainDepth,
            ch.retain_depth,
            "the retained window is sized for the outage it must survive, not defaulted",
        );
        assert!(
            standing_admitted(capabilities.durable) || ch.standing_retain_depth.is_none(),
            "config: [[channel]] {canonical:?} must not set standing_retain_depth — \
             it is the durable reaper's frontier; a non-durable channel's retention \
             is retain_depth alone",
        );
        assert!(
            sink_admitted(capabilities.durable) || ch.sink.is_none(),
            "config: [[channel]] {canonical:?} must not set sink — a non-durable \
             channel evicts from memory and has nothing to archive",
        );
        let noise = ch.noise.unwrap_or(defaults.default_noise);
        let wake_min = ch.wake_min.unwrap_or(defaults.default_wake_min);
        let send_rate = ch.send_rate.unwrap_or(defaults.default_send_rate);
        send_rate.validate(&format!("[[channel]] {canonical:?}"));

        let (standing_retain_depth, sink) = if capabilities.durable {
            (
                stated(
                    ChannelDepthKey::StandingRetainDepth,
                    ch.standing_retain_depth,
                    "it is the reaper's disk frontier and bounds what the channel keeps for \
                     subscribers that do not exist yet",
                ),
                ch.sink.unwrap_or(defaults.default_sink),
            )
        } else {
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

/// Build the tuning table from the `[[channel]]` blocks that play the tuning
/// role. Declaring blocks are skipped here and consumed by
/// [`build_channel_entries`], which skips the tuning ones — both passes classify
/// through [`channel_block_role`], so no block can fall between them.
///
/// Keys enter the table canonicalized ([`canonicalize_channel_address`]), so a block
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
                let canonical = canonicalize_channel_address(address);
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
                    ends_at_tuning_boundary(prefix),
                    "config: [[channel]] address_prefix {prefix:?} must end at a segment \
                     boundary ({}, the last of which closes an mqtt client) — a bare byte \
                     prefix would reach past the family it names",
                    tuning_boundary_list(),
                );
                let canonical = canonicalize_channel_address(prefix);
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

        let stated = |key: ChannelDepthKey, value: Option<Depth>| -> Depth {
            let required = depth_required(key, ChannelBlockRole::Tuning, TUNING_DURABILITY_IGNORED);
            match value {
                Some(depth) => depth,
                None if required => {
                    panic!(
                        "config: [[channel]] {label} requires {} — a system-minted channel \
                         has a bounded in-code default, and a block that tunes it states every \
                         depth rather than inheriting some of them",
                        key.word(),
                    )
                }
                None => unreachable!(
                    "a tuning block requires every depth, so {} has no unstated form here",
                    key.word(),
                ),
            }
        };
        let entry = SystemChannelTuningEntry {
            push_depth: stated(ChannelDepthKey::PushDepth, ch.push_depth),
            retain_depth: stated(ChannelDepthKey::RetainDepth, ch.retain_depth),
            standing_retain_depth: stated(
                ChannelDepthKey::StandingRetainDepth,
                ch.standing_retain_depth,
            ),
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
/// directory with the static (config-declared) and WASM subscribers. Each row in `rows` (loaded
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
/// the channel loader: uuid
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
    rows: &[crate::messaging::DynamicSubscriptionRow],
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
    pub kept: Vec<crate::messaging::DynamicSubscriptionRow>,
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
    use brenn_dsl::processor_needs;

    use super::*;
    use crate::config::{config_from_dsl, sole_refusal};

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
    // `Depth` deserialization
    // -----------------------------------------------------------------------

    #[test]
    fn depth_decodes_unbounded_string() {
        let d: Depth = serde_json::from_value(serde_json::json!("unbounded")).unwrap();
        assert_eq!(d, Depth::Unbounded);
    }

    /// A non-negative integer decodes to exactly that bound, zero included: a
    /// dynamic subscription's window is whatever the tool argument said, and `0`
    /// is the legal pull-only ceiling rather than a value to clamp up.
    #[test]
    fn depth_decodes_integers_to_the_same_bound() {
        let d: Depth = serde_json::from_value(serde_json::json!(5)).unwrap();
        assert_eq!(d, Depth::Bounded(5));
        let d: Depth = serde_json::from_value(serde_json::json!(0)).unwrap();
        assert_eq!(d, Depth::Bounded(0));
    }

    #[test]
    fn depth_rejects_negative_integer() {
        let err = serde_json::from_value::<Depth>(serde_json::json!(-1)).unwrap_err();
        assert!(
            err.to_string().contains("non-negative"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn depth_rejects_unknown_string() {
        let err = serde_json::from_value::<Depth>(serde_json::json!("inf")).unwrap_err();
        assert!(
            err.to_string().contains("unbounded"),
            "unexpected error: {err}"
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

    /// The retired `ephemeral_channel` kindword is a refusal, not a
    /// silently-ignored block: an operator carrying one forward is told the word
    /// is not a section rather than booting with the block dropped.
    #[test]
    fn the_retired_ephemeral_channel_kindword_is_refused() {
        let diag = sole_refusal("ephemeral_channel dev_stub { retain_depth = 1; }");
        assert!(
            diag.render().contains("ephemeral_channel"),
            "the refusal must name the retired word, got: {}",
            diag.render()
        );
    }

    /// A stray key in a `channel` body is refused: a misspelled tuning knob that
    /// parsed and did nothing would leave the operator believing the channel was
    /// sized.
    #[test]
    fn a_stray_channel_key_is_refused() {
        let diag = sole_refusal(
            r#"
channel demo at "ephemeral:protobar-demo" {
    retain_depth = 1;
    bogus = 1;
}
"#,
        );
        assert!(
            diag.render().contains("bogus"),
            "the refusal must name the stray key, got: {}",
            diag.render()
        );
    }

    /// The removed global depth defaults are refused rather than ignored: an
    /// operator who wrote a number and got silence would believe every channel
    /// was sized.
    #[test]
    fn a_removed_global_depth_default_is_refused() {
        for key in [
            "default_push_depth",
            "default_retain_depth",
            "default_standing_retain_depth",
        ] {
            let diag = sole_refusal(&format!("messaging {{ {key} = 8; }}"));
            assert!(
                diag.render().contains(key),
                "the refusal must name the offending key; got: {}",
                diag.render()
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
    // `surface` refusals: keys and words the vocabulary must not accept
    //
    // Every-key and minimal lowering rows for `surface`, `new`, and their
    // bindings live in the lowering suite (`config/tests/dsl_lower.rs`); what
    // stays here is the refusal side, where the raw structs' shape is the
    // subject.
    // -----------------------------------------------------------------------

    /// `grants` is required on a surface: omitting it would be a silent
    /// zero-grant surface rather than a stated deny-by-default posture.
    #[test]
    fn a_surface_without_grants_is_refused() {
        let diag = sole_refusal("surface bare { slug = \"bare\"; }");
        assert!(
            diag.render().contains("grants"),
            "the refusal must name the missing key, got: {}",
            diag.render()
        );
    }

    /// An unknown grant word is refused with the legal set, not silently
    /// dropped into a narrower posture than the operator wrote.
    #[test]
    fn an_unknown_surface_grant_word_is_refused() {
        let diag = sole_refusal("surface bare { grants = [not_a_grant]; }");
        assert!(
            diag.render().contains("not_a_grant"),
            "the refusal must name the bad word, got: {}",
            diag.render()
        );
    }

    /// A stray top-level surface key is refused: a misspelled ACL key would
    /// otherwise be a silent deny-by-default over-narrowing.
    #[test]
    fn a_stray_surface_key_is_refused() {
        let diag = sole_refusal("surface bare { grants = []; bogus = 1; }");
        assert!(
            diag.render().contains("bogus"),
            "the refusal must name the stray key, got: {}",
            diag.render()
        );
    }

    /// `abi` is required on a component class.
    ///
    /// A defaulted `abi` would silently pick an artifact shape for the operator;
    /// the field states which toolchain built the module, and no one but the
    /// operator knows that. Pinned because "just default it to dom" is the
    /// obvious ergonomic temptation.
    #[test]
    fn a_component_class_without_an_abi_is_refused() {
        let diag = sole_refusal("component Panel { in messages; }");
        assert!(
            diag.render().contains("abi"),
            "the refusal must name the missing key, got: {}",
            diag.render()
        );
    }

    /// `wake_min` has no referent on a WASM binding — the consumer loop is
    /// eager — so stating one is refused rather than accepted and ignored.
    #[test]
    fn a_wake_min_on_a_consumer_io_binding_is_refused() {
        let diag = sole_refusal(concat!(
            r#"
// ── packaged ──
component Router {
    "#,
            processor_needs!("ports"),
            r#"
    io tick;
}
// ── packaged ──

new router: Router {
    grants = [ports];
    io tick { push_depth = 1; retain_depth = 2; wake_min = normal; }
}
"#
        ));
        assert!(
            diag.render().contains("wake_min"),
            "the refusal must name the key with no referent, got: {}",
            diag.render()
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
    // Depth semantics
    // -----------------------------------------------------------------------

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

    /// `sender` is not a messaging key: a participant's identity is its app
    /// slug, not a string it names for itself. A document carrying the word is
    /// refused rather than lowered with the identity quietly ignored.
    ///
    /// Per-app messaging has no block of its own in the agent vocabulary — the
    /// subscriptions and the send budget are stated on the agent — so the word's
    /// absence is stated where it could otherwise land: the global `messaging`
    /// section, and an agent attr.
    #[test]
    fn the_removed_sender_key_is_refused() {
        let section = sole_refusal("messaging { sender = \"my-app\"; }").render();
        assert!(
            section.contains("sender"),
            "the refusal names the removed word: {section}"
        );

        let attr = sole_refusal(
            r#"
agent Finance() { sender = "my-app"; }

new pfin: Finance();
"#,
        )
        .render();
        assert!(
            attr.contains("sender"),
            "the refusal names the removed word: {attr}"
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
    // `new` (WASM consumer) grant words and ACL refusals
    //
    // The every-key lowering row for a consumer, its bindings and its nine ACL
    // families lives in the lowering suite; what stays here is the grant
    // vocabulary and the refusals whose subject is the raw struct's shape.
    // -----------------------------------------------------------------------

    /// Every `ComponentGrant` word a consumer may state lowers to its variant.
    ///
    /// `alert` is the reason this is not folded into the lowering suite's
    /// every-key row: that row states the five grants a router wants, and this
    /// one states all six the enum has today. It is not a gate on the enum: the
    /// words are hardcoded here, so a variant added without a DSL spelling fails
    /// nothing. That parity is held by review, per `dsl-vocabulary-config-parity`.
    #[test]
    fn every_consumer_grant_word_lowers_to_its_variant() {
        let config = config_from_dsl(concat!(
            r#"
mqtt_client broker { url = "mqtts://broker.example.com:8883"; }

channel digests at "brenn:alice-digests" {
    push_depth = 2;
    retain_depth = 32;
    standing_retain_depth = 32;
}

// ── packaged ──
component Router {
    "#,
            processor_needs!("ports, store, log, alert, config, mqtt"),
            r#"
    out digest;
}
// ── packaged ──

new router: Router {
    grants = [ports, store, log, alert, config, mqtt];
    store_path = "/state/router.db";

    acl publish [exact digests, client "mqtt:broker"];

    out digest -> digests {}
}
"#
        ));
        assert_eq!(
            config.wasm_consumers[0].grants,
            vec![
                ComponentGrant::Ports,
                ComponentGrant::Store,
                ComponentGrant::Log,
                ComponentGrant::Alert,
                ComponentGrant::Config,
                ComponentGrant::Mqtt,
            ]
        );
    }

    /// An unknown grant word is refused, not dropped: a typo would otherwise
    /// leave the component running with less authority than the operator wrote,
    /// which fails at some later runtime call instead of at load.
    #[test]
    fn an_unknown_consumer_grant_word_is_refused() {
        let diag = sole_refusal(concat!(
            r#"
// ── packaged ──
component Router { "#,
            processor_needs!(""),
            r#" io tick; }
// ── packaged ──

new router: Router { grants = [not_a_real_grant]; io tick {} }
"#
        ));
        assert!(
            diag.render().contains("not_a_real_grant"),
            "the refusal must name the bad word, got: {}",
            diag.render()
        );
    }

    /// An empty grant list lowers: a zero-capability consumer is a legal, if
    /// degenerate, thing to declare.
    #[test]
    fn a_consumer_with_no_grants_lowers_with_an_empty_list() {
        let config = config_from_dsl(concat!(
            r#"
channel feed at "ephemeral:alice-feed" { push_depth = 4; retain_depth = 8; }

// ── packaged ──
component Router { "#,
            processor_needs!(""),
            r#" in inbound; }
// ── packaged ──

new router: Router {
    grants = [];

    in inbound <- feed;
}
"#
        ));
        assert!(config.wasm_consumers[0].grants.is_empty());
        assert!(config.wasm_consumers[0].subscribe_acl.is_empty());
        assert!(config.wasm_consumers[0].publish_acl.is_empty());
        assert!(config.wasm_consumers[0].mqtt_subscribe_acl.is_empty());
        assert!(config.wasm_consumers[0].webhook_acl.is_empty());
    }

    /// A stray top-level consumer key is refused: a misspelled ACL statement or
    /// knob would otherwise be a silent over-grant or a silent no-op.
    #[test]
    fn a_stray_consumer_key_is_refused() {
        let diag = sole_refusal(concat!(
            r#"
// ── packaged ──
component Router { "#,
            processor_needs!(""),
            r#" io tick; }
// ── packaged ──

new router: Router { grants = []; subscribe_acls = []; io tick {} }
"#
        ));
        assert!(
            diag.render().contains("subscribe_acls"),
            "the refusal must name the stray key, got: {}",
            diag.render()
        );
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
    ) -> crate::messaging::DynamicSubscriptionRow {
        crate::messaging::DynamicSubscriptionRow {
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
        rows: &[crate::messaging::DynamicSubscriptionRow],
        policy: &crate::access::AppPolicy,
    ) -> DynamicMergeOutcome {
        merge_dynamic_subscriptions(dir, rows, &HashMap::new(), &|_| Some(policy))
    }

    /// As `merge_with_policy` but with a skip report naming channels whose rows
    /// exist while no block declares them.
    fn merge_with_skipped(
        dir: &MessagingDirectory,
        rows: &[crate::messaging::DynamicSubscriptionRow],
        skipped: &HashMap<Uuid, String>,
        policy: &crate::access::AppPolicy,
    ) -> DynamicMergeOutcome {
        merge_dynamic_subscriptions(dir, rows, skipped, &|_| Some(policy))
    }

    /// The dormant registration the merge reports for `row` when its channel
    /// sits at `address`.
    fn dormant_of(
        row: &crate::messaging::DynamicSubscriptionRow,
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
        rows: &[crate::messaging::DynamicSubscriptionRow],
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
        use crate::access::AppPolicy;
        use brenn_envelope::grants::AppCapability;

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
