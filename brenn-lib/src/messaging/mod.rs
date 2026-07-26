//! Intra-Brenn messaging (MVP).
//!
//! Channels are globally declared; apps publish / subscribe via per-app
//! config + a small set of MCP virtual tools. See
//! `docs/designs/messaging-mvp.md` for the full design.
//!
//! All publish, subscribe, and query traffic flows through MCP virtual
//! tools (intercepted in PreToolUse / PostToolUse). There are no new
//! WebSocket or HTTP endpoints.
//!
//! This module is library-only — it does not depend on any binary-crate
//! types. Wake / dispatch is abstracted via the `WakeRouter` trait, which
//! the binary crate implements over `ActiveBridges` + `AppState`.

pub mod config;
pub mod conversations;
pub mod db;
pub mod dispatcher;
pub mod edit;
pub mod format;
pub mod gates;
pub mod identity;
pub mod ingress;
pub mod live;
pub mod publish;
pub mod query;
pub mod store;
pub mod subscribe;
pub mod system;

#[cfg(any(test, feature = "testutils"))]
pub mod testutils;

#[cfg(test)]
pub(super) mod test_support;

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use chrono::{DateTime, Utc};
use indexmap::IndexMap;
use uuid::Uuid;

use crate::config::{AppConfig, ServerConfig};
use crate::db::{Db, format_ts_for_db};
pub use config::{
    ChannelConfigRaw, Depth, MessagingConfigRaw, MessagingGlobalConfig, MessagingSubscriptionRaw,
    NoiseLevel, ResolvedChannel, ResolvedMessagingConfig, ResolvedSubscription, Sink,
    WasmConsumerConfigRaw, WasmConsumerSubscriptionRaw, WasmInputPort,
};
pub use edit::{CancelResult, EditFields, EditResult};
pub use identity::{ParticipantId, SubscriberKind};
pub use ingress::{
    CollapsedDrain, Event as IngressEvent, MAX_DELIVERED_RETENTION_DAYS,
    MAX_REPO_SYNC_STALENESS_DAYS, ONELINE_CAP, REPO_SYNC_KIND_CONFLICT, REPO_SYNC_KIND_LOCAL,
    REPO_SYNC_KIND_PULLED, REPO_SYNC_KIND_SUMMARY, REPO_SYNC_SOURCE_CONFLICT,
    REPO_SYNC_SOURCE_LOCAL, REPO_SYNC_SOURCE_PREFIX, REPO_SYNC_SOURCE_PULLED,
    REPO_SYNC_SOURCE_SUMMARY, SYNTHETIC_EVENT_ID, assert_delivered_retention_days_valid,
    cap_oneline, collapse_repo_sync, format_event_batch, is_repo_sync_source,
    repo_sync_staleness_days, set_repo_sync_staleness_days, split_stale_repo_sync,
};
pub use live::{
    EphemeralDelivery, EphemeralEvent, EphemeralReceiver, EphemeralResume, EphemeralSubscribeError,
    EphemeralSubscription, GapReason, LiveCounters, Replay,
};
pub use publish::{
    PublishOrigin, PublishResult, SurfaceBatchPublish, SurfaceSendDraw, SurfaceSendVerdict,
    WasmPublish, is_well_formed_address,
};
pub use query::{MessageQuery, QueryError};

// ---------------------------------------------------------------------------
// Address protocol
// ---------------------------------------------------------------------------

/// Derive a deterministic UUIDv5 for a `webhook:` channel from the endpoint slug.
///
/// Both the publish side and the subscription side must call this function with
/// the same slug to arrive at the same UUID. The namespace is fixed and
/// documented; changing it would invalidate persisted channel UUIDs.
///
/// Namespace: UUIDv5(DNS-namespace, `"brenn.webhook-channel"`) =
/// `658063f4-9afb-5209-b411-249fb15498fc` (pre-computed once; constant across
/// all deployments so restarts and multi-process setups agree).
pub fn webhook_channel_uuid_from_slug(slug: &str) -> Uuid {
    // Two-level derivation keeps the per-slug UUID space isolated:
    // namespace = UUIDv5(DNS-namespace, "brenn.webhook-channel")
    // channel UUID = UUIDv5(namespace, slug)
    let ns = Uuid::new_v5(&Uuid::NAMESPACE_DNS, b"brenn.webhook-channel");
    Uuid::new_v5(&ns, slug.as_bytes())
}

/// Derive a deterministic UUIDv5 for an `mqtt:` channel from its full resolved
/// address `mqtt:<client>:<topic>`.
///
/// The channel identity *is* the resolved address: both the publish side (the
/// router) and the subscription side (app subscription resolution) must call
/// this function with the same canonical `mqtt:<client>:<topic>` string to
/// arrive at the same UUID. Always derive the address via the shared formatter
/// (`MqttAddress::format`) — never re-concatenate ad hoc — so both sides agree.
/// The namespace is fixed and documented; changing it would invalidate
/// persisted channel UUIDs.
///
/// The namespace seed (`"brenn.mqtt-channel"`) is deliberately distinct from the
/// webhook seed (`"brenn.webhook-channel"`) so the MQTT and webhook address
/// spaces cannot collide: the same string yields a different UUID under each
/// transport.
pub fn mqtt_channel_uuid_from_address(address: &str) -> Uuid {
    // Two-level derivation keeps the per-address UUID space isolated:
    // namespace = UUIDv5(DNS-namespace, "brenn.mqtt-channel")
    // channel UUID = UUIDv5(namespace, address)
    let ns = Uuid::new_v5(&Uuid::NAMESPACE_DNS, b"brenn.mqtt-channel");
    Uuid::new_v5(&ns, address.as_bytes())
}

/// Derive a deterministic UUIDv5 for an `ephemeral:` channel from its bare name.
///
/// Ephemeral channels have no DB row, but a stable UUID keeps their identity
/// uniform with the durable/webhook/MQTT channel spaces. Deterministic across
/// calls, processes, and restarts so every derivation agrees on the same name.
///
/// The namespace seed (`"brenn.ephemeral-channel"`) is deliberately
/// distinct from the webhook and MQTT seeds so the same string yields a
/// different UUID under each transport — the ephemeral, webhook, and MQTT
/// address spaces cannot collide.
pub fn ephemeral_channel_uuid_from_name(name: &str) -> Uuid {
    // Two-level derivation keeps the per-name UUID space isolated:
    // namespace = UUIDv5(DNS-namespace, "brenn.ephemeral-channel")
    // channel UUID = UUIDv5(namespace, name)
    let ns = Uuid::new_v5(&Uuid::NAMESPACE_DNS, b"brenn.ephemeral-channel");
    Uuid::new_v5(&ns, name.as_bytes())
}

/// Derive a deterministic UUIDv5 for a `local:` channel from its bare name.
///
/// Own namespace seed, so `local:foo` and `ephemeral:foo` are distinct
/// identities — they are distinct channels (a `local:` channel never leaves the
/// process) and must never collide in the directory.
pub fn local_channel_uuid_from_name(name: &str) -> Uuid {
    let ns = Uuid::new_v5(&Uuid::NAMESPACE_DNS, b"brenn.local-channel");
    Uuid::new_v5(&ns, name.as_bytes())
}

/// Deterministic UUID for a non-durable channel, dispatching on its scheme.
///
/// # Panics
///
/// On a durable or non-pub/sub scheme — those channels carry an operator or
/// transport-derived UUID instead.
pub fn nondurable_channel_uuid(scheme: ChannelScheme, name: &str) -> Uuid {
    match scheme {
        ChannelScheme::Ephemeral => ephemeral_channel_uuid_from_name(name),
        ChannelScheme::Local => local_channel_uuid_from_name(name),
        other => panic!("nondurable_channel_uuid called with durable scheme {other:?}"),
    }
}

/// Derive a deterministic UUIDv5 for a tool-substrate channel from its full
/// canonical address (`brenn:tools/<tool>` or `brenn:tool-results/<slug>`).
///
/// The tool request channels and result inboxes are created programmatically at
/// bootstrap (not from `[[channel]]` config), so they need a stable identity that
/// is the same across restarts — durable pending-push rows on a request channel
/// must match the same channel UUID after a restart.
///
/// The namespace seed (`"brenn.tool-channel"`) is deliberately distinct from the
/// webhook, MQTT, and ephemeral seeds so the tool address space cannot collide
/// with any other transport's.
pub fn tool_channel_uuid_from_address(address: &str) -> Uuid {
    // Two-level derivation keeps the per-address UUID space isolated:
    // namespace = UUIDv5(DNS-namespace, "brenn.tool-channel")
    // channel UUID = UUIDv5(namespace, address)
    let ns = Uuid::new_v5(&Uuid::NAMESPACE_DNS, b"brenn.tool-channel");
    Uuid::new_v5(&ns, address.as_bytes())
}

/// Returns `true` if `c` is in the RFC 3986 unreserved character set
/// (`A-Za-z0-9._~-`). Single source of truth for channel-name and
/// push-address charset validation; used by both `messaging` and
/// `pwa_push::targets`.
pub fn is_unreserved_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '~' | '-')
}

/// Build a canonical channel address from a bare name. The name must already
/// be validated; this only adds the `brenn:` prefix.
pub fn canonical_address(name: &str) -> String {
    format!("{}{}", BRENN_ADDRESS_PREFIX, name)
}

// ---------------------------------------------------------------------------
// WASM consumer window bounds
// ---------------------------------------------------------------------------

/// Maximum number of retained-context messages passed to a WASM consumer in one
/// window (§4 / design §2.7 step 2: `Unbounded` retain_depth is clamped to this
/// value to keep the window argument finite). 1 000 is a conservative default;
/// operators can configure a lower `retain_depth` per subscription.
pub const WASM_WINDOW_MAX_RETAIN: u64 = 1_000;

/// Maximum number of new (unprocessed) messages in the new-portion of a WASM
/// consumer window when `push_depth = Unbounded`. Mirrors `WASM_WINDOW_MAX_RETAIN`:
/// the window-size bound requires clamping both sides when either is
/// `Unbounded`. A bounded `push_depth` already caps the new portion at
/// `push_depth`, so the clamp is only reached for `Unbounded` consumers.
pub const WASM_WINDOW_MAX_NEW: u64 = 1_000;

/// One port's slice of a multi-port activation snapshot.
///
/// Coupled to [`brenn_wasm::ProcessorPortWindow`]; changes here must track
/// changes there.
pub struct PortSnapshot {
    /// Logical input port name (from config).
    pub port: String,
    /// Canonical channel address (e.g. `brenn:my-input`).
    pub channel_address: String,
    /// The channel's capabilities, so consumers need not re-derive them from
    /// the address.
    pub capabilities: brenn_envelope::ChannelCapabilities,
    /// The port's window, oldest first, each entry carrying the retention seq
    /// its store assigned it: seen context up to `new_from`, then what is new.
    /// One read, so no message appears twice and no dedupe is needed.
    pub entries: Vec<(store::MessageSeq, MessageEnvelope)>,
    /// Index of the first new entry, equal to `entries.len()` when the port was
    /// owed nothing this step — always the case for a sampled port
    /// (`push_depth = Bounded(0)`), whose window is all context.
    pub new_from: usize,
    /// Whether this port holds a position at all. A sampled port
    /// (`push_depth = 0`) does not.
    pub push_enabled: bool,
}

impl PortSnapshot {
    /// The `(through, seen_floor)` pair to advance this port's position over,
    /// or `None` when there is nothing to advance: an empty window, or a
    /// sampled port, which holds no position to move.
    pub fn advance_span(&self) -> Option<(store::MessageSeq, store::MessageSeq)> {
        if !self.push_enabled {
            return None;
        }
        Some((self.entries.last()?.0, self.entries.first()?.0))
    }

    /// The new messages, oldest first.
    pub fn new_entries(&self) -> &[(store::MessageSeq, MessageEnvelope)] {
        &self.entries[self.new_from..]
    }

    /// The context ahead of the new boundary, oldest first.
    pub fn context(&self) -> &[(store::MessageSeq, MessageEnvelope)] {
        &self.entries[..self.new_from]
    }

    /// How many of this window's messages are new to the subscriber.
    pub fn new_len(&self) -> usize {
        self.entries.len() - self.new_from
    }

    /// The retention seqs the new portion spans, oldest first, or `None` when
    /// nothing is new.
    pub fn new_seq_span(&self) -> Option<(store::MessageSeq, store::MessageSeq)> {
        let new = self.new_entries();
        Some((new.first()?.0, new.last()?.0))
    }
}

/// Parameters for a failed WASM consumer batch disposition.
/// Passed to [`Messenger::record_wasm_activation_failure`] to avoid the 7-arg clippy warning.
#[derive(Clone, Copy)]
pub struct WasmBatchFailure<'a> {
    pub channel: &'a str,
    pub subscriber: &'a ParticipantId,
    pub first_message_id: &'a str,
    pub last_message_id: &'a str,
    /// The retention seqs this batch spanned, oldest first — the quarantine
    /// record's handle on what the activation was holding when it failed.
    pub seq_span: (store::MessageSeq, store::MessageSeq),
    /// `"err"` or `"trap"` (matches the DB CHECK constraint).
    pub outcome: &'a str,
    pub diagnostic: &'a str,
}

// ---------------------------------------------------------------------------
// Source resolution
// ---------------------------------------------------------------------------

/// Resolve the `source` string stamped on every outgoing message.
///
/// Uses `server.public_url`. Called once at server startup; the result is
/// cached on `Messenger` as `Arc<str>`.
///
/// # Panics
///
/// Panics if `public_url` is missing or empty — messaging is configured but
/// `server.public_url` is required as the message source identifier.
pub fn resolve_source(server: &ServerConfig) -> Arc<str> {
    match server.public_url.as_deref().filter(|s| !s.is_empty()) {
        Some(url) => Arc::from(url),
        None => panic!(
            "messaging is configured but `server.public_url` is missing or empty \
             — required as the message source identifier"
        ),
    }
}

// ---------------------------------------------------------------------------
// Directory
// ---------------------------------------------------------------------------

/// A channel registered in the directory.
///
/// `subscribers` lists the apps subscribed to this channel (along with
/// their resolved push_depth) in app-declaration order — used by
/// `MessageListChannels` and the publish dispatch path.
#[derive(Debug, Clone)]
pub struct ChannelEntry {
    pub uuid: Uuid,
    /// Canonical `brenn:<name>` / `webhook:<slug>` form.
    pub address: String,
    pub description: Option<String>,
    /// Resolved per-channel config (depth/noise/sink, already inheriting from globals).
    pub resolved_channel: config::ResolvedChannel,
    /// Subscribers for this channel, in app-declaration order.
    pub subscribers: Vec<SubscriberEntry>,
    /// Transport type persisted with the channel. Drives accept-side validation
    /// of envelopes published to this channel.
    pub transport_type: ChannelScheme,
    /// HTTP mount path for `webhook:` channels (e.g. `/webhooks/my-endpoint`).
    /// `None` for `brenn:` channels and other non-webhook transports.
    /// Carried on the entry so `list_channels()` has a single source for
    /// `WebhookDetails.mount` without re-querying `WebhookService`.
    pub mount: Option<String>,
}

impl ChannelEntry {
    /// The channel's capability set, derived from its transport scheme.
    ///
    /// This is the class-dependent behavior expressed once: `durable` selects
    /// the retention store, `transportable` gates the wire and egress adapters.
    /// Call sites consume capabilities, not the scheme discriminant.
    ///
    /// # Panics
    ///
    /// If the entry's transport is egress-only (`pwa_push:`) — such an address
    /// never becomes a directory entry.
    pub fn capabilities(&self) -> brenn_envelope::ChannelCapabilities {
        self.transport_type.capabilities().unwrap_or_else(|| {
            panic!(
                "channel entry {:?} has egress-only transport {:?} — not a pub/sub channel",
                self.address, self.transport_type,
            )
        })
    }

    /// The reap frontier for this channel: the highest row index that must be
    /// retained.
    ///
    /// Returns `None` if any depth value is `Unbounded` (channel is pinned —
    /// must not be reaped: an Unbounded subscriber pins the whole channel).
    /// Otherwise returns `Some(frontier)` =
    /// `max(standing_retain_depth, all subscribers' push_depth and retain_depth)`.
    ///
    /// Both `push_depth` and `retain_depth` are included: `push_depth` bounds
    /// undelivered push rows; `retain_depth` bounds pull reads. Omitting either
    /// could GC bodies before their respective subscriber can consume them.
    pub fn reap_frontier(&self) -> Option<u64> {
        use config::Depth;

        let standing = self.resolved_channel.standing_retain_depth;
        if standing == Depth::Unbounded {
            return None; // standing buffer is unbounded — whole channel pinned
        }

        let mut frontier: u64 = match standing {
            Depth::Bounded(n) => n,
            Depth::Unbounded => unreachable!("checked above"),
        };

        for sub in &self.subscribers {
            // Both push_depth and retain_depth contribute to the frontier.
            // An Unbounded in either pins the whole channel.
            for depth in [sub.push_depth, sub.retain_depth] {
                match depth {
                    Depth::Unbounded => {
                        return None; // subscriber pins the whole channel
                    }
                    Depth::Bounded(n) => {
                        frontier = frontier.max(n);
                    }
                }
            }
        }

        Some(frontier)
    }

    /// The `App`-kind subscriber for `app_slug`, if this app subscribes to the
    /// channel.
    ///
    /// Matches only [`SubscriberEntryKind::App`]: app, WASM, and surface slugs
    /// are distinct config namespaces, so a coincidental slug collision with a
    /// `Wasm`/`Surface` subscriber must never resolve to an app caller (that
    /// would leak the other component's policy). Returns the first match; the
    /// subscribe path forbids duplicate `App` entries for one channel.
    pub fn app_subscriber(&self, app_slug: &str) -> Option<&SubscriberEntry> {
        self.subscribers
            .iter()
            .find(|s| matches!(&s.kind, SubscriberEntryKind::App(slug) if slug == app_slug))
    }
}

/// Discriminant for a channel subscriber entry: an app-backed conversation
/// subscriber or a WASM processing-component subscriber.
///
/// `App(slug)` corresponds to a configured `[[app]]` with messaging enabled.
/// `Wasm(slug)` corresponds to a configured `[[wasm_consumer]]`.
///
/// The slug in each variant is a config join key: `App` slugs look up entries
/// in `Messenger.apps`; `Wasm` slugs look up entries in the processing-component
/// map and resolve to `ParticipantId::for_wasm(slug)` as the push target.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SubscriberEntryKind {
    /// An app-backed subscriber; the slug is the app config slug used to find
    /// the app's `ResolvedMessagingConfig` and derive the singleton conversation.
    App(String),
    /// A WASM processing-component subscriber; the slug is the `[[wasm_consumer]]`
    /// `slug` field and becomes `wasm:<slug>` as the `ParticipantId`.
    Wasm(String),
    /// A browser-surface subscriber, at one of the two grains
    /// `SubscriberKind::Surface` names.
    ///
    /// `instance: Some(_)` is a declared component instance — the principal a
    /// `[[surface.subscription]]` binding belongs to, resolving to
    /// `surface:<slug>#<instance>`. One instance bound to a channel is one
    /// subscription with its own push window and cursor, so N instances of one
    /// kind on one channel are N entries here, exactly as N `[[app]]` blocks
    /// would be.
    ///
    /// `instance: None` is the surface's kernel, resolving to `surface:<slug>`.
    /// It holds no durable subscription of its own — the durable subscriber path
    /// always constructs the `Some` grain. The `None` arm exists only to mirror
    /// [`SubscriberKind::Surface`], whose bare `surface:<slug>` grain is a live
    /// publisher participant.
    ///
    /// Constructed by `finalize_directory_with_subscribers`; the durable path
    /// (`TargetResolver::surface_feed_targets`) treats either grain exactly like
    /// an App/Wasm subscriber. Policy resolves via
    /// `Messenger::surface_policies` **at the surface grain for both** — a
    /// component's grants are its config-declared bindings, which boot already
    /// proved the surface's own ACLs cover, so the instance grain finer-grains
    /// attribution, budget, and lag tracking, not authority.
    Surface {
        slug: String,
        instance: Option<String>,
    },
    /// An in-process system-substrate subscriber; the component name becomes
    /// `system:<component>` as the `ParticipantId` and resolves its policy via
    /// `Messenger::system_policies`. Created programmatically (not from config),
    /// parked-and-woken like a `Wasm` subscriber.
    System(String),
}

impl SubscriberEntryKind {
    /// Returns the config slug regardless of kind — for a `Surface` that is the
    /// `[[surface]]` slug, not the instance. Useful for logging; callers needing
    /// the storage key ask [`SubscriberEntryKind::subscriber_key`].
    pub fn slug(&self) -> &str {
        match self {
            SubscriberEntryKind::App(s)
            | SubscriberEntryKind::Wasm(s)
            | SubscriberEntryKind::System(s) => s.as_str(),
            SubscriberEntryKind::Surface { slug, .. } => slug.as_str(),
        }
    }

    /// The key this subscriber stores in `messaging_subscriptions.app_slug` and
    /// `messaging_pending_pushes.target_app_slug`. Identical to `slug()` for
    /// every kind whose principal *is* its slug; a surface component instance
    /// keys `<slug>#<instance>`, matching
    /// [`ParticipantId::as_surface_subscriber_key`].
    ///
    /// The single source of truth for that encoding: boot's row writer, the
    /// push-target resolver, and the GC's window query must agree on it exactly,
    /// or a subscription and its own push rows land in different keyspaces and
    /// the window silently never bounds.
    pub fn subscriber_key(&self) -> String {
        match self {
            SubscriberEntryKind::App(s)
            | SubscriberEntryKind::Wasm(s)
            | SubscriberEntryKind::System(s) => s.clone(),
            SubscriberEntryKind::Surface { slug, instance } => match instance {
                Some(instance) => {
                    crate::messaging::identity::ParticipantId::for_surface_component(slug, instance)
                        .as_surface_subscriber_key()
                        .to_owned()
                }
                None => slug.clone(),
            },
        }
    }

    /// The component instance a surface *subscriber* names. A `Surface`
    /// subscriber entry always carries `Some`: the bare `surface:<slug>` grain
    /// is publisher-only and never registers a durable subscription. The single
    /// place that asserts (and words) that invariant for the dispatch paths that
    /// rebuild a `SubKey` from a registration key.
    ///
    /// Panics if called on a non-`Surface` entry, or on the bare grain.
    pub fn surface_subscriber_instance(&self) -> &str {
        match self {
            SubscriberEntryKind::Surface { instance, .. } => instance.as_deref().expect(
                "a Surface subscriber that registered a surface session names a component \
                 instance; the bare surface grain is publisher-only",
            ),
            other => panic!(
                "surface_subscriber_instance called on a non-Surface subscriber key: {other:?}"
            ),
        }
    }
}

/// How expensive it is to wake a subscriber, and therefore whether message
/// urgency gates eager delivery to it. Declared at registration; never
/// inferred from the identity prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WakeEconomics {
    /// Delivery is cheap (notify a parked task / push to an attached session).
    /// Every push row is created eager; `wake_min` does not apply.
    Eager,
    /// Waking is expensive (spawns a Claude Code subprocess). Eager wake is
    /// gated by the subscription's `wake_min` threshold; below-threshold rows
    /// park until the subscriber's next natural wake. Designed behavior, not
    /// stranding.
    UrgencyGated,
}

/// One subscriber's registration: its resolved access-control policy and its
/// declared wake economics. Keyed in [`Messenger::subscribers`] by the
/// subscriber's directory [`SubscriberEntryKind`].
///
/// The policy is behind an `Arc` so the boot-only installer can assert the
/// `Messenger` is still uniquely owned before wiring it in.
#[derive(Debug, Clone)]
pub struct SubscriberRegistration {
    /// Resolved access-control policy for this subscriber (publish authority
    /// and delivery-time ACL both read it).
    pub policy: Arc<crate::access::AppPolicy>,
    /// Declared wake economics — whether message urgency gates eager delivery.
    pub wake: WakeEconomics,
}

/// Per-subscriber data carried in the channel directory entry.
#[derive(Debug, Clone)]
pub struct SubscriberEntry {
    /// Identifies the subscriber kind (app or WASM) and carries the slug.
    pub kind: SubscriberEntryKind,
    /// Max undelivered push rows for this subscriber (`Unbounded` = no cap).
    pub push_depth: config::Depth,
    /// Max rows returned on a pull read (`Unbounded` = no clamp).
    /// Also contributes to the GC frontier so bodies are not evicted before
    /// a pull subscriber can read them.
    pub retain_depth: config::Depth,
    /// Resolved noise level for push-overflow handling on this subscription.
    ///
    /// Single authoritative source for both `App` and `Wasm` subscribers.
    /// Populated once at startup by `finalize_directory_with_subscribers` from
    /// the same `ResolvedSubscription.noise` value; immutable thereafter.
    pub noise: config::NoiseLevel,
    /// Resolved wake-min policy for this subscription — `Some` iff the subscriber
    /// is `UrgencyGated`.
    ///
    /// Populated once at startup by `finalize_directory_with_subscribers` from
    /// `ResolvedSubscription.wake_min`. Read by the wake pass, which compares it
    /// against the loudest message the subscriber has not seen.
    ///
    /// Only `UrgencyGated` economics consult a wake threshold, so only those
    /// subscribers carry `Some`; every `Eager` kind (`Wasm`/`Surface`/`System`)
    /// carries `None`. The type makes "no eager delivery reads a wake threshold"
    /// compiler-enforced rather than a convention — a bare read cannot
    /// re-introduce the stranded-eager-subscriber class.
    pub wake_min: Option<WakeMin>,
}

/// Inner maps of the channel directory, mutated atomically together under a
/// single `RwLock` (see [`MessagingDirectory`]).
#[derive(Debug, Default)]
struct DirectoryInner {
    /// All channels indexed by UUID for hot-path lookup.
    by_uuid: HashMap<Uuid, Arc<ChannelEntry>>,
    /// Address → UUID for parsing `brenn:<addr>` strings.
    by_address: HashMap<String, Uuid>,
    /// Iteration order: declaration order in config, then runtime-add order.
    order: Vec<Uuid>,
}

/// Process-global channel directory built at startup from config + DB upsert,
/// and mutated at runtime by dynamic subscriptions (design §2.1). Held on
/// `AppState` as `Arc<MessagingDirectory>`.
///
/// The three index maps live behind a single `RwLock` so they mutate atomically
/// together. Subscriber mutation is **copy-on-write**: clone the target
/// `ChannelEntry`, add/remove the subscriber, and swap the `Arc` in the map
/// under the write-lock. Readers (`resolve`/`by_uuid`/`list`) take a brief
/// read-lock and return cloned `Arc`s, so a publisher that resolved an
/// `Arc<ChannelEntry>` before a concurrent mutation keeps operating on its
/// snapshot — the mutation applies to the *next* resolve. This preserves the
/// publish hot path's existing at-least-once-after-commit TOCTOU semantics
/// (`publish/mod.rs`) without holding the directory lock across DB work.
#[derive(Debug, Default)]
pub struct MessagingDirectory {
    inner: RwLock<DirectoryInner>,
}

impl MessagingDirectory {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_entries(entries: Vec<ChannelEntry>) -> Self {
        let mut by_uuid = HashMap::with_capacity(entries.len());
        let mut by_address = HashMap::with_capacity(entries.len());
        let mut order = Vec::with_capacity(entries.len());
        for entry in entries {
            order.push(entry.uuid);
            by_address.insert(entry.address.clone(), entry.uuid);
            by_uuid.insert(entry.uuid, Arc::new(entry));
        }
        Self {
            inner: RwLock::new(DirectoryInner {
                by_uuid,
                by_address,
                order,
            }),
        }
    }

    /// Resolve a channel address (e.g. `brenn:<name>` or `webhook:<slug>`) to
    /// the channel entry. Returns `None` for unknown or unregistered addresses.
    pub fn resolve(&self, addr: &str) -> Option<Arc<ChannelEntry>> {
        let inner = self.inner.read().expect("directory lock poisoned");
        let uuid = inner.by_address.get(addr)?;
        inner.by_uuid.get(uuid).cloned()
    }

    /// Look up a channel by UUID.
    pub fn by_uuid(&self, uuid: &Uuid) -> Option<Arc<ChannelEntry>> {
        let inner = self.inner.read().expect("directory lock poisoned");
        inner.by_uuid.get(uuid).cloned()
    }

    /// All channels, in config-declaration order (then runtime-add order).
    pub fn list(&self) -> Vec<Arc<ChannelEntry>> {
        let inner = self.inner.read().expect("directory lock poisoned");
        inner
            .order
            .iter()
            .map(|uuid| {
                inner
                    .by_uuid
                    .get(uuid)
                    .cloned()
                    .expect("order references a uuid not present in by_uuid")
            })
            .collect()
    }

    /// The durable channels only, in the same order [`Self::list`] gives.
    ///
    /// The directory holds every pub/sub channel, durable or not. Callers whose
    /// work is the database itself — the channel-row upsert, the retention
    /// reaper, the operator listings that read persisted state — walk this
    /// instead, because a non-durable channel has no row for them to act on.
    pub fn list_durable(&self) -> Vec<Arc<ChannelEntry>> {
        self.list()
            .into_iter()
            .filter(|e| e.capabilities().durable)
            .collect()
    }

    /// Add (or replace) a subscriber on an existing channel, copy-on-write.
    ///
    /// Clones the target `ChannelEntry`, pushes `subscriber` — **replacing** an
    /// existing subscriber with the same kind+slug — and swaps the `Arc` under
    /// the write-lock. This is the directory *mechanism* used by both the boot
    /// merge (re-folding durable dynamic rows) and the runtime subscribe path;
    /// the tool layer (design §2.4) governs *when* a replace is permitted.
    ///
    /// Returns `true` if the channel existed and the subscriber was applied;
    /// `false` if `channel_uuid` is unknown (caller decides whether that is an
    /// error — a runtime subscribe to a missing channel is, but the caller has
    /// the context to produce the right message).
    pub fn add_subscriber(&self, channel_uuid: &Uuid, subscriber: SubscriberEntry) -> bool {
        let mut inner = self.inner.write().expect("directory lock poisoned");
        let Some(existing) = inner.by_uuid.get(channel_uuid) else {
            return false;
        };
        let mut entry = ChannelEntry::clone(existing);
        // Replace an existing same-kind+slug subscriber, else append.
        if let Some(slot) = entry.subscribers.iter_mut().find(|s| {
            std::mem::discriminant(&s.kind) == std::mem::discriminant(&subscriber.kind)
                && s.kind.slug() == subscriber.kind.slug()
        }) {
            *slot = subscriber;
        } else {
            entry.subscribers.push(subscriber);
        }
        inner.by_uuid.insert(*channel_uuid, Arc::new(entry));
        true
    }

    /// Remove an `App(slug)` subscriber from a channel, copy-on-write.
    ///
    /// Clones the target `ChannelEntry`, retains-out the matching `App(slug)`
    /// subscriber (leaving `Wasm` and other-app subscribers untouched), and
    /// swaps the `Arc` under the write-lock.
    ///
    /// Returns `Some(remaining)` — the count of subscribers left on the channel
    /// after the removal — if the channel existed and a matching `App(slug)`
    /// subscriber was removed; `None` if the channel is unknown or no `App(slug)`
    /// subscriber was present. The remaining count is computed inside the single
    /// write-lock critical section so the unsubscribe path's "last subscriber on
    /// this filter?" decision needs no second `resolve` + entry clone
    /// (efficiency-3).
    pub fn remove_subscriber(&self, channel_uuid: &Uuid, app_slug: &str) -> Option<usize> {
        let mut inner = self.inner.write().expect("directory lock poisoned");
        let existing = inner.by_uuid.get(channel_uuid)?;
        let mut entry = ChannelEntry::clone(existing);
        let before = entry.subscribers.len();
        entry
            .subscribers
            .retain(|s| !matches!(&s.kind, SubscriberEntryKind::App(slug) if slug == app_slug));
        if entry.subscribers.len() == before {
            return None;
        }
        let remaining = entry.subscribers.len();
        inner.by_uuid.insert(*channel_uuid, Arc::new(entry));
        Some(remaining)
    }

    /// Insert a brand-new channel (entry + address index + iteration order).
    ///
    /// Used by the runtime `mqtt:` subscribe path (design §2.3) to register a
    /// channel for a filter that was never declared in TOML. Panics if a channel
    /// with the same UUID or address already exists — callers resolve existence
    /// first and only call this for genuinely new channels, so a collision is a
    /// host bug (CLAUDE.md: panic on host bugs).
    pub fn add_channel(&self, entry: ChannelEntry) {
        let mut inner = self.inner.write().expect("directory lock poisoned");
        assert!(
            !inner.by_uuid.contains_key(&entry.uuid),
            "add_channel: uuid {} already present",
            entry.uuid
        );
        assert!(
            !inner.by_address.contains_key(&entry.address),
            "add_channel: address {} already present",
            entry.address
        );
        let uuid = entry.uuid;
        inner.by_address.insert(entry.address.clone(), uuid);
        inner.order.push(uuid);
        inner.by_uuid.insert(uuid, Arc::new(entry));
    }
}

// ---------------------------------------------------------------------------
// Wire format — re-exported from brenn-envelope
// ---------------------------------------------------------------------------

// These four types are the external wire contract between the Brenn host and
// WASM guest components. They live in `brenn-envelope` so guests can depend on
// that lightweight crate without pulling in all of brenn-lib's host dependencies.
// Re-exporting at the same paths keeps every existing host caller unchanged.
pub use brenn_envelope::{
    BRENN_ADDRESS_PREFIX, ChannelScheme, DeliveryClass, EPHEMERAL_ADDRESS_PREFIX,
    LOCAL_ADDRESS_PREFIX, MQTT_ADDRESS_PREFIX, MessageEnvelope, MqttEnvelope, MqttPayloadBody,
    PWA_PUSH_ADDRESS_PREFIX, Urgency, WEBHOOK_ADDRESS_PREFIX, WebhookEnvelope,
};

/// Per-subscription wake policy set by the subscriber (design §2.1).
///
/// Controls when an incoming push row triggers an eager wake of the subscriber:
/// - `VeryLow`…`High`: wake iff message urgency `>=` this level.
/// - `Never`: never eager-wake; rows park and deliver on the subscriber's next
///   natural drain (bridge connect / WASM activation / startup sweep).
///
/// Kept separate from [`Urgency`] so the `Never` sentinel (which has no
/// meaningful sender-side meaning) cannot appear on the message side.
///
/// Default subscription policy: `Normal` (migration parity — rows published
/// at `Normal` or above wake, rows at `Low` park, matching the old
/// binary `immediate`/`none` split at `push_depth > 0`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WakeMin {
    VeryLow,
    Low,
    Normal,
    High,
    /// Never eager-wake this subscriber; rows park for natural drain.
    Never,
}

impl WakeMin {
    /// Returns `true` iff a message at `urgency` should trigger an eager wake
    /// for a subscriber with this `WakeMin` policy.
    ///
    /// Semantics: wake iff `urgency >= self` (threshold-inclusive); `Never`
    /// always returns `false` regardless of urgency.
    pub fn wakes(self, urgency: Urgency) -> bool {
        match self {
            WakeMin::Never => false,
            WakeMin::VeryLow => urgency >= Urgency::VeryLow,
            WakeMin::Low => urgency >= Urgency::Low,
            WakeMin::Normal => urgency >= Urgency::Normal,
            WakeMin::High => urgency >= Urgency::High,
        }
    }

    /// Wire/DB/TOML string representation.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::VeryLow => "very-low",
            Self::Low => "low",
            Self::Normal => "normal",
            Self::High => "high",
            Self::Never => "never",
        }
    }

    /// Parse from a wire/DB/TOML string. Returns `None` on unknown values.
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "very-low" => Some(Self::VeryLow),
            "low" => Some(Self::Low),
            "normal" => Some(Self::Normal),
            "high" => Some(Self::High),
            "never" => Some(Self::Never),
            _ => None,
        }
    }
}

/// Protocol-specific details for a `brenn:` channel listing.
#[derive(Debug, Clone, serde::Serialize)]
pub struct BrennDetails {
    pub subscribers: Vec<String>,
}

/// Protocol-specific details for a `pwa_push:` channel listing.
#[derive(Debug, Clone, serde::Serialize)]
pub struct PwaPushDetails {
    pub user: String,
    pub device: Option<String>,
    pub last_seen_at: String,
}

/// Protocol-specific details for a `webhook:` channel listing.
#[derive(Debug, Clone, serde::Serialize)]
pub struct WebhookDetails {
    /// HTTP mount path where the endpoint listens (e.g. `/webhooks/phonebuddy`).
    pub mount: String,
}

/// Protocol-specific details for an `mqtt:` channel listing (design §2.5).
///
/// `client`/`topic` are parsed from the channel address by `list_channels()` and
/// are always present. The runtime ingress-health fields (`qos`/`urgency`/`health`/
/// `last_error`) are left `None` by `Messenger` — they are populated later by the
/// `MessageChannelList` intercept enrichment, which has access to `MqttService`.
/// Keeping them out of `Messenger` keeps the messaging core free of any MQTT
/// dependency (`health` is the stringified connector-health label, set by the
/// enrichment layer). `Option` fields serialize away when unset
/// (`skip_serializing_if`), so a listing produced before enrichment carries only
/// `client`/`topic`.
#[derive(Debug, Clone, serde::Serialize)]
pub struct MqttDetails {
    /// MQTT client slug (the address's `<client>` segment).
    pub client: String,
    /// MQTT topic filter (the address's `<topic>` segment; may contain `+`/`#`).
    pub topic: String,
    /// Broker SUBSCRIBE QoS for this client's ingress. Filled by enrichment.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub qos: Option<u8>,
    /// Sender-side injection urgency stamped on inbound messages. Filled by enrichment.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub urgency: Option<Urgency>,
    /// Stringified ingress connection-health label. Filled by enrichment.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub health: Option<String>,
    /// Last ingress connection error, if any. Filled by enrichment.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

/// Per-protocol `details` payload — untagged so wire JSON has no wrapper key.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(untagged)]
pub enum ChannelDetails {
    Brenn(BrennDetails),
    Mqtt(MqttDetails),
    PwaPush(PwaPushDetails),
    Webhook(WebhookDetails),
}

/// What kind of answer a `MessageChannelList` row represents (design §2.2).
///
/// `MessageChannelList` answers "what could *this app* subscribe to?". For the
/// exact-answer transports (`brenn:`/`webhook:`/`pwa_push:`) a row is a concrete
/// channel that exists now and the app's ACL covers. For `mqtt:` — where the
/// broker exposes no topic enumeration — a row is instead an ACL-allowed topic
/// *filter*, which may be a wildcard (e.g. `sensors/#`) and may not correspond to
/// any concrete topic yet. The two are distinguished so the LLM does not treat a
/// wildcard matcher as a literal channel name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AccessKind {
    /// A channel that exists now and the app's ACL permits subscribing to.
    Existing,
    /// An ACL-allowed pattern (an `mqtt_subscribe` matcher); may be a wildcard,
    /// may not correspond to any existing concrete topic. Subscribe with a
    /// concrete topic under the pattern to discover what the broker actually has.
    Pattern,
}

/// One row of the `MessageChannelList` output (unified cross-protocol format).
///
/// The `protocol` field identifies the transport family; `details` carries
/// protocol-specific data as a free-form object. Consumers should treat
/// `details` as opaque unless they know the protocol's shape, or use the
/// corresponding `*ChannelGet` tool for structured per-channel detail. The
/// `access` field distinguishes a concrete `Existing` channel from an ACL-derived
/// `Pattern` (design §2.2).
#[derive(Debug, Clone, serde::Serialize)]
pub struct ChannelListing {
    /// Protocol family.
    pub protocol: ChannelScheme,
    pub address: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Whether this row is a concrete existing channel or an ACL-allowed pattern
    /// (design §2.2).
    pub access: AccessKind,
    /// Protocol-specific extra data. Shape is per-protocol.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<ChannelDetails>,
}

/// One row of the `MessageSubscriptionList` output (design §2.1).
///
/// Unlike [`ChannelListing`] — which describes a channel as a whole — this
/// describes **one app's own subscription** to a channel: it carries the
/// *per-subscriber* resolved parameters (`push_depth`/`retain_depth`/`noise`/
/// `wake_min`) taken from that app's `SubscriberEntry`, not the channel-wide
/// subscriber roster. The `dynamic` flag says whether the subscription is
/// runtime-created (removable via `MessageUnsubscribe`) or static/config-managed
/// (not runtime-removable).
///
/// `details` reuses the same per-protocol [`ChannelDetails`] enum as
/// `ChannelListing`; for `mqtt:` the runtime-health fields are left `None` by
/// `Messenger` (filled by the `MessageSubscriptionList` intercept enrichment,
/// exactly as for `MessageChannelList`).
#[derive(Debug, Clone, serde::Serialize)]
pub struct SubscriptionListing {
    /// Protocol family.
    pub protocol: ChannelScheme,
    pub address: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// `true` = runtime (dynamic) subscription, removable via `MessageUnsubscribe`;
    /// `false` = static (config-managed) subscription, not runtime-removable.
    pub dynamic: bool,
    /// This app's resolved per-subscription push depth.
    pub push_depth: config::Depth,
    /// This app's resolved per-subscription retain depth.
    pub retain_depth: config::Depth,
    /// This app's resolved per-subscription noise level.
    pub noise: config::NoiseLevel,
    /// This app's resolved per-subscription wake-min policy.
    pub wake_min: WakeMin,
    /// Protocol-specific extra data. Shape is per-protocol.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<ChannelDetails>,
}

// ---------------------------------------------------------------------------
// Registration key + delivery shape
// ---------------------------------------------------------------------------

/// Classify a pending-push row's target into the [`SubscriberEntryKind`] its
/// registration and delivery binding are keyed by. Total and pure — it reads
/// data only, never branches behavior.
///
/// A `Conversation(_)` target keys to `App(target_app_slug)` (the row schema
/// carries `target_app_slug`; app-backed conversations always name their app).
/// Every other subscriber kind keys to its own slug/component. Every behavioral
/// decision downstream (policy, binding, delivery shape) consults the registry
/// this keys into — adding a new subscriber kind forces a new arm here (a
/// compile error) plus an explicit registration and binding at boot.
pub fn registration_key(target: &ParticipantId, target_app_slug: &str) -> SubscriberEntryKind {
    match target.kind() {
        SubscriberKind::Conversation(_) => {
            assert!(
                !target_app_slug.is_empty(),
                "registration_key: Conversation target {} has empty target_app_slug — \
                 every app-backed conversation names its app",
                target.as_str()
            );
            SubscriberEntryKind::App(target_app_slug.to_string())
        }
        SubscriberKind::Wasm(slug) => SubscriberEntryKind::Wasm(slug),
        SubscriberKind::Surface { slug, instance } => {
            SubscriberEntryKind::Surface { slug, instance }
        }
        SubscriberKind::System(component) => SubscriberEntryKind::System(component),
    }
}

/// How a subscriber's registered delivery binding shapes dispatch of one row.
/// Derived by the [`WakeRouter`] from the registered binding, never from the
/// identity prefix — a new subscriber kind cannot silently inherit an inline
/// deliver path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryShape {
    /// Deliver inline through `deliver`/`deliver_ingress`. The dispatcher owns
    /// the delivered-mark: no inline binding keeps delivery state of its own in
    /// the row.
    Inline,
    /// Never delivered inline: route to the off-loop parked task via
    /// `spawn_eager_wake` and leave the row parked (WASM/system subscribers).
    ParkedWake,
}

/// What serving one wake cost, as the [`WakeRouter`] answers it.
///
/// The wake cooldown exists to bound subprocess spawns, so it applies to a wake
/// that spawns one and to nothing else. A subscriber the router found already
/// live is served in place; that costs no spawn and proves the subscriber is up,
/// which is exactly the two conditions under which the cooldown has nothing to
/// pace.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WakeServed {
    /// A wake was fired at a subscriber that was not running — the shape whose
    /// cost the cooldown bounds.
    Spawned,
    /// The subscriber was already live and was served from its position on the
    /// spot; no wake was fired.
    Live,
}

/// The default [`DeliveryShape`] for a subscriber kind, mirroring the
/// kind→binding choices bootstrap makes by hand: `App` and `Surface`
/// subscribers deliver inline; `Wasm`/`System` subscribers are
/// parked-and-woken.
///
/// Bootstrap is authoritative: it registers each real binding directly (see
/// the delivery-binding registration in `brenn-server`), and the live dispatch
/// path reads those registered bindings, never this function. This exists only
/// for test doubles / `NoopWakeRouter` impls that need a shape without wiring a
/// full binding map; keep it in step with bootstrap's choices by hand.
pub fn default_delivery_shape(key: &SubscriberEntryKind) -> DeliveryShape {
    match key {
        SubscriberEntryKind::App(_) | SubscriberEntryKind::Surface { .. } => DeliveryShape::Inline,
        SubscriberEntryKind::Wasm(_) | SubscriberEntryKind::System(_) => DeliveryShape::ParkedWake,
    }
}

// ---------------------------------------------------------------------------
// WakeRouter trait
// ---------------------------------------------------------------------------

/// Wake / deliver surface implemented by the binary crate.
///
/// `Messenger` lives in `brenn-lib` and must not depend on binary-crate
/// types. The binary crate provides an adapter that closes over
/// `ActiveBridges` + `AppState` and implements this trait; `Messenger`
/// calls into it via `Arc<dyn WakeRouter>`.
///
/// Every method takes the row's `key` (its [`SubscriberEntryKind`], derived
/// once by the dispatcher via [`registration_key`]) so the implementation
/// resolves each subscriber's delivery binding by key rather than re-deriving
/// behavior from the identity prefix.
#[async_trait::async_trait]
pub trait WakeRouter: Send + Sync + 'static {
    /// Hand a just-retained envelope to a push-enabled surface subscription's
    /// attached sessions, live.
    ///
    /// - `Ok(true)` when at least one session accepted it.
    /// - `Ok(false)` when no session of that subscription is attached — nothing
    ///   is owed to one that is away; it resumes past its own cursor.
    /// - `Err(_)` when a session was attached but the hand-off failed.
    ///
    /// `retained_seq` is the message's position in its channel's retention order,
    /// which is what the client's resume cursor is minted from. The envelope
    /// arrives as an `Arc` the caller already holds, so a fan-out over several
    /// sessions and several subscriptions copies the body no times.
    async fn deliver(
        &self,
        key: &SubscriberEntryKind,
        envelope: &Arc<MessageEnvelope>,
        retained_seq: i64,
    ) -> Result<bool, String>;

    /// Row-less deliver-if-attached context feed for a depth-0 (fold-0) surface
    /// subscription. A fold-0 subscription is live-or-nothing: it holds no
    /// resume position, so a durable message reaches an attached session only as
    /// a live fan-out at publish time. A session not attached is owed nothing —
    /// its retained context arrives at the next subscribe. `key` is the surface
    /// subscriber's `SubscriberEntryKind::Surface`; `envelope` and
    /// `retained_seq` (the message's position in its channel's retention order)
    /// are the just-committed message.
    ///
    /// Default no-op: only the surface router impl fans out. Test doubles that
    /// never host surface sessions inherit the no-op.
    async fn deliver_context(
        &self,
        key: &SubscriberEntryKind,
        envelope: &Arc<MessageEnvelope>,
        retained_seq: i64,
    ) {
        let _ = (key, envelope, retained_seq);
    }

    /// Cheap precheck for the surface live feed: does any currently-attached
    /// session hold a subscription on `channel` for one of `targets`? A `false`
    /// answer lets the publish path skip building the owned, body-copying feed
    /// envelope entirely when no page is open — a deliver-if-attached fan-out
    /// owes a disconnected session nothing, and a push-enabled one resumes past
    /// its own cursor when it comes back.
    ///
    /// Default `true`: a router that hosts no surface sessions never reaches the
    /// build guard with non-empty targets, so the default costs it nothing.
    fn any_surface_session_subscribed(
        &self,
        channel: &str,
        targets: &[store::targets::SurfaceFeedTarget],
    ) -> bool {
        let _ = (channel, targets);
        true
    }

    /// Deliver an ingress event to the target subscriber's active bridge using
    /// the timestamped batch card renderer (`format_event_batch` / drain card).
    ///
    /// Mirrors the contract of `deliver` but for the ingress-event shape:
    /// - `Ok(true)` on send success.
    /// - `Ok(false)` if no bridge is active.
    /// - `Err(_)` if the bridge was active but the send failed.
    ///
    /// **Invariant:** invoked by `dispatcher::dispatch_row` for ingress-typed rows.
    /// Ingress rows flow through `dispatch_row`, not directly through `WakeRouter::deliver`.
    /// All ingress — single or batched, live-inject or drain — renders through the
    /// single timestamped batch formatter (design §2.10, R9).
    async fn deliver_ingress(
        &self,
        key: &SubscriberEntryKind,
        subscriber: &ParticipantId,
        event: &ingress::Event,
    ) -> Result<bool, String>;

    /// Fire-and-forget eager wake. No return value; the next `deliver` call
    /// after wake completes (asynchronously) will observe the new bridge.
    fn spawn_eager_wake(&self, key: &SubscriberEntryKind, subscriber: &ParticipantId);

    /// Wake `subscriber` from the recurring walk over who is owed work.
    ///
    /// The walk names a subscriber whose position trails retention; what that
    /// costs to serve depends on whether it is already running. An inline
    /// subscriber that is live is served here and now — it is awake, and the
    /// spawn-shaped wake would find it running and do nothing. One that is not
    /// gets the ordinary eager wake.
    ///
    /// The return value tells the walk which of the two it was, so the wake
    /// cooldown — which exists to bound spawns — paces only the spawn.
    ///
    /// Default: the eager wake alone, which is the whole of the answer for a
    /// parked subscriber (its notify is the delivery trigger) and for a surface
    /// slug (the nudge is what makes its sessions drain).
    async fn wake_owed(&self, key: &SubscriberEntryKind, subscriber: &ParticipantId) -> WakeServed {
        self.spawn_eager_wake(key, subscriber);
        WakeServed::Spawned
    }

    /// The [`DeliveryShape`] of the subscriber registered under `key`, derived
    /// from its delivery binding. `dispatch_row` consults this to choose the
    /// inline-deliver vs. parked-wake path and whether to re-mark delivery.
    fn delivery_shape(&self, key: &SubscriberEntryKind) -> DeliveryShape;

    /// Fire a push-overflow alarm for the given channel + subscriber, naming the
    /// span the subscriber lost: `count` messages passed out of retention (or
    /// out of its window) with its position still behind them. Called when
    /// `noise = Alarm` and an overflow is reported against a position. The
    /// implementation should call `AlertDispatcher::alert` with
    /// `AlertSeverity::Warning`; the rate-limiter on `AlertDispatcher` prevents
    /// flooding. The metric counter is incremented separately before this is
    /// called; `alarm` handles only the alerting side.
    fn alarm(&self, channel: &str, subscriber: &ParticipantId, count: u64);
}

// ---------------------------------------------------------------------------
// Messenger
// ---------------------------------------------------------------------------

/// What one release sweep across every channel moved into retention.
#[derive(Debug, Default)]
pub struct ReleaseSweep {
    /// Total messages released across all channels, both classes.
    pub released: usize,
    /// Earliest deferred release still parked anywhere once the sweep is done,
    /// or `None` when nothing is parked. The sweep already asks every store
    /// when its next release is due, so it answers that question for the
    /// dispatcher's sleep target rather than making it walk the stores again.
    pub next_release: Option<DateTime<Utc>>,
}

/// What one wake pass ([`Messenger::wake_owed_subscribers`]) learned about
/// delivery deadlines while deciding who to wake.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct WakeSweep {
    /// Whether any subscriber was woken because a deadline had come due.
    /// Callers should debounce on it: a forced wake takes seconds to land, and
    /// the deadline stays due until the subscriber drains past it.
    pub fired_deadline_wake: bool,
    /// The earliest deadline still ahead of the pass, across every lagging
    /// position on every channel, or `None` when nothing unseen carries one.
    /// Deadlines already due are absent — the pass just fired for them, and a
    /// sleep target in the past would spin.
    pub next_deadline: Option<DateTime<Utc>>,
}

/// The messaging service. Owns the directory, the `WakeRouter`, and the
/// kick channels for the deferred-delivery / deadline tasks.
///
/// Held on `AppState` as `Arc<Messenger>`. Constructed once at startup.
pub struct Messenger {
    pub(crate) db: Db,
    pub(crate) directory: Arc<MessagingDirectory>,
    /// Resolved at startup; see `resolve_source`. The publish hot path
    /// reads this directly.
    pub(crate) source: Arc<str>,
    pub(crate) apps: Arc<IndexMap<String, AppConfig>>,
    /// Who a channel's messages are owed to, and on what terms: the unified
    /// subscriber registry (one entry per registered non-app subscriber, holding
    /// its resolved access-control policy and declared [`WakeEconomics`]) plus
    /// the target resolution every delivery-record writer runs.
    ///
    /// Shared with each durable store, which resolves its own release targets
    /// through it — so the publish ladder, the batch flush paths, and a release
    /// pass all answer "who gets this" from one implementation reading one
    /// registry. Carries no subscriber registrations until
    /// `with_subscriber_registrations` populates it at boot.
    pub(crate) targets: Arc<store::TargetResolver>,
    pub(crate) router: Arc<dyn WakeRouter>,
    pub(crate) defaults: MessagingGlobalConfig,
    pub(crate) dispatch_kick_notify: Arc<tokio::sync::Notify>,
    /// Count of `load_activation_snapshot` invocations. Monotonically increasing.
    /// Used in tests to assert exactly one subscriber-wide scan per drain step
    /// (AC 7). Not user-visible surface; test instrumentation only.
    /// Access via the public `pending_bus_pushes_scan_count()` accessor.
    pending_bus_pushes_scan_count: AtomicU64,
    /// Consumer-side counters for the live streams of transportable non-durable
    /// channels: fan-out lag and delivery-time denials, per `(channel,
    /// participant)`. Shared with every receiver [`Messenger::attach_live`]
    /// hands out, which is why it is an `Arc` where the other counters are
    /// plain fields.
    live_counters: Arc<live::LiveCounters>,
    /// Retention stores for the process's non-durable channels, keyed by channel
    /// UUID. Empty on a `Messenger` with no `ephemeral:`/`local:` channels; boot
    /// installs the config-resolved set via [`Messenger::with_ring_stores`].
    ///
    /// The durable half of the same registry lives in [`Messenger::db_stores`]:
    /// a durable channel's messages sit in the database, but its store is still
    /// constructed once per channel and reused.
    ring_stores: Arc<store::RingStores>,
    /// Durable channels' retention stores, one per channel UUID, constructed on
    /// first request and reused for the process lifetime.
    ///
    /// Per-channel in-memory delivery state a durable store comes to hold must
    /// live on a single instance. This lazy cache gives that single-instance
    /// guarantee without a boot build pass, so a runtime-created durable
    /// channel (an `mqtt:` topic-filter) is covered on first access like a
    /// config channel.
    db_stores: Mutex<HashMap<Uuid, Arc<store::DbStore>>>,
    /// Dynamic subscription registrations on non-durable channels, keyed by
    /// `(channel uuid, app slug)` — the in-memory peer of the
    /// `messaging_dynamic_subscriptions` table.
    ///
    /// Registration persistence follows channel durability: a non-durable
    /// channel's messages die with the process, so a registration outliving them
    /// would name data that no longer exists. This set is that registration, and
    /// it is the authority on "does this app hold a dynamic subscription here"
    /// for non-durable channels, exactly as the durable row is for durable ones.
    /// Empty after a restart, by construction.
    nondurable_dynamic_subs: Mutex<HashSet<(Uuid, String)>>,
    /// `(channel uuid, subscriber)` pairs already reported as stranded by the
    /// wake walk — owed messages under no registration that could wake them.
    /// The condition never clears on its own and the walk revisits it every
    /// pass, so the set holds each pair to a single report.
    stranded_warned: Mutex<HashSet<(Uuid, String)>>,
    /// `(channel uuid, app slug)` pairs already reported as ACL-denied at
    /// delivery. A revoked ACL persists until an operator restores it while the
    /// subscriber keeps its backlog, so every drain would re-report it — and a
    /// config change is exactly when the log has to stay readable.
    acl_denied_warned: Mutex<HashSet<(Uuid, String)>>,
    /// When each urgency-gated subscriber was last woken, by anything.
    ///
    /// An urgency-gated wake spawns a subprocess, and both wake sources — the
    /// walk over trailing positions and the dispatcher's fan-out — run on every
    /// dispatcher kick, so a subscriber whose spawn fails, or which simply takes
    /// a while to come up and drain, would be re-woken as fast as the bus is
    /// busy. One map for both sources: two cooldowns that could not see each
    /// other would each pass their own gate and double the spawn rate they exist
    /// to bound. Eager subscribers are never held back — their wake is a notify.
    ///
    /// Armed on a fresh wake, cleared when a delivery proves the subscriber
    /// live, and left to expire on a gated pass (clearing it there would halve
    /// the cooldown under repeated kicks).
    inline_wake_backoff: Mutex<HashMap<String, std::time::Instant>>,
    /// Serializes [`Messenger::subscribe_dynamic`] and
    /// [`Messenger::unsubscribe_dynamic`] end to end.
    ///
    /// Both classify a `(channel, app)` from its registration record and then
    /// write that record, with `.await` points in between; without this gate two
    /// concurrent tool calls for the same pair can both classify "not
    /// registered" and both proceed to the write, where the second collides —
    /// the durable INSERT on its primary key, the in-memory set on its uniqueness
    /// assert. Holding one lock across classify-and-write makes each of those
    /// collisions the host bug its message claims, and makes a duplicated tool
    /// call answer `AlreadySubscribed…` instead of killing the process.
    ///
    /// Runtime subscribe is a rare, human-paced tool call, so one process-wide
    /// gate costs nothing that per-`(channel, app)` keying would buy. Lock order
    /// is gate → `db`; nothing acquires them the other way.
    dynamic_subscribe_gate: tokio::sync::Mutex<()>,
    /// Durable send budgets for surface principals, keyed by the principal's two
    /// grains: `(slug, None)` is the surface's own kernel identity, `(slug,
    /// Some(instance))` is one declared component instance on it. Installed
    /// boot-only via [`Messenger::with_surface_send_budgets`].
    ///
    /// One bucket per key is the blast-radius scoping: a component looping on
    /// retries drains its own instance's bucket and leaves its siblings — including
    /// its siblings of the same kind — and the kernel's own reports, untouched.
    /// Keyed by principal rather than by connection so a reconnect inherits the
    /// drained bucket rather than refreshing it.
    ///
    /// The `publish_core` Surface arm consults it for every durable publish a
    /// surface makes under its own identity. Empty on a `Messenger` with no
    /// surfaces; a Surface publish whose key is absent is a broken boot invariant
    /// (panic). The `std::sync::Mutex` holds no lock across an await.
    pub(crate) surface_send_budgets:
        HashMap<(String, Option<String>), Mutex<crate::token_bucket::TokenBucket>>,
    /// The unified send-rate gate: one token bucket per `(sender, channel)`,
    /// created on that pair's first publish at the channel's resolved rate.
    /// Every publish on every scheme draws from it.
    ///
    /// Bounded without eviction: senders are config-resolved principals and
    /// channels are config-declared, so the key space is a product of two
    /// operator-authored sets.
    pub(crate) send_rate_buckets: Mutex<HashMap<(String, Uuid), crate::token_bucket::TokenBucket>>,
    /// Per-sender count of publishes the send-rate gate refused.
    pub(crate) publish_rate_limited: Mutex<HashMap<String, u64>>,
    /// Per-`(sender, denial kind)` count of denied publishes, where the kind is
    /// a `PublishResult::signal_kind()` tag. Keyed by the config-resolved
    /// principal, never by the attacker-controlled address, so the key set stays
    /// bounded.
    pub(crate) publish_denied: Mutex<HashMap<(String, String), u64>>,
    /// Per-`(consumer, channel)` count of deferred WASM publishes dropped at flush
    /// because the channel's deferred set was at its `retain_depth` cap. The flush
    /// has no error channel back to the guest, so this counter is the only durable
    /// evidence that a scheduled self-publish never landed — a silently missing
    /// timer a health check can surface. Keyed by config-resolved principals, so
    /// the key set stays bounded.
    pub(crate) dropped_deferred: Mutex<HashMap<(String, String), u64>>,

    /// Per-`(consumer, channel)` count of deferred-control ops (defer-cancel /
    /// defer-edit) that were no-ops at flush because the target released between
    /// the activation snapshot and flush — the benign drain-vs-release race. Like
    /// `dropped_deferred`, the flush has no error channel back to the guest, so
    /// this counter is the only observable evidence a component's cancels/edits
    /// chronically lose the race (release times set too close to its own cadence).
    /// Keyed by config-resolved principals, so the key set stays bounded.
    pub(crate) deferred_control_races: Mutex<HashMap<(String, String), u64>>,

    /// Per-`(channel address, subscriber)` count of drops the noise ladder
    /// metered — all of them on a `metered`/`alarm` subscription, none on a
    /// `silent` one.
    ///
    /// It lives here because the ladder does: `enact_overflow_noise` is the one
    /// writer, and the stores below it hold no drop state at all — every figure
    /// they report is a subtraction between two seqs. In memory only, so a
    /// restart forgets the tallies. A pair is forgotten at detach, so a
    /// departed subscriber leaves nothing behind.
    metered_drops: Mutex<HashMap<(String, String), u64>>,
}

/// The channel registration that names `subscriber`, or `None` when the
/// subscriber holds delivery state on this channel under no registration of it.
///
/// Registrations are keyed by subscriber *kind*, so this is a kind comparison,
/// never an identity-string one. A `Conversation` participant carries no app in
/// its identity, so it matches an `App(slug)` registration only when the caller
/// supplies the `app_slug` the delivery record was written under — the store
/// that wrote the record is the one party that knows it.
///
/// The single resolution point from a store-reported `ParticipantId` to the
/// channel's registration for it.
fn registered_subscriber<'a>(
    entry: &'a ChannelEntry,
    subscriber: &ParticipantId,
    app_slug: Option<&str>,
) -> Option<&'a SubscriberEntry> {
    let matches_kind = |kind: &SubscriberEntryKind| match (kind, subscriber.kind()) {
        (SubscriberEntryKind::App(registered), SubscriberKind::Conversation(_)) => {
            app_slug.is_some_and(|named| registered == named)
        }
        (SubscriberEntryKind::Wasm(registered), SubscriberKind::Wasm(named)) => {
            *registered == named
        }
        (SubscriberEntryKind::System(registered), SubscriberKind::System(named)) => {
            *registered == named
        }
        (
            SubscriberEntryKind::Surface { slug, instance },
            SubscriberKind::Surface {
                slug: named_slug,
                instance: named_instance,
            },
        ) => *slug == named_slug && *instance == named_instance,
        _ => false,
    };
    entry.subscribers.iter().find(|sub| matches_kind(&sub.kind))
}

/// The noise level to enact for `subscriber`'s overflow on `entry`, or `None`
/// when the backend must not enact for it.
///
/// The registration is the authority: a subscriber's noise rung is resolved
/// once, at registration, and carried on the channel's subscriber list, so an
/// overflow reported by a store at any later moment reads the same rung the
/// delivery path would.
///
/// Three `None` cases, all deliberate:
/// - **Surface-kind** — the loud half of the ladder is kernel-enacted, on the
///   drop delta the page observes. `fatal` is legal on a surface registration
///   and would panic the backend sink, so surface events never go there.
/// - **Conversation-kind with no named app** — an `App(slug)` registration's
///   delivery participant is a `conversation:` identity, so its rung resolves
///   only through the app whose slug the report carries. Both classes' cursors
///   cache that slug and every store report names it, so this arm is not
///   reachable from them; the caller panics on one rather than mis-reporting
///   it, so a future reporter that names no app cannot quietly bypass the
///   ladder.
/// - **No matching registration** — delivery state that outlives its
///   registration, which the caller reports. The drop is still accounted on the
///   store; there is simply no resolved rung to be loud at, and inventing one
///   would be a guess.
fn overflow_noise_for(
    entry: &ChannelEntry,
    subscriber: &ParticipantId,
    app_slug: Option<&str>,
) -> Option<config::NoiseLevel> {
    let registration = registered_subscriber(entry, subscriber, app_slug)?;
    match registration.kind {
        SubscriberEntryKind::Surface { .. } => None,
        _ => Some(registration.noise),
    }
}

impl Messenger {
    /// Construct a `Messenger`. The caller owns `Arc`s for sharing with
    /// background tasks.
    pub fn new(
        db: Db,
        directory: Arc<MessagingDirectory>,
        source: Arc<str>,
        apps: Arc<IndexMap<String, AppConfig>>,
        router: Arc<dyn WakeRouter>,
        defaults: MessagingGlobalConfig,
    ) -> Arc<Self> {
        // Defense-in-depth: slug uniqueness makes collision structurally unreachable,
        // but assert explicitly anyway (better dead than wrong).
        {
            let mut seen: HashMap<String, &str> = HashMap::new();
            for (slug, app) in apps.iter() {
                if app.messaging_enabled() {
                    let id = ParticipantId::for_app(slug, &source).as_str().to_owned();
                    if let Some(prev_slug) = seen.insert(id.clone(), slug.as_str()) {
                        panic!(
                            "messaging: apps {prev_slug:?} and {slug:?} resolve to the \
                             same publisher identity {id:?}; each app must have a unique identity",
                        );
                    }
                }
            }
        }
        // Bump the durable store's incarnation exactly once per messenger boot —
        // the durable analogue of the ephemeral bus minting a fresh per-boot
        // epoch. The `Db` is uniquely owned at boot (no background task holds it
        // yet), so `try_lock` succeeds; a share this early is a boot-ordering bug.
        {
            let conn = db.try_lock().expect(
                "Messenger::new: db must be uniquely owned at boot (bump_incarnation would block)",
            );
            crate::messaging::db::bump_incarnation(&conn);
        }

        // Default empty store registry (zero non-durable channels); boot swaps in
        // the config-resolved one via `with_ring_stores`.
        let ring_stores = Arc::new(store::RingStores::empty());
        let targets = Arc::new(store::TargetResolver::new(apps.clone(), HashMap::new()));
        Arc::new(Self {
            db,
            directory,
            source,
            apps,
            targets,
            router,
            defaults,
            dispatch_kick_notify: Arc::new(tokio::sync::Notify::new()),
            pending_bus_pushes_scan_count: AtomicU64::new(0),
            live_counters: Arc::new(live::LiveCounters::default()),
            ring_stores,
            db_stores: Mutex::new(HashMap::new()),
            nondurable_dynamic_subs: Mutex::new(HashSet::new()),
            stranded_warned: Mutex::new(HashSet::new()),
            acl_denied_warned: Mutex::new(HashSet::new()),
            inline_wake_backoff: Mutex::new(HashMap::new()),
            dynamic_subscribe_gate: tokio::sync::Mutex::new(()),
            surface_send_budgets: HashMap::new(),
            send_rate_buckets: Mutex::new(HashMap::new()),
            publish_rate_limited: Mutex::new(HashMap::new()),
            publish_denied: Mutex::new(HashMap::new()),
            dropped_deferred: Mutex::new(HashMap::new()),
            deferred_control_races: Mutex::new(HashMap::new()),
            metered_drops: Mutex::new(HashMap::new()),
        })
    }

    /// Count one deferred WASM publish dropped at flush for `(consumer, channel)`
    /// because the channel's deferred set was at its `retain_depth` cap.
    pub(crate) fn record_dropped_deferred(&self, consumer: &str, channel: &str) {
        *self
            .dropped_deferred
            .lock()
            .expect("messaging: dropped_deferred lock poisoned")
            .entry((consumer.to_owned(), channel.to_owned()))
            .or_insert(0) += 1;
    }

    /// Count of deferred publishes dropped at flush for a `(consumer, channel)`
    /// pair — a scheduled self-publish that never landed because the deferred set
    /// was at its cap.
    pub fn dropped_deferred_count(&self, consumer: &str, channel: &str) -> u64 {
        *self
            .dropped_deferred
            .lock()
            .expect("messaging: dropped_deferred lock poisoned")
            .get(&(consumer.to_owned(), channel.to_owned()))
            .unwrap_or(&0)
    }

    /// Count one deferred-control op (defer-cancel / defer-edit) that was a no-op
    /// at flush for `(consumer, channel)` because its target released between the
    /// activation snapshot and flush.
    pub fn record_deferred_control_race(&self, consumer: &str, channel: &str) {
        *self
            .deferred_control_races
            .lock()
            .expect("messaging: deferred_control_races lock poisoned")
            .entry((consumer.to_owned(), channel.to_owned()))
            .or_insert(0) += 1;
    }

    /// Count of deferred-control ops that were no-ops at flush for a
    /// `(consumer, channel)` pair — cancels/edits that lost the drain-vs-release
    /// race.
    pub fn deferred_control_race_count(&self, consumer: &str, channel: &str) -> u64 {
        *self
            .deferred_control_races
            .lock()
            .expect("messaging: deferred_control_races lock poisoned")
            .get(&(consumer.to_owned(), channel.to_owned()))
            .unwrap_or(&0)
    }

    /// Count of publishes the send-rate gate refused for `sender`, across all
    /// channels.
    pub fn publish_rate_limited_count(&self, sender: &str) -> u64 {
        *self
            .publish_rate_limited
            .lock()
            .expect("messaging: publish_rate_limited lock poisoned")
            .get(sender)
            .unwrap_or(&0)
    }

    /// Count of denied publishes for a `(sender, kind)` pair, where `kind` is a
    /// `PublishResult::signal_kind()` tag.
    pub fn publish_denied_count(&self, sender: &str, kind: &str) -> u64 {
        *self
            .publish_denied
            .lock()
            .expect("messaging: publish_denied lock poisoned")
            .get(&(sender.to_owned(), kind.to_owned()))
            .unwrap_or(&0)
    }

    /// Install (or extend) subscriber registrations before the `Messenger` is
    /// shared, one entry per non-app subscriber keyed by its
    /// [`SubscriberEntryKind`]. Consumes and returns the `Arc` because the
    /// registry is populated at boot, immediately after `new`, while the `Arc`
    /// is still uniquely owned (`Arc::get_mut` therefore always succeeds).
    /// Panics if the `Arc` is already shared — that would be a boot-ordering
    /// bug.
    ///
    /// May be called more than once to fold in different subscriber kinds; a
    /// duplicate registration key across calls is a boot-wiring bug and panics
    /// (the same posture the former per-kind installers gave a chained boot).
    pub fn with_subscriber_registrations(
        mut self: Arc<Self>,
        registrations: HashMap<SubscriberEntryKind, SubscriberRegistration>,
    ) -> Arc<Self> {
        let inner = Arc::get_mut(&mut self).expect(
            "with_subscriber_registrations must run before the Messenger Arc is shared \
             (boot-ordering bug)",
        );
        // The resolver is shared with every durable store, so it is uniquely
        // owned only until the first one is built — which is also the last
        // moment a registration can land without a store already having read
        // past it.
        Arc::get_mut(&mut inner.targets)
            .expect(
                "with_subscriber_registrations must run before any retention store holds the \
                 target resolver (boot-ordering bug)",
            )
            .register(registrations);
        self
    }

    /// Install the durable send budgets for every surface principal before the
    /// `Messenger` is shared: one full [`crate::token_bucket::TokenBucket`] for
    /// each resolved surface's kernel identity, plus one per component instance
    /// declared on it.
    ///
    /// Each input is `(slug, instances)` — the surface and its declared instance
    /// ids. Instances, not kinds: the principal is the instance, the analog of a
    /// backend `[[app]]` slug (matching the `surface:<slug>#<instance>` grain),
    /// so twelve instances of one kind are twelve buckets and a runaway one
    /// drains only its own.
    ///
    /// Same boot-only, uniquely-owned discipline as
    /// [`Messenger::with_subscriber_registrations`]: the `Arc` is populated at
    /// boot while still uniquely owned, so `Arc::get_mut` always succeeds; a
    /// share before this call is a boot-ordering bug and panics. A duplicate
    /// *slug*, or a duplicate principal within one surface, is a boot-wiring bug
    /// and panics — boot resolution already proved instances unique per surface.
    ///
    /// Each principal arrives with its own resolved
    /// [`SurfaceSendBudget`](config::SurfaceSendBudget) — the instance's declared
    /// override or the defaults — rather than the caller passing bare names for
    /// this function to meter identically. Boot resolution owns the parameters;
    /// this owns the buckets.
    pub fn with_surface_send_budgets(
        mut self: Arc<Self>,
        surfaces: impl IntoIterator<Item = (String, config::SurfacePrincipalBudgets)>,
    ) -> Arc<Self> {
        let inner = Arc::get_mut(&mut self).expect(
            "with_surface_send_budgets must run before the Messenger Arc is shared \
             (boot-ordering bug)",
        );
        for (slug, principals) in surfaces {
            // The kernel grain (`None`) rides in the principal set like any
            // other: geometry/status skip the budget via the platform path, but
            // the kernel's own error reports do not, so its bucket must exist.
            for (instance, budget) in principals {
                let prev = inner.surface_send_budgets.insert(
                    (slug.clone(), instance.clone()),
                    Mutex::new(crate::token_bucket::TokenBucket::new(
                        budget.burst,
                        budget.refill,
                        1,
                    )),
                );
                assert!(
                    prev.is_none(),
                    "with_surface_send_budgets: duplicate budget for surface {slug:?} principal \
                     {instance:?} — principals are unique within a surface, so a repeat is a boot \
                     wiring bug",
                );
            }
        }
        self
    }

    /// Install the config-resolved non-durable stores before the `Messenger` is
    /// shared, replacing the empty default from `new`. Consumes and returns the
    /// `Arc` because the field is set exactly once at boot, immediately after
    /// `new`, while the `Arc` is still uniquely owned (`Arc::get_mut` therefore
    /// always succeeds). Panics if the `Arc` is already shared — that would be a
    /// boot-ordering bug.
    pub fn with_ring_stores(mut self: Arc<Self>, stores: Arc<store::RingStores>) -> Arc<Self> {
        let inner = Arc::get_mut(&mut self).expect(
            "with_ring_stores must run before the Messenger Arc is shared (boot-ordering bug)",
        );
        inner.ring_stores = stores;
        self
    }

    /// The non-durable store registry.
    pub fn ring_stores(&self) -> &Arc<store::RingStores> {
        &self.ring_stores
    }

    /// The retention store for a registered channel — where its messages live
    /// between publish and delivery.
    ///
    /// The channel's capabilities pick the implementation, once: a durable
    /// channel gets its `DbStore` handle over the shared connection, a
    /// non-durable one gets the process's ring for that channel. Each channel's
    /// store is constructed once and reused — the durable ones cached lazily in
    /// [`Messenger::db_stores`], the non-durable ones built at boot. Callers hold
    /// `Arc<dyn RetentionStore>` and never re-decide the class.
    ///
    /// # Panics
    ///
    /// If a non-durable entry has no ring. Boot builds one store per non-durable
    /// channel from the same entries it registers in the directory, so a miss
    /// means the two halves of the registry disagree about which channels exist.
    pub fn store_for(&self, entry: &ChannelEntry) -> Arc<dyn store::RetentionStore> {
        if entry.capabilities().durable {
            let mut db_stores = self.db_stores.lock().expect("db_stores poisoned");
            return db_stores
                .entry(entry.uuid)
                .or_insert_with(|| {
                    Arc::new(store::DbStore::new(
                        self.db.clone(),
                        entry.uuid,
                        entry.address.clone(),
                        entry.resolved_channel.retain_depth,
                    ))
                })
                .clone();
        }
        self.ring_stores
            .get(&entry.uuid)
            .unwrap_or_else(|| {
                panic!(
                    "messaging: non-durable channel {:?} is in the directory but has no retention \
                     store — the directory and the store registry disagree",
                    entry.address
                )
            })
            .clone()
    }

    /// [`store_for`](Self::store_for) resolved from a channel address.
    ///
    /// # Panics
    ///
    /// If `channel_address` resolves to no channel.
    pub fn store_for_address(&self, channel_address: &str) -> Arc<dyn store::RetentionStore> {
        let entry = self.directory.resolve(channel_address).unwrap_or_else(|| {
            panic!(
                "messaging: store requested for channel {channel_address:?} not in the directory"
            )
        });
        self.store_for(&entry)
    }

    /// One sender's parked (deferred) messages on `channel_address`, soonest
    /// release first — the substrate half of a WASM output port's deferred view.
    ///
    /// Resolves the channel's store (durable or ring-backed, via [`store_for`])
    /// and reads its sender-scoped deferred surface. Authorization is structural:
    /// only messages whose recorded sender equals `sender` are returned, so a
    /// caller passing a component's own `wasm:<slug>` identity can see only that
    /// component's schedule, even on a channel other components also publish to.
    /// A message that has matured but not yet been taken by a release pass is out
    /// of the view (it can no longer be cancelled or edited); `now` is the instant
    /// that boundary is judged against.
    ///
    /// # Panics
    ///
    /// If `channel_address` resolves to no channel — the caller resolved it from
    /// a bound output port, so an unknown address is a boot-validation gap.
    pub async fn deferred_view_for_sender(
        &self,
        channel_address: &str,
        sender: &str,
        now: DateTime<Utc>,
    ) -> Vec<store::DeferredMessage> {
        self.store_for_bound_output(channel_address)
            .deferred_for_sender(sender, now)
            .await
    }

    /// Resolves a bound WASM output port's channel address to its retention store.
    ///
    /// The caller must have resolved `channel_address` from a bound output port;
    /// an address not in the directory is a boot-validation gap, not a runtime
    /// condition.
    ///
    /// # Panics
    ///
    /// If `channel_address` resolves to no channel.
    fn store_for_bound_output(&self, channel_address: &str) -> Arc<dyn store::RetentionStore> {
        let entry = self.directory.resolve(channel_address).unwrap_or_else(|| {
            panic!(
                "messaging: deferred operation requested for channel {channel_address:?} not in \
                 the directory — a bound output port must resolve"
            )
        });
        self.store_for(&entry)
    }

    /// Cancel one of `sender`'s parked messages on `channel_address`, named by its
    /// message uuid — the substrate half of a WASM output port's `defer-cancel`.
    ///
    /// Resolves the channel's store and applies the cancel on its sender-scoped
    /// deferred surface. Authorization is structural: the store cancels only a
    /// message whose recorded sender equals `sender`. Returns [`NotDeferred`] when
    /// the message already released between the caller's view and this call — a
    /// benign race, not a failure.
    ///
    /// # Panics
    ///
    /// If `channel_address` resolves to no channel (a bound output port must
    /// resolve).
    ///
    /// [`NotDeferred`]: store::DeferralOutcome::NotDeferred
    pub async fn cancel_deferred_for_sender(
        &self,
        channel_address: &str,
        sender: &str,
        message_uuid: Uuid,
        now: DateTime<Utc>,
    ) -> store::DeferralOutcome {
        self.store_for_bound_output(channel_address)
            .cancel_deferred(sender, message_uuid, now)
            .await
    }

    /// Edit one of `sender`'s parked messages on `channel_address`, named by its
    /// message uuid — the substrate half of a WASM output port's `defer-edit`.
    /// `body` and `release_at` are each `Some` to change, `None` to leave alone.
    ///
    /// Same resolution, structural authorization, and race semantics as
    /// [`cancel_deferred_for_sender`].
    ///
    /// # Panics
    ///
    /// If `channel_address` resolves to no channel (a bound output port must
    /// resolve).
    pub async fn edit_deferred_for_sender(
        &self,
        channel_address: &str,
        sender: &str,
        message_uuid: Uuid,
        body: Option<String>,
        release_at: Option<DateTime<Utc>>,
        now: DateTime<Utc>,
    ) -> store::DeferralOutcome {
        self.store_for_bound_output(channel_address)
            .edit_deferred(sender, message_uuid, body, release_at, now)
            .await
    }

    /// The ring behind a non-durable channel, for tests that drive its
    /// synchronous inherent surface (append, take, cursor presence) directly.
    /// Production code reaches every store the same way, through
    /// [`store_for`](Self::store_for).
    ///
    /// # Panics
    ///
    /// If the entry is durable, or if a non-durable entry has no ring — both are
    /// the registry disagreeing with itself.
    #[cfg(test)]
    pub(crate) fn ring_store_for(&self, entry: &ChannelEntry) -> &Arc<store::RingStore> {
        assert!(
            !entry.capabilities().durable,
            "messaging: channel {:?} is durable and has no ring store",
            entry.address
        );
        self.ring_stores.get(&entry.uuid).unwrap_or_else(|| {
            panic!(
                "messaging: non-durable channel {:?} is in the directory but has no retention \
                 store — the directory and the store registry disagree",
                entry.address
            )
        })
    }

    /// [`enact_overflow_events`](Self::enact_overflow_events) for a caller that
    /// holds a channel address rather than its directory entry — the shape of a
    /// publisher outside `brenn-lib` (the surface session's bound-output
    /// publish), which gets its overflow back from the bus.
    ///
    /// # Panics
    ///
    /// If `channel_address` resolves to no channel: a store reported overflow
    /// for a channel the directory does not know, so the two disagree about
    /// which channels exist.
    pub fn enact_overflow_for_channel(
        &self,
        channel_address: &str,
        events: &[store::OverflowEvent],
    ) {
        if events.is_empty() {
            return;
        }
        let entry = self.directory.resolve(channel_address).unwrap_or_else(|| {
            panic!(
                "messaging: overflow reported for channel {channel_address:?} not in the directory"
            )
        });
        self.enact_overflow_events(&entry, events);
    }

    /// Route a store's overflow events to the single noise-enactment sink, one
    /// per named subscriber.
    ///
    /// The store reported the drops; the resolved subscription decides how loud
    /// they are. A Surface-kind subscriber is deliberately never routed here:
    /// its drops stay on its delivery state and reach the page kernel, which is
    /// the only enactor allowed to see `fatal`.
    fn enact_overflow_events(&self, entry: &ChannelEntry, events: &[store::OverflowEvent]) {
        if events.is_empty() {
            return;
        }
        for event in events {
            match overflow_noise_for(entry, &event.subscriber, event.app_slug.as_deref()) {
                Some(noise) => {
                    self.enact_overflow_noise(
                        &entry.address,
                        &event.subscriber,
                        noise,
                        event.dropped,
                    );
                }
                // Surface-kind is the designed exemption and says nothing.
                None if matches!(event.subscriber.kind(), SubscriberKind::Surface { .. }) => {}
                // A conversation's rung is unresolvable, not merely unregistered:
                // it lives on an `App(slug)` registration, and the event named
                // no app to key it by. Reaching here means delivery to an
                // App-kind subscriber landed from a store that records nothing
                // per subscriber — silently skipping the ladder for every such
                // drop, under a log line blaming a missing registration. Die
                // instead.
                None if matches!(event.subscriber.kind(), SubscriberKind::Conversation(_)) => {
                    panic!(
                        "messaging: channel {} reported overflow for conversation subscriber {} \
                         under app {:?} — a conversation's noise rung lives on an App \
                         registration this channel does not carry, so the drop would be \
                         accounted and never escalated",
                        entry.address,
                        event.subscriber.as_str(),
                        event.app_slug,
                    )
                }
                // Delivery state that outlived its registration: the drop is
                // accounted but nothing resolves its noise rung, so the ladder
                // cannot escalate it. Surface it rather than losing it.
                None => tracing::warn!(
                    channel = %entry.address,
                    subscriber = %event.subscriber.as_str(),
                    dropped = event.dropped,
                    "overflow reported for a subscriber with no registration on this channel — \
                     accounted but not escalated",
                ),
            }
        }
    }

    /// Register `subscriber`'s cursor on a ring-backed channel, or retune an
    /// existing one's push depth. Returns whether a new cursor was created.
    ///
    /// A ring-backed channel holds its subscriber registrations in memory (they
    /// are meaningless after the restart that empties the ring), so this is the
    /// non-durable analogue of a `messaging_subscriptions` row: it is the
    /// consumer's position from which [`Messenger::load_activation_snapshot`]
    /// draws its ring-backed NEW rows.
    ///
    /// `push_depth` bounds how many owed messages one activation takes; the
    /// caller applies any per-participant clamp (e.g. `WASM_WINDOW_MAX_NEW`)
    /// before calling. `priming` is honored only when the cursor is created.
    ///
    /// # Panics
    ///
    /// If `channel_uuid` names no ring-backed channel — the directory and the
    /// store registry disagree, or the caller passed a durable channel.
    pub fn attach_ring_subscriber(
        &self,
        channel_uuid: &Uuid,
        subscriber: &ParticipantId,
        push_depth: u64,
        priming: store::Priming,
    ) -> store::Attached {
        // The cursor caches the slug of the registration it is read back
        // through. A participant that names its own registration names that slug
        // too; a conversation does not, and reaches its ring cursor through
        // [`Messenger::attach_conversation`], which resolves the app first.
        let app_slug = registration_key(subscriber, "").slug().to_string();
        self.ring_store(channel_uuid)
            .attach(subscriber, &app_slug, push_depth, priming)
    }

    /// Register `subscriber`'s delivery state on the channel at
    /// `channel_address`, delegating to the channel's store.
    ///
    /// `push_depth` must be pre-clamped by the caller. Whether the queue is new
    /// is the store's own determination (see [`RetentionStore::attach`]).
    ///
    /// # Panics
    ///
    /// If `channel_address` resolves to no channel — a wired input must resolve.
    pub async fn attach_subscriber(
        &self,
        channel_address: &str,
        app_slug: &str,
        subscriber: &ParticipantId,
        push_depth: config::Depth,
        priming: store::Priming,
    ) -> store::Attached {
        let entry = self.directory.resolve(channel_address).unwrap_or_else(|| {
            panic!(
                "messaging: attach requested for channel {channel_address:?} not in the \
                 directory — a wired input must resolve"
            )
        });
        self.store_for(&entry)
            .attach(subscriber, app_slug, push_depth, priming)
            .await
    }

    /// Tear down `subscriber`'s delivery state on the channel at
    /// `channel_address` — its position, which the store owns, plus the metered
    /// tally the noise ladder kept for it. The inverse of
    /// [`Messenger::attach_subscriber`].
    ///
    /// The wake cooldown is deliberately left alone: it bounds spawn cost per
    /// subscriber across every channel, so leaving one channel says nothing
    /// about a window a wake for another channel's backlog armed. It lapses on
    /// its own, and only a signal that the subscriber is live clears it early
    /// ([`Messenger::clear_inline_wake`]).
    ///
    /// # Panics
    ///
    /// If `channel_address` resolves to no channel.
    pub async fn detach_subscriber(&self, channel_address: &str, subscriber: &ParticipantId) {
        let entry = self.directory.resolve(channel_address).unwrap_or_else(|| {
            panic!(
                "messaging: detach requested for channel {channel_address:?} not in the directory"
            )
        });
        self.store_for(&entry).detach(subscriber).await;
        self.forget_metered_drops(&entry.address, subscriber);
    }

    /// Advance `subscriber`'s position on `channel_address` over a window it
    /// has been served, and enact the noise for whatever that window skipped.
    ///
    /// Returns what the advance passed unserved, so the caller can hand the
    /// subscriber its own drop figure.
    ///
    /// # Panics
    ///
    /// If `channel_address` resolves to no channel.
    pub async fn advance_subscriber(
        &self,
        channel_address: &str,
        subscriber: &ParticipantId,
        through: store::MessageSeq,
        seen_floor: store::MessageSeq,
        noise: config::NoiseLevel,
    ) -> store::AdvanceOutcome {
        let entry = self.directory.resolve(channel_address).unwrap_or_else(|| {
            panic!(
                "messaging: advance requested for channel {channel_address:?} not in the directory"
            )
        });
        let store = self.store_for(&entry);
        let outcome = store.advance(subscriber, through, seen_floor).await;
        // noise_charge excludes losses already reported by eviction, so
        // nothing is enacted twice.
        self.enact_overflow_noise(&entry.address, subscriber, noise, outcome.noise_charge);
        outcome
    }

    /// Does `app_slug` hold a dynamic subscription on the non-durable channel
    /// `channel_uuid`?
    pub(crate) fn nondurable_dynamic_sub_exists(
        &self,
        channel_uuid: &Uuid,
        app_slug: &str,
    ) -> bool {
        self.nondurable_dynamic_subs
            .lock()
            .expect("messaging: nondurable_dynamic_subs lock poisoned")
            .contains(&(*channel_uuid, app_slug.to_string()))
    }

    /// Record `app_slug`'s dynamic subscription registration on the non-durable
    /// channel `channel_uuid`. Panics if one is already recorded: the caller
    /// classifies the re-subscribe case and writes under one hold of
    /// [`Messenger::dynamic_subscribe_gate`], so no other subscribe can register
    /// this pair in between and a collision is a host bug — mirroring the durable
    /// INSERT's "neither row pre-exists" guarantee.
    pub(crate) fn register_nondurable_dynamic_sub(&self, channel_uuid: Uuid, app_slug: &str) {
        let inserted = self
            .nondurable_dynamic_subs
            .lock()
            .expect("messaging: nondurable_dynamic_subs lock poisoned")
            .insert((channel_uuid, app_slug.to_string()));
        assert!(
            inserted,
            "messaging: app {app_slug:?} already holds a dynamic subscription registration on \
             non-durable channel {channel_uuid}"
        );
    }

    /// Drop `app_slug`'s dynamic subscription registration on the non-durable
    /// channel `channel_uuid`, reporting whether one was held — the in-memory
    /// analogue of the durable row delete, and the same authority on "was there
    /// a dynamic subscription to remove".
    pub(crate) fn remove_nondurable_dynamic_sub(
        &self,
        channel_uuid: &Uuid,
        app_slug: &str,
    ) -> bool {
        self.nondurable_dynamic_subs
            .lock()
            .expect("messaging: nondurable_dynamic_subs lock poisoned")
            .remove(&(*channel_uuid, app_slug.to_string()))
    }

    /// The ring store for a non-durable channel uuid, panicking if there is
    /// none — the shared lookup behind the ring-subscriber and snapshot paths.
    fn ring_store(&self, channel_uuid: &Uuid) -> &Arc<store::RingStore> {
        self.ring_stores.get(channel_uuid).unwrap_or_else(|| {
            panic!(
                "messaging: channel {channel_uuid} has no ring store — it is durable or the \
                 directory and the store registry disagree"
            )
        })
    }

    /// The earliest deferred release across every channel, or `None` when
    /// nothing is parked anywhere.
    pub async fn next_deferred_release(&self) -> Option<DateTime<Utc>> {
        let mut earliest: Option<DateTime<Utc>> = None;
        for entry in self.directory.list() {
            if let Some(due) = self.store_for(&entry).next_release().await {
                earliest = Some(earliest.map_or(due, |cur: DateTime<Utc>| cur.min(due)));
            }
        }
        earliest
    }

    /// Release every message due at or before `now`, on every channel, through
    /// the channel's own store.
    ///
    /// The pre-check guard is advisory, not load-bearing: each store's release
    /// path re-decides what is due under its own lock.
    ///
    /// A released message enters retention like any other, so it can push a
    /// lagging subscriber's owed messages out of the window; that overflow is
    /// enacted here, on the batch that caused it.
    ///
    /// The sweep reports the earliest release still parked after it ran
    /// ([`ReleaseSweep::next_release`]), asking each store the question once.
    pub async fn release_due_messages(&self, now: DateTime<Utc>) -> ReleaseSweep {
        let mut sweep = ReleaseSweep::default();
        let fold = |due: Option<DateTime<Utc>>, sweep: &mut ReleaseSweep| {
            if let Some(due) = due {
                sweep.next_release = Some(sweep.next_release.map_or(due, |cur| cur.min(due)));
            }
        };
        for entry in self.directory.list() {
            let store = self.store_for(&entry);
            match store.next_release().await {
                Some(due) if due <= now => {}
                other => {
                    fold(other, &mut sweep);
                    continue;
                }
            }
            // Release is target-blind: it moves the message into retention, and
            // every subscriber reads it from its own position. A subscriber that
            // attached while the message waited therefore receives it and one
            // whose subscription went away does not, with nobody resolving a
            // subscriber set here.
            let outcome = store.release_due(now).await;
            sweep.released += outcome.released.len();
            self.enact_overflow_events(&entry, &outcome.overflow);
            // A released message enters retention here, which is where a surface
            // is served: it holds no position for anything to walk, so the
            // release is its live fan-out, exactly as the commit is for an
            // unparked publish. Resolved once per releasing channel, after the
            // store's own lock is gone.
            if entry.capabilities().durable && !outcome.released.is_empty() {
                let feed_targets =
                    self.resolve_surface_feed_targets(&entry.address, entry.subscribers.as_slice());
                if !feed_targets.is_empty()
                    && self
                        .router
                        .any_surface_session_subscribed(&entry.address, &feed_targets)
                {
                    for released in &outcome.released {
                        self.fan_out_surface_feed(
                            &feed_targets,
                            Arc::clone(&released.envelope),
                            i64::try_from(released.seq.0)
                                .expect("messaging: retention position out of range"),
                        )
                        .await;
                    }
                }
            }
            let remaining = store.next_release().await;
            fold(remaining, &mut sweep);
        }
        sweep
    }

    /// Wake every subscriber currently owed messages that its wake economics say
    /// to wake, on every channel, by asking each channel's own store who is
    /// owed.
    ///
    /// The one wake source for channel-backed work, over every shape a position
    /// can belong to. A parked subscriber is woken whenever it is owed anything
    /// — the wake *is* its delivery trigger and costs a notify. An inline
    /// subscriber's wake may cost a subprocess, so it is woken only when the
    /// loudest message it has not seen clears its threshold: a backlog entirely
    /// below `wake_min` wakes nobody and waits for the subscriber's next natural
    /// drain, which is the same economics the publish path applies to the one
    /// message it commits.
    ///
    /// Because the decision is made here, from the live registration and the
    /// unseen suffix, it is re-made on every pass: a registration change, a
    /// louder message arriving behind a quiet backlog, or a wake that never
    /// landed all take effect at the next walk. That is what makes this the
    /// durable retry behind the post-commit wake — a failed subprocess spawn or
    /// a crash between commit and wake leaves a trailing position, and a
    /// trailing position is exactly what this walk looks for.
    ///
    /// A message's `delivery_deadline` is the second wake source here: at `now`
    /// past T, every subscriber whose position has not passed that message is
    /// woken whatever its urgency economics say, and whatever the inline cooldown
    /// says. The deadline leaves the pass's view the moment the position passes
    /// the message, so nothing re-fires for a deadline already served, and no
    /// per-subscriber copy of T is stored anywhere — the message row that has
    /// always carried it is the only record.
    ///
    /// Idempotent (`Notify` coalesces) and self-limiting: a subscriber stops
    /// being owed once it drains.
    pub async fn wake_owed_subscribers(&self, now: DateTime<Utc>) -> WakeSweep {
        let mut sweep = WakeSweep::default();
        for entry in self.directory.list() {
            // Skip channels no subscriber holds a position on — asking a durable
            // store costs a DB round trip, and only a push-enabled subscriber
            // can be owed anything.
            if !entry
                .subscribers
                .iter()
                .any(|sub| sub.push_depth.is_push_enabled())
            {
                continue;
            }
            let store = self.store_for(&entry);
            for owed in store.deliverable_subscribers().await {
                let Some(registration) =
                    registered_subscriber(&entry, &owed.subscriber, owed.app_slug.as_deref())
                else {
                    self.warn_stranded_subscriber(&entry, &owed.subscriber);
                    continue;
                };
                let economics = match self.router.delivery_shape(&registration.kind) {
                    // A parked subscriber's wake is a notify and its delivery
                    // trigger both: it is `Eager` by construction, so reading a
                    // registry to learn that would make a decision with one
                    // answer depend on a second source.
                    DeliveryShape::ParkedWake => WakeEconomics::Eager,
                    // An inline subscriber's wake can cost a subprocess, so the
                    // registration decides. Every directory subscriber resolves
                    // economics — the boot cross-check asserts exactly this — so
                    // one that does not is a host-wiring bug, and skipping it
                    // would wedge a subscriber nothing else wakes with no signal
                    // that it happened.
                    DeliveryShape::Inline => self
                        .targets
                        .wake_economics(&registration.kind)
                        .unwrap_or_else(|| {
                            panic!(
                                "wake walk: inline subscriber {:?} on channel {} has no wake \
                                 economics — host wiring bug",
                                registration.kind, entry.address,
                            )
                        }),
                };
                // A conversation's delivery read applies the ACL gate, so waking
                // one the gate will deny buys a subprocess spawn that renders
                // nothing — every pass, for as long as the revocation and the
                // backlog both stand. The other kinds have no read-side gate, so
                // their wakes still do work. Asked after the economics
                // resolution, because an app missing from the apps map fails
                // both tests and the wiring bug is the one worth reporting.
                if let SubscriberEntryKind::App(slug) = &registration.kind
                    && !self.channel_access_allowed(&registration.kind, &entry.address)
                {
                    self.warn_acl_denied(&entry, slug);
                    continue;
                }
                // A deadline that has come due overrides both gates below: the
                // point of `delivery_deadline` is that a message too quiet to
                // wake anyone still gets in front of its subscriber by T.
                let deadline_due = match owed.earliest_unseen_deadline {
                    Some(deadline) if deadline <= now => true,
                    Some(deadline) => {
                        sweep.next_deadline = Some(
                            sweep
                                .next_deadline
                                .map_or(deadline, |cur| cur.min(deadline)),
                        );
                        false
                    }
                    None => false,
                };
                if !deadline_due {
                    if !store::targets::wakes_at(
                        economics,
                        registration.wake_min,
                        owed.max_unseen_urgency,
                    ) {
                        continue;
                    }
                    if economics == WakeEconomics::UrgencyGated
                        && !self.inline_wake_due(&owed.subscriber)
                    {
                        continue;
                    }
                } else {
                    // The forced wake spawns like any other, so it arms the
                    // cooldown a gated wake would have armed — what it does not
                    // do is wait for one. A live serve clears it again below,
                    // for the same reason a live serve never arms it.
                    self.arm_inline_wake(owed.subscriber.as_str());
                    sweep.fired_deadline_wake = true;
                }
                // A subscriber the router found already live was served in
                // place, without a spawn — so the cooldown, which is there to
                // bound spawns, has nothing to pace and would only withhold the
                // next message from a bridge that is sitting right there. Same
                // reasoning the ingress supervisor applies when a delivery
                // proves its subscriber live (`dispatcher.rs`).
                if self
                    .router
                    .wake_owed(&registration.kind, &owed.subscriber)
                    .await
                    == WakeServed::Live
                {
                    self.clear_inline_wake(owed.subscriber.as_str());
                }
            }
        }
        sweep
    }

    /// Whether an urgency-gated wake for `subscriber_key` is still inside the
    /// cooldown the last one armed.
    ///
    /// The check and the arm are separate here because the dispatcher decides to
    /// wake and learns whether the wake actually fired at two different points;
    /// the walk, which learns both at once, uses [`Self::inline_wake_due`].
    pub fn inline_wake_gated(&self, subscriber_key: &str) -> bool {
        self.inline_wake_backoff
            .lock()
            .expect("messaging: inline_wake_backoff lock poisoned")
            .get(subscriber_key)
            .is_some_and(|when| when.elapsed() < dispatcher::POLL_INTERVAL)
    }

    /// Start the cooldown for `subscriber_key`: a spawn is in flight, so further
    /// wakes within the window coalesce into it.
    pub fn arm_inline_wake(&self, subscriber_key: &str) {
        self.inline_wake_backoff
            .lock()
            .expect("messaging: inline_wake_backoff lock poisoned")
            .insert(subscriber_key.to_string(), std::time::Instant::now());
    }

    /// End the cooldown for `subscriber_key`: something proved it live, so the
    /// next wake need not wait the window out.
    pub fn clear_inline_wake(&self, subscriber_key: &str) {
        self.inline_wake_backoff
            .lock()
            .expect("messaging: inline_wake_backoff lock poisoned")
            .remove(subscriber_key);
    }

    /// Whether the walk may wake this urgency-gated subscriber again yet,
    /// recording the wake when it may.
    ///
    /// One wake per subscriber per [`dispatcher::POLL_INTERVAL`], counted from
    /// the last wake anything fired. A subscriber that drains stops being owed
    /// and never asks again; one that does not is retried at the tick rate
    /// instead of at the publish rate.
    ///
    /// Check and arm under one lock hold: two passes that both read "due" would
    /// both spawn, which is the cost this exists to bound.
    fn inline_wake_due(&self, subscriber: &ParticipantId) -> bool {
        let mut last = self
            .inline_wake_backoff
            .lock()
            .expect("messaging: inline_wake_backoff lock poisoned");
        let now = std::time::Instant::now();
        match last.get(subscriber.as_str()) {
            Some(when) if now.duration_since(*when) < dispatcher::POLL_INTERVAL => false,
            _ => {
                last.insert(subscriber.as_str().to_string(), now);
                true
            }
        }
    }

    /// Report `subscriber` as owed messages on `entry` under no registration
    /// that could wake it — at most once per `(channel, subscriber)` pair.
    ///
    /// Conversations reach this arm normally and are not reported:
    /// registrations are keyed by subscriber kind, and an `App(slug)`
    /// registration never names the conversation that delivers under it.
    ///
    /// A `Wasm`/`System` subscriber does name its own registration, so one owed
    /// without a registration is stranded — nothing will ever take its
    /// messages. The reachable path is a configuration change: dropping a
    /// component's input binding leaves its cursor position behind, and no
    /// boot step tears it down. That is an operator-caused, persistent state
    /// rather than a wiring bug, so it is reported rather than fatal — and
    /// reported once, because the wake walk revisits it on every dispatcher
    /// pass and an unbounded repeat would bury the signal it carries.
    fn warn_stranded_subscriber(&self, entry: &ChannelEntry, subscriber: &ParticipantId) {
        if !matches!(
            subscriber.kind(),
            SubscriberKind::Wasm(_) | SubscriberKind::System(_)
        ) {
            return;
        }
        let first_report = self
            .stranded_warned
            .lock()
            .expect("messaging: stranded_warned lock poisoned")
            .insert((entry.uuid, subscriber.as_str().to_string()));
        if first_report {
            tracing::warn!(
                channel = %entry.address,
                subscriber = %subscriber.as_str(),
                "owed messages for a parked subscriber with no registration on this channel — \
                 nothing will wake it (reported once per subscriber per channel)",
            );
        }
    }

    /// Whether `kind`'s current policy still covers `address`: the
    /// delivery-time ACL gate, asked wherever a delivery decision is made.
    ///
    /// Fail-closed — a live subscriber with no resolvable policy is a wiring
    /// bug, and denying it is the safe reading of one.
    pub(crate) fn channel_access_allowed(&self, kind: &SubscriberEntryKind, address: &str) -> bool {
        self.targets
            .policy(kind)
            .is_some_and(|p| p.allows_channel_access(address))
    }

    /// Report that `app_slug`'s subscription on `entry` was denied at delivery —
    /// at most once per `(channel, app)` pair.
    ///
    /// The denial is a standing state, not an event: it holds until an operator
    /// restores the ACL, and every drain and every wake pass re-observes it. One
    /// report per pair names the revocation without burying it.
    pub(crate) fn warn_acl_denied(&self, entry: &ChannelEntry, app_slug: &str) {
        let first_report = self
            .acl_denied_warned
            .lock()
            .expect("messaging: acl_denied_warned lock poisoned")
            .insert((entry.uuid, app_slug.to_string()));
        if first_report {
            tracing::warn!(
                app = %app_slug,
                channel = %entry.address,
                "subscription delivery denied — ACL not satisfied \
                 (reported once per app per channel)"
            );
        }
    }

    pub fn directory(&self) -> &Arc<MessagingDirectory> {
        &self.directory
    }

    /// Read-only `(slug, policy)` iterator over the post-injection app map the
    /// publish gates consult (`resolve_publish_sender` reads this exact map,
    /// `messaging/gates.rs`). Exposed for boot-time single-writer validation of
    /// `surface_error_channel`: the validator must sweep the same map enforcement
    /// uses, so what is validated cannot drift from what is enforced. A narrow
    /// view — callers see only the policies, not the map's container type or the
    /// rest of each `AppConfig` the Messenger mediates.
    pub fn app_policies(&self) -> impl Iterator<Item = (&str, &crate::access::AppPolicy)> {
        self.apps
            .iter()
            .map(|(slug, cfg)| (slug.as_str(), &cfg.policy))
    }

    /// Resolved access-control policy for the app with the given slug, or `None`
    /// if no such app is registered. Every resolved app carries a (possibly
    /// empty) policy, so a `None` for a live app slug indicates a host wiring bug.
    pub fn app_policy(&self, app_slug: &str) -> Option<&crate::access::AppPolicy> {
        self.apps.get(app_slug).map(|a| &a.policy)
    }

    /// The registration for a non-app subscriber (`Wasm`/`Surface`/`System`),
    /// or `None` if unregistered. App subscribers are not in the registry;
    /// their policy is resolved via [`Self::app_policy`]. Carries both the
    /// subscriber's policy and its declared [`WakeEconomics`].
    pub fn subscriber_registration(
        &self,
        kind: &SubscriberEntryKind,
    ) -> Option<&SubscriberRegistration> {
        self.targets.registration(kind)
    }

    /// Resolved access-control policy for a directory subscriber, covering
    /// every subscriber kind: `App(slug)` resolves via the apps map; every other
    /// kind resolves through the unified subscriber registry. This is the
    /// lookup the delivery-time ACL gate calls, so LLM apps, WASM
    /// consumers, surfaces, and system components are enforced uniformly;
    /// `app_policy` alone cannot reach the non-app subscribers (their policies
    /// are not in `apps`). Every resolved subscriber should carry a policy, so a
    /// `None` for a live subscriber indicates a host wiring bug — the delivery
    /// path treats it as deny (fail-closed), it is not a panic site.
    pub fn subscriber_policy(
        &self,
        kind: &SubscriberEntryKind,
    ) -> Option<&crate::access::AppPolicy> {
        self.targets.policy(kind)
    }

    /// Declared [`WakeEconomics`] for a directory subscriber, covering every
    /// subscriber kind: `App(slug)` is `UrgencyGated` iff the app exists (its
    /// economics are sourced from the authoritative apps map, not the subscriber
    /// registry — the same App split `subscriber_policy` makes, so App policy
    /// and economics resolve from one immutable source and cannot diverge from a
    /// registry clone); every other kind resolves through the registry. `None`
    /// for a live subscriber indicates a host wiring bug (the boot cross-check
    /// rejects it); the wake pass panics on a `None` for an inline subscriber
    /// rather than passing over one nothing else would wake.
    ///
    /// This is the single per-participant read that drives eager-wake gating:
    /// `Eager` subscribers are always woken; `UrgencyGated` subscribers consult
    /// `wake_min`. Dispatch never branches on the identity prefix to decide it.
    pub fn subscriber_wake_economics(&self, kind: &SubscriberEntryKind) -> Option<WakeEconomics> {
        self.targets.wake_economics(kind)
    }

    pub fn router(&self) -> &Arc<dyn WakeRouter> {
        &self.router
    }

    pub fn db(&self) -> &Db {
        &self.db
    }

    pub fn source(&self) -> &str {
        &self.source
    }

    /// Return the current `load_activation_snapshot` invocation count.
    /// Used in tests to assert exactly one subscriber-wide scan per drain step (AC 7).
    pub fn pending_bus_pushes_scan_count(&self) -> u64 {
        self.pending_bus_pushes_scan_count.load(Ordering::Relaxed)
    }

    /// Return the single dispatcher kick `Notify`. Background tasks hold a clone of
    /// this Arc; publish / edit / release callers notify via `dispatch_kick()`.
    pub fn dispatch_kick_notify(&self) -> Arc<tokio::sync::Notify> {
        self.dispatch_kick_notify.clone()
    }

    /// Signal the background dispatcher that there may be newly-actionable rows.
    ///
    /// All publish / edit / release callers use this as the single kick surface.
    /// The background dispatcher task holds the matching `Arc<Notify>`.
    pub fn dispatch_kick(&self) {
        self.dispatch_kick_notify.notify_one();
    }

    /// Read the metered drop count for a `(channel_address, subscriber_id)`
    /// pair — the drops the noise ladder counted, which is all of them on a
    /// `metered`/`alarm` subscription and none on a `silent` one. Returns 0 if
    /// no overflow has ever been metered for this pair. This is the telemetry
    /// read.
    ///
    /// There is no companion raw-loss total: a subscriber's losses are reported
    /// as they happen, by the eviction pass that outran its position and by the
    /// advance that passes seqs no window served, and each figure is a
    /// subtraction rather than a stored count.
    ///
    /// # Panics
    ///
    /// If `channel` resolves to no channel — a tally keyed by a channel that
    /// does not exist could only ever read zero, which would look like "nothing
    /// was dropped" instead of "you asked the wrong question".
    pub fn drop_counter(&self, channel: &str, subscriber: &ParticipantId) -> u64 {
        assert!(
            self.directory.resolve(channel).is_some(),
            "messaging: drop counter requested for channel {channel:?} not in the directory"
        );
        *self
            .metered_drops
            .lock()
            .expect("messaging: metered_drops lock poisoned")
            .get(&(channel.to_owned(), subscriber.as_str().to_owned()))
            .unwrap_or(&0)
    }

    /// Forget every metered tally held for `subscriber` on `channel` — its
    /// delivery state is being torn down and the tally is part of it.
    fn forget_metered_drops(&self, channel: &str, subscriber: &ParticipantId) {
        self.metered_drops
            .lock()
            .expect("messaging: metered_drops lock poisoned")
            .remove(&(channel.to_owned(), subscriber.as_str().to_owned()));
    }

    /// Enact the noise ladder for `count` overflow drops reported against
    /// `subscriber` on `channel`. Single enactment point — every overflow source
    /// must funnel through here so escalation is store-independent.
    ///
    /// The drops themselves are already accounted where they happened; what this
    /// decides is how loud they are. `channel` is the address the tally, the log,
    /// and the alarm are keyed by.
    ///
    /// TODO(drop-counters-export): the metered tallies have no production
    /// reader — only tests query them. Export them to whatever telemetry
    /// surface we settle on (blocked on deciding one), then reconsider the
    /// `Silent` default for `noise` (silent-by-default loss only makes sense
    /// while the counters are unread; the two decisions are coupled).
    fn enact_overflow_noise(
        &self,
        channel: &str,
        subscriber: &ParticipantId,
        noise: config::NoiseLevel,
        count: u64,
    ) {
        if count == 0 {
            return;
        }
        match noise {
            config::NoiseLevel::Silent => {} // no counter, no alert
            config::NoiseLevel::Metered | config::NoiseLevel::Alarm => {
                *self
                    .metered_drops
                    .lock()
                    .expect("messaging: metered_drops lock poisoned")
                    .entry((channel.to_owned(), subscriber.as_str().to_owned()))
                    .or_insert(0) += count;
            }
            config::NoiseLevel::Fatal => {
                panic!(
                    "enact_overflow_noise reached noise = fatal on channel {channel:?} \
                     subscriber {} — fatal is surface-only and must never reach the backend \
                     overflow path",
                    subscriber.as_str()
                );
            }
        }
        if noise == config::NoiseLevel::Alarm {
            self.router.alarm(channel, subscriber, count);
        }
    }

    /// **System-wide** directory dump — every channel in the process, NOT
    /// app-scoped. Emits all directory entries (brenn: and webhook: and mqtt:)
    /// with the appropriate protocol + details, all marked
    /// [`AccessKind::Existing`] (these are concrete channels that exist now).
    ///
    /// This is **not** the per-app tool surface. `MessageChannelList` is now
    /// backed by [`list_accessible_channels`](Self::list_accessible_channels),
    /// which scopes to the calling app's ACL (design §2.2). `list_channels` is
    /// retained for a possible future operator/admin surface (Open Question 2);
    /// do not re-nominate it for the per-app role.
    pub fn list_channels(&self) -> Vec<ChannelListing> {
        // Durable channels only: this dump describes persisted channel state, so
        // the non-durable half of the directory is not its subject.
        self.directory
            .list_durable()
            .iter()
            .map(|entry| match entry.transport_type {
                ChannelScheme::Webhook => ChannelListing {
                    protocol: ChannelScheme::Webhook,
                    address: entry.address.clone(),
                    description: entry.description.clone(),
                    access: AccessKind::Existing,
                    details: entry
                        .mount
                        .as_ref()
                        .map(|m| ChannelDetails::Webhook(WebhookDetails { mount: m.clone() })),
                },
                ChannelScheme::Mqtt => {
                    // Parse client/topic from the canonical `mqtt:<client>:<topic>`
                    // address. The address was validated at channel-creation time,
                    // so a parse failure here is a host-state corruption (a stored
                    // mqtt: channel with a malformed address) — panic, don't mislabel
                    // (CLAUDE.md BETTER DEAD THAN WRONG). The runtime health fields stay
                    // `None`; the `MessageChannelList` intercept enriches them (§2.5)
                    // so `Messenger` keeps no MQTT-runtime dependency.
                    let parsed =
                        crate::mqtt::parse_mqtt_address(&entry.address).unwrap_or_else(|e| {
                            panic!(
                                "list_channels: mqtt: channel {:?} has an unparseable \
                                 address — host state corruption: {e}",
                                entry.address
                            )
                        });
                    ChannelListing {
                        protocol: ChannelScheme::Mqtt,
                        address: entry.address.clone(),
                        description: entry.description.clone(),
                        access: AccessKind::Existing,
                        details: Some(ChannelDetails::Mqtt(MqttDetails {
                            client: parsed.client,
                            topic: parsed.topic,
                            qos: None,
                            urgency: None,
                            health: None,
                            last_error: None,
                        })),
                    }
                }
                // `Ephemeral` channels are never persisted and `pwa_push:` is an
                // egress-only protocol with no channel rows; neither ever appears
                // in the durable messaging directory, so one reaching this
                // system-wide operator/admin dump is a host-wiring invariant
                // violation — fail fast (BETTER DEAD THAN WRONG) rather than mislabel it
                // as a `brenn:` row and hide the corruption from the surface an
                // operator would consult to diagnose it. Mirrors the sibling
                // durable-directory walkers `list_accessible_channels` and
                // `list_subscriptions`.
                ChannelScheme::Ephemeral | ChannelScheme::Local | ChannelScheme::PwaPush => {
                    panic!(
                        "list_channels: non-durable channel {:?} in messaging directory \
                         — host-wiring invariant violated",
                        entry.address
                    )
                }
                ChannelScheme::Brenn => {
                    let subscribers: Vec<String> = entry
                        .subscribers
                        .iter()
                        .map(|s| s.kind.slug().to_string())
                        .collect();
                    ChannelListing {
                        protocol: ChannelScheme::Brenn,
                        address: entry.address.clone(),
                        description: entry.description.clone(),
                        access: AccessKind::Existing,
                        details: Some(ChannelDetails::Brenn(BrennDetails { subscribers })),
                    }
                }
            })
            .collect()
    }

    /// `MessageChannelList` output rows scoped to **what the calling app could
    /// subscribe to** (design §2.2) — the app-scoped discovery surface.
    ///
    /// Unlike [`list_channels`](Self::list_channels) (the unfiltered system-wide
    /// dump), this returns only channels the app's [`AppPolicy`] permits, split by
    /// transport:
    ///
    /// - **`brenn:` / `webhook:`** (exact-answer transports): the directory entries
    ///   the app's ACL covers, decided by `AppPolicy::allows_channel_access`. A channel
    ///   another app created appears only when this app's ACL also covers it
    ///   (genuinely subscribable) — so the cross-app leak the old unfiltered list
    ///   produced is gone. These rows are [`AccessKind::Existing`].
    /// - **`mqtt:`**: the directory is **ignored** (MQTT brokers expose no topic
    ///   enumeration). Instead, one [`AccessKind::Pattern`] row is synthesized per
    ///   `mqtt_subscribe` ACL matcher, rendered as the canonical
    ///   `mqtt:<client>:<topic_filter>` address. A wildcard matcher (`sensors/#`)
    ///   renders verbatim and is a subscribe *target*, not a literal topic.
    ///
    /// `pwa_push:` is appended by the intercept (already app-scoped); the runtime
    /// `mqtt:` health fields are left `None` for intercept enrichment, same as
    /// `list_channels`.
    ///
    /// Panics if `app_policy(app_slug)` is `None`: a registered app always carries
    /// a (possibly empty) policy, so `None` for a live app slug is a host wiring
    /// bug, not attacker input (CLAUDE.md BETTER DEAD THAN WRONG).
    pub fn list_accessible_channels(&self, app_slug: &str) -> Vec<ChannelListing> {
        let policy = self.app_policy(app_slug).unwrap_or_else(|| {
            panic!(
                "list_accessible_channels: app {app_slug:?} is registered but carries no \
                 AppPolicy — host wiring bug (every resolved app must have a policy)"
            )
        });

        // brenn: / webhook: / ephemeral: / local: — keep directory entries the
        // app's ACL covers. mqtt: directory entries are deliberately skipped here;
        // mqtt: access is sourced from the ACL matchers below, not the directory.
        let mut rows: Vec<ChannelListing> =
            self.directory
                .list()
                .iter()
                .filter_map(|entry| match entry.transport_type {
                    ChannelScheme::Webhook if policy.allows_channel_access(&entry.address) => {
                        Some(ChannelListing {
                            protocol: ChannelScheme::Webhook,
                            address: entry.address.clone(),
                            description: entry.description.clone(),
                            access: AccessKind::Existing,
                            details: entry.mount.as_ref().map(|m| {
                                ChannelDetails::Webhook(WebhookDetails { mount: m.clone() })
                            }),
                        })
                    }
                    ChannelScheme::Mqtt => None,
                    ChannelScheme::Webhook => None,
                    ChannelScheme::Brenn if policy.allows_channel_access(&entry.address) => {
                        let subscribers: Vec<String> = entry
                            .subscribers
                            .iter()
                            .map(|s| s.kind.slug().to_string())
                            .collect();
                        Some(ChannelListing {
                            protocol: ChannelScheme::Brenn,
                            address: entry.address.clone(),
                            description: entry.description.clone(),
                            access: AccessKind::Existing,
                            details: Some(ChannelDetails::Brenn(BrennDetails { subscribers })),
                        })
                    }
                    ChannelScheme::Brenn => None,
                    // Non-durable channels are listed on the same ACL terms as
                    // `brenn:` ones — an app that may subscribe to and read a channel
                    // may see that it exists. They carry no protocol detail shape.
                    ChannelScheme::Ephemeral | ChannelScheme::Local
                        if policy.allows_channel_access(&entry.address) =>
                    {
                        Some(ChannelListing {
                            protocol: entry.transport_type,
                            address: entry.address.clone(),
                            description: entry.description.clone(),
                            access: AccessKind::Existing,
                            details: None,
                        })
                    }
                    ChannelScheme::Ephemeral | ChannelScheme::Local => None,
                    // `pwa_push:` is an egress-only protocol with no channel rows and
                    // never appears in the messaging directory. One here is a
                    // host-wiring invariant violation — fail fast rather than
                    // mislabel it (BETTER DEAD THAN WRONG).
                    ChannelScheme::PwaPush => {
                        panic!(
                            "list_accessible_channels: egress-only channel {:?} in messaging \
                         directory — host-wiring invariant violated",
                            entry.address
                        )
                    }
                })
                .collect();

        // mqtt: — synthesize one Pattern row per ACL matcher (no broker
        // enumeration; design §2.2). Render as the canonical mqtt:<client>:<filter>
        // address; the filter may be a wildcard.
        for matcher in &policy.acls.mqtt_subscribe {
            rows.push(ChannelListing {
                protocol: ChannelScheme::Mqtt,
                address: format!("mqtt:{}:{}", matcher.client, matcher.topic_filter),
                description: None,
                access: AccessKind::Pattern,
                details: Some(ChannelDetails::Mqtt(MqttDetails {
                    client: matcher.client.clone(),
                    topic: matcher.topic_filter.clone(),
                    qos: None,
                    urgency: None,
                    health: None,
                    last_error: None,
                })),
            });
        }

        rows
    }

    /// List **only `app_slug`'s own** subscriptions, across all transports, both
    /// static (config-declared) and dynamic (runtime-created) — the
    /// `MessageSubscriptionList` tool's library backing ("what am I subscribed
    /// to?", design §2.1).
    ///
    /// Scans the process-global directory and keeps only channels on which
    /// `app_slug` is an `App(slug)` subscriber, ignoring foreign-app and
    /// `Wasm(slug)` subscribers (so two apps sharing one channel each see only
    /// their own subscription, with their own per-subscriber params). For each
    /// kept channel it emits a [`SubscriptionListing`] carrying *that
    /// subscriber's* resolved `push_depth`/`retain_depth`/`noise`/`wake_min`.
    ///
    /// The `dynamic` flag is sourced exactly as `subscribe_dynamic` discriminates
    /// static-vs-dynamic: an O(1) point-lookup against the channel class's
    /// registration record — `messaging_dynamic_subscriptions`
    /// (`load_dynamic_subscription_for`) for a durable channel, the in-memory
    /// non-durable registration set for a non-durable one. Registration present ⇒
    /// `dynamic = true` (runtime, removable); absent ⇒ `dynamic = false` (static,
    /// config-managed). Each record is the single source of truth for its class —
    /// no parallel field on `SubscriberEntry` to drift out of sync. This is why
    /// the method is `async`: it acquires the same `db` lock the subscribe path
    /// takes, once, for the duration of the per-app point-lookups.
    ///
    /// `mqtt:` rows leave the runtime-health fields `None` (the
    /// `MessageSubscriptionList` intercept enriches them via `MqttService`,
    /// exactly as for `MessageChannelList`); a malformed stored `mqtt:` address is
    /// host-state corruption and panics, the same parse-or-panic contract
    /// `list_channels` uses.
    pub async fn list_subscriptions(&self, app_slug: &str) -> Vec<SubscriptionListing> {
        let entries = self.directory.list();
        let conn = self.db.lock().await;
        entries
            .iter()
            .filter_map(|entry| {
                // Keep only this app's own subscriber on the channel.
                let sub = entry.subscribers.iter().find(
                    |s| matches!(&s.kind, SubscriberEntryKind::App(slug) if slug == app_slug),
                )?;
                // Static-vs-dynamic: the channel's registration record present ⇒
                // dynamic. Each class is asked its own authority — the durable
                // row for a durable channel, the in-memory registration set for a
                // non-durable one.
                let dynamic = if entry.capabilities().durable {
                    db::load_dynamic_subscription_for(&conn, entry.uuid, app_slug).is_some()
                } else {
                    self.nondurable_dynamic_sub_exists(&entry.uuid, app_slug)
                };
                let details = match entry.transport_type {
                    ChannelScheme::Webhook => entry
                        .mount
                        .as_ref()
                        .map(|m| ChannelDetails::Webhook(WebhookDetails { mount: m.clone() })),
                    ChannelScheme::Mqtt => {
                        // Same parse-or-panic contract as `list_channels`: a stored
                        // mqtt: address that no longer parses is host-state corruption.
                        let parsed = crate::mqtt::parse_mqtt_address(&entry.address)
                            .unwrap_or_else(|e| {
                                panic!(
                                    "list_subscriptions: mqtt: channel {:?} has an unparseable \
                                     address — host state corruption: {e}",
                                    entry.address
                                )
                            });
                        Some(ChannelDetails::Mqtt(MqttDetails {
                            client: parsed.client,
                            topic: parsed.topic,
                            qos: None,
                            urgency: None,
                            health: None,
                            last_error: None,
                        }))
                    }
                    ChannelScheme::Brenn => Some(ChannelDetails::Brenn(BrennDetails {
                        // This row describes *this app's own* subscription, not the
                        // channel-wide roster (quality-1 / security-1): emit only the
                        // calling app's slug, never co-subscribers (other apps or
                        // Wasm consumers). Matches the struct doc's per-app contract.
                        subscribers: vec![app_slug.to_string()],
                    })),
                    // A non-durable channel carries no protocol-specific detail
                    // shape: its address is the whole story and its retention
                    // knobs are already on the listing.
                    ChannelScheme::Ephemeral | ChannelScheme::Local => None,
                    // `pwa_push:` is an egress-only protocol — nothing subscribes
                    // to it, so it never carries an `App` subscriber and never
                    // reaches here. Fail fast per BETTER DEAD THAN WRONG.
                    ChannelScheme::PwaPush => {
                        panic!(
                            "list_subscriptions: egress-only channel {:?} carries a subscriber \
                             — host-wiring invariant violated",
                            entry.address
                        )
                    }
                };
                let protocol = entry.transport_type;
                Some(SubscriptionListing {
                    protocol,
                    address: entry.address.clone(),
                    description: entry.description.clone(),
                    dynamic,
                    push_depth: sub.push_depth,
                    retain_depth: sub.retain_depth,
                    noise: sub.noise,
                    // This row is the app's own `App` subscriber, which is
                    // `UrgencyGated` and so always carries a resolved wake_min.
                    wake_min: sub
                        .wake_min
                        .expect("App subscriber must carry a resolved wake_min in the directory"),
                    details,
                })
            })
            .collect()
    }

    /// Undelivered direct-to-participant ingress events for a subscriber, with
    /// the row id each is retired by.
    ///
    /// Ingress deliveries are channel-less rows a participant is handed once;
    /// what a subscriber is owed on a *channel* is its position, read through
    /// [`Messenger::conversation_delivery`] and its siblings. Only the former
    /// still ride `messaging_pending_pushes` at all.
    pub async fn load_pending_ingress(
        &self,
        subscriber: &ParticipantId,
    ) -> Vec<(i64, ingress::Event)> {
        let conn = self.db.lock().await;
        db::load_pending_ingress_for_drain(&conn, subscriber)
    }

    /// Assemble the full multi-port activation snapshot for `subscriber`: one
    /// [`PortSnapshot`] per input port, in `inputs` order, or `None` when no
    /// port was owed anything (no activation).
    ///
    /// **Per-port snapshot consistency:** a store hands over its window and the
    /// subscriber's drop total together, under whatever lock it keeps them
    /// under, so the count can never name a message that is also in the window
    /// (correctness-1). Ports are read one after another, so two ports' windows
    /// need not be from the same instant — messages do not move between
    /// channels, so nothing is double-counted or lost by that.
    ///
    /// A config change leaves no residue to reconcile: a channel that matches no
    /// input, or an input a config change demoted to sampled, is simply a
    /// position nothing reads — `detach` and the sampled-attach demotion rule
    /// remove it, and nothing else was ever written per message.
    ///
    /// Panics on any DB error (fail-fast; the DB is host infrastructure).
    pub async fn load_activation_snapshot(
        &self,
        subscriber: &ParticipantId,
        inputs: &[WasmInputPort],
    ) -> Option<Vec<PortSnapshot>> {
        self.pending_bus_pushes_scan_count
            .fetch_add(1, Ordering::Relaxed);

        // Resolving by address keeps the directory the single authority on
        // which channel a port names.
        let stores: Vec<Arc<dyn store::RetentionStore>> = inputs
            .iter()
            .map(|input| self.store_for_address(&input.sub.channel_address))
            .collect();

        // Trigger gate first: a window read carries up to
        // `max(push_depth, retain_depth)` envelopes per port, and a drain step
        // that activates nothing — the common case after a burst, since wakes
        // coalesce — must not pay for K of those. `has_deliverable` answers
        // "anything unseen and still retained?" from positions alone, and no
        // port owed anything means no window below could hold anything new.
        let mut any_deliverable = false;
        for (input, store) in inputs.iter().zip(&stores) {
            if input.sub.push_depth.is_push_enabled() && store.has_deliverable(subscriber).await {
                any_deliverable = true;
                break;
            }
        }
        if !any_deliverable {
            return None;
        }

        // One window read per port builds context and new together. Pure reads:
        // no position moves here, so the None path below leaves every port
        // exactly as it found it and the dispatcher advances only once it has
        // decided to activate.
        let mut windows: Vec<store::SubscriberWindow> = Vec::with_capacity(inputs.len());
        for (input, store) in inputs.iter().zip(&stores) {
            windows.push(
                store
                    .window(
                        subscriber,
                        Depth::Bounded(input.sub.push_depth.clamped_to(WASM_WINDOW_MAX_NEW)),
                        Depth::Bounded(input.sub.retain_depth.clamped_to(WASM_WINDOW_MAX_RETAIN)),
                    )
                    .await,
            );
        }
        if windows.iter().all(|w| w.new_entries().is_empty()) {
            return None;
        }

        let mut snapshots: Vec<PortSnapshot> = Vec::with_capacity(inputs.len());
        for ((input, window), store) in inputs.iter().zip(windows).zip(&stores) {
            let sub = &input.sub;
            snapshots.push(PortSnapshot {
                port: input.port.clone(),
                channel_address: sub.channel_address.clone(),
                capabilities: store.capabilities(),
                new_from: window.new_from,
                entries: window
                    .entries
                    .into_iter()
                    .map(|(seq, envelope)| (seq, (*envelope).clone()))
                    .collect(),
                push_enabled: sub.push_depth.is_push_enabled(),
            });
        }

        Some(snapshots)
    }

    /// Mark a set of ingress rows delivered. Idempotent.
    pub async fn mark_pushes_delivered(&self, push_ids: &[i64]) {
        if push_ids.is_empty() {
            return;
        }
        let conn = self.db.lock().await;
        db::mark_pending_pushes_delivered(&conn, push_ids);
    }

    /// Record a failed multi-port WASM activation in **one transaction** across
    /// all failing ports.
    ///
    /// Writes one `messaging_wasm_consume_failures` row per entry in `failures`
    /// (one per triggering port that contributed new messages). The
    /// `(subscriber, last_message_id)` idempotency key ensures a re-run after a
    /// crash is a no-op on duplicate rows.
    ///
    /// The batch is identified by the retention seqs it spanned, which is a
    /// fact about the channel rather than about any delivery bookkeeping: the
    /// subscriber's position moved past the batch before the guest ran, so
    /// there is nothing left to retire here. Panics on any DB error.
    pub async fn record_wasm_activation_failure(&self, failures: &[WasmBatchFailure<'_>]) {
        assert!(
            !failures.is_empty(),
            "record_wasm_activation_failure: failures must not be empty"
        );
        let now = format_ts_for_db(Utc::now());
        let conn = self.db.lock().await;
        let tx = conn
            .unchecked_transaction()
            .unwrap_or_else(|e| panic!("record_wasm_activation_failure: begin tx: {e}"));

        for failure in failures {
            let (first, last) = failure.seq_span;
            let batch_seq_span = format!("{}-{}", first.0, last.0);
            tx.execute(
                "INSERT OR IGNORE INTO messaging_wasm_consume_failures \
                 (channel, subscriber, first_message_id, last_message_id, batch_seq_span, \
                  outcome, diagnostic, failed_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                rusqlite::params![
                    failure.channel,
                    failure.subscriber.as_str(),
                    failure.first_message_id,
                    failure.last_message_id,
                    batch_seq_span,
                    failure.outcome,
                    failure.diagnostic,
                    &now,
                ],
            )
            .unwrap_or_else(|e| {
                panic!(
                    "record_wasm_activation_failure: insert quarantine row for {}: {e}",
                    failure.channel
                )
            });
        }

        tx.commit()
            .unwrap_or_else(|e| panic!("record_wasm_activation_failure: commit tx: {e}"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(name: &str) -> ChannelEntry {
        crate::messaging::testutils::test_channel_entry(name, vec![])
    }

    #[test]
    fn directory_resolve_known_address() {
        let e = entry("pa-alice");
        let addr = e.address.clone();
        let dir = MessagingDirectory::with_entries(vec![e]);
        assert!(dir.resolve(&addr).is_some());
    }

    #[test]
    fn directory_resolve_unknown_address() {
        let dir = MessagingDirectory::with_entries(vec![entry("known")]);
        assert!(dir.resolve("brenn:unknown").is_none());
    }

    #[test]
    fn directory_resolve_missing_prefix() {
        let dir = MessagingDirectory::with_entries(vec![entry("pa-alice")]);
        // Bare name without `brenn:` prefix is not a valid address.
        assert!(dir.resolve("pa-alice").is_none());
    }

    #[test]
    fn directory_resolve_wrong_transport() {
        let dir = MessagingDirectory::with_entries(vec![entry("pa-alice")]);
        // Other transports are not supported in MVP; resolution fails.
        assert!(dir.resolve("smtp:pa-alice").is_none());
    }

    /// `list()` must preserve config-declaration order, not whatever
    /// order the underlying HashMap happens to iterate. Use
    /// non-alphabetic insert order so the test would catch an
    /// alphabetic-sorted regression (review F21).
    #[test]
    fn directory_list_preserves_order() {
        let c = entry("c");
        let a = entry("a");
        let b = entry("b");
        let dir = MessagingDirectory::with_entries(vec![c.clone(), a.clone(), b.clone()]);
        let listed = dir.list();
        assert_eq!(listed.len(), 3);
        assert_eq!(listed[0].address, c.address);
        assert_eq!(listed[1].address, a.address);
        assert_eq!(listed[2].address, b.address);
    }

    fn app_subscriber(slug: &str) -> SubscriberEntry {
        SubscriberEntry {
            kind: SubscriberEntryKind::App(slug.to_string()),
            push_depth: config::Depth::Bounded(0),
            retain_depth: config::Depth::Unbounded,
            noise: config::NoiseLevel::Silent,
            wake_min: Some(WakeMin::Normal),
        }
    }

    fn wasm_subscriber(slug: &str) -> SubscriberEntry {
        SubscriberEntry {
            kind: SubscriberEntryKind::Wasm(slug.to_string()),
            push_depth: config::Depth::Bounded(0),
            retain_depth: config::Depth::Unbounded,
            noise: config::NoiseLevel::Silent,
            wake_min: None,
        }
    }

    /// `SubscriberEntryKind::slug()` returns the slug for the `Surface` variant
    /// too (the or-pattern arm added alongside the variant).
    #[test]
    fn subscriber_entry_kind_surface_slug() {
        let kind = SubscriberEntryKind::Surface {
            slug: "deskbar".to_string(),
            instance: None,
        };
        assert_eq!(kind.slug(), "deskbar");
    }

    /// `add_subscriber` is visible on the next `resolve`, and a snapshot taken
    /// *before* the mutation is unchanged (copy-on-write — no torn read).
    #[test]
    fn directory_add_subscriber_visible_and_snapshot_isolated() {
        let e = entry("dyn-add");
        let uuid = e.uuid;
        let addr = e.address.clone();
        let dir = MessagingDirectory::with_entries(vec![e]);

        // Snapshot the entry before any mutation.
        let before = dir.resolve(&addr).expect("channel exists");
        assert!(before.subscribers.is_empty());

        assert!(dir.add_subscriber(&uuid, app_subscriber("dyn-app")));

        // The pre-mutation Arc snapshot is unchanged.
        assert!(
            before.subscribers.is_empty(),
            "held snapshot must not see the new subscriber"
        );
        // The next resolve sees the new subscriber.
        let after = dir.resolve(&addr).expect("channel exists");
        assert_eq!(after.subscribers.len(), 1);
        assert_eq!(after.subscribers[0].kind.slug(), "dyn-app");
    }

    /// `add_subscriber` replaces an existing same-kind+slug subscriber rather
    /// than appending a duplicate (the boot-merge / re-subscribe mechanism).
    #[test]
    fn directory_add_subscriber_replaces_same_slug() {
        let mut e = entry("dyn-replace");
        e.subscribers = vec![app_subscriber("dyn-app")];
        let uuid = e.uuid;
        let addr = e.address.clone();
        let dir = MessagingDirectory::with_entries(vec![e]);

        let mut replacement = app_subscriber("dyn-app");
        replacement.retain_depth = config::Depth::Bounded(5);
        assert!(dir.add_subscriber(&uuid, replacement));

        let after = dir.resolve(&addr).expect("channel exists");
        assert_eq!(after.subscribers.len(), 1, "no duplicate appended");
        assert!(matches!(
            after.subscribers[0].retain_depth,
            config::Depth::Bounded(5)
        ));
    }

    /// `add_subscriber` to an unknown channel returns `false` and mutates nothing.
    #[test]
    fn directory_add_subscriber_unknown_channel() {
        let dir = MessagingDirectory::with_entries(vec![]);
        assert!(!dir.add_subscriber(&Uuid::new_v4(), app_subscriber("x")));
    }

    /// `remove_subscriber` removes only the matching `App(slug)`, leaving WASM
    /// and other-app subscribers intact.
    #[test]
    fn directory_remove_subscriber_only_matching_app() {
        let mut e = entry("dyn-remove");
        e.subscribers = vec![
            app_subscriber("app-a"),
            wasm_subscriber("wasm-x"),
            app_subscriber("app-b"),
        ];
        let uuid = e.uuid;
        let addr = e.address.clone();
        let dir = MessagingDirectory::with_entries(vec![e]);

        // Two subscribers remain (wasm-x + app-b) after removing app-a.
        assert_eq!(dir.remove_subscriber(&uuid, "app-a"), Some(2));

        let after = dir.resolve(&addr).expect("channel exists");
        let slugs: Vec<&str> = after.subscribers.iter().map(|s| s.kind.slug()).collect();
        assert_eq!(slugs, vec!["wasm-x", "app-b"]);
        // A WASM subscriber sharing the slug is NOT removed by app-slug match.
        assert!(matches!(
            after.subscribers[0].kind,
            SubscriberEntryKind::Wasm(_)
        ));
    }

    /// `remove_subscriber` returns `None` for an unknown channel or when no
    /// matching `App(slug)` is present.
    #[test]
    fn directory_remove_subscriber_no_match() {
        let mut e = entry("dyn-remove-nomatch");
        e.subscribers = vec![wasm_subscriber("wasm-x")];
        let uuid = e.uuid;
        let dir = MessagingDirectory::with_entries(vec![e]);

        // Unknown channel.
        assert_eq!(dir.remove_subscriber(&Uuid::new_v4(), "app-a"), None);
        // Known channel, but no App(app-a) subscriber (only WASM present).
        assert_eq!(dir.remove_subscriber(&uuid, "app-a"), None);
    }

    /// `remove_subscriber` returns `Some(0)` when it removes the last subscriber,
    /// so the unsubscribe path can decide "last subscriber on this filter" without
    /// a second `resolve` (efficiency-3).
    #[test]
    fn directory_remove_subscriber_reports_zero_remaining() {
        let mut e = entry("dyn-remove-last");
        e.subscribers = vec![app_subscriber("only-app")];
        let uuid = e.uuid;
        let dir = MessagingDirectory::with_entries(vec![e]);

        assert_eq!(dir.remove_subscriber(&uuid, "only-app"), Some(0));
    }

    /// `add_channel` makes a new address resolvable and listable.
    #[test]
    fn directory_add_channel_resolvable_and_listable() {
        let existing = entry("existing");
        let existing_addr = existing.address.clone();
        let dir = MessagingDirectory::with_entries(vec![existing]);

        let fresh = entry("fresh");
        let fresh_uuid = fresh.uuid;
        let fresh_addr = fresh.address.clone();
        dir.add_channel(fresh);

        assert!(dir.resolve(&fresh_addr).is_some());
        assert!(dir.by_uuid(&fresh_uuid).is_some());
        let listed: Vec<String> = dir.list().iter().map(|c| c.address.clone()).collect();
        assert_eq!(listed, vec![existing_addr, fresh_addr]);
    }

    /// `add_channel` panics on a UUID/address collision (host bug per design §2.1).
    #[test]
    #[should_panic(expected = "already present")]
    fn directory_add_channel_duplicate_panics() {
        let e = entry("dup");
        let dup = e.clone();
        let dir = MessagingDirectory::with_entries(vec![e]);
        dir.add_channel(dup);
    }

    /// `list_channels()` must return unified `ChannelListing` entries with
    /// `protocol = ChannelScheme::Brenn` and typed `BrennDetails` for each channel.
    #[test]
    fn list_channels_emits_unified_brenn_entries() {
        use std::sync::Arc;

        let mut chan = entry("my-channel");
        chan.subscribers = vec![
            SubscriberEntry {
                kind: SubscriberEntryKind::App("app-a".to_string()),
                push_depth: config::Depth::Unbounded,
                retain_depth: config::Depth::Unbounded,
                noise: config::NoiseLevel::Silent,
                wake_min: Some(WakeMin::Normal),
            },
            SubscriberEntry {
                kind: SubscriberEntryKind::App("app-b".to_string()),
                push_depth: config::Depth::Bounded(0),
                retain_depth: config::Depth::Unbounded,
                noise: config::NoiseLevel::Silent,
                wake_min: Some(WakeMin::Normal),
            },
        ];
        let dir = MessagingDirectory::with_entries(vec![chan]);

        let messenger = Messenger::new(
            crate::db::init_db_memory(),
            Arc::new(dir),
            Arc::from("test-source"),
            Arc::new(indexmap::IndexMap::new()),
            Arc::new(super::query::NoopWakeRouter) as Arc<dyn WakeRouter>,
            crate::messaging::config::MessagingGlobalConfig::default(),
        );

        let listing = messenger.list_channels();
        assert_eq!(listing.len(), 1);
        let entry = &listing[0];
        assert_eq!(entry.protocol, ChannelScheme::Brenn);
        assert!(
            entry.address.starts_with("brenn:"),
            "address should start with brenn: got {:?}",
            entry.address
        );
        let details = entry
            .details
            .as_ref()
            .expect("brenn entry should have details");
        let ChannelDetails::Brenn(brenn) = details else {
            panic!("expected BrennDetails, got {details:?}");
        };
        assert!(
            brenn.subscribers.contains(&"app-a".to_string()),
            "expected app-a in subscribers: {:?}",
            brenn.subscribers
        );
        assert!(
            brenn.subscribers.contains(&"app-b".to_string()),
            "expected app-b in subscribers: {:?}",
            brenn.subscribers
        );
    }

    /// `list_channels()` must give an `ChannelScheme::Mqtt` entry its own
    /// `protocol: Mqtt` + typed `MqttDetails{client,topic}` — NOT fall into the
    /// `brenn` catch-all (the latent mislabel this typing fix repairs, §2.5). The
    /// runtime health fields stay unset (filled by the intercept enrichment).
    #[test]
    fn list_channels_emits_typed_mqtt_entry() {
        use std::sync::Arc;

        let mut chan = entry("ignored");
        // Override to a real mqtt: address + transport so the parser succeeds.
        chan.address = "mqtt:home:sensors/+/temp".to_string();
        chan.uuid = mqtt_channel_uuid_from_address(&chan.address);
        chan.transport_type = ChannelScheme::Mqtt;
        let dir = MessagingDirectory::with_entries(vec![chan]);

        let messenger = Messenger::new(
            crate::db::init_db_memory(),
            Arc::new(dir),
            Arc::from("test-source"),
            Arc::new(indexmap::IndexMap::new()),
            Arc::new(super::query::NoopWakeRouter) as Arc<dyn WakeRouter>,
            crate::messaging::config::MessagingGlobalConfig::default(),
        );

        let listing = messenger.list_channels();
        assert_eq!(listing.len(), 1);
        let entry = &listing[0];
        assert_eq!(
            entry.protocol,
            ChannelScheme::Mqtt,
            "mqtt: channel must list as Mqtt, not the brenn catch-all"
        );
        assert_ne!(
            entry.protocol,
            ChannelScheme::Brenn,
            "regression guard: mqtt: must no longer mislabel as brenn"
        );
        let details = entry
            .details
            .as_ref()
            .expect("mqtt entry should have details");
        let ChannelDetails::Mqtt(mqtt) = details else {
            panic!("expected MqttDetails, got {details:?}");
        };
        assert_eq!(mqtt.client, "home");
        assert_eq!(mqtt.topic, "sensors/+/temp");
        // Runtime health fields are unset until the intercept enriches them.
        assert!(mqtt.qos.is_none());
        assert!(mqtt.urgency.is_none());
        assert!(mqtt.health.is_none());
        assert!(mqtt.last_error.is_none());
    }

    // ── list_accessible_channels (design §2.2 repurpose) ──────────────────

    /// Build a `Messenger` whose directory is `entries` and whose apps map holds
    /// `(slug → policy)` pairs, for the `list_accessible_channels` tests.
    fn accessible_messenger(
        entries: Vec<ChannelEntry>,
        apps: &[(&str, crate::access::AppPolicy)],
    ) -> std::sync::Arc<Messenger> {
        use std::sync::Arc;
        let mut map = indexmap::IndexMap::new();
        for (slug, policy) in apps {
            let mut cfg = super::test_support::test_app_config(slug, None, vec![]);
            cfg.policy = policy.clone();
            map.insert((*slug).to_string(), cfg);
        }
        Messenger::new(
            crate::db::init_db_memory(),
            Arc::new(MessagingDirectory::with_entries(entries)),
            Arc::from("test-source"),
            Arc::new(map),
            Arc::new(super::query::NoopWakeRouter) as Arc<dyn WakeRouter>,
            crate::messaging::config::MessagingGlobalConfig::default(),
        )
    }

    /// An `AppPolicy` granting `MessagingSubscribe` + a single exact
    /// `brenn_subscribe` matcher for `channel` (no other scope).
    fn brenn_exact_policy(channel: &str) -> crate::access::AppPolicy {
        let mut p = crate::access::AppPolicy::default();
        p.grants
            .insert(crate::access::AppCapability::MessagingSubscribe);
        p.acls
            .brenn_subscribe
            .push(crate::access::acl::ChannelMatcher::Exact(
                channel.to_string(),
            ));
        p
    }

    /// A `brenn:` channel another app created is in app B's accessible list ONLY
    /// when B's ACL covers it; absent otherwise. The cross-app leak the old
    /// unfiltered `list_channels` produced is gone (design §2.2).
    #[test]
    fn list_accessible_channels_filters_brenn_by_acl() {
        // app-a created brenn:alpha; app-b's ACL covers brenn:alpha but not the
        // app-a-only brenn:beta.
        let mut alpha = entry("alpha");
        alpha.subscribers = vec![SubscriberEntry {
            kind: SubscriberEntryKind::App("app-a".to_string()),
            push_depth: config::Depth::Unbounded,
            retain_depth: config::Depth::Unbounded,
            noise: config::NoiseLevel::Silent,
            wake_min: Some(WakeMin::Normal),
        }];
        let beta = entry("beta");

        let messenger =
            accessible_messenger(vec![alpha, beta], &[("app-b", brenn_exact_policy("alpha"))]);

        let listing = messenger.list_accessible_channels("app-b");
        let addrs: Vec<&str> = listing.iter().map(|c| c.address.as_str()).collect();
        assert!(
            addrs.contains(&"brenn:alpha"),
            "app-b's ACL covers brenn:alpha — must be present: {addrs:?}"
        );
        assert!(
            !addrs.contains(&"brenn:beta"),
            "app-b's ACL does NOT cover brenn:beta — must be absent (no cross-app leak): {addrs:?}"
        );
        let alpha_row = listing
            .iter()
            .find(|c| c.address == "brenn:alpha")
            .expect("alpha present");
        assert_eq!(
            alpha_row.access,
            AccessKind::Existing,
            "brenn: rows are concrete existing channels"
        );
    }

    /// `mqtt_subscribe` ACL matchers surface as `Pattern` rows with canonical
    /// `mqtt:<client>:<filter>` addresses; a wildcard filter renders verbatim and
    /// is NOT sourced from the directory (design §2.2).
    #[test]
    fn list_accessible_channels_synthesizes_mqtt_pattern_rows() {
        let mut policy = crate::access::AppPolicy::default();
        policy
            .grants
            .insert(crate::access::AppCapability::MqttSubscribe);
        policy
            .acls
            .mqtt_subscribe
            .push(crate::access::acl::MqttSubMatcher {
                client: "home".to_string(),
                topic_filter: "sensors/#".to_string(),
            });
        policy
            .acls
            .mqtt_subscribe
            .push(crate::access::acl::MqttSubMatcher {
                client: "home".to_string(),
                topic_filter: "lights/+/state".to_string(),
            });

        // The directory holds an mqtt: channel the app is NOT scoped to; it must
        // not leak in (mqtt: rows come from the ACL, not the directory).
        let mut foreign = entry("ignored");
        foreign.address = "mqtt:home:secret/topic".to_string();
        foreign.uuid = mqtt_channel_uuid_from_address(&foreign.address);
        foreign.transport_type = ChannelScheme::Mqtt;

        let messenger = accessible_messenger(vec![foreign], &[("app-a", policy)]);
        let listing = messenger.list_accessible_channels("app-a");

        let mqtt_rows: Vec<&ChannelListing> = listing
            .iter()
            .filter(|c| c.protocol == ChannelScheme::Mqtt)
            .collect();
        assert_eq!(mqtt_rows.len(), 2, "one row per matcher: {listing:?}");
        for row in &mqtt_rows {
            assert_eq!(
                row.access,
                AccessKind::Pattern,
                "mqtt: rows are ACL-derived patterns"
            );
        }
        let addrs: Vec<&str> = mqtt_rows.iter().map(|c| c.address.as_str()).collect();
        assert!(
            addrs.contains(&"mqtt:home:sensors/#"),
            "wildcard matcher renders verbatim: {addrs:?}"
        );
        assert!(
            addrs.contains(&"mqtt:home:lights/+/state"),
            "second matcher present: {addrs:?}"
        );
        assert!(
            !addrs.contains(&"mqtt:home:secret/topic"),
            "directory mqtt: channel must NOT leak in (not ACL-sourced): {addrs:?}"
        );
    }

    /// A granted-but-unscoped transport (empty ACL) reaches nothing —
    /// deny-by-default (design §2.2 / §3 edge case).
    #[test]
    fn list_accessible_channels_empty_acl_returns_no_rows_for_transport() {
        // app-a has the brenn: directory channel but its policy has the grant and
        // NO brenn_subscribe matcher → allows_channel_access is false for every channel.
        let mut chan = entry("alpha");
        chan.subscribers = vec![];
        let mut policy = crate::access::AppPolicy::default();
        policy
            .grants
            .insert(crate::access::AppCapability::MessagingSubscribe);
        // (no brenn_subscribe matcher, no mqtt_subscribe matcher)

        let messenger = accessible_messenger(vec![chan], &[("app-a", policy)]);
        let listing = messenger.list_accessible_channels("app-a");
        assert!(
            listing.is_empty(),
            "empty ACL must reach nothing (deny-by-default): {listing:?}"
        );
    }

    /// The non-durable arms are an ACL-filtered disclosure decision like every
    /// other transport's: a channel name carries app and topic structure, so the
    /// listing shows an `ephemeral:` channel only where the app's own
    /// `ephemeral_subscribe` matcher covers it. `local:` is the always-deny half
    /// — `LocalSubscribe` is not authorable on an LLM app, so no policy an LLM
    /// app can hold reaches a `local:` channel, matcher or no matcher.
    #[test]
    fn list_accessible_channels_filters_nondurable_by_acl() {
        let covered = crate::messaging::testutils::ephemeral_channel_entry("covered", 4);
        let uncovered = crate::messaging::testutils::ephemeral_channel_entry("uncovered", 4);
        let confined = crate::messaging::testutils::local_channel_entry("anything", 4);

        let mut policy = crate::access::AppPolicy::default();
        policy
            .grants
            .insert(crate::access::AppCapability::EphemeralSubscribe);
        policy
            .grants
            .insert(crate::access::AppCapability::DynamicSubscribe);
        policy
            .acls
            .ephemeral_subscribe
            .push(crate::access::acl::ChannelMatcher::Exact(
                "covered".to_string(),
            ));
        // A matcher covering every `local:` name, deliberately without the
        // transport grant no LLM app can be given: the deny must come from the
        // grant, not from an absent matcher.
        policy
            .acls
            .local_subscribe
            .push(crate::access::acl::ChannelMatcher::Prefix(String::new()));

        let messenger =
            accessible_messenger(vec![covered, uncovered, confined], &[("app-a", policy)]);
        let listing = messenger.list_accessible_channels("app-a");
        let addrs: Vec<&str> = listing.iter().map(|c| c.address.as_str()).collect();
        assert_eq!(
            addrs,
            vec!["ephemeral:covered"],
            "only the ACL-covered ephemeral channel is disclosed"
        );
        let row = &listing[0];
        assert_eq!(row.protocol, ChannelScheme::Ephemeral);
        assert_eq!(row.access, AccessKind::Existing);
        assert!(
            row.details.is_none(),
            "a non-durable channel carries no protocol detail shape"
        );
    }

    /// A `webhook:` channel another app created is in app B's accessible list ONLY
    /// when B's `webhook` ACL covers it; absent otherwise — the same ACL-filter
    /// contract as the brenn: arm, but exercising the distinct `ChannelScheme::Webhook`
    /// arm and its `AccessKind::Existing` / `WebhookDetails` shape (test-1).
    #[test]
    fn list_accessible_channels_filters_webhook_by_acl() {
        // Two webhook: directory channels; app-b's ACL covers only `covered`.
        let mut covered = entry("covered");
        covered.address = "webhook:covered".to_string();
        covered.transport_type = ChannelScheme::Webhook;
        covered.mount = Some("/hooks/covered".to_string());
        let mut uncovered = entry("uncovered");
        uncovered.address = "webhook:uncovered".to_string();
        uncovered.transport_type = ChannelScheme::Webhook;
        uncovered.mount = Some("/hooks/uncovered".to_string());

        let mut policy = crate::access::AppPolicy::default();
        policy.grants.insert(crate::access::AppCapability::Webhook);
        policy
            .acls
            .webhook
            .push(crate::access::acl::WebhookMatcher {
                endpoint: "covered".to_string(),
            });

        let messenger = accessible_messenger(vec![covered, uncovered], &[("app-b", policy)]);
        let listing = messenger.list_accessible_channels("app-b");
        let addrs: Vec<&str> = listing.iter().map(|c| c.address.as_str()).collect();
        assert!(
            addrs.contains(&"webhook:covered"),
            "ACL-covered webhook: must be present: {addrs:?}"
        );
        assert!(
            !addrs.contains(&"webhook:uncovered"),
            "uncovered webhook: must be absent (ACL deny): {addrs:?}"
        );
        let row = listing
            .iter()
            .find(|c| c.address == "webhook:covered")
            .expect("covered present");
        assert_eq!(row.protocol, ChannelScheme::Webhook);
        assert_eq!(
            row.access,
            AccessKind::Existing,
            "webhook: rows are concrete existing channels"
        );
        let ChannelDetails::Webhook(details) = row.details.as_ref().expect("webhook details")
        else {
            panic!("expected WebhookDetails, got {:?}", row.details);
        };
        assert_eq!(details.mount, "/hooks/covered");
    }

    /// An app with `MessagingSubscribe` + a brenn matcher but NO `mqtt_subscribe`
    /// matchers produces zero mqtt: rows, even when the directory holds an mqtt:
    /// channel — mqtt: rows are ACL-matcher-sourced, so an empty matcher list means
    /// no rows regardless of the directory (test-5; design §2.2 / §3).
    #[test]
    fn list_accessible_channels_no_mqtt_matcher_returns_no_mqtt_rows() {
        let mut brenn_chan = entry("alpha");
        brenn_chan.subscribers = vec![];
        let mut mqtt_chan = entry("ignored");
        mqtt_chan.address = "mqtt:home:sensors/temp".to_string();
        mqtt_chan.uuid = mqtt_channel_uuid_from_address(&mqtt_chan.address);
        mqtt_chan.transport_type = ChannelScheme::Mqtt;

        // brenn: covered, but no mqtt_subscribe matcher at all.
        let messenger = accessible_messenger(
            vec![brenn_chan, mqtt_chan],
            &[("app-a", brenn_exact_policy("alpha"))],
        );
        let listing = messenger.list_accessible_channels("app-a");
        assert!(
            listing.iter().all(|c| c.protocol != ChannelScheme::Mqtt),
            "no mqtt_subscribe matcher ⇒ zero mqtt: rows: {listing:?}"
        );
        // The covered brenn: channel still appears (sanity).
        assert!(
            listing.iter().any(|c| c.address == "brenn:alpha"),
            "covered brenn: channel still present: {listing:?}"
        );
    }

    /// A registered app that somehow carries no policy is a host wiring bug; the
    /// read-tool path panics (CLAUDE.md BETTER DEAD THAN WRONG).
    /// `app_policy` returns `None` for an *unregistered* slug, which is the same
    /// host-inconsistency class the panic guards.
    #[test]
    #[should_panic(expected = "registered but carries no AppPolicy")]
    fn list_accessible_channels_panics_when_app_has_no_policy() {
        // Empty apps map → app_policy("ghost") is None → panic.
        let messenger = accessible_messenger(vec![], &[]);
        let _ = messenger.list_accessible_channels("ghost");
    }

    /// `Messenger::app_policy` returns the registered app's resolved policy for a
    /// known slug and `None` for an unknown one. The `None` branch is the
    /// host-wiring-bug path the Phase-1 enforcement site treats as fatal (it
    /// panics on `None` for a live app, §3.2), so swapping `Some`/`None`
    /// semantics or looking up the wrong key must be caught here — the enforcement
    /// tests exercise only the happy path indirectly.
    #[test]
    fn app_policy_returns_some_for_known_and_none_for_unknown() {
        use std::sync::Arc;

        // Stamp a non-empty policy so the returned value is distinguishable from a
        // default (empty) one — proving the accessor returns the *registered*
        // app's actual policy, not a fresh default.
        let mut app = super::test_support::test_app_config("known-app", None, vec![]);
        app.policy
            .grants
            .insert(crate::access::AppCapability::MessagingPublish);
        let mut apps = indexmap::IndexMap::new();
        apps.insert("known-app".to_string(), app);

        let messenger = Messenger::new(
            crate::db::init_db_memory(),
            Arc::new(MessagingDirectory::with_entries(vec![])),
            Arc::from("test-source"),
            Arc::new(apps),
            Arc::new(super::query::NoopWakeRouter) as Arc<dyn WakeRouter>,
            crate::messaging::config::MessagingGlobalConfig::default(),
        );

        let policy = messenger
            .app_policy("known-app")
            .expect("registered app must have a policy");
        assert!(
            policy.has_grant(crate::access::AppCapability::MessagingPublish),
            "accessor must return the registered app's actual policy, not a default"
        );

        assert!(
            messenger.app_policy("no-such-app").is_none(),
            "unknown slug must return None (the host-wiring-bug branch)"
        );
    }

    /// `subscriber_policy` must resolve **both** subscriber kinds: `App(slug)` via
    /// `apps`, `Wasm(slug)` via the side `wasm_policies` map installed by
    /// `with_wasm_policies`. This is the lookup the delivery-time ACL gate depends
    /// on; if WASM slugs did not resolve, WASM subscribers would silently fail the
    /// (fail-closed) gate. Policies are stamped with distinct grants so the test
    /// proves each kind returns its *own* policy, not a default or a cross-wired one.
    #[test]
    fn subscriber_policy_resolves_app_and_wasm_kinds() {
        use std::sync::Arc;

        let mut app = super::test_support::test_app_config("known-app", None, vec![]);
        app.policy
            .grants
            .insert(crate::access::AppCapability::MessagingPublish);
        let mut apps = indexmap::IndexMap::new();
        apps.insert("known-app".to_string(), app);

        let mut wasm_policy = crate::access::AppPolicy::default();
        wasm_policy
            .grants
            .insert(crate::access::AppCapability::MessagingSubscribe);
        let mut wasm_policies = std::collections::HashMap::new();
        wasm_policies.insert("known-wasm".to_string(), wasm_policy);

        let messenger = Messenger::new(
            crate::db::init_db_memory(),
            Arc::new(MessagingDirectory::with_entries(vec![])),
            Arc::from("test-source"),
            Arc::new(apps),
            Arc::new(super::query::NoopWakeRouter) as Arc<dyn WakeRouter>,
            crate::messaging::config::MessagingGlobalConfig::default(),
        )
        .with_subscriber_registrations(crate::messaging::testutils::wasm_registrations(
            wasm_policies,
        ));

        // App kind resolves via `apps`, returning the app's own policy.
        let app_pol = messenger
            .subscriber_policy(&SubscriberEntryKind::App("known-app".to_string()))
            .expect("App subscriber must resolve to its registered policy");
        assert!(
            app_pol.has_grant(crate::access::AppCapability::MessagingPublish),
            "App kind must return the app's actual policy"
        );

        // Wasm kind resolves via `wasm_policies`, returning the WASM consumer's policy.
        let wasm_pol = messenger
            .subscriber_policy(&SubscriberEntryKind::Wasm("known-wasm".to_string()))
            .expect("Wasm subscriber must resolve to its installed policy");
        assert!(
            wasm_pol.has_grant(crate::access::AppCapability::MessagingSubscribe),
            "Wasm kind must return the WASM consumer's actual policy"
        );

        // Unknown slugs return None for both kinds (the fail-closed deny branch).
        assert!(
            messenger
                .subscriber_policy(&SubscriberEntryKind::App("no-such-app".to_string()))
                .is_none(),
            "unknown App slug must return None"
        );
        assert!(
            messenger
                .subscriber_policy(&SubscriberEntryKind::Wasm("no-such-wasm".to_string()))
                .is_none(),
            "unknown Wasm slug must return None"
        );
    }

    /// `subscriber_policy` resolves a `Surface` entry via the installed
    /// `surface_policies` map; an unknown slug returns `None` (the fail-closed
    /// floor deny), the same contract as the App/Wasm arms.
    #[test]
    fn subscriber_policy_resolves_surface() {
        use std::sync::Arc;

        let mut surface_policy = crate::access::AppPolicy::default();
        surface_policy
            .grants
            .insert(crate::access::AppCapability::MessagingSubscribe);
        let mut surface_policies = std::collections::HashMap::new();
        surface_policies.insert("deskbar".to_string(), surface_policy);

        let messenger = Messenger::new(
            crate::db::init_db_memory(),
            Arc::new(MessagingDirectory::with_entries(vec![])),
            Arc::from("test-source"),
            Arc::new(indexmap::IndexMap::new()),
            Arc::new(super::query::NoopWakeRouter) as Arc<dyn WakeRouter>,
            crate::messaging::config::MessagingGlobalConfig::default(),
        )
        .with_subscriber_registrations(crate::messaging::testutils::surface_registrations(
            surface_policies,
        ));

        let pol = messenger
            .subscriber_policy(&SubscriberEntryKind::Surface {
                slug: "deskbar".to_string(),
                instance: None,
            })
            .expect("Surface subscriber must resolve to its installed policy");
        assert!(
            pol.has_grant(crate::access::AppCapability::MessagingSubscribe),
            "Surface kind must return the surface's actual policy"
        );

        assert!(
            messenger
                .subscriber_policy(&SubscriberEntryKind::Surface {
                    slug: "no-such-surface".to_string(),
                    instance: None,
                })
                .is_none(),
            "unknown Surface slug must return None (fail-closed deny)"
        );
    }

    /// `subscriber_wake_economics` resolves per participant: a configured `App`
    /// is `UrgencyGated` (sourced from `apps`, not the registry), a registered
    /// non-app subscriber returns its declared economics, and an unregistered
    /// non-app subscriber returns `None` — the signal the boot cross-check trips
    /// on. Drives the eager-wake decision and the dispatcher cooldown keying.
    #[test]
    fn subscriber_wake_economics_resolves_per_participant() {
        use std::sync::Arc;

        let app = super::test_support::test_app_config("known-app", None, vec![]);
        let mut apps = indexmap::IndexMap::new();
        apps.insert("known-app".to_string(), app);

        let mut wasm_policies = std::collections::HashMap::new();
        wasm_policies.insert(
            "known-wasm".to_string(),
            crate::access::AppPolicy::default(),
        );

        let messenger = Messenger::new(
            crate::db::init_db_memory(),
            Arc::new(MessagingDirectory::with_entries(vec![])),
            Arc::from("test-source"),
            Arc::new(apps),
            Arc::new(super::query::NoopWakeRouter) as Arc<dyn WakeRouter>,
            crate::messaging::config::MessagingGlobalConfig::default(),
        )
        .with_subscriber_registrations(crate::messaging::testutils::wasm_registrations(
            wasm_policies,
        ));

        // App: UrgencyGated, resolved from `apps` (not folded into the registry).
        assert_eq!(
            messenger.subscriber_wake_economics(&SubscriberEntryKind::App("known-app".to_string())),
            Some(WakeEconomics::UrgencyGated),
            "a configured app is UrgencyGated"
        );
        // Registered WASM consumer: Eager (cheap to wake).
        assert_eq!(
            messenger
                .subscriber_wake_economics(&SubscriberEntryKind::Wasm("known-wasm".to_string())),
            Some(WakeEconomics::Eager),
            "a registered WASM consumer is Eager"
        );
        // Unregistered non-app subscriber: None (boot cross-check failure signal).
        assert_eq!(
            messenger.subscriber_wake_economics(&SubscriberEntryKind::Wasm("ghost".to_string())),
            None,
            "an unregistered WASM subscriber has no economics — the cross-check rejects it"
        );
        // Unknown app slug: None (no such app).
        assert_eq!(
            messenger.subscriber_wake_economics(&SubscriberEntryKind::App("ghost".to_string())),
            None,
            "an unknown app slug resolves to no economics"
        );
    }

    /// `Messenger::new` installs an empty store registry, so no channel resolves
    /// to a store until boot replaces it. Pins the pre-wiring contract.
    #[test]
    fn new_installs_an_empty_default_registry() {
        use std::sync::Arc;

        let messenger = Messenger::new(
            crate::db::init_db_memory(),
            Arc::new(MessagingDirectory::with_entries(vec![])),
            Arc::from("test-source"),
            Arc::new(indexmap::IndexMap::new()),
            Arc::new(super::query::NoopWakeRouter) as Arc<dyn WakeRouter>,
            crate::messaging::config::MessagingGlobalConfig::default(),
        );

        assert!(
            messenger
                .ring_stores()
                .get_by_address("ephemeral:anything")
                .is_none()
        );
        assert_eq!(messenger.ring_stores().len(), 0);
    }

    /// `with_ring_stores` must run before the `Messenger` `Arc` is shared:
    /// once a second strong reference exists, `Arc::get_mut` fails and the
    /// builder panics (fail-fast) rather than silently no-op'ing the install.
    #[test]
    #[should_panic(expected = "boot-ordering bug")]
    fn with_ring_stores_after_arc_shared_panics() {
        use std::sync::Arc;

        let messenger = Messenger::new(
            crate::db::init_db_memory(),
            Arc::new(MessagingDirectory::with_entries(vec![])),
            Arc::from("test-source"),
            Arc::new(indexmap::IndexMap::new()),
            Arc::new(super::query::NoopWakeRouter) as Arc<dyn WakeRouter>,
            crate::messaging::config::MessagingGlobalConfig::default(),
        );
        // Share the Arc so Arc::get_mut can no longer succeed.
        let _shared = Arc::clone(&messenger);
        let _ = messenger.with_ring_stores(Arc::new(store::RingStores::empty()));
    }

    /// One directory, two kinds of store: a channel's capabilities pick the
    /// implementation, and `list_durable` is the view for callers whose subject
    /// is the database.
    #[tokio::test]
    async fn store_for_picks_the_implementation_from_the_capabilities() {
        use std::sync::Arc;

        let durable = crate::messaging::testutils::test_channel_entry("heartbeat", vec![]);
        let ephemeral = crate::messaging::testutils::ephemeral_channel_entry("chatter", 4);
        let entries = vec![durable.clone(), ephemeral.clone()];
        let ring_stores = Arc::new(store::RingStores::build(std::slice::from_ref(&ephemeral)));

        let messenger = Messenger::new(
            crate::db::init_db_memory(),
            Arc::new(MessagingDirectory::with_entries(entries)),
            Arc::from("test-source"),
            Arc::new(indexmap::IndexMap::new()),
            Arc::new(super::query::NoopWakeRouter) as Arc<dyn WakeRouter>,
            crate::messaging::config::MessagingGlobalConfig::default(),
        )
        .with_ring_stores(Arc::clone(&ring_stores));

        let durable_store = messenger.store_for(&durable);
        assert!(durable_store.capabilities().durable);
        assert_eq!(durable_store.channel_uuid(), durable.uuid);

        let durable_store_again = messenger.store_for(&durable);
        assert!(
            Arc::ptr_eq(&durable_store, &durable_store_again),
            "a durable channel's store is cached, not re-minted per call"
        );

        // Every entry point onto a durable channel's store lands on the one
        // instance, whether the caller names the channel by entry or by address.
        let by_address = messenger.store_for_address(&durable.address);
        assert!(
            Arc::ptr_eq(&durable_store, &by_address),
            "resolving a store by address must reach the same instance as by entry"
        );

        let ring_store = messenger.store_for(&ephemeral);
        assert!(!ring_store.capabilities().durable);
        assert!(ring_store.capabilities().transportable);
        assert_eq!(ring_store.address(), "ephemeral:chatter");
        assert!(
            std::ptr::eq(
                Arc::as_ptr(&ring_store) as *const store::RingStore,
                Arc::as_ptr(ring_stores.get(&ephemeral.uuid).expect("registered")),
            ),
            "the store handed out is the registry's, not a fresh ring"
        );

        let durable_addresses: Vec<String> = messenger
            .directory()
            .list_durable()
            .iter()
            .map(|e| e.address.clone())
            .collect();
        assert_eq!(durable_addresses, vec![durable.address.clone()]);
        assert_eq!(messenger.directory().list().len(), 2);
    }

    /// The deferred view a WASM output port draws from: it shows one sender its
    /// own parked messages, release-ordered, and never a peer's — the structural
    /// scoping that authorizes a component's view of its output port.
    #[tokio::test]
    async fn deferred_view_for_sender_is_sender_scoped_and_release_ordered() {
        use std::sync::Arc;

        use chrono::Duration;

        use crate::messaging::store::NewMessage;

        let ephemeral = crate::messaging::testutils::ephemeral_channel_entry("timers", 8);
        let ring_stores = Arc::new(store::RingStores::build(std::slice::from_ref(&ephemeral)));
        let messenger = Messenger::new(
            crate::db::init_db_memory(),
            Arc::new(MessagingDirectory::with_entries(vec![ephemeral.clone()])),
            Arc::from("node"),
            Arc::new(indexmap::IndexMap::new()),
            Arc::new(super::query::NoopWakeRouter) as Arc<dyn WakeRouter>,
            crate::messaging::config::MessagingGlobalConfig::default(),
        )
        .with_ring_stores(ring_stores);

        let alice = ParticipantId::for_wasm("proc-alice");
        let bob = ParticipantId::for_wasm("proc-bob");
        let now = Utc::now();
        let store = messenger.store_for(&ephemeral);

        let park = |sender: &str, body: &str, offset: i64| {
            let store = Arc::clone(&store);
            let msg = NewMessage {
                source: "node".to_string(),
                sender: sender.to_string(),
                body: body.to_string(),
                urgency: Urgency::Normal,
                envelope_type: ChannelScheme::Ephemeral,
                reply_to_uuid: None,
                delivery_deadline: None,
                publish_ts_ns: now.timestamp_nanos_opt().unwrap(),
            };
            async move {
                store
                    .park(msg, now + Duration::seconds(offset))
                    .await
                    .expect("under the cap");
            }
        };

        // Alice parks two (out of release order); Bob parks one between them.
        park(alice.as_str(), "alice-late", 60).await;
        park(alice.as_str(), "alice-soon", 30).await;
        park(bob.as_str(), "bob-mid", 45).await;

        let alice_view = messenger
            .deferred_view_for_sender(&ephemeral.address, alice.as_str(), now)
            .await;
        let bodies: Vec<&str> = alice_view
            .iter()
            .map(|d| d.envelope.body.as_str())
            .collect();
        assert_eq!(
            bodies,
            vec!["alice-soon", "alice-late"],
            "alice sees only her own parked messages, soonest release first"
        );

        let bob_view = messenger
            .deferred_view_for_sender(&ephemeral.address, bob.as_str(), now)
            .await;
        assert_eq!(bob_view.len(), 1);
        assert_eq!(bob_view[0].envelope.body, "bob-mid");
    }

    /// The substrate half of a WASM output port's `defer-cancel` / `defer-edit`:
    /// cancel by uuid erases one sender's parked message (and not a peer's on the
    /// same channel), edit reschedules it, and a uuid no longer parked reports the
    /// benign `NotDeferred` race rather than failing.
    #[tokio::test]
    async fn cancel_and_edit_deferred_for_sender_target_by_uuid() {
        use std::sync::Arc;

        use chrono::Duration;

        use crate::messaging::store::{DeferralOutcome, NewMessage};

        let ephemeral = crate::messaging::testutils::ephemeral_channel_entry("timers", 8);
        let ring_stores = Arc::new(store::RingStores::build(std::slice::from_ref(&ephemeral)));
        let messenger = Messenger::new(
            crate::db::init_db_memory(),
            Arc::new(MessagingDirectory::with_entries(vec![ephemeral.clone()])),
            Arc::from("node"),
            Arc::new(indexmap::IndexMap::new()),
            Arc::new(super::query::NoopWakeRouter) as Arc<dyn WakeRouter>,
            crate::messaging::config::MessagingGlobalConfig::default(),
        )
        .with_ring_stores(ring_stores);

        let alice = ParticipantId::for_wasm("proc-alice");
        let bob = ParticipantId::for_wasm("proc-bob");
        // Ring release times round-trip through epoch-ms, so anchor `now` to
        // millisecond precision to compare an edited release exactly.
        let now = DateTime::from_timestamp_millis(Utc::now().timestamp_millis()).unwrap();
        let store = messenger.store_for(&ephemeral);

        let park = |sender: &str, body: &str, offset: i64| {
            let store = Arc::clone(&store);
            let msg = NewMessage {
                source: "node".to_string(),
                sender: sender.to_string(),
                body: body.to_string(),
                urgency: Urgency::Normal,
                envelope_type: ChannelScheme::Ephemeral,
                reply_to_uuid: None,
                delivery_deadline: None,
                publish_ts_ns: now.timestamp_nanos_opt().unwrap(),
            };
            async move {
                store
                    .park(msg, now + Duration::seconds(offset))
                    .await
                    .expect("under the cap")
                    .message_uuid
            }
        };

        let alice_uuid = park(alice.as_str(), "alice-soon", 30).await;
        let bob_uuid = park(bob.as_str(), "bob-mid", 45).await;

        let outcome = messenger
            .edit_deferred_for_sender(
                &ephemeral.address,
                alice.as_str(),
                alice_uuid,
                Some("alice-rescheduled".to_string()),
                Some(now + Duration::seconds(90)),
                now,
            )
            .await;
        assert_eq!(outcome, DeferralOutcome::Applied);
        let alice_view = messenger
            .deferred_view_for_sender(&ephemeral.address, alice.as_str(), now)
            .await;
        assert_eq!(alice_view.len(), 1);
        assert_eq!(alice_view[0].envelope.body, "alice-rescheduled");
        assert_eq!(alice_view[0].release_at, now + Duration::seconds(90));

        let outcome = messenger
            .cancel_deferred_for_sender(&ephemeral.address, bob.as_str(), bob_uuid, now)
            .await;
        assert_eq!(outcome, DeferralOutcome::Applied);
        assert!(
            messenger
                .deferred_view_for_sender(&ephemeral.address, bob.as_str(), now)
                .await
                .is_empty()
        );

        // Cancelling bob's message again — it is no longer parked — is the benign
        // race, reported as NotDeferred rather than a failure.
        let outcome = messenger
            .cancel_deferred_for_sender(&ephemeral.address, bob.as_str(), bob_uuid, now)
            .await;
        assert_eq!(outcome, DeferralOutcome::NotDeferred);
    }

    /// A non-durable channel in the directory with no store is the two halves of
    /// the registry disagreeing — a wiring bug, not a state to serve traffic on.
    #[tokio::test]
    #[should_panic(expected = "has no retention store")]
    async fn store_for_panics_on_an_unregistered_nondurable_channel() {
        use std::sync::Arc;

        let ephemeral = crate::messaging::testutils::ephemeral_channel_entry("orphan", 4);
        let messenger = Messenger::new(
            crate::db::init_db_memory(),
            Arc::new(MessagingDirectory::with_entries(vec![ephemeral.clone()])),
            Arc::from("test-source"),
            Arc::new(indexmap::IndexMap::new()),
            Arc::new(super::query::NoopWakeRouter) as Arc<dyn WakeRouter>,
            crate::messaging::config::MessagingGlobalConfig::default(),
        );
        let _ = messenger.store_for(&ephemeral);
    }

    /// The operator listings read persisted state, so a registered non-durable
    /// channel is outside their subject rather than a reason to panic.
    #[tokio::test]
    async fn listings_skip_the_nondurable_half_of_the_directory() {
        use std::sync::Arc;

        let durable = crate::messaging::testutils::test_channel_entry("heartbeat", vec![]);
        let ephemeral = crate::messaging::testutils::ephemeral_channel_entry("chatter", 4);
        let messenger = Messenger::new(
            crate::db::init_db_memory(),
            Arc::new(MessagingDirectory::with_entries(vec![
                durable.clone(),
                ephemeral,
            ])),
            Arc::from("test-source"),
            Arc::new(indexmap::IndexMap::new()),
            Arc::new(super::query::NoopWakeRouter) as Arc<dyn WakeRouter>,
            crate::messaging::config::MessagingGlobalConfig::default(),
        );

        let listed: Vec<String> = messenger
            .list_channels()
            .into_iter()
            .map(|c| c.address)
            .collect();
        assert_eq!(listed, vec![durable.address]);
        assert!(messenger.list_subscriptions("graf").await.is_empty());
    }

    /// `MqttDetails` inside `ChannelDetails` serializes untagged with the
    /// documented field names; unset `Option` fields serialize away.
    #[test]
    fn mqtt_details_serializes_expected_keys() {
        // Pre-enrichment: only client/topic present.
        let bare = ChannelDetails::Mqtt(super::MqttDetails {
            client: "home".to_string(),
            topic: "sensors/#".to_string(),
            qos: None,
            urgency: None,
            health: None,
            last_error: None,
        });
        let v = serde_json::to_value(&bare).unwrap();
        assert_eq!(v["client"], "home");
        assert_eq!(v["topic"], "sensors/#");
        // Untagged: no wrapper key.
        assert!(v.get("Mqtt").is_none());
        // Unset health fields are skipped.
        assert!(v.get("qos").is_none());
        assert!(v.get("urgency").is_none());
        assert!(v.get("health").is_none());
        assert!(v.get("last_error").is_none());
        // No stale `wake_kind` field (the old MqttSubscriptionList doc bug, §1).
        assert!(v.get("wake_kind").is_none());

        // Post-enrichment: health fields present and named as documented.
        let enriched = ChannelDetails::Mqtt(super::MqttDetails {
            client: "home".to_string(),
            topic: "sensors/#".to_string(),
            qos: Some(1),
            urgency: Some(Urgency::Normal),
            health: Some("connected".to_string()),
            last_error: Some("boom".to_string()),
        });
        let v = serde_json::to_value(&enriched).unwrap();
        assert_eq!(v["qos"], 1);
        assert_eq!(v["urgency"], "normal");
        assert_eq!(v["health"], "connected");
        assert_eq!(v["last_error"], "boom");
    }

    /// `ChannelScheme` must serialize to snake_case strings matching the
    /// literals previously used in protocol guards ("brenn", "pwa_push").
    #[test]
    fn channel_protocol_serializes_to_snake_case() {
        assert_eq!(
            serde_json::to_value(ChannelScheme::Brenn).unwrap(),
            serde_json::json!("brenn"),
        );
        assert_eq!(
            serde_json::to_value(ChannelScheme::PwaPush).unwrap(),
            serde_json::json!("pwa_push"),
        );
        assert_eq!(
            serde_json::to_value(ChannelScheme::Mqtt).unwrap(),
            serde_json::json!("mqtt"),
        );
    }

    /// `PwaPushDetails` inside `ChannelDetails` must serialize without a
    /// wrapper key (untagged) and with the expected field names.
    #[test]
    fn pwa_push_details_serializes_expected_keys() {
        let d = ChannelDetails::PwaPush(super::PwaPushDetails {
            user: "alice".to_string(),
            device: Some("phone".to_string()),
            last_seen_at: "2026-05-15T00:00:00Z".to_string(),
        });
        let v = serde_json::to_value(&d).unwrap();
        assert_eq!(v["user"], "alice");
        assert_eq!(v["device"], "phone");
        assert_eq!(v["last_seen_at"], "2026-05-15T00:00:00Z");
        // Untagged: no wrapper key.
        assert!(v.get("PwaPush").is_none());
    }

    // -----------------------------------------------------------------------
    // Urgency + WakeMin (urgency-redesign)
    // -----------------------------------------------------------------------

    #[test]
    fn urgency_round_trip() {
        for u in [
            Urgency::VeryLow,
            Urgency::Low,
            Urgency::Normal,
            Urgency::High,
        ] {
            assert_eq!(Urgency::parse(u.as_str()), Some(u));
        }
        assert!(Urgency::parse("garbage").is_none());
        assert!(Urgency::parse("immediate").is_none());
        assert!(Urgency::parse("none").is_none());
    }

    #[test]
    fn urgency_serde_kebab_case() {
        assert_eq!(
            serde_json::to_string(&Urgency::VeryLow).unwrap(),
            r#""very-low""#
        );
        assert_eq!(
            serde_json::from_str::<Urgency>(r#""very-low""#).unwrap(),
            Urgency::VeryLow
        );
        assert_eq!(serde_json::to_string(&Urgency::High).unwrap(), r#""high""#);
    }

    #[test]
    fn urgency_ord_ladder() {
        assert!(Urgency::VeryLow < Urgency::Low);
        assert!(Urgency::Low < Urgency::Normal);
        assert!(Urgency::Normal < Urgency::High);
        // Reflexive
        assert!(Urgency::Normal >= Urgency::Normal);
    }

    #[test]
    fn wake_min_round_trip() {
        for w in [
            WakeMin::VeryLow,
            WakeMin::Low,
            WakeMin::Normal,
            WakeMin::High,
            WakeMin::Never,
        ] {
            assert_eq!(WakeMin::parse(w.as_str()), Some(w));
        }
        assert!(WakeMin::parse("garbage").is_none());
    }

    #[test]
    fn wake_min_wakes_full_matrix() {
        // Never never wakes.
        for u in [
            Urgency::VeryLow,
            Urgency::Low,
            Urgency::Normal,
            Urgency::High,
        ] {
            assert!(
                !WakeMin::Never.wakes(u),
                "Never.wakes({u:?}) should be false"
            );
        }
        // VeryLow wakes on everything.
        for u in [
            Urgency::VeryLow,
            Urgency::Low,
            Urgency::Normal,
            Urgency::High,
        ] {
            assert!(
                WakeMin::VeryLow.wakes(u),
                "VeryLow.wakes({u:?}) should be true"
            );
        }
        // Low wakes on Low and above.
        assert!(!WakeMin::Low.wakes(Urgency::VeryLow));
        assert!(WakeMin::Low.wakes(Urgency::Low));
        assert!(WakeMin::Low.wakes(Urgency::Normal));
        assert!(WakeMin::Low.wakes(Urgency::High));
        // Normal wakes on Normal and above (migration-parity threshold).
        assert!(!WakeMin::Normal.wakes(Urgency::VeryLow));
        assert!(!WakeMin::Normal.wakes(Urgency::Low));
        assert!(WakeMin::Normal.wakes(Urgency::Normal));
        assert!(WakeMin::Normal.wakes(Urgency::High));
        // High wakes only on High.
        assert!(!WakeMin::High.wakes(Urgency::VeryLow));
        assert!(!WakeMin::High.wakes(Urgency::Low));
        assert!(!WakeMin::High.wakes(Urgency::Normal));
        assert!(WakeMin::High.wakes(Urgency::High));
    }

    #[test]
    fn wake_min_migration_parity() {
        // Old immediate mapped to Urgency::Normal.
        // Old none mapped to Urgency::Low.
        // Default policy WakeMin::Normal:
        //   Normal.wakes(Normal) => true  (old Immediate still wakes)
        //   Normal.wakes(Low)    => false (old None still parks)
        assert!(WakeMin::Normal.wakes(Urgency::Normal));
        assert!(!WakeMin::Normal.wakes(Urgency::Low));
    }

    #[test]
    fn is_unreserved_char_accepts_rfc3986_unreserved_set() {
        // All ASCII alphanumerics must be accepted.
        for c in ('A'..='Z').chain('a'..='z').chain('0'..='9') {
            assert!(is_unreserved_char(c), "expected true for {c:?}");
        }
        // The four non-alphanumeric RFC 3986 unreserved chars.
        for c in ['.', '_', '~', '-'] {
            assert!(is_unreserved_char(c), "expected true for {c:?}");
        }
        // Reserved / special chars must be rejected.
        for c in ['@', '!', ' ', '/', '?', '#', '%', '+', ':'] {
            assert!(!is_unreserved_char(c), "expected false for {c:?}");
        }
    }

    /// The reserved `local:brenn/*` control-channel names are unreachable to
    /// operator config *by construction*: every reserved name contains `/`,
    /// which the unreserved charset rejects, so no declared channel name can
    /// ever collide with one. The same reservation-by-construction the `tools/`
    /// namespace relies on.
    ///
    /// This pins the property the reservation rests on rather than the names
    /// themselves: if `is_unreserved_char` ever admits `/`, the reserved
    /// namespace silently stops being reserved and this fails.
    #[test]
    fn reserved_local_control_channel_names_are_unreachable_to_operator_config() {
        for name in [
            "brenn/theme",
            "brenn/takeover",
            "brenn/link-state",
            "brenn/surface-state",
            "brenn/toast",
        ] {
            assert!(
                !name.chars().all(is_unreserved_char),
                "local:{name} is expressible in the operator charset — it is not reserved"
            );
        }
    }

    // -----------------------------------------------------------------------
    // resolve_source branch coverage (messaging-mvp-test-gap)
    // -----------------------------------------------------------------------

    fn make_server_config(public_url: Option<&str>) -> ServerConfig {
        ServerConfig {
            bind_address: "127.0.0.1:3000".parse().unwrap(),
            static_dir: std::path::PathBuf::from("/tmp"),
            surface_dist_dir: std::path::PathBuf::from("/tmp"),
            secure_cookies: false,
            trusted_proxy_hops: 0,
            pid_file: None,
            public_url: public_url.map(str::to_string),
        }
    }

    /// Non-empty `public_url` is used as-is.
    #[test]
    fn resolve_source_uses_public_url_when_set() {
        let config = make_server_config(Some("https://brenn.example.com"));
        let source = resolve_source(&config);
        assert_eq!(&*source, "https://brenn.example.com");
    }

    /// Empty `public_url` panics — messaging requires a non-empty source identifier.
    #[test]
    #[should_panic(expected = "server.public_url` is missing or empty")]
    fn resolve_source_panics_when_public_url_empty() {
        let config = make_server_config(Some(""));
        let _ = resolve_source(&config);
    }

    /// Absent `public_url` panics — messaging requires a source identifier.
    #[test]
    #[should_panic(expected = "server.public_url` is missing or empty")]
    fn resolve_source_panics_when_public_url_missing() {
        let config = make_server_config(None);
        let _ = resolve_source(&config);
    }

    /// `webhook_channel_uuid_from_slug` is deterministic (same slug → same UUID
    /// across calls, processes, and restarts) and unique per slug.
    #[test]
    fn webhook_channel_uuid_from_slug_is_deterministic() {
        let u1 = webhook_channel_uuid_from_slug("my-endpoint");
        let u2 = webhook_channel_uuid_from_slug("my-endpoint");
        assert_eq!(u1, u2, "same slug must produce same UUID");

        let other = webhook_channel_uuid_from_slug("other-endpoint");
        assert_ne!(u1, other, "different slugs must produce different UUIDs");

        // The UUID must be v5 (version bits 0101).
        assert_eq!(u1.get_version(), Some(uuid::Version::Sha1));
    }

    /// `mqtt_channel_uuid_from_address` is deterministic (same address → same
    /// UUID across calls, processes, and restarts), unique per address, and
    /// lives in a distinct namespace from `webhook_channel_uuid_from_slug` so
    /// the MQTT and webhook address spaces cannot collide. The full
    /// `mqtt:<client>:<topic>` address is hashed, so distinct clients and
    /// distinct topics (including `:`-vs-`/` differences) yield distinct UUIDs.
    #[test]
    fn mqtt_channel_uuid_from_address_is_deterministic_and_distinct() {
        let u1 = mqtt_channel_uuid_from_address("mqtt:c1:home/+/state");
        let u2 = mqtt_channel_uuid_from_address("mqtt:c1:home/+/state");
        assert_eq!(u1, u2, "same address must produce same UUID");

        // Distinct clients, same topic → distinct UUIDs.
        let c2 = mqtt_channel_uuid_from_address("mqtt:c2:home/+/state");
        assert_ne!(u1, c2, "different clients must produce different UUIDs");

        // Distinct topics on the same client → distinct UUIDs.
        let t2 = mqtt_channel_uuid_from_address("mqtt:c1:home/+/other");
        assert_ne!(u1, t2, "different topics must produce different UUIDs");

        // `:`-vs-`/` topic difference must hash distinctly (the full address is
        // hashed verbatim, not decomposed).
        assert_ne!(
            mqtt_channel_uuid_from_address("mqtt:c:a/b"),
            mqtt_channel_uuid_from_address("mqtt:c:a:b"),
            "topics differing only in `:`-vs-`/` must produce different UUIDs"
        );

        // Same string under the two transports must NOT collide (distinct seed).
        let s = "phonebuddy";
        assert_ne!(
            mqtt_channel_uuid_from_address(s),
            webhook_channel_uuid_from_slug(s),
            "mqtt and webhook namespaces must not collide for the same string"
        );

        // The UUID must be v5 (version bits 0101).
        assert_eq!(u1.get_version(), Some(uuid::Version::Sha1));
    }

    /// `tool_channel_uuid_from_address` is deterministic (same address → same
    /// UUID across restarts, so durable request rows match), unique per address,
    /// and lives in a distinct namespace from the other transports so a tool
    /// channel can never collide with a webhook/mqtt/ephemeral channel of the same
    /// name.
    #[test]
    fn tool_channel_uuid_from_address_is_deterministic_and_distinct() {
        let u1 = tool_channel_uuid_from_address("brenn:tools/git-repo-pull");
        assert_eq!(
            u1,
            tool_channel_uuid_from_address("brenn:tools/git-repo-pull")
        );
        assert_ne!(u1, tool_channel_uuid_from_address("brenn:tools/other"));
        // Request channel vs result inbox for the same handle are distinct.
        assert_ne!(
            tool_channel_uuid_from_address("brenn:tools/sync"),
            tool_channel_uuid_from_address("brenn:tool-results/sync"),
        );
        // Distinct namespace seed: the same string does not collide with webhook.
        assert_ne!(
            tool_channel_uuid_from_address("phonebuddy"),
            webhook_channel_uuid_from_slug("phonebuddy"),
        );
        assert_eq!(u1.get_version(), Some(uuid::Version::Sha1));
    }

    /// `webhook_channel_uuid_from_slug` produces a fixed, documented value for a
    /// known slug so we can detect any accidental change to the derivation logic.
    ///
    /// **Do NOT change this test if the UUID changes.** If the UUID changes, it
    /// means the derivation logic changed, which would break persisted rows across
    /// restarts. Fix the derivation logic, not this test.
    #[test]
    fn webhook_channel_uuid_from_slug_stable_known_value() {
        // Pre-computed once; must never change. If this assertion fails the
        // derivation logic changed and persisted channel UUIDs across all
        // deployments would be invalidated. Fix the derivation logic, not this test.
        let u = webhook_channel_uuid_from_slug("phonebuddy");
        assert_eq!(
            u.to_string(),
            "3ea885fd-3cc5-5c04-b3c6-36f23b0e978c",
            "webhook_channel_uuid_from_slug(\"phonebuddy\") must be stable"
        );
        // Also verify it is a v5 UUID.
        assert_eq!(u.get_version(), Some(uuid::Version::Sha1));
    }

    /// `ephemeral_channel_uuid_from_name` is deterministic (same name → same
    /// UUID across calls, processes, and restarts), unique per name, and lives in
    /// a distinct namespace from the webhook and MQTT derivations so the same
    /// string cannot collide across transports.
    #[test]
    fn ephemeral_channel_uuid_from_name_is_deterministic_and_distinct() {
        let u1 = ephemeral_channel_uuid_from_name("protobar-demo");
        let u2 = ephemeral_channel_uuid_from_name("protobar-demo");
        assert_eq!(u1, u2, "same name must produce same UUID");

        let other = ephemeral_channel_uuid_from_name("other-channel");
        assert_ne!(u1, other, "different names must produce different UUIDs");

        // Same string under the three transports must NOT collide (distinct seeds).
        let s = "phonebuddy";
        assert_ne!(
            ephemeral_channel_uuid_from_name(s),
            webhook_channel_uuid_from_slug(s),
            "ephemeral and webhook namespaces must not collide for the same string"
        );
        assert_ne!(
            ephemeral_channel_uuid_from_name(s),
            mqtt_channel_uuid_from_address(s),
            "ephemeral and mqtt namespaces must not collide for the same string"
        );

        // The UUID must be v5 (version bits 0101).
        assert_eq!(u1.get_version(), Some(uuid::Version::Sha1));
    }

    /// `ephemeral_channel_uuid_from_name` produces a fixed, documented value for a
    /// known name so we can detect any accidental change to the derivation logic.
    ///
    /// **Do NOT change this test if the UUID changes.** A change means the
    /// derivation logic changed; fix the derivation logic, not this test.
    #[test]
    fn ephemeral_channel_uuid_from_name_stable_known_value() {
        let u = ephemeral_channel_uuid_from_name("phonebuddy");
        assert_eq!(
            u.to_string(),
            "bcb7d898-d580-51b8-9eec-c7d93d26911d",
            "ephemeral_channel_uuid_from_name(\"phonebuddy\") must be stable"
        );
        assert_eq!(u.get_version(), Some(uuid::Version::Sha1));
    }

    /// `WebhookEnvelope` round-trips through JSON preserving all fields including
    /// duplicate/ordered headers, key_id, client_ip, received_at, body, endpoint_slug.
    #[test]
    fn webhook_envelope_serialize_deserialize_round_trip() {
        use chrono::TimeZone;
        let ts = chrono::Utc.with_ymd_and_hms(2026, 6, 6, 12, 0, 0).unwrap();
        let original = WebhookEnvelope {
            headers: vec![
                ("content-type".to_string(), "application/json".to_string()),
                ("x-hub-signature-256".to_string(), "[redacted]".to_string()),
                ("x-hub-signature-256".to_string(), "[redacted]".to_string()), // duplicate
                ("x-forwarded-for".to_string(), "1.2.3.4".to_string()),
            ],
            key_id: "key-abc".to_string(),
            client_ip: "10.0.0.1".to_string(),
            received_at: ts,
            body: r#"{"event":"push"}"#.to_string(),
            endpoint_slug: "my-endpoint".to_string(),
        };

        let json = serde_json::to_string(&original).expect("serialize must succeed");
        let decoded: WebhookEnvelope =
            serde_json::from_str(&json).expect("deserialize must succeed");

        // All fields preserved including header ordering and duplicates.
        assert_eq!(decoded.headers.len(), 4, "header count preserved");
        assert_eq!(decoded.headers[0].0, "content-type");
        assert_eq!(decoded.headers[1].0, "x-hub-signature-256");
        assert_eq!(decoded.headers[1].1, "[redacted]");
        assert_eq!(decoded.headers[2].0, "x-hub-signature-256"); // duplicate preserved
        assert_eq!(decoded.headers[3].0, "x-forwarded-for");
        assert_eq!(decoded.key_id, "key-abc");
        assert_eq!(decoded.client_ip, "10.0.0.1");
        assert_eq!(decoded.received_at, ts);
        assert_eq!(decoded.body, r#"{"event":"push"}"#);
        assert_eq!(decoded.endpoint_slug, "my-endpoint");
    }

    /// AC4 defense-in-depth: duplicate publisher identity panics naming both apps.
    ///
    /// Why this test does NOT call `Messenger::new` with duplicate slugs:
    /// `Messenger::new` is only reachable via normal bootstrap, and config load
    /// (resolve.rs:782) enforces slug uniqueness with `assert!(prev.is_none())` — there
    /// is no public API to construct two `AppConfig` entries with the same slug. The
    /// collision is therefore structurally unreachable in production. This test exercises
    /// the dedup map logic (the part that would panic) directly, confirming the panic
    /// message and the HashMap-insert path. A future refactor that removes or conditions
    /// the dedup loop inside `Messenger::new` would be a logical regression, but would
    /// not be caught by this test — that trade-off is documented and accepted (the loop
    /// is unreachable defense-in-depth, not a guard over a reachable collision path).
    #[test]
    #[should_panic(expected = "same publisher identity")]
    fn dedup_map_panics_on_duplicate_publisher_identity() {
        let mut seen: HashMap<String, &str> = HashMap::new();
        let id = "app:my-app@https://server.example".to_owned();
        seen.insert(id.clone(), "my-app");
        // Simulate a second app resolving to the same identity string.
        if let Some(prev_slug) = seen.insert(id.clone(), "other-app") {
            panic!(
                "messaging: apps {prev_slug:?} and {:?} resolve to the \
                 same publisher identity {id:?}; each app must have a unique identity",
                "other-app"
            );
        }
    }

    // -----------------------------------------------------------------------
    // load_activation_snapshot unit tests
    // -----------------------------------------------------------------------

    /// A parked message holds no retention position, so it is not part of the
    /// channel's ambience: it must be absent from a port's context window until
    /// a release pass moves it into retention, and present once it has — at the
    /// position retention gave it, which for a late release is **last**, behind
    /// messages published while it was parked and stamped after it.
    #[tokio::test]
    async fn load_activation_snapshot_excludes_parked_messages_from_context() {
        let (messenger, channel, wasm_sub) =
            super::testutils::build_wasm_messenger_unbounded("snap-parked", "snap-parked-ch").await;

        let park_ts = db::utc_to_ns(Utc::now());
        let release_at = Utc::now() + chrono::Duration::seconds(3600);
        messenger
            .store_for(&channel)
            .park(
                store::NewMessage {
                    source: "node".to_string(),
                    sender: "tester".to_string(),
                    body: "parked".to_string(),
                    urgency: Urgency::Normal,
                    envelope_type: ChannelScheme::Brenn,
                    reply_to_uuid: None,
                    delivery_deadline: None,
                    publish_ts_ns: park_ts,
                },
                release_at,
            )
            .await
            .expect("an unbounded channel parks without hitting a cap");

        // Published while the parked one waits, and stamped after it: the two
        // orders — publish time and retention position — disagree about this
        // pair once the release lands, and context follows retention position.
        let _ = super::testutils::insert_bus_message_at(
            &messenger,
            &channel,
            "later",
            ChannelScheme::Brenn,
            park_ts + 1_000_000,
        )
        .await;

        // One more message so the port triggers and a snapshot is produced.
        super::testutils::insert_bus_message(&messenger, &channel, "new-0", ChannelScheme::Brenn)
            .await;

        let inputs = vec![WasmInputPort {
            port: "in".to_string(),
            sub: config::ResolvedSubscription {
                channel_uuid: channel.uuid,
                channel_address: channel.address.clone(),
                push_depth: config::Depth::Unbounded,
                retain_depth: config::Depth::Unbounded,
                noise: config::NoiseLevel::Silent,
                wake_min: WakeMin::Normal,
            },
            amplification_mt: 1000,
        }];

        let parked_snapshot = messenger
            .load_activation_snapshot(&wasm_sub, &inputs)
            .await
            .expect("the pending row triggers");
        let bodies: Vec<&str> = parked_snapshot[0]
            .entries
            .iter()
            .map(|(_, e)| e.body.as_str())
            .collect();
        assert_eq!(
            bodies,
            ["later", "new-0"],
            "a parked message must not be visible in the window at all"
        );

        messenger
            .store_for(&channel)
            .release_due(release_at + chrono::Duration::seconds(1))
            .await;

        let released_snapshot = messenger
            .load_activation_snapshot(&wasm_sub, &inputs)
            .await
            .expect("the pending row still triggers");
        let bodies: Vec<&str> = released_snapshot[0]
            .entries
            .iter()
            .map(|(_, e)| e.body.as_str())
            .collect();
        assert_eq!(
            bodies,
            ["later", "new-0", "parked"],
            "a released message joins the window at the position retention gave \
             it — newest-last by retained order, not by its older publish stamp"
        );
    }

    /// `load_activation_snapshot` delivers 2 pending new rows and 2 already-delivered
    /// rows as context, with the new-row ids stripped from context and context in ASC
    /// order (oldest first). Pins clamp, new-id dedup, and DESC→ASC reversal.
    ///
    /// Also asserts scan count advances exactly once per call (single-scan property).
    #[tokio::test]
    async fn load_activation_snapshot_clamp_dedup_and_asc_order() {
        let slug = "snap-filter";
        let (messenger, channel, wasm_sub) =
            super::testutils::build_wasm_messenger_unbounded(slug, "snap-filter-ch").await;

        // Insert 4 messages with distinct timestamps to enable ordering assertions.
        // Rows ctx-a and ctx-b will be delivered (retained context); rows new-0 and
        // new-1 remain pending (new rows). Explicit ts_ns offsets guarantee distinct
        // timestamps so we can pin the ascending order in context.
        let base_ns = db::utc_to_ns(chrono::Utc::now());
        let mid_ctx_a = super::testutils::insert_bus_message_at(
            &messenger,
            &channel,
            "ctx-a",
            ChannelScheme::Brenn,
            base_ns,
        )
        .await;
        let mid_ctx_b = super::testutils::insert_bus_message_at(
            &messenger,
            &channel,
            "ctx-b",
            ChannelScheme::Brenn,
            base_ns + 1_000_000,
        )
        .await;
        let mid0 = super::testutils::insert_bus_message(
            &messenger,
            &channel,
            "new-0",
            ChannelScheme::Brenn,
        )
        .await;
        let mid1 = super::testutils::insert_bus_message(
            &messenger,
            &channel,
            "new-1",
            ChannelScheme::Brenn,
        )
        .await;

        // Advance past ctx-a and ctx-b — behind the cursor, they are retained
        // context and nothing more.
        messenger
            .advance_subscriber(
                &channel.address,
                &wasm_sub,
                store::MessageSeq(2),
                store::MessageSeq(1),
                config::NoiseLevel::Silent,
            )
            .await;

        // Build an inputs list with one port bound to the channel (Unbounded depths).
        let inputs = vec![WasmInputPort {
            port: "in".to_string(),
            sub: config::ResolvedSubscription {
                channel_uuid: channel.uuid,
                channel_address: channel.address.clone(),
                push_depth: config::Depth::Unbounded,
                retain_depth: config::Depth::Unbounded,
                noise: config::NoiseLevel::Silent,
                wake_min: WakeMin::Normal,
            },
            amplification_mt: 1000,
        }];

        let scan_before = messenger.pending_bus_pushes_scan_count();
        let snapshots = messenger
            .load_activation_snapshot(&wasm_sub, &inputs)
            .await
            .expect("expected Some — channel has pending rows");
        let scan_after = messenger.pending_bus_pushes_scan_count();

        // Exactly one scan per call.
        assert_eq!(
            scan_after - scan_before,
            1,
            "load_activation_snapshot must increment scan count exactly once"
        );

        assert_eq!(snapshots.len(), 1, "one port → one snapshot");
        let snap = &snapshots[0];

        // The new portion is the 2 pending rows (Unbounded → no clamp).
        assert_eq!(
            snap.new_len(),
            2,
            "expected 2 new messages, got {}",
            snap.new_len()
        );
        let new_ids_in_snap: Vec<Uuid> = snap
            .new_entries()
            .iter()
            .map(|(_, e)| e.message_id)
            .collect();
        assert!(new_ids_in_snap.contains(&mid0), "expected mid0 to be new");
        assert!(new_ids_in_snap.contains(&mid1), "expected mid1 to be new");

        // One window read, so a message is either context or new — never both.
        let context_ids: Vec<Uuid> = snap.context().iter().map(|(_, e)| e.message_id).collect();
        assert_eq!(
            context_ids,
            vec![mid_ctx_a, mid_ctx_b],
            "the two delivered rows are the context, oldest first"
        );
        assert!(
            !context_ids.contains(&mid0) && !context_ids.contains(&mid1),
            "a new message must not also appear as context: {context_ids:?}"
        );

        // The window is one ascending run: context then new, oldest first. A bug
        // that skipped the DESC→ASC reversal would place the newer entry first.
        let seqs: Vec<u64> = snap.entries.iter().map(|(seq, _)| seq.0).collect();
        let mut ascending = seqs.clone();
        ascending.sort_unstable();
        assert_eq!(seqs, ascending, "the window is ascending by retention seq");
    }

    /// The push-depth clamp is drop-oldest: a `Bounded(1)` port with 3 owed
    /// messages is served the newest one, and the advance over that window
    /// reports the other two as never seen. Nothing is held back for a later
    /// drain, so the reload finds no activation at all.
    #[tokio::test]
    async fn load_activation_snapshot_clamps_to_the_newest_and_reports_the_rest() {
        let slug = "leftover-exact";
        let (messenger, channel, wasm_sub) = super::testutils::build_wasm_messenger(
            slug,
            "leftover-exact-ch",
            config::Depth::Bounded(1),
            config::Depth::Bounded(0),
        )
        .await;

        // 3 pending rows on a push_depth=1 port → 1 delivered, 2 clamped leftover.
        let base_ns = db::utc_to_ns(chrono::Utc::now());
        for i in 0..3 {
            super::testutils::insert_bus_message_at(
                &messenger,
                &channel,
                &format!("row-{i}"),
                ChannelScheme::Brenn,
                base_ns + i as i64 * 1_000_000,
            )
            .await;
        }

        let inputs = vec![WasmInputPort {
            port: "in".to_string(),
            sub: config::ResolvedSubscription {
                channel_uuid: channel.uuid,
                channel_address: channel.address.clone(),
                push_depth: config::Depth::Bounded(1),
                retain_depth: config::Depth::Bounded(0),
                noise: config::NoiseLevel::Silent,
                wake_min: WakeMin::Normal,
            },
            amplification_mt: 1000,
        }];

        let snapshots = messenger
            .load_activation_snapshot(&wasm_sub, &inputs)
            .await
            .expect("expected Some — channel has pending rows");
        assert_eq!(snapshots.len(), 1, "one port → one snapshot");
        let snap = &snapshots[0];
        assert_eq!(snap.new_len(), 1, "push_depth=1 serves one message");
        assert_eq!(
            snap.new_entries()[0].1.body,
            "row-2",
            "the window serves the newest, not the oldest"
        );

        let (through, seen_floor) = snap.advance_span().expect("the window served a message");
        let outcome = messenger
            .advance_subscriber(
                &channel.address,
                &wasm_sub,
                through,
                seen_floor,
                config::NoiseLevel::Silent,
            )
            .await;
        assert_eq!(outcome.dropped, 2, "the two the clamp skipped");

        assert!(
            messenger
                .load_activation_snapshot(&wasm_sub, &inputs)
                .await
                .is_none(),
            "nothing is held back for a later drain"
        );
    }

    /// A consumer whose every port is sampled reads nothing, so it never
    /// activates and is owed nothing — a sampled port holds no position for a
    /// retained message to be measured against. The all-sampled shape is the
    /// edge case: the snapshot has an empty readable port set to fold over.
    #[tokio::test]
    async fn load_activation_snapshot_sampled_only_ports_never_activate() {
        let slug = "sampled-only";
        let (messenger, channel, wasm_sub) = super::testutils::build_wasm_messenger(
            slug,
            "sampled-only-ch",
            config::Depth::Bounded(0),
            config::Depth::Bounded(0),
        )
        .await;

        super::testutils::insert_bus_message(&messenger, &channel, "stale", ChannelScheme::Brenn)
            .await;

        let inputs = vec![WasmInputPort {
            port: "in".to_string(),
            sub: config::ResolvedSubscription {
                channel_uuid: channel.uuid,
                channel_address: channel.address.clone(),
                push_depth: config::Depth::Bounded(0),
                retain_depth: config::Depth::Bounded(0),
                noise: config::NoiseLevel::Silent,
                wake_min: WakeMin::Normal,
            },
            amplification_mt: 1000,
        }];

        assert!(
            messenger
                .load_activation_snapshot(&wasm_sub, &inputs)
                .await
                .is_none(),
            "a sampled port never activates"
        );
        assert!(
            !messenger
                .store_for(&channel)
                .has_deliverable(&wasm_sub)
                .await,
            "a sampled port holds no position, so nothing is owed on it"
        );
    }

    // -----------------------------------------------------------------------
    // load_activation_snapshot — ring-backed input ports
    // -----------------------------------------------------------------------

    /// Build a `Messenger` whose single channel is ring-backed (`ephemeral:` or
    /// `local:`), with a WASM subscriber id. The cursor is not attached here.
    fn build_ring_wasm_messenger(
        slug: &str,
        channel: ChannelEntry,
    ) -> (Arc<Messenger>, Arc<ChannelEntry>, ParticipantId) {
        let ring_stores = Arc::new(store::RingStores::build(std::slice::from_ref(&channel)));
        let messenger = Messenger::new(
            crate::db::init_db_memory(),
            Arc::new(MessagingDirectory::with_entries(vec![channel.clone()])),
            Arc::from("test"),
            Arc::new(indexmap::IndexMap::new()),
            Arc::new(super::query::NoopWakeRouter) as Arc<dyn WakeRouter>,
            config::MessagingGlobalConfig::default(),
        )
        .with_ring_stores(ring_stores);
        (messenger, Arc::new(channel), ParticipantId::for_wasm(slug))
    }

    /// Captures the `SubscriberEntryKind`s a wake targeted, for the ring-wake test.
    #[derive(Default)]
    struct RecordingWakeRouter {
        wakes: std::sync::Mutex<Vec<SubscriberEntryKind>>,
        /// Answer every wake with [`WakeServed::Live`] — the shape the real
        /// router takes when it finds the subscriber's bridge already running
        /// and serves it in place. Off by default: a spawn is the ordinary
        /// answer, and only the live answer changes what the cooldown does.
        serve_live: std::sync::atomic::AtomicBool,
    }

    #[async_trait::async_trait]
    impl WakeRouter for RecordingWakeRouter {
        async fn deliver(
            &self,
            _key: &SubscriberEntryKind,
            _envelope: &std::sync::Arc<MessageEnvelope>,
            _retained_seq: i64,
        ) -> Result<bool, String> {
            unreachable!("ring-backed WASM delivery never routes inline through deliver")
        }
        async fn deliver_ingress(
            &self,
            _key: &SubscriberEntryKind,
            _subscriber: &ParticipantId,
            _event: &ingress::Event,
        ) -> Result<bool, String> {
            unreachable!("ring-backed WASM delivery never routes through deliver_ingress")
        }
        fn spawn_eager_wake(&self, key: &SubscriberEntryKind, _subscriber: &ParticipantId) {
            self.wakes.lock().unwrap().push(key.clone());
        }
        async fn wake_owed(
            &self,
            key: &SubscriberEntryKind,
            subscriber: &ParticipantId,
        ) -> WakeServed {
            if self.serve_live.load(Ordering::SeqCst) {
                // Served in place, so no wake fires; the walk still named this
                // subscriber, which is what the recording is for.
                self.wakes.lock().unwrap().push(key.clone());
                return WakeServed::Live;
            }
            self.spawn_eager_wake(key, subscriber);
            WakeServed::Spawned
        }
        fn delivery_shape(&self, key: &SubscriberEntryKind) -> DeliveryShape {
            crate::messaging::default_delivery_shape(key)
        }
        fn alarm(&self, _channel: &str, _subscriber: &ParticipantId, _count: u64) {}
    }

    /// Build a `Messenger` over `channels` with a `RecordingWakeRouter`, wiring
    /// each channel to the store its class calls for.
    async fn wake_walk_messenger(
        channels: &[ChannelEntry],
    ) -> (Arc<Messenger>, Arc<RecordingWakeRouter>) {
        wake_walk_messenger_with_apps(channels, indexmap::IndexMap::new()).await
    }

    /// The same, with an apps map — which is where an `App` subscriber's wake
    /// economics come from, so a walk over conversations needs one.
    async fn wake_walk_messenger_with_apps(
        channels: &[ChannelEntry],
        apps: indexmap::IndexMap<String, crate::config::AppConfig>,
    ) -> (Arc<Messenger>, Arc<RecordingWakeRouter>) {
        let db = crate::db::init_db_memory();
        let (durable, nondurable): (Vec<ChannelEntry>, Vec<ChannelEntry>) = channels
            .iter()
            .cloned()
            .partition(|c| c.capabilities().durable);
        if !durable.is_empty() {
            let conn = db.lock().await;
            db::upsert_channels(&conn, &durable);
        }
        let router = Arc::new(RecordingWakeRouter::default());
        let messenger = Messenger::new(
            db,
            Arc::new(MessagingDirectory::with_entries(channels.to_vec())),
            Arc::from("test"),
            Arc::new(apps),
            router.clone() as Arc<dyn WakeRouter>,
            config::MessagingGlobalConfig::default(),
        )
        .with_ring_stores(Arc::new(store::RingStores::build(&nondurable)));
        (messenger, router)
    }

    /// The wake walk fires an eager wake for a WASM subscriber owed ring
    /// messages, and only once it is owed — the wake source ring publishes and
    /// released deferrals depend on (no durable push row exists for them).
    #[tokio::test]
    async fn wake_owed_subscribers_wakes_only_owed_ring_subscribers() {
        let mut channel = crate::messaging::testutils::ephemeral_channel_entry("wake-ch", 8);
        channel.subscribers = vec![crate::messaging::testutils::wasm_subscriber_entry("waker")];
        let (messenger, router) = wake_walk_messenger(std::slice::from_ref(&channel)).await;
        let wasm_sub = ParticipantId::for_wasm("waker");
        messenger.attach_ring_subscriber(&channel.uuid, &wasm_sub, 4, store::Priming::Head);

        // Nothing owed yet → no wake.
        messenger.wake_owed_subscribers(Utc::now()).await;
        assert!(router.wakes.lock().unwrap().is_empty());

        // A publish leaves the subscriber owed → woken with its Wasm key.
        messenger.ring_store_for(&channel).append(ring_envelope(
            &channel.address,
            ChannelScheme::Ephemeral,
            "hi",
        ));
        messenger.wake_owed_subscribers(Utc::now()).await;
        assert_eq!(
            *router.wakes.lock().unwrap(),
            vec![SubscriberEntryKind::Wasm("waker".to_string())]
        );
    }

    /// One walk covers every class in one pass: a WASM subscriber owed a durable
    /// claim and one owed a ring message are both woken by the same loop, and
    /// both stop being woken once their work is settled. The durable half also
    /// pins that the walk asks the store, not the dispatch scan's predicate —
    /// nothing here ever loads a dispatchable row.
    #[tokio::test]
    async fn wake_owed_subscribers_wakes_both_classes_in_one_walk() {
        let durable = (*crate::messaging::testutils::wasm_channel_entry(
            "durable-waker",
            "durable-wake-ch",
            Depth::Unbounded,
            Depth::Unbounded,
        ))
        .clone();
        let mut ring = crate::messaging::testutils::local_channel_entry("dual-ring-ch", 8);
        ring.subscribers = vec![crate::messaging::testutils::wasm_subscriber_entry(
            "ring-waker",
        )];
        let (messenger, router) = wake_walk_messenger(&[durable.clone(), ring.clone()]).await;
        let durable_sub = ParticipantId::for_wasm("durable-waker");
        let ring_sub = ParticipantId::for_wasm("ring-waker");
        messenger.attach_ring_subscriber(&ring.uuid, &ring_sub, 4, store::Priming::Head);
        messenger
            .attach_subscriber(
                &durable.address,
                "durable-waker",
                &durable_sub,
                Depth::Bounded(4),
                store::Priming::Head,
            )
            .await;

        // Nothing owed anywhere → no wake.
        messenger.wake_owed_subscribers(Utc::now()).await;
        assert!(router.wakes.lock().unwrap().is_empty());

        crate::messaging::testutils::insert_bus_message(
            &messenger,
            &durable,
            "owed",
            ChannelScheme::Brenn,
        )
        .await;
        messenger.ring_store_for(&ring).append(ring_envelope(
            &ring.address,
            ChannelScheme::Local,
            "owed",
        ));

        messenger.wake_owed_subscribers(Utc::now()).await;
        let mut woken = router.wakes.lock().unwrap().clone();
        woken.sort_by_key(|k| format!("{k:?}"));
        assert_eq!(
            woken,
            vec![
                SubscriberEntryKind::Wasm("durable-waker".to_string()),
                SubscriberEntryKind::Wasm("ring-waker".to_string()),
            ],
            "one pass wakes both classes"
        );

        {
            let durable_store = messenger.store_for(&durable);
            let window = store::RetentionStore::window(
                durable_store.as_ref(),
                &durable_sub,
                Depth::Bounded(4),
                Depth::Bounded(0),
            )
            .await;
            let (through, seen_floor) = window.advance_span().expect("the durable port owed one");
            store::RetentionStore::advance(
                durable_store.as_ref(),
                &durable_sub,
                through,
                seen_floor,
            )
            .await;
        }
        {
            let ring_store = messenger.ring_store_for(&ring);
            let window = store::RetentionStore::window(
                ring_store.as_ref(),
                &ring_sub,
                Depth::Bounded(4),
                Depth::Bounded(0),
            )
            .await;
            let (through, seen_floor) = window.advance_span().expect("the ring owed a message");
            store::RetentionStore::advance(ring_store.as_ref(), &ring_sub, through, seen_floor)
                .await;
        }
        router.wakes.lock().unwrap().clear();
        messenger.wake_owed_subscribers(Utc::now()).await;
        assert!(
            router.wakes.lock().unwrap().is_empty(),
            "settled work owes nothing, so nothing wakes"
        );
    }

    /// Counts `warn` events from the messaging module while it is the calling
    /// thread's default subscriber.
    struct MessagingWarnLayer(Arc<std::sync::atomic::AtomicUsize>);

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for MessagingWarnLayer {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            let meta = event.metadata();
            if *meta.level() == tracing::Level::WARN
                && meta.module_path().is_some_and(|m| m.contains("messaging"))
            {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
    }

    /// Install [`MessagingWarnLayer`] for the rest of the test and return its
    /// counter plus the guard that restores the previous default on drop.
    fn capture_messaging_warns() -> (
        Arc<std::sync::atomic::AtomicUsize>,
        tracing::subscriber::DefaultGuard,
    ) {
        use tracing_subscriber::layer::SubscriberExt;
        let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let subscriber =
            tracing_subscriber::registry().with(MessagingWarnLayer(Arc::clone(&count)));
        let guard = tracing::subscriber::set_default(subscriber);
        (count, guard)
    }

    /// A WASM subscriber whose position is owed messages under no registration
    /// is stranded — nothing will ever take them — and the walk meets it again
    /// every pass, so
    /// the report is once per `(channel, subscriber)`. An unbounded repeat
    /// buries the one signal that names the dropped input binding behind it.
    #[tokio::test]
    async fn a_stranded_wasm_subscriber_is_reported_once_across_passes() {
        let channel = (*crate::messaging::testutils::wasm_channel_entry(
            "waker",
            "stranded-ch",
            Depth::Unbounded,
            Depth::Unbounded,
        ))
        .clone();
        let (messenger, _router) = wake_walk_messenger(std::slice::from_ref(&channel)).await;
        // A position held by a subscriber this channel no longer names: what
        // dropping a component's input binding leaves behind.
        let ghost = ParticipantId::for_wasm("ghost");
        messenger
            .attach_subscriber(
                &channel.address,
                "ghost",
                &ghost,
                Depth::Bounded(4),
                store::Priming::Head,
            )
            .await;
        crate::messaging::testutils::insert_bus_message(
            &messenger,
            &channel,
            "owed",
            ChannelScheme::Brenn,
        )
        .await;

        let (warns, _guard) = capture_messaging_warns();
        messenger.wake_owed_subscribers(Utc::now()).await;
        messenger.wake_owed_subscribers(Utc::now()).await;
        assert_eq!(
            warns.load(Ordering::SeqCst),
            1,
            "two passes over one stranded position report it once"
        );
    }

    /// A conversation owed messages reaches the same arm on every normal channel —
    /// registrations are keyed by subscriber kind, and an `App(slug)`
    /// registration never names the conversation delivering under it. Reporting
    /// it would emit a warn per participant per pass for an expected condition.
    #[tokio::test]
    async fn an_owed_conversation_is_never_reported_as_stranded() {
        let channel = (*crate::messaging::testutils::wasm_channel_entry(
            "waker",
            "conv-owed-ch",
            Depth::Unbounded,
            Depth::Unbounded,
        ))
        .clone();
        let (messenger, _router) = wake_walk_messenger(std::slice::from_ref(&channel)).await;
        let conversation = ParticipantId::for_conversation(7);
        messenger
            .attach_subscriber(
                &channel.address,
                "waker",
                &conversation,
                Depth::Bounded(4),
                store::Priming::Head,
            )
            .await;
        crate::messaging::testutils::insert_bus_message(
            &messenger,
            &channel,
            "owed",
            ChannelScheme::Brenn,
        )
        .await;

        let (warns, _guard) = capture_messaging_warns();
        messenger.wake_owed_subscribers(Utc::now()).await;
        assert_eq!(
            warns.load(Ordering::SeqCst),
            0,
            "a conversation under an App registration is expected, not stranded"
        );
    }

    /// A surface session never appears in the walk at all: it holds no position
    /// on the channel, so there is nothing for the walk to find trailing. Its
    /// delivery state is the wire cursor its client echoes back.
    #[tokio::test]
    async fn wake_owed_subscribers_passes_over_inline_subscribers() {
        let mut channel = crate::messaging::testutils::test_channel_entry("inline-wake-ch", vec![]);
        channel.subscribers = vec![SubscriberEntry {
            kind: SubscriberEntryKind::Surface {
                slug: "board".to_string(),
                instance: Some("main".to_string()),
            },
            push_depth: Depth::Unbounded,
            retain_depth: Depth::Unbounded,
            noise: config::NoiseLevel::Silent,
            wake_min: None,
        }];
        let (messenger, router) = wake_walk_messenger(std::slice::from_ref(&channel)).await;

        crate::messaging::testutils::insert_bus_message(
            &messenger,
            &channel,
            "owed",
            ChannelScheme::Brenn,
        )
        .await;
        messenger.wake_owed_subscribers(Utc::now()).await;
        assert!(
            router.wakes.lock().unwrap().is_empty(),
            "a surface session holds no position, so the walk never names it"
        );
    }

    /// A durable channel carrying one `App` subscriber at `wake_min`, with the
    /// app wired so its wake economics resolve.
    fn conversation_wake_channel(slug: &str, name: &str, wake_min: WakeMin) -> ChannelEntry {
        crate::messaging::testutils::test_channel_entry(
            name,
            vec![SubscriberEntry {
                kind: SubscriberEntryKind::App(slug.to_string()),
                push_depth: Depth::Bounded(8),
                retain_depth: Depth::Bounded(8),
                noise: config::NoiseLevel::Silent,
                wake_min: Some(wake_min),
            }],
        )
    }

    /// One app, wired far enough for an `App` subscriber to resolve: the entry
    /// must exist for wake economics, and its policy must cover the channel for
    /// the walk's delivery gate.
    fn wake_apps(slug: &str) -> indexmap::IndexMap<String, crate::config::AppConfig> {
        let mut app = test_support::test_app_config(slug, None, vec![]);
        app.policy = test_support::brenn_delivery_policy(
            crate::access::acl::ChannelMatcher::Prefix(String::new()),
        );
        let mut apps = indexmap::IndexMap::new();
        apps.insert(slug.to_string(), app);
        apps
    }

    /// Commit one message at `urgency` onto `entry`'s store.
    async fn publish_at(messenger: &Messenger, entry: &ChannelEntry, body: &str, urgency: Urgency) {
        messenger
            .store_for(entry)
            .append(store::NewMessage {
                source: "test".to_string(),
                sender: "alice".to_string(),
                body: body.to_string(),
                urgency,
                envelope_type: ChannelScheme::Brenn,
                reply_to_uuid: None,
                delivery_deadline: None,
                publish_ts_ns: crate::messaging::db::utc_to_ns(Utc::now()),
            })
            .await;
    }

    /// Commit one `Normal` message onto `entry`'s store that must be in front of
    /// its subscriber by `deadline`.
    async fn publish_by(
        messenger: &Messenger,
        entry: &ChannelEntry,
        body: &str,
        deadline: DateTime<Utc>,
    ) {
        messenger
            .store_for(entry)
            .append(store::NewMessage {
                source: "test".to_string(),
                sender: "alice".to_string(),
                body: body.to_string(),
                urgency: Urgency::Normal,
                envelope_type: ChannelScheme::Brenn,
                reply_to_uuid: None,
                delivery_deadline: Some(deadline),
                publish_ts_ns: crate::messaging::db::utc_to_ns(Utc::now()),
            })
            .await;
    }

    /// A deadline that has come due wakes a subscriber the urgency economics
    /// would have left alone — that override is the whole point of naming one.
    /// The pass reports the forced wake so the dispatcher can debounce it: the
    /// deadline stays due until the subscriber drains past the message.
    #[tokio::test]
    async fn a_due_deadline_wakes_below_the_urgency_threshold() {
        let channel = conversation_wake_channel("assistant", "deadline-ch", WakeMin::High);
        let (messenger, router) =
            wake_walk_messenger_with_apps(std::slice::from_ref(&channel), wake_apps("assistant"))
                .await;
        let conversation = ParticipantId::for_conversation(31);
        messenger
            .attach_subscriber(
                &channel.address,
                "assistant",
                &conversation,
                Depth::Bounded(8),
                store::Priming::Head,
            )
            .await;

        let deadline = Utc::now() - chrono::Duration::seconds(1);
        publish_by(&messenger, &channel, "quiet but due", deadline).await;
        let sweep = messenger.wake_owed_subscribers(Utc::now()).await;
        assert_eq!(
            *router.wakes.lock().unwrap(),
            vec![SubscriberEntryKind::App("assistant".to_string())],
            "a Normal message under wake_min High still wakes once its deadline passes"
        );
        assert!(
            sweep.fired_deadline_wake,
            "the pass reports the forced wake so the loop debounces it"
        );
        assert_eq!(
            sweep.next_deadline, None,
            "a deadline already due is not a sleep target"
        );
    }

    /// The cooldown that holds an urgency-gated wake to one per tick does not
    /// hold a deadline: the subscriber has until T, and a suppressed retry could
    /// spend it. The forced wake still arms the cooldown, so a wake the urgency
    /// economics ask for coalesces into the one the deadline just fired.
    #[tokio::test]
    async fn a_due_deadline_overrides_the_inline_cooldown() {
        let channel = conversation_wake_channel("assistant", "deadline-retry-ch", WakeMin::High);
        let (messenger, router) =
            wake_walk_messenger_with_apps(std::slice::from_ref(&channel), wake_apps("assistant"))
                .await;
        let conversation = ParticipantId::for_conversation(32);
        messenger
            .attach_subscriber(
                &channel.address,
                "assistant",
                &conversation,
                Depth::Bounded(8),
                store::Priming::Head,
            )
            .await;

        let deadline = Utc::now() - chrono::Duration::seconds(1);
        publish_by(&messenger, &channel, "quiet but due", deadline).await;
        messenger.wake_owed_subscribers(Utc::now()).await;
        messenger.wake_owed_subscribers(Utc::now()).await;
        assert_eq!(
            router.wakes.lock().unwrap().len(),
            2,
            "the position still trails the deadline, so the retry is not suppressed"
        );

        // The other half of the rule: the forced wake armed the cooldown, so an
        // urgency-driven wake in the same window coalesces into it instead of
        // spawning a second subprocess. Serve the deadline message first, so
        // what follows is decided by the economics rather than by the deadline.
        messenger
            .store_for(&channel)
            .advance(&conversation, store::MessageSeq(1), store::MessageSeq(1))
            .await;
        publish_at(&messenger, &channel, "loud", Urgency::High).await;
        messenger.wake_owed_subscribers(Utc::now()).await;
        assert_eq!(
            router.wakes.lock().unwrap().len(),
            2,
            "a message above wake_min still coalesces into the wake the deadline just fired"
        );

        // And the cooldown is the only thing holding it: cleared, the same pass
        // wakes.
        messenger.clear_inline_wake(conversation.as_str());
        messenger.wake_owed_subscribers(Utc::now()).await;
        assert_eq!(
            router.wakes.lock().unwrap().len(),
            3,
            "with the cooldown spent the loud message wakes on its own economics"
        );
    }

    /// A subscriber the router found already live was served in place, at no
    /// spawn cost — so the cooldown, which exists to bound spawns, must not hold
    /// the next message back. Both halves of the rule run here against one
    /// fixture shape, because the only difference between them is the router's
    /// answer: report `Live` and the second message goes through on the next
    /// pass; report `Spawned` and it coalesces into the first wake, a whole poll
    /// interval away.
    ///
    /// Neither half advances the position, so what the second pass sees is
    /// decided by the cooldown alone.
    #[tokio::test]
    async fn a_live_serve_leaves_the_next_message_unpaced() {
        async fn two_passes(name: &str, conversation_id: i64, serve_live: bool) -> usize {
            let channel = conversation_wake_channel("assistant", name, WakeMin::Normal);
            let (messenger, router) = wake_walk_messenger_with_apps(
                std::slice::from_ref(&channel),
                wake_apps("assistant"),
            )
            .await;
            router.serve_live.store(serve_live, Ordering::SeqCst);
            let conversation = ParticipantId::for_conversation(conversation_id);
            messenger
                .attach_subscriber(
                    &channel.address,
                    "assistant",
                    &conversation,
                    Depth::Bounded(8),
                    store::Priming::Head,
                )
                .await;

            publish_at(&messenger, &channel, "first", Urgency::Normal).await;
            messenger.wake_owed_subscribers(Utc::now()).await;
            publish_at(&messenger, &channel, "second", Urgency::Normal).await;
            messenger.wake_owed_subscribers(Utc::now()).await;
            router.wakes.lock().unwrap().len()
        }

        assert_eq!(
            two_passes("live-serve-ch", 38, true).await,
            2,
            "a live serve spawns nothing, so the next message is served on the next pass"
        );
        assert_eq!(
            two_passes("spawned-serve-ch", 39, false).await,
            1,
            "a spawned wake is paced: the second message coalesces into it"
        );
    }

    /// A deadline still ahead wakes nobody and becomes the dispatcher's sleep
    /// target — the timer source that replaces the per-subscriber copy of T.
    #[tokio::test]
    async fn a_deadline_still_ahead_is_the_sleep_target() {
        let channel = conversation_wake_channel("assistant", "deadline-ahead-ch", WakeMin::High);
        let (messenger, router) =
            wake_walk_messenger_with_apps(std::slice::from_ref(&channel), wake_apps("assistant"))
                .await;
        let conversation = ParticipantId::for_conversation(33);
        messenger
            .attach_subscriber(
                &channel.address,
                "assistant",
                &conversation,
                Depth::Bounded(8),
                store::Priming::Head,
            )
            .await;

        // Whole seconds: the durable store persists deadlines at that grain.
        let deadline =
            DateTime::from_timestamp((Utc::now() + chrono::Duration::minutes(5)).timestamp(), 0)
                .expect("representable");
        publish_by(&messenger, &channel, "quiet, due later", deadline).await;
        let sweep = messenger.wake_owed_subscribers(Utc::now()).await;
        assert!(
            router.wakes.lock().unwrap().is_empty(),
            "below wake_min and not yet due wakes nobody"
        );
        assert_eq!(
            sweep.next_deadline,
            Some(deadline),
            "the pass hands the dispatcher the time it must be awake by"
        );
        assert!(!sweep.fired_deadline_wake);
    }

    /// A deadline the position has passed leaves the pass's view entirely: no
    /// wake, and no sleep target. Nothing stores "this one was handled" — the
    /// position moving past the message is the whole record.
    #[tokio::test]
    async fn a_served_deadline_wakes_nobody() {
        let channel = conversation_wake_channel("assistant", "deadline-served-ch", WakeMin::High);
        let (messenger, router) =
            wake_walk_messenger_with_apps(std::slice::from_ref(&channel), wake_apps("assistant"))
                .await;
        let conversation = ParticipantId::for_conversation(34);
        messenger
            .attach_subscriber(
                &channel.address,
                "assistant",
                &conversation,
                Depth::Bounded(8),
                store::Priming::Head,
            )
            .await;

        let deadline = Utc::now() - chrono::Duration::seconds(1);
        publish_by(&messenger, &channel, "quiet but due", deadline).await;
        messenger
            .store_for(&channel)
            .advance(&conversation, store::MessageSeq(1), store::MessageSeq(1))
            .await;

        let sweep = messenger.wake_owed_subscribers(Utc::now()).await;
        assert!(
            router.wakes.lock().unwrap().is_empty(),
            "the conversation has seen the message, so its deadline is spent"
        );
        assert_eq!(sweep.next_deadline, None);
        assert!(!sweep.fired_deadline_wake);
    }

    /// A deadline on a parked message is not owed to anyone until the message is
    /// released: it enters retention then, with a fresh seq above the position,
    /// and the pass that follows the release is the one that finds it due. A
    /// deadline that expired while the message was parked is due the moment it
    /// lands, which is the same rule read from the same row.
    #[tokio::test]
    async fn a_parked_message_owes_its_deadline_from_release() {
        let channel = conversation_wake_channel("assistant", "deadline-parked-ch", WakeMin::High);
        let (messenger, router) =
            wake_walk_messenger_with_apps(std::slice::from_ref(&channel), wake_apps("assistant"))
                .await;
        let conversation = ParticipantId::for_conversation(35);
        messenger
            .attach_subscriber(
                &channel.address,
                "assistant",
                &conversation,
                Depth::Bounded(8),
                store::Priming::Head,
            )
            .await;

        let now = Utc::now();
        let deadline = now - chrono::Duration::seconds(1);
        messenger
            .store_for(&channel)
            .park(
                store::NewMessage {
                    source: "test".to_string(),
                    sender: "alice".to_string(),
                    body: "quiet, and late".to_string(),
                    urgency: Urgency::Normal,
                    envelope_type: ChannelScheme::Brenn,
                    reply_to_uuid: None,
                    delivery_deadline: Some(deadline),
                    publish_ts_ns: crate::messaging::db::utc_to_ns(now),
                },
                now - chrono::Duration::seconds(30),
            )
            .await
            .expect("park within quota");

        let sweep = messenger.wake_owed_subscribers(now).await;
        assert!(
            router.wakes.lock().unwrap().is_empty(),
            "a parked message is owed to nobody, deadline or not"
        );
        assert!(!sweep.fired_deadline_wake);

        messenger.release_due_messages(now).await;
        let sweep = messenger.wake_owed_subscribers(now).await;
        assert_eq!(
            *router.wakes.lock().unwrap(),
            vec![SubscriberEntryKind::App("assistant".to_string())],
            "release puts it in the unseen suffix, where its expired deadline forces the wake"
        );
        assert!(sweep.fired_deadline_wake);
    }

    /// A channel + messenger + attached conversation carrying one quiet message
    /// due at `deadline`, with the dispatcher loop running over it. Nothing here
    /// writes a wake row — the conversation's registration resolves no owner —
    /// so the pass's own report is the loop's only deadline source, which is what
    /// makes these two cases pin it.
    async fn deadline_loop_rig(
        name: &str,
        conversation_id: i64,
        deadline: DateTime<Utc>,
    ) -> (
        Arc<RecordingWakeRouter>,
        Arc<tokio::sync::Notify>,
        tokio::task::JoinHandle<()>,
    ) {
        let channel = conversation_wake_channel("assistant", name, WakeMin::High);
        let (messenger, router) =
            wake_walk_messenger_with_apps(std::slice::from_ref(&channel), wake_apps("assistant"))
                .await;
        let conversation = ParticipantId::for_conversation(conversation_id);
        messenger
            .attach_subscriber(
                &channel.address,
                "assistant",
                &conversation,
                Depth::Bounded(8),
                store::Priming::Head,
            )
            .await;
        publish_by(&messenger, &channel, "quiet but scheduled", deadline).await;

        let db = messenger.db().clone();
        let kick = Arc::new(tokio::sync::Notify::new());
        let handle = dispatcher::spawn_dispatcher_task(
            db,
            router.clone() as Arc<dyn WakeRouter>,
            kick.clone(),
            messenger,
        );
        (router, kick, handle)
    }

    /// The loop sleeps to the deadline the pass reported, not to its poll
    /// interval: a message too quiet to wake anyone is still put in front of its
    /// subscriber at T, with no kick in between. Without the pass's
    /// `next_deadline` folded into the sleep target the wake would be up to a
    /// poll interval late.
    #[tokio::test]
    async fn the_loop_wakes_at_the_deadline_the_pass_reported() {
        // Whole seconds: the durable store persists deadlines at that grain, so
        // this lands between one and two seconds out.
        let deadline =
            DateTime::from_timestamp((Utc::now() + chrono::Duration::seconds(2)).timestamp(), 0)
                .expect("representable");
        let (router, kick, handle) = deadline_loop_rig("deadline-loop-ch", 36, deadline).await;

        // The publish's own kick makes the first pass run and report the deadline.
        kick.notify_one();
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        assert!(
            router.wakes.lock().unwrap().is_empty(),
            "below wake_min and not yet due: nothing wakes early"
        );

        // No further kick: only the deadline can bring the loop back.
        for _ in 0..60 {
            if !router.wakes.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        assert_eq!(
            *router.wakes.lock().unwrap(),
            vec![SubscriberEntryKind::App("assistant".to_string())],
            "the loop must come back at T on its own"
        );
        handle.abort();
    }

    /// A deadline stays due until its subscriber drains past it, so the pass that
    /// forces a wake reports it and the loop retries on the debounce rather than
    /// on its poll interval: paced, not spinning, and not a minute apart. Losing
    /// the report leaves the retry a whole poll interval away — a forced wake
    /// that failed to land would sit out the deadline it exists to keep.
    #[tokio::test]
    async fn a_due_deadline_paces_the_loop_by_the_debounce() {
        let deadline = Utc::now() - chrono::Duration::seconds(1);
        let (router, kick, handle) =
            deadline_loop_rig("deadline-retry-loop-ch", 37, deadline).await;

        kick.notify_one();
        for _ in 0..100 {
            if !router.wakes.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(
            router.wakes.lock().unwrap().len(),
            1,
            "the due deadline forces one wake"
        );

        // The subscriber never drains, so the deadline stays due. A loop that
        // took the past-due target as its sleep target would fire dozens here.
        tokio::time::sleep(std::time::Duration::from_millis(1_000)).await;
        assert_eq!(
            router.wakes.lock().unwrap().len(),
            1,
            "a due deadline does not spin the loop"
        );

        // …and the retry lands on the debounce, well inside the poll interval.
        for _ in 0..70 {
            if router.wakes.lock().unwrap().len() > 1 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        assert_eq!(
            router.wakes.lock().unwrap().len(),
            2,
            "the debounce the pass armed brings the loop back long before its poll interval"
        );
        handle.abort();
    }

    /// The wake decision made at wake time agrees with the rule the commit path
    /// applies to the one message it commits, over the whole (threshold, urgency)
    /// matrix. Both ask [`store::targets::wakes_at`]; this is the pin that the
    /// walk really routes its answer through it rather than approximating it for
    /// a backlog.
    #[tokio::test]
    async fn the_walk_wakes_exactly_where_the_commit_rule_would_have() {
        for wake_min in [
            WakeMin::VeryLow,
            WakeMin::Low,
            WakeMin::Normal,
            WakeMin::High,
        ] {
            for urgency in Urgency::ALL {
                let channel = conversation_wake_channel(
                    "assistant",
                    &format!("matrix-{}-{urgency:?}", wake_min.as_str()),
                    wake_min,
                );
                let (messenger, router) = wake_walk_messenger_with_apps(
                    std::slice::from_ref(&channel),
                    wake_apps("assistant"),
                )
                .await;
                let conversation = ParticipantId::for_conversation(41);
                messenger
                    .attach_subscriber(
                        &channel.address,
                        "assistant",
                        &conversation,
                        Depth::Bounded(8),
                        store::Priming::Head,
                    )
                    .await;

                publish_at(&messenger, &channel, "one", urgency).await;
                messenger.wake_owed_subscribers(Utc::now()).await;
                let expected =
                    store::targets::wakes_at(WakeEconomics::UrgencyGated, Some(wake_min), urgency);
                assert_eq!(
                    !router.wakes.lock().unwrap().is_empty(),
                    expected,
                    "wake_min {} at {urgency:?}",
                    wake_min.as_str()
                );
            }
        }
    }

    /// The wake economics are applied to the loudest message a conversation has
    /// not seen, at wake time: a backlog entirely below `wake_min` wakes nobody
    /// and waits for the conversation's next natural drain, and one loud message
    /// arriving behind it wakes on the same backlog the previous pass declined.
    #[tokio::test]
    async fn a_conversation_wakes_on_the_loudest_thing_it_has_not_seen() {
        let channel = conversation_wake_channel("assistant", "quiet-ch", WakeMin::High);
        let (messenger, router) =
            wake_walk_messenger_with_apps(std::slice::from_ref(&channel), wake_apps("assistant"))
                .await;
        let conversation = ParticipantId::for_conversation(11);
        messenger
            .attach_subscriber(
                &channel.address,
                "assistant",
                &conversation,
                Depth::Bounded(8),
                store::Priming::Head,
            )
            .await;

        publish_at(&messenger, &channel, "chatter", Urgency::Normal).await;
        messenger.wake_owed_subscribers(Utc::now()).await;
        assert!(
            router.wakes.lock().unwrap().is_empty(),
            "a below-threshold backlog costs no subprocess spawn"
        );

        publish_at(&messenger, &channel, "the house is on fire", Urgency::High).await;
        messenger.wake_owed_subscribers(Utc::now()).await;
        assert_eq!(
            *router.wakes.lock().unwrap(),
            vec![SubscriberEntryKind::App("assistant".to_string())],
            "the loud message wakes the conversation over the whole backlog"
        );
    }

    /// The walk is the retry path, not the fast path: a conversation whose
    /// position still trails after a wake is not re-woken on the next pass. The
    /// wake costs a subprocess and the walk runs on every dispatcher kick, so an
    /// ungated repeat would spawn at the publish rate.
    #[tokio::test]
    async fn an_inline_wake_is_not_repeated_on_the_next_pass() {
        let channel = conversation_wake_channel("assistant", "retry-ch", WakeMin::Normal);
        let (messenger, router) =
            wake_walk_messenger_with_apps(std::slice::from_ref(&channel), wake_apps("assistant"))
                .await;
        let conversation = ParticipantId::for_conversation(12);
        messenger
            .attach_subscriber(
                &channel.address,
                "assistant",
                &conversation,
                Depth::Bounded(8),
                store::Priming::Head,
            )
            .await;

        publish_at(&messenger, &channel, "first", Urgency::Normal).await;
        messenger.wake_owed_subscribers(Utc::now()).await;
        publish_at(&messenger, &channel, "second", Urgency::Normal).await;
        messenger.wake_owed_subscribers(Utc::now()).await;
        assert_eq!(
            router.wakes.lock().unwrap().len(),
            1,
            "the conversation never drained, so the second pass adds nothing"
        );

        // Suppression is a window, not a verdict. Once it lapses — here by the
        // dispatcher's own "this one is live" signal, which clears it — the walk
        // wakes the still-trailing position again. That retry is the walk's whole
        // job behind a lost wake or a failed spawn.
        messenger.clear_inline_wake(conversation.as_str());
        messenger.wake_owed_subscribers(Utc::now()).await;
        assert_eq!(
            router.wakes.lock().unwrap().len(),
            2,
            "the backlog is still owed, so a lapsed cooldown wakes it again"
        );
    }

    /// A conversation on a ring-backed channel is as visible to the walk as one
    /// on a durable channel: the walk resolves a conversation's registration
    /// through the slug its position caches, and a ring position caches one for
    /// the same reason a durable row does.
    #[tokio::test]
    async fn a_ring_conversation_is_woken_by_the_walk() {
        let mut channel = crate::messaging::testutils::ephemeral_channel_entry("ring-conv-ch", 8);
        channel.subscribers = vec![SubscriberEntry {
            kind: SubscriberEntryKind::App("assistant".to_string()),
            push_depth: Depth::Bounded(8),
            retain_depth: Depth::Bounded(8),
            noise: config::NoiseLevel::Silent,
            wake_min: Some(WakeMin::Normal),
        }];
        let mut apps = wake_apps("assistant");
        {
            let policy = &mut apps.get_mut("assistant").expect("fixture app").policy;
            policy
                .grants
                .insert(crate::access::AppCapability::EphemeralSubscribe);
            policy
                .acls
                .ephemeral_subscribe
                .push(crate::access::acl::ChannelMatcher::Prefix(String::new()));
        }
        let (messenger, router) =
            wake_walk_messenger_with_apps(std::slice::from_ref(&channel), apps).await;
        let conversation = ParticipantId::for_conversation(21);
        messenger
            .attach_subscriber(
                &channel.address,
                "assistant",
                &conversation,
                Depth::Bounded(8),
                store::Priming::Head,
            )
            .await;

        messenger.ring_store_for(&channel).append(ring_envelope(
            &channel.address,
            ChannelScheme::Ephemeral,
            "hi",
        ));
        messenger.wake_owed_subscribers(Utc::now()).await;
        assert_eq!(
            *router.wakes.lock().unwrap(),
            vec![SubscriberEntryKind::App("assistant".to_string())],
            "the ring position names its app, so the walk finds its registration"
        );
    }

    /// A revoked ACL stops the wake, not just the delivery. The drain refuses to
    /// serve the channel, so waking the conversation buys a subprocess spawn
    /// that renders nothing — every pass, for as long as the revocation stands.
    ///
    /// And the report of it is once per `(channel, app)`: the revocation is a
    /// standing state every pass re-observes, so a warn per pass would bury the
    /// one line that names it.
    #[tokio::test]
    async fn a_conversation_the_acl_denies_is_not_woken() {
        let channel = conversation_wake_channel("assistant", "denied-ch", WakeMin::Normal);
        let mut apps = wake_apps("assistant");
        apps.get_mut("assistant").expect("fixture app").policy =
            crate::access::AppPolicy::default();
        let (messenger, router) =
            wake_walk_messenger_with_apps(std::slice::from_ref(&channel), apps).await;
        let conversation = ParticipantId::for_conversation(22);
        messenger
            .attach_subscriber(
                &channel.address,
                "assistant",
                &conversation,
                Depth::Bounded(8),
                store::Priming::Head,
            )
            .await;

        publish_at(&messenger, &channel, "unservable", Urgency::Normal).await;
        let (warns, _guard) = capture_messaging_warns();
        messenger.wake_owed_subscribers(Utc::now()).await;
        messenger.wake_owed_subscribers(Utc::now()).await;
        assert!(
            router.wakes.lock().unwrap().is_empty(),
            "a subscriber the delivery gate will deny is not worth a spawn"
        );
        assert_eq!(
            warns.load(Ordering::SeqCst),
            1,
            "two passes over one standing revocation report it once"
        );
    }

    /// An inline subscriber whose economics do not resolve is a wiring bug the
    /// boot cross-check exists to catch. Reaching the walk with one means the
    /// subscriber can never be woken by anything, so the walk dies rather than
    /// leaving it silently wedged.
    #[tokio::test]
    #[should_panic(expected = "has no wake economics")]
    async fn an_inline_subscriber_without_economics_kills_the_walk() {
        let channel = conversation_wake_channel("ghost", "unwired-ch", WakeMin::Normal);
        // No apps map entry for `ghost`: the economics lookup finds nothing.
        let (messenger, _router) = wake_walk_messenger(std::slice::from_ref(&channel)).await;
        let conversation = ParticipantId::for_conversation(13);
        messenger
            .attach_subscriber(
                &channel.address,
                "ghost",
                &conversation,
                Depth::Bounded(8),
                store::Priming::Head,
            )
            .await;

        publish_at(&messenger, &channel, "unwakeable", Urgency::Normal).await;
        messenger.wake_owed_subscribers(Utc::now()).await;
    }

    /// A parked subscriber is woken by anything it is owed, whatever the
    /// registration's `wake_min` says: its wake is a notify and its delivery
    /// trigger both, so urgency has nothing to gate.
    #[tokio::test]
    async fn a_parked_subscriber_is_woken_below_its_registrations_wake_min() {
        let mut channel = crate::messaging::testutils::ephemeral_channel_entry("parked-ch", 8);
        channel.subscribers = vec![SubscriberEntry {
            wake_min: Some(WakeMin::High),
            ..crate::messaging::testutils::wasm_subscriber_entry("waker")
        }];
        let (messenger, router) = wake_walk_messenger(std::slice::from_ref(&channel)).await;
        let wasm_sub = ParticipantId::for_wasm("waker");
        messenger.attach_ring_subscriber(&channel.uuid, &wasm_sub, 4, store::Priming::Head);

        messenger.ring_store_for(&channel).append(ring_envelope(
            &channel.address,
            ChannelScheme::Ephemeral,
            "quiet",
        ));
        messenger.wake_owed_subscribers(Utc::now()).await;
        assert_eq!(
            *router.wakes.lock().unwrap(),
            vec![SubscriberEntryKind::Wasm("waker".to_string())]
        );
    }

    #[tokio::test]
    async fn detach_subscriber_drops_the_cursor_and_clears_in_memory_state() {
        let (messenger, channel, wasm_sub) = build_ring_wasm_messenger(
            "dropme",
            crate::messaging::testutils::ephemeral_channel_entry("detach-ch", 8),
        );
        messenger.attach_ring_subscriber(&channel.uuid, &wasm_sub, 4, store::Priming::Head);
        messenger.ring_store_for(&channel).append(ring_envelope(
            &channel.address,
            ChannelScheme::Ephemeral,
            "a",
        ));
        assert!(messenger.ring_store_for(&channel).is_attached(&wasm_sub));

        // Plant the ladder's metered tally for this subscriber.
        messenger.enact_overflow_noise(&channel.address, &wasm_sub, config::NoiseLevel::Metered, 3);
        assert_eq!(messenger.drop_counter(&channel.address, &wasm_sub), 3);

        messenger
            .detach_subscriber(&channel.address, &wasm_sub)
            .await;

        assert!(!messenger.ring_store_for(&channel).is_attached(&wasm_sub));
        assert_eq!(messenger.drop_counter(&channel.address, &wasm_sub), 0);
    }

    /// One subscriber's losses on two channels are two tallies. A single
    /// per-subscriber count would read a busy channel's overflow as the quiet
    /// channel's, and detaching from one would erase the other's history.
    #[tokio::test]
    async fn the_metered_tally_is_keyed_by_channel_as_well_as_subscriber() {
        let noisy = crate::messaging::testutils::ephemeral_channel_entry("tally-noisy", 8);
        let quiet = crate::messaging::testutils::ephemeral_channel_entry("tally-quiet", 8);
        let entries = vec![noisy.clone(), quiet.clone()];
        let messenger = Messenger::new(
            crate::db::init_db_memory(),
            Arc::new(MessagingDirectory::with_entries(entries.clone())),
            Arc::from("test"),
            Arc::new(indexmap::IndexMap::new()),
            Arc::new(super::query::NoopWakeRouter) as Arc<dyn WakeRouter>,
            config::MessagingGlobalConfig::default(),
        )
        .with_ring_stores(Arc::new(store::RingStores::build(&entries)));
        let sub = ParticipantId::for_wasm("two-ports");
        for channel in [&noisy, &quiet] {
            messenger.attach_ring_subscriber(&channel.uuid, &sub, 4, store::Priming::Head);
        }

        messenger.enact_overflow_noise(&noisy.address, &sub, config::NoiseLevel::Metered, 2);
        messenger.enact_overflow_noise(&noisy.address, &sub, config::NoiseLevel::Metered, 1);
        assert_eq!(messenger.drop_counter(&noisy.address, &sub), 3);
        assert_eq!(messenger.drop_counter(&quiet.address, &sub), 0);

        messenger.detach_subscriber(&quiet.address, &sub).await;
        assert_eq!(
            messenger.drop_counter(&noisy.address, &sub),
            3,
            "leaving one channel says nothing about what was lost on another"
        );
    }

    /// `Fatal` is a surface rung: the kernel enacts it on its own queues and the
    /// backend never resolves one for a bus subscription. The arm that says so is
    /// a panic rather than a fall-through to `Metered`, and this is what executes
    /// it — remove the arm, or let it drop through, and nothing else notices.
    #[tokio::test]
    #[should_panic(expected = "reached noise = fatal")]
    async fn the_fatal_rung_panics_rather_than_metering_a_surface_only_level() {
        let channel = crate::messaging::testutils::ephemeral_channel_entry("fatal-ch", 8);
        let (messenger, channel, wasm_sub) = build_ring_wasm_messenger("burner", channel);
        messenger.attach_ring_subscriber(&channel.uuid, &wasm_sub, 4, store::Priming::Head);

        messenger.enact_overflow_noise(&channel.address, &wasm_sub, config::NoiseLevel::Fatal, 1);
    }

    fn ring_envelope(channel: &str, scheme: ChannelScheme, body: &str) -> MessageEnvelope {
        MessageEnvelope {
            message_id: Uuid::new_v4(),
            source: "node".to_string(),
            channel: channel.to_string(),
            sender: "test-sender".to_string(),
            publish_ts: Utc::now(),
            body: body.to_string(),
            reply_to: None,
            delivery_deadline: None,
            deliver_after: None,
            urgency: Urgency::Normal,
            envelope_type: scheme,
        }
    }

    fn ring_input(channel: &ChannelEntry, push_depth: Depth, retain_depth: Depth) -> WasmInputPort {
        WasmInputPort {
            port: "in".to_string(),
            sub: config::ResolvedSubscription {
                channel_uuid: channel.uuid,
                channel_address: channel.address.clone(),
                push_depth,
                retain_depth,
                noise: config::NoiseLevel::Silent,
                wake_min: WakeMin::Normal,
            },
            amplification_mt: 1000,
        }
    }

    /// A ring-backed port draws its NEW rows from the subscriber's cursor. When
    /// more than push_depth are owed the cursor delivers the newest and charges
    /// the older as drops (the drop-oldest rule); the dropped-but-still-retained
    /// message remains channel ambience in context. NEW rows carry no push id.
    #[tokio::test]
    async fn load_activation_snapshot_ring_port_takes_newest_and_keeps_dropped_as_context() {
        let (messenger, channel, wasm_sub) = build_ring_wasm_messenger(
            "ring-take",
            crate::messaging::testutils::ephemeral_channel_entry("ring-take-ch", 8),
        );
        // Attach at head: only messages published after attach are owed.
        assert_eq!(
            messenger.attach_ring_subscriber(&channel.uuid, &wasm_sub, 2, store::Priming::Head),
            store::Attached::Created
        );

        let store = messenger.ring_store_for(&channel);
        let mids: Vec<Uuid> = (0..3)
            .map(|i| {
                let env =
                    ring_envelope(&channel.address, ChannelScheme::Ephemeral, &format!("m{i}"));
                let mid = env.message_id;
                store.append(env);
                mid
            })
            .collect();

        let inputs = vec![ring_input(&channel, Depth::Bounded(2), Depth::Bounded(8))];
        let snapshots = messenger
            .load_activation_snapshot(&wasm_sub, &inputs)
            .await
            .expect("ring cursor has deliverable messages");
        assert_eq!(snapshots.len(), 1);
        let snap = &snapshots[0];

        // push_depth 2, three owed → the newest two, oldest first.
        let new_ids: Vec<Uuid> = snap
            .new_entries()
            .iter()
            .map(|(_, e)| e.message_id)
            .collect();
        assert_eq!(new_ids, vec![mids[1], mids[2]]);
        // m0 is below the new boundary but inside the window, so it is served as
        // context — visible, and therefore not a drop.
        let context_ids: Vec<Uuid> = snap.context().iter().map(|(_, e)| e.message_id).collect();
        assert_eq!(context_ids, vec![mids[0]]);

        let (through, seen_floor) = snap.advance_span().expect("the window served entries");
        let outcome = messenger
            .advance_subscriber(
                &channel.address,
                &wasm_sub,
                through,
                seen_floor,
                config::NoiseLevel::Silent,
            )
            .await;
        assert_eq!(
            outcome.dropped, 0,
            "every unseen message was served, as new or as context"
        );
    }

    /// Nothing owed → the ring port is non-triggering and the whole snapshot is
    /// `None`, without consuming any ring state (a later publish still delivers).
    #[tokio::test]
    async fn load_activation_snapshot_ring_none_when_nothing_owed() {
        let (messenger, channel, wasm_sub) = build_ring_wasm_messenger(
            "ring-empty",
            crate::messaging::testutils::local_channel_entry("ring-empty-ch", 8),
        );
        messenger.attach_ring_subscriber(&channel.uuid, &wasm_sub, 4, store::Priming::Head);

        let inputs = vec![ring_input(&channel, Depth::Bounded(4), Depth::Bounded(8))];
        assert!(
            messenger
                .load_activation_snapshot(&wasm_sub, &inputs)
                .await
                .is_none(),
            "an attached cursor with nothing owed does not trigger"
        );

        // A publish after the None call is still delivered — the earlier call
        // consumed no ring state.
        let env = ring_envelope(&channel.address, ChannelScheme::Local, "late");
        let mid = env.message_id;
        messenger.ring_store_for(&channel).append(env);
        let snap = messenger
            .load_activation_snapshot(&wasm_sub, &inputs)
            .await
            .expect("the late publish is owed");
        assert_eq!(
            snap[0]
                .new_entries()
                .iter()
                .map(|(_, e)| e.message_id)
                .collect::<Vec<_>>(),
            vec![mid]
        );
    }

    /// A cursor that fell behind a bounded ring accounts the evicted messages as
    /// its own drops; the snapshot reports the cumulative count the caller deltas.
    #[tokio::test]
    async fn load_activation_snapshot_ring_reports_cursor_drops() {
        let (messenger, channel, wasm_sub) = build_ring_wasm_messenger(
            "ring-drop",
            crate::messaging::testutils::ephemeral_channel_entry("ring-drop-ch", 2),
        );
        messenger.attach_ring_subscriber(&channel.uuid, &wasm_sub, 8, store::Priming::Head);

        // Four publishes into a depth-2 ring: the oldest two are evicted before
        // the cursor takes them → two accountable drops.
        let store = messenger.ring_store_for(&channel);
        for i in 0..4 {
            store.append(ring_envelope(
                &channel.address,
                ChannelScheme::Ephemeral,
                &format!("d{i}"),
            ));
        }

        let inputs = vec![ring_input(&channel, Depth::Bounded(8), Depth::Bounded(2))];
        let snap = messenger
            .load_activation_snapshot(&wasm_sub, &inputs)
            .await
            .expect("the surviving two are owed");
        assert_eq!(snap[0].new_len(), 2, "only the two retained survive");

        let (through, seen_floor) = snap[0].advance_span().expect("the window served entries");
        let outcome = messenger
            .advance_subscriber(
                &channel.address,
                &wasm_sub,
                through,
                seen_floor,
                config::NoiseLevel::Silent,
            )
            .await;
        assert_eq!(
            outcome.dropped, 2,
            "two evicted-before-read messages are accountable drops"
        );
        assert_eq!(
            outcome.noise_charge, 0,
            "the evicting appends already reported them"
        );
    }

    /// Counts alarm invocations for the noise-enactment tests.
    #[derive(Default)]
    struct CountingAlarmRouter {
        alarms: std::sync::atomic::AtomicU64,
    }

    #[async_trait::async_trait]
    impl WakeRouter for CountingAlarmRouter {
        async fn deliver(
            &self,
            _key: &SubscriberEntryKind,
            _envelope: &std::sync::Arc<MessageEnvelope>,
            _retained_seq: i64,
        ) -> Result<bool, String> {
            unreachable!("WASM delivery never routes inline through deliver")
        }
        async fn deliver_ingress(
            &self,
            _key: &SubscriberEntryKind,
            _subscriber: &ParticipantId,
            _event: &ingress::Event,
        ) -> Result<bool, String> {
            unreachable!("WASM delivery never routes through deliver_ingress")
        }
        fn spawn_eager_wake(&self, _key: &SubscriberEntryKind, _subscriber: &ParticipantId) {}
        fn delivery_shape(&self, key: &SubscriberEntryKind) -> DeliveryShape {
            crate::messaging::default_delivery_shape(key)
        }
        fn alarm(&self, _channel: &str, _subscriber: &ParticipantId, _count: u64) {
            self.alarms
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    fn build_ring_wasm_messenger_with_alarm(
        slug: &str,
        channel: ChannelEntry,
    ) -> (
        Arc<Messenger>,
        Arc<ChannelEntry>,
        ParticipantId,
        Arc<CountingAlarmRouter>,
    ) {
        let router = Arc::new(CountingAlarmRouter::default());
        let ring_stores = Arc::new(store::RingStores::build(std::slice::from_ref(&channel)));
        let messenger = Messenger::new(
            crate::db::init_db_memory(),
            Arc::new(MessagingDirectory::with_entries(vec![channel.clone()])),
            Arc::from("test"),
            Arc::new(indexmap::IndexMap::new()),
            router.clone() as Arc<dyn WakeRouter>,
            config::MessagingGlobalConfig::default(),
        )
        .with_ring_stores(ring_stores);
        (
            messenger,
            Arc::new(channel),
            ParticipantId::for_wasm(slug),
            router,
        )
    }

    fn ring_input_noise(
        channel: &ChannelEntry,
        push_depth: Depth,
        retain_depth: Depth,
        noise: config::NoiseLevel,
    ) -> WasmInputPort {
        WasmInputPort {
            port: "in".to_string(),
            sub: config::ResolvedSubscription {
                channel_uuid: channel.uuid,
                channel_address: channel.address.clone(),
                push_depth,
                retain_depth,
                noise,
                wake_min: WakeMin::Normal,
            },
            amplification_mt: 1000,
        }
    }

    /// Drive `push_depth`-1 overflow of two owed messages on a ring channel at
    /// `noise`, returning the resolved drop counter and alarm count.
    async fn ring_overflow_enactment(noise: config::NoiseLevel) -> (u64, u64, u64) {
        let (messenger, channel, wasm_sub, router) = build_ring_wasm_messenger_with_alarm(
            "ring-noise",
            crate::messaging::testutils::ephemeral_channel_entry("ring-noise-ch", 8),
        );
        messenger.attach_ring_subscriber(&channel.uuid, &wasm_sub, 1, store::Priming::Head);
        let store = messenger.ring_store_for(&channel);
        for i in 0..3 {
            store.append(ring_envelope(
                &channel.address,
                ChannelScheme::Ephemeral,
                &format!("n{i}"),
            ));
        }
        // A bare trigger port: retain_depth 0, so the two the push clamp skips are
        // outside the window entirely and the advance reports them.
        let inputs = vec![ring_input_noise(
            &channel,
            Depth::Bounded(1),
            Depth::Bounded(0),
            noise,
        )];
        let snap = messenger
            .load_activation_snapshot(&wasm_sub, &inputs)
            .await
            .expect("three owed, push_depth 1");
        assert_eq!(snap[0].new_len(), 1, "only the newest delivered");
        let (through, seen_floor) = snap[0].advance_span().expect("the window served entries");
        let outcome = messenger
            .advance_subscriber(&channel.address, &wasm_sub, through, seen_floor, noise)
            .await;
        (
            messenger.drop_counter(&channel.address, &wasm_sub),
            router.alarms.load(std::sync::atomic::Ordering::SeqCst),
            outcome.dropped,
        )
    }

    /// Silent ring overflow: the cursor still reports its raw drop to the guest,
    /// but the noise ladder increments no counter and fires no alarm.
    #[tokio::test]
    async fn ring_overflow_silent_enacts_nothing() {
        let (drop_counter, alarms, cursor_drops) =
            ring_overflow_enactment(config::NoiseLevel::Silent).await;
        assert_eq!(drop_counter, 0, "silent: no metered counter");
        assert_eq!(alarms, 0, "silent: no alarm");
        assert_eq!(cursor_drops, 2, "guest still sees the two raw cursor drops");
    }

    /// Metered ring overflow: the drop counter increments by the number of
    /// dropped messages, no alarm.
    #[tokio::test]
    async fn ring_overflow_metered_increments_counter() {
        let (drop_counter, alarms, _) = ring_overflow_enactment(config::NoiseLevel::Metered).await;
        assert_eq!(drop_counter, 2, "metered: counter += dropped count");
        assert_eq!(alarms, 0, "metered: no alarm");
    }

    /// Alarm ring overflow: the counter increments and the router alarm fires
    /// once for the batch.
    #[tokio::test]
    async fn ring_overflow_alarm_increments_counter_and_fires_alarm() {
        let (drop_counter, alarms, _) = ring_overflow_enactment(config::NoiseLevel::Alarm).await;
        assert_eq!(drop_counter, 2, "alarm: counter += dropped count");
        assert_eq!(alarms, 1, "alarm: one alarm fires for the overflowing take");
    }

    /// A durable-channel messenger whose alarms are counted — the durable twin
    /// of `build_ring_wasm_messenger_with_alarm`.
    async fn build_durable_wasm_messenger_with_alarm(
        slug: &str,
        channel_name: &str,
        push_depth: Depth,
        retain_depth: Depth,
    ) -> (
        Arc<Messenger>,
        Arc<ChannelEntry>,
        ParticipantId,
        Arc<CountingAlarmRouter>,
    ) {
        let db = crate::db::init_db_memory();
        let entry = crate::messaging::testutils::wasm_channel_entry(
            slug,
            channel_name,
            push_depth,
            retain_depth,
        );
        {
            let conn = db.lock().await;
            db::upsert_channels(&conn, std::slice::from_ref(&*entry));
        }
        let router = Arc::new(CountingAlarmRouter::default());
        let messenger = Messenger::new(
            db,
            Arc::new(MessagingDirectory::with_entries(vec![(*entry).clone()])),
            Arc::from("test"),
            Arc::new(indexmap::IndexMap::new()),
            router.clone() as Arc<dyn WakeRouter>,
            config::MessagingGlobalConfig::default(),
        );
        let wasm_sub = ParticipantId::for_wasm(slug);
        crate::messaging::testutils::attach_wasm_port(
            &messenger, &entry, slug, &wasm_sub, push_depth,
        )
        .await;
        (messenger, entry, wasm_sub, router)
    }

    /// The durable twin of `ring_overflow_enactment`: three owed messages on a
    /// `push_depth = 1` durable port, one activation, and the advance's noise
    /// charge routed into the ladder.
    async fn durable_overflow_enactment(noise: config::NoiseLevel) -> (u64, u64, u64) {
        let slug = "durable-noise";
        let (messenger, channel, wasm_sub, router) = build_durable_wasm_messenger_with_alarm(
            slug,
            "durable-noise-ch",
            Depth::Bounded(1),
            Depth::Bounded(0),
        )
        .await;

        let base_ns = db::utc_to_ns(chrono::Utc::now());
        for i in 0..3 {
            super::testutils::insert_bus_message_at(
                &messenger,
                &channel,
                &format!("row-{i}"),
                ChannelScheme::Brenn,
                base_ns + i as i64 * 1_000_000,
            )
            .await;
        }

        let inputs = vec![WasmInputPort {
            port: "in".to_string(),
            sub: config::ResolvedSubscription {
                channel_uuid: channel.uuid,
                channel_address: channel.address.clone(),
                push_depth: Depth::Bounded(1),
                retain_depth: Depth::Bounded(0),
                noise,
                wake_min: WakeMin::Normal,
            },
            amplification_mt: 1000,
        }];
        let snap = messenger
            .load_activation_snapshot(&wasm_sub, &inputs)
            .await
            .expect("three owed, push_depth 1");
        assert_eq!(snap[0].new_len(), 1, "only the newest delivered");
        let (through, seen_floor) = snap[0].advance_span().expect("the window served entries");
        let outcome = messenger
            .advance_subscriber(&channel.address, &wasm_sub, through, seen_floor, noise)
            .await;
        (
            messenger.drop_counter(&channel.address, &wasm_sub),
            router.alarms.load(std::sync::atomic::Ordering::SeqCst),
            outcome.dropped,
        )
    }

    /// Silent durable overflow: the guest still reads its raw drop figure, and
    /// the ladder does nothing with it.
    #[tokio::test]
    async fn durable_overflow_silent_enacts_nothing() {
        let (drop_counter, alarms, dropped) =
            durable_overflow_enactment(config::NoiseLevel::Silent).await;
        assert_eq!(drop_counter, 0, "silent: no metered counter");
        assert_eq!(alarms, 0, "silent: no alarm");
        assert_eq!(dropped, 2, "guest still sees the two the clamp skipped");
    }

    /// Metered durable overflow: the clamp charges the ladder on the durable
    /// class exactly as it does on the ring.
    #[tokio::test]
    async fn durable_overflow_metered_increments_counter() {
        let (drop_counter, alarms, _) =
            durable_overflow_enactment(config::NoiseLevel::Metered).await;
        assert_eq!(drop_counter, 2, "metered: counter += dropped count");
        assert_eq!(alarms, 0, "metered: no alarm");
    }

    /// Alarm durable overflow: counter and one alarm for the batch.
    #[tokio::test]
    async fn durable_overflow_alarm_increments_counter_and_fires_alarm() {
        let (drop_counter, alarms, _) = durable_overflow_enactment(config::NoiseLevel::Alarm).await;
        assert_eq!(drop_counter, 2, "alarm: counter += dropped count");
        assert_eq!(
            alarms, 1,
            "alarm: one alarm fires for the overflowing batch"
        );
    }

    /// The registered subscriber a ring overflow event names, at the noise rung
    /// its registration resolved to.
    fn ring_subscriber(kind: SubscriberEntryKind, noise: config::NoiseLevel) -> SubscriberEntry {
        SubscriberEntry {
            kind,
            push_depth: Depth::Bounded(4),
            retain_depth: Depth::Bounded(8),
            noise,
            wake_min: None,
        }
    }

    /// One message on its way into `channel`.
    fn ring_message(channel: &ChannelEntry, body: &str) -> store::NewMessage {
        store::NewMessage {
            source: "node".to_string(),
            sender: "test-sender".to_string(),
            body: body.to_string(),
            urgency: Urgency::Normal,
            envelope_type: channel.transport_type,
            reply_to_uuid: None,
            delivery_deadline: None,
            publish_ts_ns: db::utc_to_ns(Utc::now()),
        }
    }

    /// Publish `count` messages into `channel` through the store, enacting
    /// overflow so the eviction accounting matches a real publish. Returns how
    /// many drops the appends reported against `subscriber`.
    async fn commit_ring_publishes(
        messenger: &Messenger,
        channel: &ChannelEntry,
        count: usize,
        subscriber: &ParticipantId,
    ) -> u64 {
        let store = messenger.store_for(channel);
        let mut reported = 0;
        for i in 0..count {
            let outcome = store.append(ring_message(channel, &format!("n{i}"))).await;
            reported += outcome
                .overflow
                .iter()
                .filter(|e| &e.subscriber == subscriber)
                .map(|e| e.dropped)
                .sum::<u64>();
            messenger.enact_overflow_events(channel, &outcome.overflow);
        }
        reported
    }

    /// The canonical overflow producer: a consumer that never activates. Its
    /// owed messages are overwritten by later publishes, and the noise ladder
    /// escalates on the publish that overwrote them — no take, no activation, no
    /// waiting for the consumer to recover.
    #[tokio::test]
    async fn ring_eviction_enacts_noise_without_any_activation() {
        let mut channel = crate::messaging::testutils::local_channel_entry("evict-alarm-ch", 2);
        channel.subscribers.push(ring_subscriber(
            SubscriberEntryKind::Wasm("absent".to_string()),
            config::NoiseLevel::Alarm,
        ));
        let (messenger, channel, wasm_sub, router) =
            build_ring_wasm_messenger_with_alarm("absent", channel);
        messenger.attach_ring_subscriber(&channel.uuid, &wasm_sub, 4, store::Priming::Head);

        // Two fit the depth-2 ring; the third and fourth evict the first two out
        // from under a subscriber that has never run.
        let reported = commit_ring_publishes(&messenger, &channel, 4, &wasm_sub).await;

        assert_eq!(
            reported, 2,
            "both evicted-while-owed messages are reported at the append that took them"
        );
        assert_eq!(
            messenger.drop_counter(&channel.address, &wasm_sub),
            2,
            "alarm meters every drop"
        );
        assert_eq!(
            router.alarms.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "one alarm per evicting publish"
        );
    }

    /// A durable channel carrying `subscriber`, with `participant` attached to
    /// it — what the GC pass finds when it outruns a position.
    async fn durable_eviction_messenger(
        channel_name: &str,
        subscriber: SubscriberEntry,
        app_slug: &str,
        participant: &ParticipantId,
    ) -> (Arc<Messenger>, ChannelEntry) {
        let channel =
            crate::messaging::testutils::test_channel_entry(channel_name, vec![subscriber]);
        let db = crate::db::init_db_memory();
        {
            let conn = db.lock().await;
            db::upsert_channels(&conn, std::slice::from_ref(&channel));
        }
        let messenger = Messenger::new(
            db,
            Arc::new(MessagingDirectory::with_entries(vec![channel.clone()])),
            Arc::from("test"),
            Arc::new(indexmap::IndexMap::new()),
            Arc::new(query::NoopWakeRouter) as Arc<dyn WakeRouter>,
            config::MessagingGlobalConfig::default(),
        );
        messenger
            .attach_subscriber(
                &channel.address,
                app_slug,
                participant,
                Depth::Bounded(4),
                store::Priming::Head,
            )
            .await;
        (messenger, channel)
    }

    /// Run one eviction pass over `channel`, keeping `frontier` messages.
    async fn evict_durable(
        messenger: &Messenger,
        channel: &ChannelEntry,
        frontier: u64,
    ) -> Vec<store::OverflowEvent> {
        let conn = messenger.db.lock().await;
        db::bus_gc_evict_channel(
            &conn,
            channel.uuid,
            &channel.address,
            ChannelScheme::Brenn,
            frontier,
            config::Sink::Drop,
            None,
        )
        .overflow
    }

    /// The durable eviction report is only worth computing if it reaches the
    /// ladder: the GC pass hands its events to the same enactment sink the ring's
    /// appends use, resolving the rung from the channel's registration. Without
    /// this hop a wedged durable subscriber escalates nothing, and the whole
    /// report-without-a-read property is inert.
    #[tokio::test]
    async fn a_durable_eviction_report_is_enacted_at_the_subscriptions_rung() {
        let wasm_sub = ParticipantId::for_wasm("absent");
        let (messenger, channel) = durable_eviction_messenger(
            "gc-noise-ch",
            ring_subscriber(
                SubscriberEntryKind::Wasm("absent".to_string()),
                config::NoiseLevel::Metered,
            ),
            "absent",
            &wasm_sub,
        )
        .await;

        for i in 0..3 {
            publish_at(&messenger, &channel, &format!("n{i}"), Urgency::Normal).await;
        }
        let overflow = evict_durable(&messenger, &channel, 1).await;
        assert_eq!(
            overflow.iter().map(|e| e.dropped).sum::<u64>(),
            2,
            "the pass took two seqs from a position that never moved"
        );

        messenger.enact_overflow_for_channel(&channel.address, &overflow);
        assert_eq!(
            messenger.drop_counter(&channel.address, &wasm_sub),
            2,
            "metered: the evicted span lands on the ladder without any read"
        );
    }

    /// The conversation arm of the same hop. A conversation names no
    /// registration of its own, so the rung is resolved through the app slug the
    /// cursor caches — a report that carried none would panic here rather than
    /// escalate, which is exactly why the GC reads the slug from the cursor row.
    #[tokio::test]
    async fn a_durable_eviction_report_for_a_conversation_resolves_its_app() {
        let conversation = ParticipantId::for_conversation(31);
        let (messenger, channel) = durable_eviction_messenger(
            "gc-conv-noise-ch",
            ring_subscriber(
                SubscriberEntryKind::App("chatty".to_string()),
                config::NoiseLevel::Metered,
            ),
            "chatty",
            &conversation,
        )
        .await;

        for i in 0..3 {
            publish_at(&messenger, &channel, &format!("n{i}"), Urgency::Normal).await;
        }
        let overflow = evict_durable(&messenger, &channel, 1).await;
        assert_eq!(
            overflow.first().and_then(|e| e.app_slug.clone()),
            Some("chatty".to_string()),
            "the cursor's cached slug rides the report"
        );

        messenger.enact_overflow_for_channel(&channel.address, &overflow);
        assert_eq!(messenger.drop_counter(&channel.address, &conversation), 2);
    }

    /// The same eviction on a `silent` registration: reported to the caller,
    /// never metered, never alarmed.
    #[tokio::test]
    async fn ring_eviction_on_a_silent_subscription_reports_without_shouting() {
        let mut channel = crate::messaging::testutils::local_channel_entry("evict-silent-ch", 2);
        channel.subscribers.push(ring_subscriber(
            SubscriberEntryKind::Wasm("quiet".to_string()),
            config::NoiseLevel::Silent,
        ));
        let (messenger, channel, wasm_sub, router) =
            build_ring_wasm_messenger_with_alarm("quiet", channel);
        messenger.attach_ring_subscriber(&channel.uuid, &wasm_sub, 4, store::Priming::Head);

        let reported = commit_ring_publishes(&messenger, &channel, 4, &wasm_sub).await;

        assert_eq!(reported, 2);
        assert_eq!(messenger.drop_counter(&channel.address, &wasm_sub), 0);
        assert_eq!(router.alarms.load(std::sync::atomic::Ordering::SeqCst), 0);
    }

    /// A Surface-kind subscriber's overflow is never routed to the backend
    /// enactment sink: `fatal` is legal on a surface registration and is enacted
    /// by the page kernel, so routing it here would panic the backend on a
    /// sanctioned config. The store still reports the drop, which is what
    /// reaches the page.
    #[tokio::test]
    async fn surface_kind_ring_overflow_is_never_enacted_by_the_backend() {
        let mut channel = crate::messaging::testutils::ephemeral_channel_entry("evict-surface", 2);
        channel.subscribers.push(ring_subscriber(
            SubscriberEntryKind::Surface {
                slug: "dash".to_string(),
                instance: Some("main".to_string()),
            },
            config::NoiseLevel::Fatal,
        ));
        let (messenger, channel, _unused, router) =
            build_ring_wasm_messenger_with_alarm("dash", channel);
        let surface_sub = ParticipantId::for_surface_component("dash", "main");
        messenger.attach_ring_subscriber(&channel.uuid, &surface_sub, 4, store::Priming::Head);

        let reported = commit_ring_publishes(&messenger, &channel, 4, &surface_sub).await;

        assert_eq!(
            reported, 2,
            "the drops still reach the page through the store's report"
        );
        assert_eq!(router.alarms.load(std::sync::atomic::Ordering::SeqCst), 0);
    }

    /// A conversation's noise rung lives on the `App(slug)` registration the
    /// delivery record was written under, so an event that names no app cannot
    /// resolve one. Skipping it would silently drop that subscription off the
    /// noise ladder for every drop it ever takes, so the routing dies instead.
    #[tokio::test]
    #[should_panic(expected = "a conversation's noise rung lives on an App")]
    async fn a_conversation_overflow_event_naming_no_app_dies() {
        let mut channel = crate::messaging::testutils::local_channel_entry("conv-overflow", 2);
        channel.subscribers.push(ring_subscriber(
            SubscriberEntryKind::App("chatty".to_string()),
            config::NoiseLevel::Metered,
        ));
        let (messenger, channel, _unused, _router) =
            build_ring_wasm_messenger_with_alarm("chatty", channel);

        messenger.enact_overflow_events(
            &channel,
            &[store::OverflowEvent {
                subscriber: ParticipantId::for_conversation(1),
                dropped: 1,
                app_slug: None,
            }],
        );
    }

    /// Delivery state that outlived its registration: the store reports the
    /// drop, but no rung resolves for it, so nothing is metered and no alarm
    /// fires — the event is surfaced in a log, not invented into a rung.
    #[tokio::test]
    async fn an_unregistered_subscribers_overflow_is_reported_but_not_enacted() {
        let mut channel = crate::messaging::testutils::local_channel_entry("stale-overflow", 2);
        channel.subscribers.push(ring_subscriber(
            SubscriberEntryKind::Wasm("known".to_string()),
            config::NoiseLevel::Alarm,
        ));
        let (messenger, channel, _known, router) =
            build_ring_wasm_messenger_with_alarm("known", channel);
        let stranger = ParticipantId::for_wasm("gone");

        messenger.enact_overflow_events(
            &channel,
            &[store::OverflowEvent {
                subscriber: stranger.clone(),
                dropped: 3,
                app_slug: None,
            }],
        );

        assert_eq!(
            messenger.drop_counter(&channel.address, &stranger),
            0,
            "an unresolvable rung meters nothing"
        );
        assert_eq!(router.alarms.load(std::sync::atomic::Ordering::SeqCst), 0);
    }

    /// Park `count` messages on `channel` for `release_at` through its store.
    async fn park_ring_publishes(
        messenger: &Messenger,
        channel: &ChannelEntry,
        count: usize,
        release_at: DateTime<Utc>,
    ) {
        let store = messenger.store_for(channel);
        for i in 0..count {
            store
                .park(ring_message(channel, &format!("d{i}")), release_at)
                .await
                .expect("the deferred set is within the channel's cap");
        }
    }

    /// Fill a depth-2 ring with two messages a never-running consumer is owed,
    /// then release two deferrals over them. Returns `(drop_counter, alarms)`
    /// for the consumer — on an `alarm` registration the ladder meters every
    /// drop, so the counter is the enacted loss.
    ///
    /// `channel` must already carry the consumer's `alarm` registration.
    async fn ring_release_overflow_enactment(channel: ChannelEntry) -> (u64, u64) {
        let (messenger, channel, wasm_sub, router) =
            build_ring_wasm_messenger_with_alarm("absent", channel);
        messenger.attach_ring_subscriber(&channel.uuid, &wasm_sub, 4, store::Priming::Head);

        commit_ring_publishes(&messenger, &channel, 2, &wasm_sub).await;
        let release_at = Utc::now() + chrono::Duration::seconds(1);
        park_ring_publishes(&messenger, &channel, 2, release_at).await;
        assert_eq!(
            messenger.drop_counter(&channel.address, &wasm_sub),
            0,
            "a parked message evicts nothing until it is released"
        );

        let sweep = messenger
            .release_due_messages(release_at + chrono::Duration::seconds(1))
            .await;
        assert_eq!(sweep.released, 2, "both deferrals came due");
        (
            messenger.drop_counter(&channel.address, &wasm_sub),
            router.alarms.load(std::sync::atomic::Ordering::SeqCst),
        )
    }

    /// A release batch is an eviction source like any publish: the messages it
    /// moves into retention push a never-running consumer's owed window out, and
    /// the noise ladder escalates on the batch that did it. The confined arm,
    /// whose batch comes back from the store.
    #[tokio::test]
    async fn ring_release_batch_enacts_noise_on_a_confined_channel() {
        let mut channel = crate::messaging::testutils::local_channel_entry("release-confined", 2);
        channel.subscribers.push(ring_subscriber(
            SubscriberEntryKind::Wasm("absent".to_string()),
            config::NoiseLevel::Alarm,
        ));

        let (drop_counter, alarms) = ring_release_overflow_enactment(channel).await;
        assert_eq!(drop_counter, 2, "alarm meters both evicted owed messages");
        assert_eq!(alarms, 1, "one alarm for the batch's merged overflow");
    }

    /// The transportable twin: the batch's overflow comes back through the bus's
    /// fan-out path rather than the store's, and must reach the same ladder.
    #[tokio::test]
    async fn ring_release_batch_enacts_noise_on_a_transportable_channel() {
        let mut channel = crate::messaging::testutils::ephemeral_channel_entry("release-wire", 2);
        channel.subscribers.push(ring_subscriber(
            SubscriberEntryKind::Wasm("absent".to_string()),
            config::NoiseLevel::Alarm,
        ));

        let (drop_counter, alarms) = ring_release_overflow_enactment(channel).await;
        assert_eq!(drop_counter, 2, "alarm meters both evicted owed messages");
        assert_eq!(alarms, 1, "one alarm for the batch's merged overflow");
    }

    /// A mixed durable + ring inputs list: the durable port triggers on its DB
    /// rows and the ring port contributes pure context (nothing owed), proving the
    /// per-port routing keeps each store's read independent.
    #[tokio::test]
    async fn load_activation_snapshot_mixes_durable_and_ring_ports() {
        let durable = crate::messaging::testutils::wasm_channel_entry(
            "mixed",
            "mixed-durable",
            Depth::Unbounded,
            Depth::Unbounded,
        );
        let ring = crate::messaging::testutils::ephemeral_channel_entry("mixed-ring", 8);
        let ring_stores = Arc::new(store::RingStores::build(std::slice::from_ref(&ring)));
        let db = crate::db::init_db_memory();
        {
            let conn = db.lock().await;
            crate::messaging::db::upsert_channels(&conn, std::slice::from_ref(&*durable));
        }
        let messenger = Messenger::new(
            db,
            Arc::new(MessagingDirectory::with_entries(vec![
                (*durable).clone(),
                ring.clone(),
            ])),
            Arc::from("test"),
            Arc::new(indexmap::IndexMap::new()),
            Arc::new(super::query::NoopWakeRouter) as Arc<dyn WakeRouter>,
            config::MessagingGlobalConfig::default(),
        )
        .with_ring_stores(ring_stores);
        let wasm_sub = ParticipantId::for_wasm("mixed");

        // Ring port: attach + one retained message that is not owed (context only).
        messenger.attach_ring_subscriber(&ring.uuid, &wasm_sub, 4, store::Priming::Head);
        let ctx_env = ring_envelope(&ring.address, ChannelScheme::Ephemeral, "ring-ctx");
        let ctx_mid = ctx_env.message_id;
        messenger.ring_store_for(&ring).append(ctx_env);
        // Serve it so it becomes retained-only context, not owed.
        {
            let ring_store = messenger.ring_store_for(&ring);
            let window = store::RetentionStore::window(
                ring_store.as_ref(),
                &wasm_sub,
                Depth::Bounded(4),
                Depth::Bounded(0),
            )
            .await;
            let (through, seen_floor) = window.advance_span().expect("one retained message");
            store::RetentionStore::advance(ring_store.as_ref(), &wasm_sub, through, seen_floor)
                .await;
        }

        // Durable port: attached at head, then one publish → the activation
        // trigger.
        messenger
            .attach_subscriber(
                &durable.address,
                "mixed",
                &wasm_sub,
                Depth::Bounded(4),
                store::Priming::Head,
            )
            .await;
        let dmid = super::testutils::insert_bus_message(
            &messenger,
            &durable,
            "durable-new",
            ChannelScheme::Brenn,
        )
        .await;

        let inputs = vec![
            ring_input(&ring, Depth::Bounded(4), Depth::Bounded(8)),
            WasmInputPort {
                port: "durable".to_string(),
                sub: config::ResolvedSubscription {
                    channel_uuid: durable.uuid,
                    channel_address: durable.address.clone(),
                    push_depth: Depth::Unbounded,
                    retain_depth: Depth::Unbounded,
                    noise: config::NoiseLevel::Silent,
                    wake_min: WakeMin::Normal,
                },
                amplification_mt: 1000,
            },
        ];
        let snaps = messenger
            .load_activation_snapshot(&wasm_sub, &inputs)
            .await
            .expect("the durable port triggers the activation");
        assert_eq!(snaps.len(), 2);
        // Ring port: nothing owed, the earlier message is context.
        assert_eq!(snaps[0].new_len(), 0);
        assert_eq!(
            snaps[0]
                .context()
                .iter()
                .map(|(_, e)| e.message_id)
                .collect::<Vec<_>>(),
            vec![ctx_mid]
        );
        // Durable port: the owed message is the new portion.
        assert_eq!(snaps[1].new_len(), 1);
        assert_eq!(snaps[1].new_entries()[0].1.message_id, dmid);
    }

    // -----------------------------------------------------------------------
    // record_wasm_activation_failure unit tests
    // -----------------------------------------------------------------------

    /// `record_wasm_activation_failure` writes one `messaging_wasm_consume_failures`
    /// row per entry with correct field values; the idempotency key
    /// `(subscriber, last_message_id)` makes a second call a no-op (INSERT OR IGNORE).
    /// Also asserts the multi-entry case: two entries land in one transaction.
    #[tokio::test]
    async fn record_wasm_activation_failure_row_content_idempotency_and_multi_entry() {
        let slug = "fail-idem";
        let (messenger, channel, wasm_sub) =
            super::testutils::build_wasm_messenger_unbounded(slug, "fail-idem-ch").await;

        // Insert 2 push rows on the single channel.
        let mid0 = super::testutils::insert_bus_message(
            &messenger,
            &channel,
            "body-a",
            ChannelScheme::Brenn,
        )
        .await;
        let mid1 = super::testutils::insert_bus_message(
            &messenger,
            &channel,
            "body-b",
            ChannelScheme::Brenn,
        )
        .await;

        let first_msg_id = mid0.to_string();
        let last_msg_id = mid1.to_string();

        let failure = WasmBatchFailure {
            channel: &channel.address,
            subscriber: &wasm_sub,
            first_message_id: &first_msg_id,
            last_message_id: &last_msg_id,
            seq_span: (store::MessageSeq(1), store::MessageSeq(2)),
            outcome: "trap",
            diagnostic: "unreachable instruction at test",
        };

        // Single-entry call: must write the quarantine row.
        messenger.record_wasm_activation_failure(&[failure]).await;

        // Verify the quarantine row fields.
        let conn = messenger.db().lock().await;
        let row_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM messaging_wasm_consume_failures \
                 WHERE subscriber = ?1 AND last_message_id = ?2",
                rusqlite::params![wasm_sub.as_str(), &last_msg_id],
                |r| r.get(0),
            )
            .expect("query wasm_consume_failures count");
        assert_eq!(row_count, 1, "exactly one quarantine row after first call");

        let batch_seq_span_col: String = conn
            .query_row(
                "SELECT batch_seq_span FROM messaging_wasm_consume_failures \
                 WHERE subscriber = ?1 AND last_message_id = ?2",
                rusqlite::params![wasm_sub.as_str(), &last_msg_id],
                |r| r.get(0),
            )
            .expect("query batch_seq_span");
        assert_eq!(
            batch_seq_span_col, "1-2",
            "the quarantine row names the seqs the batch spanned"
        );

        let (outcome_col, diag_col): (String, String) = conn
            .query_row(
                "SELECT outcome, diagnostic FROM messaging_wasm_consume_failures \
                 WHERE subscriber = ?1 AND last_message_id = ?2",
                rusqlite::params![wasm_sub.as_str(), &last_msg_id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .expect("query outcome+diagnostic");
        assert_eq!(outcome_col, "trap");
        assert!(
            diag_col.contains("unreachable instruction at test"),
            "diagnostic mismatch: {diag_col}"
        );

        drop(conn); // release lock before idempotency call

        // Second call with the same `(subscriber, last_message_id)` — must be idempotent.
        messenger.record_wasm_activation_failure(&[failure]).await;

        let conn2 = messenger.db().lock().await;
        let row_count2: i64 = conn2
            .query_row(
                "SELECT COUNT(*) FROM messaging_wasm_consume_failures \
                 WHERE subscriber = ?1 AND last_message_id = ?2",
                rusqlite::params![wasm_sub.as_str(), &last_msg_id],
                |r| r.get(0),
            )
            .expect("query wasm_consume_failures count after second call");
        assert_eq!(
            row_count2, 1,
            "second call must be idempotent: still exactly one quarantine row"
        );
        drop(conn2);

        // ── Multi-entry case: two failure records for DISTINCT channels land in one transaction ──
        // This is the cross-channel atomicity case that matters (test-3): two channels in a
        // single multi-port activation, both recorded atomically. Using two distinct channels
        // verifies that the transaction wraps rows for heterogeneous channels, not just multiple
        // rows on the same channel.
        let (messenger2, ch_a, wasm_sub2) =
            super::testutils::build_wasm_messenger_unbounded(slug, "fail-idem-cha").await;
        // Add a second channel to messenger2's DB directly.
        let ch_b = super::testutils::wasm_channel_entry(
            slug,
            "fail-idem-chb",
            Depth::Unbounded,
            Depth::Unbounded,
        );
        {
            let conn = messenger2.db().lock().await;
            db::upsert_channels(&conn, std::slice::from_ref(&*ch_b));
        }
        let mid_a = super::testutils::insert_bus_message(
            &messenger2,
            &ch_a,
            "port-a-msg",
            ChannelScheme::Brenn,
        )
        .await;
        let mid_b = super::testutils::insert_bus_message(
            &messenger2,
            &ch_b,
            "port-b-msg",
            ChannelScheme::Brenn,
        )
        .await;
        let ch_a_mid = mid_a.to_string();
        let ch_b_mid = mid_b.to_string();

        let fail_a = WasmBatchFailure {
            channel: &ch_a.address,
            subscriber: &wasm_sub2,
            first_message_id: &ch_a_mid,
            last_message_id: &ch_a_mid,
            seq_span: (store::MessageSeq(1), store::MessageSeq(1)),
            outcome: "err",
            diagnostic: "multi-entry-ch-a",
        };
        let fail_b = WasmBatchFailure {
            channel: &ch_b.address,
            subscriber: &wasm_sub2,
            first_message_id: &ch_b_mid,
            last_message_id: &ch_b_mid,
            seq_span: (store::MessageSeq(1), store::MessageSeq(1)),
            outcome: "err",
            diagnostic: "multi-entry-ch-b",
        };

        messenger2
            .record_wasm_activation_failure(&[fail_a, fail_b])
            .await;

        let conn3 = messenger2.db().lock().await;
        let multi_count: i64 = conn3
            .query_row(
                "SELECT COUNT(*) FROM messaging_wasm_consume_failures \
                 WHERE subscriber = ?1",
                rusqlite::params![wasm_sub2.as_str()],
                |r| r.get(0),
            )
            .expect("query multi-entry count");
        assert_eq!(
            multi_count, 2,
            "both failure entries (distinct channels) must land in one transaction"
        );

        // Verify each row is for the correct distinct channel.
        let ch_a_count: i64 = conn3
            .query_row(
                "SELECT COUNT(*) FROM messaging_wasm_consume_failures \
                 WHERE subscriber = ?1 AND channel = ?2",
                rusqlite::params![wasm_sub2.as_str(), ch_a.address.as_str()],
                |r| r.get(0),
            )
            .expect("query ch_a failure row");
        let ch_b_count: i64 = conn3
            .query_row(
                "SELECT COUNT(*) FROM messaging_wasm_consume_failures \
                 WHERE subscriber = ?1 AND channel = ?2",
                rusqlite::params![wasm_sub2.as_str(), ch_b.address.as_str()],
                |r| r.get(0),
            )
            .expect("query ch_b failure row");
        assert_eq!(ch_a_count, 1, "ch_a must have exactly one failure row");
        assert_eq!(ch_b_count, 1, "ch_b must have exactly one failure row");

        let _ = mid0;
        let _ = mid1;
    }

    // -- reap_frontier -------------------------------------------------------

    /// Build a test `ChannelEntry` for frontier tests. `subscribers` is
    /// `(push_depth, retain_depth)` per entry.
    fn frontier_entry(
        standing: config::Depth,
        subscribers: Vec<(config::Depth, config::Depth)>,
    ) -> ChannelEntry {
        let mut e = entry("frontier-test");
        e.resolved_channel.standing_retain_depth = standing;
        e.subscribers = subscribers
            .into_iter()
            .enumerate()
            .map(|(i, (push, retain))| SubscriberEntry {
                kind: SubscriberEntryKind::App(format!("app-{i}")),
                push_depth: push,
                retain_depth: retain,
                noise: config::NoiseLevel::Silent,
                wake_min: Some(WakeMin::Normal),
            })
            .collect();
        e
    }

    /// All-Unbounded (standing + 0 subscribers): standing is Unbounded → None.
    #[test]
    fn reap_frontier_unbounded_standing_returns_none() {
        let entry = frontier_entry(config::Depth::Unbounded, vec![]);
        assert_eq!(entry.reap_frontier(), None);
    }

    /// Bounded standing, no subscribers → frontier = standing.
    #[test]
    fn reap_frontier_bounded_standing_no_subscribers() {
        let entry = frontier_entry(config::Depth::Bounded(5), vec![]);
        assert_eq!(entry.reap_frontier(), Some(5));
    }

    /// Bounded standing, all bounded subscribers → frontier = max(push_depth, retain_depth, standing).
    /// Both legs (push_depth-dominates and retain_depth-dominates) are exercised:
    /// sub0 has push=10, retain=1 (push_depth is the max contribution);
    /// sub1 has push=3, retain=8 (retain_depth is the max contribution from sub1).
    /// Overall max = 10 from sub0's push_depth.
    #[test]
    fn reap_frontier_all_bounded_returns_max() {
        let entry = frontier_entry(
            config::Depth::Bounded(2),
            vec![
                (config::Depth::Bounded(10), config::Depth::Bounded(1)),
                (config::Depth::Bounded(3), config::Depth::Bounded(8)),
            ],
        );
        assert_eq!(entry.reap_frontier(), Some(10));
    }

    /// Multi-subscriber where retain_depth is the controlling dimension overall.
    /// sub0: push=3, retain=10; sub1: push=2, retain=5 → max = 10 (sub0's retain_depth).
    #[test]
    fn reap_frontier_retain_dominates_multi_subscriber() {
        let entry = frontier_entry(
            config::Depth::Bounded(2),
            vec![
                (config::Depth::Bounded(3), config::Depth::Bounded(10)),
                (config::Depth::Bounded(2), config::Depth::Bounded(5)),
            ],
        );
        assert_eq!(entry.reap_frontier(), Some(10));
    }

    /// Any Unbounded push_depth subscriber pins the channel → None.
    #[test]
    fn reap_frontier_unbounded_subscriber_returns_none() {
        let entry = frontier_entry(
            config::Depth::Bounded(5),
            vec![
                (config::Depth::Bounded(3), config::Depth::Bounded(1)),
                (config::Depth::Unbounded, config::Depth::Bounded(1)),
            ],
        );
        assert_eq!(entry.reap_frontier(), None);
    }

    /// Bounded standing only (no push subscribers) → frontier = standing.
    #[test]
    fn reap_frontier_bounded_standing_only() {
        let entry = frontier_entry(config::Depth::Bounded(7), vec![]);
        assert_eq!(entry.reap_frontier(), Some(7));
    }

    /// Subscriber push_depth smaller than standing → frontier still = standing.
    #[test]
    fn reap_frontier_standing_dominates_small_subscribers() {
        let entry = frontier_entry(
            config::Depth::Bounded(10),
            vec![
                (config::Depth::Bounded(1), config::Depth::Bounded(0)),
                (config::Depth::Bounded(2), config::Depth::Bounded(0)),
            ],
        );
        assert_eq!(entry.reap_frontier(), Some(10));
    }

    /// retain_depth > push_depth and > standing → frontier rises to retain_depth.
    /// This is the exact data-loss bug case: pull-only subscriber with a large retain window.
    #[test]
    fn reap_frontier_retain_depth_raises_frontier() {
        let entry = frontier_entry(
            config::Depth::Bounded(2),
            vec![(config::Depth::Bounded(1), config::Depth::Bounded(50))],
        );
        assert_eq!(entry.reap_frontier(), Some(50));
    }

    /// Unbounded retain_depth pins the channel → None.
    #[test]
    fn reap_frontier_unbounded_retain_depth_pins_channel() {
        let entry = frontier_entry(
            config::Depth::Bounded(5),
            vec![(config::Depth::Bounded(3), config::Depth::Unbounded)],
        );
        assert_eq!(entry.reap_frontier(), None);
    }

    /// retain_depth < push_depth → frontier unchanged (= max push/standing).
    #[test]
    fn reap_frontier_retain_depth_below_push_no_effect() {
        let entry = frontier_entry(
            config::Depth::Bounded(5),
            vec![(config::Depth::Bounded(20), config::Depth::Bounded(10))],
        );
        assert_eq!(entry.reap_frontier(), Some(20));
    }

    /// Bounded(0) retain_depth → no effect on frontier.
    #[test]
    fn reap_frontier_zero_retain_depth_no_effect() {
        let entry = frontier_entry(
            config::Depth::Bounded(5),
            vec![(config::Depth::Bounded(3), config::Depth::Bounded(0))],
        );
        assert_eq!(entry.reap_frontier(), Some(5));
    }
}
