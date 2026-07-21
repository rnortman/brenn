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
pub mod db;
pub mod dispatcher;
pub mod edit;
pub mod ephemeral;
pub mod format;
pub mod gates;
pub mod identity;
pub mod ingress;
pub mod publish;
pub mod query;
pub mod subscribe;
pub mod system;

#[cfg(any(test, feature = "testutils"))]
pub mod testutils;

#[cfg(test)]
pub(super) mod test_support;

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use chrono::Utc;
use indexmap::IndexMap;
use uuid::Uuid;

use crate::config::{AppConfig, ServerConfig};
use crate::db::{Db, format_ts_for_db};
pub use config::{
    ChannelConfigRaw, DEFAULT_EPHEMERAL_CAPACITY, Depth, EphemeralChannelConfigRaw,
    EphemeralChannelEntry, MessagingConfigRaw, MessagingGlobalConfig, MessagingSubscriptionRaw,
    NoiseLevel, ResolvedChannel, ResolvedMessagingConfig, ResolvedSubscription, Sink,
    WasmConsumerConfigRaw, WasmConsumerSubscriptionRaw, WasmInputPort,
};
pub use edit::{CancelResult, EditFields, EditResult};
pub use ephemeral::{
    EPHEMERAL_SENDER_BURST, EPHEMERAL_SENDER_REFILL_AMOUNT, EPHEMERAL_SENDER_REFILL_INTERVAL,
    EphemeralBus, EphemeralDelivery, EphemeralEvent, EphemeralPublishResult, EphemeralReceiver,
    EphemeralResume, EphemeralSubscribeError, EphemeralSubscription, GapReason, Replay,
};
pub use identity::{ParticipantId, SubscriberKind};
pub use ingress::{
    CollapsedDrain, Event as IngressEvent, IngressOrBus, MAX_DELIVERED_RETENTION_DAYS,
    MAX_REPO_SYNC_STALENESS_DAYS, ONELINE_CAP, REPO_SYNC_KIND_CONFLICT, REPO_SYNC_KIND_LOCAL,
    REPO_SYNC_KIND_PULLED, REPO_SYNC_KIND_SUMMARY, REPO_SYNC_SOURCE_CONFLICT,
    REPO_SYNC_SOURCE_LOCAL, REPO_SYNC_SOURCE_PREFIX, REPO_SYNC_SOURCE_PULLED,
    REPO_SYNC_SOURCE_SUMMARY, SYNTHETIC_EVENT_ID, assert_delivered_retention_days_valid,
    cap_oneline, collapse_repo_sync, format_event_batch, is_repo_sync_source,
    repo_sync_staleness_days, set_repo_sync_staleness_days, split_stale_repo_sync,
};
pub use publish::{
    AnyPublishResult, PublishOrigin, PublishResult, SurfaceBatchPublish, SurfaceSendDraw,
    SurfaceSendVerdict, WasmPublish, is_well_formed_address,
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
/// uniform with the durable/webhook/MQTT channel spaces (`EphemeralChannelEntry`
/// carries it for the `EphemeralBus`). Deterministic across calls, processes,
/// and restarts so every derivation agrees on the same name.
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
/// the §4 window-size bound ("bounded at push_depth + retain_depth") requires
/// clamping both sides when either is `Unbounded`. With a bounded `push_depth`
/// the push-window overflow invariant already keeps undelivered rows ≤ push_depth,
/// so the clamp is only reached for `Unbounded` consumers (correctness-3 / security-3 fix).
pub const WASM_WINDOW_MAX_NEW: u64 = 1_000;

/// One port's slice of a multi-port activation snapshot.
///
/// Assembled by [`Messenger::load_activation_snapshot`] under one lock hold.
/// Fields mirror the per-port shape of [`brenn_wasm::ProcessorPortWindow`] at
/// the messaging layer (pre-serialization).
pub struct PortSnapshot {
    /// Logical input port name (from config).
    pub port: String,
    /// Canonical channel address (e.g. `brenn:my-input`).
    pub channel_address: String,
    /// Clamped new rows `(push_id, envelope)` in scan order. Empty for sampled
    /// ports (`push_depth = Bounded(0)`) and for triggering ports that had no
    /// pending rows this step.
    pub new_rows: Vec<(i64, MessageEnvelope)>,
    /// Retained context in ASC order (oldest first), deduped against `new_rows`.
    /// Empty when `retain_depth = Bounded(0)`.
    pub context: Vec<MessageEnvelope>,
    /// Drop counter value read at snapshot time, while the db lock was held.
    /// Used by `drain_step` for the `dropped` field; reading here keeps the
    /// counter value in the same T₀ snapshot as the pending rows (correctness-1).
    pub drop_counter_snapshot: u64,
    /// Pending rows scanned for this port but excluded from `new_rows` by the
    /// push_depth / `WASM_WINDOW_MAX_NEW` clamp. They remain undelivered in
    /// `messaging_pending_pushes`. 0 means the port was not clamped. Exact at
    /// snapshot time (T₀, under the db lock).
    pub clamped_leftover: usize,
}

/// Parameters for a failed WASM consumer batch disposition (design §3).
/// Passed to [`Messenger::record_wasm_activation_failure`] to avoid the 7-arg clippy warning.
#[derive(Clone, Copy)]
pub struct WasmBatchFailure<'a> {
    pub channel: &'a str,
    pub subscriber: &'a ParticipantId,
    pub first_message_id: &'a str,
    pub last_message_id: &'a str,
    pub push_ids: &'a [i64],
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
    /// Constructed by `finalize_directory_with_subscribers`; the durable
    /// dispatch path (`resolve_push_targets`, `floor_decision`) treats either
    /// grain exactly like an App/Wasm subscriber. Policy resolves via
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
    /// Both `resolve_push_targets` and `register_released_pushes` read this
    /// field directly — no secondary lookup into `messenger.apps` is required.
    pub noise: config::NoiseLevel,
    /// Resolved wake-min policy for this subscription — `Some` iff the subscriber
    /// is `UrgencyGated`.
    ///
    /// Populated once at startup by `finalize_directory_with_subscribers` from
    /// `ResolvedSubscription.wake_min`. Read by `resolve_push_targets`.
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
    /// Deliver inline through `deliver`/`deliver_ingress`. `marks_own_delivery`
    /// is `true` when the binding's own atomic claim already marks the row
    /// delivered (surface sessions), so the dispatcher must not re-mark it (a
    /// batch re-mark would race a session unclaim); `false` when the dispatcher
    /// owns the delivered-mark (conversation bridges).
    Inline { marks_own_delivery: bool },
    /// Never delivered inline: route to the off-loop parked task via
    /// `spawn_eager_wake` and leave the row parked (WASM/system subscribers).
    ParkedWake,
}

/// The default [`DeliveryShape`] for a subscriber kind, mirroring the
/// kind→binding choices bootstrap makes by hand: `App` subscribers deliver
/// inline through their conversation bridge (the dispatcher owns the mark);
/// `Surface` subscribers deliver inline but mark their own delivery via the
/// session claim; `Wasm`/`System` subscribers are parked-and-woken.
///
/// Bootstrap is authoritative: it registers each real binding directly (see
/// the delivery-binding registration in `brenn-server`), and the live dispatch
/// path reads those registered bindings, never this function. This exists only
/// for test doubles / `NoopWakeRouter` impls that need a shape without wiring a
/// full binding map; keep it in step with bootstrap's choices by hand.
pub fn default_delivery_shape(key: &SubscriberEntryKind) -> DeliveryShape {
    match key {
        SubscriberEntryKind::App(_) => DeliveryShape::Inline {
            marks_own_delivery: false,
        },
        SubscriberEntryKind::Surface { .. } => DeliveryShape::Inline {
            marks_own_delivery: true,
        },
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
    /// Inject an envelope into the target subscriber's active bridge, if
    /// any. The implementation renders the envelope to HTML *only after*
    /// confirming a bridge is present — so sleeping targets pay no render
    /// cost and a malformed-envelope panic is confined to the per-bridge
    /// path, not the shared dispatch loop.
    ///
    /// Routes through `send_system_message` in the binary crate so live
    /// delivery and drain delivery emit the same `SystemMessageBroadcast`
    /// wire shape.
    ///
    /// - `Ok(true)` on send success.
    /// - `Ok(false)` if no bridge is active.
    /// - `Err(_)` if the bridge was active but the send failed (treated by
    ///   the caller like `Ok(false)`).
    ///
    /// `push_id` is the `messaging_pending_pushes.id` of the row being
    /// delivered and `seq` is its `messaging_messages.id`. The Conversation arm
    /// ignores both; the Surface arm mints the wire cursor's high-water from
    /// `seq` and uses `push_id` for the at-most-once DB claim.
    async fn deliver(
        &self,
        key: &SubscriberEntryKind,
        subscriber: &ParticipantId,
        envelope: &MessageEnvelope,
        push_id: i64,
        seq: i64,
    ) -> Result<bool, String>;

    /// Row-less deliver-if-attached context feed for a depth-0 (fold-0) surface
    /// subscription. Unlike `deliver`, this creates and claims no
    /// `messaging_pending_pushes` row: a fold-0 subscription has no push window,
    /// so a durable message reaches an attached session only as a live fan-out at
    /// publish time. A session not attached is owed nothing — its retained
    /// context arrives at the next subscribe/resume. `key` is the surface
    /// subscriber's `SubscriberEntryKind::Surface`; `envelope` and `seq`
    /// (`messaging_messages.id`) are the just-committed message.
    ///
    /// Default no-op: only the surface router impl fans out. Test doubles that
    /// never host surface sessions inherit the no-op.
    async fn deliver_context(
        &self,
        key: &SubscriberEntryKind,
        envelope: &Arc<MessageEnvelope>,
        seq: i64,
    ) {
        let _ = (key, envelope, seq);
    }

    /// Cheap precheck for the depth-0 context feed: does any currently-attached
    /// session hold a fold-0 subscription on `channel` for one of `targets`? A
    /// `false` answer lets the publish path skip building the owned,
    /// body-copying context-feed envelope entirely when no page is open — a
    /// deliver-if-attached feed owes a disconnected session nothing (design §6).
    ///
    /// Default `true`: a router that hosts no surface sessions never reaches the
    /// build guard with non-empty targets, so the default costs it nothing.
    fn any_context_session_attached(&self, channel: &str, targets: &[SubscriberEntryKind]) -> bool {
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

    /// The [`DeliveryShape`] of the subscriber registered under `key`, derived
    /// from its delivery binding. `dispatch_row` consults this to choose the
    /// inline-deliver vs. parked-wake path and whether to re-mark delivery.
    fn delivery_shape(&self, key: &SubscriberEntryKind) -> DeliveryShape;

    /// Fire a push-overflow alarm for the given channel + subscriber. Called
    /// when `noise = Alarm` and a push-depth-overflow occurs on the publish
    /// path. The implementation should call `AlertDispatcher::alert` with
    /// `AlertSeverity::Warning`; the rate-limiter on `AlertDispatcher` prevents
    /// flooding. The metric counter is incremented separately before this is
    /// called; `alarm` handles only the alerting side.
    fn alarm(&self, channel: &str, subscriber: &ParticipantId);
}

// ---------------------------------------------------------------------------
// Messenger
// ---------------------------------------------------------------------------

/// Key for per-subscriber push-window tracking: `(channel_address, subscriber_id_str)`.
type PushWindowKey = (String, String);

/// Parameters common to both `record_push_and_check_overflow` and
/// `record_push_batch_and_check_overflow`. Groups the six shared arguments
/// to eliminate the `#[allow(clippy::too_many_arguments)]` suppressor and
/// reduce arg-swap risk between same-typed `&str` parameters.
pub(crate) struct PushRegistration<'a> {
    pub(crate) channel: &'a str,
    pub(crate) channel_uuid: uuid::Uuid,
    pub(crate) app_slug: &'a str,
    pub(crate) subscriber: &'a ParticipantId,
    pub(crate) push_depth: config::Depth,
    pub(crate) noise: config::NoiseLevel,
}

/// Per-subscriber push window: ordered deque of live push_ids (oldest first)
/// plus a `seeded` flag that records whether the deque has been initialised
/// from the DB on first touch after boot.
///
/// `seeded = false` means the key has never been touched in this process;
/// the deque is empty regardless of what the DB holds. On first touch the
/// DB is queried once and `seeded` is set to `true`. The seeded/empty state
/// (`seeded = true`, `ids.is_empty()`) is distinguishable from the never-seeded
/// state (`seeded = false`), which is necessary because the deque can be
/// legitimately empty after seed-then-overflow.
pub(super) struct PushWindow {
    pub(super) seeded: bool,
    pub(super) ids: VecDeque<i64>,
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
    /// Unified subscriber registry: one entry per registered non-app subscriber
    /// (WASM consumer, surface, or system component), keyed by its directory
    /// [`SubscriberEntryKind`]. Holds each subscriber's resolved access-control
    /// policy and its declared [`WakeEconomics`]. Replaces the former per-kind
    /// policy side maps, so both the gated publish path (`publish_from_surface`
    /// / `publish_from_system`) and the delivery-time ACL gate
    /// (`subscriber_policy`) resolve every non-app subscriber through one lookup.
    /// Empty unless populated via `with_subscriber_registrations` at boot.
    ///
    /// App subscribers are not registered here: their policy resolves from
    /// `apps` (which also carries their non-policy `AppConfig` data), and
    /// `subscriber_policy` routes `App(slug)` to `app_policy`.
    pub(crate) subscribers: HashMap<SubscriberEntryKind, SubscriberRegistration>,
    pub(crate) router: Arc<dyn WakeRouter>,
    pub(crate) defaults: MessagingGlobalConfig,
    pub(crate) dispatch_kick_notify: Arc<tokio::sync::Notify>,
    /// In-memory push-window tracking for bounded-`push_depth` subscribers.
    ///
    /// Maps `(channel_address, subscriber_id_str)` → per-subscriber push window.
    ///
    /// Each entry holds a `PushWindow { seeded, ids }`. The deque is ordered
    /// oldest-first; its capacity is the subscriber's `push_depth`. Unbounded
    /// subscribers are never present. On first touch after boot the DB is
    /// queried once to seed the deque (`PushWindow::seeded` guards the one-shot).
    ///
    /// Mutated only under the `Messenger` async context. The outer `Mutex` is
    /// a sync mutex (not async) because all publish operations already hold the
    /// SQLite mutex; the push-window update is a brief in-memory operation.
    pub(super) push_windows: Mutex<HashMap<PushWindowKey, PushWindow>>,
    /// Monotonic per-`(channel, subscriber)` overflow drop counter, incremented
    /// on push-depth-overflow for `metered` and `alarm` noise levels.
    ///
    /// Separate from `push_windows`: the deque tracks *which* push-claim ids
    /// are live; this counter records *how many times* an overflow occurred.
    pub(crate) drop_counters: Mutex<HashMap<PushWindowKey, u64>>,
    /// Count of `load_activation_snapshot` invocations. Monotonically increasing.
    /// Used in tests to assert exactly one subscriber-wide scan per drain step
    /// (AC 7). Not user-visible surface; test instrumentation only.
    /// Access via the public `pending_bus_pushes_scan_count()` accessor.
    pending_bus_pushes_scan_count: AtomicU64,
    /// Ephemeral (best-effort, non-persistent) message bus. `Messenger::new`
    /// installs an empty default bus (zero channels — every ephemeral publish
    /// returns `UnknownChannel`); boot replaces it via `with_ephemeral_bus`
    /// once the resolved `[[ephemeral_channel]]` entries are available.
    ephemeral_bus: Arc<EphemeralBus>,
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
}

// ---------------------------------------------------------------------------
// Sync helper for per-port window assembly (used inside load_activation_snapshot)
// ---------------------------------------------------------------------------

/// Mark a batch of pending-push rows delivered using `prepare_cached` so the
/// statement is compiled once per connection regardless of call count.
///
/// Works on any `&Connection` deref target — a `MutexGuard<Connection>`,
/// a `rusqlite::Transaction`, or a bare `&Connection` all satisfy this.
///
/// Panics on DB error (fail-fast; DB is host infrastructure).
fn retire_push_rows(conn: &rusqlite::Connection, now: &str, ids: &[i64]) {
    let mut stmt = conn
        .prepare_cached(
            "UPDATE messaging_pending_pushes SET delivered_at = ?1 \
             WHERE id = ?2 AND delivered_at IS NULL",
        )
        .expect("retire_push_rows: prepare_cached");
    for &id in ids {
        stmt.execute(rusqlite::params![now, id])
            .unwrap_or_else(|e| panic!("retire_push_rows: retire push {id}: {e}"));
    }
}

/// Output of [`clamp_and_fetch_context`].
struct ClampedWindow {
    new_rows: Vec<(i64, MessageEnvelope)>,
    context: Vec<MessageEnvelope>,
    /// Pending rows scanned for this port but excluded from `new_rows` by the
    /// push_depth / `WASM_WINDOW_MAX_NEW` clamp. They remain undelivered in
    /// `messaging_pending_pushes`. 0 means the port was not clamped.
    clamped_leftover: usize,
}

/// Clamp `raw_rows` to the push-depth limit and fetch retained context for
/// `channel_uuid` — all under an already-held `&Connection`.
///
/// Returns a [`ClampedWindow`]:
/// - `new_rows`: `raw_rows` truncated to `push_depth` (clamped to
///   `WASM_WINDOW_MAX_NEW`). Empty input → empty output (pure-context window).
/// - `context`: most-recent `retain_depth` channel messages in ASC order,
///   deduped against `new_rows`. Empty when `retain_depth = Bounded(0)`
///   (fast-path: no DB query).
/// - `clamped_leftover`: count of `raw_rows` dropped by the clamp (exact,
///   computed here so it stays coupled to the truncation).
///
/// `push_depth` is clamped to `WASM_WINDOW_MAX_NEW`; `retain_depth` is clamped
/// to `WASM_WINDOW_MAX_RETAIN`.
///
/// Panics on any DB error (fail-fast; DB is host infrastructure).
fn clamp_and_fetch_context(
    conn: &rusqlite::Connection,
    channel_uuid: Uuid,
    raw_rows: Vec<(i64, MessageEnvelope)>,
    push_depth: Depth,
    retain_depth: Depth,
) -> ClampedWindow {
    // Capture pre-clamp length so leftover is correct on every return path,
    // including the Bounded(0) retain fast-path below.
    let raw_len = raw_rows.len();

    // Clamp new-rows portion.
    let max_new: usize = match push_depth {
        Depth::Unbounded => WASM_WINDOW_MAX_NEW as usize,
        Depth::Bounded(n) => n.min(WASM_WINDOW_MAX_NEW) as usize,
    };
    let new_rows_clamped: Vec<(i64, MessageEnvelope)> =
        raw_rows.into_iter().take(max_new).collect();
    let clamped_leftover = raw_len - new_rows_clamped.len();

    // retain_depth = Bounded(0) → skip DB query entirely.
    if let Depth::Bounded(0) = retain_depth {
        return ClampedWindow {
            new_rows: new_rows_clamped,
            context: vec![],
            clamped_leftover,
        };
    }

    // Build dedup set from clamped new rows.
    let new_ids: std::collections::HashSet<Uuid> =
        new_rows_clamped.iter().map(|(_, e)| e.message_id).collect();

    // Retained-context read — most-recent N channel messages, DESC order.
    // This read is channel-wide: it may include messages the component was never
    // individually delivered (see the WIT doc on `new-from`). That is the documented
    // contract — the retained context is best-effort channel ambience, not a
    // per-subscriber delivered log.
    let effective_limit: i64 = match retain_depth {
        Depth::Unbounded => WASM_WINDOW_MAX_RETAIN as i64,
        Depth::Bounded(n) => n.min(WASM_WINDOW_MAX_RETAIN) as i64,
    };

    let mut stmt = conn
        .prepare(&format!(
            "{base}WHERE m.channel_uuid = ? {tail}",
            base = query::SELECT_ENVELOPE_BASE,
            tail = query::SELECT_ENVELOPE_ORDER_LIMIT_TAIL,
        ))
        .unwrap_or_else(|e| panic!("clamp_and_fetch_context: prepare recent-read: {e}"));

    let recent_desc: Vec<MessageEnvelope> = stmt
        .query_map(
            rusqlite::params![channel_uuid.as_bytes().to_vec(), effective_limit],
            query::row_to_envelope,
        )
        .unwrap_or_else(|e| panic!("clamp_and_fetch_context: query recent-read: {e}"))
        .map(|r| r.unwrap_or_else(|e| panic!("clamp_and_fetch_context: read row: {e}")))
        .collect();

    // Reverse DESC→ASC and strip ids already in new_rows_clamped.
    let context_asc: Vec<MessageEnvelope> = recent_desc
        .into_iter()
        .rev()
        .filter(|e| !new_ids.contains(&e.message_id))
        .collect();

    ClampedWindow {
        new_rows: new_rows_clamped,
        context: context_asc,
        clamped_leftover,
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

        // Default empty ephemeral bus (zero channels); boot swaps in the
        // config-resolved bus via `with_ephemeral_bus`.
        let ephemeral_bus = EphemeralBus::new(Vec::new(), source.clone(), defaults.max_body_bytes);
        Arc::new(Self {
            db,
            directory,
            source,
            apps,
            subscribers: HashMap::new(),
            router,
            defaults,
            dispatch_kick_notify: Arc::new(tokio::sync::Notify::new()),
            push_windows: Mutex::new(HashMap::<PushWindowKey, PushWindow>::new()),
            drop_counters: Mutex::new(HashMap::<PushWindowKey, u64>::new()),
            pending_bus_pushes_scan_count: AtomicU64::new(0),
            ephemeral_bus,
            surface_send_budgets: HashMap::new(),
        })
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
        for (key, reg) in registrations {
            let prev = inner.subscribers.insert(key.clone(), reg);
            assert!(
                prev.is_none(),
                "with_subscriber_registrations: duplicate registration for {key:?} — \
                 boot wiring bug",
            );
        }
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

    /// Install the config-resolved ephemeral bus before the `Messenger` is
    /// shared, replacing the empty default from `new`. Consumes and returns the
    /// `Arc` because the field is set exactly once at boot, immediately after
    /// `new`, while the `Arc` is still uniquely owned (`Arc::get_mut` therefore
    /// always succeeds). Panics if the `Arc` is already shared — that would be a
    /// boot-ordering bug.
    pub fn with_ephemeral_bus(mut self: Arc<Self>, bus: Arc<EphemeralBus>) -> Arc<Self> {
        let inner = Arc::get_mut(&mut self).expect(
            "with_ephemeral_bus must run before the Messenger Arc is shared (boot-ordering bug)",
        );
        inner.ephemeral_bus = bus;
        self
    }

    /// The ephemeral message bus handle (best-effort, non-persistent).
    pub fn ephemeral_bus(&self) -> &Arc<EphemeralBus> {
        &self.ephemeral_bus
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
        self.subscribers.get(kind)
    }

    /// Resolved access-control policy for a directory subscriber, covering
    /// every subscriber kind: `App(slug)` resolves via `self.apps`; every other
    /// kind resolves through the unified `subscribers` registry. This is the
    /// lookup the delivery-time ACL gate calls (design §2.2) so LLM apps, WASM
    /// consumers, surfaces, and system components are enforced uniformly;
    /// `app_policy` alone cannot reach the non-app subscribers (their policies
    /// are not in `apps`). Every resolved subscriber should carry a policy, so a
    /// `None` for a live subscriber indicates a host wiring bug — the delivery
    /// path treats it as deny (fail-closed), it is not a panic site.
    pub fn subscriber_policy(
        &self,
        kind: &SubscriberEntryKind,
    ) -> Option<&crate::access::AppPolicy> {
        match kind {
            SubscriberEntryKind::App(slug) => self.app_policy(slug),
            other => self.subscribers.get(other).map(|r| r.policy.as_ref()),
        }
    }

    /// Declared [`WakeEconomics`] for a directory subscriber, covering every
    /// subscriber kind: `App(slug)` is `UrgencyGated` iff the app exists (its
    /// economics are sourced from the authoritative `self.apps` map, not the
    /// `subscribers` registry — the same App split `subscriber_policy` makes, so
    /// App policy and economics resolve from one immutable source and cannot
    /// diverge from a registry clone); every other kind resolves through the
    /// registry. `None` for a live subscriber indicates a host wiring bug (the
    /// boot cross-check rejects it); on the delivery path a `None` cannot occur
    /// because the ACL gate (`subscriber_policy`) has already skipped an
    /// unresolvable subscriber.
    ///
    /// This is the single per-participant read that drives eager-wake gating:
    /// `Eager` subscribers are always woken; `UrgencyGated` subscribers consult
    /// `wake_min`. Dispatch never branches on the identity prefix to decide it.
    pub fn subscriber_wake_economics(&self, kind: &SubscriberEntryKind) -> Option<WakeEconomics> {
        match kind {
            SubscriberEntryKind::App(slug) => {
                self.apps.get(slug).map(|_| WakeEconomics::UrgencyGated)
            }
            other => self.subscribers.get(other).map(|r| r.wake),
        }
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
    /// All publish / edit / release callers use this as the single kick surface
    /// (design §2.7, R1). The background dispatcher task holds the matching
    /// `Arc<Notify>`.
    pub fn dispatch_kick(&self) {
        self.dispatch_kick_notify.notify_one();
    }

    /// Read the current drop counter for a `(channel_address, subscriber_id)` pair.
    /// Returns 0 if no overflow has ever occurred for this pair.
    /// Used by the telemetry surface (`req §5.2`).
    pub fn drop_counter(&self, channel: &str, subscriber: &ParticipantId) -> u64 {
        let key = (channel.to_string(), subscriber.as_str().to_string());
        let counters = self.drop_counters.lock().expect("drop_counters poisoned");
        counters.get(&key).copied().unwrap_or(0)
    }

    /// Register a newly-created push-claim id into the in-memory push window
    /// for a bounded-`push_depth` subscriber. If the window is full, removes
    /// and returns the oldest push_id to be retired; also records the overflow
    /// and fires the configured noise signal. Returns the push_id to retire
    /// (if any — `None` when the window has capacity or the subscriber is
    /// unbounded).
    ///
    /// On first touch after boot the deque is seeded from the DB (lazy,
    /// one-shot per key): pre-existing undelivered non-parked push rows are
    /// loaded so the bound is enforced immediately rather than waiting for the
    /// GC backstop pass (~45 min post-boot).
    ///
    /// For released parked rows (`register_released_pushes`), the caller may
    /// pass an id that is already present in the freshly-seeded deque. In that
    /// case `push_id` is skipped (the seed already counted it); no double-count.
    ///
    /// Must be called *after* the push row has been inserted into the DB and
    /// its `push_id` is known. The caller is responsible for deleting the
    /// returned push_id from `messaging_pending_pushes`.
    ///
    /// `conn` — the already-held SQLite connection; used for the one-shot seed
    /// load. Must not be `None` for the first call on a given key.
    pub(crate) fn record_push_and_check_overflow(
        &self,
        reg: &PushRegistration<'_>,
        push_id: i64,
        conn: &rusqlite::Connection,
    ) -> Option<i64> {
        let PushRegistration {
            channel,
            channel_uuid,
            app_slug,
            subscriber,
            push_depth,
            noise,
        } = reg;
        let channel_uuid = *channel_uuid;
        let depth = match push_depth {
            config::Depth::Unbounded => return None, // never tracked, never overflows
            config::Depth::Bounded(0) => {
                // Bounded(0) subscribers never get push rows; this code should
                // not be reached for them — panic if it is (structural bug).
                panic!(
                    "record_push_and_check_overflow called for Bounded(0) subscriber \
                     on channel {channel:?} — Bounded(0) subscribers never get push rows"
                );
            }
            config::Depth::Bounded(n) => *n as usize,
        };

        let key = (channel.to_string(), subscriber.as_str().to_string());
        let retired_id = {
            let mut windows = self.push_windows.lock().expect("push_windows poisoned");
            let window = windows.entry(key.clone()).or_insert(PushWindow {
                seeded: false,
                ids: VecDeque::new(),
            });

            // Lazy first-touch seed: load pre-existing rows from DB exactly once.
            // Excludes `push_id` itself so the seed captures rows that existed
            // before this publish, not the just-inserted new row.
            if !window.seeded {
                let existing =
                    db::load_push_window(conn, channel_uuid, app_slug, subscriber, push_id);
                for id in existing {
                    window.ids.push_back(id);
                }
                // Truncate to push_depth in case the DB drifted past bound before
                // this boot (backstop hadn't run). Seed is side-effect-free on the DB.
                let discarded = window.ids.len().saturating_sub(depth);
                if discarded > 0 {
                    tracing::warn!(
                        channel = channel,
                        subscriber = subscriber.as_str(),
                        push_depth = depth,
                        found_in_db = window.ids.len(),
                        discarded_from_window = discarded,
                        "push window seed: DB has more rows than push_depth (backstop hadn't run); \
                         oldest {discarded} ids discarded from in-memory window (GC backstop will \
                         clean DB rows within ~1 hour)"
                    );
                    while window.ids.len() > depth {
                        window.ids.pop_front();
                    }
                }
                window.seeded = true;
            }

            // For released parked rows: if the id is already present (seed counted
            // it), skip rather than double-count. The membership check is valid here
            // because we are still under the push_windows lock and the seed just ran
            // (or already ran), so no concurrent truncation can race this check.
            // O(n) on `push_depth`, which is always a small bounded integer in
            // practice — a linear scan is acceptable here.
            if window.ids.contains(&push_id) {
                return None; // already counted by seed
            }

            window.ids.push_back(push_id);

            if window.ids.len() <= depth {
                return None; // window has capacity — no overflow
            }

            // Window is full: retire the oldest push-claim.
            window.ids.pop_front().expect("deque non-empty after push")
            // `windows` lock released here before acquiring `drop_counters`.
        };

        // Increment the drop counter for metered/alarm. Lock acquired after
        // `push_windows` is released to avoid a deadlock with any future caller
        // that might acquire `drop_counters` first.
        //
        // TODO(drop-counters-export): `drop_counters` has no production reader —
        // only tests query it. Export it to whatever telemetry surface we settle
        // on (blocked on deciding one), then reconsider the `Silent` default for
        // `noise` (silent-by-default loss only makes sense while the counters
        // are unread; the two decisions are coupled).
        match *noise {
            config::NoiseLevel::Silent => {} // no counter, no alert
            config::NoiseLevel::Metered | config::NoiseLevel::Alarm => {
                let mut counters = self.drop_counters.lock().expect("drop_counters poisoned");
                *counters.entry(key).or_insert(0) += 1;
            }
            config::NoiseLevel::Fatal => {
                // Unreachable by construction: `fatal` is the surface-only kill
                // rung, rejected on every backend subscription where its noise
                // resolves (`resolve_subscription_params`). Reaching it here is a
                // bug in that enforcement.
                panic!(
                    "record_push_and_check_overflow reached noise = fatal on channel {channel:?} \
                     subscriber {} — fatal is surface-only and must never reach the backend \
                     overflow path",
                    subscriber.as_str()
                );
            }
        }
        // Fire alert for alarm level (counter already incremented above).
        if *noise == config::NoiseLevel::Alarm {
            self.router.alarm(channel, subscriber);
        }

        Some(retired_id)
    }

    /// Register a batch of just-released parked push ids for a single
    /// `(channel, subscriber)` key into the in-memory push window.
    ///
    /// Unlike calling `record_push_and_check_overflow` once per id, this
    /// function handles the whole batch atomically under one lock acquisition:
    /// it seeds the deque exactly once, appends only the ids not already present
    /// in the freshly-seeded deque, then trims the front to `push_depth` in one
    /// pass — collecting all retired ids for the caller to delete from DB.
    ///
    /// This is necessary because processing multiple released ids for the same
    /// key one-at-a-time via `record_push_and_check_overflow` causes data loss:
    /// an id evicted by overflow in iteration N is invisible to the `contains`
    /// guard in iteration N+1, so it gets re-added and another live id is
    /// evicted — eventually all N released ids are deleted from the DB.
    ///
    /// Returns `Vec<i64>` of retired ids the caller must `delete_pending_push_by_id`.
    /// Returns empty if the subscriber is `Unbounded` (no tracking).
    ///
    /// `push_ids` must all belong to the same `(channel, subscriber)` key.
    /// The noise / drop-counter / alarm are fired once per *overflow event*
    /// (i.e. once per retired id), not once per batch.
    pub(crate) fn record_push_batch_and_check_overflow(
        &self,
        reg: &PushRegistration<'_>,
        push_ids: &[i64],
        conn: &rusqlite::Connection,
    ) -> Vec<i64> {
        let PushRegistration {
            channel,
            channel_uuid,
            app_slug,
            subscriber,
            push_depth,
            noise,
        } = reg;
        let channel_uuid = *channel_uuid;

        if push_ids.is_empty() {
            return vec![];
        }
        let depth = match push_depth {
            config::Depth::Unbounded => return vec![], // never tracked
            config::Depth::Bounded(0) => panic!(
                "record_push_batch_and_check_overflow called for Bounded(0) subscriber \
                 on channel {channel:?} — Bounded(0) subscribers never get push rows"
            ),
            config::Depth::Bounded(n) => *n as usize,
        };

        let key = (channel.to_string(), subscriber.as_str().to_string());
        let retired_ids: Vec<i64> = {
            let mut windows = self.push_windows.lock().expect("push_windows poisoned");
            let window = windows.entry(key.clone()).or_insert(PushWindow {
                seeded: false,
                ids: VecDeque::new(),
            });

            // Seed exactly once: use any id from the batch as the exclusion id.
            // The seed query already sees all released rows (release_after IS NULL),
            // so any of these ids would be seen by the seed; excluding one avoids
            // double-counting that one via the contains-guard below.
            // We exclude the first id in the batch; the rest are also excluded
            // if already in the freshly-seeded deque by the contains check below.
            if !window.seeded {
                // Pass push_ids[0] as the exclude id — the seed will not include it.
                // The remaining ids in push_ids may be returned by the seed (if they
                // were already released before this call — possible when all were
                // released in the same `release_due_pushes` pass and the seed query
                // runs after).
                let existing =
                    db::load_push_window(conn, channel_uuid, app_slug, subscriber, push_ids[0]);
                for id in existing {
                    window.ids.push_back(id);
                }
                let discarded = window.ids.len().saturating_sub(depth);
                if discarded > 0 {
                    tracing::warn!(
                        channel = channel,
                        subscriber = subscriber.as_str(),
                        push_depth = depth,
                        found_in_db = window.ids.len(),
                        discarded_from_window = discarded,
                        "push window seed: DB has more rows than push_depth (backstop hadn't run); \
                         oldest {discarded} ids discarded from in-memory window (GC backstop will \
                         clean DB rows within ~1 hour)"
                    );
                    while window.ids.len() > depth {
                        window.ids.pop_front();
                    }
                }
                window.seeded = true;
            }

            // Append each id from the batch that is not already in the deque.
            // After seeding (above), the deque contains the authoritative snapshot
            // of pre-existing rows. Any batch id present was already counted by seed;
            // any batch id absent must be added exactly once.
            // All additions happen before any trimming so that eviction-by-trim does
            // not cause a later id in the batch to re-add an earlier one (the
            // correctness-1 bug: per-row calls allow evicted ids to be re-added).
            for &id in push_ids {
                if !window.ids.contains(&id) {
                    window.ids.push_back(id);
                }
            }

            // Trim to push_depth in one pass, collecting all retired ids.
            let mut retired = Vec::new();
            while window.ids.len() > depth {
                retired.push(window.ids.pop_front().expect("deque non-empty during trim"));
            }
            retired
        };

        // Fire noise signals once per overflow event, after releasing push_windows.
        let overflow_count = retired_ids.len();
        if overflow_count > 0 {
            match *noise {
                config::NoiseLevel::Silent => {}
                config::NoiseLevel::Metered | config::NoiseLevel::Alarm => {
                    let mut counters = self.drop_counters.lock().expect("drop_counters poisoned");
                    *counters.entry(key).or_insert(0) += overflow_count as u64;
                }
                config::NoiseLevel::Fatal => {
                    // Unreachable by construction — see the twin in
                    // `record_push_and_check_overflow`. `fatal` is surface-only.
                    panic!(
                        "batch overflow reached noise = fatal on channel {channel:?} subscriber \
                         {} — fatal is surface-only and must never reach the backend overflow \
                         path",
                        subscriber.as_str()
                    );
                }
            }
            if *noise == config::NoiseLevel::Alarm {
                // Fire once per batch regardless of how many overflowed.
                self.router.alarm(channel, subscriber);
            }
        }

        retired_ids
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
        self.directory
            .list()
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

        // brenn: / webhook: — keep directory entries the app's ACL covers. mqtt:
        // directory entries are deliberately skipped here; mqtt: access is sourced
        // from the ACL matchers below, not the directory.
        let mut rows: Vec<ChannelListing> = self
            .directory
            .list()
            .iter()
            .filter_map(|entry| match entry.transport_type {
                ChannelScheme::Webhook if policy.allows_channel_access(&entry.address) => {
                    Some(ChannelListing {
                        protocol: ChannelScheme::Webhook,
                        address: entry.address.clone(),
                        description: entry.description.clone(),
                        access: AccessKind::Existing,
                        details: entry
                            .mount
                            .as_ref()
                            .map(|m| ChannelDetails::Webhook(WebhookDetails { mount: m.clone() })),
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
                // `Ephemeral` channels are never persisted, `local:` never
                // leaves the page (and is declared per-surface, never as a
                // `[[channel]]` block), and `pwa_push:` is an egress-only
                // protocol with no channel rows; none ever appears in the
                // durable messaging directory. One here is a host-wiring
                // invariant violation — fail fast rather than mislabel it
                // (BETTER DEAD THAN WRONG).
                ChannelScheme::Ephemeral | ChannelScheme::Local | ChannelScheme::PwaPush => {
                    panic!(
                        "list_accessible_channels: non-durable channel {:?} in messaging directory \
                         — host-wiring invariant violated",
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
    /// static-vs-dynamic (design §2.4): an O(1) point-lookup against
    /// `messaging_dynamic_subscriptions` (`load_dynamic_subscription_for`). A
    /// durable row present ⇒ `dynamic = true` (runtime, removable); absent ⇒
    /// `dynamic = false` (static, config-managed). The durable table is the single
    /// source of truth — no parallel field on `SubscriberEntry` to drift out of
    /// sync. This is why the method is `async`: it acquires the same `db` lock the
    /// subscribe path takes, once, for the duration of the per-app point-lookups.
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
                // Static-vs-dynamic: durable dynamic row present ⇒ dynamic.
                let dynamic =
                    db::load_dynamic_subscription_for(&conn, entry.uuid, app_slug).is_some();
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
                    // `Ephemeral` channels are never persisted, `local:` never
                    // leaves the page (and is declared per-surface, never as a
                    // `[[channel]]` block), and `pwa_push:` is an egress-only
                    // protocol with no channel rows; none ever appears in the
                    // durable messaging directory. Fail fast per BETTER DEAD THAN WRONG.
                    ChannelScheme::Ephemeral | ChannelScheme::Local | ChannelScheme::PwaPush => {
                        panic!(
                            "list_subscriptions: non-durable channel {:?} in messaging directory \
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

    /// Load undelivered, non-suppressed pending pushes for a subscriber.
    /// Called by the binary crate's drain logic on wake.
    ///
    /// Returns tagged `IngressOrBus` payloads. The drain caller partitions
    /// these into events and envelopes for `render_combined_drain`.
    pub async fn load_pending_pushes(
        &self,
        subscriber: &ParticipantId,
    ) -> Vec<(i64, IngressOrBus)> {
        let conn = self.db.lock().await;
        db::load_pending_pushes_for_drain(&conn, subscriber)
    }

    /// Assemble the full multi-port activation snapshot for `subscriber` under
    /// **one** db-lock acquisition: pending scan + partition + per-port clamp +
    /// per-port retained-context read.
    ///
    /// Returns one [`PortSnapshot`] per input port, in `inputs` order, or
    /// `None` when no triggering input has pending rows (no activation).
    ///
    /// **T₀ hermeticity:** `Messenger` uses one process-wide `Arc<Mutex<Connection>>`;
    /// holding the guard for the whole assembly means no statement from any other
    /// task can interleave — equivalent to a single read transaction. **If
    /// messaging ever grows a second connection (e.g. a read pool), this method
    /// must gain an explicit read transaction.**
    ///
    /// Config-residue reconciliation: rows for a channel that matches no input
    /// (subscription removed) and rows on an input whose `push_depth` is now
    /// `Bounded(0)` (port demoted to sampled after a config change) are retired
    /// (marked delivered) with a `warn!`. Neither case is a bug; leaving them
    /// pending causes a scan loop, so they are retired here.
    ///
    /// Panics on any DB error (fail-fast; the DB is host infrastructure).
    pub async fn load_activation_snapshot(
        &self,
        subscriber: &ParticipantId,
        inputs: &[WasmInputPort],
    ) -> Option<Vec<PortSnapshot>> {
        self.pending_bus_pushes_scan_count
            .fetch_add(1, Ordering::Relaxed);

        let conn = self.db.lock().await;

        // ── Pending scan (absorbed from load_pending_bus_pushes) ───────────
        let all_pending = db::load_pending_pushes_for_drain(&conn, subscriber);
        let mut by_channel: std::collections::HashMap<String, Vec<(i64, MessageEnvelope)>> =
            std::collections::HashMap::new();
        for (push_id, iob) in all_pending {
            // An Ingress row on a wasm: subscriber is a host-wiring invariant
            // violation — panic fail-fast with all context in the payload.
            let env = match iob {
                IngressOrBus::Bus(e) => e,
                IngressOrBus::Ingress(ev) => panic!(
                    "load_activation_snapshot: Ingress row on wasm: subscriber — \
                     host-wiring invariant violated; subscriber={} push_id={push_id} \
                     source={:?}",
                    subscriber.as_str(),
                    ev.source,
                ),
            };
            by_channel
                .entry(env.channel.clone())
                .or_default()
                .push((push_id, env));
        }

        // ── Config-residue reconciliation ───────────────────────────────────
        // Build a set of channel addresses that are current inputs so we can
        // detect rows for removed subscriptions (case a).
        let known_channels: std::collections::HashSet<&str> = inputs
            .iter()
            .map(|inp| inp.sub.channel_address.as_str())
            .collect();

        // Single `now` timestamp for all residue retirements in this snapshot, using
        // the project-standard `+00:00` form so lex-sort on delivered_at is consistent.
        let now = format_ts_for_db(Utc::now());

        // Retire rows for channels not in any current input (subscription removed).
        // This collects keys to avoid borrow issues while iterating.
        let residue_keys: Vec<String> = by_channel
            .keys()
            .filter(|ch| !known_channels.contains(ch.as_str()))
            .cloned()
            .collect();
        for ch in residue_keys {
            let rows = by_channel.remove(&ch).expect("key came from this map");
            let count = rows.len();
            let ids: Vec<i64> = rows.into_iter().map(|(id, _)| id).collect();
            tracing::warn!(
                subscriber = %subscriber.as_str(),
                channel = %ch,
                count,
                "load_activation_snapshot: retiring residue rows for removed subscription"
            );
            retire_push_rows(&conn, &now, &ids);
        }

        // ── First pass: partition raw rows per port; retire case-b residue ───
        // Determine triggering-ness and clamp raw rows before the context
        // queries. This avoids executing up-to-K×1000-row context reads on the
        // None path (no-op drain steps after a burst), which would hold the
        // global db mutex for no useful work (efficiency-1).
        struct PerPortRaw {
            raw_rows: Vec<(i64, MessageEnvelope)>,
        }
        let mut any_triggering = false;
        let mut per_port_raw: Vec<PerPortRaw> = Vec::with_capacity(inputs.len());

        for input in inputs {
            let sub = &input.sub;
            let channel_rows = by_channel.remove(&sub.channel_address);

            // Case b: config residue — push_depth was lowered to Bounded(0) but
            // pending rows from before the config change remain. Retire them.
            if let Some(rows) = &channel_rows
                && let Depth::Bounded(0) = sub.push_depth
            {
                let count = rows.len();
                let ids: Vec<i64> = rows.iter().map(|(id, _)| *id).collect();
                tracing::warn!(
                    subscriber = %subscriber.as_str(),
                    channel = %sub.channel_address,
                    count,
                    "load_activation_snapshot: retiring residue rows for \
                     push_depth=0 (sampled) port"
                );
                retire_push_rows(&conn, &now, &ids);
                // Port produces a pure-context window; rows were retired, not triggering.
                per_port_raw.push(PerPortRaw { raw_rows: vec![] });
                continue;
            }

            let raw_rows = channel_rows.unwrap_or_default();
            if !raw_rows.is_empty() {
                any_triggering = true;
            }
            per_port_raw.push(PerPortRaw { raw_rows });
        }

        // Early-return before any context queries when nothing is triggering.
        // Residue retirement above still ran so stale rows don't accumulate.
        if !any_triggering {
            return None;
        }

        // ── Second pass: context queries + drop counter reads (Some path only) ─
        // Drop counters are read here, while the db lock is still held, to keep
        // them in the same T₀ snapshot as the pending rows (correctness-1: a
        // publish that lands between snapshot-release and a post-lock counter read
        // could evict a row from this snapshot while reporting its drop as
        // "dropped" in this activation).
        let drop_counters_guard = self
            .drop_counters
            .lock()
            .expect("load_activation_snapshot: drop_counters poisoned");

        let snapshots: Vec<PortSnapshot> = inputs
            .iter()
            .zip(per_port_raw)
            .map(|(input, pr)| {
                let sub = &input.sub;
                let clamped = clamp_and_fetch_context(
                    &conn,
                    sub.channel_uuid,
                    pr.raw_rows,
                    sub.push_depth,
                    sub.retain_depth,
                );
                let key = (sub.channel_address.clone(), subscriber.as_str().to_string());
                let drop_counter_snapshot = drop_counters_guard.get(&key).copied().unwrap_or(0);
                PortSnapshot {
                    port: input.port.clone(),
                    channel_address: sub.channel_address.clone(),
                    new_rows: clamped.new_rows,
                    context: clamped.context,
                    drop_counter_snapshot,
                    clamped_leftover: clamped.clamped_leftover,
                }
            })
            .collect();

        drop(drop_counters_guard);

        Some(snapshots)
    }

    /// Mark a set of pending-push rows delivered. Idempotent.
    pub async fn mark_pushes_delivered(&self, push_ids: &[i64]) {
        if push_ids.is_empty() {
            return;
        }
        let conn = self.db.lock().await;
        db::mark_pending_pushes_delivered(&conn, push_ids);
    }

    /// Record a failed multi-port WASM activation and retire all push rows, in
    /// **one transaction** across all failing ports (design §2.5).
    ///
    /// Writes one `messaging_wasm_consume_failures` row per entry in `failures`
    /// (one per triggering port that contributed new rows), then marks all
    /// accumulated push_ids delivered. The `(subscriber, last_message_id)`
    /// idempotency key ensures a re-run after a crash is a no-op on duplicate rows.
    ///
    /// Each `failure.push_ids` must not be empty. Panics on any DB error.
    pub async fn record_wasm_activation_failure(&self, failures: &[WasmBatchFailure<'_>]) {
        assert!(
            !failures.is_empty(),
            "record_wasm_activation_failure: failures must not be empty"
        );
        for f in failures {
            assert!(
                !f.push_ids.is_empty(),
                "record_wasm_activation_failure: push_ids must not be empty for channel {}",
                f.channel
            );
        }
        let now = format_ts_for_db(Utc::now());
        let conn = self.db.lock().await;
        let tx = conn
            .unchecked_transaction()
            .unwrap_or_else(|e| panic!("record_wasm_activation_failure: begin tx: {e}"));

        for failure in failures {
            let batch_push_ids = failure
                .push_ids
                .iter()
                .map(|id| id.to_string())
                .collect::<Vec<_>>()
                .join(",");
            tx.execute(
                "INSERT OR IGNORE INTO messaging_wasm_consume_failures \
                 (channel, subscriber, first_message_id, last_message_id, batch_push_ids, \
                  outcome, diagnostic, failed_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                rusqlite::params![
                    failure.channel,
                    failure.subscriber.as_str(),
                    failure.first_message_id,
                    failure.last_message_id,
                    batch_push_ids,
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
            retire_push_rows(&tx, &now, failure.push_ids);
        }

        tx.commit()
            .unwrap_or_else(|e| panic!("record_wasm_activation_failure: commit tx: {e}"));
    }
}

#[cfg(test)]
impl Messenger {
    /// Test-only: returns `true` if no push windows have been touched since boot.
    /// Used to assert that unbounded subscribers never create window entries.
    pub(crate) fn push_windows_is_empty(&self) -> bool {
        self.push_windows
            .lock()
            .expect("push_windows poisoned")
            .is_empty()
    }
}

#[cfg(any(test, feature = "testutils"))]
impl Messenger {
    /// Test-only: directly increment the in-memory drop counter for `(channel, subscriber)`
    /// by `amount`. Used to simulate push-overflow without going through the full publish
    /// path. Gated on `#[cfg(any(test, feature = "testutils"))]` rather than `#[cfg(test)]`
    /// only so that `testutils::inject_drop` (used by downstream test crates that enable the
    /// testutils feature) can call it; in both cases the mutation surface stays test-only
    /// and `drop_counters` is not widened beyond `pub(crate)` for non-test writes (quality-3).
    pub fn inject_drop(&self, channel: &str, subscriber: &ParticipantId, amount: u64) {
        let key = (channel.to_string(), subscriber.as_str().to_string());
        let mut counters = self.drop_counters.lock().expect("drop_counters poisoned");
        *counters.entry(key).or_insert(0) += amount;
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

    /// `Messenger::new` installs an empty default ephemeral bus (zero channels):
    /// any ephemeral publish reports `UnknownChannel` rather than silently
    /// succeeding or panicking, until boot replaces the bus via
    /// `with_ephemeral_bus`. Pins the pre-wiring contract.
    #[test]
    fn new_installs_empty_default_ephemeral_bus() {
        use std::sync::Arc;

        let messenger = Messenger::new(
            crate::db::init_db_memory(),
            Arc::new(MessagingDirectory::with_entries(vec![])),
            Arc::from("test-source"),
            Arc::new(indexmap::IndexMap::new()),
            Arc::new(super::query::NoopWakeRouter) as Arc<dyn WakeRouter>,
            crate::messaging::config::MessagingGlobalConfig::default(),
        );

        let sender = ParticipantId::for_app("someapp", "test-source");
        let policy = crate::access::AppPolicy::default();
        let result = messenger.ephemeral_bus().publish(
            &sender,
            &policy,
            "ephemeral:anything",
            "hi",
            Urgency::Normal,
        );
        assert!(
            matches!(result, EphemeralPublishResult::UnknownChannel(_)),
            "default empty bus must reject every channel as UnknownChannel, got {result:?}"
        );
    }

    /// `with_ephemeral_bus` must run before the `Messenger` `Arc` is shared:
    /// once a second strong reference exists, `Arc::get_mut` fails and the
    /// builder panics (fail-fast) rather than silently no-op'ing the install.
    #[test]
    #[should_panic(expected = "boot-ordering bug")]
    fn with_ephemeral_bus_after_arc_shared_panics() {
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
        let bus = EphemeralBus::new(Vec::new(), Arc::from("test-source"), 1024);
        let _ = messenger.with_ephemeral_bus(bus);
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
    // load_activation_snapshot unit tests (§5 test 15, first bullet)
    // -----------------------------------------------------------------------

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
        let (pid_ctx_a, mid_ctx_a) = super::testutils::insert_wasm_push_at(
            &messenger,
            &channel,
            &wasm_sub,
            "ctx-a",
            ChannelScheme::Brenn,
            base_ns,
        )
        .await;
        let (pid_ctx_b, mid_ctx_b) = super::testutils::insert_wasm_push_at(
            &messenger,
            &channel,
            &wasm_sub,
            "ctx-b",
            ChannelScheme::Brenn,
            base_ns + 1_000_000,
        )
        .await;
        let (_pid0, mid0) = super::testutils::insert_wasm_push(
            &messenger,
            &channel,
            &wasm_sub,
            "new-0",
            ChannelScheme::Brenn,
        )
        .await;
        let (_pid1, mid1) = super::testutils::insert_wasm_push(
            &messenger,
            &channel,
            &wasm_sub,
            "new-1",
            ChannelScheme::Brenn,
        )
        .await;

        // Mark ctx-a and ctx-b delivered — they become retained context only.
        messenger
            .mark_pushes_delivered(&[pid_ctx_a, pid_ctx_b])
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

        // new_rows must be the 2 pending rows (Unbounded → no truncation).
        assert_eq!(
            snap.new_rows.len(),
            2,
            "expected 2 pending new rows, got {}",
            snap.new_rows.len()
        );
        let new_ids_in_snap: Vec<Uuid> = snap.new_rows.iter().map(|(_, e)| e.message_id).collect();
        assert!(new_ids_in_snap.contains(&mid0), "expected mid0 in new_rows");
        assert!(new_ids_in_snap.contains(&mid1), "expected mid1 in new_rows");

        // context must contain ctx-a and ctx-b but NOT mid0 or mid1.
        let context_ids: Vec<Uuid> = snap.context.iter().map(|e| e.message_id).collect();
        assert!(
            context_ids.contains(&mid_ctx_a),
            "delivered ctx-a must appear in context: {context_ids:?}"
        );
        assert!(
            context_ids.contains(&mid_ctx_b),
            "delivered ctx-b must appear in context: {context_ids:?}"
        );
        assert!(
            !context_ids.contains(&mid0),
            "pending msg mid0 must be stripped from context: {context_ids:?}"
        );
        assert!(
            !context_ids.contains(&mid1),
            "pending msg mid1 must be stripped from context: {context_ids:?}"
        );

        // context must be in ASC order (oldest first). ctx-a has a smaller
        // publish_ts_ns than ctx-b, so it must appear first.
        // A bug that skipped the DESC→ASC reversal would place ctx-b (newer) at
        // index 0, failing this assertion.
        assert_eq!(
            snap.context.len(),
            2,
            "context should have exactly the 2 delivered rows"
        );
        assert_eq!(
            snap.context[0].message_id, mid_ctx_a,
            "context[0] must be the older ctx-a (ASC order)"
        );
        assert_eq!(
            snap.context[1].message_id, mid_ctx_b,
            "context[1] must be the newer ctx-b (ASC order)"
        );

        // Backlog (2 rows) is below the Unbounded cap → not clamped.
        assert_eq!(
            snap.clamped_leftover, 0,
            "unclamped port must report clamped_leftover == 0"
        );
    }

    /// `clamped_leftover` is exact: a `Bounded(1)` port with 3 pending rows yields
    /// `new_rows.len() == 1` and `clamped_leftover == 2`. Acking the delivered row
    /// and reloading drops the leftover to 1 (the leftover rows are ordinary pending
    /// rows that stay undelivered until a later drain).
    #[tokio::test]
    async fn load_activation_snapshot_clamped_leftover_exact() {
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
            super::testutils::insert_wasm_push_at(
                &messenger,
                &channel,
                &wasm_sub,
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
        assert_eq!(snap.new_rows.len(), 1, "push_depth=1 clamps to 1 new row");
        assert_eq!(
            snap.clamped_leftover, 2,
            "3 pending - 1 delivered = 2 leftover"
        );

        // Ack the delivered row, reload: leftover shrinks by exactly one.
        let delivered_id = snap.new_rows[0].0;
        messenger.mark_pushes_delivered(&[delivered_id]).await;

        let snapshots2 = messenger
            .load_activation_snapshot(&wasm_sub, &inputs)
            .await
            .expect("expected Some — 2 rows still pending");
        assert_eq!(
            snapshots2[0].clamped_leftover, 1,
            "after acking one, 2 pending - 1 delivered = 1 leftover"
        );
    }

    // -----------------------------------------------------------------------
    // record_wasm_activation_failure unit tests (§5 test 15, third bullet)
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
        let (pid0, mid0) = super::testutils::insert_wasm_push(
            &messenger,
            &channel,
            &wasm_sub,
            "body-a",
            ChannelScheme::Brenn,
        )
        .await;
        let (pid1, mid1) = super::testutils::insert_wasm_push(
            &messenger,
            &channel,
            &wasm_sub,
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
            push_ids: &[pid0, pid1],
            outcome: "trap",
            diagnostic: "unreachable instruction at test",
        };

        // Single-entry call: must write the quarantine row and retire push rows.
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

        let batch_push_ids_col: String = conn
            .query_row(
                "SELECT batch_push_ids FROM messaging_wasm_consume_failures \
                 WHERE subscriber = ?1 AND last_message_id = ?2",
                rusqlite::params![wasm_sub.as_str(), &last_msg_id],
                |r| r.get(0),
            )
            .expect("query batch_push_ids");
        assert!(
            batch_push_ids_col.contains(&pid0.to_string()),
            "batch_push_ids must contain pid0: {batch_push_ids_col}"
        );
        assert!(
            batch_push_ids_col.contains(&pid1.to_string()),
            "batch_push_ids must contain pid1: {batch_push_ids_col}"
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

        let pending: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM messaging_pending_pushes \
                 WHERE id IN (?1, ?2) AND delivered_at IS NULL",
                rusqlite::params![pid0, pid1],
                |r| r.get(0),
            )
            .expect("query pending count");
        assert_eq!(
            pending, 0,
            "both push rows must be delivered after quarantine"
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
        let (pid_a, mid_a) = super::testutils::insert_wasm_push(
            &messenger2,
            &ch_a,
            &wasm_sub2,
            "port-a-msg",
            ChannelScheme::Brenn,
        )
        .await;
        let (pid_b, mid_b) = super::testutils::insert_wasm_push(
            &messenger2,
            &ch_b,
            &wasm_sub2,
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
            push_ids: &[pid_a],
            outcome: "err",
            diagnostic: "multi-entry-ch-a",
        };
        let fail_b = WasmBatchFailure {
            channel: &ch_b.address,
            subscriber: &wasm_sub2,
            first_message_id: &ch_b_mid,
            last_message_id: &ch_b_mid,
            push_ids: &[pid_b],
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
