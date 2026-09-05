//! The channel directory and the subscriber vocabulary it holds.
//!
//! A [`ChannelEntry`] is one registered channel with its resolved config and
//! its subscribers; [`MessagingDirectory`] is the address/uuid index over
//! those entries, shared behind an `RwLock` and snapshot-cloned on read.
//! [`WakeMin`] is the per-subscription wake threshold each subscriber
//! registration carries.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use uuid::Uuid;

use super::config;
use super::identity::{AttachKind, AttachScope};
use super::{ChannelScheme, Urgency};

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
    /// retained. `None` when `standing_retain_depth` is `Unbounded` — the
    /// operator asked for the channel to be pinned in so many words.
    ///
    /// `standing_retain_depth` is the ceiling on every depth stated about the
    /// channel, so it is the frontier outright: no subscriber can be owed a row
    /// the standing buffer does not already hold. That makes the operator's one
    /// number the disk truth, readable off the `[[channel]]` block instead of
    /// emergent from the union of every subscriber's config.
    ///
    /// # Panics
    ///
    /// If any subscriber's `push_depth` or `retain_depth` exceeds standing.
    /// Config-time validation and the dynamic-subscribe gate both refuse such a
    /// subscriber, so reaching here means one of them was bypassed and the
    /// frontier would be a lie — better dead than reaping live rows.
    pub fn reap_frontier(&self) -> Option<u64> {
        use config::Depth;

        let standing = self.resolved_channel.standing_retain_depth;
        for sub in &self.subscribers {
            for depth in [sub.push_depth, sub.retain_depth] {
                assert!(
                    depth <= standing,
                    "channel {:?} subscriber {:?} holds depth {depth:?} above the channel's \
                     standing_retain_depth {standing:?} — the depth ceiling was bypassed",
                    self.address,
                    sub.kind,
                );
            }
        }

        match standing {
            Depth::Unbounded => None,
            Depth::Bounded(n) => Some(n),
        }
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
    /// A browser-surface subscriber; the slug is the `[[surface]]` slug and
    /// becomes `surface:<slug>` as the `ParticipantId`.
    ///
    /// One entry per (surface, channel): an attachment holds at most one
    /// subscription per channel, and whatever sits behind it on the attacher's
    /// side — one component's binding or six — is the attacher's own
    /// bookkeeping. `finalize_directory_with_subscribers` folds the declared
    /// bindings on a channel into that single entry, so a channel two components
    /// bind is one server-side push window rather than two feeding the same
    /// socket.
    ///
    /// Authority is per-surface and always was: a component's grants are its
    /// config-declared bindings, which boot already proved the surface's own
    /// ACLs cover, so nothing that resolves policy from this key loses anything
    /// by not naming a component. Per-component attribution and budgets are cut
    /// at the sub-identity, not at this key.
    Surface(String),
    /// An authenticated remote attacher; the slug is the `[[remote]]` slug and
    /// becomes `remote:<slug>` as the `ParticipantId`.
    ///
    /// Attach-shaped like [`SubscriberEntryKind::Surface`] — one entry per
    /// (remote, channel), no server-side position — and a distinct kind for the
    /// two reasons that matter: its authority lowers from a `[[remote]]` block
    /// rather than a `[[surface]]` one, and its entries are minted at runtime
    /// (the first successful `Subscribe` on a granted prefix) rather than folded
    /// from boot-declared bindings.
    Remote(String),
    /// An in-process system-substrate subscriber; the component name becomes
    /// `system:<component>` as the `ParticipantId` and resolves its policy via
    /// `Messenger::system_policies`. Created programmatically (not from config),
    /// parked-and-woken like a `Wasm` subscriber.
    System(String),
    /// One conversation reading its own chat command channel, as
    /// `conversation:<id>`.
    ///
    /// The only kind minted at runtime rather than declared: a conversation's
    /// chat channels carry its id in their names, so the subscription cannot
    /// exist before the conversation does. Chat provisioning is its one
    /// constructor.
    ///
    /// It is deliberately *not* an [`SubscriberEntryKind::App`] entry, and that
    /// is the whole reason it exists. An `App` subscription is walked by
    /// `attach_conversation_subscribers`, which would attach every one of the
    /// app's conversations to every other one's command channel, and is drained
    /// by the ambience path, which renders what it finds into a system message.
    /// A command is neither. Being a kind of its own keeps chat out of both
    /// walks by construction rather than by a filter someone has to remember.
    ///
    /// `app_slug` is carried, not looked up: authority for the chat tree is the
    /// owning app's derived harness policy (`AppConfig::chat_harness_policy`,
    /// the `<prefix>.app.<slug>.` matchers), not the app's authored policy, so
    /// policy and wake economics resolve through the same apps map an `App`
    /// entry uses, with no per-conversation registration anywhere.
    ChatConversation {
        app_slug: String,
        conversation_id: i64,
    },
}

impl SubscriberEntryKind {
    /// Returns the config slug regardless of kind. For logging and for the
    /// apps-map lookups every kind's authority resolves through; it is not a
    /// storage key, and the pending-push keyspace is not built from it.
    pub fn slug(&self) -> &str {
        match self {
            SubscriberEntryKind::App(s)
            | SubscriberEntryKind::Wasm(s)
            | SubscriberEntryKind::System(s)
            | SubscriberEntryKind::Surface(s)
            | SubscriberEntryKind::Remote(s) => s.as_str(),
            SubscriberEntryKind::ChatConversation { app_slug, .. } => app_slug.as_str(),
        }
    }

    /// The registration key one attacher's authority resolves through.
    ///
    /// The inverse of [`attach_slug`](Self::attach_slug), and the reason the two
    /// live together: a publish path holds an [`AttachScope`] and needs the key,
    /// a delivery path holds the key and needs the slug, and neither may spell
    /// the mapping itself.
    pub fn for_attach(scope: AttachScope<'_>) -> Self {
        match scope.kind {
            AttachKind::Surface => SubscriberEntryKind::Surface(scope.slug.to_string()),
            AttachKind::Remote => SubscriberEntryKind::Remote(scope.slug.to_string()),
        }
    }

    /// The attacher slug this kind names, or `None` for a subscriber that holds
    /// a server-side position instead of an attachment.
    ///
    /// The single definition of "attach-shaped": every caller that needs to
    /// distinguish the two families asks here, so a third attach-shaped kind
    /// joins in one place.
    pub fn attach_slug(&self) -> Option<&str> {
        match self {
            SubscriberEntryKind::Surface(slug) | SubscriberEntryKind::Remote(slug) => {
                Some(slug.as_str())
            }
            SubscriberEntryKind::App(_)
            | SubscriberEntryKind::Wasm(_)
            | SubscriberEntryKind::System(_)
            | SubscriberEntryKind::ChatConversation { .. } => None,
        }
    }

    /// Whether two kinds name the same subscriber on one channel: same variant,
    /// same [`slug`](Self::slug), and — for a conversation — the same
    /// conversation.
    ///
    /// The single definition of subscriber identity: it is what
    /// [`MessagingDirectory::add_subscriber`] replaces by and what
    /// [`MessagingDirectory::remove_subscriber`] removes by, so any caller
    /// predicting either asks here rather than restating the rule.
    /// Deliberately not the derived `PartialEq`, which also compares depths and
    /// noise — a re-registration at new depths is the same subscriber.
    ///
    /// A `ChatConversation` carries its `conversation_id` into the comparison:
    /// the app slug alone would make every conversation of one app a single
    /// subscriber, so a caller naming one conversation would remove its
    /// siblings. Two conversations sit on separate channels today, which is why
    /// the coarse reading never showed; identity is stated at the grain the key
    /// is written at rather than at the grain today's callers happen to use.
    pub fn same_principal(&self, other: &SubscriberEntryKind) -> bool {
        match (self, other) {
            (
                SubscriberEntryKind::ChatConversation {
                    app_slug,
                    conversation_id,
                },
                SubscriberEntryKind::ChatConversation {
                    app_slug: other_app,
                    conversation_id: other_conversation,
                },
            ) => app_slug == other_app && conversation_id == other_conversation,
            _ => {
                std::mem::discriminant(self) == std::mem::discriminant(other)
                    && self.slug() == other.slug()
            }
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
/// The policy is behind an `Arc` so a registration can be read out of the
/// registry without holding its lock, and so the boot-only installer can assert
/// the `Messenger` is still uniquely owned before wiring it in.
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
/// and mutated at runtime by dynamic subscriptions. Held on
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
        Self::from_arcs(entries.into_iter().map(Arc::new).collect())
    }

    /// Build a directory over entries that are already shared.
    ///
    /// Mutation is copy-on-write, so an `Arc<ChannelEntry>` a caller holds is
    /// already an immutable snapshot: a directory built from the `Arc`s another
    /// directory's `list()` handed out is a detached copy of it, and no entry is
    /// cloned to make one.
    pub fn from_arcs(entries: Vec<Arc<ChannelEntry>>) -> Self {
        let mut by_uuid = HashMap::with_capacity(entries.len());
        let mut by_address = HashMap::with_capacity(entries.len());
        let mut order = Vec::with_capacity(entries.len());
        for entry in entries {
            order.push(entry.uuid);
            by_address.insert(entry.address.clone(), entry.uuid);
            by_uuid.insert(entry.uuid, entry);
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
    /// the tool layer governs *when* a replace is permitted.
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
        if let Some(slot) = entry
            .subscribers
            .iter_mut()
            .find(|s| s.kind.same_principal(&subscriber.kind))
        {
            *slot = subscriber;
        } else {
            entry.subscribers.push(subscriber);
        }
        inner.by_uuid.insert(*channel_uuid, Arc::new(entry));
        true
    }

    /// Remove one subscriber from a channel, copy-on-write.
    ///
    /// Clones the target `ChannelEntry`, retains-out the subscriber sharing
    /// `kind`'s principal (leaving every other subscriber untouched), and swaps
    /// the `Arc` under the write-lock. The mirror of [`Self::add_subscriber`],
    /// which replaces at the same grain: an app's dynamic unsubscribe passes
    /// `App(slug)`, a departing consumer passes `Wasm(slug)`, and a
    /// `ChatConversation` names one conversation rather than every conversation
    /// of its app ([`SubscriberEntryKind::same_principal`]).
    ///
    /// Returns `Some(remaining)` — the count of subscribers left on the channel
    /// after the removal — if the channel existed and a matching subscriber was
    /// removed; `None` if the channel is unknown or no matching subscriber was
    /// present. The remaining count is computed inside the single write-lock
    /// critical section so the unsubscribe path's "last subscriber on this
    /// filter?" decision needs no second `resolve` + entry clone.
    pub fn remove_subscriber(
        &self,
        channel_uuid: &Uuid,
        kind: &SubscriberEntryKind,
    ) -> Option<usize> {
        let mut inner = self.inner.write().expect("directory lock poisoned");
        let existing = inner.by_uuid.get(channel_uuid)?;
        let mut entry = ChannelEntry::clone(existing);
        let before = entry.subscribers.len();
        entry.subscribers.retain(|s| !s.kind.same_principal(kind));
        if entry.subscribers.len() == before {
            return None;
        }
        let remaining = entry.subscribers.len();
        inner.by_uuid.insert(*channel_uuid, Arc::new(entry));
        Some(remaining)
    }

    /// Set a channel's description in place, copy-on-write.
    ///
    /// The description is metadata: it feeds the operator listings and the
    /// durable row, and nothing that routes, sizes or authorizes. So it is the
    /// one part of an entry that changes without the entry being re-created —
    /// the channel's subscribers and everything reading them are untouched. The
    /// caller owns the durable row: a persisted channel's column is written by
    /// the row upsert, not from here.
    ///
    /// Returns `true` if the channel existed and now carries `description`;
    /// `false` if `channel_uuid` is unknown.
    pub fn set_description(&self, channel_uuid: &Uuid, description: Option<String>) -> bool {
        let mut inner = self.inner.write().expect("directory lock poisoned");
        let Some(existing) = inner.by_uuid.get(channel_uuid) else {
            return false;
        };
        let mut entry = ChannelEntry::clone(existing);
        entry.description = description;
        inner.by_uuid.insert(*channel_uuid, Arc::new(entry));
        true
    }

    /// Insert a brand-new channel (entry + address index + iteration order).
    ///
    /// Used by the runtime `mqtt:` subscribe path to register a
    /// channel for a filter that was never declared in the config. Panics if a channel
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

    /// Remove a channel entirely (entry + address index + iteration order).
    ///
    /// Returns `true` if the channel was present. Used by teardown paths whose
    /// channel family is scoped to something that can end — a conversation's
    /// chat channels die with the conversation — so the directory does not
    /// accumulate names nothing can ever reach again.
    ///
    /// The caller owns whatever the channel's messages live in: a durable
    /// channel's rows and a non-durable channel's ring both outlive this call
    /// unless the caller removes them too.
    pub fn remove_channel(&self, channel_uuid: &Uuid) -> bool {
        let mut inner = self.inner.write().expect("directory lock poisoned");
        let Some(entry) = inner.by_uuid.remove(channel_uuid) else {
            return false;
        };
        inner.by_address.remove(&entry.address);
        inner.order.retain(|uuid| uuid != channel_uuid);
        true
    }
}

/// Per-subscription wake policy set by the subscriber.
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

    /// Wire/DB string representation.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::VeryLow => "very-low",
            Self::Low => "low",
            Self::Normal => "normal",
            Self::High => "high",
            Self::Never => "never",
        }
    }

    /// Parse from a wire/DB string. Returns `None` on unknown values.
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

// ---------------------------------------------------------------------------
// Durable dynamic subscriptions
// ---------------------------------------------------------------------------

/// One row of `messaging_dynamic_subscriptions`, decoded into typed values:
/// the resolved subscription parameters (`channel_uuid`/`app_slug`/`push_depth`/
/// `retain_depth`/`noise`/`wake_min`) plus the MQTT-only `qos` (`None` for
/// `brenn:`/`webhook:`) and the `created_at` timestamp.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DynamicSubscriptionRow {
    pub channel_uuid: Uuid,
    pub app_slug: String,
    pub push_depth: config::Depth,
    pub retain_depth: config::Depth,
    pub noise: config::NoiseLevel,
    pub wake_min: WakeMin,
    /// MQTT SUBSCRIBE QoS (0/1/2). `None` for non-MQTT transports.
    pub qos: Option<u8>,
    /// RFC3339 creation timestamp (DB wire form), as stored.
    pub created_at: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A `brenn:` entry with everything unbounded and no subscribers.
    fn entry(name: &str) -> ChannelEntry {
        crate::messaging::test_support::test_channel_entry(name, vec![])
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

    fn conversation_subscriber(app_slug: &str, conversation_id: i64) -> SubscriberEntry {
        SubscriberEntry {
            kind: SubscriberEntryKind::ChatConversation {
                app_slug: app_slug.to_string(),
                conversation_id,
            },
            push_depth: config::Depth::Bounded(1),
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
        assert!(dir.resolve("smtp:pa-alice").is_none());
    }

    /// `list()` preserves config-declaration order, not HashMap iteration
    /// order. Non-alphabetic insert order so the test catches a sorted
    /// regression.
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

    /// `SubscriberEntryKind::slug()` returns the slug for the `Surface` variant.
    #[test]
    fn subscriber_entry_kind_surface_slug() {
        let kind = SubscriberEntryKind::Surface("deskbar".to_string());
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
        assert_eq!(
            dir.remove_subscriber(&uuid, &SubscriberEntryKind::App("app-a".to_string())),
            Some(2)
        );

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
        assert_eq!(
            dir.remove_subscriber(
                &Uuid::new_v4(),
                &SubscriberEntryKind::App("app-a".to_string())
            ),
            None
        );
        // Known channel, but no App(app-a) subscriber (only WASM present).
        assert_eq!(
            dir.remove_subscriber(&uuid, &SubscriberEntryKind::App("app-a".to_string())),
            None
        );
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

        assert_eq!(
            dir.remove_subscriber(&uuid, &SubscriberEntryKind::App("only-app".to_string())),
            Some(0)
        );
    }

    /// `remove_subscriber` reaches a `Wasm(slug)` subscriber too — the grain a
    /// departing consumer leaves by — and leaves an app subscribed to the same
    /// channel where it was.
    #[test]
    fn directory_remove_subscriber_reaches_a_wasm_entry() {
        let mut e = entry("dyn-remove-wasm");
        e.subscribers = vec![app_subscriber("watcher"), wasm_subscriber("retiree")];
        let uuid = e.uuid;
        let addr = e.address.clone();
        let dir = MessagingDirectory::with_entries(vec![e]);

        assert_eq!(
            dir.remove_subscriber(&uuid, &SubscriberEntryKind::Wasm("retiree".to_string())),
            Some(1)
        );

        let after = dir.resolve(&addr).expect("channel exists");
        assert_eq!(after.subscribers.len(), 1);
        assert_eq!(
            after.subscribers[0].kind,
            SubscriberEntryKind::App("watcher".to_string())
        );
        // The same slug under the other kind was never there to remove.
        assert_eq!(
            dir.remove_subscriber(&uuid, &SubscriberEntryKind::Wasm("watcher".to_string())),
            None
        );
    }

    /// A conversation subscriber is named at the grain its key is written at:
    /// removing one conversation of an app leaves that app's other
    /// conversations subscribed. The coarse reading — app slug only — would
    /// take both.
    #[test]
    fn directory_remove_subscriber_names_one_conversation() {
        let mut e = entry("dyn-remove-conversation");
        e.subscribers = vec![
            conversation_subscriber("desk", 1),
            conversation_subscriber("desk", 2),
        ];
        let uuid = e.uuid;
        let addr = e.address.clone();
        let dir = MessagingDirectory::with_entries(vec![e]);

        assert_eq!(
            dir.remove_subscriber(
                &uuid,
                &SubscriberEntryKind::ChatConversation {
                    app_slug: "desk".to_string(),
                    conversation_id: 1,
                }
            ),
            Some(1)
        );

        let after = dir.resolve(&addr).expect("channel exists");
        assert_eq!(
            after
                .subscribers
                .iter()
                .map(|s| s.kind.clone())
                .collect::<Vec<_>>(),
            vec![SubscriberEntryKind::ChatConversation {
                app_slug: "desk".to_string(),
                conversation_id: 2,
            }],
        );
    }

    /// The same grain on the way in: a second conversation of one app is a
    /// second subscriber, not a replacement of the first.
    #[test]
    fn directory_add_subscriber_keeps_conversations_apart() {
        let mut e = entry("dyn-add-conversation");
        e.subscribers = vec![conversation_subscriber("desk", 1)];
        let uuid = e.uuid;
        let addr = e.address.clone();
        let dir = MessagingDirectory::with_entries(vec![e]);

        assert!(dir.add_subscriber(&uuid, conversation_subscriber("desk", 2)));

        let after = dir.resolve(&addr).expect("channel exists");
        assert_eq!(after.subscribers.len(), 2);

        // The same conversation again replaces, as any re-registration does.
        assert!(dir.add_subscriber(&uuid, conversation_subscriber("desk", 2)));
        assert_eq!(
            dir.resolve(&addr)
                .expect("channel exists")
                .subscribers
                .len(),
            2
        );
    }

    /// `set_description` swaps the text in place and touches nothing else on the
    /// entry — same uuid, same resolved config, same subscribers.
    #[test]
    fn directory_set_description_leaves_the_rest_of_the_entry() {
        let mut e = entry("described");
        e.description = Some("before".to_string());
        e.subscribers = vec![app_subscriber("watcher"), wasm_subscriber("worker")];
        let uuid = e.uuid;
        let addr = e.address.clone();
        let dir = MessagingDirectory::with_entries(vec![e]);
        let before = dir.resolve(&addr).expect("channel exists");

        assert!(dir.set_description(&uuid, Some("after".to_string())));

        let after = dir.resolve(&addr).expect("channel exists");
        assert_eq!(after.description.as_deref(), Some("after"));
        assert_eq!(after.uuid, before.uuid);
        assert_eq!(after.address, before.address);
        assert_eq!(after.transport_type, before.transport_type);
        assert_eq!(after.mount, before.mount);
        let slugs: Vec<&str> = after.subscribers.iter().map(|s| s.kind.slug()).collect();
        assert_eq!(slugs, vec!["watcher", "worker"]);

        assert!(dir.set_description(&uuid, None));
        assert!(
            dir.resolve(&addr)
                .expect("channel exists")
                .description
                .is_none()
        );
    }

    /// `set_description` on an unknown channel returns `false` and mutates
    /// nothing.
    #[test]
    fn directory_set_description_unknown_channel() {
        let dir = MessagingDirectory::with_entries(vec![]);
        assert!(!dir.set_description(&Uuid::new_v4(), Some("x".to_string())));
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

    /// `add_channel` panics on a UUID/address collision — a host bug, not
    /// operator input.
    #[test]
    #[should_panic(expected = "already present")]
    fn directory_add_channel_duplicate_panics() {
        let e = entry("dup");
        let dup = e.clone();
        let dir = MessagingDirectory::with_entries(vec![e]);
        dir.add_channel(dup);
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
}
