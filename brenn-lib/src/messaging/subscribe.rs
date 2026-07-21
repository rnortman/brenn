//! Runtime dynamic-subscribe core (design §2.3).
//!
//! This is the transport-agnostic body of "create a dynamic subscription": it
//! resolves the subscription's parameters (shared resolver, [`config::resolve_subscription_params`]),
//! persists the durable row + its `messaging_subscriptions` mirror in one
//! transaction ([`db::insert_dynamic_subscription`]), and folds the new
//! subscriber into the in-memory directory (copy-on-write
//! [`MessagingDirectory::add_subscriber`]).
//!
//! Scope of *this* layer: for `brenn:`/`webhook:` the channel must already
//! exist in the directory (design §2.3: never auto-create a channel nobody
//! publishes to). For `mqtt:`, a not-yet-existing topic-filter channel is
//! **created** here — validate the filter, derive the canonical address + UUID,
//! upsert `messaging_channels`, and `add_channel` into the directory (design
//! §2.3 step 3, the "create the channel entry" half). The transport-specific
//! *broker activation* (the live MQTT SUBSCRIBE, the configured-client check,
//! and the router `IngressRoute` add) is **not** done here — it is layered on
//! top by the per-transport activation increment (design §2.3 steps 1/5/6) and
//! the `MessageSubscribe` tool (design §2.4). The `qos` parameter is validated
//! and persisted here (so the later activation step has it), but no broker call
//! is made.
//!
//! Every failure path returns an error (never panics): a misconfigured dynamic
//! subscribe is LLM/attacker-shaped tool input, not a host bug (CLAUDE.md
//! "panic on host bug, error on bad input"). The boot path keeps its
//! fail-fast `.expect()` on the shared resolver; only this runtime path maps the
//! resolver's `Err` to a tool-facing error.

use super::config::{
    self, RawSubscriptionParams, ResolvedChannel, ResolvedSubscription, SubscribeError,
    SubscriptionParamDefaults,
};
use super::db::{
    DynamicSubscriptionRow, delete_dynamic_subscription, insert_dynamic_subscription,
    upsert_channels,
};
use super::{
    ChannelEntry, ChannelScheme, Depth, Messenger, NoiseLevel, SubscriberEntry,
    SubscriberEntryKind, WakeMin, mqtt_channel_uuid_from_address,
};
use crate::db::format_ts_for_db;
use crate::mqtt::address::{parse_mqtt_address, validate_topic_filter_str};

/// Raw (pre-resolution) parameters for a runtime dynamic subscribe, as supplied
/// by the `MessageSubscribe` tool (design §2.4).
///
/// `push_depth` and `retain_depth` are **required** at the tool surface (the LLM
/// makes the pull-vs-push and history-retention decisions explicitly on every
/// call, design §7 A); they are passed verbatim with no inheritance. `noise` and
/// `wake_min` remain `Option` and inherit from the channel/global rung when
/// omitted. `qos` is MQTT-only.
#[derive(Debug, Clone)]
pub struct DynamicSubscribeParams {
    /// Required: 0 = pull-only (the `push_depth=0` ad-hoc-read trick), >0 = push.
    pub push_depth: Depth,
    /// Required: how many historical messages stay queryable for this subscriber.
    pub retain_depth: Depth,
    pub noise: Option<NoiseLevel>,
    pub wake_min: Option<WakeMin>,
    /// MQTT SUBSCRIBE QoS (0/1/2). Required-shape only for `mqtt:` addresses;
    /// supplying it for `brenn:`/`webhook:` is an error (don't silently ignore a
    /// caller mistake, design §2.3).
    pub qos: Option<u8>,
}

/// Error from the runtime dynamic-subscribe core ([`Messenger::subscribe_dynamic`]).
///
/// All variants are returned, never panicked — a bad dynamic-subscribe is
/// tool/LLM input, not a host bug (design §4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeSubscribeError {
    /// No channel with this address exists in the directory and the transport
    /// does not auto-create one. For `brenn:`/`webhook:` this is terminal:
    /// nothing publishes there; a subscription to a channel nobody can publish to
    /// is meaningless, so it is never auto-created (design §2.3). (For `mqtt:` an
    /// absent topic-filter channel is created rather than erroring.)
    UnknownChannel { address: String },
    /// `qos` supplied for a non-MQTT (`brenn:`/`webhook:`) address (design §2.3).
    QosOnNonMqtt { address: String },
    /// An `mqtt:` address with an invalid topic filter (wildcard placement, empty
    /// topic, etc.). Surfaced here because channel creation validates the filter
    /// before deriving the channel (design §2.3 step 2). Carries the parser's
    /// detail string.
    InvalidMqttFilter { address: String, detail: String },
    /// The calling app already holds a dynamic subscription on this channel and
    /// the newly-supplied params resolve to **different** values. Re-subscribe
    /// param-mutation is withheld (design §2.4): the caller must
    /// `MessageUnsubscribe` first, then re-subscribe. (The identical-params case
    /// is the idempotent no-op `SubscribeOutcome::AlreadySubscribedIdentical`,
    /// not an error.)
    AlreadySubscribedDiffers { address: String },
    /// The calling app already has a **static** (TOML-configured) subscription on
    /// this channel. Static subs are config-managed and cannot be shadowed or
    /// mutated by a dynamic subscribe (design §2.1: an app cannot hold both a
    /// static and a dynamic sub on one channel). The app already receives this
    /// channel; no dynamic subscription is created.
    StaticSubscriptionExists { address: String },
    /// The resolved `retain_depth` of a dynamic subscribe exceeds the channel's
    /// `standing_retain_depth`. A dynamic subscriber's `retain_depth` is its read
    /// clamp for `MessageChannelGet` history, so allowing it past the operator's
    /// standing window would let a semi-trusted runtime principal read deeper
    /// history than the operator's baseline. Rejected (not clamped — silent
    /// narrowing is banned; the caller must know the depth it actually got). Only
    /// strictly-greater is rejected; equality is allowed, and an `Unbounded`
    /// standing caps nothing.
    RetainDepthExceedsStanding {
        address: String,
        requested: Depth,
        standing: Depth,
    },
    /// A dormant durable dynamic row exists for this `(channel, app)`: a row that
    /// boot-merge classified `revoked` (ACL no longer authorizes delivery, or its
    /// retain_depth exceeds the channel's current standing depth), so it is
    /// durable-only — not folded into the directory and invisible to
    /// `MessageSubscriptionList`. A fresh subscribe cannot INSERT over it (the
    /// `(channel_uuid, app_slug)` PK collides); the app must `MessageUnsubscribe`
    /// first, then re-subscribe.
    DormantSubscriptionExists { address: String },
    /// Parameter resolution / push-enabled invariant violation (delegated to the
    /// shared resolver). Carries the resolver's typed error for a faithful message.
    Params(SubscribeError),
}

impl std::fmt::Display for RuntimeSubscribeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RuntimeSubscribeError::UnknownChannel { address } => write!(
                f,
                "unknown channel {address:?} — nothing publishes there; a dynamic \
                 subscription requires an existing channel"
            ),
            RuntimeSubscribeError::QosOnNonMqtt { address } => write!(
                f,
                "qos is only valid for mqtt: addresses; channel {address:?} is not MQTT — \
                 omit qos"
            ),
            RuntimeSubscribeError::InvalidMqttFilter { address, detail } => {
                write!(f, "invalid mqtt topic filter in {address:?}: {detail}")
            }
            RuntimeSubscribeError::AlreadySubscribedDiffers { address } => write!(
                f,
                "already subscribed to {address:?} with different parameters; \
                 MessageUnsubscribe first, then re-subscribe to change parameters"
            ),
            RuntimeSubscribeError::StaticSubscriptionExists { address } => write!(
                f,
                "{address:?} already has a static (config-managed) subscription for this app; \
                 it cannot be changed at runtime and you already receive this channel"
            ),
            RuntimeSubscribeError::RetainDepthExceedsStanding {
                address,
                requested,
                standing,
            } => write!(
                f,
                "requested retain_depth {requested:?} for {address:?} exceeds the channel's \
                 standing retain depth {standing:?}; re-request with retain_depth <= {standing:?}"
            ),
            RuntimeSubscribeError::DormantSubscriptionExists { address } => write!(
                f,
                "a dormant subscription exists for {address:?} (not active under the current \
                 config); MessageUnsubscribe first, then re-subscribe"
            ),
            RuntimeSubscribeError::Params(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for RuntimeSubscribeError {}

impl From<SubscribeError> for RuntimeSubscribeError {
    fn from(e: SubscribeError) -> Self {
        RuntimeSubscribeError::Params(e)
    }
}

/// Successful outcome of [`Messenger::subscribe_dynamic`].
///
/// Distinguishes a freshly-created dynamic subscription from the idempotent
/// re-subscribe no-op (design §2.4: a re-subscribe with identical resolved
/// params is a success that did nothing, so the caller's transport-activation
/// step — broker SUBSCRIBE etc. — must be skipped, since the subscription is
/// already live).
#[derive(Debug, Clone)]
pub enum SubscribeOutcome {
    /// A new dynamic subscription was created (durable row + mirror written,
    /// channel created if absent, subscriber folded into the directory).
    Created(ResolvedSubscription),
    /// The calling app already held a dynamic subscription on this channel whose
    /// resolved params are **identical** to the request — an idempotent no-op
    /// (design §2.4). Nothing was written or mutated; the caller must NOT
    /// re-activate the transport (it is already active). Carries the existing
    /// resolved params for the caller's status reporting.
    AlreadySubscribedIdentical(ResolvedSubscription),
}

impl SubscribeOutcome {
    /// The resolved subscription params, regardless of whether this was a fresh
    /// `Created` or an idempotent `AlreadySubscribedIdentical`.
    pub fn resolved(&self) -> &ResolvedSubscription {
        match self {
            SubscribeOutcome::Created(r) | SubscribeOutcome::AlreadySubscribedIdentical(r) => r,
        }
    }

    /// True iff a new subscription was created (the caller must activate the
    /// transport). False for the idempotent no-op (already active).
    pub fn is_created(&self) -> bool {
        matches!(self, SubscribeOutcome::Created(_))
    }
}

/// Error from the runtime dynamic-unsubscribe core
/// ([`Messenger::unsubscribe_dynamic`]).
///
/// Returned, never panicked — a bad unsubscribe is tool/LLM input, not a host
/// bug (design §4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeUnsubscribeError {
    /// The calling app holds a **static** (TOML-configured) subscription on this
    /// channel. A static subscription has no durable dynamic row, so it is
    /// structurally unreachable by unsubscribe — static subs are config-managed
    /// and cannot be removed by a tool (design §2.3). Discriminated from
    /// [`NotSubscribed`](RuntimeUnsubscribeError::NotSubscribed) by the presence
    /// of an `App(app_slug)` subscriber on the resolved directory entry.
    StaticSubscription { address: String },
    /// The calling app holds **no** subscription at all on this channel (neither
    /// static nor dynamic) — there is nothing to remove (design §2.3).
    NotSubscribed { address: String },
}

impl std::fmt::Display for RuntimeUnsubscribeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RuntimeUnsubscribeError::StaticSubscription { address } => write!(
                f,
                "the subscription to {address:?} is static (config-managed) and \
                 cannot be removed at runtime"
            ),
            RuntimeUnsubscribeError::NotSubscribed { address } => write!(
                f,
                "no subscription to {address:?} to remove (this app is not \
                 subscribed to that channel)"
            ),
        }
    }
}

impl std::error::Error for RuntimeUnsubscribeError {}

/// Successful outcome of [`Messenger::unsubscribe_dynamic`] (the generic
/// transport-agnostic core).
///
/// Carries the removed channel's UUID and whether any other subscriber (static
/// or dynamic) still remains on the channel after the removal. The per-transport
/// activation layer (design §2.3, a later increment) needs both: for `mqtt:` it
/// issues a broker UNSUBSCRIBE + drops the route/`IngressSubscription` only when
/// `still_subscribed` is `false` (no other subscriber left on the filter), and
/// leaves the broker subscription in place otherwise. `brenn:`/`webhook:` ignore
/// it (no broker interaction).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnsubscribeOutcome {
    /// The channel the subscriber was removed from.
    pub channel_uuid: uuid::Uuid,
    /// `true` if at least one other subscriber (static or dynamic, any kind)
    /// still remains on the channel after this app's dynamic sub was removed;
    /// `false` if this was the last subscriber. The MQTT activation layer issues
    /// a broker UNSUBSCRIBE only when this is `false` (design §2.3 unsubscribe).
    pub still_subscribed: bool,
    /// `true` if the removed row was **dormant** — a durable dynamic row with no
    /// folded directory subscriber (a boot-merge `revoked` row: ACL revoked, or
    /// retain_depth over standing). Nothing was activated for it this boot, so the
    /// MQTT activation layer must skip deactivation entirely (no route to drop, no
    /// broker UNSUBSCRIBE to issue). `still_subscribed` in the dormant case is
    /// read from the untouched step-1 directory entry.
    pub was_dormant: bool,
}

impl Messenger {
    /// Create a dynamic subscription for `app_slug` on an **existing** channel
    /// (design §2.3, transport-agnostic core).
    ///
    /// Steps: validate `qos` placement → resolve params (shared resolver) →
    /// reject an already-present dynamic sub for this app → persist the durable
    /// row + mirror (one transaction) → fold the subscriber into the directory.
    ///
    /// This does **not** perform transport activation (the MQTT broker SUBSCRIBE
    /// and not-yet-existing-channel creation are the per-transport activation
    /// increment's job). The persisted `qos` is carried for that step. Returns the
    /// [`ResolvedSubscription`] so the caller (tool/activation layer) has the
    /// concrete resolved params.
    ///
    /// Errors (never panics): unknown channel, `qos` on a non-MQTT address, an
    /// existing dynamic sub for this app, or any resolver/invariant violation.
    /// Resolve the channel for `address`, creating it when the address is a
    /// not-yet-existing `mqtt:` topic filter (design §2.3 step 3).
    ///
    /// - **Existing channel** → returned as-is (any transport).
    /// - **Absent `brenn:`/`webhook:`** → `UnknownChannel` (never auto-created —
    ///   a channel nobody can publish to is meaningless, design §2.3).
    /// - **Absent `mqtt:`** → validate the topic filter, derive the canonical
    ///   address + deterministic UUID, build a `ChannelEntry` inheriting the
    ///   global messaging defaults (mirroring the boot mqtt-channel derivation in
    ///   `bootstrap/messaging.rs`), upsert `messaging_channels`, and `add_channel`
    ///   into the directory. The broker SUBSCRIBE / `IngressRoute` add / live
    ///   configured-client check are the bin-crate activation layer's job (design
    ///   §2.3 steps 1/5/6), not this generic core.
    async fn resolve_or_create_channel(
        &self,
        address: &str,
        is_mqtt: bool,
    ) -> Result<std::sync::Arc<super::ChannelEntry>, RuntimeSubscribeError> {
        if let Some(entry) = self.directory.resolve(address) {
            return Ok(entry);
        }
        if !is_mqtt {
            return Err(RuntimeSubscribeError::UnknownChannel {
                address: address.to_string(),
            });
        }

        // New `mqtt:` topic-filter channel. Parse + validate the filter (wildcard
        // placement, non-empty topic, byte limits) and re-derive the canonical
        // `mqtt:<client>:<topic>` address so the UUID matches the static/router
        // derivation exactly (they all key off the canonical formatter).
        let parsed =
            parse_mqtt_address(address).map_err(|e| RuntimeSubscribeError::InvalidMqttFilter {
                address: address.to_string(),
                detail: e.to_string(),
            })?;
        validate_topic_filter_str(&parsed.topic).map_err(|detail| {
            RuntimeSubscribeError::InvalidMqttFilter {
                address: address.to_string(),
                detail,
            }
        })?;
        let canonical = parsed.format();
        let uuid = mqtt_channel_uuid_from_address(&canonical);
        let entry = ChannelEntry {
            uuid,
            address: canonical,
            description: None,
            resolved_channel: ResolvedChannel {
                push_depth: self.defaults.default_push_depth,
                retain_depth: self.defaults.default_retain_depth,
                standing_retain_depth: self.defaults.default_standing_retain_depth,
                noise: self.defaults.default_noise,
                sink: self.defaults.default_sink,
                wake_min: self.defaults.default_wake_min,
            },
            subscribers: Vec::new(),
            transport_type: ChannelScheme::Mqtt,
            mount: None,
        };
        // Persist the channel row, then make it resolvable in the directory. The
        // upsert is keyed by UUID (idempotent if a concurrent path created it);
        // `add_channel` panics on a UUID/address collision, which after the
        // `resolve` miss above would be a host bug (no other path creates this
        // channel between the miss and here on this single subscribe call).
        {
            let conn = self.db.lock().await;
            upsert_channels(&conn, std::slice::from_ref(&entry));
        }
        let canonical = entry.address.clone();
        self.directory.add_channel(entry);
        // Re-resolve by the *canonical* address — the exact key `add_channel`
        // indexed (errhandling-2). The raw caller `address` may normalize to a
        // different string; resolving by it could miss the just-added entry and
        // panic with a misleading "host bug" message. The canonical key is the one
        // actually stored, so this resolve is infallible.
        Ok(self
            .directory
            .resolve(&canonical)
            .expect("subscribe_dynamic: channel absent immediately after add_channel"))
    }

    pub async fn subscribe_dynamic(
        &self,
        app_slug: &str,
        address: &str,
        params: DynamicSubscribeParams,
    ) -> Result<SubscribeOutcome, RuntimeSubscribeError> {
        // 1. `qos` is MQTT-only — reject it on a non-MQTT address rather than
        //    silently ignoring a caller mistake (design §2.3). Determine the
        //    transport from the address prefix (not the directory entry, which may
        //    not exist yet for a new `mqtt:` filter).
        let is_mqtt = matches!(ChannelScheme::of(address), Some(ChannelScheme::Mqtt));
        if params.qos.is_some() && !is_mqtt {
            return Err(RuntimeSubscribeError::QosOnNonMqtt {
                address: address.to_string(),
            });
        }

        // 2. Resolve the target channel, creating it for a not-yet-existing
        //    `mqtt:` topic-filter address (design §2.3 step 3). `brenn:`/`webhook:`
        //    never auto-create: a channel nobody publishes to is meaningless, so
        //    an absent one is a terminal `UnknownChannel` error.
        let entry = self.resolve_or_create_channel(address, is_mqtt).await?;
        let qos = params.qos;

        // 3. Resolve the requested params via the shared resolver (design §2.2).
        //    Done *before* the re-subscribe identity check so an identical /
        //    differing comparison is made against the fully-resolved values
        //    (inheritance applied), not the raw request — matching how the
        //    existing subscriber's directory entry already carries resolved
        //    values. brenn:/webhook: inherit sub → channel → global (channel
        //    rung); mqtt: has no per-channel layer, so sub → global (global rung).
        let rung = if is_mqtt {
            SubscriptionParamDefaults::from_global(&self.defaults)
        } else {
            SubscriptionParamDefaults::from_channel(&entry.resolved_channel)
        };
        let (singleton, allowed_users) = match self.apps.get(app_slug) {
            Some(app) => (app.singleton, app.allowed_users.len()),
            // No app config ⇒ not a singleton, zero allowed users. A push-enabled
            // sub would then fail the resolver invariants (correct); a pull-only
            // sub is fine. Either way this is a returned error, not a panic.
            None => (false, 0),
        };
        let raw = RawSubscriptionParams {
            channel_uuid: entry.uuid,
            channel_address: address.to_string(),
            push_depth: Some(params.push_depth),
            retain_depth: Some(params.retain_depth),
            noise: params.noise,
            wake_min: params.wake_min,
        };
        let resolved = config::resolve_subscription_params(&raw, &rung, singleton, allowed_users)?;

        // 4. Re-subscribe / existing-subscriber policy + retain-depth cap.
        //    The dynamic-path cap: a runtime dynamic
        //    sub's resolved retain_depth may not exceed the channel's standing
        //    retain depth (its read clamp for MessageChannelGet history — allowing
        //    it past standing would let a semi-trusted principal read deeper than
        //    the operator's baseline window). Only strictly-greater is rejected;
        //    equality and an Unbounded standing are fine. Computed on the resolved
        //    value so it stays correct if retain_depth ever becomes inheritable.
        let cap_exceeds = resolved.retain_depth > entry.resolved_channel.standing_retain_depth;

        // Existing-subscriber / re-subscribe policy + the dynamic-path retain cap.
        //
        // Two facts classify the state: whether this app holds an `App(app_slug)`
        // directory subscriber, and whether a durable dynamic row exists for
        // `(channel, app)`. Load the durable row once (it also carries the mqtt
        // `qos` the directory entry lacks, needed for the identity comparison):
        //   - directory subscriber, no durable row ⇒ a *static* (config-managed)
        //     sub — never shadow or mutate it.
        //   - no directory subscriber, durable row present ⇒ a *dormant* boot-merge
        //     `revoked` row (durable-only, unfolded, invisible to
        //     MessageSubscriptionList). The step-5 INSERT would collide on the
        //     (channel_uuid, app_slug) PK and panic; reject instead. The app must
        //     MessageUnsubscribe first, then re-subscribe.
        //   - directory subscriber + durable row ⇒ a live dynamic sub: identity-only
        //     re-subscribe (identical resolved params incl. qos = idempotent
        //     success; differing = error). Resolving in the core, where the resolver
        //     and the subscriber entry are both in hand, is the only place the
        //     comparison can be exact.
        //   - neither ⇒ a fresh subscribe (step 5).
        let existing_entry = entry.app_subscriber(app_slug);
        let durable_row = {
            let conn = self.db.lock().await;
            super::db::load_dynamic_subscription_for(&conn, entry.uuid, app_slug)
        };
        // Static and dormant are checked BEFORE the cap: each is the more actionable
        // error (a static holder can never succeed by lowering its depth, so the cap
        // error would be a lie; a dormant holder must unsubscribe first regardless of
        // depth). The dormant reject re-establishes insert_dynamic_subscription's
        // "neither row pre-exists" guarantee for the step-5 INSERT.
        match (existing_entry.is_some(), durable_row.is_some()) {
            (true, false) => {
                return Err(RuntimeSubscribeError::StaticSubscriptionExists {
                    address: address.to_string(),
                });
            }
            (false, true) => {
                return Err(RuntimeSubscribeError::DormantSubscriptionExists {
                    address: address.to_string(),
                });
            }
            _ => {}
        }
        // Cap BEFORE identity/insert: an over-standing request must never return
        // AlreadySubscribedIdentical for a depth the current config forbids, and must
        // never persist (fail-closed defense-in-depth — no live path can fold an
        // over-standing row, but this pins the ordering even for an unsupported
        // state).
        if cap_exceeds {
            return Err(RuntimeSubscribeError::RetainDepthExceedsStanding {
                address: address.to_string(),
                requested: resolved.retain_depth,
                standing: entry.resolved_channel.standing_retain_depth,
            });
        }
        if let (Some(existing), Some(existing_row)) = (existing_entry, durable_row) {
            // Live dynamic re-subscribe: identity-only policy.
            let identical = existing.push_depth == resolved.push_depth
                && existing.retain_depth == resolved.retain_depth
                && existing.noise == resolved.noise
                && existing.wake_min == Some(resolved.wake_min)
                && existing_row.qos == qos;
            if identical {
                return Ok(SubscribeOutcome::AlreadySubscribedIdentical(resolved));
            }
            return Err(RuntimeSubscribeError::AlreadySubscribedDiffers {
                address: address.to_string(),
            });
        }

        // 5. Persist the durable row + mirror in one transaction, then fold the
        //    subscriber into the directory. The DB write is the durable truth; the
        //    directory swap makes it visible to the publish hot path.
        let row = DynamicSubscriptionRow {
            channel_uuid: resolved.channel_uuid,
            app_slug: app_slug.to_string(),
            push_depth: resolved.push_depth,
            retain_depth: resolved.retain_depth,
            noise: resolved.noise,
            wake_min: resolved.wake_min,
            qos,
            created_at: format_ts_for_db(chrono::Utc::now()),
        };
        {
            let conn = self.db.lock().await;
            insert_dynamic_subscription(&conn, &row);
        }
        let applied = self.directory.add_subscriber(
            &resolved.channel_uuid,
            SubscriberEntry {
                kind: SubscriberEntryKind::App(app_slug.to_string()),
                push_depth: resolved.push_depth,
                retain_depth: resolved.retain_depth,
                noise: resolved.noise,
                wake_min: Some(resolved.wake_min),
            },
        );
        // The channel was present at step 1 and only this single-threaded path
        // adds/removes it between resolve and add_subscriber under boot's lock
        // ordering; a vanished channel here is a host bug.
        assert!(
            applied,
            "subscribe_dynamic: channel {address:?} vanished between resolve and add_subscriber"
        );

        Ok(SubscribeOutcome::Created(resolved))
    }

    /// Remove `app_slug`'s dynamic subscription on the channel at `address`
    /// (design §2.3, transport-agnostic core — the inverse of
    /// [`Messenger::subscribe_dynamic`]).
    ///
    /// Steps: delete the durable `messaging_dynamic_subscriptions` row **and**
    /// its `messaging_subscriptions` mirror in one transaction
    /// ([`db::delete_dynamic_subscription`]); then fold the subscriber out of the
    /// in-memory directory (copy-on-write [`MessagingDirectory::remove_subscriber`]).
    /// The durable delete is the authority on "did a dynamic sub exist": it
    /// returns `false` for a `(channel, app)` with no durable row — which is both
    /// the not-subscribed *and* the static-only case (a static sub has no durable
    /// row). Those two are then discriminated on the in-memory directory entry
    /// into [`RuntimeUnsubscribeError::StaticSubscription`] vs
    /// [`RuntimeUnsubscribeError::NotSubscribed`] (design §2.3).
    ///
    /// This does **not** perform transport activation — the MQTT broker
    /// UNSUBSCRIBE and last-subscriber route/`IngressSubscription` drop are the
    /// per-transport activation increment's job (design §2.3 unsubscribe). The
    /// returned [`UnsubscribeOutcome::still_subscribed`] tells that layer whether
    /// any other subscriber remains on the filter (so it knows whether to issue
    /// the broker UNSUBSCRIBE).
    ///
    /// Errors (never panics): no dynamic subscription for this app on the channel.
    /// Removing another app's sub, or a static TOML sub, is structurally
    /// impossible — the delete is keyed on `(channel_uuid, app_slug)` and static
    /// subs carry no durable dynamic row.
    pub async fn unsubscribe_dynamic(
        &self,
        app_slug: &str,
        address: &str,
    ) -> Result<UnsubscribeOutcome, RuntimeUnsubscribeError> {
        // 1. Resolve the channel UUID from the address. If no channel exists for
        //    this address, the app cannot hold any sub of any kind on it — that is
        //    unambiguously the not-subscribed case, not a host bug. (`address` is
        //    LLM/tool input.)
        let Some(entry) = self.directory.resolve(address) else {
            return Err(RuntimeUnsubscribeError::NotSubscribed {
                address: address.to_string(),
            });
        };
        let channel_uuid = entry.uuid;

        // 2. Delete the durable row + mirror in one transaction. The return value
        //    is the authority on whether a dynamic sub existed: a static-only or
        //    not-subscribed `(channel, app)` has no durable dynamic row, so this
        //    is `false` and no mutation happened. Static subs are config-managed
        //    and structurally unreachable here (no durable row), so this can never
        //    remove one (design §2.3 / §2.1).
        let removed = {
            let conn = self.db.lock().await;
            delete_dynamic_subscription(&conn, channel_uuid, app_slug)
        };
        if !removed {
            // No durable dynamic row for `(channel, app)`. Discriminate the two
            // cases purely in-memory on the resolved `entry` (design §2.3): a
            // surviving `App(app_slug)` directory subscriber with no durable
            // dynamic row is, by the same convention `subscribe_dynamic` uses, a
            // static (config-managed) sub; no such subscriber means the app holds
            // no sub of any kind on this channel.
            let has_static_sub = entry.app_subscriber(app_slug).is_some();
            return Err(if has_static_sub {
                RuntimeUnsubscribeError::StaticSubscription {
                    address: address.to_string(),
                }
            } else {
                RuntimeUnsubscribeError::NotSubscribed {
                    address: address.to_string(),
                }
            });
        }

        // 3. Fold the subscriber out of the directory (copy-on-write), reading the
        //    remaining-subscriber count from the same write-lock critical section.
        //    `remove_subscriber` returns `None` in two distinct conditions (channel
        //    UUID absent, or subscriber absent from a present channel); only one is
        //    designed here. Discriminate against the step-1 directory snapshot, not
        //    the bare `None`:
        //    - **Dormant row** — the step-1 snapshot had no `App(app_slug)`
        //      subscriber. This is a boot-merge `revoked` durable row (ACL revoked,
        //      or retain_depth over standing): durable-only, never folded. A
        //      successful durable delete with no directory subscriber to remove is
        //      the *designed* dormant state, not a bug — success, not a panic.
        //    - **Inconsistency** — the snapshot *did* carry the subscriber but
        //      removal still found none: a durable/directory inconsistency, a host
        //      bug. Keep the panic.
        let snapshot_had_subscriber = entry.app_subscriber(app_slug).is_some();
        let (still_subscribed, was_dormant) = match self
            .directory
            .remove_subscriber(&channel_uuid, app_slug)
        {
            Some(remaining) => {
                // Removed a folded subscriber: the remaining count came from
                // `remove_subscriber` under its own write-lock — no second
                // `resolve` + entry clone needed.
                (remaining > 0, false)
            }
            None if !snapshot_had_subscriber => {
                // Dormant row: the directory was never mutated. Report
                // `still_subscribed` from the untouched step-1 snapshot's other
                // subscribers (this app's row was never folded, so it does not
                // appear there).
                (!entry.subscribers.is_empty(), true)
            }
            None => panic!(
                "unsubscribe_dynamic: durable dynamic row for {address:?} existed and the step-1 \
                 directory snapshot carried an App({app_slug}) subscriber, but directory removal \
                 found none — durable/directory inconsistency (host bug)"
            ),
        };

        Ok(UnsubscribeOutcome {
            channel_uuid,
            still_subscribed,
            was_dormant,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AppConfig;
    use crate::db::init_db_memory;
    use crate::messaging::config::{MessagingGlobalConfig, ResolvedChannel, Sink};
    use crate::messaging::db::{load_dynamic_subscriptions, upsert_channels};
    use crate::messaging::test_support::test_app_config;
    use crate::messaging::{
        ChannelDetails, ChannelEntry, ChannelScheme, MessageEnvelope, MessagingDirectory,
        ParticipantId, WakeRouter,
    };
    use indexmap::IndexMap;
    use std::sync::Arc;
    use uuid::Uuid;

    /// No-op `WakeRouter`: this core never delivers/wakes, so the router is unused.
    struct NoopRouter;

    #[async_trait::async_trait]
    impl WakeRouter for NoopRouter {
        async fn deliver(
            &self,
            _key: &crate::messaging::SubscriberEntryKind,
            _subscriber: &ParticipantId,
            _envelope: &MessageEnvelope,
            _push_id: i64,
            _seq: i64,
        ) -> Result<bool, String> {
            Ok(false)
        }
        async fn deliver_ingress(
            &self,
            _key: &crate::messaging::SubscriberEntryKind,
            _subscriber: &ParticipantId,
            _event: &crate::messaging::ingress::Event,
        ) -> Result<bool, String> {
            Ok(false)
        }
        fn spawn_eager_wake(
            &self,
            _key: &crate::messaging::SubscriberEntryKind,
            _subscriber: &ParticipantId,
        ) {
        }
        fn delivery_shape(
            &self,
            key: &crate::messaging::SubscriberEntryKind,
        ) -> crate::messaging::DeliveryShape {
            crate::messaging::default_delivery_shape(key)
        }
        fn alarm(&self, _channel: &str, _subscriber: &ParticipantId) {}
    }

    fn channel(address: &str, transport: ChannelScheme) -> ChannelEntry {
        ChannelEntry {
            uuid: Uuid::new_v4(),
            address: address.to_string(),
            description: None,
            transport_type: transport,
            resolved_channel: ResolvedChannel {
                push_depth: Depth::Bounded(0),
                retain_depth: Depth::Bounded(10),
                standing_retain_depth: Depth::Bounded(10),
                noise: NoiseLevel::Silent,
                sink: Sink::Drop,
                wake_min: WakeMin::Normal,
            },
            subscribers: Vec::new(),
            mount: None,
        }
    }

    /// Build a `Messenger` over an in-memory DB seeded with `entries`, and an
    /// `apps` map carrying `app_specs` `(slug, singleton, allowed_users)`.
    async fn messenger(
        entries: Vec<ChannelEntry>,
        app_specs: &[(&str, bool, &[&str])],
    ) -> Arc<Messenger> {
        let db = init_db_memory();
        {
            let conn = db.lock().await;
            upsert_channels(&conn, &entries);
        }
        let directory = Arc::new(MessagingDirectory::with_entries(entries));
        let mut apps: IndexMap<String, AppConfig> = IndexMap::new();
        for (slug, singleton, users) in app_specs {
            let mut app =
                test_app_config(slug, None, users.iter().map(|u| u.to_string()).collect());
            app.singleton = *singleton;
            apps.insert(slug.to_string(), app);
        }
        Messenger::new(
            db,
            directory,
            Arc::from("test-source"),
            Arc::new(apps),
            Arc::new(NoopRouter) as Arc<dyn WakeRouter>,
            MessagingGlobalConfig::default(),
        )
    }

    fn pull_only(qos: Option<u8>) -> DynamicSubscribeParams {
        DynamicSubscribeParams {
            push_depth: Depth::Bounded(0),
            retain_depth: Depth::Bounded(5),
            noise: None,
            wake_min: None,
            qos,
        }
    }

    /// Seed a durable dynamic-subscription row directly, with no directory
    /// subscriber folded — the shape of a boot-merge `revoked`/dormant row
    /// (durable-only, invisible to `MessageSubscriptionList`). Pull-only, silent,
    /// no qos: the fields the dormant/cap tests never vary; only `retain_depth`
    /// differs across callers.
    async fn seed_dynamic_row(
        m: &Messenger,
        channel_uuid: Uuid,
        app_slug: &str,
        retain_depth: Depth,
    ) {
        let conn = m.db.lock().await;
        insert_dynamic_subscription(
            &conn,
            &DynamicSubscriptionRow {
                channel_uuid,
                app_slug: app_slug.to_string(),
                push_depth: Depth::Bounded(0),
                retain_depth,
                noise: NoiseLevel::Silent,
                wake_min: WakeMin::Normal,
                qos: None,
                created_at: crate::db::format_ts_for_db(chrono::Utc::now()),
            },
        );
    }

    /// A pull-only subscribe to an existing `brenn:` channel: resolves params,
    /// persists the durable row + mirror, and adds the subscriber to the directory.
    #[tokio::test]
    async fn subscribe_existing_brenn_channel_persists_and_folds() {
        let ch = channel("heartbeat", ChannelScheme::Brenn);
        let uuid = ch.uuid;
        let m = messenger(vec![ch], &[("graf", false, &["u"])]).await;

        let outcome = m
            .subscribe_dynamic("graf", "heartbeat", pull_only(None))
            .await
            .expect("subscribe succeeds");
        assert!(outcome.is_created());
        let resolved = outcome.resolved();
        assert_eq!(resolved.channel_uuid, uuid);
        assert_eq!(resolved.push_depth, Depth::Bounded(0));
        assert_eq!(resolved.retain_depth, Depth::Bounded(5));

        // Durable row persisted (no qos for brenn:).
        let rows = {
            let conn = m.db.lock().await;
            load_dynamic_subscriptions(&conn)
        };
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].app_slug, "graf");
        assert_eq!(rows[0].qos, None);

        // Directory now carries the App(graf) subscriber.
        let entry = m.directory.resolve("heartbeat").expect("channel present");
        assert!(
            entry
                .subscribers
                .iter()
                .any(|s| matches!(&s.kind, SubscriberEntryKind::App(slug) if slug == "graf")),
            "subscriber folded into directory"
        );
    }

    /// MQTT channel accepts a `qos`; it is persisted on the durable row.
    #[tokio::test]
    async fn subscribe_mqtt_channel_persists_qos() {
        let ch = channel("mqtt:home:sensors/temp", ChannelScheme::Mqtt);
        let m = messenger(vec![ch], &[("graf", false, &["u"])]).await;

        m.subscribe_dynamic("graf", "mqtt:home:sensors/temp", pull_only(Some(1)))
            .await
            .expect("mqtt subscribe succeeds");

        let rows = {
            let conn = m.db.lock().await;
            load_dynamic_subscriptions(&conn)
        };
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].qos, Some(1));
    }

    /// Unknown channel → error, nothing persisted.
    #[tokio::test]
    async fn subscribe_unknown_channel_errors() {
        let m = messenger(vec![], &[("graf", false, &["u"])]).await;
        let err = m
            .subscribe_dynamic("graf", "nope", pull_only(None))
            .await
            .unwrap_err();
        assert!(matches!(err, RuntimeSubscribeError::UnknownChannel { .. }));
        let rows = {
            let conn = m.db.lock().await;
            load_dynamic_subscriptions(&conn)
        };
        assert!(rows.is_empty());
    }

    /// `qos` supplied for a non-MQTT (`brenn:`) channel → error, nothing persisted.
    #[tokio::test]
    async fn subscribe_qos_on_brenn_errors() {
        let ch = channel("heartbeat", ChannelScheme::Brenn);
        let m = messenger(vec![ch], &[("graf", false, &["u"])]).await;
        let err = m
            .subscribe_dynamic("graf", "heartbeat", pull_only(Some(0)))
            .await
            .unwrap_err();
        assert!(matches!(err, RuntimeSubscribeError::QosOnNonMqtt { .. }));
        let rows = {
            let conn = m.db.lock().await;
            load_dynamic_subscriptions(&conn)
        };
        assert!(rows.is_empty());
    }

    /// Push-enabled sub on a non-singleton app → resolver invariant error
    /// (mapped to `Params`), not a panic. Nothing persisted.
    #[tokio::test]
    async fn subscribe_push_enabled_on_non_singleton_errors() {
        let ch = channel("heartbeat", ChannelScheme::Brenn);
        let m = messenger(vec![ch], &[("graf", false, &["u"])]).await;
        let params = DynamicSubscribeParams {
            push_depth: Depth::Bounded(3),
            retain_depth: Depth::Bounded(5),
            noise: None,
            wake_min: None,
            qos: None,
        };
        let err = m
            .subscribe_dynamic("graf", "heartbeat", params)
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            RuntimeSubscribeError::Params(SubscribeError::PushEnabledRequiresSingleton { .. })
        ));
        let rows = {
            let conn = m.db.lock().await;
            load_dynamic_subscriptions(&conn)
        };
        assert!(rows.is_empty());
    }

    /// A runtime `MessageSubscribe` with `noise = "fatal"` is a **returned**
    /// error (bad tool input), never a panic — `fatal` is the surface-only kill
    /// rung with no referent on a backend subscription. `alarm` still resolves.
    #[tokio::test]
    async fn subscribe_fatal_noise_returns_error() {
        let ch = channel("heartbeat", ChannelScheme::Brenn);
        let m = messenger(vec![ch], &[("graf", true, &["u"])]).await;
        let fatal = DynamicSubscribeParams {
            push_depth: Depth::Bounded(3),
            retain_depth: Depth::Bounded(5),
            noise: Some(NoiseLevel::Fatal),
            wake_min: None,
            qos: None,
        };
        let err = m
            .subscribe_dynamic("graf", "heartbeat", fatal)
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            RuntimeSubscribeError::Params(SubscribeError::FatalNoise { .. })
        ));
        // Nothing persisted.
        let rows = {
            let conn = m.db.lock().await;
            load_dynamic_subscriptions(&conn)
        };
        assert!(rows.is_empty());
        // `alarm` on the same channel resolves and persists.
        let alarm = DynamicSubscribeParams {
            push_depth: Depth::Bounded(3),
            retain_depth: Depth::Bounded(5),
            noise: Some(NoiseLevel::Alarm),
            wake_min: None,
            qos: None,
        };
        m.subscribe_dynamic("graf", "heartbeat", alarm)
            .await
            .expect("alarm noise must resolve");
    }

    /// A re-subscribe by the same app on the same channel with **identical**
    /// resolved params is an idempotent no-op success (design §2.4): the outcome
    /// is `AlreadySubscribedIdentical`, and no second durable row is written.
    #[tokio::test]
    async fn subscribe_resubscribe_identical_is_idempotent_noop() {
        let ch = channel("heartbeat", ChannelScheme::Brenn);
        let m = messenger(vec![ch], &[("graf", false, &["u"])]).await;
        m.subscribe_dynamic("graf", "heartbeat", pull_only(None))
            .await
            .expect("first subscribe succeeds");

        let outcome = m
            .subscribe_dynamic("graf", "heartbeat", pull_only(None))
            .await
            .expect("identical re-subscribe is a no-op success");
        assert!(matches!(
            outcome,
            SubscribeOutcome::AlreadySubscribedIdentical(_)
        ));

        let rows = {
            let conn = m.db.lock().await;
            load_dynamic_subscriptions(&conn)
        };
        assert_eq!(rows.len(), 1, "no duplicate durable row");
    }

    /// A re-subscribe by the same app on the same channel with **different**
    /// resolved params → error (re-subscribe param mutation is withheld;
    /// MessageUnsubscribe first). The first subscriber and its durable row are
    /// untouched.
    #[tokio::test]
    async fn subscribe_resubscribe_differs_errors() {
        let ch = channel("heartbeat", ChannelScheme::Brenn);
        let m = messenger(vec![ch], &[("graf", false, &["u"])]).await;
        m.subscribe_dynamic("graf", "heartbeat", pull_only(None))
            .await
            .expect("first subscribe succeeds");

        // Same channel, different retain_depth → differs. Kept within the
        // channel's standing depth (10) so it reaches the identity/differs
        // comparison rather than tripping the over-standing cap first.
        let differing = DynamicSubscribeParams {
            push_depth: Depth::Bounded(0),
            retain_depth: Depth::Bounded(7),
            noise: None,
            wake_min: None,
            qos: None,
        };
        let err = m
            .subscribe_dynamic("graf", "heartbeat", differing)
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            RuntimeSubscribeError::AlreadySubscribedDiffers { .. }
        ));

        let rows = {
            let conn = m.db.lock().await;
            load_dynamic_subscriptions(&conn)
        };
        assert_eq!(rows.len(), 1, "original durable row unchanged");
        assert_eq!(rows[0].retain_depth, Depth::Bounded(5), "params unmutated");
    }

    /// Subscribing to a channel the app already has a *static* subscription on
    /// (a directory `App(slug)` subscriber with no durable dynamic row) → error;
    /// static subs are config-managed and unshadowable (design §2.1). No durable
    /// row is written.
    #[tokio::test]
    async fn subscribe_over_static_subscription_errors() {
        let mut ch = channel("heartbeat", ChannelScheme::Brenn);
        // Pre-existing STATIC subscriber: in the directory, but no dynamic row.
        ch.subscribers.push(SubscriberEntry {
            kind: SubscriberEntryKind::App("graf".to_string()),
            push_depth: Depth::Unbounded,
            retain_depth: Depth::Unbounded,
            noise: NoiseLevel::Silent,
            wake_min: Some(WakeMin::Normal),
        });
        let m = messenger(vec![ch], &[("graf", false, &["u"])]).await;

        let err = m
            .subscribe_dynamic("graf", "heartbeat", pull_only(None))
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            RuntimeSubscribeError::StaticSubscriptionExists { .. }
        ));
        let rows = {
            let conn = m.db.lock().await;
            load_dynamic_subscriptions(&conn)
        };
        assert!(rows.is_empty(), "no durable row written over a static sub");
    }

    /// A dynamic subscribe params builder with an explicit `retain_depth`.
    fn pull_only_retain(retain: Depth) -> DynamicSubscribeParams {
        DynamicSubscribeParams {
            push_depth: Depth::Bounded(0),
            retain_depth: retain,
            noise: None,
            wake_min: None,
            qos: None,
        }
    }

    /// A `brenn:` channel with an explicit `standing_retain_depth`.
    fn channel_with_standing(
        address: &str,
        transport: ChannelScheme,
        standing: Depth,
    ) -> ChannelEntry {
        let mut ch = channel(address, transport);
        ch.resolved_channel.standing_retain_depth = standing;
        ch
    }

    /// Assert `(channel, app)` has neither a durable dynamic row nor a directory
    /// `App(app)` subscriber — the "rejection persisted nothing" invariant.
    async fn assert_nothing_persisted(m: &Messenger, address: &str, app: &str) {
        let rows = {
            let conn = m.db.lock().await;
            load_dynamic_subscriptions(&conn)
        };
        assert!(rows.is_empty(), "no durable row written on rejection");
        if let Some(entry) = m.directory.resolve(address) {
            assert!(
                !entry
                    .subscribers
                    .iter()
                    .any(|s| matches!(&s.kind, SubscriberEntryKind::App(slug) if slug == app)),
                "no directory subscriber folded on rejection"
            );
        }
    }

    /// The dynamic-path cap: on a channel with a **bounded** standing
    /// retain depth, a dynamic subscribe whose resolved `retain_depth` strictly
    /// exceeds standing (`Unbounded`, or `Bounded(standing+1)`) is rejected with
    /// `RetainDepthExceedsStanding` and persists nothing; equal or lesser depths
    /// are `Created`.
    #[tokio::test]
    async fn subscribe_over_standing_retain_depth_rejected() {
        // Requested Unbounded over Bounded(10) standing → rejected.
        let m = messenger(
            vec![channel_with_standing(
                "heartbeat",
                ChannelScheme::Brenn,
                Depth::Bounded(10),
            )],
            &[("graf", false, &["u"])],
        )
        .await;
        let err = m
            .subscribe_dynamic("graf", "heartbeat", pull_only_retain(Depth::Unbounded))
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            RuntimeSubscribeError::RetainDepthExceedsStanding { .. }
        ));
        assert!(
            err.to_string().contains("standing"),
            "message names the standing bound: {err}"
        );
        assert_nothing_persisted(&m, "heartbeat", "graf").await;

        // Requested Bounded(standing+1) → rejected.
        let m = messenger(
            vec![channel_with_standing(
                "heartbeat",
                ChannelScheme::Brenn,
                Depth::Bounded(10),
            )],
            &[("graf", false, &["u"])],
        )
        .await;
        let err = m
            .subscribe_dynamic("graf", "heartbeat", pull_only_retain(Depth::Bounded(11)))
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            RuntimeSubscribeError::RetainDepthExceedsStanding { .. }
        ));
        assert_nothing_persisted(&m, "heartbeat", "graf").await;

        // Requested exactly standing → Created.
        let m = messenger(
            vec![channel_with_standing(
                "heartbeat",
                ChannelScheme::Brenn,
                Depth::Bounded(10),
            )],
            &[("graf", false, &["u"])],
        )
        .await;
        let outcome = m
            .subscribe_dynamic("graf", "heartbeat", pull_only_retain(Depth::Bounded(10)))
            .await
            .expect("retain == standing is allowed");
        assert!(outcome.is_created(), "equal depth creates the sub");

        // Requested below standing → Created.
        let m = messenger(
            vec![channel_with_standing(
                "heartbeat",
                ChannelScheme::Brenn,
                Depth::Bounded(10),
            )],
            &[("graf", false, &["u"])],
        )
        .await;
        let outcome = m
            .subscribe_dynamic("graf", "heartbeat", pull_only_retain(Depth::Bounded(3)))
            .await
            .expect("retain < standing is allowed");
        assert!(outcome.is_created(), "lesser depth creates the sub");
    }

    /// `Unbounded` standing (the repo-wide default) caps nothing: even an
    /// `Unbounded` requested retain_depth is `Created`.
    #[tokio::test]
    async fn subscribe_unbounded_standing_caps_nothing() {
        let m = messenger(
            vec![channel_with_standing(
                "heartbeat",
                ChannelScheme::Brenn,
                Depth::Unbounded,
            )],
            &[("graf", false, &["u"])],
        )
        .await;
        let outcome = m
            .subscribe_dynamic("graf", "heartbeat", pull_only_retain(Depth::Unbounded))
            .await
            .expect("unbounded standing accepts unbounded retain");
        assert!(outcome.is_created());
    }

    /// The cap keys off an auto-created `mqtt:` channel's inherited global standing
    /// depth: with a bounded `default_standing_retain_depth`, an over-standing
    /// dynamic mqtt subscribe is rejected and persists **no subscription state** (no
    /// durable dynamic row, no directory subscriber). The `mqtt:` channel row itself
    /// is created at step 2 (`resolve_or_create_channel`, before the step-4 cap) — a
    /// side effect shared by every post-creation rejection on a new filter (e.g.
    /// resolver `Params` errors), so it is present after the rejected subscribe.
    #[tokio::test]
    async fn subscribe_mqtt_auto_created_channel_honors_global_standing() {
        let global = MessagingGlobalConfig {
            default_standing_retain_depth: Depth::Bounded(2),
            ..MessagingGlobalConfig::default()
        };
        let db = init_db_memory();
        let directory = Arc::new(MessagingDirectory::with_entries(vec![]));
        let mut apps: IndexMap<String, AppConfig> = IndexMap::new();
        apps.insert(
            "graf".to_string(),
            test_app_config("graf", None, vec!["u".to_string()]),
        );
        let m = Messenger::new(
            db,
            directory,
            Arc::from("test-source"),
            Arc::new(apps),
            Arc::new(NoopRouter) as Arc<dyn WakeRouter>,
            global,
        );

        let address = "mqtt:home:sensors/temp";
        let err = m
            .subscribe_dynamic("graf", address, pull_only_retain(Depth::Bounded(5)))
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            RuntimeSubscribeError::RetainDepthExceedsStanding { .. }
        ));
        // No subscription state persisted for the rejected subscribe.
        let rows = {
            let conn = m.db.lock().await;
            load_dynamic_subscriptions(&conn)
        };
        assert!(rows.is_empty(), "rejected mqtt subscribe persists no row");
        // The channel row itself IS a step-2 side effect: it was created (and folded
        // into the directory) before the step-4 cap rejected — with no subscriber.
        let entry = m
            .directory
            .resolve(address)
            .expect("auto-created mqtt channel is a step-2 side effect of the rejected subscribe");
        assert!(
            entry.subscribers.is_empty(),
            "no subscriber folded onto the auto-created channel"
        );
    }

    /// Cap-before-identity: an over-standing dynamic sub seeded
    /// directly into the directory + durable table (an unsupported state no live
    /// path produces) re-subscribed with identical params yields
    /// `RetainDepthExceedsStanding`, never `AlreadySubscribedIdentical`.
    #[tokio::test]
    async fn subscribe_cap_before_identity() {
        let mut ch = channel_with_standing("heartbeat", ChannelScheme::Brenn, Depth::Bounded(2));
        let uuid = ch.uuid;
        // Seed a folded App(graf) subscriber with an over-standing retain_depth.
        ch.subscribers.push(SubscriberEntry {
            kind: SubscriberEntryKind::App("graf".to_string()),
            push_depth: Depth::Bounded(0),
            retain_depth: Depth::Bounded(5),
            noise: NoiseLevel::Silent,
            wake_min: Some(WakeMin::Normal),
        });
        let m = messenger(vec![ch], &[("graf", false, &["u"])]).await;
        // Seed the matching durable row (so the sub reads as dynamic, not static).
        seed_dynamic_row(&m, uuid, "graf", Depth::Bounded(5)).await;

        // Re-subscribe with identical (over-standing) params.
        let err = m
            .subscribe_dynamic("graf", "heartbeat", pull_only_retain(Depth::Bounded(5)))
            .await
            .unwrap_err();
        assert!(
            matches!(
                err,
                RuntimeSubscribeError::RetainDepthExceedsStanding { .. }
            ),
            "cap wins over identity: {err:?}"
        );
    }

    /// Error precedence: an app holding a *static* sub that
    /// requests an over-standing dynamic sub gets `StaticSubscriptionExists`, not
    /// `RetainDepthExceedsStanding` — the static holder can never succeed by
    /// lowering its depth, so the cap error would be a lie.
    #[tokio::test]
    async fn subscribe_static_precedence_over_cap() {
        let mut ch = channel_with_standing("heartbeat", ChannelScheme::Brenn, Depth::Bounded(2));
        // Static App(graf) subscriber: directory only, no durable row.
        ch.subscribers.push(SubscriberEntry {
            kind: SubscriberEntryKind::App("graf".to_string()),
            push_depth: Depth::Unbounded,
            retain_depth: Depth::Unbounded,
            noise: NoiseLevel::Silent,
            wake_min: Some(WakeMin::Normal),
        });
        let m = messenger(vec![ch], &[("graf", false, &["u"])]).await;

        let err = m
            .subscribe_dynamic("graf", "heartbeat", pull_only_retain(Depth::Bounded(5)))
            .await
            .unwrap_err();
        assert!(
            matches!(err, RuntimeSubscribeError::StaticSubscriptionExists { .. }),
            "static-sub precedence over cap: {err:?}"
        );
    }

    /// Dormant-row re-subscribe: a dormant durable row (no
    /// directory subscriber) plus a *conforming* subscribe returns
    /// `DormantSubscriptionExists` — no PK-collision panic — and leaves the durable
    /// row untouched.
    #[tokio::test]
    async fn subscribe_over_dormant_row_errors() {
        let ch = channel_with_standing("heartbeat", ChannelScheme::Brenn, Depth::Bounded(10));
        let uuid = ch.uuid;
        let m = messenger(vec![ch], &[("graf", false, &["u"])]).await;
        // Seed a dormant durable row: durable-only, never folded into the directory.
        seed_dynamic_row(&m, uuid, "graf", Depth::Bounded(5)).await;

        // Conforming subscribe (retain 5 <= standing 10) must NOT panic.
        let err = m
            .subscribe_dynamic("graf", "heartbeat", pull_only_retain(Depth::Bounded(5)))
            .await
            .unwrap_err();
        assert!(
            matches!(err, RuntimeSubscribeError::DormantSubscriptionExists { .. }),
            "dormant row surfaces as DormantSubscriptionExists: {err:?}"
        );
        // The dormant durable row is untouched (still exactly one).
        let rows = {
            let conn = m.db.lock().await;
            load_dynamic_subscriptions(&conn)
        };
        assert_eq!(rows.len(), 1, "dormant durable row untouched");
    }

    /// A new `mqtt:` topic-filter address whose channel does not yet exist is
    /// **created** (design §2.3 step 3): the channel becomes resolvable + listable,
    /// the durable row persists with its `qos`, and the subscriber is folded in.
    #[tokio::test]
    async fn subscribe_new_mqtt_filter_creates_channel() {
        // No channels seeded — the filter channel must be created on subscribe.
        let m = messenger(vec![], &[("graf", false, &["u"])]).await;
        let address = "mqtt:home:sensors/+/temp";

        let outcome = m
            .subscribe_dynamic("graf", address, pull_only(Some(2)))
            .await
            .expect("new mqtt filter subscribe creates the channel and succeeds");
        assert!(outcome.is_created());
        assert_eq!(
            outcome.resolved().channel_uuid,
            mqtt_channel_uuid_from_address(address)
        );

        // Channel now resolvable + listable as an mqtt: channel.
        let entry = m.directory.resolve(address).expect("channel created");
        assert!(matches!(entry.transport_type, ChannelScheme::Mqtt));
        assert!(
            m.directory.list().iter().any(|e| e.address == address),
            "created channel is listable"
        );
        // Subscriber folded; durable row persisted with qos.
        assert!(
            entry
                .subscribers
                .iter()
                .any(|s| matches!(&s.kind, SubscriberEntryKind::App(slug) if slug == "graf"))
        );
        let rows = {
            let conn = m.db.lock().await;
            load_dynamic_subscriptions(&conn)
        };
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].qos, Some(2));
        // Channel row was upserted into messaging_channels (count == 1).
        let channel_count: i64 = {
            let conn = m.db.lock().await;
            conn.query_row("SELECT COUNT(*) FROM messaging_channels", [], |r| r.get(0))
                .unwrap()
        };
        assert_eq!(channel_count, 1);
    }

    /// An absent `mqtt:` address with an invalid topic filter (`#` not terminal)
    /// → `InvalidMqttFilter`; no channel created, nothing persisted.
    #[tokio::test]
    async fn subscribe_new_mqtt_invalid_filter_errors() {
        let m = messenger(vec![], &[("graf", false, &["u"])]).await;
        let err = m
            .subscribe_dynamic("graf", "mqtt:home:a/#/b", pull_only(None))
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            RuntimeSubscribeError::InvalidMqttFilter { .. }
        ));
        // No channel created, no durable row written.
        assert!(m.directory.resolve("mqtt:home:a/#/b").is_none());
        let (rows_empty, channel_count): (bool, i64) = {
            let conn = m.db.lock().await;
            let rows = load_dynamic_subscriptions(&conn);
            let n: i64 = conn
                .query_row("SELECT COUNT(*) FROM messaging_channels", [], |r| r.get(0))
                .unwrap();
            (rows.is_empty(), n)
        };
        assert!(rows_empty);
        assert_eq!(channel_count, 0);
    }

    /// Push-enabled sub on a singleton, single-user app succeeds and the resolved
    /// push_depth is carried through.
    #[tokio::test]
    async fn subscribe_push_enabled_on_singleton_succeeds() {
        let ch = channel("heartbeat", ChannelScheme::Brenn);
        let m = messenger(vec![ch], &[("graf", true, &["only"])]).await;
        let params = DynamicSubscribeParams {
            push_depth: Depth::Bounded(3),
            retain_depth: Depth::Bounded(5),
            noise: None,
            wake_min: None,
            qos: None,
        };
        let outcome = m
            .subscribe_dynamic("graf", "heartbeat", params)
            .await
            .expect("push-enabled singleton subscribe succeeds");
        assert_eq!(outcome.resolved().push_depth, Depth::Bounded(3));
    }

    // --- unsubscribe_dynamic (design §2.3, transport-agnostic core) ---------

    /// Unsubscribe removes the app's dynamic sub: the durable row + mirror are
    /// deleted and the directory subscriber is folded out. With no other
    /// subscriber on the channel, `still_subscribed` is `false`.
    #[tokio::test]
    async fn unsubscribe_removes_own_dynamic_sub() {
        let ch = channel("heartbeat", ChannelScheme::Brenn);
        let uuid = ch.uuid;
        let m = messenger(vec![ch], &[("graf", false, &["u"])]).await;
        m.subscribe_dynamic("graf", "heartbeat", pull_only(None))
            .await
            .expect("subscribe succeeds");

        let outcome = m
            .unsubscribe_dynamic("graf", "heartbeat")
            .await
            .expect("unsubscribe succeeds");
        assert_eq!(outcome.channel_uuid, uuid);
        assert!(
            !outcome.still_subscribed,
            "no other subscriber remains on the channel"
        );

        // Durable row gone.
        let rows = {
            let conn = m.db.lock().await;
            load_dynamic_subscriptions(&conn)
        };
        assert!(rows.is_empty(), "durable row removed");

        // Directory subscriber folded out.
        let entry = m.directory.resolve("heartbeat").expect("channel present");
        assert!(
            !entry
                .subscribers
                .iter()
                .any(|s| matches!(&s.kind, SubscriberEntryKind::App(slug) if slug == "graf")),
            "subscriber folded out of directory"
        );
    }

    /// Unsubscribing a channel the app never subscribed to → error, nothing
    /// mutated.
    #[tokio::test]
    async fn unsubscribe_not_subscribed_errors() {
        let ch = channel("heartbeat", ChannelScheme::Brenn);
        let m = messenger(vec![ch], &[("graf", false, &["u"])]).await;
        let err = m
            .unsubscribe_dynamic("graf", "heartbeat")
            .await
            .unwrap_err();
        assert!(matches!(err, RuntimeUnsubscribeError::NotSubscribed { .. }));
        assert!(
            err.to_string().contains("not subscribed"),
            "message names the not-subscribed case: {err}"
        );
    }

    /// Unsubscribing an address with no channel at all → error (the app cannot
    /// hold a sub of any kind on a non-existent channel).
    #[tokio::test]
    async fn unsubscribe_unknown_channel_errors() {
        let m = messenger(vec![], &[("graf", false, &["u"])]).await;
        let err = m.unsubscribe_dynamic("graf", "nope").await.unwrap_err();
        assert!(matches!(err, RuntimeUnsubscribeError::NotSubscribed { .. }));
    }

    /// A channel the app has only a *static* (config) sub on (a directory
    /// `App(slug)` subscriber with no durable dynamic row) → error; the static
    /// sub is config-managed and structurally unreachable by unsubscribe. The
    /// directory subscriber is left intact.
    #[tokio::test]
    async fn unsubscribe_static_only_sub_errors() {
        let mut ch = channel("heartbeat", ChannelScheme::Brenn);
        ch.subscribers.push(SubscriberEntry {
            kind: SubscriberEntryKind::App("graf".to_string()),
            push_depth: Depth::Unbounded,
            retain_depth: Depth::Unbounded,
            noise: NoiseLevel::Silent,
            wake_min: Some(WakeMin::Normal),
        });
        let m = messenger(vec![ch], &[("graf", false, &["u"])]).await;

        let err = m
            .unsubscribe_dynamic("graf", "heartbeat")
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            RuntimeUnsubscribeError::StaticSubscription { .. }
        ));
        assert!(
            err.to_string().contains("static (config-managed)"),
            "message names the static case: {err}"
        );

        // Static directory subscriber untouched.
        let entry = m.directory.resolve("heartbeat").expect("channel present");
        assert!(
            entry
                .subscribers
                .iter()
                .any(|s| matches!(&s.kind, SubscriberEntryKind::App(slug) if slug == "graf")),
            "static subscriber left intact"
        );
    }

    /// Unsubscribe is scoped to the calling app: removing one app's dynamic sub
    /// leaves another app's dynamic sub (and its durable row) on the same channel
    /// intact, and reports `still_subscribed = true`.
    #[tokio::test]
    async fn unsubscribe_leaves_other_apps_intact_and_reports_still_subscribed() {
        let ch = channel("heartbeat", ChannelScheme::Brenn);
        let m = messenger(
            vec![ch],
            &[("graf", false, &["u"]), ("pfin", false, &["u"])],
        )
        .await;
        m.subscribe_dynamic("graf", "heartbeat", pull_only(None))
            .await
            .expect("graf subscribe");
        m.subscribe_dynamic("pfin", "heartbeat", pull_only(None))
            .await
            .expect("pfin subscribe");

        let outcome = m
            .unsubscribe_dynamic("graf", "heartbeat")
            .await
            .expect("unsubscribe graf");
        assert!(
            outcome.still_subscribed,
            "pfin still subscribed on the channel"
        );

        // Only graf's durable row removed.
        let rows = {
            let conn = m.db.lock().await;
            load_dynamic_subscriptions(&conn)
        };
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].app_slug, "pfin", "other app's durable row survives");

        // Only graf's directory subscriber folded out; pfin remains.
        let entry = m.directory.resolve("heartbeat").expect("channel present");
        let slugs: Vec<&str> = entry
            .subscribers
            .iter()
            .filter_map(|s| match &s.kind {
                SubscriberEntryKind::App(slug) => Some(slug.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(slugs, vec!["pfin"], "only pfin remains in the directory");
    }

    /// Dormant-row unsubscribe: a dormant
    /// durable row (no folded directory subscriber — a boot-merge `revoked` row)
    /// unsubscribes to **success**, not a panic; the durable row is deleted and the
    /// outcome reports `was_dormant = true`.
    #[tokio::test]
    async fn unsubscribe_dormant_row_succeeds_without_panic() {
        let ch = channel("heartbeat", ChannelScheme::Brenn);
        let uuid = ch.uuid;
        let m = messenger(vec![ch], &[("graf", false, &["u"])]).await;
        // Seed a dormant durable row: durable-only, no directory subscriber.
        seed_dynamic_row(&m, uuid, "graf", Depth::Bounded(5)).await;

        let outcome = m
            .unsubscribe_dynamic("graf", "heartbeat")
            .await
            .expect("dormant-row unsubscribe succeeds, no panic");
        assert!(outcome.was_dormant, "removed row reported dormant");
        assert!(
            !outcome.still_subscribed,
            "no other subscriber on the channel"
        );
        assert_eq!(outcome.channel_uuid, uuid);

        // Durable row gone.
        let rows = {
            let conn = m.db.lock().await;
            load_dynamic_subscriptions(&conn)
        };
        assert!(rows.is_empty(), "dormant durable row deleted");
    }

    /// A dormant-row unsubscribe reports `still_subscribed` from the untouched
    /// step-1 directory snapshot: a *static* subscriber on the same channel means
    /// `still_subscribed = true` even though this app's row was never folded.
    #[tokio::test]
    async fn unsubscribe_dormant_row_reports_other_subscribers() {
        let mut ch = channel("heartbeat", ChannelScheme::Brenn);
        let uuid = ch.uuid;
        // A different app's static subscriber occupies the channel.
        ch.subscribers.push(SubscriberEntry {
            kind: SubscriberEntryKind::App("pfin".to_string()),
            push_depth: Depth::Unbounded,
            retain_depth: Depth::Unbounded,
            noise: NoiseLevel::Silent,
            wake_min: Some(WakeMin::Normal),
        });
        let m = messenger(
            vec![ch],
            &[("graf", false, &["u"]), ("pfin", false, &["u"])],
        )
        .await;
        seed_dynamic_row(&m, uuid, "graf", Depth::Bounded(5)).await;

        let outcome = m
            .unsubscribe_dynamic("graf", "heartbeat")
            .await
            .expect("dormant-row unsubscribe succeeds");
        assert!(outcome.was_dormant);
        assert!(
            outcome.still_subscribed,
            "pfin's static subscriber still occupies the channel"
        );
    }

    // -----------------------------------------------------------------------
    // list_subscriptions (MessageSubscriptionList backing, design §2.1 / §2.4)
    // -----------------------------------------------------------------------

    /// Push a STATIC `App(slug)` subscriber (directory entry only, no durable
    /// dynamic row) onto a channel before the directory is built.
    fn with_static_app_sub(mut ch: ChannelEntry, slug: &str) -> ChannelEntry {
        ch.subscribers.push(SubscriberEntry {
            kind: SubscriberEntryKind::App(slug.to_string()),
            push_depth: Depth::Unbounded,
            retain_depth: Depth::Bounded(7),
            noise: NoiseLevel::Metered,
            wake_min: Some(WakeMin::High),
        });
        ch
    }

    /// An app with one static `brenn:` sub and one dynamic `mqtt:` sub gets
    /// exactly those two entries back, with correct `dynamic` flags and the
    /// per-subscriber params from *its own* `SubscriberEntry`.
    #[tokio::test]
    async fn list_subscriptions_reports_static_and_dynamic_with_flags_and_params() {
        // Use a canonical `brenn:`-prefixed address (the production contract:
        // real directory entries carry the prefix; `list_subscriptions` passes
        // `entry.address` through verbatim). Asserting on a bare name would let a
        // prefix-stripping regression slip through.
        let brenn_ch =
            with_static_app_sub(channel("brenn:heartbeat", ChannelScheme::Brenn), "graf");
        let mqtt_ch = channel("mqtt:home:sensors/temp", ChannelScheme::Mqtt);
        let m = messenger(vec![brenn_ch, mqtt_ch], &[("graf", false, &["u"])]).await;

        // Create the dynamic mqtt: subscription at runtime.
        m.subscribe_dynamic("graf", "mqtt:home:sensors/temp", pull_only(Some(1)))
            .await
            .expect("mqtt subscribe succeeds");

        let subs = m.list_subscriptions("graf").await;
        assert_eq!(subs.len(), 2, "exactly the two subs graf holds: {subs:?}");

        let brenn = subs
            .iter()
            .find(|s| s.address == "brenn:heartbeat")
            .expect("static brenn sub present");
        assert_eq!(brenn.protocol, ChannelScheme::Brenn);
        assert!(
            !brenn.dynamic,
            "config-folded sub is static (dynamic=false)"
        );
        // Per-subscriber params come from graf's own SubscriberEntry, not the
        // channel-wide resolved_channel.
        assert_eq!(brenn.push_depth, Depth::Unbounded);
        assert_eq!(brenn.retain_depth, Depth::Bounded(7));
        assert_eq!(brenn.noise, NoiseLevel::Metered);
        assert_eq!(brenn.wake_min, WakeMin::High);

        let mqtt = subs
            .iter()
            .find(|s| s.address == "mqtt:home:sensors/temp")
            .expect("dynamic mqtt sub present");
        assert_eq!(mqtt.protocol, ChannelScheme::Mqtt);
        assert!(
            mqtt.dynamic,
            "subscribe_dynamic-created sub is dynamic=true"
        );
        // mqtt: details carry client/topic; runtime-health fields stay None
        // (filled by the intercept enrichment).
        let ChannelDetails::Mqtt(details) = mqtt.details.as_ref().expect("mqtt details present")
        else {
            panic!("expected MqttDetails, got {:?}", mqtt.details);
        };
        assert_eq!(details.client, "home");
        assert_eq!(details.topic, "sensors/temp");
        assert!(details.qos.is_none());
        assert!(details.health.is_none());
    }

    /// A static `webhook:` subscription is reported through the distinct
    /// `ChannelScheme::Webhook` arm: correct `Webhook` protocol tag, verbatim
    /// address, `WebhookDetails { mount }`, and `dynamic = false` (test-2). The
    /// other `list_subscriptions` tests exercise only brenn:/mqtt:, so this guards
    /// the webhook arm against a wrong protocol tag or dropped/None details.
    #[tokio::test]
    async fn list_subscriptions_reports_webhook_subscription() {
        let mut webhook_ch = channel("webhook:inbound", ChannelScheme::Webhook);
        webhook_ch.mount = Some("/hooks/inbound".to_string());
        let webhook_ch = with_static_app_sub(webhook_ch, "graf");
        let m = messenger(vec![webhook_ch], &[("graf", false, &["u"])]).await;

        let subs = m.list_subscriptions("graf").await;
        assert_eq!(subs.len(), 1, "exactly graf's webhook sub: {subs:?}");
        let wh = &subs[0];
        assert_eq!(wh.protocol, ChannelScheme::Webhook);
        assert_eq!(wh.address, "webhook:inbound");
        assert!(!wh.dynamic, "config-folded webhook sub is static");
        let ChannelDetails::Webhook(details) =
            wh.details.as_ref().expect("webhook details present")
        else {
            panic!("expected WebhookDetails, got {:?}", wh.details);
        };
        assert_eq!(details.mount, "/hooks/inbound");
    }

    /// On a shared channel, each app's listing shows only its own subscriber.
    #[tokio::test]
    async fn list_subscriptions_shared_channel_is_per_app() {
        let mut ch = channel("heartbeat", ChannelScheme::Brenn);
        ch.subscribers.push(SubscriberEntry {
            kind: SubscriberEntryKind::App("graf".to_string()),
            push_depth: Depth::Bounded(3),
            retain_depth: Depth::Bounded(3),
            noise: NoiseLevel::Silent,
            wake_min: Some(WakeMin::Normal),
        });
        ch.subscribers.push(SubscriberEntry {
            kind: SubscriberEntryKind::App("pfin".to_string()),
            push_depth: Depth::Bounded(9),
            retain_depth: Depth::Bounded(9),
            noise: NoiseLevel::Alarm,
            wake_min: Some(WakeMin::Low),
        });
        let m = messenger(
            vec![ch],
            &[("graf", false, &["u"]), ("pfin", false, &["u"])],
        )
        .await;

        let graf = m.list_subscriptions("graf").await;
        assert_eq!(graf.len(), 1);
        assert_eq!(graf[0].push_depth, Depth::Bounded(3), "graf's own params");
        assert_eq!(graf[0].noise, NoiseLevel::Silent);

        let pfin = m.list_subscriptions("pfin").await;
        assert_eq!(pfin.len(), 1);
        assert_eq!(pfin[0].push_depth, Depth::Bounded(9), "pfin's own params");
        assert_eq!(pfin[0].noise, NoiseLevel::Alarm);
    }

    /// An app with no subscriptions gets an empty listing.
    #[tokio::test]
    async fn list_subscriptions_empty_for_unsubscribed_app() {
        let ch = with_static_app_sub(channel("heartbeat", ChannelScheme::Brenn), "graf");
        let m = messenger(
            vec![ch],
            &[("graf", false, &["u"]), ("pfin", false, &["u"])],
        )
        .await;

        let pfin = m.list_subscriptions("pfin").await;
        assert!(pfin.is_empty(), "pfin holds no subscriptions: {pfin:?}");
    }

    /// A `Wasm(slug)` subscriber on a shared channel is excluded from an `App`
    /// listing (it is a different subscriber).
    #[tokio::test]
    async fn list_subscriptions_excludes_wasm_subscribers() {
        let mut ch = with_static_app_sub(channel("heartbeat", ChannelScheme::Brenn), "graf");
        // A WASM consumer on the same channel — must not appear in graf's listing.
        ch.subscribers.push(SubscriberEntry {
            kind: SubscriberEntryKind::Wasm("worker".to_string()),
            push_depth: Depth::Unbounded,
            retain_depth: Depth::Unbounded,
            noise: NoiseLevel::Silent,
            wake_min: None,
        });
        let m = messenger(vec![ch], &[("graf", false, &["u"])]).await;

        let graf = m.list_subscriptions("graf").await;
        assert_eq!(graf.len(), 1, "only graf's own App subscription: {graf:?}");
        assert_eq!(graf[0].address, "heartbeat");
        // `MessageSubscriptionList` rows describe *this app's own* subscription, not
        // the channel-wide roster (quality-1 / security-1): the BrennDetails
        // subscribers list carries only the calling app's slug, never the co-
        // subscribed Wasm consumer (nor any other app).
        let ChannelDetails::Brenn(details) = graf[0].details.as_ref().expect("brenn details")
        else {
            panic!("expected BrennDetails");
        };
        assert_eq!(details.subscribers, vec!["graf".to_string()]);
        assert!(!details.subscribers.contains(&"worker".to_string()));
    }
}
