//! Runtime dynamic-subscribe core.
//!
//! This is the transport-agnostic body of "create a dynamic subscription": it
//! resolves the subscription's parameters (shared resolver, [`config::resolve_subscription_params`]),
//! writes the registration, and folds the new subscriber into the in-memory
//! directory (copy-on-write [`MessagingDirectory::add_subscriber`]).
//!
//! Registration persistence follows channel durability: a durable channel keeps
//! a `messaging_dynamic_subscriptions` row
//! ([`db::insert_dynamic_subscription`]); a non-durable one keeps an in-memory
//! record that dies with the ring it names, so a subscription never outlives the
//! data it subscribes to.
//!
//! Scope of *this* layer: for `brenn:`/`webhook:` the channel must already
//! exist in the directory (never auto-create a channel nobody publishes to).
//! For `mqtt:`, a not-yet-existing topic-filter channel is **created** here —
//! validate the filter, derive the canonical address + UUID, upsert
//! `messaging_channels`, and `add_channel` into the directory. The
//! transport-specific *broker activation* (the live MQTT SUBSCRIBE, the
//! configured-client check, and the router `IngressRoute` add) is **not** done
//! here — it is layered on top by the per-transport activation increment and
//! the `MessageSubscribe` tool. The `qos` parameter is validated and persisted
//! here (so the later activation step has it), but no broker call is made.
//!
//! Every failure path returns an error (never panics): a misconfigured dynamic
//! subscribe is LLM/attacker-shaped tool input, not a host bug (CLAUDE.md
//! "panic on host bug, error on bad input"). The boot path keeps its
//! fail-fast `.expect()` on the shared resolver; only this runtime path maps the
//! resolver's `Err` to a tool-facing error.

use super::config::{
    self, RawSubscriptionParams, ResolvedSubscription, SubscribeError, SubscriptionParamDefaults,
    resolve_system_channel,
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
    /// A resolved depth of a dynamic subscribe exceeds the channel's
    /// `standing_retain_depth`, which is the ceiling on every depth stated about
    /// a channel: it is what the reaper keeps, so a deeper subscriber would
    /// either read history the operator's baseline does not cover or force the
    /// effective retention above what the channel block says. Rejected (not
    /// clamped — silent narrowing is banned; the caller must know the depth it
    /// actually got). Only strictly-greater is rejected; equality is allowed,
    /// and an `Unbounded` standing caps nothing.
    DepthExceedsStanding {
        address: String,
        /// Which depth was over the ceiling: `"push_depth"` or `"retain_depth"`.
        field: &'static str,
        requested: Depth,
        standing: Depth,
    },
    /// A dormant durable dynamic row exists for this `(channel, app)`: a row that
    /// boot-merge classified `revoked` (ACL no longer authorizes delivery, or one
    /// of its depths exceeds the channel's current standing depth), so it is
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
            RuntimeSubscribeError::DepthExceedsStanding {
                address,
                field,
                requested,
                standing,
            } => write!(
                f,
                "requested {field} {requested:?} for {address:?} exceeds the channel's \
                 standing retain depth {standing:?}; re-request with {field} <= {standing:?}"
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
    /// A new dynamic subscription was created (durable row written, channel
    /// created if absent, subscriber folded into the directory).
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
    /// Create a dynamic subscription for `app_slug` on an **existing** channel.
    ///
    /// Steps: validate `qos` placement → resolve params (shared resolver) →
    /// reject an already-present dynamic sub for this app → persist the durable
    /// row → fold the subscriber into the directory.
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
    /// not-yet-existing `mqtt:` topic filter.
    ///
    /// - **Existing channel** → returned as-is (any transport).
    /// - **Absent `brenn:`/`webhook:`** → `UnknownChannel` (never auto-created —
    ///   a channel nobody can publish to is meaningless).
    /// - **Absent `mqtt:`** → validate the topic filter, derive the canonical
    ///   address + deterministic UUID, build a `ChannelEntry` resolved through
    ///   [`resolve_system_channel`] (every system-channel creation site calls the
    ///   same function, so a runtime-minted channel and its DB-reconstructed twin
    ///   agree by construction), upsert `messaging_channels`, and `add_channel`
    ///   into the directory. The broker SUBSCRIBE / `IngressRoute` add / live
    ///   configured-client check are the bin-crate activation layer's job, not
    ///   this generic core.
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
        let resolved_channel =
            resolve_system_channel(&canonical, &self.system_channel_tuning, &self.defaults);
        let entry = ChannelEntry {
            uuid,
            address: canonical,
            description: None,
            resolved_channel,
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
        // 0. Serialize against every other dynamic (un)subscribe. Steps 4 and 5
        //    are a classify-then-write pair over the same registration record
        //    with `.await` points between them; two concurrent calls for one
        //    `(channel, app)` would otherwise both classify "fresh subscribe" and
        //    the loser would collide on the write.
        let _gate = self.dynamic_subscribe_gate.lock().await;

        // 1. `qos` is MQTT-only — reject it on a non-MQTT address rather than
        //    silently ignoring a caller mistake. Determine the transport from
        //    the address prefix (not the directory entry, which may not exist
        //    yet for a new `mqtt:` filter).
        let is_mqtt = matches!(ChannelScheme::of(address), Some(ChannelScheme::Mqtt));
        if params.qos.is_some() && !is_mqtt {
            return Err(RuntimeSubscribeError::QosOnNonMqtt {
                address: address.to_string(),
            });
        }

        // 2. Resolve the target channel, creating it for a not-yet-existing
        //    `mqtt:` topic-filter address. `brenn:`/`webhook:` never auto-create:
        //    a channel nobody publishes to is meaningless, so an absent one is a
        //    terminal `UnknownChannel` error.
        let entry = self.resolve_or_create_channel(address, is_mqtt).await?;
        // 2b. Registration persistence follows channel durability: a durable
        //     channel keeps its dynamic subscription as a row, a non-durable one
        //     keeps it in memory, where it expires with the data it names.
        let durable_channel = entry.capabilities().durable;
        let qos = params.qos;

        // 3. Resolve the requested params via the shared resolver.
        //    Done *before* the re-subscribe identity check so an identical /
        //    differing comparison is made against the fully-resolved values
        //    (inheritance applied), not the raw request — matching how the
        //    existing subscriber's directory entry already carries resolved
        //    values. One rung for every scheme: the channel's own resolved
        //    config, which for a synthesized `mqtt:` channel is its family
        //    default or the operator's tuning block. The depths never reach it —
        //    a dynamic subscribe states both — so what it actually carries here
        //    is noise and wake_min.
        let rung = SubscriptionParamDefaults::from_channel(&entry.resolved_channel);
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

        // 4. Re-subscribe / existing-subscriber policy + the depth ceiling.
        //    Neither resolved depth may exceed the channel's standing retain
        //    depth: standing is what the reaper keeps, so a deeper subscriber
        //    would read history the operator's baseline does not cover or be owed
        //    pushes over rows the reaper is free to evict. Only strictly-greater
        //    is rejected; equality and an Unbounded standing are fine. Computed on
        //    the resolved values so it stays correct if either becomes inheritable.
        let standing = entry.resolved_channel.standing_retain_depth;
        let cap_exceeded = [
            ("push_depth", resolved.push_depth),
            ("retain_depth", resolved.retain_depth),
        ]
        .into_iter()
        .find(|(_, depth)| *depth > standing);

        // Existing-subscriber / re-subscribe policy + the depth ceiling.
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
        // The registration record for this `(channel, app)`, if any, carrying the
        // stored `qos` the directory entry lacks. Each class answers from its own
        // authority: the durable table, or the in-memory non-durable registration
        // set (which carries no `qos` — `qos` is mqtt-only and mqtt is durable).
        let registration: Option<Option<u8>> = if durable_channel {
            let conn = self.db.lock().await;
            super::db::load_dynamic_subscription_for(&conn, entry.uuid, app_slug).map(|row| row.qos)
        } else {
            self.nondurable_dynamic_sub_exists(&entry.uuid, app_slug)
                .then_some(None)
        };
        // Static and dormant are checked BEFORE the cap: each is the more actionable
        // error (a static holder can never succeed by lowering its depth, so the cap
        // error would be a lie; a dormant holder must unsubscribe first regardless of
        // depth). The dormant reject re-establishes insert_dynamic_subscription's
        // "neither row pre-exists" guarantee for the step-5 INSERT.
        match (existing_entry.is_some(), registration.is_some()) {
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
        if let Some((field, requested)) = cap_exceeded {
            return Err(RuntimeSubscribeError::DepthExceedsStanding {
                address: address.to_string(),
                field,
                requested,
                standing,
            });
        }
        if let (Some(existing), Some(existing_qos)) = (existing_entry, registration) {
            // Live dynamic re-subscribe: identity-only policy.
            let identical = existing.push_depth == resolved.push_depth
                && existing.retain_depth == resolved.retain_depth
                && existing.noise == resolved.noise
                && existing.wake_min == Some(resolved.wake_min)
                && existing_qos == qos;
            if identical {
                return Ok(SubscribeOutcome::AlreadySubscribedIdentical(resolved));
            }
            return Err(RuntimeSubscribeError::AlreadySubscribedDiffers {
                address: address.to_string(),
            });
        }

        // 5. Write the registration, create the delivery position, then fold the
        //    subscriber into the directory — in that order, so nothing is
        //    deliverable here before there is something to deliver it to.
        //    The registration is the truth about "this app subscribed at runtime";
        //    the directory swap makes it visible to the publish hot path. A
        //    durable channel writes the durable dynamic row; a non-durable one
        //    records the in-memory registration, which dies with the ring it
        //    names.
        if durable_channel {
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
            let conn = self.db.lock().await;
            insert_dynamic_subscription(&conn, &row);
        } else {
            self.register_nondurable_dynamic_sub(resolved.channel_uuid, app_slug);
        }
        // A push-enabled app delivers to its conversation, and a conversation
        // reads through a position: create it before the directory starts
        // delivering here. The new cursor is primed behind the retained tail, so
        // a publish landing between the two writes lands inside its primed
        // window and is served. A cursor on a channel the directory does not yet
        // deliver to costs nothing; the reverse loses a message.
        if resolved.push_depth.is_push_enabled() {
            self.attach_conversation(address, app_slug, resolved.push_depth)
                .await;
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
    /// (transport-agnostic core — the inverse of
    /// [`Messenger::subscribe_dynamic`]).
    ///
    /// Steps: drop the registration — for a durable channel the
    /// `messaging_dynamic_subscriptions` row
    /// ([`db::delete_dynamic_subscription`]), for a non-durable channel the
    /// in-memory record; then fold the subscriber out of the in-memory directory
    /// (copy-on-write [`MessagingDirectory::remove_subscriber`]).
    /// The registration drop is the authority on "did a dynamic sub exist": it
    /// returns `false` for a `(channel, app)` holding none — which is both
    /// the not-subscribed *and* the static-only case (a static sub holds no
    /// registration). Those two are then discriminated on the in-memory directory entry
    /// into [`RuntimeUnsubscribeError::StaticSubscription`] vs
    /// [`RuntimeUnsubscribeError::NotSubscribed`].
    ///
    /// This does **not** perform transport activation — the MQTT broker
    /// UNSUBSCRIBE and last-subscriber route/`IngressSubscription` drop are the
    /// per-transport activation increment's job. The
    /// returned [`UnsubscribeOutcome::still_subscribed`] tells that layer whether
    /// any other subscriber remains on the filter (so it knows whether to issue
    /// the broker UNSUBSCRIBE).
    ///
    /// Errors (never panics): no dynamic subscription for this app on the channel.
    /// Removing another app's sub, or a static TOML sub, is structurally
    /// impossible — the drop is keyed on `(channel_uuid, app_slug)` and static
    /// subs hold no registration.
    pub async fn unsubscribe_dynamic(
        &self,
        app_slug: &str,
        address: &str,
    ) -> Result<UnsubscribeOutcome, RuntimeUnsubscribeError> {
        // 0. The same gate `subscribe_dynamic` takes: a removal interleaved with
        //    a subscribe's classify-then-write would let the subscribe write
        //    against a registration state it never observed.
        let _gate = self.dynamic_subscribe_gate.lock().await;

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

        // 2. Drop the registration — the durable dynamic row for a durable
        //    channel, the in-memory record for a non-durable one.
        //    The return value is the authority on whether a dynamic sub existed:
        //    a static-only or not-subscribed `(channel, app)` holds no
        //    registration, so this is `false` and no mutation happened. Static
        //    subs are config-managed and structurally unreachable here (they hold
        //    no registration), so this can never remove one.
        let removed = if entry.capabilities().durable {
            let conn = self.db.lock().await;
            delete_dynamic_subscription(&conn, channel_uuid, app_slug)
        } else {
            self.remove_nondurable_dynamic_sub(&channel_uuid, app_slug)
        };
        if !removed {
            // No registration for `(channel, app)`. Discriminate the two cases
            // purely in-memory on the resolved `entry`: a surviving
            // `App(app_slug)` directory subscriber with no registration is, by the
            // same convention `subscribe_dynamic` uses, a static (config-managed)
            // sub; no such subscriber means the app holds no sub of any kind on
            // this channel.
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

        // 4. Tear down delivery state. The directory still holds the channel,
        //    so `detach_subscriber` resolves.
        //
        // The app's delivery state is its conversation's position, which is what
        // this deletes. It is not serialized against a drain already in flight
        // for the same subscriber: that drain's advance finds no position and
        // no-ops, which is the whole of what a departed subscriber is owed.
        self.detach_conversation(address, app_slug).await;

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
    use crate::messaging::db::{insert_message, load_dynamic_subscriptions, upsert_channels};
    use crate::messaging::test_support::test_app_config;
    use crate::messaging::{
        ChannelDetails, ChannelEntry, ChannelScheme, MessageEnvelope, MessagingDirectory,
        ParticipantId, Urgency, WakeRouter,
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
            _envelope: &std::sync::Arc<MessageEnvelope>,
            _retained_seq: i64,
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
        fn alarm(&self, _channel: &str, _subscriber: &ParticipantId, _count: u64) {}
    }

    /// Whether `subscriber` is owed anything on the channel `uuid` names.
    async fn owed_on(m: &Messenger, uuid: Uuid, subscriber: &ParticipantId) -> bool {
        let entry = m
            .directory()
            .by_uuid(&uuid)
            .expect("the channel is in the directory");
        m.store_for(&entry).has_deliverable(subscriber).await
    }

    fn channel(address: &str, transport: ChannelScheme) -> ChannelEntry {
        ChannelEntry {
            uuid: Uuid::new_v4(),
            address: address.to_string(),
            description: None,
            transport_type: transport,
            resolved_channel: ResolvedChannel {
                send_rate: Default::default(),
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
        let mut apps: IndexMap<String, AppConfig> = IndexMap::new();
        for (slug, singleton, users) in app_specs {
            let mut app =
                test_app_config(slug, None, users.iter().map(|u| u.to_string()).collect());
            app.singleton = *singleton;
            apps.insert(slug.to_string(), app);
        }
        messenger_with_apps(entries, apps).await
    }

    /// The same build over a caller-supplied apps map, for a case whose app needs
    /// a policy `test_app_config` does not stamp.
    async fn messenger_with_apps(
        entries: Vec<ChannelEntry>,
        apps: IndexMap<String, AppConfig>,
    ) -> Arc<Messenger> {
        let db = init_db_memory();
        // Boot's split: durable channels get a DB row, non-durable ones get an
        // in-memory ring store. Both halves live in the one directory.
        let (nondurable, durable): (Vec<ChannelEntry>, Vec<ChannelEntry>) = entries
            .iter()
            .cloned()
            .partition(|e| !e.capabilities().durable);
        {
            let conn = db.lock().await;
            upsert_channels(&conn, &durable);
        }
        let ring_stores = Arc::new(crate::messaging::store::RingStores::build(&nondurable));
        let directory = Arc::new(MessagingDirectory::with_entries(entries));
        Messenger::new(
            db,
            directory,
            Arc::from("test-source"),
            Arc::new(apps),
            Arc::new(NoopRouter) as Arc<dyn WakeRouter>,
            MessagingGlobalConfig::default(),
        )
        .with_ring_stores(ring_stores)
    }

    fn push_enabled() -> DynamicSubscribeParams {
        DynamicSubscribeParams {
            push_depth: Depth::Bounded(5),
            retain_depth: Depth::Bounded(5),
            noise: None,
            wake_min: None,
            qos: None,
        }
    }

    /// Create the user an app's `allowed_users` names, so the app→owner→singleton
    /// conversation resolution a push-enabled subscribe performs can land.
    async fn seed_owner(m: &Messenger, username: &str) {
        let conn = m.db.lock().await;
        crate::auth::user::create_user(&conn, username, "$argon2id$fake");
    }

    /// An app config whose policy covers `ephemeral:` delivery — the gate every
    /// conversation read applies. `test_app_config` grants only the `brenn:`
    /// family, which the delivery-time gate denies on a ring channel.
    fn ephemeral_subscriber_app(slug: &str, users: &[&str]) -> AppConfig {
        let mut app = test_app_config(slug, None, users.iter().map(|u| u.to_string()).collect());
        app.singleton = true;
        app.policy
            .grants
            .insert(crate::access::AppCapability::EphemeralSubscribe);
        app.policy
            .acls
            .ephemeral_subscribe
            .push(crate::access::acl::ChannelMatcher::Prefix(String::new()));
        app
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

    /// A pull-only subscribe to a non-durable channel succeeds: the registration
    /// is held in memory (no durable row) and the subscriber is folded into the
    /// directory, so the app can read the channel's retained window.
    #[tokio::test]
    async fn subscribe_to_a_nondurable_channel_registers_in_memory() {
        let ch = channel("ephemeral:chatter", ChannelScheme::Ephemeral);
        let uuid = ch.uuid;
        let m = messenger(vec![ch], &[("graf", false, &["u"])]).await;

        let outcome = m
            .subscribe_dynamic("graf", "ephemeral:chatter", pull_only(None))
            .await
            .expect("a pull-only subscribe to a non-durable channel succeeds");
        assert!(outcome.is_created());

        let rows = {
            let conn = m.db.lock().await;
            load_dynamic_subscriptions(&conn)
        };
        assert!(
            rows.is_empty(),
            "a non-durable registration writes no durable row"
        );
        assert!(
            m.nondurable_dynamic_sub_exists(&uuid, "graf"),
            "the in-memory registration is recorded"
        );
        let entry = m.directory.by_uuid(&uuid).expect("channel present");
        assert!(
            entry
                .subscribers
                .iter()
                .any(|s| matches!(&s.kind, SubscriberEntryKind::App(slug) if slug == "graf")),
            "subscriber folded into directory"
        );
        // The registration is exactly as removable as a durable one.
        let removed = m
            .unsubscribe_dynamic("graf", "ephemeral:chatter")
            .await
            .expect("unsubscribe removes the in-memory registration");
        assert!(!removed.was_dormant);
        assert!(!removed.still_subscribed);
        assert!(!m.nondurable_dynamic_sub_exists(&uuid, "graf"));
        let entry = m.directory.by_uuid(&uuid).expect("channel present");
        assert!(entry.subscribers.is_empty(), "subscriber folded out");
    }

    /// What the app sees when it asks what it is subscribed to. The listing walk
    /// covers non-durable channels, and `dynamic` comes from the in-memory
    /// registration set — the same authority the subscribe path classifies
    /// against. Sourcing it from the durable table instead would report a
    /// removable subscription as config-managed while `MessageUnsubscribe` still
    /// removed it.
    #[tokio::test]
    async fn list_subscriptions_reports_a_nondurable_dynamic_subscription() {
        let ch = channel("ephemeral:chatter", ChannelScheme::Ephemeral);
        let m = messenger(vec![ch], &[("graf", false, &["u"])]).await;
        m.subscribe_dynamic("graf", "ephemeral:chatter", pull_only(None))
            .await
            .expect("a pull-only subscribe to a non-durable channel succeeds");

        let rows = m.list_subscriptions("graf").await;
        assert_eq!(rows.len(), 1, "exactly the one subscription: {rows:?}");
        assert_eq!(rows[0].protocol, ChannelScheme::Ephemeral);
        assert_eq!(rows[0].address, "ephemeral:chatter");
        assert!(
            rows[0].dynamic,
            "an in-memory registration is a dynamic — removable — subscription"
        );
        assert!(
            rows[0].details.is_none(),
            "a non-durable channel carries no protocol detail shape"
        );

        m.unsubscribe_dynamic("graf", "ephemeral:chatter")
            .await
            .expect("unsubscribe removes the in-memory registration");
        assert!(
            m.list_subscriptions("graf").await.is_empty(),
            "the removed subscription is gone from the listing too"
        );
    }

    /// A second unsubscribe on a non-durable channel reports not-subscribed: the
    /// in-memory registration is the authority on "was there a dynamic sub",
    /// exactly as the durable row is for a durable channel.
    #[tokio::test]
    async fn unsubscribe_nondurable_without_a_registration_is_not_subscribed() {
        let ch = channel("ephemeral:chatter", ChannelScheme::Ephemeral);
        let m = messenger(vec![ch], &[("graf", false, &["u"])]).await;
        let err = m
            .unsubscribe_dynamic("graf", "ephemeral:chatter")
            .await
            .expect_err("nothing to remove");
        assert!(matches!(err, RuntimeUnsubscribeError::NotSubscribed { .. }));
    }

    /// A push-enabled subscribe to a non-durable channel is accepted, and the
    /// conversation it delivers to gets its position on the ring: a ring cursor
    /// is a position like any other, and a window read serves an asleep consumer
    /// without consuming anything, which is the whole of what the durable path
    /// needs too.
    #[tokio::test]
    async fn push_enabled_subscribe_to_a_nondurable_channel_is_accepted() {
        let ch = channel("ephemeral:chatter", ChannelScheme::Ephemeral);
        let uuid = ch.uuid;
        // A singleton app, so the resolver's push-enabled invariants pass.
        let m = messenger(vec![ch], &[("graf", true, &["u"])]).await;
        seed_owner(&m, "u").await;

        m.subscribe_dynamic("graf", "ephemeral:chatter", push_enabled())
            .await
            .expect("push delivery is available on a non-durable channel");

        assert!(m.nondurable_dynamic_sub_exists(&uuid, "graf"));
        let entry = m.directory.by_uuid(&uuid).expect("channel present");
        assert_eq!(entry.subscribers.len(), 1, "the subscriber was folded");
        let conversation = {
            let conn = m.db.lock().await;
            m.targets
                .app_conversation(&conn, "graf", "ephemeral:chatter")
                .expect("the push-enabled subscribe minted the app's conversation")
        };
        assert!(
            m.ring_store_for(&entry)
                .is_attached(&ParticipantId::for_conversation(conversation)),
            "the conversation holds a position on the ring"
        );
    }

    /// The done-condition of the lifted refusal: a message published to the ring
    /// reaches the conversation, and the advance is its ack point exactly as on a
    /// durable channel.
    #[tokio::test]
    async fn a_nondurable_push_subscriber_is_served_from_the_ring() {
        let ch = channel("ephemeral:chatter", ChannelScheme::Ephemeral);
        let uuid = ch.uuid;
        let m = messenger_with_apps(
            vec![ch],
            IndexMap::from([("graf".to_string(), ephemeral_subscriber_app("graf", &["u"]))]),
        )
        .await;
        seed_owner(&m, "u").await;
        m.subscribe_dynamic("graf", "ephemeral:chatter", push_enabled())
            .await
            .expect("subscribe");
        let conversation = {
            let conn = m.db.lock().await;
            m.targets
                .app_conversation(&conn, "graf", "ephemeral:chatter")
                .expect("conversation")
        };
        let entry = m.directory.by_uuid(&uuid).expect("channel present");
        m.store_for(&entry)
            .append(crate::messaging::store::NewMessage {
                source: "node".to_string(),
                sender: "test-sender".to_string(),
                body: "hello".to_string(),
                urgency: Urgency::Normal,
                envelope_type: ChannelScheme::Ephemeral,
                reply_to: None,
                delivery_deadline: None,
                impetus: None,
                publish_ts_ns: crate::messaging::db::utc_to_ns(chrono::Utc::now()),
            })
            .await;

        let delivery = m.conversation_delivery(conversation).await;
        assert_eq!(
            delivery
                .messages
                .iter()
                .map(|e| e.body.as_str())
                .collect::<Vec<_>>(),
            vec!["hello"],
            "the ring window served the conversation its unseen suffix"
        );
        // The read moved nothing: the same suffix is owed until the advance.
        assert_eq!(
            m.conversation_delivery(conversation).await.messages.len(),
            1
        );
        m.advance_conversation(conversation, delivery).await;
        assert!(
            m.conversation_delivery(conversation).await.is_empty(),
            "the advance is the ack point"
        );
    }

    /// Re-subscribe policy is class-blind: identical params on a non-durable
    /// channel are the idempotent no-op, differing params are the error.
    #[tokio::test]
    async fn resubscribe_nondurable_follows_the_identity_policy() {
        let ch = channel("ephemeral:chatter", ChannelScheme::Ephemeral);
        let m = messenger(vec![ch], &[("graf", false, &["u"])]).await;
        m.subscribe_dynamic("graf", "ephemeral:chatter", pull_only(None))
            .await
            .expect("first subscribe succeeds");

        let outcome = m
            .subscribe_dynamic("graf", "ephemeral:chatter", pull_only(None))
            .await
            .expect("identical re-subscribe is a no-op success");
        assert!(matches!(
            outcome,
            SubscribeOutcome::AlreadySubscribedIdentical(_)
        ));

        let differing = DynamicSubscribeParams {
            push_depth: Depth::Bounded(0),
            retain_depth: Depth::Bounded(7),
            noise: None,
            wake_min: None,
            qos: None,
        };
        let err = m
            .subscribe_dynamic("graf", "ephemeral:chatter", differing)
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            RuntimeSubscribeError::AlreadySubscribedDiffers { .. }
        ));
    }

    /// A pull-only subscribe to an existing `brenn:` channel: resolves params,
    /// persists the durable row, and adds the subscriber to the directory.
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
    /// `DepthExceedsStanding` and persists nothing; equal or lesser depths
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
            RuntimeSubscribeError::DepthExceedsStanding { .. }
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
            RuntimeSubscribeError::DepthExceedsStanding { .. }
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

    /// The ceiling covers `push_depth` too: a dynamic subscribe asking to be
    /// woken over more rows than the reaper keeps is refused with the field
    /// named, and persists nothing. A single-user singleton app is required
    /// because the request is push-enabled.
    #[tokio::test]
    async fn subscribe_over_standing_push_depth_rejected() {
        let m = messenger(
            vec![channel_with_standing(
                "heartbeat",
                ChannelScheme::Brenn,
                Depth::Bounded(4),
            )],
            &[("graf", true, &["u"])],
        )
        .await;
        let err = m
            .subscribe_dynamic(
                "graf",
                "heartbeat",
                DynamicSubscribeParams {
                    push_depth: Depth::Bounded(5),
                    retain_depth: Depth::Bounded(4),
                    noise: None,
                    wake_min: None,
                    qos: None,
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            RuntimeSubscribeError::DepthExceedsStanding {
                field: "push_depth",
                ..
            }
        ));
        assert!(
            err.to_string().contains("push_depth"),
            "message names the offending field: {err}"
        );
        assert_nothing_persisted(&m, "heartbeat", "graf").await;

        // Push depth exactly at the ceiling is fine.
        let m = messenger(
            vec![channel_with_standing(
                "heartbeat",
                ChannelScheme::Brenn,
                Depth::Bounded(4),
            )],
            &[("graf", true, &["u"])],
        )
        .await;
        let outcome = m
            .subscribe_dynamic(
                "graf",
                "heartbeat",
                DynamicSubscribeParams {
                    push_depth: Depth::Bounded(4),
                    retain_depth: Depth::Bounded(4),
                    noise: None,
                    wake_min: None,
                    qos: None,
                },
            )
            .await
            .expect("push == standing is allowed");
        assert!(outcome.is_created(), "equal depth creates the sub");
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

    /// An auto-created `mqtt:` channel resolves through the same
    /// `resolve_system_channel` boot uses, so with no tuning block it takes the
    /// ingress family's bounded default window. A subscriber retain depth inside
    /// that window is admitted. The channel row is a step-2 side effect
    /// (`resolve_or_create_channel`), and here the subscribe goes on to succeed,
    /// so both the channel and its subscriber are present.
    #[tokio::test]
    async fn subscribe_mqtt_auto_created_channel_takes_the_ingress_family_default() {
        let global = MessagingGlobalConfig::default();
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
        let outcome = m
            .subscribe_dynamic("graf", address, pull_only_retain(Depth::Bounded(5)))
            .await
            .expect("a retain of 5 is inside the ingress family's default window");
        assert!(outcome.is_created());
        let entry = m
            .directory
            .resolve(address)
            .expect("auto-created mqtt channel is a step-2 side effect of the subscribe");
        assert_eq!(
            entry.resolved_channel.standing_retain_depth,
            crate::messaging::config::INGRESS_DEFAULT_RETAIN_DEPTH,
            "the synthesized ingress channel takes its family's bounded window",
        );
        assert_eq!(entry.subscribers.len(), 1);
    }

    /// And with a tuning block installed, the runtime-minted channel takes the
    /// operator's numbers — the same ones `resolve_system_channel` answers for
    /// that address, which is what makes the runtime-minted channel and its
    /// DB-reconstructed twin agree by construction. A synthesis path that read
    /// the default table instead would produce exactly the divergence: minted at
    /// the family default, reconstructed after restart at the tuned depths, with
    /// the reaper frontier moving under it.
    #[tokio::test]
    async fn subscribe_mqtt_auto_created_channel_takes_the_operators_tuning() {
        use crate::messaging::config::{ChannelConfigRaw, build_system_channel_tuning};

        let global = MessagingGlobalConfig::default();
        let tuning = build_system_channel_tuning(
            &[ChannelConfigRaw {
                send_rate: None,
                uuid: None,
                address: None,
                address_prefix: Some("mqtt:home:".to_string()),
                description: None,
                push_depth: Some(Depth::Bounded(2)),
                retain_depth: Some(Depth::Bounded(37)),
                standing_retain_depth: Some(Depth::Bounded(37)),
                noise: None,
                sink: None,
                wake_min: None,
            }],
            &global,
        );
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
            global.clone(),
        )
        .with_system_channel_tuning(tuning.clone());

        let address = "mqtt:home:sensors/temp";
        m.subscribe_dynamic("graf", address, pull_only_retain(Depth::Bounded(5)))
            .await
            .expect("a retain of 5 is inside the tuned window");
        let entry = m
            .directory
            .resolve(address)
            .expect("auto-created mqtt channel is a step-2 side effect of the subscribe");
        let expected = crate::messaging::config::resolve_system_channel(address, &tuning, &global);
        assert_eq!(entry.resolved_channel.push_depth, expected.push_depth);
        assert_eq!(entry.resolved_channel.retain_depth, Depth::Bounded(37));
        assert_eq!(
            entry.resolved_channel.standing_retain_depth,
            Depth::Bounded(37),
            "the tuning table installed on the messenger is what the synthesis reads",
        );
    }

    /// Cap-before-identity: an over-standing dynamic sub seeded
    /// directly into the directory + durable table (an unsupported state no live
    /// path produces) re-subscribed with identical params yields
    /// `DepthExceedsStanding`, never `AlreadySubscribedIdentical`.
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
            matches!(err, RuntimeSubscribeError::DepthExceedsStanding { .. }),
            "cap wins over identity: {err:?}"
        );
    }

    /// Error precedence: an app holding a *static* sub that
    /// requests an over-standing dynamic sub gets `StaticSubscriptionExists`, not
    /// `DepthExceedsStanding` — the static holder can never succeed by
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

    /// The LLM late-joiner path: subscribing with `push_depth = N` to a channel
    /// that already holds retained messages is owed the newest N of them as
    /// unseen, straight away. Attach is a delivery point, and what was published
    /// before the subscription existed is unseen to it however old it is.
    #[tokio::test]
    async fn a_dynamic_subscribe_is_owed_the_retained_tail_at_its_push_depth() {
        let ch = channel("heartbeat", ChannelScheme::Brenn);
        let uuid = ch.uuid;
        let m = messenger(vec![ch], &[("graf", true, &["u"])]).await;
        let conversation = {
            let conn = m.db.lock().await;
            let user = crate::auth::user::create_user(&conn, "u", "$argon2id$fake");
            crate::conversation::get_or_create_singleton_conversation(&conn, user, "graf").id
        };

        // Three messages land while nobody is subscribed.
        for body in ["one", "two", "three"] {
            let conn = m.db.lock().await;
            insert_message(
                &conn,
                uuid,
                "src",
                "someone",
                body,
                Urgency::Normal,
                ChannelScheme::Brenn,
                None,
                None,
                None,
                None,
                0,
            );
        }

        m.subscribe_dynamic(
            "graf",
            "heartbeat",
            DynamicSubscribeParams {
                push_depth: Depth::Bounded(2),
                ..push_enabled()
            },
        )
        .await
        .expect("subscribe succeeds");

        let subscriber = ParticipantId::for_conversation(conversation);
        {
            let conn = m.db.lock().await;
            let row = crate::messaging::db::load_subscriber_cursor(&conn, uuid, &subscriber)
                .expect("subscribe created the conversation's position");
            assert_eq!(
                row.next_owed_seq, 2,
                "primed behind the newest two of the three retained messages"
            );
        }
        assert!(
            owed_on(&m, uuid, &subscriber).await,
            "the late joiner is owed the tail it primed over"
        );
    }

    // --- unsubscribe_dynamic (transport-agnostic core) ---------

    /// Unsubscribe removes the app's dynamic sub: the durable row is deleted
    /// and the directory subscriber is folded out. With no other
    /// subscriber on the channel, `still_subscribed` is `false`.
    #[tokio::test]
    async fn unsubscribe_removes_own_dynamic_sub() {
        let ch = channel("heartbeat", ChannelScheme::Brenn);
        let uuid = ch.uuid;
        let m = messenger(vec![ch], &[("graf", true, &["u"])]).await;
        let conversation = {
            let conn = m.db.lock().await;
            let user = crate::auth::user::create_user(&conn, "u", "$argon2id$fake");
            crate::conversation::get_or_create_singleton_conversation(&conn, user, "graf").id
        };
        m.subscribe_dynamic("graf", "heartbeat", push_enabled())
            .await
            .expect("subscribe succeeds");

        // A push-enabled subscribe attaches the app's conversation: the position
        // must exist before the first publish this subscription is meant to catch.
        let subscriber = ParticipantId::for_conversation(conversation);
        {
            let conn = m.db.lock().await;
            assert!(
                crate::messaging::db::load_subscriber_cursor(&conn, uuid, &subscriber).is_some(),
                "subscribe created the conversation's position"
            );
        }

        // Publish something the position now trails, so the channel reports the
        // subscriber as owed work before the unsubscribe tears it down.
        {
            let conn = m.db.lock().await;
            insert_message(
                &conn,
                uuid,
                "src",
                "someone",
                "owed",
                Urgency::Normal,
                ChannelScheme::Brenn,
                None,
                None,
                None,
                None,
                0,
            );
        }
        assert!(
            owed_on(&m, uuid, &subscriber).await,
            "subscriber is owed the published message before unsubscribe"
        );

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

        // Delivery state torn down: the conversation's position is gone, so the
        // channel owes it nothing.
        {
            let conn = m.db.lock().await;
            assert!(
                crate::messaging::db::load_subscriber_cursor(&conn, uuid, &subscriber).is_none(),
                "unsubscribe deleted the conversation's position"
            );
        }
        assert!(
            !owed_on(&m, uuid, &subscriber).await,
            "a subscriber with no position is owed nothing"
        );

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

    /// The interleaving the teardown is not serialized against: a drain reads
    /// its windows, a full `unsubscribe_dynamic` runs before the ack, and the
    /// advance then finds no position. Both operations are legal and neither
    /// waits for the other, so the pair completes — the departed subscriber is
    /// owed nothing and charged nothing.
    #[tokio::test]
    async fn unsubscribe_between_a_drains_read_and_its_advance_completes() {
        // The delivery gate classifies by scheme, so this case needs the
        // canonical address a real channel carries.
        let ch = channel(
            &crate::messaging::canonical_address("heartbeat"),
            ChannelScheme::Brenn,
        );
        let (uuid, address) = (ch.uuid, ch.address.clone());
        // The delivery-time ACL gate reads the app's policy, so the app needs
        // one that covers the channel or the drain serves nothing to race with.
        let mut app = test_app_config("graf", None, vec!["u".to_string()]);
        app.singleton = true;
        app.policy = crate::messaging::test_support::brenn_delivery_policy(
            crate::access::acl::ChannelMatcher::Prefix(String::new()),
        );
        let mut apps: IndexMap<String, AppConfig> = IndexMap::new();
        apps.insert("graf".to_string(), app);
        let m = messenger_with_apps(vec![ch], apps).await;
        let conversation = {
            let conn = m.db.lock().await;
            let user = crate::auth::user::create_user(&conn, "u", "$argon2id$fake");
            crate::conversation::get_or_create_singleton_conversation(&conn, user, "graf").id
        };
        let subscriber = ParticipantId::for_conversation(conversation);
        m.subscribe_dynamic(
            "graf",
            &address,
            DynamicSubscribeParams {
                push_depth: Depth::Bounded(1),
                noise: Some(NoiseLevel::Metered),
                ..push_enabled()
            },
        )
        .await
        .expect("subscribe succeeds");

        // Two messages against a depth-1 window, so the span the read produces
        // carries a drop the advance would otherwise charge.
        for body in ["one", "two"] {
            let conn = m.db.lock().await;
            insert_message(
                &conn,
                uuid,
                "src",
                "someone",
                body,
                Urgency::Normal,
                ChannelScheme::Brenn,
                None,
                None,
                None,
                None,
                0,
            );
        }

        let delivery = m.conversation_delivery(conversation).await;
        assert!(
            !delivery.is_empty(),
            "the read served a batch, so it carries the span that acks it"
        );

        m.unsubscribe_dynamic("graf", &address)
            .await
            .expect("unsubscribe succeeds");

        m.advance_conversation(conversation, delivery).await;

        // The one wrong outcome: an advance that resurrects the row it found
        // missing. A "nothing owed" question cannot see that — a fresh cursor at
        // `through + 1` is owed nothing either — so ask for the position itself.
        assert!(
            m.store_for_address(&address)
                .window(&subscriber, Depth::Unbounded, Depth::Bounded(0))
                .await
                .is_none(),
            "the refused advance minted no position"
        );
        assert_eq!(
            m.drop_counter(&address, &subscriber),
            0,
            "a refused advance charges the departed subscriber nothing"
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
        assert_eq!(brenn.push_depth, Some(Depth::Unbounded));
        assert_eq!(brenn.retain_depth, Some(Depth::Bounded(7)));
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
        assert_eq!(
            graf[0].push_depth,
            Some(Depth::Bounded(3)),
            "graf's own params"
        );
        assert_eq!(graf[0].noise, NoiseLevel::Silent);

        let pfin = m.list_subscriptions("pfin").await;
        assert_eq!(pfin.len(), 1);
        assert_eq!(
            pfin[0].push_depth,
            Some(Depth::Bounded(9)),
            "pfin's own params"
        );
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
