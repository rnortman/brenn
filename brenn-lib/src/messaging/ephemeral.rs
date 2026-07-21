//! `EphemeralBus` — the in-memory, best-effort, non-persistent bus for
//! `ephemeral:` channels.
//!
//! Unlike the durable publish pipeline (`publish/mod.rs`), ephemeral messages
//! have no DB row: each channel carries a bounded retained ring plus a
//! `tokio::sync::broadcast` fan-out to currently-attached subscribers. Loss is
//! detectable via `(epoch, seq)` — a per-boot `epoch` and a dense per-channel
//! `seq`. Delivery is best-effort: a publish with no attached subscribers still
//! assigns a seq and retains in the ring, so a later fresh subscribe sees it.
//!
//! This module holds construction, the publish pipeline, and the
//! subscribe/attach/detach API (retained-ring replay + live broadcast fan-out).
//! The two pipelines share only the building-block gate helpers in `gates.rs`;
//! orchestration is separate.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use chrono::{DateTime, Utc};
use tokio::sync::broadcast;
use tracing::{debug, warn};
use uuid::Uuid;

use brenn_envelope::{ChannelScheme, MessageEnvelope, Urgency};

use crate::access::AppPolicy;
use crate::messaging::config::EphemeralChannelEntry;
use crate::messaging::gates::{check_body_size, publish_acl_allows, well_formed_name};
use crate::messaging::{EPHEMERAL_ADDRESS_PREFIX, ParticipantId};
use crate::obs::security::DenialKind;
use crate::token_bucket::{TokenBucket, TokenBucketOutcome};

// ---------------------------------------------------------------------------
// Sanity caps (memory) and rate-gate tuning
// ---------------------------------------------------------------------------

/// Maximum per-channel retained-ring depth accepted at construction.
///
/// A sanity cap against absurd config, not a memory budget: the config layer
/// (`build_ephemeral_channel_entries`) already rejects unbounded retention, but
/// a `Bounded(u64::MAX)` would still pass there. `EphemeralBus::new` owns the
/// allocation, so it bounds the depth. Worst-case live retention at this cap is
/// `depth × max_body_bytes` per channel — visible and reached only by
/// deliberately extreme config.
pub const EPHEMERAL_MAX_RETAIN_DEPTH: u64 = 4_096;

/// Maximum per-channel broadcast-ring capacity accepted at construction.
///
/// `tokio::sync::broadcast::channel` pre-allocates its ring, so an absurd
/// capacity (e.g. `u32::MAX`) would OOM at channel construction. This caps it:
/// preallocation at the cap is ≈ `capacity × size_of::<slot>` ≈ a few MiB per
/// channel.
pub const EPHEMERAL_MAX_CAPACITY: u32 = 65_536;

/// Per-sender rate-gate burst capacity (tokens available before refill matters).
pub const EPHEMERAL_SENDER_BURST: u32 = 240;

/// Per-sender rate-gate refill interval.
pub const EPHEMERAL_SENDER_REFILL_INTERVAL: Duration = Duration::from_secs(1);

/// Per-sender rate-gate sustained refill: 30 tokens (messages) per interval.
pub const EPHEMERAL_SENDER_REFILL_AMOUNT: u32 = 30;

// ---------------------------------------------------------------------------
// Core types
// ---------------------------------------------------------------------------

/// A single ephemeral message plus its per-channel sequence number. Shared by
/// the retained ring and the broadcast fan-out as one `Arc` — one allocation
/// per message regardless of subscriber count.
#[derive(Debug, Clone)]
pub struct EphemeralDelivery {
    /// Per-channel sequence number, dense and starting at 1. Travels alongside
    /// the envelope (never inside it).
    pub seq: u64,
    /// The message envelope (`envelope_type: Ephemeral`, `channel:
    /// "ephemeral:<name>"`).
    pub envelope: MessageEnvelope,
}

/// Per-channel mutable state.
///
/// Invariant: seq assignment, ring append, and broadcast send happen atomically
/// under this lock; subscriber attach takes the same lock around
/// `tx.subscribe()` + ring snapshot. So for any attach, every message is either
/// entirely before the snapshot (replay-eligible) or entirely after (delivered
/// via the receiver) — no loss, no duplication at the attach boundary.
struct ChannelState {
    /// Next seq to assign; starts at 1, so assigned seqs are `1..`.
    next_seq: u64,
    /// Retained ring, oldest-first, length `≤ retain_depth`.
    ring: VecDeque<Arc<EphemeralDelivery>>,
}

/// One ephemeral channel: its resolved config entry, precomputed address, live
/// state, broadcast sender, and successful-publish counter.
struct EphemeralChannel {
    entry: EphemeralChannelEntry,
    /// `"ephemeral:<name>"`, precomputed at construction.
    address: String,
    state: Mutex<ChannelState>,
    tx: broadcast::Sender<Arc<EphemeralDelivery>>,
    /// Successful-publish count; lock-free increment on the already-resolved
    /// channel (no parallel map to keep in sync).
    publishes: AtomicU64,
}

/// Component-owned observability counters, shared via `Arc`.
struct EphemeralMetrics {
    /// Per-sender rate-limited (denied) count.
    rate_limited: Mutex<HashMap<String, u64>>,
    /// Per-`(channel, participant)` overflow-dropped message count, summed from
    /// broadcast `Lagged` events.
    drops: Mutex<HashMap<(String, String), u64>>,
    /// Per-`(channel, participant)` delivery-time ACL denials. Expected to stay
    /// zero (policies are boot-static); nonzero signals a wiring bug.
    delivery_denied: Mutex<HashMap<(String, String), u64>>,
    /// Per-`(sender, deny_kind)` denied-publish count. The kind is a static
    /// `deny_kind()` tag, and the sender is the config-resolved principal, so
    /// the key set is bounded (never keyed by the attacker-controlled address).
    publish_denied: Mutex<HashMap<(String, String), u64>>,
}

impl EphemeralMetrics {
    /// Increment a `(channel, participant)`-keyed counter by `n`. Shared by the
    /// structurally-identical `drops` and `delivery_denied` maps so the
    /// lock/entry/increment discipline lives in one place.
    fn bump_pair(
        map: &Mutex<HashMap<(String, String), u64>>,
        channel: &str,
        participant: &str,
        n: u64,
    ) {
        *map.lock()
            .expect("ephemeral: metrics lock poisoned")
            .entry((channel.to_owned(), participant.to_owned()))
            .or_insert(0) += n;
    }

    /// Read a `(channel, participant)`-keyed counter (0 if absent).
    fn get_pair(
        map: &Mutex<HashMap<(String, String), u64>>,
        channel: &str,
        participant: &str,
    ) -> u64 {
        *map.lock()
            .expect("ephemeral: metrics lock poisoned")
            .get(&(channel.to_owned(), participant.to_owned()))
            .unwrap_or(&0)
    }
}

/// In-memory best-effort bus for `ephemeral:` channels.
pub struct EphemeralBus {
    /// Per-boot epoch (`Uuid::new_v4()` at construction). A resume across a
    /// different epoch is a guaranteed gap.
    epoch: Uuid,
    /// Instance source, written into each envelope's `source`.
    source: Arc<str>,
    /// Body-size cap (same constant as durable).
    max_body_bytes: usize,
    /// Channels keyed by bare name; immutable after boot.
    channels: HashMap<String, EphemeralChannel>,
    /// Per-sender token buckets, created lazily on first publish. Bounded by the
    /// config-declared sender set, so no eviction is needed.
    sender_buckets: Mutex<HashMap<String, TokenBucket>>,
    metrics: Arc<EphemeralMetrics>,
}

// ---------------------------------------------------------------------------
// Publish
// ---------------------------------------------------------------------------

/// Outcome of an `EphemeralBus::publish`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EphemeralPublishResult {
    /// Published: carries the assigned message id, resolved address, and seq.
    Ok {
        message_id: Uuid,
        address: String,
        seq: u64,
    },
    /// Well-formed `ephemeral:` address that names no declared channel.
    UnknownChannel(String),
    /// Address failed shape validation (missing/other prefix, empty name, or
    /// disallowed characters).
    MalformedAddress(String),
    /// Sender app holds no `EphemeralPublish` grant. Produced by the dispatch
    /// arm (scheme dispatch), never by `EphemeralBus` itself.
    MissingSender,
    /// Sender holds the grant but the target channel is not covered by any
    /// `ephemeral_publish` ACL matcher. Carries the offending address.
    AclDenied(String),
    /// Per-sender rate limit exceeded; no message was published.
    RateLimited,
    /// Body length `> max_body_bytes`.
    BodyTooLarge { len: usize, max: usize },
    /// A durable-only option (`reply_to` / `deliver_after` / `delivery_deadline`)
    /// was supplied with an `ephemeral:` target. Produced by the dispatch arm.
    UnsupportedOption { field: &'static str },
}

impl EphemeralPublishResult {
    /// Kind tag for the denial arms the bus itself produces; doubles as the
    /// `publish_denied` counter key and the security-log `kind` field.
    ///
    /// `Ok`/`RateLimited` return `None` (`RateLimited` has its own
    /// `rate_limited` counter and first-occurrence warn); `MissingSender` and
    /// `UnsupportedOption` also return `None` — neither originates inside the
    /// bus (both are produced by the dispatch arm), so the bus never counts
    /// them.
    pub fn deny_kind(&self) -> Option<DenialKind> {
        match self {
            Self::MalformedAddress(_) => Some(DenialKind::MalformedAddress),
            Self::UnknownChannel(_) => Some(DenialKind::UnknownChannel),
            Self::AclDenied(_) => Some(DenialKind::AclDenied),
            Self::BodyTooLarge { .. } => Some(DenialKind::BodyTooLarge),
            Self::Ok { .. }
            | Self::RateLimited
            | Self::MissingSender
            | Self::UnsupportedOption { .. } => None,
        }
    }

    /// Kind tag for every denial that warrants an intercept-level security
    /// signal — a superset of `deny_kind()` that also covers `MissingSender`.
    /// `MissingSender` is produced only by the dispatch arm, never inside the
    /// bus, so it has no `publish_denied` counter; the security log is its
    /// sole meter and this tag names it there. A caller that signals denials
    /// derives the log `kind` field from this method so it stays in lockstep
    /// with the bus counter key (`deny_kind()`).
    pub fn signal_kind(&self) -> Option<DenialKind> {
        match self {
            Self::MissingSender => Some(DenialKind::MissingSender),
            other => other.deny_kind(),
        }
    }

    /// The echoed target address an address-bearing denial arm carries.
    /// `MissingSender` and `BodyTooLarge` carry none; a caller substitutes the
    /// original publish target.
    pub fn denied_address(&self) -> Option<&str> {
        match self {
            Self::MalformedAddress(addr) | Self::UnknownChannel(addr) | Self::AclDenied(addr) => {
                Some(addr)
            }
            _ => None,
        }
    }
}

impl EphemeralBus {
    /// Construct the bus from resolved config entries. Panics on sanity-cap
    /// violations — boot-time operator config, fail fast.
    pub fn new(
        entries: Vec<EphemeralChannelEntry>,
        source: Arc<str>,
        max_body_bytes: usize,
    ) -> Arc<Self> {
        let mut channels = HashMap::with_capacity(entries.len());

        for entry in entries {
            assert!(
                entry.retain_depth <= EPHEMERAL_MAX_RETAIN_DEPTH,
                "ephemeral channel {:?} retain_depth {} exceeds sanity cap {}",
                entry.name,
                entry.retain_depth,
                EPHEMERAL_MAX_RETAIN_DEPTH,
            );
            assert!(
                entry.capacity <= EPHEMERAL_MAX_CAPACITY,
                "ephemeral channel {:?} capacity {} exceeds sanity cap {}",
                entry.name,
                entry.capacity,
                EPHEMERAL_MAX_CAPACITY,
            );
            // A zero-capacity broadcast ring is rejected by tokio with an opaque
            // internal panic; assert here so the failure names the channel and
            // Brenn's config contract, symmetric with the upper caps.
            assert!(
                entry.capacity != 0,
                "ephemeral channel {:?} capacity must be non-zero",
                entry.name,
            );

            let address = format!("{EPHEMERAL_ADDRESS_PREFIX}{}", entry.name);
            // The initial receiver is dropped: a publish with zero attached
            // subscribers is contract-conformant (best-effort), so `tx.send`
            // erroring on no-receivers is expected and ignored at publish time.
            let (tx, _rx) = broadcast::channel(entry.capacity as usize);
            let name = entry.name.clone();
            let displaced = channels.insert(
                name.clone(),
                EphemeralChannel {
                    entry,
                    address,
                    state: Mutex::new(ChannelState {
                        next_seq: 1,
                        ring: VecDeque::new(),
                    }),
                    tx,
                    publishes: AtomicU64::new(0),
                },
            );
            assert!(
                displaced.is_none(),
                "ephemeral channel {name:?} declared twice",
            );
        }

        Arc::new(Self {
            epoch: Uuid::new_v4(),
            source,
            max_body_bytes,
            channels,
            sender_buckets: Mutex::new(HashMap::new()),
            metrics: Arc::new(EphemeralMetrics {
                rate_limited: Mutex::new(HashMap::new()),
                drops: Mutex::new(HashMap::new()),
                delivery_denied: Mutex::new(HashMap::new()),
                publish_denied: Mutex::new(HashMap::new()),
            }),
        })
    }

    /// The per-boot epoch.
    pub fn epoch(&self) -> Uuid {
        self.epoch
    }

    /// Successful-publish count for a channel (0 for an unknown name).
    pub fn publish_count(&self, name: &str) -> u64 {
        self.channels
            .get(name)
            .map(|c| c.publishes.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    /// Rate-limited (denied) publish count for a sender.
    pub fn rate_limited_count(&self, sender: &str) -> u64 {
        *self
            .metrics
            .rate_limited
            .lock()
            .expect("ephemeral: rate_limited lock poisoned")
            .get(sender)
            .unwrap_or(&0)
    }

    /// Overflow-dropped message count for a `(channel, participant)` pair.
    /// `name` is the bare channel name (matching `publish_count`).
    pub fn drop_count(&self, name: &str, participant: &str) -> u64 {
        EphemeralMetrics::get_pair(&self.metrics.drops, name, participant)
    }

    /// Delivery-time ACL-denial count for a `(channel, participant)` pair.
    pub fn delivery_denied_count(&self, name: &str, participant: &str) -> u64 {
        EphemeralMetrics::get_pair(&self.metrics.delivery_denied, name, participant)
    }

    /// Denied-publish count for a `(sender, kind)` pair, where `kind` is a
    /// `EphemeralPublishResult::deny_kind()` tag.
    pub fn publish_denied_count(&self, sender: &str, kind: &str) -> u64 {
        EphemeralMetrics::get_pair(&self.metrics.publish_denied, sender, kind)
    }

    /// Record a denied publish under `(sender, deny_kind)` and return the
    /// result unchanged. A no-op for non-denial results (`deny_kind() == None`).
    fn record_denial(
        &self,
        sender: &str,
        result: EphemeralPublishResult,
    ) -> EphemeralPublishResult {
        if let Some(kind) = result.deny_kind() {
            EphemeralMetrics::bump_pair(&self.metrics.publish_denied, sender, kind.as_str(), 1);
        }
        result
    }

    /// Publish a message on an `ephemeral:` channel. The sender arrives already
    /// resolved (identity + policy). Synchronous: no DB, no await points.
    ///
    /// Gate order (validate before consuming a rate token): address shape →
    /// channel resolves → publish ACL → body size → per-sender rate limit.
    ///
    /// Caller invariant: `sender` and `policy` MUST both be derived from the
    /// same config-resolved principal, never from client-supplied data. `sender`
    /// becomes the rate-limit key, the envelope `sender` delivered to
    /// subscribers, and the attribution in logs/counters; a caller pairing one
    /// principal's identity with another's policy — or deriving the identity
    /// from client input — defeats rate limiting and forges attribution.
    ///
    /// Denial contract: the four denial arms (`MalformedAddress`,
    /// `UnknownChannel`, `AclDenied`, `BodyTooLarge`) bump the per-`(sender,
    /// kind)` `publish_denied` counter (read via `publish_denied_count`) before
    /// returning; they consume no rate token. The bus does NOT log denials: it
    /// lacks the boundary context to tell an attack from a server bug (the same
    /// arms are "impossible, panic" for the surface caller). Any caller passing
    /// an attacker-influenceable address owns boundary-appropriate security
    /// signaling.
    pub fn publish(
        &self,
        sender: &ParticipantId,
        policy: &AppPolicy,
        addr: &str,
        body: &str,
        urgency: Urgency,
    ) -> EphemeralPublishResult {
        // Gate 1: address shape. Yields the bare channel name.
        let name = match well_formed_name(addr, ChannelScheme::Ephemeral) {
            Some(name) => name,
            None => {
                return self.record_denial(
                    sender.as_str(),
                    EphemeralPublishResult::MalformedAddress(addr.to_string()),
                );
            }
        };

        // Gate 2: channel resolves.
        let channel = match self.channels.get(name) {
            Some(c) => c,
            None => {
                return self.record_denial(
                    sender.as_str(),
                    EphemeralPublishResult::UnknownChannel(addr.to_string()),
                );
            }
        };

        // Gate 3: layer-2 publish ACL (grant is checked inside
        // `allows_ephemeral_publish`; deny-by-default for direct callers).
        if !publish_acl_allows(policy, ChannelScheme::Ephemeral, name) {
            return self.record_denial(
                sender.as_str(),
                EphemeralPublishResult::AclDenied(addr.to_string()),
            );
        }

        // Gate 4: body size (before the rate gate — a rejected publish consumes
        // no rate token, mirroring the durable "validate before budget").
        if let Err(e) = check_body_size(body, self.max_body_bytes) {
            return self.record_denial(
                sender.as_str(),
                EphemeralPublishResult::BodyTooLarge {
                    len: e.len,
                    max: e.max,
                },
            );
        }

        // Gate 5: per-sender rate limit.
        {
            let mut buckets = self
                .sender_buckets
                .lock()
                .expect("ephemeral: sender_buckets lock poisoned");
            // Probe borrowed first: after a sender's first publish the bucket
            // already exists, so the common path avoids allocating an owned key.
            let outcome = match buckets.get_mut(sender.as_str()) {
                Some(bucket) => bucket.try_consume(),
                None => buckets
                    .entry(sender.as_str().to_owned())
                    .or_insert_with(|| {
                        TokenBucket::new(
                            EPHEMERAL_SENDER_BURST,
                            EPHEMERAL_SENDER_REFILL_INTERVAL,
                            EPHEMERAL_SENDER_REFILL_AMOUNT,
                        )
                    })
                    .try_consume(),
            };
            drop(buckets);
            match outcome {
                TokenBucketOutcome::Granted => {}
                TokenBucketOutcome::GrantedAfterSuppression { suppressed } => {
                    warn!(
                        sender = sender.as_str(),
                        suppressed, "ephemeral: sender rate limit lifted"
                    );
                }
                TokenBucketOutcome::Denied { first } => {
                    *self
                        .metrics
                        .rate_limited
                        .lock()
                        .expect("ephemeral: rate_limited lock poisoned")
                        .entry(sender.as_str().to_owned())
                        .or_insert(0) += 1;
                    if first {
                        warn!(sender = sender.as_str(), "ephemeral: rate-limiting sender");
                    }
                    return EphemeralPublishResult::RateLimited;
                }
            }
        }

        let (message_id, seq) = self.append_and_fan_out(channel, sender, body, urgency, Utc::now());

        EphemeralPublishResult::Ok {
            message_id,
            address: channel.address.clone(),
            seq,
        }
    }

    /// Apply one entry of an activation flush that is **already paid for**.
    ///
    /// Runs the validate-only gates and performs seq assignment, ring append,
    /// and broadcast send exactly as [`EphemeralBus::publish`] does, with two
    /// differences:
    ///
    /// - **The per-sender rate gate is never consulted.** This is the backend's
    ///   own tiering, not a bypass: a flush is metered per call at buffer time by
    ///   the host that mints the activation, and its wire rate is metered by the
    ///   caller's one all-or-nothing draw against the per-instance backstop
    ///   before any entry is applied. The wall-clock per-sender bucket meters
    ///   *ad-hoc* sends — the single-`Publish` path still routes through
    ///   `publish` and its gate. Nothing downstream of an admitted flush's buffer
    ///   is permitted to refuse, because refusing would lose entries of a batch
    ///   the client was already answered `Ok`.
    /// - **`publish_ts` is the caller's**, not minted here: the caller stamps the
    ///   whole batch in call order in one pass before splitting it by substrate,
    ///   so call order is visible across the class boundary at ns precision.
    ///
    /// The gates **panic rather than return**: every client-reachable failure was
    /// already answered as a violation by the caller's per-entry resolve against
    /// its boot-resolved output map, so a failure here means the caller's map
    /// disagrees with this bus's channels or the principal's policy — publishing
    /// anyway would route traffic no operator authorized.
    ///
    /// Caller invariant, as for `publish`: `sender` and `policy` MUST both be
    /// derived from the same config-resolved principal, never from client input.
    pub fn publish_prepaid(
        &self,
        sender: &ParticipantId,
        policy: &AppPolicy,
        addr: &str,
        body: &str,
        urgency: Urgency,
        publish_ts: DateTime<Utc>,
    ) {
        let name = well_formed_name(addr, ChannelScheme::Ephemeral).unwrap_or_else(|| {
            panic!(
                "publish_prepaid: bound output {addr:?} is not a well-formed ephemeral: address \
                 — boot resolved it, so this is a broken boot invariant"
            )
        });
        let channel = self.channels.get(name).unwrap_or_else(|| {
            panic!(
                "publish_prepaid: bound output {addr:?} is not a channel of this bus — boot \
                 validation proves every bound output exists, so this is a broken boot invariant"
            )
        });
        assert!(
            publish_acl_allows(policy, ChannelScheme::Ephemeral, name),
            "publish_prepaid: sender {sender:?} has no ephemeral_publish ACL covering bound \
             output {addr:?} — boot validation proves every bound output is policy-covered, so \
             this is a broken boot invariant",
            sender = sender.as_str(),
        );
        if let Err(e) = check_body_size(body, self.max_body_bytes) {
            panic!(
                "publish_prepaid: bound output {addr:?} carries a {len}-byte body over this \
                 bus's {max}-byte cap — the caller rejects an over-cap entry as a violation \
                 before drawing, so the two caps disagree",
                len = e.len,
                max = e.max,
            );
        }

        self.append_and_fan_out(channel, sender, body, urgency, publish_ts);
    }

    /// The effects half of a publish, shared by the metered and prepaid entry
    /// points: mint the envelope, then assign seq, append to the retained ring,
    /// and broadcast — the last three atomically under the channel state lock, so
    /// a concurrent subscribe sees a message either entirely in its replay or
    /// entirely on its receiver.
    ///
    /// Returns `(message_id, seq)`.
    fn append_and_fan_out(
        &self,
        channel: &EphemeralChannel,
        sender: &ParticipantId,
        body: &str,
        urgency: Urgency,
        publish_ts: DateTime<Utc>,
    ) -> (Uuid, u64) {
        // Built outside the channel state lock: it depends on none of the locked
        // state (seq lives in `EphemeralDelivery`, not the envelope), so the
        // critical section stays O(1) rather than scaling with body size.
        let message_id = Uuid::new_v4();
        let envelope = MessageEnvelope {
            message_id,
            source: self.source.as_ref().into(),
            channel: channel.address.clone(),
            sender: sender.as_str().into(),
            publish_ts,
            body: body.to_string(),
            reply_to: None,
            delivery_deadline: None,
            deliver_after: None,
            urgency,
            envelope_type: ChannelScheme::Ephemeral,
        };

        let seq = {
            let mut state = channel
                .state
                .lock()
                .expect("ephemeral: channel state lock poisoned");
            let seq = state.next_seq;
            state.next_seq += 1;

            let delivery = Arc::new(EphemeralDelivery { seq, envelope });

            let retain_depth = channel.entry.retain_depth;
            if retain_depth > 0 {
                state.ring.push_back(delivery.clone());
                if state.ring.len() as u64 > retain_depth {
                    state.ring.pop_front();
                }
            }

            // Best-effort fan-out: `send` errs exactly when no receiver is
            // attached, which for a best-effort bus is contract-conformant. The
            // ring still retains, so a later fresh subscribe sees this message.
            let _ = channel.tx.send(delivery);
            seq
        };

        channel.publishes.fetch_add(1, Ordering::Relaxed);
        debug!(
            channel = %channel.address,
            sender = sender.as_str(),
            seq,
            %message_id,
            "ephemeral publish"
        );

        (message_id, seq)
    }

    /// Attach a subscriber to an `ephemeral:` channel. Returns the retained-ring
    /// replay (computed per the resume rules), the replay decision, and a live
    /// receiver for messages published after the attach.
    ///
    /// The subscriber arrives already resolved (identity + policy). The ACL is
    /// checked once here via `allows_channel_access`; the receiver re-checks it on each
    /// live delivery (belt-and-suspenders symmetry with the durable path).
    ///
    /// Attach atomicity: `tx.subscribe()` and the ring snapshot happen under the
    /// same channel state lock that publish holds, so every message is either
    /// entirely in the replay or entirely on the receiver — no loss, no
    /// duplication at the boundary.
    ///
    /// Denial observability: unlike `publish`, the subscribe denial arms
    /// (`MalformedAddress`/`UnknownChannel`/`AclDenied`) are silent — no counter,
    /// no log. They are currently unreachable with caller-controlled input (the
    /// only production caller, the surface transport, proves the channel bound
    /// before calling and treats an `Err` as impossible). A new caller passing
    /// an attacker-influenceable address must bring its own denial observability,
    /// mirroring `publish`'s contract.
    pub fn subscribe(
        &self,
        subscriber: ParticipantId,
        policy: Arc<AppPolicy>,
        addr: &str,
        resume: Option<EphemeralResume>,
    ) -> Result<EphemeralSubscription, EphemeralSubscribeError> {
        // Validate address shape → bare channel name.
        let name = match well_formed_name(addr, ChannelScheme::Ephemeral) {
            Some(name) => name,
            None => return Err(EphemeralSubscribeError::MalformedAddress(addr.to_string())),
        };

        // Resolve channel.
        let channel = match self.channels.get(name) {
            Some(c) => c,
            None => return Err(EphemeralSubscribeError::UnknownChannel(addr.to_string())),
        };

        // Delivery-time ACL (grant + covering matcher, deny-by-default) against
        // the full stored address, matching the durable delivery gate.
        if !policy.allows_channel_access(&channel.address) {
            return Err(EphemeralSubscribeError::AclDenied(addr.to_string()));
        }

        // Under the state lock: subscribe + snapshot the ring, so the split
        // between replay and live stream is exact.
        let (rx, replay, decision) = {
            let state = channel
                .state
                .lock()
                .expect("ephemeral: channel state lock poisoned");
            let rx = channel.tx.subscribe();
            // Highest assigned seq, or 0 if none (next_seq starts at 1).
            let newest = state.next_seq - 1;
            let (replay, decision) = compute_replay(&state.ring, newest, self.epoch, resume);
            (rx, replay, decision)
        };

        if let Replay::Gap(GapReason::ResumeAhead) = decision {
            // Matching epoch but a seq this epoch never assigned: impossible for
            // an honest client. The bus only warns (fail closed, never panic);
            // the distinguishable reason lets a transport fronting the bus
            // escalate this as a protocol violation.
            warn!(
                channel = %channel.address,
                subscriber = subscriber.as_str(),
                "ephemeral subscribe: resume seq ahead of assigned range"
            );
        }

        debug!(
            channel = %channel.address,
            subscriber = subscriber.as_str(),
            ?decision,
            replay_len = replay.len(),
            "ephemeral attach"
        );

        let receiver = EphemeralReceiver {
            rx,
            subscriber,
            policy,
            address: channel.address.clone(),
            metrics: Arc::clone(&self.metrics),
        };

        Ok(EphemeralSubscription {
            replay,
            decision,
            receiver,
        })
    }
}

/// Compute the replay set and decision for an attach, given the retained ring
/// (oldest-first), the highest assigned seq (`newest`, 0 if none), the bus
/// epoch, and the client's optional resume position.
fn compute_replay(
    ring: &VecDeque<Arc<EphemeralDelivery>>,
    newest: u64,
    epoch: Uuid,
    resume: Option<EphemeralResume>,
) -> (Vec<Arc<EphemeralDelivery>>, Replay) {
    let full_ring = || ring.iter().cloned().collect::<Vec<_>>();

    let Some(resume) = resume else {
        // No resume: full ring, no gap.
        return (full_ring(), Replay::Fresh);
    };

    if resume.epoch != epoch {
        // Different epoch (server restart or another instance): guaranteed gap.
        return (full_ring(), Replay::Gap(GapReason::EpochChanged));
    }

    let last_seen = resume.seq;
    if last_seen > newest {
        // Matching epoch but a never-assigned seq.
        return (full_ring(), Replay::Gap(GapReason::ResumeAhead));
    }
    if last_seen == newest {
        // Caught up: nothing to replay, no gap.
        return (Vec::new(), Replay::UpToDate);
    }

    // last_seen < newest: some messages are owed. The ring covers the hole iff
    // its oldest retained seq is at or before the first owed seq (last_seen + 1).
    match ring.front() {
        Some(oldest) if oldest.seq <= last_seen + 1 => {
            let replay = ring.iter().filter(|d| d.seq > last_seen).cloned().collect();
            (replay, Replay::Exact)
        }
        // Empty ring (incl. retain_depth 0) or a hole older than the ring covers.
        _ => (full_ring(), Replay::Gap(GapReason::HoleExceedsRing)),
    }
}

// ---------------------------------------------------------------------------
// Subscribe / attach / detach
// ---------------------------------------------------------------------------

/// A client's resume position: the epoch and last seq it has already seen on a
/// channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EphemeralResume {
    pub epoch: Uuid,
    pub seq: u64,
}

/// Why an `EphemeralBus::subscribe` was rejected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EphemeralSubscribeError {
    /// Address failed shape validation (missing/other prefix, empty name, or
    /// disallowed characters).
    MalformedAddress(String),
    /// Well-formed `ephemeral:` address that names no declared channel.
    UnknownChannel(String),
    /// Subscriber holds no covering `EphemeralSubscribe` grant + ACL matcher.
    AclDenied(String),
}

/// Why a replay carries a gap (a discontinuity the client must be told about).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GapReason {
    /// Resume epoch differs from the bus epoch — server restarted, or the resume
    /// came from another instance.
    EpochChanged,
    /// Messages between the client's last-seen seq and the ring's oldest are gone.
    HoleExceedsRing,
    /// Client claims a seq this epoch never assigned. Logged `warn!`; a
    /// transport fronting the bus can escalate it as a protocol violation.
    ResumeAhead,
}

/// The replay decision for an attach, alongside the replayed messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Replay {
    /// No resume: the full ring was replayed, no gap.
    Fresh,
    /// Resume matched the newest seq: nothing replayed, no gap.
    UpToDate,
    /// Replay is exactly the messages with `seq > last_seen`, no gap.
    Exact,
    /// A discontinuity: the full ring was replayed and a gap is signaled.
    Gap(GapReason),
}

/// The result of a successful attach: the replay set (oldest-first, seq
/// ascending), the replay decision, and the live receiver.
pub struct EphemeralSubscription {
    pub replay: Vec<Arc<EphemeralDelivery>>,
    pub decision: Replay,
    pub receiver: EphemeralReceiver,
}

/// An event from the live stream after attach.
#[derive(Debug, Clone)]
pub enum EphemeralEvent {
    /// A message delivered on the channel.
    Delivery(Arc<EphemeralDelivery>),
    /// `n` messages were lost to broadcast overflow (this subscriber lagged).
    Dropped(u64),
}

/// The live half of a subscription. Wraps the broadcast receiver plus the
/// context needed for delivery-time ACL re-checks and per-`(channel,
/// participant)` counters. Dropping it detaches the subscriber (broadcast
/// receiver drop is the whole mechanism).
pub struct EphemeralReceiver {
    rx: broadcast::Receiver<Arc<EphemeralDelivery>>,
    subscriber: ParticipantId,
    policy: Arc<AppPolicy>,
    /// Full `"ephemeral:<name>"` address, for the ACL re-check and logs. The
    /// bare channel name (counter key, matching `publish_count`) is derived from
    /// this at the cold deny/lag bump sites — one source of truth, no pair to
    /// keep in sync.
    address: String,
    metrics: Arc<EphemeralMetrics>,
}

impl EphemeralReceiver {
    /// Bare channel name for counter keys, derived from the stored address.
    fn channel_name(&self) -> &str {
        self.address
            .strip_prefix(EPHEMERAL_ADDRESS_PREFIX)
            .expect("ephemeral receiver address is always ephemeral-prefixed")
    }
}

impl EphemeralReceiver {
    /// Block until the next event, or `None` when the bus is dropped (process
    /// shutdown). Delivery-time ACL denials and lagged-message notifications are
    /// handled internally: a denied delivery is skipped (loop continues), a lag
    /// surfaces as `Dropped(n)`.
    pub async fn recv(&mut self) -> Option<EphemeralEvent> {
        loop {
            match self.rx.recv().await {
                Ok(delivery) => {
                    // Belt-and-suspenders re-check, mirroring durable Enforcement
                    // point A: on deny, warn + count + skip, never panic. Policies
                    // are boot-static, so this cannot fire differently than the
                    // attach check — it is symmetry, not revocation.
                    if !self.policy.allows_channel_access(&self.address) {
                        warn!(
                            channel = %self.address,
                            subscriber = self.subscriber.as_str(),
                            "ephemeral delivery denied — ACL not satisfied"
                        );
                        EphemeralMetrics::bump_pair(
                            &self.metrics.delivery_denied,
                            self.channel_name(),
                            self.subscriber.as_str(),
                            1,
                        );
                        continue;
                    }
                    return Some(EphemeralEvent::Delivery(delivery));
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    EphemeralMetrics::bump_pair(
                        &self.metrics.drops,
                        self.channel_name(),
                        self.subscriber.as_str(),
                        n,
                    );
                    warn!(
                        channel = %self.address,
                        subscriber = self.subscriber.as_str(),
                        dropped = n,
                        "ephemeral subscriber lagged — messages dropped"
                    );
                    return Some(EphemeralEvent::Dropped(n));
                }
                Err(broadcast::error::RecvError::Closed) => return None,
            }
        }
    }
}

impl Drop for EphemeralReceiver {
    fn drop(&mut self) {
        debug!(
            channel = %self.address,
            subscriber = self.subscriber.as_str(),
            "ephemeral detach"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::AppCapability;
    use crate::access::acl::ChannelMatcher;
    use crate::messaging::testutils::ephemeral_channel_entry;

    const SOURCE: &str = "test-source";

    fn entry(name: &str, retain_depth: u64, capacity: u32) -> EphemeralChannelEntry {
        ephemeral_channel_entry(name, retain_depth, capacity)
    }

    fn bus_with(entries: Vec<EphemeralChannelEntry>, max_body_bytes: usize) -> Arc<EphemeralBus> {
        EphemeralBus::new(entries, Arc::from(SOURCE), max_body_bytes)
    }

    fn one_channel_bus(max_body_bytes: usize) -> Arc<EphemeralBus> {
        bus_with(vec![entry("protobar", 8, 16)], max_body_bytes)
    }

    fn publisher_policy(channel: &str) -> AppPolicy {
        let mut p = AppPolicy::with_grants(&[AppCapability::EphemeralPublish]);
        p.acls.ephemeral_publish = vec![ChannelMatcher::Exact(channel.to_string())];
        p
    }

    fn pid(slug: &str) -> ParticipantId {
        ParticipantId::for_app(slug, SOURCE)
    }

    #[test]
    fn new_prebuilds_counters_and_epoch() {
        let bus = one_channel_bus(1024);
        assert_ne!(bus.epoch(), Uuid::nil());
        assert_eq!(bus.publish_count("protobar"), 0);
        assert_eq!(bus.publish_count("nonexistent"), 0);
    }

    #[test]
    #[should_panic(expected = "exceeds sanity cap")]
    fn retain_depth_over_sanity_cap_panics() {
        bus_with(vec![entry("x", EPHEMERAL_MAX_RETAIN_DEPTH + 1, 16)], 1024);
    }

    #[test]
    #[should_panic(expected = "exceeds sanity cap")]
    fn capacity_over_sanity_cap_panics() {
        bus_with(vec![entry("x", 8, EPHEMERAL_MAX_CAPACITY + 1)], 1024);
    }

    #[test]
    fn retain_depth_at_sanity_cap_accepted() {
        // The boundary value (`<=` in the assert) must be accepted: construct at
        // the cap and confirm a publish succeeds.
        let bus = bus_with(vec![entry("x", EPHEMERAL_MAX_RETAIN_DEPTH, 16)], 1024);
        let policy = publisher_policy("x");
        let sender = pid("pub");
        assert!(matches!(
            bus.publish(&sender, &policy, "ephemeral:x", "hi", Urgency::Normal),
            EphemeralPublishResult::Ok { .. },
        ));
        assert_eq!(bus.publish_count("x"), 1);
    }

    #[test]
    #[should_panic(expected = "capacity must be non-zero")]
    fn zero_capacity_panics() {
        bus_with(vec![entry("x", 8, 0)], 1024);
    }

    #[test]
    #[should_panic(expected = "declared twice")]
    fn duplicate_channel_name_panics() {
        bus_with(vec![entry("x", 8, 16), entry("x", 4, 32)], 1024);
    }

    #[tokio::test(start_paused = true)]
    async fn publish_ok_assigns_dense_ascending_seq() {
        let bus = one_channel_bus(1024);
        let policy = publisher_policy("protobar");
        let sender = pid("pub");

        let mut seqs = Vec::new();
        for _ in 0..5 {
            match bus.publish(
                &sender,
                &policy,
                "ephemeral:protobar",
                "hi",
                Urgency::Normal,
            ) {
                EphemeralPublishResult::Ok {
                    message_id,
                    seq,
                    address,
                } => {
                    assert_ne!(message_id, Uuid::nil());
                    assert_eq!(address, "ephemeral:protobar");
                    seqs.push(seq);
                }
                other => panic!("expected Ok, got {other:?}"),
            }
        }
        assert_eq!(seqs, vec![1, 2, 3, 4, 5]);
        assert_eq!(bus.publish_count("protobar"), 5);
    }

    #[tokio::test(start_paused = true)]
    async fn malformed_address_rejected() {
        let bus = one_channel_bus(1024);
        let policy = publisher_policy("protobar");
        let sender = pid("pub");
        // Wrong scheme, missing prefix, and empty name all fail shape.
        for addr in ["brenn:protobar", "protobar", "ephemeral:", "ephemeral:a b"] {
            assert_eq!(
                bus.publish(&sender, &policy, addr, "hi", Urgency::Normal),
                EphemeralPublishResult::MalformedAddress(addr.to_string()),
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn unknown_channel_rejected() {
        let bus = one_channel_bus(1024);
        let policy = publisher_policy("other");
        let sender = pid("pub");
        assert_eq!(
            bus.publish(&sender, &policy, "ephemeral:other", "hi", Urgency::Normal),
            EphemeralPublishResult::UnknownChannel("ephemeral:other".to_string()),
        );
    }

    #[tokio::test(start_paused = true)]
    async fn acl_denied_without_matcher() {
        let bus = one_channel_bus(1024);
        // Grant held, but no ephemeral_publish matcher covers the channel.
        let policy = AppPolicy::with_grants(&[AppCapability::EphemeralPublish]);
        let sender = pid("pub");
        assert_eq!(
            bus.publish(
                &sender,
                &policy,
                "ephemeral:protobar",
                "hi",
                Urgency::Normal
            ),
            EphemeralPublishResult::AclDenied("ephemeral:protobar".to_string()),
        );
    }

    #[tokio::test(start_paused = true)]
    async fn acl_denied_without_grant() {
        let bus = one_channel_bus(1024);
        // ACL matcher present but no grant → deny-by-default.
        let mut policy = AppPolicy::with_grants(&[]);
        policy.acls.ephemeral_publish = vec![ChannelMatcher::Exact("protobar".to_string())];
        let sender = pid("pub");
        assert_eq!(
            bus.publish(
                &sender,
                &policy,
                "ephemeral:protobar",
                "hi",
                Urgency::Normal
            ),
            EphemeralPublishResult::AclDenied("ephemeral:protobar".to_string()),
        );
    }

    #[tokio::test(start_paused = true)]
    async fn body_too_large_rejected() {
        let bus = one_channel_bus(4);
        let policy = publisher_policy("protobar");
        let sender = pid("pub");
        assert_eq!(
            bus.publish(
                &sender,
                &policy,
                "ephemeral:protobar",
                "abcde",
                Urgency::Normal
            ),
            EphemeralPublishResult::BodyTooLarge { len: 5, max: 4 },
        );
        // len == max is allowed.
        assert!(matches!(
            bus.publish(
                &sender,
                &policy,
                "ephemeral:protobar",
                "abcd",
                Urgency::Normal
            ),
            EphemeralPublishResult::Ok { .. },
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn oversized_body_consumes_no_rate_token() {
        let bus = one_channel_bus(4);
        let policy = publisher_policy("protobar");
        let sender = pid("pub");
        // Flood well past the burst with oversized bodies: every one is
        // BodyTooLarge and none consumes a rate token.
        for _ in 0..(EPHEMERAL_SENDER_BURST + 10) {
            assert_eq!(
                bus.publish(
                    &sender,
                    &policy,
                    "ephemeral:protobar",
                    "abcde",
                    Urgency::Normal
                ),
                EphemeralPublishResult::BodyTooLarge { len: 5, max: 4 },
            );
        }
        assert_eq!(bus.rate_limited_count(sender.as_str()), 0);
        // The bucket is still full: a valid publish succeeds.
        assert!(matches!(
            bus.publish(
                &sender,
                &policy,
                "ephemeral:protobar",
                "ok",
                Urgency::Normal
            ),
            EphemeralPublishResult::Ok { .. },
        ));
    }

    #[test]
    fn deny_kind_maps_denial_arms_only() {
        use DenialKind as K;
        use EphemeralPublishResult as R;
        assert_eq!(
            R::MalformedAddress("x".into()).deny_kind(),
            Some(K::MalformedAddress)
        );
        assert_eq!(
            R::UnknownChannel("x".into()).deny_kind(),
            Some(K::UnknownChannel)
        );
        assert_eq!(R::AclDenied("x".into()).deny_kind(), Some(K::AclDenied));
        assert_eq!(
            R::BodyTooLarge { len: 5, max: 4 }.deny_kind(),
            Some(K::BodyTooLarge)
        );
        // Non-bus-denial results carry no kind.
        assert_eq!(
            R::Ok {
                message_id: Uuid::nil(),
                address: "ephemeral:x".into(),
                seq: 1
            }
            .deny_kind(),
            None
        );
        assert_eq!(R::RateLimited.deny_kind(), None);
        assert_eq!(R::MissingSender.deny_kind(), None);
        assert_eq!(R::UnsupportedOption { field: "reply_to" }.deny_kind(), None);
    }

    #[test]
    fn signal_kind_supersets_deny_kind_with_missing_sender() {
        use DenialKind as K;
        use EphemeralPublishResult as R;
        // The four bus-produced arms share their tag with `deny_kind()`.
        assert_eq!(
            R::MalformedAddress("x".into()).signal_kind(),
            Some(K::MalformedAddress)
        );
        assert_eq!(
            R::UnknownChannel("x".into()).signal_kind(),
            Some(K::UnknownChannel)
        );
        assert_eq!(R::AclDenied("x".into()).signal_kind(), Some(K::AclDenied));
        assert_eq!(
            R::BodyTooLarge { len: 5, max: 4 }.signal_kind(),
            Some(K::BodyTooLarge)
        );
        // `MissingSender` gets a signal tag even though the bus never counts it.
        assert_eq!(R::MissingSender.signal_kind(), Some(K::MissingSender));
        // No-signal results stay `None`.
        assert_eq!(R::RateLimited.signal_kind(), None);
        assert_eq!(
            R::UnsupportedOption { field: "reply_to" }.signal_kind(),
            None
        );
        assert_eq!(
            R::Ok {
                message_id: Uuid::nil(),
                address: "ephemeral:x".into(),
                seq: 1
            }
            .signal_kind(),
            None
        );
    }

    #[test]
    fn denied_address_echoes_only_address_bearing_arms() {
        use EphemeralPublishResult as R;
        assert_eq!(R::UnknownChannel("a".into()).denied_address(), Some("a"));
        assert_eq!(R::MalformedAddress("b".into()).denied_address(), Some("b"));
        assert_eq!(R::AclDenied("c".into()).denied_address(), Some("c"));
        assert_eq!(R::MissingSender.denied_address(), None);
        assert_eq!(R::BodyTooLarge { len: 5, max: 4 }.denied_address(), None);
    }

    #[tokio::test(start_paused = true)]
    async fn malformed_address_bumps_denied_counter_no_rate_token() {
        let bus = one_channel_bus(1024);
        let policy = publisher_policy("protobar");
        let sender = pid("pub");
        assert_eq!(
            bus.publish(&sender, &policy, "ephemeral:", "hi", Urgency::Normal),
            EphemeralPublishResult::MalformedAddress("ephemeral:".to_string()),
        );
        assert_eq!(
            bus.publish_denied_count(sender.as_str(), "malformed_address"),
            1
        );
        assert_eq!(bus.rate_limited_count(sender.as_str()), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn unknown_channel_bumps_denied_counter_no_rate_token() {
        let bus = one_channel_bus(1024);
        let policy = publisher_policy("other");
        let sender = pid("pub");
        assert_eq!(
            bus.publish(&sender, &policy, "ephemeral:other", "hi", Urgency::Normal),
            EphemeralPublishResult::UnknownChannel("ephemeral:other".to_string()),
        );
        assert_eq!(
            bus.publish_denied_count(sender.as_str(), "unknown_channel"),
            1
        );
        assert_eq!(bus.rate_limited_count(sender.as_str()), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn acl_denied_bumps_denied_counter_no_rate_token() {
        let bus = one_channel_bus(1024);
        // Grant held, but no matcher covers the channel.
        let policy = AppPolicy::with_grants(&[AppCapability::EphemeralPublish]);
        let sender = pid("pub");
        assert_eq!(
            bus.publish(
                &sender,
                &policy,
                "ephemeral:protobar",
                "hi",
                Urgency::Normal
            ),
            EphemeralPublishResult::AclDenied("ephemeral:protobar".to_string()),
        );
        assert_eq!(bus.publish_denied_count(sender.as_str(), "acl_denied"), 1);
        assert_eq!(bus.rate_limited_count(sender.as_str()), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn body_too_large_bumps_denied_counter_no_rate_token() {
        let bus = one_channel_bus(4);
        let policy = publisher_policy("protobar");
        let sender = pid("pub");
        for i in 1..=(EPHEMERAL_SENDER_BURST + 10) {
            assert_eq!(
                bus.publish(
                    &sender,
                    &policy,
                    "ephemeral:protobar",
                    "abcde",
                    Urgency::Normal
                ),
                EphemeralPublishResult::BodyTooLarge { len: 5, max: 4 },
            );
            // Every oversized publish bumps the denied counter (unbounded by the
            // rate gate) and never consumes a rate token.
            assert_eq!(
                bus.publish_denied_count(sender.as_str(), "body_too_large"),
                i as u64
            );
        }
        assert_eq!(bus.rate_limited_count(sender.as_str()), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn ok_and_rate_limited_leave_denied_untouched() {
        let bus = bus_with(vec![entry("protobar", 0, 16)], 1024);
        let policy = publisher_policy("protobar");
        let sender = pid("pub");
        // A successful publish bumps no denied counter.
        assert!(matches!(
            bus.publish(&sender, &policy, "ephemeral:protobar", "x", Urgency::Normal),
            EphemeralPublishResult::Ok { .. },
        ));
        // Exhaust the burst, then drive one RateLimited.
        for _ in 1..EPHEMERAL_SENDER_BURST {
            let _ = bus.publish(&sender, &policy, "ephemeral:protobar", "x", Urgency::Normal);
        }
        assert_eq!(
            bus.publish(&sender, &policy, "ephemeral:protobar", "x", Urgency::Normal),
            EphemeralPublishResult::RateLimited,
        );
        // RateLimited bumps only `rate_limited`, never `publish_denied`.
        assert_eq!(bus.rate_limited_count(sender.as_str()), 1);
        for kind in [
            "malformed_address",
            "unknown_channel",
            "acl_denied",
            "body_too_large",
        ] {
            assert_eq!(bus.publish_denied_count(sender.as_str(), kind), 0);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn rate_gate_floods_then_refills() {
        // retain_depth 0 keeps memory flat under the flood; capacity is
        // irrelevant with no attached receivers (a zero-subscriber `send`
        // buffers nothing).
        let bus = bus_with(vec![entry("protobar", 0, 16)], 1024);
        let policy = publisher_policy("protobar");
        let sender = pid("pub");

        // Exhaust the burst.
        for _ in 0..EPHEMERAL_SENDER_BURST {
            assert!(matches!(
                bus.publish(&sender, &policy, "ephemeral:protobar", "x", Urgency::Normal),
                EphemeralPublishResult::Ok { .. },
            ));
        }
        // Next is denied.
        assert_eq!(
            bus.publish(&sender, &policy, "ephemeral:protobar", "x", Urgency::Normal),
            EphemeralPublishResult::RateLimited,
        );
        assert_eq!(bus.rate_limited_count(sender.as_str()), 1);

        // After a whole refill interval, `refill_amount` tokens are back.
        tokio::time::advance(EPHEMERAL_SENDER_REFILL_INTERVAL).await;
        for _ in 0..EPHEMERAL_SENDER_REFILL_AMOUNT {
            assert!(matches!(
                bus.publish(&sender, &policy, "ephemeral:protobar", "x", Urgency::Normal),
                EphemeralPublishResult::Ok { .. },
            ));
        }
        assert_eq!(
            bus.publish(&sender, &policy, "ephemeral:protobar", "x", Urgency::Normal),
            EphemeralPublishResult::RateLimited,
        );
    }

    #[test]
    fn concurrent_publishers_get_dense_unique_seqs() {
        // Real OS-thread contention on one channel: the seq-assignment /
        // ring-append / broadcast-send critical section must stay atomic, so the
        // union of all assigned seqs is exactly `1..=N` with no gaps or dupes.
        // Each thread uses a distinct sender so PER_THREAD stays under the
        // per-sender burst (no rate-limit denials during the test window).
        const THREADS: u64 = 4;
        const PER_THREAD: u64 = 200;
        let bus = bus_with(vec![entry("protobar", 8, 16)], 1024);

        let handles: Vec<_> = (0..THREADS)
            .map(|t| {
                let bus = Arc::clone(&bus);
                std::thread::spawn(move || {
                    let policy = publisher_policy("protobar");
                    let sender = pid(&format!("pub{t}"));
                    let mut seqs = Vec::with_capacity(PER_THREAD as usize);
                    for _ in 0..PER_THREAD {
                        match bus.publish(
                            &sender,
                            &policy,
                            "ephemeral:protobar",
                            "x",
                            Urgency::Normal,
                        ) {
                            EphemeralPublishResult::Ok { seq, .. } => seqs.push(seq),
                            other => panic!("expected Ok, got {other:?}"),
                        }
                    }
                    seqs
                })
            })
            .collect();

        let mut all: Vec<u64> = handles
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect();
        all.sort_unstable();
        let expected: Vec<u64> = (1..=THREADS * PER_THREAD).collect();
        assert_eq!(all, expected);
        assert_eq!(bus.publish_count("protobar"), THREADS * PER_THREAD);
    }

    #[tokio::test(start_paused = true)]
    async fn rate_gate_is_per_sender() {
        let bus = bus_with(vec![entry("protobar", 0, 16)], 1024);
        let policy = publisher_policy("protobar");
        let sender_a = pid("a");
        let sender_b = pid("b");

        // Exhaust A.
        for _ in 0..EPHEMERAL_SENDER_BURST {
            assert!(matches!(
                bus.publish(
                    &sender_a,
                    &policy,
                    "ephemeral:protobar",
                    "x",
                    Urgency::Normal
                ),
                EphemeralPublishResult::Ok { .. },
            ));
        }
        assert_eq!(
            bus.publish(
                &sender_a,
                &policy,
                "ephemeral:protobar",
                "x",
                Urgency::Normal
            ),
            EphemeralPublishResult::RateLimited,
        );
        // B has its own full bucket.
        assert!(matches!(
            bus.publish(
                &sender_b,
                &policy,
                "ephemeral:protobar",
                "x",
                Urgency::Normal
            ),
            EphemeralPublishResult::Ok { .. },
        ));
    }

    // --- Subscribe / attach / detach ---

    fn subscriber_policy(channel: &str) -> Arc<AppPolicy> {
        let mut p = AppPolicy::with_grants(&[AppCapability::EphemeralSubscribe]);
        p.acls.ephemeral_subscribe = vec![ChannelMatcher::Exact(channel.to_string())];
        Arc::new(p)
    }

    /// Publish `n` messages, panicking on any non-`Ok`.
    fn publish_n(bus: &EphemeralBus, sender: &ParticipantId, policy: &AppPolicy, n: usize) {
        for _ in 0..n {
            match bus.publish(sender, policy, "ephemeral:protobar", "x", Urgency::Normal) {
                EphemeralPublishResult::Ok { .. } => {}
                other => panic!("expected Ok, got {other:?}"),
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn subscribe_malformed_and_unknown_and_denied() {
        let bus = one_channel_bus(1024);

        assert_eq!(
            bus.subscribe(pid("s"), subscriber_policy("protobar"), "protobar", None)
                .err(),
            Some(EphemeralSubscribeError::MalformedAddress(
                "protobar".to_string()
            )),
        );
        assert_eq!(
            bus.subscribe(
                pid("s"),
                subscriber_policy("other"),
                "ephemeral:other",
                None
            )
            .err(),
            Some(EphemeralSubscribeError::UnknownChannel(
                "ephemeral:other".to_string()
            )),
        );
        // Grant but no matcher → deny-by-default.
        let no_matcher = Arc::new(AppPolicy::with_grants(&[AppCapability::EphemeralSubscribe]));
        assert_eq!(
            bus.subscribe(pid("s"), no_matcher, "ephemeral:protobar", None)
                .err(),
            Some(EphemeralSubscribeError::AclDenied(
                "ephemeral:protobar".to_string()
            )),
        );
        // Matcher but no grant → deny-by-default.
        let mut p = AppPolicy::with_grants(&[]);
        p.acls.ephemeral_subscribe = vec![ChannelMatcher::Exact("protobar".to_string())];
        assert_eq!(
            bus.subscribe(pid("s"), Arc::new(p), "ephemeral:protobar", None)
                .err(),
            Some(EphemeralSubscribeError::AclDenied(
                "ephemeral:protobar".to_string()
            )),
        );
    }

    #[tokio::test(start_paused = true)]
    async fn fresh_replays_full_ring_oldest_first() {
        // retain_depth 8 ≥ 5 published: the whole history replays, seq ascending.
        let bus = one_channel_bus(1024);
        let sender = pid("pub");
        publish_n(&bus, &sender, &publisher_policy("protobar"), 5);

        let sub = bus
            .subscribe(
                pid("s"),
                subscriber_policy("protobar"),
                "ephemeral:protobar",
                None,
            )
            .expect("subscribe");
        assert_eq!(sub.decision, Replay::Fresh);
        let seqs: Vec<u64> = sub.replay.iter().map(|d| d.seq).collect();
        assert_eq!(seqs, vec![1, 2, 3, 4, 5]);
    }

    #[tokio::test(start_paused = true)]
    async fn fresh_retain_depth_zero_replays_nothing() {
        let bus = bus_with(vec![entry("protobar", 0, 16)], 1024);
        let sender = pid("pub");
        publish_n(&bus, &sender, &publisher_policy("protobar"), 3);

        let sub = bus
            .subscribe(
                pid("s"),
                subscriber_policy("protobar"),
                "ephemeral:protobar",
                None,
            )
            .expect("subscribe");
        assert_eq!(sub.decision, Replay::Fresh);
        assert!(sub.replay.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn fresh_retain_depth_one_replays_only_newest() {
        let bus = bus_with(vec![entry("protobar", 1, 16)], 1024);
        let sender = pid("pub");
        publish_n(&bus, &sender, &publisher_policy("protobar"), 4);

        let sub = bus
            .subscribe(
                pid("s"),
                subscriber_policy("protobar"),
                "ephemeral:protobar",
                None,
            )
            .expect("subscribe");
        let seqs: Vec<u64> = sub.replay.iter().map(|d| d.seq).collect();
        assert_eq!(seqs, vec![4]);
    }

    #[tokio::test(start_paused = true)]
    async fn resume_exact_replays_only_after_last_seen() {
        let bus = one_channel_bus(1024);
        let sender = pid("pub");
        publish_n(&bus, &sender, &publisher_policy("protobar"), 5);

        let resume = Some(EphemeralResume {
            epoch: bus.epoch(),
            seq: 3,
        });
        let sub = bus
            .subscribe(
                pid("s"),
                subscriber_policy("protobar"),
                "ephemeral:protobar",
                resume,
            )
            .expect("subscribe");
        assert_eq!(sub.decision, Replay::Exact);
        let seqs: Vec<u64> = sub.replay.iter().map(|d| d.seq).collect();
        assert_eq!(seqs, vec![4, 5]);
    }

    #[tokio::test(start_paused = true)]
    async fn resume_up_to_date_replays_nothing() {
        let bus = one_channel_bus(1024);
        let sender = pid("pub");
        publish_n(&bus, &sender, &publisher_policy("protobar"), 5);

        let resume = Some(EphemeralResume {
            epoch: bus.epoch(),
            seq: 5,
        });
        let sub = bus
            .subscribe(
                pid("s"),
                subscriber_policy("protobar"),
                "ephemeral:protobar",
                resume,
            )
            .expect("subscribe");
        assert_eq!(sub.decision, Replay::UpToDate);
        assert!(sub.replay.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn resume_hole_exceeds_ring_gaps_and_replays_full_ring() {
        // retain_depth 2 but resume from seq 1 with newest 5: the hole
        // (2..=3) predates the ring (holds 4,5) → gap + full ring.
        let bus = bus_with(vec![entry("protobar", 2, 16)], 1024);
        let sender = pid("pub");
        publish_n(&bus, &sender, &publisher_policy("protobar"), 5);

        let resume = Some(EphemeralResume {
            epoch: bus.epoch(),
            seq: 1,
        });
        let sub = bus
            .subscribe(
                pid("s"),
                subscriber_policy("protobar"),
                "ephemeral:protobar",
                resume,
            )
            .expect("subscribe");
        assert_eq!(sub.decision, Replay::Gap(GapReason::HoleExceedsRing));
        let seqs: Vec<u64> = sub.replay.iter().map(|d| d.seq).collect();
        assert_eq!(seqs, vec![4, 5]);
    }

    #[tokio::test(start_paused = true)]
    async fn resume_epoch_changed_gaps() {
        let bus = one_channel_bus(1024);
        let sender = pid("pub");
        publish_n(&bus, &sender, &publisher_policy("protobar"), 3);

        // A different epoch (as if from a prior boot / other instance).
        let resume = Some(EphemeralResume {
            epoch: Uuid::new_v4(),
            seq: 2,
        });
        let sub = bus
            .subscribe(
                pid("s"),
                subscriber_policy("protobar"),
                "ephemeral:protobar",
                resume,
            )
            .expect("subscribe");
        assert_eq!(sub.decision, Replay::Gap(GapReason::EpochChanged));
        let seqs: Vec<u64> = sub.replay.iter().map(|d| d.seq).collect();
        assert_eq!(seqs, vec![1, 2, 3]);
    }

    #[tokio::test(start_paused = true)]
    async fn resume_ahead_gaps() {
        let bus = one_channel_bus(1024);
        let sender = pid("pub");
        publish_n(&bus, &sender, &publisher_policy("protobar"), 3);

        let resume = Some(EphemeralResume {
            epoch: bus.epoch(),
            seq: 99,
        });
        let sub = bus
            .subscribe(
                pid("s"),
                subscriber_policy("protobar"),
                "ephemeral:protobar",
                resume,
            )
            .expect("subscribe");
        assert_eq!(sub.decision, Replay::Gap(GapReason::ResumeAhead));
        let seqs: Vec<u64> = sub.replay.iter().map(|d| d.seq).collect();
        assert_eq!(seqs, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn live_delivery_carries_ephemeral_envelope() {
        let bus = one_channel_bus(1024);
        let sub = bus
            .subscribe(
                pid("s"),
                subscriber_policy("protobar"),
                "ephemeral:protobar",
                None,
            )
            .expect("subscribe");
        assert_eq!(sub.decision, Replay::Fresh);
        assert!(sub.replay.is_empty());
        let mut receiver = sub.receiver;

        let sender = pid("pub");
        assert!(matches!(
            bus.publish(
                &sender,
                &publisher_policy("protobar"),
                "ephemeral:protobar",
                "hello",
                Urgency::Normal
            ),
            EphemeralPublishResult::Ok { seq: 1, .. },
        ));

        match receiver.recv().await {
            Some(EphemeralEvent::Delivery(d)) => {
                assert_eq!(d.seq, 1);
                assert_eq!(d.envelope.envelope_type, ChannelScheme::Ephemeral);
                assert_eq!(d.envelope.channel, "ephemeral:protobar");
                assert_eq!(d.envelope.sender, sender.as_str());
                assert_eq!(d.envelope.body, "hello");
                assert!(d.envelope.reply_to.is_none());
                assert!(d.envelope.deliver_after.is_none());
                assert!(d.envelope.delivery_deadline.is_none());
            }
            other => panic!("expected Delivery, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn fan_out_all_subscribers_receive_all_in_order() {
        const SUBS: usize = 3;
        const MSGS: u64 = 10;
        let bus = bus_with(vec![entry("protobar", 4, 64)], 1024);

        let mut receivers = Vec::new();
        for _ in 0..SUBS {
            receivers.push(
                bus.subscribe(
                    pid("s"),
                    subscriber_policy("protobar"),
                    "ephemeral:protobar",
                    None,
                )
                .expect("subscribe")
                .receiver,
            );
        }

        let sender = pid("pub");
        publish_n(&bus, &sender, &publisher_policy("protobar"), MSGS as usize);

        for mut receiver in receivers {
            let mut seqs = Vec::new();
            for _ in 0..MSGS {
                match receiver.recv().await {
                    Some(EphemeralEvent::Delivery(d)) => seqs.push(d.seq),
                    other => panic!("expected Delivery, got {other:?}"),
                }
            }
            assert_eq!(seqs, (1..=MSGS).collect::<Vec<_>>());
        }
    }

    #[tokio::test]
    async fn overflow_reports_exact_drop_count_then_resumes() {
        // capacity 2, flood 5 with no interleaved recv: the receiver lags by 3
        // (seqs 1..3 overwritten), retaining the 2 newest (4, 5).
        let bus = bus_with(vec![entry("protobar", 0, 2)], 1024);
        let subscriber = pid("s");
        let mut receiver = bus
            .subscribe(
                subscriber.clone(),
                subscriber_policy("protobar"),
                "ephemeral:protobar",
                None,
            )
            .expect("subscribe")
            .receiver;

        let sender = pid("pub");
        publish_n(&bus, &sender, &publisher_policy("protobar"), 5);

        // First recv surfaces the lag as an exact drop count.
        match receiver.recv().await {
            Some(EphemeralEvent::Dropped(n)) => assert_eq!(n, 3),
            other => panic!("expected Dropped(3), got {other:?}"),
        }
        assert_eq!(bus.drop_count("protobar", subscriber.as_str()), 3);

        // Delivery resumes with the retained newest messages.
        let mut resumed = Vec::new();
        for _ in 0..2 {
            match receiver.recv().await {
                Some(EphemeralEvent::Delivery(d)) => resumed.push(d.seq),
                other => panic!("expected Delivery, got {other:?}"),
            }
        }
        assert_eq!(resumed, vec![4, 5]);
    }

    #[tokio::test]
    async fn delivery_time_acl_deny_skips_and_counts() {
        // Construct a receiver directly with a policy that denies delivery, to
        // exercise the belt-and-suspenders re-check branch that boot-static
        // policies cannot reach through `subscribe`.
        let metrics = Arc::new(EphemeralMetrics {
            rate_limited: Mutex::new(HashMap::new()),
            drops: Mutex::new(HashMap::new()),
            delivery_denied: Mutex::new(HashMap::new()),
            publish_denied: Mutex::new(HashMap::new()),
        });
        let (tx, rx) = broadcast::channel(4);
        let subscriber = pid("s");
        let mut receiver = EphemeralReceiver {
            rx,
            subscriber: subscriber.clone(),
            // No grant → allows_channel_access is false for any address.
            policy: Arc::new(AppPolicy::with_grants(&[])),
            address: "ephemeral:protobar".to_string(),
            metrics: Arc::clone(&metrics),
        };

        tx.send(make_delivery(1)).expect("send");
        drop(tx); // close after the one message

        // The denied delivery is skipped; the loop then sees Closed → None.
        assert!(receiver.recv().await.is_none());
        assert_eq!(
            *metrics
                .delivery_denied
                .lock()
                .unwrap()
                .get(&("protobar".to_string(), subscriber.as_str().to_string()))
                .unwrap(),
            1,
        );
    }

    fn make_delivery(seq: u64) -> Arc<EphemeralDelivery> {
        Arc::new(EphemeralDelivery {
            seq,
            envelope: MessageEnvelope {
                message_id: Uuid::new_v4(),
                source: SOURCE.into(),
                channel: "ephemeral:protobar".into(),
                sender: "app:pub@test-source".into(),
                publish_ts: Utc::now(),
                body: "x".to_string(),
                reply_to: None,
                delivery_deadline: None,
                deliver_after: None,
                urgency: Urgency::Normal,
                envelope_type: ChannelScheme::Ephemeral,
            },
        })
    }

    #[tokio::test]
    async fn attach_boundary_no_loss_no_duplication() {
        // A publisher thread races an attach: every seq in 1..=N appears exactly
        // once across (replay ∪ live) — the atomicity guarantee. retain_depth and
        // capacity both exceed N so nothing is evicted or overflowed, and N stays
        // under the per-sender burst so no publish is rate-limited.
        const N: u64 = 200;
        let bus = bus_with(vec![entry("protobar", 256, 512)], 1024);

        let bus_pub = Arc::clone(&bus);
        let handle = std::thread::spawn(move || {
            let sender = pid("pub");
            let policy = publisher_policy("protobar");
            for _ in 0..N {
                match bus_pub.publish(&sender, &policy, "ephemeral:protobar", "x", Urgency::Normal)
                {
                    EphemeralPublishResult::Ok { .. } => {}
                    other => panic!("publish failed: {other:?}"),
                }
                std::thread::yield_now();
            }
        });

        let sub = bus
            .subscribe(
                pid("s"),
                subscriber_policy("protobar"),
                "ephemeral:protobar",
                None,
            )
            .expect("subscribe");
        let mut seen: Vec<u64> = sub.replay.iter().map(|d| d.seq).collect();
        let live_expected = N as usize - seen.len();
        let mut receiver = sub.receiver;
        for _ in 0..live_expected {
            match receiver.recv().await {
                Some(EphemeralEvent::Delivery(d)) => seen.push(d.seq),
                other => panic!("expected Delivery, got {other:?}"),
            }
        }
        handle.join().unwrap();

        seen.sort_unstable();
        assert_eq!(seen, (1..=N).collect::<Vec<_>>());
    }
}
