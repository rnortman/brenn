//! MQTT client registry state: the per-client handle and supervisor state.
//!
//! `MqttClientHandle` is the single handle type serving both the publish path
//! (the current client and the facts of its connection) and the ingress
//! delivery + reconnect re-assert path. One is built per declared `[[mqtt_client]]`, whether or not
//! anything is bound through it, and held on `MqttService` in a
//! `client_slug`-keyed registry.

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use tokio::sync::{Mutex, Notify, RwLock, watch};

use brenn_lib::mqtt::config::MqttClientConfig;
use brenn_lib::mqtt::subscription_identifier;
use brenn_lib::mqtt::topic_filter::covers;

use crate::reader::ReaderSlot;

/// Poisoning message for [`MqttClientHandle::pending_subscribes`]. Its critical
/// sections are synchronous and short, so poisoning means a holder panicked
/// mid-section and the FIFO no longer matches the request channel.
pub(crate) const PENDING_SUBSCRIBES_POISONED: &str =
    "pending_subscribes lock poisoned: a holder panicked inside its critical section";

/// Poisoning message for [`MqttClientHandle::pending_unsubscribes`].
pub(crate) const PENDING_UNSUBSCRIBES_POISONED: &str =
    "pending_unsubscribes lock poisoned: a holder panicked inside its critical section";

/// Poisoning message for [`MqttClientHandle::filter_requests`].
pub(crate) const FILTER_REQUESTS_POISONED: &str =
    "filter_requests lock poisoned: a holder panicked inside its critical section";

/// Poisoning message for [`MqttClientHandle::request_order`].
pub(crate) const REQUEST_ORDER_POISONED: &str =
    "request_order lock poisoned: a holder panicked inside its critical section";

/// One SUBSCRIBE or UNSUBSCRIBE the event loop has been handed and has not yet
/// reported written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingRequest {
    /// The request's one topic filter.
    pub topic_filter: String,
    /// The connection epoch the request was submitted on.
    pub epoch: u64,
}

/// The order of the SUBSCRIBE and UNSUBSCRIBE requests for each filter on the
/// current connection, each submit and each request seen written counted.
#[derive(Debug, Default)]
pub(crate) struct RequestOrder {
    next: u64,
    newest: HashMap<String, u64>,
}

impl RequestOrder {
    /// Count a request for `topic_filter` and return its number.
    pub(crate) fn bump(&mut self, topic_filter: &str) -> u64 {
        self.next += 1;
        self.newest.insert(topic_filter.to_string(), self.next);
        self.next
    }

    /// Whether `seq` is the newest request counted for `topic_filter`.
    pub(crate) fn is_newest(&self, topic_filter: &str, seq: u64) -> bool {
        self.newest.get(topic_filter) == Some(&seq)
    }

    pub(crate) fn clear(&mut self) {
        self.newest.clear();
    }
}

// ---------------------------------------------------------------------------
// Supervisor state (visible to the registry for health reporting)
// ---------------------------------------------------------------------------

/// What the broker answered for one topic filter's SUBSCRIBE on the current
/// session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubAckOutcome {
    /// Granted: the broker delivers on this filter until the session ends.
    Granted,
    /// Refused, carrying the SUBACK's own words. Final for the session — the
    /// broker does not change its answer without a new SUBSCRIBE.
    Refused(String),
}

/// The live connection state of a supervisor, as readable from outside.
#[derive(Debug, Clone)]
pub enum SupervisorState {
    /// Not yet attempted, or in backoff between attempts.
    Disconnected {
        /// Most recent failure reason, if any.
        last_error: Option<String>,
        /// When the next connect attempt is scheduled.
        next_attempt_at: Instant,
    },
    /// A connect is in progress.
    Connecting { since: Instant },
    /// Connected and subscriptions are active.
    Connected,
    /// Authoritative failure — supervisor has stopped retrying.
    Failed { reason: String },
}

/// Health label surfaced to the LLM (via `MessageChannelList` channel
/// enrichment).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ConnectorHealthLabel {
    Connected,
    Disconnected,
    /// Authoritative failure — supervisor has stopped retrying.
    Failed,
}

impl ConnectorHealthLabel {
    /// Lowercase wire string for this label — the single source of truth for the
    /// `health` field rendered to the LLM (`MessageChannelList` enrichment).
    ///
    /// Must stay identical to the `#[serde(rename_all = "lowercase")]` form; the
    /// `wire_str_matches_serde` test pins that equality so a new variant cannot
    /// drift the two apart.
    pub fn wire_str(self) -> &'static str {
        match self {
            Self::Connected => "connected",
            Self::Disconnected => "disconnected",
            Self::Failed => "failed",
        }
    }
}

impl SupervisorState {
    pub fn health_label(&self) -> ConnectorHealthLabel {
        match self {
            SupervisorState::Connected => ConnectorHealthLabel::Connected,
            SupervisorState::Connecting { .. } => ConnectorHealthLabel::Disconnected,
            SupervisorState::Disconnected { .. } => ConnectorHealthLabel::Disconnected,
            SupervisorState::Failed { .. } => ConnectorHealthLabel::Failed,
        }
    }

    pub fn last_error(&self) -> Option<&str> {
        match self {
            SupervisorState::Connected => None,
            SupervisorState::Connecting { .. } => Some("reconnecting"),
            SupervisorState::Disconnected { last_error, .. } => {
                last_error.as_deref().or(Some("unknown"))
            }
            SupervisorState::Failed { reason } => Some(reason.as_str()),
        }
    }
}

// ---------------------------------------------------------------------------
// Publish outcomes
// ---------------------------------------------------------------------------

/// What the client library reported for one tracked publish when its notice
/// resolved. The egress reader reads every outcome through
/// [`notice_outcome`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NoticeOutcome {
    /// Flushed to the network (QoS 0), or acknowledged with a success reason
    /// (QoS 1/2).
    Delivered,
    /// The broker answered with a failure reason: the packet's reason string
    /// when it carried one, else the reason code's name; and the code.
    Rejected { reason: String, code: u8 },
    /// The notice ended without the broker's answer: the broker reported no
    /// session at a reconnect, the client was dropped, or a QoS 0 write
    /// failed.
    SessionEnded,
}

/// Map a resolved publish notice to its outcome.
///
/// # Panics
///
/// On a topic-alias replay failure or a completion-check error: Brenn sets no
/// topic alias and never asks the library to check completion, so either is a
/// host bug.
pub fn notice_outcome(
    result: Result<rumqttc::PublishResult, rumqttc::PublishNoticeError>,
) -> NoticeOutcome {
    use rumqttc::{PubAckReason, PubCompReason, PublishNoticeError as E, PublishResult as R};
    match result {
        Ok(R::Qos0Flushed) => NoticeOutcome::Delivered,
        Ok(R::Qos1(ack)) => {
            if ack.reason == PubAckReason::Success
                || ack.reason == PubAckReason::NoMatchingSubscribers
            {
                NoticeOutcome::Delivered
            } else {
                NoticeOutcome::Rejected {
                    reason: reason_text(ack.properties.and_then(|p| p.reason_string), &ack.reason),
                    code: ack.reason as u8,
                }
            }
        }
        // TODO(mqtt-qos2-completion-refusal-told-rejected)
        Ok(R::Qos2Completed(comp)) => {
            if comp.reason == PubCompReason::Success {
                NoticeOutcome::Delivered
            } else {
                NoticeOutcome::Rejected {
                    reason: reason_text(
                        comp.properties.and_then(|p| p.reason_string),
                        &comp.reason,
                    ),
                    code: comp.reason as u8,
                }
            }
        }
        // TODO(mqtt-quota-exceeded-told-rejected): a Quota exceeded refusal is
        // "not now", and is told as a rejection.
        Ok(R::Qos2PubRecRejected(rec)) => NoticeOutcome::Rejected {
            reason: reason_text(rec.properties.and_then(|p| p.reason_string), &rec.reason),
            code: rec.reason as u8,
        },
        Err(E::Recv | E::SessionReset | E::Qos0NotFlushed) => NoticeOutcome::SessionEnded,
        Err(
            e @ (E::TopicAliasReplayUnavailable(_)
            | E::V5PubAck(_)
            | E::V5PubRec(_)
            | E::V5PubComp(_)),
        ) => {
            panic!(
                "publish notice resolved with {e:?}: Brenn sets no topic alias and never asks for a completion check"
            )
        }
    }
}

/// The broker's reason string when it sent one, else the reason code.
fn reason_text(reason_string: Option<String>, code: &dyn std::fmt::Debug) -> String {
    reason_string.unwrap_or_else(|| format!("{code:?}"))
}

/// The Receive Maximum MQTT 5.0 section 3.2.2.3.3 gives a CONNACK that states
/// none.
pub const DEFAULT_RECEIVE_MAXIMUM: u16 = u16::MAX;

// ---------------------------------------------------------------------------
// Ingress subscription set
// ---------------------------------------------------------------------------

/// One entry in a client's deduplicated ingress subscription set: a topic filter
/// to SUBSCRIBE to and the MQTT 5 Subscription Identifier it is subscribed
/// with. The entry carries no QoS: every filter of a client is subscribed at
/// the `qos` of the configuration the client's session runs with.
///
/// The identifier is derived from the filter string
/// ([`subscription_identifier`]), so every process gives a filter the same one.
/// The broker returns it on each copy of a PUBLISH it sends for the
/// subscription, and the inbound path resolves it back to this entry's filter
/// and routes the copy to that filter's channel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngressSubscription {
    /// The MQTT topic filter to subscribe to.
    pub topic_filter: String,
    /// [`subscription_identifier`] of `topic_filter`.
    pub sub_id: u32,
}

impl IngressSubscription {
    /// The entry for `topic_filter`, with the filter's derived identifier.
    pub fn new(topic_filter: impl Into<String>) -> Self {
        let topic_filter = topic_filter.into();
        let sub_id = subscription_identifier(&topic_filter);
        Self {
            topic_filter,
            sub_id,
        }
    }
}

/// A client's ingress subscriptions, each held under the identifier derived
/// from its filter.
///
/// A filter has the same identifier in every process and on every handle, so a
/// copy a resumed broker session queued under an earlier process resolves to
/// the filter it was queued for. Two different filters can share an
/// identifier: both are held and both are subscribed with it, and
/// [`Self::filters_for`] gives a copy carrying it to those of them whose filter
/// covers its topic. A message both cover therefore reaches each of the two
/// channels twice, which is what routing by topic gives.
///
/// Beside the held filters the set keeps the **asserted** filters: every filter
/// the broker's session may hold for the client, as far as this process knows.
/// It holds every filter this set, or a set it was cloned from, has held since
/// the last CONNACK that reported no session — since construction if there has
/// been none — less the filters an UNSUBACK has taken out. It is a superset of
/// the held filters at all times: [`Self::new`] and [`Self::add`] insert into
/// both, and [`Self::remove`] leaves it alone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngressSubscriptionSet {
    entries: Vec<IngressSubscription>,
    asserted: BTreeSet<String>,
}

/// What [`IngressSubscriptionSet::filters_for`] resolved a PUBLISH's
/// identifiers to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedIdentifiers {
    /// The filters the identifiers resolved to, each filter once, in the
    /// order the packet named the identifiers.
    pub filters: Vec<String>,
    /// The identifiers no entry holds, in packet order.
    pub unknown: Vec<usize>,
}

impl IngressSubscriptionSet {
    /// A set holding `entries`.
    ///
    /// # Panics
    ///
    /// If two entries share a filter, or an entry's identifier is not
    /// [`subscription_identifier`] of its filter; either is a host bug.
    /// Different filters sharing an identifier are accepted.
    pub fn new(entries: Vec<IngressSubscription>) -> Self {
        for (i, entry) in entries.iter().enumerate() {
            assert_eq!(
                entry.sub_id,
                subscription_identifier(&entry.topic_filter),
                "subscription {:?} carries an identifier not derived from its filter",
                entry.topic_filter,
            );
            for other in &entries[..i] {
                assert_ne!(
                    other.topic_filter, entry.topic_filter,
                    "filter {:?} appears twice in one subscription set",
                    entry.topic_filter,
                );
            }
        }
        let asserted = entries.iter().map(|e| e.topic_filter.clone()).collect();
        Self { entries, asserted }
    }

    /// The entry for `topic_filter`, adding one with the filter's derived
    /// identifier if the set does not hold the filter. The filter is asserted
    /// from then on.
    pub fn add(&mut self, topic_filter: String) -> IngressSubscription {
        if let Some(existing) = self.get(&topic_filter) {
            return existing.clone();
        }
        let entry = IngressSubscription::new(topic_filter);
        self.asserted.insert(entry.topic_filter.clone());
        self.entries.push(entry.clone());
        entry
    }

    /// Whether the broker's session may hold `topic_filter`.
    pub fn is_asserted(&self, topic_filter: &str) -> bool {
        self.asserted.contains(topic_filter)
    }

    /// The asserted filters the set does not hold: those whose withdrawal the
    /// broker may not have seen yet, in filter order.
    pub fn withdrawn(&self) -> Vec<String> {
        self.asserted
            .iter()
            .filter(|f| self.get(f).is_none())
            .cloned()
            .collect()
    }

    /// Make the asserted filters the held ones: the broker reported no
    /// session, so it holds nothing.
    pub(crate) fn reset_asserted(&mut self) {
        self.asserted = self
            .entries
            .iter()
            .map(|e| e.topic_filter.clone())
            .collect();
    }

    /// Count `topic_filter` as asserted: a SUBSCRIBE for it was written.
    pub(crate) fn mark_asserted(&mut self, topic_filter: &str) {
        self.asserted.insert(topic_filter.to_string());
    }

    /// Take `topic_filter` out of the asserted filters, unless the set holds
    /// it, returning whether it was taken out: the broker answered its
    /// UNSUBSCRIBE with Success or No subscription existed.
    pub(crate) fn withdraw_asserted(&mut self, topic_filter: &str) -> bool {
        self.get(topic_filter).is_none() && self.asserted.remove(topic_filter)
    }

    /// Remove the entry for `topic_filter`, returning whether one was held.
    /// The filter stays asserted.
    pub fn remove(&mut self, topic_filter: &str) -> bool {
        let before = self.entries.len();
        self.entries.retain(|e| e.topic_filter != topic_filter);
        self.entries.len() != before
    }

    /// The other held filters whose identifier equals `topic_filter`'s.
    pub fn sharing_identifier(&self, topic_filter: &str) -> Vec<String> {
        let id = subscription_identifier(topic_filter);
        self.entries
            .iter()
            .filter(|e| e.sub_id == id && e.topic_filter != topic_filter)
            .map(|e| e.topic_filter.clone())
            .collect()
    }

    /// Resolve the Subscription Identifiers a PUBLISH on `topic` carried to
    /// the filters of the entries holding them.
    ///
    /// An identifier one entry holds resolves to that entry's filter. One that
    /// several entries share resolves to those of them whose filter covers
    /// `topic`, or to all of them when none does, so the router's cover check
    /// reports the copy as it does for a single holder.
    pub fn filters_for(&self, ids: &[usize], topic: &str) -> ResolvedIdentifiers {
        let mut resolved = ResolvedIdentifiers {
            filters: Vec::new(),
            unknown: Vec::new(),
        };
        for &id in ids {
            let holders: Vec<&IngressSubscription> = self
                .entries
                .iter()
                .filter(|e| e.sub_id as usize == id)
                .collect();
            if holders.is_empty() {
                if !resolved.unknown.contains(&id) {
                    resolved.unknown.push(id);
                }
                continue;
            }
            let covering: Vec<&IngressSubscription> = holders
                .iter()
                .copied()
                .filter(|e| covers(&e.topic_filter, topic))
                .collect();
            let chosen = if covering.is_empty() {
                holders
            } else {
                covering
            };
            for entry in chosen {
                if !resolved.filters.contains(&entry.topic_filter) {
                    resolved.filters.push(entry.topic_filter.clone());
                }
            }
        }
        resolved
    }

    /// The entry for `topic_filter`, if held.
    pub fn get(&self, topic_filter: &str) -> Option<&IngressSubscription> {
        self.entries.iter().find(|e| e.topic_filter == topic_filter)
    }

    pub fn iter(&self) -> std::slice::Iter<'_, IngressSubscription> {
        self.entries.iter()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn to_vec(&self) -> Vec<IngressSubscription> {
        self.entries.clone()
    }
}

/// Log that `entry` and the held filter `other` share a Subscription
/// Identifier, so the broker's copies for the two cannot be told apart.
fn warn_shared_identifier(client: &str, entry: &IngressSubscription, other: &str) {
    tracing::warn!(
        client,
        filter = %entry.topic_filter,
        other_filter = %other,
        sub_id = entry.sub_id,
        "two MQTT topic filters share a Subscription Identifier; a message matching both is \
         delivered twice into each of their two channels"
    );
}

// ---------------------------------------------------------------------------
// Client handle
// ---------------------------------------------------------------------------

/// Per-client handle held in the registry. The connection supervisor task keeps
/// a clone of the shared fields and updates them; callers read them through the
/// registry.
///
/// This single handle serves both directions on one session: the publish path
/// (`client` and the facts of its connection) and the ingress delivery +
/// filter settling path (`subscriptions` with its asserted filters,
/// `filter_requests`, and the pending and inflight maps that attribute SUBACKs
/// and UNSUBACKs).
pub struct MqttClientHandle {
    /// Resolved per-client config — the single source of truth for per-client
    /// constants (slug, QoS, urgency, session expiry, broker coordinates,
    /// backoff). Shared with the connection supervisor.
    pub config: Arc<MqttClientConfig>,

    /// The deduplicated union subscription set for this client — the held
    /// filters, which the supervisor subscribes on every connect, with the
    /// asserted filters beside them, which a connection that finds the session
    /// withdraws when not held — and the table the inbound path resolves
    /// Subscription Identifiers against. Lives in memory only: a new process
    /// starts with the filters it is registered with. Runtime-mutable: a
    /// dynamic `mqtt:` subscribe adds an entry here.
    pub subscriptions: RwLock<IngressSubscriptionSet>,

    /// Live supervisor state (shared with the supervisor task via `Arc`).
    pub supervisor_state: RwLock<SupervisorState>,

    /// The MQTT client handle, set by the supervisor on connect and cleared on
    /// disconnect. Wrapped in a Mutex so callers can snapshot client + state
    /// atomically for outbound publishes.
    pub client: Mutex<Option<rumqttc::AsyncClient>>,

    /// The Maximum Packet Size the broker advertised in the CONNACK of the
    /// connection whose client [`Self::client`] holds; `0` when it advertised
    /// none, a value MQTT 5.0 section 3.2.2.3.6 forbids the broker to send.
    /// Written and read only under the `client` lock, so a reader sees the
    /// value of the connection whose client it took.
    pub(crate) broker_max_packet_size: AtomicU32,

    /// The Receive Maximum the broker stated in the CONNACK of the connection
    /// whose client [`Self::client`] holds, [`DEFAULT_RECEIVE_MAXIMUM`] when it
    /// stated none. Written and read only under the `client` lock.
    pub(crate) broker_receive_max: AtomicU16,

    /// Whether the broker said, in the CONNACK of the connection whose client
    /// [`Self::client`] holds, that it supports Subscription Identifiers.
    /// Written only by the supervisor, under the `client` lock. Another task
    /// reads it only under that lock, so a SUBSCRIBE carries an identifier only
    /// on a connection whose broker accepts one; the supervisor, its writer,
    /// reads it on the inbound path without the lock.
    pub(crate) broker_sub_ids_available: AtomicBool,

    /// Whether the inbound path has logged, on the current connection, a
    /// PUBLISH that carried no Subscription Identifier from a broker that
    /// supports them. Cleared when a connection is installed. Written and read
    /// only by the supervisor task, so it takes no lock.
    pub(crate) missing_identifier_warned: AtomicBool,

    /// Counts the connections installed on this handle; the current one's
    /// number. Written and read only under the `client` lock, so a submitter
    /// knows which connection a publish went out on.
    pub(crate) connection_epoch: AtomicU64,

    /// Woken when a connection is installed and whenever the event loop writes
    /// a PUBLISH, which is when room may have freed on the request channel. A
    /// wake, not a state: "connected" is the `client` cell alone.
    pub egress_wake: Notify,

    /// Whether the inbound path has logged, on the current connection, a
    /// PUBLISH delivered at QoS 2. Cleared when a connection is installed.
    /// Written and read only by the supervisor task, so it takes no lock.
    pub(crate) qos2_inbound_warned: AtomicBool,

    /// Requests awaiting pkid binding (FIFO) — one entry per SUBSCRIBE the
    /// event loop has been handed and has not yet reported written, in the
    /// order it was handed them, for SubAck attribution.
    ///
    /// The supervisor task is its one writer: it is the only task that submits
    /// a SUBSCRIBE, and it pushes each filter in the same synchronous section
    /// as the submit, so the FIFO's order is the order the event loop takes the
    /// requests. The entries outlive a connection: a SUBSCRIBE the library
    /// keeps across a loss is replayed on a resumed session and binds its
    /// entry then, and its epoch tells it from one submitted on the connection
    /// that writes it. The FIFO is cleared when the broker reports no session
    /// and when a supervisor body starts. A `std::sync::Mutex`: every section
    /// is synchronous.
    pub pending_subscribes: std::sync::Mutex<VecDeque<PendingRequest>>,

    /// The UNSUBSCRIBE counterpart of [`Self::pending_subscribes`], under the
    /// same rules, for UNSUBACK attribution.
    pub pending_unsubscribes: std::sync::Mutex<VecDeque<PendingRequest>>,

    /// Filters another task asks the supervisor to settle: to bring the
    /// broker's session into agreement with [`Self::subscriptions`] for that
    /// filter. The supervisor takes them in its own loop before each poll and
    /// decides at its next free slot, against the held set at that moment,
    /// whether a SUBSCRIBE, an UNSUBSCRIBE or nothing is owed. Only the
    /// supervisor submits either request.
    ///
    /// A plain queue, not a channel: nothing awaits it. Callers push and the
    /// supervisor takes everything, both in short synchronous sections, so a
    /// `std::sync::Mutex` suffices and cannot be held across an await. It
    /// lives on the handle rather than in the supervisor task so a supervisor
    /// respawned after a panic drains the same queue. Cleared at each CONNACK,
    /// which settles every asserted filter.
    pub filter_requests: std::sync::Mutex<VecDeque<String>>,

    /// Notified after a push onto [`Self::filter_requests`], so a connected
    /// supervisor settles the filter without waiting for traffic.
    pub filter_wake: Notify,

    /// Inflight subscribes (pkid → filter), resolved at SubAck by `ack.pkid`.
    pub inflight_subscribes: Mutex<HashMap<u16, String>>,

    /// Inflight unsubscribes (pkid → filter and its number in
    /// [`Self::request_order`]), resolved at UNSUBACK by `ack.pkid`.
    pub(crate) inflight_unsubscribes: Mutex<HashMap<u16, (String, u64)>>,

    /// The order of the requests for each filter on the current connection,
    /// so an UNSUBACK that a later request for its filter has overtaken takes
    /// nothing out of the asserted set. Written only by the supervisor and
    /// cleared with the rest of the per-connection bookkeeping.
    pub(crate) request_order: std::sync::Mutex<RequestOrder>,

    /// What the broker answered for each topic filter on the *current* session,
    /// as the SubAck arm attributed the answers by pkid.
    ///
    /// Observation only: nothing in the process decides on this map. It answers
    /// "how did my SUBSCRIBE fare at the broker right now", which is a stronger
    /// fact than "the SUBSCRIBE was sent". An entry exists only while
    /// [`Self::subscriptions`] holds the filter, and the map is cleared with the
    /// rest of the per-connect bookkeeping, so it is empty across a session drop
    /// until the re-assert loop's SUBSCRIBEs are answered again.
    subscribe_outcomes: Mutex<HashMap<String, SubAckOutcome>>,

    /// The stop signal, observed by the supervisor and the egress reader.
    pub stop_tx: watch::Sender<bool>,

    /// The outer supervisor task, recorded by [`Self::set_supervisor`] right
    /// after the spawn and joined by [`Self::stop_and_join`]. A
    /// `tokio::sync::Mutex` because the join is awaited under it, so a second
    /// stopper queues behind the first and finds the session joined.
    supervisor: Mutex<SupervisorSlot>,

    /// The egress reader, recorded by [`crate::reader::start_egress_reader`].
    /// A `std::sync::Mutex`: every section is a synchronous take or store.
    pub(crate) egress_reader: std::sync::Mutex<ReaderSlot>,

    /// Whether an egress reader is running on this handle: set `true` before
    /// its task is spawned, and `false` by the task as its last act, after its
    /// last report has committed. Every wait for the reader's exit reads this.
    pub(crate) egress_reader_live: watch::Sender<bool>,

    /// Whether this session's stop asks the broker to discard the session:
    /// set by [`Self::end_session_and_join`] before it signals the stop.
    session_end_requested: AtomicBool,

    /// Whether the supervisor has written the DISCONNECT that ends the
    /// session at the broker.
    session_end_written: AtomicBool,

    /// Whether the supervisor's next CONNECT is a clean start: set for a
    /// client a reload adds, before its supervisor is spawned, and cleared at
    /// its first CONNACK. A body respawned before that CONNACK finds it still
    /// set.
    clean_start_pending: AtomicBool,
}

/// The supervisor recorded on a handle.
enum SupervisorSlot {
    /// No supervisor was spawned for this handle yet.
    NotSpawned,
    Running(tokio::task::JoinHandle<()>),
    /// The supervisor has been joined after the session's stop.
    Joined,
}

/// Poisoning message for [`MqttClientHandle::egress_reader`].
pub(crate) const EGRESS_READER_POISONED: &str =
    "egress reader slot poisoned: a holder panicked inside its critical section";

impl MqttClientHandle {
    pub fn new(
        config: Arc<MqttClientConfig>,
        subscriptions: Vec<IngressSubscription>,
        stop_tx: tokio::sync::watch::Sender<bool>,
    ) -> Arc<Self> {
        let set = IngressSubscriptionSet::new(subscriptions);
        for (i, entry) in set.iter().enumerate() {
            for other in set.iter().take(i) {
                if other.sub_id == entry.sub_id {
                    warn_shared_identifier(&config.identity.slug, entry, &other.topic_filter);
                }
            }
        }
        Arc::new(Self {
            config,
            subscriptions: RwLock::new(set),
            supervisor_state: RwLock::new(SupervisorState::Disconnected {
                last_error: None,
                next_attempt_at: Instant::now(),
            }),
            client: Mutex::new(None),
            broker_max_packet_size: AtomicU32::new(0),
            broker_receive_max: AtomicU16::new(DEFAULT_RECEIVE_MAXIMUM),
            broker_sub_ids_available: AtomicBool::new(true),
            missing_identifier_warned: AtomicBool::new(false),
            connection_epoch: AtomicU64::new(0),
            egress_wake: Notify::new(),
            qos2_inbound_warned: AtomicBool::new(false),
            pending_subscribes: std::sync::Mutex::new(VecDeque::new()),
            pending_unsubscribes: std::sync::Mutex::new(VecDeque::new()),
            filter_requests: std::sync::Mutex::new(VecDeque::new()),
            filter_wake: Notify::new(),
            inflight_subscribes: Mutex::new(HashMap::new()),
            inflight_unsubscribes: Mutex::new(HashMap::new()),
            request_order: std::sync::Mutex::new(RequestOrder::default()),
            subscribe_outcomes: Mutex::new(HashMap::new()),
            stop_tx,
            supervisor: Mutex::new(SupervisorSlot::NotSpawned),
            egress_reader: std::sync::Mutex::new(ReaderSlot::Never),
            egress_reader_live: watch::Sender::new(false),
            session_end_requested: AtomicBool::new(false),
            session_end_written: AtomicBool::new(false),
            clean_start_pending: AtomicBool::new(false),
        })
    }

    /// Make the supervisor's next CONNECT, and every one up to the first
    /// CONNACK, a clean start.
    pub(crate) fn require_clean_start(&self) {
        self.clean_start_pending.store(true, Ordering::SeqCst);
    }

    /// Whether the next CONNECT is a clean start this handle requires.
    pub(crate) fn clean_start_pending(&self) -> bool {
        self.clean_start_pending.load(Ordering::SeqCst)
    }

    /// Clear the clean-start requirement at a CONNACK; returns whether it was
    /// set.
    pub(crate) fn clear_clean_start(&self) -> bool {
        self.clean_start_pending.swap(false, Ordering::SeqCst)
    }

    /// Whether this session's stop asks the broker to discard the session.
    pub(crate) fn session_end_requested(&self) -> bool {
        self.session_end_requested.load(Ordering::SeqCst)
    }

    /// Record that the session-ending DISCONNECT was written.
    pub(crate) fn record_session_end_written(&self) {
        self.session_end_written.store(true, Ordering::SeqCst);
    }

    /// The longest a stop of this session waits for the broker: the client's
    /// `stop_wait_secs`.
    pub fn stop_wait(&self) -> Duration {
        Duration::from_secs(u64::from(self.config.identity.stop_wait_secs))
    }

    /// Read the current health label + last_error without holding a long-lived lock.
    pub async fn health_snapshot(&self) -> (ConnectorHealthLabel, Option<String>) {
        let state = self.supervisor_state.read().await;
        let label = state.health_label();
        let err = state.last_error().map(|s| s.to_string());
        (label, err)
    }

    /// Add a topic filter to this client's runtime subscription set if not already
    /// present, returning the `IngressSubscription` to assert (a fresh entry, or the
    /// existing one if the filter is already subscribed).
    ///
    /// A new entry takes the identifier derived from its filter. If the filter
    /// already exists, its existing entry is returned unchanged and the set is
    /// not grown (idempotent). The filter is asserted from then on. Does not
    /// touch the broker — the caller names the filter to the supervisor with
    /// [`Self::request_settle`].
    pub async fn add_subscription(&self, topic_filter: String) -> IngressSubscription {
        let mut set = self.subscriptions.write().await;
        let held = set.get(&topic_filter).is_some();
        let entry = set.add(topic_filter);
        if !held {
            self.warn_if_shared(&set, &entry);
        }
        entry
    }

    /// Remove the subscription for `topic_filter` from the runtime set if present,
    /// returning `true` if a matching entry was removed. The filter stays
    /// asserted until the broker answers its UNSUBSCRIBE. Does not touch the
    /// broker.
    ///
    /// The broker's answer for the filter goes with it, whether or not an entry
    /// was present: a filter being taken down is not a filter granted on this
    /// session, and no caller has to remember the pairing.
    pub async fn remove_subscription(&self, topic_filter: &str) -> bool {
        let removed = self.subscriptions.write().await.remove(topic_filter);
        self.forget_outcome(topic_filter).await;
        removed
    }

    /// Ask the supervisor to settle `topic_filter` at its next free slot, and
    /// wake it.
    pub fn request_settle(&self, topic_filter: String) {
        self.filter_requests
            .lock()
            .expect(FILTER_REQUESTS_POISONED)
            .push_back(topic_filter);
        self.filter_wake.notify_one();
    }

    /// Log at `warn` when `entry`, just placed in `set`, shares its identifier
    /// with another held filter.
    fn warn_if_shared(&self, set: &IngressSubscriptionSet, entry: &IngressSubscription) {
        for other in set.sharing_identifier(&entry.topic_filter) {
            warn_shared_identifier(&self.config.identity.slug, entry, &other);
        }
    }

    /// Record the broker's answer for `topic_filter` on the current session.
    ///
    /// A filter the runtime set no longer holds records nothing: an UNSUBSCRIBE
    /// and a SUBACK can cross, and an answer for a filter this process has taken
    /// down would outlive every reader's meaning of it.
    async fn record_outcome(&self, topic_filter: &str, outcome: SubAckOutcome) {
        let held = self.subscriptions.read().await.get(topic_filter).is_some();
        if !held {
            return;
        }
        self.subscribe_outcomes
            .lock()
            .await
            .insert(topic_filter.to_string(), outcome);
    }

    /// Record `topic_filter` as granted by the broker on the current session.
    pub async fn record_grant(&self, topic_filter: &str) {
        self.record_outcome(topic_filter, SubAckOutcome::Granted)
            .await;
    }

    /// Record the broker's refusal of `topic_filter`, in the SUBACK's own words.
    pub async fn record_refusal(&self, topic_filter: &str, reason: String) {
        self.record_outcome(topic_filter, SubAckOutcome::Refused(reason))
            .await;
    }

    /// Forget the broker's answer for `topic_filter`. Idempotent.
    pub async fn forget_outcome(&self, topic_filter: &str) {
        self.subscribe_outcomes.lock().await.remove(topic_filter);
    }

    /// Whether the broker has granted `topic_filter` on the current session.
    pub async fn is_granted(&self, topic_filter: &str) -> bool {
        matches!(
            self.subscribe_outcomes.lock().await.get(topic_filter),
            Some(SubAckOutcome::Granted)
        )
    }

    /// The broker's answer for `topic_filter`, or `None` if none has come back
    /// on this session.
    pub async fn subscribe_outcome(&self, topic_filter: &str) -> Option<SubAckOutcome> {
        self.subscribe_outcomes
            .lock()
            .await
            .get(topic_filter)
            .cloned()
    }

    /// Forget every answer — the per-connect clear, since a SUBACK belongs to
    /// the session that sent it.
    pub async fn clear_outcomes(&self) {
        self.subscribe_outcomes.lock().await.clear();
    }

    /// Signal the session to stop: sets the stop watch to `true`, whether or
    /// not anything is subscribed yet, so a task that subscribes later sees the
    /// stop. Idempotent.
    pub fn stop(&self) {
        self.stop_tx.send_replace(true);
    }

    /// Record the supervisor task's join handle.
    ///
    /// # Panics
    ///
    /// Panics if a handle is already recorded. One handle serves one client for
    /// as long as that client is registered; a second spawn against the same
    /// handle would leave two supervisors sharing one broker client id.
    pub async fn set_supervisor(&self, join: tokio::task::JoinHandle<()>) {
        let mut slot = self.supervisor.lock().await;
        assert!(
            matches!(*slot, SupervisorSlot::NotSpawned),
            "MqttClientHandle::set_supervisor called twice for client {:?}",
            self.config.identity.slug,
        );
        *slot = SupervisorSlot::Running(join);
    }

    /// Whether a supervisor has been recorded on this handle.
    pub async fn supervisor_spawned(&self) -> bool {
        !matches!(*self.supervisor.lock().await, SupervisorSlot::NotSpawned)
    }

    /// Signal the session to stop, wait for its egress reader to exit, then
    /// join its supervisor.
    ///
    /// The reader stops handing and waits up to [`Self::stop_wait`] for the
    /// outcomes of what it has in flight, reports them, and exits. The
    /// supervisor keeps the connection serviced until then, so the broker's
    /// answers arrive, and then disconnects under the same bound. Neither wait
    /// here has a timeout of its own: every wait a stopping session can be in
    /// is that bound, a bus call, or a select that races the stop signal.
    /// Returning means the broker connection is gone and the client id is free
    /// for a successor to take.
    ///
    /// Idempotent and shareable: a second caller, concurrent or later, queues
    /// on the supervisor slot behind the first and returns once the session is
    /// joined.
    ///
    /// # Panics
    ///
    /// Panics if no supervisor was ever recorded, or if the supervisor task
    /// ended in a way `tokio` reports as an error — the outer task catches its
    /// body's panics itself, so a join error here means the runtime tore the
    /// task down.
    pub async fn stop_and_join(&self) {
        self.stop();
        self.egress_reader_exited().await;
        let mut slot = self.supervisor.lock().await;
        match std::mem::replace(&mut *slot, SupervisorSlot::Joined) {
            SupervisorSlot::NotSpawned => panic!(
                "MqttClientHandle::stop_and_join with no supervisor recorded for client {:?}",
                self.config.identity.slug,
            ),
            SupervisorSlot::Running(join) => {
                if let Err(e) = join.await {
                    panic!(
                        "MQTT supervisor for client {:?} could not be joined: {e}",
                        self.config.identity.slug,
                    );
                }
                // The one place a session's stop has completed, once per
                // session: its reader has exited and its supervisor has ended.
            }
            SupervisorSlot::Joined => {}
        }
    }

    /// [`Self::stop_and_join`], asking the broker to discard the session: a
    /// connected client whose `session_expiry_secs` is above 0 ends its
    /// connection with a DISCONNECT carrying Session Expiry Interval 0. A
    /// client declared with expiry 0 sends the plain DISCONNECT, since the
    /// broker discards its session when the connection closes.
    ///
    /// A session the stop could not end that way — no connection at the stop,
    /// or the DISCONNECT not written — stays at the broker until it expires,
    /// and that is logged at `warn` with the expiry.
    ///
    /// # Panics
    ///
    /// As [`Self::stop_and_join`].
    pub async fn end_session_and_join(&self) {
        self.session_end_requested.store(true, Ordering::SeqCst);
        self.stop_and_join().await;
        let expiry_secs = self.config.identity.session_expiry_secs;
        if expiry_secs > 0 && !self.session_end_written.load(Ordering::SeqCst) {
            tracing::warn!(
                client = %self.config.identity.slug,
                session_expiry_secs = expiry_secs,
                "MQTT session not ended at the broker: no session-ending DISCONNECT was written; \
                 the broker keeps the session until its expiry",
            );
        }
    }

    /// Retire the egress reader and wait for it to exit, leaving the session
    /// running. The reader hands nothing more, waits up to
    /// [`Self::stop_wait`] for the outcomes of what it has in flight, reports
    /// what it had outstanding and exits, leaving its position where it is. A
    /// reader already ending, or ended, for the session's stop is waited for
    /// in the same way. What the channel holds unread is the hub's to report
    /// when it closes the client's channel, whether or not a reader was still
    /// running.
    ///
    /// # Panics
    ///
    /// If no reader was started on this handle or it was already retired, or
    /// if the reader has exited and the session's stop is not set.
    pub async fn retire_egress_reader(&self) {
        let retire = self.take_egress_reader().unwrap_or_else(|| {
            panic!(
                "MqttClientHandle::retire_egress_reader with no egress reader running for client \
                 {:?}",
                self.config.identity.slug,
            )
        });
        if retire.send(()).is_err() {
            assert!(
                *self.stop_tx.borrow(),
                "the egress reader of MQTT client {:?} exited without being retired or stopped",
                self.config.identity.slug,
            );
        }
        self.egress_reader_exited().await;
    }

    /// Whether an egress reader is running on this handle: started, and not
    /// yet exited for a retire or the session's stop.
    #[cfg(any(test, feature = "testutils"))]
    pub fn egress_reader_running(&self) -> bool {
        *self.egress_reader_live.borrow()
    }

    /// Take the running reader's retire sender out of its slot, leaving the
    /// slot `Gone`. `None` when no reader is running.
    fn take_egress_reader(&self) -> Option<tokio::sync::oneshot::Sender<()>> {
        let mut slot = self.egress_reader.lock().expect(EGRESS_READER_POISONED);
        match std::mem::replace(&mut *slot, ReaderSlot::Gone) {
            ReaderSlot::Running { retire } => Some(retire),
            previous @ (ReaderSlot::Never | ReaderSlot::Gone) => {
                *slot = previous;
                None
            }
        }
    }

    /// Return once no egress reader is running on this handle: at once when
    /// none was started or the reader has already exited.
    pub(crate) async fn egress_reader_exited(&self) {
        self.egress_reader_live
            .subscribe()
            .wait_for(|live| !*live)
            .await
            .expect("the handle owns the reader-live sender and outlives this wait");
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use brenn_lib::mqtt::test_support::test_client_config;

    fn make_handle(client: &str) -> Arc<MqttClientHandle> {
        let (tx, _rx) = tokio::sync::watch::channel(false);
        MqttClientHandle::new(Arc::new(test_client_config(client)), vec![], tx)
    }

    /// `ConnectorHealthLabel::wire_str` must equal the serde `lowercase` form for
    /// every variant, so the single-source `wire_str` cannot drift from the serde
    /// derive.
    #[test]
    fn wire_str_matches_serde() {
        for label in [
            ConnectorHealthLabel::Connected,
            ConnectorHealthLabel::Disconnected,
            ConnectorHealthLabel::Failed,
        ] {
            let serde_form = serde_json::to_value(label)
                .expect("serialize")
                .as_str()
                .expect("string")
                .to_string();
            assert_eq!(label.wire_str(), serde_form);
        }
    }

    #[test]
    fn health_label_mapping() {
        let connected = SupervisorState::Connected;
        assert_eq!(connected.health_label(), ConnectorHealthLabel::Connected);

        let disconnected = SupervisorState::Disconnected {
            last_error: Some("timeout".to_string()),
            next_attempt_at: Instant::now(),
        };
        assert_eq!(
            disconnected.health_label(),
            ConnectorHealthLabel::Disconnected
        );

        let connecting = SupervisorState::Connecting {
            since: Instant::now(),
        };
        assert_eq!(
            connecting.health_label(),
            ConnectorHealthLabel::Disconnected
        );
    }

    #[test]
    fn health_label_failed_state() {
        let failed = SupervisorState::Failed {
            reason: "test authoritative failure".into(),
        };
        assert_eq!(failed.health_label(), ConnectorHealthLabel::Failed);
        assert_eq!(failed.last_error(), Some("test authoritative failure"));
    }

    #[tokio::test]
    async fn add_subscription_appends_with_the_filters_identifier() {
        let handle = make_handle("broker");
        let added = handle.add_subscription("sensors/#".into()).await;
        assert_eq!(added.topic_filter, "sensors/#");
        assert_eq!(added.sub_id, subscription_identifier("sensors/#"));
        let added2 = handle.add_subscription("home/#".into()).await;
        assert_eq!(added2.sub_id, subscription_identifier("home/#"));
        assert_eq!(handle.subscriptions.read().await.len(), 2);
    }

    fn entry(topic_filter: &str) -> IngressSubscription {
        IngressSubscription::new(topic_filter)
    }

    fn id(topic_filter: &str) -> usize {
        subscription_identifier(topic_filter) as usize
    }

    /// Two filters with one identifier, found by search; the collision tests
    /// assert they collide before relying on it.
    const COLLIDING: (&str, &str) = ("k/+/84471", "k/282/+");

    #[test]
    fn a_filter_takes_its_derived_identifier_whatever_else_was_added() {
        let mut set = IngressSubscriptionSet::new(vec![]);
        assert_eq!(set.add("a/#".into()).sub_id, subscription_identifier("a/#"));
        assert_eq!(set.add("a/b".into()).sub_id, subscription_identifier("a/b"));
        assert!(set.remove("a/b"));
        assert!(!set.remove("a/b"));
        assert_eq!(set.add("a/c".into()).sub_id, subscription_identifier("a/c"));
        assert_eq!(
            set.add("a/b".into()).sub_id,
            subscription_identifier("a/b"),
            "a filter added back takes the identifier it had",
        );

        let mut reordered = IngressSubscriptionSet::new(vec![]);
        for filter in ["a/c", "a/b", "a/#"] {
            reordered.add(filter.into());
        }
        for filter in ["a/#", "a/b", "a/c"] {
            assert_eq!(
                reordered.get(filter).map(|e| e.sub_id),
                set.get(filter).map(|e| e.sub_id),
                "{filter} has one identifier whatever order the set was built in",
            );
        }
    }

    #[test]
    fn a_clone_equals_the_original() {
        let mut set = IngressSubscriptionSet::new(vec![entry("a/#")]);
        set.add("a/b".into());
        assert_eq!(set.clone(), set);
    }

    /// The asserted filters: seeded from the held ones, grown by `add`, kept
    /// by `remove`, carried by a clone, and made the held ones by a reset.
    #[test]
    fn the_asserted_filters_follow_the_held_ones_and_outlive_a_removal() {
        let mut set = IngressSubscriptionSet::new(vec![entry("a/1"), entry("a/2")]);
        assert!(set.is_asserted("a/1") && set.is_asserted("a/2"));
        set.add("a/3".into());
        assert!(set.is_asserted("a/3"));
        assert!(set.remove("a/2"));
        assert!(
            set.is_asserted("a/2"),
            "a removal leaves the filter asserted"
        );
        assert_eq!(set.withdrawn(), ["a/2"]);

        let clone = set.clone();
        assert_eq!(clone.withdrawn(), ["a/2"]);
        assert_eq!(clone, set);

        assert!(
            !set.withdraw_asserted("a/1"),
            "a held filter stays asserted"
        );
        set.reset_asserted();
        assert!(!set.is_asserted("a/2"));
        assert!(set.withdrawn().is_empty());
        assert!(set.is_asserted("a/1") && set.is_asserted("a/3"));

        set.mark_asserted("a/9");
        assert_eq!(set.withdrawn(), ["a/9"]);
        assert!(set.withdraw_asserted("a/9"));
        assert!(set.withdrawn().is_empty());
    }

    #[test]
    fn filters_for_resolves_known_identifiers_and_reports_unknown_ones() {
        let set = IngressSubscriptionSet::new(vec![entry("a/#"), entry("a/b")]);
        let unknown = [9_usize, 10];
        for u in unknown {
            assert!(u != id("a/#") && u != id("a/b"));
        }
        let resolved = |ids: &[usize]| set.filters_for(ids, "a/b");
        assert_eq!(
            resolved(&[id("a/b")]),
            ResolvedIdentifiers {
                filters: vec!["a/b".into()],
                unknown: vec![],
            },
        );
        assert_eq!(
            resolved(&[id("a/b"), id("a/#")]),
            ResolvedIdentifiers {
                filters: vec!["a/b".into(), "a/#".into()],
                unknown: vec![],
            },
            "packet order is kept",
        );
        assert_eq!(
            resolved(&[id("a/#"), 9, id("a/b")]),
            ResolvedIdentifiers {
                filters: vec!["a/#".into(), "a/b".into()],
                unknown: vec![9],
            },
        );
        assert_eq!(
            resolved(&[9, 10]),
            ResolvedIdentifiers {
                filters: vec![],
                unknown: vec![9, 10],
            },
        );
        assert_eq!(
            resolved(&[id("a/b"), id("a/b")]),
            ResolvedIdentifiers {
                filters: vec!["a/b".into()],
                unknown: vec![],
            },
            "a repeated identifier yields its filter once",
        );
    }

    #[test]
    fn two_filters_sharing_an_identifier_are_both_held_and_resolved_by_cover() {
        let (wide, narrow) = COLLIDING;
        assert_eq!(
            subscription_identifier(wide),
            subscription_identifier(narrow),
            "the fixture pair must collide",
        );
        let shared = id(wide);

        let set = IngressSubscriptionSet::new(vec![entry(wide), entry(narrow)]);
        assert_eq!(set.len(), 2);
        let mut added = IngressSubscriptionSet::new(vec![entry(wide)]);
        added.add(narrow.into());
        assert_eq!(added.len(), 2);
        assert_eq!(added.sharing_identifier(narrow), vec![wide.to_string()]);
        assert_eq!(added.sharing_identifier(wide), vec![narrow.to_string()]);
        assert!(added.sharing_identifier("a/b").is_empty());

        let filters = |topic: &str| set.filters_for(&[shared], topic).filters;
        assert_eq!(
            filters("k/282/84471"),
            vec![wide.to_string(), narrow.to_string()],
            "a topic both cover resolves to both",
        );
        assert_eq!(
            filters("k/282/1"),
            vec![narrow.to_string()],
            "a topic one covers resolves to that one",
        );
        assert_eq!(
            filters("x/y"),
            vec![wide.to_string(), narrow.to_string()],
            "a topic neither covers resolves to both, for the router's cover check",
        );
    }

    #[test]
    #[should_panic(expected = "appears twice")]
    fn new_panics_on_a_duplicate_filter() {
        IngressSubscriptionSet::new(vec![entry("a/#"), entry("a/#")]);
    }

    #[test]
    #[should_panic(expected = "not derived from its filter")]
    fn new_panics_on_an_identifier_not_derived_from_its_filter() {
        IngressSubscriptionSet::new(vec![IngressSubscription {
            topic_filter: "a/#".into(),
            sub_id: 1,
        }]);
    }

    #[tokio::test]
    async fn add_subscription_is_idempotent_for_existing_filter() {
        let handle = make_handle("broker");
        handle.add_subscription("home/+/state".into()).await;
        let added = handle.add_subscription("home/+/state".into()).await;
        assert_eq!(added, entry("home/+/state"));
        assert_eq!(handle.subscriptions.read().await.len(), 1);
    }

    /// Removing a filter reports whether an entry went, and takes the broker's
    /// answer with it either way — including the second call, where no entry is
    /// present.
    #[tokio::test]
    async fn remove_subscription_reports_match() {
        let handle = make_handle("broker");
        handle.add_subscription("sensors/#".into()).await;
        handle.record_grant("sensors/#").await;
        assert!(handle.is_granted("sensors/#").await);

        assert!(handle.remove_subscription("sensors/#").await);
        assert!(!handle.is_granted("sensors/#").await);

        assert!(!handle.remove_subscription("sensors/#").await);
        assert!(!handle.is_granted("sensors/#").await);
    }

    /// An answer is recorded only for a filter the runtime set holds: with the
    /// filter gone, the SubAck arm's `record_*` is a no-op rather than an entry
    /// no reader can retire.
    #[tokio::test]
    async fn an_outcome_for_an_unheld_filter_is_not_recorded() {
        let handle = make_handle("broker");
        handle.record_grant("sensors/#").await;
        handle
            .record_refusal("sensors/#", "Failure".to_string())
            .await;
        assert_eq!(handle.subscribe_outcome("sensors/#").await, None);

        handle.add_subscription("sensors/#".into()).await;
        handle.record_grant("sensors/#").await;
        assert_eq!(
            handle.subscribe_outcome("sensors/#").await,
            Some(SubAckOutcome::Granted)
        );
    }

    fn puback(reason: rumqttc::PubAckReason, reason_string: Option<&str>) -> rumqttc::PubAck {
        rumqttc::PubAck {
            pkid: 1,
            reason,
            properties: reason_string.map(|r| rumqttc::PubAckProperties {
                reason_string: Some(r.to_string()),
                user_properties: vec![],
            }),
        }
    }

    /// Every row of the notice table: what is delivered, what is rejected
    /// and with which words, and what means the connection ended first.
    #[test]
    fn notice_outcome_maps_every_resolution() {
        use rumqttc::{
            PubAckReason, PubComp, PubCompProperties, PubCompReason, PubRec, PubRecProperties,
            PubRecReason, PublishNoticeError as E, PublishResult as R,
        };
        let rejected = |reason: &str, code: u8| NoticeOutcome::Rejected {
            reason: reason.to_string(),
            code,
        };

        assert_eq!(notice_outcome(Ok(R::Qos0Flushed)), NoticeOutcome::Delivered);
        for reason in [PubAckReason::Success, PubAckReason::NoMatchingSubscribers] {
            assert_eq!(
                notice_outcome(Ok(R::Qos1(puback(reason, None)))),
                NoticeOutcome::Delivered
            );
        }
        assert_eq!(
            notice_outcome(Ok(R::Qos1(puback(PubAckReason::NotAuthorized, None)))),
            rejected("NotAuthorized", 135),
            "with no reason string the reason code's name is the reason",
        );
        assert_eq!(
            notice_outcome(Ok(R::Qos1(puback(
                PubAckReason::QuotaExceeded,
                Some("over quota")
            )))),
            rejected("over quota", 151),
        );

        let comp = |reason, reason_string: Option<&str>| PubComp {
            pkid: 2,
            reason,
            properties: reason_string.map(|r| PubCompProperties {
                reason_string: Some(r.to_string()),
                user_properties: vec![],
            }),
        };
        assert_eq!(
            notice_outcome(Ok(R::Qos2Completed(comp(PubCompReason::Success, None)))),
            NoticeOutcome::Delivered
        );
        assert_eq!(
            notice_outcome(Ok(R::Qos2Completed(comp(
                PubCompReason::PacketIdentifierNotFound,
                None
            )))),
            rejected("PacketIdentifierNotFound", 146),
        );
        assert_eq!(
            notice_outcome(Ok(R::Qos2Completed(comp(
                PubCompReason::PacketIdentifierNotFound,
                Some("lost it")
            )))),
            rejected("lost it", 146),
        );

        let rec = PubRec {
            pkid: 3,
            reason: PubRecReason::NotAuthorized,
            properties: Some(PubRecProperties {
                reason_string: Some("denied".to_string()),
                user_properties: vec![],
            }),
        };
        assert_eq!(
            notice_outcome(Ok(R::Qos2PubRecRejected(rec))),
            rejected("denied", 135)
        );

        for e in [E::Recv, E::SessionReset, E::Qos0NotFlushed] {
            assert_eq!(notice_outcome(Err(e)), NoticeOutcome::SessionEnded);
        }
    }

    #[test]
    #[should_panic(expected = "Brenn sets no topic alias")]
    fn a_topic_alias_replay_failure_panics() {
        notice_outcome(Err(
            rumqttc::PublishNoticeError::TopicAliasReplayUnavailable(1),
        ));
    }

    #[test]
    #[should_panic(expected = "never asks for a completion check")]
    fn a_completion_check_error_panics() {
        notice_outcome(Err(rumqttc::PublishNoticeError::V5PubAck(
            rumqttc::PubAckReason::NotAuthorized,
        )));
    }

    #[test]
    fn stop_fires_watch_channel() {
        let (stop_tx, mut stop_rx) = tokio::sync::watch::channel(false);
        let handle = MqttClientHandle::new(Arc::new(test_client_config("broker")), vec![], stop_tx);
        assert!(!*stop_rx.borrow_and_update());
        handle.stop();
        assert!(
            *stop_rx.borrow(),
            "stop() must set the watch channel to true"
        );
    }
}
