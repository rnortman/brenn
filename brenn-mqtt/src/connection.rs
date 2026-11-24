//! MQTT connection supervisor: one per active `[[mqtt_client]]` session.
//!
//! Each supervisor runs in a dedicated `tokio::spawn` task. It owns one
//! `rumqttc::AsyncClient` + `EventLoop` for its life and keeps polling that
//! loop across lost connections, so the library resumes the client's half of a
//! broker session that survived; it reconnects with jittered exponential
//! backoff, settles every filter with the broker on every successful connect —
//! a SUBSCRIBE for each held, and an UNSUBSCRIBE for each asserted and no
//! longer held when the broker kept the session — records the
//! facts of each connection a publish needs, and routes every inbound PUBLISH
//! to the bridge router. One session per
//! client carries both the publish and the ingress-delivery paths.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use rumqttc::mqttbytes::QoS;
use rumqttc::{
    AsyncClient, ConnectReturnCode, ConnectionError, DisconnectReasonCode, Event, Filter, Incoming,
    MqttOptions, Outgoing, RetainForwardRule, StateError, SubscribeProperties, SubscribeReasonCode,
    TlsConfiguration, Transport,
};
use rustls_pki_types::pem::PemObject as _;

use crate::payload::InboundPayload;
use crate::service::{InboundRouting, MqttEventRouter, MqttService, SessionFault, qos_of};
use crate::state::{
    DEFAULT_RECEIVE_MAXIMUM, FILTER_REQUESTS_POISONED, IngressSubscription, IngressSubscriptionSet,
    MqttClientHandle, PENDING_SUBSCRIBES_POISONED, PENDING_UNSUBSCRIBES_POISONED, PendingRequest,
    REQUEST_ORDER_POISONED, SupervisorState,
};
use brenn_lib::mqtt::config::{MqttClientConfig, ResolvedMqttIngressChannel, TlsVersionMin};

// How long the watchdog waits before respawning a panicked supervisor body. A
// stop ends the wait at once.
const RESPAWN_PAUSE: Duration = Duration::from_secs(1);

// What the SubAck arm logs as the filter when no pending SUBSCRIBE was bound to
// the acked pkid — a broker resending a SubAck from a session brenn did not
// issue the SUBSCRIBE on.
const UNATTRIBUTED_FILTER: &str = "<unknown>";

// ---------------------------------------------------------------------------
// Union subscription set
// ---------------------------------------------------------------------------

/// Collapse one client's ingress channels into its deduplicated set of topic
/// filters.
///
/// Distinct filter *strings* on one client yield distinct SUBSCRIBEs, each with
/// the identifier derived from its filter ([`IngressSubscription::new`]).
/// Entries are in first-seen order. `channels` may include channels for other
/// clients; only those whose `client_slug == client_slug` are considered.
pub fn union_subscriptions(
    client_slug: &str,
    channels: &[ResolvedMqttIngressChannel],
) -> Vec<IngressSubscription> {
    let mut subs: Vec<IngressSubscription> = Vec::new();
    for channel in channels {
        if channel.client_slug != client_slug {
            continue;
        }
        if !subs.iter().any(|s| s.topic_filter == channel.topic) {
            subs.push(IngressSubscription::new(channel.topic.clone()));
        }
    }
    subs
}

/// The `Filter` and properties of the broker SUBSCRIBE for one ingress
/// subscription at `qos`: the `OnEverySubscribe` retain rule, so the broker
/// re-delivers the current retained message on every (re)subscribe, and — when
/// `with_identifier`, which is whether the connection's broker supports
/// Subscription Identifiers — the subscription's MQTT 5 Subscription
/// Identifier.
///
/// # Panics
///
/// If `qos` is above 1, which config validation refuses for the client named
/// by `client_slug`; reaching it is a host bug.
fn subscribe_request(
    client_slug: &str,
    sub: &IngressSubscription,
    qos: u8,
    with_identifier: bool,
) -> (Filter, SubscribeProperties) {
    let rumq_qos = qos_of(qos).filter(|_| qos <= 1).unwrap_or_else(|| {
        panic!(
            "client {client_slug:?}: subscription {:?} with QoS {qos}, which config \
             validation refuses",
            sub.topic_filter
        )
    });
    let filter = Filter {
        path: sub.topic_filter.clone(),
        qos: rumq_qos,
        nolocal: false,
        preserve_retain: false,
        retain_forward_rule: RetainForwardRule::OnEverySubscribe,
    };
    let props = SubscribeProperties {
        id: with_identifier.then_some(sub.sub_id as usize),
        user_properties: vec![],
    };
    (filter, props)
}

/// Why the MQTT library would refuse a SUBSCRIBE for `topic_filter`, or `None`
/// when it would take it.
pub(crate) fn unsendable_filter(topic_filter: &str) -> Option<String> {
    (!rumqttc::mqttbytes::valid_filter(topic_filter))
        .then(|| format!("invalid topic filter {topic_filter:?}"))
}

/// What the supervisor did with one request it tried to submit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SubscribeSend {
    /// The request is on the session's request channel.
    Sent,
    /// The request channel had no free slot (or its receiver is gone; the send
    /// cannot tell the two apart). Nothing was queued or recorded.
    ChannelFull,
}

/// Submit a single broker SUBSCRIBE for one ingress subscription onto the
/// supervisor's `AsyncClient` without waiting for room on its request channel.
///
/// Only the supervisor calls this. It is the only task that polls the event
/// loop, and only a poll frees a slot, so it must never wait for one: on
/// [`SubscribeSend::ChannelFull`] its push loop keeps the filter and polls. The
/// filter is pushed onto `handle.pending_subscribes`, with the current
/// connection epoch, in the same synchronous section as the submit, and no
/// other task submits a SUBSCRIBE, so the FIFO's order is the order the event
/// loop takes SUBSCRIBE requests and the `Outgoing::Subscribe` arm binds pkid →
/// filter correctly. A sent request is counted in `handle.request_order`.
///
/// A filter rumqttc would reject is refused up front, without touching the
/// FIFO: rumqttc reports it with the same error as a full channel, and a
/// retry loop must not mistake it for one.
///
/// The SUBSCRIBE requests the `qos` of `handle`'s configuration.
/// `with_identifier` is whether the current connection accepts a
/// Subscription Identifier; see [`subscribe_request`].
///
/// Returns `Err(message)` for a filter rumqttc would refuse; there is no
/// SUBACK wait.
///
/// # Panics
///
/// As [`submit_request`].
fn try_assert_ingress_subscription(
    handle: &MqttClientHandle,
    client: &AsyncClient,
    sub: &IngressSubscription,
    with_identifier: bool,
) -> Result<SubscribeSend, String> {
    if let Some(e) = unsendable_filter(&sub.topic_filter) {
        return Err(e);
    }
    let (filter, props) = subscribe_request(
        &handle.config.identity.slug,
        sub,
        handle.config.identity.qos,
        with_identifier,
    );
    Ok(submit_request(
        handle,
        RequestKind::Subscribe,
        &sub.topic_filter,
        || client.try_subscribe_many_with_properties(std::iter::once(filter), props),
    ))
}

/// Submit a single broker UNSUBSCRIBE for `topic_filter` without waiting for
/// room on the request channel: the UNSUBSCRIBE counterpart of
/// [`try_assert_ingress_subscription`], under the same rules, recording the
/// filter on `handle.pending_unsubscribes`. There is no UNSUBACK wait.
///
/// # Panics
///
/// As [`submit_request`].
fn try_withdraw_ingress_subscription(
    handle: &MqttClientHandle,
    client: &AsyncClient,
    topic_filter: &str,
) -> SubscribeSend {
    submit_request(handle, RequestKind::Unsubscribe, topic_filter, || {
        client.try_unsubscribe(topic_filter)
    })
}

/// Which filter request a FIFO entry, a submit or a written packet is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RequestKind {
    Subscribe,
    Unsubscribe,
}

impl RequestKind {
    /// The packet's name, for logs.
    fn packet(self) -> &'static str {
        match self {
            Self::Subscribe => "SUBSCRIBE",
            Self::Unsubscribe => "UNSUBSCRIBE",
        }
    }

    /// The FIFO that binds this kind's packet identifiers to filters, locked.
    fn fifo(
        self,
        handle: &MqttClientHandle,
    ) -> std::sync::MutexGuard<'_, std::collections::VecDeque<PendingRequest>> {
        match self {
            Self::Subscribe => handle
                .pending_subscribes
                .lock()
                .expect(PENDING_SUBSCRIBES_POISONED),
            Self::Unsubscribe => handle
                .pending_unsubscribes
                .lock()
                .expect(PENDING_UNSUBSCRIBES_POISONED),
        }
    }
}

/// Submit one `kind` request for `topic_filter` through `submit`, a
/// non-blocking rumqttc call, with the filter pushed onto `kind`'s FIFO, tagged
/// with the current connection epoch, in the same synchronous section. A sent
/// request is counted in `handle.request_order`; a refused one leaves the FIFO
/// as it was.
///
/// # Panics
///
/// If `submit` fails other than by [`rumqttc::ClientError::TryRequest`]:
/// rumqttc's non-blocking SUBSCRIBE and UNSUBSCRIBE calls report every refusal
/// — a full channel, a gone receiver, an invalid filter — that way, so any
/// other error means the library no longer behaves as this binding reads it.
fn submit_request(
    handle: &MqttClientHandle,
    kind: RequestKind,
    topic_filter: &str,
    submit: impl FnOnce() -> Result<(), rumqttc::ClientError>,
) -> SubscribeSend {
    let mut pending = kind.fifo(handle);
    pending.push_back(PendingRequest {
        topic_filter: topic_filter.to_string(),
        epoch: handle.connection_epoch.load(Ordering::Relaxed),
    });
    match submit() {
        Ok(()) => {
            count_request(handle, topic_filter);
            SubscribeSend::Sent
        }
        Err(rumqttc::ClientError::TryRequest(_)) => {
            pending.pop_back();
            SubscribeSend::ChannelFull
        }
        Err(e) => panic!(
            "mqtt client {}: rumqttc refused a {} for {topic_filter:?} with an error its \
             non-blocking submit does not report: {e:?}",
            handle.config.identity.slug,
            kind.packet(),
        ),
    }
}

/// Take the filter a `kind` request the library reported written as `pkid`
/// was submitted for: the head of `kind`'s FIFO. An empty FIFO is logged and
/// gives `None`.
fn take_written_request(
    handle: &MqttClientHandle,
    kind: RequestKind,
    pkid: u16,
) -> Option<PendingRequest> {
    let entry = kind.fifo(handle).pop_front();
    if entry.is_none() {
        tracing::warn!(
            client = %handle.config.identity.slug,
            pkid,
            packet = kind.packet(),
            "request written with no pending filter — possible session resend",
        );
    }
    entry
}

/// Count a request for `filter` seen written on a live connection and return
/// its number. One the library replays from an earlier connection — its FIFO
/// entry carries an older `epoch` — names its filter for settling again, since
/// the held set may have changed since it was submitted.
fn written_on_live_connection(handle: &MqttClientHandle, filter: &str, epoch: u64) -> u64 {
    let seq = count_request(handle, filter);
    if replayed(handle, epoch) {
        handle.request_settle(filter.to_string());
    }
    seq
}

/// Log a `kind` request for `filter` seen written on a lost connection: no
/// answer to it will be read.
fn written_on_lost_connection(
    handle: &MqttClientHandle,
    kind: RequestKind,
    pkid: u16,
    filter: &str,
) {
    tracing::debug!(
        client = %handle.config.identity.slug,
        pkid,
        topic = %filter,
        packet = kind.packet(),
        "request written on a lost connection; it will not be answered",
    );
}

/// Count one request for `topic_filter` on the current connection and return
/// its number.
fn count_request(handle: &MqttClientHandle, topic_filter: &str) -> u64 {
    handle
        .request_order
        .lock()
        .expect(REQUEST_ORDER_POISONED)
        .bump(topic_filter)
}

/// What settling one filter did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Settled {
    /// A SUBSCRIBE was submitted: the filter is held.
    Subscribed,
    /// An UNSUBSCRIBE was submitted: the filter is asserted and not held.
    Unsubscribed,
    /// Nothing is owed: the filter is neither held nor asserted, or it is not
    /// held and the library could never have subscribed it.
    Nothing,
    /// The request channel had no free slot; nothing was submitted.
    ChannelFull,
}

/// Settle `topic_filter`: submit what makes the broker's session agree with
/// `set` for it, deciding against `set` as it is now. Only the supervisor
/// calls this, with `set` locked for writing, so the decision and the submit
/// are one step against the set.
///
/// A filter that is not held and that the library would refuse is taken out
/// of the asserted filters without a packet: no SUBSCRIBE for it was ever
/// written, so the broker cannot hold it.
fn settle_filter(
    handle: &MqttClientHandle,
    client: &AsyncClient,
    set: &mut IngressSubscriptionSet,
    topic_filter: &str,
    with_identifier: bool,
) -> Result<Settled, String> {
    if let Some(sub) = set.get(topic_filter) {
        return Ok(
            match try_assert_ingress_subscription(handle, client, sub, with_identifier)? {
                SubscribeSend::Sent => Settled::Subscribed,
                SubscribeSend::ChannelFull => Settled::ChannelFull,
            },
        );
    }
    if !set.is_asserted(topic_filter) {
        return Ok(Settled::Nothing);
    }
    if unsendable_filter(topic_filter).is_some() {
        set.withdraw_asserted(topic_filter);
        return Ok(Settled::Nothing);
    }
    Ok(
        match try_withdraw_ingress_subscription(handle, client, topic_filter) {
            SubscribeSend::Sent => Settled::Unsubscribed,
            SubscribeSend::ChannelFull => Settled::ChannelFull,
        },
    )
}

// ---------------------------------------------------------------------------
// Build MqttOptions from broker config
// ---------------------------------------------------------------------------

/// The single per-client session client id: `brenn:<client-slug>`. Formatted in
/// one place so the options builder and the backoff-seed derivation cannot drift.
pub(crate) fn client_id(broker: &MqttClientConfig) -> String {
    client_id_of_slug(&broker.identity.slug)
}

/// [`client_id`] from the slug alone, for a caller that holds no config — the
/// test fixtures naming the id the broker logs for a session.
pub(crate) fn client_id_of_slug(slug: &str) -> String {
    format!("brenn:{slug}")
}

/// Build `MqttOptions` from broker config, using a pre-built `transport`.
///
/// `transport` is accepted as a parameter (rather than built here) so callers can
/// construct it once per supervisor lifetime and clone the cheap `Arc<ClientConfig>`
/// wrapper on every reconnect attempt, avoiding repeated disk I/O and crypto-provider
/// allocation on the reconnect hot path.
fn build_mqtt_options(broker: &MqttClientConfig, transport: Transport) -> MqttOptions {
    let mut opts = MqttOptions::new(
        client_id(broker),
        (broker.identity.host.clone(), broker.identity.port),
    );
    opts.set_clean_start(false);
    opts.set_session_expiry_interval(Some(broker.identity.session_expiry_secs));

    if let Some(secs) = broker.identity.keepalive_secs {
        // rumqttc-next set_keep_alive takes u16 seconds (not Duration).
        let secs = secs.max(5) as u16; // rumqttc requires >= 5s
        opts.set_keep_alive(secs);
    }

    if let Some(ref username) = broker.identity.username {
        let password = broker.password.as_deref().unwrap_or("").to_owned();
        opts.set_credentials(username.clone(), password);
    }

    // Inbound payload cap — advertised to broker in CONNECT. Validated to fit
    // `u32` at config resolution, so no cast is needed here.
    opts.set_max_packet_size(Some(broker.identity.inbound_payload_cap_bytes));

    // A publish is sent exactly as submitted: the egress reader checks the
    // packet's size against the broker's maximum before submitting it, and an
    // alias the event loop added afterwards would make the packet larger than
    // the size checked.
    opts.set_auto_topic_aliases(false);

    // TLS transport pre-built by the caller (once per supervisor lifetime).
    opts.set_transport(transport);

    // Last Will.
    if let Some(ref lw) = broker.identity.last_will {
        let rumq_qos = qos_of(lw.qos).unwrap_or_else(|| {
            panic!(
                "last will with QoS {}, which config validation refuses",
                lw.qos
            )
        });
        let will = rumqttc::LastWill::new(
            lw.topic.clone(),
            lw.payload.as_bytes().to_vec(),
            rumq_qos,
            lw.retain,
            None,
        );
        opts.set_last_will(will);
    }

    opts
}

/// Load PEM-encoded CA certificates from raw bytes into a `RootCertStore`.
///
/// Returns `(store, certs_added)`. Malformed or un-addable entries are skipped
/// with a `WARN` log; the count reflects only successfully-added certificates.
fn load_ca_cert_pem(broker_slug: &str, pem_bytes: &[u8]) -> (rustls::RootCertStore, usize) {
    let mut store = rustls::RootCertStore::empty();
    let mut added = 0usize;
    let cursor = std::io::Cursor::new(pem_bytes);
    for (idx, cert_result) in rustls_pki_types::CertificateDer::pem_reader_iter(cursor).enumerate()
    {
        match cert_result {
            Err(e) => {
                tracing::warn!(
                    broker = %broker_slug,
                    cert_index = idx,
                    error = %e,
                    "ca_cert_pem: failed to parse PEM cert; skipping"
                );
            }
            Ok(cert) => {
                if let Err(e) = store.add(cert) {
                    tracing::warn!(
                        broker = %broker_slug,
                        cert_index = idx,
                        error = %e,
                        "ca_cert_pem: failed to add cert to TLS trust store; skipping"
                    );
                } else {
                    added += 1;
                }
            }
        }
    }
    (store, added)
}

/// Build a `rustls::RootCertStore` from the operator-supplied PEM or from the
/// system trust store, shared by both the Tls12 and Tls13 branches.
fn build_root_store(broker: &MqttClientConfig) -> rustls::RootCertStore {
    if let Some(pem_bytes) = &broker.ca_cert_pem {
        let (store, _) = load_ca_cert_pem(&broker.identity.slug, pem_bytes);
        store
    } else {
        let mut store = rustls::RootCertStore::empty();
        for (idx, cert) in rustls_native_certs::load_native_certs()
            .certs
            .into_iter()
            .enumerate()
        {
            if let Err(e) = store.add(cert) {
                tracing::warn!(
                    broker = %broker.identity.slug,
                    cert_index = idx,
                    error = %e,
                    "system trust store: failed to add cert; skipping"
                );
            }
        }
        store
    }
}

/// Build a `Transport::Tls` that enforces `broker.tls_version_min`.
pub(crate) fn build_tls_transport(broker: &MqttClientConfig) -> Transport {
    let provider = rustls::crypto::aws_lc_rs::default_provider();
    let root_store = build_root_store(broker);

    match broker.identity.tls_version_min {
        TlsVersionMin::Tls13 => {
            let client_config = rustls::ClientConfig::builder_with_provider(provider.into())
                .with_protocol_versions(&[&rustls::version::TLS13])
                .expect("TLS 1.3 is always supported by the aws_lc_rs provider")
                .with_root_certificates(root_store)
                .with_no_client_auth();
            Transport::tls_with_config(TlsConfiguration::Rustls(Arc::new(client_config)))
        }
        TlsVersionMin::Tls12 => {
            let client_config = rustls::ClientConfig::builder_with_provider(provider.into())
                .with_safe_default_protocol_versions()
                .expect("default protocol versions always valid for aws_lc_rs provider")
                .with_root_certificates(root_store)
                .with_no_client_auth();
            Transport::tls_with_config(TlsConfiguration::Rustls(Arc::new(client_config)))
        }
    }
}

// ---------------------------------------------------------------------------
// Jittered backoff
// ---------------------------------------------------------------------------

/// Deterministic FNV-1a seed from a client_id string (backoff jitter seeding).
pub(crate) fn client_id_seed(client_id: &str) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in client_id.bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

pub(crate) fn backoff_duration(
    attempt: u32,
    initial_secs: u32,
    max_secs: u32,
    rng_seed: u64,
) -> Duration {
    let base = initial_secs as u64 * 2u64.saturating_pow(attempt.min(10));
    let capped = base.min(max_secs as u64);
    // ±25% jitter using a simple LCG seeded per-client.
    let jitter_range = capped / 4;
    let pseudo = (rng_seed
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407)
        >> 33)
        % (jitter_range * 2 + 1);
    let jittered = capped.saturating_sub(jitter_range) + pseudo;
    Duration::from_secs(jittered.max(1))
}

// ---------------------------------------------------------------------------
// Public entry point: spawn a supervisor for one client session
// ---------------------------------------------------------------------------

/// Spawn a connection supervisor task for `handle`'s client, connecting to the
/// broker described by `handle.config`. Returns immediately; the supervisor runs
/// in the background.
///
/// The body is wrapped by a watchdog that catches panics, logs at error, and
/// respawns after a brief pause that a stop ends at once.
///
/// The returned join handle resolves when the supervisor has exited — on the
/// stop signal or on an authoritative give-up. Callers must hand it to
/// `MqttClientHandle::set_supervisor`.
///
/// Reached only through [`UnspawnedSession::spawn`], which checks the router
/// is ready first.
#[must_use]
fn spawn_client_supervisor(
    handle: Arc<MqttClientHandle>,
    router: Arc<dyn MqttEventRouter>,
    stop_rx: tokio::sync::watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut pause_rx = stop_rx.clone();
        loop {
            let handle2 = handle.clone();
            let router2 = router.clone();
            let stop_rx2 = stop_rx.clone();
            let join = tokio::spawn(supervisor_body(handle2, router2, stop_rx2));
            match join.await {
                Ok(()) => break, // normal exit (stop signal / authoritative give-up)
                Err(e) if e.is_panic() => {
                    tracing::error!(
                        client = %handle.config.identity.slug,
                        "MQTT supervisor task panicked; respawning. This is a bug.",
                    );
                    respawn_pause(&mut pause_rx).await;
                    // continue — respawn
                }
                Err(e) => {
                    tracing::warn!(
                        client = %handle.config.identity.slug,
                        error = %e,
                        "MQTT supervisor task ended unexpectedly; not respawning",
                    );
                    break;
                }
            }
        }
    })
}

/// The watchdog's pause before a respawn: [`RESPAWN_PAUSE`], or until the stop
/// is requested if that is sooner.
async fn respawn_pause(stop_rx: &mut tokio::sync::watch::Receiver<bool>) {
    tokio::select! {
        () = tokio::time::sleep(RESPAWN_PAUSE) => {}
        () = stop_requested(stop_rx) => {}
    }
}

/// Where an arriving session's filter set comes from.
pub enum ArrivingFilters<'a> {
    /// The set the caller derived: a boot's static union.
    Declared(Vec<IngressSubscription>),
    /// A client a reload adds: an empty set, so the incoming step can report
    /// every one of its filters as a move, and a clean start on every CONNECT
    /// up to the first CONNACK. A session the broker kept under the slug was
    /// asserted for an earlier declaration, and its backlog would arrive
    /// before the incoming step has added a filter to route it by.
    Added,
    /// The predecessor this session replaces. Its filter set is inherited, and
    /// its supervisor is stopped and joined as part of the handover. A clean
    /// start the predecessor still owed is inherited with it.
    Successor(&'a MqttClientHandle),
}

/// A client session that is registered on its service and has no supervisor
/// yet: its slug resolves — for the bus's `mqtt:` admission and for the egress
/// reader — and nothing connects until [`Self::spawn`].
#[must_use = "a registered session connects nothing until it is spawned"]
pub struct UnspawnedSession {
    handle: Arc<MqttClientHandle>,
    stop_rx: tokio::sync::watch::Receiver<bool>,
}

impl UnspawnedSession {
    /// The registered handle.
    pub fn handle(&self) -> &Arc<MqttClientHandle> {
        &self.handle
    }

    /// Spawn the session's supervisor, delivering through `router`, and record
    /// its join handle. Returns the handle.
    ///
    /// # Panics
    ///
    /// Panics, before anything is spawned, if `router` is not
    /// [`MqttEventRouter::ready`]. A supervisor connects at once, the broker
    /// may hand it a retained or queued message straight away, and the
    /// library acknowledges every message on receipt: a router that cannot
    /// route it then loses it. The check does not wait for such a message, so
    /// a misordered boot dies at the spawn every time.
    pub async fn spawn(self, router: Arc<dyn MqttEventRouter>) -> Arc<MqttClientHandle> {
        assert!(
            router.ready(),
            "MQTT supervisor for client {:?} spawned against a router that cannot deliver yet \
             — the router must have its state before any supervisor delivering through it runs",
            self.handle.config.identity.slug,
        );
        let join = spawn_client_supervisor(self.handle.clone(), router, self.stop_rx);
        self.handle.set_supervisor(join).await;
        self.handle
    }
}

/// Build a client's handle with `filters` as its filter set and register it on
/// `service`. Nothing connects: the returned session is spawned by
/// [`UnspawnedSession::spawn`].
///
/// A handle registered here replaces any handle of the same slug in the
/// registry; [`register_and_spawn`] is the form that hands over from that
/// predecessor.
pub fn register_client(
    service: &MqttService,
    config: Arc<MqttClientConfig>,
    filters: Vec<IngressSubscription>,
) -> UnspawnedSession {
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let handle = MqttClientHandle::new(config, filters, stop_tx);
    service.add_client(handle.clone());
    UnspawnedSession { handle, stop_rx }
}

/// Bring one client session up on `service`: [`register_client`], hand the
/// broker client id and the filter set over from a predecessor if there is
/// one, then [`UnspawnedSession::spawn`]. Returns the registered handle.
///
/// The order is the protocol, and it lives here rather than at each caller
/// because every part of it is load-bearing:
///
/// - **Register before spawning.** A dynamic `mqtt:` subscribe reaches the live
///   `AsyncClient` through `get_client`, and egress resolves the handle by
///   slug. The supervisor must not be able to run before the slug resolves.
/// - **Register the successor before joining the predecessor.** A restart must
///   never leave the slug absent from the registry for a caller that is
///   authorised to name it.
/// - **Join the predecessor before spawning the successor.** Both sessions
///   carry one MQTT client id, so the broker must see the predecessor's
///   DISCONNECT before the successor's CONNECT.
/// - **Inherit the filter set after that join, not before, and atomically.** A
///   concurrent `subscribe_filter` can add a filter to the predecessor at any
///   instant up to the join; a set copied before it would drop that filter, and
///   nothing downstream would ever re-assert it — the durable row is on both
///   sides of the next reload's comparison, so it produces no move. After the
///   join no supervisor touches the predecessor, and a subscribe that resolved
///   it before the swap re-targets the successor
///   ([`MqttService::subscribe_filter`]). The inherit is an assignment onto a
///   handle the registry has been handing out since `add_client`, so the
///   successor's write guard is taken before the join and held across it: every
///   filter edit that reaches this slug during the handover — whether it
///   resolved the successor directly or was re-aimed at it — therefore applies
///   on top of the inherited set rather than being discarded by it. A read of the successor's set blocks for the
///   predecessor's stop, which the client's `stop_wait_secs` bounds.
/// - **Record the join handle.** `MqttClientHandle::stop_and_join` panics
///   without one, so a session that is brought up any other way cannot be
///   stopped.
///
/// Callers log; nothing is logged here, because what an operator needs to read
/// off a boot, an arrival and a restart differs.
///
/// # Panics
///
/// As [`UnspawnedSession::spawn`]: if `router` is not ready.
pub async fn register_and_spawn(
    service: &MqttService,
    config: Arc<MqttClientConfig>,
    filters: ArrivingFilters<'_>,
    router: Arc<dyn MqttEventRouter>,
) -> Arc<MqttClientHandle> {
    let declared = match &filters {
        ArrivingFilters::Declared(subs) => subs.clone(),
        ArrivingFilters::Added | ArrivingFilters::Successor(_) => Vec::new(),
    };
    let session = register_client(service, config, declared);
    match filters {
        ArrivingFilters::Declared(_) => {}
        ArrivingFilters::Added => session.handle.require_clean_start(),
        ArrivingFilters::Successor(old) => {
            // The successor's set is claimed before the predecessor is stopped
            // and held until the inherit has been written, so a filter edit on
            // this slug queues behind the inherit instead of being overwritten
            // by it.
            let mut arriving = session.handle.subscriptions.write().await;
            old.stop_and_join().await;
            *arriving = old.subscriptions.read().await.clone();
            drop(arriving);
            if old.clean_start_pending() {
                session.handle.require_clean_start();
            }
        }
    }
    session.spawn(router).await
}

/// Whether the supervisor has been asked to stop, right now.
///
/// A closed channel counts, as defence against a future ownership change
/// rather than against anything reachable today: each client's channel has one
/// sender, held by the `MqttClientHandle` this supervisor itself holds, so a
/// running supervisor cannot observe the channel closed. Should the handle
/// stop carrying it, a closed channel means the owner is gone and there is
/// nothing left to keep a broker session up for.
fn stopping_now(stop_rx: &tokio::sync::watch::Receiver<bool>) -> bool {
    *stop_rx.borrow() || stop_rx.has_changed().is_err()
}

/// Resolve when the supervisor has been asked to stop, and not before.
///
/// The counterpart of [`stopping_now`] for a `select!` arm: a write of `false`
/// onto the channel is not a stop, so this waits again rather than handing the
/// arm a wake its caller has to re-examine.
async fn stop_requested(stop_rx: &mut tokio::sync::watch::Receiver<bool>) {
    loop {
        if stopping_now(stop_rx) {
            return;
        }
        if stop_rx.changed().await.is_err() {
            return;
        }
    }
}

/// How one wait of the connected phase ended.
#[derive(Debug, PartialEq, Eq)]
enum ConnectedWait<T> {
    /// A stop was requested; the poll was dropped unfinished.
    Stop,
    /// The poll finished with this output.
    Polled(T),
}

/// Await `poll`, one `EventLoop::poll`, to its end, serving `serve_filters`
/// before it is first polled and again at each wake on `filter_wake`. Only a
/// stop request ends the wait before `poll` finishes.
///
/// `poll` is never dropped for a wake: the library's `poll` is not
/// cancel-safe. When it takes a SUBSCRIBE off the client's control channel and
/// then yields to replay requests from its pending list, dropping it drops
/// that SUBSCRIBE unsent and unreported, and every later SUBSCRIBE binds to the
/// wrong filter of `pending_subscribes`. A write that waits on a full socket
/// buffer is lost the same way. At a stop the connection is ended and its
/// request bookkeeping cleared, so a request lost there misattributes nothing
/// that outlives the stop. `poll` is not polled while `serve_filters` runs,
/// which takes the subscription set's lock and submits without waiting.
async fn await_connected_poll<F: Future>(
    poll: F,
    stop_rx: &mut tokio::sync::watch::Receiver<bool>,
    filter_wake: &tokio::sync::Notify,
    serve_filters: &mut impl ServeFilters,
) -> ConnectedWait<F::Output> {
    let mut poll = std::pin::pin!(poll);
    loop {
        serve_filters.serve().await;
        tokio::select! {
            biased;
            _ = stop_requested(stop_rx) => return ConnectedWait::Stop,
            () = filter_wake.notified() => {}
            output = &mut poll => return ConnectedWait::Polled(output),
        }
    }
}

/// The step [`await_connected_poll`] runs between polls.
trait ServeFilters {
    fn serve(&mut self) -> impl Future<Output = ()> + Send;
}

/// The connected phase's filter settling: the filters this connection still
/// owes the broker a request for, and what settling them has done so far.
struct FilterSettler<'a> {
    handle: &'a Arc<MqttClientHandle>,
    client: &'a AsyncClient,
    /// Filters to settle, in order; the head is retried after a full channel.
    unsettled: std::collections::VecDeque<String>,
    /// Whether the connection's broker accepts Subscription Identifiers.
    with_identifier: bool,
    /// How many filters the CONNACK left owed, and whether settling them has
    /// been logged.
    connack_owed: usize,
    logged: bool,
    failed: usize,
    withdrawals: usize,
}

impl<'a> FilterSettler<'a> {
    fn new(
        handle: &'a Arc<MqttClientHandle>,
        client: &'a AsyncClient,
        unsettled: std::collections::VecDeque<String>,
        with_identifier: bool,
    ) -> Self {
        let connack_owed = unsettled.len();
        Self {
            handle,
            client,
            unsettled,
            with_identifier,
            connack_owed,
            logged: connack_owed == 0,
            failed: 0,
            withdrawals: 0,
        }
    }
}

impl ServeFilters for FilterSettler<'_> {
    /// Take the filters other tasks named, then submit as many requests as the
    /// request channel will take now. A full channel is non-empty, so the
    /// running poll takes a request off it (or fails and ends the connection)
    /// and this runs again with the next poll.
    async fn serve(&mut self) {
        let handle = self.handle;
        let client_slug = handle.config.identity.slug.as_str();
        self.unsettled.extend(
            handle
                .filter_requests
                .lock()
                .expect(FILTER_REQUESTS_POISONED)
                .drain(..),
        );
        if self.unsettled.is_empty() {
            return;
        }
        let mut set = handle.subscriptions.write().await;
        while let Some(filter) = self.unsettled.front() {
            match settle_filter(handle, self.client, &mut set, filter, self.with_identifier) {
                Ok(Settled::ChannelFull) => break,
                Ok(Settled::Unsubscribed) => {
                    self.withdrawals += 1;
                    self.unsettled.pop_front();
                }
                Ok(Settled::Subscribed | Settled::Nothing) => {
                    self.unsettled.pop_front();
                }
                Err(e) => {
                    tracing::error!(
                        client = %client_slug,
                        topic = %filter,
                        error = %e,
                        "failed to send subscribe — filter not asserted; channel will \
                         receive nothing until the next reconnect",
                    );
                    self.unsettled.pop_front();
                    self.failed += 1;
                }
            }
        }
        drop(set);
        if !self.logged && self.unsettled.is_empty() {
            if self.failed == 0 {
                tracing::info!(
                    client = %client_slug,
                    filters = self.connack_owed,
                    withdrawals = self.withdrawals,
                    "settled all ingress filters",
                );
            } else {
                tracing::warn!(
                    client = %client_slug,
                    filters = self.connack_owed,
                    failed = self.failed,
                    "settling finished with ingress filters not asserted",
                );
            }
            self.logged = true;
        }
    }
}

/// What a CONNACK says that the session keeps.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ConnackFacts {
    session_present: bool,
    /// The broker's Maximum Packet Size, if it sent one.
    max_packet_size: Option<u32>,
    /// The broker's Receive Maximum, if it sent one.
    receive_max: Option<u16>,
    /// Whether the broker supports Subscription Identifiers.
    sub_ids_available: bool,
}

/// Whether a CONNACK's properties say the broker supports Subscription
/// Identifiers: `false` only when the property is present with value 0, since
/// MQTT 5.0 section 3.2.2.3.12 reads an absent property as "supported".
fn sub_ids_available(props: Option<&rumqttc::ConnAckProperties>) -> bool {
    props.and_then(|p| p.subscription_identifiers_available) != Some(0)
}

/// Which connection the events the supervisor is handling belong to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConnectionPhase {
    /// Events the event loop queued on a connection that has since failed,
    /// handed out on the next connect ahead of its CONNACK.
    Lost,
    /// Events of the connection the supervisor is servicing.
    Live,
}

/// The actual supervisor body, wrapped by the watchdog in
/// `spawn_client_supervisor`.
///
/// The body builds one `AsyncClient` and one `EventLoop` and keeps them for
/// its whole life, so the client's half of the broker session — publishes and
/// SUBSCRIBEs awaiting an answer, and their packet identifiers — survives a
/// lost connection and is resumed by the library when the broker kept the
/// session. Each turn of the loop is a connect phase, polling until a CONNACK
/// or an error, then a connected phase until the connection fails. The loop is
/// dropped only when the body ends: at a stop, at an authoritative failure, or
/// by the unwind of a panic.
async fn supervisor_body(
    handle: Arc<MqttClientHandle>,
    router: Arc<dyn MqttEventRouter>,
    mut stop_rx: tokio::sync::watch::Receiver<bool>,
) {
    // The resolved per-client config is the single source of truth; the handle
    // shares it with this supervisor (cheap Arc clone, once per supervisor
    // lifetime). Read broker coordinates/backoff/slug through it.
    let broker = handle.config.clone();
    let client_slug = broker.identity.slug.clone();
    let rng_seed = client_id_seed(&client_id(&broker));

    let mut attempt: u32 = 0;

    // First write wins; every exit path must set this before breaking.
    let mut terminal_reason: Option<SupervisorState> = None;

    // The failure the next connect attempt follows: written into `Connecting`
    // so an attempt does not blank the error a reader sees.
    let mut last_failure: Option<String> = None;

    // A body the watchdog respawned inherits a handle whose previous event loop
    // is gone, and nothing of that loop will be replayed: no SUBSCRIBE recorded
    // for it can be answered on this body's loop.
    clear_connection_tracking(&handle).await;
    clear_subscribe_fifo(&handle);

    let opts = build_mqtt_options(&broker, build_tls_transport(&broker));
    // This task is the only poller of `eventloop`, and only a poll frees a slot
    // on the client's bounded request channels. So every request this task
    // submits on `client` is a non-blocking `try_*` send interleaved with
    // `poll` — `try_assert_ingress_subscription` in the connected phase. The
    // stop path's DISCONNECT goes through the library's unbounded
    // immediate channel instead. The awaiting forms are for other tasks only.
    // The bound is kept so a stalled broker turns into a prompt `NotConnected`
    // on the publish path rather than unbounded growth.
    let (client, mut eventloop) = AsyncClient::builder(opts).capacity(64).build();
    let mut cure = SessionCure::None;

    'supervisor: loop {
        if stopping_now(&stop_rx) {
            tracing::info!(client = %client_slug, "supervisor stopping");
            // Every path back to the loop top within one body clears the client
            // cell first. A body the watchdog respawned after a panic finds the
            // dead body's client still there; its event loop died with that
            // body, so there is no session to disconnect, only a handle to drop.
            *handle.client.lock().await = None;
            if terminal_reason.is_none() {
                terminal_reason = Some(SupervisorState::Disconnected { last_error: None });
            }
            drop_event_loop(&handle, client, eventloop);
            break;
        }

        let clean_start = clean_start_before_connect(cure, &handle, &mut eventloop.options);
        tracing::info!(
            client = %client_slug,
            attempt,
            clean_start = eventloop.options.clean_start(),
            clean_start_reason = clean_start.map_or("none", CleanStartReason::as_str),
            "connecting to broker",
        );

        handle.set_state(SupervisorState::Connecting {
            last_error: last_failure.clone(),
        });

        // Poll until ConnAck or error. `None` = stop requested before any
        // connection was made. The library connects inside `poll` when it
        // holds no network, then hands out the events it queued on the lost
        // connection ahead of the new CONNACK. So the first event proves a
        // CONNACKed connection is installed; from then on the stop is left to
        // the connected phase, which ends the connection with a DISCONNECT.
        // What remains up to the CONNACK is already queued and is handed out
        // without I/O.
        let mut connected = false;
        let connack: Option<Result<ConnackFacts, ConnectionError>> = loop {
            tokio::select! {
                biased;
                _ = stop_requested(&mut stop_rx), if !connected => {
                    break None;
                }
                event = eventloop.poll() => {
                    match event {
                        Err(e) => break Some(Err(e)),
                        Ok(Event::Incoming(Incoming::ConnAck(ack))) => {
                            let props = ack.properties.as_ref();
                            break Some(Ok(ConnackFacts {
                                session_present: ack.session_present,
                                max_packet_size: props.and_then(|p| p.max_packet_size),
                                receive_max: props.and_then(|p| p.receive_max),
                                sub_ids_available: sub_ids_available(props),
                            }));
                        }
                        Ok(ev) => {
                            connected = true;
                            handle_event(ev, &handle, &router, ConnectionPhase::Lost).await;
                        }
                    }
                }
            }
        };

        let facts = match connack {
            None => {
                // Stop before the library made a connection: there is no
                // broker connection to end and no client installed.
                if terminal_reason.is_none() {
                    terminal_reason = Some(SupervisorState::Disconnected { last_error: None });
                }
                drop_event_loop(&handle, client, eventloop);
                break;
            }
            Some(Err(e)) => {
                let authoritative = is_authoritative_failure(&e);
                let error_str = e.to_string();
                {
                    // The state is written here, under the client lock: a
                    // reader that finds the client cell empty and then reads
                    // the supervisor state to tell a deferral from a dead
                    // session must not see a retrying state for a session that
                    // has already given up. For a `Failed` the tail rewrites
                    // the same value.
                    let mut client_guard = handle.client.lock().await;
                    *client_guard = None;
                    handle.set_state(if authoritative {
                        SupervisorState::Failed {
                            reason: error_str.clone(),
                        }
                    } else {
                        SupervisorState::Disconnected {
                            last_error: Some(error_str.clone()),
                        }
                    });
                }
                if authoritative {
                    tracing::error!(
                        client = %client_slug,
                        broker = %broker.identity.slug,
                        error = %error_str,
                        "broker connection failed with authoritative error; stopping retry",
                    );
                    router
                        .session_fault(
                            &client_slug,
                            SessionFault::Failed {
                                reason: error_str.clone(),
                            },
                        )
                        .await;
                    // Dropped after the state that carries the reason is
                    // written, so a publish waiter woken by the drop reads it.
                    drop_event_loop(&handle, client, eventloop);
                    let failed = SupervisorState::Failed { reason: error_str };
                    if terminal_reason.is_none() {
                        terminal_reason = Some(failed);
                    }
                    break;
                }
                // The loop holds no network after a failed attempt and keeps
                // everything it held; the next poll starts a new connection.
                tracing::warn!(
                    client = %client_slug,
                    broker = %broker.identity.slug,
                    error = %error_str,
                    "broker connection failed; will retry",
                );
                last_failure = Some(error_str);
                let delay = backoff_duration(
                    attempt,
                    broker.identity.reconnect_backoff_initial_secs,
                    broker.identity.reconnect_backoff_max_secs,
                    rng_seed,
                );
                attempt += 1;
                tokio::select! {
                    _ = tokio::time::sleep(delay) => {}
                    _ = stop_requested(&mut stop_rx) => {}
                }
                continue;
            }
            Some(Ok(sp)) => sp,
        };
        clean_start_at_connack(&mut cure, &handle, &mut eventloop.options);

        // Every filter this connection owes the broker a request for. Filter
        // requests left from a previous connection are dropped: each names a
        // filter that was held when it was made, so it is asserted and in this
        // list, unless an UNSUBACK or this CONNACK has said the broker does not
        // hold it.
        let unsettled = take_connection(&handle, client.clone(), facts).await;
        handle.set_state(SupervisorState::Connected);
        attempt = 0;

        tracing::info!(
            client = %client_slug,
            broker = %broker.identity.slug,
            session_present = facts.session_present,
            "connected to broker",
        );
        if !facts.sub_ids_available {
            tracing::warn!(
                client = %client_slug,
                broker = %broker.identity.slug,
                "the broker does not support Subscription Identifiers: inbound messages on this \
                 client are routed by topic, and overlapping filters on it deliver a message once \
                 per covering filter into each covering channel",
            );
        }

        // Settle every filter the CONNACK left owed — a SUBSCRIBE for each
        // held filter, and with the session present an UNSUBSCRIBE for each
        // asserted filter no longer held — from inside the main loop: as many
        // requests as the request channel takes before each poll and at each
        // filter wake while it runs, the rest once a poll frees room.
        let mut settler = FilterSettler::new(&handle, &client, unsettled, facts.sub_ids_available);

        let lost = loop {
            let wait = await_connected_poll(
                eventloop.poll(),
                &mut stop_rx,
                &handle.filter_wake,
                &mut settler,
            )
            .await;
            match wait {
                ConnectedWait::Polled(Err(e)) => break e,
                ConnectedWait::Polled(Ok(ev)) => {
                    handle_event(ev, &handle, &router, ConnectionPhase::Live).await;
                }
                ConnectedWait::Stop => {
                    // Write Disconnected before the stop waits on anything, so
                    // the observable health transition does not gate on broker
                    // liveness.
                    if terminal_reason.is_none() {
                        let disconnected = SupervisorState::Disconnected { last_error: None };
                        handle.set_state(disconnected.clone());
                        terminal_reason = Some(disconnected);
                    }
                    // From here no caller can start a publish, SUBSCRIBE or
                    // UNSUBSCRIBE on this session, and the pushes above that
                    // move deferred subscribes onto the connection do not run
                    // again.
                    drop(handle.client.lock().await.take());
                    end_connected_session(&handle, &router, client, eventloop).await;
                    break 'supervisor;
                }
            }
        };

        // Connection lost.
        let verdict = classify_loss(&lost, &mut cure);
        let disconnect_reason = match verdict {
            LossVerdict::GiveUp { pkid } => format!(
                "{lost}: the broker answered packet identifier {pkid}, which the \
                 client does not hold, on the connection made with a clean start after the \
                 session was discarded for the same fault"
            ),
            _ => lost.to_string(),
        };
        let disconnect_authoritative = matches!(
            verdict,
            LossVerdict::Authoritative | LossVerdict::GiveUp { .. }
        );
        {
            // State under the client lock, for the reason the connect path
            // writes it there too: an empty client cell paired with a retrying
            // supervisor state is read as "will reconnect".
            let mut client_guard = handle.client.lock().await;
            *client_guard = None;
            handle.set_state(if disconnect_authoritative {
                SupervisorState::Failed {
                    reason: disconnect_reason.clone(),
                }
            } else {
                SupervisorState::Disconnected {
                    last_error: Some(disconnect_reason.clone()),
                }
            });
        }
        clear_connection_tracking(&handle).await;

        report_loss(router.as_ref(), &client_slug, &verdict, &disconnect_reason).await;

        if disconnect_authoritative {
            // Ends the session's publish notices after the state that carries
            // the reason is written, so a waiter woken by the end reads it.
            drop_event_loop(&handle, client, eventloop);
            let failed = SupervisorState::Failed {
                reason: disconnect_reason,
            };
            if terminal_reason.is_none() {
                terminal_reason = Some(failed);
            }
            break;
        }

        sweep_after_loss(&mut eventloop);

        tracing::warn!(
            client = %client_slug,
            broker = %broker.identity.slug,
            reason = %disconnect_reason,
            unsettled = settler.unsettled.len(),
            "broker connection lost; reconnecting",
        );
        last_failure = Some(disconnect_reason);

        let delay = backoff_duration(
            attempt,
            broker.identity.reconnect_backoff_initial_secs,
            broker.identity.reconnect_backoff_max_secs,
            rng_seed,
        );
        attempt += 1;
        tokio::select! {
            _ = tokio::time::sleep(delay) => {}
            _ = stop_requested(&mut stop_rx) => {}
        }
    }

    // Terminal-state tail: every exit path must leave supervisor_state in a
    // terminal (non-Connected) state. Every path arrives here with its reason,
    // and the two authoritative-failure paths have already written that same
    // `Failed` beside clearing the client cell, so this write is idempotent
    // rather than exclusive.
    match terminal_reason {
        Some(state) => handle.set_state(state),
        None => {
            panic!(
                "MQTT supervisor for client '{client_slug}' exited without setting a terminal \
                 reason — this is a bug"
            );
        }
    }
}

/// Put every publish handed to the event loop before a loss into the library's
/// pending list, after the supervisor has emptied the client cell.
///
/// The failing `poll` already moved what the library held into that list, but
/// a submitter that took the client before the cell was emptied can have
/// queued a request since. A request left on the channel survives a CONNACK
/// that reports no session while the pending list is failed, so it would go
/// out after messages the reader handed later. On a loop holding no network
/// the library's `clean()` moves the request channels' contents to the tail of
/// the pending list, in order; from then until the next CONNACK installs a
/// client nothing can be submitted. It also drops queued PUBACK and PUBREC
/// requests, which only a client with manual acknowledgements submits, and a
/// publish with a topic alias and no topic, which Brenn never builds.
pub(crate) fn sweep_after_loss(eventloop: &mut rumqttc::EventLoop) {
    eventloop.clean();
}

/// How many incoming PUBLISH events `events` holds: messages the library
/// received from the broker and has not yet handed to the supervisor.
fn queued_incoming_publishes(events: &std::collections::VecDeque<Event>) -> usize {
    events
        .iter()
        .filter(|event| matches!(event, Event::Incoming(Incoming::Publish(_))))
        .count()
}

/// Drop the body's event loop and client, which ends the session's publish
/// notices with `Recv`. Every drop the supervisor makes goes through here.
///
/// Messages the library received and had not yet handed out go with the loop;
/// the queue is read, not changed, and when it holds any a `warn` names their
/// number.
fn drop_event_loop(handle: &MqttClientHandle, client: AsyncClient, eventloop: rumqttc::EventLoop) {
    let dropped = queued_incoming_publishes(&eventloop.state.events);
    if dropped > 0 {
        // TODO(mqtt-inbound-acked-before-routed): a QoS 1 message among these
        // may already be acknowledged to the broker, which will not send it
        // again.
        tracing::warn!(
            client = %handle.config.identity.slug,
            dropped,
            "messages received from the broker are dropped with the MQTT client without \
             reaching their channels",
        );
    }
    drop(eventloop);
    drop(client);
}

/// End a connected session whose stop was requested. The caller has written
/// the terminal state and emptied the client cell.
///
/// The connection is serviced as on a live session — inbound publishes
/// routed, acknowledgements resolving the egress reader's notices,
/// keep-alive, publishes the reader submitted before the stop written as the
/// broker's Receive Maximum allows — until the reader is not live. The reader
/// owns the wait for outcomes and its bound; this waits on the reader alone. A
/// handle with no reader running passes at once.
///
/// If the connection fails meanwhile it has ended: no DISCONNECT is owed, and
/// dropping the event loop resolves every notice still outstanding, which
/// ends the reader's wait. Otherwise the DISCONNECT is requested through the
/// library's immediate channel — with Session Expiry Interval 0 when the stop
/// came from [`MqttClientHandle::end_session_and_join`] and the client's
/// expiry is above 0, plain otherwise ([`StopDisconnect`]) — which waits for
/// no acknowledgement, and the event loop is polled until it reports the request done or until the
/// client's stop bound, counted from this call, has passed with a poll still
/// pending. The poll is tried before the deadline, so a socket that takes the
/// packet gets it even when the reader's wait used the whole bound.
///
/// # Panics
///
/// If the immediate DISCONNECT cannot be queued: its receiver is the event
/// loop this function owns.
pub(crate) async fn end_connected_session(
    handle: &Arc<MqttClientHandle>,
    router: &Arc<dyn MqttEventRouter>,
    client: AsyncClient,
    mut eventloop: rumqttc::EventLoop,
) {
    let client_slug = handle.config.identity.slug.as_str();
    let bound = handle.stop_wait();
    let deadline = tokio::time::Instant::now() + bound;
    tracing::info!(
        client = %client_slug,
        bound_secs = bound.as_secs(),
        reader_live = *handle.egress_reader_live.borrow(),
        "MQTT session stopping: servicing the connection until the egress reader exits",
    );
    let connection_ended = loop {
        tokio::select! {
            biased;
            () = handle.egress_reader_exited() => break false,
            event = eventloop.poll() => match event {
                Ok(event) => handle_event(event, handle, router, ConnectionPhase::Live).await,
                Err(e) => {
                    tracing::info!(
                        client = %client_slug,
                        error = %e,
                        "broker connection ended while the session was stopping; no DISCONNECT \
                         is owed",
                    );
                    break true;
                }
            }
        }
    };
    let packet = StopDisconnect::of(handle);
    let mut written = false;
    let disconnect = if connection_ended {
        "not owed: the connection ended"
    } else {
        packet.send(&client).unwrap_or_else(|e| {
            panic!(
                "MQTT client {client_slug:?}: the immediate DISCONNECT could not be queued \
                 although its event loop is alive: {e}"
            )
        });
        let disconnect_deadline = tokio::time::sleep_until(deadline);
        tokio::pin!(disconnect_deadline);
        loop {
            tokio::select! {
                biased;
                event = eventloop.poll() => match event {
                    Ok(event) => handle_event(event, handle, router, ConnectionPhase::Live).await,
                    Err(ConnectionError::RequestsDone) => {
                        if packet == StopDisconnect::EndSession {
                            handle.record_session_end_written();
                        }
                        written = true;
                        break "written";
                    }
                    Err(e) => {
                        tracing::warn!(
                            client = %client_slug,
                            error = %e,
                            "broker connection failed while the DISCONNECT was being written",
                        );
                        break "connection failed while writing it";
                    }
                },
                () = &mut disconnect_deadline => {
                    tracing::warn!(
                        client = %client_slug,
                        bound_secs = bound.as_secs(),
                        "DISCONNECT not written within the stop bound; closing the connection \
                         without it",
                    );
                    break "not written within the stop bound";
                }
            }
        }
    };
    // TODO(mqtt-disconnect-lost-to-reset): the `EventLoop`'s drop below closes
    // the socket with whatever the broker sent after the last read still
    // unread; that close is an RST, and the broker may discard the DISCONNECT
    // with it. rumqttc offers no read-to-EOF after `RequestsDone`.
    drop_event_loop(handle, client, eventloop);
    clear_connection_tracking(handle).await;
    if written {
        tracing::info!(
            client = %client_slug,
            disconnect,
            packet = packet.label(),
            "MQTT session stopped: connection closed",
        );
    } else {
        tracing::info!(
            client = %client_slug,
            disconnect,
            unsent_packet = packet.label(),
            "MQTT session stopped: connection closed without the DISCONNECT",
        );
    }
}

/// Clear the per-connection request bookkeeping: the pkid → filter bindings
/// of SUBSCRIBEs and UNSUBSCRIBEs written on a connection, the order of the
/// requests for each filter, and the broker's SUBSCRIBE answers.
///
/// A request written on a connection that ended is never answered — Brenn's
/// requests are untracked, so the library does not replay one it has
/// written — and an acknowledgement belongs to the connection that carried it.
/// The next connection's answers arrive when its own requests are acked.
async fn clear_connection_tracking(handle: &MqttClientHandle) {
    handle.inflight_subscribes.lock().await.clear();
    handle.inflight_unsubscribes.lock().await.clear();
    handle
        .request_order
        .lock()
        .expect(REQUEST_ORDER_POISONED)
        .clear();
    handle.clear_outcomes().await;
}

/// The CONNACK's request bookkeeping, then `client` installed, then the
/// filters the connection owes the broker a request for.
///
/// The list is read after the install, so a filter added concurrently is
/// settled on this connection either way: added before the read, it is in the
/// list; added after it, its caller finds the client and names the filter to
/// the supervisor. A filter added between the install and the read may be
/// settled twice.
async fn take_connection(
    handle: &MqttClientHandle,
    client: AsyncClient,
    facts: ConnackFacts,
) -> std::collections::VecDeque<String> {
    subscribe_bookkeeping_at_connack(handle, facts.session_present).await;
    install_connection(
        handle,
        client,
        facts.max_packet_size,
        facts.receive_max,
        facts.sub_ids_available,
    )
    .await;
    owed_at_connack(handle).await
}

/// The request bookkeeping a CONNACK leaves.
///
/// The previous connection's acknowledgements are void, and so are the filter
/// requests queued for the supervisor, since every filter is settled from
/// [`owed_at_connack`]. With no session, the broker holds nothing and the
/// library has discarded every request it kept: both FIFOs are cleared and
/// the asserted filters become the held ones. With a session, the FIFOs hold
/// exactly the requests the library replays next, in order, and are kept to
/// bind them.
async fn subscribe_bookkeeping_at_connack(handle: &MqttClientHandle, session_present: bool) {
    clear_connection_tracking(handle).await;
    handle
        .filter_requests
        .lock()
        .expect(FILTER_REQUESTS_POISONED)
        .clear();
    // TODO(mqtt-replayed-subscribe-unverified): both branches rest on the
    // library replaying a request it held unsent at the loss (first, in order)
    // with a session and discarding it without one; no test runs the library
    // through either.
    if !session_present {
        clear_subscribe_fifo(handle);
    }
    if !session_present {
        handle.subscriptions.write().await.reset_asserted();
    }
}

/// The filters a connection owes the broker a request for after its CONNACK's
/// bookkeeping, held filters first: a SUBSCRIBE for each held filter, and an
/// UNSUBSCRIBE for each asserted filter no longer held. Without a session the
/// bookkeeping has made the asserted filters the held ones, so none is owed an
/// UNSUBSCRIBE.
async fn owed_at_connack(handle: &MqttClientHandle) -> std::collections::VecDeque<String> {
    let set = handle.subscriptions.read().await;
    // TODO(mqtt-deferred-unsubscribe-not-withdrawn): a filter an earlier
    // process left in the session it resumes is not asserted here, so it is
    // never withdrawn.
    let mut owed: std::collections::VecDeque<String> =
        set.iter().map(|e| e.topic_filter.clone()).collect();
    owed.extend(set.withdrawn());
    owed
}

/// Clear the FIFOs of SUBSCRIBEs and UNSUBSCRIBEs the event loop holds
/// unwritten, when nothing in them will be written: the broker reported no
/// session, so the library discarded them, or a new body starts on a new event
/// loop.
fn clear_subscribe_fifo(handle: &MqttClientHandle) {
    handle
        .pending_subscribes
        .lock()
        .expect(PENDING_SUBSCRIBES_POISONED)
        .clear();
    handle
        .pending_unsubscribes
        .lock()
        .expect(PENDING_UNSUBSCRIBES_POISONED)
        .clear();
}

// ---------------------------------------------------------------------------
// Is a connection failure "authoritative" (vs. transient)?
// ---------------------------------------------------------------------------

fn is_authoritative_return_code(code: &ConnectReturnCode) -> bool {
    match code {
        ConnectReturnCode::BadUserNamePassword => true,
        ConnectReturnCode::NotAuthorized => true,
        ConnectReturnCode::BadAuthenticationMethod => true,
        ConnectReturnCode::Banned => true,
        ConnectReturnCode::ClientIdentifierNotValid => true,
        ConnectReturnCode::UnsupportedProtocolVersion => true,
        ConnectReturnCode::UseAnotherServer => true,
        ConnectReturnCode::ServerMoved => true,
        ConnectReturnCode::MalformedPacket => true,
        ConnectReturnCode::ProtocolError => true,
        ConnectReturnCode::RetainNotSupported => true,
        ConnectReturnCode::QoSNotSupported => true,
        ConnectReturnCode::TopicNameInvalid => true,
        ConnectReturnCode::PacketTooLarge => true,
        ConnectReturnCode::PayloadFormatInvalid => true,
        ConnectReturnCode::QuotaExceeded => true,
        ConnectReturnCode::RefusedProtocolVersion => true,
        ConnectReturnCode::BadClientId => true,
        ConnectReturnCode::ServiceUnavailable => true,
        ConnectReturnCode::ServerBusy => false,
        ConnectReturnCode::ServerUnavailable => false,
        ConnectReturnCode::UnspecifiedError => false,
        ConnectReturnCode::ConnectionRateExceeded => false,
        ConnectReturnCode::ImplementationSpecificError => false,
        ConnectReturnCode::Success => true,
    }
}

/// Returns `true` if the error indicates an authoritative rejection (auth
/// denied, a refusal by TLS itself) rather than a transient network error.
///
/// Of TLS errors, authoritative are a fault of the client's own TLS
/// configuration or server name, and an I/O error of the handshake that
/// carries a `rustls::Error`: TLS rejected the peer or was rejected by it. An
/// I/O error of the handshake that carries none — an end-of-stream, a reset, an
/// unreachable host — is the connection ending, and is transient like any
/// other I/O error.
pub(crate) fn is_authoritative_failure(err: &ConnectionError) -> bool {
    match err {
        ConnectionError::Tls(rumqttc::TlsError::Io(e)) => e
            .get_ref()
            .is_some_and(|inner| inner.downcast_ref::<rustls::Error>().is_some()),
        ConnectionError::Tls(_) => true,
        ConnectionError::ConnectionRefused(code) => is_authoritative_return_code(code),
        ConnectionError::MqttState(StateError::ConnFail { reason }) => {
            is_authoritative_return_code(reason)
        }
        ConnectionError::MqttState(StateError::ServerDisconnect { reason_code, .. }) => {
            matches!(
                reason_code,
                DisconnectReasonCode::NotAuthorized
                    | DisconnectReasonCode::SessionTakenOver
                    | DisconnectReasonCode::UseAnotherServer
                    | DisconnectReasonCode::ServerMoved
            )
        }
        ConnectionError::MqttState(_) => false,
        ConnectionError::Io(_) => false,
        ConnectionError::Timeout(_) => false,
        ConnectionError::DisconnectTimeout => false,
        ConnectionError::RequestsDone => false,
        ConnectionError::NotConnAck(_) => false,
        ConnectionError::SessionStateMismatch { .. } => true,
        ConnectionError::BrokerTransportMismatch => true,
        ConnectionError::AuthProcessingError => true,
    }
}

/// Where a body stands in curing a session that provokes `Unsolicited`.
///
/// The broker acknowledged or released a packet identifier the library's
/// session state does not hold. On a resumed session that recurs at every
/// reconnect, because the state that provokes it is the session's. The cure is
/// one clean start; if the connection it made ends the same way, the body gives
/// up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SessionCure {
    /// No cure is under way.
    None,
    /// The next CONNECT is a clean start.
    Armed,
    /// The current connection was made by that clean start.
    Applied,
}

impl SessionCure {
    /// Set the clean-start flag of the next CONNECT. The library reads it
    /// afresh for each CONNECT and for the CONNACK's session check. The flag
    /// stays set across connect attempts that fail before a CONNACK.
    fn before_connect(self, options: &mut MqttOptions) {
        if self == SessionCure::Armed {
            options.set_clean_start(true);
        }
    }

    /// At a CONNACK: a clean start made this connection, so the kept session
    /// is asked for again from the next CONNECT on.
    fn at_connack(&mut self, options: &mut MqttOptions) {
        if *self == SessionCure::Armed {
            options.set_clean_start(false);
            *self = SessionCure::Applied;
        }
    }
}

/// Why a CONNECT asks for a clean start.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CleanStartReason {
    /// The handle requires one: a client a reload added
    /// ([`ArrivingFilters::Added`]), before its first CONNACK.
    Added,
    /// A [`SessionCure`] is armed: the resumed session provoked `Unsolicited`.
    Cure,
}

impl CleanStartReason {
    /// The reason's name on the `connecting to broker` line.
    fn as_str(self) -> &'static str {
        match self {
            Self::Added => "added",
            Self::Cure => "cure",
        }
    }
}

/// Set the clean-start flag of the next CONNECT from both of its sources — the
/// armed `cure` and the handle's requirement — and return why it is set, or
/// `None` for a CONNECT that asks for the kept session. When both hold,
/// `Added` is returned.
///
/// The flag stays set across connect attempts that fail before a CONNACK, and
/// a body respawned before the first CONNACK reads the handle's requirement
/// again; the library reads the flag afresh for each CONNECT and for the
/// CONNACK's session check.
fn clean_start_before_connect(
    cure: SessionCure,
    handle: &MqttClientHandle,
    options: &mut MqttOptions,
) -> Option<CleanStartReason> {
    cure.before_connect(options);
    if handle.clean_start_pending() {
        options.set_clean_start(true);
        return Some(CleanStartReason::Added);
    }
    (cure == SessionCure::Armed).then_some(CleanStartReason::Cure)
}

/// At a CONNACK: whichever clean start made this connection is spent — an
/// armed `cure` becomes applied, the handle's requirement is cleared — and the
/// kept session is asked for from the next CONNECT on. A source that did not
/// set the flag leaves it alone.
fn clean_start_at_connack(
    cure: &mut SessionCure,
    handle: &MqttClientHandle,
    options: &mut MqttOptions,
) {
    cure.at_connack(options);
    if handle.clear_clean_start() {
        options.set_clean_start(false);
    }
}

/// The DISCONNECT a stop sends on a live connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StopDisconnect {
    /// No properties: the broker keeps the session for its expiry.
    KeepSession,
    /// Session Expiry Interval 0: the broker discards the session.
    EndSession,
}

impl StopDisconnect {
    /// The DISCONNECT `handle`'s stop sends: one that ends the session when
    /// the stop asks for that and the client asked the broker to keep its
    /// session past the connection. A client declared with expiry 0 has a
    /// session that ends with the connection, so it sends the plain one.
    fn of(handle: &MqttClientHandle) -> Self {
        if handle.session_end_requested() && handle.config.identity.session_expiry_secs > 0 {
            StopDisconnect::EndSession
        } else {
            StopDisconnect::KeepSession
        }
    }

    /// The DISCONNECT's properties: `None` for the plain packet.
    fn properties(self) -> Option<rumqttc::DisconnectProperties> {
        match self {
            StopDisconnect::KeepSession => None,
            StopDisconnect::EndSession => Some(rumqttc::DisconnectProperties {
                session_expiry_interval: Some(0),
                reason_string: None,
                user_properties: Vec::new(),
                server_reference: None,
            }),
        }
    }

    /// Queue the DISCONNECT on `client`'s immediate channel.
    fn send(self, client: &AsyncClient) -> Result<(), rumqttc::ClientError> {
        match self.properties() {
            None => client.try_disconnect_now(),
            Some(properties) => client.try_disconnect_now_with_properties(
                rumqttc::DisconnectReasonCode::NormalDisconnection,
                properties,
            ),
        }
    }

    /// The closing log line's word for the packet, true whether or not it was
    /// written.
    fn label(self) -> &'static str {
        match self {
            StopDisconnect::KeepSession => "plain: asks the broker to keep the session",
            StopDisconnect::EndSession => {
                "Session Expiry Interval 0: asks the broker to end the session"
            }
        }
    }
}

/// Log a connected session's ending and tell `router` of the faults the
/// operator must hear about: an authoritative ending, a discarded session and
/// the give-up.
async fn report_loss(
    router: &dyn MqttEventRouter,
    client_slug: &str,
    verdict: &LossVerdict,
    reason: &str,
) {
    match *verdict {
        LossVerdict::Reconnect => {}
        LossVerdict::Authoritative => {
            tracing::error!(
                client = %client_slug,
                reason = %reason,
                "broker connection lost with authoritative error; stopping retry",
            );
            router
                .session_fault(
                    client_slug,
                    SessionFault::Failed {
                        reason: reason.to_string(),
                    },
                )
                .await;
        }
        LossVerdict::DiscardSession { pkid } => {
            tracing::error!(
                client = %client_slug,
                pkid,
                reason = %reason,
                "the broker answered a packet identifier the client's session does not hold; \
                 discarding the session: the next connect is a clean start, the broker's \
                 queued messages for this client are lost, and publishes in flight end as \
                 after a session reset",
            );
            router
                .session_fault(client_slug, SessionFault::SessionDiscarded { pkid })
                .await;
        }
        LossVerdict::GiveUp { .. } => {
            tracing::error!(
                client = %client_slug,
                reason = %reason,
                "discarding the MQTT session did not cure the unsolicited acknowledgement; \
                 stopping the client until its configuration is reloaded or the server \
                 restarts",
            );
            router
                .session_fault(
                    client_slug,
                    SessionFault::GaveUp {
                        reason: reason.to_string(),
                    },
                )
                .await;
        }
    }
}

/// What the supervisor does about a connected session that ended in error.
#[derive(Debug, Clone, PartialEq, Eq)]
enum LossVerdict {
    /// Reconnect after the backoff.
    Reconnect,
    /// Reconnect with a clean start after the backoff, and report the
    /// discarded session.
    DiscardSession { pkid: u16 },
    /// Stop: no reconnect can cure the failure.
    Authoritative,
    /// Stop: discarding the session did not cure `Unsolicited`.
    GiveUp { pkid: u16 },
}

/// Class a connected session's ending error and move the cure along with it.
///
/// `Unsolicited` discards the session the first time and gives up on the
/// connection the clean start made; any other ending returns the cure to
/// `None`.
fn classify_loss(err: &ConnectionError, cure: &mut SessionCure) -> LossVerdict {
    if let ConnectionError::MqttState(StateError::Unsolicited(pkid)) = err {
        let pkid = *pkid;
        return match *cure {
            SessionCure::None => {
                *cure = SessionCure::Armed;
                LossVerdict::DiscardSession { pkid }
            }
            SessionCure::Applied => LossVerdict::GiveUp { pkid },
            SessionCure::Armed => panic!(
                "MQTT session cure is armed on a connected session: every CONNACK applies an \
                 armed cure — this is a bug"
            ),
        };
    }
    *cure = SessionCure::None;
    if is_authoritative_failure(err) {
        LossVerdict::Authoritative
    } else {
        LossVerdict::Reconnect
    }
}

// ---------------------------------------------------------------------------
// Event dispatch
// ---------------------------------------------------------------------------

/// Act on one event of the event loop. `phase` says whether it belongs to the
/// connection being serviced or to one that failed before handing it out.
///
/// An inbound PUBLISH is routed in either phase: it was received, and the
/// broker cannot be relied on to send it again. Request attribution differs:
/// a SUBSCRIBE or UNSUBSCRIBE written on a lost connection is never answered,
/// so its outgoing event takes its filter off the FIFO and binds nothing, and
/// a SUBACK or UNSUBACK of a lost connection records nothing.
async fn handle_event(
    event: Event,
    handle: &Arc<MqttClientHandle>,
    router: &Arc<dyn MqttEventRouter>,
    phase: ConnectionPhase,
) {
    let client_slug = handle.config.identity.slug.as_str();
    // A request slot may have freed. The event loop takes a request off the
    // channel only to process it, and processing any request emits an
    // outgoing event: the PUBLISH, SUBSCRIBE or UNSUBSCRIBE written, an
    // acknowledgement, or `AwaitAck` for a publish parked on a packet-id
    // collision. An outgoing packet no request caused (a keep-alive PINGREQ,
    // an automatic PUBACK) wakes the egress reader for one pass with nothing
    // new to do.
    if matches!(event, Event::Outgoing(_)) {
        handle.egress_wake.notify_one();
    }
    match event {
        // --- Inbound publish ---
        // SECURITY: inbound payloads are untrusted. Any MQTT client with write
        // access to a subscribed topic can inject arbitrary text into the LLM
        // context (prompt injection). Broker ACLs must restrict topic write access
        // to authorised publishers only. The PUBLISH's Subscription Identifiers
        // are resolved against this client's set to the filters the broker sent
        // the copy for, and the router publishes into those filters' channels
        // only, each after checking that its filter covers the topic. A copy
        // whose identifiers name no subscription this client holds is dropped
        // here. A copy with no identifier is routed by topic.
        // `p.retain` reaches the router, which reads it to tell a replay from a
        // forwarded message.
        // TODO(mqtt-inbound-retain-flag-not-on-envelope): the router does not
        // carry it to the subscriber.
        Event::Incoming(Incoming::Publish(p)) => {
            let topic_str = String::from_utf8_lossy(&p.topic).into_owned();

            tracing::debug!(
                client = %client_slug,
                topic = %topic_str,
                size = p.payload.len(),
                "inbound publish",
            );

            let content_type: Option<String> =
                p.properties.as_ref().and_then(|pp| pp.content_type.clone());

            let delivery_qos: u8 = match p.qos {
                QoS::AtMostOnce => 0,
                QoS::AtLeastOnce => 1,
                QoS::ExactlyOnce => 2,
            };
            if delivery_qos == 2 && !handle.qos2_inbound_warned.swap(true, Ordering::Relaxed) {
                tracing::warn!(
                    client = %client_slug,
                    topic = %topic_str,
                    "inbound PUBLISH at QoS 2, although Brenn does not subscribe at QoS 2: it \
                     comes from session state an earlier configuration left at the broker, and is \
                     delivered at least once, not exactly once",
                );
            }

            let identifiers: &[usize] = p
                .properties
                .as_ref()
                .map_or(&[], |pp| pp.subscription_identifiers.as_slice());
            let Some(routing) = inbound_routing(handle, &topic_str, identifiers).await else {
                return;
            };

            let inbound_payload = InboundPayload::new(p.payload, content_type);

            router
                .deliver_inbound(
                    client_slug,
                    &topic_str,
                    inbound_payload,
                    delivery_qos,
                    p.retain,
                    routing,
                )
                .await;
        }

        // --- Outgoing subscribe: bind pkid → pending filter (FIFO) ---
        //
        // A SUBSCRIBE written puts its filter back among the asserted ones in
        // either phase: the broker may hold it from now on, whatever an
        // UNSUBACK said before. One the library replays from an earlier
        // connection is settled again, since the held set may have changed
        // since it was submitted.
        Event::Outgoing(Outgoing::Subscribe(pkid)) => {
            let kind = RequestKind::Subscribe;
            let Some(PendingRequest {
                topic_filter: filter,
                epoch,
            }) = take_written_request(handle, kind, pkid)
            else {
                return;
            };
            handle.subscriptions.write().await.mark_asserted(&filter);
            match phase {
                ConnectionPhase::Live => {
                    written_on_live_connection(handle, &filter, epoch);
                    handle.inflight_subscribes.lock().await.insert(pkid, filter);
                }
                ConnectionPhase::Lost => written_on_lost_connection(handle, kind, pkid, &filter),
            }
        }

        // --- Outgoing unsubscribe: bind pkid → pending filter (FIFO) ---
        Event::Outgoing(Outgoing::Unsubscribe(pkid)) => {
            let kind = RequestKind::Unsubscribe;
            let Some(PendingRequest {
                topic_filter: filter,
                epoch,
            }) = take_written_request(handle, kind, pkid)
            else {
                return;
            };
            match phase {
                ConnectionPhase::Live => {
                    let seq = written_on_live_connection(handle, &filter, epoch);
                    handle
                        .inflight_unsubscribes
                        .lock()
                        .await
                        .insert(pkid, (filter, seq));
                }
                ConnectionPhase::Lost => written_on_lost_connection(handle, kind, pkid, &filter),
            }
        }

        // --- UnsubAck: take the filter out of the asserted set if withdrawn ---
        Event::Incoming(Incoming::UnsubAck(ack)) if phase == ConnectionPhase::Lost => {
            tracing::debug!(
                client = %client_slug,
                pkid = ack.pkid,
                "UNSUBACK of a lost connection; its outcomes are void",
            );
        }
        Event::Incoming(Incoming::UnsubAck(ack)) => {
            let attributed = handle.inflight_unsubscribes.lock().await.remove(&ack.pkid);
            let Some((filter, seq)) = attributed else {
                tracing::warn!(
                    client = %client_slug,
                    pkid = ack.pkid,
                    reasons = ?ack.reasons,
                    "UNSUBACK for a packet identifier no UNSUBSCRIBE was bound to",
                );
                return;
            };
            unsuback_resolved(handle, &filter, seq, &ack.reasons).await;
        }

        // --- SubAck: record and log the broker's answer for the attributed filter ---
        //
        // Each SUBSCRIBE carries one filter, so a SUBACK carries exactly one
        // return code.
        Event::Incoming(Incoming::SubAck(ack)) if phase == ConnectionPhase::Lost => {
            tracing::debug!(
                client = %client_slug,
                pkid = ack.pkid,
                "SUBACK of a lost connection; its outcomes are void",
            );
        }
        Event::Incoming(Incoming::SubAck(ack)) => {
            let attributed = handle.inflight_subscribes.lock().await.remove(&ack.pkid);
            let topic_filter = attributed.as_deref().unwrap_or(UNATTRIBUTED_FILTER);
            match ack.return_codes.as_slice() {
                [SubscribeReasonCode::Success(qos)] => {
                    tracing::info!(
                        client = %client_slug,
                        topic = %topic_filter,
                        granted_qos = ?qos,
                        "SUBACK: subscription granted",
                    );
                    if let Some(filter) = &attributed {
                        handle.record_grant(filter).await;
                    }
                }
                // A rejection is an authoritative broker decision (ACL
                // misconfiguration, broker policy): the bridge receives no
                // messages for this filter until it is fixed.
                [other] => {
                    tracing::error!(
                        client = %client_slug,
                        topic = %topic_filter,
                        reason = ?other,
                        "SUBACK: subscription rejected by ACL or broker — bridge will \
                         receive no messages for this filter",
                    );
                    if let Some(filter) = &attributed {
                        handle.record_refusal(filter, format!("{other:?}")).await;
                    }
                }
                // Brenn sends one filter per SUBSCRIBE, so a SUBACK with any
                // other number of return codes is a broker protocol violation.
                // The filter is recorded as refused rather than interpreted: a
                // grant read out of a packet this process cannot attribute to
                // one answer is the one wrong direction for this map. The
                // process is not taken down for a peer's malformed packet.
                codes => {
                    tracing::error!(
                        client = %client_slug,
                        topic = %topic_filter,
                        return_codes = ?codes,
                        "SUBACK carries {} return codes for a one-filter SUBSCRIBE — protocol \
                         violation; treating the filter as refused",
                        codes.len(),
                    );
                    if let Some(filter) = &attributed {
                        handle
                            .record_refusal(
                                filter,
                                format!(
                                    "SUBACK carried {} return codes for a one-filter SUBSCRIBE",
                                    codes.len()
                                ),
                            )
                            .await;
                    }
                }
            }
        }

        _ => {}
    }
}

/// Whether a request submitted on connection `epoch` and seen written now was
/// replayed by the library from an earlier connection. Read on the supervisor
/// task, the epoch's one writer, so it takes no lock.
fn replayed(handle: &MqttClientHandle, epoch: u64) -> bool {
    epoch != handle.connection_epoch.load(Ordering::Relaxed)
}

/// Act on the broker's answer to the UNSUBSCRIBE numbered `seq` for `filter`.
///
/// Success and No subscription existed take the filter out of the asserted
/// set when that UNSUBSCRIBE is still the newest request for the filter on
/// this connection and the filter is not held; otherwise it stays asserted.
/// Any other answer is logged at `error` and the filter stays asserted, to be
/// withdrawn again at the next connection that finds the session. Each
/// UNSUBSCRIBE carries one filter, so an UNSUBACK with any other number of
/// reason codes is a broker protocol violation; the process is not taken down
/// for a peer's malformed packet.
async fn unsuback_resolved(
    handle: &MqttClientHandle,
    filter: &str,
    seq: u64,
    reasons: &[rumqttc::UnsubAckReason],
) {
    use rumqttc::UnsubAckReason;
    let client_slug = handle.config.identity.slug.as_str();
    match reasons {
        [reason @ (UnsubAckReason::Success | UnsubAckReason::NoSubscriptionExisted)] => {
            let newest = handle
                .request_order
                .lock()
                .expect(REQUEST_ORDER_POISONED)
                .is_newest(filter, seq);
            let withdrawn = newest && handle.subscriptions.write().await.withdraw_asserted(filter);
            tracing::debug!(
                client = %client_slug,
                topic = %filter,
                reason = ?reason,
                withdrawn,
                "UNSUBACK: the broker holds no subscription for the filter",
            );
        }
        [other] => {
            tracing::error!(
                client = %client_slug,
                topic = %filter,
                reason = ?other,
                "UNSUBACK: the broker refused to withdraw the filter; it stays asserted and is \
                 withdrawn again at the next connection that finds the session",
            );
        }
        codes => {
            tracing::error!(
                client = %client_slug,
                topic = %filter,
                reason_codes = ?codes,
                "UNSUBACK carries {} reason codes for a one-filter UNSUBSCRIBE — protocol \
                 violation; the filter stays asserted",
                codes.len(),
            );
        }
    }
}

/// How an inbound PUBLISH to `topic` carrying `identifiers` is routed, or
/// `None` when it is dropped because none of its identifiers names a
/// subscription this client holds.
async fn inbound_routing(
    handle: &MqttClientHandle,
    topic: &str,
    identifiers: &[usize],
) -> Option<InboundRouting> {
    let client_slug = handle.config.identity.slug.as_str();
    if identifiers.is_empty() {
        // No `client` lock: this runs on the supervisor task, which is the only
        // writer of both flags (`install_connection`) and never installs a
        // connection while handling a packet, and a lost connection's packets
        // are handled before the next connection is installed, so they are the
        // flags of the connection the packet came on.
        let first_on_connection = handle.broker_sub_ids_available.load(Ordering::Relaxed)
            && !handle
                .missing_identifier_warned
                .swap(true, Ordering::Relaxed);
        if first_on_connection {
            tracing::warn!(
                client = %client_slug,
                topic = %topic,
                "a broker that supports Subscription Identifiers sent a PUBLISH without one: \
                 messages on this connection are routed by topic, and overlapping filters may \
                 deliver more than once",
            );
        }
        return Some(InboundRouting::ByTopic);
    }
    let resolved = handle
        .subscriptions
        .read()
        .await
        .filters_for(identifiers, topic);
    for id in &resolved.unknown {
        tracing::warn!(
            client = %client_slug,
            topic = %topic,
            subscription_identifier = id,
            "inbound PUBLISH names a Subscription Identifier no subscription on this client \
             holds: the copy was sent for a filter this process does not hold, or was queued by \
             the broker under an identifier an earlier Brenn version assigned — not routed for \
             that identifier",
        );
    }
    if resolved.filters.is_empty() {
        return None;
    }
    Some(InboundRouting::BySubscription(resolved.filters))
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

/// A client that never connects, with room for `capacity` requests on each
/// channel. Requests stay queued until the returned event loop is polled; with
/// the event loop dropped, every send fails as on a closed channel.
#[cfg(test)]
pub(crate) fn unpolled_client(capacity: usize) -> (AsyncClient, rumqttc::EventLoop) {
    let opts = MqttOptions::new("test-unpolled", ("127.0.0.1", 1));
    AsyncClient::builder(opts).capacity(capacity).build()
}

/// Make `client` the session's current client.
///
/// Under the `client` guard, in order: the broker's Maximum Packet Size
/// (`None`: none advertised, stored as `0`), Receive Maximum (`None`:
/// [`DEFAULT_RECEIVE_MAXIMUM`]) and whether it supports Subscription
/// Identifiers are stored, the missing-identifier and inbound-QoS-2 warnings
/// are re-armed, the connection epoch is incremented, and the client becomes
/// visible. A submitter that takes the guard therefore reads the facts of the
/// connection whose client it took. The egress wake is notified last.
pub(crate) async fn install_connection(
    handle: &MqttClientHandle,
    client: AsyncClient,
    broker_max_packet_size: Option<u32>,
    broker_receive_max: Option<u16>,
    broker_sub_ids_available: bool,
) {
    {
        let mut client_guard = handle.client.lock().await;
        handle
            .broker_max_packet_size
            .store(broker_max_packet_size.unwrap_or(0), Ordering::Relaxed);
        handle.broker_receive_max.store(
            broker_receive_max.unwrap_or(DEFAULT_RECEIVE_MAXIMUM),
            Ordering::Relaxed,
        );
        handle
            .broker_sub_ids_available
            .store(broker_sub_ids_available, Ordering::Relaxed);
        handle
            .missing_identifier_warned
            .store(false, Ordering::Relaxed);
        handle.qos2_inbound_warned.store(false, Ordering::Relaxed);
        handle.connection_epoch.fetch_add(1, Ordering::Relaxed);
        *client_guard = Some(client);
    }
    handle.egress_wake.notify_one();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::SubAckOutcome;
    use brenn_lib::messaging::Urgency;
    use brenn_lib::mqtt::subscription_identifier;

    fn channel(client: &str, topic: &str) -> ResolvedMqttIngressChannel {
        let address = brenn_lib::mqtt::config::parsed_address_canonical(client, topic);
        ResolvedMqttIngressChannel {
            channel_uuid: brenn_lib::messaging::mqtt_channel_uuid_from_address(&address),
            channel_address: address,
            client_slug: client.to_string(),
            topic: topic.to_string(),
            urgency: Urgency::Normal,
        }
    }

    // --- union_subscriptions ---

    #[test]
    fn union_empty_when_no_channels_for_client() {
        let channels = vec![channel("other", "a/b")];
        assert!(union_subscriptions("target", &channels).is_empty());
    }

    #[test]
    fn union_gives_each_filter_its_derived_identifier() {
        let channels = vec![
            channel("broker", "home/+/state"),
            channel("broker", "sensors/#"),
        ];
        let subs = union_subscriptions("broker", &channels);
        assert_eq!(
            subs,
            vec![
                IngressSubscription {
                    topic_filter: "home/+/state".into(),
                    sub_id: subscription_identifier("home/+/state"),
                },
                IngressSubscription {
                    topic_filter: "sensors/#".into(),
                    sub_id: subscription_identifier("sensors/#"),
                },
            ]
        );
    }

    #[test]
    fn union_dedups_a_shared_filter_in_first_seen_order() {
        let channels = vec![
            channel("broker", "home/+/state"),
            channel("broker", "sensors/#"),
            channel("broker", "home/+/state"),
        ];
        let subs = union_subscriptions("broker", &channels);
        assert_eq!(
            subs,
            vec![
                IngressSubscription::new("home/+/state"),
                IngressSubscription::new("sensors/#"),
            ]
        );
    }

    #[test]
    fn union_filters_out_other_clients() {
        let channels = vec![
            channel("broker-a", "a/#"),
            channel("broker-b", "b/#"),
            channel("broker-a", "c/#"),
        ];
        let subs = union_subscriptions("broker-a", &channels);
        assert_eq!(subs.len(), 2);
        assert_eq!(subs[0].topic_filter, "a/#");
        assert_eq!(subs[1].topic_filter, "c/#");
    }

    // --- SubAck pkid → filter attribution ---

    fn test_handle() -> Arc<MqttClientHandle> {
        let (stop_tx, _rx) = tokio::sync::watch::channel(false);
        let config = Arc::new(brenn_lib::mqtt::test_support::test_client_config("broker"));
        MqttClientHandle::new(config, vec![], stop_tx)
    }

    /// A FIFO entry for `topic_filter` submitted on connection epoch 0, the
    /// epoch of a handle no connection has been installed on.
    fn pending(topic_filter: &str) -> PendingRequest {
        PendingRequest {
            topic_filter: topic_filter.to_string(),
            epoch: 0,
        }
    }

    struct NullRouter;
    #[async_trait::async_trait]
    impl MqttEventRouter for NullRouter {
        fn ready(&self) -> bool {
            true
        }

        async fn deliver_inbound(
            &self,
            _client_slug: &str,
            _topic: &str,
            _payload: crate::payload::InboundPayload,
            _qos: u8,
            _retained: bool,
            _routing: InboundRouting,
        ) {
        }

        async fn session_fault(&self, client_slug: &str, fault: SessionFault) {
            panic!(
                "no test on this router provokes a session fault; got {fault:?} on {client_slug}"
            );
        }
    }

    /// A router that keeps every session fault it is told of.
    #[derive(Default)]
    struct FaultRouter {
        faults: std::sync::Mutex<Vec<(String, SessionFault)>>,
    }

    #[async_trait::async_trait]
    impl MqttEventRouter for FaultRouter {
        fn ready(&self) -> bool {
            true
        }

        async fn deliver_inbound(
            &self,
            client_slug: &str,
            topic: &str,
            _payload: crate::payload::InboundPayload,
            _qos: u8,
            _retained: bool,
            _routing: InboundRouting,
        ) {
            panic!("no test on this router delivers; got {topic:?} on {client_slug}");
        }

        async fn session_fault(&self, client_slug: &str, fault: SessionFault) {
            self.faults
                .lock()
                .unwrap()
                .push((client_slug.to_string(), fault));
        }
    }

    /// An authoritative loss is reported as `Failed` with its reason; the
    /// give-up keeps its own fault, a discarded session its own, and a loss
    /// the supervisor reconnects after reports nothing.
    #[tokio::test]
    async fn a_loss_reports_the_fault_its_verdict_names() {
        let cases = [
            (LossVerdict::Reconnect, None),
            (
                LossVerdict::Authoritative,
                Some(SessionFault::Failed {
                    reason: "the reason".to_string(),
                }),
            ),
            (
                LossVerdict::DiscardSession { pkid: 7 },
                Some(SessionFault::SessionDiscarded { pkid: 7 }),
            ),
            (
                LossVerdict::GiveUp { pkid: 7 },
                Some(SessionFault::GaveUp {
                    reason: "the reason".to_string(),
                }),
            ),
        ];
        for (verdict, expected) in cases {
            let router = FaultRouter::default();
            report_loss(&router, "home", &verdict, "the reason").await;
            let faults = router.faults.into_inner().unwrap();
            let expected: Vec<_> = expected
                .into_iter()
                .map(|fault| ("home".to_string(), fault))
                .collect();
            assert_eq!(faults, expected, "{verdict:?}");
        }
    }

    /// A router that keeps each call's topic and routing, and its delivery QoS
    /// and RETAIN flag apart.
    #[derive(Default)]
    struct RecordingRouter {
        calls: std::sync::Mutex<Vec<(String, InboundRouting)>>,
        qos: std::sync::Mutex<Vec<u8>>,
        retained: std::sync::Mutex<Vec<bool>>,
    }

    impl RecordingRouter {
        fn take(&self) -> Vec<(String, InboundRouting)> {
            std::mem::take(&mut *self.calls.lock().unwrap())
        }

        fn take_qos(&self) -> Vec<u8> {
            std::mem::take(&mut *self.qos.lock().unwrap())
        }

        fn take_retained(&self) -> Vec<bool> {
            std::mem::take(&mut *self.retained.lock().unwrap())
        }
    }

    #[async_trait::async_trait]
    impl MqttEventRouter for RecordingRouter {
        fn ready(&self) -> bool {
            true
        }

        async fn deliver_inbound(
            &self,
            _client_slug: &str,
            topic: &str,
            _payload: crate::payload::InboundPayload,
            qos: u8,
            retained: bool,
            routing: InboundRouting,
        ) {
            self.qos.lock().unwrap().push(qos);
            self.retained.lock().unwrap().push(retained);
            self.calls
                .lock()
                .unwrap()
                .push((topic.to_string(), routing));
        }

        async fn session_fault(&self, client_slug: &str, fault: SessionFault) {
            panic!(
                "no test on this router provokes a session fault; got {fault:?} on {client_slug}"
            );
        }
    }

    /// A stand-in supervisor for a predecessor a handover is about to stop: it
    /// holds no connection and exits on the stop signal, which is all
    /// `stop_and_join` needs of it.
    fn stand_in_supervisor(handle: &Arc<MqttClientHandle>) -> tokio::task::JoinHandle<()> {
        let mut stop_rx = handle.stop_tx.subscribe();
        tokio::spawn(async move {
            while !*stop_rx.borrow_and_update() {
                if stop_rx.changed().await.is_err() {
                    return;
                }
            }
        })
    }

    /// A stand-in supervisor that lingers for `delay` after the stop signal, so
    /// a test can act inside the handover's drain window.
    fn lingering_stand_in_supervisor(
        handle: &Arc<MqttClientHandle>,
        delay: Duration,
    ) -> tokio::task::JoinHandle<()> {
        let mut stop_rx = handle.stop_tx.subscribe();
        tokio::spawn(async move {
            while !*stop_rx.borrow_and_update() {
                if stop_rx.changed().await.is_err() {
                    return;
                }
            }
            tokio::time::sleep(delay).await;
        })
    }

    /// A filter edit that resolves the *successor* while the handover is still
    /// draining the predecessor survives the inherit.
    ///
    /// This is the other interleaving. The registry hands the successor out from
    /// `add_client`, which is before the join, so an edit arriving in that
    /// window writes into the successor's own set — and the inherit that follows
    /// is an assignment. Were the successor's set not claimed before the join,
    /// that assignment would discard the edit: a lost `add` is a subscription
    /// the broker never hears about and no later move re-asserts, and a lost
    /// `remove` is a filter the document no longer binds re-asserted at the
    /// broker forever.
    ///
    /// Staged rather than raced: the predecessor's supervisor lingers after the
    /// stop signal, so the edit runs inside the drain.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_edit_reaching_the_successor_mid_handover_survives_the_inherit() {
        let service = MqttService::new();
        let old = test_handle();
        old.add_subscription("home/state".to_string()).await;
        old.set_supervisor(lingering_stand_in_supervisor(
            &old,
            Duration::from_millis(400),
        ))
        .await;
        service.add_client(old.clone());

        let config = Arc::new(brenn_lib::mqtt::test_support::test_client_config("broker"));
        let router: Arc<dyn MqttEventRouter> = Arc::new(NullRouter);
        let handover_service = service.clone();
        let predecessor = old.clone();
        let handover = tokio::spawn(async move {
            register_and_spawn(
                &handover_service,
                config,
                ArrivingFilters::Successor(&predecessor),
                router.clone(),
            )
            .await
        });

        // The successor is registered by now and the drain is still running.
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            !Arc::ptr_eq(
                &service
                    .get_client("broker")
                    .expect("a session for the slug"),
                &old,
            ),
            "the registry answers with the successor before the drain finishes, which is what              makes this window reachable",
        );
        let edit = service
            .subscribe_filter("broker", "attic/+".to_string())
            .await;
        assert!(edit.is_some(), "the slug is registered throughout");

        let successor = handover.await.expect("the handover task");
        let filters: Vec<String> = successor
            .subscriptions
            .read()
            .await
            .iter()
            .map(|sub| sub.topic_filter.clone())
            .collect();
        assert!(
            filters.contains(&"home/state".to_string()),
            "the predecessor's own filter is inherited: {filters:?}",
        );
        assert!(
            filters.contains(&"attic/+".to_string()),
            "and the edit that landed on the successor mid-handover is still there: {filters:?}",
        );
        assert!(
            !old.subscriptions
                .read()
                .await
                .iter()
                .any(|sub| sub.topic_filter == "attic/+"),
            "the edit resolved the successor, not the predecessor — the interleaving under test",
        );
        let mut expected = old.subscriptions.read().await.to_vec();
        expected.push(IngressSubscription::new("attic/+"));
        assert_eq!(
            successor.subscriptions.read().await.to_vec(),
            expected,
            "the successor holds the predecessor's entries and the mid-handover edit",
        );
        successor.stop_and_join().await;
    }

    /// A successor inherits the predecessor's filter set, and it inherits a
    /// filter a concurrent subscribe adds while the handover is in flight.
    ///
    /// The window is the reason the inheritance is copied after the join rather
    /// than before it: a set copied at the top of the handover is missing that
    /// filter, and nothing ever re-asserts it — the durable row behind it is on
    /// both sides of every later reload's comparison, so it produces no move,
    /// and the process holds a subscription the broker never hears about.
    ///
    /// The concurrency is staged rather than raced: the test holds the
    /// predecessor's subscription set, which parks the subscribe inside
    /// `add_subscription`, and releases it once the handover has begun.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_successor_inherits_a_filter_added_during_the_handover() {
        let service = MqttService::new();
        let old = test_handle();
        old.add_subscription("home/state".to_string()).await;
        old.set_supervisor(stand_in_supervisor(&old)).await;
        service.add_client(old.clone());

        // Park a subscribe inside the predecessor's set.
        let held = old.subscriptions.write().await;
        let edit_service = service.clone();
        let edit = tokio::spawn(async move {
            edit_service
                .subscribe_filter("broker", "attic/+".to_string())
                .await
        });
        tokio::time::sleep(Duration::from_millis(100)).await;

        let config = Arc::new(brenn_lib::mqtt::test_support::test_client_config("broker"));
        let router: Arc<dyn MqttEventRouter> = Arc::new(NullRouter);
        let predecessor = old.clone();
        let handover = tokio::spawn(async move {
            register_and_spawn(
                &service,
                config,
                ArrivingFilters::Successor(&predecessor),
                router.clone(),
            )
            .await
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        drop(held);

        assert!(
            edit.await.expect("the subscribe task").is_some(),
            "the slug is registered throughout, so the edit resolves a session",
        );
        let successor = handover.await.expect("the handover task");
        let filters: Vec<String> = successor
            .subscriptions
            .read()
            .await
            .iter()
            .map(|sub| sub.topic_filter.clone())
            .collect();
        assert!(
            filters.contains(&"home/state".to_string()),
            "the predecessor's own filter is inherited: {filters:?}",
        );
        assert!(
            filters.contains(&"attic/+".to_string()),
            "and so is the one added while the handover ran: {filters:?}",
        );
        assert!(
            old.subscriptions
                .read()
                .await
                .iter()
                .any(|sub| sub.topic_filter == "attic/+"),
            "the late edit landed on the predecessor, which is the interleaving under test: an \
             edit that started after the swap would not be on this set at all",
        );
        assert_eq!(
            *successor.subscriptions.read().await,
            *old.subscriptions.read().await,
            "the successor's set is the predecessor's",
        );
        successor.stop_and_join().await;
    }

    /// A successor configured at a different `qos` holds each inherited filter
    /// under the same identifier, and answers its own `qos` for it: the set
    /// carries filters only, so what the successor's connect asserts is at the
    /// successor's configuration.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_successor_at_another_qos_holds_the_inherited_filter_at_its_own() {
        let service = MqttService::new();
        let old = test_handle();
        assert_eq!(old.config.identity.qos, 1);
        old.add_subscription("home/state".to_string()).await;
        old.set_supervisor(stand_in_supervisor(&old)).await;
        service.add_client(old.clone());
        assert_eq!(
            service.ingress_filter_qos("broker", "home/state").await,
            Some(1)
        );

        let mut config = brenn_lib::mqtt::test_support::test_client_config("broker");
        config.identity.qos = 0;
        let router: Arc<dyn MqttEventRouter> = Arc::new(NullRouter);
        let successor = register_and_spawn(
            &service,
            Arc::new(config),
            ArrivingFilters::Successor(&old),
            router,
        )
        .await;

        assert_eq!(
            successor.subscriptions.read().await.get("home/state"),
            Some(&IngressSubscription::new("home/state")),
            "the inherited filter keeps its identifier",
        );
        assert_eq!(
            service.ingress_filter_qos("broker", "home/state").await,
            Some(0),
            "the reader follows the registered successor's configuration",
        );
        successor.stop_and_join().await;
    }

    fn sub(topic_filter: &str) -> IngressSubscription {
        IngressSubscription::new(topic_filter)
    }

    /// The supervisor's SUBSCRIBE send returns at once on a full request
    /// channel rather than waiting for a poll only the supervisor can make, and
    /// records nothing for attribution when it did not send.
    #[test]
    fn try_assert_does_not_block_on_a_full_channel() {
        let handle = test_handle();
        let (client, _eventloop) = unpolled_client(1);

        assert_eq!(
            try_assert_ingress_subscription(&handle, &client, &sub("home/a"), true),
            Ok(SubscribeSend::Sent),
        );
        assert_eq!(
            handle
                .pending_subscribes
                .lock()
                .unwrap()
                .iter()
                .map(|p| p.topic_filter.clone())
                .collect::<Vec<_>>(),
            vec!["home/a".to_string()],
        );

        assert_eq!(
            try_assert_ingress_subscription(&handle, &client, &sub("home/b"), true),
            Ok(SubscribeSend::ChannelFull),
        );
        assert_eq!(
            handle
                .pending_subscribes
                .lock()
                .unwrap()
                .iter()
                .map(|p| p.topic_filter.clone())
                .collect::<Vec<_>>(),
            vec!["home/a".to_string()],
            "the unsent SUBSCRIBE left no attribution entry behind",
        );
    }

    /// A filter rumqttc refuses is an error, not a full channel, so a retry
    /// loop cannot spin on it, and it is never pushed for attribution.
    #[test]
    fn try_assert_rejects_a_filter_rumqttc_would_reject() {
        let handle = test_handle();
        let (client, _eventloop) = unpolled_client(4);

        let result = try_assert_ingress_subscription(&handle, &client, &sub("a/#/b"), true);
        assert!(
            matches!(result, Err(ref e) if e.contains("a/#/b")),
            "{result:?}"
        );
        assert!(handle.pending_subscribes.lock().unwrap().is_empty());
    }

    /// A body the watchdog respawns after a panic while connected inherits the
    /// dead body's client, whose event loop is gone. A stop requested before
    /// the respawned body's first check still ends the supervisor promptly in a
    /// terminal state, with the dead client dropped.
    #[tokio::test]
    async fn a_stop_after_a_panicked_connected_body_ends_the_supervisor() {
        let handle = test_handle();
        let (client, eventloop) = unpolled_client(1);
        drop(eventloop);
        *handle.client.lock().await = Some(client);
        handle.set_state(SupervisorState::Connected);
        let stop_rx = handle.stop_tx.subscribe();
        handle.stop_tx.send(true).expect("stop_rx is alive");

        tokio::time::timeout(
            Duration::from_secs(2),
            supervisor_body(handle.clone(), Arc::new(NullRouter), stop_rx),
        )
        .await
        .expect("the supervisor did not stop");

        assert!(handle.client.lock().await.is_none());
        assert_eq!(
            handle.state().state,
            SupervisorState::Disconnected { last_error: None }
        );
    }

    /// The watchdog's pause before a respawn ends at a stop: no fixed sleep is
    /// reachable after the stop is requested, whether it lands before the pause
    /// or during it.
    #[tokio::test(start_paused = true)]
    async fn the_respawn_pause_does_not_delay_a_stop() {
        let (stop_tx, mut stop_rx) = tokio::sync::watch::channel(false);
        let start = tokio::time::Instant::now();
        respawn_pause(&mut stop_rx).await;
        assert_eq!(start.elapsed(), RESPAWN_PAUSE, "with no stop it pauses");

        let start = tokio::time::Instant::now();
        let pause = tokio::spawn(async move {
            respawn_pause(&mut stop_rx).await;
            stop_rx
        });
        tokio::task::yield_now().await;
        stop_tx.send_replace(true);
        let mut stop_rx = pause.await.expect("the pause task");
        assert_eq!(
            start.elapsed(),
            Duration::ZERO,
            "a stop during the pause ends it"
        );

        respawn_pause(&mut stop_rx).await;
        assert_eq!(
            start.elapsed(),
            Duration::ZERO,
            "a stop before the pause skips it"
        );
    }

    /// Two SUBSCRIBEs bound to distinct pkids resolve to their own filters at
    /// SubAck time — the second filter is not mis-attributed to the first (the
    /// positional-index bug the merge fixes).
    #[tokio::test]
    async fn suback_resolves_bound_filter_by_pkid() {
        let handle = test_handle();
        let router: Arc<dyn MqttEventRouter> = Arc::new(NullRouter);

        // Simulate two outbound subscribes: push two pending filters (as the
        // subscribe helper would), then bind them to pkids 10 and 11 in order.
        handle
            .pending_subscribes
            .lock()
            .unwrap()
            .push_back(pending("home/+/state"));
        handle
            .pending_subscribes
            .lock()
            .unwrap()
            .push_back(pending("sensors/#"));

        handle_event(
            Event::Outgoing(Outgoing::Subscribe(10)),
            &handle,
            &router,
            ConnectionPhase::Live,
        )
        .await;
        handle_event(
            Event::Outgoing(Outgoing::Subscribe(11)),
            &handle,
            &router,
            ConnectionPhase::Live,
        )
        .await;

        // The second pkid must map to the second filter.
        assert_eq!(
            handle.inflight_subscribes.lock().await.get(&11).cloned(),
            Some("sensors/#".to_string()),
        );

        // SubAck for pkid 11 resolves and removes the second filter.
        let ack = rumqttc::SubAck {
            pkid: 11,
            return_codes: vec![SubscribeReasonCode::Success(QoS::AtLeastOnce)],
            properties: None,
        };
        handle_event(
            Event::Incoming(Incoming::SubAck(ack)),
            &handle,
            &router,
            ConnectionPhase::Live,
        )
        .await;
        assert!(handle.inflight_subscribes.lock().await.get(&11).is_none());
        // The first filter's binding is untouched.
        assert_eq!(
            handle.inflight_subscribes.lock().await.get(&10).cloned(),
            Some("home/+/state".to_string()),
        );
    }

    /// Deliver a SUBACK for `pkid` carrying `codes`.
    async fn suback(
        handle: &Arc<MqttClientHandle>,
        router: &Arc<dyn MqttEventRouter>,
        pkid: u16,
        codes: Vec<SubscribeReasonCode>,
    ) {
        handle_event(
            Event::Incoming(Incoming::SubAck(rumqttc::SubAck {
                pkid,
                return_codes: codes,
                properties: None,
            })),
            handle,
            router,
            ConnectionPhase::Live,
        )
        .await;
    }

    /// A granted SubAck records its attributed filter, a rejected one records the
    /// broker's refusal, an unattributed one records nothing, and the per-connect
    /// clear empties the map — the four facts the broker suite's grant wait rests
    /// on.
    #[tokio::test]
    async fn suback_records_only_granted_filters() {
        let handle = test_handle();
        let router: Arc<dyn MqttEventRouter> = Arc::new(NullRouter);
        handle.add_subscription("home/+/state".into()).await;
        handle.add_subscription("sensors/#".into()).await;

        handle
            .pending_subscribes
            .lock()
            .unwrap()
            .push_back(pending("home/+/state"));
        handle
            .pending_subscribes
            .lock()
            .unwrap()
            .push_back(pending("sensors/#"));
        handle_event(
            Event::Outgoing(Outgoing::Subscribe(10)),
            &handle,
            &router,
            ConnectionPhase::Live,
        )
        .await;
        handle_event(
            Event::Outgoing(Outgoing::Subscribe(11)),
            &handle,
            &router,
            ConnectionPhase::Live,
        )
        .await;

        suback(
            &handle,
            &router,
            10,
            vec![SubscribeReasonCode::Success(QoS::AtLeastOnce)],
        )
        .await;
        suback(&handle, &router, 11, vec![SubscribeReasonCode::Failure]).await;
        // pkid 12 was never bound: the broker answered a SUBSCRIBE this session
        // does not hold, which is what a SubAck landing after the per-connect
        // clear looks like.
        suback(
            &handle,
            &router,
            12,
            vec![SubscribeReasonCode::Success(QoS::AtLeastOnce)],
        )
        .await;

        assert!(handle.is_granted("home/+/state").await);
        assert!(
            !handle.is_granted("sensors/#").await,
            "a broker rejection is not a grant",
        );
        assert_eq!(
            handle.subscribe_outcome("sensors/#").await,
            Some(SubAckOutcome::Refused("Failure".to_string())),
            "the refusal carries the SUBACK's own words",
        );
        assert_eq!(
            handle.subscribe_outcome(UNATTRIBUTED_FILTER).await,
            None,
            "an unattributed SubAck records nothing under the log's display value",
        );

        clear_connection_tracking(&handle).await;
        assert!(!handle.is_granted("home/+/state").await);
        assert_eq!(handle.subscribe_outcome("sensors/#").await, None);
    }

    /// A SubAck that arrives after its filter was taken down records nothing: the
    /// UNSUBSCRIBE and the SUBACK crossed, and the broker holds no filter for it
    /// once the UnsubAck lands. Without this the grant would outlive the
    /// subscription and answer a later wait before its own SUBSCRIBE went out.
    #[tokio::test]
    async fn suback_for_an_unsubscribed_filter_records_nothing() {
        let handle = test_handle();
        let router: Arc<dyn MqttEventRouter> = Arc::new(NullRouter);
        handle.add_subscription("home/+/state".into()).await;
        handle
            .pending_subscribes
            .lock()
            .unwrap()
            .push_back(pending("home/+/state"));
        handle_event(
            Event::Outgoing(Outgoing::Subscribe(10)),
            &handle,
            &router,
            ConnectionPhase::Live,
        )
        .await;

        assert!(handle.remove_subscription("home/+/state").await);
        suback(
            &handle,
            &router,
            10,
            vec![SubscribeReasonCode::Success(QoS::AtLeastOnce)],
        )
        .await;

        assert!(!handle.is_granted("home/+/state").await);
        assert_eq!(handle.subscribe_outcome("home/+/state").await, None);
    }

    /// Brenn sends one filter per SUBSCRIBE, so a multi-code SUBACK is a broker
    /// protocol violation: the attributed filter is refused, never granted off a
    /// `Success` sitting beside a `Failure`.
    #[tokio::test]
    async fn suback_with_several_return_codes_refuses_the_filter() {
        let handle = test_handle();
        let router: Arc<dyn MqttEventRouter> = Arc::new(NullRouter);
        handle.add_subscription("home/+/state".into()).await;
        handle
            .pending_subscribes
            .lock()
            .unwrap()
            .push_back(pending("home/+/state"));
        handle_event(
            Event::Outgoing(Outgoing::Subscribe(10)),
            &handle,
            &router,
            ConnectionPhase::Live,
        )
        .await;

        suback(
            &handle,
            &router,
            10,
            vec![
                SubscribeReasonCode::Failure,
                SubscribeReasonCode::Success(QoS::AtLeastOnce),
            ],
        )
        .await;

        assert!(!handle.is_granted("home/+/state").await);
        assert!(matches!(
            handle.subscribe_outcome("home/+/state").await,
            Some(SubAckOutcome::Refused(reason)) if reason.contains("2 return codes"),
        ));
    }

    /// Installing a connection stores the broker's Maximum Packet Size and
    /// Receive Maximum, advances the epoch, makes the client visible and
    /// leaves a wake. A CONNACK that states neither stores `0` and the MQTT 5
    /// default.
    #[tokio::test]
    async fn install_connection_records_the_connection_and_wakes_egress() {
        let handle = test_handle();
        assert_eq!(handle.connection_epoch.load(Ordering::Relaxed), 0);

        let (client, _eventloop) = unpolled_client(4);
        install_connection(&handle, client, Some(4096), Some(10), true).await;
        assert!(handle.client.lock().await.is_some());
        assert_eq!(handle.broker_max_packet_size.load(Ordering::Relaxed), 4096);
        assert_eq!(handle.broker_receive_max.load(Ordering::Relaxed), 10);
        assert_eq!(handle.connection_epoch.load(Ordering::Relaxed), 1);
        tokio::time::timeout(Duration::from_secs(1), handle.egress_wake.notified())
            .await
            .expect("install leaves a wake");

        let (client, _eventloop2) = unpolled_client(4);
        install_connection(&handle, client, None, None, false).await;
        assert_eq!(handle.broker_max_packet_size.load(Ordering::Relaxed), 0);
        assert_eq!(
            handle.broker_receive_max.load(Ordering::Relaxed),
            DEFAULT_RECEIVE_MAXIMUM
        );
        assert_eq!(handle.connection_epoch.load(Ordering::Relaxed), 2);
        assert!(!handle.broker_sub_ids_available.load(Ordering::Relaxed));
    }

    // --- Subscription Identifiers: SUBSCRIBE, CONNACK and the inbound arm ---

    #[test]
    fn subscribe_request_carries_the_identifier_only_when_asked() {
        let (filter, props) = subscribe_request("broker", &sub("home/a"), 1, true);
        assert_eq!(filter.path, "home/a");
        assert_eq!(props.id, Some(subscription_identifier("home/a") as usize));
        let (_, props) = subscribe_request("broker", &sub("home/a"), 1, false);
        assert_eq!(props.id, None);
    }

    #[test]
    #[should_panic(expected = "with QoS 2, which config validation refuses")]
    fn subscribe_request_at_qos_2_panics() {
        subscribe_request("broker", &sub("home/a"), 2, true);
    }

    #[test]
    fn the_connack_says_identifiers_are_unavailable_only_with_a_zero() {
        let props = |available: Option<u8>| rumqttc::ConnAckProperties {
            session_expiry_interval: None,
            receive_max: None,
            max_qos: None,
            retain_available: None,
            max_packet_size: None,
            assigned_client_identifier: None,
            topic_alias_max: None,
            reason_string: None,
            user_properties: Vec::new(),
            wildcard_subscription_available: None,
            subscription_identifiers_available: available,
            shared_subscription_available: None,
            server_keep_alive: None,
            response_information: None,
            server_reference: None,
            authentication_method: None,
            authentication_data: None,
        };
        assert!(!sub_ids_available(Some(&props(Some(0)))));
        assert!(sub_ids_available(Some(&props(Some(1)))));
        assert!(sub_ids_available(Some(&props(None))));
        assert!(sub_ids_available(None));
    }

    /// The identifier `topic_filter` is subscribed with, as a PUBLISH carries
    /// it.
    fn id(topic_filter: &str) -> usize {
        subscription_identifier(topic_filter) as usize
    }

    /// An identifier none of `a/#`, `a/b` and `a/c` has.
    const UNKNOWN: usize = 9;

    /// A handle holding `a/#` and `a/b`, with a connection installed whose
    /// broker does or does not support Subscription Identifiers.
    async fn routing_handle(
        sub_ids_available: bool,
    ) -> (Arc<MqttClientHandle>, rumqttc::EventLoop) {
        for filter in ["a/#", "a/b", "a/c"] {
            assert_ne!(id(filter), UNKNOWN);
        }
        let handle = test_handle();
        handle.add_subscription("a/#".into()).await;
        handle.add_subscription("a/b".into()).await;
        let (client, eventloop) = unpolled_client(4);
        install_connection(&handle, client, None, None, sub_ids_available).await;
        (handle, eventloop)
    }

    /// Hand the arm a PUBLISH to `topic` with `properties`.
    async fn inbound(
        handle: &Arc<MqttClientHandle>,
        router: &Arc<RecordingRouter>,
        topic: &str,
        properties: Option<rumqttc::PublishProperties>,
    ) {
        let mut publish = rumqttc::Publish::new(topic, QoS::AtLeastOnce, "v", properties);
        publish.pkid = 1;
        let router: Arc<dyn MqttEventRouter> = router.clone();
        handle_event(
            Event::Incoming(Incoming::Publish(publish)),
            handle,
            &router,
            ConnectionPhase::Live,
        )
        .await;
    }

    /// Hand the arm a PUBLISH to `a/b` carrying `ids`.
    async fn inbound_ids(
        handle: &Arc<MqttClientHandle>,
        router: &Arc<RecordingRouter>,
        ids: &[usize],
    ) {
        let properties = rumqttc::PublishProperties {
            subscription_identifiers: ids.to_vec(),
            ..Default::default()
        };
        inbound(handle, router, "a/b", Some(properties)).await;
    }

    fn by_subscription(filters: &[&str]) -> Vec<(String, InboundRouting)> {
        vec![(
            "a/b".to_string(),
            InboundRouting::BySubscription(filters.iter().map(|f| f.to_string()).collect()),
        )]
    }

    /// A PUBLISH with no identifier is routed by topic, and the first one on a
    /// connection whose broker supports identifiers arms the warning once.
    #[tokio::test]
    async fn a_publish_with_no_identifier_is_routed_by_topic() {
        let (handle, _eventloop) = routing_handle(true).await;
        let router = Arc::new(RecordingRouter::default());
        let by_topic = || vec![("a/b".to_string(), InboundRouting::ByTopic)];

        assert!(!handle.missing_identifier_warned.load(Ordering::Relaxed));
        inbound(&handle, &router, "a/b", None).await;
        assert_eq!(router.take(), by_topic());
        assert!(handle.missing_identifier_warned.load(Ordering::Relaxed));

        inbound(
            &handle,
            &router,
            "a/b",
            Some(rumqttc::PublishProperties::default()),
        )
        .await;
        assert_eq!(router.take(), by_topic());
        assert!(handle.missing_identifier_warned.load(Ordering::Relaxed));

        let (client, _eventloop2) = unpolled_client(4);
        install_connection(&handle, client, None, None, true).await;
        assert!(
            !handle.missing_identifier_warned.load(Ordering::Relaxed),
            "a new connection re-arms the warning",
        );
    }

    /// From a broker that does not support identifiers, a PUBLISH without one
    /// is what is expected: routed by topic, and no warning armed.
    #[tokio::test]
    async fn a_broker_without_identifiers_routes_by_topic_without_warning() {
        let (handle, _eventloop) = routing_handle(false).await;
        let router = Arc::new(RecordingRouter::default());
        inbound(&handle, &router, "a/b", None).await;
        inbound(
            &handle,
            &router,
            "a/b",
            Some(rumqttc::PublishProperties::default()),
        )
        .await;
        assert_eq!(
            router.take(),
            vec![
                ("a/b".to_string(), InboundRouting::ByTopic),
                ("a/b".to_string(), InboundRouting::ByTopic),
            ],
        );
        assert!(!handle.missing_identifier_warned.load(Ordering::Relaxed));
    }

    /// Identifiers resolve to the filters they are derived from; unknown ones
    /// are left out, and a packet naming only unknown ones never reaches the
    /// router.
    #[tokio::test]
    async fn identifiers_resolve_to_their_filters() {
        let (handle, _eventloop) = routing_handle(true).await;
        let router = Arc::new(RecordingRouter::default());

        inbound_ids(&handle, &router, &[id("a/b")]).await;
        assert_eq!(router.take(), by_subscription(&["a/b"]));

        inbound_ids(&handle, &router, &[id("a/#"), id("a/b")]).await;
        assert_eq!(router.take(), by_subscription(&["a/#", "a/b"]));

        inbound_ids(&handle, &router, &[id("a/b"), UNKNOWN]).await;
        assert_eq!(router.take(), by_subscription(&["a/b"]));

        inbound_ids(&handle, &router, &[UNKNOWN]).await;
        assert_eq!(router.take(), vec![]);
        assert!(
            !handle.missing_identifier_warned.load(Ordering::Relaxed),
            "a packet that carried identifiers is not a missing-identifier packet",
        );
    }

    /// A removed filter's identifier resolves to nothing while the filter is
    /// not held, another filter's add does not take it, and the filter added
    /// back resolves under it again.
    #[tokio::test]
    async fn a_removed_filters_identifier_is_dropped_until_the_filter_is_held_again() {
        let (handle, _eventloop) = routing_handle(true).await;
        let router = Arc::new(RecordingRouter::default());

        assert!(handle.remove_subscription("a/b").await);
        inbound_ids(&handle, &router, &[id("a/b")]).await;
        assert_eq!(router.take(), vec![]);

        handle.add_subscription("a/c".into()).await;
        inbound_ids(&handle, &router, &[id("a/b")]).await;
        assert_eq!(router.take(), vec![]);
        inbound_ids(&handle, &router, &[id("a/c")]).await;
        assert_eq!(router.take(), by_subscription(&["a/c"]));

        handle.add_subscription("a/b".into()).await;
        inbound_ids(&handle, &router, &[id("a/b")]).await;
        assert_eq!(router.take(), by_subscription(&["a/b"]));
    }

    /// A copy carrying an identifier two held filters share goes to both when
    /// both cover its topic, in one call.
    #[tokio::test]
    async fn a_shared_identifier_resolves_to_every_holder_covering_the_topic() {
        let (wide, narrow) = ("k/+/84471", "k/282/+");
        assert_eq!(id(wide), id(narrow), "the fixture pair must collide");
        let handle = test_handle();
        handle.add_subscription(wide.into()).await;
        handle.add_subscription(narrow.into()).await;
        let (client, _eventloop) = unpolled_client(4);
        install_connection(&handle, client, None, None, true).await;
        let router = Arc::new(RecordingRouter::default());

        let properties = rumqttc::PublishProperties {
            subscription_identifiers: vec![id(wide)],
            ..Default::default()
        };
        inbound(&handle, &router, "k/282/84471", Some(properties)).await;
        assert_eq!(
            router.take(),
            vec![(
                "k/282/84471".to_string(),
                InboundRouting::BySubscription(vec![wide.to_string(), narrow.to_string()]),
            )],
        );
    }

    /// Every outgoing event wakes the egress side: the event loop emits one
    /// for each request it takes off the channel, whatever the request's
    /// kind, so each is a request slot that may have freed. A full channel of
    /// SUBSCRIBEs drains through `Outgoing::Subscribe` alone.
    #[tokio::test]
    async fn every_outgoing_event_wakes_egress() {
        let handle = test_handle();
        let router: Arc<dyn MqttEventRouter> = Arc::new(NullRouter);
        for outgoing in [
            Outgoing::Publish(0),
            Outgoing::Publish(3),
            Outgoing::AwaitAck(4),
            Outgoing::Subscribe(5),
            Outgoing::Unsubscribe(6),
            Outgoing::PubAck(7),
            Outgoing::PubRec(8),
            Outgoing::PubRel(9),
            Outgoing::PubComp(10),
            Outgoing::PingReq,
        ] {
            handle_event(
                Event::Outgoing(outgoing.clone()),
                &handle,
                &router,
                ConnectionPhase::Live,
            )
            .await;
            tokio::time::timeout(Duration::from_secs(1), handle.egress_wake.notified())
                .await
                .unwrap_or_else(|_| panic!("{outgoing:?} leaves no wake"));
        }
    }

    /// A client connected to an in-process broker that answers the CONNECT and
    /// then reads every packet without answering it; each SUBSCRIBE it reads
    /// arrives on the receiver. Its event loop is polled from a spawned task.
    async fn subscribe_capturing_client() -> (
        rumqttc::AsyncClient,
        tokio::sync::mpsc::UnboundedReceiver<rumqttc::Subscribe>,
    ) {
        use bytes::BytesMut;
        use rumqttc::{AsyncClient, ConnAck, ConnectReturnCode, Event, MqttOptions, Packet};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let (client_end, mut broker_end) = tokio::io::duplex(1 << 16);
        let client_end = Arc::new(std::sync::Mutex::new(Some(client_end)));
        let mut opts = MqttOptions::new("test-service", ("127.0.0.1", 1));
        opts.set_socket_connector(move |_host, _network| {
            let stream = client_end.lock().unwrap().take();
            async move {
                stream.ok_or_else(|| std::io::Error::other("the in-process broker accepts once"))
            }
        });
        let (client, mut eventloop) = AsyncClient::builder(opts).capacity(64).build();
        let (subscribes_tx, subscribes_rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(async move {
            let mut buf = BytesMut::new();
            loop {
                match Packet::read(&mut buf, None) {
                    Ok(Packet::Connect(..)) => {
                        let connack = ConnAck {
                            session_present: false,
                            code: ConnectReturnCode::Success,
                            properties: None,
                        };
                        let mut out = BytesMut::new();
                        connack.write(&mut out).unwrap();
                        broker_end.write_all(&out).await.unwrap();
                    }
                    Ok(Packet::Subscribe(subscribe)) => {
                        if subscribes_tx.send(subscribe).is_err() {
                            return;
                        }
                    }
                    Ok(_) => {}
                    Err(rumqttc::mqttbytes::Error::InsufficientBytes(_)) => {
                        if broker_end.read_buf(&mut buf).await.unwrap() == 0 {
                            return;
                        }
                    }
                    Err(e) => panic!("the in-process broker could not read a packet: {e:?}"),
                }
            }
        });
        loop {
            if let Event::Incoming(Packet::ConnAck(_)) = eventloop.poll().await.unwrap() {
                break;
            }
        }
        tokio::spawn(async move { while eventloop.poll().await.is_ok() {} });
        (client, subscribes_rx)
    }

    /// The SUBSCRIBE the supervisor puts on the wire requests the `qos` of the
    /// handle's configuration, and carries the filter's identifier when the
    /// connection's broker supports Subscription Identifiers and none when it
    /// does not.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_sent_subscribe_carries_the_identifier_only_where_the_broker_supports_it() {
        let wire = [
            (0, rumqttc::QoS::AtMostOnce),
            (1, rumqttc::QoS::AtLeastOnce),
        ];
        for available in [true, false] {
            for (qos, on_wire) in wire {
                let mut config = brenn_lib::mqtt::test_support::test_client_config("home");
                config.identity.qos = qos;
                let (stop_tx, _rx) = tokio::sync::watch::channel(false);
                let handle = MqttClientHandle::new(Arc::new(config), vec![], stop_tx);
                let (client, mut subscribes) = subscribe_capturing_client().await;

                assert_eq!(
                    try_assert_ingress_subscription(&handle, &client, &sub("sensors/#"), available),
                    Ok(SubscribeSend::Sent),
                );
                let sent = subscribes
                    .recv()
                    .await
                    .expect("the broker read the SUBSCRIBE");
                assert_eq!(sent.filters.len(), 1);
                assert_eq!(sent.filters[0].path, "sensors/#");
                assert_eq!(
                    sent.filters[0].qos, on_wire,
                    "a client configured at qos {qos} subscribes at it",
                );
                assert_eq!(
                    sent.properties.and_then(|p| p.id),
                    available.then_some(subscription_identifier("sensors/#") as usize),
                    "identifiers available: {available}",
                );
            }
        }
    }

    // --- Events of a lost connection ---

    /// Hand `event` to the dispatcher as an event of a lost connection.
    async fn lost(event: Event, handle: &Arc<MqttClientHandle>, router: &Arc<dyn MqttEventRouter>) {
        handle_event(event, handle, router, ConnectionPhase::Lost).await;
    }

    /// Of a lost connection's events, an inbound PUBLISH is routed as on a live
    /// one; an `Outgoing::Subscribe` takes its filter off the FIFO and binds
    /// nothing, so the FIFO's next entry binds the next SUBSCRIBE written; a
    /// SUBACK records nothing.
    #[tokio::test]
    async fn a_lost_connections_events_route_publishes_and_bind_no_subscribe() {
        let (handle, _eventloop) = routing_handle(true).await;
        let recording = Arc::new(RecordingRouter::default());
        let router: Arc<dyn MqttEventRouter> = recording.clone();

        let properties = rumqttc::PublishProperties {
            subscription_identifiers: vec![id("a/b")],
            ..Default::default()
        };
        let mut publish = rumqttc::Publish::new("a/b", QoS::AtLeastOnce, "v", Some(properties));
        publish.pkid = 1;
        lost(
            Event::Incoming(Incoming::Publish(publish)),
            &handle,
            &router,
        )
        .await;
        assert_eq!(recording.take(), by_subscription(&["a/b"]));

        for filter in ["a/#", "a/b"] {
            handle
                .pending_subscribes
                .lock()
                .unwrap()
                .push_back(pending(filter));
        }
        lost(Event::Outgoing(Outgoing::Subscribe(10)), &handle, &router).await;
        assert!(handle.inflight_subscribes.lock().await.is_empty());
        assert_eq!(
            handle
                .pending_subscribes
                .lock()
                .unwrap()
                .iter()
                .map(|p| p.topic_filter.clone())
                .collect::<Vec<_>>(),
            vec!["a/b".to_string()],
        );

        lost(
            Event::Incoming(Incoming::SubAck(rumqttc::SubAck {
                pkid: 10,
                return_codes: vec![SubscribeReasonCode::Success(QoS::AtLeastOnce)],
                properties: None,
            })),
            &handle,
            &router,
        )
        .await;
        assert_eq!(handle.subscribe_outcome("a/#").await, None);

        handle_event(
            Event::Outgoing(Outgoing::Subscribe(11)),
            &handle,
            &router,
            ConnectionPhase::Live,
        )
        .await;
        assert_eq!(
            handle.inflight_subscribes.lock().await.get(&11).cloned(),
            Some("a/b".to_string()),
            "the replayed SUBSCRIBE binds the filter behind the lost one",
        );
    }

    /// With a session, replayed SUBSCRIBEs bind their original filters in
    /// order; with none, the FIFO is emptied.
    #[tokio::test]
    async fn a_connack_keeps_the_subscribe_fifo_only_with_a_session() {
        for session_present in [true, false] {
            let handle = test_handle();
            let router: Arc<dyn MqttEventRouter> = Arc::new(NullRouter);
            for filter in ["a/#", "b/+", "c/d"] {
                handle.add_subscription(filter.into()).await;
            }
            handle
                .inflight_subscribes
                .lock()
                .await
                .insert(7, "c/d".to_string());
            handle.record_grant("c/d").await;
            for filter in ["a/#", "b/+"] {
                handle
                    .pending_subscribes
                    .lock()
                    .unwrap()
                    .push_back(pending(filter));
            }

            subscribe_bookkeeping_at_connack(&handle, session_present).await;

            assert!(handle.inflight_subscribes.lock().await.is_empty());
            assert_eq!(handle.subscribe_outcome("c/d").await, None);
            let fifo: Vec<String> = handle
                .pending_subscribes
                .lock()
                .unwrap()
                .iter()
                .map(|p| p.topic_filter.clone())
                .collect();
            if !session_present {
                assert!(fifo.is_empty(), "no session: nothing will be replayed");
                continue;
            }
            assert_eq!(fifo, ["a/#", "b/+"]);

            for pkid in [20, 21] {
                handle_event(
                    Event::Outgoing(Outgoing::Subscribe(pkid)),
                    &handle,
                    &router,
                    ConnectionPhase::Live,
                )
                .await;
            }
            suback(&handle, &router, 20, vec![SubscribeReasonCode::Failure]).await;
            suback(
                &handle,
                &router,
                21,
                vec![SubscribeReasonCode::Success(QoS::AtLeastOnce)],
            )
            .await;
            assert_eq!(
                handle.subscribe_outcome("a/#").await,
                Some(SubAckOutcome::Refused("Failure".to_string())),
            );
            assert!(handle.is_granted("b/+").await);
            assert_eq!(handle.subscribe_outcome("c/d").await, None);
        }
    }

    /// An inbound PUBLISH's RETAIN flag reaches the router as received, on a
    /// live connection's events and on a lost one's.
    #[tokio::test]
    async fn the_retain_flag_reaches_the_router_on_live_and_lost_events() {
        let (handle, _eventloop) = routing_handle(false).await;
        let recording = Arc::new(RecordingRouter::default());
        let router: Arc<dyn MqttEventRouter> = recording.clone();
        let publish = |retain: bool| {
            let mut publish = rumqttc::Publish::new("a/b", QoS::AtLeastOnce, "v", None);
            publish.pkid = 1;
            publish.retain = retain;
            Event::Incoming(Incoming::Publish(publish))
        };

        for phase in [ConnectionPhase::Live, ConnectionPhase::Lost] {
            handle_event(publish(true), &handle, &router, phase).await;
            handle_event(publish(false), &handle, &router, phase).await;
            assert_eq!(recording.take_retained(), [true, false], "{phase:?}");
            assert_eq!(recording.take().len(), 2, "{phase:?}");
        }
    }

    /// An inbound PUBLISH at QoS 2 is routed with its delivery QoS; the first
    /// on a connection sets the warning latch, and a new connection re-arms it.
    #[tokio::test]
    async fn a_qos_2_publish_is_routed_and_warned_once_per_connection() {
        let (handle, _eventloop) = routing_handle(false).await;
        let recording = Arc::new(RecordingRouter::default());
        let router: Arc<dyn MqttEventRouter> = recording.clone();
        let qos2 = || {
            let mut publish = rumqttc::Publish::new("a/b", QoS::ExactlyOnce, "v", None);
            publish.pkid = 1;
            Event::Incoming(Incoming::Publish(publish))
        };

        assert!(!handle.qos2_inbound_warned.load(Ordering::Relaxed));
        handle_event(qos2(), &handle, &router, ConnectionPhase::Live).await;
        assert!(handle.qos2_inbound_warned.load(Ordering::Relaxed));
        handle_event(qos2(), &handle, &router, ConnectionPhase::Live).await;
        assert_eq!(recording.take_qos(), [2, 2]);
        assert_eq!(recording.take().len(), 2);

        let (client, _eventloop2) = unpolled_client(4);
        install_connection(&handle, client, None, None, false).await;
        assert!(!handle.qos2_inbound_warned.load(Ordering::Relaxed));
    }

    // --- The sweep at a loss and the drop of the loop ---

    /// Whether `notice` has resolved, and how, without waiting for it.
    async fn resolved(
        notice: rumqttc::PublishNotice,
    ) -> Option<Result<rumqttc::PublishResult, rumqttc::PublishNoticeError>> {
        tokio::time::timeout(Duration::from_millis(50), notice.wait_async())
            .await
            .ok()
    }

    /// A publish submitted after the library's own sweep at a loss is moved
    /// into the pending list by the supervisor's, so a CONNACK reporting no
    /// session fails it with the rest. Without that sweep it stays on the
    /// request channel and its notice stays pending, which is the library
    /// behaviour the sweep exists for.
    #[tokio::test]
    async fn the_sweep_at_a_loss_puts_a_late_publish_with_the_session() {
        for swept in [true, false] {
            let (client, mut eventloop) = unpolled_client(8);
            let a = client
                .try_publish_tracked("t/a", QoS::AtLeastOnce, false, "a")
                .expect("room on the channel");
            eventloop.clean();
            let b = client
                .try_publish_tracked("t/b", QoS::AtLeastOnce, false, "b")
                .expect("room on the channel");
            if swept {
                sweep_after_loss(&mut eventloop);
            }
            eventloop.reset_session_state();

            assert_eq!(
                resolved(a).await,
                Some(Err(rumqttc::PublishNoticeError::SessionReset)),
            );
            let b = resolved(b).await;
            if swept {
                assert_eq!(b, Some(Err(rumqttc::PublishNoticeError::SessionReset)));
            } else {
                assert_eq!(b, None, "unswept, the late publish outlives the reset");
            }
        }
    }

    /// The drop-time count is of incoming PUBLISH events only, whatever else
    /// the queue holds.
    #[test]
    fn the_drop_count_is_of_incoming_publishes() {
        let publish = |qos| {
            let mut publish = rumqttc::Publish::new("a/b", qos, "v", None);
            publish.pkid = u16::from(qos != QoS::AtMostOnce);
            Event::Incoming(Incoming::Publish(publish))
        };
        let others = [
            Event::Outgoing(Outgoing::Publish(3)),
            Event::Outgoing(Outgoing::PubAck(4)),
            Event::Incoming(Incoming::SubAck(rumqttc::SubAck {
                pkid: 5,
                return_codes: vec![SubscribeReasonCode::Success(QoS::AtLeastOnce)],
                properties: None,
            })),
        ];

        assert_eq!(queued_incoming_publishes(&Default::default()), 0);
        assert_eq!(queued_incoming_publishes(&others.clone().into()), 0);
        let mut mixed: std::collections::VecDeque<Event> = others.into();
        mixed.insert(1, publish(QoS::AtMostOnce));
        mixed.push_back(publish(QoS::AtLeastOnce));
        assert_eq!(queued_incoming_publishes(&mixed), 2);
    }

    /// A new body clears both FIFOs a previous body left: nothing of that
    /// body's event loop is replayed on the new one. The asserted filters are
    /// kept, so the new body's first connection that finds the session
    /// withdraws what the old one could not.
    #[tokio::test]
    async fn a_new_body_starts_with_no_subscribe_bookkeeping() {
        let handle = test_handle();
        handle.add_subscription("a/x".to_string()).await;
        handle.remove_subscription("a/x").await;
        handle
            .pending_subscribes
            .lock()
            .unwrap()
            .push_back(pending("a/b"));
        handle
            .pending_unsubscribes
            .lock()
            .unwrap()
            .push_back(pending("a/x"));
        handle
            .inflight_subscribes
            .lock()
            .await
            .insert(7, "a/c".to_string());
        let stop_rx = handle.stop_tx.subscribe();
        handle.stop_tx.send(true).expect("stop_rx is alive");

        tokio::time::timeout(
            Duration::from_secs(2),
            supervisor_body(handle.clone(), Arc::new(NullRouter), stop_rx),
        )
        .await
        .expect("the supervisor did not stop");

        assert!(handle.pending_subscribes.lock().unwrap().is_empty());
        assert!(handle.pending_unsubscribes.lock().unwrap().is_empty());
        assert!(handle.inflight_subscribes.lock().await.is_empty());
        assert_eq!(handle.subscriptions.read().await.withdrawn(), ["a/x"]);
    }

    // --- Settling filters ---

    /// The filters of `queue`, front first.
    fn fifo(queue: &std::sync::Mutex<std::collections::VecDeque<PendingRequest>>) -> Vec<String> {
        queue
            .lock()
            .unwrap()
            .iter()
            .map(|p| p.topic_filter.clone())
            .collect()
    }

    /// A handle registered with `filters`, none of them connected yet.
    fn handle_with(filters: &[&str]) -> Arc<MqttClientHandle> {
        let (stop_tx, _rx) = tokio::sync::watch::channel(false);
        let config = Arc::new(brenn_lib::mqtt::test_support::test_client_config("broker"));
        MqttClientHandle::new(config, filters.iter().map(|f| sub(f)).collect(), stop_tx)
    }

    /// Serve `unsettled` and the queued filter requests once, as the connected
    /// phase does, and return what that submitted: the SUBSCRIBEs' filters and
    /// the UNSUBSCRIBEs'.
    async fn serve_from(
        handle: &Arc<MqttClientHandle>,
        unsettled: std::collections::VecDeque<String>,
    ) -> (Vec<String>, Vec<String>) {
        let subs_before = fifo(&handle.pending_subscribes).len();
        let unsubs_before = fifo(&handle.pending_unsubscribes).len();
        let (client, _eventloop) = unpolled_client(64);
        FilterSettler::new(handle, &client, unsettled, true)
            .serve()
            .await;
        (
            fifo(&handle.pending_subscribes)[subs_before..].to_vec(),
            fifo(&handle.pending_unsubscribes)[unsubs_before..].to_vec(),
        )
    }

    /// What a CONNACK makes the supervisor submit: the bookkeeping it leaves,
    /// the connection installed, then one serve.
    async fn connack_submits(
        handle: &Arc<MqttClientHandle>,
        session_present: bool,
    ) -> (Vec<String>, Vec<String>) {
        subscribe_bookkeeping_at_connack(handle, session_present).await;
        handle.connection_epoch.fetch_add(1, Ordering::Relaxed);
        let owed = owed_at_connack(handle).await;
        serve_from(handle, owed).await
    }

    /// A filter added after the CONNACK's bookkeeping, by a caller that reads
    /// the client before the install makes it visible and so names nothing to
    /// the supervisor, is owed by that connection.
    #[tokio::test]
    async fn a_filter_added_while_the_connection_is_installed_is_owed() {
        for session_present in [false, true] {
            let handle = handle_with(&["held/1"]);
            handle.request_settle("stale/1".to_string());
            let caller_view = handle.client.lock().await;
            let (client, _eventloop) = unpolled_client(8);
            let facts = ConnackFacts {
                session_present,
                max_packet_size: None,
                receive_max: None,
                sub_ids_available: true,
            };
            let connecting = tokio::spawn({
                let handle = handle.clone();
                async move { take_connection(&handle, client, facts).await }
            });
            // The bookkeeping clears the queued request, and the install then
            // waits on the client guard held here.
            while !handle.filter_requests.lock().unwrap().is_empty() {
                tokio::task::yield_now().await;
            }
            handle.add_subscription("late/1".into()).await;
            assert!(caller_view.is_none(), "the caller finds no client");
            drop(caller_view);

            let owed = connecting.await.unwrap();
            assert!(
                owed.contains(&"late/1".to_string()),
                "session present: {session_present}; owed: {owed:?}",
            );
            assert!(owed.contains(&"held/1".to_string()));
            assert!(handle.client.lock().await.is_some());
        }
    }

    async fn live(event: Event, handle: &Arc<MqttClientHandle>) {
        let router: Arc<dyn MqttEventRouter> = Arc::new(NullRouter);
        handle_event(event, handle, &router, ConnectionPhase::Live).await;
    }

    fn unsuback(pkid: u16, reason: rumqttc::UnsubAckReason) -> Event {
        Event::Incoming(Incoming::UnsubAck(rumqttc::UnsubAck {
            pkid,
            reasons: vec![reason],
            properties: None,
        }))
    }

    async fn asserted(handle: &MqttClientHandle, filter: &str) -> bool {
        handle.subscriptions.read().await.is_asserted(filter)
    }

    /// A filter withdrawn while disconnected — here one the handle was
    /// registered with, before its first connection — is withdrawn at a
    /// CONNACK that finds the session, and at one that finds none nothing is
    /// withdrawn and the asserted filters become the held ones.
    #[tokio::test]
    async fn a_withdrawal_while_disconnected_waits_for_a_connack_with_the_session() {
        for session_present in [true, false] {
            let handle = handle_with(&["a/1", "a/2"]);
            handle.remove_subscription("a/2").await;

            let (subs, unsubs) = connack_submits(&handle, session_present).await;

            assert_eq!(subs, ["a/1"], "session present: {session_present}");
            if session_present {
                assert_eq!(unsubs, ["a/2"]);
                assert!(asserted(&handle, "a/2").await, "until the UNSUBACK");
            } else {
                assert!(unsubs.is_empty());
                assert!(!asserted(&handle, "a/2").await);
                assert!(handle.subscriptions.read().await.withdrawn().is_empty());
            }
        }
    }

    /// A successor inherits the asserted filters with the held ones, so a
    /// filter its never-connected predecessor was registered with and withdrew
    /// is withdrawn at the successor's first CONNACK that finds the session.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_successor_withdraws_what_its_never_connected_predecessor_withdrew() {
        let service = MqttService::new();
        let old = handle_with(&["s/1", "s/2"]);
        old.remove_subscription("s/2").await;
        old.set_supervisor(stand_in_supervisor(&old)).await;
        service.add_client(old.clone());

        let router: Arc<dyn MqttEventRouter> = Arc::new(NullRouter);
        let successor = register_and_spawn(
            &service,
            old.config.clone(),
            ArrivingFilters::Successor(&old),
            router,
        )
        .await;
        successor.stop_and_join().await;
        assert_eq!(successor.subscriptions.read().await.withdrawn(), ["s/2"]);

        let (subs, unsubs) = connack_submits(&successor, true).await;
        assert_eq!(subs, ["s/1"]);
        assert_eq!(unsubs, ["s/2"]);
    }

    /// A filter added and withdrawn between two connections was never
    /// subscribed, and is still withdrawn at the next CONNACK that finds the
    /// session; the broker's No subscription existed takes it out.
    #[tokio::test]
    async fn a_filter_never_subscribed_is_withdrawn_and_taken_out_by_its_unsuback() {
        let handle = handle_with(&[]);
        connack_submits(&handle, false).await;
        handle.add_subscription("b/1".to_string()).await;
        handle.remove_subscription("b/1").await;

        let (subs, unsubs) = connack_submits(&handle, true).await;
        assert!(subs.is_empty());
        assert_eq!(unsubs, ["b/1"]);

        live(Event::Outgoing(Outgoing::Unsubscribe(5)), &handle).await;
        assert!(handle.pending_unsubscribes.lock().unwrap().is_empty());
        live(
            unsuback(5, rumqttc::UnsubAckReason::NoSubscriptionExisted),
            &handle,
        )
        .await;
        assert!(!asserted(&handle, "b/1").await);
        assert!(handle.inflight_unsubscribes.lock().await.is_empty());
    }

    /// An UNSUBACK that arrives after the filter is held again — before the
    /// supervisor has taken the request that names it — leaves the filter
    /// asserted, and that request then subscribes it.
    #[tokio::test]
    async fn an_unsuback_for_a_filter_held_again_leaves_it_asserted() {
        let handle = handle_with(&["c/1"]);
        handle.remove_subscription("c/1").await;
        let (_, unsubs) = connack_submits(&handle, true).await;
        assert_eq!(unsubs, ["c/1"]);
        live(Event::Outgoing(Outgoing::Unsubscribe(5)), &handle).await;

        handle.add_subscription("c/1".to_string()).await;
        handle.request_settle("c/1".to_string());
        live(unsuback(5, rumqttc::UnsubAckReason::Success), &handle).await;
        assert!(asserted(&handle, "c/1").await);

        let (subs, unsubs) = serve_from(&handle, Default::default()).await;
        assert_eq!(subs, ["c/1"]);
        assert!(unsubs.is_empty());
    }

    /// A filter withdrawn and held again while disconnected gets a SUBSCRIBE
    /// and no UNSUBSCRIBE at a CONNACK that finds the session.
    #[tokio::test]
    async fn a_filter_withdrawn_and_held_again_is_only_subscribed() {
        let handle = handle_with(&["d/1"]);
        handle.remove_subscription("d/1").await;
        handle.add_subscription("d/1".to_string()).await;

        let (subs, unsubs) = connack_submits(&handle, true).await;
        assert_eq!(subs, ["d/1"]);
        assert!(unsubs.is_empty());
    }

    /// A subscribe and an unsubscribe of one filter racing: in each order, and
    /// whether the supervisor serves between them or after both, the last
    /// request it submits agrees with the held set.
    #[tokio::test]
    async fn a_racing_subscribe_and_unsubscribe_end_on_the_held_set() {
        for subscribe_first in [true, false] {
            for serve_between in [true, false] {
                let handle = handle_with(&[]);
                connack_submits(&handle, false).await;
                if !subscribe_first {
                    handle.add_subscription("r/1".to_string()).await;
                    serve_from(&handle, ["r/1".to_string()].into()).await;
                }
                let change = async |subscribe: bool| {
                    if subscribe {
                        handle.add_subscription("r/1".to_string()).await;
                    } else {
                        handle.remove_subscription("r/1").await;
                    }
                    handle.request_settle("r/1".to_string());
                };
                change(subscribe_first).await;
                if serve_between {
                    serve_from(&handle, Default::default()).await;
                }
                change(!subscribe_first).await;
                let (subs, unsubs) = serve_from(&handle, Default::default()).await;

                let held = handle.subscriptions.read().await.get("r/1").is_some();
                assert_eq!(held, !subscribe_first);
                let case =
                    format!("subscribe first: {subscribe_first}, serve between: {serve_between}");
                if held {
                    assert!(!subs.is_empty() && unsubs.is_empty(), "{case}");
                } else {
                    assert!(subs.is_empty() && !unsubs.is_empty(), "{case}");
                }
            }
        }
    }

    /// Success and No subscription existed take the filter out of the
    /// asserted set; a refusal keeps it, and so does a success for an
    /// UNSUBSCRIBE that a later SUBSCRIBE of the filter has overtaken.
    #[tokio::test]
    async fn an_unsuback_takes_the_filter_out_only_when_it_withdrew_it() {
        use rumqttc::UnsubAckReason as R;
        for (reason, taken_out) in [
            (R::Success, true),
            (R::NoSubscriptionExisted, true),
            (R::NotAuthorized, false),
            (R::UnspecifiedError, false),
        ] {
            let handle = handle_with(&["e/1"]);
            handle.remove_subscription("e/1").await;
            connack_submits(&handle, true).await;
            live(Event::Outgoing(Outgoing::Unsubscribe(5)), &handle).await;
            live(unsuback(5, reason), &handle).await;
            assert_eq!(!asserted(&handle, "e/1").await, taken_out, "{reason:?}");
        }

        let handle = handle_with(&["e/1"]);
        handle.remove_subscription("e/1").await;
        connack_submits(&handle, true).await;
        live(Event::Outgoing(Outgoing::Unsubscribe(5)), &handle).await;
        handle.add_subscription("e/1".to_string()).await;
        let (subs, _) = serve_from(&handle, ["e/1".to_string()].into()).await;
        assert_eq!(subs, ["e/1"]);
        handle.remove_subscription("e/1").await;
        live(unsuback(5, R::Success), &handle).await;
        assert!(
            asserted(&handle, "e/1").await,
            "the SUBSCRIBE submitted after the UNSUBSCRIBE may stand at the broker",
        );
    }

    /// A SUBSCRIBE the library replays from an earlier connection for a filter
    /// no longer held is followed by an UNSUBSCRIBE, whether it is seen written
    /// before or after the CONNACK's settling; and a replayed UNSUBSCRIBE of a
    /// filter held again is followed by a SUBSCRIBE.
    #[tokio::test]
    async fn a_replayed_request_settles_its_filter_again() {
        for replay_first in [true, false] {
            let handle = handle_with(&["g/1"]);
            handle
                .pending_subscribes
                .lock()
                .unwrap()
                .push_back(pending("g/1"));
            handle.remove_subscription("g/1").await;

            subscribe_bookkeeping_at_connack(&handle, true).await;
            handle.connection_epoch.fetch_add(1, Ordering::Relaxed);
            let owed = owed_at_connack(&handle).await;
            let unsubs = if replay_first {
                live(Event::Outgoing(Outgoing::Subscribe(3)), &handle).await;
                serve_from(&handle, owed).await.1
            } else {
                let mut unsubs = serve_from(&handle, owed).await.1;
                assert_eq!(unsubs, ["g/1"]);
                live(Event::Outgoing(Outgoing::Subscribe(3)), &handle).await;
                unsubs.extend(serve_from(&handle, Default::default()).await.1);
                unsubs
            };
            assert_eq!(unsubs, ["g/1", "g/1"], "replay first: {replay_first}");
            assert!(asserted(&handle, "g/1").await);
        }

        let handle = handle_with(&["h/1"]);
        handle
            .pending_unsubscribes
            .lock()
            .unwrap()
            .push_back(pending("h/1"));
        let (subs, _) = connack_submits(&handle, true).await;
        assert_eq!(subs, ["h/1"]);
        live(Event::Outgoing(Outgoing::Unsubscribe(4)), &handle).await;
        let (subs, unsubs) = serve_from(&handle, Default::default()).await;
        assert_eq!(subs, ["h/1"]);
        assert!(unsubs.is_empty());
    }

    /// An UNSUBSCRIBE written on a lost connection takes its filter off the
    /// FIFO and binds nothing, and an UNSUBACK of a lost connection resolves
    /// nothing.
    #[tokio::test]
    async fn unsubscribe_events_of_a_lost_connection_record_nothing() {
        let handle = handle_with(&["x/1"]);
        handle.remove_subscription("x/1").await;
        let router: Arc<dyn MqttEventRouter> = Arc::new(NullRouter);
        handle
            .pending_unsubscribes
            .lock()
            .unwrap()
            .push_back(pending("x/1"));
        lost(Event::Outgoing(Outgoing::Unsubscribe(9)), &handle, &router).await;
        assert!(handle.pending_unsubscribes.lock().unwrap().is_empty());
        assert!(handle.inflight_unsubscribes.lock().await.is_empty());

        let seq = count_request(&handle, "x/1");
        handle
            .inflight_unsubscribes
            .lock()
            .await
            .insert(9, ("x/1".to_string(), seq));
        lost(
            unsuback(9, rumqttc::UnsubAckReason::Success),
            &handle,
            &router,
        )
        .await;
        assert!(asserted(&handle, "x/1").await);
        assert_eq!(handle.inflight_unsubscribes.lock().await.len(), 1);
    }

    // --- The connected phase's wait on a poll ---

    /// A poll that finishes when `finish` is sent and sets `dropped` when it is
    /// dropped, finished or not.
    fn watched_poll(
        dropped: &Arc<std::sync::atomic::AtomicBool>,
    ) -> (
        tokio::sync::oneshot::Sender<u32>,
        impl Future<Output = u32> + use<>,
    ) {
        struct SetOnDrop(Arc<std::sync::atomic::AtomicBool>);
        impl Drop for SetOnDrop {
            fn drop(&mut self) {
                self.0.store(true, std::sync::atomic::Ordering::SeqCst);
            }
        }
        let (finish, finished) = tokio::sync::oneshot::channel();
        let guard = SetOnDrop(dropped.clone());
        let poll = async move {
            let _guard = guard;
            finished.await.expect("the test finishes the poll")
        };
        (finish, poll)
    }

    /// A serve step that counts its runs.
    struct CountServes<'a>(&'a std::sync::atomic::AtomicUsize);

    impl ServeFilters for CountServes<'_> {
        async fn serve(&mut self) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    /// Poll `wait` once, with a waker that does nothing.
    fn poll_once<F: Future>(wait: std::pin::Pin<&mut F>) -> std::task::Poll<F::Output> {
        wait.poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
    }

    /// A filter wake runs the serve step again without dropping the
    /// poll, however often it comes, and the wait ends with the poll's own
    /// output.
    #[tokio::test]
    async fn a_filter_wake_serves_filters_and_keeps_the_poll() {
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        let dropped = Arc::new(AtomicBool::new(false));
        let (finish, poll) = watched_poll(&dropped);
        let (_stop_tx, mut stop_rx) = tokio::sync::watch::channel(false);
        let wake = tokio::sync::Notify::new();
        let served = AtomicUsize::new(0);
        let mut counter = CountServes(&served);
        let wait = await_connected_poll(poll, &mut stop_rx, &wake, &mut counter);
        let mut wait = std::pin::pin!(wait);

        assert!(poll_once(wait.as_mut()).is_pending());
        assert_eq!(
            served.load(Ordering::SeqCst),
            1,
            "served before the first poll"
        );
        for wakes in 1..=3 {
            wake.notify_one();
            assert!(poll_once(wait.as_mut()).is_pending());
            assert_eq!(
                served.load(Ordering::SeqCst),
                1 + wakes,
                "served at each wake"
            );
            assert!(
                !dropped.load(Ordering::SeqCst),
                "a wake dropped the unfinished poll"
            );
        }

        finish.send(7).unwrap();
        assert_eq!(
            poll_once(wait.as_mut()),
            std::task::Poll::Ready(ConnectedWait::Polled(7)),
        );
    }

    /// A stop ends the wait with the poll unfinished, ahead of a wake that is
    /// also ready.
    #[tokio::test]
    async fn a_stop_ends_the_wait_and_drops_the_unfinished_poll() {
        use std::sync::atomic::{AtomicBool, Ordering};
        let dropped = Arc::new(AtomicBool::new(false));
        let (_finish, poll) = watched_poll(&dropped);
        let (stop_tx, mut stop_rx) = tokio::sync::watch::channel(false);
        let wake = tokio::sync::Notify::new();
        {
            let unused = std::sync::atomic::AtomicUsize::new(0);
            let mut counter = CountServes(&unused);
            let wait = await_connected_poll(poll, &mut stop_rx, &wake, &mut counter);
            let mut wait = std::pin::pin!(wait);
            assert!(poll_once(wait.as_mut()).is_pending());
            wake.notify_one();
            stop_tx.send(true).unwrap();
            assert_eq!(
                poll_once(wait.as_mut()),
                std::task::Poll::Ready(ConnectedWait::Stop)
            );
        }
        assert!(dropped.load(Ordering::SeqCst));
    }

    // --- backoff_duration ---

    #[test]
    fn backoff_attempt_0_in_range() {
        let d = backoff_duration(0, 1, 60, 12345);
        assert!(d >= Duration::from_secs(1));
        assert!(d <= Duration::from_secs(2));
    }

    #[test]
    fn backoff_high_attempt_capped() {
        let d = backoff_duration(100, 1, 60, 99999);
        assert!(d <= Duration::from_secs(60));
        assert!(d >= Duration::from_secs(1));
    }

    #[test]
    fn backoff_degenerate_zero_max_does_not_panic() {
        let d = backoff_duration(0, 1, 0, 0);
        assert_eq!(d, Duration::from_secs(1));
    }

    // --- client_id_seed ---

    #[test]
    fn client_id_seed_pins_fnv1a_constants() {
        // The empty string yields the FNV-1a 64-bit offset basis unchanged; the
        // nonempty vector pins the prime multiply. If the constants ever change,
        // these fail.
        assert_eq!(client_id_seed(""), 0xcbf29ce484222325);
        assert_eq!(client_id_seed("brenn:ha"), FNV_BRENN_HA);
    }

    /// FNV-1a-64 of "brenn:ha" — precomputed oracle (see test above).
    const FNV_BRENN_HA: u64 = 0x77bc433d8c8fa2dd;

    // --- is_authoritative_failure ---

    fn tls_io(e: std::io::Error) -> ConnectionError {
        ConnectionError::Tls(rumqttc::TlsError::Io(e))
    }

    /// An I/O error of the TLS handshake that carries no `rustls::Error` is
    /// the connection ending, whatever its kind.
    #[test]
    fn authoritative_failure_tls_io_without_a_tls_verdict_is_transient() {
        use std::io::{Error, ErrorKind};
        for e in [
            Error::new(ErrorKind::UnexpectedEof, "tls handshake eof"),
            Error::from(ErrorKind::ConnectionReset),
            Error::from(ErrorKind::BrokenPipe),
            Error::other("certificate verify failed"),
        ] {
            let err = tls_io(e);
            assert!(!is_authoritative_failure(&err), "{err:?}");
        }
    }

    /// An I/O error of the TLS handshake that carries a `rustls::Error` is TLS
    /// refusing; a TLS error that is not I/O is the client's configuration.
    #[test]
    fn authoritative_failure_tls_verdicts_and_configuration_are_authoritative() {
        use std::io::{Error, ErrorKind};
        for e in [
            Error::new(
                ErrorKind::InvalidData,
                rustls::Error::InvalidCertificate(rustls::CertificateError::UnknownIssuer),
            ),
            Error::other(rustls::Error::General("no usable configuration".into())),
        ] {
            let err = tls_io(e);
            assert!(is_authoritative_failure(&err), "{err:?}");
        }
        assert!(is_authoritative_failure(&ConnectionError::Tls(
            rumqttc::TlsError::NoValidCertInChain
        )));
    }

    /// One poll of a fresh event loop dialling `port` on loopback with TLS
    /// trusting `roots`; returns the poll's error.
    async fn first_poll_error(port: u16, roots: rustls::RootCertStore) -> ConnectionError {
        let client_config = rustls::ClientConfig::builder_with_provider(
            rustls::crypto::aws_lc_rs::default_provider().into(),
        )
        .with_safe_default_protocol_versions()
        .expect("default protocol versions")
        .with_root_certificates(roots)
        .with_no_client_auth();
        let mut opts = MqttOptions::new("handshake-test", ("127.0.0.1", port));
        opts.set_transport(Transport::tls_with_config(TlsConfiguration::Rustls(
            Arc::new(client_config),
        )));
        let (_client, mut eventloop) = AsyncClient::builder(opts).capacity(8).build();
        eventloop
            .poll()
            .await
            .expect_err("the handshake cannot complete")
    }

    fn suite_ca_roots() -> rustls::RootCertStore {
        let (roots, added) = load_ca_cert_pem(
            "handshake-test",
            &crate::test_support::certs::ca_pem_bytes(),
        );
        assert_eq!(added, 1, "the suite's CA loads");
        roots
    }

    /// A peer that accepts the TCP connection and closes it before the TLS
    /// handshake ends the connect with a TLS I/O error that is transient.
    #[tokio::test]
    async fn a_peer_hanging_up_in_the_tls_handshake_is_transient() {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("bind");
        let port = listener.local_addr().expect("local_addr").port();
        let peer = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.expect("accept");
            drop(tcp);
        });
        let err = first_poll_error(port, suite_ca_roots()).await;
        peer.await.expect("the peer task");
        assert!(
            matches!(err, ConnectionError::Tls(rumqttc::TlsError::Io(_))),
            "{err:?}"
        );
        assert!(!is_authoritative_failure(&err), "{err:?}");
    }

    /// A server certificate the client does not trust ends the connect with a
    /// TLS I/O error carrying the `rustls::Error`, which is authoritative.
    /// This pins the classifier's downcast against the `rustls` the library
    /// stack actually wraps.
    #[tokio::test]
    async fn an_untrusted_server_certificate_is_authoritative() {
        let acceptor = tokio_rustls::TlsAcceptor::from(crate::test_support::certs::server_config());
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("bind");
        let port = listener.local_addr().expect("local_addr").port();
        let peer = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.expect("accept");
            acceptor
                .accept(tcp)
                .await
                .expect_err("the client rejects the certificate");
        });
        let err = first_poll_error(port, rustls::RootCertStore::empty()).await;
        peer.await.expect("the peer task");
        assert!(
            matches!(err, ConnectionError::Tls(rumqttc::TlsError::Io(_))),
            "{err:?}"
        );
        assert!(is_authoritative_failure(&err), "{err:?}");
    }

    fn authoritative_codes() -> Vec<ConnectReturnCode> {
        vec![
            ConnectReturnCode::BadUserNamePassword,
            ConnectReturnCode::NotAuthorized,
            ConnectReturnCode::BadAuthenticationMethod,
            ConnectReturnCode::Banned,
            ConnectReturnCode::ClientIdentifierNotValid,
            ConnectReturnCode::UnsupportedProtocolVersion,
            ConnectReturnCode::UseAnotherServer,
            ConnectReturnCode::ServerMoved,
            ConnectReturnCode::MalformedPacket,
            ConnectReturnCode::ProtocolError,
            ConnectReturnCode::RetainNotSupported,
            ConnectReturnCode::QoSNotSupported,
            ConnectReturnCode::TopicNameInvalid,
            ConnectReturnCode::PacketTooLarge,
            ConnectReturnCode::PayloadFormatInvalid,
            ConnectReturnCode::QuotaExceeded,
            ConnectReturnCode::RefusedProtocolVersion,
            ConnectReturnCode::BadClientId,
            ConnectReturnCode::ServiceUnavailable,
            ConnectReturnCode::Success,
        ]
    }

    fn transient_codes() -> Vec<ConnectReturnCode> {
        vec![
            ConnectReturnCode::ServerBusy,
            ConnectReturnCode::ServerUnavailable,
            ConnectReturnCode::UnspecifiedError,
            ConnectReturnCode::ConnectionRateExceeded,
            ConnectReturnCode::ImplementationSpecificError,
        ]
    }

    #[test]
    fn authoritative_failure_connection_refused_auth_codes() {
        for code in authoritative_codes() {
            assert!(is_authoritative_failure(
                &ConnectionError::ConnectionRefused(code)
            ));
        }
    }

    #[test]
    fn authoritative_failure_connection_refused_transient_codes() {
        for code in transient_codes() {
            assert!(!is_authoritative_failure(
                &ConnectionError::ConnectionRefused(code)
            ));
        }
    }

    #[test]
    fn authoritative_failure_connfail_auth_codes() {
        for code in authoritative_codes() {
            assert!(is_authoritative_failure(&ConnectionError::MqttState(
                StateError::ConnFail { reason: code }
            )));
        }
    }

    #[test]
    fn authoritative_failure_io_is_transient() {
        let io_err = ConnectionError::Io(std::io::Error::other("connection reset by peer"));
        assert!(!is_authoritative_failure(&io_err));
    }

    #[test]
    fn authoritative_failure_server_disconnect_not_authorized_is_authoritative() {
        let err = ConnectionError::MqttState(StateError::ServerDisconnect {
            reason_code: rumqttc::DisconnectReasonCode::NotAuthorized,
            reason_string: None,
        });
        assert!(is_authoritative_failure(&err));
    }

    #[test]
    fn authoritative_failure_server_disconnect_transient_reason_is_transient() {
        let err = ConnectionError::MqttState(StateError::ServerDisconnect {
            reason_code: rumqttc::DisconnectReasonCode::ServerBusy,
            reason_string: None,
        });
        assert!(!is_authoritative_failure(&err));
    }

    /// A connection that ends during the handshake — the CONNECT's write
    /// failing, or the peer closing before the CONNACK — is retried.
    #[test]
    fn authoritative_failure_handshake_failures_are_transient() {
        assert!(!is_authoritative_failure(&ConnectionError::MqttState(
            StateError::ConnectionAborted
        )));
        assert!(!is_authoritative_failure(&ConnectionError::MqttState(
            StateError::Deserialization(rumqttc::mqttbytes::Error::Io(std::io::Error::other(
                "broken pipe"
            )))
        )));
    }

    #[test]
    fn authoritative_failure_session_state_mismatch_is_authoritative() {
        let err = ConnectionError::SessionStateMismatch {
            clean_start: true,
            session_present: true,
        };
        assert!(is_authoritative_failure(&err));
    }

    #[test]
    fn authoritative_failure_broker_transport_mismatch_is_authoritative() {
        assert!(is_authoritative_failure(
            &ConnectionError::BrokerTransportMismatch
        ));
    }

    #[test]
    fn authoritative_failure_auth_processing_error_is_authoritative() {
        assert!(is_authoritative_failure(
            &ConnectionError::AuthProcessingError
        ));
    }

    // --- The Unsolicited cure ---

    fn unsolicited(pkid: u16) -> ConnectionError {
        ConnectionError::MqttState(StateError::Unsolicited(pkid))
    }

    /// The first `Unsolicited` arms the cure and discards the session; the
    /// connection the clean start made ending the same way gives up.
    #[test]
    fn unsolicited_discards_the_session_once_then_gives_up() {
        let mut cure = SessionCure::None;
        assert_eq!(
            classify_loss(&unsolicited(7), &mut cure),
            LossVerdict::DiscardSession { pkid: 7 }
        );
        assert_eq!(cure, SessionCure::Armed);

        let mut options = MqttOptions::new("c", ("127.0.0.1", 1));
        options.set_clean_start(false);
        cure.at_connack(&mut options);
        assert_eq!(cure, SessionCure::Applied);

        assert_eq!(
            classify_loss(&unsolicited(9), &mut cure),
            LossVerdict::GiveUp { pkid: 9 }
        );
    }

    /// Any other ending returns the cure to `None` and is classed as before,
    /// so a later `Unsolicited` is a first one again.
    #[test]
    fn any_other_ending_returns_the_cure_to_none() {
        let io = || ConnectionError::Io(std::io::Error::other("connection reset by peer"));
        let mut cure = SessionCure::Applied;
        assert_eq!(classify_loss(&io(), &mut cure), LossVerdict::Reconnect);
        assert_eq!(cure, SessionCure::None);

        let mut cure = SessionCure::Applied;
        assert_eq!(
            classify_loss(&ConnectionError::AuthProcessingError, &mut cure),
            LossVerdict::Authoritative
        );
        assert_eq!(cure, SessionCure::None);

        let mut cure = SessionCure::None;
        assert_eq!(classify_loss(&io(), &mut cure), LossVerdict::Reconnect);
        assert_eq!(
            classify_loss(&unsolicited(3), &mut cure),
            LossVerdict::DiscardSession { pkid: 3 }
        );
    }

    /// An armed cure sets clean start for every CONNECT until a CONNACK, which
    /// clears it and applies the cure; no other cure touches the flag.
    #[test]
    fn an_armed_cure_cleans_every_connect_until_the_connack() {
        let mut options = MqttOptions::new("c", ("127.0.0.1", 1));
        options.set_clean_start(false);

        for cure in [SessionCure::None, SessionCure::Applied] {
            let mut moved = cure;
            moved.before_connect(&mut options);
            assert!(!options.clean_start());
            moved.at_connack(&mut options);
            assert_eq!(moved, cure);
            assert!(!options.clean_start());
        }

        let mut cure = SessionCure::Armed;
        cure.before_connect(&mut options);
        assert!(options.clean_start());
        // A connect attempt that fails before a CONNACK leaves it armed.
        cure.before_connect(&mut options);
        assert!(options.clean_start());
        cure.at_connack(&mut options);
        assert!(!options.clean_start());
        assert_eq!(cure, SessionCure::Applied);
    }

    /// The library classes `Unsolicited` as a state error like any other; only
    /// the cure makes it anything but a reconnect.
    #[test]
    fn unsolicited_is_not_authoritative_on_its_own() {
        assert!(!is_authoritative_failure(&unsolicited(1)));
    }

    // ---------------------------------------------------------------------------
    // build_tls_transport
    // ---------------------------------------------------------------------------

    /// A self-signed CA certificate in PEM format for use in unit tests.
    const TEST_CA_PEM: &[u8] = b"-----BEGIN CERTIFICATE-----\n\
MIIDBTCCAe2gAwIBAgIUK3JCad5lqqsrUQtXXvH+nDjKctQwDQYJKoZIhvcNAQEL\n\
BQAwEjEQMA4GA1UEAwwHdGVzdC1jYTAeFw0yNjA2MTQxMzM1MjBaFw0zNjA2MTEx\n\
MzM1MjBaMBIxEDAOBgNVBAMMB3Rlc3QtY2EwggEiMA0GCSqGSIb3DQEBAQUAA4IB\n\
DwAwggEKAoIBAQDZxLnsX5rwyO8N1379I4yN13y+vMefTWH371qAQABdBnTaxveD\n\
SfJz4A/SBHbPOh/9w9Mr1kQW3CdCV0cl1n+R08vMoEtsjUwd8EeJE5Ev+dAzlFFo\n\
Uj7pTSAIBoUPHMLmaZFCaFVQf+MBBQISk5UZyzPqEOMDzS1KllKGoEjapOFEoWpr\n\
WwyZGKolbenT44vexAyS1jiaktSJIhArM/aBc7dQKQ+hxfsZZZOkg4Lj7oR4ik7C\n\
17HKhZFV7bDN+oEx9crJX6Q3GmH7dM7CuxIAaOlvJu7+20MlhisArnvetUm6W21+\n\
xETjGvzLiL8wOu5e2r5QMSZQZI9d4c44pBQTAgMBAAGjUzBRMB0GA1UdDgQWBBRQ\n\
2eqo6y9Zjo5tHmtPCakOVn3QjjAfBgNVHSMEGDAWgBRQ2eqo6y9Zjo5tHmtPCakO\n\
Vn3QjjAPBgNVHRMBAf8EBTADAQH/MA0GCSqGSIb3DQEBCwUAA4IBAQBYr9lksEme\n\
W6GL/nmRgTpK8Zvu0NeRLUu/SC/7it5VRx8ZMXjKW+THZkhL8fLMUu9Ip1njhYS1\n\
kK9Da6X9hw6OEdEbNBSdeF0ENujpAeIVjRgExlY+YIJr48W9LlibUD7PAfQDefty\n\
/wfVbrV1bOeVGB75Ybf66EEnbB0TWpVzTnKibD3hAdIYQGMGwfT9cekqcouqWK8A\n\
UT1RvGyB1Qij0sCqNhf5ZlwZSspW1jQaYjO5ENQtdbR3X3Wk49IrKP/e/tlOfbna\n\
iEzLITFM4JW7Kw0P9wqZ1rE+Ou30Px9OapRomHcTlVbyzPMmi79WBOdyvir2cnlv\n\
Lu2jWbIqZrx0\n\
-----END CERTIFICATE-----\n";

    fn make_test_broker(
        ca_cert_pem: Option<Vec<u8>>,
        tls_version_min: brenn_lib::mqtt::config::TlsVersionMin,
    ) -> brenn_lib::mqtt::config::MqttClientConfig {
        let mut config = brenn_lib::mqtt::test_support::test_client_config("test-broker");
        config.ca_cert_pem = ca_cert_pem;
        config.identity.tls_version_min = tls_version_min;
        config
    }

    #[test]
    fn build_tls_transport_tls13_custom_ca_yields_tls_transport() {
        let broker = make_test_broker(
            Some(TEST_CA_PEM.to_vec()),
            brenn_lib::mqtt::config::TlsVersionMin::Tls13,
        );
        let (store, added) = load_ca_cert_pem(&broker.identity.slug, TEST_CA_PEM);
        assert_eq!(added, 1);
        assert_eq!(store.len(), 1);
        let transport = build_tls_transport(&broker);
        assert!(matches!(transport, Transport::Tls(_)));
    }

    #[test]
    fn build_tls_transport_tls12_default_yields_tls_transport() {
        let broker = make_test_broker(None, brenn_lib::mqtt::config::TlsVersionMin::Tls12);
        let transport = build_tls_transport(&broker);
        assert!(matches!(transport, Transport::Tls(_)));
    }

    const MALFORMED_CERT_PEM: &[u8] = b"-----BEGIN CERTIFICATE-----\n\
!!!not-valid-base64!!!\n\
-----END CERTIFICATE-----\n";

    #[test]
    fn build_tls_transport_tls13_invalid_pem_returns_tls_transport() {
        let broker = make_test_broker(
            Some(MALFORMED_CERT_PEM.to_vec()),
            brenn_lib::mqtt::config::TlsVersionMin::Tls13,
        );
        let (store, added) = load_ca_cert_pem(&broker.identity.slug, MALFORMED_CERT_PEM);
        assert_eq!(added, 0);
        assert_eq!(store.len(), 0);
        let transport = build_tls_transport(&broker);
        assert!(matches!(transport, Transport::Tls(_)));
    }

    #[test]
    fn build_tls_transport_tls13_mixed_pem_loads_valid_cert() {
        let mut pem = MALFORMED_CERT_PEM.to_vec();
        pem.extend_from_slice(TEST_CA_PEM);
        let (store, added) = load_ca_cert_pem("test-broker", &pem);
        assert_eq!(added, 1);
        assert_eq!(store.len(), 1);
    }

    // --- registration and the spawn's readiness check ---

    /// A router that cannot deliver yet.
    struct UnreadyRouter;
    #[async_trait::async_trait]
    impl MqttEventRouter for UnreadyRouter {
        fn ready(&self) -> bool {
            false
        }

        async fn deliver_inbound(
            &self,
            client_slug: &str,
            topic: &str,
            _payload: crate::payload::InboundPayload,
            _qos: u8,
            _retained: bool,
            _routing: InboundRouting,
        ) {
            panic!("an unready router was handed {topic:?} on {client_slug}");
        }

        async fn session_fault(&self, client_slug: &str, fault: SessionFault) {
            panic!("an unready router was handed {fault:?} on {client_slug}");
        }
    }

    fn test_config() -> Arc<MqttClientConfig> {
        Arc::new(brenn_lib::mqtt::test_support::test_client_config("broker"))
    }

    /// Registration makes the slug resolve with its filters and spawns nothing.
    #[tokio::test]
    async fn register_client_registers_the_handle_without_a_supervisor() {
        let service = MqttService::new();
        let session = register_client(
            &service,
            test_config(),
            vec![IngressSubscription::new("home/state".to_string())],
        );
        let registered = service.get_client("broker").expect("the slug resolves");
        assert!(Arc::ptr_eq(&registered, session.handle()));
        assert!(!registered.supervisor_spawned().await);
        assert!(
            registered
                .subscriptions
                .read()
                .await
                .get("home/state")
                .is_some()
        );
    }

    /// `spawn` over a ready router records the supervisor it spawned.
    #[tokio::test]
    async fn spawn_over_a_ready_router_records_the_supervisor() {
        let service = MqttService::new();
        let handle = register_client(&service, test_config(), Vec::new())
            .spawn(Arc::new(NullRouter))
            .await;
        assert!(handle.supervisor_spawned().await);
        handle.stop_and_join().await;
    }

    /// `spawn` against a router that cannot deliver panics, and no supervisor
    /// is spawned or recorded.
    #[tokio::test]
    async fn spawn_against_an_unready_router_panics_and_spawns_nothing() {
        let service = MqttService::new();
        let session = register_client(&service, test_config(), Vec::new());
        let handle = session.handle().clone();
        let spawned = tokio::spawn(async move {
            session.spawn(Arc::new(UnreadyRouter)).await;
        })
        .await;
        let panic = spawned.expect_err("the spawn panics").into_panic();
        let message = panic
            .downcast_ref::<String>()
            .expect("the panic carries a formatted message");
        assert!(
            message.contains("spawned against a router that cannot deliver yet"),
            "{message}",
        );
        assert!(!handle.supervisor_spawned().await);
        assert!(
            handle.client.lock().await.is_none(),
            "nothing connected for the handle",
        );
    }

    /// `register_and_spawn` goes through the same check.
    #[tokio::test]
    async fn register_and_spawn_against_an_unready_router_panics_and_spawns_nothing() {
        let service = MqttService::new();
        let task_service = service.clone();
        let spawned = tokio::spawn(async move {
            register_and_spawn(
                &task_service,
                test_config(),
                ArrivingFilters::Declared(Vec::new()),
                Arc::new(UnreadyRouter),
            )
            .await;
        })
        .await;
        let panic = spawned.expect_err("the spawn panics").into_panic();
        let message = panic
            .downcast_ref::<String>()
            .expect("the panic carries a formatted message");
        assert!(
            message.contains("spawned against a router that cannot deliver yet"),
            "{message}",
        );
        let handle = service
            .get_client("broker")
            .expect("registered before the spawn");
        assert!(!handle.supervisor_spawned().await);
    }

    // --- sessions that end and start on purpose ---

    fn config_with_expiry(session_expiry_secs: u32) -> Arc<MqttClientConfig> {
        let mut config = brenn_lib::mqtt::test_support::test_client_config("broker");
        config.identity.session_expiry_secs = session_expiry_secs;
        Arc::new(config)
    }

    /// A handle with a stand-in supervisor that has already ended, so a stop
    /// joins at once.
    async fn handle_with_ended_supervisor(config: Arc<MqttClientConfig>) -> Arc<MqttClientHandle> {
        let (stop_tx, _stop_rx) = tokio::sync::watch::channel(false);
        let handle = MqttClientHandle::new(config, Vec::new(), stop_tx);
        handle.set_supervisor(tokio::spawn(async {})).await;
        handle
    }

    /// An `Added` arrival's CONNECTs are clean starts up to the first CONNACK,
    /// across failed attempts and across a body respawned before it; the
    /// CONNECT after that CONNACK asks for the kept session.
    #[test]
    fn a_required_clean_start_lasts_until_the_first_connack() {
        let (stop_tx, _stop_rx) = tokio::sync::watch::channel(false);
        let handle = MqttClientHandle::new(test_config(), Vec::new(), stop_tx);
        let mut cure = SessionCure::None;
        let mut options = MqttOptions::new("c", ("127.0.0.1", 1));
        options.set_clean_start(false);

        let reason = clean_start_before_connect(cure, &handle, &mut options);
        assert_eq!(reason, None, "a handle that requires nothing");
        assert!(!options.clean_start());

        handle.require_clean_start();
        let reason = clean_start_before_connect(cure, &handle, &mut options);
        assert_eq!(reason, Some(CleanStartReason::Added), "the first CONNECT");
        assert!(options.clean_start());
        // A connect attempt that fails before a CONNACK leaves it set.
        clean_start_before_connect(cure, &handle, &mut options);
        assert!(options.clean_start(), "a retried first CONNECT");

        // A respawned body builds its options afresh, with the kept session.
        let mut respawned = MqttOptions::new("c", ("127.0.0.1", 1));
        respawned.set_clean_start(false);
        clean_start_before_connect(cure, &handle, &mut respawned);
        assert!(
            respawned.clean_start(),
            "a body respawned before the CONNACK"
        );

        clean_start_at_connack(&mut cure, &handle, &mut respawned);
        assert!(!respawned.clean_start(), "cleared at the CONNACK");
        assert!(!handle.clean_start_pending());
        assert_eq!(cure, SessionCure::None, "no cure was armed");
        let reason = clean_start_before_connect(cure, &handle, &mut respawned);
        assert_eq!(reason, None, "the CONNECT after the first CONNACK");
        assert!(!respawned.clean_start());

        // A body respawned after the CONNACK resumes.
        let mut later = MqttOptions::new("c", ("127.0.0.1", 1));
        later.set_clean_start(false);
        clean_start_before_connect(cure, &handle, &mut later);
        assert!(!later.clean_start(), "a body respawned after the CONNACK");
    }

    /// The required clean start and the `Unsolicited` cure each clear only
    /// what they set.
    #[test]
    fn a_connack_without_a_required_clean_start_leaves_the_flag() {
        let (stop_tx, _stop_rx) = tokio::sync::watch::channel(false);
        let handle = MqttClientHandle::new(test_config(), Vec::new(), stop_tx);
        let mut options = MqttOptions::new("c", ("127.0.0.1", 1));
        options.set_clean_start(true);
        for start in [SessionCure::None, SessionCure::Applied] {
            let mut cure = start;
            clean_start_at_connack(&mut cure, &handle, &mut options);
            assert!(options.clean_start());
            assert_eq!(cure, start);
        }
    }

    /// The `connecting to broker` reason names the source that made the
    /// CONNECT clean: the cure alone gives `Cure`, and a handle requirement
    /// held at the same time gives `Added`. One CONNACK spends both.
    #[test]
    fn the_clean_start_reason_names_its_source() {
        let (stop_tx, _stop_rx) = tokio::sync::watch::channel(false);
        let handle = MqttClientHandle::new(test_config(), Vec::new(), stop_tx);
        let mut options = MqttOptions::new("c", ("127.0.0.1", 1));
        options.set_clean_start(false);

        let mut cure = SessionCure::Armed;
        let reason = clean_start_before_connect(cure, &handle, &mut options);
        assert_eq!(reason, Some(CleanStartReason::Cure));
        assert!(options.clean_start());

        handle.require_clean_start();
        let reason = clean_start_before_connect(cure, &handle, &mut options);
        assert_eq!(reason, Some(CleanStartReason::Added));

        clean_start_at_connack(&mut cure, &handle, &mut options);
        assert!(!options.clean_start());
        assert_eq!(cure, SessionCure::Applied);
        assert!(!handle.clean_start_pending());
        assert_eq!(
            clean_start_before_connect(cure, &handle, &mut options),
            None
        );
    }

    /// `register_and_spawn` marks an `Added` arrival for a clean start and a
    /// `Declared` one not; a successor inherits a clean start its predecessor
    /// still owed.
    #[tokio::test]
    async fn an_added_arrival_requires_a_clean_start_and_its_successor_inherits_it() {
        let service = MqttService::new();
        let declared = register_and_spawn(
            &service,
            test_config(),
            ArrivingFilters::Declared(Vec::new()),
            Arc::new(NullRouter),
        )
        .await;
        assert!(!declared.clean_start_pending());
        declared.stop_and_join().await;

        let added = register_and_spawn(
            &service,
            test_config(),
            ArrivingFilters::Added,
            Arc::new(NullRouter),
        )
        .await;
        assert!(
            added.clean_start_pending(),
            "no broker answers, so no CONNACK clears it"
        );
        assert!(added.subscriptions.read().await.is_empty());

        let successor = register_and_spawn(
            &service,
            test_config(),
            ArrivingFilters::Successor(&added),
            Arc::new(NullRouter),
        )
        .await;
        assert!(successor.clean_start_pending());
        successor.stop_and_join().await;
    }

    /// The DISCONNECT a stop builds: Session Expiry Interval 0 for an ended
    /// session of a client with expiry above 0, plain for an ended session of a
    /// client with expiry 0, plain for a stop that keeps the session.
    #[tokio::test]
    async fn the_disconnect_a_stop_builds() {
        let kept = handle_with_ended_supervisor(config_with_expiry(3600)).await;
        kept.stop_and_join().await;
        assert_eq!(StopDisconnect::of(&kept), StopDisconnect::KeepSession);
        assert_eq!(StopDisconnect::of(&kept).properties(), None);

        let ended = handle_with_ended_supervisor(config_with_expiry(3600)).await;
        ended.end_session_and_join().await;
        assert_eq!(StopDisconnect::of(&ended), StopDisconnect::EndSession);
        let properties = StopDisconnect::of(&ended)
            .properties()
            .expect("the session-ending DISCONNECT carries properties");
        assert_eq!(properties.session_expiry_interval, Some(0));
        assert_eq!(properties.reason_string, None);
        assert!(properties.user_properties.is_empty());
        assert_eq!(properties.server_reference, None);

        let expiry_zero = handle_with_ended_supervisor(config_with_expiry(0)).await;
        expiry_zero.end_session_and_join().await;
        assert_eq!(
            StopDisconnect::of(&expiry_zero),
            StopDisconnect::KeepSession
        );
        assert_eq!(StopDisconnect::of(&expiry_zero).properties(), None);
    }

    /// `end_session_and_join` stops the session as `stop_and_join` does.
    #[tokio::test]
    async fn end_session_and_join_stops_and_joins_the_supervisor() {
        let service = MqttService::new();
        let handle = register_and_spawn(
            &service,
            config_with_expiry(3600),
            ArrivingFilters::Declared(Vec::new()),
            Arc::new(NullRouter),
        )
        .await;
        handle.end_session_and_join().await;
        assert!(*handle.stop_tx.borrow());
        assert!(handle.client.lock().await.is_none());
        // A second stop finds the session joined.
        handle.end_session_and_join().await;
    }
}
