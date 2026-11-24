//! MQTT connection supervisor: one per active `[[mqtt_client]]` session.
//!
//! Each supervisor runs in a dedicated `tokio::spawn` task. It owns one
//! `rumqttc::AsyncClient` + `EventLoop` for its life and keeps polling that
//! loop across lost connections, so the library resumes the client's half of a
//! broker session that survived; it reconnects with jittered exponential
//! backoff, re-asserts subscriptions on every successful connect, records the
//! facts of each connection a publish needs, and routes every inbound PUBLISH
//! to the bridge router. One session per
//! client carries both the publish and the ingress-delivery paths.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

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
    DEFAULT_RECEIVE_MAXIMUM, DEFERRED_SUBSCRIBES_POISONED, IngressSubscription, MqttClientHandle,
    PENDING_SUBSCRIBES_POISONED, SupervisorState,
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
        // TODO(mqtt-retained-replay-repeated-on-resubscribe): every reconnect
        // and every repeated SUBSCRIBE replays the filter's retained messages.
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

/// What [`try_assert_ingress_subscription`] did with a SUBSCRIBE.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SubscribeSend {
    /// The SUBSCRIBE is on the session's request channel.
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
/// filter is pushed onto `handle.pending_subscribes` in the same synchronous
/// section as the submit, and no other task submits a SUBSCRIBE, so the FIFO's
/// order is the order the event loop takes SUBSCRIBE requests and the
/// `Outgoing::Subscribe` arm binds pkid → filter correctly.
///
/// A filter rumqttc would reject is refused up front, without touching the
/// FIFO: rumqttc reports it with the same error as a full channel, and a
/// retry loop must not mistake it for one.
///
/// The SUBSCRIBE requests the `qos` of `handle`'s configuration.
/// `with_identifier` is whether the current connection accepts a
/// Subscription Identifier; see [`subscribe_request`].
///
/// Returns `Err(message)` for a send that failed for any other reason; there is
/// no SUBACK wait.
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
    let mut pending = handle
        .pending_subscribes
        .lock()
        .expect(PENDING_SUBSCRIBES_POISONED);
    pending.push_back(sub.topic_filter.clone());
    match client.try_subscribe_many_with_properties(std::iter::once(filter), props) {
        Ok(()) => Ok(SubscribeSend::Sent),
        Err(rumqttc::ClientError::TryRequest(_)) => {
            pending.pop_back();
            Ok(SubscribeSend::ChannelFull)
        }
        Err(e) => {
            pending.pop_back();
            Err(e.to_string())
        }
    }
}

/// Issue a single broker UNSUBSCRIBE for one ingress topic filter on a live
/// `AsyncClient`. There is no UNSUBACK wait.
///
/// This waits for room on the request channel, so it must never be called from
/// the supervisor task, which is the only task that drains that channel.
pub async fn assert_ingress_unsubscribe(
    client: &AsyncClient,
    topic_filter: &str,
) -> Result<(), String> {
    client
        .unsubscribe(topic_filter.to_string())
        .await
        .map_err(|e| e.to_string())
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
    // TODO(mqtt-receive-maximum-unset): no Receive Maximum is set on CONNECT.
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
#[must_use]
pub fn spawn_client_supervisor(
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
    /// The set the caller derived: a boot's static union, or the empty list an
    /// arriving client is registered with so the incoming step can report every
    /// one of its filters as a move.
    Declared(Vec<IngressSubscription>),
    /// The predecessor this session replaces. Its filter set is inherited, and
    /// its supervisor is stopped and joined as part of the handover.
    Successor(&'a MqttClientHandle),
}

/// Bring one client session up on `service`: build the handle, register it,
/// hand the broker client id and the filter set over from a predecessor if
/// there is one, spawn the supervisor, and record its join handle. Returns the
/// registered handle.
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
pub async fn register_and_spawn(
    service: &MqttService,
    config: Arc<MqttClientConfig>,
    filters: ArrivingFilters<'_>,
    router: Arc<dyn MqttEventRouter>,
) -> Arc<MqttClientHandle> {
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let declared = match &filters {
        ArrivingFilters::Declared(subs) => subs.clone(),
        ArrivingFilters::Successor(_) => Vec::new(),
    };
    let handle = MqttClientHandle::new(config, declared, stop_tx);
    service.add_client(handle.clone());
    if let ArrivingFilters::Successor(old) = filters {
        // The successor's set is claimed before the predecessor is stopped and
        // held until the inherit has been written, so a filter edit on this
        // slug queues behind the inherit instead of being overwritten by it.
        let mut arriving = handle.subscriptions.write().await;
        old.stop_and_join().await;
        *arriving = old.subscriptions.read().await.clone();
        drop(arriving);
    }
    let join = spawn_client_supervisor(handle.clone(), router, stop_rx);
    handle.set_supervisor(join).await;
    handle
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

/// Await `poll`, one `EventLoop::poll`, to its end, calling `serve_subscribes`
/// before it is first polled and again at each wake on `subscribe_wake`. Only
/// a stop request ends the wait before `poll` finishes.
///
/// `poll` is never dropped for a wake: the library's `poll` is not
/// cancel-safe. When it takes a SUBSCRIBE off the client's control channel and
/// then yields to replay requests from its pending list, dropping it drops
/// that SUBSCRIBE unsent and unreported, and every later SUBSCRIBE binds to the
/// wrong filter of `pending_subscribes`. A write that waits on a full socket
/// buffer is lost the same way. At a stop the connection is ended and its
/// SUBSCRIBE bookkeeping cleared, so a request lost there misattributes
/// nothing that outlives the stop.
async fn await_connected_poll<F: Future>(
    poll: F,
    stop_rx: &mut tokio::sync::watch::Receiver<bool>,
    subscribe_wake: &tokio::sync::Notify,
    mut serve_subscribes: impl FnMut(),
) -> ConnectedWait<F::Output> {
    let mut poll = std::pin::pin!(poll);
    loop {
        serve_subscribes();
        tokio::select! {
            biased;
            _ = stop_requested(stop_rx) => return ConnectedWait::Stop,
            () = subscribe_wake.notified() => {}
            output = &mut poll => return ConnectedWait::Polled(output),
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
    // stop path's `try_disconnect_now` goes through the library's unbounded
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
                terminal_reason = Some(SupervisorState::Disconnected {
                    last_error: None,
                    next_attempt_at: Instant::now(),
                });
            }
            drop_event_loop(&handle, client, eventloop);
            break;
        }

        tracing::info!(client = %client_slug, attempt, "connecting to broker");
        cure.before_connect(&mut eventloop.options);

        {
            let mut state = handle.supervisor_state.write().await;
            *state = SupervisorState::Connecting {
                since: Instant::now(),
            };
        }

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
                    terminal_reason = Some(SupervisorState::Disconnected {
                        last_error: None,
                        next_attempt_at: Instant::now(),
                    });
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
                    let mut state = handle.supervisor_state.write().await;
                    *state = if authoritative {
                        SupervisorState::Failed {
                            reason: error_str.clone(),
                        }
                    } else {
                        SupervisorState::Disconnected {
                            last_error: Some(error_str.clone()),
                            next_attempt_at: Instant::now(),
                        }
                    };
                }
                if authoritative {
                    tracing::error!(
                        client = %client_slug,
                        broker = %broker.identity.slug,
                        error = %error_str,
                        "broker connection failed with authoritative error; stopping retry",
                    );
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
        cure.at_connack(&mut eventloop.options);

        subscribe_bookkeeping_at_connack(&handle, facts.session_present).await;
        // Deferred subscribes left from a previous connection are dropped: each
        // was added to `handle.subscriptions` before it was enqueued, so the
        // snapshot taken below covers it.
        handle
            .deferred_subscribes
            .lock()
            .expect(DEFERRED_SUBSCRIBES_POISONED)
            .clear();

        install_connection(
            &handle,
            client.clone(),
            facts.max_packet_size,
            facts.receive_max,
            facts.sub_ids_available,
        )
        .await;
        {
            let mut state = handle.supervisor_state.write().await;
            *state = SupervisorState::Connected;
        }
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

        // Re-assert all union subscriptions (static config union plus any
        // runtime-added dynamic `mqtt:` subscriptions) from inside the main
        // loop: as many SUBSCRIBEs as the request channel takes before each
        // poll and at each subscribe wake while it runs, the rest once a poll
        // frees room. Snapshot under a brief read-lock.
        let mut unasserted: std::collections::VecDeque<IngressSubscription> =
            handle.subscriptions.read().await.to_vec().into();
        let reassert_count = unasserted.len();
        let mut reasserted_logged = reassert_count == 0;
        let mut reassert_failed = 0usize;

        let mut serve_subscribes = || {
            unasserted.extend(
                handle
                    .deferred_subscribes
                    .lock()
                    .expect(DEFERRED_SUBSCRIBES_POISONED)
                    .drain(..),
            );
            // Push as many SUBSCRIBEs as the request channel will take now. A
            // full channel is non-empty, so the running poll takes a request
            // off it (or fails and ends the connection) and this runs again
            // with the next poll.
            while let Some(sub) = unasserted.front() {
                match try_assert_ingress_subscription(
                    &handle,
                    &client,
                    sub,
                    facts.sub_ids_available,
                ) {
                    Ok(SubscribeSend::Sent) => {
                        unasserted.pop_front();
                    }
                    Ok(SubscribeSend::ChannelFull) => break,
                    Err(e) => {
                        tracing::error!(
                            client = %client_slug,
                            broker = %broker.identity.slug,
                            topic = %sub.topic_filter,
                            error = %e,
                            "failed to send subscribe — filter not asserted; channel will \
                             receive nothing until the next reconnect",
                        );
                        unasserted.pop_front();
                        reassert_failed += 1;
                    }
                }
            }
            if !reasserted_logged && unasserted.is_empty() {
                if reassert_failed == 0 {
                    tracing::info!(
                        client = %client_slug,
                        broker = %broker.identity.slug,
                        filters = reassert_count,
                        "re-asserted all ingress filters",
                    );
                } else {
                    tracing::warn!(
                        client = %client_slug,
                        broker = %broker.identity.slug,
                        filters = reassert_count,
                        failed = reassert_failed,
                        "re-assert finished with ingress filters not asserted",
                    );
                }
                reasserted_logged = true;
            }
        };

        let lost = loop {
            let wait = await_connected_poll(
                eventloop.poll(),
                &mut stop_rx,
                &handle.subscribe_wake,
                &mut serve_subscribes,
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
                        let disconnected = SupervisorState::Disconnected {
                            last_error: None,
                            next_attempt_at: Instant::now(),
                        };
                        {
                            let mut state = handle.supervisor_state.write().await;
                            *state = disconnected.clone();
                        }
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
            let mut state = handle.supervisor_state.write().await;
            *state = if disconnect_authoritative {
                SupervisorState::Failed {
                    reason: disconnect_reason.clone(),
                }
            } else {
                SupervisorState::Disconnected {
                    last_error: Some(disconnect_reason.clone()),
                    next_attempt_at: Instant::now(),
                }
            };
        }
        clear_connection_tracking(&handle).await;

        match verdict {
            LossVerdict::Reconnect => {}
            LossVerdict::Authoritative => {
                tracing::error!(
                    client = %client_slug,
                    broker = %broker.identity.slug,
                    reason = %disconnect_reason,
                    "broker connection lost with authoritative error; stopping retry",
                );
            }
            LossVerdict::DiscardSession { pkid } => {
                tracing::error!(
                    client = %client_slug,
                    broker = %broker.identity.slug,
                    pkid,
                    reason = %disconnect_reason,
                    "the broker answered a packet identifier the client's session does not hold; \
                     discarding the session: the next connect is a clean start, the broker's \
                     queued messages for this client are lost, and publishes in flight end as \
                     after a session reset",
                );
                router
                    .session_fault(&client_slug, SessionFault::SessionDiscarded { pkid })
                    .await;
            }
            LossVerdict::GiveUp { .. } => {
                tracing::error!(
                    client = %client_slug,
                    broker = %broker.identity.slug,
                    reason = %disconnect_reason,
                    "discarding the MQTT session did not cure the unsolicited acknowledgement; \
                     stopping the client until its configuration is reloaded or the server \
                     restarts",
                );
                router
                    .session_fault(
                        &client_slug,
                        SessionFault::GaveUp {
                            reason: disconnect_reason.clone(),
                        },
                    )
                    .await;
            }
        }

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
            unasserted = unasserted.len(),
            "broker connection lost; reconnecting",
        );

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

    // TODO(mqtt-idle-client-visibility): a `Failed` terminal state is the end
    // of this client for the life of the process and reaches the operator
    // through this task's log line and nothing else, except the give-up on an
    // uncured `Unsolicited`, which the router also raises as an alert.
    // Terminal-state tail: every exit path must leave supervisor_state in a
    // terminal (non-Connected) state. Every path arrives here with its reason,
    // and the two authoritative-failure paths have already written that same
    // `Failed` beside clearing the client cell, so this write is idempotent
    // rather than exclusive.
    match terminal_reason {
        Some(state) => {
            let mut guard = handle.supervisor_state.write().await;
            *guard = state;
        }
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
/// library's immediate channel, which waits for no acknowledgement, and the
/// event loop is polled until it reports the request done or until the
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
    let disconnect = if connection_ended {
        "not owed: the connection ended"
    } else {
        client.try_disconnect_now().unwrap_or_else(|e| {
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
                    Err(ConnectionError::RequestsDone) => break "written",
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
    tracing::info!(
        client = %client_slug,
        disconnect,
        "MQTT session stopped: connection closed",
    );
}

/// Clear the per-connection SUBSCRIBE bookkeeping: the pkid → filter bindings
/// of SUBSCRIBEs written on a connection and the broker's answers.
///
/// A SUBSCRIBE written on a connection that ended is never answered — Brenn's
/// SUBSCRIBEs are untracked, so the library does not replay one it has
/// written — and a SUBACK belongs to the connection that carried it. The next
/// connection's answers arrive when its own SUBSCRIBEs are acked.
async fn clear_connection_tracking(handle: &MqttClientHandle) {
    handle.inflight_subscribes.lock().await.clear();
    handle.clear_outcomes().await;
}

/// The SUBSCRIBE bookkeeping a CONNACK leaves. The previous connection's
/// SUBACK answers are void. With no session, the library has discarded every
/// SUBSCRIBE it kept, so none of the FIFO's entries will be written; with a
/// session, the FIFO holds exactly the SUBSCRIBEs the library replays next, in
/// order, and is kept to bind them.
async fn subscribe_bookkeeping_at_connack(handle: &MqttClientHandle, session_present: bool) {
    clear_connection_tracking(handle).await;
    // TODO(mqtt-replayed-subscribe-unverified): both branches rest on the
    // library replaying a SUBSCRIBE it held unsent at the loss (first, in
    // order) with a session and discarding it without one; no test runs the
    // library through either.
    if !session_present {
        clear_subscribe_fifo(handle);
    }
}

/// Clear the FIFO of SUBSCRIBEs the event loop holds unwritten, when nothing
/// in it will be written: the broker reported no session, so the library
/// discarded them, or a new body starts on a new event loop.
fn clear_subscribe_fifo(handle: &MqttClientHandle) {
    handle
        .pending_subscribes
        .lock()
        .expect(PENDING_SUBSCRIBES_POISONED)
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
/// broker cannot be relied on to send it again. SUBSCRIBE attribution differs:
/// a SUBSCRIBE written on a lost connection is never answered, so its
/// `Outgoing::Subscribe` takes its filter off the FIFO and binds nothing, and
/// a SUBACK of a lost connection records nothing.
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
        // TODO(mqtt-inbound-retain-flag-not-on-envelope): `p.retain` is not
        // carried to the subscriber.
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
                    routing,
                )
                .await;
        }

        // --- Outgoing subscribe: bind pkid → pending filter (FIFO) ---
        Event::Outgoing(Outgoing::Subscribe(pkid)) => {
            let filter = handle
                .pending_subscribes
                .lock()
                .expect(PENDING_SUBSCRIBES_POISONED)
                .pop_front();
            if let Some(filter) = filter {
                match phase {
                    ConnectionPhase::Live => {
                        handle.inflight_subscribes.lock().await.insert(pkid, filter);
                    }
                    ConnectionPhase::Lost => {
                        tracing::debug!(
                            client = %client_slug,
                            pkid,
                            topic = %filter,
                            "SUBSCRIBE written on a lost connection; it will not be answered",
                        );
                    }
                }
            } else {
                tracing::warn!(
                    client = %client_slug,
                    pkid,
                    "Outgoing::Subscribe with no pending filter — possible session resend",
                );
            }
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

    struct NullRouter;
    #[async_trait::async_trait]
    impl MqttEventRouter for NullRouter {
        async fn deliver_inbound(
            &self,
            _client_slug: &str,
            _topic: &str,
            _payload: crate::payload::InboundPayload,
            _qos: u8,
            _routing: InboundRouting,
        ) {
        }

        async fn session_fault(&self, client_slug: &str, fault: SessionFault) {
            panic!(
                "no test on this router provokes a session fault; got {fault:?} on {client_slug}"
            );
        }
    }

    /// A router that keeps each call's topic and routing, and its delivery QoS
    /// apart.
    #[derive(Default)]
    struct RecordingRouter {
        calls: std::sync::Mutex<Vec<(String, InboundRouting)>>,
        qos: std::sync::Mutex<Vec<u8>>,
    }

    impl RecordingRouter {
        fn take(&self) -> Vec<(String, InboundRouting)> {
            std::mem::take(&mut *self.calls.lock().unwrap())
        }

        fn take_qos(&self) -> Vec<u8> {
            std::mem::take(&mut *self.qos.lock().unwrap())
        }
    }

    #[async_trait::async_trait]
    impl MqttEventRouter for RecordingRouter {
        async fn deliver_inbound(
            &self,
            _client_slug: &str,
            topic: &str,
            _payload: crate::payload::InboundPayload,
            qos: u8,
            routing: InboundRouting,
        ) {
            self.qos.lock().unwrap().push(qos);
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
                .cloned()
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
                .cloned()
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
        *handle.supervisor_state.write().await = SupervisorState::Connected;
        let stop_rx = handle.stop_tx.subscribe();
        handle.stop_tx.send(true).expect("stop_rx is alive");

        tokio::time::timeout(
            Duration::from_secs(2),
            supervisor_body(handle.clone(), Arc::new(NullRouter), stop_rx),
        )
        .await
        .expect("the supervisor did not stop");

        assert!(handle.client.lock().await.is_none());
        assert!(matches!(
            *handle.supervisor_state.read().await,
            SupervisorState::Disconnected {
                last_error: None,
                ..
            }
        ));
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
            .push_back("home/+/state".to_string());
        handle
            .pending_subscribes
            .lock()
            .unwrap()
            .push_back("sensors/#".to_string());

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
            .push_back("home/+/state".to_string());
        handle
            .pending_subscribes
            .lock()
            .unwrap()
            .push_back("sensors/#".to_string());
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
            .push_back("home/+/state".to_string());
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
            .push_back("home/+/state".to_string());
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
                .push_back(filter.to_string());
        }
        lost(Event::Outgoing(Outgoing::Subscribe(10)), &handle, &router).await;
        assert!(handle.inflight_subscribes.lock().await.is_empty());
        assert_eq!(
            handle
                .pending_subscribes
                .lock()
                .unwrap()
                .iter()
                .cloned()
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
                    .push_back(filter.to_string());
            }

            subscribe_bookkeeping_at_connack(&handle, session_present).await;

            assert!(handle.inflight_subscribes.lock().await.is_empty());
            assert_eq!(handle.subscribe_outcome("c/d").await, None);
            let fifo: Vec<String> = handle
                .pending_subscribes
                .lock()
                .unwrap()
                .iter()
                .cloned()
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

    /// A new body clears the SUBSCRIBE FIFO a previous body left: nothing of
    /// that body's event loop is replayed on the new one.
    #[tokio::test]
    async fn a_new_body_starts_with_no_subscribe_bookkeeping() {
        let handle = test_handle();
        handle
            .pending_subscribes
            .lock()
            .unwrap()
            .push_back("a/b".to_string());
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
        assert!(handle.inflight_subscribes.lock().await.is_empty());
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

    /// Poll `wait` once, with a waker that does nothing.
    fn poll_once<F: Future>(wait: std::pin::Pin<&mut F>) -> std::task::Poll<F::Output> {
        wait.poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
    }

    /// A subscribe wake runs the SUBSCRIBE pushes again without dropping the
    /// poll, however often it comes, and the wait ends with the poll's own
    /// output.
    #[tokio::test]
    async fn a_subscribe_wake_serves_subscribes_and_keeps_the_poll() {
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        let dropped = Arc::new(AtomicBool::new(false));
        let (finish, poll) = watched_poll(&dropped);
        let (_stop_tx, mut stop_rx) = tokio::sync::watch::channel(false);
        let wake = tokio::sync::Notify::new();
        let served = AtomicUsize::new(0);
        let wait = await_connected_poll(poll, &mut stop_rx, &wake, || {
            served.fetch_add(1, Ordering::SeqCst);
        });
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
            let wait = await_connected_poll(poll, &mut stop_rx, &wake, || {});
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
}
