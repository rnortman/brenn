//! MQTT connection supervisor: one per active `[[mqtt_client]]` session.
//!
//! Each supervisor runs in a dedicated `tokio::spawn` task. It owns the
//! `rumqttc::AsyncClient` + `EventLoop`, drives the event loop forever, handles
//! reconnect with jittered exponential backoff, re-asserts subscriptions on every
//! successful connect, binds outbound publish acks, and routes every inbound
//! PUBLISH to the bridge router. One session per client carries both the publish
//! and the ingress-delivery paths.

use std::sync::Arc;
use std::time::{Duration, Instant};

use rumqttc::mqttbytes::QoS;
use rumqttc::{
    AsyncClient, ConnectReturnCode, ConnectionError, DisconnectReasonCode, Event, Filter, Incoming,
    MqttOptions, Outgoing, PubAckReason, RetainForwardRule, StateError, SubscribeProperties,
    SubscribeReasonCode, TlsConfiguration, Transport,
};
use rustls_pki_types::pem::PemObject as _;

use crate::mqtt::config::{MqttClientConfig, ResolvedMqttIngressChannel, TlsVersionMin};
use crate::mqtt::payload::classify_inbound;
use crate::mqtt::service::MqttEventRouter;
use crate::mqtt::state::{IngressSubscription, MqttClientHandle, PubackOutcome, SupervisorState};

// Maximum time to wait for the DISCONNECT drain (flush DISCONNECT bytes to the
// broker) before giving up. The drain blocks on broker liveness; this cap
// prevents a non-conforming or slow-to-close broker from hanging the supervisor
// task indefinitely after the terminal state is already written.
const DISCONNECT_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

// ---------------------------------------------------------------------------
// Union subscription set
// ---------------------------------------------------------------------------

/// Collapse one client's ingress channels into the deduplicated set of
/// `(topic_filter, qos)` subscriptions.
///
/// Distinct filter *strings* on one client yield distinct SUBSCRIBEs (each its
/// own `sub_id`). Each distinct filter is assigned a sequential `sub_id` (1, 2,
/// …) in first-seen order. `channels` may include channels for other clients;
/// only those whose `client_slug == client_slug` are considered.
pub fn union_subscriptions(
    client_slug: &str,
    channels: &[ResolvedMqttIngressChannel],
) -> Vec<IngressSubscription> {
    let mut subs: Vec<IngressSubscription> = Vec::new();
    for channel in channels {
        if channel.client_slug != client_slug {
            continue;
        }
        if let Some(existing) = subs.iter_mut().find(|s| s.topic_filter == channel.topic) {
            existing.qos = existing.qos.max(channel.qos);
        } else {
            let sub_id = subs.len() as u32 + 1;
            subs.push(IngressSubscription {
                topic_filter: channel.topic.clone(),
                qos: channel.qos,
                sub_id,
            });
        }
    }
    subs
}

/// Issue a single broker SUBSCRIBE for one ingress subscription on a live
/// `AsyncClient`, using the `OnEverySubscribe` retain rule so the broker
/// re-delivers the current retained message on every (re)subscribe.
///
/// Shared by the supervisor's reconnect re-assert loop and the runtime
/// dynamic-`mqtt:`-subscribe path (`MqttService::subscribe_filter`), so the two
/// never drift on subscribe semantics. The filter is pushed onto
/// `handle.pending_subscribes` (under that lock, atomically with the send) so the
/// `Outgoing::Subscribe` arm can bind pkid → filter for correct SubAck
/// attribution.
///
/// Returns `Err(message)` on a failed SUBSCRIBE *send*; there is no SUBACK wait.
pub async fn assert_ingress_subscription(
    handle: &Arc<MqttClientHandle>,
    client: &AsyncClient,
    sub: &IngressSubscription,
) -> Result<(), String> {
    let rumq_qos = match sub.qos {
        0 => QoS::AtMostOnce,
        1 => QoS::AtLeastOnce,
        _ => QoS::ExactlyOnce,
    };
    let filter = Filter {
        path: sub.topic_filter.clone(),
        qos: rumq_qos,
        nolocal: false,
        preserve_retain: false,
        retain_forward_rule: RetainForwardRule::OnEverySubscribe,
    };
    let props = SubscribeProperties {
        id: Some(sub.sub_id as usize),
        user_properties: vec![],
    };
    // Hold pending_subscribes across the push + send so the push order equals the
    // submit order equals the Outgoing::Subscribe order (FIFO attribution).
    let mut pending = handle.pending_subscribes.lock().await;
    pending.push_back(sub.topic_filter.clone());
    match client
        .subscribe_many_with_properties(std::iter::once(filter), props)
        .await
    {
        Ok(()) => Ok(()),
        Err(e) => {
            pending.pop_back();
            Err(e.to_string())
        }
    }
}

/// Issue a single broker UNSUBSCRIBE for one ingress topic filter on a live
/// `AsyncClient`. The inverse of [`assert_ingress_subscription`]. There is no
/// UNSUBACK wait.
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
    format!("brenn:{}", broker.slug)
}

/// Build `MqttOptions` from broker config, using a pre-built `transport`.
///
/// `transport` is accepted as a parameter (rather than built here) so callers can
/// construct it once per supervisor lifetime and clone the cheap `Arc<ClientConfig>`
/// wrapper on every reconnect attempt, avoiding repeated disk I/O and crypto-provider
/// allocation on the reconnect hot path.
fn build_mqtt_options(broker: &MqttClientConfig, transport: Transport) -> MqttOptions {
    let mut opts = MqttOptions::new(client_id(broker), (broker.host.clone(), broker.port));
    opts.set_clean_start(false);
    opts.set_session_expiry_interval(Some(broker.session_expiry_secs));

    if let Some(secs) = broker.keepalive_secs {
        // rumqttc-next set_keep_alive takes u16 seconds (not Duration).
        let secs = secs.max(5) as u16; // rumqttc requires >= 5s
        opts.set_keep_alive(secs);
    }

    if let Some(ref username) = broker.username {
        let password = broker.password.as_deref().unwrap_or("").to_owned();
        opts.set_credentials(username.clone(), password);
    }

    // Inbound payload cap — advertised to broker in CONNECT. Validated to fit
    // `u32` at config resolution, so no cast is needed here.
    opts.set_max_packet_size(Some(broker.inbound_payload_cap_bytes));

    // TLS transport pre-built by the caller (once per supervisor lifetime).
    opts.set_transport(transport);

    // Last Will.
    if let Some(ref lw) = broker.last_will {
        let rumq_qos = match lw.qos {
            0 => QoS::AtMostOnce,
            1 => QoS::AtLeastOnce,
            _ => QoS::ExactlyOnce,
        };
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
        let (store, _) = load_ca_cert_pem(&broker.slug, pem_bytes);
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
                    broker = %broker.slug,
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

    match broker.tls_version_min {
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
/// respawns after a brief pause.
pub fn spawn_client_supervisor(
    handle: Arc<MqttClientHandle>,
    router: Arc<dyn MqttEventRouter>,
    stop_rx: tokio::sync::watch::Receiver<bool>,
) {
    tokio::spawn(async move {
        loop {
            let handle2 = handle.clone();
            let router2 = router.clone();
            let stop_rx2 = stop_rx.clone();
            let join = tokio::spawn(supervisor_body(handle2, router2, stop_rx2));
            match join.await {
                Ok(()) => break, // normal exit (stop signal / authoritative give-up)
                Err(e) if e.is_panic() => {
                    tracing::error!(
                        client = %handle.config.slug,
                        broker = %handle.config.slug,
                        "MQTT supervisor task panicked; respawning. This is a bug.",
                    );
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    // continue — respawn
                }
                Err(e) => {
                    tracing::warn!(
                        client = %handle.config.slug,
                        broker = %handle.config.slug,
                        error = %e,
                        "MQTT supervisor task ended unexpectedly; not respawning",
                    );
                    break;
                }
            }
        }
    });
}

/// The actual supervisor body, wrapped by the watchdog in
/// `spawn_client_supervisor`.
async fn supervisor_body(
    handle: Arc<MqttClientHandle>,
    router: Arc<dyn MqttEventRouter>,
    mut stop_rx: tokio::sync::watch::Receiver<bool>,
) {
    // The resolved per-client config is the single source of truth; the handle
    // shares it with this supervisor (cheap Arc clone, once per supervisor
    // lifetime). Read broker coordinates/backoff/slug through it.
    let broker = handle.config.clone();
    let client_slug = broker.slug.clone();
    let rng_seed = client_id_seed(&client_id(&broker));

    let mut attempt: u32 = 0;

    // Build the TLS transport once per supervisor lifetime (Arc-cheap to clone on
    // each reconnect).
    let tls_transport = build_tls_transport(&broker);

    // First write wins; every exit path must set this before breaking.
    let mut terminal_reason: Option<SupervisorState> = None;

    'supervisor: loop {
        if *stop_rx.borrow() {
            tracing::info!(client = %client_slug, broker = %broker.slug, "supervisor stopping");
            let client_opt = handle.client.lock().await.take();
            if let Some(client) = client_opt {
                let _ = client.disconnect().await;
            }
            if terminal_reason.is_none() {
                terminal_reason = Some(SupervisorState::Disconnected {
                    last_error: None,
                    next_attempt_at: Instant::now(),
                });
            }
            break;
        }

        tracing::info!(client = %client_slug, broker = %broker.slug, attempt, "connecting to broker");

        {
            let mut state = handle.supervisor_state.write().await;
            *state = SupervisorState::Connecting {
                since: Instant::now(),
            };
        }

        let opts = build_mqtt_options(&broker, tls_transport.clone());
        let (client, mut eventloop) = AsyncClient::builder(opts).capacity(64).build();

        // Poll until ConnAck or error. `None` = stop requested.
        let connack: Option<Result<bool, ConnectionError>> = loop {
            tokio::select! {
                biased;
                _ = stop_rx.changed() => {
                    if *stop_rx.borrow() {
                        break None;
                    }
                    continue;
                }
                event = eventloop.poll() => {
                    match event {
                        Err(e) => break Some(Err(e)),
                        Ok(Event::Incoming(Incoming::ConnAck(ack))) => {
                            break Some(Ok(ack.session_present));
                        }
                        Ok(_) => continue,
                    }
                }
            }
        };

        let session_present = match connack {
            None => {
                // Clean stop during connect handshake; no client installed yet.
                if terminal_reason.is_none() {
                    terminal_reason = Some(SupervisorState::Disconnected {
                        last_error: None,
                        next_attempt_at: Instant::now(),
                    });
                }
                break;
            }
            Some(Err(e)) => {
                let authoritative = is_authoritative_failure(&e);
                let error_str = e.to_string();
                handle.fail_all_publishes(None).await;
                clear_subscribe_tracking(&handle).await;
                {
                    let mut client_guard = handle.client.lock().await;
                    *client_guard = None;
                }
                if authoritative {
                    tracing::error!(
                        client = %client_slug,
                        broker = %broker.slug,
                        error = %error_str,
                        "broker connection failed with authoritative error; stopping retry",
                    );
                    let failed = SupervisorState::Failed { reason: error_str };
                    if terminal_reason.is_none() {
                        terminal_reason = Some(failed);
                    }
                    break;
                }
                tracing::warn!(
                    client = %client_slug,
                    broker = %broker.slug,
                    error = %error_str,
                    "broker connection failed; will retry",
                );
                {
                    let mut state = handle.supervisor_state.write().await;
                    *state = SupervisorState::Disconnected {
                        last_error: Some(error_str),
                        next_attempt_at: Instant::now(),
                    };
                }
                let delay = backoff_duration(
                    attempt,
                    broker.reconnect_backoff_initial_secs,
                    broker.reconnect_backoff_max_secs,
                    rng_seed,
                );
                attempt += 1;
                tokio::select! {
                    _ = tokio::time::sleep(delay) => {}
                    _ = stop_rx.changed() => {}
                }
                continue;
            }
            Some(Ok(sp)) => sp,
        };

        // Clear any stale SUBSCRIBE attribution before installing the client. At
        // this instant `handle.client` is still None, so no legitimate concurrent
        // `subscribe_filter` can hold a pending entry; any residue is provably
        // stale (e.g. a subscribe that raced the previous disconnect and sent onto
        // the now-dead request channel after `clear_subscribe_tracking` ran). Left
        // uncleared it would shift pkid→filter attribution for this whole cycle.
        clear_subscribe_tracking(&handle).await;

        // Connected — install client in handle.
        {
            let mut client_guard = handle.client.lock().await;
            *client_guard = Some(client.clone());
        }
        {
            let mut state = handle.supervisor_state.write().await;
            *state = SupervisorState::Connected;
        }
        attempt = 0;

        tracing::info!(
            client = %client_slug,
            broker = %broker.slug,
            session_present,
            "connected to broker",
        );

        // Re-assert all union subscriptions (static config union plus any
        // runtime-added dynamic `mqtt:` subscriptions). Snapshot under a brief
        // read-lock and drop the guard before the per-subscribe awaits.
        let subscriptions_snapshot = handle.subscriptions.read().await.clone();
        for sub in &subscriptions_snapshot {
            if let Err(e) = assert_ingress_subscription(&handle, &client, sub).await {
                tracing::error!(
                    client = %client_slug,
                    broker = %broker.slug,
                    topic = %sub.topic_filter,
                    error = %e,
                    "failed to send subscribe — filter not asserted; channel will receive \
                     nothing until the next reconnect",
                );
            }
        }

        // Main event loop.
        let disconnect_reason = loop {
            tokio::select! {
                biased;
                _ = stop_rx.changed() => {
                    if *stop_rx.borrow() {
                        // Write Disconnected BEFORE the drain so the observable
                        // health transition does not gate on broker liveness.
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
                        let client_opt = handle.client.lock().await.take();
                        if let Some(c) = client_opt {
                            let _ = c.disconnect().await;
                            let drain_result = tokio::time::timeout(
                                DISCONNECT_DRAIN_TIMEOUT,
                                async { while eventloop.poll().await.is_ok() {} },
                            )
                            .await;
                            if drain_result.is_err() {
                                tracing::warn!(
                                    client = %client_slug,
                                    broker = %broker.slug,
                                    "DISCONNECT drain timed out after {}s; broker may not have \
                                     closed the TCP connection promptly",
                                    DISCONNECT_DRAIN_TIMEOUT.as_secs(),
                                );
                            }
                        }
                        break 'supervisor;
                    }
                }
                event = eventloop.poll() => {
                    match event {
                        Err(e) => {
                            let authoritative = is_authoritative_failure(&e);
                            break (e.to_string(), authoritative);
                        }
                        Ok(ev) => {
                            handle_event(ev, &handle, &router).await;
                        }
                    }
                }
            }
        };

        // Connection lost.
        let (disconnect_reason, disconnect_authoritative) = disconnect_reason;
        {
            let mut client_guard = handle.client.lock().await;
            *client_guard = None;
        }
        handle
            .fail_all_publishes(Some(disconnect_reason.clone()))
            .await;
        clear_subscribe_tracking(&handle).await;

        if disconnect_authoritative {
            tracing::error!(
                client = %client_slug,
                broker = %broker.slug,
                reason = %disconnect_reason,
                "broker connection lost with authoritative error; stopping retry",
            );
            let failed = SupervisorState::Failed {
                reason: disconnect_reason,
            };
            if terminal_reason.is_none() {
                terminal_reason = Some(failed);
            }
            break;
        }

        tracing::warn!(
            client = %client_slug,
            broker = %broker.slug,
            reason = %disconnect_reason,
            "broker connection lost; reconnecting",
        );
        {
            let mut state = handle.supervisor_state.write().await;
            *state = SupervisorState::Disconnected {
                last_error: Some(disconnect_reason),
                next_attempt_at: Instant::now(),
            };
        }

        let delay = backoff_duration(
            attempt,
            broker.reconnect_backoff_initial_secs,
            broker.reconnect_backoff_max_secs,
            rng_seed,
        );
        attempt += 1;
        tokio::select! {
            _ = tokio::time::sleep(delay) => {}
            _ = stop_rx.changed() => {}
        }
    }

    // Terminal-state tail: every exit path must leave supervisor_state in a
    // terminal (non-Connected) state. The tail is the single write site.
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

/// Clear the per-connect SUBSCRIBE attribution bookkeeping on disconnect so a
/// stale pkid cannot mis-attribute a SubAck after reconnect.
async fn clear_subscribe_tracking(handle: &Arc<MqttClientHandle>) {
    handle.pending_subscribes.lock().await.clear();
    handle.inflight_subscribes.lock().await.clear();
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

/// Returns `true` if the error indicates an authoritative broker rejection
/// (TLS failure, auth denied) rather than a transient network error.
pub(crate) fn is_authoritative_failure(err: &ConnectionError) -> bool {
    match err {
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

// ---------------------------------------------------------------------------
// Event dispatch
// ---------------------------------------------------------------------------

async fn handle_event(
    event: Event,
    handle: &Arc<MqttClientHandle>,
    router: &Arc<dyn MqttEventRouter>,
) {
    let client_slug = handle.config.slug.as_str();
    match event {
        // --- Inbound publish ---
        // SECURITY: inbound payloads are untrusted. Any MQTT client with write
        // access to a subscribed topic can inject arbitrary text into the LLM
        // context (prompt injection). Broker ACLs must restrict topic write access
        // to authorised publishers only. Every PUBLISH is handed to the router,
        // which matches the actual topic against bridge filters for this client.
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

            let inbound_payload = classify_inbound(&p.payload[..], content_type.as_deref());

            router
                .deliver_inbound(client_slug, &topic_str, inbound_payload, delivery_qos)
                .await;
        }

        // --- Outgoing subscribe: bind pkid → pending filter (FIFO) ---
        Event::Outgoing(Outgoing::Subscribe(pkid)) => {
            let filter = handle.pending_subscribes.lock().await.pop_front();
            if let Some(filter) = filter {
                handle.inflight_subscribes.lock().await.insert(pkid, filter);
            } else {
                tracing::warn!(
                    client = %client_slug,
                    pkid,
                    "Outgoing::Subscribe with no pending filter — possible session resend",
                );
            }
        }

        // --- SubAck: log reason codes with the attributed filter ---
        //
        // Each SUBSCRIBE carries one filter, so a SUBACK has exactly one return
        // code; the filter is resolved by pkid (bound in the Outgoing::Subscribe
        // arm), not by positional index into the whole subscription set.
        Event::Incoming(Incoming::SubAck(ack)) => {
            let topic_filter = handle
                .inflight_subscribes
                .lock()
                .await
                .remove(&ack.pkid)
                .unwrap_or_else(|| "<unknown>".to_string());
            for code in ack.return_codes.iter() {
                match code {
                    SubscribeReasonCode::Success(qos) => {
                        tracing::info!(
                            client = %client_slug,
                            topic = %topic_filter,
                            granted_qos = ?qos,
                            "SUBACK: subscription granted",
                        );
                    }
                    // A rejection is an authoritative broker decision (ACL
                    // misconfiguration, broker policy): the bridge receives no
                    // messages for this filter until it is fixed. Log at ERROR.
                    other => {
                        tracing::error!(
                            client = %client_slug,
                            topic = %topic_filter,
                            reason = ?other,
                            "SUBACK: subscription rejected by ACL or broker — bridge will \
                             receive no messages for this filter",
                        );
                    }
                }
            }
        }

        // --- Outgoing publish: bind pkid → pending waiter (FIFO) ---
        Event::Outgoing(Outgoing::Publish(pkid)) if pkid != 0 => {
            let pending_entry = {
                let mut pending = handle.pending_publishes.lock().await;
                pending.pop_front()
            };
            if let Some(entry) = pending_entry {
                handle.inflight_publishes.lock().await.insert(pkid, entry);
            } else {
                tracing::warn!(
                    client = %client_slug,
                    pkid,
                    "Outgoing::Publish with non-zero pkid but no pending waiter — \
                     possible rumqttc session resend or bookkeeping mismatch",
                );
            }
        }

        // --- PubAck (QoS 1 ack) ---
        Event::Incoming(Incoming::PubAck(ack)) => {
            let entry = handle.inflight_publishes.lock().await.remove(&ack.pkid);
            if let Some(entry) = entry {
                let outcome = if ack.reason == PubAckReason::Success
                    || ack.reason == PubAckReason::NoMatchingSubscribers
                {
                    Ok(PubackOutcome::Success)
                } else {
                    let reason = ack
                        .properties
                        .as_ref()
                        .and_then(|p| p.reason_string.clone())
                        .unwrap_or_else(|| format!("{:?}", ack.reason));
                    tracing::warn!(
                        client = %client_slug,
                        pkid = ack.pkid,
                        reason = %reason,
                        "publish rejected by broker (PUBACK)",
                    );
                    Ok(PubackOutcome::BrokerRejected { reason })
                };
                if entry.ack_tx.send(outcome).is_err() {
                    tracing::debug!(
                        client = %client_slug,
                        pkid = ack.pkid,
                        "PUBACK receiver dropped before ack arrived (caller cancelled or timed out)",
                    );
                }
            } else {
                tracing::warn!(
                    client = %client_slug,
                    pkid = ack.pkid,
                    "PUBACK for unknown pkid — no matching inflight publish; possible broker \
                     double-ack or session resend",
                );
            }
        }

        // --- PubRec (QoS 2 receive-ack) ---
        //
        // Only surfaced here on broker rejection. rumqttc drives a successful
        // PubRec through to PubRel/PubComp internally, so success needs no action
        // (the PubComp arm resolves the waiter). A rejection (reason >= 0x80)
        // completes the packet WITHOUT emitting PubRel/PubComp, so if we do not
        // resolve the waiter here it blocks forever and the inflight entry leaks.
        Event::Incoming(Incoming::PubRec(rec))
            if rec.reason != rumqttc::PubRecReason::Success
                && rec.reason != rumqttc::PubRecReason::NoMatchingSubscribers =>
        {
            let entry = handle.inflight_publishes.lock().await.remove(&rec.pkid);
            if let Some(entry) = entry {
                let reason = rec
                    .properties
                    .as_ref()
                    .and_then(|p| p.reason_string.clone())
                    .unwrap_or_else(|| format!("{:?}", rec.reason));
                tracing::warn!(
                    client = %client_slug,
                    pkid = rec.pkid,
                    reason = %reason,
                    "publish rejected by broker (PUBREC)",
                );
                if entry
                    .ack_tx
                    .send(Ok(PubackOutcome::BrokerRejected { reason }))
                    .is_err()
                {
                    tracing::debug!(
                        client = %client_slug,
                        pkid = rec.pkid,
                        "PUBREC receiver dropped before ack arrived (caller cancelled or timed out)",
                    );
                }
            } else {
                tracing::warn!(
                    client = %client_slug,
                    pkid = rec.pkid,
                    "PUBREC rejection for unknown pkid — no matching inflight publish; \
                     possible broker double-ack or session resend",
                );
            }
        }

        // --- PubComp (QoS 2 final ack) ---
        Event::Incoming(Incoming::PubComp(comp)) => {
            let entry = handle.inflight_publishes.lock().await.remove(&comp.pkid);
            if let Some(entry) = entry {
                let outcome = if comp.reason == rumqttc::PubCompReason::Success {
                    Ok(PubackOutcome::Success)
                } else {
                    let reason = comp
                        .properties
                        .as_ref()
                        .and_then(|p| p.reason_string.clone())
                        .unwrap_or_else(|| format!("{:?}", comp.reason));
                    tracing::warn!(
                        client = %client_slug,
                        pkid = comp.pkid,
                        reason = %reason,
                        "publish rejected by broker (PUBCOMP)",
                    );
                    Ok(PubackOutcome::BrokerRejected { reason })
                };
                if entry.ack_tx.send(outcome).is_err() {
                    tracing::debug!(
                        client = %client_slug,
                        pkid = comp.pkid,
                        "PUBCOMP receiver dropped before ack arrived (caller cancelled or timed out)",
                    );
                }
            } else {
                tracing::warn!(
                    client = %client_slug,
                    pkid = comp.pkid,
                    "PUBCOMP for unknown pkid — no matching inflight publish; possible broker \
                     double-ack or session resend",
                );
            }
        }

        _ => {}
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messaging::Urgency;
    use crate::mqtt::state::{PendingPublish, PubackOutcome};

    fn channel(client: &str, topic: &str, qos: u8) -> ResolvedMqttIngressChannel {
        let address = crate::mqtt::config::parsed_address_canonical(client, topic);
        ResolvedMqttIngressChannel {
            channel_uuid: crate::messaging::mqtt_channel_uuid_from_address(&address),
            channel_address: address,
            client_slug: client.to_string(),
            topic: topic.to_string(),
            urgency: Urgency::Normal,
            qos,
        }
    }

    // --- union_subscriptions ---

    #[test]
    fn union_empty_when_no_channels_for_client() {
        let channels = vec![channel("other", "a/b", 1)];
        assert!(union_subscriptions("target", &channels).is_empty());
    }

    #[test]
    fn union_assigns_sequential_sub_ids_in_first_seen_order() {
        let channels = vec![
            channel("broker", "home/+/state", 1),
            channel("broker", "sensors/#", 0),
        ];
        let subs = union_subscriptions("broker", &channels);
        assert_eq!(
            subs,
            vec![
                IngressSubscription {
                    topic_filter: "home/+/state".into(),
                    qos: 1,
                    sub_id: 1,
                },
                IngressSubscription {
                    topic_filter: "sensors/#".into(),
                    qos: 0,
                    sub_id: 2,
                },
            ]
        );
    }

    #[test]
    fn union_dedups_shared_filter_at_max_qos() {
        let channels = vec![
            channel("broker", "home/+/state", 0),
            channel("broker", "home/+/state", 2),
        ];
        let subs = union_subscriptions("broker", &channels);
        assert_eq!(subs.len(), 1);
        assert_eq!(subs[0].qos, 2);
        assert_eq!(subs[0].sub_id, 1);
    }

    #[test]
    fn union_filters_out_other_clients() {
        let channels = vec![
            channel("broker-a", "a/#", 1),
            channel("broker-b", "b/#", 1),
            channel("broker-a", "c/#", 1),
        ];
        let subs = union_subscriptions("broker-a", &channels);
        assert_eq!(subs.len(), 2);
        assert_eq!(subs[0].topic_filter, "a/#");
        assert_eq!(subs[1].topic_filter, "c/#");
    }

    // --- SubAck pkid → filter attribution ---

    fn test_handle() -> Arc<MqttClientHandle> {
        let (stop_tx, _rx) = tokio::sync::watch::channel(false);
        let config = Arc::new(crate::mqtt::test_support::test_client_config("broker"));
        MqttClientHandle::new(config, vec![], stop_tx)
    }

    struct NullRouter;
    #[async_trait::async_trait]
    impl MqttEventRouter for NullRouter {
        async fn deliver_inbound(
            &self,
            _client_slug: &str,
            _topic: &str,
            _payload: crate::mqtt::payload::InboundPayload,
            _qos: u8,
        ) {
        }
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
            .await
            .push_back("home/+/state".to_string());
        handle
            .pending_subscribes
            .lock()
            .await
            .push_back("sensors/#".to_string());

        handle_event(Event::Outgoing(Outgoing::Subscribe(10)), &handle, &router).await;
        handle_event(Event::Outgoing(Outgoing::Subscribe(11)), &handle, &router).await;

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
        handle_event(Event::Incoming(Incoming::SubAck(ack)), &handle, &router).await;
        assert!(handle.inflight_subscribes.lock().await.get(&11).is_none());
        // The first filter's binding is untouched.
        assert_eq!(
            handle.inflight_subscribes.lock().await.get(&10).cloned(),
            Some("home/+/state".to_string()),
        );
    }

    // --- PubRec rejection (QoS2 caller-hang guard) ---

    /// A QoS2 publish rejected at PUBREC (reason >= 0x80) resolves its waiter with
    /// `BrokerRejected` and removes the inflight entry — without this arm the broker
    /// emits no PUBREL/PUBCOMP, so `ack_rx.await` would hang forever and the entry
    /// would leak.
    #[tokio::test]
    async fn pubrec_rejection_resolves_waiter_and_removes_inflight() {
        let handle = test_handle();
        let router: Arc<dyn MqttEventRouter> = Arc::new(NullRouter);

        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        handle
            .inflight_publishes
            .lock()
            .await
            .insert(7, PendingPublish { ack_tx });

        let rec = rumqttc::PubRec {
            pkid: 7,
            reason: rumqttc::PubRecReason::NotAuthorized,
            properties: None,
        };
        handle_event(Event::Incoming(Incoming::PubRec(rec)), &handle, &router).await;

        match ack_rx.await {
            Ok(Ok(PubackOutcome::BrokerRejected { reason })) => {
                // reason falls back to the debug form of the reason code.
                assert!(reason.contains("NotAuthorized"), "reason was {reason:?}");
            }
            other => panic!("expected BrokerRejected, got {other:?}"),
        }
        assert!(
            !handle.inflight_publishes.lock().await.contains_key(&7),
            "rejected pkid must be removed from the inflight map",
        );
    }

    /// A *successful* PUBREC is left for rumqttc to drive to PUBCOMP: the rejection
    /// arm's guard excludes it, so the inflight entry stays in place (the PubComp arm
    /// resolves it later).
    #[tokio::test]
    async fn pubrec_success_leaves_inflight_for_pubcomp() {
        let handle = test_handle();
        let router: Arc<dyn MqttEventRouter> = Arc::new(NullRouter);

        let (ack_tx, _ack_rx) = tokio::sync::oneshot::channel();
        handle
            .inflight_publishes
            .lock()
            .await
            .insert(9, PendingPublish { ack_tx });

        let rec = rumqttc::PubRec {
            pkid: 9,
            reason: rumqttc::PubRecReason::Success,
            properties: None,
        };
        handle_event(Event::Incoming(Incoming::PubRec(rec)), &handle, &router).await;

        assert!(
            handle.inflight_publishes.lock().await.contains_key(&9),
            "a successful PUBREC must leave the inflight entry for the PUBCOMP arm",
        );
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

    #[test]
    fn authoritative_failure_tls() {
        let tls_err = ConnectionError::Tls(rumqttc::TlsError::Io(std::io::Error::other(
            "certificate verify failed",
        )));
        assert!(is_authoritative_failure(&tls_err));
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

    // --- fail_all_publishes ---

    #[tokio::test]
    async fn fail_all_publishes_drains_both_deques() {
        let handle = test_handle();

        let (tx1, rx1) = tokio::sync::oneshot::channel();
        let (tx2, rx2) = tokio::sync::oneshot::channel();
        let (tx3, rx3) = tokio::sync::oneshot::channel();
        {
            let mut p = handle.pending_publishes.lock().await;
            p.push_back(crate::mqtt::state::PendingPublish { ack_tx: tx1 });
            p.push_back(crate::mqtt::state::PendingPublish { ack_tx: tx2 });
        }
        {
            let mut inflight = handle.inflight_publishes.lock().await;
            inflight.insert(42, crate::mqtt::state::PendingPublish { ack_tx: tx3 });
        }

        handle.fail_all_publishes(Some("test error".into())).await;

        assert!(matches!(
            rx1.await.unwrap(),
            Err(crate::mqtt::error::MqttError::NotConnected { .. })
        ));
        assert!(matches!(
            rx2.await.unwrap(),
            Err(crate::mqtt::error::MqttError::NotConnected { .. })
        ));
        assert!(matches!(
            rx3.await.unwrap(),
            Err(crate::mqtt::error::MqttError::NotConnected { .. })
        ));

        assert!(handle.pending_publishes.lock().await.is_empty());
        assert!(handle.inflight_publishes.lock().await.is_empty());
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
        tls_version_min: crate::mqtt::config::TlsVersionMin,
    ) -> crate::mqtt::config::MqttClientConfig {
        let mut config = crate::mqtt::test_support::test_client_config("test-broker");
        config.ca_cert_pem = ca_cert_pem;
        config.tls_version_min = tls_version_min;
        config
    }

    #[test]
    fn build_tls_transport_tls13_custom_ca_yields_tls_transport() {
        let broker = make_test_broker(
            Some(TEST_CA_PEM.to_vec()),
            crate::mqtt::config::TlsVersionMin::Tls13,
        );
        let (store, added) = load_ca_cert_pem(&broker.slug, TEST_CA_PEM);
        assert_eq!(added, 1);
        assert_eq!(store.len(), 1);
        let transport = build_tls_transport(&broker);
        assert!(matches!(transport, Transport::Tls(_)));
    }

    #[test]
    fn build_tls_transport_tls12_default_yields_tls_transport() {
        let broker = make_test_broker(None, crate::mqtt::config::TlsVersionMin::Tls12);
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
            crate::mqtt::config::TlsVersionMin::Tls13,
        );
        let (store, added) = load_ca_cert_pem(&broker.slug, MALFORMED_CERT_PEM);
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
