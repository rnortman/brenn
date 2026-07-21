// Shared integration-test harness for the MQTT integration suite.
// All public items are re-exported at module root so test functions
// can `use common::*;` without multi-level path disambiguation.
//
// Per-test brokers: the design (§3.3) specified a process-singleton mosquitto.
// The implementation uses one broker per test instead, because each
// #[tokio::test(flavor="multi_thread")] creates its own tokio runtime and a
// tokio::sync::Mutex cannot be shared across runtimes. A std::sync::Mutex
// process-singleton would work, but 7 broker spawns × 10 review-gate runs is
// acceptable on a developer machine, and per-test brokers make each test fully
// independent without any teardown coordination.
//
// NOTE: §4.4's `assert_eq!(retained.len(), 100)` is an exact count; it relies
// on each test starting with a fresh broker (persistence=false eliminates bleed).
// If the harness is ever refactored back to a shared broker, verify §4.4's
// assertion still holds across test ordering.

pub mod certs;
pub mod relay;
pub mod router;

pub use relay::TcpRelay;
pub use router::{CapturingRouter, DeliveredMessage};

use std::io::Read as _;
use std::net::TcpListener;
use std::os::unix::process::CommandExt as _;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;

use brenn_lib::messaging::Urgency;
use brenn_lib::mqtt::config::{MqttClientConfig, TlsVersionMin};
use brenn_lib::mqtt::service::IngressSubscribeOutcome;
use brenn_lib::mqtt::state::{ConnectorHealthLabel, IngressSubscription, MqttClientHandle};
use brenn_lib::mqtt::{InboundPayload, MqttEventRouter, MqttService, spawn_client_supervisor};
use rumqttc::{AsyncClient, MqttOptions, Transport};
use tempfile::TempDir;
use tokio::sync::mpsc;

// ---------------------------------------------------------------------------
// BrokerHarness: one mosquitto process per test
// ---------------------------------------------------------------------------

/// Owns the `mosquitto` child process and its temp directory.
///
/// Dropped via `Drop`: kills the process and waits for it to exit.
/// `prctl(PR_SET_PDEATHSIG, SIGTERM)` on spawn ensures the broker is also
/// killed if the test runner process dies abnormally (SIGKILL, Ctrl-C, panic).
pub struct BrokerHarness {
    pub port: u16,
    child: Option<Child>,
    /// Kept alive so broker log / config files outlive the process.
    _tempdir: TempDir,
    /// Absolute path to the mosquitto log file for failure diagnostics.
    #[allow(dead_code)]
    log_path: std::path::PathBuf,
}

impl BrokerHarness {
    /// Spawn `mosquitto` with the default TLS 1.2+ config and wait for it to become ready.
    ///
    /// # Panics
    ///
    /// - `mosquitto` binary not found.
    /// - TCP-connect readiness poll exceeds 2 seconds.
    pub fn start() -> Self {
        Self::start_with_conf_template("mosquitto.conf.tmpl")
    }

    /// Spawn `mosquitto` configured to accept TLS 1.3 connections only.
    ///
    /// Uses `mosquitto.conf.tls13.tmpl` which sets `tls_version tlsv1.3`.
    ///
    /// # Panics
    ///
    /// Same as [`start`].
    pub fn start_tls13() -> Self {
        Self::start_with_conf_template("mosquitto.conf.tls13.tmpl")
    }

    /// Spawn `mosquitto` configured to require username/password authentication
    /// (`allow_anonymous false` + a `password_file`). The checked-in `passwd`
    /// asset holds one user (`brenn-itest` / `brenn-itest-password`).
    ///
    /// # Panics
    ///
    /// Same as [`start`].
    pub fn start_auth() -> Self {
        Self::start_with_conf_template("mosquitto.conf.auth.tmpl")
    }

    fn start_with_conf_template(tmpl_name: &str) -> Self {
        let assets = certs::mqtt_assets_dir();

        // 2. Write static assets into a temp dir once; the config is rewritten on each attempt.
        let tempdir = TempDir::new().expect("failed to create tempdir for mosquitto");
        let tmp = tempdir.path();
        let log_path = tmp.join("mosquitto.log");

        // Copy static assets into tempdir. `passwd` is copied unconditionally
        // (harmless for the non-auth templates, which never reference it).
        for name in &["acl", "passwd"] {
            std::fs::copy(assets.join(name), tmp.join(name))
                .unwrap_or_else(|e| panic!("failed to copy {name} into tempdir: {e}"));
        }
        // TLS assets are generated per-run (no key material in the repo). Write
        // the shared CA + server cert/key so mosquitto's cafile/certfile/keyfile
        // resolve to real files.
        for (name, contents) in &[
            ("ca.pem", certs::ca_pem()),
            ("server.crt", certs::server_cert_pem()),
            ("server.key", certs::server_key_pem()),
        ] {
            std::fs::write(tmp.join(name), contents)
                .unwrap_or_else(|e| panic!("failed to write generated {name} into tempdir: {e}"));
        }
        // mosquitto warns on (and some versions reject) a world-readable password
        // file; chmod 0600 so the question is moot.
        {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(tmp.join("passwd"), std::fs::Permissions::from_mode(0o600))
                .expect("failed to chmod passwd to 0600");
        }

        let tmpl = std::fs::read_to_string(assets.join(tmpl_name))
            .unwrap_or_else(|e| panic!("failed to read {tmpl_name}: {e}"));

        // 3. Resolve mosquitto binary.
        let bin = std::env::var("BRENN_MOSQUITTO_BIN").unwrap_or_else(|_| "mosquitto".to_string());

        // Retry loop: pick a port, write config, spawn, poll readiness.
        // If mosquitto exits early due to a bind/address-in-use error (TOCTOU: the port
        // was taken between our bind-then-drop and mosquitto's own bind), retry with a
        // fresh ephemeral port. Any other early exit or timeout panics immediately
        // (fail-fast; do not mask real misconfigurations).
        const MAX_BIND_RETRIES: usize = 5;
        let mut last_log_tail = String::new();

        for attempt in 1..=MAX_BIND_RETRIES {
            // 1. Assign an ephemeral port by binding then dropping.
            // TOCTOU window: another process may claim this port before mosquitto binds it.
            // The retry loop above is the mitigation.
            let port = {
                let listener =
                    TcpListener::bind("127.0.0.1:0").expect("failed to bind ephemeral port");
                let p = listener.local_addr().expect("local_addr").port();
                drop(listener);
                p
            };

            // Substitute template with the chosen port.
            let config_str = tmpl
                .replace("__PORT__", &port.to_string())
                .replace("__CA_PEM__", &tmp.join("ca.pem").display().to_string())
                .replace(
                    "__SERVER_CRT__",
                    &tmp.join("server.crt").display().to_string(),
                )
                .replace(
                    "__SERVER_KEY__",
                    &tmp.join("server.key").display().to_string(),
                )
                .replace("__ACL__", &tmp.join("acl").display().to_string())
                .replace("__PASSWD__", &tmp.join("passwd").display().to_string())
                .replace("__LOG__", &log_path.display().to_string());
            let config_path = tmp.join("mosquitto.conf");
            std::fs::write(&config_path, &config_str).expect("failed to write mosquitto.conf");

            // 4. Spawn; set PR_SET_PDEATHSIG so the process is killed if the test runner dies.
            let mut cmd = Command::new(&bin);
            cmd.arg("-c")
                .arg(&config_path)
                .stdout(Stdio::null())
                .stderr(Stdio::null());

            // SAFETY: prctl is a simple syscall with no allocator interaction.
            // Linux-only (Brenn is Linux-only per design).
            unsafe {
                cmd.pre_exec(|| {
                    libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM, 0, 0, 0);
                    Ok(())
                });
            }

            let mut child = cmd.spawn().unwrap_or_else(|e| {
                panic!(
                    "mosquitto binary not found in PATH; install mosquitto or set \
                     BRENN_MOSQUITTO_BIN=/path/to/mosquitto. Tried: {bin:?}. Error: {e}"
                )
            });

            eprintln!("[BrokerHarness] mosquitto spawned on port {port} (attempt {attempt})");

            // 5. Poll TCP-connect readiness (every 25ms, cap 2s).
            // Also check whether mosquitto exited early.
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
            loop {
                if std::net::TcpStream::connect(format!("127.0.0.1:{port}")).is_ok() {
                    // Ready.
                    eprintln!("[BrokerHarness] mosquitto ready on port {port}");
                    return Self {
                        port,
                        child: Some(child),
                        _tempdir: tempdir,
                        log_path,
                    };
                }
                match child.try_wait() {
                    Ok(Some(status)) => {
                        let tail = read_tail_4k(&log_path);
                        // Distinguish TOCTOU bind failure from other failures.
                        // Mosquitto logs "Address already in use" or "Error: Unable to start"
                        // (with the bind error) when it cannot bind the configured port.
                        // Only retry on a clearly identified bind/address error; panic on
                        // anything else so real misconfigurations are not silently swallowed.
                        if tail.contains("Address already in use")
                            || (tail.contains("Error:") && tail.contains("Unable to start"))
                        {
                            eprintln!(
                                "[BrokerHarness] attempt {attempt}: mosquitto exited (bind \
                                 collision on port {port}); retrying with a new port. Log \
                                 tail:\n{tail}"
                            );
                            last_log_tail = tail;
                            break; // bind failure; retry
                        }
                        // Non-retryable: config error, cert problem, etc. Panic immediately.
                        panic!(
                            "mosquitto exited with status {status} before binding port {port} \
                             (attempt {attempt}; not a bind collision)\n\
                             Log ({}):\n{tail}",
                            log_path.display()
                        );
                    }
                    Ok(None) => {} // still running; fall through to deadline check
                    Err(e) => panic!(
                        "try_wait failed for mosquitto (attempt {attempt}, port {port}): {e}"
                    ),
                }
                if std::time::Instant::now() > deadline {
                    let tail = read_tail_4k(&log_path);
                    // Kill the timed-out child before panicking.
                    let _ = child.kill();
                    let _ = child.wait();
                    panic!(
                        "mosquitto readiness timeout on port {port} (attempt {attempt})\n\
                         Log ({}):\n{tail}",
                        log_path.display()
                    );
                }
                std::thread::sleep(std::time::Duration::from_millis(25));
            }

            // Only reached via `break` on bind failure (the ready path returns above).
        }

        // All retries exhausted due to repeated bind collisions.
        panic!(
            "BrokerHarness: failed to start mosquitto after {MAX_BIND_RETRIES} attempts; \
             repeated ephemeral-port collisions (TOCTOU). Last log tail:\n{last_log_tail}"
        );
    }

    /// Kill the broker and wait for exit. Idempotent.
    pub fn stop(&mut self) {
        if let Some(ref mut child) = self.child {
            // kill() fails with ESRCH if the process already exited — benign.
            // wait() failure is also benign in teardown; we just need the zombie reaped.
            let _ = child.kill();
            let _ = child.wait();
        }
        self.child = None;
        eprintln!("[BrokerHarness] mosquitto stopped");
    }

    /// Loopback host the broker binds to.
    pub const HOST: &'static str = "127.0.0.1";
}

impl Drop for BrokerHarness {
    fn drop(&mut self) {
        self.stop();
    }
}

fn read_tail_4k(path: &std::path::Path) -> String {
    let mut file = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(_) => return "<log file not found>".to_string(),
    };
    let len = file.metadata().map(|m| m.len()).unwrap_or(0);
    if len > 4096 {
        use std::io::Seek;
        // seek failure means we read from the beginning — still useful, just not the tail.
        let _ = file.seek(std::io::SeekFrom::End(-4096));
    }
    let mut buf = String::new();
    let _ = file.read_to_string(&mut buf); // best-effort; empty on I/O error
    buf
}

// ---------------------------------------------------------------------------
// spawn_client helper: the unified per-client session harness
// ---------------------------------------------------------------------------

/// Handle set returned by the unified spawn helpers: the service, the client
/// slug, the session handle (for `publish_on_handle` / stop / subscription-set
/// asserts), and the receiver every inbound delivery lands on.
pub struct SpawnedClient {
    pub svc: Arc<MqttService>,
    pub client_slug: String,
    pub handle: Arc<MqttClientHandle>,
    pub rx: mpsc::UnboundedReceiver<DeliveredMessage>,
}

/// Build `MqttService` + `MqttClientHandle` + the unified supervisor for
/// `test_name`, wiring a `CapturingRouter` for inbound deliveries. `static_subs`
/// are the client's union subscription set (assigned 1-based `sub_id`s in order),
/// re-asserted by the supervisor on connect.
///
/// Returns only after the session reaches `Connected` (poll every 25ms, cap 5s).
/// Connected ≠ subscriptions live — callers confirm liveness via
/// [`subscribe_live_confirmed`]'s retained barrier (or, for static subs, by
/// draining the retained payload the initial re-assert loop delivers).
pub async fn spawn_client(
    test_name: &str,
    broker: &BrokerHarness,
    ca_pem: Vec<u8>,
    static_subs: Vec<(String, u8)>,
) -> SpawnedClient {
    spawn_client_with_tls_version(test_name, broker, ca_pem, static_subs, TlsVersionMin::Tls12)
        .await
}

/// Like [`spawn_client`] but negotiates TLS 1.3 only (`tls_version_min = Tls13`).
pub async fn spawn_client_tls13(
    test_name: &str,
    broker: &BrokerHarness,
    ca_pem: Vec<u8>,
    static_subs: Vec<(String, u8)>,
) -> SpawnedClient {
    spawn_client_with_tls_version(test_name, broker, ca_pem, static_subs, TlsVersionMin::Tls13)
        .await
}

/// The `MqttClientConfig` the harness spawner uses, returned as a bare struct so
/// the caller can mutate the fields a given test needs to vary (bad credentials,
/// etc.) before wrapping it in `Arc`. `port` is whatever the client should dial —
/// the broker directly, or a [`TcpRelay`] port in front of it.
pub fn test_client_config(
    client_slug: &str,
    port: u16,
    ca_pem: Vec<u8>,
    tls_version_min: TlsVersionMin,
) -> MqttClientConfig {
    MqttClientConfig {
        slug: client_slug.to_string(),
        host: BrokerHarness::HOST.to_string(),
        port,
        username: None,
        password: None,
        ca_cert_pem: Some(ca_pem),
        tls_version_min,
        keepalive_secs: Some(30),
        inbound_payload_cap_bytes: 4 * 1024 * 1024,
        last_will: None,
        reconnect_backoff_initial_secs: 1,
        reconnect_backoff_max_secs: 60,
        qos: 1,
        urgency: Urgency::Normal,
        session_expiry_secs: 0,
    }
}

/// Poll `probe` at 25ms intervals until it reports `Connected`, capping at 5s and
/// panicking with `msg` on timeout. Shared by both harness spawners.
async fn wait_until_connected<F, Fut>(mut probe: F, msg: &str)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = ConnectorHealthLabel>,
{
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        if probe().await == ConnectorHealthLabel::Connected {
            return;
        }
        assert!(std::time::Instant::now() < deadline, "{msg}");
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
}

/// Poll `eventloop` until an incoming packet satisfies `wanted`, discarding every
/// other event. Panics on eventloop error or after 5s; `what` names the awaited
/// packet for diagnostics. Shared by the raw-rumqttc helpers.
async fn drain_until_incoming<F>(eventloop: &mut rumqttc::EventLoop, mut wanted: F, what: &str)
where
    F: FnMut(&rumqttc::Incoming) -> bool,
{
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        match tokio::time::timeout_at(deadline, eventloop.poll()).await {
            Ok(Ok(rumqttc::Event::Incoming(pkt))) if wanted(&pkt) => return,
            Ok(Ok(_)) => continue,
            Ok(Err(e)) => panic!("{what}: eventloop error before {what}: {e}"),
            Err(_) => panic!("{what}: not received within 5s"),
        }
    }
}

/// Await one delivery on `rx` (3s cap). Panics on a closed channel or timeout,
/// naming `what`. The strict 3s cap is the ingress suite's shared convention.
pub async fn recv_delivery(
    rx: &mut mpsc::UnboundedReceiver<DeliveredMessage>,
    what: &str,
) -> DeliveredMessage {
    match tokio::time::timeout(std::time::Duration::from_secs(3), rx.recv()).await {
        Ok(Some(msg)) => msg,
        Ok(None) => panic!("{what}: router receiver closed"),
        Err(_) => panic!("{what}: not delivered within 3s"),
    }
}

/// Await one PubAck on `ack_rx` (3s cap). Panics on a closed channel or timeout,
/// naming `what`. Callers await one ack per QoS-1 publish to confirm the broker
/// processed it.
pub async fn await_puback(ack_rx: &mut mpsc::UnboundedReceiver<()>, what: &str) {
    match tokio::time::timeout(std::time::Duration::from_secs(3), ack_rx.recv()).await {
        Ok(Some(())) => {}
        Ok(None) => panic!("{what}: PubAck channel closed"),
        Err(_) => panic!("{what}: PubAck not received within 3s"),
    }
}

async fn spawn_client_with_tls_version(
    test_name: &str,
    broker: &BrokerHarness,
    ca_pem: Vec<u8>,
    static_subs: Vec<(String, u8)>,
    tls_version_min: TlsVersionMin,
) -> SpawnedClient {
    let client_slug = format!("testbroker-{test_name}");
    let config = Arc::new(test_client_config(
        &client_slug,
        broker.port,
        ca_pem,
        tls_version_min,
    ));
    let spawned = spawn_client_with_config(config, static_subs).await;

    wait_until_connected(
        || async { spawned.svc.ingress_health(&spawned.client_slug).await.0 },
        "spawn_client: session never reached Connected within 5s",
    )
    .await;

    spawned
}

/// Build `MqttService` + `MqttClientHandle` + the unified supervisor for a
/// caller-supplied `config`, wiring a `CapturingRouter`. Unlike [`spawn_client`],
/// this does **not** wait for `Connected` — tests that expect a session to *never*
/// connect (bad credentials) or that drive their own connection lifecycle call
/// this directly and use [`wait_for_health`] for whatever state they expect. The
/// client slug is taken from `config.slug`.
pub async fn spawn_client_with_config(
    config: Arc<MqttClientConfig>,
    static_subs: Vec<(String, u8)>,
) -> SpawnedClient {
    let client_slug = config.slug.clone();

    let subs: Vec<IngressSubscription> = static_subs
        .into_iter()
        .enumerate()
        .map(|(i, (topic_filter, qos))| IngressSubscription {
            topic_filter,
            qos,
            sub_id: (i + 1) as u32,
        })
        .collect();

    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let handle = MqttClientHandle::new(config, subs, stop_tx);

    let svc = MqttService::new();
    svc.add_client(handle.clone()).await;

    let (router, rx) = CapturingRouter::new();
    let router_arc: Arc<dyn MqttEventRouter> = Arc::new(router);
    svc.set_router(router_arc.clone()).await;

    spawn_client_supervisor(handle.clone(), router_arc, stop_rx);

    SpawnedClient {
        svc,
        client_slug,
        handle,
        rx,
    }
}

// ---------------------------------------------------------------------------
// wait_for_health: poll until any of the accepted labels is reached
// ---------------------------------------------------------------------------

/// Poll `svc.ingress_health(client_slug)` at 25ms intervals until the label is in
/// `accepted`, then return that label. Panics with `msg` if no accepted label is
/// seen within `timeout_secs`.
pub async fn wait_for_health(
    svc: &brenn_lib::mqtt::MqttService,
    client_slug: &str,
    accepted: &[ConnectorHealthLabel],
    timeout_secs: u64,
    msg: &str,
) -> ConnectorHealthLabel {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(timeout_secs);
    loop {
        let (label, _) = svc.ingress_health(client_slug).await;
        if accepted.contains(&label) {
            return label;
        }
        assert!(std::time::Instant::now() < deadline, "{msg}");
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
}

fn uuid_v4_simple() -> String {
    // simple() produces the 32-hex-digit dashless form without an intermediate allocation.
    uuid::Uuid::new_v4().simple().to_string()
}

// ---------------------------------------------------------------------------
// direct_subscriber: a raw rumqttc client that witnesses the first delivery
// ---------------------------------------------------------------------------

/// Subscribe a direct rumqttc v5 client to `topic` and return a receiver that
/// yields the payload of the first `Publish` delivered on it.
///
/// The subscription is registered (SubAck received) before this function
/// returns, so a publish issued by the caller afterward cannot race ahead of it.
/// Use this to witness delivery independently of the brenn session, on a separate
/// client id.
///
/// If the eventloop errors before a `Publish` arrives (e.g. broker/TLS drop),
/// the error is printed to stderr — captured and shown by the test harness on
/// failure — so a delivery-timeout can be told apart from a transport fault
/// rather than surfacing only as a bare `recv` timeout.
///
/// # TEST-ONLY TLS note
/// Uses `Transport::tls(ca_pem, None, None)` with an IP-literal host. Acceptable
/// for loopback test brokers; MUST NOT be copied into production connection code.
pub async fn direct_subscriber(
    broker_port: u16,
    ca_pem: Vec<u8>,
    topic: &str,
) -> mpsc::Receiver<Vec<u8>> {
    let client_id = format!("brenn-direct-sub-{}", uuid_v4_simple());
    let mut opts = MqttOptions::new(client_id, ("127.0.0.1", broker_port));
    opts.set_clean_start(true);
    opts.set_transport(Transport::tls(ca_pem, None, None));
    let (client, mut eventloop) = AsyncClient::builder(opts).capacity(16).build();

    client
        .subscribe(topic, rumqttc::mqttbytes::QoS::AtLeastOnce)
        .await
        .expect("direct_subscriber: subscribe failed");

    // Drain the eventloop until SubAck so the subscription is registered before
    // the caller publishes, then hand off to a task collecting the first Publish.
    drain_until_incoming(
        &mut eventloop,
        |pkt| matches!(pkt, rumqttc::Incoming::SubAck(_)),
        "direct_subscriber: SubAck",
    )
    .await;

    let (deliver_tx, deliver_rx) = mpsc::channel::<Vec<u8>>(1);
    tokio::spawn(async move {
        // Keep the client alive so the eventloop keeps servicing the connection.
        let _client = client;
        loop {
            match eventloop.poll().await {
                Ok(rumqttc::Event::Incoming(rumqttc::Incoming::Publish(p))) => {
                    deliver_tx.send(p.payload.to_vec()).await.ok();
                    return; // one publish is all we need
                }
                Ok(_) => {}
                Err(e) => {
                    eprintln!("direct_subscriber: eventloop error before Publish: {e}");
                    return;
                }
            }
        }
    });

    deliver_rx
}

// ---------------------------------------------------------------------------
// direct_publisher_acked: a raw rumqttc publisher that surfaces PubAcks
// ---------------------------------------------------------------------------

/// Connect a direct rumqttc v5 client and return it plus a channel that receives
/// one `()` per PubAck the eventloop observes. Await one ack per QoS-1 publish to
/// confirm the broker has processed it — the retained-barrier handshake needs
/// "the broker stored this retained publish" as a hard fact, not a sleep.
///
/// The returned client is connected (ConnAck received) before this returns. The
/// background task forwards PubAcks (unbounded, so unread acks never block the
/// drain) and otherwise keeps draining until the eventloop errors at teardown.
///
/// # TEST-ONLY TLS note
/// Uses `Transport::tls(ca_pem, None, None)` with an IP-literal host — acceptable
/// for loopback test brokers; MUST NOT be copied into production connection code.
pub async fn direct_publisher_acked(
    broker_port: u16,
    ca_pem: Vec<u8>,
) -> (AsyncClient, mpsc::UnboundedReceiver<()>) {
    let client_id = format!("brenn-direct-acked-{}", uuid_v4_simple());
    let mut opts = MqttOptions::new(client_id, ("127.0.0.1", broker_port));
    opts.set_clean_start(true);
    opts.set_transport(Transport::tls(ca_pem, None, None));
    let (client, mut eventloop) = AsyncClient::builder(opts).capacity(64).build();

    drain_until_incoming(
        &mut eventloop,
        |pkt| matches!(pkt, rumqttc::Incoming::ConnAck(_)),
        "direct_publisher_acked: ConnAck",
    )
    .await;

    let (ack_tx, ack_rx) = mpsc::unbounded_channel::<()>();
    tokio::spawn(async move {
        loop {
            match eventloop.poll().await {
                Ok(rumqttc::Event::Incoming(rumqttc::Incoming::PubAck(_))) => {
                    // Receiver-dropped at teardown is benign; ignore the send result.
                    ack_tx.send(()).ok();
                }
                Ok(_) => {}
                // A mid-test transport drop stops PubAcks; surface it so a downstream
                // barrier timeout can be told apart from a code regression, matching
                // direct_subscriber's convention.
                Err(e) => {
                    eprintln!("direct_publisher_acked: eventloop error: {e}");
                    return;
                }
            }
        }
    });

    (client, ack_rx)
}

// ---------------------------------------------------------------------------
// subscribe_live_confirmed: the retained barrier
// ---------------------------------------------------------------------------

/// Subscribe `client_slug` to `topic` and return only once the subscription is
/// proven live at the broker.
///
/// `subscribe_filter` returns as soon as the SUBSCRIBE is *queued* (no SUBACK
/// wait), so a publish issued right after could beat the SUBSCRIBE to the broker.
/// This closes the race with a retained barrier: publish a retained sentinel on
/// `topic` (QoS 1, PubAck-confirmed so it is stored before the SUBSCRIBE), then
/// subscribe. `OnEverySubscribe` makes the broker redeliver the retained sentinel
/// when it processes the SUBSCRIBE; its arrival at the router proves the
/// subscription is live and SUBACKed.
///
/// Drains `rx` until the barrier is delivered on `topic` (cap 3s). Any other
/// delivery panics — there is no legitimate source of noise in these tests.
/// Returns the `IngressSubscribeOutcome` for the caller to assert.
pub async fn subscribe_live_confirmed(
    svc: &MqttService,
    client_slug: &str,
    topic: &str,
    pub_client: &AsyncClient,
    ack_rx: &mut mpsc::UnboundedReceiver<()>,
    rx: &mut mpsc::UnboundedReceiver<DeliveredMessage>,
) -> IngressSubscribeOutcome {
    let barrier = format!("__barrier__{}_{topic}", uuid_v4_simple());

    // The barrier handshake relies on the next PubAck being *this* barrier's. A stale
    // ack left by an earlier un-awaited QoS-1 publish would let the SUBSCRIBE proceed
    // before the retained message is stored, silently reopening the race. Fail loudly
    // if the caller left the ack channel dirty.
    assert!(
        matches!(ack_rx.try_recv(), Err(mpsc::error::TryRecvError::Empty)),
        "subscribe_live_confirmed: PubAck channel not drained — await one ack per prior \
         QoS-1 publish before calling this helper"
    );

    // Retained barrier publish, PubAck-confirmed: stored before the SUBSCRIBE.
    pub_client
        .publish(
            topic.to_string(),
            rumqttc::mqttbytes::QoS::AtLeastOnce,
            true,
            barrier.as_bytes().to_vec(),
        )
        .await
        .expect("subscribe_live_confirmed: barrier publish failed");
    await_puback(ack_rx, "subscribe_live_confirmed: barrier").await;

    let outcome = svc
        .subscribe_filter(client_slug, topic.to_string(), 1)
        .await
        .expect("subscribe_live_confirmed: no ingress supervisor for client");

    // The barrier's redelivery via OnEverySubscribe proves the SUBSCRIBE is live.
    // It is the first (and only expected) delivery on `topic` — any other delivery
    // is noise, which has no legitimate source in these tests.
    let msg = recv_delivery(
        rx,
        &format!("subscribe_live_confirmed: barrier for {topic:?}"),
    )
    .await;
    assert!(
        msg.topic == topic
            && msg.client == client_slug
            && matches!(&msg.payload, InboundPayload::Text(t) if *t == barrier),
        "subscribe_live_confirmed: expected the barrier on {topic:?} for client \
         {client_slug:?}, got a delivery on {:?} for {:?}: {:?}",
        msg.topic,
        msg.client,
        msg.payload
    );

    outcome
}
