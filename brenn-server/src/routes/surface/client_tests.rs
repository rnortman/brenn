//! Native integration tests: the real `brenn-surface-kernel` (native connector,
//! constructed with the test session cookie) driven against the real
//! router/session from `spawn_test_server`. Where `ws_tests.rs` drives the wire
//! by hand, these tests exercise the client crate end-to-end — its driver, core,
//! subscription table, and activation delivery — against a live backend, so the
//! two halves of the surface protocol are proven to agree.

use std::sync::Arc;
use std::time::Duration;

use crate::test_support::TEST_BUILD_ID;
use brenn_lib::db;
use brenn_lib::messaging::config::ResolvedSurface;
use brenn_lib::messaging::testutils::ephemeral_channel_entry;
use brenn_surface_kernel::contract::Activation;
use brenn_surface_kernel::proto::{LogLevel, MAX_LOG_MESSAGE_BYTES};
use brenn_surface_kernel::{
    ActivationEntry, ClientConfig, DisconnectReason, Event, NativeConnector, PublishBuffer,
    PublishReject, PublishStatus, new,
};
use futures::StreamExt;
use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};

use super::test_fixtures::{
    COMPONENT, EPH_ADDR, EPH_NAME, PORT, SurfaceTestHarness, TEST_MAX_BODY_BYTES, assert_no_alerts,
    publish, publish_as, publish_policy, subscribe_harness, subscribe_policy, surface_harness,
};
use crate::test_support::http::{
    http_base_addr, http_to_ws_url, setup_authenticated_user, spawn_test_server,
};
use crate::test_support::surface::SurfaceFixture;

/// A second ephemeral subscription channel, bound only on the two-channel
/// `deskbar` fixture. The kiosk scenario drops this binding on restart, so this
/// is the channel the instance stops being activated on after the reconnect.
const EPH_ADDR_B: &str = "ephemeral:protobar-extra";
const EPH_NAME_B: &str = "protobar-extra";
/// The port `EPH_ADDR_B` binds on `protobar`.
const PORT_B: &str = "extra";

/// How long a test waits for a client event or port message before failing —
/// generous, since the client drives a real socket and a real backend, but
/// bounded so a stuck path fails loudly instead of hanging.
const WAIT: Duration = Duration::from_secs(5);

/// Reconnect tests configure the client's initial backoff down to this so a
/// severed transport reconnects promptly instead of paying the production 3 s
/// default. The sever→publish ordering stays deterministic regardless: the
/// in-process publish is synchronous and completes before the reconnect (which
/// costs at least backoff + TCP + handshake).
const FAST_RECONNECT_BACKOFF: Duration = Duration::from_millis(50);

/// An activation recorder: the entry an instance registers so a test can observe
/// what the kernel delivers. Every activation the kernel invokes it with is
/// cloned onto an unbounded channel the test drains; the entry always returns
/// `Ok` (it publishes nothing, so the flush is empty). Delivery is one windowed
/// activation per instance, so a test observes windows, not individual messages.
fn recorder() -> (ActivationEntry, UnboundedReceiver<Activation>) {
    let (tx, rx) = unbounded_channel();
    let entry: ActivationEntry =
        Box::new(move |activation: &Activation, _buf: &mut PublishBuffer| {
            // A send failure means the test dropped its receiver mid-teardown; the
            // delivery is simply unobserved, which is not a protocol concern.
            let _ = tx.send(activation.clone());
            Ok(())
        });
    (entry, rx)
}

/// Await the next activation the recorder captured, failing on timeout.
async fn next_activation(rx: &mut UnboundedReceiver<Activation>) -> Activation {
    tokio::time::timeout(WAIT, rx.recv())
        .await
        .expect("timed out waiting for an activation")
        .expect("the activation recorder channel closed")
}

/// The new-half bodies delivered to `port` in one activation (empty for a
/// pure-context window, or a window this activation did not carry).
fn new_bodies(activation: &Activation, port: &str) -> Vec<String> {
    activation
        .ports
        .iter()
        .find(|w| w.port == port)
        .map(|w| {
            w.envelopes[w.new_from as usize..]
                .iter()
                .map(|e| e.body.clone())
                .collect()
        })
        .unwrap_or_default()
}

/// Drain activations until `port` has been delivered exactly `expected` new
/// messages, in order, asserting the bodies match. Batching means the messages
/// may arrive as one activation's multi-envelope window or across several
/// single-envelope activations; either is conformant, so this accumulates
/// across activations rather than pinning a batch boundary.
async fn expect_new_messages(
    rx: &mut UnboundedReceiver<Activation>,
    port: &str,
    expected: &[&str],
) {
    let mut got: Vec<String> = Vec::new();
    while got.len() < expected.len() {
        let activation = next_activation(rx).await;
        got.extend(new_bodies(&activation, port));
    }
    let got_refs: Vec<&str> = got.iter().map(String::as_str).collect();
    assert_eq!(got_refs, expected, "unexpected new messages on port {port}");
}

/// Drain activations until every `(port, body)` in `wanted` has been observed as
/// a new message, regardless of how the kernel batched them — the pairs may all
/// land in one activation's several windows or be spread across activations.
/// Used where an instance binds multiple channels and each must deliver, without
/// pinning which activation carries which.
async fn expect_ports_each_get(rx: &mut UnboundedReceiver<Activation>, wanted: &[(&str, &str)]) {
    let mut remaining: Vec<(&str, &str)> = wanted.to_vec();
    while !remaining.is_empty() {
        let activation = next_activation(rx).await;
        remaining.retain(|(port, body)| !new_bodies(&activation, port).iter().any(|b| b == body));
    }
}

/// Await a single new message on `port` and assert its body.
async fn expect_ephemeral_message(rx: &mut UnboundedReceiver<Activation>, expected_body: &str) {
    expect_new_messages(rx, PORT, &[expected_body]).await;
}

/// Assert that no further activation carrying a *new* message for `port` arrives
/// within a short window — the claim ("no duplicate", "gapless replay") that
/// nothing trails the expected tail. A pure-context activation is not a
/// violation of that claim and is ignored; a new message is.
async fn assert_no_further_message(rx: &mut UnboundedReceiver<Activation>, port: &str) {
    let deadline = tokio::time::sleep(Duration::from_millis(200));
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            _ = &mut deadline => return,
            activation = rx.recv() => {
                let activation = activation.expect("the activation recorder channel closed");
                let new = new_bodies(&activation, port);
                assert!(
                    new.is_empty(),
                    "expected no further message on port {port}, got {new:?}"
                );
            }
        }
    }
}

/// A `deskbar` surface binding TWO ephemeral subscription channels — `EPH_ADDR`
/// → `(protobar, messages)` and `EPH_ADDR_B` → `(protobar, extra)`. The kiosk
/// test connects here first, then restarts against the one-channel `deskbar_sub`
/// to drop the `EPH_ADDR_B` binding.
fn deskbar_two_sub() -> ResolvedSurface {
    SurfaceFixture::new("deskbar", COMPONENT)
        .subscribe(EPH_ADDR, COMPONENT, PORT)
        .subscribe(EPH_ADDR_B, COMPONENT, PORT_B)
        .policy(subscribe_policy(&[EPH_NAME, EPH_NAME_B]))
        .build()
}

/// Like `subscribe_harness`, but the `deskbar` surface binds both ephemeral
/// channels (`EPH_ADDR` and `EPH_ADDR_B`) over a bus carrying both, so the test
/// can seed each channel's retain ring in-process.
fn subscribe_state_two(db: &db::Db, retain_depth: u64, capacity: u32) -> SurfaceTestHarness {
    surface_harness(
        db,
        deskbar_two_sub(),
        vec![
            ephemeral_channel_entry(EPH_NAME, retain_depth, capacity),
            ephemeral_channel_entry(EPH_NAME_B, retain_depth, capacity),
        ],
    )
}

/// Build a native client against `base` for the `deskbar` surface, spawn its
/// driver, and return the handle, event stream, and the driver's `JoinHandle`.
/// The client appends `?build` itself, so the configured URL is bare.
///
/// The `JoinHandle` is returned so a test can await the driver's teardown after
/// `close()` and surface a driver-side panic as its own message, rather than as
/// the opaque event-stream timeout it would otherwise become.
fn spawn_client(
    base: &str,
    token: &str,
) -> (
    brenn_surface_kernel::ClientHandle,
    brenn_surface_kernel::EventStream,
    tokio::task::JoinHandle<()>,
) {
    spawn_client_cfg(
        base,
        token,
        TEST_BUILD_ID,
        ClientConfig::default().initial_backoff,
    )
}

/// Like `spawn_client`, but with a small initial backoff so a severed transport
/// reconnects promptly — used by the reconnect/kiosk tests.
fn spawn_client_reconnect(
    base: &str,
    token: &str,
) -> (
    brenn_surface_kernel::ClientHandle,
    brenn_surface_kernel::EventStream,
    tokio::task::JoinHandle<()>,
) {
    spawn_client_cfg(base, token, TEST_BUILD_ID, FAST_RECONNECT_BACKOFF)
}

/// Like `spawn_client`, but with a caller-chosen `build_id` — used by the
/// stale-build test to present a build the server rejects. Uses the small
/// reconnect backoff so a (buggy) reconnect attempt would fire within the
/// test's post-terminal settle window rather than being masked by the 3 s
/// default.
fn spawn_client_with_build(
    base: &str,
    token: &str,
    build_id: &str,
) -> (
    brenn_surface_kernel::ClientHandle,
    brenn_surface_kernel::EventStream,
    tokio::task::JoinHandle<()>,
) {
    spawn_client_cfg(base, token, build_id, FAST_RECONNECT_BACKOFF)
}

/// Shared client spawn: build the config against `base`, spawn the driver, and
/// return the handle, event stream, and the driver's `JoinHandle`.
fn spawn_client_cfg(
    base: &str,
    token: &str,
    build_id: &str,
    initial_backoff: Duration,
) -> (
    brenn_surface_kernel::ClientHandle,
    brenn_surface_kernel::EventStream,
    tokio::task::JoinHandle<()>,
) {
    let url = http_to_ws_url(base, "/surface/deskbar/ws");
    let config = ClientConfig {
        url,
        build_id: build_id.to_string(),
        initial_backoff,
        ..ClientConfig::default()
    };
    let (handle, events, driver) = new(config, NativeConnector::new(token.to_string()));
    let driver_task = tokio::spawn(driver.run());
    (handle, events, driver_task)
}

/// Await the next event on the stream, failing on timeout or a closed stream.
async fn next_event(events: &mut brenn_surface_kernel::EventStream) -> Event {
    tokio::time::timeout(WAIT, events.next())
        .await
        .expect("timed out waiting for a client event")
        .expect("event stream ended before an event arrived")
}

/// Await the next event and assert it is `Disconnected { TransportClosed }` — a
/// live transport dropping under the client, as a `relay.sever()` produces. The
/// sever surfaces here before the reconnect's `Connected`.
async fn expect_disconnected(events: &mut brenn_surface_kernel::EventStream) {
    match next_event(events).await {
        Event::Disconnected {
            reason: DisconnectReason::TransportClosed,
        } => {}
        other => panic!("expected Disconnected on sever, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Happy path
// ---------------------------------------------------------------------------

#[tokio::test]
async fn client_connect_attach_receives_published_message() {
    let db = db::init_db_memory();
    let SurfaceTestHarness {
        state,
        alerts,
        flusher,
        bus,
    } = subscribe_harness(&db, 4, 16);
    let (token, _) = setup_authenticated_user(&db).await;
    // Publish before the client connects: the retained ring (depth 4) holds it,
    // so the client's fresh Subscribe replays it deterministically — no
    // subscribe/publish race.
    publish(&bus, "hello");
    let (base, _sd) = spawn_test_server(state).await;

    let (client, mut events, driver_task) = spawn_client(&base, &token);

    // The handshake completes: the first event is `Connected` with the expected
    // resolved bindings and identity.
    match next_event(&mut events).await {
        Event::Connected {
            bindings,
            participant_id,
            max_body_bytes,
            alert_granted,
            takeover_granted: _,
            error_report_floor: _,
            surface_description: _,
        } => {
            assert_eq!(participant_id, "surface:deskbar");
            assert_eq!(max_body_bytes, TEST_MAX_BODY_BYTES as u64);
            assert!(!alert_granted, "default deskbar fixture has no alert grant");
            assert_eq!(bindings.components.len(), 1);
            assert_eq!(bindings.components[0].instance, COMPONENT);
            assert_eq!(bindings.components[0].kind, COMPONENT);
            assert_eq!(bindings.subscriptions.len(), 1);
            assert_eq!(bindings.subscriptions[0].channel, EPH_ADDR);
            assert_eq!(bindings.subscriptions[0].instance, COMPONENT);
            assert_eq!(bindings.subscriptions[0].port, PORT);
        }
        other => panic!("expected Connected, got {other:?}"),
    }

    // Register the instance: registration opens its subscriptions, so the client
    // sends a fresh Subscribe, the server replays the retained ring, and the
    // instance is activated with the message new in its window.
    let (entry, mut activations) = recorder();
    client.register_activation(COMPONENT, entry);
    expect_ephemeral_message(&mut activations, "hello").await;

    client.close();
    // The kernel drops the handle after an orderly close; that drops every
    // command sender, so the driver's terminal drain loop winds it down.
    drop(client);

    // Await the driver's teardown so a driver-side panic surfaces as its own
    // message, not as a downstream timeout in some later test.
    tokio::time::timeout(WAIT, driver_task)
        .await
        .expect("the driver task ends after close")
        .expect("the driver task did not panic");

    // A fully-conformant client session structurally cannot violate the protocol,
    // so it must trip zero fail2ban-feeding security events — the property this
    // whole design exists to guarantee.
    assert_no_alerts(&flusher, &alerts, "conformant happy-path session").await;
}

// ---------------------------------------------------------------------------
// Retained-ring replay on fresh attach
// ---------------------------------------------------------------------------

/// Several messages published into the retain ring before the client connects
/// all replay, in seq order, to a freshly attached port. The happy path proves
/// one-message replay; this proves the full `Replay::Fresh` ring: a fresh
/// `Subscribe { resume: None }` receives the whole retained ring oldest-first.
#[tokio::test]
async fn client_fresh_attach_replays_full_retained_ring() {
    let db = db::init_db_memory();
    // Retain depth 4 holds all three pre-published messages.
    let SurfaceTestHarness {
        state,
        alerts,
        flusher,
        bus,
    } = subscribe_harness(&db, 4, 16);
    let (token, _) = setup_authenticated_user(&db).await;

    let bodies = ["one", "two", "three"];
    for body in bodies {
        publish(&bus, body);
    }
    let (base, _sd) = spawn_test_server(state).await;

    let (client, mut events, driver_task) = spawn_client(&base, &token);

    match next_event(&mut events).await {
        Event::Connected { .. } => {}
        other => panic!("expected Connected, got {other:?}"),
    }

    // Register: the fresh Subscribe replays the whole retained ring in order.
    // The replay reaches the instance as activation windows — one multi-envelope
    // window or a run of single-envelope ones, in seq order either way.
    let (entry, mut activations) = recorder();
    client.register_activation(COMPONENT, entry);
    expect_new_messages(&mut activations, PORT, &bodies).await;

    // Nothing trails the retained ring — a duplicate replay would fail here.
    assert_no_further_message(&mut activations, PORT).await;

    client.close();
    // The kernel drops the handle after an orderly close; that drops every
    // command sender, so the driver's terminal drain loop winds it down.
    drop(client);

    tokio::time::timeout(WAIT, driver_task)
        .await
        .expect("the driver task ends after close")
        .expect("the driver task did not panic");

    assert_no_alerts(&flusher, &alerts, "conformant full-ring replay").await;
}

// ---------------------------------------------------------------------------
// Latest-value round-trip on one connection
// ---------------------------------------------------------------------------

/// On a `retain_depth = 1` (latest-value / state) channel, an in-connection
/// detach + re-attach re-reads the newest retained value via a fresh
/// `Subscribe { resume: None }`: the last port detaching sends
/// `Unsubscribe` and discards the channel's resume token, so the re-attach is a
/// fresh consumer that receives the retained ring rather than resuming past it.
/// Proves the no-indefinite-staleness property the refcount-scoped token
/// lifetime exists to guarantee — no violation, no gap.
#[tokio::test]
async fn client_reattach_replays_latest_retained_value() {
    let db = db::init_db_memory();
    // Retain depth 1: the ring holds exactly the newest value, so a fresh
    // Subscribe replays only the latest — the state-channel shape.
    let SurfaceTestHarness {
        state,
        alerts,
        flusher,
        bus,
    } = subscribe_harness(&db, 1, 16);
    let (token, _) = setup_authenticated_user(&db).await;

    publish(&bus, "v1");
    let (base, _sd) = spawn_test_server(state).await;

    let (client, mut events, driver_task) = spawn_client(&base, &token);

    match next_event(&mut events).await {
        Event::Connected { .. } => {}
        other => panic!("expected Connected, got {other:?}"),
    }

    // First registration: fresh Subscribe replays the retained value "v1".
    let (entry, mut activations) = recorder();
    client.register_activation(COMPONENT, entry);
    expect_ephemeral_message(&mut activations, "v1").await;

    // Deregister the instance: the client sends `Unsubscribe` and discards the
    // channel's resume token (its last reference is gone).
    client.deregister_activation(COMPONENT);

    // Publish a newer value server-side. Publishing into the in-process bus is
    // synchronous, so "v2" is in the (depth-1) ring — displacing "v1" — before
    // the re-registration's Subscribe frame is ever sent.
    publish(&bus, "v2");

    // Re-register: a fresh consumer, so the client sends `Subscribe { resume: None }`
    // (not a resume past the old high-water). The server replays the retained
    // ring, whose sole entry is now "v2", and the instance is activated with it.
    // The retained newest value reaches the re-registered instance immediately —
    // no gap, no violation.
    let (entry2, mut activations2) = recorder();
    client.register_activation(COMPONENT, entry2);
    expect_ephemeral_message(&mut activations2, "v2").await;

    // Third cycle, with NO intervening publish — the case the second cycle
    // cannot discriminate. Deregister again: a conformant client discards the
    // channel's resume token. Re-register: a fresh `Subscribe { resume: None }`
    // replays the retained newest ("v2") again. If the reference-0 deregister
    // wrongly RETAINED the token, the re-subscribe would resume `UpToDate` and
    // replay nothing, so the instance would never activate — the exact
    // indefinite-staleness regression the reference-scoped token lifetime exists
    // to prevent.
    client.deregister_activation(COMPONENT);
    let (entry3, mut activations3) = recorder();
    client.register_activation(COMPONENT, entry3);
    expect_ephemeral_message(&mut activations3, "v2").await;

    client.close();
    // The kernel drops the handle after an orderly close; that drops every
    // command sender, so the driver's terminal drain loop winds it down.
    drop(client);

    tokio::time::timeout(WAIT, driver_task)
        .await
        .expect("the driver task ends after close")
        .expect("the driver task did not panic");

    assert_no_alerts(&flusher, &alerts, "conformant latest-value round-trip").await;
}

// ---------------------------------------------------------------------------
// Stale build
// ---------------------------------------------------------------------------

/// A client presenting a build id the server does not recognise is closed with
/// `STALE_BUILD_CLOSE_CODE` (3001, reason = the server's build id). The client
/// surfaces `Event::ReloadRequired { server_build }` and does not reconnect —
/// its driver run loop ends (the client never reloads itself; the shell
/// bootstrap owns the capped reload). A stale first-party tab is not fail2ban
/// signal, so this trips zero security events.
#[tokio::test]
async fn client_stale_build_reloads_and_does_not_reconnect() {
    let db = db::init_db_memory();
    let SurfaceTestHarness {
        state,
        alerts,
        flusher,
        ..
    } = subscribe_harness(&db, 4, 16);
    let (token, _) = setup_authenticated_user(&db).await;
    // Front the backend with the relay so its accept counter can prove the client
    // makes exactly one connection — no reconnect after the terminal close.
    let (backend_base, _sd) = spawn_test_server(state).await;
    let relay = Relay::start_for(&backend_base).await;

    // A build id the server will reject (it only accepts its own `TEST_BUILD_ID`).
    let stale_build = format!("{TEST_BUILD_ID}-stale");
    assert_ne!(stale_build, TEST_BUILD_ID);
    let (_client, mut events, driver_task) =
        spawn_client_with_build(&relay.base(), &token, &stale_build);

    // The first (and only) event is `ReloadRequired`, carrying the server build.
    match next_event(&mut events).await {
        Event::ReloadRequired { server_build } => assert_eq!(server_build, TEST_BUILD_ID),
        other => panic!("expected ReloadRequired, got {other:?}"),
    }

    // `ReloadRequired` is terminal: the driver drains and, once the handle drops,
    // winds down rather than backing off and reconnecting. Awaiting the task
    // proves no reconnect loop is running.
    drop(_client);
    tokio::time::timeout(WAIT, driver_task)
        .await
        .expect("the driver task ends after ReloadRequired (no reconnect)")
        .expect("the driver task did not panic");

    // Settle past the small reconnect backoff, then assert the relay accepted
    // exactly one connection: a regression that fires even one spurious reconnect
    // attempt after `ReloadRequired` (the failure the design bullet calls out)
    // would land a second accept here.
    tokio::time::sleep(FAST_RECONNECT_BACKOFF * 4).await;
    assert_eq!(
        relay.accept_count(),
        1,
        "stale-build client must not reconnect after the terminal close"
    );

    // A stale build is a benign first-party tab, not a probe — no security event.
    assert_no_alerts(&flusher, &alerts, "stale-build close").await;
}

// ---------------------------------------------------------------------------
// Reconnect + in-ring resume through a severable transport
// ---------------------------------------------------------------------------

/// A controllable in-process TCP relay: the client connects to the relay's
/// stable front address, and the relay forwards bytes verbatim to a fixed
/// backend. [`Relay::sever`] drops the currently-forwarded connection at the
/// TCP layer, so the client's transport observes a close and reconnects to the
/// same front address; the relay forwards the reconnect to the same backend.
///
/// This is how a reconnect test severs a live surface WebSocket. The backend
/// serves the socket via axum's `on_upgrade`, which hands the upgraded stream
/// to a detached task the accept loop no longer owns — so neither a graceful
/// `axum::serve` shutdown nor aborting the serve task closes an established
/// session. Cutting the connection at the TCP layer is the reliable lever, and
/// keeping a single backend across the blip preserves the bus epoch, which is
/// exactly the in-ring-resume (transport-blip) case, as opposed to a genuine
/// process restart (new epoch → `Gap { EpochChanged }`).
///
/// Assumes at most one live forwarded connection at a time: `current` keeps only
/// the most recently accepted connection's abort handle, so `sever` targets that
/// one. The current tests never overlap connections (a client severs, then
/// reconnects). The accept loop task lives until the test runtime drops.
struct Relay {
    /// Front address the client connects to; stable across the sever.
    front: std::net::SocketAddr,
    /// Backend every newly accepted connection is forwarded to; `repoint` swaps
    /// it so a reconnect lands on a different server (the kiosk restart).
    backend: Arc<std::sync::Mutex<std::net::SocketAddr>>,
    /// Abort handle for the connection currently being forwarded, swapped in by
    /// the accept loop on each new inbound connection.
    current: Arc<std::sync::Mutex<Option<tokio::task::AbortHandle>>>,
    /// Count of connections accepted on the front listener. The stale-build test
    /// asserts this stays at 1 to prove no reconnect was attempted.
    accepts: Arc<std::sync::atomic::AtomicUsize>,
}

impl Relay {
    /// Bind a front listener and forward every accepted connection to the
    /// current backend, byte for byte.
    async fn start(backend: std::net::SocketAddr) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let front = listener.local_addr().unwrap();
        let backend = Arc::new(std::sync::Mutex::new(backend));
        let current: Arc<std::sync::Mutex<Option<tokio::task::AbortHandle>>> =
            Arc::new(std::sync::Mutex::new(None));
        let accepts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let current_for_loop = current.clone();
        let backend_for_loop = backend.clone();
        let accepts_for_loop = accepts.clone();
        tokio::spawn(async move {
            loop {
                // A failed accept is unambiguous harness breakage (fd exhaustion,
                // a dead listener), not a transient to tolerate: fail loud rather
                // than blackhole every future reconnect into an opaque timeout.
                let (mut inbound, _) = listener.accept().await.expect("relay accept failed");
                accepts_for_loop.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                // Read the backend fresh per connection, so a reconnect after
                // `repoint` splices onto the new server.
                let target = *backend_for_loop.lock().unwrap();
                let forward = tokio::spawn(async move {
                    // Failing to reach the backend is harness breakage (a bad
                    // `repoint` address, a backend that never bound): panic so the
                    // test names the relay, rather than letting the client
                    // reconnect-loop into a dead splice and time out.
                    let mut outbound = tokio::net::TcpStream::connect(target)
                        .await
                        .expect("relay could not reach backend");
                    // Verbatim byte splice in both directions until a peer closes
                    // the connection; `sever` cancels the whole task instead, so
                    // it never resolves here. A clean close resolves `Ok`; a peer
                    // reset (including our own `sever`) resolves `Err` with a
                    // transport-teardown io kind — the expected end of a throwaway
                    // splice. No test installs a tracing subscriber, so this trace
                    // is effectively dropped; it is only a breadcrumb if one is
                    // ever attached.
                    if let Err(e) = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await
                    {
                        tracing::trace!(error = %e, "relay splice ended with a transport error");
                    }
                });
                *current_for_loop.lock().unwrap() = Some(forward.abort_handle());
            }
        });
        Self {
            front,
            backend,
            current,
            accepts,
        }
    }

    /// Like `start`, but parse the backend address from a `spawn_test_server`
    /// `http://<addr>` base.
    async fn start_for(backend_base: &str) -> Self {
        Self::start(http_base_addr(backend_base)).await
    }

    /// The `http://` base the client dials.
    fn base(&self) -> String {
        format!("http://{}", self.front)
    }

    /// Number of connections accepted on the front listener so far.
    fn accept_count(&self) -> usize {
        self.accepts.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Point subsequent (reconnecting) connections at a different backend — the
    /// config-edit + restart the kiosk scenario models. Connections already
    /// forwarded are unaffected until `sever`.
    fn repoint(&self, backend: std::net::SocketAddr) {
        *self.backend.lock().unwrap() = backend;
    }

    /// Drop the connection currently being forwarded, breaking the client's
    /// transport at the TCP layer.
    fn sever(&self) {
        if let Some(handle) = self.current.lock().unwrap().take() {
            handle.abort();
        }
    }
}

/// A severed transport followed by a reconnect resumes the still-attached
/// port's subscription losslessly. The port stays attached across
/// the blip, so its channel keeps its resume token; on reconnect the client
/// resubscribes with that token and the backend — the same process, hence the
/// same bus epoch — replays only the messages published while the client was
/// down, with no gap and no duplicate.
#[tokio::test]
async fn client_reconnects_and_resumes_in_ring() {
    let db = db::init_db_memory();
    // Retain depth 4 comfortably holds both messages, so the resume replay is
    // in-ring (no hole).
    let SurfaceTestHarness {
        state,
        alerts,
        flusher,
        bus,
    } = subscribe_harness(&db, 4, 16);
    let (token, _) = setup_authenticated_user(&db).await;

    // "m1" into the ring, then start the backend and the severable relay in
    // front of it. The client dials the relay's stable front address.
    publish(&bus, "m1");
    let (backend_base, _sd) = spawn_test_server(state).await;
    let relay = Relay::start_for(&backend_base).await;

    let (client, mut events, driver_task) = spawn_client_reconnect(&relay.base(), &token);

    // First connection: register the instance and receive "m1", which
    // establishes the channel's resume token.
    match next_event(&mut events).await {
        Event::Connected { .. } => {}
        other => panic!("expected Connected, got {other:?}"),
    }
    let (entry, mut activations) = recorder();
    client.register_activation(COMPONENT, entry);
    expect_ephemeral_message(&mut activations, "m1").await;

    // Sever the transport. The instance stays registered across the blip, so the
    // client keeps the channel's resume token and will resubscribe with it.
    relay.sever();

    // Publish "m2" while the client is disconnected: it lands in the retain
    // ring (seq 2) and the client cannot have seen it live.
    publish(&bus, "m2");

    expect_disconnected(&mut events).await;

    // The client reconnects through the relay to the same backend (small
    // configured backoff), gets a fresh Welcome carrying the SAME epoch, and
    // resubscribes the surviving port with its resume token (seq 1). The backend
    // replays strictly seq > 1 — just "m2" — so the port receives it gaplessly,
    // with no duplicate of "m1".
    match next_event(&mut events).await {
        Event::Connected { .. } => {}
        other => panic!("expected Connected, got {other:?}"),
    }
    expect_new_messages(&mut activations, PORT, &["m2"]).await;

    // The resume is gapless with no duplicate of "m1": nothing else may follow.
    assert_no_further_message(&mut activations, PORT).await;

    client.close();
    // The kernel drops the handle after an orderly close; that drops every
    // command sender, so the driver's terminal drain loop winds it down.
    drop(client);

    tokio::time::timeout(WAIT, driver_task)
        .await
        .expect("the driver task ends after close")
        .expect("the driver task did not panic");

    // A conformant reconnect-and-resume session violates nothing.
    assert_no_alerts(&flusher, &alerts, "conformant in-ring resume").await;
}

/// The past-ring counterpart to the in-ring resume: when more is published
/// while the client is disconnected than the retain ring can hold, the resume
/// token points behind the oldest retained message, so the backend cannot heal
/// the gap exactly. It replays the full ring and flags the loss at the resume
/// layer — but the loss stops there: the activation contract has no
/// component-visible gap vocabulary, so the still-registered instance simply
/// sees the retained tail as its next window, an unremarkable
/// first-window-after-resubscribe, not a fatal, not a violation.
#[tokio::test]
async fn client_reconnect_past_ring_gaps() {
    let db = db::init_db_memory();
    // Retain depth 1: the ring keeps only the newest message, so a resume from
    // an older seq cannot be healed exactly.
    let SurfaceTestHarness {
        state,
        alerts,
        flusher,
        bus,
    } = subscribe_harness(&db, 1, 16);
    let (token, _) = setup_authenticated_user(&db).await;

    // "m1" into the ring, then start the backend and the severable relay.
    publish(&bus, "m1");
    let (backend_base, _sd) = spawn_test_server(state).await;
    let relay = Relay::start_for(&backend_base).await;

    let (client, mut events, driver_task) = spawn_client_reconnect(&relay.base(), &token);

    // First connection: register the instance and receive "m1", which
    // establishes the channel's resume token.
    match next_event(&mut events).await {
        Event::Connected { .. } => {}
        other => panic!("expected Connected, got {other:?}"),
    }
    let (entry, mut activations) = recorder();
    client.register_activation(COMPONENT, entry);
    expect_ephemeral_message(&mut activations, "m1").await;

    // Sever, then publish two more while the client is down. The depth-1 ring
    // keeps only the newest ("m3", seq 3); "m2" (seq 2) is evicted, so the
    // resume from seq 1 leaves an unhealable hole at seq 2.
    relay.sever();
    publish(&bus, "m2");
    publish(&bus, "m3");

    expect_disconnected(&mut events).await;

    // The client reconnects to the SAME backend (same epoch) and resubscribes
    // with its resume token (seq 1). The ring no longer contains everything past
    // seq 1, so the server replies with the full ring plus a HoleExceedsRing gap.
    match next_event(&mut events).await {
        Event::Connected { .. } => {}
        other => panic!("expected reconnect Connected, got {other:?}"),
    }
    // The loss is healed at the resume layer and never named to the component:
    // the instance's next window is simply the retained tail "m3".
    expect_new_messages(&mut activations, PORT, &["m3"]).await;

    // The retained tail is the whole sequence: no duplicate or extra follows.
    assert_no_further_message(&mut activations, PORT).await;

    client.close();
    // The kernel drops the handle after an orderly close; that drops every
    // command sender, so the driver's terminal drain loop winds it down.
    drop(client);

    tokio::time::timeout(WAIT, driver_task)
        .await
        .expect("the driver task ends after close")
        .expect("the driver task did not panic");

    // A reported gap is a normal outcome, not a protocol violation.
    assert_no_alerts(&flusher, &alerts, "past-ring resume").await;
}

// ---------------------------------------------------------------------------
// Kiosk scenario: config edit + restart under an auto-reconnecting client
// ---------------------------------------------------------------------------

/// The headline reconnect-reconcile case: a kiosk holds a surface open while an operator edits
/// its config to drop a binding and restarts the backend. The client
/// auto-reconnects onto the new (reduced) config, and its reconnect-reconcile
/// must drop the removed binding *before* sending any `Subscribe` — the
/// exact ordering that keeps a well-behaved client from ever emitting a
/// `SurfaceProtocolViolation` fail2ban ban when it would otherwise re-`Subscribe`
/// a now-unbound channel.
///
/// One instance binds both channels across the restart. Reconcile drops the
/// removed binding silently — the instance is neither failed nor deregistered,
/// it simply stops being activated on that channel, and there is no
/// component-visible binding-removed vocabulary. The surviving binding is
/// resubscribed (its old-epoch resume token no longer matches the restarted
/// backend's fresh bus epoch, so the server heals with a resume-layer gap and
/// replays the retained ring — the instance sees fresh data on the survivor, no
/// gap named to it, no fatal). Across the whole flow, zero security events are
/// captured on either backend.
#[tokio::test]
async fn client_kiosk_restart_drops_binding_without_violation() {
    let db = db::init_db_memory();
    let (token, _) = setup_authenticated_user(&db).await;

    // Backend 1: deskbar binding BOTH channels, retain depth 1 each. Seed each
    // ring so the two attaches confirm via a replayed message — the sync point
    // that both subscriptions are Active (and hold resume tokens) before the
    // restart.
    let SurfaceTestHarness {
        state: state1,
        alerts: alerts1,
        flusher: flusher1,
        bus: bus1,
    } = subscribe_state_two(&db, 1, 16);
    publish_as(&bus1, "publisher", EPH_ADDR, EPH_NAME, "a1", 1);
    publish_as(&bus1, "publisher", EPH_ADDR_B, EPH_NAME_B, "b1", 1);
    let (backend1_base, _sd1) = spawn_test_server(state1).await;
    let relay = Relay::start_for(&backend1_base).await;

    let (client, mut events, driver_task) = spawn_client_reconnect(&relay.base(), &token);

    // Connect to backend 1 (two bindings), attach both ports, confirm each is
    // live via its retained replay.
    match next_event(&mut events).await {
        Event::Connected { bindings, .. } => {
            assert_eq!(
                bindings.subscriptions.len(),
                2,
                "backend 1 must advertise both bindings"
            );
            let channels: Vec<&str> = bindings
                .subscriptions
                .iter()
                .map(|s| s.channel.as_str())
                .collect();
            assert!(
                channels.contains(&EPH_ADDR) && channels.contains(&EPH_ADDR_B),
                "backend 1 must bind both channels, got {channels:?}"
            );
        }
        other => panic!("expected Connected, got {other:?}"),
    }
    // One registration opens both of the instance's subscriptions; the retained
    // replay activates it with a window per channel. Confirm each channel is live
    // by its replayed message (they may arrive in one activation or two).
    let (entry, mut activations) = recorder();
    client.register_activation(COMPONENT, entry);
    expect_ports_each_get(&mut activations, &[(PORT, "a1"), (PORT_B, "b1")]).await;

    // Backend 2: the config-edited restart — deskbar with only binding A, over a
    // fresh bus (new epoch). Seed "a2" so the resumed survivor has something to
    // replay. Shares the same `db` as backend 1, so the session cookie stays
    // valid across the restart.
    let SurfaceTestHarness {
        state: state2,
        alerts: alerts2,
        flusher: flusher2,
        bus: bus2,
    } = subscribe_harness(&db, 1, 16);
    publish(&bus2, "a2");
    let (backend2_base, _sd2) = spawn_test_server(state2).await;

    // Restart under the auto-reconnecting client: point the relay at backend 2
    // and cut the live connection.
    relay.repoint(http_base_addr(&backend2_base));
    relay.sever();

    expect_disconnected(&mut events).await;

    // The client reconnects to backend 2, whose Welcome carries only binding A.
    // Reconcile force-detaches port B (BindingRemoved, and crucially no
    // Subscribe for the dropped channel) and resubscribes survivor A (small
    // configured reconnect backoff).
    match next_event(&mut events).await {
        Event::Connected { bindings, .. } => {
            assert_eq!(
                bindings.subscriptions.len(),
                1,
                "the restarted backend dropped a binding"
            );
            assert_eq!(bindings.subscriptions[0].channel, EPH_ADDR);
        }
        other => panic!("expected reconnect Connected, got {other:?}"),
    }

    // The surviving binding (PORT) is resubscribed with its old-epoch token,
    // which no longer matches backend 2's epoch, so the server heals with a
    // resume-layer gap and replays the retained ring ("a2"). The gap is not named
    // to the component: the instance simply activates with "a2" new on PORT. The
    // dropped binding (PORT_B) produces no activation of its own — the instance
    // just stops being delivered on that channel, silently.
    expect_new_messages(&mut activations, PORT, &["a2"]).await;
    // And nothing arrives for the dropped channel.
    assert_no_further_message(&mut activations, PORT_B).await;

    client.close();
    // The kernel drops the handle after an orderly close; that drops every
    // command sender, so the driver's terminal drain loop winds it down.
    drop(client);

    tokio::time::timeout(WAIT, driver_task)
        .await
        .expect("the driver task ends after close")
        .expect("the driver task did not panic");

    // The full config-edit-and-restart flow — the exact scenario that would
    // otherwise generate SurfaceProtocolViolation fail2ban bans under an
    // auto-reconnecting kiosk — trips zero security events on either backend.
    // Backend 2's empty-alerts check is the sole detector for a reconcile that
    // resubscribes the dropped channel *after* the survivor, so the drain
    // barrier in `assert_no_alerts` is load-bearing here.
    assert_no_alerts(&flusher1, &alerts1, "kiosk restart backend 1").await;
    assert_no_alerts(&flusher2, &alerts2, "kiosk restart backend 2").await;
}

// ---------------------------------------------------------------------------
// Publish outcomes
// ---------------------------------------------------------------------------

/// The writer component + output port the publish fixture binds to `EPH_ADDR`.
const WRITER: &str = "writer";
const OUT: &str = "out";

/// A `deskbar` surface wiring one ephemeral OUTPUT port — `(writer, out)` →
/// `EPH_ADDR` — with a policy that covers publishing onto it. No subscription:
/// the client only publishes here, and the `PublishResult` proves the round
/// trip. `publish_burst` / `publish_per_sec` size the connection's publish token
/// bucket, so the flood test can pick a small burst.
fn deskbar_pub(publish_burst: u32, publish_per_sec: u32) -> ResolvedSurface {
    SurfaceFixture::new("deskbar", WRITER)
        .output(EPH_ADDR, WRITER, OUT)
        .policy(publish_policy(&[EPH_NAME]))
        .publish_rate(publish_burst, publish_per_sec)
        .build()
}

/// Capturing-alerter state whose `deskbar` surface is the publish fixture: one
/// bound ephemeral output port over a real bus (no retain ring needed — the
/// client only publishes). `publish_burst` / `publish_per_sec` size the
/// connection's publish token bucket.
fn publish_state(db: &db::Db, publish_burst: u32, publish_per_sec: u32) -> SurfaceTestHarness {
    surface_harness(
        db,
        deskbar_pub(publish_burst, publish_per_sec),
        vec![ephemeral_channel_entry(EPH_NAME, 0, 16)],
    )
}

/// The publish path end to end against the real backend: a publish through a
/// bound output port is accepted and its `Ok` result routes back on the event
/// stream by correlation; a body over the connection's cap is rejected locally
/// by the handle (no frame on the wire, so no correlation, so the server never
/// sees it), and the connection stays healthy for a following publish. Both a
/// bound publish and a locally-rejected oversized publish are conformant, so the
/// session trips zero security events.
#[tokio::test]
async fn client_publish_ok_routes_and_oversized_rejected_locally() {
    let db = db::init_db_memory();
    // Burst 60: comfortably admits the handful of publishes this test makes, so
    // no outcome is `RateLimited` (that path is the flood test's).
    let SurfaceTestHarness {
        state,
        alerts,
        flusher,
        ..
    } = publish_state(&db, 60, 1);
    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;

    let (client, mut events, driver_task) = spawn_client(&base, &token);

    // Connect: the Welcome advertises the bound output port and the body cap the
    // handle's publish gate pre-validates against.
    match next_event(&mut events).await {
        Event::Connected {
            bindings,
            max_body_bytes,
            ..
        } => {
            assert_eq!(max_body_bytes, TEST_MAX_BODY_BYTES as u64);
            assert_eq!(bindings.outputs.len(), 1);
            assert_eq!(bindings.outputs[0].channel, EPH_ADDR);
            assert_eq!(bindings.outputs[0].instance, WRITER);
            assert_eq!(bindings.outputs[0].port, OUT);
        }
        other => panic!("expected Connected, got {other:?}"),
    }

    // Publish through the bound output port: the handle accepts it (gate pass),
    // returns its correlation, and the server's `Ok` routes back by that
    // correlation.
    let correlation = client
        .publish(WRITER, OUT, "hello".to_string())
        .expect("a bound, in-cap publish is accepted locally");
    match next_event(&mut events).await {
        Event::PublishResult {
            instance,
            port,
            correlation: got,
            status,
        } => {
            assert_eq!(instance, WRITER);
            assert_eq!(port, OUT);
            assert_eq!(got, correlation, "the result routes back by correlation");
            assert_eq!(status, PublishStatus::Ok);
        }
        other => panic!("expected PublishResult, got {other:?}"),
    }

    // A body over the cap is rejected locally by the handle: `Err(BodyTooLarge)`
    // synchronously, and — crucially — nothing is sent, so no correlation is
    // minted and the server never sees the doomed frame.
    let huge = "x".repeat(TEST_MAX_BODY_BYTES + 1);
    match client.publish(WRITER, OUT, huge) {
        Err(PublishReject::BodyTooLarge { len, max }) => {
            assert_eq!(len, TEST_MAX_BODY_BYTES as u64 + 1);
            assert_eq!(max, TEST_MAX_BODY_BYTES as u64);
        }
        other => panic!("expected a local BodyTooLarge rejection, got {other:?}"),
    }

    // The connection is unaffected by the locally-rejected oversized publish: a
    // following in-cap publish still round-trips `Ok`. This also proves the
    // oversized publish put nothing on the wire — a desync would surface here as
    // a wrong correlation or a missing result.
    let correlation2 = client
        .publish(WRITER, OUT, "again".to_string())
        .expect("a following in-cap publish is accepted");
    match next_event(&mut events).await {
        Event::PublishResult {
            correlation: got,
            status,
            ..
        } => {
            assert_eq!(
                got, correlation2,
                "the second result routes back by its own correlation"
            );
            assert_eq!(status, PublishStatus::Ok);
        }
        other => panic!("expected the second PublishResult, got {other:?}"),
    }

    client.close();
    // The kernel drops the handle after an orderly close; that drops every
    // command sender, so the driver's terminal drain loop winds it down.
    drop(client);

    tokio::time::timeout(WAIT, driver_task)
        .await
        .expect("the driver task ends after close")
        .expect("the driver task did not panic");

    assert_no_alerts(&flusher, &alerts, "conformant publish outcomes").await;
}

/// A synchronous publish burst that outruns the connection's publish token
/// bucket surfaces at least one `PublishResult { status: RateLimited }` routed
/// back by correlation, without killing the connection. Server-side rate
/// limiting is a metered, non-violation outcome: the client just relays the
/// per-publish result to the owning component and never retries — so the burst
/// trips zero security events and the connection stays healthy afterward.
#[tokio::test]
async fn client_publish_flood_rate_limited_stays_healthy() {
    let db = db::init_db_memory();
    // Burst 2, no refill: a tight six-publish flood exhausts the connection
    // bucket deterministically (exactly 2 Ok, 4 RateLimited) with no dependence
    // on how much wall-clock time elapses between frames.
    let SurfaceTestHarness {
        state,
        alerts,
        flusher,
        ..
    } = publish_state(&db, 2, 0);
    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;

    let (client, mut events, driver_task) = spawn_client(&base, &token);

    match next_event(&mut events).await {
        Event::Connected { bindings, .. } => {
            assert_eq!(bindings.outputs.len(), 1);
            assert_eq!(bindings.outputs[0].channel, EPH_ADDR);
        }
        other => panic!("expected Connected, got {other:?}"),
    }

    // Six publishes in a tight loop. Each passes the handle's local gate (bound,
    // in-cap, connected) — the gate is not a rate limiter — so each returns its
    // own correlation; the server's per-connection bucket is what limits them.
    const FLOOD: usize = 6;
    let mut correlations = std::collections::HashSet::new();
    for _ in 0..FLOOD {
        let correlation = client
            .publish(WRITER, OUT, "spam".to_string())
            .expect("a bound, in-cap publish is accepted locally regardless of server rate");
        assert!(
            correlations.insert(correlation),
            "each publish must mint a distinct correlation"
        );
    }

    // Every publish gets exactly one result routed back by its correlation:
    // `Ok` while tokens remained, `RateLimited` once the bucket emptied. Nothing
    // else may appear on the event stream (no violation-driven Fatal).
    let mut rate_limited = 0;
    for _ in 0..FLOOD {
        match next_event(&mut events).await {
            Event::PublishResult {
                instance,
                port,
                correlation,
                status,
            } => {
                assert_eq!(instance, WRITER);
                assert_eq!(port, OUT);
                assert!(
                    correlations.remove(&correlation),
                    "each result routes back by a distinct, previously-sent correlation"
                );
                match status {
                    PublishStatus::Ok => {}
                    PublishStatus::RateLimited => rate_limited += 1,
                    other => panic!("expected Ok or RateLimited, got {other:?}"),
                }
            }
            other => panic!("expected a PublishResult, got {other:?}"),
        }
    }
    assert!(
        correlations.is_empty(),
        "every publish must be answered exactly once"
    );
    assert!(
        rate_limited >= 1,
        "a burst-2 flood of {FLOOD} must produce at least one RateLimited result"
    );

    // The connection was rate-limited, not killed: an orderly close completes and
    // the driver task ends cleanly (a fatal frame would have ended it earlier and
    // failed the close path instead).
    client.close();
    // The kernel drops the handle after an orderly close; that drops every
    // command sender, so the driver's terminal drain loop winds it down.
    drop(client);
    tokio::time::timeout(WAIT, driver_task)
        .await
        .expect("the driver task ends after close")
        .expect("the driver task did not panic");

    // Rate limiting is a metered outcome, never a violation: no security event.
    assert_no_alerts(&flusher, &alerts, "rate-limited publish flood").await;
}

// ---------------------------------------------------------------------------
// Report path stays healthy
// ---------------------------------------------------------------------------

/// A client-originated error report leaves the session live. With no error
/// channel configured the advertised floor is `None`, so `report` publishes
/// nothing (console-only, by design) — but the calls must not desync or kill the
/// session: a subsequently-attached port still receives its retained message. A
/// report whose message exceeds the proto cap is truncated client-side before it
/// would reach the wire, so nothing here is a violation and the session trips
/// zero security events.
#[tokio::test]
async fn client_report_path_stays_healthy() {
    let db = db::init_db_memory();
    let SurfaceTestHarness {
        state,
        alerts,
        flusher,
        bus,
    } = subscribe_harness(&db, 1, 16);
    let (token, _) = setup_authenticated_user(&db).await;
    // Seed the ring so the post-report attach confirms the session is live.
    publish(&bus, "hello");
    let (base, _sd) = spawn_test_server(state).await;

    let (client, mut events, driver_task) = spawn_client(&base, &token);

    match next_event(&mut events).await {
        Event::Connected { .. } => {}
        other => panic!("expected Connected, got {other:?}"),
    }

    // A short report, then a max-length one whose message is well over the proto
    // cap. The client truncates the oversize message, and with no floor
    // advertised neither report reaches the wire; either way the report path
    // never kills the session or emits a security event.
    client.report(
        LogLevel::Error,
        "component:demo",
        "component blew up",
        Some("demo"),
    );
    let huge = "x".repeat(MAX_LOG_MESSAGE_BYTES * 2);
    client.report(LogLevel::Error, "component:demo", &huge, Some("demo"));

    // The session survived both reports: register the instance and receive the
    // retained message. Had a report killed or desynced the session, this
    // registration's replay would time out instead.
    let (entry, mut activations) = recorder();
    client.register_activation(COMPONENT, entry);
    expect_ephemeral_message(&mut activations, "hello").await;

    client.close();
    // The kernel drops the handle after an orderly close; that drops every
    // command sender, so the driver's terminal drain loop winds it down.
    drop(client);

    tokio::time::timeout(WAIT, driver_task)
        .await
        .expect("the driver task ends after close")
        .expect("the driver task did not panic");

    // Reporting an error is never a protocol violation.
    assert_no_alerts(&flusher, &alerts, "conformant Log").await;
}
