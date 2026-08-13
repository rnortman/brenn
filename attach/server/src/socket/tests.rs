//! Socket-lifecycle tests: the handshake against a stream of websocket messages,
//! and the writer against a sink of them.

use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use brenn_attach_proto::{ClientFrame, VersionRange};
use futures::stream;
use uuid::Uuid;

use super::*;
use crate::test_support::{AttachCtxBuilder, TEST_BODY_BYTES, TestProfile};

/// A `Hello` from a peer speaking `[min, max]`.
fn client_hello(min: u32, max: u32) -> Message {
    let frame = ClientFrame::Hello {
        versions: VersionRange { min, max },
        ident: "test-attacher".to_string(),
    };
    Message::Text(serde_json::to_string(&frame).unwrap().into())
}

/// A handshake over exactly these inbound messages, with a bound no test reaches
/// except the one about the bound.
async fn handshake(messages: Vec<Result<Message, axum::Error>>) -> Handshake {
    let mut stream = stream::iter(messages);
    read_client_hello(&mut stream, Duration::from_secs(60)).await
}

fn agreed(verdict: Handshake) -> u32 {
    match verdict {
        Handshake::Agreed(version) => version,
        Handshake::Incompatible(range) => panic!("expected agreement, got incompatible {range:?}"),
        Handshake::Violation(detail) => panic!("expected agreement, got violation: {detail}"),
        Handshake::Disconnect => panic!("expected agreement, got disconnect"),
    }
}

fn violation(verdict: Handshake) -> String {
    match verdict {
        Handshake::Violation(detail) => detail,
        Handshake::Agreed(version) => panic!("expected a violation, got agreement on {version}"),
        Handshake::Incompatible(range) => {
            panic!("expected a violation, got incompatible {range:?}")
        }
        Handshake::Disconnect => panic!("expected a violation, got disconnect"),
    }
}

/// The two ends deploying the same build is the ordinary case, and the version
/// it agrees on is this build's own.
#[tokio::test]
async fn matching_ranges_agree_on_this_builds_version() {
    let range = SUPPORTED_VERSIONS;
    assert_eq!(
        agreed(handshake(vec![Ok(client_hello(range.min, range.max))]).await),
        range.max
    );
}

/// A peer that can speak more than this build does not force it up: the
/// agreement is the highest *both* hold.
#[tokio::test]
async fn a_wider_peer_range_agrees_on_the_highest_both_speak() {
    assert_eq!(
        agreed(handshake(vec![Ok(client_hello(SUPPORTED_VERSIONS.min, 99))]).await),
        SUPPORTED_VERSIONS.max
    );
}

/// A peer whose whole range sits above this build's is conforming — it stated
/// what it speaks — so the answer is a close carrying its range for the log, not
/// a security event.
#[tokio::test]
async fn a_disjoint_peer_range_is_incompatible_not_a_violation() {
    let peer = VersionRange {
        min: SUPPORTED_VERSIONS.max + 1,
        max: SUPPORTED_VERSIONS.max + 4,
    };
    match handshake(vec![Ok(client_hello(peer.min, peer.max))]).await {
        Handshake::Incompatible(range) => assert_eq!(range, peer),
        _ => panic!("expected incompatible"),
    }
}

/// An empty range (`min > max`) needs no check of its own: it overlaps nothing,
/// so it falls out of the negotiation as an ordinary incompatibility.
#[tokio::test]
async fn an_empty_peer_range_is_incompatible() {
    assert!(matches!(
        handshake(vec![Ok(client_hello(9, 2))]).await,
        Handshake::Incompatible(VersionRange { min: 9, max: 2 })
    ));
}

/// Liveness frames are not application frames, so they do not stand in for the
/// `Hello` the handshake is waiting for.
#[tokio::test]
async fn liveness_frames_ahead_of_the_hello_are_skipped() {
    let messages = vec![
        Ok(Message::Ping(Vec::new().into())),
        Ok(Message::Pong(Vec::new().into())),
        Ok(client_hello(SUPPORTED_VERSIONS.min, SUPPORTED_VERSIONS.max)),
    ];
    assert_eq!(agreed(handshake(messages).await), SUPPORTED_VERSIONS.max);
}

/// Until a version is agreed there is no schema under which a second frame kind
/// could be read, so anything else first is non-conforming.
#[tokio::test]
async fn a_first_frame_that_is_not_hello_is_a_violation() {
    let subscribe = ClientFrame::Subscribe {
        channel: "brenn:demo".to_string(),
        push_depth: 1,
        retain_depth: 1,
        resume: None,
    };
    let text = Message::Text(serde_json::to_string(&subscribe).unwrap().into());
    assert_eq!(
        violation(handshake(vec![Ok(text)]).await),
        "first client frame is not Hello"
    );
}

/// The `Hello` shape is frozen across every version, so junk here is a client
/// that cannot speak the handshake — and its bytes never reach the security log.
#[tokio::test]
async fn unparseable_text_is_a_violation_that_never_echoes_it() {
    let junk = Message::Text("{\"type\":\"Hello\",\"smuggled\":\"CANARY\"}".into());
    let detail = violation(handshake(vec![Ok(junk)]).await);
    assert_eq!(detail, "unparseable client frame before Hello");
    assert!(!detail.contains("CANARY"), "payload echoed: {detail}");
}

/// The protocol is JSON text frames; binary is not a frame this transport reads,
/// at the handshake or after it.
#[tokio::test]
async fn a_binary_frame_before_the_hello_is_a_violation() {
    assert_eq!(
        violation(handshake(vec![Ok(Message::Binary(Vec::new().into()))]).await),
        "binary frame before Hello"
    );
}

/// A peer that opens a socket and closes it said nothing wrong.
#[tokio::test]
async fn a_close_before_the_hello_is_a_disconnect() {
    assert!(matches!(
        handshake(vec![Ok(Message::Close(None))]).await,
        Handshake::Disconnect
    ));
    assert!(matches!(handshake(vec![]).await, Handshake::Disconnect));
}

/// The read cap firing is tampering or a serious client bug — no config-legal
/// frame can reach it — so it is fail2ban signal even before a version is agreed.
#[tokio::test]
async fn an_oversized_frame_before_the_hello_is_a_violation() {
    let err = axum::Error::new(tungstenite::Error::Capacity(
        tungstenite::error::CapacityError::MessageTooLong {
            size: 4_000_000,
            max_size: 1_000,
        },
    ));
    assert_eq!(
        violation(handshake(vec![Err(err)]).await),
        "inbound frame exceeds size cap"
    );
}

/// Every other read error is the network, not the peer: tear down, no security
/// event.
#[tokio::test]
async fn an_ordinary_read_error_before_the_hello_is_a_disconnect() {
    let err = axum::Error::new(std::io::Error::other("connection reset"));
    assert!(matches!(
        handshake(vec![Err(err)]).await,
        Handshake::Disconnect
    ));
}

/// The attacher sends `Hello` first without waiting, so silence past the bound is
/// a client holding an attachment slot without attaching.
#[tokio::test(start_paused = true)]
async fn silence_past_the_bound_is_a_violation() {
    let mut quiet = stream::pending::<Result<Message, axum::Error>>();
    let verdict = read_client_hello(&mut quiet, Duration::from_secs(45)).await;
    assert_eq!(violation(verdict), "no Hello within 45s of upgrade");
}

#[test]
fn the_server_hello_states_this_builds_range_and_ident() {
    match server_hello("brenn-server-test") {
        ServerFrame::Hello { versions, ident } => {
            assert_eq!(versions, SUPPORTED_VERSIONS);
            assert_eq!(ident, "brenn-server-test");
        }
        _ => panic!("expected Hello"),
    }
}

// ---------------------------------------------------------------------------
// Welcome
// ---------------------------------------------------------------------------

/// An attachment context carrying only what `Welcome` reads: an identity, a
/// session id, a body cap, and a grant.
fn welcome_ctx(alert_granted: bool) -> (AttachSessionCtx, Uuid) {
    let session_id = Uuid::new_v4();
    let (ctx, _rx) = AttachCtxBuilder::new(TestProfile {
        alert_granted,
        ..TestProfile::new()
    })
    .session_id(session_id)
    .build();
    (ctx, session_id)
}

#[tokio::test]
async fn welcome_states_the_transport_contract_of_this_attachment() {
    let (ctx, session_id) = welcome_ctx(false);
    match welcome(&ctx, 1, 20) {
        ServerFrame::Welcome {
            version,
            participant_id,
            session_id: id,
            heartbeat_secs,
            max_body_bytes,
            max_frame_bytes,
            alert_granted,
        } => {
            assert_eq!(version, 1);
            assert_eq!(participant_id, "surface:deskbar");
            assert_eq!(id, session_id.simple().to_string());
            assert_eq!(heartbeat_secs, 20);
            assert_eq!(max_body_bytes, TEST_BODY_BYTES as u64);
            assert_eq!(
                max_frame_bytes,
                max_client_frame_bytes(TEST_BODY_BYTES) as u64
            );
            assert!(!alert_granted);
        }
        _ => panic!("expected Welcome"),
    }
}

/// The grant is the profile's answer and nothing else — an attacher learns its
/// rights from the server rather than guessing them.
#[tokio::test]
async fn welcome_advertises_the_profiles_alert_grant() {
    let (granted, _) = welcome_ctx(true);
    assert!(matches!(
        welcome(&granted, 1, 20),
        ServerFrame::Welcome {
            alert_granted: true,
            ..
        }
    ));
}

// ---------------------------------------------------------------------------
// Writer
// ---------------------------------------------------------------------------

/// How the sink under test behaves: take everything, fail the next write, or
/// never complete one.
#[derive(Clone, Copy, PartialEq, Eq)]
enum SinkMode {
    Record,
    Fail,
    Stall,
}

/// A `Sink<Message>` standing in for the socket half, recording what the writer
/// hands it.
struct TestSink {
    mode: SinkMode,
    written: Arc<Mutex<Vec<Message>>>,
}

impl TestSink {
    fn new(mode: SinkMode) -> (Self, Arc<Mutex<Vec<Message>>>) {
        let written = Arc::new(Mutex::new(Vec::new()));
        (
            TestSink {
                mode,
                written: written.clone(),
            },
            written,
        )
    }
}

impl Sink<Message> for TestSink {
    type Error = String;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), String>> {
        match self.mode {
            SinkMode::Record => Poll::Ready(Ok(())),
            SinkMode::Fail => Poll::Ready(Err("sink is broken".to_string())),
            SinkMode::Stall => Poll::Pending,
        }
    }

    fn start_send(self: Pin<&mut Self>, item: Message) -> Result<(), String> {
        self.written.lock().expect("written poisoned").push(item);
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), String>> {
        self.poll_ready(cx)
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), String>> {
        Poll::Ready(Ok(()))
    }
}

/// The frames the writer wrote, in order, as parsed `ServerFrame`s — liveness
/// pings dropped.
fn written_frames(written: &Arc<Mutex<Vec<Message>>>) -> Vec<ServerFrame> {
    written
        .lock()
        .expect("written poisoned")
        .iter()
        .filter_map(|msg| match msg {
            Message::Text(text) => Some(serde_json::from_str(text.as_str()).expect("ServerFrame")),
            _ => None,
        })
        .collect()
}

/// How many native pings the writer emitted.
fn written_pings(written: &Arc<Mutex<Vec<Message>>>) -> usize {
    written
        .lock()
        .expect("written poisoned")
        .iter()
        .filter(|msg| matches!(msg, Message::Ping(_)))
        .count()
}

/// The writer is the connection's one serializer: frames reach the socket in the
/// order the session enqueued them, and dropping the sender is what ends it.
#[tokio::test]
async fn the_writer_serializes_in_order_and_exits_when_the_session_drops_its_sender() {
    let (sink, written) = TestSink::new(SinkMode::Record);
    let (tx, rx) = mpsc::channel(4);
    // A heartbeat far past the test's own lifetime: this test is about the frame
    // path, and an idle tick would add frames nobody enqueued.
    let writer = tokio::spawn(writer_task(sink, rx, Duration::from_secs(3600)));

    tx.send(server_hello("brenn-server-test")).await.unwrap();
    tx.send(ServerFrame::Heartbeat).await.unwrap();
    drop(tx);
    writer.await.unwrap();

    assert!(matches!(
        written_frames(&written).as_slice(),
        [ServerFrame::Hello { .. }, ServerFrame::Heartbeat]
    ));
}

/// Idle liveness is both probes at once: the native ping the transport sees, and
/// the application `Heartbeat` a browser websocket can actually observe.
#[tokio::test(start_paused = true)]
async fn an_idle_tick_writes_a_ping_and_a_heartbeat() {
    let (sink, written) = TestSink::new(SinkMode::Record);
    let (tx, rx) = mpsc::channel(4);
    let writer = tokio::spawn(writer_task(sink, rx, Duration::from_secs(10)));

    tokio::time::sleep(Duration::from_secs(11)).await;
    assert_eq!(written_pings(&written), 1);
    assert!(matches!(
        written_frames(&written).as_slice(),
        [ServerFrame::Heartbeat]
    ));

    drop(tx);
    writer.await.unwrap();
}

/// A tick that follows real traffic still probes, but adds no idle `Heartbeat`:
/// the frame already proved the connection alive.
#[tokio::test(start_paused = true)]
async fn a_frame_written_since_the_tick_suppresses_the_idle_heartbeat() {
    let (sink, written) = TestSink::new(SinkMode::Record);
    let (tx, rx) = mpsc::channel(4);
    let writer = tokio::spawn(writer_task(sink, rx, Duration::from_secs(10)));

    tx.send(server_hello("brenn-server-test")).await.unwrap();
    tokio::time::sleep(Duration::from_secs(11)).await;
    assert_eq!(written_pings(&written), 1);
    assert!(
        matches!(
            written_frames(&written).as_slice(),
            [ServerFrame::Hello { .. }]
        ),
        "the idle heartbeat rode along anyway"
    );

    drop(tx);
    writer.await.unwrap();
}

/// A dead sink ends the writer, which drops its receiver — the session's own
/// teardown signal.
#[tokio::test]
async fn a_sink_error_exits_the_writer() {
    let (sink, written) = TestSink::new(SinkMode::Fail);
    let (tx, rx) = mpsc::channel(4);
    let writer = tokio::spawn(writer_task(sink, rx, Duration::from_secs(3600)));

    tx.send(ServerFrame::Heartbeat).await.unwrap();
    writer.await.unwrap();
    assert!(written_frames(&written).is_empty());
    // The session learns of it the same way it learns of any writer exit.
    assert!(tx.send(ServerFrame::Heartbeat).await.is_err());
}

/// A reader that stops draining cannot pin the connection open: the watchdog
/// bounds every write, and a write still pending past it tears the session down.
#[tokio::test(start_paused = true)]
async fn a_stalled_sink_exits_the_writer_after_the_watchdog() {
    let (sink, written) = TestSink::new(SinkMode::Stall);
    let (tx, rx) = mpsc::channel(4);
    let writer = tokio::spawn(writer_task(sink, rx, Duration::from_secs(10)));

    tx.send(ServerFrame::Heartbeat).await.unwrap();
    writer.await.unwrap();
    assert!(written_frames(&written).is_empty());
    assert!(tx.send(ServerFrame::Heartbeat).await.is_err());
}
