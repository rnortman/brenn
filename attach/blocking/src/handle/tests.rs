//! The supervisor and its blocking face, against a scripted transport.
//!
//! Each test drives a real [`BlockingClient`] from the test thread while its
//! supervisor runs on its own thread, as a caller would. The connector is
//! scripted: every connect takes the next plan, and every opened socket is a
//! pair of channels the test reads and writes as the server.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::mpsc as std_mpsc;
use std::thread;

use brenn_attach_client::conn::DetachReason;
use brenn_attach_client::{TransportConnection, TransportError, TransportEvent};
use brenn_attach_proto::{
    BlobReadOutcome, ClientFrame, Codec, Cursor, DeliverRow, GapInfo, GapReason, MessageBody,
    PublishBatchOutcome, PublishOutcome as WireOutcome, ServerFrame, SubscribeOutcome,
    VersionRange, WireMessage, encode_server_hello,
};
use brenn_envelope::{ChannelScheme, MessageEnvelope, Urgency};
use tokio::sync::{mpsc as tokio_mpsc, oneshot};

use super::*;
use crate::Delivery;
use crate::connector::set_failure;

const URL: &str = "ws://127.0.0.1:1/attach";
const CHANNEL: &str = "ephemeral:lab.events";
const OTHER: &str = "ephemeral:lab.other";
/// Long enough for any step a test waits on; short enough to fail a hang.
const PATIENCE: Duration = Duration::from_secs(5);
/// How long a test watches for something that must not happen.
const QUIET: Duration = Duration::from_millis(150);

fn timings() -> Timings {
    Timings {
        initial_backoff: Duration::from_millis(20),
        max_backoff: Duration::from_millis(100),
        connect_timeout: PATIENCE,
    }
}

fn never() -> impl FnMut() -> bool {
    || false
}

fn codec() -> Codec {
    Codec::negotiated(5)
}

fn depths() -> SubscriptionDepths {
    SubscriptionDepths {
        push_depth: 4,
        retain_depth: 8,
    }
}

fn cursor(token: &str) -> Cursor {
    serde_json::from_value(serde_json::Value::String(token.to_string())).expect("a cursor")
}

fn envelope(channel: &str, text: &str) -> MessageEnvelope {
    MessageEnvelope {
        message_id: uuid::Uuid::nil(),
        source: "test".to_string(),
        channel: channel.to_string(),
        sender: "attacher:alice".to_string(),
        publish_ts: chrono::DateTime::from_timestamp(0, 0).expect("a representable instant"),
        body: MessageBody::text(text),
        reply_to: None,
        delivery_deadline: None,
        deliver_after: None,
        impetus: None,
        urgency: Urgency::Normal,
        envelope_type: ChannelScheme::Ephemeral,
        transport: None,
        attachments: Default::default(),
    }
}

fn row(channel: &str, seq: u64, dropped: u64) -> DeliverRow {
    DeliverRow {
        envelope: envelope(channel, &format!("message {seq}")),
        seq,
        cursor: cursor(&format!("c{seq}")),
        dropped,
    }
}

fn deliver(channel: &str, rows: Vec<DeliverRow>) -> ServerFrame {
    ServerFrame::Deliver {
        channel: channel.to_string(),
        rows,
    }
}

fn opened(channel: &str, replay_count: u32, gap: Option<GapInfo>) -> ServerFrame {
    ServerFrame::SubscribeResult {
        channel: channel.to_string(),
        outcome: SubscribeOutcome::Ok,
        replay_count,
        gap,
    }
}

fn unavailable(channel: &str) -> ServerFrame {
    ServerFrame::SubscribeResult {
        channel: channel.to_string(),
        outcome: SubscribeOutcome::Unavailable,
        replay_count: 0,
        gap: None,
    }
}

fn publish_request(channel: &str) -> PublishRequest {
    PublishRequest::new(channel, MessageBody::text("hello"), Urgency::Normal)
}

fn gone() -> DetachReason {
    DetachReason::TransportClosed {
        code: None,
        reason: "gone".to_string(),
    }
}

/// Poll `condition` until it holds, failing the test after [`PATIENCE`].
fn wait_until(what: &str, mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + PATIENCE;
    while !condition() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        thread::sleep(Duration::from_millis(5));
    }
}

// ── the scripted transport ────────────────────────────────────────────────

enum Plan {
    /// The connect fails, leaving this failure in the dial cell.
    Fail(Option<DialFailure>),
    Succeed(Wire),
    /// The connect resolves, opened, only once the trigger fires.
    Stall(oneshot::Receiver<()>, Wire),
}

/// The connector's half of one socket.
struct Wire {
    incoming: tokio_mpsc::UnboundedReceiver<TransportEvent>,
    written: std_mpsc::Sender<WireMessage>,
    closed: Arc<AtomicBool>,
}

/// The test's half of one socket: it writes as the server and reads what the
/// client wrote.
struct Socket {
    feed: tokio_mpsc::UnboundedSender<TransportEvent>,
    written: std_mpsc::Receiver<WireMessage>,
    closed: Arc<AtomicBool>,
}

fn socket_pair() -> (Socket, Wire) {
    let (feed, incoming) = tokio_mpsc::unbounded_channel();
    let (written_tx, written) = std_mpsc::channel();
    let closed = Arc::new(AtomicBool::new(false));
    (
        Socket {
            feed,
            written,
            closed: closed.clone(),
        },
        Wire {
            incoming,
            written: written_tx,
            closed,
        },
    )
}

impl Socket {
    fn send_raw(&self, message: WireMessage) {
        let _ = self.feed.send(TransportEvent::Message(message));
    }

    fn send(&self, frame: ServerFrame) {
        self.send_raw(codec().encode_server(&frame));
    }

    /// Take the client's `Hello` and answer with the server's, speaking
    /// `versions`.
    fn hello(&self, versions: VersionRange) {
        let hello = self
            .written
            .recv_timeout(PATIENCE)
            .expect("the client's Hello");
        assert!(matches!(hello, WireMessage::Text(_)), "Hello is text");
        self.send_raw(encode_server_hello(versions, "scripted-server"));
    }

    /// Complete the handshake on v5, minting `session`.
    fn handshake(&self, session: &str) {
        self.hello(VersionRange::exactly(5));
        self.send(ServerFrame::Welcome {
            version: 5,
            participant_id: "attacher:bob".to_string(),
            session_id: session.to_string(),
            heartbeat_secs: 60,
            max_body_bytes: 4096,
            max_frame_bytes: 16384,
            alert_granted: false,
            blobs: None,
        });
    }

    /// The next frame the client wrote.
    fn next_frame(&self) -> ClientFrame {
        let message = self
            .written
            .recv_timeout(PATIENCE)
            .expect("a frame from the client");
        codec().decode_client(message).expect("a v5 client frame")
    }

    /// Assert the client writes nothing for [`QUIET`].
    fn no_frame(&self) {
        if let Ok(message) = self.written.recv_timeout(QUIET) {
            panic!(
                "the client wrote {:?}",
                codec().decode_client(message.clone()).map_err(|_| message)
            );
        }
    }

    /// The peer goes away with no close code.
    fn drop_connection(&self) {
        let _ = self.feed.send(TransportEvent::Closed {
            code: None,
            reason: "gone".to_string(),
        });
    }
}

/// Scripts connect outcomes and counts the attempts.
#[derive(Clone)]
struct Controls {
    plans: Arc<Mutex<VecDeque<Plan>>>,
    connects: Arc<AtomicUsize>,
    dial: DialCell,
}

impl Controls {
    fn new() -> Self {
        Self {
            plans: Arc::new(Mutex::new(VecDeque::new())),
            connects: Arc::new(AtomicUsize::new(0)),
            dial: DialCell::default(),
        }
    }

    fn plan(&self, plan: Plan) {
        self.plans.lock().unwrap().push_back(plan);
    }

    fn succeed(&self) -> Socket {
        let (socket, wire) = socket_pair();
        self.plan(Plan::Succeed(wire));
        socket
    }

    fn stall(&self) -> (oneshot::Sender<()>, Socket) {
        let (release, trigger) = oneshot::channel();
        let (socket, wire) = socket_pair();
        self.plan(Plan::Stall(trigger, wire));
        (release, socket)
    }

    fn fail(&self, failure: Option<DialFailure>) {
        self.plan(Plan::Fail(failure));
    }

    fn connects(&self) -> usize {
        self.connects.load(Ordering::SeqCst)
    }

    fn start(&self, timings: Timings) -> BlockingClient {
        BlockingClient::spawn(
            URL.to_string(),
            "test-build".to_string(),
            ScriptedConnector {
                plans: self.plans.clone(),
                connects: self.connects.clone(),
                dial: self.dial.clone(),
            },
            self.dial.clone(),
            timings,
        )
    }

    /// A client whose first connect opened and whose handshake completed.
    fn attached(&self) -> (BlockingClient, Socket) {
        let socket = self.succeed();
        let client = self.start(timings());
        socket.handshake("sess-1");
        client
            .await_attachment(PATIENCE, &mut never())
            .expect("the first attachment");
        (client, socket)
    }
}

struct ScriptedConnector {
    plans: Arc<Mutex<VecDeque<Plan>>>,
    connects: Arc<AtomicUsize>,
    dial: DialCell,
}

impl TransportConnector for ScriptedConnector {
    type Conn = ScriptedConnection;

    async fn connect(&mut self, _url: &str) -> Result<ScriptedConnection, TransportError> {
        self.connects.fetch_add(1, Ordering::SeqCst);
        set_failure(&self.dial, None);
        let plan = self.plans.lock().unwrap().pop_front();
        let wire = match plan {
            Some(Plan::Succeed(wire)) => wire,
            Some(Plan::Stall(trigger, wire)) => {
                let _ = trigger.await;
                wire
            }
            Some(Plan::Fail(failure)) => {
                set_failure(&self.dial, failure);
                return Err(TransportError::new("scripted connect failed"));
            }
            // The script ran dry: a failure that recorded nothing.
            None => return Err(TransportError::new("no plan")),
        };
        Ok(ScriptedConnection { wire })
    }
}

struct ScriptedConnection {
    wire: Wire,
}

impl TransportConnection for ScriptedConnection {
    async fn send(&mut self, message: WireMessage) -> Result<(), TransportError> {
        let _ = self.wire.written.send(message);
        Ok(())
    }

    async fn next_event(&mut self) -> TransportEvent {
        match self.wire.incoming.recv().await {
            Some(event) => event,
            None => TransportEvent::Closed {
                code: None,
                reason: "the test dropped the socket".to_string(),
            },
        }
    }

    async fn close(&mut self) {
        self.wire.closed.store(true, Ordering::SeqCst);
    }
}

// ── helpers over a client ─────────────────────────────────────────────────

/// Subscribe `channel` and have the server open it.
fn hold(client: &BlockingClient, socket: &Socket, channel: &str) {
    let answer = thread::scope(|scope| {
        let call = scope.spawn(|| {
            client.subscribe(
                channel,
                depths(),
                ResumePolicy::Resume,
                PATIENCE,
                &mut never(),
            )
        });
        let ClientFrame::Subscribe {
            channel: sent,
            resume: None,
            ..
        } = socket.next_frame()
        else {
            panic!("expected a first Subscribe");
        };
        assert_eq!(sent, channel);
        socket.send(opened(channel, 0, None));
        call.join().unwrap()
    });
    assert_eq!(
        answer,
        Ok(SubscribeAnswer::Subscribed {
            replay_count: 0,
            gap: None
        })
    );
}

fn buffered(client: &BlockingClient) -> usize {
    client.shared.lock().buffer.len()
}

fn session(client: &BlockingClient) -> Option<String> {
    match client.status() {
        Status::Attached(facts) => Some(facts.session_id),
        _ => None,
    }
}

fn detached(client: &BlockingClient) -> bool {
    matches!(client.status(), Status::Detached { .. })
}

/// Drop the attachment and wait for the client to see it.
fn drop_and_wait(client: &BlockingClient, socket: &Socket) {
    socket.drop_connection();
    wait_until("the detach", || detached(client));
}

/// Open the next socket and wait until the client is attached on it.
fn reattach(client: &BlockingClient, socket: &Socket, id: &str) {
    socket.handshake(id);
    wait_until("the reattachment", || {
        session(client).as_deref() == Some(id)
    });
}

fn recv_now(client: &BlockingClient) -> Option<Event> {
    client
        .recv(Some(QUIET), &mut never())
        .expect("the attachment is not over")
}

fn delivery(event: Option<Event>) -> Delivery {
    match event {
        Some(Event::Delivery(delivery)) => *delivery,
        other => panic!("expected a delivery, got {other:?}"),
    }
}

#[test]
fn deliveries_arrive_in_wire_order_with_seq_and_dropped() {
    let controls = Controls::new();
    let (client, socket) = controls.attached();
    assert_eq!(client.facts().version, 5);
    assert_eq!(client.facts().participant_id, "attacher:bob");
    hold(&client, &socket, CHANNEL);

    socket.send(deliver(
        CHANNEL,
        vec![row(CHANNEL, 1, 0), row(CHANNEL, 2, 0)],
    ));
    socket.send(deliver(CHANNEL, vec![row(CHANNEL, 3, 4)]));

    let seen: Vec<(u64, u64, String)> = (0..3)
        .map(|_| {
            let delivery = delivery(client.recv(Some(PATIENCE), &mut never()).unwrap());
            assert_eq!(delivery.channel, CHANNEL);
            let text = delivery.envelope.body.as_text().unwrap().to_string();
            (delivery.seq, delivery.dropped, text)
        })
        .collect();
    assert_eq!(
        seen,
        vec![
            (1, 0, "message 1".to_string()),
            (2, 0, "message 2".to_string()),
            (3, 4, "message 3".to_string()),
        ]
    );
    assert_eq!(recv_now(&client), None);
}

#[test]
fn a_held_subscription_is_resent_with_its_cursor_and_settles_as_an_event() {
    let controls = Controls::new();
    let (client, socket) = controls.attached();
    hold(&client, &socket, CHANNEL);
    socket.send(deliver(CHANNEL, vec![row(CHANNEL, 7, 0)]));
    assert_eq!(
        delivery(client.recv(Some(PATIENCE), &mut never()).unwrap()).seq,
        7
    );

    let second = controls.succeed();
    drop_and_wait(&client, &socket);
    reattach(&client, &second, "sess-2");
    assert_eq!(
        second.next_frame(),
        ClientFrame::Subscribe {
            channel: CHANNEL.to_string(),
            push_depth: 4,
            retain_depth: 8,
            resume: Some(cursor("c7")),
        }
    );
    let gap = Some(GapInfo {
        reason: GapReason::BeyondRetained,
    });
    second.send(opened(CHANNEL, 2, gap));
    wait_until("the settlement", || buffered(&client) == 1);

    assert_eq!(
        recv_now(&client),
        Some(Event::Resubscribed {
            channel: CHANNEL.to_string(),
            replay_count: 2,
            gap,
        })
    );
    // The detach and the reattach, unread, show as the latest state alone.
    let Some(Event::Attached(facts)) = recv_now(&client) else {
        panic!("expected the reattachment");
    };
    assert_eq!(facts.session_id, "sess-2");
    assert_eq!(recv_now(&client), None);
}

#[test]
fn unavailable_on_resubscribe_drops_the_hold() {
    let controls = Controls::new();
    let (client, socket) = controls.attached();
    hold(&client, &socket, CHANNEL);

    let second = controls.succeed();
    drop_and_wait(&client, &socket);
    reattach(&client, &second, "sess-2");
    assert!(matches!(second.next_frame(), ClientFrame::Subscribe { .. }));
    second.send(unavailable(CHANNEL));
    wait_until("the settlement", || buffered(&client) == 1);

    assert_eq!(
        recv_now(&client),
        Some(Event::SubscriptionDropped {
            channel: CHANNEL.to_string()
        })
    );
    assert_eq!(
        client.unsubscribe(CHANNEL, &mut never()),
        Err(CallError::NotSubscribed)
    );
    hold(&client, &second, CHANNEL);
}

#[test]
fn a_subscribe_inside_the_grant_can_be_unavailable() {
    let controls = Controls::new();
    let (client, socket) = controls.attached();
    let answer = thread::scope(|scope| {
        let call = scope.spawn(|| {
            client.subscribe(
                CHANNEL,
                depths(),
                ResumePolicy::Cursorless,
                PATIENCE,
                &mut never(),
            )
        });
        assert!(matches!(socket.next_frame(), ClientFrame::Subscribe { .. }));
        socket.send(unavailable(CHANNEL));
        call.join().unwrap()
    });
    assert_eq!(answer, Ok(SubscribeAnswer::Unavailable));
    assert_eq!(
        client.unsubscribe(CHANNEL, &mut never()),
        Err(CallError::NotSubscribed)
    );
}

#[test]
fn a_subscribe_outstanding_at_a_detach_is_lost_and_not_resent() {
    let controls = Controls::new();
    let (client, socket) = controls.attached();
    let second = controls.succeed();
    let answer = thread::scope(|scope| {
        let call = scope.spawn(|| {
            client.subscribe(
                CHANNEL,
                depths(),
                ResumePolicy::Resume,
                PATIENCE,
                &mut never(),
            )
        });
        assert!(matches!(socket.next_frame(), ClientFrame::Subscribe { .. }));
        socket.drop_connection();
        call.join().unwrap()
    });
    assert_eq!(answer, Err(CallError::ConnectionLost { reason: gone() }));

    reattach(&client, &second, "sess-2");
    second.no_frame();
    assert_eq!(
        client.unsubscribe(CHANNEL, &mut never()),
        Err(CallError::NotSubscribed)
    );
}

#[test]
fn a_subscribe_that_timed_out_holds_nothing_whether_answered_late_or_never() {
    let controls = Controls::new();
    let (client, socket) = controls.attached();

    // Never answered.
    let short = Duration::from_millis(50);
    assert_eq!(
        client.subscribe(OTHER, depths(), ResumePolicy::Resume, short, &mut never()),
        Err(CallError::Timeout)
    );
    assert!(matches!(socket.next_frame(), ClientFrame::Subscribe { .. }));
    assert_eq!(
        client.unsubscribe(OTHER, &mut never()),
        Err(CallError::NotSubscribed)
    );

    // Answered after the caller left: the plane closes what the server opened.
    assert_eq!(
        client.subscribe(CHANNEL, depths(), ResumePolicy::Resume, short, &mut never()),
        Err(CallError::Timeout)
    );
    assert!(matches!(socket.next_frame(), ClientFrame::Subscribe { .. }));
    assert_eq!(
        client.unsubscribe(CHANNEL, &mut never()),
        Err(CallError::NotSubscribed)
    );
    socket.send(opened(CHANNEL, 1, None));
    assert_eq!(
        socket.next_frame(),
        ClientFrame::Unsubscribe {
            channel: CHANNEL.to_string()
        }
    );
    socket.send(deliver(CHANNEL, vec![row(CHANNEL, 1, 0)]));
    assert_eq!(recv_now(&client), None);
    assert_eq!(
        client.unsubscribe(CHANNEL, &mut never()),
        Err(CallError::NotSubscribed)
    );
}

#[test]
fn an_unsubscribe_while_attached_sends_purges_and_discards_stragglers() {
    let controls = Controls::new();
    let (client, socket) = controls.attached();
    hold(&client, &socket, CHANNEL);
    socket.send(deliver(
        CHANNEL,
        vec![row(CHANNEL, 1, 0), row(CHANNEL, 2, 0)],
    ));
    wait_until("two buffered deliveries", || buffered(&client) == 2);

    assert_eq!(client.unsubscribe(CHANNEL, &mut never()), Ok(()));
    assert_eq!(buffered(&client), 0);
    assert_eq!(
        socket.next_frame(),
        ClientFrame::Unsubscribe {
            channel: CHANNEL.to_string()
        }
    );
    // In flight across the Unsubscribe.
    socket.send(deliver(CHANNEL, vec![row(CHANNEL, 3, 0)]));
    assert_eq!(recv_now(&client), None);
    assert_eq!(client.status(), Status::Attached(client.facts()));
}

#[test]
fn an_unsubscribe_between_connect_attempts_succeeds_and_is_not_resent() {
    let controls = Controls::new();
    let socket = controls.succeed();
    let client = controls.start(Timings {
        initial_backoff: Duration::from_millis(400),
        ..timings()
    });
    socket.handshake("sess-1");
    client.await_attachment(PATIENCE, &mut never()).unwrap();
    hold(&client, &socket, CHANNEL);

    drop_and_wait(&client, &socket);
    assert_eq!(client.unsubscribe(CHANNEL, &mut never()), Ok(()));
    assert_eq!(controls.connects(), 1, "the unsubscribe ran in the backoff");

    let second = controls.succeed();
    reattach(&client, &second, "sess-2");
    second.no_frame();
}

#[test]
fn calls_during_a_connect_attempt_are_answered_without_touching_it() {
    let controls = Controls::new();
    let (client, socket) = controls.attached();
    hold(&client, &socket, CHANNEL);
    let (release, second) = controls.stall();
    socket.drop_connection();
    wait_until("the second connect", || controls.connects() == 2);

    assert_eq!(client.unsubscribe(CHANNEL, &mut never()), Ok(()));
    assert_eq!(
        client.publish(publish_request(OTHER), PATIENCE, &mut never()),
        Err(CallError::NotAttached)
    );
    assert_eq!(
        client.subscribe(
            OTHER,
            depths(),
            ResumePolicy::Resume,
            PATIENCE,
            &mut never()
        ),
        Err(CallError::NotAttached)
    );

    release.send(()).unwrap();
    reattach(&client, &second, "sess-2");
    second.no_frame();
    assert_eq!(controls.connects(), 2, "the attempt in progress completed");
}

#[test]
fn an_unsubscribe_while_a_resubscribe_is_unanswered_follows_the_answer() {
    let controls = Controls::new();
    let (client, socket) = controls.attached();
    hold(&client, &socket, CHANNEL);
    let second = controls.succeed();
    drop_and_wait(&client, &socket);
    reattach(&client, &second, "sess-2");
    assert!(matches!(second.next_frame(), ClientFrame::Subscribe { .. }));

    assert_eq!(client.unsubscribe(CHANNEL, &mut never()), Ok(()));
    second.no_frame();
    second.send(opened(CHANNEL, 0, None));
    assert_eq!(
        second.next_frame(),
        ClientFrame::Unsubscribe {
            channel: CHANNEL.to_string()
        }
    );
    assert!(matches!(recv_now(&client), Some(Event::Attached(_))));
    assert_eq!(recv_now(&client), None);
}

#[test]
fn caller_errors_are_refused_before_the_wire() {
    let controls = Controls::new();
    let (client, socket) = controls.attached();
    hold(&client, &socket, CHANNEL);
    let subscribe = |channel: &str, depths: SubscriptionDepths| {
        client.subscribe(
            channel,
            depths,
            ResumePolicy::Resume,
            PATIENCE,
            &mut never(),
        )
    };

    assert_eq!(
        subscribe(CHANNEL, depths()),
        Err(CallError::AlreadySubscribed)
    );
    assert_eq!(
        client.unsubscribe(OTHER, &mut never()),
        Err(CallError::NotSubscribed)
    );
    assert_eq!(
        subscribe("local:lab.scratch", depths()),
        Err(CallError::LocalChannel)
    );
    assert_eq!(
        client.publish(publish_request("local:lab.scratch"), PATIENCE, &mut never()),
        Err(CallError::LocalChannel)
    );
    let none = SubscriptionDepths {
        push_depth: 0,
        retain_depth: 0,
    };
    assert_eq!(subscribe(OTHER, none), Err(CallError::NoDepth));
    socket.no_frame();
}

/// Publish on another thread, answer as the server, and return the answer.
fn publish_answered(
    client: &BlockingClient,
    socket: &Socket,
    outcome: WireOutcome,
) -> Result<PublishOutcome, CallError> {
    thread::scope(|scope| {
        let call = scope.spawn(|| client.publish(publish_request(OTHER), PATIENCE, &mut never()));
        let ClientFrame::Publish { correlation, .. } = socket.next_frame() else {
            panic!("expected a Publish");
        };
        socket.send(ServerFrame::PublishResult {
            correlation,
            outcome,
        });
        call.join().unwrap()
    })
}

#[test]
fn a_publish_returns_each_outcome_the_server_answers() {
    let controls = Controls::new();
    let (client, socket) = controls.attached();
    for (wire, outcome) in [
        (WireOutcome::Ok, PublishOutcome::Ok),
        (WireOutcome::Failed, PublishOutcome::Failed),
        (WireOutcome::RateLimited, PublishOutcome::RateLimited),
        (
            WireOutcome::BodyTooLarge {
                len: 9000,
                max: 4096,
            },
            PublishOutcome::BodyTooLarge {
                len: 9000,
                max: 4096,
            },
        ),
        (
            WireOutcome::TextOnlyChannel,
            PublishOutcome::TextOnlyChannel,
        ),
    ] {
        assert_eq!(publish_answered(&client, &socket, wire), Ok(outcome));
    }
}

#[test]
fn two_publishes_in_flight_each_get_their_own_answer() {
    let controls = Controls::new();
    let (client, socket) = controls.attached();
    let (first, second) = thread::scope(|scope| {
        let first =
            scope.spawn(|| client.publish(publish_request(CHANNEL), PATIENCE, &mut never()));
        let second = scope.spawn(|| client.publish(publish_request(OTHER), PATIENCE, &mut never()));
        let mut sent = Vec::new();
        for _ in 0..2 {
            let ClientFrame::Publish {
                channel,
                correlation,
                ..
            } = socket.next_frame()
            else {
                panic!("expected a Publish");
            };
            sent.push((channel, correlation));
        }
        // Answered in the reverse of the order they were sent.
        for (channel, correlation) in sent.into_iter().rev() {
            let outcome = if channel == CHANNEL {
                WireOutcome::Ok
            } else {
                WireOutcome::RateLimited
            };
            socket.send(ServerFrame::PublishResult {
                correlation,
                outcome,
            });
        }
        (first.join().unwrap(), second.join().unwrap())
    });
    assert_eq!(first, Ok(PublishOutcome::Ok));
    assert_eq!(second, Ok(PublishOutcome::RateLimited));
}

#[test]
fn a_publish_outstanding_at_a_detach_is_lost_and_one_after_it_is_not_attached() {
    let controls = Controls::new();
    let (client, socket) = controls.attached();
    let answer = thread::scope(|scope| {
        let call = scope.spawn(|| client.publish(publish_request(OTHER), PATIENCE, &mut never()));
        assert!(matches!(socket.next_frame(), ClientFrame::Publish { .. }));
        socket.drop_connection();
        call.join().unwrap()
    });
    assert_eq!(answer, Err(CallError::ConnectionLost { reason: gone() }));
    wait_until("the detach", || detached(&client));
    assert_eq!(
        client.publish(publish_request(OTHER), PATIENCE, &mut never()),
        Err(CallError::NotAttached)
    );
}

#[test]
fn a_lost_connection_reads_as_text() {
    let lost = |reason| CallError::ConnectionLost { reason }.to_string();
    assert_eq!(
        lost(DetachReason::TransportClosed {
            code: Some(1011),
            reason: "bad frame".to_string(),
        }),
        "the attachment dropped before the answer: closed with code 1011: bad frame"
    );
    assert_eq!(
        lost(gone()),
        "the attachment dropped before the answer: gone"
    );
    assert_eq!(
        lost(DetachReason::LivenessTimeout),
        "the attachment dropped before the answer: \
         no frame from the server within the liveness window"
    );
}

#[test]
fn attach_returns_once_attached_though_the_attachment_has_since_dropped() {
    let controls = Controls::new();
    let socket = controls.succeed();
    let client = controls.start(timings());
    socket.handshake("sess-1");
    wait_until("the attachment", || {
        matches!(
            client.shared.lock().standing,
            Standing::Status(Status::Attached(_))
        )
    });
    socket.drop_connection();
    wait_until("the detach", || detached(&client));
    assert_eq!(client.await_attachment(PATIENCE, &mut never()), Ok(()));
    assert_eq!(
        client.status(),
        Status::Detached {
            reason: gone(),
            last_dial_failure: None,
        }
    );
}

#[test]
fn a_drop_before_the_first_welcome_is_a_failed_attempt() {
    let controls = Controls::new();
    let first = controls.succeed();
    let second = controls.succeed();
    let client = controls.start(timings());
    first.hello(VersionRange::exactly(5));
    first.drop_connection();
    wait_until("the second connect", || controls.connects() == 2);
    assert!(matches!(
        client.shared.lock().standing,
        Standing::Attaching {
            last_dial_failure: None
        }
    ));

    second.handshake("sess-2");
    assert_eq!(client.await_attachment(PATIENCE, &mut never()), Ok(()));
    assert_eq!(session(&client).as_deref(), Some("sess-2"));
    assert_eq!(recv_now(&client), None);
}

#[test]
fn an_attempt_that_opens_clears_the_earlier_dial_failure() {
    let controls = Controls::new();
    controls.fail(Some(DialFailure::Io {
        detail: "connection refused".to_string(),
    }));
    let opened = controls.succeed();
    let (_release, _stalled) = controls.stall();
    let client = controls.start(timings());
    opened.hello(VersionRange::exactly(5));
    opened.drop_connection();
    wait_until("the third connect", || controls.connects() == 3);
    assert_eq!(
        client.await_attachment(Duration::from_millis(100), &mut never()),
        Err(AttachError::Timeout { last_failure: None })
    );
}

#[test]
fn a_connect_past_its_deadline_is_a_timed_out_dial() {
    let controls = Controls::new();
    // Held unreleased, each attempt outlives its deadline; the script runs dry
    // only after all three have.
    let _stalls: Vec<_> = (0..3).map(|_| controls.stall()).collect();
    let client = controls.start(Timings {
        connect_timeout: Duration::from_millis(200),
        ..timings()
    });
    wait_until("the timed-out dial", || {
        matches!(
            client.shared.lock().standing,
            Standing::Attaching {
                last_dial_failure: Some(DialFailure::TimedOut)
            }
        )
    });
    assert_eq!(
        client.await_attachment(Duration::from_millis(50), &mut never()),
        Err(AttachError::Timeout {
            last_failure: Some(DialFailure::TimedOut)
        })
    );
}

#[test]
fn a_full_buffer_stops_the_socket_but_not_the_commands() {
    let controls = Controls::new();
    let (client, socket) = controls.attached();
    hold(&client, &socket, CHANNEL);
    hold(&client, &socket, OTHER);

    let full = (1..=EVENT_CAPACITY as u64)
        .map(|seq| row(CHANNEL, seq, 0))
        .collect();
    socket.send(deliver(CHANNEL, full));
    wait_until("a full buffer", || buffered(&client) == EVENT_CAPACITY);
    let next = EVENT_CAPACITY as u64 + 1;
    socket.send(deliver(CHANNEL, vec![row(CHANNEL, next, 0)]));
    thread::sleep(QUIET);
    assert_eq!(buffered(&client), EVENT_CAPACITY, "the socket is not read");

    assert_eq!(client.unsubscribe(OTHER, &mut never()), Ok(()));
    assert_eq!(
        socket.next_frame(),
        ClientFrame::Unsubscribe {
            channel: OTHER.to_string()
        }
    );

    assert_eq!(delivery(recv_now(&client)).seq, 1);
    wait_until("the socket read again", || {
        buffered(&client) == EVENT_CAPACITY
    });
    let last = (2..=next)
        .map(|_| delivery(recv_now(&client)).seq)
        .last()
        .unwrap();
    assert_eq!(last, next);
    assert_eq!(recv_now(&client), None);
}

#[test]
fn a_detach_and_reattach_unread_show_as_one_attached() {
    let controls = Controls::new();
    let (client, socket) = controls.attached();
    let second = controls.succeed();
    drop_and_wait(&client, &socket);
    reattach(&client, &second, "sess-2");

    let Some(Event::Attached(facts)) = recv_now(&client) else {
        panic!("expected the reattachment");
    };
    assert_eq!(facts.session_id, "sess-2");
    assert_eq!(recv_now(&client), None);
}

#[test]
fn a_detach_read_before_the_reattach_is_shown() {
    let controls = Controls::new();
    let (client, socket) = controls.attached();
    drop_and_wait(&client, &socket);
    let Some(Event::Detached { reason, .. }) = recv_now(&client) else {
        panic!("expected the detach");
    };
    assert_eq!(reason, gone());
}

/// Open the next socket, take the resubscribe, and drop it unanswered.
fn futile_attachment(client: &BlockingClient, controls: &Controls, id: &str) {
    let socket = controls.succeed();
    reattach(client, &socket, id);
    assert!(matches!(socket.next_frame(), ClientFrame::Subscribe { .. }));
    socket.drop_connection();
    wait_until("the drop", || session(client).is_none());
}

#[test]
fn three_attachments_answered_nothing_end_with_futile() {
    let controls = Controls::new();
    let (client, socket) = controls.attached();
    hold(&client, &socket, CHANNEL);
    drop_and_wait(&client, &socket);

    futile_attachment(&client, &controls, "sess-2");
    futile_attachment(&client, &controls, "sess-3");
    assert!(detached(&client), "two are not enough");
    futile_attachment(&client, &controls, "sess-4");

    let futile = EndCause::Futile { attachments: 3 };
    wait_until("the end", || {
        client.status() == Status::Ended(futile.clone())
    });
    assert_eq!(
        client.publish(publish_request(OTHER), PATIENCE, &mut never()),
        Err(CallError::Ended(futile))
    );
}

#[test]
fn an_answered_attachment_resets_the_futile_run() {
    let controls = Controls::new();
    let (client, socket) = controls.attached();
    hold(&client, &socket, CHANNEL);
    drop_and_wait(&client, &socket);

    futile_attachment(&client, &controls, "sess-2");
    futile_attachment(&client, &controls, "sess-3");
    let answered = controls.succeed();
    reattach(&client, &answered, "sess-4");
    assert!(matches!(
        answered.next_frame(),
        ClientFrame::Subscribe { .. }
    ));
    answered.send(opened(CHANNEL, 0, None));
    wait_until("the settlement", || buffered(&client) == 1);
    drop_and_wait(&client, &answered);
    futile_attachment(&client, &controls, "sess-5");
    futile_attachment(&client, &controls, "sess-6");

    let last = controls.succeed();
    reattach(&client, &last, "sess-7");
    assert!(matches!(client.status(), Status::Attached(_)));
}

#[test]
fn idle_drops_are_not_futile() {
    let controls = Controls::new();
    let (client, socket) = controls.attached();
    drop_and_wait(&client, &socket);
    for id in ["sess-2", "sess-3", "sess-4"] {
        let socket = controls.succeed();
        reattach(&client, &socket, id);
        drop_and_wait(&client, &socket);
    }
    let last = controls.succeed();
    reattach(&client, &last, "sess-5");
}

#[test]
fn an_upgrade_rejected_401_ends_with_credential_rejected() {
    let controls = Controls::new();
    controls.fail(Some(DialFailure::UpgradeRejected { status: 401 }));
    controls.succeed();
    let client = controls.start(timings());
    assert_eq!(
        client.await_attachment(PATIENCE, &mut never()),
        Err(AttachError::Ended(EndCause::CredentialRejected {
            status: 401
        }))
    );
    assert_eq!(controls.connects(), 1, "no second dial");
}

#[test]
fn an_upgrade_rejected_503_is_retried() {
    let controls = Controls::new();
    controls.fail(Some(DialFailure::UpgradeRejected { status: 503 }));
    let socket = controls.succeed();
    let client = controls.start(timings());
    socket.handshake("sess-1");
    client.await_attachment(PATIENCE, &mut never()).unwrap();
    assert_eq!(controls.connects(), 2);
}

#[test]
fn no_version_in_common_ends_with_incompatible() {
    let controls = Controls::new();
    let socket = controls.succeed();
    let client = controls.start(timings());
    socket.hello(VersionRange::exactly(4));
    assert_eq!(
        client.await_attachment(PATIENCE, &mut never()),
        Err(AttachError::Ended(EndCause::Incompatible {
            ours: VersionRange::exactly(5),
            theirs: VersionRange::exactly(4),
        }))
    );
}

fn assert_fatal(client: &BlockingClient) {
    wait_until("a fatal end", || {
        matches!(client.status(), Status::Ended(EndCause::Fatal { .. }))
    });
    assert!(matches!(
        client.publish(publish_request(OTHER), PATIENCE, &mut never()),
        Err(CallError::Ended(EndCause::Fatal { .. }))
    ));
}

#[test]
fn a_batch_result_is_fatal() {
    let controls = Controls::new();
    let (client, socket) = controls.attached();
    socket.send(ServerFrame::PublishBatchResult {
        correlation: 9,
        outcome: PublishBatchOutcome::Ok,
    });
    assert_fatal(&client);
}

#[test]
fn a_blob_answer_is_fatal() {
    let controls = Controls::new();
    let (client, socket) = controls.attached();
    // v5 has no blob frames, so only a v6 writer can produce one.
    socket.send_raw(
        Codec::negotiated(6).encode_server(&ServerFrame::BlobReadResult {
            correlation: 1,
            outcome: BlobReadOutcome::UnknownHandle,
        }),
    );
    assert_fatal(&client);
}

#[test]
fn attachment_not_held_is_fatal_and_ends_the_publish() {
    let controls = Controls::new();
    let (client, socket) = controls.attached();
    let answer = thread::scope(|scope| {
        let call = scope.spawn(|| client.publish(publish_request(OTHER), PATIENCE, &mut never()));
        let ClientFrame::Publish { correlation, .. } = socket.next_frame() else {
            panic!("expected a Publish");
        };
        // v5 cannot write this outcome, so only a v6 writer can produce it.
        socket.send_raw(
            Codec::negotiated(6).encode_server(&ServerFrame::PublishResult {
                correlation,
                outcome: WireOutcome::AttachmentNotHeld,
            }),
        );
        call.join().unwrap()
    });
    assert!(
        matches!(answer, Err(CallError::Ended(EndCause::Fatal { .. }))),
        "{answer:?}"
    );
    assert_fatal(&client);
}

#[test]
fn attach_times_out_with_the_last_dial_failure() {
    let controls = Controls::new();
    let refused = DialFailure::Io {
        detail: "connection refused".to_string(),
    };
    for _ in 0..200 {
        controls.fail(Some(refused.clone()));
    }
    let client = controls.start(timings());
    assert_eq!(
        client.await_attachment(Duration::from_millis(300), &mut never()),
        Err(AttachError::Timeout {
            last_failure: Some(refused)
        })
    );
    assert_eq!(client.status(), Status::Ended(EndCause::Closed));
}

#[test]
fn close_during_a_connect_attempt_returns_promptly() {
    let controls = Controls::new();
    let (client, socket) = controls.attached();
    let (_release, _second) = controls.stall();
    socket.drop_connection();
    wait_until("the second connect", || controls.connects() == 2);

    let started = Instant::now();
    client.close();
    assert!(started.elapsed() < Duration::from_secs(1));
    assert_eq!(client.status(), Status::Ended(EndCause::Closed));
    assert_eq!(
        client.recv(Some(QUIET), &mut never()),
        Err(RecvError::Ended(EndCause::Closed))
    );
}

#[test]
fn close_closes_the_socket() {
    let controls = Controls::new();
    let (client, socket) = controls.attached();
    client.close();
    assert!(socket.closed.load(Ordering::SeqCst));
    assert_eq!(
        client.unsubscribe(CHANNEL, &mut never()),
        Err(CallError::Ended(EndCause::Closed))
    );
}

#[test]
fn an_interrupted_wait_cancels_its_request() {
    let controls = Controls::new();
    let (client, socket) = controls.attached();
    let mut calls = 0;
    let mut interrupt = || {
        calls += 1;
        calls > 1
    };
    assert_eq!(
        client.subscribe(
            CHANNEL,
            depths(),
            ResumePolicy::Resume,
            PATIENCE,
            &mut interrupt
        ),
        Err(CallError::Interrupted)
    );
    assert!(matches!(socket.next_frame(), ClientFrame::Subscribe { .. }));
    assert_eq!(
        client.unsubscribe(CHANNEL, &mut never()),
        Err(CallError::NotSubscribed)
    );
}
