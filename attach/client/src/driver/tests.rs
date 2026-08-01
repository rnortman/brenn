//! The I/O layer, against a scripted transport.
//!
//! Nothing here names a component, instance, port or pixel: what is under test
//! is a connector, a socket, three deadlines, and the frames that cross them.

use super::*;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use brenn_attach_proto::{SUPPORTED_VERSIONS, VersionRange};
use futures_channel::mpsc;
use futures_util::StreamExt;

use crate::conn::{AttachmentFacts, DetachReason};
use crate::transport::TransportError;

const URL: &str = "wss://host/attach";

fn config() -> ConnConfig {
    ConnConfig {
        url: URL.to_string(),
        ident: "test-build".to_string(),
        initial_backoff: Duration::from_secs(3),
        max_backoff: Duration::from_secs(60),
        connect_timeout: Duration::from_secs(15),
        liveness_multiplier: 3,
        backoff_jitter_seed: 0,
        terminal_close_code: Some(3001),
    }
}

fn server_hello() -> String {
    serde_json::to_string(&ServerFrame::Hello {
        versions: SUPPORTED_VERSIONS,
        ident: "peer".to_string(),
    })
    .unwrap()
}

fn welcome() -> String {
    serde_json::to_string(&ServerFrame::Welcome {
        version: SUPPORTED_VERSIONS.max,
        participant_id: "attacher:demo".to_string(),
        session_id: "sess-1".to_string(),
        heartbeat_secs: 20,
        max_body_bytes: 4096,
        max_frame_bytes: 8192,
        alert_granted: false,
    })
    .unwrap()
}

// ── the scripted transport ────────────────────────────────────────────────

enum Plan {
    Fail,
    Succeed {
        incoming: mpsc::UnboundedReceiver<TransportEvent>,
        closed: Arc<AtomicBool>,
        fail_writes: Arc<AtomicUsize>,
    },
    /// The connect resolves only once the returned trigger fires — an attempt
    /// still in flight while the embedder does something else.
    Stall {
        release: futures_channel::oneshot::Receiver<()>,
        incoming: mpsc::UnboundedReceiver<TransportEvent>,
        closed: Arc<AtomicBool>,
    },
}

/// Scripts connect outcomes and records what the driver wrote and closed.
#[derive(Clone)]
struct Controls {
    plans: Arc<Mutex<VecDeque<Plan>>>,
    written: Arc<Mutex<Vec<String>>>,
    connects: Arc<AtomicUsize>,
}

impl Controls {
    fn new() -> Self {
        Self {
            plans: Arc::new(Mutex::new(VecDeque::new())),
            written: Arc::new(Mutex::new(Vec::new())),
            connects: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Queue a connect that succeeds; hands back the event feed for that socket
    /// and its close flag.
    fn succeed(&self) -> (mpsc::UnboundedSender<TransportEvent>, Arc<AtomicBool>) {
        let (tx, closed, _) = self.succeed_failing_writes(0);
        (tx, closed)
    }

    /// Queue a connect that succeeds but whose first `n` writes fail. The counter
    /// comes back too, so a test that needs a live attachment first can arm the
    /// failure after the handshake rather than during it.
    fn succeed_failing_writes(
        &self,
        n: usize,
    ) -> (
        mpsc::UnboundedSender<TransportEvent>,
        Arc<AtomicBool>,
        Arc<AtomicUsize>,
    ) {
        let (tx, rx) = mpsc::unbounded();
        let closed = Arc::new(AtomicBool::new(false));
        let fail_writes = Arc::new(AtomicUsize::new(n));
        self.plans.lock().unwrap().push_back(Plan::Succeed {
            incoming: rx,
            closed: closed.clone(),
            fail_writes: fail_writes.clone(),
        });
        (tx, closed, fail_writes)
    }

    /// Queue a connect that hangs until the returned trigger fires.
    fn stall(
        &self,
    ) -> (
        futures_channel::oneshot::Sender<()>,
        mpsc::UnboundedSender<TransportEvent>,
    ) {
        let (release_tx, release_rx) = futures_channel::oneshot::channel();
        let (tx, rx) = mpsc::unbounded();
        self.plans.lock().unwrap().push_back(Plan::Stall {
            release: release_rx,
            incoming: rx,
            closed: Arc::new(AtomicBool::new(false)),
        });
        (release_tx, tx)
    }

    fn fail(&self) {
        self.plans.lock().unwrap().push_back(Plan::Fail);
    }

    fn connector(&self) -> ScriptedConnector {
        ScriptedConnector {
            plans: self.plans.clone(),
            written: self.written.clone(),
            connects: self.connects.clone(),
        }
    }

    fn written(&self) -> Vec<String> {
        self.written.lock().unwrap().clone()
    }

    fn frames(&self) -> Vec<ClientFrame> {
        self.written()
            .iter()
            .map(|text| serde_json::from_str(text).expect("the driver writes client frames"))
            .collect()
    }

    fn connects(&self) -> usize {
        self.connects.load(Ordering::SeqCst)
    }
}

struct ScriptedConnector {
    plans: Arc<Mutex<VecDeque<Plan>>>,
    written: Arc<Mutex<Vec<String>>>,
    connects: Arc<AtomicUsize>,
}

impl TransportConnector for ScriptedConnector {
    type Conn = ScriptedConnection;

    async fn connect(&mut self, _url: &str) -> Result<ScriptedConnection, TransportError> {
        self.connects.fetch_add(1, Ordering::SeqCst);
        let plan = self.plans.lock().unwrap().pop_front();
        match plan {
            Some(Plan::Succeed {
                incoming,
                closed,
                fail_writes,
            }) => Ok(ScriptedConnection {
                incoming,
                written: self.written.clone(),
                closed,
                fail_writes,
            }),
            Some(Plan::Stall {
                release,
                incoming,
                closed,
            }) => {
                let _ = release.await;
                Ok(ScriptedConnection {
                    incoming,
                    written: self.written.clone(),
                    closed,
                    fail_writes: Arc::new(AtomicUsize::new(0)),
                })
            }
            // A scripted failure, or the script ran dry: a retryable connect
            // error either way, never a panic.
            Some(Plan::Fail) | None => Err(TransportError::new("scripted connect refused")),
        }
    }
}

struct ScriptedConnection {
    incoming: mpsc::UnboundedReceiver<TransportEvent>,
    written: Arc<Mutex<Vec<String>>>,
    closed: Arc<AtomicBool>,
    fail_writes: Arc<AtomicUsize>,
}

impl TransportConnection for ScriptedConnection {
    async fn send_text(&mut self, text: String) -> Result<(), TransportError> {
        if self.fail_writes.load(Ordering::SeqCst) > 0 {
            self.fail_writes.fetch_sub(1, Ordering::SeqCst);
            return Err(TransportError::new("scripted write failed"));
        }
        self.written.lock().unwrap().push(text);
        Ok(())
    }

    async fn next_event(&mut self) -> TransportEvent {
        match self.incoming.next().await {
            Some(event) => event,
            // The test dropped the feed: model it as a peer close.
            None => TransportEvent::Closed {
                code: None,
                reason: String::new(),
            },
        }
    }

    async fn close(&mut self) {
        self.closed.store(true, Ordering::SeqCst);
    }
}

/// A started driver whose first connect has succeeded and whose handshake has
/// completed: the live state most of these tests begin in.
async fn attached(
    controls: &Controls,
) -> (
    AttachDriver<ScriptedConnector>,
    mpsc::UnboundedSender<TransportEvent>,
    Arc<AtomicBool>,
) {
    let (driver, feed, closed, _) = attached_armable(controls).await;
    (driver, feed, closed)
}

/// [`attached`], handing back the socket's write-failure counter as well: a test
/// that needs a write to fail on a *live* attachment arms it after the handshake.
async fn attached_armable(
    controls: &Controls,
) -> (
    AttachDriver<ScriptedConnector>,
    mpsc::UnboundedSender<TransportEvent>,
    Arc<AtomicBool>,
    Arc<AtomicUsize>,
) {
    let (feed, closed, fail_writes) = controls.succeed_failing_writes(0);
    let mut driver = AttachDriver::new(config(), controls.connector());
    driver.start().await;
    let url = driver
        .take_pending_connect()
        .expect("a connect was asked for");
    let opened = driver.connect(&url).await;
    driver.on_input(opened).await;
    feed.unbounded_send(TransportEvent::Text(server_hello()))
        .unwrap();
    let hello = driver.wait().await;
    let IoEvent::Conn(hello) = hello else {
        panic!("expected the peer hello");
    };
    driver.on_input(hello).await;
    feed.unbounded_send(TransportEvent::Text(welcome()))
        .unwrap();
    let IoEvent::Conn(welcome) = driver.wait().await else {
        panic!("expected the welcome");
    };
    let step = driver.on_input(welcome).await;
    assert!(matches!(step.events.as_slice(), [ConnEvent::Attached(_)]));
    (driver, feed, closed, fail_writes)
}

#[tokio::test]
async fn start_asks_for_a_connect_and_arms_the_handshake_deadline() {
    let controls = Controls::new();
    let mut driver = AttachDriver::new(config(), controls.connector());
    let step = driver.start().await;

    assert!(step.events.is_empty());
    assert_eq!(driver.take_pending_connect().as_deref(), Some(URL));
    assert_eq!(controls.connects(), 0);
    assert_eq!(driver.state(), ConnState::Connecting);
}

#[tokio::test]
async fn an_opened_socket_carries_this_end_s_hello_without_waiting() {
    let controls = Controls::new();
    let (_feed, _closed) = controls.succeed();
    let mut driver = AttachDriver::new(config(), controls.connector());
    driver.start().await;
    let url = driver.take_pending_connect().unwrap();

    let opened = driver.connect(&url).await;
    assert_eq!(opened, ConnInput::Opened);
    driver.on_input(opened).await;

    assert_eq!(controls.connects(), 1);
    assert!(matches!(
        controls.frames().as_slice(),
        [ClientFrame::Hello { versions, ident }]
            if *versions == SUPPORTED_VERSIONS && ident == "test-build"
    ));
}

#[tokio::test]
async fn a_refused_connect_is_retryable_and_asks_for_another_attempt() {
    let controls = Controls::new();
    controls.fail();
    let mut driver = AttachDriver::new(config(), controls.connector());
    driver.start().await;
    let url = driver.take_pending_connect().unwrap();

    let failed = driver.connect(&url).await;
    assert_eq!(failed, ConnInput::ConnectFailed);
    let step = driver.on_input(failed).await;

    assert!(step.events.is_empty());
    assert_eq!(driver.state(), ConnState::Backoff);
    assert!(driver.take_pending_connect().is_none());
}

#[tokio::test(start_paused = true)]
async fn a_stalled_connect_loses_the_race_to_the_handshake_deadline() {
    let controls = Controls::new();
    let (_release, _feed) = controls.stall();
    let mut driver = AttachDriver::new(config(), controls.connector());
    driver.start().await;
    let url = driver.take_pending_connect().unwrap();

    let timed_out = driver.connect(&url).await;
    assert_eq!(timed_out, ConnInput::Tick);
    driver.on_input(timed_out).await;
    assert_eq!(driver.state(), ConnState::Backoff);
}

#[tokio::test(start_paused = true)]
async fn the_backoff_deadline_asks_for_the_next_attempt() {
    let controls = Controls::new();
    controls.fail();
    let mut driver = AttachDriver::new(config(), controls.connector());
    driver.start().await;
    let url = driver.take_pending_connect().unwrap();
    let failed = driver.connect(&url).await;
    driver.on_input(failed).await;

    let IoEvent::Conn(tick) = driver.wait().await else {
        panic!("expected the backoff tick");
    };
    assert_eq!(tick, ConnInput::Tick);
    driver.on_input(tick).await;

    assert_eq!(driver.take_pending_connect().as_deref(), Some(URL));
}

#[tokio::test]
async fn a_plane_s_frame_is_handed_back_rather_than_consumed() {
    let controls = Controls::new();
    let (mut driver, feed, _closed) = attached(&controls).await;

    let result = serde_json::to_string(&ServerFrame::PublishResult {
        correlation: Some(7),
        outcome: brenn_attach_proto::PublishOutcome::Ok,
    })
    .unwrap();
    feed.unbounded_send(TransportEvent::Text(result)).unwrap();
    let IoEvent::Conn(input) = driver.wait().await else {
        panic!("expected the frame");
    };
    let step = driver.on_input(input).await;

    assert!(step.events.is_empty());
    assert!(matches!(
        step.routed,
        Some(ServerFrame::PublishResult {
            correlation: Some(7),
            ..
        })
    ));
}

#[tokio::test]
async fn the_connection_s_own_frames_are_consumed_and_route_nothing() {
    let controls = Controls::new();
    let (mut driver, feed, _closed) = attached(&controls).await;

    let heartbeat = serde_json::to_string(&ServerFrame::Heartbeat).unwrap();
    feed.unbounded_send(TransportEvent::Text(heartbeat))
        .unwrap();
    let IoEvent::Conn(input) = driver.wait().await else {
        panic!("expected the heartbeat");
    };
    let step = driver.on_input(input).await;

    assert!(step.routed.is_none());
    assert!(step.events.is_empty());
}

#[tokio::test]
async fn a_peer_close_drops_the_transport_before_the_connection_reacts() {
    let controls = Controls::new();
    let (mut driver, feed, _closed) = attached(&controls).await;

    feed.unbounded_send(TransportEvent::Closed {
        code: Some(1006),
        reason: "gone".to_string(),
    })
    .unwrap();
    let IoEvent::Conn(input) = driver.wait().await else {
        panic!("expected the close");
    };
    assert_eq!(
        input,
        ConnInput::Disconnected {
            code: Some(1006),
            reason: "gone".to_string(),
        }
    );
    let step = driver.on_input(input).await;

    assert_eq!(
        step.events,
        vec![ConnEvent::Detached {
            reason: DetachReason::TransportClosed {
                code: Some(1006),
                reason: "gone".to_string(),
            }
        }]
    );
    assert!(!driver.is_active());
}

#[tokio::test]
async fn a_transport_failure_carries_no_close_code() {
    let controls = Controls::new();
    let (mut driver, feed, _closed) = attached(&controls).await;

    feed.unbounded_send(TransportEvent::Failed("reset by peer".to_string()))
        .unwrap();
    let IoEvent::Conn(input) = driver.wait().await else {
        panic!("expected the failure");
    };
    assert_eq!(
        input,
        ConnInput::Disconnected {
            code: None,
            // The description is the only diagnosis a failure has, so it
            // passes through as the reason.
            reason: "reset by peer".to_string(),
        }
    );
}

/// The protocol is JSON text, so a binary frame is the peer's violation — and the
/// one input this end is least entitled to swallow.
#[tokio::test]
async fn a_binary_frame_reaches_the_connection_as_one() {
    let controls = Controls::new();
    let (mut driver, feed, _closed) = attached(&controls).await;

    feed.unbounded_send(TransportEvent::Binary(vec![0]))
        .unwrap();
    assert_eq!(
        driver.wait().await,
        IoEvent::Conn(ConnInput::BinaryFrame),
        "the driver maps it rather than dropping it"
    );
}

#[tokio::test]
async fn the_terminal_close_code_stops_the_schedule() {
    let controls = Controls::new();
    let (mut driver, feed, _closed) = attached(&controls).await;

    feed.unbounded_send(TransportEvent::Closed {
        code: Some(3001),
        reason: "stale build".to_string(),
    })
    .unwrap();
    let IoEvent::Conn(input) = driver.wait().await else {
        panic!("expected the close");
    };
    let step = driver.on_input(input).await;

    assert_eq!(
        step.events,
        vec![ConnEvent::PeerClosedTerminal {
            code: 3001,
            reason: "stale build".to_string(),
        }]
    );
    assert!(driver.is_terminal());
    assert!(driver.take_pending_connect().is_none());
}

#[tokio::test(start_paused = true)]
async fn inbound_silence_reaps_the_attachment_and_closes_its_socket() {
    let controls = Controls::new();
    let (mut driver, _feed, closed) = attached(&controls).await;

    // 3 × 20s of silence: the tick that fires past the deadline reaps.
    let IoEvent::Conn(tick) = driver.wait().await else {
        panic!("expected the liveness tick");
    };
    let step = driver.on_input(tick).await;

    assert_eq!(
        step.events,
        vec![ConnEvent::Detached {
            reason: DetachReason::LivenessTimeout
        }]
    );
    assert!(closed.load(Ordering::SeqCst));
    assert_eq!(driver.state(), ConnState::Backoff);
}

#[tokio::test]
async fn planes_frames_are_written_in_order() {
    let controls = Controls::new();
    let (mut driver, _feed, _closed) = attached(&controls).await;

    let step = driver
        .send(vec![
            ClientFrame::Unsubscribe {
                channel: "ephemeral:one".to_string(),
            },
            ClientFrame::Unsubscribe {
                channel: "ephemeral:two".to_string(),
            },
        ])
        .await;

    assert!(step.events.is_empty());
    let channels: Vec<String> = controls
        .frames()
        .into_iter()
        .filter_map(|frame| match frame {
            ClientFrame::Unsubscribe { channel } => Some(channel),
            _ => None,
        })
        .collect();
    assert_eq!(channels, vec!["ephemeral:one", "ephemeral:two"]);
}

/// The rest of a batch was computed against the transport that just died, so it
/// is dropped rather than written to a corpse — and dropping it is a return, not
/// the no-transport panic that sits one arm below.
#[tokio::test]
async fn a_failed_write_drops_the_rest_of_the_batch_rather_than_panicking() {
    let controls = Controls::new();
    let (mut driver, _feed, _closed, fail_writes) = attached_armable(&controls).await;
    let before = controls.written().len();
    // Every write from here on fails: the first loses the transport, and the two
    // behind it reach the arm that has no transport and a failure to explain it.
    fail_writes.store(3, Ordering::SeqCst);

    let step = driver
        .send(vec![
            ClientFrame::Unsubscribe {
                channel: "ephemeral:one".to_string(),
            },
            ClientFrame::Unsubscribe {
                channel: "ephemeral:two".to_string(),
            },
            ClientFrame::Unsubscribe {
                channel: "ephemeral:three".to_string(),
            },
        ])
        .await;

    assert_eq!(
        step.events,
        vec![ConnEvent::Detached {
            reason: DetachReason::TransportClosed {
                code: None,
                reason: "write failed: scripted write failed".to_string(),
            }
        }]
    );
    assert_eq!(
        controls.written().len(),
        before,
        "nothing of the batch was written"
    );
    assert_eq!(
        fail_writes.load(Ordering::SeqCst),
        2,
        "only the first frame was ever offered to the socket"
    );
    assert_eq!(driver.state(), ConnState::Backoff);
}

#[tokio::test]
async fn a_failed_write_disconnects_and_drops_the_rest_of_the_batch() {
    let controls = Controls::new();
    let (feed, closed, _fail_writes) = controls.succeed_failing_writes(2);
    let mut driver = AttachDriver::new(config(), controls.connector());
    driver.start().await;
    let url = driver.take_pending_connect().unwrap();
    let opened = driver.connect(&url).await;
    // The hello is the first write and it fails: the connection is told, and the
    // second scripted failure is never reached because nothing else is written.
    let step = driver.on_input(opened).await;

    assert_eq!(
        step.events,
        vec![ConnEvent::Detached {
            reason: DetachReason::TransportClosed {
                code: None,
                reason: "write failed: scripted write failed".to_string(),
            }
        }]
    );
    assert!(controls.written().is_empty());
    assert_eq!(driver.state(), ConnState::Backoff);
    // The transport was dropped rather than closed: it is already gone.
    assert!(!closed.load(Ordering::SeqCst));
    drop(feed);
}

#[tokio::test]
#[should_panic(expected = "a frame to send with no live transport")]
async fn a_frame_with_no_transport_is_an_embedder_bug() {
    let controls = Controls::new();
    controls.fail();
    let mut driver = AttachDriver::new(config(), controls.connector());
    driver.start().await;
    let url = driver.take_pending_connect().unwrap();
    let failed = driver.connect(&url).await;
    driver.on_input(failed).await;

    driver
        .send(vec![ClientFrame::Unsubscribe {
            channel: "ephemeral:one".to_string(),
        }])
        .await;
}

#[tokio::test(start_paused = true)]
async fn the_retry_deadline_wakes_on_its_own_arm() {
    let controls = Controls::new();
    let (mut driver, _feed, _closed) = attached(&controls).await;

    let at = driver.now().saturating_add_ms(1_000);
    driver.set_retry_wakeup(Some(TimerChange::Arm(at)));
    // Well inside the liveness window, so only the retry arm can fire.
    assert_eq!(driver.wait().await, IoEvent::RetryDue);
}

#[tokio::test(start_paused = true)]
async fn an_unchanged_answer_leaves_the_armed_retry_deadline_alone() {
    let controls = Controls::new();
    let (mut driver, _feed, _closed) = attached(&controls).await;

    let at = driver.now().saturating_add_ms(1_000);
    driver.set_retry_wakeup(Some(TimerChange::Arm(at)));
    driver.set_retry_wakeup(None);
    assert_eq!(driver.wait().await, IoEvent::RetryDue);
}

#[tokio::test(start_paused = true)]
async fn a_disarmed_retry_deadline_never_fires() {
    let controls = Controls::new();
    let (mut driver, _feed, _closed) = attached(&controls).await;

    let at = driver.now().saturating_add_ms(1_000);
    driver.set_retry_wakeup(Some(TimerChange::Arm(at)));
    driver.set_retry_wakeup(Some(TimerChange::Disarm));
    // Nothing but the liveness deadline is armed now, so that is what fires.
    assert_eq!(driver.wait().await, IoEvent::Conn(ConnInput::Tick));
}

#[tokio::test(start_paused = true)]
async fn the_release_deadline_answers_with_the_wall_clock_read_at_the_fire() {
    let controls = Controls::new();
    let (mut driver, _feed, _closed) = attached(&controls).await;

    let before = epoch_ms(wall_now());
    let armed = before + 1_000;
    driver.set_release_wakeup(Some(ReleaseTimer::Arm(armed)));
    let IoEvent::ReleaseDue { now_ms } = driver.wait().await else {
        panic!("expected the release");
    };
    let after = epoch_ms(wall_now());

    // What releases is what is due at the fire, so the answer is a fresh read
    // and not the deadline that armed it. Paused time makes that visible: the
    // virtual timer reaches the deadline while the wall clock has barely moved,
    // which is the same decoupling a throttled host or a clock step produces.
    assert!(
        (before..=after).contains(&now_ms),
        "{now_ms} outside {before}..={after}"
    );
    assert!(
        now_ms < armed,
        "the deadline was answered instead of a read"
    );
}

#[tokio::test(start_paused = true)]
async fn a_release_deadline_already_past_fires_at_once() {
    let controls = Controls::new();
    let (mut driver, _feed, _closed) = attached(&controls).await;

    driver.set_release_wakeup(Some(ReleaseTimer::Arm(1)));
    assert!(matches!(driver.wait().await, IoEvent::ReleaseDue { .. }));
}

/// The router answers `None` on almost every call, so a `None` that disarmed
/// would silently kill every confined schedule on the next unrelated input.
#[tokio::test(start_paused = true)]
async fn an_unchanged_answer_leaves_the_armed_release_deadline_alone() {
    let controls = Controls::new();
    let (mut driver, _feed, _closed) = attached(&controls).await;

    let armed = epoch_ms(wall_now()) + 1_000;
    driver.set_release_wakeup(Some(ReleaseTimer::Arm(armed)));
    driver.set_release_wakeup(None);
    assert!(matches!(driver.wait().await, IoEvent::ReleaseDue { .. }));
}

#[tokio::test(start_paused = true)]
async fn a_disarmed_release_deadline_never_fires() {
    let controls = Controls::new();
    let (mut driver, _feed, _closed) = attached(&controls).await;

    let armed = epoch_ms(wall_now()) + 1_000;
    driver.set_release_wakeup(Some(ReleaseTimer::Arm(armed)));
    driver.set_release_wakeup(Some(ReleaseTimer::Disarm));
    // Nothing but the liveness deadline is armed now, so that is what fires.
    assert_eq!(driver.wait().await, IoEvent::Conn(ConnInput::Tick));
}

/// The select's bias is load-bearing: an arrived frame is evidence about the very
/// deadline the liveness tick would act on, so a wake with both ready must answer
/// the frame. A plain `select!` would choose between them at random and reap a
/// live attachment under load.
#[tokio::test(start_paused = true)]
async fn an_arrived_frame_outranks_an_expired_liveness_deadline() {
    let controls = Controls::new();
    let (mut driver, feed, _closed) = attached(&controls).await;

    let heartbeat = serde_json::to_string(&ServerFrame::Heartbeat).unwrap();
    feed.unbounded_send(TransportEvent::Text(heartbeat.clone()))
        .unwrap();
    // 3 × 20s: the liveness deadline is now in the past, and the frame is already
    // sitting in the feed.
    tokio::time::advance(Duration::from_secs(61)).await;

    assert_eq!(
        driver.wait().await,
        IoEvent::Conn(ConnInput::TextFrame(heartbeat))
    );
}

/// A terminal attachment answers nobody, so the deadlines the planes armed go
/// with it — including a release deadline already in the past, which would
/// otherwise resolve every `wait` at once and spin a winding-down embedder.
#[tokio::test(start_paused = true)]
async fn reaching_terminal_disarms_the_planes_deadlines() {
    let controls = Controls::new();
    let (mut driver, _feed, _closed) = attached(&controls).await;

    driver.set_retry_wakeup(Some(TimerChange::Arm(driver.now())));
    driver.set_release_wakeup(Some(ReleaseTimer::Arm(1)));
    driver
        .host_fatal("the document will not validate".to_string())
        .await;

    assert!(driver.is_terminal());
    // Nothing is armed and there is no transport, so the wait pends forever.
    assert!(
        tokio::time::timeout(Duration::from_secs(3_600), driver.wait())
            .await
            .is_err(),
        "a terminal driver handed back a timer event"
    );
}

#[tokio::test]
async fn a_host_fatal_closes_the_socket_and_names_its_cause() {
    let controls = Controls::new();
    let (mut driver, _feed, closed) = attached(&controls).await;

    let step = driver
        .host_fatal("the document will not validate".to_string())
        .await;

    assert_eq!(
        step.events,
        vec![ConnEvent::Fatal {
            detail: "the document will not validate".to_string(),
        }]
    );
    assert!(closed.load(Ordering::SeqCst));
    assert!(driver.is_terminal());
}

#[tokio::test]
async fn a_host_fatal_before_the_start_never_connects() {
    let controls = Controls::new();
    let mut driver = AttachDriver::new(config(), controls.connector());

    let step = driver
        .host_fatal("the clock reads before the epoch".to_string())
        .await;

    assert!(matches!(step.events.as_slice(), [ConnEvent::Fatal { .. }]));
    assert!(driver.take_pending_connect().is_none());
    assert_eq!(controls.connects(), 0);
}

#[tokio::test]
async fn an_embedder_close_is_terminal_and_silent() {
    let controls = Controls::new();
    let (mut driver, _feed, closed) = attached(&controls).await;

    let step = driver.close().await;

    assert!(step.events.is_empty());
    assert!(closed.load(Ordering::SeqCst));
    assert!(driver.is_terminal());
}

#[tokio::test]
async fn an_incompatible_peer_is_terminal_without_a_refusal_frame() {
    let controls = Controls::new();
    let (feed, _closed) = controls.succeed();
    let mut driver = AttachDriver::new(config(), controls.connector());
    driver.start().await;
    let url = driver.take_pending_connect().unwrap();
    let opened = driver.connect(&url).await;
    driver.on_input(opened).await;

    let hello = serde_json::to_string(&ServerFrame::Hello {
        versions: VersionRange { min: 7, max: 9 },
        ident: "future".to_string(),
    })
    .unwrap();
    feed.unbounded_send(TransportEvent::Text(hello)).unwrap();
    let IoEvent::Conn(input) = driver.wait().await else {
        panic!("expected the peer hello");
    };
    let step = driver.on_input(input).await;

    assert!(matches!(
        step.events.as_slice(),
        [ConnEvent::Incompatible { ours, theirs }]
            if *ours == SUPPORTED_VERSIONS && theirs.min == 7
    ));
    assert!(driver.is_terminal());
    // Only this end's hello ever went out; nothing refuses on the wire.
    assert!(matches!(
        controls.frames().as_slice(),
        [ClientFrame::Hello { .. }]
    ));
}

#[tokio::test]
async fn the_attachment_facts_reach_the_embedder_whole() {
    let controls = Controls::new();
    let (feed, _closed) = controls.succeed();
    let mut driver = AttachDriver::new(config(), controls.connector());
    driver.start().await;
    let url = driver.take_pending_connect().unwrap();
    let opened = driver.connect(&url).await;
    driver.on_input(opened).await;
    feed.unbounded_send(TransportEvent::Text(server_hello()))
        .unwrap();
    let IoEvent::Conn(hello) = driver.wait().await else {
        panic!("expected the peer hello");
    };
    driver.on_input(hello).await;
    feed.unbounded_send(TransportEvent::Text(welcome()))
        .unwrap();
    let IoEvent::Conn(input) = driver.wait().await else {
        panic!("expected the welcome");
    };

    let step = driver.on_input(input).await;

    assert_eq!(
        step.events,
        vec![ConnEvent::Attached(AttachmentFacts {
            version: SUPPORTED_VERSIONS.max,
            participant_id: "attacher:demo".to_string(),
            session_id: "sess-1".to_string(),
            heartbeat_secs: 20,
            max_body_bytes: 4096,
            max_frame_bytes: 8192,
            alert_granted: false,
        })]
    );
    assert!(driver.is_active());
    assert_eq!(driver.version(), Some(SUPPORTED_VERSIONS.max));
}

/// A connect the connection asked for but the embedder has not run yet is
/// abandoned with the transport: a close must not be followed by an attempt it
/// predates, or a fatal document validation resurrects the socket it ended.
#[tokio::test(start_paused = true)]
async fn an_embedder_close_abandons_a_connect_already_asked_for() {
    let controls = Controls::new();
    let mut driver = pending_connect(&controls).await;
    let connects = controls.connects();

    driver.close().await;

    assert!(driver.take_pending_connect().is_none());
    assert_eq!(controls.connects(), connects, "nothing was dialled");
}

#[tokio::test(start_paused = true)]
async fn a_host_fatal_abandons_a_connect_already_asked_for() {
    let controls = Controls::new();
    let mut driver = pending_connect(&controls).await;
    let connects = controls.connects();

    driver
        .host_fatal("the document will not validate".to_string())
        .await;

    assert!(driver.take_pending_connect().is_none());
    assert_eq!(controls.connects(), connects, "nothing was dialled");
}

/// A driver holding a genuinely queued connect: the first attempt failed, the
/// backoff deadline fired, and the retry's `Connect` is sitting unexecuted.
async fn pending_connect(controls: &Controls) -> AttachDriver<ScriptedConnector> {
    controls.fail();
    let mut driver = AttachDriver::new(config(), controls.connector());
    driver.start().await;
    let url = driver.take_pending_connect().expect("the first attempt");
    let failed = driver.connect(&url).await;
    driver.on_input(failed).await;
    let IoEvent::Conn(tick) = driver.wait().await else {
        panic!("expected the backoff tick");
    };
    driver.on_input(tick).await;
    assert_eq!(driver.state(), ConnState::Connecting);
    driver
}

/// A flush is one commit, so its entries must agree about when now was — and
/// each must still be its own message, since the store dedups by `message_id`
/// and a repeated stamp would collapse the flush into a single arrival.
#[tokio::test]
async fn a_flush_s_stamps_share_one_instant_and_no_identity() {
    let controls = Controls::new();
    let driver = AttachDriver::new(config(), controls.connector());

    let stamps = driver.flush_stamps(3);

    let instants: std::collections::BTreeSet<_> = stamps.iter().map(|s| s.publish_ts).collect();
    assert_eq!(instants.len(), 1, "one clock read for the whole flush");
    let ids: std::collections::BTreeSet<_> = stamps.iter().map(|s| s.message_id).collect();
    assert_eq!(ids.len(), 3, "three messages, three identities");
}

#[tokio::test]
async fn two_single_stamps_are_two_messages() {
    let controls = Controls::new();
    let driver = AttachDriver::new(config(), controls.connector());

    assert_ne!(driver.new_stamp().message_id, driver.new_stamp().message_id);
}
