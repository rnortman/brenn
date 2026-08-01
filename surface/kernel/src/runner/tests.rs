//! The loop, against a scripted transport and a real page.
//!
//! What a runner does is observable in three places, and these tests read those:
//! the frames written to the socket, the events handed to the platform half, and
//! whether the socket was closed and re-opened. What the page holds is its own
//! suites' business, with one exception: the terminal drain's whole job is to
//! route a confined publish, which produces no frame and no event by design and
//! would be unobservable in principle under that rule. So the run hands the page
//! back, and the two drain tests read the plane it wrote.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use brenn_attach_client::{TransportConnection, TransportError, TransportEvent};
use brenn_attach_proto::{ClientFrame, SUPPORTED_VERSIONS, ServerFrame, SubscribeOutcome};
use brenn_envelope::{ChannelScheme, MessageEnvelope, Urgency};
use brenn_surface_schema::bindings::BindingsDocument;
use brenn_surface_schema::{
    CONTROL_PLANE_VERSION, LOCAL_TOAST_CHANNEL, ToastBody, ToastSeverity, ToastSource,
};
use uuid::Uuid;

use crate::test_support::bindings as fixtures;
use crate::test_support::pages;

use super::*;

const URL: &str = "wss://host/surface/bar/ws?build=b1";
const CONFIG: &str = "ephemeral:site.surface.bar.bindings";
const WIRE_ONE: &str = "brenn:site.bar.one";
const WIRE_TWO: &str = "brenn:site.bar.two";
const EPOCH: Uuid = Uuid::from_u128(0x1_11c0);

fn config() -> ConnConfig {
    ConnConfig {
        url: URL.to_string(),
        ident: "b1".to_string(),
        initial_backoff: Duration::from_secs(3),
        max_backoff: Duration::from_secs(60),
        connect_timeout: Duration::from_secs(15),
        liveness_multiplier: 3,
        backoff_jitter_seed: 0,
        terminal_close_code: Some(3001),
    }
}

/// `p1` and `p2` each read one wire channel, so a document that mounts both opens
/// two subscriptions in one turn — the batch a mid-batch write failure cuts short.
///
/// `p2` also reads the toast plane, which is what gives that plane a store deep
/// enough to retain: its contract depth is zero (a toast is a signal, delivered at
/// the append and kept by nobody), and a binding is the only thing that raises it.
/// That is what makes a routed toast readable off the page the run hands back.
fn doc() -> BindingsDocument {
    fixtures::doc(
        vec![
            fixtures::component("p1"),
            fixtures::component("p2"),
            fixtures::component(fixtures::CHROME),
        ],
        vec![
            fixtures::subscription("p1", "in", WIRE_ONE, 4, 0),
            fixtures::subscription("p2", "in", WIRE_TWO, 4, 0),
            fixtures::subscription("p2", "toast", LOCAL_TOAST_CHANNEL, 1, 1),
        ],
        Vec::new(),
        vec![fixtures::local(LOCAL_TOAST_CHANNEL, 0)],
    )
}

fn server_hello() -> String {
    frame(&ServerFrame::Hello {
        versions: SUPPORTED_VERSIONS,
        ident: "peer".to_string(),
    })
}

fn welcome() -> String {
    let facts = pages::facts();
    frame(&ServerFrame::Welcome {
        version: facts.version,
        participant_id: facts.participant_id,
        session_id: facts.session_id,
        heartbeat_secs: facts.heartbeat_secs,
        max_body_bytes: facts.max_body_bytes,
        max_frame_bytes: facts.max_frame_bytes,
        alert_granted: facts.alert_granted,
    })
}

fn subscribe_result(channel: &str, replay_count: u32) -> String {
    frame(&ServerFrame::SubscribeResult {
        channel: channel.to_string(),
        outcome: SubscribeOutcome::Ok,
        replay_count,
        gap: None,
    })
}

fn deliver(channel: &str, body: &str) -> String {
    frame(&ServerFrame::Deliver {
        channel: channel.to_string(),
        envelope: MessageEnvelope {
            message_id: Uuid::from_u128(0x9001),
            source: "test".into(),
            channel: channel.into(),
            sender: "system:surface-config".into(),
            publish_ts: chrono::DateTime::from_timestamp(0, 0).expect("a representable instant"),
            body: body.into(),
            reply_to: None,
            delivery_deadline: None,
            deliver_after: None,
            impetus: None,
            urgency: Urgency::Normal,
            envelope_type: ChannelScheme::Ephemeral,
        },
        seq: 1,
        cursor: serde_json::from_value(serde_json::Value::String("c1".to_string()))
            .expect("a cursor is a JSON string"),
        dropped: 0,
    })
}

fn frame(frame: &ServerFrame) -> String {
    serde_json::to_string(frame).expect("a server frame serializes")
}

/// A toast, which is a plane the kernel may state whether or not it is attached.
fn toast() -> RunnerCommand {
    RunnerCommand::PublishControl {
        channel: LOCAL_TOAST_CHANNEL.to_string(),
        body: serde_json::to_string(&ToastBody {
            v: CONTROL_PLANE_VERSION,
            severity: ToastSeverity::Warning,
            text: "hello".to_string(),
            source: ToastSource::Kernel,
        })
        .expect("a toast body serializes"),
    }
}

// ── the scripted transport ────────────────────────────────────────────────

enum Plan {
    Fail,
    Succeed {
        incoming: mpsc::UnboundedReceiver<TransportEvent>,
        closed: Arc<AtomicBool>,
        fail_writes: Arc<AtomicUsize>,
    },
    /// The connect resolves only once the returned trigger fires.
    Stall {
        release: futures_channel::oneshot::Receiver<()>,
        incoming: mpsc::UnboundedReceiver<TransportEvent>,
    },
}

/// Scripts connect outcomes and records what the runner wrote and closed.
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

    /// Queue a connect that succeeds; hands back that socket's event feed, its
    /// close flag, and the counter that arms write failures on it.
    fn succeed(
        &self,
    ) -> (
        mpsc::UnboundedSender<TransportEvent>,
        Arc<AtomicBool>,
        Arc<AtomicUsize>,
    ) {
        let (tx, rx) = mpsc::unbounded();
        let closed = Arc::new(AtomicBool::new(false));
        let fail_writes = Arc::new(AtomicUsize::new(0));
        self.plans.lock().unwrap().push_back(Plan::Succeed {
            incoming: rx,
            closed: closed.clone(),
            fail_writes: fail_writes.clone(),
        });
        (tx, closed, fail_writes)
    }

    fn fail(&self) {
        self.plans.lock().unwrap().push_back(Plan::Fail);
    }

    /// Queue a connect that hangs until the returned trigger fires.
    fn stall(&self) -> futures_channel::oneshot::Sender<()> {
        let (release_tx, release_rx) = futures_channel::oneshot::channel();
        let (_tx, rx) = mpsc::unbounded();
        self.plans.lock().unwrap().push_back(Plan::Stall {
            release: release_rx,
            incoming: rx,
        });
        release_tx
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
            .map(|text| serde_json::from_str(text).expect("the runner writes client frames"))
            .collect()
    }

    /// The channels the runner has subscribed so far, in write order.
    fn subscribed(&self) -> Vec<String> {
        self.frames()
            .into_iter()
            .filter_map(|frame| match frame {
                ClientFrame::Subscribe { channel, .. } => Some(channel),
                _ => None,
            })
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
            Some(Plan::Stall { release, incoming }) => {
                let _ = release.await;
                Ok(ScriptedConnection {
                    incoming,
                    written: self.written.clone(),
                    closed: Arc::new(AtomicBool::new(false)),
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

/// A spawned run and the two ends the test holds: the event sink's receiver and
/// the control channel's sender.
struct Running {
    events: mpsc::Receiver<Event>,
    control: mpsc::Sender<RunnerCommand>,
    task: tokio::task::JoinHandle<SurfacePage>,
}

fn spawn(controls: &Controls) -> Running {
    spawn_from(controls, wall_now())
}

/// As [`spawn`], from a wall-clock reading the caller chose — the one input to the
/// boot clock check.
fn spawn_from(controls: &Controls, wall: DateTime<Utc>) -> Running {
    let (events_tx, events) = mpsc::channel(64);
    let (control, control_rx) = mpsc::channel(8);
    let runner = SurfaceRunner::new(
        SurfacePage::new(CONFIG.to_string(), EPOCH),
        config(),
        controls.connector(),
        events_tx,
        control_rx,
    );
    Running {
        events,
        control,
        task: tokio::spawn(runner.run_from(wall)),
    }
}

impl Running {
    /// Await the next event, failing rather than hanging if none arrives; the
    /// bound is virtual time, so it costs nothing under a paused clock.
    async fn event(&mut self) -> Event {
        tokio::time::timeout(Duration::from_secs(3_600), self.events.next())
            .await
            .expect("an event within the (virtual) bound")
            .expect("the event sink is not closed")
    }

    fn send(&mut self, command: RunnerCommand) {
        self.control
            .try_send(command)
            .expect("the channel has room");
    }

    /// Drop the control channel and take the page the run hands back, failing
    /// rather than hanging if the run never ends.
    ///
    /// The event sink is held open across the wait: dropping it is the *other*
    /// thing that ends a run, and a test asking what the drain did must leave the
    /// control channel as the only answer.
    async fn end(self) -> SurfacePage {
        let Running {
            events,
            control,
            task,
        } = self;
        drop(control);
        let page = tokio::time::timeout(Duration::from_secs(3_600), task)
            .await
            .expect("the run ends within the (virtual) bound")
            .expect("the run does not panic");
        drop(events);
        page
    }
}

/// Poll until `cond` holds, advancing virtual time between checks; fails rather
/// than hanging if it never does.
async fn wait_until(mut cond: impl FnMut() -> bool) {
    for _ in 0..10_000 {
        if cond() {
            return;
        }
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(1)).await;
    }
    panic!("condition never held within the bound");
}

/// Poll until `cond` holds without moving the clock; fails rather than hanging.
///
/// The twin of [`wait_until`] for a test asserting what has *not* happened by a
/// given instant: advancing to find the state would be advancing past the
/// deadline under test.
async fn settle_until(mut cond: impl FnMut() -> bool) {
    for _ in 0..1_000 {
        if cond() {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("condition never held without advancing the clock");
}

/// Let the run reach quiescence at the instant the clock is at, for a test
/// asserting that nothing happened by it.
async fn settle() {
    for _ in 0..64 {
        tokio::task::yield_now().await;
    }
}

/// What the page's own toast plane retains, oldest first.
fn toasts(page: &SurfacePage) -> Vec<String> {
    page.stores
        .get(LOCAL_TOAST_CHANNEL)
        .expect("a page hosts its reserved planes from its first instant")
        .retained()
        .map(|(envelope, _)| {
            serde_json::from_str::<ToastBody>(&envelope.body)
                .expect("the toast plane carries toast bodies")
                .text
        })
        .collect()
}

/// Play the peer's half of a handshake and the config channel's replay, so the
/// page reaches phase 2 and announces itself.
async fn attach(running: &mut Running, feed: &mpsc::UnboundedSender<TransportEvent>) {
    for text in [
        server_hello(),
        welcome(),
        subscribe_result(CONFIG, 1),
        deliver(CONFIG, &doc().to_body()),
    ] {
        feed.unbounded_send(TransportEvent::Text(text)).unwrap();
    }
    assert!(matches!(running.event().await, Event::Connected { .. }));
}

// ── tests ─────────────────────────────────────────────────────────────────

#[tokio::test(start_paused = true)]
async fn a_started_run_connects_and_greets_the_peer() {
    let controls = Controls::new();
    let (_feed, _closed, _writes) = controls.succeed();
    let running = spawn(&controls);

    wait_until(|| !controls.written().is_empty()).await;
    assert_eq!(controls.connects(), 1);
    assert!(matches!(
        controls.frames().first(),
        Some(ClientFrame::Hello { .. })
    ));
    running.task.abort();
}

/// The delay is the point, not just the retry: a backoff that resolves to zero is
/// a reconnect storm against a peer that just refused the connect, and it reads as
/// green under an assertion that only counts attempts.
#[tokio::test(start_paused = true)]
async fn a_refused_connect_backs_off_and_tries_again() {
    let controls = Controls::new();
    controls.fail();
    let (_feed, _closed, _writes) = controls.succeed();
    let running = spawn(&controls);

    // The clock stays where it is until the refusal has landed, so what is
    // advanced below is the backoff and nothing else.
    settle_until(|| controls.connects() >= 1).await;
    assert_eq!(controls.connects(), 1);
    // Equal jitter spreads the delay over [initial/2, initial], so half the
    // configured 3s is the floor no attempt may precede.
    tokio::time::advance(Duration::from_millis(1_400)).await;
    settle().await;
    assert_eq!(
        controls.connects(),
        1,
        "a refused connect waits out its backoff before trying again"
    );

    wait_until(|| controls.connects() >= 2).await;
    running.task.abort();
}

#[tokio::test(start_paused = true)]
async fn the_handshake_subscribes_the_config_channel_and_announces_nothing() {
    let controls = Controls::new();
    let (feed, _closed, _writes) = controls.succeed();
    let mut running = spawn(&controls);

    feed.unbounded_send(TransportEvent::Text(server_hello()))
        .unwrap();
    feed.unbounded_send(TransportEvent::Text(welcome()))
        .unwrap();

    wait_until(|| controls.subscribed() == vec![CONFIG.to_string()]).await;
    // Phase 1 is not a usable page: nothing is announced until the wiring lands.
    assert!(running.events.next().now_or_never().is_none());
    let subscribe = controls
        .frames()
        .into_iter()
        .find(|frame| matches!(frame, ClientFrame::Subscribe { .. }))
        .expect("the config subscribe");
    assert!(matches!(
        subscribe,
        ClientFrame::Subscribe {
            resume: None,
            push_depth: 1,
            retain_depth: 1,
            ..
        }
    ));
    running.task.abort();
}

#[tokio::test(start_paused = true)]
async fn the_delivered_document_makes_the_page_connected() {
    let controls = Controls::new();
    let (feed, _closed, _writes) = controls.succeed();
    let mut running = spawn(&controls);

    attach(&mut running, &feed).await;
    running.task.abort();
}

#[tokio::test(start_paused = true)]
async fn a_mount_subscribes_its_channel_and_an_unmount_closes_it() {
    let controls = Controls::new();
    let (feed, _closed, _writes) = controls.succeed();
    let mut running = spawn(&controls);
    attach(&mut running, &feed).await;

    running.send(RunnerCommand::RegisterActivation {
        instance: "p1".to_string(),
        entry: Box::new(|_, _| Ok(())),
    });
    wait_until(|| controls.subscribed().contains(&WIRE_ONE.to_string())).await;

    // Acknowledged first: an unsubscribe of a subscription the peer has not
    // answered yet waits for its result rather than crossing it on the wire.
    feed.unbounded_send(TransportEvent::Text(subscribe_result(WIRE_ONE, 0)))
        .unwrap();
    running.send(RunnerCommand::DeregisterActivation {
        instance: "p1".to_string(),
    });
    wait_until(|| {
        controls.frames().iter().any(
            |frame| matches!(frame, ClientFrame::Unsubscribe { channel } if channel == WIRE_ONE),
        )
    })
    .await;
    running.task.abort();
}

#[tokio::test(start_paused = true)]
async fn a_peer_close_detaches_and_reconnects() {
    let controls = Controls::new();
    let (feed, _closed, _writes) = controls.succeed();
    let (_feed2, _closed2, _writes2) = controls.succeed();
    let mut running = spawn(&controls);
    attach(&mut running, &feed).await;

    feed.unbounded_send(TransportEvent::Closed {
        code: Some(1006),
        reason: "gone".to_string(),
    })
    .unwrap();

    assert!(matches!(running.event().await, Event::Disconnected { .. }));
    wait_until(|| controls.connects() >= 2).await;
    running.task.abort();
}

#[tokio::test(start_paused = true)]
async fn an_unreconcilable_frame_takes_the_attachment_fatal() {
    let controls = Controls::new();
    let (feed, closed, _writes) = controls.succeed();
    let mut running = spawn(&controls);
    attach(&mut running, &feed).await;

    // A delivery on a channel this attachment never subscribed: a peer contract
    // the page cannot reconcile.
    feed.unbounded_send(TransportEvent::Text(deliver(WIRE_ONE, "{}")))
        .unwrap();

    assert!(matches!(running.event().await, Event::Fatal { .. }));
    wait_until(|| closed.load(Ordering::SeqCst)).await;
    // Terminal is terminal: nothing reconnects.
    assert_eq!(controls.connects(), 1);
    running.task.abort();
}

#[tokio::test(start_paused = true)]
async fn a_write_failure_mid_batch_drops_the_rest_of_it_and_reconnects() {
    let controls = Controls::new();
    let (feed, _closed, writes) = controls.succeed();
    let (_feed2, _closed2, _writes2) = controls.succeed();
    let mut running = spawn(&controls);

    // Both instances mount before the wiring lands, so the document's own turn
    // composes two `Subscribe`s — and the first of them is the write that fails.
    feed.unbounded_send(TransportEvent::Text(server_hello()))
        .unwrap();
    feed.unbounded_send(TransportEvent::Text(welcome()))
        .unwrap();
    feed.unbounded_send(TransportEvent::Text(subscribe_result(CONFIG, 1)))
        .unwrap();
    wait_until(|| controls.subscribed() == vec![CONFIG.to_string()]).await;
    for instance in ["p1", "p2"] {
        running.send(RunnerCommand::RegisterActivation {
            instance: instance.to_string(),
            entry: Box::new(|_, _| Ok(())),
        });
    }
    wait_until(|| controls.connects() == 1).await;
    writes.store(1, Ordering::SeqCst);
    feed.unbounded_send(TransportEvent::Text(deliver(CONFIG, &doc().to_body())))
        .unwrap();

    assert!(matches!(running.event().await, Event::Connected { .. }));
    assert!(matches!(running.event().await, Event::Disconnected { .. }));
    // The second subscribe was composed against the attachment that just died, so
    // it is dropped rather than written to a corpse — and the run carries on.
    wait_until(|| controls.connects() >= 2).await;
    assert_eq!(controls.subscribed(), vec![CONFIG.to_string()]);
    running.task.abort();
}

#[tokio::test(start_paused = true)]
async fn a_close_ends_the_attachment_and_the_run_outlives_it() {
    let controls = Controls::new();
    let (feed, closed, _writes) = controls.succeed();
    let mut running = spawn(&controls);
    attach(&mut running, &feed).await;

    running.send(RunnerCommand::Close);
    wait_until(|| closed.load(Ordering::SeqCst)).await;
    // The platform half folds the death a hop later and states its own terminal
    // link-state, so the run is still there to route it.
    running.send(toast());
    // Only the platform half dropping every sender ends it.
    let page = running.end().await;
    assert_eq!(
        toasts(&page),
        vec!["hello".to_string()],
        "the drain routed the banner the page draws its own death from"
    );
}

#[tokio::test(start_paused = true)]
async fn a_control_publish_after_a_fatal_still_routes() {
    let controls = Controls::new();
    let (feed, _closed, _writes) = controls.succeed();
    let mut running = spawn(&controls);
    attach(&mut running, &feed).await;

    feed.unbounded_send(TransportEvent::Text(deliver(WIRE_ONE, "{}")))
        .unwrap();
    assert!(matches!(running.event().await, Event::Fatal { .. }));

    running.send(toast());
    let page = running.end().await;
    assert_eq!(toasts(&page), vec!["hello".to_string()]);
}

#[tokio::test(start_paused = true)]
async fn a_command_buffered_during_a_connect_is_applied_once_it_settles() {
    let controls = Controls::new();
    let release = controls.stall();
    let mut running = spawn(&controls);

    wait_until(|| controls.connects() == 1).await;
    // Nothing can be fed to a page whose attempt is still in flight; the close is
    // held until it settles rather than filling the channel or cancelling it.
    running.send(RunnerCommand::Close);
    tokio::task::yield_now().await;
    release.send(()).unwrap();

    // The event sink stays open for the whole wait, so `host_gone` is false
    // throughout and the run can only end by reaching terminal — which the
    // buffered `Close` is the sole cause of. A dropped or cancelled buffer hangs
    // here instead, and the virtual-time bound fires.
    running.end().await;
}

/// The boot clock check, which is the one branch a machine cannot be put into:
/// the refusal is diagnosed through the ordinary fatal path (so the cause is
/// named), no connect is attempted, and the turns that follow read no clock —
/// where carrying on would trade the diagnosis for an undiagnosed panic at the
/// first publish.
#[tokio::test(start_paused = true)]
async fn a_pre_epoch_device_clock_is_refused_before_any_connect() {
    let controls = Controls::new();
    let (_feed, _closed, _writes) = controls.succeed();
    let before = chrono::DateTime::from_timestamp_millis(-1).expect("a representable instant");
    let mut running = spawn_from(&controls, before);

    match running.event().await {
        Event::Fatal { detail } => {
            assert!(detail.contains("before the Unix epoch"), "{detail}");
        }
        other => panic!("expected the clock's own fatal, got {other:?}"),
    }
    assert_eq!(
        controls.connects(),
        0,
        "a page that cannot timestamp has no business attaching"
    );
    // The run still winds down through the drain, on a clock it never reads.
    running.end().await;
}

/// The two `Err` arms of [`SurfaceRunner::emit`] are one `is_full()` apart and
/// mean opposite things: a receiver that has gone is the run winding down, a sink
/// that is full is a platform half not draining. Collapsing them would turn the
/// loud one into silently discarded events.
#[test]
#[should_panic(expected = "event sink overflow")]
fn a_full_event_sink_is_a_platform_half_not_draining_bug() {
    let controls = Controls::new();
    // Zero buffer plus the one slot every sender is guaranteed: the second emit
    // has nowhere to go, with the receiver still very much alive.
    let (events_tx, _events) = mpsc::channel(0);
    let (_control, control_rx) = mpsc::channel(8);
    let mut runner = SurfaceRunner::new(
        SurfacePage::new(CONFIG.to_string(), EPOCH),
        config(),
        controls.connector(),
        events_tx,
        control_rx,
    );
    runner.emit(Event::WiringChanged);
    runner.emit(Event::WiringChanged);
}

#[tokio::test(start_paused = true)]
async fn the_platform_half_leaving_ends_the_run() {
    let controls = Controls::new();
    let (feed, _closed, _writes) = controls.succeed();
    let (_feed2, _closed2, _writes2) = controls.succeed();
    let mut running = spawn(&controls);
    attach(&mut running, &feed).await;

    // Nothing consumes what a turn produces now.
    drop(running.events);
    feed.unbounded_send(TransportEvent::Closed {
        code: None,
        reason: String::new(),
    })
    .unwrap();

    tokio::time::timeout(Duration::from_secs(3_600), running.task)
        .await
        .expect("the run ends within the (virtual) bound")
        .expect("the run does not panic");
}
