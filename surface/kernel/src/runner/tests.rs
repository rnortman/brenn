//! The loop, against a scripted transport and a real page.
//!
//! What a runner does is observable in three places, and these tests read those:
//! the frames written to the socket, the events handed to the platform half, and
//! whether the socket was closed and re-opened. What the page holds is its own
//! suites' business, with one exception: the terminal drain's whole job is to
//! route a confined publish and activate the readers it wakes, which produces no
//! frame and no event by design and would be unobservable in principle under that
//! rule. So the run hands the page back, and the two drain tests read the plane it
//! wrote and what the reader's entry was shown off it.

use crate::publish_buffer::PortFault;

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use brenn_attach_client::conn::AttachmentFacts;
use brenn_attach_client::{TransportConnection, TransportError, TransportEvent};
use brenn_attach_proto::{
    AlertSeverity, ClientFrame, PublishBatchOutcome, PublishOutcome, ServerFrame,
};
use brenn_surface_contract::ActivationError;
use brenn_surface_schema::bindings::BindingsDocument;
use brenn_surface_schema::telemetry::StatusCounters;
use brenn_surface_schema::{
    CONTROL_PLANE_VERSION, LOCAL_TOAST_CHANNEL, LogLevel, ToastBody, ToastSeverity, ToastSource,
};
use uuid::Uuid;

use crate::front::{self, EventStream, PublishReject, SurfaceHandle};
use crate::outbound::PublishStatus;
use crate::test_support::bindings as fixtures;
use crate::test_support::frames::{
    deliver, deliver_at, deliver_pass, frame, server_hello, subscribe_result,
    welcome as welcome_under,
};
use crate::test_support::pages;

use super::*;

const URL: &str = "wss://host/surface/bar/ws?build=b1";
const CONFIG: &str = "ephemeral:site.surface.bar.bindings";
const WIRE_ONE: &str = "brenn:site.bar.one";
const WIRE_TWO: &str = "brenn:site.bar.two";
const WIRE_OUT: &str = "brenn:site.bar.out";
const NOTES: &str = "local:app/notes";
const ERRORS: &str = "brenn:site.surface.bar.errors";
const GEOMETRY: &str = "brenn:site.surface.bar.geometry";
const STATUS: &str = "brenn:site.surface.bar.status";
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
/// `p1` writes both classes: `WIRE_OUT`, which an activation's flush puts on the
/// wire, and the page-local `NOTES`, which `p2` reads — so one component's
/// activation is what makes the next one ready.
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
            fixtures::subscription("p2", "notes", NOTES, 4, 0),
            fixtures::subscription("p2", "toast", LOCAL_TOAST_CHANNEL, 1, 1),
        ],
        vec![
            fixtures::output("p1", "out", WIRE_OUT),
            fixtures::output("p1", "notes", NOTES),
        ],
        vec![
            fixtures::local(NOTES, 4),
            fixtures::local(LOCAL_TOAST_CHANNEL, 0),
        ],
    )
}

/// A document in which `p1` reads the very channel it writes: its own activation
/// makes it ready again, forever. What a page does under one of those is the
/// containment question the bounded pass and the bias order exist to answer.
///
/// `p2` reads a channel of its own and writes nothing, so it is the sibling the
/// fairness half of that question is asked about: a spinner must not be able to
/// take every pass.
fn spin_doc() -> BindingsDocument {
    fixtures::doc(
        vec![
            fixtures::component("p1"),
            fixtures::component("p2"),
            fixtures::component(fixtures::CHROME),
        ],
        vec![
            fixtures::subscription("p1", "in", WIRE_ONE, 4, 0),
            fixtures::subscription("p1", "notes", NOTES, 4, 0),
            fixtures::subscription("p2", "in", WIRE_TWO, 4, 0),
        ],
        vec![fixtures::output("p1", "notes", NOTES)],
        vec![fixtures::local(NOTES, 4)],
    )
}

/// The wiring above, with error reports switched on at `warn` — the platform
/// section's other optional pair, which decides whether the log path publishes at
/// all.
fn reporting_doc() -> BindingsDocument {
    let mut document = doc();
    document.platform.error_channel = Some(ERRORS.to_string());
    document.platform.error_report_floor = Some(LogLevel::Warn);
    document
}

/// [`reporting_doc`] narrowed: `p1`'s wire output is gone and the report floor is
/// a rung higher. The second document a running page can be handed, and the two
/// changes are exactly the two things the handle's snapshot answers from.
fn narrowed_doc() -> BindingsDocument {
    let mut document = reporting_doc();
    document.outputs.retain(|binding| binding.port != "out");
    document.platform.error_report_floor = Some(LogLevel::Error);
    document
}

fn welcome() -> String {
    welcome_under(pages::facts())
}

/// The attachment's facts, with the alert grant this surface's default fixture
/// does not carry.
fn alert_granting_facts() -> AttachmentFacts {
    AttachmentFacts {
        alert_granted: true,
        ..pages::facts()
    }
}

/// The body [`toast`] states, which is also what a reader bound to the plane is
/// shown.
fn toast_body() -> String {
    serde_json::to_string(&ToastBody {
        v: CONTROL_PLANE_VERSION,
        severity: ToastSeverity::Warning,
        text: "hello".to_string(),
        source: ToastSource::Kernel,
    })
    .expect("a toast body serializes")
}

/// A toast, which is a plane the kernel may state whether or not it is attached.
fn toast() -> RunnerCommand {
    RunnerCommand::PublishControl {
        channel: LOCAL_TOAST_CHANNEL.to_string(),
        body: toast_body(),
    }
}

/// One single publish as it reached the socket.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Published {
    channel: String,
    /// The sub-identity the peer attributes it to: a component for its own port,
    /// `None` for the documents the surface writes about itself.
    attribution: Option<String>,
    body: String,
    /// The *wire* correlation the page minted, which is what the peer answers —
    /// never the caller's own.
    correlation: Option<u64>,
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

    /// The single publishes the runner has put on the wire, in write order.
    fn publishes(&self) -> Vec<Published> {
        self.frames()
            .into_iter()
            .filter_map(|frame| match frame {
                ClientFrame::Publish {
                    channel,
                    attribution,
                    body,
                    correlation,
                    ..
                } => Some(Published {
                    channel,
                    attribution,
                    body,
                    correlation,
                }),
                _ => None,
            })
            .collect()
    }

    /// The alerts the runner has put on the wire, in write order: each one's
    /// title and body.
    fn alerts(&self) -> Vec<(String, String)> {
        self.frames()
            .into_iter()
            .filter_map(|frame| match frame {
                ClientFrame::Alert { title, body, .. } => Some((title, body)),
                _ => None,
            })
            .collect()
    }

    /// The flushes the runner has put on the wire, in write order: each one's
    /// correlation and the bodies it carries.
    fn batches(&self) -> Vec<(u64, Vec<String>)> {
        self.frames()
            .into_iter()
            .filter_map(|frame| match frame {
                ClientFrame::PublishBatch {
                    correlation,
                    publishes,
                    ..
                } => Some((
                    correlation,
                    publishes.into_iter().map(|entry| entry.body).collect(),
                )),
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

/// A spawned run and the two ends the test holds: the front door's handle and the
/// events it drains.
///
/// The handle is the real one, gate and all, so a test that publishes through it
/// is exercising the pre-check the run keeps current as well as the plane behind
/// it.
struct Running {
    events: EventStream,
    handle: SurfaceHandle,
    task: tokio::task::JoinHandle<SurfacePage>,
}

fn spawn(controls: &Controls) -> Running {
    spawn_from(controls, wall_now())
}

/// As [`spawn`], from a wall-clock reading the caller chose — the one input to the
/// boot clock check.
fn spawn_from(controls: &Controls, wall: DateTime<Utc>) -> Running {
    let (handle, events, front) = front::new();
    let runner = SurfaceRunner::new(
        SurfacePage::new(CONFIG.to_string(), EPOCH),
        config(),
        controls.connector(),
        front,
    );
    Running {
        events,
        handle,
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

    /// Ask for one lifecycle command through the handle that composes it — the
    /// control plane as a test states it.
    fn send(&mut self, command: RunnerCommand) {
        match command {
            RunnerCommand::RegisterActivation {
                instance,
                entry,
                mount,
            } => {
                self.handle.register_activation(&instance, entry, mount);
            }
            RunnerCommand::DeregisterActivation { instance } => {
                self.handle.deregister_activation(&instance);
            }
            RunnerCommand::PublishControl { channel, body } => {
                self.handle.publish_control(&channel, body);
            }
            RunnerCommand::Close => self.handle.close(),
        }
    }

    /// Drop the handle and take the page the run hands back, failing rather than
    /// hanging if the run never ends.
    ///
    /// The event sink is held open across the wait: dropping it is the *other*
    /// thing that ends a run, and a test asking what the drain did must leave the
    /// front door's own channels as the only answer.
    async fn end(self) -> SurfacePage {
        let Running {
            events,
            handle,
            task,
        } = self;
        drop(handle);
        let page = tokio::time::timeout(Duration::from_secs(3_600), task)
            .await
            .expect("the run ends within the (virtual) bound")
            .expect("the run does not panic");
        drop(events);
        page
    }
}

/// The front door's sending ends, test-owned rather than composed by a handle.
///
/// Two things need them. A command the handle's own gate would refuse — a
/// straggler publish after the run went terminal is the case that matters, since
/// the gate is refreshed at exactly that edge — and closing one channel alone,
/// which a handle cannot do because it holds all four.
struct RawFront {
    #[expect(
        dead_code,
        reason = "held to keep the sink open; the run reads its closure"
    )]
    events_tx: mpsc::Sender<Event>,
    control_tx: mpsc::Sender<RunnerCommand>,
    publish_tx: mpsc::Sender<PublishSlot>,
    alert_tx: mpsc::Sender<AlertCommand>,
    telemetry_tx: mpsc::Sender<TelemetryCommand>,
}

/// The front door's two halves, with the event sink at the capacity the caller
/// wants.
fn raw_front(events: usize) -> (RawFront, mpsc::Receiver<Event>, FrontChannels) {
    let (events_tx, events_rx) = mpsc::channel(events);
    let (control_tx, control_rx) = mpsc::channel(8);
    let (publish_tx, publish_rx) = mpsc::channel(8);
    let (alert_tx, alert_rx) = mpsc::channel(8);
    let (telemetry_tx, telemetry_rx) = mpsc::channel(8);
    (
        RawFront {
            events_tx: events_tx.clone(),
            control_tx,
            publish_tx,
            alert_tx,
            telemetry_tx,
        },
        events_rx,
        FrontChannels {
            events_tx,
            control_rx,
            publish_rx,
            alert_rx,
            telemetry_rx,
            gate: Arc::new(Mutex::new(SurfaceGate::default())),
        },
    )
}

/// A run whose front door the test drives sender by sender.
struct RawRunning {
    front: RawFront,
    events: mpsc::Receiver<Event>,
    task: tokio::task::JoinHandle<SurfacePage>,
}

fn spawn_raw(controls: &Controls) -> RawRunning {
    let (front, events, channels) = raw_front(64);
    let runner = SurfaceRunner::new(
        SurfacePage::new(CONFIG.to_string(), EPOCH),
        config(),
        controls.connector(),
        channels,
    );
    RawRunning {
        front,
        events,
        task: tokio::spawn(runner.run_from(wall_now())),
    }
}

impl RawRunning {
    /// Await the next event, failing rather than hanging if none arrives.
    async fn event(&mut self) -> Event {
        tokio::time::timeout(Duration::from_secs(3_600), self.events.next())
            .await
            .expect("an event within the (virtual) bound")
            .expect("the event sink is not closed")
    }

    /// Drop every sender and wait for the run to end.
    async fn end(self) {
        let RawRunning {
            front,
            events,
            task,
        } = self;
        drop(front);
        tokio::time::timeout(Duration::from_secs(3_600), task)
            .await
            .expect("the run ends within the (virtual) bound")
            .expect("the run does not panic");
        drop(events);
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

// ── scripted components ───────────────────────────────────────────────────

/// What a component's entry was shown, body by body in call order.
#[derive(Clone, Default)]
struct Seen(Arc<Mutex<Vec<String>>>);

impl Seen {
    fn bodies(&self) -> Vec<String> {
        self.0.lock().unwrap().clone()
    }

    fn count(&self) -> usize {
        self.0.lock().unwrap().len()
    }

    fn saw(&self, body: &str) -> bool {
        self.bodies().iter().any(|seen| seen == body)
    }
}

/// An entry that records every new envelope it is shown and then does `act` with
/// the activation and its buffer.
fn entry<F>(seen: &Seen, act: F) -> ActivationEntry
where
    F: Fn(&Activation, &mut PublishBuffer) -> Result<Option<String>, ActivationError>
        + Send
        + 'static,
{
    let seen = seen.clone();
    Box::new(move |activation, buffer| {
        for window in &activation.ports {
            for envelope in window.new_envelopes() {
                seen.0
                    .lock()
                    .unwrap()
                    .push(brenn_surface_test_fixtures::parse_envelope(envelope).body);
            }
        }
        act(activation, buffer)
    })
}

/// An entry that reads and publishes nothing.
fn quiet(seen: &Seen) -> ActivationEntry {
    entry(seen, |_, _| Ok(None))
}

/// What each activation was shown, one vector of new bodies per call.
///
/// [`Seen`] flattens every activation into one list, which cannot tell one
/// activation of three new messages from three activations of one — the
/// distinction batching must preserve.
#[derive(Clone, Default)]
struct Windows(Arc<Mutex<Vec<Vec<String>>>>);

impl Windows {
    fn activations(&self) -> Vec<Vec<String>> {
        self.0.lock().unwrap().clone()
    }
}

/// An entry that records the new slice of every window it is handed, activation
/// by activation.
fn windowing(windows: &Windows) -> ActivationEntry {
    let windows = windows.clone();
    Box::new(move |activation: &Activation, _: &mut PublishBuffer| {
        let bodies = activation
            .ports
            .iter()
            .flat_map(|window| window.new_envelopes())
            .map(|envelope| brenn_surface_test_fixtures::parse_envelope(envelope).body)
            .collect();
        windows.0.lock().unwrap().push(bodies);
        Ok(None)
    })
}

/// The `sync` port of every activation an entry was handed, in order.
#[derive(Clone, Default)]
struct Syncs(Arc<Mutex<Vec<Option<String>>>>);

impl Syncs {
    fn ports(&self) -> Vec<Option<String>> {
        self.0.lock().unwrap().clone()
    }
}

/// An entry that records which port — if any — each of its activations was a
/// sync call on.
fn recording_sync(syncs: &Syncs) -> ActivationEntry {
    let syncs = syncs.clone();
    Box::new(move |activation: &Activation, _: &mut PublishBuffer| {
        syncs.0.lock().unwrap().push(activation.sync.clone());
        Ok(None)
    })
}

/// An entry that publishes `body` on its output `port` every time it runs.
fn publishing(seen: &Seen, port: &str, body: &str) -> ActivationEntry {
    let (port, body) = (port.to_string(), body.to_string());
    entry(seen, move |_, buffer| {
        buffer
            .publish(&port, body.clone())
            .expect("the fixture binds the port it publishes on");
        Ok(None)
    })
}

/// An entry that publishes `body` on its output `port`, held until shortly after
/// the activation's own clock reading.
///
/// The offset is generous in wall-clock terms and invisible in virtual ones: it
/// must outlast the microseconds between the activation's reading and the flush's,
/// or the flush would publish it immediately and there would be no schedule to
/// release.
fn deferring(seen: &Seen, port: &str, body: &str) -> ActivationEntry {
    let (port, body) = (port.to_string(), body.to_string());
    entry(seen, move |activation, buffer| {
        // Only on an activation carrying something. The guaranteed mount
        // activation runs with empty windows, and parking a schedule there too
        // would put two on the channel for one delivery.
        if activation.ports.iter().all(|window| window.new_len() == 0) {
            return Ok(None);
        }
        let now = activation.now.expect("the page has a wall clock");
        buffer
            .publish_deferred(&port, body.clone(), now + 20)
            .expect("the fixture binds the port it publishes on");
        Ok(None)
    })
}

/// An entry that fails without dying: the instance keeps running.
fn erring(seen: &Seen) -> ActivationEntry {
    entry(seen, |_, _| {
        Err(ActivationError {
            message: "no thanks".to_string(),
        })
    })
}

/// Register `instance`'s entry, headless: no mount activation.
fn mount(running: &mut Running, instance: &str, entry: ActivationEntry) {
    running.send(RunnerCommand::RegisterActivation {
        instance: instance.to_string(),
        entry,
        mount: false,
    });
}

/// Register `instance`'s entry as a rendering instance: the loop owes it one
/// mount activation as soon as the entry is installed.
fn mount_rendering(running: &mut Running, instance: &str, entry: ActivationEntry) {
    running.send(RunnerCommand::RegisterActivation {
        instance: instance.to_string(),
        entry,
        mount: true,
    });
}

/// Wait for the page to subscribe `channel` and answer it, so a delivery there is
/// one the page will take.
async fn ack(controls: &Controls, feed: &mpsc::UnboundedSender<TransportEvent>, channel: &str) {
    wait_until(|| controls.subscribed().contains(&channel.to_string())).await;
    feed.unbounded_send(TransportEvent::Text(subscribe_result(channel, 0)))
        .unwrap();
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
    attach_with(running, feed, &doc()).await;
}

/// As [`attach`], under a document the test chose.
async fn attach_with(
    running: &mut Running,
    feed: &mpsc::UnboundedSender<TransportEvent>,
    document: &BindingsDocument,
) {
    attach_as(running, feed, welcome(), document).await;
}

/// As [`attach_with`], under attachment facts the test chose — the grants are
/// stated in the `Welcome`, not in the document.
async fn attach_as(
    running: &mut Running,
    feed: &mpsc::UnboundedSender<TransportEvent>,
    welcome: String,
    document: &BindingsDocument,
) {
    for text in [
        server_hello(),
        welcome,
        subscribe_result(CONFIG, 1),
        deliver(CONFIG, &document.to_body()),
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
        entry: Box::new(|_, _| Ok(None)),
        mount: false,
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
            entry: Box::new(|_, _| Ok(None)),
            mount: false,
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
    // The plane's reader, mounted before the close and still mounted through the
    // drain: routing the banner is only half of drawing it.
    let reader = Seen::default();
    mount(&mut running, "p2", quiet(&reader));
    wait_until(|| controls.subscribed().contains(&WIRE_TWO.to_string())).await;

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
    assert_eq!(
        reader.bodies(),
        vec![toast_body()],
        "and activated the reader that draws it"
    );
}

#[tokio::test(start_paused = true)]
async fn a_control_publish_after_a_fatal_still_routes() {
    let controls = Controls::new();
    let (feed, _closed, _writes) = controls.succeed();
    let mut running = spawn(&controls);
    attach(&mut running, &feed).await;
    let reader = Seen::default();
    mount(&mut running, "p2", quiet(&reader));
    wait_until(|| controls.subscribed().contains(&WIRE_TWO.to_string())).await;

    feed.unbounded_send(TransportEvent::Text(deliver(WIRE_ONE, "{}")))
        .unwrap();
    assert!(matches!(running.event().await, Event::Fatal { .. }));

    running.send(toast());
    let page = running.end().await;
    assert_eq!(toasts(&page), vec!["hello".to_string()]);
    assert_eq!(
        reader.bodies(),
        vec![toast_body()],
        "a post-fatal banner reaches the entry that draws it"
    );
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
    let (raw, _events, front) = raw_front(0);
    let mut runner = SurfaceRunner::new(
        SurfacePage::new(CONFIG.to_string(), EPOCH),
        config(),
        controls.connector(),
        front,
    );
    runner.emit(Event::WiringChanged);
    runner.emit(Event::WiringChanged);
    drop(raw);
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

// ── the activation pass ───────────────────────────────────────────────────

#[tokio::test(start_paused = true)]
async fn a_delivered_message_reaches_the_entry_that_reads_it() {
    let controls = Controls::new();
    let (feed, _closed, _writes) = controls.succeed();
    let mut running = spawn(&controls);
    attach(&mut running, &feed).await;

    let seen = Seen::default();
    mount(&mut running, "p1", quiet(&seen));
    ack(&controls, &feed, WIRE_ONE).await;
    feed.unbounded_send(TransportEvent::Text(deliver(WIRE_ONE, "hello")))
        .unwrap();

    wait_until(|| seen.bodies() == ["hello"]).await;
    running.task.abort();
}

/// **One catch-up pass is one activation.** The defect the batching exists to
/// fix is a retained window arriving as N frames and waking the reader N times;
/// this is the composition that stops it, asserted where "activation" is a real
/// observable rather than inferred from the window arithmetic.
///
/// Three properties compose here and none of them is enough alone: the driver
/// routes one frame per step, arrival moves no position, and the activation pass
/// runs once per turn. A change to any one of them — a driver that splits a
/// frame, a runner that pumps a step per row, an intake that advances a position
/// mid-pass — restores the burst.
#[tokio::test(start_paused = true)]
async fn a_multi_row_pass_wakes_its_reader_once() {
    let controls = Controls::new();
    let (feed, _closed, _writes) = controls.succeed();
    let mut running = spawn(&controls);
    attach(&mut running, &feed).await;

    let windows = Windows::default();
    mount(&mut running, "p1", windowing(&windows));
    ack(&controls, &feed, WIRE_ONE).await;
    feed.unbounded_send(TransportEvent::Text(deliver_pass(
        WIRE_ONE,
        &[("m1", 1, 0x1), ("m2", 2, 0x2), ("m3", 3, 0x3)],
    )))
    .unwrap();

    wait_until(|| !windows.activations().is_empty()).await;
    settle().await;
    assert_eq!(
        windows.activations(),
        vec![
            Vec::<String>::new(),
            vec!["m1".to_string(), "m2".to_string(), "m3".to_string()]
        ],
        "the mount's guaranteed activation, then one pass, one activation, every \
         row of it in the new slice"
    );
    running.task.abort();
}

/// The contrast, and the half that must stay: three rows the peer wrote as three
/// frames are three delivery points and wake the reader three times. Coalescing
/// is the server's statement about one catch-up pass, not something the page
/// does to distinct live publishes.
#[tokio::test(start_paused = true)]
async fn three_separate_frames_wake_their_reader_three_times() {
    let controls = Controls::new();
    let (feed, _closed, _writes) = controls.succeed();
    let mut running = spawn(&controls);
    attach(&mut running, &feed).await;

    let windows = Windows::default();
    mount(&mut running, "p1", windowing(&windows));
    ack(&controls, &feed, WIRE_ONE).await;
    for (body, seq, id) in [("m1", 1, 0x1), ("m2", 2, 0x2), ("m3", 3, 0x3)] {
        feed.unbounded_send(TransportEvent::Text(deliver_at(WIRE_ONE, body, seq, id)))
            .unwrap();
    }

    wait_until(|| windows.activations().len() == 4).await;
    settle().await;
    assert_eq!(
        windows.activations(),
        vec![
            // The mount's guaranteed activation, ahead of any delivery.
            Vec::<String>::new(),
            vec!["m1".to_string()],
            vec!["m2".to_string()],
            vec!["m3".to_string()]
        ]
    );
    running.task.abort();
}

/// The other half of an activation: what the entry buffered commits, and a
/// transportable output's commit is a flush on the wire.
#[tokio::test(start_paused = true)]
async fn an_entrys_publish_is_flushed_to_the_peer() {
    let controls = Controls::new();
    let (feed, _closed, _writes) = controls.succeed();
    let mut running = spawn(&controls);
    attach(&mut running, &feed).await;

    let seen = Seen::default();
    mount(&mut running, "p1", publishing(&seen, "out", "made"));
    ack(&controls, &feed, WIRE_ONE).await;
    feed.unbounded_send(TransportEvent::Text(deliver(WIRE_ONE, "hello")))
        .unwrap();

    wait_until(|| !controls.batches().is_empty()).await;
    let [(_, bodies)] = &controls.batches()[..] else {
        panic!("one activation, one flush: {:?}", controls.batches())
    };
    assert_eq!(bodies, &["made".to_string()]);
    running.task.abort();
}

/// A confined commit is the delivery, so one component's activation is what makes
/// the next one ready — and the pass picks that up without waiting for anything to
/// arrive from the peer.
#[tokio::test(start_paused = true)]
async fn a_confined_publish_activates_the_sibling_that_reads_it() {
    let controls = Controls::new();
    let (feed, _closed, _writes) = controls.succeed();
    let mut running = spawn(&controls);
    attach(&mut running, &feed).await;

    let (writer, reader) = (Seen::default(), Seen::default());
    mount(
        &mut running,
        "p1",
        publishing(&writer, "notes", "passed on"),
    );
    mount(&mut running, "p2", quiet(&reader));
    ack(&controls, &feed, WIRE_ONE).await;
    feed.unbounded_send(TransportEvent::Text(deliver(WIRE_ONE, "hello")))
        .unwrap();

    wait_until(|| reader.bodies() == ["passed on"]).await;
    running.task.abort();
}

/// A panicking entry is a trap, which only the invocation boundary can tell from
/// an err — and a trap is terminal for its instance.
///
/// Both payload shapes `panic!` produces are driven, because the message is the
/// only answer an operator has to "failed *how*?" and recovering it is two
/// separate downcasts: `p1` panics with a literal (a `&'static str`) and `p2` with
/// a formatted message (a `String`), which is what a real component's panic
/// almost always carries.
#[tokio::test(start_paused = true)]
async fn a_trapped_entry_takes_its_instance_terminal() {
    let controls = Controls::new();
    let (feed, _closed, _writes) = controls.succeed();
    let mut running = spawn(&controls);
    attach(&mut running, &feed).await;

    mount(
        &mut running,
        "p1",
        Box::new(|_, _| panic!("the component fell over")),
    );
    ack(&controls, &feed, WIRE_ONE).await;
    feed.unbounded_send(TransportEvent::Text(deliver(WIRE_ONE, "hello")))
        .unwrap();

    assert!(matches!(
        running.event().await,
        Event::ActivationFailed { instance, message } if instance == "p1" && message.contains("fell over")
    ));
    assert!(matches!(
        running.event().await,
        Event::InstanceFailed { instance, reason } if instance == "p1" && reason.contains("fell over")
    ));

    mount(
        &mut running,
        "p2",
        Box::new(|_, _| panic!("the component fell over at rung {}", 7)),
    );
    ack(&controls, &feed, WIRE_TWO).await;
    feed.unbounded_send(TransportEvent::Text(deliver(WIRE_TWO, "hello")))
        .unwrap();

    assert!(matches!(
        running.event().await,
        Event::ActivationFailed { instance, message }
            if instance == "p2" && message == "the component fell over at rung 7"
    ));
    assert!(matches!(
        running.event().await,
        Event::InstanceFailed { instance, reason }
            if instance == "p2" && reason == "the component fell over at rung 7"
    ));
    running.task.abort();
}

/// A publish to a port outside the component's declared vocabulary is a
/// contract violation, and the seam that observes it ends the activation.
///
/// The entry stands in for the browser's port export, which answers the
/// contract's error words for a refusal and throws for a violation; a native
/// entry says the same thing with a panic, which is what `invoke_native` reads as
/// a trap. What is under test is the fold beyond that: trap ⇒ the instance is
/// terminal, with the violation's own reason on both events.
#[tokio::test(start_paused = true)]
async fn an_undeclared_publish_traps_and_takes_its_instance_terminal() {
    let controls = Controls::new();
    let (feed, _closed, _writes) = controls.succeed();
    let mut running = spawn(&controls);
    attach(&mut running, &feed).await;

    mount(
        &mut running,
        "p1",
        Box::new(
            |_, buffer| match buffer.publish("ghost", "hi".to_string()) {
                Err(PortFault::Undeclared(port)) => panic!("undeclared port {port}"),
                other => panic!("expected a vocabulary violation, got {other:?}"),
            },
        ),
    );
    ack(&controls, &feed, WIRE_ONE).await;
    feed.unbounded_send(TransportEvent::Text(deliver(WIRE_ONE, "hello")))
        .unwrap();

    assert!(matches!(
        running.event().await,
        Event::ActivationFailed { instance, message }
            if instance == "p1" && message.contains("undeclared port \"ghost\"")
    ));
    assert!(matches!(
        running.event().await,
        Event::InstanceFailed { instance, reason }
            if instance == "p1" && reason.contains("undeclared port \"ghost\"")
    ));
    running.task.abort();
}

/// A declared port the deployer did not wire answers ok and delivers nothing: the
/// instance runs on, and the flush carries no entry for it.
///
/// `narrowed_doc` is exactly that shape — it is `reporting_doc` with `p1`'s wire
/// output binding removed, which leaves `out` in the vocabulary the components
/// declare and out of the bound-output table.
#[tokio::test(start_paused = true)]
async fn a_publish_to_an_unwired_declared_port_delivers_nothing_and_runs_on() {
    let controls = Controls::new();
    let (feed, _closed, _writes) = controls.succeed();
    let mut running = spawn(&controls);
    attach_with(&mut running, &feed, &narrowed_doc()).await;

    let seen = Seen::default();
    mount(&mut running, "p1", publishing(&seen, "out", "dropped"));
    ack(&controls, &feed, WIRE_ONE).await;
    feed.unbounded_send(TransportEvent::Text(deliver(WIRE_ONE, "hello")))
        .unwrap();

    wait_until(|| seen.count() == 1).await;
    assert!(
        !controls.written().iter().any(|w| w.contains("dropped")),
        "an unwired port publishes nowhere: {:?}",
        controls.written()
    );
    running.task.abort();
}

/// A reply answers a question, and an async activation asked none. An entry that
/// returns one anyway is a trap at the native seam exactly as it is at the
/// browser's — its buffer was built under a misapprehension, so the one thing that
/// must not happen is a flush.
///
/// The mount activation is the async activation used, because it needs nothing
/// arranged: it is guaranteed, it carries no sync port, and it runs before any
/// delivery could.
#[tokio::test(start_paused = true)]
async fn a_reply_to_an_async_activation_traps_at_the_native_seam() {
    let controls = Controls::new();
    let (feed, _closed, _writes) = controls.succeed();
    let mut running = spawn(&controls);
    attach(&mut running, &feed).await;

    mount(
        &mut running,
        "p1",
        Box::new(|_, buffer| {
            buffer
                .publish("out", "never flushed".to_string())
                .expect("the fixture binds the port it publishes on");
            Ok(Some("{\"cancel\":true}".to_string()))
        }),
    );

    assert!(matches!(
        running.event().await,
        Event::ActivationFailed { instance, message }
            if instance == "p1" && message.contains("no sync port")
    ));
    assert!(matches!(
        running.event().await,
        Event::InstanceFailed { instance, reason }
            if instance == "p1" && reason.contains("no sync port")
    ));
    assert!(
        !controls
            .written()
            .iter()
            .any(|frame| frame.contains("never flushed")),
        "the buffer of a trapped activation is discarded, not written"
    );
    running.task.abort();
}

#[tokio::test(start_paused = true)]
async fn an_erring_entry_is_reported_and_keeps_activating() {
    let controls = Controls::new();
    let (feed, _closed, _writes) = controls.succeed();
    let mut running = spawn(&controls);
    attach(&mut running, &feed).await;

    let seen = Seen::default();
    mount(&mut running, "p1", erring(&seen));
    ack(&controls, &feed, WIRE_ONE).await;
    feed.unbounded_send(TransportEvent::Text(deliver(WIRE_ONE, "one")))
        .unwrap();

    assert!(matches!(
        running.event().await,
        Event::ActivationFailed { instance, message } if instance == "p1" && message == "no thanks"
    ));
    feed.unbounded_send(TransportEvent::Text(deliver_at(WIRE_ONE, "two", 2, 0x9002)))
        .unwrap();
    wait_until(|| seen.bodies() == ["one", "two"]).await;
    running.task.abort();
}

/// The containment story, driven: a component that republishes what it reads is
/// ready again the instant its own flush commits, so its arm never stops being
/// ready. The socket above it must still be read and the platform half's commands
/// must still be served — a livelock the page survives, not a hang it does not.
#[tokio::test(start_paused = true)]
async fn a_component_that_republishes_what_it_reads_does_not_starve_the_page() {
    let controls = Controls::new();
    let (feed, closed, _writes) = controls.succeed();
    let mut running = spawn(&controls);
    attach_with(&mut running, &feed, &spin_doc()).await;

    let seen = Seen::default();
    mount(&mut running, "p1", publishing(&seen, "notes", "again"));
    ack(&controls, &feed, WIRE_ONE).await;
    feed.unbounded_send(TransportEvent::Text(deliver(WIRE_ONE, "kick")))
        .unwrap();
    wait_until(|| seen.count() > 20).await;

    // The transport is still read while it spins.
    feed.unbounded_send(TransportEvent::Text(deliver_at(
        WIRE_ONE, "arrived", 2, 0x9002,
    )))
    .unwrap();
    wait_until(|| seen.saw("arrived")).await;

    // And so is the control channel.
    running.send(RunnerCommand::Close);
    wait_until(|| closed.load(Ordering::SeqCst)).await;
    running.task.abort();
}

/// The other half of that story, and the one the pass's budget is written for: a
/// pass gets one activation per registered instance and the page's pick rotates,
/// so a component whose own flush re-readies it cannot absorb every pass. A
/// sibling with a message waiting is activated regardless — otherwise the page
/// would keep reading its socket and answering commands while quietly delivering
/// nothing to a mounted, healthy component.
#[tokio::test(start_paused = true)]
async fn a_spinning_component_does_not_starve_its_sibling() {
    let controls = Controls::new();
    let (feed, _closed, _writes) = controls.succeed();
    let mut running = spawn(&controls);
    attach_with(&mut running, &feed, &spin_doc()).await;

    let spinner = Seen::default();
    let reader = Seen::default();
    mount(&mut running, "p1", publishing(&spinner, "notes", "again"));
    mount(&mut running, "p2", quiet(&reader));
    ack(&controls, &feed, WIRE_ONE).await;
    ack(&controls, &feed, WIRE_TWO).await;
    feed.unbounded_send(TransportEvent::Text(deliver(WIRE_ONE, "kick")))
        .unwrap();
    wait_until(|| spinner.count() > 20).await;

    feed.unbounded_send(TransportEvent::Text(deliver_at(
        WIRE_TWO,
        "for the sibling",
        1,
        0x9101,
    )))
    .unwrap();
    wait_until(|| reader.saw("for the sibling")).await;
    running.task.abort();
}

/// The retry deadline's whole round trip through the runner: an activation's flush
/// is metered by the peer, its head is re-parked, the deadline the page states is
/// armed on the driver, and the fire comes back as the turn that re-offers it.
#[tokio::test(start_paused = true)]
async fn a_metered_flush_is_re_offered_when_the_retry_deadline_fires() {
    let controls = Controls::new();
    let (feed, _closed, _writes) = controls.succeed();
    let mut running = spawn(&controls);
    attach(&mut running, &feed).await;

    let seen = Seen::default();
    mount(&mut running, "p1", publishing(&seen, "out", "made"));
    ack(&controls, &feed, WIRE_ONE).await;
    feed.unbounded_send(TransportEvent::Text(deliver(WIRE_ONE, "hello")))
        .unwrap();
    wait_until(|| !controls.batches().is_empty()).await;

    let (correlation, _) = controls.batches()[0].clone();
    feed.unbounded_send(TransportEvent::Text(frame(
        &ServerFrame::PublishBatchResult {
            correlation,
            outcome: PublishBatchOutcome::RateLimited,
        },
    )))
    .unwrap();

    wait_until(|| controls.batches().len() == 2).await;
    let batches = controls.batches();
    assert_eq!(batches[1].1, vec!["made".to_string()], "the same flush");
    assert_ne!(
        batches[1].0, correlation,
        "under a correlation of its own, the first one having been answered"
    );
    running.task.abort();
}

/// The release deadline's round trip, and the second half of the page's two clocks:
/// an activation parks a confined publish, the deadline is armed on the driver, and
/// the fire releases it into the store its reader is positioned on — which activates
/// that reader.
#[tokio::test(start_paused = true)]
async fn a_deferred_publish_reaches_its_reader_when_the_release_deadline_fires() {
    let controls = Controls::new();
    let (feed, _closed, _writes) = controls.succeed();
    let mut running = spawn(&controls);
    attach(&mut running, &feed).await;

    let (writer, reader) = (Seen::default(), Seen::default());
    mount(&mut running, "p1", deferring(&writer, "notes", "later"));
    mount(&mut running, "p2", quiet(&reader));
    ack(&controls, &feed, WIRE_ONE).await;
    feed.unbounded_send(TransportEvent::Text(deliver(WIRE_ONE, "hello")))
        .unwrap();
    wait_until(|| writer.saw("hello")).await;

    // The wall clock the release is judged against is the real one, so what the
    // fire waits for is real milliseconds passing — virtual time only decides how
    // often the runner asks.
    wait_until(|| reader.bodies() == ["later"]).await;
    running.task.abort();
}

/// The publish channel's whole round trip: the handle's gate admits it, the runner
/// mints its stamp and hands it the page, the page puts it on the wire under a
/// correlation of its own, and the peer's answer comes back to the caller under
/// *theirs*.
#[tokio::test(start_paused = true)]
async fn a_publish_through_the_handle_reaches_the_peer_and_is_answered() {
    let controls = Controls::new();
    let (feed, _closed, _writes) = controls.succeed();
    let mut running = spawn(&controls);
    attach(&mut running, &feed).await;

    let correlation = running
        .handle
        .publish("p1", "out", "written".to_string())
        .expect("a configured page admits a bound output port");

    wait_until(|| !controls.publishes().is_empty()).await;
    let published = controls.publishes().remove(0);
    assert_eq!(published.channel, WIRE_OUT);
    assert_eq!(
        published.attribution,
        Some("p1".to_string()),
        "attributed to the component whose port it is"
    );
    assert_eq!(published.body, "written");

    feed.unbounded_send(TransportEvent::Text(frame(&ServerFrame::PublishResult {
        correlation: published.correlation,
        outcome: PublishOutcome::Ok,
    })))
    .unwrap();
    assert_eq!(
        running.event().await,
        Event::PublishResult {
            instance: "p1".to_string(),
            port: "out".to_string(),
            correlation,
            status: PublishStatus::Ok,
        }
    );
    running.task.abort();
}

/// The gate is a snapshot of the page, and a run that never refreshed it would
/// leave it at its default — which refuses everything, so the refusal alone proves
/// nothing. What it must refuse is a page that has no wiring *yet*, and admit the
/// same port the moment the document lands.
#[tokio::test(start_paused = true)]
async fn the_gate_refuses_a_publish_until_the_document_lands() {
    let controls = Controls::new();
    let (feed, _closed, _writes) = controls.succeed();
    let mut running = spawn(&controls);

    assert_eq!(
        running.handle.publish("p1", "out", "early".to_string()),
        Err(PublishReject::NotConnected)
    );
    settle().await;
    assert!(
        controls.publishes().is_empty(),
        "a refused publish is answered on the caller's own stack and queues nothing"
    );

    attach(&mut running, &feed).await;
    assert!(
        running
            .handle
            .publish("p1", "out", "late".to_string())
            .is_ok()
    );
    running.task.abort();
}

/// The detach edge, both ways: the wire port stops being publishable the moment the
/// attachment goes, and the confined one does not — a page whose link is down is
/// still a page, and its own planes are what chrome draws the outage from.
#[tokio::test(start_paused = true)]
async fn a_detach_refreshes_the_gate_without_closing_the_confined_port() {
    let controls = Controls::new();
    let (feed, _closed, _writes) = controls.succeed();
    let mut running = spawn(&controls);
    attach(&mut running, &feed).await;
    let reader = Seen::default();
    mount(&mut running, "p2", quiet(&reader));

    feed.unbounded_send(TransportEvent::Closed {
        code: None,
        reason: String::new(),
    })
    .unwrap();
    assert!(matches!(running.event().await, Event::Disconnected { .. }));

    assert_eq!(
        running.handle.publish("p1", "out", "gone".to_string()),
        Err(PublishReject::NotConnected),
        "the wire port needs an attachment the peer is judging against"
    );
    running
        .handle
        .publish("p1", "notes", "offline".to_string())
        .expect("a page-local port never touches the wire");
    wait_until(|| reader.saw("offline")).await;
    running.task.abort();
}

/// The close edge, which is the one that ends an attachment without the
/// connection reporting anything — every other gate refresh hangs off an event
/// the connection produced. So this is the only edge whose refresh nothing else
/// would make, and a run that dropped it would keep admitting wire publishes on a
/// page that has closed itself: each one queued, each one refused late by the
/// drain instead of synchronously on the caller's own stack.
///
/// The confined port survives the close for the same reason it survives a detach,
/// and the drain routes it and activates the reader — a terminal page is still a
/// page.
#[tokio::test(start_paused = true)]
async fn a_close_refreshes_the_gate_without_closing_the_confined_port() {
    let controls = Controls::new();
    let (feed, closed, _writes) = controls.succeed();
    let mut running = spawn(&controls);
    attach(&mut running, &feed).await;
    let reader = Seen::default();
    mount(&mut running, "p2", quiet(&reader));
    wait_until(|| controls.subscribed().contains(&WIRE_TWO.to_string())).await;

    running.send(RunnerCommand::Close);
    wait_until(|| closed.load(Ordering::SeqCst)).await;
    settle().await;

    assert_eq!(
        running.handle.publish("p1", "out", "gone".to_string()),
        Err(PublishReject::NotConnected),
        "a page that closed itself has no wire to admit a publish for"
    );
    running
        .handle
        .publish("p1", "notes", "still here".to_string())
        .expect("a page-local port never touches the wire");
    wait_until(|| reader.saw("still here")).await;
    running.task.abort();
}

/// A second document over a running page moves both things the handle's snapshot
/// answers from, and the run refreshes it off `WiringChanged` for exactly that
/// reason: a port the new document dropped must stop being admitted, and a floor
/// it raised must start swallowing the reports below it.
#[tokio::test(start_paused = true)]
async fn a_second_document_refreshes_the_gate_and_the_report_floor() {
    let controls = Controls::new();
    let (feed, _closed, _writes) = controls.succeed();
    let mut running = spawn(&controls);
    attach_with(&mut running, &feed, &reporting_doc()).await;
    running
        .handle
        .publish("p1", "out", "bound".to_string())
        .expect("the first document binds the port");

    feed.unbounded_send(TransportEvent::Text(deliver_at(
        CONFIG,
        &narrowed_doc().to_body(),
        2,
        0x9201,
    )))
    .unwrap();
    assert!(matches!(running.event().await, Event::WiringChanged));

    assert_eq!(
        running.handle.publish("p1", "out", "unbound".to_string()),
        Err(PublishReject::UnboundPort),
        "the wiring in force no longer binds the port"
    );
    running
        .handle
        .report(LogLevel::Warn, "component:protobar", "under", Some("p1"));
    running
        .handle
        .report(LogLevel::Error, "component:protobar", "over", Some("p1"));
    wait_until(|| {
        controls
            .publishes()
            .iter()
            .any(|published| published.body.contains("over"))
    })
    .await;
    settle().await;
    let published = controls.publishes();
    assert!(
        !published
            .iter()
            .any(|published| published.body.contains("under")),
        "the raised floor is read off the same refreshed snapshot"
    );
    assert!(
        !published
            .iter()
            .any(|published| published.body == "unbound"),
        "a refused publish is answered on the caller's own stack and queues nothing"
    );
    running.task.abort();
}

/// A report is an ordinary publish on the channel the wiring names, gated by the
/// floor it states — and the floor is read off the same snapshot the publish gate
/// is, so a run that never refreshed it would publish nothing at all.
#[tokio::test(start_paused = true)]
async fn a_report_clearing_the_floor_reaches_the_error_channel() {
    let controls = Controls::new();
    let (feed, _closed, _writes) = controls.succeed();
    let mut running = spawn(&controls);
    attach_with(&mut running, &feed, &reporting_doc()).await;

    running
        .handle
        .report(LogLevel::Debug, "component:protobar", "noise", Some("p1"));
    running
        .handle
        .report(LogLevel::Error, "component:protobar", "boom", Some("p1"));

    wait_until(|| !controls.publishes().is_empty()).await;
    settle().await;
    let published = controls.publishes();
    assert_eq!(published.len(), 1, "the debug report never left the handle");
    assert_eq!(published[0].channel, ERRORS);
    assert_eq!(
        published[0].attribution,
        Some("p1".to_string()),
        "attributed to the component the report is about"
    );
    assert!(published[0].body.contains("boom"));
    running.task.abort();
}

/// The alert plane end to end. Its own channel, because a paging event must not
/// queue behind a publish flood, and the grant is the attachment's rather than the
/// wiring's.
#[tokio::test(start_paused = true)]
async fn an_alert_reaches_the_peer_on_a_granted_attachment() {
    let controls = Controls::new();
    let (feed, _closed, _writes) = controls.succeed();
    let mut running = spawn(&controls);
    attach_as(
        &mut running,
        &feed,
        welcome_under(alert_granting_facts()),
        &doc(),
    )
    .await;

    running.handle.alert(
        None,
        AlertSeverity::Warning,
        "wall down",
        "the wall is down",
    );

    wait_until(|| !controls.alerts().is_empty()).await;
    assert_eq!(
        controls.alerts(),
        vec![("wall down".to_string(), "the wall is down".to_string())]
    );
    running.task.abort();
}

/// The telemetry plane: both documents, on the channels the wiring names, published
/// under the bare identity — the peer admits no component-attributed writer on
/// them.
#[tokio::test(start_paused = true)]
async fn the_two_telemetry_documents_go_out_unattributed() {
    let controls = Controls::new();
    let (feed, _closed, _writes) = controls.succeed();
    let mut running = spawn(&controls);
    attach(&mut running, &feed).await;

    running.handle.send_geometry(1_280, 720, 2.0);
    running
        .handle
        .send_status(Vec::new(), 42, StatusCounters::default());

    wait_until(|| controls.publishes().len() == 2).await;
    let published = controls.publishes();
    assert_eq!(published[0].channel, GEOMETRY);
    assert_eq!(published[1].channel, STATUS);
    assert!(
        published.iter().all(|p| p.attribution.is_none()),
        "the surface writes its own documents as itself"
    );
    assert!(published[0].body.contains("1280"));
    assert!(published[1].body.contains("42"));
    running.task.abort();
}

/// A publish the gate admitted before the attachment ended is still owed a
/// disposition: its caller holds a correlation and has nothing else to wait for.
/// The gate refresh closes the window the moment the run notices, so the queued
/// one takes the ordinary turn in the drain and comes back with the page's own
/// answer — the same one the refreshed gate now gives synchronously.
#[tokio::test(start_paused = true)]
async fn a_straggler_publish_after_the_attachment_ended_is_answered() {
    let controls = Controls::new();
    let (_feed, _closed, _writes) = controls.succeed();
    let mut running = spawn_raw(&controls);

    running
        .front
        .control_tx
        .try_send(RunnerCommand::Close)
        .expect("the channel has room");
    running
        .front
        .publish_tx
        .try_send(PublishSlot::Publish(PublishCommand {
            correlation: 7,
            instance: "p1".to_string(),
            port: "out".to_string(),
            body: "stranded".to_string(),
            urgency: None,
        }))
        .expect("the channel has room");

    assert_eq!(
        running.event().await,
        Event::PublishResult {
            instance: "p1".to_string(),
            port: "out".to_string(),
            correlation: 7,
            status: PublishStatus::NotConnected,
        }
    );
    running.end().await;
}

/// The best-effort planes after the attachment ended: absorbed, not answered and
/// not queued for a wire that is gone — and none of them keeps the drain from
/// ending once every sender has dropped.
#[tokio::test(start_paused = true)]
async fn the_best_effort_planes_are_absorbed_after_the_attachment_ended() {
    let controls = Controls::new();
    let (_feed, _closed, _writes) = controls.succeed();
    let mut running = spawn_raw(&controls);

    running
        .front
        .control_tx
        .try_send(RunnerCommand::Close)
        .expect("the channel has room");
    running
        .front
        .alert_tx
        .try_send(AlertCommand {
            attribution: None,
            severity: AlertSeverity::Warning,
            title: "late".to_string(),
            body: "later".to_string(),
        })
        .expect("the channel has room");
    running
        .front
        .telemetry_tx
        .try_send(TelemetryCommand::Status {
            instances: Vec::new(),
            uptime_secs: 1,
            counters: StatusCounters::default(),
        })
        .expect("the channel has room");

    running.end().await;
    assert!(
        controls.alerts().is_empty(),
        "nothing is composed against an attachment that has ended"
    );
    assert!(controls.publishes().is_empty());
}

/// A rendering instance's registration is answered with one mount activation on
/// the reserved port, before anything else is delivered. The mount settles the
/// scheduler's debt, so no redundant empty activation follows.
#[tokio::test(start_paused = true)]
async fn a_rendering_registration_is_answered_with_the_mount_activation() {
    let controls = Controls::new();
    let (feed, _closed, _writes) = controls.succeed();
    let mut running = spawn(&controls);
    attach(&mut running, &feed).await;

    let syncs = Syncs::default();
    mount_rendering(&mut running, "p1", recording_sync(&syncs));

    wait_until(|| !syncs.ports().is_empty()).await;
    assert_eq!(
        syncs.ports(),
        vec![Some(MOUNT_SYNC_PORT.to_string())],
        "the first call is the mount, and it is the only call so far"
    );

    ack(&controls, &feed, WIRE_ONE).await;
    feed.unbounded_send(TransportEvent::Text(deliver(WIRE_ONE, "hello")))
        .unwrap();
    wait_until(|| syncs.ports().len() > 1).await;
    assert_eq!(
        syncs.ports(),
        vec![Some(MOUNT_SYNC_PORT.to_string()), None],
        "the delivery is an ordinary activation, and the mount did not repeat"
    );
    running.task.abort();
}

/// A rendering instance may register before the page's first bindings document —
/// registration is admitted with the wiring still in flight — and its mount is
/// owed, not dropped. The document that lands settles the debt, so the UI is
/// built before any delivery reaches the component.
#[tokio::test(start_paused = true)]
async fn a_mount_owed_before_the_wiring_is_settled_by_the_document() {
    let controls = Controls::new();
    let (feed, _closed, _writes) = controls.succeed();
    let mut running = spawn(&controls);

    // Everything of the handshake except the document, so the page is attached
    // and has no wiring.
    for text in [server_hello(), welcome(), subscribe_result(CONFIG, 1)] {
        feed.unbounded_send(TransportEvent::Text(text)).unwrap();
    }
    wait_until(|| controls.subscribed() == vec![CONFIG.to_string()]).await;

    let syncs = Syncs::default();
    mount_rendering(&mut running, "p1", recording_sync(&syncs));
    // Served before the document is fed, so the mount is genuinely attempted
    // against a page with no wiring — which is the window under test, and which
    // the loop's socket-first bias would otherwise close.
    settle().await;
    assert!(
        syncs.ports().is_empty(),
        "there is nothing to window against yet, so nothing was called"
    );

    feed.unbounded_send(TransportEvent::Text(deliver(CONFIG, &doc().to_body())))
        .unwrap();
    assert!(matches!(running.event().await, Event::Connected { .. }));
    ack(&controls, &feed, WIRE_ONE).await;
    feed.unbounded_send(TransportEvent::Text(deliver(WIRE_ONE, "hello")))
        .unwrap();

    wait_until(|| syncs.ports().len() > 1).await;
    assert_eq!(
        syncs.ports(),
        vec![Some(MOUNT_SYNC_PORT.to_string()), None],
        "the owed mount is the instance's first call, ahead of the delivery"
    );
    running.task.abort();
}

/// The other arm of the owed-mount rule: an instance that was withdrawn while
/// its mount was owed is owed nothing, so the document that lands drops the debt
/// instead of re-queueing it against an instance with no entry.
///
/// The re-registration afterwards is the other half of the claim — the drop is a
/// drop of one debt, not of the loop's willingness to mount.
#[tokio::test(start_paused = true)]
async fn a_mount_owed_by_a_withdrawn_instance_is_dropped_not_retried() {
    let controls = Controls::new();
    let (feed, _closed, _writes) = controls.succeed();
    let mut running = spawn(&controls);

    // Attached with no document, so the registration's mount is owed rather
    // than served.
    for text in [server_hello(), welcome(), subscribe_result(CONFIG, 1)] {
        feed.unbounded_send(TransportEvent::Text(text)).unwrap();
    }
    wait_until(|| controls.subscribed() == vec![CONFIG.to_string()]).await;

    let syncs = Syncs::default();
    mount_rendering(&mut running, "p1", recording_sync(&syncs));
    settle().await;
    assert!(syncs.ports().is_empty(), "the mount is owed, not served");

    running.send(RunnerCommand::DeregisterActivation {
        instance: "p1".to_string(),
    });
    // Served before the document is fed: the loop's socket-first bias would
    // otherwise let the owed mount run against the entry still installed, and the
    // withdrawal under test would never be the state the mount met.
    settle().await;

    feed.unbounded_send(TransportEvent::Text(deliver(CONFIG, &doc().to_body())))
        .unwrap();
    assert!(matches!(running.event().await, Event::Connected { .. }));
    settle().await;
    assert!(
        syncs.ports().is_empty(),
        "the withdrawn instance holds no entry, so its owed mount is dropped"
    );

    // The debt was dropped, not the mechanism: a fresh registration mounts.
    mount_rendering(&mut running, "p1", recording_sync(&syncs));
    wait_until(|| !syncs.ports().is_empty()).await;
    assert_eq!(syncs.ports(), vec![Some(MOUNT_SYNC_PORT.to_string())]);
    running.task.abort();
}

/// A headless registration asks for no mount activation.
#[tokio::test(start_paused = true)]
async fn a_headless_registration_gets_no_mount_activation() {
    let controls = Controls::new();
    let (feed, _closed, _writes) = controls.succeed();
    let mut running = spawn(&controls);
    attach(&mut running, &feed).await;

    let syncs = Syncs::default();
    mount(&mut running, "p1", recording_sync(&syncs));

    wait_until(|| !syncs.ports().is_empty()).await;
    assert_eq!(
        syncs.ports(),
        vec![None],
        "nothing in a headless instance's first call names a sync port"
    );
    running.task.abort();
}

/// The mount activation is a pass like any other: input already pending when the
/// instance registers is windowed *in* the mount call, beside the request.
///
/// A component that assumed an empty mount would silently drop that input, which
/// is why the contract forbids assuming why you woke.
#[tokio::test(start_paused = true)]
async fn the_mount_activation_windows_the_input_already_pending() {
    let controls = Controls::new();
    let (feed, _closed, _writes) = controls.succeed();
    let mut running = spawn(&controls);
    attach(&mut running, &feed).await;

    // `p1` publishes before `p2` registers, so its output is already in the
    // store when `p2`'s mount fires.
    let writer = Seen::default();
    mount(&mut running, "p1", publishing(&writer, "notes", "waiting"));

    let seen = Seen::default();
    let syncs = Syncs::default();
    let recorder = {
        let syncs = syncs.clone();
        entry(&seen, move |activation, _| {
            syncs.0.lock().unwrap().push(activation.sync.clone());
            Ok(None)
        })
    };
    mount_rendering(&mut running, "p2", recorder);

    wait_until(|| !seen.bodies().is_empty()).await;
    assert_eq!(syncs.ports(), vec![Some(MOUNT_SYNC_PORT.to_string())]);
    assert_eq!(
        seen.bodies(),
        vec!["waiting".to_string(), MOUNT_REQUEST_BODY.to_string()],
        "the mount call carries the input the instance already had, and the \
         synthesized request rides as one more window beside it"
    );
    running.task.abort();
}

/// A trap inside the mount activation is an ordinary trap: terminal for the
/// instance, contained to it, and reported as the instance's own death.
#[tokio::test(start_paused = true)]
async fn a_trap_in_the_mount_activation_kills_the_instance() {
    let controls = Controls::new();
    let (feed, _closed, _writes) = controls.succeed();
    let mut running = spawn(&controls);
    attach(&mut running, &feed).await;

    mount_rendering(
        &mut running,
        "p1",
        Box::new(|_, _| panic!("the component fell over building its UI")),
    );

    assert!(matches!(
        running.event().await,
        Event::ActivationFailed { instance, message }
            if instance == "p1" && message.contains("building its UI")
    ));
    assert!(matches!(
        running.event().await,
        Event::InstanceFailed { instance, reason }
            if instance == "p1" && reason.contains("building its UI")
    ));
    running.task.abort();
}
