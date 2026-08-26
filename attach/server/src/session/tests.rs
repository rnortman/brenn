//! Session tests: the counters and the outbound write, the dispatch that routes
//! each frame to its plane, the alert plane, and whole attachments driven over a
//! duplex of websocket messages.
//!
//! The stub profile is what makes these transport tests: nothing here names a
//! component, a port, or an instance, and the socket is a pair of channels, so
//! the lifecycle is exercised without a browser, a page, or a server.

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr};
use std::pin::Pin;
use std::task::{Context, Poll};

use brenn_attach_proto::{
    ClientFrame, SUPPORTED_VERSIONS, Urgency, VersionRange, max_client_frame_bytes,
};
use brenn_db::Db;
use brenn_envelope::grants::AppCapability;
use brenn_lib::access::AppPolicy;
use brenn_lib::access::acl::ChannelMatcher;
use brenn_obs::alerting::{
    AlertDispatcher, AlertSeverity as NativeAlertSeverity, make_capturing_alerter_with_severity,
};
use futures::Sink;
use uuid::Uuid;

use super::*;
use crate::profile::SubscriptionFacts;
use crate::registry::{AttachSessionHandle, PUSH_QUEUE_FRAMES, SessionCaps};
use crate::test_support::{
    AttachCtxBuilder, TEST_ATTACHER as ATTACHER, TEST_BODY_BYTES, TestProfile,
};

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

/// A context over the given profile and alert dispatcher, with an outbound queue
/// the test reads frames back from.
fn ctx_with(
    profile: TestProfile,
    dispatcher: AlertDispatcher,
) -> (AttachSessionCtx, mpsc::Receiver<ServerFrame>) {
    AttachCtxBuilder::new(profile)
        .alert_dispatcher(dispatcher)
        .build()
}

/// The mutable half of one connection, over an unshared active set and a fixed
/// store incarnation — no test here reads either through the registry.
fn session_state(ctx: &AttachSessionCtx) -> SessionState {
    SessionState::new(
        ctx.profile.as_ref(),
        Arc::new(Mutex::new(HashSet::new())),
        1,
    )
}

/// Dispatch one frame, as the loop would.
async fn dispatch(
    ctx: &AttachSessionCtx,
    state: &mut SessionState,
    frame: &ClientFrame,
) -> FrameOutcome {
    let text = serde_json::to_string(frame).expect("frame serializes");
    handle_client_frame(ctx, &text, state).await
}

fn violation_detail(outcome: FrameOutcome) -> String {
    match outcome {
        FrameOutcome::Violation(detail) => detail,
        FrameOutcome::Continue => panic!("expected a violation, got Continue"),
        FrameOutcome::Disconnect => panic!("expected a violation, got Disconnect"),
    }
}

// ---------------------------------------------------------------------------
// Counters and the outbound write
// ---------------------------------------------------------------------------

/// The attacher's own publishes have no column, and the two counters move
/// together for the ones that do — the breakdown cannot stop tracking the total
/// it decomposes.
#[test]
fn publish_counters_move_the_total_and_the_column_together() {
    let mut counters = SessionCounters::default();

    counters.publish_ok(None);
    assert_eq!(counters.publishes, 1);
    assert!(counters.by_attribution.is_empty());

    counters.publish_ok(Some("clock"));
    counters.publish_ok(Some("clock"));
    assert_eq!(counters.publishes, 3);
    assert_eq!(
        counters.by_attribution.get("clock"),
        Some(&AttributionPublishCounters {
            publishes: 2,
            publish_rate_limited: 0,
        })
    );

    counters.publish_rate_limited(None);
    assert_eq!(counters.publish_rate_limited, 1);
    assert_eq!(counters.by_attribution.len(), 1);

    counters.publish_rate_limited(Some("clock"));
    assert_eq!(counters.publish_rate_limited, 2);
    assert_eq!(
        counters.by_attribution.get("clock"),
        Some(&AttributionPublishCounters {
            publishes: 2,
            publish_rate_limited: 1,
        })
    );
}

/// The count follows the enqueue, and a dead writer is a `Disconnect` that counts
/// nothing — the session tears down without a security event.
#[tokio::test]
async fn send_frame_counts_an_enqueue_and_reports_a_dead_writer() {
    let (tx, mut rx) = mpsc::channel::<ServerFrame>(4);
    let mut counters = SessionCounters::default();

    assert!(matches!(
        send_frame(&tx, ServerFrame::Heartbeat, &mut counters).await,
        FrameOutcome::Continue
    ));
    assert_eq!(counters.frames_out, 1);
    assert!(matches!(rx.try_recv(), Ok(ServerFrame::Heartbeat)));

    drop(rx);
    assert!(matches!(
        send_frame(&tx, ServerFrame::Heartbeat, &mut counters).await,
        FrameOutcome::Disconnect
    ));
    assert_eq!(
        counters.frames_out, 1,
        "a frame that never left is not counted"
    );
}

/// A hostile client's bytes reach the security log line and the phone alert
/// through this and nothing else: bounded length, and no raw control byte.
#[test]
fn sanitize_bounds_length_and_escapes_control_characters() {
    let long = sanitize_client_detail(&"a".repeat(500));
    assert_eq!(long, format!("{}...", "a".repeat(128)));

    let exact = sanitize_client_detail(&"b".repeat(128));
    assert_eq!(
        exact,
        "b".repeat(128),
        "an exactly-bounded input is unmarked"
    );

    let escaped = sanitize_client_detail("one\ntwo\rthree\u{1b}[0m");
    assert_eq!(escaped, "one\\ntwo\\rthree\\u{1b}[0m");
    assert!(
        !escaped.contains('\n') && !escaped.contains('\r') && !escaped.contains('\u{1b}'),
        "no raw control byte survives: {escaped:?}"
    );
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

/// Both ends agreed which schema is in force, so unparseable traffic is a bug or
/// tampering — and its bytes never reach the security log.
#[tokio::test]
async fn an_unparseable_frame_is_a_violation_that_never_echoes_it() {
    let (ctx, _rx) = ctx_with(TestProfile::new(), AlertDispatcher::noop().0);
    let mut state = session_state(&ctx);

    let outcome = handle_client_frame(
        &ctx,
        "{\"type\":\"Publish\",\"smuggled\":\"CANARY\"}",
        &mut state,
    )
    .await;

    let detail = violation_detail(outcome);
    assert_eq!(
        detail,
        format!("attacher surface:{ATTACHER} account dev: unparseable client frame")
    );
    assert!(!detail.contains("CANARY"), "payload echoed: {detail}");
}

/// The handshake happens once. A version in force is in force for the
/// connection's life, so a second `Hello` is not a renegotiation — it is a client
/// that is not speaking this protocol.
#[tokio::test]
async fn a_second_hello_is_a_violation() {
    let (ctx, _rx) = ctx_with(TestProfile::new(), AlertDispatcher::noop().0);
    let mut state = session_state(&ctx);

    let outcome = dispatch(
        &ctx,
        &mut state,
        &ClientFrame::Hello {
            versions: SUPPORTED_VERSIONS,
            ident: "test".to_string(),
        },
    )
    .await;

    assert!(violation_detail(outcome).ends_with("Hello after the handshake"));
}

/// Subscribe and Unsubscribe reach their plane, and each draws the one bucket
/// that gates both: with no tokens the rate answer wins over the handler's, which
/// is what proves the charge runs first.
#[tokio::test]
async fn subscription_frames_draw_one_bucket_before_their_handler_runs() {
    let (ctx, _rx) = ctx_with(TestProfile::new(), AlertDispatcher::noop().0);
    let mut state = session_state(&ctx);

    // With tokens in hand the frames reach their handler: an unsubscribable
    // channel and a non-active unsubscribe are the plane's own answers.
    let subscribe = ClientFrame::Subscribe {
        channel: "brenn:nonesuch".to_string(),
        push_depth: 1,
        retain_depth: 1,
        resume: None,
    };
    assert!(
        violation_detail(dispatch(&ctx, &mut state, &subscribe).await)
            .ends_with("Subscribe to unsubscribable channel brenn:nonesuch")
    );
    let unsubscribe = ClientFrame::Unsubscribe {
        channel: "brenn:nonesuch".to_string(),
    };
    assert!(
        violation_detail(dispatch(&ctx, &mut state, &unsubscribe).await)
            .ends_with("Unsubscribe of non-active subscription brenn:nonesuch")
    );

    // Both frames drew from the same bucket, so the burst is spent by the pair.
    let (empty_ctx, _rx) = ctx_with(
        TestProfile {
            subscribe_burst: 0,
            ..TestProfile::new()
        },
        AlertDispatcher::noop().0,
    );
    let mut empty = session_state(&empty_ctx);
    assert!(
        violation_detail(dispatch(&empty_ctx, &mut empty, &unsubscribe).await)
            .ends_with("Subscribe/Unsubscribe rate exceeded")
    );
}

/// A single publish reaches the publish plane, which answers the authority
/// question the profile owns.
#[tokio::test]
async fn a_publish_reaches_the_publish_plane() {
    let (ctx, _rx) = ctx_with(TestProfile::new(), AlertDispatcher::noop().0);
    let mut state = session_state(&ctx);

    let outcome = dispatch(
        &ctx,
        &mut state,
        &ClientFrame::Publish {
            channel: "brenn:nonesuch".to_string(),
            attribution: None,
            body: "{}".to_string(),
            urgency: Urgency::Normal,
            correlation: Some(7),
        },
    )
    .await;

    assert!(
        violation_detail(outcome).ends_with("Publish names unpublishable channel brenn:nonesuch"),
        "the publish plane's own answer"
    );
}

/// A batch reaches the batch handler, whose shape gates run before any authority
/// question — an empty flush is a frame a conforming attacher never sends.
#[tokio::test]
async fn a_publish_batch_reaches_the_batch_handler() {
    let (ctx, _rx) = ctx_with(TestProfile::new(), AlertDispatcher::noop().0);
    let mut state = session_state(&ctx);

    let outcome = dispatch(
        &ctx,
        &mut state,
        &ClientFrame::PublishBatch {
            attribution: None,
            correlation: 1,
            publishes: Vec::new(),
            deferred_ops: Vec::new(),
        },
    )
    .await;

    assert!(violation_detail(outcome).ends_with("empty PublishBatch"));
}

// ---------------------------------------------------------------------------
// Alert
// ---------------------------------------------------------------------------

/// An attacher declaring one sub-identity, `pager`, which holds the
/// per-component alert right, and one, `mute`, which does not — the two answers
/// the containment half of the plane has.
fn alert_profile(granted: bool) -> TestProfile {
    TestProfile {
        alert_granted: granted,
        declared: ["pager".to_string(), "mute".to_string()]
            .into_iter()
            .collect(),
        alertable: ["pager".to_string()].into_iter().collect(),
        ..TestProfile::new()
    }
}

/// An alert plane fixture: the granted flag over [`alert_profile`], and a
/// dispatcher whose alerts the test can read back.
#[allow(clippy::type_complexity)]
fn alert_fixture(
    granted: bool,
) -> (
    AttachSessionCtx,
    SessionState,
    AlertDispatcher,
    Arc<Mutex<Vec<(NativeAlertSeverity, String, String)>>>,
) {
    let (dispatcher, captured, _handle) = make_capturing_alerter_with_severity();
    let (ctx, _rx) = ctx_with(alert_profile(granted), dispatcher.clone());
    let state = session_state(&ctx);
    (ctx, state, dispatcher, captured)
}

/// Deny-by-default: the grant is advertised in `Welcome` and a conforming
/// attacher suppresses ungranted alerts itself, so one arriving is not conforming.
#[tokio::test]
async fn an_ungranted_alert_is_a_violation() {
    let (ctx, mut state, _dispatcher, captured) = alert_fixture(false);

    let outcome = handle_alert(
        &ctx,
        &mut state.buckets.alert,
        &mut state.counters,
        None,
        ProtoAlertSeverity::Critical,
        "title",
        "body",
    );

    assert!(
        violation_detail(outcome).ends_with("Alert from an attacher without the alert grant"),
        "the grant is checked first"
    );
    assert!(captured.lock().expect("captured").is_empty());
}

/// The plane is opt-in and its client is expected to conform, so an oversized
/// field is a violation — and the payload is never echoed into the detail.
#[tokio::test]
async fn an_oversized_alert_is_a_violation_that_never_echoes_it() {
    let (ctx, mut state, _dispatcher, _captured) = alert_fixture(true);

    let detail = violation_detail(handle_alert(
        &ctx,
        &mut state.buckets.alert,
        &mut state.counters,
        None,
        ProtoAlertSeverity::Info,
        &"CANARY".repeat(MAX_ALERT_TITLE_BYTES),
        "body",
    ));

    assert!(detail.contains("Alert field exceeds size cap"));
    assert!(!detail.contains("CANARY"), "payload echoed: {detail}");
}

/// The dispatched alert carries what the operator needs to act: the severity the
/// attacher declared, a title it cannot spell the prefix of, and the attribution
/// its body cannot carry.
///
/// Every severity, not one: a transposed arm of the 1:1 map — the bug a
/// three-arm match invites, and a likely one when the WIT vocabulary next gains a
/// level — downgrades a page to informational, and the operator finds out by not
/// being paged.
#[tokio::test]
async fn a_granted_alert_dispatches_prefixed_and_attributed() {
    let cases = [
        (ProtoAlertSeverity::Info, NativeAlertSeverity::Info),
        (ProtoAlertSeverity::Warning, NativeAlertSeverity::Warning),
        (ProtoAlertSeverity::Critical, NativeAlertSeverity::Critical),
    ];
    for (wire, native) in cases {
        let (ctx, mut state, dispatcher, captured) = alert_fixture(true);

        assert!(matches!(
            handle_alert(
                &ctx,
                &mut state.buckets.alert,
                &mut state.counters,
                None,
                wire,
                "disk filling",
                "92% used",
            ),
            FrameOutcome::Continue
        ));
        dispatcher.flush().await;

        let alerts = captured.lock().expect("captured");
        assert_eq!(alerts.len(), 1);
        let (severity, title, body) = &alerts[0];
        assert_eq!(
            std::mem::discriminant(severity),
            std::mem::discriminant(&native),
            "{wire:?} dispatched as {severity}"
        );
        assert_eq!(title, &format!("Attacher surface:{ATTACHER}: disk filling"));
        assert!(body.starts_with("92% used\n"));
        assert!(body.contains(&format!("attacher=surface:{ATTACHER}")));
        assert!(body.contains("account=dev"));
        assert_eq!(state.counters.alerts_dispatched, 1);
    }
}

/// Beyond the burst an alert is dropped, counted, and warned — never a kill. A
/// legitimately unhealthy attacher must not lose its attachment for being noisy.
#[tokio::test]
async fn alerts_beyond_the_burst_are_dropped_not_fatal() {
    let (ctx, mut state, dispatcher, captured) = alert_fixture(true);

    for _ in 0..ALERT_BURST + 3 {
        assert!(matches!(
            handle_alert(
                &ctx,
                &mut state.buckets.alert,
                &mut state.counters,
                None,
                ProtoAlertSeverity::Info,
                "noisy",
                "again",
            ),
            FrameOutcome::Continue
        ));
    }
    dispatcher.flush().await;

    assert_eq!(state.counters.alerts_dispatched, u64::from(ALERT_BURST));
    assert_eq!(state.counters.alerts_suppressed, 3);
    assert_eq!(
        captured.lock().expect("captured").len(),
        ALERT_BURST as usize
    );
}

/// An `Alert` frame reaches the alert plane through the dispatch, so the grant
/// gate governs the frame and not just the function.
#[tokio::test]
async fn an_alert_frame_reaches_the_alert_plane() {
    let (ctx, _rx) = ctx_with(TestProfile::new(), AlertDispatcher::noop().0);
    let mut state = session_state(&ctx);

    let outcome = dispatch(
        &ctx,
        &mut state,
        &ClientFrame::Alert {
            attribution: None,
            severity: brenn_attach_proto::AlertSeverity::Info,
            title: "t".to_string(),
            body: "b".to_string(),
        },
    )
    .await;

    assert!(violation_detail(outcome).ends_with("Alert from an attacher without the alert grant"));
}

/// The two scopes are independent rights, so an attributed alert names the
/// sub-identity that raised it rather than the attacher behind it: an operator
/// reading the page must be able to tell which component is broken.
#[tokio::test]
async fn an_attributed_alert_dispatches_under_the_component_that_raised_it() {
    let (ctx, mut state, dispatcher, captured) = alert_fixture(true);

    assert!(matches!(
        handle_alert(
            &ctx,
            &mut state.buckets.alert,
            &mut state.counters,
            Some("pager"),
            ProtoAlertSeverity::Warning,
            "camera unreachable",
            "no frames for 30s",
        ),
        FrameOutcome::Continue
    ));
    dispatcher.flush().await;

    let alerts = captured.lock().expect("captured");
    let (_severity, title, body) = &alerts[0];
    assert_eq!(
        title,
        &format!("Attacher surface:{ATTACHER}#pager: camera unreachable")
    );
    assert!(
        body.contains(&format!("attacher=surface:{ATTACHER}")),
        "the attacher stays in the body: it is the connection the operator kills"
    );
}

/// Undeclared attribution is a violation: admitting it would let a non-conforming
/// client page under a name no operator wrote.
#[tokio::test]
async fn an_alert_under_an_undeclared_attribution_is_a_violation() {
    let (ctx, mut state, _dispatcher, captured) = alert_fixture(true);

    let detail = violation_detail(handle_alert(
        &ctx,
        &mut state.buckets.alert,
        &mut state.counters,
        Some("ghost"),
        ProtoAlertSeverity::Info,
        "t",
        "b",
    ));

    assert!(detail.ends_with("Alert under undeclared attribution ghost"));
    assert!(captured.lock().expect("captured").is_empty());
}

/// Declared but ungranted is a violation too, not a drop: the client gates on the
/// component's own grant first, so a frame that arrives anyway means that gate was
/// bypassed.
#[tokio::test]
async fn an_alert_from_a_declared_but_ungranted_attribution_is_a_violation() {
    let (ctx, mut state, _dispatcher, captured) = alert_fixture(true);

    let detail = violation_detail(handle_alert(
        &ctx,
        &mut state.buckets.alert,
        &mut state.counters,
        Some("mute"),
        ProtoAlertSeverity::Info,
        "t",
        "b",
    ));

    assert!(detail.ends_with("Alert from declared attribution mute without the alert grant"));
    assert!(captured.lock().expect("captured").is_empty());
}

// ---------------------------------------------------------------------------
// Whole attachments
// ---------------------------------------------------------------------------

/// A socket the test drives: inbound messages it hands the session, outbound
/// messages it collects. Both halves are channels, so the whole lifecycle runs
/// without a network, a browser, or a server.
struct TestSocket {
    inbound: mpsc::UnboundedReceiver<Result<Message, axum::Error>>,
    outbound: mpsc::UnboundedSender<Message>,
}

impl Stream for TestSocket {
    type Item = Result<Message, axum::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inbound.poll_recv(cx)
    }
}

impl Sink<Message> for TestSocket {
    // The sink cannot fail: a test that drops the collector is not testing a
    // write error, and the writer's own error path has its own fixture.
    type Error = std::convert::Infallible;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: Message) -> Result<(), Self::Error> {
        let _ = self.outbound.send(item);
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

/// The heartbeat a test that is not about liveness attaches with — long enough
/// that nothing in it reaches a liveness tick.
const IDLE_HEARTBEAT_SECS: u32 = 3600;

/// One running attachment and the two ends of its socket.
struct Attachment {
    inbound: mpsc::UnboundedSender<Result<Message, axum::Error>>,
    outbound: mpsc::UnboundedReceiver<Message>,
    join: tokio::task::JoinHandle<AttachSessionOutcome>,
    registry: AttachRegistry,
    dispatcher: AlertDispatcher,
    captured: Arc<Mutex<Vec<(NativeAlertSeverity, String, String)>>>,
    /// The router's end of this attachment's push queue — live rows and
    /// deferred-view snapshots arriving from outside its task.
    push_tx: mpsc::Sender<SessionPush>,
    /// The router's eager-wake nudge.
    drain_notify: Arc<Notify>,
    /// The same bus the session holds, for seeding rows and reading windows.
    messenger: Arc<Messenger>,
}

impl Attachment {
    /// Start an attachment, registered and reading its socket, over the empty
    /// authority every dispatch and alert test wants.
    fn start() -> Self {
        Self::start_with(
            TestProfile::new(),
            brenn_messaging::testutils::empty_directory_messenger("test-origin"),
            AppPolicy::default(),
            IDLE_HEARTBEAT_SECS,
        )
    }

    /// Start an attachment over a stated profile, bus, delivery floor, and
    /// heartbeat cadence.
    fn start_with(
        profile: TestProfile,
        messenger: Arc<Messenger>,
        policy: AppPolicy,
        heartbeat_secs: u32,
    ) -> Self {
        let (dispatcher, captured, _handle) = make_capturing_alerter_with_severity();
        let (inbound, inbound_rx) = mpsc::unbounded_channel();
        let (outbound_tx, outbound) = mpsc::unbounded_channel();
        let registry = AttachRegistry::default();
        let (push_tx, push_rx) = mpsc::channel(PUSH_QUEUE_FRAMES);
        let mut handle = AttachSessionHandle::for_test("dev");
        handle.push_tx = push_tx.clone();
        let session_id = handle.session_id;
        let active_channels = handle.active_channels.clone();
        let drain_notify = handle.drain_notify.clone();
        let guard = registry
            .try_register(ATTACHER, handle, SessionCaps::UNCAPPED)
            .expect("uncapped registration");

        let join = tokio::spawn(run_attach_session(AttachSessionParams {
            profile: Arc::new(profile),
            messenger: messenger.clone(),
            policy: Arc::new(policy),
            registry: registry.clone(),
            guard,
            session_id,
            account: "dev".to_string(),
            ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
            max_body_bytes: TEST_BODY_BYTES,
            heartbeat_secs,
            store_incarnation: messenger.store_incarnation(),
            ident: "brenn-server-test".to_string(),
            alert_dispatcher: dispatcher.clone(),
            push_rx,
            active_channels,
            drain_notify: drain_notify.clone(),
            socket: TestSocket {
                inbound: inbound_rx,
                outbound: outbound_tx,
            },
        }));

        Self {
            inbound,
            outbound,
            join,
            registry,
            dispatcher,
            captured,
            push_tx,
            drain_notify,
            messenger,
        }
    }

    fn send(&self, message: Message) {
        self.inbound.send(Ok(message)).expect("session is reading");
    }

    fn send_frame(&self, frame: &ClientFrame) {
        self.send(Message::Text(
            serde_json::to_string(frame)
                .expect("client frame serializes")
                .into(),
        ));
    }

    fn send_hello(&self, versions: VersionRange) {
        self.send_frame(&ClientFrame::Hello {
            versions,
            ident: "test-attacher".to_string(),
        });
    }

    /// The next application frame the session wrote, waiting for it.
    ///
    /// Bounded, because the thing a wiring test proves is that a frame arrives
    /// at all: an unwired wake source would otherwise hang the suite instead of
    /// failing it.
    async fn next_frame(&mut self) -> ServerFrame {
        let next = async {
            loop {
                let message = self.outbound.recv().await.expect("the writer is alive");
                if let Message::Text(text) = message {
                    return serde_json::from_str(text.as_str()).expect("server frame parses");
                }
            }
        };
        tokio::time::timeout(Duration::from_secs(10), next)
            .await
            .expect("the session wrote a frame")
    }

    /// Close the socket, wait for the session to finish, and collect everything
    /// it wrote and alerted.
    async fn finish(self) -> (AttachSessionOutcome, Vec<ServerFrame>) {
        self.send(Message::Close(None));
        self.join_session().await
    }

    /// Wait for a session that ends on its own, and collect what it wrote and
    /// alerted. Sending a close first would race the teardown.
    async fn join_session(mut self) -> (AttachSessionOutcome, Vec<ServerFrame>) {
        let outcome = self.join.await.expect("session task");
        self.dispatcher.flush().await;
        let mut frames = Vec::new();
        while let Ok(message) = self.outbound.try_recv() {
            if let Message::Text(text) = message {
                frames.push(serde_json::from_str(text.as_str()).expect("server frame parses"));
            }
        }
        (outcome, frames)
    }
}

/// The opening sequence in order: the server's `Hello` goes out without waiting,
/// and `Welcome` follows the agreement, stating the contract of this attachment.
#[tokio::test]
async fn an_attachment_opens_with_hello_then_welcome() {
    let attachment = Attachment::start();
    attachment.send_hello(SUPPORTED_VERSIONS);
    let (outcome, frames) = attachment.finish().await;

    assert!(!outcome.violation);
    assert!(outcome.last_detach, "no sibling attachment remained");
    match &frames[0] {
        ServerFrame::Hello { versions, ident } => {
            assert_eq!(*versions, SUPPORTED_VERSIONS);
            assert_eq!(ident, "brenn-server-test");
        }
        other => panic!("expected Hello first, got {other:?}"),
    }
    match &frames[1] {
        ServerFrame::Welcome {
            version,
            participant_id,
            max_body_bytes,
            max_frame_bytes,
            alert_granted,
            ..
        } => {
            assert_eq!(*version, SUPPORTED_VERSIONS.max);
            assert_eq!(participant_id, &format!("surface:{ATTACHER}"));
            assert_eq!(*max_body_bytes, TEST_BODY_BYTES as u64);
            assert_eq!(
                *max_frame_bytes,
                max_client_frame_bytes(TEST_BODY_BYTES) as u64
            );
            assert!(!alert_granted);
        }
        other => panic!("expected Welcome second, got {other:?}"),
    }
    assert_eq!(frames.len(), 2, "no deferred views to seed: {frames:?}");
}

/// An attacher that states a range this build does not speak is conforming — it
/// said what it speaks. The attachment closes after the server's `Hello`, with no
/// `Welcome` and no security event.
#[tokio::test]
async fn an_incompatible_peer_closes_without_a_security_event() {
    let attachment = Attachment::start();
    let captured = attachment.captured.clone();
    attachment.send_hello(VersionRange {
        min: SUPPORTED_VERSIONS.max + 1,
        max: SUPPORTED_VERSIONS.max + 4,
    });
    let (outcome, frames) = attachment.finish().await;

    assert!(!outcome.violation);
    assert!(matches!(frames.as_slice(), [ServerFrame::Hello { .. }]));
    assert!(
        captured.lock().expect("captured").is_empty(),
        "an incompatible peer is not a security event"
    );
}

/// **A client that opens a socket and probes without handshaking is fail2ban
/// signal.** The incompatible peer above is conforming and gets no security
/// event; this one is not, and the arm that tells them apart is the whole reason
/// the plane reports "opened a socket and never attached" at all.
#[tokio::test]
async fn a_first_frame_that_is_not_hello_raises_a_security_event() {
    let attachment = Attachment::start();
    let captured = attachment.captured.clone();
    attachment.send_frame(&ClientFrame::Unsubscribe {
        channel: "brenn:nonesuch".to_string(),
    });
    let (outcome, frames) = attachment.finish().await;

    assert!(outcome.violation);
    let alerts = captured.lock().expect("captured");
    assert_eq!(alerts.len(), 1);
    assert!(alerts[0].2.contains(&format!(
        "attacher surface:{ATTACHER} account dev: first client frame is not Hello"
    )));
    assert!(
        matches!(frames.as_slice(), [ServerFrame::Hello { .. }]),
        "the server's Hello and nothing else — no Welcome: {frames:?}"
    );
}

/// A violation ends the attachment and raises exactly one security event, whose
/// detail names the attacher, the account, and the rule — and none of the
/// client's bytes.
#[tokio::test]
async fn a_violation_ends_the_attachment_and_raises_one_security_event() {
    let attachment = Attachment::start();
    let captured = attachment.captured.clone();
    attachment.send_hello(SUPPORTED_VERSIONS);
    attachment.send(Message::Text("{\"type\":\"Nonesuch\"}".into()));
    let (outcome, _frames) = attachment.finish().await;

    assert!(outcome.violation);
    let alerts = captured.lock().expect("captured");
    assert_eq!(alerts.len(), 1);
    let (_severity, title, body) = &alerts[0];
    assert!(title.contains("attach_protocol_violation"), "{title}");
    assert!(body.contains(&format!(
        "attacher surface:{ATTACHER} account dev: unparseable client frame"
    )));
    assert!(!body.contains("Nonesuch"), "payload echoed: {body}");
}

/// **Attribution over a real socket, both verdicts.**
///
/// The unit suites judge `handle_alert`'s arguments; this one carries the
/// attribution through serialization, the frame reader, and the dispatcher, which
/// is the only place a field that never reached the wire would show — an alert
/// arriving unattributed reads as the platform's own and dispatches under the
/// bare attacher.
#[tokio::test]
async fn an_attributed_alert_crosses_the_socket_and_is_judged_on_the_instance_it_names() {
    let attachment = Attachment::start_with(
        alert_profile(true),
        brenn_messaging::testutils::empty_directory_messenger("test-origin"),
        AppPolicy::default(),
        IDLE_HEARTBEAT_SECS,
    );
    let captured = attachment.captured.clone();
    attachment.send_hello(SUPPORTED_VERSIONS);
    attachment.send_frame(&ClientFrame::Alert {
        attribution: Some("pager".to_string()),
        severity: brenn_attach_proto::AlertSeverity::Warning,
        title: "camera unreachable".to_string(),
        body: "no frames for 10m".to_string(),
    });
    let (outcome, _frames) = attachment.finish().await;

    assert!(!outcome.violation, "a granted sub-identity may page");
    {
        let alerts = captured.lock().expect("captured");
        assert_eq!(alerts.len(), 1, "one alert, got {alerts:?}");
        assert_eq!(
            &alerts[0].1,
            &format!("Attacher surface:{ATTACHER}#pager: camera unreachable"),
            "the dispatched title names the minted sub-principal"
        );
    }

    // The other verdict over the same path: declared, ungranted, and a kill.
    let attachment = Attachment::start_with(
        alert_profile(true),
        brenn_messaging::testutils::empty_directory_messenger("test-origin"),
        AppPolicy::default(),
        IDLE_HEARTBEAT_SECS,
    );
    let captured = attachment.captured.clone();
    attachment.send_hello(SUPPORTED_VERSIONS);
    attachment.send_frame(&ClientFrame::Alert {
        attribution: Some("mute".to_string()),
        severity: brenn_attach_proto::AlertSeverity::Warning,
        title: "paging anyway".to_string(),
        body: String::new(),
    });
    let (outcome, _frames) = attachment.join_session().await;

    assert!(
        outcome.violation,
        "the kernel gates first, so a frame that arrives anyway means it was bypassed"
    );
    let alerts = captured.lock().expect("captured");
    assert_eq!(alerts.len(), 1, "the violation, and no dispatched alert");
    assert!(
        alerts[0].1.contains("attach_protocol_violation"),
        "got {:?}",
        alerts[0].1
    );
    assert!(
        alerts[0].2.contains("without the alert grant"),
        "got {:?}",
        alerts[0].2
    );
}

/// The protocol is JSON text in both directions, so a binary frame is not a frame
/// this attachment could have meant.
#[tokio::test]
async fn a_binary_frame_is_a_violation() {
    let attachment = Attachment::start();
    let captured = attachment.captured.clone();
    attachment.send_hello(SUPPORTED_VERSIONS);
    attachment.send(Message::Binary(Vec::new().into()));
    let (outcome, _frames) = attachment.finish().await;

    assert!(outcome.violation);
    let alerts = captured.lock().expect("captured");
    assert_eq!(alerts.len(), 1);
    assert!(alerts[0].2.contains(&format!(
        "attacher surface:{ATTACHER} account dev: binary frame"
    )));
}

/// `last_detach` is the attacher's question, not the connection's: a sibling
/// attachment still registered means this was not the last one, so a route's
/// terminal action does not run while the attacher is still attached.
#[tokio::test]
async fn a_surviving_sibling_attachment_denies_the_last_detach() {
    let attachment = Attachment::start();
    let _sibling = attachment
        .registry
        .try_register(
            ATTACHER,
            AttachSessionHandle::for_test("other"),
            SessionCaps::UNCAPPED,
        )
        .expect("uncapped registration");
    attachment.send_hello(SUPPORTED_VERSIONS);
    let (outcome, _frames) = attachment.finish().await;

    assert!(!outcome.last_detach);
    assert!(!outcome.violation);
}

// ---------------------------------------------------------------------------
// The loop's other three wake sources
// ---------------------------------------------------------------------------

/// The one channel the loop fixtures subscribe, scheme-qualified and bare.
const CHANNEL: &str = "brenn:loop-demo";
const CHANNEL_BARE: &str = "loop-demo";

/// An attachment that may subscribe [`CHANNEL`], over a real bus with that one
/// channel on it — the shape the push and drain arms need to have anything to
/// deliver. Returns the attachment and the channel uuid rows are seeded against.
async fn subscribing_attachment(db: &Db, heartbeat_secs: u32) -> (Attachment, Uuid) {
    let (messenger, channel_uuid) =
        crate::test_support::one_channel_messenger(db, CHANNEL_BARE).await;
    let mut policy = AppPolicy::default();
    policy.grants.insert(AppCapability::MessagingSubscribe);
    policy.acls.brenn_subscribe = vec![ChannelMatcher::Exact(CHANNEL_BARE.to_string())];
    let profile = TestProfile {
        subscribable: HashMap::from([(
            CHANNEL.to_string(),
            SubscriptionFacts {
                push_depth: 8,
                retain_depth: 8,
            },
        )]),
        ..TestProfile::new()
    };
    let attachment = Attachment::start_with(profile, messenger, policy, heartbeat_secs);
    (attachment, channel_uuid)
}

/// Drive an attachment to the point where it holds an open subscription on
/// [`CHANNEL`], consuming the frames the opening sequence wrote.
async fn attached_and_subscribed(attachment: &mut Attachment) {
    attachment.send_hello(SUPPORTED_VERSIONS);
    attachment.send_frame(&ClientFrame::Subscribe {
        channel: CHANNEL.to_string(),
        push_depth: 8,
        retain_depth: 8,
        resume: None,
    });
    // Consume Hello and Welcome; the assertion takes the SubscribeResult.
    for _ in 0..2 {
        attachment.next_frame().await;
    }
    assert!(matches!(
        attachment.next_frame().await,
        ServerFrame::SubscribeResult { .. }
    ));
}

/// Insert one message on `channel_uuid`.
async fn seed(db: &Db, channel_uuid: Uuid, body: &str, ts_ns: i64) {
    let conn = db.lock().await;
    brenn_messaging_store::db::insert_message(
        &conn,
        channel_uuid,
        "test",
        "sender",
        body,
        brenn_lib::messaging::Urgency::Normal,
        brenn_lib::messaging::ChannelScheme::Brenn,
        None,
        None,
        None,
        None,
        ts_ns,
    );
}

/// A live push of the channel's retained row at `seq`.
async fn live_push(attachment: &Attachment, seq: u64) -> SessionPush {
    let window = attachment
        .messenger
        .store_for_address(CHANNEL)
        .replay_from(None, brenn_lib::messaging::config::Depth::Bounded(64))
        .await
        .messages;
    let row = window
        .iter()
        .find(|row| row.seq == seq)
        .unwrap_or_else(|| panic!("retained window holds no row at seq {seq}"));
    SessionPush::Live(crate::registry::LiveDelivery {
        envelope: row.message.clone(),
        retained_seq: row.seq,
    })
}

/// **Co-available pushes are taken in one turn, so rows precede views.** The
/// router queues a message's sibling rows back to back; the loop drains every
/// push it can before writing, and the composed batch writes rows first. Queued
/// view-then-row, an uncoalesced loop would write them in arrival order — which
/// is what this ordering assertion rules out.
#[tokio::test]
async fn co_available_pushes_are_coalesced_into_one_batch() {
    let db = brenn_messaging_store::db::init_db_memory();
    let (mut attachment, channel_uuid) = subscribing_attachment(&db, IDLE_HEARTBEAT_SECS).await;
    attached_and_subscribed(&mut attachment).await;

    seed(&db, channel_uuid, r#"{"n":1}"#, 100).await;
    let row = live_push(&attachment, 1).await;
    let view = SessionPush::DeferredView(crate::registry::DeferredViewPush {
        channel: CHANNEL.to_string(),
        attribution: None,
        entries: Vec::new(),
    });
    // Both without an await between them, so the session task cannot wake and
    // service the first before the second is queued.
    attachment.push_tx.try_send(view).expect("queue has room");
    attachment.push_tx.try_send(row).expect("queue has room");

    assert!(
        matches!(attachment.next_frame().await, ServerFrame::Deliver { .. }),
        "the row goes out first, from the batch the loop composed"
    );
    assert!(matches!(
        attachment.next_frame().await,
        ServerFrame::DeferredView { .. }
    ));

    let (outcome, _frames) = attachment.finish().await;
    assert!(!outcome.violation);
}

/// **The eager-wake nudge serves every active subscription its suffix, as one
/// frame.** The router fires it for rows it did not hand over itself — a quiet
/// channel's, or a released schedule's — so without this arm wired those rows
/// wait for the next live message that may never come. The nudge's drain is one
/// pass, so the whole suffix reaches the attacher as one delivery point.
#[tokio::test]
async fn a_drain_nudge_serves_the_retained_suffix() {
    let db = brenn_messaging_store::db::init_db_memory();
    let (mut attachment, channel_uuid) = subscribing_attachment(&db, IDLE_HEARTBEAT_SECS).await;
    attached_and_subscribed(&mut attachment).await;

    seed(&db, channel_uuid, r#"{"n":1}"#, 100).await;
    seed(&db, channel_uuid, r#"{"n":2}"#, 200).await;
    attachment.drain_notify.notify_one();

    match attachment.next_frame().await {
        ServerFrame::Deliver { channel, rows } => {
            assert_eq!(channel, CHANNEL);
            assert_eq!(
                rows.iter().map(|row| row.seq).collect::<Vec<_>>(),
                vec![1, 2],
                "the span's seq advances per row within the one pass"
            );
        }
        other => panic!("expected a Deliver, got {other:?}"),
    }

    let (outcome, _frames) = attachment.finish().await;
    assert!(!outcome.violation);
}

/// **An attachment that answers nothing for three probes is reaped.** Silence is
/// the only liveness signal the server has — a half-open socket still claims to
/// be a socket — and a zombie attachment pins a registry slot against the
/// profile's caps until the TCP stack notices. The pong in the middle is what
/// proves the reap keys off inbound traffic rather than firing on a schedule.
#[tokio::test(start_paused = true)]
async fn a_silent_attachment_is_reaped_and_a_talking_one_is_not() {
    let db = brenn_messaging_store::db::init_db_memory();
    let (attachment, _uuid) = subscribing_attachment(&db, 1).await;
    attachment.send_hello(SUPPORTED_VERSIONS);

    // Two heartbeats of silence is inside the 3x window.
    tokio::time::sleep(Duration::from_millis(2500)).await;
    assert!(
        !attachment.join.is_finished(),
        "reaped inside the 3x window"
    );

    // A pong resets the clock, so another 2.5s — past the 4s mark an unreset
    // reaper would have fired at — still leaves the attachment alive.
    attachment.send(Message::Pong(Vec::new().into()));
    tokio::time::sleep(Duration::from_millis(2500)).await;
    assert!(
        !attachment.join.is_finished(),
        "inbound liveness did not refresh the reaper"
    );

    // Now go quiet. The bound is generous against the 3s window and costs
    // nothing under a paused clock; without it a dead reaper would hang.
    let (outcome, _frames) =
        tokio::time::timeout(Duration::from_secs(60), attachment.join_session())
            .await
            .expect("a silent attachment is reaped");
    assert!(
        !outcome.violation,
        "a reaped attachment is gone, not misbehaving"
    );
    assert!(outcome.last_detach);
}
