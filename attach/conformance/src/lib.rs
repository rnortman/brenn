//! A whole attacher that is not a browser.
//!
//! [`AttachClient`] embeds `brenn-attach-client` the way a native daemon would:
//! it opens a native websocket, negotiates a version, subscribes channels,
//! publishes onto them, reads what comes back, and survives a severed transport
//! by resuming each subscription from the cursor it held. It knows nothing of
//! components, ports, mounts, or pixels — it has no surface crate to know them
//! from — so whatever it can drive is, by construction, drivable by an attacher
//! with no page behind it.
//!
//! That makes it two things at once: a second embedder shaping the client
//! crate's API against a non-browser caller, and the executable form of the
//! claim that the attachment protocol carries no browser assumptions.
//!
//! **It is a test instrument, not a library to build daemons on.** The
//! ergonomics are built for assertions: every server frame it absorbs becomes an
//! [`Observation`] in an ordered queue, and the awaited helpers pump the driver
//! until the observation they want appears, leaving the rest of the queue in
//! order behind them. A real daemon would keep the queue and hand the events to
//! its own logic instead.
//!
//! [`relay`] is the fault injection the reconnect run needs: a severable TCP
//! relay to put in front of the peer, so a test can cut a live transport at the
//! socket rather than asking either end to pretend.

pub mod relay;

use std::collections::VecDeque;
use std::time::Duration;

use brenn_attach_client::conn::{ConnConfig, ConnEvent};
use brenn_attach_client::driver::{AttachDriver, DriverStep, IoEvent};
use brenn_attach_client::publish::PendingPublishes;
use brenn_attach_client::subs::{DeliverDisposition, Subscriptions};
use brenn_attach_client::transport::native::NativeConnector;
use brenn_attach_proto::{
    ClientFrame, DeferredViewEntry, PublishBatchOutcome, PublishOutcome, ServerFrame, VersionRange,
};

// The vocabulary this client's own API is stated in, re-exported so a caller
// names it through the crate it is calling rather than reaching past it.
pub use brenn_attach_client::conn::{AttachmentFacts, DetachReason};
pub use brenn_attach_client::publish::PublishRequest;
pub use brenn_attach_client::subs::{ResumePolicy, SubscribeAck, SubscriptionDepths};

/// How long any awaited helper will pump before it gives up and panics.
///
/// A generous bound whose only job is turning a hang into a failure with a
/// message: every wait here is on a local peer that answers in milliseconds, so
/// reaching this means something is wedged, not slow.
const WAIT_TIMEOUT: Duration = Duration::from_secs(10);

/// One message this attacher was delivered, flattened to what an assertion
/// reads.
///
/// `sender` is the peer's stamp, not the publisher's claim, which is why it is
/// worth asserting on at all. The rest of the envelope is dropped here: a
/// conformance run asserts on what the protocol decided, not on what a message
/// carried.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delivery {
    pub channel: String,
    /// The envelope sender the peer minted — bare for the attacher itself, or
    /// carrying the sub-identity an `attribution` named.
    pub sender: String,
    pub body: String,
    /// The delivery's span sequence, restarting at 1 with each subscription
    /// span.
    pub seq: u64,
    /// Messages this attachment lost on the channel before this one.
    pub dropped: u64,
}

/// Why an attachment ended for good.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TerminalCause {
    /// A server frame could not be reconciled with the protocol.
    Fatal { detail: String },
    /// The two ends speak no version in common.
    Incompatible {
        ours: VersionRange,
        theirs: VersionRange,
    },
    /// The peer closed with the code this client declared terminal.
    PeerClosed { code: u16, reason: String },
}

/// Something the attachment did that a test may assert on, in the order it
/// happened.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Observation {
    /// Negotiation completed and `Welcome` was accepted. Every subscription that
    /// survived the previous attachment has already been re-sent by the time
    /// this is observed.
    Attached(AttachmentFacts),
    /// The transport went away; a reconnect is on the backoff schedule.
    Detached { reason: DetachReason },
    /// The attachment is over and will not reconnect.
    Terminal(TerminalCause),
    /// A subscription was acknowledged.
    Subscribed { channel: String, ack: SubscribeAck },
    /// An envelope arrived and the subscription plane accepted it.
    Delivered(Delivery),
    /// A single publish was answered.
    Published {
        correlation: u64,
        outcome: PublishOutcome,
    },
    /// A batch flush was answered. This client sends no batches itself; the
    /// variant exists so an unexpected one is a visible observation rather than
    /// a silent drop.
    BatchPublished {
        correlation: u64,
        outcome: PublishBatchOutcome,
    },
    /// The peer's snapshot of what one sub-identity has parked on one channel.
    Deferred {
        channel: String,
        attribution: Option<String>,
        entries: Vec<DeferredViewEntry>,
    },
    /// A publish that will never be answered: the transport went away while it
    /// was outstanding.
    PublishLost { correlation: u64 },
}

/// What an [`AttachClient`] needs to reach its peer.
pub struct ClientConfig {
    /// The fully-formed websocket URL, query included. Composing whatever a
    /// route demands ahead of the socket is the attacher's business, exactly as
    /// it is the browser kernel's.
    pub url: String,
    /// The raw session-cookie token the native connector authenticates with.
    pub session_cookie: String,
    /// The build string put on this end's `Hello`, for the peer's logs.
    pub ident: String,
}

/// A non-browser attachment, driven by awaited assertions.
pub struct AttachClient {
    driver: AttachDriver<NativeConnector>,
    subs: Subscriptions,
    /// Tagged with the correlation itself: this client answers its caller from
    /// the observation queue rather than from a routing table, so the tag exists
    /// only to name what was lost when a transport dies mid-publish.
    pending: PendingPublishes<u64>,
    next_correlation: u64,
    observed: VecDeque<Observation>,
    started: bool,
}

impl AttachClient {
    /// Build the client. Nothing connects until [`AttachClient::attach`].
    ///
    /// The timings are test timings: a short backoff so a reconnect run finishes
    /// promptly, and a liveness multiplier of 3, which is the browser kernel's
    /// too — the rule is the protocol's, not the page's.
    pub fn new(config: ClientConfig) -> Self {
        let ClientConfig {
            url,
            session_cookie,
            ident,
        } = config;
        let driver = AttachDriver::new(
            ConnConfig {
                url,
                ident,
                initial_backoff: Duration::from_millis(20),
                max_backoff: Duration::from_millis(200),
                connect_timeout: Duration::from_secs(5),
                liveness_multiplier: 3,
                // Fixed: this is load-spreading entropy across a fleet, and there
                // is one of these.
                backoff_jitter_seed: 0x5eed_c0de,
                // No close code means anything special to a conformance run:
                // every peer close is an ordinary drop to reconnect from.
                terminal_close_code: None,
            },
            NativeConnector::new(session_cookie),
        );
        Self {
            driver,
            subs: Subscriptions::new(),
            pending: PendingPublishes::new(),
            next_correlation: 1,
            observed: VecDeque::new(),
            started: false,
        }
    }

    /// Connect, negotiate, and return the transport facts the peer stated.
    ///
    /// Also the way to await a *re*attachment: the driver reconnects on its own
    /// schedule after a severed transport, so a caller that has observed a
    /// [`Observation::Detached`] calls this again to wait out the backoff.
    pub async fn attach(&mut self) -> AttachmentFacts {
        match self
            .take_first("an attachment", true, |o| {
                matches!(o, Observation::Attached(_))
            })
            .await
        {
            Observation::Attached(facts) => facts,
            other => unreachable!("the predicate admits only Attached, got {other:?}"),
        }
    }

    /// Subscribe `channel` and return the peer's acknowledgement.
    ///
    /// The subscription is held until [`AttachClient::unsubscribe`], which is
    /// what makes it survive a severed transport: a surviving subscription is
    /// re-sent at the next attachment carrying the cursor of the last delivery
    /// accepted on it.
    pub async fn subscribe(
        &mut self,
        channel: &str,
        depths: SubscriptionDepths,
        resume_policy: ResumePolicy,
    ) -> SubscribeAck {
        let frames = self.subs.acquire(channel, depths, resume_policy);
        self.write(frames).await;
        self.next_subscribe_ack(channel).await
    }

    /// The next acknowledgement for `channel`, pumping until one arrives.
    ///
    /// What a caller awaits after a reattachment: the subscriptions that
    /// survived were re-sent before [`AttachClient::attach`] returned, and this
    /// is the answer to one of them.
    pub async fn next_subscribe_ack(&mut self, channel: &str) -> SubscribeAck {
        let channel = channel.to_string();
        match self
            .take_first(
                "a subscribe answer",
                true,
                |o| matches!(o, Observation::Subscribed { channel: c, .. } if *c == channel),
            )
            .await
        {
            Observation::Subscribed { ack, .. } => ack,
            other => unreachable!("the predicate admits only Subscribed, got {other:?}"),
        }
    }

    /// Drop this client's reference on `channel`, closing the subscription.
    pub async fn unsubscribe(&mut self, channel: &str) {
        let frames = self.subs.release(channel);
        self.write(frames).await;
    }

    /// Publish one message and return the outcome the peer answered with.
    pub async fn publish(&mut self, request: PublishRequest) -> PublishOutcome {
        let correlation = self.next_correlation;
        self.next_correlation += 1;
        let frame = self.pending.send(correlation, correlation, request);
        self.write(vec![frame]).await;
        match self
            .take_first(
                "a publish answer",
                true,
                |o| matches!(o, Observation::Published { correlation: c, .. } if *c == correlation),
            )
            .await
        {
            Observation::Published { outcome, .. } => outcome,
            other => unreachable!("the predicate admits only Published, got {other:?}"),
        }
    }

    /// The next delivery on `channel`, pumping until one arrives.
    pub async fn next_delivery(&mut self, channel: &str) -> Delivery {
        let channel = channel.to_string();
        match self
            .take_first(
                "a delivery",
                true,
                |o| matches!(o, Observation::Delivered(d) if d.channel == channel),
            )
            .await
        {
            Observation::Delivered(delivery) => delivery,
            other => unreachable!("the predicate admits only Delivered, got {other:?}"),
        }
    }

    /// The next observation of any kind, pumping until there is one.
    ///
    /// The raw entry point, and the only one that will hand back a
    /// [`Observation::Terminal`]: the typed helpers panic on a terminal
    /// attachment, because nothing they promise can arrive after one.
    pub async fn next_observation(&mut self) -> Observation {
        self.take_first("an observation", false, |_| true).await
    }

    /// Whether a live attachment is up right now.
    pub fn is_attached(&self) -> bool {
        self.driver.is_active()
    }

    /// Whether this channel has a live wire subscription right now.
    pub fn is_subscribed(&self, channel: &str) -> bool {
        self.subs.is_active(channel)
    }

    /// End the attachment at this client's own request. Terminal.
    pub async fn close(&mut self) {
        let step = self.driver.close().await;
        self.absorb(step).await;
    }

    // -----------------------------------------------------------------------
    // The pump
    // -----------------------------------------------------------------------

    /// Take the first queued observation the predicate admits, pumping the
    /// driver until one appears.
    ///
    /// Removal by index rather than a pop-and-hold: the observations the
    /// predicate rejected keep their order and their place, so a caller that
    /// awaits an answer first and reads the deliveries afterwards still sees
    /// them in the order the peer sent them.
    async fn take_first(
        &mut self,
        what: &str,
        terminal_is_fatal: bool,
        admits: impl Fn(&Observation) -> bool,
    ) -> Observation {
        loop {
            if let Some(index) = self.observed.iter().position(&admits) {
                return self
                    .observed
                    .remove(index)
                    .expect("an index just found in the queue");
            }
            if terminal_is_fatal
                && let Some(Observation::Terminal(cause)) = self
                    .observed
                    .iter()
                    .find(|o| matches!(o, Observation::Terminal(_)))
            {
                panic!(
                    "attach conformance: waiting for {what}, but the attachment ended: {cause:?}"
                );
            }
            assert!(
                !self.driver.is_terminal(),
                "attach conformance: waiting for {what} on an attachment that is over"
            );
            match tokio::time::timeout(WAIT_TIMEOUT, self.step()).await {
                Ok(()) => {}
                Err(_) => panic!("attach conformance: timed out waiting for {what}"),
            }
        }
    }

    /// One turn of the embedder's loop: start it, run a connect it asked for, or
    /// wait for the next thing that concerns it.
    ///
    /// A connect is checked before the wait for the reason the driver's doc
    /// gives: it is a long await of its own, and this loop has no other arms to
    /// race it against.
    async fn step(&mut self) {
        if !self.started {
            self.started = true;
            let step = self.driver.start().await;
            self.absorb(step).await;
            return;
        }
        if let Some(url) = self.driver.take_pending_connect() {
            let input = self.driver.connect(&url).await;
            let step = self.driver.on_input(input).await;
            self.absorb(step).await;
            return;
        }
        match self.driver.wait().await {
            IoEvent::Conn(input) => {
                let step = self.driver.on_input(input).await;
                self.absorb(step).await;
            }
            // Neither deadline is ever armed: this client registers no outbox
            // and hosts no confined channel, so nothing here can arm one.
            other => panic!("attach conformance: an unarmed deadline fired: {other:?}"),
        }
    }

    /// Turn one driver step into observations, executing whatever the planes
    /// answer with.
    ///
    /// Iterative rather than recursive: writing frames produces a step of its
    /// own (a failed write is a lost transport), and that step joins the same
    /// queue instead of nesting an async call inside itself.
    async fn absorb(&mut self, step: DriverStep) {
        let mut queue = VecDeque::from([step]);
        while let Some(step) = queue.pop_front() {
            for event in step.events {
                if let Some(next) = self.on_event(event).await {
                    queue.push_back(next);
                }
            }
            let Some(frame) = step.routed else { continue };
            match self.route(frame) {
                Ok(frames) => {
                    if let Some(next) = self.emit(frames).await {
                        queue.push_back(next);
                    }
                }
                // The planes above the connection judge the peer against a
                // contract the connection cannot check. A frame that fails one
                // of those checks is a peer that is not keeping it, which is
                // terminal — the same verdict the connection reaches on a frame
                // it cannot parse.
                Err(detail) => queue.push_back(self.driver.host_fatal(detail).await),
            }
        }
    }

    /// Record one connection event, answering with the step any frames it owed
    /// produced.
    async fn on_event(&mut self, event: ConnEvent) -> Option<DriverStep> {
        match event {
            ConnEvent::Attached(facts) => {
                // Before the observation: a caller that awaits the attachment
                // and immediately asserts on a subscription must not be able to
                // observe the gap between the two.
                let frames = self.subs.on_attached();
                self.observed.push_back(Observation::Attached(facts));
                self.emit(frames).await
            }
            ConnEvent::Detached { reason } => {
                self.subs.on_detached();
                for (correlation, _) in self.pending.fail_all() {
                    self.observed
                        .push_back(Observation::PublishLost { correlation });
                }
                self.observed.push_back(Observation::Detached { reason });
                None
            }
            ConnEvent::Fatal { detail } => {
                self.observed
                    .push_back(Observation::Terminal(TerminalCause::Fatal { detail }));
                None
            }
            ConnEvent::Incompatible { ours, theirs } => {
                self.observed
                    .push_back(Observation::Terminal(TerminalCause::Incompatible {
                        ours,
                        theirs,
                    }));
                None
            }
            ConnEvent::PeerClosedTerminal { code, reason } => {
                self.observed
                    .push_back(Observation::Terminal(TerminalCause::PeerClosed {
                        code,
                        reason,
                    }));
                None
            }
        }
    }

    /// Route one server frame into the plane that owns it, answering with the
    /// frames that plane wants sent.
    ///
    /// An `Err` is the peer breaking the protocol, not this client's own bug:
    /// both planes below answer that way for a frame they cannot reconcile.
    fn route(&mut self, frame: ServerFrame) -> Result<Vec<ClientFrame>, String> {
        match frame {
            ServerFrame::SubscribeResult {
                channel,
                outcome,
                replay_count,
                gap,
            } => {
                let ack = self
                    .subs
                    .on_subscribe_result(&channel, outcome, replay_count, gap)?;
                let frames = ack.frames.clone();
                self.observed
                    .push_back(Observation::Subscribed { channel, ack });
                Ok(frames)
            }
            ServerFrame::Deliver { channel, rows } => {
                match self.subs.on_deliver(&channel, &rows)? {
                    // The observation queue is delivery-granular, not
                    // frame-granular: one pass is one delivery point on the wire
                    // and still N messages a client reads, so each row is its own
                    // observation carrying its own wire facts.
                    DeliverDisposition::Accept { .. } => {
                        for row in rows {
                            self.observed.push_back(Observation::Delivered(Delivery {
                                channel: channel.clone(),
                                sender: row.envelope.sender,
                                body: row.envelope.body,
                                seq: row.seq,
                                dropped: row.dropped,
                            }));
                        }
                    }
                    // A pass from a span this client has already left. It
                    // advanced nothing below and is nothing to assert on.
                    DeliverDisposition::Discard { .. } => {}
                }
                Ok(Vec::new())
            }
            ServerFrame::PublishResult {
                correlation,
                outcome,
            } => {
                let correlation = self.pending.on_result(correlation)?;
                self.observed.push_back(Observation::Published {
                    correlation,
                    outcome,
                });
                Ok(Vec::new())
            }
            ServerFrame::PublishBatchResult {
                correlation,
                outcome,
            } => {
                self.observed.push_back(Observation::BatchPublished {
                    correlation,
                    outcome,
                });
                Ok(Vec::new())
            }
            ServerFrame::DeferredView {
                channel,
                attribution,
                entries,
            } => {
                self.observed.push_back(Observation::Deferred {
                    channel,
                    attribution,
                    entries,
                });
                Ok(Vec::new())
            }
            // The connection consumes its own frames and routes nothing else.
            other => Err(format!(
                "a frame the connection should have consumed reached a plane above it: {other:?}"
            )),
        }
    }

    /// Write frames the planes produced, or drop them when there is no wire.
    ///
    /// Dropping off `Active` is correct rather than lenient: the subscription
    /// plane answers with frames whenever its statement changes, and a statement
    /// made while detached is re-sent whole at the next attachment.
    async fn emit(&mut self, frames: Vec<ClientFrame>) -> Option<DriverStep> {
        if frames.is_empty() || !self.driver.is_active() {
            return None;
        }
        Some(self.driver.send(frames).await)
    }

    /// [`emit`](AttachClient::emit) for a caller-initiated write, absorbing what
    /// it produced.
    async fn write(&mut self, frames: Vec<ClientFrame>) {
        if let Some(step) = self.emit(frames).await {
            self.absorb(step).await;
        }
    }
}
