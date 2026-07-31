//! The connection lifecycle of an attachment: open a socket, negotiate a
//! transport version, hold the attachment live against an inbound-silence rule,
//! and back off to reconnect when it drops.
//!
//! Sans-I/O. [`Connection`] is a pure state machine: it takes [`ConnInput`]s the
//! driver produces from the transport and its timer, and answers with
//! [`ConnEffect`]s the driver executes in order. It reads no clock — every input
//! carries the driver's [`Millis`] reading — and no entropy beyond the backoff
//! seed it is constructed with.
//!
//! The layer owns exactly the frames that are *about* the connection —
//! [`ServerFrame::Hello`], [`ServerFrame::Welcome`], [`ServerFrame::Heartbeat`] —
//! and hands every other server frame back to the embedder on
//! [`ConnStep::routed`]. The subscription, publish, and application planes are
//! not its business; what they all share is that an inbound frame of any kind is
//! evidence the peer is alive, so liveness re-arms here for all of them.

use std::time::Duration;

use brenn_attach_proto::{ClientFrame, SUPPORTED_VERSIONS, ServerFrame, VersionRange, negotiate};

use crate::Millis;

/// Why a live attachment dropped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DetachReason {
    /// No inbound frame arrived within `liveness_multiplier × heartbeat_secs`;
    /// the peer is treated as gone.
    LivenessTimeout,
    /// The transport went away — a peer close or a transport-level failure —
    /// while negotiating or live.
    ///
    /// Carries what the loss said about itself so an embedder can log or alert on
    /// it at its own policy level: a peer closing 1011 with a diagnostic reason is
    /// how a server tells an attacher it did something wrong, and a reconnect loop
    /// with no such text is indistinguishable from a network outage. `code` is
    /// absent for a transport-level failure, which has none. `reason` is opaque
    /// peer- or stack-supplied text: render it as text, never interpolate it into
    /// markup or a URL.
    TransportClosed { code: Option<u16>, reason: String },
}

/// The transport contract of one attachment, as stated by
/// [`ServerFrame::Welcome`].
///
/// Everything the peer tells the attacher about *this connection* and nothing
/// else: application configuration is state, and state on this bus is a retained
/// channel the embedder subscribes to like anything else.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttachmentFacts {
    pub version: u32,
    pub participant_id: String,
    pub session_id: String,
    pub heartbeat_secs: u32,
    pub max_body_bytes: u64,
    pub max_frame_bytes: u64,
    pub alert_granted: bool,
}

/// Something the embedder must know about the connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnEvent {
    /// Negotiation completed and `Welcome` was accepted: the attachment is live.
    /// The embedder resubscribes its channels and flushes whatever it parked.
    Attached(AttachmentFacts),
    /// A live (or negotiating) attachment went away. Reconnection proceeds on
    /// the backoff schedule; the embedder tears down its per-connection state.
    Detached { reason: DetachReason },
    /// A protocol error: a server frame could not be reconciled with the
    /// contract. Terminal — no reconnect.
    Fatal { detail: String },
    /// The two ends' version ranges do not overlap. Terminal, and deliberately
    /// *not* a backoff: both ranges are build constants, so retrying against the
    /// same peer build can only reach the same verdict. An embedder that wants
    /// to try a redeployed peer builds a fresh [`Connection`].
    ///
    /// Not a protocol error either — the peer conformed by stating what it
    /// speaks, which is exactly what the handshake is for.
    Incompatible {
        ours: VersionRange,
        theirs: VersionRange,
    },
    /// The peer closed with the code the embedder declared terminal
    /// ([`ConnConfig::terminal_close_code`]). Terminal — no reconnect; what the
    /// code means is the embedder's business. `reason` is opaque peer-supplied
    /// text, bounded only by the websocket close-reason limit: render it as text,
    /// never interpolate it into markup or a URL.
    PeerClosedTerminal { code: u16, reason: String },
}

/// An effect the driver must execute, in order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnEffect {
    /// Open a transport connection to this fully-formed URL. Nothing from any
    /// earlier transport may be fed to the connection once this one is opened
    /// (see [`ConnInput`]).
    Connect { url: String },
    /// Close the current transport, best-effort. While connecting this cancels a
    /// still-pending attempt. Its events stop here: the driver feeds none of them
    /// afterwards (see [`ConnInput`]).
    CloseTransport,
    /// Arm the connection timer to fire at this deadline, or disarm it (`None`).
    SetWakeup(Option<Millis>),
    /// Send a client frame; the driver serializes and writes it.
    SendFrame(ClientFrame),
    /// Hand an event to the embedder.
    Emit(ConnEvent),
}

/// An input to the connection, produced by the driver from transport and timer
/// events.
///
/// **Driver obligation: one transport at a time.** Every transport-sourced input
/// must come from the transport the connection currently owns, and once that
/// transport is done with — [`ConnEffect::CloseTransport`] was executed, or its
/// own [`Disconnected`](ConnInput::Disconnected) was fed — nothing further from
/// it may be fed at all. The machine keys on state alone and cannot tell two
/// transports apart: it absorbs a straggler only in a state that owns no live
/// transport of its own. An event leaked from a previous transport into a fresh
/// attachment is processed as if the live peer had sent it — re-arming liveness
/// off a dead socket, or walking a subscription onto a `seq` and cursor from
/// another connection's numbering.
///
/// Given that obligation, the connection never panics on peer input: whatever it
/// cannot own in a state is a post-close straggler and is absorbed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnInput {
    /// The connect attempt succeeded; the socket is open.
    Opened,
    /// The connect attempt failed before a socket was established.
    ///
    /// Every outcome short of an open socket arrives here, a peer close during
    /// the opening handshake included: the connector resolves the handshake
    /// before the connection is handed any event, so a close carrying
    /// [`ConnConfig::terminal_close_code`] is only distinguishable once the
    /// socket is open. A peer that means the code to be honoured must therefore
    /// close an established socket, which is what a server closing after the
    /// upgrade does.
    ConnectFailed,
    /// An established transport went away — a peer close (carrying its close
    /// `code` and `reason`) or a transport-level failure (`code: None`). Only
    /// ever from a socket that opened, per [`ConnectFailed`](ConnInput::ConnectFailed).
    Disconnected { code: Option<u16>, reason: String },
    /// A text frame (JSON [`ServerFrame`]) arrived.
    TextFrame(String),
    /// A binary frame arrived. The protocol is JSON text, so this is always a
    /// fatal protocol error.
    BinaryFrame,
    /// A precondition the connection cannot check for itself failed host-side.
    /// Terminal, carrying its own diagnosis into the ordinary fatal path.
    HostFatal { detail: String },
    /// The armed connection timer fired.
    Tick,
}

/// What one input produced: effects to execute, and — for a frame belonging to a
/// plane above this one — the parsed frame to route.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnStep {
    pub effects: Vec<ConnEffect>,
    /// A server frame this layer does not own. Never a `Hello`, `Welcome`, or
    /// `Heartbeat`; those are the connection's own and are consumed here.
    pub routed: Option<ServerFrame>,
}

impl ConnStep {
    fn effects(effects: Vec<ConnEffect>) -> Self {
        Self {
            effects,
            routed: None,
        }
    }

    fn none() -> Self {
        Self::effects(Vec::new())
    }
}

/// Where the connection is in its lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnState {
    /// A connect attempt is in flight. Handshake deadline armed.
    Connecting,
    /// The socket is open and this end's `Hello` is sent; awaiting the peer's.
    /// Same handshake deadline.
    Negotiating,
    /// Versions agreed; awaiting `Welcome`. Same handshake deadline.
    AwaitingWelcome,
    /// `Welcome` accepted; the attachment is live. Liveness deadline armed.
    Active,
    /// Waiting out a backoff delay before the next connect attempt.
    Backoff,
    /// Terminal: no reconnect, and every further input is absorbed. Which of the
    /// terminal events got here is the embedder's to remember.
    Terminal,
}

/// Construction parameters for a [`Connection`].
pub struct ConnConfig {
    /// The fully-formed connect URL, query included. The connection appends
    /// nothing: an attacher that needs query parameters — a served-asset build
    /// check, say — composes them into this string, since what belongs there is
    /// route policy and not the protocol's business.
    pub url: String,
    /// A free-form build identifier put on this end's `Hello`. For the peer's
    /// logs only; never parsed by either end.
    pub ident: String,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    /// How long a connect-plus-handshake may take before the attempt is
    /// abandoned and backed off.
    pub connect_timeout: Duration,
    /// Multiple of the peer's advertised `heartbeat_secs` of inbound silence that
    /// marks the attachment dead. Nonzero: zero tolerates no silence at all and
    /// reaps every attachment on its first tick.
    pub liveness_multiplier: u32,
    /// Seed for the backoff-jitter PRNG. Distinct per attacher so a fleet
    /// reconnecting in lockstep after a peer restart decorrelates; a fixed value
    /// in tests keeps the state machine deterministic. Only cross-attacher
    /// distinctness matters — this is load-spreading entropy, never a secret.
    pub backoff_jitter_seed: u64,
    /// A peer close code the embedder declares terminal, if it has one. A close
    /// carrying it stops the reconnect schedule and raises
    /// [`ConnEvent::PeerClosedTerminal`] instead of backing off. Every other
    /// close is an ordinary drop.
    pub terminal_close_code: Option<u16>,
}

/// The sans-I/O connection state machine.
pub struct Connection {
    url: String,
    ident: String,
    initial_backoff_ms: u64,
    max_backoff_ms: u64,
    connect_timeout_ms: u64,
    liveness_multiplier: u32,
    terminal_close_code: Option<u16>,
    /// Inbound-silence window in millis, computed at each `Welcome` from
    /// `heartbeat_secs × liveness_multiplier`. Zero until the first one.
    liveness_ms: u64,
    state: ConnState,
    backoff_step: u32,
    /// The single armed deadline: a handshake deadline while connecting or
    /// negotiating, a backoff deadline in `Backoff`, a liveness deadline in
    /// `Active`.
    deadline: Millis,
    /// The version `negotiate` settled on, once the peer's `Hello` has arrived.
    /// `Welcome` must echo it.
    version: Option<u32>,
    jitter: SplitMix64,
}

impl Connection {
    /// Build a connection and start its first attempt.
    pub fn start(config: ConnConfig, now: Millis) -> (Self, Vec<ConnEffect>) {
        assert!(
            config.liveness_multiplier > 0,
            "attach client: a liveness multiplier of zero tolerates no inbound silence"
        );
        let mut conn = Self {
            url: config.url,
            ident: config.ident,
            initial_backoff_ms: duration_ms(config.initial_backoff),
            max_backoff_ms: duration_ms(config.max_backoff),
            connect_timeout_ms: duration_ms(config.connect_timeout),
            liveness_multiplier: config.liveness_multiplier,
            terminal_close_code: config.terminal_close_code,
            liveness_ms: 0,
            state: ConnState::Backoff,
            backoff_step: 0,
            deadline: now,
            version: None,
            jitter: SplitMix64::new(config.backoff_jitter_seed),
        };
        let effects = conn.begin_connect(now);
        (conn, effects)
    }

    pub fn state(&self) -> ConnState {
        self.state
    }

    /// Whether frames may be sent right now. The planes above ask before
    /// emitting anything: off `Active` there is no wire to carry it.
    pub fn is_active(&self) -> bool {
        self.state == ConnState::Active
    }

    /// The negotiated transport version, once the handshake reached one.
    pub fn version(&self) -> Option<u32> {
        self.version
    }

    /// Feed one input.
    pub fn on_input(&mut self, input: ConnInput, now: Millis) -> ConnStep {
        match (self.state, input) {
            (_, ConnInput::HostFatal { detail }) => ConnStep::effects(self.go_fatal(detail)),
            // Terminal: any in-flight transport or timer event is expected and
            // absorbed, never a bug to panic on.
            (ConnState::Terminal, _) => ConnStep::none(),
            (ConnState::Connecting, ConnInput::Opened) => {
                // This end's `Hello` goes out without waiting for the peer's:
                // the exchange is symmetric and each side computes the same
                // verdict from the two ranges.
                self.state = ConnState::Negotiating;
                ConnStep::effects(vec![ConnEffect::SendFrame(ClientFrame::Hello {
                    versions: SUPPORTED_VERSIONS,
                    ident: self.ident.clone(),
                })])
            }
            (ConnState::Connecting, ConnInput::ConnectFailed) => {
                ConnStep::effects(self.enter_backoff(now))
            }
            (
                ConnState::Negotiating | ConnState::AwaitingWelcome | ConnState::Active,
                ConnInput::Disconnected { code, reason },
            ) => ConnStep::effects(self.on_disconnected(code, reason, now)),
            (ConnState::Negotiating, ConnInput::TextFrame(text)) => self.on_text_negotiating(&text),
            (ConnState::AwaitingWelcome, ConnInput::TextFrame(text)) => {
                self.on_text_awaiting_welcome(&text, now)
            }
            (ConnState::Active, ConnInput::TextFrame(text)) => self.on_text_active(&text, now),
            (
                ConnState::Negotiating | ConnState::AwaitingWelcome | ConnState::Active,
                ConnInput::BinaryFrame,
            ) => ConnStep::effects(self.go_fatal("unexpected binary frame from peer".to_string())),
            (
                ConnState::Connecting | ConnState::Negotiating | ConnState::AwaitingWelcome,
                ConnInput::Tick,
            ) => ConnStep::effects(if now >= self.deadline {
                let mut effects = vec![ConnEffect::CloseTransport];
                effects.extend(self.enter_backoff(now));
                effects
            } else {
                vec![ConnEffect::SetWakeup(Some(self.deadline))]
            }),
            (ConnState::Active, ConnInput::Tick) => ConnStep::effects(if now >= self.deadline {
                let mut effects = vec![
                    ConnEffect::CloseTransport,
                    ConnEffect::Emit(ConnEvent::Detached {
                        reason: DetachReason::LivenessTimeout,
                    }),
                ];
                effects.extend(self.enter_backoff(now));
                effects
            } else {
                vec![ConnEffect::SetWakeup(Some(self.deadline))]
            }),
            (ConnState::Backoff, ConnInput::Tick) => ConnStep::effects(if now >= self.deadline {
                self.begin_connect(now)
            } else {
                vec![ConnEffect::SetWakeup(Some(self.deadline))]
            }),
            // Nothing left here can have come from the transport this state owns:
            // `Opened`/`ConnectFailed` answer a connect attempt already settled,
            // a frame or close while `Connecting` predates the socket being
            // opened, and `Backoff` owns no transport at all. So each is a
            // straggler from one the driver has already stopped feeding —
            // an ordinary async race, not a bug.
            (
                ConnState::Connecting
                | ConnState::Negotiating
                | ConnState::AwaitingWelcome
                | ConnState::Active
                | ConnState::Backoff,
                ConnInput::Opened
                | ConnInput::ConnectFailed
                | ConnInput::Disconnected { .. }
                | ConnInput::TextFrame(_)
                | ConnInput::BinaryFrame,
            ) => ConnStep::none(),
        }
    }

    /// Enter the terminal fatal state. Public because the planes above reach
    /// their own unreconcilable answers — a document that will not validate, a
    /// result for a correlation nobody sent — and a fatal is a fatal however it
    /// was diagnosed.
    pub fn go_fatal(&mut self, detail: String) -> Vec<ConnEffect> {
        self.state = ConnState::Terminal;
        vec![
            ConnEffect::CloseTransport,
            ConnEffect::Emit(ConnEvent::Fatal { detail }),
            ConnEffect::SetWakeup(None),
        ]
    }

    /// Shut the attachment down at the embedder's request. Terminal and silent:
    /// the embedder asked, so it needs no event telling it what it did.
    pub fn close(&mut self) -> Vec<ConnEffect> {
        self.state = ConnState::Terminal;
        vec![ConnEffect::CloseTransport, ConnEffect::SetWakeup(None)]
    }

    fn on_disconnected(
        &mut self,
        code: Option<u16>,
        reason: String,
        now: Millis,
    ) -> Vec<ConnEffect> {
        if let Some(code) = code
            && Some(code) == self.terminal_close_code
        {
            self.state = ConnState::Terminal;
            return vec![
                ConnEffect::Emit(ConnEvent::PeerClosedTerminal { code, reason }),
                ConnEffect::SetWakeup(None),
            ];
        }
        // No `CloseTransport`: the driver already dropped the connection before
        // feeding this input.
        let mut effects = vec![ConnEffect::Emit(ConnEvent::Detached {
            reason: DetachReason::TransportClosed { code, reason },
        })];
        effects.extend(self.enter_backoff(now));
        effects
    }

    /// Enter `Connecting`: emit a `Connect` and arm the handshake deadline.
    fn begin_connect(&mut self, now: Millis) -> Vec<ConnEffect> {
        self.state = ConnState::Connecting;
        self.deadline = now.saturating_add_ms(self.connect_timeout_ms);
        vec![
            ConnEffect::Connect {
                url: self.url.clone(),
            },
            ConnEffect::SetWakeup(Some(self.deadline)),
        ]
    }

    /// Enter `Backoff`: consume one backoff step and arm the retry deadline. The
    /// negotiated version is dropped with the connection that agreed it — the
    /// next attempt negotiates afresh, possibly against a redeployed peer.
    fn enter_backoff(&mut self, now: Millis) -> Vec<ConnEffect> {
        self.version = None;
        let delay = self.backoff_delay_ms();
        self.backoff_step = self.backoff_step.saturating_add(1);
        self.state = ConnState::Backoff;
        self.deadline = now.saturating_add_ms(delay);
        vec![ConnEffect::SetWakeup(Some(self.deadline))]
    }

    /// The next backoff delay: doubling-capped nominal with equal jitter applied.
    ///
    /// The nominal is plain doubling from `initial_backoff_ms`, capped at
    /// `max_backoff_ms`. Equal jitter then spreads it uniformly over
    /// `[nominal/2, nominal]`: an attacher never retries sooner than half its
    /// nominal step (backoff stays meaningful against a genuinely-down peer)
    /// while a lockstep fleet decorrelates across a `nominal/2`-wide window at
    /// every step, including the cap. Integer arithmetic only — modulo bias over
    /// a `u64` draw against a range this small is irrelevant for load-spreading;
    /// `nominal == 0` degenerates to `0`, harmless.
    fn backoff_delay_ms(&mut self) -> u64 {
        let mut nominal = self.initial_backoff_ms;
        for _ in 0..self.backoff_step {
            nominal = nominal.saturating_mul(2);
            if nominal >= self.max_backoff_ms {
                break;
            }
        }
        // Clamps both the loop's overshoot on the last doubling and the
        // `initial_backoff > max_backoff` config edge, so the cap lives in one
        // place.
        let nominal = nominal.min(self.max_backoff_ms);
        let half = nominal / 2;
        (nominal - half) + (self.jitter.next_u64() % (half + 1))
    }

    fn arm_liveness(&mut self, now: Millis) -> Millis {
        self.deadline = now.saturating_add_ms(self.liveness_ms);
        self.deadline
    }

    /// A frame arrived while awaiting the peer's `Hello`. Only `Hello` is legal
    /// here: nothing else can be trusted to mean what it says before both ends
    /// know which schema is in force.
    fn on_text_negotiating(&mut self, text: &str) -> ConnStep {
        let frame = match parse_frame(text) {
            Ok(frame) => frame,
            Err(detail) => return ConnStep::effects(self.go_fatal(detail)),
        };
        let ServerFrame::Hello { versions, .. } = frame else {
            return ConnStep::effects(self.go_fatal(format!(
                "expected Hello as the first server frame, got {}",
                frame_type_name(&frame)
            )));
        };
        match negotiate(SUPPORTED_VERSIONS, versions) {
            Some(version) => {
                self.version = Some(version);
                self.state = ConnState::AwaitingWelcome;
                ConnStep::none()
            }
            None => {
                self.state = ConnState::Terminal;
                ConnStep::effects(vec![
                    ConnEffect::CloseTransport,
                    ConnEffect::Emit(ConnEvent::Incompatible {
                        ours: SUPPORTED_VERSIONS,
                        theirs: versions,
                    }),
                    ConnEffect::SetWakeup(None),
                ])
            }
        }
    }

    /// A frame arrived after a successful negotiation. Only `Welcome` is legal,
    /// and it must echo the version both ends computed — a peer that agreed one
    /// version and is speaking another is unreconcilable, not merely surprising.
    fn on_text_awaiting_welcome(&mut self, text: &str, now: Millis) -> ConnStep {
        let frame = match parse_frame(text) {
            Ok(frame) => frame,
            Err(detail) => return ConnStep::effects(self.go_fatal(detail)),
        };
        let ServerFrame::Welcome {
            version,
            participant_id,
            session_id,
            heartbeat_secs,
            max_body_bytes,
            max_frame_bytes,
            alert_granted,
        } = frame
        else {
            return ConnStep::effects(self.go_fatal(format!(
                "expected Welcome after the version handshake, got {}",
                frame_type_name(&frame)
            )));
        };
        let agreed = self.version.expect("attach client: negotiated version");
        if version != agreed {
            return ConnStep::effects(self.go_fatal(format!(
                "Welcome states version {version}, but the handshake agreed {agreed}"
            )));
        }
        // A zero heartbeat yields a zero liveness window, which reaps the
        // attachment on its first tick and reconnects into the identical
        // `Welcome` — an endless churn nobody can diagnose. It is a contract fact
        // this end cannot honour, so it dies like every other one.
        if heartbeat_secs == 0 {
            return ConnStep::effects(
                self.go_fatal("Welcome states a zero heartbeat interval".to_string()),
            );
        }
        // The attachment is up: the backoff schedule resets, so the next drop
        // starts from the initial delay rather than wherever the last outage
        // left off.
        self.backoff_step = 0;
        self.state = ConnState::Active;
        self.liveness_ms = u64::from(heartbeat_secs)
            .saturating_mul(u64::from(self.liveness_multiplier))
            .saturating_mul(1_000);
        let deadline = self.arm_liveness(now);
        ConnStep::effects(vec![
            ConnEffect::Emit(ConnEvent::Attached(AttachmentFacts {
                version,
                participant_id,
                session_id,
                heartbeat_secs,
                max_body_bytes,
                max_frame_bytes,
                alert_granted,
            })),
            ConnEffect::SetWakeup(Some(deadline)),
        ])
    }

    /// A frame arrived while live. Every inbound frame re-arms liveness; the
    /// connection's own frames are consumed here and everything else is routed.
    /// A repeated handshake frame is fatal — there is no renegotiation, and a
    /// second `Welcome` would restate a contract the planes above have already
    /// built on.
    fn on_text_active(&mut self, text: &str, now: Millis) -> ConnStep {
        let frame = match parse_frame(text) {
            Ok(frame) => frame,
            Err(detail) => return ConnStep::effects(self.go_fatal(detail)),
        };
        match frame {
            ServerFrame::Hello { .. } => {
                ConnStep::effects(self.go_fatal("second Hello frame".to_string()))
            }
            ServerFrame::Welcome { .. } => {
                ConnStep::effects(self.go_fatal("second Welcome frame".to_string()))
            }
            ServerFrame::Heartbeat => {
                ConnStep::effects(vec![ConnEffect::SetWakeup(Some(self.arm_liveness(now)))])
            }
            routed => ConnStep {
                effects: vec![ConnEffect::SetWakeup(Some(self.arm_liveness(now)))],
                routed: Some(routed),
            },
        }
    }
}

/// Parse a server frame, or diagnose why it could not be.
///
/// The diagnosis includes serde's own message, which names the failing field and
/// may quote the offending value. Safe here because the peer is the server this
/// attacher already takes its whole contract from, the detail rides
/// [`ConnEvent::Fatal`] into the attacher's own diagnostics, and a frame this
/// end cannot parse is otherwise undiagnosable.
fn parse_frame(text: &str) -> Result<ServerFrame, String> {
    serde_json::from_str::<ServerFrame>(text)
        .map_err(|err| format!("unparseable server frame: {err}"))
}

/// The `type` tag of a server frame, for diagnostics.
fn frame_type_name(frame: &ServerFrame) -> &'static str {
    match frame {
        ServerFrame::Hello { .. } => "Hello",
        ServerFrame::Welcome { .. } => "Welcome",
        ServerFrame::Heartbeat => "Heartbeat",
        ServerFrame::SubscribeResult { .. } => "SubscribeResult",
        ServerFrame::Deliver { .. } => "Deliver",
        ServerFrame::PublishResult { .. } => "PublishResult",
        ServerFrame::PublishBatchResult { .. } => "PublishBatchResult",
        ServerFrame::DeferredView { .. } => "DeferredView",
    }
}

/// Milliseconds of a config `Duration`. Config durations are small (seconds); a
/// value large enough to overflow `u64` millis is a configuration error.
fn duration_ms(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).expect("attach client: config duration too large")
}

/// A minimal splitmix64 PRNG: one `u64` of state, advanced by the standard
/// splitmix64 step. Deterministic given its seed, dependency-free (no `rand`, no
/// `getrandom`). Used only to jitter the reconnect backoff, where the whole
/// requirement is cross-attacher distinctness of the schedule, not statistical
/// quality — splitmix64 vastly exceeds that bar.
struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

#[cfg(test)]
mod tests;
