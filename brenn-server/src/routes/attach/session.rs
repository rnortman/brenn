//! The per-connection substrate every attachment plane shares: the context a
//! frame handler reads, the outcome it answers with, the counters it folds into
//! the disconnect line, and the one write to the outbound queue.
//!
//! Nothing here is application-shaped. The context holds a profile, not a
//! surface; an account, not a browser session; and its violation helper spells
//! the attacher by its principal, so one log format serves a page and a daemon
//! alike.

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::fmt::Display;
use std::net::IpAddr;
use std::sync::Arc;

use brenn_attach_proto::ServerFrame;
use brenn_lib::access::AppPolicy;
use brenn_lib::messaging::Messenger;
use tokio::sync::mpsc;
use uuid::Uuid;

use super::profile::AttachProfile;

/// Outbound frame queue depth. A slow reader fills this, then the delivery path
/// blocks (backpressure at the socket) rather than dropping control frames.
pub const OUTBOUND_QUEUE_FRAMES: usize = 256;

/// Immutable per-connection context every frame handler reads but none mutates.
/// Built once when the session starts and passed as `&AttachSessionCtx`, so
/// handler signatures carry one shared reference rather than a positional list
/// of same-typed identity params (`account`, `ip`, …) a caller could transpose.
/// The genuinely mutable per-session state — the subscription map, the wire
/// cursors, the rate buckets, the counters — is threaded separately as `&mut`.
pub struct AttachSessionCtx {
    /// The authority half of this attachment: which channels it may subscribe
    /// and publish, which sub-identities it may act as. Boot-built by the route
    /// and shared by every session of the attacher.
    pub profile: Arc<dyn AttachProfile>,
    /// The bus. Every store read and every publish this session makes goes
    /// through it.
    pub messenger: Arc<Messenger>,
    /// The attacher's resolved access policy. The session consults it as a
    /// delivery floor on the send path — boot already proved every subscribable
    /// channel granted, so a deny here is fail-closed hygiene, not a feature.
    pub policy: Arc<AppPolicy>,
    /// Per-connection id, minted at attach and advertised in `Welcome` so the
    /// attacher can self-attribute the documents it authors.
    pub session_id: Uuid,
    /// The authenticated account behind this attachment. Held for log
    /// attribution only: authority comes from the profile, never from here.
    pub account: String,
    pub ip: IpAddr,
    /// Outbound frame sender to the writer task. Owning it here means dropping
    /// the context at teardown closes the channel and exits the writer.
    pub tx: mpsc::Sender<ServerFrame>,
}

impl AttachSessionCtx {
    /// A protocol violation on this attachment, named by attacher and account.
    ///
    /// One spelling for every plane, so the security log line that feeds
    /// fail2ban has a stable prefix whatever the frame was. `detail` names the
    /// violated rule and must not echo unsanitized client payload — see
    /// `sanitize_client_detail`.
    pub fn violation(&self, detail: impl Display) -> FrameOutcome {
        FrameOutcome::Violation(format!(
            "attacher {} account {}: {detail}",
            self.profile.attacher().as_str(),
            self.account,
        ))
    }
}

/// What the session loop does after a dispatched inbound frame.
pub enum FrameOutcome {
    /// Frame handled; keep the session running.
    Continue,
    /// Protocol violation: the caller logs+alerts it as a security event and
    /// tears the session down. The detail names the attacher, the account, and
    /// the violated rule, and never echoes the client payload.
    Violation(String),
    /// The writer is gone (socket died mid-send): tear the session down without
    /// a security event.
    Disconnect,
}

/// Per-session counters folded into the single disconnect line. Frame counts
/// cover the application frames the session task processes and enqueues; the
/// writer's liveness `Ping`/`Heartbeat` frames are transport plumbing and are
/// not counted here.
#[derive(Default)]
pub struct SessionCounters {
    /// Inbound text (application) frames dispatched. Binary-frame and
    /// cap-overflow violations tear down before this counts them.
    pub frames_in: u64,
    /// Server frames the session task enqueued to the writer.
    pub frames_out: u64,
    /// Publishes that reached the bus with an `Ok` outcome.
    pub publishes: u64,
    /// Publishes denied by either rate gate — the connection bucket or the
    /// bus-level per-sender gate.
    pub publish_rate_limited: u64,
    /// Publishes rejected for an oversized body at the transport pre-check.
    /// Drives the first-occurrence warn and the escalation-to-violation count.
    pub publish_body_too_large: u64,
    /// Publishes where the transport pre-check admitted a body the bus then
    /// rejected as oversized — a config-wiring bug (both caps derive from one
    /// `max_body_bytes`). Each such arm already `error!`s; this counter keeps
    /// them out of the transport-reject count so escalation is not conflated
    /// with an internal disagreement.
    pub publish_body_cap_disagreement: u64,
    /// `Alert` frames dispatched to the process alert dispatcher (granted, and
    /// within the per-connection alert bucket) — the operator's count of how
    /// many times this attachment paged.
    pub alerts_dispatched: u64,
    /// `Alert` frames dropped by the per-connection alert bucket. Not a kill (a
    /// noisy but legitimate attacher must not lose its session); the
    /// process-wide alert rate limiter bounds total paging downstream.
    pub alerts_suppressed: u64,
    /// Per-attribution publish breakdown — the same grain the send budget meters
    /// and the sender identity carries, so "which sub-identity drained its
    /// budget?" is answerable from the disconnect line without correlating
    /// against the bus.
    ///
    /// **Does not sum to `publishes`/`publish_rate_limited`**, by construction:
    /// the attacher's own publishes name no attribution and have no column here.
    /// The totals are the session's; this is the attributable part of them.
    pub by_attribution: BTreeMap<String, AttributionPublishCounters>,
}

/// One sub-identity's publish outcomes within a session ([`SessionCounters`]).
#[derive(Debug, Default, PartialEq, Eq)]
pub struct AttributionPublishCounters {
    /// Publishes this sub-identity landed on the bus.
    pub publishes: u64,
    /// Publishes denied by either rate gate — the connection bucket or this
    /// sub-identity's own send budget. A component looping on retries shows up
    /// here, under its own name.
    pub publish_rate_limited: u64,
}

impl SessionCounters {
    /// Count one publish that reached the bus, attachment-wide and against the
    /// sub-identity that made it.
    ///
    /// `attribution` is `None` for a publish by the attacher itself, which has
    /// no column — see `by_attribution`. Both counters move together here rather
    /// than at each call site so the breakdown cannot silently stop tracking the
    /// total it decomposes.
    pub fn publish_ok(&mut self, attribution: Option<&str>) {
        self.publishes += 1;
        if let Some(name) = attribution {
            self.by_attribution
                .entry(name.to_string())
                .or_default()
                .publishes += 1;
        }
    }

    /// Count one publish denied by a rate gate, attachment-wide and against the
    /// sub-identity that made it. See [`SessionCounters::publish_ok`].
    pub fn publish_rate_limited(&mut self, attribution: Option<&str>) {
        self.publish_rate_limited += 1;
        if let Some(name) = attribution {
            self.by_attribution
                .entry(name.to_string())
                .or_default()
                .publish_rate_limited += 1;
        }
    }
}

/// Render a client-supplied string for inclusion in a security-event detail.
///
/// Truncates to a short prefix and control-character-escapes the result, so a
/// hostile client cannot inject unbounded length or raw newline/escape bytes
/// into the security log line or the phone-alert body.
pub(crate) fn sanitize_client_detail(s: &str) -> String {
    const MAX_CHARS: usize = 128;
    let mut rendered: String = s
        .chars()
        .take(MAX_CHARS)
        .flat_map(char::escape_debug)
        .collect();
    if s.chars().nth(MAX_CHARS).is_some() {
        rendered.push_str("...");
    }
    rendered
}

/// Send one `ServerFrame` to the writer, counting it and mapping a closed
/// channel (writer gone) to `Disconnect`.
pub async fn send_frame(
    tx: &mpsc::Sender<ServerFrame>,
    frame: ServerFrame,
    counters: &mut SessionCounters,
) -> FrameOutcome {
    match tx.send(frame).await {
        Ok(()) => {
            counters.frames_out += 1;
            FrameOutcome::Continue
        }
        Err(_) => FrameOutcome::Disconnect,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn heartbeat() -> ServerFrame {
        ServerFrame::Heartbeat
    }

    /// The attacher's own publishes have no column, and the two counters move
    /// together for the ones that do — the breakdown cannot stop tracking the
    /// total it decomposes.
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

    /// The count follows the enqueue, and a dead writer is a `Disconnect` that
    /// counts nothing — the session tears down without a security event.
    #[tokio::test]
    async fn send_frame_counts_an_enqueue_and_reports_a_dead_writer() {
        let (tx, mut rx) = mpsc::channel::<ServerFrame>(4);
        let mut counters = SessionCounters::default();

        assert!(matches!(
            send_frame(&tx, heartbeat(), &mut counters).await,
            FrameOutcome::Continue
        ));
        assert_eq!(counters.frames_out, 1);
        assert!(matches!(rx.try_recv(), Ok(ServerFrame::Heartbeat)));

        drop(rx);
        assert!(matches!(
            send_frame(&tx, heartbeat(), &mut counters).await,
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
}
