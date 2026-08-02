//! The attachment's socket lifecycle: the symmetric version handshake that opens
//! a connection, the `Welcome` that states its transport contract, and the writer
//! task that owns the sink for the rest of the connection's life.
//!
//! Nothing here reads a channel, a body, or an application payload. The two
//! halves are deliberately independent: each takes one side of the socket as a
//! generic stream or sink, so both are exercised without one.

#![allow(dead_code)]

#[cfg(test)]
mod tests;

use std::fmt::Display;
use std::time::Duration;

use axum::extract::ws::Message;
use brenn_attach_proto::{
    ClientFrame, SUPPORTED_VERSIONS, ServerFrame, VersionRange, max_client_frame_bytes, negotiate,
};
use futures::{Sink, SinkExt, Stream, StreamExt};
use tokio::sync::mpsc;
use tokio::time::MissedTickBehavior;
use tracing::{info, warn};

use super::session::{AttachSessionCtx, sanitize_client_detail};

/// How long an upgraded socket may stay silent before its `Hello` is overdue.
///
/// Generous against a slow link and a cold client, and short against a socket
/// holding an attachment slot it never attaches with: the protocol has the
/// attacher send `Hello` first without waiting, so this bounds a client that has
/// nothing to wait for.
pub const HELLO_TIMEOUT: Duration = Duration::from_secs(30);

/// The server's half of the symmetric handshake, sent immediately on upgrade
/// without waiting for the attacher's.
///
/// Both ends state their whole range and each computes the agreement itself, so
/// neither waits on the other and an incompatibility needs no refusal frame.
/// `ident` is a free-form build string for logs; the peer never parses it.
pub fn server_hello(ident: &str) -> ServerFrame {
    ServerFrame::Hello {
        versions: SUPPORTED_VERSIONS,
        ident: ident.to_string(),
    }
}

/// What reading the attacher's `Hello` answered.
pub enum Handshake {
    /// Both ends speak this version; it is in force for the rest of the
    /// connection.
    Agreed(u32),
    /// Two well-formed ranges that do not overlap, or a peer range that is empty
    /// (`min > max`). The attacher is conforming — it stated what it speaks — so
    /// this closes with a log naming both ranges and no security event. Carries
    /// the peer's range for that log.
    Incompatible(VersionRange),
    /// Non-conforming behaviour before a version was in force: the rule broken,
    /// rendered by the caller through [`AttachSessionCtx::violation`] so the
    /// security line has the same prefix as every other plane's.
    Violation(String),
    /// The socket closed or died before the attacher said anything. Tear down
    /// without a security event.
    Disconnect,
}

/// Read the attacher's `Hello` and negotiate against this build's range.
///
/// `within` bounds the wait: the protocol has the attacher send `Hello` as its
/// first frame without waiting for the server's, so silence past this bound is a
/// client holding an attachment slot without attaching — non-conforming, and a
/// violation. Liveness frames ahead of the `Hello` are skipped; any other frame
/// before it is a violation, because until a version is agreed there is no schema
/// under which a second frame kind could be read.
pub async fn read_client_hello<S>(stream: &mut S, within: Duration) -> Handshake
where
    S: Stream<Item = Result<Message, axum::Error>> + Unpin,
{
    match tokio::time::timeout(within, next_hello(stream)).await {
        Ok(verdict) => verdict,
        Err(_) => Handshake::Violation(format!("no Hello within {}s of upgrade", within.as_secs())),
    }
}

/// The unbounded half of [`read_client_hello`]: read until the first frame that
/// is not a liveness frame, and judge it.
async fn next_hello<S>(stream: &mut S) -> Handshake
where
    S: Stream<Item = Result<Message, axum::Error>> + Unpin,
{
    loop {
        match stream.next().await {
            Some(Ok(Message::Text(text))) => return classify_hello(text.as_str()),
            // Not application frames. Pong replies are handled by axum.
            Some(Ok(Message::Ping(_) | Message::Pong(_))) => continue,
            Some(Ok(Message::Binary(_))) => {
                return Handshake::Violation("binary frame before Hello".to_string());
            }
            Some(Ok(Message::Close(_))) | None => return Handshake::Disconnect,
            Some(Err(e)) => {
                return match classify_read_error(e) {
                    InboundError::Oversized => {
                        Handshake::Violation("inbound frame exceeds size cap".to_string())
                    }
                    InboundError::Transport(detail) => {
                        warn!("attachment WS read error before Hello: {detail}");
                        Handshake::Disconnect
                    }
                };
            }
        }
    }
}

/// Judge one text frame as the attacher's `Hello`.
///
/// The frame's own shape is frozen across every version, so a parse failure here
/// is not version skew — it is a client that cannot speak the handshake at all.
fn classify_hello(text: &str) -> Handshake {
    let Ok(frame) = serde_json::from_str::<ClientFrame>(text) else {
        return Handshake::Violation("unparseable client frame before Hello".to_string());
    };
    let ClientFrame::Hello { versions, ident } = frame else {
        return Handshake::Violation("first client frame is not Hello".to_string());
    };
    match negotiate(SUPPORTED_VERSIONS, versions) {
        Some(version) => {
            info!(
                version,
                peer_min = versions.min,
                peer_max = versions.max,
                // Client-supplied and never parsed: bounded and escaped before it
                // reaches a log line.
                peer_ident = %sanitize_client_detail(&ident),
                "attachment negotiated"
            );
            Handshake::Agreed(version)
        }
        None => Handshake::Incompatible(versions),
    }
}

/// This attachment's transport contract, sent once the handshake agreed.
///
/// Every field is a fact of the connection: the identity it speaks as, the id it
/// self-attributes with, what it may send, and how it knows the peer is alive.
/// The frame cap is derived from the body cap here rather than client-side, so
/// the number the server enforces is the number the attacher honours.
pub fn welcome(ctx: &AttachSessionCtx, version: u32, heartbeat_secs: u32) -> ServerFrame {
    ServerFrame::Welcome {
        version,
        participant_id: ctx.profile.attacher().as_str().to_string(),
        session_id: ctx.session_id.simple().to_string(),
        heartbeat_secs,
        max_body_bytes: ctx.max_body_bytes as u64,
        max_frame_bytes: max_client_frame_bytes(ctx.max_body_bytes) as u64,
        alert_granted: ctx.profile.alert_granted(),
    }
}

/// What an inbound websocket read error was.
pub enum InboundError {
    /// The read cap fired: the peer sent a frame larger than
    /// [`max_client_frame_bytes`] of the attachment's body cap. Every legal
    /// frame fits under it — a single publish by the body cap, a batch by the
    /// legality law the cap is derived from — so this is tampering or a serious
    /// client bug, a protocol violation and fail2ban signal.
    Oversized,
    /// Anything else — TCP resets, proxy framing, a half-open connection. Tear
    /// down, no security event. Carries the rendered error for the log.
    Transport(String),
}

/// Classify one inbound read error.
///
/// axum wraps the underlying tungstenite error and exposes it through
/// `into_inner`, so the downcast is deterministic — provided the direct
/// `tungstenite` dependency stays version-unified with axum's
/// `tokio-tungstenite` (`Cargo.toml` notes this).
pub fn classify_read_error(err: axum::Error) -> InboundError {
    let inner = err.into_inner();
    let oversized = inner
        .downcast_ref::<tungstenite::Error>()
        .is_some_and(|te| {
            matches!(
                te,
                tungstenite::Error::Capacity(
                    tungstenite::error::CapacityError::MessageTooLong { .. }
                )
            )
        });
    if oversized {
        InboundError::Oversized
    } else {
        InboundError::Transport(inner.to_string())
    }
}

/// Owns the WS sink for the life of the connection.
///
/// Serializes outbound frames, emits the server-side liveness probe (a native
/// `Ping`) every `heartbeat`, adds an idle [`ServerFrame::Heartbeat`] when
/// nothing else was written since the last tick — a browser websocket cannot
/// observe protocol-level pings, so the application frame is what an attacher's
/// inbound-silence rule actually sees — and bounds every write with a
/// stalled-reader watchdog.
///
/// Exits on any sink error, watchdog timeout, or sender drop; exiting drops `rx`,
/// which is what tears the session down.
pub async fn writer_task<S>(mut sink: S, mut rx: mpsc::Receiver<ServerFrame>, heartbeat: Duration)
where
    S: Sink<Message> + Unpin,
    S::Error: Display,
{
    let watchdog = heartbeat * 3;
    let mut ticker = tokio::time::interval(heartbeat);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    ticker.tick().await; // consume the immediate first tick
    let mut wrote_frame_since_tick = false;

    loop {
        tokio::select! {
            maybe_frame = rx.recv() => {
                match maybe_frame {
                    Some(frame) => {
                        let json = serde_json::to_string(&frame)
                            .expect("ServerFrame serialization");
                        if !write_with_watchdog(&mut sink, Message::Text(json.into()), watchdog)
                            .await
                        {
                            return;
                        }
                        wrote_frame_since_tick = true;
                    }
                    None => return,
                }
            }
            _ = ticker.tick() => {
                if !write_with_watchdog(&mut sink, Message::Ping(Vec::new().into()), watchdog).await
                {
                    return;
                }
                if !wrote_frame_since_tick {
                    let json = serde_json::to_string(&ServerFrame::Heartbeat)
                        .expect("ServerFrame serialization");
                    if !write_with_watchdog(&mut sink, Message::Text(json.into()), watchdog).await {
                        return;
                    }
                }
                wrote_frame_since_tick = false;
            }
        }
    }
}

/// One watchdog-bounded sink write. Returns `false` (caller must exit) on sink
/// error or on a stalled reader that keeps a write pending past the watchdog.
///
/// Attribution comes from the session span the writer task is instrumented with,
/// so the `warn!`s below need no explicit fields.
async fn write_with_watchdog<S>(sink: &mut S, msg: Message, watchdog: Duration) -> bool
where
    S: Sink<Message> + Unpin,
    S::Error: Display,
{
    match tokio::time::timeout(watchdog, sink.send(msg)).await {
        Ok(Ok(())) => true,
        Ok(Err(e)) => {
            warn!("attachment WS write failed: {e}");
            false
        }
        Err(_) => {
            warn!("attachment WS writer stalled (reader not draining); tearing down");
            false
        }
    }
}
