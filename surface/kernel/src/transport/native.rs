//! Native WebSocket transport backed by `tokio-tungstenite`.
//!
//! Version-unified with the backend's tungstenite pin so the shared WS types
//! agree. Authentication is the connector's business: it is constructed with
//! the session cookie and injects it into the handshake via
//! [`insert_session_cookie`], the single source of truth for the handshake
//! auth header shape. The backend test helper
//! `test_support::http::surface_ws_open` calls the same helper, so the
//! `brenn_session=<token>` shape lives in one place.
//!
//! Server liveness pings never cross the transport boundary: tungstenite
//! auto-queues a Pong on every inbound Ping, and this impl flushes it out
//! immediately so an idle (read-only) connection still answers the server's
//! heartbeat and is not reaped. Only text/binary/close/failure become
//! [`TransportEvent`]s.

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::header::{COOKIE, InvalidHeaderValue};
use tokio_tungstenite::tungstenite::http::{HeaderMap, HeaderValue, Uri};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use super::{TransportConnection, TransportConnector, TransportError, TransportEvent};

/// Single source of truth for the surface WS handshake auth header: sets
/// `cookie: brenn_session=<token>` on `headers`. Client-side counterpart of
/// the backend's cookie extraction (`extract_session_cookie` in brenn's
/// middleware). Errs when the token contains bytes invalid in a header value.
///
/// `headers` must not already carry a `cookie` header: this helper owns the
/// handshake `cookie` header outright and panics on a preexisting one rather
/// than silently clobbering a caller's data. Callers with their own cookies to
/// send must fold them into the token/value they pass here.
///
/// `HeaderMap` / `InvalidHeaderValue` are tungstenite's `http` types
/// (re-exported by this crate): callers pass headers from their own
/// tungstenite request, so their tungstenite dependency must stay
/// semver-compatible with this crate's pin (see the module doc's
/// version-unification note). A tungstenite/`http` major bump here is a
/// breaking change to this function.
pub fn insert_session_cookie(
    headers: &mut HeaderMap,
    session_token: &str,
) -> Result<(), InvalidHeaderValue> {
    assert!(
        !headers.contains_key(COOKIE),
        "insert_session_cookie: headers already carry a cookie header; \
         this helper owns the handshake cookie and will not clobber it"
    );
    let cookie = HeaderValue::from_str(&format!("brenn_session={session_token}"))?;
    headers.insert(COOKIE, cookie);
    Ok(())
}

/// Opens native surface WS connections, injecting the session cookie into the
/// handshake headers.
pub struct NativeConnector {
    session_cookie: String,
}

impl NativeConnector {
    /// Construct a connector that authenticates with `session_cookie` (the raw
    /// `brenn_session` token value, sent as a `cookie` header on connect).
    pub fn new(session_cookie: impl Into<String>) -> Self {
        Self {
            session_cookie: session_cookie.into(),
        }
    }
}

impl TransportConnector for NativeConnector {
    type Conn = NativeConnection;

    async fn connect(&mut self, url: &str) -> Result<Self::Conn, TransportError> {
        let mut req = url
            .into_client_request()
            .map_err(|err| TransportError::new(err.to_string()))?;
        reject_cleartext_to_remote(req.uri())?;
        insert_session_cookie(req.headers_mut(), &self.session_cookie)
            .map_err(|err| TransportError::new(err.to_string()))?;
        let (ws, _resp) = tokio_tungstenite::connect_async(req)
            .await
            .map_err(|err| TransportError::new(err.to_string()))?;
        Ok(NativeConnection { ws })
    }
}

/// Refuse to attach the session cookie to a cleartext `ws://` handshake bound
/// for a non-loopback host: the token would transit in the clear and a passive
/// on-path observer could capture it. `wss://` (encrypted) is always allowed;
/// the native transport is otherwise for tests against loopback, and this
/// encodes that scoping structurally rather than leaving it to prose.
fn reject_cleartext_to_remote(uri: &Uri) -> Result<(), TransportError> {
    if uri.scheme_str() == Some("ws") {
        let host = uri.host().unwrap_or_default();
        let is_loopback = host == "localhost"
            || host
                .parse::<std::net::IpAddr>()
                .map(|ip| ip.is_loopback())
                .unwrap_or(false);
        if !is_loopback {
            return Err(TransportError::new(format!(
                "refusing to send the session cookie over cleartext ws:// to non-loopback host {host:?}"
            )));
        }
    }
    Ok(())
}

/// A live native WebSocket connection.
pub struct NativeConnection {
    ws: WebSocketStream<MaybeTlsStream<TcpStream>>,
}

impl TransportConnection for NativeConnection {
    async fn send_text(&mut self, text: String) -> Result<(), TransportError> {
        // `send` feeds and flushes, keeping the stream drained.
        self.ws
            .send(Message::Text(text.into()))
            .await
            .map_err(|err| TransportError::new(err.to_string()))
    }

    async fn next_event(&mut self) -> TransportEvent {
        loop {
            match self.ws.next().await {
                Some(Ok(Message::Text(text))) => return TransportEvent::Text(text.to_string()),
                Some(Ok(Message::Binary(bytes))) => {
                    return TransportEvent::Binary(bytes.to_vec());
                }
                Some(Ok(Message::Ping(_))) => {
                    // tungstenite queued the Pong; flush it so an otherwise idle
                    // connection answers the server's liveness Ping.
                    if let Err(err) = self.ws.flush().await {
                        return TransportEvent::Failed(err.to_string());
                    }
                }
                // An unsolicited Pong carries nothing the core cares about.
                Some(Ok(Message::Pong(_))) => {}
                // tungstenite never yields a raw Frame from a message read; if
                // one ever surfaces the read contract has changed under us —
                // fail loudly rather than silently drop protocol data.
                Some(Ok(Message::Frame(_))) => {
                    unreachable!("tungstenite never yields raw frames from a message read")
                }
                Some(Ok(Message::Close(frame))) => {
                    // tungstenite queued the close-handshake reply at read time;
                    // flush it so the peer sees a clean close rather than a
                    // truncated teardown (which the backend would log as a
                    // protocol anomaly).
                    if let Err(err) = self.ws.flush().await {
                        tracing::debug!(%err, "surface native transport close-reply flush failed");
                    }
                    let (code, reason) = match frame {
                        Some(frame) => (Some(u16::from(frame.code)), frame.reason.to_string()),
                        None => (None, String::new()),
                    };
                    return TransportEvent::Closed { code, reason };
                }
                Some(Err(err)) => return TransportEvent::Failed(err.to_string()),
                None => {
                    return TransportEvent::Closed {
                        code: None,
                        reason: String::new(),
                    };
                }
            }
        }
    }

    async fn close(&mut self) {
        // Best-effort orderly close; a failure means the peer is already gone,
        // which the driver observes on the next transport event anyway.
        if let Err(err) = self.ws.close(None).await {
            tracing::debug!(%err, "surface native transport close failed (peer likely gone)");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `insert_session_cookie` writes exactly one `cookie` header whose value is
    /// `brenn_session=<token>`.
    #[test]
    fn insert_session_cookie_writes_expected_header() {
        let mut headers = HeaderMap::new();
        insert_session_cookie(&mut headers, "devtoken").unwrap();
        let values: Vec<_> = headers.get_all(COOKIE).iter().collect();
        assert_eq!(values.len(), 1, "exactly one cookie header expected");
        assert_eq!(values[0], "brenn_session=devtoken");
    }

    /// A token containing bytes invalid in a header value returns `Err` rather
    /// than panicking or writing a malformed header.
    #[test]
    fn insert_session_cookie_rejects_invalid_bytes() {
        let mut headers = HeaderMap::new();
        let result = insert_session_cookie(&mut headers, "bad\nvalue");
        assert!(result.is_err(), "newline in token should be rejected");
        assert!(
            headers.get(COOKIE).is_none(),
            "no header written on rejection"
        );
    }

    /// A preexisting `cookie` header is unexpected caller input: the helper
    /// panics rather than silently clobbering it.
    #[test]
    #[should_panic(expected = "already carry a cookie header")]
    fn insert_session_cookie_panics_on_preexisting_cookie() {
        let mut headers = HeaderMap::new();
        headers.insert(COOKIE, HeaderValue::from_static("other=1"));
        let _ = insert_session_cookie(&mut headers, "devtoken");
    }

    /// Connecting to a refused address is a normal retryable outcome: it
    /// returns `Err`, never panics.
    #[tokio::test]
    async fn connect_refused_is_error_not_panic() {
        // Bind then immediately drop to obtain an address nothing is listening
        // on, so the connect is refused deterministically (no timeout wait).
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let mut connector = NativeConnector::new("devtoken");
        let url = format!("ws://{addr}/surface/demo/ws");
        let result = connector.connect(&url).await;
        assert!(result.is_err(), "refused connect should be Err, got Ok");
    }

    /// A malformed WS URL fails at request construction, mapped to a
    /// `TransportError` rather than panicking.
    #[tokio::test]
    async fn connect_bad_url_is_error() {
        let mut connector = NativeConnector::new("devtoken");
        let result = connector.connect("http://not-websocket/").await;
        assert!(result.is_err(), "non-ws URL should be Err, got Ok");
    }

    /// A cleartext `ws://` URL bound for a non-loopback host is refused before
    /// any connect, so the session cookie never transits in the clear.
    #[tokio::test]
    async fn connect_cleartext_remote_is_refused() {
        let mut connector = NativeConnector::new("devtoken");
        // TEST-NET-1 address (RFC 5737), non-loopback; the guard fires before
        // any network activity, so this is deterministic and offline.
        let result = connector.connect("ws://192.0.2.1/surface/demo/ws").await;
        assert!(result.is_err(), "cleartext remote ws:// should be refused");
    }

    /// `next_event` maps each wire `Message` to the right `TransportEvent`, and
    /// a server Ping produces no event while its auto-queued Pong is flushed to
    /// the peer.
    #[tokio::test]
    async fn next_event_maps_frames_and_answers_ping() {
        use tokio_tungstenite::tungstenite::protocol::CloseFrame;
        use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            ws.send(Message::Text("hello".into())).await.unwrap();
            ws.send(Message::Binary(vec![1, 2, 3].into()))
                .await
                .unwrap();
            ws.send(Message::Ping(vec![9].into())).await.unwrap();
            // The client flushes the auto-queued Pong when it processes the Ping.
            loop {
                match ws.next().await {
                    Some(Ok(Message::Pong(payload))) => {
                        assert_eq!(payload.as_ref(), &[9]);
                        break;
                    }
                    Some(Ok(_)) => continue,
                    other => panic!("expected a pong, got {other:?}"),
                }
            }
            ws.send(Message::Close(Some(CloseFrame {
                code: CloseCode::from(3001),
                reason: "build".into(),
            })))
            .await
            .unwrap();
        });

        let mut connector = NativeConnector::new("devtoken");
        let url = format!("ws://{addr}/surface/demo/ws");
        let mut conn = connector.connect(&url).await.unwrap();

        assert_eq!(
            conn.next_event().await,
            TransportEvent::Text("hello".into())
        );
        assert_eq!(
            conn.next_event().await,
            TransportEvent::Binary(vec![1, 2, 3])
        );
        // The Ping produces no event; the next event is the Close, meaning the
        // Ping was silently consumed (and its Pong flushed — the server asserts
        // it received one before sending Close).
        assert_eq!(
            conn.next_event().await,
            TransportEvent::Closed {
                code: Some(3001),
                reason: "build".into(),
            }
        );

        server.await.unwrap();
    }

    /// Stream exhaustion (peer drops without a Close frame) surfaces as a
    /// `Closed` event with no code.
    #[tokio::test]
    async fn next_event_stream_end_is_closed_without_code() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            // Drop the connection abruptly, no close handshake.
            drop(ws);
        });

        let mut connector = NativeConnector::new("devtoken");
        let url = format!("ws://{addr}/surface/demo/ws");
        let mut conn = connector.connect(&url).await.unwrap();

        match conn.next_event().await {
            TransportEvent::Closed { code: None, .. } | TransportEvent::Failed(_) => {}
            other => panic!("expected Closed/Failed on abrupt drop, got {other:?}"),
        }

        server.await.unwrap();
    }

    /// `send_text` and `close` reach the peer: the server observes the text
    /// frame and then the close handshake.
    #[tokio::test]
    async fn send_text_and_close_reach_the_peer() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            match ws.next().await {
                Some(Ok(Message::Text(text))) => assert_eq!(text.as_str(), "from-client"),
                other => panic!("expected the client's text frame, got {other:?}"),
            }
            loop {
                match ws.next().await {
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(_)) => continue,
                    other => panic!("expected a close from the client, got {other:?}"),
                }
            }
        });

        let mut connector = NativeConnector::new("devtoken");
        let url = format!("ws://{addr}/surface/demo/ws");
        let mut conn = connector.connect(&url).await.unwrap();
        conn.send_text("from-client".into()).await.unwrap();
        conn.close().await;

        server.await.unwrap();
    }
}
