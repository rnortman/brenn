//! Browser WebSocket transport backed by `web_sys::WebSocket`.
//!
//! The browser API is callback-based; this adapts it to the async transport
//! trait by installing `onopen`/`onmessage`/`onclose`/`onerror` handlers that
//! push into an mpsc channel the async methods read from. Authentication needs
//! no work here: the browser attaches the `brenn_session` cookie automatically
//! on a same-origin connect, so the connector carries nothing (unlike the
//! native connector, which injects the cookie itself).
//!
//! Server liveness pings/pongs are invisible to the browser WebSocket API, so —
//! exactly as with the native transport — they never become a [`TransportEvent`]
//! and both transports age identically under the client liveness rule. The
//! binary type is arraybuffer; the server never sends binary, so a binary
//! message is surfaced as [`TransportEvent::Binary`] and the core treats it as a
//! fatal protocol error.

use futures_channel::mpsc::{self, UnboundedReceiver, UnboundedSender};
use futures_util::StreamExt;
use wasm_bindgen::JsCast;
use wasm_bindgen::JsValue;
use wasm_bindgen::closure::Closure;
use web_sys::{BinaryType, CloseEvent, MessageEvent, WebSocket};

use super::{TransportConnection, TransportConnector, TransportError, TransportEvent};

/// An event pushed from a JS callback into the connection's channel. `Open` lets
/// `connect` resolve once the handshake completes; every other outcome rides as
/// a ready-made [`TransportEvent`].
enum Incoming {
    /// The socket opened: `connect` may resolve.
    Open,
    /// A transport event to surface to the core.
    Event(TransportEvent),
}

/// Opens browser WebSocket connections. Carries nothing: the browser attaches
/// the same-origin session cookie for us.
#[derive(Default)]
pub struct WebSysConnector;

impl WebSysConnector {
    /// Construct a browser connector.
    pub fn new() -> Self {
        Self
    }
}

impl TransportConnector for WebSysConnector {
    type Conn = WebSysConnection;

    async fn connect(&mut self, url: &str) -> Result<Self::Conn, TransportError> {
        // Unbounded, unlike every other channel in this crate (the EventStream and
        // control channel panic on overflow; port queues are bounded with an
        // explicit overflow policy). A JS event handler can neither block nor
        // await, so it cannot exert backpressure and cannot drop safely. The
        // in-practice bound is the driver's drain rate: wasm is single-threaded
        // and the driver's effect loop has no suspension point while a connection
        // is live, so it drains one frame per browser macrotask and depth stays
        // ~1. If a future driver change introduces a real await while connected,
        // a flooding server could grow this without bound — keep the drain
        // invariant intact, or add a depth cap that dies loudly here.
        let (tx, mut rx) = mpsc::unbounded();
        // `Socket::open` starts the handshake and installs the handlers; on any
        // early return below the `socket` drops, unregistering them and closing.
        let socket = Socket::open(url, tx)?;
        // Await the handshake outcome: an `Open` means the socket is live; a
        // close or error before it is a normal, retryable connect failure.
        match rx.next().await {
            Some(Incoming::Open) => Ok(WebSysConnection { socket, rx }),
            Some(Incoming::Event(TransportEvent::Closed { code, reason })) => {
                Err(TransportError::new(format!(
                    "websocket closed before open (code {code:?}): {reason}"
                )))
            }
            Some(Incoming::Event(TransportEvent::Failed(desc))) => Err(TransportError::new(desc)),
            // A message before `open` violates the WS spec ordering; treat it as
            // a failed connect rather than pretend the socket is live.
            Some(Incoming::Event(_)) => Err(TransportError::new(
                "websocket delivered a message before opening",
            )),
            None => Err(TransportError::new(
                "websocket channel closed before opening",
            )),
        }
    }
}

/// A live browser WebSocket connection.
pub struct WebSysConnection {
    socket: Socket,
    rx: UnboundedReceiver<Incoming>,
}

impl TransportConnection for WebSysConnection {
    async fn send_text(&mut self, text: String) -> Result<(), TransportError> {
        self.socket.ws.send_with_str(&text).map_err(js_err)
    }

    async fn next_event(&mut self) -> TransportEvent {
        loop {
            match self.rx.next().await {
                Some(Incoming::Event(event)) => return event,
                // The spec fires `open` exactly once and `connect` already
                // consumed it; ignore any spurious duplicate.
                Some(Incoming::Open) => continue,
                // Every sender lives in a handler closure this connection owns,
                // so `None` means the socket was torn down under us.
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
        // Best-effort orderly close; the socket's own `Drop` also closes, and a
        // failure means the peer is already gone (observed on the next event).
        if let Err(err) = self.socket.ws.close() {
            tracing::debug!(
                ?err,
                "surface websys transport close failed (peer likely gone)"
            );
        }
    }
}

/// The socket plus the handler closures that must outlive it. Holding the
/// closures here keeps them registered; dropping this unregisters them (before
/// they drop, so a queued callback cannot fire into freed memory) and closes.
struct Socket {
    ws: WebSocket,
    _onopen: Closure<dyn FnMut()>,
    _onmessage: Closure<dyn FnMut(MessageEvent)>,
    _onclose: Closure<dyn FnMut(CloseEvent)>,
    _onerror: Closure<dyn FnMut()>,
}

impl Socket {
    /// Open a socket to `url` and install the four handlers, each pushing into
    /// `tx`. Fails if the browser rejects the URL.
    fn open(url: &str, tx: UnboundedSender<Incoming>) -> Result<Self, TransportError> {
        let ws = WebSocket::new(url).map_err(js_err)?;
        // Binary frames arrive as ArrayBuffers, decoded in the message handler.
        ws.set_binary_type(BinaryType::Arraybuffer);

        let onopen = {
            let tx = tx.clone();
            Closure::<dyn FnMut()>::new(move || deliver(&tx, Incoming::Open))
        };
        ws.set_onopen(Some(onopen.as_ref().unchecked_ref()));

        let onmessage = {
            let tx = tx.clone();
            Closure::<dyn FnMut(MessageEvent)>::new(move |event: MessageEvent| {
                deliver(&tx, Incoming::Event(message_event(event)));
            })
        };
        ws.set_onmessage(Some(onmessage.as_ref().unchecked_ref()));

        let onclose = {
            let tx = tx.clone();
            Closure::<dyn FnMut(CloseEvent)>::new(move |event: CloseEvent| {
                deliver(
                    &tx,
                    Incoming::Event(TransportEvent::Closed {
                        code: Some(event.code()),
                        reason: event.reason(),
                    }),
                );
            })
        };
        ws.set_onclose(Some(onclose.as_ref().unchecked_ref()));

        let onerror = {
            let tx = tx.clone();
            Closure::<dyn FnMut()>::new(move || {
                // A WebSocket error event carries no actionable detail and is
                // normally followed by a close; surface a transport failure.
                deliver(
                    &tx,
                    Incoming::Event(TransportEvent::Failed("websocket error".to_string())),
                );
            })
        };
        ws.set_onerror(Some(onerror.as_ref().unchecked_ref()));

        Ok(Self {
            ws,
            _onopen: onopen,
            _onmessage: onmessage,
            _onclose: onclose,
            _onerror: onerror,
        })
    }
}

impl Drop for Socket {
    fn drop(&mut self) {
        // Unregister before the closures drop: a queued event firing into a
        // dropped closure would panic. Then close best-effort.
        self.ws.set_onopen(None);
        self.ws.set_onmessage(None);
        self.ws.set_onclose(None);
        self.ws.set_onerror(None);
        if let Err(err) = self.ws.close() {
            tracing::debug!(?err, "surface websys transport close-on-drop failed");
        }
    }
}

/// Push one event into the channel, tolerating a dropped receiver.
fn deliver(tx: &UnboundedSender<Incoming>, incoming: Incoming) {
    match tx.unbounded_send(incoming) {
        Ok(()) => {}
        // Receiver dropped: the connection was torn down, so the event has
        // nowhere to go and is safely discarded. That is the only error
        // `unbounded_send` can return.
        Err(_dropped) => {}
    }
}

/// Decode a `MessageEvent`'s payload into a [`TransportEvent`]. A string is a
/// text frame; an arraybuffer is binary (fatal at the core). Anything else is a
/// transport failure — the browser only yields these two with an arraybuffer
/// binary type.
fn message_event(event: MessageEvent) -> TransportEvent {
    let data = event.data();
    if let Some(text) = data.as_string() {
        TransportEvent::Text(text)
    } else if let Ok(buffer) = data.dyn_into::<js_sys::ArrayBuffer>() {
        TransportEvent::Binary(js_sys::Uint8Array::new(&buffer).to_vec())
    } else {
        TransportEvent::Failed("websocket message was neither text nor arraybuffer".to_string())
    }
}

/// Map a browser `JsValue` error into a [`TransportError`]; the core does not
/// branch on the contents, only on success vs failure.
fn js_err(value: JsValue) -> TransportError {
    TransportError::new(format!("{value:?}"))
}
