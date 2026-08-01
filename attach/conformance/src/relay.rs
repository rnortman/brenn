//! A TCP relay whose live connections can be cut on command.
//!
//! Reconnect behaviour is only worth asserting on if the loss is real: a peer
//! that politely closes, or a client asked to pretend it lost the socket, tests
//! the pretence rather than the recovery. So a conformance run puts this in
//! front of the peer and severs it — every relayed pair's sockets are dropped
//! mid-stream, which is what a network does, and both ends find out the way they
//! would in production.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use tokio::net::{TcpListener, TcpStream};
use tokio::task::{AbortHandle, JoinHandle};

/// A listening relay: everything it accepts is copied to `upstream` and back
/// until the pair ends or [`SeverableRelay::sever`] cuts it.
///
/// Dropping the relay stops accepting and cuts whatever is still live, so a
/// test that forgets to sever leaves no task behind.
pub struct SeverableRelay {
    addr: SocketAddr,
    live: Arc<Mutex<Vec<AbortHandle>>>,
    accepting: JoinHandle<()>,
}

impl SeverableRelay {
    /// Bind a relay on an ephemeral loopback port in front of `upstream`.
    pub async fn spawn(upstream: SocketAddr) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("relay: bind a loopback port");
        let addr = listener
            .local_addr()
            .expect("relay: read back the bound address");
        let live: Arc<Mutex<Vec<AbortHandle>>> = Arc::new(Mutex::new(Vec::new()));
        let pairs = Arc::clone(&live);
        let accepting = tokio::spawn(async move {
            loop {
                let Ok((inbound, _peer)) = listener.accept().await else {
                    // The listener is this task's alone, so an accept error is
                    // the socket being gone, not a per-connection failure.
                    return;
                };
                let pair = tokio::spawn(relay_pair(inbound, upstream));
                let mut held = pairs.lock().expect("relay: the live-pair lock");
                // Finished pairs are the common case; sweeping here keeps the
                // list from growing across a long run without a reaper task.
                held.retain(|handle| !handle.is_finished());
                held.push(pair.abort_handle());
            }
        });
        Self {
            addr,
            live,
            accepting,
        }
    }

    /// The address a client connects to.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Cut every live pair. The relay keeps listening, so the client's next
    /// connect attempt succeeds and reaches the same upstream.
    pub fn sever(&self) {
        let mut held = self.live.lock().expect("relay: the live-pair lock");
        for handle in held.drain(..) {
            handle.abort();
        }
    }
}

impl Drop for SeverableRelay {
    fn drop(&mut self) {
        self.sever();
        self.accepting.abort();
    }
}

/// Copy one accepted connection to and from `upstream` until either end stops.
///
/// Aborting this task drops both sockets, which is the sever.
async fn relay_pair(mut inbound: TcpStream, upstream: SocketAddr) {
    let mut outbound = match TcpStream::connect(upstream).await {
        Ok(outbound) => outbound,
        Err(err) => {
            tracing::debug!(%err, %upstream, "relay: upstream connect failed");
            return;
        }
    };
    match tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await {
        Ok((to_upstream, to_client)) => {
            tracing::debug!(to_upstream, to_client, "relay: pair closed");
        }
        Err(err) => tracing::debug!(%err, "relay: pair ended"),
    }
}
