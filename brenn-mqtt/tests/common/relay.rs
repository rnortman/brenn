//! `TcpRelay` — a dumb loopback TCP byte relay between the brenn client and the
//! mosquitto broker port, used to drop a connection mid-session while leaving the
//! broker itself running (the one thing `BrokerHarness` cannot do).
//!
//! TLS passes through untouched: the relay forwards opaque bytes in both
//! directions and the client still dials `127.0.0.1`, so certificate validation
//! is unaffected. Every error path panics or drops the connection — a relay fault
//! surfaces as a failing test (via a capped wait), never a silent pass.

use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::JoinHandle;

/// Forwarding mode for one live relayed connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    /// Pump bytes in both directions.
    Forward,
    /// Freeze: issue no reads, hold both sockets open, move no bytes.
    Stall,
    /// Tear the connection down (drop both stream halves).
    Sever,
}

/// A loopback TCP relay in front of a broker port. Point a client config's `port`
/// at [`TcpRelay::port`]; the relay forwards to `target_port`.
pub struct TcpRelay {
    /// Loopback port the relay listens on; point the client config here.
    pub port: u16,
    /// Accept-loop task; aborted on `Drop`.
    accept_task: JoinHandle<()>,
    /// One mode sender per accepted connection (never pruned; dead senders are
    /// harmlessly ignored on broadcast).
    conns: Arc<Mutex<Vec<watch::Sender<Mode>>>>,
}

impl TcpRelay {
    /// Bind `127.0.0.1:0` and start the accept loop forwarding to `target_port`.
    pub async fn start(target_port: u16) -> Self {
        let listener = TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("TcpRelay: failed to bind ephemeral port");
        let port = listener.local_addr().expect("TcpRelay: local_addr").port();

        let conns: Arc<Mutex<Vec<watch::Sender<Mode>>>> = Arc::new(Mutex::new(Vec::new()));
        let conns_for_task = conns.clone();

        let accept_task = tokio::spawn(async move {
            loop {
                let (inbound, _peer) = match listener.accept().await {
                    Ok(pair) => pair,
                    Err(e) => panic!("TcpRelay: accept error: {e}"),
                };
                let outbound = match TcpStream::connect(("127.0.0.1", target_port)).await {
                    Ok(s) => s,
                    Err(e) => {
                        panic!("TcpRelay: failed to connect to target port {target_port}: {e}")
                    }
                };
                let (mode_tx, mode_rx) = watch::channel(Mode::Forward);
                conns_for_task
                    .lock()
                    .expect("TcpRelay: conns mutex poisoned")
                    .push(mode_tx);
                tokio::spawn(relay_connection(inbound, outbound, mode_rx));
            }
        });

        Self {
            port,
            accept_task,
            conns,
        }
    }

    /// Freeze forwarding on all current connections: after `stall()` returns, no
    /// byte read *after* the mode change is forwarded (the `biased;` select in
    /// `relay_connection` makes the mode change win over a pending read). A chunk
    /// already read when the stall lands may still complete its in-flight
    /// `write_all`; the current test never relies on the stricter boundary
    /// because it issues the stalled publish only after `stall()` returns.
    /// Sockets stay open, no new bytes move. New connections are unaffected.
    pub async fn stall(&self) {
        self.broadcast(Mode::Stall);
    }

    /// Close all current connections (drop both stream halves). The listener keeps
    /// accepting, so a reconnect goes through in `Forward` mode.
    pub async fn sever(&self) {
        self.broadcast(Mode::Sever);
    }

    fn broadcast(&self, mode: Mode) {
        for tx in self
            .conns
            .lock()
            .expect("TcpRelay: conns mutex poisoned")
            .iter()
        {
            // A dead receiver (connection already torn down) is expected; ignore it.
            let _ = tx.send(mode);
        }
    }
}

impl Drop for TcpRelay {
    fn drop(&mut self) {
        self.accept_task.abort();
        self.broadcast(Mode::Sever);
    }
}

/// Own both stream halves of one relayed connection and pump bytes per `mode`.
///
/// The `select!` is `biased;` with the mode branch first: with tokio's default
/// random polling a ready read could win over an already-signaled mode change,
/// letting a forward slip through after `stall()` returns. Biased polling makes
/// the mode change win deterministically.
async fn relay_connection(
    mut inbound: TcpStream,
    mut outbound: TcpStream,
    mut mode_rx: watch::Receiver<Mode>,
) {
    let mut in_buf = [0u8; 8192];
    let mut out_buf = [0u8; 8192];

    loop {
        let mode = *mode_rx.borrow_and_update();
        match mode {
            Mode::Sever => return,
            Mode::Stall => {
                // Issue no reads; await a mode change only.
                if mode_rx.changed().await.is_err() {
                    return; // sender dropped (relay torn down)
                }
            }
            Mode::Forward => {
                tokio::select! {
                    biased;
                    changed = mode_rx.changed() => {
                        if changed.is_err() {
                            return; // sender dropped
                        }
                    }
                    r = inbound.read(&mut in_buf) => {
                        match r {
                            Ok(0) | Err(_) => return,
                            Ok(n) => {
                                if outbound.write_all(&in_buf[..n]).await.is_err() {
                                    return;
                                }
                            }
                        }
                    }
                    r = outbound.read(&mut out_buf) => {
                        match r {
                            Ok(0) | Err(_) => return,
                            Ok(n) => {
                                if inbound.write_all(&out_buf[..n]).await.is_err() {
                                    return;
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
