//! `PacketRelay` — a loopback relay between a Brenn client and the broker that
//! terminates TLS on both legs, frames the MQTT packets in each direction, logs
//! every one, and can lose a chosen packet: drop it with the connection kept,
//! or cut the connection at it. It can inject a packet toward the client,
//! refuse a filter in the broker's SUBACK, turn new connections away, and
//! sever the live ones.
//!
//! It turns a client away in two ways, both an unreachable broker to the
//! supervisor and different errors on the client's side. Refusing closes the
//! listening socket, so a dial fails with "connection refused", as against a
//! broker that is down. Hanging up accepts the TCP connection and closes it at
//! once, so the client's TLS handshake ends early, as against a broker at its
//! connection limit or a TLS proxy whose backend is away.
//!
//! Toward the client it presents the suite's server certificate on
//! `127.0.0.1`, so a client that trusts the suite's CA connects to it as to the
//! broker; toward the broker it is a client trusting the same CA.
//!
//! Every failure of the relay's own making panics; a connection the client or
//! the broker ends is logged and closed.

use std::sync::{Arc, Mutex};

use rustls_pki_types::pem::PemObject as _;
use rustls_pki_types::{CertificateDer, ServerName};
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio_rustls::{TlsAcceptor, TlsConnector};

use brenn_mqtt::test_support::certs;

pub const CONNECT: u8 = 1;
pub const CONNACK: u8 = 2;
pub const PUBLISH: u8 = 3;
pub const PUBACK: u8 = 4;
pub const PUBREC: u8 = 5;
pub const PUBREL: u8 = 6;
pub const PUBCOMP: u8 = 7;
pub const SUBSCRIBE: u8 = 8;
pub const SUBACK: u8 = 9;
pub const UNSUBSCRIBE: u8 = 10;
pub const DISCONNECT: u8 = 14;

/// The SUBACK reason code Not authorized.
const NOT_AUTHORIZED: u8 = 0x87;

/// The direction a packet travels.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Dir {
    ClientToBroker,
    BrokerToClient,
}

/// What became of a packet, or what happened to a connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fate {
    Forwarded,
    /// Discarded; the connection carried on.
    Dropped,
    /// The connection was severed instead of forwarding it.
    CutBefore,
    /// Forwarded, then the connection was severed.
    CutAfter,
    /// Written toward the client by the test; the broker never sent it.
    Injected,
    /// A SUBACK forwarded with a refused filter's reason code changed to Not
    /// authorized.
    Refused,
    /// A connection event, not a packet: ended by one side, severed by the
    /// test, or accepted and closed at once while hanging up.
    Ended,
    Severed,
    HungUp,
}

/// One packet the relay saw, or one connection event.
#[derive(Clone, Debug)]
pub struct Packet {
    /// Connection number, from 1 in accept order.
    pub conn: usize,
    pub dir: Dir,
    /// The MQTT packet type; 0 for a connection event.
    pub ptype: u8,
    pub fate: Fate,
    /// The whole packet as framed, fixed header included; empty for a
    /// connection event.
    pub bytes: Vec<u8>,
    pub pkid: Option<u16>,
    /// A PUBLISH's DUP flag.
    pub dup: bool,
    /// A PUBLISH's QoS.
    pub qos: u8,
    /// A PUBLISH's topic.
    pub topic: Option<String>,
    /// A PUBLISH's Message Expiry Interval property.
    pub expiry: Option<u32>,
    /// A CONNECT's Clean Start flag.
    pub clean_start: Option<bool>,
    /// A CONNACK's Session Present flag.
    pub session_present: Option<bool>,
    /// A DISCONNECT's Session Expiry Interval property.
    pub session_expiry: Option<u32>,
}

impl Packet {
    fn event(conn: usize, dir: Dir, fate: Fate) -> Self {
        Self {
            conn,
            dir,
            ptype: 0,
            fate,
            bytes: Vec::new(),
            pkid: None,
            dup: false,
            qos: 0,
            topic: None,
            expiry: None,
            clean_start: None,
            session_present: None,
            session_expiry: None,
        }
    }

    /// Whether this is a packet of `ptype` travelling in `dir`.
    pub fn is(&self, dir: Dir, ptype: u8) -> bool {
        self.dir == dir && self.ptype == ptype
    }
}

/// Lose the next packet of `ptype` travelling in `dir`: sever the connection
/// before forwarding it, or after when `after`. One-shot.
#[derive(Clone, Copy, Debug)]
pub struct Cut {
    pub dir: Dir,
    pub ptype: u8,
    pub after: bool,
}

struct State {
    log: Vec<Packet>,
    cut: Option<Cut>,
    /// Packets discarded with the connection kept, by direction and type.
    drops: Vec<(Dir, u8)>,
    next_conn: usize,
    severs: Vec<watch::Sender<bool>>,
    /// Packets to write toward the client, by connection.
    injects: Vec<(usize, mpsc::UnboundedSender<Vec<u8>>)>,
    hang_up: bool,
    /// Filters whose SUBACK reason code is changed to Not authorized.
    refused: Vec<String>,
    /// SUBSCRIBEs naming a refused filter, awaiting their SUBACK: connection,
    /// packet identifier, and the refused filters' positions in the packet.
    refusing: Vec<(usize, u16, Vec<usize>)>,
}

/// The relay. Point a client config's `port` at [`PacketRelay::port`].
pub struct PacketRelay {
    pub port: u16,
    state: Arc<Mutex<State>>,
    accept_task: JoinHandle<()>,
    refuse: watch::Sender<bool>,
    /// Whether the listener is bound, as the accept task last left it.
    listening: watch::Receiver<bool>,
}

impl Drop for PacketRelay {
    fn drop(&mut self) {
        self.accept_task.abort();
        self.sever();
    }
}

impl PacketRelay {
    /// Bind `127.0.0.1:0` and relay every accepted connection to the TLS
    /// broker on `broker_port`.
    pub async fn start(broker_port: u16) -> Self {
        let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        let mut roots = rustls::RootCertStore::empty();
        roots
            .add(
                CertificateDer::from_pem_slice(certs::ca_pem().as_bytes())
                    .expect("PacketRelay: the suite's CA parses"),
            )
            .expect("PacketRelay: the suite's CA is a trust anchor");
        let client = rustls::ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .expect("PacketRelay: default protocol versions")
            .with_root_certificates(roots)
            .with_no_client_auth();
        let acceptor = TlsAcceptor::from(certs::server_config());
        let connector = TlsConnector::from(Arc::new(client));

        let listener = TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("PacketRelay: bind");
        let port = listener
            .local_addr()
            .expect("PacketRelay: local_addr")
            .port();
        let state = Arc::new(Mutex::new(State {
            log: Vec::new(),
            cut: None,
            drops: Vec::new(),
            next_conn: 1,
            severs: Vec::new(),
            injects: Vec::new(),
            hang_up: false,
            refused: Vec::new(),
            refusing: Vec::new(),
        }));
        let st = state.clone();
        let (refuse, mut refuse_rx) = watch::channel(false);
        let (listening_tx, listening) = watch::channel(true);
        let accept_task = tokio::spawn(async move {
            let mut listener = Some(listener);
            loop {
                if *refuse_rx.borrow_and_update() {
                    drop(listener.take());
                    listening_tx.send_replace(false);
                    while *refuse_rx.borrow_and_update() {
                        refuse_rx.changed().await.expect("PacketRelay: dropped");
                    }
                    listener = Some(
                        TcpListener::bind(("127.0.0.1", port))
                            .await
                            .expect("PacketRelay: rebind its port"),
                    );
                    listening_tx.send_replace(true);
                    continue;
                }
                let bound = listener
                    .as_ref()
                    .expect("PacketRelay: bound while accepting");
                let inbound = tokio::select! {
                    biased;
                    _ = refuse_rx.changed() => continue,
                    accepted = bound.accept() => accepted.expect("PacketRelay: accept").0,
                };
                let (sever_tx, sever_rx) = watch::channel(false);
                let (inject_tx, inject_rx) = mpsc::unbounded_channel();
                let mut s = st.lock().unwrap();
                let conn = s.next_conn;
                s.next_conn += 1;
                if s.hang_up {
                    s.log
                        .push(Packet::event(conn, Dir::ClientToBroker, Fate::HungUp));
                    drop(inbound);
                    continue;
                }
                s.severs.push(sever_tx);
                s.injects.push((conn, inject_tx));
                drop(s);
                tokio::spawn(relay_connection(
                    st.clone(),
                    conn,
                    inbound,
                    broker_port,
                    acceptor.clone(),
                    connector.clone(),
                    Controls {
                        sever: sever_rx,
                        inject: inject_rx,
                    },
                ));
            }
        });
        Self {
            port,
            state,
            accept_task,
            refuse,
            listening,
        }
    }

    /// Arm a one-shot cut, replacing any armed one.
    pub fn arm(&self, cut: Cut) {
        self.state.lock().unwrap().cut = Some(cut);
    }

    /// Discard every packet of `ptype` travelling in `dir` from now on.
    pub fn drop_packets(&self, dir: Dir, ptype: u8) {
        self.state.lock().unwrap().drops.push((dir, ptype));
    }

    /// Refuse new connections, or stop refusing. Returns once the listener
    /// is closed, or bound again.
    pub async fn refuse(&self, refuse: bool) {
        self.refuse.send_replace(refuse);
        self.listening
            .clone()
            .wait_for(|listening| *listening != refuse)
            .await
            .expect("PacketRelay: the accept task ended");
    }

    /// Accept each new connection and close it at once, logging it as
    /// [`Fate::HungUp`], or stop doing so.
    pub fn hang_up(&self, hang_up: bool) {
        self.state.lock().unwrap().hang_up = hang_up;
    }

    /// Answer every SUBSCRIBE for `filter` from now on with Not authorized
    /// for it, whatever the broker answered, as a broker whose ACL refuses it
    /// would.
    pub fn refuse_filter(&self, filter: &str) {
        self.state.lock().unwrap().refused.push(filter.to_string());
    }

    /// Stop discarding packets.
    pub fn clear_drops(&self) {
        self.state.lock().unwrap().drops.clear();
    }

    /// Write the framed packet `bytes` toward the client on connection
    /// `conn`, logged as [`Fate::Injected`].
    pub fn inject(&self, conn: usize, bytes: Vec<u8>) {
        let s = self.state.lock().unwrap();
        let (_, tx) = s
            .injects
            .iter()
            .find(|(c, _)| *c == conn)
            .unwrap_or_else(|| panic!("PacketRelay: no conn{conn} to inject into"));
        tx.send(bytes)
            .unwrap_or_else(|_| panic!("PacketRelay: conn{conn} has ended"));
    }

    /// Sever every live connection.
    pub fn sever(&self) {
        for tx in &self.state.lock().unwrap().severs {
            tx.send_replace(true);
        }
    }

    /// Everything seen so far, in order.
    pub fn log(&self) -> Vec<Packet> {
        self.state.lock().unwrap().log.clone()
    }

    /// The packets of `ptype` travelling in `dir` on connection `conn` that
    /// reached the other side.
    pub fn forwarded(&self, conn: usize, dir: Dir, ptype: u8) -> Vec<Packet> {
        self.log()
            .into_iter()
            .filter(|p| {
                p.conn == conn
                    && p.is(dir, ptype)
                    && matches!(p.fate, Fate::Forwarded | Fate::CutAfter)
            })
            .collect()
    }

    /// Wait up to `secs` for a logged packet or event matching `wanted` and
    /// return it; panics naming `what` and the log otherwise.
    pub async fn wait_for(
        &self,
        secs: u64,
        what: &str,
        wanted: impl Fn(&Packet) -> bool,
    ) -> Packet {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(secs);
        loop {
            if let Some(p) = self.log().into_iter().find(&wanted) {
                return p;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!(
                    "{what}: not seen within {secs}s; relay log:\n{}",
                    self.dump()
                );
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    }

    /// The log, one line per entry, for a failure message.
    pub fn dump(&self) -> String {
        self.log()
            .iter()
            .map(|p| {
                format!(
                    "conn{} {:?} type={} pkid={:?} dup={} qos={} topic={:?} expiry={:?} \
                     clean_start={:?} session_present={:?} {:?}",
                    p.conn,
                    p.dir,
                    p.ptype,
                    p.pkid,
                    p.dup,
                    p.qos,
                    p.topic,
                    p.expiry,
                    p.clean_start,
                    p.session_present,
                    p.fate
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    }
}

/// What the test steers one connection with.
struct Controls {
    sever: watch::Receiver<bool>,
    inject: mpsc::UnboundedReceiver<Vec<u8>>,
}

/// Terminate TLS toward the client, open TLS toward the broker, and pump.
async fn relay_connection(
    state: Arc<Mutex<State>>,
    conn: usize,
    inbound: TcpStream,
    broker_port: u16,
    acceptor: TlsAcceptor,
    connector: TlsConnector,
    controls: Controls,
) {
    // A client that gives up during the handshake ends the connection.
    let Ok(client) = acceptor.accept(inbound).await else {
        state
            .lock()
            .unwrap()
            .log
            .push(Packet::event(conn, Dir::ClientToBroker, Fate::Ended));
        return;
    };
    let outbound = TcpStream::connect(("127.0.0.1", broker_port))
        .await
        .expect("PacketRelay: connect to the broker");
    let broker = connector
        .connect(
            ServerName::IpAddress(std::net::Ipv4Addr::LOCALHOST.into()),
            outbound,
        )
        .await
        .expect("PacketRelay: TLS handshake with the broker");
    pump(state, conn, client, broker, controls).await;
}

async fn pump<C, B>(state: Arc<Mutex<State>>, conn: usize, client: C, broker: B, controls: Controls)
where
    C: AsyncRead + AsyncWrite + Unpin,
    B: AsyncRead + AsyncWrite + Unpin,
{
    let Controls {
        mut sever,
        mut inject,
    } = controls;
    let (mut cr, mut cw) = tokio::io::split(client);
    let (mut br, mut bw) = tokio::io::split(broker);
    let mut cbuf = Vec::new();
    let mut bbuf = Vec::new();
    let mut ctmp = [0u8; 8192];
    let mut btmp = [0u8; 8192];
    let ended = |dir| {
        state
            .lock()
            .unwrap()
            .log
            .push(Packet::event(conn, dir, Fate::Ended));
    };
    loop {
        tokio::select! {
            biased;
            _ = sever.changed() => {
                state
                    .lock()
                    .unwrap()
                    .log
                    .push(Packet::event(conn, Dir::ClientToBroker, Fate::Severed));
                return;
            }
            Some(bytes) = inject.recv() => {
                let mut packet = describe(conn, Dir::BrokerToClient, bytes.clone());
                packet.fate = Fate::Injected;
                state.lock().unwrap().log.push(packet);
                if cw.write_all(&bytes).await.is_err() || cw.flush().await.is_err() {
                    return ended(Dir::BrokerToClient);
                }
            }
            r = cr.read(&mut ctmp) => match r {
                Ok(0) | Err(_) => return ended(Dir::ClientToBroker),
                Ok(n) => {
                    cbuf.extend_from_slice(&ctmp[..n]);
                    if !forward(&state, conn, Dir::ClientToBroker, &mut cbuf, &mut bw).await {
                        return;
                    }
                }
            },
            r = br.read(&mut btmp) => match r {
                Ok(0) | Err(_) => return ended(Dir::BrokerToClient),
                Ok(n) => {
                    bbuf.extend_from_slice(&btmp[..n]);
                    if !forward(&state, conn, Dir::BrokerToClient, &mut bbuf, &mut cw).await {
                        return;
                    }
                }
            },
        }
    }
}

/// Forward every complete packet in `buf` to `out`, applying the armed cut and
/// the drops. Returns false once the connection is to be torn down.
async fn forward<O: AsyncWrite + Unpin>(
    state: &Mutex<State>,
    conn: usize,
    dir: Dir,
    buf: &mut Vec<u8>,
    out: &mut O,
) -> bool {
    while let Some(total) = frame(buf) {
        let mut bytes: Vec<u8> = buf.drain(..total).collect();
        let fate = {
            let mut s = state.lock().unwrap();
            let mut packet = describe(conn, dir, bytes.clone());
            let cut = s.cut.filter(|c| packet.is(c.dir, c.ptype));
            packet.fate = match cut {
                Some(c) => {
                    s.cut = None;
                    if c.after {
                        Fate::CutAfter
                    } else {
                        Fate::CutBefore
                    }
                }
                None if s.drops.contains(&(dir, packet.ptype)) => Fate::Dropped,
                None => Fate::Forwarded,
            };
            if packet.is(Dir::ClientToBroker, SUBSCRIBE) {
                let positions = refused_positions(&bytes, &s.refused);
                if !positions.is_empty() {
                    let pkid = packet.pkid.expect("a SUBSCRIBE's identifier");
                    s.refusing.push((conn, pkid, positions));
                }
            }
            if packet.is(Dir::BrokerToClient, SUBACK)
                && packet.fate == Fate::Forwarded
                && let Some(at) = s
                    .refusing
                    .iter()
                    .position(|(c, pkid, _)| *c == conn && Some(*pkid) == packet.pkid)
            {
                let (_, _, positions) = s.refusing.remove(at);
                refuse_in_suback(&mut bytes, &positions);
                packet.bytes = bytes.clone();
                packet.fate = Fate::Refused;
            }
            let fate = packet.fate;
            s.log.push(packet);
            fate
        };
        match fate {
            Fate::Dropped => continue,
            Fate::CutBefore => return false,
            _ => {}
        }
        if out.write_all(&bytes).await.is_err() || out.flush().await.is_err() {
            return false;
        }
        if fate == Fate::CutAfter {
            return false;
        }
    }
    true
}

/// The length of the first complete packet in `buf`, if one is there.
fn frame(buf: &[u8]) -> Option<usize> {
    let mut len = 0usize;
    let mut mult = 1usize;
    for i in 1..5 {
        let b = *buf.get(i)?;
        len += usize::from(b & 0x7f) * mult;
        if b & 0x80 == 0 {
            let total = 1 + i + len;
            return (buf.len() >= total).then_some(total);
        }
        mult *= 128;
    }
    panic!(
        "PacketRelay: malformed remaining length in {:02x?}",
        &buf[..5]
    );
}

/// An MQTT variable byte integer at the start of `buf`: (value, bytes used).
fn varint(buf: &[u8]) -> (usize, usize) {
    let mut value = 0usize;
    let mut mult = 1usize;
    for (i, b) in buf.iter().enumerate().take(4) {
        value += usize::from(b & 0x7f) * mult;
        if b & 0x80 == 0 {
            return (value, i + 1);
        }
        mult *= 128;
    }
    panic!("PacketRelay: malformed variable byte integer {buf:02x?}");
}

fn be16(b: &[u8]) -> u16 {
    u16::from_be_bytes([b[0], b[1]])
}

/// The positions, among the filters of the framed SUBSCRIBE `bytes`, of
/// those in `refused`.
fn refused_positions(bytes: &[u8], refused: &[String]) -> Vec<usize> {
    let (_, used) = varint(&bytes[1..]);
    let body = &bytes[1 + used..];
    let (props_len, used) = varint(&body[2..]);
    let mut at = 2 + used + props_len;
    let mut positions = Vec::new();
    let mut position = 0;
    while at < body.len() {
        let len = usize::from(be16(&body[at..]));
        let filter = String::from_utf8_lossy(&body[at + 2..at + 2 + len]);
        if refused.iter().any(|r| *r == filter) {
            positions.push(position);
        }
        // The filter, then its one byte of options.
        at += 2 + len + 1;
        position += 1;
    }
    positions
}

/// Set the reason codes at `positions` of the framed SUBACK `bytes` to Not
/// authorized.
fn refuse_in_suback(bytes: &mut [u8], positions: &[usize]) {
    let (_, used) = varint(&bytes[1..]);
    let body = 1 + used;
    let (props_len, used) = varint(&bytes[body + 2..]);
    let codes = body + 2 + used + props_len;
    for position in positions {
        bytes[codes + position] = NOT_AUTHORIZED;
    }
}

/// Parse what the rows read from one framed packet.
fn describe(conn: usize, dir: Dir, bytes: Vec<u8>) -> Packet {
    let ptype = bytes[0] >> 4;
    let (_, used) = varint(&bytes[1..]);
    let body = &bytes[1 + used..];
    let mut packet = Packet::event(conn, dir, Fate::Forwarded);
    packet.ptype = ptype;
    match ptype {
        CONNECT => {
            // Protocol name (2 + 4 bytes), then the level, then the flags.
            packet.clean_start = Some(body[7] & 0x02 != 0);
        }
        CONNACK => {
            packet.session_present = Some(body[0] & 0x01 != 0);
        }
        PUBLISH => {
            packet.qos = (bytes[0] >> 1) & 0x03;
            packet.dup = bytes[0] & 0x08 != 0;
            let topic_len = usize::from(be16(body));
            packet.topic = Some(String::from_utf8_lossy(&body[2..2 + topic_len]).into_owned());
            let mut at = 2 + topic_len;
            if packet.qos > 0 {
                packet.pkid = Some(be16(&body[at..]));
                at += 2;
            }
            let (props_len, used) = varint(&body[at..]);
            at += used;
            packet.expiry = message_expiry(&body[at..at + props_len]);
        }
        PUBACK..=SUBACK => {
            packet.pkid = Some(be16(body));
        }
        // A reason code alone, or nothing, carries no properties.
        DISCONNECT if body.len() > 1 => {
            let (props_len, used) = varint(&body[1..]);
            let at = 1 + used;
            packet.session_expiry = disconnect_session_expiry(&body[at..at + props_len]);
        }
        _ => {}
    }
    packet.bytes = bytes;
    packet
}

/// The Session Expiry Interval among a DISCONNECT's properties.
fn disconnect_session_expiry(mut props: &[u8]) -> Option<u32> {
    let mut expiry = None;
    while let Some((&id, rest)) = props.split_first() {
        let skip = match id {
            0x11 => {
                expiry = Some(u32::from_be_bytes([rest[0], rest[1], rest[2], rest[3]]));
                4
            }
            // Server Reference, Reason String.
            0x1c | 0x1f => 2 + usize::from(be16(rest)),
            // User Property: two strings.
            0x26 => {
                let key = 2 + usize::from(be16(rest));
                key + 2 + usize::from(be16(&rest[key..]))
            }
            other => panic!("PacketRelay: unknown DISCONNECT property {other:#04x}"),
        };
        props = &rest[skip..];
    }
    expiry
}

/// The Message Expiry Interval among a PUBLISH's properties.
fn message_expiry(mut props: &[u8]) -> Option<u32> {
    let mut expiry = None;
    while let Some((&id, rest)) = props.split_first() {
        let skip = match id {
            // Payload Format Indicator.
            0x01 => 1,
            0x02 => {
                expiry = Some(u32::from_be_bytes([rest[0], rest[1], rest[2], rest[3]]));
                4
            }
            // Content Type, Response Topic, Correlation Data.
            0x03 | 0x08 | 0x09 => 2 + usize::from(be16(rest)),
            // Subscription Identifier.
            0x0b => varint(rest).1,
            // Topic Alias.
            0x23 => 2,
            // User Property: two strings.
            0x26 => {
                let key = 2 + usize::from(be16(rest));
                key + 2 + usize::from(be16(&rest[key..]))
            }
            other => panic!("PacketRelay: unknown PUBLISH property {other:#04x}"),
        };
        props = &rest[skip..];
    }
    expiry
}
