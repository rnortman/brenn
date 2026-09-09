//! One `mosquitto` process per test, on an ephemeral loopback port.
//!
//! Per-test brokers rather than a process singleton: each
//! `#[tokio::test(flavor = "multi_thread")]` creates its own runtime, and a
//! `tokio::sync::Mutex` cannot be shared across runtimes. Per-test brokers also
//! make each test independent with no teardown coordination, and `persistence
//! false` means no state bleeds between them.
//!
//! `mosquitto` is the one non-hermetic dependency in the build. A suite that
//! uses this harness sets its own gate so a missing broker fails the test
//! instead of skipping it.

use std::io::Read as _;
use std::net::TcpListener;
use std::os::unix::process::CommandExt as _;
use std::process::{Child, Command, Stdio};

use tempfile::TempDir;

use super::certs;

/// The plain TLS-1.2+ broker configuration, anonymous, ACL-gated.
///
/// A string constant rather than a data file so a crate above this one needs no
/// runfiles plumbing to start a broker. The variants that need a different
/// listener — TLS 1.3 only, password authentication — pass their own template
/// to [`BrokerHarness::start_with`].
pub const DEFAULT_CONF_TEMPLATE: &str = "\
listener __PORT__ 127.0.0.1
protocol mqtt
cafile   __CA_PEM__
certfile __SERVER_CRT__
keyfile  __SERVER_KEY__
require_certificate false
allow_anonymous true
acl_file __ACL__
log_dest file __LOG__
log_type all
persistence false
";

/// The ACL every template references as `__ACL__`.
///
/// The general section applies to anonymous clients only, so each authenticated
/// user of the password-authentication template needs its own section; those
/// sections are a no-op for the anonymous templates, which never log anyone in.
pub const DEFAULT_ACL: &str = "\
topic readwrite brenn/itest/#

user brenn-itest
topic readwrite brenn/itest/#

user brenn-itest-rotated
topic readwrite brenn/itest/#
";

/// The password-authentication listener: anonymous clients are rejected and
/// credentials come from `__PASSWD__`.
///
/// A constant beside the default one, for the same reason: a crate above this
/// one drives a credential change through a real broker and cannot reach a data
/// file in another package's runfiles.
pub const AUTH_CONF_TEMPLATE: &str = "\
listener __PORT__ 127.0.0.1
protocol mqtt
cafile   __CA_PEM__
certfile __SERVER_CRT__
keyfile  __SERVER_KEY__
require_certificate false
allow_anonymous false
password_file __PASSWD__
acl_file __ACL__
log_dest file __LOG__
log_type all
persistence false
";

/// The password file [`AUTH_CONF_TEMPLATE`] reads, holding the two accounts
/// [`AUTH_CREDENTIALS`] and [`ROTATED_CREDENTIALS`] name.
///
/// Two accounts because a mosquitto password file holds one entry per user, so
/// a credential *rotation* a live broker authenticates on both sides of needs a
/// second account: the username moves with the password. Checked in as the
/// `$7$` sha512-pbkdf2 hash `mosquitto_passwd` writes, so no test needs that
/// binary at runtime. A mosquitto too old to read the format fails the auth
/// control case at connect; regenerate with
/// `mosquitto_passwd -c -b <file> <user> <password>` per account.
pub const AUTH_PASSWD: &str = "\
brenn-itest:$7$101$iebdWDoklc/lomy/$D6Z40ukuECDWm6zM+OL3bbBA0PnwVcJdi4kQinefi6q87obwcb2/Hv2kfU9x6LphTSUcYVH2umdgMvV2/H0/Qw==
brenn-itest-rotated:$7$101$/VrRzaON6/KYjAhm$9X3+QV4Z4u+LT5jt71wL0B3dZy8PwZqJiXqVBQGIj2HeD5n5/Ags+9YUsTa3y8MGjWL3F1JM3dAQOVsbkr7e2Q==
";

/// The account a credentialed session starts on: username, then password.
pub const AUTH_CREDENTIALS: (&str, &str) = ("brenn-itest", "brenn-itest-password");

/// The account a credential rotation moves to.
pub const ROTATED_CREDENTIALS: (&str, &str) =
    ("brenn-itest-rotated", "brenn-itest-rotated-password");

/// How many ephemeral ports to try before giving up.
///
/// A port is chosen by binding and dropping a listener, so another process can
/// claim it before `mosquitto` binds. Retrying is the mitigation.
const MAX_BIND_RETRIES: usize = 5;

/// Owns the `mosquitto` child process and its temp directory.
///
/// Dropped via `Drop`: kills the process and waits for it to exit.
/// `prctl(PR_SET_PDEATHSIG, SIGTERM)` on spawn ensures the broker is also
/// killed if the test runner process dies abnormally (SIGKILL, Ctrl-C, panic).
pub struct BrokerHarness {
    pub port: u16,
    child: Option<Child>,
    /// Kept alive so broker log / config files outlive the process.
    _tempdir: TempDir,
    /// Absolute path to the mosquitto log file for failure diagnostics.
    log_path: std::path::PathBuf,
}

impl BrokerHarness {
    /// Loopback host the broker binds to.
    pub const HOST: &'static str = "127.0.0.1";

    /// Spawn `mosquitto` with the default TLS 1.2+ configuration and wait for it
    /// to become ready.
    ///
    /// # Panics
    ///
    /// - `mosquitto` binary not found.
    /// - TCP-connect readiness poll exceeds 2 seconds on every attempt.
    pub fn start() -> Self {
        Self::start_with(DEFAULT_CONF_TEMPLATE, &[("acl", DEFAULT_ACL.as_bytes())])
    }

    /// Spawn `mosquitto` on [`AUTH_CONF_TEMPLATE`] with [`AUTH_PASSWD`],
    /// rejecting anonymous clients.
    ///
    /// # Panics
    ///
    /// Same as [`BrokerHarness::start`].
    pub fn start_auth() -> Self {
        Self::start_with(
            AUTH_CONF_TEMPLATE,
            &[
                ("acl", DEFAULT_ACL.as_bytes()),
                ("passwd", AUTH_PASSWD.as_bytes()),
            ],
        )
    }

    /// Spawn `mosquitto` on `template`, with `extras` written beside it.
    ///
    /// Each `(name, bytes)` pair is written into the broker's temp directory as
    /// `name` at mode 0600 and substituted into the template wherever
    /// `__NAME__` appears — so a template naming `acl_file __ACL__` is served by
    /// an extra called `acl`. The TLS material and `__PORT__` / `__LOG__` are
    /// always substituted.
    ///
    /// # Panics
    ///
    /// Same as [`BrokerHarness::start`], plus a template whose substitutions
    /// name a file no extra provides (mosquitto then refuses to start, which
    /// this reports with the log tail).
    pub fn start_with(template: &str, extras: &[(&str, &[u8])]) -> Self {
        let tempdir = TempDir::new().expect("failed to create tempdir for mosquitto");
        let tmp = tempdir.path();
        let log_path = tmp.join("mosquitto.log");

        // TLS assets are generated per-run (no key material in the repo). Write
        // the shared CA + server cert/key so mosquitto's cafile/certfile/keyfile
        // resolve to real files.
        for (name, contents) in &[
            ("ca.pem", certs::ca_pem()),
            ("server.crt", certs::server_cert_pem()),
            ("server.key", certs::server_key_pem()),
        ] {
            std::fs::write(tmp.join(name), contents)
                .unwrap_or_else(|e| panic!("failed to write generated {name} into tempdir: {e}"));
        }

        // mosquitto warns on (and some versions reject) a world-readable
        // password file; 0600 on every extra makes the question moot.
        let mut substitutions: Vec<(String, String)> = vec![
            ("__CA_PEM__".to_string(), path_str(tmp, "ca.pem")),
            ("__SERVER_CRT__".to_string(), path_str(tmp, "server.crt")),
            ("__SERVER_KEY__".to_string(), path_str(tmp, "server.key")),
            ("__LOG__".to_string(), log_path.display().to_string()),
        ];
        for (name, bytes) in extras {
            let path = tmp.join(name);
            std::fs::write(&path, bytes)
                .unwrap_or_else(|e| panic!("failed to write broker asset {name}: {e}"));
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
                .unwrap_or_else(|e| panic!("failed to chmod broker asset {name}: {e}"));
            substitutions.push((
                format!("__{}__", name.to_ascii_uppercase()),
                path.display().to_string(),
            ));
        }

        let bin = std::env::var("BRENN_MOSQUITTO_BIN").unwrap_or_else(|_| "mosquitto".to_string());
        let mut last_log_tail = String::new();

        for attempt in 1..=MAX_BIND_RETRIES {
            let port = ephemeral_port();
            let mut config_str = template.replace("__PORT__", &port.to_string());
            for (marker, value) in &substitutions {
                config_str = config_str.replace(marker.as_str(), value);
            }
            let config_path = tmp.join("mosquitto.conf");
            std::fs::write(&config_path, &config_str).expect("failed to write mosquitto.conf");

            let mut cmd = Command::new(&bin);
            cmd.arg("-c")
                .arg(&config_path)
                .stdout(Stdio::null())
                .stderr(Stdio::null());

            // SAFETY: prctl is a simple syscall with no allocator interaction.
            // Linux-only, which this project is.
            unsafe {
                cmd.pre_exec(|| {
                    libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM, 0, 0, 0);
                    Ok(())
                });
            }

            let mut child = cmd.spawn().unwrap_or_else(|e| {
                panic!(
                    "mosquitto binary not found in PATH; install mosquitto or set \
                     BRENN_MOSQUITTO_BIN=/path/to/mosquitto. Tried: {bin:?}. Error: {e}"
                )
            });
            eprintln!("[BrokerHarness] mosquitto spawned on port {port} (attempt {attempt})");

            match Self::await_ready(&mut child, port, attempt, &log_path) {
                Ready::Bound => {
                    eprintln!("[BrokerHarness] mosquitto ready on port {port}");
                    return Self {
                        port,
                        child: Some(child),
                        _tempdir: tempdir,
                        log_path,
                    };
                }
                Ready::BindCollision(tail) => last_log_tail = tail,
            }
        }

        panic!(
            "BrokerHarness: failed to start mosquitto after {MAX_BIND_RETRIES} attempts; \
             repeated ephemeral-port collisions. Last log tail:\n{last_log_tail}"
        );
    }

    /// Poll TCP-connect readiness every 25ms, capped at 2s, watching for an
    /// early exit.
    ///
    /// A bind collision is the one retryable exit: another process took the port
    /// between this harness dropping its probe listener and mosquitto binding.
    /// Every other early exit — a config error, unreadable TLS material — panics
    /// here rather than being retried into a confusing exhaustion message.
    fn await_ready(
        child: &mut Child,
        port: u16,
        attempt: usize,
        log_path: &std::path::Path,
    ) -> Ready {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        loop {
            if std::net::TcpStream::connect(format!("127.0.0.1:{port}")).is_ok() {
                return Ready::Bound;
            }
            match child.try_wait() {
                Ok(Some(status)) => {
                    let tail = read_tail_4k(log_path);
                    if tail.contains("Address already in use")
                        || (tail.contains("Error:") && tail.contains("Unable to start"))
                    {
                        eprintln!(
                            "[BrokerHarness] attempt {attempt}: mosquitto exited (bind collision \
                             on port {port}); retrying with a new port. Log tail:\n{tail}"
                        );
                        return Ready::BindCollision(tail);
                    }
                    panic!(
                        "mosquitto exited with status {status} before binding port {port} \
                         (attempt {attempt}; not a bind collision)\n\
                         Log ({}):\n{tail}",
                        log_path.display()
                    );
                }
                Ok(None) => {}
                Err(e) => {
                    panic!("try_wait failed for mosquitto (attempt {attempt}, port {port}): {e}")
                }
            }
            if std::time::Instant::now() > deadline {
                let tail = read_tail_4k(log_path);
                let _ = child.kill();
                let _ = child.wait();
                panic!(
                    "mosquitto readiness timeout on port {port} (attempt {attempt})\n\
                     Log ({}):\n{tail}",
                    log_path.display()
                );
            }
            std::thread::sleep(std::time::Duration::from_millis(25));
        }
    }

    /// Kill the broker and wait for exit. Idempotent.
    pub fn stop(&mut self) {
        if let Some(ref mut child) = self.child {
            // kill() fails with ESRCH if the process already exited — benign.
            // wait() failure is also benign in teardown; we just need the zombie
            // reaped.
            let _ = child.kill();
            let _ = child.wait();
        }
        self.child = None;
        eprintln!("[BrokerHarness] mosquitto stopped");
    }

    /// The tail of the broker's own log, for a test that wants it in a failure
    /// message of its own.
    pub fn log_tail(&self) -> String {
        read_tail_4k(&self.log_path)
    }

    /// The broker log up to its last complete line, read fresh.
    ///
    /// Content matching needs the whole log — the 4 KiB tail form is for
    /// failure messages and can miss lines that scrolled off. What it does not
    /// need is the fragment at the end: `mosquitto` is appending while this
    /// reads, so a snapshot can stop inside a line, and a matcher keyed on a
    /// line's ending would answer about a filter or a topic that is a
    /// truncation of the one being written. Everything past the last newline is
    /// dropped, so every matcher sees whole lines only.
    ///
    /// # Panics
    ///
    /// If the file cannot be read. The harness wrote its path into the config
    /// it started `mosquitto` on, so a missing log is a harness bug.
    fn log(&self) -> String {
        let raw = std::fs::read_to_string(&self.log_path).unwrap_or_else(|e| {
            panic!(
                "failed to read the broker log at {}: {e}",
                self.log_path.display()
            )
        });
        complete_lines(&raw).to_string()
    }

    /// The length of the current [`log`](Self::log) snapshot, to hand a later
    /// [`wait_for_log`](Self::wait_for_log) so it reads only what the broker
    /// records after this instant.
    ///
    /// Taken before the action under test, this is what makes a broker-side
    /// observation say "the broker did this *now*" rather than "at some point
    /// since it started". The offset is line-aligned, because the snapshot ends
    /// at a newline.
    pub fn log_len(&self) -> usize {
        self.log().len()
    }

    /// Poll the broker's log from `since` until `seen` accepts what it holds,
    /// then return that window. Panics with `msg` and the log tail past
    /// `timeout_secs`.
    ///
    /// `since` is a [`log_len`](Self::log_len) taken before the action under
    /// test, or `0` for the whole log. A wait on the whole log answers "the
    /// broker recorded this at some point since it started", which a fixture
    /// that acts twice, or a case reusing one broker across two reloads, can
    /// satisfy with a line the action under test did not write.
    ///
    /// What this waits on is a fact the broker has recorded about *itself*, so
    /// it is the counterpart, on the broker's side of the wire, of a wait on
    /// what brenn's own session state says. A test asserting that the broker
    /// acted on a packet brenn sent has no other observation available when the
    /// packet's acknowledgement is not attributed in-process.
    ///
    /// Needs `log_type all` in the broker's configuration: at lower levels
    /// mosquitto records neither the packets it received nor the ones it sent.
    /// A broker started without it turns this wait into a timeout.
    ///
    /// Each tick reads the whole file and each matcher rescans it from the
    /// start, so one tick costs O(log size). Fine for a per-test broker whose
    /// log is tens of KiB; a broker left running across many cases, or one on a
    /// high-volume topic, wants a matcher fed only the bytes since the last
    /// read.
    pub async fn wait_for_log(
        &self,
        since: usize,
        timeout_secs: u64,
        msg: &str,
        seen: impl Fn(&str) -> bool,
    ) -> String {
        let seen = &seen;
        super::poll::poll_until(
            timeout_secs,
            || async move {
                let snapshot = self.log();
                let window = snapshot.get(since..).unwrap_or_default().to_string();
                seen(&window).then_some(window)
            },
            || async move { format!("{msg}\nBroker log tail:\n{}", self.log_tail()) },
        )
        .await
    }
}

/// Whether `log` records the broker receiving an UNSUBSCRIBE from `client_id`
/// for exactly `topic_filter` — [`session_client_id`](super::client::session_client_id)
/// for a brenn session.
///
/// At `log_type all` mosquitto brackets the packet it received with
/// `Received UNSUBSCRIBE from <id>` and `Sending UNSUBACK to <id>`, and lists
/// the filters between them one per line, tab-indented. A filter line is read
/// only inside a block opened by this client's id, so one session's withdrawal
/// never reads as another's: a broker with two sessions withdrawing filters is
/// what these suites run, and the id is the only thing that tells them apart.
/// A SUBSCRIBE's filter lines trail a `(QoS n)` and are outside the block
/// besides, so a subscription does not read as its own withdrawal, and
/// requiring the tab immediately before the filter keeps out a filter that is
/// a suffix or a prefix of another.
///
/// The three broker strings it names are pinned against a real broker by
/// `the_matchers_read_a_live_brokers_wording` in `brenn-mqtt`'s integration
/// suite, which is what fails when a `mosquitto` release re-spells one of them
/// rather than this answering a constant.
pub fn log_records_unsubscribe(log: &str, client_id: &str, topic_filter: &str) -> bool {
    let opening = format!("Received UNSUBSCRIBE from {client_id}");
    let closing = format!("Sending UNSUBACK to {client_id}");
    let listed = format!("\t{topic_filter}");
    let mut inside = false;
    for line in log.lines() {
        if line.contains("Received UNSUBSCRIBE from") {
            inside = line.ends_with(&opening);
            continue;
        }
        if !inside {
            continue;
        }
        if line.ends_with(&listed) {
            return true;
        }
        if line.ends_with(&closing) {
            inside = false;
        }
    }
    false
}

/// Whether `log` records the broker receiving an orderly DISCONNECT from
/// `client_id` — [`session_client_id`](super::client::session_client_id) for a
/// brenn session.
///
/// The sibling of [`log_records_unsubscribe`] for the other packet a stopping
/// supervisor sends, and the only account available of the fact: brenn drops
/// the session's state as it stops, so nothing in-process afterwards says
/// whether the DISCONNECT reached the broker. A session the broker lost instead
/// — killed, or timed out — is logged as a socket error rather than as this
/// line, so a supervisor that never drained its DISCONNECT does not read as one
/// that did.
///
/// The id is matched to the end of the line, so one client id that is a prefix
/// of another is not it. The wording is pinned against a real broker by
/// `the_disconnect_matcher_reads_a_live_brokers_wording` in `brenn-mqtt`'s
/// integration suite.
pub fn log_records_disconnect(log: &str, client_id: &str) -> bool {
    let received = format!("Received DISCONNECT from {client_id}");
    log.lines().any(|line| line.ends_with(&received))
}

/// Whether `log` records an orderly DISCONNECT from `client_id` and records it
/// *before* any session connects under that same id.
///
/// The claim a restart makes at the broker: the predecessor's DISCONNECT is
/// drained before the successor's CONNECT, because both carry one MQTT client
/// id and a broker that sees the CONNECT first performs a takeover — it kicks
/// one of the two sessions and the persistent session's queued QoS-1 messages
/// go with it. The supervisor's own reconnect loop then heals the connection,
/// so every in-process health check still passes: the order is the only place
/// the difference shows.
///
/// Read over a window opened before the restart, so the first connect line in
/// it is the successor's. Both wordings are pinned against a real broker by
/// `the_disconnect_matcher_reads_a_live_brokers_wording` in `brenn-mqtt`'s
/// integration suite.
pub fn log_records_disconnect_before_reconnect(log: &str, client_id: &str) -> bool {
    let disconnected = format!("Received DISCONNECT from {client_id}");
    let connected = format!(" as {client_id} (");
    for line in log.lines() {
        if line.contains(&connected) {
            return false;
        }
        if line.ends_with(&disconnected) {
            return true;
        }
    }
    false
}

/// Whether `log` records the broker sending a PUBLISH on `topic` to
/// `subscriber` — the MQTT client id of the session that should have received
/// it, [`session_client_id`](super::client::session_client_id) for a brenn one.
///
/// The inbound counterpart — `Received PUBLISH from` — is the publisher's own
/// line and does not match, so a test's publish on a topic never reads as the
/// broker having delivered it to anyone. The topic is matched with mosquitto's
/// own quotes around it, so one topic that is a prefix of another is not it, and
/// the client id is matched up to the bracket that opens the packet's fields, so
/// one client id that is a prefix of another is not it either.
pub fn log_records_publish_to_subscriber(log: &str, subscriber: &str, topic: &str) -> bool {
    let quoted = format!("'{topic}'");
    let sending = format!("Sending PUBLISH to {subscriber} (");
    log.lines()
        .any(|line| line.contains(&sending) && line.contains(&quoted))
}

/// `log` up to and including its last newline — the lines `mosquitto` has
/// finished writing. Empty when nothing complete has been written yet.
fn complete_lines(log: &str) -> &str {
    match log.rfind('\n') {
        Some(last) => &log[..=last],
        None => "",
    }
}

impl Drop for BrokerHarness {
    fn drop(&mut self) {
        self.stop();
    }
}

/// The outcome of one readiness poll: bound, or exited on a port that was taken.
enum Ready {
    Bound,
    BindCollision(String),
}

fn path_str(dir: &std::path::Path, name: &str) -> String {
    dir.join(name).display().to_string()
}

/// Bind an ephemeral port and drop the listener, reporting the number.
///
/// The window between the drop and mosquitto's own bind is what
/// [`MAX_BIND_RETRIES`] covers.
fn ephemeral_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("failed to bind ephemeral port");
    let port = listener.local_addr().expect("local_addr").port();
    drop(listener);
    port
}

fn read_tail_4k(path: &std::path::Path) -> String {
    let mut file = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(_) => return "<log file not found>".to_string(),
    };
    let len = file.metadata().map(|m| m.len()).unwrap_or(0);
    if len > 4096 {
        use std::io::Seek;
        // seek failure means we read from the beginning — still useful, just
        // not the tail.
        let _ = file.seek(std::io::SeekFrom::End(-4096));
    }
    let mut buf = String::new();
    let _ = file.read_to_string(&mut buf); // best-effort; empty on I/O error
    buf
}

#[cfg(test)]
mod tests {
    use super::{
        complete_lines, log_records_disconnect_before_reconnect, log_records_publish_to_subscriber,
        log_records_unsubscribe,
    };

    /// One `log_type all` transcript in mosquitto's own shape: a SUBSCRIBE for
    /// three filters, an UNSUBSCRIBE for one of them, a publish delivered on one
    /// of the remaining two, and a second session withdrawing a filter of its
    /// own.
    ///
    /// A literal rather than a captured file so the matchers can be read
    /// against a fixed shape with no broker running. What it pins is the
    /// matchers *against this shape*, which is mosquitto's wording as of the
    /// version measured — both sides of the pairing are written here, so a
    /// broker release that renames a line leaves these cases green. The case
    /// that fails on such a rename is
    /// `the_matchers_read_a_live_brokers_wording` in
    /// `tests/mqtt_integration.rs`, which puts the same two matchers on a log an
    /// actual broker wrote.
    const TRANSCRIPT: &str = "\
1700000000: New client connected from 10.0.0.1:36992 as alice (p2, c1, k60).
1700000000: Received SUBSCRIBE from alice
1700000000: \tbrenn/itest/one (QoS 1)
1700000000: alice 1 brenn/itest/one
1700000000: \tbrenn/itest/one/deep (QoS 1)
1700000000: alice 1 brenn/itest/one/deep
1700000000: \tbrenn/itest/two (QoS 1)
1700000000: alice 1 brenn/itest/two
1700000000: Sending SUBACK to alice
1700000001: Received UNSUBSCRIBE from alice
1700000001: \tbrenn/itest/one
1700000001: alice brenn/itest/one
1700000001: Sending UNSUBACK to alice
1700000002: Received PUBLISH from bob (d0, q1, r0, m1, 'brenn/itest/two', ... (4 bytes))
1700000002: Sending PUBLISH to alice (d0, q1, r0, m1, 'brenn/itest/two', ... (4 bytes))
1700000002: Sending PUBACK to bob (m1, rc0)
1700000003: Received PUBLISH from bob (d0, q1, r0, m2, 'brenn/itest/one', ... (4 bytes))
1700000004: New client connected from 10.0.0.1:36994 as carol (p2, c1, k60).
1700000004: Received SUBSCRIBE from carol
1700000004: \tbrenn/itest/other (QoS 1)
1700000004: carol 1 brenn/itest/other
1700000004: Sending SUBACK to carol
1700000005: Received UNSUBSCRIBE from carol
1700000005: \tbrenn/itest/other
1700000005: carol brenn/itest/other
1700000005: Sending UNSUBACK to carol
";

    /// The restart shape: one session under `brenn:ha` disconnects and another
    /// connects under the same id, in that order, with an unrelated session's
    /// traffic interleaved.
    const RESTART: &str = "\
1700000000: Received DISCONNECT from brenn:ha
1700000000: Client brenn:ha disconnected.
1700000001: New client connected from 10.0.0.1:36992 as brenn:ha (p2, c0, k30).
1700000001: Received SUBSCRIBE from brenn:ha
";

    #[test]
    fn a_disconnect_before_the_successors_connect_is_the_restart_order() {
        assert!(log_records_disconnect_before_reconnect(RESTART, "brenn:ha"));
    }

    #[test]
    fn a_connect_before_the_disconnect_is_not() {
        let takeover = "\
1700000000: New client connected from 10.0.0.1:36992 as brenn:ha (p2, c0, k30).
1700000000: Received DISCONNECT from brenn:ha
";
        assert!(!log_records_disconnect_before_reconnect(
            takeover, "brenn:ha"
        ));
    }

    #[test]
    fn a_window_with_neither_packet_makes_no_claim() {
        assert!(!log_records_disconnect_before_reconnect(
            TRANSCRIPT, "brenn:ha"
        ));
    }

    #[test]
    fn an_unsubscribed_filter_is_found() {
        assert!(log_records_unsubscribe(
            TRANSCRIPT,
            "alice",
            "brenn/itest/one"
        ));
    }

    #[test]
    fn a_filter_only_ever_subscribed_is_not_an_unsubscribe() {
        assert!(!log_records_unsubscribe(
            TRANSCRIPT,
            "alice",
            "brenn/itest/two"
        ));
    }

    #[test]
    fn an_unsubscribe_by_another_client_is_not_this_clients() {
        // `carol` withdrew it; `alice` never held it.
        assert!(log_records_unsubscribe(
            TRANSCRIPT,
            "carol",
            "brenn/itest/other"
        ));
        assert!(!log_records_unsubscribe(
            TRANSCRIPT,
            "alice",
            "brenn/itest/other"
        ));
    }

    #[test]
    fn a_client_id_sharing_a_prefix_with_the_unsubscriber_is_not_found() {
        assert!(!log_records_unsubscribe(
            TRANSCRIPT,
            "ali",
            "brenn/itest/one"
        ));
    }

    #[test]
    fn a_filter_sharing_a_prefix_with_the_unsubscribed_one_is_not_found() {
        // Either direction: the unsubscribed filter extended, and the
        // unsubscribed filter truncated.
        assert!(!log_records_unsubscribe(
            TRANSCRIPT,
            "alice",
            "brenn/itest/one/deep"
        ));
        assert!(!log_records_unsubscribe(TRANSCRIPT, "alice", "brenn/itest"));
    }

    #[test]
    fn a_filter_that_is_a_suffix_of_the_unsubscribed_one_is_not_found() {
        assert!(!log_records_unsubscribe(TRANSCRIPT, "alice", "itest/one"));
    }

    #[test]
    fn a_topic_the_broker_sent_on_is_found() {
        assert!(log_records_publish_to_subscriber(
            TRANSCRIPT,
            "alice",
            "brenn/itest/two"
        ));
    }

    #[test]
    fn a_topic_only_received_from_a_publisher_is_not_a_send() {
        assert!(!log_records_publish_to_subscriber(
            TRANSCRIPT,
            "alice",
            "brenn/itest/one"
        ));
    }

    #[test]
    fn a_send_to_another_subscriber_is_not_a_send_to_this_one() {
        // `bob` is the publisher in the transcript: the broker sent nothing to
        // it, and the topic alone would have matched.
        assert!(!log_records_publish_to_subscriber(
            TRANSCRIPT,
            "bob",
            "brenn/itest/two"
        ));
    }

    #[test]
    fn a_client_id_sharing_a_prefix_with_the_subscriber_is_not_found() {
        assert!(!log_records_publish_to_subscriber(
            TRANSCRIPT,
            "ali",
            "brenn/itest/two"
        ));
    }

    #[test]
    fn a_topic_sharing_a_prefix_with_the_sent_one_is_not_found() {
        assert!(!log_records_publish_to_subscriber(
            TRANSCRIPT,
            "alice",
            "brenn/itest/two/deep"
        ));
        assert!(!log_records_publish_to_subscriber(
            TRANSCRIPT,
            "alice",
            "brenn/itest"
        ));
    }

    #[test]
    fn a_fragment_past_the_last_newline_is_not_a_line() {
        let torn = "1700000001: Received UNSUBSCRIBE from alice\n1700000001: \tbrenn/itest";
        assert_eq!(
            complete_lines(torn),
            "1700000001: Received UNSUBSCRIBE from alice\n"
        );
        assert_eq!(complete_lines("1700000001: Rec"), "");
        // The fragment is a truncation of the filter being written, and a
        // matcher keyed on a line's ending would answer about it.
        assert!(log_records_unsubscribe(torn, "alice", "brenn/itest"));
        assert!(!log_records_unsubscribe(
            complete_lines(torn),
            "alice",
            "brenn/itest"
        ));
    }
}
