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
/// The general section applies to anonymous clients only, so the authenticated
/// user of the password-authentication template needs its own section; that
/// section is a no-op for the anonymous templates, which never log anyone in.
pub const DEFAULT_ACL: &str = "\
topic readwrite brenn/itest/#

user brenn-itest
topic readwrite brenn/itest/#
";

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
