//! Integration tests that drive the real `brenn-cli` binary.

use brenn_lib::auth::device::{UNENROLLED_TOKEN_PREFIX, resolve_or_create_device};
use brenn_lib::auth::user::create_user;
use brenn_lib::db::init_db;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::process::{Command, Stdio};

const BIN: &str = env!("CARGO_BIN_EXE_brenn-cli");

struct Run {
    success: bool,
    /// `None` only if the child was killed by a signal.
    code: Option<i32>,
    stdout: String,
    stderr: String,
}

/// Every env-backed argument the CLI declares is removed, so no test inherits
/// the operator's live configuration; the caller adds back exactly what the
/// case needs via `extra_env`.
const CLI_ENV_VARS: &[&str] = &[
    "BRENN_PUSH_SECRET",
    "BRENN_PUSH_SECRET_FILE",
    "BRENN_PUSH_URL",
    "BRENN_PUSH_KEY_ID",
    "BRENN_DB",
];

fn base_command(args: &[&str], extra_env: &[(&str, &str)]) -> Command {
    let mut cmd = Command::new(BIN);
    cmd.args(args);
    for var in CLI_ENV_VARS {
        cmd.env_remove(var);
    }
    for (k, v) in extra_env {
        cmd.env(k, v);
    }
    cmd
}

fn run_cli(args: &[&str], extra_env: &[(&str, &str)]) -> Run {
    let out = base_command(args, extra_env)
        .output()
        .expect("run brenn-cli");
    Run {
        success: out.status.success(),
        code: out.status.code(),
        stdout: String::from_utf8_lossy(&out.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
    }
}

/// Same as `run_cli` but feeds `stdin` to the child, exercising the CLI's
/// stdin message path.
fn run_cli_stdin(args: &[&str], extra_env: &[(&str, &str)], stdin: &str) -> Run {
    let mut child = base_command(args, extra_env)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn brenn-cli");
    child
        .stdin
        .take()
        .expect("child stdin")
        .write_all(stdin.as_bytes())
        .expect("write child stdin");
    let out = child.wait_with_output().expect("wait for brenn-cli");
    Run {
        success: out.status.success(),
        code: out.status.code(),
        stdout: String::from_utf8_lossy(&out.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
    }
}

/// One HTTP request captured by the single-shot test server.
struct Captured {
    request_line: String,
    /// Header names lowercased; values trimmed.
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Captured {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }
}

/// Single-shot HTTP server on an ephemeral loopback port. `url` is the
/// endpoint to point the CLI at; `join()` yields the captured request, or
/// `None` if nothing connected.
struct TestServer {
    url: String,
    handle: std::thread::JoinHandle<Option<Captured>>,
}

impl TestServer {
    fn start(status_line: &'static str, reply_body: &'static str) -> TestServer {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind test server");
        listener
            .set_nonblocking(true)
            .expect("nonblocking listener");
        let addr = listener.local_addr().expect("test server addr");
        let url = format!("http://{addr}/push");
        let handle = std::thread::spawn(move || {
            // Poll for a connection rather than blocking forever: a CLI that exits
            // before connecting must fail the test, not hang it.
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
            let mut stream = loop {
                match listener.accept() {
                    Ok((stream, _)) => break stream,
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        if std::time::Instant::now() >= deadline {
                            return None;
                        }
                        std::thread::sleep(std::time::Duration::from_millis(10));
                    }
                    Err(e) => panic!("accept test connection: {e}"),
                }
            };
            stream.set_nonblocking(false).expect("blocking stream");
            stream
                .set_read_timeout(Some(std::time::Duration::from_secs(10)))
                .expect("set read timeout");

            let mut buf = Vec::new();
            let mut chunk = [0u8; 1024];
            let head_end = loop {
                if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n").map(|p| p + 4) {
                    break pos;
                }
                let n = stream.read(&mut chunk).expect("read request head");
                assert!(n > 0, "connection closed before end of headers");
                buf.extend_from_slice(&chunk[..n]);
            };

            let head = String::from_utf8(buf[..head_end].to_vec()).expect("request head is UTF-8");
            let mut lines = head.split("\r\n");
            let request_line = lines.next().expect("request line").to_string();
            let headers: Vec<(String, String)> = lines
                .filter(|l| !l.is_empty())
                .map(|l| {
                    let (k, v) = l.split_once(':').expect("header has a colon");
                    (k.to_ascii_lowercase(), v.trim().to_string())
                })
                .collect();

            let content_length: usize = headers
                .iter()
                .find(|(k, _)| k == "content-length")
                .map(|(_, v)| v.parse().expect("content-length is a number"))
                .expect("request must carry content-length");
            let mut body = buf[head_end..].to_vec();
            while body.len() < content_length {
                let n = stream.read(&mut chunk).expect("read request body");
                assert!(n > 0, "connection closed before end of body");
                body.extend_from_slice(&chunk[..n]);
            }
            body.truncate(content_length);

            let response = format!(
                "{status_line}\r\ncontent-type: text/plain\r\ncontent-length: {len}\r\nconnection: close\r\n\r\n{reply_body}",
                len = reply_body.len(),
            );
            stream
                .write_all(response.as_bytes())
                .expect("write response");
            stream.flush().expect("flush response");

            Some(Captured {
                request_line,
                headers,
                body,
            })
        });
        TestServer { url, handle }
    }

    fn join(self) -> Option<Captured> {
        self.handle.join().expect("test server thread")
    }
}

fn find_device_line(stdout: &str, id: i64) -> Option<&str> {
    stdout.lines().find(|l| {
        l.split('\t')
            .next()
            .map(|f| f.trim() == id.to_string())
            .unwrap_or(false)
    })
}

/// Fresh on-disk SQLite DB with migrations applied; caller holds `TempDir` to keep it alive.
fn setup_db() -> (PathBuf, tempfile::TempDir) {
    let dir = tempfile::TempDir::new().expect("create tmpdir");
    let path = dir.path().join("test.db");
    let _db = init_db(&path);
    (path, dir)
}

/// Creates a user and device in the DB at `path`; returns the device id.
fn create_device(path: &std::path::Path, username: &str) -> i64 {
    let db = init_db(path);
    let conn = db.blocking_lock();
    let user_id = create_user(&conn, username, "hash");
    let r = resolve_or_create_device(&conn, None, user_id, "Mozilla/5.0 Chrome/125");
    r.id
}

#[test]
fn cli_list_shows_enrolled_and_unenrolled() {
    let (db_path, _dir) = setup_db();

    let dev1 = create_device(&db_path, "alice");
    let dev2 = create_device(&db_path, "bob");

    let unenroll_out = run_cli(
        &[
            "device",
            "--db",
            db_path.to_str().unwrap(),
            "unenroll",
            "--id",
            &dev2.to_string(),
            "--reason",
            "test setup",
        ],
        &[],
    );
    assert!(
        unenroll_out.success,
        "unenroll must succeed; stderr: {}",
        unenroll_out.stderr
    );

    let list_out = run_cli(&["device", "--db", db_path.to_str().unwrap(), "list"], &[]);
    assert!(
        list_out.success,
        "list must succeed; stderr: {}",
        list_out.stderr
    );
    let stdout = list_out.stdout;

    let dev1_line = find_device_line(&stdout, dev1).unwrap_or_else(|| {
        panic!("enrolled device {dev1} must appear in list output; got:\n{stdout}")
    });
    let dev1_last_col = dev1_line.trim().rsplit('\t').next().unwrap_or("");
    assert_eq!(
        dev1_last_col.trim(),
        "-",
        "enrolled device must show '-' for unenrolled_at; line: {dev1_line}"
    );

    let dev2_line = find_device_line(&stdout, dev2).unwrap_or_else(|| {
        panic!("unenrolled device {dev2} must appear in list output; got:\n{stdout}")
    });
    let dev2_last_col = dev2_line.trim().rsplit('\t').next().unwrap_or("");
    assert_ne!(
        dev2_last_col.trim(),
        "-",
        "unenrolled device must show a non-null unenrolled_at timestamp; line: {dev2_line}"
    );
}

#[test]
fn cli_unenroll_emits_confirmation_and_invalidates() {
    let (db_path, _dir) = setup_db();
    let device_id = create_device(&db_path, "alice");

    let out = run_cli(
        &[
            "device",
            "--db",
            db_path.to_str().unwrap(),
            "unenroll",
            "--id",
            &device_id.to_string(),
            "--reason",
            "stolen",
        ],
        &[],
    );

    assert!(out.success, "unenroll must exit 0; stderr: {}", out.stderr);
    let stdout = out.stdout;
    assert!(
        stdout.contains("unenrolled at"),
        "stdout must contain 'unenrolled at'; got: {stdout}"
    );

    let db = init_db(&db_path);
    let conn = db.blocking_lock();
    let (unenrolled_at_ms, token): (Option<i64>, String) = conn
        .query_row(
            "SELECT unenrolled_at, token FROM devices WHERE id = ?1",
            rusqlite::params![device_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("query device row");
    assert!(
        unenrolled_at_ms.is_some(),
        "unenrolled_at must be set in DB after unenroll"
    );
    assert!(
        token.starts_with(UNENROLLED_TOKEN_PREFIX),
        "token must start with sentinel after unenroll; got: {token}"
    );
}

#[test]
fn cli_unenroll_idempotent() {
    let (db_path, _dir) = setup_db();
    let device_id = create_device(&db_path, "alice");
    let id_str = device_id.to_string();
    let db_str = db_path.to_str().unwrap();

    let unenroll_args = [
        "device",
        "--db",
        db_str,
        "unenroll",
        "--id",
        &id_str,
        "--reason",
        "idempotency test",
    ];

    let out1 = run_cli(&unenroll_args, &[]);
    assert!(
        out1.success,
        "first unenroll must succeed; stderr: {}",
        out1.stderr
    );

    let out2 = run_cli(&unenroll_args, &[]);
    assert!(
        out2.success,
        "second unenroll must exit 0; stderr: {}",
        out2.stderr
    );
    let stdout2 = out2.stdout;
    assert!(
        stdout2.contains("already unenrolled at"),
        "second unenroll must print 'already unenrolled at'; got: {stdout2}"
    );
}

#[test]
fn cli_unenroll_unknown_id_panics() {
    let (db_path, _dir) = setup_db();

    let out = run_cli(
        &[
            "device",
            "--db",
            db_path.to_str().unwrap(),
            "unenroll",
            "--id",
            "99999",
            "--reason",
            "should fail",
        ],
        &[],
    );

    assert!(!out.success, "unenroll with unknown id must exit non-zero");
}

mod push {
    use super::{TestServer, run_cli, run_cli_stdin};
    use brenn_lib::webhook::signature::hmac_sha256_hex;

    /// HMAC the CLI is expected to have produced for `body` at the timestamp the
    /// server observed.
    fn expected_signature(secret: &[u8], timestamp: &str, body: &[u8]) -> String {
        let mut canonical = Vec::new();
        canonical.extend_from_slice(timestamp.as_bytes());
        canonical.push(b'.');
        canonical.extend_from_slice(body);
        format!("v1={}", hmac_sha256_hex(secret, &canonical))
    }

    /// Empty positional message → exit 2, no HTTP attempted.
    #[test]
    fn empty_message_exits_nonzero() {
        let out = run_cli(
            &[
                "push",
                "--url",
                "http://127.0.0.1:1", // unreachable; must never be contacted
                "",
            ],
            &[("BRENN_PUSH_SECRET", "test-secret")],
        );
        assert_eq!(out.code, Some(2), "empty message must exit 2");
        assert!(
            out.stderr.contains("empty") || out.stderr.contains("whitespace"),
            "stderr must mention empty/whitespace; got: {}",
            out.stderr
        );
    }

    /// Whitespace-only message → exit 2 with the message diagnostic.
    #[test]
    fn whitespace_only_message_exits_nonzero() {
        let out = run_cli(
            &["push", "--url", "http://127.0.0.1:1", "   "],
            &[("BRENN_PUSH_SECRET", "test-secret")],
        );
        assert_eq!(out.code, Some(2), "whitespace-only message must exit 2");
        assert!(
            out.stderr.contains("empty or whitespace-only"),
            "stderr must carry the message diagnostic; got: {}",
            out.stderr
        );
    }

    // ── Secret resolution ───────────────────────────────────────────────────

    /// No secret provided → non-zero exit with a diagnostic naming the env var /
    /// flag, never printing a secret value.
    #[test]
    fn no_secret_exits_nonzero_with_diagnostic() {
        let out = run_cli(&["push", "--url", "http://127.0.0.1:1", "hello"], &[]);
        assert_eq!(out.code, Some(2), "missing secret must exit 2");
        assert!(
            out.stderr.contains("BRENN_PUSH_SECRET") || out.stderr.contains("secret-file"),
            "diagnostic must name the missing input; got: {}",
            out.stderr
        );
    }

    /// `BRENN_PUSH_SECRET` is used when no `--secret-file` is provided: the
    /// signature observed by the server is the HMAC of the env secret.
    #[test]
    fn env_secret_used_when_no_file() {
        let server = TestServer::start("HTTP/1.1 200 OK", "ok");
        let out = run_cli(
            &["push", "--url", &server.url, "hello"],
            &[("BRENN_PUSH_SECRET", "some-secret")],
        );
        let req = server.join().expect("CLI must connect to the server");

        assert_eq!(out.code, Some(0), "2xx must exit 0; stderr: {}", out.stderr);
        let ts = req
            .header("x-brenn-push-timestamp")
            .expect("timestamp header");
        assert_eq!(
            req.header("x-brenn-push-signature"),
            Some(expected_signature(b"some-secret", ts, &req.body).as_str()),
            "signature must be the HMAC of the env secret"
        );
    }

    /// `--secret-file` takes precedence over `BRENN_PUSH_SECRET` when both are
    /// set: the observed signature is the file secret's HMAC and not the env
    /// secret's.
    #[test]
    fn secret_file_takes_precedence_over_env() {
        let dir = tempfile::tempdir().expect("tempdir");
        let file_path = dir.path().join("secret.txt");
        std::fs::write(&file_path, "file-secret\n").expect("write secret file");

        let server = TestServer::start("HTTP/1.1 200 OK", "ok");
        let out = run_cli(
            &[
                "push",
                "--url",
                &server.url,
                "--secret-file",
                file_path.to_str().unwrap(),
                "hello",
            ],
            &[("BRENN_PUSH_SECRET", "env-secret-different")],
        );
        let req = server.join().expect("CLI must connect to the server");

        assert_eq!(out.code, Some(0), "2xx must exit 0; stderr: {}", out.stderr);
        let ts = req
            .header("x-brenn-push-timestamp")
            .expect("timestamp header");
        let sig = req
            .header("x-brenn-push-signature")
            .expect("signature header");
        assert_eq!(
            sig,
            expected_signature(b"file-secret", ts, &req.body),
            "the file secret must be the one signed with"
        );
        assert_ne!(
            sig,
            expected_signature(b"env-secret-different", ts, &req.body),
            "the env secret must not win over --secret-file"
        );
    }

    /// Secret file content is trimmed on both ends: the bytes signed are the
    /// trimmed value.
    #[test]
    fn secret_file_trimmed_both_ends() {
        let dir = tempfile::tempdir().expect("tempdir");
        let file_path = dir.path().join("padded-secret.txt");
        std::fs::write(&file_path, "  my-secret  \n").expect("write padded secret file");

        let server = TestServer::start("HTTP/1.1 200 OK", "ok");
        let out = run_cli(
            &[
                "push",
                "--url",
                &server.url,
                "--secret-file",
                file_path.to_str().unwrap(),
                "hello",
            ],
            &[],
        );
        let req = server.join().expect("CLI must connect to the server");

        assert_eq!(out.code, Some(0), "2xx must exit 0; stderr: {}", out.stderr);
        let ts = req
            .header("x-brenn-push-timestamp")
            .expect("timestamp header");
        let sig = req
            .header("x-brenn-push-signature")
            .expect("signature header");
        assert_eq!(
            sig,
            expected_signature(b"my-secret", ts, &req.body),
            "the trimmed secret bytes must be the ones signed with"
        );
        assert_ne!(
            sig,
            expected_signature(b"  my-secret  \n", ts, &req.body),
            "untrimmed file content must not be used as the secret"
        );
    }

    /// A secret file that holds only whitespace is rejected before any request
    /// is sent, which pins the trim-then-check ordering.
    #[test]
    fn whitespace_only_secret_file_exits_two() {
        let dir = tempfile::tempdir().expect("tempdir");
        let file_path = dir.path().join("blank-secret.txt");
        std::fs::write(&file_path, "   \n").expect("write blank secret file");

        let out = run_cli(
            &[
                "push",
                "--url",
                "http://127.0.0.1:1",
                "--secret-file",
                file_path.to_str().unwrap(),
                "hello",
            ],
            &[],
        );
        assert_eq!(out.code, Some(2), "blank secret file must exit 2");
        assert!(
            out.stderr.contains("empty or all-whitespace"),
            "stderr must name the blank secret file; got: {}",
            out.stderr
        );
    }

    /// Diagnostics must never echo the secret value. The secret is supplied via
    /// `BRENN_PUSH_SECRET` so the CLI reads it, then a refused-connection URL
    /// triggers a transport error; the secret must not appear in the stderr.
    #[test]
    fn secret_never_in_error_diagnostic() {
        let out = run_cli(
            &["push", "--url", "http://127.0.0.1:1", "hello"],
            &[("BRENN_PUSH_SECRET", "supersecretvalue")],
        );
        assert_eq!(out.code, Some(2), "transport failure must exit 2");
        assert!(
            out.stderr.contains("error: transport error:"),
            "the CLI must have reached the transport phase; stderr: {}",
            out.stderr
        );
        assert!(
            !out.stderr.contains("supersecretvalue"),
            "secret value must not appear in any diagnostic; got: {}",
            out.stderr
        );
    }

    // ── Send path ───────────────────────────────────────────────────────────

    /// The full signed request as the webhook endpoint sees it: POST, plain-text
    /// body, timestamp / `v1=`-prefixed signature / key-id headers, exit 0.
    #[test]
    fn signed_request_shape_and_success_exit() {
        let server = TestServer::start("HTTP/1.1 200 OK", "ok");
        let out = run_cli(
            &[
                "push",
                "--url",
                &server.url,
                "--key-id",
                "primary",
                "héllo a.b.c",
            ],
            &[("BRENN_PUSH_SECRET", "test-secret")],
        );
        let req = server.join().expect("CLI must connect to the server");

        assert_eq!(out.code, Some(0), "2xx must exit 0; stderr: {}", out.stderr);
        assert!(
            req.request_line.starts_with("POST /push "),
            "must POST to the given path; got: {}",
            req.request_line
        );
        assert_eq!(req.header("content-type"), Some("text/plain"));
        assert_eq!(req.header("x-brenn-push-key-id"), Some("primary"));
        assert_eq!(req.body, "héllo a.b.c".as_bytes(), "body must be verbatim");

        let ts = req
            .header("x-brenn-push-timestamp")
            .expect("timestamp header");
        assert!(
            ts.parse::<i64>().expect("timestamp is an integer") > 1_700_000_000,
            "timestamp must be a plausible unix time; got: {ts}"
        );
        let sig = req
            .header("x-brenn-push-signature")
            .expect("signature header");
        assert_eq!(
            sig,
            expected_signature(b"test-secret", ts, &req.body),
            "signature must cover `timestamp . body` with the resolved secret"
        );
    }

    /// No key-id supplied → the header is omitted entirely.
    #[test]
    fn key_id_header_omitted_when_unset() {
        let server = TestServer::start("HTTP/1.1 200 OK", "ok");
        let out = run_cli(
            &["push", "--url", &server.url, "hello"],
            &[("BRENN_PUSH_SECRET", "test-secret")],
        );
        let req = server.join().expect("CLI must connect to the server");

        assert_eq!(out.code, Some(0), "2xx must exit 0; stderr: {}", out.stderr);
        assert_eq!(req.header("x-brenn-push-key-id"), None);
    }

    /// Server rejects the push (non-2xx) → exit 1, with status and body echoed.
    #[test]
    fn non_2xx_exits_one() {
        let server = TestServer::start("HTTP/1.1 403 Forbidden", "bad signature");
        let out = run_cli(
            &["push", "--url", &server.url, "hello"],
            &[("BRENN_PUSH_SECRET", "test-secret")],
        );
        server.join().expect("CLI must connect to the server");

        assert_eq!(out.code, Some(1), "HTTP rejection must exit 1");
        assert!(
            out.stderr.contains("403") && out.stderr.contains("bad signature"),
            "stderr must carry the server's status and body; got: {}",
            out.stderr
        );
    }

    /// An invalid `--key-id` is rejected locally, before any connection.
    #[test]
    fn invalid_key_id_exits_two_before_sending() {
        let out = run_cli(
            &[
                "push",
                "--url",
                "http://127.0.0.1:1",
                "--key-id",
                "bad key",
                "hello",
            ],
            &[("BRENN_PUSH_SECRET", "test-secret")],
        );
        assert_eq!(out.code, Some(2), "invalid key-id must exit 2");
        assert!(
            out.stderr.contains("key-id") && out.stderr.contains("invalid"),
            "stderr must name the invalid key-id; got: {}",
            out.stderr
        );
        assert!(
            !out.stderr.contains("transport error"),
            "no request may be attempted; stderr: {}",
            out.stderr
        );
    }

    // ── stdin message path ──────────────────────────────────────────────────

    /// Message read from stdin: exactly one trailing newline is stripped, so
    /// `echo hi | brenn-cli push` sends the same body as `brenn-cli push hi`.
    #[test]
    fn stdin_message_strips_one_trailing_newline() {
        let server = TestServer::start("HTTP/1.1 200 OK", "ok");
        let out = run_cli_stdin(
            &["push", "--url", &server.url],
            &[("BRENN_PUSH_SECRET", "test-secret")],
            "hi\r\n",
        );
        let req = server.join().expect("CLI must connect to the server");

        assert_eq!(out.code, Some(0), "2xx must exit 0; stderr: {}", out.stderr);
        assert_eq!(req.body, b"hi", "CRLF must be stripped, and only once");
    }

    /// Trailing newlines beyond the first stay in the body.
    #[test]
    fn stdin_message_keeps_extra_newlines() {
        let server = TestServer::start("HTTP/1.1 200 OK", "ok");
        let out = run_cli_stdin(
            &["push", "--url", &server.url],
            &[("BRENN_PUSH_SECRET", "test-secret")],
            "hi\n\n",
        );
        let req = server.join().expect("CLI must connect to the server");

        assert_eq!(out.code, Some(0), "2xx must exit 0; stderr: {}", out.stderr);
        assert_eq!(req.body, b"hi\n", "only one trailing newline is stripped");
    }

    /// Whitespace-only stdin is rejected by the real message resolver.
    #[test]
    fn stdin_whitespace_only_exits_two() {
        let out = run_cli_stdin(
            &["push", "--url", "http://127.0.0.1:1"],
            &[("BRENN_PUSH_SECRET", "test-secret")],
            "   \n",
        );
        assert_eq!(out.code, Some(2), "whitespace-only stdin must exit 2");
        assert!(
            out.stderr.contains("empty or whitespace-only"),
            "stderr must carry the message diagnostic; got: {}",
            out.stderr
        );
    }
}
