//! The per-conversation tool socket: a unix listener through which
//! `brenn-tool`, run inside the agent's container, calls the conversation's
//! virtual tools.
//!
//! The socket carries no credential. Its directory is mounted into one
//! conversation's container only, so whoever can connect is that conversation,
//! and every call runs with the owning bridge's identity through the same
//! admission and dispatch as the hook path.
//!
//! One connection carries one request line and one response line (the wire
//! types live in `brenn_tool_proto`). Intake is bounded, not queued: a
//! connection beyond `MAX_IN_FLIGHT` open ones is answered `busy` before any
//! of its bytes are read, and a request line is read up to
//! `MAX_REQUEST_BYTES`.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock, Weak};
use std::time::{Duration, Instant};

use brenn_common::{MAX_LOGGED_UNTRUSTED_BYTES, sanitize_untrusted_str};
use brenn_lib::integration::CliAccess;
use brenn_lib::runtime_dir::{recreate_private_dir, tool_socket_dir, tool_socket_in};
use brenn_obs::security::{SecurityEventType, log_app_security_event};
use brenn_tool_proto::{CatalogEntry, MAX_REQUEST_BYTES, Request, Response, WireError};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::unix::OwnedReadHalf;
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;

use super::ActiveBridge;
use super::brenn_tools::{CallOrigin, ExecuteError, execute_tool};
use super::cc_spawn_config::{find_virtual_tool, virtual_tools};

/// Open connections served at once per conversation; one more is answered
/// `busy` and closed.
pub const MAX_IN_FLIGHT: usize = 8;

/// How long a connection may take to deliver its request line. A local client
/// writes one line it assembled before connecting; a connection still silent
/// after this is answered `invalid_request` and gives back its in-flight
/// permit.
pub const REQUEST_READ_DEADLINE: Duration = Duration::from_secs(10);

/// Where the agent's container sees the per-conversation directory.
pub const CONTAINER_SOCKET_DIR: &str = "/run/brenn";

/// How long the accept loop pauses after an accept error that is not about
/// one connection (fd or memory exhaustion) before it accepts again.
const ACCEPT_RETRY_PAUSE: Duration = Duration::from_secs(1);

/// The podman volume spec mounting `host_dir` into the agent's container.
///
/// Read-only: connecting to a unix socket needs no write access to the
/// mount, and the container cannot add, remove or replace entries in a
/// directory the server owns on the runtime tmpfs.
pub fn container_mount(host_dir: &Path) -> String {
    format!("{}:{CONTAINER_SOCKET_DIR}:ro,z", host_dir.display())
}

/// Where the agent's container sees the socket.
pub fn container_socket_path() -> PathBuf {
    tool_socket_in(Path::new(CONTAINER_SOCKET_DIR))
}

/// What every connection task reads: who the socket belongs to, and the bridge
/// once it exists.
struct Shared {
    app_slug: String,
    conversation_id: i64,
    bridge: OnceLock<Weak<ActiveBridge>>,
    in_flight: Arc<Semaphore>,
    read_deadline: Duration,
}

/// A conversation's tool socket. Closing (or dropping) it stops accepting and
/// removes its directory; calls already running finish.
pub struct ToolSocket {
    dir: PathBuf,
    accept_task: JoinHandle<()>,
    shared: Arc<Shared>,
    closed: AtomicBool,
}

impl ToolSocket {
    /// Bind `<tool_socket_root>/<app_slug>-conv<id>/tool.sock` in a fresh 0700
    /// directory, removing whatever an earlier socket for the conversation
    /// left at that path, and start accepting. Must run inside a tokio
    /// runtime.
    pub fn bind(tool_socket_root: &Path, app_slug: &str, conversation_id: i64) -> io::Result<Self> {
        Self::bind_with_read_deadline(
            tool_socket_root,
            app_slug,
            conversation_id,
            REQUEST_READ_DEADLINE,
        )
    }

    fn bind_with_read_deadline(
        tool_socket_root: &Path,
        app_slug: &str,
        conversation_id: i64,
        read_deadline: Duration,
    ) -> io::Result<Self> {
        let dir = tool_socket_dir(tool_socket_root, app_slug, conversation_id);
        recreate_private_dir(&dir)?;
        let socket_path = tool_socket_in(&dir);
        let listener = UnixListener::bind(&socket_path).map_err(|e| {
            io::Error::new(e.kind(), format!("binding {}: {e}", socket_path.display()))
        })?;
        let shared = Arc::new(Shared {
            app_slug: app_slug.to_string(),
            conversation_id,
            bridge: OnceLock::new(),
            in_flight: Arc::new(Semaphore::new(MAX_IN_FLIGHT)),
            read_deadline,
        });
        let accept_task = tokio::spawn(accept_loop(listener, shared.clone()));
        Ok(Self {
            dir,
            accept_task,
            shared,
            closed: AtomicBool::new(false),
        })
    }

    /// Install the bridge calls run against. Connections before this are
    /// answered `not_ready`. Connection tasks upgrade the `Weak` per request,
    /// so the socket never keeps the bridge alive.
    pub fn attach(&self, bridge: Weak<ActiveBridge>) {
        if self.shared.bridge.set(bridge).is_err() {
            panic!(
                "BUG: tool socket for conversation {} attached twice",
                self.shared.conversation_id
            );
        }
    }

    /// The per-conversation directory holding the socket, the mount source
    /// for the agent's container.
    pub fn host_dir(&self) -> &Path {
        &self.dir
    }

    /// Stop accepting and remove the directory with everything in it.
    /// Idempotent. Calls already running are not aborted: they complete and
    /// their answer goes to whatever is still listening.
    ///
    /// The next socket for the same conversation binds the same directory, so
    /// this runs before the bridge leaves the registry and the conversation
    /// can spawn again.
    pub fn close(&self) {
        if self.closed.swap(true, Ordering::SeqCst) {
            return;
        }
        self.accept_task.abort();
        match std::fs::remove_dir_all(&self.dir) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => panic!("removing tool socket directory {}: {e}", self.dir.display()),
        }
    }
}

impl Drop for ToolSocket {
    fn drop(&mut self) {
        self.close();
    }
}

async fn accept_loop(listener: UnixListener, shared: Arc<Shared>) {
    loop {
        let stream = match listener.accept().await {
            Ok((stream, _)) => stream,
            Err(e)
                if matches!(
                    e.kind(),
                    io::ErrorKind::ConnectionAborted
                        | io::ErrorKind::ConnectionRefused
                        | io::ErrorKind::ConnectionReset
                ) =>
            {
                continue;
            }
            Err(e) => {
                tracing::error!(
                    app = %shared.app_slug,
                    conversation_id = shared.conversation_id,
                    error = %e,
                    pause = ?ACCEPT_RETRY_PAUSE,
                    "tool socket accept failed; accepting again after a pause"
                );
                tokio::time::sleep(ACCEPT_RETRY_PAUSE).await;
                continue;
            }
        };
        let Ok(permit) = shared.in_flight.clone().try_acquire_owned() else {
            refuse_busy(stream, &shared);
            continue;
        };
        let shared = shared.clone();
        tokio::spawn(async move {
            let _permit = permit;
            serve_connection(stream, &shared).await;
        });
    }
}

/// Answer `busy` without waiting: the line goes out in one non-blocking write
/// straight to the fresh connection's empty buffer, then the connection
/// closes.
fn refuse_busy(stream: UnixStream, shared: &Shared) {
    tracing::info!(
        app = %shared.app_slug,
        conversation_id = shared.conversation_id,
        max_in_flight = MAX_IN_FLIGHT,
        "tool socket busy; connection refused"
    );
    let line = response_line(&Response::Err(WireError::Busy {
        detail: format!(
            "{MAX_IN_FLIGHT} requests are already open on this socket; run calls sequentially"
        ),
    }));
    // The tokio stream has no write readiness yet, so its `try_write` would
    // refuse; the std stream writes directly and stays non-blocking.
    let delivered = stream
        .into_std()
        .and_then(|std_stream| std::io::Write::write_all(&mut &std_stream, line.as_bytes()));
    if let Err(e) = delivered {
        tracing::debug!(error = %e, "tool socket busy answer not delivered");
    }
}

enum LineError {
    TooLong,
    Unterminated(Vec<u8>),
    Io(io::Error),
}

/// Read one newline-terminated request line of at most `MAX_REQUEST_BYTES`
/// bytes, newline excluded.
async fn read_request_line(read: OwnedReadHalf) -> Result<Vec<u8>, LineError> {
    let mut reader = BufReader::new(read).take(MAX_REQUEST_BYTES as u64 + 1);
    let mut buf = Vec::new();
    reader
        .read_until(b'\n', &mut buf)
        .await
        .map_err(LineError::Io)?;
    if buf.last() == Some(&b'\n') {
        buf.pop();
        return Ok(buf);
    }
    if buf.len() > MAX_REQUEST_BYTES {
        return Err(LineError::TooLong);
    }
    Err(LineError::Unterminated(buf))
}

async fn serve_connection(stream: UnixStream, shared: &Shared) {
    let (read, mut write) = stream.into_split();
    let read = tokio::time::timeout(shared.read_deadline, read_request_line(read)).await;
    let response = match read {
        Err(_elapsed) => {
            let detail = format!(
                "no request line within {}s of connecting",
                shared.read_deadline.as_secs_f64()
            );
            signal_violation(shared, &detail, None);
            invalid_request(detail)
        }
        Ok(Ok(line)) => answer(shared, &line).await,
        Ok(Err(LineError::Io(e))) => {
            tracing::debug!(error = %e, "tool socket read failed");
            return;
        }
        Ok(Err(LineError::TooLong)) => {
            let detail = format!("request line exceeds {MAX_REQUEST_BYTES} bytes");
            signal_violation(shared, &detail, None);
            invalid_request(detail)
        }
        Ok(Err(LineError::Unterminated(bytes))) => {
            let detail = "request line ends without a newline".to_string();
            signal_violation(shared, &detail, Some(&bytes));
            invalid_request(detail)
        }
    };
    let line = response_line(&response);
    if let Err(e) = write.write_all(line.as_bytes()).await {
        tracing::debug!(error = %e, "tool socket response not delivered; the client is gone");
        return;
    }
    if let Err(e) = write.shutdown().await {
        tracing::debug!(error = %e, "tool socket shutdown after response failed");
    }
}

/// Answer one request line.
async fn answer(shared: &Shared, line: &[u8]) -> Response {
    let request = match Request::parse(line) {
        Ok(request) => request,
        Err(detail) => {
            signal_violation(shared, &detail, Some(line));
            return invalid_request(detail);
        }
    };
    let Some(bridge) = shared.bridge.get().and_then(Weak::upgrade) else {
        return Response::Err(WireError::NotReady {
            detail: "this conversation is not serving tool calls".into(),
        });
    };
    match request {
        Request::List => {
            let entries: Vec<CatalogEntry> = virtual_tools(&bridge.app_config(), &bridge.tools)
                .into_iter()
                .map(|def| catalog_entry(def, false))
                .collect();
            Response::Ok(serde_json::to_value(entries).expect("catalog entries serialize"))
        }
        Request::Describe { tool } => find_virtual_tool(&bridge.app_config(), &bridge.tools, &tool)
            .map(|def| {
                Response::Ok(
                    serde_json::to_value(catalog_entry(def, true))
                        .expect("a catalog entry serializes"),
                )
            })
            .unwrap_or_else(|| Response::Err(ExecuteError::UnknownTool.to_wire(&tool))),
        Request::Call { tool, input } => {
            let started = Instant::now();
            let response = match execute_tool(&bridge, CallOrigin::Socket, &tool, input).await {
                Ok(super::ToolOutcome::Ok(result)) => Response::Ok(result),
                Ok(super::ToolOutcome::Err(output)) => {
                    Response::Err(WireError::ToolError { output })
                }
                Err(e) => Response::Err(e.to_wire(&tool)),
            };
            tracing::info!(
                conversation_id = shared.conversation_id,
                app = %shared.app_slug,
                tool = %sanitize_untrusted_str(&tool, MAX_LOGGED_UNTRUSTED_BYTES),
                outcome = match &response {
                    Response::Ok(_) => "ok",
                    Response::Err(e) => e.kind(),
                },
                elapsed = ?started.elapsed(),
                request_bytes = line.len(),
                "tool socket invocation"
            );
            response
        }
    }
}

fn catalog_entry(def: brenn_lib::integration::VirtualToolDef, with_schema: bool) -> CatalogEntry {
    CatalogEntry {
        name: def.name,
        description: def.description,
        available: def.cli == CliAccess::Available,
        reason: def.cli.reason().map(str::to_string),
        input_schema: with_schema.then_some(def.input_schema),
    }
}

fn invalid_request(detail: String) -> Response {
    Response::Err(WireError::InvalidRequest { detail })
}

fn response_line(response: &Response) -> String {
    let mut line = response.to_line();
    line.push('\n');
    line
}

/// Log a protocol violation, attributed to the socket's conversation. The
/// offending bytes are sanitized and bounded before they reach the log.
fn signal_violation(shared: &Shared, reason: &str, bytes: Option<&[u8]>) {
    let safe_reason = sanitize_untrusted_str(reason, MAX_LOGGED_UNTRUSTED_BYTES);
    let detail = match bytes {
        Some(bytes) => format!(
            "{safe_reason}; request={}",
            sanitize_untrusted_str(&String::from_utf8_lossy(bytes), MAX_LOGGED_UNTRUSTED_BYTES)
        ),
        None => safe_reason,
    };
    log_app_security_event(
        SecurityEventType::ToolSocketViolation,
        &shared.app_slug,
        shared.conversation_id,
        &detail,
    );
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;

    use serde_json::json;

    use super::*;
    use crate::active_bridge::test_support::test_bridge;

    fn bind_in(root: &Path) -> ToolSocket {
        ToolSocket::bind(root, "testapp", 42).unwrap()
    }

    /// Send `bytes`, close the write side, and read the one response line.
    /// The response is read up to its newline, not to EOF: a server that
    /// closes with request bytes unread resets the connection after it.
    async fn send_raw(path: &Path, bytes: &[u8]) -> Response {
        let stream = UnixStream::connect(path).await.unwrap();
        let (read, mut write) = stream.into_split();
        let (_, out) = tokio::join!(
            async {
                // The server stops reading an over-cap line, or never reads a
                // refused one, and closes first.
                let written = match write.write_all(bytes).await {
                    Ok(()) => write.shutdown().await,
                    Err(e) => Err(e),
                };
                if let Err(e) = written {
                    assert!(
                        matches!(
                            e.kind(),
                            io::ErrorKind::BrokenPipe | io::ErrorKind::ConnectionReset
                        ),
                        "{e}"
                    );
                }
            },
            async {
                let mut out = Vec::new();
                BufReader::new(read)
                    .read_until(b'\n', &mut out)
                    .await
                    .unwrap();
                out
            }
        );
        assert_eq!(
            out.last(),
            Some(&b'\n'),
            "{}",
            String::from_utf8_lossy(&out)
        );
        Response::parse(&out[..out.len() - 1]).unwrap()
    }

    async fn send(path: &Path, request: &Request) -> Response {
        let mut line = request.to_line().into_bytes();
        line.push(b'\n');
        send_raw(path, &line).await
    }

    fn sock(s: &ToolSocket) -> PathBuf {
        tool_socket_in(s.host_dir())
    }

    fn call(tool: &str, input: serde_json::Value) -> Request {
        Request::Call {
            tool: tool.into(),
            input,
        }
    }

    #[tokio::test]
    async fn binds_a_private_per_conversation_directory() {
        let root = tempfile::tempdir().unwrap();
        let s = bind_in(root.path());
        assert_eq!(s.host_dir(), root.path().join("testapp-conv42"));
        let mode = std::fs::metadata(s.host_dir())
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o700);
        assert!(sock(&s).exists());
    }

    #[tokio::test]
    async fn list_and_describe_report_the_live_catalog() {
        let root = tempfile::tempdir().unwrap();
        let (bridge, _e, _b, _a) = test_bridge().await;
        let s = bind_in(root.path());
        s.attach(Arc::downgrade(&bridge));

        let Response::Ok(list) = send(&sock(&s), &Request::List).await else {
            panic!("list failed");
        };
        let entries: Vec<CatalogEntry> = serde_json::from_value(list).unwrap();
        let expected = virtual_tools(&bridge.app_config(), &bridge.tools);
        assert_eq!(entries.len(), expected.len());
        let by_name = |n: &str| entries.iter().find(|e| e.name == n).unwrap().clone();
        let display = by_name("DisplayFile");
        assert!(!display.available);
        assert_eq!(display.reason.as_deref(), Some("conversation_bound"));
        assert!(display.input_schema.is_none());
        let channels = by_name("MessageChannelList");
        assert!(channels.available);
        assert!(channels.reason.is_none());

        let Response::Ok(described) = send(
            &sock(&s),
            &Request::Describe {
                tool: "MessageChannelGet".into(),
            },
        )
        .await
        else {
            panic!("describe failed");
        };
        let entry: CatalogEntry = serde_json::from_value(described).unwrap();
        let schema = entry.input_schema.expect("describe carries the schema");
        assert_eq!(
            &schema,
            &expected
                .iter()
                .find(|d| d.name == "MessageChannelGet")
                .unwrap()
                .input_schema
        );

        assert!(matches!(
            send(
                &sock(&s),
                &Request::Describe {
                    tool: "Nope".into()
                }
            )
            .await,
            Response::Err(WireError::UnknownTool { .. })
        ));
    }

    #[tokio::test]
    async fn calls_answer_with_the_tool_outcome_or_the_refusal() {
        let root = tempfile::tempdir().unwrap();
        let (bridge, _e, _b, _a) = test_bridge().await;
        let s = bind_in(root.path());
        s.attach(Arc::downgrade(&bridge));
        let path = sock(&s);

        match send(&path, &call("MessageChannelList", json!({}))).await {
            Response::Ok(v) => assert!(v["channels"].is_array(), "{v}"),
            other => panic!("expected a listing, got {other:?}"),
        }
        assert_eq!(
            send(&path, &call("DisplayFile", json!({"file_path": "a.md"}))).await,
            Response::Err(WireError::NotAvailable {
                detail: "DisplayFile: conversation_bound".into()
            })
        );
        assert!(matches!(
            send(&path, &call("MessageChannelGet", json!({}))).await,
            Response::Err(WireError::InvalidArgs { .. })
        ));
        assert!(matches!(
            send(&path, &call("NoSuchTool", json!({}))).await,
            Response::Err(WireError::UnknownTool { .. })
        ));
        // Admitted, and refused by the tool itself: this bridge has no messenger.
        match send(
            &path,
            &call(
                "MessageChannelGet",
                json!({"address": "brenn:nope.x", "limit": 1}),
            ),
        )
        .await
        {
            Response::Err(WireError::ToolError { output }) => {
                assert!(output.get("error").is_some(), "{output}")
            }
            other => panic!("expected the tool's own error, got {other:?}"),
        }
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn malformed_requests_are_violations() {
        let root = tempfile::tempdir().unwrap();
        let (bridge, _e, _b, _a) = test_bridge().await;
        let s = bind_in(root.path());
        s.attach(Arc::downgrade(&bridge));
        let path = sock(&s);

        for bad in [
            &b"garbage\n"[..],
            b"{\"v\":2,\"op\":\"list\"}\n",
            b"{\"v\":1,\"op\":\"call\",\"tool\":\"T\"}\n",
            b"{\"v\":1,\"op\":\"list\"}",
        ] {
            assert!(
                matches!(
                    send_raw(&path, bad).await,
                    Response::Err(WireError::InvalidRequest { .. })
                ),
                "{}",
                String::from_utf8_lossy(bad)
            );
        }
        assert!(logs_contain("tool_socket_violation"));
        assert!(logs_contain("request=garbage"));
    }

    #[tokio::test]
    async fn the_line_cap_is_exact() {
        let root = tempfile::tempdir().unwrap();
        let (bridge, _e, _b, _a) = test_bridge().await;
        let s = bind_in(root.path());
        s.attach(Arc::downgrade(&bridge));
        let path = sock(&s);

        let mut over = vec![b'a'; MAX_REQUEST_BYTES + 1];
        over.push(b'\n');
        match send_raw(&path, &over).await {
            Response::Err(WireError::InvalidRequest { detail }) => {
                assert!(detail.contains("exceeds"), "{detail}")
            }
            other => panic!("expected invalid_request, got {other:?}"),
        }

        // A request of exactly the cap is read and parsed.
        let base = br#"{"v":1,"op":"call","tool":"MessageChannelList","input":{}}"#;
        let mut at_cap = base.to_vec();
        at_cap.resize(MAX_REQUEST_BYTES, b' ');
        at_cap.push(b'\n');
        assert!(matches!(send_raw(&path, &at_cap).await, Response::Ok(_)));
    }

    /// The socket path reaches the policy inside the publish: a send outside
    /// the agent's `brenn_publish` ACL is the tool's own refusal.
    #[tokio::test]
    async fn a_send_outside_the_publish_acl_is_refused() {
        let root = tempfile::tempdir().unwrap();
        let bridge = ActiveBridge::test_new_with_combined_services().await;
        // The fixture's messenger holds the grant and the ACL; the agent
        // offers `BrennSend` once it holds the grant too.
        crate::active_bridge::test_fixtures::swap_single_app(&bridge.apps, "testapp", |app| {
            app.policy_mut()
                .grants
                .insert(brenn_envelope::grants::AppCapability::MessagingPublish);
        });
        let s = bind_in(root.path());
        s.attach(Arc::downgrade(&bridge));
        match send(
            &sock(&s),
            &call(
                "BrennSend",
                json!({"to": "brenn:locked-channel", "body": "hello"}),
            ),
        )
        .await
        {
            Response::Err(WireError::ToolError { output }) => {
                assert_eq!(output["ok"], json!(false), "{output}");
                let err = output["error"].as_str().unwrap_or_default();
                assert!(err.contains("brenn_publish"), "{output}");
            }
            other => panic!("expected the publish's denial, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn not_ready_before_attach() {
        let root = tempfile::tempdir().unwrap();
        let s = bind_in(root.path());
        assert!(matches!(
            send(&sock(&s), &Request::List).await,
            Response::Err(WireError::NotReady { .. })
        ));
    }

    /// The socket holds only a `Weak`: once the bridge is gone, calls are
    /// answered `not_ready` instead of running against it.
    #[tokio::test]
    async fn not_ready_once_the_bridge_is_gone() {
        let root = tempfile::tempdir().unwrap();
        let bridge = ActiveBridge::test_new_with_combined_services().await;
        let s = bind_in(root.path());
        let weak = Arc::downgrade(&bridge);
        s.attach(weak.clone());
        drop(bridge);
        assert!(
            weak.upgrade().is_none(),
            "the fixture holds no other strong ref"
        );
        assert!(matches!(
            send(&sock(&s), &call("MessageChannelList", json!({}))).await,
            Response::Err(WireError::NotReady { .. })
        ));
    }

    #[tokio::test]
    #[should_panic(expected = "attached twice")]
    async fn a_second_attach_is_a_bug() {
        let root = tempfile::tempdir().unwrap();
        let (bridge, _e, _b, _a) = test_bridge().await;
        let s = bind_in(root.path());
        s.attach(Arc::downgrade(&bridge));
        s.attach(Arc::downgrade(&bridge));
    }

    #[tokio::test]
    async fn the_ninth_open_connection_is_busy_at_once() {
        let root = tempfile::tempdir().unwrap();
        let (bridge, _e, _b, _a) = test_bridge().await;
        let s = bind_in(root.path());
        s.attach(Arc::downgrade(&bridge));
        let path = sock(&s);

        let mut held = Vec::new();
        for _ in 0..MAX_IN_FLIGHT {
            held.push(UnixStream::connect(&path).await.unwrap());
        }
        let ninth = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            send(&path, &Request::List),
        )
        .await
        .expect("busy is answered without waiting");
        assert!(matches!(ninth, Response::Err(WireError::Busy { .. })));
        drop(held);
    }

    /// Silent connections are answered `invalid_request` at the read deadline
    /// and give their permits back, so they cannot hold the socket `busy`.
    #[tracing_test::traced_test]
    #[tokio::test]
    async fn silent_connections_time_out_and_free_their_permits() {
        let root = tempfile::tempdir().unwrap();
        let (bridge, _e, _b, _a) = test_bridge().await;
        let s = ToolSocket::bind_with_read_deadline(
            root.path(),
            "testapp",
            42,
            std::time::Duration::from_millis(200),
        )
        .unwrap();
        s.attach(Arc::downgrade(&bridge));
        let path = sock(&s);

        let mut held = Vec::new();
        for _ in 0..MAX_IN_FLIGHT {
            held.push(UnixStream::connect(&path).await.unwrap());
        }
        for stream in held {
            let mut out = Vec::new();
            tokio::time::timeout(
                std::time::Duration::from_secs(10),
                BufReader::new(stream).read_until(b'\n', &mut out),
            )
            .await
            .expect("a silent connection is answered at the deadline")
            .unwrap();
            assert!(matches!(
                Response::parse(&out[..out.len() - 1]).unwrap(),
                Response::Err(WireError::InvalidRequest { .. })
            ));
        }
        assert!(logs_contain("tool_socket_violation"));
        assert!(logs_contain("no request line within"));
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while s.shared.in_flight.available_permits() < MAX_IN_FLIGHT {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("timed-out connections give their permits back");
        assert!(matches!(send(&path, &Request::List).await, Response::Ok(_)));
    }

    #[tokio::test]
    async fn close_removes_the_directory_and_refuses_connections() {
        let root = tempfile::tempdir().unwrap();
        let s = bind_in(root.path());
        let dir = s.host_dir().to_path_buf();
        let path = sock(&s);
        s.close();
        s.close();
        assert!(!dir.exists());
        assert!(UnixStream::connect(&path).await.is_err());
    }

    #[tokio::test]
    async fn drop_closes() {
        let root = tempfile::tempdir().unwrap();
        let s = bind_in(root.path());
        let dir = s.host_dir().to_path_buf();
        let path = sock(&s);
        drop(s);
        assert!(!dir.exists());
        assert!(UnixStream::connect(&path).await.is_err());
    }

    /// Whatever sits in the directory at close is removed with it; a symlink
    /// is removed, not followed.
    #[tokio::test]
    async fn close_removes_entries_it_did_not_create() {
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let kept = outside.path().join("kept");
        std::fs::write(&kept, b"x").unwrap();
        let s = bind_in(root.path());
        let dir = s.host_dir().to_path_buf();
        std::fs::write(dir.join("stray"), b"x").unwrap();
        std::fs::create_dir(dir.join("sub")).unwrap();
        std::fs::write(dir.join("sub").join("nested"), b"x").unwrap();
        std::os::unix::fs::symlink(outside.path(), dir.join("link")).unwrap();
        s.close();
        assert!(!dir.exists());
        assert!(kept.exists(), "a symlink's target is not touched");
    }

    /// A directory left by an earlier socket for the conversation, with a
    /// stale socket file, a directory in the socket's place and other entries,
    /// is replaced by a fresh one that serves.
    #[tokio::test]
    async fn bind_replaces_what_an_earlier_socket_left() {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("testapp-conv42");
        std::fs::create_dir(&dir).unwrap();
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755)).unwrap();
        drop(std::os::unix::net::UnixListener::bind(dir.join("stale.sock")).unwrap());
        std::fs::create_dir(tool_socket_in(&dir)).unwrap();
        std::fs::write(dir.join("stray"), b"x").unwrap();
        let (bridge, _e, _b, _a) = test_bridge().await;
        let s = bind_in(root.path());
        s.attach(Arc::downgrade(&bridge));
        let names: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(
            names,
            vec![std::ffi::OsString::from(
                brenn_lib::runtime_dir::TOOL_SOCKET_FILE_NAME
            )]
        );
        let mode = std::fs::metadata(&dir).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o700);
        assert!(matches!(
            send(&sock(&s), &Request::List).await,
            Response::Ok(_)
        ));
    }

    #[tokio::test]
    async fn bind_refuses_a_non_directory_in_the_way() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("testapp-conv42"), b"").unwrap();
        let err = ToolSocket::bind(root.path(), "testapp", 42).err().unwrap();
        assert_eq!(err.kind(), io::ErrorKind::NotADirectory, "{err}");
        assert!(err.to_string().contains("testapp-conv42"), "{err}");
    }

    #[test]
    fn the_container_mount_is_read_only() {
        assert_eq!(
            container_mount(Path::new("/run/user/1000/brenn-tool-sockets/a-conv1")),
            "/run/user/1000/brenn-tool-sockets/a-conv1:/run/brenn:ro,z"
        );
    }
}
