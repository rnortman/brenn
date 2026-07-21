pub mod approval;
pub mod tasks;

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use brenn_lib::config::ContainerSpawnConfig;
use brenn_lib::obs::alerting::AlertDispatcher;
use brenn_lib::obs::transcript::TranscriptWriter;
use brenn_lib::ws_types::PermissionModeValue;
use tokio::process::{Child, Command};
use tokio::sync::{mpsc, oneshot};

use tracing::warn;

use crate::error::{CcError, TransportError};
use crate::protocol::outgoing::*;
use crate::protocol::{self, CcIncoming, CcOutgoing, SystemMessage};

pub use approval::{ApprovalDecision, ApprovalKind, ApprovalRequest};

/// An outgoing message envelope carrying an optional flush-ack sender.
///
/// Most senders use `ack: None` (fire-and-forget). Callers that need to know
/// the message was successfully flushed to CC's stdin (e.g. the dispatcher's
/// delivered-marking path) create a oneshot pair, set `ack: Some(sender)`, and
/// await the receiver. The writer task fires the ack after `write_all` + `flush`
/// succeed (`Ok` arm) or on the first write/flush error before it breaks (`Err`
/// arm). A dropped ack sender (writer task exited without firing) resolves the
/// receiver as `Err(RecvError)` — treated as flush-failure by the caller.
pub struct OutgoingEnvelope {
    /// The message to send to CC.
    pub msg: CcOutgoing,
    /// Optional flush-ack channel. Fired after `write_all` + `flush` succeed or fail.
    ///
    /// `let _ =` on `ack.send` is intentional on both arms:
    /// - Ok arm: flush succeeded; a send error means the receiver was dropped (caller
    ///   no longer awaiting, e.g. fan-out task cancelled). Nothing to handle.
    /// - Err arm: flush did not succeed; a dropped receiver means the caller already
    ///   stopped awaiting, and the row stays parked regardless. Benign.
    pub ack: Option<oneshot::Sender<Result<(), TransportError>>>,
}

impl std::fmt::Debug for OutgoingEnvelope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OutgoingEnvelope")
            .field("msg", &self.msg)
            .field("ack", &self.ack.as_ref().map(|_| "<ack sender>"))
            .finish()
    }
}

/// Configuration for spawning a CC session.
pub struct CcSessionConfig {
    /// Model to use (e.g., "haiku", "sonnet", "opus").
    pub model: String,
    /// Working directory for CC (host-side for bare process, ignored if containerized).
    pub cwd: PathBuf,
    /// Hook configuration (PreToolUse, PostToolUse, etc.)
    pub hooks: Option<HooksConfig>,
    /// MCP server configuration.
    pub mcp_config: Option<serde_json::Value>,
    /// Restrict CC's tool set via `--tools` flag.
    /// If None, CC uses its default tool set.
    pub allowed_tools: Option<Vec<String>>,
    /// Resume a previous session by ID (maps to `--resume <id>`).
    pub resume_session_id: Option<String>,
    /// Transcript writer for raw NDJSON protocol logging.
    pub transcript: Arc<TranscriptWriter>,
    /// Alert dispatcher for phone alerts on unexpected CC behavior.
    pub alert_dispatcher: AlertDispatcher,
    /// Container configuration. If set, CC is spawned inside a podman container.
    pub container: Option<brenn_lib::config::ContainerSpawnConfig>,
    /// App slug, used for container naming.
    pub app_slug: String,
    /// Suffix for the container name (e.g. "conv42"). Combined with
    /// `app_slug` to form `brenn-{app_slug}-{suffix}`. Keeps container naming
    /// out of the session's concern — callers decide the label.
    pub container_name_suffix: String,
    /// Directories to pass as `--add-dir` to CC, expanding its
    /// workspace-trust scope beyond `cwd`. Paths must already be in
    /// CC-visible form — use `ResolvedMount::visible_path(containerized)`.
    pub add_dirs: Vec<PathBuf>,
    /// Extra CLI arguments passed verbatim to the `claude` command.
    pub cc_extra_args: Vec<String>,
    /// Extra environment variables for the CC process (bare apps only).
    /// For containerized apps, env vars are injected as podman -e flags instead.
    pub env_vars: Vec<(String, String)>,
    /// Pre-created shutdown flag for the reader task to check on EOF.
    ///
    /// If provided, `spawn()` uses this instead of creating its own. This lets
    /// the caller hold a clone and set it when the spawn is cancelled
    /// mid-init-handshake — before a `CcSession` is ever constructed.
    ///
    /// Leave `None` for normal spawns; `spawn()` will create its own.
    pub shutting_down: Option<Arc<AtomicBool>>,
}

/// A model available for selection, sourced from CC's init ack.
#[derive(Debug, Clone)]
pub struct ModelOption {
    /// The value to pass to CC (e.g. "default", "sonnet", "haiku").
    pub value: String,
    /// Human-readable name (e.g. "Sonnet", "Haiku").
    pub display_name: String,
    /// Short description (e.g. "Sonnet 4.6 · Best for everyday tasks").
    pub description: String,
}

/// Metadata from CC's system/init message.
#[derive(Debug, Clone)]
pub struct SessionInfo {
    pub session_id: String,
    pub tools: Vec<String>,
    pub model: String,
    pub cwd: String,
    pub claude_code_version: Option<String>,
    pub mcp_servers: Vec<crate::protocol::incoming::McpServerStatus>,
    /// CC's reported permission mode from the init frame. `None` if CC omitted
    /// the field or sent it as `null`. Known value is `Auto`; anything else
    /// lands in `Other` so the string is preserved for alerting.
    pub permission_mode: Option<PermissionModeValue>,
}

/// Metadata from the init ack (control_response to initialize).
#[derive(Debug, Clone, Default)]
pub struct InitAckInfo {
    /// Available models for selection. Empty if CC didn't provide them.
    pub models: Vec<ModelOption>,
}

impl SessionInfo {
    /// Extract session info from a system/init message.
    /// Panics if the message is not a `SystemMessage::Init`.
    fn from_system_init(msg: &SystemMessage) -> Self {
        match msg {
            SystemMessage::Init {
                session_id,
                tools,
                model,
                cwd,
                claude_code_version,
                mcp_servers,
                permission_mode,
                ..
            } => Self {
                session_id: session_id
                    .clone()
                    .expect("system/init must have session_id"),
                tools: tools.clone().unwrap_or_default(),
                model: model.clone().unwrap_or_else(|| "unknown".to_string()),
                cwd: cwd.clone().unwrap_or_else(|| "unknown".to_string()),
                claude_code_version: claude_code_version.clone(),
                mcp_servers: mcp_servers.clone().unwrap_or_default(),
                permission_mode: permission_mode.clone(),
            },
            SystemMessage::Status { .. }
            | SystemMessage::CompactBoundary { .. }
            | SystemMessage::TaskStarted { .. }
            | SystemMessage::TaskProgress { .. }
            | SystemMessage::TaskNotification { .. }
            | SystemMessage::TaskUpdated { .. }
            | SystemMessage::Unknown => {
                panic!("from_system_init called with non-Init system message: {msg:?}");
            }
        }
    }
}

/// Events delivered from the CC session to the consumer.
pub enum SessionEvent {
    /// CC session initialized. Contains session metadata.
    Initialized(SessionInfo),
    /// Assistant message (complete turn).
    AssistantMessage(crate::protocol::incoming::AssistantMessage),
    /// Stream event (partial token).
    StreamEvent(crate::protocol::incoming::StreamEventMessage),
    /// User/tool result message.
    ToolResult(crate::protocol::incoming::UserMessage),
    /// Approval required. Consumer must send decision via the oneshot.
    ApprovalRequired(ApprovalRequest),
    /// Pending approval cancelled by CC.
    ApprovalCancelled { request_id: String },
    /// Rate limit event.
    RateLimit(crate::protocol::incoming::RateLimitEventMessage),
    /// CC status change (e.g. "compacting" during `/compact`).
    StatusChange {
        status: Option<String>,
        compact_result: Option<String>,
    },
    /// Compact boundary — compaction completed. Carries metadata about the compaction.
    CompactBoundary {
        metadata: Option<crate::protocol::incoming::CompactMetadata>,
    },
    /// Turn complete (CC emitted a `result` message). The session stays alive —
    /// CC is waiting for the next user message on stdin.
    TurnCompleted(crate::protocol::incoming::ResultMessage),
    /// Session died unexpectedly (process exit, broken pipe, etc.)
    Died(CcError),
    /// CC sent something we couldn't parse. Not an error — probably a protocol
    /// upgrade. Logged + alerted; raw line preserved for diagnosis.
    UnrecognizedMessage { raw_line: String },
}

impl std::fmt::Debug for SessionEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Initialized(info) => write!(f, "Initialized({:?})", info.session_id),
            Self::AssistantMessage(_) => write!(f, "AssistantMessage(...)"),
            Self::StreamEvent(_) => write!(f, "StreamEvent(...)"),
            Self::ToolResult(_) => write!(f, "ToolResult(...)"),
            Self::ApprovalRequired(req) => {
                write!(f, "ApprovalRequired({})", req.request_id)
            }
            Self::ApprovalCancelled { request_id } => {
                write!(f, "ApprovalCancelled({request_id})")
            }
            Self::RateLimit(_) => write!(f, "RateLimit(...)"),
            Self::StatusChange {
                status,
                compact_result,
            } => {
                write!(
                    f,
                    "StatusChange(status={status:?}, compact_result={compact_result:?})"
                )
            }
            Self::CompactBoundary { .. } => write!(f, "CompactBoundary(...)"),
            Self::TurnCompleted(_) => write!(f, "TurnCompleted(...)"),
            Self::Died(e) => write!(f, "Died({e})"),
            Self::UnrecognizedMessage { raw_line } => {
                write!(f, "UnrecognizedMessage({raw_line})")
            }
        }
    }
}

/// A live CC session. Holds the subprocess handle and communication channels.
pub struct CcSession {
    /// Channel for sending outgoing messages to the stdin writer task.
    outgoing_tx: mpsc::Sender<OutgoingEnvelope>,
    /// Whether the session is still alive.
    alive: Arc<AtomicBool>,
    /// Set to true before dropping the session to indicate intentional shutdown.
    /// The reader task checks this on EOF to avoid firing spurious alerts.
    shutting_down: Arc<AtomicBool>,
    /// The child process handle. Held here so kill_on_drop works.
    _child: Child,
    /// Stdout reader and stdin writer task handles. `None` in test sessions
    /// that never spawn the real I/O tasks. Retained so the bridge watchdog can
    /// tell whether the session's I/O is still alive — the `alive` flag alone
    /// misses the case where the reader task exits via its "consumer gone"
    /// branch (event loop dropped the receiver) without clearing `alive`.
    io_tasks: Option<(tokio::task::JoinHandle<()>, tokio::task::JoinHandle<()>)>,
    /// Test-only: when true, `send_message_acked` fires the flush-ack with
    /// `Ok(())` immediately after enqueue (no writer task needed). The ack
    /// receiver returned to the caller is pre-resolved. This prevents
    /// `persist_broadcast_send`'s ack await from deadlocking in test harnesses
    /// that have no stdin-writer task draining the channel.
    ///
    /// Production sessions always have `auto_ack = false`.
    #[cfg(any(test, feature = "testutils"))]
    auto_ack: bool,
}

/// The resolved command to spawn: either a bare `claude` process or `podman run ... claude`.
#[derive(Debug, PartialEq)]
pub(crate) struct SpawnCommand {
    /// The program to execute ("claude" or "podman").
    pub program: String,
    /// Arguments to the program.
    pub args: Vec<String>,
    /// Working directory (only set for bare-process mode; containerized uses -w).
    pub cwd: Option<PathBuf>,
    /// Extra environment variables for the process (bare-process mode only).
    pub env_vars: Vec<(String, String)>,
}

/// Build the CC CLI arguments common to both bare and containerized modes.
fn build_cc_args(config: &CcSessionConfig) -> Vec<String> {
    // `--permission-mode auto` lets CC's classifier auto-approve safe tool
    // calls; risky ones still fall back to `--permission-prompt-tool stdio`
    // and surface in Brenn's approval UI. See docs/designs/auto-mode-default.md.
    let mut args = vec![
        "--input-format".into(),
        "stream-json".into(),
        "--output-format".into(),
        "stream-json".into(),
        "--verbose".into(),
        "--include-partial-messages".into(),
        "--permission-prompt-tool".into(),
        "stdio".into(),
        "--permission-mode".into(),
        "auto".into(),
        "--model".into(),
        config.model.clone(),
    ];

    // `--add-dir` per mount expands CC's workspace-trust scope so reads/edits
    // on non-working-dir mounts don't trigger approval prompts.
    for dir in &config.add_dirs {
        args.push("--add-dir".into());
        args.push(dir.display().to_string());
    }

    if let Some(ref tools) = config.allowed_tools {
        args.push("--tools".into());
        args.push(tools.join(","));
    }

    if let Some(ref mcp) = config.mcp_config {
        args.push("--mcp-config".into());
        args.push(mcp.to_string());
    }

    if let Some(ref session_id) = config.resume_session_id {
        args.push("--resume".into());
        args.push(session_id.clone());
    }

    args.extend(config.cc_extra_args.iter().cloned());

    args
}

/// Build the full spawn command from a session config.
///
/// Separated from `spawn()` so the command construction logic can be tested
/// without actually executing anything.
pub(crate) fn build_spawn_command(config: &CcSessionConfig) -> SpawnCommand {
    let cc_args = build_cc_args(config);

    if let Some(ref container) = config.container {
        let container_name = format!("brenn-{}-{}", config.app_slug, config.container_name_suffix);

        let mut podman_args = container.base_podman_args();

        let extra_flags: Vec<String> = vec![
            "-i".into(),
            "--name".into(),
            container_name,
            "--label".into(),
            "brenn-managed=true".to_string(),
        ];
        ContainerSpawnConfig::insert_podman_flags(&mut podman_args, &extra_flags);

        // Command: claude + its args.
        podman_args.push("claude".into());
        podman_args.extend(cc_args);

        SpawnCommand {
            program: "podman".into(),
            args: podman_args,
            cwd: None,
            env_vars: vec![],
        }
    } else {
        SpawnCommand {
            program: "claude".into(),
            args: cc_args,
            cwd: Some(config.cwd.clone()),
            env_vars: config.env_vars.clone(),
        }
    }
}

impl CcSession {
    /// Spawn a CC subprocess and perform the initialization handshake.
    ///
    /// Returns `(session, init_ack_info)` — the session handle and metadata
    /// extracted from the init ack (e.g., available models). The caller is
    /// responsible for draining the event receiver.
    pub async fn spawn(
        config: CcSessionConfig,
        event_tx: mpsc::Sender<SessionEvent>,
    ) -> Result<(Self, InitAckInfo), CcError> {
        let cmd = build_spawn_command(&config);

        let mut command = Command::new(&cmd.program);
        command
            .args(&cmd.args)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);

        if let Some(ref cwd) = cmd.cwd {
            command.current_dir(cwd);
        }
        if !cmd.env_vars.is_empty() {
            command.envs(cmd.env_vars.iter().map(|(k, v)| (k.as_str(), v.as_str())));
        }

        let mut child = command.spawn().map_err(CcError::SpawnFailed)?;

        let stdout = child.stdout.take().expect("stdout was piped");
        let stdin = child.stdin.take().expect("stdin was piped");
        let stderr = child.stderr.take().expect("stderr was piped");

        // Channel for outgoing messages to the stdin writer task.
        let (outgoing_tx, outgoing_rx) = mpsc::channel::<OutgoingEnvelope>(64);

        // Channel for the init handshake. The stdout reader task sends init
        // messages here; we drain them to complete the handshake.
        let (init_tx, mut init_rx) = mpsc::channel::<CcIncoming>(16);

        let alive = Arc::new(AtomicBool::new(true));
        let shutting_down = config
            .shutting_down
            .unwrap_or_else(|| Arc::new(AtomicBool::new(false)));

        // Start background tasks.
        tasks::spawn_stderr_drain(stderr);
        let writer_handle =
            tasks::spawn_stdin_writer(stdin, outgoing_rx, config.transcript.clone());
        let reader_handle = tasks::spawn_stdout_reader(
            stdout,
            event_tx.clone(),
            init_tx,
            outgoing_tx.clone(),
            config.transcript.clone(),
            config.alert_dispatcher.clone(),
            alive.clone(),
            shutting_down.clone(),
        );

        // Send initialization request (fire-and-forget; no ack needed for the init handshake).
        let init_msg = protocol::initialize(config.hooks, None);
        outgoing_tx
            .send(OutgoingEnvelope {
                msg: init_msg,
                ack: None,
            })
            .await
            .map_err(|_| CcError::SendFailed)?;

        // Wait for control_response (init ack).
        let init_timeout = tokio::time::Duration::from_secs(30);
        let init_ack = tokio::time::timeout(init_timeout, async {
            loop {
                match init_rx.recv().await {
                    Some(CcIncoming::ControlResponse { response }) => {
                        if response.subtype == "error" {
                            return Err(CcError::InitFailed(
                                response.error.unwrap_or_else(|| "unknown error".into()),
                            ));
                        }
                        // Extract available models from the init ack payload.
                        let init_ack_info = parse_init_ack_info(&response);
                        return Ok(init_ack_info);
                    }
                    Some(_) => {
                        // Ignore non-init messages during handshake.
                        continue;
                    }
                    None => {
                        return Err(CcError::InitFailed(
                            "CC process exited during initialization".into(),
                        ));
                    }
                }
            }
        })
        .await;

        let init_ack_info = match init_ack {
            Ok(Ok(info)) => info,
            Ok(Err(e)) => return Err(e),
            Err(_) => return Err(CcError::InitTimeout),
        };

        Ok((
            Self {
                outgoing_tx,
                alive,
                shutting_down,
                _child: child,
                io_tasks: Some((reader_handle, writer_handle)),
                #[cfg(any(test, feature = "testutils"))]
                auto_ack: false,
            },
            init_ack_info,
        ))
    }

    /// Send a user message to CC (fire-and-forget; no flush ack).
    pub async fn send_message(&self, text: &str) -> Result<(), CcError> {
        let msg = protocol::user_message(text);
        self.outgoing_tx
            .send(OutgoingEnvelope { msg, ack: None })
            .await
            .map_err(|_| CcError::SendFailed)
    }

    /// Send a user message to CC and return a receiver that resolves after the
    /// message has been flushed to CC's stdin (or on flush failure).
    ///
    /// The caller must await the returned `Receiver` **after** dropping any
    /// `session.lock()` guards it may hold (see design §2.6): FIFO order is
    /// fixed at `outgoing_tx.send` return; releasing the lock before the await
    /// cannot reorder stdin writes. `RecvError` on the receiver means the
    /// writer task exited before firing the ack — treat as flush failure (row
    /// stays parked).
    ///
    /// In test sessions with `auto_ack = true` (set by `recording_for_test`), the
    /// ack is fired immediately after enqueue and the returned receiver is
    /// pre-resolved. This prevents `persist_broadcast_send`'s ack await from
    /// deadlocking in harnesses that have no stdin-writer task.
    pub async fn send_message_acked(
        &self,
        text: &str,
    ) -> Result<oneshot::Receiver<Result<(), TransportError>>, CcError> {
        let (ack_tx, ack_rx) = oneshot::channel();

        #[cfg(any(test, feature = "testutils"))]
        if self.auto_ack {
            // Fire the ack immediately — no writer task in test mode. The envelope
            // is placed in the channel with ack: None (fire-and-forget) so that
            // test receivers still see the message without a dangling ack sender.
            // The receiver is pre-resolved and the caller's ack_rx.await returns
            // immediately with Ok(()).
            let _ = ack_tx.send(Ok(()));
            let msg = protocol::user_message(text);
            self.outgoing_tx
                .send(OutgoingEnvelope { msg, ack: None })
                .await
                .map_err(|_| CcError::SendFailed)?;
            return Ok(ack_rx);
        }

        let msg = protocol::user_message(text);
        self.outgoing_tx
            .send(OutgoingEnvelope {
                msg,
                ack: Some(ack_tx),
            })
            .await
            .map_err(|_| CcError::SendFailed)?;
        Ok(ack_rx)
    }

    /// Send a pre-built outgoing message to CC (fire-and-forget; no flush ack).
    pub async fn send_outgoing(&self, msg: protocol::CcOutgoing) -> Result<(), CcError> {
        self.outgoing_tx
            .send(OutgoingEnvelope { msg, ack: None })
            .await
            .map_err(|_| CcError::SendFailed)
    }

    /// Send an interrupt to CC (stop current generation).
    pub async fn interrupt(&self) -> Result<(), CcError> {
        let msg = protocol::interrupt();
        self.outgoing_tx
            .send(OutgoingEnvelope { msg, ack: None })
            .await
            .map_err(|_| CcError::SendFailed)
    }

    /// Send a set_model control request to CC.
    pub async fn set_model(&self, model: &str) -> Result<(), CcError> {
        let msg = protocol::set_model(model);
        self.outgoing_tx
            .send(OutgoingEnvelope { msg, ack: None })
            .await
            .map_err(|_| CcError::SendFailed)
    }

    /// Check if the session is still alive.
    pub fn is_alive(&self) -> bool {
        self.alive.load(Ordering::Relaxed)
    }

    /// Whether the session's stdout reader and stdin writer tasks are both
    /// still running.
    ///
    /// Returns `true` when the I/O task handles are absent (test sessions that
    /// never spawned them) so this reads as "no evidence of dead I/O" rather
    /// than a false wedge signal. A production session whose reader task exited
    /// via the "consumer gone" branch (the event loop dropped the receiver)
    /// leaves `is_alive()` `true` but this `false` — the signal the watchdog
    /// needs to catch a wedged bridge.
    pub fn io_alive(&self) -> bool {
        match &self.io_tasks {
            Some((reader, writer)) => !reader.is_finished() && !writer.is_finished(),
            None => true,
        }
    }

    /// Signal that this session is being intentionally shut down.
    ///
    /// Call this before dropping the session to prevent the reader task from
    /// firing spurious "CC process died" alerts. The reader task checks this
    /// flag on EOF and suppresses alerts when set.
    pub fn mark_shutting_down(&self) {
        self.shutting_down.store(true, Ordering::SeqCst);
    }

    /// Check whether `shutting_down` has been set. Test-only.
    #[cfg(any(test, feature = "testutils"))]
    pub fn is_shutting_down(&self) -> bool {
        self.shutting_down.load(Ordering::SeqCst)
    }

    /// Return a clone of the `shutting_down` flag for external observation.
    ///
    /// Allows tests to check the flag after the `CcSession` has been dropped.
    #[cfg(any(test, feature = "testutils"))]
    pub fn shutting_down_flag(&self) -> Arc<AtomicBool> {
        self.shutting_down.clone()
    }

    /// Mark this session as dead (simulate a session that exited without clearing
    /// the `Option` wrapper). Use in tests that need to exercise the `is_alive()`
    /// guard without waiting for the reader task to terminate.
    #[cfg(any(test, feature = "testutils"))]
    pub fn mark_dead_for_test(&self) {
        self.alive.store(false, Ordering::SeqCst);
    }

    /// Shared implementation for test constructors.
    ///
    /// Spawns `sleep 60` so the child stays alive long enough for the test to
    /// inspect flags before drop. The child is killed on drop via `kill_on_drop`.
    /// Returns `(session, rx)`; callers decide whether to use or drop the receiver.
    #[cfg(any(test, feature = "testutils"))]
    fn new_for_test(
        channel_cap: usize,
        auto_ack: bool,
    ) -> (Self, mpsc::Receiver<OutgoingEnvelope>) {
        let child = Command::new("sleep")
            .arg("60")
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .expect("failed to spawn sleep for test");

        let (outgoing_tx, outgoing_rx) = mpsc::channel(channel_cap);

        let session = Self {
            outgoing_tx,
            alive: Arc::new(AtomicBool::new(true)),
            shutting_down: Arc::new(AtomicBool::new(false)),
            _child: child,
            io_tasks: None,
            auto_ack,
        };
        (session, outgoing_rx)
    }

    /// Create a `CcSession` with a live outgoing channel for recording sends.
    ///
    /// Returns `(session, rx)` where `rx` receives every `OutgoingEnvelope` sent via
    /// `send_message` / `send_outgoing` / `send_message_acked`. Use this in tests that
    /// need to assert on what was delivered to CC on the success path. The `.msg` field
    /// of each envelope holds the `CcOutgoing` message; `.ack` is always `None` for
    /// envelopes sent via `send_message_acked` (the ack is pre-fired with `Ok(())` so
    /// `persist_broadcast_send`'s ack await resolves immediately without a writer task).
    ///
    /// Internally sets `auto_ack = true`: `send_message_acked` fires the ack with
    /// `Ok(())` immediately after enqueue and places the envelope with `ack: None`
    /// into the channel, so test observers still see all messages without a dangling
    /// ack sender. This prevents the ack-await in `persist_broadcast_send` from
    /// deadlocking in test harnesses that have no stdin-writer task.
    ///
    /// Tests that need to exercise the ack-failure path (ack resolves `Err`) should
    /// drive the production `spawn_stdin_writer` instead and inject the failure there.
    ///
    /// Spawns `sleep 60` so the child stays alive long enough for the test to
    /// drain the channel and inspect flags before drop.
    #[cfg(any(test, feature = "testutils"))]
    pub fn recording_for_test() -> (Self, mpsc::Receiver<OutgoingEnvelope>) {
        Self::new_for_test(64, true)
    }

    /// Create a `CcSession` backed by a trivial subprocess (for unit tests).
    ///
    /// Spawns `sleep 60` so the child stays alive long enough for the test to
    /// inspect flags before drop. The child is killed on drop via `kill_on_drop`.
    /// Uses `auto_ack = false` — the outgoing channel is dropped immediately (cap 1)
    /// so any send fails with `SendFailed`, simulating a dead session.
    #[cfg(any(test, feature = "testutils"))]
    pub fn dummy_for_test() -> Self {
        Self::new_for_test(1, false).0
    }

    /// Create a `CcSession` whose I/O task handles are installed but whose reader
    /// task has already finished, so `io_alive()` returns `false` while
    /// `is_alive()` stays `true`.
    ///
    /// Reproduces the production wedge signature (reader exits via the "consumer
    /// gone" branch without clearing `alive`) so the watchdog's `!io_alive()`
    /// predicate can be exercised. The writer handle is a never-finishing task.
    #[cfg(any(test, feature = "testutils"))]
    pub async fn dummy_with_dead_io_for_test() -> Self {
        let mut session = Self::new_for_test(1, false).0;
        let reader = tokio::spawn(async {});
        while !reader.is_finished() {
            tokio::task::yield_now().await;
        }
        let writer = tokio::spawn(std::future::pending::<()>());
        session.io_tasks = Some((reader, writer));
        session
    }

    /// Create a `CcSession` whose `send_message_acked` enqueues messages and places
    /// the ack `Sender` into the channel (`ack: Some(tx)`) without firing it.
    ///
    /// Returns `(session, rx)` where `rx` receives every `OutgoingEnvelope`.
    /// Each envelope for an acked send carries `ack: Some(tx)` — the test controls
    /// when the ack fires by calling `tx.send(Ok(()))` (success) or
    /// `tx.send(Err(...))` (failure). This lets tests simulate an alive-but-stalled
    /// writer: the caller awaiting the ack blocks until the test releases the sender.
    ///
    /// Uses `auto_ack = false` with a full-capacity (64) channel so sends succeed
    /// immediately. The session is alive (`alive = true`).
    #[cfg(any(test, feature = "testutils"))]
    pub fn stalling_for_test() -> (Self, mpsc::Receiver<OutgoingEnvelope>) {
        Self::new_for_test(64, false)
    }
}

/// Extract available models from the init ack's response payload.
fn parse_init_ack_info(response: &protocol::incoming::ControlResponsePayload) -> InitAckInfo {
    let mut info = InitAckInfo::default();

    let Some(ref resp_value) = response.response else {
        return info;
    };

    if let Some(models_array) = resp_value.get("models").and_then(|v| v.as_array()) {
        for m in models_array {
            let value = m
                .get("value")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            let display_name = m
                .get("displayName")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            let description = m
                .get("description")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            if value.is_empty() {
                warn!("skipping model entry with missing/empty value: {m}");
                continue;
            }
            info.models.push(ModelOption {
                value,
                display_name,
                description,
            });
        }
    }

    info
}

#[cfg(test)]
mod tests {
    use super::*;
    use brenn_lib::config::ContainerSpawnConfig;
    use brenn_lib::obs::transcript::TranscriptWriter;

    /// Build a minimal CcSessionConfig for testing (bare process mode).
    fn bare_config() -> CcSessionConfig {
        let dir = tempfile::tempdir().unwrap();
        let transcript = Arc::new(TranscriptWriter::new(dir.path(), "test.log").unwrap());
        let (alert_dispatcher, _handle) = brenn_lib::obs::alerting::noop_alert_dispatcher();
        CcSessionConfig {
            model: "sonnet".into(),
            cwd: PathBuf::from("/home/user/src/myapp"),
            hooks: None,
            mcp_config: None,
            allowed_tools: None,
            resume_session_id: None,
            transcript,
            alert_dispatcher,
            container: None,
            app_slug: "myapp".into(),
            container_name_suffix: "conv42".into(),
            add_dirs: vec![],
            cc_extra_args: vec![],
            env_vars: vec![],
            shutting_down: None,
        }
    }

    /// Build a CcSessionConfig with container mode enabled.
    fn container_config() -> CcSessionConfig {
        let mut config = bare_config();
        config.container = Some(ContainerSpawnConfig {
            image: "brenn-cc:latest".into(),
            home_dir: PathBuf::from("/home/alice/.brenn-homes/myapp"),
            container_home: PathBuf::from("/home/user"),
            host_working_dir: PathBuf::from("/home/alice/src/myapp"),
            container_working_dir: PathBuf::from("/workspace/myapp"),
            working_dir_is_repo: false,
            repo_mounts: vec![],
            extra_mounts: vec![],
            extra_args: vec![],
        });
        config
    }

    #[tokio::test]
    async fn bare_process_command() {
        let config = bare_config();
        let cmd = build_spawn_command(&config);

        assert_eq!(cmd.program, "claude");
        assert_eq!(cmd.cwd, Some(PathBuf::from("/home/user/src/myapp")));

        // Must have the core flags.
        assert!(cmd.args.contains(&"--input-format".to_string()));
        assert!(cmd.args.contains(&"stream-json".to_string()));
        assert!(cmd.args.contains(&"--model".to_string()));
        assert!(cmd.args.contains(&"sonnet".to_string()));
    }

    #[tokio::test]
    async fn container_command_structure() {
        let config = container_config();
        let cmd = build_spawn_command(&config);

        assert_eq!(cmd.program, "podman");
        assert_eq!(cmd.cwd, None); // No host-side cwd for containerized.

        // Check podman subcommand and flags.
        assert_eq!(cmd.args[0], "run");
        assert!(cmd.args.contains(&"--rm".to_string()));
        assert!(cmd.args.contains(&"-i".to_string()));
        assert!(cmd.args.contains(&"--network=host".to_string()));

        // Container name.
        let name_idx = cmd.args.iter().position(|a| a == "--name").unwrap();
        assert_eq!(cmd.args[name_idx + 1], "brenn-myapp-conv42");

        // HOME env var.
        let env_idx = cmd.args.iter().position(|a| a == "-e").unwrap();
        assert_eq!(cmd.args[env_idx + 1], "HOME=/home/user");

        // Home dir volume mount.
        assert!(
            cmd.args
                .contains(&"/home/alice/.brenn-homes/myapp:/home/user:z".to_string())
        );

        // Working dir volume mount.
        assert!(
            cmd.args
                .contains(&"/home/alice/src/myapp:/workspace/myapp:z".to_string())
        );

        // Working dir inside container.
        let w_idx = cmd.args.iter().position(|a| a == "-w").unwrap();
        assert_eq!(cmd.args[w_idx + 1], "/workspace/myapp");

        // Image comes before "claude".
        let image_idx = cmd
            .args
            .iter()
            .position(|a| a == "brenn-cc:latest")
            .unwrap();
        let claude_idx = cmd.args.iter().position(|a| a == "claude").unwrap();
        assert!(
            image_idx < claude_idx,
            "image must come before claude binary"
        );

        // CC args come after "claude".
        assert!(cmd.args[claude_idx + 1..].contains(&"--model".to_string()));
        assert!(cmd.args[claude_idx + 1..].contains(&"sonnet".to_string()));
    }

    /// Containerized spawns must carry `--label brenn-managed=true` so stale-container
    /// cleanup at startup can find and remove stopped brenn containers without
    /// needing per-instance scoping.
    #[tokio::test]
    async fn container_command_carries_brenn_managed_label() {
        let config = container_config();
        let cmd = build_spawn_command(&config);

        // --label brenn-managed=true must appear before the image.
        let label_idx = cmd
            .args
            .iter()
            .position(|a| a == "brenn-managed=true")
            .expect("expected --label brenn-managed=true in args");
        assert_eq!(cmd.args[label_idx - 1], "--label");

        let image_idx = cmd
            .args
            .iter()
            .position(|a| a == "brenn-cc:latest")
            .unwrap();
        assert!(label_idx < image_idx, "--label must precede the image name");
    }

    /// Bare-process spawns produce a `claude` command with no podman wrapper.
    #[tokio::test]
    async fn bare_command_is_bare_claude() {
        let config = bare_config();
        let cmd = build_spawn_command(&config);
        assert_eq!(cmd.program, "claude");
        // No --label flag in bare-process args.
        assert!(!cmd.args.iter().any(|a| a == "--label"));
    }

    #[tokio::test]
    async fn container_extra_mounts_passed_through() {
        let mut config = container_config();
        config.container.as_mut().unwrap().extra_mounts = vec![
            "/data/shared:/mnt/shared:ro".into(),
            "/tmp/cache:/cache:Z".into(),
        ];
        let cmd = build_spawn_command(&config);

        // Each extra mount gets a -v flag, passed through verbatim.
        assert!(
            cmd.args
                .contains(&"/data/shared:/mnt/shared:ro".to_string())
        );
        assert!(cmd.args.contains(&"/tmp/cache:/cache:Z".to_string()));

        // -v flags for extra mounts appear before the image name.
        let image_idx = cmd
            .args
            .iter()
            .position(|a| a == "brenn-cc:latest")
            .unwrap();
        let mount_idx = cmd
            .args
            .iter()
            .position(|a| a == "/data/shared:/mnt/shared:ro")
            .unwrap();
        assert!(mount_idx < image_idx);
    }

    #[tokio::test]
    async fn container_extra_args_passed_through() {
        let mut config = container_config();
        config.container.as_mut().unwrap().extra_args =
            vec!["--memory=4g".into(), "--cpus=2".into()];
        let cmd = build_spawn_command(&config);

        assert!(cmd.args.contains(&"--memory=4g".to_string()));
        assert!(cmd.args.contains(&"--cpus=2".to_string()));

        // Extra args appear before the image name.
        let image_idx = cmd
            .args
            .iter()
            .position(|a| a == "brenn-cc:latest")
            .unwrap();
        let mem_idx = cmd.args.iter().position(|a| a == "--memory=4g").unwrap();
        assert!(mem_idx < image_idx);
    }

    #[tokio::test]
    async fn cc_args_with_tools_and_resume() {
        let mut config = bare_config();
        config.allowed_tools = Some(vec!["Read".into(), "Write".into()]);
        config.resume_session_id = Some("abc-123".into());
        let cmd = build_spawn_command(&config);

        let tools_idx = cmd.args.iter().position(|a| a == "--tools").unwrap();
        assert_eq!(cmd.args[tools_idx + 1], "Read,Write");

        let resume_idx = cmd.args.iter().position(|a| a == "--resume").unwrap();
        assert_eq!(cmd.args[resume_idx + 1], "abc-123");
    }

    #[tokio::test]
    async fn cc_args_with_mcp_config() {
        let mut config = bare_config();
        config.mcp_config = Some(serde_json::json!({"servers": {}}));
        let cmd = build_spawn_command(&config);

        let mcp_idx = cmd.args.iter().position(|a| a == "--mcp-config").unwrap();
        assert_eq!(cmd.args[mcp_idx + 1], r#"{"servers":{}}"#);
    }

    #[tokio::test]
    async fn cc_extra_args_appended() {
        let mut config = bare_config();
        config.cc_extra_args = vec!["--max-turns".into(), "50".into()];
        let cmd = build_spawn_command(&config);

        let idx = cmd.args.iter().position(|a| a == "--max-turns").unwrap();
        assert_eq!(cmd.args[idx + 1], "50");
        // Extra args come after the standard args.
        let model_idx = cmd.args.iter().position(|a| a == "--model").unwrap();
        assert!(idx > model_idx);
    }

    /// Locate `flag` in `args` (starting at `offset`) and return the value
    /// that immediately follows. Panics if the flag is absent — the caller's
    /// `expect` message names which flag was missing.
    fn flag_value<'a>(args: &'a [String], flag: &str, offset: usize) -> &'a str {
        let idx = args[offset..]
            .iter()
            .position(|a| a == flag)
            .unwrap_or_else(|| panic!("flag {flag} not found in args"));
        &args[offset + idx + 1]
    }

    #[tokio::test]
    async fn permission_mode_auto_is_default() {
        let bare_cmd = build_spawn_command(&bare_config());
        assert_eq!(flag_value(&bare_cmd.args, "--permission-mode", 0), "auto");

        let container_cmd = build_spawn_command(&container_config());
        let claude_idx = container_cmd
            .args
            .iter()
            .position(|a| a == "claude")
            .unwrap();
        assert_eq!(
            flag_value(&container_cmd.args, "--permission-mode", claude_idx + 1),
            "auto",
        );
    }

    #[test]
    fn session_info_carries_permission_mode() {
        use crate::protocol::incoming::SystemMessage;
        let msg = SystemMessage::Init {
            session_id: Some("sess-xyz".into()),
            tools: Some(vec!["Read".into()]),
            mcp_servers: Some(vec![]),
            model: Some("claude-sonnet-4".into()),
            cwd: Some("/tmp".into()),
            claude_code_version: Some("2.1.111".into()),
            permission_mode: Some(PermissionModeValue::Auto),
            extra: serde_json::Value::Object(Default::default()),
        };
        let info = SessionInfo::from_system_init(&msg);
        assert_eq!(info.permission_mode, Some(PermissionModeValue::Auto));
    }

    #[tokio::test]
    async fn add_dirs_emits_one_flag_per_entry() {
        let mut config = bare_config();
        config.add_dirs = vec![PathBuf::from("/repos/life"), PathBuf::from("/repos/docs")];
        let cmd = build_spawn_command(&config);

        let occurrences: Vec<usize> = cmd
            .args
            .iter()
            .enumerate()
            .filter(|(_, a)| a.as_str() == "--add-dir")
            .map(|(i, _)| i)
            .collect();
        assert_eq!(occurrences.len(), 2, "expected one --add-dir per entry");
        assert_eq!(cmd.args[occurrences[0] + 1], "/repos/life");
        assert_eq!(cmd.args[occurrences[1] + 1], "/repos/docs");
    }

    #[tokio::test]
    async fn add_dirs_empty_emits_no_flag() {
        let config = bare_config();
        let cmd = build_spawn_command(&config);
        assert!(!cmd.args.iter().any(|a| a == "--add-dir"));
    }

    // --- parse_init_ack_info tests ---

    fn make_control_response(
        response: Option<serde_json::Value>,
    ) -> protocol::incoming::ControlResponsePayload {
        protocol::incoming::ControlResponsePayload {
            subtype: "success".into(),
            request_id: Some("req_0".into()),
            response,
            error: None,
            extra: serde_json::Value::Object(Default::default()),
        }
    }

    #[test]
    fn parse_init_ack_info_extracts_models() {
        let resp = make_control_response(Some(serde_json::json!({
            "models": [
                {"value": "default", "displayName": "Default", "description": "The default model"},
                {"value": "sonnet", "displayName": "Sonnet", "description": "Fast"},
                {"value": "haiku", "displayName": "Haiku", "description": "Fastest"},
            ]
        })));
        let info = parse_init_ack_info(&resp);
        assert_eq!(info.models.len(), 3);
        assert_eq!(info.models[0].value, "default");
        assert_eq!(info.models[0].display_name, "Default");
        assert_eq!(info.models[1].value, "sonnet");
        assert_eq!(info.models[2].value, "haiku");
        assert_eq!(info.models[2].description, "Fastest");
    }

    #[test]
    fn parse_init_ack_info_no_response() {
        let resp = make_control_response(None);
        let info = parse_init_ack_info(&resp);
        assert!(info.models.is_empty());
    }

    #[test]
    fn parse_init_ack_info_no_models_key() {
        let resp = make_control_response(Some(serde_json::json!({
            "commands": []
        })));
        let info = parse_init_ack_info(&resp);
        assert!(info.models.is_empty());
    }

    #[test]
    fn parse_init_ack_info_skips_entries_without_value() {
        let resp = make_control_response(Some(serde_json::json!({
            "models": [
                {"displayName": "Mystery", "description": "No value field"},
                {"value": "", "displayName": "Empty", "description": "Empty value"},
                {"value": "sonnet", "displayName": "Sonnet", "description": "Good"},
            ]
        })));
        let info = parse_init_ack_info(&resp);
        assert_eq!(info.models.len(), 1);
        assert_eq!(info.models[0].value, "sonnet");
    }

    #[tokio::test]
    async fn bare_command_propagates_env_vars() {
        let mut config = bare_config();
        config.env_vars = vec![
            (
                "GRAF_MANIFEST".to_string(),
                "/home/user/.brenn/manifest.toml".to_string(),
            ),
            ("CUSTOM_VAR".to_string(), "custom_value".to_string()),
        ];
        let cmd = build_spawn_command(&config);
        assert_eq!(cmd.program, "claude");
        assert_eq!(cmd.env_vars.len(), 2);
        assert_eq!(cmd.env_vars[0].0, "GRAF_MANIFEST");
        assert_eq!(cmd.env_vars[1].1, "custom_value");
    }

    #[tokio::test]
    async fn container_command_has_empty_env_vars() {
        let config = container_config();
        let cmd = build_spawn_command(&config);
        assert_eq!(cmd.program, "podman");
        assert!(
            cmd.env_vars.is_empty(),
            "containerized mode should have empty env_vars (injected as podman -e flags instead)"
        );
    }
}
