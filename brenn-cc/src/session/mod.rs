pub mod approval;
pub mod tasks;

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use brenn_lib::config::{ContainerSpawnConfig, container_rm_args};
use brenn_obs::alerting::{AlertDispatcher, AlertSeverity};
use brenn_obs::transcript::TranscriptWriter;
use brenn_ws_types::PermissionModeValue;
use tokio::process::{Child, Command};
use tokio::sync::{mpsc, oneshot};

use tracing::{debug, error, info, warn};

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
    /// Pre-created **per-session** shutdown flag for the reader task to check
    /// on EOF.
    ///
    /// If provided, `spawn()` uses this instead of creating its own. This lets
    /// the caller hold a clone and set it when the spawn is cancelled
    /// mid-init-handshake — before a `CcSession` is ever constructed.
    ///
    /// Must never be a flag shared across sessions: `mark_shutting_down()`
    /// writes through it, so a shared Arc would let one session's teardown
    /// suppress every other session's death alert. Use `server_shutting_down`
    /// for the process-wide flag.
    ///
    /// Leave `None` for normal spawns; `spawn()` will create its own.
    pub shutting_down: Option<Arc<AtomicBool>>,
    /// The process-wide shutdown flag, read-only from the session's
    /// perspective. The reader task checks it alongside the per-session flag,
    /// so a session that EOFs while the whole server is going down does not
    /// page. Nothing in the session ever writes it.
    pub server_shutting_down: Option<Arc<AtomicBool>>,
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
    /// The child process handle plus the container it owns. Held here so
    /// `kill_on_drop` and the container removal in `CcChild::drop` both run
    /// when the session is discarded.
    _child: CcChild,
    /// Stdout reader and stdin writer task handles. `None` in test sessions
    /// that never spawn the real I/O tasks. Retained so the bridge watchdog can
    /// tell whether the session's I/O is still alive — the `alive` flag alone
    /// misses the case where the reader task exits via its "consumer gone"
    /// branch (event loop dropped the receiver) without clearing `alive`.
    io_tasks: Option<(tokio::task::JoinHandle<()>, tokio::task::JoinHandle<()>)>,
    /// Rolling tail of the child's stderr, attached to death reports.
    stderr_tail: tasks::StderrTail,
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
    /// Name of the container this command creates. `None` for bare-process mode.
    pub container_name: Option<String>,
}

/// How long a failing spawn waits to reap the child for its exit status.
const CHILD_REAP_TIMEOUT: tokio::time::Duration = tokio::time::Duration::from_secs(5);

/// How long the EOF failure path waits for the stderr drain to finish before
/// snapshotting the tail. The drain ends promptly once the child is gone; the
/// bound only covers a child whose stderr pipe outlives its stdout.
const STDERR_DRAIN_TIMEOUT: tokio::time::Duration = tokio::time::Duration::from_secs(2);

const PODMAN: &str = "podman";

/// Child-process handle that also owns the container the child started.
///
/// For a containerized app the child is the `podman run` attach client, not the
/// container: SIGKILLing it only closes the container's stdin, which a wedged
/// CC never reads. Removing the container explicitly on drop is what makes
/// teardown work for exactly the sessions teardown exists for.
struct CcChild {
    /// The spawned process, configured with `kill_on_drop(true)`. Drop delivers
    /// the SIGKILL; the failure paths reap it first for its exit status.
    child: Child,
    /// Container to remove on drop. `None` for bare apps and test sessions.
    container_name: Option<String>,
    /// The binary to invoke for the removal — the same program that started the
    /// container, so a spawn pointed at a stand-in removes through that
    /// stand-in too. Unused when `container_name` is `None`.
    podman_program: String,
}

impl CcChild {
    fn new(child: Child, container_name: Option<String>, podman_program: String) -> Self {
        Self {
            child,
            container_name,
            podman_program,
        }
    }

    /// Wrap a child that owns no container.
    fn bare(child: Child) -> Self {
        Self::new(child, None, PODMAN.to_string())
    }

    /// Reap the child and return its exit code, waiting at most `CHILD_REAP_TIMEOUT`.
    ///
    /// Only called once the child's stdout is at EOF, so the wait is expected to
    /// resolve immediately. `None` means the child could not be reaped in time
    /// or exited on a signal.
    async fn wait_for_exit_code(&mut self) -> Option<i32> {
        match tokio::time::timeout(CHILD_REAP_TIMEOUT, self.child.wait()).await {
            Ok(Ok(status)) => status.code(),
            Ok(Err(e)) => {
                debug!(error = %e, "failed to reap CC child after stdout EOF");
                None
            }
            Err(_) => {
                debug!("CC child did not exit within the reap timeout");
                None
            }
        }
    }
}

impl Drop for CcChild {
    /// Remove the container synchronously, before the `Child` field's own drop
    /// SIGKILLs the podman client.
    ///
    /// Blocking is deliberate: container names are stable per conversation, so
    /// a removal that outlived this drop could delete the *next* session's
    /// container. Waiting here gives every in-process respawn a happens-before
    /// edge, and also reaps the `podman rm` process.
    fn drop(&mut self) {
        let Some(name) = self.container_name.clone() else {
            return;
        };
        let program = self.podman_program.clone();
        let run = || {
            std::process::Command::new(&program)
                .args(container_rm_args(std::slice::from_ref(&name)))
                .output()
        };
        // On a multi-thread runtime hand the block off to a blocking thread so
        // the worker keeps serving other tasks; `block_in_place` still returns
        // only once the removal is done, preserving the happens-before edge.
        // Anywhere else (current-thread runtime, plain thread, process
        // teardown) run it directly.
        let result = match tokio::runtime::Handle::try_current() {
            Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
                tokio::task::block_in_place(run)
            }
            _ => run(),
        };
        match result {
            // The removal runs before the `Child` field's drop SIGKILLs the
            // podman client, so on an ordinary teardown the container has not yet
            // been given its stdin EOF and is still up: podman echoes the name it
            // removed and that is the normal case, not an anomaly. Empty stdout
            // means the container was already gone — CC exited on its own and
            // `--rm` reaped it, which `--ignore` turns into a success. Anomalous
            // liveness is signalled by the pre-spawn reclaim, not here.
            Ok(out) if out.status.success() => {
                if String::from_utf8_lossy(&out.stdout).trim().is_empty() {
                    debug!(container = %name, "no container to remove on session teardown");
                } else {
                    info!(container = %name, "removed CC container on session teardown");
                }
            }
            Ok(out) => {
                error!(
                    container = %name,
                    code = ?out.status.code(),
                    stderr = %String::from_utf8_lossy(&out.stderr).trim(),
                    "failed to remove CC container on session teardown"
                );
            }
            Err(e) => {
                error!(
                    container = %name,
                    error = %e,
                    "failed to run `podman rm` on session teardown"
                );
            }
        }
    }
}

/// Force-remove anything still holding `name` before a spawn claims it.
///
/// All spawns for a conversation are serialized under the per-conversation wake
/// lock, and teardown's own removal completes before the session is
/// deregistered, so a container still holding this name here is unowned by
/// definition — its bridge is gone and it can never do useful work again.
/// Removing it is recovery, but it is never silent: a reclaim that actually
/// removed something means a teardown failed.
async fn reclaim_container_name(
    name: &str,
    podman_program: &str,
    alert_dispatcher: &AlertDispatcher,
) -> Result<(), CcError> {
    let output = Command::new(podman_program)
        .args(container_rm_args(std::slice::from_ref(&name)))
        .output()
        .await
        .map_err(|e| {
            CcError::ContainerReclaimFailed(format!("failed to run `podman rm` for {name}: {e}"))
        })?;

    if !output.status.success() {
        return Err(CcError::ContainerReclaimFailed(format!(
            "`podman rm` for {name} exited {:?}: {}",
            output.status.code(),
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }

    // podman prints the removed container's name on stdout; a no-op `--ignore`
    // removal prints nothing.
    if String::from_utf8_lossy(&output.stdout).trim().is_empty() {
        debug!(container = %name, "no container held the name before spawn");
        return Ok(());
    }

    error!(
        container = %name,
        "orphaned CC container reclaimed — a previous teardown failed to remove it"
    );
    alert_dispatcher.alert(
        AlertSeverity::Warning,
        "Orphaned CC container reclaimed".to_string(),
        format!(
            "container {name} was still running before this spawn — \
             a previous teardown failed to remove it"
        ),
    );
    Ok(())
}

/// Why the init handshake did not complete, before enrichment with the child's
/// exit status and stderr tail.
enum InitFailure {
    /// CC answered the initialize request with an error. CC is still alive.
    Reported(String),
    /// CC's stdout closed before the init ack arrived.
    Eof,
}

/// Wait for the stderr drain to finish (bounded) and return the tail, so a
/// failure report carries everything the child wrote.
///
/// Only for the paths where the child is known to be gone. A live child holds
/// its stderr pipe open and the drain cannot finish, so those paths snapshot
/// the tail directly instead of waiting out the deadline.
async fn drained_stderr_tail(
    tail: &tasks::StderrTail,
    handle: tokio::task::JoinHandle<()>,
) -> Vec<String> {
    match tokio::time::timeout(STDERR_DRAIN_TIMEOUT, handle).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => debug!(error = %e, "stderr drain task ended abnormally"),
        Err(_) => debug!("stderr drain unfinished before deadline; tail may be incomplete"),
    }
    tail.snapshot()
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
            container_name.clone(),
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
            container_name: Some(container_name),
        }
    } else {
        SpawnCommand {
            program: "claude".into(),
            args: cc_args,
            cwd: Some(config.cwd.clone()),
            env_vars: config.env_vars.clone(),
            container_name: None,
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
        Self::spawn_inner(config, event_tx, None).await
    }

    /// `spawn()` with an optional replacement for the program to execute.
    ///
    /// Tests point the override at a stub that writes to stderr and exits with a
    /// known code; production always passes `None`.
    async fn spawn_inner(
        config: CcSessionConfig,
        event_tx: mpsc::Sender<SessionEvent>,
        program_override: Option<String>,
    ) -> Result<(Self, InitAckInfo), CcError> {
        let mut cmd = build_spawn_command(&config);
        if let Some(program) = program_override {
            cmd.program = program;
        }

        // In containerized mode the program being spawned *is* podman, so it is
        // also what removes the container — one spelling, and a test override
        // reaches the removal paths as well.
        let podman_program = cmd.program.clone();

        // Claim the container name before `podman run` can collide with an
        // orphan holding it.
        if let Some(ref name) = cmd.container_name {
            reclaim_container_name(name, &podman_program, &config.alert_dispatcher).await?;
        }

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

        // Wrap before the init handshake so every early `Err` return below tears
        // the container down through the same drop.
        let mut child = CcChild::new(child, cmd.container_name, podman_program);

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
        let (stderr_tail, stderr_handle) = tasks::spawn_stderr_drain(stderr);
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
            config.server_shutting_down.clone(),
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
                            return Err(InitFailure::Reported(
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
                        return Err(InitFailure::Eof);
                    }
                }
            }
        })
        .await;

        let init_ack_info = match init_ack {
            Ok(Ok(info)) => info,
            Ok(Err(InitFailure::Eof)) => {
                // Stdout is closed, so the child is gone or going: reap it for
                // its exit code. For a containerized app this is the `podman
                // run` client's status, which is what carries a name conflict.
                let exit_status = child.wait_for_exit_code().await;
                return Err(CcError::InitFailed {
                    reason: "CC process exited during initialization".into(),
                    exit_status,
                    stderr_tail: drained_stderr_tail(&stderr_tail, stderr_handle).await,
                });
            }
            // CC is still alive on both of the paths below and is holding its
            // stderr pipe open, so the drain task cannot finish: snapshot what
            // it has read rather than waiting out the drain deadline.
            Ok(Err(InitFailure::Reported(reason))) => {
                return Err(CcError::InitFailed {
                    reason,
                    exit_status: None,
                    stderr_tail: stderr_tail.snapshot(),
                });
            }
            Err(_) => {
                return Err(CcError::InitTimeout {
                    stderr_tail: stderr_tail.snapshot(),
                });
            }
        };

        Ok((
            Self {
                outgoing_tx,
                alive,
                shutting_down,
                _child: child,
                io_tasks: Some((reader_handle, writer_handle)),
                stderr_tail,
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

    /// The most recent lines the child wrote to stderr, oldest first.
    ///
    /// Best effort: empty when the child wrote nothing, and frozen at whatever
    /// was read if the drain stopped early (non-UTF-8 output).
    pub fn stderr_tail(&self) -> Vec<String> {
        self.stderr_tail.snapshot()
    }

    /// Seed the stderr tail of a test session, which has no child writing to it.
    ///
    /// Lets the server-side death and wedge paths be exercised with a non-empty
    /// tail, which is the only state in which they add it to their reports.
    #[cfg(any(test, feature = "testutils"))]
    pub fn push_stderr_line_for_test(&self, line: &str) {
        self.stderr_tail.push(line.to_string());
    }

    /// Signal that **this** session is being intentionally shut down.
    ///
    /// Call this before dropping the session to prevent the reader task from
    /// firing spurious "CC process died" alerts. The reader task checks this
    /// flag on EOF and suppresses alerts when set. The flag is per-session, so
    /// reaping one conversation leaves every other session's death alert armed.
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
            _child: CcChild::bare(child),
            io_tasks: None,
            stderr_tail: tasks::StderrTail::new(),
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

    /// A `dummy_for_test` session that owns a container, so dropping it runs
    /// `<podman_program> rm ...` and blocks until that program exits.
    ///
    /// Point `podman_program` at a script the test controls to make the teardown
    /// observably slow, which is what lets callers assert on what is and is not
    /// held while a container teardown is in flight.
    #[cfg(any(test, feature = "testutils"))]
    pub fn dummy_with_container_for_test(podman_program: &str, container_name: &str) -> Self {
        let mut session = Self::new_for_test(1, false).0;
        session._child.container_name = Some(container_name.to_string());
        session._child.podman_program = podman_program.to_string();
        session
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
    use brenn_obs::transcript::TranscriptWriter;

    /// Build a minimal CcSessionConfig for testing (bare process mode).
    fn bare_config() -> CcSessionConfig {
        let dir = tempfile::tempdir().unwrap();
        let transcript = Arc::new(TranscriptWriter::new(dir.path(), "test.log").unwrap());
        let (alert_dispatcher, _handle) = brenn_obs::alerting::noop_alert_dispatcher();
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
            server_shutting_down: None,
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

    // --- container teardown tests ---

    #[tokio::test]
    async fn container_command_carries_container_name() {
        let cmd = build_spawn_command(&container_config());
        assert_eq!(cmd.container_name.as_deref(), Some("brenn-myapp-conv42"));
    }

    #[tokio::test]
    async fn bare_command_has_no_container_name() {
        let cmd = build_spawn_command(&bare_config());
        assert_eq!(cmd.container_name, None);
    }

    /// Serializes every test in this module that forks a child process.
    ///
    /// Exec of a file that any process holds open for writing fails with
    /// `ETXTBSY`. A child forked while another thread is writing a shim inherits
    /// that write descriptor until it execs, so a shim written and run
    /// concurrently with unrelated process spawns intermittently fails to start.
    /// Holding this across the write *and* the run keeps that window empty.
    static SUBPROCESS_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    async fn subprocess_guard() -> tokio::sync::MutexGuard<'static, ()> {
        SUBPROCESS_LOCK.lock().await
    }

    fn write_script(dir: &std::path::Path, name: &str, body: &str) -> PathBuf {
        use std::os::unix::fs::PermissionsExt;
        let path = dir.join(name);
        std::fs::write(&path, format!("#!/bin/sh\n{body}")).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        path
    }

    /// A shell script that records its argv, one argument per line, to
    /// `marker`. Returned path is executable and usable as a `podman` stand-in.
    fn write_recording_shim(dir: &std::path::Path, marker: &std::path::Path) -> PathBuf {
        write_script(
            dir,
            "podman-shim",
            &format!("printf '%s\\n' \"$@\" > '{}'\n", marker.display()),
        )
    }

    /// A `podman` stand-in that emits fixed output and exits with `code`.
    fn write_podman_stub(dir: &std::path::Path, stdout: &str, stderr: &str, code: i32) -> PathBuf {
        let mut body = String::new();
        if !stdout.is_empty() {
            body.push_str(&format!("echo '{stdout}'\n"));
        }
        if !stderr.is_empty() {
            body.push_str(&format!("echo '{stderr}' >&2\n"));
        }
        body.push_str(&format!("exit {code}\n"));
        write_script(dir, "podman-stub", &body)
    }

    /// Run the reclaim against `stub` and report its result plus the number of
    /// alerts that actually reached the alerter.
    ///
    /// Dispatch is a `try_send` onto a channel a background task drains, so the
    /// count is only meaningful after every dispatcher clone is dropped and the
    /// drainer has run to completion. A yield proves nothing — an assertion of
    /// zero alerts would pass simply because the drainer never got there.
    async fn reclaim_with_stub(stub: &std::path::Path) -> (Result<(), CcError>, u32) {
        let _guard = subprocess_guard().await;
        let (dispatcher, count, drainer) = brenn_obs::alerting::make_counting_alerter();
        let result = reclaim_container_name(
            "brenn-myapp-conv42",
            &stub.display().to_string(),
            &dispatcher,
        )
        .await;
        drop(dispatcher);
        drainer.await.expect("alert drainer panicked");
        (result, count.load(Ordering::SeqCst))
    }

    /// The common case: nothing held the name. podman prints nothing, so the
    /// reclaim must stay quiet — no alert, no error.
    #[tokio::test]
    async fn reclaim_with_no_orphan_is_silent() {
        let dir = tempfile::tempdir().unwrap();
        let stub = write_podman_stub(dir.path(), "", "", 0);
        let (result, alerts) = reclaim_with_stub(&stub).await;
        assert!(result.is_ok(), "{result:?}");
        assert_eq!(alerts, 0, "a no-op reclaim must not alert");
    }

    /// podman echoes the name of a container it actually removed. That means a
    /// previous teardown failed, which is exactly what must not be silent.
    #[tokio::test]
    async fn reclaim_of_orphan_alerts() {
        let dir = tempfile::tempdir().unwrap();
        let stub = write_podman_stub(dir.path(), "brenn-myapp-conv42", "", 0);
        let (result, alerts) = reclaim_with_stub(&stub).await;
        assert!(result.is_ok(), "{result:?}");
        assert_eq!(alerts, 1, "a reclaimed orphan must fire exactly one alert");
    }

    /// A reclaim that podman rejects abandons the spawn carrying podman's own
    /// error text, rather than running into the name conflict anyway.
    #[tokio::test]
    async fn reclaim_failure_carries_stderr() {
        let dir = tempfile::tempdir().unwrap();
        let stub = write_podman_stub(dir.path(), "", "cannot remove container in use", 1);
        let (result, alerts) = reclaim_with_stub(&stub).await;
        match result {
            Err(CcError::ContainerReclaimFailed(msg)) => {
                assert!(msg.contains("cannot remove container in use"), "{msg}");
                assert!(msg.contains("brenn-myapp-conv42"), "{msg}");
            }
            other => panic!("expected ContainerReclaimFailed, got {other:?}"),
        }
        assert_eq!(alerts, 0, "a failed reclaim reports through the error");
    }

    fn spawn_sleeper() -> Child {
        Command::new("sleep")
            .arg("60")
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .expect("failed to spawn sleep for test")
    }

    /// Dropping a `CcChild` that owns a container must run the removal command
    /// **and wait for it**. Reading the marker immediately after `drop` returns
    /// asserts the happens-before edge that the pre-spawn reclaim's safety
    /// argument depends on: no removal can slip past the next spawn of the same
    /// conversation.
    #[tokio::test]
    async fn cc_child_drop_removes_container_before_returning() {
        let _guard = subprocess_guard().await;
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("podman-argv");
        let shim = write_recording_shim(dir.path(), &marker);

        let cc_child = CcChild::new(
            spawn_sleeper(),
            Some("brenn-myapp-conv42".into()),
            shim.display().to_string(),
        );
        drop(cc_child);

        let recorded = std::fs::read_to_string(&marker)
            .expect("drop must wait for the removal command to complete");
        let args: Vec<&str> = recorded.lines().collect();
        assert_eq!(
            args,
            [
                "rm",
                "--force",
                "--time",
                "0",
                "--ignore",
                "brenn-myapp-conv42"
            ]
        );
    }

    /// The same property on a multi-thread runtime, dropping from inside a
    /// worker task. That is the flavor every production teardown runs on, and it
    /// is the only one that takes the `block_in_place` arm — an arm that panics
    /// if it is ever reached on a current-thread runtime.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cc_child_drop_on_multi_thread_runtime_removes_container() {
        let _guard = subprocess_guard().await;
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("podman-argv");
        let shim = write_recording_shim(dir.path(), &marker);

        let marker_path = marker.clone();
        let program = shim.display().to_string();
        tokio::spawn(async move {
            let cc_child =
                CcChild::new(spawn_sleeper(), Some("brenn-myapp-conv42".into()), program);
            drop(cc_child);
            let recorded = std::fs::read_to_string(&marker_path)
                .expect("drop must wait for the removal command to complete");
            let args: Vec<&str> = recorded.lines().collect();
            assert_eq!(
                args,
                [
                    "rm",
                    "--force",
                    "--time",
                    "0",
                    "--ignore",
                    "brenn-myapp-conv42"
                ]
            );
        })
        .await
        .expect("drop task panicked");
    }

    /// Bare apps own no container, so drop must not invoke podman at all.
    #[tokio::test]
    async fn cc_child_drop_without_container_invokes_nothing() {
        let _guard = subprocess_guard().await;
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("podman-argv");
        let shim = write_recording_shim(dir.path(), &marker);

        let cc_child = CcChild::new(spawn_sleeper(), None, shim.display().to_string());
        drop(cc_child);

        assert!(
            !marker.exists(),
            "no removal command may run for a bare session"
        );
    }

    // --- spawn-failure enrichment tests ---

    /// A shell script that writes `lines` to stderr and exits `code`, standing
    /// in for a `claude`/`podman` that dies before the init handshake.
    fn write_failing_stub(dir: &std::path::Path, lines: &[&str], code: i32) -> PathBuf {
        use std::os::unix::fs::PermissionsExt;
        let stub = dir.join("failing-stub");
        let echoes: String = lines.iter().map(|l| format!("echo '{l}' >&2\n")).collect();
        std::fs::write(&stub, format!("#!/bin/sh\n{echoes}exit {code}\n")).unwrap();
        std::fs::set_permissions(&stub, std::fs::Permissions::from_mode(0o755)).unwrap();
        stub
    }

    /// A child that dies before answering the init request must surface its exit
    /// status and what it wrote to stderr — the text that turns "CC process
    /// exited during initialization" into a diagnosis.
    #[tokio::test]
    async fn spawn_failure_carries_exit_status_and_stderr() {
        let _guard = subprocess_guard().await;
        let dir = tempfile::tempdir().unwrap();
        let stub = write_failing_stub(
            dir.path(),
            &[
                "Error: the container name is already in use",
                "You have to remove that container to be able to reuse that name",
            ],
            7,
        );

        let mut config = bare_config();
        // The child runs in this cwd, so it has to exist.
        config.cwd = dir.path().to_path_buf();

        let (event_tx, _event_rx) = mpsc::channel(16);
        let err = CcSession::spawn_inner(config, event_tx, Some(stub.display().to_string()))
            .await
            .err()
            .expect("spawn must fail when the child exits during init");

        match err {
            CcError::InitFailed {
                ref reason,
                exit_status,
                ref stderr_tail,
            } => {
                assert_eq!(reason, "CC process exited during initialization");
                assert_eq!(exit_status, Some(7));
                assert_eq!(
                    stderr_tail,
                    &[
                        "Error: the container name is already in use".to_string(),
                        "You have to remove that container to be able to reuse that name"
                            .to_string(),
                    ]
                );
            }
            other => panic!("expected InitFailed, got {other:?}"),
        }

        // The operator sees all three parts through the existing spawn-failure
        // logging, which formats the error with Display.
        let rendered = err.to_string();
        assert!(rendered.contains("exit status 7"), "{rendered}");
        assert!(rendered.contains("already in use"), "{rendered}");
    }

    #[test]
    fn init_failure_display_omits_absent_detail() {
        let err = CcError::InitFailed {
            reason: "CC rejected the initialize request".into(),
            exit_status: None,
            stderr_tail: vec![],
        };
        assert_eq!(
            err.to_string(),
            "CC initialization failed: CC rejected the initialize request"
        );

        let err = CcError::InitTimeout {
            stderr_tail: vec!["still starting up".into()],
        };
        assert_eq!(
            err.to_string(),
            "CC initialization timed out; stderr tail: still starting up"
        );
    }

    /// A rendered error ends up in an alert body, so a chatty child must not be
    /// able to inflate it to the full tail capacity.
    #[test]
    fn init_failure_display_bounds_a_long_tail() {
        let err = CcError::InitTimeout {
            stderr_tail: (0..tasks::STDERR_TAIL_LINES)
                .map(|i| format!("line {i}"))
                .collect(),
        };
        let rendered = err.to_string();
        assert!(rendered.len() < 1024, "{} bytes", rendered.len());
        assert!(rendered.contains("line 49"), "{rendered}");
        assert!(
            !rendered.contains("line 0\n"),
            "oldest lines must be elided: {rendered}"
        );
    }

    // --- containerized spawn wiring tests ---

    /// A `podman` stand-in that appends one line per invocation — the argv joined
    /// by spaces — to `log`, so a single shim records the whole sequence of
    /// removals and runs a spawn performs. `rm` exits 0 with no stdout (nothing
    /// was holding the name); `run` exits `run_code` immediately, which closes
    /// stdout and drives the spawn down its EOF failure path.
    fn write_sequence_shim(dir: &std::path::Path, log: &std::path::Path, run_code: i32) -> PathBuf {
        write_script(
            dir,
            "podman-sequence",
            &format!(
                "echo \"$*\" >> '{}'\n\
                 if [ \"$1\" = run ]; then exit {run_code}; fi\n\
                 exit 0\n",
                log.display()
            ),
        )
    }

    fn recorded_invocations(log: &std::path::Path) -> Vec<String> {
        std::fs::read_to_string(log)
            .unwrap_or_default()
            .lines()
            .map(str::to_string)
            .collect()
    }

    const RM_ARGV: &str = "rm --force --time 0 --ignore brenn-myapp-conv42";

    /// A containerized spawn must reclaim the name *before* `podman run` claims
    /// it, and — when the handshake then fails — must remove the container it
    /// created on its way out.
    #[tokio::test]
    async fn containerized_spawn_reclaims_then_runs_then_tears_down() {
        let _guard = subprocess_guard().await;
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join("podman-invocations");
        let shim = write_sequence_shim(dir.path(), &log, 3);

        let (event_tx, _event_rx) = mpsc::channel(16);
        let err = CcSession::spawn_inner(
            container_config(),
            event_tx,
            Some(shim.display().to_string()),
        )
        .await
        .err()
        .expect("spawn must fail when the run stub exits during init");
        assert!(
            matches!(err, CcError::InitFailed { .. }),
            "expected InitFailed, got {err:?}"
        );

        // Read only after `spawn_inner` returned: the teardown removal is
        // synchronous in `CcChild::drop`, so it is already recorded.
        let invocations = recorded_invocations(&log);
        assert_eq!(invocations.len(), 3, "{invocations:?}");
        assert_eq!(invocations[0], RM_ARGV, "reclaim must come first");
        assert!(
            invocations[1].starts_with("run "),
            "the run must follow the reclaim: {invocations:?}"
        );
        assert_eq!(
            invocations[2], RM_ARGV,
            "a failed spawn must remove its container"
        );
    }

    /// A reclaim podman rejects abandons the spawn: no `podman run` may follow
    /// it into the name conflict.
    #[tokio::test]
    async fn containerized_spawn_aborts_when_reclaim_fails() {
        let _guard = subprocess_guard().await;
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join("podman-invocations");
        let shim = write_script(
            dir.path(),
            "podman-failing-rm",
            &format!(
                "echo \"$*\" >> '{}'\n\
                 if [ \"$1\" = rm ]; then echo 'container is in use' >&2; exit 2; fi\n\
                 exit 0\n",
                log.display()
            ),
        );

        let (event_tx, _event_rx) = mpsc::channel(16);
        let err = CcSession::spawn_inner(
            container_config(),
            event_tx,
            Some(shim.display().to_string()),
        )
        .await
        .err()
        .expect("spawn must fail when the reclaim fails");
        match err {
            CcError::ContainerReclaimFailed(ref msg) => {
                assert!(msg.contains("container is in use"), "{msg}");
            }
            other => panic!("expected ContainerReclaimFailed, got {other:?}"),
        }

        let invocations = recorded_invocations(&log);
        assert_eq!(invocations, [RM_ARGV], "no run may follow a failed reclaim");
    }

    /// podman missing or unexecutable is an explicit accepted failure mode whose
    /// entire value is that the operator can read what happened.
    #[tokio::test]
    async fn reclaim_reports_when_podman_cannot_be_executed() {
        let _guard = subprocess_guard().await;
        let (dispatcher, _count, _drainer) = brenn_obs::alerting::make_counting_alerter();
        let result = reclaim_container_name(
            "brenn-myapp-conv42",
            "/nonexistent/podman-does-not-exist",
            &dispatcher,
        )
        .await;
        match result {
            Err(CcError::ContainerReclaimFailed(msg)) => {
                assert!(msg.contains("brenn-myapp-conv42"), "{msg}");
                assert!(msg.contains("No such file"), "{msg}");
            }
            other => panic!("expected ContainerReclaimFailed, got {other:?}"),
        }
    }

    // --- server-shutdown flag threading ---

    /// A stub that answers the init request and then exits, so the handshake
    /// succeeds and the reader immediately hits stdout EOF — the state whose
    /// alerting the two shutdown flags govern.
    fn write_ack_then_exit_stub(dir: &std::path::Path) -> PathBuf {
        write_script(
            dir,
            "ack-stub",
            "echo '{\"type\":\"control_response\",\"response\":\
             {\"subtype\":\"success\",\"request_id\":\"req_0\"}}'\n",
        )
    }

    /// Run a spawn whose child acks and exits, and report how many alerts the
    /// resulting reader EOF produced.
    async fn spawn_eof_alert_count(server_shutting_down: bool) -> u32 {
        let _guard = subprocess_guard().await;
        let dir = tempfile::tempdir().unwrap();
        let stub = write_ack_then_exit_stub(dir.path());
        let (dispatcher, count, drainer) = brenn_obs::alerting::make_counting_alerter();

        let mut config = bare_config();
        config.cwd = dir.path().to_path_buf();
        config.alert_dispatcher = dispatcher;
        config.server_shutting_down = Some(Arc::new(AtomicBool::new(server_shutting_down)));

        let (event_tx, mut event_rx) = mpsc::channel(16);
        let (session, _info) =
            CcSession::spawn_inner(config, event_tx, Some(stub.display().to_string()))
                .await
                .expect("handshake completes before the stub exits");

        let event = tokio::time::timeout(tokio::time::Duration::from_secs(5), event_rx.recv())
            .await
            .expect("the child exits, so the reader must report a death")
            .expect("event");
        assert!(matches!(event, SessionEvent::Died(_)));
        drop(session);

        // Resolves once the reader task has dropped its dispatcher clone.
        tokio::time::timeout(tokio::time::Duration::from_secs(5), drainer)
            .await
            .expect("alert drainer did not finish")
            .expect("alert drainer panicked");
        count.load(Ordering::SeqCst)
    }

    /// `spawn()` must hand the server-level flag to the reader task. Dropping it
    /// would page the operator for every session on every deploy.
    #[tokio::test]
    async fn spawn_threads_server_flag_to_the_reader() {
        assert_eq!(
            spawn_eof_alert_count(true).await,
            0,
            "a death during server shutdown must not alert"
        );
    }

    /// The mirror: with the server flag clear and no per-session mark, the same
    /// EOF must page. Also pins that `spawn()` creates a *fresh* per-session flag
    /// (clear) when the config supplies none.
    #[tokio::test]
    async fn spawn_with_clear_flags_alerts_on_death() {
        assert_eq!(
            spawn_eof_alert_count(false).await,
            1,
            "an unexpected death must fire the Critical alert"
        );
    }

    // --- failure-path timeout bounds ---

    /// A child that closed its stdout but is still running must not hang the
    /// failing spawn. The bound is real wall clock rather than a paused clock
    /// because the thing under test is a subprocess the runtime cannot see: with
    /// auto-advancing time the reap would resolve before the child ever ran, and
    /// the test would prove nothing.
    #[tokio::test]
    async fn wait_for_exit_code_gives_up_on_a_child_that_will_not_exit() {
        let _guard = subprocess_guard().await;
        let mut child = CcChild::bare(spawn_sleeper());
        let started = std::time::Instant::now();
        assert_eq!(
            child.wait_for_exit_code().await,
            None,
            "a live child yields no exit status"
        );
        assert!(
            started.elapsed() >= CHILD_REAP_TIMEOUT,
            "the reap must have waited out its bound"
        );
        assert!(
            started.elapsed() < CHILD_REAP_TIMEOUT * 4,
            "the reap must be bounded, not open-ended"
        );
    }

    /// A child killed by a signal has no exit code. That must read as "unknown",
    /// not as a panic or a fabricated status.
    #[tokio::test]
    async fn wait_for_exit_code_is_none_for_a_signal_death() {
        let _guard = subprocess_guard().await;
        let mut child = CcChild::bare(spawn_sleeper());
        child.child.start_kill().expect("failed to signal child");
        assert_eq!(child.wait_for_exit_code().await, None);
    }

    /// The drain wait exists so a child holding its stderr pipe open cannot stall
    /// the spawn's failure report. On timeout the tail is whatever was captured,
    /// not nothing.
    #[tokio::test]
    async fn drained_stderr_tail_returns_what_it_has_when_the_drain_stalls() {
        let tail = tasks::StderrTail::new();
        tail.push("partial output".into());
        let never_finishes = tokio::spawn(std::future::pending::<()>());

        let started = std::time::Instant::now();
        let snapshot = drained_stderr_tail(&tail, never_finishes).await;
        assert_eq!(snapshot, ["partial output".to_string()]);
        assert!(
            started.elapsed() >= STDERR_DRAIN_TIMEOUT,
            "the drain wait must have run to its bound"
        );
        assert!(
            started.elapsed() < STDERR_DRAIN_TIMEOUT * 4,
            "the drain wait must be bounded, not open-ended"
        );
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
