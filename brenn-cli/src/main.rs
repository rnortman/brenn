//! CLI test harness for the brenn-cc library, plus operator admin verbs
//! and a couple of small build-time helpers.
//!
//! Subcommands:
//! - `cc` (default): manual CC test harness — shells out to real Claude
//!   Code. NOT part of the automated test suite.
//! - `device`: operator device management (list, unenroll).
//! - `emit-frontmatter-css`: write the Lit `css` template wrapping
//!   `brenn_lib::frontmatter_css::FRONTMATTER_CSS` to a TS file.
//!   Invoked from the Makefile to regenerate
//!   `frontend/src/styles/frontmatter.generated.ts`.
//! - `push`: sign and POST a plain-text message to an
//!   `hmac-timestamped-body` webhook ingress endpoint..

use std::io::{Read as _, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use brenn_cc::session::approval::{ApprovalDecision, ApprovalKind};
use brenn_cc::session::{CcSession, CcSessionConfig, SessionEvent};
use brenn_lib::auth::device::{UnenrollOutcome, unenroll_device};
use brenn_lib::obs::alerting::AlertDispatcher;
use brenn_lib::obs::transcript::TranscriptWriter;
use brenn_lib::webhook::signature::hmac_sha256_hex;
use clap::{Parser, Subcommand};
use tokio::sync::mpsc;

/// Read-only tools that are safe to auto-approve.
const SAFE_TOOLS: &[&str] = &["Read", "Glob", "Grep", "ToolSearch"];

#[derive(Parser)]
#[command(
    name = "brenn-cli",
    about = "CLI test harness, operator admin verbs, and build helpers"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Manual CC test harness (the original behavior).
    Cc(CcArgs),
    /// Operator device management: list devices or unenroll a device.
    Device(DeviceArgs),
    /// Write the frontend's frontmatter-CSS Lit template to a file.
    /// Source of truth lives in `brenn_lib::frontmatter_css`.
    EmitFrontmatterCss {
        /// Output TS file path.
        #[arg(long)]
        out: PathBuf,
    },
    /// Sign and POST a plain-text message to an hmac-timestamped-body
    /// webhook ingress endpoint.
    Push(PushArgs),
}

/// Arguments for the `push` subcommand.
///
/// Note: the secret file content is trimmed on both ends (matching the server's
/// `load_secret_file`) so secret bytes are byte-identical on both sides.
///
/// Environment variables accepted (in addition to those documented per-flag):
///   BRENN_PUSH_SECRET — the HMAC secret value directly (env-only; never a flag, to keep
///                       the secret out of argv/process listings/shell history). Takes
///                       lower precedence than --secret-file / BRENN_PUSH_SECRET_FILE.
#[derive(clap::Args)]
struct PushArgs {
    /// Full URL of the target webhook endpoint.
    /// (e.g. `https://host/webhooks/push-test`)
    #[arg(long, env = "BRENN_PUSH_URL")]
    url: String,

    /// Path to a file containing the shared HMAC secret.
    /// The file content is trimmed (both ends) to produce the secret bytes,
    /// matching the server's `load_secret_file` behavior.
    /// Takes precedence over `BRENN_PUSH_SECRET`.
    #[arg(long, env = "BRENN_PUSH_SECRET_FILE")]
    secret_file: Option<PathBuf>,

    /// Optional key-id to send in `x-brenn-push-key-id`.
    /// Only needed for multi-key endpoints.
    /// Charset: `[A-Za-z0-9._-]{1,64}`.
    #[arg(long, env = "BRENN_PUSH_KEY_ID")]
    key_id: Option<String>,

    /// The message to push. If absent, the message is read from stdin.
    message: Option<String>,
}

#[derive(clap::Args)]
struct DeviceArgs {
    /// Path to the Brenn SQLite database (or set BRENN_DB env var).
    #[arg(long, env = "BRENN_DB")]
    db: PathBuf,

    #[command(subcommand)]
    verb: DeviceVerb,
}

#[derive(Subcommand)]
enum DeviceVerb {
    /// List all devices (enrolled and unenrolled) with enough information
    /// to identify the target device_id for the unenroll verb.
    List,
    /// Unenroll a device by id (obtained from `device list`).
    Unenroll {
        /// Device id to unenroll.
        #[arg(long)]
        id: i64,
        /// Free-form audit reason (required; surfaced in structured log).
        #[arg(long)]
        reason: String,
    },
}

#[derive(clap::Args)]
struct CcArgs {
    /// CC model to use.
    #[arg(long, default_value = "haiku")]
    model: String,

    /// The prompt to send to CC.
    #[arg(long)]
    prompt: String,

    /// Comma-separated list of allowed tools. Always specify this.
    #[arg(long)]
    tools: Option<String>,

    /// Working directory for CC.
    #[arg(long, default_value = ".")]
    cwd: PathBuf,

    /// Resume a previous session by ID.
    #[arg(long)]
    resume: Option<String>,

    /// Print all raw NDJSON to stderr.
    #[arg(long)]
    verbose: bool,
}

fn is_safe_tool(tool_name: &str) -> bool {
    SAFE_TOOLS.contains(&tool_name)
}

/// Ask the user for approval on stdin. Returns true for approve, false for deny.
fn prompt_approval(tool_name: &str, input: &serde_json::Value) -> bool {
    eprintln!("\n--- Tool approval required ---");
    eprintln!("Tool: {tool_name}");
    eprintln!(
        "Input: {}",
        serde_json::to_string_pretty(input).unwrap_or_else(|_| input.to_string())
    );
    eprint!("Approve? [y/N] ");
    std::io::stderr().flush().expect("flush stderr");

    let mut line = String::new();
    std::io::stdin().read_line(&mut line).expect("read stdin");
    matches!(line.trim().to_lowercase().as_str(), "y" | "yes")
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    match cli.command {
        Command::Cc(args) => run_cc(args).await,
        Command::Device(args) => run_device(args).await,
        Command::EmitFrontmatterCss { out } => emit_frontmatter_css(&out),
        Command::Push(args) => run_push(args).await,
    }
}

/// Format a ms-epoch timestamp as a human-readable local-zone string, or "-"
/// for NULL (represented as `None`).
fn format_ms_option(ms: Option<i64>) -> String {
    match ms {
        None => "-".to_string(),
        Some(ms) => {
            let dt = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(ms)
                .expect("format_ms_option: invalid ms timestamp");
            // Convert to local time for human readability.
            let local: chrono::DateTime<chrono::Local> = dt.into();
            local.format("%Y-%m-%dT%H:%M:%S%z").to_string()
        }
    }
}

/// Run the `device` subcommand family.
async fn run_device(args: DeviceArgs) {
    let db_path = args.db;
    let db = brenn_lib::db::init_db(&db_path);

    match args.verb {
        DeviceVerb::List => {
            let conn = db.lock().await;
            run_device_list(&conn);
        }
        DeviceVerb::Unenroll { id, reason } => {
            let conn = db.lock().await;
            // Print the resolved DB path and device identifiers before acting so
            // an operator targeting the wrong file or wrong id can catch the
            // mistake before the unenroll is irreversible.
            let resolved_db = db_path.canonicalize().unwrap_or_else(|_| db_path.clone());
            let device_info: Option<(String, Option<String>, String)> = conn
                .query_row(
                    "SELECT d.guessed_slug, u.username, d.last_seen_at \
                     FROM devices d \
                     LEFT JOIN device_users du ON du.device_id = d.id \
                     LEFT JOIN users u ON u.id = du.user_id \
                     WHERE d.id = ?1 \
                     LIMIT 1",
                    rusqlite::params![id],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, Option<String>>(1)?,
                            row.get::<_, String>(2)?,
                        ))
                    },
                )
                .ok();
            match &device_info {
                Some((guessed_slug, username, last_seen_at)) => {
                    println!(
                        "db: {}\ndevice: id={id} slug={guessed_slug} user={} last_seen_at={last_seen_at}",
                        resolved_db.display(),
                        username.as_deref().unwrap_or("-"),
                    );
                }
                None => {
                    // Device id not found; unenroll_device will panic with a clear
                    // message — proceed so the caller gets the fail-fast behavior.
                    println!(
                        "db: {}\ndevice: id={id} (not found — will panic)",
                        resolved_db.display(),
                    );
                }
            }
            let outcome = unenroll_device(&conn, id, &reason);
            drop(conn);
            match outcome {
                UnenrollOutcome::Unenrolled { unenrolled_at_ms } => {
                    println!(
                        "device {id} unenrolled at {}",
                        format_ms_option(Some(unenrolled_at_ms))
                    );
                    eprintln!(
                        "NOTE: existing WebSocket connections from this device will \
                         not be terminated automatically. Restart the server to close \
                         any active sessions."
                    );
                }
                UnenrollOutcome::AlreadyUnenrolled { unenrolled_at_ms } => {
                    println!(
                        "device {id} already unenrolled at {}",
                        format_ms_option(Some(unenrolled_at_ms))
                    );
                }
            }
        }
    }
}

/// Execute the `device list` query and print TSV output.
///
/// One row per (device, user) pair. Devices with no `device_users` rows appear
/// once with assigned_slug and username blank. Enrolled devices first, then
/// by last_seen_at DESC, then by username ASC for stable output on shared devices.
fn run_device_list(conn: &rusqlite::Connection) {
    // Header.
    println!(
        "{:<10}\t{:<20}\t{:<20}\t{:<20}\t{:<12}\t{:<25}\tunenrolled_at",
        "device_id", "guessed_slug", "assigned_slug", "username", "platform", "last_seen_at"
    );

    let mut stmt = conn
        .prepare(
            "SELECT d.id, d.guessed_slug, du.assigned_slug, u.username, \
                    d.platform, d.last_seen_at, d.unenrolled_at \
             FROM devices d \
             LEFT JOIN device_users du ON du.device_id = d.id \
             LEFT JOIN users u ON u.id = du.user_id \
             ORDER BY (d.unenrolled_at IS NULL) DESC, d.last_seen_at DESC, u.username ASC",
        )
        .expect("device list: prepare statement");

    let mut rows = stmt
        .query(rusqlite::params![])
        .expect("device list: execute query");

    while let Some(row) = rows.next().expect("device list: fetch row") {
        let device_id: i64 = row.get(0).expect("device list: get device_id");
        let guessed_slug: String = row.get(1).expect("device list: get guessed_slug");
        let assigned_slug: Option<String> = row.get(2).expect("device list: get assigned_slug");
        let username: Option<String> = row.get(3).expect("device list: get username");
        let platform: Option<String> = row.get(4).expect("device list: get platform");
        let last_seen_at: String = row.get(5).expect("device list: get last_seen_at");
        let unenrolled_at_ms: Option<i64> = row.get(6).expect("device list: get unenrolled_at");

        println!(
            "{:<10}\t{:<20}\t{:<20}\t{:<20}\t{:<12}\t{:<25}\t{}",
            device_id,
            guessed_slug,
            assigned_slug.as_deref().unwrap_or("-"),
            username.as_deref().unwrap_or("-"),
            platform.as_deref().unwrap_or("-"),
            last_seen_at,
            format_ms_option(unenrolled_at_ms),
        );
    }
}

/// Write a TS file exporting a Lit `css` template wrapping
/// `FRONTMATTER_CSS`. The output is overwritten unconditionally; callers
/// (the Makefile) are responsible for ensuring the parent directory
/// exists.
fn emit_frontmatter_css(out: &std::path::Path) {
    let css = brenn_lib::frontmatter_css::FRONTMATTER_CSS;
    // Lit's `css` tagged template uses backticks. The CSS contents
    // never include a backtick (verified by an assertion below); if
    // they ever do, the build fails so the developer can switch to a
    // different escape strategy.
    assert!(
        !css.contains('`'),
        "FRONTMATTER_CSS contains a backtick — Lit's `css` tagged \
         template would mis-parse it. Update the emitter."
    );
    assert!(
        !css.contains("${"),
        "FRONTMATTER_CSS contains '${{' — Lit's `css` tagged template \
         would interpret it as a template substitution. Update the \
         emitter."
    );
    let body = format!(
        "// AUTO-GENERATED by `brenn-cli emit-frontmatter-css`. Do not edit.\n\
         // Source of truth: brenn-lib/src/frontmatter_css.rs\n\
         import {{ css }} from \"lit\";\n\
         \n\
         export const frontmatterStyles = css`\n{css}`;\n",
    );
    std::fs::write(out, body).unwrap_or_else(|e| {
        panic!(
            "emit-frontmatter-css: failed to write {}: {e}",
            out.display(),
        );
    });
}

/// Resolve the HMAC secret for the push subcommand.
///
/// Precedence: `--secret-file` (or `BRENN_PUSH_SECRET_FILE` env) → `BRENN_PUSH_SECRET` env.
/// The file variant applies `str::trim()` (both ends) to match the server's
/// `load_secret_file` behavior. The env variant is used as-is.
/// Returns the secret bytes, or exits non-zero with a diagnostic.
///
fn resolve_push_secret(args: &PushArgs) -> Vec<u8> {
    if let Some(path) = &args.secret_file {
        let content = std::fs::read_to_string(path).unwrap_or_else(|e| {
            eprintln!(
                "error: cannot read BRENN_PUSH_SECRET_FILE ({}): {e}",
                path.display()
            );
            std::process::exit(2);
        });
        let trimmed = content.trim();
        if trimmed.is_empty() {
            eprintln!(
                "error: secret file ({}) is empty or all-whitespace",
                path.display()
            );
            std::process::exit(2);
        }
        return trimmed.as_bytes().to_vec();
    }
    if let Ok(val) = std::env::var("BRENN_PUSH_SECRET") {
        if val.is_empty() {
            eprintln!("error: BRENN_PUSH_SECRET is set but empty");
            std::process::exit(2);
        }
        return val.into_bytes();
    }
    eprintln!(
        "error: no secret provided; supply --secret-file <PATH> (or BRENN_PUSH_SECRET_FILE) \
         or set BRENN_PUSH_SECRET"
    );
    std::process::exit(2);
}

/// Resolve and validate the message text for the push subcommand.
///
/// Source: positional `MESSAGE` arg if present; else all of stdin.
/// One trailing newline is stripped so `echo hi | brenn-cli push` and
/// `brenn-cli push hi` behave identically.
/// Rejects empty / whitespace-only messages client-side (exits non-zero).
fn resolve_push_message(args: &PushArgs) -> String {
    let raw = if let Some(msg) = &args.message {
        msg.clone()
    } else {
        let mut buf = String::new();
        std::io::stdin()
            .read_to_string(&mut buf)
            .unwrap_or_else(|e| {
                eprintln!("error: failed to read message from stdin: {e}");
                std::process::exit(2);
            });
        buf
    };
    // Strip a single trailing newline (LF or CRLF).
    let stripped = raw.strip_suffix('\n').unwrap_or(&raw);
    let stripped = stripped.strip_suffix('\r').unwrap_or(stripped);
    if stripped.trim().is_empty() {
        eprintln!("error: message is empty or whitespace-only; no request sent");
        std::process::exit(2);
    }
    stripped.to_string()
}

/// Validate a key-id string: `[A-Za-z0-9._-]{1,64}`.
/// Delegates to `brenn_lib::webhook::is_valid_key_id` — single source of truth.
/// (reuse-1/quality-1: removed local copy that used .chars() instead of .bytes().)
fn is_valid_key_id(id: &str) -> bool {
    brenn_lib::webhook::is_valid_key_id(id)
}

/// Execute the `push` subcommand: sign and POST a plain-text message.
///
/// Exit codes:
///   0 — HTTP 2xx; message delivered.
///   1 — HTTP-level rejection (non-2xx).
///   2 — Local error (missing input, I/O, transport/TLS failure).
async fn run_push(args: PushArgs) {
    let secret = resolve_push_secret(&args);
    let message = resolve_push_message(&args);

    // Validate key-id if provided.
    if let Some(kid) = &args.key_id
        && !is_valid_key_id(kid)
    {
        eprintln!(
            "error: key-id {:?} is invalid; must match [A-Za-z0-9._-]{{1,64}}",
            kid
        );
        std::process::exit(2);
    }

    // Canonical form: t_str || "." || body  (matches template "{t}.{body}").
    let t: i64 = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| {
            eprintln!("error: system clock is before the Unix epoch; check NTP");
            std::process::exit(2);
        })
        .as_secs()
        .try_into()
        .unwrap_or_else(|_| {
            eprintln!(
                "error: unix timestamp overflows i64; clock is implausibly far in the future"
            );
            std::process::exit(2);
        });
    let t_str = t.to_string();
    let message_bytes = message.as_bytes();

    // Canonical bytes = t_str || "." || message_bytes
    let mut canonical = Vec::with_capacity(t_str.len() + 1 + message_bytes.len());
    canonical.extend_from_slice(t_str.as_bytes());
    canonical.push(b'.');
    canonical.extend_from_slice(message_bytes);

    let hex = hmac_sha256_hex(&secret, &canonical);
    let signature = format!("v1={hex}");

    // Build the HTTP client.
    let client = reqwest::Client::builder().build().unwrap_or_else(|e| {
        eprintln!("error: failed to build HTTP client: {e}");
        std::process::exit(2);
    });

    let mut req = client
        .post(&args.url)
        .header("content-type", "text/plain")
        .header("x-brenn-push-timestamp", &t_str)
        .header("x-brenn-push-signature", &signature)
        .body(message_bytes.to_vec());

    if let Some(kid) = &args.key_id {
        req = req.header("x-brenn-push-key-id", kid);
    }

    let response = req.send().await.unwrap_or_else(|e| {
        eprintln!("error: transport error: {e}");
        std::process::exit(2);
    });

    let status = response.status();
    if status.is_success() {
        eprintln!("ok");
        std::process::exit(0);
    }

    // Non-2xx: print status + body to stderr, exit 1 (HTTP rejection).
    // If the response body itself cannot be read (e.g. truncated TLS stream after
    // headers), exit 2 (transport error) so callers distinguish "server said no"
    // from "couldn't read the server's full response". (errhandling-3)
    match response.text().await {
        Err(e) => {
            eprintln!("error: server returned {status} (response body unreadable: {e})");
            std::process::exit(2);
        }
        Ok(body_text) if body_text.is_empty() => {
            eprintln!("error: server returned {status}");
            std::process::exit(1);
        }
        Ok(body_text) => {
            eprintln!("error: server returned {status}: {body_text}");
            std::process::exit(1);
        }
    }
}

async fn run_cc(cli: CcArgs) {
    // Initialize tracing.
    let filter = if cli.verbose { "debug" } else { "info" };
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .init();

    // Set up transcript writer.
    let log_dir = PathBuf::from("logs");
    let transcript = Arc::new(
        TranscriptWriter::new(&log_dir, "cli_transcript.log").expect("create transcript writer"),
    );

    // Set up alert dispatcher (noop for CLI).
    let (alert_dispatcher, _alert_handle) = AlertDispatcher::noop();

    // Parse allowed tools.
    let allowed_tools = cli.tools.map(|t| t.split(',').map(String::from).collect());

    // Hook config — register PreToolUse and PostToolUse catch-all hooks.
    let hooks = Some(brenn_cc::protocol::outgoing::HooksConfig {
        pre_tool_use: Some(vec![brenn_cc::protocol::outgoing::HookMatcher {
            hook_callback_ids: vec!["hook_pre_tool_0".into()],
            timeout: 120,
            matcher: None,
        }]),
        post_tool_use: Some(vec![brenn_cc::protocol::outgoing::HookMatcher {
            hook_callback_ids: vec!["hook_post_tool_0".into()],
            timeout: 10,
            matcher: None,
        }]),
    });

    let config = CcSessionConfig {
        model: cli.model,
        cwd: cli.cwd,
        hooks,
        mcp_config: None,
        allowed_tools,
        resume_session_id: cli.resume,
        transcript,
        alert_dispatcher,
        container: None,
        app_slug: "cli".to_string(),
        container_name_suffix: "cli".to_string(),
        add_dirs: vec![],
        cc_extra_args: vec![],
        env_vars: vec![],
        shutting_down: None,
    };

    let (event_tx, mut event_rx) = mpsc::channel(256);

    tracing::info!("spawning CC session...");
    let (session, _init_ack_info) = match CcSession::spawn(config, event_tx).await {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(error = %e, "failed to spawn CC session");
            std::process::exit(1);
        }
    };
    tracing::info!("CC session spawned, sending prompt...");

    // Send the prompt.
    if let Err(e) = session.send_message(&cli.prompt).await {
        tracing::error!(error = %e, "failed to send message");
        std::process::exit(1);
    }

    // Process events until the session completes.
    while let Some(event) = event_rx.recv().await {
        match event {
            SessionEvent::Initialized(info) => {
                tracing::info!(
                    session_id = %info.session_id,
                    model = %info.model,
                    tools = ?info.tools,
                    "CC session initialized"
                );
            }
            SessionEvent::AssistantMessage(asst) => {
                println!("\n--- Assistant ---");
                for block in &asst.message.content {
                    match block {
                        brenn_cc::protocol::incoming::ContentBlock::Text { text } => {
                            println!("{text}");
                        }
                        brenn_cc::protocol::incoming::ContentBlock::ToolUse {
                            name, input, ..
                        } => {
                            println!("[Tool use: {name}]");
                            if cli.verbose {
                                println!(
                                    "  Input: {}",
                                    serde_json::to_string_pretty(input)
                                        .unwrap_or_else(|_| input.to_string())
                                );
                            }
                        }
                        brenn_cc::protocol::incoming::ContentBlock::Thinking {
                            thinking, ..
                        } => {
                            println!("[Thinking: {}...]", &thinking[..thinking.len().min(100)]);
                        }
                        brenn_cc::protocol::incoming::ContentBlock::Unknown => {
                            println!("[Unknown content block]");
                        }
                    }
                }
            }
            SessionEvent::StreamEvent(se) => {
                // Extract text delta if present.
                if let Some(delta) = se.event.get("delta")
                    && let Some(text) = delta.get("text").and_then(|t| t.as_str())
                {
                    print!("{text}");
                    std::io::stdout().flush().expect("flush stdout");
                }
            }
            SessionEvent::ToolResult(_) => {
                // Tool results are visible in the message stream; no action needed.
            }
            SessionEvent::ApprovalRequired(req) => {
                let (tool_name, input, decision) = match &req.kind {
                    ApprovalKind::Permission {
                        tool_name, input, ..
                    } => {
                        let approved = if is_safe_tool(tool_name) {
                            tracing::info!(tool = %tool_name, "auto-approving safe tool");
                            true
                        } else {
                            prompt_approval(tool_name, input)
                        };
                        if approved {
                            (
                                tool_name.clone(),
                                input.clone(),
                                ApprovalDecision::Allow {
                                    updated_input: Some(input.clone()),
                                },
                            )
                        } else {
                            (
                                tool_name.clone(),
                                input.clone(),
                                ApprovalDecision::Deny {
                                    reason: "Denied by user".into(),
                                },
                            )
                        }
                    }
                    ApprovalKind::PreToolUse {
                        tool_name,
                        tool_input,
                        ..
                    } => {
                        let approved = if is_safe_tool(tool_name) {
                            tracing::info!(tool = %tool_name, "auto-approving safe tool (hook)");
                            true
                        } else {
                            prompt_approval(tool_name, tool_input)
                        };
                        if approved {
                            (
                                tool_name.clone(),
                                tool_input.clone(),
                                ApprovalDecision::Allow {
                                    updated_input: None,
                                },
                            )
                        } else {
                            (
                                tool_name.clone(),
                                tool_input.clone(),
                                ApprovalDecision::Deny {
                                    reason: "Denied by user".into(),
                                },
                            )
                        }
                    }
                    ApprovalKind::PostToolUse { tool_name, .. } => {
                        tracing::debug!(tool = %tool_name, "PostToolUse hook — continuing");
                        (
                            tool_name.clone(),
                            serde_json::Value::Null,
                            ApprovalDecision::Continue {
                                updated_output: None,
                            },
                        )
                    }
                    ApprovalKind::OtherHook { event_name, .. } => {
                        tracing::debug!(event = %event_name, "other hook — continuing");
                        (
                            event_name.clone(),
                            serde_json::Value::Null,
                            ApprovalDecision::Continue {
                                updated_output: None,
                            },
                        )
                    }
                };
                let _ = (tool_name, input); // Used for logging above.
                // Send the decision back.
                req.response_tx.send(decision).ok();
            }
            SessionEvent::ApprovalCancelled { request_id } => {
                tracing::info!(request_id = %request_id, "approval cancelled");
            }
            SessionEvent::RateLimit(rle) => {
                tracing::warn!("rate limit event: {:?}", rle.rate_limit_info);
            }
            SessionEvent::StatusChange {
                status,
                compact_result,
            } => {
                tracing::debug!(?status, ?compact_result, "CC status change");
            }
            SessionEvent::CompactBoundary { metadata } => {
                tracing::info!(?metadata, "compact boundary");
            }
            SessionEvent::TurnCompleted(res) => {
                println!("\n--- Turn complete ---");
                if let Some(cost) = res.total_cost_usd {
                    println!("Cost: ${cost:.4}");
                }
                if let Some(turns) = res.num_turns {
                    println!("Turns: {turns}");
                }
                if let Some(ms) = res.duration_ms {
                    println!("Duration: {ms}ms");
                }
                // In persistent mode, CC stays alive. For the CLI, we exit
                // after the first turn since it's a single-prompt tool.
                break;
            }
            SessionEvent::Died(err) => {
                tracing::error!(error = %err, "CC session died");
                break;
            }
            SessionEvent::UnrecognizedMessage { raw_line } => {
                tracing::warn!(raw = %raw_line, "unrecognized CC message");
            }
        }
    }
}

#[cfg(test)]
mod tests {

    mod push {
        use brenn_lib::webhook::signature::hmac_sha256_hex;

        /// Verify the canonical form produced by the push signer:
        /// canonical bytes = t_str || "." || body.
        /// Also cross-check the resulting `v1=<hex>` against an independent
        /// reference HMAC on the same bytes.
        #[test]
        fn canonical_form_and_signature() {
            let secret = b"test-secret";
            let t_str = "1749200000";
            let body = b"hello world";

            let mut canonical = Vec::new();
            canonical.extend_from_slice(t_str.as_bytes());
            canonical.push(b'.');
            canonical.extend_from_slice(body);

            let hex = hmac_sha256_hex(secret, &canonical);
            let sig = format!("v1={hex}");

            // Signature must be 3 + 64 chars ("v1=" prefix + 64 hex chars).
            assert_eq!(sig.len(), 67, "signature length wrong: {sig}");
            assert!(sig.starts_with("v1="), "signature must start with v1=");
            assert!(
                sig[3..].chars().all(|c| c.is_ascii_hexdigit()),
                "signature hex portion must be all hex digits: {sig}"
            );

            // Known-vector check: compare against HMAC-SHA256 computed independently
            // via Python: hmac.new(b'test-secret', b'1749200000.hello world', sha256).hexdigest()
            // → e28cab4123de6f823dc1a740b63e0af9c1a844d3b14a17715ff4c4b79d341878
            // This verifies the canonical form (t_str || "." || body) and HMAC function
            // against a reference implementation, catching any separator or byte-order bugs.
            assert_eq!(
                hex, "e28cab4123de6f823dc1a740b63e0af9c1a844d3b14a17715ff4c4b79d341878",
                "HMAC does not match reference vector; canonical form or HMAC function is broken"
            );
        }

        /// Body containing dots: canonical form is unambiguous because body is
        /// the last field with no further delimiter.
        #[test]
        fn canonical_form_body_with_dots() {
            let secret = b"secret";
            let t_str = "1000000000";
            let body = b"a.b.c";

            let mut canonical = Vec::new();
            canonical.extend_from_slice(t_str.as_bytes());
            canonical.push(b'.');
            canonical.extend_from_slice(body);

            assert_eq!(canonical, b"1000000000.a.b.c");
            // Reference vector via Python:
            // hmac.new(b'secret', b'1000000000.a.b.c', sha256).hexdigest()
            let hex = hmac_sha256_hex(secret, &canonical);
            assert_eq!(
                hex, "25a448a46291eba41fcfd7b98a77263af1b7c0f31df18dbc4a106bc97990b66f",
                "HMAC over a dot-containing body does not match the reference vector"
            );
        }

        /// Multi-byte (UTF-8) body.
        #[test]
        fn canonical_form_multibyte_body() {
            let secret = b"secret";
            let t_str = "1000000000";
            let body = "héllo wörld".as_bytes();

            let mut canonical = Vec::new();
            canonical.extend_from_slice(t_str.as_bytes());
            canonical.push(b'.');
            canonical.extend_from_slice(body);

            // Reference vector via Python:
            // hmac.new(b'secret', '1000000000.héllo wörld'.encode(), sha256).hexdigest()
            let hex = hmac_sha256_hex(secret, &canonical);
            assert_eq!(
                hex, "80cf105497af44590e1ca534a8575059e4d83a08f61ea3187d8a42505b9523fd",
                "HMAC over a multi-byte body does not match the reference vector; \
                 the body must be signed as bytes, not chars"
            );
        }

        /// `is_valid_key_id` accepts valid key-ids and rejects invalid ones.
        #[test]
        fn key_id_validation() {
            use super::super::is_valid_key_id;
            assert!(is_valid_key_id("primary"));
            assert!(is_valid_key_id("key-1"));
            assert!(is_valid_key_id("key.id_v2"));
            assert!(is_valid_key_id(&"a".repeat(64)));
            // Invalid: empty.
            assert!(!is_valid_key_id(""));
            // Invalid: too long.
            assert!(!is_valid_key_id(&"a".repeat(65)));
            // Invalid: space.
            assert!(!is_valid_key_id("bad key"));
            // Invalid: slash.
            assert!(!is_valid_key_id("bad/key"));
        }

        /// Different secrets produce different signatures for the same message.
        #[test]
        fn different_secrets_produce_different_signatures() {
            let body = b"same message";
            let t_str = "1000000000";
            let mut canonical = Vec::new();
            canonical.extend_from_slice(t_str.as_bytes());
            canonical.push(b'.');
            canonical.extend_from_slice(body);

            let hex1 = hmac_sha256_hex(b"secret-a", &canonical);
            let hex2 = hmac_sha256_hex(b"secret-b", &canonical);
            assert_ne!(hex1, hex2, "different secrets must produce different HMACs");
        }
    }
}
