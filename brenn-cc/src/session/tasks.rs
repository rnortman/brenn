//! Background tasks for CC session management.
//!
//! Three tasks per session:
//! 1. Stdout reader — parses NDJSON, routes messages
//! 2. Stdin writer — serializes and writes outgoing messages
//! 3. Stderr drain — logs CC debug output

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use brenn_lib::obs::alerting::{AlertDispatcher, AlertSeverity};
use brenn_lib::obs::transcript::TranscriptWriter;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{ChildStderr, ChildStdout};
use tokio::sync::{mpsc, oneshot};

use crate::error::{CcError, TransportError};
use crate::protocol::incoming::*;
use crate::protocol::{self, CcIncoming, CcOutgoing};
use crate::transport::{NdjsonReader, NdjsonWriter};

use super::OutgoingEnvelope;
use super::approval::{ApprovalDecision, ApprovalKind, ApprovalRequest};
use super::{SessionEvent, SessionInfo};

/// Stored per pending approval so we can cancel the wait task.
/// The `cancel_tx` is never explicitly sent on — dropping it closes the
/// channel, which the wait task detects via `cancel_rx`.
struct PendingApproval {
    #[expect(dead_code, reason = "dropped to signal cancellation, not read")]
    cancel_tx: oneshot::Sender<()>,
}

/// Spawn the stdout reader task using a real child process stdout.
#[expect(
    clippy::too_many_arguments,
    reason = "internal plumbing, not a public API surface"
)]
pub fn spawn_stdout_reader(
    stdout: ChildStdout,
    event_tx: mpsc::Sender<SessionEvent>,
    init_tx: mpsc::Sender<CcIncoming>,
    outgoing_tx: mpsc::Sender<OutgoingEnvelope>,
    transcript: Arc<TranscriptWriter>,
    alert_dispatcher: AlertDispatcher,
    alive: Arc<AtomicBool>,
    shutting_down: Arc<AtomicBool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(run_stdout_reader(
        stdout,
        event_tx,
        init_tx,
        outgoing_tx,
        transcript,
        alert_dispatcher,
        alive,
        shutting_down,
    ))
}

/// The stdout reader loop, generic over any async reader.
/// Extracted so it can be tested with mock streams.
#[expect(
    clippy::too_many_arguments,
    reason = "internal plumbing, not a public API surface"
)]
async fn run_stdout_reader<R: tokio::io::AsyncRead + Unpin>(
    reader: R,
    event_tx: mpsc::Sender<SessionEvent>,
    init_tx: mpsc::Sender<CcIncoming>,
    outgoing_tx: mpsc::Sender<OutgoingEnvelope>,
    transcript: Arc<TranscriptWriter>,
    alert_dispatcher: AlertDispatcher,
    alive: Arc<AtomicBool>,
    shutting_down: Arc<AtomicBool>,
) {
    let mut reader = NdjsonReader::new(reader);
    let mut init_done = false;
    let mut pending_approvals: HashMap<String, PendingApproval> = HashMap::new();
    // Approval wait tasks send their request_id here when they complete,
    // so we can clean up the pending_approvals entry. Unbounded because
    // these are tiny Strings and the count is bounded by approvals in the session.
    let (approval_done_tx, mut approval_done_rx) = mpsc::unbounded_channel::<String>();

    loop {
        // Drain completed approvals (non-blocking).
        while let Ok(request_id) = approval_done_rx.try_recv() {
            pending_approvals.remove(&request_id);
        }

        match reader.next().await {
            Ok(Some((msg, raw_line))) => {
                if let Err(e) = transcript.log_received(&raw_line).await {
                    tracing::error!(error = %e, "failed to write to transcript");
                }

                // During init handshake, route to init channel.
                if !init_done {
                    if matches!(&msg, CcIncoming::ControlResponse { .. }) {
                        if init_tx.send(msg).await.is_err() {
                            tracing::debug!("init receiver dropped during handshake");
                            break;
                        }
                        init_done = true;
                        continue;
                    }
                    if init_tx.send(msg).await.is_err() {
                        tracing::debug!("init receiver dropped during handshake");
                        break;
                    }
                    continue;
                }

                // Normal mode — route by message type.
                let send_ok = match msg {
                    CcIncoming::System(ref sys @ SystemMessage::Init { .. }) => {
                        let info = SessionInfo::from_system_init(sys);
                        event_tx.send(SessionEvent::Initialized(info)).await.is_ok()
                    }
                    CcIncoming::System(SystemMessage::Status { status, extra, .. }) => {
                        let compact_result = extra
                            .get("compact_result")
                            .and_then(|v| v.as_str())
                            .map(String::from);
                        tracing::debug!(?status, ?compact_result, "CC status change");
                        event_tx
                            .send(SessionEvent::StatusChange {
                                status,
                                compact_result,
                            })
                            .await
                            .is_ok()
                    }
                    CcIncoming::System(SystemMessage::CompactBoundary {
                        compact_metadata, ..
                    }) => {
                        tracing::info!(?compact_metadata, "compact boundary");
                        event_tx
                            .send(SessionEvent::CompactBoundary {
                                metadata: compact_metadata,
                            })
                            .await
                            .is_ok()
                    }
                    // Task tool lifecycle: informational-only; Brenn's turn
                    // model has no slot for subtask telemetry today.
                    CcIncoming::System(SystemMessage::TaskStarted {
                        task_id,
                        description,
                        task_type,
                        ..
                    }) => {
                        tracing::debug!(?task_id, ?description, ?task_type, "CC task started");
                        true
                    }
                    CcIncoming::System(SystemMessage::TaskProgress {
                        task_id,
                        description,
                        last_tool_name,
                        ..
                    }) => {
                        tracing::debug!(
                            ?task_id,
                            ?description,
                            ?last_tool_name,
                            "CC task progress"
                        );
                        true
                    }
                    CcIncoming::System(SystemMessage::TaskNotification {
                        task_id,
                        status,
                        summary,
                        ..
                    }) => {
                        tracing::debug!(?task_id, ?status, ?summary, "CC task notification");
                        true
                    }
                    CcIncoming::System(SystemMessage::TaskUpdated { task_id, patch, .. }) => {
                        tracing::debug!(?task_id, ?patch, "CC task updated");
                        true
                    }
                    CcIncoming::System(SystemMessage::Unknown) => {
                        // CC sent a system message with an unrecognized subtype.
                        // Fire-and-forget from CC's side, so safe to continue —
                        // but alert because this probably means CC was upgraded.
                        // `#[serde(other)]` discards the subtype, so we
                        // re-parse it from `raw_line` for the dedup key.
                        // Fallback "unknown" if the field is missing (also
                        // unusual; the alert body surfaces the raw line).
                        // #[serde(other)] on SystemMessage::Unknown discards
                        // the subtype string; recover it from the raw line for
                        // the dedup key. Narrowing to a typed struct is not
                        // worth the churn — this path fires only on unknown
                        // subtypes (CC upgrade introduces a new one).
                        let subtype = serde_json::from_str::<serde_json::Value>(&raw_line)
                            .ok()
                            .and_then(|v| {
                                v.get("subtype")
                                    .and_then(|s| s.as_str())
                                    .map(|s| s.to_string())
                            })
                            .unwrap_or_else(|| "unknown".to_string());
                        tracing::warn!(
                            subtype = %subtype,
                            raw_line = %raw_line,
                            "unknown system message subtype — possible CC upgrade"
                        );
                        alert_dispatcher.alert_once_per_process(
                            AlertSeverity::Warning,
                            "Unknown CC system message subtype".into(),
                            &subtype,
                            format!("CC sent a system message with an unrecognized subtype ({subtype}). This likely means CC was upgraded and Brenn needs updating.\nRaw: {raw_line}"),
                        );
                        true
                    }
                    CcIncoming::Assistant(asst) => event_tx
                        .send(SessionEvent::AssistantMessage(asst))
                        .await
                        .is_ok(),
                    CcIncoming::StreamEvent(se) => {
                        event_tx.send(SessionEvent::StreamEvent(se)).await.is_ok()
                    }
                    CcIncoming::User(u) => event_tx.send(SessionEvent::ToolResult(u)).await.is_ok(),
                    CcIncoming::ControlRequest {
                        request_id,
                        request,
                    } => {
                        route_control_request(
                            request_id,
                            request,
                            &event_tx,
                            &outgoing_tx,
                            &mut pending_approvals,
                            &approval_done_tx,
                        )
                        .await
                    }
                    CcIncoming::ControlCancelRequest { request_id } => {
                        // Cancel the pending wait task by dropping cancel_tx.
                        // The wait task selects on cancel_rx and will clean up.
                        pending_approvals.remove(&request_id);
                        // Always emit so the consumer can dismiss approval UI.
                        event_tx
                            .send(SessionEvent::ApprovalCancelled {
                                request_id: request_id.clone(),
                            })
                            .await
                            .is_ok()
                    }
                    CcIncoming::ControlResponse { response } => {
                        // Post-init control response (e.g., ack for interrupt).
                        tracing::debug!(
                            request_id = ?response.request_id,
                            subtype = %response.subtype,
                            "control_response (post-init)"
                        );
                        true
                    }
                    CcIncoming::Result(res) => {
                        // Turn complete. Clear pending approvals — any in-flight
                        // approval requests are now invalid (CC moved on).
                        pending_approvals.clear();
                        // Emit TurnCompleted but do NOT break or set alive=false.
                        // CC stays alive, waiting for the next user message.
                        event_tx
                            .send(SessionEvent::TurnCompleted(res))
                            .await
                            .is_ok()
                    }
                    CcIncoming::RateLimitEvent(rle) => {
                        event_tx.send(SessionEvent::RateLimit(rle)).await.is_ok()
                    }
                };

                if !send_ok {
                    tracing::debug!("event receiver dropped — consumer gone");
                    break;
                }
            }
            Ok(None) => {
                // EOF — CC process exited without result message.
                alive.store(false, Ordering::Relaxed);
                if event_tx
                    .send(SessionEvent::Died(CcError::ProcessDied {
                        exit_status: None,
                    }))
                    .await
                    .is_err()
                {
                    tracing::debug!("consumer gone when sending Died (EOF)");
                }
                if shutting_down.load(Ordering::SeqCst) {
                    tracing::info!("CC stdout closed (intentional shutdown)");
                } else {
                    alert_dispatcher.alert(
                        AlertSeverity::Critical,
                        "CC process died".into(),
                        "CC stdout closed without a result message".into(),
                    );
                }
                break;
            }
            Err(TransportError::ParseError { line, error }) => {
                if let Err(e) = transcript.log_received(&line).await {
                    tracing::error!(error = %e, "failed to write parse-failed line to transcript");
                }

                // If this looks like a control_request, CC is waiting for a response
                // we can't give (unknown subtype). Kill the session — per philosophy,
                // we refuse to guess at the right response.
                let is_control_request = serde_json::from_str::<serde_json::Value>(&line)
                    .ok()
                    .and_then(|v| v.get("type")?.as_str().map(|s| s == "control_request"))
                    .unwrap_or(false);

                if is_control_request {
                    tracing::error!(
                        raw_line = %line,
                        error = %error,
                        "unknown control_request subtype — killing session (can't respond safely)"
                    );
                    alert_dispatcher.alert(
                        AlertSeverity::Critical,
                        "Unknown CC control request — session killed".into(),
                        format!("CC sent a control_request we can't parse. This likely means CC was upgraded and Brenn needs updating.\nParse error: {error}\nRaw: {line}"),
                    );
                    alive.store(false, Ordering::Relaxed);
                    if event_tx
                        .send(SessionEvent::Died(CcError::UnknownControlRequest {
                            raw_line: line,
                        }))
                        .await
                        .is_err()
                    {
                        tracing::debug!(
                            "consumer gone when sending Died (unknown control_request)"
                        );
                    }
                    break;
                }

                // Non-control parse failure: log, alert, surface, continue.
                // Dedup on the serde_json error category — a stable enum
                // (Syntax/Data/Io/Eof) that doesn't change with error format
                // wording, preventing an alert-storm if serde_json rewrites
                // its position-info string.
                tracing::warn!(
                    raw_line = %line,
                    error = %error,
                    "unrecognized message from CC — possible protocol upgrade"
                );
                let dedup_key = format!("{:?}", error.classify());
                alert_dispatcher.alert_once_per_process(
                    AlertSeverity::Warning,
                    "Unrecognized CC message".into(),
                    &dedup_key,
                    format!("Parse error: {error}\nRaw: {line}"),
                );
                if event_tx
                    .send(SessionEvent::UnrecognizedMessage { raw_line: line })
                    .await
                    .is_err()
                {
                    tracing::debug!("event receiver dropped — consumer gone");
                    break;
                }
            }
            Err(e) => {
                // Fatal transport error (I/O error, line too long).
                alive.store(false, Ordering::Relaxed);
                if event_tx
                    .send(SessionEvent::Died(CcError::Transport(e)))
                    .await
                    .is_err()
                {
                    tracing::debug!("consumer gone when sending Died (transport error)");
                }
                break;
            }
        }
    }

    tracing::info!("CC stdout reader task exited");
}

/// Route a control_request from CC to the approval handler.
///
/// Returns true if the event was sent to the consumer, false if the
/// consumer's channel is closed.
async fn route_control_request(
    request_id: String,
    request: CcControlRequest,
    event_tx: &mpsc::Sender<SessionEvent>,
    outgoing_tx: &mpsc::Sender<OutgoingEnvelope>,
    pending_approvals: &mut HashMap<String, PendingApproval>,
    approval_done_tx: &mpsc::UnboundedSender<String>,
) -> bool {
    // Create two oneshot pairs:
    // 1. response: consumer sends decision → wait task receives and routes to CC
    // 2. cancel: we drop cancel_tx → wait task sees cancel and cleans up
    let (response_tx, response_rx) = oneshot::channel();
    let (cancel_tx, cancel_rx) = oneshot::channel();

    let kind = match request {
        CcControlRequest::CanUseTool {
            tool_name,
            tool_use_id,
            input,
            ..
        } => ApprovalKind::Permission {
            tool_name,
            tool_use_id,
            input,
        },
        CcControlRequest::HookCallback {
            callback_id, input, ..
        } => {
            let event_name = input.hook_event_name.clone();
            match event_name.as_str() {
                "PreToolUse" => ApprovalKind::PreToolUse {
                    callback_id,
                    tool_name: input.tool_name.unwrap_or_default(),
                    tool_input: input.tool_input.unwrap_or(serde_json::Value::Null),
                    tool_use_id: input.tool_use_id.unwrap_or_default(),
                },
                "PostToolUse" | "PostToolUseFailure" => ApprovalKind::PostToolUse {
                    callback_id,
                    tool_name: input.tool_name.unwrap_or_default(),
                    tool_input: input.tool_input.unwrap_or(serde_json::Value::Null),
                    tool_response: input.tool_response.unwrap_or(serde_json::Value::Null),
                    tool_use_id: input.tool_use_id.unwrap_or_default(),
                },
                _ => ApprovalKind::OtherHook {
                    callback_id,
                    event_name,
                },
            }
        }
    };

    // Store cancel token so control_cancel_request can abort the wait task.
    pending_approvals.insert(request_id.clone(), PendingApproval { cancel_tx });

    // Spawn the wait task that bridges consumer decision → CC response.
    spawn_approval_wait_task(
        request_id.clone(),
        kind.to_summary(),
        response_rx,
        cancel_rx,
        outgoing_tx.clone(),
        approval_done_tx.clone(),
    );

    // Send approval request to consumer. Consumer sends decision via response_tx.
    let approval_req = ApprovalRequest {
        request_id,
        kind,
        response_tx,
    };
    event_tx
        .send(SessionEvent::ApprovalRequired(approval_req))
        .await
        .is_ok()
}

/// Spawn a task that waits for the consumer's approval decision and routes
/// the appropriate CC response through the outgoing channel.
fn spawn_approval_wait_task(
    request_id: String,
    kind_summary: ApprovalKindSummary,
    response_rx: oneshot::Receiver<ApprovalDecision>,
    cancel_rx: oneshot::Receiver<()>,
    outgoing_tx: mpsc::Sender<OutgoingEnvelope>,
    approval_done_tx: mpsc::UnboundedSender<String>,
) {
    tokio::spawn(async move {
        // Wait for either consumer decision or cancellation.
        tokio::select! {
            decision_result = response_rx => {
                match decision_result {
                    Ok(decision) => {
                        let cc_response = build_cc_response(
                            &request_id,
                            &kind_summary,
                            &decision,
                        );
                        // Approval responses are fire-and-forget (no pending-push row; no ack needed).
                        if outgoing_tx.send(OutgoingEnvelope { msg: cc_response, ack: None }).await.is_err() {
                            tracing::error!(
                                request_id = %request_id,
                                "failed to send approval response — outgoing channel closed"
                            );
                        }
                    }
                    Err(_) => {
                        // Consumer dropped response_tx without sending.
                        tracing::debug!(
                            request_id = %request_id,
                            "consumer dropped approval without responding"
                        );
                    }
                }
            }
            _ = cancel_rx => {
                // Cancelled by control_cancel_request. Do NOT send a response
                // to CC — cancel means CC no longer expects one.
                tracing::debug!(
                    request_id = %request_id,
                    "approval wait cancelled by CC"
                );
            }
        }
        // Notify the reader loop to clean up our pending_approvals entry.
        // If the reader loop already exited, the channel is closed and cleanup is moot
        // (the HashMap is gone too).
        if approval_done_tx.send(request_id).is_err() {
            tracing::debug!("approval cleanup channel closed — reader loop exited");
        }
    });
}

/// Summary of an ApprovalKind used by the wait task to build the CC response.
/// We need this because ApprovalRequest takes ownership of the ApprovalKind,
/// but the wait task needs to know what kind it was to build the right response.
enum ApprovalKindSummary {
    Permission,
    PreToolUse,
    PostToolUse,
    OtherHook { event_name: String },
}

impl ApprovalKind {
    fn to_summary(&self) -> ApprovalKindSummary {
        match self {
            Self::Permission { .. } => ApprovalKindSummary::Permission,
            Self::PreToolUse { .. } => ApprovalKindSummary::PreToolUse,
            Self::PostToolUse { .. } => ApprovalKindSummary::PostToolUse,
            Self::OtherHook { event_name, .. } => ApprovalKindSummary::OtherHook {
                event_name: event_name.clone(),
            },
        }
    }
}

/// Build the correct CC response message based on the approval kind and decision.
fn build_cc_response(
    request_id: &str,
    kind: &ApprovalKindSummary,
    decision: &ApprovalDecision,
) -> CcOutgoing {
    match (kind, decision) {
        (ApprovalKindSummary::Permission, ApprovalDecision::Allow { updated_input }) => {
            // CC protocol requires updated_input for Permission Allow.
            // Callers must always provide it (echoing original if unchanged).
            // handle_approval() stashes the original input and fills it in as
            // a fallback, so None here means a bug in the caller.
            let input = updated_input
                .as_ref()
                .expect("Permission Allow requires updated_input — caller bug");
            protocol::permission_allow(request_id, input)
        }
        (ApprovalKindSummary::Permission, ApprovalDecision::Deny { reason }) => {
            protocol::permission_deny(request_id, reason)
        }
        (ApprovalKindSummary::PreToolUse, ApprovalDecision::Allow { updated_input }) => {
            protocol::hook_pre_allow(request_id, updated_input.as_ref())
        }
        (ApprovalKindSummary::PreToolUse, ApprovalDecision::Deny { reason }) => {
            protocol::hook_pre_deny(request_id, reason)
        }
        (ApprovalKindSummary::PreToolUse, ApprovalDecision::Continue { .. }) => {
            protocol::hook_pre_no_opinion(request_id)
        }
        (ApprovalKindSummary::PostToolUse, ApprovalDecision::Continue { updated_output }) => {
            protocol::hook_post(request_id, updated_output.as_deref())
        }
        (ApprovalKindSummary::OtherHook { event_name }, ApprovalDecision::Continue { .. }) => {
            protocol::hook_continue(request_id, event_name)
        }
        (kind, decision) => {
            panic!(
                "mismatched approval kind/decision: {:?} / {decision:?}",
                match kind {
                    ApprovalKindSummary::Permission => "Permission",
                    ApprovalKindSummary::PreToolUse => "PreToolUse",
                    ApprovalKindSummary::PostToolUse => "PostToolUse",
                    ApprovalKindSummary::OtherHook { .. } => "OtherHook",
                }
            );
        }
    }
}

/// Spawn the stdin writer task.
///
/// Generic over any `AsyncWrite + Unpin + Send + 'static` sink so that tests
/// can pass a `tokio::io::DuplexStream` or `Vec<u8>` pipe in place of the
/// real `ChildStdin`. Production callers pass the actual `ChildStdin`.
pub fn spawn_stdin_writer<W>(
    stdin: W,
    mut outgoing_rx: mpsc::Receiver<OutgoingEnvelope>,
    transcript: Arc<TranscriptWriter>,
) -> tokio::task::JoinHandle<()>
where
    W: tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut writer = NdjsonWriter::new(stdin);
        while let Some(OutgoingEnvelope { msg, ack }) = outgoing_rx.recv().await {
            match writer.send(&msg).await {
                Ok(raw_line) => {
                    if let Err(e) = transcript.log_sent(&raw_line).await {
                        tracing::error!(error = %e, "failed to write to transcript");
                    }
                    // Fire flush ack after successful write_all + flush.
                    // `let _ =`: flush succeeded; a send error means only the receiver was
                    // dropped (caller no longer awaiting, e.g. fan-out task cancelled).
                    // Nothing to handle — the message is already on the wire.
                    if let Some(ack_tx) = ack {
                        let _ = ack_tx.send(Ok(()));
                    }
                }
                Err(e) => {
                    tracing::error!(error = %e, "failed to write to CC stdin");
                    // Fire flush ack with the error before breaking.
                    // `let _ =`: flush did not succeed; a dropped receiver means the caller
                    // already stopped awaiting. The row stays parked regardless
                    // (both ack-failure and receiver-gone paths leave delivered_at IS NULL).
                    if let Some(ack_tx) = ack {
                        let _ = ack_tx.send(Err(e));
                    }
                    // Count queued-but-unwritten envelopes whose ack senders will be
                    // dropped when outgoing_rx is dropped below. These ack senders resolve
                    // with RecvError for their awaiting callers (rows stay delivered_at IS
                    // NULL → redelivered on restart). Log the count so post-mortem
                    // diagnosis shows how many messages were in the queue, not just the
                    // one that failed.
                    let mut dropped_count = 0usize;
                    while outgoing_rx.try_recv().is_ok() {
                        dropped_count += 1;
                    }
                    if dropped_count > 0 {
                        tracing::warn!(
                            dropped_count,
                            "stdin writer exiting after write failure; \
                             {} queued messages were not sent (their push rows \
                             stay undelivered and will redeliver on restart)",
                            dropped_count
                        );
                    }
                    break;
                }
            }
        }
        tracing::info!("CC stdin writer task exited");
    })
}

/// Spawn the stderr drain task.
pub fn spawn_stderr_drain(stderr: ChildStderr) {
    tokio::spawn(async move {
        let reader = BufReader::new(stderr);
        let mut lines = reader.lines();
        while let Ok(Some(line)) = lines.next_line().await {
            tracing::debug!(target: "cc_stderr", "{}", line);
        }
        tracing::info!("CC stderr drain task exited");
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- build_cc_response tests ---

    #[test]
    fn permission_allow_builds_correct_response() {
        let input = serde_json::json!({"file_path": "/tmp/foo"});
        let decision = ApprovalDecision::Allow {
            updated_input: Some(input.clone()),
        };
        let resp = build_cc_response("req_1", &ApprovalKindSummary::Permission, &decision);
        let json = serde_json::to_value(&resp).expect("serialize");
        assert_eq!(json["response"]["response"]["behavior"], "allow");
        assert_eq!(
            json["response"]["response"]["updatedInput"]["file_path"],
            "/tmp/foo"
        );
    }

    #[test]
    fn permission_deny_builds_correct_response() {
        let decision = ApprovalDecision::Deny {
            reason: "blocked".into(),
        };
        let resp = build_cc_response("req_2", &ApprovalKindSummary::Permission, &decision);
        let json = serde_json::to_value(&resp).expect("serialize");
        assert_eq!(json["response"]["response"]["behavior"], "deny");
        assert_eq!(json["response"]["response"]["message"], "blocked");
    }

    #[test]
    fn pre_tool_use_allow_builds_correct_response() {
        let decision = ApprovalDecision::Allow {
            updated_input: None,
        };
        let resp = build_cc_response("req_3", &ApprovalKindSummary::PreToolUse, &decision);
        let json = serde_json::to_value(&resp).expect("serialize");
        let hook = &json["response"]["response"]["hookSpecificOutput"];
        assert_eq!(hook["hookEventName"], "PreToolUse");
        assert_eq!(hook["permissionDecision"], "allow");
    }

    #[test]
    fn pre_tool_use_deny_builds_correct_response() {
        let decision = ApprovalDecision::Deny {
            reason: "nope".into(),
        };
        let resp = build_cc_response("req_4", &ApprovalKindSummary::PreToolUse, &decision);
        let json = serde_json::to_value(&resp).expect("serialize");
        let hook = &json["response"]["response"]["hookSpecificOutput"];
        assert_eq!(hook["permissionDecision"], "deny");
        assert_eq!(hook["permissionDecisionReason"], "nope");
    }

    #[test]
    fn pre_tool_use_continue_builds_no_opinion_response() {
        let decision = ApprovalDecision::Continue {
            updated_output: None,
        };
        let resp = build_cc_response(
            "req_no_opinion",
            &ApprovalKindSummary::PreToolUse,
            &decision,
        );
        let json = serde_json::to_value(&resp).expect("serialize");
        let hook = &json["response"]["response"]["hookSpecificOutput"];
        assert_eq!(hook["hookEventName"], "PreToolUse");
        assert!(
            hook.get("permissionDecision").is_none(),
            "no-opinion response must not include permissionDecision"
        );
    }

    #[test]
    fn post_tool_use_continue_builds_correct_response() {
        let decision = ApprovalDecision::Continue {
            updated_output: Some("real output".into()),
        };
        let resp = build_cc_response("req_5", &ApprovalKindSummary::PostToolUse, &decision);
        let json = serde_json::to_value(&resp).expect("serialize");
        let hook = &json["response"]["response"]["hookSpecificOutput"];
        assert_eq!(hook["hookEventName"], "PostToolUse");
        assert_eq!(hook["updatedMCPToolOutput"], "real output");
    }

    #[test]
    fn other_hook_continue_builds_correct_response() {
        let decision = ApprovalDecision::Continue {
            updated_output: None,
        };
        let kind = ApprovalKindSummary::OtherHook {
            event_name: "Stop".into(),
        };
        let resp = build_cc_response("req_6", &kind, &decision);
        let json = serde_json::to_value(&resp).expect("serialize");
        let hook = &json["response"]["response"]["hookSpecificOutput"];
        assert_eq!(hook["hookEventName"], "Stop");
    }

    #[test]
    #[should_panic(expected = "mismatched")]
    fn mismatched_kind_decision_panics() {
        let decision = ApprovalDecision::Continue {
            updated_output: None,
        };
        build_cc_response("req_7", &ApprovalKindSummary::Permission, &decision);
    }

    // --- Approval routing integration tests ---

    #[tokio::test]
    async fn approval_flow_permission_allow() {
        let (event_tx, mut event_rx) = mpsc::channel::<SessionEvent>(16);
        let (outgoing_tx, mut outgoing_rx) = mpsc::channel::<OutgoingEnvelope>(16);
        let mut pending = HashMap::new();

        let request = CcControlRequest::CanUseTool {
            tool_name: "Write".into(),
            tool_use_id: "toolu_1".into(),
            input: serde_json::json!({"file_path": "/tmp/foo"}),
            permission_suggestions: None,
            decision_reason: None,
            extra: serde_json::Value::Null,
        };

        let (approval_done_tx, _approval_done_rx) = mpsc::unbounded_channel();
        route_control_request(
            "req_10".into(),
            request,
            &event_tx,
            &outgoing_tx,
            &mut pending,
            &approval_done_tx,
        )
        .await;

        // Consumer receives approval request.
        let event = event_rx.recv().await.expect("should get event");
        match event {
            SessionEvent::ApprovalRequired(req) => {
                assert_eq!(req.request_id, "req_10");
                assert!(matches!(req.kind, ApprovalKind::Permission { .. }));
                // Consumer approves.
                req.response_tx
                    .send(ApprovalDecision::Allow {
                        updated_input: Some(serde_json::json!({"file_path": "/tmp/foo"})),
                    })
                    .expect("send decision");
            }
            other => panic!("expected ApprovalRequired, got {other:?}"),
        }

        // Wait for the CC response to appear on the outgoing channel.
        // Approval responses are fire-and-forget (ack: None).
        let envelope =
            tokio::time::timeout(tokio::time::Duration::from_secs(1), outgoing_rx.recv())
                .await
                .expect("should not timeout")
                .expect("should get response");

        assert!(
            envelope.ack.is_none(),
            "approval responses must be fire-and-forget"
        );
        let json = serde_json::to_value(&envelope.msg).expect("serialize");
        assert_eq!(json["response"]["response"]["behavior"], "allow");
        assert_eq!(json["response"]["request_id"], "req_10");
    }

    #[tokio::test]
    async fn approval_flow_hook_pre_deny() {
        let (event_tx, mut event_rx) = mpsc::channel::<SessionEvent>(16);
        let (outgoing_tx, mut outgoing_rx) = mpsc::channel::<OutgoingEnvelope>(16);
        let mut pending = HashMap::new();

        let request = CcControlRequest::HookCallback {
            callback_id: "hook_pre_tool_0".into(),
            tool_use_id: Some("toolu_2".into()),
            input: HookInput {
                hook_event_name: "PreToolUse".into(),
                tool_name: Some("Bash".into()),
                tool_input: Some(serde_json::json!({"command": "rm -rf /"})),
                tool_use_id: Some("toolu_2".into()),
                tool_response: None,
                session_id: None,
                cwd: None,
                extra: serde_json::Value::Null,
            },
        };

        let (approval_done_tx, _approval_done_rx) = mpsc::unbounded_channel();
        route_control_request(
            "req_11".into(),
            request,
            &event_tx,
            &outgoing_tx,
            &mut pending,
            &approval_done_tx,
        )
        .await;

        let event = event_rx.recv().await.expect("should get event");
        match event {
            SessionEvent::ApprovalRequired(req) => {
                assert!(matches!(req.kind, ApprovalKind::PreToolUse { .. }));
                req.response_tx
                    .send(ApprovalDecision::Deny {
                        reason: "dangerous".into(),
                    })
                    .expect("send decision");
            }
            other => panic!("expected ApprovalRequired, got {other:?}"),
        }

        let envelope =
            tokio::time::timeout(tokio::time::Duration::from_secs(1), outgoing_rx.recv())
                .await
                .expect("should not timeout")
                .expect("should get response");

        let json = serde_json::to_value(&envelope.msg).expect("serialize");
        let hook = &json["response"]["response"]["hookSpecificOutput"];
        assert_eq!(hook["permissionDecision"], "deny");
        assert_eq!(hook["permissionDecisionReason"], "dangerous");
    }

    #[tokio::test]
    async fn cancel_prevents_response() {
        let (event_tx, mut event_rx) = mpsc::channel::<SessionEvent>(16);
        let (outgoing_tx, mut outgoing_rx) = mpsc::channel::<OutgoingEnvelope>(16);
        let mut pending = HashMap::new();

        let request = CcControlRequest::CanUseTool {
            tool_name: "Write".into(),
            tool_use_id: "toolu_3".into(),
            input: serde_json::json!({}),
            permission_suggestions: None,
            decision_reason: None,
            extra: serde_json::Value::Null,
        };

        let (approval_done_tx, _approval_done_rx) = mpsc::unbounded_channel();
        route_control_request(
            "req_12".into(),
            request,
            &event_tx,
            &outgoing_tx,
            &mut pending,
            &approval_done_tx,
        )
        .await;

        // Consumer receives the approval request but hasn't responded yet.
        let event = event_rx.recv().await.expect("should get event");
        assert!(matches!(event, SessionEvent::ApprovalRequired(_)));

        // Cancel arrives — drop the pending approval.
        pending.remove("req_12");

        // Give the wait task time to notice the cancel.
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // No CC response should have been sent.
        assert!(outgoing_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn post_tool_use_continues_with_output() {
        let (event_tx, mut event_rx) = mpsc::channel::<SessionEvent>(16);
        let (outgoing_tx, mut outgoing_rx) = mpsc::channel::<OutgoingEnvelope>(16);
        let mut pending = HashMap::new();

        let request = CcControlRequest::HookCallback {
            callback_id: "hook_post_0".into(),
            tool_use_id: Some("toolu_4".into()),
            input: HookInput {
                hook_event_name: "PostToolUse".into(),
                tool_name: Some("mcp__pfin__context".into()),
                tool_input: Some(serde_json::json!({"account": "Checking"})),
                tool_use_id: Some("toolu_4".into()),
                tool_response: Some(serde_json::json!([{"text": "__NOOP__"}])),
                session_id: None,
                cwd: None,
                extra: serde_json::Value::Null,
            },
        };

        let (approval_done_tx, _approval_done_rx) = mpsc::unbounded_channel();
        route_control_request(
            "req_13".into(),
            request,
            &event_tx,
            &outgoing_tx,
            &mut pending,
            &approval_done_tx,
        )
        .await;

        let event = event_rx.recv().await.expect("should get event");
        match event {
            SessionEvent::ApprovalRequired(req) => {
                assert!(matches!(req.kind, ApprovalKind::PostToolUse { .. }));
                req.response_tx
                    .send(ApprovalDecision::Continue {
                        updated_output: Some("real balance data".into()),
                    })
                    .expect("send decision");
            }
            other => panic!("expected ApprovalRequired, got {other:?}"),
        }

        let envelope =
            tokio::time::timeout(tokio::time::Duration::from_secs(1), outgoing_rx.recv())
                .await
                .expect("should not timeout")
                .expect("should get response");

        let json = serde_json::to_value(&envelope.msg).expect("serialize");
        let hook = &json["response"]["response"]["hookSpecificOutput"];
        assert_eq!(hook["updatedMCPToolOutput"], "real balance data");
    }

    // --- Full reader loop integration tests ---
    //
    // These test run_stdout_reader with mock byte streams, exercising the
    // full message routing pipeline.

    use brenn_lib::obs::alerting::{RateLimiter, noop_alert_dispatcher};

    /// Test fixture for reader loop integration tests.
    #[allow(dead_code)]
    struct ReaderTestFixture {
        event_tx: mpsc::Sender<SessionEvent>,
        event_rx: mpsc::Receiver<SessionEvent>,
        init_tx: mpsc::Sender<CcIncoming>,
        init_rx: mpsc::Receiver<CcIncoming>,
        outgoing_tx: mpsc::Sender<OutgoingEnvelope>,
        outgoing_rx: mpsc::Receiver<OutgoingEnvelope>,
        alert: AlertDispatcher,
        alive: Arc<AtomicBool>,
        shutting_down: Arc<AtomicBool>,
        alert_handle: tokio::task::JoinHandle<()>,
        transcript: Arc<TranscriptWriter>,
        _transcript_dir: tempfile::TempDir,
    }

    /// Helper: create a transcript writer backed by a temporary directory.
    fn make_test_transcript() -> (Arc<TranscriptWriter>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let transcript = Arc::new(TranscriptWriter::new(dir.path(), "test.log").unwrap());
        (transcript, dir)
    }

    /// Helper: set up reader loop infrastructure.
    fn setup_reader_test() -> ReaderTestFixture {
        // Alert dispatcher for tests (noop alerter).
        let (alert_dispatcher, alert_handle) = noop_alert_dispatcher();

        let (event_tx, event_rx) = mpsc::channel(64);
        let (init_tx, init_rx) = mpsc::channel(16);
        let (outgoing_tx, outgoing_rx) = mpsc::channel(64);
        let alive = Arc::new(AtomicBool::new(true));
        let shutting_down = Arc::new(AtomicBool::new(false));
        let (transcript, _transcript_dir) = make_test_transcript();

        ReaderTestFixture {
            event_tx,
            event_rx,
            init_tx,
            init_rx,
            outgoing_tx,
            outgoing_rx,
            alert: alert_dispatcher,
            alive,
            shutting_down,
            alert_handle,
            transcript,
            _transcript_dir,
        }
    }

    #[tokio::test]
    async fn reader_loop_routes_system_init() {
        let ReaderTestFixture {
            event_tx,
            mut event_rx,
            init_tx,
            init_rx: _init_rx,
            outgoing_tx,
            outgoing_rx: _outgoing_rx,
            alert,
            alive,
            shutting_down,
            alert_handle: _ah,
            transcript,
            _transcript_dir,
        } = setup_reader_test();

        // Feed a control_response (init ack) then a system/init then EOF.
        let input = concat!(
            r#"{"type":"control_response","response":{"subtype":"success","request_id":"req_0"}}"#,
            "\n",
            r#"{"type":"system","subtype":"init","session_id":"sess-1","tools":["Read"],"model":"haiku","cwd":"/tmp"}"#,
            "\n",
        );

        tokio::spawn(run_stdout_reader(
            input.as_bytes(),
            event_tx,
            init_tx,
            outgoing_tx,
            transcript,
            alert,
            alive.clone(),
            shutting_down.clone(),
        ));

        // First event: Initialized from system/init.
        let event = tokio::time::timeout(tokio::time::Duration::from_secs(1), event_rx.recv())
            .await
            .expect("no timeout")
            .expect("should get event");

        match event {
            SessionEvent::Initialized(info) => {
                assert_eq!(info.session_id, "sess-1");
                assert_eq!(info.model, "haiku");
                assert_eq!(info.tools, vec!["Read"]);
            }
            other => panic!("expected Initialized, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn reader_loop_unknown_system_subtype_continues() {
        let ReaderTestFixture {
            event_tx,
            mut event_rx,
            init_tx,
            init_rx: _init_rx,
            outgoing_tx,
            outgoing_rx: _outgoing_rx,
            alert,
            alive,
            shutting_down,
            alert_handle: _ah,
            transcript,
            _transcript_dir,
        } = setup_reader_test();

        // Feed init ack, then an unknown system subtype, then a result.
        // The unknown system subtype should be handled (alert + continue),
        // and the result should still arrive.
        //
        // Uses a made-up subtype (not one of the task_* variants we've
        // since taught Brenn about) so this test actually exercises the
        // SystemMessage::Unknown arm, not a typed passthrough.
        let input = concat!(
            r#"{"type":"control_response","response":{"subtype":"success","request_id":"req_0"}}"#,
            "\n",
            r#"{"type":"system","subtype":"subtype_from_the_future","foo":"bar"}"#,
            "\n",
            r#"{"type":"result","subtype":"success","is_error":false}"#,
            "\n",
        );

        tokio::spawn(run_stdout_reader(
            input.as_bytes(),
            event_tx,
            init_tx,
            outgoing_tx,
            transcript,
            alert,
            alive.clone(),
            shutting_down.clone(),
        ));

        // The unknown system subtype is handled inline (alert, no event emitted).
        // Next event should be TurnCompleted from the result message.
        let event = tokio::time::timeout(tokio::time::Duration::from_secs(1), event_rx.recv())
            .await
            .expect("no timeout")
            .expect("event");

        assert!(matches!(event, SessionEvent::TurnCompleted(_)));

        // After TurnCompleted, alive is still true (CC stays alive in persistent mode).
        // The reader continues, hits EOF (end of test input), and emits Died — but
        // that's asynchronous. We verify that TurnCompleted itself doesn't kill the session.
        // (The Died event will set alive=false, but we don't wait for it here.)
    }

    #[tokio::test]
    async fn reader_loop_routes_status_change() {
        let ReaderTestFixture {
            event_tx,
            mut event_rx,
            init_tx,
            init_rx: _init_rx,
            outgoing_tx,
            outgoing_rx: _outgoing_rx,
            alert,
            alive,
            shutting_down,
            alert_handle: _ah,
            transcript,
            _transcript_dir,
        } = setup_reader_test();

        let input = concat!(
            r#"{"type":"control_response","response":{"subtype":"success","request_id":"req_0"}}"#,
            "\n",
            r#"{"type":"system","subtype":"status","status":"compacting","session_id":"sess-1"}"#,
            "\n",
            r#"{"type":"system","subtype":"status","status":null,"session_id":"sess-1"}"#,
            "\n",
        );

        tokio::spawn(run_stdout_reader(
            input.as_bytes(),
            event_tx,
            init_tx,
            outgoing_tx,
            transcript,
            alert,
            alive.clone(),
            shutting_down.clone(),
        ));

        // First event: StatusChange with "compacting".
        let event = tokio::time::timeout(tokio::time::Duration::from_secs(1), event_rx.recv())
            .await
            .expect("no timeout")
            .expect("event");
        match event {
            SessionEvent::StatusChange {
                status,
                compact_result,
            } => {
                assert_eq!(status.as_deref(), Some("compacting"));
                assert!(compact_result.is_none());
            }
            other => panic!("expected StatusChange, got {other:?}"),
        }

        // Second event: StatusChange with None (status cleared).
        let event = tokio::time::timeout(tokio::time::Duration::from_secs(1), event_rx.recv())
            .await
            .expect("no timeout")
            .expect("event");
        match event {
            SessionEvent::StatusChange {
                status,
                compact_result,
            } => {
                assert!(status.is_none());
                assert!(compact_result.is_none());
            }
            other => panic!("expected StatusChange(None), got {other:?}"),
        }
    }

    /// Verify that the compact_result extraction path in tasks.rs correctly parses
    /// the wire-format compact_result field into SessionEvent::StatusChange.compact_result.
    /// This covers the tasks.rs JSON→extra→compact_result extraction, which the
    /// cc_event_loop tests bypass by injecting StatusChange directly.
    #[tokio::test]
    async fn reader_loop_routes_status_change_with_compact_result() {
        let ReaderTestFixture {
            event_tx,
            mut event_rx,
            init_tx,
            init_rx: _init_rx,
            outgoing_tx,
            outgoing_rx: _outgoing_rx,
            alert,
            alive,
            shutting_down,
            alert_handle: _ah,
            transcript,
            _transcript_dir,
        } = setup_reader_test();

        // Two status:null messages: one with compact_result:"success", one with "failure".
        let input = concat!(
            r#"{"type":"control_response","response":{"subtype":"success","request_id":"req_0"}}"#,
            "\n",
            r#"{"type":"system","subtype":"status","status":null,"compact_result":"success","session_id":"sess-cr"}"#,
            "\n",
            r#"{"type":"system","subtype":"status","status":null,"compact_result":"failure","session_id":"sess-cr"}"#,
            "\n",
        );

        tokio::spawn(run_stdout_reader(
            input.as_bytes(),
            event_tx,
            init_tx,
            outgoing_tx,
            transcript,
            alert,
            alive.clone(),
            shutting_down.clone(),
        ));

        // First event: compact_result == "success"
        let event = tokio::time::timeout(tokio::time::Duration::from_secs(1), event_rx.recv())
            .await
            .expect("no timeout")
            .expect("event");
        match event {
            SessionEvent::StatusChange {
                status,
                compact_result,
            } => {
                assert!(status.is_none(), "status should be None");
                assert_eq!(
                    compact_result.as_deref(),
                    Some("success"),
                    "compact_result should be 'success'"
                );
            }
            other => panic!("expected StatusChange with compact_result=success, got {other:?}"),
        }

        // Second event: compact_result == "failure"
        let event = tokio::time::timeout(tokio::time::Duration::from_secs(1), event_rx.recv())
            .await
            .expect("no timeout")
            .expect("event");
        match event {
            SessionEvent::StatusChange {
                status,
                compact_result,
            } => {
                assert!(status.is_none(), "status should be None");
                assert_eq!(
                    compact_result.as_deref(),
                    Some("failure"),
                    "compact_result should be 'failure'"
                );
            }
            other => panic!("expected StatusChange with compact_result=failure, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn reader_loop_routes_compact_boundary() {
        let ReaderTestFixture {
            event_tx,
            mut event_rx,
            init_tx,
            init_rx: _init_rx,
            outgoing_tx,
            outgoing_rx: _outgoing_rx,
            alert,
            alive,
            shutting_down,
            alert_handle: _ah,
            transcript,
            _transcript_dir,
        } = setup_reader_test();

        let input = concat!(
            r#"{"type":"control_response","response":{"subtype":"success","request_id":"req_0"}}"#,
            "\n",
            r#"{"type":"system","subtype":"compact_boundary","session_id":"sess-1","compact_metadata":{"trigger":"manual","pre_tokens":16807}}"#,
            "\n",
        );

        tokio::spawn(run_stdout_reader(
            input.as_bytes(),
            event_tx,
            init_tx,
            outgoing_tx,
            transcript,
            alert,
            alive.clone(),
            shutting_down.clone(),
        ));

        let event = tokio::time::timeout(tokio::time::Duration::from_secs(1), event_rx.recv())
            .await
            .expect("no timeout")
            .expect("event");
        match event {
            SessionEvent::CompactBoundary { metadata } => {
                let meta = metadata.expect("should have metadata");
                assert_eq!(meta.trigger.as_deref(), Some("manual"));
                assert_eq!(meta.pre_tokens, Some(16807));
            }
            other => panic!("expected CompactBoundary, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn reader_loop_routes_assistant_message() {
        let ReaderTestFixture {
            event_tx,
            mut event_rx,
            init_tx,
            init_rx: _init_rx,
            outgoing_tx,
            outgoing_rx: _outgoing_rx,
            alert,
            alive,
            shutting_down,
            alert_handle: _ah,
            transcript,
            _transcript_dir,
        } = setup_reader_test();

        let input = concat!(
            r#"{"type":"control_response","response":{"subtype":"success","request_id":"req_0"}}"#,
            "\n",
            r#"{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"hello"}]},"uuid":"msg-1"}"#,
            "\n",
        );

        tokio::spawn(run_stdout_reader(
            input.as_bytes(),
            event_tx,
            init_tx,
            outgoing_tx,
            transcript,
            alert,
            alive,
            shutting_down.clone(),
        ));

        let event = tokio::time::timeout(tokio::time::Duration::from_secs(1), event_rx.recv())
            .await
            .expect("no timeout")
            .expect("event");

        assert!(matches!(event, SessionEvent::AssistantMessage(_)));
    }

    #[tokio::test]
    async fn reader_loop_eof_emits_died() {
        let ReaderTestFixture {
            event_tx,
            mut event_rx,
            init_tx,
            init_rx: _init_rx,
            outgoing_tx,
            outgoing_rx: _outgoing_rx,
            alert,
            alive,
            shutting_down,
            alert_handle: _ah,
            transcript,
            _transcript_dir,
        } = setup_reader_test();

        // Just a control_response then EOF — no result message.
        let input = concat!(
            r#"{"type":"control_response","response":{"subtype":"success","request_id":"req_0"}}"#,
            "\n",
        );

        tokio::spawn(run_stdout_reader(
            input.as_bytes(),
            event_tx,
            init_tx,
            outgoing_tx,
            transcript,
            alert,
            alive.clone(),
            shutting_down.clone(),
        ));

        let event = tokio::time::timeout(tokio::time::Duration::from_secs(1), event_rx.recv())
            .await
            .expect("no timeout")
            .expect("event");

        assert!(matches!(event, SessionEvent::Died(_)));
        assert!(!alive.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn reader_loop_parse_failure_emits_unrecognized() {
        let ReaderTestFixture {
            event_tx,
            mut event_rx,
            init_tx,
            init_rx: _init_rx,
            outgoing_tx,
            outgoing_rx: _outgoing_rx,
            alert,
            alive,
            shutting_down,
            alert_handle: _ah,
            transcript,
            _transcript_dir,
        } = setup_reader_test();

        let input = concat!(
            r#"{"type":"control_response","response":{"subtype":"success","request_id":"req_0"}}"#,
            "\n",
            "this is not valid json\n",
            r#"{"type":"result","subtype":"success","is_error":false}"#,
            "\n",
        );

        tokio::spawn(run_stdout_reader(
            input.as_bytes(),
            event_tx,
            init_tx,
            outgoing_tx,
            transcript,
            alert,
            alive,
            shutting_down.clone(),
        ));

        // First event: UnrecognizedMessage from the bad JSON line.
        let event = tokio::time::timeout(tokio::time::Duration::from_secs(1), event_rx.recv())
            .await
            .expect("no timeout")
            .expect("event");

        match event {
            SessionEvent::UnrecognizedMessage { raw_line } => {
                assert_eq!(raw_line, "this is not valid json");
            }
            other => panic!("expected UnrecognizedMessage, got {other:?}"),
        }

        // Second event: TurnCompleted from the result message (reader continued).
        let event = tokio::time::timeout(tokio::time::Duration::from_secs(1), event_rx.recv())
            .await
            .expect("no timeout")
            .expect("event");

        assert!(matches!(event, SessionEvent::TurnCompleted(_)));
    }

    #[tokio::test]
    async fn reader_loop_unknown_control_request_kills_session() {
        let ReaderTestFixture {
            event_tx,
            mut event_rx,
            init_tx,
            init_rx: _init_rx,
            outgoing_tx,
            outgoing_rx: _outgoing_rx,
            alert,
            alive,
            shutting_down,
            alert_handle: _ah,
            transcript,
            _transcript_dir,
        } = setup_reader_test();

        // An unknown control_request subtype should kill the session because
        // we can't safely respond (don't know the expected response format).
        let input = concat!(
            r#"{"type":"control_response","response":{"subtype":"success","request_id":"req_0"}}"#,
            "\n",
            r#"{"type":"control_request","request_id":"req_99","request":{"subtype":"brand_new_thing","data":"whatever"}}"#,
            "\n",
        );

        tokio::spawn(run_stdout_reader(
            input.as_bytes(),
            event_tx,
            init_tx,
            outgoing_tx,
            transcript,
            alert,
            alive.clone(),
            shutting_down.clone(),
        ));

        let event = tokio::time::timeout(tokio::time::Duration::from_secs(1), event_rx.recv())
            .await
            .expect("no timeout")
            .expect("event");

        match event {
            SessionEvent::Died(CcError::UnknownControlRequest { raw_line }) => {
                assert!(raw_line.contains("brand_new_thing"));
            }
            other => panic!("expected Died(UnknownControlRequest), got {other:?}"),
        }
        assert!(!alive.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn reader_loop_result_emits_completed() {
        let ReaderTestFixture {
            event_tx,
            mut event_rx,
            init_tx,
            init_rx: _init_rx,
            outgoing_tx,
            outgoing_rx: _outgoing_rx,
            alert,
            alive,
            shutting_down,
            alert_handle: _ah,
            transcript,
            _transcript_dir,
        } = setup_reader_test();

        let input = concat!(
            r#"{"type":"control_response","response":{"subtype":"success","request_id":"req_0"}}"#,
            "\n",
            r#"{"type":"result","subtype":"success","is_error":false,"total_cost_usd":0.01,"num_turns":2}"#,
            "\n",
        );

        tokio::spawn(run_stdout_reader(
            input.as_bytes(),
            event_tx,
            init_tx,
            outgoing_tx,
            transcript,
            alert,
            alive.clone(),
            shutting_down.clone(),
        ));

        let event = tokio::time::timeout(tokio::time::Duration::from_secs(1), event_rx.recv())
            .await
            .expect("no timeout")
            .expect("event");

        match event {
            SessionEvent::TurnCompleted(res) => {
                assert_eq!(res.total_cost_usd, Some(0.01));
                assert_eq!(res.num_turns, Some(2));
            }
            other => panic!("expected TurnCompleted, got {other:?}"),
        }
        // alive stays true after TurnCompleted — CC is persistent.
        // The reader will hit EOF next (end of test input) and set alive=false
        // via the Died path, but we don't need to verify that here.
    }

    #[tokio::test]
    async fn reader_loop_continues_after_result() {
        // The key behavioral change: after a result message, the reader loop
        // continues processing. Feed: init ack → result → assistant → EOF.
        // We should get TurnCompleted, then AssistantMessage, then Died.
        //
        // Use a capacity-1 event channel so the reader blocks after sending
        // TurnCompleted, giving us time to assert alive==true before it
        // reaches EOF.
        let (alert, _ah) = noop_alert_dispatcher();
        let (event_tx, mut event_rx) = mpsc::channel(1);
        let (init_tx, _init_rx) = mpsc::channel(16);
        let (outgoing_tx, _outgoing_rx) = mpsc::channel(64);
        let alive = Arc::new(AtomicBool::new(true));
        let shutting_down = Arc::new(AtomicBool::new(false));
        let (transcript, _transcript_dir) = make_test_transcript();

        let input = concat!(
            r#"{"type":"control_response","response":{"subtype":"success","request_id":"req_0"}}"#,
            "\n",
            r#"{"type":"result","subtype":"success","is_error":false,"total_cost_usd":0.01}"#,
            "\n",
            r#"{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"second turn"}]},"uuid":"msg-2"}"#,
            "\n",
        );

        tokio::spawn(run_stdout_reader(
            input.as_bytes(),
            event_tx,
            init_tx,
            outgoing_tx,
            transcript,
            alert,
            alive.clone(),
            shutting_down,
        ));

        // First event: TurnCompleted from the result message.
        let event = tokio::time::timeout(tokio::time::Duration::from_secs(1), event_rx.recv())
            .await
            .expect("no timeout")
            .expect("event");
        assert!(
            matches!(event, SessionEvent::TurnCompleted(_)),
            "expected TurnCompleted, got {event:?}"
        );

        // alive should still be true after TurnCompleted.
        assert!(
            alive.load(Ordering::Relaxed),
            "alive should be true after TurnCompleted"
        );

        // Second event: AssistantMessage from the next turn's output.
        let event = tokio::time::timeout(tokio::time::Duration::from_secs(1), event_rx.recv())
            .await
            .expect("no timeout")
            .expect("event");
        assert!(
            matches!(event, SessionEvent::AssistantMessage(_)),
            "expected AssistantMessage after TurnCompleted, got {event:?}"
        );

        // Third event: Died (EOF after assistant message).
        let event = tokio::time::timeout(tokio::time::Duration::from_secs(1), event_rx.recv())
            .await
            .expect("no timeout")
            .expect("event");
        assert!(
            matches!(event, SessionEvent::Died(_)),
            "expected Died on EOF, got {event:?}"
        );
        assert!(
            !alive.load(Ordering::Relaxed),
            "alive should be false after Died"
        );
    }

    #[tokio::test]
    async fn result_clears_pending_approvals() {
        // When a result message arrives, any pending approvals should be cleared.
        // This means if we had a pending approval's cancel_tx, dropping it would
        // signal cancellation to the wait task.
        let ReaderTestFixture {
            event_tx,
            mut event_rx,
            init_tx,
            init_rx: _init_rx,
            outgoing_tx,
            mut outgoing_rx,
            alert,
            alive,
            shutting_down,
            alert_handle: _ah,
            transcript,
            _transcript_dir,
        } = setup_reader_test();

        // Feed: init ack → permission request → result.
        // The result should clear the pending approval (dropping cancel_tx),
        // which means the approval wait task gets cancelled.
        let input = concat!(
            r#"{"type":"control_response","response":{"subtype":"success","request_id":"req_0"}}"#,
            "\n",
            r#"{"type":"control_request","request_id":"approval-1","request":{"subtype":"can_use_tool","tool_name":"Bash","tool_use_id":"tu-1","input":{"command":"ls"}}}"#,
            "\n",
            r#"{"type":"result","subtype":"success","is_error":false}"#,
            "\n",
        );

        tokio::spawn(run_stdout_reader(
            input.as_bytes(),
            event_tx,
            init_tx,
            outgoing_tx,
            transcript,
            alert,
            alive,
            shutting_down.clone(),
        ));

        // First event: ApprovalRequired.
        let event = tokio::time::timeout(tokio::time::Duration::from_secs(1), event_rx.recv())
            .await
            .expect("no timeout")
            .expect("event");
        let approval_req = match event {
            SessionEvent::ApprovalRequired(req) => req,
            other => panic!("expected ApprovalRequired, got {other:?}"),
        };

        // Send a decision on the approval — but the result message should have
        // already cleared pending_approvals (dropping cancel_tx). The wait task
        // should detect this and NOT send a response to CC.
        // Send the decision after a brief yield to let the reader process the result.
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        // The approval's response_tx should still work (it's held by the wait task),
        // but the wait task's cancel_rx was dropped, so it may have already exited.
        // Either way, no CC response should be generated for a cancelled approval.
        let _ = approval_req.response_tx.send(ApprovalDecision::Allow {
            updated_input: None,
        });

        // Second event: TurnCompleted.
        let event = tokio::time::timeout(tokio::time::Duration::from_secs(1), event_rx.recv())
            .await
            .expect("no timeout")
            .expect("event");
        assert!(
            matches!(event, SessionEvent::TurnCompleted(_)),
            "expected TurnCompleted, got {event:?}"
        );

        // Drain any CC responses that might have been generated.
        // With the approval cleared, we should NOT see a response for "approval-1".
        // Allow a small window for any async tasks to complete.
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        let mut responses = vec![];
        while let Ok(msg) = outgoing_rx.try_recv() {
            responses.push(msg);
        }
        // If any response was sent for the cleared approval, it's a bug.
        // The responses should be empty (no CC response for a cancelled approval).
        assert!(
            responses.is_empty(),
            "expected no CC responses after approval cleared by result, got {} response(s)",
            responses.len()
        );
    }

    #[tokio::test]
    async fn reader_loop_eof_with_shutting_down_skips_alert() {
        use brenn_lib::obs::alerting::CountingAlerter;
        use std::sync::atomic::AtomicU32;

        let alert_count = Arc::new(AtomicU32::new(0));
        let limiter = RateLimiter::new(10, 60);
        let (alert, _ah) = AlertDispatcher::new(CountingAlerter(alert_count.clone()), limiter);

        let (event_tx, mut event_rx) = mpsc::channel(64);
        let (init_tx, _init_rx) = mpsc::channel(16);
        let (outgoing_tx, _outgoing_rx) = mpsc::channel(64);
        let alive = Arc::new(AtomicBool::new(true));
        let shutting_down = Arc::new(AtomicBool::new(true)); // Pre-set: intentional shutdown
        let (transcript, _transcript_dir) = make_test_transcript();

        // Init ack then EOF — no result message.
        let input = concat!(
            r#"{"type":"control_response","response":{"subtype":"success","request_id":"req_0"}}"#,
            "\n",
        );

        tokio::spawn(run_stdout_reader(
            input.as_bytes(),
            event_tx,
            init_tx,
            outgoing_tx,
            transcript,
            alert,
            alive.clone(),
            shutting_down,
        ));

        // Should still get Died event (needed for cleanup).
        let event = tokio::time::timeout(tokio::time::Duration::from_secs(1), event_rx.recv())
            .await
            .expect("no timeout")
            .expect("event");
        assert!(matches!(event, SessionEvent::Died(_)));
        assert!(!alive.load(Ordering::Relaxed));

        // But no alert should have been dispatched.
        // Small yield to let any async alert dispatch complete.
        tokio::task::yield_now().await;
        assert_eq!(
            alert_count.load(Ordering::SeqCst),
            0,
            "no alert should fire when shutting_down is set"
        );
    }

    // --- Task tool lifecycle subtypes (task_started, task_progress,
    //     task_notification, task_updated) — routing behavior ---

    /// Helper: run the reader loop over a single NDJSON line, return how many
    /// SessionEvents were emitted and how many alerts fired.
    ///
    /// Takes `&'static str` so the byte slice has a static lifetime for the
    /// spawned reader task. Tests pass string literals (sometimes
    /// `concat!`d), which are static.
    async fn run_reader_with(lines: &'static str) -> (Vec<SessionEvent>, u32) {
        use brenn_lib::obs::alerting::CountingAlerter;
        use std::sync::atomic::AtomicU32;

        let alert_count = Arc::new(AtomicU32::new(0));
        let limiter = RateLimiter::new(100, 60);
        let (alert, _ah) = AlertDispatcher::new(CountingAlerter(alert_count.clone()), limiter);

        let (event_tx, mut event_rx) = mpsc::channel(64);
        let (init_tx, _init_rx) = mpsc::channel(16);
        let (outgoing_tx, _outgoing_rx) = mpsc::channel(64);
        let alive = Arc::new(AtomicBool::new(true));
        let shutting_down = Arc::new(AtomicBool::new(false));

        // Init ack first so the reader exits init handshake mode, then the
        // lines under test. The reader hits EOF after the input is drained.
        // Each call builds a fresh static slice via `Box::leak` — cheap for
        // tests (a few hundred bytes each, freed at process exit).
        let init_ack =
            r#"{"type":"control_response","response":{"subtype":"success","request_id":"req_0"}}"#;
        let combined = format!("{init_ack}\n{lines}");
        let input: &'static [u8] = Box::leak(combined.into_boxed_str()).as_bytes();

        let (transcript, _transcript_dir) = make_test_transcript();

        tokio::spawn(run_stdout_reader(
            input,
            event_tx,
            init_tx,
            outgoing_tx,
            transcript,
            alert,
            alive,
            shutting_down,
        ));

        // Collect events until the channel closes (reader exits on EOF).
        let mut events = vec![];
        while let Some(ev) = event_rx.recv().await {
            events.push(ev);
        }

        (events, alert_count.load(Ordering::SeqCst))
    }

    #[tokio::test]
    async fn reader_routes_task_started_without_alert_or_event() {
        // task_started is parsed + debug-logged + dropped. Must NOT emit a
        // SessionEvent (Brenn has no turn-model slot for it) and must NOT
        // fire an alert (it was the #3 warning storm source).
        let line = concat!(
            r#"{"type":"system","subtype":"task_started","task_id":"t1","#,
            r#""description":"test subtask","task_type":"local_bash","#,
            r#""uuid":"u1","session_id":"s1"}"#,
            "\n",
        );
        let (events, alerts) = run_reader_with(line).await;

        // The only event expected is Died (from EOF after our input).
        let non_died: Vec<_> = events
            .iter()
            .filter(|e| !matches!(e, SessionEvent::Died(_)))
            .collect();
        assert!(
            non_died.is_empty(),
            "task_started should not emit a SessionEvent; got {non_died:?}"
        );
        // The Died event fires its own Critical alert (EOF, shutting_down=false).
        // Exactly one alert expected — nothing from task_started.
        assert_eq!(
            alerts, 1,
            "only the EOF Died alert should fire; got {alerts}"
        );
    }

    #[tokio::test]
    async fn reader_routes_task_progress_without_alert_or_event() {
        let line = concat!(
            r#"{"type":"system","subtype":"task_progress","task_id":"t1","#,
            r#""description":"running","usage":{"total_tokens":10,"tool_uses":1,"duration_ms":50},"#,
            r#""last_tool_name":"Bash","uuid":"u2","session_id":"s1"}"#,
            "\n",
        );
        let (events, alerts) = run_reader_with(line).await;
        let non_died: Vec<_> = events
            .iter()
            .filter(|e| !matches!(e, SessionEvent::Died(_)))
            .collect();
        assert!(non_died.is_empty(), "got {non_died:?}");
        assert_eq!(alerts, 1, "only the EOF Died alert; got {alerts}");
    }

    #[tokio::test]
    async fn reader_routes_task_notification_without_alert_or_event() {
        let line = concat!(
            r#"{"type":"system","subtype":"task_notification","task_id":"t1","#,
            r#""status":"completed","output_file":"/tmp/x","summary":"done","#,
            r#""uuid":"u3","session_id":"s1"}"#,
            "\n",
        );
        let (events, alerts) = run_reader_with(line).await;
        let non_died: Vec<_> = events
            .iter()
            .filter(|e| !matches!(e, SessionEvent::Died(_)))
            .collect();
        assert!(non_died.is_empty(), "got {non_died:?}");
        assert_eq!(alerts, 1, "only the EOF Died alert; got {alerts}");
    }

    #[tokio::test]
    async fn reader_routes_task_updated_without_alert_or_event() {
        let line = concat!(
            r#"{"type":"system","subtype":"task_updated","task_id":"t1","#,
            r#""patch":{"status":"completed","end_time":123},"uuid":"u4","session_id":"s1"}"#,
            "\n",
        );
        let (events, alerts) = run_reader_with(line).await;
        let non_died: Vec<_> = events
            .iter()
            .filter(|e| !matches!(e, SessionEvent::Died(_)))
            .collect();
        assert!(non_died.is_empty(), "got {non_died:?}");
        assert_eq!(alerts, 1, "only the EOF Died alert; got {alerts}");
    }

    #[tokio::test]
    async fn reader_unknown_subtype_alerts_but_once_per_process() {
        // Regression: a genuinely new subtype (not one we've added) still
        // alerts — but only once per process per subtype, not once per
        // message. Two identical-subtype messages yield exactly one alert.
        let line = concat!(
            r#"{"type":"system","subtype":"subtype_from_the_future","foo":"bar"}"#,
            "\n",
            r#"{"type":"system","subtype":"subtype_from_the_future","foo":"baz"}"#,
            "\n",
        );
        let (events, alerts) = run_reader_with(line).await;
        // No SessionEvent for unknown subtypes (passthrough-with-alert).
        let non_died: Vec<_> = events
            .iter()
            .filter(|e| !matches!(e, SessionEvent::Died(_)))
            .collect();
        assert!(
            non_died.is_empty(),
            "unknown subtype must not emit a SessionEvent; got {non_died:?}"
        );
        // 1 alert for the unknown subtype (first occurrence, deduped
        // thereafter) + 1 alert for the EOF Died.
        assert_eq!(
            alerts, 2,
            "exactly one unknown-subtype alert + one EOF Died alert"
        );
    }

    #[tokio::test]
    async fn reader_unknown_control_request_kills_session_and_alerts() {
        // An unknown control_request subtype kills the session (we can't
        // respond safely). Exactly one alert fires.
        let line = concat!(
            r#"{"type":"control_request","request_id":"r1","request":{"subtype":"wut"}}"#,
            "\n",
        );
        let (events, alerts) = run_reader_with(line).await;
        assert!(
            events.iter().any(|e| matches!(e, SessionEvent::Died(_))),
            "unknown control_request must kill the session"
        );
        assert_eq!(
            alerts, 1,
            "one Critical control_request alert; got {alerts}"
        );
    }

    #[tokio::test]
    async fn reader_unrecognized_message_dedupes_by_error_shape() {
        // A non-control-request parse failure doesn't kill the session; it
        // fires an alert and emits SessionEvent::UnrecognizedMessage for
        // persistence. Two messages that produce the SAME serde error
        // category (serde_json::Error::classify()) should yield exactly one
        // phone alert (dedup by category).
        //
        // Two different unknown top-level `type` values both produce
        // serde_json::error::Category::Data, so they share a dedup key.
        let line = concat!(
            r#"{"type":"unknown_type_alpha","foo":"bar"}"#,
            "\n",
            r#"{"type":"unknown_type_beta","baz":"qux"}"#,
            "\n",
        );
        let (events, alerts) = run_reader_with(line).await;
        // Both messages surface as SessionEvent::UnrecognizedMessage.
        let unrec_count = events
            .iter()
            .filter(|e| matches!(e, SessionEvent::UnrecognizedMessage { .. }))
            .count();
        assert_eq!(unrec_count, 2, "both parse failures surface as events");
        // But only ONE alert fires (dedup on the same Data category), plus
        // the Critical EOF Died alert at the end. Total = 2.
        assert_eq!(
            alerts, 2,
            "one parse-failure alert + one EOF Died alert; got {alerts}"
        );
    }

    #[tokio::test]
    async fn reader_unrecognized_different_error_shapes_alert_separately() {
        // Complement of the dedup test: two parse failures with DIFFERENT
        // serde_json error categories must each fire their own alert.
        //
        // A Syntax error (malformed JSON) and a Data error (valid JSON but
        // unknown type) produce different classify() categories, so they
        // use different dedup keys and generate separate alerts.
        let line = concat!(
            "{broken json}\n",
            r#"{"type":"unknown_type_beta","baz":"qux"}"#,
            "\n",
        );
        let (_events, alerts) = run_reader_with(line).await;
        // 2 distinct parse-failure alerts (Syntax + Data) + 1 EOF Died = 3.
        assert_eq!(
            alerts, 3,
            "two distinct parse-failure alerts + one EOF Died alert; got {alerts}"
        );
    }

    // -----------------------------------------------------------------------
    // spawn_stdin_writer ack-on-flush tests (test-8)
    //
    // These verify the D-c invariant: `spawn_stdin_writer` fires `ack.send(Ok(()))`
    // *after* `write_all + flush` succeeds, and fires `ack.send(Err(e))` when the
    // write fails (broken pipe). No test exercised this path before: the
    // `recording_for_test` auto-ack fires before enqueue; the `stalling_for_test`
    // ack is held by the test, never touching the real writer.
    // -----------------------------------------------------------------------

    /// `spawn_stdin_writer` fires `Ok(())` ack after successful flush.
    ///
    /// Wires the writer to the write-half of a `tokio::io::duplex` pipe.
    /// The read-half keeps the pipe open so writes succeed.
    /// Enqueues one envelope with `ack: Some(tx)`, awaits the ack, asserts `Ok`.
    #[tokio::test]
    async fn stdin_writer_fires_ok_ack_after_flush() {
        use tokio::io::duplex;
        use tokio::sync::oneshot;

        let (client, _server) = duplex(4096);
        let (outgoing_tx, outgoing_rx) = mpsc::channel::<OutgoingEnvelope>(16);
        let (transcript, _dir) = make_test_transcript();

        spawn_stdin_writer(client, outgoing_rx, transcript);

        let (ack_tx, ack_rx) = oneshot::channel();
        let msg = crate::protocol::builders::user_message("hello from test");
        outgoing_tx
            .send(OutgoingEnvelope {
                msg,
                ack: Some(ack_tx),
            })
            .await
            .expect("send to writer");

        let result = tokio::time::timeout(tokio::time::Duration::from_secs(2), ack_rx)
            .await
            .expect("ack must arrive within 2s")
            .expect("ack_tx must not be dropped before firing");

        assert!(
            result.is_ok(),
            "spawn_stdin_writer must fire Ok(()) ack after successful write_all+flush; \
             got {result:?}"
        );
    }

    /// `spawn_stdin_writer` fires `Err` ack when the write end is broken.
    ///
    /// Drops the read-half of the duplex pipe *before* the writer flushes.
    /// The writer task gets an I/O error on `write_all` / `flush`, fires
    /// `ack.send(Err(e))`, and exits.  Asserts the ack resolves with `Err`.
    #[tokio::test]
    async fn stdin_writer_fires_err_ack_on_broken_pipe() {
        use tokio::io::duplex;
        use tokio::sync::oneshot;

        let (client, server) = duplex(4096);
        // Drop the read end immediately: the first write the writer task
        // attempts will fail with a broken-pipe I/O error.
        drop(server);

        let (outgoing_tx, outgoing_rx) = mpsc::channel::<OutgoingEnvelope>(16);
        let (transcript, _dir) = make_test_transcript();

        spawn_stdin_writer(client, outgoing_rx, transcript);

        let (ack_tx, ack_rx) = oneshot::channel();
        let msg = crate::protocol::builders::user_message("should fail");
        outgoing_tx
            .send(OutgoingEnvelope {
                msg,
                ack: Some(ack_tx),
            })
            .await
            .expect("send to writer");

        let result = tokio::time::timeout(tokio::time::Duration::from_secs(2), ack_rx)
            .await
            .expect("ack must arrive within 2s (writer fires Err before breaking)")
            .expect("ack_tx must not be dropped before firing");

        assert!(
            result.is_err(),
            "spawn_stdin_writer must fire Err ack when write fails (broken pipe); \
             got {result:?}"
        );
    }
}
