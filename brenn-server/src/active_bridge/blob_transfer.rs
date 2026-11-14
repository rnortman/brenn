//! Bytes across the tool socket. `save` streams one part of a message the
//! conversation was shown — an attachment, or the body — after its header
//! line. `send_files` takes the bytes of the files a `BrennSend` attaches
//! after its go-ahead line, makes a blob of each, and publishes the send.
//!
//! The server opens no path a conversation names: `brenn-tool`, in the
//! conversation's own environment, reads and writes the files. A save is
//! refused before its first byte or it completes from its hold, which keeps
//! the blob's bytes whatever happens to the message once the name is
//! admitted. A send makes no blob that no message names: every writer and
//! hold lives in the connection's task, so a refused publish, a dropped
//! connection or a failed write leaves nothing behind. The byte phase is
//! bounded by a deadline; nothing waits beyond it.

use std::sync::Arc;
use std::time::{Duration, Instant};

use brenn_common::{MAX_LOGGED_UNTRUSTED_BYTES, sanitize_untrusted_str};
use brenn_envelope::{BodyError, MAX_ATTACHMENTS_PER_MESSAGE, MessageBody, check_content_type};
use brenn_lib::mcp_tool_names::MCP_BRENN_PREFIX;
use brenn_messaging::{AdmittedPart, AttachmentName, Messenger, NamedPart};
use brenn_messaging_store::blob::{BlobError, BlobHold, BlobWriter};
use brenn_render::tools::messaging::MCP_MESSAGE_SEND_TOOL;
use brenn_tool_proto::{
    FileDecl, Response, SaveHeader, SavePart, TRANSFER_PIECE_BYTES, WireError, go_ahead_line,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};

use super::brenn_tools::{CallOrigin, admit_tool_call};
use super::tool_socket::response_line;
use super::{ActiveBridge, ToolOutcome};
use crate::messaging_intercept::{
    SendArgs, attachment_name, held_attachments, message_uuid, missing_messenger, parse_send,
    publish_send, refusal, resolve_forwarded,
};

/// The tool a `send_files` publishes through, as the catalog names it.
fn send_tool() -> &'static str {
    MCP_MESSAGE_SEND_TOOL
        .strip_prefix(MCP_BRENN_PREFIX)
        .expect("the BrennSend MCP name carries the brenn prefix")
}

/// How long the byte phase of a transfer may take, from its header line to
/// its last byte: the CLI's default `--timeout`.
pub const TRANSFER_DEADLINE: Duration = Duration::from_secs(300);

/// The length of the next piece when `remaining` bytes are left.
fn piece_len(remaining: u64) -> usize {
    usize::try_from(remaining.min(TRANSFER_PIECE_BYTES as u64)).expect("a piece fits in memory")
}

/// Why a transfer cut at its deadline ended.
fn deadline_passed(deadline: Duration) -> String {
    format!(
        "the transfer deadline of {} s passed",
        deadline.as_secs_f64()
    )
}

/// Who a transfer's log lines are about.
struct TransferLog {
    app: String,
    conversation_id: i64,
    op: &'static str,
    started: Instant,
}

impl TransferLog {
    fn new(bridge: &ActiveBridge, op: &'static str) -> TransferLog {
        TransferLog {
            app: bridge.app_slug.clone(),
            conversation_id: bridge.conversation_id,
            op,
            started: Instant::now(),
        }
    }

    /// The one line a transfer that answered logs: `ok`, or the wire kind of
    /// its refusal.
    fn done(&self, outcome: &str, bytes: u64) {
        tracing::info!(
            app = %self.app,
            conversation_id = self.conversation_id,
            op = self.op,
            outcome,
            bytes,
            elapsed = ?self.started.elapsed(),
            "tool socket transfer"
        );
    }

    /// A transfer whose bytes stopped short of `size`: the client went away,
    /// the deadline passed, or the store failed. Not a violation.
    fn ended_early(&self, bytes: u64, size: u64, detail: &str) {
        tracing::info!(
            app = %self.app,
            conversation_id = self.conversation_id,
            op = self.op,
            bytes,
            size,
            elapsed = ?self.started.elapsed(),
            detail,
            "tool socket transfer ended early"
        );
    }
}

/// Where a save's bytes come from.
enum Source {
    /// A hold on the attachment's blob.
    Blob(Arc<BlobHold>),
    /// The message's body.
    Body(MessageBody),
}

impl Source {
    fn header(&self) -> SaveHeader {
        match self {
            Source::Blob(hold) => {
                let reference = hold.reference();
                SaveHeader {
                    size: reference.size,
                    content_type: Some(reference.content_type),
                }
            }
            Source::Body(body) => SaveHeader {
                size: body.len() as u64,
                content_type: body.content_type().map(str::to_string),
            },
        }
    }

    /// Write `len` bytes from `offset` to `write`. A blob piece is read on the
    /// blocking pool, since the store's I/O is synchronous, into `buf`, which
    /// is kept for the next piece; a body's bytes are written where they are.
    /// A store I/O error is the host's fault, not the client's, and is logged
    /// at `warn`.
    async fn write_piece(
        &self,
        offset: u64,
        len: usize,
        buf: &mut Vec<u8>,
        write: &mut OwnedWriteHalf,
    ) -> Result<(), String> {
        let bytes: &[u8] = match self {
            Source::Blob(hold) => {
                let reader = hold.clone();
                let mut piece = std::mem::take(buf);
                piece.resize(len, 0);
                let (piece, read) = tokio::task::spawn_blocking(move || {
                    let read = reader.read_into(offset, &mut piece);
                    (piece, read)
                })
                .await
                .expect("a blob read task does not panic or get cancelled");
                *buf = piece;
                match read {
                    Ok(()) => {}
                    Err(BlobError::Io(e)) => {
                        tracing::warn!(
                            blob = %hold.reference().id,
                            offset,
                            error = %e,
                            "blob store read failed during a save"
                        );
                        return Err(format!("the blob store could not read: {e}"));
                    }
                    Err(e) => panic!(
                        "BUG: a read of {len} bytes at offset {offset} inside the blob's size \
                         failed: {e}"
                    ),
                }
                &buf[..]
            }
            Source::Body(body) => {
                let start = usize::try_from(offset).expect("a body offset fits in memory");
                &body.as_bytes()[start..start + len]
            }
        };
        write
            .write_all(bytes)
            .await
            .map_err(|e| format!("writing: {e}"))
    }
}

/// Serve one `save`: admit the name, write the header line, then the bytes,
/// then close. A refusal is one error line. The bridge is released once the
/// name is admitted; the transfer runs from what admission handed over.
pub(super) async fn serve_save(
    bridge: Arc<ActiveBridge>,
    message_id: &str,
    part: &SavePart,
    write: &mut OwnedWriteHalf,
    deadline: Duration,
) {
    let log = TransferLog::new(&bridge, "save");
    let source = admit_save(&bridge, message_id, part).await;
    drop(bridge);
    let source = match source {
        Ok(source) => source,
        Err(output) => {
            let refused = Response::Err(WireError::ToolError { output });
            log.done(response_kind(&refused), 0);
            finish(write, &refused).await;
            return;
        }
    };
    let header = source.header();
    let size = header.size;
    let mut sent = 0u64;
    let streamed = tokio::time::timeout(deadline, async {
        write
            .write_all(response_line(&Response::Ok(header.to_result())).as_bytes())
            .await
            .map_err(|e| format!("writing the header: {e}"))?;
        let mut buf = Vec::new();
        while sent < size {
            let len = piece_len(size - sent);
            source.write_piece(sent, len, &mut buf, write).await?;
            sent += len as u64;
        }
        write.shutdown().await.map_err(|e| format!("closing: {e}"))
    })
    .await
    .unwrap_or_else(|_elapsed| Err(deadline_passed(deadline)));
    match streamed {
        Ok(()) => log.done("ok", sent),
        Err(detail) => log.ended_early(sent, size, &detail),
    }
}

/// The source of a save's bytes, or the tool's error JSON refusing it.
async fn admit_save(
    bridge: &ActiveBridge,
    message_id: &str,
    part: &SavePart,
) -> Result<Source, serde_json::Value> {
    let Some(messenger) = bridge.messenger() else {
        return Err(outcome_json(missing_messenger()));
    };
    let uuid = message_uuid(message_id).map_err(outcome_json)?;
    let named = match part {
        SavePart::Attachment(name) => {
            NamedPart::Attachment(attachment_name(name).map_err(outcome_json)?)
        }
        SavePart::Body => NamedPart::Body,
    };
    match messenger
        .admit_conversation_name(bridge.conversation_id, &bridge.app_slug, uuid, &named)
        .await
    {
        Ok(AdmittedPart::Attachment(hold)) => Ok(Source::Blob(Arc::new(hold))),
        Ok(AdmittedPart::Body(body)) => Ok(Source::Body(body)),
        Err(refused) => Err(outcome_json(refusal(refused.reason(), refused.to_string()))),
    }
}

/// A file of a `send_files` whose blob is reserved and not yet written.
struct Incoming {
    name: AttachmentName,
    size: u64,
    writer: BlobWriter,
}

/// A `send_files` admitted up to its go-ahead: the publish's arguments, the
/// forwarded attachments' holds, and a reserved writer per file.
struct Prepared<'a> {
    messenger: &'a Arc<Messenger>,
    args: SendArgs<'a>,
    forwarded: Vec<(AttachmentName, BlobHold)>,
    files: Vec<Incoming>,
}

/// Serve one `send_files`: every refusal the server can make before a byte
/// crosses, as one error line; then the go-ahead, the files' bytes, and the
/// `BrennSend` answer. The bridge is held until the publish, as a `call`
/// holds it while it runs.
pub(super) async fn serve_send_files(
    bridge: Arc<ActiveBridge>,
    input: serde_json::Value,
    files: Vec<FileDecl>,
    read: &mut BufReader<OwnedReadHalf>,
    write: &mut OwnedWriteHalf,
    deadline: Duration,
) {
    let log = TransferLog::new(&bridge, "send_files");
    let prepared = match prepare_send(&bridge, &input, &files).await {
        Ok(prepared) => prepared,
        Err(refused) => {
            log.done(response_kind(&refused), 0);
            finish(write, &refused).await;
            return;
        }
    };
    let mut go_ahead = go_ahead_line();
    go_ahead.push('\n');
    if let Err(e) = write.write_all(go_ahead.as_bytes()).await {
        tracing::debug!(error = %e, "tool socket go-ahead not delivered; the client is gone");
        return;
    }
    let declared: u64 = prepared.files.iter().map(|f| f.size).sum();
    let mut received = 0u64;
    let Prepared {
        messenger,
        args,
        forwarded,
        files,
    } = prepared;
    let streamed = tokio::time::timeout(deadline, receive(read, files, &mut received))
        .await
        .unwrap_or_else(|_elapsed| Err(deadline_passed(deadline)));
    let sealed = match streamed {
        Ok(sealed) => sealed,
        Err(detail) => {
            log.ended_early(received, declared, &detail);
            finish(
                write,
                &Response::Err(WireError::TransferAborted {
                    detail: format!("{detail}; nothing was published"),
                }),
            )
            .await;
            return;
        }
    };
    let response = match held_attachments(forwarded.into_iter().chain(sealed).collect()) {
        Err(refused) => tool_error(refused),
        Ok(attachments) => match publish_send(&bridge, messenger, args, attachments).await {
            ToolOutcome::Ok(result) => Response::Ok(result),
            refused @ ToolOutcome::Err(_) => tool_error(refused),
        },
    };
    log.done(response_kind(&response), received);
    finish(write, &response).await;
}

/// `ok`, or the wire kind of the refusal.
fn response_kind(response: &Response) -> &'static str {
    match response {
        Response::Ok(_) => "ok",
        Response::Err(e) => e.kind(),
    }
}

/// Write the final line and close; a client that is gone is not an error.
async fn finish(write: &mut OwnedWriteHalf, response: &Response) {
    if let Err(e) = write.write_all(response_line(response).as_bytes()).await {
        tracing::debug!(error = %e, "tool socket transfer answer not delivered");
        return;
    }
    if let Err(e) = write.shutdown().await {
        tracing::debug!(error = %e, "tool socket shutdown after transfer failed");
    }
}

fn tool_error(outcome: ToolOutcome) -> Response {
    Response::Err(WireError::ToolError {
        output: outcome_json(outcome),
    })
}

/// Every check of a `send_files` before its go-ahead: admission as a
/// `BrennSend` call, its arguments and forwarded attachments, the file
/// declarations, and a reserved blob per file.
async fn prepare_send<'a>(
    bridge: &'a ActiveBridge,
    input: &'a serde_json::Value,
    files: &[FileDecl],
) -> Result<Prepared<'a>, Response> {
    admit_tool_call(bridge, CallOrigin::Socket, send_tool(), input)
        .map_err(|e| Response::Err(e.to_wire(send_tool())))?;
    let (messenger, args) = parse_send(bridge, MCP_MESSAGE_SEND_TOOL, input).map_err(tool_error)?;
    let forwarded = resolve_forwarded(bridge, messenger, MCP_MESSAGE_SEND_TOOL, input)
        .await
        .map_err(tool_error)?;
    let declared = check_declarations(&forwarded, files).map_err(tool_error)?;
    let Some(blobs) = messenger.blob_store() else {
        return Err(tool_error(refusal(
            "blobs_unavailable",
            "this brenn server has no blob store; files cannot be attached".into(),
        )));
    };
    let limits = blobs.limits();
    for (name, decl) in &declared {
        if decl.size > limits.max_blob_bytes {
            return Err(tool_error(refusal(
                "blob_too_large",
                format!(
                    "file {name} is {} bytes; a blob holds at most {}",
                    decl.size, limits.max_blob_bytes
                ),
            )));
        }
    }
    let total = declared
        .iter()
        .try_fold(0u64, |sum, (_, decl)| sum.checked_add(decl.size))
        .unwrap_or(u64::MAX);
    if total > limits.max_blob_bytes {
        return Err(tool_error(refusal(
            "send_files_too_large",
            format!(
                "the files of one send total {total} bytes; one send attaches at most {}",
                limits.max_blob_bytes
            ),
        )));
    }
    let mut incoming = Vec::with_capacity(declared.len());
    for (name, decl) in declared {
        let reserved = blobs
            .create(&decl.content_type)
            .and_then(|mut writer| writer.reserve(decl.size).map(|()| writer));
        match reserved {
            Ok(writer) => incoming.push(Incoming {
                name,
                size: decl.size,
                writer,
            }),
            Err(BlobError::BudgetExhausted { budget }) => {
                return Err(tool_error(refusal(
                    "blob_budget_exhausted",
                    format!(
                        "the blob store's budget of {budget} bytes cannot take file {name} \
                         now; nothing waits for room, so retry later or send less"
                    ),
                )));
            }
            Err(BlobError::Io(e)) => {
                tracing::warn!(error = %e, "blob store could not start a conversation's file");
                return Err(Response::Err(WireError::TransferAborted {
                    detail: format!("the blob store could not take file {name}: {e}"),
                }));
            }
            Err(e) => panic!(
                "BUG: a checked declaration of file {name} ({} bytes, {}) was refused: {e}",
                decl.size, decl.content_type
            ),
        }
    }
    Ok(Prepared {
        messenger,
        args,
        forwarded,
        files: incoming,
    })
}

/// The declared files, each with its checked name, or the first refusal:
/// none declared, more attachments than a message carries, a malformed name
/// or content type, or a name given twice.
fn check_declarations<'f>(
    forwarded: &[(AttachmentName, BlobHold)],
    files: &'f [FileDecl],
) -> Result<Vec<(AttachmentName, &'f FileDecl)>, ToolOutcome> {
    if files.is_empty() {
        return Err(refusal(
            "no_files",
            "send_files declares no file; a send without files is `brenn-tool call BrennSend`"
                .into(),
        ));
    }
    let count = forwarded.len() + files.len();
    if count > MAX_ATTACHMENTS_PER_MESSAGE {
        return Err(refusal(
            "too_many_attachments",
            format!(
                "{count} attachments, forwarded and files together; a message carries at \
                 most {MAX_ATTACHMENTS_PER_MESSAGE}"
            ),
        ));
    }
    let mut declared: Vec<(AttachmentName, &FileDecl)> = Vec::with_capacity(files.len());
    for decl in files {
        let name = attachment_name(&decl.name)?;
        // `reason` and not the error itself: its `Display` repeats the whole
        // input, which the sanitizer caps.
        match check_content_type(&decl.content_type) {
            Ok(()) => {}
            Err(BodyError::InvalidContentType { reason, .. }) => {
                return Err(refusal(
                    "malformed_content_type",
                    format!(
                        "file {name}: content type {:?}: {reason}",
                        sanitize_untrusted_str(&decl.content_type, MAX_LOGGED_UNTRUSTED_BYTES)
                    ),
                ));
            }
            Err(e @ BodyError::NotUtf8 { .. }) => {
                unreachable!("check_content_type reads no bytes, yet refused them: {e}")
            }
        }
        let taken = forwarded
            .iter()
            .map(|(n, _)| n)
            .chain(declared.iter().map(|(n, _)| n));
        if taken.into_iter().any(|n| *n == name) {
            return Err(refusal(
                "duplicate_attachment_name",
                format!("attachment name {name} given twice"),
            ));
        }
        declared.push((name, decl));
    }
    Ok(declared)
}

/// Read each file's declared bytes in order, written at their offsets on the
/// blocking pool, and seal each blob. `Err` names why the bytes stopped;
/// every writer drops with it, deleting its file and returning its charge.
async fn receive(
    read: &mut BufReader<OwnedReadHalf>,
    files: Vec<Incoming>,
    received: &mut u64,
) -> Result<Vec<(AttachmentName, BlobHold)>, String> {
    let mut sealed = Vec::with_capacity(files.len());
    let mut buf = vec![0u8; TRANSFER_PIECE_BYTES];
    for Incoming {
        name,
        size,
        mut writer,
    } in files
    {
        let mut offset = 0u64;
        while offset < size {
            let len = piece_len(size - offset);
            read.read_exact(&mut buf[..len]).await.map_err(|e| {
                format!("file {name}: the connection ended at byte {offset} of {size}: {e}")
            })?;
            let (back, back_buf, written) = tokio::task::spawn_blocking(move || {
                let written = writer.write(offset, &buf[..len]);
                (writer, buf, written)
            })
            .await
            .expect("a blob write task does not panic or get cancelled");
            writer = back;
            buf = back_buf;
            match written {
                Ok(()) => {}
                Err(BlobError::Io(e)) => {
                    tracing::warn!(error = %e, "blob store write failed during a send_files");
                    return Err(format!("file {name}: the blob store could not write: {e}"));
                }
                Err(e) => panic!("BUG: a write inside file {name}'s reserved extent failed: {e}"),
            }
            offset += len as u64;
            *received += len as u64;
        }
        let hold = writer.seal().unwrap_or_else(|refused| {
            panic!(
                "BUG: file {name} sealed with a gap at {:?} after every declared byte",
                refused.gap
            )
        });
        sealed.push((name, hold));
    }
    Ok(sealed)
}

/// The JSON of a tool outcome that is an error.
fn outcome_json(outcome: ToolOutcome) -> serde_json::Value {
    match outcome {
        ToolOutcome::Err(output) => output,
        ToolOutcome::Ok(output) => unreachable!("a refusal is an error outcome, got {output}"),
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod send_tests;

#[cfg(test)]
mod cli_tests;
