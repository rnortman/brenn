use std::fmt;

/// Errors from the NDJSON transport layer.
#[derive(Debug)]
pub enum TransportError {
    /// I/O error reading from or writing to the stream.
    Io(std::io::Error),
    /// Failed to parse a JSON line from CC.
    ParseError {
        line: String,
        error: serde_json::Error,
    },
    /// A line exceeded the maximum allowed size.
    LineTooLong { length: usize, max: usize },
    /// Failed to serialize an outgoing message.
    Serialize(serde_json::Error),
}

impl fmt::Display for TransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "I/O error: {e}"),
            Self::ParseError { line, error } => {
                let truncated = if line.len() > 500 {
                    &line[..line.floor_char_boundary(500)]
                } else {
                    line.as_str()
                };
                write!(f, "parse error: {error} (line: {truncated})")
            }
            Self::LineTooLong { length, max } => {
                write!(f, "line too long: {length} bytes (max {max})")
            }
            Self::Serialize(e) => write!(f, "serialize error: {e}"),
        }
    }
}

impl std::error::Error for TransportError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            Self::ParseError { error, .. } => Some(error),
            Self::Serialize(e) => Some(e),
            Self::LineTooLong { .. } => None,
        }
    }
}

impl From<std::io::Error> for TransportError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

/// Errors from the CC session layer.
#[derive(Debug)]
pub enum CcError {
    /// Failed to spawn the claude subprocess.
    SpawnFailed(std::io::Error),
    /// Initialization did not complete. Carries the child's exit code when the
    /// child was reaped (the EOF case) and the tail of its stderr, which is
    /// where podman's and CC's own error text lands.
    InitFailed {
        reason: String,
        exit_status: Option<i32>,
        stderr_tail: Vec<String>,
    },
    /// Timed out waiting for initialization. The child may still be alive, so
    /// there is no exit status — only what it wrote to stderr.
    InitTimeout { stderr_tail: Vec<String> },
    /// CC process died unexpectedly.
    ProcessDied {
        exit_status: Option<std::process::ExitStatus>,
    },
    /// Transport error (I/O or parse).
    Transport(TransportError),
    /// Stdin channel closed (writer task died).
    SendFailed,
    /// CC sent a control_request with an unknown subtype. We can't respond
    /// safely (don't know what response format it expects), so we kill the session.
    UnknownControlRequest { raw_line: String },
    /// The pre-spawn removal of whatever holds this conversation's container
    /// name could not be completed. The spawn is abandoned rather than run into
    /// a name conflict. Carries podman's own error text.
    ContainerReclaimFailed(String),
}

/// How many of the tail's most recent lines a rendered error carries.
const DISPLAY_TAIL_LINES: usize = 10;

/// Byte ceiling on the rendered excerpt.
const DISPLAY_TAIL_BYTES: usize = 2048;

/// A notification-sized rendering of a captured stderr tail: the last
/// `DISPLAY_TAIL_LINES`, capped at `DISPLAY_TAIL_BYTES`, with a marker when
/// anything was dropped. Empty string for an empty tail.
///
/// Every place a tail reaches an operator *notification* goes through this. A
/// full tail is 50 KiB, which many push transports truncate or reject outright —
/// losing the alert. The complete tail belongs in the journal as a structured
/// field at the sites that hold it, which is where it is greppable anyway.
pub fn stderr_tail_excerpt(tail: &[String]) -> String {
    if tail.is_empty() {
        return String::new();
    }
    let start = tail.len().saturating_sub(DISPLAY_TAIL_LINES);
    let mut excerpt = tail[start..].join("\n");
    if excerpt.len() > DISPLAY_TAIL_BYTES {
        excerpt.truncate(excerpt.floor_char_boundary(DISPLAY_TAIL_BYTES));
        return format!("(truncated) {excerpt}");
    }
    if start > 0 {
        return format!("(last {DISPLAY_TAIL_LINES} lines) {excerpt}");
    }
    excerpt
}

/// Append a tail excerpt after an error message. Emits nothing when the tail is
/// empty.
fn write_stderr_tail(f: &mut fmt::Formatter<'_>, tail: &[String]) -> fmt::Result {
    if tail.is_empty() {
        return Ok(());
    }
    write!(f, "; stderr tail: {}", stderr_tail_excerpt(tail))
}

impl fmt::Display for CcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SpawnFailed(e) => write!(f, "failed to spawn claude: {e}"),
            Self::InitFailed {
                reason,
                exit_status,
                stderr_tail,
            } => {
                write!(f, "CC initialization failed: {reason}")?;
                if let Some(code) = exit_status {
                    write!(f, " (exit status {code})")?;
                }
                write_stderr_tail(f, stderr_tail)
            }
            Self::InitTimeout { stderr_tail } => {
                write!(f, "CC initialization timed out")?;
                write_stderr_tail(f, stderr_tail)
            }
            Self::ProcessDied { exit_status } => {
                write!(f, "CC process died unexpectedly: {exit_status:?}")
            }
            Self::Transport(e) => write!(f, "transport error: {e}"),
            Self::SendFailed => write!(f, "send failed: stdin channel closed"),
            Self::UnknownControlRequest { raw_line } => {
                let truncated = if raw_line.len() > 500 {
                    &raw_line[..raw_line.floor_char_boundary(500)]
                } else {
                    raw_line.as_str()
                };
                write!(
                    f,
                    "unknown control_request subtype (session killed): {truncated}"
                )
            }
            Self::ContainerReclaimFailed(msg) => {
                write!(f, "container name reclaim failed: {msg}")
            }
        }
    }
}

impl std::error::Error for CcError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::SpawnFailed(e) => Some(e),
            Self::Transport(e) => Some(e),
            _ => None,
        }
    }
}

impl From<TransportError> for CcError {
    fn from(e: TransportError) -> Self {
        Self::Transport(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every call site branches on emptiness to keep a bare "stderr tail:" out
    /// of logs and alert bodies, so the empty case must render as nothing at all.
    #[test]
    fn excerpt_of_an_empty_tail_is_empty() {
        assert_eq!(stderr_tail_excerpt(&[]), "");
    }

    /// A tail that fits under both caps is passed through untouched — no marker,
    /// nothing elided.
    #[test]
    fn short_tail_renders_verbatim() {
        let tail: Vec<String> = (0..DISPLAY_TAIL_LINES)
            .map(|i| format!("line {i}"))
            .collect();
        let excerpt = stderr_tail_excerpt(&tail);
        assert_eq!(excerpt, tail.join("\n"));
        assert!(!excerpt.starts_with('('), "{excerpt}");
    }

    /// Past the line cap the *newest* lines are kept, and the drop is marked so
    /// the reader knows there was more.
    #[test]
    fn long_tail_keeps_the_newest_lines_and_says_so() {
        let tail: Vec<String> = (0..DISPLAY_TAIL_LINES + 5)
            .map(|i| format!("line {i}"))
            .collect();
        let excerpt = stderr_tail_excerpt(&tail);
        assert!(
            excerpt.starts_with(&format!("(last {DISPLAY_TAIL_LINES} lines) ")),
            "{excerpt}"
        );
        assert!(excerpt.contains("line 5"), "{excerpt}");
        assert!(excerpt.contains(&format!("line {}", DISPLAY_TAIL_LINES + 4)));
        assert!(!excerpt.contains("line 4\n"), "{excerpt}");
    }

    /// The byte cap is what keeps an alert small enough for push transports to
    /// carry. Ten full-width lines exceed it, so this branch is reachable in
    /// production, not just in theory.
    #[test]
    fn oversized_tail_is_byte_capped_and_marked() {
        let tail: Vec<String> = (0..DISPLAY_TAIL_LINES).map(|_| "x".repeat(1024)).collect();
        let excerpt = stderr_tail_excerpt(&tail);
        assert!(excerpt.starts_with("(truncated) "), "{}", &excerpt[..32]);
        assert!(
            excerpt.len() <= DISPLAY_TAIL_BYTES + "(truncated) ".len(),
            "{} bytes",
            excerpt.len()
        );
    }

    /// The cap is a byte count applied to text that need not be ASCII. Cutting
    /// mid-character would panic on an alert path, during an incident.
    #[test]
    fn oversized_multibyte_tail_does_not_split_a_character() {
        let tail: Vec<String> = (0..DISPLAY_TAIL_LINES).map(|_| "é☃".repeat(300)).collect();
        let excerpt = stderr_tail_excerpt(&tail);
        assert!(excerpt.starts_with("(truncated) "), "{}", &excerpt[..32]);
        assert!(excerpt.len() <= DISPLAY_TAIL_BYTES + "(truncated) ".len());
        // Reaching here at all proves no panic; round-tripping proves the cut
        // landed on a character boundary.
        assert_eq!(
            std::str::from_utf8(excerpt.as_bytes()).unwrap(),
            excerpt.as_str()
        );
    }
}
