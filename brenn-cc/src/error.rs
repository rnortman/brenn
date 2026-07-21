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
    /// CC process exited during initialization.
    InitFailed(String),
    /// Timed out waiting for initialization.
    InitTimeout,
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
}

impl fmt::Display for CcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SpawnFailed(e) => write!(f, "failed to spawn claude: {e}"),
            Self::InitFailed(msg) => write!(f, "CC initialization failed: {msg}"),
            Self::InitTimeout => write!(f, "CC initialization timed out"),
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
