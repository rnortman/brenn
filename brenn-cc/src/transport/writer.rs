//! NDJSON writer for CC stdin.

use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::error::TransportError;
use crate::protocol::CcOutgoing;

/// Writes NDJSON messages to an async byte stream.
///
/// Generic over `W: AsyncWrite` so it works with both real subprocess stdin
/// and test mock streams.
pub struct NdjsonWriter<W> {
    writer: W,
}

impl<W: AsyncWrite + Unpin> NdjsonWriter<W> {
    pub fn new(writer: W) -> Self {
        Self { writer }
    }

    /// Serialize a message to JSON, write it followed by a newline, and flush.
    ///
    /// Returns the raw JSON line (without the trailing newline) for transcript
    /// logging.
    pub async fn send(&mut self, msg: &CcOutgoing) -> Result<String, TransportError> {
        let json = serde_json::to_string(msg).map_err(TransportError::Serialize)?;
        let line = format!("{json}\n");
        self.writer
            .write_all(line.as_bytes())
            .await
            .map_err(TransportError::Io)?;
        self.writer.flush().await.map_err(TransportError::Io)?;
        Ok(json)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::builders;

    #[tokio::test]
    async fn writes_message_with_newline() {
        let mut buf = Vec::new();
        {
            let mut writer = NdjsonWriter::new(&mut buf);
            let msg = builders::user_message("hello");
            let raw = writer.send(&msg).await.expect("write");
            assert!(!raw.contains('\n'), "raw should not contain newline");
            assert!(raw.contains("hello"));
        }
        let written = String::from_utf8(buf).expect("utf8");
        assert!(written.ends_with('\n'));
        // Parse back to verify it's valid JSON.
        let parsed: serde_json::Value = serde_json::from_str(written.trim()).expect("valid json");
        assert_eq!(parsed["type"], "user");
    }

    #[tokio::test]
    async fn writes_multiple_messages() {
        let mut buf = Vec::new();
        {
            let mut writer = NdjsonWriter::new(&mut buf);
            writer
                .send(&builders::user_message("first"))
                .await
                .expect("write 1");
            writer
                .send(&builders::user_message("second"))
                .await
                .expect("write 2");
        }
        let written = String::from_utf8(buf).expect("utf8");
        let lines: Vec<&str> = written.lines().collect();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].contains("first"));
        assert!(lines[1].contains("second"));
    }
}
