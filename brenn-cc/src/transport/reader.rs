//! NDJSON reader for CC stdout.

use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};

use crate::error::TransportError;
use crate::protocol::CcIncoming;

/// Default maximum line size: 10 MB.
const DEFAULT_MAX_LINE_BYTES: usize = 10 * 1024 * 1024;

/// Reads NDJSON messages from an async byte stream.
///
/// Generic over `R: AsyncRead` so it works with both real subprocess stdout
/// and test mock streams.
pub struct NdjsonReader<R> {
    reader: BufReader<R>,
    line_buf: String,
    max_line_bytes: usize,
}

impl<R: AsyncRead + Unpin> NdjsonReader<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader: BufReader::new(reader),
            line_buf: String::new(),
            max_line_bytes: DEFAULT_MAX_LINE_BYTES,
        }
    }

    pub fn with_max_line_bytes(reader: R, max: usize) -> Self {
        Self {
            reader: BufReader::new(reader),
            line_buf: String::new(),
            max_line_bytes: max,
        }
    }

    /// Read the next message from the stream.
    ///
    /// Returns `Ok(Some((parsed, raw_line)))` on success, `Ok(None)` on EOF.
    ///
    /// Parse failures return `Err(TransportError::ParseError)`. The caller
    /// decides whether to skip or escalate — CC sending something we don't
    /// recognize is expected (protocol evolution), not a panic.
    pub async fn next(&mut self) -> Result<Option<(CcIncoming, String)>, TransportError> {
        loop {
            self.line_buf.clear();
            let bytes_read = self.reader.read_line(&mut self.line_buf).await?;

            if bytes_read == 0 {
                return Ok(None); // EOF
            }

            let line = self.line_buf.trim();

            // Skip blank lines.
            if line.is_empty() {
                continue;
            }

            // Safety check: reject unreasonably large lines.
            if line.len() > self.max_line_bytes {
                return Err(TransportError::LineTooLong {
                    length: line.len(),
                    max: self.max_line_bytes,
                });
            }

            let raw = line.to_string();
            let parsed = serde_json::from_str::<CcIncoming>(&raw).map_err(|error| {
                TransportError::ParseError {
                    line: raw.clone(),
                    error,
                }
            })?;

            return Ok(Some((parsed, raw)));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_reader(data: &str) -> NdjsonReader<&[u8]> {
        NdjsonReader::new(data.as_bytes())
    }

    #[tokio::test]
    async fn reads_single_message() {
        let json = r#"{"type":"result","subtype":"success","is_error":false}"#;
        let input = format!("{json}\n");
        let mut reader = make_reader(&input);
        let (msg, raw) = reader.next().await.unwrap().expect("should get message");
        assert!(matches!(msg, CcIncoming::Result(_)));
        assert_eq!(raw, json);
    }

    #[tokio::test]
    async fn reads_multiple_messages() {
        let input = concat!(
            r#"{"type":"result","subtype":"success","is_error":false}"#,
            "\n",
            r#"{"type":"control_cancel_request","request_id":"req_1"}"#,
            "\n",
        );
        let mut reader = make_reader(input);
        let (msg1, _) = reader.next().await.unwrap().unwrap();
        let (msg2, _) = reader.next().await.unwrap().unwrap();
        assert!(matches!(msg1, CcIncoming::Result(_)));
        assert!(matches!(msg2, CcIncoming::ControlCancelRequest { .. }));
        assert!(reader.next().await.unwrap().is_none()); // EOF
    }

    #[tokio::test]
    async fn skips_blank_lines() {
        let input = concat!(
            "\n",
            "\n",
            r#"{"type":"result","subtype":"success","is_error":false}"#,
            "\n",
            "\n",
        );
        let mut reader = make_reader(input);
        let (msg, _) = reader.next().await.unwrap().unwrap();
        assert!(matches!(msg, CcIncoming::Result(_)));
    }

    #[tokio::test]
    async fn eof_returns_none() {
        let mut reader = make_reader("");
        assert!(reader.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn parse_error_returns_error() {
        let input = "this is not json\n";
        let mut reader = make_reader(input);
        let err = reader.next().await.unwrap_err();
        assert!(matches!(err, TransportError::ParseError { .. }));
    }

    #[tokio::test]
    async fn oversized_line_rejected() {
        let big_line = format!(
            r#"{{"type":"result","subtype":"success","data":"{}"}}"#,
            "x".repeat(200)
        );
        let input = format!("{big_line}\n");
        let mut reader = NdjsonReader::with_max_line_bytes(input.as_bytes(), 100);
        let err = reader.next().await.unwrap_err();
        assert!(matches!(err, TransportError::LineTooLong { .. }));
    }
}
