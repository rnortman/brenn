//! Building a draft's RFC 5322 message in pieces a caller writes front to
//! back: the header block from values the argument parser already holds to
//! printable ASCII or free of control characters, the body and every file as
//! base64 lines, the `multipart/mixed` frame around the text and its files,
//! and the `multipart/related` frame a reply's upload wraps the message in.
//! No value here can hold a line break, so the builder never strips one.
//!
//! The boundaries are constants. Every line the builder writes starts with a
//! header name, a space (a folded header), a base64 character, `{` (the
//! upload's JSON metadata), or is a delimiter line; base64's alphabet has no
//! `-`, so no line but a delimiter starts with `--`, and neither boundary is
//! a prefix of the other.

/// The longest header line the builder writes where a space allows a fold:
/// RFC 2047's 76 for a line holding an encoded-word, which is also under RFC
/// 5322's SHOULD of 78. Every line is under RFC 5322's 998-byte MUST by the
/// parser's bounds on header values.
const LINE_MAX: usize = 76;

/// The most UTF-8 bytes one RFC 2047 encoded-word carries: 45 bytes are 60
/// base64 characters, and with `=?utf-8?B?` and `?=` a word is 72, under
/// RFC 2047's 75.
const WORD_MAX_BYTES: usize = 45;
/// An encoded-word's characters other than its base64.
const WORD_OVERHEAD: usize = "=?utf-8?B??=".len();

/// The standard base64 alphabet.
const STANDARD: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// The input bytes one full base64 line carries: 57 bytes are RFC 2045's 76
/// characters.
pub const LINE_INPUT_BYTES: usize = 57;
/// A full base64 line as written: 76 characters and CRLF.
const LINE_BYTES: usize = 78;

/// The boundary of a message that carries files.
const MIXED_BOUNDARY: &str = "brenn-draft-mixed";
/// The boundary of a reply's upload, between its metadata and its message.
const RELATED_BOUNDARY: &str = "brenn-draft-related";

/// The content type of a new draft's upload: the message itself.
pub const MESSAGE_TYPE: &str = "message/rfc822";
/// The content type of a reply's upload: its metadata, then its message.
pub const RELATED_TYPE: &str = "multipart/related; boundary=brenn-draft-related";

/// A file part's content type when the file's own is `message/*` or
/// `multipart/*`, which RFC 2045 section 6.4 forbids base64-encoding.
const OPAQUE_TYPE: &str = "application/octet-stream";

/// The header values of a draft's message, every one already held to the
/// parser's rules: `to`, `cc`, `in_reply_to` and `references` printable ASCII
/// of at most 980 bytes, `subject` free of control characters.
pub struct Head<'a> {
    pub to: &'a str,
    pub cc: Option<&'a str>,
    pub subject: &'a str,
    pub in_reply_to: Option<&'a str>,
    pub references: Option<&'a str>,
}

/// A body with every line break as CRLF: the bytes the message encodes, so
/// a budget on its length measures what is sent.
pub struct Body(String);

impl Body {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// The message's head, CRLF-terminated lines, through the empty line before
/// the body's base64: the addressing, then a `text/plain` single part, or,
/// with `files`, the `multipart/mixed` opening and the text part's headers.
/// No `From` or `Date`: Gmail writes the account's `From` on a draft and the
/// `Date` when it is sent.
pub fn head(h: &Head, files: bool) -> String {
    let mut out = fold_ascii("To", h.to);
    if let Some(cc) = h.cc {
        out.push_str(&fold_ascii("Cc", cc));
    }
    out.push_str(&header("Subject", h.subject));
    if let Some(in_reply_to) = h.in_reply_to {
        out.push_str(&fold_ascii("In-Reply-To", in_reply_to));
    }
    if let Some(references) = h.references {
        out.push_str(&fold_ascii("References", references));
    }
    out.push_str("MIME-Version: 1.0\r\n");
    if files {
        out.push_str(&format!(
            "Content-Type: multipart/mixed; boundary={MIXED_BOUNDARY}\r\n\r\n--{MIXED_BOUNDARY}\r\n"
        ));
    }
    out.push_str("Content-Type: text/plain; charset=utf-8\r\n");
    out.push_str("Content-Transfer-Encoding: base64\r\n");
    out.push_str("\r\n");
    out
}

/// A file part's delimiter and headers, through the empty line before its
/// base64: `name` is 1 to 64 of `[A-Za-z0-9._-]`, which a quoted string holds
/// unescaped, and `content_type` 1 to 255 printable ASCII characters, written
/// unfolded and verbatim unless its top-level type is `message` or
/// `multipart`.
pub fn file_head(name: &str, content_type: &str) -> String {
    let top = content_type.split('/').next().unwrap_or_default();
    let content_type =
        if top.eq_ignore_ascii_case("message") || top.eq_ignore_ascii_case("multipart") {
            OPAQUE_TYPE
        } else {
            content_type
        };
    format!(
        "--{MIXED_BOUNDARY}\r\n\
         Content-Type: {content_type}\r\n\
         Content-Disposition: attachment; filename=\"{name}\"\r\n\
         Content-Transfer-Encoding: base64\r\n\
         \r\n"
    )
}

/// The `multipart/mixed` closing delimiter, after the last file.
pub const MIXED_CLOSE: &str = "--brenn-draft-mixed--\r\n";

/// A reply's upload through the empty line before its message: the
/// metadata part naming `thread_id`, then the message part's headers.
pub fn related_open(thread_id: &str) -> String {
    let metadata = serde_json::json!({ "message": { "threadId": thread_id } });
    format!(
        "--{RELATED_BOUNDARY}\r\n\
         Content-Type: application/json; charset=UTF-8\r\n\
         \r\n\
         {metadata}\r\n\
         --{RELATED_BOUNDARY}\r\n\
         Content-Type: {MESSAGE_TYPE}\r\n\
         \r\n"
    )
}

/// A reply's upload after its message: the CRLF the closing delimiter
/// begins with, then the delimiter.
pub const RELATED_CLOSE: &str = "\r\n--brenn-draft-related--\r\n";

/// `body` with every line break as CRLF: a lone LF or a lone CR becomes CRLF,
/// a CRLF stays.
pub fn canonical(body: &str) -> Body {
    let mut out = String::with_capacity(body.len());
    let mut chars = body.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\r' => {
                out.push_str("\r\n");
                chars.next_if_eq(&'\n');
            }
            '\n' => out.push_str("\r\n"),
            c => out.push(c),
        }
    }
    Body(out)
}

/// Standard padded base64 in lines of 76 characters, each ending CRLF, of
/// input fed in slices of any size: every line but the last is full, and no
/// input writes no line. Between calls it holds under one line's input.
pub struct Lines {
    held: [u8; LINE_INPUT_BYTES],
    len: usize,
}

impl Default for Lines {
    fn default() -> Lines {
        Lines {
            held: [0; LINE_INPUT_BYTES],
            len: 0,
        }
    }
}

impl Lines {
    /// Append to `out` every full line `input` completes.
    pub fn push(&mut self, mut input: &[u8], out: &mut Vec<u8>) {
        if self.len > 0 {
            let take = (LINE_INPUT_BYTES - self.len).min(input.len());
            self.held[self.len..self.len + take].copy_from_slice(&input[..take]);
            self.len += take;
            input = &input[take..];
            if self.len < LINE_INPUT_BYTES {
                return;
            }
            out.extend_from_slice(&line(&self.held));
            self.len = 0;
        }
        let mut lines = input.chunks_exact(LINE_INPUT_BYTES);
        out.reserve(lines.len() * LINE_BYTES);
        for full in &mut lines {
            out.extend_from_slice(&line(full));
        }
        let rest = lines.remainder();
        self.held[..rest.len()].copy_from_slice(rest);
        self.len = rest.len();
    }

    /// Append the last line, padded, if any input is held.
    pub fn finish(self, out: &mut Vec<u8>) {
        if self.len == 0 {
            return;
        }
        let held = &self.held[..self.len];
        let mut groups = held.chunks_exact(3);
        for group in &mut groups {
            out.extend_from_slice(&quad(group[0], group[1], group[2]));
        }
        match *groups.remainder() {
            [a] => {
                let [c0, c1, ..] = quad(a, 0, 0);
                out.extend_from_slice(&[c0, c1, b'=', b'=']);
            }
            [a, b] => {
                let [c0, c1, c2, _] = quad(a, b, 0);
                out.extend_from_slice(&[c0, c1, c2, b'=']);
            }
            _ => {}
        }
        out.extend_from_slice(b"\r\n");
    }
}

/// The bytes [`Lines`] writes for `len` bytes of input.
pub fn encoded_len(len: usize) -> usize {
    let rest = len % LINE_INPUT_BYTES;
    len / LINE_INPUT_BYTES * LINE_BYTES
        + if rest == 0 {
            0
        } else {
            rest.div_ceil(3) * 4 + 2
        }
}

/// One full line: 57 bytes as 76 characters and CRLF.
fn line(input: &[u8]) -> [u8; LINE_BYTES] {
    let mut out = [0; LINE_BYTES];
    for (group, chars) in input.chunks_exact(3).zip(out.chunks_exact_mut(4)) {
        chars.copy_from_slice(&quad(group[0], group[1], group[2]));
    }
    out[LINE_BYTES - 2..].copy_from_slice(b"\r\n");
    out
}

/// Three bytes as four base64 characters.
fn quad(a: u8, b: u8, c: u8) -> [u8; 4] {
    let n = (u32::from(a) << 16) | (u32::from(b) << 8) | u32::from(c);
    [
        STANDARD[(n >> 18) as usize & 0x3f],
        STANDARD[(n >> 12) as usize & 0x3f],
        STANDARD[(n >> 6) as usize & 0x3f],
        STANDARD[n as usize & 0x3f],
    ]
}

/// Standard padded base64 of `bytes`, on one line: an encoded-word's text.
fn base64(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for group in bytes.chunks(3) {
        let chars = quad(
            group[0],
            group.get(1).copied().unwrap_or(0),
            group.get(2).copied().unwrap_or(0),
        );
        for (i, &c) in chars.iter().enumerate() {
            out.push(if i <= group.len() { char::from(c) } else { '=' });
        }
    }
    out
}

/// The header `name: value`, CRLF-terminated: written as is when `value` is
/// printable ASCII and the line fits `LINE_MAX`, else as RFC 2047
/// encoded-words, one to a line. An unstructured header only: RFC 2047 admits
/// encoding any of its text, ASCII included.
fn header(name: &str, value: &str) -> String {
    let line = format!("{name}: {value}");
    if line.len() <= LINE_MAX && value.bytes().all(|b| (0x20..=0x7e).contains(&b)) {
        return line + "\r\n";
    }
    format!("{name}: {}\r\n", encoded_words(name, value).join("\r\n "))
}

/// `text` as `=?utf-8?B?…?=` encoded-words of at most `WORD_MAX_BYTES` UTF-8
/// bytes each, cut on character boundaries; the first word is shorter when
/// `name: ` leaves its line less room, so every line is at most `LINE_MAX`.
fn encoded_words(name: &str, text: &str) -> Vec<String> {
    let first_room = (LINE_MAX - name.len() - 2 - WORD_OVERHEAD) / 4 * 3;
    let mut words = Vec::new();
    let mut start = 0;
    let mut room = first_room.min(WORD_MAX_BYTES);
    let mut end = 0;
    for (i, c) in text.char_indices() {
        if i + c.len_utf8() - start > room {
            words.push(word(&text[start..end]));
            start = end;
            room = WORD_MAX_BYTES;
        }
        end = i + c.len_utf8();
    }
    words.push(word(&text[start..end]));
    words
}

/// One encoded-word carrying `text`.
fn word(text: &str) -> String {
    format!("=?utf-8?B?{}?=", base64(text.as_bytes()))
}

/// The header `name: value`, CRLF-terminated, folded before a space wherever
/// the line would pass `LINE_MAX`. A run without spaces is never broken, and
/// no fold leaves a line of whitespace alone; `value` is printable ASCII, so
/// each space is folding whitespace, inside a quoted display name too.
fn fold_ascii(name: &str, value: &str) -> String {
    let mut out = format!("{name}:");
    let mut line = out.len();
    for (i, piece) in value.split(' ').enumerate() {
        if i > 0 && !piece.is_empty() && line + 1 + piece.len() > LINE_MAX {
            out.push_str("\r\n");
            line = 0;
        }
        out.push(' ');
        out.push_str(piece);
        line += 1 + piece.len();
    }
    out + "\r\n"
}

/// The text an unfolded header value of encoded-words carries, decoded:
/// every space-separated word one `utf-8` `B` encoded-word of whole
/// characters.
#[cfg(test)]
pub(super) fn decoded_words(value: &str) -> String {
    let mut text = Vec::new();
    for word in value.split(' ') {
        let encoded = word
            .strip_prefix("=?utf-8?B?")
            .and_then(|rest| rest.strip_suffix("?="))
            .unwrap_or_else(|| panic!("{word:?} is not one encoded-word"));
        let bytes = super::decode::base64url(encoded).unwrap();
        text.push(String::from_utf8(bytes).expect("a word is cut on a character boundary"));
    }
    text.concat()
}

/// `bytes` through one [`Lines`], fed in one slice.
#[cfg(test)]
pub(super) fn lines_of(bytes: &[u8]) -> String {
    let mut out = Vec::new();
    let mut lines = Lines::default();
    lines.push(bytes, &mut out);
    lines.finish(&mut out);
    String::from_utf8(out).expect("base64 is ASCII")
}

#[cfg(test)]
mod tests {
    use super::super::decode;
    use super::*;

    const VECTORS: [(&str, &str); 7] = [
        ("", ""),
        ("f", "Zg=="),
        ("fo", "Zm8="),
        ("foo", "Zm9v"),
        ("foob", "Zm9vYg=="),
        ("fooba", "Zm9vYmE="),
        ("foobar", "Zm9vYmFy"),
    ];

    #[test]
    fn base64_matches_rfc_4648s_vectors() {
        for (bytes, padded) in VECTORS {
            assert_eq!(base64(bytes.as_bytes()), padded);
            let line = if padded.is_empty() {
                String::new()
            } else {
                format!("{padded}\r\n")
            };
            assert_eq!(lines_of(bytes.as_bytes()), line);
        }
        assert_eq!(base64(&[0xfb, 0xff]), "+/8=");
        assert_eq!(lines_of(&[0xfb, 0xff]), "+/8=\r\n");
        let every: Vec<u8> = (0..=255).collect();
        assert_eq!(decode::base64url(&base64(&every)).unwrap(), every);
    }

    #[test]
    fn base64_lines_are_76_characters_ending_crlf() {
        let bytes: Vec<u8> = (0..200).collect();
        let text = lines_of(&bytes);
        let lines: Vec<&str> = text.split_terminator("\r\n").collect();
        assert_eq!(lines.len(), 4, "268 characters");
        assert!(lines[..3].iter().all(|line| line.len() == 76));
        assert_eq!(lines[3].len(), 268 - 3 * 76);
        assert!(text.ends_with("\r\n") && !text.contains("\n\n"));
        assert_eq!(decode::base64url(&text).unwrap(), bytes);
    }

    /// The same bytes fed in one slice and in slices of every size from 1 to
    /// 58 give the same lines, for lengths around each multiple of 3 and of
    /// 57.
    #[test]
    fn lines_do_not_depend_on_how_the_input_is_sliced() {
        let lengths = (0..=10)
            .chain([56, 57, 58, 113, 114, 115, 170, 171, 172])
            .chain([57 * 20 - 1, 57 * 20, 57 * 20 + 1]);
        for len in lengths {
            let bytes: Vec<u8> = (0..len).map(|i| (i * 31 % 251) as u8).collect();
            let whole = lines_of(&bytes);
            assert_eq!(decode::base64url(&whole).unwrap(), bytes, "{len} bytes");
            assert_eq!(whole.len(), encoded_len(len), "{len} bytes");
            let lines: Vec<&str> = whole.split_terminator("\r\n").collect();
            for line in lines.iter().rev().skip(1) {
                assert_eq!(line.len(), 76, "{len} bytes: a line but the last is full");
            }
            for size in 1..=58 {
                let mut out = Vec::new();
                let mut lines = Lines::default();
                for slice in bytes.chunks(size) {
                    lines.push(slice, &mut out);
                }
                lines.finish(&mut out);
                assert_eq!(
                    String::from_utf8(out).unwrap(),
                    whole,
                    "{len} bytes in slices of {size}"
                );
            }
        }
    }

    #[test]
    fn canonical_writes_every_line_break_as_crlf() {
        assert_eq!(canonical("a\nb\rc\r\nd").as_str(), "a\r\nb\r\nc\r\nd");
        assert_eq!(canonical("\r\r\n\n\n\r").as_str(), "\r\n\r\n\r\n\r\n\r\n");
        assert_eq!(canonical("é\u{2028}").as_str(), "é\u{2028}");
    }

    #[test]
    fn a_subject_is_plain_when_it_fits_and_encoded_words_otherwise() {
        assert_eq!(
            header("Subject", "Lunch at noon?"),
            "Subject: Lunch at noon?\r\n"
        );
        let fits = "x".repeat(LINE_MAX - "Subject: ".len());
        assert_eq!(header("Subject", &fits), format!("Subject: {fits}\r\n"));

        for text in [
            "Grüße aus Köln".to_string(),
            format!("{fits}x"),
            "€".repeat(500),
            "a🎵".repeat(200),
        ] {
            let line = header("Subject", &text);
            assert!(line.starts_with("Subject: =?utf-8?B?"), "{line}");
            for physical in line.trim_end_matches("\r\n").split("\r\n") {
                assert!(physical.len() <= LINE_MAX, "{physical:?}");
                let word = physical.trim_start_matches("Subject: ").trim_start();
                assert!(word.len() <= 75, "{word:?}");
            }
            let value = line.strip_prefix("Subject: ").unwrap();
            assert_eq!(
                decoded_words(&value.trim_end_matches("\r\n").replace("\r\n ", " ")),
                text
            );
        }
    }

    #[test]
    fn a_recipient_header_folds_at_spaces_only() {
        assert_eq!(
            fold_ascii("To", "alice@example.com"),
            "To: alice@example.com\r\n"
        );
        let many = ["Alice Example <alice@example.com>"; 6].join(", ");
        let folded = fold_ascii("To", &many);
        assert_eq!(folded.replace("\r\n", ""), format!("To: {many}"));
        let lines: Vec<&str> = folded.split_terminator("\r\n").collect();
        assert!(lines.len() > 1);
        for line in &lines {
            assert!(line.len() <= LINE_MAX, "{line:?}");
            assert!(!line.trim().is_empty(), "{line:?}");
        }
        assert!(lines[1..].iter().all(|line| line.starts_with(' ')));

        // A space-free run is left whole, on one line under 998.
        let run = "x".repeat(980);
        assert_eq!(fold_ascii("To", &run), format!("To: {run}\r\n"));
        let folded = fold_ascii("Cc", &format!("a {run} b"));
        assert_eq!(folded, format!("Cc: a\r\n {run}\r\n b\r\n"));

        // Spaces in a run are kept, and no fold leaves whitespace alone.
        let spaced = format!("{}   {}  ", "a".repeat(70), "b".repeat(10));
        let folded = fold_ascii("To", &spaced);
        assert_eq!(folded.replace("\r\n", ""), format!("To: {spaced}"));
        for line in folded.split_terminator("\r\n") {
            assert!(!line.trim().is_empty(), "{folded:?}");
        }
    }

    #[test]
    fn a_single_part_head_is_its_headers_and_an_empty_line() {
        let full = head(
            &Head {
                to: "Alice Example <alice@example.com>",
                cc: Some("bob@example.com"),
                subject: "Lunch",
                in_reply_to: Some("<m1@mail.example.com>"),
                references: Some("<m0@mail.example.com> <m1@mail.example.com>"),
            },
            false,
        );
        assert_eq!(
            full,
            "To: Alice Example <alice@example.com>\r\n\
             Cc: bob@example.com\r\n\
             Subject: Lunch\r\n\
             In-Reply-To: <m1@mail.example.com>\r\n\
             References: <m0@mail.example.com> <m1@mail.example.com>\r\n\
             MIME-Version: 1.0\r\n\
             Content-Type: text/plain; charset=utf-8\r\n\
             Content-Transfer-Encoding: base64\r\n\
             \r\n"
        );
        assert_eq!(
            lines_of(canonical("Noon?\nSee you.").as_str().as_bytes()),
            "Tm9vbj8NClNlZSB5b3Uu\r\n"
        );

        let bare = head(
            &Head {
                to: "alice@example.com",
                cc: None,
                subject: "Lunch",
                in_reply_to: None,
                references: None,
            },
            false,
        );
        assert!(bare.starts_with("To: alice@example.com\r\nSubject: Lunch\r\nMIME-Version"));
    }

    #[test]
    fn a_head_with_files_opens_multipart_mixed_with_the_text_part() {
        let mixed = head(
            &Head {
                to: "alice@example.com",
                cc: None,
                subject: "Lunch",
                in_reply_to: None,
                references: None,
            },
            true,
        );
        assert_eq!(
            mixed,
            "To: alice@example.com\r\n\
             Subject: Lunch\r\n\
             MIME-Version: 1.0\r\n\
             Content-Type: multipart/mixed; boundary=brenn-draft-mixed\r\n\
             \r\n\
             --brenn-draft-mixed\r\n\
             Content-Type: text/plain; charset=utf-8\r\n\
             Content-Transfer-Encoding: base64\r\n\
             \r\n"
        );
    }

    #[test]
    fn a_file_part_carries_its_name_and_type_unless_mime_forbids_base64() {
        let part = |content_type: &str| {
            format!(
                "--brenn-draft-mixed\r\n\
                 Content-Type: {content_type}\r\n\
                 Content-Disposition: attachment; filename=\"report.pdf\"\r\n\
                 Content-Transfer-Encoding: base64\r\n\
                 \r\n"
            )
        };
        assert_eq!(
            file_head("report.pdf", "application/pdf"),
            part("application/pdf")
        );
        assert_eq!(
            file_head("report.pdf", "text/plain; charset=utf-8; format=flowed"),
            part("text/plain; charset=utf-8; format=flowed")
        );
        for forbidden in ["message/rfc822", "Multipart/Mixed; boundary=x", "MESSAGE/x"] {
            assert_eq!(
                file_head("report.pdf", forbidden),
                part("application/octet-stream"),
                "{forbidden}"
            );
        }
        // Only the top-level type is compared.
        assert_eq!(
            file_head("report.pdf", "application/message"),
            part("application/message")
        );
    }

    #[test]
    fn a_replys_upload_frames_its_metadata_and_message() {
        assert_eq!(
            related_open("t-1_x"),
            "--brenn-draft-related\r\n\
             Content-Type: application/json; charset=UTF-8\r\n\
             \r\n\
             {\"message\":{\"threadId\":\"t-1_x\"}}\r\n\
             --brenn-draft-related\r\n\
             Content-Type: message/rfc822\r\n\
             \r\n"
        );
        assert_eq!(RELATED_CLOSE, "\r\n--brenn-draft-related--\r\n");
        assert_eq!(MIXED_CLOSE, format!("--{MIXED_BOUNDARY}--\r\n"));
        assert_eq!(
            RELATED_TYPE,
            format!("multipart/related; boundary={RELATED_BOUNDARY}")
        );
        assert!(!RELATED_BOUNDARY.starts_with(MIXED_BOUNDARY));
        assert!(!MIXED_BOUNDARY.starts_with(RELATED_BOUNDARY));
    }
}
