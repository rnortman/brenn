//! Building a draft's RFC 5322 message: the header block from values the
//! argument parser already holds to printable ASCII or free of control
//! characters, the body as base64 of its CRLF form, and the whole as the
//! base64url Gmail's `raw` takes. No value here can hold a line break, so the
//! builder never strips one.

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
/// The URL- and filename-safe base64 alphabet.
const URL_SAFE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

/// The characters a base64 line of a body holds: RFC 2045's limit.
const BASE64_LINE_CHARS: usize = 76;

/// What a draft's message is built from, every header value already held to
/// the parser's rules: `to`, `cc`, `in_reply_to` and `references` printable
/// ASCII of at most 980 bytes, `subject` free of control characters.
pub struct Message<'a> {
    pub to: &'a str,
    pub cc: Option<&'a str>,
    pub subject: &'a str,
    pub in_reply_to: Option<&'a str>,
    pub references: Option<&'a str>,
    pub body: &'a Body,
}

/// A body with every line break as CRLF: the bytes the message encodes, so
/// a budget on its length measures what is sent.
pub struct Body(String);

impl Body {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// The RFC 5322 message, CRLF-terminated lines, as bytes: the headers, an
/// empty line, and the body's CRLF form as base64. No `From` or `Date`: Gmail
/// writes the account's `From` on a draft and the `Date` when it is sent.
pub fn message(m: &Message) -> Vec<u8> {
    let mut out = fold_ascii("To", m.to);
    if let Some(cc) = m.cc {
        out.push_str(&fold_ascii("Cc", cc));
    }
    out.push_str(&header("Subject", m.subject));
    if let Some(in_reply_to) = m.in_reply_to {
        out.push_str(&fold_ascii("In-Reply-To", in_reply_to));
    }
    if let Some(references) = m.references {
        out.push_str(&fold_ascii("References", references));
    }
    out.push_str("MIME-Version: 1.0\r\n");
    out.push_str("Content-Type: text/plain; charset=utf-8\r\n");
    out.push_str("Content-Transfer-Encoding: base64\r\n");
    out.push_str("\r\n");
    out.push_str(&base64_lines(m.body.0.as_bytes()));
    out.into_bytes()
}

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

/// Standard base64 of `bytes`, padded, in lines of 76 characters each ending
/// CRLF. Empty for no bytes.
pub fn base64_lines(bytes: &[u8]) -> String {
    let encoded = base64(bytes, STANDARD, true);
    let mut out = String::with_capacity(encoded.len() + encoded.len() / BASE64_LINE_CHARS * 2 + 2);
    for line in encoded.as_bytes().chunks(BASE64_LINE_CHARS) {
        out.push_str(std::str::from_utf8(line).expect("base64 is ASCII"));
        out.push_str("\r\n");
    }
    out
}

/// base64url of `bytes`, unpadded: what Gmail's `raw` takes.
pub fn base64url(bytes: &[u8]) -> String {
    base64(bytes, URL_SAFE, false)
}

/// base64 of `bytes` over `alphabet`, padded with `=` when `pad`.
fn base64(bytes: &[u8], alphabet: &[u8; 64], pad: bool) -> String {
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for group in bytes.chunks(3) {
        let n = group
            .iter()
            .enumerate()
            .fold(0u32, |n, (i, &b)| n | (u32::from(b) << (16 - 8 * i)));
        let chars = group.len() + 1;
        for i in 0..4 {
            if i < chars {
                let index = (n >> (18 - 6 * i)) & 0x3f;
                out.push(char::from(alphabet[index as usize]));
            } else if pad {
                out.push('=');
            }
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
    format!("=?utf-8?B?{}?=", base64(text.as_bytes(), STANDARD, true))
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

#[cfg(test)]
mod tests {
    use super::super::decode;
    use super::*;

    #[test]
    fn base64_matches_rfc_4648s_vectors() {
        for (bytes, padded, unpadded) in [
            ("", "", ""),
            ("f", "Zg==", "Zg"),
            ("fo", "Zm8=", "Zm8"),
            ("foo", "Zm9v", "Zm9v"),
            ("foob", "Zm9vYg==", "Zm9vYg"),
            ("fooba", "Zm9vYmE=", "Zm9vYmE"),
            ("foobar", "Zm9vYmFy", "Zm9vYmFy"),
        ] {
            assert_eq!(base64(bytes.as_bytes(), STANDARD, true), padded);
            assert_eq!(base64url(bytes.as_bytes()), unpadded);
        }
        // The two alphabets differ in their last two characters only.
        assert_eq!(base64(&[0xfb, 0xff], STANDARD, true), "+/8=");
        assert_eq!(base64url(&[0xfb, 0xff]), "-_8");
        let every: Vec<u8> = (0..=255).collect();
        assert_eq!(decode::base64url(&base64url(&every)).unwrap(), every);
    }

    #[test]
    fn base64_lines_are_76_characters_ending_crlf() {
        assert_eq!(base64_lines(b""), "");
        assert_eq!(base64_lines(b"foobar"), "Zm9vYmFy\r\n");
        let bytes: Vec<u8> = (0..200).collect();
        let text = base64_lines(&bytes);
        let lines: Vec<&str> = text.split_terminator("\r\n").collect();
        assert_eq!(lines.len(), 4, "268 characters");
        assert!(lines[..3].iter().all(|line| line.len() == 76));
        assert_eq!(lines[3].len(), 268 - 3 * 76);
        assert!(text.ends_with("\r\n") && !text.contains("\n\n"));
        assert_eq!(decode::base64url(&text).unwrap(), bytes);
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
    fn a_message_is_its_headers_an_empty_line_and_its_body_as_base64() {
        let full = message(&Message {
            to: "Alice Example <alice@example.com>",
            cc: Some("bob@example.com"),
            subject: "Lunch",
            in_reply_to: Some("<m1@mail.example.com>"),
            references: Some("<m0@mail.example.com> <m1@mail.example.com>"),
            body: &canonical("Noon?\nSee you."),
        });
        assert_eq!(
            String::from_utf8(full).unwrap(),
            "To: Alice Example <alice@example.com>\r\n\
             Cc: bob@example.com\r\n\
             Subject: Lunch\r\n\
             In-Reply-To: <m1@mail.example.com>\r\n\
             References: <m0@mail.example.com> <m1@mail.example.com>\r\n\
             MIME-Version: 1.0\r\n\
             Content-Type: text/plain; charset=utf-8\r\n\
             Content-Transfer-Encoding: base64\r\n\
             \r\n\
             Tm9vbj8NClNlZSB5b3Uu\r\n"
        );
        assert_eq!(
            decode::base64url("Tm9vbj8NClNlZSB5b3Uu").unwrap(),
            b"Noon?\r\nSee you."
        );

        let bare = message(&Message {
            to: "alice@example.com",
            cc: None,
            subject: "Lunch",
            in_reply_to: None,
            references: None,
            body: &canonical("x"),
        });
        let bare = String::from_utf8(bare).unwrap();
        assert!(bare.starts_with("To: alice@example.com\r\nSubject: Lunch\r\nMIME-Version"));
    }
}
