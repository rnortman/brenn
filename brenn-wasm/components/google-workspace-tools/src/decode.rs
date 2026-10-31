//! Turning Gmail's encodings into text a model reads: a header's value from
//! `payload.headers`, HTML character references in `snippet`, `internalDate`
//! as an RFC 3339 instant, and a `format=full` message's MIME tree as its text
//! parts, attachments and decoding notes — base64url and three charsets
//! decoded here. Pure functions with no crate behind them beyond
//! `serde_json`'s value type.

use serde_json::Value;

/// `headers` is not Gmail's header list: not an array, or an entry in it
/// without a string `name` and a string `value`.
#[derive(Debug, PartialEq, Eq)]
pub struct MalformedHeaders;

/// The value of the first header in `headers` — Gmail's `[{ "name", "value" }]`
/// list — whose name is `name`, compared case-insensitively; `None` when no
/// entry has that name.
pub fn header<'a>(headers: &'a Value, name: &str) -> Result<Option<&'a str>, MalformedHeaders> {
    let entries = headers.as_array().ok_or(MalformedHeaders)?;
    let mut found = None;
    for entry in entries {
        let (Some(Value::String(n)), Some(Value::String(value))) =
            (entry.get("name"), entry.get("value"))
        else {
            return Err(MalformedHeaders);
        };
        if found.is_none() && n.eq_ignore_ascii_case(name) {
            found = Some(value.as_str());
        }
    }
    Ok(found)
}

/// Named references decoded, each to its text. An empty text drops the
/// reference: a soft hyphen or a zero-width joiner carries nothing a reader
/// needs.
const NAMED: &[(&str, &str)] = &[
    ("amp", "&"),
    ("lt", "<"),
    ("gt", ">"),
    ("quot", "\""),
    ("apos", "'"),
    ("nbsp", "\u{a0}"),
    ("shy", ""),
    ("zwj", ""),
    ("zwnj", ""),
];

/// The longest reference body read between `&` and `;`: `#x10FFFF` and every
/// named reference fit.
const REFERENCE_MAX_CHARS: usize = 32;

/// `text` with its HTML character references decoded: the named ones above,
/// and decimal (`&#39;`) and hexadecimal (`&#x2019;`) numeric ones, a numeric
/// reference to no valid character becoming U+FFFD. An unknown name, or an
/// `&` that does not open a `;`-terminated reference, is left verbatim.
pub fn decode_entities(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(at) = rest.find('&') {
        out.push_str(&rest[..at]);
        let after = &rest[at + 1..];
        let body = after
            .char_indices()
            .take(REFERENCE_MAX_CHARS + 1)
            .find(|(_, c)| *c == ';')
            .map(|(end, _)| &after[..end]);
        match body.and_then(reference) {
            Some(decoded) => {
                out.push_str(&decoded);
                rest = &after[body.map_or(0, str::len) + 1..];
            }
            None => {
                out.push('&');
                rest = after;
            }
        }
    }
    out.push_str(rest);
    out
}

/// The text one reference body (between `&` and `;`) stands for, or `None`
/// when it is not one this decoder knows.
fn reference(body: &str) -> Option<String> {
    if let Some(number) = body.strip_prefix('#') {
        let code = match number.strip_prefix(['x', 'X']) {
            Some(hex) => parse_digits(hex, 16)?,
            None => parse_digits(number, 10)?,
        };
        let decoded = u32::try_from(code)
            .ok()
            .filter(|code| *code != 0)
            .and_then(char::from_u32)
            .unwrap_or(char::REPLACEMENT_CHARACTER);
        return Some(decoded.to_string());
    }
    NAMED
        .iter()
        .find(|(name, _)| *name == body)
        .map(|(_, text)| text.to_string())
}

/// `digits` in `radix`, saturating far past any code point; `None` when empty
/// or holding a non-digit.
fn parse_digits(digits: &str, radix: u32) -> Option<u64> {
    if digits.is_empty() {
        return None;
    }
    digits.chars().try_fold(0u64, |value, c| {
        let digit = c.to_digit(radix)?;
        Some(
            value
                .saturating_mul(u64::from(radix))
                .saturating_add(u64::from(digit)),
        )
    })
}

/// Gmail's `internalDate` — milliseconds since the Unix epoch, as a decimal
/// string — as an RFC 3339 UTC instant to the second
/// (`2026-10-31T14:05:09Z`). `None` when the string is not a non-negative
/// decimal integer that fits `u64`.
pub fn internal_date(epoch_ms: &str) -> Option<String> {
    if epoch_ms.is_empty() || !epoch_ms.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let ms: u64 = epoch_ms.parse().ok()?;
    Some(brenn_cal::ms_to_rfc3339_seconds(ms))
}

/// The bytes `data` encodes in base64url — `+` and `/` accepted for `-` and
/// `_`, `=` padding and whitespace ignored. `None` for any other character,
/// or a final group of one character, which encodes no whole byte.
pub fn base64url(data: &str) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(data.len() / 4 * 3 + 2);
    let mut bits: u32 = 0;
    let mut held = 0u32;
    let mut group = 0usize;
    for b in data.bytes() {
        let value = match b {
            b'A'..=b'Z' => b - b'A',
            b'a'..=b'z' => b - b'a' + 26,
            b'0'..=b'9' => b - b'0' + 52,
            b'-' | b'+' => 62,
            b'_' | b'/' => 63,
            b'=' | b' ' | b'\t' | b'\r' | b'\n' => continue,
            _ => return None,
        };
        bits = (bits << 6) | u32::from(value);
        held += 6;
        group = (group + 1) % 4;
        if held >= 8 {
            held -= 8;
            out.push((bits >> held) as u8);
            bits &= (1 << held) - 1;
        }
    }
    (group != 1).then_some(out)
}

/// Windows-1252's 0x80–0x9F, where it differs from ISO-8859-1; `None` for
/// the five bytes it leaves undefined.
const CP1252_HIGH: [Option<char>; 32] = [
    Some('\u{20ac}'),
    None,
    Some('\u{201a}'),
    Some('\u{0192}'),
    Some('\u{201e}'),
    Some('\u{2026}'),
    Some('\u{2020}'),
    Some('\u{2021}'),
    Some('\u{02c6}'),
    Some('\u{2030}'),
    Some('\u{0160}'),
    Some('\u{2039}'),
    Some('\u{0152}'),
    None,
    Some('\u{017d}'),
    None,
    None,
    Some('\u{2018}'),
    Some('\u{2019}'),
    Some('\u{201c}'),
    Some('\u{201d}'),
    Some('\u{2022}'),
    Some('\u{2013}'),
    Some('\u{2014}'),
    Some('\u{02dc}'),
    Some('\u{2122}'),
    Some('\u{0161}'),
    Some('\u{203a}'),
    Some('\u{0153}'),
    None,
    Some('\u{017e}'),
    Some('\u{0178}'),
];

/// Something about a message's decoding the reader is told, rather than shown
/// wrong text silently. Strings quoted are the sender's, verbatim.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Note {
    /// A text part's declared charset is not one decoded here; it was read as
    /// UTF-8.
    UnknownCharset(String),
    /// A text part declared as UTF-8 or ASCII held invalid bytes.
    InvalidUtf8,
    /// A `multipart/*` or `message/rfc822` part at `depth` (the payload is 0)
    /// came without its children.
    UnlistedChildren { mime_type: String, depth: usize },
    /// An HTML part was chosen as text, and this build does not render HTML.
    HtmlNotRendered,
}

/// Which kind of part a text came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Source {
    Plain,
    Html,
}

/// A part listed rather than read: what the sender attached, or a non-text
/// part of the body.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Attachment {
    /// The part's `filename`, `""` when Google gives none.
    pub filename: String,
    pub mime_type: String,
    /// `body.size`, when Google gives it.
    pub size: Option<u64>,
    /// Shown in the body (`Content-Disposition: inline` or a `Content-ID`),
    /// not attached beside it.
    pub inline: bool,
}

/// What a MIME tree yields: its text parts in reading order, every part
/// listed as an attachment, and the decoding notes.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Extracted {
    pub text_parts: Vec<(String, Source)>,
    pub attachments: Vec<Attachment>,
    pub notes: Vec<Note>,
    /// A `text/plain` or `text/html` part came as an attachment reference
    /// (`body.attachmentId`, no `data`) and was listed, not read.
    pub text_by_reference: bool,
}

/// Why a MIME tree could not be read.
#[derive(Debug, PartialEq, Eq)]
pub enum WalkError {
    /// A part is not of Gmail's shape.
    Shape,
    /// A part's `body.data` is not base64url; the part's `partId`, or its
    /// MIME type when it has none.
    Base64(String),
}

/// A message's MIME tree, `payload` and its `parts` as Gmail gives them,
/// read into text, attachments and notes.
///
/// A part with a non-empty `filename` or a `body.attachmentId` is an
/// attachment. `multipart/alternative` takes the first child yielding
/// `text/plain` text, else the first yielding `text/html`, else the first
/// yielding any text, and lists every child's attachments. Every other
/// `multipart/*` and `message/rfc822` yields its children's in order; one
/// with no `parts` yields nothing and a note. A `text/plain` leaf is decoded
/// in its charset; a `text/html` leaf yields an empty HTML text and a note. A
/// text leaf with no `data` is empty and yields nothing; any other leaf is an
/// attachment.
pub fn extract(payload: &Value) -> Result<Extracted, WalkError> {
    let mut out = Extracted::default();
    walk(payload, 0, &mut out)?;
    Ok(out)
}

fn walk(part: &Value, depth: usize, out: &mut Extracted) -> Result<(), WalkError> {
    let declared = part
        .get("mimeType")
        .and_then(Value::as_str)
        .ok_or(WalkError::Shape)?;
    let mime_type = declared.to_ascii_lowercase();
    let filename = match part.get("filename") {
        None => "",
        Some(Value::String(name)) => name.as_str(),
        Some(_) => return Err(WalkError::Shape),
    };
    let body = match part.get("body") {
        None => None,
        Some(body @ Value::Object(_)) => Some(body),
        Some(_) => return Err(WalkError::Shape),
    };
    let field = |name: &str| body.and_then(|body| body.get(name));
    let attachment_id = field("attachmentId").is_some();
    let no_headers = Value::Array(Vec::new());
    let headers = part.get("headers").unwrap_or(&no_headers);
    let header = |name: &str| header(headers, name).map_err(|MalformedHeaders| WalkError::Shape);
    let is_text = mime_type == "text/plain" || mime_type == "text/html";
    let is_container = mime_type.starts_with("multipart/") || mime_type == "message/rfc822";

    if !filename.is_empty() || attachment_id || (!is_text && !is_container) {
        if is_text && filename.is_empty() {
            out.text_by_reference = true;
        }
        let inline = header("Content-Disposition")?
            .is_some_and(|d| d.trim_start().to_ascii_lowercase().starts_with("inline"))
            || header("Content-ID")?.is_some();
        let size = match field("size") {
            None => None,
            Some(size) => Some(size.as_u64().ok_or(WalkError::Shape)?),
        };
        out.attachments.push(Attachment {
            filename: filename.to_string(),
            mime_type: declared.to_string(),
            size,
            inline,
        });
        return Ok(());
    }

    if is_container {
        let children = match part.get("parts") {
            None => {
                out.notes.push(Note::UnlistedChildren {
                    mime_type: declared.to_string(),
                    depth,
                });
                return Ok(());
            }
            Some(Value::Array(children)) => children,
            Some(_) => return Err(WalkError::Shape),
        };
        if mime_type != "multipart/alternative" {
            for child in children {
                walk(child, depth + 1, out)?;
            }
            return Ok(());
        }
        let mut read = Vec::with_capacity(children.len());
        for child in children {
            let mut one = Extracted::default();
            walk(child, depth + 1, &mut one)?;
            read.push(one);
        }
        let yields = |source: Option<Source>| {
            read.iter().position(|one| {
                one.text_parts
                    .iter()
                    .any(|(_, s)| source.is_none_or(|source| *s == source))
            })
        };
        let chosen = yields(Some(Source::Plain))
            .or_else(|| yields(Some(Source::Html)))
            .or_else(|| yields(None));
        for (index, one) in read.into_iter().enumerate() {
            out.attachments.extend(one.attachments);
            out.text_by_reference |= one.text_by_reference;
            if Some(index) == chosen {
                out.text_parts.extend(one.text_parts);
                out.notes.extend(one.notes);
            } else {
                // A child not read still lists its attachments, so an
                // incomplete listing below it is still said.
                out.notes.extend(
                    one.notes
                        .into_iter()
                        .filter(|note| matches!(note, Note::UnlistedChildren { .. })),
                );
            }
        }
        return Ok(());
    }

    let data = match field("data") {
        None => return Ok(()),
        Some(Value::String(data)) => data,
        Some(_) => return Err(WalkError::Shape),
    };
    let bytes = base64url(data).ok_or_else(|| {
        WalkError::Base64(match part.get("partId").and_then(Value::as_str) {
            Some(id) if !id.is_empty() => id.to_string(),
            _ => mime_type.clone(),
        })
    })?;
    if mime_type == "text/html" {
        out.text_parts.push((String::new(), Source::Html));
        out.notes.push(Note::HtmlNotRendered);
        return Ok(());
    }
    let content_type = header("Content-Type")?;
    let text = decode_charset(&bytes, content_type.and_then(charset), &mut out.notes);
    out.text_parts.push((text, Source::Plain));
    Ok(())
}

/// The `charset` parameter of a `Content-Type` value, unquoted.
fn charset(content_type: &str) -> Option<&str> {
    content_type.split(';').skip(1).find_map(|param| {
        let (key, value) = param.split_once('=')?;
        key.trim()
            .eq_ignore_ascii_case("charset")
            .then(|| value.trim().trim_matches('"'))
    })
}

/// `bytes` as text in `charset`: UTF-8 (and ASCII, and no charset at all) and
/// Windows-1252 exactly; any other charset as UTF-8, with a note. Invalid
/// UTF-8 is replaced with U+FFFD, with a note.
///
/// The ISO-8859-1 labels are read as Windows-1252, as browsers read them:
/// mail so labelled routinely carries Windows-1252 punctuation in 0x80–0x9F,
/// where ISO-8859-1 has only invisible control characters.
pub fn decode_charset(bytes: &[u8], charset: Option<&str>, notes: &mut Vec<Note>) -> String {
    let name = charset.map(str::to_ascii_lowercase);
    match name.as_deref() {
        Some("windows-1252" | "cp1252" | "iso-8859-1" | "latin1" | "iso_8859-1" | "l1") => bytes
            .iter()
            .map(|b| match b {
                0x80..=0x9f => {
                    CP1252_HIGH[usize::from(b - 0x80)].unwrap_or(char::REPLACEMENT_CHARACTER)
                }
                _ => char::from(*b),
            })
            .collect(),
        None | Some("utf-8" | "utf8" | "us-ascii" | "ascii") => match std::str::from_utf8(bytes) {
            Ok(text) => text.to_string(),
            Err(_) => {
                notes.push(Note::InvalidUtf8);
                String::from_utf8_lossy(bytes).into_owned()
            }
        },
        Some(_) => {
            notes.push(Note::UnknownCharset(
                charset.unwrap_or_default().to_string(),
            ));
            String::from_utf8_lossy(bytes).into_owned()
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn a_header_is_the_first_case_insensitive_match() {
        let headers = json!([
            { "name": "Received", "value": "by mx.example.com" },
            { "name": "SUBJECT", "value": "first" },
            { "name": "Subject", "value": "second" },
        ]);
        assert_eq!(header(&headers, "subject"), Ok(Some("first")));
        assert_eq!(header(&headers, "Subject"), Ok(Some("first")));
        assert_eq!(header(&headers, "Date"), Ok(None));
        assert_eq!(header(&json!([]), "Subject"), Ok(None));
    }

    #[test]
    fn a_header_list_not_of_gmails_shape_is_malformed() {
        for headers in [
            json!({}),
            json!("Subject: Lunch"),
            json!([{ "name": "From" }]),
            json!([{ "value": "nameless" }]),
            json!([{ "name": "Subject", "value": 7 }]),
            json!([{ "name": 7, "value": "Lunch" }]),
            json!([{ "name": "Subject", "value": "Lunch" }, "Date"]),
        ] {
            assert_eq!(
                header(&headers, "Subject"),
                Err(MalformedHeaders),
                "{headers}"
            );
        }
    }

    #[test]
    fn named_references_decode() {
        assert_eq!(
            decode_entities("Tom &amp; Jerry &lt;b&gt; &quot;hi&quot; it&apos;s"),
            "Tom & Jerry <b> \"hi\" it's"
        );
        assert_eq!(decode_entities("a&nbsp;b"), "a\u{a0}b");
        assert_eq!(decode_entities("co&shy;op&zwj;&zwnj;"), "coop");
    }

    #[test]
    fn numeric_references_decode() {
        assert_eq!(decode_entities("it&#39;s"), "it's");
        assert_eq!(
            decode_entities("&#x2019;&#X2014;&#8364;"),
            "\u{2019}\u{2014}€"
        );
        assert_eq!(decode_entities("&#x1F600;"), "😀");
    }

    #[test]
    fn a_numeric_reference_to_no_character_is_the_replacement_character() {
        for text in [
            "&#0;",
            "&#xD800;",
            "&#x110000;",
            "&#99999999999999999999999;",
        ] {
            assert_eq!(decode_entities(text), "\u{fffd}", "{text}");
        }
    }

    #[test]
    fn what_is_not_a_reference_is_left_verbatim() {
        for text in [
            "AT&T",
            "a & b",
            "&unknown;",
            "&amp",
            "&#;",
            "&#x;",
            "&#12a;",
            "&;",
            "trailing &",
        ] {
            assert_eq!(decode_entities(text), text, "{text}");
        }
        let long = format!("&{};", "a".repeat(40));
        assert_eq!(decode_entities(&long), long);
        assert_eq!(decode_entities("&&amp;&"), "&&&");
        assert_eq!(decode_entities("é&amp;ü"), "é&ü");
    }

    #[test]
    fn decoding_is_one_pass() {
        assert_eq!(decode_entities("&amp;lt;"), "&lt;");
    }

    #[test]
    fn internal_date_is_rfc3339_utc_to_the_second() {
        assert_eq!(internal_date("0").as_deref(), Some("1970-01-01T00:00:00Z"));
        assert_eq!(
            internal_date("1793455509123").as_deref(),
            Some("2026-10-31T14:05:09Z")
        );
        // A leap day, and the day after.
        assert_eq!(
            internal_date("1709208000000").as_deref(),
            Some("2024-02-29T12:00:00Z")
        );
        assert_eq!(
            internal_date("1709251199999").as_deref(),
            Some("2024-02-29T23:59:59Z")
        );
        assert_eq!(
            internal_date("1709251200000").as_deref(),
            Some("2024-03-01T00:00:00Z")
        );
        // 2100 is not a leap year; 2000 was.
        assert_eq!(
            internal_date("4107542400000").as_deref(),
            Some("2100-03-01T00:00:00Z")
        );
        assert_eq!(
            internal_date("951782400000").as_deref(),
            Some("2000-02-29T00:00:00Z")
        );
        // A far date.
        assert_eq!(
            internal_date("253402300799000").as_deref(),
            Some("9999-12-31T23:59:59Z")
        );
    }

    #[test]
    fn internal_date_refuses_what_is_not_epoch_milliseconds() {
        for text in [
            "",
            "-1",
            "12a",
            "1.5",
            " 1",
            "+1",
            "99999999999999999999999",
        ] {
            assert_eq!(internal_date(text), None, "{text}");
        }
    }

    #[test]
    fn base64url_decodes_either_alphabet_padded_or_not() {
        assert_eq!(base64url("").unwrap(), b"");
        assert_eq!(base64url("TWFu").unwrap(), b"Man");
        assert_eq!(base64url("TWE").unwrap(), b"Ma");
        assert_eq!(base64url("TWE=").unwrap(), b"Ma");
        assert_eq!(base64url("TQ==").unwrap(), b"M");
        assert_eq!(base64url("-_-_").unwrap(), [0xfb, 0xff, 0xbf]);
        assert_eq!(base64url("+/+/").unwrap(), [0xfb, 0xff, 0xbf]);
        assert_eq!(base64url("TW\r\nFu IA\t").unwrap(), b"Man ");
    }

    #[test]
    fn base64url_refuses_what_is_not_base64() {
        for data in ["TW*u", "TWFuT", "é", "TWFu.", "T"] {
            assert_eq!(base64url(data), None, "{data}");
        }
    }

    #[test]
    fn three_charsets_decode_exactly() {
        let mut notes = Vec::new();
        assert_eq!(
            decode_charset(b"caf\xe9", Some("ISO-8859-1"), &mut notes),
            "café"
        );
        for latin1 in ["latin1", "iso_8859-1", "L1"] {
            assert_eq!(
                decode_charset(b"\x80\x93", Some(latin1), &mut notes),
                "€\u{201c}"
            );
        }
        assert_eq!(
            decode_charset(
                b"\x80 \x93x\x94 \x9f \xe9",
                Some("Windows-1252"),
                &mut notes
            ),
            "€ \u{201c}x\u{201d} Ÿ é"
        );
        for undefined in [0x81, 0x8d, 0x8f, 0x90, 0x9d] {
            assert_eq!(
                decode_charset(&[undefined], Some("cp1252"), &mut notes),
                "\u{fffd}"
            );
        }
        assert_eq!(CP1252_HIGH.iter().flatten().count(), 27);
        for name in [None, Some("utf-8"), Some("UTF8"), Some("us-ascii")] {
            assert_eq!(decode_charset("é😀".as_bytes(), name, &mut notes), "é😀");
        }
        assert_eq!(notes, []);
    }

    #[test]
    fn invalid_utf8_and_unknown_charsets_are_noted() {
        let mut notes = Vec::new();
        assert_eq!(decode_charset(b"a\xffb", None, &mut notes), "a\u{fffd}b");
        assert_eq!(notes, [Note::InvalidUtf8]);
        let mut notes = Vec::new();
        assert_eq!(decode_charset(b"ok", Some("KOI8-R"), &mut notes), "ok");
        assert_eq!(notes, [Note::UnknownCharset("KOI8-R".to_string())]);
    }

    #[test]
    fn the_charset_parameter_is_read_case_insensitively_and_unquoted() {
        assert_eq!(charset("text/plain; CharSet=\"UTF-8\""), Some("UTF-8"));
        assert_eq!(
            charset("text/plain; format=flowed; charset=iso-8859-1"),
            Some("iso-8859-1")
        );
        assert_eq!(charset("text/plain"), None);
        assert_eq!(charset("charset=utf-8"), None);
    }

    /// A `multipart/alternative` payload of `children`.
    fn alternative(children: Value) -> Value {
        json!({ "mimeType": "multipart/alternative", "parts": children })
    }

    fn html_part() -> Value {
        // "<p>hi</p>"
        json!({ "mimeType": "text/html", "body": { "data": "PHA-aGk8L3A-" } })
    }

    fn plain_part(data: &str, charset: &str) -> Value {
        json!({ "mimeType": "text/plain", "body": { "data": data },
                "headers": [{ "name": "Content-Type",
                              "value": format!("text/plain; charset={charset}") }] })
    }

    #[test]
    fn an_alternative_prefers_plain_wherever_it_stands() {
        // "plain"
        let read = extract(&alternative(json!([
            html_part(),
            plain_part("cGxhaW4", "utf-8")
        ])))
        .unwrap();
        assert_eq!(read.text_parts, [("plain".to_string(), Source::Plain)]);
        assert_eq!(read.notes, []);
    }

    #[test]
    fn an_alternative_without_plain_takes_html_and_lists_the_rest() {
        let image = json!({ "mimeType": "image/png", "body": { "size": 4 } });
        let read = extract(&alternative(json!([image, html_part()]))).unwrap();
        assert_eq!(read.text_parts, [(String::new(), Source::Html)]);
        assert_eq!(read.notes, [Note::HtmlNotRendered]);
        assert_eq!(
            read.attachments,
            [Attachment {
                filename: String::new(),
                mime_type: "image/png".to_string(),
                size: Some(4),
                inline: false,
            }]
        );
    }

    #[test]
    fn an_alternative_not_read_keeps_only_its_unlisted_children_note() {
        let read = extract(&alternative(json!([
            plain_part("cGxhaW4", "utf-8"),
            plain_part("b3RoZXI", "KOI8-R"),
            html_part(),
            { "mimeType": "multipart/related" },
        ])))
        .unwrap();
        assert_eq!(read.text_parts, [("plain".to_string(), Source::Plain)]);
        assert_eq!(
            read.notes,
            [Note::UnlistedChildren {
                mime_type: "multipart/related".to_string(),
                depth: 1,
            }]
        );
    }

    #[test]
    fn either_inline_signal_alone_marks_an_attachment_inline() {
        let part = |headers: Value| {
            json!({ "mimeType": "image/png", "filename": "a.png",
                    "body": { "attachmentId": "A" }, "headers": headers })
        };
        let read = extract(&json!({ "mimeType": "multipart/mixed", "parts": [
            part(json!([{ "name": "Content-ID", "value": "<a@example.com>" },
                        { "name": "Content-Disposition", "value": "attachment" }])),
            part(json!([{ "name": "content-disposition", "value": " INLINE; filename=a.png" }])),
            part(json!([{ "name": "Content-Disposition", "value": "attachment; filename=a.png" }])),
            part(json!([])),
        ] }))
        .unwrap();
        let inline: Vec<bool> = read.attachments.iter().map(|a| a.inline).collect();
        assert_eq!(inline, [true, true, false, false]);
    }
}
