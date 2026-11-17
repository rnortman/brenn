//! Text copied from Google into an answer: control characters replaced,
//! cuts by characters and by serialized width, page tokens, and the shape
//! failure every reader answers.

use brenn_provider_kit::{Args, Failure, cut, internal, serialized_len};
use serde_json::{Value, json};

use crate::gmail::HEADER_MAX_BYTES;

/// The most characters a `page` argument may hold.
pub const PAGE_MAX_CHARS: usize = 4096;
/// The most bytes of a page token Google answers as `nextPageToken`. Counted
/// in bytes because it sizes the answer; no more than `PAGE_MAX_CHARS`, as
/// asserted below, so a token Google gave is one `page` admits back.
pub const PAGE_MAX_BYTES: usize = 4096;
const _: () = assert!(PAGE_MAX_BYTES <= PAGE_MAX_CHARS);

/// `text`'s first `max_chars` characters, and whether any were left out.
pub fn chars_prefix(text: &str, max_chars: usize) -> (&str, bool) {
    match text.char_indices().nth(max_chars) {
        Some((end, _)) => (&text[..end], true),
        None => (text, false),
    }
}

/// `text` with every C0 control character other than tab, newline and
/// carriage return replaced by a space. Each of those would serialize as six
/// bytes (`\u00XX`) and has no use in a header, a snippet or a body.
pub fn plain(text: &str) -> String {
    text.chars()
        .map(|c| {
            if c.is_ascii() {
                char::from(plain_byte(c as u8))
            } else {
                c
            }
        })
        .collect()
}

/// One byte of UTF-8 text as [`plain`] maps it; `plain` applies this rule to
/// each ASCII character. A C0 control character is one byte and every byte
/// of a longer character is above 0x7F, so mapping a text's bytes one by one
/// is `plain` of the text, piece by piece.
pub fn plain_byte(byte: u8) -> u8 {
    match byte {
        b'\t' | b'\n' | b'\r' => byte,
        0x00..=0x1f => b' ',
        _ => byte,
    }
}

/// Whether [`plain_byte`] changes any of `bytes`. The mask marks the bytes
/// of a word that `plain_byte` changes, so it states the same rule.
pub fn plain_changes(bytes: &[u8]) -> bool {
    use crate::bytes::{below, equal};
    let mask = |word| match below(word, 0x20) {
        0 => 0,
        controls => controls & !equal(word, b'\t') & !equal(word, b'\n') & !equal(word, b'\r'),
    };
    crate::bytes::find(bytes, 0, mask, |b| plain_byte(b) != b) < bytes.len()
}

/// A sender-written string quoted in a fixed sentence: through `plain`, cut
/// to `HEADER_MAX_BYTES`. `plain` changes no character's length, so the cut
/// is made first, where it falls on the plain text too, and only what is
/// kept is mapped.
pub fn bounded_quote(text: &str) -> String {
    let (quoted, _) = cut(text, HEADER_MAX_BYTES);
    if plain_changes(quoted.as_bytes()) {
        plain(&quoted)
    } else {
        quoted
    }
}

/// The serialized width of `c` in a JSON string, as `serde_json` writes it.
pub fn json_width(c: char) -> usize {
    match c {
        '"' | '\\' | '\n' | '\r' | '\t' | '\u{8}' | '\u{c}' => 2,
        '\u{0}'..='\u{1f}' => 6,
        _ => c.len_utf8(),
    }
}

/// `text`, `plain`ed and cut at `max_chars` characters, under `key`, with
/// `{key}_chars` its whole length and `{key}_truncated` set when it was cut.
pub fn put_cut_text(out: &mut Args, key: &str, text: &str, max_chars: usize) {
    let text = plain(text);
    let (cut, truncated) = chars_prefix(&text, max_chars);
    if truncated {
        out.insert(format!("{key}_truncated"), json!(true));
    }
    out.insert(key.to_string(), json!(cut));
    out.insert(format!("{key}_chars"), json!(text.chars().count()));
}

/// `answer` with its text `key` cut to the longest prefix with which the
/// whole answer serializes within `ceiling`, and `{key}_truncated` set, when
/// it does not fit as it is. An answer that fits, or has no such text, is
/// unchanged; one that does not fit with an empty text is left for the ceiling
/// to refuse.
pub fn fit_text(mut answer: Value, key: &str, ceiling: usize) -> Value {
    if serialized_len(&answer) <= ceiling {
        return answer;
    }
    let Some(Value::String(text)) = answer.get_mut(key).map(std::mem::take) else {
        return answer;
    };
    answer[key] = json!("");
    answer[format!("{key}_truncated")] = json!(true);
    let budget = ceiling.saturating_sub(serialized_len(&answer));
    let mut used = 0;
    let mut end = 0;
    for (at, c) in text.char_indices() {
        used += json_width(c);
        if used > budget {
            break;
        }
        end = at + c.len_utf8();
    }
    answer[key] = json!(text[..end]);
    answer
}

/// Google's `nextPageToken` into `out` as `next_page`, when Google gave one;
/// one that `page` would refuse back — not a string, blank, or longer than
/// `PAGE_MAX_BYTES` — is Google's fault.
pub fn next_page(out: &mut Args, body: &Args, api: &str) -> Result<(), Failure> {
    match body.get("nextPageToken") {
        None => {}
        Some(Value::String(token)) if token.len() <= PAGE_MAX_BYTES && !token.trim().is_empty() => {
            out.insert("next_page".to_string(), json!(token));
        }
        Some(_) => return Err(shape(api)),
    }
    Ok(())
}

pub fn shape(api: &str) -> Failure {
    internal(format!("unexpected Google {api} response shape"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_byte_maps_every_byte_as_plain_maps_its_text() {
        let text = "a\u{0}b\tc\u{1b}é\u{7f}😀\r\n\u{1f}";
        let bytes: Vec<u8> = text.bytes().map(plain_byte).collect();
        assert_eq!(String::from_utf8(bytes).unwrap(), plain(text));
        for byte in 0x80..=0xff {
            assert_eq!(plain_byte(byte), byte);
        }
    }

    #[test]
    fn a_quote_is_the_plain_text_cut() {
        let wide = "é".repeat(HEADER_MAX_BYTES);
        let cases = [
            String::new(),
            "a\u{1}b\tc".to_string(),
            "x".repeat(HEADER_MAX_BYTES),
            "x".repeat(HEADER_MAX_BYTES + 1),
            format!("{}\u{1}", "x".repeat(HEADER_MAX_BYTES)),
            format!("\u{1f}{}", "x".repeat(2 * HEADER_MAX_BYTES)),
            format!("x{wide}"),
        ];
        for text in cases {
            assert_eq!(
                bounded_quote(&text),
                cut(&plain(&text), HEADER_MAX_BYTES).0,
                "{text:?}"
            );
        }
    }

    #[test]
    fn plain_changes_whatever_plain_byte_changes_wherever_it_stands() {
        for byte in 0..=0xff {
            for len in [1, 7, 8, 9, 23] {
                for at in 0..len {
                    let mut bytes = vec![b'a'; len];
                    bytes[at] = byte;
                    assert_eq!(
                        plain_changes(&bytes),
                        plain_byte(byte) != byte,
                        "{byte:#x} at {at} of {len}"
                    );
                }
            }
        }
        assert!(!plain_changes(b""));
    }

    #[test]
    fn plain_replaces_c0_controls_other_than_tab_newline_and_return() {
        assert_eq!(plain("a\u{0}b\u{1b}c\u{1f}d"), "a b c d");
        assert_eq!(plain("a\tb\nc\rd"), "a\tb\nc\rd");
        assert_eq!(plain("a\u{7f}é😀"), "a\u{7f}é😀");
    }

    #[test]
    fn fit_text_cuts_to_the_longest_text_that_fits() {
        let text = "a\"\\\n\r\t\u{8}\u{c}\u{1}é😀".repeat(20);
        let answer = json!({ "id": "m1", "text": text });
        let with_text = |text: &str| {
            let mut answer = answer.clone();
            answer["text"] = json!(text);
            answer["text_truncated"] = json!(true);
            serialized_len(&answer)
        };
        let whole = serialized_len(&answer);
        assert_eq!(fit_text(answer.clone(), "text", whole), answer);
        for ceiling in with_text("")..whole {
            let fitted = fit_text(answer.clone(), "text", ceiling);
            let cut = fitted["text"].as_str().unwrap();
            assert_eq!(fitted["text_truncated"], json!(true));
            assert!(serialized_len(&fitted) <= ceiling, "{ceiling}");
            let next = text[cut.len()..].chars().next().unwrap();
            assert!(
                with_text(&text[..cut.len() + next.len_utf8()]) > ceiling,
                "{ceiling}"
            );
        }
    }
}
