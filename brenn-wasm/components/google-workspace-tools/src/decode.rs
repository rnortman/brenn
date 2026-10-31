//! Turning Gmail's encodings into text a model reads: a header's value from
//! `payload.headers`, HTML character references in `snippet`, and
//! `internalDate` as an RFC 3339 instant. Pure functions with no crate behind
//! them beyond `serde_json`'s value type.

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
}
