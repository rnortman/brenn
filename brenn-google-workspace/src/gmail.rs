//! What every Gmail component shares: ids, the messages path, the metadata
//! query, header reading and the MIME decoder.

pub mod decode;

use brenn_provider_kit::{Args, Failure, Query, QueryValue, query};
use serde_json::Value;

use crate::args::{id_arg, is_token};
use crate::text::shape;

/// The most characters a Gmail id (message, thread or label) may hold. Ids
/// are one path segment of letters, digits, `_` and `-`, which `api-request`
/// neither decodes nor refuses; being ASCII, an id's characters are its bytes,
/// so the bound holds for ids Google answers too.
pub const ID_MAX_CHARS: usize = 128;

/// The most UTF-8 bytes of a message header's value copied into a result. A
/// sender writes these, and nothing bounds their length.
///
/// After `plain`, a string serializes to at most two bytes per UTF-8 byte, so
/// a value cut here serializes to at most 2 003 bytes (`…` is three). A
/// search line is then at most 8 438 serialized bytes plus its label names
/// (three headers and a snippet at 2 003 each, two 128-byte ids, keys and
/// date), the page's envelope at most 8 300 (a page token of
/// `PAGE_MAX_BYTES` serializes to at most 8 206), and a page of 25 lines
/// stays under `COMPACT_RESULT_CEILING` while each line's label names
/// serialize to at most 1 715 bytes. A page past that answers `internal`. A
/// listed message is a line or an `unavailable` entry, and an entry (an id and
/// a reason cut to `google::MESSAGE_MAX_BYTES`) serializes to at most 2 155.
pub const HEADER_MAX_BYTES: usize = 1000;

/// The path Gmail lists messages at, and reads one message below.
pub const MESSAGES_PATH: &str = "/gmail/v1/users/me/messages";

/// A `format=metadata` read's query: the `fields` mask, and `headers`
/// requested by repeating `metadataHeaders`.
pub fn metadata_query(fields: &str, headers: &[&str]) -> Query {
    let mut query = query([
        ("format", "metadata".to_string()),
        ("fields", fields.to_string()),
    ]);
    query.insert(
        "metadataHeaders".to_string(),
        QueryValue::Many(headers.iter().map(|h| h.to_string()).collect()),
    );
    query
}

/// Whether `id` is a Gmail message, thread or label id.
pub fn is_gmail_id(id: &str) -> bool {
    is_token(id, ID_MAX_CHARS)
}

/// A Gmail id argument: one path segment of letters, digits, `_` and `-`.
pub fn gmail_id(args: &Args, key: &str) -> Result<Option<String>, String> {
    id_arg(args, key, is_gmail_id, gmail_id_rule)
}

/// What `is_gmail_id` admits, as a refusal states it.
pub fn gmail_id_rule() -> String {
    format!("1 to {ID_MAX_CHARS} letters, digits, `_` or `-`")
}

/// The value of the header `name` in `message`'s `payload.headers` list as
/// Google sent it, uncut and unfiltered: compared, or held to a rule of the
/// caller's before it is written anywhere. Absent and malformed as
/// `header_value` reads them.
pub fn raw_header<'a>(
    message: &'a Value,
    name: &str,
    api: &str,
) -> Result<Option<&'a str>, Failure> {
    let headers = match message.get("payload") {
        None => return Ok(None),
        Some(Value::Object(payload)) => match payload.get("headers") {
            None => return Ok(None),
            Some(headers) => headers,
        },
        Some(_) => return Err(shape(api)),
    };
    decode::header(headers, name).map_err(|decode::MalformedHeaders| shape(api))
}
