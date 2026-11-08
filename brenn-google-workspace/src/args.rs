//! Argument parsers every Google Workspace tool shares: the account, ids and
//! id lists, `max_chars`, printable ASCII.

use std::collections::BTreeSet;

use brenn_provider_kit::{Args, capped, integer};
use serde_json::Value;

/// The most characters an `account` argument may hold: a mailbox address's
/// limit.
pub const ACCOUNT_MAX_CHARS: usize = 320;

/// `GmailRead`'s `max_chars`: the range and the default.
pub const MAX_CHARS_MIN: u64 = 200;
pub const MAX_CHARS_MAX: u64 = 60_000;
pub const MAX_CHARS_DEFAULT: u64 = 12_000;

/// The most characters a search query may hold.
pub const QUERY_MAX_CHARS: usize = 1024;

/// The Google account to act on, when the caller named one.
pub fn account(args: &Args) -> Result<Option<String>, String> {
    Ok(capped(args, "account", ACCOUNT_MAX_CHARS)?.map(str::to_string))
}

/// Whether `id` is 1 to `max` letters, digits, `_` and `-`: one path segment
/// as Google writes its token ids.
pub fn is_token(id: &str, max: usize) -> bool {
    (1..=max).contains(&id.len())
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

/// An id argument held to `valid`; a refusal says "`key` must be" `what`.
pub fn id_arg(
    args: &Args,
    key: &str,
    valid: fn(&str) -> bool,
    what: impl FnOnce() -> String,
) -> Result<Option<String>, String> {
    match args.get(key) {
        None => Ok(None),
        Some(Value::String(id)) if valid(id) => Ok(Some(id.clone())),
        Some(_) => Err(format!("`{key}` must be {}", what())),
    }
}

/// An argument that is `min` to `max` distinct ids, each held to `valid`;
/// absent is `None`. Refusals say "`key` must be an array of `min` to `max`"
/// `what`, "each of `key` must be" `rule`, or that it names an id twice.
pub fn id_list(
    args: &Args,
    key: &str,
    (min, max): (usize, usize),
    what: &str,
    valid: fn(&str) -> bool,
    rule: impl Fn() -> String,
) -> Result<Option<Vec<String>>, String> {
    let Some(value) = args.get(key) else {
        return Ok(None);
    };
    let ids = match value {
        Value::Array(ids) if (min..=max).contains(&ids.len()) => ids,
        _ => return Err(format!("`{key}` must be an array of {min} to {max} {what}")),
    };
    let mut out: Vec<String> = Vec::with_capacity(ids.len());
    let mut seen = BTreeSet::new();
    for id in ids {
        match id {
            Value::String(id) if valid(id) => {
                if !seen.insert(id.as_str()) {
                    return Err(format!("`{key}` names {id:?} twice"));
                }
                out.push(id.clone());
            }
            _ => return Err(format!("each of `{key}` must be {}", rule())),
        }
    }
    Ok(Some(out))
}

/// The `max_chars` argument: `MAX_CHARS_MIN` to `MAX_CHARS_MAX`, default
/// `MAX_CHARS_DEFAULT`.
pub fn max_chars(args: &Args) -> Result<usize, String> {
    let n = integer(args, "max_chars", MAX_CHARS_MIN, MAX_CHARS_MAX)?.unwrap_or(MAX_CHARS_DEFAULT);
    Ok(usize::try_from(n).expect("max_chars is at most MAX_CHARS_MAX"))
}

/// Whether `value` is 1 to `max` bytes of 0x20 to 0x7E.
pub fn is_printable_ascii(value: &str, max: usize) -> bool {
    (1..=max).contains(&value.len()) && value.bytes().all(|b| (0x20..=0x7e).contains(&b))
}

/// What `is_printable_ascii` admits for `max`, as the `pattern` an
/// `input_schema` states it.
pub fn printable_ascii_pattern(max: usize) -> String {
    format!(r"^[\x20-\x7e]{{1,{max}}}$")
}

/// The host's cap on a nested call's serialized args, which the components'
/// tests hold their widest calls to: a copy of
/// `PROCESSOR_MAX_TOOL_ARGS_BYTES`, which a component cannot see.
// TODO(reuse-host-args-cap-copied-per-component-test): one copy, pinned
// to the host's constant, for every provider component's tests.
pub const HOST_ARGS_CAP_BYTES: usize = 64 * 1024;
