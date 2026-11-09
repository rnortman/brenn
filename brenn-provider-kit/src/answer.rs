//! Reading a nested `api-request` call's answer envelope, bounding what is
//! copied out of it, and the ceiling on a request's compact result. The text
//! the model sees names the API by the display name the caller passes.

use std::collections::BTreeMap;

use base64ct::{Base64, Encoding};
use serde_json::{Value, json};

use crate::args::Args;
use crate::call::{ErrorKind, Failure, Next, Target, internal};

/// A nested call's typed error, as the executor wrote it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NestedError {
    pub kind: String,
    pub detail: String,
}

/// A compact result larger than this is a bug in a size bound, answered
/// `internal` rather than sent.
pub const COMPACT_RESULT_CEILING: usize = 256 * 1024;

/// `text` cut to the longest prefix of at most `max_bytes` UTF-8 bytes that
/// ends on a char boundary, ending in `…` when cut, and whether it was cut. A
/// character straddling the bound is dropped whole.
pub fn cut(text: &str, max_bytes: usize) -> (String, bool) {
    if text.len() <= max_bytes {
        return (text.to_string(), false);
    }
    let at = text.floor_char_boundary(max_bytes);
    (format!("{}…", &text[..at]), true)
}

/// The detail of a nested call that issued but could not be queued: Brenn's
/// refusal text plus the one sentence of operator guidance.
pub(crate) fn not_issued(refusal: &str) -> String {
    format!(
        "api-request could not be issued: {refusal}. If this names the clause, the instance's \
         `tool api-request` grant must admit the method, host and path."
    )
}

/// One line for the log: status, size and truncation, or the error kind.
/// Never a body or a header.
pub(crate) fn result_summary(outcome: &Result<Value, NestedError>) -> String {
    match outcome {
        Ok(value) => format!(
            "status {} bytes {} truncated {}",
            value.get("status").unwrap_or(&Value::Null),
            value.get("bytes").unwrap_or(&Value::Null),
            value.get("truncated").unwrap_or(&Value::Null),
        ),
        Err(error) => format!("error {}", error.kind),
    }
}

/// The `derived` key carrying an [`Unreadable`] to a re-read step.
const UNREADABLE_KEY: &str = "unreadable";

/// Why a nested read's answer cannot be used whole: `api-request` cut it at
/// the provider's read limit (the size as the messages state it: `<n> bytes`,
/// or `of unstated length`), or the host did not deliver it (the nested
/// `internal` error's detail — the bus cap's refusal, or `api-request`'s
/// transport or refresh failure, which the detail alone tells apart).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Unreadable {
    Cut(String),
    Refused(String),
}

impl Unreadable {
    /// The one sentence that says why, naming the API by its display name
    /// `api`, with `note` — what the step knows of the answer's size — when
    /// it has one.
    pub fn render(&self, api: &str, note: Option<&str>) -> String {
        let note = note.map(|note| format!("; {note}")).unwrap_or_default();
        match self {
            Unreadable::Cut(size) => format!(
                "{api}'s answer ({size}{note}) exceeds api-request's read limit for this \
                 provider (max_response_bytes)"
            ),
            Unreadable::Refused(detail) => {
                format!("the host did not deliver api-request's answer: {detail}{note}")
            }
        }
    }

    /// The request's failure: `internal` with the rendering, and `hint` — what
    /// the caller can narrow — after it when the step has one.
    pub fn failure(&self, api: &str, hint: Option<&str>) -> Failure {
        let why = self.render(api, None);
        internal(match hint {
            Some(hint) => format!("{why}; {hint}"),
            None => why,
        })
    }

    /// The `derived` a re-read step is issued with: `unreadable` as
    /// `{"cut": size}` or `{"refused": detail}`.
    pub fn to_derived(&self) -> Args {
        let reason = match self {
            Unreadable::Cut(size) => json!({ "cut": size }),
            Unreadable::Refused(detail) => json!({ "refused": detail }),
        };
        Args::from_iter([(UNREADABLE_KEY.to_string(), reason)])
    }

    /// The reason [`Unreadable::to_derived`] wrote, read back at the re-read
    /// step.
    pub fn from_derived(derived: &Args) -> Result<Unreadable, Failure> {
        let reason = match derived.get(UNREADABLE_KEY) {
            Some(Value::Object(map)) if map.len() == 1 => map.iter().next(),
            _ => None,
        };
        match reason {
            Some((key, Value::String(text))) if key == "cut" => Ok(Unreadable::Cut(text.clone())),
            Some((key, Value::String(text))) if key == "refused" => {
                Ok(Unreadable::Refused(text.clone()))
            }
            _ => Err(internal(
                "in-flight row at a re-read step carries no `unreadable` reason",
            )),
        }
    }
}

/// What a nested `api-request` call answered, read.
#[derive(Clone, Debug, PartialEq)]
pub enum Answer {
    /// The API's 2xx JSON body.
    Body(Value),
    /// The API's 2xx body when it is not UTF-8, decoded. `api-request` sends a
    /// body as text whenever its bytes are valid UTF-8, whatever the API's
    /// content type, so a UTF-8 payload never arrives here: it is read as
    /// JSON, and fails as "a body that is not JSON" when it is not. Never
    /// empty: an empty body is text, and reads as [`Answer::NoBody`]. A
    /// component that wants a payload's bytes whichever field carried them
    /// calls [`read_api_payload`].
    Bytes { status: u64, body: Vec<u8> },
    /// A 2xx with no body: the API accepted the call and answers nothing (a
    /// 204, or an endpoint that answers an empty 200). Only `status` is known.
    NoBody { status: u64 },
    /// `api-request`'s `credential_invalid` value: the user has no usable
    /// credential, and the request is answered with this value unchanged so
    /// the caller runs `oauth-connect`.
    Relay(Value),
    /// The answer was cut at the read limit, or not delivered.
    Unreadable(Unreadable),
    /// The API's whole 4xx/5xx answer: its HTTP status and body text, for the
    /// component to map to the request's failure.
    Error { status: u64, body: String },
}

/// A 2xx answer, whole: its status, its response headers and its body's bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Payload {
    pub status: u64,
    /// The response headers as `api-request` returns them: names lower-case,
    /// a repeated header's values joined with `, `.
    pub headers: BTreeMap<String, String>,
    /// The body's bytes, whichever of `body` and `body_base64` carried them;
    /// empty when the API answered no body.
    pub body: Vec<u8>,
}

impl Payload {
    /// The header `name`, matched without regard to ASCII case.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }

    /// The body as text; `None` when its bytes are not UTF-8.
    pub fn text(&self) -> Option<&str> {
        std::str::from_utf8(&self.body).ok()
    }
}

/// What a nested `api-request` call answered, read without interpreting a
/// 2xx body.
#[derive(Clone, Debug, PartialEq)]
pub enum PayloadAnswer {
    /// The API's 2xx answer: status, headers and body bytes.
    Payload(Payload),
    /// `api-request`'s `credential_invalid` value, as [`Answer::Relay`].
    Relay(Value),
    /// The answer was cut at the read limit, or not delivered.
    Unreadable(Unreadable),
    /// The API's whole 4xx/5xx answer: its HTTP status and body text.
    Error { status: u64, body: String },
}

/// A 2xx body as the envelope carried it.
enum SuccessBody {
    Text(String),
    Bytes(Vec<u8>),
}

/// An answer read up to its 2xx body, which each reader interprets its own
/// way.
enum Envelope {
    /// A 2xx: its status, its body as carried, and the envelope it was read
    /// from, a text body taken out of it.
    Success {
        status: u64,
        body: SuccessBody,
        value: Value,
    },
    Relay(Value),
    Unreadable(Unreadable),
    Error {
        status: u64,
        body: String,
    },
}

/// Read `api-request`'s answer up to the 2xx body: the nested error's
/// mapping, the credential relay, the cut answer, the base64 body, the
/// redirect, the 4xx/5xx error and the status that is neither.
fn read_envelope(
    outcome: Result<Value, NestedError>,
    target: &Target,
    api: &str,
) -> Result<Envelope, Failure> {
    let mut value = match outcome {
        Ok(value) => value,
        Err(error) => {
            let NestedError { kind, detail } = error;
            return match kind.as_str() {
                "denied" => Err(Failure {
                    kind: ErrorKind::Denied,
                    detail: format!(
                        "api-request refused the nested {} {}{}: {detail}",
                        target.method, target.host, target.path
                    ),
                }),
                "invalid_args" => Err(internal(format!(
                    "api-request rejected the wrapper's request: {detail}"
                ))),
                "internal" => Ok(Envelope::Unreadable(Unreadable::Refused(detail))),
                other => Err(internal(format!("{other}: {detail}"))),
            };
        }
    };
    if value.get("error").and_then(Value::as_str) == Some("credential_invalid") {
        return Ok(Envelope::Relay(value));
    }
    let status = value
        .get("status")
        .and_then(Value::as_u64)
        .ok_or_else(|| internal("api-request's answer carries no status"))?;
    if value.get("truncated").and_then(Value::as_bool) == Some(true) {
        // On a cut answer `bytes` is the response's Content-Length, not a
        // count of what was read; null when the API sent none.
        return Ok(Envelope::Unreadable(Unreadable::Cut(
            match value.get("bytes").and_then(Value::as_u64) {
                Some(bytes) => format!("{bytes} bytes"),
                None => "of unstated length".to_string(),
            },
        )));
    }
    if let Some(encoded) = value.get("body_base64") {
        let body = encoded
            .as_str()
            .and_then(|encoded| Base64::decode_vec(encoded).ok())
            .ok_or_else(|| {
                internal("api-request's answer carries a body_base64 that is not base64")
            })?;
        if !(200..300).contains(&status) {
            return Err(internal(format!(
                "{api} answered HTTP {status} with a non-text body"
            )));
        }
        return Ok(Envelope::Success {
            status,
            body: SuccessBody::Bytes(body),
            value,
        });
    }
    if (300..400).contains(&status) {
        return Err(internal(format!(
            "{api} answered {status} with a redirect, which Brenn does not follow"
        )));
    }
    let body = match value.get_mut("body").map(Value::take) {
        Some(Value::String(body)) => body,
        _ => return Err(internal("api-request's answer carries no body")),
    };
    if status >= 400 {
        return Ok(Envelope::Error { status, body });
    }
    if !(200..300).contains(&status) {
        return Err(internal(format!(
            "{api} answered HTTP {status}, which is neither a success nor an error"
        )));
    }
    Ok(Envelope::Success {
        status,
        body: SuccessBody::Text(body),
        value,
    })
}

/// `api-request`'s answer read: the API's JSON, its bytes or its empty 2xx,
/// the credential relay, a cut or undelivered answer or the API's error for
/// the component to decide on, or the request's error answer. A 2xx body of
/// zero bytes is [`Answer::NoBody`], a 2xx `body_base64` is [`Answer::Bytes`],
/// and any other 2xx body must be JSON; a non-text body under any other
/// status is internal. `api` is the API's display name, which the refusals the
/// model sees name. An API that answers something other than JSON, or whose
/// response headers the component needs, is read with [`read_api_payload`].
pub fn read_api_result(
    outcome: Result<Value, NestedError>,
    target: &Target,
    api: &str,
) -> Result<Answer, Failure> {
    Ok(match read_envelope(outcome, target, api)? {
        Envelope::Relay(value) => Answer::Relay(value),
        Envelope::Unreadable(why) => Answer::Unreadable(why),
        Envelope::Error { status, body } => Answer::Error { status, body },
        Envelope::Success {
            status,
            body: SuccessBody::Bytes(body),
            ..
        } => Answer::Bytes { status, body },
        Envelope::Success {
            status,
            body: SuccessBody::Text(body),
            ..
        } => {
            if body.is_empty() {
                Answer::NoBody { status }
            } else {
                serde_json::from_str(&body).map(Answer::Body).map_err(|_| {
                    internal(format!(
                        "{api} returned HTTP {status} with a body that is not JSON"
                    ))
                })?
            }
        }
    })
}

// TODO(kit-payload-reader-cannot-read-304): decide whether a 304 gets a case
// of its own.
/// `api-request`'s answer read as [`read_api_result`] reads it, except that a
/// 2xx is never interpreted: it is a [`Payload`] of the status, the response
/// headers and the body's bytes, whatever its content type and whichever
/// field carried it. A 3xx is internal, `304 Not Modified` included, so a
/// conditional `GET` or `HEAD` cannot be read. `api` is the API's display
/// name, which the refusals the model sees name.
pub fn read_api_payload(
    outcome: Result<Value, NestedError>,
    target: &Target,
    api: &str,
) -> Result<PayloadAnswer, Failure> {
    Ok(match read_envelope(outcome, target, api)? {
        Envelope::Relay(value) => PayloadAnswer::Relay(value),
        Envelope::Unreadable(why) => PayloadAnswer::Unreadable(why),
        Envelope::Error { status, body } => PayloadAnswer::Error { status, body },
        Envelope::Success {
            status,
            body,
            value,
        } => PayloadAnswer::Payload(Payload {
            status,
            headers: read_headers(&value)?,
            body: match body {
                SuccessBody::Text(text) => text.into_bytes(),
                SuccessBody::Bytes(bytes) => bytes,
            },
        }),
    })
}

/// The envelope's `headers` object. `api-request` always writes one of
/// string values, so anything else is a host bug.
fn read_headers(value: &Value) -> Result<BTreeMap<String, String>, Failure> {
    let unreadable = || internal("api-request's answer carries no readable headers");
    value
        .get("headers")
        .and_then(Value::as_object)
        .ok_or_else(unreadable)?
        .iter()
        .map(|(name, value)| {
            value
                .as_str()
                .map(|value| (name.clone(), value.to_string()))
                .ok_or_else(unreadable)
        })
        .collect()
}

/// `value`'s length as serialized JSON.
pub fn serialized_len(value: &Value) -> usize {
    serde_json::to_vec(value)
        .expect("a JSON value serializes")
        .len()
}

/// `value` as the request's answer, unless it serializes larger than
/// `ceiling`: then an internal failure, ending with `hint` (what the caller
/// should narrow) when one is given. Nothing is dropped to fit.
pub fn bounded(value: Value, ceiling: usize, hint: Option<&str>) -> Next {
    let size = serialized_len(&value);
    if size > ceiling {
        let detail = format!("compact result of {size} bytes exceeds the wrapper's ceiling");
        return Next::Fail(internal(match hint {
            Some(hint) => format!("{detail}; {hint}"),
            None => detail,
        }));
    }
    Next::Complete(value)
}

/// Copy `from` into `to[name]` when present and not null; absent fields are
/// omitted, never invented.
pub fn copy(to: &mut Args, name: &str, from: Option<&Value>) {
    if let Some(value) = from.filter(|value| !value.is_null()) {
        to.insert(name.to_string(), value.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::google::API_NAME;
    use crate::testutil::{binary, failure, http, nested, payload, truncated};

    fn target() -> Target {
        Target {
            method: "GET".to_string(),
            host: "api.example.com".to_string(),
            path: "/v1/items".to_string(),
        }
    }

    fn read(outcome: Result<Value, NestedError>) -> Result<Answer, Failure> {
        read_api_result(outcome, &target(), API_NAME)
    }

    #[test]
    fn a_missing_credential_is_relayed_unchanged() {
        for account in [Value::Null, json!("alice@example.com")] {
            let value = json!({
                "error": "credential_invalid",
                "provider": "google",
                "account": account,
                "detail": "no account is connected for this provider",
            });
            assert_eq!(read(Ok(value.clone())), Ok(Answer::Relay(value)));
        }
    }

    #[test]
    fn a_denied_nested_call_is_denied_naming_it() {
        assert_eq!(
            read(nested(
                "denied",
                "several google accounts are connected; name one as `account`"
            )),
            Err(failure(
                ErrorKind::Denied,
                "api-request refused the nested GET api.example.com/v1/items: \
                 several google accounts are connected; name one as `account`"
            ))
        );
    }

    #[test]
    fn a_nested_internal_error_is_an_undelivered_answer() {
        assert_eq!(
            read(nested(
                "internal",
                "result exceeds [messaging] max_body_bytes"
            )),
            Ok(Answer::Unreadable(Unreadable::Refused(
                "result exceeds [messaging] max_body_bytes".to_string()
            )))
        );
    }

    #[test]
    fn other_nested_errors_are_internal() {
        assert_eq!(
            read(nested("invalid_args", "bad query")),
            Err(failure(
                ErrorKind::Internal,
                "api-request rejected the wrapper's request: bad query"
            ))
        );
        assert_eq!(
            read(nested("rate_limited", "slow down")),
            Err(failure(ErrorKind::Internal, "rate_limited: slow down"))
        );
    }

    #[test]
    fn a_truncated_answer_states_its_size() {
        assert_eq!(
            read(truncated(200, json!(300000), "{\"lab")),
            Ok(Answer::Unreadable(Unreadable::Cut(
                "300000 bytes".to_string()
            )))
        );
        assert_eq!(
            read(truncated(200, Value::Null, "")),
            Ok(Answer::Unreadable(Unreadable::Cut(
                "of unstated length".to_string()
            )))
        );
        // A cut error answer is cut first: its body is not whole.
        assert_eq!(
            read(truncated(500, json!(9000), "{")),
            Ok(Answer::Unreadable(Unreadable::Cut(
                "9000 bytes".to_string()
            )))
        );
    }

    #[test]
    fn an_unreadable_read_renders_one_sentence() {
        let cut = Unreadable::Cut("300000 bytes".to_string());
        let refused = Unreadable::Refused("result exceeds [messaging] max_body_bytes".to_string());
        assert_eq!(
            cut.render(API_NAME, Some("sizeEstimate 4096")),
            "Google's answer (300000 bytes; sizeEstimate 4096) exceeds api-request's read \
             limit for this provider (max_response_bytes)"
        );
        assert_eq!(
            refused.render(API_NAME, Some("sizeEstimate 4096")),
            "the host did not deliver api-request's answer: result exceeds [messaging] \
             max_body_bytes; sizeEstimate 4096"
        );
        assert_eq!(
            cut.failure(API_NAME, None),
            failure(
                ErrorKind::Internal,
                "Google's answer (300000 bytes) exceeds api-request's read limit for this \
                 provider (max_response_bytes)"
            )
        );
        assert_eq!(
            refused.failure(API_NAME, Some("ask for less")),
            failure(
                ErrorKind::Internal,
                "the host did not deliver api-request's answer: result exceeds [messaging] \
                 max_body_bytes; ask for less"
            )
        );
        // Carried to a re-read step and back.
        for why in [cut, refused] {
            assert_eq!(Unreadable::from_derived(&why.to_derived()), Ok(why));
        }
        for carried in [
            json!(null),
            json!({}),
            json!({ "cut": 7 }),
            json!({ "lost": "x" }),
            json!({ "cut": "x", "refused": "y" }),
        ] {
            let derived = Args::from_iter([(UNREADABLE_KEY.to_string(), carried.clone())]);
            assert!(Unreadable::from_derived(&derived).is_err(), "{carried}");
        }
        assert!(Unreadable::from_derived(&Args::new()).is_err());
    }

    #[test]
    fn a_binary_2xx_body_is_read_as_bytes() {
        assert_eq!(
            read(binary(200, &[0, 1, 2])),
            Ok(Answer::Bytes {
                status: 200,
                body: vec![0, 1, 2],
            })
        );
        assert_eq!(
            read(binary(206, &[0xff])),
            Ok(Answer::Bytes {
                status: 206,
                body: vec![0xff],
            })
        );
    }

    #[test]
    fn a_binary_body_under_another_status_or_a_redirect_is_internal() {
        for status in [404, 302, 500, 101] {
            assert_eq!(
                read(binary(status, &[0, 1, 2])),
                Err(failure(
                    ErrorKind::Internal,
                    &format!("Google answered HTTP {status} with a non-text body")
                ))
            );
        }
        assert_eq!(
            read(http(302, "")),
            Err(failure(
                ErrorKind::Internal,
                "Google answered 302 with a redirect, which Brenn does not follow"
            ))
        );
    }

    #[test]
    fn a_body_base64_that_is_not_base64_is_internal() {
        for encoded in [json!("not base64!"), json!("AAE"), json!(7)] {
            let outcome = Ok(json!({
                "status": 200, "headers": {}, "bytes": 3, "truncated": false, "body_base64": encoded,
            }));
            assert_eq!(
                read(outcome),
                Err(failure(
                    ErrorKind::Internal,
                    "api-request's answer carries a body_base64 that is not base64"
                ))
            );
        }
    }

    #[test]
    fn a_truncated_binary_answer_is_cut() {
        let outcome = Ok(json!({
            "status": 200, "headers": {}, "bytes": 900000, "truncated": true, "body_base64": "AAEC",
        }));
        assert_eq!(
            read(outcome),
            Ok(Answer::Unreadable(Unreadable::Cut(
                "900000 bytes".to_string()
            )))
        );
    }

    #[test]
    fn the_refusals_name_the_api_they_are_given() {
        let read = |outcome| read_api_result(outcome, &target(), "Fastmail");
        for (outcome, detail) in [
            (
                binary(404, &[0, 1, 2]),
                "Fastmail answered HTTP 404 with a non-text body",
            ),
            (
                http(302, ""),
                "Fastmail answered 302 with a redirect, which Brenn does not follow",
            ),
            (
                http(101, ""),
                "Fastmail answered HTTP 101, which is neither a success nor an error",
            ),
            (
                http(200, "not json"),
                "Fastmail returned HTTP 200 with a body that is not JSON",
            ),
        ] {
            assert_eq!(read(outcome), Err(failure(ErrorKind::Internal, detail)));
        }
        let cut = Unreadable::Cut("300000 bytes".to_string());
        assert_eq!(
            cut.render("Fastmail", None),
            "Fastmail's answer (300000 bytes) exceeds api-request's read limit for this \
             provider (max_response_bytes)"
        );
        assert_eq!(
            cut.failure("Fastmail", Some("ask for less")),
            failure(
                ErrorKind::Internal,
                "Fastmail's answer (300000 bytes) exceeds api-request's read limit for this \
                 provider (max_response_bytes); ask for less"
            )
        );
    }

    #[test]
    fn a_success_body_must_be_json() {
        assert_eq!(
            read(http(200, "not json")),
            Err(failure(
                ErrorKind::Internal,
                "Google returned HTTP 200 with a body that is not JSON"
            ))
        );
        assert_eq!(
            read(http(200, "{\"a\": 1}")),
            Ok(Answer::Body(json!({ "a": 1 })))
        );
    }

    #[test]
    fn an_empty_2xx_answer_is_accepted_with_nothing_to_read() {
        for status in [200, 204] {
            assert_eq!(read(http(status, "")), Ok(Answer::NoBody { status }));
        }
        // Whitespace is a body, and not JSON.
        assert_eq!(
            read(http(200, " ")),
            Err(failure(
                ErrorKind::Internal,
                "Google returned HTTP 200 with a body that is not JSON"
            ))
        );
        assert_eq!(
            read(http(204, "{\"a\": 1}")),
            Ok(Answer::Body(json!({ "a": 1 })))
        );
    }

    #[test]
    fn an_answer_without_status_or_body_is_internal() {
        assert_eq!(
            read(Ok(json!({ "truncated": false }))),
            Err(failure(
                ErrorKind::Internal,
                "api-request's answer carries no status"
            ))
        );
        assert_eq!(
            read(Ok(json!({ "status": 200, "truncated": false }))),
            Err(failure(
                ErrorKind::Internal,
                "api-request's answer carries no body"
            ))
        );
        assert!(read(http(101, "")).unwrap_err().detail.contains("HTTP 101"));
    }

    fn read_payload(outcome: Result<Value, NestedError>) -> Result<PayloadAnswer, Failure> {
        read_api_payload(outcome, &target(), API_NAME)
    }

    fn headers(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect()
    }

    const MULTISTATUS: &str = "<?xml version=\"1.0\"?>\r\n<d:multistatus xmlns:d=\"DAV:\">\
                               </d:multistatus>\r\n";

    #[test]
    fn a_multistatus_answer_is_read_with_its_headers() {
        let answer = read_payload(payload(
            207,
            &[
                ("Content-Type", "application/xml; charset=utf-8"),
                ("ETag", "\"e1\""),
            ],
            MULTISTATUS.as_bytes(),
        ));
        let Ok(PayloadAnswer::Payload(read)) = answer else {
            panic!("expected a payload, got {answer:?}");
        };
        assert_eq!(
            read,
            Payload {
                status: 207,
                headers: headers(&[
                    ("content-type", "application/xml; charset=utf-8"),
                    ("etag", "\"e1\""),
                ]),
                body: MULTISTATUS.as_bytes().to_vec(),
            }
        );
        assert_eq!(read.text(), Some(MULTISTATUS));
        assert_eq!(read.header("ETag"), Some("\"e1\""));
        assert_eq!(read.header("etag"), Some("\"e1\""));
        assert_eq!(read.header("location"), None);
    }

    #[test]
    fn a_download_reads_to_its_bytes_whichever_field_carried_it() {
        let text = "BEGIN:VCALENDAR\r\nEND:VCALENDAR\r\n".as_bytes();
        let binary_body: &[u8] = &[0xff, 0, 0xfe];
        for (body, as_text) in [(text, Some(text)), (binary_body, None)] {
            let answer = read_payload(payload(200, &[("Content-Type", "x/y")], body));
            let Ok(PayloadAnswer::Payload(read)) = answer else {
                panic!("expected a payload, got {answer:?}");
            };
            assert_eq!(read.status, 200);
            assert_eq!(read.body, body);
            assert_eq!(read.headers, headers(&[("content-type", "x/y")]));
            assert_eq!(read.text().map(str::as_bytes), as_text);
        }
    }

    #[test]
    fn a_bodyless_2xx_is_a_payload_with_its_headers() {
        assert_eq!(
            read_payload(payload(204, &[("ETag", "\"e2\"")], b"")),
            Ok(PayloadAnswer::Payload(Payload {
                status: 204,
                headers: headers(&[("etag", "\"e2\"")]),
                body: Vec::new(),
            }))
        );
    }

    /// Everything before the 2xx body reads as it does through
    /// [`read_api_result`], with the same texts.
    #[test]
    fn the_payload_reader_shares_the_envelope_handling() {
        let relay = json!({
            "error": "credential_invalid", "provider": "google", "account": null, "detail": "x",
        });
        assert_eq!(
            read_payload(Ok(relay.clone())),
            Ok(PayloadAnswer::Relay(relay))
        );
        assert_eq!(
            read_payload(nested("denied", "no clause")),
            Err(failure(
                ErrorKind::Denied,
                "api-request refused the nested GET api.example.com/v1/items: no clause"
            ))
        );
        assert_eq!(
            read_payload(nested("invalid_args", "bad query")),
            Err(failure(
                ErrorKind::Internal,
                "api-request rejected the wrapper's request: bad query"
            ))
        );
        assert_eq!(
            read_payload(nested("internal", "result exceeds the cap")),
            Ok(PayloadAnswer::Unreadable(Unreadable::Refused(
                "result exceeds the cap".to_string()
            )))
        );
        assert_eq!(
            read_payload(truncated(207, json!(300000), "<d:")),
            Ok(PayloadAnswer::Unreadable(Unreadable::Cut(
                "300000 bytes".to_string()
            )))
        );
        assert_eq!(
            read_payload(http(412, "precondition failed")),
            Ok(PayloadAnswer::Error {
                status: 412,
                body: "precondition failed".to_string(),
            })
        );
        assert_eq!(
            read_payload(http(302, "")),
            Err(failure(
                ErrorKind::Internal,
                "Google answered 302 with a redirect, which Brenn does not follow"
            ))
        );
        assert_eq!(
            read_payload(payload(304, &[("etag", "\"a1\"")], b"")),
            Err(failure(
                ErrorKind::Internal,
                "Google answered 304 with a redirect, which Brenn does not follow"
            ))
        );
        assert_eq!(
            read_payload(binary(404, &[0, 1])),
            Err(failure(
                ErrorKind::Internal,
                "Google answered HTTP 404 with a non-text body"
            ))
        );
        assert_eq!(
            read_payload(Ok(json!({
                "status": 200, "headers": {}, "bytes": 3, "truncated": false,
                "body_base64": "not base64!",
            }))),
            Err(failure(
                ErrorKind::Internal,
                "api-request's answer carries a body_base64 that is not base64"
            ))
        );
        assert_eq!(
            read_payload(Ok(json!({ "truncated": false }))),
            Err(failure(
                ErrorKind::Internal,
                "api-request's answer carries no status"
            ))
        );
    }

    #[test]
    fn a_payload_needs_readable_headers() {
        for envelope in [
            json!({ "status": 200, "bytes": 2, "truncated": false, "body": "{}" }),
            json!({ "status": 200, "headers": [], "bytes": 2, "truncated": false, "body": "{}" }),
            json!({
                "status": 200, "headers": { "etag": 7 }, "bytes": 2, "truncated": false,
                "body": "{}",
            }),
        ] {
            assert_eq!(
                read_payload(Ok(envelope.clone())),
                Err(failure(
                    ErrorKind::Internal,
                    "api-request's answer carries no readable headers"
                )),
                "{envelope}"
            );
            assert_eq!(
                read(Ok(envelope.clone())),
                Ok(Answer::Body(json!({}))),
                "{envelope}"
            );
        }
    }

    #[test]
    fn the_json_reader_still_refuses_a_payload() {
        assert_eq!(
            read(payload(
                207,
                &[("Content-Type", "application/xml")],
                MULTISTATUS.as_bytes()
            )),
            Err(failure(
                ErrorKind::Internal,
                "Google returned HTTP 207 with a body that is not JSON"
            ))
        );
    }

    #[test]
    fn the_payload_fixture_carries_bytes_as_the_host_does() {
        let text = payload(200, &[("ETag", "\"e\""), ("X-Thing", "1")], b"abc").unwrap();
        assert_eq!(text["body"], "abc");
        assert!(text.get("body_base64").is_none(), "{text}");
        assert_eq!(text["bytes"], 3);
        assert_eq!(text["headers"], json!({ "etag": "\"e\"", "x-thing": "1" }));
        let bytes = payload(200, &[], &[0xff, 0]).unwrap();
        assert_eq!(bytes["body_base64"], Base64::encode_string(&[0xff, 0]));
        assert!(bytes.get("body").is_none(), "{bytes}");
        assert_eq!(bytes["bytes"], 2);
    }

    #[test]
    fn the_summary_names_status_size_and_truncation_only() {
        assert_eq!(
            result_summary(&http(200, "{\"secret\": 1}")),
            "status 200 bytes 13 truncated false"
        );
        assert_eq!(result_summary(&nested("denied", "secret")), "error denied");
    }

    #[test]
    fn not_issued_carries_brenns_text_and_the_guidance() {
        let detail = not_issued("call-async api-request: denied: no clause admits GET");
        assert!(detail.starts_with(
            "api-request could not be issued: call-async api-request: denied: no clause admits GET."
        ));
        assert!(detail.ends_with("must admit the method, host and path."));
    }

    #[test]
    fn cut_counts_bytes_and_says_whether_it_cut() {
        assert_eq!(cut("abc", 3), ("abc".to_string(), false));
        assert_eq!(cut("abcd", 3), ("abc…".to_string(), true));
        // `é` is two bytes: at a bound of three, the second one straddles it.
        assert_eq!(cut("éé", 3), ("é…".to_string(), true));
        assert_eq!(cut("😀", 3), ("…".to_string(), true));
    }

    #[test]
    fn the_result_ceiling_reports_rather_than_sends() {
        let value = json!({ "text": "x".repeat(100) });
        assert_eq!(
            bounded(value.clone(), 1_000, None),
            Next::Complete(value.clone())
        );
        let size = serde_json::to_vec(&value).unwrap().len();
        assert_eq!(serialized_len(&value), size);
        assert_eq!(
            bounded(value.clone(), 50, None),
            Next::Fail(failure(
                ErrorKind::Internal,
                &format!("compact result of {size} bytes exceeds the wrapper's ceiling")
            ))
        );
        assert_eq!(
            bounded(value, 50, Some("ask for less")),
            Next::Fail(failure(
                ErrorKind::Internal,
                &format!(
                    "compact result of {size} bytes exceeds the wrapper's ceiling; ask for less"
                )
            ))
        );
    }

    #[test]
    fn copy_omits_an_absent_or_null_field() {
        let mut out = Args::new();
        copy(&mut out, "a", Some(&json!(1)));
        copy(&mut out, "b", Some(&Value::Null));
        copy(&mut out, "c", None);
        assert_eq!(Value::Object(out), json!({ "a": 1 }));
    }
}
