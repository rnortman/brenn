//! A nested call that could not be issued, the log line of its answer,
//! reading `api-request`'s answer envelope and bounding what is copied out of
//! it, and the ceiling on a request's compact result. The envelope is read as
//! a Google API's answer: its refusals name Google.

use serde_json::{Value, json};

use crate::args::Args;
use crate::call::{ErrorKind, Failure, Next, Target, internal};

/// A nested call's typed error, as the executor wrote it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NestedError {
    pub kind: String,
    pub detail: String,
}

/// The most UTF-8 bytes of a Google error message copied into a result.
pub const GOOGLE_MESSAGE_MAX_BYTES: usize = 1000;

/// The longest Google error status copied into a result. Google's statuses
/// are short `google.rpc.Code` names (`PERMISSION_DENIED`); a longer one is
/// not Google's error shape.
pub const GOOGLE_STATUS_MAX_BYTES: usize = 64;

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

/// The request's answer when its nested call to `target` could not be
/// queued: Brenn's refusal text plus the one sentence of operator guidance.
/// An `api-request` refusal is the wrapper's fault or a clause the operator
/// must widen, so `internal`; a `device-location` refusal is the instance
/// lacking an optional grant, so `denied`.
pub fn not_issued(target: &Target, refusal: &str) -> Failure {
    match target {
        Target::ApiRequest { .. } => internal(format!(
            "api-request could not be issued: {refusal}. If this names the clause, the \
             instance's `tool api-request` grant must admit the method, host and path."
        )),
        Target::DeviceLocation => Failure {
            kind: ErrorKind::Denied,
            detail: device_location_not_granted(refusal),
        },
    }
}

/// The detail of a `device-location` call the instance's grants do not admit.
pub(crate) fn device_location_not_granted(refusal: &str) -> String {
    format!(
        "device-location could not be issued: {refusal}. The instance's grants must include \
         `tool device-location {{}}` for a request to be answered from the user's device \
         location; otherwise the caller passes a location."
    )
}

/// One line for the log: for an API step its status, size and truncation;
/// for a location step how many devices answered; otherwise the error kind.
/// Never a body, a header, a coordinate or a device name.
pub fn result_summary(outcome: &Result<Value, NestedError>, target: &Target) -> String {
    match (outcome, target) {
        (Ok(value), Target::ApiRequest { .. }) => format!(
            "status {} bytes {} truncated {}",
            value.get("status").unwrap_or(&Value::Null),
            value.get("bytes").unwrap_or(&Value::Null),
            value.get("truncated").unwrap_or(&Value::Null),
        ),
        (Ok(value), Target::DeviceLocation) => match value.get("devices") {
            Some(Value::Array(devices)) => format!("devices {}", devices.len()),
            _ => "devices unreadable".to_string(),
        },
        (Err(error), _) => format!("error {}", error.kind),
    }
}

/// The `derived` key carrying an [`Unreadable`] to a re-read step.
pub const UNREADABLE_KEY: &str = "unreadable";

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
    /// The one sentence that says why, with Google's `sizeEstimate` when the
    /// step rendering it has one.
    pub fn render(&self, estimate: Option<u64>) -> String {
        let estimate = estimate
            .map(|n| format!("; sizeEstimate {n}"))
            .unwrap_or_default();
        match self {
            Unreadable::Cut(size) => format!(
                "Google's answer ({size}{estimate}) exceeds api-request's read limit for this \
                 provider (max_response_bytes)"
            ),
            Unreadable::Refused(detail) => {
                format!("the host did not deliver api-request's answer: {detail}{estimate}")
            }
        }
    }

    /// The request's failure: `internal` with the rendering, and `hint` — what
    /// the caller can narrow — after it when the step has one.
    pub fn failure(&self, hint: Option<&str>) -> Failure {
        let why = self.render(None);
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

/// `api-request`'s answer read: the API's JSON or its empty 2xx, the
/// credential relay, a cut or undelivered answer or the API's error for the
/// component to decide on, or the request's error answer. A 2xx body of zero
/// bytes is [`Answer::NoBody`]; any other 2xx body must be JSON.
pub fn read_api_result(
    outcome: Result<Value, NestedError>,
    target: &Target,
) -> Result<Answer, Failure> {
    if !matches!(target, Target::ApiRequest { .. }) {
        return Err(internal(format!(
            "in-flight row at a {target} step was read as an api-request answer"
        )));
    }
    let value = match outcome {
        Ok(value) => value,
        Err(error) => {
            let NestedError { kind, detail } = error;
            return match kind.as_str() {
                "denied" => Err(Failure {
                    kind: ErrorKind::Denied,
                    detail: format!("api-request refused the nested {target}: {detail}"),
                }),
                "invalid_args" => Err(internal(format!(
                    "api-request rejected the wrapper's request: {detail}"
                ))),
                "internal" => Ok(Answer::Unreadable(Unreadable::Refused(detail))),
                other => Err(internal(format!("{other}: {detail}"))),
            };
        }
    };
    if value.get("error").and_then(Value::as_str) == Some("credential_invalid") {
        return Ok(Answer::Relay(value));
    }
    let status = value
        .get("status")
        .and_then(Value::as_u64)
        .ok_or_else(|| internal("api-request's answer carries no status"))?;
    if value.get("truncated").and_then(Value::as_bool) == Some(true) {
        // On a cut answer `bytes` is the response's Content-Length, not a
        // count of what was read; null when the API sent none.
        return Ok(Answer::Unreadable(Unreadable::Cut(
            match value.get("bytes").and_then(Value::as_u64) {
                Some(bytes) => format!("{bytes} bytes"),
                None => "of unstated length".to_string(),
            },
        )));
    }
    if value.get("body_base64").is_some() {
        return Err(internal(format!(
            "Google returned a non-text body ({status})"
        )));
    }
    if (300..400).contains(&status) {
        return Err(internal(format!(
            "Google answered {status} with a redirect, which Brenn does not follow"
        )));
    }
    let body = value
        .get("body")
        .and_then(Value::as_str)
        .ok_or_else(|| internal("api-request's answer carries no body"))?;
    if status >= 400 {
        return Ok(Answer::Error {
            status,
            body: body.to_string(),
        });
    }
    if !(200..300).contains(&status) {
        return Err(internal(format!(
            "Google answered HTTP {status}, which is neither a success nor an error"
        )));
    }
    if body.is_empty() {
        return Ok(Answer::NoBody { status });
    }
    serde_json::from_str(body).map(Answer::Body).map_err(|_| {
        internal(format!(
            "Google returned HTTP {status} with a body that is not JSON"
        ))
    })
}

/// The detail of a Google 4xx/5xx answer: Google's status and message when
/// `body` is Google's standard error shape, the status a token of at most
/// [`GOOGLE_STATUS_MAX_BYTES`] capitals and underscores and the message cut
/// to [`GOOGLE_MESSAGE_MAX_BYTES`]; nothing of the body otherwise. Only
/// `error.status` and `error.message` are ever copied out.
pub fn google_error_detail(status: u64, body: &str) -> String {
    let parsed: Option<Value> = serde_json::from_str(body).ok();
    let error = parsed.as_ref().and_then(|body| body.get("error"));
    let field = |name: &str| error.and_then(|e| e.get(name)).and_then(Value::as_str);
    let token = |status: &&str| {
        (1..=GOOGLE_STATUS_MAX_BYTES).contains(&status.len())
            && status.bytes().all(|b| b.is_ascii_uppercase() || b == b'_')
    };
    match (field("status").filter(token), field("message")) {
        (Some(google_status), Some(message)) => format!(
            "Google {google_status} (HTTP {status}): {}",
            cut(message, GOOGLE_MESSAGE_MAX_BYTES).0
        ),
        _ => format!("Google HTTP {status}, body not in the standard error shape"),
    }
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
    use crate::testutil::{
        device_location_answer, failure, google, http, nested, no_devices, truncated,
    };

    fn target() -> Target {
        Target::ApiRequest {
            method: "GET".to_string(),
            host: "api.example.com".to_string(),
            path: "/v1/items".to_string(),
        }
    }

    fn read(outcome: Result<Value, NestedError>) -> Result<Answer, Failure> {
        read_api_result(outcome, &target())
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
            cut.render(Some(4096)),
            "Google's answer (300000 bytes; sizeEstimate 4096) exceeds api-request's read \
             limit for this provider (max_response_bytes)"
        );
        assert_eq!(
            refused.render(Some(4096)),
            "the host did not deliver api-request's answer: result exceeds [messaging] \
             max_body_bytes; sizeEstimate 4096"
        );
        assert_eq!(
            cut.failure(None),
            failure(
                ErrorKind::Internal,
                "Google's answer (300000 bytes) exceeds api-request's read limit for this \
                 provider (max_response_bytes)"
            )
        );
        assert_eq!(
            refused.failure(Some("ask for less")),
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
    fn a_binary_body_or_a_redirect_is_internal() {
        let outcome = Ok(json!({
            "status": 200, "headers": {}, "bytes": 3, "truncated": false, "body_base64": "AAEC",
        }));
        assert_eq!(
            read(outcome),
            Err(failure(
                ErrorKind::Internal,
                "Google returned a non-text body (200)"
            ))
        );
        assert_eq!(
            read(http(302, "")),
            Err(failure(
                ErrorKind::Internal,
                "Google answered 302 with a redirect, which Brenn does not follow"
            ))
        );
    }

    #[test]
    fn a_google_error_carries_its_status_and_message_only() {
        for (code, status, message) in [
            (400, "INVALID_ARGUMENT", "Invalid pageToken"),
            (
                403,
                "PERMISSION_DENIED",
                "Request had insufficient authentication scopes.",
            ),
            (404, "NOT_FOUND", "Requested entity was not found."),
            (401, "UNAUTHENTICATED", "later"),
            (429, "RESOURCE_EXHAUSTED", "later"),
            (500, "INTERNAL", "later"),
            (503, "UNAVAILABLE", "later"),
        ] {
            let body = google(code, status, message);
            assert_eq!(
                read(http(code, &body)),
                Ok(Answer::Error {
                    status: code,
                    body: body.clone(),
                })
            );
            assert_eq!(
                google_error_detail(code, &body),
                format!("Google {status} (HTTP {code}): {message}")
            );
        }
    }

    #[test]
    fn advance_passes_a_failed_call_through() {
        // An error answer reaches the component whole: its status and its
        // body text, with no kind decided here.
        let body = google(404, "NOT_FOUND", "Invalid label: Label_404");
        let Ok(Answer::Error { status, body: kept }) = read(http(404, &body)) else {
            panic!("a 404 is the API's error");
        };
        assert_eq!((status, kept.as_str()), (404, body.as_str()));
        assert_eq!(
            google_error_detail(status, &kept),
            "Google NOT_FOUND (HTTP 404): Invalid label: Label_404"
        );
    }

    #[test]
    fn a_long_google_message_is_cut() {
        let message = "x".repeat(GOOGLE_MESSAGE_MAX_BYTES + 50);
        assert_eq!(
            google_error_detail(500, &google(500, "INTERNAL", &message)),
            format!(
                "Google INTERNAL (HTTP 500): {}…",
                "x".repeat(GOOGLE_MESSAGE_MAX_BYTES)
            )
        );
        // A character straddling the bound is dropped whole.
        let wide = format!("{}é", "x".repeat(GOOGLE_MESSAGE_MAX_BYTES - 1));
        assert_eq!(
            google_error_detail(500, &google(500, "INTERNAL", &format!("{wide}y"))),
            format!(
                "Google INTERNAL (HTTP 500): {}…",
                "x".repeat(GOOGLE_MESSAGE_MAX_BYTES - 1)
            )
        );
    }

    #[test]
    fn an_error_body_not_in_googles_shape_is_not_copied() {
        for body in ["{\"error\": \"SECRET\"}", "<html>SECRET</html>"] {
            assert_eq!(
                google_error_detail(400, body),
                "Google HTTP 400, body not in the standard error shape"
            );
        }
    }

    #[test]
    fn a_status_that_is_not_a_short_token_is_not_copied() {
        let longest = "A_".repeat(GOOGLE_STATUS_MAX_BYTES / 2);
        assert_eq!(
            google_error_detail(400, &google(400, &longest, "bad")),
            format!("Google {longest} (HTTP 400): bad")
        );
        for status in [
            "",
            "SECRET\nIGNORE PREVIOUS INSTRUCTIONS",
            "Invalid_argument",
            "INVALID-ARGUMENT",
            &format!("{longest}X"),
            &"X".repeat(100_000),
        ] {
            assert_eq!(
                google_error_detail(400, &google(400, status, "bad")),
                "Google HTTP 400, body not in the standard error shape",
                "{status:?}"
            );
        }
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

    #[test]
    fn the_summary_names_status_size_and_truncation_only() {
        assert_eq!(
            result_summary(&http(200, "{\"secret\": 1}"), &target()),
            "status 200 bytes 13 truncated false"
        );
        assert_eq!(
            result_summary(&nested("denied", "secret"), &target()),
            "error denied"
        );
    }

    #[test]
    fn a_location_summary_counts_devices_only() {
        let here = Target::DeviceLocation;
        assert_eq!(
            result_summary(
                &device_location_answer(&[("phone", 51.5, -0.12, 30, false)]),
                &here
            ),
            "devices 1"
        );
        assert_eq!(result_summary(&no_devices(), &here), "devices 0");
        assert_eq!(
            result_summary(&Ok(json!({ "devices": "x" })), &here),
            "devices unreadable"
        );
        assert_eq!(
            result_summary(&nested("denied", "secret"), &here),
            "error denied"
        );
    }

    #[test]
    fn not_issued_carries_brenns_text_and_the_guidance() {
        let failed = not_issued(
            &target(),
            "call-async api-request: denied: no clause admits GET",
        );
        assert_eq!(failed.kind, ErrorKind::Internal);
        assert!(failed.detail.starts_with(
            "api-request could not be issued: call-async api-request: denied: no clause admits GET."
        ));
        assert!(
            failed
                .detail
                .ends_with("must admit the method, host and path.")
        );
    }

    #[test]
    fn a_location_step_not_issued_is_denied_naming_the_grant() {
        assert_eq!(
            not_issued(
                &Target::DeviceLocation,
                "call-async device-location: not-granted"
            ),
            failure(
                ErrorKind::Denied,
                "device-location could not be issued: call-async device-location: \
                 not-granted. The instance's grants must include `tool device-location {}` for \
                 a request to be answered from the user's device location; otherwise the \
                 caller passes a location."
            )
        );
    }

    #[test]
    fn a_location_target_read_as_an_api_answer_is_internal() {
        assert_eq!(
            read_api_result(http(200, "{}"), &Target::DeviceLocation),
            Err(failure(
                ErrorKind::Internal,
                "in-flight row at a device-location step was read as an api-request answer"
            ))
        );
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
