//! Google APIs' answers: the display name the neutral answer reading takes,
//! and the detail of Google's standard error shape. The one API-specific
//! module in the kit, here because two in-tree components share Google's
//! error shape; a provider for another API writes its own `error_detail`
//! beside its logic.

use serde_json::Value;

use crate::answer::cut;

/// The display name [`crate::read_api_result`] and [`crate::Unreadable`]
/// take for a Google API.
pub const API_NAME: &str = "Google";

/// The most UTF-8 bytes of a Google error message copied into a result.
pub const MESSAGE_MAX_BYTES: usize = 1000;

/// The longest Google error status copied into a result. Google's statuses
/// are short `google.rpc.Code` names (`PERMISSION_DENIED`); a longer one is
/// not Google's error shape.
pub const STATUS_MAX_BYTES: usize = 64;

/// The detail of a Google 4xx/5xx answer: Google's status and message when
/// `body` is Google's standard error shape, the status a token of at most
/// [`STATUS_MAX_BYTES`] capitals and underscores and the message cut
/// to [`MESSAGE_MAX_BYTES`]; nothing of the body otherwise. Only
/// `error.status` and `error.message` are ever copied out.
pub fn error_detail(status: u64, body: &str) -> String {
    let parsed: Option<Value> = serde_json::from_str(body).ok();
    let error = parsed.as_ref().and_then(|body| body.get("error"));
    let field = |name: &str| error.and_then(|e| e.get(name)).and_then(Value::as_str);
    let token = |status: &&str| {
        (1..=STATUS_MAX_BYTES).contains(&status.len())
            && status.bytes().all(|b| b.is_ascii_uppercase() || b == b'_')
    };
    match (field("status").filter(token), field("message")) {
        (Some(google_status), Some(message)) => format!(
            "Google {google_status} (HTTP {status}): {}",
            cut(message, MESSAGE_MAX_BYTES).0
        ),
        _ => format!("Google HTTP {status}, body not in the standard error shape"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::answer::{Answer, NestedError, read_api_result};
    use crate::call::{Failure, Target};
    use crate::testutil::{google, http};

    fn read(outcome: Result<Value, NestedError>) -> Result<Answer, Failure> {
        let target = Target {
            method: "GET".to_string(),
            host: "api.example.com".to_string(),
            path: "/v1/items".to_string(),
        };
        read_api_result(outcome, &target, API_NAME)
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
                error_detail(code, &body),
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
            error_detail(status, &kept),
            "Google NOT_FOUND (HTTP 404): Invalid label: Label_404"
        );
    }

    #[test]
    fn a_long_google_message_is_cut() {
        let message = "x".repeat(MESSAGE_MAX_BYTES + 50);
        assert_eq!(
            error_detail(500, &google(500, "INTERNAL", &message)),
            format!(
                "Google INTERNAL (HTTP 500): {}…",
                "x".repeat(MESSAGE_MAX_BYTES)
            )
        );
        // A character straddling the bound is dropped whole.
        let wide = format!("{}é", "x".repeat(MESSAGE_MAX_BYTES - 1));
        assert_eq!(
            error_detail(500, &google(500, "INTERNAL", &format!("{wide}y"))),
            format!(
                "Google INTERNAL (HTTP 500): {}…",
                "x".repeat(MESSAGE_MAX_BYTES - 1)
            )
        );
    }

    #[test]
    fn an_error_body_not_in_googles_shape_is_not_copied() {
        for body in ["{\"error\": \"SECRET\"}", "<html>SECRET</html>"] {
            assert_eq!(
                error_detail(400, body),
                "Google HTTP 400, body not in the standard error shape"
            );
        }
    }

    #[test]
    fn a_status_that_is_not_a_short_token_is_not_copied() {
        let longest = "A_".repeat(STATUS_MAX_BYTES / 2);
        assert_eq!(
            error_detail(400, &google(400, &longest, "bad")),
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
                error_detail(400, &google(400, status, "bad")),
                "Google HTTP 400, body not in the standard error shape",
                "{status:?}"
            );
        }
    }
}
