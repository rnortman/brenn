//! The Google scope a call needs, and Google's 4xx/5xx answers mapped to the
//! kit's error kinds with the hint that names it.

use brenn_provider_kit::{ErrorKind, Failure, google, internal};

/// The Google scope a step's call needs, named in a 403's hint.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
// TODO(gmail-filters-tier-3): Gmail filter tools would add a scope here,
// `gmail.settings.basic`.
pub enum Scope {
    GmailRead,
    GmailCompose,
    GmailOrganize,
    CalendarRead,
    CalendarWrite,
}

impl Scope {
    /// How the hint names the scope: the API and the scope set's own word,
    /// as the operator guide names the scope sets.
    pub fn name(self) -> &'static str {
        match self {
            Scope::GmailRead => "Gmail read",
            Scope::GmailCompose => "Gmail compose",
            Scope::GmailOrganize => "Gmail organize",
            Scope::CalendarRead => "Calendar read",
            Scope::CalendarWrite => "Calendar events",
        }
    }
}

/// The sentence every Google 403 is answered with: Google answers an account
/// connected without the step's scope this way, and the component does not
/// read Google's reason codes to tell that refusal from another.
pub fn scope_hint(scope: Scope) -> String {
    format!(
        "If Google names insufficient authentication scopes, the account was connected without \
         the {} scope: run oauth-connect with that scope set and relay its steps.",
        scope.name()
    )
}

/// A 4xx/5xx answer, its detail as `google::error_detail` writes it. 400 and
/// 404 are the caller's arguments; 403 is Google's refusal, answered with the
/// hint naming the step's scope; every other status (401 after
/// `api-request`'s retry, 429, 5xx) is the wrapper's or Google's.
// TODO(errhandling-gmail-403-always-denied-with-scope-hint): a quota or
// API-not-enabled 403 is answered `denied` with the scope hint.
// TODO(errhandling-gmail-4xx-on-fixed-request-blamed-on-caller): a 400/404 on
// a request built from constants alone is answered `invalid_args`.
pub fn google_error(status: u64, body: &str, scope: Scope) -> Failure {
    let kind = match status {
        400 | 404 => ErrorKind::InvalidArgs,
        403 => ErrorKind::Denied,
        _ => ErrorKind::Internal,
    };
    let mut detail = google::error_detail(status, body);
    if kind == ErrorKind::Denied {
        let stop = if detail.ends_with('.') { "" } else { "." };
        detail = format!("{detail}{stop} {}", scope_hint(scope));
    }
    Failure { kind, detail }
}

/// `failure` as a step answers it: at a write step (`hint` is `Some`), an
/// `internal` failure gets the hint appended, since Google may have acted
/// before answering. Every other failure, and every failure at a read step
/// (`None`), passes through unchanged.
pub fn with_write_hint(hint: Option<&str>, failure: Failure) -> Failure {
    match hint {
        Some(hint) if failure.kind == ErrorKind::Internal => {
            internal(format!("{}; {hint}", failure.detail))
        }
        _ => failure,
    }
}

/// A 2xx with no body at a step that reads one: Google's fault, refused
/// `internal`, naming the API whose answer the step expected and what that
/// API answers.
pub fn no_body_refused(status: u64, api: &str, answers: &str) -> Failure {
    internal(format!(
        "Google answered HTTP {status} with no body where {api} answers {answers}"
    ))
}

#[cfg(test)]
mod tests {
    use brenn_provider_kit::testutil::{failure, google};

    use super::*;

    #[test]
    fn a_google_error_maps_its_status_to_a_kind() {
        for (code, status, kind) in [
            (400, "INVALID_ARGUMENT", ErrorKind::InvalidArgs),
            (404, "NOT_FOUND", ErrorKind::InvalidArgs),
            (401, "UNAUTHENTICATED", ErrorKind::Internal),
            (429, "RESOURCE_EXHAUSTED", ErrorKind::Internal),
            (500, "INTERNAL", ErrorKind::Internal),
            (503, "UNAVAILABLE", ErrorKind::Internal),
        ] {
            assert_eq!(
                google_error(code, &google(code, status, "later"), Scope::GmailRead),
                failure(kind, &format!("Google {status} (HTTP {code}): later")),
                "{code}"
            );
        }
    }

    #[test]
    fn a_google_403_is_denied_with_the_scope_hint() {
        assert_eq!(
            google_error(
                403,
                &google(
                    403,
                    "PERMISSION_DENIED",
                    "Request had insufficient authentication scopes."
                ),
                Scope::GmailRead
            ),
            failure(
                ErrorKind::Denied,
                "Google PERMISSION_DENIED (HTTP 403): Request had insufficient authentication \
                 scopes. If Google names insufficient authentication scopes, the account was \
                 connected without the Gmail read scope: run oauth-connect with that scope set \
                 and relay its steps."
            )
        );
        assert_eq!(
            google_error(403, "<html>SECRET</html>", Scope::GmailRead),
            failure(
                ErrorKind::Denied,
                &format!(
                    "Google HTTP 403, body not in the standard error shape. {}",
                    scope_hint(Scope::GmailRead)
                )
            )
        );
    }

    #[test]
    fn every_scopes_hint_names_its_scope_set() {
        for (scope, name) in [
            (Scope::GmailRead, "Gmail read"),
            (Scope::GmailCompose, "Gmail compose"),
            (Scope::GmailOrganize, "Gmail organize"),
            (Scope::CalendarRead, "Calendar read"),
            (Scope::CalendarWrite, "Calendar events"),
        ] {
            assert_eq!(
                scope_hint(scope),
                format!(
                    "If Google names insufficient authentication scopes, the account was \
                     connected without the {name} scope: run oauth-connect with that scope \
                     set and relay its steps."
                ),
                "{name}"
            );
        }
    }

    #[test]
    fn only_an_internal_failure_at_a_write_step_carries_the_hint() {
        let hint = "look for the change before asking again";
        assert_eq!(
            with_write_hint(Some(hint), failure(ErrorKind::Internal, "Google HTTP 500")),
            failure(
                ErrorKind::Internal,
                "Google HTTP 500; look for the change before asking again"
            )
        );
        for kind in [ErrorKind::Denied, ErrorKind::InvalidArgs] {
            assert_eq!(
                with_write_hint(Some(hint), failure(kind, "refused")),
                failure(kind, "refused"),
                "{kind:?}"
            );
        }
        for kind in [
            ErrorKind::Internal,
            ErrorKind::Denied,
            ErrorKind::InvalidArgs,
        ] {
            assert_eq!(
                with_write_hint(None, failure(kind, "refused")),
                failure(kind, "refused"),
                "{kind:?}"
            );
        }
    }

    #[test]
    fn a_missing_body_is_internal_and_reads_back_as_its_api() {
        let refused = no_body_refused(204, "events.insert", "the event");
        assert_eq!(
            refused,
            failure(
                ErrorKind::Internal,
                "Google answered HTTP 204 with no body where events.insert answers the event"
            )
        );
        assert_eq!(
            crate::testutil::no_body_parts(&refused.detail, 204),
            Some(("events.insert", "the event"))
        );
        assert_eq!(crate::testutil::no_body_parts(&refused.detail, 200), None);
    }
}
