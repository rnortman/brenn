//! `api-request`'s reading of a request path and of its `path` ACL value.
//!
//! A request path is split on `/`, each segment percent-decoded once and
//! checked; the decoded segments are what an ACL template is matched against
//! and what the outbound URL is built from, so the bytes on the wire are the
//! segments that matched.
//!
//! A template is `*` (any path) or `/`-initial segments, each matching its
//! request segment exactly or, as `*`, any one segment. Segment counts must be
//! equal: there is no prefix match.

use percent_encoding::percent_decode_str;
use url::Url;

/// The `path` value meaning any path.
const ANY_PATH: &str = "*";

/// The `/`-separated segments after the leading `/`. `None` when `text` does
/// not begin with `/`.
fn raw_segments(text: &str) -> Option<std::str::Split<'_, char>> {
    text.strip_prefix('/').map(|rest| rest.split('/'))
}

/// Check a clause's `path` value: `*`, or `/`-initial with no empty, `.`,
/// `..` or `%`-bearing segment (a decoded request segment is never any of
/// those, so such a template could match nothing).
pub fn validate_template(template: &str) -> Result<(), String> {
    if template == ANY_PATH {
        return Ok(());
    }
    let Some(segments) = raw_segments(template) else {
        return Err(format!(
            "path template {template:?} must be \"*\" or begin with \"/\""
        ));
    };
    for segment in segments {
        if segment.is_empty() || segment == "." || segment == ".." || segment.contains('%') {
            return Err(format!(
                "path template {template:?} has segment {segment:?}; a segment is a literal \
                 (no \"%\", \".\" or \"..\") or \"*\", and none is empty"
            ));
        }
    }
    Ok(())
}

/// Split a request path, decode each segment once, and refuse any segment
/// that is empty, `.`, `..`, or holds `/`, `\` or `%` once decoded.
pub fn decode_path(path: &str) -> Result<Vec<String>, String> {
    let Some(raw) = raw_segments(path) else {
        return Err(format!("path {path:?} must begin with \"/\""));
    };
    raw.map(|segment| {
        let decoded = percent_decode_str(segment)
            .decode_utf8()
            .map_err(|_| format!("path segment {segment:?} is not UTF-8 once decoded"))?;
        if decoded.is_empty() || decoded == "." || decoded == ".." {
            return Err(format!(
                "path {path:?} has an empty, \".\" or \"..\" segment"
            ));
        }
        if decoded.contains(['/', '\\', '%']) {
            return Err(format!(
                "path segment {segment:?} decodes to {decoded:?}, which holds \"/\", \"\\\" or \
                 \"%\" (an encoded separator or double encoding)"
            ));
        }
        Ok(decoded.into_owned())
    })
    .collect()
}

/// Whether `template` (already valid) admits the decoded `segments`.
pub fn template_admits(template: &str, segments: &[String]) -> bool {
    if template == ANY_PATH {
        return true;
    }
    let Some(parts) = raw_segments(template) else {
        return false;
    };
    let parts: Vec<&str> = parts.collect();
    parts.len() == segments.len()
        && parts
            .iter()
            .zip(segments)
            .all(|(part, segment)| *part == "*" || part == segment)
}

/// The path as it will be sent, for logs and ACL messages: each decoded
/// segment re-encoded the way the URL builder encodes it.
pub fn display_path(segments: &[String]) -> String {
    let mut url = Url::parse("http://localhost").expect("a literal URL parses");
    push_segments(&mut url, segments);
    url.path().to_string()
}

/// `origin` with `segments` as its path and `query` as its query, every
/// segment and pair percent-encoded by `url`.
pub fn build_url(origin: &Url, segments: &[String], query: &[(String, String)]) -> Url {
    let mut url = origin.clone();
    push_segments(&mut url, segments);
    if !query.is_empty() {
        let mut pairs = url.query_pairs_mut();
        for (key, value) in query {
            pairs.append_pair(key, value);
        }
    }
    url
}

fn push_segments(url: &mut Url, segments: &[String]) {
    url.path_segments_mut()
        .expect("an http(s) URL has path segments")
        .clear()
        .extend(segments);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn segs(path: &str) -> Vec<String> {
        decode_path(path).unwrap_or_else(|e| panic!("{path}: {e}"))
    }

    #[test]
    fn templates_validate() {
        for ok in [
            "*",
            "/gmail/v1/users/me/drafts",
            "/calendar/v3/calendars/*/events",
            "/*",
        ] {
            assert_eq!(validate_template(ok), Ok(()), "{ok}");
        }
        for bad in [
            "gmail/v1", "", "/", "/a//b", "/a/", "/a/./b", "/a/../b", "/a%2Fb", "**",
        ] {
            assert!(validate_template(bad).is_err(), "{bad:?} must be refused");
        }
    }

    #[test]
    fn paths_refused_before_matching() {
        for bad in [
            "a/b",
            "/a/../b",
            "/a/./b",
            "/a/%2e%2e/b",
            "/a/%2E/b",
            "/a%2Fb",
            "/a%2fb",
            "/a/%252e%252e/b",
            "/a/%252F",
            "/a/",
            "/",
            "/a//b",
            "/a%5Cb",
            "/a\\b",
            "/a/%ff",
        ] {
            assert!(decode_path(bad).is_err(), "{bad:?} must be refused");
        }
    }

    #[test]
    fn segment_template_matching() {
        let drafts = "/gmail/v1/users/me/drafts";
        assert!(template_admits(drafts, &segs("/gmail/v1/users/me/drafts")));
        assert!(!template_admits(
            drafts,
            &segs("/gmail/v1/users/me/drafts/send")
        ));
        assert!(!template_admits(drafts, &segs("/gmail/v1/users/me")));
        assert!(!template_admits(
            drafts,
            &segs("/gmail/v1/users/you/drafts")
        ));

        let events = "/calendar/v3/calendars/*/events";
        assert!(template_admits(
            events,
            &segs("/calendar/v3/calendars/primary/events")
        ));
        assert!(!template_admits(
            events,
            &segs("/calendar/v3/calendars/x/clear")
        ));
        assert!(!template_admits(
            events,
            &segs("/calendar/v3/calendars/x/events/y")
        ));

        let holiday =
            segs("/calendar/v3/calendars/en.usa%23holiday%40group.v.calendar.google.com/events");
        assert_eq!(holiday[3], "en.usa#holiday@group.v.calendar.google.com");
        assert!(template_admits(events, &holiday));
        let question = segs("/calendar/v3/calendars/x%3F/events/y");
        assert!(template_admits(
            "/calendar/v3/calendars/*/events/*",
            &question
        ));

        assert!(template_admits("*", &segs("/anything/at/all")));
        // A literal `*` in a request segment is content, matched like any other.
        assert!(template_admits("/a/*", &segs("/a/*")));
    }

    #[test]
    fn url_is_built_from_the_decoded_segments() {
        let origin = |text: &str| Url::parse(text).unwrap();
        let url = build_url(
            &origin("https://www.googleapis.com"),
            &segs("/calendar/v3/calendars/en.usa%23holiday%40group.v.calendar.google.com/events"),
            &[
                ("maxResults".to_string(), "5".to_string()),
                ("q".to_string(), "a b&c".to_string()),
            ],
        );
        assert_eq!(
            url.as_str(),
            "https://www.googleapis.com/calendar/v3/calendars/en.usa%23holiday@group.v.calendar.google.com/events?maxResults=5&q=a+b%26c"
        );
        let url = build_url(
            &origin("http://127.0.0.1:8080"),
            &segs("/x%3F/a%20b/c:d"),
            &[],
        );
        assert_eq!(url.as_str(), "http://127.0.0.1:8080/x%3F/a%20b/c:d");
        assert_eq!(display_path(&segs("/x%3F/a b")), "/x%3F/a%20b");
    }
}
