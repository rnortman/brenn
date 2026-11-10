//! `api-request`'s reading of a request path and of its `path` ACL value.
//!
//! A request path is `/`, or `/`-initial segments optionally followed by one
//! trailing `/`. It is split on `/`, each segment percent-decoded once and
//! checked; the decoded segments and whether the path ends in `/` are what an
//! ACL template is matched against and what the outbound URL is built from,
//! so the bytes on the wire are the path that matched. `/` is the root: no
//! segments, ending in `/`. An empty segment anywhere but the one trailing
//! `/` is refused.
//!
//! A template is `*`, `/`, or `/`-initial segments optionally followed by one
//! `/`, each segment matching its request segment exactly or, as `*`, any one
//! segment. Segment counts must be equal, except that a template's last
//! segment may be `**`, which matches one or more segments: `/files/**`
//! admits every path beneath `/files/` and not `/files/` itself. A template
//! ending in `/` admits only a path ending in `/`, and one that does not
//! admits only a path that does not, `**` included; `*` admits every path
//! not ending in `/` and none that does.

use percent_encoding::percent_decode_str;
use url::Url;

/// The `path` value meaning any path that does not end in `/`.
pub const ANY_PATH: &str = "*";

/// The template segment matching one or more request segments; only a
/// template's last segment.
pub const ANY_DEPTH: &str = "**";

/// A decoded request path: its segments, and whether it ends in `/`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequestPath {
    pub segments: Vec<String>,
    pub trailing_slash: bool,
}

/// The raw `/`-separated segments of `text` and whether it ends in one
/// trailing `/`: the leading `/` stripped, an empty rest the root (no
/// segments, trailing slash), otherwise one trailing `/` taken off and the
/// rest split. `None` when `text` does not begin with `/`.
fn raw_split(text: &str) -> Option<(Vec<&str>, bool)> {
    let rest = text.strip_prefix('/')?;
    if rest.is_empty() {
        return Some((Vec::new(), true));
    }
    let (rest, trailing_slash) = match rest.strip_suffix('/') {
        Some(rest) => (rest, true),
        None => (rest, false),
    };
    Some((rest.split('/').collect(), trailing_slash))
}

/// Check a clause's `path` value: `*`, `/`, or `/`-initial segments
/// optionally followed by one `/`, with no empty, `.`, `..` or `%`-bearing
/// segment (a decoded request segment is never any of those, so such a
/// template could match nothing), and `**` as the last segment only.
pub fn validate_template(template: &str) -> Result<(), String> {
    if template == ANY_PATH {
        return Ok(());
    }
    let Some((segments, _)) = raw_split(template) else {
        return Err(format!(
            "path template {template:?} must be \"*\" or begin with \"/\""
        ));
    };
    let last = segments.len().saturating_sub(1);
    for (index, segment) in segments.into_iter().enumerate() {
        if segment == ANY_DEPTH && index != last {
            return Err(format!(
                "path template {template:?} has \"**\" before its last segment; \"**\" is \
                 only the last segment (before the one trailing \"/\", when there is one)"
            ));
        }
        if segment.is_empty() || segment == "." || segment == ".." || segment.contains('%') {
            return Err(format!(
                "path template {template:?} has segment {segment:?}; a segment is a literal \
                 (no \"%\", \".\" or \"..\") or \"*\", and none is empty (one trailing \"/\" \
                 is allowed; an empty segment anywhere else is not)"
            ));
        }
    }
    Ok(())
}

/// Split a request path, decode each segment once, and refuse any segment
/// that is empty, `.`, `..`, or holds `/`, `\` or `%` once decoded. One
/// trailing `/` is allowed and recorded; an empty segment anywhere else is
/// refused.
pub fn decode_path(path: &str) -> Result<RequestPath, String> {
    let Some((raw, trailing_slash)) = raw_split(path) else {
        return Err(format!("path {path:?} must begin with \"/\""));
    };
    let segments = raw
        .into_iter()
        .map(|segment| {
            let decoded = percent_decode_str(segment)
                .decode_utf8()
                .map_err(|_| format!("path segment {segment:?} is not UTF-8 once decoded"))?;
            if decoded.is_empty() || decoded == "." || decoded == ".." {
                return Err(format!(
                    "path {path:?} has an empty, \".\" or \"..\" segment (one trailing \"/\" \
                     is allowed; an empty segment anywhere else is not)"
                ));
            }
            if decoded.contains(['/', '\\', '%']) {
                return Err(format!(
                    "path segment {segment:?} decodes to {decoded:?}, which holds \"/\", \"\\\" \
                     or \"%\" (an encoded separator or double encoding)"
                ));
            }
            Ok(decoded.into_owned())
        })
        .collect::<Result<_, _>>()?;
    Ok(RequestPath {
        segments,
        trailing_slash,
    })
}

/// Whether `template` (already valid) admits `path`: `*` admits any path not
/// ending in `/`; any other template admits a path only when both end in `/`
/// or neither does. A template whose last segment is `**` then admits a path
/// with more segments than the template has before `**`, those leading
/// segments matching one for one; any other template admits a path whose
/// segments match its own one for one.
pub fn template_admits(template: &str, path: &RequestPath) -> bool {
    if template == ANY_PATH {
        return !path.trailing_slash;
    }
    let Some((parts, trailing_slash)) = raw_split(template) else {
        return false;
    };
    if trailing_slash != path.trailing_slash {
        return false;
    }
    let matches = |part: &&str, segment: &String| *part == "*" || part == segment;
    match parts.split_last() {
        Some((&ANY_DEPTH, prefix)) => {
            path.segments.len() > prefix.len()
                && prefix
                    .iter()
                    .zip(&path.segments)
                    .all(|(p, s)| matches(p, s))
        }
        _ => {
            parts.len() == path.segments.len()
                && parts.iter().zip(&path.segments).all(|(p, s)| matches(p, s))
        }
    }
}

/// The path as it will be sent, for logs and ACL messages: each decoded
/// segment re-encoded the way the URL builder encodes it, and the trailing
/// `/` when the path has one.
pub fn display_path(path: &RequestPath) -> String {
    let mut url = Url::parse("http://localhost").expect("a literal URL parses");
    push_path(&mut url, path);
    url.path().to_string()
}

/// `origin` with `path` as its path and `query` as its query, every segment
/// and pair percent-encoded by `url`, and the trailing `/` when the path has
/// one.
pub fn build_url(origin: &Url, path: &RequestPath, query: &[(String, String)]) -> Url {
    let mut url = origin.clone();
    push_path(&mut url, path);
    if !query.is_empty() {
        let mut pairs = url.query_pairs_mut();
        for (key, value) in query {
            pairs.append_pair(key, value);
        }
    }
    url
}

/// Write `path` as `url`'s path. A trailing `/` after segments is one empty
/// segment pushed last; the root is a path with no segments, which `url`
/// writes as `/`.
fn push_path(url: &mut Url, path: &RequestPath) {
    let mut segments = url
        .path_segments_mut()
        .expect("an http(s) URL has path segments");
    segments.clear().extend(&path.segments);
    if path.trailing_slash && !path.segments.is_empty() {
        segments.push("");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn segs(path: &str) -> RequestPath {
        decode_path(path).unwrap_or_else(|e| panic!("{path}: {e}"))
    }

    #[test]
    fn templates_validate() {
        for ok in [
            "*",
            "/gmail/v1/users/me/drafts",
            "/calendar/v3/calendars/*/events",
            "/*",
            "/",
            "/a/",
            "/a/*/",
        ] {
            assert_eq!(validate_template(ok), Ok(()), "{ok}");
        }
        for bad in [
            "gmail/v1", "", "/a//b", "/a//", "//", "/a/./b", "/a/../b", "/a%2Fb", "**",
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
            "/a//",
            "//",
            "/a//b",
            "/a/%2F",
            "/a/%2F/",
            "/a%5Cb",
            "/a\\b",
            "/a/%ff",
        ] {
            assert!(decode_path(bad).is_err(), "{bad:?} must be refused");
        }
    }

    #[test]
    fn a_trailing_slash_is_part_of_the_path() {
        let path = |segments: &[&str], trailing_slash| RequestPath {
            segments: segments.iter().map(|s| s.to_string()).collect(),
            trailing_slash,
        };
        assert_eq!(segs("/a/"), path(&["a"], true));
        assert_eq!(segs("/"), path(&[], true));
        assert_eq!(segs("/a"), path(&["a"], false));
        assert_eq!(segs("/a/b/"), path(&["a", "b"], true));
    }

    #[test]
    fn a_slashed_path_needs_a_slashed_template() {
        for (template, path) in [("/a/", "/a/"), ("/a/*/", "/a/x/"), ("/", "/")] {
            assert!(
                template_admits(template, &segs(path)),
                "{template} must admit {path}"
            );
        }
        for (template, path) in [
            ("/a", "/a/"),
            ("/a/", "/a"),
            ("/a/*", "/a/"),
            ("/a/*", "/a/x/"),
            ("/a/*/", "/a/x"),
            ("/", "/a/"),
            ("/*", "/"),
            ("*", "/a/"),
            ("*", "/"),
        ] {
            assert!(
                !template_admits(template, &segs(path)),
                "{template} must not admit {path}"
            );
        }
    }

    #[test]
    fn a_deep_template_admits_its_kind_at_every_depth_beneath_its_prefix() {
        for (template, paths) in [
            ("/files/**", ["/files/a", "/files/a/b", "/files/a/b/c/d/e"]),
            (
                "/files/**/",
                ["/files/a/", "/files/a/b/", "/files/a/b/c/d/e/"],
            ),
            ("/**/", ["/a/", "/a/b/", "/a/b/c/d/e/"]),
        ] {
            for path in paths {
                assert!(
                    template_admits(template, &segs(path)),
                    "{template} must admit {path}"
                );
            }
        }
        for (template, path) in [
            ("/files/**", "/files/a/"),
            ("/files/**", "/files/a/b/c/d/e/"),
            ("/files/**", "/files"),
            ("/files/**", "/files/"),
            ("/files/**", "/files2/a"),
            ("/files/**", "/a"),
            ("/files/**", "/"),
            ("/files/**/", "/files/a"),
            ("/files/**/", "/files/a/b/c/d/e"),
            ("/files/**/", "/files"),
            ("/files/**/", "/files/"),
            ("/files/**/", "/files2/a/"),
            ("/files/**/", "/a/"),
            ("/files/**/", "/"),
            ("/**/", "/"),
            ("/**/", "/a"),
            ("/**", "/a/"),
            ("/**", "/"),
        ] {
            assert!(
                !template_admits(template, &segs(path)),
                "{template} must not admit {path}"
            );
        }
    }

    /// `/**` admits exactly what `*` admits.
    #[test]
    fn a_root_deep_template_is_the_any_path_set() {
        for path in ["/a/", "/a/x/", "/", "/a", "/a/x", "/a/b/c", "/x/", "/*"] {
            let path = segs(path);
            assert_eq!(
                template_admits("/**", &path),
                template_admits(ANY_PATH, &path),
                "{path:?}"
            );
        }
    }

    #[test]
    fn a_star_before_a_deep_segment_matches_one_segment() {
        let template = "/dav/*/**/";
        assert!(template_admits(template, &segs("/dav/alice/a/")));
        assert!(template_admits(template, &segs("/dav/bob/a/b/c/")));
        assert!(!template_admits(template, &segs("/dav/alice/")));
        assert!(!template_admits(template, &segs("/other/alice/a/")));
        // A request segment literally `**` is content: `*` and `**` admit it.
        assert!(template_admits("/a/**", &segs("/a/**")));
        assert!(template_admits("/a/*", &segs("/a/**")));
    }

    #[test]
    fn a_deep_segment_is_only_the_last() {
        for ok in ["/**", "/**/", "/a/**", "/a/*/**/", "/a/**x", "/a/x**/b"] {
            assert_eq!(validate_template(ok), Ok(()), "{ok}");
        }
        for bad in ["/**/a", "/a/**/b/", "/a/**/**", "/**/**/"] {
            let err = validate_template(bad).expect_err(bad);
            assert!(err.contains("only the last segment"), "{bad}: {err}");
        }
    }

    #[test]
    fn a_trailing_slash_is_sent_as_written() {
        let origin = Url::parse("https://dav.example.com").unwrap();
        let home = segs("/dav/calendars/user/alice%40example.com/");
        assert_eq!(
            build_url(&origin, &home, &[]).as_str(),
            "https://dav.example.com/dav/calendars/user/alice@example.com/"
        );
        assert_eq!(
            display_path(&home),
            "/dav/calendars/user/alice@example.com/"
        );
        assert_eq!(
            build_url(&origin, &segs("/a/b/"), &[]).as_str(),
            "https://dav.example.com/a/b/"
        );
        assert_eq!(display_path(&segs("/a/b/")), "/a/b/");
        assert_eq!(
            build_url(&origin, &segs("/"), &[]).as_str(),
            "https://dav.example.com/"
        );
        assert_eq!(display_path(&segs("/")), "/");
        assert_eq!(
            build_url(&origin, &segs("/a/b"), &[]).as_str(),
            "https://dav.example.com/a/b"
        );
        assert_eq!(display_path(&segs("/a/b")), "/a/b");
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
        assert_eq!(
            holiday.segments[3],
            "en.usa#holiday@group.v.calendar.google.com"
        );
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

    /// A raw `#` or `?` in a path segment is content, not a fragment or a
    /// query: it decodes as itself, matches one `*`, and goes on the wire
    /// encoded.
    #[test]
    fn a_raw_hash_or_question_mark_is_segment_content() {
        let holiday =
            segs("/calendar/v3/calendars/en.usa#holiday@group.v.calendar.google.com/events");
        assert_eq!(holiday.segments.len(), 5);
        assert_eq!(
            holiday.segments[3],
            "en.usa#holiday@group.v.calendar.google.com"
        );
        assert!(template_admits("/calendar/v3/calendars/*/events", &holiday));
        let url = build_url(
            &Url::parse("https://www.googleapis.com").unwrap(),
            &holiday,
            &[],
        );
        assert_eq!(
            url.as_str(),
            "https://www.googleapis.com/calendar/v3/calendars/en.usa%23holiday@group.v.calendar.google.com/events"
        );
        assert_eq!(url.fragment(), None);

        let question = segs("/calendar/v3/calendars/a?b/events/e1");
        assert_eq!(question.segments[3], "a?b");
        assert!(template_admits(
            "/calendar/v3/calendars/*/events/*",
            &question
        ));
        assert_eq!(
            display_path(&question),
            "/calendar/v3/calendars/a%3Fb/events/e1"
        );
        let url = build_url(
            &Url::parse("https://www.googleapis.com").unwrap(),
            &question,
            &[],
        );
        assert_eq!(url.query(), None);
        assert_eq!(
            url.as_str(),
            "https://www.googleapis.com/calendar/v3/calendars/a%3Fb/events/e1"
        );
    }
}
