//! MQTT topic-filter **subset** matching for `mqtt_subscribe` ACLs.
//!
//! The ACL question is "may this app subscribe to the *requested* filter, given
//! it is allowed the *allowed* filter?" — i.e. is `requested` a **subset** of
//! `allowed`? This is *not* string equality, and it is *not* the existing
//! [`crate::mqtt::address::mqtt_topic_matches`], which matches a **concrete**
//! topic (no wildcards) against a filter. A requested *filter* itself contains
//! `+`/`#`, so subset matching must reason about wildcards on **both** sides
//! (filter ⊑ filter).
//!
//! Getting this wrong over-grants (e.g. allow `sensors/+/temp`, request
//! `sensors/#` must be **rejected**) or under-grants, so this is the single most
//! error-prone piece of the access-control work. The unit tests below are the
//! load-bearing adversarial table.

/// Returns `true` iff every concrete topic that `requested` could match is also
/// matched by `allowed` — i.e. `requested` is a subset of `allowed`.
///
/// **Precondition (load-bearing):** both inputs are *validated* MQTT topic
/// filters (`#` terminal and a whole segment, `+` a whole segment) — see
/// [`crate::mqtt::address::validate_topic_filter_str`]. The `allowed` side is
/// validated at policy-resolution time; the `requested` side must be validated
/// by the caller *before* invoking this function (the enforcement site does so
/// and falls through to the core's canonical `InvalidMqttFilter` error on
/// failure). Behavior on a malformed segment (e.g. `#extra`, `+x`,
/// `sensors/#/extra`) is **unspecified**; the caller must never pass one.
///
/// The function is **total** and **pure** over validated input (no panics):
/// when in doubt it denies (returns `false`).
///
/// **Out of scope:** the MQTT `$`-prefix reservation (brokers exclude
/// `$SYS/...` and other `$`-topics from a root `#`/`+` match) is **not**
/// modeled here. `filter_covers` treats `$`-prefixed segments as ordinary
/// literals, so e.g. `filter_covers("#", "$SYS/broker")` returns `true`. This
/// is conservative for an *allow*-list ACL (it never under-grants relative to
/// broker behavior), and the `$SYS` exception is a broker delivery rule, not a
/// subset-coverage rule. Pinning it would belong with broker-side handling, not
/// the ACL matcher.
pub fn filter_covers(allowed: &str, requested: &str) -> bool {
    let allowed_segments: Vec<&str> = allowed.split('/').collect();
    let requested_segments: Vec<&str> = requested.split('/').collect();

    let mut ai = 0;
    let mut ri = 0;

    loop {
        // `allowed` exhausted.
        if ai >= allowed_segments.len() {
            // If `requested` still has segments, it is deeper than `allowed`
            // (and `allowed` did not end in `#`, else we'd have returned true
            // already), so it is not a subset.
            return ri >= requested_segments.len();
        }

        let aseg = allowed_segments[ai];

        // A terminal `#` on the allowed side covers all remaining requested
        // segments (including the case where `requested` is shorter, mirroring
        // the MQTT rule that `sport/#` matches `sport`).
        if aseg == "#" {
            return true;
        }

        // `allowed` still requires a level but `requested` is exhausted: the
        // allowed filter demands more depth than requested provides (e.g. allow
        // `a/b`, request `a`), so requested is not a subset.
        if ri >= requested_segments.len() {
            return false;
        }

        let rseg = requested_segments[ri];

        // A `#` on the requested side (which, being validated, is terminal here)
        // spans arbitrary depth. The only allowed segment that can cover that is
        // `#`, already handled above — so any non-`#` allowed segment under a
        // requested `#` is an over-grant trap: reject. (E.g. allow
        // `sensors/+/temp`, request `sensors/#`.)
        if rseg == "#" {
            return false;
        }

        if aseg == "+" {
            // `+` covers any single requested level — including a requested `+`
            // (single level ⊑ single level). A requested `#` was already
            // rejected above. Consume one level on each side.
        } else {
            // `aseg` is a literal. It covers a requested literal only when
            // equal; a requested `+` spans values the literal does not.
            if rseg == "+" || rseg != aseg {
                return false;
            }
        }

        ai += 1;
        ri += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::filter_covers;

    /// The adversarial subset table (design §6.3 / Test Plan §4). Each row is
    /// `(allowed, requested, expected)`.
    #[test]
    fn subset_table() {
        let cases: &[(&str, &str, bool)] = &[
            // The canonical over-grant trap: a single-level allow must not
            // cover an arbitrary-depth request.
            ("sensors/+/temp", "sensors/#", false),
            // Terminal `#` covers a shorter/concrete request.
            ("sensors/#", "sensors/temp", true),
            // Terminal `#` covers a deeper wildcard request.
            ("sensors/#", "sensors/+/temp", true),
            // Identical filters cover each other.
            ("sensors/+/temp", "sensors/+/temp", true),
            // Differing trailing literal under matching `+` ⇒ reject.
            ("sensors/+/temp", "sensors/+/humidity", false),
            // `+` covers a concrete level.
            ("sensors/+/temp", "sensors/kitchen/temp", true),
            // Requesting greater depth than allowed ⇒ reject.
            ("sensors/+", "sensors/+/temp", false),
            // Allowed requires more levels than requested ⇒ reject; but a
            // trailing allowed `#` covers the shorter request.
            ("a/b", "a", false),
            ("sport/#", "sport", true),
            // Mirror of the literal-depth case but with a trailing `+`: `+`
            // demands one more level, so a shorter request is not covered. Pins
            // that the exhaustion check rejects via the unconsumed `+` rather
            // than treating `+` like a terminal `#`.
            ("sensors/+", "sensors", false),
            // Root `#` covers everything, including a `#` request.
            ("#", "anything/deep", true),
            ("#", "#", true),
            // Pure exact matching.
            ("a/b/c", "a/b/c", true),
            ("a/b/c", "a/b/d", false),
            // Single-segment degenerate cases: the loop exits on its first
            // iteration, so these pin the exhaustion checks at the boundary.
            ("+", "x", true),  // single-level wildcard covers one concrete level
            ("+", "+", true),  // `+` ⊑ `+` at the root
            ("a", "a", true),  // single-segment exact match
            ("a", "b", false), // single-segment mismatch
            // Mixed `+`/`#` filter identical on both sides, plus concrete and
            // wildcard requests under it (consume `+`, then terminal `#`).
            ("a/+/#", "a/+/#", true),
            ("a/+/#", "a/b/c/d", true),
            ("a/+/#", "a/b/#", true),
        ];

        for &(allowed, requested, expected) in cases {
            assert_eq!(
                filter_covers(allowed, requested),
                expected,
                "filter_covers(allowed={allowed:?}, requested={requested:?}) \
                 should be {expected}",
            );
        }
    }

    #[test]
    fn requested_plus_under_allowed_literal_rejected() {
        // A requested `+` spans values a literal allowed segment does not cover.
        assert!(!filter_covers("sensors/kitchen/temp", "sensors/+/temp"));
    }

    #[test]
    fn allowed_deeper_than_requested_rejected() {
        // Allowed requires depth the request does not provide (and allowed does
        // not end in `#`).
        assert!(!filter_covers("a/b/c", "a/b"));
    }

    #[test]
    fn requested_deeper_than_allowed_literal_rejected() {
        assert!(!filter_covers("a/b", "a/b/c"));
    }

    #[test]
    fn terminal_hash_covers_equal_depth_and_deeper() {
        assert!(filter_covers("a/#", "a/b"), "a/# should cover a/b");
        assert!(
            filter_covers("a/#", "a/b/c/d"),
            "a/# should cover deeper paths"
        );
        // `a/#` also matches `a` itself per MQTT semantics.
        assert!(
            filter_covers("a/#", "a"),
            "a/# should cover the bare prefix per MQTT semantics"
        );
    }
}
