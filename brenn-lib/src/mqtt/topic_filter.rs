//! The one MQTT topic-filter match: is one filter covered by another.
//!
//! A topic name is a filter with no wildcard, so "does this topic match this
//! filter" is the case of "is this filter a subset of that one" where the inner
//! side has no wildcard. The subscribe ACL, the publish ACL and inbound routing
//! all ask it through [`covers`].

/// Returns `true` only if every topic `inner` matches is also matched by
/// `outer`.
///
/// **Precondition:** both arguments are validated topic filters (see
/// [`crate::mqtt::address::validate_topic_filter_str`]); a topic name is a
/// filter with no wildcard and is passed as `inner`. Behaviour on a malformed
/// filter (`#extra`, `+x`, `a/#/b`) is unspecified, and every caller validates
/// first. The function is total: it never panics on any input.
///
/// **Sound, and exact with one deliberate exception.** It never returns `true`
/// unless the subset relation holds. It returns `false` for an `inner` ending
/// in `#` whose parent is not a topic (`#`, `/#`) against an `outer` that
/// spells the same topic set with `+/#` (`+/#`, `/+/#`): a requested `#` is
/// covered only by an allowed `#`. The entry that covers it is `#` (or `/#`).
///
/// **The `$` rule.** A first-level `#` or `+` matches no topic whose first
/// level starts with `$`, so it covers no filter whose first level starts with
/// `$`. First level only: `a/#` covers `a/$x`, and `$SYS/#` covers
/// `$SYS/broker/load` because its first level is spelled.
///
/// Empty levels are ordinary literals. `$share/…` gets no special reading.
pub fn covers(outer: &str, inner: &str) -> bool {
    // TODO(efficiency-covers-allocates-two-vecs-per-call-on-ingress-path): two
    // `split('/')` iterators in step would avoid both allocations.
    let outer_levels: Vec<&str> = outer.split('/').collect();
    let inner_levels: Vec<&str> = inner.split('/').collect();

    // `split` on a non-empty pattern always yields at least one level.
    if (outer_levels[0] == "#" || outer_levels[0] == "+") && inner_levels[0].starts_with('$') {
        return false;
    }

    let mut oi = 0;
    let mut ii = 0;

    loop {
        // `outer` exhausted without a terminal `#`: covered only if `inner` is
        // exhausted too, else `inner` is deeper.
        if oi >= outer_levels.len() {
            return ii >= inner_levels.len();
        }

        let olevel = outer_levels[oi];

        // A terminal `#` covers every remaining inner level, including none
        // (`sport/#` matches `sport`).
        if olevel == "#" {
            return true;
        }

        // `outer` still requires a level that `inner` does not have.
        if ii >= inner_levels.len() {
            return false;
        }

        let ilevel = inner_levels[ii];

        // An inner `#` (terminal, being validated) spans arbitrary depth; only an
        // outer `#`, handled above, covers it. This also denies `+/#` against
        // `#`, the documented exception.
        if ilevel == "#" {
            return false;
        }

        // An outer `+` covers any single inner level, `+` included. An outer
        // literal covers only the equal literal; an inner `+` spans values it
        // does not.
        if olevel != "+" && (ilevel == "+" || ilevel != olevel) {
            return false;
        }

        oi += 1;
        ii += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::covers;

    /// The one table. Each row is `(outer, inner, expected)`.
    #[test]
    fn covers_table() {
        let cases: &[(&str, &str, bool)] = &[
            // --- Filter against filter ---
            // A single-level outer must not cover an arbitrary-depth inner.
            ("sensors/+/temp", "sensors/#", false),
            // Terminal `#` covers a concrete inner and a deeper wildcard inner.
            ("sensors/#", "sensors/temp", true),
            ("sensors/#", "sensors/+/temp", true),
            // Identical filters.
            ("sensors/+/temp", "sensors/+/temp", true),
            // Differing trailing literal under matching `+`.
            ("sensors/+/temp", "sensors/+/humidity", false),
            // `+` covers a concrete level.
            ("sensors/+/temp", "sensors/kitchen/temp", true),
            // Inner deeper than outer.
            ("sensors/+", "sensors/+/temp", false),
            // Outer requires more levels than inner; a terminal `#` does not.
            ("a/b", "a", false),
            ("sport/#", "sport", true),
            // A trailing `+` demands one more level; it is not a terminal `#`.
            ("sensors/+", "sensors", false),
            // Root `#` covers everything non-`$`, a `#` inner included.
            ("#", "anything/deep", true),
            ("#", "#", true),
            // Exact matching.
            ("a/b/c", "a/b/c", true),
            ("a/b/c", "a/b/d", false),
            // Single-level boundaries.
            ("+", "x", true),
            ("+", "+", true),
            ("a", "a", true),
            ("a", "b", false),
            // Mixed `+`/`#` on both sides.
            ("a/+/#", "a/+/#", true),
            ("a/+/#", "a/b/c/d", true),
            ("a/+/#", "a/b/#", true),
            // An inner `+` under an outer literal spans values the literal does not.
            ("sensors/kitchen/temp", "sensors/+/temp", false),
            // Outer deeper than inner, without a terminal `#`.
            ("a/b/c", "a/b", false),
            // Inner deeper than an all-literal outer.
            ("a/b", "a/b/c", false),
            // Terminal `#` covers equal depth, deeper, and the bare prefix.
            ("a/#", "a/b", true),
            ("a/#", "a/b/c/d", true),
            ("a/#", "a", true),
            // --- Filter against topic ---
            ("home/switch/state", "home/switch/state", true),
            ("home/switch/state", "home/switch/other", false),
            ("home/switch", "home/switch/state", false),
            ("home/switch/state", "home/switch", false),
            ("home/+/state", "home/switch/state", true),
            ("home/+/state", "home/lamp/state", true),
            ("home/+/state", "home/switch/level", false),
            // `+` matches exactly one level and never crosses `/`.
            ("home/+/state", "home/a/b/state", false),
            ("home/+", "home/a/b", false),
            ("home/+/state", "home/state", false),
            ("home/#", "home/switch/state", true),
            ("home/#", "home/switch", true),
            ("home/#", "office/switch", false),
            // `sport/#` matches its parent and every descendant, not a sibling.
            ("sport/#", "sport/tennis", true),
            ("sport/#", "sport/tennis/player1", true),
            ("sport/#", "sporting", false),
            ("#", "anything", true),
            ("#", "a/b/c", true),
            ("#", "a", true),
            ("+", "a", true),
            ("+", "a/b", false),
            ("+/state", "home/state", true),
            ("+/state", "home/switch/state", false),
            // A leading wildcard matches no `$`-first-level topic.
            ("#", "$SYS/broker/load", false),
            ("#", "$SYS", false),
            ("+", "$SYS", false),
            ("+/monitor", "$SYS/monitor", false),
            // A spelled `$` first level matches.
            ("$SYS/#", "$SYS/broker/load", true),
            ("$SYS/+", "$SYS/broker", true),
            ("$SYS/broker", "$SYS/broker", true),
            // The rule is first level only.
            ("a/#", "a/$weird", true),
            ("#", "sport/tennis", true),
            // --- The `$` rule, against a filter and a topic ---
            ("#", "$SYS/x", false),
            // Every topic `$SYS/#` matches starts with `$SYS`.
            ("#", "$SYS/#", false),
            ("+/+", "$share/g", false),
            ("+/#", "$SYS/#", false),
            ("$SYS/#", "$SYS/#", true),
            ("a/#", "a/$x", true),
            // The inner's first level is `+`, which itself excludes `$`.
            ("#", "+/$x", true),
            // The inner matches `a/x` and `a`; the outer matches neither.
            ("$SYS/#", "+/x", false),
            ("$SYS/#", "#", false),
            // --- An inner wildcard under an outer `+` ---
            // `#` matches `a/b`; `+` does not.
            ("+", "#", false),
            // `a/#` matches `a` and `a/b/c`; `a/+` matches neither.
            ("a/+", "a/#", false),
            // `a/#` matches `a`; `a/+/#` needs a second level.
            ("a/+/#", "a/#", false),
            // `a/#` matches `a/b`.
            ("a/+/c", "a/#", false),
            // Deliberate: `+/#` and `#` match the same topics, but a requested
            // `#` is covered only by an allowed `#`.
            ("+/#", "#", false),
            ("#", "+/#", true),
            // --- Empty levels are literals ---
            ("a//b", "a//b", true),
            ("a/+/b", "a//b", true),
            ("a/b", "a//b", false),
        ];

        for &(outer, inner, expected) in cases {
            assert_eq!(
                covers(outer, inner),
                expected,
                "covers(outer={outer:?}, inner={inner:?}) should be {expected}",
            );
        }
    }
}
