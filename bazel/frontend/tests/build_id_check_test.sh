#!/usr/bin/env bash
# Liveness proof for the bundle build-id gate.
#
# The gate passes over the real bundles, which says nothing about whether it
# would notice the substitution failing. Here the bundles are fixtures: the
# placeholder present is right unstamped and wrong stamped, absent is the
# reverse, a bundle that takes no id must never carry one, a missing bundle is
# rejected rather than skipped, and an invocation naming no bundles fails
# instead of reporting clean.
set -uo pipefail

check="$1"
tmp="${TEST_TMPDIR:?TEST_TMPDIR must be set}"
placeholder="{STABLE_BRENN_BUILD_ID}"
failures=0

fail() {
    echo "FAIL: $1"
    failures=$((failures + 1))
}

mkdir -p "$tmp/dev" "$tmp/rel" "$tmp/leak"
printf 'const raw = "%s";\n' "$placeholder" > "$tmp/dev/main.js"
printf 'export const boot = 1;\n' > "$tmp/dev/surface.js"
printf 'const raw = "v1.2.3";\n' > "$tmp/rel/main.js"
printf 'export const boot = 1;\n' > "$tmp/rel/surface.js"
printf 'const raw = "v1.2.3";\n' > "$tmp/leak/main.js"
printf 'const stray = "%s";\n' "$placeholder" > "$tmp/leak/surface.js"

if ! "$check" unstamped "$tmp/dev" main.js -- surface.js > "$tmp/dev.log" 2>&1; then
    fail "a dev bundle carrying the placeholder should pass: $(cat "$tmp/dev.log")"
fi

if ! "$check" stamped "$tmp/rel" main.js -- surface.js > "$tmp/rel.log" 2>&1; then
    fail "a stamped bundle carrying a real id should pass: $(cat "$tmp/rel.log")"
fi

# The release failure: a placeholder that no stamp replaced.
if out=$("$check" stamped "$tmp/dev" main.js -- surface.js 2>&1); then
    fail "a placeholder in a stamped bundle should be rejected, exited 0: $out"
elif ! printf '%s' "$out" | grep -qF "main.js"; then
    fail "the rejection does not name the bundle: $out"
fi

# The dev failure: the define dropped, so nothing substitutes anything.
if out=$("$check" unstamped "$tmp/rel" main.js -- surface.js 2>&1); then
    fail "an unstamped bundle with no placeholder should be rejected, exited 0: $out"
elif ! printf '%s' "$out" | grep -qF "main.js"; then
    fail "the rejection does not name the bundle: $out"
fi

# A bundle that takes no build id must not have acquired one.
if out=$("$check" stamped "$tmp/leak" main.js -- surface.js 2>&1); then
    fail "a no-id bundle carrying the placeholder should be rejected, exited 0: $out"
elif ! printf '%s' "$out" | grep -qF "surface.js"; then
    fail "the rejection does not name the no-id bundle: $out"
fi

# A bundle that stopped being produced.
if out=$("$check" unstamped "$tmp/dev" main.js absent.js -- surface.js 2>&1); then
    fail "a missing bundle should be rejected, exited 0: $out"
elif ! printf '%s' "$out" | grep -qF "absent.js"; then
    fail "the rejection does not name the missing bundle: $out"
fi

# Nothing to check is not the same as everything being fine.
if out=$("$check" unstamped "$tmp/dev" -- surface.js 2>&1); then
    fail "an invocation naming no build-id bundles should be rejected, exited 0: $out"
elif ! printf '%s' "$out" | grep -qF "assert nothing"; then
    fail "the rejection does not say why: $out"
fi

if [ "$failures" -ne 0 ]; then
    echo "$failures case(s) failed"
    exit 1
fi
echo "build_id_check: all cases passed"
