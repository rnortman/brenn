#!/usr/bin/env bash
# Liveness proof for the `config/specs/` accountability gate.
#
# The gate runs once per build over the four real lists, which are equal by
# construction — so neither of its refusal branches ever executes there, and an
# inverted comparison or a lost `-x` would leave it passing forever while the
# failure it exists to catch (a specification every fit test compiles against
# and no release stages, first read at a deployment's boot) came back unseen.
# Here the lists are fixtures, wrong one way at a time.
#
# The last case is about concatenation rather than comparison: a generated list
# is written without a trailing newline, so joining two with `cat` would glue
# the last name of one to the first of the next and account for neither.
set -uo pipefail

gate="$1"
tmp="${TEST_TMPDIR:?TEST_TMPDIR must be set}"

# The gate writes its own scratch files directly under `$TEST_TMPDIR`, so the
# fixtures live in a directory of their own.
fixtures="$tmp/fixtures"
mkdir -p "$fixtures"
failures=0

fail() {
    echo "FAIL: $1"
    failures=$((failures + 1))
}

# One list per line of `$2`, newline-terminated unless `$3` says otherwise.
write_list() {
    local path="$fixtures/$1"
    printf '%s' "$2" >"$path"
    [ "${3:-newline}" = "raw" ] || printf '\n' >>"$path"
    echo "$path"
}

# `set -e` is off here on purpose: every case reads the gate's status.
run_gate() {
    "$gate" "$@" 2>&1
}

# ── Equal lists pass ──────────────────────────────────────────────────────
offered=$(write_list offered.txt $'chrome.brenn\nsurface-description.brenn')
packages=$(write_list packages.txt 'chrome.brenn')
library=$(write_list library.txt 'surface-description.brenn')
out=$(run_gate "$offered" "$packages" "$library")
status=$?
[ "$status" -eq 0 ] || fail "equal lists were refused: $out"
printf '%s' "$out" | grep -qF "PASS:" || fail "the passing run printed no verdict: $out"

# ── An offered name nothing ships is refused ──────────────────────────────
offered=$(write_list offered.txt $'chrome.brenn\nphantom.brenn')
out=$(run_gate "$offered" "$packages")
status=$?
[ "$status" -eq 1 ] || fail "a specification shipped by nothing was accepted: $out"
printf '%s' "$out" | grep -qF "config/specs/phantom.brenn is offered by //:modules and shipped by nothing" ||
    fail "the refusal does not name the unaccounted file: $out"
printf '%s' "$out" | grep -qF "BRENN_LIBRARY_MODULES" ||
    fail "the refusal does not name the three shipping mechanisms: $out"

# ── A shipped name that does not exist is refused ─────────────────────────
offered=$(write_list offered.txt 'chrome.brenn')
missing=$(write_list library.txt 'gone.brenn')
out=$(run_gate "$offered" "$packages" "$missing")
status=$?
[ "$status" -eq 1 ] || fail "a shipping list naming a file that does not exist was accepted: $out"
printf '%s' "$out" | grep -qF "gone.brenn is named as a shipped specification and does not exist" ||
    fail "the refusal does not name the missing file: $out"

# ── Two lists with no trailing newline stay two names ─────────────────────
offered=$(write_list offered.txt $'alpha.brenn\nomega.brenn')
first=$(write_list first.txt 'alpha.brenn' raw)
second=$(write_list second.txt 'omega.brenn' raw)
out=$(run_gate "$offered" "$first" "$second")
status=$?
[ "$status" -eq 0 ] || fail "unterminated lists were glued into a name that is neither: $out"

if [ "$failures" -ne 0 ]; then
    echo "$failures case(s) failed"
    exit 1
fi
echo "config_specs_accounted: the gate refuses in both directions and joins lists without gluing them"
