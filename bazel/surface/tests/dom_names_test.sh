#!/usr/bin/env bash
# Liveness proof for a dom kind's file grammar.
#
# Three readers act on this one script — the record emitter, the staged-tree
# gate, and the dom package rule — and they consume its output positionally, one
# line each. The order the four names are printed in is therefore a join key:
# swap two and the gate hashes a record against its specification's name, with
# nothing else in the build noticing.
set -uo pipefail

dom_names="$1"
failures=0

fail() {
    echo "FAIL: $1"
    failures=$((failures + 1))
}

expect_names() {
    local label="$1" kind="$2" want="$3" got
    if ! got="$("$dom_names" "$kind" 2>&1)"; then
        fail "$label should be named, exited nonzero: $got"
        return
    fi
    if [ "$got" != "$want" ]; then
        fail "$label yields $(printf '%q' "$got"), wanted $(printf '%q' "$want")"
    fi
}

# Loader, wasm sibling, record, specification — the order the emitter and the
# gate read them in.
expect_names "a hyphenated kind" mode-clock \
    'brenn_mode_clock.js
brenn_mode_clock_bg.wasm
brenn_mode_clock.manifest.json
brenn_mode_clock.spec.brenn'

expect_names "a single-word kind" chrome \
    'brenn_chrome.js
brenn_chrome_bg.wasm
brenn_chrome.manifest.json
brenn_chrome.spec.brenn'

refuses() {
    local label="$1" kind="$2" out
    if out="$("$dom_names" "$kind" 2>&1)"; then
        fail "$label should be refused, printed: $out"
    fi
}

# The charset the host freezes: anything outside it names files the host would
# derive differently, or not at all.
refuses "an empty kind" ""
refuses "a kind holding a space" "mode clock"
refuses "an uppercase kind" "ModeClock"
refuses "a kind holding an underscore" "mode_clock"
refuses "a kind with a leading hyphen" "-clock"
refuses "a kind holding a -- run" "mode--clock"

if [ "$failures" -ne 0 ]; then
    echo "$failures case(s) failed"
    exit 1
fi
echo "dom_names: all cases passed"
