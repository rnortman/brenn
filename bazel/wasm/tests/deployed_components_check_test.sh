#!/usr/bin/env bash
# Liveness proof for the deploy-manifest gate.
#
# The gate passes over the real manifest, which says nothing about whether it
# would notice a bad entry. Here the manifest is a fixture: an undeclared name
# is rejected, an entry that nothing packages is rejected, an entry with no
# trailing newline is still read (the shape of edit most likely to have just
# been appended), and a manifest that yields no entries at all fails rather than
# reporting nothing missing.
set -uo pipefail

names="$1"
check="$2"
tmp="${TEST_TMPDIR:?TEST_TMPDIR must be set}"
declared="brenn_replay.wasm brenn_processor_demo.wasm brenn_unpackaged.wasm"
packaged="brenn_replay.wasm brenn_processor_demo.wasm"
failures=0

expect() {
    local want="$1" name="$2" needle="${3:-}"
    local out rc
    out="$("$check" "$names" "$declared" "$packaged" "$tmp/$name" "fixture:$name" 2>&1)"
    rc=$?
    if [ "$want" = "pass" ] && [ "$rc" -ne 0 ]; then
        echo "FAIL: $name should have passed, exited $rc: $out"
        failures=$((failures + 1))
        return
    fi
    if [ "$want" = "fail" ] && [ "$rc" -eq 0 ]; then
        echo "FAIL: $name should have been rejected, exited 0: $out"
        failures=$((failures + 1))
        return
    fi
    if [ -n "$needle" ] && ! printf '%s' "$out" | grep -qF "$needle"; then
        echo "FAIL: $name output does not mention '$needle': $out"
        failures=$((failures + 1))
    fi
}

printf '# a comment\n\nbrenn_replay.wasm\nbrenn_processor_demo.wasm  # trailing\n' > "$tmp/good"
printf 'brenn_replay.wasm\nbrenn_typo.wasm\n' > "$tmp/undeclared"
printf 'brenn_replay.wasm\nbrenn_typo.wasm' > "$tmp/no_final_newline"
printf 'brenn_replay.wasm' > "$tmp/single_no_newline"
printf 'brenn_replay.wasm\nbrenn_unpackaged.wasm\n' > "$tmp/unpackaged"
: > "$tmp/empty"
printf '# only comments\n\n' > "$tmp/comments_only"

expect pass good
expect fail undeclared "brenn_typo.wasm"
expect fail no_final_newline "brenn_typo.wasm"
expect pass single_no_newline
expect fail unpackaged "no component_package target packages"
expect fail empty "lists no components"
expect fail comments_only "lists no components"

if [ "$failures" -ne 0 ]; then
    echo "$failures case(s) failed"
    exit 1
fi
echo "deployed_components_check: all cases passed"
