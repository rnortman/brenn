#!/usr/bin/env bash
# Liveness proof for the deploy manifest's grammar.
#
# Three callers act on this one reader — the built-artifact gate, the release
# assembly, and the staged-tree gate — so what counts as an entry is asserted
# here rather than in each of them: comments, blanks, whitespace, a CR from an
# editor that wrote CRLF, and a final line with no newline.
set -uo pipefail

names="$1"
tmp="${TEST_TMPDIR:?TEST_TMPDIR must be set}"
failures=0

fail() {
    echo "FAIL: $1"
    failures=$((failures + 1))
}

expect_names() {
    local label="$1" want="$2" got
    if ! got="$("$names" "$tmp/manifest" 2>&1)"; then
        fail "$label should be read, exited nonzero: $got"
        return
    fi
    if [ "$got" != "$want" ]; then
        fail "$label yields $(printf '%q' "$got"), wanted $(printf '%q' "$want")"
    fi
}

printf 'brenn_replay.wasm\nbrenn_processor_demo.wasm\n' > "$tmp/manifest"
expect_names "two plain entries" 'brenn_replay.wasm
brenn_processor_demo.wasm'

# File order, not sorted: the assembly reports the first bad name it reaches.
printf 'b.wasm\na.wasm\n' > "$tmp/manifest"
expect_names "entries in file order" 'b.wasm
a.wasm'

printf '# a header comment\n\nbrenn_replay.wasm  # why it ships\n\n' > "$tmp/manifest"
expect_names "comments and blank lines" 'brenn_replay.wasm'

printf '   brenn_replay.wasm\t\n' > "$tmp/manifest"
expect_names "surrounding whitespace" 'brenn_replay.wasm'

printf 'brenn_replay.wasm\r\nbrenn_processor_demo.wasm\r\n' > "$tmp/manifest"
expect_names "CRLF line endings" 'brenn_replay.wasm
brenn_processor_demo.wasm'

# The edit most likely to have just been appended.
printf 'brenn_replay.wasm\nbrenn_processor_demo.wasm' > "$tmp/manifest"
expect_names "a final line with no newline" 'brenn_replay.wasm
brenn_processor_demo.wasm'

# Emptiness is the caller's to judge, so it is reported as no entries rather
# than as an error.
: > "$tmp/manifest"
expect_names "an empty file" ''

printf '# only comments\n\n   \n' > "$tmp/manifest"
expect_names "a file with no entries" ''

# Preconditions. A missing manifest must not read as empty: every caller treats
# no entries as a manifest that stopped being read, and would then name the
# wrong cause.
if out=$("$names" "$tmp/absent" 2>&1); then
    fail "a manifest that does not exist should be rejected, exited 0: $out"
elif ! printf '%s' "$out" | grep -qF "not a readable file"; then
    fail "the rejection does not say what went wrong: $out"
fi

if out=$("$names" 2>&1); then
    fail "no manifest argument should be rejected, exited 0: $out"
elif ! printf '%s' "$out" | grep -qF "usage:"; then
    fail "the usage error does not state the usage: $out"
fi

if [ "$failures" -ne 0 ]; then
    echo "$failures case(s) failed"
    exit 1
fi
echo "manifest_names: all cases passed"
