#!/usr/bin/env bash
# Liveness proof for the asset-tree merge.
#
# The merge passes over the real stages, which says nothing about whether it
# would notice a collision. Here the stages are fixtures: disjoint trees merge,
# a path present in two stages with identical bytes merges once, the same path
# with different bytes is rejected rather than resolved by listing order, a
# symlinked stage entry is followed (a sandboxed action's inputs are symlinks,
# and an unfollowed walk would merge nothing), and a stage that is not a
# directory fails instead of being skipped.
set -uo pipefail

merge="$1"
tmp="${TEST_TMPDIR:?TEST_TMPDIR must be set}"
failures=0

fail() {
    echo "FAIL: $1"
    failures=$((failures + 1))
}

# Two stages with no overlap.
mkdir -p "$tmp/a/nested" "$tmp/b"
printf 'alpha\n' > "$tmp/a/one.txt"
printf 'nested\n' > "$tmp/a/nested/two.txt"
printf 'beta\n' > "$tmp/b/three.txt"

out="$tmp/disjoint"
if ! "$merge" "$out" "$tmp/a" "$tmp/b" > "$tmp/disjoint.log" 2>&1; then
    fail "disjoint stages should merge: $(cat "$tmp/disjoint.log")"
fi
for rel in one.txt nested/two.txt three.txt; do
    [ -f "$out/$rel" ] || fail "disjoint merge is missing $rel"
done

# The snippets case: the same path in two stages, byte for byte.
mkdir -p "$tmp/same1/snippets" "$tmp/same2/snippets"
printf 'shared\n' > "$tmp/same1/snippets/inline0.js"
printf 'shared\n' > "$tmp/same2/snippets/inline0.js"
if ! "$merge" "$tmp/identical" "$tmp/same1" "$tmp/same2" > "$tmp/identical.log" 2>&1; then
    fail "identical duplicates should merge: $(cat "$tmp/identical.log")"
fi

# The same path with different bytes: which one reaches the browser would
# otherwise depend on the order the stages are listed in.
mkdir -p "$tmp/diff1" "$tmp/diff2"
printf 'one\n' > "$tmp/diff1/clash.js"
printf 'other\n' > "$tmp/diff2/clash.js"
if out=$("$merge" "$tmp/conflict" "$tmp/diff1" "$tmp/diff2" 2>&1); then
    fail "a differing duplicate should be rejected, exited 0: $out"
elif ! printf '%s' "$out" | grep -qF "clash.js"; then
    fail "the rejection does not name the clashing path: $out"
fi

# A stage whose entries are symlinks, as a sandboxed action's inputs are.
mkdir -p "$tmp/linked"
ln -s "$tmp/a/one.txt" "$tmp/linked/linked.txt"
if ! "$merge" "$tmp/symlinks" "$tmp/linked" > "$tmp/symlinks.log" 2>&1; then
    fail "a symlinked stage entry should merge: $(cat "$tmp/symlinks.log")"
fi
[ -f "$tmp/symlinks/linked.txt" ] || fail "the symlinked entry was not merged"

# A stage that is not a directory at all.
printf 'not a stage\n' > "$tmp/regular-file"
if out=$("$merge" "$tmp/notdir" "$tmp/regular-file" 2>&1); then
    fail "a non-directory stage should be rejected, exited 0: $out"
elif ! printf '%s' "$out" | grep -qF "not a directory"; then
    fail "the rejection does not say what went wrong: $out"
fi

if [ "$failures" -ne 0 ]; then
    echo "$failures case(s) failed"
    exit 1
fi
echo "merge_stages: all cases passed"
