#!/usr/bin/env bash
# Liveness proof for the served-tree file-set gate.
#
# The gate passes over the real dist trees, which says nothing about whether it
# would notice a file going missing. Here the trees are fixtures: an exact match
# passes, a missing file and an extra file are each rejected naming the path, a
# symlinked entry is followed (a test's runfiles are symlinks, and an unfollowed
# walk would compare two empty sets), and an empty expected list or an empty
# tree is rejected rather than reported clean.
set -uo pipefail

check="$1"
tmp="${TEST_TMPDIR:?TEST_TMPDIR must be set}"
failures=0

fail() {
    echo "FAIL: $1"
    failures=$((failures + 1))
}

mkdir -p "$tmp/tree/nested"
printf 'a\n' > "$tmp/tree/one.js"
printf 'b\n' > "$tmp/tree/nested/two.css"

cat > "$tmp/expected.txt" <<'EOF'
# The served set.
nested/two.css

one.js
EOF

if ! "$check" "$tmp/tree" "$tmp/expected.txt" dist > "$tmp/match.log" 2>&1; then
    fail "an exact match should pass: $(cat "$tmp/match.log")"
fi

# A file the tree has and the list does not.
printf 'c\n' > "$tmp/tree/stray.map"
if out=$("$check" "$tmp/tree" "$tmp/expected.txt" dist 2>&1); then
    fail "an unlisted file should be rejected, exited 0: $out"
elif ! printf '%s' "$out" | grep -qF "stray.map"; then
    fail "the rejection does not name the unlisted file: $out"
fi
rm "$tmp/tree/stray.map"

# A file the list has and the tree does not: the shipping failure mode.
mv "$tmp/tree/one.js" "$tmp/one.js.stash"
if out=$("$check" "$tmp/tree" "$tmp/expected.txt" dist 2>&1); then
    fail "a missing file should be rejected, exited 0: $out"
elif ! printf '%s' "$out" | grep -qF "one.js"; then
    fail "the rejection does not name the missing file: $out"
fi
mv "$tmp/one.js.stash" "$tmp/tree/one.js"

# A tree of symlinks, as a test's runfiles are.
mkdir -p "$tmp/linked/nested"
ln -s "$tmp/tree/one.js" "$tmp/linked/one.js"
ln -s "$tmp/tree/nested/two.css" "$tmp/linked/nested/two.css"
if ! "$check" "$tmp/linked" "$tmp/expected.txt" dist > "$tmp/linked.log" 2>&1; then
    fail "a symlinked tree should pass: $(cat "$tmp/linked.log")"
fi

# An expected list with nothing in it asserts nothing.
printf '# only a comment\n\n' > "$tmp/empty.txt"
if out=$("$check" "$tmp/tree" "$tmp/empty.txt" dist 2>&1); then
    fail "an empty expected list should be rejected, exited 0: $out"
elif ! printf '%s' "$out" | grep -qF "names no paths"; then
    fail "the rejection does not say the list is empty: $out"
fi

# A tree that was never built.
mkdir -p "$tmp/unbuilt"
if out=$("$check" "$tmp/unbuilt" "$tmp/expected.txt" dist 2>&1); then
    fail "an empty tree should be rejected, exited 0: $out"
elif ! printf '%s' "$out" | grep -qF "holds no files"; then
    fail "the rejection does not say the tree is empty: $out"
fi

# A path that is not a directory at all.
if out=$("$check" "$tmp/expected.txt" "$tmp/expected.txt" dist 2>&1); then
    fail "a non-directory tree should be rejected, exited 0: $out"
elif ! printf '%s' "$out" | grep -qF "not a directory"; then
    fail "the rejection does not say what went wrong: $out"
fi

if [ "$failures" -ne 0 ]; then
    echo "$failures case(s) failed"
    exit 1
fi
echo "path_set_check: all cases passed"
