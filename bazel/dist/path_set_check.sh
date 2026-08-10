#!/usr/bin/env bash
# Assert a built asset tree holds exactly the paths a checked-in list names.
#
# Usage: path_set_check.sh <tree-dir> <expected-list> <label>
#
# What reaches the browser is a file set, and every way of losing a file from it
# — a `srcs` entry dropped, a stage left out of a merge, a rename that only half
# landed — produces a green build and a 404 at page load. Comparing against a
# list a human edits is what makes an addition or a removal a reviewable act
# rather than a diff nobody sees. The list is sorted, one workspace-agnostic
# tree-relative path per line; blank lines and `#` comments are ignored.
set -euo pipefail

if [ "$#" -ne 3 ]; then
    echo "usage: $0 <tree-dir> <expected-list> <label>" >&2
    exit 2
fi
tree="$1"
expected_file="$2"
label="$3"

if [ ! -d "$tree" ]; then
    echo "ERROR: $tree is not a directory; the comparison would assert nothing."
    exit 1
fi

# -L, because a test's runfiles and input trees are staged as symlinks: an
# unfollowed walk finds no regular files and compares an empty set, which
# passes over anything.
actual="$(cd "$tree" && find -L . -type f -printf '%P\n' | LC_ALL=C sort)"
# `|| true`: a list with nothing but comments matches no line, and grep's exit 1
# for that is the case the emptiness check below exists to report.
expected="$(grep -v '^[[:space:]]*\(#.*\)\?$' "$expected_file" | LC_ALL=C sort || true)"

if [ -z "$expected" ]; then
    echo "ERROR: $expected_file names no paths, so it asserts nothing about $label."
    exit 1
fi
if [ -z "$actual" ]; then
    echo "ERROR: $tree holds no files; $label was not built."
    exit 1
fi

if [ "$actual" != "$expected" ]; then
    echo "ERROR: the $label tree does not hold the paths $expected_file names."
    echo "Update the list deliberately if the change is intended."
    diff -u <(echo "$expected") <(echo "$actual") \
        --label "expected ($expected_file)" --label "built ($label)" || true
    exit 1
fi
