#!/usr/bin/env bash
# Drive tree_parity_check.sh over fixtures, in both drift directions and over
# the empty tree that would otherwise make the comparison vacuous.
set -euo pipefail

check="$(realpath "$1")"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
cd "$work"

fail() {
    echo "FAIL: $1" >&2
    exit 1
}

# committed/ is the reference; each generated tree below differs from it in one
# way.
mkdir -p committed same missing extra changed nested empty
printf 'export type A = string;\n' >committed/A.ts
printf 'export type B = number;\n' >committed/B.ts

cp committed/A.ts committed/B.ts same/
cp committed/A.ts missing/
cp committed/A.ts committed/B.ts extra/
printf 'export type C = boolean;\n' >extra/C.ts
cp committed/A.ts changed/
printf 'export type B = string;\n' >changed/B.ts
cp committed/A.ts nested/
mkdir nested/sub
cp committed/B.ts nested/sub/B.ts

if ! "$check" committed same //frontend:generated >/dev/null; then
    fail "an identical tree was rejected"
fi

if "$check" committed missing //frontend:generated >/dev/null 2>&1; then
    fail "a generated tree missing a committed file was accepted"
fi

if "$check" committed extra //frontend:generated >/dev/null 2>&1; then
    fail "a generated tree with an uncommitted file was accepted"
fi

if "$check" committed changed //frontend:generated >/dev/null 2>&1; then
    fail "a generated file with different bytes was accepted"
fi

if "$check" committed nested //frontend:generated >/dev/null 2>&1; then
    fail "a generated tree with a different layout was accepted"
fi

if "$check" committed empty //frontend:generated >/dev/null 2>&1; then
    fail "an empty generated tree was accepted"
fi

if "$check" committed not_a_directory //frontend:generated >/dev/null 2>&1; then
    fail "a missing generated directory was accepted"
fi

echo "PASS"
