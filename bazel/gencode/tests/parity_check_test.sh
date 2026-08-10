#!/usr/bin/env bash
# Drive parity_check.sh over fixtures. A gate that passes over every real input
# still says nothing about whether it can fail.
set -euo pipefail

check="$(realpath "$1")"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
cd "$work"

fail() {
    echo "FAIL: $1" >&2
    exit 1
}

printf 'a\nb\n' >committed
printf 'a\nb\n' >same
printf 'a\nc\n' >differs
mkdir a_directory

if ! "$check" committed same //frontend:committed >/dev/null; then
    fail "identical bytes were rejected"
fi

if "$check" committed differs //frontend:committed >/dev/null 2>&1; then
    fail "differing bytes were accepted"
fi

if "$check" committed missing //frontend:committed >/dev/null 2>&1; then
    fail "a missing generated file was accepted"
fi

if "$check" a_directory same //frontend:committed >/dev/null 2>&1; then
    fail "a directory in place of the committed file was accepted"
fi

if "$check" committed same >/dev/null 2>&1; then
    fail "a call with too few arguments was accepted"
fi

echo "PASS"
