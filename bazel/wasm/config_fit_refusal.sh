#!/usr/bin/env bash
# One document that must not compile, and the refusal it must earn.
#
# The gate logic lives in a checked-in script rather than in the generated
# wrapper so the two conditions — a non-zero status, and a refusal that is about
# this case rather than about a typo in the fixture — are read where they are
# written.
set -uo pipefail

dsl_cli="$1"
expect="$2"
shift 2

if out=$("$dsl_cli" "$@" 2>&1); then
    echo "FAIL: a document that must not compile was accepted: $out"
    exit 1
fi
if ! printf '%s' "$out" | grep -qF -e "$expect"; then
    echo "FAIL: the refusal is not the one this case is about."
    echo "  expected to find: $expect"
    echo "  got: $out"
    exit 1
fi
echo "config_fit refusal: the gate refuses the document, naming $expect"
