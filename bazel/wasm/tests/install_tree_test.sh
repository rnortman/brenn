#!/usr/bin/env bash
# The workspace components root holds what a host resolves.
#
# A server run from the workspace passes this directory as `--components` and
# resolves a package by name through the same verification a deployment does:
# `<root>/<name>/` holding the record, the artifact the record names, and — for
# a processor world — `<name>.brenn`. The rule that stages them declares its
# outputs, so a tree missing half of them builds green, and the only thing that
# would notice is a boot panic in `make e2e` — a target skipped locally,
# reporting a configuration error for what is a staging bug. So the shape is
# asserted here instead, over the built tree.
set -uo pipefail

record_lib="$1"
shift
# shellcheck source=/dev/null
. "$record_lib"
failures=0

fail() {
    echo "FAIL: $1"
    failures=$((failures + 1))
}

packages=0
for path in "$@"; do
    [ "$(basename "$path")" = package.json ] || continue
    packages=$((packages + 1))
    dir="$(dirname "$path")"
    if [ ! -s "$path" ]; then
        fail "$dir/package.json is empty; the host refuses a package whose record did not travel with it"
        continue
    fi

    # The naming rules are the record library's, shared with the staged
    # release tree's gate; what is asked here is that every file they name is
    # actually in the directory a server would resolve.
    while IFS="$(printf '\t')" read -r kind value; do
        case "$kind" in
            fail) fail "$dir/package.json: $value" ;;
            artifact | spec)
                [ -s "$dir/$value" ] ||
                    fail "$dir/package.json binds $value, which is not staged beside it"
                ;;
        esac
    done < <(package_shape "$dir")
done

# An empty tree passes every assertion above by having nothing to assert.
if [ "$packages" -eq 0 ]; then
    fail "the components root stages no package at all"
fi

if [ "$failures" -ne 0 ]; then
    echo "$failures case(s) failed"
    exit 1
fi
echo "ok"
