#!/usr/bin/env bash
# The workspace install tree holds what a host reads.
#
# A server run from the workspace names this directory as a `component_path`
# and loads a component through the same package verification a deployment
# does: the artifact, its record and the packaged module, flat and side by side.
# The rule that stages them declares its outputs, so a tree missing half of
# them builds green, and the only thing that would notice is a boot panic in
# `make e2e` — a target skipped locally, reporting a configuration error for
# what is a staging bug. So the shape is asserted here instead, over the built
# tree, with the same names the gate on the release tree derives.
set -uo pipefail

package_names="$1"
record_lib="$2"
shift 2
# shellcheck source=/dev/null
. "$record_lib"
failures=0

fail() {
    echo "FAIL: $1"
    failures=$((failures + 1))
}

artifacts=0
for path in "$@"; do
    case "$path" in
        *.wasm) ;;
        *) continue ;;
    esac
    artifacts=$((artifacts + 1))
    dir="$(dirname "$path")"
    { read -r record_name; read -r spec_name; } <<< "$("$package_names" "$(basename "$path")")"
    record="$dir/$record_name"
    if [ ! -s "$record" ]; then
        fail "$path has no $record_name beside it; the host refuses an artifact whose record did not travel with it"
        continue
    fi
    # A record that names a specification requires that file beside it in the
    # `component_path` directory.
    stated="$(record_field "$record" spec)"
    if [ -z "$stated" ]; then
        continue
    fi
    if [ "$stated" != "$spec_name" ]; then
        fail "$record_name names $stated as its spec, but the host derives that name as $spec_name and reads no other file"
        continue
    fi
    [ -s "$dir/$spec_name" ] || fail "$record_name binds $spec_name, which is not staged beside it"
done

# An empty tree passes every assertion above by having nothing to assert.
if [ "$artifacts" -eq 0 ]; then
    fail "the install tree stages no component artifact at all"
fi

if [ "$failures" -ne 0 ]; then
    echo "$failures case(s) failed"
    exit 1
fi
echo "ok"
