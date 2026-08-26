#!/usr/bin/env bash
# Liveness proof for a component package's file grammar.
#
# Four readers act on this one script — the release assembly, the staged-tree
# gate, the emitter's Starlark, and the host that reads the sidecars at boot —
# and two of them consume its output positionally, one line each. The order the
# two names are printed in is therefore a join key: swap it and every record is
# staged under its spec's name, with nothing else in the build noticing.
set -uo pipefail

package_names="$1"
failures=0

fail() {
    echo "FAIL: $1"
    failures=$((failures + 1))
}

expect_names() {
    local label="$1" artifact="$2" want="$3" got
    if ! got="$("$package_names" "$artifact" 2>&1)"; then
        fail "$label should be named, exited nonzero: $got"
        return
    fi
    if [ "$got" != "$want" ]; then
        fail "$label yields $(printf '%q' "$got"), wanted $(printf '%q' "$want")"
    fi
}

# The record first, the spec second — the order `assemble.sh` and
# `package_check.sh` read them in.
expect_names "a shipped component" brenn_processor_demo.wasm \
    'brenn_processor_demo.package.json
brenn_processor_demo.spec.brenn'

# Only the final extension is the artifact's; a stem holding dots keeps them.
expect_names "a stem holding dots" my.component.v2.wasm \
    'my.component.v2.package.json
my.component.v2.spec.brenn'

# The names are computed from the basename alone, so a path is a caller error
# rather than something to strip: stripping it would print sidecar names that
# resolve nowhere near the artifact.
expect_names "a stem that is only an extension" .wasm \
    '.package.json
.spec.brenn'

# Preconditions. A caller that hands over something other than an artifact must
# hear about the argument, not about a package that does not exist.
if out=$("$package_names" brenn_processor_demo.spec.brenn 2>&1); then
    fail "a name that is not an artifact should be rejected, exited 0: $out"
elif ! printf '%s' "$out" | grep -qF "not a component artifact"; then
    fail "the rejection does not say what went wrong: $out"
fi

# A wrong argument count is the usage status, distinct from the refusal above:
# the callers pass exactly one name and a second one means a caller changed.
expect_usage() {
    local label="$1"
    shift
    local out status
    out="$("$package_names" "$@" 2>&1)"
    status=$?
    if [ "$status" -ne 2 ]; then
        fail "$label should exit 2, the usage status; exited $status: $out"
    elif ! printf '%s' "$out" | grep -qF "usage:"; then
        fail "$label: the usage error does not state the usage: $out"
    fi
}

expect_usage "no argument"
expect_usage "two arguments" a.wasm b.wasm

if [ "$failures" -ne 0 ]; then
    echo "$failures case(s) failed"
    exit 1
fi
echo "package_names: all cases passed"
