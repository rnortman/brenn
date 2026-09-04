#!/usr/bin/env bash
# The library-module gate's other direction.
#
# `library_module_test` is the only thing in the graph that compiles a library
# module — no component owns one, so nothing else reads it before a deployment's
# boot does. A gate that went permanently green would leave that boot as the
# first reader, on a target host with the service stopped. So the refusal is
# driven here, over a module that declares a top-level channel: the packaged
# subset admits vocabulary and instantiates nothing.
#
# A sibling test rather than an `expect_failure` knob on the macro, for the
# reason the fit gate's sibling gives: a macro that can be told to want a
# failure is one an author can leave pointed the wrong way.
set -uo pipefail

dsl_cli="$1"
root="$2"
module="$3"

if out=$("$dsl_cli" check --modules "$(dirname "$module")" "$root" 2>&1); then
    echo "FAIL: a library module that instantiates was accepted: $out"
    exit 1
fi
if ! printf '%s' "$out" | grep -qF -e "instantiates nothing"; then
    echo "FAIL: the refusal does not name the discipline: $out"
    exit 1
fi
echo "library_module refusal: the gate refuses a module that instantiates"
