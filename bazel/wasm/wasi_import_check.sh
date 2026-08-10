#!/usr/bin/env bash
# Assert a WIT component imports nothing from `wasi:`.
#
# Usage: wasi_import_check.sh <wasm-tools> <component.wasm>
#
# The host's wasmtime linker provides no WASI, so a component that acquires a
# WASI import fails to instantiate at runtime rather than at build time.
#
# The transcript shape is asserted before the pattern is applied: a grep that
# finds nothing over empty or reshaped output is indistinguishable from a grep
# that finds nothing over a clean component, and a gate that cannot tell those
# apart asserts nothing.
set -euo pipefail

if [ "$#" -ne 2 ]; then
    echo "usage: $0 <wasm-tools> <component.wasm>" >&2
    exit 2
fi
wasm_tools="$1"
component="$2"

wit="$("$wasm_tools" component wit "$component")"

case "$wit" in
    *"world "*) ;;
    *)
        echo "ERROR: '$wasm_tools component wit $component' printed no world."
        echo "The WASI pattern below would match nothing whatever the component imports."
        echo "--- transcript ---"
        echo "$wit"
        exit 1
        ;;
esac

if echo "$wit" | grep -q 'import wasi:'; then
    echo "ERROR: $component imports wasi:* — WASI must not appear in the component"
    echo "$wit" | grep 'import wasi:'
    exit 1
fi
