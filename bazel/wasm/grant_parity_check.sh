#!/usr/bin/env bash
# Assert a component's authored specification and its built artifact agree about
# what the component needs.
#
# Usage: grant_parity_check.sh <wasm-tools> <wit-lib> <dsl-cli> <artifact> <spec>
#
# The scrape is the shared one every other reader of an artifact's imports uses;
# the comparison is Rust, where the grant word to WIT interface mapping is
# single-sourced with the host's own linker gating. Neither half knows what the
# other knows: this script holds no grant vocabulary, and the CLI shells out to
# no toolchain.
set -euo pipefail

if [ "$#" -ne 5 ]; then
    echo "usage: $0 <wasm-tools> <wit-lib> <dsl-cli> <artifact> <spec>" >&2
    exit 2
fi
wasm_tools="$1"
wit_lib="$2"
dsl_cli="$3"
artifact="$4"
spec="$5"

export WASM_TOOLS="$wasm_tools"
# shellcheck source=/dev/null
. "$wit_lib"

imports="$(mktemp)"
trap 'rm -f "$imports"' EXIT
# Versions kept: the comparison judges an import against the exact canonical
# name the host links, so a stripped list would hide a version drift the host
# refuses at load.
wit_imports_versioned "$artifact" > "$imports"

# The CLI's own rendered diagnostic is the diagnosis — it distinguishes drift
# from a specification this check cannot read at all, and this script cannot.
# All that is added here is the input the diagnostic was formed against, which
# is not reproducible from a Bazel log otherwise.
if ! "$dsl_cli" grant-parity --spec "$spec" --imports "$imports"; then
    echo
    echo "The check above refused this component; its message says why. The import list it"
    echo "was given, scraped from the built artifact:"
    echo "--- $artifact imports ---"
    cat "$imports"
    exit 1
fi
