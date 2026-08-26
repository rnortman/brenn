#!/usr/bin/env bash
# Emit the boot-validation manifest for one jco-transpiled processor kind.
#
# Usage: emit-processor-manifest.sh <kind> <source-component.wasm> <out-dir> <jco-version>
#
# WASM_TOOLS names the `wasm-tools` binary; unset, one is looked up on PATH.
# WIT_LIB names the shared WIT-scraping library; unset, it is read from its path
# relative to the execroot.
#
# The manifest binds the transpiled tree to the component bytes it came from:
# `source_sha256` is the hash of the transpile's input, which the server
# re-computes at boot against the copied artifact, so a component rebuilt
# without re-transpiling fails the deploy instead of the page load. `imports` is
# read out of the artifact itself (never hand-written) and is the import profile
# boot validation checks against the transpilable set.
set -euo pipefail

kind="$1"
component="$2"
out_dir="$3"
jco_version="$4"

sha=$(sha256sum "$component" | awk '{print $1}')

# The world's import list, fully qualified — package namespace included — so
# the server's profile check can reject a foreign-namespace import (a stray
# `wasi:*` pulled in by a dependency) rather than have it silently vanish from
# the profile and resurface as a page-load instantiation failure. The scrape and
# its completeness guard are shared with the component-package emitter.
. "${WIT_LIB:-bazel/wasm/wit_lib.sh}"
imports=$(wit_imports "$component")

# Every emitted file except the manifest itself (which cannot list itself) —
# jco's output set is version-dependent, so the list is observed, not predicted.
files=$(cd "$out_dir" && find . -type f ! -name manifest.json | sed 's|^\./||' | LC_ALL=C sort)

# Read newline-separated stdin into a JSON string array. Callers must feed a
# trailing newline (printf '%s\n'): `read` returns non-zero on an unterminated
# final line, which would silently drop the last entry.
json_array() {
    local first=1
    printf '['
    while IFS= read -r item; do
        [ -z "$item" ] && continue
        [ $first -eq 1 ] || printf ', '
        printf '"%s"' "$(json_escape "$item")"
        first=0
    done
    printf ']'
}

{
    printf '{\n'
    printf '  "v": 1,\n'
    printf '  "kind": "%s",\n' "$(json_escape "$kind")"
    printf '  "source_sha256": "%s",\n' "$(json_escape "$sha")"
    printf '  "jco_version": "%s",\n' "$(json_escape "$jco_version")"
    printf '  "imports": '
    printf '%s\n' "$imports" | json_array
    printf ',\n'
    printf '  "files": '
    printf '%s\n' "$files" | json_array
    printf '\n}\n'
} > "$out_dir/manifest.json"
