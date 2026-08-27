#!/usr/bin/env bash
# Emit the boot-validation manifest for one jco-transpiled processor kind.
#
# Usage: emit-processor-manifest.sh <kind> <source-component.wasm> <out-dir> \
#            <jco-version> <spec-path>
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
#
# `spec_sha256` binds the tree to the specification the component was authored
# against, the same way the backend package record binds a component to its
# spec: the deployment's configuration compiled against exactly those bytes, so
# byte equality at boot carries every compile-time check over to the installed
# artifact. The specification is named by its path alone and the name is taken
# from it: the boot reader derives `<kind>.spec.brenn` and refuses anything
# else, so a caller that could state a name and a path separately could write a
# record hashing one file and naming another. It is copied into the staged tree
# before this runs, so it joins the observed `files` list.
set -euo pipefail

kind="$1"
component="$2"
out_dir="$3"
jco_version="$4"
spec_path="$5"

# The one name the boot reader will accept, and the one this record may state.
spec_name="$(basename "$spec_path")"
if [ "$spec_name" != "$kind.spec.brenn" ]; then
    echo "emit-processor-manifest: $kind packages its specification as $spec_name, but the" \
         "reader derives $kind.spec.brenn and reads no other file." >&2
    exit 1
fi

sha=$(sha256sum "$component" | awk '{print $1}')
spec_sha=$(sha256sum "$spec_path" | awk '{print $1}')

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
    printf '  "v": 2,\n'
    printf '  "kind": "%s",\n' "$(json_escape "$kind")"
    printf '  "source_sha256": "%s",\n' "$(json_escape "$sha")"
    printf '  "jco_version": "%s",\n' "$(json_escape "$jco_version")"
    printf '  "spec": "%s",\n' "$(json_escape "$spec_name")"
    printf '  "spec_sha256": "%s",\n' "$(json_escape "$spec_sha")"
    printf '  "imports": '
    printf '%s\n' "$imports" | json_array
    printf ',\n'
    printf '  "files": '
    printf '%s\n' "$files" | json_array
    printf '\n}\n'
} > "$out_dir/manifest.json"
