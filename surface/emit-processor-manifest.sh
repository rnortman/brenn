#!/usr/bin/env bash
# Emit the boot-validation manifest for one jco-transpiled processor kind.
#
# Usage: emit-processor-manifest.sh <kind> <source-component.wasm> <out-dir> <jco-version>
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

# The world's import list. `wasm-tools component wit` prints the artifact's
# resolved WIT; every host interface appears as an `import <pkg>/<iface>[@ver];`
# line. The *fully qualified* name is captured — package namespace included — so
# the server's profile check can reject a foreign-namespace import (a stray
# `wasi:*` pulled in by a dependency) rather than have it silently vanish from
# the profile and resurface as a page-load instantiation failure. Only the
# version suffix and trailing `;` are dropped; the namespace stays.
#
# LC_ALL=C pins byte order, matching the Rust twin's `sort_unstable()` on
# `String`. Without it the collation follows the invoking environment's locale,
# where punctuation weights differ from byte values, so the emitted order — and
# the parity assertion against the twin — would depend on the machine that ran
# the transpile.
wit=$(wasm-tools component wit "$component")
imports=$(printf '%s\n' "$wit" \
    | sed -n 's/^[[:space:]]*import[[:space:]]\{1,\}\([A-Za-z0-9_-]\{1,\}:[^;[:space:]]*\);.*/\1/p' \
    | sed 's/@[^/]*$//' \
    | LC_ALL=C sort -u)

# The capture above requires a `ns:pkg/...` shape. An import the regex does not
# consume is not a no-op: it vanishes from the profile boot validation checks,
# so the artifact ships with an under-reported import list and the omission
# resurfaces as a page-load instantiation failure. Refuse to emit rather than
# emit a manifest that lies about its own artifact.
raw_count=$(printf '%s\n' "$wit" | grep -c '^[[:space:]]*import[[:space:]]' || true)
captured_count=$(printf '%s\n' "$wit" \
    | sed -n 's/^[[:space:]]*import[[:space:]]\{1,\}\([A-Za-z0-9_-]\{1,\}:[^;[:space:]]*\);.*/\1/p' \
    | grep -c . || true)
if [ "$raw_count" -ne "$captured_count" ]; then
    echo "emit-processor-manifest: $component has $raw_count import lines but only" \
         "$captured_count are fully-qualified \`ns:pkg/iface\` imports." >&2
    echo "An unqualified or non-interface import cannot be expressed in the manifest" \
         "profile. Fix the component's world, or extend this emitter and" \
         "processor_component_imports together." >&2
    exit 1
fi

# Every emitted file except the manifest itself (which cannot list itself) —
# jco's output set is version-dependent, so the list is observed, not predicted.
files=$(cd "$out_dir" && find . -type f ! -name manifest.json | sed 's|^\./||' | LC_ALL=C sort)

# Escape a string for embedding as a JSON string literal: backslash and
# double-quote are the two characters that break the literal (a bare `"` ends it,
# a bare `\` starts an escape). Filenames and versions are toolchain-controlled,
# not operator-typed, but the file list is *observed* from jco's output, so its
# characters get the same humility the output-set list already gets.
json_escape() {
    local s="$1"
    s="${s//\\/\\\\}"
    s="${s//\"/\\\"}"
    printf '%s' "$s"
}

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
