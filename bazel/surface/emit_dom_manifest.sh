#!/usr/bin/env bash
# Emit one dom kind's binding record, and the packaged copy of its
# specification.
#
# Usage: emit_dom_manifest.sh <kind> <module.js> <module_bg.wasm> \
#            <spec-in> <record-out> <spec-out>
#
# WIT_LIB names the shared library holding the JSON escaping every record
# emitter writes with; DOM_NAMES names the script stating the dom file grammar.
# Unset, both are read from their paths relative to the execroot.
#
# A dom kind ships flat in the asset root as a wasm-bindgen `--target web`
# module pair, so its record is a file beside that pair rather than a file
# inside a per-kind directory: `brenn_<kind>.manifest.json`, named from the
# artifact stem like every other dom sidecar, because the root is shared and the
# merge refuses two stages writing one path.
#
# The record states three files and hashes all three. The pair's hashes are the
# staleness check — a module loaded from one release beside a record from
# another is a refusal to boot rather than a page that misbehaves — and
# `spec_sha256` binds the tree to the specification the component was authored
# against, which is what a configured instance's own spec hash is checked
# against.
#
# The shared `snippets/` tree is deliberately unhashed: wasm-bindgen attributes
# it to whichever crate emits the inline JS, not to the kind that links it, so
# no per-kind record can state it truthfully. The `.d.ts` and documentation
# sidecars are unhashed too — nothing loads them at boot.
set -euo pipefail

if [ "$#" -ne 6 ]; then
    echo "usage: $0 <kind> <module.js> <module_bg.wasm> <spec-in> <record-out> <spec-out>" >&2
    exit 2
fi

kind="$1"
module="$2"
module_wasm="$3"
spec_in="$4"
record_out="$5"
spec_out="$6"

# The four names are asked of the grammar, never spelled here, and every path
# this rule was handed is held to them: a rule wired to the wrong bundle fails
# at the emit rather than at somebody's boot.
# Assigned before being read: a grammar failure inside a process substitution
# is invisible to `set -e`, and every name would then compare against empty.
names="$("${DOM_NAMES:-bazel/surface/dom_names.sh}" "$kind")"
{
    read -r want_module
    read -r want_module_wasm
    read -r want_record
    read -r want_spec
} <<< "$names"

module_name="$(basename "$module")"
module_wasm_name="$(basename "$module_wasm")"
spec_name="$(basename "$spec_out")"
record_name="$(basename "$record_out")"

check_name() {
    if [ "$2" != "$3" ]; then
        echo "emit_dom_manifest: $kind names its $1 $2, but a dom kind's files all hang off one" \
             "stem, so it must be $3." >&2
        exit 1
    fi
}
check_name module "$module_name" "$want_module"
check_name "module wasm" "$module_wasm_name" "$want_module_wasm"
check_name record "$record_name" "$want_record"
check_name specification "$spec_name" "$want_spec"

for file in "$module" "$module_wasm" "$spec_in"; do
    if [ ! -f "$file" ]; then
        echo "emit_dom_manifest: $file is not a readable file" >&2
        exit 1
    fi
done

module_sha="$(sha256sum "$module" | awk '{print $1}')"
module_wasm_sha="$(sha256sum "$module_wasm" | awk '{print $1}')"
spec_sha="$(sha256sum "$spec_in" | awk '{print $1}')"

. "${WIT_LIB:-bazel/wasm/wit_lib.sh}"

# One `"key": "value"` per line, the shape the shell-side record scrape reads:
# the staged-tree gate re-verifies these hashes without a JSON parser, and a
# record it cannot scrape is indistinguishable to it from a field never stated.
{
    printf '{\n'
    printf '  "v": 1,\n'
    printf '  "kind": "%s",\n' "$(json_escape "$kind")"
    printf '  "module": "%s",\n' "$(json_escape "$module_name")"
    printf '  "module_sha256": "%s",\n' "$(json_escape "$module_sha")"
    printf '  "module_wasm": "%s",\n' "$(json_escape "$module_wasm_name")"
    printf '  "module_wasm_sha256": "%s",\n' "$(json_escape "$module_wasm_sha")"
    printf '  "spec": "%s",\n' "$(json_escape "$spec_name")"
    printf '  "spec_sha256": "%s"\n' "$(json_escape "$spec_sha")"
    printf '}\n'
} > "$record_out"

# -L, because the action's inputs are staged as symlinks out of the sandbox.
# The packaged copy is the author's file byte for byte, so the hash in the
# record is the hash of what boot reads.
cp -L "$spec_in" "$spec_out"
chmod u+w "$spec_out"
