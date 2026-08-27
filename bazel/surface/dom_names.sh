#!/usr/bin/env bash
# A dom component kind's file names, in one place.
#
# Usage: dom_names.sh <kind>
#
# Prints the four basenames a dom kind contributes to the flat surface asset
# root, in this order and one per line:
#
#     brenn_<kind>.js               the served ES-module loader
#     brenn_<kind>_bg.wasm          its wasm-bindgen sibling
#     brenn_<kind>.manifest.json    the binding record
#     brenn_<kind>.spec.brenn       the packaged specification
#
# The hyphen→underscore mapping is wasm-bindgen's, derived from the crate name;
# every one of the four names hangs off the same stem.
#
# Three shell/Starlark readers act on this grammar — the record emitter, the
# staged-tree gate, and the dom package rule — and `brenn_surface_contract`'s
# `module_stem`/`dom_record_artifact`/`dom_spec_artifact` state it for the host
# that reads the tree at boot. The layout is provisional: a dom kind becomes a
# directory when out-of-tree components arrive. Readers that agree only by
# inspection disagree the first time it moves, and a shell reader that disagrees
# matches nothing rather than failing to compile.
set -euo pipefail

if [ "$#" -ne 1 ]; then
    echo "usage: $0 <kind>" >&2
    exit 2
fi
kind="$1"

# The charset the host freezes (`is_valid_kind`): a kind outside it names a
# custom element the browser rejects, and here it would name files nothing
# derives the same way twice.
case "$kind" in
    ""|*[!a-z0-9-]*|-*|*--*)
        echo "ERROR: $kind is not a component kind; kinds are ^[a-z0-9][a-z0-9-]*\$ with no -- run" >&2
        exit 1
        ;;
esac

stem="brenn_${kind//-/_}"
printf '%s.js\n' "$stem"
printf '%s_bg.wasm\n' "$stem"
printf '%s.manifest.json\n' "$stem"
printf '%s.spec.brenn\n' "$stem"
