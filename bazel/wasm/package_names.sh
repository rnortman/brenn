#!/usr/bin/env bash
# A component package's file names, in one place.
#
# Usage: package_names.sh <artifact-basename>
#
# Prints the two sidecar basenames a package holds beside its artifact, in this
# order and one per line:
#
#     <stem>.package.json    the binding record, always present
#     <stem>.spec.brenn      the packaged specification, for a processor world
#
# Whether the spec is *there* is the world's business and each caller's to
# decide; what is stated here is only what it would be called.
#
# Four readers act on this grammar — the assembly that stages a package into the
# release tree, the gate on the staged tree, the emitter's Starlark, and the
# host that reads the sidecars at boot — and the flat layout is provisional: the
# package becomes a directory when configuration resolves components by name.
# Readers that agree only by inspection disagree the first time that happens,
# and a shell reader that disagrees copies nothing rather than failing to
# compile.
set -euo pipefail

if [ "$#" -ne 1 ]; then
    echo "usage: $0 <artifact-basename>" >&2
    exit 2
fi
artifact="$1"

case "$artifact" in
    *.wasm) ;;
    *)
        echo "ERROR: $artifact is not a component artifact; a package is named for its .wasm" >&2
        exit 1
        ;;
esac

stem="${artifact%.wasm}"
printf '%s.package.json\n' "$stem"
printf '%s.spec.brenn\n' "$stem"
