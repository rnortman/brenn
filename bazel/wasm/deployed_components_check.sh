#!/usr/bin/env bash
# Assert every artifact the deploy manifest names is actually built.
#
# Usage: deployed_components_check.sh <names-tool> <space-joined basenames>
#                                     <manifest> [label]
#
# The manifest is what the packaging step ships; a name in it that no target
# produces ships nothing, and the failure would first appear on the deploy
# target.
#
# `<names-tool>` is `manifest_names.sh`, which states the manifest's grammar for
# every reader of it.
set -euo pipefail

if [ "$#" -lt 3 ]; then
    echo "usage: $0 <names-tool> <declared basenames> <manifest> [label]" >&2
    exit 2
fi
names="$1"
declared="$2"
manifest="$3"
label="${4:-$manifest}"

missing=0
entries=0
listed="$("$names" "$manifest")"
while read -r line; do
    [ -n "$line" ] || continue
    entries=$((entries + 1))
    if ! echo "$declared" | tr ' ' '\n' | grep -qx "$line"; then
        echo "ERROR: $label lists $line, which no wasm_component target produces"
        missing=1
    fi
done <<< "$listed"

# A manifest that yields nothing is a manifest that stopped being read.
if [ "$entries" -eq 0 ]; then
    echo "ERROR: $label lists no components — the deploy manifest is empty or unreadable"
    exit 1
fi

exit "$missing"
