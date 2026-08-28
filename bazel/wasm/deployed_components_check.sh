#!/usr/bin/env bash
# Assert every artifact the deploy manifest names is built, and packaged.
#
# Usage: deployed_components_check.sh <names-tool> <space-joined basenames>
#                                     <space-joined packaged basenames>
#                                     <manifest> [label]
#
# The manifest is what the packaging step ships; a name in it that no target
# produces ships nothing, and the failure would first appear on the deploy
# target.
#
# Packaging is the second direction. A component reaches a host with a binding
# record beside it or it does not load at all, so a manifest entry with no
# `component_package` target ships an artifact the host refuses — the same
# never-deployed outcome as a missing artifact, one release later.
#
# Packaging is also checked the other way round, which is what makes the two
# lists one set. A package's authored module is staged into the release's module
# root unconditionally, while its artifact and sidecars ship only if the
# manifest names them, so a package the manifest omits puts a module in that
# root standing for a component nobody installed — refused by the release
# contract test, in a message about the module root that says nothing about the
# manifest. It is refused here instead, where the manifest is the subject.
#
# `<names-tool>` is `manifest_names.sh`, which states the manifest's grammar for
# every reader of it.
set -euo pipefail

if [ "$#" -lt 4 ]; then
    echo "usage: $0 <names-tool> <declared basenames> <packaged basenames>" \
         "<manifest> [label]" >&2
    exit 2
fi
names="$1"
declared="$2"
packaged="$3"
manifest="$4"
label="${5:-$manifest}"

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
    if ! echo "$packaged" | tr ' ' '\n' | grep -qx "$line"; then
        echo "ERROR: $label lists $line, which no component_package target packages;" \
             "a component installs with its binding record or the host refuses it"
        missing=1
    fi
done <<< "$listed"

# Every packaged component is a listed one.
while read -r name; do
    [ -n "$name" ] || continue
    if ! echo "$listed" | grep -qx "$name"; then
        echo "ERROR: $name has a component_package target but $label does not list it;" \
             "its authored module would stage into the release's module root with no" \
             "component installed beside it"
        missing=1
    fi
done <<< "$(echo "$packaged" | tr ' ' '\n')"

# A manifest that yields nothing is a manifest that stopped being read.
if [ "$entries" -eq 0 ]; then
    echo "ERROR: $label lists no components — the deploy manifest is empty or unreadable"
    exit 1
fi

exit "$missing"
