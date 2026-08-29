#!/usr/bin/env bash
# Assert the deploy manifest and the packaged set name one set of components.
#
# Usage: deployed_components_check.sh <names-tool> <space-joined package names>
#                                     <manifest> [label]
#
# The manifest names packages, and a package is the whole of what a host
# resolves a component by: the directory it installs as, the record inside it,
# and the `@<name>` a configuration imports the class from. So a manifest entry
# with no `component_package` target ships nothing at all, and the failure would
# first appear on the deploy target.
#
# The other direction is what makes the two lists one set. A package's authored
# module is staged into the release's module root unconditionally, while the
# package itself ships only if the manifest names it, so a package the manifest
# omits puts a module in that root standing for a component nobody installed —
# refused by the release contract test, in a message about the module root that
# says nothing about the manifest. It is refused here instead, where the
# manifest is the subject.
#
# `<names-tool>` is `manifest_names.sh`, which states the manifest's grammar for
# every reader of it.
set -euo pipefail

if [ "$#" -lt 3 ]; then
    echo "usage: $0 <names-tool> <packaged names> <manifest> [label]" >&2
    exit 2
fi
names="$1"
packaged="$2"
manifest="$3"
label="${4:-$manifest}"

missing=0
entries=0
listed="$("$names" "$manifest")"
while read -r line; do
    [ -n "$line" ] || continue
    entries=$((entries + 1))
    # -F: a name is a fixed string, not a pattern — a `.` in a manifest entry
    # must not match some other package's character at that position.
    if ! echo "$packaged" | tr ' ' '\n' | grep -Fqx "$line"; then
        echo "ERROR: $label lists $line, which no component_package target packages;" \
             "a component resolves by the name of the package it installs as, so a name" \
             "no package answers to installs nothing"
        missing=1
    fi
done <<< "$listed"

# Every packaged component is a listed one.
while read -r name; do
    [ -n "$name" ] || continue
    if ! echo "$listed" | grep -Fqx "$name"; then
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
