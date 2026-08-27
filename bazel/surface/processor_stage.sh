#!/usr/bin/env bash
# Assemble one processor kind's slice of the surface asset tree.
#
# Usage: processor_stage.sh <kind> <component.wasm> <transpiled-dir> \
#            <jco-version-file> <spec.brenn> <emitter> <out-dir>
#
# The output holds `processor/<kind>/`: jco's transpiled module tree, the
# component bytes it came from (copied beside the output so boot validation
# verifies provenance against the actual bytes), a verbatim copy of the
# component's authored specification, and the manifest, written last because it
# lists the files it sits among.
#
# WASM_TOOLS points the emitter at the pinned binary; without it the emitter
# falls back to a `wasm-tools` on PATH, which no sandbox carries. WIT_LIB points
# it at the shared import scrape the same way. Both are read from the
# environment by the emitter, so this script only has to pass them through.
set -euo pipefail

kind="$1"
component="$2"
transpiled="$3"
version_file="$4"
spec="$5"
emitter="$6"
out="$7"

version=$(cat "$version_file")
if [ -z "$version" ]; then
    echo "processor_stage: empty jco version in $version_file" >&2
    exit 1
fi

dest="$out/processor/$kind"
mkdir -p "$dest"

# -L, because the action's inputs are staged as symlinks out of the sandbox:
# copying them as symlinks would put dangling links in the output tree, and the
# manifest's file list — which walks this directory — would miss them.
cp -RL "$transpiled/." "$dest/"
cp "$component" "$dest/$kind.component.wasm"
chmod u+w "$dest/$kind.component.wasm"

# Copied before the emitter runs, so the emitter's observed file walk lists it
# and boot validation checks its existence along with every transpiled file.
spec_name="$kind.spec.brenn"
cp -L "$spec" "$dest/$spec_name"
chmod u+w "$dest/$spec_name"

"$emitter" "$kind" "$component" "$dest" "$version" "$dest/$spec_name"
