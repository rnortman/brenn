#!/usr/bin/env bash
# Assemble one processor kind's slice of the surface asset tree.
#
# Usage: processor_stage.sh <kind> <component.wasm> <transpiled-dir> \
#            <jco-version-file> <spec.brenn> <emitter> <dsl-cli> <out-dir> \
#            --stage-lib FILE [--help FILE] [--schema FILE]
#
# The output holds `processor/<kind>/`: jco's transpiled module tree, the
# component bytes it came from (copied beside the output so boot validation
# verifies provenance against the actual bytes), a verbatim copy of the
# component's authored specification, and the manifest, written last because it
# lists the files it sits among.
#
# `--help` and `--schema` are the kind's documentation sidecars. They land at the
# output's root as `brenn_<kind_underscored>.help.md` / `.schema.json`, beside
# `processor/` rather than in it: the server reads a kind's sidecars from the root
# that serves it, under the stem `sidecar_stem` spells, and the record's file list
# must equal `processor/<kind>/`'s contents. `--stage-lib` is
# `bazel/release/stage_lib.sh`, whose `stage_sidecar_stem` is the stem the
# bundle assembler and the release checker hold these names to.
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
dsl_cli="$7"
out="$8"
shift 8

stage_lib=""
help=""
schema=""
while [ "$#" -gt 0 ]; do
    case "$1" in
        --stage-lib) stage_lib="$2"; shift 2 ;;
        --help) help="$2"; shift 2 ;;
        --schema) schema="$2"; shift 2 ;;
        *) echo "processor_stage: unrecognized argument: $1" >&2; exit 2 ;;
    esac
done
if [ -z "$stage_lib" ]; then
    echo "processor_stage: --stage-lib is required" >&2
    exit 2
fi
# shellcheck source=/dev/null
. "$stage_lib"

# The served kind is the kebab fold of the specification's component class name;
# the directory staged here is the string the BUILD author typed. Nothing
# downstream compares them, so a mismatch would build green and leave the page
# asking for a directory that does not exist.
expected=$("$dsl_cli" wire-kind "$spec")
if [ "$kind" != "$expected" ]; then
    echo "processor_stage: kind \"$kind\" is not the wire kind of the class in $spec, which is \"$expected\"" >&2
    exit 1
fi

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

stem="$(stage_sidecar_stem "$kind")"
if [ -n "$help" ]; then
    cp -L "$help" "$out/$stem.help.md"
    chmod u+w "$out/$stem.help.md"
fi
if [ -n "$schema" ]; then
    cp -L "$schema" "$out/$stem.schema.json"
    chmod u+w "$out/$stem.schema.json"
fi
