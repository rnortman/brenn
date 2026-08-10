#!/usr/bin/env bash
# Merge staged surface asset trees into one output directory.
#
# Usage: merge_stages.sh <out-dir> <stage-dir>...
#
# Overlap between stages is expected: wasm-bindgen emits a crate's inline-JS
# snippet tree into every bundle that links that crate, so several stages carry
# the same `snippets/<crate>-<hash>/` files. Identical bytes merge silently. A
# path that appears twice with *different* bytes does not: which one reaches the
# browser would then depend on the order the stages happen to be listed in, so
# the merge fails instead.
set -euo pipefail

out="$1"
shift

mkdir -p "$out"

for stage in "$@"; do
    if [ ! -d "$stage" ]; then
        echo "merge_stages: $stage is not a directory" >&2
        exit 1
    fi
    # -L, because a sandboxed action's input trees are staged as symlinks: an
    # unfollowed walk sees no regular files at all and merges nothing.
    mapfile -d '' -t rels < <(cd "$stage" && find -L . -type f -print0)
    for rel in ${rels[@]+"${rels[@]}"}; do
        rel="${rel#./}"
        if [ -e "$out/$rel" ]; then
            if ! cmp -s "$stage/$rel" "$out/$rel"; then
                echo "merge_stages: $stage/$rel and the already-staged $rel differ;" \
                     "two surface stages ship different bytes under one name" >&2
                exit 1
            fi
            continue
        fi
        mkdir -p "$out/$(dirname "$rel")"
        cp "$stage/$rel" "$out/$rel"
    done
done
