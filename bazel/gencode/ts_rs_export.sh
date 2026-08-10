#!/usr/bin/env bash
# Run a crate's ts-rs exporters into a declared output directory.
#
# Usage: ts_rs_export.sh <out-dir> <test-binary> <filter>
#
# ts-rs exports from tests: the derive emits one `export_bindings_<type>` test
# per `#[ts(export)]` type, each writing the crate's TypeScript beside its
# siblings. `TS_RS_EXPORT_DIR` is the base the types' own `export_to` paths
# resolve against, so where the files land is a property of the source, not of
# this script — hence the collect-and-flatten below rather than a hardcoded
# subdirectory.
set -euo pipefail

if [ "$#" -ne 3 ]; then
    echo "usage: $0 <out-dir> <test-binary> <filter>" >&2
    exit 2
fi
out="$1"
generator="$(realpath "$2")"
filter="$3"

root="$(mktemp -d)"
trap 'rm -rf "$root"' EXIT
work="$root/collected"
log="$work/log"

# The export dir is nested so that `export_to` paths reaching upward out of it —
# which is how a crate points its bindings at the frontend tree — land back
# inside the collection dir. How far they climb is a property of the source, so
# the collection below searches the whole dir rather than assuming a landing
# spot. The bound is exact: seventeen `../` segments still land inside `$work`,
# an eighteenth lands in `$root` and is caught by the escape check below, and
# beyond that the write leaves the scratch tree entirely and nothing here can
# see it. Today's annotations climb two.
export TS_RS_EXPORT_DIR="$work/export/d1/d2/d3/d4/d5/d6/d7/d8/d9/d10/d11/d12/d13/d14/d15/d16"
mkdir -p "$TS_RS_EXPORT_DIR"

# libtest filters by substring and exits 0 when nothing matches, so a filter
# that has gone stale is silent here and caught by the emptiness check below.
"$generator" "$filter" --test-threads=1 >"$log" 2>&1 || {
    echo "ERROR: $generator $filter failed" >&2
    cat "$log" >&2
    exit 1
}

# One level of headroom above the collection dir, watched: a climb that
# overshoots writes files the collection below cannot see, and both the
# emptiness check and the single-directory check would still pass because the
# shallower types landed correctly.
mapfile -d '' -t escaped < <(find "$root" -mindepth 1 -maxdepth 1 -not -name collected -print0)
if [ "${#escaped[@]}" -ne 0 ]; then
    echo "ERROR: '$generator $filter' wrote outside the collection dir:" >&2
    printf '%s\n' "${escaped[@]}" >&2
    echo "An export_to path climbs past what this script contains." >&2
    exit 1
fi

mapfile -d '' -t emitted < <(find "$work" -type f -not -path "$log" -print0)
if [ "${#emitted[@]}" -eq 0 ]; then
    echo "ERROR: '$generator $filter' exported no files." >&2
    echo "Either the filter matches no test or the types stopped exporting." >&2
    cat "$log" >&2
    exit 1
fi

# Every exported type lands in one directory today. Flattening a nested layout
# would silently drop the structure the committed tree has, so refuse instead.
dirs="$(printf '%s\n' "${emitted[@]}" | xargs -n1 dirname | LC_ALL=C sort -u)"
if [ "$(printf '%s\n' "$dirs" | wc -l)" -ne 1 ]; then
    echo "ERROR: '$generator $filter' exported into more than one directory:" >&2
    printf '%s\n' "$dirs" >&2
    exit 1
fi

mkdir -p "$out"
for f in "${emitted[@]}"; do
    mv "$f" "$out/$(basename "$f")"
done
