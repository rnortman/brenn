#!/usr/bin/env bash
# Assert the frontend bundles carry the build id the build promised them.
#
# Usage: build_id_check.sh <stamped|unstamped> <dist-dir> \
#            <bundle-with-id>... -- <bundle-without-id>...
#
# `resolveBuildId` is unit-tested and the stamp key is held equal across the
# files that write it, but nothing between those two reaches the artifact. A
# `build_id` attribute slipping to False, a `stamp` reverting to 0, or the
# `define` being dropped all produce a bundle whose build id is the literal
# placeholder while the backend reports the real one — which the stale-tab
# handshake reads as a permanently stale tab and force-refreshes, every user,
# every connection. This looks at the bytes:
#
#   unstamped — the placeholder must be there verbatim, which is what proves the
#               substitution ran at all;
#   stamped   — no placeholder anywhere, which is what proves it resolved.
#
# Bundles listed after `--` take no build id and must never carry one.
#
# The placeholder is spelled here rather than passed in, because a BUILD file
# naming the build-id variable is what the leaf guard forbids. The sync guard
# holds this spelling equal to the key the workspace status script emits.
set -euo pipefail

PLACEHOLDER="{STABLE_BRENN_BUILD_ID}"

if [ "$#" -lt 3 ]; then
    echo "usage: $0 <stamped|unstamped> <dist-dir> <with-id>... -- <without-id>..." >&2
    exit 2
fi
mode="$1"
dist="$2"
shift 2

case "$mode" in
    stamped | unstamped) ;;
    *)
        echo "ERROR: mode must be stamped or unstamped, got $mode" >&2
        exit 2
        ;;
esac

with_id=()
without_id=()
seen_separator=0
for arg in "$@"; do
    if [ "$arg" = "--" ]; then
        seen_separator=1
        continue
    fi
    if [ "$seen_separator" -eq 0 ]; then
        with_id+=("$arg")
    else
        without_id+=("$arg")
    fi
done

if [ "${#with_id[@]}" -eq 0 ]; then
    echo "ERROR: no build-id bundles named; the check would assert nothing."
    exit 1
fi

status=0
for bundle in "${with_id[@]}" "${without_id[@]}"; do
    if [ ! -f "$dist/$bundle" ]; then
        echo "ERROR: $dist/$bundle does not exist; the bundle set has changed."
        status=1
    fi
done
[ "$status" -eq 0 ] || exit 1

for bundle in "${with_id[@]}"; do
    if grep -qF "$PLACEHOLDER" "$dist/$bundle"; then
        if [ "$mode" = "stamped" ]; then
            echo "ERROR: $bundle carries the unstamped placeholder on a stamped build."
            status=1
        fi
    elif [ "$mode" = "unstamped" ]; then
        echo "ERROR: $bundle carries no build-id placeholder, so nothing substitutes its id."
        echo "A bundle built without one throws at load, or ships an id nothing set."
        status=1
    fi
done

for bundle in "${without_id[@]}"; do
    if grep -qF "$PLACEHOLDER" "$dist/$bundle"; then
        echo "ERROR: $bundle takes no build id but carries the placeholder."
        status=1
    fi
done

exit "$status"
