#!/usr/bin/env bash
# Stage the deploy tarball's directory tree.
#
# Usage: assemble.sh --out DIR --manifest FILE --names FILE --record-lib FILE
#                    --stage-lib FILE --frontend DIR --surface DIR
#                    [--bin FILE]... [--lib FILE]...
#                    [--package FILE]...
#
# `--names` is `manifest_names.sh`, which states the manifest's grammar for
# every reader of it; `--record-lib` is `record_lib.sh`, which states how a
# binding record's fields are read; `--stage-lib` is `stage_lib.sh`, the
# staging body this script and `bazel/wasm/bundle_assemble.sh` share, so a
# packaging rule holds for brenn's own tarball and for a component bundle alike.
#
# The layout is the one `deploy.sh` unpacks and reads: `bin/` for the two host
# binaries, `frontend/` and `surface/` for the served asset trees, `lib/` for
# the MCP stub, `components/` for the deploy manifest and one package directory
# per entry, `modules/` for the authored module of every component the release
# ships, and `scripts/` for the manifest grammar the deploying repo's preflight
# execs instead of transcribing. `VERSION` and `deploy.sh` itself are not here:
# the script lives in the deploying repo and the version is the pin that repo
# resolved, so both are added there.
#
# `modules/` is what a deployment's `use @<name>::…` imports resolve against:
# one file per component, named `<name>.brenn` for the wire kind, which is the
# authored basename. Both halves are harvested rather than listed — the backend
# one off each staged package directory, the surface one off the staged surface
# tree, whose per-kind directories already carry each kind's packaged copy — so
# the staged set equals the shipped set by construction and there is no second
# list of the same files to keep in step.
#
# A shipped component is a package directory named for the component, holding
# the artifact, its `package.json` binding record, and, for a processor-world
# component, the `<name>.brenn` copy of its specification. The host resolves the
# directory by the name a configuration states and refuses a package with no
# record, so a manifest entry reaching here with no package fails the build
# rather than the deploy target's next boot.
#
# Nothing here sets file modes except the staged grammar tool's executable bit,
# which the deploying repo's preflight execs. Bazel normalizes a declared output
# directory to read-only after the action runs, so any write mode this script
# chose would be overwritten; the consumer is responsible for restoring
# owner-write before archiving.
#
# Which components ship is decided by the manifest at assembly time, not by the
# caller's list — every package in the tree is offered and only the listed ones
# are copied, which is what makes the manifest the single statement of the
# deployed set. A name it holds that nothing produces is a hard failure.
set -euo pipefail

out=""
manifest=""
names=""
record_lib=""
stage_lib=""
frontend=""
surface=""
bins=()
libs=()
packages=()

while [ "$#" -gt 0 ]; do
    case "$1" in
        --out) out="$2"; shift 2 ;;
        --manifest) manifest="$2"; shift 2 ;;
        --names) names="$2"; shift 2 ;;
        --frontend) frontend="$2"; shift 2 ;;
        --surface) surface="$2"; shift 2 ;;
        --bin) bins+=("$2"); shift 2 ;;
        --lib) libs+=("$2"); shift 2 ;;
        --package) packages+=("$2"); shift 2 ;;
        --record-lib) record_lib="$2"; shift 2 ;;
        --stage-lib) stage_lib="$2"; shift 2 ;;
        *) echo "ERROR: unrecognized argument: $1" >&2; exit 2 ;;
    esac
done

for required in out manifest names frontend surface; do
    if [ -z "${!required}" ]; then
        echo "ERROR: --$required is required" >&2
        exit 2
    fi
done
# Named apart from the loop above: their flags spell the underscore as a hyphen.
if [ -z "$record_lib" ]; then
    echo "ERROR: --record-lib is required" >&2
    exit 2
fi
if [ -z "$stage_lib" ]; then
    echo "ERROR: --stage-lib is required" >&2
    exit 2
fi

# shellcheck source=/dev/null
. "$record_lib"
# shellcheck source=/dev/null
. "$stage_lib"

if [ "${#bins[@]}" -eq 0 ]; then
    echo "ERROR: no --bin given; a package with no binaries deploys nothing" >&2
    exit 2
fi

mkdir -p "$out/bin" "$out/frontend" "$out/surface" "$out/lib" "$out/modules" \
    "$out/components" "$out/scripts"

for bin in "${bins[@]}"; do
    # -L throughout: an input tree or file reaches a sandboxed action as a
    # symlink into the output base, and copying the link itself would put a
    # dangling path in the tarball.
    cp -L "$bin" "$out/bin/$(basename "$bin")"
done

copy_tree() {
    local src="$1" dest="$2" label="$3"
    if [ ! -d "$src" ]; then
        echo "ERROR: $label is not a directory: $src" >&2
        exit 1
    fi
    if [ -z "$(find -L "$src" -type f -print -quit)" ]; then
        echo "ERROR: $label holds no files; it was not built" >&2
        exit 1
    fi
    cp -RL "$src/." "$dest/"
}

copy_tree "$frontend" "$out/frontend" "the frontend asset tree"
copy_tree "$surface" "$out/surface" "the surface asset tree"

for lib in "${libs[@]}"; do
    cp -L "$lib" "$out/lib/$(basename "$lib")"
done

# Shipped beside the packages it names so the deploy step can resolve them, and
# with the grammar that reads it.
stage_manifest "$out" "$manifest" "$names"

stage_associate_packages "${packages[@]}"

shipped=0
listed="$("$names" "$manifest")"
while read -r line; do
    [ -n "$line" ] || continue
    if [ -z "${STAGE_PKG_DIR[$line]:-}" ]; then
        echo "ERROR: $manifest lists $line, which no component_package target packages" >&2
        exit 1
    fi
    stage_package "$out" "$line"
    shipped=$((shipped + 1))
done <<< "$listed"

# A manifest that yields nothing is a manifest that stopped being read, and the
# tarball it produced would deploy a brenn with no components at all.
if [ "$shipped" -eq 0 ]; then
    echo "ERROR: $manifest names no components" >&2
    exit 1
fi

# ---------------------------------------------------------------------------
# The module root
# ---------------------------------------------------------------------------

stage_harvest_surface_modules "$out"
stage_assert_modules_owed "$out"
