#!/usr/bin/env bash
# Stage the deploy tarball's directory tree.
#
# Usage: assemble.sh --out DIR --manifest FILE --names FILE --package-names FILE
#                    --frontend DIR --surface DIR
#                    [--bin FILE]... [--lib FILE]... [--component FILE]...
#                    [--package FILE]...
#
# `--names` is `manifest_names.sh`, which states the manifest's grammar for
# every reader of it; `--package-names` is `package_names.sh`, which states a
# package's file grammar the same way.
#
# The layout is the one `deploy.sh` unpacks and reads: `bin/` for the two host
# binaries, `frontend/` and `surface/` for the served asset trees, and `lib/`
# for the MCP stub, the deploy manifest, and the WASM components the manifest
# names with their package sidecars. `VERSION` and `deploy.sh` itself are not
# here: the script lives in the deploying repo and the version is the pin that
# repo resolved, so both are added there.
#
# A shipped component is three flat sibling files sharing the artifact's stem —
# the `.wasm`, its `.package.json` binding record, and, for a processor-world
# component, the `.spec.brenn` copy of its specification. The host refuses to
# load an artifact whose record is absent, so a manifest entry reaching here
# with no record fails the build rather than the deploy target's next boot.
#
# Nothing here sets file modes. Bazel normalizes a declared output directory to
# read-only after the action runs, so any mode this script chose would be
# overwritten; the consumer is responsible for restoring owner-write before
# archiving.
#
# Which components ship is decided by the manifest at assembly time, not by the
# caller's list — every component in the tree is offered and only the listed
# ones are copied, which is what makes the manifest the single statement of the
# deployed set. A name it holds that nothing produces is a hard failure.
set -euo pipefail

out=""
manifest=""
names=""
package_names=""
frontend=""
surface=""
bins=()
libs=()
components=()
packages=()

while [ "$#" -gt 0 ]; do
    case "$1" in
        --out) out="$2"; shift 2 ;;
        --manifest) manifest="$2"; shift 2 ;;
        --names) names="$2"; shift 2 ;;
        --package-names) package_names="$2"; shift 2 ;;
        --frontend) frontend="$2"; shift 2 ;;
        --surface) surface="$2"; shift 2 ;;
        --bin) bins+=("$2"); shift 2 ;;
        --lib) libs+=("$2"); shift 2 ;;
        --component) components+=("$2"); shift 2 ;;
        --package) packages+=("$2"); shift 2 ;;
        *) echo "ERROR: unrecognized argument: $1" >&2; exit 2 ;;
    esac
done

for required in out manifest names frontend surface; do
    if [ -z "${!required}" ]; then
        echo "ERROR: --$required is required" >&2
        exit 2
    fi
done
# Named apart from the loop above: its flag spells the underscore as a hyphen.
if [ -z "$package_names" ]; then
    echo "ERROR: --package-names is required" >&2
    exit 2
fi
if [ "${#bins[@]}" -eq 0 ]; then
    echo "ERROR: no --bin given; a package with no binaries deploys nothing" >&2
    exit 2
fi

mkdir -p "$out/bin" "$out/frontend" "$out/surface" "$out/lib"

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

# Shipped beside the artifacts it names so the deploy step can resolve them.
cp -L "$manifest" "$out/lib/deployed-components.txt"

# Basename → path, so a manifest entry is resolved by the name it states. The
# sidecars share the index: their basenames are the artifact's stem plus a
# second extension, so a manifest entry resolves all three by name alone.
declare -A by_name=()
for file in "${components[@]}" "${packages[@]}"; do
    name="$(basename "$file")"
    if [ -n "${by_name[$name]:-}" ]; then
        echo "ERROR: two inputs share the basename $name: ${by_name[$name]} and $file" >&2
        exit 1
    fi
    by_name["$name"]="$file"
done

shipped=0
listed="$("$names" "$manifest")"
while read -r line; do
    [ -n "$line" ] || continue
    src="${by_name[$line]:-}"
    if [ -z "$src" ]; then
        echo "ERROR: $manifest lists $line, which no component target produces" >&2
        exit 1
    fi
    cp -L "$src" "$out/lib/$line"
    shipped=$((shipped + 1))

    # The record is mandatory: without it the artifact is unloadable, so a
    # tarball missing one deploys a host that panics on the component it was
    # built to ship.
    # Assigned before being read: a names-tool failure inside a process
    # substitution is invisible to `set -e`, and a manifest entry that is not a
    # component artifact would surface as "no component_package target packages"
    # rather than as the name grammar refusing it.
    sidecars="$("$package_names" "$line")"
    { read -r record_name; read -r spec_name; } <<< "$sidecars"
    record="${by_name[$record_name]:-}"
    if [ -z "$record" ]; then
        echo "ERROR: $manifest lists $line, which no component_package target packages" >&2
        exit 1
    fi
    cp -L "$record" "$out/lib/$record_name"

    spec="${by_name[$spec_name]:-}"
    if [ -n "$spec" ]; then
        cp -L "$spec" "$out/lib/$spec_name"
    fi
done <<< "$listed"

# A manifest that yields nothing is a manifest that stopped being read, and the
# tarball it produced would deploy a brenn with no components at all.
if [ "$shipped" -eq 0 ]; then
    echo "ERROR: $manifest names no components" >&2
    exit 1
fi
