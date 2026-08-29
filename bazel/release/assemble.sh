#!/usr/bin/env bash
# Stage the deploy tarball's directory tree.
#
# Usage: assemble.sh --out DIR --manifest FILE --names FILE
#                    --dom-names FILE --record-lib FILE
#                    --frontend DIR --surface DIR
#                    [--bin FILE]... [--lib FILE]...
#                    [--package FILE]... [--module FILE]...
#
# `--names` is `manifest_names.sh`, which states the manifest's grammar for
# every reader of it; `--dom-names` is `dom_names.sh`, which states a surface
# dom kind's file grammar the same way; `--record-lib` is `record_lib.sh`, which
# states how a binding record's fields are read.
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
# authored basename. The backend half arrives as `--module` arguments; the
# surface half is harvested out of the staged surface tree, whose per-kind
# records already carry each kind's packaged copy and name the kind. Harvesting
# rather than listing keeps the staged set equal to the shipped set by
# construction.
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
dom_names=""
record_lib=""
frontend=""
surface=""
bins=()
libs=()
packages=()
modules=()

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
        --module) modules+=("$2"); shift 2 ;;
        --dom-names) dom_names="$2"; shift 2 ;;
        --record-lib) record_lib="$2"; shift 2 ;;
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
if [ -z "$dom_names" ]; then
    echo "ERROR: --dom-names is required" >&2
    exit 2
fi
if [ -z "$record_lib" ]; then
    echo "ERROR: --record-lib is required" >&2
    exit 2
fi

# shellcheck source=/dev/null
. "$record_lib"

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

# Shipped beside the packages it names so the deploy step can resolve them.
cp -L "$manifest" "$out/components/deployed-components.txt"

# The manifest's grammar travels with the manifest. The deploying repo's
# preflight reads the same file on the target host, and a transcription of the
# grammar there is one that goes stale the release the format grows an
# annotation; execing this copy is what keeps the readers at one.
cp -L "$names" "$out/scripts/manifest_names.sh"
chmod +x "$out/scripts/manifest_names.sh"

# Package name → the files of its directory, so a manifest entry resolves a
# whole package by the name it states. The build declares each package's files
# under `<something>/<name>/`, so the parent directory's basename is the name.
declare -A pkg_files=()
declare -A pkg_dir=()
for file in "${packages[@]}"; do
    dir="$(dirname "$file")"
    name="$(basename "$dir")"
    if [ -n "${pkg_dir[$name]:-}" ] && [ "${pkg_dir[$name]}" != "$dir" ]; then
        echo "ERROR: two package directories are named $name: ${pkg_dir[$name]} and $dir" >&2
        exit 1
    fi
    pkg_dir["$name"]="$dir"
    pkg_files["$name"]="${pkg_files[$name]:-}$file"$'\n'
done

shipped=0
listed="$("$names" "$manifest")"
while read -r line; do
    [ -n "$line" ] || continue
    if [ -z "${pkg_dir[$line]:-}" ]; then
        echo "ERROR: $manifest lists $line, which no component_package target packages" >&2
        exit 1
    fi
    dest="$out/components/$line"
    mkdir -p "$dest"
    while read -r file; do
        [ -n "$file" ] || continue
        cp -L "$file" "$dest/$(basename "$file")"
    done <<< "${pkg_files[$line]}"

    # The record is mandatory: without it the package is unresolvable, so a
    # tarball missing one deploys a host that panics on the component it was
    # built to ship.
    if [ ! -f "$dest/package.json" ]; then
        echo "ERROR: $manifest lists $line, whose package holds no package.json" >&2
        exit 1
    fi
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

# One name per staged module, so the flat root cannot hold two files claiming
# one import. A backend component and a surface kind sharing a name would be the
# same authored file, and staging it twice is a decision to make deliberately
# rather than to resolve by copy order.
stage_module() {
    local src="$1" name="$2"
    if [ -e "$out/modules/$name" ]; then
        echo "ERROR: two components stage the module $name; a module root is flat" >&2
        exit 1
    fi
    cp -L "$src" "$out/modules/$name"
}

for module in "${modules[@]}"; do
    stage_module "$module" "$(basename "$module")"
done

# The surface half, read off the staged tree. A dom kind's record sits flat and
# its files hang off wasm-bindgen's stem, so the packaged copy's name is derived
# through the names tool and never by pattern: kind `mode-clock` files itself
# under `brenn_mode_clock`, which no textual match on the kind would find.
while read -r record; do
    [ -n "$record" ] || continue
    kind="$(record_field "$record" kind)"
    if [ -z "$kind" ]; then
        echo "ERROR: $record states no kind; its packaged module cannot be named" >&2
        exit 1
    fi
    if ! dom_files="$("$dom_names" "$kind" 2>&1)"; then
        echo "ERROR: $record states kind $kind, which no dom kind can be named: $dom_files" >&2
        exit 1
    fi
    { read -r _module; read -r _module_wasm; read -r _record; read -r spec_name; } <<< "$dom_files"
    if [ ! -f "$out/surface/$spec_name" ]; then
        echo "ERROR: $record names kind $kind, whose packaged module $spec_name did not ship" >&2
        exit 1
    fi
    stage_module "$out/surface/$spec_name" "$kind.brenn"
done <<< "$(surface_dom_records "$out/surface")"

# A processor kind's record is directory-keyed, and its packaged copy sits
# beside it under the kind itself.
for kind_dir in "$out"/surface/processor/*/; do
    [ -f "$kind_dir/manifest.json" ] || continue
    kind="$(basename "$kind_dir")"
    if [ ! -f "$kind_dir/$kind.spec.brenn" ]; then
        echo "ERROR: surface processor kind $kind ships no packaged module" >&2
        exit 1
    fi
    stage_module "$kind_dir/$kind.spec.brenn" "$kind.brenn"
done
