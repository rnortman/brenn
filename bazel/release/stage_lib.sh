#!/usr/bin/env bash
# The staging body brenn's release tree and a component bundle share.
#
# Sourced, never executed: `bazel/release/assemble.sh` and
# `bazel/wasm/bundle_assemble.sh` each take it as `--stage-lib FILE`, the shape
# `assemble.sh` already uses for `--record-lib`. Both trees are certified by one
# `bazel/release/bundle_check.sh`, so the staging that produces them is one
# implementation too: a packaging rule added to one half and not the other is a
# green build and a tree the shared checker rejects.
#
# What is here is what both callers do with `components/` and `modules/`. What
# is not: brenn's binaries, asset trees and offer-all-ship-listed manifest loop,
# and the bundle's manifest-equals-packages set check, per-stage shape checks
# and authored-root equality. Those are each one caller's own.
#
# Every function refuses by writing to stderr and exiting non-zero, so a caller
# reads a return value from none of them.
#
# Two globals carry the association from `stage_associate_packages` to
# `stage_package`, because a bash array cannot cross a function boundary by
# name:
#
#   STAGE_PKG_DIR[name]    the package's directory
#   STAGE_PKG_FILES[name]  its files, one per line

declare -gA STAGE_PKG_DIR=()
declare -gA STAGE_PKG_FILES=()

# Package name → the files of its directory, so a manifest entry resolves a
# whole package by the name it states. The build declares each package's files
# under `<something>/<name>/`, so the parent directory's basename is the name.
stage_associate_packages() {
    local file dir name
    for file in "$@"; do
        dir="$(dirname "$file")"
        name="$(basename "$dir")"
        if [ -n "${STAGE_PKG_DIR[$name]:-}" ] && [ "${STAGE_PKG_DIR[$name]}" != "$dir" ]; then
            echo "ERROR: two package directories are named $name:" \
                 "${STAGE_PKG_DIR[$name]} and $dir" >&2
            exit 1
        fi
        STAGE_PKG_DIR["$name"]="$dir"
        STAGE_PKG_FILES["$name"]="${STAGE_PKG_FILES[$name]:-}$file"$'\n'
    done
}

# The manifest and its grammar, side by side. The reader on the target host
# execs the shipped grammar rather than transcribing it, and a transcription is
# one that goes stale the release the format grows an annotation.
stage_manifest() {
    local out="$1" manifest="$2" names="$3"
    mkdir -p "$out/components" "$out/scripts"
    cp -L "$manifest" "$out/components/deployed-components.txt"
    cp -L "$names" "$out/scripts/manifest_names.sh"
    chmod +x "$out/scripts/manifest_names.sh"
}

# One package directory, plus the module it owes. A processor-world package
# carries its authored specification as `<name>.brenn`; a replay-world one has
# no component class and so nothing to import, which is a shape and not a
# mistake.
stage_package() {
    local out="$1" name="$2" dest file
    dest="$out/components/$name"
    mkdir -p "$dest"
    while read -r file; do
        [ -n "$file" ] || continue
        cp -L "$file" "$dest/$(basename "$file")"
    done <<< "${STAGE_PKG_FILES[$name]}"

    # The record is mandatory: without it the package is unresolvable, so a
    # tarball missing one deploys a host that panics on the component it was
    # built to ship.
    if [ ! -f "$dest/package.json" ]; then
        echo "ERROR: the package $name holds no package.json" >&2
        exit 1
    fi

    if [ -f "$dest/$name.brenn" ]; then
        stage_module "$out" "$dest/$name.brenn" "$name.brenn" "components/$name/$name.brenn"
    fi
}

# One name per staged module, so the flat root cannot hold two files claiming
# one import. Reached twice means one authored file staged at both placements;
# a second copy that differs is two different files under one import name, and
# which of them a deployment compiles against cannot come down to copy order.
stage_module() {
    local out="$1" src="$2" name="$3" shown="$4"
    if [ -e "$out/modules/$name" ]; then
        if cmp -s "$out/modules/$name" "$src"; then
            return 0
        fi
        echo "ERROR: $shown differs from the copy already staged as modules/$name;" \
             "one name is one authored module" >&2
        exit 1
    fi
    mkdir -p "$out/modules"
    cp -L "$src" "$out/modules/$name"
}

# The surface half, read off the staged tree: a kind's packaged copy sits inside
# the kind's own directory. Harvesting rather than listing keeps the staged set
# equal to the shipped set by construction.
stage_harvest_surface_modules() {
    local out="$1" kind_dir kind
    for kind_dir in "$out"/surface/processor/*/; do
        [ -d "$kind_dir" ] || continue
        kind="$(basename "$kind_dir")"
        if [ ! -f "$kind_dir$kind.spec.brenn" ]; then
            echo "ERROR: surface processor kind $kind ships no packaged module" >&2
            exit 1
        fi
        stage_module "$out" "$kind_dir$kind.spec.brenn" "$kind.brenn" \
            "surface/processor/$kind/$kind.spec.brenn"
    done
}

# What the staged trees owe the module root, recomputed from those trees: every
# specification a staged package or a staged surface kind carries has to be
# under `modules/` as the name an import spells. A tree that owes a module and
# does not carry it is a deployment whose config refuses `use @<name>::…` at
# boot, with the service already stopped.
#
# Recomputed rather than counted as the staging goes, because a counter the
# staging increments cannot answer this: an edit that stops the harvest running
# stops the increments in the same edit, and the assertion goes quiet exactly
# when it is needed. Reading the trees back is independent of how they got
# there, and is a check a caller's test can drive over a tree it built by hand.
#
# Owing nothing is legal: a bundle whose packages are all replay-world carries
# no specification at all and is named by a `replay_protection` block's
# `component =` rather than by an import.
stage_assert_modules_owed() {
    local out="$1" dir name src missing=""
    # A package's specification is the one named for the package, and a kind's
    # is the one named for the kind: the same two names the staging copies from.
    for dir in "$out"/components/*/ "$out"/surface/processor/*/; do
        [ -d "$dir" ] || continue
        name="$(basename "$dir")"
        case "$dir" in
            */surface/processor/*) src="$dir$name.spec.brenn" ;;
            *)                     src="$dir$name.brenn" ;;
        esac
        [ -f "$src" ] || continue
        [ -f "$out/modules/$name.brenn" ] \
            || missing="$missing modules/$name.brenn (${src#"$out"/})"
    done
    if [ -n "$missing" ]; then
        echo "ERROR: the staged trees carry specifications with no module under" \
             "modules/:$missing; the harvest that puts them under one import" \
             "root did not run" >&2
        exit 1
    fi
}
