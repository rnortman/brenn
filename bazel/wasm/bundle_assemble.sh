#!/usr/bin/env bash
# Stage a component bundle's directory tree.
#
# Usage: bundle_assemble.sh --out DIR --names FILE --stage-lib FILE
#                           [--manifest FILE]
#                           [--package FILE]... [--surface-stage DIR]...
#                           [--spec FILE]...
#
# `--stage-lib` is `bazel/release/stage_lib.sh`, the staging body this script
# and brenn's own `bazel/release/assemble.sh` share: the package-name
# association, the manifest and grammar staging, the per-package copy, the
# module staging and the surface-kind harvest. Both trees are certified by the
# one `bundle_check.sh`, so the halves that produce them are one implementation.
#
# A bundle is the release of a component repository: up to three of the
# subdirectories brenn's own tarball carries, and nothing else. `components/`
# holds one package directory per shipped backend component beside the manifest
# naming them; `surface/` holds `processor/<kind>/` per page-hosted kind;
# `modules/` holds the authored module of every one of them, flat, which is what
# a deployment's `use @<name>::…` imports resolve against. Each installs into a
# root of its own on the target host, named by one boot flag apiece.
#
# `--names` is `manifest_names.sh`, which states the manifest's grammar for
# every reader of it. It is staged under `scripts/` beside a manifest, and only
# beside one: the installer execs the shipped copy rather than transcribing the
# grammar, and a bundle with no `components/` has no manifest for it to read.
#
# The manifest and the packaged set are held equal in both directions. brenn's
# own assembly offers every package in the tree and ships the listed ones,
# because brenn builds components it does not deploy; a bundle repository has
# no unshipped packages, so a name on either side that the other lacks is a
# mistake rather than a selection.
#
# `modules/` is harvested off the staged trees rather than listed, so the staged
# set equals the shipped set by construction. A name reached both ways — one
# component shipped for backend and page hosting alike — stages once, and the
# two packaged copies must be byte-identical: they are one authored file, and
# which of them a deployment compiles against cannot come down to copy order.
# A replay-world package has no specification and so no module; a bundle whose
# packages are all replay-world stages an empty `modules/`, and is named by a
# `replay_protection` block's `component =` rather than by an import.
#
# `--spec` names the repository's *authored* module root, and the staged
# `modules/` tree is held set-equal to it, byte-identical file by file. That is
# what makes the authored root usable as a stand-in for the installed one: a
# config gate reads a bundle repository's checkout instead of building it, and
# without this direction a file authored under that root but shipped by no
# package and no surface kind is vocabulary a config can import, a gate accepts,
# and a host refuses at boot with the service already stopped.
#
# Nothing here sets file modes except the staged grammar tool's executable bit.
# Bazel normalizes a declared output directory to read-only after the action
# runs, so any write mode this script chose would be overwritten; the consumer
# restores owner-write before archiving.
set -euo pipefail

out=""
names=""
stage_lib=""
manifest=""
packages=()
stages=()
specs=()

while [ "$#" -gt 0 ]; do
    case "$1" in
        --out) out="$2"; shift 2 ;;
        --names) names="$2"; shift 2 ;;
        --stage-lib) stage_lib="$2"; shift 2 ;;
        --manifest) manifest="$2"; shift 2 ;;
        --package) packages+=("$2"); shift 2 ;;
        --surface-stage) stages+=("$2"); shift 2 ;;
        --spec) specs+=("$2"); shift 2 ;;
        *) echo "ERROR: unrecognized argument: $1" >&2; exit 2 ;;
    esac
done

for required in out names; do
    if [ -z "${!required}" ]; then
        echo "ERROR: --$required is required" >&2
        exit 2
    fi
done
# Named apart from the loop above: its flag spells the underscore as a hyphen.
if [ -z "$stage_lib" ]; then
    echo "ERROR: --stage-lib is required" >&2
    exit 2
fi

# shellcheck source=/dev/null
. "$stage_lib"

if [ "${#packages[@]}" -eq 0 ] && [ "${#stages[@]}" -eq 0 ]; then
    echo "ERROR: no --package and no --surface-stage; a bundle with neither ships nothing" >&2
    exit 2
fi
if [ "${#packages[@]}" -gt 0 ] && [ -z "$manifest" ]; then
    echo "ERROR: --manifest is required when the bundle ships packages" >&2
    exit 2
fi

mkdir -p "$out/modules"

# ---------------------------------------------------------------------------
# components/
# ---------------------------------------------------------------------------

if [ "${#packages[@]}" -gt 0 ]; then
    stage_associate_packages "${packages[@]}"
    stage_manifest "$out" "$manifest" "$names"

    listed="$("$names" "$manifest" | LC_ALL=C sort)"
    if [ -z "$listed" ]; then
        echo "ERROR: $manifest names no components" >&2
        exit 1
    fi
    built="$(printf '%s\n' "${!STAGE_PKG_DIR[@]}" | LC_ALL=C sort)"
    if [ "$listed" != "$built" ]; then
        echo "ERROR: $manifest and the packages given are not the same set;" \
             "a bundle ships every package it builds" >&2
        diff -u <(echo "$listed") <(echo "$built") \
            --label "$manifest" --label "packages" >&2 || true
        exit 1
    fi

    while read -r name; do
        [ -n "$name" ] || continue
        stage_package "$out" "$name"
    done <<< "$listed"
fi

# ---------------------------------------------------------------------------
# surface/
# ---------------------------------------------------------------------------

# A staging target's output directory holds `processor/<kind>/` and nothing
# else; the kernel bundle and the flat sidecars are brenn's tree, and a bundle
# that carried a second copy of them would be a second kernel the host refuses.
for stage in ${stages[@]+"${stages[@]}"}; do
    if [ ! -d "$stage" ]; then
        echo "ERROR: $stage is not a directory" >&2
        exit 1
    fi
    outside="$(cd "$stage" && find -L . -mindepth 1 -maxdepth 1 ! -name processor -printf '%P\n' | LC_ALL=C sort)"
    if [ -n "$outside" ]; then
        echo "ERROR: $stage carries entries outside processor/: $(echo "$outside" | tr '\n' ' ')" >&2
        exit 1
    fi
    mkdir -p "$out/surface/processor"
    for kind_dir in "$stage"/processor/*/; do
        [ -d "$kind_dir" ] || continue
        kind="$(basename "$kind_dir")"
        dest="$out/surface/processor/$kind"
        if [ -e "$dest" ]; then
            echo "ERROR: two staging targets ship the surface kind $kind;" \
                 "a surface root holds one directory per kind" >&2
            exit 1
        fi
        mkdir -p "$dest"
        # -L: an input tree reaches a sandboxed action as a symlink into the
        # output base, and copying the links themselves would put dangling
        # paths in the tarball.
        cp -RL "$kind_dir." "$dest/"
        chmod -R u+w "$dest"
    done
done

# Off the staged tree rather than off each stage, so one harvest serves this
# assembly and brenn's own.
stage_harvest_surface_modules "$out"
stage_assert_modules_owed "$out"

# ---------------------------------------------------------------------------
# The authored module root
# ---------------------------------------------------------------------------

# Both directions, so the authored root and the staged one hold the same names
# and the same bytes: a spec the repository authors and nothing ships is refused, and
# a staged module the authored root does not offer is refused. `cmp` rather than
# name equality, because the claim a config gate rests on is the bytes.
for spec in ${specs[@]+"${specs[@]}"}; do
    name="$(basename "$spec")"
    if [ ! -f "$out/modules/$name" ]; then
        echo "ERROR: the authored module root offers $name, which no package and no" \
             "surface kind ships; a bundle's module root is exactly what it releases" >&2
        exit 1
    fi
    if ! cmp -s "$out/modules/$name" "$spec"; then
        echo "ERROR: $spec differs from the modules/$name staged from what ships;" \
             "the authored root and the released one are one file" >&2
        exit 1
    fi
done

while read -r staged; do
    [ -n "$staged" ] || continue
    for spec in ${specs[@]+"${specs[@]}"}; do
        if [ "$(basename "$spec")" = "$staged" ]; then
            continue 2
        fi
    done
    echo "ERROR: modules/$staged is staged from what ships and the authored module" \
         "root does not offer it; the two are one set" >&2
    exit 1
done < <(cd "$out/modules" && find -L . -mindepth 1 -maxdepth 1 -printf '%P\n' | LC_ALL=C sort)
