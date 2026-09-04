#!/usr/bin/env bash
# Assert a staged tree of component trees is internally bound.
#
# Usage: bundle_check.sh <names-tool> <record-lib> <tree> --stage-lib FILE
#                        [--manifest FILE] [--module-candidate GLOB]...
#
# `<names-tool>` is `manifest_names.sh`, which states the manifest's grammar for
# every reader of it; `<record-lib>` is `record_lib.sh`, which states how a
# binding record's fields are read; `--stage-lib` is `stage_lib.sh`, which names
# the files the staging half writes, the library-module list among them.
#
# Three subdirectories are checked, each only if it is there: `components/` with
# its manifest and one package directory per entry, `surface/` with one record
# per `processor/<kind>/`, and `modules/`, which is always required because a
# tree that ships either of the other two ships importable vocabulary.
#
# A package is a directory named for the component, holding its `package.json`
# binding record, the artifact that record names, and — for a processor-world
# component — the `<name>.brenn` copy of its specification. A surface kind
# carries the same kind of binding inside its transpile directory. The hashes in
# both are re-computed here, so the tree is proven bound before it ships and an
# installer can copy a directory wholesale. The host re-computes them once more
# at boot; this gate is what keeps that check from being the first one anybody
# runs — and the first one runs on a target host with the service stopped.
#
# `modules/` is the module root a deployment's `use @<name>::…` imports resolve
# against. Every file in it is checked against the packaged specification it
# copies, in both directions, so the root holds exactly the authored modules of
# the components installed beside it. The closing direction searches the
# candidate globs, which default to the two trees this script stages and which a
# caller with trees of its own replaces.
#
# A **library module** is the one thing that direction cannot pair: vocabulary
# the tree ships that no component and no surface kind carries, so there is no
# packaged copy to be byte-identical to. Those are listed, in
# `modules/library-modules.txt`, and the list is what makes them legal — every
# staged module is either byte-identical to an owning copy *or* listed, never
# both, and every listed name is a file in the root. A tree that lists none has
# no such file and is checked exactly as it always was.
set -euo pipefail

if [ "$#" -lt 3 ]; then
    echo "usage: $0 <names-tool> <record-lib> <tree> --stage-lib FILE" \
         "[--manifest FILE] [--module-candidate GLOB]..." >&2
    exit 2
fi
names="$1"
record_lib="$2"
pkg="$3"
shift 3

manifest=""
stage_lib=""
candidates=()
while [ "$#" -gt 0 ]; do
    case "$1" in
        --manifest) manifest="$2"; shift 2 ;;
        --stage-lib) stage_lib="$2"; shift 2 ;;
        --module-candidate) candidates+=("$2"); shift 2 ;;
        *) echo "ERROR: unrecognized argument: $1" >&2; exit 2 ;;
    esac
done

if [ -z "$stage_lib" ]; then
    echo "ERROR: --stage-lib is required" >&2
    exit 2
fi

if [ "${#candidates[@]}" -eq 0 ]; then
    candidates=("$pkg/components/*/*.brenn" "$pkg/surface/processor/*/*.spec.brenn")
fi

# The candidates are patterns, so a caller can state a placement this script
# does not stage without knowing what is in the tree yet. Expanded once, here,
# rather than per module.
shopt -s nullglob
module_candidates=()
for pattern in ${candidates[@]+"${candidates[@]}"}; do
    # shellcheck disable=SC2206  # the pattern is meant to glob and to split.
    module_candidates+=($pattern)
done
shopt -u nullglob

# shellcheck source=/dev/null
. "$record_lib"
# The staging half's own body, for the names it writes into the tree: what this
# gate looks for and what the assembler wrote have to be one fact, and a
# transcription of it here is the copy that rots.
# shellcheck source=/dev/null
. "$stage_lib"

if [ ! -d "$pkg" ]; then
    echo "ERROR: $pkg is not a directory; the check would assert nothing."
    exit 1
fi

failures=0
fail() {
    echo "ERROR: $1"
    failures=$((failures + 1))
}

# One record's binding of one file, in the two halves every arm of this gate
# needs: `check_recorded_hash` re-computes the bytes against the hash the record
# states, and `check_recorded_file` adds the question of whether the name the
# record states is the name the host derives — because the host reads the
# derived name and refuses a record stating any other, so a record this gate
# certified would be refused at the bounce with the service already stopped.
#
# `$1` is the file, `$2` the record's label in messages, `$3` the record, `$4`
# the field holding the hash, `$5` the file's label in messages.
check_recorded_hash() {
    local file="$1" label="$2" record="$3" hash_field="$4" shown="$5" stated actual
    if [ ! -s "$file" ]; then
        fail "$shown is missing or empty, but $label binds it"
        return
    fi
    stated="$(record_field "$record" "$hash_field")"
    actual="$(sha256sum "$file" | awk '{print $1}')"
    if [ -z "$stated" ]; then
        fail "$label states no $hash_field"
    elif [ "$stated" != "$actual" ]; then
        fail "$shown hashes to $actual, but $label binds $stated"
    fi
}

# `$1` is the record, `$2` its label in messages, `$3` the directory the named
# file lives in, `$4` the field naming it, `$5` the field holding its hash, `$6`
# the name the host derives for it.
check_recorded_file() {
    local record="$1" label="$2" dir="$3" name_field="$4" hash_field="$5" derived="$6" name
    name="$(record_field "$record" "$name_field")"
    if [ -z "$name" ]; then
        fail "$label states no $name_field"
        return
    fi
    if [ "$name" != "$derived" ]; then
        fail "$label names $name as its $name_field, but the host derives that name as $derived and reads no other file"
        return
    fi
    check_recorded_hash "$dir/$name" "$label" "$record" "$hash_field" "$dir/$name"
}

# One component's staged module: the file `use @<name>::…` resolves to must be
# the packaged copy that component's record binds, byte for byte, since the host
# compares the configuration's hash against that record at boot.
#
# Called from the package loop and the surface loop, which is where a name and
# its packaged copy are already in hand. A tree with no module root at all has
# been refused once by then, so this says nothing more about it.
#
# `$1` is the packaged copy, `$2` the name, `$3` the copy's label in messages.
check_staged_module() {
    local packaged="$1" name="$2" shown="$3" module
    modules_owed=$((modules_owed + 1))
    [ "$modules_root" -eq 1 ] || return 0
    module="$pkg/modules/$name.brenn"
    if [ ! -s "$module" ]; then
        fail "modules/$name.brenn is missing or empty, but the tree ships $name"
        return
    fi
    if [ ! -s "$packaged" ]; then
        fail "$shown is missing or empty, so modules/$name.brenn stands for nothing"
        return
    fi
    if ! cmp -s "$module" "$packaged"; then
        fail "modules/$name.brenn differs from $shown; a configuration compiled against it binds to no installed component"
    fi
}

# The module root's presence, settled before anything reads it. Settled here so
# the package loop and the surface loop below can each check a staged module
# where they stand, holding the packaged copy and its name already.
modules_root=1
# How many staged items carry a specification. A replay-world package ships
# none, so a bundle of replay packages alone owes no module at all: its
# components are named by a `replay_protection` block's `component =`, not by
# an import, and an empty module root is that bundle's correct shape.
modules_owed=0
if [ ! -d "$pkg/modules" ]; then
    fail "modules/ is missing; a deployment importing @<name> has nothing to resolve against"
    modules_root=0
fi

# ---------------------------------------------------------------------------
# The backend packages, and the manifest that names them.
# ---------------------------------------------------------------------------

expected=""
if [ -d "$pkg/components" ]; then
    if [ -z "$manifest" ]; then
        echo "ERROR: components/ is staged but no --manifest was given to check it against" >&2
        exit 2
    fi

    # The manifest grammar the installer execs instead of transcribing. A tree
    # missing it installs from a preflight that cannot read the manifest at all.
    staged_names="$pkg/scripts/manifest_names.sh"
    if [ ! -s "$staged_names" ]; then
        fail "scripts/manifest_names.sh is missing or empty; the installer execs it to read the manifest"
    elif [ ! -x "$staged_names" ]; then
        fail "scripts/manifest_names.sh is not executable; the installer execs it"
    fi

    installed_manifest="$pkg/components/deployed-components.txt"
    if [ ! -f "$installed_manifest" ]; then
        fail "components/deployed-components.txt is missing; the installer reads it to decide what to install"
    elif ! cmp -s "$installed_manifest" "$manifest"; then
        fail "components/deployed-components.txt differs from $manifest"
    fi

    # What the manifest names, and only that: a package nobody asked for is a
    # test-only component reaching a deployment.
    if [ -f "$installed_manifest" ]; then
        expected="$("$names" "$installed_manifest" | LC_ALL=C sort)"
    fi
    if [ -z "$expected" ]; then
        fail "components/deployed-components.txt names no components"
    else
        while read -r name; do
            dir="$pkg/components/$name"
            record="$dir/package.json"
            if [ ! -s "$record" ]; then
                fail "components/$name/package.json is missing or empty; the host resolves a package by its directory and refuses one with no record"
                continue
            fi
            label="components/$name/package.json"

            # The naming rules are the record library's, shared with the gate
            # over the workspace components root; what is left here is what only
            # a staged tree can be asked — that every file the record names
            # shipped and hashes to what it states.
            artifact=""
            spec=""
            recorded_files=("package.json")
            while IFS="$(printf '\t')" read -r kind value; do
                case "$kind" in
                    fail) fail "$label: $value" ;;
                    artifact) artifact="$value" ;;
                    spec) spec="$value" ;;
                esac
            done < <(package_shape "$dir")

            if [ -n "$artifact" ]; then
                recorded_files+=("$artifact")
                check_recorded_hash "$dir/$artifact" "$label" "$record" artifact_sha256 \
                    "components/$name/$artifact"
            fi

            # Spec-iff-named, both directions, which is the one thing the shape
            # rules cannot say: a record naming a spec that did not ship
            # installs a component that cannot be verified, and a spec beside a
            # record that names none is a file the host will never read.
            if [ -n "$spec" ]; then
                recorded_files+=("$spec")
                check_recorded_hash "$dir/$spec" "$label" "$record" spec_sha256 \
                    "components/$name/$spec"
                # The import and the package share a name, so the two copies are
                # compared where both are in hand.
                check_staged_module "$dir/$spec" "$name" "components/$name/$spec"
            elif [ -z "$(record_field "$record" spec)" ] && [ -e "$dir/$name.brenn" ]; then
                fail "components/$name/$name.brenn shipped, but $label names no spec"
            fi

            # A package directory holds what its record binds and nothing else:
            # a leftover file is a second artifact or a second spec the host
            # would never open, and the install sync would install it all the
            # same. Every entry counts, at any depth and of any type — a nested
            # directory or a link whose target never shipped is content no gate
            # looked at, and the sync installs those wholesale too.
            staged_files="$(cd "$dir" && find . -mindepth 1 -printf '%P\n' | LC_ALL=C sort)"
            want_files="$(printf '%s\n' "${recorded_files[@]}" | LC_ALL=C sort)"
            if [ "$staged_files" != "$want_files" ]; then
                fail "components/$name/ does not hold exactly the files its record binds."
                diff -u <(echo "$want_files") <(echo "$staged_files") \
                    --label "$label" --label "components/$name/" || true
            fi
        done <<< "$expected"

        # The manifest's set, and only it. A directory nobody listed is a
        # component the install sync installs and the manifest never mentioned;
        # a loose file is one no package stands behind.
        actual="$(cd "$pkg/components" && find -L . -mindepth 1 -maxdepth 1 -type d -printf '%P\n' | LC_ALL=C sort)"
        if [ "$actual" != "$expected" ]; then
            fail "components/ does not hold exactly the packages the manifest names."
            diff -u <(echo "$expected") <(echo "$actual") \
                --label "manifest" --label "components/" || true
        fi
        stray="$(cd "$pkg/components" && find -L . -mindepth 1 -maxdepth 1 -type f \
            ! -name deployed-components.txt -printf '%P\n' | LC_ALL=C sort)"
        if [ -n "$stray" ]; then
            fail "components/ holds files beside the manifest: $(echo "$stray" | tr '\n' ' ')"
        fi
    fi
fi

# ---------------------------------------------------------------------------
# The surface asset records. A configured surface component kind is refused at
# boot unless the tree holds the record its artifacts were built with and every
# file that record names hashes to what it states — so the same re-computation
# the host does at boot is done here, where the tree is still on the build
# machine and the service is still running.
#
# The record is scraped, not parsed: the emitter writes one scalar field per
# line for exactly this reader (see the record library).
# ---------------------------------------------------------------------------

kinds=0
if [ -d "$pkg/surface" ]; then
    # A processor kind's record lives inside its transpile directory and binds
    # the component bytes the transpile consumed as well as the specification.
    for kind_dir in "$pkg"/surface/processor/*/; do
        [ -d "$kind_dir" ] || continue
        kinds=$((kinds + 1))
        kind="$(basename "$kind_dir")"
        record="$kind_dir/manifest.json"
        if [ ! -s "$record" ]; then
            fail "surface/processor/$kind/manifest.json is missing or empty; the host refuses a processor kind with no record"
            continue
        fi
        label="surface/processor/$kind/manifest.json"
        stated_v="$(record_number "$record" v)"
        if [ "$stated_v" != "2" ]; then
            fail "$label states record version ${stated_v:-none}; the host reads 2 and refuses anything else"
        fi
        stated_kind="$(record_field "$record" kind)"
        if [ "$stated_kind" != "$kind" ]; then
            fail "$label carries a record for kind $stated_kind, but it is staged under $kind"
        fi
        # The component file and the specification are both named from the
        # kind, which is how the host derives them: the record states the
        # specification's name and is held to it, and names the component's not
        # at all, so that one is only re-hashed.
        check_recorded_hash "$kind_dir/$kind.component.wasm" "$label" "$record" source_sha256 \
            "surface/processor/$kind/$kind.component.wasm"
        check_recorded_file "$record" "$label" "$kind_dir" \
            spec spec_sha256 "$kind.spec.brenn"
        check_staged_module "$kind_dir/$kind.spec.brenn" "$kind" \
            "surface/processor/$kind/$kind.spec.brenn"

        # The record's file list is the boot-time closure over the transpiled
        # tree: a file it omits is one the host never verifies, and the install
        # sync copies it all the same. The manifest cannot list itself, so it is
        # the one entry held out of both sides.
        want_listed="$(record_list "$record" files | LC_ALL=C sort)"
        have_listed="$(cd "$kind_dir" && find -L . -mindepth 1 -type f ! -name manifest.json -printf '%P\n' | LC_ALL=C sort)"
        if [ "$want_listed" != "$have_listed" ]; then
            fail "surface/processor/$kind/ does not hold exactly the files its record lists."
            diff -u <(echo "$want_listed") <(echo "$have_listed") \
                --label "$label" --label "surface/processor/$kind/" || true
        fi
    done
fi

# ---------------------------------------------------------------------------
# The module root, closing direction. Every shipped specification was compared
# against its staged module in the loops above — the backend half in the package
# loop, the surface half in the surface loop — so what is left is the other way
# round: a stray or tampered module is text no package stands behind.
# ---------------------------------------------------------------------------

library_list="$pkg/modules/$STAGE_LIBRARY_LIST"
listed_modules=""
if [ "$modules_root" -eq 1 ] && [ -e "$library_list" ]; then
    if [ ! -s "$library_list" ]; then
        fail "modules/$STAGE_LIBRARY_LIST is empty; a tree that lists no library module carries no list"
    else
        listed_modules="$(LC_ALL=C sort "$library_list")"
        # Every listed name is a file in the root: a name with nothing behind it
        # is an import a config resolves in the build's list and nowhere on the
        # host.
        while read -r name; do
            [ -n "$name" ] || continue
            case "$name" in
                *.brenn) ;;
                *)
                    fail "modules/$STAGE_LIBRARY_LIST lists $name, which is not a .brenn module"
                    continue
                    ;;
            esac
            if [ ! -s "$pkg/modules/$name" ]; then
                fail "modules/$STAGE_LIBRARY_LIST lists $name, but modules/$name is missing or empty"
            fi
        done <<< "$listed_modules"
    fi
fi

if [ "$modules_root" -eq 1 ]; then
    shopt -s nullglob
    staged_modules=("$pkg"/modules/*)
    shopt -u nullglob
    if [ "${#staged_modules[@]}" -eq 0 ] && [ "$modules_owed" -gt 0 ]; then
        fail "modules/ holds no files, but $modules_owed staged item(s) ship a specification; the harvest that puts them under one import root did not run"
    fi
    for module in "${staged_modules[@]}"; do
        name="$(basename "$module")"
        # The list is a fact about the root, not vocabulary in it.
        if [ "$name" = "$STAGE_LIBRARY_LIST" ]; then
            continue
        fi
        case "$name" in
            *.brenn) ;;
            *)
                fail "modules/$name is not a .brenn module; the root holds authored modules and nothing else"
                continue
                ;;
        esac
        found=""
        for candidate in ${module_candidates[@]+"${module_candidates[@]}"}; do
            [ -f "$candidate" ] || continue
            if cmp -s "$module" "$candidate"; then
                found="$candidate"
                break
            fi
        done
        # Owned or listed, and exactly one of the two: a listed name that also
        # copies a component's specification is a second statement of the same
        # ownership, and the two could then disagree at the next pin.
        if printf '%s\n' "$listed_modules" | grep -qxF "$name"; then
            if [ -n "$found" ]; then
                fail "modules/$name is listed as a library module and is also the packaged specification $found; one module has one owner"
            fi
        elif [ -z "$found" ]; then
            fail "modules/$name is byte-identical to no packaged specification in this tree and is listed by no library-modules.txt; it is text nothing installed stands behind"
        fi
    done
fi

if [ "$failures" -ne 0 ]; then
    echo "$failures problem(s) with the staged tree at $pkg"
    exit 1
fi

echo "staged tree: $(echo "$expected" | grep -c '[^[:space:]]' || true) package(s), $kinds surface kind(s)"
