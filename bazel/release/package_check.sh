#!/usr/bin/env bash
# Assert the staged release tree satisfies what the deploy script reads from it.
#
# Usage: package_check.sh <names-tool> <record-lib>
#                         <package-dir> <manifest> <static|dynamic>
#
# `<names-tool>` is `manifest_names.sh`, which states the manifest's grammar for
# every reader of it; `<record-lib>` is `record_lib.sh`, which states how a
# binding record's fields are read.
#
# `deploy.sh` lives in the deploying repo, so nothing in this tree can be held
# equal to it mechanically. This is the in-repo statement of the contract it
# reads: the two binary paths it copies, the MCP stub, the manifest, the
# manifest grammar it execs, and one component package per manifest entry. A
# packaging change that breaks any of them produces a green build and a deploy
# that fails on the target host, halfway through, with the service already
# stopped.
#
# A package is a directory named for the component, holding its `package.json`
# binding record, the artifact that record names, and — for a processor-world
# component — the `<name>.brenn` copy of its specification. The hashes in the
# record are re-computed here, so the tarball is proven internally bound before
# it ships and `deploy.sh` can install a package directory wholesale. The host
# re-computes them once more at boot; this gate is what keeps that check from
# being the first one anybody runs.
#
# `modules/` is the module root a deployment's `use @<name>::…` imports resolve
# against. Every file in it is checked against the packaged specification it
# copies, in both directions, so the root holds exactly the authored modules of
# the components this release installs.
#
# The surface asset tree carries the same kind of binding: a kind's record sits
# inside its transpile directory. It is re-hashed here and every name it states
# is held to the name the host derives.
#
# The linkage mode is the packaging config's own claim about the binaries. A
# musl request that silently resolves to glibc is a real failure mode — a glibc
# binary on a musl host does not run — and so is a musl request that resolves to
# musl but not static, whose interpreter does not exist on the deploy host
# either. The static arm rejects any named loader, not just the glibc one.
set -euo pipefail

if [ "$#" -ne 5 ]; then
    echo "usage: $0 <names-tool> <record-lib>" \
         "<package-dir> <manifest> <static|dynamic>" >&2
    exit 2
fi
names="$1"
record_lib="$2"
pkg="$3"
manifest="$4"
linkage="$5"

case "$linkage" in
    static|dynamic) ;;
    *)
        echo "usage: $0 <names-tool> <record-lib>" \
             "<package-dir> <manifest> <static|dynamic>" >&2
        exit 2
        ;;
esac

# shellcheck source=/dev/null
. "$record_lib"

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
# Called from the package loop and the surface loops, which is where a name and
# its packaged copy are already in hand. A tree with no module root at all has
# been refused once by then, so this says nothing more about it.
#
# `$1` is the packaged copy, `$2` the name, `$3` the copy's label in messages.
check_staged_module() {
    local packaged="$1" name="$2" shown="$3" module
    [ "$modules_root" -eq 1 ] || return 0
    module="$pkg/modules/$name.brenn"
    if [ ! -s "$module" ]; then
        fail "modules/$name.brenn is missing or empty, but the release ships $name"
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

# The paths deploy.sh copies out of bin/. Hard-coded, because they are the
# contract: renaming one here without renaming it there is the bug this catches.
for name in brenn brenn-cli; do
    bin="$pkg/bin/$name"
    if [ ! -f "$bin" ]; then
        fail "bin/$name is missing; deploy.sh copies it unconditionally"
        continue
    fi
    if [ ! -s "$bin" ]; then
        fail "bin/$name is empty"
        continue
    fi
    if [ ! -x "$bin" ]; then
        fail "bin/$name is not executable; the service would fail to start"
    fi
    # A dynamically linked binary names its interpreter in the ELF header; a
    # static-pie one names none. Both loaders are rejected: a glibc-dynamic
    # binary names `ld-linux`, and a musl-dynamic one names `ld-musl` and would
    # otherwise pass an arm that only knows the glibc name.
    if [ "$linkage" = static ] && LC_ALL=C grep -qaE 'ld-(linux|musl)' "$bin"; then
        fail "bin/$name names a dynamic loader, so this is not a static build"
    fi
done

if [ ! -s "$pkg/lib/noop_mcp.py" ]; then
    fail "lib/noop_mcp.py is missing or empty"
fi

# The module root's presence, settled before anything reads it.
# `modules/<name>.brenn` is what a deployment's `use @<name>::…` imports resolve
# against, and every file in it must be the authored module of a component this
# release ships — byte for byte, because that is what the host binds at boot.
# Settled here so the package loop and the surface loops below can each check a
# staged module where they stand, holding the packaged copy and its name
# already.
modules_root=1
if [ ! -d "$pkg/modules" ]; then
    fail "modules/ is missing; a deployment importing @<name> has nothing to resolve against"
    modules_root=0
fi

# The manifest grammar the deploying repo's preflight execs instead of
# transcribing. A tarball missing it deploys a preflight that cannot read the
# manifest at all.
staged_names="$pkg/scripts/manifest_names.sh"
if [ ! -s "$staged_names" ]; then
    fail "scripts/manifest_names.sh is missing or empty; preflight execs it to read the manifest"
elif [ ! -x "$staged_names" ]; then
    fail "scripts/manifest_names.sh is not executable; preflight execs it"
fi

installed_manifest="$pkg/components/deployed-components.txt"
if [ ! -f "$installed_manifest" ]; then
    fail "components/deployed-components.txt is missing; deploy.sh reads it to decide what to install"
elif ! cmp -s "$installed_manifest" "$manifest"; then
    fail "components/deployed-components.txt differs from $manifest"
fi

# What the manifest names, and only that: a package nobody asked for is a
# test-only component reaching a deployment.
expected=""
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

        # The naming rules are the record library's, shared with the gate over
        # the workspace components root; what is left here is what only a
        # staged tree can be asked — that every file the record names shipped
        # and hashes to what it states.
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
        # rules cannot say: a record naming a spec that did not ship deploys a
        # component that cannot be verified, and a spec beside a record that
        # names none is a file the host will never read.
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

        # A package directory holds what its record binds and nothing else: a
        # leftover file is a second artifact or a second spec the host would
        # never open, and the deploy sync would install it all the same. Every
        # entry counts, at any depth and of any type — a nested directory or a
        # link whose target never shipped is content no gate looked at, and the
        # sync installs those wholesale too.
        staged_files="$(cd "$dir" && find . -mindepth 1 -printf '%P\n' | LC_ALL=C sort)"
        want_files="$(printf '%s\n' "${recorded_files[@]}" | LC_ALL=C sort)"
        if [ "$staged_files" != "$want_files" ]; then
            fail "components/$name/ does not hold exactly the files its record binds."
            diff -u <(echo "$want_files") <(echo "$staged_files") \
                --label "$label" --label "components/$name/" || true
        fi
    done <<< "$expected"

    # The manifest's set, and only it. A directory nobody listed is a component
    # the deploy sync installs and the manifest never mentioned; a loose file is
    # one no package stands behind.
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

for tree in frontend surface; do
    if [ ! -d "$pkg/$tree" ]; then
        fail "$tree/ is missing"
    elif [ -z "$(find -L "$pkg/$tree" -type f -print -quit)" ]; then
        fail "$tree/ holds no files"
    fi
done

# ---------------------------------------------------------------------------
# The surface asset records. A configured surface component kind is refused at
# boot unless the tree holds the record its artifacts were built with and every
# file that record names hashes to what it states — so the same re-computation
# the host does at boot is done here, where the tarball is still on the build
# machine and the service is still running.
#
# The record is scraped, not parsed: the emitter writes one scalar field per
# line for exactly this reader (see the record library).
# ---------------------------------------------------------------------------

if [ -d "$pkg/surface" ]; then
    # A processor kind's record lives inside its transpile directory and binds
    # the component bytes the transpile consumed as well as the specification.
    for kind_dir in "$pkg"/surface/processor/*/; do
        [ -d "$kind_dir" ] || continue
        kind="$(basename "$kind_dir")"
        record="$kind_dir/manifest.json"
        if [ ! -s "$record" ]; then
            fail "surface/processor/$kind/manifest.json is missing or empty; the host refuses a processor kind with no record"
            continue
        fi
        label="surface/processor/$kind/manifest.json"
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
    done
fi

# ---------------------------------------------------------------------------
# The module root, closing direction. Every shipped specification was compared
# against its staged module in the loops above — the backend half in the package
# loop, the surface half in the surface loop — so what is left is the other
# way round: a stray or tampered module is text no package stands behind.
# ---------------------------------------------------------------------------

if [ "$modules_root" -eq 1 ]; then
    # The direction that makes the root a closed set: a file
    # nobody packaged is a module a deployment could import and the host would
    # never bind.
    shopt -s nullglob
    staged_modules=("$pkg"/modules/*)
    shopt -u nullglob
    if [ "${#staged_modules[@]}" -eq 0 ]; then
        fail "modules/ holds no files; every release ships components, so this root was never staged"
    fi
    for module in "${staged_modules[@]}"; do
        name="$(basename "$module")"
        case "$name" in
            *.brenn) ;;
            *)
                fail "modules/$name is not a .brenn module; the root holds authored modules and nothing else"
                continue
                ;;
        esac
        found=""
        for candidate in "$pkg"/components/*/*.brenn "$pkg"/surface/*.spec.brenn \
            "$pkg"/surface/processor/*/*.spec.brenn; do
            [ -f "$candidate" ] || continue
            if cmp -s "$module" "$candidate"; then
                found="$candidate"
                break
            fi
        done
        if [ -z "$found" ]; then
            fail "modules/$name is byte-identical to no packaged specification in this release; it is text nothing installed stands behind"
        fi
    done
fi

if [ "$failures" -ne 0 ]; then
    echo "$failures problem(s) with the staged release tree at $pkg"
    exit 1
fi

# Named, so the test log says which arm ran: the linkage mode is selected on a
# build flag, and a release that quietly took the dev arm would pass this gate
# over a glibc binary.
echo "release package: $linkage linkage, $(echo "$expected" | grep -c '') package(s)"
