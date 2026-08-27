#!/usr/bin/env bash
# Assert the staged release tree satisfies what the deploy script reads from it.
#
# Usage: package_check.sh <names-tool> <package-names-tool> <record-lib>
#                         <dom-names-tool> <package-dir> <manifest>
#                         <static|dynamic>
#
# `<names-tool>` is `manifest_names.sh`, which states the manifest's grammar for
# every reader of it; `<package-names-tool>` is `package_names.sh`, which states
# a package's file grammar the same way; `<dom-names-tool>` is `dom_names.sh`,
# which does it for a surface dom kind; `<record-lib>` is `record_lib.sh`, which
# states how a binding record's fields are read.
#
# `deploy.sh` lives in the deploying repo, so nothing in this tree can be held
# equal to it mechanically. This is the in-repo statement of the contract it
# reads: the two binary paths it copies, the MCP stub, the manifest, and one
# component package per manifest entry. A packaging change that breaks any of
# them produces a green build and a deploy that fails on the target host,
# halfway through, with the service already stopped.
#
# A package is the artifact, its `.package.json` binding record, and — for a
# processor-world component — the `.spec.brenn` copy of its specification. The
# hashes in the record are re-computed here, so the tarball is proven internally
# bound before it ships and `deploy.sh` can install the sidecars by presence.
# The host re-computes them once more at boot; this gate is what keeps that
# check from being the first one anybody runs.
#
# The surface asset tree carries the same kind of binding in two shapes: a dom
# kind's record sits flat beside its module pair, a processor kind's inside its
# transpile directory. Both are re-hashed here, every name they state is held to
# the name the host derives, and a tree holding no dom record at all is refused
# outright — every surface has a chrome, so such a tree was
# built before surface components carried records and the host would refuse it
# at the bounce.
#
# The linkage mode is the packaging config's own claim about the binaries. A
# musl request that silently resolves to glibc is a real failure mode — a glibc
# binary on a musl host does not run — and so is a musl request that resolves to
# musl but not static, whose interpreter does not exist on the deploy host
# either. The static arm rejects any named loader, not just the glibc one.
set -euo pipefail

if [ "$#" -ne 7 ]; then
    echo "usage: $0 <names-tool> <package-names-tool> <record-lib> <dom-names-tool>" \
         "<package-dir> <manifest> <static|dynamic>" >&2
    exit 2
fi
names="$1"
package_names="$2"
record_lib="$3"
dom_names="$4"
pkg="$5"
manifest="$6"
linkage="$7"

case "$linkage" in
    static|dynamic) ;;
    *)
        echo "usage: $0 <names-tool> <package-names-tool> <record-lib> <dom-names-tool>" \
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

installed_manifest="$pkg/lib/deployed-components.txt"
if [ ! -f "$installed_manifest" ]; then
    fail "lib/deployed-components.txt is missing; deploy.sh reads it to decide what to install"
elif ! cmp -s "$installed_manifest" "$manifest"; then
    fail "lib/deployed-components.txt differs from $manifest"
fi

# What the manifest names, and only that: an artifact nobody asked for is a
# test-only component reaching a deployment.
expected=""
if [ -f "$installed_manifest" ]; then
    expected="$("$names" "$installed_manifest" | LC_ALL=C sort)"
fi
if [ -z "$expected" ]; then
    fail "lib/deployed-components.txt names no components"
else
    while read -r name; do
        if [ ! -s "$pkg/lib/$name" ]; then
            fail "lib/$name is missing or empty, but the manifest ships it"
            continue
        fi

        # Assigned before being read: a names-tool failure inside a process
        # substitution is invisible to `set -e`, and the loop would go on to
        # look for a sidecar under an empty name.
        sidecars="$("$package_names" "$name")"
        { read -r record_name; read -r spec_name; } <<< "$sidecars"
        record="$pkg/lib/$record_name"
        if [ ! -s "$record" ]; then
            fail "lib/$record_name is missing or empty; the host refuses a component with no binding record"
            continue
        fi

        check_recorded_file "$record" "lib/$record_name" "$pkg/lib" \
            artifact artifact_sha256 "$name"

        # Spec-iff-named, both directions, which is the one thing the shared
        # helper cannot say: a record naming a spec that did not ship deploys a
        # component that cannot be verified, and a spec beside a record that
        # names none is a file the host will never read.
        if [ -n "$(record_field "$record" spec)" ]; then
            check_recorded_file "$record" "lib/$record_name" "$pkg/lib" \
                spec spec_sha256 "$spec_name"
        elif [ -e "$pkg/lib/$spec_name" ]; then
            fail "lib/$spec_name shipped, but lib/$record_name names no spec"
        fi
    done <<< "$expected"

    actual="$(cd "$pkg/lib" && find -L . -maxdepth 1 -name '*.wasm' -printf '%P\n' | LC_ALL=C sort)"
    if [ "$actual" != "$expected" ]; then
        fail "lib/ does not hold exactly the components the manifest names."
        diff -u <(echo "$expected") <(echo "$actual") \
            --label "manifest" --label "lib/" || true
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
# The records are scraped, not parsed: both emitters write one scalar field per
# line for exactly this reader (see the record library).
# ---------------------------------------------------------------------------

if [ -d "$pkg/surface" ]; then
    # Chrome is a dom kind and every surface has one, so a tree with no dom
    # record at all was built before surface components carried records. It
    # would install cleanly and be refused at the bounce, which is later and
    # worse than here.
    dom_records="$(find -L "$pkg/surface" -maxdepth 1 -name 'brenn_*.manifest.json' | LC_ALL=C sort)"
    if [ -z "$dom_records" ]; then
        fail "surface/ holds no brenn_<kind>.manifest.json; the host refuses every dom component kind whose record did not ship"
    else
        while read -r record; do
            [ -z "$record" ] && continue
            record_name="$(basename "$record")"
            label="surface/$record_name"
            kind="$(record_field "$record" kind)"
            if [ -z "$kind" ]; then
                fail "$label states no kind; the host looks a record up by the kind it configures"
                continue
            fi
            # Assigned before being read: a grammar failure inside a process
            # substitution is invisible to `set -e`, and every name below would
            # then be compared against empty.
            if ! dom_files="$("$dom_names" "$kind" 2>&1)"; then
                fail "$label states kind $kind, which no dom kind can be named: $dom_files"
                continue
            fi
            { read -r want_module; read -r want_module_wasm; read -r want_record
              read -r want_spec; } <<< "$dom_files"
            if [ "$record_name" != "$want_record" ]; then
                fail "$label carries a record for kind $kind, whose record the host reads as $want_record; a record filed under any other name is one it never opens"
                continue
            fi
            check_recorded_file "$record" "$label" "$pkg/surface" \
                module module_sha256 "$want_module"
            check_recorded_file "$record" "$label" "$pkg/surface" \
                module_wasm module_wasm_sha256 "$want_module_wasm"
            check_recorded_file "$record" "$label" "$pkg/surface" \
                spec spec_sha256 "$want_spec"
        done <<< "$dom_records"
    fi

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
    done
fi

if [ "$failures" -ne 0 ]; then
    echo "$failures problem(s) with the staged release tree at $pkg"
    exit 1
fi

# Named, so the test log says which arm ran: the linkage mode is selected on a
# build flag, and a release that quietly took the dev arm would pass this gate
# over a glibc binary.
echo "release package: $linkage linkage, $(echo "$expected" | grep -c '') component(s)"
