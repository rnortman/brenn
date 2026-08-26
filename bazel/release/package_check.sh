#!/usr/bin/env bash
# Assert the staged release tree satisfies what the deploy script reads from it.
#
# Usage: package_check.sh <names-tool> <package-names-tool> <record-lib>
#                         <package-dir> <manifest> <static|dynamic>
#
# `<names-tool>` is `manifest_names.sh`, which states the manifest's grammar for
# every reader of it; `<package-names-tool>` is `package_names.sh`, which states
# a package's file grammar the same way; `<record-lib>` is `record_lib.sh`,
# which states how a binding record's fields are read.
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
# The linkage mode is the packaging config's own claim about the binaries. A
# musl request that silently resolves to glibc is a real failure mode — a glibc
# binary on a musl host does not run — and so is a musl request that resolves to
# musl but not static, whose interpreter does not exist on the deploy host
# either. The static arm rejects any named loader, not just the glibc one.
set -euo pipefail

if [ "$#" -ne 6 ]; then
    echo "usage: $0 <names-tool> <package-names-tool> <record-lib> <package-dir> <manifest>" \
         "<static|dynamic>" >&2
    exit 2
fi
names="$1"
package_names="$2"
record_lib="$3"
pkg="$4"
manifest="$5"
linkage="$6"

case "$linkage" in
    static|dynamic) ;;
    *)
        echo "usage: $0 <names-tool> <package-names-tool> <record-lib> <package-dir>" \
             "<manifest> <static|dynamic>" >&2
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

        artifact_sha="$(record_field "$record" artifact_sha256)"
        actual_sha="$(sha256sum "$pkg/lib/$name" | awk '{print $1}')"
        if [ -z "$artifact_sha" ]; then
            fail "lib/$record_name states no artifact_sha256"
        elif [ "$artifact_sha" != "$actual_sha" ]; then
            fail "lib/$name hashes to $actual_sha, but its record binds $artifact_sha"
        fi

        # The name the record states has to be the name the host derives, or
        # the host refuses a package this gate certified: `load_record`
        # compares the `spec` field against `<stem>.spec.brenn` and `verify`
        # reads only that file. Checked before presence, so the rest of the
        # arm speaks about one name rather than two.
        spec="$(record_field "$record" spec)"
        if [ -n "$spec" ] && [ "$spec" != "$spec_name" ]; then
            fail "lib/$record_name names $spec, but a package's specification is $spec_name; the host derives that name and would refuse this record"
            continue
        fi

        # Spec-iff-named, both directions: a record naming a spec that did not
        # ship deploys a component that cannot be verified, and a spec beside a
        # record that names none is a file the host will never read.
        if [ -n "$spec" ]; then
            if [ ! -s "$pkg/lib/$spec_name" ]; then
                fail "lib/$record_name names $spec_name, which is missing or empty in lib/"
                continue
            fi
            spec_sha="$(record_field "$record" spec_sha256)"
            actual_spec_sha="$(sha256sum "$pkg/lib/$spec_name" | awk '{print $1}')"
            if [ -z "$spec_sha" ]; then
                fail "lib/$record_name names a spec but states no spec_sha256"
            elif [ "$spec_sha" != "$actual_spec_sha" ]; then
                fail "lib/$spec_name hashes to $actual_spec_sha, but its record binds $spec_sha"
            fi
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

if [ "$failures" -ne 0 ]; then
    echo "$failures problem(s) with the staged release tree at $pkg"
    exit 1
fi

# Named, so the test log says which arm ran: the linkage mode is selected on a
# build flag, and a release that quietly took the dev arm would pass this gate
# over a glibc binary.
echo "release package: $linkage linkage, $(echo "$expected" | grep -c '') component(s)"
