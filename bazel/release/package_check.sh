#!/usr/bin/env bash
# Assert the staged release tree satisfies what the deploy script reads from it.
#
# Usage: package_check.sh <names-tool> <package-dir> <manifest> <static|dynamic>
#
# `<names-tool>` is `manifest_names.sh`, which states the manifest's grammar for
# every reader of it.
#
# `deploy.sh` lives in the deploying repo, so nothing in this tree can be held
# equal to it mechanically. This is the in-repo statement of the contract it
# reads: the two binary paths it copies, the MCP stub, the manifest, and one
# artifact per manifest entry. A packaging change that breaks any of them
# produces a green build and a deploy that fails on the target host, halfway
# through, with the service already stopped.
#
# The linkage mode is the packaging config's own claim about the binaries. A
# musl request that silently resolves to glibc is a real failure mode — a glibc
# binary on a musl host does not run — and so is a musl request that resolves to
# musl but not static, whose interpreter does not exist on the deploy host
# either. The static arm rejects any named loader, not just the glibc one.
set -euo pipefail

if [ "$#" -ne 4 ]; then
    echo "usage: $0 <names-tool> <package-dir> <manifest> <static|dynamic>" >&2
    exit 2
fi
names="$1"
pkg="$2"
manifest="$3"
linkage="$4"

case "$linkage" in
    static|dynamic) ;;
    *) echo "usage: $0 <names-tool> <package-dir> <manifest> <static|dynamic>" >&2; exit 2 ;;
esac

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
