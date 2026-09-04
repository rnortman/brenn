#!/usr/bin/env bash
# Assert the staged release tree satisfies what the deploy script reads from it.
#
# Usage: package_check.sh <bundle-check> <names-tool> <record-lib>
#                         <package-dir> <manifest> <static|dynamic> <stage-lib>
#
# `<bundle-check>` is `bundle_check.sh`, which holds the three component trees
# every staged tree carries — `components/`, `surface/`, `modules/` — to their
# records; `<names-tool>` is `manifest_names.sh` and `<record-lib>` is
# `record_lib.sh`, which that script reads the manifest and the records through.
#
# `deploy.sh` lives in the deploying repo, so nothing in this tree can be held
# equal to it mechanically. This is the in-repo statement of the contract it
# reads. What is here is what is brenn's alone: the two binary paths it copies,
# the MCP stub, the two served asset trees, and the claim the packaging config
# makes about how the binaries are linked. The component trees are checked by
# the delegate, which a component repository's bundle runs over its own release
# — one implementation, two callers, so a rule tightened for one tree is
# tightened for both.
#
# Delegation is an `exec` at the end, so a brenn-specific failure is reported
# before the component trees are read at all: the two halves fail for unrelated
# reasons and a tarball missing a binary is not more informative for also
# listing every package that is fine.
#
# The linkage mode is the packaging config's own claim about the binaries. A
# musl request that silently resolves to glibc is a real failure mode — a glibc
# binary on a musl host does not run — and so is a musl request that resolves to
# musl but not static, whose interpreter does not exist on the deploy host
# either. The static arm rejects any named loader, not just the glibc one.
set -euo pipefail

if [ "$#" -ne 7 ]; then
    echo "usage: $0 <bundle-check> <names-tool> <record-lib>" \
         "<package-dir> <manifest> <static|dynamic> <stage-lib>" >&2
    exit 2
fi
bundle_check="$1"
names="$2"
record_lib="$3"
pkg="$4"
manifest="$5"
linkage="$6"
stage_lib="$7"

case "$linkage" in
    static|dynamic) ;;
    *)
        echo "usage: $0 <bundle-check> <names-tool> <record-lib>" \
             "<package-dir> <manifest> <static|dynamic> <stage-lib>" >&2
        exit 2
        ;;
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
echo "release package: $linkage linkage"

# The component trees, and the module root they stand behind. The candidate
# globs are stated rather than defaulted because this tree carries a third
# placement the delegate knows nothing about: the flat specification copies
# beside the surface kernel bundle.
exec "$bundle_check" "$names" "$record_lib" "$pkg" \
    --stage-lib "$stage_lib" \
    --manifest "$manifest" \
    --module-candidate "$pkg/components/*/*.brenn" \
    --module-candidate "$pkg/surface/*.spec.brenn" \
    --module-candidate "$pkg/surface/processor/*/*.spec.brenn"
