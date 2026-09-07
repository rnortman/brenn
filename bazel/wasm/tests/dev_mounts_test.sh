#!/usr/bin/env bash
# Liveness proof for the dev mounts generator.
#
# The generator's only in-tree callers are Makefile targets no gate runs
# (`launchdev`, `e2e`), and its out-of-tree callers are two other repositories'.
# So without this, every refusal below would first run in a consumer's
# repository, and the shape of the document it writes — which the host parses —
# would be gated by nothing.
#
# The document itself is checked structurally rather than by running `brenn
# mounts`: the binary is a host-platform build of the whole server and this is a
# small shell test.
set -uo pipefail

gen="$1"
tmp="${TEST_TMPDIR:?TEST_TMPDIR must be set}"
failures=0

fail() {
    echo "FAIL: $1"
    failures=$((failures + 1))
}

components="$tmp/build/install_tree"
surface="$tmp/build/dist"
modules="$tmp/src/specs"
mkdir -p "$components/demo" "$surface/processor" "$modules"
printf 'component Demo { abi = processor; }\n' > "$modules/demo.brenn"

out="$tmp/.dev-mounts"

# --- the happy path: two mounts, a mixed tree set ---------------------------
document="$("$gen" "$out" \
    "brenn=components:$components,surface:$surface,modules:$modules" \
    "demo=modules:$modules")" || fail "the generator refused a correct spec"

[ -f "$document" ] || fail "the generator did not write the document it printed"
case "$document" in
    /*) ;;
    *) fail "the printed document path '$document' is not absolute" ;;
esac

# One declaration per mount, absolute path, the grammar the host parses.
expected_brenn="mount brenn { path = \"$out/brenn\"; }"
expected_demo="mount demo { path = \"$out/demo\"; }"
grep -Fqx "$expected_brenn" "$document" || fail "no declaration for mount brenn: $(cat "$document")"
grep -Fqx "$expected_demo" "$document" || fail "no declaration for mount demo: $(cat "$document")"
[ "$(wc -l < "$document")" -eq 2 ] || fail "the document declares more than the two mounts"

# Every mount is mount-shaped: a VERSION beside the trees it was given.
for name in brenn demo; do
    [ "$(cat "$out/$name/VERSION")" = "dev" ] || fail "mount $name has no dev VERSION"
done

# The trees are symlinks that resolve to what the spec named, and the host
# reads through them.
[ -L "$out/brenn/components" ] || fail "the components tree is not a symlink"
[ "$(cd "$out/brenn/components" && pwd -P)" = "$(cd "$components" && pwd -P)" ] \
    || fail "the components symlink does not resolve to the build output"
[ -f "$out/brenn/modules/demo.brenn" ] || fail "the modules tree does not read through"
[ -d "$out/brenn/surface/processor" ] || fail "the surface tree does not read through"

# A mount named with fewer trees offers fewer trees, and nothing else.
[ -e "$out/demo/components" ] && fail "mount demo offers a tree it did not name"
[ -e "$out/demo/surface" ] && fail "mount demo offers a tree it did not name"

# --- regeneration drops what the spec stopped naming ------------------------
"$gen" "$out" "brenn=modules:$modules" > /dev/null || fail "the generator refused a narrowed spec"
[ -e "$out/brenn/components" ] && fail "a dropped tree survived regeneration"
grep -Fq "mount demo" "$out/mounts.brenn" && fail "a dropped mount survived the document"
[ -e "$out/demo" ] && fail "a dropped mount's tree survived on disk"

# --- refusals ---------------------------------------------------------------
refuses() {
    local why="$1"
    shift
    if "$gen" "$@" > /dev/null 2>&1; then
        fail "$why"
    fi
}

refuses "a spec with no '=' was accepted" "$out" "brenn"
refuses "an unknown tree name was accepted" "$out" "brenn=binaries:$components"
refuses "a tree directory that does not exist was accepted" "$out" "brenn=modules:$tmp/nowhere"
refuses "a mount with no tree was accepted" "$out" "brenn="
refuses "no mount at all was accepted" "$out"
# A name the host's kebab identity refuses is refused here, where it was
# written, rather than at a boot in the consumer's repository.
refuses "an uppercase mount name was accepted" "$out" "myBundle=modules:$modules"
refuses "an underscore in a mount name was accepted" "$out" "demo_bundle=modules:$modules"

# --- the output directory is not deleted unless it is this generator's -------
# OUT_DIR is a Make variable in the two out-of-tree callers; a mistyped one
# resolving to a source tree must not be a recursive delete.
theirs="$tmp/not-ours"
mkdir -p "$theirs"
printf 'do not delete me\n' > "$theirs/precious"
refuses "a non-empty directory with no mounts.brenn was recreated" "$theirs" "brenn=modules:$modules"
[ -f "$theirs/precious" ] || fail "the generator deleted a directory that was not its output"

fresh="$tmp/fresh"
mkdir -p "$fresh"
"$gen" "$fresh" "brenn=modules:$modules" > /dev/null \
    || fail "the generator refused an empty output directory"

if [ "$failures" -gt 0 ]; then
    echo "dev_mounts_test: $failures failure(s)"
    exit 1
fi
echo "dev_mounts_test: ok"
