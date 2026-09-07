#!/usr/bin/env bash
# Build a mounts document over loose build outputs, for a dev server.
#
# Usage: dev_mounts.sh OUT_DIR NAME=tree:DIR[,tree:DIR]... [NAME=...]...
#
# A server takes its roots from a mounts document, and a mount is one directory
# holding `components/`, `surface/` and/or `modules/` beside a `VERSION` file.
# Nothing a build produces is shaped that way: the three trees are separate
# Bazel outputs under `.bazel-bin`, and an authored module root is a source
# directory. So a dev spawner cannot check in a document — it has to synthesize
# one, which is what this does.
#
# Per mount it recreates `OUT_DIR/NAME/` holding one symlink per named tree
# (`components`, `surface`, `modules`), each pointing at the resolved target,
# plus `VERSION` reading `dev`. Symlinked trees are fine: the host canonicalizes
# the mount *path* and reads through whatever `<mount>/<tree>` resolves to.
# Then it writes `OUT_DIR/mounts.brenn` with one absolute-path declaration per
# mount, and prints that file's path — which is what a Makefile passes to
# `--mounts`.
#
# Exported beside `bundle_assemble.sh` so an out-of-tree component repository's
# `run`/`e2e` targets reach it through `$(BRENN_EXEC)` and spawn a dev server
# over their own bundle tree the same way brenn's own targets do.
set -euo pipefail

die() {
    echo "dev_mounts.sh: $1" >&2
    exit 1
}

[ "$#" -ge 2 ] || die "usage: dev_mounts.sh OUT_DIR NAME=tree:DIR[,tree:DIR]... [NAME=...]..."

out_dir="$1"
shift

mkdir -p "$out_dir"
# Absolute, because a mount path in a document is absolute and the host's
# working directory is not the spawner's.
out_dir="$(cd "$out_dir" && pwd)"

# The whole directory is recreated, not updated: a mount dropped from the spec
# must leave the disk as well as the document, or a later reader finds a
# mount-shaped tree nothing declares.
#
# Recreating means deleting, and OUT_DIR is a Make variable in the out-of-tree
# repositories that reach this through `$(BRENN_EXEC)`: an unset or mistyped one
# resolves to a source directory, a repository root, or `$HOME`. So the delete
# is refused unless the directory is this generator's own — empty, or holding
# the `mounts.brenn` it writes.
if [ -n "$(ls -A "$out_dir")" ] && [ ! -e "$out_dir/mounts.brenn" ]; then
    die "refusing to recreate '$out_dir': it is not empty and holds no mounts.brenn, so it is not this generator's output directory"
fi
rm -rf "$out_dir"
mkdir -p "$out_dir"

document="$out_dir/mounts.brenn"
: > "$document"

for spec in "$@"; do
    case "$spec" in
        *=*) ;;
        *) die "mount spec '$spec' is not NAME=tree:DIR[,tree:DIR]..." ;;
    esac
    name="${spec%%=*}"
    trees="${spec#*=}"
    # Mount names must be kebab slugs to match the host's mount identity
    # constraint; catching a violation here avoids a boot refusal later.
    case "$name" in
        [a-z0-9]*) ;;
        *) die "mount name '$name' must start with a lowercase letter or a digit" ;;
    esac
    case "$name" in
        *[!a-z0-9-]*)
            die "mount name '$name' must hold only lowercase letters, digits and hyphens" ;;
    esac
    [ -n "$trees" ] || die "mount '$name' names no tree"

    mount_dir="$out_dir/$name"
    mkdir -p "$mount_dir"

    saw_tree=0
    IFS=',' read -r -a entries <<< "$trees"
    for entry in "${entries[@]}"; do
        case "$entry" in
            components:*|surface:*|modules:*) ;;
            *) die "mount '$name': '$entry' is not components:DIR, surface:DIR or modules:DIR" ;;
        esac
        tree="${entry%%:*}"
        dir="${entry#*:}"
        [ -d "$dir" ] || die "mount '$name': $tree directory '$dir' does not exist — build it first"
        target="$(cd "$dir" && pwd -P)"
        ln -s "$target" "$mount_dir/$tree"
        saw_tree=1
    done
    [ "$saw_tree" -eq 1 ] || die "mount '$name' names no tree"

    # The host wants a version; a dev tree has none. `dev` is the whole of the
    # answer, and it is what a boot log and a `mounts` listing will report.
    printf 'dev\n' > "$mount_dir/VERSION"

    printf 'mount %s { path = "%s"; }\n' "$name" "$mount_dir" >> "$document"
done

echo "$document"
