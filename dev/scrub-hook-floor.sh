#!/usr/bin/env sh
# Asserts the hook binary — the brenn-scrub the git and Claude hooks exec from
# PATH — is at or above the release tools/scrub/Cargo.toml pins.
# The floor rejects a release below the pin and the in-tree crate's path
# build, which cargo lists as `scrub v0.1.0`.
#
# With `--registry`, asks only whether cargo's registry lists such a release,
# without requiring it on PATH: the question `make setup-hooks` asks before
# installing. Run from the repo root.
#
# Exit status: 0 at or above the floor; 3 below it, the one outcome an install
# remedies; 2 on a usage error or no pin in tools/scrub/Cargo.toml; any other
# non-zero when cargo is missing, `cargo install --list` fails, or the binary
# is off PATH.
set -eu

usage() { echo "usage: $0 [--registry]" >&2; exit 2; }
registry_only=0
case $# in
    0) ;;
    1) [ "$1" = --registry ] || usage; registry_only=1 ;;
    *) usage ;;
esac

die() { echo "scrub-hook-floor: $*" >&2; exit 1; }

floor=$(sed -n 's/^brenn-scrub = "=\([^"]*\)"$/\1/p' tools/scrub/Cargo.toml)
[ -n "$floor" ] || {
    echo "scrub-hook-floor: tools/scrub/Cargo.toml has no brenn-scrub pin" >&2
    exit 2
}

command -v cargo >/dev/null 2>&1 ||
    die "cargo not on PATH: cannot read which brenn-scrub is installed. Install the Rust toolchain (rustup) and re-run"

# This reads cargo's registry, not the binary on PATH: a brenn-scrub elsewhere
# on PATH that shadows ~/.cargo/bin goes undetected, and one installed by
# anything but cargo fails as `none` even when it is newer than the floor.
# TODO(scrub-binary-version-flag)
# Only a plain release triple counts: a pre-release or build-metadata version
# (`v0.2.0-rc.1`, `v0.2.0+x`) never meets the floor.
list=$(cargo install --list)
match=$(printf '%s\n' "$list" | awk -v floor="$floor" '
    /^brenn-scrub v[0-9]+\.[0-9]+\.[0-9]+[ :]/ {
        v = $2
        sub(/^v/, "", v)
        sub(/:$/, "", v)
        split(v, a, ".")
        split(floor, f, ".")
        for (i = 1; i <= 3; i++) {
            if (a[i] + 0 > f[i] + 0) { print; exit }
            if (a[i] + 0 < f[i] + 0) next
        }
        print
        exit
    }')
if [ -z "$match" ]; then
    installed=$(printf '%s\n' "$list" | grep -E '^(brenn-scrub|scrub) ' || true)
    echo "scrub-hook-floor: the brenn-scrub cargo installed is ${installed:-none}; the hooks need brenn-scrub >= $floor. Run: make setup-hooks" >&2
    exit 3
fi

if [ "$registry_only" -eq 0 ]; then
    command -v brenn-scrub >/dev/null 2>&1 ||
        die "cargo lists $match but brenn-scrub is not on PATH. Add cargo's install bin directory (~/.cargo/bin unless CARGO_HOME or CARGO_INSTALL_ROOT says otherwise) to PATH"
fi

echo "scrub-hook-floor: brenn-scrub >= $floor ($match)"
