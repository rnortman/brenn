#!/usr/bin/env sh
# Runs dev/scrub-hook-floor.sh (the path in $1) against a stub cargo whose
# `install --list` output each case supplies, and asserts the exit status and
# a substring of the output. Each case runs in its own directory holding a
# tools/scrub/Cargo.toml and a bin/ that is the script's whole PATH: sed, awk
# and grep, plus the stubs the case asks for.
set -eu

script=$(cd "$(dirname "$1")" && pwd)/$(basename "$1")
sh=$(command -v sh)
work=${TEST_TMPDIR:?}/scrub-hook-floor
failures=0
n=0

# setup <tools/scrub/Cargo.toml line> <cargo: a list status | absent> <brenn-scrub on
# PATH: yes | no> [cargo install --list line]...
setup() {
    n=$((n + 1))
    dir=$work/$n
    mkdir -p "$dir/bin" "$dir/tools/scrub"
    printf '%s\n' "$1" >"$dir/tools/scrub/Cargo.toml"
    for tool in sed awk grep; do
        ln -s "$(command -v "$tool")" "$dir/bin/$tool"
    done
    cargo=$2 on_path=$3
    shift 3
    : >"$dir/list"
    for line in "$@"; do
        printf '%s\n' "$line" "    brenn-scrub" >>"$dir/list"
    done
    if [ "$cargo" != absent ]; then
        cat >"$dir/bin/cargo" <<EOF
#!$sh
[ "\$*" = "install --list" ] || exit 99
while IFS= read -r l; do printf '%s\n' "\$l"; done <"$dir/list"
exit $cargo
EOF
        chmod +x "$dir/bin/cargo"
    fi
    if [ "$on_path" = yes ]; then
        printf '#!%s\nexit 0\n' "$sh" >"$dir/bin/brenn-scrub"
        chmod +x "$dir/bin/brenn-scrub"
    fi
}

# check <case name> <expected status> <expected output substring> [script arg]
check() {
    name=$1 want=$2 needle=$3
    shift 3
    set +e
    out=$(cd "$dir" && PATH="$dir/bin" "$sh" "$script" "$@" 2>&1)
    got=$?
    set -e
    case $out in
        *"$needle"*) found=1 ;;
        *) found=0 ;;
    esac
    if [ "$got" -ne "$want" ] || [ "$found" -eq 0 ]; then
        failures=$((failures + 1))
        printf 'FAIL %s: status %s (want %s), output:\n%s\n(want substring: %s)\n' \
            "$name" "$got" "$want" "$out" "$needle"
    else
        printf 'ok   %s\n' "$name"
    fi
}

pin='brenn-scrub = "=0.2.0"'

setup "$pin" 0 yes "brenn-scrub v0.2.0:"
check "equal to the floor" 0 "brenn-scrub >= 0.2.0 (brenn-scrub v0.2.0:)"

setup "$pin" 0 yes "brenn-scrub v0.10.0:"
check "minor above the floor, compared numerically" 0 "(brenn-scrub v0.10.0:)"

setup "$pin" 0 yes "brenn-scrub v1.0.0 (/home/alice/brenn-scrub):"
check "major above the floor, checkout build" 0 "(brenn-scrub v1.0.0 (/home/alice/brenn-scrub):)"

setup "$pin" 0 yes "brenn-scrub v0.1.9:"
check "below the floor" 3 "installed is brenn-scrub v0.1.9:; the hooks need brenn-scrub >= 0.2.0"

setup "$pin" 0 yes "brenn-scrub v0.2.0-rc.1:"
check "pre-release of the floor release" 3 "installed is brenn-scrub v0.2.0-rc.1:;"

setup "$pin" 0 yes "brenn-scrub v0.2.0-alpha (/home/alice/brenn-scrub):"
check "pre-release checkout build" 3 "installed is brenn-scrub v0.2.0-alpha"

setup "$pin" 0 yes "scrub v0.1.0 (/home/alice/brenn/scrub):"
check "only the in-tree path build" 3 "installed is scrub v0.1.0 (/home/alice/brenn/scrub):;"

setup "$pin" 0 yes
check "nothing installed" 3 "installed is none;"

setup "$pin" 101 yes "brenn-scrub v0.2.0:"
check "cargo install --list fails" 101 ""

setup "$pin" absent yes
check "no cargo" 1 "cargo not on PATH"

setup 'brenn-scrub = "0.2.0"' 0 yes "brenn-scrub v0.2.0:"
check "no exact pin in tools/scrub/Cargo.toml" 2 "tools/scrub/Cargo.toml has no brenn-scrub pin"

setup "$pin" 0 yes "brenn-scrub v0.2.0:"
check "unknown argument" 2 "usage:" --list

setup "$pin" 0 no "brenn-scrub v0.2.0:"
check "off PATH" 1 "but brenn-scrub is not on PATH"
check "off PATH, registry only" 0 "brenn-scrub >= 0.2.0 (brenn-scrub v0.2.0:)" --registry

setup "$pin" 0 no "brenn-scrub v0.1.9:"
check "below the floor, registry only" 3 "the hooks need brenn-scrub >= 0.2.0" --registry

if [ "$failures" -ne 0 ]; then
    echo "$failures case(s) failed"
    exit 1
fi
