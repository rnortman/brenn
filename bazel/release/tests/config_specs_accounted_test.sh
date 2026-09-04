#!/usr/bin/env bash
# `config/specs/` is offered by a glob and shipped by three lists.
#
# `//:modules` is the glob, and it is what every `config_fit_test` — brenn's own
# and every bundle repository's — compiles against. What a release stages under
# `modules/` is narrower: the specifications harvested off the packages and the
# surface kinds that ship, plus the library modules `//deploy` names. Nothing in
# the staging gates compares the two: `package_check.sh` and `bundle_check.sh`
# check the staged tree against itself.
#
# So a `.brenn` that lands in that directory owned by nothing is vocabulary
# every fit test accepts and no release carries, and the first reader of the
# mistake is a deployment's boot, after the tarball is installed and the service
# stopped. This holds the offer equal to the promise instead.
set -uo pipefail

offered="$1"
shift

fail=0
accounted="${TEST_TMPDIR:?TEST_TMPDIR must be set}/accounted.txt"
: >"$accounted"
# `awk 1` rather than `cat`: the lists are generated files and the last line of
# one carries no newline, so concatenating them raw would glue two names into a
# third that is neither.
for list in "$@"; do
  awk 1 "$list" >>"$accounted"
done

# Sorted and de-duplicated on both sides: the lists are written sorted, and a
# name appearing in two of them is a different mistake with its own gate (a
# library module may not shadow a harvested basename).
sort -u "$accounted" -o "$accounted"
offered_sorted="${TEST_TMPDIR}/offered.txt"
awk 1 "$offered" | sort -u >"$offered_sorted"

while read -r name; do
  [ -n "$name" ] || continue
  if ! grep -qxF "$name" "$accounted"; then
    echo "FAIL: config/specs/$name is offered by //:modules and shipped by nothing." >&2
    echo "      A specification ships by being named at a component_package in" >&2
    echo "      //brenn-wasm or a surface_processor_assets in //surface; a module" >&2
    echo "      no artifact implements ships by joining BRENN_LIBRARY_MODULES in" >&2
    echo "      //deploy. A file in neither compiles in every fit test and reaches" >&2
    echo "      no host." >&2
    fail=1
  fi
done <"$offered_sorted"

while read -r name; do
  [ -n "$name" ] || continue
  if ! grep -qxF "$name" "$offered_sorted"; then
    echo "FAIL: $name is named as a shipped specification and does not exist under" >&2
    echo "      config/specs/." >&2
    fail=1
  fi
done <"$accounted"

if [ "$fail" -ne 0 ]; then
  exit 1
fi

echo "PASS: every file under config/specs/ is accounted for by the three shipping lists"
