#!/usr/bin/env bash
# Liveness proof for the bundle staging script.
#
# `component_bundle` runs the assembler once per build over inputs that are
# correct, and the only in-tree call passes packages and no surface kinds — so
# the surface arm, the module harvest and every one of the refusals below would
# otherwise first run in a consumer's repository, where a defect here is
# indistinguishable from the consumer's own mistake.
#
# The fixtures are opaque bytes: the assembler copies files and reads no record,
# so what a `package.json` says is `bundle_check.sh`'s question and not this
# one's.
set -uo pipefail

names="$1"
assemble="$2"
stage_lib="$3"
tmp="${TEST_TMPDIR:?TEST_TMPDIR must be set}"
failures=0

fail() {
    echo "FAIL: $1"
    failures=$((failures + 1))
}

# The counter is shipped for both hostings off one authored file, which is the
# arrangement `stage_module` exists for; the panel is page-only.
counter_spec="$tmp/authored/demo-counter.brenn"
panel_spec="$tmp/authored/demo-panel.brenn"
mkdir -p "$tmp/authored"
printf 'component DemoCounter { abi = processor; }\n' > "$counter_spec"
printf 'component DemoPanel { abi = processor; }\n' > "$panel_spec"

manifest="$tmp/deployed-components.txt"
printf '# shipped\ndemo-counter\n' > "$manifest"

# A package as the build declares it: files under `<anything>/<name>/`.
build_package() {
    local dir="$1" name="$2"
    mkdir -p "$dir/$name"
    printf '{"v": 2}\n' > "$dir/$name/package.json"
    printf '\0asm\1\0\0\0' > "$dir/$name/brenn_$name.wasm"
    if [ "${3:-with-spec}" = "with-spec" ]; then
        cp "$counter_spec" "$dir/$name/$name.brenn"
    fi
}

# A `surface_processor_assets` output directory: `processor/<kind>/` and nothing
# else.
build_stage() {
    local stage="$1" kind="$2" spec="${3:-}"
    mkdir -p "$stage/processor/$kind"
    printf '\0asm\1\0\0\0' > "$stage/processor/$kind/$kind.component.wasm"
    printf 'export function instantiate() {}\n' > "$stage/processor/$kind/$kind.js"
    printf '{"v": 2}\n' > "$stage/processor/$kind/manifest.json"
    if [ -n "$spec" ]; then
        cp "$spec" "$stage/processor/$kind/$kind.spec.brenn"
    fi
}

# One fixture set per case, from scratch: a case that mutated a shared tree
# would make the next case's meaning depend on the order they run in.
setup() {
    rm -rf "$tmp/pkgs" "$tmp/stage-a" "$tmp/stage-b" "$tmp/out"
    build_package "$tmp/pkgs" demo-counter
    build_stage "$tmp/stage-a" demo-counter "$counter_spec"
    build_stage "$tmp/stage-b" demo-panel "$panel_spec"
}

package_args() {
    local dir="$1" file
    for file in "$dir"/*/*; do
        printf -- '--package\n%s\n' "$file"
    done
}

run() {
    "$assemble" --out "$tmp/out" --names "$names" --stage-lib "$stage_lib" "$@"
}

reject() {
    local label="$1" needle="$2" out
    shift 2
    if out=$(run "$@" 2>&1); then
        fail "$label should be rejected, exited 0: $out"
    elif ! printf '%s' "$out" | grep -qF -e "$needle"; then
        fail "$label: the rejection does not name the problem: $out"
    fi
}

entries() {
    (cd "$tmp/out" && find . -mindepth 1 -printf '%P\n' | LC_ALL=C sort)
}

# ---------------------------------------------------------------------------
# One package, one surface kind sharing that package's authored file, one
# surface-only kind.
# ---------------------------------------------------------------------------
setup
mapfile -t pkg_args < <(package_args "$tmp/pkgs")
if ! out=$(run --manifest "$manifest" "${pkg_args[@]}" \
    --surface-stage "$tmp/stage-a" --surface-stage "$tmp/stage-b" \
    --spec "$counter_spec" --spec "$panel_spec" 2>&1); then
    fail "a complete bundle should stage: $out"
else
    want="$(cat <<'EOF'
components
components/demo-counter
components/demo-counter/brenn_demo-counter.wasm
components/demo-counter/demo-counter.brenn
components/demo-counter/package.json
components/deployed-components.txt
modules
modules/demo-counter.brenn
modules/demo-panel.brenn
scripts
scripts/manifest_names.sh
surface
surface/processor
surface/processor/demo-counter
surface/processor/demo-counter/demo-counter.component.wasm
surface/processor/demo-counter/demo-counter.js
surface/processor/demo-counter/demo-counter.spec.brenn
surface/processor/demo-counter/manifest.json
surface/processor/demo-panel/demo-panel.component.wasm
surface/processor/demo-panel/demo-panel.js
surface/processor/demo-panel/demo-panel.spec.brenn
surface/processor/demo-panel/manifest.json
surface/processor/demo-panel
EOF
)"
    got="$(entries)"
    if [ "$got" != "$(printf '%s\n' "$want" | LC_ALL=C sort)" ]; then
        fail "the staged tree is not the three trees and nothing else:
$(diff -u <(printf '%s\n' "$want" | LC_ALL=C sort) <(printf '%s\n' "$got") \
    --label expected --label staged)"
    fi
    # The name reached both ways stages once, off either copy — they are one
    # authored file, so which one was copied cannot be observable.
    if ! cmp -s "$tmp/out/modules/demo-counter.brenn" "$counter_spec"; then
        fail "the shared module is not the authored file"
    fi
    if [ ! -x "$tmp/out/scripts/manifest_names.sh" ]; then
        fail "the staged manifest grammar is not executable; the installer execs it"
    fi
fi

# ---------------------------------------------------------------------------
# A bundle whose only package is replay-world: no specification, so no module,
# so an empty module root. It is named by a `replay_protection` block's
# `component =` rather than by an import, and refusing it would leave an
# out-of-tree replay author with nothing to ship.
# ---------------------------------------------------------------------------
rm -rf "$tmp/pkgs" "$tmp/out"
build_package "$tmp/pkgs" demo-counter no-spec
mapfile -t pkg_args < <(package_args "$tmp/pkgs")
if ! out=$(run --manifest "$manifest" "${pkg_args[@]}" 2>&1); then
    fail "a replay-only bundle should stage: $out"
elif [ -n "$(find "$tmp/out/modules" -mindepth 1 -print -quit)" ]; then
    fail "a replay-only bundle staged a module: $(entries)"
fi

# ---------------------------------------------------------------------------
# The manifest and the package set, in both directions. A bundle repository has
# no unshipped packages, so a name on either side the other lacks is a mistake.
# ---------------------------------------------------------------------------
setup
mapfile -t pkg_args < <(package_args "$tmp/pkgs")
printf 'demo-counter\nabsent\n' > "$tmp/wider.txt"
reject "a manifest naming a package nothing built" "not the same set" \
    --manifest "$tmp/wider.txt" "${pkg_args[@]}"

setup
build_package "$tmp/pkgs" demo-extra
mapfile -t pkg_args < <(package_args "$tmp/pkgs")
reject "a package the manifest does not name" "not the same set" \
    --manifest "$manifest" "${pkg_args[@]}"

setup
printf '# nothing\n' > "$tmp/empty.txt"
mapfile -t pkg_args < <(package_args "$tmp/pkgs")
reject "a manifest naming nothing" "names no components" \
    --manifest "$tmp/empty.txt" "${pkg_args[@]}"

setup
mapfile -t pkg_args < <(package_args "$tmp/pkgs")
reject "packages with no manifest to hold them to" "--manifest is required" "${pkg_args[@]}"

# Two directories, one basename: the name is the directory's, so this is two
# components claiming one install directory.
setup
build_package "$tmp/pkgs/nested" demo-counter
mapfile -t pkg_args < <(package_args "$tmp/pkgs")
mapfile -t nested_args < <(package_args "$tmp/pkgs/nested")
reject "two package directories with one name" "two package directories are named demo-counter" \
    --manifest "$manifest" "${pkg_args[@]}" "${nested_args[@]}"

setup
rm "$tmp/pkgs/demo-counter/package.json"
mapfile -t pkg_args < <(package_args "$tmp/pkgs")
reject "a package with no binding record" "holds no package.json" \
    --manifest "$manifest" "${pkg_args[@]}"

# ---------------------------------------------------------------------------
# The surface arm. A bundle's surface root is kinds alone: the kernel and the
# flat sidecars are brenn's tree, and a second copy of them is a second kernel
# the host refuses at boot.
# ---------------------------------------------------------------------------
setup
printf 'export {};\n' > "$tmp/stage-b/brenn_surface_kernel.js"
reject "a stage carrying entries outside processor/" "carries entries outside processor/" \
    --surface-stage "$tmp/stage-b"

setup
build_stage "$tmp/stage-b" demo-counter "$counter_spec"
rm -rf "$tmp/stage-b/processor/demo-panel"
reject "two stages shipping one kind" "two staging targets ship the surface kind demo-counter" \
    --surface-stage "$tmp/stage-a" --surface-stage "$tmp/stage-b"

setup
rm "$tmp/stage-b/processor/demo-panel/demo-panel.spec.brenn"
reject "a surface kind with no packaged module" "ships no packaged module" \
    --surface-stage "$tmp/stage-b"

# And one carrying neither its record nor its specification: every kind
# directory is a kind, so an empty one must fail. Held here as well as on
# brenn's own assembler because the harvest is one body under both.
setup
rm "$tmp/stage-b/processor/demo-panel/demo-panel.spec.brenn"
rm "$tmp/stage-b/processor/demo-panel/manifest.json"
reject "a surface kind directory with no record at all" "ships no packaged module" \
    --surface-stage "$tmp/stage-b"

# The rule that deliberately diverges from brenn's own assembly, and the whole
# point of a component shipped for both hostings: one import name is one
# authored file, so which copy a deployment compiles against cannot come down to
# copy order.
setup
printf 'component DemoCounter { abi = processor; } // and a comment\n' \
    > "$tmp/stage-a/processor/demo-counter/demo-counter.spec.brenn"
mapfile -t pkg_args < <(package_args "$tmp/pkgs")
reject "a name reached twice with differing bytes" "one name is one authored module" \
    --manifest "$manifest" "${pkg_args[@]}" --surface-stage "$tmp/stage-a"

# ---------------------------------------------------------------------------
# The assembler's own preconditions.
# ---------------------------------------------------------------------------
setup
reject "a bundle staging neither tree" "a bundle with neither ships nothing"

setup
reject "a stage that is not a directory" "is not a directory" \
    --surface-stage "$tmp/authored/demo-panel.brenn"

# ---------------------------------------------------------------------------
# The authored module root must be set-equal to the staged one; a divergence
# is vocabulary that compiles in CI and refuses to boot on the host.
# ---------------------------------------------------------------------------
setup
mapfile -t pkg_args < <(package_args "$tmp/pkgs")
reject "an authored spec nothing ships" "no package and no surface kind ships" \
    --manifest "$manifest" "${pkg_args[@]}" --surface-stage "$tmp/stage-a" \
    --spec "$counter_spec" --spec "$panel_spec"

setup
mapfile -t pkg_args < <(package_args "$tmp/pkgs")
reject "a staged module the authored root does not offer" "the two are one set" \
    --manifest "$manifest" "${pkg_args[@]}" --surface-stage "$tmp/stage-a" \
    --surface-stage "$tmp/stage-b" --spec "$counter_spec"

# Name equality is not the claim the gate rests on; the bytes are.
setup
mkdir -p "$tmp/drift"
printf 'component DemoPanel { abi = processor; } // authored later\n' > "$tmp/drift/demo-panel.brenn"
reject "an authored spec whose bytes are not the released ones" "are one file" \
    --surface-stage "$tmp/stage-b" --spec "$tmp/drift/demo-panel.brenn"

if [ "$failures" -ne 0 ]; then
    echo "$failures case(s) failed"
    exit 1
fi
echo "bundle_assemble: all cases passed"
