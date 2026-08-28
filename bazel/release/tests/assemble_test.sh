#!/usr/bin/env bash
# Liveness proof for the release tree's assembly.
#
# The real invocation produces a tree the gates then pass over, which says
# nothing about whether the script would notice its inputs going wrong. Here
# every input is a fixture: the happy path is checked layout entry by layout
# entry, and each way the packaging can be handed something broken — a manifest
# naming an artifact nobody built, a manifest naming a component nothing
# packaged, a manifest that reads as empty, an asset tree that was never built,
# two components with one basename — is checked to fail rather than to ship a
# tarball missing a piece.
#
# The module root gets the same treatment: the backend modules arrive as
# arguments and the surface ones are harvested off the staged tree, so both
# halves are staged here and a name claimed twice is checked to fail.
set -uo pipefail

names="$1"
assemble="$2"
package_names="$3"
dom_names="$4"
record_lib="$5"
tmp="${TEST_TMPDIR:?TEST_TMPDIR must be set}"
failures=0

fail() {
    echo "FAIL: $1"
    failures=$((failures + 1))
}

mkdir -p "$tmp/in/frontend/skins" "$tmp/in/surface/processor" "$tmp/in/bin" "$tmp/in/wasm"
printf 'binary\n' > "$tmp/in/bin/brenn"
printf 'binary\n' > "$tmp/in/bin/brenn-cli"
chmod +x "$tmp/in/bin/brenn" "$tmp/in/bin/brenn-cli"
printf 'main\n' > "$tmp/in/frontend/main.js"
printf 'skin\n' > "$tmp/in/frontend/skins/dark.css"
printf 'shell\n' > "$tmp/in/surface/brenn_kernel.js"
printf 'proc\n' > "$tmp/in/surface/processor/one.js"

# One kind of each surface record shape, because the harvest reads them
# differently: a dom kind's record sits flat and its packaged module hangs off
# wasm-bindgen's stem, a processor kind's sits inside the kind's own directory.
# Only the `kind` field is scraped here, so the rest of the record is elided.
printf 'component ModeClock {}\n' > "$tmp/in/surface/brenn_mode_clock.spec.brenn"
printf 'export function init() {}\n' > "$tmp/in/surface/brenn_mode_clock.js"
printf '{\n  "kind": "mode-clock"\n}\n' > "$tmp/in/surface/brenn_mode_clock.manifest.json"
mkdir -p "$tmp/in/surface/processor/transplant"
printf 'component Transplant {}\n' > "$tmp/in/surface/processor/transplant/transplant.spec.brenn"
printf '{\n  "kind": "transplant"\n}\n' > "$tmp/in/surface/processor/transplant/manifest.json"

mkdir -p "$tmp/in/modules"
printf 'component Shipped {}\n' > "$tmp/in/modules/shipped-component.brenn"
printf 'stub\n' > "$tmp/in/noop_mcp.py"
for name in shipped.wasm also_shipped.wasm test_only.wasm; do
    printf '\0asm\1\0\0\0' > "$tmp/in/wasm/$name"
done

mkdir -p "$tmp/in/pkg"
printf '{"v": 1, "artifact": "shipped.wasm"}\n' > "$tmp/in/pkg/shipped.package.json"
printf 'component Shipped {}\n' > "$tmp/in/pkg/shipped.spec.brenn"
printf '{"v": 1, "artifact": "also_shipped.wasm"}\n' > "$tmp/in/pkg/also_shipped.package.json"
printf '{"v": 1, "artifact": "test_only.wasm"}\n' > "$tmp/in/pkg/test_only.package.json"

# Repeated at every invocation below, because a package is not optional: the
# host refuses an artifact whose record did not travel with it.
packages=(
    --package "$tmp/in/pkg/shipped.package.json"
    --package "$tmp/in/pkg/shipped.spec.brenn"
    --package "$tmp/in/pkg/also_shipped.package.json"
    --package "$tmp/in/pkg/test_only.package.json"
)

cat > "$tmp/in/manifest.txt" <<'EOF'
# Components shipped to deployments.
shipped.wasm

also_shipped.wasm
EOF

run() {
    "$assemble" \
        --names "$names" --package-names "$package_names" \
        --dom-names "$dom_names" --record-lib "$record_lib" \
        --module "$tmp/in/modules/shipped-component.brenn" \
        --out "$1" \
        --manifest "$2" \
        --frontend "$tmp/in/frontend" \
        --surface "$tmp/in/surface" \
        --bin "$tmp/in/bin/brenn" \
        --bin "$tmp/in/bin/brenn-cli" \
        --lib "$tmp/in/noop_mcp.py" \
        --component "$tmp/in/wasm/shipped.wasm" \
        --component "$tmp/in/wasm/also_shipped.wasm" \
        --component "$tmp/in/wasm/test_only.wasm" \
        "${packages[@]}"
}

if ! run "$tmp/out" "$tmp/in/manifest.txt" > "$tmp/out.log" 2>&1; then
    fail "the happy path should assemble: $(cat "$tmp/out.log")"
fi

for path in \
    bin/brenn \
    bin/brenn-cli \
    frontend/main.js \
    frontend/skins/dark.css \
    surface/brenn_kernel.js \
    surface/processor/one.js \
    lib/noop_mcp.py \
    lib/deployed-components.txt \
    lib/shipped.wasm \
    lib/shipped.package.json \
    lib/shipped.spec.brenn \
    lib/also_shipped.wasm \
    lib/also_shipped.package.json \
    modules/shipped-component.brenn \
    modules/mode-clock.brenn \
    modules/transplant.brenn; do
    [ -f "$tmp/out/$path" ] || fail "the staged tree is missing $path"
done

# The manifest is what decides; a component nobody listed must not ride along —
# nor may its record, which would name an artifact that is not there.
[ ! -e "$tmp/out/lib/test_only.wasm" ] || fail "an unlisted component reached lib/"
[ ! -e "$tmp/out/lib/test_only.package.json" ] || fail "an unlisted component's record reached lib/"

# A replay-world package is two files; a spec beside it is a file the host would
# never read.
[ ! -e "$tmp/out/lib/also_shipped.spec.brenn" ] \
    || fail "a spec-less package staged a spec"

# Staged under the authored basename and the record's kind respectively, and
# byte-identical to what they copy: an import resolves to these bytes and the
# host binds them against the package.
cmp -s "$tmp/in/modules/shipped-component.brenn" "$tmp/out/modules/shipped-component.brenn" \
    || fail "a backend module was not staged verbatim"
cmp -s "$tmp/in/surface/brenn_mode_clock.spec.brenn" "$tmp/out/modules/mode-clock.brenn" \
    || fail "a dom kind's module was not harvested from its packaged copy"
cmp -s "$tmp/in/surface/processor/transplant/transplant.spec.brenn" "$tmp/out/modules/transplant.brenn" \
    || fail "a processor kind's module was not harvested from its packaged copy"

[ -x "$tmp/out/bin/brenn" ] || fail "bin/brenn is not executable"
cmp -s "$tmp/in/manifest.txt" "$tmp/out/lib/deployed-components.txt" \
    || fail "the shipped manifest is not the one that was read"

# ---------------------------------------------------------------------------
# Rejections
# ---------------------------------------------------------------------------
expect_failure() {
    local label="$1" needle="$2" out
    shift 2
    if out=$("$@" 2>&1); then
        fail "$label should be rejected, exited 0: $out"
    # `--` because a needle can name a flag, which grep would read as its own.
    elif ! printf '%s' "$out" | grep -qF -- "$needle"; then
        fail "$label: the rejection does not name the problem: $out"
    fi
}

# The shipping failure mode: a name in the manifest that nothing produces.
printf 'shipped.wasm\nabsent.wasm\n' > "$tmp/in/bad-name.txt"
expect_failure "a manifest naming an unbuilt artifact" "absent.wasm" \
    run "$tmp/out-bad-name" "$tmp/in/bad-name.txt"

# The other shipping failure mode: a manifest entry whose record nobody emitted.
# The artifact would install and the host would refuse it.
expect_failure "a manifest entry with no package" "no component_package target packages" \
    "$assemble" --names "$names" --package-names "$package_names" --dom-names "$dom_names" --record-lib "$record_lib" --out "$tmp/out-nopkg" --manifest "$tmp/in/manifest.txt" \
    --frontend "$tmp/in/frontend" --surface "$tmp/in/surface" \
    --bin "$tmp/in/bin/brenn" \
    --component "$tmp/in/wasm/shipped.wasm" \
    --component "$tmp/in/wasm/also_shipped.wasm" \
    --package "$tmp/in/pkg/also_shipped.package.json"

# A manifest that yields nothing has stopped being read.
printf '# only a comment\n\n' > "$tmp/in/empty.txt"
expect_failure "an empty manifest" "names no components" \
    run "$tmp/out-empty" "$tmp/in/empty.txt"

# An asset tree that was never built.
mkdir -p "$tmp/in/unbuilt"
expect_failure "an empty asset tree" "holds no files" \
    "$assemble" --names "$names" --package-names "$package_names" --dom-names "$dom_names" --record-lib "$record_lib" --out "$tmp/out-unbuilt" --manifest "$tmp/in/manifest.txt" \
    --frontend "$tmp/in/unbuilt" --surface "$tmp/in/surface" \
    --bin "$tmp/in/bin/brenn" --component "$tmp/in/wasm/shipped.wasm" \
    --component "$tmp/in/wasm/also_shipped.wasm" "${packages[@]}"

expect_failure "a non-directory asset tree" "not a directory" \
    "$assemble" --names "$names" --package-names "$package_names" --dom-names "$dom_names" --record-lib "$record_lib" --out "$tmp/out-notdir" --manifest "$tmp/in/manifest.txt" \
    --frontend "$tmp/in/noop_mcp.py" --surface "$tmp/in/surface" \
    --bin "$tmp/in/bin/brenn" --component "$tmp/in/wasm/shipped.wasm" \
    --component "$tmp/in/wasm/also_shipped.wasm" "${packages[@]}"

# Two components with one basename: the manifest names basenames, so one of the
# two would silently win.
mkdir -p "$tmp/in/wasm-dup"
printf '\0asm\1\0\0\0' > "$tmp/in/wasm-dup/shipped.wasm"
expect_failure "two components sharing a basename" "share the basename" \
    "$assemble" --names "$names" --package-names "$package_names" --dom-names "$dom_names" --record-lib "$record_lib" --out "$tmp/out-dup" --manifest "$tmp/in/manifest.txt" \
    --frontend "$tmp/in/frontend" --surface "$tmp/in/surface" \
    --bin "$tmp/in/bin/brenn" \
    --component "$tmp/in/wasm/shipped.wasm" \
    --component "$tmp/in/wasm-dup/shipped.wasm" \
    --component "$tmp/in/wasm/also_shipped.wasm" "${packages[@]}"

expect_failure "no binaries at all" "no --bin given" \
    "$assemble" --names "$names" --package-names "$package_names" --dom-names "$dom_names" --record-lib "$record_lib" --out "$tmp/out-nobin" --manifest "$tmp/in/manifest.txt" \
    --frontend "$tmp/in/frontend" --surface "$tmp/in/surface" \
    --component "$tmp/in/wasm/shipped.wasm" \
    --component "$tmp/in/wasm/also_shipped.wasm" "${packages[@]}"

expect_failure "an unrecognized argument" "unrecognized argument" \
    "$assemble" --names "$names" --package-names "$package_names" --dom-names "$dom_names" --record-lib "$record_lib" --out "$tmp/out-badarg" --whatever

# Each required flag in turn. A rule wired without one of these still fails
# somewhere downstream, but as a `mkdir: cannot create directory '/bin'` or a
# command-not-found rather than as the name of the argument nobody passed.
required_args=(
    --out "$tmp/out-required"
    --manifest "$tmp/in/manifest.txt"
    --names "$names" --package-names "$package_names"
    --dom-names "$dom_names" --record-lib "$record_lib"
    --frontend "$tmp/in/frontend"
    --surface "$tmp/in/surface"
)
for dropped in out manifest names package-names dom-names record-lib frontend surface; do
    argv=()
    for ((i = 0; i < ${#required_args[@]}; i += 2)); do
        [ "${required_args[i]}" = "--$dropped" ] && continue
        argv+=("${required_args[i]}" "${required_args[i + 1]}")
    done
    expect_failure "a missing --$dropped" "--$dropped is required" \
        "$assemble" "${argv[@]}" --bin "$tmp/in/bin/brenn" \
        --component "$tmp/in/wasm/shipped.wasm" \
        --component "$tmp/in/wasm/also_shipped.wasm" "${packages[@]}"
done

# Two modules claiming one import: the root is flat, so one would silently win.
mkdir -p "$tmp/in/modules-dup"
printf 'component Other {}\n' > "$tmp/in/modules-dup/mode-clock.brenn"
expect_failure "a module name claimed twice" "a module root is flat" \
    "$assemble" --names "$names" --package-names "$package_names" \
    --dom-names "$dom_names" --record-lib "$record_lib" \
    --out "$tmp/out-dupmod" --manifest "$tmp/in/manifest.txt" \
    --frontend "$tmp/in/frontend" --surface "$tmp/in/surface" \
    --bin "$tmp/in/bin/brenn" \
    --module "$tmp/in/modules-dup/mode-clock.brenn" \
    --component "$tmp/in/wasm/shipped.wasm" \
    --component "$tmp/in/wasm/also_shipped.wasm" "${packages[@]}"

# A surface kind whose packaged copy did not ship: the tree names the kind, so
# the module a deployment would import is the one thing missing.
mkdir -p "$tmp/in/surface-nospec/processor"
cp -R "$tmp/in/surface/." "$tmp/in/surface-nospec/"
rm "$tmp/in/surface-nospec/brenn_mode_clock.spec.brenn"
expect_failure "a surface kind with no packaged module" "did not ship" \
    "$assemble" --names "$names" --package-names "$package_names" \
    --dom-names "$dom_names" --record-lib "$record_lib" \
    --out "$tmp/out-nospec" --manifest "$tmp/in/manifest.txt" \
    --frontend "$tmp/in/frontend" --surface "$tmp/in/surface-nospec" \
    --bin "$tmp/in/bin/brenn" \
    --component "$tmp/in/wasm/shipped.wasm" \
    --component "$tmp/in/wasm/also_shipped.wasm" "${packages[@]}"

# A record the kind cannot be scraped out of. A missing kind must be a hard
# error; silent absence would let a module root ship without its dom kinds.
mkdir -p "$tmp/in/surface-nokind/processor"
cp -R "$tmp/in/surface/." "$tmp/in/surface-nokind/"
printf '{\n  "version": "1.4.0"\n}\n' > "$tmp/in/surface-nokind/brenn_mode_clock.manifest.json"
expect_failure "a surface record stating no kind" "states no kind" \
    "$assemble" --names "$names" --package-names "$package_names" \
    --dom-names "$dom_names" --record-lib "$record_lib" \
    --out "$tmp/out-nokind" --manifest "$tmp/in/manifest.txt" \
    --frontend "$tmp/in/frontend" --surface "$tmp/in/surface-nokind" \
    --bin "$tmp/in/bin/brenn" \
    --component "$tmp/in/wasm/shipped.wasm" \
    --component "$tmp/in/wasm/also_shipped.wasm" "${packages[@]}"

# A record stating a kind outside the frozen charset: the names tool refuses it,
# and the module a deployment would import can be named by nothing else.
mkdir -p "$tmp/in/surface-badkind/processor"
cp -R "$tmp/in/surface/." "$tmp/in/surface-badkind/"
printf '{\n  "kind": "Mode_Clock"\n}\n' > "$tmp/in/surface-badkind/brenn_mode_clock.manifest.json"
expect_failure "a surface record stating an impossible kind" "no dom kind can be named" \
    "$assemble" --names "$names" --package-names "$package_names" \
    --dom-names "$dom_names" --record-lib "$record_lib" \
    --out "$tmp/out-badkind" --manifest "$tmp/in/manifest.txt" \
    --frontend "$tmp/in/frontend" --surface "$tmp/in/surface-badkind" \
    --bin "$tmp/in/bin/brenn" \
    --component "$tmp/in/wasm/shipped.wasm" \
    --component "$tmp/in/wasm/also_shipped.wasm" "${packages[@]}"

# ---------------------------------------------------------------------------
# Symlinked inputs, as a sandboxed action's are
# ---------------------------------------------------------------------------
mkdir -p "$tmp/in/linked-frontend/skins"
ln -s "$tmp/in/frontend/main.js" "$tmp/in/linked-frontend/main.js"
ln -s "$tmp/in/frontend/skins/dark.css" "$tmp/in/linked-frontend/skins/dark.css"
ln -s "$tmp/in/bin/brenn" "$tmp/in/linked-brenn"

if ! "$assemble" --names "$names" --package-names "$package_names" --dom-names "$dom_names" --record-lib "$record_lib" --out "$tmp/out-linked" --manifest "$tmp/in/manifest.txt" \
    --frontend "$tmp/in/linked-frontend" --surface "$tmp/in/surface" \
    --bin "$tmp/in/linked-brenn" \
    --component "$tmp/in/wasm/shipped.wasm" \
    --component "$tmp/in/wasm/also_shipped.wasm" "${packages[@]}" > "$tmp/linked.log" 2>&1; then
    fail "symlinked inputs should assemble: $(cat "$tmp/linked.log")"
fi
# A copied link rather than its target leaves a dangling path in the tarball,
# which unpacks on the deploy host as a broken file.
[ -f "$tmp/out-linked/frontend/main.js" ] && [ ! -L "$tmp/out-linked/frontend/main.js" ] \
    || fail "a symlinked asset was staged as a link, not as its content"
[ -f "$tmp/out-linked/bin/linked-brenn" ] && [ ! -L "$tmp/out-linked/bin/linked-brenn" ] \
    || fail "a symlinked binary was staged as a link, not as its content"

if [ "$failures" -ne 0 ]; then
    echo "$failures case(s) failed"
    exit 1
fi
echo "assemble: all cases passed"
