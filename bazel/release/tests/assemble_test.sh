#!/usr/bin/env bash
# Liveness proof for the release tree's assembly.
#
# The real invocation produces a tree the gates then pass over, which says
# nothing about whether the script would notice its inputs going wrong. Here
# every input is a fixture: the happy path is checked layout entry by layout
# entry, and each way the packaging can be handed something broken — a manifest
# naming a package nobody built, a package with no record, a manifest that reads
# as empty, an asset tree that was never built, two package directories with one
# name — is checked to fail rather than to ship a tarball missing a piece.
#
# The module root gets the same treatment: the backend modules arrive as
# arguments and the surface ones are harvested off the staged tree, so both
# halves are staged here and a name claimed twice is checked to fail.
set -uo pipefail

names="$1"
assemble="$2"
dom_names="$3"
record_lib="$4"
tmp="${TEST_TMPDIR:?TEST_TMPDIR must be set}"
failures=0

fail() {
    echo "FAIL: $1"
    failures=$((failures + 1))
}

mkdir -p "$tmp/in/frontend/skins" "$tmp/in/surface/processor" "$tmp/in/bin"
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

# The build declares each package's files under `<target>/<name>/`, so the
# fixtures do the same: the parent directory's basename is the package name, and
# that is all the assembly reads to group them.
mkdir -p "$tmp/in/pkg/a/shipped" "$tmp/in/pkg/b/also_shipped" "$tmp/in/pkg/c/test_only"
printf '{"v": 2, "name": "shipped", "artifact": "brenn_shipped.wasm"}\n' \
    > "$tmp/in/pkg/a/shipped/package.json"
printf 'component Shipped {}\n' > "$tmp/in/pkg/a/shipped/shipped.brenn"
printf '\0asm\1\0\0\0' > "$tmp/in/pkg/a/shipped/brenn_shipped.wasm"
printf '{"v": 2, "name": "also_shipped", "artifact": "brenn_also_shipped.wasm"}\n' \
    > "$tmp/in/pkg/b/also_shipped/package.json"
printf '\0asm\1\0\0\0' > "$tmp/in/pkg/b/also_shipped/brenn_also_shipped.wasm"
printf '{"v": 2, "name": "test_only", "artifact": "brenn_test_only.wasm"}\n' \
    > "$tmp/in/pkg/c/test_only/package.json"
printf '\0asm\1\0\0\0' > "$tmp/in/pkg/c/test_only/brenn_test_only.wasm"

# Repeated at every invocation below, because a package is not optional: the
# host resolves a component by the directory it installs as.
packages=(
    --package "$tmp/in/pkg/a/shipped/package.json"
    --package "$tmp/in/pkg/a/shipped/shipped.brenn"
    --package "$tmp/in/pkg/a/shipped/brenn_shipped.wasm"
    --package "$tmp/in/pkg/b/also_shipped/package.json"
    --package "$tmp/in/pkg/b/also_shipped/brenn_also_shipped.wasm"
    --package "$tmp/in/pkg/c/test_only/package.json"
    --package "$tmp/in/pkg/c/test_only/brenn_test_only.wasm"
)

cat > "$tmp/in/manifest.txt" <<'EOF'
# Component packages shipped to deployments.
shipped

also_shipped
EOF

run() {
    "$assemble" \
        --names "$names" \
        --dom-names "$dom_names" --record-lib "$record_lib" \
        --module "$tmp/in/modules/shipped-component.brenn" \
        --out "$1" \
        --manifest "$2" \
        --frontend "$tmp/in/frontend" \
        --surface "$tmp/in/surface" \
        --bin "$tmp/in/bin/brenn" \
        --bin "$tmp/in/bin/brenn-cli" \
        --lib "$tmp/in/noop_mcp.py" \
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
    scripts/manifest_names.sh \
    components/deployed-components.txt \
    components/shipped/package.json \
    components/shipped/shipped.brenn \
    components/shipped/brenn_shipped.wasm \
    components/also_shipped/package.json \
    components/also_shipped/brenn_also_shipped.wasm \
    modules/shipped-component.brenn \
    modules/mode-clock.brenn \
    modules/transplant.brenn; do
    [ -f "$tmp/out/$path" ] || fail "the staged tree is missing $path"
done

# The manifest is what decides; a package nobody listed must not ride along.
[ ! -e "$tmp/out/components/test_only" ] \
    || fail "an unlisted package reached components/"

# The grammar tool travels executable, because preflight execs it.
[ -x "$tmp/out/scripts/manifest_names.sh" ] \
    || fail "the staged manifest grammar is not executable"

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
cmp -s "$tmp/in/manifest.txt" "$tmp/out/components/deployed-components.txt" \
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

# The shipping failure mode: a name in the manifest that nothing packages. The
# component would be resolvable by nothing on the host.
printf 'shipped\nabsent\n' > "$tmp/in/bad-name.txt"
expect_failure "a manifest naming an unpackaged component" "no component_package target packages" \
    run "$tmp/out-bad-name" "$tmp/in/bad-name.txt"

# A package directory that holds no record. The host resolves the directory and
# refuses it for want of the one file that binds its contents.
mkdir -p "$tmp/in/pkg-norecord/shipped"
printf '\0asm\1\0\0\0' > "$tmp/in/pkg-norecord/shipped/brenn_shipped.wasm"
expect_failure "a package with no record" "holds no package.json" \
    "$assemble" --names "$names" --dom-names "$dom_names" --record-lib "$record_lib" \
    --out "$tmp/out-norecord" --manifest "$tmp/in/manifest.txt" \
    --frontend "$tmp/in/frontend" --surface "$tmp/in/surface" \
    --bin "$tmp/in/bin/brenn" \
    --package "$tmp/in/pkg-norecord/shipped/brenn_shipped.wasm" \
    --package "$tmp/in/pkg/b/also_shipped/package.json"

# A manifest that yields nothing has stopped being read.
printf '# only a comment\n\n' > "$tmp/in/empty.txt"
expect_failure "an empty manifest" "names no components" \
    run "$tmp/out-empty" "$tmp/in/empty.txt"

# An asset tree that was never built.
mkdir -p "$tmp/in/unbuilt"
expect_failure "an empty asset tree" "holds no files" \
    "$assemble" --names "$names" --dom-names "$dom_names" --record-lib "$record_lib" \
    --out "$tmp/out-unbuilt" --manifest "$tmp/in/manifest.txt" \
    --frontend "$tmp/in/unbuilt" --surface "$tmp/in/surface" \
    --bin "$tmp/in/bin/brenn" "${packages[@]}"

expect_failure "a non-directory asset tree" "not a directory" \
    "$assemble" --names "$names" --dom-names "$dom_names" --record-lib "$record_lib" \
    --out "$tmp/out-notdir" --manifest "$tmp/in/manifest.txt" \
    --frontend "$tmp/in/noop_mcp.py" --surface "$tmp/in/surface" \
    --bin "$tmp/in/bin/brenn" "${packages[@]}"

# Two package directories with one name: the manifest names packages, so one of
# the two would silently win.
mkdir -p "$tmp/in/pkg-dup/shipped"
printf '{"v": 2, "name": "shipped", "artifact": "other.wasm"}\n' \
    > "$tmp/in/pkg-dup/shipped/package.json"
expect_failure "two package directories sharing a name" "two package directories are named shipped" \
    "$assemble" --names "$names" --dom-names "$dom_names" --record-lib "$record_lib" \
    --out "$tmp/out-dup" --manifest "$tmp/in/manifest.txt" \
    --frontend "$tmp/in/frontend" --surface "$tmp/in/surface" \
    --bin "$tmp/in/bin/brenn" \
    --package "$tmp/in/pkg-dup/shipped/package.json" "${packages[@]}"

expect_failure "no binaries at all" "no --bin given" \
    "$assemble" --names "$names" --dom-names "$dom_names" --record-lib "$record_lib" \
    --out "$tmp/out-nobin" --manifest "$tmp/in/manifest.txt" \
    --frontend "$tmp/in/frontend" --surface "$tmp/in/surface" "${packages[@]}"

expect_failure "an unrecognized argument" "unrecognized argument" \
    "$assemble" --names "$names" --dom-names "$dom_names" --record-lib "$record_lib" \
    --out "$tmp/out-badarg" --whatever

# Each required flag in turn. A rule wired without one of these still fails
# somewhere downstream, but as a `mkdir: cannot create directory '/bin'` or a
# command-not-found rather than as the name of the argument nobody passed.
required_args=(
    --out "$tmp/out-required"
    --manifest "$tmp/in/manifest.txt"
    --names "$names"
    --dom-names "$dom_names" --record-lib "$record_lib"
    --frontend "$tmp/in/frontend"
    --surface "$tmp/in/surface"
)
for dropped in out manifest names dom-names record-lib frontend surface; do
    argv=()
    for ((i = 0; i < ${#required_args[@]}; i += 2)); do
        [ "${required_args[i]}" = "--$dropped" ] && continue
        argv+=("${required_args[i]}" "${required_args[i + 1]}")
    done
    expect_failure "a missing --$dropped" "--$dropped is required" \
        "$assemble" "${argv[@]}" --bin "$tmp/in/bin/brenn" "${packages[@]}"
done

# Two modules claiming one import: the root is flat, so one would silently win.
mkdir -p "$tmp/in/modules-dup"
printf 'component Other {}\n' > "$tmp/in/modules-dup/mode-clock.brenn"
expect_failure "a module name claimed twice" "a module root is flat" \
    "$assemble" --names "$names" \
    --dom-names "$dom_names" --record-lib "$record_lib" \
    --out "$tmp/out-dupmod" --manifest "$tmp/in/manifest.txt" \
    --frontend "$tmp/in/frontend" --surface "$tmp/in/surface" \
    --bin "$tmp/in/bin/brenn" \
    --module "$tmp/in/modules-dup/mode-clock.brenn" "${packages[@]}"

# A surface kind whose packaged copy did not ship: the tree names the kind, so
# the module a deployment would import is the one thing missing.
mkdir -p "$tmp/in/surface-nospec/processor"
cp -R "$tmp/in/surface/." "$tmp/in/surface-nospec/"
rm "$tmp/in/surface-nospec/brenn_mode_clock.spec.brenn"
expect_failure "a surface kind with no packaged module" "did not ship" \
    "$assemble" --names "$names" \
    --dom-names "$dom_names" --record-lib "$record_lib" \
    --out "$tmp/out-nospec" --manifest "$tmp/in/manifest.txt" \
    --frontend "$tmp/in/frontend" --surface "$tmp/in/surface-nospec" \
    --bin "$tmp/in/bin/brenn" "${packages[@]}"

# A record the kind cannot be scraped out of. A missing kind must be a hard
# error; silent absence would let a module root ship without its dom kinds.
mkdir -p "$tmp/in/surface-nokind/processor"
cp -R "$tmp/in/surface/." "$tmp/in/surface-nokind/"
printf '{\n  "version": "1.4.0"\n}\n' > "$tmp/in/surface-nokind/brenn_mode_clock.manifest.json"
expect_failure "a surface record stating no kind" "states no kind" \
    "$assemble" --names "$names" \
    --dom-names "$dom_names" --record-lib "$record_lib" \
    --out "$tmp/out-nokind" --manifest "$tmp/in/manifest.txt" \
    --frontend "$tmp/in/frontend" --surface "$tmp/in/surface-nokind" \
    --bin "$tmp/in/bin/brenn" "${packages[@]}"

# A record stating a kind outside the frozen charset: the names tool refuses it,
# and the module a deployment would import can be named by nothing else.
mkdir -p "$tmp/in/surface-badkind/processor"
cp -R "$tmp/in/surface/." "$tmp/in/surface-badkind/"
printf '{\n  "kind": "Mode_Clock"\n}\n' > "$tmp/in/surface-badkind/brenn_mode_clock.manifest.json"
expect_failure "a surface record stating an impossible kind" "no dom kind can be named" \
    "$assemble" --names "$names" \
    --dom-names "$dom_names" --record-lib "$record_lib" \
    --out "$tmp/out-badkind" --manifest "$tmp/in/manifest.txt" \
    --frontend "$tmp/in/frontend" --surface "$tmp/in/surface-badkind" \
    --bin "$tmp/in/bin/brenn" "${packages[@]}"

# ---------------------------------------------------------------------------
# Symlinked inputs, as a sandboxed action's are
# ---------------------------------------------------------------------------
mkdir -p "$tmp/in/linked-frontend/skins"
ln -s "$tmp/in/frontend/main.js" "$tmp/in/linked-frontend/main.js"
ln -s "$tmp/in/frontend/skins/dark.css" "$tmp/in/linked-frontend/skins/dark.css"
ln -s "$tmp/in/bin/brenn" "$tmp/in/linked-brenn"

if ! "$assemble" --names "$names" --dom-names "$dom_names" --record-lib "$record_lib" \
    --out "$tmp/out-linked" --manifest "$tmp/in/manifest.txt" \
    --frontend "$tmp/in/linked-frontend" --surface "$tmp/in/surface" \
    --bin "$tmp/in/linked-brenn" "${packages[@]}" > "$tmp/linked.log" 2>&1; then
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
