#!/usr/bin/env bash
# Liveness proof for the release tree's assembly.
#
# The real invocation produces a tree the gates then pass over, which says
# nothing about whether the script would notice its inputs going wrong. Here
# every input is a fixture: the happy path is checked layout entry by layout
# entry, and each way the packaging can be handed something broken — a manifest
# naming an artifact nobody built, a manifest that reads as empty, an asset tree
# that was never built, two components with one basename — is checked to fail
# rather than to ship a tarball missing a piece.
set -uo pipefail

names="$1"
assemble="$2"
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
printf 'stub\n' > "$tmp/in/noop_mcp.py"
for name in shipped.wasm also_shipped.wasm test_only.wasm; do
    printf '\0asm\1\0\0\0' > "$tmp/in/wasm/$name"
done

cat > "$tmp/in/manifest.txt" <<'EOF'
# Components shipped to deployments.
shipped.wasm

also_shipped.wasm
EOF

run() {
    "$assemble" \
        --names "$names" \
        --out "$1" \
        --manifest "$2" \
        --frontend "$tmp/in/frontend" \
        --surface "$tmp/in/surface" \
        --bin "$tmp/in/bin/brenn" \
        --bin "$tmp/in/bin/brenn-cli" \
        --lib "$tmp/in/noop_mcp.py" \
        --component "$tmp/in/wasm/shipped.wasm" \
        --component "$tmp/in/wasm/also_shipped.wasm" \
        --component "$tmp/in/wasm/test_only.wasm"
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
    lib/also_shipped.wasm; do
    [ -f "$tmp/out/$path" ] || fail "the staged tree is missing $path"
done

# The manifest is what decides; a component nobody listed must not ride along.
[ ! -e "$tmp/out/lib/test_only.wasm" ] || fail "an unlisted component reached lib/"

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

# A manifest that yields nothing has stopped being read.
printf '# only a comment\n\n' > "$tmp/in/empty.txt"
expect_failure "an empty manifest" "names no components" \
    run "$tmp/out-empty" "$tmp/in/empty.txt"

# An asset tree that was never built.
mkdir -p "$tmp/in/unbuilt"
expect_failure "an empty asset tree" "holds no files" \
    "$assemble" --names "$names" --out "$tmp/out-unbuilt" --manifest "$tmp/in/manifest.txt" \
    --frontend "$tmp/in/unbuilt" --surface "$tmp/in/surface" \
    --bin "$tmp/in/bin/brenn" --component "$tmp/in/wasm/shipped.wasm" \
    --component "$tmp/in/wasm/also_shipped.wasm"

expect_failure "a non-directory asset tree" "not a directory" \
    "$assemble" --names "$names" --out "$tmp/out-notdir" --manifest "$tmp/in/manifest.txt" \
    --frontend "$tmp/in/noop_mcp.py" --surface "$tmp/in/surface" \
    --bin "$tmp/in/bin/brenn" --component "$tmp/in/wasm/shipped.wasm" \
    --component "$tmp/in/wasm/also_shipped.wasm"

# Two components with one basename: the manifest names basenames, so one of the
# two would silently win.
mkdir -p "$tmp/in/wasm-dup"
printf '\0asm\1\0\0\0' > "$tmp/in/wasm-dup/shipped.wasm"
expect_failure "two components sharing a basename" "share the basename" \
    "$assemble" --names "$names" --out "$tmp/out-dup" --manifest "$tmp/in/manifest.txt" \
    --frontend "$tmp/in/frontend" --surface "$tmp/in/surface" \
    --bin "$tmp/in/bin/brenn" \
    --component "$tmp/in/wasm/shipped.wasm" \
    --component "$tmp/in/wasm-dup/shipped.wasm" \
    --component "$tmp/in/wasm/also_shipped.wasm"

expect_failure "no binaries at all" "no --bin given" \
    "$assemble" --names "$names" --out "$tmp/out-nobin" --manifest "$tmp/in/manifest.txt" \
    --frontend "$tmp/in/frontend" --surface "$tmp/in/surface" \
    --component "$tmp/in/wasm/shipped.wasm" \
    --component "$tmp/in/wasm/also_shipped.wasm"

expect_failure "an unrecognized argument" "unrecognized argument" \
    "$assemble" --names "$names" --out "$tmp/out-badarg" --whatever

# Each required flag in turn. A rule wired without one of these still fails
# somewhere downstream, but as a `mkdir: cannot create directory '/bin'` or a
# command-not-found rather than as the name of the argument nobody passed.
required_args=(
    --out "$tmp/out-required"
    --manifest "$tmp/in/manifest.txt"
    --names "$names"
    --frontend "$tmp/in/frontend"
    --surface "$tmp/in/surface"
)
for dropped in out manifest names frontend surface; do
    argv=()
    for ((i = 0; i < ${#required_args[@]}; i += 2)); do
        [ "${required_args[i]}" = "--$dropped" ] && continue
        argv+=("${required_args[i]}" "${required_args[i + 1]}")
    done
    expect_failure "a missing --$dropped" "--$dropped is required" \
        "$assemble" "${argv[@]}" --bin "$tmp/in/bin/brenn" \
        --component "$tmp/in/wasm/shipped.wasm" \
        --component "$tmp/in/wasm/also_shipped.wasm"
done

# ---------------------------------------------------------------------------
# Symlinked inputs, as a sandboxed action's are
# ---------------------------------------------------------------------------
mkdir -p "$tmp/in/linked-frontend/skins"
ln -s "$tmp/in/frontend/main.js" "$tmp/in/linked-frontend/main.js"
ln -s "$tmp/in/frontend/skins/dark.css" "$tmp/in/linked-frontend/skins/dark.css"
ln -s "$tmp/in/bin/brenn" "$tmp/in/linked-brenn"

if ! "$assemble" --names "$names" --out "$tmp/out-linked" --manifest "$tmp/in/manifest.txt" \
    --frontend "$tmp/in/linked-frontend" --surface "$tmp/in/surface" \
    --bin "$tmp/in/linked-brenn" \
    --component "$tmp/in/wasm/shipped.wasm" \
    --component "$tmp/in/wasm/also_shipped.wasm" > "$tmp/linked.log" 2>&1; then
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
