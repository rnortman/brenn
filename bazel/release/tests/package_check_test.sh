#!/usr/bin/env bash
# Liveness proof for the release tree's contract gate.
#
# The gate passes over the real staged tree on every run, which says nothing
# about whether it would notice a missing binary or a component that never got
# copied. Here the tree is a fixture, mutated one way at a time: each mutation
# is a way the tarball can reach the deploy host incomplete, and each must be
# rejected naming what is wrong.
set -uo pipefail

names="$1"
check="$2"
tmp="${TEST_TMPDIR:?TEST_TMPDIR must be set}"
failures=0

fail() {
    echo "FAIL: $1"
    failures=$((failures + 1))
}

manifest="$tmp/manifest.txt"
cat > "$manifest" <<'EOF'
# Components shipped to deployments.
shipped.wasm

also_shipped.wasm
EOF

build_tree() {
    local pkg="$1"
    rm -rf "$pkg"
    mkdir -p "$pkg/bin" "$pkg/frontend" "$pkg/surface" "$pkg/lib"
    printf 'ELF static\n' > "$pkg/bin/brenn"
    printf 'ELF static\n' > "$pkg/bin/brenn-cli"
    chmod +x "$pkg/bin/brenn" "$pkg/bin/brenn-cli"
    printf 'main\n' > "$pkg/frontend/main.js"
    printf 'shell\n' > "$pkg/surface/brenn_kernel.js"
    printf 'stub\n' > "$pkg/lib/noop_mcp.py"
    cp "$manifest" "$pkg/lib/deployed-components.txt"
    printf '\0asm\1\0\0\0' > "$pkg/lib/shipped.wasm"
    printf '\0asm\1\0\0\0' > "$pkg/lib/also_shipped.wasm"
}

pkg="$tmp/pkg"
build_tree "$pkg"

if ! "$check" "$names" "$pkg" "$manifest" dynamic > "$tmp/ok.log" 2>&1; then
    fail "a complete tree should pass: $(cat "$tmp/ok.log")"
fi
if ! "$check" "$names" "$pkg" "$manifest" static > "$tmp/ok-static.log" 2>&1; then
    fail "a complete tree with no loader named should pass static: $(cat "$tmp/ok-static.log")"
fi

reject() {
    local label="$1" needle="$2" linkage="${3:-dynamic}" out
    if out=$("$check" "$names" "$pkg" "$manifest" "$linkage" 2>&1); then
        fail "$label should be rejected, exited 0: $out"
    elif ! printf '%s' "$out" | grep -qF "$needle"; then
        fail "$label: the rejection does not name the problem: $out"
    fi
}

# Every path deploy.sh copies unconditionally.
build_tree "$pkg"; rm "$pkg/bin/brenn-cli"
reject "a missing binary" "bin/brenn-cli is missing"

build_tree "$pkg"; chmod -x "$pkg/bin/brenn"
reject "a non-executable binary" "not executable"

build_tree "$pkg"; : > "$pkg/bin/brenn"
reject "an empty binary" "bin/brenn is empty"

build_tree "$pkg"; rm "$pkg/lib/noop_mcp.py"
reject "a missing MCP stub" "lib/noop_mcp.py is missing"

# The linkage arm, both loaders. A glibc binary carries `ld-linux`, which is the
# musl-request-resolved-to-glibc outcome; a musl binary built without
# `crt-static` carries `ld-musl`, which is one toolchain flag away and names an
# interpreter the glibc deploy host does not have either.
for loader in /lib64/ld-linux-x86-64.so.2 /lib/ld-musl-x86_64.so.1; do
    build_tree "$pkg"; printf 'ELF %s\n' "$loader" > "$pkg/bin/brenn"
    reject "a binary naming $loader in a static build" "not a static build" static
    if ! "$check" "$names" "$pkg" "$manifest" dynamic > "$tmp/dyn.log" 2>&1; then
        fail "the same binary should pass in dynamic mode: $(cat "$tmp/dyn.log")"
    fi
done

# The manifest and the artifacts beside it.
build_tree "$pkg"; rm "$pkg/lib/deployed-components.txt"
reject "a missing manifest" "lib/deployed-components.txt is missing"

build_tree "$pkg"; printf 'shipped.wasm\n' > "$pkg/lib/deployed-components.txt"
reject "a manifest that is not the one checked against" "differs from"

build_tree "$pkg"; rm "$pkg/lib/shipped.wasm"
reject "a component the manifest ships but the tree lacks" "lib/shipped.wasm is missing"

build_tree "$pkg"; printf '\0asm\1\0\0\0' > "$pkg/lib/test_only.wasm"
reject "a component nothing listed" "test_only.wasm"

build_tree "$pkg"
printf '# nothing\n' > "$pkg/lib/deployed-components.txt"
cp "$pkg/lib/deployed-components.txt" "$manifest"
reject "a manifest naming no components" "names no components"
cat > "$manifest" <<'EOF'
# Components shipped to deployments.
shipped.wasm

also_shipped.wasm
EOF

# The served trees.
build_tree "$pkg"; rm -rf "$pkg/surface"
reject "a missing asset tree" "surface/ is missing"

build_tree "$pkg"; rm "$pkg/frontend/main.js"
reject "an asset tree that was never built" "frontend/ holds no files"

# And the gate's own preconditions.
if out=$("$check" "$names" "$tmp/absent" "$manifest" dynamic 2>&1); then
    fail "a package dir that does not exist should be rejected, exited 0: $out"
elif ! printf '%s' "$out" | grep -qF "not a directory"; then
    fail "the rejection does not say what went wrong: $out"
fi
if "$check" "$names" "$pkg" "$manifest" sideways > /dev/null 2>&1; then
    fail "an unrecognized linkage mode should be rejected"
fi

if [ "$failures" -ne 0 ]; then
    echo "$failures case(s) failed"
    exit 1
fi
echo "package_check: all cases passed"
