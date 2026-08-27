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
package_names="$3"
record_lib="$4"
emit="$5"
export WIT_LIB="$6"
emit_dom="$7"
emit_processor="$8"
dom_names="$9"
export DOM_NAMES="$dom_names"
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
    build_surface_tree "$pkg"
    printf 'stub\n' > "$pkg/lib/noop_mcp.py"
    cp "$manifest" "$pkg/lib/deployed-components.txt"
    printf '\0asm\1\0\0\0' > "$pkg/lib/shipped.wasm"
    printf '\0asm\1\0\0\0' > "$pkg/lib/also_shipped.wasm"

    # The records are written by the build's own emitter, over the fixtures'
    # own bytes. Hand-written ones would prove the gate against a format
    # nothing holds equal to what ships, and a record whose hashes were made up
    # would pass a gate that re-computed nothing.
    "$emit" shipped brenn:processor "$pkg/lib/shipped.wasm" \
        "$pkg/lib/shipped.package.json" "$authored_spec" "$pkg/lib/shipped.spec.brenn"
    "$emit" also_shipped brenn:replay "$pkg/lib/also_shipped.wasm" \
        "$pkg/lib/also_shipped.package.json"
}

# One dom kind and one processor kind, both written by the emitters the build
# uses, for the same reason the component packages are: a surface fixture whose
# records were hand-written would prove the gate against a shape nothing ships.
build_surface_tree() {
    local pkg="$1"
    printf 'export function init() {}\n' > "$pkg/surface/brenn_protobar.js"
    printf '\0asm\1\0\0\0' > "$pkg/surface/brenn_protobar_bg.wasm"
    "$emit_dom" protobar \
        "$pkg/surface/brenn_protobar.js" "$pkg/surface/brenn_protobar_bg.wasm" \
        "$authored_spec" "$pkg/surface/brenn_protobar.manifest.json" \
        "$pkg/surface/brenn_protobar.spec.brenn"

    local kind_dir="$pkg/surface/processor/transplant"
    mkdir -p "$kind_dir"
    printf '\0asm\1\0\0\0' > "$kind_dir/transplant.component.wasm"
    printf 'export function instantiate() {}\n' > "$kind_dir/transplant.js"
    cp "$authored_spec" "$kind_dir/transplant.spec.brenn"
    "$emit_processor" transplant "$kind_dir/transplant.component.wasm" "$kind_dir" \
        1.4.0 "$kind_dir/transplant.spec.brenn"
}

# The emitter reads the artifact's imports through `wasm-tools`, and these
# artifacts are seven bytes of fixture. The stub answers with a world that
# imports nothing, which is all the emitter needs to agree the declared world is
# not contradicted; what is under test here is the gate, not the scrape.
export WASM_TOOLS="$tmp/wasm-tools-stub"
cat > "$WASM_TOOLS" <<'EOF'
#!/usr/bin/env bash
echo "package brenn:fixture;"
EOF
chmod +x "$WASM_TOOLS"

authored_spec="$tmp/authored.brenn"
printf 'component Shipped {}\n' > "$authored_spec"

pkg="$tmp/pkg"
build_tree "$pkg"

if ! "$check" "$names" "$package_names" "$record_lib" "$dom_names" "$pkg" "$manifest" dynamic > "$tmp/ok.log" 2>&1; then
    fail "a complete tree should pass: $(cat "$tmp/ok.log")"
fi
if ! "$check" "$names" "$package_names" "$record_lib" "$dom_names" "$pkg" "$manifest" static > "$tmp/ok-static.log" 2>&1; then
    fail "a complete tree with no loader named should pass static: $(cat "$tmp/ok-static.log")"
fi

reject() {
    local label="$1" needle="$2" linkage="${3:-dynamic}" out
    if out=$("$check" "$names" "$package_names" "$record_lib" "$dom_names" "$pkg" "$manifest" "$linkage" 2>&1); then
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
    if ! "$check" "$names" "$package_names" "$record_lib" "$dom_names" "$pkg" "$manifest" dynamic > "$tmp/dyn.log" 2>&1; then
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

# ---------------------------------------------------------------------------
# The component packages. Each of these reaches the deploy host as a component
# the service refuses to load, with the service already stopped.
# ---------------------------------------------------------------------------
build_tree "$pkg"; rm "$pkg/lib/shipped.package.json"
reject "a component with no binding record" "lib/shipped.package.json is missing"

build_tree "$pkg"; printf '\0asm\1\0\0\1' > "$pkg/lib/shipped.wasm"
reject "an artifact its record does not bind" "hashes to"

build_tree "$pkg"; printf 'component Shipped { abi = processor; }\n' > "$pkg/lib/shipped.spec.brenn"
reject "a spec its record does not bind" "lib/shipped.spec.brenn hashes to"

build_tree "$pkg"; rm "$pkg/lib/shipped.spec.brenn"
reject "a record naming a spec that did not ship" "lib/shipped.spec.brenn is missing or empty"

build_tree "$pkg"; printf 'component Stray {}\n' > "$pkg/lib/also_shipped.spec.brenn"
reject "a spec beside a record that names none" "names no spec"

build_tree "$pkg"
sed -i '/spec_sha256/d' "$pkg/lib/shipped.package.json"
sed -i 's/"spec": "shipped.spec.brenn",/"spec": "shipped.spec.brenn"/' "$pkg/lib/shipped.package.json"
reject "a record naming a spec with no hash" "states no spec_sha256"

# The host reads the stem-derived name and compares the record's `spec` field
# against it, so a record naming any other file is one it refuses — even when
# the file is there and hashes correctly, which is what would otherwise walk
# past this gate.
build_tree "$pkg"
cp "$pkg/lib/shipped.spec.brenn" "$pkg/lib/elsewhere.brenn"
sed -i 's/"spec": "shipped.spec.brenn"/"spec": "elsewhere.brenn"/' "$pkg/lib/shipped.package.json"
reject "a record naming a spec that is not the stem-derived one" "the host derives that name as shipped.spec.brenn"

build_tree "$pkg"
sed -i '/artifact_sha256/d' "$pkg/lib/also_shipped.package.json"
sed -i 's/"artifact": "also_shipped.wasm",/"artifact": "also_shipped.wasm"/' "$pkg/lib/also_shipped.package.json"
reject "a record stating no artifact hash" "states no artifact_sha256"

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

# ---------------------------------------------------------------------------
# The surface asset records. Each of these reaches the deploy host as a tree the
# service refuses at the bounce, with the service already stopped.
# ---------------------------------------------------------------------------
build_tree "$pkg"; rm "$pkg"/surface/brenn_*.manifest.json
reject "a surface tree with no dom record" "holds no brenn_<kind>.manifest.json"

build_tree "$pkg"; printf 'export function init() { /* newer */ }\n' > "$pkg/surface/brenn_protobar.js"
reject "a dom module its record does not bind" "brenn_protobar.js hashes to"

build_tree "$pkg"; printf '\0asm\1\0\0\1' > "$pkg/surface/brenn_protobar_bg.wasm"
reject "a dom module wasm its record does not bind" "brenn_protobar_bg.wasm hashes to"

build_tree "$pkg"; printf 'component Protobar { abi = dom; }\n' > "$pkg/surface/brenn_protobar.spec.brenn"
reject "a dom spec its record does not bind" "brenn_protobar.spec.brenn hashes to"

build_tree "$pkg"; rm "$pkg/surface/brenn_protobar.spec.brenn"
reject "a dom record naming a spec that did not ship" "brenn_protobar.spec.brenn is missing or empty"

build_tree "$pkg"; rm "$pkg/surface/processor/transplant/manifest.json"
reject "a processor kind with no record" "surface/processor/transplant/manifest.json is missing"

build_tree "$pkg"; printf '\0asm\1\0\0\1' > "$pkg/surface/processor/transplant/transplant.component.wasm"
reject "a processor component its record does not bind" "transplant.component.wasm hashes to"

build_tree "$pkg"; printf 'component Transplant { abi = processor; }\n' > "$pkg/surface/processor/transplant/transplant.spec.brenn"
reject "a processor spec its record does not bind" "transplant.spec.brenn hashes to"

build_tree "$pkg"
sed -i 's/"kind": "transplant"/"kind": "elsewhere"/' "$pkg/surface/processor/transplant/manifest.json"
reject "a processor record staged under another kind's name" "but it is staged under transplant"

# The name a record states has to be the name the host derives from the kind.
# Each of these ships a file that is there and hashes correctly, which is what
# would otherwise walk past a gate that only re-hashed what it was pointed at.
build_tree "$pkg"
cp "$pkg/surface/brenn_protobar.js" "$pkg/surface/brenn_elsewhere.js"
sed -i 's/"module": "brenn_protobar.js"/"module": "brenn_elsewhere.js"/' \
    "$pkg/surface/brenn_protobar.manifest.json"
reject "a dom record naming a module the host would not read" "the host derives that name as brenn_protobar.js"

build_tree "$pkg"
cp "$pkg/surface/brenn_protobar.spec.brenn" "$pkg/surface/brenn_elsewhere.spec.brenn"
sed -i 's/"spec": "brenn_protobar.spec.brenn"/"spec": "brenn_elsewhere.spec.brenn"/' \
    "$pkg/surface/brenn_protobar.manifest.json"
reject "a dom record naming a spec the host would not read" "the host derives that name as brenn_protobar.spec.brenn"

build_tree "$pkg"
sed -i 's/"kind": "protobar"/"kind": "mode-clock"/' "$pkg/surface/brenn_protobar.manifest.json"
reject "a dom record filed under a name its kind does not derive" "the host reads as brenn_mode_clock.manifest.json"

# A record must state a kind, and the kind must be one the naming convention
# can derive a filename from — otherwise validation must reject it.
build_tree "$pkg"
sed -i '/"kind":/d' "$pkg/surface/brenn_protobar.manifest.json"
reject "a dom record stating no kind" "states no kind"

build_tree "$pkg"
sed -i 's/"kind": "protobar"/"kind": "Protobar"/' "$pkg/surface/brenn_protobar.manifest.json"
reject "a dom record stating a kind no dom kind can be named" "which no dom kind can be named"

build_tree "$pkg"
cp "$pkg/surface/processor/transplant/transplant.spec.brenn" \
    "$pkg/surface/processor/transplant/elsewhere.brenn"
sed -i 's/"spec": "transplant.spec.brenn"/"spec": "elsewhere.brenn"/' \
    "$pkg/surface/processor/transplant/manifest.json"
reject "a processor record naming a spec the host would not read" "the host derives that name as transplant.spec.brenn"

# And the gate's own preconditions.
if out=$("$check" "$names" "$package_names" "$record_lib" "$dom_names" "$tmp/absent" "$manifest" dynamic 2>&1); then
    fail "a package dir that does not exist should be rejected, exited 0: $out"
elif ! printf '%s' "$out" | grep -qF "not a directory"; then
    fail "the rejection does not say what went wrong: $out"
fi
if "$check" "$names" "$package_names" "$record_lib" "$dom_names" "$pkg" "$manifest" sideways > /dev/null 2>&1; then
    fail "an unrecognized linkage mode should be rejected"
fi

if [ "$failures" -ne 0 ]; then
    echo "$failures case(s) failed"
    exit 1
fi
echo "package_check: all cases passed"
