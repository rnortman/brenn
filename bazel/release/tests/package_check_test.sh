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
record_lib="$3"
emit="$4"
export WIT_LIB="$5"
emit_processor="$6"
tmp="${TEST_TMPDIR:?TEST_TMPDIR must be set}"
failures=0

fail() {
    echo "FAIL: $1"
    failures=$((failures + 1))
}

manifest="$tmp/manifest.txt"
cat > "$manifest" <<'EOF'
# Component packages shipped to deployments.
shipped

also_shipped
EOF

build_tree() {
    local pkg="$1"
    rm -rf "$pkg"
    mkdir -p "$pkg/bin" "$pkg/frontend" "$pkg/surface" "$pkg/lib" "$pkg/scripts" \
        "$pkg/components/shipped" "$pkg/components/also_shipped"
    printf 'ELF static\n' > "$pkg/bin/brenn"
    printf 'ELF static\n' > "$pkg/bin/brenn-cli"
    chmod +x "$pkg/bin/brenn" "$pkg/bin/brenn-cli"
    printf 'main\n' > "$pkg/frontend/main.js"
    build_module_root "$pkg"
    printf 'shell\n' > "$pkg/surface/brenn_kernel.js"
    build_surface_tree "$pkg"
    printf 'stub\n' > "$pkg/lib/noop_mcp.py"
    cp "$names" "$pkg/scripts/manifest_names.sh"
    chmod +x "$pkg/scripts/manifest_names.sh"
    cp "$manifest" "$pkg/components/deployed-components.txt"

    # The artifact basenames are deliberately unrelated to the package names:
    # the record states the artifact and the directory states the package, so a
    # gate that derived one from the other would pass a tree the host refuses.
    printf '\0asm\1\0\0\0' > "$pkg/components/shipped/brenn_one.wasm"
    printf '\0asm\1\0\0\0' > "$pkg/components/also_shipped/brenn_two.wasm"

    # The records are written by the build's own emitter, over the fixtures'
    # own bytes. Hand-written ones would prove the gate against a format
    # nothing holds equal to what ships, and a record whose hashes were made up
    # would pass a gate that re-computed nothing.
    "$emit" shipped brenn:processor "$pkg/components/shipped/brenn_one.wasm" \
        "$pkg/components/shipped/package.json" "$authored_spec" \
        "$pkg/components/shipped/shipped.brenn"
    "$emit" also_shipped brenn:replay "$pkg/components/also_shipped/brenn_two.wasm" \
        "$pkg/components/also_shipped/package.json"
}

# The module root a deployment imports against: a surface kind's module under
# its kind, a backend component's under its authored basename. Each is a copy of
# the specification its package carries, which is the whole of what the gate
# checks — the two authored files here differ from each other so that a module
# joined to the wrong package cannot pass by coincidence.
build_module_root() {
    local pkg="$1"
    mkdir -p "$pkg/modules"
    cp "$authored_spec" "$pkg/modules/shipped.brenn"
    cp "$processor_spec" "$pkg/modules/transplant.brenn"
}

# One processor kind, written by the emitter the build uses, for the same reason
# the component packages are: a surface fixture whose record was hand-written
# would prove the gate against a shape nothing ships.
build_surface_tree() {
    local pkg="$1"
    local kind_dir="$pkg/surface/processor/transplant"
    mkdir -p "$kind_dir"
    printf '\0asm\1\0\0\0' > "$kind_dir/transplant.component.wasm"
    printf 'export function instantiate() {}\n' > "$kind_dir/transplant.js"
    cp "$processor_spec" "$kind_dir/transplant.spec.brenn"
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

# Named for the package it belongs to: the emitter holds a package's spec to its
# own name, because the release stages this file into the module root under it.
mkdir -p "$tmp/authored"
authored_spec="$tmp/authored/shipped.brenn"
printf 'component Shipped {}\n' > "$authored_spec"
processor_spec="$tmp/authored-processor.brenn"
printf 'component Transplant {}\n' > "$processor_spec"

pkg="$tmp/pkg"
build_tree "$pkg"

if ! "$check" "$names" "$record_lib" "$pkg" "$manifest" dynamic > "$tmp/ok.log" 2>&1; then
    fail "a complete tree should pass: $(cat "$tmp/ok.log")"
fi
if ! "$check" "$names" "$record_lib" "$pkg" "$manifest" static > "$tmp/ok-static.log" 2>&1; then
    fail "a complete tree with no loader named should pass static: $(cat "$tmp/ok-static.log")"
fi

reject() {
    local label="$1" needle="$2" linkage="${3:-dynamic}" out
    if out=$("$check" "$names" "$record_lib" "$pkg" "$manifest" "$linkage" 2>&1); then
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
    if ! "$check" "$names" "$record_lib" "$pkg" "$manifest" dynamic > "$tmp/dyn.log" 2>&1; then
        fail "the same binary should pass in dynamic mode: $(cat "$tmp/dyn.log")"
    fi
done

# The manifest, the grammar tool beside it, and the packages it names.
build_tree "$pkg"; rm "$pkg/components/deployed-components.txt"
reject "a missing manifest" "components/deployed-components.txt is missing"

build_tree "$pkg"; printf 'shipped\n' > "$pkg/components/deployed-components.txt"
reject "a manifest that is not the one checked against" "differs from"

build_tree "$pkg"; rm "$pkg/scripts/manifest_names.sh"
reject "a tarball with no manifest grammar" "scripts/manifest_names.sh is missing"

build_tree "$pkg"; chmod -x "$pkg/scripts/manifest_names.sh"
reject "a manifest grammar preflight cannot exec" "scripts/manifest_names.sh is not executable"

build_tree "$pkg"; rm -rf "$pkg/components/shipped"
reject "a package the manifest ships but the tree lacks" \
    "components/shipped/package.json is missing"

build_tree "$pkg"; mkdir -p "$pkg/components/test_only"
reject "a package nothing listed" "test_only"

build_tree "$pkg"; printf 'notes\n' > "$pkg/components/README.txt"
reject "a loose file in the components root" "holds files beside the manifest"

# ---------------------------------------------------------------------------
# The component packages. Each of these reaches the deploy host as a component
# the service refuses to resolve, with the service already stopped.
# ---------------------------------------------------------------------------
build_tree "$pkg"; rm "$pkg/components/shipped/package.json"
reject "a package with no binding record" "components/shipped/package.json is missing"

build_tree "$pkg"; printf '\0asm\1\0\0\1' > "$pkg/components/shipped/brenn_one.wasm"
reject "an artifact its record does not bind" "hashes to"

build_tree "$pkg"; rm "$pkg/components/shipped/brenn_one.wasm"
reject "an artifact that did not ship" "components/shipped/brenn_one.wasm is missing or empty"

build_tree "$pkg"; printf 'component Shipped { abi = processor; }\n' > "$pkg/components/shipped/shipped.brenn"
reject "a spec its record does not bind" "components/shipped/shipped.brenn hashes to"

build_tree "$pkg"; rm "$pkg/components/shipped/shipped.brenn"
reject "a record naming a spec that did not ship" "components/shipped/shipped.brenn is missing or empty"

build_tree "$pkg"; printf 'component Stray {}\n' > "$pkg/components/also_shipped/also_shipped.brenn"
reject "a spec beside a record that names none" "names no spec"

build_tree "$pkg"; printf 'stray\n' > "$pkg/components/shipped/notes.txt"
reject "a file the record does not bind" \
    "components/shipped/ does not hold exactly the files its record binds"

# Not only loose files: a nested directory is content the deploy sync installs
# and no record binds, so the same comparison has to see entries of every type.
build_tree "$pkg"; mkdir -p "$pkg/components/shipped/extra"
printf 'stray\n' > "$pkg/components/shipped/extra/notes.txt"
reject "a directory nested inside a package" \
    "components/shipped/ does not hold exactly the files its record binds"

build_tree "$pkg"; ln -s did-not-ship.wasm "$pkg/components/shipped/dangling.wasm"
reject "a link inside a package whose target did not ship" \
    "components/shipped/ does not hold exactly the files its record binds"

build_tree "$pkg"
sed -i '/spec_sha256/d' "$pkg/components/shipped/package.json"
sed -i 's/"spec": "shipped.brenn",/"spec": "shipped.brenn"/' "$pkg/components/shipped/package.json"
reject "a record naming a spec with no hash" "states no spec_sha256"

# The host reads the name derived from the package and compares the record's
# `spec` field against it, so a record naming any other file is one it refuses —
# even when the file is there and hashes correctly, which is what would
# otherwise walk past this gate.
build_tree "$pkg"
cp "$pkg/components/shipped/shipped.brenn" "$pkg/components/shipped/elsewhere.brenn"
sed -i 's/"spec": "shipped.brenn"/"spec": "elsewhere.brenn"/' "$pkg/components/shipped/package.json"
reject "a record naming a spec that is not the package's own name" \
    "the host derives that name as shipped.brenn"

# The directory is the name a configuration states; the record repeats it, and
# the host holds the two equal.
build_tree "$pkg"
sed -i 's/"name": "shipped"/"name": "elsewhere"/' "$pkg/components/shipped/package.json"
reject "a record staged under another package's name" "calls itself elsewhere"

# The artifact is the one name the record states rather than the host derives,
# so it is held inside the package directory here and nowhere else.
build_tree "$pkg"
sed -i 's|"artifact": "brenn_one.wasm"|"artifact": "../also_shipped/brenn_two.wasm"|' \
    "$pkg/components/shipped/package.json"
reject "a record naming an artifact outside its package" "reaches outside the package directory"

build_tree "$pkg"
sed -i 's/"artifact": "brenn_one.wasm"/"artifact": "brenn_one.txt"/' \
    "$pkg/components/shipped/package.json"
reject "a record naming an artifact that is not a component" "which is not a component"

build_tree "$pkg"
sed -i 's/"artifact": "brenn_one.wasm"/"artifact": ""/' \
    "$pkg/components/shipped/package.json"
reject "a record stating no artifact at all" "states no artifact"

build_tree "$pkg"
sed -i '/artifact_sha256/d' "$pkg/components/also_shipped/package.json"
sed -i 's/"artifact": "brenn_two.wasm",/"artifact": "brenn_two.wasm"/' \
    "$pkg/components/also_shipped/package.json"
reject "a record stating no artifact hash" "states no artifact_sha256"

build_tree "$pkg"
printf '# nothing\n' > "$pkg/components/deployed-components.txt"
cp "$pkg/components/deployed-components.txt" "$manifest"
reject "a manifest naming no components" "names no components"
cat > "$manifest" <<'EOF'
# Component packages shipped to deployments.
shipped

also_shipped
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
# This ships a file that is there and hashes correctly, which is what would
# otherwise walk past a gate that only re-hashed what it was pointed at.
build_tree "$pkg"
cp "$pkg/surface/processor/transplant/transplant.spec.brenn" \
    "$pkg/surface/processor/transplant/elsewhere.brenn"
sed -i 's/"spec": "transplant.spec.brenn"/"spec": "elsewhere.brenn"/' \
    "$pkg/surface/processor/transplant/manifest.json"
reject "a processor record naming a spec the host would not read" "the host derives that name as transplant.spec.brenn"

# ---------------------------------------------------------------------------
# The module root. Each of these reaches the deploy host as a configuration that
# cannot compile, or as one that compiles against bytes nothing installed binds.
# ---------------------------------------------------------------------------
build_tree "$pkg"; rm -rf "$pkg/modules"
reject "a tree with no module root" "modules/ is missing"
# An absent module root must not abort the gate early; later checks and the
# summary must still run.
out=$("$check" "$names" "$record_lib" "$pkg" "$manifest" dynamic 2>&1 || true)
if ! printf '%s' "$out" | grep -qF "problem(s) with the staged release tree"; then
    fail "a tree with no module root: the gate stopped before its summary: $out"
fi

build_tree "$pkg"; rm "$pkg"/modules/*
reject "an empty module root" "modules/ holds no files"

build_tree "$pkg"; printf 'component Transplant { abi = processor; }\n' > "$pkg/modules/transplant.brenn"
reject "a staged module that differs from the kind's packaged copy" \
    "modules/transplant.brenn differs from surface/processor/transplant/transplant.spec.brenn"

build_tree "$pkg"; rm "$pkg/modules/transplant.brenn"
reject "a surface processor kind whose module did not stage" \
    "modules/transplant.brenn is missing or empty"

build_tree "$pkg"; rm "$pkg/modules/shipped.brenn"
reject "a backend package whose module did not stage" \
    "modules/shipped.brenn is missing or empty"

build_tree "$pkg"; printf 'component Shipped { abi = processor; }\n' > "$pkg/modules/shipped.brenn"
reject "a backend module that differs from its packaged copy" \
    "modules/shipped.brenn differs from components/shipped/shipped.brenn"

# The staged module is fine and the copy it stands for is not: a module root is
# only worth what the packaged copies behind it are, so an empty one is named
# here rather than reported as a difference between two files.
build_tree "$pkg"; : > "$pkg/surface/processor/transplant/transplant.spec.brenn"
reject "a packaged copy emptied under its staged module" \
    "stands for nothing"

build_tree "$pkg"; printf 'component Stray {}\n' > "$pkg/modules/stray.brenn"
reject "a module no package stands behind" \
    "modules/stray.brenn is byte-identical to no packaged specification"

build_tree "$pkg"; printf 'notes\n' > "$pkg/modules/README.txt"
reject "a file in the module root that is not a module" \
    "modules/README.txt is not a .brenn module"

# And the gate's own preconditions.
if out=$("$check" "$names" "$record_lib" "$tmp/absent" "$manifest" dynamic 2>&1); then
    fail "a package dir that does not exist should be rejected, exited 0: $out"
elif ! printf '%s' "$out" | grep -qF "not a directory"; then
    fail "the rejection does not say what went wrong: $out"
fi
if "$check" "$names" "$record_lib" "$pkg" "$manifest" sideways > /dev/null 2>&1; then
    fail "an unrecognized linkage mode should be rejected"
fi

if [ "$failures" -ne 0 ]; then
    echo "$failures case(s) failed"
    exit 1
fi
echo "package_check: all cases passed"
