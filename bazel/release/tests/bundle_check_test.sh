#!/usr/bin/env bash
# Liveness proof for the component-tree contract gate.
#
# The gate passes over brenn's own release tree on every run, which says nothing
# about whether it would notice a package that never got copied or a kind whose
# record binds bytes that changed. Here the tree is a fixture, mutated one way at
# a time. Two shapes are covered that brenn's tarball never takes and a bundle
# does: a tree with no `components/` at all, and a tree with no `surface/`.
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
EOF

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
# own name. The two authored files differ from each other so that a module
# joined to the wrong component cannot pass by coincidence.
mkdir -p "$tmp/authored"
authored_spec="$tmp/authored/shipped.brenn"
printf 'component Shipped {}\n' > "$authored_spec"
kind_spec="$tmp/authored/panel.spec.brenn"
printf 'component Panel { abi = processor; }\n' > "$kind_spec"

# The records are written by the build's own emitters, over the fixtures' own
# bytes. Hand-written ones would prove the gate against a format nothing holds
# equal to what ships, and a record whose hashes were made up would pass a gate
# that re-computed nothing.
build_components() {
    local tree="$1"
    mkdir -p "$tree/components/shipped" "$tree/scripts"
    cp "$names" "$tree/scripts/manifest_names.sh"
    chmod +x "$tree/scripts/manifest_names.sh"
    cp "$manifest" "$tree/components/deployed-components.txt"
    printf '\0asm\1\0\0\0' > "$tree/components/shipped/brenn_one.wasm"
    "$emit" shipped brenn:processor "$tree/components/shipped/brenn_one.wasm" \
        "$tree/components/shipped/package.json" "$authored_spec" \
        "$tree/components/shipped/shipped.brenn"
    cp "$authored_spec" "$tree/modules/shipped.brenn"
}

build_surface() {
    local tree="$1" kind_dir="$1/surface/processor/panel"
    mkdir -p "$kind_dir"
    printf '\0asm\1\0\0\0' > "$kind_dir/panel.component.wasm"
    printf 'export function instantiate() {}\n' > "$kind_dir/panel.js"
    cp "$kind_spec" "$kind_dir/panel.spec.brenn"
    "$emit_processor" panel "$kind_dir/panel.component.wasm" "$kind_dir" \
        1.4.0 "$kind_dir/panel.spec.brenn"
    cp "$kind_spec" "$tree/modules/panel.brenn"
}

# A replay-world package: artifact and record, no specification, and so no
# module. The whole tree owes nothing to the module root.
build_replay() {
    local tree="$1"
    mkdir -p "$tree/components/shipped" "$tree/scripts"
    cp "$names" "$tree/scripts/manifest_names.sh"
    chmod +x "$tree/scripts/manifest_names.sh"
    cp "$manifest" "$tree/components/deployed-components.txt"
    printf '\0asm\1\0\0\0' > "$tree/components/shipped/brenn_one.wasm"
    "$emit" shipped brenn:replay "$tree/components/shipped/brenn_one.wasm" \
        "$tree/components/shipped/package.json"
}

build_tree() {
    local tree="$1" shape="${2:-both}"
    rm -rf "$tree"
    mkdir -p "$tree/modules"
    case "$shape" in
        both) build_components "$tree"; build_surface "$tree" ;;
        components) build_components "$tree" ;;
        surface) build_surface "$tree" ;;
        replay) build_replay "$tree" ;;
    esac
}

tree="$tmp/tree"

build_tree "$tree"
if ! "$check" "$names" "$record_lib" "$tree" --manifest "$manifest" > "$tmp/ok.log" 2>&1; then
    fail "a complete tree should pass: $(cat "$tmp/ok.log")"
fi

# A bundle need not ship both placements, and a gate that required either would
# refuse the two shapes a component repository is most likely to release.
build_tree "$tree" components
if ! "$check" "$names" "$record_lib" "$tree" --manifest "$manifest" > "$tmp/be.log" 2>&1; then
    fail "a tree with no surface kinds should pass: $(cat "$tmp/be.log")"
fi
build_tree "$tree" surface
if ! "$check" "$names" "$record_lib" "$tree" > "$tmp/fe.log" 2>&1; then
    fail "a tree with no packages should pass with no manifest: $(cat "$tmp/fe.log")"
fi

# A bundle of replay-world packages alone: its components are named by a
# `replay_protection` block's `component =`, not by an import, so its module
# root is empty on purpose. Requiring a module here would leave an out-of-tree
# replay author with a bundle no gate would pass.
build_tree "$tree" replay
if ! "$check" "$names" "$record_lib" "$tree" --manifest "$manifest" > "$tmp/replay.log" 2>&1; then
    fail "a replay-only tree should pass with an empty module root: $(cat "$tmp/replay.log")"
fi

reject() {
    local label="$1" needle="$2" out
    shift 2
    if out=$("$check" "$names" "$record_lib" "$tree" "$@" 2>&1); then
        fail "$label should be rejected, exited 0: $out"
    elif ! printf '%s' "$out" | grep -qF "$needle"; then
        fail "$label: the rejection does not name the problem: $out"
    fi
}

# The caller's own arguments. A tree carrying packages and checked without a
# manifest is a check that asserts nothing about the set that ships.
build_tree "$tree"
reject "a components tree with nothing to check the manifest against" \
    "no --manifest was given"

# The manifest, the grammar tool beside it, and the packages it names.
build_tree "$tree"; rm "$tree/components/deployed-components.txt"
reject "a missing manifest" "components/deployed-components.txt is missing" --manifest "$manifest"

build_tree "$tree"; printf 'shipped\nalso\n' > "$tree/components/deployed-components.txt"
reject "a manifest that is not the one checked against" "differs from" --manifest "$manifest"

build_tree "$tree"; rm "$tree/scripts/manifest_names.sh"
reject "a tree with no manifest grammar" "scripts/manifest_names.sh is missing" --manifest "$manifest"

build_tree "$tree"; mkdir -p "$tree/components/test_only"
reject "a package nothing listed" "test_only" --manifest "$manifest"

build_tree "$tree"; printf '\0asm\1\0\0\1' > "$tree/components/shipped/brenn_one.wasm"
reject "an artifact its record does not bind" "hashes to" --manifest "$manifest"

build_tree "$tree"; printf 'stray\n' > "$tree/components/shipped/notes.txt"
reject "a file the record does not bind" \
    "components/shipped/ does not hold exactly the files its record binds" --manifest "$manifest"

# ---------------------------------------------------------------------------
# The surface asset records. Each of these reaches the target host as a tree the
# service refuses at the bounce, with the service already stopped.
# ---------------------------------------------------------------------------
build_tree "$tree" surface; rm "$tree/surface/processor/panel/manifest.json"
reject "a processor kind with no record" "surface/processor/panel/manifest.json is missing"

build_tree "$tree" surface
printf '\0asm\1\0\0\1' > "$tree/surface/processor/panel/panel.component.wasm"
reject "a processor component its record does not bind" "panel.component.wasm hashes to"

build_tree "$tree" surface
sed -i 's/"kind": "panel"/"kind": "elsewhere"/' "$tree/surface/processor/panel/manifest.json"
reject "a processor record staged under another kind's name" "but it is staged under panel"

build_tree "$tree" surface
sed -i 's/"v": 2,/"v": 1,/' "$tree/surface/processor/panel/manifest.json"
reject "a record version the host does not read" "states record version 1"

# The record's file list is what boot validation walks, so a file it does not
# list is one the host never verifies and the install sync copies anyway.
build_tree "$tree" surface
printf 'stray\n' > "$tree/surface/processor/panel/extra.js"
reject "a file no record lists" \
    "surface/processor/panel/ does not hold exactly the files its record lists"

build_tree "$tree" surface; rm "$tree/surface/processor/panel/panel.js"
reject "a transpiled file the record lists and the tree lacks" \
    "surface/processor/panel/ does not hold exactly the files its record lists"

# ---------------------------------------------------------------------------
# The module root. Each of these reaches the target host as a configuration that
# cannot compile, or as one that compiles against bytes nothing installed binds.
# ---------------------------------------------------------------------------
build_tree "$tree"; rm -rf "$tree/modules"
reject "a tree with no module root" "modules/ is missing" --manifest "$manifest"

build_tree "$tree"; rm "$tree"/modules/*
reject "an empty module root" "modules/ holds no files" --manifest "$manifest"

build_tree "$tree"; rm "$tree/modules/panel.brenn"
reject "a surface kind whose module did not stage" \
    "modules/panel.brenn is missing or empty" --manifest "$manifest"

build_tree "$tree"; printf 'component Shipped { abi = processor; }\n' > "$tree/modules/shipped.brenn"
reject "a backend module that differs from its packaged copy" \
    "modules/shipped.brenn differs from components/shipped/shipped.brenn" --manifest "$manifest"

build_tree "$tree"; printf 'component Stray {}\n' > "$tree/modules/stray.brenn"
reject "a module no package stands behind" \
    "modules/stray.brenn is byte-identical to no packaged specification" --manifest "$manifest"

build_tree "$tree"; printf 'notes\n' > "$tree/modules/README.txt"
reject "a file in the module root that is not a module" \
    "modules/README.txt is not a .brenn module" --manifest "$manifest"

# The candidate globs are what a caller with a placement of its own states
# instead of the defaults; stating one that matches nothing must not quietly
# pass the module it was meant to stand behind.
build_tree "$tree" surface
if out=$("$check" "$names" "$record_lib" "$tree" --module-candidate "$tree/nowhere/*.brenn" 2>&1); then
    fail "a candidate glob matching nothing should leave every module unbacked: $out"
elif ! printf '%s' "$out" | grep -qF "modules/panel.brenn is byte-identical to no packaged specification"; then
    fail "a candidate glob matching nothing: the rejection does not name the problem: $out"
fi

# And the gate's own preconditions.
if out=$("$check" "$names" "$record_lib" "$tmp/absent" 2>&1); then
    fail "a tree that does not exist should be rejected, exited 0: $out"
elif ! printf '%s' "$out" | grep -qF "not a directory"; then
    fail "the rejection does not say what went wrong: $out"
fi

if [ "$failures" -ne 0 ]; then
    echo "$failures case(s) failed"
    exit 1
fi
echo "bundle_check: all cases passed"
