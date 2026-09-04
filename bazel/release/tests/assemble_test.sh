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
# The module root gets the same treatment: both halves are harvested off the
# staged tree — the backend one off each shipped package directory, the surface
# one off the staged surface tree — so the good case asserts `modules/` entry by
# entry, and a name reached both ways is checked to stage once when the two
# copies agree and to fail when they do not. The listed half — a library module,
# owed by no component — is driven separately, because what it claims is the
# opposite: that the tree carries a file the harvest could never have found, and
# a list saying so.
set -uo pipefail

names="$1"
assemble="$2"
record_lib="$3"
stage_lib="$4"
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

# A surface kind's packaged module sits inside the kind's own directory beside
# the record, which is where the harvest looks for it. Nothing in the record is
# scraped here, so the rest of it is elided.
mkdir -p "$tmp/in/surface/processor/transplant"
printf 'component Transplant {}\n' > "$tmp/in/surface/processor/transplant/transplant.spec.brenn"
printf '{\n  "kind": "transplant"\n}\n' > "$tmp/in/surface/processor/transplant/manifest.json"

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
# A specification of its own, so "an unlisted package's module was harvested" is
# a thing the exact-set assertion below can actually see. brenn builds packages
# it does not deploy, and the module root is derived from the packages that
# ship: a harvest reading the built set instead of the listed set would put
# import vocabulary for this one on every host.
printf 'component TestOnly {}\n' > "$tmp/in/pkg/c/test_only/test_only.brenn"

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
    --package "$tmp/in/pkg/c/test_only/test_only.brenn"
)

cat > "$tmp/in/manifest.txt" <<'EOF'
# Component packages shipped to deployments.
shipped

also_shipped
EOF

run() {
    local out="$1" manifest="$2"
    shift 2
    "$assemble" \
        --names "$names" --record-lib "$record_lib" --stage-lib "$stage_lib" \
        --out "$out" \
        --manifest "$manifest" \
        --frontend "$tmp/in/frontend" \
        --surface "$tmp/in/surface" \
        --bin "$tmp/in/bin/brenn" \
        --bin "$tmp/in/bin/brenn-cli" \
        --lib "$tmp/in/noop_mcp.py" \
        "${packages[@]}" "$@"
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
    modules/shipped.brenn \
    modules/transplant.brenn; do
    [ -f "$tmp/out/$path" ] || fail "the staged tree is missing $path"
done

# The manifest is what decides; a package nobody listed must not ride along.
[ ! -e "$tmp/out/components/test_only" ] \
    || fail "an unlisted package reached components/"

# The grammar tool travels executable, because preflight execs it.
[ -x "$tmp/out/scripts/manifest_names.sh" ] \
    || fail "the staged manifest grammar is not executable"

# Exactly what ships, and nothing else. A presence-only list would not notice a
# module appearing (an unshipped package's spec) or disappearing (a harvest that
# stopped running), which is the whole of what the derived module root claims.
staged_modules="$(cd "$tmp/out/modules" && find . -mindepth 1 -printf '%P\n' | LC_ALL=C sort | tr '\n' ' ')"
[ "$staged_modules" = "shipped.brenn transplant.brenn " ] \
    || fail "modules/ holds $staged_modules"

# Byte-identical to what they copy: an import resolves to these bytes and the
# host binds them against the package.
cmp -s "$tmp/in/pkg/a/shipped/shipped.brenn" "$tmp/out/modules/shipped.brenn" \
    || fail "a backend module was not harvested from its packaged copy"
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
    "$assemble" --names "$names" --record-lib "$record_lib" \
    --stage-lib "$stage_lib" \
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
    "$assemble" --names "$names" --record-lib "$record_lib" \
    --stage-lib "$stage_lib" \
    --out "$tmp/out-unbuilt" --manifest "$tmp/in/manifest.txt" \
    --frontend "$tmp/in/unbuilt" --surface "$tmp/in/surface" \
    --bin "$tmp/in/bin/brenn" "${packages[@]}"

expect_failure "a non-directory asset tree" "not a directory" \
    "$assemble" --names "$names" --record-lib "$record_lib" \
    --stage-lib "$stage_lib" \
    --out "$tmp/out-notdir" --manifest "$tmp/in/manifest.txt" \
    --frontend "$tmp/in/noop_mcp.py" --surface "$tmp/in/surface" \
    --bin "$tmp/in/bin/brenn" "${packages[@]}"

# Two package directories with one name: the manifest names packages, so one of
# the two would silently win.
mkdir -p "$tmp/in/pkg-dup/shipped"
printf '{"v": 2, "name": "shipped", "artifact": "other.wasm"}\n' \
    > "$tmp/in/pkg-dup/shipped/package.json"
expect_failure "two package directories sharing a name" "two package directories are named shipped" \
    "$assemble" --names "$names" --record-lib "$record_lib" \
    --stage-lib "$stage_lib" \
    --out "$tmp/out-dup" --manifest "$tmp/in/manifest.txt" \
    --frontend "$tmp/in/frontend" --surface "$tmp/in/surface" \
    --bin "$tmp/in/bin/brenn" \
    --package "$tmp/in/pkg-dup/shipped/package.json" "${packages[@]}"

expect_failure "no binaries at all" "no --bin given" \
    "$assemble" --names "$names" --record-lib "$record_lib" \
    --stage-lib "$stage_lib" \
    --out "$tmp/out-nobin" --manifest "$tmp/in/manifest.txt" \
    --frontend "$tmp/in/frontend" --surface "$tmp/in/surface" "${packages[@]}"

expect_failure "an unrecognized argument" "unrecognized argument" \
    "$assemble" --names "$names" --record-lib "$record_lib" \
    --stage-lib "$stage_lib" \
    --out "$tmp/out-badarg" --whatever

# Each required flag in turn. A rule wired without one of these still fails
# somewhere downstream, but as a `mkdir: cannot create directory '/bin'` or a
# command-not-found rather than as the name of the argument nobody passed.
required_args=(
    --out "$tmp/out-required"
    --manifest "$tmp/in/manifest.txt"
    --names "$names"
    --record-lib "$record_lib"
    --stage-lib "$stage_lib"
    --frontend "$tmp/in/frontend"
    --surface "$tmp/in/surface"
)
for dropped in out manifest names record-lib stage-lib frontend surface; do
    argv=()
    for ((i = 0; i < ${#required_args[@]}; i += 2)); do
        [ "${required_args[i]}" = "--$dropped" ] && continue
        argv+=("${required_args[i]}" "${required_args[i + 1]}")
    done
    expect_failure "a missing --$dropped" "--$dropped is required" \
        "$assemble" "${argv[@]}" --bin "$tmp/in/bin/brenn" "${packages[@]}"
done

# One name reached both ways — a shipped package and a surface kind — is the
# only shape brenn can produce. Byte-identical copies are one authored module
# and stage once; differing ones are two files claiming one import, and which of
# them a deployment compiles against cannot come down to copy order.
mkdir -p "$tmp/in/surface-same/processor/shipped"
cp -R "$tmp/in/surface/." "$tmp/in/surface-same/"
printf '{\n  "kind": "shipped"\n}\n' > "$tmp/in/surface-same/processor/shipped/manifest.json"
cp "$tmp/in/pkg/a/shipped/shipped.brenn" "$tmp/in/surface-same/processor/shipped/shipped.spec.brenn"

if ! "$assemble" --names "$names" --record-lib "$record_lib" \
    --stage-lib "$stage_lib" \
    --out "$tmp/out-samemod" --manifest "$tmp/in/manifest.txt" \
    --frontend "$tmp/in/frontend" --surface "$tmp/in/surface-same" \
    --bin "$tmp/in/bin/brenn" "${packages[@]}" > "$tmp/samemod.log" 2>&1; then
    fail "one authored module at both placements should stage: $(cat "$tmp/samemod.log")"
fi
cmp -s "$tmp/in/pkg/a/shipped/shipped.brenn" "$tmp/out-samemod/modules/shipped.brenn" \
    || fail "the module staged at both placements is not the authored file"

cp -R "$tmp/in/surface-same" "$tmp/in/surface-drift"
printf 'component Shipped {} // authored later\n' \
    > "$tmp/in/surface-drift/processor/shipped/shipped.spec.brenn"
expect_failure "a module name reached twice with differing bytes" "one name is one authored module" \
    "$assemble" --names "$names" --record-lib "$record_lib" \
    --stage-lib "$stage_lib" \
    --out "$tmp/out-driftmod" --manifest "$tmp/in/manifest.txt" \
    --frontend "$tmp/in/frontend" --surface "$tmp/in/surface-drift" \
    --bin "$tmp/in/bin/brenn" "${packages[@]}"

# A surface kind whose packaged copy did not ship: the tree names the kind, so
# the module a deployment would import is the one thing missing.
mkdir -p "$tmp/in/surface-nospec/processor"
cp -R "$tmp/in/surface/." "$tmp/in/surface-nospec/"
rm "$tmp/in/surface-nospec/processor/transplant/transplant.spec.brenn"
expect_failure "a surface kind with no packaged module" "ships no packaged module" \
    "$assemble" --names "$names" --record-lib "$record_lib" \
    --stage-lib "$stage_lib" \
    --out "$tmp/out-nospec" --manifest "$tmp/in/manifest.txt" \
    --frontend "$tmp/in/frontend" --surface "$tmp/in/surface-nospec" \
    --bin "$tmp/in/bin/brenn" "${packages[@]}"

# And one carrying neither its record nor its specification: every kind
# directory is a kind, so an empty one must fail rather than be silently skipped.
mkdir -p "$tmp/in/surface-recordless/processor"
cp -R "$tmp/in/surface/." "$tmp/in/surface-recordless/"
mkdir -p "$tmp/in/surface-recordless/processor/orphan"
expect_failure "a surface kind directory with no record at all" "ships no packaged module" \
    "$assemble" --names "$names" --record-lib "$record_lib" \
    --stage-lib "$stage_lib" \
    --out "$tmp/out-recordless" --manifest "$tmp/in/manifest.txt" \
    --frontend "$tmp/in/frontend" --surface "$tmp/in/surface-recordless" \
    --bin "$tmp/in/bin/brenn" "${packages[@]}"

# ---------------------------------------------------------------------------
# What the staged trees owe the module root
# ---------------------------------------------------------------------------
# The assertion reads the staged trees, so we can drive it over a tree built by
# hand — the only way to present a tree whose harvest never ran.
cp -R "$tmp/out" "$tmp/out-lostharvest"
rm -f "$tmp/out-lostharvest/modules/"*
if ! (. "$stage_lib"; stage_assert_modules_owed "$tmp/out") > "$tmp/owed.log" 2>&1; then
    fail "a tree whose modules were harvested should pass: $(cat "$tmp/owed.log")"
fi
expect_failure "a staged tree whose harvest did not run" "modules/shipped.brenn" \
    bash -c '. "$1"; stage_assert_modules_owed "$2"' bash "$stage_lib" "$tmp/out-lostharvest"
expect_failure "a staged tree whose harvest did not run" "modules/transplant.brenn" \
    bash -c '. "$1"; stage_assert_modules_owed "$2"' bash "$stage_lib" "$tmp/out-lostharvest"

# ---------------------------------------------------------------------------
# The listed half of the module root
# ---------------------------------------------------------------------------
# A library module is vocabulary the release ships that no package and no
# surface kind carries, so the harvest cannot reach it and every checker's
# pair-it-with-an-owner rule cannot pass it. The list is what carries the fact.
mkdir -p "$tmp/in/lib-modules"
printf 'assembly Commons() {}\n' > "$tmp/in/lib-modules/surface-description.brenn"
printf 'assembly More() {}\n' > "$tmp/in/lib-modules/aardvark.brenn"

if ! run "$tmp/out-lib" "$tmp/in/manifest.txt" \
    --library-module "$tmp/in/lib-modules/surface-description.brenn" \
    --library-module "$tmp/in/lib-modules/aardvark.brenn" > "$tmp/lib.log" 2>&1; then
    fail "a release with library modules should assemble: $(cat "$tmp/lib.log")"
fi

staged_lib="$(cd "$tmp/out-lib/modules" && find . -mindepth 1 -printf '%P\n' | LC_ALL=C sort | tr '\n' ' ')"
[ "$staged_lib" = "aardvark.brenn library-modules.txt shipped.brenn surface-description.brenn transplant.brenn " ] \
    || fail "modules/ with library modules holds $staged_lib"
cmp -s "$tmp/in/lib-modules/surface-description.brenn" \
    "$tmp/out-lib/modules/surface-description.brenn" \
    || fail "a library module was not staged from its authored copy"

# Sorted and one per line: the list is a function of the set, not of the
# caller's argument order, so a reader can compare two releases' lists.
listed="$(cat "$tmp/out-lib/modules/library-modules.txt")"
[ "$listed" = "$(printf 'aardvark.brenn\nsurface-description.brenn')" ] \
    || fail "library-modules.txt reads: $listed"

# A release that lists none carries no list file.
[ ! -e "$tmp/out/modules/library-modules.txt" ] \
    || fail "a release with no library modules staged a list anyway"

# A listed name that a component's own authored module already holds: two files
# under one import, decided by copy order.
expect_failure "a library module shadowing a component's module" \
    "one name is one authored module" \
    run "$tmp/out-lib-shadow" "$tmp/in/manifest.txt" \
    --library-module "$tmp/in/pkg/a/shipped/shipped.brenn"

# Two roots each shipping a `commons.brenn`: the same collision, and a refusal
# that says which of the two ways a name was already taken.
mkdir -p "$tmp/in/lib-modules-other"
printf 'assembly Other() {}\n' > "$tmp/in/lib-modules-other/aardvark.brenn"
expect_failure "one basename listed twice" \
    "which another library module of this tree is listed under" \
    run "$tmp/out-lib-twice" "$tmp/in/manifest.txt" \
    --library-module "$tmp/in/lib-modules/aardvark.brenn" \
    --library-module "$tmp/in/lib-modules-other/aardvark.brenn"

printf 'not a module\n' > "$tmp/in/lib-modules/notes.txt"
expect_failure "a listed file that is not a module" "is not a .brenn file" \
    run "$tmp/out-lib-ext" "$tmp/in/manifest.txt" \
    --library-module "$tmp/in/lib-modules/notes.txt"

# ---------------------------------------------------------------------------
# Symlinked inputs, as a sandboxed action's are
# ---------------------------------------------------------------------------
mkdir -p "$tmp/in/linked-frontend/skins"
ln -s "$tmp/in/frontend/main.js" "$tmp/in/linked-frontend/main.js"
ln -s "$tmp/in/frontend/skins/dark.css" "$tmp/in/linked-frontend/skins/dark.css"
ln -s "$tmp/in/bin/brenn" "$tmp/in/linked-brenn"

if ! "$assemble" --names "$names" --record-lib "$record_lib" \
    --stage-lib "$stage_lib" \
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
