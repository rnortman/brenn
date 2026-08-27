#!/usr/bin/env bash
# Liveness proof for the dom record emitter.
#
# The emitter runs once per dom kind over inputs that are correct, so those runs
# prove only that it can succeed. Here it runs over fixtures: the three hashes
# it writes are what boot validation re-computes, so ground truth is
# `sha256sum` and nothing else, and the record is read back with the same line
# scrape the staged-tree gate uses — which is what keeps the emitted shape and
# the shell readers from drifting apart. The refusal cases are the half no
# production run exercises: a rule wired to the wrong bundle must fail at the
# emit rather than at somebody's boot.
set -uo pipefail

emit="$1"
export WIT_LIB="$2"
record_lib="$3"
export DOM_NAMES="$4"
tmp="${TEST_TMPDIR:?TEST_TMPDIR must be set}"
failures=0

fail() {
    echo "FAIL: $1"
    failures=$((failures + 1))
}

# shellcheck source=/dev/null
. "$record_lib"

kind="mode-clock"
stem="brenn_mode_clock"
src="$tmp/src"
out="$tmp/out"
mkdir -p "$src" "$out"

printf 'export function init() {}\n' > "$src/$stem.js"
printf 'not really wasm, but hashed the same\n' > "$src/${stem}_bg.wasm"
cat > "$src/spec.brenn" <<'EOF'
component ModeClock {
  abi = dom;
  out theme;
}
EOF

record="$out/$stem.manifest.json"
spec_out="$out/$stem.spec.brenn"

if ! "$emit" "$kind" "$src/$stem.js" "$src/${stem}_bg.wasm" "$src/spec.brenn" \
        "$record" "$spec_out" > "$tmp/emit.log" 2>&1; then
    fail "a well-formed module pair should emit: $(cat "$tmp/emit.log")"
fi

module_sha="$(sha256sum "$src/$stem.js" | awk '{print $1}')"
wasm_sha="$(sha256sum "$src/${stem}_bg.wasm" | awk '{print $1}')"
spec_sha="$(sha256sum "$src/spec.brenn" | awk '{print $1}')"
for pair in \
    "kind:$kind" \
    "module:$stem.js" \
    "module_sha256:$module_sha" \
    "module_wasm:${stem}_bg.wasm" \
    "module_wasm_sha256:$wasm_sha" \
    "spec:$stem.spec.brenn" \
    "spec_sha256:$spec_sha"; do
    key="${pair%%:*}"
    want="${pair#*:}"
    got="$(record_field "$record" "$key")"
    if [ "$got" != "$want" ]; then
        fail "$key is $got, expected $want"
    fi
done

# The version is a number, so the string scrape above cannot read it.
if ! grep -qE '^[[:space:]]*"v"[[:space:]]*:[[:space:]]*1,' "$record"; then
    fail "the record does not declare v = 1: $(cat "$record")"
fi

# The packaged copy is the author's file, byte for byte: the hash in the record
# is only a binding if what boot reads is what was hashed.
if ! cmp -s "$src/spec.brenn" "$spec_out"; then
    fail "the packaged specification is not the author's file verbatim"
fi
if [ ! -w "$spec_out" ]; then
    fail "the packaged specification is not writable, so the staging copy cannot re-copy it"
fi

# The hash follows the bytes: an edit in place is a divergence the next emission
# does not paper over.
printf '// edited\n' >> "$src/spec.brenn"
edited_sha="$(sha256sum "$src/spec.brenn" | awk '{print $1}')"
if [ "$edited_sha" = "$spec_sha" ]; then
    fail "the fixture edit did not change the specification's bytes"
fi
if ! "$emit" "$kind" "$src/$stem.js" "$src/${stem}_bg.wasm" "$src/spec.brenn" \
        "$record" "$spec_out" > "$tmp/emit2.log" 2>&1; then
    fail "re-emission over the edited specification failed: $(cat "$tmp/emit2.log")"
elif [ "$(record_field "$record" spec_sha256)" != "$edited_sha" ]; then
    fail "the record's spec hash does not follow the specification's bytes"
fi

# Every name derives from one stem, and the reader re-derives all four from the
# kind. A pair, a wasm sibling or a spec output that does not follow the stem is
# a mis-wired rule, and the emit is where it is cheap to catch.
refuses() {
    local what="$1"
    shift
    if "$@" > "$tmp/refuse.log" 2>&1; then
        fail "$what was accepted: $(cat "$tmp/refuse.log")"
    fi
}

cp "$src/$stem.js" "$src/loader.mjs"
refuses "a module that is not a .js" \
    "$emit" "$kind" "$src/loader.mjs" "$src/${stem}_bg.wasm" "$src/spec.brenn" \
    "$record" "$spec_out"

cp "$src/${stem}_bg.wasm" "$src/brenn_other_bg.wasm"
refuses "a wasm sibling from another stem" \
    "$emit" "$kind" "$src/$stem.js" "$src/brenn_other_bg.wasm" "$src/spec.brenn" \
    "$record" "$spec_out"

refuses "a packaged specification outside the stem" \
    "$emit" "$kind" "$src/$stem.js" "$src/${stem}_bg.wasm" "$src/spec.brenn" \
    "$record" "$out/brenn_other.spec.brenn"

refuses "a record output outside the stem" \
    "$emit" "$kind" "$src/$stem.js" "$src/${stem}_bg.wasm" "$src/spec.brenn" \
    "$out/brenn_other.manifest.json" "$spec_out"

refuses "a kind outside the frozen charset" \
    "$emit" "Mode Clock" "$src/$stem.js" "$src/${stem}_bg.wasm" "$src/spec.brenn" \
    "$record" "$spec_out"

# Names that derive consistently, over files that are not there: the arm past
# the naming checks.
refuses "a module pair that is not there" \
    "$emit" "$kind" "$src/absent/$stem.js" "$src/absent/${stem}_bg.wasm" "$src/spec.brenn" \
    "$record" "$spec_out"

refuses "a wrong argument count" "$emit" "$kind" "$src/$stem.js"

if [ "$failures" -ne 0 ]; then
    echo "$failures case(s) failed"
    exit 1
fi
echo "emit_dom_manifest: all cases passed"
