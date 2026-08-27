#!/usr/bin/env bash
# Liveness proof for the processor manifest emitter.
#
# The emitter runs once per surface-hosted kind over inputs that are correct, so
# those runs prove only that it can succeed. Here it runs over the real
# transplant artifact — the hashes it writes are what boot validation
# re-computes, so ground truth is `sha256sum` and nothing else — and the record
# is read back with the same line scrape the staged-tree gate uses, which is
# what keeps the emitted shape and the shell readers from drifting apart.
set -uo pipefail

emit="$1"
component_wasm="$2"
export WASM_TOOLS="$3"
export WIT_LIB="$4"
record_lib="$5"
tmp="${TEST_TMPDIR:?TEST_TMPDIR must be set}"
failures=0

fail() {
    echo "FAIL: $1"
    failures=$((failures + 1))
}

# shellcheck source=/dev/null
. "$record_lib"

kind="transplant"
dest="$tmp/processor/$kind"
mkdir -p "$dest/interfaces"

# The tree the stage script has already assembled when the emitter runs: the
# transpiled modules, the component bytes, and the packaged specification.
printf 'export function instantiate() {}\n' > "$dest/$kind.js"
printf 'export type T = 1;\n' > "$dest/interfaces/brenn-processor-ports.d.ts"
cp "$component_wasm" "$dest/$kind.component.wasm"
spec_name="$kind.spec.brenn"
cat > "$dest/$spec_name" <<'EOF'
component Demo {
  abi = processor;
  requires = [ports];
  out out;
}
EOF

if ! "$emit" "$kind" "$dest/$kind.component.wasm" "$dest" 1.4.0 \
        "$dest/$spec_name" > "$tmp/emit.log" 2>&1; then
    fail "a well-formed kind directory should emit: $(cat "$tmp/emit.log")"
fi

record="$dest/manifest.json"
source_sha="$(sha256sum "$dest/$kind.component.wasm" | awk '{print $1}')"
spec_sha="$(sha256sum "$dest/$spec_name" | awk '{print $1}')"
for pair in \
    "kind:$kind" \
    "source_sha256:$source_sha" \
    "jco_version:1.4.0" \
    "spec:$spec_name" \
    "spec_sha256:$spec_sha"; do
    key="${pair%%:*}"
    want="${pair#*:}"
    got="$(record_field "$record" "$key")"
    if [ "$got" != "$want" ]; then
        fail "$key is $got, expected $want"
    fi
done

# The version is a number, so the string scrape above cannot read it.
if ! grep -qE '^[[:space:]]*"v"[[:space:]]*:[[:space:]]*2,' "$record"; then
    fail "the record does not declare v = 2: $(cat "$record")"
fi

# The specification is staged before this runs, so it joins the observed file
# list by construction — and boot validation checks its existence from there.
if ! grep -qF "\"$spec_name\"" "$record"; then
    fail "the file list does not hold the specification: $(cat "$record")"
fi
if ! grep -qF "\"interfaces/brenn-processor-ports.d.ts\"" "$record"; then
    fail "the file list does not hold the transpiled modules: $(cat "$record")"
fi
if grep -qF '"manifest.json"' "$record"; then
    fail "the record lists itself, which no reader can verify: $(cat "$record")"
fi

# Read out of the artifact, never hand-written: an empty profile would pass every
# subset check boot validation makes.
if ! grep -qF '"brenn:processor/ports"' "$record"; then
    fail "the record does not hold the artifact's own imports: $(cat "$record")"
fi

# The hash is over the specification's bytes, so an edit in place is a
# divergence the next emission does not paper over.
printf '// edited\n' >> "$dest/$spec_name"
edited_sha="$(sha256sum "$dest/$spec_name" | awk '{print $1}')"
if [ "$edited_sha" = "$spec_sha" ]; then
    fail "the fixture edit did not change the specification's bytes"
fi
if ! "$emit" "$kind" "$dest/$kind.component.wasm" "$dest" 1.4.0 \
        "$dest/$spec_name" > "$tmp/emit2.log" 2>&1; then
    fail "re-emission over the edited specification failed: $(cat "$tmp/emit2.log")"
elif [ "$(record_field "$record" spec_sha256)" != "$edited_sha" ]; then
    fail "the record's spec hash does not follow the specification's bytes"
fi

# The reader derives `<kind>.spec.brenn` and reads no other file, so a
# specification staged under any other name is a record that would hash a file
# nobody reads — refused here, where the tarball is still being built.
cp "$dest/$spec_name" "$dest/elsewhere.brenn"
if "$emit" "$kind" "$dest/$kind.component.wasm" "$dest" 1.4.0 \
        "$dest/elsewhere.brenn" > "$tmp/emit3.log" 2>&1; then
    fail "a specification outside the derived name was accepted: $(cat "$tmp/emit3.log")"
fi

if [ "$failures" -ne 0 ]; then
    echo "$failures case(s) failed"
    exit 1
fi
echo "emit-processor-manifest: all cases passed"
