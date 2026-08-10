#!/usr/bin/env bash
# Liveness proof for the processor staging script.
#
# The staging passes over the real transpile, and the parity pin that reads its
# output reads only the manifest and the copied component — so the transpiled
# modules could all be dangling links and every gate would still be green. Here
# the inputs are fixtures: a transpile directory of symlinks (the shape a
# sandboxed action's inputs actually have) must stage as real files, an empty
# version file must be rejected naming the file, and the copied component must
# land writable, because the emitter runs over the staged tree.
set -uo pipefail

stage="$1"
tmp="${TEST_TMPDIR:?TEST_TMPDIR must be set}"
failures=0

fail() {
    echo "FAIL: $1"
    failures=$((failures + 1))
}

# The real emitter shells out to wasm-tools; this one only records that it was
# handed the staged directory, which is what the copy above it has to produce.
emitter="$tmp/emit.sh"
cat > "$emitter" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
kind="$1"; component="$2"; dest="$3"; version="$4"
printf '{"kind":"%s","jco_version":"%s","files":[' "$kind" "$version" > "$dest/manifest.json"
(cd "$dest" && find -L . -type f -printf '%P\n' | LC_ALL=C sort | paste -sd,) \
    >> "$dest/manifest.json"
printf ']}\n' >> "$dest/manifest.json"
[ -r "$component" ] || { echo "emitter cannot read the component" >&2; exit 1; }
STUB
chmod +x "$emitter"

# A transpile directory whose entries are symlinks, nested as jco's output is.
mkdir -p "$tmp/real/interfaces" "$tmp/transpiled/interfaces"
printf 'export const mod = 1;\n' > "$tmp/real/demo.js"
printf 'export type T = 1;\n' > "$tmp/real/interfaces/ports.d.ts"
ln -s "$tmp/real/demo.js" "$tmp/transpiled/demo.js"
ln -s "$tmp/real/interfaces/ports.d.ts" "$tmp/transpiled/interfaces/ports.d.ts"

printf '\0asm fixture\n' > "$tmp/demo.wasm"
chmod a-w "$tmp/demo.wasm"
printf '1.4.0' > "$tmp/version.txt"

out="$tmp/out"
if ! "$stage" demo "$tmp/demo.wasm" "$tmp/transpiled" "$tmp/version.txt" "$emitter" "$out" \
    > "$tmp/stage.log" 2>&1; then
    fail "staging a well-formed transpile failed: $(cat "$tmp/stage.log")"
fi

dest="$out/processor/demo"
for rel in demo.js interfaces/ports.d.ts; do
    if [ -L "$dest/$rel" ]; then
        fail "$rel staged as a symlink; the served tree would hold a dangling link"
    elif [ ! -f "$dest/$rel" ]; then
        fail "$rel did not reach the staged tree"
    fi
done

# The emitter walks the staged tree, so an unfollowed copy shows up as a
# manifest that lists the component and nothing else.
if ! grep -qF "interfaces/ports.d.ts" "$dest/manifest.json"; then
    fail "the manifest does not list the transpiled modules: $(cat "$dest/manifest.json")"
fi
if ! grep -qF '"1.4.0"' "$dest/manifest.json"; then
    fail "the manifest does not record the version it was given"
fi

# The component is copied out of a read-only input and then rewritten by nothing
# here, but the real emitter needs it writable beside the output.
if [ ! -w "$dest/demo.component.wasm" ]; then
    fail "the staged component is not writable"
fi

# A version file the derivation left empty would put an empty jco version in
# every manifest.
: > "$tmp/empty-version.txt"
if out_text=$("$stage" demo "$tmp/demo.wasm" "$tmp/transpiled" "$tmp/empty-version.txt" \
    "$emitter" "$tmp/out_empty" 2>&1); then
    fail "an empty version file should be rejected, exited 0: $out_text"
elif ! printf '%s' "$out_text" | grep -qF "empty-version.txt"; then
    fail "the rejection does not name the version file: $out_text"
fi

if [ "$failures" -ne 0 ]; then
    echo "$failures case(s) failed"
    exit 1
fi
echo "processor_stage: all cases passed"
