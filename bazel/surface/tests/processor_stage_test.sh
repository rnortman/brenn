#!/usr/bin/env bash
# Liveness proof for the processor staging script.
#
# The staging passes over the real transpile, and the parity pin that reads its
# output reads only the manifest and the copied component — so the transpiled
# modules could all be dangling links and every gate would still be green. Here
# the inputs are fixtures: a transpile directory of symlinks (the shape a
# sandboxed action's inputs actually have) must stage as real files, an empty
# version file must be rejected naming the file, a kind that is not the fold of
# the specification's class must be rejected naming both, and the copied
# component must land writable, because the emitter runs over the staged tree.
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
kind="$1"; component="$2"; dest="$3"; version="$4"; spec_path="$5"
spec_name="$(basename "$spec_path")"
printf '{"kind":"%s","jco_version":"%s","spec":"%s","files":[' \
    "$kind" "$version" "$spec_name" > "$dest/manifest.json"
(cd "$dest" && find -L . -type f -printf '%P\n' | LC_ALL=C sort | paste -sd,) \
    >> "$dest/manifest.json"
printf ']}\n' >> "$dest/manifest.json"
[ -r "$component" ] || { echo "emitter cannot read the component" >&2; exit 1; }
[ -r "$spec_path" ] || { echo "emitter cannot read the spec" >&2; exit 1; }
STUB
chmod +x "$emitter"

# The real one parses the specification; this one answers with whatever the
# fixture puts in a file, which is what lets the mismatch arm be driven without
# a compiler in the test.
dsl_cli="$tmp/dsl_cli.sh"
cat > "$dsl_cli" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
[ "$1" = "wire-kind" ] || { echo "unexpected subcommand $1" >&2; exit 1; }
cat "$(dirname "$2")/wire-kind.txt"
STUB
chmod +x "$dsl_cli"

# A transpile directory whose entries are symlinks, nested as jco's output is.
mkdir -p "$tmp/real/interfaces" "$tmp/transpiled/interfaces"
printf 'export const mod = 1;\n' > "$tmp/real/demo.js"
printf 'export type T = 1;\n' > "$tmp/real/interfaces/ports.d.ts"
ln -s "$tmp/real/demo.js" "$tmp/transpiled/demo.js"
ln -s "$tmp/real/interfaces/ports.d.ts" "$tmp/transpiled/interfaces/ports.d.ts"

printf '\0asm fixture\n' > "$tmp/demo.wasm"
chmod a-w "$tmp/demo.wasm"
printf '1.4.0' > "$tmp/version.txt"
# Read-only, like every source input a sandboxed action is handed.
printf 'component Demo {\n  abi = processor;\n}\n' > "$tmp/demo.brenn"
chmod a-w "$tmp/demo.brenn"
printf 'demo' > "$tmp/wire-kind.txt"

out="$tmp/out"
if ! "$stage" demo "$tmp/demo.wasm" "$tmp/transpiled" "$tmp/version.txt" "$tmp/demo.brenn" \
    "$emitter" "$dsl_cli" "$out" > "$tmp/stage.log" 2>&1; then
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

# The specification is staged under the kind's name, byte-identical to the
# authored file, and early enough that the emitter's file walk lists it.
if ! cmp -s "$tmp/demo.brenn" "$dest/demo.spec.brenn"; then
    fail "the staged specification is not a verbatim copy of the authored file"
fi
if ! grep -qF "demo.spec.brenn" "$dest/manifest.json"; then
    fail "the manifest does not list the staged specification: $(cat "$dest/manifest.json")"
fi
if [ ! -w "$dest/demo.spec.brenn" ]; then
    fail "the staged specification is not writable"
fi

# A version file the derivation left empty would put an empty jco version in
# every manifest.
: > "$tmp/empty-version.txt"
if out_text=$("$stage" demo "$tmp/demo.wasm" "$tmp/transpiled" "$tmp/empty-version.txt" \
    "$tmp/demo.brenn" "$emitter" "$dsl_cli" "$tmp/out_empty" 2>&1); then
    fail "an empty version file should be rejected, exited 0: $out_text"
elif ! printf '%s' "$out_text" | grep -qF "empty-version.txt"; then
    fail "the rejection does not name the version file: $out_text"
fi

# A kind the BUILD author typed that is not the fold of the class name in the
# specification beside it. Every file would stage, every hash would verify, and
# the page would ask for a directory that does not exist.
if out_text=$("$stage" demo-panel "$tmp/demo.wasm" "$tmp/transpiled" "$tmp/version.txt" \
    "$tmp/demo.brenn" "$emitter" "$dsl_cli" "$tmp/out_mismatch" 2>&1); then
    fail "a kind that is not the class's wire kind should be rejected, exited 0: $out_text"
else
    for needle in demo-panel demo demo.brenn; do
        if ! printf '%s' "$out_text" | grep -qF "$needle"; then
            fail "the kind rejection does not name $needle: $out_text"
        fi
    done
    if [ -e "$tmp/out_mismatch/processor" ]; then
        fail "a rejected kind staged files anyway"
    fi
fi

if [ "$failures" -ne 0 ]; then
    echo "$failures case(s) failed"
    exit 1
fi
echo "processor_stage: all cases passed"
