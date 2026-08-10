#!/usr/bin/env bash
# Drive ts_rs_export.sh over stub generators. The real generator is a crate's
# test binary, so the cases that matter here are the ones a passing export can
# still hide: a filter that matches nothing, a generator that fails, and an
# export that lands somewhere other than one flat directory.
set -euo pipefail

export_sh="$(realpath "$1")"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
cd "$work"

fail() {
    echo "FAIL: $1" >&2
    exit 1
}

# The stubs write where ts-rs writes: `export_to` paths reach up out of
# TS_RS_EXPORT_DIR and back down into the frontend tree.
cat >flat <<'STUB'
#!/usr/bin/env bash
dir="$TS_RS_EXPORT_DIR/../../frontend/src/generated"
mkdir -p "$dir"
printf 'export type A = string;\n' >"$dir/A.ts"
printf 'export type B = number;\n' >"$dir/B.ts"
STUB

cat >silent <<'STUB'
#!/usr/bin/env bash
exit 0
STUB

cat >broken <<'STUB'
#!/usr/bin/env bash
echo "the crate did not compile" >&2
exit 101
STUB

cat >split <<'STUB'
#!/usr/bin/env bash
base="$TS_RS_EXPORT_DIR/../../frontend/src/generated"
mkdir -p "$base" "$base/sub"
printf 'export type A = string;\n' >"$base/A.ts"
printf 'export type B = number;\n' >"$base/sub/B.ts"
STUB

# A climb one level past what the nesting contains. The shallow type still
# exports, so every other check here passes while one file is silently missing.
cat >overshoot <<'STUB'
#!/usr/bin/env bash
up="../../../../../../../../../../../../../../../../../.."
near="$TS_RS_EXPORT_DIR/../../frontend/src/generated"
mkdir -p "$near" "$TS_RS_EXPORT_DIR/$up/escaped"
printf 'export type A = string;\n' >"$near/A.ts"
printf 'export type Deep = boolean;\n' >"$TS_RS_EXPORT_DIR/$up/escaped/Deep.ts"
STUB

chmod +x flat silent broken split overshoot

if ! "$export_sh" out ./flat export_bindings_ >/dev/null; then
    fail "a well-behaved generator was rejected"
fi
emitted="$(cd out && find . -type f -printf '%P\n' | LC_ALL=C sort | tr '\n' ' ')"
if [ "$emitted" != "A.ts B.ts " ]; then
    fail "expected the exported files flattened into the out dir, got: $emitted"
fi

if "$export_sh" out_silent ./silent export_bindings_ >/dev/null 2>&1; then
    fail "a generator that exported nothing was accepted"
fi

if "$export_sh" out_broken ./broken export_bindings_ >/dev/null 2>&1; then
    fail "a failing generator was accepted"
fi

if "$export_sh" out_split ./split export_bindings_ >/dev/null 2>&1; then
    fail "an export spanning two directories was accepted"
fi

if out="$("$export_sh" out_overshoot ./overshoot export_bindings_ 2>&1)"; then
    fail "an export climbing past the collection dir was accepted"
elif ! printf '%s' "$out" | grep -qF "outside the collection dir"; then
    fail "the rejection does not say the export escaped: $out"
fi

echo "PASS"
