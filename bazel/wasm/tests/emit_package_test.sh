#!/usr/bin/env bash
# Liveness proof for the component-package emitter.
#
# The emitter runs once per shipped component over inputs that are correct, so
# those runs prove only that it can succeed. Here it runs over the real
# artifacts — the hashes it writes are what the host will re-compute, so ground
# truth is `sha256sum` and nothing else — and then over the ways a package can
# be wrong: a world tag the artifact's own imports contradict, a world nobody
# links, and a spec present or absent against the world's rule about it.
set -uo pipefail

emit="$1"
processor_wasm="$2"
replay_wasm="$3"
export WASM_TOOLS="$4"
export WIT_LIB="$5"
record_lib="$6"
tmp="${TEST_TMPDIR:?TEST_TMPDIR must be set}"
failures=0

fail() {
    echo "FAIL: $1"
    failures=$((failures + 1))
}

# The same scrape the staged-tree gate reads records with. Sourced rather than
# restated: a test that read the record its own way would go on passing over a
# record shape the gate can no longer read.
# shellcheck source=/dev/null
. "$record_lib"
field() {
    record_field "$1" "$2"
}

spec="$tmp/spec.brenn"
cat > "$spec" <<'EOF'
component Demo {
  abi = processor;
  requires = [];
}
EOF

# --- A processor package: every field, against sha256sum ground truth. ---
record="$tmp/proc.package.json"
packaged_spec="$tmp/brenn_demo.spec.brenn"
if ! "$emit" brenn_demo brenn:processor "$processor_wasm" "$record" "$spec" "$packaged_spec" \
        > "$tmp/proc.log" 2>&1; then
    fail "a processor package should be emitted: $(cat "$tmp/proc.log")"
else
    artifact_sha="$(sha256sum "$processor_wasm" | awk '{print $1}')"
    spec_sha="$(sha256sum "$spec" | awk '{print $1}')"
    for pair in \
        "v:1" \
        "name:brenn_demo" \
        "world:brenn:processor" \
        "artifact:$(basename "$processor_wasm")" \
        "artifact_sha256:$artifact_sha" \
        "spec:brenn_demo.spec.brenn" \
        "spec_sha256:$spec_sha"; do
        key="${pair%%:*}"
        want="${pair#*:}"
        # `v` is a JSON number, so it is read as a bare literal rather than
        # through the string reader every other field uses.
        if [ "$key" = v ]; then
            got="$(sed -n 's/^[[:space:]]*"v"[[:space:]]*:[[:space:]]*\([0-9]\{1,\}\),\{0,1\}[[:space:]]*$/\1/p' "$record")"
        else
            got="$(field "$record" "$key")"
        fi
        if [ "$got" != "$want" ]; then
            fail "record field $key is '$got', expected '$want'"
        fi
    done
    # The packaged copy is the author's bytes, which is what makes the hash in
    # the record the hash of what boot reads.
    if ! cmp -s "$spec" "$packaged_spec"; then
        fail "the packaged spec is not byte-identical to the authored one"
    fi
fi

# --- A replay package: artifact only, and no spec fields at all. ---
replay_record="$tmp/replay.package.json"
if ! "$emit" brenn_replay brenn:replay "$replay_wasm" "$replay_record" \
        > "$tmp/replay.log" 2>&1; then
    fail "a replay package should be emitted: $(cat "$tmp/replay.log")"
else
    if [ "$(field "$replay_record" world)" != "brenn:replay" ]; then
        fail "the replay record does not carry its world"
    fi
    if [ "$(field "$replay_record" artifact_sha256)" != "$(sha256sum "$replay_wasm" | awk '{print $1}')" ]; then
        fail "the replay record's artifact_sha256 is not the artifact's hash"
    fi
    if [ -n "$(field "$replay_record" spec)" ] || [ -n "$(field "$replay_record" spec_sha256)" ]; then
        fail "the replay record carries spec fields, which describe a shape replay has no room for"
    fi
fi

reject() {
    local label="$1" needle="$2"
    shift 2
    local out
    if out=$("$@" 2>&1); then
        fail "$label should be rejected, exited 0: $out"
    elif ! printf '%s' "$out" | grep -qF "$needle"; then
        fail "$label: the rejection does not name the problem: $out"
    fi
}

# The cross-check: the artifact imports `brenn:processor`, so no other world tag
# can be stapled to it. This is the failure a component moved between worlds
# would otherwise carry silently into a release.
reject "a stale world tag" "imports from" \
    "$emit" brenn_demo brenn:replay "$processor_wasm" "$tmp/stale.package.json"

reject "a world nobody links" "not one this host" \
    "$emit" brenn_demo brenn:invented "$processor_wasm" "$tmp/invented.package.json" \
    "$spec" "$tmp/invented.spec.brenn"

# Spec-iff-processor, both directions.
reject "a processor package with no spec" "must package the specification" \
    "$emit" brenn_demo brenn:processor "$processor_wasm" "$tmp/nospec.package.json"

reject "a replay package carrying a spec" "no component class" \
    "$emit" brenn_replay brenn:replay "$replay_wasm" "$tmp/replayspec.package.json" \
    "$spec" "$tmp/replayspec.spec.brenn"

reject "an artifact that is not there" "not a readable file" \
    "$emit" brenn_demo brenn:processor "$tmp/absent.wasm" "$tmp/absent.package.json" \
    "$spec" "$tmp/absent.spec.brenn"

reject "a spec that is not there" "not a readable file" \
    "$emit" brenn_demo brenn:processor "$processor_wasm" "$tmp/nofile.package.json" \
    "$tmp/absent.brenn" "$tmp/nofile.spec.brenn"

# An import line the scrape cannot read fully is the failure that would
# otherwise be silent: the world cross-check finds nothing to contradict and
# writes whatever world the target declared. `wasm-tools` is stubbed because the
# shape is one no artifact this repo builds can produce today — which is the
# point, since the shape that breaks the scrape is the one wasm-tools has not
# printed yet.
stub_tools() {
    local wit="$1" path="$tmp/wasm-tools-stub"
    cat > "$path" <<EOF
#!/usr/bin/env bash
cat <<'WIT'
$wit
WIT
EOF
    chmod +x "$path"
    printf '%s' "$path"
}

unreadable_import="$(stub_tools 'package brenn:processor;

world processor {
  import brenn:processor/log;
  import an-inline-interface;
}')"
WASM_TOOLS="$unreadable_import" reject "an import the scrape cannot read" "fully-qualified" \
    "$emit" brenn_demo brenn:processor "$processor_wasm" "$tmp/unreadable.package.json" \
    "$spec" "$tmp/unreadable.spec.brenn"

# The escaping, through the emitter. Most values are toolchain-controlled
# basenames, but the record is an external contract and a name that broke the
# JSON literal would first be noticed by `serde_json` on the deploy host. The
# order of the two substitutions is the easy thing to get wrong — escaping the
# quote before the backslash doubles the escape it just wrote — so the case
# carries both characters and pins the exact bytes the record must hold.
escaped_record="$tmp/escaped.package.json"
if ! "$emit" 'brenn"de\mo' brenn:processor "$processor_wasm" "$escaped_record" \
        "$spec" "$tmp/escaped.spec.brenn" > "$tmp/escaped.log" 2>&1; then
    fail "a name holding JSON metacharacters should still be emitted: $(cat "$tmp/escaped.log")"
elif ! grep -qF '  "name": "brenn\"de\\mo",' "$escaped_record"; then
    fail "the name is not escaped as a JSON string literal: $(grep name "$escaped_record")"
fi

if [ "$failures" -ne 0 ]; then
    echo "$failures case(s) failed"
    exit 1
fi
echo "emit_package: all cases passed"
