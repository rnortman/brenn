# The artifact's import list, and the JSON escaping its readers emit it with.
#
# Sourced, not executed: `WIT_LIB` names this file, defaulting to its path
# relative to the execroot, which is where every action that needs it runs.
#
# Two emitters read `wasm-tools component wit` output — the surface processor
# manifest and the component package record — and both judge an artifact by the
# imports they scrape out of it. Two independent scrapes would be two things to
# fix when wasm-tools changes how it prints an import, and the one that was not
# fixed would go on matching nothing, which for both readers is indistinguishable
# from an artifact with no imports. One implementation, so a format change
# cannot reach one caller and miss the other.
#
# WASM_TOOLS names the `wasm-tools` binary; unset, one is looked up on PATH.

# Every host interface the artifact imports, fully qualified as `ns:pkg/iface`,
# one per line, byte-sorted and deduplicated. Version suffixes are dropped; the
# package namespace is kept, because both callers judge by it.
#
# Usage: wit_imports <artifact>
wit_imports() {
    _wit_imports_impl "$1" _wit_capture
}

# The same list with each name's `@version` suffix intact.
#
# The grant-parity check judges an import against the exact canonical name the
# host links, version included, so it cannot be fed the stripped list: a name
# whose version drifted would arrive indistinguishable from one that did not,
# and the check would answer "these agree" about a component the host refuses at
# load. The two emitters that judge by interface identity alone keep using
# `wit_imports`.
#
# Usage: wit_imports_versioned <artifact>
wit_imports_versioned() {
    _wit_imports_impl "$1" _wit_capture_versioned
}

# The scrape, the guard over it, and the ordering — parameterized by which
# capture emits. One implementation so the guard always measures the list that
# ships.
_wit_imports_impl() {
    local artifact="$1"
    local capture="$2"
    local wit raw_count captured_count
    wit="$("${WASM_TOOLS:-wasm-tools}" component wit "$artifact")"

    # The capture requires a `ns:pkg/…` shape. An import line it does not
    # consume is not a no-op: it vanishes from what the caller judges, so the
    # surface manifest under-reports its own import profile and the package
    # record's world cross-check finds nothing to contradict and writes whatever
    # world was declared. Both failures are silent and survive releases. Refuse
    # to emit instead.
    raw_count=$(printf '%s\n' "$wit" | grep -c '^[[:space:]]*import[[:space:]]' || true)
    captured_count=$(printf '%s\n' "$wit" | "$capture" | grep -c . || true)
    if [ "$raw_count" -ne "$captured_count" ]; then
        echo "wit_imports: $artifact has $raw_count import lines but only $captured_count are" \
             "fully-qualified \`ns:pkg/iface\` imports." >&2
        echo "An unqualified or non-interface import cannot be judged: it would silently leave" \
             "the import list this artifact is checked against. Fix the component's world, or" \
             "extend this scrape and processor_component_imports together." >&2
        return 1
    fi

    # LC_ALL=C pins byte order, matching the Rust twin's `sort_unstable()` on
    # `String`. Without it the collation follows the invoking environment's
    # locale, where punctuation weights differ from byte values, so the emitted
    # order — and the parity assertion against the twin — would depend on the
    # machine that ran the build.
    printf '%s\n' "$wit" | "$capture" | LC_ALL=C sort -u
}

# The scrape itself, over stdin, with each name's version intact. One
# expression: the counting pass and the emitting pass must agree on what
# "captured" means or the guard measures something other than what ships.
_wit_capture_versioned() {
    sed -n 's/^[[:space:]]*import[[:space:]]\{1,\}\([A-Za-z0-9_-]\{1,\}:[^;[:space:]]*\);.*/\1/p'
}

# The same scrape with the `@version` suffix dropped.
_wit_capture() {
    _wit_capture_versioned | sed 's/@[^/]*$//'
}

# Escape a string for embedding as a JSON string literal: backslash and
# double-quote are the two characters that break the literal (a bare `"` ends
# it, a bare `\` starts an escape). Most values these emitters write are
# toolchain-controlled basenames, but some are *observed* from a tool's output,
# and both files are external contracts whose readers should not have to assume
# an unescaped emitter.
json_escape() {
    local s="$1"
    s="${s//\\/\\\\}"
    s="${s//\"/\\\"}"
    printf '%s' "$s"
}
