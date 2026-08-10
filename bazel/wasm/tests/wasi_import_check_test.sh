#!/usr/bin/env bash
# Liveness proof for the WASI-import gate.
#
# Sixteen component targets run that gate and all sixteen pass; nothing in that
# fact distinguishes "no component imports wasi:" from "the pattern stopped
# matching". Here the transcript is canned and the tool is a stub, so both
# directions are asserted: a wasi-importing world is rejected, a clean one is
# accepted, and output that carries no world at all is rejected rather than
# silently satisfying the grep.
set -uo pipefail

check="$1"
tmp="${TEST_TMPDIR:?TEST_TMPDIR must be set}"
failures=0

# A stand-in for `wasm-tools` that prints a fixed transcript whatever it is
# asked. Writes "$tmp/$1"; the transcript is "$2".
stub() {
    printf '%s\n' "$2" > "$tmp/$1.wit"
    printf '#!/usr/bin/env bash\ncat %q\n' "$tmp/$1.wit" > "$tmp/$1"
    chmod +x "$tmp/$1"
}

expect() {
    local want="$1" name="$2" needle="${3:-}"
    local out rc
    out="$("$check" "$tmp/$name" "$tmp/fake.wasm" 2>&1)"
    rc=$?
    if [ "$want" = "pass" ] && [ "$rc" -ne 0 ]; then
        echo "FAIL: $name should have passed, exited $rc: $out"
        failures=$((failures + 1))
        return
    fi
    if [ "$want" = "fail" ] && [ "$rc" -eq 0 ]; then
        echo "FAIL: $name should have been rejected, exited 0: $out"
        failures=$((failures + 1))
        return
    fi
    if [ -n "$needle" ] && ! printf '%s' "$out" | grep -qF "$needle"; then
        echo "FAIL: $name output does not mention '$needle': $out"
        failures=$((failures + 1))
    fi
}

: > "$tmp/fake.wasm"

stub clean 'package root:component;

world root {
  import brenn:processor/ports@0.1.0;
  export receive: func();
}'

stub wasi 'package root:component;

world root {
  import wasi:io/streams@0.2.0;
  import brenn:processor/ports@0.1.0;
}'

stub empty ''

stub reshaped 'package root:component;
nothing else here'

expect pass clean
expect fail wasi "imports wasi:"
expect fail empty "printed no world"
expect fail reshaped "printed no world"

if [ "$failures" -ne 0 ]; then
    echo "$failures case(s) failed"
    exit 1
fi
echo "wasi_import_check: all cases passed"
