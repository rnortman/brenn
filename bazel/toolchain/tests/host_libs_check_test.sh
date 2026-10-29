#!/usr/bin/env bash
# The hermetic C toolchain's binaries may load from the host only the libraries
# every Linux image has: libz, libstdc++, libgcc_s and glibc. Anything else is
# an undeclared system input that a build host can lack, and the loader error
# it causes ("error while loading shared libraries") names no toolchain change.
#
# Usage: host_libs_check_test.sh <readelf> <binary>...
#
# Every binary must list libc.so.6: a binary with no NEEDED entries at all means
# the dynamic section was not read, and an empty read would pass vacuously.
set -uo pipefail

readelf="$1"
shift
failures=0

if [ "$#" -eq 0 ]; then
    echo "FAIL: no toolchain binaries given"
    exit 1
fi

for bin in "$@"; do
    if ! dynamic=$("$readelf" -d "$bin" 2>&1); then
        echo "FAIL: $bin: readelf failed: $dynamic"
        failures=$((failures + 1))
        continue
    fi
    needed=$(sed -n 's/.*(NEEDED).*\[\(.*\)\].*/\1/p' <<< "$dynamic")
    if ! grep -qxF "libc.so.6" <<< "$needed"; then
        echo "FAIL: $bin: no libc.so.6 among NEEDED entries: [${needed//$'\n'/, }]"
        failures=$((failures + 1))
        continue
    fi
    while IFS= read -r lib; do
        case "$lib" in
            libz.so.1 | libm.so.6 | libstdc++.so.6 | libgcc_s.so.1 | libc.so.6 | ld-linux-*.so.*) ;;
            *)
                echo "FAIL: $bin loads $lib from the host"
                failures=$((failures + 1))
                ;;
        esac
    done <<< "$needed"
done

if [ "$failures" -ne 0 ]; then
    echo "$failures failure(s)"
    exit 1
fi
echo "PASS: $# binaries load only libz, libstdc++, libgcc_s and glibc"
