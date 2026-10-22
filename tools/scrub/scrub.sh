#!/usr/bin/env bash
# Runs brenn-scrub from runfiles with the pinned gitleaks first on PATH, in the
# directory `bazel run` was invoked from. stdin and the arguments pass through.
set -euo pipefail

# --- begin runfiles.bash initialization v3 ---
# Copy-pasted from the Bazel Bash runfiles library v3.
set -uo pipefail; set +e; f=bazel_tools/tools/bash/runfiles/runfiles.bash
# shellcheck disable=SC1090
source "${RUNFILES_DIR:-/dev/null}/$f" 2>/dev/null || \
  source "$(grep -sm1 "^$f " "${RUNFILES_MANIFEST_FILE:-/dev/null}" | cut -f2- -d' ')" 2>/dev/null || \
  source "$0.runfiles/$f" 2>/dev/null || \
  source "$(grep -sm1 "^$f " "$0.runfiles_manifest" | cut -f2- -d' ')" 2>/dev/null || \
  source "$(grep -sm1 "^$f " "$0.exe.runfiles_manifest" | cut -f2- -d' ')" 2>/dev/null || \
  { echo>&2 "ERROR: cannot find $f"; exit 1; }; f=; set -e
# --- end runfiles.bash initialization v3 ---

if [[ -z "${BUILD_WORKING_DIRECTORY:-}" ]]; then
  echo "brenn-scrub: BUILD_WORKING_DIRECTORY is not set; run this through 'bazel run', not from bazel-bin" >&2
  exit 1
fi

scrub="$(rlocation "$SCRUB_BIN")"
gitleaks="$(rlocation "$GITLEAKS_BIN")"
if [[ -z "$scrub" || ! -x "$scrub" ]]; then
  echo "brenn-scrub: runfile $SCRUB_BIN not found" >&2
  exit 1
fi
if [[ -z "$gitleaks" || ! -x "$gitleaks" ]]; then
  echo "brenn-scrub: runfile $GITLEAKS_BIN not found" >&2
  exit 1
fi

PATH="$(dirname "$gitleaks"):$PATH"
export PATH
cd "$BUILD_WORKING_DIRECTORY"
exec "$scrub" "$@"
