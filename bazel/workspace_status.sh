#!/usr/bin/env bash
# Emits stable stamp keys for `bazel build --stamp`. Only release lanes pass
# --stamp; unstamped builds get the placeholder baked into the target's
# rustc_env, so the build id never enters a dev cache key.
#
# STABLE_ keys invalidate stamped actions when they change; volatile keys do
# not. The build id must invalidate, so it is STABLE_.
#
# No default: this script runs only on the lanes where the id is mandatory, so
# an unset variable is a release build that would ship an unidentifiable
# artifact. Fail instead.
set -euo pipefail

: "${BRENN_BUILD_ID:?must be set for a stamped build; export it or drop --stamp}"

echo "STABLE_BRENN_BUILD_ID ${BRENN_BUILD_ID}"
