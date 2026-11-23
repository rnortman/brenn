#!/usr/bin/env bash
# Records the Bazel files the `xtask timing` reader's fixture tests parse.
#
#   xtask/tests/fixtures/timing/record-fixtures.sh
#
# Run by hand from the repository root, and only when `.bazelversion` changes:
# the reader's version test fails until the fixtures are the pinned Bazel's.
# Needs `bazel` (bazelisk), `jq` and `zstd` on PATH, and network access the
# first time a Bazel version or `rules_shell` is fetched.
#
# A scratch workspace in a temporary directory holds one genrule, one failing
# genrule (tagged manual, so `//...` leaves it out), one test and one binary.
# Seven invocations run there with no rc file and a disk cache of their own,
# each writing a build event file and a compact execution log:
#
#   cold     bazel test //...      everything executes, the test runs
#   warm     the same again: nothing executes, the test is cached locally
#   cached   the same after `bazel clean`: spawns and the test come from the
#            disk cache
#   option   the same with a changed --define: the analysis-cache discard
#   failing  bazel build //:fails  a genrule exits 3
#   cquery   bazel cquery //...
#   run      bazel run //:hello
#
# Each build event file is cut down to the events the reader reads — `started`,
# `buildFinished`, `buildMetrics`, `testResult`, `progress` — so no command
# line, option list, workspace status or exec request (which carries the
# environment) is kept, nor is the option list in `started`; a test result's
# output URIs (which name the output base) are dropped, and the paths and host
# in `started` are replaced by generic values. The execution logs are kept as
# Bazel wrote them; if any file names the scratch directory, the user, the host
# or the home directory, the script writes no fixture at all.
set -euo pipefail

repo=$(git rev-parse --show-toplevel)
out="$repo/xtask/tests/fixtures/timing"
version=$(<"$repo/.bazelversion")

scratch=$(mktemp -d)
cleanup() {
    (cd "$scratch/ws" && bazel shutdown) >/dev/null 2>&1 || true
    rm -rf "$scratch"
}
trap cleanup EXIT

ws="$scratch/ws"
mkdir -p "$ws" "$scratch/files"
printf '%s\n' "$version" >"$ws/.bazelversion"
cat >"$ws/MODULE.bazel" <<'EOF'
module(name = "fixture")

bazel_dep(name = "rules_shell", version = "0.6.1")
EOF
cat >"$ws/BUILD.bazel" <<'EOF'
load("@rules_shell//shell:sh_binary.bzl", "sh_binary")
load("@rules_shell//shell:sh_test.bzl", "sh_test")

genrule(
    name = "slow",
    outs = ["slow.txt"],
    cmd = "sleep 0.3; echo $(FIXTURE_VALUE) > $@",
)

genrule(
    name = "fails",
    outs = ["fails.txt"],
    cmd = "sleep 0.1; exit 3",
    tags = ["manual"],
)

sh_test(
    name = "check_test",
    srcs = ["check_test.sh"],
    data = [":slow"],
)

sh_binary(
    name = "hello",
    srcs = ["hello.sh"],
)
EOF
cat >"$ws/check_test.sh" <<'EOF'
#!/usr/bin/env bash
sleep 0.4
EOF
cat >"$ws/hello.sh" <<'EOF'
#!/usr/bin/env bash
echo hello
EOF
chmod +x "$ws/check_test.sh" "$ws/hello.sh"

# The define keeps `$(FIXTURE_VALUE)` resolvable in every invocation, and the
# `option` invocation changes it.
common=(--disk_cache="$scratch/disk-cache" --define=FIXTURE_VALUE=1)

# record <name> <verb> [args...] — one invocation, its two files into
# $scratch/files/<name>/. A non-zero exit is kept: the failing fixture has one.
record() {
    local name=$1 verb=$2
    shift 2
    mkdir -p "$scratch/files/$name"
    local status=0
    (cd "$ws" && bazel --ignore_all_rc_files "$verb" \
        --build_event_json_file="$scratch/files/$name/bep.json" \
        --execution_log_compact_file="$scratch/files/$name/exec.zst" \
        "$@") || status=$?
    printf '%s: exit %s\n' "$name" "$status" >&2
}

record cold test "${common[@]}" //...
record warm test "${common[@]}" //...
(cd "$ws" && bazel --ignore_all_rc_files clean)
record cached test "${common[@]}" //...
record option test --disk_cache="$scratch/disk-cache" --define=FIXTURE_VALUE=2 //...
record failing build "${common[@]}" //:fails
record cquery cquery "${common[@]}" //...
record run run "${common[@]}" //:hello

# Every file is cut down and checked under $scratch first; the fixture
# directory is written only once all seven have passed, so a leak leaves the
# tree as it was.
user=$(id -un)
host=$(hostname)
staged="$scratch/staged"
for name in cold warm cached option failing cquery run; do
    src="$scratch/files/$name"
    dst="$staged/$name"
    mkdir -p "$dst"
    jq -c '
        select(.id | has("started") or has("buildFinished") or has("buildMetrics")
               or has("testResult") or has("progress"))
        | if .id | has("started") then
              .started.workspaceDirectory = "/src/alice/fixture"
            | .started.workingDirectory = "/src/alice/fixture"
            | .started.serverPid = "1"
            | del(.started.optionsDescription)
            | if .started | has("host") then .started.host = "example" else . end
            | if .started | has("user") then .started.user = "alice" else . end
          else . end
        | if .id | has("testResult") then del(.testResult.testActionOutput) else . end
    ' "$src/bep.json" |
        sed -e "s#$ws#/src/alice/fixture#g" -e "s#$scratch#/tmp/fixture#g" >"$dst/bep.json"
    cp "$src/exec.zst" "$dst/exec.zst"
    for leak in "$scratch" "$user" "$host" "$HOME"; do
        if zstd -dc "$dst/exec.zst" | grep -aqF "$leak" || grep -qF "$leak" "$dst/bep.json"; then
            printf 'record-fixtures: the %s fixture names %s; %s is unchanged\n' "$name" "$leak" "$out" >&2
            exit 1
        fi
    done
done
for name in cold warm cached option failing cquery run; do
    mkdir -p "$out/$name"
    cp "$staged/$name/bep.json" "$staged/$name/exec.zst" "$out/$name/"
done
printf 'record-fixtures: wrote %s with Bazel %s\n' "$out" "$version" >&2
