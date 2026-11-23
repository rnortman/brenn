#!/usr/bin/env bash
# Runs one command of a `make` recipe and leaves a timing record of it in the
# per-user store that `xtask timing` reads.
#
#   record.sh <step> <command> [args...]
#
# <step> is the make target whose recipe runs the command. When <command> is
# `bazel`, Bazel is asked for its build event file and its compact execution log,
# written into the record; any other command is only timed. The store is
# $BRENN_BAZEL_TIMING_DIR, else ~/.cache/brenn-bazel-timing, outside every
# working tree so every checkout and worktree of this user shares one store and
# no untracked file appears in a tree.
#
# The Makefile exports BRENN_TIMING_RUN (one id per make process) and
# BRENN_TIMING_GOALS. Both are removed from the environment of the command, so
# what the command sees is the same with recording on and off.
#
# Contract with the Makefile, which is why there is no option parsing:
# - Nothing is ever written to stdout: `bazel-dsl-coherence` decides on whether
#   a query's stdout is empty. On success nothing is written to stderr either.
# - The exit status of a command that ran is passed through unchanged.
# - A record that cannot be written fails the step. A store with silent holes
#   would make the across-runs view wrong by omission, and a warning in the
#   middle of a `make check` is read by nobody. Every such message starts
#   `bazel-timing:` and names the off switch.
# - No trap. An interrupted command leaves an invocation directory without
#   `meta`, which is the one incomplete state the reader knows how to show.
#
# docs/build-timing.md describes the store's layout and what is recorded.
set -euo pipefail

die() {
    echo "bazel-timing: $*; run with BAZEL_TIMING=off to build without recording" >&2
    exit 1
}

[ $# -ge 2 ] || die "usage: record.sh <step> <command> [args...]"
step=$1
shift

run_id=${BRENN_TIMING_RUN:-}
goals=${BRENN_TIMING_GOALS-}
[ -n "$run_id" ] || die "BRENN_TIMING_RUN is not set; the Makefile exports it"
[ -n "${BRENN_TIMING_GOALS+set}" ] || die "BRENN_TIMING_GOALS is not set; the Makefile exports it"
unset BRENN_TIMING_RUN BRENN_TIMING_GOALS

# Only the verbs that write both files are accepted, and before anything is
# created, so a refused command leaves no record behind.
kind="command"
if [ "$1" = bazel ]; then
    [ $# -ge 2 ] || die "a bare 'bazel' with no command is not recorded"
    case $2 in
        build | test | run | cquery) ;;
        *) die "'bazel $2' is not recorded; only build, test, run and cquery are" ;;
    esac
    kind=bazel
fi

# The id becomes a directory name, so it is held to the shape the Makefile
# makes: a UTC timestamp to the nanosecond, then make's pid. Ids sort by start.
[[ $run_id =~ ^[0-9]{8}T[0-9]{6}\.[0-9]{9}Z-[0-9]+$ ]] \
    || die "BRENN_TIMING_RUN '$run_id' is not <yyyymmddThhmmss.nnnnnnnnnZ>-<pid>"

# Values go into `key value` line files; a newline would forge a key.
no_newline() {
    case $2 in
        *$'\n'*) die "the $1 '$2' contains a newline and cannot be recorded" ;;
    esac
}
no_newline step "$step"
no_newline goals "$goals"

cap_mb=${BRENN_BAZEL_TIMING_MAX_MB:-2048}
[[ $cap_mb =~ ^[0-9]+$ ]] || die "BRENN_BAZEL_TIMING_MAX_MB '$cap_mb' is not a number of MiB"

# GNU date's %N; a date without it prints the letter N, which the reader would
# otherwise take as a time.
now_ns() {
    local t
    t=$(date +%s%N)
    [[ $t =~ ^[0-9]+$ ]] || die "'date +%s%N' printed '$t', not nanoseconds; GNU date is required"
    printf '%s' "$t"
}

# Write <file> from stdin through a temporary and a rename, so a reader never
# sees a half-written key file.
write_atomically() {
    local file=$1
    cat >"$file.tmp" || return 1
    mv "$file.tmp" "$file" || return 1
}

store=${BRENN_BAZEL_TIMING_DIR:-${HOME:?HOME is not set}/.cache/brenn-bazel-timing}
runs=$store/v1/runs
run_dir=$runs/$run_id

# Mode 0700: a build event file holds the invocation's full command line, which
# includes every --client_env= — the developer's whole environment.
(umask 077 && mkdir -p "$runs") 2>/dev/null || die "cannot create the store $runs"

# The tree's identity, read once per run: the tree does not change inside one
# make, and a `git status` in front of every Bazel invocation would be a
# measurable part of a no-op check. Every GIT_* variable is unset so a hook's
# exported GIT_DIR cannot point the read at another repository; from a linked
# worktree `--show-toplevel` is that worktree. `--no-optional-locks` keeps the
# status from taking the index lock that a commit running this hook holds.
#
# Only git's own "not a git repository" makes the state unknown; any other
# failure of a read returns non-zero with git's message on stderr, because a
# header that says `dirty no` for a tree whose status was never read is wrong.
# The match runs under LC_ALL=C, where git's messages are untranslated.
git_state() {
    local var inside
    for var in $(compgen -v GIT_ || true); do unset "$var"; done
    if ! inside=$(LC_ALL=C git rev-parse --is-inside-work-tree 2>&1); then
        case $inside in
            *"not a git repository"*) inside=false ;;
            *)
                printf '%s\n' "$inside" >&2
                return 1
                ;;
        esac
    fi
    case $inside in
        true) ;;
        false)
            printf 'checkout %s\nbranch -\ncommit -\ndirty unknown\n' "$(pwd -P)"
            return
            ;;
        *)
            printf '%s\n' "$inside" >&2
            return 1
            ;;
    esac
    local top branch commit porcelain dirty
    top=$(git rev-parse --show-toplevel) || return 1
    branch=$(git symbolic-ref --short -q HEAD || echo '(detached)')
    commit=$(git rev-parse -q --verify HEAD || echo -)
    porcelain=$(git --no-optional-locks status --porcelain) || return 1
    if [ -n "$porcelain" ]; then dirty=yes; else dirty=no; fi
    printf 'checkout %s\nbranch %s\ncommit %s\ndirty %s\n' "$top" "$branch" "$commit" "$dirty"
}

# Delete whole runs, oldest first, until the store is under its cap. The cap is
# on size because a record's size varies by orders of magnitude between a no-op
# check and a rebuild, so neither a count nor an age bounds it. Never the run
# being recorded.
enforce_bound() {
    # Base 10: bash arithmetic reads a leading zero as octal.
    local cap_kb=$((10#$cap_mb * 1024)) total=0 size dir
    local -a sizes=() dirs=()
    while IFS=$'\t' read -r size dir; do
        sizes+=("$size")
        dirs+=("$dir")
        total=$((total + size))
    done < <(cd "$runs" && shopt -s nullglob && set -- */ && [ $# -gt 0 ] \
        && du -sk -- "$@" | LC_ALL=C sort -t $'\t' -k2,2)
    local i
    for i in "${!dirs[@]}"; do
        [ "$total" -gt "$cap_kb" ] || break
        dir=${dirs[$i]%/}
        [ "$dir" != "$run_id" ] || continue
        rm -rf -- "${runs:?}/$dir" || die "cannot delete the old run $runs/$dir to keep the store under ${cap_mb} MiB"
        total=$((total - sizes[i]))
    done
}

# `mkdir` is the test: the command that creates the run directory writes its
# header. Under `make -j` another step of the same run can create it first, and
# this command's record then joins that run.
if mkdir "$run_dir" 2>/dev/null; then
    run_started=$(now_ns)
    state=$(git_state) || die "cannot read the git state of $PWD"
    # Four keys, four lines; a checkout path holding a newline would add one.
    [ "$(printf '%s\n' "$state" | wc -l)" -eq 4 ] \
        || die "the checkout path of $PWD contains a newline and cannot be recorded"
    {
        printf 'format 1\nid %s\ngoals %s\n' "$run_id" "$goals"
        printf '%s\n' "$state"
        printf 'started_ns %s\n' "$run_started"
    } | write_atomically "$run_dir/run" || die "cannot write $run_dir/run"
    enforce_bound
elif [ ! -d "$run_dir" ]; then
    die "cannot create the run directory $run_dir"
fi

started=$(now_ns)
inv_dir=$run_dir/$started-$$
mkdir "$inv_dir" 2>/dev/null || die "cannot create the invocation directory $inv_dir"

cmd=("$@")
if [ "$kind" = bazel ]; then
    # Directly after the verb, before every other argument and so before any
    # `--`, where Bazel stops reading its own options.
    cmd=(bazel "$2" "--build_event_json_file=$inv_dir/bep.json"
        "--execution_log_compact_file=$inv_dir/exec.zst" "${@:3}")
fi

set +e
"${cmd[@]}"
status=$?
set -e
ended=$(now_ns)

record_failed() {
    if [ "$status" -eq 0 ]; then
        die "the command passed, but $1"
    fi
    echo "bazel-timing: the command failed with status $status, and $1" >&2
    exit "$status"
}

printf '%s\0' "$@" | write_atomically "$inv_dir/argv" \
    || record_failed "cannot write $inv_dir/argv"
printf 'format 1\nstep %s\nkind %s\nstarted_ns %s\nended_ns %s\nexit %s\n' \
    "$step" "$kind" "$started" "$ended" "$status" \
    | write_atomically "$inv_dir/meta" \
    || record_failed "cannot write $inv_dir/meta"

exit "$status"
