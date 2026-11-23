# Build timing record

`make check` and `make build` leave a record of every Bazel invocation they
make: what it configured, which spawns it executed and which it took from a
cache, which tests ran, and how long each part took. `make timing` reads it.

The record measures; it changes nothing about what is built. With recording on
or off, every Bazel invocation gets the same options it would get anyway, plus
two output-file flags that Bazel does not key analysis on.

## What is recorded

A **run** is one `make` process. A run holds one record per **command** that a
recorded recipe line ran, in order.

| Target | Commands recorded |
|---|---|
| `check` | the four `bazel cquery` calls of `bazel-dsl-coherence`; the three Bazel invocations of `bazel-check`; the two of `bazel-policy-parity`; `xtask-deny`; `scrub-selfcheck`'s `./dev/scrub-hook-floor.sh` and its `bazel run //tools/scrub:brenn-scrub` |
| `build` | its one `bazel build` |
| `run-artifacts` (so `dev-bin`, `dev-mounts`, `e2e`, `launchdev`) | its `bazel build` |
| `e2e-mounts` | its `bazel build` |
| `scrub-tree` | its `bazel run` of the scrub tool |
| `npm-audit` | its `bazel run` of `pnpm` |
| `bazel-release` | its `bazel build` and `bazel test` (off in practice: see below) |

A `make check` therefore records twelve commands: eleven Bazel invocations and
`scrub-hook-floor.sh`, the one part of the run not under a Bazel command, timed
so the commands add up to the run.

Not recorded:

- `bazel-release-dir`: its two Bazel calls sit inside command substitutions
  whose stdout is the recipe's product, one of them is `bazel info`, which
  builds nothing, and the target exists for the deploy lane.
- `example-check`: it is its own Bazel workspace and not part of `check`.
- `clean`: it builds nothing.
- `timing` itself: reading a run must not add a run.

For a Bazel command the recorder asks Bazel for two files:

- the **build event file** (`--build_event_json_file`), which gives the
  invocation's wall, analysis and execution times, targets configured, spawns
  per runner, local action-cache hits and misses, each test's result, and
  Bazel's stderr, where an analysis-cache discard is reported;
- the **compact execution log** (`--execution_log_compact_file`), which lists
  every spawn — executed or taken from a cache — with its target label,
  mnemonic, runner, cache-hit flag, exit code and duration.

Around every command, Bazel or not, it records the step (the `make` target
whose recipe ran it), the command line, the exit status and its own start and
end clock. Once per run it records the goals, the checkout, the branch, the
commit and whether the tree had tracked changes or untracked, unignored files.

## Switching it off

```
make check BAZEL_TIMING=off
BAZEL_TIMING=off git commit …
```

`BAZEL_TIMING` is `on` or `off`; any other value is an error. It defaults to
`on` when `BAZEL_CONFIG` is empty and `off` when it is set, which is how every
CI and deploy lane calls `make`, so those lanes write no store unless asked to.
A `git commit` reaches the pre-commit hook's `make check` with the
environment's value.

Off means `$(REC)` is empty: every recipe line is then exactly what it is
without the recorder, and nothing is exported. That is the comparison to make
when the recorder itself is suspected of changing a result.

If a record cannot be written — the store cannot be created, the disk is full
— the step fails with a message starting `bazel-timing:` that names
`BAZEL_TIMING=off`. A record that silently skipped some commands would make
the across-runs view wrong by omission. If the command itself ran and passed
before the record failed, the message says so.

## The store

```
~/.cache/brenn-bazel-timing/v1/runs/<run id>/
    run                      # the run's header, written by its first command
    <start ns>-<pid>/        # one directory per recorded command
        meta                 # written last; present means complete
        argv                 # the command as the Makefile wrote it, NUL-separated
        bep.json             # Bazel's build event file, for a Bazel command
        exec.zst             # Bazel's compact execution log, for a Bazel command
```

`BRENN_BAZEL_TIMING_DIR` replaces `~/.cache/brenn-bazel-timing`. The store is
outside every working tree, beside Bazel's disk cache, so every checkout and
worktree of the user shares it and no tree gains an untracked file. A run id
is the run's UTC start time to the nanosecond and `make`'s pid, so ids sort by
start time. Each run writes only inside its own directory; concurrent `make`
runs in different checkouts do not share a file.

**Bound: 2 GiB.** When a run starts, its first command deletes whole runs,
oldest first, while the store is over the cap; never the run just started.
`BRENN_BAZEL_TIMING_MAX_MB` sets the cap in MiB. A no-op `make check` writes
about 6 MB; a run that executes or fetches thousands of spawns writes much
more.

**`bep.json` holds the developer's environment.** Bazel writes the full
command line it saw into the build event file, including every
`--client_env=`. Bazel already writes the same lines to its server log in the
output base, and the store directory is created mode 0700, but do not paste a
raw `bep.json` anywhere. The reader never prints those events.

The reader never deletes anything. To start over, delete the store; that is
also the remedy after a Bazel upgrade leaves old records the reader refuses.

## Reading it: `make timing`

```
make timing                                   # show the newest run of this checkout
make timing TIMING_ARGS="show <run id or unique prefix>"
make timing TIMING_ARGS="runs"
make timing TIMING_ARGS="rebuilt --last 10"
```

`make timing` runs `bazel run //xtask -- timing --root $(CURDIR) …` with the
same options as `make build` and the last four invocations of `make check`,
so it discards no analysis cache after either. The full command line:

```
timing [--root <dir>] [--store <dir>] runs [--limit N]
timing [--root <dir>] [--store <dir>] show [<run id or unique prefix>] [--top N] [--all-checkouts]
timing [--root <dir>] [--store <dir>] rebuilt [--last N | --since <run id or prefix>]
                                              [--all-checkouts] [--branch B] [--goal G]
```

`--root` is the checkout. `show` and `rebuilt` default to runs recorded from
it, so the run you just made in a worktree is the one shown, not a later run
from another checkout; `--all-checkouts` widens them. A run id or prefix
names one run on the machine whichever checkout recorded it. `runs` lists
every checkout's runs.

### `runs`

One line per run, newest first (20 by default):

```
run                                 start                goals  checkout                branch  commit      dirty  wall  cmds  status
20261123T045423.080659493Z-3289808  2026-11-22 23:54:23  check  /home/alice/src/brenn   main    db705d64e4  yes    6.2s  12    no failure recorded
20261123T045046.806725865Z-3280832  2026-11-22 23:50:46  check  /home/alice/src/brenn   main    89f76ee56e  yes    7.4s  12    no failure recorded
(2 of 10 runs; --limit N for more)
```

`wall` is the first command's start to the last command's end. `status` is
one of:

- `no failure recorded` — every recorded command exited 0. A recipe can still
  fail in a line the recorder does not wrap (`bazel-dsl-coherence`'s `test -s`
  checks, for one), so this is not "the `make` succeeded";
- `failed (<step>)` — a recorded command exited non-zero, and `<step>` is the
  first one that did. A failure outside any recorded command is not shown;
- `incomplete` — a command has no `meta`: the `make` was interrupted, or is
  still running in another checkout.

### `show`

The run's header, then one row per recorded command, then the run's executed
spawns ranked by duration, then its tests. A warm, no-op `make check`:

```
run 20261123T052623.641292960Z-3623639  no failure recorded
  started 2026-11-23 00:26:23  goals check
  checkout /home/alice/src/brenn  branch main  commit 5166b8b6407d544dc47914edfe4c299cae58e1a5  dirty no

#   step                 command                                           exit  wall  bazel  analysis  exec  outside  configured  executed  ac hits  ac misses  cached
1   bazel-dsl-coherence  cquery somepath(//brenn-dsl:brenn-dsl, @fltk/...  0     0.5s  0.5s   0.0s      0.0s  0.0s     133         0         0        0          0
2   bazel-dsl-coherence  cquery filter("pyo3", deps(//brenn-dsl:all))      0     0.5s  0.5s   0.0s      0.0s  0.0s     1158        0         0        0          0
3   bazel-dsl-coherence  cquery somepath(//brenn-dsl:brenn-dsl, @crate...  0     0.5s  0.5s   0.0s      0.0s  0.0s     1           0         0        0          0
4   bazel-dsl-coherence  cquery somepath(@fltk//crates/fltk-serde-core...  0     0.5s  0.4s   0.0s      0.0s  0.0s     4           0         0        0          0
5   bazel-check          test //...                                        0     0.8s  0.8s   0.3s      0.6s  0.0s     4809        0         6642     116        89 disk cache hit
6   bazel-check          build //brenn-wasm/components/...                 0     0.4s  0.4s   0.1s      0.1s  0.0s     2484        0         1084     50         49 disk cache hit
7   bazel-check          build //surface/...                               0     0.3s  0.3s   0.1s      0.1s  0.0s     1657        0         370      55         54 disk cache hit
8   bazel-policy-parity  build //:policy_manifest                          0     0.3s  0.2s   0.0s      0.0s  0.0s     132         0         1        1          0
9   bazel-policy-parity  run //xtask                                       0     0.4s  0.3s   0.0s      0.1s  0.0s     1199        0         422      16         15 disk cache hit
10  xtask-deny           run //xtask                                       0     1.2s  0.2s   0.0s      0.0s  1.0s     0           0         0        1          0
11  scrub-selfcheck      ./dev/scrub-hook-floor.sh                         0     0.0s  -      -         -     -        -           -         -        -          -
12  scrub-selfcheck      run //tools/scrub:brenn-scrub                     0     0.3s  0.3s   0.0s      0.0s  0.1s     76          0         130      1          0
sum of the commands 5.7s; run wall 5.8s (first start to last end)
wall and outside are the recorder's clock; bazel, analysis and exec are Bazel's own, and Bazel interleaves the two phases, so they need not add up to its wall. outside is wall less bazel: for a bazel run, the binary's own time.
#6 bazel-check: "WARNING: Build option --platforms has changed, discarding analysis cache (this can be expensive, see https://bazel.build/advanced/performance/iteration-speed)."
#8 bazel-policy-parity: "WARNING: Build option --platforms has changed, discarding analysis cache (this can be expensive, see https://bazel.build/advanced/performance/iteration-speed)."

executed spawns: none
Spawns, not actions: a test action is two spawns, and actions Bazel runs in-process have none.

tests: 341 results; 341 cached locally, 0 cached by a cache service, 0 ran
```

The columns:

- `wall` — the recorder's clock around the command.
- `bazel`, `analysis`, `exec` — Bazel's own wall, analysis-phase and
  execution-phase times. Bazel runs the two phases interleaved, so they need
  not add up to its wall.
- `outside` — `wall` less `bazel`. For a `bazel run` it is the binary's own
  run time (row 10 is `cargo deny`).
- `configured` — targets configured. A jump in it beside a quoted
  `discarding analysis cache` line is a re-analysis.
- `executed` — spawns that ran, not taken from any cache.
- `ac hits`, `ac misses` — Bazel's local action-cache statistics.
- `cached` — spawns served from a cache, per the runner string Bazel wrote
  (`disk cache hit`, or Bazel's name for a remote hit).

A failed command without complete Bazel files shows `no Bazel metrics`; a
command that is not Bazel shows dashes; an incomplete command shows
`incomplete` and its directory. Where the execution log's spawn counts per
runner differ from the build event file's for the same command, `show` prints
both: both are Bazel's statements, and the record does not pick one. (Bazel's
own counts leave out a spawn that failed; the execution log lists it.)

Below the rows, the **sum of the commands** and the **run wall** are both
printed: the difference is time outside every recorded command. With
`make -j` the commands overlap and the sum exceeds the wall; nothing relates
the two then.

**Executed spawns** of the whole run, longest first, `--top` of them (30 by
default): duration, mnemonic, target label, step.

**Tests**: how many were cached locally, how many by a cache service, and each
test that ran with its duration, longest first. A cached test's recorded
duration is that of the run that filled the cache and is not shown as time
spent.

### `rebuilt`

Over the selected runs — by default the last 20 of this checkout — one row per
target label that executed at least one spawn:

```
executed spawns per target over 5 runs from /home/alice/src/brenn, 20261123T042510.491476736Z-3240527 to 20261123T045423.080659493Z-3289808
target                  runs  spawns  total  max   mnemonics
//xtask:policy_guards   4     4       4.7s   1.2s  TestRunner
//xtask:xtask_test      3     9       4.1s   1.5s  Clippy,Rustc,Rustfmt,TestRunner
//xtask:xtask           3     7       2.9s   0.9s  Clippy,Rustc,Rustfmt
//brenn-wasm:wit_gates  4     4       1.2s   0.3s  TestRunner
```

`runs` is how many of the selected runs executed at least one of the target's
spawns; `spawns`, `total` and `max` are over all of them. It counts exactly
the executed spawns `show` lists. `--branch` keeps runs recorded on one
branch; `--goal` keeps runs whose goals include one word. Runs without a
header, incomplete commands and failed commands with no Bazel metrics are left
out and counted on a `left out:` line.

## What the numbers mean and do not mean

- **Spawns, not actions.** The unit is the spawn: a process Bazel ran or would
  have run. A test action is two spawns (the test and its XML writer). Actions
  Bazel runs in-process (symlinks, file writes) have no spawn; they appear in
  the build event file's `internal` count and are never listed.
- **An aspect's spawn carries the label of the target it is applied to**, with
  its own mnemonic (`Clippy`, `Rustfmt`).
- **A cache hit's duration** in the execution log is the hit's own; a cached
  test's duration is the original run's.
- **The analysis-cache discard is read from a sentence** in Bazel's stderr;
  no structured field reports it. If a Bazel release rewords the warning,
  discards stop being quoted while `configured` still jumps.
- **A Bazel upgrade** fails the reader's fixture tests until the fixtures are
  recorded again with `xtask/tests/fixtures/timing/record-fixtures.sh`. Old
  records stay readable while they carry the fields the reader needs; one that
  does not is refused by name.

## When the reader refuses

The reader does not skip what it cannot read. A `run` or `meta` with an
unknown format, a missing key or an unparsable value, or a complete,
successful Bazel command whose `bep.json` or `exec.zst` is missing or
unreadable, fails the whole command with the file named and the run directory
to delete if you want the rest of the store readable.
