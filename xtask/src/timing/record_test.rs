//! `bazel/timing/record.sh`, run by `bash` against a temporary store with a
//! stand-in `bazel` first on `PATH`, and its records read back by [`store`] —
//! so the writing and the reading side of the format cannot drift apart.
//!
//! The script is reached by its workspace-relative path: it is in the policy
//! manifest this test target carries, whose runfiles sit at their source paths.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use super::store::{self, Dirty, Invocation, Kind};

const SCRIPT: &str = "bazel/timing/record.sh";
const RUN_ID: &str = "20261122T120000.000000001Z-4242";

/// Records every argument (NUL-separated) and its environment, prints one line
/// to stdout, and exits with `STUB_EXIT`. With `STUB_LOCK` set it first makes
/// the invocation directory its build event file is in read-only, so the
/// recorder's writes after the command fail.
const STUB: &str = "#!/usr/bin/env bash\n\
    printf '%s\\0' \"$@\" > \"$STUB_OUT/args\"\n\
    env > \"$STUB_OUT/env\"\n\
    if [ -n \"${STUB_LOCK:-}\" ]; then\n\
        for a in \"$@\"; do\n\
            case $a in --build_event_json_file=*) f=${a#*=}; chmod 555 \"${f%/*}\" ;; esac\n\
        done\n\
    fi\n\
    echo 'stand-in stdout'\n\
    exit \"${STUB_EXIT:-0}\"\n";

struct Harness {
    _tmp: tempfile::TempDir,
    store: PathBuf,
    stub_bin: PathBuf,
    stub_out: PathBuf,
    home: PathBuf,
    script: PathBuf,
}

impl Harness {
    fn new() -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let stub_bin = tmp.path().join("bin");
        let stub_out = tmp.path().join("out");
        let home = tmp.path().join("home");
        for dir in [&stub_bin, &stub_out, &home] {
            std::fs::create_dir(dir).unwrap();
        }
        let stub = stub_bin.join("bazel");
        std::fs::write(&stub, STUB).unwrap();
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(&stub, std::fs::Permissions::from_mode(0o755)).unwrap();
        let script = std::fs::canonicalize(SCRIPT).unwrap_or_else(|e| {
            panic!("the recorder is missing at {SCRIPT} ({e}); it is in //:policy_manifest")
        });
        Harness {
            store: tmp.path().join("store"),
            _tmp: tmp,
            stub_bin,
            stub_out,
            home,
            script,
        }
    }

    /// The script, in `cwd`, with the Makefile's two variables set.
    fn command(&self, cwd: &Path, args: &[&str]) -> Command {
        let path = std::env::var_os("PATH").unwrap_or_default();
        let mut joined = self.stub_bin.clone().into_os_string();
        joined.push(":");
        joined.push(path);
        let mut cmd = Command::new("bash");
        cmd.arg(&self.script)
            .args(args)
            .current_dir(cwd)
            .env("PATH", joined)
            .env("HOME", &self.home)
            .env("BRENN_BAZEL_TIMING_DIR", &self.store)
            .env("BRENN_TIMING_RUN", RUN_ID)
            .env("BRENN_TIMING_GOALS", "check")
            .env("STUB_OUT", &self.stub_out)
            .env_remove("BRENN_BAZEL_TIMING_MAX_MB");
        cmd
    }

    fn run(&self, cwd: &Path, args: &[&str]) -> Output {
        self.command(cwd, args).output().expect("run the recorder")
    }

    /// What the stand-in received, or `None` when it never ran.
    fn stub_args(&self) -> Option<Vec<String>> {
        let bytes = std::fs::read(self.stub_out.join("args")).ok()?;
        let body = bytes.strip_suffix(b"\0").unwrap_or(&bytes);
        Some(
            body.split(|&b| b == 0)
                .map(|a| String::from_utf8(a.to_vec()).unwrap())
                .collect(),
        )
    }

    fn stub_env(&self) -> String {
        std::fs::read_to_string(self.stub_out.join("env")).unwrap()
    }

    fn load(&self) -> store::Run {
        store::load_run(&self.store, RUN_ID).unwrap()
    }
}

fn complete(inv: &Invocation) -> (&Path, &store::Meta, &[String]) {
    match inv {
        Invocation::Complete { dir, meta, argv } => (dir, meta, argv),
        Invocation::Incomplete { dir } => panic!("{} has no meta", dir.display()),
    }
}

fn seeded_repo() -> tempfile::TempDir {
    let repo = tempfile::tempdir().unwrap();
    git_fixture::seed_repo(repo.path());
    repo
}

fn stderr(out: &Output) -> String {
    String::from_utf8_lossy(&out.stderr).into_owned()
}

#[test]
fn a_bazel_command_gets_both_flags_after_the_verb_and_the_rest_unchanged() {
    let h = Harness::new();
    let repo = seeded_repo();
    let args = [
        "bazel-policy-parity",
        "bazel",
        "run",
        "--config=x",
        "//xtask",
        "--",
        "--root",
        "a b (c)",
    ];
    let out = h.run(repo.path(), &args);
    assert!(out.status.success(), "{}", stderr(&out));
    assert_eq!(out.stdout, b"stand-in stdout\n");
    assert_eq!(stderr(&out), "");

    let run = h.load();
    assert_eq!(run.invocations.len(), 1);
    let (dir, meta, argv) = complete(&run.invocations[0]);
    let received = h.stub_args().expect("the stand-in ran");
    assert_eq!(
        received,
        [
            "run".to_string(),
            format!("--build_event_json_file={}", dir.join("bep.json").display()),
            format!(
                "--execution_log_compact_file={}",
                dir.join("exec.zst").display()
            ),
            "--config=x".to_string(),
            "//xtask".to_string(),
            "--".to_string(),
            "--root".to_string(),
            "a b (c)".to_string(),
        ]
    );
    assert_eq!(argv, &args[1..]);
    assert_eq!(meta.step, "bazel-policy-parity");
    assert_eq!(meta.kind, Kind::Bazel);
    assert_eq!(meta.exit, 0);
}

#[test]
fn the_exit_status_is_the_commands_and_is_recorded() {
    let h = Harness::new();
    let repo = seeded_repo();
    let out = h
        .command(repo.path(), &["build", "bazel", "build", "//..."])
        .env("STUB_EXIT", "3")
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(3));
    assert_eq!(out.stdout, b"stand-in stdout\n");
    let run = h.load();
    assert_eq!(complete(&run.invocations[0]).1.exit, 3);
    assert_eq!(run.status(), store::RunStatus::Failed("build".into()));
}

#[test]
fn the_recorders_variables_do_not_reach_the_command() {
    let h = Harness::new();
    let repo = seeded_repo();
    let out = h.run(repo.path(), &["build", "bazel", "build", "//..."]);
    assert!(out.status.success(), "{}", stderr(&out));
    let env = h.stub_env();
    assert!(!env.contains("BRENN_TIMING_RUN="), "{env}");
    assert!(!env.contains("BRENN_TIMING_GOALS="), "{env}");
    assert!(env.contains("STUB_OUT="), "{env}");
}

#[test]
fn the_run_header_names_the_tree_even_when_git_dir_points_elsewhere() {
    let h = Harness::new();
    let repo = seeded_repo();
    let other = seeded_repo();
    std::fs::write(repo.path().join("untracked.txt"), "x").unwrap();
    let out = h
        .command(repo.path(), &["build", "bazel", "build", "//..."])
        .env("GIT_DIR", other.path().join(".git"))
        .output()
        .unwrap();
    assert!(out.status.success(), "{}", stderr(&out));

    let header = h
        .load()
        .header
        .expect("the first command writes the header");
    let top = std::fs::canonicalize(repo.path()).unwrap();
    assert_eq!(Path::new(&header.checkout), top);
    assert_eq!(header.branch, "main");
    let head = git_fixture::git(repo.path(), &["rev-parse", "HEAD"]);
    assert_eq!(header.commit, head.trim());
    assert_eq!(header.dirty, Dirty::Yes);
    assert_eq!(header.goals, "check");
}

#[test]
fn a_clean_tree_is_not_dirty_and_a_second_command_keeps_the_header() {
    let h = Harness::new();
    let repo = seeded_repo();
    let first = h.run(repo.path(), &["bazel-check", "bazel", "test", "//..."]);
    assert!(first.status.success(), "{}", stderr(&first));
    let header_path = store::runs_dir(&h.store).join(RUN_ID).join("run");
    let header_before = std::fs::read(&header_path).unwrap();
    assert_eq!(h.load().header.unwrap().dirty, Dirty::No);

    // The tree changes between the two; the run's header is not rewritten.
    std::fs::write(repo.path().join("untracked.txt"), "x").unwrap();
    let second = h.run(repo.path(), &["xtask-deny", "bazel", "run", "//xtask"]);
    assert!(second.status.success(), "{}", stderr(&second));
    assert_eq!(std::fs::read(&header_path).unwrap(), header_before);
    let run = h.load();
    let steps: Vec<_> = run
        .invocations
        .iter()
        .map(|inv| complete(inv).1.step.clone())
        .collect();
    assert_eq!(steps, ["bazel-check", "xtask-deny"]);
}

/// A `mkdir` that, asked for exactly `$RACE_DIR`, creates it and then fails as
/// the real one does when another process created it between a check and the
/// call. Every other call is the real `mkdir`, found on `PATH` past this one.
const LOSING_MKDIR: &str = "#!/usr/bin/env bash\n\
    PATH=${PATH#*:}\n\
    if [ \"$#\" -eq 1 ] && [ \"$1\" = \"$RACE_DIR\" ]; then\n\
        mkdir -- \"$1\"\n\
        echo \"mkdir: cannot create directory '$1': File exists\" >&2\n\
        exit 1\n\
    fi\n\
    exec mkdir \"$@\"\n";

#[test]
fn a_command_that_loses_the_race_for_the_run_directory_joins_the_run() {
    let h = Harness::new();
    let repo = seeded_repo();
    let mkdir = h.stub_bin.join("mkdir");
    std::fs::write(&mkdir, LOSING_MKDIR).unwrap();
    use std::os::unix::fs::PermissionsExt as _;
    std::fs::set_permissions(&mkdir, std::fs::Permissions::from_mode(0o755)).unwrap();
    let run_dir = store::runs_dir(&h.store).join(RUN_ID);

    let out = h
        .command(repo.path(), &["bazel-check", "bazel", "test", "//..."])
        .env("RACE_DIR", &run_dir)
        .output()
        .unwrap();
    assert!(out.status.success(), "{}", stderr(&out));
    assert_eq!(stderr(&out), "");
    assert!(h.stub_args().is_some(), "the stand-in ran");
    let run = h.load();
    assert!(
        run.header.is_none(),
        "the step that created the directory writes it"
    );
    assert_eq!(run.invocations.len(), 1);
    assert_eq!(complete(&run.invocations[0]).1.step, "bazel-check");
}

#[test]
fn outside_a_work_tree_the_checkout_is_the_directory() {
    let h = Harness::new();
    let plain = tempfile::tempdir().unwrap();
    let out = h.run(plain.path(), &["build", "bazel", "build", "//..."]);
    assert!(out.status.success(), "{}", stderr(&out));
    let header = h.load().header.unwrap();
    assert_eq!(
        std::fs::canonicalize(&header.checkout).unwrap(),
        std::fs::canonicalize(plain.path()).unwrap()
    );
    assert_eq!(header.branch, "-");
    assert_eq!(header.commit, "-");
    assert_eq!(header.dirty, Dirty::Unknown);
}

#[test]
fn another_command_runs_unchanged_and_is_only_timed() {
    let h = Harness::new();
    let repo = seeded_repo();
    let out = h.run(
        repo.path(),
        &["scrub-selfcheck", "bash", "-c", "echo plain; exit 0"],
    );
    assert!(out.status.success(), "{}", stderr(&out));
    assert_eq!(out.stdout, b"plain\n");
    let run = h.load();
    let (dir, meta, argv) = complete(&run.invocations[0]);
    assert_eq!(meta.kind, Kind::Command);
    assert_eq!(argv, ["bash", "-c", "echo plain; exit 0"]);
    assert!(!dir.join("bep.json").exists());
}

#[test]
fn an_unsupported_verb_is_refused_without_running_or_recording() {
    let h = Harness::new();
    let repo = seeded_repo();
    let out = h.run(repo.path(), &["bazel-release-dir", "bazel", "info", "x"]);
    assert!(!out.status.success());
    assert!(
        stderr(&out).starts_with("bazel-timing: "),
        "{}",
        stderr(&out)
    );
    assert!(
        stderr(&out).contains("BAZEL_TIMING=off"),
        "{}",
        stderr(&out)
    );
    assert_eq!(out.stdout, b"");
    assert_eq!(h.stub_args(), None);
    assert!(!store::runs_dir(&h.store).join(RUN_ID).exists());
}

#[test]
fn a_missing_run_id_is_refused_without_running() {
    let h = Harness::new();
    let repo = seeded_repo();
    let out = h
        .command(repo.path(), &["build", "bazel", "build", "//..."])
        .env_remove("BRENN_TIMING_RUN")
        .output()
        .unwrap();
    assert!(!out.status.success());
    assert!(
        stderr(&out).starts_with("bazel-timing: "),
        "{}",
        stderr(&out)
    );
    assert_eq!(h.stub_args(), None);
}

#[test]
fn an_unwritable_store_is_refused_without_running() {
    let h = Harness::new();
    let repo = seeded_repo();
    let blocker = h.home.join("a-file");
    std::fs::write(&blocker, "not a directory").unwrap();
    let out = h
        .command(repo.path(), &["build", "bazel", "build", "//..."])
        .env("BRENN_BAZEL_TIMING_DIR", blocker.join("store"))
        .output()
        .unwrap();
    assert!(!out.status.success());
    assert!(
        stderr(&out).starts_with("bazel-timing: "),
        "{}",
        stderr(&out)
    );
    assert!(
        stderr(&out).contains("BAZEL_TIMING=off"),
        "{}",
        stderr(&out)
    );
    assert_eq!(h.stub_args(), None);
}

#[test]
fn over_the_cap_the_oldest_runs_go_first_and_the_current_one_stays() {
    let h = Harness::new();
    let repo = seeded_repo();
    let runs = store::runs_dir(&h.store);
    let oldest = runs.join("20261122T100000.000000001Z-1");
    let older = runs.join("20261122T110000.000000001Z-2");
    std::fs::create_dir_all(&oldest).unwrap();
    std::fs::create_dir_all(&older).unwrap();
    std::fs::write(oldest.join("bulk"), vec![7u8; 1024 * 1024]).unwrap();
    std::fs::write(older.join("bulk"), b"small").unwrap();

    let out = h
        .command(repo.path(), &["build", "bazel", "build", "//..."])
        .env("BRENN_BAZEL_TIMING_MAX_MB", "1")
        .output()
        .unwrap();
    assert!(out.status.success(), "{}", stderr(&out));
    assert!(
        !oldest.exists(),
        "the oldest run is over the cap and must go"
    );
    assert!(older.exists(), "once under the cap nothing more is deleted");
    assert!(runs.join(RUN_ID).exists());
}

#[test]
fn a_record_that_cannot_be_written_after_the_command_fails_only_a_passing_step() {
    for (stub_exit, want) in [("0", 1), ("3", 3)] {
        let h = Harness::new();
        let repo = seeded_repo();
        let out = h
            .command(repo.path(), &["build", "bazel", "build", "//..."])
            .env("STUB_LOCK", "1")
            .env("STUB_EXIT", stub_exit)
            .output()
            .unwrap();
        assert_eq!(out.status.code(), Some(want), "{}", stderr(&out));
        let err = stderr(&out);
        if stub_exit == "0" {
            assert!(
                err.contains("bazel-timing: the command passed, but cannot write "),
                "{err}"
            );
            assert!(err.contains("BAZEL_TIMING=off"), "{err}");
        } else {
            assert!(
                err.contains("bazel-timing: the command failed with status 3, and "),
                "{err}"
            );
        }
        assert!(h.stub_args().is_some(), "the command ran");
    }
}

#[test]
fn malformed_input_from_the_makefile_is_refused_before_anything_is_created() {
    type Tweak = fn(&mut Command);
    let cases: &[(&str, &[&str], Tweak)] = &[
        (
            "a run id with a path in it",
            &["build", "bazel", "build", "//..."],
            |c| {
                c.env("BRENN_TIMING_RUN", "x/../y");
            },
        ),
        (
            "a step with a newline",
            &["build\nkind x", "bazel", "build", "//..."],
            |_| {},
        ),
        (
            "goals with a newline",
            &["build", "bazel", "build", "//..."],
            |c| {
                c.env("BRENN_TIMING_GOALS", "check\ncommit x");
            },
        ),
        ("goals unset", &["build", "bazel", "build", "//..."], |c| {
            c.env_remove("BRENN_TIMING_GOALS");
        }),
        (
            "a cap that is not a number",
            &["build", "bazel", "build", "//..."],
            |c| {
                c.env("BRENN_BAZEL_TIMING_MAX_MB", "abc");
            },
        ),
        ("a bare bazel", &["build", "bazel"], |_| {}),
    ];
    for (what, args, tweak) in cases {
        let h = Harness::new();
        let repo = seeded_repo();
        let mut cmd = h.command(repo.path(), args);
        tweak(&mut cmd);
        let out = cmd.output().unwrap();
        assert!(!out.status.success(), "{what}");
        assert!(
            stderr(&out).starts_with("bazel-timing: "),
            "{what}: {}",
            stderr(&out)
        );
        assert_eq!(h.stub_args(), None, "{what}: the command ran");
        assert!(!h.store.exists(), "{what}: the store was created");
    }
}

#[test]
fn a_cap_with_a_leading_zero_is_decimal() {
    let h = Harness::new();
    let repo = seeded_repo();
    let runs = store::runs_dir(&h.store);
    let oldest = runs.join("20261122T100000.000000001Z-1");
    std::fs::create_dir_all(&oldest).unwrap();
    std::fs::write(oldest.join("bulk"), vec![7u8; 8 * 1024 * 1024]).unwrap();
    for cap in ["010", "09"] {
        let out = h
            .command(repo.path(), &["build", "bazel", "build", "//..."])
            .env("BRENN_BAZEL_TIMING_MAX_MB", cap)
            .output()
            .unwrap();
        assert!(out.status.success(), "{cap}: {}", stderr(&out));
        assert_eq!(stderr(&out), "", "{cap}");
        assert!(oldest.exists(), "{cap} MiB holds an 8 MiB store");
        std::fs::remove_dir_all(runs.join(RUN_ID)).unwrap();
    }
}

#[test]
fn a_git_status_that_fails_in_a_work_tree_is_refused_not_recorded_as_clean() {
    let h = Harness::new();
    let repo = seeded_repo();
    std::fs::write(repo.path().join(".git/index"), b"not an index").unwrap();
    let out = h.run(repo.path(), &["build", "bazel", "build", "//..."]);
    assert!(!out.status.success());
    assert!(
        stderr(&out).contains("bazel-timing: cannot read the git state of "),
        "{}",
        stderr(&out)
    );
    assert_eq!(h.stub_args(), None);
}

#[test]
fn a_git_failure_other_than_not_a_repository_is_refused() {
    let h = Harness::new();
    let plain = tempfile::tempdir().unwrap();
    let git = h.stub_bin.join("git");
    std::fs::write(
        &git,
        "#!/usr/bin/env bash\necho 'fatal: detected dubious ownership in repository' >&2\nexit 128\n",
    )
    .unwrap();
    use std::os::unix::fs::PermissionsExt as _;
    std::fs::set_permissions(&git, std::fs::Permissions::from_mode(0o755)).unwrap();
    let out = h.run(plain.path(), &["build", "bazel", "build", "//..."]);
    assert!(!out.status.success());
    let err = stderr(&out);
    assert!(
        err.starts_with("fatal: detected dubious ownership"),
        "{err}"
    );
    assert!(
        err.contains("bazel-timing: cannot read the git state of "),
        "{err}"
    );
    assert_eq!(h.stub_args(), None);
}

/// `make` over the repository's `Makefile` with none of the switches inherited.
fn make(args: &[&str]) -> Output {
    let makefile = std::fs::canonicalize("Makefile")
        .unwrap_or_else(|e| panic!("the Makefile is missing ({e}); it is in //:policy_manifest"));
    Command::new("make")
        .arg("--no-print-directory")
        .arg("-f")
        .arg(&makefile)
        .args(args)
        .current_dir(makefile.parent().unwrap())
        .env_remove("MAKEFLAGS")
        .env_remove("MFLAGS")
        .env_remove("MAKELEVEL")
        .env_remove("BAZEL_TIMING")
        .env_remove("BAZEL_CONFIG")
        .env_remove("BRENN_TIMING_RUN")
        .env_remove("BRENN_TIMING_GOALS")
        .output()
        .expect("run make")
}

fn stdout(out: &Output) -> String {
    String::from_utf8_lossy(&out.stdout).into_owned()
}

#[test]
fn the_makefile_records_by_default_and_not_when_off_or_in_a_lane() {
    let cases: &[(&[&str], bool)] = &[
        (&[], true),
        (&["BAZEL_TIMING=off"], false),
        (&["BAZEL_CONFIG=--config=ci"], false),
        (&["BAZEL_CONFIG=--config=ci", "BAZEL_TIMING=on"], true),
    ];
    for (vars, recorded) in cases {
        let mut args = vec!["-n", "xtask-deny"];
        args.extend_from_slice(vars);
        let out = make(&args);
        assert!(out.status.success(), "{vars:?}: {}", stderr(&out));
        let line = stdout(&out);
        let line = line.trim();
        if *recorded {
            assert!(
                line.starts_with("bazel/timing/record.sh xtask-deny bazel run "),
                "{vars:?}: {line}"
            );
        } else {
            assert!(line.starts_with("bazel run "), "{vars:?}: {line}");
            assert!(!line.contains("record.sh"), "{vars:?}: {line}");
        }
    }

    let out = make(&["-n", "xtask-deny", "BAZEL_TIMING=maybe"]);
    assert!(!out.status.success());
    assert!(
        stderr(&out).contains("BAZEL_TIMING must be on or off, not \"maybe\""),
        "{}",
        stderr(&out)
    );
}

#[test]
fn every_recipe_line_of_one_make_sees_the_same_run_id() {
    let probe = "--eval=timing-probe: ; @echo \"$$BRENN_TIMING_RUN $$BRENN_TIMING_GOALS\"\n\t@echo \"$$BRENN_TIMING_RUN $$BRENN_TIMING_GOALS\"";
    let out = make(&[probe, "timing-probe"]);
    assert!(out.status.success(), "{}", stderr(&out));
    let text = stdout(&out);
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines.len(), 2, "{text}");
    assert_eq!(lines[0], lines[1], "one make must be one run");
    let (id, goals) = lines[0].split_once(' ').unwrap();
    let (stamp, pid) = id.split_once("Z-").unwrap_or_else(|| panic!("{id}"));
    let digits = |s: &str| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit());
    assert!(
        stamp.len() == 25
            && digits(&stamp[..8])
            && &stamp[8..9] == "T"
            && digits(&stamp[9..15])
            && &stamp[15..16] == "."
            && digits(&stamp[16..])
            && digits(pid),
        "{id}"
    );
    assert_eq!(goals, "timing-probe");

    let out = make(&[probe, "timing-probe", "BAZEL_TIMING=off"]);
    assert!(out.status.success(), "{}", stderr(&out));
    assert_eq!(stdout(&out), " \n \n", "off exports nothing");
}
