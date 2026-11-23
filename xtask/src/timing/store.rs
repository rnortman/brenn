//! The record store `bazel/timing/record.sh` writes, read back.
//!
//! ```text
//! <store>/v1/runs/<run id>/
//!     run                  written once, by the run's first recorded command
//!     <start ns>-<pid>/    one per recorded command
//!         meta             written last; its presence means "complete"
//!         argv             the command as the Makefile wrote it, NUL-separated
//!         bep.json         Bazel's build event file, for a Bazel command
//!         exec.zst         Bazel's compact execution log, for a Bazel command
//! ```
//!
//! `run` and `meta` are `key value` line files: the key up to the first space,
//! the value to the end of the line. The script cannot escape JSON reliably,
//! hence plain text.
//!
//! Nothing here skips what it cannot read. A record with an unknown format, a
//! missing or unknown key, or a value that does not parse is an error naming the
//! file; one such record fails the whole command, because a report that quietly
//! leaves out a run is wrong by omission. The one state the store is designed to
//! have that is not complete — a directory without `meta` or a run without
//! `run`, from an interrupted or still-running `make` — is represented as
//! [`Invocation::Incomplete`] or a run without a header, never as an error.

use std::collections::BTreeMap;
use std::fmt;
use std::path::{Path, PathBuf};

/// The only `format` this reader knows, in both `run` and `meta`.
const FORMAT: &str = "1";

/// The store's layout version, a directory under the store root.
const LAYOUT: &str = "v1";

/// A record the reader refuses. Displayed as the file, what is wrong with it,
/// and — when the file belongs to a run — the run directory whose deletion makes
/// the rest of the store readable. The reader itself never deletes anything.
#[derive(Debug)]
pub struct RecordError {
    pub path: PathBuf,
    pub message: String,
    pub run_dir: Option<PathBuf>,
}

impl fmt::Display for RecordError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.path.display(), self.message)?;
        if let Some(run_dir) = &self.run_dir {
            write!(
                f,
                "\n  this run cannot be read; delete {} to read the rest of the store",
                run_dir.display()
            )?;
        }
        Ok(())
    }
}

impl RecordError {
    pub(super) fn new(path: &Path, message: impl Into<String>) -> Self {
        RecordError {
            path: path.to_path_buf(),
            message: message.into(),
            run_dir: None,
        }
    }

    pub(super) fn in_run(mut self, run_dir: &Path) -> Self {
        self.run_dir = Some(run_dir.to_path_buf());
        self
    }
}

/// Whether the working tree had changes when the run started.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dirty {
    Yes,
    No,
    /// The run was not made in a git work tree.
    Unknown,
}

impl Dirty {
    pub fn as_str(self) -> &'static str {
        match self {
            Dirty::Yes => "yes",
            Dirty::No => "no",
            Dirty::Unknown => "unknown",
        }
    }
}

/// A run's `run` file: one `make` process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunHeader {
    /// The `make` command goals, or the default goal.
    pub goals: String,
    /// The working tree's top level, or the directory `make` ran in outside one.
    pub checkout: String,
    /// The branch, `(detached)`, or `-` outside a work tree.
    pub branch: String,
    /// The full commit hash, or `-`.
    pub commit: String,
    pub dirty: Dirty,
    pub started_ns: u64,
}

/// Whether a recorded command ran Bazel (and so has Bazel's two files) or was
/// only timed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Bazel,
    Command,
}

/// A recorded command's `meta` file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Meta {
    /// The `make` target whose recipe ran the command.
    pub step: String,
    pub kind: Kind,
    pub started_ns: u64,
    pub ended_ns: u64,
    /// The command's exit status, which the recorder passed through.
    pub exit: i32,
}

/// One recorded command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Invocation {
    Complete {
        dir: PathBuf,
        meta: Meta,
        /// The command as the Makefile wrote it, before the recorder added
        /// Bazel's two output flags.
        argv: Vec<String>,
    },
    /// No `meta`: interrupted, or still running in another checkout.
    Incomplete { dir: PathBuf },
}

/// One `make` process's records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Run {
    pub id: String,
    pub dir: PathBuf,
    /// `None` when the run directory has no `run` file yet.
    pub header: Option<RunHeader>,
    /// In start order.
    pub invocations: Vec<Invocation>,
}

/// How a run ended, as far as its records say.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunStatus {
    /// Every recorded command finished with exit 0. Not "the make passed": a
    /// recipe can fail in a line the recorder never wraps (a `test -s` after a
    /// query, say), and that failure is not in the store.
    NoFailureRecorded,
    /// The step of the first recorded command that exited non-zero.
    Failed(String),
    Incomplete,
}

impl fmt::Display for RunStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RunStatus::NoFailureRecorded => f.write_str("no failure recorded"),
            RunStatus::Failed(step) => write!(f, "failed ({step})"),
            RunStatus::Incomplete => f.write_str("incomplete"),
        }
    }
}

impl Run {
    pub fn status(&self) -> RunStatus {
        if self.header.is_none() {
            return RunStatus::Incomplete;
        }
        let mut failed = None;
        for inv in &self.invocations {
            match inv {
                Invocation::Incomplete { .. } => return RunStatus::Incomplete,
                Invocation::Complete { meta, .. } => {
                    if meta.exit != 0 && failed.is_none() {
                        failed = Some(meta.step.clone());
                    }
                }
            }
        }
        match failed {
            Some(step) => RunStatus::Failed(step),
            None => RunStatus::NoFailureRecorded,
        }
    }

    /// First recorded start to last recorded end over the complete commands, or
    /// `None` when there are none.
    pub fn wall_ns(&self) -> Option<u64> {
        let metas = self.invocations.iter().filter_map(|inv| match inv {
            Invocation::Complete { meta, .. } => Some(meta),
            Invocation::Incomplete { .. } => None,
        });
        let mut span: Option<(u64, u64)> = None;
        for meta in metas {
            span = Some(match span {
                None => (meta.started_ns, meta.ended_ns),
                Some((start, end)) => (start.min(meta.started_ns), end.max(meta.ended_ns)),
            });
        }
        span.map(|(start, end)| end - start)
    }
}

/// The store root: `--store`, else `BRENN_BAZEL_TIMING_DIR`, else
/// `~/.cache/brenn-bazel-timing` — the same rule the recorder applies.
pub fn default_store_root() -> PathBuf {
    if let Some(dir) = std::env::var_os("BRENN_BAZEL_TIMING_DIR")
        && !dir.is_empty()
    {
        return PathBuf::from(dir);
    }
    let home = std::env::var_os("HOME")
        .filter(|h| !h.is_empty())
        .unwrap_or_else(|| panic!("xtask timing: HOME is not set and no --store was given"));
    PathBuf::from(home).join(".cache/brenn-bazel-timing")
}

/// The directory runs live in under a store root.
pub fn runs_dir(store_root: &Path) -> PathBuf {
    store_root.join(LAYOUT).join("runs")
}

/// Whether `name` has the run-id shape the Makefile makes:
/// `yyyymmddThhmmss.nnnnnnnnnZ-<pid>`. Ids of that shape sort by start time.
pub fn is_run_id(name: &str) -> bool {
    let Some((stamp, pid)) = name.split_once('-') else {
        return false;
    };
    let b = stamp.as_bytes();
    let digits = |r: std::ops::Range<usize>| b[r].iter().all(u8::is_ascii_digit);
    b.len() == 26
        && digits(0..8)
        && b[8] == b'T'
        && digits(9..15)
        && b[15] == b'.'
        && digits(16..25)
        && b[25] == b'Z'
        && !pid.is_empty()
        && pid.bytes().all(|c| c.is_ascii_digit())
}

/// The ids of every run in the store, oldest first. A store that does not exist
/// yet holds no runs. An entry that is not a run directory is refused: nothing
/// but the recorder writes here, so a stranger means the layout is not what
/// this reader thinks it is.
pub fn run_ids(store_root: &Path) -> Result<Vec<String>, RecordError> {
    let runs = runs_dir(store_root);
    let entries = match std::fs::read_dir(&runs) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(RecordError::new(&runs, format!("cannot list: {e}"))),
    };
    let mut ids = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|e| RecordError::new(&runs, format!("cannot list: {e}")))?;
        let path = entry.path();
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| RecordError::new(&path, "not a run directory: the name is not UTF-8"))?;
        if !is_run_id(&name) || !path.is_dir() {
            return Err(RecordError::new(
                &path,
                "not a run directory; nothing but the recorder writes in the store",
            ));
        }
        ids.push(name);
    }
    ids.sort();
    Ok(ids)
}

/// Read one run: its header and every recorded command's `meta` and `argv`.
/// Bazel's own files are not opened here.
pub fn load_run(store_root: &Path, id: &str) -> Result<Run, RecordError> {
    let dir = runs_dir(store_root).join(id);
    load_run_dir(id, &dir).map_err(|e| e.in_run(&dir))
}

/// Read only a run's `run` file, `None` when the run has none yet: enough to
/// decide whether a view selects the run without reading its commands.
pub fn load_header(store_root: &Path, id: &str) -> Result<Option<RunHeader>, RecordError> {
    let dir = runs_dir(store_root).join(id);
    read_header(id, &dir).map_err(|e| e.in_run(&dir))
}

fn read_header(id: &str, dir: &Path) -> Result<Option<RunHeader>, RecordError> {
    let path = dir.join("run");
    match read_optional(&path)? {
        Some(text) => parse_run_header(&path, &text, id).map(Some),
        None => Ok(None),
    }
}

fn load_run_dir(id: &str, dir: &Path) -> Result<Run, RecordError> {
    let header = read_header(id, dir)?;

    let entries =
        std::fs::read_dir(dir).map_err(|e| RecordError::new(dir, format!("cannot list: {e}")))?;
    let mut keyed = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|e| RecordError::new(dir, format!("cannot list: {e}")))?;
        let path = entry.path();
        let name = entry.file_name().into_string().map_err(|_| {
            RecordError::new(&path, "unexpected entry in a run directory: not UTF-8")
        })?;
        // `run.tmp` is the recorder's half-written header, left by an interrupt.
        if name == "run" || name == "run.tmp" {
            continue;
        }
        let key = parse_invocation_name(&name)
            .filter(|_| path.is_dir())
            .ok_or_else(|| RecordError::new(&path, "unexpected entry in a run directory"))?;
        keyed.push((key, path));
    }
    keyed.sort();

    let mut invocations = Vec::with_capacity(keyed.len());
    for ((started_ns, _pid), path) in keyed {
        invocations.push(load_invocation(&path, started_ns)?);
    }
    Ok(Run {
        id: id.to_string(),
        dir: dir.to_path_buf(),
        header,
        invocations,
    })
}

/// `<start ns>-<pid>`, as the recorder names an invocation directory.
fn parse_invocation_name(name: &str) -> Option<(u64, u64)> {
    let (ns, pid) = name.split_once('-')?;
    Some((parse_decimal(ns)?, parse_decimal(pid)?))
}

fn load_invocation(dir: &Path, dir_started_ns: u64) -> Result<Invocation, RecordError> {
    let meta_path = dir.join("meta");
    let Some(text) = read_optional(&meta_path)? else {
        return Ok(Invocation::Incomplete {
            dir: dir.to_path_buf(),
        });
    };
    let meta = parse_meta(&meta_path, &text)?;
    if meta.started_ns != dir_started_ns {
        return Err(RecordError::new(
            &meta_path,
            format!(
                "started_ns {} is not the {} its directory is named for",
                meta.started_ns, dir_started_ns
            ),
        ));
    }
    // The recorder writes `argv` before `meta`, so a complete record has one.
    let argv_path = dir.join("argv");
    let bytes = std::fs::read(&argv_path)
        .map_err(|e| RecordError::new(&argv_path, format!("cannot read: {e}")))?;
    let argv = parse_argv(&argv_path, &bytes)?;
    Ok(Invocation::Complete {
        dir: dir.to_path_buf(),
        meta,
        argv,
    })
}

/// The file's bytes, `None` when it does not exist.
pub(super) fn read_optional_bytes(path: &Path) -> Result<Option<Vec<u8>>, RecordError> {
    match std::fs::read(path) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(RecordError::new(path, format!("cannot read: {e}"))),
    }
}

/// The file's text, `None` when it does not exist.
pub(super) fn read_optional(path: &Path) -> Result<Option<String>, RecordError> {
    read_optional_bytes(path)?
        .map(|bytes| String::from_utf8(bytes).map_err(|_| RecordError::new(path, "not UTF-8")))
        .transpose()
}

/// NUL-terminated arguments, as `printf '%s\0'` writes them.
fn parse_argv(path: &Path, bytes: &[u8]) -> Result<Vec<String>, RecordError> {
    let Some(body) = bytes.strip_suffix(b"\0") else {
        return Err(RecordError::new(path, "not a NUL-terminated argument list"));
    };
    body.split(|&b| b == 0)
        .map(|arg| {
            String::from_utf8(arg.to_vec())
                .map_err(|_| RecordError::new(path, "an argument is not UTF-8"))
        })
        .collect()
}

/// A `key value` line file with exactly the keys in `keys`, `format` first
/// among them and equal to [`FORMAT`].
fn parse_key_file<'t>(
    path: &Path,
    text: &'t str,
    keys: &[&str],
) -> Result<BTreeMap<&'t str, &'t str>, RecordError> {
    let Some(body) = text.strip_suffix('\n') else {
        return Err(RecordError::new(path, "does not end in a newline"));
    };
    let mut map = BTreeMap::new();
    for line in body.split('\n') {
        let (key, value) = line
            .split_once(' ')
            .ok_or_else(|| RecordError::new(path, format!("line {line:?} is not `key value`")))?;
        if !keys.contains(&key) {
            return Err(RecordError::new(path, format!("unknown key {key:?}")));
        }
        if map.insert(key, value).is_some() {
            return Err(RecordError::new(path, format!("key {key:?} appears twice")));
        }
    }
    match map.get("format") {
        Some(&FORMAT) => {}
        Some(other) => {
            return Err(RecordError::new(
                path,
                format!("format {other:?} is not one this reader knows ({FORMAT})"),
            ));
        }
        None => return Err(RecordError::new(path, "missing key \"format\"")),
    }
    for key in keys {
        if !map.contains_key(key) {
            return Err(RecordError::new(path, format!("missing key {key:?}")));
        }
    }
    Ok(map)
}

/// A non-empty run of ASCII digits; `u64::from_str` alone also takes a sign.
fn parse_decimal(text: &str) -> Option<u64> {
    if text.is_empty() || !text.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    text.parse().ok()
}

fn number(path: &Path, key: &str, value: &str) -> Result<u64, RecordError> {
    parse_decimal(value)
        .ok_or_else(|| RecordError::new(path, format!("{key} {value:?} is not a number")))
}

fn parse_run_header(path: &Path, text: &str, id: &str) -> Result<RunHeader, RecordError> {
    let map = parse_key_file(
        path,
        text,
        &[
            "format",
            "id",
            "goals",
            "checkout",
            "branch",
            "commit",
            "dirty",
            "started_ns",
        ],
    )?;
    if map["id"] != id {
        return Err(RecordError::new(
            path,
            format!("id {:?} is not the run directory's {id:?}", map["id"]),
        ));
    }
    let dirty = match map["dirty"] {
        "yes" => Dirty::Yes,
        "no" => Dirty::No,
        "unknown" => Dirty::Unknown,
        other => {
            return Err(RecordError::new(
                path,
                format!("dirty {other:?} is not yes, no or unknown"),
            ));
        }
    };
    Ok(RunHeader {
        goals: map["goals"].to_string(),
        checkout: map["checkout"].to_string(),
        branch: map["branch"].to_string(),
        commit: map["commit"].to_string(),
        dirty,
        started_ns: number(path, "started_ns", map["started_ns"])?,
    })
}

fn parse_meta(path: &Path, text: &str) -> Result<Meta, RecordError> {
    let map = parse_key_file(
        path,
        text,
        &["format", "step", "kind", "started_ns", "ended_ns", "exit"],
    )?;
    let kind = match map["kind"] {
        "bazel" => Kind::Bazel,
        "command" => Kind::Command,
        other => {
            return Err(RecordError::new(
                path,
                format!("kind {other:?} is not bazel or command"),
            ));
        }
    };
    let started_ns = number(path, "started_ns", map["started_ns"])?;
    let ended_ns = number(path, "ended_ns", map["ended_ns"])?;
    if ended_ns < started_ns {
        return Err(RecordError::new(
            path,
            format!("ended_ns {ended_ns} is before started_ns {started_ns}"),
        ));
    }
    let exit = number(path, "exit", map["exit"])?;
    let exit = i32::try_from(exit)
        .map_err(|_| RecordError::new(path, format!("exit {exit} is out of range")))?;
    Ok(Meta {
        step: map["step"].to_string(),
        kind,
        started_ns,
        ended_ns,
        exit,
    })
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub(crate) const ID_A: &str = "20261122T100000.000000001Z-100";
    pub(crate) const ID_B: &str = "20261122T110000.000000001Z-200";

    pub(crate) fn header_text(id: &str, checkout: &str) -> String {
        format!(
            "format 1\nid {id}\ngoals check\ncheckout {checkout}\nbranch main\n\
             commit 0123456789abcdef0123456789abcdef01234567\ndirty no\nstarted_ns 1000\n"
        )
    }

    pub(crate) fn meta_text(step: &str, kind: &str, start: u64, end: u64, exit: i32) -> String {
        format!(
            "format 1\nstep {step}\nkind {kind}\nstarted_ns {start}\nended_ns {end}\nexit {exit}\n"
        )
    }

    /// A run directory with a header from the checkout `/src/alice/brenn`.
    pub(crate) fn write_run(store: &Path, id: &str) -> PathBuf {
        let dir = runs_dir(store).join(id);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("run"), header_text(id, "/src/alice/brenn")).unwrap();
        dir
    }

    /// One complete recorded command.
    pub(crate) fn write_invocation(run: &Path, meta: &str, start: u64, argv: &[&str]) -> PathBuf {
        let dir = run.join(format!("{start}-7"));
        std::fs::create_dir(&dir).unwrap();
        let mut bytes = Vec::new();
        for arg in argv {
            bytes.extend_from_slice(arg.as_bytes());
            bytes.push(0);
        }
        std::fs::write(dir.join("argv"), bytes).unwrap();
        std::fs::write(dir.join("meta"), meta).unwrap();
        dir
    }

    fn refusal(store: &Path, id: &str) -> RecordError {
        load_run(store, id).expect_err("the record must be refused")
    }

    #[test]
    fn a_complete_run_reads_back_in_start_order() {
        let store = tempfile::tempdir().unwrap();
        let run = write_run(store.path(), ID_A);
        // Written out of order; the reader orders by the start in the name.
        write_invocation(
            &run,
            &meta_text("bazel-policy-parity", "bazel", 3000, 4500, 0),
            3000,
            &["bazel", "build", "//:policy_manifest"],
        );
        write_invocation(
            &run,
            &meta_text("bazel-check", "bazel", 1000, 2500, 0),
            1000,
            &["bazel", "test", "//..."],
        );
        let loaded = load_run(store.path(), ID_A).unwrap();
        let header = loaded.header.as_ref().unwrap();
        assert_eq!(header.checkout, "/src/alice/brenn");
        assert_eq!(header.dirty, Dirty::No);
        let steps: Vec<_> = loaded
            .invocations
            .iter()
            .map(|inv| match inv {
                Invocation::Complete { meta, .. } => meta.step.as_str(),
                Invocation::Incomplete { .. } => panic!("all complete"),
            })
            .collect();
        assert_eq!(steps, ["bazel-check", "bazel-policy-parity"]);
        assert_eq!(loaded.status(), RunStatus::NoFailureRecorded);
        assert_eq!(loaded.wall_ns(), Some(3500));
    }

    #[test]
    fn an_empty_goals_value_and_spaces_in_values_are_kept() {
        let store = tempfile::tempdir().unwrap();
        let dir = runs_dir(store.path()).join(ID_A);
        std::fs::create_dir_all(&dir).unwrap();
        let text = header_text(ID_A, "/src/alice/my brenn").replace("goals check", "goals ");
        std::fs::write(dir.join("run"), text).unwrap();
        let header = load_run(store.path(), ID_A).unwrap().header.unwrap();
        assert_eq!(header.goals, "");
        assert_eq!(header.checkout, "/src/alice/my brenn");
    }

    #[test]
    fn argv_with_spaces_quotes_and_parentheses_round_trips() {
        let store = tempfile::tempdir().unwrap();
        let run = write_run(store.path(), ID_A);
        let query = "somepath(//a:b, @c//d:\"e f\")";
        write_invocation(
            &run,
            &meta_text("bazel-dsl-coherence", "bazel", 1000, 2000, 0),
            1000,
            &["bazel", "cquery", query, ""],
        );
        let loaded = load_run(store.path(), ID_A).unwrap();
        let Invocation::Complete { argv, .. } = &loaded.invocations[0] else {
            panic!("complete");
        };
        assert_eq!(argv, &["bazel", "cquery", query, ""]);
    }

    #[test]
    fn an_invocation_without_meta_is_incomplete_not_an_error() {
        let store = tempfile::tempdir().unwrap();
        let run = write_run(store.path(), ID_A);
        write_invocation(
            &run,
            &meta_text("bazel-check", "bazel", 1000, 2000, 0),
            1000,
            &["bazel", "test"],
        );
        std::fs::create_dir(run.join("3000-9")).unwrap();
        let loaded = load_run(store.path(), ID_A).unwrap();
        assert!(matches!(
            loaded.invocations[1],
            Invocation::Incomplete { .. }
        ));
        assert_eq!(loaded.status(), RunStatus::Incomplete);
        assert_eq!(loaded.wall_ns(), Some(1000));
    }

    #[test]
    fn load_header_reads_the_header_alone() {
        let store = tempfile::tempdir().unwrap();
        let run = write_run(store.path(), ID_A);
        // A command record the header read must not open.
        std::fs::write(run.join("notes.txt"), "hi").unwrap();
        let header = load_header(store.path(), ID_A).unwrap().unwrap();
        assert_eq!(header.checkout, "/src/alice/brenn");
        std::fs::remove_file(run.join("run")).unwrap();
        assert_eq!(load_header(store.path(), ID_A).unwrap(), None);
        std::fs::write(run.join("run"), "format 2\n").unwrap();
        let err = load_header(store.path(), ID_A).unwrap_err();
        assert_eq!(err.path, run.join("run"));
        assert_eq!(err.run_dir.as_deref(), Some(run.as_path()));
    }

    #[test]
    fn a_run_without_its_header_is_incomplete() {
        let store = tempfile::tempdir().unwrap();
        let dir = runs_dir(store.path()).join(ID_A);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("run.tmp"), "format 1\n").unwrap();
        let loaded = load_run(store.path(), ID_A).unwrap();
        assert_eq!(loaded.header, None);
        assert_eq!(loaded.status(), RunStatus::Incomplete);
        assert_eq!(loaded.wall_ns(), None);
    }

    #[test]
    fn the_first_failing_step_names_a_failed_run() {
        let store = tempfile::tempdir().unwrap();
        let run = write_run(store.path(), ID_A);
        write_invocation(
            &run,
            &meta_text("bazel-check", "bazel", 1000, 2000, 3),
            1000,
            &["bazel", "test"],
        );
        let loaded = load_run(store.path(), ID_A).unwrap();
        assert_eq!(loaded.status(), RunStatus::Failed("bazel-check".into()));
    }

    #[test]
    fn meta_with_an_unknown_format_is_refused_by_name() {
        let store = tempfile::tempdir().unwrap();
        let run = write_run(store.path(), ID_A);
        let inv = write_invocation(
            &run,
            &meta_text("s", "bazel", 1000, 2000, 0).replace("format 1", "format 2"),
            1000,
            &["bazel"],
        );
        let err = refusal(store.path(), ID_A);
        assert_eq!(err.path, inv.join("meta"));
        assert!(err.message.contains("format \"2\""), "{err}");
        assert_eq!(err.run_dir.as_deref(), Some(run.as_path()));
        assert!(err.to_string().contains("delete"), "{err}");
    }

    #[test]
    fn meta_with_a_missing_key_is_refused_by_name() {
        let store = tempfile::tempdir().unwrap();
        let run = write_run(store.path(), ID_A);
        let inv = write_invocation(
            &run,
            &meta_text("s", "bazel", 1000, 2000, 0).replace("exit 0\n", ""),
            1000,
            &["bazel"],
        );
        let err = refusal(store.path(), ID_A);
        assert_eq!(err.path, inv.join("meta"));
        assert!(err.message.contains("missing key \"exit\""), "{err}");
    }

    #[test]
    fn meta_with_a_non_numeric_time_is_refused_by_name() {
        let store = tempfile::tempdir().unwrap();
        let run = write_run(store.path(), ID_A);
        let dir = run.join("1000-7");
        std::fs::create_dir(&dir).unwrap();
        std::fs::write(dir.join("argv"), b"bazel\0").unwrap();
        std::fs::write(
            dir.join("meta"),
            meta_text("s", "bazel", 1000, 2000, 0).replace("ended_ns 2000", "ended_ns 17N"),
        )
        .unwrap();
        let err = refusal(store.path(), ID_A);
        assert_eq!(err.path, dir.join("meta"));
        assert!(err.message.contains("ended_ns \"17N\""), "{err}");
    }

    #[test]
    fn meta_with_an_unknown_key_or_kind_is_refused() {
        for bad in [
            meta_text("s", "bazel", 1000, 2000, 0) + "extra 1\n",
            meta_text("s", "make", 1000, 2000, 0),
            meta_text("s", "bazel", 1000, 2000, 0).replace("exit 0", "exit -1"),
            meta_text("s", "bazel", 1000, 900, 0),
        ] {
            let store = tempfile::tempdir().unwrap();
            let run = write_run(store.path(), ID_A);
            let dir = run.join("1000-7");
            std::fs::create_dir(&dir).unwrap();
            std::fs::write(dir.join("argv"), b"bazel\0").unwrap();
            std::fs::write(dir.join("meta"), &bad).unwrap();
            let err = refusal(store.path(), ID_A);
            assert_eq!(err.path, dir.join("meta"), "{bad:?}");
        }
    }

    #[test]
    fn a_header_with_a_bad_dirty_value_or_another_id_is_refused() {
        for bad in [
            header_text(ID_A, "/src/alice/brenn").replace("dirty no", "dirty maybe"),
            header_text(ID_B, "/src/alice/brenn"),
        ] {
            let store = tempfile::tempdir().unwrap();
            let dir = runs_dir(store.path()).join(ID_A);
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join("run"), &bad).unwrap();
            let err = refusal(store.path(), ID_A);
            assert_eq!(err.path, dir.join("run"), "{bad:?}");
        }
    }

    #[test]
    fn meta_without_argv_is_refused() {
        let store = tempfile::tempdir().unwrap();
        let run = write_run(store.path(), ID_A);
        let dir = run.join("1000-7");
        std::fs::create_dir(&dir).unwrap();
        std::fs::write(dir.join("meta"), meta_text("s", "bazel", 1000, 2000, 0)).unwrap();
        let err = refusal(store.path(), ID_A);
        assert_eq!(err.path, dir.join("argv"));
    }

    #[test]
    fn meta_whose_start_is_not_its_directorys_is_refused() {
        let store = tempfile::tempdir().unwrap();
        let run = write_run(store.path(), ID_A);
        let dir = run.join("1000-7");
        std::fs::create_dir(&dir).unwrap();
        std::fs::write(dir.join("argv"), b"bazel\0").unwrap();
        std::fs::write(dir.join("meta"), meta_text("s", "bazel", 1001, 2000, 0)).unwrap();
        assert_eq!(refusal(store.path(), ID_A).path, dir.join("meta"));
    }

    #[test]
    fn a_stranger_in_a_run_directory_is_refused() {
        let store = tempfile::tempdir().unwrap();
        let run = write_run(store.path(), ID_A);
        std::fs::write(run.join("notes.txt"), "hi").unwrap();
        assert_eq!(refusal(store.path(), ID_A).path, run.join("notes.txt"));
    }

    #[test]
    fn run_ids_are_oldest_first_and_a_stranger_in_the_store_is_refused() {
        let store = tempfile::tempdir().unwrap();
        assert_eq!(run_ids(store.path()).unwrap(), Vec::<String>::new());
        write_run(store.path(), ID_B);
        write_run(store.path(), ID_A);
        assert_eq!(run_ids(store.path()).unwrap(), [ID_A, ID_B]);
        let stray = runs_dir(store.path()).join("scratch");
        std::fs::create_dir(&stray).unwrap();
        assert_eq!(run_ids(store.path()).unwrap_err().path, stray);
    }

    #[test]
    fn run_id_shape() {
        assert!(is_run_id(ID_A));
        for bad in [
            "",
            "20261122T100000.000000001Z",
            "20261122T100000.000000001Z-",
            "20261122T100000.00000001Z-1",
            "20261122T100000.000000001Z-1a",
            "../20261122T100000.00000001Z-1",
            "20261122t100000.000000001Z-1",
        ] {
            assert!(!is_run_id(bad), "{bad:?}");
        }
    }
}
