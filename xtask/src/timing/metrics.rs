//! A recorded Bazel command's metrics: its build event file and execution log
//! read together, under the store's rules for which records may lack them.

use std::path::Path;

use super::bep::{self, Bep, BepError};
use super::execlog::{self, ExecLogError, Spawn};
use super::store::{
    self, Invocation, Kind, Meta, RecordError, Run, read_optional, read_optional_bytes,
};

/// What Bazel's two files say about one recorded Bazel command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BazelMetrics {
    pub bep: Bep,
    /// In the order Bazel wrote them.
    pub spawns: Vec<Spawn>,
}

/// A complete recorded command's Bazel metrics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Metrics {
    /// The command was not Bazel and was only timed.
    NotBazel,
    Read(Box<BazelMetrics>),
    /// No Bazel metrics: a command that exited non-zero and left no complete
    /// files. A Bazel command-line error writes no build event file, and an
    /// interrupted command can stop early. Shown with the recorder's times
    /// alone.
    Unavailable,
}

/// Read Bazel's two files for a complete recorded command.
///
/// A command that exited 0 must have both, complete and readable; anything less
/// is refused. A command that exited non-zero may have neither, or files that
/// stop early, and then has [`Metrics::Unavailable`]; complete files are read
/// as usual, because a failed build writes them in full. A file that is complete
/// but not what Bazel writes is refused whatever the exit status.
pub fn load_metrics(run_dir: &Path, dir: &Path, meta: &Meta) -> Result<Metrics, RecordError> {
    if meta.kind == Kind::Command {
        return Ok(Metrics::NotBazel);
    }
    let failed = meta.exit != 0;
    let refuse = |path: &Path, message: String| RecordError::new(path, message).in_run(run_dir);
    let absent = |path: &Path| {
        if failed {
            Ok(Metrics::Unavailable)
        } else {
            Err(refuse(path, "missing for a command that exited 0".into()))
        }
    };

    let bep_path = dir.join("bep.json");
    let Some(text) = read_optional(&bep_path).map_err(|e| e.in_run(run_dir))? else {
        return absent(&bep_path);
    };
    let bep = match bep::parse(&text) {
        Ok(bep) => bep,
        Err(BepError::Incomplete(_)) if failed => return Ok(Metrics::Unavailable),
        Err(e) => return Err(refuse(&bep_path, e.to_string())),
    };

    let exec_path = dir.join("exec.zst");
    let Some(bytes) = read_optional_bytes(&exec_path).map_err(|e| e.in_run(run_dir))? else {
        return absent(&exec_path);
    };
    let spawns = match execlog::parse(&bytes) {
        Ok(spawns) => spawns,
        Err(ExecLogError::Incomplete(_)) if failed => return Ok(Metrics::Unavailable),
        Err(e) => return Err(refuse(&exec_path, e.to_string())),
    };
    Ok(Metrics::Read(Box::new(BazelMetrics { bep, spawns })))
}

/// A run read in full: its records and, for every complete command, Bazel's
/// metrics. One unreadable record fails the whole read.
#[derive(Debug)]
pub struct RunMetrics {
    /// Private with `metrics`, so nothing outside [`RunMetrics::load`] can
    /// change one without the other.
    run: Run,
    /// One per entry of `run.invocations`; `None` exactly for an incomplete one.
    metrics: Vec<Option<Metrics>>,
}

/// One recorded command of a [`RunMetrics`].
#[derive(Debug, Clone, Copy)]
pub enum Command<'a> {
    /// No `meta`: interrupted, or still running.
    Incomplete(&'a Path),
    Complete {
        meta: &'a Meta,
        argv: &'a [String],
        metrics: &'a Metrics,
    },
}

/// A spawn that was executed rather than served from a cache, and the step
/// whose command ran it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Executed<'a> {
    pub step: &'a str,
    pub spawn: &'a Spawn,
}

impl RunMetrics {
    pub fn load(store_root: &Path, id: &str) -> Result<Self, RecordError> {
        let run = store::load_run(store_root, id)?;
        let mut metrics = Vec::with_capacity(run.invocations.len());
        for inv in &run.invocations {
            metrics.push(match inv {
                Invocation::Complete { dir, meta, .. } => Some(load_metrics(&run.dir, dir, meta)?),
                Invocation::Incomplete { .. } => None,
            });
        }
        Ok(RunMetrics { run, metrics })
    }

    /// The run these metrics were loaded from.
    pub fn run(&self) -> &Run {
        &self.run
    }

    /// The recorded commands in start order.
    pub fn commands(&self) -> impl Iterator<Item = Command<'_>> {
        self.run
            .invocations
            .iter()
            .zip(&self.metrics)
            .map(|(inv, metrics)| match (inv, metrics) {
                (Invocation::Incomplete { dir }, None) => Command::Incomplete(dir),
                (Invocation::Complete { meta, argv, .. }, Some(metrics)) => Command::Complete {
                    meta,
                    argv,
                    metrics,
                },
                _ => panic!("RunMetrics: metrics are read for exactly the complete commands"),
            })
    }

    /// Every executed spawn of the run, command by command in the order Bazel
    /// wrote them. Both `show` and `rebuilt` count exactly these.
    pub fn executed(&self) -> Vec<Executed<'_>> {
        let mut out = Vec::new();
        for command in self.commands() {
            if let Command::Complete {
                meta,
                metrics: Metrics::Read(m),
                ..
            } = command
            {
                out.extend(
                    m.spawns
                        .iter()
                        .filter(|s| s.executed())
                        .map(|spawn| Executed {
                            step: &meta.step,
                            spawn,
                        }),
                );
            }
        }
        out
    }
}
