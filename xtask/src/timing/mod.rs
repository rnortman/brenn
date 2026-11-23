//! `xtask timing`: the reader of the Bazel timing record.
//!
//! `bazel/timing/record.sh` files one record per command a recorded `make`
//! recipe runs, grouped into one run per `make` process, in a per-user store
//! outside every working tree; [`store`] reads it back. This module is the
//! command line over it. It never writes to the store: reading a run must not
//! add one, and the reader is a summariser, not a manager of the store.
//! `docs/build-timing.md` describes the views and what their numbers mean.

mod bep;
mod execlog;
mod metrics;
mod rebuilt;
mod runs;
mod select;
mod show;
pub(crate) mod store;

#[cfg(test)]
mod fixture_test;
#[cfg(test)]
mod record_test;

use std::fmt;
use std::path::PathBuf;

use rebuilt::{Filter, Window};
use select::Scope;
use store::RecordError;

pub const USAGE: &str = "timing [--root <dir>] [--store <dir>] <view>
         views: runs [--limit N]
                show [<run id or unique prefix>] [--top N] [--all-checkouts]
                rebuilt [--last N | --since <run id or prefix>] [--all-checkouts]
                        [--branch B] [--goal G]";

/// How many runs `runs` lists when `--limit` is not given.
const DEFAULT_RUNS_LIMIT: usize = 20;

/// How many executed spawns `show` ranks when `--top` is not given.
const DEFAULT_TOP: usize = 30;

/// Why a view printed no report.
#[derive(Debug)]
pub enum TimingError {
    /// A record the reader refuses.
    Record(RecordError),
    /// The arguments name no run, or more than one.
    Select(String),
}

impl From<RecordError> for TimingError {
    fn from(e: RecordError) -> Self {
        TimingError::Record(e)
    }
}

impl fmt::Display for TimingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TimingError::Record(e) => e.fmt(f),
            TimingError::Select(m) => f.write_str(m),
        }
    }
}

fn usage_error(message: &str) -> ! {
    eprintln!("xtask timing: {message}");
    eprintln!("Usage: xtask {USAGE}");
    std::process::exit(2);
}

/// A view and its options, as the command line gave them.
#[derive(Debug, PartialEq, Eq)]
enum View {
    Runs {
        limit: usize,
    },
    Show {
        run: Option<String>,
        top: usize,
        all_checkouts: bool,
    },
    Rebuilt {
        window: Window,
        all_checkouts: bool,
        branch: Option<String>,
        goal: Option<String>,
    },
}

/// The parsed command line. The checkout is still the `--root` as given:
/// resolving it reads the filesystem, and only a view scoped to one checkout
/// needs it.
#[derive(Debug, PartialEq, Eq)]
struct Invocation {
    store: Option<PathBuf>,
    root: Option<PathBuf>,
    view: View,
}

fn value(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<String, String> {
    args.next().ok_or_else(|| format!("{flag} needs a value"))
}

fn count(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<usize, String> {
    value(args, flag)?
        .parse()
        .ok()
        .filter(|n| *n > 0)
        .ok_or_else(|| format!("{flag} needs a positive count"))
}

fn parse(args: &mut impl Iterator<Item = String>) -> Result<Invocation, String> {
    let mut store = None;
    let mut root = None;
    let view = loop {
        let arg = args.next().ok_or("no view given")?;
        match arg.as_str() {
            "--store" => store = Some(PathBuf::from(value(args, "--store")?)),
            "--root" => root = Some(PathBuf::from(value(args, "--root")?)),
            _ => break arg,
        }
    };
    let view = match view.as_str() {
        "runs" => {
            let mut limit = DEFAULT_RUNS_LIMIT;
            while let Some(flag) = args.next() {
                match flag.as_str() {
                    "--limit" => limit = count(args, "--limit")?,
                    other => return Err(format!("unknown option {other:?} for runs")),
                }
            }
            View::Runs { limit }
        }
        "show" => {
            let mut run = None;
            let mut top = DEFAULT_TOP;
            let mut all_checkouts = false;
            while let Some(arg) = args.next() {
                match arg.as_str() {
                    "--top" => top = count(args, "--top")?,
                    "--all-checkouts" => all_checkouts = true,
                    other if other.starts_with('-') => {
                        return Err(format!("unknown option {other:?} for show"));
                    }
                    _ if run.is_some() => return Err("show takes one run".into()),
                    _ => run = Some(arg),
                }
            }
            View::Show {
                run,
                top,
                all_checkouts,
            }
        }
        "rebuilt" => {
            let mut window = None;
            let mut all_checkouts = false;
            let mut branch = None;
            let mut goal = None;
            while let Some(flag) = args.next() {
                let next = match flag.as_str() {
                    "--last" => Window::Last(count(args, "--last")?),
                    "--since" => Window::Since(value(args, "--since")?),
                    "--all-checkouts" => {
                        all_checkouts = true;
                        continue;
                    }
                    "--branch" => {
                        branch = Some(value(args, "--branch")?);
                        continue;
                    }
                    "--goal" => {
                        goal = Some(value(args, "--goal")?);
                        continue;
                    }
                    other => return Err(format!("unknown option {other:?} for rebuilt")),
                };
                if window.replace(next).is_some() {
                    return Err("give one of --last and --since, once".into());
                }
            }
            View::Rebuilt {
                window: window.unwrap_or(Window::Last(rebuilt::DEFAULT_LAST)),
                all_checkouts,
                branch,
                goal,
            }
        }
        other => return Err(format!("unknown view {other:?}")),
    };
    Ok(Invocation { store, root, view })
}

/// The checkout `show` and `rebuilt` default to: `--root`, else the workspace
/// `bazel run` was started from, else the current directory — resolved the way
/// the recorder's `git rev-parse --show-toplevel` resolves it, to a real path.
fn checkout(root: Option<PathBuf>) -> String {
    let root = root
        .or_else(|| std::env::var_os("BUILD_WORKSPACE_DIRECTORY").map(PathBuf::from))
        .unwrap_or_else(|| {
            std::env::current_dir().unwrap_or_else(|e| panic!("xtask timing: no cwd: {e}"))
        });
    let real = root
        .canonicalize()
        .unwrap_or_else(|e| usage_error(&format!("--root {}: {e}", root.display())));
    real.into_os_string()
        .into_string()
        .unwrap_or_else(|p| usage_error(&format!("--root {}: not UTF-8", p.display())))
}

/// One checkout unless `--all-checkouts` was given.
fn scope(all_checkouts: bool, root: Option<PathBuf>) -> Scope {
    Scope {
        checkout: (!all_checkouts).then(|| checkout(root)),
    }
}

/// Run `xtask timing` over the remaining arguments. Returns false when a record
/// is refused or the arguments select no run; the error has been printed and
/// no report has been.
pub fn run_timing(args: &mut impl Iterator<Item = String>) -> bool {
    let Invocation { store, root, view } = parse(args).unwrap_or_else(|e| usage_error(&e));
    let store_root = store.unwrap_or_else(store::default_store_root);

    let report = match view {
        View::Runs { limit } => runs::report(&store_root, limit).map_err(TimingError::from),
        View::Show {
            run,
            top,
            all_checkouts,
        } => show::report(
            &store_root,
            &scope(all_checkouts, root),
            run.as_deref(),
            top,
        ),
        View::Rebuilt {
            window,
            all_checkouts,
            branch,
            goal,
        } => {
            let filter = Filter {
                scope: scope(all_checkouts, root),
                branch,
                goal,
            };
            rebuilt::report(&store_root, &filter, &window)
        }
    };

    match report {
        Ok(text) => {
            print!("{text}");
            true
        }
        Err(e) => {
            eprintln!("xtask timing: {e}");
            false
        }
    }
}

/// A duration in seconds, to a tenth: the resolution a build is waited on at.
pub(crate) fn seconds(ns: u64) -> String {
    format!("{:.1}s", ns as f64 / 1e9)
}

/// Rows of cells as left-aligned columns separated by two spaces, the last
/// column unpadded.
pub(crate) fn columns(rows: &[Vec<String>]) -> String {
    let width = rows.iter().map(Vec::len).max().unwrap_or(0);
    let mut widths = vec![0; width];
    for row in rows {
        for (i, cell) in row.iter().enumerate() {
            widths[i] = widths[i].max(cell.chars().count());
        }
    }
    let mut out = String::new();
    for row in rows {
        let mut line = String::new();
        for (i, cell) in row.iter().enumerate() {
            if i + 1 == row.len() {
                line.push_str(cell);
            } else {
                line.push_str(cell);
                line.push_str(&" ".repeat(widths[i] - cell.chars().count() + 2));
            }
        }
        out.push_str(line.trim_end());
        out.push('\n');
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seconds_round_to_a_tenth() {
        assert_eq!(seconds(0), "0.0s");
        assert_eq!(seconds(1_240_000_000), "1.2s");
        assert_eq!(seconds(312_460_000_000), "312.5s");
    }

    fn parsed(args: &[&str]) -> Result<Invocation, String> {
        parse(&mut args.iter().map(|a| a.to_string()))
    }

    fn view(args: &[&str]) -> View {
        parsed(args)
            .unwrap_or_else(|e| panic!("{args:?}: {e}"))
            .view
    }

    #[test]
    fn global_options_come_before_the_view() {
        assert_eq!(
            parsed(&["--store", "/s", "--root", "/r", "runs"]),
            Ok(Invocation {
                store: Some(PathBuf::from("/s")),
                root: Some(PathBuf::from("/r")),
                view: View::Runs {
                    limit: DEFAULT_RUNS_LIMIT
                },
            })
        );
        assert_eq!(
            parsed(&["runs", "--root", "/r"]),
            Err("unknown option \"--root\" for runs".into())
        );
    }

    #[test]
    fn each_view_reads_its_options_and_defaults_the_rest() {
        assert_eq!(view(&["runs", "--limit", "3"]), View::Runs { limit: 3 });
        assert_eq!(
            view(&["show"]),
            View::Show {
                run: None,
                top: DEFAULT_TOP,
                all_checkouts: false
            }
        );
        assert_eq!(
            view(&["show", "--all-checkouts", "2026", "--top", "5"]),
            View::Show {
                run: Some("2026".into()),
                top: 5,
                all_checkouts: true
            }
        );
        assert_eq!(
            view(&["rebuilt"]),
            View::Rebuilt {
                window: Window::Last(rebuilt::DEFAULT_LAST),
                all_checkouts: false,
                branch: None,
                goal: None
            }
        );
        assert_eq!(
            view(&[
                "rebuilt",
                "--since",
                "2026",
                "--all-checkouts",
                "--branch",
                "main",
                "--goal",
                "check"
            ]),
            View::Rebuilt {
                window: Window::Since("2026".into()),
                all_checkouts: true,
                branch: Some("main".into()),
                goal: Some("check".into())
            }
        );
        assert_eq!(
            view(&["rebuilt", "--last", "4"]),
            View::Rebuilt {
                window: Window::Last(4),
                all_checkouts: false,
                branch: None,
                goal: None
            }
        );
    }

    #[test]
    fn a_malformed_command_line_is_refused_with_its_reason() {
        let cases: &[(&[&str], &str)] = &[
            (&[], "no view given"),
            (&["--store"], "--store needs a value"),
            (&["chart"], "unknown view \"chart\""),
            (&["runs", "--limit", "0"], "--limit needs a positive count"),
            (&["runs", "--limit", "x"], "--limit needs a positive count"),
            (&["show", "--top", "0"], "--top needs a positive count"),
            (&["show", "a", "b"], "show takes one run"),
            (
                &["show", "--verbose"],
                "unknown option \"--verbose\" for show",
            ),
            (
                &["rebuilt", "--last", "2", "--since", "x"],
                "give one of --last and --since, once",
            ),
            (
                &["rebuilt", "--last", "2", "--last", "3"],
                "give one of --last and --since, once",
            ),
            (&["rebuilt", "--since"], "--since needs a value"),
            (&["rebuilt", "x"], "unknown option \"x\" for rebuilt"),
        ];
        for (args, reason) in cases {
            assert_eq!(parsed(args), Err(reason.to_string()), "{args:?}");
        }
    }

    #[test]
    fn columns_align_and_leave_the_last_unpadded() {
        let rows = vec![
            vec!["a".to_string(), "bbb".to_string(), "c".to_string()],
            vec!["aaaa".to_string(), "b".to_string(), "cc".to_string()],
        ];
        assert_eq!(columns(&rows), "a     bbb  c\naaaa  b    cc\n");
    }
}
