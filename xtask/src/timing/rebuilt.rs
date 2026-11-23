//! `timing rebuilt`: across runs, which targets had spawns executed rather than
//! served from a cache, how often, and for how long.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use super::metrics::{Command, Metrics, RunMetrics};
use super::select::{self, Scope};
use super::store;
use super::{TimingError, columns, seconds};

/// How many runs `rebuilt` reads when no window is given.
pub const DEFAULT_LAST: usize = 20;

/// Which runs, by recency.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Window {
    /// The newest N runs the filter admits.
    Last(usize),
    /// The run named by this id or unique prefix and every newer one.
    Since(String),
}

/// Which runs, by what was recorded about them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Filter {
    pub scope: Scope,
    pub branch: Option<String>,
    /// One of the run's `make` goals.
    pub goal: Option<String>,
}

impl Filter {
    fn admits(&self, header: &store::RunHeader) -> bool {
        self.scope.admits(header)
            && self.branch.as_ref().is_none_or(|b| header.branch == *b)
            && self
                .goal
                .as_ref()
                .is_none_or(|g| header.goals.split_whitespace().any(|w| w == g))
    }
}

/// One target's executed spawns across the selected runs.
#[derive(Debug, Clone, PartialEq, Eq)]
struct TargetRow {
    label: String,
    /// Runs in which at least one of its spawns executed.
    runs: usize,
    spawns: usize,
    total_ns: u64,
    max_ns: u64,
    mnemonics: BTreeSet<String>,
}

/// What the selected runs held that `rebuilt` cannot count.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct LeftOut {
    headerless_runs: usize,
    incomplete_commands: usize,
    no_metrics_commands: usize,
}

pub fn report(store_root: &Path, filter: &Filter, window: &Window) -> Result<String, TimingError> {
    let ids = store::run_ids(store_root)?;
    let first = match window {
        Window::Since(prefix) => select::resolve(store_root, prefix)?,
        Window::Last(_) => String::new(),
    };
    let mut admitted = Vec::new();
    let mut headerless = Vec::new();
    for id in ids.iter().filter(|id| **id >= first) {
        match store::load_header(store_root, id)? {
            Some(header) if filter.admits(&header) => admitted.push(id.clone()),
            Some(_) => {}
            None => headerless.push(id.clone()),
        }
    }
    if let Window::Last(n) = window {
        admitted.drain(..admitted.len().saturating_sub(*n));
    }
    let Some(oldest) = admitted.first() else {
        return Ok(format!(
            "no complete runs recorded {}\n",
            filter.scope.describe()
        ));
    };
    // A run without a header names no checkout; those inside the window may be
    // the scope's and are counted, not read.
    let headerless_runs = match window {
        Window::Since(_) => headerless.len(),
        Window::Last(_) => headerless.iter().filter(|id| *id > oldest).count(),
    };
    let mut left_out = LeftOut {
        headerless_runs,
        ..LeftOut::default()
    };
    let mut runs = Vec::with_capacity(admitted.len());
    for id in &admitted {
        let rm = RunMetrics::load(store_root, id)?;
        for command in rm.commands() {
            match command {
                Command::Incomplete(_) => left_out.incomplete_commands += 1,
                Command::Complete {
                    metrics: Metrics::Unavailable,
                    ..
                } => left_out.no_metrics_commands += 1,
                Command::Complete { .. } => {}
            }
        }
        runs.push(rm);
    }
    Ok(render(&runs, &aggregate(&runs), left_out, &filter.scope))
}

/// Per target, ranked by total time, longest first.
fn aggregate(runs: &[RunMetrics]) -> Vec<TargetRow> {
    let mut by_label: BTreeMap<&str, TargetRow> = BTreeMap::new();
    for rm in runs {
        let mut seen: BTreeSet<&str> = BTreeSet::new();
        for e in rm.executed() {
            let label = e.spawn.target_label.as_str();
            let row = by_label.entry(label).or_insert_with(|| TargetRow {
                label: label.to_string(),
                runs: 0,
                spawns: 0,
                total_ns: 0,
                max_ns: 0,
                mnemonics: BTreeSet::new(),
            });
            if seen.insert(label) {
                row.runs += 1;
            }
            row.spawns += 1;
            row.total_ns += e.spawn.total_ns;
            row.max_ns = row.max_ns.max(e.spawn.total_ns);
            row.mnemonics.insert(e.spawn.mnemonic.clone());
        }
    }
    let mut rows: Vec<TargetRow> = by_label.into_values().collect();
    rows.sort_by(|a, b| {
        b.total_ns
            .cmp(&a.total_ns)
            .then_with(|| a.label.cmp(&b.label))
    });
    rows
}

fn render(runs: &[RunMetrics], rows: &[TargetRow], left_out: LeftOut, scope: &Scope) -> String {
    let mut out = format!(
        "executed spawns per target over {} runs {}, {} to {}\n",
        runs.len(),
        scope.describe(),
        runs[0].run().id,
        runs[runs.len() - 1].run().id
    );
    if left_out != LeftOut::default() {
        out.push_str(&format!(
            "left out: {} runs without a header, {} incomplete commands, {} commands with no \
             Bazel metrics\n",
            left_out.headerless_runs, left_out.incomplete_commands, left_out.no_metrics_commands
        ));
    }
    if rows.is_empty() {
        out.push_str("no spawn executed\n");
        return out;
    }
    let mut table = vec![
        ["target", "runs", "spawns", "total", "max", "mnemonics"]
            .map(String::from)
            .to_vec(),
    ];
    for row in rows {
        table.push(vec![
            row.label.clone(),
            row.runs.to_string(),
            row.spawns.to_string(),
            seconds(row.total_ns),
            seconds(row.max_ns),
            row.mnemonics.iter().cloned().collect::<Vec<_>>().join(","),
        ]);
    }
    out.push_str(&columns(&table));
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::timing::fixture_test::add_fixture;
    use crate::timing::select::tests::{ID_C, OTHER, write_run_in};
    use crate::timing::store::runs_dir;
    use crate::timing::store::tests::{ID_A, ID_B, meta_text, write_invocation, write_run};

    fn here() -> Filter {
        Filter {
            scope: Scope {
                checkout: Some("/src/alice/brenn".into()),
            },
            branch: None,
            goal: None,
        }
    }

    /// Three runs of the checkout: cold; option; cached then failing.
    fn three_runs(store: &Path) {
        let a = write_run(store, ID_A);
        add_fixture(
            &a,
            "cold",
            &meta_text("bazel-check", "bazel", 1000, 2000, 0),
            &["bazel", "test", "//..."],
        );
        let b = write_run(store, ID_B);
        add_fixture(
            &b,
            "option",
            &meta_text("bazel-check", "bazel", 1000, 2000, 0),
            &["bazel", "test", "//..."],
        );
        let c = write_run(store, ID_C);
        add_fixture(
            &c,
            "cached",
            &meta_text("bazel-check", "bazel", 1000, 2000, 0),
            &["bazel", "test", "//..."],
        );
        add_fixture(
            &c,
            "failing",
            &meta_text("build", "bazel", 3000, 4000, 1),
            &["bazel", "build", "//:fails"],
        );
    }

    #[test]
    fn a_target_counts_once_per_run_and_sums_its_time() {
        let store = tempfile::tempdir().unwrap();
        three_runs(store.path());
        let runs: Vec<RunMetrics> = [ID_A, ID_B, ID_C]
            .iter()
            .map(|id| RunMetrics::load(store.path(), id).unwrap())
            .collect();
        let rows = aggregate(&runs);
        let summary: Vec<_> = rows
            .iter()
            .map(|r| {
                (
                    r.label.as_str(),
                    r.runs,
                    r.spawns,
                    r.total_ns / 1_000_000,
                    r.max_ns / 1_000_000,
                    r.mnemonics.iter().cloned().collect::<Vec<_>>(),
                )
            })
            .collect();
        assert_eq!(
            summary,
            [
                (
                    "//:check_test",
                    2,
                    3,
                    849,
                    420,
                    vec!["TestRunner".to_string()]
                ),
                ("//:slow", 2, 2, 616, 308, vec!["Genrule".to_string()]),
                ("//:fails", 1, 1, 106, 106, vec!["Genrule".to_string()]),
            ]
        );
    }

    #[test]
    fn rebuilt_counts_exactly_the_spawns_show_lists() {
        let store = tempfile::tempdir().unwrap();
        three_runs(store.path());
        let runs: Vec<RunMetrics> = [ID_A, ID_B, ID_C]
            .iter()
            .map(|id| RunMetrics::load(store.path(), id).unwrap())
            .collect();
        let mut listed: BTreeMap<String, (usize, u64)> = BTreeMap::new();
        for rm in &runs {
            for e in rm.executed() {
                let entry = listed.entry(e.spawn.target_label.clone()).or_default();
                entry.0 += 1;
                entry.1 += e.spawn.total_ns;
            }
        }
        let counted: BTreeMap<String, (usize, u64)> = aggregate(&runs)
            .into_iter()
            .map(|r| (r.label, (r.spawns, r.total_ns)))
            .collect();
        assert_eq!(counted, listed);
    }

    #[test]
    fn the_report_ranks_targets_and_the_window_selects_runs() {
        let store = tempfile::tempdir().unwrap();
        three_runs(store.path());
        let text = report(store.path(), &here(), &Window::Last(20)).unwrap();
        assert!(
            text.starts_with(&format!(
                "executed spawns per target over 3 runs from /src/alice/brenn, {ID_A} to {ID_C}\n\
                 target"
            )),
            "{text}"
        );
        assert!(
            text.contains("//:check_test  2     3       0.8s   0.4s  TestRunner\n"),
            "{text}"
        );
        assert!(
            text.ends_with("//:fails       1     1       0.1s   0.1s  Genrule\n"),
            "{text}"
        );

        let text = report(store.path(), &here(), &Window::Last(1)).unwrap();
        assert!(text.contains("over 1 runs"), "{text}");
        assert!(text.contains("//:fails"), "{text}");
        assert!(!text.contains("//:slow"), "{text}");

        let since = Window::Since("20261122T11".into());
        let text = report(store.path(), &here(), &since).unwrap();
        assert!(text.contains(&format!(
            "over 2 runs from /src/alice/brenn, {ID_B} to {ID_C}"
        )));
    }

    #[test]
    fn the_filter_selects_by_checkout_branch_and_goal() {
        let store = tempfile::tempdir().unwrap();
        three_runs(store.path());
        let other = write_run_in(store.path(), "20261122T130000.000000001Z-400", OTHER);
        add_fixture(
            &other,
            "cold",
            &meta_text("bazel-check", "bazel", 1000, 2000, 0),
            &["bazel", "test", "//..."],
        );
        let text = report(store.path(), &here(), &Window::Last(20)).unwrap();
        assert!(text.contains("over 3 runs"), "{text}");
        let all = Filter {
            scope: Scope { checkout: None },
            ..here()
        };
        let text = report(store.path(), &all, &Window::Last(20)).unwrap();
        assert!(text.contains("over 4 runs from any checkout"), "{text}");
        assert!(text.contains("//:check_test  3"), "{text}");

        let on_branch = Filter {
            branch: Some("topic".into()),
            ..here()
        };
        assert_eq!(
            report(store.path(), &on_branch, &Window::Last(20)).unwrap(),
            "no complete runs recorded from /src/alice/brenn\n"
        );
        let goal = Filter {
            goal: Some("check".into()),
            ..here()
        };
        assert!(
            report(store.path(), &goal, &Window::Last(20))
                .unwrap()
                .contains("over 3 runs")
        );
        let goal = Filter {
            goal: Some("build".into()),
            ..here()
        };
        assert!(
            report(store.path(), &goal, &Window::Last(20))
                .unwrap()
                .starts_with("no complete runs")
        );
    }

    #[test]
    fn incomplete_and_unmeasured_records_are_left_out_and_counted() {
        let store = tempfile::tempdir().unwrap();
        three_runs(store.path());
        std::fs::create_dir(runs_dir(store.path()).join(ID_C).join("9000-1")).unwrap();
        write_invocation(
            &runs_dir(store.path()).join(ID_B),
            &meta_text("build", "bazel", 5000, 6000, 2),
            5000,
            &["bazel", "build", "--bogus"],
        );
        std::fs::create_dir(runs_dir(store.path()).join("20261122T140000.000000001Z-500")).unwrap();
        let text = report(store.path(), &here(), &Window::Last(20)).unwrap();
        assert!(
            text.contains(
                "left out: 1 runs without a header, 1 incomplete commands, 1 commands with no \
                 Bazel metrics\n"
            ),
            "{text}"
        );
        assert!(text.contains("over 3 runs"), "{text}");
    }

    #[test]
    fn one_unreadable_selected_run_fails_the_view() {
        let store = tempfile::tempdir().unwrap();
        three_runs(store.path());
        let bep = runs_dir(store.path())
            .join(ID_B)
            .join("1000-7")
            .join("bep.json");
        std::fs::remove_file(&bep).unwrap();
        let Err(TimingError::Record(e)) = report(store.path(), &here(), &Window::Last(20)) else {
            panic!("refused");
        };
        assert_eq!(e.path, bep);
    }
}
