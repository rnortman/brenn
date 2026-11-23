//! `timing runs`: one line per run, newest first, across every checkout that
//! shares the store — the one view that sees the whole machine.

use std::path::Path;

use chrono::TimeZone as _;

use super::store::{self, RecordError, Run};
use super::{columns, seconds};

/// The newest `limit` runs of the store as a table. Every listed run is read in
/// full, so one unreadable record among them fails the view.
pub fn report(store_root: &Path, limit: usize) -> Result<String, RecordError> {
    let ids = store::run_ids(store_root)?;
    if ids.is_empty() {
        return Ok(format!(
            "no runs recorded in {}\n",
            store::runs_dir(store_root).display()
        ));
    }
    let mut runs = Vec::new();
    for id in ids.iter().rev().take(limit) {
        runs.push(store::load_run(store_root, id)?);
    }
    let mut out = render(&runs, local_time);
    if ids.len() > runs.len() {
        out.push_str(&format!(
            "({} of {} runs; --limit N for more)\n",
            runs.len(),
            ids.len()
        ));
    }
    Ok(out)
}

/// Nanoseconds since the epoch as local wall-clock time, to the second.
pub(super) fn local_time(ns: u64) -> String {
    let ns = i64::try_from(ns).unwrap_or_else(|_| panic!("timestamp {ns} ns is past year 2262"));
    chrono::Local
        .timestamp_nanos(ns)
        .format("%Y-%m-%d %H:%M:%S")
        .to_string()
}

fn render(runs: &[Run], time: fn(u64) -> String) -> String {
    let mut rows = vec![
        [
            "run", "start", "goals", "checkout", "branch", "commit", "dirty", "wall", "cmds",
            "status",
        ]
        .map(String::from)
        .to_vec(),
    ];
    for run in runs {
        let status = run.status().to_string();
        let wall = run.wall_ns().map_or_else(|| "-".to_string(), seconds);
        let cmds = run.invocations.len().to_string();
        let row = match &run.header {
            Some(h) => vec![
                run.id.clone(),
                time(h.started_ns),
                h.goals.clone(),
                h.checkout.clone(),
                h.branch.clone(),
                h.commit.chars().take(10).collect(),
                h.dirty.as_str().to_string(),
                wall,
                cmds,
                status,
            ],
            None => {
                let mut row = vec![run.id.clone()];
                row.extend(std::iter::repeat_n("-".to_string(), 6));
                row.extend([wall, cmds, status]);
                row
            }
        };
        rows.push(row);
    }
    columns(&rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::timing::store::tests::{ID_A, ID_B, meta_text, write_invocation, write_run};

    fn fixed_time(ns: u64) -> String {
        format!("t{ns}")
    }

    #[test]
    fn newest_first_with_status_wall_and_command_count() {
        let store = tempfile::tempdir().unwrap();
        let a = write_run(store.path(), ID_A);
        write_invocation(
            &a,
            &meta_text("bazel-check", "bazel", 1000, 2_000_001_000, 0),
            1000,
            &["bazel", "test"],
        );
        let b = write_run(store.path(), ID_B);
        write_invocation(
            &b,
            &meta_text("build", "bazel", 5000, 6000, 1),
            5000,
            &["bazel", "build"],
        );
        std::fs::create_dir(b.join("7000-1")).unwrap();
        let runs = vec![
            store::load_run(store.path(), ID_B).unwrap(),
            store::load_run(store.path(), ID_A).unwrap(),
        ];
        let text = render(&runs, fixed_time);
        let rows: Vec<Vec<_>> = text
            .lines()
            .map(|l| l.split_whitespace().collect())
            .collect();
        assert_eq!(rows.len(), 3, "{text}");
        assert_eq!(rows[0][0], "run", "{text}");
        // An incomplete command outranks a failed one: the run did not finish.
        assert_eq!(
            rows[1],
            [
                ID_B,
                "t1000",
                "check",
                "/src/alice/brenn",
                "main",
                "0123456789",
                "no",
                "0.0s",
                "2",
                "incomplete"
            ],
            "{text}"
        );
        assert_eq!(
            rows[2][..9],
            [
                ID_A,
                "t1000",
                "check",
                "/src/alice/brenn",
                "main",
                "0123456789",
                "no",
                "2.0s",
                "1",
            ],
            "{text}"
        );
        // A run whose recorded commands all passed claims no more than that.
        assert!(
            text.lines()
                .nth(2)
                .unwrap()
                .ends_with("  no failure recorded"),
            "{text}"
        );
    }

    #[test]
    fn a_failed_run_names_its_step_and_a_headerless_run_shows_dashes() {
        let store = tempfile::tempdir().unwrap();
        let a = write_run(store.path(), ID_A);
        write_invocation(
            &a,
            &meta_text("xtask-deny", "bazel", 1000, 2000, 1),
            1000,
            &["bazel", "run"],
        );
        let b = store::runs_dir(store.path()).join(ID_B);
        std::fs::create_dir_all(&b).unwrap();
        let runs = vec![
            store::load_run(store.path(), ID_B).unwrap(),
            store::load_run(store.path(), ID_A).unwrap(),
        ];
        let text = render(&runs, fixed_time);
        let rows: Vec<Vec<_>> = text
            .lines()
            .map(|l| l.split_whitespace().collect())
            .collect();
        assert_eq!(
            rows[1],
            [ID_B, "-", "-", "-", "-", "-", "-", "-", "0", "incomplete"],
            "{text}"
        );
        assert!(
            text.lines()
                .nth(2)
                .unwrap()
                .ends_with("failed (xtask-deny)"),
            "{text}"
        );
    }

    #[test]
    fn the_limit_keeps_the_newest_and_says_how_many_were_left_out() {
        let store = tempfile::tempdir().unwrap();
        write_run(store.path(), ID_A);
        write_run(store.path(), ID_B);
        let text = report(store.path(), 1).unwrap();
        assert!(text.contains(ID_B), "{text}");
        assert!(!text.contains(ID_A), "{text}");
        assert!(
            text.ends_with("(1 of 2 runs; --limit N for more)\n"),
            "{text}"
        );
    }

    #[test]
    fn an_empty_store_says_so_and_a_bad_record_fails_the_view() {
        let store = tempfile::tempdir().unwrap();
        assert!(
            report(store.path(), 5)
                .unwrap()
                .starts_with("no runs recorded")
        );
        let a = write_run(store.path(), ID_A);
        std::fs::write(a.join("run"), "format 9\n").unwrap();
        assert_eq!(report(store.path(), 5).unwrap_err().path, a.join("run"));
    }
}
