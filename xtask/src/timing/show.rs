//! `timing show`: one run in full — each recorded command's times and cache
//! counts, the run's executed spawns ranked by time, and its tests.

use std::collections::BTreeMap;
use std::path::Path;

use super::metrics::{Command, Metrics, RunMetrics};
use super::runs::local_time;
use super::select::{self, Scope};
use super::{TimingError, columns, seconds};

/// Bazel's `runnerCount` rows that count no spawn of their own: the sum, and
/// the actions it runs in-process.
const NOT_SPAWN_RUNNERS: [&str; 2] = ["total", "internal"];

/// The run named by `run` (an id or a unique prefix), else the newest run the
/// scope admits.
pub fn report(
    store_root: &Path,
    scope: &Scope,
    run: Option<&str>,
    top: usize,
) -> Result<String, TimingError> {
    let id = match run {
        Some(prefix) => select::resolve(store_root, prefix)?,
        None => match select::newest(store_root, scope)? {
            Some(id) => id,
            None => return Ok(format!("no runs recorded {}\n", scope.describe())),
        },
    };
    Ok(render(&RunMetrics::load(store_root, &id)?, top, local_time))
}

fn ms(ms: u64) -> u64 {
    ms * 1_000_000
}

/// A signed difference of two nanosecond times, in seconds to a tenth.
fn signed_seconds(ns: i128) -> String {
    if ns < 0 {
        format!("-{}", seconds(ns.unsigned_abs() as u64))
    } else {
        seconds(ns as u64)
    }
}

/// The command shortened to Bazel's verb and the targets it names, or the
/// whole command line for one that is not Bazel. A long query expression is cut.
fn short_command(argv: &[String]) -> String {
    const MAX: usize = 48;
    let text = if argv.first().map(String::as_str) == Some("bazel") && argv.len() > 1 {
        let verb = &argv[1];
        let mut words = vec![verb.clone()];
        let mut after_dashes = false;
        for arg in &argv[2..] {
            if arg == "--" && !after_dashes {
                after_dashes = true;
                continue;
            }
            if !after_dashes && arg.starts_with('-') {
                continue;
            }
            words.push(arg.clone());
            // What follows a `bazel run` target is the binary's own arguments.
            if verb == "run" {
                break;
            }
        }
        words.join(" ")
    } else {
        argv.join(" ")
    };
    if text.chars().count() > MAX {
        let cut: String = text.chars().take(MAX - 3).collect();
        format!("{cut}...")
    } else {
        text
    }
}

/// Spawn counts per runner string, as `<count> <runner>` joined, or `0`.
fn runner_cell(counts: &BTreeMap<&str, u64>) -> String {
    if counts.is_empty() {
        return "0".to_string();
    }
    counts
        .iter()
        .map(|(runner, n)| format!("{n} {runner}"))
        .collect::<Vec<_>>()
        .join(", ")
}

fn render(rm: &RunMetrics, top: usize, time: fn(u64) -> String) -> String {
    let run = rm.run();
    let mut out = format!("run {}  {}\n", run.id, run.status());
    match &run.header {
        Some(h) => {
            out.push_str(&format!(
                "  started {}  goals {}\n  checkout {}  branch {}  commit {}  dirty {}\n",
                time(h.started_ns),
                h.goals,
                h.checkout,
                h.branch,
                h.commit,
                h.dirty.as_str()
            ));
        }
        None => out.push_str("  no run header yet: interrupted or still starting\n"),
    }
    out.push('\n');

    let mut rows = vec![
        [
            "#",
            "step",
            "command",
            "exit",
            "wall",
            "bazel",
            "analysis",
            "exec",
            "outside",
            "configured",
            "executed",
            "ac hits",
            "ac misses",
            "cached",
        ]
        .map(String::from)
        .to_vec(),
    ];
    let mut notes = Vec::new();
    let mut sum_ns = 0u64;
    let mut tests = Vec::new();
    for (i, command) in rm.commands().enumerate() {
        let n = (i + 1).to_string();
        let (meta, argv, metrics) = match command {
            Command::Incomplete(dir) => {
                let name = dir
                    .file_name()
                    .map_or_else(String::new, |f| f.to_string_lossy().into_owned());
                let mut row = vec![n, "-".into(), name, "incomplete".into()];
                row.extend(std::iter::repeat_n("-".to_string(), 10));
                rows.push(row);
                continue;
            }
            Command::Complete {
                meta,
                argv,
                metrics,
            } => (meta, argv, metrics),
        };
        let wall = meta.ended_ns - meta.started_ns;
        sum_ns += wall;
        let mut row = vec![
            n.clone(),
            meta.step.clone(),
            short_command(argv),
            meta.exit.to_string(),
            seconds(wall),
        ];
        match metrics {
            Metrics::NotBazel => row.extend(std::iter::repeat_n("-".to_string(), 9)),
            Metrics::Unavailable => {
                row.push("no Bazel metrics".into());
                row.extend(std::iter::repeat_n("-".to_string(), 8));
            }
            Metrics::Read(m) => {
                let b = &m.bep;
                let mut executed = 0u64;
                let mut cached: BTreeMap<&str, u64> = BTreeMap::new();
                let mut logged: BTreeMap<&str, u64> = BTreeMap::new();
                for s in &m.spawns {
                    *logged.entry(&s.runner).or_default() += 1;
                    if s.executed() {
                        executed += 1;
                    } else {
                        *cached.entry(&s.runner).or_default() += 1;
                    }
                }
                row.extend([
                    seconds(ms(b.wall_ms)),
                    seconds(ms(b.analysis_ms)),
                    seconds(ms(b.execution_ms)),
                    signed_seconds(i128::from(wall) - i128::from(ms(b.wall_ms))),
                    b.targets_configured.to_string(),
                    executed.to_string(),
                    b.action_cache_hits.to_string(),
                    b.action_cache_misses.to_string(),
                    runner_cell(&cached),
                ]);
                for line in &b.discards {
                    notes.push(format!("#{n} {}: \"{line}\"", meta.step));
                }
                let summary: BTreeMap<&str, u64> = b
                    .runner_counts
                    .iter()
                    .filter(|(r, c)| !NOT_SPAWN_RUNNERS.contains(&r.as_str()) && **c > 0)
                    .map(|(r, c)| (r.as_str(), *c))
                    .collect();
                if summary != logged {
                    notes.push(format!(
                        "#{n} {}: the spawn counts differ; execution log: {}; Bazel's summary: {}",
                        meta.step,
                        runner_cell(&logged),
                        runner_cell(&summary)
                    ));
                }
                tests.extend(b.tests.iter().map(|t| (meta.step.as_str(), t)));
            }
        }
        rows.push(row);
    }
    out.push_str(&columns(&rows));
    out.push_str(&format!(
        "sum of the commands {}; run wall {} (first start to last end)\n",
        seconds(sum_ns),
        run.wall_ns().map_or_else(|| "-".to_string(), seconds)
    ));
    out.push_str(
        "wall and outside are the recorder's clock; bazel, analysis and exec are Bazel's \
         own, and Bazel interleaves the two phases, so they need not add up to its wall. \
         outside is wall less bazel: for a bazel run, the binary's own time.\n",
    );
    for note in &notes {
        out.push_str(note);
        out.push('\n');
    }

    let mut executed = rm.executed();
    executed.sort_by_key(|e| std::cmp::Reverse(e.spawn.total_ns));
    out.push('\n');
    if executed.is_empty() {
        out.push_str("executed spawns: none\n");
    } else {
        out.push_str(&format!(
            "executed spawns: {}, the {} longest\n",
            executed.len(),
            top.min(executed.len())
        ));
        let mut rows = vec![
            ["time", "mnemonic", "target", "step"]
                .map(String::from)
                .to_vec(),
        ];
        for e in executed.iter().take(top) {
            rows.push(vec![
                seconds(e.spawn.total_ns),
                e.spawn.mnemonic.clone(),
                e.spawn.target_label.clone(),
                e.step.to_string(),
            ]);
        }
        out.push_str(&columns(&rows));
    }
    out.push_str(
        "Spawns, not actions: a test action is two spawns, and actions Bazel runs \
         in-process have none.\n",
    );

    out.push('\n');
    let cached_locally = tests.iter().filter(|(_, t)| t.cached_locally).count();
    let cached_remotely = tests.iter().filter(|(_, t)| t.cached_remotely).count();
    let mut ran: Vec<_> = tests
        .iter()
        .filter(|(_, t)| !t.cached_locally && !t.cached_remotely)
        .collect();
    ran.sort_by_key(|(_, t)| std::cmp::Reverse(t.duration_ms));
    out.push_str(&format!(
        "tests: {} results; {cached_locally} cached locally, {cached_remotely} cached by a \
         cache service, {} ran\n",
        tests.len(),
        ran.len()
    ));
    if !ran.is_empty() {
        let mut rows = vec![
            ["time", "status", "test", "step"]
                .map(String::from)
                .to_vec(),
        ];
        for (step, t) in ran {
            rows.push(vec![
                seconds(ms(t.duration_ms)),
                t.status.clone(),
                t.label.clone(),
                step.to_string(),
            ]);
        }
        out.push_str(&columns(&rows));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::timing::fixture_test::add_fixture;
    use crate::timing::select::tests::{OTHER, write_run_in};
    use crate::timing::store::tests::{ID_A, ID_B, meta_text, write_invocation, write_run};

    fn fixed_time(ns: u64) -> String {
        format!("t{ns}")
    }

    fn here() -> Scope {
        Scope {
            checkout: Some("/src/alice/brenn".into()),
        }
    }

    fn argv(words: &[&str]) -> Vec<String> {
        words.iter().map(|w| w.to_string()).collect()
    }

    #[test]
    fn a_command_is_shortened_to_its_verb_and_targets() {
        assert_eq!(
            short_command(&argv(&[
                "bazel",
                "test",
                "--config=clippy",
                "--config=rustfmt",
                "//..."
            ])),
            "test //..."
        );
        assert_eq!(
            short_command(&argv(&[
                "bazel", "run", "//xtask", "--", "deny", "--root", "/x"
            ])),
            "run //xtask"
        );
        assert_eq!(
            short_command(&argv(&["bazel", "run", "--", "@pnpm//:pnpm", "audit"])),
            "run @pnpm//:pnpm"
        );
        assert_eq!(
            short_command(&argv(&["bazel", "build", "--", "//a/...", "-//a/b"])),
            "build //a/... -//a/b"
        );
        assert_eq!(
            short_command(&argv(&["./dev/scrub-hook-floor.sh"])),
            "./dev/scrub-hook-floor.sh"
        );
        let long = short_command(&argv(&[
            "bazel",
            "cquery",
            "somepath(//surface/server:server, //brenn-wasm/components/some/long:target)",
        ]));
        assert_eq!(long.chars().count(), 48);
        assert!(long.starts_with("cquery somepath(") && long.ends_with("..."));
    }

    fn rows_of(text: &str) -> Vec<Vec<&str>> {
        text.lines()
            .map(|l| {
                l.split("  ")
                    .map(str::trim)
                    .filter(|c| !c.is_empty())
                    .collect()
            })
            .collect()
    }

    #[test]
    fn a_two_command_run_has_its_rows_their_sum_and_the_ranking() {
        let store = tempfile::tempdir().unwrap();
        let run = write_run(store.path(), ID_A);
        add_fixture(
            &run,
            "cold",
            &meta_text("bazel-check", "bazel", 1000, 5_000_001_000, 0),
            &["bazel", "test", "--config=clippy", "//..."],
        );
        add_fixture(
            &run,
            "cached",
            &meta_text("build", "bazel", 6_000_000_000, 7_000_000_000, 0),
            &["bazel", "build", "//..."],
        );
        let text = render(
            &RunMetrics::load(store.path(), ID_A).unwrap(),
            30,
            fixed_time,
        );
        let rows = rows_of(&text);
        assert_eq!(
            rows[0],
            [format!("run {ID_A}").as_str(), "no failure recorded"],
            "{text}"
        );
        let table = rows.iter().position(|r| r.first() == Some(&"#")).unwrap();
        assert_eq!(
            rows[table + 1],
            [
                "1",
                "bazel-check",
                "test //...",
                "0",
                "5.0s",
                "4.0s",
                "0.3s",
                "0.9s",
                "1.0s",
                "845",
                "3",
                "0",
                "13",
                "0"
            ],
            "{text}"
        );
        assert_eq!(
            rows[table + 2][..5],
            ["2", "build", "build //...", "0", "1.0s"],
            "{text}"
        );
        assert_eq!(rows[table + 2][13], "3 disk cache hit", "{text}");
        assert!(
            text.contains("sum of the commands 6.0s; run wall 7.0s"),
            "{text}"
        );
        assert!(!text.contains("differ"), "{text}");
        let ranked = rows
            .iter()
            .position(|r| {
                r.first()
                    .is_some_and(|c| c.starts_with("executed spawns: 3"))
            })
            .unwrap();
        assert_eq!(
            rows[ranked + 2..ranked + 5],
            [
                ["0.4s", "TestRunner", "//:check_test", "bazel-check"],
                ["0.3s", "Genrule", "//:slow", "bazel-check"],
                ["0.0s", "TestRunner", "//:check_test", "bazel-check"],
            ],
            "{text}"
        );
        assert!(
            text.contains(
                "tests: 2 results; 0 cached locally, 1 cached by a cache service, 1 ran\n"
            ),
            "{text}"
        );
        assert!(
            text.ends_with("0.4s  PASSED  //:check_test  bazel-check\n"),
            "{text}"
        );
    }

    #[test]
    fn the_top_limits_the_ranking() {
        let store = tempfile::tempdir().unwrap();
        let run = write_run(store.path(), ID_A);
        add_fixture(
            &run,
            "cold",
            &meta_text("bazel-check", "bazel", 1000, 2000, 0),
            &["bazel", "test", "//..."],
        );
        let text = render(
            &RunMetrics::load(store.path(), ID_A).unwrap(),
            1,
            fixed_time,
        );
        assert!(
            text.contains("executed spawns: 3, the 1 longest\n"),
            "{text}"
        );
        assert!(!text.contains("Genrule"), "{text}");
    }

    #[test]
    fn a_discard_and_a_runner_count_disagreement_are_printed() {
        let store = tempfile::tempdir().unwrap();
        let run = write_run(store.path(), ID_A);
        add_fixture(
            &run,
            "option",
            &meta_text("bazel-check", "bazel", 1000, 2000, 0),
            &["bazel", "build", "//..."],
        );
        add_fixture(
            &run,
            "failing",
            &meta_text("build", "bazel", 3000, 4000, 1),
            &["bazel", "build", "//:fails"],
        );
        let text = render(
            &RunMetrics::load(store.path(), ID_A).unwrap(),
            30,
            fixed_time,
        );
        assert!(
            text.starts_with(&format!("run {ID_A}  failed (build)\n")),
            "{text}"
        );
        assert!(
            text.contains(
                "#1 bazel-check: \"WARNING: Build option --define has changed, discarding \
                 analysis cache"
            ),
            "{text}"
        );
        assert!(
            text.contains(
                "#2 build: the spawn counts differ; execution log: 1 linux-sandbox; \
                 Bazel's summary: 0\n"
            ),
            "{text}"
        );
        // The option fixture's counts agree with Bazel's.
        assert!(!text.contains("#1 bazel-check: the spawn counts"), "{text}");
    }

    #[test]
    fn timed_commands_missing_metrics_and_incomplete_commands_have_rows() {
        let store = tempfile::tempdir().unwrap();
        let run = write_run(store.path(), ID_A);
        write_invocation(
            &run,
            &meta_text("scrub-selfcheck", "command", 1000, 3_000_001_000, 0),
            1000,
            &["./dev/scrub-hook-floor.sh"],
        );
        write_invocation(
            &run,
            &meta_text("build", "bazel", 4_000_000_000, 4_500_000_000, 2),
            4_000_000_000,
            &["bazel", "build", "--bogus"],
        );
        std::fs::create_dir(run.join("5000000000-9")).unwrap();
        let text = render(
            &RunMetrics::load(store.path(), ID_A).unwrap(),
            30,
            fixed_time,
        );
        let rows = rows_of(&text);
        let table = rows.iter().position(|r| r.first() == Some(&"#")).unwrap();
        assert_eq!(
            rows[table + 1][..6],
            [
                "1",
                "scrub-selfcheck",
                "./dev/scrub-hook-floor.sh",
                "0",
                "3.0s",
                "-"
            ],
            "{text}"
        );
        assert_eq!(
            rows[table + 2][..6],
            ["2", "build", "build", "2", "0.5s", "no Bazel metrics"],
            "{text}"
        );
        assert_eq!(
            rows[table + 3][..4],
            ["3", "-", "5000000000-9", "incomplete"],
            "{text}"
        );
        assert!(text.contains("sum of the commands 3.5s"), "{text}");
        assert!(text.contains("executed spawns: none\n"), "{text}");
        assert!(text.contains("tests: 0 results"), "{text}");
    }

    #[test]
    fn show_picks_the_newest_run_of_the_checkout_or_one_by_prefix() {
        let store = tempfile::tempdir().unwrap();
        let a = write_run(store.path(), ID_A);
        write_invocation(
            &a,
            &meta_text("build", "command", 1000, 2000, 0),
            1000,
            &["true"],
        );
        write_run_in(store.path(), ID_B, OTHER);
        let text = report(store.path(), &here(), None, 30).unwrap();
        assert!(text.starts_with(&format!("run {ID_A}")), "{text}");
        let all = Scope { checkout: None };
        let text = report(store.path(), &all, None, 30).unwrap();
        assert!(text.starts_with(&format!("run {ID_B}")), "{text}");
        let text = report(store.path(), &here(), Some("20261122T11"), 30).unwrap();
        assert!(text.starts_with(&format!("run {ID_B}")), "{text}");
        assert!(matches!(
            report(store.path(), &here(), Some("2026"), 30),
            Err(TimingError::Select(_))
        ));
        let empty = tempfile::tempdir().unwrap();
        assert_eq!(
            report(empty.path(), &here(), None, 30).unwrap(),
            "no runs recorded from /src/alice/brenn\n"
        );
    }

    #[test]
    fn an_unreadable_record_in_the_shown_run_fails_the_view() {
        let store = tempfile::tempdir().unwrap();
        let run = write_run(store.path(), ID_A);
        let inv = write_invocation(
            &run,
            &meta_text("build", "bazel", 1000, 2000, 0),
            1000,
            &["bazel", "build"],
        );
        let Err(TimingError::Record(e)) = report(store.path(), &here(), None, 30) else {
            panic!("refused");
        };
        assert_eq!(e.path, inv.join("bep.json"));
    }
}
