use std::num::NonZeroU32;
use std::path::PathBuf;

use chrono::{DateTime, TimeZone, Utc};

use cc_usage::aggregate::Scope;
use cc_usage::config::Config;
use cc_usage::format::{csv, json, markdown};
use cc_usage::parse::{SessionAccumulator, parse_main_session, parse_subagent_file};
use cc_usage::report::{ReportType, RowRole, Window};
use cc_usage::{RunOptions, run};

fn fixture_path(rel: &str) -> PathBuf {
    let manifest = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    PathBuf::from(manifest).join("tests/fixtures").join(rel)
}

fn fixed_clock() -> Box<dyn Fn() -> DateTime<Utc>> {
    Box::new(|| Utc.with_ymd_and_hms(2026, 4, 1, 12, 0, 0).unwrap())
}

// ─── Parse-level tests ───────────────────────────────────────────────────────

#[test]
fn parse_main_session_counts() {
    let path = fixture_path("project-root/projects/my-project/test-session-001.jsonl");
    let mut acc = SessionAccumulator::default();
    parse_main_session(&path, &mut acc).unwrap();

    // 4 assistant records with usage in the main session file
    assert_eq!(
        acc.usage_records.len(),
        4,
        "expected 4 usage records, got {}",
        acc.usage_records.len()
    );

    // 1 agent-setting record
    assert_eq!(acc.agent_settings.len(), 1);
    assert_eq!(
        acc.agent_settings[0].agent_setting,
        "alice-core:orchestrator"
    );

    // 3 agent invocations
    assert_eq!(acc.agent_invocations.len(), 3);
}

#[test]
fn parse_subagent_file_counts() {
    let path = fixture_path(
        "project-root/projects/my-project/test-session-001/subagents/agent-agent-aaa111.jsonl",
    );
    let mut acc = SessionAccumulator::default();
    parse_subagent_file(&path, &mut acc).unwrap();

    // 2 assistant records (user record ignored for usage)
    assert_eq!(acc.usage_records.len(), 2);
    // All sidechain
    assert!(acc.usage_records.iter().all(|r| r.is_sidechain));
}

#[test]
fn malformed_line_skipped_exit_0() {
    let path = fixture_path("malformed_session.jsonl");
    let mut acc = SessionAccumulator::default();
    parse_main_session(&path, &mut acc).unwrap();

    // 2 valid records, 1 malformed line
    assert_eq!(acc.usage_records.len(), 2, "expected 2 valid records");
    assert_eq!(acc.warnings.len(), 1, "expected 1 malformed-line warning");
    assert!(matches!(
        acc.warnings[0].kind,
        cc_usage::warnings::WarningKind::MalformedLine
    ));
}

// ─── Session report tests ─────────────────────────────────────────────────────

#[test]
fn session_report_row_structure() {
    let root = fixture_path("project-root/projects");
    let mut cfg = Config::defaults();
    cfg.project_roots = vec![root];

    let mut opts = RunOptions::new(
        cfg,
        Scope::Explicit(vec!["test-session-001".to_string()]),
        ReportType::Session,
    );
    opts.include_invocations = true;
    opts.clock = fixed_clock();

    let report = run(opts).unwrap();
    assert_eq!(report.sessions.len(), 1);
    let sr = &report.sessions[0];

    // Count rows by role
    let parent_rows: Vec<_> = sr
        .rows
        .iter()
        .filter(|r| r.role == RowRole::Parent)
        .collect();
    let subagent_rows: Vec<_> = sr
        .rows
        .iter()
        .filter(|r| r.role == RowRole::Subagent)
        .collect();
    let inv_rows: Vec<_> = sr
        .rows
        .iter()
        .filter(|r| r.role == RowRole::SubagentInvocation)
        .collect();
    let total_rows: Vec<_> = sr
        .rows
        .iter()
        .filter(|r| r.role == RowRole::SessionTotal)
        .collect();

    assert_eq!(total_rows.len(), 1, "expected 1 session total row");
    assert!(!parent_rows.is_empty(), "expected parent rows");
    assert!(!subagent_rows.is_empty(), "expected subagent rows");
    // 3 invocations (aaa111, bbb222, ccc333)
    assert_eq!(inv_rows.len(), 3, "expected 3 invocation rows");
}

#[test]
fn session_total_equals_sum_of_parent_and_subagent() {
    let root = fixture_path("project-root/projects");
    let mut cfg = Config::defaults();
    cfg.project_roots = vec![root];

    let mut opts = RunOptions::new(
        cfg,
        Scope::Explicit(vec!["test-session-001".to_string()]),
        ReportType::Session,
    );
    opts.include_invocations = true;
    opts.clock = fixed_clock();

    let report = run(opts).unwrap();
    let sr = &report.sessions[0];

    let total = sr
        .rows
        .iter()
        .find(|r| r.role == RowRole::SessionTotal)
        .unwrap();

    let summed_input: u64 = sr
        .rows
        .iter()
        .filter(|r| matches!(r.role, RowRole::Parent | RowRole::Subagent))
        .map(|r| r.input_tokens)
        .sum();
    let summed_output: u64 = sr
        .rows
        .iter()
        .filter(|r| matches!(r.role, RowRole::Parent | RowRole::Subagent))
        .map(|r| r.output_tokens)
        .sum();
    let summed_entries: u64 = sr
        .rows
        .iter()
        .filter(|r| matches!(r.role, RowRole::Parent | RowRole::Subagent))
        .map(|r| r.entry_count)
        .sum();

    assert_eq!(
        total.input_tokens, summed_input,
        "session total input mismatch"
    );
    assert_eq!(
        total.output_tokens, summed_output,
        "session total output mismatch"
    );
    assert_eq!(
        total.entry_count, summed_entries,
        "session total entry_count mismatch"
    );

    // start_time on session_total must be the minimum first_timestamp across all
    // parent and subagent rows (not SubagentInvocation).
    // test-session-001: parent min=10:01:00, aaa111 min=10:01:30, bbb222 min=10:02:30,
    //   ccc333 min=10:03:30 → overall min = 10:01:00.
    let expected_total_start: DateTime<Utc> = "2026-04-01T10:01:00Z".parse().unwrap();
    assert_eq!(
        total.start_time,
        Some(expected_total_start),
        "session_total start_time must equal min first_timestamp across all parent/subagent buckets"
    );
    // Every individual parent/subagent row's start_time must be >= total's start_time.
    for row in sr
        .rows
        .iter()
        .filter(|r| matches!(r.role, RowRole::Parent | RowRole::Subagent))
    {
        let row_st = row
            .start_time
            .expect("parent/subagent row must have start_time");
        assert!(
            row_st >= expected_total_start,
            "row start_time {row_st} is before session_total start_time {expected_total_start}"
        );
    }
}

#[test]
fn invocation_rows_sum_to_subagent_rows() {
    let root = fixture_path("project-root/projects");
    let mut cfg = Config::defaults();
    cfg.project_roots = vec![root];

    let mut opts = RunOptions::new(
        cfg,
        Scope::Explicit(vec!["test-session-001".to_string()]),
        ReportType::Session,
    );
    opts.include_invocations = true;
    opts.clock = fixed_clock();

    let report = run(opts).unwrap();
    let sr = &report.sessions[0];

    // For each subagent type, sum invocation entries vs rolled-up entries
    let inv_total: u64 = sr
        .rows
        .iter()
        .filter(|r| r.role == RowRole::SubagentInvocation)
        .map(|r| r.entry_count)
        .sum();
    let sub_total: u64 = sr
        .rows
        .iter()
        .filter(|r| r.role == RowRole::Subagent)
        .map(|r| r.entry_count)
        .sum();
    assert_eq!(
        inv_total, sub_total,
        "invocation entry_count sum should equal subagent entry_count sum"
    );

    // Token check too
    let inv_input: u64 = sr
        .rows
        .iter()
        .filter(|r| r.role == RowRole::SubagentInvocation)
        .map(|r| r.input_tokens)
        .sum();
    let sub_input: u64 = sr
        .rows
        .iter()
        .filter(|r| r.role == RowRole::Subagent)
        .map(|r| r.input_tokens)
        .sum();
    assert_eq!(
        inv_input, sub_input,
        "invocation input_tokens sum should equal subagent input_tokens sum"
    );
}

#[test]
fn no_invocations_flag_suppresses_invocation_rows() {
    let root = fixture_path("project-root/projects");
    let mut cfg = Config::defaults();
    cfg.project_roots = vec![root];

    let mut opts = RunOptions::new(
        cfg,
        Scope::Explicit(vec!["test-session-001".to_string()]),
        ReportType::Session,
    );
    opts.include_invocations = false;
    opts.clock = fixed_clock();

    let report = run(opts).unwrap();
    let sr = &report.sessions[0];

    let inv_rows: Vec<_> = sr
        .rows
        .iter()
        .filter(|r| r.role == RowRole::SubagentInvocation)
        .collect();
    assert!(inv_rows.is_empty(), "invocation rows should be suppressed");
}

// ─── Output format consistency ────────────────────────────────────────────────

#[test]
fn output_formats_are_consistent() {
    let root = fixture_path("project-root/projects");
    let mut cfg = Config::defaults();
    cfg.project_roots = vec![root];

    let make_report = || {
        let mut opts = RunOptions::new(
            cfg.clone(),
            Scope::Explicit(vec!["test-session-001".to_string()]),
            ReportType::Session,
        );
        opts.include_invocations = false;
        opts.clock = fixed_clock();
        run(opts).unwrap()
    };

    let report = make_report();

    // Write JSON and re-parse as flat array of objects (no longer a Report struct)
    let mut json_buf = Vec::new();
    json::write(&report, &mut json_buf).unwrap();
    let flat: Vec<serde_json::Value> = serde_json::from_slice(&json_buf).unwrap();

    // Session-mode JSON: all elements must have non-empty session_id
    assert!(
        flat.iter()
            .all(|v| !v["session_id"].as_str().unwrap_or("").is_empty()),
        "all session-mode JSON elements must have non-empty session_id"
    );
    let orig_total = report.sessions[0]
        .rows
        .iter()
        .find(|r| r.role == RowRole::SessionTotal)
        .unwrap();
    // Find the session_total row in the flat array
    let repr_total = flat
        .iter()
        .find(|v| v["role"] == "session_total")
        .expect("expected session_total in JSON output");
    assert_eq!(
        repr_total["total_tokens"].as_u64().unwrap(),
        orig_total.total_tokens
    );
    assert_eq!(
        repr_total["entry_count"].as_u64().unwrap(),
        orig_total.entry_count
    );
    assert_eq!(repr_total["session_id"], "test-session-001");
    assert!(!repr_total["project"].as_str().unwrap_or("").is_empty());

    // Write markdown — just verify it doesn't panic and contains the session id
    let mut md_buf = Vec::new();
    markdown::write(&report, &mut md_buf).unwrap();
    let md_str = String::from_utf8(md_buf).unwrap();
    assert!(
        md_str.contains("test-session-001"),
        "markdown should contain session id"
    );

    // Write CSV — verify header now starts with session_id,project
    let mut csv_buf = Vec::new();
    csv::write(&report, &mut csv_buf).unwrap();
    let csv_str = String::from_utf8(csv_buf).unwrap();
    assert!(
        csv_str.contains("session_id,project,role,agent_type"),
        "csv should contain updated header"
    );
}

// ─── Determinism ─────────────────────────────────────────────────────────────

#[test]
fn output_is_deterministic() {
    let root = fixture_path("project-root/projects");
    let mut cfg = Config::defaults();
    cfg.project_roots = vec![root];

    let make_report = || {
        let mut opts = RunOptions::new(
            cfg.clone(),
            Scope::Explicit(vec!["test-session-001".to_string()]),
            ReportType::Session,
        );
        opts.include_invocations = true;
        opts.clock = fixed_clock();
        run(opts).unwrap()
    };

    let mut buf1 = Vec::new();
    json::write(&make_report(), &mut buf1).unwrap();
    let mut buf2 = Vec::new();
    json::write(&make_report(), &mut buf2).unwrap();

    assert_eq!(buf1, buf2, "JSON output should be byte-identical on re-run");
}

// ─── --last N ordering ────────────────────────────────────────────────────────

#[test]
fn last_1_selects_most_recent_session() {
    let root = fixture_path("project-root/projects");
    let mut cfg = Config::defaults();
    cfg.project_roots = vec![root];

    let mut opts = RunOptions::new(
        cfg,
        Scope::LastN(NonZeroU32::new(1).unwrap()),
        ReportType::Session,
    );
    opts.clock = fixed_clock();

    let report = run(opts).unwrap();
    assert_eq!(report.sessions.len(), 1);
    // test-session-001 has timestamps up to 2026-04-01, the latest
    assert_eq!(report.sessions[0].session_id, "test-session-001");
}

#[test]
fn last_3_selects_three_most_recent() {
    let root = fixture_path("project-root/projects");
    let mut cfg = Config::defaults();
    cfg.project_roots = vec![root];

    let mut opts = RunOptions::new(
        cfg,
        Scope::LastN(NonZeroU32::new(3).unwrap()),
        ReportType::Session,
    );
    opts.clock = fixed_clock();

    let report = run(opts).unwrap();
    assert_eq!(report.sessions.len(), 3);
    // Should be ordered most-recent first
    assert_eq!(report.sessions[0].session_id, "test-session-001");
}

// ─── Window filtering ─────────────────────────────────────────────────────────

#[test]
fn window_filtering_per_record() {
    let root = fixture_path("project-root/projects");
    let mut cfg = Config::defaults();
    cfg.project_roots = vec![root];

    // Window covers only the start of the session — only records at/after 10:01:30 through 10:02:00
    let from = Utc.with_ymd_and_hms(2026, 4, 1, 10, 1, 30).unwrap();
    let to = Utc.with_ymd_and_hms(2026, 4, 1, 10, 2, 0).unwrap();

    let mut opts = RunOptions::new(
        cfg,
        Scope::Window(Window {
            from: Some(from),
            to: Some(to),
        }),
        ReportType::Session,
    );
    opts.include_invocations = false;
    opts.clock = fixed_clock();

    let report = run(opts).unwrap();
    // All sessions are selected in Window mode, but only records in window count
    // test-session-001: records at 10:01:00 (excluded - before from), 10:02:00 (excluded - == to)
    //   subagent aaa111 has records at 10:01:30 (included), 10:01:45 (included)
    // So total entry count > 0 only for the subagent
    let test_sr = report
        .sessions
        .iter()
        .find(|s| s.session_id == "test-session-001")
        .unwrap();
    let total = test_sr
        .rows
        .iter()
        .find(|r| r.role == RowRole::SessionTotal)
        .unwrap();
    // Exactly 2 records from subagent aaa111 should be in window
    assert_eq!(total.entry_count, 2, "window should include 2 records");

    // out-of-window sessions (old-session-002 in March, mid-session-003 mid-March)
    // must be suppressed entirely — they have zero records in the April window.
    assert_eq!(
        report.sessions.len(),
        1,
        "only sessions with records in window should be retained; got: {:?}",
        report
            .sessions
            .iter()
            .map(|s| &s.session_id)
            .collect::<Vec<_>>()
    );
    assert_eq!(report.sessions[0].session_id, "test-session-001");
}

#[test]
fn window_from_only_drops_sessions_before_from() {
    // --since 7d style: from set, to None.
    let root = fixture_path("project-root/projects");
    let mut cfg = Config::defaults();
    cfg.project_roots = vec![root];

    // from = 2026-04-01 00:00:00 — old-session-002 (March 1) and mid-session-003
    // (March 15) are both before; test-session-001 (April 1 10:01:00+) is after.
    let from = Utc.with_ymd_and_hms(2026, 4, 1, 0, 0, 0).unwrap();

    let mut opts = RunOptions::new(
        cfg,
        Scope::Window(Window {
            from: Some(from),
            to: None,
        }),
        ReportType::Session,
    );
    opts.include_invocations = false;
    opts.clock = fixed_clock();

    let report = run(opts).unwrap();
    assert_eq!(
        report.sessions.len(),
        1,
        "from-only window should retain only sessions with records >= from"
    );
    assert_eq!(report.sessions[0].session_id, "test-session-001");
}

#[test]
fn window_to_only_drops_sessions_after_to() {
    // Inverse: to set, from None.
    let root = fixture_path("project-root/projects");
    let mut cfg = Config::defaults();
    cfg.project_roots = vec![root];

    // to = 2026-03-10 00:00:00 — only old-session-002 (March 1) is before;
    // mid-session-003 (March 15) and test-session-001 (April 1) are after.
    let to = Utc.with_ymd_and_hms(2026, 3, 10, 0, 0, 0).unwrap();

    let mut opts = RunOptions::new(
        cfg,
        Scope::Window(Window {
            from: None,
            to: Some(to),
        }),
        ReportType::Session,
    );
    opts.include_invocations = false;
    opts.clock = fixed_clock();

    let report = run(opts).unwrap();
    assert_eq!(
        report.sessions.len(),
        1,
        "to-only window should retain only sessions with records < to"
    );
    assert_eq!(report.sessions[0].session_id, "old-session-002");
}

// ─── Cost calculation ─────────────────────────────────────────────────────────

#[test]
fn cost_matches_manual_calculation() {
    let root = fixture_path("project-root/projects");
    let mut cfg = Config::defaults();
    cfg.project_roots = vec![root];

    let mut opts = RunOptions::new(
        cfg,
        Scope::Explicit(vec!["old-session-002".to_string()]),
        ReportType::Session,
    );
    opts.include_invocations = false;
    opts.clock = fixed_clock();

    let report = run(opts).unwrap();
    let sr = &report.sessions[0];
    let parent = sr.rows.iter().find(|r| r.role == RowRole::Parent).unwrap();

    // old-session-002 has no `agent-setting` record → parent should be `untyped`.
    assert_eq!(parent.agent_type.as_deref(), Some("untyped"));

    // old-session-002: input=500, output=100, cache_write_5m=0, cache_write_1h=0, cache_read=0
    // After token computation: input = 500 - 0 - 0 = 500, output = 100
    // Sonnet prices: input=3.0, output=15.0 per MTok
    // cost = (500 * 3.0 + 100 * 15.0) / 1_000_000 = (1500 + 1500) / 1_000_000 = 0.003
    let expected_cost = (500.0 * 3.0 + 100.0 * 15.0) / 1_000_000.0;
    let actual_cost = parent.cost_usd.unwrap();
    assert!(
        (actual_cost - expected_cost).abs() < 1e-10,
        "cost mismatch: expected {expected_cost}, got {actual_cost}"
    );
}

#[test]
fn price_override_shifts_only_affected_row() {
    use std::io::Write;
    use tempfile::NamedTempFile;

    let root = fixture_path("project-root/projects");

    let baseline_cfg = {
        let mut cfg = Config::defaults();
        cfg.project_roots = vec![root.clone()];
        cfg
    };

    let toml = r#"
[prices."claude-sonnet-4-6"]
input = 4.00
cache_write_5m = 3.75
cache_write_1h = 6.00
cache_read = 0.30
output = 15.00
"#;
    let mut f = NamedTempFile::new().unwrap();
    f.write_all(toml.as_bytes()).unwrap();
    let modified_cfg = cc_usage::config::load(Some(f.path())).unwrap();
    let mut modified_cfg2 = modified_cfg;
    modified_cfg2.project_roots = vec![root];

    let make_report = |cfg: Config| {
        let mut opts = RunOptions::new(
            cfg,
            Scope::Explicit(vec!["old-session-002".to_string()]),
            ReportType::Session,
        );
        opts.include_invocations = false;
        opts.clock = fixed_clock();
        run(opts).unwrap()
    };

    let baseline = make_report(baseline_cfg);
    let modified = make_report(modified_cfg2);

    let base_parent = baseline.sessions[0]
        .rows
        .iter()
        .find(|r| r.role == RowRole::Parent)
        .unwrap();
    let mod_parent = modified.sessions[0]
        .rows
        .iter()
        .find(|r| r.role == RowRole::Parent)
        .unwrap();

    // old-session-002 uses sonnet. Input price changed from 3.0 to 4.0.
    // Delta = input_tokens * (4.0 - 3.0) / 1_000_000
    let expected_delta = base_parent.input_tokens as f64 * 1.0 / 1_000_000.0;
    let actual_delta = mod_parent.cost_usd.unwrap() - base_parent.cost_usd.unwrap();
    assert!(
        (actual_delta - expected_delta).abs() < 1e-10,
        "cost delta mismatch: expected {expected_delta}, got {actual_delta}"
    );
}

// ─── Unknown model ────────────────────────────────────────────────────────────

#[test]
fn unknown_model_produces_null_cost_and_warning() {
    // Create a temp session with an unrecognized model
    use std::io::Write;
    use tempfile::TempDir;

    let tmpdir = TempDir::new().unwrap();
    let proj_dir = tmpdir.path().join("projects").join("tmp-project");
    std::fs::create_dir_all(&proj_dir).unwrap();

    let session_content = r#"{"type":"assistant","sessionId":"unk-session","timestamp":"2026-04-01T10:00:00Z","message":{"model":"gpt-99-turbo","usage":{"input_tokens":100,"output_tokens":20}}}
"#;
    let mut f = std::fs::File::create(proj_dir.join("unk-session.jsonl")).unwrap();
    f.write_all(session_content.as_bytes()).unwrap();

    let mut cfg = Config::defaults();
    cfg.project_roots = vec![tmpdir.path().join("projects")];

    let mut opts = RunOptions::new(
        cfg,
        Scope::Explicit(vec!["unk-session".to_string()]),
        ReportType::Session,
    );
    opts.include_invocations = false;
    opts.clock = fixed_clock();

    let report = run(opts).unwrap();

    // Should have a warning about unknown model
    let has_unknown_warning = report
        .warnings
        .iter()
        .any(|w| matches!(w.kind, cc_usage::warnings::WarningKind::UnknownModel));
    assert!(has_unknown_warning, "expected UnknownModel warning");

    // cost_usd should be null on the parent row
    let sr = &report.sessions[0];
    let parent = sr.rows.iter().find(|r| r.role == RowRole::Parent).unwrap();
    assert!(
        parent.cost_usd.is_none(),
        "cost_usd should be None for unknown model"
    );
    // But tokens should be populated
    assert!(parent.input_tokens > 0 || parent.output_tokens > 0);
}

// ─── Unreadable root ─────────────────────────────────────────────────────────

#[test]
fn unreadable_explicit_root_returns_error() {
    let mut cfg = Config::defaults();
    cfg.project_roots = vec![PathBuf::from("/nonexistent/path/that/does/not/exist")];

    let opts = RunOptions::new(
        cfg,
        Scope::LastN(NonZeroU32::new(1).unwrap()),
        ReportType::Session,
    );
    let result = run(opts);
    assert!(result.is_err(), "expected error for unreadable root");
}

// ─── Aggregate report ────────────────────────────────────────────────────────

#[test]
fn aggregate_report_sums_across_sessions() {
    let root = fixture_path("project-root/projects");
    let mut cfg = Config::defaults();
    cfg.project_roots = vec![root];

    let mut opts = RunOptions::new(
        cfg,
        Scope::LastN(NonZeroU32::new(3).unwrap()),
        ReportType::Aggregate,
    );
    opts.clock = fixed_clock();

    let report = run(opts).unwrap();

    // Aggregate report shape: aggregate_rows populated, sessions empty.
    assert!(
        report.sessions.is_empty(),
        "aggregate report should have empty sessions"
    );
    assert!(
        !report.aggregate_rows.is_empty(),
        "aggregate report should have rows"
    );

    // Aggregate has no SessionTotal or SubagentInvocation rows — only Parent + Subagent.
    for row in &report.aggregate_rows {
        assert!(
            matches!(row.role, RowRole::Parent | RowRole::Subagent),
            "aggregate should not contain {:?} rows",
            row.role
        );
        assert!(
            row.agent_id.is_none(),
            "aggregate rows should not have agent_id"
        );
    }

    // Compare aggregate parent totals against the per-session sum we'd
    // get by running each session in Session mode.
    let parent_aggregate_input: u64 = report
        .aggregate_rows
        .iter()
        .filter(|r| r.role == RowRole::Parent)
        .map(|r| r.input_tokens)
        .sum();

    // Session-mode equivalent
    let mut session_cfg = Config::defaults();
    session_cfg.project_roots = vec![fixture_path("project-root/projects")];
    let mut session_opts = RunOptions::new(
        session_cfg,
        Scope::LastN(NonZeroU32::new(3).unwrap()),
        ReportType::Session,
    );
    session_opts.clock = fixed_clock();
    let session_report = run(session_opts).unwrap();
    let session_parent_input: u64 = session_report
        .sessions
        .iter()
        .flat_map(|s| s.rows.iter())
        .filter(|r| r.role == RowRole::Parent)
        .map(|r| r.input_tokens)
        .sum();

    assert_eq!(
        parent_aggregate_input, session_parent_input,
        "aggregate parent input_tokens must match per-session sum"
    );
}

// ─── Orphan subagent ─────────────────────────────────────────────────────────

#[test]
fn orphan_subagent_warns_and_buckets_as_unknown() {
    use std::io::Write;
    use tempfile::TempDir;

    let tmpdir = TempDir::new().unwrap();
    let proj_dir = tmpdir.path().join("projects").join("orphan-project");
    let session_dir = proj_dir.join("orphan-session");
    let subagents_dir = session_dir.join("subagents");
    std::fs::create_dir_all(&subagents_dir).unwrap();

    // Main session with no Agent tool invocations
    let main_content = r#"{"type":"assistant","sessionId":"orphan-session","timestamp":"2026-04-01T10:00:00Z","message":{"model":"claude-sonnet-4-6","usage":{"input_tokens":100,"output_tokens":20}}}
"#;
    let mut f = std::fs::File::create(proj_dir.join("orphan-session.jsonl")).unwrap();
    f.write_all(main_content.as_bytes()).unwrap();

    // Subagent file with an agentId never registered in the main session
    let sub_content = r#"{"type":"assistant","sessionId":"orphan-session","agentId":"orphan-aaa","isSidechain":true,"timestamp":"2026-04-01T10:00:30Z","message":{"model":"claude-haiku-4-5","usage":{"input_tokens":500,"output_tokens":80}}}
"#;
    let mut sf = std::fs::File::create(subagents_dir.join("agent-orphan-aaa.jsonl")).unwrap();
    sf.write_all(sub_content.as_bytes()).unwrap();

    let mut cfg = Config::defaults();
    cfg.project_roots = vec![tmpdir.path().join("projects")];

    let mut opts = RunOptions::new(
        cfg,
        Scope::Explicit(vec!["orphan-session".to_string()]),
        ReportType::Session,
    );
    opts.include_invocations = false;
    opts.clock = fixed_clock();

    let report = run(opts).unwrap();

    // Should emit OrphanAgentId warning
    let has_orphan = report
        .warnings
        .iter()
        .any(|w| matches!(w.kind, cc_usage::warnings::WarningKind::OrphanAgentId));
    assert!(has_orphan, "expected OrphanAgentId warning");

    // Subagent row with agent_type="unknown" must exist with the orphan tokens
    let sr = &report.sessions[0];
    let unknown_row = sr
        .rows
        .iter()
        .find(|r| r.role == RowRole::Subagent && r.agent_type.as_deref() == Some("unknown"))
        .expect("expected an 'unknown' subagent row");
    // Orphan tokens: input=500 → after computation input=500, output=80
    assert_eq!(unknown_row.input_tokens, 500);
    assert_eq!(unknown_row.output_tokens, 80);
}

// ─── Rejected-invocation subagent attributed via meta.json ──────────────────
//
// When a parent file has a non-object toolUseResult (e.g. "User rejected tool
// use"), no AgentInvocation is created by normal correlation.  The subagent
// still ran and wrote a JSONL + meta.json sidecar.  Reading the meta supplies
// the agentType so the usage is bucketed correctly instead of falling through
// to "unknown".

#[test]
fn rejected_invocation_attributed_from_meta_json() {
    use std::io::Write;
    use tempfile::TempDir;

    let tmpdir = TempDir::new().unwrap();
    let proj_dir = tmpdir.path().join("projects").join("meta-test-project");
    let subagents_dir = proj_dir.join("meta-test-session").join("subagents");
    std::fs::create_dir_all(&subagents_dir).unwrap();

    // Parent: Agent tool_use block followed by non-object rejection toolUseResult.
    let main_content = r#"{"type":"assistant","sessionId":"meta-test-session","timestamp":"2026-04-01T10:00:00Z","message":{"model":"claude-sonnet-4-6","usage":{"input_tokens":100,"output_tokens":10},"content":[{"type":"tool_use","id":"toolu_reject01","name":"Agent","input":{"subagent_type":"Explore","description":"Explore codebase"}}]}}
{"type":"user","sessionId":"meta-test-session","timestamp":"2026-04-01T10:00:05Z","message":{"content":[{"type":"tool_result","tool_use_id":"toolu_reject01"}]},"toolUseResult":"User rejected tool use"}
"#;
    let mut f = std::fs::File::create(proj_dir.join("meta-test-session.jsonl")).unwrap();
    f.write_all(main_content.as_bytes()).unwrap();

    // Subagent that ran despite the rejection.
    let sub_content = r#"{"type":"assistant","sessionId":"meta-test-session","agentId":"a-meta-aaa","isSidechain":true,"timestamp":"2026-04-01T10:00:03Z","message":{"model":"claude-haiku-4-5-20251001","usage":{"input_tokens":400,"output_tokens":60}}}
"#;
    let mut sf = std::fs::File::create(subagents_dir.join("agent-a-meta-aaa.jsonl")).unwrap();
    sf.write_all(sub_content.as_bytes()).unwrap();

    // Meta sidecar — carries the authoritative agentType.
    let meta_content = r#"{"agentType":"Explore","description":"Explore codebase"}"#;
    let mut mf = std::fs::File::create(subagents_dir.join("agent-a-meta-aaa.meta.json")).unwrap();
    mf.write_all(meta_content.as_bytes()).unwrap();

    let mut cfg = Config::defaults();
    cfg.project_roots = vec![tmpdir.path().join("projects")];

    let mut opts = RunOptions::new(
        cfg,
        Scope::Explicit(vec!["meta-test-session".to_string()]),
        ReportType::Session,
    );
    opts.include_invocations = false;
    opts.clock = fixed_clock();

    let report = run(opts).unwrap();

    // No OrphanAgentId warning — meta.json resolved the type.
    let has_orphan = report
        .warnings
        .iter()
        .any(|w| matches!(w.kind, cc_usage::warnings::WarningKind::OrphanAgentId));
    assert!(
        !has_orphan,
        "unexpected OrphanAgentId warning when meta.json present"
    );

    // The subagent row must be attributed to "Explore", not "unknown".
    let sr = &report.sessions[0];
    let explore_row = sr
        .rows
        .iter()
        .find(|r| r.role == RowRole::Subagent && r.agent_type.as_deref() == Some("Explore"))
        .expect("expected an 'Explore' subagent row attributed from meta.json");
    assert_eq!(explore_row.input_tokens, 400);
    assert_eq!(explore_row.output_tokens, 60);

    // No "unknown" subagent row must exist.
    let unknown_row = sr
        .rows
        .iter()
        .find(|r| r.role == RowRole::Subagent && r.agent_type.as_deref() == Some("unknown"));
    assert!(
        unknown_row.is_none(),
        "found unexpected 'unknown' subagent row"
    );
}

// ─── Orphan subagent directory (no parent .jsonl) is silently skipped ───────

#[test]
fn orphan_subagent_directory_silently_skipped() {
    use std::io::Write;
    use tempfile::TempDir;

    let tmpdir = TempDir::new().unwrap();
    let proj_dir = tmpdir.path().join("projects").join("orphan-only");
    let session_dir = proj_dir.join("session-without-parent");
    let subagents_dir = session_dir.join("subagents");
    std::fs::create_dir_all(&subagents_dir).unwrap();

    // Subagent file present, but NO matching session-without-parent.jsonl in proj_dir.
    let sub_content = r#"{"type":"assistant","sessionId":"session-without-parent","agentId":"orphan-bbb","isSidechain":true,"timestamp":"2026-04-01T10:00:30Z","message":{"model":"claude-haiku-4-5","usage":{"input_tokens":500,"output_tokens":80}}}
"#;
    let mut sf = std::fs::File::create(subagents_dir.join("agent-orphan-bbb.jsonl")).unwrap();
    sf.write_all(sub_content.as_bytes()).unwrap();

    let mut cfg = Config::defaults();
    cfg.project_roots = vec![tmpdir.path().join("projects")];

    // Use a wide-open Window scope to ask for "every session that exists".
    let mut opts = RunOptions::new(
        cfg,
        Scope::Window(Window {
            from: None,
            to: None,
        }),
        ReportType::Session,
    );
    opts.include_invocations = false;
    opts.clock = fixed_clock();

    let report = run(opts).unwrap();

    // The orphan session must not appear in the report at all.
    assert!(
        report.sessions.is_empty(),
        "expected no session reports for orphan-only subagent dir, got {}",
        report.sessions.len()
    );

    // No warnings of any kind.
    assert!(
        report.warnings.is_empty(),
        "expected no warnings for silently-skipped orphan, got {:?}",
        report
            .warnings
            .iter()
            .map(|w| &w.message)
            .collect::<Vec<_>>()
    );
}

// ─── String-valued message.content is tolerated (custom deserializer) ──────

#[test]
fn string_valued_content_is_tolerated() {
    // The agent harness sometimes embeds task-notification XML as a plain
    // string in message.content (instead of an array of content blocks).
    // The custom `deserialize_content` must accept this without producing
    // a malformed-line warning. Token counts on the surrounding records
    // must still be captured correctly.
    //
    // Three records:
    //   1. Normal assistant record with usage and array content (must parse).
    //   2. Assistant record where message.content is a STRING (must parse,
    //      content treated as None; usage still recorded).
    //   3. Normal assistant record with usage (must parse).
    let main = r#"{"type":"assistant","sessionId":"strcontent-session","timestamp":"2026-04-01T10:00:00Z","message":{"model":"claude-sonnet-4-6","usage":{"input_tokens":100,"output_tokens":20},"content":[{"type":"text","text":"hello"}]}}
{"type":"assistant","sessionId":"strcontent-session","timestamp":"2026-04-01T10:00:10Z","message":{"model":"claude-sonnet-4-6","usage":{"input_tokens":50,"output_tokens":10},"content":"<task-notification>\n<status>completed</status>\n</task-notification>"}}
{"type":"assistant","sessionId":"strcontent-session","timestamp":"2026-04-01T10:00:20Z","message":{"model":"claude-sonnet-4-6","usage":{"input_tokens":30,"output_tokens":5},"content":[{"type":"text","text":"bye"}]}}
"#;
    let (_tmp, projects) = build_session_fixture("p", "strcontent-session", main, &[]);

    let mut cfg = Config::defaults();
    cfg.project_roots = vec![projects];

    let mut opts = RunOptions::new(
        cfg,
        Scope::Explicit(vec!["strcontent-session".to_string()]),
        ReportType::Session,
    );
    opts.include_invocations = false;
    opts.clock = fixed_clock();

    let report = run(opts).unwrap();

    // No malformed-line warnings — the string content path must be accepted.
    let malformed: Vec<_> = report
        .warnings
        .iter()
        .filter(|w| matches!(w.kind, cc_usage::warnings::WarningKind::MalformedLine))
        .collect();
    assert!(
        malformed.is_empty(),
        "expected no MalformedLine warnings; got {:?}",
        malformed.iter().map(|w| &w.message).collect::<Vec<_>>()
    );

    // All three usage-bearing assistant records contribute to the parent
    // total, including the one whose content was a plain string.
    let sr = &report.sessions[0];
    let total = sr
        .rows
        .iter()
        .find(|r| r.role == RowRole::SessionTotal)
        .expect("SessionTotal row");
    assert_eq!(total.input_tokens, 100 + 50 + 30);
    assert_eq!(total.output_tokens, 20 + 10 + 5);
}

// ─── Override path: user-record agentType wins over assistant-block subagent_type ─

/// Helper: build a small main + subagent fixture under a tmpdir, returning
/// (tmpdir, projects-root). Caller composes the JSONL strings.
fn build_session_fixture(
    project: &str,
    session_id: &str,
    main_jsonl: &str,
    subagent_files: &[(&str, &str)],
) -> (tempfile::TempDir, std::path::PathBuf) {
    use std::io::Write;
    let tmpdir = tempfile::TempDir::new().unwrap();
    let proj_dir = tmpdir.path().join("projects").join(project);
    std::fs::create_dir_all(&proj_dir).unwrap();
    let main_path = proj_dir.join(format!("{session_id}.jsonl"));
    let mut f = std::fs::File::create(&main_path).unwrap();
    f.write_all(main_jsonl.as_bytes()).unwrap();
    if !subagent_files.is_empty() {
        let sub_dir = proj_dir.join(session_id).join("subagents");
        std::fs::create_dir_all(&sub_dir).unwrap();
        for (fname, content) in subagent_files {
            let mut sf = std::fs::File::create(sub_dir.join(fname)).unwrap();
            sf.write_all(content.as_bytes()).unwrap();
        }
    }
    let projects = tmpdir.path().join("projects");
    (tmpdir, projects)
}

#[test]
fn user_record_agent_type_overrides_assistant_block_subagent_type() {
    // The assistant block declares subagent_type="explorer" but the user
    // record's toolUseResult.agentType says "renamed-explorer". The fix
    // must use the user-record value (the ground-truth completed type).
    let main = r#"{"type":"assistant","sessionId":"override-session","timestamp":"2026-04-01T10:00:00Z","message":{"model":"claude-sonnet-4-6","usage":{"input_tokens":100,"output_tokens":20},"content":[{"type":"tool_use","name":"Agent","id":"tu_x","input":{"subagent_type":"explorer","description":"d"}}]}}
{"type":"user","sessionId":"override-session","timestamp":"2026-04-01T10:00:10Z","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"tu_x","content":[{"type":"text","text":"done"}]}]},"toolUseResult":{"status":"completed","agentId":"agent-X","agentType":"renamed-explorer"}}
"#;
    let sub = r#"{"type":"assistant","sessionId":"override-session","agentId":"agent-X","isSidechain":true,"timestamp":"2026-04-01T10:00:05Z","message":{"model":"claude-haiku-4-5","usage":{"input_tokens":50,"output_tokens":5}}}
"#;
    let (_tmp, projects) = build_session_fixture(
        "p",
        "override-session",
        main,
        &[("agent-agent-X.jsonl", sub)],
    );

    let mut cfg = Config::defaults();
    cfg.project_roots = vec![projects];

    let mut opts = RunOptions::new(
        cfg,
        Scope::Explicit(vec!["override-session".to_string()]),
        ReportType::Session,
    );
    opts.include_invocations = true;
    opts.clock = fixed_clock();

    let report = run(opts).unwrap();
    let sr = &report.sessions[0];

    // Subagent invocation row should carry the user-record value, not the
    // assistant-block value. There must be NO row labeled "explorer".
    let has_renamed = sr.rows.iter().any(|r| {
        matches!(r.role, RowRole::Subagent | RowRole::SubagentInvocation)
            && r.agent_type.as_deref() == Some("renamed-explorer")
    });
    let has_assistant_value = sr.rows.iter().any(|r| {
        matches!(r.role, RowRole::Subagent | RowRole::SubagentInvocation)
            && r.agent_type.as_deref() == Some("explorer")
    });
    assert!(
        has_renamed,
        "expected a row with agent_type='renamed-explorer' (user-record value)"
    );
    assert!(
        !has_assistant_value,
        "must NOT have a row with agent_type='explorer' (assistant-block value)"
    );

    // And the invocation row's agent_id should match the user-record agentId.
    let inv = sr
        .rows
        .iter()
        .find(|r| r.role == RowRole::SubagentInvocation)
        .expect("expected one invocation row");
    assert_eq!(inv.agent_id.as_deref(), Some("agent-X"));
    assert_eq!(inv.agent_type.as_deref(), Some("renamed-explorer"));
}

// ─── Warning branches: missing block id, missing sessionId on assistant ──────

#[test]
fn agent_tool_use_block_missing_id_warns() {
    // Agent tool_use block with no `id` field — should emit a SchemaMismatch warning.
    let main = r#"{"type":"assistant","sessionId":"miss-id-session","timestamp":"2026-04-01T10:00:00Z","message":{"model":"claude-sonnet-4-6","usage":{"input_tokens":100,"output_tokens":20},"content":[{"type":"tool_use","name":"Agent","input":{"subagent_type":"explorer","description":"d"}}]}}
"#;
    let (_tmp, projects) = build_session_fixture("p", "miss-id-session", main, &[]);

    let mut cfg = Config::defaults();
    cfg.project_roots = vec![projects];

    let mut opts = RunOptions::new(
        cfg,
        Scope::Explicit(vec!["miss-id-session".to_string()]),
        ReportType::Session,
    );
    opts.clock = fixed_clock();

    let report = run(opts).unwrap();
    let has_warn = report.warnings.iter().any(|w| {
        matches!(w.kind, cc_usage::warnings::WarningKind::SchemaMismatch)
            && w.message.contains("missing 'id'")
    });
    assert!(
        has_warn,
        "expected SchemaMismatch warning naming missing 'id' field"
    );
}

#[test]
fn agent_tool_use_record_missing_session_id_warns() {
    // Agent tool_use record without sessionId — should emit a SchemaMismatch warning.
    // The record's missing top-level sessionId means the usage extraction also warns;
    // we just assert the missing-sessionId warning fires.
    let main = r#"{"type":"assistant","timestamp":"2026-04-01T10:00:00Z","message":{"model":"claude-sonnet-4-6","usage":{"input_tokens":100,"output_tokens":20},"content":[{"type":"tool_use","name":"Agent","id":"tu_x","input":{"subagent_type":"explorer","description":"d"}}]}}
"#;
    let (_tmp, projects) = build_session_fixture("p", "miss-sess-session", main, &[]);

    let mut cfg = Config::defaults();
    cfg.project_roots = vec![projects];

    // Use Window scope to avoid Explicit's missing-session error path.
    let mut opts = RunOptions::new(
        cfg,
        Scope::Window(Window {
            from: None,
            to: None,
        }),
        ReportType::Session,
    );
    opts.clock = fixed_clock();

    let report = run(opts).unwrap();
    let has_warn = report.warnings.iter().any(|w| {
        matches!(w.kind, cc_usage::warnings::WarningKind::SchemaMismatch)
            && w.message.contains("missing 'sessionId'")
    });
    assert!(
        has_warn,
        "expected SchemaMismatch warning naming missing 'sessionId' on Agent record"
    );
}

// ─── Unmatched user-record toolUseResult silently skipped (no panic, no spurious row) ─

#[test]
fn user_tool_use_result_with_no_pending_silently_skipped() {
    // user record carries toolUseResult with agentId, but no preceding
    // assistant Agent block registered the tool_use_id "tu_orphan".
    // Must not panic and must not emit a SubagentInvocation.
    let main = r#"{"type":"user","sessionId":"unmatched-session","timestamp":"2026-04-01T10:00:00Z","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"tu_orphan","content":[{"type":"text","text":"done"}]}]},"toolUseResult":{"status":"completed","agentId":"agent-Z","agentType":"some-type"}}
{"type":"assistant","sessionId":"unmatched-session","timestamp":"2026-04-01T10:00:10Z","message":{"model":"claude-sonnet-4-6","usage":{"input_tokens":100,"output_tokens":20}}}
"#;
    let (_tmp, projects) = build_session_fixture("p", "unmatched-session", main, &[]);

    let mut cfg = Config::defaults();
    cfg.project_roots = vec![projects];

    let mut opts = RunOptions::new(
        cfg,
        Scope::Explicit(vec!["unmatched-session".to_string()]),
        ReportType::Session,
    );
    opts.include_invocations = true;
    opts.clock = fixed_clock();

    let report = run(opts).unwrap();
    let sr = &report.sessions[0];

    // No SubagentInvocation row should exist.
    let inv_count = sr
        .rows
        .iter()
        .filter(|r| r.role == RowRole::SubagentInvocation)
        .count();
    assert_eq!(
        inv_count, 0,
        "unmatched toolUseResult must not produce an invocation row"
    );
}

// ─── Pending invocation drained on EOF emits a schema_mismatch warning ───────

#[test]
fn unconsumed_pending_invocation_warns_at_eof() {
    // Assistant Agent tool_use registered, but no matching user toolUseResult
    // before EOF (truncated/interrupted file). Drain logic must emit a warning.
    let main = r#"{"type":"assistant","sessionId":"truncated-session","timestamp":"2026-04-01T10:00:00Z","message":{"model":"claude-sonnet-4-6","usage":{"input_tokens":100,"output_tokens":20},"content":[{"type":"tool_use","name":"Agent","id":"tu_lost","input":{"subagent_type":"explorer","description":"d"}}]}}
"#;
    let (_tmp, projects) = build_session_fixture("p", "truncated-session", main, &[]);

    let mut cfg = Config::defaults();
    cfg.project_roots = vec![projects];

    let mut opts = RunOptions::new(
        cfg,
        Scope::Explicit(vec!["truncated-session".to_string()]),
        ReportType::Session,
    );
    opts.clock = fixed_clock();

    let report = run(opts).unwrap();

    let has_warn = report.warnings.iter().any(|w| {
        matches!(w.kind, cc_usage::warnings::WarningKind::SchemaMismatch)
            && w.message.contains("tu_lost")
            && w.message.contains("no matching user toolUseResult")
    });
    assert!(
        has_warn,
        "expected SchemaMismatch warning for unconsumed pending Agent block 'tu_lost'"
    );
}

// ─── Invocation row agent_type explicitly checked in main fixture ────────────

#[test]
fn invocation_rows_carry_correct_agent_types_from_fixture() {
    let root = fixture_path("project-root/projects");
    let mut cfg = Config::defaults();
    cfg.project_roots = vec![root];

    let mut opts = RunOptions::new(
        cfg,
        Scope::Explicit(vec!["test-session-001".to_string()]),
        ReportType::Session,
    );
    opts.include_invocations = true;
    opts.clock = fixed_clock();

    let report = run(opts).unwrap();
    let sr = &report.sessions[0];

    // Each invocation row must have a non-None agent_type and a non-None agent_id.
    let invs: Vec<_> = sr
        .rows
        .iter()
        .filter(|r| r.role == RowRole::SubagentInvocation)
        .collect();
    assert_eq!(invs.len(), 3, "expected 3 invocation rows");
    for inv in &invs {
        assert!(
            inv.agent_type.is_some(),
            "invocation row must have agent_type"
        );
        assert!(inv.agent_id.is_some(), "invocation row must have agent_id");
    }
    // Specific agent_id → agent_type pairings from the fixture.
    let by_id: std::collections::HashMap<_, _> = invs
        .iter()
        .map(|r| {
            (
                r.agent_id.as_deref().unwrap(),
                r.agent_type.as_deref().unwrap(),
            )
        })
        .collect();
    assert_eq!(by_id.get("agent-aaa111"), Some(&"alice-core:explorer"));
    assert_eq!(by_id.get("agent-bbb222"), Some(&"alice-core:impl"));
    assert_eq!(by_id.get("agent-ccc333"), Some(&"alice-core:impl"));
}

// ─── agent-acompact-* files are excluded from subagent discovery ──────────────

#[test]
fn compact_subagent_files_excluded_from_discovery() {
    // A compact file replays invocations from the original agent file with the
    // same agentId, which would cause duplicate-agentId warnings and double-
    // counting. Discovery must silently skip agent-acompact-*.jsonl files.
    let main = r#"{"type":"agent-setting","sessionId":"compact-session","agentSetting":"alice-core:orchestrator"}
{"type":"assistant","sessionId":"compact-session","timestamp":"2026-04-01T10:00:00Z","message":{"model":"claude-opus-4-7","usage":{"input_tokens":100,"output_tokens":10},"content":[{"type":"tool_use","name":"Agent","id":"tu_c1","input":{"subagent_type":"alice-core:impl","description":"impl task"}}]}}
{"type":"user","sessionId":"compact-session","timestamp":"2026-04-01T10:00:05Z","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"tu_c1","content":[{"type":"text","text":"done"}]}]},"toolUseResult":{"status":"completed","agentId":"agent-real1","agentType":"alice-core:impl"}}
"#;
    // The real subagent file.
    let real_sub = r#"{"type":"assistant","sessionId":"compact-session","agentId":"agent-real1","isSidechain":true,"timestamp":"2026-04-01T10:00:02Z","message":{"model":"claude-sonnet-4-6","usage":{"input_tokens":50,"output_tokens":5}}}
"#;
    // A compact file that replays the same invocation with the same agentId.
    let compact_sub = r#"{"type":"user","sessionId":"compact-session","timestamp":"2026-04-01T10:00:06Z","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"tu_c1","content":[{"type":"text","text":"done"}]}]},"toolUseResult":{"status":"completed","agentId":"agent-real1","agentType":"alice-core:impl"}}
"#;
    let (_tmp, projects) = build_session_fixture(
        "p",
        "compact-session",
        main,
        &[
            ("agent-agent-real1.jsonl", real_sub),
            ("agent-acompact-deadbeef12345678.jsonl", compact_sub),
        ],
    );

    let mut cfg = Config::defaults();
    cfg.project_roots = vec![projects];

    let mut opts = RunOptions::new(
        cfg,
        Scope::Explicit(vec!["compact-session".to_string()]),
        ReportType::Session,
    );
    opts.include_invocations = true;
    opts.clock = fixed_clock();

    let report = run(opts).unwrap();

    // No duplicate-agentId warnings — compact file was excluded.
    let dup_warns: Vec<_> = report
        .warnings
        .iter()
        .filter(|w| w.message.contains("duplicate agentId"))
        .collect();
    assert!(
        dup_warns.is_empty(),
        "expected no duplicate-agentId warnings, got: {dup_warns:?}"
    );

    // Exactly one SubagentInvocation row for agent-real1.
    let sr = &report.sessions[0];
    let inv_rows: Vec<_> = sr
        .rows
        .iter()
        .filter(|r| r.role == RowRole::SubagentInvocation)
        .collect();
    assert_eq!(
        inv_rows.len(),
        1,
        "expected exactly one SubagentInvocation row"
    );
    assert_eq!(inv_rows[0].agent_id.as_deref(), Some("agent-real1"));
}

// ─── CSV/JSON format tests (session_id column, flat JSON, invocation sort) ────

#[test]
fn csv_session_mode_is_rfc4180() {
    // AC 1: multi-session CSV is a single RFC-4180 document parseable without
    // special-casing comment or repeated header lines.
    let root = fixture_path("project-root/projects");
    let mut cfg = Config::defaults();
    cfg.project_roots = vec![root];

    let session_ids = vec![
        "test-session-001".to_string(),
        "old-session-002".to_string(),
    ];
    let mut opts = RunOptions::new(
        cfg,
        Scope::Explicit(session_ids.clone()),
        ReportType::Session,
    );
    opts.include_invocations = true;
    opts.clock = fixed_clock();

    let report = run(opts).unwrap();

    let mut csv_buf = Vec::new();
    csv::write(&report, &mut csv_buf).unwrap();

    let mut rdr = ::csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(csv_buf.as_slice());

    let headers = rdr.headers().unwrap().clone();
    // (a) header matches expected column list
    let expected_headers = [
        "session_id",
        "project",
        "role",
        "agent_type",
        "agent_id",
        "model",
        "input_tokens",
        "cache_write_5m_tokens",
        "cache_write_1h_tokens",
        "cache_read_tokens",
        "output_tokens",
        "total_tokens",
        "cost_usd",
        "entry_count",
        "start_time",
    ];
    assert_eq!(
        headers.iter().collect::<Vec<_>>(),
        expected_headers,
        "csv header mismatch"
    );

    let expected_total_rows: usize = report.sessions.iter().map(|sr| sr.rows.len()).sum();
    let mut record_count = 0usize;

    for result in rdr.records() {
        let rec =
            result.unwrap_or_else(|e| panic!("csv parse error on record {record_count}: {e}"));
        // (b) every record has same field count as header
        assert_eq!(
            rec.len(),
            expected_headers.len(),
            "record has wrong field count"
        );
        // (c) no record's first cell starts with '#'
        assert!(
            !rec.get(0).unwrap_or("").starts_with('#'),
            "record first cell starts with '#'"
        );
        // (d) no record equals the header (no re-emitted header rows)
        assert_ne!(
            rec.iter().collect::<Vec<_>>(),
            expected_headers,
            "data record equals header row — per-session header re-emitted"
        );
        // (e) session_id is non-empty and matches one of the requested ids
        let sid = rec.get(0).unwrap_or("");
        assert!(
            session_ids.contains(&sid.to_string()),
            "session_id '{sid}' not in expected set"
        );
        // (g) start_time field is either empty (entry_count == 0) or a valid RFC-3339 UTC string
        let start_time_field = rec.get(expected_headers.len() - 1).unwrap_or("");
        let entry_count: u64 = rec
            .get(expected_headers.len() - 2)
            .unwrap_or("0")
            .parse()
            .unwrap_or(0);
        if entry_count == 0 {
            assert!(
                start_time_field.is_empty(),
                "start_time must be empty when entry_count is 0, got '{start_time_field}'"
            );
        } else {
            start_time_field
                .parse::<DateTime<Utc>>()
                .unwrap_or_else(|_| {
                    panic!("start_time '{start_time_field}' is not a valid RFC-3339 UTC timestamp")
                });
            assert!(
                !start_time_field.is_empty(),
                "start_time must be non-empty when entry_count > 0"
            );
        }
        record_count += 1;
    }
    // (f) total record count matches sum of sr.rows.len()
    assert_eq!(
        record_count, expected_total_rows,
        "csv row count mismatch: expected {expected_total_rows}, got {record_count}"
    );

    // (h) session_total for test-session-001 has start_time == min of all buckets (10:01:00)
    let mut rdr2 = ::csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(csv_buf.as_slice());
    let expected_min: DateTime<Utc> = "2026-04-01T10:01:00Z".parse().unwrap();
    for rec in rdr2.records().flatten() {
        if rec.get(0) == Some("test-session-001") && rec.get(2) == Some("session_total") {
            let st: DateTime<Utc> = rec
                .get(expected_headers.len() - 1)
                .unwrap_or("")
                .parse()
                .expect("session_total start_time must be valid RFC-3339");
            assert_eq!(
                st, expected_min,
                "test-session-001 session_total start_time must equal earliest bucket timestamp"
            );
        }
    }
}

#[test]
fn csv_aggregate_mode_omits_session_columns() {
    // AC 2: aggregate-mode CSV header does not contain session_id or project.
    let root = fixture_path("project-root/projects");
    let mut cfg = Config::defaults();
    cfg.project_roots = vec![root];

    let mut opts = RunOptions::new(
        cfg,
        Scope::LastN(std::num::NonZeroU32::new(3).unwrap()),
        ReportType::Aggregate,
    );
    opts.clock = fixed_clock();

    let report = run(opts).unwrap();

    let mut csv_buf = Vec::new();
    csv::write(&report, &mut csv_buf).unwrap();

    let mut rdr = ::csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(csv_buf.as_slice());

    let headers = rdr.headers().unwrap().clone();
    let header_names: Vec<&str> = headers.iter().collect();

    // Assert the exact full header for aggregate mode (no session_id/project, all other columns present).
    let expected_aggregate_headers: &[&str] = &[
        "role",
        "agent_type",
        "agent_id",
        "model",
        "input_tokens",
        "cache_write_5m_tokens",
        "cache_write_1h_tokens",
        "cache_read_tokens",
        "output_tokens",
        "total_tokens",
        "cost_usd",
        "entry_count",
    ];
    assert_eq!(
        header_names, expected_aggregate_headers,
        "aggregate CSV header mismatch"
    );

    // All data rows have correct field count
    for result in rdr.records() {
        let rec = result.unwrap();
        assert_eq!(
            rec.len(),
            header_names.len(),
            "aggregate row field count mismatch"
        );
    }
}

#[test]
fn json_session_mode_flat_array() {
    // AC 3: session-mode JSON is a flat array; every element has non-empty session_id.
    let root = fixture_path("project-root/projects");
    let mut cfg = Config::defaults();
    cfg.project_roots = vec![root];

    let mut opts = RunOptions::new(
        cfg,
        Scope::Explicit(vec!["test-session-001".to_string()]),
        ReportType::Session,
    );
    opts.include_invocations = false;
    opts.clock = fixed_clock();

    let report = run(opts).unwrap();

    let mut json_buf = Vec::new();
    json::write(&report, &mut json_buf).unwrap();

    let flat: Vec<serde_json::Value> = serde_json::from_slice(&json_buf).unwrap();
    assert!(!flat.is_empty(), "session-mode JSON must be non-empty");

    let always_present_keys = [
        "session_id",
        "project",
        "role",
        "agent_type",
        "agent_id",
        "model",
        "input_tokens",
        "cache_write_5m_tokens",
        "cache_write_1h_tokens",
        "cache_read_tokens",
        "output_tokens",
        "total_tokens",
        "cost_usd",
        "entry_count",
    ];
    for obj in &flat {
        let sid = obj["session_id"].as_str().unwrap_or("");
        assert!(
            !sid.is_empty(),
            "session_id must be non-empty in session mode"
        );
        let proj = obj["project"].as_str().unwrap_or("__missing__");
        assert_ne!(proj, "__missing__", "project field must be present");
        assert!(
            !proj.is_empty(),
            "project must be non-empty in session mode"
        );
        for key in &always_present_keys {
            assert!(obj.get(key).is_some(), "missing key '{key}' in JSON object");
        }
        // start_time: present when entry_count > 0; must be valid RFC-3339 UTC string.
        // Absent (not null) when entry_count == 0 (skip_serializing_if = "Option::is_none").
        let entry_count = obj["entry_count"].as_u64().unwrap_or(0);
        if entry_count > 0 {
            let st_str = obj["start_time"].as_str().unwrap_or_else(|| {
                panic!(
                    "start_time must be a string when entry_count > 0, got: {:?}",
                    obj.get("start_time")
                )
            });
            st_str.parse::<DateTime<Utc>>().unwrap_or_else(|_| {
                panic!("start_time '{st_str}' is not a valid RFC-3339 UTC timestamp")
            });
        }
    }
    // Assert session_total start_time == min of all bucket first_timestamps (10:01:00)
    let expected_min: DateTime<Utc> = "2026-04-01T10:01:00Z".parse().unwrap();
    let session_total = flat
        .iter()
        .find(|obj| obj["role"].as_str() == Some("session_total"))
        .expect("session_total row must exist");
    let total_st: DateTime<Utc> = session_total["start_time"]
        .as_str()
        .unwrap_or_else(|| panic!("session_total start_time must be present"))
        .parse()
        .expect("session_total start_time must be valid RFC-3339");
    assert_eq!(
        total_st, expected_min,
        "session_total start_time must equal earliest bucket timestamp"
    );
    // Assert no start_time field is ever null (null would indicate serializing None, which
    // should be absent instead via skip_serializing_if)
    for obj in &flat {
        if let Some(v) = obj.get("start_time") {
            assert!(
                !v.is_null(),
                "start_time must be absent (not null) when None; got null in {:?}",
                obj
            );
        }
    }
}

#[test]
fn json_aggregate_mode_flat_array_with_empty_session() {
    // AC 3 (aggregate): every element has session_id == "" and project == "".
    let root = fixture_path("project-root/projects");
    let mut cfg = Config::defaults();
    cfg.project_roots = vec![root];

    let mut opts = RunOptions::new(
        cfg,
        Scope::LastN(std::num::NonZeroU32::new(3).unwrap()),
        ReportType::Aggregate,
    );
    opts.clock = fixed_clock();

    let report = run(opts).unwrap();

    let mut json_buf = Vec::new();
    json::write(&report, &mut json_buf).unwrap();

    let flat: Vec<serde_json::Value> = serde_json::from_slice(&json_buf).unwrap();
    assert!(!flat.is_empty(), "aggregate-mode JSON must be non-empty");

    for obj in &flat {
        assert_eq!(
            obj["session_id"], "",
            "session_id must be empty string in aggregate mode"
        );
        assert_eq!(
            obj["project"], "",
            "project must be empty string in aggregate mode"
        );
        assert!(
            obj.get("start_time").is_none(),
            "start_time must be absent in aggregate JSON, got: {:?}",
            obj.get("start_time")
        );
    }
}

#[test]
fn invocation_rows_sorted_by_first_timestamp() {
    // AC 4: invocation rows within a session are sorted by first-seen timestamp
    // ascending, not by UUID lexicographic order.
    //
    // We construct two invocations where the alphabetically-later UUID ("zzz999")
    // occurs chronologically first (timestamp 10:00:01) and the alphabetically-
    // earlier UUID ("aaa111") occurs later (timestamp 10:00:30).
    // After the fix, "zzz999" must appear before "aaa111" in the rows.
    let main = r#"{"type":"agent-setting","sessionId":"sort-test","agentSetting":"alice-core:orchestrator"}
{"type":"assistant","sessionId":"sort-test","timestamp":"2026-04-01T10:00:00Z","message":{"model":"claude-sonnet-4-6","usage":{"input_tokens":100,"output_tokens":10},"content":[{"type":"tool_use","name":"Agent","id":"tu_zzz","input":{"subagent_type":"impl","description":"second alpha but first time"}},{"type":"tool_use","name":"Agent","id":"tu_aaa","input":{"subagent_type":"impl","description":"first alpha but second time"}}]}}
{"type":"user","sessionId":"sort-test","timestamp":"2026-04-01T10:00:05Z","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"tu_zzz","content":[{"type":"text","text":"done"}]}]},"toolUseResult":{"status":"completed","agentId":"agent-zzz999","agentType":"impl"}}
{"type":"user","sessionId":"sort-test","timestamp":"2026-04-01T10:00:35Z","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"tu_aaa","content":[{"type":"text","text":"done"}]}]},"toolUseResult":{"status":"completed","agentId":"agent-aaa111","agentType":"impl"}}
"#;
    // zzz999: two records arriving out of timestamp order (10:00:03 then 10:00:01).
    // This exercises the min() path in RowAcc::add — the first_timestamp must be
    // 10:00:01 (the minimum), not 10:00:03 (the first-seen).
    let sub_zzz = r#"{"type":"assistant","sessionId":"sort-test","agentId":"agent-zzz999","isSidechain":true,"timestamp":"2026-04-01T10:00:03Z","message":{"model":"claude-sonnet-4-6","usage":{"input_tokens":30,"output_tokens":3}}}
{"type":"assistant","sessionId":"sort-test","agentId":"agent-zzz999","isSidechain":true,"timestamp":"2026-04-01T10:00:01Z","message":{"model":"claude-sonnet-4-6","usage":{"input_tokens":20,"output_tokens":2}}}
"#;
    // aaa111: first record at 10:00:30 (chronologically second, alphabetically first)
    let sub_aaa = r#"{"type":"assistant","sessionId":"sort-test","agentId":"agent-aaa111","isSidechain":true,"timestamp":"2026-04-01T10:00:30Z","message":{"model":"claude-sonnet-4-6","usage":{"input_tokens":60,"output_tokens":6}}}
"#;

    let (_tmp, projects) = build_session_fixture(
        "p",
        "sort-test",
        main,
        &[
            ("agent-agent-zzz999.jsonl", sub_zzz),
            ("agent-agent-aaa111.jsonl", sub_aaa),
        ],
    );

    let mut cfg = Config::defaults();
    cfg.project_roots = vec![projects];

    let mut opts = RunOptions::new(
        cfg,
        Scope::Explicit(vec!["sort-test".to_string()]),
        ReportType::Session,
    );
    opts.include_invocations = true;
    opts.clock = fixed_clock();

    let report = run(opts).unwrap();
    let sr = &report.sessions[0];

    let inv_rows: Vec<_> = sr
        .rows
        .iter()
        .filter(|r| r.role == RowRole::SubagentInvocation)
        .collect();

    assert_eq!(inv_rows.len(), 2, "expected 2 invocation rows");
    // zzz999 must come first (earlier timestamp), aaa111 must come second
    assert_eq!(
        inv_rows[0].agent_id.as_deref(),
        Some("agent-zzz999"),
        "first invocation row must be zzz999 (earliest timestamp)"
    );
    assert_eq!(
        inv_rows[1].agent_id.as_deref(),
        Some("agent-aaa111"),
        "second invocation row must be aaa111 (later timestamp)"
    );
    // Assert exact start_time values on invocation rows.
    // zzz999 has records at 10:00:03 and 10:00:01; min must be 10:00:01.
    // aaa111 has a single record at 10:00:30.
    let expected_zzz_start: DateTime<Utc> = "2026-04-01T10:00:01Z".parse().unwrap();
    let expected_aaa_start: DateTime<Utc> = "2026-04-01T10:00:30Z".parse().unwrap();
    assert_eq!(
        inv_rows[0].start_time,
        Some(expected_zzz_start),
        "zzz999 start_time must be the minimum (10:00:01), not first-seen (10:00:03)"
    );
    assert_eq!(
        inv_rows[1].start_time,
        Some(expected_aaa_start),
        "aaa111 start_time must be 10:00:30"
    );
}

#[test]
fn markdown_unchanged_byte_for_byte() {
    // AC 5: markdown output is stable. We use a fixture without invocation rows
    // (include_invocations = false) so timestamp-vs-UUID ordering is irrelevant.
    // This is a snapshot test against a known golden string. The fixture is
    // old-session-002 (single parent row, no subagents) for minimal surface area.
    let root = fixture_path("project-root/projects");
    let mut cfg = Config::defaults();
    cfg.project_roots = vec![root];

    let make_report = || {
        let mut opts = RunOptions::new(
            cfg.clone(),
            Scope::Explicit(vec!["old-session-002".to_string()]),
            ReportType::Session,
        );
        opts.include_invocations = false;
        opts.clock = fixed_clock();
        run(opts).unwrap()
    };

    let report = make_report();

    let mut md = Vec::new();
    markdown::write(&report, &mut md).unwrap();

    // Write golden file only when UPDATE_GOLDEN is explicitly set by the developer.
    // Do NOT auto-generate on missing file — a missing golden means CI has no baseline,
    // which must be an error, not a silent self-heal.
    let golden_path = fixture_path("markdown_old-session-002.golden");
    if std::env::var("UPDATE_GOLDEN").is_ok() {
        std::fs::write(&golden_path, &md).expect("failed to write golden file");
    }

    let golden = std::fs::read(&golden_path)
        .expect("golden file missing; run with UPDATE_GOLDEN=1 to generate");
    assert_eq!(
        md, golden,
        "markdown output does not match golden file; run with UPDATE_GOLDEN=1 to regenerate"
    );

    // Sanity check: must contain the session id
    let md_str = String::from_utf8(md).unwrap();
    assert!(
        md_str.contains("old-session-002"),
        "markdown must contain session id"
    );
}

// ─── Non-object toolUseResult (rejection) silently drains pending entry ───────

#[test]
fn rejected_tool_use_drains_pending_no_eof_warning() {
    // When a user sends "User rejected tool use" (a non-object toolUseResult),
    // the pending Agent block must be consumed so no spurious
    // "no matching user toolUseResult" warning is emitted at end-of-file.
    let main = r#"{"type":"assistant","sessionId":"reject-session","timestamp":"2026-04-01T10:00:00Z","message":{"model":"claude-sonnet-4-6","usage":{"input_tokens":100,"output_tokens":10},"content":[{"type":"tool_use","name":"Agent","id":"tu_rej","input":{"subagent_type":"Explore","description":"explore something"}}]}}
{"type":"user","sessionId":"reject-session","timestamp":"2026-04-01T10:00:02Z","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"tu_rej","is_error":true,"content":"The user doesn't want to proceed with this tool use."}]},"toolUseResult":"User rejected tool use"}
{"type":"assistant","sessionId":"reject-session","timestamp":"2026-04-01T10:00:10Z","message":{"model":"claude-sonnet-4-6","usage":{"input_tokens":110,"output_tokens":5}}}
"#;
    let (_tmp, projects) = build_session_fixture("p", "reject-session", main, &[]);

    let mut cfg = Config::defaults();
    cfg.project_roots = vec![projects];

    let mut opts = RunOptions::new(
        cfg,
        Scope::Explicit(vec!["reject-session".to_string()]),
        ReportType::Session,
    );
    opts.clock = fixed_clock();

    let report = run(opts).unwrap();

    // No "no matching user toolUseResult" warning.
    let eof_warns: Vec<_> = report
        .warnings
        .iter()
        .filter(|w| w.message.contains("no matching user toolUseResult"))
        .collect();
    assert!(
        eof_warns.is_empty(),
        "rejected tool use must not produce an EOF warning, got: {eof_warns:?}"
    );
}
