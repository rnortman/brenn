//! `cc-usage`: parse Claude Code session JSONL logs and produce per-subagent
//! usage and cost breakdowns.
//!
//! # Architecture
//!
//! The library is split into focused modules:
//!
//! - [`discovery`] — locate project roots and session/subagent files.
//! - [`schema`] — serde structs for raw JSONL records.
//! - [`parse`] — JSONL line iterator and record classification.
//! - [`tokens`] — the precise five-class token computation.
//! - [`pricing`] — price table, defaults, family fallback, fingerprinting.
//! - [`config`] — TOML config loader.
//! - [`attribution`] — parent agent type lookup; agentId → subagent_type map.
//! - [`aggregate`] — bucketing into rows; per-session and aggregate reports.
//! - [`report`] — the canonical `Report` / `Row` structs.
//! - [`format`] — Markdown, CSV, JSON formatters.
//! - [`error`] — error enum.
//! - [`warnings`] — `Warning` struct collected during a run.

pub mod aggregate;
pub mod attribution;
pub mod config;
pub mod discovery;
pub mod error;
pub mod format;
pub mod parse;
pub mod pricing;
pub mod report;
pub mod schema;
pub mod tokens;
pub mod warnings;

use chrono::{DateTime, Utc};

use std::collections::HashSet;
use std::path::Path;

use crate::aggregate::{Scope, build_aggregate_report, build_session_report, select_sessions};
use crate::attribution::{AgentInvocation, AgentSettingRecord, AgentTypeMap, ParentTypeMap};
use crate::config::Config;
use crate::discovery::{DiscoveredSession, SubagentEntry, discover_roots, discover_sessions};
use crate::error::Result;
use crate::parse::{SessionAccumulator, UsageRecord, parse_main_session, parse_subagent_file};
use crate::report::{Report, ReportType, Window};
use crate::warnings::Warning;

/// Options controlling a usage analysis run.
pub struct RunOptions {
    pub config: Config,
    pub scope: Scope,
    pub report_type: ReportType,
    pub include_invocations: bool,
    /// Injected clock for determinism in tests. Production uses `Utc::now`.
    pub clock: Box<dyn Fn() -> DateTime<Utc>>,
}

impl RunOptions {
    pub fn new(config: Config, scope: Scope, report_type: ReportType) -> Self {
        Self {
            config,
            scope,
            report_type,
            include_invocations: true,
            clock: Box::new(Utc::now),
        }
    }
}

/// All parsed data for a single session.
struct ParsedSession {
    ds: DiscoveredSession,
    usage_records: Vec<UsageRecord>,
    agent_settings: Vec<AgentSettingRecord>,
    agent_invocations: Vec<AgentInvocation>,
}

/// Parse a discovered session into its components.
fn parse_session(ds: &DiscoveredSession, warnings: &mut Vec<Warning>) -> Result<ParsedSession> {
    let mut acc = SessionAccumulator::default();
    parse_main_session(&ds.main_path, &mut acc)?;
    for entry in &ds.subagents {
        parse_subagent_file(&entry.jsonl_path, &mut acc)?;
    }
    // Supplement agent_invocations from .meta.json sidecars.
    // The parent-file correlation (tool_use → toolUseResult) only registers
    // invocations whose toolUseResult is an object with agentId.  When the
    // result is a non-object string (e.g. "User rejected tool use" or a tool
    // error), the subagent may still have run and written a JSONL + meta file.
    // Reading the meta fills in those gaps so the usage records in the subagent
    // JSONL get attributed to the correct type instead of falling through to
    // "unknown".
    supplement_invocations_from_meta(
        &ds.session_id,
        &ds.subagents,
        &mut acc.agent_invocations,
        warnings,
    );
    warnings.extend(acc.warnings);
    Ok(ParsedSession {
        ds: ds.clone(),
        usage_records: acc.usage_records,
        agent_settings: acc.agent_settings,
        agent_invocations: acc.agent_invocations,
    })
}

/// Read each subagent's `.meta.json` sidecar and append an `AgentInvocation`
/// for any `agentId` not already registered from parent-file correlation.
///
/// The meta file schema is `{"agentType": "...", "description": "..."}`.
/// Missing fields fall back to `"unknown"` / empty string.  A file that
/// cannot be read or parsed emits an `IoWarning` and is skipped — the
/// corresponding usage records will still fall through to "unknown", which
/// is better than crashing.
fn supplement_invocations_from_meta(
    session_id: &str,
    subagents: &[SubagentEntry],
    invocations: &mut Vec<AgentInvocation>,
    warnings: &mut Vec<Warning>,
) {
    // Build the set of agent_ids already registered so we only supplement gaps.
    // Collect owned strings to avoid holding a borrow on `invocations`.
    let registered: HashSet<String> = invocations.iter().map(|inv| inv.agent_id.clone()).collect();

    for entry in subagents {
        let meta_path = match &entry.meta_path {
            Some(p) => p,
            None => continue,
        };

        // Extract agent_id from the JSONL filename: "agent-{id}.jsonl"
        let agent_id = match extract_agent_id_from_path(&entry.jsonl_path) {
            Some(id) => id,
            None => continue,
        };

        if registered.contains(&agent_id) {
            continue;
        }

        match read_meta_json(meta_path) {
            Ok((agent_type, description)) => {
                invocations.push(AgentInvocation {
                    session_id: session_id.to_string(),
                    agent_id,
                    subagent_type: agent_type,
                    description,
                });
            }
            Err(e) => {
                warnings.push(crate::warnings::Warning {
                    kind: crate::warnings::WarningKind::IoWarning,
                    message: format!(
                        "could not read meta file '{}': {e}; subagent usage will be \
                         classified as 'unknown'",
                        meta_path.display()
                    ),
                    context: serde_json::json!({
                        "session_id": session_id,
                        "agent_id": agent_id,
                        "path": meta_path.display().to_string(),
                    }),
                });
            }
        }
    }
}

/// Extract the `agentId` from a path like `.../agent-{id}.jsonl`.
fn extract_agent_id_from_path(path: &Path) -> Option<String> {
    let stem = path.file_stem()?.to_str()?;
    stem.strip_prefix("agent-").map(|s| s.to_string())
}

/// Read and parse a `.meta.json` sidecar.  Returns `(agent_type, description)`.
fn read_meta_json(path: &Path) -> std::result::Result<(String, String), std::io::Error> {
    let content = std::fs::read_to_string(path)?;
    let v: serde_json::Value = serde_json::from_str(&content)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    let agent_type = v
        .get("agentType")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_string();
    let description = v
        .get("description")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    Ok((agent_type, description))
}

/// Run a complete analysis and return the `Report`.
///
/// No output is written here — the caller chooses a formatter.
pub fn run(opts: RunOptions) -> Result<Report> {
    let mut warnings: Vec<Warning> = vec![];

    let roots = discover_roots(&opts.config)?;
    let all_sessions = discover_sessions(&roots)?;

    let selected = select_sessions(&all_sessions, &opts.scope, &mut warnings)?;

    let parsed: Vec<ParsedSession> = selected
        .iter()
        .map(|ds| parse_session(ds, &mut warnings))
        .collect::<Result<Vec<_>>>()?;

    let all_agent_settings: Vec<AgentSettingRecord> = parsed
        .iter()
        .flat_map(|p| p.agent_settings.iter().cloned())
        .collect();
    let all_agent_invocations: Vec<AgentInvocation> = parsed
        .iter()
        .flat_map(|p| p.agent_invocations.iter().cloned())
        .collect();

    let parent_types = ParentTypeMap::build(&all_agent_settings);
    let agent_map = AgentTypeMap::build(&all_agent_invocations, &mut warnings);

    let window = match &opts.scope {
        Scope::Window(w) => Some(w.clone()),
        _ => None,
    };

    let generated_at = (opts.clock)();
    let fingerprint = opts.config.prices.fingerprint();

    let report = match opts.report_type {
        ReportType::Session => {
            let mut sessions: Vec<_> = parsed
                .iter()
                .map(|p| {
                    build_session_report(
                        &p.ds,
                        &p.usage_records,
                        &parent_types,
                        &agent_map,
                        &opts.config.prices,
                        opts.include_invocations,
                        window.as_ref(),
                        &mut warnings,
                    )
                })
                .collect();

            // When a window filter is active, drop sessions that had no
            // records inside the window. The session_total row carries
            // entry_count == 0 iff nothing passed the filter.
            if window.is_some() {
                sessions.retain(|sr| {
                    sr.rows.iter().any(|r| {
                        r.role == crate::report::RowRole::SessionTotal && r.entry_count > 0
                    })
                });
            }

            Report {
                schema_version: 1,
                report_type: ReportType::Session,
                generated_at,
                window: window.unwrap_or(Window {
                    from: None,
                    to: None,
                }),
                price_table_fingerprint: fingerprint,
                sessions,
                aggregate_rows: vec![],
                warnings,
            }
        }
        ReportType::Aggregate => {
            let session_data: Vec<(DiscoveredSession, Vec<UsageRecord>)> = parsed
                .into_iter()
                .map(|p| (p.ds, p.usage_records))
                .collect();

            let agg_rows = build_aggregate_report(
                &session_data,
                &parent_types,
                &agent_map,
                &opts.config.prices,
                window.as_ref(),
                &mut warnings,
            );

            Report {
                schema_version: 1,
                report_type: ReportType::Aggregate,
                generated_at,
                window: window.unwrap_or(Window {
                    from: None,
                    to: None,
                }),
                price_table_fingerprint: fingerprint,
                sessions: vec![],
                aggregate_rows: agg_rows,
                warnings,
            }
        }
    };

    Ok(report)
}
