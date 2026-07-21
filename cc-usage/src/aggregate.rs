use std::collections::BTreeMap;
use std::num::NonZeroU32;

use chrono::{DateTime, Utc};

use crate::attribution::{AgentTypeMap, ParentTypeMap};
use crate::discovery::DiscoveredSession;
use crate::parse::{FileRole, UsageRecord};
use crate::pricing::PriceTable;
use crate::report::{Row, RowRole, SessionReport, Window};
use crate::tokens::TokenCounts;
use crate::warnings::Warning;

/// The three mutually-exclusive scope modes.
#[derive(Debug, Clone)]
pub enum Scope {
    /// Named session IDs.
    Explicit(Vec<String>),
    /// Time-window filter applied per-record.
    Window(Window),
    /// Last N sessions by most-recent usage timestamp.
    LastN(NonZeroU32),
}

impl Default for Scope {
    fn default() -> Self {
        // Default: last 1
        Scope::LastN(NonZeroU32::new(1).unwrap())
    }
}

/// A lightweight index mapping session_id → latest usage timestamp.
pub struct SessionRecencyIndex {
    inner: BTreeMap<String, DateTime<Utc>>,
}

impl SessionRecencyIndex {
    /// Build the index by scanning timestamps from the given sessions.
    ///
    /// Parse failures on a session file are hard errors: a corrupt file would
    /// otherwise silently alter which sessions `--last N` selects. mtime-read
    /// failures on otherwise-empty sessions are not fatal — a warning is
    /// emitted and the session is left out of the recency map (so it sorts
    /// last and is unlikely to be selected).
    pub fn build(
        sessions: &[DiscoveredSession],
        warnings: &mut Vec<Warning>,
    ) -> crate::error::Result<Self> {
        let mut inner = BTreeMap::new();
        for ds in sessions {
            if ds.main_path.as_os_str().is_empty() {
                // No main file, try using mtime from subagent paths
                let mtime = ds
                    .subagent_paths
                    .iter()
                    .filter_map(|p| {
                        std::fs::metadata(p)
                            .ok()
                            .and_then(|m| m.modified().ok().map(DateTime::<Utc>::from))
                    })
                    .max();
                match mtime {
                    Some(t) => {
                        inner
                            .entry(ds.session_id.clone())
                            .and_modify(|e: &mut DateTime<Utc>| {
                                if t > *e {
                                    *e = t;
                                }
                            })
                            .or_insert(t);
                    }
                    None => {
                        warnings.push(Warning {
                            kind: crate::warnings::WarningKind::IoWarning,
                            message: format!(
                                "could not read mtime for any subagent file of session '{}'; \
                                 session will be missing from --last N ranking",
                                ds.session_id
                            ),
                            context: serde_json::json!({
                                "session_id": ds.session_id,
                            }),
                        });
                    }
                }
                continue;
            }

            // Parse the main session file once for timestamps. Discard parse
            // warnings: the same file is re-parsed by the main run path,
            // which will surface them once. Hard errors (file open / I/O)
            // still propagate via `?`.
            let mut acc = crate::parse::SessionAccumulator::default();
            crate::parse::parse_main_session(&ds.main_path, &mut acc)?;

            let latest = acc.usage_records.iter().map(|r| r.timestamp).max();
            let ts = if let Some(t) = latest {
                t
            } else {
                // No usage records: fall back to file mtime.
                match std::fs::metadata(&ds.main_path)
                    .and_then(|m| m.modified())
                    .map(DateTime::<Utc>::from)
                {
                    Ok(t) => t,
                    Err(e) => {
                        warnings.push(Warning {
                            kind: crate::warnings::WarningKind::IoWarning,
                            message: format!(
                                "could not read mtime for empty session file '{}': {e}; \
                                 session will be missing from --last N ranking",
                                ds.main_path.display()
                            ),
                            context: serde_json::json!({
                                "session_id": ds.session_id,
                                "path": ds.main_path.display().to_string(),
                            }),
                        });
                        continue;
                    }
                }
            };

            inner
                .entry(ds.session_id.clone())
                .and_modify(|e: &mut DateTime<Utc>| {
                    if ts > *e {
                        *e = ts;
                    }
                })
                .or_insert(ts);
        }
        Ok(Self { inner })
    }

    pub fn latest(&self, session_id: &str) -> Option<DateTime<Utc>> {
        self.inner.get(session_id).copied()
    }
}

/// Select which sessions to include based on the scope.
pub fn select_sessions(
    all: &[DiscoveredSession],
    scope: &Scope,
    warnings: &mut Vec<Warning>,
) -> crate::error::Result<Vec<DiscoveredSession>> {
    match scope {
        Scope::Explicit(ids) => {
            let mut result = vec![];
            for id in ids {
                let found = all.iter().find(|ds| &ds.session_id == id);
                match found {
                    Some(ds) => result.push(ds.clone()),
                    None => return Err(crate::error::Error::UnknownSession(id.clone())),
                }
            }
            Ok(result)
        }
        Scope::Window(_) => {
            // Return all sessions; record-level filtering happens inside build_*_report
            Ok(all.to_vec())
        }
        Scope::LastN(n) => {
            let idx = SessionRecencyIndex::build(all, warnings)?;
            // Sort by recency descending, then by session_id for stability
            let mut with_recency: Vec<_> = all
                .iter()
                .map(|ds| {
                    let ts = idx.latest(&ds.session_id);
                    (ts, ds)
                })
                .collect();
            with_recency.sort_by(|(ta, a), (tb, b)| {
                tb.cmp(ta) // descending timestamp
                    .then_with(|| a.session_id.cmp(&b.session_id))
            });
            let n = n.get() as usize;
            Ok(with_recency
                .into_iter()
                .take(n)
                .map(|(_, ds)| ds.clone())
                .collect())
        }
    }
}

/// Accumulator for a single (agent_type, model) bucket.
#[derive(Debug, Default)]
struct RowAcc {
    tokens: TokenCounts,
    entry_count: u64,
    first_timestamp: Option<DateTime<Utc>>,
}

impl RowAcc {
    fn add(&mut self, rec: &UsageRecord) {
        self.tokens += rec.tokens;
        self.entry_count += 1;
        // records may arrive out of timestamp order (parent/sidecar interleave for
        // parent/subagent buckets; multi-sidecar interleave for invocation buckets);
        // min() is the safe definition
        self.first_timestamp = Some(match self.first_timestamp {
            None => rec.timestamp,
            Some(existing) => existing.min(rec.timestamp),
        });
    }

    fn finalize(
        &self,
        role: RowRole,
        agent_type: Option<String>,
        agent_id: Option<String>,
        model: Option<String>,
        prices: &PriceTable,
    ) -> Row {
        let cost_usd = model
            .as_deref()
            .and_then(|m| prices.lookup(m))
            .map(|p| p.cost(&self.tokens));
        Row {
            role,
            agent_type,
            agent_id,
            model,
            input_tokens: self.tokens.input,
            cache_write_5m_tokens: self.tokens.cache_write_5m,
            cache_write_1h_tokens: self.tokens.cache_write_1h,
            cache_read_tokens: self.tokens.cache_read,
            output_tokens: self.tokens.output,
            total_tokens: self.tokens.total(),
            cost_usd,
            entry_count: self.entry_count,
            start_time: self.first_timestamp,
        }
    }

    /// Like `finalize`, but always sets `start_time: None`. Use for aggregate
    /// rows where `start_time` is semantically absent and must not be exposed
    /// to callers (e.g. formatters) that would misinterpret a populated
    /// `first_timestamp` as a meaningful session start.
    fn finalize_aggregate(
        &self,
        role: RowRole,
        agent_type: Option<String>,
        agent_id: Option<String>,
        model: Option<String>,
        prices: &PriceTable,
    ) -> Row {
        let mut row = self.finalize(role, agent_type, agent_id, model, prices);
        row.start_time = None;
        row
    }
}

/// Classify a usage record as parent or subagent.
fn is_parent(rec: &UsageRecord) -> bool {
    rec.file_role == FileRole::MainSession && !rec.is_sidechain
}

fn check_unknown_model(
    model: &Option<String>,
    prices: &PriceTable,
    warned_models: &mut std::collections::HashSet<Option<String>>,
    warnings: &mut Vec<Warning>,
) {
    if !warned_models.contains(model)
        && let Some(m) = model
        && prices.lookup(m).is_none()
    {
        warnings.push(Warning::unknown_model(m));
        warned_models.insert(model.clone());
    }
}

/// Build a per-session report from parsed records.
///
/// The returned `SessionReport` always contains a `RowRole::SessionTotal`
/// row. Its `entry_count` is the sum of parent + rolled-up subagent entry
/// counts **after** any `window` filtering. Callers rely on this invariant:
/// when `window` is `Some(_)`, `session_total.entry_count == 0` is the
/// signal that no records fell inside the window, and the session can be
/// suppressed from the output. Do not change the meaning of `entry_count`
/// or skip emitting the `SessionTotal` row without updating `lib.rs::run`.
#[allow(clippy::too_many_arguments)]
pub fn build_session_report(
    ds: &DiscoveredSession,
    usage_records: &[UsageRecord],
    parent_types: &ParentTypeMap,
    agent_map: &AgentTypeMap,
    prices: &PriceTable,
    include_invocations: bool,
    window: Option<&Window>,
    warnings: &mut Vec<Warning>,
) -> SessionReport {
    // Bucket keys
    let mut parent_buckets: BTreeMap<(String, Option<String>), RowAcc> = BTreeMap::new();
    let mut subagent_buckets: BTreeMap<(String, Option<String>), RowAcc> = BTreeMap::new();
    let mut invocation_buckets: BTreeMap<(String, String, Option<String>), RowAcc> =
        BTreeMap::new();

    // Track which orphan agent ids we've already warned about
    let mut warned_orphans: std::collections::HashSet<String> = std::collections::HashSet::new();
    // Track which unknown models we've warned about
    let mut warned_models: std::collections::HashSet<Option<String>> =
        std::collections::HashSet::new();

    let parent_type = parent_types
        .get(&ds.session_id)
        .unwrap_or("untyped")
        .to_string();

    for rec in usage_records {
        // Window filtering
        if let Some(w) = window
            && !in_window(rec.timestamp, w)
        {
            continue;
        }

        check_unknown_model(&rec.model, prices, &mut warned_models, warnings);

        if is_parent(rec) {
            let key = (parent_type.clone(), rec.model.clone());
            parent_buckets.entry(key).or_default().add(rec);
        } else {
            // Subagent: look up type
            let agent_id = rec.agent_id.as_deref().unwrap_or("");
            let subagent_type = match agent_map.get(&ds.session_id, agent_id) {
                Some(info) => info.subagent_type.clone(),
                None => {
                    if !agent_id.is_empty() && !warned_orphans.contains(agent_id) {
                        warnings.push(Warning::orphan_agent_id(&ds.session_id, agent_id));
                        warned_orphans.insert(agent_id.to_string());
                    }
                    "unknown".to_string()
                }
            };

            let key = (subagent_type.clone(), rec.model.clone());
            subagent_buckets.entry(key).or_default().add(rec);

            if include_invocations {
                let inv_key = (subagent_type, agent_id.to_string(), rec.model.clone());
                invocation_buckets.entry(inv_key).or_default().add(rec);
            }
        }
    }

    let mut rows: Vec<Row> = vec![];

    // Parent rows
    for ((agent_type, model), acc) in &parent_buckets {
        rows.push(acc.finalize(
            RowRole::Parent,
            Some(agent_type.clone()),
            None,
            model.clone(),
            prices,
        ));
    }

    // Subagent rolled-up rows
    for ((subagent_type, model), acc) in &subagent_buckets {
        rows.push(acc.finalize(
            RowRole::Subagent,
            Some(subagent_type.clone()),
            None,
            model.clone(),
            prices,
        ));
    }

    // Invocation rows — sorted by first-seen timestamp ascending, with
    // the existing lex key as tie-breaker for determinism.
    if include_invocations {
        let mut inv_pairs: Vec<(DateTime<Utc>, Row)> = invocation_buckets
            .iter()
            .map(|((subagent_type, agent_id, model), acc)| {
                let first_ts = acc
                    .first_timestamp
                    .expect("invocation row missing first_timestamp — invariant violation");
                let row = acc.finalize(
                    RowRole::SubagentInvocation,
                    Some(subagent_type.clone()),
                    Some(agent_id.clone()),
                    model.clone(),
                    prices,
                );
                (first_ts, row)
            })
            .collect();
        // Sort by (first_timestamp, agent_type, agent_id, model) for full determinism.
        // agent_type/agent_id/model are extracted explicitly so the tie-break does not
        // depend on BTreeMap iteration order being preserved through any future refactor.
        inv_pairs.sort_by(|(ts_a, row_a), (ts_b, row_b)| {
            ts_a.cmp(ts_b)
                .then_with(|| row_a.agent_type.cmp(&row_b.agent_type))
                .then_with(|| row_a.agent_id.cmp(&row_b.agent_id))
                .then_with(|| row_a.model.cmp(&row_b.model))
        });
        rows.extend(inv_pairs.into_iter().map(|(_, r)| r));
    }

    // Session total (sum of parent + rolled-up subagent, NOT invocations)
    let total_tokens = parent_buckets
        .values()
        .chain(subagent_buckets.values())
        .fold(TokenCounts::default(), |acc, b| acc + b.tokens);
    let total_entries: u64 = parent_buckets
        .values()
        .chain(subagent_buckets.values())
        .map(|b| b.entry_count)
        .sum();
    // Cost: sum individual row costs
    let total_cost: Option<f64> = {
        let mut sum = 0.0f64;
        let mut any_priced = false;
        for row in rows
            .iter()
            .filter(|r| matches!(r.role, RowRole::Parent | RowRole::Subagent))
        {
            if let Some(c) = row.cost_usd {
                sum += c;
                any_priced = true;
            }
        }
        if any_priced { Some(sum) } else { None }
    };

    // start_time for session total = min first_timestamp across parent + subagent buckets
    let session_start_time: Option<DateTime<Utc>> = parent_buckets
        .values()
        .chain(subagent_buckets.values())
        .filter_map(|b| b.first_timestamp)
        .min();

    rows.push(Row {
        role: RowRole::SessionTotal,
        agent_type: None,
        agent_id: None,
        model: None,
        input_tokens: total_tokens.input,
        cache_write_5m_tokens: total_tokens.cache_write_5m,
        cache_write_1h_tokens: total_tokens.cache_write_1h,
        cache_read_tokens: total_tokens.cache_read,
        output_tokens: total_tokens.output,
        total_tokens: total_tokens.total(),
        cost_usd: total_cost,
        entry_count: total_entries,
        start_time: session_start_time,
    });

    SessionReport {
        session_id: ds.session_id.clone(),
        project: ds.project.clone(),
        rows,
    }
}

/// Build aggregate rows across multiple sessions.
pub fn build_aggregate_report(
    sessions: &[(DiscoveredSession, Vec<UsageRecord>)],
    parent_type_map: &ParentTypeMap,
    agent_map: &AgentTypeMap,
    prices: &PriceTable,
    window: Option<&Window>,
    warnings: &mut Vec<Warning>,
) -> Vec<Row> {
    let mut parent_buckets: BTreeMap<(String, Option<String>), RowAcc> = BTreeMap::new();
    let mut subagent_buckets: BTreeMap<(String, Option<String>), RowAcc> = BTreeMap::new();

    let mut warned_orphans: std::collections::HashSet<(String, String)> =
        std::collections::HashSet::new();
    let mut warned_models: std::collections::HashSet<Option<String>> =
        std::collections::HashSet::new();

    for (ds, usage_records) in sessions {
        let parent_type = parent_type_map
            .get(&ds.session_id)
            .unwrap_or("untyped")
            .to_string();

        for rec in usage_records {
            if let Some(w) = window
                && !in_window(rec.timestamp, w)
            {
                continue;
            }

            check_unknown_model(&rec.model, prices, &mut warned_models, warnings);

            if is_parent(rec) {
                let key = (parent_type.clone(), rec.model.clone());
                parent_buckets.entry(key).or_default().add(rec);
            } else {
                let agent_id = rec.agent_id.as_deref().unwrap_or("");
                let subagent_type = match agent_map.get(&ds.session_id, agent_id) {
                    Some(info) => info.subagent_type.clone(),
                    None => {
                        let key = (ds.session_id.clone(), agent_id.to_string());
                        if !agent_id.is_empty() && !warned_orphans.contains(&key) {
                            warnings.push(Warning::orphan_agent_id(&ds.session_id, agent_id));
                            warned_orphans.insert(key);
                        }
                        "unknown".to_string()
                    }
                };
                let bucket_key = (subagent_type, rec.model.clone());
                subagent_buckets.entry(bucket_key).or_default().add(rec);
            }
        }
    }

    let mut rows: Vec<Row> = vec![];

    for ((agent_type, model), acc) in &parent_buckets {
        rows.push(acc.finalize_aggregate(
            RowRole::Parent,
            Some(agent_type.clone()),
            None,
            model.clone(),
            prices,
        ));
    }

    for ((subagent_type, model), acc) in &subagent_buckets {
        rows.push(acc.finalize_aggregate(
            RowRole::Subagent,
            Some(subagent_type.clone()),
            None,
            model.clone(),
            prices,
        ));
    }

    rows
}

fn in_window(ts: DateTime<Utc>, window: &Window) -> bool {
    if let Some(from) = window.from
        && ts < from
    {
        return false;
    }
    if let Some(to) = window.to
        && ts >= to
    {
        return false;
    }
    true
}
