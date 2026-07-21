use std::path::{Path, PathBuf};

use crate::config::Config;
use crate::error::{Error, Result};

/// A subagent file pair: the usage JSONL and its optional sidecar metadata.
///
/// The `.meta.json` file is written by Claude Code alongside every subagent
/// JSONL and records `agentType` + `description`. It is the authoritative
/// source of subagent type when the parent file's `toolUseResult` for that
/// invocation is a non-object (e.g. `"User rejected tool use"` or an error
/// string) — those cases never produce an `AgentInvocation` via the normal
/// parent-file correlation path.
#[derive(Debug, Clone)]
pub struct SubagentEntry {
    pub jsonl_path: PathBuf,
    /// Path to the `.meta.json` sidecar, if it exists alongside the JSONL.
    pub meta_path: Option<PathBuf>,
}

/// A discovered session with its associated files.
#[derive(Debug, Clone)]
pub struct DiscoveredSession {
    /// The `{projectName}` directory name (parent of the main session file).
    pub project: String,
    pub session_id: String,
    pub main_path: PathBuf,
    pub subagent_paths: Vec<PathBuf>,
    /// Subagent entries with optional meta sidecars.
    pub subagents: Vec<SubagentEntry>,
}

/// Resolve the list of project roots from config and environment.
///
/// If `cfg.project_roots` is non-empty, it is used as-is (config wins).
/// Otherwise: union of `CLAUDE_CONFIG_DIR` split on `,` plus the two
/// standard default locations (`~/.config/claude/projects` and
/// `~/.claude/projects`). Explicitly-listed roots that do not exist are
/// a hard error; default fallback roots that do not exist are silently
/// skipped.
pub fn discover_roots(cfg: &Config) -> Result<Vec<PathBuf>> {
    if !cfg.project_roots.is_empty() {
        // User explicitly specified roots: all must exist.
        for root in &cfg.project_roots {
            if !root.exists() {
                return Err(Error::MissingRoot(root.clone()));
            }
        }
        return Ok(cfg.project_roots.clone());
    }

    let mut explicit: Vec<PathBuf> = vec![];
    let mut fallback: Vec<PathBuf> = vec![];

    // CLAUDE_CONFIG_DIR (comma-separated)
    if let Ok(dirs) = std::env::var("CLAUDE_CONFIG_DIR") {
        for dir in dirs.split(',') {
            let dir = dir.trim();
            if !dir.is_empty() {
                // Add projects/ subdirectory inside each config dir
                let projects = PathBuf::from(dir).join("projects");
                explicit.push(projects);
            }
        }
    }

    // Default locations
    if let Some(home) = home_dir() {
        fallback.push(home.join(".config/claude/projects"));
        fallback.push(home.join(".claude/projects"));
    }

    // Explicit ones must exist
    for root in &explicit {
        if !root.exists() {
            return Err(Error::MissingRoot(root.clone()));
        }
    }

    // Fallbacks: silently skip if missing
    let mut result = explicit;
    for root in fallback {
        if root.exists() {
            result.push(root);
        }
    }

    Ok(result)
}

/// Walk each root's `projects/*/` one level deep and collect session files.
///
/// Returns sessions sorted by `(project, session_id)` for determinism.
/// Subagent directories without a matching main session `.jsonl` are silently
/// skipped — they are a normal artifact when a session is interrupted before
/// the parent file is written.
pub fn discover_sessions(roots: &[PathBuf]) -> Result<Vec<DiscoveredSession>> {
    // Build a map from session_id → DiscoveredSession, then attach subagent
    // paths only to sessions that have a main file.
    let mut sessions: std::collections::BTreeMap<(String, String), DiscoveredSession> =
        std::collections::BTreeMap::new();

    for root in roots {
        // root is already the projects/ dir; entries are project dirs
        let entries = read_dir_sorted(root)?;
        for project_entry in entries {
            let project_path = project_entry;
            if !project_path.is_dir() {
                continue;
            }
            let project_name = project_path
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .into_owned();

            // Find main session files (*.jsonl directly inside project dir)
            let project_entries = read_dir_sorted(&project_path)?;
            for entry in &project_entries {
                if entry.is_file() && entry.extension().and_then(|e| e.to_str()) == Some("jsonl") {
                    let session_id = entry
                        .file_stem()
                        .unwrap_or_default()
                        .to_string_lossy()
                        .into_owned();
                    let key = (project_name.clone(), session_id.clone());
                    sessions.entry(key).or_insert_with(|| DiscoveredSession {
                        project: project_name.clone(),
                        session_id: session_id.clone(),
                        main_path: entry.clone(),
                        subagent_paths: vec![],
                        subagents: vec![],
                    });
                }
            }

            // Find subagent files: {sessionId}/subagents/agent-*.jsonl
            for entry in &project_entries {
                if !entry.is_dir() {
                    continue;
                }
                let session_id = entry
                    .file_name()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .into_owned();
                let subagents_dir = entry.join("subagents");
                if !subagents_dir.is_dir() {
                    continue;
                }
                let sub_entries = read_dir_sorted(&subagents_dir)?;
                for sub in sub_entries {
                    let fname = sub
                        .file_name()
                        .unwrap_or_default()
                        .to_string_lossy()
                        .into_owned();
                    // Exclude compaction continuation files (agent-acompact-*.jsonl).
                    // These files replay prior invocations with the same agentId as the
                    // originals, which would cause duplicate-agentId warnings and
                    // double-counting. The originals are already present.
                    if fname.starts_with("agent-")
                        && !fname.starts_with("agent-acompact-")
                        && sub.extension().and_then(|e| e.to_str()) == Some("jsonl")
                    {
                        let key = (project_name.clone(), session_id.clone());
                        // Only add subagent files when a main session already
                        // exists. Orphan subagent directories (no matching
                        // .jsonl) are silently skipped — they are a normal
                        // artifact when a session is interrupted before the
                        // parent file is written.
                        if let Some(ds) = sessions.get_mut(&key) {
                            // Look for the sidecar .meta.json next to the .jsonl.
                            // Claude Code writes this file for every subagent
                            // invocation; it carries agentType + description and
                            // is the fallback when parent-file correlation fails
                            // (e.g. rejected invocations).
                            let meta_path = sub.with_extension("meta.json");
                            let meta_path = if meta_path.exists() {
                                Some(meta_path)
                            } else {
                                None
                            };
                            ds.subagent_paths.push(sub.clone());
                            ds.subagents.push(SubagentEntry {
                                jsonl_path: sub,
                                meta_path,
                            });
                        }
                    }
                }
            }
        }
    }

    Ok(sessions.into_values().collect())
}

/// Read directory entries as sorted `PathBuf` list. Returns empty vec if
/// directory doesn't exist (caller checked existence before calling).
fn read_dir_sorted(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut entries = vec![];
    let rd = std::fs::read_dir(dir).map_err(|e| Error::Io {
        path: dir.to_path_buf(),
        source: e,
    })?;
    for entry in rd {
        let entry = entry.map_err(|e| Error::Io {
            path: dir.to_path_buf(),
            source: e,
        })?;
        entries.push(entry.path());
    }
    entries.sort();
    Ok(entries)
}

fn home_dir() -> Option<PathBuf> {
    std::env::var("HOME").ok().map(PathBuf::from)
}
