use std::collections::HashMap;
use std::io::{BufRead, BufReader};
use std::path::Path;

use chrono::{DateTime, Utc};

use crate::attribution::{AgentInvocation, AgentSettingRecord};
use crate::error::{Error, Result};
use crate::schema::RawRecord;
use crate::tokens::{self, TokenCounts};
use crate::warnings::Warning;

// JSONL protocol tokens. Centralized so a typo can't silently drop records.
const RECORD_TYPE_AGENT_SETTING: &str = "agent-setting";
const RECORD_TYPE_ASSISTANT: &str = "assistant";
const RECORD_TYPE_USER: &str = "user";
const BLOCK_TYPE_TOOL_USE: &str = "tool_use";
const BLOCK_TYPE_TOOL_RESULT: &str = "tool_result";
const TOOL_NAME_AGENT: &str = "Agent";

/// Pending agent invocation state collected from an `assistant` tool_use block.
/// Stored until the corresponding `user` record arrives with toolUseResult.
struct PendingInvocation {
    session_id: String,
    subagent_type: String,
    description: String,
    /// File line number where this Agent block was registered — used to attribute
    /// "unmatched pending" warnings to the right source line.
    line_no: usize,
}

/// Classification of a file role.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileRole {
    MainSession,
    SubagentSidecar,
}

/// A normalized usage-bearing record.
#[derive(Debug, Clone)]
pub struct UsageRecord {
    pub file_role: FileRole,
    pub session_id: String,
    pub agent_id: Option<String>,
    pub timestamp: DateTime<Utc>,
    pub model: Option<String>,
    pub tokens: TokenCounts,
    /// Whether this record was from a sidechain (isSidechain: true).
    pub is_sidechain: bool,
}

/// Accumulated state per session during parsing.
#[derive(Debug, Default)]
pub struct SessionAccumulator {
    pub usage_records: Vec<UsageRecord>,
    pub agent_settings: Vec<AgentSettingRecord>,
    pub agent_invocations: Vec<AgentInvocation>,
    pub warnings: Vec<Warning>,
}

/// Parse a main session JSONL file, appending into `acc`.
pub fn parse_main_session(path: &Path, acc: &mut SessionAccumulator) -> Result<()> {
    parse_file(path, FileRole::MainSession, acc)
}

/// Parse a subagent sidecar JSONL file, appending into `acc`.
pub fn parse_subagent_file(path: &Path, acc: &mut SessionAccumulator) -> Result<()> {
    parse_file(path, FileRole::SubagentSidecar, acc)
}

fn parse_file(path: &Path, role: FileRole, acc: &mut SessionAccumulator) -> Result<()> {
    let file = std::fs::File::open(path).map_err(|e| Error::Io {
        path: path.to_path_buf(),
        source: e,
    })?;
    let reader = BufReader::new(file);
    let path_str = path.to_string_lossy().into_owned();

    // Maps tool_use block id → PendingInvocation, filled from assistant records,
    // consumed when the subsequent user record carries toolUseResult.
    let mut pending: HashMap<String, PendingInvocation> = HashMap::new();

    for (line_idx, line_result) in reader.lines().enumerate() {
        let line_no = line_idx + 1;
        let line = line_result.map_err(|e| Error::Io {
            path: path.to_path_buf(),
            source: e,
        })?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let raw: RawRecord = match serde_json::from_str(trimmed) {
            Ok(r) => r,
            Err(e) => {
                acc.warnings
                    .push(Warning::malformed_line(&path_str, line_no, &e.to_string()));
                continue;
            }
        };

        match raw.record_type.as_deref() {
            Some(RECORD_TYPE_AGENT_SETTING) => match (&raw.agent_setting, &raw.session_id) {
                (Some(setting), Some(session_id)) => {
                    acc.agent_settings.push(AgentSettingRecord {
                        session_id: session_id.clone(),
                        agent_setting: setting.clone(),
                    });
                }
                (None, _) => {
                    acc.warnings.push(Warning::schema_mismatch(
                        &path_str,
                        line_no,
                        "agent-setting record is missing 'agentSetting' field",
                    ));
                }
                (_, None) => {
                    acc.warnings.push(Warning::schema_mismatch(
                        &path_str,
                        line_no,
                        "agent-setting record is missing 'sessionId' field",
                    ));
                }
            },
            Some(RECORD_TYPE_ASSISTANT) => {
                // Register any Agent tool_use blocks into the pending map.
                // The toolUseResult (with agentId) arrives on the NEXT user record,
                // not on this assistant record. We record the subagent_type/description
                // from the block input so we can emit the full AgentInvocation later.
                if let Some(msg) = &raw.message
                    && let Some(content) = &msg.content
                {
                    for block in content {
                        if block.block_type.as_deref() == Some(BLOCK_TYPE_TOOL_USE)
                            && block.name.as_deref() == Some(TOOL_NAME_AGENT)
                        {
                            if let (Some(block_id), Some(session_id)) = (&block.id, &raw.session_id)
                            {
                                let input = block.input.as_ref();
                                let subagent_type = input
                                    .and_then(|v| v.get("subagent_type"))
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("unknown")
                                    .to_string();
                                let description = input
                                    .and_then(|v| v.get("description"))
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("")
                                    .to_string();
                                pending.insert(
                                    block_id.clone(),
                                    PendingInvocation {
                                        session_id: session_id.clone(),
                                        subagent_type,
                                        description,
                                        line_no,
                                    },
                                );
                            } else if block.id.is_none() {
                                acc.warnings.push(Warning::schema_mismatch(
                                    &path_str,
                                    line_no,
                                    "Agent tool_use block is missing 'id' field",
                                ));
                            } else {
                                acc.warnings.push(Warning::schema_mismatch(
                                    &path_str,
                                    line_no,
                                    "Agent tool_use record is missing 'sessionId'",
                                ));
                            }
                        }
                    }
                }

                // Extract usage
                if let Some(msg) = &raw.message
                    && let Some(usage) = &msg.usage
                {
                    // Claude Code writes synthetic assistant records (model == "<synthetic>")
                    // as placeholders for API errors, interrupted invocations, etc. They
                    // always carry zero tokens and contribute nothing to cost. Skip them
                    // silently rather than emitting a spurious UnknownModel warning.
                    if msg.model.as_deref() == Some("<synthetic>") {
                        continue;
                    }

                    // Validate: if both input and output are missing, that's a schema mismatch
                    if usage.input_tokens.is_none() && usage.output_tokens.is_none() {
                        acc.warnings.push(Warning::schema_mismatch(
                            &path_str,
                            line_no,
                            "assistant record has usage block but both input_tokens and \
                             output_tokens are missing",
                        ));
                        continue;
                    }

                    // Timestamp is required on usage-bearing records
                    let Some(timestamp) = raw.timestamp else {
                        acc.warnings.push(Warning::schema_mismatch(
                            &path_str,
                            line_no,
                            "assistant record with usage is missing timestamp",
                        ));
                        continue;
                    };

                    // session_id is required on usage-bearing records — without
                    // it we can't attribute the record to a session.
                    let Some(session_id) = raw.session_id.clone() else {
                        acc.warnings.push(Warning::schema_mismatch(
                            &path_str,
                            line_no,
                            "assistant record with usage is missing sessionId",
                        ));
                        continue;
                    };

                    let is_sidechain = raw.is_sidechain.unwrap_or(false);

                    acc.usage_records.push(UsageRecord {
                        file_role: role,
                        session_id,
                        agent_id: raw.agent_id.clone(),
                        timestamp,
                        model: msg.model.clone(),
                        tokens: tokens::compute(usage),
                        is_sidechain,
                    });
                }
            }
            Some(RECORD_TYPE_USER) => {
                // A user record may carry toolUseResult with agentId when a
                // subagent invocation completes. Correlate via tool_use_id in
                // the message.content tool_result block.
                //
                // Two structurally-different cases:
                //
                // 1. toolUseResult is an object with agentId → completed invocation.
                //    Create an AgentInvocation and remove from pending.
                //
                // 2. toolUseResult is a non-object (e.g. the string
                //    "User rejected tool use") → the Agent tool call was cancelled
                //    before the subagent ran.  We still need to consume the pending
                //    entry so we don't emit a spurious "no matching toolUseResult"
                //    warning at end-of-file.
                if let Some(tur) = &raw.tool_use_result {
                    // Resolve the tool_use_id from the content block — common to both cases.
                    let tool_use_id = raw
                        .message
                        .as_ref()
                        .and_then(|m| m.content.as_ref())
                        .and_then(|blocks| {
                            blocks
                                .iter()
                                .find(|b| {
                                    b.block_type.as_deref() == Some(BLOCK_TYPE_TOOL_RESULT)
                                        && b.tool_use_id.is_some()
                                })
                                .and_then(|b| b.tool_use_id.as_deref())
                        });

                    if let Some(tur_obj) = tur.as_object()
                        && let Some(agent_id_val) = tur_obj.get("agentId")
                        && let Some(agent_id) = agent_id_val.as_str()
                    {
                        // Case 1: completed invocation.
                        if let Some(tool_use_id) = tool_use_id
                            && let Some(pending_inv) = pending.remove(tool_use_id)
                        {
                            // agentType from toolUseResult is the ground-truth type for
                            // completed invocations; prefer it over subagent_type from block input.
                            let resolved_type = tur_obj
                                .get("agentType")
                                .and_then(|v| v.as_str())
                                .map(|s| s.to_string())
                                .unwrap_or(pending_inv.subagent_type);
                            acc.agent_invocations.push(AgentInvocation {
                                session_id: pending_inv.session_id,
                                agent_id: agent_id.to_string(),
                                subagent_type: resolved_type,
                                description: pending_inv.description,
                            });
                        }
                        // If no tool_use_id or not in pending map: silently skip.
                        // This is not a schema error — some toolUseResult objects have no
                        // corresponding content block, or may already be registered.
                    } else if !tur.is_object() {
                        // Case 2: non-object toolUseResult (e.g. rejection string).
                        // Consume the pending entry without creating an invocation.
                        if let Some(tool_use_id) = tool_use_id {
                            pending.remove(tool_use_id);
                        }
                    }
                }
            }
            _ => {
                // Ignored record types
            }
        }
    }

    // After the line loop: any entries left in `pending` are Agent tool_use
    // blocks whose corresponding user toolUseResult never arrived (truncated
    // file, mid-invocation interrupt). Emit a schema_mismatch warning each so
    // the user gets a diagnostic signal — the corresponding subagent records
    // (if any) will fall through to OrphanAgentId attribution downstream.
    for (block_id, pending_inv) in pending {
        acc.warnings.push(Warning::schema_mismatch(
            &path_str,
            pending_inv.line_no,
            &format!(
                "Agent tool_use block '{}' (subagent_type='{}') had no matching \
                 user toolUseResult before end of file; invocation dropped",
                block_id, pending_inv.subagent_type
            ),
        ));
    }

    Ok(())
}
