use std::collections::BTreeMap;

use crate::warnings::{Warning, WarningKind};

/// Records collected from `type: "agent-setting"` lines in main session files.
#[derive(Debug, Clone)]
pub struct AgentSettingRecord {
    pub session_id: String,
    pub agent_setting: String,
}

/// A recorded Agent tool invocation from the main session file.
#[derive(Debug, Clone)]
pub struct AgentInvocation {
    pub session_id: String,
    pub agent_id: String,
    pub subagent_type: String,
    pub description: String,
}

/// Maps session_id → parent agent type (last agent-setting record wins).
#[derive(Debug, Default)]
pub struct ParentTypeMap {
    inner: BTreeMap<String, String>,
}

impl ParentTypeMap {
    pub fn build(records: &[AgentSettingRecord]) -> Self {
        let mut inner = BTreeMap::new();
        // Last record per session wins (records should be in file order)
        for rec in records {
            inner.insert(rec.session_id.clone(), rec.agent_setting.clone());
        }
        Self { inner }
    }

    pub fn get(&self, session_id: &str) -> Option<&str> {
        self.inner.get(session_id).map(|s| s.as_str())
    }
}

/// Info about a subagent invocation.
#[derive(Debug, Clone)]
pub struct AgentInfo {
    pub subagent_type: String,
    pub description: String,
}

/// Maps session_id → agent_id → AgentInfo. Nested map structure avoids
/// the two `String` allocations per lookup that `BTreeMap<(String, String), _>`
/// would require (`get` is called once per subagent usage record).
#[derive(Debug, Default)]
pub struct AgentTypeMap {
    inner: BTreeMap<String, BTreeMap<String, AgentInfo>>,
}

impl AgentTypeMap {
    pub fn build(invocations: &[AgentInvocation], warnings: &mut Vec<Warning>) -> Self {
        let mut inner: BTreeMap<String, BTreeMap<String, AgentInfo>> = BTreeMap::new();
        for inv in invocations {
            let session_map = inner.entry(inv.session_id.clone()).or_default();
            if session_map.contains_key(&inv.agent_id) {
                warnings.push(Warning {
                    kind: WarningKind::OrphanAgentId,
                    message: format!(
                        "duplicate agentId '{}' in session '{}'; keeping first",
                        inv.agent_id, inv.session_id
                    ),
                    context: serde_json::json!({
                        "session_id": inv.session_id,
                        "agent_id": inv.agent_id,
                    }),
                });
                // Keep the first
                continue;
            }
            session_map.insert(
                inv.agent_id.clone(),
                AgentInfo {
                    subagent_type: inv.subagent_type.clone(),
                    description: inv.description.clone(),
                },
            );
        }
        Self { inner }
    }

    pub fn get(&self, session_id: &str, agent_id: &str) -> Option<&AgentInfo> {
        self.inner.get(session_id)?.get(agent_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn last_agent_setting_wins() {
        let records = vec![
            AgentSettingRecord {
                session_id: "s1".into(),
                agent_setting: "first".into(),
            },
            AgentSettingRecord {
                session_id: "s1".into(),
                agent_setting: "second".into(),
            },
            AgentSettingRecord {
                session_id: "s1".into(),
                agent_setting: "third".into(),
            },
        ];
        let map = ParentTypeMap::build(&records);
        assert_eq!(map.get("s1"), Some("third"));
    }

    #[test]
    fn agent_map_collision_warns_keeps_first() {
        let invocations = vec![
            AgentInvocation {
                session_id: "s1".into(),
                agent_id: "a1".into(),
                subagent_type: "type-a".into(),
                description: "first".into(),
            },
            AgentInvocation {
                session_id: "s1".into(),
                agent_id: "a1".into(),
                subagent_type: "type-b".into(),
                description: "second".into(),
            },
        ];
        let mut warnings = vec![];
        let map = AgentTypeMap::build(&invocations, &mut warnings);
        // First wins
        assert_eq!(map.get("s1", "a1").unwrap().subagent_type, "type-a");
        // Warning emitted
        assert_eq!(warnings.len(), 1);
        assert!(matches!(warnings[0].kind, WarningKind::OrphanAgentId));
    }
}
