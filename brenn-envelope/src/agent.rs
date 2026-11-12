//! Agent status bodies: what the `system:agent-status` participant publishes
//! about running Claude Code sessions and the Claude accounts they spend.
//!
//! Three bodies on three channels:
//!
//! - [`AgentSessions`] — **state**: every conversation that has a running Claude
//!   Code process. Each snapshot subsumes the one before it.
//! - [`AgentSessionEnded`] — **fact**: one per run of a session that ended.
//! - [`AgentAccounts`] — **state**: every configured Claude profile, with the
//!   spend and refusal flags the host has observed on it.
//!
//! Every body is a JSON object carrying `"v": AGENT_STATUS_VERSION`, stamped by
//! [`encode`] and checked by [`decode`]. Instants are epoch milliseconds, the
//! unit of a guest's only clock. Absent optional fields are omitted, and unknown
//! fields are ignored on decode, so fields are added without a version move.
//!
//! Guest-safe: plain serde types, no clock, no I/O.

use std::collections::BTreeMap;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::chat::CcState;

/// Schema version stamped into every body. Moves only on a reshape a reader
/// cannot absorb.
pub const AGENT_STATUS_VERSION: u32 = 1;

/// The `sender` of every message on the three agent-status channels. A reader
/// believes only envelopes from this sender: an operator ACL can let another
/// principal publish there, and its body would claim sessions, spend and flags
/// the host never observed.
pub const AGENT_STATUS_SENDER: &str = "system:agent-status";

/// Rate-limit bucket names a subscription account can be refused in.
pub mod bucket {
    /// The rolling five-hour window.
    pub const FIVE_HOUR: &str = "five_hour";
    /// The weekly window, and any limit not otherwise named.
    pub const GENERAL: &str = "general";
    /// A model-specific limit on a Fable model.
    pub const FABLE: &str = "fable";
}

/// How an account pays for turns.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AccountKind {
    /// A Claude subscription, reached through a long-lived OAuth token.
    Subscription,
    /// An Anthropic API key, billed per turn.
    ApiKey,
}

impl AccountKind {
    /// The wire word: `subscription` or `api-key`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Subscription => "subscription",
            Self::ApiKey => "api-key",
        }
    }
}

/// One conversation while its Claude Code process is running.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentSession {
    /// The conversation id. A conversation that stops and wakes again runs
    /// again under the same id.
    pub conversation_id: i64,
    /// The agent's app slug.
    pub app: String,
    pub state: CcState,
    /// Wall clock at the start of this run.
    pub started_at_ms: u64,
    /// Claude Code's own session id, once the process has reported it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cc_session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// The profile the current process was spawned under; absent for an agent
    /// without profiles.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account: Option<String>,
    /// The kind of `account`; present exactly when `account` is.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account_kind: Option<AccountKind>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    /// Dollars spent by this run so far, as Claude Code estimates them.
    pub cost_usd: f64,
}

/// Every conversation with a running Claude Code process, ascending by
/// `conversation_id`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentSessions {
    pub at_ms: u64,
    pub sessions: Vec<AgentSession>,
}

/// One run of a session ended.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentSessionEnded {
    /// The session as it last stood.
    pub session: AgentSession,
    pub ended_at_ms: u64,
    pub duration_ms: u64,
    /// The process stopped on purpose (drain, shutdown, reload, supersede)
    /// rather than dying.
    pub ok: bool,
}

/// A refusal observed on an account: the whole account (an API key out of
/// credit) or one rate-limit bucket of a subscription.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountFlag {
    /// When the refusal was observed.
    pub at_ms: u64,
    /// The conversation id the refusal was observed in, as a decimal string.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session: Option<String>,
    /// The refusal text, cut short.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// When the refusal lifts. Always present on a bucket flag; absent on an
    /// account-wide one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub until_ms: Option<u64>,
    /// Claude Code's own name for the limit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
}

/// One configured Claude profile as an account.
///
/// A flag present here is in force: the host removes a flag from its record
/// rather than marking it cleared.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentAccount {
    /// The profile name.
    pub account: String,
    pub account_kind: AccountKind,
    /// `YYYY-MM` of the accounting month for an API key; `-` for a
    /// subscription, which has no spend.
    pub period: String,
    /// Dollars spent in `period`; 0 for a subscription.
    pub usd: f64,
    /// Turns charged in `period`; absent for a subscription.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turns: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub budget_usd: Option<f64>,
    /// The account-wide refusal in force, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exhausted: Option<AccountFlag>,
    /// Bucket refusals in force, by bucket name (see [`bucket`]).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub exhausted_buckets: BTreeMap<String, AccountFlag>,
    pub updated_at_ms: u64,
}

/// Every configured profile, ascending by `account`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentAccounts {
    pub at_ms: u64,
    pub accounts: Vec<AgentAccount>,
}

/// Why a body could not be decoded: the shared versioned codec's error.
pub type AgentDecodeError = crate::versioned::VersionedDecodeError;

/// Serialize a body, stamping `"v": AGENT_STATUS_VERSION`.
///
/// # Panics
///
/// If `body` does not serialize to a JSON object. Every body type in this
/// module does; anything else is a programming error.
pub fn encode<T: Serialize>(body: &T) -> String {
    crate::versioned::encode(AGENT_STATUS_VERSION, body)
}

/// Parse a body, requiring `v` equal to [`AGENT_STATUS_VERSION`].
pub fn decode<T: DeserializeOwned>(body: &str) -> Result<T, AgentDecodeError> {
    crate::versioned::decode(AGENT_STATUS_VERSION, body)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    fn bare_session() -> AgentSession {
        AgentSession {
            conversation_id: 42,
            app: "assistant".to_string(),
            state: CcState::Idle,
            started_at_ms: 1_700_000_000_000,
            cc_session_id: None,
            model: None,
            account: None,
            account_kind: None,
            cwd: None,
            cost_usd: 0.0,
        }
    }

    fn full_session() -> AgentSession {
        AgentSession {
            cc_session_id: Some("8f3c".to_string()),
            model: Some("claude-opus".to_string()),
            account: Some("work-key".to_string()),
            account_kind: Some(AccountKind::ApiKey),
            cwd: Some("/srv/agents/assistant".to_string()),
            state: CcState::Thinking,
            cost_usd: 1.25,
            ..bare_session()
        }
    }

    fn flag() -> AccountFlag {
        AccountFlag {
            at_ms: 1_700_000_100_000,
            session: Some("42".to_string()),
            message: Some("Credit balance is too low".to_string()),
            until_ms: None,
            kind: None,
        }
    }

    fn key_account() -> AgentAccount {
        AgentAccount {
            account: "work-key".to_string(),
            account_kind: AccountKind::ApiKey,
            period: "2026-11".to_string(),
            usd: 3.5,
            turns: Some(7),
            budget_usd: Some(50.0),
            exhausted: Some(flag()),
            exhausted_buckets: BTreeMap::new(),
            updated_at_ms: 1_700_000_100_000,
        }
    }

    fn subscription_account() -> AgentAccount {
        AgentAccount {
            account: "personal".to_string(),
            account_kind: AccountKind::Subscription,
            period: "-".to_string(),
            usd: 0.0,
            turns: None,
            budget_usd: None,
            exhausted: None,
            exhausted_buckets: BTreeMap::from([(
                bucket::FIVE_HOUR.to_string(),
                AccountFlag {
                    until_ms: Some(1_700_018_000_000),
                    kind: Some("five_hour".to_string()),
                    ..flag()
                },
            )]),
            updated_at_ms: 1_700_000_100_000,
        }
    }

    #[test]
    fn every_body_round_trips_with_its_version() {
        let sessions = AgentSessions {
            at_ms: 1_700_000_200_000,
            sessions: vec![bare_session(), full_session()],
        };
        let body = encode(&sessions);
        assert_eq!(serde_json::from_str::<Value>(&body).unwrap()["v"], json!(1));
        assert_eq!(decode::<AgentSessions>(&body), Ok(sessions));

        let ended = AgentSessionEnded {
            session: full_session(),
            ended_at_ms: 1_700_000_300_000,
            duration_ms: 300_000,
            ok: true,
        };
        assert_eq!(decode::<AgentSessionEnded>(&encode(&ended)), Ok(ended));

        let accounts = AgentAccounts {
            at_ms: 1_700_000_200_000,
            accounts: vec![subscription_account(), key_account()],
        };
        assert_eq!(decode::<AgentAccounts>(&encode(&accounts)), Ok(accounts));
    }

    #[test]
    fn absent_optionals_are_omitted() {
        let value = serde_json::to_value(bare_session()).unwrap();
        let object = value.as_object().unwrap();
        for absent in ["cc_session_id", "model", "account", "account_kind", "cwd"] {
            assert!(!object.contains_key(absent), "{absent} should be omitted");
        }

        let value = serde_json::to_value(AgentAccount {
            exhausted: None,
            turns: None,
            budget_usd: None,
            ..key_account()
        })
        .unwrap();
        let object = value.as_object().unwrap();
        for absent in ["turns", "budget_usd", "exhausted", "exhausted_buckets"] {
            assert!(!object.contains_key(absent), "{absent} should be omitted");
        }

        let value = serde_json::to_value(AccountFlag {
            at_ms: 1,
            session: None,
            message: None,
            until_ms: None,
            kind: None,
        })
        .unwrap();
        assert_eq!(value, json!({"at_ms": 1}));
    }

    /// The field names out-of-tree readers decode by. A rename here breaks
    /// every one of them, so the literals are pinned.
    #[test]
    fn wire_names_are_pinned() {
        let value = serde_json::to_value(full_session()).unwrap();
        assert_eq!(value["state"], json!("thinking"));
        assert_eq!(value["account_kind"], json!("api-key"));
        assert_eq!(value["conversation_id"], json!(42));
        assert_eq!(value["started_at_ms"], json!(1_700_000_000_000u64));

        let value = serde_json::to_value(subscription_account()).unwrap();
        assert_eq!(value["account_kind"], json!("subscription"));
        assert_eq!(value["period"], json!("-"));
        assert_eq!(
            value["exhausted_buckets"]["five_hour"]["until_ms"],
            json!(1_700_018_000_000u64)
        );

        for kind in [AccountKind::Subscription, AccountKind::ApiKey] {
            assert_eq!(serde_json::to_value(kind).unwrap(), json!(kind.as_str()));
        }
    }

    #[test]
    fn decode_refuses_another_version() {
        assert_eq!(
            decode::<AgentSessions>(r#"{"v":2,"at_ms":5,"sessions":[]}"#),
            Err(AgentDecodeError::UnsupportedVersion(2))
        );
    }
}
