//! Tool-grant policy vocabulary shared across participant kinds (LLM apps and
//! WASM consumers).
//!
//! A tool grant authorizes a participant to address a named registry tool,
//! optionally narrowed by an ACL and throttled by a rate limit. Both participant
//! kinds author identical `[[*.tool_grant]]` tables (`config::ToolGrantRaw`)
//! that resolve into the `ResolvedToolGrant` values below, keyed by tool name in
//! `AppPolicy::tool_grants`. Backend-only — no `ts-rs` derive.
//!
//! This module owns config parsing/resolution and the ACL-matching primitive.
//! The registry itself (tool objects, execution, MCP projection) lives in
//! brenn-server; brenn-lib holds only the operator-authored vocabulary because
//! config resolution lives here.

pub mod config;

use std::collections::BTreeMap;

/// One resolved ACL clause: a conjunction of `key = value` requirements. A
/// clause matches a call's resource attributes iff every key it names is
/// present with a matching value (`"*"` matches any value). Keys are AND'd
/// within a clause; clauses are OR'd within a grant (`ResolvedToolGrant::acl`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AclClause(BTreeMap<String, String>);

impl AclClause {
    /// Wrap a resolved `key → value` map as a clause. Values are exact match
    /// strings except `"*"`, the sole wildcard.
    pub fn new(requirements: BTreeMap<String, String>) -> Self {
        Self(requirements)
    }

    /// The keys this clause constrains (e.g. `"repo"`). Used by registry config
    /// validation to confirm every clause key is one the tool declares.
    pub fn keys(&self) -> impl Iterator<Item = &str> {
        self.0.keys().map(String::as_str)
    }

    /// Does this clause admit a call whose resource attributes are `attrs`?
    /// Every constrained key must be present with a matching value (`"*"` any).
    pub fn matches(&self, attrs: &BTreeMap<String, String>) -> bool {
        self.0
            .iter()
            .all(|(k, allowed)| attrs.get(k).is_some_and(|v| allowed == "*" || allowed == v))
    }
}

/// Resolved per-`(participant, tool)` rate limit (token-bucket parameters).
/// `burst` is the bucket capacity; `sustained_per_minute` is the refill rate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedRateLimit {
    pub burst: u32,
    pub sustained_per_minute: u32,
}

/// A resolved tool grant: the ACL clauses (OR'd) narrowing which resources the
/// grant covers, plus an optional rate limit. An empty `acl` means the tool
/// takes no ACL — the grant alone authorizes every call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedToolGrant {
    pub acl: Vec<AclClause>,
    pub rate_limit: Option<ResolvedRateLimit>,
}

impl ResolvedToolGrant {
    /// Does this grant's ACL admit a call whose resource attributes are `attrs`?
    /// An empty ACL admits everything (a tool that takes no ACL); otherwise any
    /// covering clause (OR semantics) admits.
    pub fn acl_allows(&self, attrs: &BTreeMap<String, String>) -> bool {
        self.acl.is_empty() || self.acl.iter().any(|c| c.matches(attrs))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn attrs(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn clause(pairs: &[(&str, &str)]) -> AclClause {
        AclClause::new(
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        )
    }

    #[test]
    fn clause_matches_exact_and_requires_every_key() {
        let c = clause(&[("repo", "brenn"), ("branch", "main")]);
        // All constrained keys present with matching values ⇒ match.
        assert!(c.matches(&attrs(&[("repo", "brenn"), ("branch", "main")])));
        // Extra attributes are ignored.
        assert!(c.matches(&attrs(&[("repo", "brenn"), ("branch", "main"), ("x", "y")])));
        // One key mismatches ⇒ no match (AND within a clause).
        assert!(!c.matches(&attrs(&[("repo", "brenn"), ("branch", "dev")])));
        // A required key missing ⇒ no match.
        assert!(!c.matches(&attrs(&[("repo", "brenn")])));
    }

    #[test]
    fn clause_wildcard_matches_any_value_but_key_must_exist() {
        let c = clause(&[("repo", "*")]);
        assert!(c.matches(&attrs(&[("repo", "brenn")])));
        assert!(c.matches(&attrs(&[("repo", "pfin")])));
        // Wildcard still requires the key to be present.
        assert!(!c.matches(&attrs(&[("branch", "main")])));
    }

    #[test]
    fn grant_acl_ors_clauses_and_empty_admits_all() {
        let grant = ResolvedToolGrant {
            acl: vec![clause(&[("repo", "brenn")]), clause(&[("repo", "pfin")])],
            rate_limit: None,
        };
        assert!(grant.acl_allows(&attrs(&[("repo", "brenn")])));
        assert!(grant.acl_allows(&attrs(&[("repo", "pfin")])));
        assert!(!grant.acl_allows(&attrs(&[("repo", "graf")])));

        // Empty ACL ⇒ tool takes no ACL ⇒ every call admitted.
        let no_acl = ResolvedToolGrant {
            acl: vec![],
            rate_limit: None,
        };
        assert!(no_acl.acl_allows(&attrs(&[("repo", "anything")])));
        assert!(no_acl.acl_allows(&attrs(&[])));
    }

    #[test]
    fn clause_keys_enumerates_constrained_keys() {
        let c = clause(&[("repo", "brenn"), ("branch", "main")]);
        let mut keys: Vec<&str> = c.keys().collect();
        keys.sort_unstable();
        assert_eq!(keys, vec!["branch", "repo"]);
    }
}
