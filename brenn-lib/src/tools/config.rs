//! Operator-authored `[[*.tool_grant]]` config and its resolution into
//! `ResolvedToolGrant` values.
//!
//! The raw shape is identical under `[[app.tool_grant]]` and
//! `[[wasm_consumer.tool_grant]]` — one grant vocabulary, both participant
//! kinds. Resolution is fail-fast (CLAUDE.md robustness): a duplicate tool, an
//! empty/malformed ACL clause, or an out-of-range rate limit panics at config
//! load.

use std::collections::BTreeMap;

use serde::Deserialize;

use super::{AclClause, ResolvedRateLimit, ResolvedToolGrant};

/// Canonical name of the git-repo-pull tool, used both as the registry key and
/// for the implicit-from-mounts grant an app earns from its git mounts.
pub const GIT_REPO_PULL_TOOL: &str = "git-repo-pull";

/// Raw `[[*.tool_grant]]` table: a tool name, an optional list of ACL clauses
/// (each a TOML table of `key = value` requirements), and an optional rate
/// limit. `acl` clauses are OR'd; keys within a clause are AND'd.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolGrantRaw {
    /// Canonical (kebab-case) tool name this grant addresses.
    pub tool: String,
    /// ACL clauses narrowing the grant. Each table's values must be strings
    /// (`"*"` is the sole wildcard). Absent/empty ⇒ the tool takes no ACL.
    #[serde(default)]
    pub acl: Vec<toml::Table>,
    /// Optional token-bucket throttle for `(participant, tool)`. Absent ⇒
    /// unlimited (the grant itself is the gate).
    pub rate_limit: Option<RateLimitRaw>,
}

/// Raw `rate_limit = { burst = N, sustained_per_minute = M }` table.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RateLimitRaw {
    /// Token-bucket capacity (`>= 1`, validated at resolution).
    pub burst: u32,
    /// Refill tokens per minute (`>= 1`, validated at resolution).
    pub sustained_per_minute: u32,
}

/// Resolve a participant's `[[*.tool_grant]]` tables into a tool-name-keyed map.
/// `owner` is a diagnostic label such as `app "pfin"` or `wasm consumer "sync"`.
///
/// # Panics
///
/// Panics (operator config — fail-fast) on: a duplicate `tool` entry, an empty
/// `tool` name, an empty ACL clause (would match everything), an ACL clause
/// value that is not a string or is empty, or a rate limit with `burst < 1` or
/// `sustained_per_minute < 1`.
pub fn resolve_tool_grants(
    owner: &str,
    raw: &[ToolGrantRaw],
) -> BTreeMap<String, ResolvedToolGrant> {
    let mut grants = BTreeMap::new();
    for g in raw {
        let resolved = resolve_one(owner, g);
        let prev = grants.insert(g.tool.clone(), resolved);
        assert!(
            prev.is_none(),
            "{owner}: duplicate tool_grant for tool {:?}",
            g.tool,
        );
    }
    grants
}

/// Resolve an LLM app's tool grants: its explicit `[[app.tool_grant]]` tables
/// plus an implicit `git-repo-pull` grant derived from `mount_slugs` when the
/// app mounts repos and did not author an explicit `git-repo-pull` grant.
/// Explicit replaces derived (an operator may tighten the mount-derived grant).
pub fn resolve_app_tool_grants(
    owner: &str,
    raw: &[ToolGrantRaw],
    mount_slugs: &[String],
) -> BTreeMap<String, ResolvedToolGrant> {
    let mut grants = resolve_tool_grants(owner, raw);
    if !grants.contains_key(GIT_REPO_PULL_TOOL)
        && let Some(derived) = derived_git_repo_pull_grant(mount_slugs.iter().map(String::as_str))
    {
        grants.insert(GIT_REPO_PULL_TOOL.to_string(), derived);
    }
    grants
}

/// Build the implicit `git-repo-pull` grant an app earns from its git mounts:
/// one `{ repo = "<slug>" }` ACL clause per mounted repo slug (mounted ⇒
/// pullable). Returns `None` when there are no mount slugs (no grant to derive).
pub fn derived_git_repo_pull_grant<'a>(
    slugs: impl IntoIterator<Item = &'a str>,
) -> Option<ResolvedToolGrant> {
    let acl: Vec<AclClause> = slugs
        .into_iter()
        .map(|slug| AclClause::new(BTreeMap::from([("repo".to_string(), slug.to_string())])))
        .collect();
    if acl.is_empty() {
        None
    } else {
        Some(ResolvedToolGrant {
            acl,
            rate_limit: None,
        })
    }
}

fn resolve_one(owner: &str, raw: &ToolGrantRaw) -> ResolvedToolGrant {
    assert!(
        !raw.tool.is_empty(),
        "{owner}: tool_grant has an empty `tool` name",
    );
    let acl = raw
        .acl
        .iter()
        .map(|table| resolve_clause(owner, &raw.tool, table))
        .collect();
    let rate_limit = raw.rate_limit.map(|rl| {
        assert!(
            rl.burst >= 1,
            "{owner}: tool_grant {:?} rate_limit.burst must be >= 1",
            raw.tool,
        );
        assert!(
            rl.sustained_per_minute >= 1,
            "{owner}: tool_grant {:?} rate_limit.sustained_per_minute must be >= 1",
            raw.tool,
        );
        ResolvedRateLimit {
            burst: rl.burst,
            sustained_per_minute: rl.sustained_per_minute,
        }
    });
    ResolvedToolGrant { acl, rate_limit }
}

fn resolve_clause(owner: &str, tool: &str, table: &toml::Table) -> AclClause {
    assert!(
        !table.is_empty(),
        "{owner}: tool_grant {tool:?} has an empty ACL clause (would match everything)",
    );
    let mut clause = BTreeMap::new();
    for (key, value) in table {
        let s = value.as_str().unwrap_or_else(|| {
            panic!("{owner}: tool_grant {tool:?} ACL clause key {key:?} must be a string value",)
        });
        assert!(
            !s.is_empty(),
            "{owner}: tool_grant {tool:?} ACL clause key {key:?} has an empty value",
        );
        clause.insert(key.clone(), s.to_string());
    }
    AclClause::new(clause)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AppConfigRaw;
    use crate::messaging::config::WasmConsumerConfigRaw;

    #[test]
    fn parses_tool_grant_under_app() {
        // A `[[app.tool_grant]]` block round-trips through the app raw config.
        let toml = r#"
            slug = "pfin"
            working_dir = "/srv/pfin"

            [[tool_grant]]
            tool = "git-repo-pull"
            acl = [{ repo = "brenn" }, { repo = "pfin" }]
            rate_limit = { burst = 4, sustained_per_minute = 12 }
        "#;
        let raw: AppConfigRaw = toml::from_str(toml).expect("app parses");
        assert_eq!(raw.tool_grants.len(), 1);
        assert_eq!(raw.tool_grants[0].tool, "git-repo-pull");
        assert_eq!(raw.tool_grants[0].acl.len(), 2);
        let rl = raw.tool_grants[0].rate_limit.expect("rate limit present");
        assert_eq!(rl.burst, 4);
        assert_eq!(rl.sustained_per_minute, 12);
    }

    #[test]
    fn parses_tool_grant_under_wasm_consumer() {
        // The identical table shape parses under `[[wasm_consumer.tool_grant]]`.
        let toml = r#"
            slug = "sync"
            component_path = "/srv/sync.wasm"
            grants = ["store"]
            store_path = "/srv/sync.db"

            [[tool_grant]]
            tool = "git-repo-pull"
            acl = [{ repo = "brenn" }]
        "#;
        let raw: WasmConsumerConfigRaw = toml::from_str(toml).expect("wasm consumer parses");
        assert_eq!(raw.tool_grants.len(), 1);
        assert_eq!(raw.tool_grants[0].tool, "git-repo-pull");
        assert_eq!(raw.tool_grants[0].acl.len(), 1);
        assert!(raw.tool_grants[0].rate_limit.is_none());
    }

    fn grant(tool: &str, acl: &[&[(&str, &str)]]) -> ToolGrantRaw {
        ToolGrantRaw {
            tool: tool.to_string(),
            acl: acl
                .iter()
                .map(|clause| {
                    clause
                        .iter()
                        .map(|(k, v)| (k.to_string(), toml::Value::String(v.to_string())))
                        .collect()
                })
                .collect(),
            rate_limit: None,
        }
    }

    #[test]
    fn resolves_clauses_and_rate_limit() {
        let raw = vec![ToolGrantRaw {
            tool: "git-repo-pull".to_string(),
            acl: vec![
                [("repo".to_string(), toml::Value::String("brenn".to_string()))]
                    .into_iter()
                    .collect(),
            ],
            rate_limit: Some(RateLimitRaw {
                burst: 4,
                sustained_per_minute: 12,
            }),
        }];
        let resolved = resolve_tool_grants("app \"pfin\"", &raw);
        let g = resolved.get("git-repo-pull").expect("grant present");
        assert_eq!(g.acl.len(), 1);
        assert_eq!(
            g.rate_limit,
            Some(ResolvedRateLimit {
                burst: 4,
                sustained_per_minute: 12,
            })
        );
        let allowed: BTreeMap<String, String> = [("repo".to_string(), "brenn".to_string())]
            .into_iter()
            .collect();
        assert!(g.acl_allows(&allowed));
    }

    #[test]
    #[should_panic(expected = "duplicate tool_grant")]
    fn duplicate_tool_grant_panics() {
        let raw = vec![
            grant("git-repo-pull", &[&[("repo", "brenn")]]),
            grant("git-repo-pull", &[&[("repo", "pfin")]]),
        ];
        resolve_tool_grants("app \"pfin\"", &raw);
    }

    #[test]
    #[should_panic(expected = "empty ACL clause")]
    fn empty_acl_clause_panics() {
        let raw = vec![grant("git-repo-pull", &[&[]])];
        resolve_tool_grants("app \"pfin\"", &raw);
    }

    #[test]
    #[should_panic(expected = "must be a string value")]
    fn non_string_acl_value_panics() {
        let raw = vec![ToolGrantRaw {
            tool: "git-repo-pull".to_string(),
            acl: vec![
                [("repo".to_string(), toml::Value::Integer(7))]
                    .into_iter()
                    .collect(),
            ],
            rate_limit: None,
        }];
        resolve_tool_grants("app \"pfin\"", &raw);
    }

    #[test]
    #[should_panic(expected = "rate_limit.burst must be >= 1")]
    fn zero_burst_panics() {
        let raw = vec![ToolGrantRaw {
            tool: "git-repo-pull".to_string(),
            acl: vec![],
            rate_limit: Some(RateLimitRaw {
                burst: 0,
                sustained_per_minute: 12,
            }),
        }];
        resolve_tool_grants("app \"pfin\"", &raw);
    }

    #[test]
    fn app_tool_grants_derive_git_from_mounts() {
        // No explicit grants; two mounts ⇒ implicit git-repo-pull with a clause
        // per slug.
        let mounts = vec!["brenn".to_string(), "pfin".to_string()];
        let resolved = resolve_app_tool_grants("app \"pfin\"", &[], &mounts);
        let g = resolved.get(GIT_REPO_PULL_TOOL).expect("derived grant");
        assert_eq!(g.acl.len(), 2);
        let brenn: BTreeMap<String, String> = [("repo".to_string(), "brenn".to_string())]
            .into_iter()
            .collect();
        let graf: BTreeMap<String, String> = [("repo".to_string(), "graf".to_string())]
            .into_iter()
            .collect();
        assert!(g.acl_allows(&brenn));
        assert!(!g.acl_allows(&graf)); // graf not mounted
    }

    #[test]
    fn no_mounts_yields_no_derived_grant() {
        let resolved = resolve_app_tool_grants("app \"chat\"", &[], &[]);
        assert!(resolved.is_empty());
    }

    #[test]
    fn explicit_git_grant_replaces_derived() {
        // An explicit git-repo-pull grant tightening the scope to just `brenn`
        // wins over the mount-derived grant that would cover both mounts.
        let mounts = vec!["brenn".to_string(), "pfin".to_string()];
        let explicit = vec![grant("git-repo-pull", &[&[("repo", "brenn")]])];
        let resolved = resolve_app_tool_grants("app \"pfin\"", &explicit, &mounts);
        let g = resolved.get(GIT_REPO_PULL_TOOL).expect("grant present");
        assert_eq!(g.acl.len(), 1);
        let pfin: BTreeMap<String, String> = [("repo".to_string(), "pfin".to_string())]
            .into_iter()
            .collect();
        // pfin is mounted but the explicit grant excludes it.
        assert!(!g.acl_allows(&pfin));
    }
}
