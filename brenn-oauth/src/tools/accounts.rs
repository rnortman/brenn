//! `oauth-accounts`: list the acting user's connected accounts, never a token.

use std::sync::Arc;

use async_trait::async_trait;
use brenn_lib::tools::AclClause;
use brenn_tool_registry::{
    AclDenied, AsyncTool, Idempotency, ToolClass, ToolCtx, ToolDescriptor, ToolError, UserBinding,
};
use serde::Deserialize;

use super::provider_admitted;
use crate::OAuthService;

const NAME: &str = "oauth-accounts";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AccountsArgs {
    #[serde(default)]
    provider: Option<String>,
}

/// The `oauth-accounts` tool.
pub struct OAuthAccountsTool {
    descriptor: ToolDescriptor,
    service: Arc<OAuthService>,
}

impl OAuthAccountsTool {
    pub fn new(service: Arc<OAuthService>) -> Self {
        Self {
            descriptor: ToolDescriptor {
                name: NAME,
                mcp_name: "mcp__brenn__OAuthAccounts",
                description: "List the user's accounts connected through OAuth providers (e.g. \
                              Google): provider, account, the scopes the user granted, and when \
                              it was connected. Use it to confirm a connection after \
                              `oauth-connect`, or to learn which `account` to name in a request. \
                              A missing scope means the user did not grant it; connect again \
                              with that scope set.",
                input_schema: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "provider": {
                            "type": "string",
                            "description": "Only this provider's accounts. Omitted: every \
                                            provider you may use.",
                        },
                    },
                    "additionalProperties": false,
                }),
                class: ToolClass::Async { max_concurrency: 4 },
                acl_keys: &["provider"],
                idempotency: Idempotency::Natural,
                auto_approve: true,
                user_binding: UserBinding::ActingUser,
            },
            service,
        }
    }
}

#[async_trait]
impl AsyncTool for OAuthAccountsTool {
    fn descriptor(&self) -> &ToolDescriptor {
        &self.descriptor
    }

    fn check_acl(&self, args: &serde_json::Value, acl: &[AclClause]) -> Result<(), AclDenied> {
        // Unparseable args are not an ACL question; `execute` refuses them.
        // With no provider named, `execute` filters to the admitted ones.
        match serde_json::from_value::<AccountsArgs>(args.clone()) {
            Ok(AccountsArgs {
                provider: Some(provider),
            }) => provider_admitted(acl, &provider),
            _ => Ok(()),
        }
    }

    async fn execute(
        &self,
        ctx: &ToolCtx,
        args: serde_json::Value,
    ) -> Result<serde_json::Value, ToolError> {
        let user = ctx.require_acting_user(NAME).clone();
        let args: AccountsArgs = serde_json::from_value(args)
            .map_err(|e| ToolError::InvalidArgs(format!("{NAME} args: {e}")))?;
        let providers: Vec<String> = match args.provider {
            Some(provider) => {
                self.service.require_provider(&provider)?;
                vec![provider]
            }
            None => self
                .service
                .provider_names()
                .into_iter()
                .filter(|p| provider_admitted(&ctx.grant.acl, p).is_ok())
                .map(str::to_string)
                .collect(),
        };
        let mut credentials = Vec::new();
        for provider in &providers {
            credentials.extend(self.service.vault().list(provider, &user.username).await);
        }
        let accounts: Vec<serde_json::Value> = credentials
            .into_iter()
            .map(|c| {
                serde_json::json!({
                    "provider": c.provider,
                    "account": c.account,
                    "scopes": c.scopes,
                    "obtained_at": c.obtained_at.to_rfc3339(),
                })
            })
            .collect();
        Ok(serde_json::json!({ "accounts": accounts }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{alice, credential, provider_clause, provider_config, service, tool_ctx};

    fn tool() -> (OAuthAccountsTool, Arc<OAuthService>, tempfile::TempDir) {
        let (svc, dir) = service(vec![
            provider_config("google", "http://127.0.0.1:9"),
            provider_config("other", "http://127.0.0.1:9"),
        ]);
        (OAuthAccountsTool::new(svc.clone()), svc, dir)
    }

    async fn connect(svc: &OAuthService, user: &str, provider: &str, account: &str) {
        svc.vault()
            .store(user, &credential(provider, account, "secret-refresh"))
            .await;
    }

    fn listed(out: &serde_json::Value) -> Vec<(String, String)> {
        out["accounts"]
            .as_array()
            .unwrap()
            .iter()
            .map(|a| {
                (
                    a["provider"].as_str().unwrap().to_string(),
                    a["account"].as_str().unwrap().to_string(),
                )
            })
            .collect()
    }

    #[tokio::test]
    async fn lists_the_acting_users_accounts_without_tokens() {
        let (tool, svc, _dir) = tool();
        connect(&svc, "alice", "google", "alice@example.com").await;
        connect(&svc, "alice", "google", "work@example.com").await;
        connect(&svc, "alice", "other", "alice@example.com").await;
        connect(&svc, "bob", "google", "bob@example.com").await;

        let out = tool
            .execute(
                &tool_ctx(Some(alice()), vec![provider_clause("*")]),
                serde_json::json!({}),
            )
            .await
            .unwrap();
        assert_eq!(
            listed(&out),
            vec![
                ("google".to_string(), "alice@example.com".to_string()),
                ("google".to_string(), "work@example.com".to_string()),
                ("other".to_string(), "alice@example.com".to_string()),
            ]
        );
        let first = &out["accounts"][0];
        assert_eq!(first["scopes"], serde_json::json!(["openid", "mail.read"]));
        assert!(first["obtained_at"].as_str().unwrap().contains('T'));
        let text = out.to_string();
        assert!(!text.contains("secret-refresh"), "{text}");
        assert!(!text.contains("\"access\""), "{text}");
    }

    #[tokio::test]
    async fn a_named_provider_lists_only_its_accounts() {
        let (tool, svc, _dir) = tool();
        connect(&svc, "alice", "google", "alice@example.com").await;
        connect(&svc, "alice", "other", "alice@example.com").await;
        let out = tool
            .execute(
                &tool_ctx(Some(alice()), vec![provider_clause("*")]),
                serde_json::json!({"provider": "other"}),
            )
            .await
            .unwrap();
        assert_eq!(
            listed(&out),
            vec![("other".to_string(), "alice@example.com".to_string())]
        );
    }

    #[tokio::test]
    async fn no_provider_lists_only_providers_the_grant_admits() {
        let (tool, svc, _dir) = tool();
        connect(&svc, "alice", "google", "alice@example.com").await;
        connect(&svc, "alice", "other", "alice@example.com").await;
        let out = tool
            .execute(
                &tool_ctx(Some(alice()), vec![provider_clause("google")]),
                serde_json::json!({}),
            )
            .await
            .unwrap();
        assert_eq!(
            listed(&out),
            vec![("google".to_string(), "alice@example.com".to_string())]
        );
    }

    #[tokio::test]
    async fn nothing_connected_is_an_empty_list() {
        let (tool, _svc, _dir) = tool();
        let out = tool
            .execute(
                &tool_ctx(Some(alice()), vec![provider_clause("*")]),
                serde_json::json!({}),
            )
            .await
            .unwrap();
        assert_eq!(out, serde_json::json!({"accounts": []}));
    }

    #[tokio::test]
    async fn unknown_provider_and_extra_keys_are_invalid_args() {
        let (tool, _svc, _dir) = tool();
        for args in [
            serde_json::json!({"provider": "nope"}),
            serde_json::json!({"x": 1}),
        ] {
            let err = tool
                .execute(
                    &tool_ctx(Some(alice()), vec![provider_clause("*")]),
                    args.clone(),
                )
                .await;
            assert!(matches!(err, Err(ToolError::InvalidArgs(_))), "{args}");
        }
    }

    #[tokio::test]
    #[should_panic(expected = "requires an acting user")]
    async fn no_acting_user_panics() {
        let (tool, _svc, _dir) = tool();
        let _ = tool
            .execute(
                &tool_ctx(None, vec![provider_clause("*")]),
                serde_json::json!({}),
            )
            .await;
    }

    #[tokio::test]
    async fn check_acl_matches_a_named_provider_only() {
        let (tool, _svc, _dir) = tool();
        let acl = [provider_clause("google")];
        assert!(
            tool.check_acl(&serde_json::json!({"provider": "google"}), &acl)
                .is_ok()
        );
        assert!(tool.check_acl(&serde_json::json!({}), &acl).is_ok());
        assert_eq!(
            tool.check_acl(&serde_json::json!({"provider": "other"}), &acl),
            Err(AclDenied {
                resource: "other".to_string()
            })
        );
    }

    #[tokio::test]
    async fn descriptor_binds_the_acting_user() {
        let (tool, _svc, _dir) = tool();
        let d = tool.descriptor();
        assert_eq!(d.user_binding, UserBinding::ActingUser);
        assert_eq!(d.acl_keys, &["provider"]);
        assert_eq!(d.class, ToolClass::Async { max_concurrency: 4 });
        assert!(d.auto_approve);
    }
}
