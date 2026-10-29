//! `oauth-disconnect`: revoke one of the acting user's credentials at the
//! provider and delete it from the vault.

use std::sync::Arc;

use async_trait::async_trait;
use brenn_lib::tools::AclClause;
use brenn_tool_registry::{
    AclDenied, AsyncTool, Idempotency, ToolClass, ToolCtx, ToolDescriptor, ToolError, UserBinding,
};
use serde::Deserialize;
use tracing::info;

use super::provider_admitted;
use crate::http::Revoked;
use crate::{CallSite, OAuthService};

const NAME: &str = "oauth-disconnect";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DisconnectArgs {
    provider: String,
    account: String,
}

/// The `oauth-disconnect` tool.
pub struct OAuthDisconnectTool {
    descriptor: ToolDescriptor,
    service: Arc<OAuthService>,
}

impl OAuthDisconnectTool {
    pub fn new(service: Arc<OAuthService>) -> Self {
        Self {
            descriptor: ToolDescriptor {
                name: NAME,
                mcp_name: "mcp__brenn__OAuthDisconnect",
                description: "Disconnect one of the user's accounts: revoke Brenn's access at \
                              the provider and delete the stored credential. Only when the user \
                              asks for it. `revoked: false` means the provider was not told (it \
                              has no revocation endpoint, or already considered the credential \
                              dead); `detail` says which, and what the user can do. An error \
                              means nothing was deleted; the user can try again later.",
                input_schema: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "provider": {
                            "type": "string",
                            "description": "The provider's configured name, e.g. \"google\".",
                        },
                        "account": {
                            "type": "string",
                            "description": "The account, as `oauth-accounts` lists it.",
                        },
                    },
                    "required": ["provider", "account"],
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
impl AsyncTool for OAuthDisconnectTool {
    fn descriptor(&self) -> &ToolDescriptor {
        &self.descriptor
    }

    fn check_acl(&self, args: &serde_json::Value, acl: &[AclClause]) -> Result<(), AclDenied> {
        // Unparseable args are not an ACL question; `execute` refuses them.
        match serde_json::from_value::<DisconnectArgs>(args.clone()) {
            Ok(parsed) => provider_admitted(acl, &parsed.provider),
            Err(_) => Ok(()),
        }
    }

    async fn execute(
        &self,
        ctx: &ToolCtx,
        args: serde_json::Value,
    ) -> Result<serde_json::Value, ToolError> {
        let user = ctx.require_acting_user(NAME).clone();
        let args: DisconnectArgs = serde_json::from_value(args)
            .map_err(|e| ToolError::InvalidArgs(format!("{NAME} args: {e}")))?;
        let provider = self.service.require_provider(&args.provider)?;
        let vault = self.service.vault();
        // Held across the revocation, so a refresh or reconnect of this
        // credential cannot land between the revoke and the delete.
        let guard = vault
            .lock(&args.provider, &user.username, &args.account)
            .await;
        let Some(credential) = vault.read(guard.path()).await else {
            drop(guard);
            let connected = vault.accounts(&args.provider, &user.username).await;
            return Err(ToolError::InvalidArgs(format!(
                "no {:?} account {:?} is connected; connected: {connected:?}",
                args.provider, args.account
            )));
        };

        let (revoked, detail) = match &provider.raw.revoke_url {
            None => (
                false,
                Some(format!(
                    "{} has no revocation endpoint configured; Brenn deleted its copy, and the \
                     user can remove {} from the account's security settings at the provider",
                    args.provider, provider.raw.app_name
                )),
            ),
            Some(url) => match self
                .service
                .http()
                .revoke(url, &credential.refresh_token)
                .await
            {
                Ok(Revoked::Revoked) => (true, None),
                Ok(Revoked::AlreadyInvalid) => (
                    false,
                    Some("the provider already considered the credential invalid".to_string()),
                ),
                Err(failure) => {
                    let site = CallSite {
                        provider: &args.provider,
                        call: "revoke",
                        user: Some(&user.username),
                        account: Some(&args.account),
                    };
                    let detail = self.service.report_failure(site, failure).into_detail();
                    return Err(ToolError::Internal(format!(
                        "the provider did not revoke the credential ({detail}); nothing was \
                         deleted"
                    )));
                }
            },
        };
        vault.remove(&guard).await;
        info!(
            provider = %args.provider,
            user = %user.username,
            account = %args.account,
            revoked,
            "oauth disconnect: credential deleted"
        );
        let mut result = serde_json::json!({ "revoked": revoked, "deleted": true });
        if let Some(detail) = detail {
            result["detail"] = serde_json::Value::String(detail);
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{
        Stub, alice, credential, provider_clause, provider_config, service, start_stub, tool_ctx,
    };

    struct Fixture {
        tool: OAuthDisconnectTool,
        svc: Arc<OAuthService>,
        stub: Stub,
        _dir: tempfile::TempDir,
    }

    async fn fixture() -> Fixture {
        let (base, stub) = start_stub().await;
        let (_, google) = provider_config("google", &base);
        let (_, mut norevoke) = provider_config("norevoke", &base);
        norevoke.raw.revoke_url = None;
        let (svc, dir) = service(vec![
            ("google".to_string(), google),
            ("norevoke".to_string(), norevoke),
        ]);
        svc.vault()
            .store("alice", &credential("google", "alice@example.com", "RT-a"))
            .await;
        svc.vault()
            .store(
                "alice",
                &credential("norevoke", "alice@example.com", "RT-n"),
            )
            .await;
        Fixture {
            tool: OAuthDisconnectTool::new(svc.clone()),
            svc,
            stub,
            _dir: dir,
        }
    }

    impl Fixture {
        async fn disconnect(
            &self,
            provider: &str,
            account: &str,
        ) -> Result<serde_json::Value, ToolError> {
            self.tool
                .execute(
                    &tool_ctx(Some(alice()), vec![provider_clause("*")]),
                    serde_json::json!({"provider": provider, "account": account}),
                )
                .await
        }

        async fn stored(&self, provider: &str) -> bool {
            let vault = self.svc.vault();
            vault
                .read(&vault.credential_path(provider, "alice", "alice@example.com"))
                .await
                .is_some()
        }
    }

    #[tokio::test]
    async fn revokes_then_deletes() {
        let f = fixture().await;
        let out = f.disconnect("google", "alice@example.com").await.unwrap();
        assert_eq!(out, serde_json::json!({"revoked": true, "deleted": true}));
        assert!(!f.stored("google").await);
        assert!(f.stored("norevoke").await, "only the named credential goes");
        let forms = f.stub.lock().unwrap().revoke_forms.clone();
        assert_eq!(forms.len(), 1);
        assert_eq!(forms[0]["token"], "RT-a");
    }

    #[tokio::test]
    async fn invalid_token_deletes_without_revoking() {
        let f = fixture().await;
        {
            let mut s = f.stub.lock().unwrap();
            s.revoke_status = Some(400);
            s.revoke_body = Some(serde_json::json!({"error": "invalid_token"}));
        }
        let out = f.disconnect("google", "alice@example.com").await.unwrap();
        assert_eq!(out["revoked"], false);
        assert_eq!(out["deleted"], true);
        assert!(out["detail"].as_str().unwrap().contains("already"));
        assert!(!f.stored("google").await);
    }

    #[tokio::test]
    async fn any_other_refusal_keeps_the_credential() {
        let f = fixture().await;
        for (status, body) in [
            (503, serde_json::json!({})),
            (400, serde_json::json!({"error": "invalid_request"})),
            (401, serde_json::json!({"error": "invalid_token"})),
        ] {
            {
                let mut s = f.stub.lock().unwrap();
                s.revoke_status = Some(status);
                s.revoke_body = Some(body.clone());
            }
            let err = f.disconnect("google", "alice@example.com").await;
            let Err(ToolError::Internal(detail)) = err else {
                panic!("{status} {body}: expected Internal, got {err:?}");
            };
            assert!(detail.contains(&format!("HTTP {status}")), "{detail}");
            assert!(f.stored("google").await, "{status} {body}: credential kept");
        }
    }

    #[tokio::test]
    async fn unreachable_provider_keeps_the_credential() {
        let (_, google) = provider_config("google", "http://127.0.0.1:9");
        let (svc, _dir) = service(vec![("google".to_string(), google)]);
        svc.vault()
            .store("alice", &credential("google", "alice@example.com", "RT"))
            .await;
        let tool = OAuthDisconnectTool::new(svc.clone());
        let err = tool
            .execute(
                &tool_ctx(Some(alice()), vec![provider_clause("*")]),
                serde_json::json!({"provider": "google", "account": "alice@example.com"}),
            )
            .await;
        assert!(matches!(err, Err(ToolError::Internal(_))), "{err:?}");
        let vault = svc.vault();
        assert!(
            vault
                .read(&vault.credential_path("google", "alice", "alice@example.com"))
                .await
                .is_some()
        );
    }

    #[tokio::test]
    async fn no_revoke_url_deletes_and_says_so() {
        let f = fixture().await;
        let out = f.disconnect("norevoke", "alice@example.com").await.unwrap();
        assert_eq!(out["revoked"], false);
        assert_eq!(out["deleted"], true);
        assert!(
            out["detail"]
                .as_str()
                .unwrap()
                .contains("no revocation endpoint")
        );
        assert!(!f.stored("norevoke").await);
        assert!(f.stub.lock().unwrap().revoke_forms.is_empty());
    }

    #[tokio::test]
    async fn an_unconnected_account_is_invalid_args_listing_the_connected() {
        let f = fixture().await;
        let err = f.disconnect("google", "nobody@example.com").await;
        let Err(ToolError::InvalidArgs(detail)) = err else {
            panic!("expected InvalidArgs, got {err:?}");
        };
        assert!(detail.contains("alice@example.com"), "{detail}");
        assert!(f.stub.lock().unwrap().revoke_forms.is_empty());
    }

    #[tokio::test]
    async fn an_account_too_long_for_a_file_name_is_invalid_args() {
        let f = fixture().await;
        let err = f.disconnect("google", &"+".repeat(100)).await;
        assert!(matches!(err, Err(ToolError::InvalidArgs(_))), "{err:?}");
        assert!(f.stub.lock().unwrap().revoke_forms.is_empty());
        assert!(f.stored("google").await);
    }

    #[tokio::test]
    async fn another_users_credential_is_not_reachable() {
        let f = fixture().await;
        let bob = brenn_tool_registry::ActingUser {
            id: 2,
            username: "bob".to_string(),
        };
        let err = f
            .tool
            .execute(
                &tool_ctx(Some(bob), vec![provider_clause("*")]),
                serde_json::json!({"provider": "google", "account": "alice@example.com"}),
            )
            .await;
        assert!(matches!(err, Err(ToolError::InvalidArgs(_))), "{err:?}");
        assert!(f.stored("google").await);
    }

    #[tokio::test]
    async fn unknown_provider_is_invalid_args() {
        let f = fixture().await;
        let err = f.disconnect("nope", "alice@example.com").await;
        assert!(matches!(err, Err(ToolError::InvalidArgs(_))), "{err:?}");
    }

    #[tokio::test]
    #[should_panic(expected = "requires an acting user")]
    async fn no_acting_user_panics() {
        let f = fixture().await;
        let _ = f
            .tool
            .execute(
                &tool_ctx(None, vec![provider_clause("*")]),
                serde_json::json!({"provider": "google", "account": "alice@example.com"}),
            )
            .await;
    }

    #[tokio::test]
    async fn check_acl_and_descriptor() {
        let f = fixture().await;
        let args = serde_json::json!({"provider": "google", "account": "a@example.com"});
        assert!(
            f.tool
                .check_acl(&args, &[provider_clause("google")])
                .is_ok()
        );
        assert_eq!(
            f.tool.check_acl(&args, &[provider_clause("other")]),
            Err(AclDenied {
                resource: "google".to_string()
            })
        );
        let d = f.tool.descriptor();
        assert_eq!(d.user_binding, UserBinding::ActingUser);
        assert_eq!(d.acl_keys, &["provider"]);
        assert!(d.auto_approve);
    }
}
