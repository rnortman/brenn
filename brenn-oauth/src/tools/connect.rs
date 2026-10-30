//! `oauth-connect`: start a connect flow for the acting user and hand back
//! the link and the steps to relay.

use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use brenn_lib::tools::AclClause;
use brenn_tool_registry::{
    AclDenied, AsyncTool, Idempotency, ToolClass, ToolCtx, ToolDescriptor, ToolError, UserBinding,
};
use serde::Deserialize;
use tracing::info;

use super::provider_admitted;
use crate::OAuthService;
use crate::flow::{PendingFlow, authorize_url, mint_state, mint_verifier};

const NAME: &str = "oauth-connect";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConnectArgs {
    provider: String,
    scope_sets: Vec<String>,
    #[serde(default)]
    login_hint: Option<String>,
}

/// The `oauth-connect` tool.
pub struct OAuthConnectTool {
    descriptor: ToolDescriptor,
    service: Arc<OAuthService>,
}

impl OAuthConnectTool {
    pub fn new(service: Arc<OAuthService>) -> Self {
        Self {
            descriptor: ToolDescriptor {
                name: NAME,
                mcp_name: "mcp__brenn__OAuthConnect",
                description: "Start connecting one of the user's accounts at an OAuth provider \
                              (e.g. Google) so Brenn can act with it. Returns a link and the \
                              steps for the user. Relay `steps` to the user verbatim, with the \
                              link; the link works for 10 minutes and only for this user. When \
                              the user says they are done, `oauth-accounts` confirms the \
                              connection.",
                input_schema: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "provider": {
                            "type": "string",
                            "description": "The provider's configured name, e.g. \"google\".",
                        },
                        "scope_sets": {
                            "type": "array",
                            "items": { "type": "string" },
                            "minItems": 1,
                            "description": "Named scope sets the provider declares, e.g. \
                                            [\"gmail_readonly\", \"calendar_readonly\"].",
                        },
                        "login_hint": {
                            "type": "string",
                            "description": "The account address to preselect, when known.",
                        },
                    },
                    "required": ["provider", "scope_sets"],
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

/// The steps a user follows, in Brenn's words rather than the model's.
fn steps(app_name: &str) -> Vec<String> {
    [
        "Open the link in the browser you use for Brenn.".to_string(),
        "Choose the account to connect.".to_string(),
        format!(
            "If you are warned that the app is unverified, tap \"Advanced\", then \
             \"Go to {app_name}\"."
        ),
        "Tick every box you are asked about.".to_string(),
        "If you see a Brenn login page, log in; you will be brought back.".to_string(),
        "When the page says \"Connected\", come back here and say so.".to_string(),
    ]
    .into()
}

#[async_trait]
impl AsyncTool for OAuthConnectTool {
    fn descriptor(&self) -> &ToolDescriptor {
        &self.descriptor
    }

    fn check_acl(&self, args: &serde_json::Value, acl: &[AclClause]) -> Result<(), AclDenied> {
        // Unparseable args are not an ACL question; `execute` refuses them.
        match serde_json::from_value::<ConnectArgs>(args.clone()) {
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
        let args: ConnectArgs = serde_json::from_value(args)
            .map_err(|e| ToolError::InvalidArgs(format!("{NAME} args: {e}")))?;
        let provider = self.service.require_provider(&args.provider)?;
        if args.scope_sets.is_empty() {
            return Err(ToolError::InvalidArgs(
                "scope_sets must name at least one set".to_string(),
            ));
        }
        let mut scopes: Vec<String> = provider.raw.base_scopes.clone();
        for set in &args.scope_sets {
            let members = provider.raw.scope_sets.get(set).ok_or_else(|| {
                ToolError::InvalidArgs(format!(
                    "provider {:?} has no scope set {set:?}; it has {:?}",
                    args.provider,
                    provider.raw.scope_sets.keys().collect::<Vec<_>>()
                ))
            })?;
            scopes.extend(members.iter().cloned());
        }
        let mut seen = std::collections::HashSet::new();
        scopes.retain(|s| seen.insert(s.clone()));

        let state = mint_state();
        let verifier = mint_verifier();
        let url = authorize_url(
            provider,
            &scopes,
            &state,
            &verifier,
            args.login_hint.as_deref(),
        );
        info!(
            provider = %args.provider,
            user = %user.username,
            scope_sets = ?args.scope_sets,
            "oauth connect: flow started"
        );
        self.service.flows().insert(
            state,
            PendingFlow {
                provider: args.provider.clone(),
                user,
                conversation_id: ctx.acting_conversation_id,
                verifier,
                scopes,
                created_at: Instant::now(),
            },
        );
        let mut result = serde_json::json!({
            "url": url,
            "steps": steps(&provider.raw.app_name),
        });
        if let Some(hint) = args.login_hint {
            result["account_hint"] = serde_json::Value::String(hint);
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use brenn_lib::tools::ResolvedToolGrant;
    use brenn_tool_registry::ActingUser;

    use super::*;
    use crate::flow::s256_challenge;
    use crate::testutil::{provider_config, service};

    fn ctx(user: Option<ActingUser>) -> ToolCtx {
        ToolCtx {
            caller: brenn_lib::messaging::ParticipantId::for_app("pa", "test-origin"),
            grant: ResolvedToolGrant {
                acl: vec![],
                rate_limit: None,
            },
            acting_conversation_id: Some(7),
            acting_user: user,
            deadline: None,
        }
    }

    fn alice() -> ActingUser {
        ActingUser {
            id: 1,
            username: "alice".to_string(),
        }
    }

    fn tool() -> (OAuthConnectTool, Arc<OAuthService>, tempfile::TempDir) {
        let (svc, dir) = service(vec![provider_config("google", "http://127.0.0.1:9")]);
        (OAuthConnectTool::new(svc.clone()), svc, dir)
    }

    #[tokio::test]
    async fn connect_records_a_flow_and_returns_its_link() {
        let (tool, svc, _dir) = tool();
        let out = tool
            .execute(
                &ctx(Some(alice())),
                serde_json::json!({
                    "provider": "google",
                    "scope_sets": ["mail", "calendar", "mail"],
                    "login_hint": "alice@example.com",
                }),
            )
            .await
            .unwrap();
        assert_eq!(out["account_hint"], "alice@example.com");
        let steps = out["steps"].as_array().unwrap();
        assert!(
            steps
                .iter()
                .any(|s| s.as_str().unwrap().contains("Go to Brenn Test"))
        );

        let url = url::Url::parse(out["url"].as_str().unwrap()).unwrap();
        let q: BTreeMap<String, String> = url.query_pairs().into_owned().collect();
        assert_eq!(q["scope"], "openid mail.read calendar.read");
        let flow = svc
            .flows()
            .take(&q["state"])
            .expect("flow recorded under state");
        assert_eq!(flow.user, alice());
        assert_eq!(flow.provider, "google");
        assert_eq!(flow.conversation_id, Some(7));
        assert_eq!(q["code_challenge"], s256_challenge(&flow.verifier));
        assert_eq!(flow.scopes, vec!["openid", "mail.read", "calendar.read"]);
    }

    #[tokio::test]
    async fn unknown_provider_and_scope_set_are_invalid_args() {
        let (tool, _svc, _dir) = tool();
        for args in [
            serde_json::json!({"provider": "nope", "scope_sets": ["mail"]}),
            serde_json::json!({"provider": "google", "scope_sets": ["nope"]}),
            serde_json::json!({"provider": "google", "scope_sets": []}),
            serde_json::json!({"provider": "google", "scope_sets": ["mail"], "x": 1}),
        ] {
            let err = tool.execute(&ctx(Some(alice())), args.clone()).await;
            assert!(matches!(err, Err(ToolError::InvalidArgs(_))), "{args}");
        }
    }

    #[tokio::test]
    #[should_panic(expected = "requires an acting user")]
    async fn no_acting_user_panics() {
        let (tool, _svc, _dir) = tool();
        let _ = tool
            .execute(
                &ctx(None),
                serde_json::json!({"provider": "google", "scope_sets": ["mail"]}),
            )
            .await;
    }

    #[tokio::test]
    async fn check_acl_matches_provider() {
        let (tool, _svc, _dir) = tool();
        let clause =
            |v: &str| AclClause::new(BTreeMap::from([("provider".to_string(), v.to_string())]));
        let args = serde_json::json!({"provider": "google", "scope_sets": ["mail"]});
        assert!(tool.check_acl(&args, &[clause("google")]).is_ok());
        assert!(tool.check_acl(&args, &[clause("*")]).is_ok());
        assert_eq!(
            tool.check_acl(&args, &[clause("other")]),
            Err(AclDenied {
                resource: "google".to_string()
            })
        );
    }

    #[tokio::test]
    async fn descriptor_binds_the_acting_user() {
        let (tool, _svc, _dir) = tool();
        let d = tool.descriptor();
        assert_eq!(d.user_binding, UserBinding::ActingUser);
        assert_eq!(d.acl_keys, &["provider"]);
        assert!(d.auto_approve);
    }
}
