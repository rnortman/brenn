//! Users' OAuth credentials, held where no agent can read them.
//!
//! [`OAuthService`] is built once at boot from the resolved providers. It owns
//! the vault, the pending connect flows and the outbound HTTP client; the
//! server's callback route completes a flow through it, and the registry tools
//! in [`tools`] act through it on a conversation owner's behalf. `api-request`
//! also sends the deployment's keys for the `api_key_provider`s.

pub mod flow;
pub mod http;
pub mod path_template;
pub mod tools;
pub mod vault;

use std::collections::BTreeMap;

use brenn_lib::config::{OAuthProviderConfig, ResolvedOAuth};
use brenn_obs::alerting::{AlertDispatcher, AlertSeverity};
use brenn_tool_registry::ToolError;
use chrono::{DateTime, Utc};
use tracing::{error, info};

use crate::flow::{CallbackQuery, PendingFlows};
use crate::http::{CallFailure, OAuthHttp};
use crate::vault::{Credential, Vault};

/// Every declared provider, the vault, the pending flows and the client.
pub struct OAuthService {
    providers: BTreeMap<String, OAuthProviderConfig>,
    vault: Vault,
    flows: PendingFlows,
    http: OAuthHttp,
    alerts: AlertDispatcher,
}

/// Where a failed authorization-server call was made, for its log line.
pub(crate) struct CallSite<'a> {
    pub provider: &'a str,
    /// The call: `token exchange`, `userinfo`, `refresh`, `revoke`.
    pub call: &'a str,
    pub user: Option<&'a str>,
    pub account: Option<&'a str>,
}

/// What a failed authorization-server call comes to, once logged.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Failure {
    /// No usable answer: unreachable, timed out, or a non-2xx (named by its
    /// status alone; the body is in the log).
    Unavailable(String),
    /// A 2xx answer the protocol does not allow. The operator was alerted.
    Contract(String),
    /// The provider refused Brenn's client credentials. The operator was
    /// alerted; nothing a user does can help.
    ClientRefused(String),
}

impl Failure {
    /// The failure's text, for a tool error or a page.
    pub(crate) fn into_detail(self) -> String {
        match self {
            Failure::Unavailable(detail) | Failure::Contract(detail) => detail,
            Failure::ClientRefused(detail) => format!(
                "the provider refused Brenn's client credentials ({detail}); the operator has \
                 been alerted"
            ),
        }
    }
}

/// What a callback came to, for the route to render.
#[derive(Debug, PartialEq, Eq)]
pub enum CallbackOutcome {
    /// No live flow under the `state`: unknown, expired or already used.
    Expired,
    /// The flow was started by another Brenn user than the one whose session
    /// carried the callback. The flow is gone.
    WrongUser { flow_user: String },
    /// The flow was started for another provider than the path names. A
    /// provider redirects only to the redirect URI it was given, so the
    /// request was composed by hand. The flow is gone.
    ProviderMismatch { flow_provider: String },
    /// The provider reports the user did not grant access.
    Declined {
        error: String,
        description: Option<String>,
    },
    /// The provider issued no refresh token; nothing was stored.
    NoRefreshToken,
    /// The provider did not answer usably (transport failure or non-2xx).
    ProviderUnavailable { detail: String },
    /// The provider answered 2xx with something the protocol does not allow.
    /// The operator was alerted.
    ProviderContract { detail: String },
    /// The provider refused Brenn's client credentials. The operator was
    /// alerted.
    ClientRefused { detail: String },
    /// The credential for `account` is stored.
    Connected { account: String },
}

impl OAuthService {
    /// The service over the boot-resolved providers and vault. `alerts`
    /// receives a provider's protocol breaches.
    pub fn new(resolved: ResolvedOAuth, alerts: AlertDispatcher) -> Self {
        Self {
            providers: resolved.providers,
            vault: Vault::new(resolved.vault_dir),
            flows: PendingFlows::default(),
            http: OAuthHttp::new(),
            alerts,
        }
    }

    /// The configured provider named `name`.
    pub fn provider(&self, name: &str) -> Option<&OAuthProviderConfig> {
        self.providers.get(name)
    }

    /// The configured provider named `name`, or the refusal a tool returns for
    /// an unknown one, naming the configured providers.
    pub fn require_provider(&self, name: &str) -> Result<&OAuthProviderConfig, ToolError> {
        self.provider(name).ok_or_else(|| {
            ToolError::InvalidArgs(format!(
                "unknown provider {name:?}; configured: {:?}",
                self.provider_names()
            ))
        })
    }

    /// Every configured provider name, in order.
    pub fn provider_names(&self) -> Vec<&str> {
        self.providers.keys().map(String::as_str).collect()
    }

    pub fn vault(&self) -> &Vault {
        &self.vault
    }

    pub fn flows(&self) -> &PendingFlows {
        &self.flows
    }

    pub fn http(&self) -> &OAuthHttp {
        &self.http
    }

    /// Finish the flow a provider's redirect names: consume it, check whose it
    /// is, exchange the code, learn the account, and store the credential.
    ///
    /// `provider` is a configured provider (the route checked) and
    /// `session_user_id` the Brenn user whose session carried the request.
    pub async fn complete_callback(
        &self,
        provider: &str,
        query: &CallbackQuery,
        session_user_id: i64,
    ) -> CallbackOutcome {
        let Some(flow) = self.flows.take(&query.state) else {
            return CallbackOutcome::Expired;
        };
        if flow.user.id != session_user_id {
            return CallbackOutcome::WrongUser {
                flow_user: flow.user.username,
            };
        }
        if flow.provider != provider {
            return CallbackOutcome::ProviderMismatch {
                flow_provider: flow.provider,
            };
        }
        if let Some(error) = &query.error {
            return CallbackOutcome::Declined {
                error: error.clone(),
                description: query.error_description.clone(),
            };
        }
        let site = |call| CallSite {
            provider,
            call,
            user: Some(&flow.user.username),
            account: None,
        };
        let Some(code) = &query.code else {
            return self.callback_failure(
                site("redirect"),
                CallFailure::Contract(
                    "the redirect carries neither `code` nor `error`".to_string(),
                ),
            );
        };
        let config = self
            .providers
            .get(provider)
            .expect("the callback route admits only configured providers");

        let tokens = match self.http.exchange_code(config, code, &flow.verifier).await {
            Ok(tokens) => tokens,
            Err(failure) => return self.callback_failure(site("token exchange"), failure),
        };
        let Some(refresh_token) = tokens.refresh_token else {
            info!(
                provider,
                user = %flow.user.username,
                "oauth connect: the provider issued no refresh token; nothing stored"
            );
            return CallbackOutcome::NoRefreshToken;
        };
        let userinfo = match self.http.userinfo(config, &tokens.access_token).await {
            Ok(userinfo) => userinfo,
            Err(failure) => return self.callback_failure(site("userinfo"), failure),
        };
        let field = &config.raw.account_field;
        let account = match userinfo.get(field) {
            Some(serde_json::Value::String(account)) if !account.is_empty() => account.clone(),
            _ => {
                return self.callback_failure(
                    site("userinfo"),
                    CallFailure::Contract(format!("userinfo has no non-empty string `{field}`")),
                );
            }
        };

        let now = Utc::now();
        let Some(expires_at) = expiry(now, tokens.expires_in) else {
            return self.callback_failure(
                site("token exchange"),
                CallFailure::Contract(format!(
                    "expires_in {} is out of range",
                    tokens.expires_in.unwrap_or_default()
                )),
            );
        };
        let credential = Credential::new(
            provider,
            &account,
            refresh_token,
            tokens.access_token,
            expires_at,
            tokens.scope.unwrap_or(flow.scopes),
            now,
        );
        self.vault.store(&flow.user.username, &credential).await;
        info!(
            provider,
            user = %flow.user.username,
            account = %account,
            scopes = ?credential.scopes,
            "oauth connect: credential stored"
        );
        CallbackOutcome::Connected { account }
    }

    /// Log a failed authorization-server call once, and alert the operator
    /// when the provider broke the protocol: a provider changing what it sends
    /// is something Brenn must be adapted to.
    pub(crate) fn report_failure(&self, site: CallSite<'_>, failure: CallFailure) -> Failure {
        let CallSite {
            provider,
            call,
            user,
            account,
        } = site;
        match failure {
            CallFailure::Transport(text) => {
                error!(provider, call, user, account, error = %text, "oauth: provider unreachable");
                Failure::Unavailable(text)
            }
            CallFailure::Status { status, body } => {
                error!(provider, call, user, account, status, body = %body, "oauth: provider refused");
                Failure::Unavailable(format!("HTTP {status}"))
            }
            CallFailure::Contract(detail) => {
                error!(provider, call, user, account, detail = %detail, "oauth: provider broke the protocol");
                self.alerts.alert(
                    AlertSeverity::Warning,
                    format!("OAuth provider {provider} broke the protocol"),
                    format!("{call}: {detail}"),
                );
                Failure::Contract(detail)
            }
            CallFailure::ClientRefused { status, body } => {
                error!(provider, call, user, account, status, body = %body, "oauth: provider refused the client credentials");
                // The client credentials are read at boot only, so the refusal
                // lasts until a restart: one alert per provider, not one per
                // call.
                self.alerts.alert_once_per_process(
                    AlertSeverity::Warning,
                    format!("OAuth provider {provider} refused Brenn's client credentials"),
                    provider,
                    format!(
                        "{call}: HTTP {status}: {body}\nCheck the client id and secret in \
                         client_secret_file against the provider's console."
                    ),
                );
                Failure::ClientRefused(format!("HTTP {status}"))
            }
        }
    }

    fn callback_failure(&self, site: CallSite<'_>, failure: CallFailure) -> CallbackOutcome {
        match self.report_failure(site, failure) {
            Failure::Unavailable(detail) => CallbackOutcome::ProviderUnavailable { detail },
            Failure::Contract(detail) => CallbackOutcome::ProviderContract { detail },
            Failure::ClientRefused(detail) => CallbackOutcome::ClientRefused { detail },
        }
    }
}

/// When an access token issued at `now` with `expires_in` seconds expires;
/// `None` when that is out of range. No `expires_in`: the lifetime is
/// unknown, so the token counts as already expired and its first use
/// refreshes it.
pub(crate) fn expiry(now: DateTime<Utc>, expires_in: Option<u64>) -> Option<DateTime<Utc>> {
    i64::try_from(expires_in.unwrap_or(0))
        .ok()
        .and_then(chrono::Duration::try_seconds)
        .and_then(|d| now.checked_add_signed(d))
}

#[cfg(any(test, feature = "testutils"))]
pub mod testutil;

#[cfg(test)]
mod tests;
