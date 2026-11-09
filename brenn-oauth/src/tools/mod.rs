//! The registry tools that act through [`crate::OAuthService`], and
//! `api-request`, which also sends the deployment's API keys.

pub mod accounts;
pub mod api_request;
pub mod connect;
pub mod disconnect;

use std::collections::BTreeMap;
use std::sync::Arc;

use brenn_lib::config::ApiKeyTable;
use brenn_lib::tools::{AclClause, acl_admits};
use brenn_tool_registry::{AclDenied, RegisteredTool};

use crate::OAuthService;

/// The registry tools for the declared providers: the three OAuth tools when
/// `oauth` is present, and `api-request` when either kind is declared — when
/// `keys` holds a provider as it stands now, for the key kind.
pub fn registered(oauth: Option<&Arc<OAuthService>>, keys: ApiKeyTable) -> Vec<RegisteredTool> {
    let mut tools: Vec<RegisteredTool> = Vec::new();
    if let Some(service) = oauth {
        tools.push(RegisteredTool::Async(Arc::new(
            connect::OAuthConnectTool::new(service.clone()),
        )));
        tools.push(RegisteredTool::Async(Arc::new(
            accounts::OAuthAccountsTool::new(service.clone()),
        )));
        tools.push(RegisteredTool::Async(Arc::new(
            disconnect::OAuthDisconnectTool::new(service.clone()),
        )));
    }
    if oauth.is_some() || !keys.load().is_empty() {
        tools.push(RegisteredTool::Async(Arc::new(
            api_request::ApiRequestTool::new(oauth.cloned(), keys),
        )));
    }
    tools
}

/// Whether `acl` admits `provider`, for a tool whose only ACL key is
/// `provider`.
pub(crate) fn provider_admitted(acl: &[AclClause], provider: &str) -> Result<(), AclDenied> {
    let attrs = BTreeMap::from([("provider".to_string(), provider.to_string())]);
    if acl_admits(acl, &attrs) {
        Ok(())
    } else {
        Err(AclDenied {
            resource: provider.to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{key_provider_config, provider_config, service};

    fn maps(base: &str) -> ApiKeyTable {
        ApiKeyTable::new(Arc::new(BTreeMap::from([key_provider_config(
            "maps", base,
        )])))
    }

    fn names(tools: &[RegisteredTool]) -> Vec<&'static str> {
        tools.iter().map(|tool| tool.descriptor().name).collect()
    }

    #[tokio::test]
    async fn each_kind_registers_its_tools() {
        let base = "http://127.0.0.1:9";
        let (svc, _dir) = service(vec![provider_config("google", base)]);
        let all = [
            "oauth-connect",
            "oauth-accounts",
            "oauth-disconnect",
            "api-request",
        ];
        assert_eq!(names(&registered(None, maps(base))), ["api-request"]);
        assert_eq!(names(&registered(Some(&svc), ApiKeyTable::empty())), all);
        assert_eq!(names(&registered(Some(&svc), maps(base))), all);
        assert!(registered(None, ApiKeyTable::empty()).is_empty());
    }

    /// Every descriptor's schema has the shape a call is validated against.
    #[tokio::test]
    async fn every_descriptor_schema_has_the_checked_shape() {
        let base = "http://127.0.0.1:9";
        let (svc, _dir) = service(vec![provider_config("google", base)]);
        let tools = registered(Some(&svc), maps(base));
        assert_eq!(tools.len(), 4);
        for tool in &tools {
            let d = tool.descriptor();
            assert_eq!(
                brenn_envelope::tool_schema::check_input_schema(&d.input_schema),
                Ok(()),
                "{}",
                d.name
            );
        }
    }
}
