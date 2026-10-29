//! The registry tools that act through [`crate::OAuthService`].

pub mod accounts;
pub mod api_request;
pub mod connect;
pub mod disconnect;

use std::collections::BTreeMap;

use brenn_lib::tools::{AclClause, acl_admits};
use brenn_tool_registry::AclDenied;

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
