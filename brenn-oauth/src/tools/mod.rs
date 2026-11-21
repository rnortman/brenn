//! The registry tools that act through [`crate::OAuthService`];
//! `api-request`, which also sends the deployment's API keys; and
//! `one-click-unsubscribe`, which sends no credential at all.

pub mod accounts;
pub mod api_request;
#[cfg(test)]
mod api_request_blob_tests;
pub mod connect;
pub mod disconnect;
pub mod one_click_unsubscribe;

use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};

use brenn_lib::config::ApiKeyTable;
use brenn_lib::tools::{AclClause, acl_admits};
use brenn_messaging_store::blob::BlobStore;
use brenn_obs::alerting::AlertDispatcher;
use brenn_tool_registry::{AclDenied, RegisteredTool};

use crate::OAuthService;

/// The blob store `api-request` streams bodies from and into, attached once
/// messaging is built: the registry that holds the tool is built before the
/// store exists. Clones share one slot.
// TODO(quality-blobstoreslot-copies-the-late-attach-newtype): one generic
// late-attach type for this, `DeviceLocationBus` and `ProviderRuntime`'s
// messenger.
#[derive(Clone, Default)]
pub struct BlobStoreSlot(Arc<OnceLock<Arc<BlobStore>>>);

impl BlobStoreSlot {
    /// Attach `store` for every clone of this slot.
    ///
    /// # Panics
    ///
    /// A store is already attached.
    pub fn attach(&self, store: Arc<BlobStore>) {
        assert!(
            self.0.set(store).is_ok(),
            "BUG: a blob store attached twice to api-request's slot"
        );
    }

    /// The attached store, or `None` in a process with no messaging.
    pub fn get(&self) -> Option<&Arc<BlobStore>> {
        self.0.get()
    }
}

/// The registry tools for the declared providers: the three OAuth tools when
/// `oauth` is present, and `api-request` when either kind is declared — when
/// `keys` holds a provider as it stands now, for the key kind. `blobs` is the
/// slot `api-request` finds its blob store in.
pub fn registered(
    oauth: Option<&Arc<OAuthService>>,
    keys: ApiKeyTable,
    blobs: BlobStoreSlot,
) -> Vec<RegisteredTool> {
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
    if api_request_registered(oauth.is_some(), !keys.load().is_empty()) {
        tools.push(api_request(oauth.cloned(), keys, blobs));
    }
    tools
}

/// Whether a process registers `api-request`: when it runs an OAuth service or
/// holds at least one key provider.
pub fn api_request_registered(has_oauth: bool, has_key_providers: bool) -> bool {
    has_oauth || has_key_providers
}

/// The `api-request` tool over `oauth` and the key table `keys`, which it reads
/// per call, writing answers into the store `blobs` holds.
pub fn api_request(
    oauth: Option<Arc<OAuthService>>,
    keys: ApiKeyTable,
    blobs: BlobStoreSlot,
) -> RegisteredTool {
    RegisteredTool::Async(Arc::new(api_request::ApiRequestTool::new(
        oauth, keys, blobs,
    )))
}

/// The `one-click-unsubscribe` tool, raising its refused-destination alerts
/// through `alerts`. Registered in every process: it needs no configuration,
/// and a tool nobody is granted does nothing.
pub fn one_click_unsubscribe(alerts: AlertDispatcher) -> RegisteredTool {
    RegisteredTool::Async(Arc::new(
        one_click_unsubscribe::OneClickUnsubscribeTool::new(alerts),
    ))
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
        assert_eq!(
            names(&registered(None, maps(base), BlobStoreSlot::default())),
            ["api-request"]
        );
        assert_eq!(
            names(&registered(
                Some(&svc),
                ApiKeyTable::empty(),
                BlobStoreSlot::default()
            )),
            all
        );
        assert_eq!(
            names(&registered(
                Some(&svc),
                maps(base),
                BlobStoreSlot::default()
            )),
            all
        );
        assert!(registered(None, ApiKeyTable::empty(), BlobStoreSlot::default()).is_empty());
    }

    /// Every descriptor's schema has the shape a call is validated against.
    #[tokio::test]
    async fn every_descriptor_schema_has_the_checked_shape() {
        let base = "http://127.0.0.1:9";
        let (svc, _dir) = service(vec![provider_config("google", base)]);
        let tools = registered(Some(&svc), maps(base), BlobStoreSlot::default());
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
