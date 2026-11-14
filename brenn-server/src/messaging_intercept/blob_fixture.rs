//! A conversation whose app reads and publishes blob-carrying channels, over
//! a messenger with a blob store: the fixture of the conversation blob verbs'
//! tests, on the hook path and on the tool socket.

use std::sync::Arc;

use serde_json::json;
use uuid::Uuid;

use super::*;
use brenn_envelope::grants::AppCapability;
use brenn_lib::access::acl::ChannelMatcher;
use brenn_messaging::testutils::{
    TEST_BLOB_LIMITS, TempBlobDir, covering_system_publisher, declared_non_text,
    ephemeral_channel_entry, install_blob_store_with, publish_attached, seal_blob,
    test_channel_entry,
};
use brenn_messaging_store::blob::BlobLimits;

pub(crate) const PRODUCER: &str = "blob-producer";
pub(crate) const SRC: &str = "brenn:src";
pub(crate) const DST: &str = "brenn:dst";
pub(crate) const HIDDEN: &str = "brenn:hidden";
pub(crate) const RING: &str = "ephemeral:ring";
pub(crate) const RING_DEPTH: u64 = 2;
/// A provider's request inbox, which `testapp`'s policy covers by an exact
/// matcher: a watch of tool-call traffic.
pub(crate) const WATCHED: &str = "brenn:tool-requests/watched";

pub(crate) struct Fixture {
    pub(crate) bridge: Arc<ActiveBridge>,
    pub(crate) m: Arc<brenn_messaging::Messenger>,
    // Declared after the messenger, so the directory outlives its store.
    pub(crate) blobs: TempBlobDir,
}

impl Fixture {
    /// Run `tool` for the conversation and return its answer, success or
    /// error.
    pub(crate) async fn call(&self, tool: &str, input: serde_json::Value) -> serde_json::Value {
        match execute_messaging_tool(&self.bridge, tool, &input)
            .await
            .expect("a messaging tool")
        {
            ToolOutcome::Ok(v) | ToolOutcome::Err(v) => v,
        }
    }

    pub(crate) async fn send(&self, input: serde_json::Value) -> serde_json::Value {
        self.call(MCP_MESSAGE_SEND_TOOL, input).await
    }

    /// Read `address` through `MessageChannelGet`, as the conversation.
    pub(crate) async fn read(&self, address: &str) {
        let answer = self
            .call(
                MCP_MESSAGE_QUERY_CHANNEL_TOOL,
                json!({ "address": address, "limit": 50 }),
            )
            .await;
        assert!(answer.is_array(), "MessageChannelGet failed: {answer}");
    }

    pub(crate) fn has_read(&self, message_id: Uuid) -> bool {
        self.m
            .conversation_has_read(self.bridge.conversation_id, message_id)
    }

    pub(crate) fn sealed(&self) -> usize {
        self.m.blob_store().expect("a blob store").usage().sealed
    }

    /// The retained envelopes of `address`, newest first, as the app reads them.
    pub(crate) async fn retained(&self, address: &str) -> Vec<MessageEnvelope> {
        self.m
            .query(&MessageQuery {
                channel: address.to_string(),
                limit: 50,
                before: None,
                after: None,
                sender: None,
                search: None,
                calling_app_slug: "testapp".to_string(),
            })
            .await
            .expect("query")
    }

    /// A message from the producer on `address` carrying `scan.pdf`.
    pub(crate) async fn attached(
        &self,
        address: &str,
        bytes: &[u8],
    ) -> (Uuid, brenn_envelope::BlobId) {
        let hold = seal_blob(&self.m, bytes);
        let id = hold.id().clone();
        let message = publish_attached(
            &self.m,
            PRODUCER,
            address,
            "a scan",
            &[("scan.pdf", &hold)],
            None,
        )
        .await;
        (message, id)
    }
}

/// `testapp` reads and publishes `brenn:src`, `brenn:dst` and `ephemeral:ring`,
/// reads [`WATCHED`], and may not read `brenn:hidden`; a producer may publish
/// anywhere.
/// `brenn:src` may hold a body that is not text.
pub(crate) async fn fixture() -> Fixture {
    fixture_with_limits(TEST_BLOB_LIMITS).await
}

/// [`fixture`] with a blob store of `limits`.
pub(crate) async fn fixture_with_limits(limits: BlobLimits) -> Fixture {
    build(limits, None).await
}

/// [`fixture`] whose bridge holds `tools` and grants `testapp` each tool
/// named in `granted`, with the tool executor's system policy wired as boot
/// wires it, and `testapp`'s tool-result inbox when `inbox`.
pub(crate) async fn fixture_with_tools(
    tools: Arc<brenn_tool_registry::ToolRegistry>,
    granted: &[&str],
    inbox: bool,
) -> Fixture {
    fixture_with_rate_limited_tools(tools, granted, inbox, None).await
}

/// [`fixture_with_tools`] whose every grant carries `rate_limit`.
pub(crate) async fn fixture_with_rate_limited_tools(
    tools: Arc<brenn_tool_registry::ToolRegistry>,
    granted: &[&str],
    inbox: bool,
    rate_limit: Option<brenn_lib::tools::ResolvedRateLimit>,
) -> Fixture {
    build(TEST_BLOB_LIMITS, Some((tools, granted, inbox, rate_limit))).await
}

type ToolWiring<'a> = (
    Arc<brenn_tool_registry::ToolRegistry>,
    &'a [&'a str],
    bool,
    Option<brenn_lib::tools::ResolvedRateLimit>,
);

async fn build(limits: BlobLimits, tools: Option<ToolWiring<'_>>) -> Fixture {
    let mut durable: Vec<_> = ["src", "dst", "hidden", "tool-requests/watched"]
        .into_iter()
        .map(|name| test_channel_entry(name, vec![]))
        .map(|entry| match entry.address.as_str() {
            SRC => declared_non_text(entry),
            _ => entry,
        })
        .collect();
    let mut registrations = covering_system_publisher(PRODUCER);
    if let Some((_, _, inbox, _)) = tools {
        if inbox {
            durable.push(brenn_tool_registry::bus_wiring::app_result_inbox_entry(
                "testapp",
                &brenn_lib::messaging::config::SystemChannelTuning::default(),
                &brenn_lib::messaging::MessagingGlobalConfig::default(),
            ));
        }
        registrations.extend(brenn_messaging::testutils::system_registrations(
            std::collections::HashMap::from([(
                brenn_tool_registry::TOOL_EXECUTOR_COMPONENT.to_string(),
                brenn_tool_registry::bus_wiring::tool_executor_system_policy(),
            )]),
        ));
    }
    let ring = ephemeral_channel_entry("ring", RING_DEPTH);
    let db = crate::test_support::init_db_memory();
    {
        let conn = db.lock().await;
        brenn_messaging_store::db::upsert_channels(&conn, &durable);
    }
    let mut cfg = crate::test_support::app_config::default_test_app_config("testapp", "testapp");
    let policy = cfg.policy_mut();
    for grant in [
        AppCapability::MessagingSubscribe,
        AppCapability::MessagingPublish,
        AppCapability::EphemeralSubscribe,
        AppCapability::EphemeralPublish,
    ] {
        policy.grants.insert(grant);
    }
    for name in ["src", "dst"] {
        policy
            .acls
            .brenn_subscribe
            .push(ChannelMatcher::Exact(name.to_string()));
        policy
            .acls
            .brenn_publish
            .push(ChannelMatcher::Exact(name.to_string()));
    }
    policy
        .acls
        .brenn_subscribe
        .push(ChannelMatcher::Exact("tool-requests/watched".to_string()));
    policy
        .acls
        .ephemeral_subscribe
        .push(ChannelMatcher::Exact("ring".to_string()));
    policy
        .acls
        .ephemeral_publish
        .push(ChannelMatcher::Exact("ring".to_string()));
    let mut apps = indexmap::IndexMap::new();
    apps.insert("testapp".to_string(), cfg);
    let rings = vec![ring];
    let m = brenn_messaging::Messenger::new(
        db,
        Arc::new(brenn_lib::messaging::MessagingDirectory::with_entries(
            durable.into_iter().chain(rings.iter().cloned()).collect(),
        )),
        Arc::from("test-source"),
        Arc::new(apps),
        Arc::new(brenn_messaging::query::NoopWakeRouter) as Arc<dyn brenn_messaging::WakeRouter>,
        brenn_lib::messaging::MessagingGlobalConfig::default(),
    )
    .with_subscriber_registrations(registrations)
    .with_ring_stores(Arc::new(brenn_messaging_store::store::RingStores::build(
        &rings,
    )));
    let (m, blobs) = install_blob_store_with(m, limits).await;
    let bridge = match tools {
        None => ActiveBridge::test_new_with_messenger(m.clone()).await,
        Some((tools, granted, _, rate_limit)) => {
            let grants = granted
                .iter()
                .map(|name| {
                    (
                        name.to_string(),
                        brenn_lib::tools::ResolvedToolGrant {
                            acl: vec![],
                            rate_limit,
                        },
                    )
                })
                .collect();
            ActiveBridge::test_new_with_messenger_and_tools(m.clone(), tools, grants).await
        }
    };
    Fixture { bridge, m, blobs }
}
