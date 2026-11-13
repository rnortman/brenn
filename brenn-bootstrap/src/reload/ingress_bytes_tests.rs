//! Transport ingress in both shapes, end to end: a payload handed to the real
//! `MqttEventRouterImpl`, or a request posted over TCP to the server's real
//! router and handled by the real `WebhookEventRouterImpl`, is published by the
//! real `Messenger`, dispatched to the 0.2.0 `processor-sdk-bytes-test`
//! fixture, and read back from the report the fixture publishes of what the
//! guest SDK's accessors return.
//!
//! MQTT: three channels, each read by its own instance of the fixture and
//! reported on its own channel: `mqtt:broker:cam/front` is raw and declares
//! `untyped_content_type`, `mqtt:broker:raw/front` is raw and declares none,
//! `mqtt:broker:plain/front` is wrapped. Webhook: `webhook:inbox` is raw under
//! an `application/octet-stream` endpoint, `webhook:plainhook` is wrapped.

use std::sync::Arc;

use brenn_lib::config::PACKAGED;
use brenn_lib::messaging::Urgency;
use brenn_messaging::format::ConversationEnvelopes;
use brenn_messaging::query::MessageQuery;
use brenn_mqtt::{InboundPayload, MqttEventRouter};
use brenn_server::mqtt_router::{IngressRoute, MqttEventRouterImpl};
use brenn_server::webhook_router::WebhookEventRouterImpl;
use chrono::{DateTime, Utc};
use serde_json::{Value, json};

use super::driver::tests::{
    BootFixture, Booted, READER, Tree, bodies_on, boot_with, document_subscribing_acl,
    install_package_from, poll_until, staged_module,
};

/// The declared client every channel here is minted on.
const CLIENT: &str = "broker";
/// Raw, typed `application/octet-stream` when the broker supplies no type.
const CAM: &str = "mqtt:broker:cam/front";
/// Raw, with no declared type.
const RAW: &str = "mqtt:broker:raw/front";
/// Wrapped: no tuning block.
const PLAIN: &str = "mqtt:broker:plain/front";
/// Bytes that are not UTF-8.
const NOT_UTF8: &[u8] = &[0xff, 0xd8, 0x00, 0x7f];

/// The document: one client, the two raw prefix tunings, and one fixture
/// instance per channel, each reporting on `brenn:report-<name>`. The reader
/// agent may read the `cam/` channels, as a conversation's
/// `MessageChannelGet` does.
fn ingress_document() -> String {
    let reports: String = ["cam", "raw", "plain"]
        .iter()
        .map(|name| {
            format!(
                "channel report_{name} at \"brenn:report-{name}\" {{\n    push_depth = 1;\n    \
                 retain_depth = 16;\n    standing_retain_depth = 16;\n}}\n"
            )
        })
        .collect();
    let instances: String = [("cam", CAM), ("raw", RAW), ("plain", PLAIN)]
        .iter()
        .map(|(name, address)| {
            format!(
                "new feeder-{name}: Feeder {{\n    grants = [ports, log];\n    \
                 in feed <- \"{address}\" {{ push_depth = 4; retain_depth = 4; }}\n    \
                 out report -> report_{name};\n}}\n"
            )
        })
        .collect();
    document_subscribing_acl(
        &format!(
            r#"mqtt_client {CLIENT} {{
    url = "mqtts://127.0.0.1:8883";
    qos = 1;
}}

channel at prefix "mqtt:broker:cam/" {{
    push_depth = 1;
    retain_depth = 16;
    standing_retain_depth = 16;
    non_text = true;
    untyped_content_type = "application/octet-stream";
}}
channel at prefix "mqtt:broker:raw/" {{
    push_depth = 1;
    retain_depth = 16;
    standing_retain_depth = 16;
    non_text = true;
}}
{reports}{PACKAGED}component Feeder {{
    abi = processor;
    requires = [ports, log];
    in feed;
    out report;
}}
{PACKAGED}
{instances}"#
        ),
        &[],
        &["topic_filter \"mqtt:broker:cam/#\""],
    )
}

/// The booted process, with the dispatcher running, and an ingress router
/// that publishes through its messenger over the three channels' routes.
async fn boot_ingress() -> (Tree, tempfile::TempDir, Booted, MqttEventRouterImpl) {
    let tree = Tree::holding(&ingress_document());
    let components = tempfile::tempdir().expect("a components root");
    install_package_from(
        components.path(),
        &staged_module(&tree),
        "brenn_processor_sdk_bytes_test.wasm",
    );
    let booted = boot_with(
        &tree,
        BootFixture {
            components_roots: vec![components.path().to_path_buf()],
            dispatcher: true,
            ..BootFixture::default()
        },
    )
    .await;
    let routes = [CAM, RAW, PLAIN]
        .iter()
        .map(|address| {
            let entry = booted
                .messenger
                .directory()
                .resolve(address)
                .expect("the binding mints the channel");
            IngressRoute {
                client_slug: CLIENT.to_string(),
                topic_filter: address
                    .strip_prefix("mqtt:broker:")
                    .expect("an mqtt address on the client")
                    .to_string(),
                channel_address: address.to_string(),
                channel_uuid: entry.uuid,
                urgency: Urgency::Normal,
            }
        })
        .collect();
    let router = MqttEventRouterImpl::new();
    let mut state = brenn_server::test_support::state::test_state(&booted.db);
    state.messenger = Some(Arc::clone(&booted.messenger));
    router.set_state(state, routes);
    (tree, components, booted, router)
}

/// The messages on `channel` as the fixture reported them, from the first
/// report on `report` whose `feed` window holds `wanted` of them; panics if
/// none arrives.
async fn reported(booted: &mut Booted, report: &str, channel: &str, wanted: usize) -> Vec<Value> {
    let messenger = Arc::clone(&booted.messenger);
    let found = poll_until(
        &format!("reports of {wanted} message(s) on {channel}"),
        1,
        booted.dispatcher.as_mut(),
        async || {
            bodies_on(&messenger, report)
                .await
                .iter()
                .filter_map(|body| {
                    let report: Value = serde_json::from_str(body).expect("a report is JSON");
                    let entries: Vec<Value> = report["windows"]
                        .as_array()
                        .expect("windows")
                        .iter()
                        .flat_map(|window| window["envelopes"].as_array().expect("envelopes"))
                        .filter(|entry| entry["header"]["channel"] == channel)
                        .cloned()
                        .collect();
                    (entries.len() == wanted).then_some(entries)
                })
                .collect::<Vec<_>>()
        },
    )
    .await;
    found.into_iter().next().expect("one report")
}

/// The number of rows stored on `address`.
async fn stored(booted: &Booted, address: &str) -> usize {
    let uuid = booted
        .messenger
        .directory()
        .resolve(address)
        .expect("the channel is live")
        .uuid;
    let conn = booted.messenger.db().lock().await;
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM messaging_messages WHERE channel_uuid = ?1",
            rusqlite::params![uuid.as_bytes().to_vec()],
            |row| row.get(0),
        )
        .expect("the messages table is readable");
    usize::try_from(count).expect("a row count")
}

/// Lower-case hex, as the fixture reports a body.
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// `entry`'s `mqtt_transport` is the raw message's metadata — client, the
/// actual topic, the delivery QoS, and a receive time within `[from, to]` —
/// and the header's `transport` key carries the same value.
fn assert_mqtt_transport(entry: &Value, topic: &str, from: DateTime<Utc>, to: DateTime<Utc>) {
    let transport = &entry["mqtt_transport"]["ok"];
    assert_eq!(transport["client_slug"], CLIENT, "{entry}");
    assert_eq!(transport["topic"], topic, "{entry}");
    assert_eq!(transport["qos"], 1, "{entry}");
    let received: DateTime<Utc> = transport["received_at"]
        .as_str()
        .expect("a receive time")
        .parse()
        .expect("an RFC 3339 instant");
    assert!(
        from <= received && received <= to,
        "{received} in [{from}, {to}]"
    );
    assert_eq!(entry["header"]["transport"], json!({ "mqtt": transport }));
}

/// The three accessors that refuse a raw MQTT message, each with the named
/// error kind.
fn assert_raw_refusals(entry: &Value) {
    let failed = json!({ "err": "processing-failed" });
    for accessor in ["webhook_transport", "mqtt_body", "webhook_body"] {
        assert_eq!(entry[accessor], failed, "{accessor}: {entry}");
    }
}

/// A binary payload with its own Content-Type, on a raw channel: the fixture
/// reads it byte-exact under that type, `mqtt_transport()` returns the
/// metadata, and the wrapper accessors refuse it. A conversation reading the
/// channel is shown the placeholder and the metadata, never the bytes.
#[tokio::test(flavor = "multi_thread")]
async fn a_typed_binary_payload_reaches_a_component_byte_exact_on_a_raw_channel() {
    let (_tree, _components, mut booted, router) = boot_ingress().await;

    let from = Utc::now();
    router
        .deliver_inbound(
            CLIENT,
            "cam/front",
            InboundPayload::new(NOT_UTF8.to_vec(), Some("image/jpeg".to_string())),
            1,
        )
        .await;
    let to = Utc::now();

    let entries = reported(&mut booted, "brenn:report-cam", CAM, 1).await;
    let entry = &entries[0];
    assert_eq!(entry["content_type"], "image/jpeg", "{entry}");
    assert_eq!(entry["hex"], hex(NOT_UTF8), "{entry}");
    assert_mqtt_transport(entry, "cam/front", from, to);
    assert_raw_refusals(entry);

    let shown = booted
        .messenger
        .query(&MessageQuery {
            channel: CAM.to_string(),
            limit: 10,
            before: None,
            after: None,
            sender: None,
            search: None,
            calling_app_slug: READER.to_string(),
        })
        .await
        .expect("the reader may read the raw channel");
    let shown = serde_json::to_value(ConversationEnvelopes(&shown)).expect("serializes");
    let shown = &shown[0];
    assert_eq!(
        shown["body"],
        json!({ "binary": true, "content_type": "image/jpeg", "size": NOT_UTF8.len() }),
        "{shown}"
    );
    assert_eq!(shown["transport"], entry["header"]["transport"], "{shown}");
    assert!(
        !shown.to_string().contains(&hex(NOT_UTF8)),
        "no encoding of the bytes reaches a conversation: {shown}"
    );
}

/// A binary payload with no Content-Type is typed by the channel's
/// `untyped_content_type` and read byte-exact under it; on a raw channel that
/// declares none, the same payload is dropped and the fixture never sees it.
#[tokio::test(flavor = "multi_thread")]
async fn an_untyped_binary_payload_takes_the_channels_declared_type_or_is_dropped() {
    let (_tree, _components, mut booted, router) = boot_ingress().await;

    let from = Utc::now();
    for topic in ["cam/front", "raw/front"] {
        router
            .deliver_inbound(
                CLIENT,
                topic,
                InboundPayload::new(NOT_UTF8.to_vec(), None),
                1,
            )
            .await;
    }
    let to = Utc::now();

    let entries = reported(&mut booted, "brenn:report-cam", CAM, 1).await;
    let entry = &entries[0];
    assert_eq!(entry["content_type"], "application/octet-stream", "{entry}");
    assert_eq!(entry["hex"], hex(NOT_UTF8), "{entry}");
    assert_mqtt_transport(entry, "cam/front", from, to);

    // A typed payload behind the dropped one: once the fixture has reported
    // it, the dropped one would have been in the same window had it been
    // stored.
    router
        .deliver_inbound(
            CLIENT,
            "raw/front",
            InboundPayload::new(b"ok".to_vec(), Some("text/plain".to_string())),
            1,
        )
        .await;
    let entries = reported(&mut booted, "brenn:report-raw", RAW, 1).await;
    assert_eq!(entries[0]["content_type"], "text/plain", "{entries:?}");
    assert_eq!(entries[0]["hex"], hex(b"ok"), "{entries:?}");
    assert_eq!(
        stored(&booted, RAW).await,
        1,
        "the untyped payload was dropped"
    );
    for body in bodies_on(&booted.messenger, "brenn:report-raw").await {
        assert!(!body.contains(&hex(NOT_UTF8)), "never reported: {body}");
    }
}

/// On a wrapped channel the payload arrives inside the `MqttEnvelope`:
/// `mqtt_body()` reads it, and the transport accessors refuse the message.
#[tokio::test(flavor = "multi_thread")]
async fn a_wrapped_channel_is_read_through_mqtt_body_and_refuses_the_transport_accessors() {
    let (_tree, _components, mut booted, router) = boot_ingress().await;

    router
        .deliver_inbound(
            CLIENT,
            "plain/front",
            InboundPayload::new(b"21.5".to_vec(), None),
            1,
        )
        .await;

    let entries = reported(&mut booted, "brenn:report-plain", PLAIN, 1).await;
    let entry = &entries[0];
    assert_eq!(entry["header"].get("transport"), None, "{entry}");
    assert_eq!(entry["content_type"], Value::Null, "{entry}");
    let wrapper = &entry["mqtt_body"]["ok"];
    assert_eq!(wrapper["client_slug"], CLIENT, "{entry}");
    assert_eq!(wrapper["topic"], "plain/front", "{entry}");
    assert_eq!(wrapper["payload"], json!({ "text": "21.5" }), "{entry}");
    let failed = json!({ "err": "processing-failed" });
    for accessor in ["mqtt_transport", "webhook_transport", "webhook_body"] {
        assert_eq!(entry[accessor], failed, "{accessor}: {entry}");
    }
}

// ── Webhook ──────────────────────────────────────────────────────────────

/// Raw: its channel is tuned `non_text`, its endpoint types bodies
/// `application/octet-stream`.
const INBOX: &str = "webhook:inbox";
/// Wrapped: no tuning block, the default `application/json` endpoint.
const PLAINHOOK: &str = "webhook:plainhook";
/// The bearer token both endpoints verify, under the token id `phone`.
const TOKEN: &str = "s3cret";

/// The document: the two endpoints over the token in `secret`, the raw
/// tuning, and one fixture instance owning each endpoint, reporting on
/// `brenn:report-<slug>`.
fn webhook_document(secret: &std::path::Path) -> String {
    let secret = secret.display();
    let blocks: String = [
        (
            "inbox",
            "    content_type = \"application/octet-stream\";\n",
        ),
        ("plainhook", ""),
    ]
    .iter()
    .map(|(slug, content_type)| {
        format!(
            "webhook {slug} {{\n    mount = \"/webhooks/{slug}\";\n    \
             transport_ceiling_bytes = 65536;\n{content_type}    signature {{\n        \
             scheme = bearer-token;\n        header = \"authorization\";\n    }}\n    \
             token phone {{ secret_file = \"{secret}\"; }}\n}}\n\
             channel report_{slug} at \"brenn:report-{slug}\" {{\n    push_depth = 1;\n    \
             retain_depth = 16;\n    standing_retain_depth = 16;\n}}\n\
             new feeder-{slug}: Feeder {{\n    grants = [ports, log];\n    \
             in feed <- \"webhook:{slug}\" {{ push_depth = 4; retain_depth = 4; }}\n    \
             out report -> report_{slug};\n}}\n"
        )
    })
    .collect();
    document_subscribing_acl(
        &format!(
            r#"channel at "{INBOX}" {{
    push_depth = 1;
    retain_depth = 16;
    standing_retain_depth = 16;
    non_text = true;
}}
{PACKAGED}component Feeder {{
    abi = processor;
    requires = [ports, log];
    in feed;
    out report;
}}
{PACKAGED}
{blocks}"#
        ),
        &[],
        &[],
    )
}

/// The booted process, with the dispatcher running and the server's router
/// serving its endpoint table over TCP, the real webhook event router
/// publishing through its messenger. Answers the server's address.
async fn boot_webhooks() -> (Tree, tempfile::TempDir, Booted, std::net::SocketAddr) {
    let tree = Tree::new();
    let secret = tree.secret("inbox.token", TOKEN);
    tree.write(&webhook_document(&secret));
    let components = tempfile::tempdir().expect("a components root");
    install_package_from(
        components.path(),
        &staged_module(&tree),
        "brenn_processor_sdk_bytes_test.wasm",
    );
    let booted = boot_with(
        &tree,
        BootFixture {
            components_roots: vec![components.path().to_path_buf()],
            dispatcher: true,
            ..BootFixture::default()
        },
    )
    .await;
    let mut state = brenn_server::test_support::state::test_state(&booted.db);
    state.messenger = Some(Arc::clone(&booted.messenger));
    state.webhook = Arc::clone(&booted.webhook);
    let events = Arc::new(WebhookEventRouterImpl::new());
    events.set_state(state.clone());
    booted.webhook.set_router(events);
    let app = brenn_server::router::build_router(state, None, 0, 2576)
        .into_make_service_with_connect_info::<std::net::SocketAddr>();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("a local port");
    let addr = listener.local_addr().expect("the bound address");
    tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("the test server serves");
    });
    (tree, components, booted, addr)
}

/// POST `body` to `/webhooks/<slug>` on `addr` as `content_type`, carrying
/// the bearer token; answers the response's status code.
async fn post(addr: std::net::SocketAddr, slug: &str, content_type: &str, body: &[u8]) -> u16 {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = tokio::net::TcpStream::connect(addr)
        .await
        .expect("the test server accepts");
    let head = format!(
        "POST /webhooks/{slug} HTTP/1.1\r\nhost: 127.0.0.1\r\ncontent-type: {content_type}\r\n\
         authorization: {TOKEN}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        body.len()
    );
    stream
        .write_all(head.as_bytes())
        .await
        .expect("the head is sent");
    stream.write_all(body).await.expect("the body is sent");
    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .await
        .expect("the response is read");
    let response = String::from_utf8_lossy(&response);
    response
        .strip_prefix("HTTP/1.1 ")
        .and_then(|rest| rest.get(..3))
        .expect("an HTTP/1.1 status line")
        .parse()
        .expect("a numeric status")
}

/// A non-UTF-8 body posted to a raw endpoint is read by the fixture
/// byte-exact under the endpoint's type, and `webhook_transport()` returns the
/// slug, the token id, the client address, the receive time and the headers
/// with the credential masked; every other accessor refuses it.
#[tokio::test(flavor = "multi_thread")]
async fn a_non_utf8_webhook_body_reaches_a_component_byte_exact_on_a_raw_endpoint() {
    let (_tree, _components, mut booted, addr) = boot_webhooks().await;

    let from = Utc::now();
    let status = post(addr, "inbox", "application/octet-stream", NOT_UTF8).await;
    let to = Utc::now();
    assert_eq!(status, 204);

    let entries = reported(&mut booted, "brenn:report-inbox", INBOX, 1).await;
    let entry = &entries[0];
    assert_eq!(entry["content_type"], "application/octet-stream", "{entry}");
    assert_eq!(entry["hex"], hex(NOT_UTF8), "{entry}");
    let transport = &entry["webhook_transport"]["ok"];
    assert_eq!(transport["endpoint_slug"], "inbox", "{entry}");
    assert_eq!(transport["key_id"], "phone", "{entry}");
    assert_eq!(transport["client_ip"], "127.0.0.1", "{entry}");
    let received: DateTime<Utc> = transport["received_at"]
        .as_str()
        .expect("a receive time")
        .parse()
        .expect("an RFC 3339 instant");
    assert!(
        from <= received && received <= to,
        "{received} in [{from}, {to}]"
    );
    let headers = transport["headers"].as_array().expect("headers");
    for (name, value) in [
        ("authorization", "[redacted]"),
        ("content-type", "application/octet-stream"),
    ] {
        assert!(
            headers.contains(&json!([name, value])),
            "{name}: {value} in {entry}"
        );
    }
    assert_eq!(
        entry["header"]["transport"],
        json!({ "webhook": transport })
    );
    let failed = json!({ "err": "processing-failed" });
    for accessor in ["mqtt_transport", "mqtt_body", "webhook_body"] {
        assert_eq!(entry[accessor], failed, "{accessor}: {entry}");
    }
}

/// On a wrapped endpoint the request arrives inside the `WebhookEnvelope`:
/// `webhook_body()` reads it, and the transport accessors refuse the message.
#[tokio::test(flavor = "multi_thread")]
async fn a_wrapped_endpoint_is_read_through_webhook_body_and_refuses_the_transport_accessors() {
    let (_tree, _components, mut booted, addr) = boot_webhooks().await;

    let body = br#"{"kind":"ping"}"#;
    assert_eq!(post(addr, "plainhook", "application/json", body).await, 204);

    let entries = reported(&mut booted, "brenn:report-plainhook", PLAINHOOK, 1).await;
    let entry = &entries[0];
    assert_eq!(entry["header"].get("transport"), None, "{entry}");
    let wrapper = &entry["webhook_body"]["ok"];
    assert_eq!(wrapper["endpoint_slug"], "plainhook", "{entry}");
    assert_eq!(wrapper["key_id"], "phone", "{entry}");
    assert_eq!(wrapper["body"], r#"{"kind":"ping"}"#, "{entry}");
    let failed = json!({ "err": "processing-failed" });
    for accessor in ["mqtt_transport", "webhook_transport", "mqtt_body"] {
        assert_eq!(entry[accessor], failed, "{accessor}: {entry}");
    }
}
