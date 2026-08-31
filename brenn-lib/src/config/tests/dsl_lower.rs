//! Lowering a derived `.brenn` document to a [`BrennConfig`].
//!
//! The shape of every test here is the same claim: a `.brenn` document lowers to
//! the `BrennConfig` written out beside it. The expected value is stated as a
//! Rust literal, so the assertion runs against what the runtime will actually
//! see — everything downstream of `load_config` is provenance-blind.
//!
//! Whole-struct equality is the default, and deliberately so: it is what catches
//! a lowering arm setting a field the document never stated.

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;

use brenn_dsl::diag::{Diagnostic, render_all};
use brenn_dsl::{processor_needs, surface_any};
use brenn_surface_schema::LogLevel;

use crate::access::raw::{
    AppAclRaw, ChannelMatcherRaw, MqttClientMatcherRaw, MqttSubMatcherRaw, WebhookMatcherRaw,
};
use crate::config::alerting::{AlertingConfig, MailConfig, NtfyConfig, default_subject_label};
use crate::config::app::AppConfigRaw;
use crate::config::attachment::{
    AttachmentHandlerConfig, AttachmentTargetRaw, default_timeout_secs,
};
use crate::config::automation::AutomationGlobalConfig;
use crate::config::claude_defaults::ClaudeDefaultsConfig;
use crate::config::container::{ContainerConfig, default_container_home};
use crate::config::events::EventsConfig;
use crate::config::hooks::{PostPullHooksConfig, StartHooksConfig, StartupHooksConfig};
use crate::config::llm_chat::LlmChatConfig;
use crate::config::logging::{LevelFilter, LoggingConfig};
use crate::config::mcp::McpServerConfig;
use crate::config::observability::{ObservabilityConfig, UsageObservabilityConfig};
use crate::config::repo::{AccessLevel, MountConfigRaw, RepoDeclRaw, RepoSyncConfig, default_true};
use crate::config::security::SecurityConfig;
use crate::config::server::{DatabaseConfig, ServerConfig};
use crate::config::surface_description::SurfaceDescriptionConfig;
use crate::config::wasm::WasmConfig;
use crate::config::watchdog::WatchdogConfig;
use crate::config::{BrennConfig, PACKAGED_MODULE, config_from_dsl, lower_document, sole_refusal};
use crate::messaging::AttachGrant;
use crate::messaging::ComponentGrant;
use crate::messaging::Urgency;
use crate::messaging::WakeMin;
use crate::messaging::config::{
    ChannelConfigRaw, Depth, LinkConfigRaw, LinkEndpointRaw, LinkHostRaw, MessagingConfigRaw,
    MessagingGlobalConfig, MessagingSubscriptionRaw, NoiseLevel, SendRate, Sink,
    SurfaceComponentRaw, SurfaceConfigRaw, SurfaceIoPortRaw, SurfaceOutputRaw,
    SurfaceSubscriptionRaw, WasmConsumerConfigRaw, WasmConsumerIoPortRaw,
    WasmConsumerMqttOutputRaw, WasmConsumerOutputRaw, WasmConsumerSubscriptionRaw,
};
use crate::messaging::remote::{RemoteConfigRaw, RemoteSubscribeAclRaw};
use crate::mqtt::config::{
    AppMqttIngressSubscriptionRaw, MqttClientConfigRaw, default_backoff_initial,
    default_backoff_max, default_client_urgency, default_inbound_payload_cap,
    default_subscription_qos, default_tls_version_min,
};
use crate::pwa_push::config::PwaPushGlobalConfig;
use crate::webhook::config::{
    AppWebhookSubscriptionRaw, ReplayProtectionConfigRaw, WebhookEndpointConfigRaw,
    WebhookKeyConfigRaw, WebhookSignatureConfigRaw, WebhookTokenConfigRaw, default_content_type,
    default_hmac_algorithm, default_transport_ceiling,
};
use brenn_envelope::grants::AppCapability;

/// The document lowers, and to exactly `expected`.
///
/// Every consumer's `spec_sha256` is checked against the document's own hash
/// first and then cleared, so the structural comparison below is written
/// against `expected` literals that state nothing about content hashes. A
/// document that fences no packaged module declares its classes in itself, so
/// the document's own hash is what every class in it carries.
fn assert_lowers(document: &str, expected: BrennConfig) {
    let mut actual = config_from_dsl(document);
    let document_hash = brenn_dsl::source_sha256(&crate::config::declaring_text(document));
    for consumer in &mut actual.wasm_consumers {
        assert_eq!(
            consumer.spec_sha256, document_hash,
            "consumer {:?} carries the hash of the file its class was declared in",
            consumer.slug
        );
        consumer.spec_sha256 = String::new();
    }
    for surface in &mut actual.surfaces {
        for component in &mut surface.components {
            assert_eq!(
                component.spec_sha256, document_hash,
                "surface {:?} component {:?} carries the hash of the file its class was declared in",
                surface.slug, component.instance
            );
            component.spec_sha256 = String::new();
        }
    }
    assert_eq!(actual, expected);
}

/// The one diagnostic the document produces.
fn refusal(document: &str) -> Diagnostic {
    sole_refusal(document)
}

/// Every diagnostic the document produces, for the few tests whose subject is
/// more than one of them.
fn refusals(document: &str) -> Vec<Diagnostic> {
    lower_document(document).expect_err("the document must be refused")
}

/// A channel block's lowered form with nothing stated but its address: what the
/// tests below start from and then fill in.
fn channel_at(address: &str) -> ChannelConfigRaw {
    ChannelConfigRaw {
        uuid: None,
        address: Some(address.to_string()),
        address_prefix: None,
        description: None,
        push_depth: None,
        retain_depth: None,
        standing_retain_depth: None,
        noise: None,
        sink: None,
        wake_min: None,
        send_rate: None,
    }
}

/// The same, for a tuning block keyed by an address prefix.
fn channel_at_prefix(prefix: &str) -> ChannelConfigRaw {
    ChannelConfigRaw {
        address: None,
        address_prefix: Some(prefix.to_string()),
        ..channel_at("unused")
    }
}

fn mqtt_client_at(slug: &str, url: &str) -> MqttClientConfigRaw {
    MqttClientConfigRaw {
        slug: slug.to_string(),
        url: url.to_string(),
        username: None,
        password_file: None,
        ca_file: None,
        tls_version_min: default_tls_version_min(),
        keepalive_secs: None,
        inbound_payload_cap_bytes: default_inbound_payload_cap(),
        last_will: None,
        reconnect_backoff_initial_secs: default_backoff_initial(),
        reconnect_backoff_max_secs: default_backoff_max(),
        qos: default_subscription_qos(),
        urgency: default_client_urgency(),
        session_expiry_secs: 0,
    }
}

fn alice_cmd_channel() -> ChannelConfigRaw {
    ChannelConfigRaw {
        uuid: Some("c88e5596-574b-53d1-9b55-6e612b8f3d49".to_string()),
        push_depth: Some(Depth::Bounded(8)),
        retain_depth: Some(Depth::Bounded(32)),
        standing_retain_depth: Some(Depth::Bounded(64)),
        ..channel_at("brenn:alice.cmd")
    }
}

fn cmd_subscriber(subscribe: MessagingSubscriptionRaw) -> AppConfigRaw {
    AppConfigRaw {
        slug: "alice".to_string(),
        grants: vec![AppCapability::MessagingSubscribe],
        messaging: Some(MessagingConfigRaw {
            subscribe: vec![subscribe],
            send_budget: None,
        }),
        acl: AppAclRaw {
            brenn_subscribe: vec![ChannelMatcherRaw::Exact("alice.cmd".to_string())],
            ..Default::default()
        },
        ..Default::default()
    }
}

fn bearer_token_endpoint(slug: &str) -> WebhookEndpointConfigRaw {
    WebhookEndpointConfigRaw {
        slug: slug.to_string(),
        mount: Some(format!("/webhooks/{slug}")),
        description: None,
        transport_ceiling_bytes: default_transport_ceiling(),
        content_type: default_content_type(),
        signature: WebhookSignatureConfigRaw::BearerToken {
            header: "authorization".to_string(),
            token_id_header: None,
        },
        keys: vec![],
        tokens: vec![WebhookTokenConfigRaw {
            token_id: "phone".to_string(),
            secret_file: PathBuf::from(format!("/home/alice/.secrets/{slug}.token")),
        }],
        replay_protection: None,
        urgency: None,
    }
}

/// A raw consumer expectation. Every consumer fixture here declares its class
/// in the fenced packaged half, so the package the lowering carries is that
/// module's name for all of them.
fn consumer(slug: &str) -> WasmConsumerConfigRaw {
    WasmConsumerConfigRaw::minimal(slug, PACKAGED_MODULE, &[])
}

fn attrless_subscription(port: &str, channel: &str) -> WasmConsumerSubscriptionRaw {
    WasmConsumerSubscriptionRaw {
        port: port.to_string(),
        channel: Some(channel.to_string()),
        push_depth: None,
        retain_depth: None,
        noise: None,
        wake_min: None,
        amplification: None,
    }
}

fn tick_io_port() -> WasmConsumerIoPortRaw {
    WasmConsumerIoPortRaw {
        port: "tick".to_string(),
        channel: None,
        push_depth: Some(Depth::Bounded(1)),
        retain_depth: Some(Depth::Bounded(2)),
        noise: None,
        amplification: None,
        urgency: None,
        publish_per_activation: None,
        publish_capacity: None,
    }
}

/// A surface block's lowered form with nothing stated but its wire slug and its
/// grants: no ACL entry, no component, no binding, no ceiling.
fn surface(slug: &str, grants: Vec<AttachGrant>) -> SurfaceConfigRaw {
    SurfaceConfigRaw {
        slug: slug.to_string(),
        grants,
        subscribe_acl: vec![],
        publish_acl: vec![],
        ephemeral_subscribe_acl: vec![],
        ephemeral_publish_acl: vec![],
        components: vec![],
        subscriptions: vec![],
        outputs: vec![],
        io_ports: vec![],
        skin: None,
        allowed_users: vec![],
        publish_burst: None,
        publish_per_sec: None,
    }
}

/// A surface-placed component instance whose instance id is its kind — what a
/// `new` handle that matches the class's lowercased name lowers to.
fn placed_component(kind: &str) -> SurfaceComponentRaw {
    // The hash `minimal` leaves empty is cleared by `assert_lowers` after it
    // checks the real one, like every consumer's.
    SurfaceComponentRaw {
        instance: Some(kind.to_string()),
        ..SurfaceComponentRaw::minimal(kind)
    }
}

/// An input binding stating nothing but the channel it reads and the port it
/// feeds.
fn surface_input(instance: &str, port: &str, channel: &str) -> SurfaceSubscriptionRaw {
    SurfaceSubscriptionRaw {
        channel: Some(channel.to_string()),
        instance: instance.to_string(),
        port: port.to_string(),
        push_depth: None,
        retain_depth: None,
        noise: None,
        wake_min: None,
    }
}

/// An output binding stating nothing but the port and the channel it writes.
fn surface_output(instance: &str, port: &str, channel: &str) -> SurfaceOutputRaw {
    SurfaceOutputRaw {
        instance: instance.to_string(),
        port: port.to_string(),
        channel: Some(channel.to_string()),
        urgency: None,
        publish_per_activation: None,
        publish_capacity: None,
    }
}

/// An `io` port on the surface's own auto channel: no address, no attr.
fn surface_io_port(instance: &str, port: &str) -> SurfaceIoPortRaw {
    SurfaceIoPortRaw {
        instance: instance.to_string(),
        port: port.to_string(),
        channel: None,
        push_depth: None,
        retain_depth: None,
        noise: None,
        urgency: None,
        publish_per_activation: None,
        publish_capacity: None,
    }
}

/// A remote block's lowered form with nothing stated but its slug and the token
/// file it authenticates against.
fn remote(slug: &str, token_file: &str) -> RemoteConfigRaw {
    RemoteConfigRaw {
        slug: slug.to_string(),
        token_file: PathBuf::from(token_file),
        grants: vec![],
        subscribe_acl: vec![],
        ephemeral_subscribe_acl: vec![],
        publish_acl: vec![],
        ephemeral_publish_acl: vec![],
        publish_burst: None,
        publish_per_sec: None,
        max_sessions: None,
        max_subscriptions: None,
    }
}

/// An exact subscribe entry with both ceilings, which a remote's entries always
/// carry.
fn remote_exact(channel: &str, push_depth: u64, retain_depth: u64) -> RemoteSubscribeAclRaw {
    RemoteSubscribeAclRaw {
        exact: Some(channel.to_string()),
        prefix: None,
        push_depth,
        retain_depth,
    }
}

/// The same, keyed by prefix.
fn remote_prefix(prefix: &str, push_depth: u64, retain_depth: u64) -> RemoteSubscribeAclRaw {
    RemoteSubscribeAclRaw {
        exact: None,
        prefix: Some(prefix.to_string()),
        push_depth,
        retain_depth,
    }
}

/// A webhook endpoint with no signature block yet: every optional attr absent
/// and the two defaulted scalars at their defaults. The caller supplies the
/// `signature` and whichever of `keys`/`tokens` its scheme reads.
fn webhook_endpoint(slug: &str) -> WebhookEndpointConfigRaw {
    WebhookEndpointConfigRaw {
        slug: slug.to_string(),
        mount: None,
        description: None,
        transport_ceiling_bytes: default_transport_ceiling(),
        content_type: default_content_type(),
        signature: WebhookSignatureConfigRaw::BearerToken {
            header: "unused".to_string(),
            token_id_header: None,
        },
        keys: vec![],
        tokens: vec![],
        replay_protection: None,
        urgency: None,
    }
}

fn webhook_key(key_id: &str, secret_file: &str) -> WebhookKeyConfigRaw {
    WebhookKeyConfigRaw {
        key_id: key_id.to_string(),
        secret_file: PathBuf::from(secret_file),
    }
}

fn webhook_token(token_id: &str, secret_file: &str) -> WebhookTokenConfigRaw {
    WebhookTokenConfigRaw {
        token_id: token_id.to_string(),
        secret_file: PathBuf::from(secret_file),
    }
}

/// A command handler with no `cc_instructions`, no file roles and the default
/// timeout: the shape a minimal `handler` block lowers to.
fn command_handler(program: &str, args: &[&str]) -> AttachmentHandlerConfig {
    AttachmentHandlerConfig::Command {
        program: program.to_string(),
        args: args.iter().map(|arg| arg.to_string()).collect(),
        file_roles: HashMap::new(),
        timeout_secs: default_timeout_secs(),
        cc_instructions: None,
    }
}

/// A config whose only content is its channel blocks.
fn config_with_channels(channels: Vec<ChannelConfigRaw>) -> BrennConfig {
    BrennConfig {
        channels,
        ..Default::default()
    }
}

// ---------------------------------------------------------------------------
// Channels and tunings
// ---------------------------------------------------------------------------

/// A durable channel carries a configured identity, and the DSL derives it from
/// the address rather than making the operator mint one. The literal below is
/// that derivation's answer, pinned here so a seed change in `brenn-dsl` breaks
/// a test outside `brenn-dsl` too — the row name of every persisted channel a
/// lowered config created depends on it.
#[test]
fn a_durable_channel_lowers_with_its_derived_uuid() {
    assert_lowers(
        r#"
channel alerts at "brenn:alice-alerts" {
    description = "Where alice's alerts land.";
    push_depth = 8;
    retain_depth = 128;
    standing_retain_depth = unbounded;
    noise = metered;
    sink = archive;
    wake_min = low;
    send_rate = { burst = 4, refill_interval_secs = 60, refill = 2 };
}
"#,
        config_with_channels(vec![ChannelConfigRaw {
            uuid: Some("85a5cf7e-6874-5766-9d69-712784754a1f".to_string()),
            description: Some("Where alice's alerts land.".to_string()),
            push_depth: Some(Depth::Bounded(8)),
            retain_depth: Some(Depth::Bounded(128)),
            standing_retain_depth: Some(Depth::Unbounded),
            noise: Some(NoiseLevel::Metered),
            sink: Some(Sink::Archive),
            wake_min: Some(WakeMin::Low),
            send_rate: Some(SendRate {
                burst: 4,
                refill_interval_secs: 60,
                refill: 2,
            }),
            ..channel_at("brenn:alice-alerts")
        }]),
    );
}

/// A non-durable channel carries no configured identity — the runtime derives
/// one from its address — and states only the two depths it has.
#[test]
fn an_ephemeral_channel_lowers_without_a_uuid() {
    assert_lowers(
        r#"
channel presence at "ephemeral:alice-desk.presence" {
    push_depth = 4;
    retain_depth = 16;
}
"#,
        config_with_channels(vec![ChannelConfigRaw {
            push_depth: Some(Depth::Bounded(4)),
            retain_depth: Some(Depth::Bounded(16)),
            ..channel_at("ephemeral:alice-desk.presence")
        }]),
    );
}

/// The minimal body: every optional attr omitted. An omitted attr must stay
/// `None` in the lowered block — inheriting from the global defaults is the
/// runtime's job, not lowering's.
#[test]
fn a_local_channel_with_a_minimal_body_states_only_its_depths() {
    assert_lowers(
        r#"
channel scratch at "local:alice-scratch" {
    push_depth = 1;
    retain_depth = 1;
}
"#,
        config_with_channels(vec![ChannelConfigRaw {
            push_depth: Some(Depth::Bounded(1)),
            retain_depth: Some(Depth::Bounded(1)),
            ..channel_at("local:alice-scratch")
        }]),
    );
}

/// A doctype is a compile-time expectation checked against the component ports
/// bound to the channel, so it reaches no runtime field: a channel stating one
/// lowers exactly as the same channel without it.
#[test]
fn a_channel_doctype_reaches_no_lowered_field() {
    assert_lowers(
        r#"
channel scratch at "local:alice-scratch" {
    push_depth = 1;
    retain_depth = 1;
    doctype = "alice.scratch@1";
}
"#,
        config_with_channels(vec![ChannelConfigRaw {
            push_depth: Some(Depth::Bounded(1)),
            retain_depth: Some(Depth::Bounded(1)),
            ..channel_at("local:alice-scratch")
        }]),
    );
}

/// A tuning block keyed by a whole address tunes the one channel the system
/// mints at it; it is not a declaration, so it carries no uuid.
#[test]
fn a_tuning_at_an_address_lowers_to_an_address_keyed_entry() {
    assert_lowers(
        r#"
channel at "brenn:tool-results/alice" {
    push_depth = 2;
    retain_depth = 32;
    standing_retain_depth = 32;
}
"#,
        config_with_channels(vec![ChannelConfigRaw {
            push_depth: Some(Depth::Bounded(2)),
            retain_depth: Some(Depth::Bounded(32)),
            standing_retain_depth: Some(Depth::Bounded(32)),
            ..channel_at("brenn:tool-results/alice")
        }]),
    );
}

/// A tuning keyed by a prefix covers a whole family of dynamically named
/// channels, and lands in `address_prefix` rather than `address`.
#[test]
fn a_tuning_at_a_prefix_lowers_to_a_prefix_keyed_entry() {
    assert_lowers(
        r#"
channel at prefix "brenn:tool-results/" {
    push_depth = 2;
    retain_depth = 32;
    standing_retain_depth = 32;
}
"#,
        config_with_channels(vec![ChannelConfigRaw {
            push_depth: Some(Depth::Bounded(2)),
            retain_depth: Some(Depth::Bounded(32)),
            standing_retain_depth: Some(Depth::Bounded(32)),
            ..channel_at_prefix("brenn:tool-results/")
        }]),
    );
}

// ---------------------------------------------------------------------------
// Refusals
// ---------------------------------------------------------------------------

/// A word that spells no variant of the target enum is refused at the word
/// itself, with the legal spellings named. The spellings come from the enum's
/// own `Deserialize`, so there is no second table here to drift.
#[test]
fn a_word_spelling_no_noise_level_is_refused_at_the_word() {
    let refusal = refusal(
        r#"
channel alerts at "brenn:alice-alerts" {
    push_depth = 1;
    retain_depth = 1;
    standing_retain_depth = 1;
    noise = loud;
}
"#,
    );
    let message = refusal.render();
    assert!(message.contains("`noise`"), "{message}");
    assert!(message.contains("silent"), "{message}");
    assert!(message.contains("metered"), "{message}");
    assert!(
        message.contains("main.brenn:6:13:"),
        "the span is the offending word's own: {message}"
    );
}

/// A depth is a non-negative count or the word `unbounded`; a negative count is
/// neither, and the refusal says so where it was written.
#[test]
fn a_negative_depth_is_refused() {
    let refusal = refusal(
        r#"
channel presence at "ephemeral:alice-desk.presence" {
    push_depth = -1;
    retain_depth = 1;
}
"#,
    );
    let message = refusal.render();
    assert!(message.contains("`push_depth`"), "{message}");
    assert!(message.contains("unbounded"), "{message}");
}

/// A word other than `unbounded` in a depth position is refused the same way a
/// negative count is.
#[test]
fn a_word_other_than_unbounded_is_refused_in_a_depth_position() {
    let refusal = refusal(
        r#"
channel presence at "ephemeral:alice-desk.presence" {
    push_depth = infinite;
    retain_depth = 1;
}
"#,
    );
    let message = refusal.render();
    assert!(message.contains("`push_depth`"), "{message}");
    assert!(message.contains("`infinite`"), "{message}");
}

/// `send_rate` is a table with no vocabulary behind it, so its keys are matched
/// by hand — and a stray key is refused at its own token, so a typo inside the
/// table is caught where it was written.
#[test]
fn a_stray_send_rate_key_is_refused_with_the_legal_set() {
    let refusal = refusal(
        r#"
channel presence at "ephemeral:alice-desk.presence" {
    push_depth = 1;
    retain_depth = 1;
    send_rate = { burst = 4, refil = 2 };
}
"#,
    );
    let message = refusal.render();
    assert!(message.contains("`refil`"), "{message}");
    assert!(message.contains("`refill`"), "{message}");
    assert!(message.contains("`burst`"), "{message}");
}

/// A `send_rate` that states only some of its keys gets `SendRate::default()`
/// for the rest.
#[test]
fn a_partial_send_rate_table_keeps_the_defaults_for_the_rest() {
    assert_lowers(
        r#"
channel presence at "ephemeral:alice-desk.presence" {
    push_depth = 1;
    retain_depth = 1;
    send_rate = { burst = 4 };
}
"#,
        config_with_channels(vec![ChannelConfigRaw {
            push_depth: Some(Depth::Bounded(1)),
            retain_depth: Some(Depth::Bounded(1)),
            send_rate: Some(SendRate {
                burst: 4,
                ..SendRate::default()
            }),
            ..channel_at("ephemeral:alice-desk.presence")
        }]),
    );
}

/// Every refusal in a document is reported, not just the first: lowering
/// accumulates and fails at the end.
#[test]
fn every_bad_value_in_a_document_is_reported() {
    let errors = refusals(
        r#"
channel presence at "ephemeral:alice-desk.presence" {
    push_depth = -1;
    retain_depth = -2;
    noise = loud;
}
"#,
    );
    // The identity of the three refusals is the substance; a count alone would
    // pass on one refusal reported three times.
    let mut keys: Vec<&str> = errors
        .iter()
        .map(|error| match &error.message {
            message if message.starts_with("`push_depth`") => "push_depth",
            message if message.starts_with("`retain_depth`") => "retain_depth",
            message if message.starts_with("`noise`") => "noise",
            message => panic!("unexpected refusal: {message}"),
        })
        .collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        ["noise", "push_depth", "retain_depth"],
        "{}",
        render_all(&errors)
    );
}

/// A path that is not a `.brenn` file is not this module's concern, but a
/// document that compiles to nothing lowers to the default config — the empty
/// case has to be the identity, or every equivalence test above is measuring
/// the wrong thing.
#[test]
fn an_empty_document_lowers_to_the_default_config() {
    assert_eq!(config_from_dsl(""), BrennConfig::default());
}

// ---------------------------------------------------------------------------
// Configuration sections
// ---------------------------------------------------------------------------

/// Every scalar section, every key stated, each value chosen away from its
/// default so a lowering line that dropped its value could not pass.
#[test]
fn every_section_lowers_with_every_key_stated() {
    assert_lowers(
        r#"
server {
    bind_address = "127.0.0.1:3100";
    static_dir = "/opt/brenn/frontend/dist";
    surface_dist_dir = "/opt/brenn/surface/dist";
    secure_cookies = false;
    trusted_proxy_hops = 1;
    pid_file = "/run/brenn/brenn.pid";
    public_url = "https://brenn.example.com";
}

database { path = "/var/lib/brenn/alice.db"; }

logging {
    log_dir = "/var/log/brenn";
    console_level = info;
    file_level = trace;
}

security {
    auth_rate_interval_secs = 7;
    auth_rate_burst = 11;
    global_rate_interval_secs = 2;
    global_rate_burst = 101;
    asset_rate_interval_secs = 3;
    asset_rate_burst = 2001;
    auth_body_limit = 4097;
    global_body_limit = 1048577;
    upload_body_limit = 26214401;
    max_image_long_edge = 2577;
}

alerting {
    max_alerts = 11;
    window_secs = 3601;
    ntfy { url = "https://ntfy.example.com/alice-alerts"; }
    mail {
        to = "alice@example.com";
        subject_label = "Alice's Brenn";
    }
}

claude_defaults {
    mcp_script_path = "/opt/brenn/alice_mcp.py";
    model = "opus";
}

repo_sync {
    repo_dir = "/home/alice/repos";
    poll_interval_secs = 301;
    stale_conversation_days = 8;
}

messaging {
    default_send_budget = 101;
    max_body_bytes = 65537;
    default_noise = metered;
    default_sink = archive;
    default_wake_min = high;
    default_send_rate = { burst = 5, refill_interval_secs = 61, refill = 3 };
    archive_path = "/var/lib/brenn/archive";
}

observability {
    surface_error_channel = "brenn:alice-surface-errors";
    surface_error_publish_floor = error;
    usage { session_gap_minutes = 45; }
}

surface_description {
    prefix = "alice-surface";
    status_interval_secs = 61;
}

llm_chat {
    prefix = "alice-chat";
    retained_window = 1001;
    wake_min = low;
    idle_timeout_secs = 301;
}

pwa_push {
    keypair_file = "/var/lib/brenn/vapid.json";
    subject = "mailto:alice@example.com";
    endpoint_host_allowlist = ["push.example.com", "push.example.org"];
    endpoint_host_allowlist_enforce = false;
}

automation {
    max_fires_per_hour_per_job = 61;
    max_error_reports_per_hour_per_job = 4;
    consecutive_failures_to_disable = 6;
    max_jobs_per_app = 51;
}

events { delivered_retention_days = 8; }

wasm { store_size_limit = "128MiB"; }

watchdog {
    sweep_interval_secs = 31;
    wedge_grace_secs = 61;
}

container cc {
    image = "brenn-cc:latest";
    home_dir = "/home/alice/container-home";
    container_home = "/home/bob";
    extra_mounts = ["/srv/shared:/srv/shared"];
    extra_args = ["--pids-limit", "512"];
}
"#,
        BrennConfig {
            server: ServerConfig {
                bind_address: "127.0.0.1:3100".parse().expect("a socket address"),
                static_dir: PathBuf::from("/opt/brenn/frontend/dist"),
                surface_dist_dir: PathBuf::from("/opt/brenn/surface/dist"),
                secure_cookies: false,
                trusted_proxy_hops: 1,
                pid_file: Some(PathBuf::from("/run/brenn/brenn.pid")),
                public_url: Some("https://brenn.example.com".to_string()),
            },
            database: DatabaseConfig {
                path: PathBuf::from("/var/lib/brenn/alice.db"),
            },
            logging: LoggingConfig {
                log_dir: PathBuf::from("/var/log/brenn"),
                console_level: LevelFilter::INFO,
                file_level: LevelFilter::TRACE,
            },
            security: SecurityConfig {
                auth_rate_interval_secs: 7,
                auth_rate_burst: 11,
                global_rate_interval_secs: 2,
                global_rate_burst: 101,
                asset_rate_interval_secs: 3,
                asset_rate_burst: 2001,
                auth_body_limit: 4097,
                global_body_limit: 1048577,
                upload_body_limit: 26214401,
                max_image_long_edge: 2577,
            },
            alerting: Some(AlertingConfig {
                max_alerts: 11,
                window_secs: 3601,
                ntfy: Some(NtfyConfig {
                    url: "https://ntfy.example.com/alice-alerts".to_string(),
                }),
                mail: Some(MailConfig {
                    to: "alice@example.com".to_string(),
                    subject_label: "Alice's Brenn".to_string(),
                }),
            }),
            claude_defaults: ClaudeDefaultsConfig {
                mcp_script_path: PathBuf::from("/opt/brenn/alice_mcp.py"),
                model: "opus".to_string(),
            },
            repo_sync: RepoSyncConfig {
                repo_dir: Some(PathBuf::from("/home/alice/repos")),
                poll_interval_secs: 301,
                stale_conversation_days: 8,
            },
            messaging: MessagingGlobalConfig {
                default_send_budget: 101,
                max_body_bytes: 65537,
                default_noise: NoiseLevel::Metered,
                default_sink: Sink::Archive,
                default_wake_min: WakeMin::High,
                default_send_rate: SendRate {
                    burst: 5,
                    refill_interval_secs: 61,
                    refill: 3,
                },
                archive_path: Some(PathBuf::from("/var/lib/brenn/archive")),
            },
            observability: ObservabilityConfig {
                usage: UsageObservabilityConfig {
                    session_gap_minutes: 45,
                },
                surface_error_channel: Some("brenn:alice-surface-errors".to_string()),
                surface_error_publish_floor: LogLevel::Error,
            },
            surface_description: SurfaceDescriptionConfig {
                prefix: "alice-surface".to_string(),
                status_interval_secs: 61,
            },
            llm_chat: LlmChatConfig {
                prefix: "alice-chat".to_string(),
                retained_window: 1001,
                wake_min: WakeMin::Low,
                idle_timeout_secs: 301,
            },
            pwa_push: PwaPushGlobalConfig {
                keypair_file: Some(PathBuf::from("/var/lib/brenn/vapid.json")),
                subject: Some("mailto:alice@example.com".to_string()),
                endpoint_host_allowlist: vec![
                    "push.example.com".to_string(),
                    "push.example.org".to_string(),
                ],
                endpoint_host_allowlist_enforce: false,
            },
            automation: AutomationGlobalConfig {
                max_fires_per_hour_per_job: 61,
                max_error_reports_per_hour_per_job: 4,
                consecutive_failures_to_disable: 6,
                max_jobs_per_app: 51,
            },
            events: EventsConfig {
                delivered_retention_days: 8,
            },
            wasm: WasmConfig {
                store_size_limit: "128MiB".to_string(),
            },
            watchdog: WatchdogConfig {
                sweep_interval_secs: 31,
                wedge_grace_secs: 61,
            },
            container: HashMap::from([(
                "cc".to_string(),
                ContainerConfig {
                    image: "brenn-cc:latest".to_string(),
                    home_dir: PathBuf::from("/home/alice/container-home"),
                    container_home: PathBuf::from("/home/bob"),
                    extra_mounts: vec!["/srv/shared:/srv/shared".to_string()],
                    extra_args: vec!["--pids-limit".to_string(), "512".to_string()],
                },
            )]),
            ..Default::default()
        },
    );
}

/// The minimal body of every section that has one: only the keys the target
/// requires. What the omitted keys get must be the target's own `Default`, which
/// is what pins lowering to the default impls rather than to restated literals.
#[test]
fn minimal_sections_take_their_targets_defaults() {
    assert_lowers(
        r#"
server { public_url = "https://brenn.example.com"; }
alerting {
    max_alerts = 5;
    window_secs = 60;
    mail { to = "alice@example.com"; }
}
observability { surface_error_channel = "brenn:alice-surface-errors"; }
pwa_push { subject = "mailto:alice@example.com"; }
container cc {
    image = "brenn-cc:latest";
    home_dir = "/home/alice/container-home";
}
"#,
        BrennConfig {
            server: ServerConfig {
                public_url: Some("https://brenn.example.com".to_string()),
                ..Default::default()
            },
            alerting: Some(AlertingConfig {
                max_alerts: 5,
                window_secs: 60,
                ntfy: None,
                mail: Some(MailConfig {
                    to: "alice@example.com".to_string(),
                    subject_label: default_subject_label(),
                }),
            }),
            observability: ObservabilityConfig {
                surface_error_channel: Some("brenn:alice-surface-errors".to_string()),
                ..Default::default()
            },
            pwa_push: PwaPushGlobalConfig {
                subject: Some("mailto:alice@example.com".to_string()),
                ..Default::default()
            },
            container: HashMap::from([(
                "cc".to_string(),
                ContainerConfig {
                    image: "brenn-cc:latest".to_string(),
                    home_dir: PathBuf::from("/home/alice/container-home"),
                    container_home: default_container_home(),
                    extra_mounts: vec![],
                    extra_args: vec![],
                },
            )]),
            ..Default::default()
        },
    );
}

/// A section that states a subset of its keys keeps the module's defaults for
/// the rest — the shape an operator actually writes, between the empty document
/// and the every-key row above.
///
/// The allowlist arm is security-relevant: an operator who states only the
/// keypair and subject must still get the default enforcement against the three
/// vendor hosts.
#[test]
fn a_partial_pwa_push_section_keeps_the_default_endpoint_allowlist() {
    let config = config_from_dsl(
        r#"
pwa_push {
    keypair_file = "/var/lib/brenn/vapid.json";
    subject = "mailto:alice@example.com";
}
"#,
    );
    assert_eq!(
        config.pwa_push,
        PwaPushGlobalConfig {
            keypair_file: Some(PathBuf::from("/var/lib/brenn/vapid.json")),
            subject: Some("mailto:alice@example.com".to_string()),
            endpoint_host_allowlist: vec![
                "fcm.googleapis.com".to_string(),
                "updates.push.services.mozilla.com".to_string(),
                "web.push.apple.com".to_string(),
            ],
            endpoint_host_allowlist_enforce: true,
        }
    );
}

/// The same property on a section whose keys are both plain scalars: the key the
/// document does not state keeps the module's default, not the type's.
#[test]
fn a_partial_watchdog_section_keeps_the_defaults_for_the_rest() {
    let config = config_from_dsl("watchdog { sweep_interval_secs = 10; }");
    assert_eq!(
        config.watchdog,
        WatchdogConfig {
            sweep_interval_secs: 10,
            wedge_grace_secs: 60,
        }
    );
}

/// A minimal section per configuration kindword: the required keys and nothing
/// else.
///
/// Lowering dispatches on the kindword string; an unhandled kindword panics on
/// a document the front end called valid. This table
/// is the tripwire for that: the test below asserts it covers every kindword the
/// language admits, and lowers all of them, so a kindword added to the
/// vocabulary is a red test rather than a boot panic.
const MINIMAL_SECTIONS: [(&str, &str); 18] = [
    (
        "server",
        r#"server { public_url = "https://brenn.example.com"; }"#,
    ),
    ("database", "database { }"),
    ("logging", "logging { }"),
    ("security", "security { }"),
    ("alerting", "alerting { max_alerts = 5; window_secs = 60; }"),
    ("claude_defaults", "claude_defaults { }"),
    ("repo_sync", "repo_sync { }"),
    ("messaging", "messaging { }"),
    ("observability", "observability { }"),
    ("surface_description", "surface_description { }"),
    ("llm_chat", "llm_chat { }"),
    ("pwa_push", "pwa_push { }"),
    ("automation", "automation { }"),
    ("events", "events { }"),
    ("wasm", "wasm { }"),
    ("watchdog", "watchdog { }"),
    (
        "container",
        r#"container cc { image = "brenn-cc:latest"; home_dir = "/home/alice/container-home"; }"#,
    ),
    ("integration", r#"integration graf { command = "graf"; }"#),
];

/// Every kindword the language admits reaches a lowering arm.
#[test]
fn every_configuration_section_kindword_lowers() {
    let mut covered: Vec<&str> = MINIMAL_SECTIONS
        .iter()
        .map(|(kindword, _)| *kindword)
        .collect();
    covered.sort_unstable();
    let mut admitted: Vec<&str> = brenn_dsl::model::CONFIG_BLOCK_KINDWORDS.to_vec();
    admitted.sort_unstable();
    assert_eq!(
        covered, admitted,
        "every configuration section kindword needs a row above, and a lowering arm"
    );

    let document: String = MINIMAL_SECTIONS
        .iter()
        .map(|(_, section)| format!("{section}\n"))
        .collect();
    config_from_dsl(&document);
}

/// Every row above whose body is empty — the sections with no required key —
/// states nothing at all, so a document of just those rows must lower to the
/// default config.
///
/// This is what keeps the table a coverage gate rather than a panic check: a
/// section arm that invents a value it was never given, or reaches for the
/// type's `Default` instead of the module's, differs from the default config
/// here.
#[test]
fn the_empty_bodied_configuration_sections_lower_to_the_defaults() {
    let empty_bodied: Vec<&str> = MINIMAL_SECTIONS
        .iter()
        .map(|(_, section)| *section)
        .filter(|section| section.ends_with("{ }"))
        .collect();
    assert_eq!(
        empty_bodied.len(),
        14,
        "empty-bodied rows: {empty_bodied:?}"
    );
    let document: String = empty_bodied
        .iter()
        .map(|section| format!("{section}\n"))
        .collect();
    assert_eq!(config_from_dsl(&document), BrennConfig::default());
}

/// A minimal document per sub-block kindword, in the parent that admits it.
///
/// Sub-blocks are looked up by name, so a kindword the language admits and no
/// arm reads lowers to nothing at all. Each row below is a document that must
/// lower, which is what makes the tables below coverage rather than a second
/// copy of the constants.
/// What a row's document must put in the lowered config for the row to count as
/// covered: a lowering arm that quietly drops its block would otherwise pass.
type BlockWitness = fn(&BrennConfig) -> bool;

/// The one app a sub-block row's document declares.
fn only_app(config: &BrennConfig) -> &crate::config::app::AppConfigRaw {
    config.apps.first().expect("the row declares one app")
}

const MINIMAL_ALERTING_BLOCKS: [(&str, &str, BlockWitness); 2] = [
    (
        "ntfy",
        r#"alerting { max_alerts = 5; window_secs = 60;
               ntfy { url = "https://ntfy.example.com/alice"; } }"#,
        |config| {
            config
                .alerting
                .as_ref()
                .is_some_and(|alerting| alerting.ntfy.is_some())
        },
    ),
    (
        "mail",
        r#"alerting { max_alerts = 5; window_secs = 60;
               mail { to = "alice@example.com"; } }"#,
        |config| {
            config
                .alerting
                .as_ref()
                .is_some_and(|alerting| alerting.mail.is_some())
        },
    ),
];

const MINIMAL_OBSERVABILITY_BLOCKS: [(&str, &str, BlockWitness); 1] = [(
    "usage",
    "observability { usage { session_gap_minutes = 45; } }",
    |config| config.observability.usage.session_gap_minutes == 45,
)];

const MINIMAL_AGENT_BLOCKS: [(&str, &str, BlockWitness); 6] = [
    (
        "start_hooks",
        r#"agent A() { start_hooks { host = ["git fetch"]; } }
           new alice: A();"#,
        |config| only_app(config).start_hooks.is_some(),
    ),
    (
        "post_pull_hooks",
        r#"agent A() { post_pull_hooks { host = ["cargo build"]; } }
           new alice: A();"#,
        |config| only_app(config).post_pull_hooks.is_some(),
    ),
    (
        "startup_hooks",
        r#"agent A() { startup_hooks { host = ["pf migrate"]; } }
           new alice: A();"#,
        |config| only_app(config).startup_hooks.is_some(),
    ),
    (
        "attachment_target",
        r#"agent A() {
               attachment_target import {
                   label = "Import";
                   accept = [".ofx"];
                   handler {
                       type = command;
                       program = "pf";
                       args = ["import"];
                       file_roles = { ofx = [".ofx"] };
                   }
               }
           }
           new alice: A();"#,
        |config| only_app(config).attachment_targets.len() == 1,
    ),
    (
        "integration_config",
        r#"agent A() { integration_config ledger { env = { LEDGER_DATA = "/srv/ledger" }; } }
           new alice: A();"#,
        |config| only_app(config).integration_config.contains_key("ledger"),
    ),
    (
        "tool",
        r#"agent A() { tool git-repo-pull { allow { repo = "ws"; } } }
           new alice: A();"#,
        |config| {
            only_app(config)
                .tool_grants
                .iter()
                .any(|grant| grant.tool == "git-repo-pull")
        },
    ),
];

/// Every sub-block kindword the language admits reaches a lowering arm, and the
/// arm reads the block rather than dropping it.
#[test]
fn every_sub_block_kindword_lowers() {
    for (admitted, table) in [
        (
            brenn_dsl::model::ALERTING_BLOCK_KINDWORDS,
            MINIMAL_ALERTING_BLOCKS.as_slice(),
        ),
        (
            brenn_dsl::model::OBSERVABILITY_BLOCK_KINDWORDS,
            MINIMAL_OBSERVABILITY_BLOCKS.as_slice(),
        ),
        (
            brenn_dsl::model::AGENT_BLOCK_KINDWORDS,
            MINIMAL_AGENT_BLOCKS.as_slice(),
        ),
    ] {
        let mut covered: Vec<&str> = table.iter().map(|(kindword, _, _)| *kindword).collect();
        covered.sort_unstable();
        let mut admitted: Vec<&str> = admitted.to_vec();
        admitted.sort_unstable();
        assert_eq!(
            covered, admitted,
            "every sub-block kindword needs a row above, and a lowering arm"
        );
        for (kindword, document, carried) in table {
            let config = config_from_dsl(document);
            assert!(
                carried(&config),
                "`{kindword}` lowered to nothing: its arm read the block and dropped it"
            );
        }
    }
}

// Lowering's multiplicity and "no sub-blocks" refusals are unreachable through
// the pipeline — resolution refuses them at check time, for section sub-blocks,
// webhook blocks and an agent's hook blocks alike. They stay as defense in
// depth; the resolve-side refusals are tested in brenn-dsl.

/// A token in a section reaches lowering as text, and goes through the same
/// enum `Deserialize` a channel's token does — so the refusal names the legal
/// spellings from serde's own tables.
#[test]
fn a_bad_section_token_names_the_legal_spellings() {
    let diagnostic = refusal("messaging { default_noise = loud; }");
    assert!(
        diagnostic.message.starts_with("`default_noise`:"),
        "{}",
        diagnostic.render()
    );
    assert!(
        diagnostic.message.contains("silent"),
        "the legal spellings are named: {}",
        diagnostic.message
    );
    assert_eq!(diagnostic.span.text_str(), Some("loud"));
}

/// A log level goes through the config module's own level parser, so lowering
/// refuses exactly what that parser refuses.
#[test]
fn a_bad_log_level_is_refused_by_the_configs_own_parser() {
    let diagnostic = refusal("logging { console_level = chatty; }");
    assert!(
        diagnostic.message.contains("invalid log level"),
        "{}",
        diagnostic.render()
    );
    assert_eq!(diagnostic.span.text_str(), Some("chatty"));
}

/// Every level word an operator can write, and the case-insensitivity of the
/// spelling. `off` is the one that silences a plane outright, and it is the
/// reason this row enumerates instead of sampling.
#[test]
fn every_log_level_word_lowers_to_its_filter() {
    for (word, expected) in [
        ("trace", LevelFilter::TRACE),
        ("debug", LevelFilter::DEBUG),
        ("info", LevelFilter::INFO),
        ("warn", LevelFilter::WARN),
        ("error", LevelFilter::ERROR),
        ("off", LevelFilter::OFF),
        ("Info", LevelFilter::INFO),
        ("WARN", LevelFilter::WARN),
    ] {
        let config = config_from_dsl(&format!(
            "logging {{ console_level = {word}; file_level = {word}; }}"
        ));
        assert_eq!(
            config.logging.console_level, expected,
            "console_level = {word}"
        );
        assert_eq!(config.logging.file_level, expected, "file_level = {word}");
    }
}

/// A value of the wrong shape is refused at the value, saying what it found.
#[test]
fn a_string_where_a_count_belongs_is_refused() {
    let diagnostic = refusal(r#"security { auth_rate_burst = "eleven"; }"#);
    assert_eq!(
        diagnostic.message,
        "`auth_rate_burst`: expected an integer, got a string"
    );
}

/// A count too large for the field it targets is refused rather than
/// truncated: the narrowing is the target type's own range.
#[test]
fn a_count_out_of_the_targets_range_is_refused() {
    let diagnostic =
        refusal("server { public_url = \"https://brenn.example.com\"; trusted_proxy_hops = 300; }");
    assert_eq!(
        diagnostic.message,
        "`trusted_proxy_hops`: 300 is out of range for this key"
    );
}

// ---------------------------------------------------------------------------
// Repos and mqtt clients
// ---------------------------------------------------------------------------

/// A repo is a top-level declaration whose handle is its wire slug, and an
/// agent reaches one through a `mount` statement whose tail is the mount's own
/// body.
#[test]
fn repos_and_the_mounts_that_reach_them_lower() {
    assert_lowers(
        r#"
repo life {
    remote = "forgejo@example.com:alice/life.git";
    auto_pull = true;
}

repo notes {
    remote = "forgejo@example.com:alice/notes.git";
    auto_pull = false;
}

agent Assistant() {
    name = "Assistant";

    mount life { working_dir = true; primary = true; }
    mount notes { access = read-only; auto_pull = false; }
}

new alice: Assistant();
"#,
        BrennConfig {
            repos: vec![
                RepoDeclRaw {
                    slug: "life".to_string(),
                    remote: "forgejo@example.com:alice/life.git".to_string(),
                    auto_pull: true,
                },
                RepoDeclRaw {
                    slug: "notes".to_string(),
                    remote: "forgejo@example.com:alice/notes.git".to_string(),
                    auto_pull: false,
                },
            ],
            apps: vec![AppConfigRaw {
                slug: "alice".to_string(),
                name: Some("Assistant".to_string()),
                mounts: vec![
                    MountConfigRaw {
                        repo: "life".to_string(),
                        access: AccessLevel::default(),
                        working_dir: true,
                        auto_pull: None,
                        primary: true,
                    },
                    MountConfigRaw {
                        repo: "notes".to_string(),
                        access: AccessLevel::ReadOnly,
                        working_dir: false,
                        auto_pull: Some(false),
                        primary: false,
                    },
                ],
                ..Default::default()
            }],
            ..Default::default()
        },
    );
}

/// The minimal repo body: only the remote, with `auto_pull` taking its default.
#[test]
fn a_minimal_repo_takes_its_default_auto_pull() {
    assert_lowers(
        r#"
repo life {
    remote = "forgejo@example.com:alice/life.git";
}
"#,
        BrennConfig {
            repos: vec![RepoDeclRaw {
                slug: "life".to_string(),
                remote: "forgejo@example.com:alice/life.git".to_string(),
                auto_pull: default_true(),
            }],
            ..Default::default()
        },
    );
}

/// Only entries that state at least one budget key produce an override block;
/// an unbudgeted entry must not emit one (would collide with the default row
/// at boot).
#[test]
fn an_mqtt_sink_override_is_lowered_per_budgeted_entry() {
    let document = format!(
        concat!(
            "mqtt_client plain {{ url = \"mqtts://plain.example.com:8883\"; }}\n",
            "mqtt_client half {{ url = \"mqtts://half.example.com:8883\"; }}\n",
            "mqtt_client full {{ url = \"mqtts://full.example.com:8883\"; }}\n",
            "channel feed at \"brenn:feed\" {{\n",
            "    push_depth = 1; retain_depth = 1; standing_retain_depth = 1;\n",
            "}}\n",
            "// ── packaged ──\n",
            "component Sink {{\n{}\n    in feed;\n    out echo;\n}}\n",
            "// ── packaged ──\n",
            "new sink: Sink {{\n",
            "    grants = [ports, mqtt];\n",
            "    acl subscribe [ exact feed ];\n",
            "    in feed <- feed {{ push_depth = 1; retain_depth = 1; }}\n",
            "    out echo -> feed;\n",
            "    acl publish [\n",
            "        exact feed,\n",
            "        client \"mqtt:plain\",\n",
            "        client \"mqtt:half\" {{ publish_per_activation = 2.5 }},\n",
            "        client \"mqtt:full\" {{ publish_per_activation = 2, publish_capacity = 3.5 }}\n",
            "    ];\n",
            "}}\n",
        ),
        processor_needs!("ports, mqtt"),
    );
    let config = config_from_dsl(&document);
    assert_eq!(
        config.wasm_consumers[0].mqtt_outputs,
        vec![
            WasmConsumerMqttOutputRaw {
                client: "half".to_string(),
                publish_per_activation: Some(2.5),
                publish_capacity: None,
            },
            WasmConsumerMqttOutputRaw {
                client: "full".to_string(),
                publish_per_activation: Some(2.0),
                publish_capacity: Some(3.5),
            },
        ],
    );
}

/// Everything an mqtt client body can say, each value chosen away from its
/// default so a dropped key shows up as a difference.
#[test]
fn an_mqtt_client_lowers_every_key_it_states() {
    assert_lowers(
        r#"
mqtt_client broker {
    url = "mqtts://broker.example.com:8883";
    username = "alice";
    password_file = "/home/alice/.secrets/broker.password";
    ca_file = "/home/alice/.secrets/broker-ca.pem";
    tls_version_min = "1.3";
    keepalive_secs = 30;
    inbound_payload_cap_bytes = 262144;
    reconnect_backoff_initial_secs = 2;
    reconnect_backoff_max_secs = 120;
    session_expiry_secs = 300;
    qos = 2;
    urgency = high;
}
"#,
        BrennConfig {
            mqtt_clients: vec![MqttClientConfigRaw {
                slug: "broker".to_string(),
                url: "mqtts://broker.example.com:8883".to_string(),
                username: Some("alice".to_string()),
                password_file: Some(PathBuf::from("/home/alice/.secrets/broker.password")),
                ca_file: Some(PathBuf::from("/home/alice/.secrets/broker-ca.pem")),
                tls_version_min: "1.3".to_string(),
                keepalive_secs: Some(30),
                inbound_payload_cap_bytes: 262144,
                last_will: None,
                reconnect_backoff_initial_secs: 2,
                reconnect_backoff_max_secs: 120,
                qos: 2,
                urgency: Urgency::High,
                session_expiry_secs: 300,
            }],
            ..Default::default()
        },
    );
}

/// The minimal mqtt client body: the broker url alone. Every other field is
/// filled from the default function the config module owns — which function
/// fills which field is what this row locks; the values those functions return
/// are pinned in `invariants.rs`.
#[test]
fn a_minimal_mqtt_client_takes_every_default() {
    assert_lowers(
        r#"
mqtt_client broker {
    url = "mqtts://broker.example.com:8883";
}
"#,
        BrennConfig {
            mqtt_clients: vec![MqttClientConfigRaw {
                slug: "broker".to_string(),
                url: "mqtts://broker.example.com:8883".to_string(),
                username: None,
                password_file: None,
                ca_file: None,
                tls_version_min: default_tls_version_min(),
                keepalive_secs: None,
                inbound_payload_cap_bytes: default_inbound_payload_cap(),
                last_will: None,
                reconnect_backoff_initial_secs: default_backoff_initial(),
                reconnect_backoff_max_secs: default_backoff_max(),
                qos: default_subscription_qos(),
                urgency: default_client_urgency(),
                session_expiry_secs: 0,
            }],
            ..Default::default()
        },
    );
}

// ---------------------------------------------------------------------------
// Agents
// ---------------------------------------------------------------------------

/// Every scalar attr an agent body can state, each value away from its default.
#[test]
fn an_agent_lowers_every_scalar_attr_it_states() {
    assert_lowers(
        r#"
agent Assistant() {
    slug = "alice-pa";
    name = "Personal Assistant";
    description = "The desk assistant.";
    icon = "assistant";
    working_dir = "/home/alice/work";
    model = "sonnet";
    single_instance = true;
    singleton = true;
    persistent = true;
    multiuser = true;
    idle_timeout_secs = 900;
    idle_hook_secs = 60;
    compact_reminder_pct = 60;
    compact_soft_pct = 70;
    compact_red_pct = 85;
    compact_hard_pct = 95;
    compact_reminder_tokens = 120000;
    compact_soft_tokens = 140000;
    compact_red_tokens = 170000;
    compact_hard_tokens = 190000;
    compact_idle_secs = 1800;
    history_replay_limit = 50;
    allowed_users = ["alice", "bob"];
    disabled_tools = ["WebSearch"];
    cc_extra_args = ["--verbose"];
    integrations = ["calendar"];
    extra_mounts = ["/home/alice/notes"];
    prefix_username = true;
    prefix_timestamp = false;
    prefix_device = true;
    container = "sandbox";
    container_working_dir = "/work";
}

new alice: Assistant();
"#,
        BrennConfig {
            apps: vec![AppConfigRaw {
                slug: "alice-pa".to_string(),
                name: Some("Personal Assistant".to_string()),
                description: Some("The desk assistant.".to_string()),
                icon: Some("assistant".to_string()),
                working_dir: Some(PathBuf::from("/home/alice/work")),
                model: Some("sonnet".to_string()),
                single_instance: true,
                singleton: true,
                persistent: true,
                multiuser: true,
                idle_timeout_secs: Some(900),
                idle_hook_secs: Some(60),
                compact_reminder_pct: Some(60),
                compact_soft_pct: Some(70),
                compact_red_pct: Some(85),
                compact_hard_pct: Some(95),
                compact_reminder_tokens: Some(120000),
                compact_soft_tokens: Some(140000),
                compact_red_tokens: Some(170000),
                compact_hard_tokens: Some(190000),
                compact_idle_secs: Some(1800),
                history_replay_limit: Some(50),
                allowed_users: vec!["alice".to_string(), "bob".to_string()],
                disabled_tools: vec!["WebSearch".to_string()],
                cc_extra_args: vec!["--verbose".to_string()],
                integrations: vec!["calendar".to_string()],
                extra_mounts: vec!["/home/alice/notes".to_string()],
                prefix_username: Some(true),
                prefix_timestamp: Some(false),
                prefix_device: Some(true),
                container: Some("sandbox".to_string()),
                container_working_dir: Some(PathBuf::from("/work")),
                ..Default::default()
            }],
            ..Default::default()
        },
    );
}

/// The minimal agent body: nothing but the wire slug the handle supplies. Every
/// list is empty, and `messaging` is absent because an agent with no
/// subscriptions and no budget has nothing to put in it.
#[test]
fn a_minimal_agent_states_only_its_slug() {
    assert_lowers(
        r#"
agent Assistant() {
}

new alice: Assistant();
"#,
        BrennConfig {
            apps: vec![AppConfigRaw {
                slug: "alice".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        },
    );
}

/// Hooks are named sub-blocks, one per point in the agent's life; mcp servers
/// arrive either as a reference to a top-level definition or as a body defined
/// inside the agent.
#[test]
fn an_agent_lowers_its_hook_blocks_and_both_mcp_forms() {
    assert_lowers(
        r#"
mcp_server graf {
    command = "graf";
    args = ["mcp"];
    env = { GRAF_ROOT = "/home/alice/kb" };
}

agent Assistant() {
    mcp_server graf;
    mcp_server pfin {
        command = "pf";
        args = ["mcp", "--quiet"];
    }

    start_hooks {
        host = ["git fetch"];
        container = ["pf rebuild"];
    }
    post_pull_hooks {
        host = ["cargo build"];
    }
    startup_hooks {
        container = ["pf migrate"];
    }
}

new alice: Assistant();
"#,
        BrennConfig {
            apps: vec![AppConfigRaw {
                slug: "alice".to_string(),
                mcp_servers: HashMap::from([
                    (
                        "graf".to_string(),
                        McpServerConfig {
                            command: "graf".to_string(),
                            args: vec!["mcp".to_string()],
                            env: HashMap::from([(
                                "GRAF_ROOT".to_string(),
                                "/home/alice/kb".to_string(),
                            )]),
                        },
                    ),
                    (
                        "pfin".to_string(),
                        McpServerConfig {
                            command: "pf".to_string(),
                            args: vec!["mcp".to_string(), "--quiet".to_string()],
                            env: HashMap::new(),
                        },
                    ),
                ]),
                start_hooks: Some(StartHooksConfig {
                    host: vec!["git fetch".to_string()],
                    container: vec!["pf rebuild".to_string()],
                }),
                post_pull_hooks: Some(PostPullHooksConfig {
                    host: vec!["cargo build".to_string()],
                    container: vec![],
                }),
                startup_hooks: Some(StartupHooksConfig {
                    host: vec![],
                    container: vec!["pf migrate".to_string()],
                }),
                ..Default::default()
            }],
            ..Default::default()
        },
    );
}

/// Per-integration overrides are named sub-blocks with open bodies, one per map
/// key, carrying an opaque value tree: a nested table, a scalar, and a list all
/// land as their own `toml::Value`.
///
/// The block only overrides; naming an integration here implicitly enables it.
/// This test does not verify that enablement — the `integrations` list below is
/// the one the agent states, unchanged.
#[test]
fn an_agent_lowers_its_per_integration_config_blocks() {
    assert_lowers(
        r#"
agent Assistant() {
    integrations = ["ledger"];

    integration_config ledger {
        env = { LEDGER_DATA = "/home/alice/ledger", LEDGER_STRICT = "1" };
        timeout_secs = 30;
    }
    integration_config calendar {
        accounts = ["alice", "bob"];
    }
}

new alice: Assistant();
"#,
        BrennConfig {
            apps: vec![AppConfigRaw {
                slug: "alice".to_string(),
                integrations: vec!["ledger".to_string()],
                integration_config: HashMap::from([
                    (
                        "ledger".to_string(),
                        toml::Value::Table(toml::Table::from_iter([
                            (
                                "env".to_string(),
                                toml::Value::Table(toml::Table::from_iter([
                                    (
                                        "LEDGER_DATA".to_string(),
                                        toml::Value::String("/home/alice/ledger".to_string()),
                                    ),
                                    (
                                        "LEDGER_STRICT".to_string(),
                                        toml::Value::String("1".to_string()),
                                    ),
                                ])),
                            ),
                            ("timeout_secs".to_string(), toml::Value::Integer(30)),
                        ])),
                    ),
                    (
                        "calendar".to_string(),
                        toml::Value::Table(toml::Table::from_iter([(
                            "accounts".to_string(),
                            toml::Value::Array(vec![
                                toml::Value::String("alice".to_string()),
                                toml::Value::String("bob".to_string()),
                            ]),
                        )])),
                    ),
                ]),
                ..Default::default()
            }],
            ..Default::default()
        },
    );
}

/// The three subscription families ride one statement form, dispatched on the
/// scheme of the address it names, and `send_budget` nests into the app's
/// `messaging` table. Each subscription also derives the ACL entry that
/// authorizes it, which is why the expected config spells them.
#[test]
fn an_agent_lowers_all_three_subscription_families_and_its_send_budget() {
    assert_lowers(
        r#"
channel cmd at "brenn:alice.cmd" {
    push_depth = 8;
    retain_depth = 32;
    standing_retain_depth = 64;
}

channel presence at "ephemeral:alice.presence" { push_depth = 4; retain_depth = 8; }

mqtt_client broker {
    url = "mqtts://broker.example.com:8883";
}

webhook push_alice {
    slug = "push-alice";
    mount = "/webhooks/push-alice";

    signature {
        scheme = bearer-token;
        header = "authorization";
    }

    token phone { secret_file = "/home/alice/.secrets/push-alice.token"; }
}

agent Assistant() {
    grants = [subscribe];
    send_budget = 40;

    subscribe cmd { push_depth = 1000; retain_depth = 2000; noise = metered; wake_min = low; }
    subscribe presence { push_depth = 4; retain_depth = 8; }
    subscribe "webhook:push-alice" { push_depth = 10; retain_depth = 20; wake_min = normal; }
    subscribe "mqtt:broker:alice/lamp" { push_depth = 2; retain_depth = 4; noise = alarm; }
}

new alice: Assistant();
"#,
        BrennConfig {
            channels: vec![
                ChannelConfigRaw {
                    uuid: Some("c88e5596-574b-53d1-9b55-6e612b8f3d49".to_string()),
                    push_depth: Some(Depth::Bounded(8)),
                    retain_depth: Some(Depth::Bounded(32)),
                    standing_retain_depth: Some(Depth::Bounded(64)),
                    ..channel_at("brenn:alice.cmd")
                },
                ChannelConfigRaw {
                    push_depth: Some(Depth::Bounded(4)),
                    retain_depth: Some(Depth::Bounded(8)),
                    ..channel_at("ephemeral:alice.presence")
                },
            ],
            mqtt_clients: vec![mqtt_client_at("broker", "mqtts://broker.example.com:8883")],
            apps: vec![AppConfigRaw {
                slug: "alice".to_string(),
                grants: vec![
                    AppCapability::MessagingSubscribe,
                    AppCapability::EphemeralSubscribe,
                    AppCapability::MqttSubscribe,
                    AppCapability::Webhook,
                ],
                messaging: Some(MessagingConfigRaw {
                    send_budget: Some(40),
                    subscribe: vec![
                        MessagingSubscriptionRaw {
                            channel: "brenn:alice.cmd".to_string(),
                            push_depth: Some(Depth::Bounded(1000)),
                            retain_depth: Some(Depth::Bounded(2000)),
                            noise: Some(NoiseLevel::Metered),
                            wake_min: Some(WakeMin::Low),
                        },
                        MessagingSubscriptionRaw {
                            channel: "ephemeral:alice.presence".to_string(),
                            push_depth: Some(Depth::Bounded(4)),
                            retain_depth: Some(Depth::Bounded(8)),
                            noise: None,
                            wake_min: None,
                        },
                    ],
                }),
                webhook_subscriptions: vec![AppWebhookSubscriptionRaw {
                    endpoint: "push-alice".to_string(),
                    push_depth: Some(Depth::Bounded(10)),
                    retain_depth: Some(Depth::Bounded(20)),
                    wake_min: Some(WakeMin::Normal),
                }],
                mqtt_subscriptions: vec![AppMqttIngressSubscriptionRaw {
                    channel: "mqtt:broker:alice/lamp".to_string(),
                    push_depth: Some(Depth::Bounded(2)),
                    retain_depth: Some(Depth::Bounded(4)),
                    noise: Some(NoiseLevel::Alarm),
                    wake_min: None,
                }],
                acl: AppAclRaw {
                    brenn_subscribe: vec![ChannelMatcherRaw::Exact("alice.cmd".to_string())],
                    ephemeral_subscribe: vec![ChannelMatcherRaw::Exact(
                        "alice.presence".to_string(),
                    )],
                    mqtt_subscribe: vec![MqttSubMatcherRaw {
                        client: "broker".to_string(),
                        topic_filter: "alice/lamp".to_string(),
                    }],
                    webhook: vec![WebhookMatcherRaw {
                        endpoint: "push-alice".to_string(),
                    }],
                    ..Default::default()
                },
                ..Default::default()
            }],
            webhook_endpoints: vec![bearer_token_endpoint("push-alice")],
            ..Default::default()
        },
    );
}

/// The `[app.acl]` block has three provenances that all land in the same
/// lists: an `acl` statement in the agent's own body, the entry a subscription
/// derives, and a `grant` statement written about the agent from outside. The
/// patterns arrive scheme-stripped, which is how the raw config spells them.
#[test]
fn an_agent_lowers_acl_entries_from_every_provenance() {
    assert_lowers(
        r#"
channel notes at "brenn:shared.notes" {
    push_depth = 4;
    retain_depth = 8;
    standing_retain_depth = 16;
}

agent Assistant() {
    grants = [subscribe, publish];

    acl subscribe [prefix "brenn:alice.", exact "brenn:alice-errors"];
    acl publish [prefix "ephemeral:alice."];
}

new alice: Assistant();

grant alice publish exact notes;
"#,
        BrennConfig {
            channels: vec![ChannelConfigRaw {
                uuid: Some("46cec031-27ab-5416-b9ac-a72c8eb8a0d9".to_string()),
                push_depth: Some(Depth::Bounded(4)),
                retain_depth: Some(Depth::Bounded(8)),
                standing_retain_depth: Some(Depth::Bounded(16)),
                ..channel_at("brenn:shared.notes")
            }],
            apps: vec![AppConfigRaw {
                slug: "alice".to_string(),
                grants: vec![
                    AppCapability::MessagingSubscribe,
                    AppCapability::MessagingPublish,
                    AppCapability::EphemeralPublish,
                ],
                acl: AppAclRaw {
                    brenn_subscribe: vec![
                        ChannelMatcherRaw::Prefix("alice.".to_string()),
                        ChannelMatcherRaw::Exact("alice-errors".to_string()),
                    ],
                    brenn_publish: vec![ChannelMatcherRaw::Exact("shared.notes".to_string())],
                    ephemeral_publish: vec![ChannelMatcherRaw::Prefix("alice.".to_string())],
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        },
    );
}

// ---------------------------------------------------------------------------
// Refusals in the open positions the entity families read
// ---------------------------------------------------------------------------

/// A key the union vocabulary admits and the family the address selects has no
/// field for. The front end cannot know which family a `subscribe` statement
/// is — the address decides that, and resolution is where an address is
/// known — so this one is lowering's refusal, at the value's own token.
#[test]
fn a_noise_policy_on_a_webhook_subscription_is_refused() {
    let error = refusal(
        r#"
webhook push_alice {
    slug = "push-alice";
    mount = "/webhooks/push-alice";

    signature {
        scheme = bearer-token;
        header = "authorization";
    }

    token phone { secret_file = "/home/alice/.secrets/push-alice.token"; }
}

agent Assistant() {
    grants = [subscribe];
    subscribe "webhook:push-alice" { push_depth = 4; noise = metered; }
}

new alice: Assistant();
"#,
    );
    assert_eq!(
        error.message,
        "`noise` is not a key of a subscription to `webhook:push-alice`; expected \
         `push_depth`, `retain_depth` or `wake_min`"
    );
    assert_eq!(
        error.line_col(),
        Some((16, 62)),
        "the span is the refused key's own token: {}",
        error.render()
    );
}

/// A key this family does not read earns one diagnostic, not two: the refusal
/// is the whole answer, and lowering the same token again would add a spelling
/// complaint implying the key would be fine once respelled.
#[test]
fn a_refused_family_key_is_not_also_value_checked() {
    let error = refusal(
        r#"
webhook push_alice {
    slug = "push-alice";
    mount = "/webhooks/push-alice";

    signature {
        scheme = bearer-token;
        header = "authorization";
    }

    token phone { secret_file = "/home/alice/.secrets/push-alice.token"; }
}

agent Assistant() {
    grants = [subscribe];
    subscribe "webhook:push-alice" { push_depth = 4; noise = loud; }
}

new alice: Assistant();
"#,
    );
    assert!(
        error.message.starts_with("`noise` is not a key of"),
        "{}",
        error.render()
    );
}

/// A tail value in the wrong shape is refused where it was written, with the
/// spelling the target admits named.
#[test]
fn a_bad_token_in_a_subscription_tail_names_the_legal_spellings() {
    let error = refusal(
        r#"
channel cmd at "brenn:alice.cmd" {
    push_depth = 8;
    retain_depth = 32;
    standing_retain_depth = 64;
}

agent Assistant() {
    grants = [subscribe];
    subscribe cmd { noise = loud; }
}

new alice: Assistant();
"#,
    );
    assert!(
        error.message.starts_with("`noise`: unknown variant `loud`"),
        "{}",
        error.message
    );
}

/// A depth in a tail takes a count or the word `unbounded`, and nothing else —
/// the same two spellings a depth in a channel body takes.
#[test]
fn a_negative_depth_in_a_subscription_tail_is_refused() {
    let error = refusal(
        r#"
channel cmd at "brenn:alice.cmd" {
    push_depth = 8;
    retain_depth = 32;
    standing_retain_depth = 64;
}

agent Assistant() {
    grants = [subscribe];
    subscribe cmd { push_depth = -1; }
}

new alice: Assistant();
"#,
    );
    assert_eq!(
        error.message,
        "`push_depth`: expected a non-negative integer or the word `unbounded`, got -1"
    );
}

/// Two mcp servers under one key would be one server with two definitions, so
/// the second is refused and the first is cited.
#[test]
fn a_duplicate_mcp_key_in_one_agent_is_refused_at_both_sites() {
    let error = refusal(
        r#"
mcp_server graf {
    command = "graf";
}

agent Assistant() {
    mcp_server graf;
    mcp_server graf;
}

new alice: Assistant();
"#,
    );
    assert_eq!(
        error.message,
        "agent `alice` states `graf` once, and this is the second"
    );
    assert_eq!(error.related.len(), 1);
}

// ---------------------------------------------------------------------------
// Channels and tunings in one document
// ---------------------------------------------------------------------------

/// Declarations and tunings share one `channels` vec, and lowering emits every
/// declaration before every tuning whatever order the document wrote them in.
/// The mixed document is also the only place the "a declaration carries a uuid,
/// a tuning never does" rule meets its own counterexample.
#[test]
fn a_document_mixing_a_tuning_and_a_declaration_emits_declarations_first() {
    assert_lowers(
        r#"
channel at prefix "brenn:tool-results/" {
    push_depth = 2;
    retain_depth = 32;
    standing_retain_depth = 32;
}

channel alerts at "brenn:alice-alerts" {
    push_depth = 8;
    retain_depth = 128;
    standing_retain_depth = 256;
}
"#,
        config_with_channels(vec![
            ChannelConfigRaw {
                uuid: Some("85a5cf7e-6874-5766-9d69-712784754a1f".to_string()),
                push_depth: Some(Depth::Bounded(8)),
                retain_depth: Some(Depth::Bounded(128)),
                standing_retain_depth: Some(Depth::Bounded(256)),
                ..channel_at("brenn:alice-alerts")
            },
            ChannelConfigRaw {
                push_depth: Some(Depth::Bounded(2)),
                retain_depth: Some(Depth::Bounded(32)),
                standing_retain_depth: Some(Depth::Bounded(32)),
                ..channel_at_prefix("brenn:tool-results/")
            },
        ]),
    );
}

// ---------------------------------------------------------------------------
// The `[app.messaging]` presence rule
// ---------------------------------------------------------------------------

/// A budget with no subscriptions still needs the block — it is the only place
/// the budget can go.
#[test]
fn an_agent_with_only_a_send_budget_still_gets_a_messaging_block() {
    assert_lowers(
        r#"
agent Assistant() {
    send_budget = 40;
}

new alice: Assistant();
"#,
        BrennConfig {
            apps: vec![AppConfigRaw {
                slug: "alice".to_string(),
                messaging: Some(MessagingConfigRaw {
                    subscribe: vec![],
                    send_budget: Some(40),
                }),
                ..Default::default()
            }],
            ..Default::default()
        },
    );
}

/// And subscriptions with no budget: the block is present with the subscribe
/// list and no budget, so the app inherits the global one.
#[test]
fn an_agent_with_only_subscriptions_gets_a_messaging_block_without_a_budget() {
    assert_lowers(
        r#"
channel cmd at "brenn:alice.cmd" {
    push_depth = 8;
    retain_depth = 32;
    standing_retain_depth = 64;
}

agent Assistant() {
    grants = [subscribe];
    subscribe cmd { push_depth = 4; retain_depth = 8; }
}

new alice: Assistant();
"#,
        BrennConfig {
            channels: vec![alice_cmd_channel()],
            apps: vec![cmd_subscriber(MessagingSubscriptionRaw {
                channel: "brenn:alice.cmd".to_string(),
                push_depth: Some(Depth::Bounded(4)),
                retain_depth: Some(Depth::Bounded(8)),
                noise: None,
                wake_min: None,
            })],
            ..Default::default()
        },
    );
}

/// An unbounded window in a tail is the bare word, exactly as it is in a
/// channel body: a tail is a vocabulary position, so there is one spelling of
/// the token per language.
#[test]
fn an_unbounded_depth_in_a_subscription_tail_lowers() {
    assert_lowers(
        r#"
channel cmd at "brenn:alice.cmd" {
    push_depth = 8;
    retain_depth = 32;
    standing_retain_depth = 64;
}

agent Assistant() {
    grants = [subscribe];
    subscribe cmd { push_depth = 4; retain_depth = unbounded; }
}

new alice: Assistant();
"#,
        BrennConfig {
            channels: vec![alice_cmd_channel()],
            apps: vec![cmd_subscriber(MessagingSubscriptionRaw {
                channel: "brenn:alice.cmd".to_string(),
                push_depth: Some(Depth::Bounded(4)),
                retain_depth: Some(Depth::Unbounded),
                noise: None,
                wake_min: None,
            })],
            ..Default::default()
        },
    );
}

// ---------------------------------------------------------------------------
// Refusals in the value layer
// ---------------------------------------------------------------------------

/// A matcher is a legal thing to write in an `acl` statement, so one reaching
/// a value position is user error with its own wording rather than a shape
/// mismatch or a broken tree. A consumer body is one of the positions that
/// carries a value with no vocabulary over it, so a matcher reaches lowering
/// there.
#[test]
fn a_matcher_in_a_value_position_is_refused_as_one() {
    let error = refusal(concat!(
        r#"
channel cmd at "brenn:alice.cmd" {
    push_depth = 8;
    retain_depth = 32;
    standing_retain_depth = 64;
}

// ── packaged ──
component Router {
    "#,
        processor_needs!(""),
        r#"
    in inbound;
}
// ── packaged ──

new router: Router {
    grants = [];
    store_path = exact "alice.";

    in inbound <- cmd;
}
"#
    ));
    assert_eq!(error.message, "`store_path`: a matcher is not a value here");
    assert_eq!(
        error.line_col(),
        Some((18, 18)),
        "the span is the matcher's own token: {}",
        error.render()
    );
}

/// A list of strings refuses the element that is not a string, at that element
/// — not the whole list at the attr.
#[test]
fn a_non_string_element_of_a_string_list_is_refused_at_the_element() {
    let error = refusal(
        r#"
agent Assistant() {
    allowed_users = ["alice", 3];
}

new alice: Assistant();
"#,
    );
    assert_eq!(
        error.message,
        "`allowed_users`: expected a string, got an integer"
    );
}

/// Two bad elements report twice: a list is walked whole, like every other
/// position here.
#[test]
fn two_bad_elements_of_a_string_list_are_both_reported() {
    let errors = refusals(
        r#"
agent Assistant() {
    allowed_users = ["alice", 3, true];
}

new alice: Assistant();
"#,
    );
    assert_eq!(errors.len(), 2, "{}", render_all(&errors));
    for error in &errors {
        assert!(
            error
                .message
                .starts_with("`allowed_users`: expected a string"),
            "{}",
            error.render()
        );
    }
}

/// A single string where a list belongs is refused at the attr, with the list
/// named.
#[test]
fn a_bare_string_where_a_string_list_belongs_is_refused() {
    let error = refusal(
        r#"
agent Assistant() {
    disabled_tools = "WebSearch";
}

new alice: Assistant();
"#,
    );
    assert_eq!(
        error.message,
        "`disabled_tools`: expected a list of strings, got a string"
    );
}

/// An mcp server's `env` is a table of strings, and the entry whose value is
/// not one is refused at that value.
#[test]
fn a_non_string_env_entry_is_refused_at_the_entry() {
    let error = refusal(
        r#"
agent Assistant() {
    mcp_server graf {
        command = "graf";
        env = { GRAF_ROOT = 3 };
    }
}

new alice: Assistant();
"#,
    );
    assert_eq!(error.message, "`env`: expected a string, got an integer");
}

/// A table where a table of strings belongs — the whole-value refusal of the
/// same reader.
#[test]
fn a_bare_string_where_an_env_table_belongs_is_refused() {
    let error = refusal(
        r#"
agent Assistant() {
    mcp_server graf {
        command = "graf";
        env = "GRAF_ROOT";
    }
}

new alice: Assistant();
"#,
    );
    assert_eq!(
        error.message,
        "`env`: expected a table of strings, got a string"
    );
}

// ---------------------------------------------------------------------------
// A lowered config through the boot gates
// ---------------------------------------------------------------------------

/// Equality against an expected config says lowering emits the right fields; it
/// does not say the runtime accepts them. This runs a lowered config through the
/// two gates a boot puts it through — the channel directory and the whole-config
/// resolver — and compares the resolved channel entries against those a config
/// spelling the same durable channel bare produces. Bare is the form the
/// directory still canonicalizes and lowering never emits.
#[test]
fn a_lowered_config_resolves_the_same_entries_a_bare_address_config_does() {
    let dir = tempfile::tempdir().expect("a tempdir");
    let repo_dir = dir.path().join("repos");
    let clone = repo_dir.join("life");
    std::fs::create_dir_all(&clone).expect("the repo clone directory");
    let runtime_dir = dir.path().join("run");
    std::fs::create_dir_all(&runtime_dir).expect("the runtime directory");
    let repos = repo_dir.display();

    let from_dsl = config_from_dsl(&format!(
        r#"
server {{ public_url = "https://brenn.example.com"; }}
repo_sync {{ repo_dir = "{repos}"; }}

repo life {{ remote = "forgejo@example.com:alice/life.git"; }}

channel cmd at "brenn:alice.cmd" {{
    push_depth = 8;
    retain_depth = 32;
    standing_retain_depth = 64;
}}

channel presence at "ephemeral:alice.presence" {{ push_depth = 4; retain_depth = 8; }}

agent Assistant() {{
    grants = [subscribe];
    mount life {{ working_dir = true; }}
    // Pull-only: a pushed subscription would need `singleton = true`, which
    // the resolver checks and which is beside this test's point.
    subscribe cmd {{ push_depth = 0; retain_depth = 8; }}
}}

new alice: Assistant();
"#
    ));

    // Nothing but the channel blocks and the global messaging defaults feeds the
    // channel directory, so the comparison config carries only those.
    let bare_address = config_with_channels(vec![
        ChannelConfigRaw {
            address: Some("alice.cmd".to_string()),
            ..alice_cmd_channel()
        },
        ChannelConfigRaw {
            push_depth: Some(Depth::Bounded(4)),
            retain_depth: Some(Depth::Bounded(8)),
            ..channel_at("ephemeral:alice.presence")
        },
    ]);

    // The runtime canonicalizes a bare address to `brenn:`, so the two configs
    // — one qualified on emit, one bare as written — must resolve to the same
    // entries. `ChannelEntry` has no `PartialEq`; its `Debug` covers every
    // resolved field.
    let dsl_entries =
        crate::messaging::config::build_channel_entries(&from_dsl.channels, &from_dsl.messaging);
    let bare_entries = crate::messaging::config::build_channel_entries(
        &bare_address.channels,
        &bare_address.messaging,
    );
    assert_eq!(format!("{dsl_entries:#?}"), format!("{bare_entries:#?}"));

    // And the whole config passes the resolver every boot runs.
    let resolved = crate::config::validate_and_resolve(
        &from_dsl,
        &crate::integration::IntegrationRegistry::new(vec![]),
        Some(&runtime_dir),
    );
    let app = resolved
        .apps
        .get("alice")
        .expect("the lowered app resolves");
    assert_eq!(app.working_dir, clone);
}

// ---------------------------------------------------------------------------
// WASM consumers
// ---------------------------------------------------------------------------

/// A consumer stating every key its body admits, every binding direction, and
/// entries on all nine ACL families a consumer has: the whole transcription
/// surface of `[[wasm_consumer]]` in one row.
///
/// Nine families means nine adjacent, identical-shaped lowering lines, which is
/// the shape where a copy-paste crosses two permission families in silence —
/// the highest-consequence mistake this module can make. So every one of them
/// carries an entry here, and the two families
/// of each plane carry *different* patterns: identical ones would be blind to
/// exactly the crossing, since a transposed pair would still compare equal.
#[test]
fn a_consumer_lowers_with_every_key_binding_and_acl_family() {
    assert_lowers(
        concat!(
            r#"
mqtt_client broker {
    url = "mqtts://broker.example.com:8883";
}

channel alerts at "brenn:alice-alerts" {
    push_depth = 8;
    retain_depth = 128;
    standing_retain_depth = 128;
}

channel digests at "brenn:alice-digests" {
    push_depth = 2;
    retain_depth = 32;
    standing_retain_depth = 32;
}

channel presence at "ephemeral:alice-desk.presence" {
    push_depth = 4;
    retain_depth = 16;
}

channel status at "ephemeral:alice-desk.status" {
    push_depth = 2;
    retain_depth = 8;
}

webhook push_alice {
    slug = "push-alice";
    mount = "/webhooks/push-alice";

    signature {
        scheme = bearer-token;
        header = "authorization";
    }

    token phone { secret_file = "/home/alice/.secrets/push-alice.token"; }
}

// ── packaged ──
component Router {
    "#,
            processor_needs!("ports, store, log, config, mqtt"),
            r#"
    in inbound;
    in feed;
    in status;
    in hook;
    out outbound;
    out digest;
    io acks;
    io tick;
}
// ── packaged ──

new router: Router {
    slug = "router";
    grants = [ports, store, log, config, mqtt];
    store_path = "/state/router.db";
    store_size_limit = "64MiB";
    activation_burst = 4;
    activation_min_period_ms = 250;
    config = { mode = "fanout", depth = 3, verbose = true };

    acl subscribe [
        exact alerts,
        exact presence,
        prefix "local:router.in.",
        exact "local:router.acks",
        topic_filter "mqtt:broker:alice/#",
        endpoint "webhook:push-alice"
    ];
    acl publish [
        exact digests,
        exact status,
        prefix "local:router.out.",
        exact "local:router.acks",
        client "mqtt:broker" { publish_per_activation = 2, publish_capacity = 3.5 }
    ];

    in inbound <- alerts {
        push_depth = 4;
        retain_depth = unbounded;
        noise = metered;
        wake_min = low;
        amplification = 0.5;
    }
    in feed <- "local:router.in.feed" { push_depth = 2; retain_depth = 4; }
    in status <- presence { push_depth = 2; retain_depth = 4; }
    in hook <- "webhook:push-alice" { push_depth = 1; retain_depth = 2; }
    out digest -> digests { urgency = low; }
    out outbound -> status {
        urgency = high;
        publish_per_activation = 2;
        publish_capacity = 3.5;
    }
    io acks <-> "local:router.acks" { push_depth = 1; retain_depth = 2; }
    io tick {
        push_depth = 1;
        retain_depth = 2;
        noise = alarm;
        amplification = 1;
        urgency = low;
        publish_per_activation = 1;
        publish_capacity = 1;
    }
}
"#
        ),
        BrennConfig {
            mqtt_clients: vec![mqtt_client_at("broker", "mqtts://broker.example.com:8883")],
            channels: vec![
                ChannelConfigRaw {
                    uuid: Some("85a5cf7e-6874-5766-9d69-712784754a1f".to_string()),
                    push_depth: Some(Depth::Bounded(8)),
                    retain_depth: Some(Depth::Bounded(128)),
                    standing_retain_depth: Some(Depth::Bounded(128)),
                    ..channel_at("brenn:alice-alerts")
                },
                ChannelConfigRaw {
                    uuid: Some("8b2e83fc-6121-55ef-a665-7bea3fb6a9a6".to_string()),
                    push_depth: Some(Depth::Bounded(2)),
                    retain_depth: Some(Depth::Bounded(32)),
                    standing_retain_depth: Some(Depth::Bounded(32)),
                    ..channel_at("brenn:alice-digests")
                },
                ChannelConfigRaw {
                    push_depth: Some(Depth::Bounded(4)),
                    retain_depth: Some(Depth::Bounded(16)),
                    ..channel_at("ephemeral:alice-desk.presence")
                },
                ChannelConfigRaw {
                    push_depth: Some(Depth::Bounded(2)),
                    retain_depth: Some(Depth::Bounded(8)),
                    ..channel_at("ephemeral:alice-desk.status")
                },
            ],
            webhook_endpoints: vec![bearer_token_endpoint("push-alice")],
            wasm_consumers: vec![WasmConsumerConfigRaw {
                grants: vec![
                    ComponentGrant::Ports,
                    ComponentGrant::Store,
                    ComponentGrant::Log,
                    ComponentGrant::Config,
                    ComponentGrant::Mqtt,
                ],
                store_path: Some(PathBuf::from("/state/router.db")),
                store_size_limit: Some("64MiB".to_string()),
                subscriptions: vec![
                    WasmConsumerSubscriptionRaw {
                        port: "inbound".to_string(),
                        channel: Some("brenn:alice-alerts".to_string()),
                        push_depth: Some(Depth::Bounded(4)),
                        retain_depth: Some(Depth::Unbounded),
                        noise: Some(NoiseLevel::Metered),
                        wake_min: Some(WakeMin::Low),
                        amplification: Some(0.5),
                    },
                    WasmConsumerSubscriptionRaw {
                        port: "feed".to_string(),
                        channel: Some("local:router.in.feed".to_string()),
                        push_depth: Some(Depth::Bounded(2)),
                        retain_depth: Some(Depth::Bounded(4)),
                        noise: None,
                        wake_min: None,
                        amplification: None,
                    },
                    WasmConsumerSubscriptionRaw {
                        port: "status".to_string(),
                        channel: Some("ephemeral:alice-desk.presence".to_string()),
                        push_depth: Some(Depth::Bounded(2)),
                        retain_depth: Some(Depth::Bounded(4)),
                        noise: None,
                        wake_min: None,
                        amplification: None,
                    },
                    WasmConsumerSubscriptionRaw {
                        port: "hook".to_string(),
                        channel: Some("webhook:push-alice".to_string()),
                        push_depth: Some(Depth::Bounded(1)),
                        retain_depth: Some(Depth::Bounded(2)),
                        noise: None,
                        wake_min: None,
                        amplification: None,
                    },
                ],
                outputs: vec![
                    WasmConsumerOutputRaw {
                        port: "digest".to_string(),
                        channel: Some("brenn:alice-digests".to_string()),
                        urgency: Some(Urgency::Low),
                        publish_per_activation: None,
                        publish_capacity: None,
                    },
                    WasmConsumerOutputRaw {
                        port: "outbound".to_string(),
                        channel: Some("ephemeral:alice-desk.status".to_string()),
                        urgency: Some(Urgency::High),
                        publish_per_activation: Some(2.0),
                        publish_capacity: Some(3.5),
                    },
                ],
                io_ports: vec![
                    WasmConsumerIoPortRaw {
                        port: "acks".to_string(),
                        channel: Some("local:router.acks".to_string()),
                        push_depth: Some(Depth::Bounded(1)),
                        retain_depth: Some(Depth::Bounded(2)),
                        noise: None,
                        amplification: None,
                        urgency: None,
                        publish_per_activation: None,
                        publish_capacity: None,
                    },
                    WasmConsumerIoPortRaw {
                        port: "tick".to_string(),
                        channel: None,
                        push_depth: Some(Depth::Bounded(1)),
                        retain_depth: Some(Depth::Bounded(2)),
                        noise: Some(NoiseLevel::Alarm),
                        amplification: Some(1.0),
                        urgency: Some(Urgency::Low),
                        publish_per_activation: Some(1.0),
                        publish_capacity: Some(1.0),
                    },
                ],
                subscribe_acl: vec![ChannelMatcherRaw::Exact("alice-alerts".to_string())],
                ephemeral_subscribe_acl: vec![ChannelMatcherRaw::Exact(
                    "alice-desk.presence".to_string(),
                )],
                local_subscribe_acl: vec![
                    ChannelMatcherRaw::Prefix("router.in.".to_string()),
                    ChannelMatcherRaw::Exact("router.acks".to_string()),
                ],
                publish_acl: vec![ChannelMatcherRaw::Exact("alice-digests".to_string())],
                ephemeral_publish_acl: vec![ChannelMatcherRaw::Exact(
                    "alice-desk.status".to_string(),
                )],
                local_publish_acl: vec![
                    ChannelMatcherRaw::Prefix("router.out.".to_string()),
                    ChannelMatcherRaw::Exact("router.acks".to_string()),
                ],
                mqtt_publish_acl: vec![MqttClientMatcherRaw {
                    client: "broker".to_string(),
                }],
                mqtt_subscribe_acl: vec![MqttSubMatcherRaw {
                    client: "broker".to_string(),
                    topic_filter: "alice/#".to_string(),
                }],
                webhook_acl: vec![WebhookMatcherRaw {
                    endpoint: "push-alice".to_string(),
                }],
                mqtt_outputs: vec![WasmConsumerMqttOutputRaw {
                    client: "broker".to_string(),
                    publish_per_activation: Some(2.0),
                    publish_capacity: Some(3.5),
                }],
                config: Some(toml::Table::from_iter([
                    (
                        "mode".to_string(),
                        toml::Value::String("fanout".to_string()),
                    ),
                    ("depth".to_string(), toml::Value::Integer(3)),
                    ("verbose".to_string(), toml::Value::Boolean(true)),
                ])),
                activation_burst: Some(4),
                activation_min_period_ms: Some(250),
                ..consumer("router")
            }],
            ..Default::default()
        },
    );
}

/// Every other lowering test asserts `spec_sha256` through `assert_lowers`;
/// this one names it directly.
#[test]
fn a_consumer_lowers_the_hash_of_the_file_its_class_was_declared_in() {
    let document = concat!(
        r#"
channel utterance at "ephemeral:alice-pod.utterance" {
    push_depth = 4;
    retain_depth = 16;
}

// ── packaged ──
component Logger {
    "#,
        processor_needs!("log"),
        r#"
    in heard;
}
// ── packaged ──

new logger: Logger {
    grants = [log];

    in heard <- utterance;
}
"#
    );
    let config = config_from_dsl(document);
    assert_eq!(
        config.wasm_consumers[0].spec_sha256,
        brenn_dsl::source_sha256(&crate::config::declaring_text(document))
    );
    // A comment-only edit to the declaring file is a different hash: the
    // binding is byte identity, not semantic equality.
    let commented = format!(
        "// a note
{document}"
    );
    assert_ne!(
        config_from_dsl(&commented).wasm_consumers[0].spec_sha256,
        config.wasm_consumers[0].spec_sha256
    );
}

/// The surface twin of the consumer hash test: a placed instance carries the
/// hash of the file its class was declared in, and a comment-only edit to that
/// file is a different hash — the binding boot checks is byte identity, not
/// semantic equality.
#[test]
fn a_surface_component_lowers_the_hash_of_the_file_its_class_was_declared_in() {
    let document = concat!(
        r#"
channel acks at "ephemeral:alice-desk.acks" {
    push_depth = 2;
    retain_depth = 4;
}

component Panel {
    "#,
        processor_needs!("ports"),
        r#"
    io acks;
}

surface alice_desk {
    grants = [subscribe, publish];

    new panel: Panel {
        grants = [ports];
        io acks <-> acks;
    }
}
"#
    );
    let config = config_from_dsl(document);
    assert_eq!(
        config.surfaces[0].components[0].spec_sha256,
        brenn_dsl::source_sha256(document)
    );
    let commented = format!(
        "// a note
{document}"
    );
    assert_ne!(
        config_from_dsl(&commented).surfaces[0].components[0].spec_sha256,
        config.surfaces[0].components[0].spec_sha256
    );
}

/// The minimal consumer body: a class, a grant and one input. Every optional
/// key is omitted, so this row locks what a `[[wasm_consumer]]` looks like when
/// the operator states nothing — and with no `acl` statement the input's own
/// authority is what derivation reads off the binding.
#[test]
fn a_minimal_consumer_states_only_its_grant_and_its_input() {
    assert_lowers(
        concat!(
            r#"
channel utterance at "ephemeral:alice-pod.utterance" {
    push_depth = 4;
    retain_depth = 16;
}

// ── packaged ──
component Logger {
    "#,
            processor_needs!("log"),
            r#"
    in heard;
}
// ── packaged ──

new logger: Logger {
    grants = [log];

    in heard <- utterance;
}
"#
        ),
        BrennConfig {
            channels: vec![ChannelConfigRaw {
                push_depth: Some(Depth::Bounded(4)),
                retain_depth: Some(Depth::Bounded(16)),
                ..channel_at("ephemeral:alice-pod.utterance")
            }],
            wasm_consumers: vec![WasmConsumerConfigRaw {
                grants: vec![ComponentGrant::Log],
                subscriptions: vec![attrless_subscription(
                    "heard",
                    "ephemeral:alice-pod.utterance",
                )],
                ephemeral_subscribe_acl: vec![ChannelMatcherRaw::Exact(
                    "alice-pod.utterance".to_string(),
                )],
                ..consumer("logger")
            }],
            ..Default::default()
        },
    );
}

/// A component's ports are its own names and nothing reserves the binding
/// keywords, so a port may be called `in` or `out` — `in in <- ...`. This
/// gates resolve and lowering, not just parsing: the port names must reach the
/// raw config's wire-facing `port` field.
#[test]
fn a_port_named_after_its_direction_keyword_lowers_to_that_wire_name() {
    assert_lowers(
        concat!(
            r#"
channel utterance at "ephemeral:alice-pod.utterance" {
    push_depth = 4;
    retain_depth = 16;
}

channel notes at "ephemeral:alice-pod.notes" {
    push_depth = 4;
    retain_depth = 16;
}

// ── packaged ──
component Reserved {
    "#,
            processor_needs!("log, ports"),
            r#"
    in in;
    out out;
}
// ── packaged ──

new reserved: Reserved {
    grants = [log, ports];

    in in <- utterance;
    out out -> notes;
}
"#
        ),
        BrennConfig {
            channels: vec![
                ChannelConfigRaw {
                    push_depth: Some(Depth::Bounded(4)),
                    retain_depth: Some(Depth::Bounded(16)),
                    ..channel_at("ephemeral:alice-pod.utterance")
                },
                ChannelConfigRaw {
                    push_depth: Some(Depth::Bounded(4)),
                    retain_depth: Some(Depth::Bounded(16)),
                    ..channel_at("ephemeral:alice-pod.notes")
                },
            ],
            wasm_consumers: vec![WasmConsumerConfigRaw {
                grants: vec![ComponentGrant::Log, ComponentGrant::Ports],
                subscriptions: vec![attrless_subscription("in", "ephemeral:alice-pod.utterance")],
                outputs: vec![WasmConsumerOutputRaw {
                    port: "out".to_string(),
                    channel: Some("ephemeral:alice-pod.notes".to_string()),
                    urgency: None,
                    publish_per_activation: None,
                    publish_capacity: None,
                }],
                ephemeral_subscribe_acl: vec![ChannelMatcherRaw::Exact(
                    "alice-pod.utterance".to_string(),
                )],
                ephemeral_publish_acl: vec![ChannelMatcherRaw::Exact(
                    "alice-pod.notes".to_string(),
                )],
                ..consumer("reserved")
            }],
            ..Default::default()
        },
    );
}

/// The operator config map is one of the two positions where a raw field
/// literally stores TOML, so it is the one place a value tree is the target.
///
/// The entries are strings, integers and booleans because that is what the
/// runtime accepts here: `resolve_component_config` is a flat surface and
/// panics at boot on a float, an array or a nested table, whichever front end
/// wrote it. So this row pins the transcription *and* a config that boots.
#[test]
fn a_consumers_config_map_carries_typed_scalars() {
    assert_lowers(
        concat!(
            r#"
// ── packaged ──
component Sink {
    "#,
            processor_needs!("ports, config"),
            r#"
    io tick;
}
// ── packaged ──

new sink: Sink {
    grants = [ports, config];
    config = {
        mode = "fast",
        window_secs = 30,
        strict = true,
    };

    io tick { push_depth = 1; retain_depth = 2; }
}
"#
        ),
        BrennConfig {
            wasm_consumers: vec![WasmConsumerConfigRaw {
                grants: vec![ComponentGrant::Ports, ComponentGrant::Config],
                io_ports: vec![tick_io_port()],
                config: Some(toml::Table::from_iter([
                    ("mode".to_string(), toml::Value::String("fast".to_string())),
                    ("window_secs".to_string(), toml::Value::Integer(30)),
                    ("strict".to_string(), toml::Value::Boolean(true)),
                ])),
                ..consumer("sink")
            }],
            ..Default::default()
        },
    );
}

/// Consumer twin of the surface row below: a declared channel's `io` port
/// lowers to subscription/output, not `[[wasm_consumer.io_port]]`. Also
/// covers `amplification`, a consumer-only knob carried on the subscription.
#[test]
fn a_consumers_io_port_on_a_declared_channel_lowers_to_the_pair() {
    assert_lowers(
        concat!(
            r#"
channel acks at "ephemeral:alice-pod.acks" {
    push_depth = 2;
    retain_depth = 4;
}

// ── packaged ──
component Sink {
    "#,
            processor_needs!("ports"),
            r#"
    io acks;
}
// ── packaged ──

new sink: Sink {
    grants = [ports];

    io acks <-> acks {
        push_depth = 1;
        retain_depth = 2;
        noise = alarm;
        amplification = 0.5;
        urgency = low;
        publish_per_activation = 1;
        publish_capacity = 1;
    }
}
"#
        ),
        BrennConfig {
            channels: vec![ChannelConfigRaw {
                push_depth: Some(Depth::Bounded(2)),
                retain_depth: Some(Depth::Bounded(4)),
                ..channel_at("ephemeral:alice-pod.acks")
            }],
            wasm_consumers: vec![WasmConsumerConfigRaw {
                grants: vec![ComponentGrant::Ports],
                subscriptions: vec![WasmConsumerSubscriptionRaw {
                    push_depth: Some(Depth::Bounded(1)),
                    retain_depth: Some(Depth::Bounded(2)),
                    noise: Some(NoiseLevel::Alarm),
                    amplification: Some(0.5),
                    ..attrless_subscription("acks", "ephemeral:alice-pod.acks")
                }],
                outputs: vec![WasmConsumerOutputRaw {
                    port: "acks".to_string(),
                    channel: Some("ephemeral:alice-pod.acks".to_string()),
                    urgency: Some(Urgency::Low),
                    publish_per_activation: Some(1.0),
                    publish_capacity: Some(1.0),
                }],
                ephemeral_subscribe_acl: vec![ChannelMatcherRaw::Exact(
                    "alice-pod.acks".to_string(),
                )],
                ephemeral_publish_acl: vec![ChannelMatcherRaw::Exact("alice-pod.acks".to_string())],
                ..consumer("sink")
            }],
            ..Default::default()
        },
    );
}

#[test]
fn an_attrless_consumer_io_port_on_a_declared_channel_lowers_to_the_pair() {
    assert_lowers(
        concat!(
            r#"
channel acks at "ephemeral:alice-pod.acks" {
    push_depth = 2;
    retain_depth = 4;
}

// ── packaged ──
component Sink {
    "#,
            processor_needs!("ports"),
            r#"
    io acks;
}
// ── packaged ──

new sink: Sink {
    grants = [ports];

    io acks <-> acks;
}
"#
        ),
        BrennConfig {
            channels: vec![ChannelConfigRaw {
                push_depth: Some(Depth::Bounded(2)),
                retain_depth: Some(Depth::Bounded(4)),
                ..channel_at("ephemeral:alice-pod.acks")
            }],
            wasm_consumers: vec![WasmConsumerConfigRaw {
                grants: vec![ComponentGrant::Ports],
                subscriptions: vec![attrless_subscription("acks", "ephemeral:alice-pod.acks")],
                outputs: vec![WasmConsumerOutputRaw {
                    port: "acks".to_string(),
                    channel: Some("ephemeral:alice-pod.acks".to_string()),
                    urgency: None,
                    publish_per_activation: None,
                    publish_capacity: None,
                }],
                ephemeral_subscribe_acl: vec![ChannelMatcherRaw::Exact(
                    "alice-pod.acks".to_string(),
                )],
                ephemeral_publish_acl: vec![ChannelMatcherRaw::Exact("alice-pod.acks".to_string())],
                ..consumer("sink")
            }],
            ..Default::default()
        },
    );
}

/// Two consumers in one document, distinguishable on the axis lowering zips:
/// each pairs a resolved instance with its derived authority by position, so a
/// mis-pairing would hand one consumer the other's grants and ACLs. That is a
/// silent privilege transfer rather than a parse error, and a single-entity row
/// cannot see it.
#[test]
fn two_consumers_keep_their_own_grants_and_acls() {
    assert_lowers(
        concat!(
            r#"
channel alerts at "brenn:alice-alerts" {
    push_depth = 8;
    retain_depth = 128;
    standing_retain_depth = 128;
}

channel presence at "ephemeral:alice-desk.presence" {
    push_depth = 4;
    retain_depth = 16;
}

// ── packaged ──
component Router {
    "#,
            processor_needs!("log"),
            r#"
    in inbound;
}
// ── packaged ──

// ── packaged ──
component Sink {
    "#,
            processor_needs!("store, config"),
            r#"
    in feed;
}
// ── packaged ──

new router: Router {
    grants = [log];

    in inbound <- alerts { push_depth = 4; retain_depth = 8; }
}

new sink: Sink {
    grants = [store, config];
    store_path = "/state/sink.db";

    in feed <- presence { push_depth = 2; retain_depth = 4; }
}
"#
        ),
        BrennConfig {
            channels: vec![
                ChannelConfigRaw {
                    uuid: Some("85a5cf7e-6874-5766-9d69-712784754a1f".to_string()),
                    push_depth: Some(Depth::Bounded(8)),
                    retain_depth: Some(Depth::Bounded(128)),
                    standing_retain_depth: Some(Depth::Bounded(128)),
                    ..channel_at("brenn:alice-alerts")
                },
                ChannelConfigRaw {
                    push_depth: Some(Depth::Bounded(4)),
                    retain_depth: Some(Depth::Bounded(16)),
                    ..channel_at("ephemeral:alice-desk.presence")
                },
            ],
            wasm_consumers: vec![
                WasmConsumerConfigRaw {
                    grants: vec![ComponentGrant::Log],
                    subscriptions: vec![WasmConsumerSubscriptionRaw {
                        push_depth: Some(Depth::Bounded(4)),
                        retain_depth: Some(Depth::Bounded(8)),
                        ..attrless_subscription("inbound", "brenn:alice-alerts")
                    }],
                    subscribe_acl: vec![ChannelMatcherRaw::Exact("alice-alerts".to_string())],
                    ..consumer("router")
                },
                WasmConsumerConfigRaw {
                    grants: vec![ComponentGrant::Store, ComponentGrant::Config],
                    store_path: Some(PathBuf::from("/state/sink.db")),
                    subscriptions: vec![WasmConsumerSubscriptionRaw {
                        push_depth: Some(Depth::Bounded(2)),
                        retain_depth: Some(Depth::Bounded(4)),
                        ..attrless_subscription("feed", "ephemeral:alice-desk.presence")
                    }],
                    ephemeral_subscribe_acl: vec![ChannelMatcherRaw::Exact(
                        "alice-desk.presence".to_string(),
                    )],
                    ..consumer("sink")
                },
            ],
            ..Default::default()
        },
    );
}

/// The operator config map is a `toml::Value` position, and the transcription
/// recurses: a float stays a float, a list stays an array in order, and an
/// inner table stays a table.
///
/// A value of every shape the map admits appears here, which is the claim
/// `rval_to_toml`'s recursion makes.
#[test]
fn a_consumers_config_map_transcribes_floats_lists_and_nested_tables() {
    assert_lowers(
        concat!(
            r#"
// ── packaged ──
component Sink {
    "#,
            processor_needs!("ports, config"),
            r#"
    io tick;
}
// ── packaged ──

new sink: Sink {
    grants = [ports, config];
    config = {
        rate = 1.5,
        tags = ["fast", "quiet"],
        limits = { max = 3, spill = false },
    };

    io tick { push_depth = 1; retain_depth = 2; }
}
"#
        ),
        BrennConfig {
            wasm_consumers: vec![WasmConsumerConfigRaw {
                grants: vec![ComponentGrant::Ports, ComponentGrant::Config],
                io_ports: vec![tick_io_port()],
                config: Some(toml::Table::from_iter([
                    ("rate".to_string(), toml::Value::Float(1.5)),
                    (
                        "tags".to_string(),
                        toml::Value::Array(vec![
                            toml::Value::String("fast".to_string()),
                            toml::Value::String("quiet".to_string()),
                        ]),
                    ),
                    (
                        "limits".to_string(),
                        toml::Value::Table(toml::Table::from_iter([
                            ("max".to_string(), toml::Value::Integer(3)),
                            ("spill".to_string(), toml::Value::Boolean(false)),
                        ])),
                    ),
                ])),
                ..consumer("sink")
            }],
            ..Default::default()
        },
    );
}

/// A refusal inside a nested config value cites the inner token, not the map.
#[test]
fn a_matcher_nested_in_a_config_list_is_refused_at_the_inner_token() {
    let refusal = refusal(concat!(
        r#"
// ── packaged ──
component Sink {
    "#,
        processor_needs!("ports, config"),
        r#"
    io tick;
}
// ── packaged ──

new sink: Sink {
    grants = [ports, config];
    config = { tags = ["fast", exact "alice."] };

    io tick { push_depth = 1; retain_depth = 2; }
}
"#
    ));
    assert_eq!(refusal.message, "`config`: a matcher is not a value here");
    assert_eq!(
        refusal.span.text_str(),
        Some("exact \"alice.\""),
        "the span is the inner element's own, not the map's"
    );
}

/// A budget knob is a number, and a whole one written without a point is the
/// same number: what is refused is a value that is no number at all.
#[test]
fn a_non_number_in_a_budget_position_is_refused() {
    let refusal = refusal(concat!(
        r#"
// ── packaged ──
component Sink {
    "#,
        processor_needs!("ports, log"),
        r#"
    io tick;
}
// ── packaged ──

new sink: Sink {
    grants = [ports, log];

    io tick { push_depth = 1; retain_depth = 2; amplification = "fast"; }
}
"#
    ));
    assert_eq!(
        refusal.message,
        "`amplification`: expected a number, got a string"
    );
}

/// A matcher is a legal thing to write in an open position, so a matcher where
/// the config map expects a value is user error rather than a broken tree —
/// refused at the matcher's own token.
#[test]
fn a_matcher_in_a_consumers_config_map_is_refused() {
    let refusal = refusal(concat!(
        r#"
// ── packaged ──
component Sink {
    "#,
        processor_needs!("ports, config"),
        r#"
    io tick;
}
// ── packaged ──

new sink: Sink {
    grants = [ports, config];
    config = { mode = exact "alice." };

    io tick { push_depth = 1; retain_depth = 2; }
}
"#
    ));
    assert_eq!(refusal.message, "`config`: a matcher is not a value here");
}

// ---------------------------------------------------------------------------
// Surfaces
// ---------------------------------------------------------------------------

/// A surface stating every attr its vocabulary admits, two component instances
/// between them stating every body key and every binding direction, and entries
/// on all four ACL families a surface has: the whole transcription surface of
/// `[[surface]]` in one row.
///
/// The binding tables are flat under the surface and carry the instance name,
/// which is the raw config's shape rather than the document's nesting.
#[test]
fn a_surface_lowers_with_every_key_component_and_acl_family() {
    assert_lowers(
        concat!(
            r#"
channel alerts at "brenn:alice-alerts" {
    push_depth = 8;
    retain_depth = 128;
    standing_retain_depth = 128;
}

channel presence at "ephemeral:alice-desk.presence" {
    push_depth = 4;
    retain_depth = 16;
}

component Panel {
    "#,
            processor_needs!("ports"),
            r#"
    in messages;
    out outbound;
    io acks;
    io tick;
}

component Chrome {
    "#,
            processor_needs!("dom, page-dom"),
            r#"
    in state;
}

surface alice_desk {
    slug = "alice-desk";
    grants = [subscribe, publish, alert];
    skin = "bench";
    allowed_users = ["alice", "bob"];
    publish_burst = 32;
    publish_per_sec = 4;

    acl subscribe [exact alerts, prefix "ephemeral:alice-desk."];
    acl publish [prefix "brenn:alice-desk.", exact presence];

    new panel: Panel {
        grants = [ports];
        send_burst = 16;
        send_refill_secs = 30;
        parked_batch_depth = unbounded;
        config = { mode = "compact", layout = "wide" };

        in messages <- alerts {
            push_depth = 4;
            retain_depth = 8;
            noise = metered;
            wake_min = low;
        }
        out outbound -> presence {
            urgency = high;
            publish_per_activation = 2;
            publish_capacity = 3.5;
        }
        io acks <-> "local:panel/acks" { push_depth = 1; retain_depth = 2; }
        io tick {
            push_depth = 1;
            retain_depth = 2;
            noise = alarm;
            urgency = low;
            publish_per_activation = 1;
            publish_capacity = 1;
        }
    }

    new chrome: Chrome {
        grants = [dom, page-dom];
        chrome = true;

        in state <- presence { push_depth = 1; }
    }
}
"#
        ),
        BrennConfig {
            channels: vec![
                ChannelConfigRaw {
                    uuid: Some("85a5cf7e-6874-5766-9d69-712784754a1f".to_string()),
                    push_depth: Some(Depth::Bounded(8)),
                    retain_depth: Some(Depth::Bounded(128)),
                    standing_retain_depth: Some(Depth::Bounded(128)),
                    ..channel_at("brenn:alice-alerts")
                },
                ChannelConfigRaw {
                    push_depth: Some(Depth::Bounded(4)),
                    retain_depth: Some(Depth::Bounded(16)),
                    ..channel_at("ephemeral:alice-desk.presence")
                },
            ],
            surfaces: vec![SurfaceConfigRaw {
                slug: "alice-desk".to_string(),
                grants: vec![
                    AttachGrant::Subscribe,
                    AttachGrant::EphemeralSubscribe,
                    AttachGrant::Publish,
                    AttachGrant::EphemeralPublish,
                    AttachGrant::Alert,
                ],
                subscribe_acl: vec![ChannelMatcherRaw::Exact("alice-alerts".to_string())],
                publish_acl: vec![ChannelMatcherRaw::Prefix("alice-desk.".to_string())],
                ephemeral_subscribe_acl: vec![ChannelMatcherRaw::Prefix("alice-desk.".to_string())],
                ephemeral_publish_acl: vec![ChannelMatcherRaw::Exact(
                    "alice-desk.presence".to_string(),
                )],
                components: vec![
                    SurfaceComponentRaw {
                        send_burst: Some(16),
                        send_refill_secs: Some(30),
                        parked_batch_depth: Some(Depth::Unbounded),
                        config: Some(BTreeMap::from_iter([
                            ("mode".to_string(), "compact".to_string()),
                            ("layout".to_string(), "wide".to_string()),
                        ])),
                        grants: vec![ComponentGrant::Ports],
                        ..placed_component("panel")
                    },
                    SurfaceComponentRaw {
                        chrome: true,
                        grants: vec![ComponentGrant::Dom, ComponentGrant::PageDom],
                        ..placed_component("chrome")
                    },
                ],
                subscriptions: vec![
                    SurfaceSubscriptionRaw {
                        push_depth: Some(Depth::Bounded(4)),
                        retain_depth: Some(Depth::Bounded(8)),
                        noise: Some(NoiseLevel::Metered),
                        wake_min: Some(WakeMin::Low),
                        ..surface_input("panel", "messages", "brenn:alice-alerts")
                    },
                    SurfaceSubscriptionRaw {
                        push_depth: Some(Depth::Bounded(1)),
                        ..surface_input("chrome", "state", "ephemeral:alice-desk.presence")
                    },
                ],
                outputs: vec![SurfaceOutputRaw {
                    urgency: Some(Urgency::High),
                    publish_per_activation: Some(2.0),
                    publish_capacity: Some(3.5),
                    ..surface_output("panel", "outbound", "ephemeral:alice-desk.presence")
                }],
                io_ports: vec![
                    SurfaceIoPortRaw {
                        channel: Some("local:panel/acks".to_string()),
                        push_depth: Some(Depth::Bounded(1)),
                        retain_depth: Some(Depth::Bounded(2)),
                        ..surface_io_port("panel", "acks")
                    },
                    SurfaceIoPortRaw {
                        push_depth: Some(Depth::Bounded(1)),
                        retain_depth: Some(Depth::Bounded(2)),
                        noise: Some(NoiseLevel::Alarm),
                        urgency: Some(Urgency::Low),
                        publish_per_activation: Some(1.0),
                        publish_capacity: Some(1.0),
                        ..surface_io_port("panel", "tick")
                    },
                ],
                skin: Some("bench".to_string()),
                allowed_users: vec!["alice".to_string(), "bob".to_string()],
                publish_burst: Some(32),
                publish_per_sec: Some(4),
            }],
            ..Default::default()
        },
    );
}

/// A placed instance's own `acl` statement stops at this seam: `grants` crosses
/// into the raw carrier and the ACL families do not, because the carrier has no
/// field for them. The front end still refuses a binding outside the statement,
/// so the statement is not inert — it is just not carried.
///
/// TODO(surface-instance-acl-bound) is the reason; when the field lands this
/// test inverts rather than being written from scratch.
#[test]
fn a_placed_instances_acl_statement_does_not_cross_the_lowering_seam() {
    assert_lowers(
        concat!(
            r#"
channel cmd at "ephemeral:alice-desk.cmd" {
    push_depth = 2;
    retain_depth = 4;
}

component Panel {
    "#,
            surface_any!(),
            r#"
    in messages;
}

surface alice_desk {
    grants = [subscribe];
    acl subscribe [prefix "ephemeral:alice-desk."];

    new panel: Panel {
        grants = [];
        acl subscribe [prefix "ephemeral:alice-desk."];
        in messages <- cmd;
    }
}
"#
        ),
        BrennConfig {
            channels: vec![ChannelConfigRaw {
                push_depth: Some(Depth::Bounded(2)),
                retain_depth: Some(Depth::Bounded(4)),
                ..channel_at("ephemeral:alice-desk.cmd")
            }],
            surfaces: vec![SurfaceConfigRaw {
                ephemeral_subscribe_acl: vec![ChannelMatcherRaw::Prefix("alice-desk.".to_string())],
                components: vec![placed_component("panel")],
                subscriptions: vec![surface_input(
                    "panel",
                    "messages",
                    "ephemeral:alice-desk.cmd",
                )],
                ..surface("alice_desk", vec![AttachGrant::EphemeralSubscribe])
            }],
            ..Default::default()
        },
    );
}

/// A declared channel's `io` port lowers to subscription/output, not
/// `[[surface.io_port]]`.
#[test]
fn an_io_port_on_a_declared_channel_lowers_to_the_pair() {
    assert_lowers(
        concat!(
            r#"
channel acks at "ephemeral:alice-desk.acks" {
    push_depth = 2;
    retain_depth = 4;
}

component Panel {
    "#,
            processor_needs!("ports"),
            r#"
    io acks;
}

surface alice_desk {
    grants = [subscribe, publish];

    new panel: Panel {
        grants = [ports];
        io acks <-> acks {
            push_depth = 1;
            retain_depth = 2;
            noise = alarm;
            urgency = low;
            publish_per_activation = 1;
            publish_capacity = 1;
        }
    }
}
"#
        ),
        BrennConfig {
            channels: vec![ChannelConfigRaw {
                push_depth: Some(Depth::Bounded(2)),
                retain_depth: Some(Depth::Bounded(4)),
                ..channel_at("ephemeral:alice-desk.acks")
            }],
            surfaces: vec![SurfaceConfigRaw {
                ephemeral_subscribe_acl: vec![ChannelMatcherRaw::Exact(
                    "alice-desk.acks".to_string(),
                )],
                ephemeral_publish_acl: vec![ChannelMatcherRaw::Exact(
                    "alice-desk.acks".to_string(),
                )],
                components: vec![SurfaceComponentRaw {
                    grants: vec![ComponentGrant::Ports],
                    ..placed_component("panel")
                }],
                subscriptions: vec![SurfaceSubscriptionRaw {
                    push_depth: Some(Depth::Bounded(1)),
                    retain_depth: Some(Depth::Bounded(2)),
                    noise: Some(NoiseLevel::Alarm),
                    ..surface_input("panel", "acks", "ephemeral:alice-desk.acks")
                }],
                outputs: vec![SurfaceOutputRaw {
                    urgency: Some(Urgency::Low),
                    publish_per_activation: Some(1.0),
                    publish_capacity: Some(1.0),
                    ..surface_output("panel", "acks", "ephemeral:alice-desk.acks")
                }],
                ..surface(
                    "alice_desk",
                    vec![
                        AttachGrant::EphemeralSubscribe,
                        AttachGrant::EphemeralPublish,
                    ],
                )
            }],
            ..Default::default()
        },
    );
}

#[test]
fn an_attrless_io_port_on_a_declared_channel_lowers_to_the_pair() {
    assert_lowers(
        concat!(
            r#"
channel acks at "ephemeral:alice-desk.acks" {
    push_depth = 2;
    retain_depth = 4;
}

component Panel {
    "#,
            processor_needs!("ports"),
            r#"
    io acks;
}

surface alice_desk {
    grants = [subscribe, publish];

    new panel: Panel {
        grants = [ports];
        io acks <-> acks;
    }
}
"#
        ),
        BrennConfig {
            channels: vec![ChannelConfigRaw {
                push_depth: Some(Depth::Bounded(2)),
                retain_depth: Some(Depth::Bounded(4)),
                ..channel_at("ephemeral:alice-desk.acks")
            }],
            surfaces: vec![SurfaceConfigRaw {
                ephemeral_subscribe_acl: vec![ChannelMatcherRaw::Exact(
                    "alice-desk.acks".to_string(),
                )],
                ephemeral_publish_acl: vec![ChannelMatcherRaw::Exact(
                    "alice-desk.acks".to_string(),
                )],
                components: vec![SurfaceComponentRaw {
                    grants: vec![ComponentGrant::Ports],
                    ..placed_component("panel")
                }],
                subscriptions: vec![surface_input("panel", "acks", "ephemeral:alice-desk.acks")],
                outputs: vec![surface_output("panel", "acks", "ephemeral:alice-desk.acks")],
                ..surface(
                    "alice_desk",
                    vec![
                        AttachGrant::EphemeralSubscribe,
                        AttachGrant::EphemeralPublish,
                    ],
                )
            }],
            ..Default::default()
        },
    );
}

/// An `optional` port an instance does not bind is inert: it contributes no
/// subscription, no output, no `io_port` and no ACL entry. This is what makes a
/// component class shareable between documents that bind different subsets of
/// its ports — the class permits the absence, the instance decides.
///
/// The grant list holds only `subscribe` because a granted right that no bound
/// port or acl statement reaches is separately refused: an unbound `out` port
/// does not earn its class's instances a `publish` grant.
#[test]
fn a_port_the_instance_does_not_bind_lowers_to_nothing() {
    assert_lowers(
        concat!(
            r#"
channel messages at "ephemeral:alice-desk.messages" {
    push_depth = 2;
    retain_depth = 4;
}

component Panel {
    "#,
            surface_any!(),
            r#"
    in messages;
    optional out outbound;
    optional io tick;
}

surface alice_desk {
    grants = [subscribe];

    new panel: Panel {
        grants = [];
        in messages <- messages;
    }
}
"#
        ),
        BrennConfig {
            channels: vec![ChannelConfigRaw {
                push_depth: Some(Depth::Bounded(2)),
                retain_depth: Some(Depth::Bounded(4)),
                ..channel_at("ephemeral:alice-desk.messages")
            }],
            surfaces: vec![SurfaceConfigRaw {
                ephemeral_subscribe_acl: vec![ChannelMatcherRaw::Exact(
                    "alice-desk.messages".to_string(),
                )],
                components: vec![placed_component("panel")],
                subscriptions: vec![surface_input(
                    "panel",
                    "messages",
                    "ephemeral:alice-desk.messages",
                )],
                ..surface("alice_desk", vec![AttachGrant::EphemeralSubscribe])
            }],
            ..Default::default()
        },
    );
}

/// The minimal surface: a grant, one instance and one input. Every optional
/// attr is omitted, so this row is where the defaults land — the wire slug
/// falls back to the handle, the instance name to the `new` handle, and
/// `chrome` to false.
#[test]
fn a_minimal_surface_states_only_its_grant_and_one_input() {
    assert_lowers(
        concat!(
            r#"
channel utterance at "ephemeral:alice-pod.utterance" {
    push_depth = 4;
    retain_depth = 16;
}

component Widget {
    "#,
            surface_any!(),
            r#"
    in heard;
}

surface alice_pod {
    grants = [subscribe];

    new widget: Widget {
        grants = [];
        in heard <- utterance { push_depth = 2; }
    }
}
"#
        ),
        BrennConfig {
            channels: vec![ChannelConfigRaw {
                push_depth: Some(Depth::Bounded(4)),
                retain_depth: Some(Depth::Bounded(16)),
                ..channel_at("ephemeral:alice-pod.utterance")
            }],
            surfaces: vec![SurfaceConfigRaw {
                ephemeral_subscribe_acl: vec![ChannelMatcherRaw::Exact(
                    "alice-pod.utterance".to_string(),
                )],
                components: vec![placed_component("widget")],
                subscriptions: vec![SurfaceSubscriptionRaw {
                    push_depth: Some(Depth::Bounded(2)),
                    ..surface_input("widget", "heard", "ephemeral:alice-pod.utterance")
                }],
                ..surface("alice_pod", vec![AttachGrant::EphemeralSubscribe])
            }],
            ..Default::default()
        },
    );
}

/// Two surfaces in one document, each with its own component class: lowering
/// zips a resolved surface with both its derived authority and its wire-kind
/// list, so a mis-pairing would render one surface with the other's component
/// kinds and subscribe with the other's ACLs. A one-surface row cannot see it.
#[test]
fn two_surfaces_keep_their_own_component_kinds_and_acls() {
    assert_lowers(
        concat!(
            r#"
channel alerts at "brenn:alice-alerts" {
    push_depth = 8;
    retain_depth = 128;
    standing_retain_depth = 128;
}

channel presence at "ephemeral:alice-desk.presence" {
    push_depth = 4;
    retain_depth = 16;
}

component Panel {
    "#,
            surface_any!(),
            r#"
    in messages;
}

component Board {
    "#,
            surface_any!(),
            r#"
    in feed;
}

surface alice_desk {
    grants = [subscribe];
    skin = "bench";

    new panel: Panel {
        grants = [];
        in messages <- alerts { push_depth = 4; }
    }
}

surface bob_desk {
    grants = [subscribe];
    skin = "lab";

    new board: Board {
        grants = [];
        in feed <- presence { push_depth = 2; }
    }
}
"#
        ),
        BrennConfig {
            channels: vec![
                ChannelConfigRaw {
                    uuid: Some("85a5cf7e-6874-5766-9d69-712784754a1f".to_string()),
                    push_depth: Some(Depth::Bounded(8)),
                    retain_depth: Some(Depth::Bounded(128)),
                    standing_retain_depth: Some(Depth::Bounded(128)),
                    ..channel_at("brenn:alice-alerts")
                },
                ChannelConfigRaw {
                    push_depth: Some(Depth::Bounded(4)),
                    retain_depth: Some(Depth::Bounded(16)),
                    ..channel_at("ephemeral:alice-desk.presence")
                },
            ],
            surfaces: vec![
                SurfaceConfigRaw {
                    skin: Some("bench".to_string()),
                    subscribe_acl: vec![ChannelMatcherRaw::Exact("alice-alerts".to_string())],
                    components: vec![placed_component("panel")],
                    subscriptions: vec![SurfaceSubscriptionRaw {
                        push_depth: Some(Depth::Bounded(4)),
                        ..surface_input("panel", "messages", "brenn:alice-alerts")
                    }],
                    ..surface("alice_desk", vec![AttachGrant::Subscribe])
                },
                SurfaceConfigRaw {
                    skin: Some("lab".to_string()),
                    ephemeral_subscribe_acl: vec![ChannelMatcherRaw::Exact(
                        "alice-desk.presence".to_string(),
                    )],
                    components: vec![placed_component("board")],
                    subscriptions: vec![SurfaceSubscriptionRaw {
                        push_depth: Some(Depth::Bounded(2)),
                        ..surface_input("board", "feed", "ephemeral:alice-desk.presence")
                    }],
                    ..surface("bob_desk", vec![AttachGrant::EphemeralSubscribe])
                },
            ],
            ..Default::default()
        },
    );
}

/// A binding tail's vocabulary is the union across the families the statement
/// can lower into, so the key a surface port has no field for — `amplification`,
/// a consumer's throughput knob — is refused at lowering, at its own entry,
/// naming the port and the keys that direction reads.
#[test]
fn amplification_on_a_surface_in_binding_is_refused() {
    let refusal = refusal(concat!(
        r#"
channel utterance at "ephemeral:alice-pod.utterance" {
    push_depth = 4;
    retain_depth = 16;
}

component Widget {
    "#,
        surface_any!(),
        r#"
    in heard;
}

surface alice_pod {
    grants = [subscribe];

    new widget: Widget {
        grants = [];
        in heard <- utterance { push_depth = 2; amplification = 0.5; }
    }
}
"#
    ));
    assert_eq!(
        refusal.message,
        "`amplification` is not a key of the `heard` port of instance `widget` of surface \
         `alice_pod`; expected `push_depth`, `retain_depth`, `noise` or `wake_min`"
    );
    assert_eq!(
        refusal.line_col(),
        Some((17, 65)),
        "the span is the refused key's own value: {}",
        refusal.render()
    );
}

/// The `io` twin of the refusal above: an io tail unions both directions, and
/// the surface families still hold no `amplification` field.
#[test]
fn amplification_on_a_surface_io_binding_is_refused() {
    let refusal = refusal(concat!(
        r#"
channel utterance at "ephemeral:alice-pod.utterance" {
    push_depth = 4;
    retain_depth = 16;
}

component Widget {
    "#,
        processor_needs!("ports"),
        r#"
    in heard;
    io tick;
}

surface alice_pod {
    grants = [subscribe];

    new widget: Widget {
        grants = [ports];
        in heard <- utterance { push_depth = 2; }
        io tick { push_depth = 1; retain_depth = 2; amplification = 0.5; }
    }
}
"#
    ));
    assert_eq!(
        refusal.message,
        "`amplification` is not a key of the `tick` port of instance `widget` of surface \
         `alice_pod`; expected `push_depth`, `retain_depth`, `noise`, `urgency`, \
         `publish_per_activation` or `publish_capacity`"
    );
    assert_eq!(
        refusal.line_col(),
        Some((19, 69)),
        "the span is the refused key's own value: {}",
        refusal.render()
    );
}

/// The surface twin of the one-token-one-diagnostic rule: a refused
/// `amplification` is not also read as a number.
#[test]
fn a_refused_surface_binding_key_is_not_also_value_checked() {
    let refusal = refusal(concat!(
        r#"
channel utterance at "ephemeral:alice-pod.utterance" {
    push_depth = 4;
    retain_depth = 16;
}

component Widget {
    "#,
        surface_any!(),
        r#"
    in heard;
}

surface alice_pod {
    grants = [subscribe];

    new widget: Widget {
        grants = [];
        in heard <- utterance { push_depth = 2; amplification = "half"; }
    }
}
"#
    ));
    assert!(
        refusal
            .message
            .starts_with("`amplification` is not a key of"),
        "{}",
        refusal.render()
    );
}

/// A component body's values are typed at lowering, and a refusal cites the
/// offending token rather than the instance.
#[test]
fn a_bad_value_in_a_surface_component_body_is_refused() {
    let refusal = refusal(concat!(
        r#"
channel utterance at "ephemeral:alice-pod.utterance" {
    push_depth = 4;
    retain_depth = 16;
}

component Widget {
    "#,
        surface_any!(),
        r#"
    in heard;
}

surface alice_pod {
    grants = [subscribe];

    new widget: Widget {
        grants = [];
        parked_batch_depth = -1;

        in heard <- utterance { push_depth = 2; }
    }
}
"#
    ));
    assert_eq!(
        refusal.message,
        "`parked_batch_depth`: expected a non-negative integer or the word `unbounded`, got -1"
    );
}

/// A surface component's `config` is a map of strings, not the `toml::Table` a
/// consumer's is, so a non-string value is refused at that value.
#[test]
fn a_non_string_in_a_surface_components_config_is_refused() {
    let refusal = refusal(concat!(
        r#"
channel utterance at "ephemeral:alice-pod.utterance" {
    push_depth = 4;
    retain_depth = 16;
}

component Widget {
    "#,
        surface_any!(),
        r#"
    in heard;
}

surface alice_pod {
    grants = [subscribe];

    new widget: Widget {
        grants = [];
        config = { depth = 3 };

        in heard <- utterance { push_depth = 2; }
    }
}
"#
    ));
    assert_eq!(
        refusal.message,
        "`config`: expected a string, got an integer"
    );
}

// ---------------------------------------------------------------------------
// Remotes
// ---------------------------------------------------------------------------

/// A remote stating every attr, with ceilings on both subscribe planes and
/// matchers on both publish planes.
#[test]
fn a_remote_lowers_with_ceilings_on_both_subscribe_planes() {
    assert_lowers(
        r#"
remote bob_pod {
    token_file = "/home/alice/.secrets/bob-pod.token";
    grants = [subscribe, publish, alert];
    publish_burst = 16;
    publish_per_sec = 2;
    max_sessions = 4;
    max_subscriptions = 64;

    acl subscribe [
        exact "brenn:alice.cmd" { push_depth = 0, retain_depth = 32 },
        prefix "ephemeral:alice." { push_depth = 8, retain_depth = 1 }
    ];
    acl publish [prefix "brenn:alice.in.", exact "ephemeral:bob.presence"];
}
"#,
        BrennConfig {
            remotes: vec![RemoteConfigRaw {
                grants: vec![
                    AttachGrant::Subscribe,
                    AttachGrant::EphemeralSubscribe,
                    AttachGrant::Publish,
                    AttachGrant::EphemeralPublish,
                    AttachGrant::Alert,
                ],
                subscribe_acl: vec![remote_exact("alice.cmd", 0, 32)],
                ephemeral_subscribe_acl: vec![remote_prefix("alice.", 8, 1)],
                publish_acl: vec![ChannelMatcherRaw::Prefix("alice.in.".to_string())],
                ephemeral_publish_acl: vec![ChannelMatcherRaw::Exact("bob.presence".to_string())],
                publish_burst: Some(16),
                publish_per_sec: Some(2),
                max_sessions: Some(4),
                max_subscriptions: Some(64),
                ..remote("bob_pod", "/home/alice/.secrets/bob-pod.token")
            }],
            ..Default::default()
        },
    );
}

/// The minimal remote: a token file, one grant and the entry that grant is
/// about. Every optional attr is omitted, so every ceiling stays absent and
/// resolution supplies its own.
#[test]
fn a_minimal_remote_states_only_its_token_file_and_one_entry() {
    assert_lowers(
        r#"
remote bob_pod {
    token_file = "/home/alice/.secrets/bob-pod.token";
    grants = [subscribe];

    acl subscribe [exact "brenn:alice.cmd" { push_depth = 1, retain_depth = 8 }];
}
"#,
        BrennConfig {
            remotes: vec![RemoteConfigRaw {
                grants: vec![AttachGrant::Subscribe],
                subscribe_acl: vec![remote_exact("alice.cmd", 1, 8)],
                ..remote("bob_pod", "/home/alice/.secrets/bob-pod.token")
            }],
            ..Default::default()
        },
    );
}

/// Two remotes in one document, with different ceilings on different planes:
/// lowering zips a resolved remote with its derived authority by position, and
/// a mis-pairing here hands one peer the other's subscribe ceilings. A
/// one-remote row cannot see it.
#[test]
fn two_remotes_keep_their_own_ceilings() {
    assert_lowers(
        r#"
remote bob_pod {
    token_file = "/home/alice/.secrets/bob-pod.token";
    grants = [subscribe];
    max_sessions = 4;

    acl subscribe [exact "brenn:alice.cmd" { push_depth = 1, retain_depth = 8 }];
}

remote charlie_pod {
    token_file = "/home/alice/.secrets/charlie-pod.token";
    grants = [subscribe, publish];
    max_sessions = 2;

    acl subscribe [prefix "ephemeral:alice." { push_depth = 8, retain_depth = 1 }];
    acl publish [exact "brenn:alice.in.charlie"];
}
"#,
        BrennConfig {
            remotes: vec![
                RemoteConfigRaw {
                    grants: vec![AttachGrant::Subscribe],
                    subscribe_acl: vec![remote_exact("alice.cmd", 1, 8)],
                    max_sessions: Some(4),
                    ..remote("bob_pod", "/home/alice/.secrets/bob-pod.token")
                },
                RemoteConfigRaw {
                    grants: vec![AttachGrant::EphemeralSubscribe, AttachGrant::Publish],
                    ephemeral_subscribe_acl: vec![remote_prefix("alice.", 8, 1)],
                    publish_acl: vec![ChannelMatcherRaw::Exact("alice.in.charlie".to_string())],
                    max_sessions: Some(2),
                    ..remote("charlie_pod", "/home/alice/.secrets/charlie-pod.token")
                },
            ],
            ..Default::default()
        },
    );
}

/// A remote attr is typed at lowering like any other, and a count out of the
/// target's range is refused at the token that wrote it.
#[test]
fn a_remote_count_out_of_range_is_refused() {
    let refusal = refusal(
        r#"
remote bob_pod {
    token_file = "/home/alice/.secrets/bob-pod.token";
    grants = [subscribe];
    max_sessions = 5000000000;

    acl subscribe [exact "brenn:alice.cmd" { push_depth = 1, retain_depth = 8 }];
}
"#,
    );
    assert_eq!(
        refusal.message,
        "`max_sessions`: 5000000000 is out of range for this key"
    );
}

// ---------------------------------------------------------------------------
// Webhook endpoints
// ---------------------------------------------------------------------------

/// A webhook stating every attr, an HMAC signature scheme, two key entries and
/// a replay-protection binding with a nested config map.
#[test]
fn a_webhook_endpoint_lowers_with_every_block() {
    assert_lowers(
        r#"
webhook alice_inbox {
    slug = "alice-inbox";
    mount = "/webhooks/alice-inbox";
    description = "Pushes from alice's phone.";
    transport_ceiling_bytes = 65536;
    content_type = "application/json";
    urgency = high;

    signature {
        scheme = hmac-raw-body;
        algorithm = "hmac-sha512";
        header = "x-signature";
        format = "hex-lower";
        key_id_header = "x-key-id";
    }

    key primary { secret_file = "/home/alice/.secrets/inbox-primary.key"; }
    key rotated { secret_file = "/home/alice/.secrets/inbox-rotated.key"; }

    replay_protection {
        component = "replay-generic";
        store_path = "/var/lib/brenn/alice-inbox-replay.sqlite";
        store_size_limit = "128MiB";
        config = { window_secs = 300, strict = true };
    }
}
"#,
        BrennConfig {
            webhook_endpoints: vec![WebhookEndpointConfigRaw {
                slug: "alice-inbox".to_string(),
                mount: Some("/webhooks/alice-inbox".to_string()),
                description: Some("Pushes from alice's phone.".to_string()),
                transport_ceiling_bytes: 65536,
                content_type: "application/json".to_string(),
                signature: WebhookSignatureConfigRaw::HmacRawBody {
                    algorithm: "hmac-sha512".to_string(),
                    header: "x-signature".to_string(),
                    format: "hex-lower".to_string(),
                    key_id_header: Some("x-key-id".to_string()),
                },
                keys: vec![
                    webhook_key("primary", "/home/alice/.secrets/inbox-primary.key"),
                    webhook_key("rotated", "/home/alice/.secrets/inbox-rotated.key"),
                ],
                tokens: vec![],
                replay_protection: Some(ReplayProtectionConfigRaw {
                    component: "replay-generic".to_string(),
                    store_path: PathBuf::from("/var/lib/brenn/alice-inbox-replay.sqlite"),
                    store_size_limit: Some("128MiB".to_string()),
                    config: Some(toml::Table::from_iter([
                        ("window_secs".to_string(), toml::Value::Integer(300)),
                        ("strict".to_string(), toml::Value::Boolean(true)),
                    ])),
                }),
                urgency: Some(Urgency::High),
            }],
            ..Default::default()
        },
    );
}

/// The minimal webhook: a bearer scheme and the one token it checks against.
/// Every optional attr is omitted, so this row is where the defaults land — the
/// wire slug falls back to the handle, and `transport_ceiling_bytes` and
/// `content_type` to the module's own default functions.
#[test]
fn a_minimal_webhook_endpoint_states_only_its_scheme_and_one_token() {
    assert_lowers(
        r#"
webhook alice_inbox {
    signature {
        scheme = bearer-token;
        header = "authorization";
    }

    token phone { secret_file = "/home/alice/.secrets/inbox-phone.token"; }
}
"#,
        BrennConfig {
            webhook_endpoints: vec![WebhookEndpointConfigRaw {
                signature: WebhookSignatureConfigRaw::BearerToken {
                    header: "authorization".to_string(),
                    token_id_header: None,
                },
                tokens: vec![webhook_token(
                    "phone",
                    "/home/alice/.secrets/inbox-phone.token",
                )],
                ..webhook_endpoint("alice_inbox")
            }],
            ..Default::default()
        },
    );
}

/// The timestamped-body scheme, whose fields are the widest of the four: the
/// signature parity row for that variant.
#[test]
fn the_timestamped_body_signature_scheme_lowers() {
    assert_lowers(
        r#"
webhook alice_inbox {
    signature {
        scheme = hmac-timestamped-body;
        sig_header = "x-signature";
        sig_format = "v0=hex-lower";
        timestamp_header = "x-request-timestamp";
        template = "v0:{t}:{body}";
        max_skew_secs = 300;
    }

    key primary { secret_file = "/home/alice/.secrets/inbox-primary.key"; }
}
"#,
        BrennConfig {
            webhook_endpoints: vec![WebhookEndpointConfigRaw {
                signature: WebhookSignatureConfigRaw::HmacTimestampedBody {
                    algorithm: default_hmac_algorithm(),
                    sig_header: "x-signature".to_string(),
                    sig_format: "v0=hex-lower".to_string(),
                    timestamp_header: "x-request-timestamp".to_string(),
                    template: "v0:{t}:{body}".to_string(),
                    max_skew_secs: 300,
                    key_id_header: None,
                },
                keys: vec![webhook_key(
                    "primary",
                    "/home/alice/.secrets/inbox-primary.key",
                )],
                ..webhook_endpoint("alice_inbox")
            }],
            ..Default::default()
        },
    );
}

/// The combined-header scheme: the signature parity row for that variant.
#[test]
fn the_stripe_signature_scheme_lowers() {
    assert_lowers(
        r#"
webhook alice_inbox {
    signature {
        scheme = hmac-stripe;
        header = "stripe-signature";
        max_skew_secs = 300;
    }

    key primary { secret_file = "/home/alice/.secrets/inbox-primary.key"; }
}
"#,
        BrennConfig {
            webhook_endpoints: vec![WebhookEndpointConfigRaw {
                signature: WebhookSignatureConfigRaw::HmacStripe {
                    algorithm: default_hmac_algorithm(),
                    header: "stripe-signature".to_string(),
                    max_skew_secs: 300,
                    key_id_header: None,
                },
                keys: vec![webhook_key(
                    "primary",
                    "/home/alice/.secrets/inbox-primary.key",
                )],
                ..webhook_endpoint("alice_inbox")
            }],
            ..Default::default()
        },
    );
}

/// The `signature` vocabulary is the union of every scheme's fields, so an attr
/// belonging to another variant is refused at lowering, at the key that was
/// written and naming the fields the chosen scheme reads.
#[test]
fn a_signature_attr_the_chosen_scheme_has_no_field_for_is_refused() {
    let refusal = refusal(
        r#"
webhook alice_inbox {
    signature {
        scheme = bearer-token;
        header = "authorization";
        max_skew_secs = 300;
    }

    token phone { secret_file = "/home/alice/.secrets/inbox-phone.token"; }
}
"#,
    );
    assert_eq!(
        refusal.message,
        "`max_skew_secs` is not a key of `signature` of webhook `alice_inbox`; expected \
         `scheme`, `header` or `token_id_header`"
    );
}

/// A field the chosen scheme requires and the block does not state is refused
/// at the block, which is the finest position a missing key has.
#[test]
fn a_signature_missing_a_required_field_is_refused() {
    let refusal = refusal(
        r#"
webhook alice_inbox {
    signature {
        scheme = hmac-raw-body;
        header = "x-signature";
    }

    key primary { secret_file = "/home/alice/.secrets/inbox-primary.key"; }
}
"#,
    );
    assert_eq!(
        refusal.message,
        "`signature` of webhook `alice_inbox` states no `format`, which it requires"
    );
}

/// The scheme word is matched against the schemes there are, and a word that is
/// not one of them is refused naming the four that are.
#[test]
fn an_unknown_signature_scheme_is_refused() {
    let refusal = refusal(
        r#"
webhook alice_inbox {
    signature {
        scheme = hmac-blake3;
        header = "x-signature";
    }
}
"#,
    );
    assert_eq!(
        refusal.message,
        "`hmac-blake3` is not a signature scheme; expected `hmac-raw-body`, \
         `hmac-timestamped-body`, `hmac-stripe` or `bearer-token`"
    );
    assert_eq!(
        refusal.span.text_str(),
        Some("hmac-blake3"),
        "the span is the scheme word's own, not the block's"
    );
}

/// A webhook with no signature block is refused rather than defaulted: which
/// scheme guards an endpoint is the reason for declaring one.
#[test]
fn a_webhook_endpoint_with_no_signature_block_is_refused() {
    let refusal = refusal(
        r#"
webhook alice_inbox {
    mount = "/webhooks/alice-inbox";
}
"#,
    );
    assert_eq!(
        refusal.message,
        "webhook `alice_inbox` states no `signature` block: which scheme guards an endpoint \
         has no default"
    );
}

// ---------------------------------------------------------------------------
// Integration sections
// ---------------------------------------------------------------------------

/// The one open-bodied section: every key the body wrote reaches the config's
/// `toml::Value` tree, scalars and nested tables alike.
#[test]
fn integration_sections_lower_to_a_value_tree_of_every_key_they_state() {
    assert_lowers(
        r#"
integration graf {
    command = "graf";
    timeout_secs = 30;
    strict = true;
    ratio = 0.5;
    args = ["mcp", "--quiet"];
    env = { GRAF_ROOT = "/home/alice/kb", GRAF_LOG = "warn" };
}

integration pfin { command = "pf"; }
"#,
        BrennConfig {
            integrations: HashMap::from_iter([
                (
                    "graf".to_string(),
                    toml::Value::Table(toml::Table::from_iter([
                        (
                            "command".to_string(),
                            toml::Value::String("graf".to_string()),
                        ),
                        ("timeout_secs".to_string(), toml::Value::Integer(30)),
                        ("strict".to_string(), toml::Value::Boolean(true)),
                        ("ratio".to_string(), toml::Value::Float(0.5)),
                        (
                            "args".to_string(),
                            toml::Value::Array(vec![
                                toml::Value::String("mcp".to_string()),
                                toml::Value::String("--quiet".to_string()),
                            ]),
                        ),
                        (
                            "env".to_string(),
                            toml::Value::Table(toml::Table::from_iter([
                                (
                                    "GRAF_ROOT".to_string(),
                                    toml::Value::String("/home/alice/kb".to_string()),
                                ),
                                (
                                    "GRAF_LOG".to_string(),
                                    toml::Value::String("warn".to_string()),
                                ),
                            ])),
                        ),
                    ])),
                ),
                (
                    "pfin".to_string(),
                    toml::Value::Table(toml::Table::from_iter([(
                        "command".to_string(),
                        toml::Value::String("pf".to_string()),
                    )])),
                ),
            ]),
            ..Default::default()
        },
    );
}

/// A body nesting two levels deep: an inline table inside an inline table
/// nests the value tree the same way.
#[test]
fn an_integration_body_nests_as_deep_as_it_is_written() {
    assert_lowers(
        r#"
integration graf {
    limits = { queries = { per_minute = 60, burst = 10 }, bytes = 4096 };
}
"#,
        BrennConfig {
            integrations: HashMap::from_iter([(
                "graf".to_string(),
                toml::Value::Table(toml::Table::from_iter([(
                    "limits".to_string(),
                    toml::Value::Table(toml::Table::from_iter([
                        (
                            "queries".to_string(),
                            toml::Value::Table(toml::Table::from_iter([
                                ("per_minute".to_string(), toml::Value::Integer(60)),
                                ("burst".to_string(), toml::Value::Integer(10)),
                            ])),
                        ),
                        ("bytes".to_string(), toml::Value::Integer(4096)),
                    ])),
                )])),
            )]),
            ..Default::default()
        },
    );
}

#[test]
fn a_matcher_in_an_integration_body_is_refused() {
    let refusal = refusal(
        r#"
integration graf {
    command = "graf";
    store_path = exact "alice.";
}
"#,
    );
    assert_eq!(
        refusal.message,
        "`store_path`: a matcher is not a value here"
    );
    assert_eq!(
        refusal.line_col(),
        Some((4, 18)),
        "the span is the matcher's own token: {}",
        refusal.render()
    );
}

// ---------------------------------------------------------------------------
// Attachment targets
// ---------------------------------------------------------------------------

/// A target stating every key, and the one handler type there is.
#[test]
fn an_attachment_target_lowers_with_every_key_and_its_handler() {
    assert_lowers(
        r#"
agent Assistant() {
    attachment_target import {
        label = "Import bank export";
        accept = [".ofx", ".qfx", ".csv"];
        multi = true;
        handler {
            type = command;
            program = "pf";
            args = ["--json", "import", "{ofx}", "--csv", "{csv}"];
            timeout_secs = 120;
            cc_instructions = "Reconcile the import against the ledger.";
            file_roles = { ofx = [".ofx", ".qfx"], csv = [".csv"] };
        }
    }
}

new alice: Assistant();
"#,
        BrennConfig {
            apps: vec![AppConfigRaw {
                slug: "alice".to_string(),
                attachment_targets: vec![AttachmentTargetRaw {
                    name: "import".to_string(),
                    label: "Import bank export".to_string(),
                    accept: vec![".ofx".to_string(), ".qfx".to_string(), ".csv".to_string()],
                    multi: true,
                    handler: AttachmentHandlerConfig::Command {
                        program: "pf".to_string(),
                        args: vec![
                            "--json".to_string(),
                            "import".to_string(),
                            "{ofx}".to_string(),
                            "--csv".to_string(),
                            "{csv}".to_string(),
                        ],
                        file_roles: HashMap::from_iter([
                            (
                                "ofx".to_string(),
                                vec![".ofx".to_string(), ".qfx".to_string()],
                            ),
                            ("csv".to_string(), vec![".csv".to_string()]),
                        ]),
                        timeout_secs: 120,
                        cc_instructions: Some(
                            "Reconcile the import against the ledger.".to_string(),
                        ),
                    },
                }],
                ..Default::default()
            }],
            ..Default::default()
        },
    );
}

/// The minimal target: `multi` and `timeout_secs` omitted, so `multi` is false
/// and the timeout is the module's default. The wire name falls back to the
/// block's own name, and an explicit `name` overrides it, which is why the
/// second target states one.
#[test]
fn a_minimal_attachment_target_states_only_its_label_accept_and_handler() {
    assert_lowers(
        r#"
agent Assistant() {
    attachment_target import {
        label = "Import";
        accept = [".ofx"];
        handler { type = command; program = "pf"; args = ["import"]; file_roles = {}; }
    }
    attachment_target receipt {
        name = "receipt-scan";
        label = "Scan a receipt";
        accept = [".jpg"];
        handler { type = command; program = "pf"; args = ["scan"]; file_roles = {}; }
    }
}

new alice: Assistant();
"#,
        BrennConfig {
            apps: vec![AppConfigRaw {
                slug: "alice".to_string(),
                attachment_targets: vec![
                    AttachmentTargetRaw {
                        name: "import".to_string(),
                        label: "Import".to_string(),
                        accept: vec![".ofx".to_string()],
                        multi: false,
                        handler: command_handler("pf", &["import"]),
                    },
                    AttachmentTargetRaw {
                        name: "receipt-scan".to_string(),
                        label: "Scan a receipt".to_string(),
                        accept: vec![".jpg".to_string()],
                        multi: false,
                        handler: command_handler("pf", &["scan"]),
                    },
                ],
                ..Default::default()
            }],
            ..Default::default()
        },
    );
}

// The union vocabulary has exactly one variant's fields in it today, so a key
// the chosen type has no field for cannot be written: the vocabulary refuses it
// first, and that refusal is tested in brenn-dsl. `Body::finish` here is the
// belt that becomes reachable the day a second handler type lands.

/// A field the chosen type requires and the block does not state is refused at
/// the block, which is the finest position a missing key has.
#[test]
fn a_handler_missing_a_required_field_is_refused() {
    let refusal = refusal(
        r#"
agent Assistant() {
    attachment_target import {
        label = "Import";
        accept = [".ofx"];
        handler { type = command; program = "pf"; file_roles = {}; }
    }
}

new alice: Assistant();
"#,
    );
    assert_eq!(
        refusal.message,
        "`handler` of `attachment_target import` of app `alice` states no `args`, which it requires"
    );
    assert_eq!(
        refusal.line_col(),
        Some((6, 9)),
        "the position is the `handler` kindword's, which is the finest a missing key has: {}",
        refusal.render()
    );
}

/// `file_roles` is required, and a required map is refused when it is absent
/// like every other required field — not read as an empty one.
#[test]
fn a_handler_stating_no_file_roles_is_refused() {
    let refusal = refusal(
        r#"
agent Assistant() {
    attachment_target import {
        label = "Import";
        accept = [".ofx"];
        handler { type = command; program = "pf"; args = ["import"]; }
    }
}

new alice: Assistant();
"#,
    );
    assert_eq!(
        refusal.message,
        "`handler` of `attachment_target import` of app `alice` states no `file_roles`, \
         which it requires"
    );
}

/// The type word is matched against the handler types there are, so an unknown
/// one names them.
#[test]
fn an_unknown_handler_type_names_the_legal_set() {
    let refusal = refusal(
        r#"
agent Assistant() {
    attachment_target import {
        label = "Import";
        accept = [".ofx"];
        handler { type = webhook; program = "pf"; args = ["import"]; }
    }
}

new alice: Assistant();
"#,
    );
    assert_eq!(
        refusal.message,
        "`webhook` is not an attachment handler type; expected `command`"
    );
}

/// `file_roles` maps a role to the extensions that fill it, so a value that is
/// not a list of strings is refused at that value.
#[test]
fn a_file_role_that_is_not_a_list_of_strings_is_refused() {
    let refusal = refusal(
        r#"
agent Assistant() {
    attachment_target import {
        label = "Import";
        accept = [".ofx"];
        handler {
            type = command;
            program = "pf";
            args = ["import"];
            file_roles = { ofx = ".ofx" };
        }
    }
}

new alice: Assistant();
"#,
    );
    assert_eq!(
        refusal.message,
        "`file_roles`: expected a list of strings, got a string"
    );
}

/// The whole map, not one entry of it: a `file_roles` that is not a table is
/// refused at the value.
#[test]
fn a_file_roles_that_is_not_a_table_is_refused() {
    let refusal = refusal(
        r#"
agent Assistant() {
    attachment_target import {
        label = "Import";
        accept = [".ofx"];
        handler {
            type = command;
            program = "pf";
            args = ["import"];
            file_roles = [".ofx"];
        }
    }
}

new alice: Assistant();
"#,
    );
    assert_eq!(
        refusal.message,
        "`file_roles`: expected a table of string lists, got a list"
    );
}

/// A consumer's `tool` statements lower to the raw grants the registry
/// resolves, clause for clause and bucket for bucket.
#[test]
fn a_consumers_tool_statements_lower_to_raw_grants() {
    let config = config_from_dsl(concat!(
        "// ── packaged ──\n",
        "component Sink {\n    abi = processor; requires = [tools];\n}\n",
        "// ── packaged ──\n",
        "new alice_sink: Sink {\n",
        "    grants = [tools];\n",
        "    tool git-repo-pull {\n",
        "        allow { repo = \"ws\"; }\n",
        "        allow { repo = \"notes\"; }\n",
        "        rate_limit { burst = 2; sustained_per_minute = 10; }\n",
        "    }\n",
        "}\n",
    ));
    assert_eq!(
        config.wasm_consumers[0].tool_grants,
        vec![crate::tools::config::ToolGrantRaw {
            tool: "git-repo-pull".to_string(),
            acl: vec![
                std::collections::BTreeMap::from([("repo".to_string(), "ws".to_string())]),
                std::collections::BTreeMap::from([("repo".to_string(), "notes".to_string())]),
            ],
            rate_limit: Some(crate::tools::config::RateLimitRaw {
                burst: 2,
                sustained_per_minute: 10,
            }),
        }]
    );
}

/// A `link` lowers to a `[[link]]` whose endpoints carry their roles and, on
/// the subscribing halves, the window their own port declarations state.
///
/// Written across the wire — a backend consumer and a surface-placed instance —
/// because that is the shape whose two host arms differ, and every binding
/// direction rides along.
#[test]
fn a_link_lowers_to_its_endpoint_set() {
    let document = format!(
        concat!(
            "// ── packaged ──\n",
            "component Feeder {{\n{}\n    out events;\n}}\n",
            "// ── packaged ──\n",
            "// ── packaged ──\n",
            "component Panel {{\n{}\n    in feed;\n    io chatter;\n}}\n",
            "// ── packaged ──\n",
            "/// Frames the feeder hands the panel.\n",
            // The description an operator reads keeps the author's shape:
            // a deeper indent inside the doc block survives lowering.
            "///     wire format: one frame per event\n",
            "link relay;\n",
            "surface desk {{\n",
            // The surface states no rights: what a link's endpoints need is
            // injected at boot, once the channel it places exists.
            "    grants = [];\n",
            "    new view: Panel {{\n",
            "        grants = [ports];\n",
            "        in feed <- relay {{ push_depth = 8; retain_depth = 16; }}\n",
            "        io chatter <-> relay {{ push_depth = 2; retain_depth = 4; }}\n",
            "    }}\n",
            "}}\n",
            "new feeder: Feeder {{\n",
            "    grants = [ports];\n",
            "    out events -> relay;\n",
            "}}\n",
        ),
        processor_needs!("ports"),
        processor_needs!("ports"),
    );
    let config = config_from_dsl(&document);
    assert_eq!(
        config.links,
        vec![LinkConfigRaw {
            link: "relay".to_string(),
            description: Some(
                "Frames the feeder hands the panel.\n    wire format: one frame per event"
                    .to_string()
            ),
            endpoints: vec![
                LinkEndpointRaw {
                    host: LinkHostRaw::Wasm {
                        slug: "feeder".to_string(),
                    },
                    port: "events".to_string(),
                    publishes: true,
                    subscribes: false,
                    io_port: false,
                    push_depth: None,
                    retain_depth: None,
                },
                LinkEndpointRaw {
                    host: LinkHostRaw::Surface {
                        slug: "desk".to_string(),
                        instance: "view".to_string(),
                    },
                    port: "feed".to_string(),
                    publishes: false,
                    subscribes: true,
                    io_port: false,
                    push_depth: Some(Depth::Bounded(8)),
                    retain_depth: Some(Depth::Bounded(16)),
                },
                LinkEndpointRaw {
                    host: LinkHostRaw::Surface {
                        slug: "desk".to_string(),
                        instance: "view".to_string(),
                    },
                    port: "chatter".to_string(),
                    publishes: true,
                    subscribes: true,
                    io_port: true,
                    push_depth: Some(Depth::Bounded(2)),
                    retain_depth: Some(Depth::Bounded(4)),
                },
            ],
        }],
    );
    // Every link-bound port lowers channel-less: boot places the ring.
    assert!(config.wasm_consumers[0].outputs[0].channel.is_none());
    assert!(config.surfaces[0].subscriptions[0].channel.is_none());
    assert!(config.surfaces[0].io_ports[0].channel.is_none());
}
