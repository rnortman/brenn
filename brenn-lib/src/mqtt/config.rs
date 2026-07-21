//! MQTT config types: raw (TOML deserialized) and resolved forms.
//!
//! Wired into `BrennConfig` via:
//! - top-level `[[mqtt_client]]` arrays → `Vec<MqttClientConfigRaw>`
//!
//! Validation free functions paralleling `messaging::config::resolve_app_messaging`.

use std::collections::HashSet;
use std::path::PathBuf;

use indexmap::IndexMap;
use serde::Deserialize;
use uuid::Uuid;

use crate::config::{AppConfigRaw, load_secret_file};
use crate::messaging::config::{Depth, NoiseLevel};
use crate::messaging::{Urgency, WakeMin, mqtt_channel_uuid_from_address};
use crate::mqtt::address::{parse_mqtt_address, validate_topic_filter_str};

// ---------------------------------------------------------------------------
// Raw config types (TOML deserialized)
// ---------------------------------------------------------------------------

/// Top-level `[[mqtt_client]]` block.
///
/// # Security — prompt injection via MQTT
///
/// Every MQTT client with write access to a topic this app subscribes to can
/// inject arbitrary text into the LLM context. This is an inherent property of
/// inbound MQTT: payloads arrive from the broker as opaque bytes and are treated
/// as untrusted by Brenn, but the LLM sees them verbatim.
///
/// Mitigation: configure the broker (e.g., mosquitto ACL file or MQTT 5 auth
/// plugin) to restrict publish rights so only authorised clients can write to
/// topics the app reads. The principle of least privilege applies: each client
/// should be allowed to publish only to the topics it owns.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MqttClientConfigRaw {
    /// URL-safe identifier for this client; charset `[A-Za-z0-9._~-]+`.
    pub slug: String,
    /// Broker URL. Must be `mqtts://` scheme.
    pub url: String,
    /// MQTT username, if required by the broker.
    pub username: Option<String>,
    /// Path to a file containing the MQTT password (trimmed of whitespace).
    /// Required when `username` is set.
    pub password_file: Option<PathBuf>,
    /// Path to a PEM CA certificate file. When absent, system trust store used.
    pub ca_file: Option<PathBuf>,
    /// Minimum TLS version. Default: `"1.2"`.
    #[serde(default = "default_tls_version_min")]
    pub tls_version_min: String,
    /// MQTT keepalive interval in seconds.
    pub keepalive_secs: Option<u32>,
    /// Maximum inbound payload size in bytes. Default: 4 MiB.
    #[serde(default = "default_inbound_payload_cap")]
    pub inbound_payload_cap_bytes: usize,
    /// Optional Last-Will configuration.
    pub last_will: Option<LastWillRaw>,
    /// Reconnect backoff initial delay in seconds. Default: 1.
    #[serde(default = "default_backoff_initial")]
    pub reconnect_backoff_initial_secs: u32,
    /// Reconnect backoff maximum delay in seconds. Default: 60.
    #[serde(default = "default_backoff_max")]
    pub reconnect_backoff_max_secs: u32,
    /// Broker SUBSCRIBE QoS for this client's ingress subscriptions
    /// (per-connection default). Transport/sender-side feed property; applied to
    /// every `[[app.mqtt_subscription]]` naming this client. Default: 1.
    #[serde(default = "default_subscription_qos")]
    pub qos: u8,
    /// Sender-side injection urgency stamped on every inbound message ingested
    /// via this client (distinct from the subscriber-side `wake_min`). Applied
    /// to all of this client's ingress subscriptions. Default: `normal`.
    #[serde(default = "default_client_urgency")]
    pub urgency: Urgency,
    /// Broker-side session expiry in seconds for this client's single shared
    /// session. `0` (default) = ephemeral: the broker discards session state on
    /// disconnect.
    #[serde(default)]
    pub session_expiry_secs: u32,
}

fn default_client_urgency() -> Urgency {
    Urgency::Normal
}

fn default_tls_version_min() -> String {
    "1.2".to_string()
}

fn default_inbound_payload_cap() -> usize {
    4 * 1024 * 1024 // 4 MiB
}

fn default_backoff_initial() -> u32 {
    1
}

fn default_backoff_max() -> u32 {
    60
}

/// Last-Will configuration for a broker connection.
#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct LastWillRaw {
    /// Topic on which the will is published.
    pub topic: String,
    /// Will payload as UTF-8 string.
    pub payload: String,
    /// QoS level: 0, 1, or 2.
    pub qos: u8,
    /// Whether the will is retained.
    pub retain: bool,
}

fn default_subscription_qos() -> u8 {
    1
}

/// Per-app `[[app.mqtt_subscription]]` block (ingress).
///
/// An app subscribes to an MQTT ingress channel by naming its full address
/// `mqtt:<client>:<topic>` (the client segment is mandatory). Many apps may
/// subscribe to the same `(client, topic)` (multi-subscriber fanout via the
/// bus). The subscriber-side params are the shared generic messaging set —
/// identical to `[[app.messaging.subscribe]]` (`MessagingSubscriptionRaw`) —
/// resolved via the same sub → channel → global ladder.
///
/// Transport/sender-side feed properties (`qos`/`urgency`) live on
/// `[[mqtt_client]]`, not here.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppMqttIngressSubscriptionRaw {
    /// Full channel address `mqtt:<client>:<topic>`; client segment mandatory.
    pub channel: String,
    /// Per-subscription push depth. `None` ⇒ inherit (channel → global).
    pub push_depth: Option<Depth>,
    /// Per-subscription retain depth. `None` ⇒ inherit (channel → global).
    pub retain_depth: Option<Depth>,
    /// Per-subscription noise level for push overflow. `None` ⇒ inherit.
    pub noise: Option<NoiseLevel>,
    /// Per-subscription wake-min policy (subscriber side). `None` ⇒ inherit
    /// (channel → global).
    pub wake_min: Option<WakeMin>,
}

// ---------------------------------------------------------------------------
// Resolved config types
// ---------------------------------------------------------------------------

/// Minimum TLS protocol version accepted for a broker connection.
///
/// Config TOML string values `"1.2"` and `"1.3"` are parsed into this enum
/// during `resolve_clients`; any other value panics at startup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TlsVersionMin {
    /// TLS 1.2 or later (rumqttc default; equivalent to `"1.2"` in TOML).
    Tls12,
    /// TLS 1.3 only; a custom `rustls::ClientConfig` is built to enforce this.
    Tls13,
}

/// Resolved `[[mqtt_client]]` entry, with secrets loaded and fields validated.
///
/// This is Brenn's *client* connection to a remote MQTT broker (server). The
/// `slug` is the `<client>` segment of channel addresses; `host`/`port`/
/// credentials are the coordinates of the broker (server) this client dials.
/// The raw `[[mqtt_client]].url` is parsed into `host`/`port` once, here, at
/// config resolution.
#[derive(Clone)]
pub struct MqttClientConfig {
    pub slug: String,
    /// Broker hostname, or bare IP literal (IPv6 brackets stripped).
    pub host: String,
    pub port: u16,
    pub username: Option<String>,
    /// Password loaded from `password_file`, trimmed.
    pub password: Option<String>,
    /// CA certificate PEM bytes, if a `ca_file` was provided.
    pub ca_cert_pem: Option<Vec<u8>>,
    pub tls_version_min: TlsVersionMin,
    pub keepalive_secs: Option<u32>,
    pub inbound_payload_cap_bytes: u32,
    pub last_will: Option<LastWillRaw>,
    pub reconnect_backoff_initial_secs: u32,
    pub reconnect_backoff_max_secs: u32,
    /// Broker SUBSCRIBE QoS for this client's ingress subscriptions.
    pub qos: u8,
    /// Sender-side injection urgency stamped on inbound messages from this client.
    pub urgency: Urgency,
    /// Broker-side session expiry in seconds for the single shared session.
    pub session_expiry_secs: u32,
}

/// Manual `Debug` that never renders the broker `password` (a secret loaded from
/// `password_file`; security-posture item 11 treats logging a secret as a High
/// regression) and elides the bulky `ca_cert_pem` bytes to a length. The config
/// now rides on the widely-shared `MqttClientHandle`, so keep it structurally
/// impossible to dump the credential via `{:?}`.
impl std::fmt::Debug for MqttClientConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MqttClientConfig")
            .field("slug", &self.slug)
            .field("host", &self.host)
            .field("port", &self.port)
            .field("username", &self.username)
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .field(
                "ca_cert_pem",
                &self
                    .ca_cert_pem
                    .as_ref()
                    .map(|b| format!("{} bytes", b.len())),
            )
            .field("tls_version_min", &self.tls_version_min)
            .field("keepalive_secs", &self.keepalive_secs)
            .field("inbound_payload_cap_bytes", &self.inbound_payload_cap_bytes)
            .field("last_will", &self.last_will)
            .field(
                "reconnect_backoff_initial_secs",
                &self.reconnect_backoff_initial_secs,
            )
            .field(
                "reconnect_backoff_max_secs",
                &self.reconnect_backoff_max_secs,
            )
            .field("qos", &self.qos)
            .field("urgency", &self.urgency)
            .field("session_expiry_secs", &self.session_expiry_secs)
            .finish()
    }
}

/// Resolved per-app MQTT ingress subscription.
///
/// The channel identity is the full resolved address `mqtt:<client>:<topic>`;
/// the subscriber-side generic params are resolved sub → channel → global. The
/// parsed `client_slug`/`topic` are retained for the router table and ingress
/// union-set derivation.
#[derive(Debug, Clone)]
pub struct ResolvedMqttIngressSubscription {
    /// Canonical channel address `mqtt:<client>:<topic>` (the channel identity).
    pub channel_address: String,
    /// Channel UUID = `mqtt_channel_uuid_from_address(channel_address)`.
    pub channel_uuid: Uuid,
    /// Declared client (the address's `<client>` segment; the ACL boundary).
    pub client_slug: String,
    /// MQTT topic filter (the subscribed pattern, not an actual published topic).
    pub topic: String,
    /// Resolved push depth (sub → channel → global).
    pub push_depth: Depth,
    /// Resolved retain depth (sub → channel → global).
    pub retain_depth: Depth,
    /// Resolved noise level (sub → channel → global).
    pub noise: NoiseLevel,
    /// Resolved wake-min policy (sub → channel → global).
    pub wake_min: WakeMin,
}

/// Resolved distinct MQTT ingress channel, deduplicated by `channel_uuid` across
/// all apps' subscriptions. Drives `mqtt:` channel-entry derivation, the ingress
/// union-subscription set, and the router routing table at bootstrap.
#[derive(Debug, Clone)]
pub struct ResolvedMqttIngressChannel {
    /// Canonical channel address `mqtt:<client>:<topic>` (the channel identity).
    pub channel_address: String,
    /// Channel UUID = `mqtt_channel_uuid_from_address(channel_address)`.
    pub channel_uuid: Uuid,
    /// Declared client (the ACL/provenance boundary; resolves to `[[mqtt_client]]`).
    pub client_slug: String,
    /// MQTT topic filter (the subscribed pattern).
    pub topic: String,
    /// Broker SUBSCRIBE QoS (from the client's `[[mqtt_client]].qos`).
    pub qos: u8,
    /// Sender-side injection urgency (from the client's `[[mqtt_client]].urgency`).
    pub urgency: Urgency,
}

// ---------------------------------------------------------------------------
// Validation / resolution
// ---------------------------------------------------------------------------

/// Validate the client slug charset.
///
/// Shared with `mqtt::address` — kept here as the authoritative definition.
pub(crate) fn is_valid_client_slug(slug: &str) -> bool {
    !slug.is_empty()
        && slug
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'_' || b == b'~' || b == b'-')
}

/// Default broker port when the URL omits one: the IANA-registered `mqtts`
/// (MQTT-over-TLS) port. Brenn is `mqtts://`-only.
const DEFAULT_MQTTS_PORT: u16 = 8883;

/// Parse a broker `host`/`port` out of a `mqtts://` URL.
///
/// Trims the `mqtts://` prefix itself (the scheme is asserted separately in
/// `resolve_clients`, and dropping it here keeps the function self-contained for
/// unit tests); `slug` names the client in panic messages. This runs at config
/// resolution (startup), where fail-fast is the contract, so it panics on any
/// malformation. IPv6 literals must be bracketed and the stored host is the bare
/// address (brackets stripped) — the only form both the rumqttc TCP resolver and
/// the rustls `ServerName` TLS path accept. Port defaults to 8883 when absent;
/// port `0` is rejected in every form.
fn parse_broker_host_port(slug: &str, url: &str) -> (String, u16) {
    // Strip at most one scheme prefix; a doubled `mqtts://mqtts://…` leaves the
    // second prefix in place so the path/userinfo reject below trips on its `//`.
    let rest = url.strip_prefix("mqtts://").unwrap_or(url);
    assert!(
        !rest.is_empty(),
        "config: [[mqtt_client]] {slug:?} url {url:?} has an empty broker address",
    );
    if rest.contains('/') || rest.contains('@') {
        // Redact any userinfo so a mistakenly-embedded credential never reaches
        // logs. `@` is the whole reason this branch rejects: credentials go
        // through username/password_file, not the URL.
        let shown = match url.rsplit_once('@') {
            Some((_, after)) => format!("mqtts://<redacted>@{after:?}"),
            None => format!("{url:?}"),
        };
        panic!(
            "config: [[mqtt_client]] {slug:?} url {shown} must be host[:port] only — paths and \
             userinfo are not part of the broker address (credentials go through \
             username/password_file)",
        );
    }

    if let Some(after_open) = rest.strip_prefix('[') {
        // Bracketed IPv6 literal.
        let close = after_open.find(']').unwrap_or_else(|| {
            panic!(
                "config: [[mqtt_client]] {slug:?} url {url:?} opens an IPv6 bracket but has no \
                 closing ]",
            )
        });
        let addr = &after_open[..close];
        addr.parse::<std::net::Ipv6Addr>().unwrap_or_else(|e| {
            panic!(
                "config: [[mqtt_client]] {slug:?} url {url:?} has an invalid bracketed IPv6 \
                 literal {addr:?}: {e}",
            )
        });
        let tail = &after_open[close + 1..];
        let port = if tail.is_empty() {
            DEFAULT_MQTTS_PORT
        } else if let Some(port_str) = tail.strip_prefix(':') {
            parse_broker_port(slug, url, port_str)
        } else {
            panic!(
                "config: [[mqtt_client]] {slug:?} url {url:?} has unexpected text {tail:?} after \
                 the IPv6 bracket; expected end of string or :<port>",
            )
        };
        return (addr.to_string(), port);
    }

    match rest.rsplit_once(':') {
        Some((host, port_str)) => {
            assert!(
                !host.contains(':'),
                "config: [[mqtt_client]] {slug:?} url {url:?}: IPv6 literals must be bracketed, \
                 e.g. mqtts://[::1]:8883",
            );
            assert!(
                !host.is_empty(),
                "config: [[mqtt_client]] {slug:?} url {url:?} has an empty host",
            );
            (host.to_string(), parse_broker_port(slug, url, port_str))
        }
        // No colon: the whole (non-empty) remainder is the host; default port.
        None => (rest.to_string(), DEFAULT_MQTTS_PORT),
    }
}

/// Parse a broker port string as a connectable (nonzero) `u16`, panicking on
/// anything else. Shared by the bracketed and unbracketed paths above.
fn parse_broker_port(slug: &str, url: &str, port_str: &str) -> u16 {
    let port: u16 = port_str.parse().unwrap_or_else(|_| {
        panic!(
            "config: [[mqtt_client]] {slug:?} url {url:?} has an invalid port {port_str:?}; \
             expected a number 1-65535",
        )
    });
    assert!(
        port != 0,
        "config: [[mqtt_client]] {slug:?} url {url:?} has port 0, which is not connectable",
    );
    port
}

/// Resolve `[[mqtt_client]]` raw entries into a validated, indexed map.
///
/// # Panics
///
/// Panics on:
/// - URL not starting with `mqtts://`.
/// - A malformed broker address: empty host, path/userinfo, unbracketed IPv6,
///   invalid or non-numeric port, or port `0` (see [`parse_broker_host_port`]).
/// - `inbound_payload_cap_bytes` of `0` or greater than `u32::MAX`.
/// - Invalid client slug charset.
/// - Duplicate client slugs.
/// - `password_file` missing or empty when present.
/// - `ca_file` missing or unreadable when present.
/// - Invalid `tls_version_min` value (must be `"1.2"` or `"1.3"`).
/// - QoS on Last-Will out of range (must be 0, 1, or 2).
pub fn resolve_clients(raw_clients: &[MqttClientConfigRaw]) -> IndexMap<String, MqttClientConfig> {
    let mut result = IndexMap::new();

    for raw in raw_clients {
        // Slug charset.
        assert!(
            is_valid_client_slug(&raw.slug),
            "config: [[mqtt_client]] slug {:?} is invalid; must match [A-Za-z0-9._~-]+",
            raw.slug,
        );

        // Duplicate slug.
        assert!(
            !result.contains_key(&raw.slug),
            "config: duplicate [[mqtt_client]] slug {:?}",
            raw.slug,
        );

        // TLS-only URL.
        assert!(
            raw.url.starts_with("mqtts://"),
            "config: [[mqtt_client]] {:?} url must use the mqtts:// scheme (plaintext MQTT is rejected). Got: {:?}",
            raw.slug,
            raw.url,
        );

        // Parse host + port once, here at startup; the resolved struct carries the
        // structured fields so no connect-time re-parse exists.
        let (host, port) = parse_broker_host_port(&raw.slug, &raw.url);

        // Payload cap: reject 0 (breaks the connection opaquely) and values that
        // would truncate on the `u32` the CONNECT packet carries.
        assert!(
            raw.inbound_payload_cap_bytes != 0,
            "config: [[mqtt_client]] {:?} inbound_payload_cap_bytes must be greater than 0",
            raw.slug,
        );
        let inbound_payload_cap_bytes: u32 = raw
            .inbound_payload_cap_bytes
            .try_into()
            .unwrap_or_else(|_| {
                panic!(
                    "config: [[mqtt_client]] {:?} inbound_payload_cap_bytes {} exceeds the u32 \
                     limit of {}",
                    raw.slug,
                    raw.inbound_payload_cap_bytes,
                    u32::MAX,
                )
            });

        // TLS version.
        let tls_version_min = match raw.tls_version_min.as_str() {
            "1.2" => TlsVersionMin::Tls12,
            "1.3" => TlsVersionMin::Tls13,
            other => panic!(
                "config: [[mqtt_client]] {:?} tls_version_min must be \"1.2\" or \"1.3\", got {:?}",
                raw.slug, other,
            ),
        };

        // Password file.
        let password = raw.password_file.as_ref().map(|path| {
            load_secret_file(
                &format!("[[mqtt_client]] {:?} password_file", raw.slug),
                path,
            )
        });

        // CA file.
        let ca_cert_pem = match &raw.ca_file {
            None => None,
            Some(path) => {
                let bytes = std::fs::read(path).unwrap_or_else(|e| {
                    panic!(
                        "config: [[mqtt_client]] {:?} ca_file at {} is unreadable: {e}",
                        raw.slug,
                        path.display(),
                    )
                });
                if bytes.is_empty() {
                    panic!(
                        "config: [[mqtt_client]] {:?} ca_file at {} is empty",
                        raw.slug,
                        path.display(),
                    );
                }
                Some(bytes)
            }
        };

        // Last-Will QoS.
        if let Some(ref lw) = raw.last_will {
            assert!(
                lw.qos <= 2,
                "config: [[mqtt_client]] {:?} last_will.qos must be 0, 1, or 2; got {}",
                raw.slug,
                lw.qos,
            );
        }

        // Ingress SUBSCRIBE QoS range.
        assert!(
            raw.qos <= 2,
            "config: [[mqtt_client]] {:?} qos must be 0, 1, or 2; got {}",
            raw.slug,
            raw.qos,
        );

        result.insert(
            raw.slug.clone(),
            MqttClientConfig {
                slug: raw.slug.clone(),
                host,
                port,
                username: raw.username.clone(),
                password,
                ca_cert_pem,
                tls_version_min,
                keepalive_secs: raw.keepalive_secs,
                inbound_payload_cap_bytes,
                last_will: raw.last_will.clone(),
                reconnect_backoff_initial_secs: raw.reconnect_backoff_initial_secs,
                reconnect_backoff_max_secs: raw.reconnect_backoff_max_secs,
                qos: raw.qos,
                urgency: raw.urgency,
                session_expiry_secs: raw.session_expiry_secs,
            },
        );
    }

    result
}

/// Resolve per-app `[[app.mqtt_subscription]]` (ingress) entries into validated,
/// fully-resolved ingress subscriptions.
///
/// `app` is the raw app config; `clients` is the global resolved client
/// map; `global_messaging` supplies the generic-param fallbacks. For each raw
/// subscription this:
///
/// - parses the `channel` address `mqtt:<client>:<topic>` (client mandatory),
/// - validates the parsed client against the client map,
/// - validates the topic filter,
/// - resolves the generic subscriber params (`push_depth`/`retain_depth`/
///   `noise`/`wake_min`) via sub → channel → global. The `mqtt:` channel is
///   synthesized from global defaults (no operator-authored `[[channel]]` block),
///   so the channel rung *is* the global default and the effective ladder is
///   sub → global — identical resolution to `brenn:`/`webhook:`, no per-channel
///   override possible.
/// - enforces the shared push-enabled constraint: a push-enabled subscription
///   (`push_depth > 0`, including the push-enabled global default) requires
///   `singleton = true` with exactly one `allowed_users` entry, exactly as
///   `brenn:` does. A pull-only (`push_depth = 0`) subscription may be
///   multi-user / non-singleton.
///
/// `qos`/`urgency` are NOT per-subscription — they are connection-level on
/// `[[mqtt_client]]` and applied later from the client config.
///
/// # Panics
///
/// Panics on:
/// - A `channel` not prefixed `mqtt:`, or missing the client segment (no `:`
///   after the prefix — the client is mandatory).
/// - A parsed client not present in `clients`.
/// - An invalid topic filter.
/// - `noise`/`wake_min` set on a pull-only (`push_depth = 0`) subscription.
/// - A push-enabled subscription on a non-singleton or multi-user app.
pub fn resolve_app_mqtt_subscriptions(
    app: &AppConfigRaw,
    clients: &IndexMap<String, MqttClientConfig>,
    global_messaging: &crate::messaging::config::MessagingGlobalConfig,
) -> Vec<ResolvedMqttIngressSubscription> {
    let mut result = Vec::new();
    // Reject two `[[app.mqtt_subscription]]` blocks in one app that resolve to
    // the same channel (`mqtt:<client>:<topic>` → same `channel_uuid`). Without
    // this guard the duplicate survives into `app.mqtt_subscriptions`, the
    // subscriber appears twice in the channel's subscriber set, and each inbound
    // MQTT message is enqueued twice to the same conversation. Every other
    // subscription resolver fails fast on an in-app duplicate (`resolve_app_messaging`
    // `seen_addresses`, `resolve_wasm_consumers` `seen_addresses`); mirror that.
    let mut seen_channels: HashSet<String> = HashSet::new();

    for raw_sub in &app.mqtt_subscriptions {
        // Address→channel resolution (parse, mandatory-client check, declared-client
        // cross-check, topic-filter validation, canonical address + UUID) via the
        // shared ingress-channel resolver, so this path and every other MQTT-ingress
        // source validate identically and share one set of panic messages.
        let owner_desc = format!("app {:?}: [[app.mqtt_subscription]]", app.slug);
        let channel = resolve_mqtt_ingress_channel(&raw_sub.channel, clients, &owner_desc);

        assert!(
            seen_channels.insert(channel.channel_address.clone()),
            "app {:?}: duplicate [[app.mqtt_subscription]] for channel {:?} \
             (resolves to {:?}) — one subscription per channel per app",
            app.slug,
            raw_sub.channel,
            channel.channel_address,
        );

        // Generic subscriber params + push-enabled invariants via the shared
        // resolver. The `mqtt:` channel is synthesized from global defaults (no
        // operator-authored `[[channel]]` block), so the rung is the global default
        // and the ladder collapses to sub → global — identical resolution to
        // `brenn:`/`webhook:`. At boot a violation is an operator-config error, so
        // `.expect()` preserves today's fail-fast `panic!`.
        let raw_params = crate::messaging::config::RawSubscriptionParams {
            channel_uuid: channel.channel_uuid,
            channel_address: channel.channel_address.clone(),
            push_depth: raw_sub.push_depth,
            retain_depth: raw_sub.retain_depth,
            noise: raw_sub.noise,
            wake_min: raw_sub.wake_min,
        };
        let rung =
            crate::messaging::config::SubscriptionParamDefaults::from_global(global_messaging);
        let resolved = crate::messaging::config::resolve_subscription_params(
            &raw_params,
            &rung,
            app.singleton,
            app.allowed_users.len(),
        )
        .unwrap_or_else(|e| panic!("app {:?}: [[app.mqtt_subscription]]: {e}", app.slug));

        result.push(ResolvedMqttIngressSubscription {
            channel_address: channel.channel_address,
            channel_uuid: channel.channel_uuid,
            client_slug: channel.client_slug,
            topic: channel.topic,
            push_depth: resolved.push_depth,
            retain_depth: resolved.retain_depth,
            noise: resolved.noise,
            wake_min: resolved.wake_min,
        });
    }

    result
}

/// Build the canonical `mqtt:<client>:<topic>` address via the shared formatter.
///
/// Both the subscribe side (here) and the publish side (router) must derive the
/// channel address identically so the two-caller UUID contract holds. Always
/// route through `MqttAddress::format` rather than re-concatenating ad hoc.
pub fn parsed_address_canonical(client_slug: &str, topic: &str) -> String {
    crate::mqtt::address::MqttAddress {
        client: client_slug.to_string(),
        topic: topic.to_string(),
    }
    .format()
}

/// Resolve a single `mqtt:<client>:<topic>` channel address into a distinct
/// `ResolvedMqttIngressChannel` (channel identity + broker-SUBSCRIBE params),
/// with fail-fast validation of the client and topic filter. `owner_desc` names
/// the declaring block in panic messages (e.g. `[[wasm_consumer]] "foo"`).
///
/// This is the address→channel half of ingress resolution, shared by any config
/// source that contributes MQTT ingress channels. The app path additionally
/// resolves per-subscription params (`resolve_app_mqtt_subscriptions`); callers
/// that need only the channel identity a broker SUBSCRIBE and router route
/// require (e.g. WASM consumers, whose sub params resolve later against the
/// derived channel entry) use this directly.
pub fn resolve_mqtt_ingress_channel(
    channel: &str,
    clients: &IndexMap<String, MqttClientConfig>,
    owner_desc: &str,
) -> ResolvedMqttIngressChannel {
    let parsed = parse_mqtt_address(channel).unwrap_or_else(|e| {
        panic!(
            "{owner_desc}: mqtt subscription channel {channel:?} is not a valid \
             mqtt:<client>:<topic> address (client is mandatory): {e}",
        )
    });
    let client_slug = parsed.client;
    let topic = parsed.topic;

    // The parsed client must be a declared `[[mqtt_client]]` (the ACL/session
    // boundary); qos/urgency are read from it for the broker SUBSCRIBE.
    let client = clients.get(&client_slug).unwrap_or_else(|| {
        panic!(
            "{owner_desc}: mqtt subscription channel {channel:?} references client \
             {client_slug:?} which is not declared in any [[mqtt_client]] block",
        )
    });

    validate_topic_filter_str(&topic).unwrap_or_else(|detail| {
        panic!(
            "{owner_desc}: mqtt subscription channel {channel:?} topic {topic:?} is invalid: {detail}",
        )
    });

    let channel_address = parsed_address_canonical(&client_slug, &topic);
    let channel_uuid = mqtt_channel_uuid_from_address(&channel_address);
    ResolvedMqttIngressChannel {
        channel_address,
        channel_uuid,
        client_slug,
        topic,
        qos: client.qos,
        urgency: client.urgency,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raw_broker(slug: &str, url: &str) -> MqttClientConfigRaw {
        MqttClientConfigRaw {
            slug: slug.to_string(),
            url: url.to_string(),
            username: None,
            password_file: None,
            ca_file: None,
            tls_version_min: "1.2".to_string(),
            keepalive_secs: None,
            inbound_payload_cap_bytes: default_inbound_payload_cap(),
            last_will: None,
            reconnect_backoff_initial_secs: 1,
            reconnect_backoff_max_secs: 60,
            qos: default_subscription_qos(),
            urgency: Urgency::Normal,
            session_expiry_secs: 0,
        }
    }

    fn minimal_app_raw(slug: &str, singleton: bool, allowed_users: Vec<String>) -> AppConfigRaw {
        AppConfigRaw {
            slug: slug.to_string(),
            singleton,
            allowed_users,
            ..Default::default()
        }
    }

    // --- resolve_clients ---

    #[test]
    fn valid_broker_resolves() {
        let brokers = resolve_clients(&[raw_broker("ha", "mqtts://broker.example.com:8883")]);
        assert!(brokers.contains_key("ha"));
        let b = &brokers["ha"];
        assert_eq!(b.slug, "ha");
        assert_eq!(b.host, "broker.example.com");
        assert_eq!(b.port, 8883);
        assert!(b.password.is_none());
    }

    #[test]
    #[should_panic(expected = "mqtts://")]
    fn plaintext_url_panics() {
        resolve_clients(&[raw_broker("ha", "mqtt://broker.example.com:1883")]);
    }

    #[test]
    #[should_panic(expected = "duplicate [[mqtt_client]] slug")]
    fn duplicate_client_slug_panics() {
        resolve_clients(&[
            raw_broker("ha", "mqtts://broker.example.com:8883"),
            raw_broker("ha", "mqtts://other.example.com:8883"),
        ]);
    }

    #[test]
    #[should_panic(expected = "tls_version_min")]
    fn invalid_tls_version_panics() {
        let mut broker = raw_broker("ha", "mqtts://broker.example.com:8883");
        broker.tls_version_min = "1.1".to_string();
        resolve_clients(&[broker]);
    }

    #[test]
    #[should_panic(expected = "slug")]
    fn invalid_client_slug_panics() {
        resolve_clients(&[raw_broker("bad slug!", "mqtts://broker.example.com:8883")]);
    }

    #[test]
    fn password_file_loaded_when_present() {
        let tmpdir = tempfile::tempdir().unwrap();
        let path = tmpdir.path().join("password");
        std::fs::write(&path, "supersecret\n").unwrap();
        let mut broker = raw_broker("ha", "mqtts://broker.example.com:8883");
        broker.password_file = Some(path);
        let brokers = resolve_clients(&[broker]);
        assert_eq!(brokers["ha"].password.as_deref(), Some("supersecret"));
    }

    #[test]
    #[should_panic(expected = "secret file")]
    fn missing_password_file_panics() {
        let mut broker = raw_broker("ha", "mqtts://broker.example.com:8883");
        broker.password_file = Some(PathBuf::from("/nonexistent/password"));
        resolve_clients(&[broker]);
    }

    #[test]
    #[should_panic(expected = "empty")]
    fn empty_password_file_panics() {
        let tmpdir = tempfile::tempdir().unwrap();
        let path = tmpdir.path().join("password");
        std::fs::write(&path, "   \n").unwrap();
        let mut broker = raw_broker("ha", "mqtts://broker.example.com:8883");
        broker.password_file = Some(path);
        resolve_clients(&[broker]);
    }

    // test-10: empty ca_file should panic at startup (mirrors empty_password_file_panics)
    #[test]
    #[should_panic(expected = "empty")]
    fn empty_ca_file_panics() {
        let tmpdir = tempfile::tempdir().unwrap();
        let path = tmpdir.path().join("ca.pem");
        std::fs::write(&path, "").unwrap();
        let mut broker = raw_broker("ha", "mqtts://broker.example.com:8883");
        broker.ca_file = Some(path);
        resolve_clients(&[broker]);
    }

    // errhandling-7: malformed port in URL should panic at startup
    #[test]
    #[should_panic(expected = "invalid port")]
    fn malformed_url_port_panics() {
        resolve_clients(&[raw_broker("ha", "mqtts://broker.example.com:notaport")]);
    }

    #[test]
    #[should_panic(expected = "last_will.qos")]
    fn invalid_last_will_qos_panics() {
        let mut broker = raw_broker("ha", "mqtts://broker.example.com:8883");
        broker.last_will = Some(LastWillRaw {
            topic: "status".to_string(),
            payload: "offline".to_string(),
            qos: 5,
            retain: true,
        });
        resolve_clients(&[broker]);
    }

    #[test]
    fn client_qos_urgency_defaults_applied_when_omitted() {
        // `raw_broker` mirrors a TOML block that omits qos/urgency; the
        // #[serde(default …)] attributes supply 1 / Normal, and resolution
        // carries them onto the resolved broker.
        let brokers = resolve_clients(&[raw_broker("ha", "mqtts://broker.example.com:8883")]);
        assert_eq!(brokers["ha"].qos, 1, "default ingress qos is 1");
        assert_eq!(
            brokers["ha"].urgency,
            Urgency::Normal,
            "default injection urgency is Normal"
        );
    }

    #[test]
    #[should_panic(expected = "qos must be 0, 1, or 2")]
    fn client_qos_out_of_range_panics() {
        let mut broker = raw_broker("ha", "mqtts://broker.example.com:8883");
        broker.qos = 3;
        resolve_clients(&[broker]);
    }

    #[test]
    fn client_session_expiry_default_and_explicit() {
        // Omitted ⇒ serde default 0 (ephemeral); `raw_broker` mirrors that.
        let brokers = resolve_clients(&[raw_broker("ha", "mqtts://broker.example.com:8883")]);
        assert_eq!(brokers["ha"].session_expiry_secs, 0);
        // An explicit value carries onto the resolved client.
        let mut broker = raw_broker("ha2", "mqtts://broker.example.com:8883");
        broker.session_expiry_secs = 3600;
        let brokers = resolve_clients(&[broker]);
        assert_eq!(brokers["ha2"].session_expiry_secs, 3600);
    }

    // --- parse_broker_host_port ---

    #[test]
    fn parse_host_with_explicit_port() {
        assert_eq!(
            parse_broker_host_port("ha", "host.example.com:8883"),
            ("host.example.com".to_string(), 8883),
        );
    }

    #[test]
    fn parse_host_defaults_port() {
        assert_eq!(
            parse_broker_host_port("ha", "host.example.com"),
            ("host.example.com".to_string(), 8883),
        );
    }

    #[test]
    fn parse_bracketed_ipv6_with_port_strips_brackets() {
        assert_eq!(
            parse_broker_host_port("ha", "[::1]:1234"),
            ("::1".to_string(), 1234),
        );
    }

    #[test]
    fn parse_bracketed_ipv6_no_port_defaults_and_strips() {
        // The real IPv6-no-port hazard the old rfind(':') split mangled.
        assert_eq!(
            parse_broker_host_port("ha", "[2001:db8::5]"),
            ("2001:db8::5".to_string(), 8883),
        );
    }

    #[test]
    fn parse_scheme_prefix_is_trimmed() {
        assert_eq!(
            parse_broker_host_port("ha", "mqtts://host.example.com:8883"),
            ("host.example.com".to_string(), 8883),
        );
    }

    #[test]
    #[should_panic(expected = "must be bracketed")]
    fn parse_unbracketed_ipv6_panics() {
        parse_broker_host_port("ha", "::1");
    }

    #[test]
    #[should_panic(expected = "paths and")]
    fn parse_doubled_scheme_panics() {
        // strip_prefix removes one scheme; the leftover `mqtts://` trips the
        // path/userinfo reject rather than being silently accepted as a host.
        parse_broker_host_port("ha", "mqtts://mqtts://host.example.com:8883");
    }

    #[test]
    #[should_panic(expected = "no closing ]")]
    fn parse_missing_close_bracket_panics() {
        parse_broker_host_port("ha", "[::1");
    }

    #[test]
    #[should_panic(expected = "empty host")]
    fn parse_empty_host_with_port_panics() {
        parse_broker_host_port("ha", ":8883");
    }

    #[test]
    #[should_panic(expected = "invalid port")]
    fn parse_non_numeric_port_panics() {
        parse_broker_host_port("ha", "host.example.com:notaport");
    }

    #[test]
    #[should_panic(expected = "invalid port")]
    fn parse_empty_port_panics() {
        parse_broker_host_port("ha", "host.example.com:");
    }

    #[test]
    #[should_panic(expected = "port 0")]
    fn parse_port_zero_panics() {
        parse_broker_host_port("ha", "host.example.com:0");
    }

    #[test]
    #[should_panic(expected = "empty broker address")]
    fn parse_empty_remainder_panics() {
        parse_broker_host_port("ha", "mqtts://");
    }

    #[test]
    #[should_panic(expected = "paths and")]
    fn parse_path_in_url_panics() {
        parse_broker_host_port("ha", "host.example.com:8883/path");
    }

    #[test]
    #[should_panic(expected = "userinfo")]
    fn parse_userinfo_in_url_panics() {
        parse_broker_host_port("ha", "user@host.example.com:8883");
    }

    #[test]
    #[should_panic(expected = "after the IPv6 bracket")]
    fn parse_junk_after_bracket_panics() {
        parse_broker_host_port("ha", "[::1]junk");
    }

    #[test]
    #[should_panic(expected = "invalid bracketed IPv6")]
    fn parse_non_ipv6_bracket_content_panics() {
        parse_broker_host_port("ha", "[nonsense]:8883");
    }

    #[test]
    #[should_panic(expected = "invalid bracketed IPv6")]
    fn parse_zone_id_in_brackets_panics() {
        // rustls ServerName::IpAddress cannot carry a zone; reject at startup.
        parse_broker_host_port("ha", "[fe80::1%eth0]:8883");
    }

    // --- payload cap validation ---

    #[test]
    #[should_panic(expected = "must be greater than 0")]
    fn zero_payload_cap_panics() {
        let mut broker = raw_broker("ha", "mqtts://broker.example.com:8883");
        broker.inbound_payload_cap_bytes = 0;
        resolve_clients(&[broker]);
    }

    #[test]
    fn payload_cap_at_u32_max_accepted() {
        let mut broker = raw_broker("ha", "mqtts://broker.example.com:8883");
        broker.inbound_payload_cap_bytes = u32::MAX as usize;
        let brokers = resolve_clients(&[broker]);
        assert_eq!(brokers["ha"].inbound_payload_cap_bytes, u32::MAX);
    }

    #[test]
    #[cfg(target_pointer_width = "64")]
    #[should_panic(expected = "exceeds the u32 limit")]
    fn payload_cap_over_u32_max_panics() {
        let mut broker = raw_broker("ha", "mqtts://broker.example.com:8883");
        broker.inbound_payload_cap_bytes = u32::MAX as usize + 1;
        resolve_clients(&[broker]);
    }

    fn clients_map() -> IndexMap<String, MqttClientConfig> {
        resolve_clients(&[raw_broker("ha", "mqtts://broker.example.com:8883")])
    }

    // --- resolve_app_mqtt_subscriptions (ingress, channel-address form) ---

    use crate::messaging::WakeMin;
    use crate::messaging::config::{Depth, MessagingGlobalConfig};

    fn ingress_sub(channel: &str) -> AppMqttIngressSubscriptionRaw {
        AppMqttIngressSubscriptionRaw {
            channel: channel.to_string(),
            push_depth: None,
            retain_depth: None,
            noise: None,
            wake_min: None,
        }
    }

    #[test]
    fn empty_mqtt_subscriptions_returns_empty() {
        let app = minimal_app_raw("myapp", false, vec![]);
        let result =
            resolve_app_mqtt_subscriptions(&app, &clients_map(), &MessagingGlobalConfig::default());
        assert!(result.is_empty());
    }

    #[test]
    fn mqtt_subscription_to_known_client_resolves() {
        let mut app = minimal_app_raw("myapp", true, vec!["alice".to_string()]);
        app.mqtt_subscriptions = vec![ingress_sub("mqtt:ha:home/+/state")];
        let result =
            resolve_app_mqtt_subscriptions(&app, &clients_map(), &MessagingGlobalConfig::default());
        assert_eq!(result.len(), 1);
        let sub = &result[0];
        assert_eq!(sub.channel_address, "mqtt:ha:home/+/state");
        assert_eq!(sub.client_slug, "ha");
        assert_eq!(sub.topic, "home/+/state");
        assert_eq!(
            sub.channel_uuid,
            mqtt_channel_uuid_from_address("mqtt:ha:home/+/state")
        );
        // Generic params absent ⇒ inherit global defaults.
        let g = MessagingGlobalConfig::default();
        assert_eq!(sub.push_depth, g.default_push_depth);
        assert_eq!(sub.retain_depth, g.default_retain_depth);
        assert_eq!(sub.noise, g.default_noise);
        assert_eq!(sub.wake_min, g.default_wake_min);
    }

    #[test]
    fn mqtt_subscription_generic_params_override_kept() {
        let mut app = minimal_app_raw("myapp", true, vec!["alice".to_string()]);
        let mut raw = ingress_sub("mqtt:ha:home/+/state");
        raw.wake_min = Some(WakeMin::High);
        raw.push_depth = Some(Depth::Bounded(8));
        raw.retain_depth = Some(Depth::Bounded(1));
        app.mqtt_subscriptions = vec![raw];
        let result =
            resolve_app_mqtt_subscriptions(&app, &clients_map(), &MessagingGlobalConfig::default());
        assert_eq!(result[0].wake_min, WakeMin::High);
        assert_eq!(result[0].push_depth, Depth::Bounded(8));
        assert_eq!(result[0].retain_depth, Depth::Bounded(1));
    }

    #[test]
    #[should_panic(expected = "duplicate [[app.mqtt_subscription]]")]
    fn mqtt_subscription_in_app_duplicate_channel_panics() {
        // Two subscriptions in one app resolving to the same channel must
        // fail-fast, or the subscriber appears twice in the channel's subscriber
        // set and each inbound message is delivered twice (correctness-1). Use
        // pull-only subs so the dedup check fires (not the singleton requirement).
        let mut app = minimal_app_raw("myapp", false, vec!["alice".to_string()]);
        let mut a = ingress_sub("mqtt:ha:home/+/state");
        a.push_depth = Some(Depth::Bounded(0));
        let mut b = ingress_sub("mqtt:ha:home/+/state");
        b.push_depth = Some(Depth::Bounded(0));
        app.mqtt_subscriptions = vec![a, b];
        resolve_app_mqtt_subscriptions(&app, &clients_map(), &MessagingGlobalConfig::default());
    }

    #[test]
    #[should_panic(expected = "not declared in any [[mqtt_client]] block")]
    fn mqtt_subscription_to_unknown_client_panics() {
        let mut app = minimal_app_raw("myapp", true, vec!["alice".to_string()]);
        app.mqtt_subscriptions = vec![ingress_sub("mqtt:nonexistent:home/+/state")];
        resolve_app_mqtt_subscriptions(&app, &clients_map(), &MessagingGlobalConfig::default());
    }

    #[test]
    #[should_panic(expected = "is not a valid")]
    fn mqtt_subscription_missing_client_segment_panics() {
        // `mqtt:home/x` — no `:` after the prefix, so the client segment is
        // missing. The client is mandatory; this is a parse error.
        let mut app = minimal_app_raw("myapp", true, vec!["alice".to_string()]);
        app.mqtt_subscriptions = vec![ingress_sub("mqtt:home/x")];
        resolve_app_mqtt_subscriptions(&app, &clients_map(), &MessagingGlobalConfig::default());
    }

    #[test]
    #[should_panic(expected = "is not a valid")]
    fn mqtt_subscription_not_mqtt_prefixed_panics() {
        let mut app = minimal_app_raw("myapp", true, vec!["alice".to_string()]);
        app.mqtt_subscriptions = vec![ingress_sub("brenn:ha:home/+/state")];
        resolve_app_mqtt_subscriptions(&app, &clients_map(), &MessagingGlobalConfig::default());
    }

    #[test]
    #[should_panic(expected = "is invalid")]
    fn mqtt_subscription_invalid_topic_filter_panics() {
        let mut app = minimal_app_raw("myapp", true, vec!["alice".to_string()]);
        app.mqtt_subscriptions = vec![ingress_sub("mqtt:ha:home/#/extra")]; // # not terminal
        resolve_app_mqtt_subscriptions(&app, &clients_map(), &MessagingGlobalConfig::default());
    }

    #[test]
    fn multi_user_app_may_subscribe_pull_only() {
        // A multi-user / non-singleton app may subscribe pull-only
        // (push_depth = 0): no push target is needed. Must NOT panic.
        let mut app = minimal_app_raw("myapp", false, vec!["alice".to_string(), "bob".to_string()]);
        let mut raw = ingress_sub("mqtt:ha:home/+/state");
        raw.push_depth = Some(Depth::Bounded(0));
        app.mqtt_subscriptions = vec![raw];
        let result =
            resolve_app_mqtt_subscriptions(&app, &clients_map(), &MessagingGlobalConfig::default());
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].client_slug, "ha");
        assert_eq!(result[0].push_depth, Depth::Bounded(0));
    }

    #[test]
    #[should_panic(expected = "requires `singleton = true`")]
    fn multi_user_app_push_enabled_subscription_panics() {
        // The default push_depth (Unbounded) is push-enabled, so a multi-user /
        // non-singleton app subscribing without overriding push_depth must panic
        // (parity with `brenn:`).
        let mut app = minimal_app_raw("myapp", false, vec!["alice".to_string(), "bob".to_string()]);
        app.mqtt_subscriptions = vec![ingress_sub("mqtt:ha:home/+/state")];
        resolve_app_mqtt_subscriptions(&app, &clients_map(), &MessagingGlobalConfig::default());
    }
}
