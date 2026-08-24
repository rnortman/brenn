//! Operator-authored *raw* ACL config shapes for LLM apps.
//!
//! These mirror the resolved matcher types in `acl.rs` but carry the operator's
//! un-validated strings. Validation (client charset, topic filter syntax,
//! channel/endpoint matcher rules) and conversion into the resolved `acl::*`
//! types happen in `build_app_policy` at resolution time. Nothing here
//! validates or converts.
//!
//! The LLM authoring surface nests these under a single `[app.acl.*]` sub-table
//! (`AppAclRaw`), in contrast to the WASM side's flat top-level ACL `Vec`s; both
//! resolve into the same `AppPolicy`.
//!
//! Backend-only, like the rest of `access` — no `ts-rs` derive.

// TODO(config-syntax-in-operator-messages): the field docs below name these
// shapes in table notation, which no document spells.

/// Raw `[app.acl]` sub-table for an LLM app. An agent that states no `acl`
/// leaves every list empty, which is deny-everything.
#[derive(Debug, Default, PartialEq)]
pub struct AppAclRaw {
    /// `[[app.acl.mqtt_subscribe]]` entries: `(client, topic_filter)` pairs.
    pub mqtt_subscribe: Vec<MqttSubMatcherRaw>,
    /// `[[app.acl.mqtt_publish]]` entries: client-slug only.
    pub mqtt_publish: Vec<MqttClientMatcherRaw>,
    /// `[[app.acl.brenn_subscribe]]` entries: channel matchers.
    pub brenn_subscribe: Vec<ChannelMatcherRaw>,
    /// `[[app.acl.brenn_publish]]` entries: channel matchers.
    pub brenn_publish: Vec<ChannelMatcherRaw>,
    /// `[[app.acl.ephemeral_publish]]` entries: ephemeral channel matchers.
    ///
    /// Matcher values are **bare channel names, no scheme** (`protobar-demo`,
    /// not `ephemeral:protobar-demo`) — same convention as `brenn_publish`, since
    /// `allows_channel_access`/`allows_ephemeral_publish` strip the scheme before
    /// matching and the ACL list name carries the class.
    pub ephemeral_publish: Vec<ChannelMatcherRaw>,
    /// `[[app.acl.ephemeral_subscribe]]` entries: ephemeral channel matchers.
    ///
    /// Bare channel names, no scheme — same convention as `ephemeral_publish`.
    /// Scopes which `ephemeral:` channels this app may hold a subscription on and
    /// read; combined with the `ephemeral_subscribe` grant by
    /// `allows_ephemeral_delivery`. There is no `local_subscribe` field: that
    /// grant token has no LLM-app path and boot-panics in `build_app_policy`, and
    /// this field stays in lockstep with the token so neither can exist
    /// without the other.
    pub ephemeral_subscribe: Vec<ChannelMatcherRaw>,
    /// `[[app.acl.local_publish]]` entries: confined (`local:`) channel matchers.
    ///
    /// Bare channel names, no scheme — same convention as `ephemeral_publish`.
    /// Non-empty derives the `LocalPublish` grant in `build_app_policy`.
    pub local_publish: Vec<ChannelMatcherRaw>,
    /// `[[app.acl.webhook]]` entries: endpoint slugs.
    pub webhook: Vec<WebhookMatcherRaw>,
}

/// Raw MQTT subscribe matcher: `{ client = "...", topic_filter = "..." }`.
/// Strings are validated and converted to `acl::MqttSubMatcher` at resolution.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct MqttSubMatcherRaw {
    /// MQTT client slug (validated at resolution time).
    pub client: String,
    /// MQTT topic filter (validated at resolution time).
    pub topic_filter: String,
}

/// Raw MQTT publish matcher: `{ client = "..." }`. Publish is client-scoped
/// only; there is no topic dimension.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct MqttClientMatcherRaw {
    /// MQTT client slug (validated at resolution time).
    pub client: String,
}

/// Raw `brenn:` channel matcher. Carries an explicit kind: `exact` matches one
/// channel, `prefix` matches every channel a name starts. An `acl` entry names
/// exactly one of the two keywords, so neither "no kind" nor "both kinds" has a
/// spelling.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum ChannelMatcherRaw {
    /// `{ exact = "channel" }` — matches the channel exactly.
    Exact(String),
    /// `{ prefix = "channel-prefix" }` — matches channels with this prefix.
    Prefix(String),
}

/// Raw inbound webhook matcher: `{ endpoint = "..." }`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct WebhookMatcherRaw {
    /// Webhook endpoint slug (validated at resolution time).
    pub endpoint: String,
}

/// Borrowed view of a WASM consumer's ACL lists, passed as one argument to
/// [`build_wasm_policy`](crate::access::resolve::build_wasm_policy).
///
/// Named fields prevent transposing the two same-typed slices (`subscribe` and
/// `publish` are both `&[ChannelMatcherRaw]`, so a positional swap would silently
/// exchange subscribe/publish authorization). [`Default`] yields all-empty lists
/// for the common case; `..Default::default()` isolates the one or two lists a
/// caller actually populates.
#[derive(Debug, Clone, Copy, Default)]
pub struct WasmAclsRaw<'a> {
    /// `brenn:` subscribe matchers (non-empty derives the `MessagingSubscribe` grant).
    pub subscribe: &'a [ChannelMatcherRaw],
    /// `ephemeral:` subscribe matchers (non-empty derives the `EphemeralSubscribe` grant).
    pub ephemeral_subscribe: &'a [ChannelMatcherRaw],
    /// `brenn:` publish matchers.
    pub publish: &'a [ChannelMatcherRaw],
    /// `ephemeral:` publish matchers (non-empty derives the `EphemeralPublish` grant).
    pub ephemeral_publish: &'a [ChannelMatcherRaw],
    /// `local:` publish matchers (non-empty derives the `LocalPublish` grant).
    pub local_publish: &'a [ChannelMatcherRaw],
    /// `local:` subscribe matchers (non-empty derives the `LocalSubscribe` grant).
    pub local_subscribe: &'a [ChannelMatcherRaw],
    /// MQTT publish matchers (client-scoped).
    pub mqtt_publish: &'a [MqttClientMatcherRaw],
    /// MQTT subscribe matchers (non-empty derives the `MqttSubscribe` grant).
    pub mqtt_subscribe: &'a [MqttSubMatcherRaw],
    /// Inbound webhook matchers (non-empty derives the `Webhook` grant).
    pub webhook: &'a [WebhookMatcherRaw],
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::config_from_dsl;

    /// Agent `alice`'s ACL, from a document of `declarations` plus an
    /// `Assistant` class whose body is `body`.
    ///
    /// The preamble keeps the declarations an `acl` statement has to name out
    /// of each test body, so a matcher test states matchers and nothing else.
    fn alice_acl(declarations: &str, body: &str) -> AppAclRaw {
        let document = format!(
            "{declarations}\n\nagent Assistant() {{\n{body}\n}}\n\nnew alice: Assistant();\n"
        );
        config_from_dsl(&document)
            .apps
            .into_iter()
            .find(|app| app.slug == "alice")
            .expect("the instantiated agent")
            .acl
    }

    const MQTT_CLIENTS: &str = r#"
mqtt_client home { url = "mqtts://broker.example.com:8883"; }
mqtt_client office { url = "mqtts://office.example.com:8883"; }
"#;

    const WEBHOOK: &str = r#"
webhook push_alice {
    slug = "github";
    mount = "/webhooks/github";

    signature {
        scheme = bearer-token;
        header = "authorization";
    }

    token phone { secret_file = "/home/alice/.secrets/github.token"; }
}
"#;

    /// Patterns arrive scheme-stripped: an `acl` statement names
    /// `brenn:alerts.`, the matcher holds `alerts.`.
    #[test]
    fn brenn_channel_matchers_land_scheme_stripped() {
        let acl = alice_acl(
            "",
            r#"
    grants = [subscribe, publish];

    acl subscribe [prefix "brenn:alerts.", exact "brenn:status.ready"];
    acl publish [exact "brenn:outbox", prefix "brenn:outbox."];
"#,
        );

        assert_eq!(
            acl.brenn_subscribe,
            vec![
                ChannelMatcherRaw::Prefix("alerts.".to_string()),
                ChannelMatcherRaw::Exact("status.ready".to_string()),
            ]
        );
        assert_eq!(
            acl.brenn_publish,
            vec![
                ChannelMatcherRaw::Exact("outbox".to_string()),
                ChannelMatcherRaw::Prefix("outbox.".to_string()),
            ]
        );
    }

    /// The `ephemeral:` plane is its own matcher list, stripped the same way.
    #[test]
    fn ephemeral_matchers_land_on_their_own_plane() {
        let acl = alice_acl(
            "",
            r#"
    grants = [publish];

    acl publish [exact "ephemeral:protobar-demo"];
"#,
        );

        assert_eq!(
            acl.ephemeral_publish,
            vec![ChannelMatcherRaw::Exact("protobar-demo".to_string())]
        );
        assert!(acl.brenn_publish.is_empty());
    }

    /// An MQTT matcher is client-scoped: the client name leaves the pattern and
    /// becomes the matcher's own field.
    #[test]
    fn mqtt_matchers_land_client_scoped() {
        let acl = alice_acl(
            MQTT_CLIENTS,
            r#"
    grants = [subscribe, publish];

    acl subscribe [topic_filter "mqtt:home:sensors/+/temp"];
    acl publish [client "mqtt:office"];

    subscribe "mqtt:home:sensors/+/temp" { push_depth = 1; retain_depth = 1; }
"#,
        );

        assert_eq!(
            acl.mqtt_subscribe,
            vec![MqttSubMatcherRaw {
                client: "home".to_string(),
                topic_filter: "sensors/+/temp".to_string(),
            }]
        );
        assert_eq!(
            acl.mqtt_publish,
            vec![MqttClientMatcherRaw {
                client: "office".to_string(),
            }]
        );
    }

    /// A webhook matcher names the endpoint's slug, scheme stripped.
    #[test]
    fn webhook_matchers_name_the_endpoint() {
        let acl = alice_acl(
            WEBHOOK,
            r#"
    grants = [subscribe];

    acl subscribe [endpoint "webhook:github"];

    subscribe "webhook:github" { push_depth = 1; retain_depth = 1; }
"#,
        );

        assert_eq!(
            acl.webhook,
            vec![WebhookMatcherRaw {
                endpoint: "github".to_string(),
            }]
        );
    }

    /// Deny by default: an agent that states no `acl` gets empty lists on every
    /// plane, not a wildcard.
    #[test]
    fn an_agent_without_acls_is_empty_on_every_plane() {
        assert_eq!(alice_acl("", "    grants = [];"), AppAclRaw::default());
    }
}
