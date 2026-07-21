//! Operator-authored *raw* ACL config shapes for LLM apps (deserialize-only).
//!
//! These mirror the resolved matcher types in `acl.rs` but carry the operator's
//! un-validated strings straight from TOML. Validation (client charset, topic
//! filter syntax, channel/endpoint matcher rules) and conversion into the
//! resolved `acl::*` types happen in `build_app_policy` at resolution time — a
//! later increment (design §2.5.2/§2.5.3). Nothing here validates or converts.
//!
//! The LLM authoring surface nests these under a single `[app.acl.*]` sub-table
//! (`AppAclRaw`), in contrast to the WASM side's flat top-level ACL `Vec`s; both
//! resolve into the same `AppPolicy` (design §2.5.1 "Authoring-shape asymmetry").
//!
//! Backend-only, like the rest of `access` — no `ts-rs` derive.

use serde::Deserialize;

/// Raw `[app.acl]` sub-table for an LLM app. Absent in TOML ⇒ all lists empty
/// (every field `#[serde(default)]`). `deny_unknown_fields` so a misspelled ACL
/// key fails to parse rather than being silently ignored (design §2.5.1).
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppAclRaw {
    /// `[[app.acl.mqtt_subscribe]]` entries: `(client, topic_filter)` pairs.
    #[serde(default)]
    pub mqtt_subscribe: Vec<MqttSubMatcherRaw>,
    /// `[[app.acl.mqtt_publish]]` entries: client-slug only.
    #[serde(default)]
    pub mqtt_publish: Vec<MqttClientMatcherRaw>,
    /// `[[app.acl.brenn_subscribe]]` entries: channel matchers.
    #[serde(default)]
    pub brenn_subscribe: Vec<ChannelMatcherRaw>,
    /// `[[app.acl.brenn_publish]]` entries: channel matchers.
    #[serde(default)]
    pub brenn_publish: Vec<ChannelMatcherRaw>,
    /// `[[app.acl.ephemeral_publish]]` entries: ephemeral channel matchers.
    ///
    /// Matcher values are **bare channel names, no scheme** (`protobar-demo`,
    /// not `ephemeral:protobar-demo`) — same convention as `brenn_publish`, since
    /// `allows_channel_access`/`allows_ephemeral_publish` strip the scheme before
    /// matching and the ACL list name carries the class. There is
    /// **no `ephemeral_subscribe` field**: the `ephemeral_subscribe` grant token
    /// has no LLM-app enforcement point in v1 and boot-panics in `build_app_policy`;
    /// the serde field and the grant token stay in lockstep so
    /// neither can exist without the other.
    #[serde(default)]
    pub ephemeral_publish: Vec<ChannelMatcherRaw>,
    /// `[[app.acl.webhook]]` entries: endpoint slugs.
    #[serde(default)]
    pub webhook: Vec<WebhookMatcherRaw>,
}

/// Raw MQTT subscribe matcher: `{ client = "...", topic_filter = "..." }`.
/// Strings are validated and converted to `acl::MqttSubMatcher` at resolution.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MqttSubMatcherRaw {
    /// MQTT client slug (validated at resolution time).
    pub client: String,
    /// MQTT topic filter (validated at resolution time).
    pub topic_filter: String,
}

/// Raw MQTT publish matcher: `{ client = "..." }`. Publish is client-scoped only
/// (design §2.1); there is no topic dimension.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MqttClientMatcherRaw {
    /// MQTT client slug (validated at resolution time).
    pub client: String,
}

/// Raw `brenn:` channel matcher. Carries an explicit kind, exactly one of
/// `{ exact = "..." }` xor `{ prefix = "..." }` (design §2.5.1: start narrow per
/// high-level failure mode 4). This is an **externally-tagged** enum (serde's
/// default — no explicit tag attribute), so each variant is a single-key map:
/// `{ exact = "..." }` or `{ prefix = "..." }`. A TOML entry with neither key
/// (empty table) is rejected because no variant tag is present; one with both
/// keys is rejected because an external-tag variant value is a single newtype
/// string, not a two-key table (`deny_unknown_fields` reinforces this).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub enum ChannelMatcherRaw {
    /// `{ exact = "channel" }` — matches the channel exactly.
    Exact(String),
    /// `{ prefix = "channel-prefix" }` — matches channels with this prefix.
    Prefix(String),
}

/// Raw inbound webhook matcher: `{ endpoint = "..." }`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WebhookMatcherRaw {
    /// Webhook endpoint slug (validated at resolution time).
    pub endpoint: String,
}

/// Borrowed view of a WASM consumer's five ACL lists, passed as one argument to
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
    /// `brenn:` publish matchers.
    pub publish: &'a [ChannelMatcherRaw],
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

    /// Build an `AppAclRaw` from a TOML fragment by wrapping it in a throwaway
    /// table (TOML has no top-level standalone table literal). Mirrors how the
    /// real `[app.acl]` sub-table parses inside `[[app]]`.
    fn parse_acl(toml_src: &str) -> AppAclRaw {
        #[derive(Deserialize)]
        struct Wrap {
            acl: AppAclRaw,
        }
        let wrap: Wrap = toml::from_str(toml_src).expect("acl fragment parses");
        wrap.acl
    }

    #[test]
    fn full_acl_round_trips_each_matcher_kind() {
        let acl = parse_acl(
            r#"
            [[acl.mqtt_subscribe]]
            client = "home"
            topic_filter = "sensors/+/temp"

            [[acl.mqtt_publish]]
            client = "office"

            [[acl.brenn_subscribe]]
            prefix = "alerts"

            [[acl.brenn_subscribe]]
            exact = "status.ready"

            [[acl.brenn_publish]]
            exact = "outbox"

            [[acl.brenn_publish]]
            prefix = "outbox."

            [[acl.ephemeral_publish]]
            exact = "protobar-demo"

            [[acl.webhook]]
            endpoint = "github"
            "#,
        );

        assert_eq!(acl.mqtt_subscribe.len(), 1);
        assert_eq!(acl.mqtt_subscribe[0].client, "home");
        assert_eq!(acl.mqtt_subscribe[0].topic_filter, "sensors/+/temp");

        assert_eq!(acl.mqtt_publish.len(), 1);
        assert_eq!(acl.mqtt_publish[0].client, "office");

        assert_eq!(acl.brenn_subscribe.len(), 2);
        assert!(matches!(
            &acl.brenn_subscribe[0],
            ChannelMatcherRaw::Prefix(p) if p == "alerts"
        ));
        assert!(matches!(
            &acl.brenn_subscribe[1],
            ChannelMatcherRaw::Exact(e) if e == "status.ready"
        ));

        assert_eq!(acl.brenn_publish.len(), 2);
        assert!(matches!(
            &acl.brenn_publish[0],
            ChannelMatcherRaw::Exact(e) if e == "outbox"
        ));
        assert!(matches!(
            &acl.brenn_publish[1],
            ChannelMatcherRaw::Prefix(p) if p == "outbox."
        ));

        assert_eq!(acl.ephemeral_publish.len(), 1);
        assert!(matches!(
            &acl.ephemeral_publish[0],
            ChannelMatcherRaw::Exact(e) if e == "protobar-demo"
        ));

        assert_eq!(acl.webhook.len(), 1);
        assert_eq!(acl.webhook[0].endpoint, "github");
    }

    #[test]
    fn absent_acl_block_yields_empty_lists() {
        // An `[app.acl]` with no sub-tables (or, here, an empty table) leaves
        // every matcher list empty — the deny-by-default starting point.
        let acl = parse_acl("acl = {}\n");
        assert!(acl.mqtt_subscribe.is_empty());
        assert!(acl.mqtt_publish.is_empty());
        assert!(acl.brenn_subscribe.is_empty());
        assert!(acl.brenn_publish.is_empty());
        assert!(acl.ephemeral_publish.is_empty());
        assert!(acl.webhook.is_empty());
    }

    #[test]
    fn channel_matcher_requires_exactly_one_kind() {
        // Exercised through the real `[app.acl.brenn_subscribe]` path.
        // Each kind, on its own, parses to the matching variant — the positive
        // cases that prove the rejections below are not vacuous (i.e. that a
        // single valid key really is accepted, not that *every* shape is
        // rejected).
        let exact = toml::from_str::<AppAclRaw>("[[brenn_subscribe]]\nexact = \"x\"\n")
            .expect("a single `exact` key parses");
        assert!(matches!(
            exact.brenn_subscribe.as_slice(),
            [ChannelMatcherRaw::Exact(e)] if e == "x"
        ));
        let prefix = toml::from_str::<AppAclRaw>("[[brenn_subscribe]]\nprefix = \"y\"\n")
            .expect("a single `prefix` key parses");
        assert!(matches!(
            prefix.brenn_subscribe.as_slice(),
            [ChannelMatcherRaw::Prefix(p)] if p == "y"
        ));
        // Neither key ⇒ reject (no enum variant tag present).
        let neither = toml::from_str::<AppAclRaw>("[[brenn_subscribe]]\n");
        assert!(neither.is_err(), "a matcher with no kind must not parse");
        // Both keys ⇒ reject (a variant value is a single newtype string, not a
        // two-key table).
        let both =
            toml::from_str::<AppAclRaw>("[[brenn_subscribe]]\nexact = \"a\"\nprefix = \"b\"\n");
        assert!(both.is_err(), "a matcher with two kinds must not parse");
    }

    #[test]
    fn unknown_acl_key_is_rejected() {
        // `deny_unknown_fields` on AppAclRaw: a misspelled sub-table fails fast
        // rather than being silently dropped (design §2.5.1).
        let bad = toml::from_str::<AppAclRaw>("[[mqtt_subscibe]]\nclient = \"home\"\n");
        assert!(bad.is_err(), "an unknown ACL key must not parse");
    }

    #[test]
    fn unknown_field_inside_matcher_is_rejected() {
        // `deny_unknown_fields` on the *leaf* matcher structs: a misspelled field
        // inside an otherwise-valid sub-table fails fast, not just a misspelled
        // sub-table name (design §2.5.1). The outer `AppAclRaw` check above only
        // guards sub-table *names*; this guards each leaf struct's own fields.
        let bad_mqtt_sub = toml::from_str::<AppAclRaw>(
            "[[mqtt_subscribe]]\nclient = \"home\"\ntopic_filer = \"sensors/#\"\n",
        );
        assert!(
            bad_mqtt_sub.is_err(),
            "a misspelled mqtt_subscribe field must not parse"
        );
        let bad_mqtt_pub =
            toml::from_str::<AppAclRaw>("[[mqtt_publish]]\nclient = \"home\"\nextra = \"x\"\n");
        assert!(
            bad_mqtt_pub.is_err(),
            "a misspelled mqtt_publish field must not parse"
        );
        let bad_webhook =
            toml::from_str::<AppAclRaw>("[[webhook]]\nendpoint = \"github\"\nextra = \"x\"\n");
        assert!(
            bad_webhook.is_err(),
            "a misspelled webhook field must not parse"
        );
    }

    #[test]
    fn matcher_missing_required_field_is_rejected() {
        // `MqttSubMatcherRaw` requires both `client` and `topic_filter` (neither
        // is `#[serde(default)]`). An entry missing either must fail to parse,
        // rather than silently defaulting to an empty string — an empty filter
        // would be a narrow silent over-grant at `filter_covers` time.
        let no_filter = toml::from_str::<AppAclRaw>("[[mqtt_subscribe]]\nclient = \"home\"\n");
        assert!(
            no_filter.is_err(),
            "mqtt_subscribe missing topic_filter must not parse"
        );
        let no_client =
            toml::from_str::<AppAclRaw>("[[mqtt_subscribe]]\ntopic_filter = \"sensors/#\"\n");
        assert!(
            no_client.is_err(),
            "mqtt_subscribe missing client must not parse"
        );
    }
}
