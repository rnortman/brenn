//! Stage 5 of the `.brenn` pipeline: a derived DSL document becomes a
//! [`BrennConfig`].
//!
//! Parse → deserialize → resolve → derive happen in `brenn-dsl`, which depends
//! on no brenn domain crate. This module is the other side of that boundary: it
//! reads the derived model
//! field by field and constructs the raw config structs directly, with no
//! intermediate value tree — that would only be a middleman between two typed
//! representations.
//!
//! Two properties make the transcription mechanical rather than a second
//! specification:
//!
//! - **Every DSL attr key is its raw struct's field name.** A lowering line is
//!   `field: <same-named attr>` through one of the conversions below.
//! - **Every raw struct is built with an exhaustive struct literal** — every
//!   field named, never `..Default::default()`. A field added to, removed from
//!   or renamed in a raw struct fails compilation here, which is what forces a
//!   decision instead of a silent default.
//!
//! Diagnostics accumulate: a document reports every bad value it holds, not the
//! first. A refused value leaves its field absent and records the refusal, and
//! the whole lowering fails at the end.

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::LazyLock;

use serde::de::DeserializeOwned;
use serde::de::value::StrDeserializer;

use brenn_dsl::Span;
use brenn_dsl::derived::{
    DAclSet, DAuthority, DMatcher, DMqttClient, DMqttSub, DRemoteAuthority, DRemoteSubEntry,
    DWebhook, DerivedConfig,
};
use brenn_dsl::diag::{Diagnostic, duplicate_statement, or_list};
use brenn_dsl::model::{
    AgentAttrs, Attr, ChannelAttrs, DocComment, InTail, IntOrWord, IoTail, McpServerAttrs,
    MountTail, MqttClientAttrs, OutTail, RemoteAttrs, RepoAttrs, SubscribeTail, SurfaceAttrs,
    WebhookAttrs, Word, section_key,
};
use brenn_dsl::resolved::{
    RAgent, RAttachmentTarget, RChanRef, RComponentInst, RConsumer, RHooks, RMcp, RMount, RNamed,
    RRemote, RSection, RSubscribe, RSurface, RTail, RToolGrant, RVal, RValue, RWebhook,
    RWebhookBlock, ResolvedConfig as DslResolved,
};

use crate::access::raw::{
    AppAclRaw, ChannelMatcherRaw, MqttClientMatcherRaw, MqttSubMatcherRaw, WebhookMatcherRaw,
};
use crate::messaging::config::{
    ChannelConfigRaw, Depth, LinkConfigRaw, LinkEndpointRaw, LinkHostRaw, MessagingConfigRaw,
    MessagingGlobalConfig, MessagingSubscriptionRaw, NoiseLevel, SendRate, SurfaceComponentRaw,
    SurfaceConfigRaw, SurfaceIoPortRaw, SurfaceOutputRaw, SurfaceSubscriptionRaw,
    WasmConsumerConfigRaw, WasmConsumerIoPortRaw, WasmConsumerMqttOutputRaw, WasmConsumerOutputRaw,
    WasmConsumerSubscriptionRaw,
};
use crate::messaging::remote::{RemoteConfigRaw, RemoteSubscribeAclRaw};
use crate::messaging::{AttachGrant, ComponentGrant, Urgency, WakeMin};
use crate::mqtt::config::{
    AppMqttIngressSubscriptionRaw, MqttClientConfigRaw, default_backoff_initial,
    default_backoff_max, default_client_urgency, default_inbound_payload_cap,
    default_subscription_qos, default_tls_version_min,
};
use crate::pwa_push::config::{
    PwaPushGlobalConfig, default_endpoint_host_allowlist, default_endpoint_host_allowlist_enforce,
};
use crate::tools::config::{RateLimitRaw, ToolGrantRaw};
use crate::webhook::config::{
    AppWebhookSubscriptionRaw, ReplayProtectionConfigRaw, WebhookEndpointConfigRaw,
    WebhookKeyConfigRaw, WebhookSignatureConfigRaw, WebhookTokenConfigRaw, default_content_type,
    default_hmac_algorithm, default_transport_ceiling,
};
use brenn_envelope::grants::AppCapability;

use super::alerting::{AlertingConfig, MailConfig, NtfyConfig, default_subject_label};
use super::app::AppConfigRaw;
use super::attachment::{AttachmentHandlerConfig, AttachmentTargetRaw, default_timeout_secs};
use super::automation::AutomationGlobalConfig;
use super::brenn::BrennConfig;
use super::claude_defaults::ClaudeDefaultsConfig;
use super::container::{ContainerConfig, default_container_home};
use super::events::EventsConfig;
use super::hooks::{PostPullHooksConfig, StartHooksConfig, StartupHooksConfig};
use super::llm_chat::LlmChatConfig;
use super::logging::{LevelFilter, LoggingConfig, deserialize_level_filter};
use super::mcp::McpServerConfig;
use super::observability::{
    ObservabilityConfig, UsageObservabilityConfig, default_surface_error_publish_floor,
};
use super::repo::{AccessLevel, MountConfigRaw, RepoDeclRaw, RepoSyncConfig, default_true};
use super::security::SecurityConfig;
use super::server::{DatabaseConfig, ServerConfig};
use super::surface_description::SurfaceDescriptionConfig;
use super::wasm::{WasmConfig, default_store_size_limit};
use super::watchdog::WatchdogConfig;

/// Construct a [`BrennConfig`] from a derived `.brenn` document.
///
/// A pure function of its input: the same derived model always lowers to the
/// same config, and nothing here reads the filesystem, the clock or the
/// environment.
///
/// Lowering re-checks nothing that `validate_and_resolve`, the messaging boot
/// or the access resolvers already refuse; a lowered config walks all three.
pub fn lower(derived: DerivedConfig) -> Result<BrennConfig, Vec<Diagnostic>> {
    let mut errors = Vec::new();
    let resolved = &derived.resolved;

    let mut channels = Vec::with_capacity(resolved.channels.len() + resolved.tunings.len());
    for (index, declared) in resolved.channels.iter().enumerate() {
        // Durable declarations carry a uuid and non-durable ones carry none;
        // derivation decided which, and the runtime requires exactly that.
        let uuid = derived.channel_uuids[index].map(|uuid| uuid.to_string());
        channels.push(channel(
            Some(declared.address.value().clone()),
            None,
            uuid,
            &declared.attrs,
            &mut errors,
        ));
    }
    for tuning in &resolved.tunings {
        // A whole address tunes one channel; a prefix tunes the family under
        // it. A tuning never carries a uuid — synthesis owns that identity.
        let address = tuning.address.value().clone();
        let (address, address_prefix) = if tuning.is_prefix {
            (None, Some(address))
        } else {
            (Some(address), None)
        };
        channels.push(channel(
            address,
            address_prefix,
            None,
            &tuning.attrs,
            &mut errors,
        ));
    }

    let sections = sections(&resolved.sections, &mut errors);
    let repos = repos(&resolved.repos, &mut errors);
    let mqtt_clients = mqtt_clients(&resolved.mqtt_clients, &mut errors);
    let apps = apps(&derived, &mut errors);
    let mut link_endpoints = LinkEndpoints::new();
    let wasm_consumers = consumers(&derived, &mut link_endpoints, &mut errors);
    let surfaces = surfaces(&derived, &mut link_endpoints, &mut errors);
    let remotes = remotes(&derived, &mut errors);
    let webhook_endpoints = webhook_endpoints(&resolved.webhooks, &mut errors);
    let links = links(resolved, link_endpoints);

    let config = BrennConfig {
        server: sections.server.unwrap_or_default(),
        database: sections.database.unwrap_or_default(),
        logging: sections.logging.unwrap_or_default(),
        security: sections.security.unwrap_or_default(),
        alerting: sections.alerting,
        claude_defaults: sections.claude_defaults.unwrap_or_default(),
        repo_sync: sections.repo_sync.unwrap_or_default(),
        repos,
        container: sections.container,
        integrations: sections.integrations,
        apps,
        channels,
        messaging: sections.messaging.unwrap_or_default(),
        observability: sections.observability.unwrap_or_default(),
        surface_description: sections.surface_description.unwrap_or_default(),
        llm_chat: sections.llm_chat.unwrap_or_default(),
        pwa_push: sections.pwa_push.unwrap_or_default(),
        automation: sections.automation.unwrap_or_default(),
        mqtt_clients,
        webhook_endpoints,
        events: sections.events.unwrap_or_default(),
        wasm_consumers,
        surfaces,
        remotes,
        links,
        wasm: sections.wasm.unwrap_or_default(),
        watchdog: sections.watchdog.unwrap_or_default(),
    };

    if errors.is_empty() {
        Ok(config)
    } else {
        Err(errors)
    }
}

// ---------------------------------------------------------------------------
// Channels and tunings
// ---------------------------------------------------------------------------

/// One `[[channel]]` entry, from a declaration or a tuning block.
///
/// Both spell the same body, so both read the same vocabulary; what differs is
/// which address field is set and whether a uuid rides along.
fn channel(
    address: Option<String>,
    address_prefix: Option<String>,
    uuid: Option<String>,
    attrs: &ChannelAttrs<RVal>,
    errors: &mut Vec<Diagnostic>,
) -> ChannelConfigRaw {
    // Destructured with no `..`, like every entity lowering below: an attr
    // added to the vocabulary fails compilation here instead of being read by
    // nobody and silently dropped.
    let ChannelAttrs {
        description,
        push_depth,
        retain_depth,
        standing_retain_depth,
        noise,
        sink,
        wake_min,
        send_rate: rate,
        // Bound and dropped: a doctype is the expectation the component ports
        // bound to this channel are checked against at compile time, and no
        // runtime field carries it.
        doctype: _,
    } = attrs;
    ChannelConfigRaw {
        uuid,
        address,
        address_prefix,
        description: opt_str(description.as_ref(), "description", errors),
        push_depth: opt_depth(push_depth.as_ref(), "push_depth", errors),
        retain_depth: opt_depth(retain_depth.as_ref(), "retain_depth", errors),
        standing_retain_depth: opt_depth(
            standing_retain_depth.as_ref(),
            "standing_retain_depth",
            errors,
        ),
        noise: opt_token(noise.as_ref(), "noise", errors),
        sink: opt_token(sink.as_ref(), "sink", errors),
        wake_min: opt_token(wake_min.as_ref(), "wake_min", errors),
        send_rate: rate
            .as_ref()
            .and_then(|attr| send_rate(&attr.value, errors)),
    }
}

/// Record a refusal and carry on.
///
/// A refused value leaves its field absent, so the walk reaches every other
/// value in the document and reports all of them at once. The config built
/// around the hole is discarded — `lower` returns the errors instead.
fn keep<T>(result: Result<T, Diagnostic>, errors: &mut Vec<Diagnostic>) -> Option<T> {
    match result {
        Ok(value) => Some(value),
        Err(diagnostic) => {
            errors.push(diagnostic);
            None
        }
    }
}

/// A value in the wrong shape, cited at its own token.
///
/// A matcher gets its own wording: it is a legal thing to write in a statement
/// tail, so reaching a value position with one is user error rather than a
/// broken tree.
fn mismatch(value: &RVal, key: &str, expected: &str) -> Diagnostic {
    let message = match value.value() {
        RValue::Matcher(_) => format!("`{key}`: a matcher is not a value here"),
        found => format!("`{key}`: expected {expected}, got {}", found.kind()),
    };
    Diagnostic::at(message, value.span().clone())
}

fn expect_str(value: &RVal, key: &str) -> Result<String, Diagnostic> {
    match value.value() {
        RValue::Str(text) => Ok(text.clone()),
        _ => Err(mismatch(value, key, "a string")),
    }
}

/// An integer value, narrowed to the target's own width.
///
/// The DSL carries every integer as `i64`, so every narrowing is a range check
/// the target type states. A value out of range is refused at the token that
/// wrote it rather than truncated.
fn expect_int<T>(value: &RVal, key: &str) -> Result<T, Diagnostic>
where
    T: TryFrom<i64>,
{
    match value.value() {
        RValue::Int(number) => T::try_from(*number).map_err(|_| {
            Diagnostic::at(
                format!("`{key}`: {number} is out of range for this key"),
                value.span().clone(),
            )
        }),
        _ => Err(mismatch(value, key, "an integer")),
    }
}

fn expect_bool(value: &RVal, key: &str) -> Result<bool, Diagnostic> {
    match value.value() {
        RValue::Bool(flag) => Ok(*flag),
        _ => Err(mismatch(value, key, "a boolean")),
    }
}

/// A float value.
///
/// An integer widens: a budget knob written `1` is the same number as `1.0`,
/// and the DSL has no float literal for a whole number.
fn expect_flt(value: &RVal, key: &str) -> Result<f64, Diagnostic> {
    match value.value() {
        RValue::Flt(number) => Ok(*number),
        #[expect(
            clippy::cast_precision_loss,
            reason = "a budget knob is a small count; the wide end of i64 is not a rate"
        )]
        RValue::Int(count) => Ok(*count as f64),
        _ => Err(mismatch(value, key, "a number")),
    }
}

/// A table value, as the entries it holds.
fn expect_table<'a>(value: &'a RVal, key: &str) -> Result<&'a [(String, RVal)], Diagnostic> {
    match value.value() {
        RValue::Table(entries) => Ok(entries),
        _ => Err(mismatch(value, key, "a table")),
    }
}

/// A `toml::Value`, for the two raw fields that literally store one.
///
/// Everywhere else the transcription is field to typed field; these two
/// positions are operator-supplied maps the config keeps as a value tree, so
/// this is where a value tree is the target rather than a middleman.
///
/// The recursion accepts every shape the tree can hold; that is not a claim
/// about what the runtime accepts, because both positions resolve
/// through `crate::config::wasm::resolve_component_config`, a flat
/// string/integer/boolean surface that panics on a float, an array or a nested
/// table. Refusing those here instead would move a boot check into lowering.
fn rval_to_toml(value: &RVal, key: &str) -> Result<toml::Value, Diagnostic> {
    Ok(match value.value() {
        RValue::Str(text) => toml::Value::String(text.clone()),
        RValue::Int(count) => toml::Value::Integer(*count),
        RValue::Flt(number) => toml::Value::Float(*number),
        RValue::Bool(flag) => toml::Value::Boolean(*flag),
        RValue::List(items) => toml::Value::Array(
            items
                .iter()
                .map(|item| rval_to_toml(item, key))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        RValue::Table(entries) => toml::Value::Table(toml_table(entries, key)?),
        RValue::Matcher(_) => return Err(mismatch(value, key, "a value")),
    })
}

/// The entries of a table, as a `toml::Table`.
fn toml_table(entries: &[(String, RVal)], key: &str) -> Result<toml::Table, Diagnostic> {
    let mut table = toml::Table::new();
    for (name, value) in entries {
        table.insert(name.clone(), rval_to_toml(value, key)?);
    }
    Ok(table)
}

fn expect_path(value: &RVal, key: &str) -> Result<PathBuf, Diagnostic> {
    expect_str(value, key).map(PathBuf::from)
}

/// A list of strings: `endpoint_host_allowlist = ["push.example.com"]`.
///
/// Every element that is not a string is refused at that element: a list with
/// two bad entries reports both, like every other position here.
fn expect_strings(value: &RVal, key: &str, errors: &mut Vec<Diagnostic>) -> Option<Vec<String>> {
    let elements = match value.value() {
        RValue::List(elements) => elements,
        _ => {
            errors.push(mismatch(value, key, "a list of strings"));
            return None;
        }
    };
    let before = errors.len();
    let strings: Vec<String> = elements
        .iter()
        .filter_map(|element| keep(expect_str(element, key), errors))
        .collect();
    (errors.len() == before).then_some(strings)
}

/// A map from a table attr, one entry reader deep.
fn expect_table_of<T, M: FromIterator<(String, T)>>(
    value: &RVal,
    key: &str,
    expected: &str,
    entry: impl Fn(&RVal, &str, &mut Vec<Diagnostic>) -> Option<T>,
    errors: &mut Vec<Diagnostic>,
) -> Option<M> {
    let entries = match value.value() {
        RValue::Table(entries) => entries,
        _ => {
            errors.push(mismatch(value, key, expected));
            return None;
        }
    };
    let before = errors.len();
    let table: M = entries
        .iter()
        .filter_map(|(name, held)| entry(held, key, errors).map(|built| (name.clone(), built)))
        .collect();
    (errors.len() == before).then_some(table)
}

/// A map of strings from a table attr: `env = { GRAF_ROOT = "/kb" }`.
fn expect_string_map<M: FromIterator<(String, String)>>(
    value: &RVal,
    key: &str,
    errors: &mut Vec<Diagnostic>,
) -> Option<M> {
    expect_table_of(
        value,
        key,
        "a table of strings",
        |held, key, errors| keep(expect_str(held, key), errors),
        errors,
    )
}

/// A map of string lists from a table attr: `file_roles = { ofx = [".ofx"] }`.
fn expect_string_list_map<M: FromIterator<(String, Vec<String>)>>(
    value: &RVal,
    key: &str,
    errors: &mut Vec<Diagnostic>,
) -> Option<M> {
    expect_table_of(
        value,
        key,
        "a table of string lists",
        expect_strings,
        errors,
    )
}

/// An optional attr the vocabulary types as a value, read as a string.
///
/// The `opt_*` readers are the typed-vocabulary counterparts of [`Body`]'s:
/// where a body's keys are open and matched by name, a vocabulary field is
/// named by the code that reads it, and what stays shared is the refusal.
fn opt_str(attr: Option<&Attr<RVal>>, key: &str, errors: &mut Vec<Diagnostic>) -> Option<String> {
    keep(expect_str(&attr?.value, key), errors)
}

fn opt_int<T: TryFrom<i64>>(
    attr: Option<&Attr<RVal>>,
    key: &str,
    errors: &mut Vec<Diagnostic>,
) -> Option<T> {
    keep(expect_int(&attr?.value, key), errors)
}

fn opt_bool(attr: Option<&Attr<RVal>>, key: &str, errors: &mut Vec<Diagnostic>) -> Option<bool> {
    keep(expect_bool(&attr?.value, key), errors)
}

fn opt_path(attr: Option<&Attr<RVal>>, key: &str, errors: &mut Vec<Diagnostic>) -> Option<PathBuf> {
    keep(expect_path(&attr?.value, key), errors)
}

fn opt_strings(
    attr: Option<&Attr<RVal>>,
    key: &str,
    errors: &mut Vec<Diagnostic>,
) -> Option<Vec<String>> {
    expect_strings(&attr?.value, key, errors)
}

/// An optional depth attr: a count, or the word `unbounded`.
fn opt_depth(
    attr: Option<&Attr<IntOrWord>>,
    key: &str,
    errors: &mut Vec<Diagnostic>,
) -> Option<Depth> {
    keep(depth(&attr?.value, key), errors)
}

/// An optional attr the vocabulary types as a value, read as a number.
fn opt_flt(attr: Option<&Attr<RVal>>, key: &str, errors: &mut Vec<Diagnostic>) -> Option<f64> {
    keep(expect_flt(&attr?.value, key), errors)
}

/// A depth that rode beside a body's keys rather than among them.
fn opt_projected_depth(
    value: Option<&IntOrWord>,
    key: &str,
    errors: &mut Vec<Diagnostic>,
) -> Option<Depth> {
    keep(depth(value?, key), errors)
}

/// A token attr the vocabulary admits and this position has no field for.
///
/// A statement tail is typed by the union of the tail fields across the
/// families its statement can lower into, because which family it is depends
/// on the address it names and the front end does not know that. So the key
/// that is legal in one family and not in another is refused here, at its own
/// token, naming what this family reads.
fn refuse_word(
    attr: Option<&Attr<Word>>,
    key: &str,
    what: &str,
    legal: &[&str],
    errors: &mut Vec<Diagnostic>,
) {
    if let Some(attr) = attr {
        refuse_key(attr.value.name.span(), key, what, legal, errors);
    }
}

/// The same refusal, for a union key the vocabulary types as a value.
fn refuse_val(
    attr: Option<&Attr<RVal>>,
    key: &str,
    what: &str,
    legal: &[&str],
    errors: &mut Vec<Diagnostic>,
) {
    if let Some(attr) = attr {
        refuse_key(attr.value.span(), key, what, legal, errors);
    }
}

/// A key this position has no field for, refused at its own token.
fn refuse_key(span: &Span, key: &str, what: &str, legal: &[&str], errors: &mut Vec<Diagnostic>) {
    errors.push(Diagnostic::at(
        format!(
            "`{key}` is not a key of {what}; expected {}",
            or_list(legal)
        ),
        span.clone(),
    ));
}

/// An optional token attr, through the enum's own `Deserialize`.
fn opt_token<T: DeserializeOwned>(
    attr: Option<&Attr<Word>>,
    key: &str,
    errors: &mut Vec<Diagnostic>,
) -> Option<T> {
    keep(token(&attr?.value, key), errors)
}

/// A bare word targeting a closed config enum, through that enum's own
/// `Deserialize`.
///
/// One token, no value tree: the serde spelling tables — `rename_all`, per
/// enum — stay the one source of truth for how a variant is written, and the
/// serde error, which names the legal spellings, becomes the diagnostic.
fn token<T: DeserializeOwned>(word: &Word, key: &str) -> Result<T, Diagnostic> {
    token_text(word.name.value(), word.name.span(), key)
}

/// The same seam, for a token that reached here as text.
///
/// A token context resolves to the word that was written, so a section's attrs
/// — a flat key/value list by the time lowering sees them — carry their tokens
/// as strings; the deserializer behind them is the same one.
fn token_text<T: DeserializeOwned>(text: &str, span: &Span, key: &str) -> Result<T, Diagnostic> {
    let deserializer = StrDeserializer::<serde::de::value::Error>::new(text);
    T::deserialize(deserializer)
        .map_err(|error| Diagnostic::at(format!("`{key}`: {error}"), span.clone()))
}

// ---------------------------------------------------------------------------
// Value types with their own spelling
// ---------------------------------------------------------------------------

/// The one spelling of an unbounded window.
const UNBOUNDED: &str = "unbounded";

/// A depth: a non-negative count, or the word `unbounded`.
///
/// The runtime's `Depth` deserializer never runs on this path, so this is the
/// one place the DSL spelling of a depth is decided; the equivalence tests pin
/// the two paths to the same result.
fn depth(value: &IntOrWord, key: &str) -> Result<Depth, Diagnostic> {
    const EXPECTED: &str = "a non-negative integer or the word `unbounded`";
    match value {
        IntOrWord::Int(count) => match u64::try_from(*count.value()) {
            Ok(bounded) => Ok(Depth::Bounded(bounded)),
            Err(_) => Err(Diagnostic::at(
                format!("`{key}`: expected {EXPECTED}, got {}", count.value()),
                count.span().clone(),
            )),
        },
        IntOrWord::Word(word) if word.name.value() == UNBOUNDED => Ok(Depth::Unbounded),
        IntOrWord::Word(word) => Err(Diagnostic::at(
            format!("`{key}`: expected {EXPECTED}, got `{}`", word.name.value()),
            word.name.span().clone(),
        )),
    }
}

/// The three keys a `send_rate` table states.
///
/// A table attr with no vocabulary behind it, so the keys are matched by hand
/// and a stray one is refused at its own token.
///
/// TODO(dsl-vocabulary-config-parity): this key set is a hand transcription of
/// `SendRate`'s fields.
const SEND_RATE_KEYS: [&str; 3] = ["burst", "refill_interval_secs", "refill"];

/// A `send_rate` table.
///
/// Starts from `SendRate::default()` and overwrites the keys the table states,
/// so an unstated key gets the struct's own default. A table holding two bad
/// keys reports both.
fn send_rate(value: &RVal, errors: &mut Vec<Diagnostic>) -> Option<SendRate> {
    let entries = match value.value() {
        RValue::Table(entries) => entries,
        _ => {
            errors.push(mismatch(value, "send_rate", "a table"));
            return None;
        }
    };
    let before = errors.len();
    let mut rate = SendRate::default();
    for (key, entry) in entries {
        match key.as_str() {
            "burst" => {
                if let Some(count) = keep(expect_int(entry, key), errors) {
                    rate.burst = count;
                }
            }
            "refill_interval_secs" => {
                if let Some(count) = keep(expect_int(entry, key), errors) {
                    rate.refill_interval_secs = count;
                }
            }
            "refill" => {
                if let Some(count) = keep(expect_int(entry, key), errors) {
                    rate.refill = count;
                }
            }
            _ => errors.push(Diagnostic::at(
                format!(
                    "`{key}` is not a send_rate key; expected {}",
                    or_list(SEND_RATE_KEYS)
                ),
                entry.span().clone(),
            )),
        }
    }
    (errors.len() == before).then_some(rate)
}

// ---------------------------------------------------------------------------
// Open key positions
// ---------------------------------------------------------------------------

/// A key/value body whose keys are matched by hand.
///
/// Sections reach lowering as a flat key/value list — resolution flattens the
/// typed vocabulary struct — so the keys are read by name here, and a key no
/// reader claimed is refused at its own token.
///
/// What that refusal is for depends on the position. Where the vocabulary gates
/// the keys, as it does for a section, an unclaimed key is a *stated attr that
/// lowering ignores*: the one way a value could otherwise disappear silently
/// between the two representations. Where the vocabulary does not gate them — a
/// statement tail, a component body — it is typo protection, at the key that
/// was written.
///
/// The legal set a refusal names is the set of keys the readers asked for, so
/// it cannot drift from the code that reads them.
struct Body<'a> {
    /// What the body is, for a refusal that has to say so.
    what: String,
    /// Where the body starts, for a key that is missing and therefore has no
    /// token of its own.
    at: Span,
    /// Keys not yet claimed by a reader.
    entries: Vec<(&'a str, &'a RVal)>,
    /// Every key a reader asked for, claimed or not.
    asked: Vec<&'static str>,
}

impl<'a> Body<'a> {
    fn new(what: impl Into<String>, at: Span, attrs: &'a [(String, RVal)]) -> Self {
        Body {
            what: what.into(),
            at,
            entries: attrs
                .iter()
                .map(|(key, value)| (key.as_str(), value))
                .collect(),
            asked: Vec::new(),
        }
    }

    /// The value under `key`, claimed.
    fn take(&mut self, key: &'static str) -> Option<&'a RVal> {
        self.asked.push(key);
        let position = self.entries.iter().position(|(name, _)| *name == key)?;
        Some(self.entries.remove(position).1)
    }

    /// The value under a key the target requires.
    ///
    /// Presence is the vocabulary's own `req`, checked when the block was
    /// deserialized, so this refusal is a belt to that brace rather than the
    /// first line of defence — and it is a refusal rather than a panic because
    /// a diagnostic at the block is what a reader of a `.brenn` file can act
    /// on.
    fn required(&mut self, key: &'static str, errors: &mut Vec<Diagnostic>) -> Option<&'a RVal> {
        match self.take(key) {
            Some(value) => Some(value),
            None => {
                errors.push(Diagnostic::at(
                    format!("{} states no `{key}`, which it requires", self.what),
                    self.at.clone(),
                ));
                None
            }
        }
    }

    fn str(&mut self, key: &'static str, errors: &mut Vec<Diagnostic>) -> Option<String> {
        let value = self.take(key)?;
        keep(expect_str(value, key), errors)
    }

    fn required_str(&mut self, key: &'static str, errors: &mut Vec<Diagnostic>) -> Option<String> {
        Some(self.required_spanned_str(key, errors)?.0)
    }

    /// A required string with its own token's span, for a refusal that must
    /// underline the word rather than the block that holds it.
    fn required_spanned_str(
        &mut self,
        key: &'static str,
        errors: &mut Vec<Diagnostic>,
    ) -> Option<(String, Span)> {
        let value = self.required(key, errors)?;
        let span = value.span().clone();
        keep(expect_str(value, key), errors).map(|text| (text, span))
    }

    fn int<T: TryFrom<i64>>(
        &mut self,
        key: &'static str,
        errors: &mut Vec<Diagnostic>,
    ) -> Option<T> {
        let value = self.take(key)?;
        keep(expect_int(value, key), errors)
    }

    fn required_int<T: TryFrom<i64>>(
        &mut self,
        key: &'static str,
        errors: &mut Vec<Diagnostic>,
    ) -> Option<T> {
        let value = self.required(key, errors)?;
        keep(expect_int(value, key), errors)
    }

    fn bool(&mut self, key: &'static str, errors: &mut Vec<Diagnostic>) -> Option<bool> {
        let value = self.take(key)?;
        keep(expect_bool(value, key), errors)
    }

    fn path(&mut self, key: &'static str, errors: &mut Vec<Diagnostic>) -> Option<PathBuf> {
        let value = self.take(key)?;
        keep(expect_path(value, key), errors)
    }

    fn required_path(
        &mut self,
        key: &'static str,
        errors: &mut Vec<Diagnostic>,
    ) -> Option<PathBuf> {
        let value = self.required(key, errors)?;
        keep(expect_path(value, key), errors)
    }

    fn strings(&mut self, key: &'static str, errors: &mut Vec<Diagnostic>) -> Option<Vec<String>> {
        let value = self.take(key)?;
        expect_strings(value, key, errors)
    }

    fn required_strings(
        &mut self,
        key: &'static str,
        errors: &mut Vec<Diagnostic>,
    ) -> Option<Vec<String>> {
        let value = self.required(key, errors)?;
        expect_strings(value, key, errors)
    }

    /// A token attr, through the enum's own `Deserialize`.
    ///
    /// A section's attrs reach lowering as a flat key/value list, so a token
    /// the vocabulary already gated arrives here as the text of the word that
    /// was written.
    fn token<T: DeserializeOwned>(
        &mut self,
        key: &'static str,
        errors: &mut Vec<Diagnostic>,
    ) -> Option<T> {
        let value = self.take(key)?;
        let text = keep(expect_str(value, key), errors)?;
        keep(token_text(&text, value.span(), key), errors)
    }

    /// A log level, through the config's own `deserialize_with`.
    fn level(&mut self, key: &'static str, errors: &mut Vec<Diagnostic>) -> Option<LevelFilter> {
        let value = self.take(key)?;
        let text = keep(expect_str(value, key), errors)?;
        let deserializer = StrDeserializer::<serde::de::value::Error>::new(&text);
        keep(
            deserialize_level_filter(deserializer)
                .map_err(|error| Diagnostic::at(format!("`{key}`: {error}"), value.span().clone())),
            errors,
        )
    }

    /// An operator-supplied map, as the `toml::Table` the raw field stores.
    fn config(&mut self, key: &'static str, errors: &mut Vec<Diagnostic>) -> Option<toml::Table> {
        let value = self.take(key)?;
        let entries = keep(expect_table(value, key), errors)?;
        keep(toml_table(entries, key), errors)
    }

    /// Every key the body wrote, as the `toml::Value` tree the raw field
    /// stores.
    ///
    /// The one open position among the sections: no key is claimed by name
    /// because none is known, so the whole body is claimed at once and
    /// [`Body::finish`] has nothing left to refuse. Each value is converted
    /// under its own key, so a value the TOML tree cannot hold — a matcher is
    /// the only one — is refused by name; `None` where any entry was refused.
    fn open(&mut self, errors: &mut Vec<Diagnostic>) -> Option<toml::Table> {
        let before = errors.len();
        let table: toml::Table = self
            .entries
            .drain(..)
            .filter_map(|(name, value)| {
                keep(rval_to_toml(value, name), errors).map(|held| (name.to_owned(), held))
            })
            .collect();
        (errors.len() == before).then_some(table)
    }

    /// A map of strings, as the ordered map the raw field stores.
    fn string_map(
        &mut self,
        key: &'static str,
        errors: &mut Vec<Diagnostic>,
    ) -> Option<BTreeMap<String, String>> {
        let value = self.take(key)?;
        expect_string_map(value, key, errors)
    }

    /// A map of string lists the target requires, as the map the raw field
    /// stores.
    fn required_string_list_map(
        &mut self,
        key: &'static str,
        errors: &mut Vec<Diagnostic>,
    ) -> Option<HashMap<String, Vec<String>>> {
        let value = self.required(key, errors)?;
        expect_string_list_map(value, key, errors)
    }

    fn send_rate(&mut self, key: &'static str, errors: &mut Vec<Diagnostic>) -> Option<SendRate> {
        let value = self.take(key)?;
        send_rate(value, errors)
    }

    /// Refuse whatever no reader claimed.
    ///
    /// Called once per body, after every reader has run.
    fn finish(self, errors: &mut Vec<Diagnostic>) {
        for (key, value) in self.entries {
            errors.push(Diagnostic::at(
                format!(
                    "`{key}` is not a key of {}; expected {}",
                    self.what,
                    or_list(&self.asked)
                ),
                value.span().clone(),
            ));
        }
    }
}

// ---------------------------------------------------------------------------
// Configuration sections
// ---------------------------------------------------------------------------

/// The scalar configuration sections of a document.
///
/// One field per section kindword, `None` where the document states no such
/// section: the assembly then gives that `BrennConfig` field its own default.
/// The two named kindwords are maps
/// instead — a document states as many `container` and `integration` sections as
/// it has names for.
#[derive(Default)]
struct Sections {
    server: Option<ServerConfig>,
    database: Option<DatabaseConfig>,
    logging: Option<LoggingConfig>,
    security: Option<SecurityConfig>,
    alerting: Option<AlertingConfig>,
    claude_defaults: Option<ClaudeDefaultsConfig>,
    repo_sync: Option<RepoSyncConfig>,
    messaging: Option<MessagingGlobalConfig>,
    observability: Option<ObservabilityConfig>,
    surface_description: Option<SurfaceDescriptionConfig>,
    llm_chat: Option<LlmChatConfig>,
    pwa_push: Option<PwaPushGlobalConfig>,
    automation: Option<AutomationGlobalConfig>,
    events: Option<EventsConfig>,
    wasm: Option<WasmConfig>,
    watchdog: Option<WatchdogConfig>,
    container: HashMap<String, ContainerConfig>,
    integrations: HashMap<String, toml::Value>,
}

/// TODO(dsl-vocabulary-config-parity): the kindword arms below, and every key
/// set the section bodies read, are a hand transcription of `BrennConfig`'s
/// section fields. The raw-field direction is mechanical — each arm ends in an
/// exhaustive struct literal — but a *new section* in the config, or a new attr
/// in an existing one, is caught by nothing here.
fn sections(list: &[RSection], errors: &mut Vec<Diagnostic>) -> Sections {
    let mut out = Sections::default();
    for section in unique(list, "a document", errors) {
        let kindword = section.kindword.value().as_str();
        let at = section.kindword.span().clone();
        let what = match &section.name {
            Some(name) => format!("a `{kindword} {}` section", name.value()),
            None => format!("a `{kindword}` section",),
        };
        let mut body = Body::new(what.clone(), at, &section.attrs);
        let mut subs = Subs::new(section, &what, errors);
        match kindword {
            "server" => out.server = Some(server(&mut body, errors)),
            "database" => out.database = Some(database(&mut body, errors)),
            "logging" => out.logging = Some(logging(&mut body, errors)),
            "security" => out.security = Some(security(&mut body, errors)),
            "alerting" => out.alerting = Some(alerting(&mut body, &mut subs, errors)),
            "claude_defaults" => out.claude_defaults = Some(claude_defaults(&mut body, errors)),
            "repo_sync" => out.repo_sync = Some(repo_sync(&mut body, errors)),
            "messaging" => out.messaging = Some(messaging(&mut body, errors)),
            "observability" => {
                out.observability = Some(observability(&mut body, &mut subs, errors));
            }
            "surface_description" => {
                out.surface_description = Some(surface_description(&mut body, errors));
            }
            "llm_chat" => out.llm_chat = Some(llm_chat(&mut body, errors)),
            "pwa_push" => out.pwa_push = Some(pwa_push(&mut body, errors)),
            "automation" => out.automation = Some(automation(&mut body, errors)),
            "events" => out.events = Some(events(&mut body, errors)),
            "wasm" => out.wasm = Some(wasm(&mut body, errors)),
            "watchdog" => out.watchdog = Some(watchdog(&mut body, errors)),
            "container" => {
                let name = section
                    .name
                    .as_ref()
                    .expect("a container section carries a name")
                    .value()
                    .clone();
                out.container.insert(name, container(&mut body, errors));
            }
            "integration" => {
                let name = section
                    .name
                    .as_ref()
                    .expect("an integration section carries a name")
                    .value()
                    .clone();
                if let Some(table) = body.open(errors) {
                    out.integrations.insert(name, toml::Value::Table(table));
                }
            }
            other => panic!("`{other}` is not a configuration section kindword"),
        }
        body.finish(errors);
        subs.finish(&what, errors);
    }
    out
}

/// "At most one of this key here", with both sites cited when there are two.
///
/// Sections, mcp servers and hook blocks all arrive as flat lists with no
/// deduplication, and all three want the same answer: keep the first, refuse
/// the second, and say where the first was. One place decides that so the
/// wording cannot drift between them — and the wording itself is
/// `brenn_dsl::diag::duplicate_statement`, shared with resolution's own
/// duplicate checks, so belt and brace read alike.
#[derive(Default)]
struct FirstWins {
    first: HashMap<String, Span>,
}

impl FirstWins {
    /// Whether `key` is the first of its name, recording a two-site refusal
    /// when it is not.
    fn admit(
        &mut self,
        key: String,
        span: &Span,
        context: &str,
        errors: &mut Vec<Diagnostic>,
    ) -> bool {
        if let Some(earlier) = self.first.get(&key) {
            errors.push(duplicate_statement(
                context,
                &key,
                span.clone(),
                earlier.clone(),
            ));
            return false;
        }
        self.first.insert(key, span.clone());
        true
    }
}

/// The first section under each kindword and name, refusing every later one.
///
/// Defense in depth behind resolution's own `duplicate_sections`
/// (`brenn-dsl/src/resolve.rs`): a second section must not win silently even if
/// that check is loosened or bypassed. Both layers key on
/// `brenn_dsl::model::section_key`, so neither can come to count a different
/// thing.
fn unique<'a>(
    list: &'a [RSection],
    context: &str,
    errors: &mut Vec<Diagnostic>,
) -> Vec<&'a RSection> {
    let mut seen = FirstWins::default();
    let mut kept = Vec::new();
    for section in list {
        let key = section_key(
            section.kindword.value(),
            section.name.as_ref().map(|name| name.value().as_str()),
        );
        if seen.admit(key, section.kindword.span(), context, errors) {
            kept.push(section);
        }
    }
    kept
}

/// The sub-blocks a section holds, and which of them a reader took.
///
/// Defense in depth: resolution refuses sub-blocks under parents with no
/// vocabulary for them, but [`Subs::finish`] is what prevents a nested block
/// from vanishing silently if that check ever loosens. The arms that call
/// [`Subs::take`] are the sole record of which kindwords nest — no separate
/// list to fall out of step.
struct Subs<'a> {
    held: Vec<(&'a RSection, bool)>,
}

impl<'a> Subs<'a> {
    fn new(section: &'a RSection, context: &str, errors: &mut Vec<Diagnostic>) -> Self {
        Self {
            held: unique(&section.subs, context, errors)
                .into_iter()
                .map(|held| (held, false))
                .collect(),
        }
    }

    /// The sub-block under `kindword`, if the body states one, marked as read.
    fn take(&mut self, kindword: &str) -> Option<&'a RSection> {
        let held = self
            .held
            .iter_mut()
            .find(|(section, _)| section.kindword.value() == kindword)?;
        held.1 = true;
        Some(held.0)
    }

    /// Refuse whatever no reader took.
    ///
    /// Called once per block, after every reader has run — the sub-block twin
    /// of [`Body::finish`].
    fn finish(self, what: &str, errors: &mut Vec<Diagnostic>) {
        for (held, read) in self.held {
            if read {
                continue;
            }
            // Worded for both kinds of parent: the one that nests nothing, and
            // the one whose sub-block this simply is not. A block reaching
            // here under `alerting` is a kindword typo, not proof that
            // `alerting` nests nothing.
            errors.push(Diagnostic::at(
                format!("`{}` is not a sub-block of {what}", held.kindword.value()),
                held.kindword.span().clone(),
            ));
        }
    }
}

fn server(body: &mut Body, errors: &mut Vec<Diagnostic>) -> ServerConfig {
    let defaults = ServerConfig::default();
    ServerConfig {
        bind_address: body
            .token::<SocketAddr>("bind_address", errors)
            .unwrap_or(defaults.bind_address),
        static_dir: body
            .path("static_dir", errors)
            .unwrap_or(defaults.static_dir),
        surface_dist_dir: body
            .path("surface_dist_dir", errors)
            .unwrap_or(defaults.surface_dist_dir),
        secure_cookies: body
            .bool("secure_cookies", errors)
            .unwrap_or(defaults.secure_cookies),
        trusted_proxy_hops: body
            .int("trusted_proxy_hops", errors)
            .unwrap_or(defaults.trusted_proxy_hops),
        pid_file: body.path("pid_file", errors),
        // An `Option` the config refuses to boot without, and the vocabulary
        // requires: absence is refused here rather than at startup.
        public_url: body.required_str("public_url", errors),
    }
}

fn database(body: &mut Body, errors: &mut Vec<Diagnostic>) -> DatabaseConfig {
    let defaults = DatabaseConfig::default();
    DatabaseConfig {
        path: body.path("path", errors).unwrap_or(defaults.path),
    }
}

fn logging(body: &mut Body, errors: &mut Vec<Diagnostic>) -> LoggingConfig {
    let defaults = LoggingConfig::default();
    LoggingConfig {
        log_dir: body.path("log_dir", errors).unwrap_or(defaults.log_dir),
        console_level: body
            .level("console_level", errors)
            .unwrap_or(defaults.console_level),
        file_level: body
            .level("file_level", errors)
            .unwrap_or(defaults.file_level),
    }
}

fn security(body: &mut Body, errors: &mut Vec<Diagnostic>) -> SecurityConfig {
    let defaults = SecurityConfig::default();
    SecurityConfig {
        auth_rate_interval_secs: body
            .int("auth_rate_interval_secs", errors)
            .unwrap_or(defaults.auth_rate_interval_secs),
        auth_rate_burst: body
            .int("auth_rate_burst", errors)
            .unwrap_or(defaults.auth_rate_burst),
        global_rate_interval_secs: body
            .int("global_rate_interval_secs", errors)
            .unwrap_or(defaults.global_rate_interval_secs),
        global_rate_burst: body
            .int("global_rate_burst", errors)
            .unwrap_or(defaults.global_rate_burst),
        asset_rate_interval_secs: body
            .int("asset_rate_interval_secs", errors)
            .unwrap_or(defaults.asset_rate_interval_secs),
        asset_rate_burst: body
            .int("asset_rate_burst", errors)
            .unwrap_or(defaults.asset_rate_burst),
        auth_body_limit: body
            .int("auth_body_limit", errors)
            .unwrap_or(defaults.auth_body_limit),
        global_body_limit: body
            .int("global_body_limit", errors)
            .unwrap_or(defaults.global_body_limit),
        upload_body_limit: body
            .int("upload_body_limit", errors)
            .unwrap_or(defaults.upload_body_limit),
        max_image_long_edge: body
            .int("max_image_long_edge", errors)
            .unwrap_or(defaults.max_image_long_edge),
    }
}

/// The `alerting` section, and the backend sub-block that delivers for it.
///
/// The section has no `Default` — its two rate-limit keys are required both in
/// the config and in the vocabulary — so a refused key leaves a zero here and
/// the whole lowering fails.
fn alerting(body: &mut Body, subs: &mut Subs, errors: &mut Vec<Diagnostic>) -> AlertingConfig {
    AlertingConfig {
        max_alerts: body.required_int("max_alerts", errors).unwrap_or_default(),
        window_secs: body.required_int("window_secs", errors).unwrap_or_default(),
        ntfy: subs.take("ntfy").map(|block| {
            let what = "an `ntfy` block";
            let mut body = Body::new(what, block.kindword.span().clone(), &block.attrs);
            let config = NtfyConfig {
                url: body.required_str("url", errors).unwrap_or_default(),
            };
            body.finish(errors);
            Subs::new(block, what, errors).finish(what, errors);
            config
        }),
        mail: subs.take("mail").map(|block| {
            let what = "a `mail` block";
            let mut body = Body::new(what, block.kindword.span().clone(), &block.attrs);
            let config = MailConfig {
                to: body.required_str("to", errors).unwrap_or_default(),
                subject_label: body
                    .str("subject_label", errors)
                    .unwrap_or_else(default_subject_label),
            };
            body.finish(errors);
            Subs::new(block, what, errors).finish(what, errors);
            config
        }),
    }
}

fn claude_defaults(body: &mut Body, errors: &mut Vec<Diagnostic>) -> ClaudeDefaultsConfig {
    let defaults = ClaudeDefaultsConfig::default();
    ClaudeDefaultsConfig {
        mcp_script_path: body
            .path("mcp_script_path", errors)
            .unwrap_or(defaults.mcp_script_path),
        model: body.str("model", errors).unwrap_or(defaults.model),
    }
}

fn repo_sync(body: &mut Body, errors: &mut Vec<Diagnostic>) -> RepoSyncConfig {
    let defaults = RepoSyncConfig::default();
    RepoSyncConfig {
        repo_dir: body.path("repo_dir", errors),
        poll_interval_secs: body
            .int("poll_interval_secs", errors)
            .unwrap_or(defaults.poll_interval_secs),
        stale_conversation_days: body
            .int("stale_conversation_days", errors)
            .unwrap_or(defaults.stale_conversation_days),
    }
}

fn messaging(body: &mut Body, errors: &mut Vec<Diagnostic>) -> MessagingGlobalConfig {
    let defaults = MessagingGlobalConfig::default();
    MessagingGlobalConfig {
        default_send_budget: body
            .int("default_send_budget", errors)
            .unwrap_or(defaults.default_send_budget),
        max_body_bytes: body
            .int("max_body_bytes", errors)
            .unwrap_or(defaults.max_body_bytes),
        default_noise: body
            .token("default_noise", errors)
            .unwrap_or(defaults.default_noise),
        default_sink: body
            .token("default_sink", errors)
            .unwrap_or(defaults.default_sink),
        archive_path: body.path("archive_path", errors),
        default_wake_min: body
            .token("default_wake_min", errors)
            .unwrap_or(defaults.default_wake_min),
        default_send_rate: body
            .send_rate("default_send_rate", errors)
            .unwrap_or(defaults.default_send_rate),
    }
}

fn observability(
    body: &mut Body,
    subs: &mut Subs,
    errors: &mut Vec<Diagnostic>,
) -> ObservabilityConfig {
    ObservabilityConfig {
        usage: subs
            .take("usage")
            .map_or_else(UsageObservabilityConfig::default, |block| {
                let what = "a `usage` block";
                let defaults = UsageObservabilityConfig::default();
                let mut body = Body::new(what, block.kindword.span().clone(), &block.attrs);
                let config = UsageObservabilityConfig {
                    session_gap_minutes: body
                        .int("session_gap_minutes", errors)
                        .unwrap_or(defaults.session_gap_minutes),
                };
                body.finish(errors);
                Subs::new(block, what, errors).finish(what, errors);
                config
            }),
        surface_error_channel: body.str("surface_error_channel", errors),
        surface_error_publish_floor: body
            .token("surface_error_publish_floor", errors)
            .unwrap_or_else(default_surface_error_publish_floor),
    }
}

fn surface_description(body: &mut Body, errors: &mut Vec<Diagnostic>) -> SurfaceDescriptionConfig {
    let defaults = SurfaceDescriptionConfig::default();
    SurfaceDescriptionConfig {
        prefix: body.str("prefix", errors).unwrap_or(defaults.prefix),
        status_interval_secs: body
            .int("status_interval_secs", errors)
            .unwrap_or(defaults.status_interval_secs),
    }
}

fn llm_chat(body: &mut Body, errors: &mut Vec<Diagnostic>) -> LlmChatConfig {
    let defaults = LlmChatConfig::default();
    LlmChatConfig {
        prefix: body.str("prefix", errors).unwrap_or(defaults.prefix),
        retained_window: body
            .int("retained_window", errors)
            .unwrap_or(defaults.retained_window),
        wake_min: body.token("wake_min", errors).unwrap_or(defaults.wake_min),
        idle_timeout_secs: body
            .int("idle_timeout_secs", errors)
            .unwrap_or(defaults.idle_timeout_secs),
    }
}

fn pwa_push(body: &mut Body, errors: &mut Vec<Diagnostic>) -> PwaPushGlobalConfig {
    PwaPushGlobalConfig {
        keypair_file: body.path("keypair_file", errors),
        subject: body.str("subject", errors),
        endpoint_host_allowlist: body
            .strings("endpoint_host_allowlist", errors)
            .unwrap_or_else(default_endpoint_host_allowlist),
        endpoint_host_allowlist_enforce: body
            .bool("endpoint_host_allowlist_enforce", errors)
            .unwrap_or_else(default_endpoint_host_allowlist_enforce),
    }
}

fn automation(body: &mut Body, errors: &mut Vec<Diagnostic>) -> AutomationGlobalConfig {
    let defaults = AutomationGlobalConfig::default();
    AutomationGlobalConfig {
        max_fires_per_hour_per_job: body
            .int("max_fires_per_hour_per_job", errors)
            .unwrap_or(defaults.max_fires_per_hour_per_job),
        max_error_reports_per_hour_per_job: body
            .int("max_error_reports_per_hour_per_job", errors)
            .unwrap_or(defaults.max_error_reports_per_hour_per_job),
        consecutive_failures_to_disable: body
            .int("consecutive_failures_to_disable", errors)
            .unwrap_or(defaults.consecutive_failures_to_disable),
        max_jobs_per_app: body
            .int("max_jobs_per_app", errors)
            .unwrap_or(defaults.max_jobs_per_app),
    }
}

fn events(body: &mut Body, errors: &mut Vec<Diagnostic>) -> EventsConfig {
    let defaults = EventsConfig::default();
    EventsConfig {
        delivered_retention_days: body
            .int("delivered_retention_days", errors)
            .unwrap_or(defaults.delivered_retention_days),
    }
}

fn wasm(body: &mut Body, errors: &mut Vec<Diagnostic>) -> WasmConfig {
    WasmConfig {
        store_size_limit: body
            .str("store_size_limit", errors)
            .unwrap_or_else(default_store_size_limit),
    }
}

fn watchdog(body: &mut Body, errors: &mut Vec<Diagnostic>) -> WatchdogConfig {
    let defaults = WatchdogConfig::default();
    WatchdogConfig {
        sweep_interval_secs: body
            .int("sweep_interval_secs", errors)
            .unwrap_or(defaults.sweep_interval_secs),
        wedge_grace_secs: body
            .int("wedge_grace_secs", errors)
            .unwrap_or(defaults.wedge_grace_secs),
    }
}

fn container(body: &mut Body, errors: &mut Vec<Diagnostic>) -> ContainerConfig {
    ContainerConfig {
        image: body.required_str("image", errors).unwrap_or_default(),
        home_dir: body.required_path("home_dir", errors).unwrap_or_default(),
        container_home: body
            .path("container_home", errors)
            .unwrap_or_else(default_container_home),
        extra_mounts: body.strings("extra_mounts", errors).unwrap_or_default(),
        extra_args: body.strings("extra_args", errors).unwrap_or_default(),
    }
}

// ---------------------------------------------------------------------------
// Repos and mqtt clients
// ---------------------------------------------------------------------------

/// `[[repo]]` per `repo` declaration.
///
/// Repos are top-level only — an assembly cannot stamp one — so the dotted
/// handle is the single segment as written, and it is the wire slug.
fn repos(list: &[RNamed<RepoAttrs<RVal>>], errors: &mut Vec<Diagnostic>) -> Vec<RepoDeclRaw> {
    list.iter()
        .map(|repo| {
            let RepoAttrs { remote, auto_pull } = &repo.attrs;
            RepoDeclRaw {
                slug: repo.handle.dotted(),
                remote: keep(expect_str(&remote.value, "remote"), errors).unwrap_or_default(),
                auto_pull: opt_bool(auto_pull.as_ref(), "auto_pull", errors)
                    .unwrap_or_else(default_true),
            }
        })
        .collect()
}

/// `[[mqtt_client]]` per `mqtt_client` declaration.
///
/// `last_will` has no attr spelling, so a lowered client never carries one, and
/// the runtime treats `None` as no last will.
fn mqtt_clients(
    list: &[RNamed<MqttClientAttrs<RVal>>],
    errors: &mut Vec<Diagnostic>,
) -> Vec<MqttClientConfigRaw> {
    list.iter()
        .map(|client| {
            let MqttClientAttrs {
                url,
                username,
                password_file,
                ca_file,
                tls_version_min,
                keepalive_secs,
                inbound_payload_cap_bytes,
                reconnect_backoff_initial_secs,
                reconnect_backoff_max_secs,
                session_expiry_secs,
                qos,
                urgency,
            } = &client.attrs;
            MqttClientConfigRaw {
                slug: client.handle.dotted(),
                url: keep(expect_str(&url.value, "url"), errors).unwrap_or_default(),
                username: opt_str(username.as_ref(), "username", errors),
                password_file: opt_path(password_file.as_ref(), "password_file", errors),
                ca_file: opt_path(ca_file.as_ref(), "ca_file", errors),
                tls_version_min: opt_str(tls_version_min.as_ref(), "tls_version_min", errors)
                    .unwrap_or_else(default_tls_version_min),
                keepalive_secs: opt_int(keepalive_secs.as_ref(), "keepalive_secs", errors),
                inbound_payload_cap_bytes: opt_int(
                    inbound_payload_cap_bytes.as_ref(),
                    "inbound_payload_cap_bytes",
                    errors,
                )
                .unwrap_or_else(default_inbound_payload_cap),
                last_will: None,
                reconnect_backoff_initial_secs: opt_int(
                    reconnect_backoff_initial_secs.as_ref(),
                    "reconnect_backoff_initial_secs",
                    errors,
                )
                .unwrap_or_else(default_backoff_initial),
                reconnect_backoff_max_secs: opt_int(
                    reconnect_backoff_max_secs.as_ref(),
                    "reconnect_backoff_max_secs",
                    errors,
                )
                .unwrap_or_else(default_backoff_max),
                qos: opt_int(qos.as_ref(), "qos", errors).unwrap_or_else(default_subscription_qos),
                urgency: opt_token(urgency.as_ref(), "urgency", errors)
                    .unwrap_or_else(default_client_urgency),
                session_expiry_secs: opt_int(
                    session_expiry_secs.as_ref(),
                    "session_expiry_secs",
                    errors,
                )
                .unwrap_or_default(),
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Agents
// ---------------------------------------------------------------------------

/// `[[app]]` per agent, with the authority derivation computed for it.
fn apps(derived: &DerivedConfig, errors: &mut Vec<Diagnostic>) -> Vec<AppConfigRaw> {
    let resolved = &derived.resolved;
    resolved
        .agents
        .iter()
        .zip(&derived.agents)
        .map(|(agent, authority)| app(resolved, agent, authority, errors))
        .collect()
}

/// One `[[app]]`.
///
/// The scalar attrs transcribe by name; what is synthesized is the wire slug
/// (resolution applied the explicit-slug-else-dotted-handle rule), the
/// capability tokens and the ACL block from the derivation, and the statement
/// families — mounts, mcp servers, hooks and subscriptions.
///
/// The config's nested tables with no attr spelling are set empty here rather
/// than defaulted implicitly: approval rules, tool grants, frontmatter
/// rendering and the per-app push block.
fn app(
    resolved: &DslResolved,
    agent: &RAgent,
    authority: &DAuthority,
    errors: &mut Vec<Diagnostic>,
) -> AppConfigRaw {
    // Destructured with no `..`: an attr added to `AgentAttrs` fails
    // compilation here rather than being read by nobody. The two bindings the
    // lowering deliberately does not read are the skip list, named so the
    // compiler still counts them.
    let AgentAttrs {
        // Unused: the resolved slug is on `agent.slug`.
        slug: _slug,
        // Unused: lowered through the `authority` parameter.
        grants: _grants,
        name,
        description,
        icon,
        working_dir,
        model,
        single_instance,
        singleton,
        persistent,
        multiuser,
        idle_timeout_secs,
        idle_hook_secs,
        compact_reminder_pct,
        compact_soft_pct,
        compact_red_pct,
        compact_hard_pct,
        compact_reminder_tokens,
        compact_soft_tokens,
        compact_red_tokens,
        compact_hard_tokens,
        compact_idle_secs,
        history_replay_limit,
        allowed_users,
        disabled_tools,
        cc_extra_args,
        integrations,
        extra_mounts,
        prefix_username,
        prefix_timestamp,
        prefix_device,
        container,
        container_working_dir,
        send_budget,
    }: &AgentAttrs<RVal> = &agent.attrs;
    let label = agent.slug.value().clone();
    let (start_hooks, post_pull_hooks, startup_hooks) = hook_blocks(&agent.hooks, errors);
    let send_budget = opt_int(send_budget.as_ref(), "send_budget", errors);
    let subs = subscriptions(resolved, agent, errors);
    // The `messaging` block is present exactly when the agent has something to
    // put in it. An empty block carries no meaning — whether an app may use the
    // bus is decided by its grants, not by the block's presence.
    let messaging =
        (!subs.messaging.is_empty() || send_budget.is_some()).then_some(MessagingConfigRaw {
            subscribe: subs.messaging,
            send_budget,
        });
    AppConfigRaw {
        slug: label.clone(),
        name: opt_str(name.as_ref(), "name", errors),
        description: opt_str(description.as_ref(), "description", errors),
        icon: opt_str(icon.as_ref(), "icon", errors),
        working_dir: opt_path(working_dir.as_ref(), "working_dir", errors),
        model: opt_str(model.as_ref(), "model", errors),
        single_instance: opt_bool(single_instance.as_ref(), "single_instance", errors)
            .unwrap_or_default(),
        singleton: opt_bool(singleton.as_ref(), "singleton", errors).unwrap_or_default(),
        persistent: opt_bool(persistent.as_ref(), "persistent", errors).unwrap_or_default(),
        idle_timeout_secs: opt_int(idle_timeout_secs.as_ref(), "idle_timeout_secs", errors),
        compact_reminder_pct: opt_int(
            compact_reminder_pct.as_ref(),
            "compact_reminder_pct",
            errors,
        ),
        compact_soft_pct: opt_int(compact_soft_pct.as_ref(), "compact_soft_pct", errors),
        compact_red_pct: opt_int(compact_red_pct.as_ref(), "compact_red_pct", errors),
        compact_hard_pct: opt_int(compact_hard_pct.as_ref(), "compact_hard_pct", errors),
        compact_reminder_tokens: opt_int(
            compact_reminder_tokens.as_ref(),
            "compact_reminder_tokens",
            errors,
        ),
        compact_soft_tokens: opt_int(compact_soft_tokens.as_ref(), "compact_soft_tokens", errors),
        compact_red_tokens: opt_int(compact_red_tokens.as_ref(), "compact_red_tokens", errors),
        compact_hard_tokens: opt_int(compact_hard_tokens.as_ref(), "compact_hard_tokens", errors),
        compact_idle_secs: opt_int(compact_idle_secs.as_ref(), "compact_idle_secs", errors),
        idle_hook_secs: opt_int(idle_hook_secs.as_ref(), "idle_hook_secs", errors),
        allowed_users: opt_strings(allowed_users.as_ref(), "allowed_users", errors)
            .unwrap_or_default(),
        disabled_tools: opt_strings(disabled_tools.as_ref(), "disabled_tools", errors)
            .unwrap_or_default(),
        mcp_servers: mcp_servers(resolved, agent, errors),
        multiuser: opt_bool(multiuser.as_ref(), "multiuser", errors).unwrap_or_default(),
        prefix_username: opt_bool(prefix_username.as_ref(), "prefix_username", errors),
        prefix_timestamp: opt_bool(prefix_timestamp.as_ref(), "prefix_timestamp", errors),
        prefix_device: opt_bool(prefix_device.as_ref(), "prefix_device", errors),
        container: opt_str(container.as_ref(), "container", errors),
        container_working_dir: opt_path(
            container_working_dir.as_ref(),
            "container_working_dir",
            errors,
        ),
        start_hooks,
        post_pull_hooks,
        startup_hooks,
        cc_extra_args: opt_strings(cc_extra_args.as_ref(), "cc_extra_args", errors)
            .unwrap_or_default(),
        // No attr spelling: a nested table of rule patterns.
        approval_rules: Vec::new(),
        attachment_targets: attachment_targets(&agent.attachment_targets, &label, errors),
        integrations: opt_strings(integrations.as_ref(), "integrations", errors)
            .unwrap_or_default(),
        integration_config: integration_config(&agent.integration_configs, &label, errors),
        mounts: agent
            .mounts
            .iter()
            .map(|entry| mount(entry, errors))
            .collect(),
        extra_mounts: opt_strings(extra_mounts.as_ref(), "extra_mounts", errors)
            .unwrap_or_default(),
        history_replay_limit: opt_int(
            history_replay_limit.as_ref(),
            "history_replay_limit",
            errors,
        ),
        // No attr spelling: a nested rendering-rules table.
        frontmatter: super::frontmatter::FrontmatterRenderConfig::default(),
        messaging,
        // No attr spelling: the per-app push block states a default title.
        pwa_push: None,
        webhook_subscriptions: subs.webhook,
        mqtt_subscriptions: subs.mqtt,
        grants: authority
            .grants
            .iter()
            .map(|granted| grant::<AppCapability>(granted.value(), granted.span(), &label))
            .collect(),
        acl: app_acl(&authority.acl, &label),
        tool_grants: tool_grants(&agent.tools),
    }
}

/// The `tool` statements a participant holds, one raw grant each.
///
/// Same shape for an agent and for a component instance: one statement form,
/// one raw. What the clause keys mean is the registry's at boot — the document
/// carried them unexamined and so does this.
fn tool_grants(tools: &[RToolGrant]) -> Vec<ToolGrantRaw> {
    tools
        .iter()
        .map(|grant| ToolGrantRaw {
            tool: grant.tool.value().clone(),
            acl: grant
                .clauses
                .iter()
                .map(|clause| clause.iter().cloned().collect())
                .collect(),
            rate_limit: grant.rate_limit.map(|limit| RateLimitRaw {
                burst: limit.burst,
                sustained_per_minute: limit.sustained_per_minute,
            }),
        })
        .collect()
}

/// One `[[app.mount]]`, from a `mount` statement's tail.
fn mount(entry: &RMount, errors: &mut Vec<Diagnostic>) -> MountConfigRaw {
    let MountTail {
        access,
        working_dir,
        auto_pull,
        primary,
    } = &entry.tail;
    MountConfigRaw {
        repo: entry.repo.dotted(),
        access: opt_token::<AccessLevel>(access.as_ref(), "access", errors).unwrap_or_default(),
        working_dir: opt_bool(working_dir.as_ref(), "working_dir", errors).unwrap_or_default(),
        auto_pull: opt_bool(auto_pull.as_ref(), "auto_pull", errors),
        primary: opt_bool(primary.as_ref(), "primary", errors).unwrap_or_default(),
    }
}

/// The `mcp_servers` map: a named reference inlines the top-level definition it
/// names, a body defines one scoped to this agent.
///
/// Two entries under one key would be one server with two definitions, so the
/// second is refused at both sites. The reserved `brenn` key stays
/// `validate_and_resolve`'s check — it is the same check for both front ends.
fn mcp_servers(
    resolved: &DslResolved,
    agent: &RAgent,
    errors: &mut Vec<Diagnostic>,
) -> HashMap<String, McpServerConfig> {
    let mut servers = HashMap::new();
    let mut seen = FirstWins::default();
    let context = format!("agent `{}`", agent.slug.value());
    for entry in &agent.mcps {
        let (key, span, attrs) = match entry {
            RMcp::Ref(name) => {
                // TODO(dsl-mcp-ref-index): resolution hands out an index for a
                // channel reference and a name for an mcp reference, so this
                // one is matched by string; an index would drop the rescan and
                // the invariant that backs the panic below.
                let definition = resolved
                    .mcp_servers
                    .iter()
                    .find(|candidate| candidate.handle.dotted() == *name.value())
                    .expect("a resolved mcp reference names a definition of the document");
                (name.value().clone(), name.span().clone(), &definition.attrs)
            }
            RMcp::Inline(named) => {
                let leaf = named
                    .handle
                    .0
                    .last()
                    .expect("an inline mcp definition carries its name");
                (leaf.value().clone(), leaf.span().clone(), &named.attrs)
            }
        };
        if !seen.admit(key.clone(), &span, &context, errors) {
            continue;
        }
        servers.insert(key, mcp(attrs, errors));
    }
    servers
}

fn mcp(attrs: &McpServerAttrs<RVal>, errors: &mut Vec<Diagnostic>) -> McpServerConfig {
    let McpServerAttrs { command, args, env } = attrs;
    McpServerConfig {
        command: keep(expect_str(&command.value, "command"), errors).unwrap_or_default(),
        // The raw field is a plain `Vec`, so an omitted `args` is the empty one.
        args: opt_strings(args.as_ref(), "args", errors).unwrap_or_default(),
        env: env
            .as_ref()
            .and_then(|attr| expect_string_map(&attr.value, "env", errors))
            .unwrap_or_default(),
    }
}

/// The three hook fields, each filled by the block whose kindword names it.
///
/// A second block under one kindword is two answers to one question, so it is
/// refused at both sites.
fn hook_blocks(
    blocks: &[RHooks],
    errors: &mut Vec<Diagnostic>,
) -> (
    Option<StartHooksConfig>,
    Option<PostPullHooksConfig>,
    Option<StartupHooksConfig>,
) {
    let mut start = None;
    let mut post_pull = None;
    let mut startup = None;
    let mut seen = FirstWins::default();
    for block in blocks {
        let kindword = block.kindword.value().as_str();
        if !seen.admit(
            kindword.to_string(),
            block.kindword.span(),
            "an agent",
            errors,
        ) {
            continue;
        }
        let (host, container) = hook_scripts(block, errors);
        match kindword {
            "start_hooks" => start = Some(StartHooksConfig { host, container }),
            "post_pull_hooks" => post_pull = Some(PostPullHooksConfig { host, container }),
            "startup_hooks" => startup = Some(StartupHooksConfig { host, container }),
            other => panic!("`{other}` is not a hook block kindword"),
        }
    }
    (start, post_pull, startup)
}

/// The `[app.integration_config.<name>]` tables of one agent.
///
/// One entry per block, keyed by the block's name; the body is open, so the
/// whole tree is claimed at once and nothing is read by name. A block whose
/// body holds a value the TOML tree cannot carry is dropped and its refusal
/// stands. Keys cannot collide — resolution refused two blocks under one name.
fn integration_config(
    blocks: &[RSection],
    app: &str,
    errors: &mut Vec<Diagnostic>,
) -> HashMap<String, toml::Value> {
    let mut out = HashMap::new();
    for block in blocks {
        let name = block
            .name
            .as_ref()
            .expect("an integration_config block carries a name")
            .value()
            .clone();
        let what = format!("`integration_config {name}` of app `{app}`");
        let mut body = Body::new(what, block.kindword.span().clone(), &block.attrs);
        if let Some(table) = body.open(errors) {
            out.insert(name, toml::Value::Table(table));
        }
        body.finish(errors);
    }
    out
}

/// The `[[app.attachment_target]]` entries of one agent, in declaration order.
///
/// A target that cannot be built is dropped and its refusals stand: the app
/// still lowers, and the whole lowering fails at the end.
fn attachment_targets(
    blocks: &[RAttachmentTarget],
    app: &str,
    errors: &mut Vec<Diagnostic>,
) -> Vec<AttachmentTargetRaw> {
    blocks
        .iter()
        .filter_map(|block| attachment_target(block, app, errors))
        .collect()
}

/// One `attachment_target` block.
///
/// The wire name is the `name` attr where the block states one, and the block's
/// own name otherwise — the rule an entity's `slug` follows.
fn attachment_target(
    block: &RAttachmentTarget,
    app: &str,
    errors: &mut Vec<Diagnostic>,
) -> Option<AttachmentTargetRaw> {
    let declared = block
        .name
        .as_ref()
        .expect("an attachment_target block carries a name")
        .value()
        .clone();
    let what = format!("`attachment_target {declared}` of app `{app}`");
    let mut body = Body::new(what.clone(), block.kindword.span().clone(), &block.attrs);
    let name = body.str("name", errors).unwrap_or(declared);
    let label = body.required_str("label", errors);
    let accept = body.required_strings("accept", errors);
    let multi = body.bool("multi", errors).unwrap_or_default();
    body.finish(errors);
    let handler = handler(block, &what, errors);
    Some(AttachmentTargetRaw {
        name,
        label: label?,
        accept: accept?,
        multi,
        handler: handler?,
    })
}

/// The `handler` block, as the internally tagged variant its `type` word names.
///
/// Hand-dispatched: `StrDeserializer` builds unit variants only, and this one
/// carries fields. The reader set per arm is what makes a stated attr the
/// chosen type has no field for a refusal — [`Body::finish`] names exactly the
/// keys that arm asked for.
///
/// TODO(dsl-vocabulary-config-parity): the type words below and each arm's
/// field set are a hand transcription of `AttachmentHandlerConfig`'s variants.
/// Nothing holds the two in step; the words here are the only spelling left.
fn handler(
    block: &RAttachmentTarget,
    what: &str,
    errors: &mut Vec<Diagnostic>,
) -> Option<AttachmentHandlerConfig> {
    let held = block.subs.first().unwrap_or_else(|| {
        panic!("{what} states no `handler` block, which resolution refuses before lowering runs")
    });
    let mut body = Body::new(
        format!("`handler` of {what}"),
        held.kindword.span().clone(),
        &held.attrs,
    );
    let (kind, at) = body.required_spanned_str("type", errors)?;
    // Every reader runs before the first `?`: a block missing two required
    // fields is refused for both.
    let built = match kind.as_str() {
        "command" => {
            let program = body.required_str("program", errors);
            let args = body.required_strings("args", errors);
            let file_roles = body.required_string_list_map("file_roles", errors);
            let timeout_secs = body.int("timeout_secs", errors);
            let cc_instructions = body.str("cc_instructions", errors);
            Some(AttachmentHandlerConfig::Command {
                program: program?,
                args: args?,
                file_roles: file_roles?,
                timeout_secs: timeout_secs.unwrap_or_else(default_timeout_secs),
                cc_instructions,
            })
        }
        other => {
            errors.push(Diagnostic::at(
                format!(
                    "`{other}` is not an attachment handler type; expected {}",
                    or_list(HANDLER_TYPES.as_slice())
                ),
                at,
            ));
            None
        }
    };
    // A block whose type word is unknown leaves its body unread, so naming the
    // leftover keys would name every key it wrote.
    if built.is_some() {
        body.finish(errors);
    }
    built
}

/// The type words a `handler` block may name, one per `AttachmentHandlerConfig`
/// variant.
const HANDLER_TYPES: [&str; 1] = ["command"];

/// The two script lists a hook block states, each empty where it says nothing.
fn hook_scripts(block: &RHooks, errors: &mut Vec<Diagnostic>) -> (Vec<String>, Vec<String>) {
    let host = block
        .host
        .as_ref()
        .and_then(|value| expect_strings(value, "host", errors))
        .unwrap_or_default();
    let container = block
        .container
        .as_ref()
        .and_then(|value| expect_strings(value, "container", errors))
        .unwrap_or_default();
    (host, container)
}

/// An agent's subscriptions, split into the three raw families they land in.
#[derive(Default)]
struct Subscriptions {
    messaging: Vec<MessagingSubscriptionRaw>,
    webhook: Vec<AppWebhookSubscriptionRaw>,
    mqtt: Vec<AppMqttIngressSubscriptionRaw>,
}

/// One `subscribe` statement form, dispatched on the scheme of the address it
/// names: that is the grammar's own rule, and it is why there is one statement
/// rather than three.
fn subscriptions(
    resolved: &DslResolved,
    agent: &RAgent,
    errors: &mut Vec<Diagnostic>,
) -> Subscriptions {
    let mut out = Subscriptions::default();
    for sub in &agent.subs {
        let address = chan_address(resolved, &sub.chan);
        match address.split_once(':') {
            Some(("brenn" | "ephemeral", _)) => {
                out.messaging
                    .push(messaging_subscription(address, sub, errors));
            }
            Some(("webhook", endpoint)) => {
                let endpoint = endpoint.to_string();
                out.webhook
                    .push(webhook_subscription(endpoint, sub, errors));
            }
            Some(("mqtt", _)) => out.mqtt.push(mqtt_subscription(address, sub, errors)),
            // Derivation refuses an agent subscription on a confined channel —
            // there is no local delivery path to a conversation — and every
            // other scheme with it, so reaching here is a parity break between
            // the two crates.
            _ => panic!(
                "agent `{}` subscribes to `{address}`, which derivation does not admit",
                agent.slug.value()
            ),
        }
    }
    out
}

/// The full scheme-qualified address a statement or binding names.
fn chan_address(resolved: &DslResolved, chan: &RChanRef) -> String {
    match chan {
        RChanRef::Decl(id) => resolved.channels[id.0].address.value().clone(),
        RChanRef::Addr(address) => address.value().clone(),
        // A link has no address until boot places it; every caller filters one
        // out before asking.
        RChanRef::Link(_) => unreachable!("a link names no address"),
    }
}

// ---------------------------------------------------------------------------
// Statement tails
// ---------------------------------------------------------------------------
//
// A statement tail is a vocabulary whose keys are the union of the raw tail
// fields across the families the statement can lower into, so more than one
// family reads the same tail. One reader per tail vocabulary lowers every key
// once; each family then spreads the result into its own exhaustive struct
// literal, naming every raw field there. A key added to a tail vocabulary
// fails compilation in the reader, and a raw field added to a family fails it
// at that family's literal — both directions, each in one place.
//
// A key one family has no field for is refused at the family, not here: which
// family a tail turns out to be depends on the address it names.

/// The lowered values of a `subscribe` tail.
struct LoweredSubscribe {
    push_depth: Option<Depth>,
    retain_depth: Option<Depth>,
    noise: Option<NoiseLevel>,
    wake_min: Option<WakeMin>,
}

fn subscribe_tail(tail: &SubscribeTail, errors: &mut Vec<Diagnostic>) -> LoweredSubscribe {
    let SubscribeTail {
        push_depth,
        retain_depth,
        noise,
        wake_min,
    } = tail;
    LoweredSubscribe {
        push_depth: opt_depth(push_depth.as_ref(), "push_depth", errors),
        retain_depth: opt_depth(retain_depth.as_ref(), "retain_depth", errors),
        noise: opt_token(noise.as_ref(), "noise", errors),
        wake_min: opt_token(wake_min.as_ref(), "wake_min", errors),
    }
}

/// The lowered values of an `in` binding tail.
struct LoweredIn {
    push_depth: Option<Depth>,
    retain_depth: Option<Depth>,
    noise: Option<NoiseLevel>,
    wake_min: Option<WakeMin>,
    amplification: Option<f64>,
}

fn in_tail(tail: &InTail<RVal>, errors: &mut Vec<Diagnostic>) -> LoweredIn {
    let InTail {
        push_depth,
        retain_depth,
        noise,
        wake_min,
        amplification,
    } = tail;
    LoweredIn {
        push_depth: opt_depth(push_depth.as_ref(), "push_depth", errors),
        retain_depth: opt_depth(retain_depth.as_ref(), "retain_depth", errors),
        noise: opt_token(noise.as_ref(), "noise", errors),
        wake_min: opt_token(wake_min.as_ref(), "wake_min", errors),
        amplification: opt_flt(amplification.as_ref(), "amplification", errors),
    }
}

/// The lowered values of an `out` binding tail.
struct LoweredOut {
    urgency: Option<Urgency>,
    publish_per_activation: Option<f64>,
    publish_capacity: Option<f64>,
}

fn out_tail(tail: &OutTail<RVal>, errors: &mut Vec<Diagnostic>) -> LoweredOut {
    let OutTail {
        urgency,
        publish_per_activation,
        publish_capacity,
    } = tail;
    LoweredOut {
        urgency: opt_token(urgency.as_ref(), "urgency", errors),
        publish_per_activation: opt_flt(
            publish_per_activation.as_ref(),
            "publish_per_activation",
            errors,
        ),
        publish_capacity: opt_flt(publish_capacity.as_ref(), "publish_capacity", errors),
    }
}

/// The lowered values of an `io` binding tail: both halves of the port.
struct LoweredIo {
    push_depth: Option<Depth>,
    retain_depth: Option<Depth>,
    noise: Option<NoiseLevel>,
    urgency: Option<Urgency>,
    amplification: Option<f64>,
    publish_per_activation: Option<f64>,
    publish_capacity: Option<f64>,
}

fn io_tail(tail: &IoTail<RVal>, errors: &mut Vec<Diagnostic>) -> LoweredIo {
    let IoTail {
        push_depth,
        retain_depth,
        noise,
        urgency,
        amplification,
        publish_per_activation,
        publish_capacity,
    } = tail;
    LoweredIo {
        push_depth: opt_depth(push_depth.as_ref(), "push_depth", errors),
        retain_depth: opt_depth(retain_depth.as_ref(), "retain_depth", errors),
        noise: opt_token(noise.as_ref(), "noise", errors),
        urgency: opt_token(urgency.as_ref(), "urgency", errors),
        amplification: opt_flt(amplification.as_ref(), "amplification", errors),
        publish_per_activation: opt_flt(
            publish_per_activation.as_ref(),
            "publish_per_activation",
            errors,
        ),
        publish_capacity: opt_flt(publish_capacity.as_ref(), "publish_capacity", errors),
    }
}

const WEBHOOK_MASKED_KEY: &str = "noise";

/// The keys of a `subscribe` tail that this family's raw struct has a field
/// for, for the refusal a family-inappropriate key gets.
static WEBHOOK_SUBSCRIPTION_KEYS: LazyLock<Vec<&'static str>> = LazyLock::new(|| {
    SubscribeTail::KEYS
        .iter()
        .copied()
        .filter(|key| *key != WEBHOOK_MASKED_KEY)
        .collect()
});

fn messaging_subscription(
    channel: String,
    sub: &RSubscribe,
    errors: &mut Vec<Diagnostic>,
) -> MessagingSubscriptionRaw {
    let tail = subscribe_tail(&sub.tail, errors);
    MessagingSubscriptionRaw {
        channel,
        push_depth: tail.push_depth,
        retain_depth: tail.retain_depth,
        noise: tail.noise,
        wake_min: tail.wake_min,
    }
}

/// The endpoint is the bare name: the scheme said which family this is, and
/// that is how the raw config spells it.
///
/// A webhook subscription has no noise policy — the endpoint's traffic is not
/// a channel whose volume the agent tunes — so `noise`, which the union
/// vocabulary admits, is refused here.
fn webhook_subscription(
    endpoint: String,
    sub: &RSubscribe,
    errors: &mut Vec<Diagnostic>,
) -> AppWebhookSubscriptionRaw {
    // Masked out of the tail before it is lowered: one token earns one
    // diagnostic, and a spelling error in a key this family does not read is
    // not a second thing to fix.
    let mut masked = sub.tail.clone();
    refuse_word(
        masked.noise.take().as_ref(),
        WEBHOOK_MASKED_KEY,
        &format!("a subscription to `webhook:{endpoint}`"),
        &WEBHOOK_SUBSCRIPTION_KEYS,
        errors,
    );
    let tail = subscribe_tail(&masked, errors);
    AppWebhookSubscriptionRaw {
        endpoint,
        push_depth: tail.push_depth,
        retain_depth: tail.retain_depth,
        wake_min: tail.wake_min,
    }
}

/// The channel keeps its whole canonical `mqtt:<client>:<topic>` spelling: the
/// client segment is what scopes the topic, so the raw config carries both.
fn mqtt_subscription(
    channel: String,
    sub: &RSubscribe,
    errors: &mut Vec<Diagnostic>,
) -> AppMqttIngressSubscriptionRaw {
    let tail = subscribe_tail(&sub.tail, errors);
    AppMqttIngressSubscriptionRaw {
        channel,
        push_depth: tail.push_depth,
        retain_depth: tail.retain_depth,
        noise: tail.noise,
        wake_min: tail.wake_min,
    }
}

// ---------------------------------------------------------------------------
// WASM consumers
// ---------------------------------------------------------------------------

/// `[[wasm_consumer]]` per top-level component instance, with the authority
/// derivation computed for it.
fn consumers(
    derived: &DerivedConfig,
    endpoints: &mut LinkEndpoints,
    errors: &mut Vec<Diagnostic>,
) -> Vec<WasmConsumerConfigRaw> {
    let resolved = &derived.resolved;
    resolved
        .consumers
        .iter()
        .zip(&derived.consumers)
        .map(|(instance, authority)| consumer(resolved, instance, authority, endpoints, errors))
        .collect()
}

/// One `[[wasm_consumer]]`.
///
/// The key set the body reads is gated against the struct's fields by
/// `tests::dsl_key_parity`, and the nine ACL family assignments by
/// `tests::acl_family_parity`.
fn consumer(
    resolved: &DslResolved,
    instance: &RConsumer,
    authority: &DAuthority,
    endpoints: &mut LinkEndpoints,
    errors: &mut Vec<Diagnostic>,
) -> WasmConsumerConfigRaw {
    let label = instance.slug.value().clone();
    let mut body = Body::new(
        format!("consumer `{label}`"),
        instance.slug.span().clone(),
        &instance.attrs,
    );
    // Consume so `Body::finish` does not reject it; the slug is already
    // captured in `instance.slug`.
    body.take("slug");
    let bindings = wasm_bindings(resolved, instance, endpoints, errors);
    let raw = WasmConsumerConfigRaw {
        slug: label.clone(),
        // A class fact, not a body key: the packaged module the class was
        // declared in is the package the host resolves the artifact from.
        // Resolution refuses a top-level instance of a class no package
        // declares, so an absent package here is a resolve-vs-lowering parity
        // break and not a document state — die at the break rather than emit a
        // nameless package the host would blame the configuration for.
        package: instance
            .class
            .package
            .clone()
            .expect("a top-level consumer's class is declared in a packaged module"),
        // A class fact, not a body key: carried from the declaring file.
        spec_sha256: instance.class.spec_sha256.clone(),
        grants: authority
            .grants
            .iter()
            .map(|granted| grant::<ComponentGrant>(granted.value(), granted.span(), &label))
            .collect(),
        store_path: body.path("store_path", errors),
        store_size_limit: body.str("store_size_limit", errors),
        subscriptions: bindings.subscriptions,
        outputs: bindings.outputs,
        io_ports: bindings.io_ports,
        subscribe_acl: channel_matchers(&authority.acl.brenn_subscribe),
        ephemeral_subscribe_acl: channel_matchers(&authority.acl.ephemeral_subscribe),
        local_subscribe_acl: channel_matchers(&authority.acl.local_subscribe),
        publish_acl: channel_matchers(&authority.acl.brenn_publish),
        ephemeral_publish_acl: channel_matchers(&authority.acl.ephemeral_publish),
        local_publish_acl: channel_matchers(&authority.acl.local_publish),
        mqtt_publish_acl: mqtt_client_matchers(&authority.acl.mqtt_publish),
        mqtt_subscribe_acl: mqtt_sub_matchers(&authority.acl.mqtt_subscribe),
        webhook_acl: webhook_matchers(&authority.acl.webhook),
        config: body.config("config", errors),
        activation_burst: body.int("activation_burst", errors),
        activation_min_period_ms: body.int("activation_min_period_ms", errors),
        mqtt_outputs: mqtt_outputs(&authority.acl.mqtt_publish),
        tool_grants: tool_grants(&instance.tools),
    };
    body.finish(errors);
    raw
}

/// A consumer's bindings, split into the three raw families they land in.
#[derive(Default)]
struct WasmBindings {
    subscriptions: Vec<WasmConsumerSubscriptionRaw>,
    outputs: Vec<WasmConsumerOutputRaw>,
    io_ports: Vec<WasmConsumerIoPortRaw>,
}

/// One binding statement per port, dispatched on direction.
///
/// `in` and `out` bindings must carry a channel address; only a free `io`
/// port may name none. A tail is a vocabulary, so each key is a same-name
/// transcription; the one key a consumer holds and a surface does not —
/// `amplification` — is read here and refused there.
fn wasm_bindings(
    resolved: &DslResolved,
    instance: &RConsumer,
    endpoints: &mut LinkEndpoints,
    errors: &mut Vec<Diagnostic>,
) -> WasmBindings {
    let mut out = WasmBindings::default();
    for binding in &instance.bindings {
        let channel = binding
            .chan
            .as_ref()
            .filter(|chan| !matches!(chan, RChanRef::Link(_)))
            .map(|chan| chan_address(resolved, chan));
        let port = binding.port.value().clone();
        let owner = format!("consumer `{}`", instance.slug.value());
        match &binding.tail {
            RTail::In(tail) => {
                let tail = in_tail(tail, errors);
                link_endpoint(
                    endpoints,
                    binding.chan.as_ref(),
                    || LinkHostRaw::Wasm {
                        slug: instance.slug.value().clone(),
                    },
                    &port,
                    (false, true, false),
                    (tail.push_depth, tail.retain_depth),
                );
                out.subscriptions.push(WasmConsumerSubscriptionRaw {
                    channel: connected(binding.chan.as_ref(), channel, &port, &owner),
                    port,
                    push_depth: tail.push_depth,
                    retain_depth: tail.retain_depth,
                    noise: tail.noise,
                    wake_min: tail.wake_min,
                    amplification: tail.amplification,
                });
            }
            RTail::Out(tail) => {
                let tail = out_tail(tail, errors);
                link_endpoint(
                    endpoints,
                    binding.chan.as_ref(),
                    || LinkHostRaw::Wasm {
                        slug: instance.slug.value().clone(),
                    },
                    &port,
                    (true, false, false),
                    (None, None),
                );
                out.outputs.push(WasmConsumerOutputRaw {
                    channel: connected(binding.chan.as_ref(), channel, &port, &owner),
                    port,
                    urgency: tail.urgency,
                    publish_per_activation: tail.publish_per_activation,
                    publish_capacity: tail.publish_capacity,
                });
            }
            RTail::Io(tail) => {
                let tail = io_tail(tail, errors);
                link_endpoint(
                    endpoints,
                    binding.chan.as_ref(),
                    || LinkHostRaw::Wasm {
                        slug: instance.slug.value().clone(),
                    },
                    &port,
                    (true, true, true),
                    (tail.push_depth, tail.retain_depth),
                );
                match io_shape(binding.chan.as_ref(), channel) {
                    IoShape::Pair(address) => {
                        out.subscriptions.push(WasmConsumerSubscriptionRaw {
                            channel: Some(address.clone()),
                            port: port.clone(),
                            push_depth: tail.push_depth,
                            retain_depth: tail.retain_depth,
                            noise: tail.noise,
                            wake_min: None,
                            amplification: tail.amplification,
                        });
                        out.outputs.push(WasmConsumerOutputRaw {
                            channel: Some(address),
                            port,
                            urgency: tail.urgency,
                            publish_per_activation: tail.publish_per_activation,
                            publish_capacity: tail.publish_capacity,
                        });
                    }
                    IoShape::Port(channel) => {
                        out.io_ports.push(WasmConsumerIoPortRaw {
                            port,
                            channel,
                            push_depth: tail.push_depth,
                            retain_depth: tail.retain_depth,
                            noise: tail.noise,
                            amplification: tail.amplification,
                            urgency: tail.urgency,
                            publish_per_activation: tail.publish_per_activation,
                            publish_capacity: tail.publish_capacity,
                        });
                    }
                }
            }
        }
    }
    out
}

/// Every link endpoint one document states, keyed by the link's id.
///
/// Filled by the binding walks themselves: the arm that lowers a binding is the
/// arm that knows its roles and its window, so the endpoint is a second reading
/// of nothing.
type LinkEndpoints = BTreeMap<usize, Vec<LinkEndpointRaw>>;

/// Record the endpoint a link-bound binding presents, where it is one.
///
/// A window rides only on the subscribing half: an endpoint that only publishes
/// folds nothing into the ring's retention, and boot refuses one that carries a
/// window anyway.
fn link_endpoint(
    endpoints: &mut LinkEndpoints,
    chan: Option<&RChanRef>,
    host: impl FnOnce() -> LinkHostRaw,
    port: &str,
    roles: (bool, bool, bool),
    window: (Option<Depth>, Option<Depth>),
) {
    let Some(RChanRef::Link(id)) = chan else {
        return;
    };
    let (publishes, subscribes, io_port) = roles;
    let (push_depth, retain_depth) = match subscribes {
        true => window,
        false => (None, None),
    };
    endpoints.entry(id.0).or_default().push(LinkEndpointRaw {
        host: host(),
        port: port.to_owned(),
        publishes,
        subscribes,
        io_port,
        push_depth,
        retain_depth,
    });
}

/// `[[link]]` per declared link, with the ports bound to it as its endpoints.
///
/// Every endpoint the binding walks recorded belongs to a link this walk
/// claims: the ids they key on are positions in the same list. An endpoint left
/// over is a parity break between the resolver and lowering, and a dropped `io`
/// endpoint would boot as a free port on its own private ring rather than fail,
/// so the leftovers are asserted away.
fn links(resolved: &DslResolved, mut endpoints: LinkEndpoints) -> Vec<LinkConfigRaw> {
    let links: Vec<LinkConfigRaw> = resolved
        .links
        .iter()
        .enumerate()
        .map(|(index, link)| LinkConfigRaw {
            link: link.handle.dotted(),
            description: link.doc.as_ref().map(doc_text),
            endpoints: endpoints.remove(&index).unwrap_or_default(),
        })
        .collect();
    assert!(
        endpoints.is_empty(),
        "link ids {:?} carry endpoints no declared link claims",
        endpoints.keys().collect::<Vec<_>>()
    );
    links
}

/// A doc comment as one block of text.
///
/// The line's own leading space is trivia of the comment marker, not content;
/// a deeper indent inside a doc block is the author's and stays.
fn doc_text(doc: &DocComment) -> String {
    doc.lines
        .iter()
        .map(|line| line.value().strip_prefix(' ').unwrap_or(line.value()))
        .collect::<Vec<_>>()
        .join("\n")
}

/// What an `io` binding lowers to.
///
/// A declared channel becomes a subscription/output `Pair` on the declared
/// address. An absent or literal channel becomes a `Port`.
enum IoShape {
    Pair(String),
    Port(Option<String>),
}

/// Which of the two an `io` binding takes, from the reference it names and the
/// address that reference already resolved to.
fn io_shape(chan: Option<&RChanRef>, channel: Option<String>) -> IoShape {
    match (chan, channel) {
        (Some(RChanRef::Decl(_)), address) => IoShape::Pair(
            address.expect("a binding on a declared channel names that channel's address"),
        ),
        // A link-bound `io` port is a `Port` with no channel, exactly like a
        // free one: boot places the ring and re-joins the port to it.
        (None | Some(RChanRef::Addr(_) | RChanRef::Link(_)), channel) => IoShape::Port(channel),
    }
}

/// The address a connected binding names, or nothing where it names a link.
///
/// `in` and `out` bindings must name a channel or a link; `None` from a binding
/// that names neither is a parity break between the DSL and lowering crates,
/// not a state a document can produce. `owner` names the port's holder — a
/// consumer, or an instance on a surface.
fn connected(
    chan: Option<&RChanRef>,
    channel: Option<String>,
    port: &str,
    owner: &str,
) -> Option<String> {
    if matches!(chan, Some(RChanRef::Link(_))) {
        return None;
    }
    Some(channel.unwrap_or_else(|| {
        panic!("the `{port}` port of {owner} is connected in one direction and names no channel")
    }))
}

// ---------------------------------------------------------------------------
// Surfaces
// ---------------------------------------------------------------------------

/// `[[surface]]` per declared surface, with the authority derivation and the
/// wire kinds computed for it.
fn surfaces(
    derived: &DerivedConfig,
    endpoints: &mut LinkEndpoints,
    errors: &mut Vec<Diagnostic>,
) -> Vec<SurfaceConfigRaw> {
    let resolved = &derived.resolved;
    resolved
        .surfaces
        .iter()
        .zip(&derived.surfaces)
        .zip(&derived.surface_component_kinds)
        .zip(&derived.surface_components)
        .map(|(((surface, authority), kinds), placed)| {
            self::surface(
                resolved, surface, authority, kinds, placed, endpoints, errors,
            )
        })
        .collect()
}

/// One `[[surface]]`.
///
/// The scalar attrs transcribe by name; what is synthesized is the wire slug,
/// the grant tokens and the four ACL families from the derivation, and the
/// component instances with the bindings they hold.
///
/// The component tables are flat under the surface rather than nested inside a
/// component's own table, so a binding names the instance it belongs to. That
/// is the raw config's shape, and it is why lowering carries the instance name
/// down into each binding.
fn surface(
    resolved: &DslResolved,
    surface: &RSurface,
    authority: &DAuthority,
    kinds: &[String],
    placed: &[DAuthority],
    endpoints: &mut LinkEndpoints,
    errors: &mut Vec<Diagnostic>,
) -> SurfaceConfigRaw {
    // Destructured with no `..`: an attr added to `SurfaceAttrs` fails
    // compilation here. `slug` and `grants` are the skip list — both
    // are already resolved before lowering sees them and arrive via other
    // parameters.
    let SurfaceAttrs {
        slug: _slug,
        grants: _grants,
        skin,
        allowed_users,
        publish_burst,
        publish_per_sec,
    }: &SurfaceAttrs<RVal> = &surface.attrs;
    let label = surface.slug.value().clone();
    let (components, bindings) =
        surface_components(resolved, surface, kinds, placed, &label, endpoints, errors);
    // The raw struct has neither a local, an mqtt nor a webhook family: a
    // surface's `local:` frames are authorized by the page it is served to, and
    // it reaches neither a broker nor a webhook endpoint. Derivation holds the
    // same rule; a non-empty list here means the two crates disagree.
    let acl = &authority.acl;
    assert!(
        acl.local_subscribe.is_empty()
            && acl.local_publish.is_empty()
            && acl.mqtt_subscribe.is_empty()
            && acl.mqtt_publish.is_empty()
            && acl.webhook.is_empty(),
        "a surface has only the durable and ephemeral families, and `{label}` derived entries \
         outside them"
    );
    SurfaceConfigRaw {
        slug: label.clone(),
        grants: authority
            .grants
            .iter()
            .map(|granted| grant::<AttachGrant>(granted.value(), granted.span(), &label))
            .collect(),
        subscribe_acl: channel_matchers(&acl.brenn_subscribe),
        publish_acl: channel_matchers(&acl.brenn_publish),
        ephemeral_subscribe_acl: channel_matchers(&acl.ephemeral_subscribe),
        ephemeral_publish_acl: channel_matchers(&acl.ephemeral_publish),
        components,
        subscriptions: bindings.subscriptions,
        outputs: bindings.outputs,
        io_ports: bindings.io_ports,
        skin: opt_str(skin.as_ref(), "skin", errors),
        allowed_users: opt_strings(allowed_users.as_ref(), "allowed_users", errors)
            .unwrap_or_default(),
        publish_burst: opt_int(publish_burst.as_ref(), "publish_burst", errors),
        publish_per_sec: opt_int(publish_per_sec.as_ref(), "publish_per_sec", errors),
    }
}

/// A surface's bindings, split into the three raw families they land in.
#[derive(Default)]
struct SurfaceBindings {
    subscriptions: Vec<SurfaceSubscriptionRaw>,
    outputs: Vec<SurfaceOutputRaw>,
    io_ports: Vec<SurfaceIoPortRaw>,
}

/// `[[surface.component]]` per instance placed on the surface, and the bindings
/// those instances hold.
///
/// TODO(dsl-vocabulary-config-parity): the per-family key sets the two
/// `amplification` refusals in [`surface_bindings`] name are hand lists that no
/// reflected destructure reaches — the refusals themselves are gated, but the
/// key set each names in its message is not. (The key set the body below reads
/// is gated against `SurfaceComponentRaw`'s fields by `tests::dsl_key_parity`.)
fn surface_components(
    resolved: &DslResolved,
    surface: &RSurface,
    kinds: &[String],
    placed: &[DAuthority],
    label: &str,
    endpoints: &mut LinkEndpoints,
    errors: &mut Vec<Diagnostic>,
) -> (Vec<SurfaceComponentRaw>, SurfaceBindings) {
    let mut components = Vec::with_capacity(surface.components.len());
    let mut bindings = SurfaceBindings::default();
    for ((instance, kind), authority) in surface.components.iter().zip(kinds).zip(placed) {
        let name = instance.instance.value().clone();
        let owner = format!("instance `{name}` of surface `{label}`");
        let instance_label = format!("{label}#{name}");
        let mut body = Body::new(
            owner.clone(),
            instance.instance.span().clone(),
            &instance.attrs,
        );
        components.push(SurfaceComponentRaw {
            // The wire kind, not the class name — the class never reaches
            // the raw config.
            kind: kind.clone(),
            instance: Some(name.clone()),
            // A class fact, not a body key: carried from the declaring file.
            spec_sha256: instance.class.spec_sha256.clone(),
            send_burst: body.int("send_burst", errors),
            send_refill_secs: body.int("send_refill_secs", errors),
            // A depth is a count or a bare word, so it was projected out of
            // the body rather than resolved among its values.
            parked_batch_depth: opt_projected_depth(
                instance.parked_batch_depth.as_ref(),
                "parked_batch_depth",
                errors,
            ),
            chrome: body.bool("chrome", errors).unwrap_or_default(),
            config: body.string_map("config", errors),
            // The words this instance was given, in the runtime's own
            // spellings. A capability names no scheme, so derivation expands
            // nothing here and the list crosses as written.
            //
            // TODO(surface-instance-acl-bound): `authority` also carries this
            // instance's derived-or-explicit ACL families, and only `grants`
            // crosses. What the front end already refuses is a binding outside
            // the instance's own explicit statement; what nothing checks is the
            // instance's set against its surface's on the wire planes, because
            // the raw carrier has no field for it.
            grants: authority
                .grants
                .iter()
                .map(|granted| {
                    grant::<ComponentGrant>(granted.value(), granted.span(), &instance_label)
                })
                .collect(),
        });
        body.finish(errors);
        let at = Placement {
            surface: label,
            instance: &name,
            owner: &owner,
        };
        surface_bindings(resolved, instance, &at, &mut bindings, endpoints, errors);
    }
    (components, bindings)
}

/// Which instance of which surface a binding belongs to.
struct Placement<'a> {
    surface: &'a str,
    instance: &'a str,
    /// What a diagnostic about one of its bindings calls the pair.
    owner: &'a str,
}

/// One binding statement per port of one instance, dispatched on direction.
///
/// The surface twin of [`wasm_bindings`], with the instance name carried into
/// each entry: the raw tables live on the surface, not on the component. The
/// union key a surface port has no field for — `amplification`, a consumer's
/// throughput knob — is refused here, at its own value.
fn surface_bindings(
    resolved: &DslResolved,
    instance: &RComponentInst,
    at: &Placement<'_>,
    out: &mut SurfaceBindings,
    endpoints: &mut LinkEndpoints,
    errors: &mut Vec<Diagnostic>,
) {
    for binding in &instance.bindings {
        let channel = binding
            .chan
            .as_ref()
            .filter(|chan| !matches!(chan, RChanRef::Link(_)))
            .map(|chan| chan_address(resolved, chan));
        let port = binding.port.value().clone();
        let what = format!("the `{port}` port of {}", at.owner);
        match &binding.tail {
            RTail::In(tail) => {
                // Masked out before the tail is lowered, so one token earns
                // one diagnostic rather than a refusal and a value error.
                let mut masked = tail.clone();
                refuse_val(
                    masked.amplification.take().as_ref(),
                    "amplification",
                    &what,
                    &SURFACE_IN_KEYS,
                    errors,
                );
                let tail = in_tail(&masked, errors);
                link_endpoint(
                    endpoints,
                    binding.chan.as_ref(),
                    || LinkHostRaw::Surface {
                        slug: at.surface.to_owned(),
                        instance: at.instance.to_owned(),
                    },
                    &port,
                    (false, true, false),
                    (tail.push_depth, tail.retain_depth),
                );
                out.subscriptions.push(SurfaceSubscriptionRaw {
                    channel: connected(binding.chan.as_ref(), channel, &port, at.owner),
                    instance: at.instance.to_owned(),
                    port,
                    push_depth: tail.push_depth,
                    retain_depth: tail.retain_depth,
                    noise: tail.noise,
                    wake_min: tail.wake_min,
                });
            }
            RTail::Out(tail) => {
                let tail = out_tail(tail, errors);
                link_endpoint(
                    endpoints,
                    binding.chan.as_ref(),
                    || LinkHostRaw::Surface {
                        slug: at.surface.to_owned(),
                        instance: at.instance.to_owned(),
                    },
                    &port,
                    (true, false, false),
                    (None, None),
                );
                out.outputs.push(SurfaceOutputRaw {
                    instance: at.instance.to_owned(),
                    channel: connected(binding.chan.as_ref(), channel, &port, at.owner),
                    port,
                    urgency: tail.urgency,
                    publish_per_activation: tail.publish_per_activation,
                    publish_capacity: tail.publish_capacity,
                });
            }
            RTail::Io(tail) => {
                let mut masked = tail.clone();
                refuse_val(
                    masked.amplification.take().as_ref(),
                    "amplification",
                    &what,
                    &SURFACE_IO_KEYS,
                    errors,
                );
                let tail = io_tail(&masked, errors);
                link_endpoint(
                    endpoints,
                    binding.chan.as_ref(),
                    || LinkHostRaw::Surface {
                        slug: at.surface.to_owned(),
                        instance: at.instance.to_owned(),
                    },
                    &port,
                    (true, true, true),
                    (tail.push_depth, tail.retain_depth),
                );
                match io_shape(binding.chan.as_ref(), channel) {
                    IoShape::Pair(address) => {
                        out.subscriptions.push(SurfaceSubscriptionRaw {
                            channel: Some(address.clone()),
                            instance: at.instance.to_owned(),
                            port: port.clone(),
                            push_depth: tail.push_depth,
                            retain_depth: tail.retain_depth,
                            noise: tail.noise,
                            wake_min: None,
                        });
                        out.outputs.push(SurfaceOutputRaw {
                            instance: at.instance.to_owned(),
                            channel: Some(address),
                            port,
                            urgency: tail.urgency,
                            publish_per_activation: tail.publish_per_activation,
                            publish_capacity: tail.publish_capacity,
                        });
                    }
                    IoShape::Port(channel) => {
                        out.io_ports.push(SurfaceIoPortRaw {
                            instance: at.instance.to_owned(),
                            port,
                            channel,
                            push_depth: tail.push_depth,
                            retain_depth: tail.retain_depth,
                            noise: tail.noise,
                            urgency: tail.urgency,
                            publish_per_activation: tail.publish_per_activation,
                            publish_capacity: tail.publish_capacity,
                        });
                    }
                }
            }
        }
    }
}

/// The keys a surface `in` binding reads, for the refusal that says
/// `amplification` is not one of them.
const SURFACE_IN_KEYS: [&str; 4] = ["push_depth", "retain_depth", "noise", "wake_min"];

/// The keys a surface `io` binding reads.
const SURFACE_IO_KEYS: [&str; 6] = [
    "push_depth",
    "retain_depth",
    "noise",
    "urgency",
    "publish_per_activation",
    "publish_capacity",
];

// ---------------------------------------------------------------------------
// Remotes
// ---------------------------------------------------------------------------

/// `[[remote]]` per `remote` declaration, with the authority derived for it.
fn remotes(derived: &DerivedConfig, errors: &mut Vec<Diagnostic>) -> Vec<RemoteConfigRaw> {
    derived
        .resolved
        .remotes
        .iter()
        .zip(&derived.remotes)
        .map(|(remote, authority)| self::remote(remote, authority, errors))
        .collect()
}

/// One `[[remote]]`.
///
/// The subscribe families are the one place a matcher carries numbers: a network
/// peer holds no channel declaration to inherit a depth from, so each entry
/// states its own ceilings. The entries arrive as plain counts; unbounded is
/// not valid in this position.
fn remote(
    remote: &RRemote,
    authority: &DRemoteAuthority,
    errors: &mut Vec<Diagnostic>,
) -> RemoteConfigRaw {
    // Destructured with no `..`: an attr added to `RemoteAttrs` fails
    // compilation here. `grants` and `acl` are the skip list — both are already
    // resolved into the authority.
    let RemoteAttrs {
        token_file,
        grants: _grants,
        publish_burst,
        publish_per_sec,
        max_sessions,
        max_subscriptions,
    }: &RemoteAttrs<RVal> = &remote.attrs;
    let label = remote.slug.value().clone();
    RemoteConfigRaw {
        slug: label.clone(),
        token_file: keep(expect_path(&token_file.value, "token_file"), errors).unwrap_or_default(),
        grants: authority
            .grants
            .iter()
            .map(|granted| grant::<AttachGrant>(granted.value(), granted.span(), &label))
            .collect(),
        subscribe_acl: remote_ceilings(&authority.subscribe),
        ephemeral_subscribe_acl: remote_ceilings(&authority.ephemeral_subscribe),
        publish_acl: channel_matchers(&authority.publish),
        ephemeral_publish_acl: channel_matchers(&authority.ephemeral_publish),
        publish_burst: opt_int(publish_burst.as_ref(), "publish_burst", errors),
        publish_per_sec: opt_int(publish_per_sec.as_ref(), "publish_per_sec", errors),
        max_sessions: opt_int(max_sessions.as_ref(), "max_sessions", errors),
        max_subscriptions: opt_int(max_subscriptions.as_ref(), "max_subscriptions", errors),
    }
}

/// One subscribe-side family, each entry a matcher and the two depths it caps a
/// matching subscription at.
fn remote_ceilings(entries: &[DRemoteSubEntry]) -> Vec<RemoteSubscribeAclRaw> {
    entries
        .iter()
        .map(|entry| {
            // Which of the two the matcher is decides which field is set; the
            // raw struct takes exactly one of them.
            let (exact, prefix) = match &entry.m {
                DMatcher::Exact(pattern) => (Some(pattern.value().clone()), None),
                DMatcher::Prefix(pattern) => (None, Some(pattern.value().clone())),
            };
            RemoteSubscribeAclRaw {
                exact,
                prefix,
                push_depth: entry.push_depth,
                retain_depth: entry.retain_depth,
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Webhook endpoints
// ---------------------------------------------------------------------------

/// `[[webhook_endpoint]]` per `webhook` declaration.
///
/// A declaration stating no `signature` block contributes a diagnostic and no
/// entry: the raw struct's field is not optional, and there is nothing to
/// default it to — which signature scheme guards an endpoint is the whole point
/// of declaring one.
fn webhook_endpoints(
    list: &[RWebhook],
    errors: &mut Vec<Diagnostic>,
) -> Vec<WebhookEndpointConfigRaw> {
    list.iter()
        .filter_map(|endpoint| webhook_endpoint(endpoint, errors))
        .collect()
}

/// One `[[webhook_endpoint]]`, with the four block kindwords its body admits.
fn webhook_endpoint(
    endpoint: &RWebhook,
    errors: &mut Vec<Diagnostic>,
) -> Option<WebhookEndpointConfigRaw> {
    // Destructured with no `..`: an attr added to `WebhookAttrs` fails
    // compilation here. `slug` is the skip list — the handle-to-slug fallback
    // is already resolved by this point.
    let WebhookAttrs {
        slug: _slug,
        mount,
        description,
        transport_ceiling_bytes,
        content_type,
        urgency,
    }: &WebhookAttrs<RVal> = &endpoint.attrs;
    let label = endpoint.slug.value().clone();
    // Every attr is read before the first `?`: diagnostics accumulate, so a
    // mistyped attr is reported in the same pass as a block that will not build
    // rather than on the operator's next boot attempt.
    let mount = opt_str(mount.as_ref(), "mount", errors);
    let description = opt_str(description.as_ref(), "description", errors);
    let transport_ceiling_bytes = opt_int(
        transport_ceiling_bytes.as_ref(),
        "transport_ceiling_bytes",
        errors,
    )
    .unwrap_or_else(default_transport_ceiling);
    let content_type =
        opt_str(content_type.as_ref(), "content_type", errors).unwrap_or_else(default_content_type);
    let urgency = opt_token(urgency.as_ref(), "urgency", errors);
    let blocks = webhook_blocks(&label, endpoint.slug.span(), &endpoint.blocks, errors)?;
    Some(WebhookEndpointConfigRaw {
        slug: label,
        mount,
        description,
        transport_ceiling_bytes,
        content_type,
        signature: blocks.signature,
        keys: blocks.keys,
        tokens: blocks.tokens,
        replay_protection: blocks.replay_protection,
        urgency,
    })
}

/// What a webhook body's sub-blocks say, gathered by kindword.
struct WebhookBlocks {
    signature: WebhookSignatureConfigRaw,
    keys: Vec<WebhookKeyConfigRaw>,
    tokens: Vec<WebhookTokenConfigRaw>,
    replay_protection: Option<ReplayProtectionConfigRaw>,
}

/// The blocks of one webhook body.
///
/// `None` when the body states no signature scheme, or when the scheme it states
/// cannot be built — the endpoint is then dropped and the diagnostics stand.
fn webhook_blocks(
    label: &str,
    at: &Span,
    blocks: &[RWebhookBlock],
    errors: &mut Vec<Diagnostic>,
) -> Option<WebhookBlocks> {
    let mut signature = None;
    // Whether a `signature` block was written at all, which is a different
    // report from one written and unbuildable.
    let mut stated_signature = false;
    let mut keys = Vec::new();
    let mut tokens = Vec::new();
    let mut replay_protection = None;
    // A `key` and a `token` are lists, so only the two unnamed blocks are
    // at-most-once; repeated credential ids are caught at boot.
    let mut seen = FirstWins::default();
    for block in blocks {
        let kindword = block.kindword.value().as_str();
        let named = |what: &str| match &block.name {
            Some(name) => format!("`{what} {}` of webhook `{label}`", name.value()),
            None => format!("`{what}` of webhook `{label}`"),
        };
        let mut body = Body::new(named(kindword), block.kindword.span().clone(), &block.attrs);
        // A block that could not be built leaves its body unread past the
        // refusal, so `finish` runs only where every reader did: naming the keys
        // that are left over is only the truth when nothing else went wrong.
        let read = match kindword {
            "signature" => {
                stated_signature = true;
                if seen.admit(
                    kindword.to_string(),
                    block.kindword.span(),
                    &format!("webhook `{label}`"),
                    errors,
                ) {
                    signature = self::signature(&mut body, errors);
                    signature.is_some()
                } else {
                    false
                }
            }
            "key" => match secret_file(&mut body, block, errors) {
                Some((key_id, secret_file)) => {
                    keys.push(WebhookKeyConfigRaw {
                        key_id,
                        secret_file,
                    });
                    true
                }
                None => false,
            },
            "token" => match secret_file(&mut body, block, errors) {
                Some((token_id, secret_file)) => {
                    tokens.push(WebhookTokenConfigRaw {
                        token_id,
                        secret_file,
                    });
                    true
                }
                None => false,
            },
            "replay_protection" => {
                if seen.admit(
                    kindword.to_string(),
                    block.kindword.span(),
                    &format!("webhook `{label}`"),
                    errors,
                ) {
                    replay_protection = replay(&mut body, errors);
                    replay_protection.is_some()
                } else {
                    false
                }
            }
            other => panic!("`{other}` is not a webhook block kindword"),
        };
        if read {
            body.finish(errors);
        }
    }
    let signature = match signature {
        Some(signature) => signature,
        // A block that was written and refused already reported why.
        None if stated_signature => return None,
        None => {
            errors.push(Diagnostic::at(
                format!(
                    "webhook `{label}` states no `signature` block: which scheme guards an \
                     endpoint has no default"
                ),
                at.clone(),
            ));
            return None;
        }
    };
    Some(WebhookBlocks {
        signature,
        keys,
        tokens,
        replay_protection,
    })
}

/// The `signature` block, as the internally tagged variant its scheme word
/// names.
///
/// Hand-dispatched rather than fed through the token seam: a `StrDeserializer`
/// builds unit variants only, and each of these carries fields. The reader set
/// per arm is what makes a stated attr the chosen variant has no field for a
/// refusal — [`Body::finish`] names exactly the keys that arm asked for.
///
/// TODO(dsl-vocabulary-config-parity): the scheme words below and each arm's
/// field set are a hand transcription of `WebhookSignatureConfigRaw`'s
/// variants. Nothing holds the two in step; the words here are the only
/// spelling left.
fn signature(body: &mut Body, errors: &mut Vec<Diagnostic>) -> Option<WebhookSignatureConfigRaw> {
    let (scheme, at) = body.required_spanned_str("scheme", errors)?;
    let algorithm = |body: &mut Body, errors: &mut Vec<Diagnostic>| {
        body.str("algorithm", errors)
            .unwrap_or_else(default_hmac_algorithm)
    };
    // Every reader runs before the first `?`: a block missing two required
    // fields is refused for both, and a key the arm does not read is refused by
    // `Body::finish` naming exactly the keys it does.
    match scheme.as_str() {
        "hmac-raw-body" => {
            let algorithm = algorithm(body, errors);
            let header = body.required_str("header", errors);
            let format = body.required_str("format", errors);
            let key_id_header = body.str("key_id_header", errors);
            Some(WebhookSignatureConfigRaw::HmacRawBody {
                algorithm,
                header: header?,
                format: format?,
                key_id_header,
            })
        }
        "hmac-timestamped-body" => {
            let algorithm = algorithm(body, errors);
            let sig_header = body.required_str("sig_header", errors);
            let sig_format = body.required_str("sig_format", errors);
            let timestamp_header = body.required_str("timestamp_header", errors);
            let template = body.required_str("template", errors);
            let max_skew_secs = body.required_int("max_skew_secs", errors);
            let key_id_header = body.str("key_id_header", errors);
            Some(WebhookSignatureConfigRaw::HmacTimestampedBody {
                algorithm,
                sig_header: sig_header?,
                sig_format: sig_format?,
                timestamp_header: timestamp_header?,
                template: template?,
                max_skew_secs: max_skew_secs?,
                key_id_header,
            })
        }
        "hmac-stripe" => {
            let algorithm = algorithm(body, errors);
            let header = body.required_str("header", errors);
            let max_skew_secs = body.required_int("max_skew_secs", errors);
            let key_id_header = body.str("key_id_header", errors);
            Some(WebhookSignatureConfigRaw::HmacStripe {
                algorithm,
                header: header?,
                max_skew_secs: max_skew_secs?,
                key_id_header,
            })
        }
        "bearer-token" => {
            let header = body.required_str("header", errors);
            let token_id_header = body.str("token_id_header", errors);
            Some(WebhookSignatureConfigRaw::BearerToken {
                header: header?,
                token_id_header,
            })
        }
        other => {
            errors.push(Diagnostic::at(
                format!(
                    "`{other}` is not a signature scheme; expected {}",
                    or_list(SIGNATURE_SCHEMES.as_slice())
                ),
                at,
            ));
            None
        }
    }
}

/// The scheme words a `signature` block may name — the raw enum's own kebab-case
/// tags.
const SIGNATURE_SCHEMES: [&str; 4] = [
    "hmac-raw-body",
    "hmac-timestamped-body",
    "hmac-stripe",
    "bearer-token",
];

/// A `key <id>` or `token <id>` block: the id is the block's name, the secret is
/// its one attr.
fn secret_file(
    body: &mut Body,
    block: &RWebhookBlock,
    errors: &mut Vec<Diagnostic>,
) -> Option<(String, PathBuf)> {
    let id = block
        .name
        .as_ref()
        .expect("a credential block carries a name")
        .value()
        .clone();
    Some((id, body.required_path("secret_file", errors)?))
}

/// The `replay_protection` block. Its `config` map is one of the two positions a
/// raw field literally stores a `toml::Table` in.
fn replay(body: &mut Body, errors: &mut Vec<Diagnostic>) -> Option<ReplayProtectionConfigRaw> {
    let component = body.required_str("component", errors);
    let store_path = body.required_path("store_path", errors);
    let store_size_limit = body.str("store_size_limit", errors);
    let config = body.config("config", errors);
    Some(ReplayProtectionConfigRaw {
        component: component?,
        store_path: store_path?,
        store_size_limit,
        config,
    })
}

// ---------------------------------------------------------------------------
// Authority
// ---------------------------------------------------------------------------

/// One capability token, through the runtime enum's own `Deserialize`.
///
/// A panic rather than a diagnostic: derivation emits these in the spellings the
/// runtime's config uses, so a token the enum refuses means the two crates'
/// spelling tables have drifted — a parity break, not something a `.brenn`
/// document can cause.
fn grant<T: DeserializeOwned>(text: &str, span: &Span, label: &str) -> T {
    token_text(text, span, "grants").unwrap_or_else(|error| {
        panic!(
            "derivation emits `{label}`'s grants in the runtime's own spellings, \
             and `{text}` is not one of them: {}",
            error.message
        )
    })
}

/// The `[app.acl]` block, family for family from the derivation.
///
/// The `DAclSet` field names were chosen to match this struct's keys, so each
/// line is a same-name transcription of a matcher list.
fn app_acl(acl: &DAclSet, label: &str) -> AppAclRaw {
    // The raw struct has no `local_subscribe` field by design — the grant has
    // no LLM-app path — and derivation derives no agent entry for it, so a
    // non-empty list here is a parity break between the two crates.
    assert!(
        acl.local_subscribe.is_empty(),
        "an agent has no `local_subscribe` family, and `{label}` derived {} entries for it",
        acl.local_subscribe.len()
    );
    AppAclRaw {
        mqtt_subscribe: mqtt_sub_matchers(&acl.mqtt_subscribe),
        mqtt_publish: mqtt_client_matchers(&acl.mqtt_publish),
        brenn_subscribe: channel_matchers(&acl.brenn_subscribe),
        brenn_publish: channel_matchers(&acl.brenn_publish),
        ephemeral_publish: channel_matchers(&acl.ephemeral_publish),
        ephemeral_subscribe: channel_matchers(&acl.ephemeral_subscribe),
        local_publish: channel_matchers(&acl.local_publish),
        webhook: webhook_matchers(&acl.webhook),
    }
}

/// Inbound MQTT matchers: each entry carries both a client and a topic filter.
fn mqtt_sub_matchers(entries: &[DMqttSub]) -> Vec<MqttSubMatcherRaw> {
    entries
        .iter()
        .map(|entry| MqttSubMatcherRaw {
            client: entry.client.value().clone(),
            topic_filter: entry.topic_filter.value().clone(),
        })
        .collect()
}

/// Outbound MQTT matchers. Publish is client-scoped only.
fn mqtt_client_matchers(entries: &[DMqttClient]) -> Vec<MqttClientMatcherRaw> {
    entries
        .iter()
        .map(|entry| MqttClientMatcherRaw {
            client: entry.client.value().clone(),
        })
        .collect()
}

/// The per-client egress budgets, one per outbound MQTT entry that carried one.
///
/// The entry that authorizes a client is what mints its sink, so the override
/// rides that entry rather than a table of its own; an entry with no budget
/// leaves the sink on the runtime's default and produces no block here.
fn mqtt_outputs(entries: &[DMqttClient]) -> Vec<WasmConsumerMqttOutputRaw> {
    entries
        .iter()
        .filter(|entry| entry.publish_per_activation.is_some() || entry.publish_capacity.is_some())
        .map(|entry| WasmConsumerMqttOutputRaw {
            client: entry.client.value().clone(),
            publish_per_activation: entry.publish_per_activation,
            publish_capacity: entry.publish_capacity,
        })
        .collect()
}

/// Webhook matchers, by endpoint slug.
fn webhook_matchers(entries: &[DWebhook]) -> Vec<WebhookMatcherRaw> {
    entries
        .iter()
        .map(|entry| WebhookMatcherRaw {
            endpoint: entry.endpoint.value().clone(),
        })
        .collect()
}

/// One channel-family list. The patterns are already scheme-stripped: the list
/// they land in is what says which scheme they are about.
fn channel_matchers(entries: &[DMatcher]) -> Vec<ChannelMatcherRaw> {
    entries
        .iter()
        .map(|entry| match entry {
            DMatcher::Exact(pattern) => ChannelMatcherRaw::Exact(pattern.value().clone()),
            DMatcher::Prefix(pattern) => ChannelMatcherRaw::Prefix(pattern.value().clone()),
        })
        .collect()
}
