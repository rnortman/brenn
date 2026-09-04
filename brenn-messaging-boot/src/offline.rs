//! Resolve the config-pure slice of the messaging layer, with no environment.
//!
//! [`build_messaging`](super::build_messaging) is the boot-time lowering, and
//! most of what it refuses is refused by passes that read nothing but the
//! configuration. This module runs exactly those passes, so a config-checking
//! tool on a workstation reaches the same verdict the service reaches on the
//! target host, for the gates that do not need the host.

use brenn_lib::config::BrennConfig;

use crate::{
    finish_surface_policies, lower_channel_topology, messaging_configured_by_document,
    resolve_surfaces,
};

/// Run every messaging resolution pass that reads only `config`.
///
/// The value is the asserts firing: each pass panics on a refusal, in the same
/// words the service prints as it dies, so a caller that wants a report rather
/// than a death catches the unwind.
///
/// # The boundary rule
///
/// Every step here reads only [`BrennConfig`]. Anything that stats a path, reads
/// a secret, or touches the DB is excluded, and the exclusions are named:
///
/// - `validate_and_resolve` and everything downstream of it: container and
///   working-dir stats, the XDG runtime dir, the integration registry, webhook
///   endpoint resolution (which mutates the resolved-app registry), and mqtt
///   client resolution (which reads `password_file` / `ca_file`).
/// - [`resolve_wasm_consumers`](crate::resolve_wasm_consumers): it takes the
///   resolved mqtt-client map, so it is environment-coupled today. Consumer
///   gates stay boot-only.
/// - The per-instance import⊆grants assert and the per-instance specification
///   binding (`brenn_surface_server`'s `validate_surface_assets`): both read the
///   built surface asset tree — the component trees and the binding records that
///   state which specification each kind's artifacts were built against — which
///   a config checker does not have.
/// - `load_remote_token`, and only it: a `[[remote]]`'s bearer token is read off
///   the deployment host's disk, mode bits and all. Every other `[[remote]]`
///   gate is a fact about the document and runs here through
///   [`check_remotes`](brenn_lib::messaging::remote::check_remotes).
///
/// # Validator ordering
///
/// [`validate_surface_error_channel`](brenn_surface_server::validate_surface_error_channel)
/// runs before
/// [`validate_surface_description_set`](brenn_surface_server::description::validate_surface_description_set),
/// matching boot's order. A caller that reports rather than dies catches one
/// unwind and prints one message: a document both passes refuse must be
/// refused for the same reason in both places.
///
/// # This is a subset certifier
///
/// The directory built here holds the `[[channel]]` entries and the auto
/// channels lowered from `link` declarations, and nothing else: `webhook:`
/// and `mqtt:` entries are environment-derived. No surface binding can name
/// either scheme, so no surface gate can miss them — but an address collision
/// between a declared channel and a webhook or mqtt channel escapes this pass.
/// Boot still catches it, and boot stays authoritative.
///
/// The directory reaches the two validators only when
/// [`messaging_configured_by_document`] holds — the document half of the
/// predicate boot uses to decide whether a `Messenger`, and so a directory,
/// exists at all. That keeps the subset property: a document boot would hand
/// `None` is never validated here against a directory boot never built.
///
/// # Advisories
///
/// The return is what the passes found worth saying that is not a refusal, in
/// the words they would have logged at boot. A caller with no `tracing`
/// subscriber — the config-check tool — prints them beside its verdict.
///
/// # Panics
///
/// On any refusal from the passes it runs.
// TODO(config-check-offline-residue): extend this to wasm-consumer resolution
// once mqtt-client resolution separates client identity from its secret reads.
pub fn resolve_messaging_offline(config: &BrennConfig) -> Vec<String> {
    let topology = lower_channel_topology(config, Vec::new());
    let pre_directory = topology.pre_directory();
    let mut resolved_surfaces = resolve_surfaces(
        &config.surfaces,
        &pre_directory,
        &config.messaging,
        &topology.auto_wiring,
    );
    brenn_lib::messaging::remote::check_remotes(&config.remotes, &config.messaging);
    finish_surface_policies(&mut resolved_surfaces, config);
    let directory = messaging_configured_by_document(config).then_some(&pre_directory);
    let advisories = brenn_surface_server::validate_surface_error_channel(
        config.observability.surface_error_channel.as_deref(),
        directory,
        config.messaging.max_body_bytes,
    )
    .map(|advisory| advisory.to_string())
    .into_iter()
    .collect();
    brenn_surface_server::description::validate_surface_description_set(
        &config.surface_description,
        &resolved_surfaces,
        directory,
    );
    advisories
}

#[cfg(test)]
mod tests {
    use indexmap::IndexMap;

    use brenn_lib::access::raw::ChannelMatcherRaw;
    use brenn_lib::config::{AppConfigRaw, BrennConfig};
    use brenn_lib::messaging::AttachGrant;
    use brenn_lib::messaging::config::{ChannelConfigRaw, Depth};
    use brenn_lib::messaging::remote::RemoteConfigRaw;

    use super::resolve_messaging_offline;
    use crate::{messaging_configured, messaging_configured_by_document};

    /// One declared durable channel, which is all it takes to activate
    /// messaging.
    fn durable_channel(address: &str) -> ChannelConfigRaw {
        ChannelConfigRaw {
            send_rate: None,
            uuid: Some(uuid::Uuid::new_v4().to_string()),
            address: Some(address.to_string()),
            address_prefix: None,
            description: None,
            push_depth: Some(Depth::Bounded(1)),
            retain_depth: Some(Depth::Bounded(1)),
            standing_retain_depth: Some(Depth::Bounded(1)),
            noise: None,
            sink: None,
            wake_min: None,
        }
    }

    /// The pass runs the description set validator: a document that activates
    /// messaging owes boot `brenn:surface.index`, and the refusal is made here
    /// rather than at a service start.
    #[test]
    #[should_panic(expected = "brenn:surface.index")]
    fn a_messaging_document_without_the_index_is_refused() {
        let mut config = BrennConfig::default();
        config.channels.push(durable_channel("brenn:alerts"));
        resolve_messaging_offline(&config);
    }

    /// The same document with the index declared passes, so the refusal above is
    /// about the missing declaration and not about running the validator at all.
    #[test]
    fn the_index_declaration_is_the_whole_of_what_that_document_owes() {
        let mut config = BrennConfig::default();
        config.channels.push(durable_channel("brenn:alerts"));
        config.channels.push(durable_channel("brenn:surface.index"));
        resolve_messaging_offline(&config);
    }

    /// The `None` arm: a document that activates no messaging is handed no
    /// directory, so the validator has no derived channel to require.
    #[test]
    fn a_document_with_no_messaging_is_handed_no_directory() {
        resolve_messaging_offline(&BrennConfig::default());
    }

    /// A remote is a disjunct of its own: it attaches to the bus, so the
    /// document has activated messaging and owes boot the index, with no
    /// `[[channel]]` anywhere in it.
    #[test]
    #[should_panic(expected = "brenn:surface.index")]
    fn a_remote_only_document_owes_the_index_too() {
        let mut config = BrennConfig::default();
        config.remotes.push(remote("pod"));
        resolve_messaging_offline(&config);
    }

    /// And a consumer is another. Its own resolution is boot-only
    /// (`TODO(config-check-offline-residue)`), but the document has still
    /// activated messaging, so the description set is required here.
    #[test]
    #[should_panic(expected = "brenn:surface.index")]
    fn a_consumer_only_document_owes_the_index_too() {
        let mut config = BrennConfig::default();
        config
            .wasm_consumers
            .push(crate::test_fixtures::minimal_wasm_consumer());
        resolve_messaging_offline(&config);
    }

    /// A remote that names a token file it never reads here: the file is an
    /// environment fact and this pass excludes it by name.
    fn remote(slug: &str) -> RemoteConfigRaw {
        RemoteConfigRaw {
            slug: slug.to_string(),
            token_file: std::path::PathBuf::from("/nonexistent/remote.token"),
            grants: vec![AttachGrant::Publish],
            subscribe_acl: vec![],
            ephemeral_subscribe_acl: vec![],
            publish_acl: vec![ChannelMatcherRaw::Prefix("brenn:reports.".to_string())],
            ephemeral_publish_acl: vec![],
            publish_burst: None,
            publish_per_sec: None,
            max_sessions: None,
            max_subscriptions: None,
        }
    }

    /// The subset property this pass rests on, stated mechanically: with no
    /// environment to supply, boot's predicate and the document half are the
    /// same answer. A disjunct about the document added to
    /// [`messaging_configured`] instead of to
    /// [`messaging_configured_by_document`] would un-wire this pass for that
    /// class of document — it would pass the gate and die at the boot the
    /// installer already stopped the service for — and nothing else compares
    /// the two.
    #[test]
    fn the_document_predicate_is_boots_predicate_with_no_environment() {
        let mut channel_only = BrennConfig::default();
        channel_only.channels.push(durable_channel("brenn:alerts"));
        let mut consumer_only = BrennConfig::default();
        consumer_only
            .wasm_consumers
            .push(crate::test_fixtures::minimal_wasm_consumer());
        let mut surface_only = BrennConfig::default();
        surface_only
            .surfaces
            .push(crate::test_fixtures::minimal_surface_raw());
        let mut remote_only = BrennConfig::default();
        remote_only.remotes.push(remote("pod"));
        let mut app_only = BrennConfig::default();
        app_only.apps.push(AppConfigRaw::default());

        for (name, config) in [
            ("channel-only", &channel_only),
            ("consumer-only", &consumer_only),
            ("surface-only", &surface_only),
            ("remote-only", &remote_only),
            ("app-only", &app_only),
            ("empty", &BrennConfig::default()),
        ] {
            assert_eq!(
                messaging_configured(config, &IndexMap::new(), &[]),
                messaging_configured_by_document(config),
                "{name}: the two predicates disagree with no environment supplied",
            );
        }
        // Both directions are exercised above only if the table holds a case of
        // each, so say so rather than trust the reading.
        assert!(messaging_configured_by_document(&channel_only));
        assert!(!messaging_configured_by_document(&BrennConfig::default()));
    }
}
