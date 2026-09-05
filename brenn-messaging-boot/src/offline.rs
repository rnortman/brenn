//! Resolve the config-pure slice of the messaging layer, with no environment.
//!
//! [`build_messaging`](super::build_messaging) is the boot-time lowering, and
//! most of what it refuses is refused by the planner, which reads nothing but
//! the configuration. This module runs the planner and throws its plan away, so
//! a config-checking tool on a workstation reaches the same verdict the service
//! reaches on the target host, for the gates that do not need the host.

use brenn_lib::config::BrennConfig;

use crate::{PlanInputs, plan_messaging};

/// Plan the messaging layer from `config` alone, and report what the planner
/// found worth saying that is not a refusal.
///
/// The value is the asserts firing: every pass panics on a refusal, in the same
/// words the service prints as it dies, so a caller that wants a report rather
/// than a death catches the unwind.
///
/// # The boundary rule
///
/// The planner reads only [`BrennConfig`] and the values a document determines,
/// so the whole of it runs here. What this pass therefore cannot see is exactly
/// what the planner is not handed: no resolved apps, no tool registry, no
/// replay store paths, and no environment anywhere.
///
/// - `validate_and_resolve` and everything downstream of it: container and
///   working-dir stats, the XDG runtime dir, the integration registry, webhook
///   endpoint resolution beyond the channel entry the planner mints from the raw
///   block (slug charset, duplicate slug and mount, ownership, signature
///   scheme, secrets, replay protection), per-app webhook and mqtt subscription
///   stamping, and mqtt client secrets (`password_file` / `ca_file`).
/// - The async tool substrate: with no registry there are no request channels,
///   no result inboxes, no executor participant and no derived async grants, so
///   the `brenn:tools/` and `brenn:tool-results/` arms of the exact-tuning
///   cross-check have nothing to check against and per-consumer
///   `validate_grants` does not run.
/// - The description single-writer sweep, which reads the resolved app
///   policies.
/// - `assert_unique_store_paths` against the replay endpoints' stores, of which
///   this pass is handed none. Consumer stores are still held unique against
///   each other.
/// - The per-instance import⊆grants assert and the per-instance specification
///   binding (`brenn_surface_server`'s `validate_surface_assets`): both read the
///   built surface asset tree — the component trees and the binding records that
///   state which specification each kind's artifacts were built against — which
///   a config checker does not have.
/// - `load_remote_token`, and only it: a `[[remote]]`'s bearer token is read off
///   the deployment host's disk, mode bits and all. Every other `[[remote]]`
///   gate is a fact about the document and runs here.
///
/// Each is a *missed* refusal, never a false one: boot runs the same planner
/// with those inputs supplied and stays authoritative.
///
/// # Advisories
///
/// The return is what the planner found worth saying that is not a refusal, in
/// the words boot would have logged. A caller with no `tracing` subscriber —
/// the config-check tool — prints it beside its verdict.
///
/// # Panics
///
/// On any refusal the planner makes.
// TODO(config-check-offline-residue): the residue above is what a config check
// still cannot answer. Closing the webhook-endpoint half wants endpoint
// resolution split the way the mqtt client resolution was.
pub fn resolve_messaging_offline(
    config: &BrennConfig,
) -> Option<brenn_surface_server::SurfaceErrorAdvisory> {
    let plan = plan_messaging(&PlanInputs {
        config,
        apps: None,
        mqtt_clients: &brenn_lib::mqtt::config::resolve_client_identities(&config.mqtt_clients),
        tool_registry: None,
        replay_store_paths: &[],
    });
    plan.and_then(|plan| plan.surface_error_advisory)
}

#[cfg(test)]
mod tests {
    use brenn_lib::access::raw::ChannelMatcherRaw;
    use brenn_lib::config::BrennConfig;
    use brenn_lib::messaging::AttachGrant;
    use brenn_lib::messaging::config::Depth;
    use brenn_lib::messaging::remote::RemoteConfigRaw;

    use super::resolve_messaging_offline;
    use crate::test_fixtures::durable_channel;

    /// The pass runs the description set validator: a document that activates
    /// messaging owes boot `brenn:surface.index`, and the refusal is made here
    /// rather than at a service start.
    #[test]
    #[should_panic(expected = "brenn:surface.index")]
    fn a_messaging_document_without_the_index_is_refused() {
        let mut config = BrennConfig::default();
        config
            .channels
            .push(durable_channel("brenn:alerts", Depth::Bounded(1)));
        resolve_messaging_offline(&config);
    }

    /// The same document with the index declared passes, so the refusal above is
    /// about the missing declaration and not about running the validator at all.
    #[test]
    fn the_index_declaration_is_the_whole_of_what_that_document_owes() {
        let mut config = BrennConfig::default();
        config
            .channels
            .push(durable_channel("brenn:alerts", Depth::Bounded(1)));
        config
            .channels
            .push(durable_channel("brenn:surface.index", Depth::Bounded(1)));
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

    /// And a consumer is another: the document has activated messaging, so the
    /// description set is required here.
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
}
