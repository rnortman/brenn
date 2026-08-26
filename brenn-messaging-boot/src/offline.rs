//! Resolve the config-pure slice of the messaging layer, with no environment.
//!
//! [`build_messaging`](super::build_messaging) is the boot-time lowering, and
//! most of what it refuses is refused by passes that read nothing but the
//! configuration. This module runs exactly those passes, so a config-checking
//! tool on a workstation reaches the same verdict the service reaches on the
//! target host, for the gates that do not need the host.

use brenn_lib::config::BrennConfig;

use crate::{finish_surface_policies, lower_channel_topology, resolve_surfaces};

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
/// - The per-instance import⊆grants assert (`brenn_surface_server`'s
///   `validate_surface_assets`): it reads the built `.wasm` component trees,
///   which a config checker does not have.
/// - `load_remote_token`, and only it: a `[[remote]]`'s bearer token is read off
///   the deployment host's disk, mode bits and all. Every other `[[remote]]`
///   gate is a fact about the document and runs here through
///   [`check_remotes`](brenn_lib::messaging::remote::check_remotes).
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
/// # Panics
///
/// On any refusal from the passes it runs.
// TODO(config-check-offline-residue): extend this to wasm-consumer resolution
// once mqtt-client resolution separates client identity from its secret reads.
pub fn resolve_messaging_offline(config: &BrennConfig) {
    let topology = lower_channel_topology(config, Vec::new());
    let mut resolved_surfaces = resolve_surfaces(
        &config.surfaces,
        &topology.pre_directory(),
        &config.messaging,
        &topology.auto_wiring,
    );
    brenn_lib::messaging::remote::check_remotes(&config.remotes, &config.messaging);
    finish_surface_policies(&mut resolved_surfaces, config);
}
