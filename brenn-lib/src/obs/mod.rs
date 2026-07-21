pub mod alerting;
pub mod config;
pub mod layers;
pub mod panic_hook;
pub mod security;
pub mod transcript;

pub use config::ObsConfig;
pub use layers::{ObsGuard, init};
pub use panic_hook::install_pending_panic_hook;
pub use security::{
    SecurityEventType, log_and_alert_security_event, log_app_security_event, log_security_event,
};

/// Build the instance-level `AlertContext` from `ObsConfig`.
///
/// Always adds `Host` (via `gethostname`). Adds `Instance` when
/// `config.instance_name` is set. Field order is the canonical convention:
/// Host first, Instance second. Both the pending panic hook and the full hook
/// call this helper so their alert bodies are byte-identical for equal inputs.
pub(crate) fn build_alert_context(config: &config::ObsConfig) -> alerting::AlertContext {
    let hostname = gethostname::gethostname().to_string_lossy().into_owned();
    let ctx = alerting::AlertContext::default().with_field("Host", &hostname);
    if let Some(ref name) = config.instance_name {
        ctx.with_field("Instance", name)
    } else {
        ctx
    }
}
