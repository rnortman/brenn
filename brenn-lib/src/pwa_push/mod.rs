pub mod config;
pub mod db;
pub mod endpoint_validator;
pub mod payload;
pub mod publish;
pub mod targets;
pub mod vapid;

#[cfg(test)]
pub(crate) mod test_helpers;

pub use publish::{PwaPushSender, PwaPushService};

/// Return the first 16 chars of an endpoint URL for log preview.
///
/// Used in tracing fields and debug impls across `pwa_push` — the 16-char
/// constant is the single source of truth; change here to apply everywhere.
pub fn endpoint_preview(endpoint: &str) -> String {
    endpoint.chars().take(16).collect()
}
