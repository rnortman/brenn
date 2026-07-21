pub use std::net::SocketAddr;
use std::path::PathBuf;

use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ServerConfig {
    /// Socket address to bind to (e.g. "0.0.0.0:3000").
    pub bind_address: SocketAddr,
    /// Path to the static file directory (frontend/dist).
    pub static_dir: PathBuf,
    /// Path to the surface asset directory (surface/dist), holding the
    /// wasm-bindgen shell and component modules served under `/surface-static`.
    pub surface_dist_dir: PathBuf,
    /// Whether to set the Secure flag on session cookies.
    /// Default: true (production-safe). Set to false only for local HTTP dev.
    pub secure_cookies: bool,
    /// Number of trusted reverse-proxy hops in front of Brenn.
    ///
    /// Controls how the client IP is derived from `X-Forwarded-For`:
    /// - `0` (default): no trusted proxy. The TCP peer address is used directly
    ///   and `X-Forwarded-For` is ignored entirely. Correct when Brenn is exposed
    ///   directly to the internet.
    /// - `N >= 1`: trust the `N` rightmost `X-Forwarded-For` tokens as written by
    ///   trusted proxies, and take the client identity as the `N`-th token counted
    ///   from the right (the address the outermost trusted proxy observed as its
    ///   peer). Behind a single nginx that appends via
    ///   `proxy_add_x_forwarded_for`, set this to `1`.
    ///
    /// The value must equal the number of trusted proxies that append to
    /// `X-Forwarded-For`; setting it higher selects a token further left, back
    /// into attacker-controlled territory, re-enabling client-IP spoofing.
    /// Validated at config load: values above 8 are rejected (`validate_and_resolve`).
    pub trusted_proxy_hops: u8,
    /// Path to PID file. Used by logrotate's postrotate to send SIGHUP for log
    /// file reopening. None means no PID file is written (typical for dev).
    pub pid_file: Option<PathBuf>,
    /// Public HTTPS URL at which Brenn is reachable in front of the reverse
    /// proxy (e.g. `https://brenn.example.com`). No trailing slash.
    ///
    /// Used wherever Brenn needs to mention its own externally-visible URL —
    /// runbook messages, alert bodies, webhook endpoint documentation,
    /// eventually shareable links. Separate from `bind_address`, which is the
    /// internal socket the reverse proxy connects to.
    ///
    /// Optional, but recommended for every deployment.
    pub public_url: Option<String>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_address: SocketAddr::from(([0, 0, 0, 0], 3000)),
            static_dir: PathBuf::from("/opt/brenn/frontend/dist"),
            surface_dist_dir: PathBuf::from("/opt/brenn/surface/dist"),
            secure_cookies: true,
            trusted_proxy_hops: 0,
            pid_file: None,
            public_url: None,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DatabaseConfig {
    /// Path to the SQLite database file.
    pub path: PathBuf,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            path: PathBuf::from("/var/lib/brenn/brenn.db"),
        }
    }
}
