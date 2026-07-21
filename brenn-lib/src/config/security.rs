use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SecurityConfig {
    /// Auth endpoint rate limit: token replenishment interval in seconds.
    /// One token is added every this many seconds.
    pub auth_rate_interval_secs: u64,
    /// Auth endpoint rate limit: burst size (max tokens).
    pub auth_rate_burst: u32,
    /// Global rate limit: token replenishment interval in seconds.
    pub global_rate_interval_secs: u64,
    /// Global rate limit: burst size (max tokens).
    pub global_rate_burst: u32,
    /// Asset (auth-gated static) rate limit: token replenishment interval in seconds.
    pub asset_rate_interval_secs: u64,
    /// Asset rate limit: burst size (max tokens).
    pub asset_rate_burst: u32,
    /// Auth form body size limit in bytes.
    pub auth_body_limit: usize,
    /// Global body size limit in bytes.
    pub global_body_limit: usize,
    /// Per-route body size limit for the upload endpoint, in bytes.
    /// Replaces the hardcoded 20 MiB cap on `POST /app/{slug}/upload`.
    pub upload_body_limit: usize,
    /// Maximum image long edge (pixels) delivered to the browser as the
    /// client-side resize cap. Not enforced server-side.
    pub max_image_long_edge: u32,
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            auth_rate_interval_secs: 6,
            auth_rate_burst: 10,
            global_rate_interval_secs: 1,
            global_rate_burst: 100,
            asset_rate_interval_secs: 1,
            // Auth-gated static assets have their own generous per-client-IP
            // bucket so a synchronized post-deploy reload of a kiosk fleet behind
            // one NAT IP (~7 asset requests per single-component surface load)
            // does not drain the global limiter and feed fail2ban. Sized for a
            // reload storm of ~150+ single-component surfaces on the asset bucket
            // alone; see the router's asset sub-router for the end-to-end bound.
            asset_rate_burst: 2000,
            auth_body_limit: 4096,
            global_body_limit: 1024 * 1024,
            upload_body_limit: 25 * 1024 * 1024,
            max_image_long_edge: 2576,
        }
    }
}
