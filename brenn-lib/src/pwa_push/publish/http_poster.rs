//! HTTP execution abstraction for outbound web-push requests.
//!
//! `HttpPoster` decouples `PwaPushService` from `reqwest::Client` so tests can
//! inject a mock that simulates delays, errors, hangs, and panics without
//! hitting real endpoints.

/// Abstraction over HTTP execution for outbound push requests.
///
/// The production implementation wraps `reqwest::Client`. Tests inject a
/// `MockHttpPoster` to simulate delays, errors, hangs, and panics without
/// hitting real endpoints.
#[async_trait::async_trait]
pub(crate) trait HttpPoster: Send + Sync + 'static {
    async fn execute(&self, req: reqwest::Request) -> reqwest::Result<reqwest::Response>;
}

/// Production `HttpPoster` wrapping a `reqwest::Client`.
pub(crate) struct ReqwestPoster {
    pub(super) client: reqwest::Client,
}

#[async_trait::async_trait]
impl HttpPoster for ReqwestPoster {
    async fn execute(&self, req: reqwest::Request) -> reqwest::Result<reqwest::Response> {
        self.client.execute(req).await
    }
}
