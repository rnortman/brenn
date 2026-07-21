//! Webhook address parsing: `webhook:<endpoint-slug>`.

use crate::messaging::is_unreserved_char;
use crate::webhook::error::WebhookError;

/// The webhook address prefix.
pub const WEBHOOK_PREFIX: &str = "webhook:";

/// A parsed webhook address.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebhookAddress {
    /// Endpoint slug (validated charset `[A-Za-z0-9._~-]+`).
    pub endpoint_slug: String,
}

impl WebhookAddress {
    /// Produce canonical `webhook:<endpoint-slug>` string.
    pub fn format(&self) -> String {
        format!("webhook:{}", self.endpoint_slug)
    }
}

impl std::fmt::Display for WebhookAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.format())
    }
}

/// Parse a raw address string into a `WebhookAddress`.
///
/// Validates:
/// - `webhook:` prefix present.
/// - Slug is non-empty and matches `[A-Za-z0-9._~-]+` (RFC 3986 unreserved chars).
pub fn parse_webhook_address(addr: &str) -> Result<WebhookAddress, WebhookError> {
    let slug = addr
        .strip_prefix(WEBHOOK_PREFIX)
        .ok_or_else(|| WebhookError::WrongProtocol {
            address: addr.to_string(),
        })?;

    if slug.is_empty() {
        return Err(WebhookError::AddressInvalid {
            address: addr.to_string(),
            detail: "endpoint slug must be non-empty".to_string(),
        });
    }

    if !slug.chars().all(is_unreserved_char) {
        return Err(WebhookError::AddressInvalid {
            address: addr.to_string(),
            detail: format!(
                "endpoint slug {:?} is invalid; must match [A-Za-z0-9._~-]+",
                slug,
            ),
        });
    }

    Ok(WebhookAddress {
        endpoint_slug: slug.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_simple_address() {
        let addr = parse_webhook_address("webhook:phonebuddy").unwrap();
        assert_eq!(addr.endpoint_slug, "phonebuddy");
    }

    #[test]
    fn address_format_roundtrip() {
        let raw = "webhook:my-endpoint.1";
        let addr = parse_webhook_address(raw).unwrap();
        assert_eq!(addr.format(), raw);
    }

    #[test]
    fn all_unreserved_chars_valid() {
        let addr = parse_webhook_address("webhook:aZ0._~-test").unwrap();
        assert_eq!(addr.endpoint_slug, "aZ0._~-test");
    }

    #[test]
    fn wrong_protocol_prefix() {
        let err = parse_webhook_address("brenn:foo").unwrap_err();
        assert!(matches!(err, WebhookError::WrongProtocol { .. }));
    }

    #[test]
    fn mqtt_prefix_wrong_protocol() {
        let err = parse_webhook_address("mqtt:client:topic").unwrap_err();
        assert!(matches!(err, WebhookError::WrongProtocol { .. }));
    }

    #[test]
    fn empty_slug_rejected() {
        let err = parse_webhook_address("webhook:").unwrap_err();
        assert!(matches!(err, WebhookError::AddressInvalid { .. }));
    }

    #[test]
    fn space_in_slug_rejected() {
        let err = parse_webhook_address("webhook:bad slug").unwrap_err();
        assert!(matches!(err, WebhookError::AddressInvalid { .. }));
    }

    #[test]
    fn slash_in_slug_rejected() {
        let err = parse_webhook_address("webhook:foo/bar").unwrap_err();
        assert!(matches!(err, WebhookError::AddressInvalid { .. }));
    }

    #[test]
    fn colon_in_slug_rejected() {
        // `webhook:foo:bar` — the slug part would be `foo:bar` which contains a colon.
        // Colons are not unreserved chars; reject.
        let err = parse_webhook_address("webhook:foo:bar").unwrap_err();
        assert!(matches!(err, WebhookError::AddressInvalid { .. }));
    }
}
