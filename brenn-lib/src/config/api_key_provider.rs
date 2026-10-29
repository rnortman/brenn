//! API-key providers — services Brenn sends a deployment-wide key to on the
//! caller's behalf.
//!
//! The key is the operator's: one `0600` host file per provider, read once at
//! boot and held as a [`SecretString`]. Brenn attaches it to a request as the
//! value of the header the block names; it is never passed into a container.

use std::collections::BTreeMap;
use std::path::PathBuf;

use http::HeaderName;

use super::brenn::BrennConfig;
use super::oauth_provider::{assert_max_response_bytes, resolve_api_origins};
use super::secret::{SecretString, load_secret_file_private};

/// Header names neither a key provider's header nor a caller may set: the
/// transport's own framing headers, and `cookie`.
pub const TRANSPORT_HEADERS: &[&str] = &[
    "host",
    "content-length",
    "transfer-encoding",
    "connection",
    "cookie",
];

/// One `api_key_provider` block, as the document states it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApiKeyProviderRaw {
    /// Host path to the file holding the key. 0600; read once at boot.
    pub key_file: PathBuf,
    /// The header the key is the whole value of, as the block writes it.
    pub header: String,
    /// The origins (`scheme://host[:port]`) requests carrying the key may be
    /// sent to. Non-empty.
    pub api_hosts: Vec<String>,
    /// The most response body bytes a keyed request reads.
    pub max_response_bytes: usize,
}

/// One key provider with its key in hand.
#[derive(Clone, Debug)]
pub struct ApiKeyProviderConfig {
    pub raw: ApiKeyProviderRaw,
    pub key: SecretString,
    /// `raw.header`, lowercased: a valid header name that is not a
    /// [`TRANSPORT_HEADERS`] entry.
    pub header: HeaderName,
    /// `raw.api_hosts`, parsed: each origin's `host[:port]` and the origin
    /// itself, as `OAuthProviderConfig::api_origins`.
    pub api_origins: Vec<(String, url::Url)>,
}

/// Validate every declared key provider, reading each key file once.
///
/// # Panics
///
/// A provider's size limit, origin, header name or key file is refused, or
/// its key holds a byte that is not visible ASCII or a space.
pub fn resolve_api_key_providers(config: &BrennConfig) -> BTreeMap<String, ApiKeyProviderConfig> {
    config
        .api_key_providers
        .iter()
        .map(|(name, raw)| (name.clone(), resolve_provider(name, raw)))
        .collect()
}

fn resolve_provider(name: &str, raw: &ApiKeyProviderRaw) -> ApiKeyProviderConfig {
    let label = format!("api_key_provider `{name}`");
    assert_max_response_bytes(&label, raw.max_response_bytes);
    let api_origins = resolve_api_origins(&label, &raw.api_hosts);
    let header = resolve_header(&label, &raw.header);
    // TODO(security-key-file-not-checked-disjoint-from-agent-visible-paths):
    // only the file's mode is checked; nothing refuses a key file inside a
    // path an agent can reach.
    let key = load_secret_file_private(&label, &raw.key_file);
    // Visible ASCII and space: every such string is a valid header value, and
    // a byte outside it (a BOM, a control or non-ASCII character pasted in) is
    // a damaged file. The message names the file, never the key.
    assert!(
        key.bytes().all(|b| b == b' ' || b.is_ascii_graphic()),
        "config: {label}: the key in {} holds a byte that is not visible ASCII or a space \
         (a control or non-ASCII character, such as a byte-order mark)",
        raw.key_file.display(),
    );
    ApiKeyProviderConfig {
        raw: raw.clone(),
        key: SecretString::new(key),
        header,
        api_origins,
    }
}

fn resolve_header(label: &str, text: &str) -> HeaderName {
    let header = HeaderName::from_bytes(text.as_bytes()).unwrap_or_else(|e| {
        panic!("config: {label}: header {text:?} is not a valid HTTP header name: {e}")
    });
    assert!(
        !TRANSPORT_HEADERS.contains(&header.as_str()),
        "config: {label}: header {text:?} is a transport header or the cookie; \
         the key cannot be sent in it",
    );
    header
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;
    use std::path::Path;

    use super::*;
    use crate::config::MAX_RESPONSE_BYTES_CEILING;
    use crate::config::test_api_key_provider_raw;
    use crate::test_utils::unwrap_panic_msg;

    fn panic_of(f: impl FnOnce() + std::panic::UnwindSafe) -> String {
        unwrap_panic_msg(std::panic::catch_unwind(f).expect_err("expected a panic"))
    }

    fn write_key(dir: &Path, text: &str, mode: u32) -> PathBuf {
        let path = dir.join("key");
        std::fs::write(&path, text).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).unwrap();
        path
    }

    fn raw(key_file: PathBuf) -> ApiKeyProviderRaw {
        ApiKeyProviderRaw {
            key_file,
            ..test_api_key_provider_raw("https://api.example.com")
        }
    }

    fn resolve_panic(raw: ApiKeyProviderRaw) -> String {
        panic_of(move || {
            resolve_provider("maps", &raw);
        })
    }

    #[test]
    fn the_key_is_read_trimmed_and_the_header_lowercased() {
        let tmp = tempfile::tempdir().unwrap();
        let mut provider = raw(write_key(tmp.path(), "  k3y-value\n", 0o600));
        provider.header = "X-Api-Key".to_string();
        let resolved = resolve_provider("maps", &provider);
        assert_eq!(resolved.key.expose(), "k3y-value");
        assert_eq!(resolved.header, HeaderName::from_static("x-api-key"));
        assert_eq!(
            resolved
                .api_origins
                .iter()
                .map(|(host, url)| (host.as_str(), url.as_str()))
                .collect::<Vec<_>>(),
            [("api.example.com", "https://api.example.com/")]
        );
    }

    #[test]
    fn the_key_never_reaches_debug_output() {
        let tmp = tempfile::tempdir().unwrap();
        let resolved = resolve_provider("maps", &raw(write_key(tmp.path(), "k3y-value", 0o600)));
        assert!(!format!("{resolved:?}").contains("k3y-value"));
    }

    #[test]
    fn a_missing_empty_or_shared_key_file_is_refused() {
        let tmp = tempfile::tempdir().unwrap();
        let msg = resolve_panic(raw(tmp.path().join("absent")));
        assert!(
            msg.contains("api_key_provider `maps`") && msg.contains("failed to read"),
            "{msg}"
        );
        let msg = resolve_panic(raw(write_key(tmp.path(), " \n", 0o600)));
        assert!(msg.contains("is empty"), "{msg}");
        let msg = resolve_panic(raw(write_key(tmp.path(), "k3y-value", 0o640)));
        assert!(msg.contains("group/world-accessible"), "{msg}");
    }

    #[test]
    fn a_key_with_a_control_or_non_ascii_byte_is_refused_without_printing_it() {
        for text in [
            "k3y\u{7}value",
            "\u{feff}k3y-value",
            "k3y\u{200b}value",
            "k3y\u{e9}value",
            "k3y\nvalue",
            "k3y\tvalue",
        ] {
            let tmp = tempfile::tempdir().unwrap();
            let msg = resolve_panic(raw(write_key(tmp.path(), text, 0o600)));
            assert!(
                msg.contains("api_key_provider `maps`")
                    && msg.contains("not visible ASCII or a space"),
                "{text:?}: {msg}"
            );
            assert!(!msg.contains("k3y"), "{msg}");
        }
    }

    #[test]
    fn a_key_with_an_interior_space_is_accepted() {
        let tmp = tempfile::tempdir().unwrap();
        let resolved = resolve_provider(
            "maps",
            &raw(write_key(tmp.path(), "Bearer k3y-value\n", 0o600)),
        );
        assert_eq!(resolved.key.expose(), "Bearer k3y-value");
    }

    #[test]
    fn an_invalid_or_transport_header_name_is_refused() {
        for (header, expect) in [
            ("X Api Key", "not a valid HTTP header name"),
            ("", "not a valid HTTP header name"),
            ("Host", "transport header"),
            ("content-length", "transport header"),
            ("Transfer-Encoding", "transport header"),
            ("connection", "transport header"),
            ("Cookie", "transport header"),
        ] {
            let mut provider = raw(PathBuf::from("/nonexistent"));
            provider.header = header.to_string();
            let msg = resolve_panic(provider);
            assert!(msg.contains(expect), "{header:?}: {msg}");
        }
    }

    #[test]
    fn an_api_host_that_is_not_an_origin_is_refused() {
        for (entry, expect) in [
            ("https://api.example.com/v1", "must be an origin"),
            ("https://api.example.com/", "must be written as"),
            ("http://api.example.com", "must be https"),
            ("api.example.com", "not a valid URL"),
        ] {
            let mut provider = raw(PathBuf::from("/nonexistent"));
            provider.api_hosts = vec![entry.to_string()];
            let msg = resolve_panic(provider);
            assert!(
                msg.contains("api_key_provider `maps`") && msg.contains(expect),
                "{entry}: {msg}"
            );
        }
    }

    #[test]
    fn max_response_bytes_outside_its_bounds_is_refused() {
        for bytes in [0, MAX_RESPONSE_BYTES_CEILING + 1] {
            let mut provider = raw(PathBuf::from("/nonexistent"));
            provider.max_response_bytes = bytes;
            let msg = resolve_panic(provider);
            assert!(msg.contains("max_response_bytes"), "{bytes}: {msg}");
        }
    }

    #[test]
    fn every_declared_provider_resolves() {
        let tmp = tempfile::tempdir().unwrap();
        let mut config = BrennConfig::default();
        assert!(resolve_api_key_providers(&config).is_empty());
        config.api_key_providers.insert(
            "maps".to_string(),
            raw(write_key(tmp.path(), "k3y-value", 0o600)),
        );
        let resolved = resolve_api_key_providers(&config);
        assert_eq!(resolved.keys().collect::<Vec<_>>(), ["maps"]);
    }
}
