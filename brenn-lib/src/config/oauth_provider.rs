//! OAuth providers — the authorization servers Brenn connects users' accounts
//! through — and the credential vault their tokens are kept in.
//!
//! The vault is the first place Brenn holds something no agent may read: it is
//! a host directory owned by the server's uid, mode 0700, and disjoint from
//! every path any agent can see. Everything here runs at boot and refuses a
//! configuration that would put a credential within an agent's reach.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use indexmap::IndexMap;

use super::app::AppConfig;
use super::brenn::BrennConfig;
use super::resolve::canonical_with_missing_tail;
use super::secret::{SecretString, load_secret_file_private};

/// The body limit a provider gets when its block states no
/// `max_response_bytes`.
// TODO(api-request-read-limit-vs-bus-cap): this default is above the bus
// default `[messaging] max_body_bytes` (64 KiB), so an answer between the two
// reaches the caller only as the executor's `internal` over-cap error.
pub const DEFAULT_MAX_RESPONSE_BYTES: usize = 256 * 1024;

/// The largest `max_response_bytes` a provider may state: the async tool
/// result cap. `brenn-tool-registry` asserts at compile time that the two are
/// equal.
pub const MAX_RESPONSE_BYTES_CEILING: usize = 512 * 1024;

/// One `oauth_provider` block, as the document states it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OAuthProviderRaw {
    /// The name the provider's consent screen shows, repeated on Brenn's pages
    /// so the user recognises it.
    pub app_name: String,
    /// Host path to the provider's client file (Google's `client_secret.json`
    /// download). 0600; read once at boot.
    pub client_secret_file: PathBuf,
    pub authorize_url: String,
    pub token_url: String,
    /// Absent: disconnecting deletes the credential without revoking it.
    pub revoke_url: Option<String>,
    pub userinfo_url: String,
    /// The userinfo JSON field whose string value names the account.
    pub account_field: String,
    /// Scopes requested on every connect.
    pub base_scopes: Vec<String>,
    /// Named scope groups a connect request chooses among. Non-empty, and no
    /// set is empty.
    pub scope_sets: BTreeMap<String, Vec<String>>,
    /// Query parameters appended to every authorize URL.
    pub extra_authorize_params: BTreeMap<String, String>,
    /// The origins (`scheme://host[:port]`) requests may be sent to with this
    /// provider's credentials. Non-empty.
    pub api_hosts: Vec<String>,
    /// The most response body bytes a credentialed request reads.
    pub max_response_bytes: usize,
}

/// One provider with its client identity in hand.
#[derive(Clone, Debug)]
pub struct OAuthProviderConfig {
    pub raw: OAuthProviderRaw,
    pub client_id: String,
    pub client_secret: SecretString,
    /// `<server.public_url>/oauth/<name>/callback`.
    pub redirect_uri: String,
    /// `raw.api_hosts`, parsed: each origin's `host[:port]` (the form a
    /// request names its host in, see [`origin_host`]) and the origin itself.
    pub api_origins: Vec<(String, url::Url)>,
}

/// Every declared provider and the vault their credentials are kept in.
#[derive(Clone, Debug)]
pub struct ResolvedOAuth {
    /// `server.vault_dir`, canonicalised at boot.
    pub vault_dir: PathBuf,
    pub providers: BTreeMap<String, OAuthProviderConfig>,
}

/// Validate the vault and every provider, reading each client file once.
///
/// `None` when the document declares no provider. `apps` is the resolved app
/// map: the vault is checked against the paths resolution settled.
///
/// # Panics
///
/// - `server.vault_dir` is set with no provider declared, or a provider is
///   declared without it.
/// - [`validate_vault_dir`] or [`assert_vault_disjoint_from_apps`] refuses the
///   vault.
/// - A provider's URL, origin, size limit or client file is refused.
pub fn resolve_oauth(
    config: &BrennConfig,
    apps: &IndexMap<String, AppConfig>,
) -> Option<ResolvedOAuth> {
    let vault_dir = match (&config.server.vault_dir, config.oauth_providers.is_empty()) {
        (None, true) => return None,
        (Some(dir), true) => panic!(
            "config: server.vault_dir is set ({}) but no oauth_provider is declared; \
             remove it",
            dir.display(),
        ),
        (None, false) => panic!(
            "config: oauth_provider {} is declared but server.vault_dir is not set; \
             credentials need a vault",
            config
                .oauth_providers
                .keys()
                .map(|name| format!("`{name}`"))
                .collect::<Vec<_>>()
                .join(", "),
        ),
        (Some(dir), false) => dir,
    };
    validate_vault_dir(vault_dir);
    assert_vault_disjoint_from_apps(config, apps);
    // The vault runs on the canonical path, so a symlink along the configured
    // one cannot be repointed under a running server.
    let vault_dir = std::fs::canonicalize(vault_dir).unwrap_or_else(|e| {
        panic!(
            "server.vault_dir validation failed: cannot canonicalize {}: {e}",
            vault_dir.display()
        )
    });

    let public_url = config
        .server
        .public_url
        .as_deref()
        .expect("server.public_url is validated before oauth providers resolve");
    let providers = config
        .oauth_providers
        .iter()
        .map(|(name, raw)| (name.clone(), resolve_provider(name, raw, public_url)))
        .collect();
    Some(ResolvedOAuth {
        vault_dir,
        providers,
    })
}

/// The callback URL a provider's redirect lands on.
pub fn redirect_uri(public_url: &str, provider: &str) -> String {
    format!(
        "{}/oauth/{provider}/callback",
        public_url.trim_end_matches('/')
    )
}

fn resolve_provider(name: &str, raw: &OAuthProviderRaw, public_url: &str) -> OAuthProviderConfig {
    let label = format!("oauth_provider `{name}`");
    assert_max_response_bytes(&label, raw.max_response_bytes);
    let authorize = parse_url(&label, "authorize_url", &raw.authorize_url);
    assert!(
        matches!(authorize.scheme(), "https" | "http"),
        "config: {label}: authorize_url {:?} is not an http(s) URL",
        raw.authorize_url,
    );
    validate_credential_url(&label, "token_url", &raw.token_url);
    validate_credential_url(&label, "userinfo_url", &raw.userinfo_url);
    if let Some(revoke_url) = &raw.revoke_url {
        validate_credential_url(&label, "revoke_url", revoke_url);
    }
    let api_origins = resolve_api_origins(&label, &raw.api_hosts);

    let redirect_uri = redirect_uri(public_url, name);
    let text = load_secret_file_private(&label, &raw.client_secret_file);
    let client = parse_client_file(&label, &raw.client_secret_file, &text);
    if let Some(registered) = &client.redirect_uris {
        assert!(
            registered.contains(&redirect_uri),
            "config: {label}: the redirect URI {redirect_uri:?} is not among the client's \
             redirect_uris {registered:?} in {}; add it in the provider's console or fix \
             server.public_url",
            raw.client_secret_file.display(),
        );
    }
    OAuthProviderConfig {
        raw: raw.clone(),
        client_id: client.client_id,
        client_secret: client.client_secret,
        redirect_uri,
        api_origins,
    }
}

/// Refuse a provider's `max_response_bytes` outside
/// `1..=`[`MAX_RESPONSE_BYTES_CEILING`].
pub(crate) fn assert_max_response_bytes(label: &str, bytes: usize) {
    assert!(
        bytes > 0 && bytes <= MAX_RESPONSE_BYTES_CEILING,
        "config: {label}: max_response_bytes = {bytes} is outside \
         1..={MAX_RESPONSE_BYTES_CEILING} (the tool result cap); page the request instead",
    );
}

fn parse_url(label: &str, key: &str, text: &str) -> url::Url {
    url::Url::parse(text)
        .unwrap_or_else(|e| panic!("config: {label}: {key} {text:?} is not a valid URL: {e}"))
}

/// Whether a URL may carry a credential: `https`, or `http` to a loopback
/// literal (the form a local stub server needs, never a route off the host).
fn credential_scheme_ok(url: &url::Url) -> bool {
    match url.scheme() {
        "https" => true,
        "http" => match url.host() {
            Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
            Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
            Some(url::Host::Domain(domain)) => domain == "localhost",
            None => false,
        },
        _ => false,
    }
}

fn validate_credential_url(label: &str, key: &str, text: &str) {
    let url = parse_url(label, key, text);
    assert!(
        credential_scheme_ok(&url),
        "config: {label}: {key} {text:?} must be https (http only to 127.0.0.1, [::1] or \
         localhost): a credential is sent to it",
    );
}

/// `host[:port]` of an http(s) origin, the port only when it is not the
/// scheme's default.
///
/// # Panics
///
/// `origin` has no host.
pub fn origin_host(origin: &url::Url) -> String {
    let host = origin.host_str().expect("an http(s) origin has a host");
    match origin.port() {
        Some(port) => format!("{host}:{port}"),
        None => host.to_string(),
    }
}

/// A provider's `api_hosts`, each validated as an origin and paired with its
/// [`origin_host`], the key a request's host is matched against. Every
/// provider kind resolves its hosts here, so the match compares alike across
/// kinds.
///
/// # Panics
///
/// An entry is not a canonical https (or loopback http) origin.
pub(crate) fn resolve_api_origins(label: &str, hosts: &[String]) -> Vec<(String, url::Url)> {
    hosts
        .iter()
        .map(|text| {
            let url = validate_origin(label, text);
            (origin_host(&url), url)
        })
        .collect()
}

/// An `api_hosts` entry: exactly an origin, written canonically, so the exact
/// host match a request is held to compares like with like.
fn validate_origin(label: &str, text: &str) -> url::Url {
    let url = parse_url(label, "api_hosts entry", text);
    assert!(
        credential_scheme_ok(&url),
        "config: {label}: api_hosts entry {text:?} must be https (http only to 127.0.0.1, \
         [::1] or localhost): a credential is sent to it",
    );
    assert!(
        url.username().is_empty()
            && url.password().is_none()
            && url.path() == "/"
            && url.query().is_none()
            && url.fragment().is_none(),
        "config: {label}: api_hosts entry {text:?} must be an origin — scheme, host and \
         optional port, with no user info, path, query or fragment",
    );
    let canonical = url.origin().ascii_serialization();
    assert!(
        text == canonical,
        "config: {label}: api_hosts entry {text:?} must be written as {canonical:?}",
    );
    url
}

/// The identity a client file carries.
#[derive(Debug)]
struct ClientFile {
    client_id: String,
    client_secret: SecretString,
    /// Present when the file lists the redirect URIs registered for the client.
    redirect_uris: Option<Vec<String>>,
}

/// Read a client file in either documented shape: Google's Web-client
/// download (`{"web": {...}}`) or the bare object.
fn parse_client_file(label: &str, path: &Path, text: &str) -> ClientFile {
    let at = path.display();
    let value: serde_json::Value = serde_json::from_str(text)
        .unwrap_or_else(|e| panic!("config: {label}: client file {at} is not JSON: {e}"));
    let top = value
        .as_object()
        .unwrap_or_else(|| panic!("config: {label}: client file {at} is not a JSON object"));
    let client = match top.get("web") {
        Some(web) => web
            .as_object()
            .unwrap_or_else(|| panic!("config: {label}: client file {at}: `web` is not an object")),
        None if top.contains_key("client_id") && top.contains_key("client_secret") => top,
        None => panic!(
            "config: {label}: client file {at} has neither a `web` object nor top-level \
             `client_id` and `client_secret`; keys found: {:?}",
            top.keys().collect::<Vec<_>>(),
        ),
    };
    let field = |key: &str| -> String {
        match client.get(key) {
            Some(serde_json::Value::String(s)) if !s.is_empty() => s.clone(),
            _ => panic!(
                "config: {label}: client file {at}: `{key}` is missing or not a non-empty string"
            ),
        }
    };
    let redirect_uris = client.get("redirect_uris").map(|uris| {
        uris.as_array()
            .and_then(|list| {
                list.iter()
                    .map(|uri| uri.as_str().map(str::to_string))
                    .collect::<Option<Vec<_>>>()
            })
            .unwrap_or_else(|| {
                panic!(
                    "config: {label}: client file {at}: `redirect_uris` is not a list of strings"
                )
            })
    });
    ClientFile {
        client_id: field("client_id"),
        client_secret: SecretString::new(field("client_secret")),
        redirect_uris,
    }
}

/// Refuse a vault directory that is not absolute, missing, not a directory,
/// not owned by the server's euid, or not mode exactly 0700.
///
/// The operator creates it; Brenn only creates directories beneath it.
pub fn validate_vault_dir(path: &Path) {
    crate::runtime_dir::validate_private_dir("server.vault_dir", path);
}

/// Refuse a `server.vault_dir` that is not disjoint from [`vault_roots`] of
/// `config` and `apps`; nothing to check when no vault is configured.
///
/// Boot and every reload call this: a reload can move an app's roots without
/// a restart.
pub fn assert_vault_disjoint_from_apps(config: &BrennConfig, apps: &IndexMap<String, AppConfig>) {
    if let Some(vault_dir) = &config.server.vault_dir {
        assert_vault_disjoint(vault_dir, &vault_roots(config, apps));
    }
}

/// Every host path the configuration declares an agent can see or the server
/// serves, labelled for the refusal that names it: each app's working
/// directory, repo mounts, state directory, container home and `extra_mounts`
/// host sides; `repo_sync.repo_dir`; every declared container's home.
///
/// A container's `extra_args` are passed to podman unread and are not roots:
/// a mount written there is outside this check.
pub fn vault_roots(
    config: &BrennConfig,
    apps: &IndexMap<String, AppConfig>,
) -> Vec<(String, PathBuf)> {
    let mut roots = Vec::new();
    for (slug, app) in apps {
        roots.push((format!("app {slug:?} working_dir"), app.working_dir.clone()));
        roots.push((format!("app {slug:?} state_dir"), app.state_dir.clone()));
        for mount in &app.mounts {
            roots.push((
                format!("app {slug:?} repo mount {:?}", mount.slug),
                mount.host_path.clone(),
            ));
        }
        if let Some(spawn) = &app.container_spawn {
            roots.push((
                format!("app {slug:?} container home_dir"),
                spawn.home_dir.clone(),
            ));
            for spec in &spawn.extra_mounts {
                // `host:container[:options]`; a source that is not an absolute
                // path is a named volume, not a host directory.
                let host = spec.split(':').next().unwrap_or_default();
                if host.starts_with('/') {
                    roots.push((
                        format!("app {slug:?} extra_mounts entry {spec:?}"),
                        PathBuf::from(host),
                    ));
                }
            }
        }
    }
    if let Some(repo_dir) = &config.repo_sync.repo_dir {
        roots.push(("repo_sync.repo_dir".to_string(), repo_dir.clone()));
    }
    let mut containers: Vec<_> = config.container.iter().collect();
    containers.sort_by(|a, b| a.0.cmp(b.0));
    for (name, container) in containers {
        roots.push((
            format!("container {name:?} home_dir"),
            container.home_dir.clone(),
        ));
    }
    roots
}

/// Refuse a vault that lies inside, or contains, any of `roots`, both sides
/// compared by where their symlinks lead now.
pub fn assert_vault_disjoint(vault_dir: &Path, roots: &[(String, PathBuf)]) {
    let vault = canonical_with_missing_tail(vault_dir);
    for (what, root) in roots {
        let canonical = canonical_with_missing_tail(root);
        let relation = if vault.starts_with(&canonical) {
            "is inside"
        } else if canonical.starts_with(&vault) {
            "contains"
        } else {
            continue;
        };
        panic!(
            "server.vault_dir validation failed: {} ({}) {relation} {what} {} ({}); the vault \
             must be disjoint from every path an agent can reach",
            vault_dir.display(),
            vault.display(),
            root.display(),
            canonical.display(),
        );
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;

    use super::*;
    use crate::config::test_fixtures::test_app_config;
    use crate::config::{ContainerConfig, ContainerSpawnConfig, ServerConfig};
    use crate::test_utils::unwrap_panic_msg;

    fn panic_of(f: impl FnOnce() + std::panic::UnwindSafe) -> String {
        unwrap_panic_msg(std::panic::catch_unwind(f).expect_err("expected a panic"))
    }

    fn private_dir() -> tempfile::TempDir {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::set_permissions(tmp.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        tmp
    }

    fn write_private(dir: &Path, name: &str, text: &str) -> PathBuf {
        let path = dir.join(name);
        std::fs::write(&path, text).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        path
    }

    const CALLBACK: &str = "https://brenn.example.com/oauth/google/callback";

    fn web_client(redirect: &str) -> String {
        format!(
            r#"{{"web": {{"client_id": "cid.apps.example.com", "client_secret": "s3cret",
                 "redirect_uris": ["{redirect}"], "project_id": "p"}}}}"#
        )
    }

    fn raw(client_secret_file: PathBuf) -> OAuthProviderRaw {
        OAuthProviderRaw {
            client_secret_file,
            ..crate::config::test_oauth_provider_raw("https://auth.example.com")
        }
    }

    fn resolve(raw: &OAuthProviderRaw) -> OAuthProviderConfig {
        resolve_provider("google", raw, "https://brenn.example.com")
    }

    fn resolve_panic(raw: OAuthProviderRaw) -> String {
        panic_of(move || {
            resolve(&raw);
        })
    }

    #[test]
    fn the_web_client_shape_resolves_with_the_callback_uri() {
        let tmp = private_dir();
        let file = write_private(tmp.path(), "client.json", &web_client(CALLBACK));
        let provider = resolve(&raw(file));
        assert_eq!(provider.client_id, "cid.apps.example.com");
        assert_eq!(provider.client_secret.expose(), "s3cret");
        assert_eq!(provider.redirect_uri, CALLBACK);
    }

    #[test]
    fn the_bare_shape_resolves_without_redirect_uris() {
        let tmp = private_dir();
        let file = write_private(
            tmp.path(),
            "client.json",
            r#"{"client_id": "cid", "client_secret": "s3cret"}"#,
        );
        let provider = resolve(&raw(file));
        assert_eq!(provider.client_id, "cid");
        assert_eq!(provider.client_secret.expose(), "s3cret");
    }

    #[test]
    fn a_third_client_file_shape_is_refused_naming_the_keys_found() {
        let tmp = private_dir();
        let file = write_private(
            tmp.path(),
            "client.json",
            r#"{"installed": {"client_id": "cid", "client_secret": "s"}}"#,
        );
        let msg = resolve_panic(raw(file));
        assert!(msg.contains("neither a `web` object"), "{msg}");
        assert!(msg.contains("installed"), "{msg}");
    }

    #[test]
    fn a_client_file_readable_by_others_is_refused() {
        let tmp = private_dir();
        let file = write_private(tmp.path(), "client.json", &web_client(CALLBACK));
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o644)).unwrap();
        let msg = resolve_panic(raw(file));
        assert!(msg.contains("group/world-accessible"), "{msg}");
    }

    #[test]
    fn a_redirect_uri_missing_from_the_client_is_refused_naming_both() {
        let tmp = private_dir();
        let file = write_private(
            tmp.path(),
            "client.json",
            &web_client("https://old.example.com/oauth/google/callback"),
        );
        let msg = resolve_panic(raw(file));
        assert!(msg.contains(CALLBACK), "{msg}");
        assert!(msg.contains("old.example.com"), "{msg}");
    }

    #[test]
    fn the_secret_never_reaches_debug_output() {
        let tmp = private_dir();
        let file = write_private(tmp.path(), "client.json", &web_client(CALLBACK));
        let provider = resolve(&raw(file));
        assert!(!format!("{provider:?}").contains("s3cret"));
    }

    #[test]
    fn max_response_bytes_above_the_cap_is_refused() {
        let mut provider = raw(PathBuf::from("/nonexistent"));
        provider.max_response_bytes = MAX_RESPONSE_BYTES_CEILING + 1;
        let msg = resolve_panic(provider);
        assert!(msg.contains("max_response_bytes"), "{msg}");
    }

    #[test]
    fn credential_urls_must_be_https_or_loopback_http() {
        for (key, set) in [
            (
                "token_url",
                (|p: &mut OAuthProviderRaw, v: &str| p.token_url = v.to_string())
                    as fn(&mut OAuthProviderRaw, &str),
            ),
            ("userinfo_url", |p, v| p.userinfo_url = v.to_string()),
            ("revoke_url", |p, v| p.revoke_url = Some(v.to_string())),
        ] {
            let mut provider = raw(PathBuf::from("/nonexistent"));
            set(&mut provider, "http://oauth2.example.com/x");
            let msg = resolve_panic(provider);
            assert!(msg.contains(key) && msg.contains("must be https"), "{msg}");
        }
        let mut provider = raw(PathBuf::from("/nonexistent"));
        provider.token_url = "not a url".to_string();
        let msg = resolve_panic(provider);
        assert!(
            msg.contains("token_url") && msg.contains("not a valid URL"),
            "{msg}"
        );
    }

    #[test]
    fn loopback_http_is_admitted_for_a_stub_server() {
        let tmp = private_dir();
        let file = write_private(tmp.path(), "client.json", &web_client(CALLBACK));
        let mut provider = raw(file);
        provider.token_url = "http://127.0.0.1:4100/token".to_string();
        provider.userinfo_url = "http://[::1]:4100/userinfo".to_string();
        provider.revoke_url = Some("http://localhost:4100/revoke".to_string());
        provider.api_hosts = vec!["http://127.0.0.1:4100".to_string()];
        resolve(&provider);
    }

    #[test]
    fn api_hosts_resolve_to_their_host_keys_and_origins() {
        let tmp = private_dir();
        let file = write_private(tmp.path(), "client.json", &web_client(CALLBACK));
        let mut provider = raw(file);
        provider.api_hosts = vec![
            "https://mail.example.com".to_string(),
            "http://127.0.0.1:4100".to_string(),
        ];
        let origins: Vec<(String, String)> = resolve(&provider)
            .api_origins
            .into_iter()
            .map(|(host, url)| (host, url.to_string()))
            .collect();
        assert_eq!(
            origins,
            [
                (
                    "mail.example.com".to_string(),
                    "https://mail.example.com/".to_string()
                ),
                (
                    "127.0.0.1:4100".to_string(),
                    "http://127.0.0.1:4100/".to_string()
                ),
            ]
        );
    }

    #[test]
    fn an_api_host_that_is_not_an_origin_is_refused() {
        for (entry, expect) in [
            ("https://mail.example.com/gmail", "must be an origin"),
            ("https://alice@mail.example.com", "must be an origin"),
            ("https://mail.example.com?x=1", "must be an origin"),
            ("https://mail.example.com/", "must be written as"),
            ("https://MAIL.example.com", "must be written as"),
            ("https://mail.example.com:443", "must be written as"),
            ("http://mail.example.com", "must be https"),
            ("mail.example.com", "not a valid URL"),
        ] {
            let mut provider = raw(PathBuf::from("/nonexistent"));
            provider.api_hosts = vec![entry.to_string()];
            let msg = resolve_panic(provider);
            assert!(msg.contains(expect), "{entry}: {msg}");
        }
    }

    #[test]
    fn the_redirect_uri_ignores_a_trailing_slash_on_public_url() {
        assert_eq!(
            redirect_uri("https://brenn.example.com/", "google"),
            CALLBACK
        );
    }

    #[test]
    fn a_valid_vault_dir_is_accepted() {
        let tmp = private_dir();
        validate_vault_dir(tmp.path());
    }

    /// The refusals themselves are `validate_private_dir`'s, tested there;
    /// this checks the vault's are labelled as such.
    #[test]
    fn vault_dir_refusals_name_the_vault() {
        let tmp = private_dir();
        let loose = tmp.path().join("loose");
        std::fs::create_dir(&loose).unwrap();
        std::fs::set_permissions(&loose, std::fs::Permissions::from_mode(0o750)).unwrap();
        let msg = panic_of(|| validate_vault_dir(&loose));
        assert!(
            msg.starts_with("server.vault_dir validation failed:")
                && msg.contains("0750")
                && msg.contains("0700"),
            "{msg}"
        );
    }

    /// A config and app map holding one of every root class under `base`.
    fn config_with_every_root(base: &Path) -> (BrennConfig, IndexMap<String, AppConfig>) {
        let mut app = test_app_config("pa");
        app.working_dir = base.join("work");
        app.state_dir = base.join("home/.config/brenn/pa");
        app.mounts = vec![crate::config::ResolvedMount {
            slug: "ws".to_string(),
            host_path: base.join("repos/ws"),
            container_path: None,
            access: crate::config::AccessLevel::ReadWrite,
            auto_pull: false,
            is_working_dir: false,
            primary: false,
        }];
        app.container_spawn = Some(ContainerSpawnConfig {
            image: "brenn-cc:latest".to_string(),
            home_dir: base.join("home"),
            container_home: PathBuf::from("/home/agent"),
            host_working_dir: base.join("work"),
            container_working_dir: PathBuf::from("/work"),
            working_dir_is_repo: false,
            repo_mounts: vec![],
            extra_mounts: vec![
                format!("{}:/srv/shared:ro", base.join("shared").display()),
                "named-volume:/data".to_string(),
            ],
            extra_args: vec![],
            tool_socket_selinux_context: None,
        });
        let mut config = BrennConfig {
            server: ServerConfig {
                public_url: Some("https://brenn.example.com".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };
        config.repo_sync.repo_dir = Some(base.join("repos"));
        config.container.insert(
            "spare".to_string(),
            ContainerConfig {
                image: "brenn-cc:latest".to_string(),
                home_dir: base.join("spare-home"),
                container_home: PathBuf::from("/home/agent"),
                extra_mounts: vec![],
                extra_args: vec![],
                tool_socket_selinux_context: None,
            },
        );
        (config, IndexMap::from([("pa".to_string(), app)]))
    }

    #[test]
    fn vault_roots_cover_every_root_class() {
        let base = Path::new("/srv/brenn");
        let (config, apps) = config_with_every_root(base);
        let labels: Vec<String> = vault_roots(&config, &apps)
            .into_iter()
            .map(|(label, _)| label)
            .collect();
        assert_eq!(
            labels,
            vec![
                "app \"pa\" working_dir",
                "app \"pa\" state_dir",
                "app \"pa\" repo mount \"ws\"",
                "app \"pa\" container home_dir",
                "app \"pa\" extra_mounts entry \"/srv/brenn/shared:/srv/shared:ro\"",
                "repo_sync.repo_dir",
                "container \"spare\" home_dir",
            ],
        );
    }

    #[test]
    fn a_vault_inside_or_containing_any_root_is_refused() {
        let tmp = tempfile::tempdir().unwrap();
        let base = tmp.path();
        let (config, apps) = config_with_every_root(base);
        for (label, root) in vault_roots(&config, &apps) {
            let roots = vec![(label.clone(), root.clone())];
            let inside = root.join("vault");
            let msg = panic_of(|| assert_vault_disjoint(&inside, &roots));
            assert!(msg.contains("is inside") && msg.contains(&label), "{msg}");
            let containing = root.parent().unwrap().to_path_buf();
            let msg = panic_of(|| assert_vault_disjoint(&containing, &roots));
            assert!(msg.contains("contains") && msg.contains(&label), "{msg}");
        }
        assert_vault_disjoint(&base.join("vault"), &vault_roots(&config, &apps));
    }

    #[test]
    fn a_symlink_cannot_hide_a_vault_inside_a_root() {
        let tmp = tempfile::tempdir().unwrap();
        let work = tmp.path().join("work");
        std::fs::create_dir(&work).unwrap();
        let link = tmp.path().join("innocent");
        std::os::unix::fs::symlink(&work, &link).unwrap();
        let roots = vec![("app \"pa\" working_dir".to_string(), work)];
        let msg = panic_of(|| assert_vault_disjoint(&link.join("vault"), &roots));
        assert!(msg.contains("is inside"), "{msg}");
    }

    #[test]
    fn vault_dir_and_providers_come_together() {
        let (mut config, apps) = config_with_every_root(Path::new("/srv/brenn"));
        assert!(resolve_oauth(&config, &apps).is_none());

        config.server.vault_dir = Some(PathBuf::from("/srv/vault"));
        let msg = panic_of(std::panic::AssertUnwindSafe(|| {
            resolve_oauth(&config, &apps);
        }));
        assert!(
            msg.contains("server.vault_dir is set") && msg.contains("no oauth_provider"),
            "{msg}"
        );

        config.server.vault_dir = None;
        config
            .oauth_providers
            .insert("google".to_string(), raw(PathBuf::from("/nonexistent")));
        let msg = panic_of(std::panic::AssertUnwindSafe(|| {
            resolve_oauth(&config, &apps);
        }));
        assert!(
            msg.contains("`google`") && msg.contains("server.vault_dir is not set"),
            "{msg}"
        );
    }

    #[test]
    fn resolve_oauth_reads_every_provider_under_a_valid_vault() {
        let tmp = tempfile::tempdir().unwrap();
        let (mut config, apps) = config_with_every_root(tmp.path());
        let vault = tmp.path().join("vault");
        std::fs::create_dir(&vault).unwrap();
        std::fs::set_permissions(&vault, std::fs::Permissions::from_mode(0o700)).unwrap();
        let file = write_private(tmp.path(), "client.json", &web_client(CALLBACK));
        config.server.vault_dir = Some(vault.clone());
        config
            .oauth_providers
            .insert("google".to_string(), raw(file));
        let resolved = resolve_oauth(&config, &apps).expect("a provider is declared");
        assert_eq!(resolved.vault_dir, std::fs::canonicalize(&vault).unwrap());
        assert_eq!(
            resolved.providers.keys().collect::<Vec<_>>(),
            vec!["google"]
        );
    }

    /// A vault named through a symlink inside an agent's home resolves to the
    /// target, so the running vault never follows the link the agent can
    /// replace.
    #[test]
    fn a_vault_named_through_a_symlink_runs_on_its_target() {
        let tmp = tempfile::tempdir().unwrap();
        let (mut config, apps) = config_with_every_root(tmp.path());
        let home = tmp.path().join("home");
        std::fs::create_dir(&home).unwrap();
        let real = tmp.path().join("real-vault");
        std::fs::create_dir(&real).unwrap();
        std::fs::set_permissions(&real, std::fs::Permissions::from_mode(0o700)).unwrap();
        let link = home.join("vault");
        std::os::unix::fs::symlink(&real, &link).unwrap();
        let file = write_private(tmp.path(), "client.json", &web_client(CALLBACK));
        config.server.vault_dir = Some(link.clone());
        config
            .oauth_providers
            .insert("google".to_string(), raw(file));

        let resolved = resolve_oauth(&config, &apps).expect("a provider is declared");
        let target = std::fs::canonicalize(&real).unwrap();
        assert_eq!(resolved.vault_dir, target);

        // The agent replaces the link with a directory of its own; the vault
        // path the server holds still names the target.
        std::fs::remove_file(&link).unwrap();
        std::fs::create_dir(&link).unwrap();
        assert_eq!(std::fs::canonicalize(&resolved.vault_dir).unwrap(), target);
    }

    #[test]
    fn the_app_level_disjointness_check_reads_the_configured_vault() {
        let tmp = tempfile::tempdir().unwrap();
        let (mut config, mut apps) = config_with_every_root(tmp.path());
        assert_vault_disjoint_from_apps(&config, &apps);

        config.server.vault_dir = Some(tmp.path().join("vault"));
        assert_vault_disjoint_from_apps(&config, &apps);

        let spawn = apps
            .get_mut("pa")
            .unwrap()
            .container_spawn
            .as_mut()
            .unwrap();
        spawn
            .extra_mounts
            .push(format!("{}:/data", tmp.path().display()));
        let msg = panic_of(std::panic::AssertUnwindSafe(|| {
            assert_vault_disjoint_from_apps(&config, &apps);
        }));
        assert!(
            msg.contains("is inside") && msg.contains("extra_mounts entry"),
            "{msg}"
        );
    }
}
