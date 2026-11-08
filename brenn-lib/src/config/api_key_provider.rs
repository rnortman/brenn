//! API-key providers — services Brenn sends a key to on the caller's behalf.
//!
//! A provider holds either the deployment's key — one `0600` host file, sent
//! for every caller — or named accounts, each a user's key in its own `0600`
//! file, chosen by the acting user and the call's `account`. Every key is read
//! once at boot and held as a [`SecretString`]. Brenn attaches it to a request
//! as the value of the header the block names; it is never passed into a
//! container.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

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

/// Where an `api_key_provider`'s keys come from, as the document states it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ApiKeySourceRaw {
    /// `key_file`: one key, the deployment's, sent for every caller.
    Deployment { key_file: PathBuf },
    /// `account` blocks: each a user's key under a name. Non-empty; names
    /// unique within the provider; at most one default per user.
    Accounts(Vec<ApiKeyAccountRaw>),
}

/// One `account` block of an `api_key_provider`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApiKeyAccountRaw {
    /// The block's name: what a call passes as `account` and a clause names
    /// under `account`.
    pub name: String,
    /// The Brenn username whose key this is.
    pub user: String,
    /// Host path to the file holding the key. 0600; read once at boot.
    pub key_file: PathBuf,
    /// The account an omitted `account` resolves to for `user`.
    pub default: bool,
}

/// One `api_key_provider` block, as the document states it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApiKeyProviderRaw {
    pub source: ApiKeySourceRaw,
    /// The header the key is the whole value of, as the block writes it.
    pub header: String,
    /// The origins (`scheme://host[:port]`) requests carrying the key may be
    /// sent to. Non-empty.
    pub api_hosts: Vec<String>,
    /// The most response body bytes a keyed request reads.
    pub max_response_bytes: usize,
}

/// A provider's keys, read.
#[derive(Clone, Debug)]
pub enum ApiKeys {
    /// The deployment's one key.
    Deployment(SecretString),
    /// Each user's named accounts.
    Accounts(AccountKeys),
}

/// A per-user provider's keys, keyed by username.
#[derive(Clone, Debug)]
pub struct AccountKeys(BTreeMap<String, UserAccounts>);

/// One user's accounts on one provider.
#[derive(Clone, Debug)]
struct UserAccounts {
    /// Keyed by account name.
    keys: BTreeMap<String, SecretString>,
    /// The account marked `default`, if one is.
    default: Option<String>,
}

/// Which of a user's accounts a call uses.
#[derive(Debug)]
pub enum AccountLookup<'a> {
    Found {
        account: &'a str,
        key: &'a SecretString,
    },
    /// The provider holds no account for this user.
    NoUser,
    /// The named account is not one of the user's; `available` lists theirs.
    NoSuchAccount { available: Vec<&'a str> },
    /// Several accounts, none the default, none named.
    Ambiguous { available: Vec<&'a str> },
}

impl AccountKeys {
    /// The keys of `accounts`, each `account` block with its key: grouped by
    /// user, a block marked `default` its user's default.
    pub fn new<'a>(
        accounts: impl IntoIterator<Item = (&'a ApiKeyAccountRaw, SecretString)>,
    ) -> AccountKeys {
        let mut users: BTreeMap<String, UserAccounts> = BTreeMap::new();
        for (account, key) in accounts {
            let user = users
                .entry(account.user.clone())
                .or_insert_with(|| UserAccounts {
                    keys: BTreeMap::new(),
                    default: None,
                });
            user.keys.insert(account.name.clone(), key);
            if account.default {
                user.default = Some(account.name.clone());
            }
        }
        AccountKeys(users)
    }

    /// The account `account` names for `user`, or the user's default or only
    /// one when `account` is `None`.
    pub fn lookup(&self, user: &str, account: Option<&str>) -> AccountLookup<'_> {
        self.0
            .get(user)
            .map_or(AccountLookup::NoUser, |accounts| accounts.lookup(account))
    }
}

impl UserAccounts {
    /// The account `account` names, or with `None` the default, else the only
    /// one.
    fn lookup(&self, account: Option<&str>) -> AccountLookup<'_> {
        let available = || self.keys.keys().map(String::as_str).collect();
        let name = match account {
            Some(name) => name,
            None => match (&self.default, self.keys.len()) {
                (Some(default), _) => default.as_str(),
                (None, 1) => self.keys.keys().next().expect("one account").as_str(),
                (None, _) => {
                    return AccountLookup::Ambiguous {
                        available: available(),
                    };
                }
            },
        };
        match self.keys.get_key_value(name) {
            Some((account, key)) => AccountLookup::Found { account, key },
            None => AccountLookup::NoSuchAccount {
                available: available(),
            },
        }
    }
}

/// One key provider with its keys in hand.
#[derive(Clone, Debug)]
pub struct ApiKeyProviderConfig {
    pub raw: ApiKeyProviderRaw,
    pub keys: ApiKeys,
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

/// Every user an `account` block names, once each, in order.
pub fn api_key_account_users(config: &BrennConfig) -> Vec<String> {
    let users: std::collections::BTreeSet<&str> = account_bindings(config)
        .map(|(_, account)| account.user.as_str())
        .collect();
    users.into_iter().map(str::to_string).collect()
}

/// Hold every `account` block's `user` to the registered users.
///
/// # Panics
///
/// An account names a user `registered` does not hold; the message names the
/// provider, the account and the user.
pub fn assert_api_key_account_users_registered(config: &BrennConfig, registered: &[String]) {
    for (provider, account) in account_bindings(config) {
        assert!(
            registered.contains(&account.user),
            "config: api_key_provider `{provider}` account `{}` names user {:?}, who is not \
             registered; register the user, then restart Brenn",
            account.name,
            account.user,
        );
    }
}

/// Every `account` block, with the name of the provider holding it.
fn account_bindings(config: &BrennConfig) -> impl Iterator<Item = (&str, &ApiKeyAccountRaw)> {
    config
        .api_key_providers
        .iter()
        .filter_map(|(name, raw)| match &raw.source {
            ApiKeySourceRaw::Deployment { .. } => None,
            ApiKeySourceRaw::Accounts(accounts) => Some((name.as_str(), accounts)),
        })
        .flat_map(|(name, accounts)| accounts.iter().map(move |account| (name, account)))
}

fn resolve_provider(name: &str, raw: &ApiKeyProviderRaw) -> ApiKeyProviderConfig {
    let label = format!("api_key_provider `{name}`");
    assert_max_response_bytes(&label, raw.max_response_bytes);
    let api_origins = resolve_api_origins(&label, &raw.api_hosts);
    let header = resolve_header(&label, &raw.header);
    let keys = match &raw.source {
        ApiKeySourceRaw::Deployment { key_file } => ApiKeys::Deployment(load_key(&label, key_file)),
        ApiKeySourceRaw::Accounts(accounts) => {
            ApiKeys::Accounts(AccountKeys::new(accounts.iter().map(|account| {
                let key = load_key(
                    &format!("{label} account `{}`", account.name),
                    &account.key_file,
                );
                (account, key)
            })))
        }
    };
    ApiKeyProviderConfig {
        raw: raw.clone(),
        keys,
        header,
        api_origins,
    }
}

/// Read one key file. `label` names the provider, and the account when the key
/// is one; a refusal names the file, never the key.
fn load_key(label: &str, key_file: &Path) -> SecretString {
    // TODO(security-key-file-not-checked-disjoint-from-agent-visible-paths):
    // only the file's mode is checked; nothing refuses a key file inside a
    // path an agent can reach.
    let key = load_secret_file_private(label, key_file);
    // Visible ASCII and space: every such string is a valid header value, and
    // a byte outside it (a BOM, a control or non-ASCII character pasted in) is
    // a damaged file. The message names the file, never the key.
    assert!(
        key.bytes().all(|b| b == b' ' || b.is_ascii_graphic()),
        "config: {label}: the key in {} holds a byte that is not visible ASCII or a space \
         (a control or non-ASCII character, such as a byte-order mark)",
        key_file.display(),
    );
    SecretString::new(key)
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
            source: ApiKeySourceRaw::Deployment { key_file },
            ..test_api_key_provider_raw("https://api.example.com")
        }
    }

    fn deployment_key(resolved: &ApiKeyProviderConfig) -> &str {
        let ApiKeys::Deployment(key) = &resolved.keys else {
            panic!("a deployment key");
        };
        key.expose()
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
        assert_eq!(deployment_key(&resolved), "k3y-value");
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
        assert_eq!(deployment_key(&resolved), "Bearer k3y-value");
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

    fn write_named_key(dir: &Path, name: &str, text: &str, mode: u32) -> PathBuf {
        let path = dir.join(name);
        std::fs::write(&path, text).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).unwrap();
        path
    }

    fn account(name: &str, user: &str, key_file: PathBuf, default: bool) -> ApiKeyAccountRaw {
        ApiKeyAccountRaw {
            name: name.to_string(),
            user: user.to_string(),
            key_file,
            default,
        }
    }

    fn accounts_raw(accounts: Vec<ApiKeyAccountRaw>) -> ApiKeyProviderRaw {
        ApiKeyProviderRaw {
            source: ApiKeySourceRaw::Accounts(accounts),
            ..test_api_key_provider_raw("https://api.example.com")
        }
    }

    /// alice: `alice` (default) and `alice-work`; bob: `bob`; dan: `dan-home`
    /// and `dan-work`, no default.
    fn accounts_fixture(dir: &Path) -> ApiKeyProviderConfig {
        let key = |name: &str| write_named_key(dir, name, &format!("k3y-{name}\n"), 0o600);
        resolve_provider(
            "mail",
            &accounts_raw(vec![
                account("alice", "alice", key("alice"), true),
                account("alice-work", "alice", key("alice-work"), false),
                account("bob", "bob", key("bob"), false),
                account("dan-home", "dan", key("dan-home"), false),
                account("dan-work", "dan", key("dan-work"), false),
            ]),
        )
    }

    fn account_keys(resolved: ApiKeyProviderConfig) -> AccountKeys {
        let ApiKeys::Accounts(keys) = resolved.keys else {
            panic!("per-user accounts");
        };
        keys
    }

    fn found<'a>(lookup: AccountLookup<'a>) -> (&'a str, &'a str) {
        match lookup {
            AccountLookup::Found { account, key } => (account, key.expose()),
            other => panic!("expected an account, got {other:?}"),
        }
    }

    #[test]
    fn each_account_key_is_read_under_its_user() {
        let tmp = tempfile::tempdir().unwrap();
        let AccountKeys(users) = account_keys(accounts_fixture(tmp.path()));
        assert_eq!(users.keys().collect::<Vec<_>>(), ["alice", "bob", "dan"]);
        assert_eq!(users["alice"].default.as_deref(), Some("alice"));
        assert_eq!(users["bob"].default, None);
        assert_eq!(
            users["alice"].keys.keys().collect::<Vec<_>>(),
            ["alice", "alice-work"]
        );
        assert_eq!(users["alice"].keys["alice-work"].expose(), "k3y-alice-work");
    }

    #[test]
    fn lookup_picks_the_named_the_default_or_the_only_account() {
        let tmp = tempfile::tempdir().unwrap();
        let keys = account_keys(accounts_fixture(tmp.path()));
        assert_eq!(found(keys.lookup("alice", None)), ("alice", "k3y-alice"));
        assert_eq!(
            found(keys.lookup("alice", Some("alice-work"))),
            ("alice-work", "k3y-alice-work")
        );
        assert_eq!(found(keys.lookup("bob", None)), ("bob", "k3y-bob"));
        assert_eq!(
            found(keys.lookup("dan", Some("dan-work"))),
            ("dan-work", "k3y-dan-work")
        );
    }

    #[test]
    fn lookup_refuses_an_absent_user_a_foreign_account_and_an_ambiguous_omission() {
        let tmp = tempfile::tempdir().unwrap();
        let keys = account_keys(accounts_fixture(tmp.path()));
        assert!(matches!(keys.lookup("carol", None), AccountLookup::NoUser));
        // Another user's account is not one of hers, whatever its name.
        match keys.lookup("alice", Some("bob")) {
            AccountLookup::NoSuchAccount { available } => {
                assert_eq!(available, ["alice", "alice-work"]);
            }
            other => panic!("{other:?}"),
        }
        match keys.lookup("dan", None) {
            AccountLookup::Ambiguous { available } => {
                assert_eq!(available, ["dan-home", "dan-work"]);
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn no_account_key_reaches_debug_output() {
        let tmp = tempfile::tempdir().unwrap();
        let resolved = accounts_fixture(tmp.path());
        assert!(!format!("{resolved:?}").contains("k3y"));
    }

    #[test]
    fn a_bad_account_key_file_is_refused_naming_provider_account_and_file() {
        let tmp = tempfile::tempdir().unwrap();
        let good = write_named_key(tmp.path(), "good", "k3y-good", 0o600);
        for (file, expect) in [
            (tmp.path().join("absent"), "failed to read"),
            (
                write_named_key(tmp.path(), "shared", "k3y-shared", 0o640),
                "group/world-accessible",
            ),
            (
                write_named_key(tmp.path(), "control", "k3y\u{7}value", 0o600),
                "not visible ASCII or a space",
            ),
        ] {
            let raw = accounts_raw(vec![
                account("alice", "alice", good.clone(), false),
                account("alice-work", "alice", file.clone(), false),
            ]);
            let msg = resolve_panic(raw);
            assert!(
                msg.contains("api_key_provider `maps` account `alice-work`")
                    && msg.contains(&file.display().to_string())
                    && msg.contains(expect),
                "{expect}: {msg}"
            );
            assert!(!msg.contains("k3y"), "{msg}");
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

    /// A deployment-key provider `maps` beside a per-user provider `mail`
    /// holding bob's account and two of alice's.
    fn account_users_config() -> BrennConfig {
        let mut config = BrennConfig::default();
        config
            .api_key_providers
            .insert("maps".to_string(), raw(PathBuf::from("/keys/maps")));
        config.api_key_providers.insert(
            "mail".to_string(),
            accounts_raw(vec![
                account("bob", "bob", PathBuf::from("/keys/bob"), false),
                account("alice", "alice", PathBuf::from("/keys/alice"), true),
                account("alice-work", "alice", PathBuf::from("/keys/aw"), false),
            ]),
        );
        config
    }

    #[test]
    fn account_users_are_each_named_user_once() {
        assert_eq!(
            api_key_account_users(&account_users_config()),
            ["alice", "bob"]
        );
        assert!(api_key_account_users(&BrennConfig::default()).is_empty());
    }

    #[test]
    fn registered_account_users_pass() {
        assert_api_key_account_users_registered(
            &account_users_config(),
            &["carol".to_string(), "bob".to_string(), "alice".to_string()],
        );
    }

    #[test]
    fn an_account_naming_an_unregistered_user_is_refused_by_name() {
        let config = account_users_config();
        let msg = panic_of(move || {
            assert_api_key_account_users_registered(&config, &["alice".to_string()]);
        });
        assert!(
            msg.contains("api_key_provider `mail` account `bob` names user \"bob\"")
                && msg.contains("not registered"),
            "{msg}"
        );
    }
}
