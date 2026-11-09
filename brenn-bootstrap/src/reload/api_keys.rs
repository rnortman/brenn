//! What a reload does to the key table `api-request` reads.
//!
//! Every `api_key_provider`'s key files are read off the host on every reload,
//! as a fresh boot reads them, and the resolved table is compared against the
//! one the tool is reading now. Commit installs the candidate's table whole;
//! a call already executing keeps the snapshot it took.

use std::collections::BTreeMap;
use std::sync::Arc;

use brenn_lib::config::ApiKeyProviderConfig;

/// Which key providers a reload installs, drops or replaces. The baseline is
/// the table `api-request` is reading now; both sides are compared in
/// resolved form — header, origins, size limit and the bytes of every key —
/// so a rotated key file under an unmoved document is a change and a
/// re-spelled block that resolves to the same provider is not.
#[derive(Default)]
pub(crate) struct ApiKeyDelta {
    /// Provider names the candidate declares and the live table lacks, in
    /// name order.
    pub added: Vec<String>,
    /// Provider names the live table holds and the candidate does not
    /// declare, in name order.
    pub removed: Vec<String>,
    /// Provider names on both sides whose resolved value moved, in name order.
    pub changed: Vec<String>,
    /// The whole candidate table: commit stores it as one value.
    pub candidate: Arc<BTreeMap<String, ApiKeyProviderConfig>>,
}

impl ApiKeyDelta {
    pub fn is_empty(&self) -> bool {
        self.added.is_empty() && self.removed.is_empty() && self.changed.is_empty()
    }
}

/// Which providers moved, comparing the live table against the candidate's
/// resolved providers.
pub(crate) fn api_keys_delta(
    live: &BTreeMap<String, ApiKeyProviderConfig>,
    candidate: Arc<BTreeMap<String, ApiKeyProviderConfig>>,
) -> ApiKeyDelta {
    let mut delta = ApiKeyDelta::default();
    for (name, provider) in candidate.iter() {
        match live.get(name) {
            None => delta.added.push(name.clone()),
            Some(running) if running != provider => delta.changed.push(name.clone()),
            Some(_) => {}
        }
    }
    delta.removed = live
        .keys()
        .filter(|name| !candidate.contains_key(*name))
        .cloned()
        .collect();
    delta.candidate = candidate;
    delta
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;
    use std::path::{Path, PathBuf};

    use brenn_lib::config::{
        ApiKeyAccountRaw, ApiKeyProviderRaw, ApiKeySourceRaw, BrennConfig,
        resolve_api_key_providers, test_api_key_provider_raw,
    };

    use super::*;

    /// Write `text` to `dir/name` at 0600, creating `dir`.
    fn key(dir: &Path, name: &str, text: &str) -> PathBuf {
        std::fs::create_dir_all(dir).unwrap();
        let path = dir.join(name);
        std::fs::write(&path, text).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        path
    }

    fn deployment(key_file: PathBuf) -> ApiKeyProviderRaw {
        ApiKeyProviderRaw {
            source: ApiKeySourceRaw::Deployment { key_file },
            ..test_api_key_provider_raw("https://api.example.com")
        }
    }

    /// A per-user provider with alice's and bob's accounts, keys under `dir`.
    fn two_accounts(dir: &Path) -> ApiKeyProviderRaw {
        let account = |name: &str| ApiKeyAccountRaw {
            name: name.to_string(),
            user: name.to_string(),
            key_file: key(dir, name, &format!("k3y-{name}")),
            default: false,
        };
        ApiKeyProviderRaw {
            source: ApiKeySourceRaw::Accounts(vec![account("alice"), account("bob")]),
            ..test_api_key_provider_raw("https://api.example.com")
        }
    }

    fn resolved(
        providers: impl IntoIterator<Item = (&'static str, ApiKeyProviderRaw)>,
    ) -> BTreeMap<String, ApiKeyProviderConfig> {
        resolve_api_key_providers(&BrennConfig {
            api_key_providers: providers
                .into_iter()
                .map(|(name, raw)| (name.to_string(), raw))
                .collect(),
            ..BrennConfig::default()
        })
    }

    fn delta(
        live: &BTreeMap<String, ApiKeyProviderConfig>,
        candidate: BTreeMap<String, ApiKeyProviderConfig>,
    ) -> ApiKeyDelta {
        api_keys_delta(live, Arc::new(candidate))
    }

    #[test]
    fn added_and_removed_providers_are_named_in_name_order() {
        let tmp = tempfile::tempdir().unwrap();
        let file = |name: &str| key(tmp.path(), name, "k3y");
        let live = resolved([("maps", deployment(file("maps")))]);
        let moved = delta(
            &live,
            resolved([
                ("zebra", deployment(file("zebra"))),
                ("alpha", deployment(file("alpha"))),
            ]),
        );
        assert_eq!(moved.added, ["alpha", "zebra"]);
        assert_eq!(moved.removed, ["maps"]);
        assert!(moved.changed.is_empty());
        assert!(!moved.is_empty());
        assert_eq!(
            moved.candidate.keys().collect::<Vec<_>>(),
            ["alpha", "zebra"],
            "the candidate is carried whole for commit to store",
        );
    }

    #[test]
    fn a_key_rotated_in_place_is_a_change() {
        let tmp = tempfile::tempdir().unwrap();
        let path = key(tmp.path(), "maps", "k3y-one");
        let live = resolved([("maps", deployment(path.clone()))]);
        key(tmp.path(), "maps", "k3y-two");
        let moved = delta(&live, resolved([("maps", deployment(path))]));
        assert_eq!(moved.changed, ["maps"]);
        assert!(moved.added.is_empty() && moved.removed.is_empty());
    }

    #[test]
    fn a_rotated_account_key_is_a_change() {
        let tmp = tempfile::tempdir().unwrap();
        let raw = two_accounts(tmp.path());
        let live = resolved([("mail", raw.clone())]);
        key(tmp.path(), "bob", "k3y-bob-rotated");
        assert_eq!(delta(&live, resolved([("mail", raw)])).changed, ["mail"]);
    }

    #[test]
    fn a_moved_header_origin_or_size_limit_is_a_change() {
        let tmp = tempfile::tempdir().unwrap();
        let path = key(tmp.path(), "maps", "k3y");
        let live = resolved([("maps", deployment(path.clone()))]);
        let edits: [fn(&mut ApiKeyProviderRaw); 3] = [
            |raw| raw.header = "x-other-key".to_string(),
            |raw| raw.api_hosts.push("https://other.example.com".to_string()),
            |raw| raw.max_response_bytes -= 1,
        ];
        for edit in edits {
            let mut raw = deployment(path.clone());
            edit(&mut raw);
            assert_eq!(delta(&live, resolved([("maps", raw)])).changed, ["maps"]);
        }
    }

    #[test]
    fn reordered_api_hosts_are_a_change() {
        let tmp = tempfile::tempdir().unwrap();
        let path = key(tmp.path(), "maps", "k3y");
        let hosts = |hosts: &[&str]| ApiKeyProviderRaw {
            api_hosts: hosts.iter().map(|host| host.to_string()).collect(),
            ..deployment(path.clone())
        };
        let a = "https://a.example.com";
        let b = "https://b.example.com";
        let live = resolved([("maps", hosts(&[a, b]))]);
        assert_eq!(
            delta(&live, resolved([("maps", hosts(&[b, a]))])).changed,
            ["maps"]
        );
    }

    /// A re-cased header, reordered accounts and keys re-pathed to files
    /// holding the same bytes resolve to the same provider: no delta.
    #[test]
    fn a_respelled_provider_is_no_change() {
        let tmp = tempfile::tempdir().unwrap();
        let live = resolved([("mail", two_accounts(&tmp.path().join("a")))]);
        assert!(
            delta(
                &live,
                resolved([("mail", two_accounts(&tmp.path().join("a")))])
            )
            .is_empty()
        );

        let mut recased = two_accounts(&tmp.path().join("a"));
        recased.header = recased.header.to_uppercase();
        assert!(delta(&live, resolved([("mail", recased)])).is_empty());

        let mut reordered = two_accounts(&tmp.path().join("a"));
        let ApiKeySourceRaw::Accounts(accounts) = &mut reordered.source else {
            unreachable!("two_accounts is per-user")
        };
        accounts.reverse();
        assert!(delta(&live, resolved([("mail", reordered)])).is_empty());

        let repathed = two_accounts(&tmp.path().join("b"));
        assert!(delta(&live, resolved([("mail", repathed)])).is_empty());
    }
}
