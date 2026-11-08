//! A Google Workspace instance's `config` map: the `oauth_provider` and the
//! one host its component calls.

use brenn_provider_kit::{host, required};

/// The instance's `config` map, validated.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Settings {
    /// The `oauth_provider` every call names.
    pub provider: String,
    /// The `host[:port]` every call goes to: Gmail's or Calendar's, read
    /// under the key the component names.
    pub host: String,
}

impl Settings {
    /// Read the map through `get`, the host under `host_key` (`gmail_host` or
    /// `calendar_host`). A missing key, an empty value, or a host containing
    /// `/` is an error naming the key: nothing a misconfigured instance
    /// answered would be right.
    pub fn from_lookup(
        get: impl Fn(&str) -> Option<String>,
        host_key: &str,
    ) -> Result<Settings, String> {
        Ok(Settings {
            provider: required(&get, "provider")?,
            host: host(&get, host_key)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::lookup;

    /// Each host key with a host it might name.
    const KEYS: [(&str, &str); 2] = [
        ("gmail_host", "gmail.example.com"),
        ("calendar_host", "calendar.example.com"),
    ];

    fn base(key: &'static str, host: &'static str) -> Vec<(&'static str, &'static str)> {
        vec![("provider", "google"), (key, host)]
    }

    fn with(
        pairs: &[(&'static str, &'static str)],
        key: &'static str,
        value: &'static str,
    ) -> Vec<(&'static str, &'static str)> {
        let mut pairs: Vec<_> = pairs.iter().filter(|(k, _)| *k != key).copied().collect();
        pairs.push((key, value));
        pairs
    }

    #[test]
    fn settings_read_every_key() {
        for (key, value) in KEYS {
            assert_eq!(
                Settings::from_lookup(lookup(&base(key, value)), key).unwrap(),
                Settings {
                    provider: "google".to_string(),
                    host: value.to_string(),
                }
            );
            let s =
                Settings::from_lookup(lookup(&with(&base(key, value), key, "127.0.0.1:8080")), key)
                    .unwrap();
            assert_eq!(s.host, "127.0.0.1:8080");
        }
        // Only the named host key is read: the other one is neither needed
        // nor taken.
        let both = [
            ("provider", "google"),
            ("gmail_host", "gmail.example.com"),
            ("calendar_host", "calendar.example.com"),
        ];
        assert_eq!(
            Settings::from_lookup(lookup(&both), "calendar_host")
                .unwrap()
                .host,
            "calendar.example.com"
        );
    }

    #[test]
    fn settings_refuse_a_missing_key_by_name() {
        for (host_key, value) in KEYS {
            let pairs = base(host_key, value);
            for key in ["provider", host_key] {
                let missing: Vec<_> = pairs.iter().filter(|(k, _)| *k != key).copied().collect();
                let err = Settings::from_lookup(lookup(&missing), host_key).unwrap_err();
                assert_eq!(err, format!("config {key} is required"));
            }
        }
    }

    #[test]
    fn settings_refuse_bad_values() {
        for (host_key, value) in KEYS {
            let pairs = base(host_key, value);
            for key in ["provider", host_key] {
                let err =
                    Settings::from_lookup(lookup(&with(&pairs, key, "")), host_key).unwrap_err();
                assert_eq!(err, format!("config {key} is empty"));
            }
        }
        let pairs = base("gmail_host", "gmail.example.com");
        let err = Settings::from_lookup(
            lookup(&with(&pairs, "gmail_host", "gmail.example.com/gmail")),
            "gmail_host",
        )
        .unwrap_err();
        assert_eq!(
            err,
            "config gmail_host: \"gmail.example.com/gmail\" is not a host[:port]; it contains `/`"
        );
        let pairs = base("calendar_host", "calendar.example.com");
        let err = Settings::from_lookup(
            lookup(&with(&pairs, "calendar_host", "www.example.com/calendar")),
            "calendar_host",
        )
        .unwrap_err();
        assert_eq!(
            err,
            "config calendar_host: \"www.example.com/calendar\" is not a host[:port]; it \
             contains `/`"
        );
    }

    /// The guide's instances, by opening line, with the host key each
    /// component reads and the host the guide gives it.
    const GUIDE_INSTANCES: [(&str, &str, &str); 5] = [
        (
            "new gmail-read: GoogleGmailRead {",
            "gmail_host",
            "gmail.googleapis.com",
        ),
        (
            "new gmail-compose: GoogleGmailCompose {",
            "gmail_host",
            "gmail.googleapis.com",
        ),
        (
            "new gmail-organize: GoogleGmailOrganize {",
            "gmail_host",
            "gmail.googleapis.com",
        ),
        (
            "new calendar-read: GoogleCalendarRead {",
            "calendar_host",
            "www.googleapis.com",
        ),
        (
            "new calendar-write: GoogleCalendarWrite {",
            "calendar_host",
            "www.googleapis.com",
        ),
    ];

    /// The `config` map of each Google Workspace instance example in
    /// `docs/google-workspace.md` — the first `config = {` after that
    /// instance's opening line — is one this parser accepts under the key its
    /// component reads, with the values the guide states.
    #[test]
    fn the_google_workspace_guide_config_map_parses() {
        let path =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../docs/google-workspace.md");
        let doc = std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("{} must be readable: {error}", path.display()));
        for (instance, host_key, expected_host) in GUIDE_INSTANCES {
            let mut lines = doc
                .lines()
                .map(str::trim)
                .skip_while(|line| *line != instance);
            assert_eq!(
                lines.next(),
                Some(instance),
                "docs/google-workspace.md carries no `{instance}` line"
            );
            let mut lines = lines.skip_while(|line| *line != "config = {");
            assert_eq!(
                lines.next(),
                Some("config = {"),
                "the guide's `{instance}` instance carries no `config = {{` line"
            );
            let pairs: Vec<(String, String)> = lines
                .take_while(|line| *line != "};")
                .map(|line| {
                    let (key, value) = line
                        .trim_end_matches(',')
                        .split_once(" = ")
                        .unwrap_or_else(|| panic!("{line:?} is not `key = \"value\"`"));
                    let value = value
                        .strip_prefix('"')
                        .and_then(|v| v.strip_suffix('"'))
                        .unwrap_or_else(|| panic!("{line:?} has no quoted value"));
                    (key.to_string(), value.to_string())
                })
                .collect();
            let pairs: Vec<(&str, &str)> = pairs
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect();
            let s = Settings::from_lookup(lookup(&pairs), host_key).unwrap_or_else(|error| {
                panic!("`{instance}`'s config map fails every activation: {error}")
            });
            assert_eq!(s.provider, "google", "`{instance}`'s provider");
            assert_eq!(s.host, expected_host, "`{instance}`'s {host_key}");
        }
    }
}
