//! Claude account profiles: a name bound to a credential file — a
//! `claude setup-token` token or an Anthropic API key.
//!
//! An account, to Claude Code, is an environment variable —
//! `CLAUDE_CODE_OAUTH_TOKEN` or `ANTHROPIC_API_KEY`, either of which outranks
//! the `/login` credential in the shared `~/.claude`. So a profile is a name
//! bound to a credential file, the shared config root is never touched, and
//! switching accounts is spawning a process with a different value in that
//! variable.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use brenn_envelope::agent::AccountKind;
use chrono::NaiveDate;

use super::secret::{SecretString, load_secret_file_private};

/// The file name a `claude_profile` block without a credential file looks for
/// under `claude_defaults.profile_token_dir`.
pub fn default_token_file_name(profile: &str) -> String {
    format!("claude-profile-{profile}.token")
}

/// The environment variable Claude Code reads a `claude setup-token` token from.
pub const CLAUDE_OAUTH_TOKEN_VAR: &str = "CLAUDE_CODE_OAUTH_TOKEN";

/// The environment variable Claude Code reads an Anthropic API key from.
pub const ANTHROPIC_API_KEY_VAR: &str = "ANTHROPIC_API_KEY";

/// The `result` text a key profile's refused turn is recognised by when the
/// profile states no `exhausted_markers`.
pub const DEFAULT_EXHAUSTED_MARKERS: [&str; 1] = ["credit balance is too low"];

/// The credential-selecting environment variables no profiled agent may carry
/// from anywhere but its profile: the five Claude Code ranks *above* the token,
/// plus the token variable itself. Any of them could win silently over the
/// profile, or sit beside it as a second credential, so every place Brenn can
/// see one refuses to run — an agent's own `env` at lowering, an integration's
/// contribution at the spawn config build, and the server's own environment
/// (which a bare child inherits wholesale) at boot. The rule is the same for a
/// key profile: a profiled agent has exactly one credential in its
/// environment, the one its profile put there.
pub const OUTRANKING_CREDENTIAL_VARS: [&str; 6] = [
    ANTHROPIC_API_KEY_VAR,
    "ANTHROPIC_AUTH_TOKEN",
    CLAUDE_OAUTH_TOKEN_VAR,
    "CLAUDE_CODE_USE_BEDROCK",
    "CLAUDE_CODE_USE_VERTEX",
    "CLAUDE_CODE_USE_FOUNDRY",
];

const EXPIRY_WARNING_DAYS: i64 = 30;

/// Where a profile's credential is read from, by kind, with what only that kind
/// states.
#[derive(Clone, Debug, PartialEq)]
pub enum ClaudeCredentialFile {
    /// A `claude setup-token` token file.
    OauthToken(PathBuf),
    /// An Anthropic API key file.
    ApiKey {
        path: PathBuf,
        settings: KeyProfileSettings,
    },
}

impl ClaudeCredentialFile {
    /// The host path, whichever the kind.
    pub fn path(&self) -> &Path {
        match self {
            ClaudeCredentialFile::OauthToken(path) | ClaudeCredentialFile::ApiKey { path, .. } => {
                path
            }
        }
    }
}

/// What only an API-key profile states: how its spend is accounted and how a
/// refused turn is recognised.
#[derive(Clone, Debug, PartialEq)]
pub struct KeyProfileSettings {
    /// The month's spend the operator budgets for this key. Finite and above
    /// zero. Reported, never enforced: the Platform spend limit is the hard
    /// stop.
    pub budget_usd: Option<f64>,
    /// The zone whose calendar month is the accounting period.
    pub reset_tz: chrono_tz::Tz,
    /// Substrings, matched ASCII case-insensitively, of the `result` text of a
    /// turn refused for lack of credit. Non-empty, no marker empty.
    pub exhausted_markers: Vec<String>,
}

/// One `claude_profile` block, as the document states it.
///
/// The path is final by the time this exists: lowering has already applied the
/// `profile_token_dir` convention to a block that stated no credential file.
#[derive(Clone, Debug, PartialEq)]
pub struct ClaudeProfileRaw {
    /// Host path to the credential file, by kind. 0600, one line.
    pub credential: ClaudeCredentialFile,
    /// The date the operator recorded the credential as ceasing to work. Brenn
    /// cannot read a credential's lifetime — the value is opaque — so this is
    /// the operator's note to itself and the only expiry signal there is.
    pub expires: Option<NaiveDate>,
}

/// Which accounts one agent may run under, and where its goal comes from.
///
/// Present exactly when the agent states `claude_profiles`. Absent, the agent
/// gets no credential at spawn and authenticates with whatever `/login` left in
/// its home — which is every agent in every deployment that declares no
/// profiles.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppClaudeProfiles {
    /// Declared profile names, in preference order. Non-empty, without
    /// repeats, and every entry a declared `claude_profile`. The first entry
    /// is what the agent runs under until a goal names another.
    pub allowed: Vec<String>,
    /// Canonical address of the retained channel whose latest message names
    /// the profile this agent should run under. Absent means the agent runs
    /// under the first allowed profile and never moves.
    pub goal: Option<String>,
}

/// A profile's credential, in hand, by kind, with what only that kind states.
#[derive(Clone, Debug, PartialEq)]
pub enum ClaudeCredential {
    OauthToken(SecretString),
    ApiKey {
        secret: SecretString,
        settings: KeyProfileSettings,
    },
}

impl ClaudeCredential {
    /// The environment variable Claude Code reads this credential from.
    pub fn env_var(&self) -> &'static str {
        match self {
            ClaudeCredential::OauthToken(_) => CLAUDE_OAUTH_TOKEN_VAR,
            ClaudeCredential::ApiKey { .. } => ANTHROPIC_API_KEY_VAR,
        }
    }

    /// The account kind, whose wire word is `subscription` or `api-key`.
    pub fn kind(&self) -> AccountKind {
        match self {
            ClaudeCredential::OauthToken(_) => AccountKind::Subscription,
            ClaudeCredential::ApiKey { .. } => AccountKind::ApiKey,
        }
    }

    /// The secret, whichever the kind.
    pub fn secret(&self) -> &SecretString {
        match self {
            ClaudeCredential::OauthToken(secret) | ClaudeCredential::ApiKey { secret, .. } => {
                secret
            }
        }
    }

    /// An API key's settings; `None` for a token.
    pub fn key_settings(&self) -> Option<&KeyProfileSettings> {
        match self {
            ClaudeCredential::OauthToken(_) => None,
            ClaudeCredential::ApiKey { settings, .. } => Some(settings),
        }
    }
}

/// One profile with its credential in hand.
#[derive(Clone, Debug, PartialEq)]
pub struct ClaudeProfile {
    pub credential: ClaudeCredential,
    pub expires: Option<NaiveDate>,
}

/// Load every declared profile's credential.
///
/// A declared profile whose credential file is missing, empty, unreadable, or
/// readable by any other local account is a misconfiguration, not a runtime
/// condition.
///
/// # Panics
///
/// Whatever [`load_secret_file_private`] panics on, with the profile name and
/// path in the message.
pub fn load_claude_profiles(
    raw: &BTreeMap<String, ClaudeProfileRaw>,
) -> BTreeMap<String, ClaudeProfile> {
    raw.iter()
        .map(|(name, profile)| {
            let label = format!("claude_profile `{name}`");
            let secret =
                SecretString::new(load_secret_file_private(&label, profile.credential.path()));
            let credential = match &profile.credential {
                ClaudeCredentialFile::OauthToken(_) => ClaudeCredential::OauthToken(secret),
                ClaudeCredentialFile::ApiKey { settings, .. } => ClaudeCredential::ApiKey {
                    secret,
                    settings: settings.clone(),
                },
            };
            (
                name.clone(),
                ClaudeProfile {
                    credential,
                    expires: profile.expires,
                },
            )
        })
        .collect()
}

/// The boot warning a profile's `expires` earns, if any.
///
/// A date already past or within thirty days of `today` is worth a human's
/// attention; anything further out is not, and a profile with no date recorded
/// says nothing either way. The remedy named is the one for the profile's
/// credential kind.
pub fn expiry_warning(name: &str, profile: &ClaudeProfile, today: NaiveDate) -> Option<String> {
    let expires = profile.expires?;
    let days = (expires - today).num_days();
    if days > EXPIRY_WARNING_DAYS {
        return None;
    }
    let remedy = match profile.credential {
        ClaudeCredential::OauthToken(_) => {
            "Mint a new token with `claude setup-token` and overwrite its token file."
        }
        ClaudeCredential::ApiKey { .. } => {
            "Rotate the key on the Anthropic Platform console and overwrite its key file."
        }
    };
    Some(if days < 0 {
        format!(
            "Claude profile `{name}` expired on {expires} ({} days ago). {remedy}",
            -days,
        )
    } else {
        format!("Claude profile `{name}` expires on {expires} (in {days} days). {remedy}")
    })
}

/// Every expiry warning a set of profiles has to give today, as
/// `(profile name, warning)` in name order.
///
/// One entry per profile that is past its recorded date or inside the warning
/// window, and none for the rest — a credential is opaque, so a date the
/// operator wrote down is the only expiry signal there is, and it is worth
/// exactly one alert per profile at boot.
pub fn expiry_alerts(
    profiles: &std::collections::BTreeMap<String, ClaudeProfile>,
    today: NaiveDate,
) -> Vec<(String, String)> {
    profiles
        .iter()
        .filter_map(|(name, profile)| {
            expiry_warning(name, profile, today).map(|warning| (name.clone(), warning))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;

    use super::*;

    fn profile(expires: Option<&str>) -> ClaudeProfile {
        ClaudeProfile {
            credential: ClaudeCredential::OauthToken(SecretString::new("tok".to_string())),
            expires: expires.map(|date| date.parse().unwrap()),
        }
    }

    fn key_profile(expires: Option<&str>) -> ClaudeProfile {
        ClaudeProfile {
            credential: ClaudeCredential::ApiKey {
                secret: SecretString::new("sk".to_string()),
                settings: settings(),
            },
            expires: expires.map(|date| date.parse().unwrap()),
        }
    }

    fn settings() -> KeyProfileSettings {
        KeyProfileSettings {
            budget_usd: None,
            reset_tz: chrono_tz::UTC,
            exhausted_markers: vec![DEFAULT_EXHAUSTED_MARKERS[0].to_string()],
        }
    }

    fn today() -> NaiveDate {
        "2026-09-03".parse().unwrap()
    }

    #[test]
    fn no_expiry_recorded_says_nothing() {
        assert!(expiry_warning("main", &profile(None), today()).is_none());
    }

    #[test]
    fn a_distant_expiry_says_nothing() {
        assert!(expiry_warning("main", &profile(Some("2027-09-01")), today()).is_none());
    }

    #[test]
    fn an_expiry_inside_the_window_names_the_profile_and_the_date() {
        let warning = expiry_warning("spare", &profile(Some("2026-09-20")), today()).unwrap();
        assert!(warning.contains("spare"), "{warning}");
        assert!(warning.contains("2026-09-20"), "{warning}");
        assert!(warning.contains("in 17 days"), "{warning}");
    }

    #[test]
    fn the_boundary_day_is_inside_the_window() {
        assert!(expiry_warning("main", &profile(Some("2026-10-03")), today()).is_some());
        assert!(expiry_warning("main", &profile(Some("2026-10-04")), today()).is_none());
    }

    #[test]
    fn a_past_expiry_says_so() {
        let warning = expiry_warning("legacy", &profile(Some("2026-08-01")), today()).unwrap();
        assert!(warning.contains("expired on 2026-08-01"), "{warning}");
        assert!(warning.contains("33 days ago"), "{warning}");
    }

    /// What boot dispatches: one alert per profile that needs one, naming that
    /// profile — not one per boot, and not one for a profile with years left.
    #[test]
    fn only_the_profiles_that_need_a_warning_get_one() {
        let profiles = BTreeMap::from([
            ("distant".to_string(), profile(Some("2027-09-01"))),
            ("dateless".to_string(), profile(None)),
            ("soon".to_string(), profile(Some("2026-09-20"))),
            ("gone".to_string(), profile(Some("2026-08-01"))),
        ]);
        let alerts = expiry_alerts(&profiles, today());
        let named: Vec<&str> = alerts.iter().map(|(name, _)| name.as_str()).collect();
        assert_eq!(named, vec!["gone", "soon"]);
        assert!(alerts[0].1.contains("expired on 2026-08-01"), "{alerts:?}");
        assert!(alerts[1].1.contains("in 17 days"), "{alerts:?}");
    }

    #[cfg(unix)]
    #[test]
    fn loading_reads_the_token_and_keeps_the_expiry() {
        use std::os::unix::fs::PermissionsExt as _;
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(b"sk-ant-oat01-abc\n").unwrap();
        std::fs::set_permissions(file.path(), std::fs::Permissions::from_mode(0o600)).unwrap();
        let raw = BTreeMap::from([(
            "main".to_string(),
            ClaudeProfileRaw {
                credential: ClaudeCredentialFile::OauthToken(file.path().to_path_buf()),
                expires: Some("2027-09-01".parse().unwrap()),
            },
        )]);
        let loaded = load_claude_profiles(&raw);
        assert_eq!(
            loaded["main"].credential,
            ClaudeCredential::OauthToken(SecretString::new("sk-ant-oat01-abc".to_string()))
        );
        assert_eq!(loaded["main"].expires, Some("2027-09-01".parse().unwrap()));
    }

    #[test]
    #[should_panic(expected = "claude_profile `gone`")]
    fn a_missing_token_file_panics_naming_the_profile() {
        let raw = BTreeMap::from([(
            "gone".to_string(),
            ClaudeProfileRaw {
                credential: ClaudeCredentialFile::OauthToken(PathBuf::from(
                    "/nonexistent/claude-profile-gone.token",
                )),
                expires: None,
            },
        )]);
        load_claude_profiles(&raw);
    }

    #[cfg(unix)]
    #[test]
    #[should_panic(expected = "group/world-accessible")]
    fn a_world_readable_token_file_panics() {
        use std::os::unix::fs::PermissionsExt as _;
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(b"loose\n").unwrap();
        std::fs::set_permissions(file.path(), std::fs::Permissions::from_mode(0o644)).unwrap();
        let raw = BTreeMap::from([(
            "loose".to_string(),
            ClaudeProfileRaw {
                credential: ClaudeCredentialFile::OauthToken(file.path().to_path_buf()),
                expires: None,
            },
        )]);
        load_claude_profiles(&raw);
    }

    #[test]
    fn a_token_profile_names_setup_token_as_the_remedy() {
        let warning = expiry_warning("main", &profile(Some("2026-09-20")), today()).unwrap();
        assert!(warning.contains("claude setup-token"), "{warning}");
        assert!(!warning.contains("Platform"), "{warning}");
    }

    #[test]
    fn a_key_profile_names_the_platform_console_as_the_remedy() {
        let warning = expiry_warning("work", &key_profile(Some("2026-08-01")), today()).unwrap();
        assert!(warning.contains("expired on 2026-08-01"), "{warning}");
        assert!(warning.contains("Anthropic Platform console"), "{warning}");
        assert!(warning.contains("key file"), "{warning}");
        assert!(!warning.contains("setup-token"), "{warning}");
    }

    #[test]
    fn each_credential_kind_has_its_variable_and_wire_word() {
        let token = ClaudeCredential::OauthToken(SecretString::new("t".to_string()));
        let key = ClaudeCredential::ApiKey {
            secret: SecretString::new("k".to_string()),
            settings: settings(),
        };
        assert_eq!(token.env_var(), "CLAUDE_CODE_OAUTH_TOKEN");
        assert_eq!(key.env_var(), "ANTHROPIC_API_KEY");
        assert_eq!(token.kind(), AccountKind::Subscription);
        assert_eq!(key.kind(), AccountKind::ApiKey);
        assert_eq!(key.secret().expose(), "k");
        assert_eq!(token.key_settings(), None);
        assert_eq!(key.key_settings(), Some(&settings()));
    }

    #[cfg(unix)]
    fn private_file(contents: &[u8], mode: u32) -> tempfile::NamedTempFile {
        use std::os::unix::fs::PermissionsExt as _;
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(contents).unwrap();
        std::fs::set_permissions(file.path(), std::fs::Permissions::from_mode(mode)).unwrap();
        file
    }

    #[cfg(unix)]
    fn key_raw(path: PathBuf) -> ClaudeProfileRaw {
        ClaudeProfileRaw {
            credential: ClaudeCredentialFile::ApiKey {
                path,
                settings: KeyProfileSettings {
                    budget_usd: Some(50.0),
                    reset_tz: chrono_tz::America::Chicago,
                    exhausted_markers: vec!["credit balance is too low".to_string()],
                },
            },
            expires: None,
        }
    }

    #[cfg(unix)]
    #[test]
    fn loading_a_key_profile_reads_the_key_and_keeps_its_settings() {
        let file = private_file(b"sk-ant-api03-abc\n", 0o600);
        let raw = BTreeMap::from([("work".to_string(), key_raw(file.path().to_path_buf()))]);
        let loaded = load_claude_profiles(&raw);
        let ClaudeCredentialFile::ApiKey { settings, .. } = &raw["work"].credential else {
            unreachable!("key_raw builds an API-key profile");
        };
        assert_eq!(
            loaded["work"].credential,
            ClaudeCredential::ApiKey {
                secret: SecretString::new("sk-ant-api03-abc".to_string()),
                settings: settings.clone(),
            }
        );
    }

    #[cfg(unix)]
    #[test]
    #[should_panic(expected = "group/world-accessible")]
    fn a_group_readable_key_file_panics() {
        let file = private_file(b"sk-ant-api03-abc\n", 0o640);
        let raw = BTreeMap::from([("work".to_string(), key_raw(file.path().to_path_buf()))]);
        load_claude_profiles(&raw);
    }

    #[cfg(unix)]
    #[test]
    #[should_panic(expected = "is empty")]
    fn an_empty_key_file_panics() {
        let file = private_file(b"\n", 0o600);
        let raw = BTreeMap::from([("work".to_string(), key_raw(file.path().to_path_buf()))]);
        load_claude_profiles(&raw);
    }
}
