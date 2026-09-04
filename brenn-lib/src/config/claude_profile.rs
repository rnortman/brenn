//! Claude account profiles: a name bound to a `claude setup-token` token.
//!
//! An account, to Claude Code, is an environment variable —
//! `CLAUDE_CODE_OAUTH_TOKEN` outranks the `/login` credential in the shared
//! `~/.claude`. So a profile is a name bound to a token file, the shared config
//! root is never touched, and switching accounts is spawning a process with a
//! different value in that variable.

use std::collections::BTreeMap;
use std::path::PathBuf;

use chrono::NaiveDate;

use super::secret::{SecretString, load_secret_file_private};

/// The file name a `claude_profile` block without a `token_file` looks for
/// under `claude_defaults.profile_token_dir`.
pub fn default_token_file_name(profile: &str) -> String {
    format!("claude-profile-{profile}.token")
}

const EXPIRY_WARNING_DAYS: i64 = 30;

/// One `claude_profile` block, as the document states it.
///
/// The path is final by the time this exists: lowering has already applied the
/// `profile_token_dir` convention to a block that stated no `token_file`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClaudeProfileRaw {
    /// Host path to the token file. 0600, one line.
    pub token_file: PathBuf,
    /// The date the operator recorded the token as ceasing to work. Brenn
    /// cannot read a token's lifetime — the value is opaque — so this is the
    /// operator's note to itself and the only expiry signal there is.
    pub expires: Option<NaiveDate>,
}

/// Which accounts one agent may run under, and where its goal comes from.
///
/// Present exactly when the agent states `claude_profiles`. Absent, the agent
/// gets no token at spawn and authenticates with whatever `/login` left in its
/// home — which is every agent in every deployment that declares no profiles.
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

/// One profile with its token in hand.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClaudeProfile {
    pub token: SecretString,
    pub expires: Option<NaiveDate>,
}

/// Load every declared profile's token.
///
/// A declared profile whose token is missing, empty, unreadable, or readable by
/// any other local account is a misconfiguration, not a runtime condition.
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
            let token = SecretString::new(load_secret_file_private(&label, &profile.token_file));
            (
                name.clone(),
                ClaudeProfile {
                    token,
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
/// says nothing either way.
pub fn expiry_warning(name: &str, profile: &ClaudeProfile, today: NaiveDate) -> Option<String> {
    let expires = profile.expires?;
    let days = (expires - today).num_days();
    if days > EXPIRY_WARNING_DAYS {
        return None;
    }
    Some(if days < 0 {
        format!(
            "Claude profile `{name}` expired on {expires} ({} days ago). \
             Mint a new token with `claude setup-token` and overwrite its token file.",
            -days,
        )
    } else {
        format!(
            "Claude profile `{name}` expires on {expires} (in {days} days). \
             Mint a new token with `claude setup-token` and overwrite its token file.",
        )
    })
}

/// Every expiry warning a set of profiles has to give today, as
/// `(profile name, warning)` in name order.
///
/// One entry per profile that is past its recorded date or inside the warning
/// window, and none for the rest — a token is opaque, so a date the operator
/// wrote down is the only expiry signal there is, and it is worth exactly one
/// alert per profile at boot.
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
            token: SecretString::new("tok".to_string()),
            expires: expires.map(|date| date.parse().unwrap()),
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
                token_file: file.path().to_path_buf(),
                expires: Some("2027-09-01".parse().unwrap()),
            },
        )]);
        let loaded = load_claude_profiles(&raw);
        assert_eq!(loaded["main"].token.expose(), "sk-ant-oat01-abc");
        assert_eq!(loaded["main"].expires, Some("2027-09-01".parse().unwrap()));
    }

    #[test]
    #[should_panic(expected = "claude_profile `gone`")]
    fn a_missing_token_file_panics_naming_the_profile() {
        let raw = BTreeMap::from([(
            "gone".to_string(),
            ClaudeProfileRaw {
                token_file: PathBuf::from("/nonexistent/claude-profile-gone.token"),
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
                token_file: file.path().to_path_buf(),
                expires: None,
            },
        )]);
        load_claude_profiles(&raw);
    }
}
