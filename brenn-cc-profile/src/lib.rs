//! Claude account profiles at runtime.
//!
//! An account, to Claude Code, is the value of `CLAUDE_CODE_OAUTH_TOKEN`, and a
//! profile is a name bound to one. Which profile an agent runs under is state,
//! not a request: the latest message on the agent's goal channel names it, and
//! this crate holds what that latest message said.
//!
//! Nothing here decides anything. [`ProfileGoal::apply`] takes what a publisher
//! said, checks it against the agent's declared set, and remembers it;
//! [`ProfileGoal::resolve`] hands the spawn path the token to put in the
//! environment. The deciding lives in a policy component on the other side of
//! the channel.

use std::collections::BTreeMap;
use std::sync::RwLock;

use brenn_lib::config::{AppClaudeProfiles, ClaudeProfile, SecretString};
use brenn_lib::messaging::ChannelScheme;
use brenn_messaging::system::SystemParticipantSpec;
use brenn_obs::alerting::{AlertDispatcher, AlertSeverity};
use tracing::{info, warn};

/// Component name of the system participant that subscribes to every goal
/// channel. Its bus identity is `system:cc-profile`.
pub const CC_PROFILE_COMPONENT: &str = "cc-profile";

/// The environment variable Claude Code reads a `claude setup-token` token from.
pub const CLAUDE_OAUTH_TOKEN_VAR: &str = "CLAUDE_CODE_OAUTH_TOKEN";

/// The credential-selecting environment variables no profiled agent may carry
/// from anywhere but its profile: the five Claude Code ranks *above* the token,
/// plus the token variable itself. Any of them wins silently over the profile,
/// so both places Brenn can see one refuse to run — the spawn config build, for
/// an integration's contribution, and [`refuse_outranking_server_env`] for the
/// server's own environment, which a bare child inherits wholesale.
pub const OUTRANKING_CREDENTIAL_VARS: [&str; 6] = [
    "ANTHROPIC_API_KEY",
    "ANTHROPIC_AUTH_TOKEN",
    CLAUDE_OAUTH_TOKEN_VAR,
    "CLAUDE_CODE_USE_BEDROCK",
    "CLAUDE_CODE_USE_VERTEX",
    "CLAUDE_CODE_USE_FOUNDRY",
];

/// Refuse to boot when the server's own environment carries a credential that
/// outranks the token and any **bare** agent runs under a profile.
///
/// A bare child inherits the server's environment through additive `envs`, so
/// the variable would be there and would win: the profile would name one
/// account while Claude Code billed another. A containerized agent is immune —
/// its environment is built from scratch by podman — so the check is scoped to
/// the population that inherits.
///
/// # Panics
///
/// Naming the variable, when one is set and `bare_profiled` is non-empty.
pub fn refuse_outranking_server_env(bare_profiled: &[String]) {
    refuse_outranking_env(bare_profiled, |var| std::env::var_os(var));
}

/// [`refuse_outranking_server_env`] over an arbitrary environment.
///
/// The lookup is a parameter because the real one is process-global: a test
/// that set a variable to see the refusal would be setting it for every other
/// test running beside it.
///
/// # Panics
///
/// Naming the variable, when `lookup` finds one and `bare_profiled` is
/// non-empty.
pub fn refuse_outranking_env(
    bare_profiled: &[String],
    lookup: impl Fn(&str) -> Option<std::ffi::OsString>,
) {
    if bare_profiled.is_empty() {
        return;
    }
    for var in OUTRANKING_CREDENTIAL_VARS {
        assert!(
            lookup(var).is_none(),
            "config: the server's environment carries {var}, which Claude Code reads ahead of \
             the profile's token, and these bare agents run under a claude_profile: {}. A bare \
             child inherits this variable, so the profile would name one account and CC would \
             use another. Unset it, or containerize those agents.",
            bare_profiled.join(", "),
        );
    }
}

/// Why a goal body was refused for one agent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GoalError {
    /// The body was empty, or nothing but whitespace.
    Empty,
    /// A profile name this agent is not allowed to run under — either
    /// undeclared, or declared and granted to some other agent.
    NotAllowed(String),
}

impl std::fmt::Display for GoalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GoalError::Empty => f.write_str("empty goal body"),
            GoalError::NotAllowed(name) => {
                write!(f, "profile {name:?} is not in this agent's claude_profiles")
            }
        }
    }
}

impl GoalError {
    /// A stable short tag for alert dedup, so one bad publisher pages once per
    /// `(agent, reason)` rather than once per publish.
    fn tag(&self) -> &'static str {
        match self {
            GoalError::Empty => "empty",
            GoalError::NotAllowed(_) => "not-allowed",
        }
    }
}

/// Read a goal channel body as a profile name for an agent allowed `allowed`.
///
/// The whole doctype is the name: trimmed text, no JSON. That is the entire
/// contract an out-of-tree publisher has to meet.
pub fn accept(body: &str, allowed: &[String]) -> Result<String, GoalError> {
    let name = body.trim();
    if name.is_empty() {
        return Err(GoalError::Empty);
    }
    if !allowed.iter().any(|a| a == name) {
        return Err(GoalError::NotAllowed(name.to_string()));
    }
    Ok(name.to_string())
}

/// A profile with its token in hand, as the spawn path wants it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedProfile {
    pub name: String,
    pub token: SecretString,
}

/// The live goal state: which profile each app should run under right now.
///
/// Seeded at boot from each app's first allowed profile, then moved by
/// [`apply`](Self::apply) as goals arrive. Read by the spawn path through
/// [`resolve`](Self::resolve).
pub struct ProfileGoal {
    /// Every declared profile, with its token.
    profiles: BTreeMap<String, ClaudeProfile>,
    /// Per app slug, the accounts it may run under and where its goal comes
    /// from. Only apps that declared `claude_profiles` appear.
    apps: BTreeMap<String, AppClaudeProfiles>,
    /// Canonical goal address → the apps bound to it. Several agents may share
    /// one channel; each accepts or rejects a body on its own set.
    channel_apps: BTreeMap<String, Vec<String>>,
    /// App slug → the profile name last accepted for it.
    current: RwLock<BTreeMap<String, String>>,
    /// Where a rejected body is reported. A sandboxed publisher's mistake is
    /// contained, not fatal.
    alerts: AlertDispatcher,
}

impl ProfileGoal {
    /// Build the handle from resolved config.
    ///
    /// # Panics
    ///
    /// When an app's allowed set names a profile that was not declared — the
    /// config layer refuses that document, so reaching here means the check was
    /// bypassed.
    pub fn new(
        profiles: BTreeMap<String, ClaudeProfile>,
        apps: BTreeMap<String, AppClaudeProfiles>,
        alerts: AlertDispatcher,
    ) -> Self {
        let mut channel_apps: BTreeMap<String, Vec<String>> = BTreeMap::new();
        let mut current = BTreeMap::new();
        for (slug, app) in &apps {
            for name in &app.allowed {
                assert!(
                    profiles.contains_key(name),
                    "BUG: app {slug:?} allows undeclared claude_profile {name:?} \
                     (config resolution should have refused this document)",
                );
            }
            let seed = app
                .allowed
                .first()
                .unwrap_or_else(|| panic!("BUG: app {slug:?} has an empty claude_profiles list"))
                .clone();
            current.insert(slug.clone(), seed);
            if let Some(addr) = &app.goal {
                channel_apps
                    .entry(addr.clone())
                    .or_default()
                    .push(slug.clone());
            }
        }
        Self {
            profiles,
            apps,
            channel_apps,
            current: RwLock::new(current),
            alerts,
        }
    }

    /// Every distinct goal channel address, ascending. The set the system
    /// participant subscribes to.
    pub fn goal_addresses(&self) -> Vec<String> {
        self.channel_apps.keys().cloned().collect()
    }

    /// The profile `app_slug` should run under, with its token. `None` for an
    /// app that declared no `claude_profiles` — it gets no token and
    /// authenticates with whatever `/login` left in its home.
    ///
    /// # Panics
    ///
    /// When the recorded name is not a declared profile. `new` and `apply` both
    /// only ever record a declared, allowed name, so this is a bug in one of
    /// them rather than a runtime condition.
    pub fn resolve(&self, app_slug: &str) -> Option<ResolvedProfile> {
        let name = self
            .current
            .read()
            .expect("cc-profile: current goals lock poisoned")
            .get(app_slug)
            .cloned()?;
        let profile = self.profiles.get(&name).unwrap_or_else(|| {
            panic!("BUG: app {app_slug:?} holds goal {name:?}, which is not a declared profile")
        });
        Some(ResolvedProfile {
            name,
            token: profile.token.clone(),
        })
    }

    /// The profile name `app_slug` should run under, without touching its
    /// token.
    pub fn current(&self, app_slug: &str) -> Option<String> {
        self.current
            .read()
            .expect("cc-profile: current goals lock poisoned")
            .get(app_slug)
            .cloned()
    }

    /// Apply one goal message to every app bound to `addr`, returning the slugs
    /// whose value actually changed.
    ///
    /// A body one app accepts another may refuse: acceptance is per agent,
    /// against that agent's own allowed set. A refusal alerts once per process
    /// per `(agent, reason)` and leaves that agent's previous goal standing.
    pub fn apply(&self, addr: &str, body: &str) -> Vec<String> {
        let Some(slugs) = self.channel_apps.get(addr) else {
            // The participant subscribes to exactly the declared goal channels,
            // so a body from anywhere else is a wiring bug rather than a
            // publisher's mistake.
            panic!("BUG: cc-profile received a message on {addr:?}, which no agent named as a goal")
        };
        let mut changed = Vec::new();
        for slug in slugs {
            let allowed = &self
                .apps
                .get(slug)
                .unwrap_or_else(|| panic!("BUG: goal channel {addr:?} names unknown app {slug:?}"))
                .allowed;
            match accept(body, allowed) {
                Ok(name) => {
                    let mut current = self
                        .current
                        .write()
                        .expect("cc-profile: current goals lock poisoned");
                    let prev = current.insert(slug.clone(), name.clone());
                    if prev.as_deref() != Some(name.as_str()) {
                        info!(app = %slug, profile = %name, channel = %addr, "claude profile goal changed");
                        changed.push(slug.clone());
                    }
                }
                Err(err) => {
                    warn!(app = %slug, channel = %addr, "claude profile goal rejected: {err}");
                    self.alerts.alert_once_per_process(
                        AlertSeverity::Warning,
                        "Claude profile goal rejected".to_string(),
                        &format!("cc-profile:{slug}:{}", err.tag()),
                        format!(
                            "A message on {addr} was refused for agent {slug}: {err}. The agent \
                             keeps the profile it had. Whoever publishes this channel is naming \
                             something the agent's claude_profiles does not list."
                        ),
                    );
                }
            }
        }
        changed
    }
}

/// The `system:cc-profile` participant: subscribe-only, on exactly the declared
/// goal channels.
///
/// The matchers carry **bare** channel names, not canonical addresses. The
/// delivery gate splits the scheme off before matching, so an
/// `Exact("brenn:cc-profile.pa")` would never match and every goal would sit
/// undelivered with nothing but a once-per-pair warning to show for it.
///
/// # Panics
///
/// On a goal address that is not `brenn:` — goal channels are durable by
/// config-time rule, and one scheme means one ACL family.
pub fn cc_profile_spec(goal_addrs: &[String]) -> SystemParticipantSpec {
    let mut policy = brenn_lib::access::AppPolicy::default();
    policy
        .grants
        .insert(brenn_envelope::grants::AppCapability::MessagingSubscribe);
    for addr in goal_addrs {
        let bare = match ChannelScheme::split(addr) {
            Some((ChannelScheme::Brenn, bare)) => bare,
            _ => panic!(
                "BUG: claude profile goal channel {addr:?} is not a durable `brenn:` address; \
                 config resolution should have refused it",
            ),
        };
        policy
            .acls
            .brenn_subscribe
            .push(brenn_lib::access::acl::ChannelMatcher::Exact(
                bare.to_string(),
            ));
    }
    SystemParticipantSpec {
        component: CC_PROFILE_COMPONENT,
        policy,
        subscriptions: goal_addrs.to_vec(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(list: &[&str]) -> Vec<String> {
        list.iter().map(|s| s.to_string()).collect()
    }

    fn profile(token: &str) -> ClaudeProfile {
        ClaudeProfile {
            token: SecretString::new(token.to_string()),
            expires: None,
        }
    }

    fn profiles(list: &[&str]) -> BTreeMap<String, ClaudeProfile> {
        list.iter()
            .map(|n| (n.to_string(), profile(&format!("token-{n}"))))
            .collect()
    }

    fn app(allowed: &[&str], goal: Option<&str>) -> AppClaudeProfiles {
        AppClaudeProfiles {
            allowed: names(allowed),
            goal: goal.map(str::to_string),
        }
    }

    fn goal_handle(
        profile_names: &[&str],
        apps: &[(&str, AppClaudeProfiles)],
    ) -> (ProfileGoal, tokio::task::JoinHandle<()>) {
        let (alerts, handle) = brenn_obs::alerting::noop_alert_dispatcher();
        let apps = apps
            .iter()
            .map(|(slug, cfg)| (slug.to_string(), cfg.clone()))
            .collect();
        (
            ProfileGoal::new(profiles(profile_names), apps, alerts),
            handle,
        )
    }

    #[test]
    fn accept_takes_an_allowed_name() {
        assert_eq!(
            accept("main", &names(&["main", "spare"])),
            Ok("main".into())
        );
    }

    #[test]
    fn accept_trims_surrounding_whitespace() {
        assert_eq!(
            accept("  spare \n", &names(&["main", "spare"])),
            Ok("spare".into())
        );
    }

    #[test]
    fn accept_refuses_a_name_outside_the_set() {
        assert_eq!(
            accept("legacy", &names(&["main"])),
            Err(GoalError::NotAllowed("legacy".into()))
        );
    }

    #[test]
    fn accept_refuses_an_empty_body() {
        assert_eq!(accept("", &names(&["main"])), Err(GoalError::Empty));
        assert_eq!(accept(" \t\n", &names(&["main"])), Err(GoalError::Empty));
    }

    #[tokio::test]
    async fn seeds_each_app_with_its_first_allowed_profile() {
        let (goal, _h) = goal_handle(
            &["main", "spare"],
            &[
                ("pa", app(&["spare", "main"], None)),
                ("kb", app(&["main"], None)),
            ],
        );
        assert_eq!(goal.current("pa").as_deref(), Some("spare"));
        assert_eq!(goal.current("kb").as_deref(), Some("main"));
        assert_eq!(
            goal.resolve("pa").map(|r| r.token.expose().to_string()),
            Some("token-spare".to_string())
        );
    }

    #[tokio::test]
    async fn resolve_is_none_for_an_app_without_profiles() {
        let (goal, _h) = goal_handle(&["main"], &[("pa", app(&["main"], None))]);
        assert!(goal.resolve("other").is_none());
        assert!(goal.current("other").is_none());
    }

    #[tokio::test]
    async fn apply_changes_only_the_apps_whose_set_allows_the_name() {
        let addr = "brenn:cc-profile.shared";
        let (goal, _h) = goal_handle(
            &["main", "spare"],
            &[
                ("pa", app(&["main", "spare"], Some(addr))),
                ("kb", app(&["main"], Some(addr))),
            ],
        );
        assert_eq!(goal.apply(addr, "spare"), vec!["pa".to_string()]);
        assert_eq!(goal.current("pa").as_deref(), Some("spare"));
        // `kb` does not allow `spare`, so its previous goal stands.
        assert_eq!(goal.current("kb").as_deref(), Some("main"));
    }

    #[tokio::test]
    async fn apply_reports_nothing_when_the_value_is_unchanged() {
        let addr = "brenn:cc-profile.pa";
        let (goal, _h) = goal_handle(
            &["main", "spare"],
            &[("pa", app(&["main", "spare"], Some(addr)))],
        );
        assert!(goal.apply(addr, "main").is_empty());
        assert_eq!(goal.apply(addr, "spare"), vec!["pa".to_string()]);
        assert!(goal.apply(addr, " spare ").is_empty());
    }

    #[tokio::test]
    async fn goal_addresses_are_distinct_and_sorted() {
        let (goal, _h) = goal_handle(
            &["main"],
            &[
                ("pa", app(&["main"], Some("brenn:cc-profile.shared"))),
                ("kb", app(&["main"], Some("brenn:cc-profile.shared"))),
                ("wm", app(&["main"], Some("brenn:cc-profile.a"))),
                ("nx", app(&["main"], None)),
            ],
        );
        assert_eq!(
            goal.goal_addresses(),
            vec![
                "brenn:cc-profile.a".to_string(),
                "brenn:cc-profile.shared".to_string()
            ]
        );
    }

    #[test]
    fn spec_matchers_are_bare_names() {
        let spec = cc_profile_spec(&names(&["brenn:cc-profile.pa", "brenn:cc-profile.kb"]));
        assert_eq!(spec.component, CC_PROFILE_COMPONENT);
        assert_eq!(
            spec.subscriptions,
            names(&["brenn:cc-profile.pa", "brenn:cc-profile.kb"])
        );
        assert!(spec.policy.allows_channel_access("brenn:cc-profile.pa"));
        assert!(!spec.policy.allows_channel_access("brenn:cc-profile.other"));
        assert!(!spec.policy.allows_channel_access("brenn:some-other-thing"));
    }

    #[test]
    #[should_panic(expected = "is not a durable `brenn:` address")]
    fn spec_refuses_a_non_durable_goal_address() {
        cc_profile_spec(&names(&["ephemeral:cc-profile.pa"]));
    }

    /// The operator-facing half of "reject and log". The dedup key carries both
    /// the agent and the reason: without the agent a second misconfigured agent
    /// would be silently invisible, and without the reason an agent flipping
    /// between two kinds of bad body would page once and never again.
    #[tokio::test]
    async fn a_rejected_goal_pages_once_per_agent_and_reason() {
        let addr = "brenn:cc-profile.shared";
        let (alerts, captured, drainer) =
            brenn_obs::alerting::make_capturing_alerter_with_severity();
        let apps = BTreeMap::from([
            ("pa".to_string(), app(&["main"], Some(addr))),
            ("kb".to_string(), app(&["main", "spare"], Some(addr))),
        ]);
        let goal = ProfileGoal::new(profiles(&["main", "spare"]), apps, alerts);

        // `pa` may not run `spare`; `kb` may, and does.
        goal.apply(addr, "spare");
        // Same agent, same reason: already paged.
        goal.apply(addr, "spare");
        // A different reason, for both agents this time.
        goal.apply(addr, "   ");

        drop(goal);
        drainer.await.expect("alert drainer panicked");
        let fired = captured.lock().expect("captured alerts lock").clone();
        assert!(
            fired
                .iter()
                .all(|(severity, _, _)| matches!(severity, AlertSeverity::Warning)),
            "a publisher's mistake is contained, not fatal: {fired:?}"
        );
        let bodies: Vec<&str> = fired.iter().map(|(_, _, body)| body.as_str()).collect();
        assert_eq!(bodies.len(), 3, "one page per (agent, reason): {bodies:?}");
        assert!(
            bodies
                .iter()
                .any(|b| b.contains("agent pa") && b.contains("\"spare\"")),
            "the page names the agent and what was refused: {bodies:?}"
        );
        assert_eq!(
            bodies
                .iter()
                .filter(|b| b.contains("empty goal body"))
                .count(),
            2,
            "both agents refuse the empty body, and each pages for itself: {bodies:?}"
        );
    }

    /// The refusal that keeps a bare agent's profile from being a lie. Every
    /// variable in the list is covered, so adding a seventh is covered too.
    #[test]
    fn every_outranking_variable_in_the_server_env_refuses_a_bare_profiled_agent() {
        for var in OUTRANKING_CREDENTIAL_VARS {
            let refusal = std::panic::catch_unwind(|| {
                refuse_outranking_env(&names(&["pa"]), |name| {
                    (name == var).then(|| std::ffi::OsString::from("something"))
                });
            })
            .expect_err("a bare profiled agent must not boot with an outranking variable set");
            let message = refusal
                .downcast_ref::<String>()
                .expect("the assertion panics with a formatted message");
            assert!(
                message.contains(var),
                "the refusal names the variable: {message}"
            );
            assert!(message.contains("pa"), "and the agent: {message}");
        }
    }

    /// The guard that keeps every deployment *without* bare profiled agents
    /// booting, however the host's environment is set up.
    #[test]
    fn no_bare_profiled_agent_means_the_server_env_is_not_our_business() {
        refuse_outranking_env(&[], |_| Some(std::ffi::OsString::from("set")));
    }

    #[test]
    fn a_clean_server_env_boots_with_bare_profiled_agents() {
        refuse_outranking_env(&names(&["pa"]), |_| None);
    }

    /// The operator guide lists exactly the variables this constant holds.
    ///
    /// Set equality, so drift fails in both directions: a variable added to the
    /// constant and missing from the doc, and a stray credential name in the
    /// prose that the constant does not hold.
    #[test]
    fn the_operator_guide_names_every_outranking_credential_variable() {
        let path =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../docs/claude-accounts.md");
        let doc = std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("{} must be readable: {error}", path.display()));

        let mut named: std::collections::BTreeSet<&str> = std::collections::BTreeSet::new();
        for token in doc.split(|c: char| !(c.is_ascii_alphanumeric() || c == '_')) {
            if token.starts_with("ANTHROPIC_") || token.starts_with("CLAUDE_CODE_") {
                named.insert(token);
            }
        }

        let expected: std::collections::BTreeSet<&str> =
            OUTRANKING_CREDENTIAL_VARS.iter().copied().collect();
        assert_eq!(
            named, expected,
            "docs/claude-accounts.md and OUTRANKING_CREDENTIAL_VARS disagree about which \
             credentials outrank the profile token; the document is what an operator checks \
             their environment against, so update it"
        );
    }
}
