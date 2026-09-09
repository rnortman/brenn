//! Level 2, the agent half: which agents moved, and what moving them means.
//!
//! An agent's fields fall in three classes ([`super::compare`]). The two
//! convergible ones reach a reload by different routes, and this module is
//! where the route is decided per agent:
//!
//! - **Authority and per-call** — grants, ACLs, tool grants, static
//!   subscriptions, send budget, and the display and door settings — converge
//!   the instant the agent map is swapped, because every gate reads them per
//!   call. What the swap alone does *not* do is fold the agent's subscriber
//!   entries onto the channels the candidate says it reads, so those are
//!   computed here as `subs_added`/`subs_removed`.
//! - **Per-process** — what a Claude Code process was spawned with — cannot be
//!   converged in place at all. An agent whose per-process view moved is in
//!   this delta on level 1's word, and what the commit owes it is a retirement
//!   at its next idle moment.
//!
//! An agent reaches this delta three ways: its resolved authority moved, level
//! 1 saw one of its per-call or per-process fields move, or — by closure, the
//! same rule consumers and surfaces answer to — a channel one of its
//! subscriptions names is one this reload is taking out and putting back, which
//! shows up as the subscription sitting on both sides below.

use std::collections::{BTreeMap, HashMap, HashSet};

use brenn_lib::config::AppConfig;
use brenn_lib::messaging::{ChannelEntry, SubscriberEntry, SubscriberEntryKind};
use indexmap::IndexMap;
use uuid::Uuid;

use super::compare::AppFieldDiff;
use super::delta::LiveFacts;
use super::dynamic::{DynamicRemerge, RemergeSides, remerge_of};
use super::subscribers::PlannedSubscribers;

/// What moved about one agent.
pub(crate) struct AgentChange {
    pub slug: String,
    /// Static subscriptions to fold out / fold in, by channel uuid and address.
    /// A retuned subscription or one on a channel this reload moves appears on
    /// both sides.
    pub subs_removed: SubSide,
    pub subs_added: SubSide,
    /// What the candidate does to the dynamic subscriptions this agent asked
    /// for at runtime: which are revoked to dormancy, which come back, and
    /// which a static declaration now replaces.
    pub dynamic: DynamicRemerge,
    /// The agent's per-process view moved — a class-B field per level 1, or a
    /// virtual-tools rendering a running `noop_mcp.py` no longer matches. Its
    /// live sessions are retired at their next idle moment after the swap. An
    /// authority-only or per-call-only change leaves this `false`: nothing a
    /// live process holds is stale.
    pub respawn: bool,
    /// Whether the agent's virtual-tools rendering moved, so prepare staged the
    /// candidate's beside the running one and commit renames it into place.
    pub virtual_tools_staged: bool,
    /// `allowed_users.first()` differs: the singleton conversation the bus path
    /// targets is a different one, so every push-enabled entry the agent holds
    /// live is re-attached rather than only the ones that moved.
    pub owner_changed: bool,
    /// The baseline's `allowed_users.first()` when `owner_changed` — the owner
    /// whose singleton conversation held the agent's positions until this
    /// reload, and whose positions the commit reaps. `None` when the baseline
    /// was open to all: no owner resolved, so no position was ever held under
    /// one.
    pub previous_owner: Option<String>,
    /// The candidate's `allowed_users`, empty meaning open to all. What the
    /// commit condemns is every bridge whose owner this list denies, so an
    /// agent that goes from open to restricted severs the users it now denies
    /// even though it names none of them as removed.
    pub allowed_users: Vec<String>,
    /// The candidate denies a user the baseline allowed: either the list lost
    /// an entry, or a previously empty list — open to all — became a list.
    /// Their connections are closed and their bridges retired. A non-empty list
    /// going empty denies nobody.
    pub users_restricted: bool,
    /// Users named in the old `allowed_users` and not in the new, for the log
    /// line and the operator's reading of what changed. Empty when an open
    /// agent was restricted: the users it denies are everyone else, and no
    /// document names them.
    pub users_removed: Vec<String>,
}

/// The channel delta's two sets, as the agent half reads them.
pub(crate) struct AgentClosure<'a> {
    /// Every uuid either side of the channel delta names.
    pub moved: &'a HashSet<Uuid>,
    /// The narrower set this commit takes *away* — removed outright, or the old
    /// side of a retune. No pair may be classified against one: the entry a
    /// classification reads is one the channel walk deletes.
    pub departing: &'a HashSet<Uuid>,
}

/// Level 1's per-agent field classes and what a rendering diff needs, fed into
/// the agent delta.
pub(crate) struct AgentInputs<'a> {
    pub app_diffs: &'a BTreeMap<String, AppFieldDiff>,
    /// The registry the virtual-tools file is rendered against — the same one
    /// boot renders with, since the tool set is a process constant.
    pub tool_registry: &'a brenn_tool_registry::ToolRegistry,
}

/// Classify every agent the two maps hold.
///
/// The slug set is the same on both sides: adding, removing and renaming an
/// agent is a level-1 refusal, so an agent in one map and not the other is a
/// host bug and is skipped rather than guessed at.
pub(crate) fn agent_delta(
    baseline_apps: &IndexMap<String, AppConfig>,
    candidate_apps: &IndexMap<String, AppConfig>,
    baseline_entries: &[std::sync::Arc<ChannelEntry>],
    candidate_entries: &[std::sync::Arc<ChannelEntry>],
    channels: &AgentClosure<'_>,
    inputs: &AgentInputs<'_>,
    live: &LiveFacts<'_>,
) -> Vec<AgentChange> {
    let old_subs = PlannedSubscribers::of(baseline_entries);
    let new_subs = PlannedSubscribers::of(candidate_entries);
    let mut out = Vec::new();

    for (slug, new_app) in candidate_apps {
        let Some(old_app) = baseline_apps.get(slug) else {
            continue;
        };
        let kind = SubscriberEntryKind::App(slug.clone());
        let (subs_removed, subs_added) = subscription_sides(
            old_subs.of_principal(&kind),
            new_subs.of_principal(&kind),
            channels.moved,
        );
        // The file `noop_mcp.py` read once at its start. A grant that adds or
        // withdraws a tool renders differently; a matcher-only ACL change does
        // not, and retires nothing.
        let virtual_tools_staged =
            brenn_server::active_bridge::render_virtual_tools(old_app, inputs.tool_registry)
                != brenn_server::active_bridge::render_virtual_tools(new_app, inputs.tool_registry);
        let spawn_changed = inputs
            .app_diffs
            .get(slug)
            .is_some_and(|diff| diff.spawn_changed);

        let changed = old_app.authority() != new_app.authority()
            || inputs.app_diffs.contains_key(slug)
            || virtual_tools_staged
            || !subs_removed.is_empty()
            || !subs_added.is_empty();
        if !changed {
            continue;
        }

        // The candidate's static subscriptions decide the prune arm: a
        // `subscribe` line declared where a dynamic row already sits is boot's
        // "static config wins", asked of the candidate.
        let candidate_static: HashMap<Uuid, String> = new_subs
            .of_principal(&kind)
            .iter()
            .map(|(entry, _)| (entry.uuid, entry.address.clone()))
            .collect();
        let dynamic = remerge_of(
            slug,
            &new_app.policy,
            live.dynamic,
            live.directory,
            &RemergeSides {
                departing: channels.departing,
                candidate_static: &candidate_static,
            },
        );

        out.push(AgentChange {
            slug: slug.clone(),
            subs_removed,
            subs_added,
            dynamic,
            respawn: spawn_changed || virtual_tools_staged,
            virtual_tools_staged,
            owner_changed: old_app.allowed_users.first() != new_app.allowed_users.first(),
            previous_owner: old_app.allowed_users.first().cloned(),
            allowed_users: new_app.allowed_users.clone(),
            users_restricted: restricts_users(&old_app.allowed_users, &new_app.allowed_users),
            users_removed: removed_users(&old_app.allowed_users, &new_app.allowed_users),
        });
    }
    out
}

/// Where prepare stages an agent's candidate virtual-tools rendering, beside
/// the file the running `noop_mcp.py` read.
///
/// A rename onto the live path is atomic on one filesystem, so no process ever
/// reads a half-written tool list; `state_dir` is the same directory in both
/// documents because `container` is a level-1 refusal.
pub(crate) fn staged_virtual_tools_path(app: &AppConfig) -> std::path::PathBuf {
    let path = app.virtual_tools_path();
    let mut name = path.file_name().unwrap_or_default().to_os_string();
    name.push(".next");
    path.with_file_name(name)
}

/// Whether the candidate denies a user the baseline allowed.
///
/// Two shapes do: an entry dropped from a list, and an empty list — open to all
/// — becoming a list, which denies every user not named in it. The second names
/// nobody, which is why the commit asks the candidate's list who is allowed
/// rather than iterating a removed set. A non-empty list going empty opens the
/// agent and denies nobody, the reading every `user_has_access` gate takes.
fn restricts_users(old: &[String], new: &[String]) -> bool {
    if new.is_empty() {
        return false;
    }
    old.is_empty() || old.iter().any(|user| !new.contains(user))
}

/// The users the candidate no longer allows *by name*.
///
/// An empty candidate list is open to all, so it removes nobody — the same
/// reading every `user_has_access` gate takes.
fn removed_users(old: &[String], new: &[String]) -> Vec<String> {
    if new.is_empty() {
        return Vec::new();
    }
    old.iter()
        .filter(|user| !new.contains(user))
        .cloned()
        .collect()
}

/// One side of an agent's subscription move: the channel uuid and its address.
type SubSide = Vec<(Uuid, String)>;

/// Which of an agent's static subscriptions leave and which arrive.
///
/// Each side is the agent's entries in one plan's directory, which holds
/// exactly the static subscriptions that document declares: dynamic rows are
/// folded into the *live* directory after boot and are in neither plan, which
/// is why rule 2 answers for them separately.
///
/// A subscription present on both sides at the same tuning, on a channel this
/// reload does not move, is neither: nothing has to be folded out and back in.
/// One whose channel *does* move is on both sides, because the entry the commit
/// removes and the entry it adds sit on two different channel entries.
fn subscription_sides(
    old: &[(&ChannelEntry, &SubscriberEntry)],
    new: &[(&ChannelEntry, &SubscriberEntry)],
    moved: &HashSet<Uuid>,
) -> (SubSide, SubSide) {
    let mut removed = Vec::new();
    let mut added = Vec::new();

    for (channel, entry) in old {
        match new.iter().find(|(other, _)| other.uuid == channel.uuid) {
            Some((_, new_entry))
                if same_tuning(entry, new_entry) && !moved.contains(&channel.uuid) => {}
            _ => removed.push((channel.uuid, channel.address.clone())),
        }
    }
    for (channel, entry) in new {
        match old.iter().find(|(other, _)| other.uuid == channel.uuid) {
            Some((_, old_entry))
                if same_tuning(entry, old_entry) && !moved.contains(&channel.uuid) => {}
            _ => added.push((channel.uuid, channel.address.clone())),
        }
    }
    (removed, added)
}

/// Whether two subscriber entries subscribe on the same terms: everything the
/// fold and the cursor read.
fn same_tuning(a: &SubscriberEntry, b: &SubscriberEntry) -> bool {
    a.push_depth == b.push_depth
        && a.retain_depth == b.retain_depth
        && a.noise == b.noise
        && a.wake_min == b.wake_min
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use brenn_lib::access::acl::ChannelMatcher;
    use brenn_lib::messaging::ChannelEntry;
    use brenn_lib::messaging::config::{Depth, NoiseLevel};
    use brenn_lib::messaging::directory::WakeMin;
    use brenn_server::test_support::app_config::minimal_app_config;

    const SLUG: &str = "assistant";

    /// One agent, resolved as far as this comparison reads: a policy that
    /// covers `subscribes` and nothing else.
    fn app(slug: &str, subscribes: &[&str]) -> AppConfig {
        let mut config = minimal_app_config(slug, None, Vec::new());
        let policy = Arc::make_mut(&mut config.policy);
        policy.acls.brenn_subscribe = subscribes
            .iter()
            .map(|address| ChannelMatcher::Exact((*address).to_string()))
            .collect();
        config
    }

    fn map(apps: Vec<AppConfig>) -> IndexMap<String, AppConfig> {
        apps.into_iter()
            .map(|app| (app.slug.clone(), app))
            .collect()
    }

    /// A channel `slug` holds a pull-only subscriber entry on, under `uuid`.
    fn channel(uuid: Uuid, address: &str, slug: Option<&str>) -> Arc<ChannelEntry> {
        entry_at(uuid, address, slug, Depth::Bounded(0))
    }

    fn entry_at(
        uuid: Uuid,
        address: &str,
        slug: Option<&str>,
        push_depth: Depth,
    ) -> Arc<ChannelEntry> {
        let subscribers = slug
            .map(|slug| SubscriberEntry {
                kind: SubscriberEntryKind::App(slug.to_string()),
                push_depth,
                retain_depth: Depth::Bounded(4),
                noise: NoiseLevel::Silent,
                wake_min: Some(WakeMin::Never),
            })
            .into_iter()
            .collect();
        let mut entry =
            brenn_lib::messaging::test_support::test_channel_entry(address, subscribers);
        entry.uuid = uuid;
        Arc::new(entry)
    }

    /// The delta over two sides, with no channel moving and no field class set.
    fn delta(
        old: &IndexMap<String, AppConfig>,
        new: &IndexMap<String, AppConfig>,
        old_entries: &[Arc<ChannelEntry>],
        new_entries: &[Arc<ChannelEntry>],
    ) -> Vec<AgentChange> {
        delta_with(old, new, old_entries, new_entries, &HashSet::new(), &[])
    }

    fn delta_with(
        old: &IndexMap<String, AppConfig>,
        new: &IndexMap<String, AppConfig>,
        old_entries: &[Arc<ChannelEntry>],
        new_entries: &[Arc<ChannelEntry>],
        moved: &HashSet<Uuid>,
        diffs: &[&str],
    ) -> Vec<AgentChange> {
        delta_classed(old, new, old_entries, new_entries, moved, diffs, false)
    }

    /// The delta with level 1's word on each named agent, `spawn` deciding
    /// which class it says moved.
    fn delta_classed(
        old: &IndexMap<String, AppConfig>,
        new: &IndexMap<String, AppConfig>,
        old_entries: &[Arc<ChannelEntry>],
        new_entries: &[Arc<ChannelEntry>],
        moved: &HashSet<Uuid>,
        diffs: &[&str],
        spawn: bool,
    ) -> Vec<AgentChange> {
        let app_diffs = diffs
            .iter()
            .map(|slug| {
                (
                    (*slug).to_string(),
                    AppFieldDiff {
                        per_call_changed: !spawn,
                        spawn_changed: spawn,
                    },
                )
            })
            .collect();
        let registry = brenn_tool_registry::ToolRegistry::new(vec![]);
        agent_delta(
            old,
            new,
            old_entries,
            new_entries,
            &AgentClosure {
                moved,
                departing: &HashSet::new(),
            },
            &AgentInputs {
                app_diffs: &app_diffs,
                tool_registry: &registry,
            },
            &LiveFacts {
                directory: &brenn_lib::messaging::MessagingDirectory::with_entries(Vec::new()),
                dynamic: &crate::reload::dynamic::DynamicSnapshot::default(),
                mqtt_clients: &IndexMap::new(),
                clients_stopping: &std::collections::BTreeSet::new(),
            },
        )
    }

    fn addresses(side: &SubSide) -> Vec<&str> {
        side.iter().map(|(_, address)| address.as_str()).collect()
    }

    #[test]
    fn an_untouched_agent_is_not_in_the_delta() {
        let old = map(vec![app(SLUG, &["work"])]);
        let new = map(vec![app(SLUG, &["work"])]);
        assert!(delta(&old, &new, &[], &[]).is_empty());
    }

    #[test]
    fn a_widened_acl_alone_changes_the_agent_and_moves_no_subscription() {
        let old = map(vec![app(SLUG, &["work"])]);
        let new = map(vec![app(SLUG, &["work", "automation.out"])]);

        let changes = delta(&old, &new, &[], &[]);
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].slug, SLUG);
        assert!(changes[0].subs_added.is_empty());
        assert!(changes[0].subs_removed.is_empty());
    }

    /// Without level 1's word, the delta would be empty and the edit would be
    /// adopted as baseline without ever reaching a reader.
    #[test]
    fn a_per_call_field_puts_the_agent_in_the_delta_on_level_ones_word() {
        let old = map(vec![app(SLUG, &["work"])]);
        let new = map(vec![app(SLUG, &["work"])]);

        let changes = delta_with(&old, &new, &[], &[], &HashSet::new(), &[SLUG]);
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].slug, SLUG);
    }

    #[test]
    fn a_subscription_added_and_one_removed_land_on_their_own_sides() {
        let (a, b) = (Uuid::new_v4(), Uuid::new_v4());
        let old = map(vec![app(SLUG, &["work"])]);
        let new = map(vec![app(SLUG, &["sink"])]);
        let old_entries = [channel(a, "work", Some(SLUG)), channel(b, "sink", None)];
        let new_entries = [channel(a, "work", None), channel(b, "sink", Some(SLUG))];

        let changes = delta(&old, &new, &old_entries, &new_entries);
        assert_eq!(changes.len(), 1);
        assert_eq!(addresses(&changes[0].subs_removed), ["brenn:work"]);
        assert_eq!(addresses(&changes[0].subs_added), ["brenn:sink"]);
    }

    /// A retune is a fold-out and a fold-in of one entry, so it is on both
    /// sides — the commit has no in-place edit for a subscriber's depths.
    #[test]
    fn a_retuned_subscription_is_on_both_sides() {
        let uuid = Uuid::new_v4();
        let apps = map(vec![app(SLUG, &["work"])]);
        let old_entries = [entry_at(uuid, "work", Some(SLUG), Depth::Bounded(0))];
        let new_entries = [entry_at(uuid, "work", Some(SLUG), Depth::Bounded(2))];

        let changes = delta(&apps, &apps, &old_entries, &new_entries);
        assert_eq!(changes.len(), 1);
        assert_eq!(addresses(&changes[0].subs_removed), ["brenn:work"]);
        assert_eq!(addresses(&changes[0].subs_added), ["brenn:work"]);
    }

    #[test]
    fn an_agent_on_a_moved_channel_is_promoted_with_its_entry_on_both_sides() {
        let uuid = Uuid::new_v4();
        let apps = map(vec![app(SLUG, &["work"])]);
        let entries = [channel(uuid, "work", Some(SLUG))];

        let changes = delta_with(
            &apps,
            &apps,
            &entries,
            &entries,
            &HashSet::from([uuid]),
            &[],
        );
        assert_eq!(changes.len(), 1);
        assert_eq!(addresses(&changes[0].subs_removed), ["brenn:work"]);
        assert_eq!(addresses(&changes[0].subs_added), ["brenn:work"]);
    }

    #[test]
    fn an_unmoved_subscription_moves_neither_side() {
        let uuid = Uuid::new_v4();
        let apps = map(vec![app(SLUG, &["work"])]);
        let entries = [channel(uuid, "work", Some(SLUG))];

        assert!(delta(&apps, &apps, &entries, &entries).is_empty());
    }

    #[test]
    fn one_agent_changing_leaves_the_other_out() {
        let old = map(vec![app(SLUG, &["work"]), app("other", &["work"])]);
        let new = map(vec![app(SLUG, &["work", "sink"]), app("other", &["work"])]);

        let changes = delta(&old, &new, &[], &[]);
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].slug, SLUG);
    }

    /// A user dropped from `allowed_users`: their connections close and their
    /// bridges retire, and neither happens without this list.
    #[test]
    fn a_narrowed_allowed_users_list_names_the_user_it_dropped() {
        let mut old_app = app(SLUG, &["work"]);
        old_app.allowed_users = vec!["alice".to_string(), "bob".to_string()];
        let mut new_app = app(SLUG, &["work"]);
        new_app.allowed_users = vec!["alice".to_string()];
        let (old, new) = (map(vec![old_app]), map(vec![new_app]));

        let changes = delta_with(&old, &new, &[], &[], &HashSet::new(), &[SLUG]);
        assert_eq!(changes[0].users_removed, ["bob".to_string()]);
        assert!(changes[0].users_restricted);
        assert_eq!(changes[0].allowed_users, ["alice".to_string()]);
        assert!(!changes[0].owner_changed, "the first entry did not move");
        assert!(!changes[0].respawn);
    }

    /// An agent open to all, restricted to one user: the document names no
    /// removed user, and the commit still has to sever everyone the candidate
    /// denies — which is why it asks the candidate's list who is allowed
    /// rather than iterating a removed set.
    #[test]
    fn restricting_an_open_agent_restricts_without_naming_anyone() {
        let mut old_app = app(SLUG, &["work"]);
        old_app.allowed_users = Vec::new();
        let mut new_app = app(SLUG, &["work"]);
        new_app.allowed_users = vec!["alice".to_string()];
        let (old, new) = (map(vec![old_app]), map(vec![new_app]));

        let changes = delta_with(&old, &new, &[], &[], &HashSet::new(), &[SLUG]);
        assert!(
            changes[0].users_restricted,
            "open to all becoming a list denies every user it omits",
        );
        assert!(
            changes[0].users_removed.is_empty(),
            "and names none of them",
        );
        assert_eq!(changes[0].allowed_users, ["alice".to_string()]);
    }

    /// Empty is open to all, so emptying a list removes nobody.
    #[test]
    fn an_allowed_users_list_going_empty_removes_nobody() {
        let mut old_app = app(SLUG, &["work"]);
        old_app.allowed_users = vec!["alice".to_string()];
        let mut new_app = app(SLUG, &["work"]);
        new_app.allowed_users = Vec::new();
        let (old, new) = (map(vec![old_app]), map(vec![new_app]));

        let changes = delta_with(&old, &new, &[], &[], &HashSet::new(), &[SLUG]);
        assert!(changes[0].users_removed.is_empty());
        assert!(
            !changes[0].users_restricted,
            "and nobody is denied: empty is open to all",
        );
    }

    /// The owner is the first entry: moving it moves the conversation the bus
    /// path targets, so every push-enabled entry is re-attached.
    #[test]
    fn moving_the_first_allowed_user_changes_the_owner() {
        let mut old_app = app(SLUG, &["work"]);
        old_app.allowed_users = vec!["alice".to_string()];
        let mut new_app = app(SLUG, &["work"]);
        new_app.allowed_users = vec!["bob".to_string()];
        let (old, new) = (map(vec![old_app]), map(vec![new_app]));

        let changes = delta_with(&old, &new, &[], &[], &HashSet::new(), &[SLUG]);
        assert!(changes[0].owner_changed);
        assert_eq!(
            changes[0].previous_owner,
            Some("alice".to_string()),
            "the commit reaps the positions held under the old owner's conversation, so it has              to know who that was",
        );
        assert_eq!(changes[0].users_removed, ["alice".to_string()]);
    }

    /// An agent open to all, restricted to one user, has an owner where it had
    /// none: nobody's conversation held its positions, so there is nothing to
    /// reap and no previous owner to name.
    #[test]
    fn restricting_an_open_agent_changes_the_owner_with_no_previous_one() {
        let mut old_app = app(SLUG, &["work"]);
        old_app.allowed_users = Vec::new();
        let mut new_app = app(SLUG, &["work"]);
        new_app.allowed_users = vec!["bob".to_string()];
        let (old, new) = (map(vec![old_app]), map(vec![new_app]));

        let changes = delta_with(&old, &new, &[], &[], &HashSet::new(), &[SLUG]);
        assert!(changes[0].owner_changed);
        assert_eq!(changes[0].previous_owner, None);
    }

    /// A class-B field is level 1's word alone, and it is what condemns the
    /// agent's live processes.
    #[test]
    fn a_class_b_field_sets_respawn() {
        let apps = map(vec![app(SLUG, &["work"])]);
        let changes = delta_classed(&apps, &apps, &[], &[], &HashSet::new(), &[SLUG], true);
        assert!(changes[0].respawn);
        assert!(
            !changes[0].virtual_tools_staged,
            "nothing about the tool list moved",
        );
    }

    /// A matcher-only ACL change renders the same tool list, so the running
    /// `noop_mcp.py` is still describing the truth.
    #[test]
    fn a_matcher_only_acl_change_retires_nothing() {
        let old = map(vec![app(SLUG, &["work"])]);
        let new = map(vec![app(SLUG, &["work", "sink"])]);

        let changes = delta(&old, &new, &[], &[]);
        assert!(!changes[0].respawn);
        assert!(!changes[0].virtual_tools_staged);
    }

    /// A publish grant is a different rendering, which the running process read
    /// once and cannot re-read: `respawn` without level 1 saying anything.
    #[test]
    fn a_grant_that_adds_a_tool_sets_respawn_through_the_rendering() {
        let old = map(vec![app(SLUG, &["work"])]);
        let mut granted = app(SLUG, &["work"]);
        Arc::make_mut(&mut granted.policy)
            .grants
            .insert(brenn_envelope::grants::AppCapability::MessagingPublish);
        let new = map(vec![granted]);

        let changes = delta(&old, &new, &[], &[]);
        assert_eq!(changes.len(), 1);
        assert!(changes[0].virtual_tools_staged);
        assert!(changes[0].respawn);
    }
}
