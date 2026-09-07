//! The surface half of level 2: which surfaces moved, and whether the two
//! plans agree about the process's system participants.
//!
//! The surface-description pair is set aside from the participant comparison
//! because it is the one pair whose contents track the surface list itself.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use brenn_lib::messaging::config::ResolvedSurface;
use brenn_lib::messaging::directory::SubscriberEntryKind;
use brenn_lib::messaging::gates::{BodySizeExceeded, check_body_size};
use brenn_messaging::SubscriberRegistration;
use brenn_messaging::system::SystemParticipantSpec;
use brenn_surface_schema::LogLevel;
use brenn_surface_server::SurfaceRoots;
use brenn_surface_server::bindings_doc::{BindingsDocParams, build_bindings_documents};
use brenn_surface_server::description::{
    DescriptionSelection, SURFACE_CONFIG_COMPONENT, SURFACE_HELP_COMPONENT,
    build_description_docs_selected, distinct_kinds,
};
use uuid::Uuid;

use super::NEEDS_RESTART;

/// A surface present in both plans that is not the same surface: the commit
/// retires the old one and starts the new one.
pub(crate) struct SurfaceChange {
    /// The baseline's value: the sessions to close and the subscriber entries
    /// to unfold are the running surface's.
    pub old: ResolvedSurface,
    /// The candidate's value: the wiring, the budgets and the runtime the
    /// commit installs are its.
    pub new: ResolvedSurface,
}

/// Which surfaces a reload has to retire, start, or replace.
///
/// Keyed by slug throughout: a surface's slug is its participant identity, its
/// runtime-table key and its URL, so a surface that kept its slug and changed
/// everything else is one entry to walk and not a removal beside an add.
#[derive(Default)]
pub(crate) struct SurfaceDelta {
    /// Surfaces the candidate has and the baseline does not.
    pub added: Vec<ResolvedSurface>,
    /// Surfaces the baseline has and the candidate does not.
    pub removed: Vec<ResolvedSurface>,
    /// Surfaces present in both whose resolved value moved, plus the ones
    /// promoted by closure because a channel they bind or a kind they
    /// instantiate moved.
    pub changed: Vec<SurfaceChange>,
}

impl SurfaceDelta {
    pub fn is_empty(&self) -> bool {
        self.added.is_empty() && self.removed.is_empty() && self.changed.is_empty()
    }
}

/// What a surface is compared against besides its own resolved value.
///
/// Both are *closures* in the sense the consumer delta already uses: a surface
/// whose inputs moved is re-derived against what moved, because that is what a
/// fresh boot would give it. The channel half is the directory entries the
/// channel delta names; the kind half is the fingerprints two scans of the
/// declared mounts disagree about.
pub(crate) struct SurfaceClosure<'a> {
    /// Uuids of every entry in the channel delta.
    pub moved_channels: &'a HashSet<Uuid>,
    /// Addresses of the same entries, which is the grain a surface's output
    /// bindings name a channel at.
    pub moved_addresses: &'a HashSet<String>,
    /// Kinds whose installed bytes or root moved between the two scans.
    pub kinds_changed: &'a BTreeSet<String>,
}

/// Classify every difference between the two plans' surface lists.
pub(crate) fn surface_delta(
    baseline: &[ResolvedSurface],
    candidate: &[ResolvedSurface],
    closure: &SurfaceClosure<'_>,
) -> SurfaceDelta {
    let old = by_slug(baseline);
    let new = by_slug(candidate);
    let mut delta = SurfaceDelta::default();
    for surface in candidate {
        match old.get(surface.slug.as_str()) {
            None => delta.added.push(surface.clone()),
            Some(previous) => {
                // Both sides' bindings are consulted: a channel a surface
                // stopped binding is named only by the old value, one it
                // started binding only by the new. Same for kinds.
                let moved = *previous != surface
                    || touches_moved_channel(previous, closure)
                    || touches_moved_channel(surface, closure)
                    || instantiates_moved_kind(previous, closure)
                    || instantiates_moved_kind(surface, closure);
                if moved {
                    delta.changed.push(SurfaceChange {
                        old: (*previous).clone(),
                        new: surface.clone(),
                    });
                }
            }
        }
    }
    for surface in baseline {
        if !new.contains_key(surface.slug.as_str()) {
            delta.removed.push(surface.clone());
        }
    }
    delta
}

/// Whether any channel this surface binds is in the channel delta.
///
/// Inputs are compared by uuid (a transportable subscription carries the
/// directory's own key) and outputs by address (an output binding names a
/// channel by address and never resolves a uuid). A `local:` binding is in
/// neither set: that traffic has no directory entry to move.
fn touches_moved_channel(surface: &ResolvedSurface, closure: &SurfaceClosure<'_>) -> bool {
    surface.wire_subscriptions.iter().any(|sub| {
        closure
            .moved_channels
            .contains(&sub.subscription.channel_uuid)
    }) || surface
        .outputs
        .iter()
        .any(|out| closure.moved_addresses.contains(&out.channel_address))
}

/// Whether any kind this surface instantiates was installed differently by the
/// candidate's mounts than by the baseline's.
fn instantiates_moved_kind(surface: &ResolvedSurface, closure: &SurfaceClosure<'_>) -> bool {
    surface
        .components
        .iter()
        .any(|comp| closure.kinds_changed.contains(&comp.kind))
}

fn by_slug(surfaces: &[ResolvedSurface]) -> HashMap<&str, &ResolvedSurface> {
    surfaces
        .iter()
        .map(|surface| (surface.slug.as_str(), surface))
        .collect()
}

/// The two participants whose policies are a function of the surface list, and
/// so move whenever a surface or a kind does.
const SURFACE_DESCRIPTION_PARTICIPANTS: [&str; 2] =
    [SURFACE_HELP_COMPONENT, SURFACE_CONFIG_COMPONENT];

/// Rule 7: the two plans must derive the same system participants.
///
/// Every participant but the surface-description pair is derived from a
/// non-convergible block, so level 1 has already frozen its inputs and a
/// difference here means one was derived from something else. It is refused
/// rather than asserted because the derivation reads the document, and an
/// operator must never meet a panic where a refusal will do.
///
/// The pair set aside carries one exact-match ACL matcher per surface and per
/// distinct kind, which is surface *identity* rather than a frozen input.
pub(crate) fn system_participant_refusals(
    baseline: &[SystemParticipantSpec],
    candidate: &[SystemParticipantSpec],
) -> Vec<String> {
    let old = compared(baseline);
    let new = compared(candidate);
    let mut out = Vec::new();
    for (component, spec) in &old {
        match new.get(component) {
            Some(now) if now == spec => {}
            Some(_) => out.push(format!(
                "the {component:?} system participant's code-built policy is not the one this \
                 process is running: {NEEDS_RESTART}"
            )),
            None => out.push(format!(
                "the {component:?} system participant is no longer derived: {NEEDS_RESTART}"
            )),
        }
    }
    for component in new.keys() {
        if !old.contains_key(component) {
            out.push(format!(
                "the {component:?} system participant is newly derived: {NEEDS_RESTART}"
            ));
        }
    }
    out
}

/// The surfaces a reload brings into service: the arrivals and the new half of
/// every replacement.
///
/// One list because the two are the same work — a runtime to build, a bindings
/// document to publish, a registration and a set of subscriber entries to
/// install. What separates them is only whether something has to be retired
/// first, which each element carries as its [`Arrival`] so a caller never has
/// to re-scan the delta to find out which list a surface came from.
pub(crate) fn arriving(delta: &SurfaceDelta) -> Vec<(&ResolvedSurface, Arrival)> {
    delta
        .added
        .iter()
        .map(|surface| (surface, Arrival::Added))
        .chain(
            delta
                .changed
                .iter()
                .map(|change| (&change.new, Arrival::Replaced)),
        )
        .collect()
}

/// Which half of the delta an arriving surface came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Arrival {
    /// A surface this document declares and the running one did not.
    Added,
    /// The new value of a surface that was already running.
    Replaced,
}

/// Which description documents this reload has to rebuild.
///
/// The set is decided by what *moved*, and each body is then a function of the
/// whole candidate topology — the index lists every surface and a kind's help
/// lists every instance mounting it — so a surface's own document is rebuilt
/// only when that surface arrives, while a kind's pair is rebuilt whenever its
/// bytes moved or its instance set did.
///
/// A removal moves the index and the removed surface's kinds as surely as an
/// add does, which is why the removed side is read here and not only in
/// [`arriving`].
pub(crate) fn description_selection(
    delta: &SurfaceDelta,
    kinds_changed: &BTreeSet<String>,
    candidate: &[ResolvedSurface],
) -> DescriptionSelection {
    if delta.is_empty() && kinds_changed.is_empty() {
        return DescriptionSelection::default();
    }
    let moved_instances: BTreeSet<String> = delta
        .added
        .iter()
        .chain(delta.removed.iter())
        .chain(delta.changed.iter().map(|change| &change.old))
        .chain(delta.changed.iter().map(|change| &change.new))
        .flat_map(|surface| surface.components.iter().map(|comp| comp.kind.clone()))
        .collect();
    DescriptionSelection {
        index: true,
        surfaces: arriving(delta)
            .into_iter()
            .map(|(surface, _)| surface.slug.clone())
            .collect(),
        kinds: distinct_kinds(candidate)
            .into_iter()
            .filter(|kind| kinds_changed.contains(kind) || moved_instances.contains(kind))
            .collect(),
    }
}

/// The document parameters a reload rebuilds under: everything the bodies read
/// that is not the surface list itself.
///
/// Every field comes off a block level 1 has frozen, so these are the values
/// boot published under too; they are read from the candidate anyway, because
/// the candidate is what the bodies are a projection of.
pub(crate) struct SurfaceDocParams<'a> {
    /// Bare-name namespace rooting every derived channel address.
    pub prefix: &'a str,
    /// The build stamped into the index and the per-surface help documents.
    pub build_id: &'a str,
    /// Status document cadence, seconds.
    pub status_interval_secs: u32,
    /// `(channel address, publish floor)` from `[observability]`, or `None`.
    pub error_report: Option<(&'a str, LogLevel)>,
    /// `[messaging] max_body_bytes`, which every rebuilt body is held to here
    /// rather than at the publisher.
    pub max_body_bytes: usize,
}

/// What a reload rebuilds and republishes about surfaces, built in prepare so
/// that commit has no body left to be refused by.
#[derive(Debug, Default)]
pub(crate) struct SurfaceDocs {
    /// Description documents — the index, per-surface help, per-kind help and
    /// schema — published under `system:surface-help`.
    pub description: Vec<(String, String)>,
    /// Per-surface bindings documents, published under
    /// `system:surface-config`.
    pub bindings: Vec<(String, String)>,
    /// The surface-description participants whose code-built policy moved, with
    /// the registration the candidate derives for each. Swapped before either
    /// document set is published: the boot-installed policy holds one exact
    /// matcher per surface and per kind, so a document for a surface that did
    /// not exist at boot has no writer until this lands.
    pub registrations: Vec<(SubscriberEntryKind, SubscriberRegistration)>,
}

/// Everything the document build reads besides the parameters.
pub(crate) struct SurfaceDocInputs<'a> {
    /// The whole candidate surface list: every body is a function of all of it.
    pub surfaces: &'a [ResolvedSurface],
    /// The asset roots this reload's scan resolved, which is where a kind's
    /// sidecar help and schema files are read from.
    pub roots: &'a SurfaceRoots,
    pub delta: &'a SurfaceDelta,
    /// Kinds the two scans of the declared mounts disagree about.
    pub kinds_changed: &'a BTreeSet<String>,
    /// The participants a fresh boot of the running document derives.
    pub baseline_participants: &'a [SystemParticipantSpec],
    /// The participants the candidate derives, and the registrations built from
    /// them.
    pub candidate_participants: &'a [SystemParticipantSpec],
    pub candidate_registrations: &'a HashMap<SubscriberEntryKind, SubscriberRegistration>,
}

/// Build every document this reload republishes, and pick the registrations it
/// has to swap first.
///
/// Runs in prepare, so a body that cannot be published is a refusal naming the
/// address and both sizes rather than a panic in the middle of the walk. The
/// bound is the publisher's own gate, asked here: `max_body_bytes` is not
/// convergible, so a body that fits now fits at commit.
pub(crate) fn build_surface_docs(
    inputs: &SurfaceDocInputs<'_>,
    params: &SurfaceDocParams<'_>,
) -> Result<SurfaceDocs, Vec<String>> {
    let selection = description_selection(inputs.delta, inputs.kinds_changed, inputs.surfaces);
    let description = build_description_docs_selected(
        params.prefix,
        params.build_id,
        inputs.surfaces,
        inputs.roots,
        &selection,
    );
    let bindings = build_bindings_documents(
        arriving(inputs.delta)
            .into_iter()
            .map(|(surface, _)| surface),
        &BindingsDocParams {
            prefix: params.prefix,
            status_interval_secs: params.status_interval_secs,
            error_report: params.error_report,
        },
    );

    let refusals: Vec<String> = description
        .iter()
        .chain(bindings.iter())
        .filter_map(|(address, body)| {
            check_body_size(body, params.max_body_bytes).err().map(
                |BodySizeExceeded { len, max }| {
                    format!(
                        "the document this reload would publish onto {address} is {len} bytes \
                         but [messaging] max_body_bytes is {max}; raise max_body_bytes above \
                         {len} (a restart — [messaging] is not convergible) or shrink what \
                         the document describes"
                    )
                },
            )
        })
        .collect();
    if !refusals.is_empty() {
        return Err(refusals);
    }

    Ok(SurfaceDocs {
        description,
        bindings,
        registrations: moved_registrations(inputs),
    })
}

/// The surface-description participants whose policy the candidate derives
/// differently, with the registration to install for each.
///
/// A removal narrows the specs as surely as an add widens them, so this is
/// asked on every reload and not only on the arriving side.
///
/// # Panics
///
/// If the candidate derives one of the two specs and no registration for it,
/// or derives no spec at all where the baseline did. Both come off one pass
/// over one list and the planner pushes both participants unconditionally, so
/// either is a host wiring bug — and skipping the second would leave the
/// baseline's publish policy live under a candidate that never named it, which
/// is the one thing this facility promises not to do.
fn moved_registrations(
    inputs: &SurfaceDocInputs<'_>,
) -> Vec<(SubscriberEntryKind, SubscriberRegistration)> {
    let mut out = Vec::new();
    for component in SURFACE_DESCRIPTION_PARTICIPANTS {
        let old = named(inputs.baseline_participants, component);
        let new = named(inputs.candidate_participants, component);
        if old == new {
            continue;
        }
        assert!(
            new.is_some(),
            "the baseline derives the {component:?} system participant and the candidate does \
             not — the planner pushes both unconditionally, so this is a host bug. Leaving the \
             baseline's registration live would run the candidate under a publish policy naming \
             surfaces it does not have.",
        );
        let key = SubscriberEntryKind::System(component.to_string());
        let registration = inputs
            .candidate_registrations
            .get(&key)
            .unwrap_or_else(|| {
                panic!(
                    "the candidate derives the {component:?} system participant but no \
                     registration for it — the two come off one pass over one list"
                )
            })
            .clone();
        out.push((key, registration));
    }
    out
}

fn named<'a>(
    specs: &'a [SystemParticipantSpec],
    component: &str,
) -> Option<&'a SystemParticipantSpec> {
    specs.iter().find(|spec| spec.component == component)
}

/// The participants a plan derives, keyed by component, with the
/// surface-description pair removed.
fn compared(specs: &[SystemParticipantSpec]) -> BTreeMap<&str, &SystemParticipantSpec> {
    specs
        .iter()
        .filter(|spec| !SURFACE_DESCRIPTION_PARTICIPANTS.contains(&spec.component))
        .map(|spec| (spec.component, spec))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    use brenn_lib::messaging::ChannelScheme;
    use brenn_surface_server::fixtures_config::SurfaceFixture;

    // -----------------------------------------------------------------------
    // The surface delta.
    // -----------------------------------------------------------------------

    /// A one-component surface of kind `chart`, optionally reading and writing
    /// one `brenn:` channel each.
    fn surface(slug: &str, reads: Option<&str>, writes: Option<&str>) -> ResolvedSurface {
        let mut fixture = SurfaceFixture::new(slug, "chart");
        if let Some(address) = reads {
            fixture = fixture.subscribe(address, "chart", "in");
        }
        if let Some(address) = writes {
            fixture = fixture.output(address, "chart", "out");
        }
        fixture.build()
    }

    /// The delta with both closures empty: value differences only.
    fn value_delta(baseline: &[ResolvedSurface], candidate: &[ResolvedSurface]) -> SurfaceDelta {
        closed_delta(baseline, candidate, &HashSet::new(), &BTreeSet::new())
    }

    fn closed_delta(
        baseline: &[ResolvedSurface],
        candidate: &[ResolvedSurface],
        moved_addresses: &HashSet<String>,
        kinds_changed: &BTreeSet<String>,
    ) -> SurfaceDelta {
        let moved_channels = moved_uuids(baseline, candidate, moved_addresses);
        surface_delta(
            baseline,
            candidate,
            &SurfaceClosure {
                moved_channels: &moved_channels,
                moved_addresses,
                kinds_changed,
            },
        )
    }

    /// The uuids the named addresses wear on either side's wire subscriptions.
    /// A fixture's derived subscriptions carry a nil uuid, so a case that wants
    /// the uuid arm states its own; this is what joins the two.
    fn moved_uuids(
        baseline: &[ResolvedSurface],
        candidate: &[ResolvedSurface],
        addresses: &HashSet<String>,
    ) -> HashSet<Uuid> {
        baseline
            .iter()
            .chain(candidate)
            .flat_map(|surface| surface.wire_subscriptions.iter())
            .filter(|sub| addresses.contains(&sub.subscription.channel_address))
            .map(|sub| sub.subscription.channel_uuid)
            .collect()
    }

    fn moved(addresses: &[&str]) -> HashSet<String> {
        addresses.iter().map(|a| (*a).to_string()).collect()
    }

    fn slugs(surfaces: &[ResolvedSurface]) -> Vec<&str> {
        surfaces.iter().map(|s| s.slug.as_str()).collect()
    }

    // -----------------------------------------------------------------------
    // The description selection and the documents built from it.
    // -----------------------------------------------------------------------

    fn kinds(names: &[&str]) -> BTreeSet<String> {
        names.iter().map(|n| (*n).to_string()).collect()
    }

    fn names(set: &BTreeSet<String>) -> Vec<&str> {
        set.iter().map(String::as_str).collect()
    }

    /// A surface of a named kind, binding nothing.
    fn of_kind(slug: &str, kind: &str) -> ResolvedSurface {
        SurfaceFixture::new(slug, kind).build()
    }

    #[test]
    fn a_reload_that_moved_no_surface_and_no_kind_selects_no_document() {
        let selection = description_selection(
            &SurfaceDelta::default(),
            &BTreeSet::new(),
            &[of_kind("wall", "chart")],
        );
        assert!(selection.is_empty());
    }

    /// An arriving surface takes its own help document; the index moves because
    /// it lists every surface, and the kind's pair because it lists every
    /// instance mounting it.
    #[test]
    fn an_arriving_surface_selects_the_index_its_own_help_and_its_kind() {
        let before = vec![of_kind("wall", "chart")];
        let after = vec![of_kind("wall", "chart"), of_kind("kiosk", "chart")];
        let delta = value_delta(&before, &after);
        let selection = description_selection(&delta, &BTreeSet::new(), &after);
        assert!(selection.index);
        assert_eq!(names(&selection.surfaces), vec!["kiosk"]);
        assert_eq!(names(&selection.kinds), vec!["chart"]);
    }

    /// A removal selects no per-surface document — the retired surface's own
    /// help is not rebuilt — but does move the index and the help of every kind
    /// it was mounting that another surface still mounts.
    #[test]
    fn a_removal_selects_the_index_and_the_kind_it_vacated() {
        let before = vec![of_kind("wall", "chart"), of_kind("kiosk", "chart")];
        let after = vec![of_kind("kiosk", "chart")];
        let delta = value_delta(&before, &after);
        let selection = description_selection(&delta, &BTreeSet::new(), &after);
        assert!(selection.index);
        assert!(selection.surfaces.is_empty());
        assert_eq!(names(&selection.kinds), vec!["chart"]);
    }

    /// The last surface of a kind leaving takes the kind's documents out of the
    /// derived set entirely: they are no longer addresses this topology has, so
    /// the selection names them for nobody to build.
    #[test]
    fn the_last_instance_of_a_kind_leaving_selects_nothing_for_that_kind() {
        let before = vec![of_kind("wall", "chart"), of_kind("kiosk", "gauge")];
        let after = vec![of_kind("kiosk", "gauge")];
        let delta = value_delta(&before, &after);
        let selection = description_selection(&delta, &BTreeSet::new(), &after);
        assert!(selection.index);
        assert!(selection.kinds.is_empty());
    }

    /// A surface that swapped kinds selects the kind it took up. The vacated
    /// kind is selected only when the candidate still has it — a kind no
    /// candidate surface mounts derives no channels to publish onto.
    #[test]
    fn a_surface_that_swapped_kinds_selects_the_kind_it_took_up() {
        let before = vec![of_kind("wall", "chart")];
        let after = vec![of_kind("wall", "gauge")];
        let delta = value_delta(&before, &after);
        assert_eq!(delta.changed.len(), 1, "the surface's value moved");
        let selection = description_selection(&delta, &BTreeSet::new(), &after);
        assert!(selection.index);
        assert_eq!(names(&selection.surfaces), vec!["wall"]);
        assert_eq!(names(&selection.kinds), vec!["gauge"]);
    }

    /// The `changed.old` chain, on the one shape that needs it: a surface swaps
    /// kinds while a sibling keeps the vacated one. That kind's help lists
    /// every mounting instance, so it has to be rebuilt without it — and the
    /// only place the reload learns the kind was vacated is the old value.
    #[test]
    fn a_kind_a_changed_surface_vacated_is_selected_while_a_sibling_holds_it() {
        let before = vec![of_kind("wall", "chart"), of_kind("kiosk", "chart")];
        let after = vec![of_kind("wall", "gauge"), of_kind("kiosk", "chart")];
        let delta = value_delta(&before, &after);
        let selection = description_selection(&delta, &BTreeSet::new(), &after);
        assert_eq!(names(&selection.surfaces), vec!["wall"]);
        assert_eq!(
            names(&selection.kinds),
            vec!["chart", "gauge"],
            "the vacated kind's help still lists the surface that left it",
        );
    }

    /// A bundle upgrade that moved a kind's bytes under an unchanged document:
    /// no surface moved, and the kind's help and schema are still rebuilt.
    #[test]
    fn a_kind_whose_bytes_moved_is_selected_with_no_surface_moving() {
        let candidate = vec![of_kind("wall", "chart")];
        let selection =
            description_selection(&SurfaceDelta::default(), &kinds(&["chart"]), &candidate);
        assert!(selection.index);
        assert!(selection.surfaces.is_empty());
        assert_eq!(names(&selection.kinds), vec!["chart"]);
    }

    /// An upgraded kind no surface instantiates has no derived documents, so it
    /// selects none — the index still moves, because the scan disagreed about
    /// what the mounts offer.
    #[test]
    fn an_upgraded_kind_nothing_instantiates_selects_no_document_of_its_own() {
        let candidate = vec![of_kind("wall", "chart")];
        let selection =
            description_selection(&SurfaceDelta::default(), &kinds(&["gauge"]), &candidate);
        assert!(selection.index);
        assert!(selection.kinds.is_empty());
    }

    #[test]
    fn arriving_is_the_added_surfaces_and_the_new_half_of_every_change() {
        let before = vec![of_kind("wall", "chart"), of_kind("kiosk", "chart")];
        let after = vec![
            of_kind("wall", "gauge"),
            of_kind("kiosk", "chart"),
            of_kind("desk", "chart"),
        ];
        let delta = value_delta(&before, &after);
        let mut arrived: Vec<&str> = arriving(&delta)
            .into_iter()
            .map(|(surface, _)| surface.slug.as_str())
            .collect();
        arrived.sort_unstable();
        assert_eq!(arrived, vec!["desk", "wall"]);
    }

    /// The two publish-only participants, with the help side carrying whatever
    /// matchers a case wants to move.
    fn participants(help_channels: &[&str]) -> Vec<SystemParticipantSpec> {
        let bare: Vec<String> = help_channels.iter().map(|c| (*c).to_string()).collect();
        vec![
            SystemParticipantSpec::publish_only(
                SURFACE_HELP_COMPONENT,
                ChannelScheme::Brenn,
                &bare,
            ),
            SystemParticipantSpec::publish_only(
                SURFACE_CONFIG_COMPONENT,
                ChannelScheme::Ephemeral,
                &[],
            ),
        ]
    }

    fn params(max_body_bytes: usize) -> SurfaceDocParams<'static> {
        SurfaceDocParams {
            prefix: "surface",
            build_id: "test-build",
            status_interval_secs: 60,
            error_report: None,
            max_body_bytes,
        }
    }

    /// The whole input bundle for one case: two participant lists, the
    /// registrations the candidate derives, and the delta between two surface
    /// lists.
    struct DocCase {
        candidate: Vec<ResolvedSurface>,
        delta: SurfaceDelta,
        kinds_changed: BTreeSet<String>,
        baseline_participants: Vec<SystemParticipantSpec>,
        candidate_participants: Vec<SystemParticipantSpec>,
        registrations: HashMap<SubscriberEntryKind, SubscriberRegistration>,
        roots: SurfaceRoots,
    }

    impl DocCase {
        fn new(baseline: Vec<ResolvedSurface>, candidate: Vec<ResolvedSurface>) -> Self {
            let delta = value_delta(&baseline, &candidate);
            let baseline_participants = participants(&["surface.surface.wall.help"]);
            let candidate_participants = participants(&["surface.surface.wall.help"]);
            let registrations =
                brenn_messaging::system::registrations_from_specs(&candidate_participants);
            Self {
                candidate,
                delta,
                kinds_changed: BTreeSet::new(),
                baseline_participants,
                candidate_participants,
                registrations,
                roots: SurfaceRoots::default(),
            }
        }

        /// Move the help participant's matcher set, as an added or retired
        /// surface does.
        fn help_matchers(mut self, channels: &[&str]) -> Self {
            self.candidate_participants = participants(channels);
            self.registrations =
                brenn_messaging::system::registrations_from_specs(&self.candidate_participants);
            self
        }

        fn build(&self, max_body_bytes: usize) -> Result<SurfaceDocs, Vec<String>> {
            build_surface_docs(
                &SurfaceDocInputs {
                    surfaces: &self.candidate,
                    roots: &self.roots,
                    delta: &self.delta,
                    kinds_changed: &self.kinds_changed,
                    baseline_participants: &self.baseline_participants,
                    candidate_participants: &self.candidate_participants,
                    candidate_registrations: &self.registrations,
                },
                &params(max_body_bytes),
            )
        }
    }

    fn addresses(docs: &[(String, String)]) -> Vec<&str> {
        docs.iter().map(|(address, _)| address.as_str()).collect()
    }

    /// One bindings document per arriving surface, and the description set the
    /// selection named — with no document for the surface that stayed.
    #[test]
    fn an_added_surface_builds_its_bindings_document_and_the_moved_descriptions() {
        let case = DocCase::new(
            vec![of_kind("wall", "chart")],
            vec![of_kind("wall", "chart"), of_kind("kiosk", "chart")],
        );
        let docs = case.build(1_000_000).expect("nothing here is oversize");
        assert_eq!(
            addresses(&docs.bindings),
            vec!["ephemeral:surface.surface.kiosk.bindings"]
        );
        assert_eq!(
            addresses(&docs.description),
            vec![
                "brenn:surface.index",
                "brenn:surface.surface.kiosk.help",
                "brenn:surface.kind.chart.help",
                "brenn:surface.kind.chart.schema",
            ]
        );
    }

    /// A removal republishes the index and the surviving kind's pair and builds
    /// no bindings document at all.
    #[test]
    fn a_removal_builds_descriptions_and_no_bindings_document() {
        let case = DocCase::new(
            vec![of_kind("wall", "chart"), of_kind("kiosk", "chart")],
            vec![of_kind("kiosk", "chart")],
        );
        let docs = case.build(1_000_000).expect("nothing here is oversize");
        assert!(docs.bindings.is_empty());
        assert_eq!(
            addresses(&docs.description),
            vec![
                "brenn:surface.index",
                "brenn:surface.kind.chart.help",
                "brenn:surface.kind.chart.schema",
            ]
        );
    }

    /// The publisher's own gate, asked in prepare: the refusal names the
    /// address, the body's size and the configured ceiling.
    #[test]
    fn an_oversize_document_is_a_refusal_naming_the_address_and_both_sizes() {
        let case = DocCase::new(vec![], vec![of_kind("wall", "chart")]);
        let refusals = case.build(16).expect_err("16 bytes holds no document");
        assert!(
            refusals
                .iter()
                .any(|r| r.contains("brenn:surface.index") && r.contains("max_body_bytes is 16")),
            "{refusals:?}"
        );
        assert!(
            refusals
                .iter()
                .any(|r| r.contains("ephemeral:surface.surface.wall.bindings")),
            "{refusals:?}"
        );
    }

    /// A participant whose matcher set moved is swapped; the one that did not
    /// is left alone.
    #[test]
    fn only_the_participant_whose_policy_moved_is_swapped() {
        let case = DocCase::new(
            vec![of_kind("wall", "chart")],
            vec![of_kind("wall", "chart"), of_kind("kiosk", "chart")],
        )
        .help_matchers(&["surface.surface.wall.help", "surface.surface.kiosk.help"]);
        let docs = case.build(1_000_000).expect("nothing here is oversize");
        assert_eq!(docs.registrations.len(), 1);
        assert_eq!(
            docs.registrations[0].0,
            SubscriberEntryKind::System(SURFACE_HELP_COMPONENT.to_string())
        );
    }

    #[test]
    fn an_unmoved_participant_pair_swaps_nothing() {
        let case = DocCase::new(
            vec![of_kind("wall", "chart")],
            vec![of_kind("wall", "chart"), of_kind("kiosk", "chart")],
        );
        let docs = case.build(1_000_000).expect("nothing here is oversize");
        assert!(docs.registrations.is_empty());
    }

    #[test]
    fn two_identical_surface_lists_move_nothing() {
        let before = vec![surface("wall", Some("brenn:a"), None)];
        let after = before.clone();
        assert!(value_delta(&before, &after).is_empty());
    }

    /// Declaration order is not identity: the delta is keyed by slug, so a
    /// reordered list is not three moves.
    #[test]
    fn a_reordered_surface_list_moves_nothing() {
        let before = vec![surface("wall", None, None), surface("kiosk", None, None)];
        let after = vec![surface("kiosk", None, None), surface("wall", None, None)];
        assert!(value_delta(&before, &after).is_empty());
    }

    #[test]
    fn a_surface_that_arrives_or_leaves_is_added_or_removed() {
        let before = vec![surface("wall", None, None)];
        let after = vec![surface("kiosk", None, None)];
        let delta = value_delta(&before, &after);
        assert_eq!(slugs(&delta.added), vec!["kiosk"]);
        assert_eq!(slugs(&delta.removed), vec!["wall"]);
        assert!(delta.changed.is_empty());
    }

    /// A surface that kept its slug and moved its value is one entry to walk,
    /// carrying both sides — the old is what gets unwired, the new is what gets
    /// started.
    #[test]
    fn a_surface_whose_value_moved_is_changed_and_carries_both_sides() {
        let before = vec![surface("wall", Some("brenn:a"), None)];
        let after = vec![surface("wall", Some("brenn:b"), None)];
        let delta = value_delta(&before, &after);
        assert!(delta.added.is_empty() && delta.removed.is_empty());
        assert_eq!(delta.changed.len(), 1);
        let change = &delta.changed[0];
        assert_eq!(change.old.subscriptions[0].channel_address, "brenn:a");
        assert_eq!(change.new.subscriptions[0].channel_address, "brenn:b");
    }

    /// Channel closure, input side: the surface's own value did not move, but
    /// the entry it reads did, so what it would be wired to is not what it is
    /// wired to.
    #[test]
    fn a_surface_reading_a_moved_channel_is_changed() {
        let mut before = vec![surface("wall", Some("brenn:a"), None)];
        before[0].wire_subscriptions[0].subscription.channel_uuid = Uuid::from_u128(3);
        let after = before.clone();
        let delta = closed_delta(&before, &after, &moved(&["brenn:a"]), &BTreeSet::new());
        assert_eq!(delta.changed.len(), 1, "the read channel moved");
        assert!(value_delta(&before, &after).is_empty(), "and only that");
    }

    /// Channel closure, output side. An output binding names a channel by
    /// address and never resolves a uuid, so the address set is what answers
    /// for it.
    #[test]
    fn a_surface_writing_a_moved_channel_is_changed() {
        let before = vec![surface("wall", None, Some("brenn:out"))];
        let after = before.clone();
        let delta = closed_delta(&before, &after, &moved(&["brenn:out"]), &BTreeSet::new());
        assert_eq!(delta.changed.len(), 1);
    }

    /// A channel neither side binds moving is not this surface's business.
    #[test]
    fn a_surface_bound_to_nothing_that_moved_is_untouched() {
        let before = vec![surface("wall", Some("brenn:a"), Some("brenn:out"))];
        let after = before.clone();
        let delta = closed_delta(
            &before,
            &after,
            &moved(&["brenn:elsewhere"]),
            &BTreeSet::new(),
        );
        assert!(delta.is_empty());
    }

    /// Both sides' bindings are read: a channel the candidate stopped binding
    /// is named only by the baseline's value.
    #[test]
    fn a_channel_only_the_old_side_bound_still_promotes_the_surface() {
        let before = vec![surface("wall", None, Some("brenn:gone"))];
        let after = vec![surface("wall", None, None)];
        let delta = closed_delta(&before, &after, &moved(&["brenn:gone"]), &BTreeSet::new());
        assert_eq!(delta.changed.len(), 1);
    }

    /// Kind closure: a bundle upgrade moves the fingerprint of a kind whose
    /// instances nobody edited, and every surface mounting it is re-derived.
    #[test]
    fn a_surface_instantiating_a_moved_kind_is_changed() {
        let before = vec![surface("wall", None, None), surface("kiosk", None, None)];
        let after = before.clone();
        let kinds: BTreeSet<String> = ["chart".to_string()].into_iter().collect();
        let delta = closed_delta(&before, &after, &HashSet::new(), &kinds);
        assert_eq!(delta.changed.len(), 2, "both mount the kind");
        let kinds: BTreeSet<String> = ["gauge".to_string()].into_iter().collect();
        assert!(
            closed_delta(&before, &after, &HashSet::new(), &kinds).is_empty(),
            "a kind no surface instantiates promotes nobody",
        );
    }

    /// An arriving surface is `added`, never `changed`: there is nothing to
    /// retire, and the closure has no old value to ask about.
    #[test]
    fn an_arriving_surface_is_not_promoted_by_a_closure() {
        let before: Vec<ResolvedSurface> = vec![];
        let after = vec![surface("wall", Some("brenn:a"), None)];
        let kinds: BTreeSet<String> = ["chart".to_string()].into_iter().collect();
        let delta = closed_delta(&before, &after, &moved(&["brenn:a"]), &kinds);
        assert_eq!(slugs(&delta.added), vec!["wall"]);
        assert!(delta.changed.is_empty());
    }

    // -----------------------------------------------------------------------
    // Rule 7.
    // -----------------------------------------------------------------------

    fn spec(component: &'static str, channels: &[&str]) -> SystemParticipantSpec {
        let bare: Vec<String> = channels.iter().map(|c| (*c).to_string()).collect();
        SystemParticipantSpec::publish_only(component, ChannelScheme::Brenn, &bare)
    }

    #[test]
    fn two_plans_deriving_the_same_participants_refuse_nothing() {
        let one = vec![spec("tool-executor", &["a"]), spec("roster", &["b"])];
        let two = vec![spec("roster", &["b"]), spec("tool-executor", &["a"])];
        assert!(system_participant_refusals(&one, &two).is_empty());
    }

    #[test]
    fn a_participant_whose_policy_moved_is_refused() {
        let one = vec![spec("tool-executor", &["a"])];
        let two = vec![spec("tool-executor", &["a", "b"])];
        let refusals = system_participant_refusals(&one, &two);
        assert_eq!(refusals.len(), 1, "{refusals:?}");
        assert!(refusals[0].contains("\"tool-executor\" system participant's code-built policy"));
        assert!(refusals[0].ends_with(NEEDS_RESTART));
    }

    #[test]
    fn a_participant_that_arrives_or_leaves_is_refused() {
        let one = vec![spec("tool-executor", &["a"])];
        let two = vec![spec("cc-profile", &["c"])];
        let refusals = system_participant_refusals(&one, &two);
        assert_eq!(refusals.len(), 2, "{refusals:?}");
        assert!(refusals[0].contains("\"tool-executor\" system participant is no longer derived"));
        assert!(refusals[1].contains("\"cc-profile\" system participant is newly derived"));
    }

    /// The surface-description pair is the one whose matchers follow the
    /// surface list, so it is never what this rule reports.
    #[test]
    fn the_surface_description_pair_is_set_aside() {
        let one = vec![
            spec(SURFACE_HELP_COMPONENT, &["surface.index"]),
            spec(SURFACE_CONFIG_COMPONENT, &["surface.config.bar"]),
        ];
        let two = vec![spec(SURFACE_HELP_COMPONENT, &["surface.index", "x"])];
        assert!(system_participant_refusals(&one, &two).is_empty());
    }
}
