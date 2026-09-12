//! The prepare phase: deciding what a reload would do to *this* process.
//!
//! [`compare`](super::compare) and [`delta`](super::delta) answer questions
//! about two documents and two plans. This module is what holds a document and
//! a plan to compare against — the **baseline**, the projection the process is
//! actually running — re-reads the tree on disk, and asks them.
//!
//! Prepare is fallible and touches nothing. Every step below either produces a
//! refusal, in which case the running system is exactly as it was and the
//! operator is told what needs a restart, or it produces a [`ReadyReload`]: a
//! candidate document, its plan, the delta to walk, and every component the
//! delta needs already loaded and instantiated. Applying that is the commit
//! phase's job, and the hard line between the two is what makes commit
//! infallible: by the time it runs, everything that could have refused already
//! has.
//!
//! Two panics are caught here rather than allowed to kill the process, and they
//! are caught for different reasons:
//!
//! - The **planner's** asserts are the same population `brenn config-check`
//!   catches, so this module reuses that tool's reading of what counts as a
//!   refusal — but not its verdict on a payload that does not read as one.
//!   `config-check` re-panics there, which costs a CLI exit; here it would cost
//!   the process a healthy operator is still being served by, over a document
//!   nothing has applied. So an unrecognized payload is reported as a refusal
//!   too, with a line saying it may be a host defect and a `warn!` carrying the
//!   whole of it to the journal.
//! - The **environment** asserts — a package no root holds, a store parent that
//!   does not exist, an artifact its record does not bind — are boot-only
//!   spellings with no marker discipline, so anything they say is read as a
//!   refusal. The direction is the safe one: prepare has mutated nothing, so a
//!   defect misread as a refusal costs one reload, while a refusal misread as a
//!   defect kills a healthy process over a document it never had to accept.

use std::collections::{BTreeSet, HashMap};
use std::panic::AssertUnwindSafe;
use std::path::PathBuf;
use std::sync::Arc;

use indexmap::IndexMap;
use tracing::{info, warn};

use brenn_lib::config::{
    AppConfig, DocumentInputs, LoadedDocument, LoadedMounts, Roots, check_config,
    deployment_inputs, try_load_mounts,
};
use brenn_lib::messaging::MessagingDirectory;
use brenn_lib::messaging::config::{ResolvedSurface, ResolvedWasmConsumer};
use brenn_lib::messaging::gates::{BodySizeExceeded, check_body_size};
use brenn_lib::mqtt::config::{MqttClientIdentity, ResolvedMqttIngressChannel};
use brenn_lib::panic_util::{catch_quietly, panic_message};
use brenn_lib::wasm_package::Verified;
use brenn_messaging::Messenger;
use brenn_messaging_boot::{MessagingPlan, PlanInputs, plan_messaging};
use brenn_obs::alerting::{AlertDispatcher, AlertSeverity};

use crate::consumers::{ConsumerLoadContext, ConsumerRegistry, LoadedConsumer, load_consumer};
use crate::reload::agents::AgentInputs;
use crate::reload::compare::non_convergible_differences;
use crate::reload::delta::{LiveFacts, PlanDelta, PlanFacts, convergibility_refusals, plan_delta};
use crate::reload::dynamic::DynamicSnapshot;
use crate::reload::mqtt::mqtt_clients_delta;
use crate::reload::surfaces::{
    SurfaceDocInputs, SurfaceDocParams, SurfaceDocs, arriving, build_surface_docs,
    system_participant_refusals,
};
use crate::reload::webhook::{WebhookArrivals, replay_releases, webhook_delta};
use brenn_messaging::config_reload::{
    Outcome, ReloadStatus, STATUS_VERSION, StatusDelta, StatusMount, Trigger, now, publish_status,
    refusal_alert_body,
};
use brenn_messaging::system::SystemParticipantSpec;

/// Which door a reload came through.
///
/// Boot is not here: boot publishes its own outcome and never runs prepare.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TriggerSource {
    /// A message on the request channel.
    Bus,
    /// `SIGUSR1`.
    Signal,
}

impl From<TriggerSource> for Trigger {
    fn from(source: TriggerSource) -> Self {
        match source {
            TriggerSource::Bus => Trigger::Bus,
            TriggerSource::Signal => Trigger::Signal,
        }
    }
}

/// Everything prepare reads that is neither the document on disk nor the
/// baseline: the boot-time plan inputs, the services a consumer's load is wired
/// to, and the two channels an outcome is reported through.
///
/// Every plan input here is a *booted* value, which is legitimate exactly
/// because level 1 refuses any candidate that would have moved one: an app
/// map, a tool registry and an integration registry are all projections of
/// blocks a reload cannot converge. The roots are not among
/// them: they are the mounts document's answer, re-read on every reload, so a
/// bundle installed since boot is a tree this process may read the moment the
/// operator declared it.
pub(crate) struct ReloadEnv {
    /// Where the deployment document is. Re-read on every reload: the point of
    /// the facility is that the bytes may have changed since.
    ///
    /// What its packaged imports resolve against is *not* here: the module
    /// roots are the mounts document's, re-derived on every reload, so the
    /// inputs a candidate compiles under are built per reload rather than held.
    pub config_path: PathBuf,
    /// The root document's path as the status body reports it.
    pub root: Option<String>,
    /// This binary's build identifier, stamped into the description documents a
    /// reload rebuilds. A process constant: the documents boot published carry
    /// it too, and a rebuilt one that carried a different value would advertise
    /// a build that nothing is running.
    pub build_id: &'static str,
    /// The agent table this process reads through. Held rather than
    /// snapshotted: a reload plans against the map the gates are reading now.
    pub apps: brenn_lib::config::AppTable,
    /// The integration factories this process was built with. Immutable after
    /// construction; every reload resolves the same factories.
    pub integration_registry: Arc<brenn_lib::integration::IntegrationRegistry>,
    /// The validated `XDG_RUNTIME_DIR` boot resolved, present exactly when some
    /// agent is bare. A candidate cannot change that: `container` is refused at
    /// level 1, so the same answer serves every reload.
    pub runtime_dir: Option<PathBuf>,
    pub tool_registry: Arc<brenn_tool_registry::ToolRegistry>,
    /// The endpoint table every inbound webhook request is matched against.
    ///
    /// Held rather than snapshotted, for the reason the surface cell is: a
    /// reload's baseline for endpoints is what the HTTP layer is serving right
    /// now, and commit swaps this same table.
    pub webhook: Arc<brenn_webhook::WebhookService>,
    /// The cell holding the surface asset roots this process is serving from —
    /// the same cell `/surface-static` resolves against, not a copy of it.
    ///
    /// Held rather than snapshotted: a reload compares the declared mounts'
    /// answer against what is actually being served, and then installs the new
    /// answer in this same cell, so a kind whose bytes moved is served from the
    /// tree the reload scanned.
    pub surface_roots: Arc<std::sync::RwLock<Arc<brenn_surface_server::SurfaceRoots>>>,
    /// The runtime table the surface doors read, and the mid-swap marks that
    /// keep a page reconnecting through a reconfiguration out of the security
    /// event log. The commit's surface steps write it.
    pub surfaces: brenn_server::state::SurfaceCell,
    /// The live attach sessions, by attacher. A retired or replaced surface's
    /// pages are closed through it, with the reason that tells each page
    /// whether to reload or to stop.
    pub attach_registry: brenn_attach_server::registry::AttachRegistry,
    /// The broker client registry. Always present; empty when the document
    /// declares no `[[mqtt_client]]`.
    pub mqtt_service: Arc<brenn_mqtt::MqttService>,
    /// The concrete ingress router. Always present.
    pub mqtt_event_router: Arc<brenn_server::mqtt_router::MqttEventRouterImpl>,
    pub max_payload_bytes: usize,
    /// The live Claude Code sessions. A reload condemns the ones whose agent's
    /// per-process view moved, or whose conversation belongs to a user the
    /// candidate no longer allows; each dies at its next idle moment and the
    /// wake path spawns its successor from the swapped table.
    pub active_bridges: brenn_server::active_bridge::ActiveBridges,
    /// Pulsed once per commit that removed a user from some agent, so every
    /// open WebSocket re-asks the connect-time access question against the
    /// swapped table and closes itself when the answer changed.
    pub apps_swapped_tx: tokio::sync::broadcast::Sender<()>,
    pub messenger: Arc<Messenger>,
    /// The wake router, whose delivery bindings a consumer joins and leaves
    /// through.
    pub router: Arc<brenn_server::messaging_router::WakeRouterImpl>,
    /// The async tool executor's per-caller grant table, when this process has
    /// one. `None` where no async tool is registered — the executor and its
    /// table do not exist then, and a fresh boot of the candidate would not
    /// build them either.
    pub tool_caller_grants: Option<Arc<brenn_tool_registry::ToolCallerGrants>>,
    pub alert_dispatcher: AlertDispatcher,
}

impl ReloadEnv {
    /// The asset roots the process is serving right now, as one `Arc` clone.
    pub(crate) fn surface_roots(&self) -> Arc<brenn_surface_server::SurfaceRoots> {
        self.surface_roots
            .read()
            .expect("the surface-roots lock is held only for a clone and a swap")
            .clone()
    }
}

/// The document the process is projecting, and the projection itself.
///
/// The directory is a detached **snapshot** rather than the live one: the live
/// directory is edited after boot — dynamic app subscriptions, attach-minted
/// surface and remote entries — and a baseline that drifted with it would stop
/// being what a fresh boot of the baseline document produces, which is the one
/// thing it has to be. Detaching costs nothing: directory mutation is
/// copy-on-write, so the `Arc`s a `list()` hands out are already the entries as
/// they stood.
pub(crate) struct Baseline {
    pub document: LoadedDocument,
    /// The mounts document as this process last read it, and the roots it
    /// derived. The status body reports these on a refusal, which is the one
    /// outcome where what the process is reading is not what the candidate
    /// declared.
    pub mounts: LoadedMounts,
    directory: MessagingDirectory,
    consumers: Vec<ResolvedWasmConsumer>,
    /// The plan's static `mqtt:` ingress channels — what the broker set of a
    /// fresh boot of this document would be. Boot's own list has the
    /// re-activated dynamic subscriptions appended to it; those are not here,
    /// for the same reason the directory is a plan snapshot rather than the
    /// live one.
    mqtt_ingress: Vec<ResolvedMqttIngressChannel>,
    /// The system participants a fresh boot of this document derives. Held so
    /// rule 7 has a previous value: every one of them but the
    /// surface-description pair comes off a block that level 1 has frozen, and
    /// a participant that moved anyway is not something a walk can converge.
    system_participants: Vec<SystemParticipantSpec>,
    /// The surfaces a fresh boot of this document resolves. The surface delta's
    /// previous value; like the directory, it is the *planned* list and not
    /// whatever the runtime table happens to hold.
    surfaces: Vec<ResolvedSurface>,
}

impl Baseline {
    /// Build a baseline from a document, the mounts it was read against, and
    /// the plan it lowered to.
    pub fn of(document: LoadedDocument, mounts: LoadedMounts, plan: &MessagingPlan) -> Self {
        Self {
            document,
            mounts,
            directory: snapshot(&plan.directory),
            consumers: plan.wasm_consumers.clone(),
            mqtt_ingress: plan.mqtt_ingress_channels.clone(),
            system_participants: plan.system_participants.clone(),
            surfaces: plan.surfaces.clone(),
        }
    }

    /// Build a baseline from the parts that survive after `commit_messaging`
    /// consumes the plan.
    ///
    /// The directory must be the *planned* one, not the live one — see
    /// [`Baseline`].
    pub fn from_parts(
        document: LoadedDocument,
        mounts: LoadedMounts,
        directory: MessagingDirectory,
        consumers: Vec<ResolvedWasmConsumer>,
        mqtt_ingress: Vec<ResolvedMqttIngressChannel>,
        system_participants: Vec<SystemParticipantSpec>,
        surfaces: Vec<ResolvedSurface>,
    ) -> Self {
        Self {
            document,
            mounts,
            directory,
            consumers,
            mqtt_ingress,
            system_participants,
            surfaces,
        }
    }
}

#[cfg(test)]
impl Baseline {
    /// The surfaces a fresh boot of the running document resolves.
    pub(crate) fn surfaces(&self) -> &[ResolvedSurface] {
        &self.surfaces
    }
}

/// A detached copy of a directory's entries, unaffected by later edits to it.
fn snapshot(directory: &MessagingDirectory) -> MessagingDirectory {
    MessagingDirectory::from_arcs(directory.list())
}

/// A reload that passed prepare: everything commit needs, and nothing left that
/// can refuse.
pub(crate) struct ReadyReload {
    pub document: LoadedDocument,
    /// The mounts this candidate was read against, which the baseline adopts
    /// when the walk is done.
    pub mounts: LoadedMounts,
    pub plan: MessagingPlan,
    pub delta: PlanDelta,
    /// One loaded component per consumer the delta adds or changes, by slug.
    /// Instantiated during prepare so that commit's "start this consumer" step
    /// cannot fail on an artifact.
    pub loaded: Vec<(String, LoadedConsumer)>,
    /// What every consumer in the candidate resolved to, by slug. Commit points
    /// the registry at these, so a package whose paths moved without its bytes
    /// moving — a re-deploy under the versioned-tree scheme — is recorded
    /// without a restart.
    pub records: HashMap<String, Verified>,
    /// The surface asset roots this reload's scan resolved. Commit installs
    /// them in the cell `/surface-static` reads, so a mount whose symlink was
    /// swapped onto a fresh versioned tree is served out of the tree that is
    /// still on disk rather than the one the installer is about to prune.
    pub surface_roots: brenn_surface_server::SurfaceRoots,
    /// One runtime per surface the delta brings into service, by slug. Built
    /// during prepare for the reason the consumers are: commit's "serve this
    /// surface" step must have nothing left to fail on.
    pub surface_runtimes: HashMap<String, Arc<brenn_surface_server::SurfaceRuntime>>,
    /// The description and bindings documents this reload republishes, and the
    /// surface-description registrations it swaps first. Built and size-checked
    /// in prepare, so commit publishes bodies already proved publishable.
    pub surface_docs: SurfaceDocs,
    /// The endpoint runtimes commit installs, the arriving replay stores it
    /// opens, and the retiring holders it drops first. A store file admits one
    /// holder, so a handover is "drop, then open" and never the reverse.
    pub webhook: WebhookArrivals,
    /// The `applied` outcome this reload will publish, built and measured in
    /// prepare.
    ///
    /// Constructed at one site so that the body commit publishes is the body
    /// prepare proved publishable, byte for byte apart from `at` — which
    /// commit restamps at a fixed width, so restamping moves no byte count.
    pub applied: ReloadStatus,
}

/// A document and what it lowers to: the pair a baseline is made of.
pub(crate) struct Projection {
    pub document: LoadedDocument,
    pub mounts: LoadedMounts,
    pub plan: MessagingPlan,
    /// What every consumer resolved to under this reload's roots. Adopted like
    /// the rest of the projection: an install that moved a package's paths
    /// without moving its bytes is `unchanged`, and the record the registry
    /// holds must still name the tree this process is now reading.
    pub records: HashMap<String, Verified>,
    /// The surface asset roots this reload's scan resolved. Adopted for the
    /// same reason the records are: a byte-identical re-install relocates every
    /// tree under the mount and produces no delta at all.
    pub surface_roots: brenn_surface_server::SurfaceRoots,
    /// The kinds whose installed bytes moved. Empty for every `unchanged`
    /// reload but one: a mount upgraded a kind no surface instantiates, which
    /// moves the roots this process serves from and moves nothing else. It is
    /// reported because the retained outcome is the installer's only evidence
    /// that the tree it just swapped in is the tree being served.
    pub kinds_changed: BTreeSet<String>,
}

/// What prepare decided.
pub(crate) enum Prepared {
    /// The candidate was not applied and nothing was touched.
    Refused {
        /// The candidate's identity, absent when it did not compile far enough
        /// to have one.
        document_sha256: Option<String>,
        refusals: Vec<String>,
    },
    /// The bytes on disk moved and the projection did not.
    Unchanged(Box<Projection>),
    /// The candidate may be committed.
    Ready(Box<ReadyReload>),
}

/// The state a reload is decided against, and the machinery to decide it.
///
/// One driver per process, owning the baseline and the generation counter. The
/// consumer registry lives here for the same reason: what is running and what
/// the document says should be running are two halves of one question, and
/// splitting their owners is how they come to disagree.
pub(crate) struct ReloadDriver {
    env: ReloadEnv,
    baseline: Baseline,
    registry: ConsumerRegistry,
    /// Applied reloads since boot. Boot published 0.
    generation: u64,
}

/// Readers a test needs and production does not: what the driver believes the
/// process is projecting, and what is in service. One struct definition for
/// both builds — a `cfg`-gated *field* would give the release build and the
/// test build different types, so a compile error on either would be invisible
/// to the other.
#[cfg(test)]
impl ReloadDriver {
    pub(crate) fn baseline(&self) -> &Baseline {
        &self.baseline
    }

    pub(crate) fn registry(&self) -> &ConsumerRegistry {
        &self.registry
    }

    /// The surface table, served asset roots, and attach registry.
    pub(crate) fn env(&self) -> &ReloadEnv {
        &self.env
    }
}

impl ReloadDriver {
    pub fn new(env: ReloadEnv, baseline: Baseline, registry: ConsumerRegistry) -> Self {
        Self {
            env,
            baseline,
            registry,
            generation: 0,
        }
    }

    /// Decide what the document on disk would do to this process.
    ///
    /// Reads the tree, the components roots and nothing else; writes nothing at
    /// all. The steps are ordered so that the cheapest refusal comes first, so
    /// that no package is resolved before the roots it would be resolved out of
    /// are proved a set of distinct releases, and so that nothing is
    /// instantiated until every verdict on the document itself has been
    /// reached.
    ///
    /// `source` is the door this reload came through. Prepare needs it because
    /// it stamps the `applied` body commit will publish, and `trigger` is one of
    /// that body's fields: a body measured under one trigger and published under
    /// another is not the body that was measured.
    pub fn prepare(&self, source: TriggerSource, dynamic: &DynamicSnapshot) -> Prepared {
        // 0. The mounts document, re-read and re-verified before anything asks
        //    what is installed. It is what says which trees this host may read
        //    at all, so the deployment document's imports, the cross-root scans
        //    and every package resolution below are all statements about the
        //    roots it derives *now* — a bundle installed since boot is visible
        //    the moment its mount line is there, and a mount declared but not
        //    installed is a refusal rather than a host serving half a document.
        // Where to re-read from is the loaded mounts' own answer, so the file a
        // reload reads cannot drift from the one the running mounts came from.
        let mounts = match try_load_mounts(self.baseline.mounts.path.as_deref()) {
            Ok(mounts) => mounts,
            Err(report) => return refused(None, vec![report]),
        };

        // 1. The document, compiled and lowered exactly as boot would, against
        //    the module roots the mounts just derived.
        let candidate = match check_config(&self.inputs(&mounts.roots)) {
            Ok(document) => document,
            Err(report) => return refused(None, vec![report]),
        };
        let sha = candidate.document_sha256.clone();

        // 2. Level 1: everything a reload cannot converge must be equal, and
        //    for each agent that survives it, which of its convergible field
        //    classes moved.
        let level_one =
            non_convergible_differences(&self.baseline.document.config, &candidate.config);
        if !level_one.refusals.is_empty() {
            return refused(Some(sha), level_one.refusals);
        }
        let app_diffs = level_one.app_diffs;

        // 2w. The webhook document half, on the candidate: slug and mount
        //     uniqueness, ownership, scheme shape, replay configuration, and
        //     the per-agent subscription stamps every later step reads. No
        //     secret is touched here — that is step 4w, and this half is the
        //     one `brenn config-check` runs offline.
        let (webhook_identities, webhook_subscriptions) =
            match catch_quietly(AssertUnwindSafe(|| {
                brenn_lib::webhook::config::resolve_webhook_identities(
                    &candidate.config.webhook_endpoints,
                    &candidate.config.apps,
                    &candidate.config.wasm_consumers,
                    &candidate.config.wasm,
                    &candidate.config.messaging,
                )
            })) {
                Ok(resolved) => resolved,
                Err(payload) => return refused(Some(sha), vec![app_resolver_refusal(payload)]),
            };
        let replay_store_paths =
            brenn_lib::webhook::config::webhook_store_paths(webhook_identities.values());

        //     The `[[mqtt_client]]` document half, on the candidate for the
        //     same reason: the clients are what an agent's MQTT authority, the
        //     ingress channels and a dynamic row's injection urgency are all
        //     resolved against, and the block converges, so every one of those
        //     reads the candidate's answer rather than the booted one. The
        //     credentials are step 4w's.
        let client_identities = match catch_quietly(AssertUnwindSafe(|| {
            brenn_lib::mqtt::config::resolve_client_identities(&candidate.config.mqtt_clients)
        })) {
            Ok(identities) => identities,
            Err(payload) => return refused(Some(sha), vec![app_resolver_refusal(payload)]),
        };

        // 2a. The candidate's agent map — must be the candidate's, not the
        //     booted one, because the plan derives static subscriptions from it
        //     and the gates read authority per call through the swapped table.
        let candidate_apps = match self.resolve_candidate_apps(
            &candidate.config,
            &client_identities,
            &webhook_subscriptions,
        ) {
            Ok(apps) => apps,
            Err(refusals) => return refused(Some(sha), refusals),
        };

        // 3. The cross-root scans boot runs before anything else, re-run because
        //    a bundle installed since boot may have landed a name brenn's own
        //    roots already hold. Before any package is resolved: a root set that
        //    is not a set of distinct releases resolves a name ambiguously, and
        //    the record read out of the wrong root is what the delta would then
        //    compare.
        if let Err(refusals) = self.check_roots(&mounts.roots) {
            return refused(Some(sha), refusals);
        }

        // 4. The candidate's plan, and level 2 over it.
        let plan = match self.plan_of(
            &candidate,
            &candidate_apps,
            &client_identities,
            &replay_store_paths,
        ) {
            Ok(plan) => plan,
            Err(refusals) => return refused(Some(sha), refusals),
        };
        // 4a. Rule 7, before the surface trees: the two plans must derive the
        //     same system participants. Everything but the surface-description
        //     pair comes off a block that level 1 froze, so a difference is a
        //     derivation reading something the comparison does not.
        let participants = system_participant_refusals(
            &self.baseline.system_participants,
            &plan.system_participants,
        );
        if !participants.is_empty() {
            return refused(Some(sha), participants);
        }
        // 4b. Surface assets, over the *whole* candidate surface list: an
        //     unchanged surface loses its kind when the mount offering it
        //     goes away, so every surface the candidate would run is
        //     re-validated, not only the ones that moved.
        let candidate_surface_roots = match self.scan_surface_roots(&mounts.roots, &plan.surfaces) {
            Ok(scanned) => scanned,
            Err(refusals) => return refused(Some(sha), refusals),
        };
        // 4c. A kind whose installed bytes moved is convergible — the surfaces
        //     instantiating it are promoted to `changed` by the delta's kind
        //     closure and their pages reload onto the new tree. The kernel is
        //     not: every page loads it, nothing republishes a page manifest for
        //     a surface that did not otherwise move, and a kernel from another
        //     tree than the running binary's release is a restart.
        let serving_surface_roots = self.env.surface_roots();
        // A kind this scan withholds is alerted where the scan is adopted
        // (`refresh_surface_roots`), not here: everything below can still
        // refuse the reload, and a refused reload leaves the serving roots —
        // and therefore what is withheld — exactly as they were.
        let kind_differences = serving_surface_roots.kind_differences(&candidate_surface_roots);
        if let Err(refusals) =
            surface_kernel_refusal(&serving_surface_roots, &candidate_surface_roots)
        {
            return refused(Some(sha), refusals);
        }
        let candidate_records = match self.records_of(&plan.wasm_consumers, &mounts.roots) {
            Ok(records) => records,
            Err(refusals) => return refused(Some(sha), refusals),
        };
        // 4w. The webhook environment half: every declared endpoint's signing
        //     secrets, read off the host on every reload because a fresh boot
        //     reads them. A missing or unreadable file refuses the whole
        //     reload — a fresh boot could not have produced that state either.
        let candidate_endpoints = match catch_quietly(AssertUnwindSafe(|| {
            brenn_lib::webhook::config::resolve_webhook_endpoints(&webhook_identities)
        })) {
            Ok(endpoints) => endpoints,
            Err(payload) => return refused(Some(sha), vec![environment_refusal(payload)]),
        };
        //     The same step re-verifies every replay-protected endpoint's
        //     package against the candidate roots: a bundle can ship new bytes
        //     under a package name the document never mentions moving, and a
        //     fresh boot would compile those bytes.
        let releases = match catch_quietly(AssertUnwindSafe(|| {
            replay_releases(&candidate_endpoints, &mounts.roots)
        })) {
            Ok(releases) => releases,
            Err(payload) => return refused(Some(sha), vec![environment_refusal(payload)]),
        };
        let webhook = webhook_delta(&self.env.webhook.baseline(), &candidate_endpoints, releases);

        // 4m. The `[[mqtt_client]]` environment half, beside the webhook one
        //     and for the same reason: `password_file` and `ca_file` are read
        //     off the host on every reload, so a rotated credential under an
        //     unmoved document is a change and an unreadable one refuses the
        //     whole reload. It runs against step 2w's identity map rather than
        //     re-resolving the blocks, so the document grammar is asserted
        //     once per reload and every panic here is a host fact.
        let candidate_clients = match catch_quietly(AssertUnwindSafe(|| {
            brenn_lib::mqtt::config::resolve_client_secrets(
                &client_identities,
                &candidate.config.mqtt_clients,
            )
        })) {
            Ok(clients) => clients,
            Err(payload) => return refused(Some(sha), vec![environment_refusal(payload)]),
        };
        let mqtt_clients =
            mqtt_clients_delta(&self.env.mqtt_service.baseline(), &candidate_clients);

        let baseline_records = self.baseline_records();
        let baseline_apps = self.env.apps.load();
        let baseline_facts = PlanFacts {
            directory: &self.baseline.directory,
            apps: &baseline_apps,
            consumers: &self.baseline.consumers,
            records: &baseline_records,
            mqtt_ingress: &self.baseline.mqtt_ingress,
            surfaces: &self.baseline.surfaces,
        };
        let candidate_facts = PlanFacts {
            directory: &plan.directory,
            apps: &candidate_apps,
            consumers: &plan.wasm_consumers,
            records: &candidate_records,
            mqtt_ingress: &plan.mqtt_ingress_channels,
            surfaces: &plan.surfaces,
        };
        let mut delta = plan_delta(
            &baseline_facts,
            &candidate_facts,
            kind_differences.into_keys().collect(),
            &AgentInputs {
                app_diffs: &app_diffs,
                tool_registry: &self.env.tool_registry,
            },
            &LiveFacts {
                directory: self.env.messenger.directory(),
                dynamic,
                mqtt_clients: &client_identities,
                clients_stopping: &mqtt_clients.removed,
            },
        );
        delta.webhook = webhook;
        delta.mqtt_clients = mqtt_clients;
        let refusals = convergibility_refusals(
            &baseline_facts,
            &candidate_facts,
            &delta,
            self.env.messenger.directory(),
        );
        if !refusals.is_empty() {
            return refused(Some(sha), refusals);
        }

        // 5. Nothing moved: the file bytes did, and the projection did not.
        //    Taken before the loads, which have nothing to load.
        if delta.is_empty() {
            return Prepared::Unchanged(Box::new(Projection {
                document: candidate,
                mounts,
                plan,
                records: candidate_records,
                surface_roots: candidate_surface_roots,
                kinds_changed: delta.kinds_changed,
            }));
        }

        // 6. The outcome this reload would publish, measured before the
        //    cranelift compiles below would make the measurement pointless. An
        //    `applied` body carries the whole delta — every moved address and
        //    every consumer slug — so a large enough edit pushes it past
        //    `[messaging] max_body_bytes` with no bug anywhere, and the
        //    design's answer for a change that cannot be applied live is a
        //    refusal in this phase rather than a panic after the walk.
        let applied = self.applied_status(source, &sha, &delta, &mounts);
        // Through the publisher's own gate, so prepare's verdict and the
        // publish-side verdict cannot drift apart at the boundary.
        if let Err(BodySizeExceeded { len, max }) =
            check_body_size(&applied.body(), self.env.messenger.max_body_bytes())
        {
            return refused(
                Some(sha),
                vec![format!(
                    "the `applied` outcome this reload would publish is {len} bytes but \
                     [messaging] max_body_bytes is {max}; raise max_body_bytes above \
                     {len} (a restart — [messaging] is not convergible) or make this change \
                     in smaller steps"
                )],
            );
        }

        // 7. Every consumer the delta brings into service, loaded and
        //    instantiated, so commit has no artifact left to be refused by.
        let arriving: Vec<&ResolvedWasmConsumer> = plan
            .wasm_consumers
            .iter()
            .filter(|consumer| {
                delta.consumers_added.contains(&consumer.slug)
                    || delta.consumers_changed.contains(&consumer.slug)
            })
            .collect();
        let loaded = match self.load_arriving(&arriving, &candidate_records, &mounts.roots) {
            Ok(loaded) => loaded,
            Err(refusals) => return refused(Some(sha), refusals),
        };

        // 7w. Every arriving endpoint's replay component, verified and
        //     compiled but *not* opened: the store file is still held by the
        //     entry this reload replaces, and commit is where it is handed
        //     over. An endpoint whose replay configuration did not move keeps
        //     the guard it is being served through, component and lock and all.
        let webhook = match self.webhook_arrivals(&delta, &mounts.roots) {
            Ok(arrivals) => arrivals,
            Err(refusals) => return refused(Some(sha), refusals),
        };

        // 8. What the reload republishes about surfaces, and the runtimes it
        //    installs. Both are built here for the reason step 7 is: a
        //    malformed sidecar or an oversize body is a refusal that leaves the
        //    process untouched, and commit's surface steps then have nothing
        //    left that can decline.
        let surface_docs =
            match self.surface_docs(&candidate.config, &plan, &delta, &candidate_surface_roots) {
                Ok(docs) => docs,
                Err(refusals) => return refused(Some(sha), refusals),
            };
        let surface_runtimes = self.arriving_surface_runtimes(&candidate.config, &delta);

        // 9. The last thing prepare does, because it is the only thing it
        //    writes. Each changed agent whose tool list moved gets the
        //    candidate's rendering staged beside the file its running
        //    `noop_mcp.py` read; commit renames. A write failure is an
        //    environment refusal and takes every staged file with it.
        if let Err(refusals) = self.stage_virtual_tools(&delta, &candidate_apps) {
            return refused(Some(sha), refusals);
        }

        Prepared::Ready(Box::new(ReadyReload {
            document: candidate,
            mounts,
            plan,
            delta,
            loaded,
            records: candidate_records,
            surface_roots: candidate_surface_roots,
            surface_runtimes,
            surface_docs,
            webhook,
            applied,
        }))
    }

    /// Write each changed agent's candidate virtual-tools rendering beside the
    /// file its sessions were spawned against.
    ///
    /// Staged rather than written in place because a reload that refuses after
    /// this point must leave the running process reading exactly what it was
    /// reading; commit's rename is what makes the new list the live one, and it
    /// happens in the same step as the map swap.
    fn stage_virtual_tools(
        &self,
        delta: &PlanDelta,
        apps: &IndexMap<String, AppConfig>,
    ) -> Result<(), Vec<String>> {
        let mut written = Vec::new();
        for change in &delta.agents_changed {
            if !change.virtual_tools_staged {
                continue;
            }
            let app = apps.get(&change.slug).unwrap_or_else(|| {
                panic!(
                    "reload prepare: agent {:?} is in the delta but not in the candidate map it \
                     was computed from — host bug",
                    change.slug,
                )
            });
            let path = super::agents::staged_virtual_tools_path(app);
            let rendered =
                brenn_server::active_bridge::render_virtual_tools(app, &self.env.tool_registry);
            if let Err(error) = std::fs::write(&path, rendered) {
                for path in &written {
                    let _: std::io::Result<()> = std::fs::remove_file(path);
                }
                return Err(vec![format!(
                    "agent {:?}: writing the new virtual tools list to {} failed: {error}",
                    change.slug,
                    path.display(),
                )]);
            }
            written.push(path);
        }
        Ok(())
    }

    /// Remove every staged tool list, for a reload that refused after prepare
    /// staged them.
    fn discard_staged_virtual_tools(&self, delta: &PlanDelta, plan: &MessagingPlan) {
        let Some(apps) = plan.planned_apps() else {
            return;
        };
        for change in &delta.agents_changed {
            if !change.virtual_tools_staged {
                continue;
            }
            if let Some(app) = apps.get(&change.slug) {
                let _: std::io::Result<()> =
                    std::fs::remove_file(super::agents::staged_virtual_tools_path(app));
            }
        }
    }

    /// The `applied` outcome a ready reload will publish, exactly as commit
    /// would publish it.
    ///
    /// `running_document_sha256` is the *candidate's* hash, not the baseline's:
    /// commit moves the baseline to the candidate before it publishes, so this
    /// is what that field reads there — and a body stamped with the baseline it
    /// is about to leave is the false retained status the check below exists to
    /// rule out.
    fn applied_status(
        &self,
        source: TriggerSource,
        document_sha256: &str,
        delta: &PlanDelta,
        mounts: &LoadedMounts,
    ) -> ReloadStatus {
        ReloadStatus {
            v: STATUS_VERSION,
            outcome: Outcome::Applied,
            trigger: source.into(),
            generation: self.generation + 1,
            at: now(),
            document_sha256: Some(document_sha256.to_string()),
            root: self.env.root.clone(),
            running_document_sha256: document_sha256.to_string(),
            delta: StatusDelta::from(delta),
            mounts: StatusMount::of(&mounts.config),
            refusals: Vec::new(),
        }
    }

    /// What a candidate compiles under: the deployment document, read against
    /// the roots this reload's mounts derive — the module roots its packaged
    /// imports resolve through, and the config-carrying mounts whose trees are
    /// part of the document.
    fn inputs(&self, roots: &Roots) -> DocumentInputs {
        deployment_inputs(&self.env.config_path, roots)
    }

    /// The candidate's agent map, resolved as boot resolves it.
    ///
    /// Both of the resolver's other inputs are the *candidate's*, resolved by
    /// step 2w: an agent's MQTT authority is validated against the declared
    /// clients and its webhook subscriptions are stamped off the declared
    /// endpoints, and both blocks converge, so the booted answer would be the
    /// wrong one to gate the new document with. A panic out of the resolver is
    /// classified as a refusal.
    fn resolve_candidate_apps(
        &self,
        candidate: &brenn_lib::config::BrennConfig,
        clients: &IndexMap<String, MqttClientIdentity>,
        webhook_subscriptions: &std::collections::BTreeMap<
            String,
            Vec<brenn_lib::webhook::config::ResolvedWebhookSubscription>,
        >,
    ) -> Result<Arc<IndexMap<String, AppConfig>>, Vec<String>> {
        let registry = &self.env.integration_registry;
        let runtime_dir = self.env.runtime_dir.as_deref();
        let resolved = catch_quietly(AssertUnwindSafe(|| {
            let apps = brenn_lib::config::resolve_apps(
                candidate,
                registry,
                runtime_dir,
                clients,
                webhook_subscriptions,
            );
            self.env.tool_registry.validate_config(&apps);
            apps
        }))
        .map_err(|payload| vec![app_resolver_refusal(payload)])?;
        Ok(Arc::new(resolved))
    }

    /// Lower a candidate document with the candidate's agents, the candidate's
    /// clients, the candidate's replay store paths and the booted plan inputs
    /// level 1 froze.
    fn plan_of(
        &self,
        candidate: &LoadedDocument,
        apps: &Arc<IndexMap<String, AppConfig>>,
        clients: &IndexMap<String, MqttClientIdentity>,
        replay_store_paths: &[PathBuf],
    ) -> Result<MessagingPlan, Vec<String>> {
        let planned = catch_quietly(AssertUnwindSafe(|| {
            plan_messaging(&PlanInputs {
                config: &candidate.config,
                apps: Some(apps),
                mqtt_clients: clients,
                tool_registry: Some(&self.env.tool_registry),
                replay_store_paths,
            })
        }))
        .map_err(|payload| vec![planner_refusal(payload)])?;
        // A running process has messaging — the reload facility itself is a
        // pair of declared channels — so a candidate that configures none is a
        // document for some other process, not a convergence.
        planned.ok_or_else(|| {
            vec![format!(
                "the candidate document configures no messaging at all: {}",
                super::NEEDS_RESTART
            )]
        })
    }

    /// What each candidate consumer's package binds to *now*, read off the
    /// roots without loading anything.
    ///
    /// This is what makes a bundle upgrade under an unmoved document visible: a
    /// new artifact under the same package is a different record, and level 2
    /// reads that as a changed consumer.
    fn records_of(
        &self,
        consumers: &[ResolvedWasmConsumer],
        roots: &Roots,
    ) -> Result<HashMap<String, Verified>, Vec<String>> {
        if consumers.is_empty() {
            return Ok(HashMap::new());
        }
        // Asked once for the whole walk: the root list is this reload's mounts'
        // answer. What is under those roots can move while prepare runs — a
        // bundle install is a symlink swap over them — which is why the records
        // this reads are the ones handed to the load rather than read again.
        let roots = match catch_quietly(AssertUnwindSafe(|| {
            brenn_lib::wasm_package::require_components_root(
                &roots.components_roots,
                "the candidate document's components",
            )
        })) {
            Ok(roots) => roots,
            Err(payload) => return Err(vec![environment_refusal(payload)]),
        };
        let mut records = HashMap::new();
        let mut refusals = Vec::new();
        for consumer in consumers {
            match catch_quietly(AssertUnwindSafe(|| {
                brenn_lib::wasm_package::verify_consumer(
                    roots,
                    &consumer.package,
                    &consumer.slug,
                    &consumer.spec_sha256,
                )
            })) {
                Ok(verified) => {
                    records.insert(consumer.slug.clone(), verified);
                }
                Err(payload) => refusals.push(environment_refusal(payload)),
            }
        }
        if refusals.is_empty() {
            Ok(records)
        } else {
            Err(refusals)
        }
    }

    /// What every running consumer was loaded from, which is the other side of
    /// the record comparison.
    fn baseline_records(&self) -> HashMap<String, Verified> {
        self.registry
            .iter()
            .map(|(slug, running)| (slug.clone(), running.verified.clone()))
            .collect()
    }

    /// Load every consumer the delta brings into service, against the records
    /// step 4 already read.
    ///
    /// The record is handed over rather than read again: the delta was computed
    /// on it, so loading a second reading of the same package would let the
    /// status body name a change that is not the one that got instantiated.
    fn load_arriving(
        &self,
        arriving: &[&ResolvedWasmConsumer],
        records: &HashMap<String, Verified>,
        roots: &Roots,
    ) -> Result<Vec<(String, LoadedConsumer)>, Vec<String>> {
        let ctx = ConsumerLoadContext {
            components_roots: &roots.components_roots,
            alert_dispatcher: &self.env.alert_dispatcher,
            mqtt_service: self.env.mqtt_service.clone(),
            tool_registry: &self.env.tool_registry,
            max_payload_bytes: self.env.max_payload_bytes,
        };
        let mut loaded = Vec::new();
        let mut refusals = Vec::new();
        for consumer in arriving {
            let record = records.get(&consumer.slug).cloned();
            match catch_quietly(AssertUnwindSafe(|| load_consumer(&ctx, consumer, record))) {
                Ok(one) => loaded.push((consumer.slug.clone(), one)),
                Err(payload) => refusals.push(environment_refusal(payload)),
            }
        }
        if refusals.is_empty() {
            Ok(loaded)
        } else {
            Err(refusals)
        }
    }

    /// What commit's webhook steps need: the runtimes to install, the stores to
    /// open, and the holders to drop first.
    ///
    /// A replay-protected endpoint that is arriving fresh, or whose replay
    /// configuration moved, gets its component verified and compiled here — the
    /// fallible, root-dependent half — and a guard whose store file is not open
    /// yet, because the entry this reload replaces is still holding it. A
    /// changed endpoint whose replay configuration is *equal* carries the
    /// running guard forward — same slot, same component, same lock — so an
    /// in-flight request checks against the component it always did.
    fn webhook_arrivals(
        &self,
        delta: &PlanDelta,
        roots: &Roots,
    ) -> Result<WebhookArrivals, Vec<String>> {
        let mut arrivals = WebhookArrivals {
            runtimes: Vec::new(),
            opening: Vec::new(),
            retiring: Vec::new(),
        };
        let mut refusals = Vec::new();
        for entry in &delta.webhook.removed {
            arrivals.retiring.extend(entry.replay.clone());
        }
        let arriving = delta
            .webhook
            .added
            .iter()
            .map(|endpoint| (None, endpoint))
            .chain(
                delta
                    .webhook
                    .changed
                    .iter()
                    .map(|change| (Some(&change.old), &change.new)),
            );
        for (old, endpoint) in arriving {
            // A running guard is carried forward only when the endpoint's
            // replay configuration *and* the bytes behind its package are the
            // ones it was compiled from: a bumped package is a different
            // component over the same store, which is a fresh guard and a
            // handover.
            let carried = old.and_then(|old| {
                let same_config = old.endpoint.replay_protection == endpoint.replay_protection;
                let same_release = old.replay.as_ref().is_none_or(|guard| {
                    delta
                        .webhook
                        .releases
                        .get(endpoint.slug.as_str())
                        .is_some_and(|release| guard.verified.same_release(release))
                });
                (same_config && same_release)
                    .then(|| old.replay.clone())
                    .flatten()
            });
            if carried.is_none()
                && let Some(old) = old
            {
                arrivals.retiring.extend(old.replay.clone());
            }
            let replay = match (carried, endpoint.replay_protection.as_ref()) {
                (Some(guard), _) => Some(guard),
                (None, None) => None,
                (None, Some(rp)) => {
                    match catch_quietly(AssertUnwindSafe(|| {
                        crate::load_verified_replay(
                            &endpoint.slug,
                            &roots.components_roots,
                            &rp.component,
                            &rp.store_path,
                            rp.max_page_count,
                            rp.config.clone(),
                        )
                    })) {
                        Ok((component, verified)) => {
                            let guard = brenn_webhook::ReplayGuard::new(
                                rp.store_path.clone(),
                                verified,
                                Arc::new(component),
                            );
                            arrivals.opening.push(Arc::clone(&guard));
                            Some(guard)
                        }
                        Err(payload) => {
                            refusals.push(environment_refusal(payload));
                            continue;
                        }
                    }
                }
            };
            arrivals.runtimes.push(brenn_webhook::EndpointRuntime::new(
                Arc::clone(endpoint),
                replay,
            ));
        }
        if refusals.is_empty() {
            Ok(arrivals)
        } else {
            Err(refusals)
        }
    }

    /// The surface documents and registration swaps this reload owes, built
    /// against the candidate's own parameters.
    ///
    /// Under `catch_quietly` because the description builders read each kind's
    /// sidecar files off the mount that serves it: a `.schema.json` that is not
    /// JSON is a boot panic and must be a refusal here.
    fn surface_docs(
        &self,
        config: &brenn_lib::config::BrennConfig,
        plan: &MessagingPlan,
        delta: &PlanDelta,
        roots: &brenn_surface_server::SurfaceRoots,
    ) -> Result<SurfaceDocs, Vec<String>> {
        if delta.surfaces.is_empty() && delta.kinds_changed.is_empty() {
            return Ok(SurfaceDocs::default());
        }
        let inputs = SurfaceDocInputs {
            surfaces: &plan.surfaces,
            roots,
            delta: &delta.surfaces,
            kinds_changed: &delta.kinds_changed,
            baseline_participants: &self.baseline.system_participants,
            candidate_participants: &plan.system_participants,
            candidate_registrations: &plan.registrations,
        };
        let params = SurfaceDocParams {
            prefix: &config.surface_description.prefix,
            build_id: self.env.build_id,
            status_interval_secs: config.surface_description.status_interval_secs,
            error_report: config
                .observability
                .surface_error_channel
                .as_deref()
                .map(|address| (address, config.observability.surface_error_publish_floor)),
            max_body_bytes: config.messaging.max_body_bytes,
        };
        match catch_quietly(AssertUnwindSafe(|| build_surface_docs(&inputs, &params))) {
            Ok(result) => result,
            Err(payload) => Err(vec![environment_refusal(payload)]),
        }
    }

    /// One runtime per arriving surface, built exactly as boot builds them.
    ///
    /// Not fallible: everything a runtime is built from is resolved config the
    /// plan already produced, and the asset scan above has already refused a
    /// surface whose kind no declared mount offers.
    fn arriving_surface_runtimes(
        &self,
        config: &brenn_lib::config::BrennConfig,
        delta: &PlanDelta,
    ) -> HashMap<String, Arc<brenn_surface_server::SurfaceRuntime>> {
        let surfaces: Vec<ResolvedSurface> = arriving(&delta.surfaces)
            .into_iter()
            .map(|(surface, _)| surface.clone())
            .collect();
        if surfaces.is_empty() {
            return HashMap::new();
        }
        brenn_surface_server::build_surface_runtimes(
            surfaces,
            Some(self.env.messenger.clone()),
            config.messaging.max_body_bytes,
            config.observability.surface_error_channel.clone(),
            brenn_surface_server::SurfaceDescriptionParams {
                prefix: config.surface_description.prefix.clone(),
            },
        )
    }

    /// Boot's surface-asset validation, re-run over this reload's roots and the
    /// whole candidate surface list. Over the whole list because a surface
    /// nobody edited loses its kind the moment the mount offering it stops
    /// being declared.
    fn scan_surface_roots(
        &self,
        roots: &Roots,
        surfaces: &[ResolvedSurface],
    ) -> Result<brenn_surface_server::SurfaceRoots, Vec<String>> {
        catch_quietly(AssertUnwindSafe(|| {
            brenn_surface_server::validate_surface_assets_in(
                brenn_surface_server::AssetContext::RELOAD,
                &roots.surface_roots,
                surfaces,
            )
        }))
        .map_err(|payload| vec![environment_refusal(payload)])
    }
}

/// The surface kernel the declared mounts offer, held against the one this
/// process is serving.
///
/// Kinds converge; the kernel does not. Every surface page loads it, and a
/// reload only reloads the pages of surfaces that moved — so a kernel swapped
/// under an untouched surface would leave that page running bytes from a tree
/// no longer installed, with nothing to tell it. It moves with the release that
/// builds it, which is a restart anyway.
///
/// Bytes as well as path, because the claim above is about bytes: the kernel
/// carries no manifest, so an in-place rewrite of the pair under an unmoved
/// root is exactly the mixed state this refusal exists to prevent and is the
/// one shape a path comparison cannot see.
fn surface_kernel_refusal(
    serving: &brenn_surface_server::SurfaceRoots,
    scanned: &brenn_surface_server::SurfaceRoots,
) -> Result<(), Vec<String>> {
    match (&scanned.kernel, &serving.kernel) {
        (scanned, serving) if scanned == serving => Ok(()),
        (Some(scanned), Some(serving)) if scanned.root == serving.root => Err(vec![format!(
            "the surface kernel under {} has been rewritten in place since this process started \
             serving it: {}",
            serving.root.display(),
            super::NEEDS_RESTART,
        )]),
        _ => Err(vec![format!(
            "the surface kernel root the declared mounts offer is not the one this process is \
             serving: {}",
            super::NEEDS_RESTART,
        )]),
    }
}

impl ReloadDriver {
    /// Boot's cross-root preconditions, asked again over this reload's roots.
    fn check_roots(&self, roots: &Roots) -> Result<(), Vec<String>> {
        catch_quietly(AssertUnwindSafe(|| {
            brenn_lib::wasm_package::assert_components_roots(&roots.components_roots);
            brenn_lib::wasm_package::assert_disjoint_components_roots(&roots.components_roots);
        }))
        .map_err(|payload| vec![environment_refusal(payload)])
    }

    /// The dynamic subscriptions this process holds right now: every durable
    /// row in the table and every non-durable registration the messenger keeps.
    ///
    /// Read once per reload, before prepare, and carried through to commit. The
    /// set is the one input to a reload that a live session can move while
    /// prepare runs — a `MessageSubscribe` mints a row at any moment, on the
    /// old document's authority — so commit compares what it reads against
    /// this and declines a reload whose subject moved underneath it.
    pub(crate) async fn dynamic_snapshot(&self) -> DynamicSnapshot {
        let rows = {
            let conn = self.env.messenger.db().lock().await;
            brenn_messaging_store::db::load_dynamic_subscriptions(&conn)
        };
        DynamicSnapshot {
            rows,
            nondurable: self.env.messenger.nondurable_dynamic_subs(),
        }
    }

    /// Run prepare and report every outcome that is settled without touching
    /// the running system.
    ///
    /// Returns the reload the caller has to apply, or `None` when there was
    /// nothing to apply — a refusal or an unchanged document, both of which are
    /// fully handled here: the status is published, the baseline is moved where
    /// it should be, and the operator is alerted if they need to be.
    ///
    /// # Panics
    ///
    /// Requires the multi-threaded runtime: prepare runs under
    /// `tokio::task::block_in_place`, which panics on a current-thread runtime.
    /// A test calling this needs `#[tokio::test(flavor = "multi_thread")]`.
    pub async fn prepare_and_report(&mut self, source: TriggerSource) -> Option<Box<ReadyReload>> {
        // Prepare is synchronous and not cheap: it hashes every arriving
        // artifact and cranelift-compiles every arriving component. Run plainly
        // on the worker that awaited this call it would stall every other task
        // sharing that worker for the whole of a component compile, so it runs
        // under `block_in_place`, which turns this worker into a blocking
        // thread and relocates the tasks it was sharing rather than stalling
        // them. That needs the multi-threaded runtime. The driver still decides
        // one reload at a time — this is where the reload waits, not a second
        // one starting.
        // The one input prepare cannot read for itself: the durable dynamic
        // rows are behind the database's async mutex, and prepare is
        // synchronous. Read here, classified in there, and re-read at commit,
        // which is where a set that moved in between is caught.
        let dynamic = self.dynamic_snapshot().await;
        let prepared = tokio::task::block_in_place(|| self.prepare(source, &dynamic));
        match prepared {
            Prepared::Refused {
                document_sha256,
                refusals,
            } => {
                self.report_refusal(source, document_sha256, refusals).await;
                None
            }
            Prepared::Unchanged(projection) => {
                let Projection {
                    document,
                    mounts,
                    plan,
                    records,
                    surface_roots,
                    kinds_changed,
                } = *projection;
                super::commit::refresh_records(&mut self.registry, &records);
                super::commit::refresh_surface_roots(&self.env, surface_roots);
                // The running state already *is* this document's projection, so
                // adopting it is an identity update and nothing else. Without
                // it the retained status would keep naming a document nobody
                // has on disk.
                let sha = document.document_sha256.clone();
                self.baseline = Baseline::of(document, mounts, &plan);
                info!(
                    trigger = ?source,
                    document_sha256 = %sha,
                    "reload: the document on disk projects to the running state"
                );
                self.publish(
                    Outcome::Unchanged,
                    source,
                    Some(sha),
                    StatusDelta {
                        kinds_changed: kinds_changed.into_iter().collect(),
                        ..StatusDelta::default()
                    },
                    Vec::new(),
                )
                .await;
                None
            }
            Prepared::Ready(ready) => Some(ready),
        }
    }

    /// Report a refusal: the journal line, the operator's phone, and the
    /// retained outcome. Nothing was touched, whichever phase declined.
    async fn report_refusal(
        &mut self,
        source: TriggerSource,
        document_sha256: Option<String>,
        refusals: Vec<String>,
    ) {
        warn!(
            trigger = ?source,
            refusals = refusals.len(),
            reason = %refusals.join("; "),
            "reload refused; running state untouched"
        );
        // The operator's phone is where a refusal has to land: the principal
        // that asked for this reload — an assistant writing an automation —
        // cannot itself decide that a restart is due. The body is cut to fit
        // what a phone backend accepts; the whole of it is in the line above.
        self.env.alert_dispatcher.alert(
            AlertSeverity::Warning,
            "Config reload refused".to_string(),
            refusal_alert_body(&refusals),
        );
        self.publish(
            Outcome::Refused,
            source,
            document_sha256,
            StatusDelta::default(),
            refusals,
        )
        .await;
    }

    /// One reload, end to end: decide, and apply what may be applied.
    ///
    /// This is what a door calls. It returns once the process is either
    /// converged to the document on disk or reported as unable to be, so a
    /// caller that serializes its calls has serialized its reloads.
    pub async fn reload(&mut self, source: TriggerSource) {
        let Some(ready) = self.prepare_and_report(source).await else {
            return;
        };
        self.commit(source, *ready).await;
    }

    /// Apply a prepared reload and report it.
    ///
    /// The walk declines only on its own pre-mutation check — a subscriber that
    /// arrived on a departing channel while prepare was compiling — which is a
    /// refusal like any other, since nothing has been touched when it is made.
    /// Past that nothing declines: prepare has already refused everything that
    /// could be refused, so a failure inside the walk is a host bug, panics, and
    /// takes the process with it — as does a failure in the publish below, for
    /// the same reason. The baseline moves to the candidate *before* the outcome
    /// is published, so the body's `running_document_sha256` names what the
    /// process is projecting as of that publish rather than what it was
    /// projecting a moment ago; the body itself was built and measured in
    /// prepare, so nothing about its size can be discovered here.
    async fn commit(&mut self, source: TriggerSource, ready: ReadyReload) {
        let ReadyReload {
            document,
            mounts,
            plan,
            delta,
            loaded,
            records,
            surface_roots,
            surface_runtimes,
            surface_docs,
            webhook,
            mut applied,
        } = ready;
        let sha = document.document_sha256.clone();
        // The walk is `async` throughout — it awaits a stopping consumer's last
        // drain step and the database — so unlike prepare it is not the
        // blocking pool's to run.
        let report = match super::commit::apply(
            &self.env,
            &mut self.registry,
            &plan,
            &delta,
            super::commit::CommitArtifacts {
                loaded,
                records: &records,
                surfaces: super::commit::SurfaceCommit {
                    roots: &surface_roots,
                    runtimes: &surface_runtimes,
                    docs: &surface_docs,
                    prefix: &document.config.surface_description.prefix,
                },
                webhook: &webhook,
            },
        )
        .await
        {
            Ok(report) => report,
            Err(refusals) => {
                // The staged tool lists go with the refusal: nothing was
                // touched, so nothing may be left beside a running agent's file
                // for a later commit to rename into place.
                self.discard_staged_virtual_tools(&delta, &plan);
                self.report_refusal(source, Some(sha.clone()), refusals)
                    .await;
                return;
            }
        };
        // The two delta fields prepare could not know: which filters the broker
        // took now, which it will take on the next connect, and which no
        // connect in this process will take. Prepare measured the body with
        // every moved filter listed in both, so replacing that with the ones
        // that actually deferred or failed only shrinks it.
        applied.delta.mqtt_deferred = report.mqtt.deferred;
        applied.delta.mqtt_failed = report.mqtt.failed;
        applied.delta.sessions_retired = report.sessions_retired;
        applied.delta.sessions_retire_pending = report.sessions_retire_pending;
        self.generation += 1;
        self.baseline = Baseline::of(document, mounts, &plan);
        // The delta on the line, not just in the retained body: an operator
        // reading the journal during an incident is exactly the reader who
        // cannot reach the bus to ask what moved. Read off the very struct that
        // is about to be published, so the two cannot say different things.
        info!(
            trigger = ?source,
            generation = applied.generation,
            document_sha256 = %sha,
            consumers_added = ?applied.delta.consumers_added,
            consumers_removed = ?applied.delta.consumers_removed,
            consumers_changed = ?applied.delta.consumers_changed,
            channels_added = ?applied.delta.channels_added,
            channels_removed = ?applied.delta.channels_removed,
            channels_changed = ?applied.delta.channels_changed,
            channels_described = ?applied.delta.channels_described,
            surfaces_added = ?applied.delta.surfaces_added,
            surfaces_removed = ?applied.delta.surfaces_removed,
            surfaces_changed = ?applied.delta.surfaces_changed,
            // Not on the line for the other mqtt lists, which the journal
            // already carries per filter: this is the one an operator reading
            // an applied reload has to act on.
            mqtt_failed = ?applied.delta.mqtt_failed,
            // An agent's authority widening is the most security-relevant thing
            // a reload can do, and the journal is where an operator who cannot
            // reach the bus reads what moved.
            agents_changed = ?applied.delta.agents_changed,
            subscriptions_added = ?applied.delta.subscriptions_added,
            subscriptions_removed = ?applied.delta.subscriptions_removed,
            sessions_retired = ?applied.delta.sessions_retired,
            sessions_retire_pending = ?applied.delta.sessions_retire_pending,
            "reload applied"
        );
        fit_session_lists(&mut applied, self.env.messenger.max_body_bytes());
        // The one field that is not prepare's: the outcome was reached now, not
        // when it was decided. Fixed width, so the size prepare measured stands.
        applied.at = now();
        publish_status(&self.env.messenger, &applied).await;
    }

    /// Publish one outcome under the facility's own identity.
    ///
    /// The `refused` and `unchanged` callers. `applied` is published from the
    /// [`ReloadStatus`] prepare built and measured, not from here.
    async fn publish(
        &self,
        outcome: Outcome,
        source: TriggerSource,
        document_sha256: Option<String>,
        delta: StatusDelta,
        refusals: Vec<String>,
    ) {
        publish_status(
            &self.env.messenger,
            &ReloadStatus {
                v: STATUS_VERSION,
                outcome,
                trigger: source.into(),
                generation: self.generation,
                at: now(),
                document_sha256,
                root: self.env.root.clone(),
                running_document_sha256: self.baseline.document.document_sha256.clone(),
                delta,
                // The baseline's, which for `unchanged` is the candidate the
                // caller has already adopted and for `refused` is what the
                // process is still reading — the mounts a refusal did not move.
                mounts: StatusMount::of(&self.baseline.mounts.config),
                refusals,
            },
        )
        .await;
    }
}

/// Keep the `applied` body publishable when commit's session lists push it past
/// the limit prepare measured.
///
/// Prepare measures the body with both session lists empty, and cannot do
/// otherwise: which live sessions were killable at the swap is a fact about the
/// process a moment later, and the bridge registry is behind an async lock a
/// synchronous prepare may not take. Unlike the two mqtt lists, which prepare
/// measures at their maximum and commit only shrinks, these two only grow — so
/// a host with many live sessions of a changed agent could measure as fitting
/// and then publish oversize, and an `applied` the publish gate rejects never
/// reaches the retained status channel, which reads to a bundle installer as a
/// failed reload over a successful one.
///
/// The names are the part that can be given up: they are replaced by their
/// counts, and cleared outright if even that does not fit, which restores the
/// body prepare proved publishable. The journal line above carries them in full
/// either way.
fn fit_session_lists(applied: &mut ReloadStatus, max: usize) {
    if check_body_size(&applied.body(), max).is_ok() {
        return;
    }
    let retired = applied.delta.sessions_retired.len();
    let pending = applied.delta.sessions_retire_pending.len();
    warn!(
        retired,
        pending,
        max,
        "the applied status body does not fit with the session lists; publishing their counts"
    );
    applied.delta.sessions_retired = vec![format!("{retired} sessions retired; names omitted")];
    applied.delta.sessions_retire_pending = vec![format!(
        "{pending} sessions retiring at turn end; names omitted"
    )];
    if check_body_size(&applied.body(), max).is_ok() {
        return;
    }
    applied.delta.sessions_retired.clear();
    applied.delta.sessions_retire_pending.clear();
}

fn refused(document_sha256: Option<String>, refusals: Vec<String>) -> Prepared {
    Prepared::Refused {
        document_sha256,
        refusals,
    }
}

/// Read a caught agent-resolver panic as a refusal, whatever it says.
///
/// Unlike the messaging planner, [`brenn_lib::config::resolve_apps`] is a pure
/// function of the document plus three filesystem stats and one mkdir: every
/// assert in it is a verdict on what the operator wrote or on the directory it
/// named. Most of them are spelled in boot's own words rather than with the
/// [`CONFIG_REFUSAL`](brenn_lib::panic_util::CONFIG_REFUSAL) prefix, because on
/// the boot path nothing classifies the text — so classifying by prefix here
/// would report a `working_dir` typo as a possible host defect and burn the one
/// log line that is supposed to mean "a resolver is broken". The message is
/// reported verbatim instead, which is what it already is: the refusal a fresh
/// boot of this document would have printed.
fn app_resolver_refusal(payload: Box<dyn std::any::Any + Send>) -> String {
    panic_message(&*payload).map_or_else(
        || {
            "the agent resolver panicked with a payload carrying no message, which is a host \
             defect rather than a verdict on the document"
                .to_string()
        },
        ToString::to_string,
    )
}

/// Read a caught planner panic as a refusal, whatever it says.
///
/// A payload the config-check classifier recognizes is the planner's verdict on
/// the document and is reported verbatim. One it does not recognize is either a
/// refusal spelled some way the classifier has not been told about or a genuine
/// defect in the resolvers; both are reported as refusals here, because prepare
/// has mutated nothing and the alternative is unwinding the driver of a process
/// that is otherwise healthy. The `warn!` is what a bug report is built from —
/// the backtrace is gone either way, since [`catch_quietly`] has already
/// returned by the time anything classifies.
fn planner_refusal(payload: Box<dyn std::any::Any + Send>) -> String {
    let Some(message) = panic_message(&*payload) else {
        return "the messaging planner panicked with a payload carrying no message, which is a \
                host defect rather than a verdict on the document"
            .to_string();
    };
    if crate::config_check::is_config_refusal(message) {
        return message.to_string();
    }
    warn!(
        panic = %message,
        "reload: the planner panicked with a message that is not a config refusal; reporting it \
         as one because prepare has changed nothing, but this may be a defect in the resolvers"
    );
    format!(
        "the messaging planner refused the document in words it may not have meant as a \
         verdict, so this may be a host defect: {message}"
    )
}

/// Read a caught environment panic as a refusal.
///
/// # Panics
///
/// On a payload carrying no text. Those come from `panic_any` with some other
/// type, which nothing on this path does, and a refusal nobody can read is not
/// a refusal.
fn environment_refusal(payload: Box<dyn std::any::Any + Send>) -> String {
    let Some(message) = panic_message(&*payload) else {
        panic!(
            "reload: loading a candidate's components panicked with a payload carrying no \
             message, so this is a host bug rather than a verdict on the document"
        );
    };
    message.to_string()
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    use brenn_lib::config::{BrennConfig, PACKAGED, PACKAGED_MODULE};
    use brenn_lib::messaging::SubscriberEntryKind;
    use brenn_messaging::config_reload::{RELOAD_ADDRESS, STATUS_ADDRESS};
    use brenn_messaging::query::MessageQuery;
    use brenn_obs::alerting::make_capturing_alerter_with_severity;
    use brenn_server::messaging_router::DeliveryBinding;
    use brenn_server::test_support::init_db_memory;
    use rusqlite::OptionalExtension;
    use tracing_test::traced_test;

    pub(crate) type Captured = Arc<std::sync::Mutex<Vec<(AlertSeverity, String, String)>>>;

    /// The app the tests read the retained outcome back through. Nothing
    /// publishes it: the facility's own identity does, and this one only holds
    /// the read gate open.
    pub(crate) const READER: &str = "some-reader";

    /// The build identifier every fixture process stamps into the description
    /// documents it publishes. One value for boot's publish and for the
    /// reload's, so a rebuilt document differs from the one it replaced only
    /// where the topology did — and the same one the server's own fixtures
    /// stamp, so the description documents the oracle compares verbatim and the
    /// handshake cases are tied at compile time rather than by coincidence.
    pub(crate) use brenn_server::test_support::TEST_BUILD_ID;

    /// The floor every fixture document stands on: the description index, the
    /// reload facility's declared pair — without which no outcome can be
    /// published at all — one work channel to move around, and one
    /// `ephemeral:` channel, so that the ring stores are a live part of every
    /// comparison rather than an empty list compared with an empty list.
    pub(crate) fn document(extra: &str) -> String {
        document_subscribing(extra, &[])
    }

    /// [`document`] with the reader agent subscribing to the channels
    /// `subscribes` names by their handles.
    ///
    /// Pull-only (`push_depth = 0`) on purpose: an `App` subscriber entry is
    /// what these cases want on the channel, and a push-enabled one would put
    /// the conversation-delivery path between the publish and the consumer this
    /// is about.
    pub(crate) fn document_subscribing(extra: &str, subscribes: &[&str]) -> String {
        document_subscribing_acl(extra, subscribes, &[])
    }

    /// [`document_subscribing`] with `extra_acl`'s clauses added to the
    /// agent's subscribe ACL — `exact work`, `topic_filter "mqtt:ha:x"` — for
    /// the cases about a channel the ACL covers with no `subscribe` statement
    /// on it, which is what a dynamic subscription needs.
    ///
    /// A parameter rather than a `.replace` on this function's own output: the
    /// anchor would be this fixture's formatting, and a reflow of the ACL line
    /// would silently produce documents without the clause the caller asked
    /// for.
    pub(crate) fn document_subscribing_acl(
        extra: &str,
        subscribes: &[&str],
        extra_acl: &[&str],
    ) -> String {
        let subscriptions: String = subscribes
            .iter()
            .map(|channel| {
                format!("    subscribe {channel} {{ push_depth = 0; retain_depth = 4; }}\n")
            })
            .collect();
        // An explicit `acl subscribe` is the whole of the plane's authority, so
        // a `subscribe` statement beside it derives nothing and has to be
        // covered by hand.
        let subscribe_acl: String = subscribes
            .iter()
            .map(|channel| format!(", exact {channel}"))
            .chain(extra_acl.iter().map(|clause| format!(", {clause}")))
            .collect();
        document_with_agent(
            extra,
            &format!(
                r#"
agent Reader() {{
    working_dir = ".";
    grants = [subscribe, publish];
    send_budget = 1000000;
    acl subscribe [exact reload_outcomes, prefix "brenn:surface.", prefix "ephemeral:surface."{subscribe_acl}];
    acl publish [exact reload_requests, exact work];
{subscriptions}}}

new some-reader: Reader();
"#
            ),
        )
    }

    /// [`document_subscribing`], but the agent is a singleton owned by one
    /// user and its subscriptions are push-enabled — the shape that holds a
    /// position under the agent's own conversation, which is what makes the
    /// attach half of the commit reachable.
    ///
    /// `owner` is the whole of `allowed_users`: a push-enabled subscription is
    /// only resolvable on a singleton agent with exactly one user, and the
    /// first entry is the conversation the bus path targets.
    pub(crate) fn document_push_subscribing(
        extra: &str,
        owner: &[&str],
        subscribes: &[&str],
    ) -> String {
        document_push_subscribing_acl(extra, owner, subscribes, &[], &[])
    }

    /// [`document_push_subscribing`] with two more knobs, for the same reason
    /// [`document_subscribing_acl`] has one: `mqtt_addresses` adds a
    /// push-enabled `subscribe` on each raw `mqtt:<client>:<topic>` address
    /// with the matching `topic_filter` clause, and `extra_acl` adds bare
    /// clauses to the subscribe ACL.
    ///
    /// An `mqtt:` subscription is spelled by address because the channel is
    /// system-synthesized and has no `[[channel]]` block to name. That is also
    /// why it states its own `wake_min`: there is no operator rung to inherit
    /// one from, and the family default would have the dispatcher wake a
    /// conversation this rig seats no bridge for.
    pub(crate) fn document_push_subscribing_acl(
        extra: &str,
        owner: &[&str],
        subscribes: &[&str],
        mqtt_addresses: &[&str],
        extra_acl: &[&str],
    ) -> String {
        let subscriptions: String = subscribes
            .iter()
            .map(|channel| {
                format!("    subscribe {channel} {{ push_depth = 1; retain_depth = 4; }}\n")
            })
            .chain(mqtt_addresses.iter().map(|address| {
                format!(
                    "    subscribe \"{address}\" {{ push_depth = 1; retain_depth = 4; \
                     wake_min = never; }}\n"
                )
            }))
            .collect();
        let subscribe_acl: String = subscribes
            .iter()
            .map(|channel| format!(", exact {channel}"))
            .chain(
                mqtt_addresses
                    .iter()
                    .map(|address| format!(", topic_filter \"{address}\"")),
            )
            .chain(extra_acl.iter().map(|clause| format!(", {clause}")))
            .collect();
        let users: String = owner
            .iter()
            .map(|user| format!("\"{user}\""))
            .collect::<Vec<_>>()
            .join(", ");
        document_with_agent(
            extra,
            &format!(
                r#"
agent Reader() {{
    working_dir = ".";
    singleton = true;
    // A singleton agent is required to state at least one compaction setting.
    compact_soft_pct = 70;
    allowed_users = [{users}];
    grants = [subscribe, publish];
    send_budget = 1000000;
    acl subscribe [exact reload_outcomes, prefix "brenn:surface.", prefix "ephemeral:surface."{subscribe_acl}];
    acl publish [exact reload_requests, exact work];
{subscriptions}}}

new some-reader: Reader();
"#
            ),
        )
    }

    /// [`document`] with no agent at all.
    ///
    /// Only for the case about the planner's "configures no messaging" arm: an
    /// agent is itself enough to configure messaging, so a candidate that
    /// reaches that arm has no agent, and level 1 refuses a candidate whose
    /// agent set moved.
    pub(crate) fn document_agentless(extra: &str) -> String {
        document_with_agent(extra, "")
    }

    /// The channel floor every fixture stands on, `agent` between it and
    /// `extra`.
    fn document_with_agent(extra: &str, agent: &str) -> String {
        format!(
            r#"
channel index at "brenn:surface.index" {{
    push_depth = 1;
    retain_depth = 1;
    standing_retain_depth = 1;
}}

channel reload_requests at "brenn:config.reload" {{
    push_depth = 1;
    retain_depth = 4;
    standing_retain_depth = 4;
}}

channel reload_outcomes at "brenn:config.status" {{
    push_depth = 1;
    retain_depth = 1;
    standing_retain_depth = 8;
}}

channel work at "brenn:work" {{
    push_depth = 4;
    retain_depth = 16;
    standing_retain_depth = 64;
    // The rung an agent's pull-only subscription inherits. A `subscribe` at
    // `push_depth = 0` may not state a `wake_min` of its own, and the default
    // rung would have the dispatcher eager-wake a conversation these fixtures
    // never seat a bridge for.
    wake_min = never;
    // Effectively unrated: the case that publishes continuously across a
    // reload asks what a publish meets while a subscriber is leaving, and a
    // throttle inside that window would answer a different question.
    send_rate = {{ burst = 1000000, refill_interval_secs = 1, refill = 1000000 }};
}}

channel scratch at "ephemeral:scratch" {{
    push_depth = 1;
    retain_depth = 4;
}}
{agent}
{extra}
"#
        )
    }

    /// The document tree on disk, rewritable under a running driver.
    pub(crate) struct Tree {
        dir: tempfile::TempDir,
    }

    impl Tree {
        /// An empty tree, for a case that has to build what its document names
        /// — a kind's asset tree, say — before it can write the document.
        pub(crate) fn new() -> Self {
            Self {
                dir: tempfile::tempdir().expect("a temporary directory"),
            }
        }

        pub(crate) fn holding(text: &str) -> Self {
            let tree = Self::new();
            tree.write(text);
            tree
        }

        /// Write the document, fencing any packaged half out into the module
        /// root the way a deployment's installed packages hold it — a top-level
        /// instance's class cannot be declared in the root document.
        pub(crate) fn write(&self, text: &str) {
            let modules = self.modules();
            if !modules.exists() {
                std::fs::create_dir(&modules).expect("a module root");
            }
            let module_file = modules.join(format!("{PACKAGED_MODULE}.brenn"));
            match brenn_lib::config::split_packaged(text) {
                Some((module, root)) => {
                    std::fs::write(&module_file, module).expect("the module is writable");
                    std::fs::write(self.root(), root).expect("the document is writable");
                }
                None => {
                    if module_file.exists() {
                        std::fs::remove_file(&module_file).expect("the module is removable");
                    }
                    std::fs::write(self.root(), text).expect("the document is writable");
                }
            }
        }

        pub(crate) fn root(&self) -> PathBuf {
            self.dir.path().join("main.brenn")
        }

        /// Write a secret file under this tree and hand back its path, so a
        /// document can name a `secret_file` a resolution will really read.
        /// Rewriting one with different bytes is a rotation.
        pub(crate) fn secret(&self, name: &str, bytes: &str) -> PathBuf {
            let path = self.secret_path(name);
            std::fs::create_dir_all(path.parent().expect("a secrets directory"))
                .expect("a secrets directory");
            std::fs::write(&path, bytes).expect("the secret file is writable");
            path
        }

        /// Where [`Tree::secret`] puts a named secret, written or not.
        pub(crate) fn secret_path(&self, name: &str) -> PathBuf {
            self.dir.path().join("secrets").join(name)
        }

        pub(crate) fn modules(&self) -> PathBuf {
            self.dir.path().join("modules")
        }

        /// The runtime directory the agent state dirs are minted under, in the
        /// role `XDG_RUNTIME_DIR` plays for a bare agent at boot. Under this
        /// tree so that dropping it takes the state dirs with it.
        pub(crate) fn runtime_dir(&self) -> PathBuf {
            let dir = self.dir.path().join("runtime");
            std::fs::create_dir_all(&dir).expect("a runtime directory");
            dir
        }

        pub(crate) fn inputs(&self) -> DocumentInputs {
            DocumentInputs::with_modules(self.root(), self.modules())
        }

        /// A mounts document over this tree, in the shape a host reads: one
        /// mount per tree the fixture offers, each a directory holding a
        /// symlink to the real tree and a `VERSION`, exactly as the dev-mounts
        /// generator builds them.
        ///
        /// The mount directories live in a tempdir of their own rather than
        /// under this one: a mount may not nest inside another, and the
        /// document's own tree is where the fixture's `modules/` is.
        pub(crate) fn mounts(&self, components_roots: &[PathBuf]) -> Mounts {
            Mounts::over(
                self.dir.path().join(".mounts"),
                &self.modules(),
                components_roots,
            )
        }

        pub(crate) fn load(&self) -> LoadedDocument {
            check_config(&self.inputs()).expect("the fixture document must load")
        }
    }

    /// A mounts document and the mount directories it declares.
    ///
    /// The directories live under the [`Tree`]'s own tempdir, so a case that
    /// holds its tree holds its mounts — including the cases that destructure
    /// [`Booted`] and drop everything they did not name.
    #[derive(Clone)]
    pub(crate) struct Mounts {
        dir: PathBuf,
    }

    impl Mounts {
        /// One mount offering `modules`, and one per components root.
        ///
        /// Split that way because a fixture's module root and its components
        /// roots are separate directories with no common parent, and a mount is
        /// one directory holding its trees — so each becomes a mount of its
        /// own, reaching the real tree through a symlink the way a dev mount
        /// does.
        fn over(dir: PathBuf, modules: &std::path::Path, components_roots: &[PathBuf]) -> Self {
            let mounts = Self { dir };
            std::fs::create_dir_all(&mounts.dir).expect("a mounts directory");
            mounts.declare("tree", &[("modules", modules)]);
            for (index, root) in components_roots.iter().enumerate() {
                mounts.declare(&format!("components-{index}"), &[("components", root)]);
            }
            mounts.write();
            mounts
        }

        /// Add one mount directory, with a symlink per tree it offers.
        fn declare(&self, name: &str, trees: &[(&str, &std::path::Path)]) {
            let path = self.dir.join(name);
            std::fs::create_dir_all(&path).expect("a mount directory");
            std::fs::write(path.join("VERSION"), "test\n").expect("a VERSION");
            for (tree, target) in trees {
                let link = path.join(tree);
                // A case may boot twice over one tree — the oracle does — and
                // the second boot re-declares the mounts it already has.
                if std::fs::symlink_metadata(&link).is_ok() {
                    std::fs::remove_file(&link).expect("the stale tree symlink is removable");
                }
                std::os::unix::fs::symlink(target, link).expect("a tree symlink");
            }
        }

        /// Rewrite the document over whatever mount directories exist now, so a
        /// case can retire one between reloads.
        ///
        /// A mount's ceiling is read back off the sidecar [`UNDER_FILE`] rather
        /// than held in this struct: the document is rewritten from the
        /// directories on disk, so a case that installs a config mount and then
        /// installs another must not lose the first one's `under` clause.
        pub(crate) fn write(&self) {
            let mut names: Vec<String> = std::fs::read_dir(&self.dir)
                .expect("the mounts directory is readable")
                .map(|entry| entry.expect("a readable entry").file_name())
                .filter_map(|name| name.into_string().ok())
                .filter(|name| name != MOUNTS_FILE)
                .collect();
            names.sort();
            let mut text = String::new();
            for name in names {
                let path = self.dir.join(&name);
                let under = match std::fs::read_to_string(path.join(UNDER_FILE)) {
                    Ok(principal) => format!("under {} ", principal.trim()),
                    Err(_) => String::new(),
                };
                text.push_str(&format!(
                    "mount {name} {under}{{ path = \"{}\"; }}\n",
                    path.display()
                ));
            }
            std::fs::write(self.path(), text).expect("the mounts document is writable");
        }

        /// Declare a config-carrying mount under `principal`, with `main.brenn`
        /// holding `fragment`, and return the mount's `config/` directory.
        ///
        /// A real directory rather than a symlink to one elsewhere: a case
        /// rewrites the fragment between reloads, and the tree it rewrites is
        /// the one the compiler reads.
        pub(crate) fn config(&self, name: &str, principal: &str, fragment: &str) -> PathBuf {
            let path = self.dir.join(name);
            let config = path.join("config");
            std::fs::create_dir_all(&config).expect("a config tree");
            std::fs::write(path.join("VERSION"), "test\n").expect("a VERSION");
            std::fs::write(path.join(UNDER_FILE), principal).expect("a ceiling");
            std::fs::write(config.join("main.brenn"), fragment).expect("a fragment");
            self.write();
            config
        }

        /// Rewrite a config mount's entry document, the way its author pushing
        /// a commit into the clone does.
        pub(crate) fn edit(&self, name: &str, fragment: &str) {
            std::fs::write(self.dir.join(name).join("config/main.brenn"), fragment)
                .expect("the fragment is writable");
        }

        /// Move a config mount's ceiling to another principal, which is the
        /// operator's edit and not the author's.
        pub(crate) fn move_ceiling(&self, name: &str, principal: &str) {
            std::fs::write(self.dir.join(name).join(UNDER_FILE), principal)
                .expect("the ceiling is writable");
            self.write();
        }

        /// Declare one more mount and put it in the document, the way an
        /// operator installing a bundle between reloads does.
        pub(crate) fn install(&self, name: &str, trees: &[(&str, &std::path::Path)]) -> PathBuf {
            self.declare(name, trees);
            self.write();
            self.dir.join(name)
        }

        /// Declare a mount whose path is a *symlink* to a versioned tree, which
        /// is the layout the bundle installer produces: an install stages
        /// `<mount>.v<VERSION>/` and swaps the link onto it, so the mount's
        /// canonical path moves on every deploy. Re-pointing an existing link is
        /// the upgrade.
        pub(crate) fn link(&self, name: &str, target: &std::path::Path) {
            let path = self.dir.join(name);
            if std::fs::symlink_metadata(&path).is_ok() {
                std::fs::remove_file(&path).expect("the stale mount symlink is removable");
            }
            std::os::unix::fs::symlink(target, &path).expect("a mount symlink");
            self.write();
        }

        /// Drop a declared mount's directory, the way an operator retiring a
        /// bundle does. The document still names it until [`Mounts::write`].
        pub(crate) fn uninstall(&self, name: &str) {
            std::fs::remove_dir_all(self.dir.join(name)).expect("the mount is removable");
        }

        pub(crate) fn path(&self) -> PathBuf {
            self.dir.join(MOUNTS_FILE)
        }

        /// What the document declares one mount's path as.
        pub(crate) fn declared_path(&self, name: &str) -> PathBuf {
            self.dir.join(name)
        }

        pub(crate) fn load(&self) -> LoadedMounts {
            brenn_lib::config::load_mounts(Some(&self.path()))
        }
    }

    /// The mounts document's name inside a fixture's mounts directory.
    const MOUNTS_FILE: &str = "mounts.brenn";

    /// Where a fixture mount records the principal its `config/` tree runs
    /// under. A dotfile inside the mount, which `verify_one` reads past: it
    /// looks for `VERSION` and the four tree directories and nothing else.
    const UNDER_FILE: &str = ".under";

    /// A booted process the driver decides against: the messaging layer the
    /// document brought up, the driver holding that document as its baseline,
    /// and the alerts anything raised.
    pub(crate) struct Booted {
        pub(crate) driver: ReloadDriver,
        /// The pulse the WS event loops re-check their user against.
        pub(crate) apps_swapped_tx: tokio::sync::broadcast::Sender<()>,
        /// The live bridge registry the commit's session step sweeps. Held so a
        /// case can seat a session and watch a reload retire it.
        pub(crate) active_bridges: brenn_server::active_bridge::ActiveBridges,
        pub(crate) messenger: Arc<Messenger>,
        pub(crate) router: Arc<brenn_server::messaging_router::WakeRouterImpl>,
        pub(crate) captured: Captured,
        pub(crate) db: brenn_db::Db,
        /// Held so a door can be opened over this process.
        pub(crate) reload_notify: Arc<tokio::sync::Notify>,
        /// The async tool executor's per-caller grant table, present on exactly
        /// the terms `run_server` builds one on: a document with an async tool
        /// grant somewhere in it, which is what mints the executor's spec.
        pub(crate) tool_caller_grants: Option<Arc<brenn_tool_registry::ToolCallerGrants>>,
        /// The mounts document the driver re-reads on every reload, and the
        /// directories it declares. Held because dropping it would take the
        /// mount trees with it.
        pub(crate) mounts: Mounts,
        /// The MQTT runtime the reload walks. Always present, as it is on a
        /// booted process; a fixture that asked for neither a registered nor a
        /// live client gets an empty registry.
        pub(crate) mqtt: (
            Arc<brenn_mqtt::MqttService>,
            Arc<brenn_server::mqtt_router::MqttEventRouterImpl>,
        ),
        /// The endpoint table the reload's webhook steps walk — the same one
        /// the HTTP layer would be matching requests against. Built over the
        /// booted document's endpoints, with a real replay component per
        /// replay-protected one, so a candidate is compared against what is
        /// actually being served.
        pub(crate) webhook: Arc<brenn_webhook::WebhookService>,
        /// The dispatcher task, when the fixture asked for one.
        ///
        /// Held rather than detached so that a wait for something the
        /// dispatcher produces can answer with the dispatcher's own panic. The
        /// dispatch and wake paths report host bugs by panicking, and a panic
        /// on a detached tokio task is swallowed by the runtime: the case would
        /// otherwise wait out its whole budget and fail as "saw 0 messages",
        /// losing the message that says which invariant broke.
        pub(crate) dispatcher: Option<tokio::task::JoinHandle<()>>,
    }

    /// Plan a document the way the driver plans a candidate, so the baseline
    /// and every candidate are lowered by one pass with one set of inputs.
    fn plan_like_the_driver(
        config: &BrennConfig,
        apps: &Arc<IndexMap<String, AppConfig>>,
        tool_registry: &Arc<brenn_tool_registry::ToolRegistry>,
    ) -> MessagingPlan {
        plan_messaging(&PlanInputs {
            config,
            apps: Some(apps),
            // The document's own, so a fixture declaring an `mqtt_client`
            // derives the ingress channels a host would. The driver reads the
            // booted identities for the same reason.
            mqtt_clients: &client_identities(config),
            tool_registry: Some(tool_registry),
            replay_store_paths: &brenn_lib::webhook::config::webhook_store_paths(
                webhook_identities(config).values(),
            ),
        })
        .expect("the fixture document configures messaging")
    }

    /// The per-agent webhook subscription stamps a fixture document resolves,
    /// which is what boot hands `resolve_apps`. Read off the document rather
    /// than defaulted to empty: a fixture declaring an endpoint an agent
    /// subscribes to has to resolve the same map on both paths, or the
    /// baseline's agents would hold no webhook subscription and every candidate
    /// would report one added.
    fn webhook_stamps(
        config: &BrennConfig,
    ) -> std::collections::BTreeMap<
        String,
        Vec<brenn_lib::webhook::config::ResolvedWebhookSubscription>,
    > {
        webhook_halves(config).1
    }

    /// The endpoint identities a fixture document declares.
    fn webhook_identities(
        config: &BrennConfig,
    ) -> IndexMap<String, brenn_lib::webhook::config::WebhookEndpointIdentity> {
        webhook_halves(config).0
    }

    /// The webhook document half, run the way boot and prepare both run it.
    fn webhook_halves(
        config: &BrennConfig,
    ) -> (
        IndexMap<String, brenn_lib::webhook::config::WebhookEndpointIdentity>,
        std::collections::BTreeMap<
            String,
            Vec<brenn_lib::webhook::config::ResolvedWebhookSubscription>,
        >,
    ) {
        brenn_lib::webhook::config::resolve_webhook_identities(
            &config.webhook_endpoints,
            &config.apps,
            &config.wasm_consumers,
            &config.wasm,
            &config.messaging,
        )
    }

    /// The `[[mqtt_client]]` identities a fixture document declares.
    fn client_identities(config: &BrennConfig) -> IndexMap<String, MqttClientIdentity> {
        brenn_lib::mqtt::config::resolve_client_identities(&config.mqtt_clients)
    }

    /// A live `MqttService` and ingress router over the document's **declared**
    /// clients, as boot builds them.
    ///
    /// Declared and not referenced, because that is the set boot spawns a
    /// session for.
    ///
    /// The handles carry the document's own resolved config — secrets and all,
    /// which is what a reload's client delta compares against, so an unmoved
    /// `[[mqtt_client]]` block produces no change.
    ///
    /// No *connection* supervisor is spawned, so every session exists and none
    /// has a connection: a SUBSCRIBE at commit comes back
    /// `DeferredDisconnected`, which is the outcome a broker-down reload has and
    /// the one that leaves the filter in the reconnect-survival set for a test to
    /// read. What is spawned is a stand-in task that watches the same stop
    /// signal and exits when it is set, so a reload that stops or restarts one
    /// of these clients has a supervisor to join — which is what commit does.
    async fn mqtt_runtime(
        plan: &MessagingPlan,
        clients: &IndexMap<String, brenn_lib::mqtt::config::MqttClientConfig>,
        db: &brenn_db::Db,
    ) -> (
        Arc<brenn_mqtt::MqttService>,
        Arc<brenn_server::mqtt_router::MqttEventRouterImpl>,
    ) {
        use brenn_server::mqtt_router::IngressRoute;

        let service = brenn_mqtt::MqttService::new();
        for (slug, config) in clients {
            let (stop_tx, mut stop_rx) = tokio::sync::watch::channel(false);
            let handle = brenn_mqtt::MqttClientHandle::new(
                Arc::new(config.clone()),
                brenn_mqtt::union_subscriptions(slug, &plan.mqtt_ingress_channels),
                stop_tx,
            );
            service.add_client(handle.clone());
            handle
                .set_supervisor(tokio::spawn(async move {
                    while !*stop_rx.borrow_and_update() {
                        if stop_rx.changed().await.is_err() {
                            return;
                        }
                    }
                }))
                .await;
        }
        let router = Arc::new(brenn_server::mqtt_router::MqttEventRouterImpl::new());
        router.set_state(
            brenn_server::test_support::state::test_state(db),
            plan.mqtt_ingress_channels
                .iter()
                .map(IngressRoute::from)
                .collect(),
        );
        (service, router)
    }

    /// The MQTT subsystem boot builds, over a broker that is really listening.
    ///
    /// This is `start_mqtt` and `wire_mqtt_state` themselves rather than a
    /// fixture-shaped imitation of them: the question a live case asks is
    /// whether a filter the reload SUBSCRIBEs on is one the process then
    /// receives on, and an imitation of the wiring is the one thing that cannot
    /// answer it.
    async fn live_mqtt_runtime(
        config: &BrennConfig,
        plan: &MessagingPlan,
        dynamic_ingress: &[brenn_messaging_boot::DynamicMqttIngress],
        db: &brenn_db::Db,
        messenger: &Arc<Messenger>,
    ) -> (
        Arc<brenn_mqtt::MqttService>,
        Arc<brenn_server::mqtt_router::MqttEventRouterImpl>,
    ) {
        let clients = brenn_lib::mqtt::config::resolve_clients(&config.mqtt_clients);
        // The supervisor's SUBSCRIBE union and the router's route table are
        // built from the static ingress set alone, so a durable dynamic `mqtt:`
        // row the boot merge kept is appended here, with `urgency` filled from
        // the client the row was created against. Without it a dormant row
        // revived by a reload would be the first filter this process ever
        // asserted for its channel, and the case about revoking one would have
        // nothing to revoke.
        let mut ingress = plan.mqtt_ingress_channels.clone();
        for kept in dynamic_ingress {
            let client = clients.get(&kept.client_slug).unwrap_or_else(|| {
                panic!(
                    "the fixture document declares no client {:?} for the dynamic row on {:?}",
                    kept.client_slug, kept.channel_address,
                )
            });
            ingress.push(brenn_lib::mqtt::config::ResolvedMqttIngressChannel {
                channel_address: kept.channel_address.clone(),
                channel_uuid: kept.channel_uuid,
                client_slug: kept.client_slug.clone(),
                topic: kept.topic.clone(),
                qos: kept.qos,
                urgency: client.identity.urgency,
            });
        }
        let result = crate::mqtt::start_mqtt(&ingress, &clients).await;
        // The state the router delivers through must carry the messenger this
        // process publishes with: an inbound packet reaches the bus through it,
        // and a state without one panics the delivery path on the first
        // message.
        let mut state = brenn_server::test_support::state::test_state(db);
        state.messenger = Some(messenger.clone());
        crate::mqtt::wire_mqtt_state(&result.service, &result.event_router, state, &ingress).await;
        (result.service, result.event_router)
    }

    /// A fixture document's agent map, resolved as boot resolves it.
    ///
    /// The integration registry is empty and the webhook stamps are: no
    /// fixture document declares an integration or a webhook subscription. The
    /// client identities are the document's own, as boot derives them and as
    /// the driver's `ReloadEnv` holds them — an agent whose ACL names a
    /// declared broker resolves on both paths or on neither.
    pub(crate) fn resolve_fixture_apps(
        config: &BrennConfig,
        runtime_dir: &std::path::Path,
    ) -> IndexMap<String, AppConfig> {
        brenn_lib::config::resolve_apps(
            config,
            &brenn_lib::integration::IntegrationRegistry::new(vec![]),
            Some(runtime_dir),
            &client_identities(config),
            &webhook_stamps(config),
        )
    }

    /// The axes a fixture boot varies. Every field defaults to what most cases
    /// want, so a case names only what it is about.
    #[derive(Default)]
    pub(crate) struct BootFixture {
        /// The database to boot over; absent mints an in-memory one. Named by
        /// the cases that boot twice over one store.
        pub(crate) db: Option<brenn_db::Db>,
        /// The components roots. Absent is the shape of a document declaring no
        /// consumer, whose roots are read only by the cross-root scan prepare
        /// re-runs.
        pub(crate) components_roots: Vec<PathBuf>,
        /// The tool registry, for the documents whose consumers hold async tool
        /// grants; absent mints an empty one.
        pub(crate) tool_registry: Option<Arc<brenn_tool_registry::ToolRegistry>>,
        /// A deployed surface asset tree to declare as one more mount, in the
        /// shape a release installs: the kernel pair at its root and one
        /// `processor/<kind>/` directory per kind it serves.
        pub(crate) surface_assets: Option<PathBuf>,
        /// A second surface tree, declared under a mount of its own, in the
        /// shape a component bundle installs: kinds and no kernel pair. Present
        /// only for the cases about a kind whose record this binary does not
        /// read, which is a withheld kind under a bundle mount and a boot
        /// refusal under brenn's own.
        pub(crate) surface_bundle: Option<PathBuf>,
        /// Stand up the MQTT subsystem the way boot does, against whatever
        /// broker the document's `mqtt_client` names — one real supervisor per
        /// declared client, dialing and staying connected. Needs a broker to be
        /// listening.
        ///
        /// Off by default, which is not "no MQTT": every fixture registers a
        /// session per declared client, with a stand-in supervisor that holds
        /// no connection, so every SUBSCRIBE defers and no packet moves. That
        /// is the shape the cases about the *plan* want.
        pub(crate) mqtt_live: bool,
        /// Run a dispatcher over this process. Off by default: a published row
        /// is then stored and nobody is woken, so nothing advances a cursor
        /// behind a test's back — which is what the oracle's comparison of two
        /// independently timed processes rests on. The cases that watch an
        /// activation turn it on.
        pub(crate) dispatcher: bool,
    }

    /// Boot the messaging layer on `tree`'s document and hand back a driver
    /// whose baseline is it, varying only the components roots.
    pub(crate) async fn boot(tree: &Tree, components_roots: Vec<PathBuf>) -> Booted {
        boot_with(
            tree,
            BootFixture {
                components_roots,
                ..BootFixture::default()
            },
        )
        .await
    }

    /// [`boot`] over every axis a case may vary.
    pub(crate) async fn boot_with(tree: &Tree, fixture: BootFixture) -> Booted {
        let BootFixture {
            db,
            components_roots,
            tool_registry,
            surface_assets,
            surface_bundle,
            mqtt_live,
            dispatcher,
        } = fixture;
        // The roots every load below reads are the mounts document's, exactly
        // as `run_server` derives them: a record read out of a path the reload
        // would not name is a baseline that disagrees with every candidate.
        let mounts = tree.mounts(&components_roots);
        if let Some(assets) = &surface_assets {
            mounts.install(SURFACE_MOUNT, &[("surface", assets)]);
        }
        if let Some(bundle) = &surface_bundle {
            mounts.install(SURFACE_BUNDLE_MOUNT, &[("surface", bundle)]);
        }
        let loaded_mounts = mounts.load();
        let components_roots = loaded_mounts.roots.components_roots.clone();
        let db = db.unwrap_or_else(init_db_memory);
        let tool_registry = tool_registry
            .unwrap_or_else(|| Arc::new(brenn_tool_registry::ToolRegistry::new(vec![])));
        let document = check_config(&deployment_inputs(&tree.root(), &loaded_mounts.roots))
            .expect("the fixture document must load");
        // Resolved by the same function boot and reload call, so the
        // document and the map cannot disagree.
        let runtime_dir = tree.runtime_dir();
        let apps = Arc::new(resolve_fixture_apps(&document.config, &runtime_dir));
        // What boot writes for every agent, so the file the reload renames onto
        // has a predecessor and a fresh boot of the same rig produces its own.
        // Without it the oracle's virtual-tools field would compare a file the
        // reload wrote against one nothing wrote.
        for app in apps.values() {
            brenn_server::active_bridge::write_virtual_tools_file(app, &tool_registry);
        }
        let (alert_dispatcher, captured, _drain) = make_capturing_alerter_with_severity();

        // One registry for the whole rig: the reload's session steps, the wake
        // router and a case that seats a session all read the same one.
        let active_bridges = brenn_server::active_bridge::ActiveBridges::new();
        let result = brenn_messaging_boot::test_fixtures::boot_messaging_over_bridges(
            &document.config,
            db.clone(),
            &apps,
            alert_dispatcher.clone(),
            "brenn://test",
            &tool_registry,
            active_bridges.clone(),
        )
        .await
        .0;
        let messenger = result.messenger.clone().expect("messaging must be up");
        let router = result.router.clone().expect("the wake router must be up");
        // The delivery bindings boot registers for everything that is not a
        // consumer. Without them the cross-check commit runs at the end of its
        // walk would fail on the reader app and the facility's own participant
        // — which is boot's wiring, not the reload's.
        for slug in apps.keys() {
            router.register_delivery_binding(
                SubscriberEntryKind::App(slug.clone()),
                DeliveryBinding::ConversationBridge,
            );
        }
        let mut reload_notify = None;
        for spec in &result.system_participants {
            if spec.subscriptions.is_empty() {
                continue;
            }
            let notify = Arc::new(tokio::sync::Notify::new());
            if spec.component == brenn_messaging::config_reload::CONFIG_RELOAD_COMPONENT {
                reload_notify = Some(Arc::clone(&notify));
            }
            router.register_delivery_binding(
                SubscriberEntryKind::System(spec.component.to_string()),
                DeliveryBinding::ParkedNotify(notify),
            );
        }
        let reload_notify =
            reload_notify.expect("every fixture document declares the reload facility");
        let dispatcher = dispatcher.then(|| {
            let handle = brenn_messaging::dispatcher::spawn_dispatcher_task(
                db.clone(),
                router.clone() as Arc<dyn brenn_messaging::WakeRouter>,
                messenger.dispatch_kick_notify(),
                messenger.clone(),
            );
            // Kick immediately so the first dispatch does not wait out the
            // poll interval — production boot does the same.
            messenger.dispatch_kick();
            handle
        });
        let plan = plan_like_the_driver(&document.config, &apps, &tool_registry);
        // Registered from the document's own declarations whether or not a
        // fixture asked for the live variant: boot spawns a session per
        // *declared* client, so a rig that left one unregistered would be a
        // process a fresh boot of its own document does not match — and the
        // reload would read the gap as a client to add.
        let mqtt = if mqtt_live {
            live_mqtt_runtime(
                &document.config,
                &plan,
                &result.dynamic_mqtt_ingress,
                &db,
                &messenger,
            )
            .await
        } else {
            mqtt_runtime(
                &plan,
                &brenn_lib::mqtt::config::resolve_clients(&document.config.mqtt_clients),
                &db,
            )
            .await
        };

        // The async tool executor's grant table: the plan's own value, installed
        // where the executor's spec exists — which is when some consumer holds
        // an async tool grant, and is the condition `run_server` reads too.
        let tool_caller_grants = result
            .system_participants
            .iter()
            .any(|spec| spec.component == brenn_tool_registry::TOOL_EXECUTOR_COMPONENT)
            .then(|| {
                Arc::new(brenn_tool_registry::ToolCallerGrants::new(
                    plan.tool_caller_grants.clone(),
                ))
            });

        // Every consumer the booted document declares must be loaded, bound,
        // started, and held. Without this the baseline's registry would disagree
        // with its directory, which is the one thing a reload may never see.
        let mut registry = ConsumerRegistry::new();
        for consumer in &plan.wasm_consumers {
            let one = load_consumer(
                &ConsumerLoadContext {
                    components_roots: &components_roots,
                    alert_dispatcher: &alert_dispatcher,
                    mqtt_service: mqtt.0.clone(),
                    tool_registry: &tool_registry,
                    max_payload_bytes: document.config.messaging.max_body_bytes,
                },
                consumer,
                None,
            );
            router.register_delivery_binding(
                SubscriberEntryKind::Wasm(consumer.slug.clone()),
                DeliveryBinding::ParkedNotify(one.notify.clone()),
            );
            registry.insert(
                consumer.slug.clone(),
                crate::consumers::start_consumer(one, consumer, &messenger, &alert_dispatcher),
            );
        }

        // The surface wiring boot installs beside the runtimes: one delivery
        // binding per surface, and the runtime table the doors read. Held so a
        // case can watch a reload retire and start a surface in it.
        let surfaces_cell =
            brenn_server::state::SurfaceCell::holding(match plan.surfaces.is_empty() {
                true => std::collections::HashMap::new(),
                false => brenn_surface_server::build_surface_runtimes(
                    plan.surfaces.clone(),
                    Some(messenger.clone()),
                    document.config.messaging.max_body_bytes,
                    document.config.observability.surface_error_channel.clone(),
                    brenn_surface_server::SurfaceDescriptionParams {
                        prefix: document.config.surface_description.prefix.clone(),
                    },
                ),
            });
        for surface in &plan.surfaces {
            router.register_surface_delivery_routes(surface);
        }
        // The endpoint table boot installs: the document half, then the host
        // half over whatever secret files the fixture wrote, then the same
        // builder `run_server` calls — so a replay-protected endpoint's
        // component and store are the real ones.
        let webhook_service = crate::webhook::build_webhook(
            brenn_lib::webhook::config::resolve_webhook_endpoints(&webhook_identities(
                &document.config,
            )),
            &components_roots,
        )
        .service;

        let attach_registry = brenn_attach_server::registry::AttachRegistry::default();

        let surface_roots = brenn_surface_server::validate_surface_assets(
            &loaded_mounts.roots.surface_roots,
            &plan.surfaces,
        );
        // Boot's own announcement, called here for the same reason the
        // description publish below is: a fixture that skipped it would leave
        // the operator's only unprompted notice of a withheld kind unreachable
        // from every test.
        crate::alert_withheld_kinds(&alert_dispatcher, &surface_roots);

        // Without this the fresh side would read back whatever the database
        // it booted over still retained rather than what boot publishes.
        crate::publish_boot_surface_documents(
            &messenger,
            &document.config,
            TEST_BUILD_ID,
            &plan.surfaces,
            &surface_roots,
        )
        .await;

        let root = tree.root().display().to_string();
        // The pulse is held by the fixture too, so a case can watch the commit
        // ask every open socket to re-check its user.
        let bridges_for_fixture = active_bridges.clone();
        let apps_swapped_tx = tokio::sync::broadcast::channel(16).0;
        let driver = ReloadDriver::new(
            ReloadEnv {
                config_path: tree.root(),
                root: Some(root),
                build_id: TEST_BUILD_ID,
                apps: messenger.app_table(),
                integration_registry: Arc::new(brenn_lib::integration::IntegrationRegistry::new(
                    vec![],
                )),
                runtime_dir: Some(runtime_dir),
                tool_registry,
                webhook: webhook_service.clone(),
                surface_roots: Arc::new(std::sync::RwLock::new(Arc::new(surface_roots))),
                surfaces: surfaces_cell,
                attach_registry,
                mqtt_service: mqtt.0.clone(),
                mqtt_event_router: mqtt.1.clone(),
                max_payload_bytes: document.config.messaging.max_body_bytes,
                active_bridges,
                apps_swapped_tx: apps_swapped_tx.clone(),
                messenger: messenger.clone(),
                router: router.clone(),
                tool_caller_grants: tool_caller_grants.clone(),
                alert_dispatcher,
            },
            Baseline::of(document, loaded_mounts, &plan),
            registry,
        );
        Booted {
            driver,
            webhook: webhook_service,
            apps_swapped_tx,
            active_bridges: bridges_for_fixture,
            mounts,
            mqtt,
            messenger,
            router,
            captured,
            db,
            reload_notify,
            tool_caller_grants,
            dispatcher,
        }
    }

    pub(crate) async fn last_status_on(messenger: &Arc<Messenger>) -> ReloadStatus {
        outcomes_on(messenger)
            .await
            .pop()
            .expect("an outcome was published")
    }

    /// Seat the user and conversation an app-origin publish requires (the send
    /// budget row is foreign-keyed to a real conversation).
    pub(crate) async fn seat_a_conversation(db: &brenn_db::Db, conversation_id: i64) {
        let conn = db.lock().await;
        conn.execute(
            "INSERT INTO users (id, username, password_hash, created_at) \
             VALUES (1, 'reader', 'h', '2024-01-01')",
            [],
        )
        .expect("the user seats");
        conn.execute(
            "INSERT INTO conversations (id, user_id, status, app_slug, created_at, updated_at) \
             VALUES (?1, 1, 'active', ?2, '2024-01-01', '2024-01-01')",
            rusqlite::params![conversation_id, READER],
        )
        .expect("the conversation seats");
    }

    /// Every outcome on the status channel. Empty until something reports
    /// (boot's own `booted` publish is not part of these fixtures).
    pub(crate) async fn outcomes_on(messenger: &Arc<Messenger>) -> Vec<ReloadStatus> {
        messenger
            .query(&MessageQuery {
                channel: STATUS_ADDRESS.to_string(),
                limit: 100,
                before: None,
                after: None,
                sender: None,
                search: None,
                calling_app_slug: READER.to_string(),
            })
            .await
            .expect("the status channel is declared and readable")
            .into_iter()
            // The query answers newest first; reverse to chronological order.
            .rev()
            .map(|envelope| {
                serde_json::from_str(&envelope.body).expect("the retained body is the schema")
            })
            .collect()
    }

    /// Poll `read` until it answers at least `wanted` items, or panic naming
    /// `what` and what it saw.
    ///
    /// The suite's one timing policy: every case that waits on something a
    /// background task produces waits here, so a budget raised for a flaky
    /// case is raised for all of them.
    ///
    /// `watch` is the task the items are expected to come from, when there is
    /// one. A task that has ended is why they are not coming, so the wait stops
    /// there and re-raises its panic rather than reporting the silence it left.
    pub(crate) async fn poll_until<T: std::fmt::Debug>(
        what: &str,
        wanted: usize,
        mut watch: Option<&mut tokio::task::JoinHandle<()>>,
        read: impl AsyncFn() -> Vec<T>,
    ) -> Vec<T> {
        for _ in 0..400 {
            let seen = read().await;
            if seen.len() >= wanted {
                return seen;
            }
            if let Some(handle) = watch.as_deref_mut()
                && handle.is_finished()
            {
                match handle.await {
                    Err(ended) if ended.is_panic() => std::panic::resume_unwind(ended.into_panic()),
                    ended => panic!(
                        "the task {what} were waited on from ended before they arrived: {ended:?}"
                    ),
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        let seen = read().await;
        panic!(
            "waited for {wanted} {what} and saw {}: {seen:?}",
            seen.len()
        );
    }

    /// Poll until `wanted` outcomes appear, or panic.
    pub(crate) async fn outcomes_until(
        messenger: &Arc<Messenger>,
        wanted: usize,
    ) -> Vec<ReloadStatus> {
        poll_until("outcomes", wanted, None, async || {
            outcomes_on(messenger).await
        })
        .await
    }

    impl Booted {
        pub(crate) async fn last_status(&self) -> ReloadStatus {
            last_status_on(&self.messenger).await
        }

        pub(crate) async fn published_outcomes(&self) -> Vec<ReloadStatus> {
            outcomes_on(&self.messenger).await
        }

        /// The alerts raised so far. The dispatcher hands its queue to a drain
        /// task, so a fresh alert is visible a moment after it is raised.
        pub(crate) async fn alerts(&self) -> Vec<(AlertSeverity, String, String)> {
            for _ in 0..200 {
                let seen = self.captured.lock().expect("alert capture").clone();
                if !seen.is_empty() {
                    return seen;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            Vec::new()
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_comment_only_edit_is_unchanged_and_the_baseline_follows_it() {
        let tree = Tree::holding(&document(""));
        let mut booted = boot(&tree, Vec::new()).await;
        let booted_sha = booted.driver.baseline().document.document_sha256.clone();

        tree.write(&document("// what the operator was thinking\n"));
        let candidate_sha = tree.load().document_sha256;
        assert_ne!(candidate_sha, booted_sha, "the bytes must have moved");

        assert!(
            booted
                .driver
                .prepare_and_report(TriggerSource::Signal)
                .await
                .is_none(),
            "nothing to commit: the projection did not move"
        );

        // The running state already is this document's projection, so the
        // process now says it is projecting the text on disk — which is the
        // question the retained body exists to answer.
        assert_eq!(
            booted.driver.baseline().document.document_sha256,
            candidate_sha
        );
        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Unchanged);
        assert_eq!(status.trigger, Trigger::Signal);
        assert_eq!(status.document_sha256.as_deref(), Some(&*candidate_sha));
        assert_eq!(status.running_document_sha256, candidate_sha);
        // An unchanged outcome moved nothing, so it is not a generation.
        assert_eq!(status.generation, 0);
        assert!(status.refusals.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_document_that_no_longer_compiles_is_refused_with_its_diagnostics() {
        let tree = Tree::holding(&document(""));
        let mut booted = boot(&tree, Vec::new()).await;
        let booted_sha = booted.driver.baseline().document.document_sha256.clone();

        tree.write("channel work at {\n");
        assert!(
            booted
                .driver
                .prepare_and_report(TriggerSource::Bus)
                .await
                .is_none()
        );

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Refused);
        assert_eq!(status.trigger, Trigger::Bus);
        // Nothing compiled, so there is no candidate identity to name — and the
        // process is still projecting exactly what it booted on.
        assert_eq!(status.document_sha256, None);
        assert_eq!(status.running_document_sha256, booted_sha);
        assert_eq!(
            booted.driver.baseline().document.document_sha256,
            booted_sha
        );
        assert_eq!(status.refusals.len(), 1);
        assert!(
            status.refusals[0].contains("failed to"),
            "{:?}",
            status.refusals
        );

        // The principal that asked for the reload cannot decide that a restart
        // is due; the operator's phone is where that lands.
        let alerts = booted.alerts().await;
        assert_eq!(alerts.len(), 1, "{alerts:?}");
        assert!(matches!(alerts[0].0, AlertSeverity::Warning), "{alerts:?}");
        assert_eq!(alerts[0].1, "Config reload refused");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_non_convergible_edit_is_refused_naming_its_section() {
        let tree = Tree::holding(&document(""));
        let mut booted = boot(&tree, Vec::new()).await;
        let booted_sha = booted.driver.baseline().document.document_sha256.clone();

        tree.write(&document("messaging { max_body_bytes = 131072; }\n"));
        let candidate_sha = tree.load().document_sha256;
        assert!(
            booted
                .driver
                .prepare_and_report(TriggerSource::Signal)
                .await
                .is_none()
        );

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Refused);
        // The candidate compiled, so it has an identity — and it is not what
        // the process is projecting.
        assert_eq!(status.document_sha256.as_deref(), Some(&*candidate_sha));
        assert_eq!(status.running_document_sha256, booted_sha);
        assert_eq!(status.refusals.len(), 1);
        assert!(
            status.refusals[0].starts_with("messaging ")
                && status.refusals[0].ends_with(super::super::NEEDS_RESTART),
            "{:?}",
            status.refusals
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_new_channel_is_prepared_for_commit_and_nothing_is_published() {
        let tree = Tree::holding(&document(""));
        let booted = boot(&tree, Vec::new()).await;

        tree.write(&document(
            r#"
channel spare at "brenn:spare" {
    push_depth = 1;
    retain_depth = 1;
    standing_retain_depth = 1;
}
"#,
        ));
        let dynamic = booted.driver.dynamic_snapshot().await;
        let ready = match booted.driver.prepare(TriggerSource::Signal, &dynamic) {
            Prepared::Ready(ready) => ready,
            other => panic!("the candidate is applicable: {}", outcome_of(&other)),
        };
        assert_eq!(
            ready
                .delta
                .channels_added
                .iter()
                .map(|entry| entry.address.as_str())
                .collect::<Vec<_>>(),
            vec!["brenn:spare"],
        );
        assert!(ready.delta.channels_removed.is_empty());
        assert!(ready.loaded.is_empty(), "the document declares no consumer");
        assert_eq!(ready.document.document_sha256, tree.load().document_sha256);

        // Prepare is a computation: the live directory does not hold the new
        // channel until something commits it, and nothing has been said about
        // the reload on the bus either — reporting is `prepare_and_report`'s,
        // which is what makes prepare safe to call speculatively.
        assert!(
            booted
                .messenger
                .directory()
                .resolve("brenn:spare")
                .is_none()
        );
        assert!(
            booted.published_outcomes().await.is_empty(),
            "prepare publishes nothing"
        );
    }

    /// A consumer with an output and no input at all. The compiler admits it —
    /// every declared port is bound, which is the whole of the language's
    /// contract — and the *planner* refuses it, from inside the `catch_unwind`
    /// prepare wraps it in: a consumer with no subscriptions never activates,
    /// so its outputs are dead config.
    ///
    /// The refusal is spelled `[[wasm_consumer]] "slug": …` rather than with
    /// the resolvers' `config: ` marker, which is exactly the shape that used
    /// to be re-panicked as a host defect: a one-line edit to a *convergible*
    /// block would then have taken the process down instead of refusing a
    /// reload.
    ///
    /// The fixture is load-bearing on that asymmetry: it needs a document the
    /// compiler admits and the planner refuses, and there are few left. Teaching
    /// the language that an output-only consumer is dead config would take this
    /// one away, and whoever does that owes this test another such document
    /// rather than a weakened assertion.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_consumer_the_planner_refuses_is_a_refusal_and_not_an_unwind() {
        let tree = Tree::holding(&document(""));
        let mut booted = boot(&tree, Vec::new()).await;
        let booted_sha = booted.driver.baseline().document.document_sha256.clone();

        tree.write(&document(&format!(
            r#"{PACKAGED}component Sifter {{
    abi = processor;
    requires = [ports];
    out digest;
}}
{PACKAGED}
new sifter: Sifter {{
    grants = [ports];
    out digest -> work;
}}
"#
        )));
        let candidate_sha = tree.load().document_sha256;

        assert!(
            booted
                .driver
                .prepare_and_report(TriggerSource::Bus)
                .await
                .is_none()
        );

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Refused);
        assert_eq!(status.document_sha256.as_deref(), Some(&*candidate_sha));
        assert_eq!(status.running_document_sha256, booted_sha);
        assert_eq!(status.refusals.len(), 1, "{:?}", status.refusals);
        assert!(
            status.refusals[0].starts_with("[[wasm_consumer]] \"sifter\"")
                && status.refusals[0].contains("has output port(s) but no subscriptions"),
            "{:?}",
            status.refusals
        );
        assert_eq!(
            booted.driver.baseline().document.document_sha256,
            booted_sha
        );
    }

    /// Install `text` as the packaged module's component package under a
    /// components root: the artifact a real build produced, the record that
    /// binds it, and the module's own bytes as the packaged specification.
    ///
    /// The spec bytes have to be the module's, because that is the file the
    /// instance's class was declared in and therefore the hash the document
    /// carries — a package whose spec is anything else is refused by
    /// `verify_consumer`, which is the binding this fixture is here to satisfy.
    pub(crate) fn install_package(root: &std::path::Path, module_text: &str) {
        install_package_from(root, module_text, "brenn_processor_demo.wasm");
    }

    /// [`install_package`] over a named artifact, for the cases whose subject
    /// is the bytes under a package rather than the document over it.
    /// Returns the artifact hash it wrote into the record, so a caller whose
    /// subject is the bytes asserts against what was installed rather than
    /// re-deriving one from the staging root.
    pub(crate) fn install_package_from(
        root: &std::path::Path,
        module_text: &str,
        artifact: &str,
    ) -> String {
        let dir = root.join(PACKAGED_MODULE);
        std::fs::create_dir_all(&dir).expect("a package directory");
        let artifact_bytes = crate::consumers::fixture_artifact(artifact);
        let artifact_sha256 = brenn_lib::util::sha256_hex(&artifact_bytes);
        std::fs::write(dir.join("demo.wasm"), &artifact_bytes).expect("write the artifact");
        std::fs::write(dir.join(format!("{PACKAGED_MODULE}.brenn")), module_text)
            .expect("write the packaged spec");
        std::fs::write(
            dir.join("package.json"),
            format!(
                "{{\n  \"v\": 2,\n  \"name\": \"{PACKAGED_MODULE}\",\n  \"world\": \
                 \"brenn:processor\",\n  \"artifact\": \"demo.wasm\",\n  \
                 \"artifact_sha256\": \"{artifact_sha256}\",\n  \"spec\": \
                 \"{PACKAGED_MODULE}.brenn\",\n  \"spec_sha256\": \"{}\"\n}}\n",
                brenn_lib::util::sha256_hex(module_text.as_bytes()),
            ),
        )
        .expect("write the record");
        artifact_sha256
    }

    /// The staged module's bytes — the file the instance's class was declared
    /// in, and therefore the spec the package record has to carry.
    pub(crate) fn staged_module(tree: &Tree) -> String {
        staged_module_opt(tree).expect("the document stages a module")
    }

    /// The same, `None` for a document that declares no component and so stages
    /// no module. Lets a caller install a package iff there is one to install,
    /// instead of re-deriving from the document's shape which documents have
    /// one. Absence is the only tolerated failure; an unreadable file panics.
    pub(crate) fn staged_module_opt(tree: &Tree) -> Option<String> {
        let path = tree.modules().join(format!("{PACKAGED_MODULE}.brenn"));
        path.try_exists()
            .expect("the staged module directory is readable")
            .then(|| std::fs::read_to_string(&path).expect("the staged module is readable"))
    }

    /// A document declaring one consumer of the `processor-config` fixture
    /// component: it reads the work channel, and each directive it takes off it
    /// is answered on the sink channel with what its `config` map holds.
    pub(crate) fn document_with_a_configured_consumer(value: &str) -> String {
        configured_consumer_document(value, &[])
    }

    /// A document with a consumer and a push-enabled reader agent that
    /// subscribes to each channel `subscribes` names.
    ///
    /// Both sides of the reload this fixture drives must be the same height:
    /// the spec hash the installed package's record is bound to moves with
    /// line count. A filler comment stands where the candidate's `subscribe`
    /// line goes.
    pub(crate) fn consumer_and_push_subscriber(subscribes: &[&str]) -> String {
        let document = document_push_subscribing(&consumer_block("answer"), &["alice"], subscribes);
        if !subscribes.is_empty() {
            return document;
        }
        document.replace(
            "    send_budget = 1000000;\n",
            "    send_budget = 1000000;\n    // Nothing subscribed yet.\n",
        )
    }

    /// [`document_with_a_configured_consumer`] with the reader agent
    /// subscribing to the channels `subscribes` names.
    pub(crate) fn configured_consumer_document(value: &str, subscribes: &[&str]) -> String {
        document_subscribing(&consumer_block(value), subscribes)
    }

    /// The sink channel and the configured consumer that writes it, answering
    /// every directive with `value`.
    fn consumer_block(value: &str) -> String {
        format!(
            r#"channel sink at "brenn:sink" {{
    push_depth = 1;
    retain_depth = 4;
    standing_retain_depth = 4;
}}
{PACKAGED}component Prober {{
    abi = processor;
    requires = [ports, config];
    in inbound;
    out out;
}}
{PACKAGED}
new prober: Prober {{
    grants = [ports, config];
    config = {{ test-key = "{value}" }};
    in inbound <- work {{ push_depth = 4; }}
    out out -> sink;
}}
"#
        )
    }

    /// A document declaring one consumer of the demo component, reading the
    /// work channel.
    pub(crate) fn document_with_a_consumer() -> String {
        document(&format!(
            r#"channel sink at "brenn:sink" {{
    push_depth = 1;
    retain_depth = 4;
    standing_retain_depth = 4;
}}
{PACKAGED}component Demo {{
    abi = processor;
    requires = [ports];
    in inbound;
    out digest;
}}
{PACKAGED}
new sifter: Demo {{
    grants = [ports];
    in inbound <- work {{ push_depth = 4; }}
    out digest -> sink;
}}
"#
        ))
    }

    /// A document declaring one broker and a consumer bound to one `mqtt:`
    /// topic per entry in `topics`. The channels are literal addresses: an
    /// `mqtt:` entry is minted by the binding, never declared.
    pub(crate) fn document_with_an_mqtt_consumer(topics: &[&str]) -> String {
        let bindings: Vec<(&str, &str)> = topics.iter().map(|topic| ("ha", *topic)).collect();
        document_with_clients(&[("ha", 8883, None)], &bindings)
    }

    /// Two declared brokers and a consumer bound to one `mqtt:` topic per
    /// `(client, topic)` pair. A client no pair names is declared and
    /// referenced by nothing, and has a session all the same.
    pub(crate) fn document_with_two_brokers(bindings: &[(&str, &str)]) -> String {
        document_with_clients(&[("ha", 8883, None), ("spare", 8884, None)], bindings)
    }

    /// A client the document declares and nothing binds still has a session, so
    /// the first binding a reload puts on it converges.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_mqtt_binding_on_a_declared_but_unreferenced_client_converges() {
        const ADDRESS: &str = "mqtt:spare:home/other";
        let tree = Tree::holding(&document_with_two_brokers(&[("ha", "home/state")]));
        let components = tempfile::tempdir().expect("a components root");
        install_package(components.path(), &staged_module(&tree));
        let mut booted = boot_with(
            &tree,
            BootFixture {
                components_roots: vec![components.path().to_path_buf()],
                ..BootFixture::default()
            },
        )
        .await;
        let (service, router) = booted.mqtt.clone();
        assert_eq!(
            service.ingress_filter_qos("spare", "home/other").await,
            None
        );

        tree.write(&document_with_two_brokers(&[
            ("ha", "home/state"),
            ("spare", "home/other"),
        ]));
        // The second binding adds a port, so the component's specification
        // moves with it; the installed package has to be the one the candidate
        // document names or the outcome would be the spec-binding refusal.
        install_package(components.path(), &staged_module(&tree));
        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert_eq!(status.delta.mqtt_subscribed, vec![ADDRESS.to_string()]);
        assert_eq!(
            status.delta.mqtt_deferred,
            vec![ADDRESS.to_string()],
            "a registered but disconnected client defers every SUBSCRIBE",
        );
        assert_eq!(
            service.ingress_filter_qos("spare", "home/other").await,
            Some(1),
            "the arriving filter is not in the reconnect-survival set",
        );
        let uuid = booted
            .messenger
            .directory()
            .resolve(ADDRESS)
            .expect("the entry is in the directory")
            .uuid;
        assert!(
            router.route_uuids().contains(&uuid),
            "{ADDRESS} has no route"
        );
    }

    /// The same reload against a client whose supervisor has given up: the
    /// reload applies and the filter is registered, but no connect in this
    /// process will assert it, so the status body must say `mqtt_failed` and
    /// not `mqtt_deferred`. Consumers of the status body treat the deferred
    /// list as "wait" and would wait forever.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_binding_on_a_failed_client_is_reported_failed_not_deferred() {
        const ADDRESS: &str = "mqtt:spare:home/other";
        let tree = Tree::holding(&document_with_two_brokers(&[("ha", "home/state")]));
        let components = tempfile::tempdir().expect("a components root");
        install_package(components.path(), &staged_module(&tree));
        let mut booted = boot_with(
            &tree,
            BootFixture {
                components_roots: vec![components.path().to_path_buf()],
                ..BootFixture::default()
            },
        )
        .await;
        let (service, _router) = booted.mqtt.clone();
        // A placeholder broker whose credentials are wrong: declared, dialled
        // at boot, rejected authoritatively, not retrying.
        let handle = service.get_client("spare").expect("a declared client");
        *handle.supervisor_state.write().await = brenn_mqtt::state::SupervisorState::Failed {
            reason: "authoritative connect failure: bad user name or password".to_string(),
        };

        tree.write(&document_with_two_brokers(&[
            ("ha", "home/state"),
            ("spare", "home/other"),
        ]));
        install_package(components.path(), &staged_module(&tree));
        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert_eq!(status.delta.mqtt_subscribed, vec![ADDRESS.to_string()]);
        assert_eq!(
            status.delta.mqtt_failed,
            vec![ADDRESS.to_string()],
            "a client that stopped retrying has no reconnect to defer to",
        );
        assert!(
            status.delta.mqtt_deferred.is_empty(),
            "a filter is on one list or the other, never both: {:?}",
            status.delta.mqtt_deferred,
        );
        assert_eq!(
            service.ingress_filter_qos("spare", "home/other").await,
            Some(1),
            "the filter is registered all the same — a fixed process asserts it",
        );
    }

    /// One declared broker and nothing bound through it: a client a fresh boot
    /// spawns a supervisor for and subscribes nothing on.
    pub(crate) fn document_with_a_broker_only() -> String {
        document(
            r#"mqtt_client ha {
    url = "mqtts://127.0.0.1:8883";
    qos = 1;
}
"#,
        )
    }

    /// A document declares a broker and binds nothing through it; a later
    /// reload brings the first consumer. The declaration gave the client a
    /// session at boot, so the binding converges.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_first_binding_on_a_broker_only_document_converges() {
        const TOPIC: &str = "home/state";
        let address = format!("mqtt:ha:{TOPIC}");
        let tree = Tree::holding(&document_with_a_broker_only());
        let components = tempfile::tempdir().expect("a components root");
        let mut booted = boot_with(
            &tree,
            BootFixture {
                components_roots: vec![components.path().to_path_buf()],
                ..BootFixture::default()
            },
        )
        .await;
        let (service, router) = booted.mqtt.clone();
        assert_eq!(service.client_slugs(), vec!["ha".to_string()]);
        assert!(router.route_uuids().is_empty());

        tree.write(&document_with_an_mqtt_consumer(&[TOPIC]));
        install_package(components.path(), &staged_module(&tree));
        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert_eq!(status.delta.mqtt_subscribed, vec![address.clone()]);
        assert_eq!(
            service.ingress_filter_qos("ha", TOPIC).await,
            Some(1),
            "the first filter on the idle session is not in its set",
        );
        let uuid = booted
            .messenger
            .directory()
            .resolve(&address)
            .expect("the entry is in the directory")
            .uuid;
        assert_eq!(router.route_uuids(), vec![uuid]);
    }

    /// The other direction: the last binding on a client leaves, its filter and
    /// route go with it, and the session stays — which is what a fresh boot of
    /// the new document has too.
    #[tokio::test(flavor = "multi_thread")]
    async fn dropping_a_clients_last_binding_converges_and_keeps_the_session() {
        const TOPIC: &str = "home/state";
        let address = format!("mqtt:ha:{TOPIC}");
        let tree = Tree::holding(&document_with_an_mqtt_consumer(&[TOPIC]));
        let components = tempfile::tempdir().expect("a components root");
        install_package(components.path(), &staged_module(&tree));
        let mut booted = boot_with(
            &tree,
            BootFixture {
                components_roots: vec![components.path().to_path_buf()],
                ..BootFixture::default()
            },
        )
        .await;
        let (service, router) = booted.mqtt.clone();
        assert_eq!(service.ingress_filter_qos("ha", TOPIC).await, Some(1));

        tree.write(&document_with_a_broker_only());
        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert_eq!(status.delta.mqtt_unsubscribed, vec![address.clone()]);
        assert_eq!(service.ingress_filter_qos("ha", TOPIC).await, None);
        assert!(router.route_uuids().is_empty());
        assert_eq!(
            service.client_slugs(),
            vec!["ha".to_string()],
            "the session outlives its last binding",
        );
    }

    /// A document declaring one `mqtt_client` per entry — slug, broker port and
    /// an optional `password_file` — with a consumer bound to one `mqtt:` topic
    /// per binding.
    pub(crate) fn document_with_clients(
        clients: &[(&str, u16, Option<&std::path::Path>)],
        bindings: &[(&str, &str)],
    ) -> String {
        let blocks: String = clients
            .iter()
            .map(|(slug, port, password)| {
                let credential = password.map_or_else(String::new, |path| {
                    format!("    password_file = \"{}\";\n", path.display())
                });
                format!(
                    "mqtt_client {slug} {{\n    url = \"mqtts://127.0.0.1:{port}\";\n\
                     {credential}    qos = 1;\n}}\n\n"
                )
            })
            .collect();
        let ports: String = (0..bindings.len())
            .map(|index| format!("    in inbound{index};\n"))
            .collect();
        let wiring: String = bindings
            .iter()
            .enumerate()
            .map(|(index, (client, topic))| {
                format!(
                    "    in inbound{index} <- \"mqtt:{client}:{topic}\" {{ push_depth = 4; \
                     retain_depth = 4; }}\n"
                )
            })
            .collect();
        document(&format!(
            r#"{blocks}channel sink at "brenn:sink" {{
    push_depth = 1;
    retain_depth = 4;
    standing_retain_depth = 4;
}}
{PACKAGED}component Demo {{
    abi = processor;
    requires = [ports];
{ports}    out digest;
}}
{PACKAGED}
new sifter: Demo {{
    grants = [ports];
{wiring}    out digest -> sink;
}}
"#
        ))
    }

    /// A `[[mqtt_client]]` the candidate declares and the process does not hold
    /// is registered by the commit, with a supervisor of its own — the deploy
    /// story the facility exists for, and a level-1 refusal until this slice.
    ///
    /// Its filters are reported `mqtt_deferred`: the supervisor was spawned
    /// microseconds earlier and asserts them on its first connect, which is
    /// what that list has always promised.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_added_client_is_registered_and_its_first_binding_defers() {
        const ADDRESS: &str = "mqtt:spare:home/other";
        let one = [("ha", 8883u16, None)];
        let two = [("ha", 8883u16, None), ("spare", 8884u16, None)];
        let tree = Tree::holding(&document_with_clients(&one, &[("ha", "home/state")]));
        let components = tempfile::tempdir().expect("a components root");
        install_package(components.path(), &staged_module(&tree));
        let mut booted = boot_with(
            &tree,
            BootFixture {
                components_roots: vec![components.path().to_path_buf()],
                ..BootFixture::default()
            },
        )
        .await;
        let (service, router) = booted.mqtt.clone();
        assert_eq!(service.client_slugs(), vec!["ha".to_string()]);

        tree.write(&document_with_clients(
            &two,
            &[("ha", "home/state"), ("spare", "home/other")],
        ));
        install_package(components.path(), &staged_module(&tree));
        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert_eq!(status.delta.mqtt_clients_added, vec!["spare".to_string()]);
        assert!(status.delta.mqtt_clients_removed.is_empty());
        assert!(status.delta.mqtt_clients_changed.is_empty());
        assert_eq!(
            service.client_slugs(),
            vec!["ha".to_string(), "spare".to_string()],
        );
        assert_eq!(status.delta.mqtt_subscribed, vec![ADDRESS.to_string()]);
        assert_eq!(
            status.delta.mqtt_deferred,
            vec![ADDRESS.to_string()],
            "a supervisor spawned in this walk asserts its filters on its first connect",
        );
        assert!(status.delta.mqtt_failed.is_empty());
        assert_eq!(
            service.ingress_filter_qos("spare", "home/other").await,
            Some(1),
            "the arriving filter is not on the arriving client's handle",
        );
        let uuid = booted
            .messenger
            .directory()
            .resolve(ADDRESS)
            .expect("the entry is in the directory")
            .uuid;
        assert!(
            router.route_uuids().contains(&uuid),
            "{ADDRESS} has no route"
        );
        booted.stop_mqtt();
    }

    /// A client the candidate no longer declares loses its session, and its
    /// filters are reported as no move at all: they leave with the session, and
    /// an UNSUBSCRIBE named in the status body would be a packet nothing sent.
    /// Its route still goes — the ingress table has to lose it.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_removed_client_is_stopped_and_contributes_no_filter_moves() {
        const ADDRESS: &str = "mqtt:spare:home/other";
        let one = [("ha", 8883u16, None)];
        let two = [("ha", 8883u16, None), ("spare", 8884u16, None)];
        let tree = Tree::holding(&document_with_clients(
            &two,
            &[("ha", "home/state"), ("spare", "home/other")],
        ));
        let components = tempfile::tempdir().expect("a components root");
        install_package(components.path(), &staged_module(&tree));
        let mut booted = boot_with(
            &tree,
            BootFixture {
                components_roots: vec![components.path().to_path_buf()],
                ..BootFixture::default()
            },
        )
        .await;
        let (service, router) = booted.mqtt.clone();
        let uuid = booted
            .messenger
            .directory()
            .resolve(ADDRESS)
            .expect("the entry is in the directory")
            .uuid;
        assert!(router.route_uuids().contains(&uuid));

        tree.write(&document_with_clients(&one, &[("ha", "home/state")]));
        install_package(components.path(), &staged_module(&tree));
        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert_eq!(status.delta.mqtt_clients_removed, vec!["spare".to_string()]);
        assert_eq!(service.client_slugs(), vec!["ha".to_string()]);
        assert!(
            status.delta.mqtt_unsubscribed.is_empty(),
            "a stopped client's filters are not moves: {:?}",
            status.delta.mqtt_unsubscribed,
        );
        assert!(
            status.delta.mqtt_deferred.is_empty(),
            "{:?}",
            status.delta.mqtt_deferred,
        );
        assert!(
            !router.route_uuids().contains(&uuid),
            "{ADDRESS} still has a route",
        );
        assert!(service.get_client("spare").is_none());
    }

    /// A rotated broker password with no document edit at all: the file's bytes
    /// are part of the resolved client, so the reload applies and the client's
    /// supervisor is restarted with the new credential — carrying the filters
    /// the predecessor held.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_rotated_broker_password_restarts_the_client_without_a_document_edit() {
        const TOPIC: &str = "home/state";
        // The secret is written before the document that names it, because it
        // has to be readable at boot as well as at the reload.
        let tree = Tree::new();
        let password = tree.secret("broker.pw", "first");
        let clients = [("ha", 8883u16, Some(password.as_path()))];
        tree.write(&document_with_clients(&clients, &[("ha", TOPIC)]));
        let components = tempfile::tempdir().expect("a components root");
        install_package(components.path(), &staged_module(&tree));
        let mut booted = boot_with(
            &tree,
            BootFixture {
                components_roots: vec![components.path().to_path_buf()],
                ..BootFixture::default()
            },
        )
        .await;
        let (service, _router) = booted.mqtt.clone();
        let before = service.get_client("ha").expect("a declared client");
        assert_eq!(before.config.password.as_deref(), Some("first"));

        std::fs::write(&password, "second").expect("the secret file is writable");
        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert_eq!(status.delta.mqtt_clients_changed, vec!["ha".to_string()]);
        assert!(status.delta.mqtt_clients_added.is_empty());
        assert!(status.delta.mqtt_clients_removed.is_empty());
        let after = service.get_client("ha").expect("the slug is never absent");
        assert_eq!(after.config.password.as_deref(), Some("second"));
        assert_eq!(
            service.ingress_filter_qos("ha", TOPIC).await,
            Some(1),
            "the successor did not carry the predecessor's filters",
        );
        assert!(
            status.delta.mqtt_subscribed.is_empty(),
            "a carried filter is not a move: {:?}",
            status.delta.mqtt_subscribed,
        );
        booted.stop_mqtt();
    }

    /// The other side of reading secrets at every prepare: a `password_file`
    /// that is missing refuses the whole reload in the environment grammar,
    /// whatever the edit was — a fresh boot could not have produced that state
    /// either — and the running client keeps serving on the credential it has.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_missing_password_file_refuses_the_reload_in_the_environment_grammar() {
        const TOPIC: &str = "home/state";
        // The secret is written before the document that names it, because it
        // has to be readable at boot as well as at the reload.
        let tree = Tree::new();
        let password = tree.secret("broker.pw", "first");
        let clients = [("ha", 8883u16, Some(password.as_path()))];
        tree.write(&document_with_clients(&clients, &[("ha", TOPIC)]));
        let components = tempfile::tempdir().expect("a components root");
        install_package(components.path(), &staged_module(&tree));
        let mut booted = boot_with(
            &tree,
            BootFixture {
                components_roots: vec![components.path().to_path_buf()],
                ..BootFixture::default()
            },
        )
        .await;
        let (service, _router) = booted.mqtt.clone();

        std::fs::remove_file(&password).expect("the secret file is removable");
        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Refused);
        assert_eq!(status.refusals.len(), 1, "{:?}", status.refusals);
        assert!(
            status.refusals[0].contains(&password.display().to_string()),
            "the refusal must name the file: {:?}",
            status.refusals,
        );
        assert_eq!(
            service
                .get_client("ha")
                .expect("the running client is untouched")
                .config
                .password
                .as_deref(),
            Some("first"),
        );
    }

    /// One sample of a concurrent observation: whether the process was in the
    /// forbidden state, and which side of the transition the sample landed on.
    struct Sample {
        /// What was wrong, if the sample caught the process in the state
        /// commit's step order forbids.
        violation: Option<String>,
        /// A tag naming the state this sample observed, so [`Probe::stop`] can
        /// hold the run to having straddled the transition it is about.
        witness: &'static str,
    }

    /// What one probe run saw, shared between the sampler and its controller.
    #[derive(Default)]
    struct Observed {
        samples: usize,
        violations: Vec<String>,
        witnesses: std::collections::HashSet<&'static str>,
    }

    /// A concurrent observer of a fact about the running process, sampled as
    /// tightly as the runtime allows while a reload commits.
    ///
    /// What it is for: commit's client steps — `start_added_clients` before the
    /// agent swap, `restart_changed_and_stop_removed_clients` after it — are
    /// statements about instants *inside* one `apply` call, and nothing is
    /// published between them. A sampler on another worker thread is the only
    /// thing outside commit that can see those instants at all.
    ///
    /// A probe cannot invent a violation: every predicate below reads the same
    /// tables the publish path reads. What it must not be allowed to do is pass
    /// while saying nothing, which is what a sampler that was never scheduled
    /// across the transition does. Two gates keep that from reading as a green
    /// run:
    ///
    /// - [`Self::spawn`] does not return until the sampler has taken its first
    ///   sample, so every run is sampling *before* the reload under test
    ///   starts.
    /// - [`Self::stop`] waits, bounded, for the sampler to observe the state
    ///   the transition leaves behind, and fails the run as inconclusive if it
    ///   never does.
    ///
    /// So a green run is one in which the sampler was live on both sides of the
    /// step under test and never saw the forbidden state. It is still no proof
    /// that a sample landed in the interior of the window — nothing outside
    /// commit can prove that — which is why a mutation of the commit order is
    /// caught in a fraction of samples rather than all of them.
    struct Probe {
        stop: Arc<std::sync::atomic::AtomicBool>,
        observed: Arc<std::sync::Mutex<Observed>>,
        task: tokio::task::JoinHandle<()>,
    }

    /// How long [`Probe::stop`] waits for the post-transition witness before
    /// calling the run inconclusive. Generous: the transition has already
    /// happened when `stop` is called, so this bounds only how long the sampler
    /// may stay descheduled.
    const PROBE_WITNESS_WAIT: std::time::Duration = std::time::Duration::from_secs(10);

    impl Probe {
        /// Start sampling, returning once the first sample is in.
        async fn spawn(mut sample: impl FnMut() -> Sample + Send + 'static) -> Self {
            let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let observed = Arc::new(std::sync::Mutex::new(Observed::default()));
            let flag = Arc::clone(&stop);
            let shared = Arc::clone(&observed);
            let task = tokio::spawn(async move {
                while !flag.load(std::sync::atomic::Ordering::Relaxed) {
                    let taken = sample();
                    {
                        let mut observed =
                            shared.lock().expect("the probe's state is not poisoned");
                        observed.samples += 1;
                        if let Some(violation) = taken.violation {
                            observed.violations.push(violation);
                        }
                        observed.witnesses.insert(taken.witness);
                    }
                    tokio::task::yield_now().await;
                }
            });
            let probe = Self {
                stop,
                observed,
                task,
            };
            probe
                .wait_for(PROBE_WITNESS_WAIT, |observed| observed.samples > 0)
                .await
                .expect("the probe never took its first sample, so it observed nothing at all");
            probe
        }

        /// Poll the shared state until `ready`, or give up after `budget`.
        async fn wait_for(
            &self,
            budget: std::time::Duration,
            ready: impl Fn(&Observed) -> bool,
        ) -> Result<(), ()> {
            let deadline = tokio::time::Instant::now() + budget;
            loop {
                if ready(
                    &self
                        .observed
                        .lock()
                        .expect("the probe's state is not poisoned"),
                ) {
                    return Ok(());
                }
                if tokio::time::Instant::now() >= deadline {
                    return Err(());
                }
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        }

        /// Stop sampling and hold the run to account: the sampler must have
        /// observed every state in `straddled` — the last of them is the one
        /// the transition leaves behind, which it may still be about to see —
        /// and none of the forbidden ones.
        async fn stop(self, straddled: &[&'static str]) {
            for state in straddled {
                if self
                    .wait_for(PROBE_WITNESS_WAIT, |observed| {
                        observed.witnesses.contains(state)
                    })
                    .await
                    .is_err()
                {
                    let observed = self
                        .observed
                        .lock()
                        .expect("the probe's state is not poisoned");
                    panic!(
                        "inconclusive: the probe never observed {state:?} in {} samples, so it \
                         was not sampling across the transition it is about and its silence \
                         about the ordering says nothing",
                        observed.samples,
                    );
                }
            }
            self.stop.store(true, std::sync::atomic::Ordering::Relaxed);
            self.task.await.expect("the probe task ran to completion");
            let observed = self
                .observed
                .lock()
                .expect("the probe's state is not poisoned");
            assert!(
                observed.violations.is_empty(),
                "the process was observed in a state commit's step order forbids, {} of {} \
                 samples: {:?}",
                observed.violations.len(),
                observed.samples,
                &observed.violations[..observed.violations.len().min(3)],
            );
        }
    }

    /// The invariant both client steps are placed by: a client is registered
    /// whenever anything authorized to name it is live.
    ///
    /// One predicate for both directions, because it is one invariant. An
    /// arriving client is registered at `10m` and its agent's authority goes
    /// live at the swap that follows, so the forbidden state is never entered;
    /// a departing client's authority stops naming it at that same swap and its
    /// session is taken out afterwards, so it is never entered on the way out
    /// either. A single step on the wrong side of the swap enters it for the
    /// width of everything between.
    async fn authority_probe(booted: &Booted, client: &'static str) -> Probe {
        let apps = booted.messenger.app_table();
        let service = booted.mqtt.0.clone();
        Probe::spawn(move || {
            let authorized = apps
                .load()
                .values()
                .any(|app| app.policy.allows_mqtt_publish(client));
            Sample {
                violation: (authorized && service.get_client(client).is_none()).then(|| {
                    format!("an agent may publish through {client}, which has no session")
                }),
                // The agent swap is the transition: the authority naming the
                // client is live on exactly one side of it, whichever direction
                // this reload moves.
                witness: if authorized {
                    AUTHORITY_LIVE
                } else {
                    AUTHORITY_ABSENT
                },
            }
        })
        .await
    }

    /// The two states `authority_probe` has to see for its silence to mean
    /// anything: the agent table before the swap and after it.
    const AUTHORITY_ABSENT: &str = "no agent may publish through the client";
    const AUTHORITY_LIVE: &str = "an agent may publish through the client";

    /// The two states `presence_probe` has to see: the registry holding the
    /// predecessor's handle, and holding the successor's.
    const PREDECESSOR: &str = "the predecessor's session";
    const SUCCESSOR: &str = "the successor's session";

    /// The other half of the same invariant, for a client the reload restarts: the
    /// successor is swapped into the registry before the predecessor is joined,
    /// so the slug is never absent.
    async fn presence_probe(
        booted: &Booted,
        client: &'static str,
        predecessor: &Arc<brenn_mqtt::state::MqttClientHandle>,
    ) -> Probe {
        let service = booted.mqtt.0.clone();
        let predecessor = Arc::clone(predecessor);
        Probe::spawn(move || match service.get_client(client) {
            None => Sample {
                violation: Some(format!("{client} was absent from the registry")),
                // The registry answered neither handle, which is the forbidden
                // state itself; it witnesses no side of the swap.
                witness: "",
            },
            Some(held) => Sample {
                violation: None,
                witness: if Arc::ptr_eq(&held, &predecessor) {
                    PREDECESSOR
                } else {
                    SUCCESSOR
                },
            },
        })
        .await
    }

    /// The `client "mqtt:<slug>"` clause on the reader's publish ACL — the
    /// authority whose going-live the arriving client's registration is ordered
    /// before.
    fn publishing_through(document: &str, client: &str) -> String {
        document.replace(
            "acl publish [exact reload_requests, exact work];",
            &format!("acl publish [exact reload_requests, exact work, client \"mqtt:{client}\"];"),
        )
    }

    /// Commit registers an arriving client before the swap that makes an
    /// agent's new authority live: an agent that may publish through a client
    /// the registry does not hold reaches `enforce_and_publish`'s per-client
    /// panic, and the window would be every step between.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_added_client_is_registered_before_agents_swap() {
        let one = [("ha", 8883u16, None)];
        let two = [("ha", 8883u16, None), ("spare", 8884u16, None)];
        let tree = Tree::holding(&document_with_clients(&one, &[("ha", "home/state")]));
        let components = tempfile::tempdir().expect("a components root");
        install_package(components.path(), &staged_module(&tree));
        let mut booted = boot_with(
            &tree,
            BootFixture {
                components_roots: vec![components.path().to_path_buf()],
                ..BootFixture::default()
            },
        )
        .await;

        let probe = authority_probe(&booted, "spare").await;
        tree.write(&publishing_through(
            &document_with_clients(&two, &[("ha", "home/state"), ("spare", "home/other")]),
            "spare",
        ));
        install_package(components.path(), &staged_module(&tree));
        booted.driver.reload(TriggerSource::Signal).await;
        probe.stop(&[AUTHORITY_ABSENT, AUTHORITY_LIVE]).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert_eq!(status.delta.mqtt_clients_added, vec!["spare".to_string()]);
        assert!(
            booted.messenger.app_table().load()[READER]
                .policy
                .allows_mqtt_publish("spare"),
            "the authority the ordering is about never went live",
        );
        booted.stop_mqtt();
    }

    /// The other direction, one invariant: commit stops a departing client
    /// after that swap, so the authority naming it is gone before its session
    /// is.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_removed_client_is_stopped_after_agents_swap() {
        let one = [("ha", 8883u16, None)];
        let two = [("ha", 8883u16, None), ("spare", 8884u16, None)];
        let tree = Tree::holding(&publishing_through(
            &document_with_clients(&two, &[("ha", "home/state"), ("spare", "home/other")]),
            "spare",
        ));
        let components = tempfile::tempdir().expect("a components root");
        install_package(components.path(), &staged_module(&tree));
        let mut booted = boot_with(
            &tree,
            BootFixture {
                components_roots: vec![components.path().to_path_buf()],
                ..BootFixture::default()
            },
        )
        .await;
        assert!(
            booted.messenger.app_table().load()[READER]
                .policy
                .allows_mqtt_publish("spare"),
            "the authority the ordering is about was never live",
        );

        let probe = authority_probe(&booted, "spare").await;
        tree.write(&document_with_clients(&one, &[("ha", "home/state")]));
        install_package(components.path(), &staged_module(&tree));
        booted.driver.reload(TriggerSource::Signal).await;
        probe.stop(&[AUTHORITY_LIVE, AUTHORITY_ABSENT]).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert_eq!(status.delta.mqtt_clients_removed, vec!["spare".to_string()]);
        assert!(booted.mqtt.0.get_client("spare").is_none());
    }

    /// A restarted client keeps its slug in the registry throughout: the
    /// successor handle is swapped in before the predecessor's supervisor is
    /// joined, and it carries the filters the predecessor held so no `mqtt:`
    /// binding has to be re-planned.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_changed_client_is_restarted_with_its_filters_carried() {
        const TOPIC: &str = "home/state";
        let tree = Tree::holding(&document_with_clients(
            &[("ha", 8883u16, None)],
            &[("ha", TOPIC)],
        ));
        let components = tempfile::tempdir().expect("a components root");
        install_package(components.path(), &staged_module(&tree));
        let mut booted = boot_with(
            &tree,
            BootFixture {
                components_roots: vec![components.path().to_path_buf()],
                ..BootFixture::default()
            },
        )
        .await;
        let (service, _router) = booted.mqtt.clone();
        let before = service.get_client("ha").expect("a declared client");

        let probe = presence_probe(&booted, "ha", &before).await;
        tree.write(&document_with_clients(
            &[("ha", 8885u16, None)],
            &[("ha", TOPIC)],
        ));
        booted.driver.reload(TriggerSource::Signal).await;
        probe.stop(&[PREDECESSOR, SUCCESSOR]).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert_eq!(status.delta.mqtt_clients_changed, vec!["ha".to_string()]);
        let after = service.get_client("ha").expect("the slug is never absent");
        assert_eq!(after.config.identity.port, 8885);
        assert!(
            !Arc::ptr_eq(&before, &after),
            "a changed client is a new handle, not an edited one",
        );
        assert_eq!(
            service.ingress_filter_qos("ha", TOPIC).await,
            Some(1),
            "the successor did not carry the predecessor's filters",
        );
        assert!(
            status.delta.mqtt_subscribed.is_empty(),
            "a carried filter is not a move: {:?}",
            status.delta.mqtt_subscribed,
        );
        booted.stop_mqtt();
    }

    /// A filter arriving on a client this same reload restarted is reported
    /// `mqtt_deferred`: the successor's supervisor was spawned in this walk and
    /// is still connecting when the move is asserted, so the SUBSCRIBE goes out
    /// on its first connect.
    ///
    /// `mqtt_subscribed` is every move this reload made; `mqtt_deferred` is
    /// the subset the broker has not taken yet. The claim the case can make is
    /// that the move is on the deferred list and on neither the failed nor the
    /// withdrawn one.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_added_or_changed_clients_filter_moves_are_reported_deferred() {
        const KEPT: &str = "home/state";
        const ARRIVED: &str = "home/other";
        let address = format!("mqtt:spare:{ARRIVED}");
        let clients = |port: u16| [("ha", 8883u16, None), ("spare", port, None)];
        let tree = Tree::holding(&document_with_clients(&clients(8884), &[("ha", KEPT)]));
        let components = tempfile::tempdir().expect("a components root");
        install_package(components.path(), &staged_module(&tree));
        let mut booted = boot_with(
            &tree,
            BootFixture {
                components_roots: vec![components.path().to_path_buf()],
                ..BootFixture::default()
            },
        )
        .await;

        // One reload, two moves on one client: its broker coordinates change,
        // and the first binding through it arrives.
        tree.write(&document_with_clients(
            &clients(8886),
            &[("ha", KEPT), ("spare", ARRIVED)],
        ));
        install_package(components.path(), &staged_module(&tree));
        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert_eq!(status.delta.mqtt_clients_changed, vec!["spare".to_string()]);
        assert_eq!(status.delta.mqtt_subscribed, vec![address.clone()]);
        assert_eq!(
            status.delta.mqtt_deferred,
            vec![address.clone()],
            "the move on a client restarted in this walk is deferred to its first connect",
        );
        assert!(status.delta.mqtt_failed.is_empty());
        assert!(status.delta.mqtt_unsubscribed.is_empty());
        assert_eq!(
            booted.mqtt.0.ingress_filter_qos("spare", ARRIVED).await,
            Some(1),
            "the deferred filter is registered on the successor all the same",
        );
        booted.stop_mqtt();
    }

    /// A restart and a withdrawal in one reload: the successor inherits the
    /// filter the document still binds and not the one it dropped.
    ///
    /// This is what the step order is for. The outgoing step runs before the
    /// restart, so the predecessor's set is pruned before the successor
    /// inherits it; inherit first, or restart before the prune, and the
    /// successor holds a filter the document no longer binds and re-asserts it
    /// at the broker on every connect, on a route this same reload removed —
    /// every delivery a zero-match drop, with no status field and no log line
    /// to say so.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_restarted_client_does_not_inherit_a_withdrawn_filter() {
        const KEPT: &str = "home/state";
        const DROPPED: &str = "home/other";
        let dropped_address = format!("mqtt:ha:{DROPPED}");
        let tree = Tree::new();
        let password = tree.secret("broker.pw", "first");
        let before = [("ha", 8883u16, Some(password.as_path()))];
        let after = [("ha", 8886u16, Some(password.as_path()))];
        tree.write(&document_with_clients(
            &before,
            &[("ha", KEPT), ("ha", DROPPED)],
        ));
        let components = tempfile::tempdir().expect("a components root");
        install_package(components.path(), &staged_module(&tree));
        let mut booted = boot_with(
            &tree,
            BootFixture {
                components_roots: vec![components.path().to_path_buf()],
                ..BootFixture::default()
            },
        )
        .await;
        let (service, router) = booted.mqtt.clone();
        assert_eq!(service.ingress_filter_qos("ha", DROPPED).await, Some(1));
        let dropped_uuid = booted
            .messenger
            .directory()
            .resolve(&dropped_address)
            .expect("the entry is in the directory")
            .uuid;

        // One reload, every move at once: the broker coordinates move, the
        // credential rotates, and one of the two bindings goes.
        std::fs::write(&password, "second").expect("the secret file is writable");
        tree.write(&document_with_clients(&after, &[("ha", KEPT)]));
        install_package(components.path(), &staged_module(&tree));
        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert_eq!(status.delta.mqtt_clients_changed, vec!["ha".to_string()]);
        assert_eq!(
            status.delta.mqtt_unsubscribed,
            vec![dropped_address.clone()]
        );
        let after_handle = service.get_client("ha").expect("the slug is never absent");
        assert_eq!(after_handle.config.password.as_deref(), Some("second"));
        assert_eq!(
            service.ingress_filter_qos("ha", KEPT).await,
            Some(1),
            "the surviving binding's filter is inherited",
        );
        assert_eq!(
            service.ingress_filter_qos("ha", DROPPED).await,
            None,
            "and the withdrawn one is not: the successor would re-assert it at the broker \
             forever, on a route this reload removed",
        );
        assert!(
            !router.route_uuids().contains(&dropped_uuid),
            "{dropped_address} still has a route",
        );
        assert_eq!(after_handle.config.identity.port, 8886);
        booted.stop_mqtt();
    }

    /// A dormant durable row on a channel a removed client's binding took with
    /// it is refused.
    ///
    /// The rule is what keeps the removal honest. A stopping client contributes
    /// no filter move at all — its filters leave with its session — so nothing
    /// else in the walk would notice that a durable subscription is left
    /// naming a broker session this process will not have. A fresh boot of the
    /// candidate reconstructs the channel from the store and holds the row
    /// dormant against it; the reload cannot reproduce that reconstruction, so
    /// it refuses and says which pair it could not follow.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_dormant_row_on_a_removed_clients_channel_is_refused() {
        const ADDRESS: &str = "mqtt:spare:home/other";
        let one = [("ha", 8883u16, None)];
        let two = [("ha", 8883u16, None), ("spare", 8884u16, None)];
        let tree = Tree::holding(&document_with_clients(
            &two,
            &[("ha", "home/state"), ("spare", "home/other")],
        ));
        let components = tempfile::tempdir().expect("a components root");
        install_package(components.path(), &staged_module(&tree));
        let mut booted = boot_with(
            &tree,
            BootFixture {
                components_roots: vec![components.path().to_path_buf()],
                ..BootFixture::default()
            },
        )
        .await;
        // Dormant: the row is stored and is not folded onto the entry, which is
        // where an ACL narrowed under a runtime `MessageSubscribe` leaves it.
        insert_dynamic_mqtt_row(&booted, ADDRESS, false, 1).await;

        tree.write(&document_with_clients(&one, &[("ha", "home/state")]));
        install_package(components.path(), &staged_module(&tree));
        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Refused, "{:?}", status.delta);
        assert!(
            status
                .refusals
                .iter()
                .any(|refusal| refusal.contains(ADDRESS) && refusal.contains(READER)),
            "the refusal names the pair it cannot follow: {:?}",
            status.refusals,
        );
        assert_eq!(
            booted.mqtt.0.client_slugs(),
            vec!["ha".to_string(), "spare".to_string()],
            "a refusal changes nothing: the session the candidate would have stopped is up",
        );
    }

    /// Three clients moving in one reload, one per list: the commit walk runs a
    /// task per client against one registry, and every existing case moves
    /// exactly one, where the walk is indistinguishable from a serial call.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn several_clients_move_in_one_reload() {
        const SURVIVOR_TOPIC: &str = "home/state";
        let tree = Tree::new();
        let password = tree.secret("broker.pw", "first");
        let before = [
            ("ha", 8883u16, None),
            ("attic", 8884u16, Some(password.as_path())),
            ("shed", 8885u16, None),
        ];
        // `ha` survives untouched, `attic`'s credential rotates, `shed` goes
        // and `spare` arrives.
        let after = [
            ("ha", 8883u16, None),
            ("attic", 8884u16, Some(password.as_path())),
            ("spare", 8886u16, None),
        ];
        tree.write(&document_with_clients(&before, &[("ha", SURVIVOR_TOPIC)]));
        let components = tempfile::tempdir().expect("a components root");
        install_package(components.path(), &staged_module(&tree));
        let mut booted = boot_with(
            &tree,
            BootFixture {
                components_roots: vec![components.path().to_path_buf()],
                ..BootFixture::default()
            },
        )
        .await;
        let (service, _router) = booted.mqtt.clone();
        assert_eq!(
            service.client_slugs(),
            vec!["attic".to_string(), "ha".to_string(), "shed".to_string()],
        );

        std::fs::write(&password, "second").expect("the secret file is writable");
        tree.write(&document_with_clients(&after, &[("ha", SURVIVOR_TOPIC)]));
        install_package(components.path(), &staged_module(&tree));
        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert_eq!(status.delta.mqtt_clients_added, vec!["spare".to_string()]);
        assert_eq!(status.delta.mqtt_clients_removed, vec!["shed".to_string()]);
        assert_eq!(status.delta.mqtt_clients_changed, vec!["attic".to_string()]);
        assert_eq!(
            service.client_slugs(),
            vec!["attic".to_string(), "ha".to_string(), "spare".to_string()],
            "the registry is exactly the candidate's set",
        );
        assert_eq!(
            service
                .get_client("attic")
                .expect("the restarted slug is never absent")
                .config
                .password
                .as_deref(),
            Some("second"),
        );
        assert_eq!(
            service.ingress_filter_qos("ha", SURVIVOR_TOPIC).await,
            Some(1),
            "the untouched client's filter is where it was",
        );
        booted.stop_mqtt();
    }

    /// Shutdown reads the live registry, so a client a reload added is
    /// disconnected cleanly on SIGTERM: `stop_all` signals its supervisor and
    /// the join returns.
    #[tokio::test(flavor = "multi_thread")]
    async fn stop_all_disconnects_a_client_added_at_reload() {
        let one = [("ha", 8883u16, None)];
        let two = [("ha", 8883u16, None), ("spare", 8884u16, None)];
        let tree = Tree::holding(&document_with_clients(&one, &[("ha", "home/state")]));
        let components = tempfile::tempdir().expect("a components root");
        install_package(components.path(), &staged_module(&tree));
        let mut booted = boot_with(
            &tree,
            BootFixture {
                components_roots: vec![components.path().to_path_buf()],
                ..BootFixture::default()
            },
        )
        .await;

        tree.write(&document_with_clients(
            &two,
            &[("ha", "home/state"), ("spare", "home/other")],
        ));
        install_package(components.path(), &staged_module(&tree));
        booted.driver.reload(TriggerSource::Signal).await;
        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);

        let service = booted.mqtt.0.clone();
        let added = service.get_client("spare").expect("the reload added it");
        assert_eq!(
            service.stop_all(),
            2,
            "the shutdown path signals every registered supervisor, the arriving one included",
        );
        // The supervisor the commit spawned exits on the signal shutdown just
        // sent: an added client whose task outlived `stop_all` would hold its
        // broker session open past the process's last word.
        added.stop_and_join().await;
    }

    /// Ingress convergence against a live service and router: a second `mqtt:`
    /// entry arrives, its filter joins the client's reconnect-survival set and
    /// its route joins the table; it leaves, and both go — while the first
    /// entry's filter and route are untouched, which is what keeps
    /// `unsubscribe_filter`'s contract. The client is registered and
    /// disconnected, so each SUBSCRIBE is deferred to the next connect, which
    /// is a success and is what the status body's `deferred` list is for.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_mqtt_binding_converges_its_filter_and_its_route() {
        const KEPT: &str = "home/state";
        const MOVED: &str = "home/power";
        let address = format!("mqtt:ha:{MOVED}");
        let tree = Tree::holding(&document_with_an_mqtt_consumer(&[KEPT]));
        let components = tempfile::tempdir().expect("a components root");
        install_package(components.path(), &staged_module(&tree));
        let mut booted = boot_with(
            &tree,
            BootFixture {
                components_roots: vec![components.path().to_path_buf()],
                ..BootFixture::default()
            },
        )
        .await;
        let (service, router) = booted.mqtt.clone();
        let kept_uuid = booted
            .messenger
            .directory()
            .resolve(&format!("mqtt:ha:{KEPT}"))
            .expect("the booted entry is in the directory")
            .uuid;
        assert_eq!(router.route_uuids(), vec![kept_uuid]);
        assert_eq!(service.ingress_filter_qos("ha", MOVED).await, None);

        tree.write(&document_with_an_mqtt_consumer(&[KEPT, MOVED]));
        install_package(components.path(), &staged_module(&tree));
        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert!(
            status.delta.channels_added.contains(&address),
            "{:?}",
            status.delta.channels_added,
        );
        assert_eq!(status.delta.mqtt_subscribed, vec![address.clone()]);
        assert!(status.delta.mqtt_unsubscribed.is_empty());
        assert_eq!(
            status.delta.mqtt_deferred,
            vec![address.clone()],
            "a registered but disconnected client defers every SUBSCRIBE",
        );
        assert_eq!(
            service.ingress_filter_qos("ha", MOVED).await,
            Some(1),
            "the arriving filter is not in the reconnect-survival set",
        );
        let moved_uuid = booted
            .messenger
            .directory()
            .resolve(&address)
            .expect("the entry is in the directory")
            .uuid;
        assert_eq!(router.route_uuids(), vec![kept_uuid, moved_uuid]);

        // And back: the binding goes, and with it the filter and the route.
        tree.write(&document_with_an_mqtt_consumer(&[KEPT]));
        install_package(components.path(), &staged_module(&tree));
        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert_eq!(status.delta.mqtt_unsubscribed, vec![address.clone()]);
        assert!(status.delta.mqtt_subscribed.is_empty());
        assert_eq!(service.ingress_filter_qos("ha", MOVED).await, None);
        assert_eq!(
            service.ingress_filter_qos("ha", KEPT).await,
            Some(1),
            "the surviving filter was unsubscribed",
        );
        assert_eq!(router.route_uuids(), vec![kept_uuid]);
        assert!(booted.messenger.directory().resolve(&address).is_none());
    }

    /// A filter a dynamic subscribe minted, across a reload that moves another
    /// filter on the same client.
    ///
    /// This is the case the whole plan-only diff rests on: a purely dynamic
    /// filter is in neither plan and its route is keyed by a uuid no plan entry
    /// carries, so nothing in the walk can reach either. Driven through the two
    /// steps `mqtt_subscribe` composes — the transport-blind core that mints
    /// the channel and folds the subscriber, then the activation that
    /// SUBSCRIBEs and adds the route — because a delta computed off the live
    /// router table instead of the plan would pass every unit test and take
    /// this filter down.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_dynamically_minted_filter_and_route_survive_a_reload() {
        const KEPT: &str = "home/state";
        const ARRIVING: &str = "home/power";
        const DYNAMIC: &str = "home/dynamic";
        let tree = Tree::holding(&document_with_an_mqtt_consumer(&[KEPT]));
        let components = tempfile::tempdir().expect("a components root");
        install_package(components.path(), &staged_module(&tree));
        let mut booted = boot_with(
            &tree,
            BootFixture {
                components_roots: vec![components.path().to_path_buf()],
                ..BootFixture::default()
            },
        )
        .await;
        let (service, router) = booted.mqtt.clone();

        let address = format!("mqtt:ha:{DYNAMIC}");
        booted
            .messenger
            .subscribe_dynamic(
                READER,
                &address,
                brenn_messaging::subscribe::DynamicSubscribeParams {
                    push_depth: brenn_lib::messaging::config::Depth::Bounded(0),
                    retain_depth: brenn_lib::messaging::config::Depth::Bounded(1),
                    noise: None,
                    wake_min: None,
                    qos: Some(1),
                },
            )
            .await
            .expect("a dynamic subscribe on a filter the document does not declare");
        let dynamic = brenn_lib::mqtt::config::ResolvedMqttIngressChannel {
            channel_uuid: brenn_lib::messaging::mqtt_channel_uuid_from_address(&address),
            channel_address: address.clone(),
            client_slug: "ha".to_string(),
            topic: DYNAMIC.to_string(),
            qos: 1,
            urgency: brenn_lib::messaging::Urgency::Normal,
        };
        assert!(
            router.add_route(brenn_server::mqtt_router::IngressRoute::from(&dynamic)),
            "the dynamic route is new to the table",
        );
        service
            .subscribe_filter("ha", DYNAMIC.to_string(), 1)
            .await
            .expect("the client has a session");

        // An unrelated reload: another filter on the same client arrives.
        tree.write(&document_with_an_mqtt_consumer(&[KEPT, ARRIVING]));
        install_package(components.path(), &staged_module(&tree));
        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert_eq!(
            status.delta.mqtt_subscribed,
            vec![format!("mqtt:ha:{ARRIVING}")],
        );
        assert!(
            status.delta.mqtt_unsubscribed.is_empty(),
            "{:?}",
            status.delta.mqtt_unsubscribed,
        );
        assert_eq!(
            service.ingress_filter_qos("ha", DYNAMIC).await,
            Some(1),
            "the dynamic filter left the reconnect-survival set",
        );
        assert!(
            router.route_uuids().contains(&dynamic.channel_uuid),
            "the dynamic route left the table",
        );
    }

    /// The consumer half of prepare, end to end: the candidate's records are
    /// read off the roots and every arriving consumer is instantiated, so that
    /// commit has no artifact left to be refused by. Nothing else here declares
    /// a `[[wasm_consumer]]`, and a `records_of` that returned an empty map
    /// would leave every other test green while making bundle upgrades
    /// invisible.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_arriving_consumer_is_verified_and_instantiated_by_prepare() {
        let tree = Tree::holding(&document(""));
        let components = tempfile::tempdir().expect("a components root");
        let booted = boot(&tree, vec![components.path().to_path_buf()]).await;

        let text = document_with_a_consumer();
        tree.write(&text);
        install_package(components.path(), &staged_module(&tree));

        let dynamic = booted.driver.dynamic_snapshot().await;
        let ready = match booted.driver.prepare(TriggerSource::Signal, &dynamic) {
            Prepared::Ready(ready) => ready,
            other => panic!("the candidate is applicable: {}", outcome_of(&other)),
        };
        assert_eq!(ready.delta.consumers_added, vec!["sifter".to_string()]);
        assert_eq!(
            ready
                .loaded
                .iter()
                .map(|(slug, _)| slug.as_str())
                .collect::<Vec<_>>(),
            vec!["sifter"],
            "the arriving consumer is instantiated before anything commits",
        );
        // Nothing started: the component is loaded and its store, had it one,
        // is not open.
        assert!(booted.messenger.directory().resolve("brenn:work").is_some());
    }

    /// The same document with nothing installed under the root: the package
    /// resolution refuses, in boot's words, and the reload is a refusal rather
    /// than an unwind.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_consumer_whose_package_no_root_holds_is_refused() {
        let tree = Tree::holding(&document(""));
        let components = tempfile::tempdir().expect("a components root");
        let mut booted = boot(&tree, vec![components.path().to_path_buf()]).await;

        tree.write(&document_with_a_consumer());

        assert!(
            booted
                .driver
                .prepare_and_report(TriggerSource::Bus)
                .await
                .is_none()
        );
        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Refused);
        assert_eq!(status.refusals.len(), 1, "{:?}", status.refusals);
        assert!(
            status.refusals[0].contains(PACKAGED_MODULE) && status.refusals[0].contains("sifter"),
            "{:?}",
            status.refusals
        );
    }

    /// A candidate that configures no messaging at all is refused: a process
    /// running the reload facility has messaging by construction.
    ///
    /// Booted agentless and verdict read off `prepare`: an agent alone
    /// configures messaging, so the reader agent other fixtures use would
    /// prevent the candidate from reaching the `None` arm.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_candidate_that_configures_no_messaging_is_refused() {
        let tree = Tree::holding(&document_agentless(""));
        let booted = boot(&tree, Vec::new()).await;

        tree.write("// a document that configures nothing at all\n");
        let dynamic = booted.driver.dynamic_snapshot().await;
        let Prepared::Refused { refusals, .. } =
            booted.driver.prepare(TriggerSource::Signal, &dynamic)
        else {
            panic!("a candidate that configures no messaging must be refused");
        };
        assert_eq!(refusals.len(), 1, "{refusals:?}");
        assert!(
            refusals[0].contains("configures no messaging")
                && refusals[0].ends_with(super::super::NEEDS_RESTART),
            "{refusals:?}",
        );
    }

    // ── The commit phase ──────────────────────────────────────────────────

    /// The `messaging_channels` row for `uuid`, as `Some(description)` when the
    /// row is there. Read straight out of the table because the two questions
    /// this answers — is the row still there, and does its description column
    /// carry the new text — are about the row rather than about the directory.
    pub(crate) async fn channel_row(
        messenger: &Messenger,
        uuid: uuid::Uuid,
    ) -> Option<Option<String>> {
        let conn = messenger.db().lock().await;
        conn.query_row(
            "SELECT description FROM messaging_channels WHERE uuid = ?1",
            rusqlite::params![uuid.as_bytes().to_vec()],
            |row| row.get::<_, Option<String>>(0),
        )
        .optional()
        .expect("the channels table is readable")
    }

    /// `subscriber`'s cursor on `address`, or `None` where it holds none.
    pub(crate) async fn cursor_of(
        messenger: &Messenger,
        address: &str,
        subscriber: &brenn_lib::messaging::ParticipantId,
    ) -> Option<brenn_messaging_store::db::SubscriberCursorRow> {
        let entry = messenger
            .directory()
            .resolve(address)
            .expect("the channel is declared");
        let conn = messenger.db().lock().await;
        brenn_messaging_store::db::load_subscriber_cursor(&conn, entry.uuid, subscriber)
    }

    /// Addresses of live directory entries that carry `kind`.
    pub(crate) fn subscribed_anywhere(
        messenger: &Messenger,
        kind: &SubscriberEntryKind,
    ) -> Vec<String> {
        messenger
            .directory()
            .list()
            .iter()
            .filter(|entry| {
                entry
                    .subscribers
                    .iter()
                    .any(|sub| sub.kind.same_principal(kind))
            })
            .map(|entry| entry.address.clone())
            .collect()
    }

    /// An entry's subscribers, each through `Debug`, sorted.
    ///
    /// `Debug` rather than a field list: what a reload case asks is whether a
    /// subscriber came through untouched, and a comparison that named the
    /// fields would stop seeing a field added later.
    pub(crate) fn subscriber_debug_lines(
        entry: &brenn_lib::messaging::ChannelEntry,
    ) -> Vec<String> {
        let mut lines: Vec<String> = entry
            .subscribers
            .iter()
            .map(|subscriber| format!("{subscriber:?}"))
            .collect();
        lines.sort();
        lines
    }

    /// [`subscriber_debug_lines`] for the entry `address` resolves to.
    pub(crate) fn subscriber_lines(messenger: &Messenger, address: &str) -> Vec<String> {
        subscriber_debug_lines(
            &messenger
                .directory()
                .resolve(address)
                .expect("the channel is declared"),
        )
    }

    /// Every message body on `address`, oldest first.
    ///
    /// Read out of the table rather than through `Messenger::query`, because
    /// the question is what a consumer's activation published and not what some
    /// app is permitted to read.
    pub(crate) async fn bodies_on(messenger: &Messenger, address: &str) -> Vec<String> {
        let uuid = messenger
            .directory()
            .resolve(address)
            .expect("the channel is declared")
            .uuid;
        let conn = messenger.db().lock().await;
        conn.prepare("SELECT body FROM messaging_messages WHERE channel_uuid = ?1 ORDER BY id")
            .expect("the messages table is readable")
            .query_map(rusqlite::params![uuid.as_bytes().to_vec()], |row| {
                row.get::<_, String>(0)
            })
            .expect("the message rows read")
            .map(|row| row.expect("a message row"))
            .collect()
    }

    impl Booted {
        /// Signal every live supervisor to disconnect, for a case that booted
        /// against a real broker.
        ///
        /// Teardown rather than assertion: the broker is killed a moment later
        /// either way, and a session that leaves with a DISCONNECT keeps the
        /// broker's log free of the abnormal-close lines a failing case has to
        /// read past. Reads the live registry, so a client a reload under test
        /// added is signalled too.
        pub(crate) fn stop_mqtt(&self) {
            self.mqtt.0.stop_all();
        }

        /// Poll until `address` holds at least `wanted` messages, and answer
        /// with them.
        ///
        /// A message an activation publishes arrives through this process's
        /// dispatcher, so the wait watches it: a panic there is what the wait
        /// is really about, and reporting the empty channel instead would read
        /// as a slow test.
        pub(crate) async fn bodies_until(&mut self, address: &str, wanted: usize) -> Vec<String> {
            let messenger = Arc::clone(&self.messenger);
            poll_until(
                &format!("messages on {address}"),
                wanted,
                self.dispatcher.as_mut(),
                async || bodies_on(&messenger, address).await,
            )
            .await
        }
    }

    /// The motivating shape, committed: a document grows a channel and a
    /// consumer, and after the reload the consumer is wired, running, and
    /// draining what the channel already held.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_added_channel_and_consumer_are_committed_and_the_consumer_runs() {
        let tree = Tree::holding(&document(""));
        let components = tempfile::tempdir().expect("a components root");
        let mut booted = boot(&tree, vec![components.path().to_path_buf()]).await;

        tree.write(&document_with_a_consumer());
        install_package(components.path(), &staged_module(&tree));
        // A message the channel already holds. The arriving consumer's position
        // is primed behind the retained tail, exactly as at boot, so this is
        // what its mount activation drains — which is how the test sees that the
        // task is really running rather than merely registered.
        let work = booted
            .messenger
            .directory()
            .resolve("brenn:work")
            .expect("the work channel is declared");
        brenn_messaging::testutils::insert_bus_message(
            &booted.messenger,
            &work,
            "a job",
            brenn_lib::messaging::ChannelScheme::Brenn,
        )
        .await;

        let candidate_sha = tree.load().document_sha256;
        booted.driver.reload(TriggerSource::Bus).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert_eq!(status.generation, 1, "an applied reload is a generation");
        assert_eq!(status.document_sha256.as_deref(), Some(&*candidate_sha));
        // The process now says it projects the text on disk, which is the whole
        // question the retained body exists to answer.
        assert_eq!(status.running_document_sha256, candidate_sha);
        assert_eq!(status.delta.consumers_added, vec!["sifter".to_string()]);
        assert!(
            status
                .delta
                .channels_added
                .contains(&"brenn:sink".to_string()),
            "{:?}",
            status.delta.channels_added
        );
        assert_eq!(
            booted.driver.baseline().document.document_sha256,
            candidate_sha
        );

        let kind = SubscriberEntryKind::Wasm("sifter".to_string());
        assert_eq!(
            subscribed_anywhere(&booted.messenger, &kind),
            ["brenn:work"]
        );
        assert!(booted.messenger.subscriber_registration(&kind).is_some());
        assert!(booted.router.has_delivery_binding(&kind));
        assert!(booted.driver.registry().contains_key("sifter"));

        // The new channel is in the directory and has a durable row of its own.
        let sink = booted
            .messenger
            .directory()
            .resolve("brenn:sink")
            .expect("the added channel is live");
        assert!(channel_row(&booted.messenger, sink.uuid).await.is_some());

        let participant = brenn_lib::messaging::ParticipantId::for_wasm("sifter");
        assert!(
            brenn_wasm_dispatch::tests::wait_pending_empty(
                &booted.messenger,
                &participant,
                std::time::Duration::from_secs(10),
            )
            .await,
            "the started consumer drains the backlog its attach primed it behind",
        );
    }

    /// The shape the slice exists for: an agent already reads a channel, and an
    /// automation arrives on it and later leaves.
    ///
    /// What no other case here covers is the *foreign* subscriber: the channel
    /// already carries an app's entry, so adding and removing a consumer must
    /// preserve it rather than replace or clear the subscriber list. Both
    /// paths are live in production and both look identical on a channel nobody
    /// else reads.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_consumer_joins_and_leaves_a_channel_an_app_already_reads() {
        let components = tempfile::tempdir().expect("a components root");
        let tree = Tree::holding(&document_subscribing("", &["work"]));
        let mut booted = boot_with(
            &tree,
            BootFixture {
                components_roots: vec![components.path().to_path_buf()],
                dispatcher: true,
                ..BootFixture::default()
            },
        )
        .await;
        seat_a_conversation(&booted.db, 1).await;
        let app = SubscriberEntryKind::App(READER.to_string());
        let consumer = SubscriberEntryKind::Wasm("prober".to_string());
        // The agent's cursor on the shared channel, distinct from its
        // subscriber entry.  Seated here so that a reload path that
        // incorrectly resets foreign cursors fails, rather than passing
        // because no case ever places one on a channel a reload touches.
        let reader_cursor = brenn_lib::messaging::ParticipantId::for_conversation(1);
        booted
            .messenger
            .attach_subscriber(
                "brenn:work",
                READER,
                &reader_cursor,
                brenn_lib::messaging::config::Depth::Bounded(4),
            )
            .await;
        let seated = cursor_of(&booted.messenger, "brenn:work", &reader_cursor)
            .await
            .expect("the agent holds a position on the shared channel");
        let before = subscriber_lines(&booted.messenger, "brenn:work");
        assert_eq!(
            subscribed_anywhere(&booted.messenger, &app),
            ["brenn:work"],
            "the fixture seats the foreign subscriber this case is about",
        );

        // The consumer arrives on the channel the app already reads.
        tree.write(&configured_consumer_document("v1", &["work"]));
        install_package_from(
            components.path(),
            &staged_module(&tree),
            "brenn_processor_config.wasm",
        );
        booted.driver.reload(TriggerSource::Bus).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        for moved in [
            &status.delta.channels_added,
            &status.delta.channels_removed,
            &status.delta.channels_changed,
        ] {
            assert!(
                !moved.contains(&"brenn:work".to_string()),
                "the shared channel is unchanged: {moved:?}",
            );
        }
        assert_eq!(status.delta.consumers_added, vec!["prober".to_string()]);

        let joined = subscriber_lines(&booted.messenger, "brenn:work");
        assert_eq!(joined.len(), 2, "{joined:?}");
        assert!(
            joined.iter().any(|line| line == &before[0]),
            "the app's subscriber entry came through untouched: {joined:?}",
        );
        assert_eq!(
            subscribed_anywhere(&booted.messenger, &consumer),
            ["brenn:work"]
        );
        assert_eq!(
            cursor_of(&booted.messenger, "brenn:work", &reader_cursor).await,
            Some(seated.clone()),
            "the arriving consumer's attach left the agent's position where it was",
        );

        // And the arrival is a running consumer, not merely a wired one: a
        // publish on the shared channel reaches its activation, whose output
        // lands on the channel the candidate declared for it.
        probe(&booted.messenger).await;
        assert_eq!(
            booted.bodies_until("brenn:sink", 1).await,
            vec!["v1".to_string()],
            "the activation publishes on the output channel",
        );

        // The other direction: the automation leaves and the agent's
        // subscription is exactly where it was.
        tree.write(&document_subscribing("", &["work"]));
        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert_eq!(status.delta.consumers_removed, vec!["prober".to_string()]);
        assert_eq!(subscriber_lines(&booted.messenger, "brenn:work"), before);
        assert!(booted.messenger.directory().resolve("brenn:sink").is_none());
        assert_eq!(
            cursor_of(&booted.messenger, "brenn:work", &reader_cursor).await,
            Some(seated),
            "and the departing consumer's detach took only its own position",
        );
    }

    /// A `config`-map-only edit, observed by the activation.
    ///
    /// The delta half of this holds by construction — `config` is a field of
    /// `ResolvedWasmConsumer`'s derived `PartialEq`. What no unit test can see
    /// is the other half: that the instance the commit starts is the one built
    /// from the *candidate*, so the guest reads the new value rather than the
    /// one its predecessor was loaded with.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_config_only_edit_restarts_the_consumer_on_the_new_value() {
        let components = tempfile::tempdir().expect("a components root");
        let tree = Tree::holding(&document_with_a_configured_consumer("v1"));
        install_package_from(
            components.path(),
            &staged_module(&tree),
            "brenn_processor_config.wasm",
        );
        let mut booted = boot_with(
            &tree,
            BootFixture {
                components_roots: vec![components.path().to_path_buf()],
                dispatcher: true,
                ..BootFixture::default()
            },
        )
        .await;
        seat_a_conversation(&booted.db, 1).await;
        let kind = SubscriberEntryKind::Wasm("prober".to_string());

        probe(&booted.messenger).await;
        assert_eq!(
            booted.bodies_until("brenn:sink", 1).await,
            vec!["v1".to_string()],
            "the booted instance answers with the map its document carries",
        );

        // The map, and nothing else. The packaged half is untouched, so the
        // class and the spec hash the instance is bound to stand still.
        tree.write(&document_with_a_configured_consumer("v2"));
        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert_eq!(status.delta.consumers_changed, vec!["prober".to_string()]);
        assert!(status.delta.channels_added.is_empty());
        assert!(status.delta.channels_removed.is_empty());
        assert!(status.delta.channels_changed.is_empty());

        // Live again rather than a tombstone: the replacement re-registered the
        // key its predecessor's retirement retired.
        assert!(booted.router.has_delivery_binding(&kind));
        assert!(!booted.router.delivery_binding_retired(&kind));

        probe(&booted.messenger).await;
        assert_eq!(
            booted.bodies_until("brenn:sink", 2).await,
            vec!["v1".to_string(), "v2".to_string()],
            "the replacement answers with the candidate's map",
        );

        // Exactly two, once the replacement owes nothing: a reload that
        // re-primed the position instead of keeping it would answer the first
        // directive over again with the new map, and the poll above — which
        // returns at the count it wanted — would have read that replay as the
        // second answer and stopped one short of seeing the third.
        let participant = brenn_lib::messaging::ParticipantId::for_wasm("prober");
        assert!(
            brenn_wasm_dispatch::tests::wait_pending_empty(
                &booted.messenger,
                &participant,
                std::time::Duration::from_secs(10),
            )
            .await,
            "the replacement drains what it is owed",
        );
        assert_eq!(
            bodies_on(&booted.messenger, "brenn:sink").await,
            vec!["v1".to_string(), "v2".to_string()],
            "the replacement answered the new directive and did not replay the old one",
        );
    }

    /// Ask the configured consumer for `test-key`, as its guest reads the
    /// directive: the key travels in the message, so the document's key and the
    /// directive's key are the same string.
    async fn probe(messenger: &Arc<Messenger>) {
        let published = messenger
            .publish(
                brenn_messaging::PublishOrigin::Conversation { id: 1 },
                READER,
                "brenn:work",
                r#"{"cmd":"get","key":"test-key"}"#,
                brenn_messaging::Urgency::Normal,
                None,
                None,
                None,
            )
            .await;
        assert!(
            matches!(published, brenn_messaging::PublishResult::Ok { .. }),
            "{published:?}"
        );
    }

    /// The other direction: a consumer leaves, and every table that named it
    /// stops naming it — entries, registration, binding, position. The channel
    /// only it read leaves with it, and the row that channel had stays, because
    /// that is what a restart does with a row the config no longer names.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_removed_consumer_leaves_no_wiring_behind() {
        let components = tempfile::tempdir().expect("a components root");
        let tree = Tree::holding(&document_with_a_consumer());
        install_package(components.path(), &staged_module(&tree));
        let mut booted = boot(&tree, vec![components.path().to_path_buf()]).await;
        let kind = SubscriberEntryKind::Wasm("sifter".to_string());
        let participant = brenn_lib::messaging::ParticipantId::for_wasm("sifter");
        let sink_uuid = booted
            .messenger
            .directory()
            .resolve("brenn:sink")
            .expect("the sink channel is declared")
            .uuid;
        assert!(
            cursor_of(&booted.messenger, "brenn:work", &participant)
                .await
                .is_some()
        );

        tree.write(&document(""));
        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert_eq!(status.delta.consumers_removed, vec!["sifter".to_string()]);
        assert!(
            status
                .delta
                .channels_removed
                .contains(&"brenn:sink".to_string()),
            "{:?}",
            status.delta.channels_removed
        );

        assert!(booted.driver.registry().is_empty());
        assert!(subscribed_anywhere(&booted.messenger, &kind).is_empty());
        assert!(booted.messenger.subscriber_registration(&kind).is_none());
        // Retired rather than never-wired: a wake still in flight for it
        // resolves "gone" instead of tearing the process down.
        assert!(booted.messenger.subscriber_registration_retired(&kind));
        assert!(booted.router.delivery_binding_retired(&kind));
        assert!(booted.messenger.directory().resolve("brenn:sink").is_none());
        assert!(
            cursor_of(&booted.messenger, "brenn:work", &participant)
                .await
                .is_none(),
            "a fresh boot of this document would reap the position as an orphan",
        );
        assert!(
            channel_row(&booted.messenger, sink_uuid).await.is_some(),
            "a durable row the config stops naming is the operator's to delete",
        );
    }

    /// A description is metadata: the entry keeps its uuid, its tuning, its
    /// subscribers and its consumer, and only the text moves — in the directory
    /// and in the row.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_description_only_change_is_applied_in_place() {
        let components = tempfile::tempdir().expect("a components root");
        let tree = Tree::holding(&document_with_a_consumer());
        install_package(components.path(), &staged_module(&tree));
        let mut booted = boot(&tree, vec![components.path().to_path_buf()]).await;
        let before = booted
            .messenger
            .directory()
            .resolve("brenn:work")
            .expect("the work channel is declared");
        let started = booted.driver.registry()["sifter"].verified.clone();

        // On the one line, deliberately: the fixture's packaged half stands
        // line for line against its root half, so an edit that adds a line to
        // one moves the other's bytes and with them the spec hash the
        // document is bound to.
        tree.write(&document_with_a_consumer().replace(
            r#"channel work at "brenn:work" {"#,
            r#"channel work at "brenn:work" { description = "the job queue";"#,
        ));
        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert_eq!(status.delta.channels_described, vec!["brenn:work"]);
        assert!(status.delta.channels_changed.is_empty());
        assert!(status.delta.consumers_changed.is_empty());

        let after = booted
            .messenger
            .directory()
            .resolve("brenn:work")
            .expect("the work channel is still declared");
        assert_eq!(after.description.as_deref(), Some("the job queue"));
        assert_eq!(after.uuid, before.uuid);
        assert_eq!(after.resolved_channel, before.resolved_channel);
        assert_eq!(after.subscribers.len(), before.subscribers.len());
        assert_eq!(
            channel_row(&booted.messenger, after.uuid).await,
            Some(Some("the job queue".to_string())),
            "the row carries the column the listings read",
        );
        // Nothing was restarted: the consumer in the registry is the instance
        // that was there before.
        assert_eq!(booted.driver.registry()["sifter"].verified, started);
    }

    /// A consumer whose own block moved is retired and started again under the
    /// same slug: the tombstone its retirement planted is cleared by the
    /// replacement, and its position carries over rather than being re-primed —
    /// which is what a restart of the process would do with it.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_changed_consumer_is_replaced_and_keeps_its_position() {
        let components = tempfile::tempdir().expect("a components root");
        let tree = Tree::holding(&document_with_a_consumer());
        install_package(components.path(), &staged_module(&tree));
        let mut booted = boot(&tree, vec![components.path().to_path_buf()]).await;
        let kind = SubscriberEntryKind::Wasm("sifter".to_string());
        let participant = brenn_lib::messaging::ParticipantId::for_wasm("sifter");
        let before = cursor_of(&booted.messenger, "brenn:work", &participant)
            .await
            .expect("the booted consumer holds a position");

        tree.write(&document_with_a_consumer().replace(
            "in inbound <- work { push_depth = 4; }",
            "in inbound <- work { push_depth = 2; }",
        ));
        booted.driver.reload(TriggerSource::Bus).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert_eq!(status.delta.consumers_changed, vec!["sifter".to_string()]);
        assert!(status.delta.channels_changed.is_empty());

        assert!(booted.driver.registry().contains_key("sifter"));
        assert_eq!(
            subscribed_anywhere(&booted.messenger, &kind),
            ["brenn:work"]
        );
        // Live again, not a tombstone: the replacement re-registered the key.
        assert!(booted.messenger.subscriber_registration(&kind).is_some());
        assert!(!booted.messenger.subscriber_registration_retired(&kind));
        assert!(!booted.router.delivery_binding_retired(&kind));
        assert!(booted.router.has_delivery_binding(&kind));

        let after = cursor_of(&booted.messenger, "brenn:work", &participant)
            .await
            .expect("the replacement holds a position");
        assert_eq!(
            after.next_owed_seq, before.next_owed_seq,
            "a replaced consumer resumes where it was rather than re-reading the retained tail",
        );
        assert_eq!(
            after.push_depth,
            brenn_lib::messaging::config::Depth::Bounded(2),
            "and it is attached at the depth the new document gives its port",
        );
    }

    /// A mount that was uninstalled under a running process. The roots are the
    /// mounts document's answer and it is re-read first, so the reload refuses
    /// on the mount rather than resolving a document against half a host.
    /// Reached before anything reads the deployment document at all.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_mount_that_went_missing_is_refused_naming_it() {
        let components = tempfile::tempdir().expect("a components root");
        let tree = Tree::holding(&document(""));
        let mut booted = boot(&tree, vec![components.path().to_path_buf()]).await;
        let absent = booted.mounts.declared_path("components-0");
        booted.mounts.uninstall("components-0");

        tree.write(&document(
            r#"
channel spare at "brenn:spare" {
    push_depth = 1;
    retain_depth = 1;
    standing_retain_depth = 1;
}
"#,
        ));
        assert!(
            booted
                .driver
                .prepare_and_report(TriggerSource::Signal)
                .await
                .is_none()
        );

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Refused);
        assert_eq!(status.refusals.len(), 1);
        assert!(
            status.refusals[0].contains(&absent.display().to_string()),
            "{:?}",
            status.refusals
        );
    }

    /// The whole point of the mounts document: a bundle installed after boot is
    /// a tree this process may read, with no restart and no argv edit. The
    /// consumer's record must name the *mount* as its root, since that is the
    /// path every later reload will resolve it under.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_mount_declared_since_boot_brings_its_package_into_reach() {
        let components = tempfile::tempdir().expect("a components root");
        let tree = Tree::holding(&document(""));
        let mut booted = boot(&tree, Vec::new()).await;

        // The install an operator does between two reloads: stage the tree,
        // then write the mount line.
        tree.write(&document_with_a_consumer());
        install_package(components.path(), &staged_module(&tree));
        let mount = booted
            .mounts
            .install("bundle", &[("components", components.path())]);
        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert_eq!(status.delta.consumers_added, vec!["sifter".to_string()]);
        let verified = &booted.driver.registry()["sifter"].verified;
        assert_eq!(
            verified.root,
            mount.join("components"),
            "the record must name the mount the package was resolved under",
        );
    }

    /// A package that moved on disk without its bytes moving.
    ///
    /// The install scheme a mount is deployed under stages each release into a
    /// fresh versioned tree and swaps the symlink, so a re-deploy or a rollback
    /// of identical bytes moves every canonical path under the mount. A
    /// consumer's identity is its world and its two digests, not those paths:
    /// restarting it here would drop its in-memory state for no change at all.
    /// The record is still pointed at the tree the process is now reading,
    /// because the one the consumer was loaded from is what the installer
    /// prunes.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_package_that_moved_without_changing_does_not_restart_its_consumer() {
        let first = tempfile::tempdir().expect("a components root");
        let tree = Tree::holding(&document_with_a_consumer());
        install_package(first.path(), &staged_module(&tree));
        let mut booted = boot(&tree, vec![first.path().to_path_buf()]).await;
        let started = booted.driver.registry()["sifter"].verified.clone();

        // The same release under a different tree, and the mount list moved
        // onto it in one edit — two mounts offering one package name at once
        // would be a cross-root refusal.
        let second = tempfile::tempdir().expect("the next versioned tree");
        install_package(second.path(), &staged_module(&tree));
        booted.mounts.uninstall("components-0");
        let mount = booted
            .mounts
            .install("components-1", &[("components", second.path())]);

        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(
            status.outcome,
            Outcome::Unchanged,
            "a byte-identical install moved the projection: {:?}",
            status.delta.consumers_changed,
        );
        let now = &booted.driver.registry()["sifter"].verified;
        assert_eq!(now.artifact_sha256, started.artifact_sha256);
        assert_eq!(
            now.root,
            mount.join("components"),
            "the record still names the tree the install replaced",
        );
    }

    /// **A mount that newly offers the surface kernel is a restart.** Every
    /// page loads the kernel, and a reload reloads only the pages of surfaces
    /// that moved, so a kernel arriving under a mount this process was not
    /// serving one from cannot be walked to.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_surface_kernel_that_arrived_under_a_mount_is_refused() {
        let tree = Tree::holding(&document(""));
        let mut booted = boot(&tree, Vec::new()).await;

        let surface = release_surface_tree("chart");
        booted
            .mounts
            .install("release", &[("surface", surface.path())]);

        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Refused);
        assert!(
            status.refusals.iter().any(|line| {
                line.contains("surface kernel root") && line.ends_with(super::super::NEEDS_RESTART)
            }),
            "{:?}",
            status.refusals,
        );
    }

    /// **A kind a newly declared mount offers converges.** No surface
    /// instantiates it, so nothing in the document moved — but the served roots
    /// must carry it afterwards, or the first surface to use it would be
    /// planned against a kind `/surface-static` cannot find.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_kind_offered_by_a_new_mount_converges_into_the_served_roots() {
        let tree = Tree::holding(&document(""));
        let mut booted = boot(&tree, Vec::new()).await;

        let release = release_surface_tree("chart");
        booted
            .mounts
            .install("release", &[("surface", release.path())]);
        serve_current_roots(&booted);

        let home = tempfile::tempdir().expect("a bundle home");
        let v1 = versioned_bundle(home.path(), "bundle", "1", "widget");
        booted.mounts.link("bundle", &v1);

        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(
            status.outcome,
            Outcome::Unchanged,
            "a kind nothing instantiates moves no part of the document: {:?}",
            status.refusals,
        );
        let serving = booted
            .driver
            .env
            .surface_roots
            .read()
            .expect("the cell is uncontended")
            .clone();
        assert!(
            serving.kinds.contains_key("widget"),
            "the served roots must offer the kind the new mount installed: {:?}",
            serving.kinds.keys().collect::<Vec<_>>(),
        );
    }

    /// A `SurfaceRoots` spelled directly, for the message-level cases below.
    fn roots_of(kernel: &str, kinds: &[(&str, &str, &str)]) -> brenn_surface_server::SurfaceRoots {
        brenn_surface_server::SurfaceRoots {
            withheld: Default::default(),
            kernel: Some(brenn_surface_server::KernelRoot::for_test(kernel)),
            kinds: kinds
                .iter()
                .map(|(kind, mount, root)| {
                    (
                        (*kind).to_string(),
                        brenn_surface_server::KindRoot {
                            mount: (*mount).to_string(),
                            root: PathBuf::from(root),
                            source_sha256: "s".to_string(),
                            spec_sha256: "p".to_string(),
                            cores: Vec::new(),
                        },
                    )
                })
                .collect(),
        }
    }

    /// Kinds converge and the kernel does not, which is the whole of what this
    /// check is left holding. Every kind-grain difference — offered by another
    /// mount, reinstalled, withdrawn, newly offered — passes it, because each
    /// promotes the surfaces instantiating it into the surface delta; a kernel
    /// from a different tree is the one restart.
    #[test]
    fn only_a_moved_kernel_root_is_refused() {
        let serving = roots_of("/brenn/surface", &[("chart", "bundle", "/b.v1/surface")]);

        for scanned in [
            roots_of("/brenn/surface", &[("chart", "other", "/o/surface")]),
            roots_of("/brenn/surface", &[("chart", "bundle", "/b.v2/surface")]),
            roots_of("/brenn/surface", &[]),
            roots_of(
                "/brenn/surface",
                &[
                    ("chart", "bundle", "/b.v1/surface"),
                    ("gauge", "extra", "/e/surface"),
                ],
            ),
        ] {
            assert!(
                surface_kernel_refusal(&serving, &scanned).is_ok(),
                "a kind-grain difference is the surface delta's to walk, not a refusal",
            );
        }

        let other_kernel = roots_of("/other/surface", &[("chart", "bundle", "/b.v1/surface")]);
        let refusals = surface_kernel_refusal(&serving, &other_kernel)
            .expect_err("a moved kernel root is refused");
        assert_eq!(refusals.len(), 1, "{refusals:?}");
        assert!(refusals[0].contains("surface kernel root"), "{refusals:?}");
        assert!(
            refusals[0].ends_with(super::super::NEEDS_RESTART),
            "{refusals:?}",
        );
    }

    /// A release's surface tree: the kernel module pair every page references,
    /// plus one conforming kind.
    fn release_surface_tree(kind: &str) -> tempfile::TempDir {
        let surface = tempfile::tempdir().expect("a surface root");
        brenn_surface_server::test_fixtures::write_kernel_pair(surface.path());
        brenn_surface_server::test_fixtures::write_valid_kind(surface.path(), kind);
        surface
    }

    /// **The bundle-upgrade shape converges.** The mount is a symlink to a
    /// versioned tree, so an install moves the bytes under a path that never
    /// changes; only the kind's fingerprint says it happened. No surface
    /// instantiates the kind here, so the whole of the reload's work is
    /// installing the roots it scanned — which is what lets the next surface of
    /// that kind be planned against the bytes on disk.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_surface_kind_reinstalled_under_its_mount_converges() {
        let tree = Tree::holding(&document(""));
        let mut booted = boot(&tree, Vec::new()).await;

        let surface = release_surface_tree("chart");
        booted
            .mounts
            .install("release", &[("surface", surface.path())]);
        serve_current_roots(&booted);
        let before = booted
            .driver
            .env
            .surface_roots
            .read()
            .expect("the cell is uncontended")
            .kinds["chart"]
            .source_sha256
            .clone();

        brenn_surface_server::test_fixtures::write_processor_tree_from_bytes(
            surface.path(),
            "chart",
            b"chart-v2",
            &brenn_surface_server::test_fixtures::spec_bytes_for("chart"),
            Vec::new(),
            true,
            |_| {},
        );

        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(
            status.outcome,
            Outcome::Unchanged,
            "an upgraded kind nothing instantiates moves no part of the document: {:?}",
            status.refusals,
        );
        let after = booted
            .driver
            .env
            .surface_roots
            .read()
            .expect("the cell is uncontended")
            .kinds["chart"]
            .source_sha256
            .clone();
        assert_ne!(
            before, after,
            "the served roots must carry the reinstalled kind's fingerprint",
        );
    }

    /// **An asset failure found at reload is framed as a reload.** The process
    /// is still serving the document it has, so the refusal names the reload
    /// and must not end in boot's "Refusing to start" — which is the whole
    /// reason the validator takes a context at all, and the only production
    /// site that passes `RELOAD` is the scan this drives.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_reload_asset_refusal_is_framed_as_a_reload_and_not_as_a_boot() {
        let tree = Tree::holding(&document(""));
        let mut booted = boot(&tree, Vec::new()).await;

        let surface = tempfile::tempdir().expect("a surface root");
        brenn_surface_server::test_fixtures::write_kernel_pair(surface.path());
        // A kind directory with no manifest: the scan maps it, and only the
        // validator's per-kind record pass can refuse it.
        std::fs::create_dir_all(surface.path().join("processor").join("broken"))
            .expect("a kind directory");
        booted
            .mounts
            .install("release", &[("surface", surface.path())]);

        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Refused);
        let line = status
            .refusals
            .iter()
            .find(|line| line.contains("broken"))
            .unwrap_or_else(|| panic!("{:?}", status.refusals));
        assert!(line.starts_with("reload:"), "{line}");
        assert!(!line.contains("Refusing to start"), "{line}");
    }

    /// One versioned tree of a bundle mount: `<home>/<name>.v<version>/`,
    /// holding a `VERSION` and a `surface/` tree with one kind. The kind's
    /// bytes are a function of its name alone, so two versions of one kind are
    /// byte-identical — which is the point of the case below.
    fn versioned_bundle(home: &std::path::Path, name: &str, version: &str, kind: &str) -> PathBuf {
        let versioned = home.join(format!("{name}.v{version}"));
        let surface = versioned.join("surface");
        std::fs::create_dir_all(&surface).expect("a versioned surface tree");
        std::fs::write(versioned.join("VERSION"), format!("{version}\n")).expect("a VERSION");
        brenn_surface_server::test_fixtures::write_valid_kind(&surface, kind);
        versioned
    }

    /// Seed the roots cell with what the declared mounts offer right now, which
    /// is what boot does before the driver is built.
    fn serve_current_roots(booted: &Booted) {
        let mounts = brenn_lib::config::load_mounts(Some(&booted.mounts.path()));
        let serving =
            brenn_surface_server::validate_surface_assets(&mounts.roots.surface_roots, &[]);
        *booted
            .driver
            .env
            .surface_roots
            .write()
            .expect("the cell is uncontended") = Arc::new(serving);
    }

    /// **A bundle re-installed at byte-identical contents converges.** The
    /// installer swaps the mount symlink onto a fresh versioned tree, so every
    /// canonical root under that mount moves — but nothing about any kind
    /// changed, so this is a relocation and not a restart. The roots cell must
    /// follow it: the tree it was serving from is the one the installer prunes.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_bundle_whose_tree_relocated_converges_and_the_served_roots_follow() {
        let tree = Tree::holding(&document(""));
        let mut booted = boot(&tree, Vec::new()).await;

        let release = release_surface_tree("chart");
        booted
            .mounts
            .install("release", &[("surface", release.path())]);
        let home = tempfile::tempdir().expect("a bundle home");
        let v1 = versioned_bundle(home.path(), "bundle", "1", "widget");
        booted.mounts.link("bundle", &v1);
        serve_current_roots(&booted);

        let v2 = versioned_bundle(home.path(), "bundle", "2", "widget");
        booted.mounts.link("bundle", &v2);

        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(
            status.outcome,
            Outcome::Unchanged,
            "a byte-identical re-install moves nothing: {:?}",
            status.refusals,
        );
        let serving = booted
            .driver
            .env
            .surface_roots
            .read()
            .expect("the cell is uncontended")
            .clone();
        assert_eq!(
            serving.kinds["widget"].root,
            v2.canonicalize()
                .expect("the new tree exists")
                .join("surface"),
            "the served root follows the swapped symlink",
        );
        assert_eq!(
            serving.kinds["chart"].root,
            booted
                .mounts
                .declared_path("release")
                .canonicalize()
                .expect("the release mount exists")
                .join("surface"),
            "the mount that did not move is untouched",
        );
    }

    /// The mounts array in the outcome body: what a reader asks to learn which
    /// revision of which bundle this process is serving.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_status_body_names_the_mounts_the_process_reads() {
        let tree = Tree::holding(&document(""));
        let mut booted = boot(&tree, Vec::new()).await;

        tree.write(&one_more_channel());
        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        let named: Vec<&str> = status
            .mounts
            .iter()
            .map(|mount| mount.name.as_str())
            .collect();
        assert_eq!(named, vec!["tree"]);
        assert_eq!(status.mounts[0].version, "test");
        assert_eq!(status.mounts[0].trees, vec!["modules".to_string()]);
    }

    /// A refusal changed nothing, so the mounts it reports are the ones the
    /// process is still reading — not the candidate's.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_refusal_reports_the_running_mounts() {
        let components = tempfile::tempdir().expect("a components root");
        let tree = Tree::holding(&document(""));
        let mut booted = boot(&tree, vec![components.path().to_path_buf()]).await;

        // A mount whose VERSION went away is not a mount: the reload refuses
        // before it reads a line of the deployment document.
        std::fs::remove_file(booted.mounts.declared_path("components-0").join("VERSION"))
            .expect("the VERSION is removable");
        tree.write(&one_more_channel());
        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Refused);
        assert!(
            status.refusals[0].contains("VERSION"),
            "{:?}",
            status.refusals
        );
        let named: Vec<&str> = status
            .mounts
            .iter()
            .map(|mount| mount.name.as_str())
            .collect();
        assert_eq!(named, vec!["components-0", "tree"]);
    }

    /// A mount whose line is gone takes its packages with it. The document
    /// still declares the consumer, so the refusal is the package's: the
    /// process keeps running the bytes it has.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_retired_mount_under_a_running_consumer_is_refused() {
        let components = tempfile::tempdir().expect("a components root");
        let tree = Tree::holding(&document_with_a_consumer());
        install_package(components.path(), &staged_module(&tree));
        let mut booted = boot(&tree, vec![components.path().to_path_buf()]).await;

        booted.mounts.uninstall("components-0");
        booted.mounts.write();
        // The document is untouched; only the mount line went.
        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Refused);
        assert!(
            status.refusals[0].contains("components"),
            "{:?}",
            status.refusals
        );
        assert!(
            booted
                .driver
                .registry()
                .iter()
                .any(|(slug, _)| slug == "sifter"),
            "the running consumer is untouched by a refusal",
        );
    }

    /// Two mounts offering one packaged module: the module scan the compile
    /// runs refuses the pair, because a name resolved out of two releases is
    /// resolved out of neither.
    #[tokio::test(flavor = "multi_thread")]
    async fn one_module_under_two_mounts_is_refused() {
        let tree = Tree::holding(&document_with_a_consumer());
        let components = tempfile::tempdir().expect("a components root");
        install_package(components.path(), &staged_module(&tree));
        let mut booted = boot(&tree, vec![components.path().to_path_buf()]).await;

        let second = tempfile::tempdir().expect("a second module root");
        std::fs::write(
            second.path().join(format!("{PACKAGED_MODULE}.brenn")),
            staged_module(&tree),
        )
        .expect("the duplicate module is writable");
        booted
            .mounts
            .install("other", &[("modules", second.path())]);
        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Refused);
        assert!(
            status.refusals[0].contains(PACKAGED_MODULE),
            "{:?}",
            status.refusals
        );
    }

    /// The smallest convergible change: one added channel.
    fn one_more_channel() -> String {
        document(
            r#"
channel extra at "brenn:extra" {
    push_depth = 1;
    retain_depth = 4;
    standing_retain_depth = 4;
}
"#,
        )
    }

    fn live_addresses(messenger: &Arc<Messenger>) -> Vec<String> {
        messenger
            .directory()
            .list()
            .iter()
            .map(|entry| entry.address.clone())
            .collect()
    }

    /// The bus door, end to end: a message on the request channel is a reload.
    ///
    /// The request is published *before* the door opens, so the drain loop's
    /// startup sweep picks it up. This fixture runs no dispatcher task, so
    /// the sweep is the only delivery arm exercised.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_message_on_the_request_channel_converges_the_process() {
        let tree = Tree::holding(&document(""));
        let Booted {
            driver,
            messenger,
            db,
            reload_notify,
            ..
        } = boot(&tree, Vec::new()).await;
        assert!(!live_addresses(&messenger).contains(&"brenn:extra".to_string()));

        tree.write(&one_more_channel());
        seat_a_conversation(&db, 1).await;

        let published = messenger
            .publish(
                brenn_messaging::PublishOrigin::Conversation { id: 1 },
                READER,
                RELOAD_ADDRESS,
                "please",
                brenn_messaging::Urgency::Normal,
                None,
                None,
                None,
            )
            .await;
        assert!(
            matches!(published, brenn_messaging::PublishResult::Ok { .. }),
            "the reload request must reach the channel: {published:?}"
        );

        let requests = super::super::doors::spawn_driver(driver);
        super::super::doors::spawn_bus_door(&messenger, reload_notify, requests.clone());

        let outcomes = outcomes_until(&messenger, 1).await;
        assert_eq!(outcomes[0].outcome, Outcome::Applied);
        assert_eq!(outcomes[0].trigger, Trigger::Bus);
        assert_eq!(outcomes[0].generation, 1);
        assert_eq!(
            outcomes[0].delta.channels_added,
            vec!["brenn:extra".to_string()]
        );
        assert!(live_addresses(&messenger).contains(&"brenn:extra".to_string()));
    }

    /// Two triggers with no reload in flight are two reloads; coalescing
    /// applies only to what arrives *during* one.
    #[tokio::test(flavor = "multi_thread")]
    async fn two_triggers_back_to_back_are_two_reloads_the_second_unchanged() {
        let tree = Tree::holding(&document(""));
        let Booted {
            driver, messenger, ..
        } = boot(&tree, Vec::new()).await;

        tree.write(&one_more_channel());

        let requests = super::super::doors::spawn_driver(driver);
        requests.ask(TriggerSource::Signal);
        requests.ask(TriggerSource::Signal);

        let outcomes = outcomes_until(&messenger, 2).await;
        assert_eq!(outcomes.len(), 2, "{outcomes:?}");
        assert_eq!(outcomes[0].outcome, Outcome::Applied);
        assert_eq!(outcomes[0].generation, 1);
        assert_eq!(
            outcomes[0].delta.channels_added,
            vec!["brenn:extra".to_string()]
        );
        assert_eq!(outcomes[1].outcome, Outcome::Unchanged);
        assert_eq!(outcomes[1].trigger, Trigger::Signal);
        assert_eq!(outcomes[1].generation, 1);
        assert_eq!(
            outcomes[1].running_document_sha256,
            outcomes[0].running_document_sha256
        );
        assert!(outcomes[1].delta.channels_added.is_empty());
    }

    /// Every trigger that arrives while a reload is running collapses into one
    /// further reload — not one each.
    ///
    /// The triggers are enqueued before the driver is put on the queue, which
    /// is the same arm without the race: the driver takes the first, runs a
    /// reload, and then finds the rest waiting exactly as it would have found
    /// them arriving during that reload. A driver that simply looped on `recv`
    /// would produce one outcome per trigger.
    #[tokio::test(flavor = "multi_thread")]
    async fn triggers_arriving_during_a_reload_coalesce_into_one() {
        let tree = Tree::holding(&document(""));
        let Booted {
            driver, messenger, ..
        } = boot(&tree, Vec::new()).await;

        tree.write(&one_more_channel());

        let (requests, rx) = super::super::doors::trigger_channel();
        for _ in 0..super::super::doors::TRIGGER_QUEUE_DEPTH {
            requests.ask(TriggerSource::Signal);
        }
        super::super::doors::spawn_driver_on(driver, rx);

        drop(outcomes_until(&messenger, 2).await);
        // Settle: if the queue were being drained one reload per trigger, more
        // would arrive after the second.
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        let outcomes = outcomes_on(&messenger).await;
        assert_eq!(
            outcomes.len(),
            2,
            "{} triggers must collapse into the one reload that answers them all: {outcomes:?}",
            super::super::doors::TRIGGER_QUEUE_DEPTH,
        );
        assert_eq!(outcomes[0].outcome, Outcome::Applied);
        assert_eq!(outcomes[1].outcome, Outcome::Unchanged);
    }

    /// A registry holding one async tool `apull` (acl key `repo`), which is
    /// what makes a consumer's `tool` grant resolvable and mints the executor
    /// and its request channel.
    pub(crate) fn async_tool_registry() -> Arc<brenn_tool_registry::ToolRegistry> {
        use brenn_tool_registry::ToolError;
        use brenn_tool_registry::descriptor::{AclDenied, Idempotency, ToolClass, ToolDescriptor};
        use brenn_tool_registry::tool::{AsyncTool, RegisteredTool, ToolCtx};
        use serde_json::{Value, json};

        struct APull(ToolDescriptor);
        #[async_trait::async_trait]
        impl AsyncTool for APull {
            fn descriptor(&self) -> &ToolDescriptor {
                &self.0
            }
            fn check_acl(
                &self,
                _a: &Value,
                _c: &[brenn_lib::tools::AclClause],
            ) -> Result<(), AclDenied> {
                Ok(())
            }
            async fn execute(&self, _c: &ToolCtx, _a: Value) -> Result<Value, ToolError> {
                Ok(json!({}))
            }
        }
        Arc::new(brenn_tool_registry::ToolRegistry::new(vec![
            RegisteredTool::Async(Arc::new(APull(ToolDescriptor {
                name: "apull",
                mcp_name: "mcp__brenn__APull",
                description: "stub async",
                input_schema: json!({ "type": "object" }),
                class: ToolClass::Async { max_concurrency: 4 },
                acl_keys: &["repo"],
                idempotency: Idempotency::Natural,
                auto_approve: true,
            }))),
        ]))
    }

    /// A document whose `sifter` holds an async grant on that tool, narrowed to
    /// one repository, plus whatever `extra` stamps beside it.
    fn document_with_tool_grants(sifter_repo: &str, extra: &str) -> String {
        document(&format!(
            r#"channel sink at "brenn:sink" {{
    push_depth = 1;
    retain_depth = 4;
    standing_retain_depth = 4;
}}
{PACKAGED}component Demo {{
    abi = processor;
    requires = [ports, tools];
    in inbound;
    in tool-results;
    out digest;
}}
{PACKAGED}
new sifter: Demo {{
    grants = [ports, tools];
    in inbound <- work {{ push_depth = 4; }}
    out digest -> sink;
    tool apull {{ allow {{ repo = "{sifter_repo}"; }} }}
}}
{extra}
"#
        ))
    }

    /// The second tool-granted consumer, reading what the first writes.
    const A_SECOND_TOOL_GRANTED_CONSUMER: &str = r#"
new grinder: Demo {
    grants = [ports, tools];
    in inbound <- sink { push_depth = 2; }
    out digest -> work;
    tool apull { allow { repo = "notes"; } }
}
"#;

    /// Whether `caller` may call `apull` on `repo`, as the executor's table
    /// answers it right now.
    fn may_pull(grants: &brenn_tool_registry::ToolCallerGrants, slug: &str, repo: &str) -> bool {
        let caller = brenn_lib::messaging::ParticipantId::for_wasm(slug);
        grants.grant(caller.as_str(), "apull").is_some_and(|grant| {
            grant.acl_allows(&std::collections::BTreeMap::from([(
                "repo".to_string(),
                repo.to_string(),
            )]))
        })
    }

    /// The async tool executor's per-caller grant table across a consumer's
    /// whole life: installed when it arrives, replaced when its document moves,
    /// and gone when it leaves.
    ///
    /// This is the authorization table for async tool calls, so the ordering
    /// inside the commit walk is what the last phase pins: a replacement's
    /// `set_caller` has to land *after* the retirement's `remove_caller`, or
    /// the consumer runs on with the grants its previous document conferred.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_tool_grant_table_follows_the_consumer_delta() {
        let components = tempfile::tempdir().expect("a components root");
        let roots = vec![components.path().to_path_buf()];
        let tree = Tree::holding(&document_with_tool_grants("brenn", ""));
        let install = || install_package(components.path(), &staged_module(&tree));
        install();
        let mut booted = boot_with(
            &tree,
            BootFixture {
                components_roots: roots,
                tool_registry: Some(async_tool_registry()),
                ..BootFixture::default()
            },
        )
        .await;
        let grants = booted
            .tool_caller_grants
            .clone()
            .expect("a document holding an async tool grant mints the executor's table");
        assert!(may_pull(&grants, "sifter", "brenn"));

        // Arriving: the caller key is installed with the document's grants.
        tree.write(&document_with_tool_grants(
            "brenn",
            A_SECOND_TOOL_GRANTED_CONSUMER,
        ));
        install();
        booted.driver.reload(TriggerSource::Signal).await;
        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert_eq!(status.delta.consumers_added, vec!["grinder".to_string()]);
        assert!(may_pull(&grants, "grinder", "notes"));
        assert!(!may_pull(&grants, "grinder", "brenn"));
        assert!(may_pull(&grants, "sifter", "brenn"));

        // Leaving: the caller key goes with it, and nobody else's moves.
        tree.write(&document_with_tool_grants("brenn", ""));
        install();
        booted.driver.reload(TriggerSource::Signal).await;
        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert_eq!(status.delta.consumers_removed, vec!["grinder".to_string()]);
        assert!(
            grants
                .grant(
                    brenn_lib::messaging::ParticipantId::for_wasm("grinder").as_str(),
                    "apull",
                )
                .is_none(),
            "a retired consumer's tool authority does not outlive it",
        );
        assert!(may_pull(&grants, "sifter", "brenn"));

        // Changed: the table carries the new document's set, not the old one.
        tree.write(&document_with_tool_grants("notes", ""));
        install();
        booted.driver.reload(TriggerSource::Signal).await;
        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert_eq!(status.delta.consumers_changed, vec!["sifter".to_string()]);
        assert!(may_pull(&grants, "sifter", "notes"));
        assert!(
            !may_pull(&grants, "sifter", "brenn"),
            "the replacement runs on the grants this document confers, not its predecessor's",
        );

        // Replace sifter with a component that holds no tool grant at all.
        tree.write(&document(&format!(
            r#"channel sink at "brenn:sink" {{
    push_depth = 1;
    retain_depth = 4;
    standing_retain_depth = 4;
}}
{PACKAGED}component Demo {{
    abi = processor;
    requires = [ports, tools];
    in inbound;
    in tool-results;
    out digest;
}}

component Plain {{
    abi = processor;
    requires = [ports];
    in inbound;
    out digest;
}}
{PACKAGED}
new sifter: Plain {{
    grants = [ports];
    in inbound <- work {{ push_depth = 4; }}
    out digest -> sink;
}}
"#
        )));
        install();
        booted.driver.reload(TriggerSource::Signal).await;
        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert_eq!(status.delta.consumers_changed, vec!["sifter".to_string()]);
        assert!(
            grants
                .grant(
                    brenn_lib::messaging::ParticipantId::for_wasm("sifter").as_str(),
                    "apull",
                )
                .is_none(),
            "a consumer whose document withdrew its grants may address no tool",
        );
        assert!(
            grants.snapshot().is_empty(),
            "and holds no caller key at all, as a fresh boot of the same document would: {:?}",
            grants.snapshot(),
        );
    }

    // ---------------------------------------------------------------------
    // The `applied` body is measured in prepare, where a refusal costs nothing.
    // ---------------------------------------------------------------------

    /// The `[messaging]` block both sides of these two cases stand on. Small
    /// enough that a modest delta overruns it, roomy enough that the refusal
    /// naming the overrun still publishes.
    const A_SMALL_BODY_LIMIT: &str = "\nmessaging {\n    max_body_bytes = 2000;\n}\n";

    /// `count` declared channels whose only purpose is to make the delta long.
    fn padding_channels(count: usize) -> String {
        (0..count)
            .map(|n| {
                format!(
                    "channel pad{n} at \"brenn:padding-channel-for-the-body-size-check-{n:02}\" \
                     {{\n    push_depth = 1;\n    retain_depth = 1;\n    \
                     standing_retain_depth = 1;\n}}\n"
                )
            })
            .collect()
    }

    /// A consumer whose package is installed and whose record binds, but whose
    /// store parent directory does not exist: the plan and the record checks
    /// pass, and `load_consumer` is what refuses it.
    ///
    /// The candidate that carries it also overruns the body limit, so which of
    /// the two refusals comes back says which step ran first.
    fn a_consumer_whose_load_would_refuse(store_dir: &std::path::Path) -> String {
        let store = store_dir.join("no-such-directory").join("sifter.db");
        format!(
            r#"{PACKAGED}component Sifter {{
    abi = processor;
    requires = [ports, store];
    in inbound;
    out digest;
}}
{PACKAGED}
new sifter: Sifter {{
    grants = [ports, store];
    store_path = "{}";
    in inbound <- work {{ push_depth = 4; }}
    out digest -> scratch;
}}
"#,
            store.display(),
        )
    }

    /// A delta whose `applied` body would not fit the channel it is published on
    /// is refused before anything is loaded, and the running state is where it
    /// was.
    ///
    /// This is reachable without a bug: the body carries every moved address and
    /// every consumer slug, so a large enough edit overruns any limit. The
    /// design's answer for a change that cannot be applied live is a refusal in
    /// prepare, not a panic after the process has already converged.
    ///
    /// The candidate brings a consumer whose *load* would refuse, so the
    /// ordering is what is under test and not merely the outcome: the size
    /// check runs before the cranelift compiles it would make pointless, and a
    /// measurement moved below them would come back naming the store path
    /// instead.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_applied_body_that_would_not_fit_is_refused_before_anything_moves() {
        let tree = Tree::holding(&document(A_SMALL_BODY_LIMIT));
        let components = tempfile::tempdir().expect("a components root");
        let store_dir = tempfile::tempdir().expect("a store root");
        let mut booted = boot(&tree, vec![components.path().to_path_buf()]).await;
        let baseline = booted.driver.baseline().document.document_sha256.clone();
        let before = booted.messenger.directory().list().len();

        tree.write(&document(&format!(
            "{A_SMALL_BODY_LIMIT}{}{}",
            padding_channels(40),
            a_consumer_whose_load_would_refuse(store_dir.path()),
        )));
        install_package(components.path(), &staged_module(&tree));
        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Refused);
        let reason = status.refusals.join("; ");
        assert!(
            reason.contains("max_body_bytes is 2000"),
            "the refusal names the knob and its value: {reason}"
        );
        assert!(
            reason.contains("bytes but"),
            "the refusal names the body's size: {reason}"
        );
        assert!(
            !reason.contains("no-such-directory"),
            "the body is measured before the arriving consumer is loaded, so the load's own \
             refusal is never reached: {reason}"
        );
        assert_eq!(
            booted.messenger.directory().list().len(),
            before,
            "a refusal moves nothing"
        );
        assert_eq!(
            booted.driver.baseline().document.document_sha256,
            baseline,
            "and leaves the baseline naming the document the process projects"
        );
    }

    /// The same candidate under a limit that fits: the load refusal the body
    /// check pre-empted above is what comes back, which is what makes the
    /// ordering assertion there mean something.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_same_candidate_under_a_roomy_limit_reaches_the_load() {
        let tree = Tree::holding(&document(""));
        let components = tempfile::tempdir().expect("a components root");
        let store_dir = tempfile::tempdir().expect("a store root");
        let mut booted = boot(&tree, vec![components.path().to_path_buf()]).await;

        tree.write(&document(&format!(
            "{}{}",
            padding_channels(40),
            a_consumer_whose_load_would_refuse(store_dir.path()),
        )));
        install_package(components.path(), &staged_module(&tree));
        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Refused);
        let reason = status.refusals.join("; ");
        assert!(
            reason.contains("no-such-directory"),
            "the arriving consumer's load is what refuses: {reason}"
        );
    }

    /// The same candidate under the default limit applies, and what is published
    /// is the very body prepare measured — only `at` is restamped, at a fixed
    /// width, so the size that was proved is the size that goes out.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_published_applied_body_is_the_one_prepare_measured() {
        let tree = Tree::holding(&document(""));
        let mut booted = boot(&tree, Vec::new()).await;

        tree.write(&document(&padding_channels(40)));
        let dynamic = booted.driver.dynamic_snapshot().await;
        let measured = match booted.driver.prepare(TriggerSource::Signal, &dynamic) {
            Prepared::Ready(ready) => ready.applied,
            other => panic!("the candidate is applicable: {}", outcome_of(&other)),
        };

        booted.driver.reload(TriggerSource::Signal).await;
        let published = booted.last_status().await;

        assert_eq!(published.outcome, Outcome::Applied, "{published:?}");
        assert_eq!(
            ReloadStatus {
                at: measured.at.clone(),
                ..published.clone()
            },
            measured,
            "every field but the timestamp is prepare's",
        );
        assert_eq!(
            published.body().len(),
            measured.body().len(),
            "the timestamp is fixed-width, so the measured size stands",
        );
    }

    /// What a `Prepared` says, for a panic message.
    fn outcome_of(prepared: &Prepared) -> String {
        match prepared {
            Prepared::Refused { refusals, .. } => format!("refused: {refusals:?}"),
            Prepared::Unchanged(_) => "unchanged".to_string(),
            Prepared::Ready(_) => "ready".to_string(),
        }
    }

    /// A retuned channel, committed: the one step of the walk that is a removal
    /// and an addition of the same address.
    ///
    /// Rule 1 makes this shape the only convergible one — every subscriber on a
    /// moving entry has to be a consumer that moves with it, which delta
    /// closure arranges by promoting the consumer reading it. What is under
    /// test is the ordering: add-before-remove would leave two entries at one
    /// address, and an entry re-added with its old subscribers un-cleared would
    /// name a consumer twice.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_retuned_channel_is_removed_and_re_added_with_its_consumer() {
        let components = tempfile::tempdir().expect("a components root");
        let tree = Tree::holding(&document_with_a_consumer());
        install_package(components.path(), &staged_module(&tree));
        let mut booted = boot(&tree, vec![components.path().to_path_buf()]).await;
        let before = booted
            .messenger
            .directory()
            .resolve("brenn:work")
            .expect("the work channel is declared");

        // One line for one line, so the packaged half's bytes do not move.
        tree.write(&document_with_a_consumer().replace(
            "    standing_retain_depth = 64;",
            "    standing_retain_depth = 32;",
        ));
        booted.driver.reload(TriggerSource::Bus).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert_eq!(status.delta.channels_changed, vec!["brenn:work"]);
        // Closure: the consumer reading a moved entry moves with it.
        assert_eq!(status.delta.consumers_changed, vec!["sifter".to_string()]);

        let live = booted.messenger.directory();
        let at_the_address: Vec<_> = live
            .list()
            .iter()
            .filter(|entry| entry.address == "brenn:work")
            .cloned()
            .collect();
        assert_eq!(
            at_the_address.len(),
            1,
            "the removal frees the address before the addition claims it",
        );
        let after = &at_the_address[0];
        assert_eq!(
            after.resolved_channel.standing_retain_depth,
            brenn_lib::messaging::config::Depth::Bounded(32),
            "the live entry carries the candidate's tuning",
        );
        // A retune is not a rename: the uuid is the address's, so the durable
        // row — and with it the resume epoch — is the one that was already
        // there.
        assert_eq!(after.uuid, before.uuid);
        assert_eq!(
            after
                .subscribers
                .iter()
                .map(|sub| sub.kind.clone())
                .collect::<Vec<_>>(),
            vec![SubscriberEntryKind::Wasm("sifter".to_string())],
            "the entry arrives empty and step 5 folds the replacement in, once",
        );
        assert!(
            channel_row(&booted.messenger, after.uuid).await.is_some(),
            "the durable row survives the remove-then-add",
        );
        assert!(booted.driver.registry().contains_key("sifter"));
    }

    /// The same retune, on a channel an agent declares a subscription to:
    /// applied, with the agent promoted into the delta by closure. The agent's
    /// subscription is on both sides, so the commit folds the entry out with
    /// the old channel and back in from the candidate's plan.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_retune_of_a_channel_an_agent_subscribes_to_moves_the_agent_with_it() {
        let tree = Tree::holding(&document_subscribing("", &["work"]));
        let mut booted = boot_with(
            &tree,
            BootFixture {
                ..BootFixture::default()
            },
        )
        .await;
        assert_eq!(
            subscribed_anywhere(
                &booted.messenger,
                &SubscriberEntryKind::App(READER.to_string())
            ),
            ["brenn:work"],
            "the fixture seats the declared subscriber this case is about",
        );

        tree.write(&document_subscribing("", &["work"]).replace(
            "    standing_retain_depth = 64;",
            "    standing_retain_depth = 32;",
        ));
        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert_eq!(
            status.delta.channels_changed,
            vec!["brenn:work".to_string()]
        );
        assert_eq!(status.delta.agents_changed, vec![READER.to_string()]);
        // Both sides: the entry the commit takes out sits on the old channel
        // entry and the one it puts back on the new.
        let moved = format!("{READER} brenn:work");
        assert_eq!(status.delta.subscriptions_removed, vec![moved.clone()]);
        assert_eq!(status.delta.subscriptions_added, vec![moved]);
        assert_eq!(
            booted
                .messenger
                .directory()
                .resolve("brenn:work")
                .expect("the work channel is still declared")
                .resolved_channel
                .standing_retain_depth,
            brenn_lib::messaging::config::Depth::Bounded(32),
            "the live entry carries the candidate's tuning",
        );
        // The point of the case: the channel left and came back, and the agent
        // came back with it.
        assert_eq!(
            subscribed_anywhere(
                &booted.messenger,
                &SubscriberEntryKind::App(READER.to_string())
            ),
            ["brenn:work"],
            "the agent is folded back onto the re-added channel",
        );
    }

    /// An edit to nothing but the agent's authority: applied, with the agent
    /// named and no channel and no subscription moved. The reload swaps the
    /// map and every gate reads the new authority per call.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_authority_only_edit_applies_and_names_the_agent() {
        let tree = Tree::holding(&document(""));
        let mut booted = boot(&tree, Vec::new()).await;

        tree.write(&document("").replace(
            "acl subscribe [exact reload_outcomes",
            "acl subscribe [prefix \"brenn:automation.\", exact reload_outcomes",
        ));
        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert_eq!(status.delta.agents_changed, vec![READER.to_string()]);
        assert!(status.delta.subscriptions_added.is_empty());
        assert!(status.delta.subscriptions_removed.is_empty());
        for moved in [
            &status.delta.channels_added,
            &status.delta.channels_removed,
            &status.delta.channels_changed,
        ] {
            assert!(moved.is_empty(), "no channel moved: {moved:?}");
        }
        // The effect, not the report: the map every gate reads is the
        // candidate's, so the widened matcher decides the next publish.
        let apps = booted.messenger.app_table().load();
        assert!(
            apps[READER]
                .policy
                .acls
                .brenn_subscribe
                .iter()
                .any(|matcher| matches!(
                    matcher,
                    brenn_lib::access::acl::ChannelMatcher::Prefix(p) if p == "automation."
                )),
            "the swapped table carries the candidate's ACL: {:?}",
            apps[READER].policy.acls.brenn_subscribe,
        );
        assert!(
            status.delta.sessions_retired.is_empty()
                && status.delta.sessions_retire_pending.is_empty(),
            "an authority-only edit is live at once; nothing a process holds is stale",
        );
        // The baseline moved *and* the process did, so the same bytes read
        // again are a no-op rather than a second application of an edit that
        // never landed.
        booted.driver.reload(TriggerSource::Signal).await;
        assert_eq!(booted.last_status().await.outcome, Outcome::Unchanged);
    }

    /// A per-call field alone — nothing an agent's authority view carries.
    /// Level 1's word is what puts the agent in the delta, which is what makes
    /// the reload commit rather than adopt the candidate as baseline with the
    /// edit unapplied.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_per_call_edit_alone_applies_and_names_the_agent() {
        let tree = Tree::holding(&document(""));
        let mut booted = boot(&tree, Vec::new()).await;

        tree.write(&document("").replace(
            "    working_dir = \".\";",
            "    working_dir = \".\";\n    icon = \"🧪\";",
        ));
        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert_eq!(status.delta.agents_changed, vec![READER.to_string()]);
        assert!(status.delta.subscriptions_added.is_empty());
        assert!(status.delta.subscriptions_removed.is_empty());
        assert!(
            status.delta.sessions_retired.is_empty()
                && status.delta.sessions_retire_pending.is_empty(),
            "a per-call field reaches a live process through the table, not a respawn",
        );
        // What a route would serve: the per-call readers take the icon off the
        // table on each request, so the swap is the whole of the convergence.
        assert_eq!(
            booted.messenger.app_table().load()[READER].icon,
            "\u{1f9ea}",
        );
        booted.driver.reload(TriggerSource::Signal).await;
        assert_eq!(booted.last_status().await.outcome, Outcome::Unchanged);
    }

    /// A `subscribe` line added to an agent: the motivating half of the
    /// request. The entry lands on the live channel, which is what makes the
    /// agent a delivery target.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_subscription_added_to_an_agent_is_folded_onto_the_live_channel() {
        let tree = Tree::holding(&document_subscribing("", &[]));
        let mut booted = boot(&tree, Vec::new()).await;
        assert!(
            subscribed_anywhere(
                &booted.messenger,
                &SubscriberEntryKind::App(READER.to_string())
            )
            .is_empty(),
            "the agent starts with no static subscription",
        );

        tree.write(&document_subscribing("", &["work"]));
        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert_eq!(status.delta.agents_changed, vec![READER.to_string()]);
        assert_eq!(
            status.delta.subscriptions_added,
            vec![format!("{READER} brenn:work")]
        );
        assert!(status.delta.subscriptions_removed.is_empty());
        assert_eq!(
            subscribed_anywhere(
                &booted.messenger,
                &SubscriberEntryKind::App(READER.to_string())
            ),
            ["brenn:work"],
        );
    }

    /// And its inverse: the line removed takes the entry off the channel.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_subscription_removed_from_an_agent_leaves_the_live_channel() {
        let tree = Tree::holding(&document_subscribing("", &["work"]));
        let mut booted = boot(&tree, Vec::new()).await;

        tree.write(&document_subscribing("", &[]));
        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert_eq!(
            status.delta.subscriptions_removed,
            vec![format!("{READER} brenn:work")]
        );
        assert!(
            subscribed_anywhere(
                &booted.messenger,
                &SubscriberEntryKind::App(READER.to_string())
            )
            .is_empty(),
        );
    }

    /// A grant that changes what the agent's `noop_mcp.py` would list: the
    /// staged rendering is renamed onto the live path at the swap, and nothing
    /// is left staged.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_grant_that_moves_the_tool_list_rewrites_the_virtual_tools_file() {
        // A declared broker, so the candidate's `client` matcher — which is
        // what derives the MQTT publish grant, and with it the tool — names a
        // client both documents hold.
        const BROKER: &str =
            "mqtt_client ha {\n    url = \"mqtts://127.0.0.1:8883\";\n    qos = 1;\n}\n";
        let tree = Tree::holding(&document(BROKER));
        let mut booted = boot(&tree, Vec::new()).await;
        let before = booted.messenger.app_table().load();
        let path = before[READER].virtual_tools_path();
        let staged = super::super::agents::staged_virtual_tools_path(&before[READER]);
        assert!(!std::fs::read_to_string(&path).unwrap().contains("MqttSend"));

        tree.write(&document(BROKER).replace(
            "acl publish [exact reload_requests, exact work];",
            "acl publish [exact reload_requests, exact work, client \"mqtt:ha\"];",
        ));
        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert_eq!(status.delta.agents_changed, vec![READER.to_string()]);
        assert!(
            std::fs::read_to_string(&path)
                .expect("the live file is still there")
                .contains("MqttSend"),
            "the staged rendering was renamed onto the live path",
        );
        assert!(
            !staged.exists(),
            "the rename consumed the staged file: {}",
            staged.display(),
        );
    }

    /// A user dropped from `allowed_users`: their session is retired and the
    /// commit pulses every open socket, which is what makes each re-ask the
    /// connect-time question against the swapped table.
    #[tokio::test(flavor = "multi_thread")]
    async fn removing_a_user_retires_their_session_and_pulses_the_open_connections() {
        let tree = Tree::holding(&document("").replace(
            "    working_dir = \".\";",
            "    working_dir = \".\";\n    allowed_users = [\"alice\", \"bob\"];",
        ));
        let mut booted = boot(&tree, Vec::new()).await;
        let alice = seat_user(&booted.db, "alice").await;
        let bob = seat_user(&booted.db, "bob").await;
        let alices = seat_bridge(&booted, alice).await;
        let bobs = seat_bridge(&booted, bob).await;
        let mut pulses = booted.apps_swapped_tx.subscribe();

        tree.write(&document("").replace(
            "    working_dir = \".\";",
            "    working_dir = \".\";\n    allowed_users = [\"alice\"];",
        ));
        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert_eq!(status.delta.agents_changed, vec![READER.to_string()]);
        assert_eq!(
            status.delta.sessions_retired,
            vec![format!("{READER} conv {}", bobs.conversation_id)],
            "the dropped user's session is retired",
        );
        assert!(
            booted
                .active_bridges
                .get(alices.conversation_id)
                .await
                .is_some(),
            "the user still named keeps theirs",
        );
        assert_eq!(
            pulses.try_recv(),
            Ok(()),
            "every WS loop is asked to re-check its user",
        );
    }

    /// The agent's singleton conversation, or `None` where nothing has minted
    /// one. Read through the owner the document names, as the bus path does.
    pub(crate) async fn conversation_of(booted: &Booted, slug: &str) -> Option<i64> {
        let owner = booted.messenger.app_table().load()[slug]
            .allowed_users
            .first()?
            .clone();
        let conn = booted.db.lock().await;
        let user = brenn_db::auth::user::get_user_by_username(&conn, &owner)?;
        brenn_db::conversation::get_singleton_conversation_id(&conn, user.id, slug)
    }

    /// Seat a user row and return its id. The bus path resolves an agent's
    /// owner through this table, and a bridge is keyed on it.
    pub(crate) async fn seat_user(db: &brenn_db::Db, username: &str) -> i64 {
        let conn = db.lock().await;
        brenn_db::auth::user::create_user(&conn, username, "$argon2id$fake")
    }

    /// A registered session for the reader agent, owned by `user_id`: what the
    /// commit's session step sweeps.
    pub(crate) async fn seat_bridge(
        booted: &Booted,
        user_id: i64,
    ) -> Arc<brenn_server::active_bridge::ActiveBridge> {
        let conversation_id = {
            let conn = booted.db.lock().await;
            brenn_db::conversation::create_conversation(&conn, user_id, READER, false)
        };
        let bridge = brenn_server::active_bridge::test_bridge_for_reload(
            booted.db.clone(),
            user_id,
            conversation_id,
            READER,
            booted.messenger.app_table(),
            booted.active_bridges.clone(),
        );
        booted
            .active_bridges
            .insert(conversation_id, bridge.clone())
            .await;
        bridge
    }

    /// A registered session for the reader agent on `conversation_id` — the
    /// conversation the agent's subscriptions hold their positions under — that
    /// can take a delivery: the messenger the drain reads through and a
    /// recording session in place of the CC process.
    ///
    /// The returned receiver is held by the caller for as long as the delivery
    /// matters: it is what keeps the recording session's channel open.
    pub(crate) async fn seat_receiving_bridge(
        booted: &Booted,
        user_id: i64,
        conversation_id: i64,
    ) -> (
        Arc<brenn_server::active_bridge::ActiveBridge>,
        brenn_server::active_bridge::RecordedSession,
    ) {
        let (bridge, recorded) = brenn_server::active_bridge::test_bridge_receiving_bus(
            booted.db.clone(),
            user_id,
            conversation_id,
            READER,
            booted.messenger.app_table(),
            booted.active_bridges.clone(),
            booted.messenger.clone(),
        )
        .await;
        booted
            .active_bridges
            .insert(conversation_id, bridge.clone())
            .await;
        (bridge, recorded)
    }

    /// The motivating shape, end to end: one reload adds an agent's
    /// push-enabled `subscribe` to a channel a consumer writes, and a publish
    /// through that consumer afterwards reaches the agent's conversation — its
    /// cursor passes the message the consumer produced.
    ///
    /// Every other case here asserts the wiring the reload leaves behind. This
    /// one asserts what the wiring is for, over the whole path: the agent's
    /// directive on the work channel, the consumer's answer on the sink
    /// channel, the wake walk that finds the agent owed it, the delivery into
    /// its conversation, and the position that moves past it.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_publish_through_the_consumer_reaches_the_agent_the_reload_subscribed() {
        let components = tempfile::tempdir().expect("a components root");
        let tree = Tree::holding(&consumer_and_push_subscriber(&[]));
        install_package_from(
            components.path(),
            &staged_module(&tree),
            "brenn_processor_config.wasm",
        );
        let mut booted = boot_with(
            &tree,
            BootFixture {
                components_roots: vec![components.path().to_path_buf()],
                // The consumer must receive its directive through the shared
                // dispatch loop for the end-to-end path to be exercised.
                dispatcher: true,
                ..BootFixture::default()
            },
        )
        .await;
        // The directive's own sender, seated first so it takes user id 1 as
        // `probe` assumes, and the agent's owner after it.
        seat_a_conversation(&booted.db, 1).await;
        let alice = seat_user(&booted.db, "alice").await;
        // Seated before the reload: the attach publishes the chat roster,
        // and with the dispatcher running that wakes the conversation. A
        // missing session would trigger a CC spawn this rig has no stand-in
        // for.
        let conversation = {
            let conn = booted.db.lock().await;
            brenn_db::conversation::get_or_create_singleton_conversation(&conn, alice, READER).id
        };
        let participant = brenn_lib::messaging::ParticipantId::for_conversation(conversation);
        let (_bridge, mut recorded) = seat_receiving_bridge(&booted, alice, conversation).await;

        tree.write(&consumer_and_push_subscriber(&["sink"]));
        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert_eq!(
            status.delta.subscriptions_added,
            vec![format!("{READER} brenn:sink")],
        );
        assert_eq!(
            conversation_of(&booted, READER).await,
            Some(conversation),
            "the attach seated the agent's own conversation",
        );
        let seated = cursor_of(&booted.messenger, "brenn:sink", &participant)
            .await
            .expect("with a position on the channel it now reads")
            .next_owed_seq;

        probe(&booted.messenger).await;
        assert_eq!(
            booted.bodies_until("brenn:sink", 1).await,
            vec!["answer".to_string()],
            "the consumer answered on the sink channel",
        );

        // The dispatcher is running, so the wake walk happens on its own; this
        // one is belt-and-braces, and harmless because the walk is idempotent.
        // What the case is about is what the walk finds — the wiring the reload
        // installed — not which caller ran it. The delivery task the router
        // spawns is asynchronous, so the advance is polled for.
        let mut advanced = None;
        for _ in 0..100 {
            booted
                .messenger
                .wake_owed_subscribers(chrono::Utc::now())
                .await;
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            let owed = cursor_of(&booted.messenger, "brenn:sink", &participant)
                .await
                .expect("the position is still there")
                .next_owed_seq;
            if owed > seated {
                advanced = Some(owed);
                break;
            }
        }
        assert!(
            advanced.is_some(),
            "the agent's conversation cursor moves past the consumer's message \
             (still at {seated})",
        );
        let texts = recorded.texts();
        assert!(
            texts.iter().any(|text| text.contains("answer")),
            "and the consumer's message is what the agent's process was handed: {texts:?}",
        );
    }

    /// The motivating shape's other half: a push-enabled `subscribe` added to
    /// an agent that had none mints its conversation, provisions that
    /// conversation's chat channel family into the live directory and gives it
    /// a position on the channel — the sequence a fresh boot of the candidate
    /// would run for the same entry.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_first_push_subscription_mints_the_agents_conversation_and_position() {
        let tree = Tree::holding(&document_push_subscribing("", &["alice"], &[]));
        let mut booted = boot(&tree, Vec::new()).await;
        seat_user(&booted.db, "alice").await;
        assert!(
            conversation_of(&booted, READER).await.is_none(),
            "the agent has no conversation before it reads anything",
        );

        tree.write(&document_push_subscribing("", &["alice"], &["work"]));
        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert_eq!(
            status.delta.subscriptions_added,
            vec![format!("{READER} brenn:work")]
        );
        let conversation = conversation_of(&booted, READER)
            .await
            .expect("the attach minted the agent's singleton conversation");
        assert!(
            cursor_of(
                &booted.messenger,
                "brenn:work",
                &brenn_lib::messaging::ParticipantId::for_conversation(conversation),
            )
            .await
            .is_some(),
            "and gave it a position on the channel it now reads",
        );
        assert!(
            booted.messenger.directory().list().iter().any(|entry| entry
                .address
                .contains(&format!("chat.app.{READER}.in.{conversation}"))),
            "the conversation's chat channel family is in the live directory: {:?}",
            booted
                .messenger
                .directory()
                .list()
                .iter()
                .map(|e| e.address.clone())
                .collect::<Vec<_>>(),
        );
    }

    /// And its inverse: the line removed takes the position with it — the
    /// orphan cursor a fresh boot's reconcile would reap — while the channel's
    /// retained messages stay where they are.
    #[tokio::test(flavor = "multi_thread")]
    async fn removing_a_push_subscription_deletes_the_agents_cursor() {
        let tree = Tree::holding(&document_push_subscribing("", &["alice"], &["work"]));
        let mut booted = boot(&tree, Vec::new()).await;
        seat_user(&booted.db, "alice").await;
        // Boot's own attach runs before the user row exists in this rig, so the
        // position is minted by a reload that re-states the same document.
        booted
            .messenger
            .attach_conversation(
                "brenn:work",
                READER,
                brenn_lib::messaging::config::Depth::Bounded(1),
            )
            .await;
        let conversation = conversation_of(&booted, READER)
            .await
            .expect("the agent has a conversation");
        let participant = brenn_lib::messaging::ParticipantId::for_conversation(conversation);
        assert!(
            cursor_of(&booted.messenger, "brenn:work", &participant)
                .await
                .is_some(),
        );

        tree.write(&document_push_subscribing("", &["alice"], &[]));
        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert!(
            cursor_of(&booted.messenger, "brenn:work", &participant)
                .await
                .is_none(),
            "a genuine removal deletes the cursor row",
        );
        assert!(
            booted.messenger.directory().resolve("brenn:work").is_some(),
            "the channel and its retained messages are untouched",
        );
    }

    /// The owner moved: positions are held under the owner's conversation, so
    /// the new owner has none until every push-enabled entry is re-attached —
    /// not only the ones this reload moved, of which there are none here.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_owner_change_attaches_the_new_owners_conversation() {
        let tree = Tree::holding(&document_push_subscribing("", &["alice"], &["work"]));
        let mut booted = boot(&tree, Vec::new()).await;
        seat_user(&booted.db, "alice").await;
        seat_user(&booted.db, "bob").await;
        booted
            .messenger
            .attach_conversation(
                "brenn:work",
                READER,
                brenn_lib::messaging::config::Depth::Bounded(1),
            )
            .await;
        let alices = conversation_of(&booted, READER)
            .await
            .expect("alice's conversation");
        let alices_participant = brenn_lib::messaging::ParticipantId::for_conversation(alices);

        tree.write(&document_push_subscribing("", &["bob"], &["work"]));
        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert!(
            status.delta.subscriptions_added.is_empty(),
            "no subscription moved: what moved is whose conversation holds them",
        );
        let bobs = conversation_of(&booted, READER)
            .await
            .expect("the new owner's conversation was minted");
        assert_ne!(bobs, alices, "the owner is a different conversation");
        assert!(
            cursor_of(
                &booted.messenger,
                "brenn:work",
                &brenn_lib::messaging::ParticipantId::for_conversation(bobs),
            )
            .await
            .is_some(),
            "with a position on every push-enabled channel",
        );
        assert!(
            cursor_of(&booted.messenger, "brenn:work", &alices_participant)
                .await
                .is_none(),
            "and the old owner's position on the agent's channel is reaped, as a fresh boot's \
             reconcile reaps it",
        );
        assert!(
            conversation_row_exists(&booted, alices).await,
            "its conversation and chat history are not the reload's to delete",
        );
        assert!(
            booted
                .messenger
                .directory()
                .list()
                .iter()
                .any(|entry| entry.address.contains(&format!(".{READER}.in.{alices}"))),
            "nor its chat channel family, which its own entries justify",
        );
    }

    /// The same owner change with a live push-enabled dynamic row: boot seats
    /// the new owner on every push-enabled `App` entry the directory holds,
    /// dynamic ones included, so the reload has to walk the live directory
    /// rather than the candidate plan's static entries.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_owner_change_seats_the_new_owner_on_a_dynamic_channel_too() {
        let tree = Tree::holding(&push_owner_covering_work(&["alice"]));
        let mut booted = boot(&tree, Vec::new()).await;
        seat_user(&booted.db, "alice").await;
        seat_user(&booted.db, "bob").await;
        let uuid = insert_push_dynamic_row(&booted, "brenn:work").await;
        booted
            .messenger
            .attach_conversation(
                "brenn:work",
                READER,
                brenn_lib::messaging::config::Depth::Bounded(1),
            )
            .await;
        let alices = conversation_of(&booted, READER)
            .await
            .expect("alice's conversation");
        let alices_participant = brenn_lib::messaging::ParticipantId::for_conversation(alices);

        tree.write(&push_owner_covering_work(&["bob"]));
        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        let bobs = conversation_of(&booted, READER)
            .await
            .expect("the new owner's conversation was minted");
        assert!(
            cursor_of(
                &booted.messenger,
                "brenn:work",
                &brenn_lib::messaging::ParticipantId::for_conversation(bobs),
            )
            .await
            .is_some(),
            "the new owner is positioned on the dynamic channel, as a fresh boot positions it",
        );
        assert!(
            cursor_of(&booted.messenger, "brenn:work", &alices_participant)
                .await
                .is_none(),
            "and the old owner's position on it is reaped",
        );
        let rows = dynamic_rows(&booted).await;
        assert_eq!(
            rows.iter()
                .map(|row| (row.channel_uuid, row.app_slug.clone()))
                .collect::<Vec<_>>(),
            vec![(uuid, READER.to_string())],
            "the row itself is untouched: what moved is whose conversation holds its position",
        );
    }

    /// An owner change with a position on a channel the directory no longer
    /// holds — a dormant dynamic row whose `[[channel]]` block is gone, which a
    /// restart between the two edits leaves behind. The reap reaches it through
    /// the cursor table rather than through the directory, so it neither
    /// survives nor panics.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_owner_change_reaps_a_position_on_an_undeclared_channel() {
        let tree = Tree::holding(&document_push_subscribing("", &["alice"], &["work"]));
        let mut booted = boot(&tree, Vec::new()).await;
        seat_user(&booted.db, "alice").await;
        seat_user(&booted.db, "bob").await;
        booted
            .messenger
            .attach_conversation(
                "brenn:work",
                READER,
                brenn_lib::messaging::config::Depth::Bounded(1),
            )
            .await;
        let alices = conversation_of(&booted, READER)
            .await
            .expect("alice's conversation");
        let alices_participant = brenn_lib::messaging::ParticipantId::for_conversation(alices);
        let ghost = seat_undeclared_cursor(&booted, &alices_participant).await;

        tree.write(&document_push_subscribing("", &["bob"], &["work"]));
        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        let conn = booted.db.lock().await;
        assert!(
            brenn_messaging_store::db::load_subscriber_cursor(&conn, ghost, &alices_participant)
                .is_none(),
            "the row on the undeclared channel is reaped through the cursor table",
        );
    }

    /// A push-subscribing owner document whose ACL covers `work` without
    /// declaring a static `subscribe` on it: what a dynamic row on that channel
    /// needs to stay kept rather than be revoked.
    pub(crate) fn push_owner_covering_work(owner: &[&str]) -> String {
        document_push_subscribing_acl("", owner, &[], &[], &["exact work"])
    }

    /// Whether the conversation row is still there — the reap deletes positions,
    /// never conversations.
    async fn conversation_row_exists(booted: &Booted, conversation: i64) -> bool {
        let conn = booted.db.lock().await;
        brenn_db::conversation::get_conversation_opt(&conn, conversation).is_some()
    }

    /// One position for `participant` on `uuid`, at the depth a push-enabled
    /// dynamic subscription holds: what a dormant row resumes from, and the row
    /// a fresh boot's reconcile keeps because the dormant row justifies it.
    pub(crate) async fn seat_position(
        booted: &Booted,
        uuid: uuid::Uuid,
        participant: &brenn_lib::messaging::ParticipantId,
    ) {
        let conn = booted.db.lock().await;
        brenn_messaging_store::db::ensure_subscriber_cursor(
            &conn,
            uuid,
            participant,
            READER,
            brenn_lib::messaging::config::Depth::Bounded(1),
            0,
        );
    }

    /// A durable channel row with no directory entry, carrying one position for
    /// `participant`: what a removed `[[channel]]` block leaves behind for a
    /// dormant dynamic subscription.
    ///
    /// Returns the channel uuid.
    async fn seat_undeclared_cursor(
        booted: &Booted,
        participant: &brenn_lib::messaging::ParticipantId,
    ) -> uuid::Uuid {
        let entry = booted
            .messenger
            .directory()
            .resolve("brenn:work")
            .expect("the channel is declared");
        let mut undeclared = (*entry).clone();
        undeclared.uuid = uuid::Uuid::new_v4();
        undeclared.address = "brenn:gone".to_string();
        {
            let conn = booted.db.lock().await;
            brenn_messaging_store::db::upsert_channels(&conn, std::slice::from_ref(&undeclared));
        }
        seat_position(booted, undeclared.uuid, participant).await;
        undeclared.uuid
    }

    /// A per-process edit with two live sessions: the idle one dies at the
    /// swap, the busy one is named pending and dies at its turn end.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_per_process_edit_retires_the_idle_session_and_defers_the_busy_one() {
        let tree = Tree::holding(&document(""));
        let mut booted = boot(&tree, Vec::new()).await;
        let user = seat_user(&booted.db, "alice").await;
        let idle = seat_bridge(&booted, user).await;
        let busy = seat_bridge(&booted, user).await;
        busy.set_cc_idle_for_test(false);

        tree.write(&document("").replace(
            "    working_dir = \".\";",
            "    working_dir = \".\";\n    model = \"sonnet\";",
        ));
        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert_eq!(status.delta.agents_changed, vec![READER.to_string()]);
        assert_eq!(
            status.delta.sessions_retired,
            vec![format!("{READER} conv {}", idle.conversation_id)],
        );
        assert_eq!(
            status.delta.sessions_retire_pending,
            vec![format!("{READER} conv {}", busy.conversation_id)],
        );
        assert!(
            booted
                .active_bridges
                .get(idle.conversation_id)
                .await
                .is_none(),
            "the idle session's process is gone",
        );
        assert!(
            booted
                .active_bridges
                .get(busy.conversation_id)
                .await
                .is_some(),
            "the busy one finishes its turn first",
        );
    }

    /// An agent restricted from open-to-all to one user: the document names no
    /// removed user, and every other user's session is still severed.
    #[tokio::test(flavor = "multi_thread")]
    async fn restricting_an_open_agent_retires_the_sessions_it_now_denies() {
        let tree = Tree::holding(&document(""));
        let mut booted = boot(&tree, Vec::new()).await;
        let alice = seat_user(&booted.db, "alice").await;
        let bob = seat_user(&booted.db, "bob").await;
        let alices = seat_bridge(&booted, alice).await;
        let bobs = seat_bridge(&booted, bob).await;
        let mut pulses = booted.apps_swapped_tx.subscribe();

        tree.write(&document("").replace(
            "    working_dir = \".\";",
            "    working_dir = \".\";\n    allowed_users = [\"alice\"];",
        ));
        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert_eq!(
            status.delta.sessions_retired,
            vec![format!("{READER} conv {}", bobs.conversation_id)],
            "the user the candidate no longer allows loses their session",
        );
        assert!(
            booted
                .active_bridges
                .get(alices.conversation_id)
                .await
                .is_some(),
            "the user it names keeps theirs",
        );
        assert_eq!(
            pulses.try_recv(),
            Ok(()),
            "and every open socket is asked to re-check its user",
        );
    }

    /// Prepare measures the `applied` body with the session lists empty — the
    /// live bridge set is a fact about the process a moment later — and commit
    /// then fills them. On a host with many sessions of a changed agent that
    /// body would be published oversize and rejected, which reads to a bundle
    /// installer as a failed reload over a successful one. The names give way,
    /// the outcome does not.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_swarm_of_retired_sessions_does_not_push_the_applied_body_oversize() {
        const LIMIT: &str = "\nmessaging {\n    max_body_bytes = 1500;\n}\n";
        let tree = Tree::holding(&document(LIMIT));
        let mut booted = boot(&tree, Vec::new()).await;
        let user = seat_user(&booted.db, "alice").await;
        for _ in 0..30 {
            seat_bridge(&booted, user).await;
        }

        tree.write(&document(LIMIT).replace(
            "    working_dir = \".\";",
            "    working_dir = \".\";\n    model = \"sonnet\";",
        ));
        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert_eq!(
            status.delta.sessions_retired,
            vec!["30 sessions retired; names omitted".to_string()],
            "the names gave way so the outcome could be published",
        );
    }

    /// The staging step's own refusal: a state directory that cannot be
    /// written is an environment refusal in prepare, with nothing touched.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_tool_list_that_cannot_be_staged_is_an_environment_refusal() {
        use std::os::unix::fs::PermissionsExt;

        const BROKER: &str =
            "mqtt_client ha {\n    url = \"mqtts://127.0.0.1:8883\";\n    qos = 1;\n}\n";
        let tree = Tree::holding(&document(BROKER));
        let mut booted = boot(&tree, Vec::new()).await;
        let before = booted.messenger.app_table().load();
        let state_dir = before[READER]
            .virtual_tools_path()
            .parent()
            .expect("the file sits in the agent's state directory")
            .to_path_buf();
        let original = std::fs::metadata(&state_dir)
            .expect("the state directory exists")
            .permissions();
        std::fs::set_permissions(&state_dir, std::fs::Permissions::from_mode(0o555))
            .expect("the directory is ours to lock");

        tree.write(&document(BROKER).replace(
            "acl publish [exact reload_requests, exact work];",
            "acl publish [exact reload_requests, exact work, client \"mqtt:ha\"];",
        ));
        booted.driver.reload(TriggerSource::Signal).await;
        std::fs::set_permissions(&state_dir, original).expect("and ours to unlock");

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Refused, "{:?}", status.refusals);
        assert!(
            status.refusals.iter().any(|refusal| refusal
                .contains("writing the new virtual tools list")
                && refusal.contains(READER)),
            "the refusal names the agent and what could not be written: {:?}",
            status.refusals,
        );
        // Untouched: the swap never ran, so the booted policy is still what
        // every gate reads.
        assert!(
            booted.messenger.app_table().load()[READER]
                .policy
                .acls
                .mqtt_publish
                .is_empty(),
            "the candidate's mqtt publish matcher never reached the table",
        );
    }

    /// A refusal after prepare has staged a tool list leaves nothing beside the
    /// running agent's file: a surviving `.next` would be renamed onto the live
    /// path by the next commit that touches this agent, handing a successor
    /// process a tool list from a document that was refused.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_refused_reload_leaves_no_staged_tool_list() {
        const BROKER: &str =
            "mqtt_client ha {\n    url = \"mqtts://127.0.0.1:8883\";\n    qos = 1;\n}\n";
        let tree = Tree::holding(&document(BROKER));
        let mut booted = boot(&tree, Vec::new()).await;
        let before = booted.messenger.app_table().load();
        let path = before[READER].virtual_tools_path();
        let staged = super::super::agents::staged_virtual_tools_path(&before[READER]);
        let booted_rendering = brenn_server::active_bridge::render_virtual_tools(
            &before[READER],
            booted.driver.env().tool_registry.as_ref(),
        );
        std::fs::write(&path, &booted_rendering).expect("the rig writes the live file");

        tree.write(&document(BROKER).replace(
            "acl publish [exact reload_requests, exact work];",
            "acl publish [exact reload_requests, exact work, client \"mqtt:ha\"];",
        ));
        let dynamic = booted.driver.dynamic_snapshot().await;
        let ready = match booted.driver.prepare(TriggerSource::Signal, &dynamic) {
            Prepared::Ready(ready) => ready,
            other => panic!("the candidate is applicable: {}", outcome_of(&other)),
        };

        // The window, and what refuses this reload at commit after prepare has
        // already staged the candidate's rendering: the agent subscribes
        // dynamically while prepare is working, so the set commit would act on
        // is not the one the re-merge classified.
        insert_dynamic_row(&booted, "brenn:work", false).await;

        booted.driver.commit(TriggerSource::Signal, *ready).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Refused, "{:?}", status.refusals);
        assert_eq!(
            std::fs::read_to_string(&path).expect("the live file is still there"),
            booted_rendering,
            "the running agent's tool list is untouched",
        );
        assert!(
            !staged.exists(),
            "and the staged one was discarded: {}",
            staged.display(),
        );
    }

    /// The base document with the reader's subscribe ACL widened to cover the
    /// work channel — the shape a dynamic subscription to it needs.
    pub(crate) fn document_covering_work(extra: &str) -> String {
        document_subscribing_acl(extra, &[], &["exact work"])
    }

    /// Give the reader a durable dynamic subscription to `address`, as a
    /// runtime `MessageSubscribe` would have left it: the row in the table, and
    /// — when `folded` — the subscriber entry in the live directory.
    pub(crate) async fn insert_dynamic_row(
        booted: &Booted,
        address: &str,
        folded: bool,
    ) -> uuid::Uuid {
        insert_dynamic_row_at(
            booted,
            address,
            folded,
            brenn_lib::messaging::config::Depth::Bounded(0),
            None,
        )
        .await
    }

    /// The same, push-enabled and folded: a dynamic subscription that holds a
    /// position, which is what an owner change has to move.
    pub(crate) async fn insert_push_dynamic_row(booted: &Booted, address: &str) -> uuid::Uuid {
        insert_dynamic_row_at(
            booted,
            address,
            true,
            brenn_lib::messaging::config::Depth::Bounded(1),
            None,
        )
        .await
    }

    /// The same on an `mqtt:` channel: the row carries the SUBSCRIBE QoS.
    /// Boot and reload both require it when re-asserting the broker filter.
    pub(crate) async fn insert_dynamic_mqtt_row(
        booted: &Booted,
        address: &str,
        folded: bool,
        qos: u8,
    ) -> uuid::Uuid {
        insert_dynamic_row_at(
            booted,
            address,
            folded,
            brenn_lib::messaging::config::Depth::Bounded(0),
            Some(qos),
        )
        .await
    }

    async fn insert_dynamic_row_at(
        booted: &Booted,
        address: &str,
        folded: bool,
        push_depth: brenn_lib::messaging::config::Depth,
        qos: Option<u8>,
    ) -> uuid::Uuid {
        let uuid = booted
            .messenger
            .directory()
            .resolve(address)
            .expect("the channel is declared")
            .uuid;
        {
            let conn = booted.db.lock().await;
            brenn_messaging_store::db::insert_dynamic_subscription(
                &conn,
                &brenn_lib::messaging::DynamicSubscriptionRow {
                    channel_uuid: uuid,
                    app_slug: READER.to_string(),
                    push_depth,
                    retain_depth: brenn_lib::messaging::config::Depth::Bounded(2),
                    noise: brenn_lib::messaging::config::NoiseLevel::Silent,
                    wake_min: brenn_lib::messaging::WakeMin::Never,
                    qos,
                    created_at: "2026-09-07T00:00:00Z".to_string(),
                },
            );
        }
        if folded {
            assert!(booted.messenger.directory().add_subscriber(
                &uuid,
                brenn_lib::messaging::SubscriberEntry {
                    kind: SubscriberEntryKind::App(READER.to_string()),
                    push_depth,
                    retain_depth: brenn_lib::messaging::config::Depth::Bounded(2),
                    noise: brenn_lib::messaging::config::NoiseLevel::Silent,
                    wake_min: Some(brenn_lib::messaging::WakeMin::Never),
                },
            ));
        }
        uuid
    }

    /// The reader's subscriber entry on `address`, as the live directory holds
    /// it now.
    pub(crate) fn live_entry_of_reader(
        booted: &Booted,
        address: &str,
    ) -> Option<brenn_lib::messaging::SubscriberEntry> {
        booted
            .messenger
            .directory()
            .resolve(address)
            .expect("the channel is declared")
            .subscribers
            .iter()
            .find(|subscriber| {
                matches!(&subscriber.kind, SubscriberEntryKind::App(slug) if slug == READER)
            })
            .cloned()
    }

    /// Every durable dynamic row the store holds.
    pub(crate) async fn dynamic_rows(
        booted: &Booted,
    ) -> Vec<brenn_lib::messaging::DynamicSubscriptionRow> {
        let conn = booted.db.lock().await;
        brenn_messaging_store::db::load_dynamic_subscriptions(&conn)
    }

    /// An ACL narrowed under a live dynamic subscription: the entry is folded
    /// out and the durable row is kept, which is the dormant state a fresh boot
    /// of the same document would put it in.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_acl_narrowed_under_a_dynamic_subscription_revokes_it_to_dormant() {
        let tree = Tree::holding(&document_covering_work(""));
        let mut booted = boot(&tree, Vec::new()).await;
        insert_dynamic_row(&booted, "brenn:work", true).await;

        tree.write(&document(""));
        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert_eq!(
            status.delta.dynamic_revoked,
            vec![format!("{READER} brenn:work")],
        );
        assert!(
            live_entry_of_reader(&booted, "brenn:work").is_none(),
            "the revoked subscription is still folded",
        );
        assert_eq!(
            dynamic_rows(&booted).await.len(),
            1,
            "the durable row is kept, so the subscription resumes if the ACL comes back",
        );
        // And the state the reload left is the one a restart leaves: the
        // agent's next `MessageSubscribe` on the address is told to
        // unsubscribe first rather than colliding with the row.
        let err = booted
            .messenger
            .subscribe_dynamic(
                READER,
                "brenn:work",
                brenn_messaging::subscribe::DynamicSubscribeParams {
                    push_depth: brenn_lib::messaging::config::Depth::Bounded(0),
                    retain_depth: brenn_lib::messaging::config::Depth::Bounded(2),
                    noise: None,
                    wake_min: None,
                    qos: None,
                },
            )
            .await
            .expect_err("the dormant row is in the way");
        assert!(
            matches!(
                err,
                brenn_messaging::subscribe::RuntimeSubscribeError::DormantSubscriptionExists { .. }
            ),
            "{err:?}",
        );
    }

    /// The mirror: a dormant row the candidate authorizes again is folded back
    /// in at the depths it was granted, not at any the document names.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_acl_widened_over_a_dormant_row_revives_it_at_its_own_depths() {
        let tree = Tree::holding(&document(""));
        let mut booted = boot(&tree, Vec::new()).await;
        insert_dynamic_row(&booted, "brenn:work", false).await;

        tree.write(&document_covering_work(""));
        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert_eq!(
            status.delta.dynamic_revived,
            vec![format!("{READER} brenn:work")],
        );
        let entry = live_entry_of_reader(&booted, "brenn:work").expect("the subscription is back");
        assert_eq!(
            entry.retain_depth,
            brenn_lib::messaging::config::Depth::Bounded(2),
            "the row's own depth, not the document's",
        );
    }

    /// Static config wins: a `subscribe` line declared where a dynamic row
    /// already sits deletes the row and replaces the entry with the document's.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_static_subscription_declared_over_a_dynamic_row_prunes_it() {
        let tree = Tree::holding(&document_covering_work(""));
        let mut booted = boot(&tree, Vec::new()).await;
        insert_dynamic_row(&booted, "brenn:work", true).await;

        tree.write(&document_subscribing("", &["work"]));
        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert_eq!(
            status.delta.dynamic_pruned,
            vec![format!("{READER} brenn:work")],
        );
        assert!(
            dynamic_rows(&booted).await.is_empty(),
            "the row the static declaration overrides is gone",
        );
        let entry = live_entry_of_reader(&booted, "brenn:work").expect("the static entry is there");
        assert_eq!(
            entry.retain_depth,
            brenn_lib::messaging::config::Depth::Bounded(4),
            "the document's depth, which is what a fresh boot would fold",
        );
    }

    /// The document with the reader authorized on the base document's
    /// `ephemeral:` channel — the non-durable shape, where a dynamic
    /// subscription is an in-memory registration and no durable row.
    fn document_covering_scratch(extra: &str) -> String {
        document_subscribing_acl(extra, &[], &["exact scratch"])
    }

    /// Give the reader a non-durable dynamic subscription to `address` the way
    /// a runtime `MessageSubscribe` does: through the messenger, so the
    /// registration set and the directory entry are both what the live path
    /// left.
    async fn subscribe_nondurable(booted: &Booted, address: &str) {
        booted
            .messenger
            .subscribe_dynamic(
                READER,
                address,
                brenn_messaging::subscribe::DynamicSubscribeParams {
                    push_depth: brenn_lib::messaging::config::Depth::Bounded(0),
                    retain_depth: brenn_lib::messaging::config::Depth::Bounded(2),
                    noise: None,
                    wake_min: None,
                    qos: None,
                },
            )
            .await
            .expect("the channel is declared and the policy covers it");
        assert_eq!(
            booted.messenger.nondurable_dynamic_subs().len(),
            1,
            "a non-durable channel keeps its dynamic subscription in memory",
        );
    }

    /// An ACL narrowed under a *non-durable* dynamic subscription: the entry is
    /// folded out and the in-memory registration goes with it, since there is
    /// no row to keep dormant and a restart would have lost it anyway.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_acl_narrowed_under_a_nondurable_dynamic_subscription_revokes_it() {
        let tree = Tree::holding(&document_covering_scratch(""));
        let mut booted = boot(&tree, Vec::new()).await;
        subscribe_nondurable(&booted, "ephemeral:scratch").await;

        tree.write(&document(""));
        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert_eq!(
            status.delta.dynamic_revoked,
            vec![format!("{READER} ephemeral:scratch")],
        );
        assert!(
            live_entry_of_reader(&booted, "ephemeral:scratch").is_none(),
            "the revoked subscription is still folded",
        );
        assert!(
            booted.messenger.nondurable_dynamic_subs().is_empty(),
            "and the registration went with it, so the agent's next subscribe is a fresh one \
             rather than a collision with a registration the candidate denies",
        );
        assert!(
            dynamic_rows(&booted).await.is_empty(),
            "a non-durable channel never had a row",
        );
    }

    /// The same registration, replaced by a static declaration: the prune arm's
    /// non-durable branch. Nothing is deleted from the durable table — there
    /// was never a row — and the registration is dropped so the document's
    /// entry is the only subscription on the channel.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_static_subscription_declared_over_a_nondurable_registration_prunes_it() {
        let tree = Tree::holding(&document_covering_scratch(""));
        let mut booted = boot(&tree, Vec::new()).await;
        subscribe_nondurable(&booted, "ephemeral:scratch").await;

        tree.write(&document_subscribing("", &["scratch"]));
        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert_eq!(
            status.delta.dynamic_pruned,
            vec![format!("{READER} ephemeral:scratch")],
        );
        assert!(
            booted.messenger.nondurable_dynamic_subs().is_empty(),
            "the registration the static declaration overrides is gone",
        );
        let entry =
            live_entry_of_reader(&booted, "ephemeral:scratch").expect("the static entry is there");
        assert_eq!(
            entry.retain_depth,
            brenn_lib::messaging::config::Depth::Bounded(4),
            "the document's depth, which is what a fresh boot would fold",
        );
    }

    /// A second durable channel, for the two cases that need one this reload
    /// can remove or retune: every channel the base document declares is
    /// load-bearing for the facility itself.
    ///
    /// `standing` is what the retune moves. The stated depths are at the floor
    /// so that lowering standing to 1 is a legal document — standing is the
    /// ceiling on every depth a channel states.
    pub(crate) fn spill_channel(standing: u64) -> String {
        format!(
            r#"
channel spill at "brenn:spill" {{
    push_depth = 1;
    retain_depth = 1;
    standing_retain_depth = {standing};
    wake_min = never;
}}
"#
        )
    }

    /// The subscribe-ACL clause covering [`spill_channel`] for a candidate
    /// that *removes* the channel: by address, because there is no handle left
    /// to name — and the compiler insists on the handle wherever there is one.
    pub(crate) const SPILL_ACL_BY_ADDRESS: &str = r#"exact "brenn:spill""#;

    /// A dormant durable row on a channel this reload *removes* is left where
    /// a fresh boot of the candidate leaves it: dormant, row and position kept.
    ///
    /// This is the two-step retirement of a channel an agent subscribed to
    /// dynamically — narrow the ACL (the row goes dormant), then remove the
    /// block — and the second step must not need a restart. A fresh boot of the
    /// candidate finds the channel's store row but no `[[channel]]` block
    /// declaring it, so it mints no entry and holds the row dormant with its
    /// cursor; the commit's channel walk keeps the durable row too, so touching
    /// nothing reproduces that exactly. The ACL is re-granted in the same
    /// document, which is what would otherwise classify the row `revive` off
    /// the entry still in the directory at prepare.
    ///
    /// The position is seeded as well as the row: without it the cursor
    /// assertion below, and the oracle transition beside it, compare an empty
    /// set on both sides and prove nothing.
    #[tokio::test(flavor = "multi_thread")]
    #[traced_test]
    async fn a_dormant_row_on_a_removed_channel_is_left_dormant() {
        let tree = Tree::holding(&document_push_subscribing_acl(
            &spill_channel(4),
            &["alice"],
            &["work"],
            &[],
            &[],
        ));
        let mut booted = boot(&tree, Vec::new()).await;
        seat_user(&booted.db, "alice").await;
        // The agent's conversation: what `app_conversation` resolves the
        // dormant row's position through, on both sides of the comparison.
        booted
            .messenger
            .attach_conversation(
                "brenn:work",
                READER,
                brenn_lib::messaging::config::Depth::Bounded(1),
            )
            .await;
        let conversation = conversation_of(&booted, READER)
            .await
            .expect("alice's conversation");
        let participant = brenn_lib::messaging::ParticipantId::for_conversation(conversation);
        let spill_uuid = insert_dynamic_row(&booted, "brenn:spill", false).await;
        seat_position(&booted, spill_uuid, &participant).await;

        // The cleanup edit: the ACL re-granted and the block dropped at once.
        let candidate =
            document_push_subscribing_acl("", &["alice"], &["work"], &[], &[SPILL_ACL_BY_ADDRESS]);
        tree.write(&candidate);
        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert!(
            booted
                .messenger
                .directory()
                .resolve("brenn:spill")
                .is_none(),
            "the channel the document no longer declares is out of the directory",
        );
        assert_eq!(
            dynamic_rows(&booted).await.len(),
            1,
            "the durable row is retained, which is what dormancy is",
        );
        {
            let conn = booted.db.lock().await;
            assert!(
                brenn_messaging_store::db::load_subscriber_cursor(&conn, spill_uuid, &participant)
                    .is_some(),
                "and so is the position it resumes from",
            );
        }
        assert!(
            status.delta.dynamic_revived.is_empty()
                && status.delta.dynamic_revoked.is_empty()
                && status.delta.dynamic_pruned.is_empty(),
            "nothing moved, so the re-merge names nothing: {:?}",
            status.delta,
        );

        // The journal is the whole operator-facing surface of the carve-out —
        // the status body deliberately names nothing — so it is asserted here
        // rather than left to a refactor of the channel walk to drop.
        assert!(
            logs_contain("dynamic subscription dormant"),
            "the removal walk journals the row it left dormant",
        );
        assert!(
            logs_contain("brenn:spill") && logs_contain(READER),
            "naming the channel and the agent",
        );

        // And the reload converged: the same document again is a no-op.
        tree.write(&candidate);
        booted.driver.reload(TriggerSource::Signal).await;
        assert_eq!(
            booted.last_status().await.outcome,
            Outcome::Unchanged,
            "{:?}",
            booted.last_status().await.refusals,
        );

        // Re-declaring the block is the state `TODO(reload-revive-on-redeclared-
        // channel)` tracks: the entry comes back, the row stays dormant where a
        // fresh boot of the same document would fold it, and the operator is
        // told so at the one moment they are looking. The ACL is spelled by
        // handle again, because the compiler insists on the handle wherever a
        // `channel` block declares one.
        tree.write(&document_push_subscribing_acl(
            &spill_channel(4),
            &["alice"],
            &["work"],
            &[],
            &["exact spill"],
        ));
        booted.driver.reload(TriggerSource::Signal).await;
        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert!(
            booted
                .messenger
                .directory()
                .resolve("brenn:spill")
                .is_some(),
            "the re-declared channel is back in the directory",
        );
        assert_eq!(
            dynamic_rows(&booted).await.len(),
            1,
            "the row is still there, and still dormant",
        );
        assert!(
            live_entry_of_reader(&booted, "brenn:spill").is_none(),
            "and it is not folded onto the arriving entry: nothing re-classifies \
             after the channel walk",
        );
        assert!(
            status.delta.dynamic_revived.is_empty(),
            "which is why the re-merge names no revival: {:?}",
            status.delta,
        );
        assert!(
            logs_contain("still dormant"),
            "the arrival walk journals that delivery does not resume yet",
        );
    }

    /// The same re-declaration, with the document also declaring a *static*
    /// subscription for the pair: the row is pruned, as boot's rule 3 prunes it.
    ///
    /// This is the arm the re-merge does answer on an arriving channel, and it
    /// has to: leaving the row beside the static entry puts the process in the
    /// one pairing the runtime treats as impossible — a directory subscriber
    /// with a durable row behind it (`RuntimeUnsubscribeError::
    /// StaticSubscription`'s "structurally unreachable" invariant).
    #[tokio::test(flavor = "multi_thread")]
    #[traced_test]
    async fn a_static_subscribe_on_a_redeclared_channel_prunes_the_dormant_row() {
        let tree = Tree::holding(&document_push_subscribing_acl(
            &spill_channel(4),
            &["alice"],
            &["work"],
            &[],
            &[],
        ));
        let mut booted = boot(&tree, Vec::new()).await;
        seat_user(&booted.db, "alice").await;
        booted
            .messenger
            .attach_conversation(
                "brenn:work",
                READER,
                brenn_lib::messaging::config::Depth::Bounded(1),
            )
            .await;
        let conversation = conversation_of(&booted, READER)
            .await
            .expect("alice's conversation");
        let participant = brenn_lib::messaging::ParticipantId::for_conversation(conversation);
        let spill_uuid = insert_dynamic_row(&booted, "brenn:spill", false).await;
        seat_position(&booted, spill_uuid, &participant).await;

        // Step one of the pair: the block goes, the row is left dormant.
        tree.write(&document_push_subscribing_acl(
            "",
            &["alice"],
            &["work"],
            &[],
            &[SPILL_ACL_BY_ADDRESS],
        ));
        booted.driver.reload(TriggerSource::Signal).await;
        assert_eq!(booted.last_status().await.outcome, Outcome::Applied);

        // Step two: the block comes back and the agent subscribes to it in the
        // document.
        tree.write(&document_push_subscribing_acl(
            &spill_channel(4),
            &["alice"],
            &["work", "spill"],
            &[],
            &[],
        ));
        booted.driver.reload(TriggerSource::Signal).await;
        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert!(
            dynamic_rows(&booted).await.is_empty(),
            "static config wins: the row is deleted, as a fresh boot deletes it",
        );
        assert_eq!(
            status.delta.dynamic_pruned,
            vec![format!("{READER} brenn:spill")],
            "and the status body names it: {:?}",
            status.delta,
        );
        assert!(
            live_entry_of_reader(&booted, "brenn:spill").is_some(),
            "the static entry the document declares is what serves the channel now",
        );
        assert!(
            !logs_contain("still dormant"),
            "so nothing is left dormant to journal",
        );
    }

    /// A candidate that declares an address under a uuid other than the one its
    /// store row carries is refused, with the running document still in force.
    ///
    /// `messaging_channels.address` is unique and a channel's row is never
    /// deleted by a reload, so there is nowhere for such a declaration to
    /// write: the insert is refused by the index, the update by uuid matches
    /// nothing, and the directory would hold a channel with no row until the
    /// first publish failed the foreign key. Boot answers this with a panic in
    /// the store; the reload has a door and uses it.
    ///
    /// A declared durable channel derives its uuid from its address, so
    /// reaching this needs a `uuid_pins` entry moving one — the shape an
    /// operator writes when they re-declare a removed channel and mint a fresh
    /// uuid rather than reusing the row's. Two reloads, because that is the
    /// only way there: while the block is still declared, the address is in the
    /// live directory and the arriving entry is refused earlier, as a channel
    /// "newly minted but already exists".
    #[tokio::test(flavor = "multi_thread")]
    async fn a_channel_declared_under_another_rows_uuid_is_refused() {
        let tree = Tree::holding(&document(&spill_channel(4)));
        let mut booted = boot(&tree, Vec::new()).await;
        let derived = booted
            .messenger
            .directory()
            .resolve("brenn:spill")
            .expect("the declared channel")
            .uuid;

        // The block goes first, which leaves the store row behind — that is
        // what makes the address free in the directory and taken in the table.
        tree.write(&document(""));
        booted.driver.reload(TriggerSource::Signal).await;
        assert_eq!(booted.last_status().await.outcome, Outcome::Applied);

        let pinned = uuid::Uuid::new_v4();
        tree.write(&document(&format!(
            "{}\nuuid_pins {{\n    \"brenn:spill\" = \"{pinned}\";\n}}\n",
            spill_channel(4),
        )));
        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Refused, "{:?}", status.refusals);
        assert!(
            status
                .refusals
                .iter()
                .any(|refusal| refusal.contains("brenn:spill")
                    && refusal.contains("already belongs")
                    && refusal.contains(&derived.to_string())),
            "naming the address and the uuid its row carries: {:?}",
            status.refusals,
        );
        assert!(
            booted
                .messenger
                .directory()
                .resolve("brenn:spill")
                .is_none(),
            "and the running process still projects the document it applied — the \
             one with no spill block",
        );
        {
            let conn = booted.db.lock().await;
            assert_eq!(
                brenn_messaging_store::db::channel_uuid_by_address(&conn, "brenn:spill")
                    .expect("the channel table reads"),
                Some(derived),
                "the row the pin collided with is untouched",
            );
        }
    }

    /// The same question on a channel this reload *retunes*.
    ///
    /// The uuid survives a retune, so the fold would succeed here — onto an
    /// entry whose standing depth the conformance gate never read. The row is
    /// granted more retain depth than the candidate stands behind, so a fresh
    /// boot of this document holds it dormant while the pre-refusal reload
    /// read the old standing depth off the live entry and revived it.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_dormant_row_on_a_channel_this_reload_retunes_is_refused() {
        let tree = Tree::holding(&document(&spill_channel(4)));
        let mut booted = boot(&tree, Vec::new()).await;
        // The row is granted retain depth 2, which the candidate's standing
        // depth of 1 no longer stands behind.
        insert_dynamic_row(&booted, "brenn:spill", false).await;

        tree.write(&document_subscribing_acl(
            &spill_channel(1),
            &[],
            &["exact spill"],
        ));
        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Refused, "{status:?}");
        assert!(
            status
                .refusals
                .iter()
                .any(|line| line.contains("brenn:spill") && line.contains("dormant")),
            "{:?}",
            status.refusals,
        );
        assert!(
            live_entry_of_reader(&booted, "brenn:spill").is_none(),
            "the row is still dormant, which is what a restart would re-classify",
        );
        assert_eq!(
            booted
                .messenger
                .directory()
                .resolve("brenn:spill")
                .expect("the spill channel is still declared")
                .resolved_channel
                .standing_retain_depth,
            brenn_lib::messaging::config::Depth::Bounded(4),
            "the running tuning is the booted one",
        );
    }

    /// A dynamic subscription minted by a changed agent *after* prepare read
    /// the row set.
    ///
    /// The row set is the one input to a reload a live session can change while
    /// prepare runs: a `MessageSubscribe` is authorized under the old policy
    /// until the swap, so a row minted in that window is one the re-merge never
    /// classified — and leaving it folded past the swap is an entry the
    /// candidate's ACL denies. Prepare's answer is re-asked before the walk
    /// touches anything, so a hit is an ordinary refusal.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_dynamic_subscription_minted_after_prepare_refuses_the_commit() {
        let tree = Tree::holding(&document_covering_work(""));
        let mut booted = boot(&tree, Vec::new()).await;
        let booted_sha = booted.driver.baseline().document.document_sha256.clone();

        // The candidate narrows the reader's ACL, which is what makes it a
        // changed agent and its dynamic rows this reload's business.
        tree.write(&document(""));
        let dynamic = booted.driver.dynamic_snapshot().await;
        let ready = match booted.driver.prepare(TriggerSource::Signal, &dynamic) {
            Prepared::Ready(ready) => ready,
            other => panic!("the candidate is applicable: {}", outcome_of(&other)),
        };

        // The window: the agent subscribes to the channel it is about to lose
        // its authority over.
        insert_dynamic_row(&booted, "brenn:work", true).await;

        booted.driver.commit(TriggerSource::Signal, *ready).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Refused, "{status:?}");
        assert_eq!(status.refusals.len(), 1, "{:?}", status.refusals);
        assert!(
            status.refusals[0].contains(READER) && status.refusals[0].contains("brenn:work"),
            "{:?}",
            status.refusals,
        );
        // Refused means untouched: the row is still folded, the table still
        // holds it, and the baseline is still the booted document.
        assert_eq!(status.generation, 0);
        assert_eq!(
            booted.driver.baseline().document.document_sha256,
            booted_sha
        );
        assert!(live_entry_of_reader(&booted, "brenn:work").is_some());
        assert_eq!(dynamic_rows(&booted).await.len(), 1);
        assert!(
            booted.messenger.app_table().load()[READER]
                .policy
                .allows_brenn_delivery("work"),
            "the old policy is still the one every gate reads",
        );
    }

    /// The non-durable arm of both channel steps: an `ephemeral:` channel's
    /// ring store is minted when the entry arrives and dropped when it leaves,
    /// which is the whole difference between it and a `brenn:` channel, whose
    /// row is kept for an operator to delete.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_added_ephemeral_channel_gets_a_ring_and_loses_it_again() {
        let tree = Tree::holding(&document(""));
        let mut booted = boot(&tree, Vec::new()).await;
        assert!(ring_addresses(&booted.messenger).contains(&"ephemeral:scratch".to_string()));

        let extra = r#"
channel spill at "ephemeral:spill" {
    push_depth = 1;
    retain_depth = 4;
}
"#;
        tree.write(&document(extra));
        booted.driver.reload(TriggerSource::Signal).await;
        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert_eq!(
            status.delta.channels_added,
            vec!["ephemeral:spill".to_string()]
        );
        assert!(
            ring_addresses(&booted.messenger).contains(&"ephemeral:spill".to_string()),
            "a publish to an added ephemeral channel has to have somewhere to land",
        );
        let spill = booted
            .messenger
            .directory()
            .resolve("ephemeral:spill")
            .expect("the added channel is live");
        assert!(
            channel_row(&booted.messenger, spill.uuid).await.is_none(),
            "an ephemeral channel's messages live in its ring, so it has no row",
        );

        tree.write(&document(""));
        booted.driver.reload(TriggerSource::Signal).await;
        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert!(
            !ring_addresses(&booted.messenger).contains(&"ephemeral:spill".to_string()),
            "the ring goes with the entry: nothing else would ever free it",
        );
    }

    /// A subscriber that arrives on a departing channel *after* prepare has
    /// approved the reload.
    ///
    /// Rule 2 is a check-then-act: prepare reads the live directory, and the
    /// walk acts on that answer later — after hashing and cranelift-compiling
    /// every arriving component, which is seconds, and three other writers can
    /// add a subscriber to the channel in the meantime. So the walk asks again
    /// before it touches anything, and a hit is an ordinary refusal, because
    /// nothing has moved yet.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_subscriber_arriving_after_prepare_refuses_the_commit() {
        let components = tempfile::tempdir().expect("a components root");
        let tree = Tree::holding(&document_with_a_consumer());
        install_package(components.path(), &staged_module(&tree));
        let mut booted = boot(&tree, vec![components.path().to_path_buf()]).await;
        let booted_sha = booted.driver.baseline().document.document_sha256.clone();

        // The candidate retires the consumer, and with it the channel only it
        // read.
        tree.write(&document(""));
        let dynamic = booted.driver.dynamic_snapshot().await;
        let ready = match booted.driver.prepare(TriggerSource::Signal, &dynamic) {
            Prepared::Ready(ready) => ready,
            other => panic!("the candidate is applicable: {}", outcome_of(&other)),
        };

        // The window: an attach session subscribes to the departing channel
        // while prepare is still working.
        let sink = booted
            .messenger
            .directory()
            .resolve("brenn:sink")
            .expect("the sink channel is declared");
        assert!(booted.messenger.directory().add_subscriber(
            &sink.uuid,
            brenn_lib::messaging::SubscriberEntry {
                kind: SubscriberEntryKind::Surface("wall".to_string()),
                push_depth: brenn_lib::messaging::config::Depth::Bounded(4),
                retain_depth: brenn_lib::messaging::config::Depth::Bounded(4),
                noise: brenn_lib::messaging::config::NoiseLevel::Silent,
                wake_min: None,
            },
        ));

        booted.driver.commit(TriggerSource::Signal, *ready).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Refused, "{status:?}");
        assert_eq!(status.refusals.len(), 1, "{:?}", status.refusals);
        assert!(
            status.refusals[0].contains("brenn:sink") && status.refusals[0].contains("wall"),
            "{:?}",
            status.refusals
        );
        // Refused means untouched, wherever the refusal was made.
        assert_eq!(status.generation, 0);
        assert_eq!(
            booted.driver.baseline().document.document_sha256,
            booted_sha
        );
        assert!(booted.driver.registry().contains_key("sifter"));
        assert!(booted.messenger.directory().resolve("brenn:sink").is_some());
        assert!(
            subscribed_anywhere(
                &booted.messenger,
                &SubscriberEntryKind::Wasm("sifter".to_string())
            )
            .contains(&"brenn:work".to_string())
        );
    }

    /// The addresses holding a ring store.
    pub(crate) fn ring_addresses(messenger: &Messenger) -> Vec<String> {
        messenger
            .ring_stores()
            .stores()
            .iter()
            .map(|store| store.address().to_string())
            .collect()
    }

    // ── surface convergence ─────────────────────────────────────────────────
    //
    // The rig a `[[surface]]` needs is heavier than any other block's: a
    // deployed asset tree under a mount, a packaged class module whose bytes
    // the tree's manifest hashes, and the seven derived self-description
    // channels declared in the document. Built once here so the cases below
    // read as what they are about.

    /// The mount a fixture's deployed surface tree is declared under.
    pub(crate) const SURFACE_MOUNT: &str = "surface-release";

    /// The mount a fixture's component bundle is declared under: a surface tree
    /// carrying kinds and no kernel pair, which is what makes a stale record
    /// under it withheld rather than a boot refusal.
    pub(crate) const SURFACE_BUNDLE_MOUNT: &str = "surface-bundle";

    /// The prefix `[surface_description]` defaults to, and so the root of every
    /// derived address a surface-carrying document declares.
    const SURFACE_PREFIX: &str = "surface";

    /// Write `kind`'s class module into the tree's module root and its deployed
    /// assets into `assets`, bound to each other by bytes.
    ///
    /// A placed component carries the hash of the file its class was declared
    /// in, and a kind's manifest carries the hash of the packaged specification
    /// shipped beside its artifact; boot refuses a surface whose two disagree.
    /// So the module text *is* the specification bytes.
    ///
    /// Written straight into the module root rather than through the fixture
    /// fence: the fenced half is as tall as the whole fixture, so a document
    /// that gained a line would move the class hash and with it the kind's
    /// fingerprint — which is exactly what a case about a document edit must
    /// not do.
    pub(crate) fn write_surface_kind(
        tree: &Tree,
        assets: &std::path::Path,
        kind: &str,
        class: &str,
    ) {
        write_surface_kind_needing(tree, assets, kind, class, "dom, page-dom");
    }

    /// [`write_surface_kind`] with the class's grant list stated, for a case
    /// that needs a second kind a non-chrome component may hold.
    fn write_surface_kind_needing(
        tree: &Tree,
        assets: &std::path::Path,
        kind: &str,
        class: &str,
        needs: &str,
    ) {
        let modules = tree.modules();
        std::fs::create_dir_all(&modules).expect("a module root");
        let spec = format!(
            "component {class} {{\n    {}\n    in feed;\n}}\n",
            brenn_dsl::fixture_text::processor_header(needs),
        );
        std::fs::write(modules.join(format!("{kind}.brenn")), &spec)
            .expect("the class module is writable");
        std::fs::create_dir_all(assets).expect("a surface asset tree");
        brenn_surface_server::test_fixtures::write_kernel_pair(assets);
        brenn_surface_server::test_fixtures::write_processor_tree_from_bytes(
            assets,
            kind,
            format!("component-bytes-for-{kind}").as_bytes(),
            spec.as_bytes(),
            Vec::new(),
            true,
            |_| {},
        );
    }

    /// The seven channels one surface of one kind derives, declared with the
    /// retention each family requires.
    ///
    /// The index is in [`document`] already, so it is not repeated here; the
    /// kind pair is emitted once per kind and a caller declaring two surfaces
    /// of one kind passes it an empty kind list for the second.
    fn description_channels(slug: &str, kinds: &[&str]) -> String {
        let mut text = String::new();
        for kind in kinds {
            for family in ["help", "schema"] {
                text.push_str(&format!(
                    "channel {kind}_{family} at \"brenn:{SURFACE_PREFIX}.kind.{kind}.{family}\" {{\
                     \n    push_depth = 1;\n    retain_depth = 1;\n    \
                     standing_retain_depth = 1;\n}}\n\n",
                ));
            }
        }
        text.push_str(&format!(
            "channel {slug}_help at \"brenn:{SURFACE_PREFIX}.surface.{slug}.help\" {{\
             \n    push_depth = 1;\n    retain_depth = 1;\n    standing_retain_depth = 1;\n}}\n\n",
        ));
        for family in ["geometry", "status"] {
            text.push_str(&format!(
                "channel {slug}_{family} at \"brenn:{SURFACE_PREFIX}.surface.{slug}.{family}\" {{\
                 \n    push_depth = 1;\n    retain_depth = 4;\n    \
                 standing_retain_depth = 4;\n}}\n\n",
            ));
        }
        text.push_str(&format!(
            "channel {slug}_bindings at \"ephemeral:{SURFACE_PREFIX}.surface.{slug}.bindings\" {{\
             \n    push_depth = 1;\n    retain_depth = 1;\n}}\n\n",
        ));
        text
    }

    /// A document declaring every `(slug, attrs)` in `surfaces` as an instance
    /// of `kind`, each reading `feed` off a channel of its own.
    ///
    /// `attrs` is whatever a case varies on the surface block itself, which is
    /// how a case moves one surface's resolved value without touching a
    /// channel. The kind's help/schema pair is declared once however many
    /// surfaces mount it.
    pub(crate) fn surfaces_document(kind: &str, class: &str, surfaces: &[(&str, &str)]) -> String {
        let only = [kind];
        let mut extra = String::new();
        for (index, (slug, attrs)) in surfaces.iter().enumerate() {
            extra.push_str(&description_channels(
                slug,
                match index {
                    0 => &only,
                    _ => &[],
                },
            ));
            extra.push_str(&format!(
                "channel {slug}_feed at \"ephemeral:{slug}.feed\" {{\n    push_depth = 4;\n    \
                 retain_depth = 16;\n}}\n\n\
                 surface {slug} {{\n    grants = [subscribe];\n{attrs}\
                 \n    new panel: {class} {{\n        grants = [dom, page-dom];\n        \
                 chrome = true;\n        in feed <- {slug}_feed {{ push_depth = 2; }}\n    \
                 }}\n}}\n\n",
            ));
        }
        format!("use @{kind}::*;\n{}", document(&extra))
    }

    /// The one-surface form, which most cases want.
    pub(crate) fn surface_document(slug: &str, kind: &str, class: &str, attrs: &str) -> String {
        surfaces_document(kind, class, &[(slug, attrs)])
    }

    /// The bindings document the kernel replays on attach.
    const DESKBAR_BINDINGS: &str = "ephemeral:surface.surface.deskbar.bindings";
    /// The retained index, a function of the whole surface list.
    const SURFACE_INDEX: &str = "brenn:surface.index";
    /// The surface's own help document.
    const DESKBAR_HELP: &str = "brenn:surface.surface.deskbar.help";
    /// The kind's help document, which lists every surface mounting it.
    const PANEL_HELP: &str = "brenn:surface.kind.panel.help";

    /// The rig every one-surface case opens with: a tree, the `panel` kind's
    /// deployed assets beside it, a document declaring one surface of that kind
    /// with `attrs`, and the booted process serving it.
    ///
    /// The asset tree is returned because it is a `TempDir` — dropping it takes
    /// the mount out from under the running process — and the tree because
    /// every case rewrites the document it booted from.
    pub(crate) async fn boot_one_panel_surface(attrs: &str) -> (Tree, tempfile::TempDir, Booted) {
        let tree = Tree::new();
        let assets = tempfile::tempdir().expect("a surface asset tree");
        write_surface_kind(&tree, assets.path(), "panel", "Panel");
        tree.write(&surface_document("deskbar", "panel", "Panel", attrs));
        let booted = boot_with_panel(&tree, assets.path()).await;
        (tree, assets, booted)
    }

    /// Boot a process with the panel kind installed under a mount.
    pub(crate) async fn boot_with_panel(tree: &Tree, assets: &std::path::Path) -> Booted {
        boot_with(
            tree,
            BootFixture {
                surface_assets: Some(assets.to_path_buf()),
                ..BootFixture::default()
            },
        )
        .await
    }

    /// The newest retained envelope on `address`, or nothing.
    ///
    /// The envelope rather than the body where a case has to tell "republished
    /// with the same bytes" from "never rewritten": the two are one string, and
    /// only the envelope's own identity says which happened.
    async fn newest_envelope(
        messenger: &Arc<Messenger>,
        address: &str,
    ) -> Option<brenn_envelope::MessageEnvelope> {
        messenger
            .query(&MessageQuery {
                channel: address.to_string(),
                limit: 1,
                before: None,
                after: None,
                sender: None,
                search: None,
                calling_app_slug: READER.to_string(),
            })
            .await
            .expect("the fixture reader may read every address it is given")
            .into_iter()
            .next()
    }

    /// The newest retained body on `address`, or nothing.
    async fn newest_body(messenger: &Arc<Messenger>, address: &str) -> Option<String> {
        messenger
            .query(&MessageQuery {
                channel: address.to_string(),
                limit: 1,
                before: None,
                after: None,
                sender: None,
                search: None,
                calling_app_slug: READER.to_string(),
            })
            .await
            .expect("the fixture reader may read every address it is given")
            .into_iter()
            .next()
            .map(|envelope| envelope.body)
    }

    /// Whether the runtime table serves `slug`.
    fn serves_surface(booted: &Booted, slug: &str) -> bool {
        matches!(
            booted.driver.env.surfaces.lookup(slug),
            brenn_server::state::SurfaceLookup::Ready(_)
        )
    }

    /// **A surface added to the document converges.** Every part of a surface's
    /// runtime footprint arrives in one reload: the entry the doors read, the
    /// subscriber entries its input bindings fold into, the registration its
    /// publishes are gated by, and the retained bindings document the kernel
    /// replays on attach.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_surface_added_to_the_document_converges() {
        let tree = Tree::holding(&document(""));
        let assets = tempfile::tempdir().expect("a surface asset tree");
        write_surface_kind(&tree, assets.path(), "panel", "Panel");
        let mut booted = boot_with_panel(&tree, assets.path()).await;
        assert!(!serves_surface(&booted, "deskbar"));

        tree.write(&surface_document("deskbar", "panel", "Panel", ""));
        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert_eq!(status.delta.surfaces_added, vec!["deskbar".to_string()]);
        assert!(status.delta.surfaces_removed.is_empty());
        assert!(serves_surface(&booted, "deskbar"), "the door must find it");
        assert!(
            subscribed_anywhere(
                &booted.messenger,
                &SubscriberEntryKind::Surface("deskbar".to_string())
            )
            .contains(&"ephemeral:deskbar.feed".to_string()),
            "the surface's input binding must be folded into the directory",
        );
        let bindings = newest_body(&booted.messenger, DESKBAR_BINDINGS)
            .await
            .expect("the bindings document is retained on the config channel");
        assert!(bindings.contains("deskbar"), "{bindings}");
        assert!(
            newest_body(&booted.messenger, PANEL_HELP)
                .await
                .expect("the kind's help document is republished")
                .contains("deskbar"),
            "the kind help lists every surface mounting it",
        );
    }

    /// A stand-in attached page on `slug`, leaving the way a real session does:
    /// the task holds the registry guard, wakes on the host-close watch and
    /// drops the guard on its way out, so the commit step's wait for the
    /// registry to empty is answered by the session and not by the fixture.
    ///
    /// The task reports the close code it was sent, which is what a page's
    /// behaviour is decided by.
    fn attach_a_session(booted: &Booted, slug: &str) -> tokio::task::JoinHandle<u16> {
        use brenn_attach_server::registry::{AttachSessionHandle, SessionCaps};

        let handle = AttachSessionHandle::for_test(READER);
        let mut close_rx = handle.close.subscribe();
        let guard = booted
            .driver
            .env
            .attach_registry
            .try_register(slug, handle, SessionCaps::UNCAPPED)
            .expect("uncapped registration");
        tokio::spawn(async move {
            let _guard = guard;
            loop {
                close_rx
                    .changed()
                    .await
                    .expect("the registry holds the sender until this guard drops");
                let reason = close_rx.borrow_and_update().clone();
                if let Some(reason) = reason {
                    return reason.code;
                }
            }
        })
    }

    /// The `Surface(slug)` registration the messenger gates that surface's
    /// publishes on, if it holds one.
    fn holds_surface_registration(booted: &Booted, slug: &str) -> bool {
        booted
            .messenger
            .subscriber_registration(&SubscriberEntryKind::Surface(slug.to_string()))
            .is_some()
    }

    /// **A surface whose declaration moved is replaced in place.** The value
    /// moved and no channel did, so the whole of the work is the surface's own:
    /// every open page is closed with the reconfigured code, the runtime the
    /// doors hand out is the new one, and the documents that carry the moved
    /// value are rebuilt.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_changed_surface_closes_its_pages_and_is_replaced() {
        let (tree, _assets, mut booted) = boot_one_panel_surface("").await;
        assert!(serves_surface(&booted, "deskbar"));
        let page = attach_a_session(&booted, "deskbar");
        let before = newest_envelope(&booted.messenger, DESKBAR_BINDINGS)
            .await
            .expect("boot published the surface's bindings document");

        tree.write(&surface_document(
            "deskbar",
            "panel",
            "Panel",
            "    skin = \"foundry\";\n",
        ));
        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert_eq!(status.delta.surfaces_changed, vec!["deskbar".to_string()]);
        assert!(status.delta.surfaces_added.is_empty());
        assert_eq!(
            page.await.expect("the stand-in page task"),
            brenn_surface_schema::SURFACE_RECONFIGURED_CLOSE_CODE,
            "a replaced surface's pages reload rather than reporting a retirement",
        );
        assert!(serves_surface(&booted, "deskbar"), "the new runtime is in");
        assert!(holds_surface_registration(&booted, "deskbar"));
        let after = newest_envelope(&booted.messenger, DESKBAR_BINDINGS)
            .await
            .expect("the bindings document is still retained");
        // Rebuilt, and rebuilt to the same bytes: the message is a different
        // one, so the reload did write it, and its body is what boot's said,
        // because a skin is not part of the wiring. Comparing bodies alone
        // would be satisfied by boot's own copy and would pass a reload that
        // stopped republishing for a changed surface altogether.
        assert_ne!(
            before.message_id, after.message_id,
            "the bindings document is republished rather than left where boot put it",
        );
        assert_eq!(
            before.body, after.body,
            "a skin is not part of the wiring, so the rebuilt bindings document says what the \
             one boot published said",
        );
        // What the skin *is* part of: the surface's own help document.
        let help = newest_body(&booted.messenger, DESKBAR_HELP)
            .await
            .expect("the surface help document is retained");
        assert!(
            help.contains("- skin: `foundry`"),
            "the rebuilt help document carries the new skin: {help}",
        );
    }

    /// **A surface removed from the document leaves nothing behind.** The pages
    /// are told it is retired, the door stops answering for the slug, and every
    /// piece of runtime wiring a surface holds — registration, delivery binding,
    /// send budgets, subscriber entries — is gone. This is also the
    /// removal-only shape: nothing arrives, and the two surface-description
    /// participants still have to be renarrowed and their documents rebuilt.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_removed_surface_retires_its_pages_and_its_wiring() {
        let (tree, _assets, mut booted) = boot_one_panel_surface("").await;
        let page = attach_a_session(&booted, "deskbar");

        // The surface leaves; its channels stay declared, as a document that
        // dropped only the block does.
        tree.write(&document(&description_channels("deskbar", &["panel"])));
        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert_eq!(status.delta.surfaces_removed, vec!["deskbar".to_string()]);
        assert_eq!(
            page.await.expect("the stand-in page task"),
            brenn_surface_schema::SURFACE_RETIRED_CLOSE_CODE,
            "a retired surface's pages must not try to come back",
        );
        assert!(!serves_surface(&booted, "deskbar"), "the door must 404 it");
        assert!(!holds_surface_registration(&booted, "deskbar"));
        assert!(
            subscribed_anywhere(
                &booted.messenger,
                &SubscriberEntryKind::Surface("deskbar".to_string())
            )
            .is_empty(),
            "no directory entry may still name a surface that no longer exists",
        );
        // The index is a function of the whole surface list, so a removal
        // republishes it even though nothing arrived.
        let index = newest_body(&booted.messenger, SURFACE_INDEX)
            .await
            .expect("the index is retained");
        assert!(
            !index.contains("deskbar"),
            "the retained index still lists the retired surface: {index}",
        );
    }

    /// Every publish matcher one surface-description participant's registration
    /// carries, across both schemes: the help participant writes `brenn:`
    /// documents and the config participant `ephemeral:` ones, and what a case
    /// asks is whether either can still name a retired surface.
    fn description_matchers(booted: &Booted, component: &str) -> Vec<String> {
        let acls = booted
            .messenger
            .subscriber_registration(&SubscriberEntryKind::System(component.to_string()))
            .unwrap_or_else(|| panic!("boot registers {component}"))
            .policy
            .acls
            .clone();
        acls.brenn_publish
            .iter()
            .chain(acls.ephemeral_publish.iter())
            .map(|matcher| format!("{matcher:?}"))
            .collect()
    }

    /// Boot a two-surface document, so the cases below can move one surface and
    /// watch what happens to the other.
    async fn boot_two_surfaces(tree: &Tree, assets: &std::path::Path) -> Booted {
        tree.write(&surfaces_document(
            "panel",
            "Panel",
            &[("deskbar", ""), ("sidebar", "")],
        ));
        boot_with(
            tree,
            BootFixture {
                surface_assets: Some(assets.to_path_buf()),
                ..BootFixture::default()
            },
        )
        .await
    }

    /// **A second surface of an existing kind republishes that kind's document
    /// and leaves the first surface's alone.** The document set a reload owes
    /// is decided by what each document is a function of: a kind's help lists
    /// every surface mounting it, so an arrival moves it; an untouched
    /// surface's own help is a function of that surface, so it is not
    /// rebuilt and not restamped.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_second_surface_of_a_kind_republishes_only_what_moved() {
        let tree = Tree::new();
        let assets = tempfile::tempdir().expect("a surface asset tree");
        write_surface_kind(&tree, assets.path(), "panel", "Panel");
        tree.write(&surface_document("deskbar", "panel", "Panel", ""));
        let mut booted = boot_with(
            &tree,
            BootFixture {
                surface_assets: Some(assets.path().to_path_buf()),
                ..BootFixture::default()
            },
        )
        .await;
        booted.driver.reload(TriggerSource::Signal).await;
        let deskbar_help = newest_body(&booted.messenger, DESKBAR_HELP).await;
        let kind_help = newest_body(&booted.messenger, PANEL_HELP).await;

        tree.write(&surfaces_document(
            "panel",
            "Panel",
            &[("deskbar", ""), ("sidebar", "")],
        ));
        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert_eq!(status.delta.surfaces_added, vec!["sidebar".to_string()]);
        assert!(
            status.delta.surfaces_changed.is_empty(),
            "the first surface did not move: {:?}",
            status.delta.surfaces_changed,
        );
        let after_kind = newest_body(&booted.messenger, PANEL_HELP).await;
        assert_ne!(kind_help, after_kind, "the kind help lists both now");
        assert!(
            after_kind
                .as_deref()
                .is_some_and(|body| body.contains("sidebar")),
            "{after_kind:?}",
        );
        assert_eq!(
            deskbar_help,
            newest_body(&booted.messenger, DESKBAR_HELP).await,
            "an untouched surface's own help must not be restamped",
        );
    }

    /// **Removing one of two surfaces of a kind narrows what the description
    /// participants may write and rebuilds that kind's help.** The removal-only
    /// half of step 5b: nothing arrives, and the two participants must still
    /// lose every matcher naming the retired surface — the registrations a
    /// fresh boot of the emptied document would build.
    #[tokio::test(flavor = "multi_thread")]
    async fn removing_a_surface_narrows_the_description_participants() {
        let tree = Tree::holding(&document(""));
        let assets = tempfile::tempdir().expect("a surface asset tree");
        write_surface_kind(&tree, assets.path(), "panel", "Panel");
        let mut booted = boot_two_surfaces(&tree, assets.path()).await;
        assert!(
            description_matchers(
                &booted,
                brenn_surface_server::description::SURFACE_HELP_COMPONENT
            )
            .iter()
            .any(|matcher| matcher.contains("sidebar")),
            "boot admits every surface's own help address",
        );
        booted.driver.reload(TriggerSource::Signal).await;
        let kind_help = newest_body(&booted.messenger, PANEL_HELP).await;

        let mut text = surface_document("deskbar", "panel", "Panel", "");
        text.push_str(&description_channels("sidebar", &[]));
        tree.write(&text);
        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert_eq!(status.delta.surfaces_removed, vec!["sidebar".to_string()]);
        for component in [
            brenn_surface_server::description::SURFACE_HELP_COMPONENT,
            brenn_surface_server::description::SURFACE_CONFIG_COMPONENT,
        ] {
            let matchers = description_matchers(&booted, component);
            assert!(
                !matchers.iter().any(|matcher| matcher.contains("sidebar")),
                "{component} may still write the retired surface's addresses: {matchers:?}",
            );
            assert!(
                matchers.iter().any(|matcher| matcher.contains("deskbar")),
                "{component} lost the surviving surface's addresses: {matchers:?}",
            );
        }
        let after_kind = newest_body(&booted.messenger, PANEL_HELP).await;
        assert_ne!(kind_help, after_kind, "the kind help lost an instance");
        assert!(
            after_kind
                .as_deref()
                .is_some_and(|body| !body.contains("sidebar")),
            "{after_kind:?}",
        );
    }

    /// **A kind whose installed bytes moved promotes every surface mounting
    /// it.** The bundle-upgrade case with the document untouched: nothing in
    /// the configuration says anything happened, and the kind's fingerprint is
    /// the only witness, so the surface is replaced and its pages reload onto
    /// the new assets.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_kind_upgrade_replaces_every_surface_mounting_it() {
        let (tree, assets, mut booted) = boot_one_panel_surface("").await;
        let page = attach_a_session(&booted, "deskbar");

        // The installer's shape: the same specification, new artifact bytes.
        let spec = std::fs::read(tree.modules().join("panel.brenn")).expect("the class module");
        brenn_surface_server::test_fixtures::write_processor_tree_from_bytes(
            assets.path(),
            "panel",
            b"the-upgraded-artifact",
            &spec,
            Vec::new(),
            true,
            |_| {},
        );
        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert_eq!(status.delta.kinds_changed, vec!["panel".to_string()]);
        assert_eq!(status.delta.surfaces_changed, vec!["deskbar".to_string()]);
        assert_eq!(
            page.await.expect("the stand-in page task"),
            brenn_surface_schema::SURFACE_RECONFIGURED_CLOSE_CODE,
            "the page has to come back for the new assets",
        );
        assert_eq!(
            booted
                .driver
                .env
                .surface_roots
                .read()
                .expect("the cell is uncontended")
                .kinds["panel"]
                .source_sha256,
            brenn_lib::util::sha256_hex(b"the-upgraded-artifact"),
            "the served roots must be the ones the reload was decided against",
        );
    }

    // ── a withheld kind, and the two transitions a reload converges ─────────
    //
    // A kind whose record this binary does not read is withheld under a bundle
    // mount: not served, alerted, and picked up at the reload that follows the
    // bundle's re-release. The rig below is the only one in this suite with two
    // surface trees, because that is what the distinction rests on — brenn's
    // own tree is the one carrying the kernel pair, and a stale record under it
    // is a boot refusal rather than a withholding.

    /// Write `panel`'s class module into `tree`'s module root, the kernel pair
    /// into brenn's own surface tree, and `panel`'s deployed assets into the
    /// bundle's, at record version `record_v`.
    ///
    /// Everything but `record_v` is a function of the kind's name, so the two
    /// versions of a kind differ in exactly the field the withholding decision
    /// reads — which is what the bundle's re-release looks like from here.
    fn write_bundled_panel_kind(
        tree: &Tree,
        kernel: &std::path::Path,
        bundle: &std::path::Path,
        record_v: u32,
    ) {
        let modules = tree.modules();
        std::fs::create_dir_all(&modules).expect("a module root");
        let spec = format!(
            "component Panel {{\n    {}\n    in feed;\n}}\n",
            brenn_dsl::fixture_text::processor_header("dom, page-dom"),
        );
        std::fs::write(modules.join("panel.brenn"), &spec).expect("the class module is writable");
        std::fs::create_dir_all(kernel).expect("brenn's own surface tree");
        brenn_surface_server::test_fixtures::write_kernel_pair(kernel);
        std::fs::create_dir_all(bundle).expect("the bundle's surface tree");
        brenn_surface_server::test_fixtures::write_processor_tree_from_bytes(
            bundle,
            "panel",
            b"component-bytes-for-panel",
            spec.as_bytes(),
            Vec::new(),
            true,
            |manifest| manifest["v"] = serde_json::json!(record_v),
        );
    }

    /// One surface of a bundled `panel`, whose record declares `record_v`.
    ///
    /// Both tempdirs are returned: dropping either takes a declared mount out
    /// from under the running process.
    async fn boot_one_bundled_panel_surface(
        record_v: u32,
    ) -> (Tree, tempfile::TempDir, tempfile::TempDir, Booted) {
        let tree = Tree::new();
        let kernel = tempfile::tempdir().expect("brenn's own surface tree");
        let bundle = tempfile::tempdir().expect("a bundle surface tree");
        write_bundled_panel_kind(&tree, kernel.path(), bundle.path(), record_v);
        tree.write(&surface_document("deskbar", "panel", "Panel", ""));
        let booted = boot_with(
            &tree,
            BootFixture {
                surface_assets: Some(kernel.path().to_path_buf()),
                surface_bundle: Some(bundle.path().to_path_buf()),
                ..BootFixture::default()
            },
        )
        .await;
        (tree, kernel, bundle, booted)
    }

    /// The bodies of every "surface kind withheld" alert raised so far.
    ///
    /// The severity is asserted here rather than returned: every caller wants
    /// the same one, and an alert that reached the operator at `Info` would
    /// otherwise pass every assertion below.
    fn withheld_alerts(booted: &Booted) -> Vec<String> {
        booted
            .captured
            .lock()
            .expect("alert capture")
            .iter()
            .filter(|(_, title, _)| title == brenn_surface_server::WITHHELD_ALERT_TITLE)
            .map(|(severity, _, body)| {
                assert!(
                    matches!(severity, brenn_obs::alerting::AlertSeverity::Warning),
                    "a withheld kind is a warning, not {severity}: {body}",
                );
                body.clone()
            })
            .collect()
    }

    /// Poll until `wanted` withheld alerts have drained, or panic. Alerts
    /// arrive asynchronously after the reload that raised them returns.
    async fn withheld_alerts_until(booted: &Booted, wanted: usize) -> Vec<String> {
        for _ in 0..200 {
            let seen = withheld_alerts(booted);
            if seen.len() >= wanted {
                return seen;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!(
            "wanted {wanted} withheld alert(s), saw {:?}",
            withheld_alerts(booted)
        );
    }

    /// The served roots the driver is deciding against right now.
    fn serving_roots(booted: &Booted) -> Arc<brenn_surface_server::SurfaceRoots> {
        booted
            .driver
            .env
            .surface_roots
            .read()
            .expect("the cell is uncontended")
            .clone()
    }

    /// **A bundle's re-release converges a withheld kind into service.** The
    /// process booted over a record it cannot read, so the kind was withheld
    /// and the surface mounting it came up on the withheld manifest. The
    /// operator re-releases the bundle against this brenn; nothing in the
    /// document moves, and the kind's arrival in the served set is the whole
    /// witness — `Offered`, which promotes the surface stamping it, so its
    /// pages come back for assets that now exist.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_withheld_kind_that_comes_back_at_its_record_converges_and_restarts_its_surface() {
        let (tree, kernel, bundle, mut booted) = boot_one_bundled_panel_surface(2).await;
        let booted_roots = serving_roots(&booted);
        assert!(
            booted_roots.withheld.contains_key("panel"),
            "boot must withhold a kind whose record it does not read: {:?}",
            booted_roots.kinds.keys().collect::<Vec<_>>(),
        );
        assert!(!booted_roots.kinds.contains_key("panel"));
        // Boot's own announcement: the process is up and serving a page that
        // cannot bring the instance up, so this alert is the only unprompted
        // notice the operator gets.
        let boot_alerts = withheld_alerts_until(&booted, 1).await;
        assert_eq!(boot_alerts.len(), 1, "{boot_alerts:?}");
        assert!(boot_alerts[0].contains("panel"), "{boot_alerts:?}");
        assert!(
            boot_alerts[0].contains(SURFACE_BUNDLE_MOUNT),
            "the operator is told which mount to re-release: {boot_alerts:?}",
        );
        let page = attach_a_session(&booted, "deskbar");

        write_bundled_panel_kind(&tree, kernel.path(), bundle.path(), 3);
        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert_eq!(status.delta.kinds_changed, vec!["panel".to_string()]);
        assert_eq!(status.delta.surfaces_changed, vec!["deskbar".to_string()]);
        assert_eq!(
            page.await.expect("the stand-in page task"),
            brenn_surface_schema::SURFACE_RECONFIGURED_CLOSE_CODE,
            "the page has to come back for the assets it was denied",
        );
        let after = serving_roots(&booted);
        assert!(
            after.kinds.contains_key("panel"),
            "the re-released kind must be served: {:?}",
            after.withheld.keys().collect::<Vec<_>>(),
        );
        assert!(after.withheld.is_empty(), "{:?}", after.withheld);
    }

    /// **A kind whose record goes stale under a running process is withheld,
    /// alerted once, and takes its surface with it.** The other direction: a
    /// bundle installed at a record this binary does not read, under a document
    /// that did not move. `Withdrawn` promotes the surface, whose pages come
    /// back onto the withheld manifest, and the operator is told — once. A
    /// second reload finding the same kind still withheld says nothing: an
    /// alert repeated at every reload is one an operator learns to ignore.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_kind_that_goes_stale_is_withheld_alerted_once_and_restarts_its_surface() {
        let (tree, kernel, bundle, mut booted) = boot_one_bundled_panel_surface(3).await;
        assert!(serving_roots(&booted).kinds.contains_key("panel"));
        assert!(
            withheld_alerts(&booted).is_empty(),
            "nothing is withheld yet",
        );
        let page = attach_a_session(&booted, "deskbar");

        write_bundled_panel_kind(&tree, kernel.path(), bundle.path(), 2);
        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert_eq!(status.delta.kinds_changed, vec!["panel".to_string()]);
        assert_eq!(status.delta.surfaces_changed, vec!["deskbar".to_string()]);
        assert_eq!(
            page.await.expect("the stand-in page task"),
            brenn_surface_schema::SURFACE_RECONFIGURED_CLOSE_CODE,
            "the page has to come back onto the withheld manifest",
        );
        let after = serving_roots(&booted);
        let held = after
            .withheld
            .get("panel")
            .unwrap_or_else(|| panic!("{:?}", after.kinds.keys().collect::<Vec<_>>()));
        assert!(!after.kinds.contains_key("panel"));
        assert_eq!(held.record_v, 2);
        assert_eq!(held.mount, SURFACE_BUNDLE_MOUNT);
        let alerts = withheld_alerts_until(&booted, 1).await;
        assert_eq!(alerts.len(), 1, "{alerts:?}");
        assert!(alerts[0].contains("panel"), "{alerts:?}");
        assert!(
            alerts[0].contains(SURFACE_BUNDLE_MOUNT),
            "the operator is told which mount to re-release: {alerts:?}",
        );

        // The same scan again: still withheld, and already told.
        booted.driver.reload(TriggerSource::Signal).await;
        assert_eq!(
            booted.last_status().await.outcome,
            Outcome::Unchanged,
            "nothing moved between the two scans",
        );
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        assert_eq!(
            withheld_alerts(&booted).len(),
            1,
            "a kind withheld in both scans is alerted once",
        );
    }

    /// **A bundle re-released and still unreadable is applied, and alerted
    /// again.** The operator acted on the boot alert and rebuilt against the
    /// wrong brenn, so the kind is withheld before and after — the one shape
    /// the served map cannot witness. Reporting `unchanged` here would tell an
    /// operator watching the reload outcome that their install did nothing,
    /// while the page, the alert and the description documents all still
    /// carried the version they had just replaced.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_rereleased_kind_this_binary_still_cannot_read_is_applied_and_realerted() {
        let (tree, kernel, bundle, mut booted) = boot_one_bundled_panel_surface(2).await;
        let boot_alerts = withheld_alerts_until(&booted, 1).await;
        assert!(boot_alerts[0].contains("v = 2"), "{boot_alerts:?}");
        let page = attach_a_session(&booted, "deskbar");

        // Rebuilt against a brenn newer than the one running.
        let ahead = brenn_surface_server::processor_assets::MANIFEST_VERSION + 1;
        write_bundled_panel_kind(&tree, kernel.path(), bundle.path(), ahead);
        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert_eq!(status.delta.kinds_changed, vec!["panel".to_string()]);
        assert_eq!(status.delta.surfaces_changed, vec!["deskbar".to_string()]);
        assert_eq!(
            page.await.expect("the stand-in page task"),
            brenn_surface_schema::SURFACE_RECONFIGURED_CLOSE_CODE,
            "the page has to come back onto the reason this scan found",
        );
        let after = serving_roots(&booted);
        assert_eq!(
            after
                .withheld
                .get("panel")
                .unwrap_or_else(|| panic!("{:?}", after.kinds.keys().collect::<Vec<_>>()))
                .record_v,
            ahead,
        );
        let alerts = withheld_alerts_until(&booted, 2).await;
        assert_eq!(alerts.len(), 2, "{alerts:?}");
        assert!(
            alerts[1].contains(&format!("v = {ahead}")),
            "the second alert names the version the operator actually shipped: {alerts:?}",
        );
    }

    /// **A refused reload that scanned a stale record alerts about nothing.**
    /// The scan found the bundle's kind withheld and the kernel rewritten under
    /// its own root; the second refuses the reload, so the process keeps
    /// serving the kind and nothing about it is withheld. An alert here would
    /// name a state this process never entered — and, since the "already told"
    /// question is asked of the roots being served, would be raised again at
    /// every refused reload that followed.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_stale_record_in_a_refused_reload_is_not_alerted() {
        let (tree, kernel, bundle, mut booted) = boot_one_bundled_panel_surface(3).await;
        assert!(serving_roots(&booted).kinds.contains_key("panel"));

        write_bundled_panel_kind(&tree, kernel.path(), bundle.path(), 2);
        std::fs::write(
            kernel.path().join(brenn_surface_server::KERNEL_ARTIFACT),
            b"export function instantiate() { /* the next release */ }",
        )
        .expect("the kernel module is writable");
        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Refused, "{:?}", status.refusals);
        let after = serving_roots(&booted);
        assert!(
            after.withheld.is_empty() && after.kinds.contains_key("panel"),
            "a refused reload serves what it served: {:?}",
            after.withheld.keys().collect::<Vec<_>>(),
        );
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        assert!(
            withheld_alerts(&booted).is_empty(),
            "{:?}",
            withheld_alerts(&booted),
        );
    }

    /// **A kind upgraded past the specification the document was written
    /// against is refused.** The mount landed and the configuration was not
    /// re-checked, so the class hash the surface carries no longer matches the
    /// specification shipped with the assets. Nothing moves; the process keeps
    /// serving what it has.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_kind_whose_specification_moved_under_the_document_is_refused() {
        let (_tree, assets, mut booted) = boot_one_panel_surface("").await;

        // Only the assets move: a release whose specification changed, with the
        // document still compiled against the old one.
        brenn_surface_server::test_fixtures::write_processor_tree_from_bytes(
            assets.path(),
            "panel",
            b"the-upgraded-artifact",
            b"component Panel { abi = processor; requires = [dom, page-dom]; in feed; }\n",
            Vec::new(),
            true,
            |_| {},
        );
        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Refused);
        assert_eq!(status.refusals.len(), 1, "{:?}", status.refusals);
        assert!(
            status.refusals[0].contains("deskbar") && status.refusals[0].contains("panel"),
            "{:?}",
            status.refusals,
        );
        assert!(serves_surface(&booted, "deskbar"), "nothing was touched");
    }

    /// A document whose one surface mounts two instances of the kind, both
    /// reading the same channel — the shape in which the surface's declared
    /// bindings outnumber the directory entries they fold into.
    fn two_instance_document(slug: &str, attrs: &str) -> String {
        let mut extra = description_channels(slug, &["panel", "aside"]);
        extra.push_str(&format!(
            "channel {slug}_feed at \"ephemeral:{slug}.feed\" {{\n    push_depth = 4;\n    \
             retain_depth = 16;\n}}\n\n\
             surface {slug} {{\n    grants = [subscribe];\n{attrs}\
             \n    new panel-a: Panel {{\n        grants = [dom, page-dom];\n        \
             chrome = true;\n        in feed <- {slug}_feed {{ push_depth = 2; }}\n    }}\n    \
             new aside-b: Aside {{\n        grants = [dom];\n        \
             in feed <- {slug}_feed {{ push_depth = 3; }}\n    }}\n}}\n\n",
        ));
        format!("use @panel::*;\nuse @aside::*;\n{}", document(&extra))
    }

    /// **A surface whose components share a channel unfolds once.** A surface's
    /// wire subscriptions are one per (instance, channel) and the directory
    /// carries one subscriber per (surface, channel), so a surface with two
    /// components on one channel has two bindings and one entry. Both halves of
    /// the departure walk — a replacement and a retirement — must ask the
    /// directory once, and a walk that asked twice would take the second answer
    /// as a host bug and kill the process mid-commit.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_surface_whose_components_share_a_channel_is_replaced_and_retired() {
        let tree = Tree::new();
        let assets = tempfile::tempdir().expect("a surface asset tree");
        write_surface_kind(&tree, assets.path(), "panel", "Panel");
        write_surface_kind_needing(&tree, assets.path(), "aside", "Aside", "dom");
        tree.write(&two_instance_document("deskbar", ""));
        let mut booted = boot_with_panel(&tree, assets.path()).await;
        assert_eq!(
            subscribed_anywhere(
                &booted.messenger,
                &SubscriberEntryKind::Surface("deskbar".to_string())
            )
            .len(),
            1,
            "two bindings on one channel are one directory subscriber",
        );

        tree.write(&two_instance_document(
            "deskbar",
            "    skin = \"foundry\";\n",
        ));
        booted.driver.reload(TriggerSource::Signal).await;
        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert_eq!(status.delta.surfaces_changed, vec!["deskbar".to_string()]);
        assert!(serves_surface(&booted, "deskbar"));
        assert_eq!(
            subscribed_anywhere(
                &booted.messenger,
                &SubscriberEntryKind::Surface("deskbar".to_string())
            )
            .len(),
            1,
            "the replacement re-folded the entry rather than doubling it",
        );

        tree.write(&document(&description_channels(
            "deskbar",
            &["panel", "aside"],
        )));
        booted.driver.reload(TriggerSource::Signal).await;
        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert_eq!(status.delta.surfaces_removed, vec!["deskbar".to_string()]);
        assert!(
            subscribed_anywhere(
                &booted.messenger,
                &SubscriberEntryKind::Surface("deskbar".to_string())
            )
            .is_empty(),
        );
    }

    /// **A corrupt sidecar under an upgraded kind is a refusal, not a panic.**
    /// The description builders read each kind's `.schema.json` off the mount
    /// and a malformed one is a boot panic; at reload the process is already
    /// serving a document, so the honest answer is that nothing happens. The
    /// artifact moves too, because a sidecar alone is in no fingerprint and
    /// would leave the reload with no document to rebuild.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_kind_upgraded_with_an_unparseable_schema_sidecar_is_refused() {
        let (tree, assets, mut booted) = boot_one_panel_surface("").await;

        let spec = std::fs::read(tree.modules().join("panel.brenn")).expect("the class module");
        brenn_surface_server::test_fixtures::write_processor_tree_from_bytes(
            assets.path(),
            "panel",
            b"the-upgraded-artifact",
            &spec,
            Vec::new(),
            true,
            |_| {},
        );
        std::fs::write(
            assets.path().join("brenn_panel.schema.json"),
            b"{ this is not json",
        )
        .expect("the sidecar is writable");
        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Refused, "{:?}", status.delta);
        assert_eq!(status.refusals.len(), 1, "{:?}", status.refusals);
        assert!(
            status.refusals[0].contains("schema sidecar")
                && status.refusals[0].contains("not valid JSON"),
            "{:?}",
            status.refusals,
        );
        assert!(serves_surface(&booted, "deskbar"), "nothing was touched");
    }

    /// A stand-in page that leaves the way the surface door's session really
    /// does: the registry slot goes first, and the terminal telemetry the route
    /// owes is published *after* it, under the surface's own identity.
    ///
    /// The gap between the two is a close-frame flush to a real client, which
    /// is tens to hundreds of milliseconds on anything but loopback; the sleep
    /// stands in for it. The task reports whether that publish succeeded — a
    /// reload that took the registration away while the page still owed a stamp
    /// answers `MissingSender`, which is a panic at the real call site.
    fn attach_a_session_that_owes_a_stamp(
        booted: &Booted,
        slug: &str,
    ) -> tokio::task::JoinHandle<brenn_messaging::PublishResult> {
        use brenn_attach_server::registry::{AttachSessionHandle, SessionCaps};

        let handle = AttachSessionHandle::for_test(READER);
        let mut close_rx = handle.close.subscribe();
        let registry = booted.driver.env.attach_registry.clone();
        let guard = registry
            .try_register(slug, handle, SessionCaps::UNCAPPED)
            .expect("uncapped registration");
        let ticket = registry.drain_ticket(slug);
        let messenger = Arc::clone(&booted.messenger);
        let channel = format!("brenn:{SURFACE_PREFIX}.surface.{slug}.status");
        let slug = slug.to_string();
        tokio::spawn(async move {
            let _ticket = ticket;
            loop {
                close_rx
                    .changed()
                    .await
                    .expect("the registry holds the sender until this guard drops");
                if close_rx.borrow_and_update().is_some() {
                    break;
                }
            }
            drop(guard);
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            messenger
                .publish_from_surface_platform(
                    &slug,
                    &channel,
                    "{}",
                    brenn_messaging::Urgency::Normal,
                )
                .await
        })
    }

    /// **A retired surface keeps its wiring until its last page has finished
    /// leaving.** The session releases its registry slot before the route
    /// publishes the terminal stamp, and that publish resolves the surface's
    /// own registration — so a wait that ended at "the slot list is empty"
    /// would retire the writer out from under a publish that panics on any
    /// answer but `Ok`.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_retired_surface_keeps_its_registration_until_the_stamp_is_written() {
        let (tree, _assets, mut booted) = boot_one_panel_surface("").await;
        let page = attach_a_session_that_owes_a_stamp(&booted, "deskbar");

        tree.write(&document(&description_channels("deskbar", &["panel"])));
        booted.driver.reload(TriggerSource::Signal).await;

        let published = page.await.expect("the stand-in page task");
        assert!(
            matches!(published, brenn_messaging::PublishResult::Ok { .. }),
            "the page still owed a stamp when the reload took the wiring: {published:?}",
        );
        assert!(!holds_surface_registration(&booted, "deskbar"));
        assert!(!serves_surface(&booted, "deskbar"));
    }

    /// **A kind nothing mounts moves the served roots and nothing else, and
    /// the retained outcome says so.** The projection is identical, so the
    /// verdict is `unchanged` — but the process is serving bytes it was not
    /// serving a moment ago, and the retained body is the only evidence the
    /// installer that just swapped the tree in ever sees.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_kind_no_surface_mounts_is_named_on_the_unchanged_outcome() {
        let (_tree, assets, mut booted) = boot_one_panel_surface("").await;

        // A second kind arrives under the mount with no surface instantiating
        // it — a bundle upgrade landing ahead of the document that uses it.
        brenn_surface_server::test_fixtures::write_processor_tree_from_bytes(
            assets.path(),
            "aside",
            b"the-aside-artifact",
            b"component Aside { abi = processor; requires = [dom]; }\n",
            Vec::new(),
            true,
            |_| {},
        );
        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Unchanged, "{:?}", status.refusals);
        assert_eq!(
            status.delta.kinds_changed,
            vec!["aside".to_string()],
            "the one thing that moved has to be in the body",
        );
        assert!(
            booted
                .driver
                .env
                .surface_roots
                .read()
                .expect("the cell is uncontended")
                .kinds
                .contains_key("aside"),
            "the roots the doors serve from are the ones just scanned",
        );
    }

    /// **A kernel rewritten in place is refused.** The kernel carries no
    /// manifest, so its root does not move when a sync or a rebuild overwrites
    /// the pair under it — and a page of an untouched surface keeps running the
    /// old bytes while every fresh load takes the new ones. That mixed state is
    /// what the refusal exists for, so the comparison has to be of bytes.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_kernel_rewritten_under_an_unmoved_root_is_refused() {
        let (_tree, assets, mut booted) = boot_one_panel_surface("").await;

        std::fs::write(
            assets.path().join(brenn_surface_server::KERNEL_ARTIFACT),
            b"export function instantiate() { /* the next release */ }",
        )
        .expect("the kernel module is writable");
        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Refused);
        assert_eq!(status.refusals.len(), 1, "{:?}", status.refusals);
        assert!(
            status.refusals[0].contains("rewritten in place")
                && status.refusals[0].ends_with(super::super::NEEDS_RESTART),
            "{:?}",
            status.refusals,
        );
        assert!(serves_surface(&booted, "deskbar"), "nothing was touched");
    }

    /// **A mount withdrawn from under a running surface's kind is refused.**
    /// The surface is unchanged and its assets are gone; converging would mean
    /// serving a page whose kind no root offers, so the honest answer is that
    /// nothing happens.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_mount_withdrawn_from_under_a_running_surface_is_refused() {
        let (_tree, _assets, mut booted) = boot_one_panel_surface("").await;

        booted.mounts.uninstall(SURFACE_MOUNT);
        booted.mounts.write();
        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Refused);
        assert!(
            status
                .refusals
                .iter()
                .any(|line| line.contains("`surface/` tree")),
            "{:?}",
            status.refusals,
        );
        assert!(serves_surface(&booted, "deskbar"), "nothing was touched");
    }

    // ---------------------------------------------------------------------
    // Webhook endpoints: the ingress edge converges.
    // ---------------------------------------------------------------------

    /// A document whose singleton agent owns one endpoint per entry in `slugs`.
    ///
    /// `blocks` is the `webhook` declarations themselves, so a case writes the
    /// scheme, the mount and the secret paths it is about; the agent side is
    /// the ownership rule's minimum — singleton, one user, a `subscribe` on
    /// each `webhook:` address and the `endpoint` ACL clause that admits it.
    pub(crate) fn document_with_webhooks(blocks: &str, slugs: &[&str]) -> String {
        let subscriptions: String = slugs
            .iter()
            .map(|slug| {
                format!(
                    "    subscribe \"webhook:{slug}\" {{ push_depth = 1; retain_depth = 4; }}\n"
                )
            })
            .collect();
        let subscribe_acl: String = slugs
            .iter()
            .map(|slug| format!(", endpoint \"webhook:{slug}\""))
            .collect();
        document_with_agent(
            blocks,
            &format!(
                r#"
agent Reader() {{
    working_dir = ".";
    singleton = true;
    compact_soft_pct = 70;
    allowed_users = ["alice"];
    grants = [subscribe, publish];
    send_budget = 1000000;
    acl subscribe [exact reload_outcomes, prefix "brenn:surface.", prefix "ephemeral:surface."{subscribe_acl}];
    acl publish [exact reload_requests, exact work];
{subscriptions}}}

new some-reader: Reader();
"#
            ),
        )
    }

    /// One `webhook` block over a bearer token read from `secret`.
    pub(crate) fn bearer_endpoint(slug: &str, mount: &str, secret: &std::path::Path) -> String {
        format!(
            r#"
webhook {slug} {{
    mount = "{mount}";
    signature {{
        scheme = bearer-token;
        header = "authorization";
    }}
    token phone {{ secret_file = "{}"; }}
}}
"#,
            secret.display(),
        )
    }

    /// The endpoint the service is serving under `slug`, or `None`.
    fn served(booted: &Booted, slug: &str) -> Option<Arc<brenn_webhook::EndpointRuntime>> {
        booted.webhook.endpoint_by_slug(slug)
    }

    /// The bearer tokens an endpoint verifies against, by token id.
    fn tokens(entry: &brenn_webhook::EndpointRuntime) -> HashMap<String, Vec<u8>> {
        match &entry.endpoint.scheme {
            brenn_lib::webhook::scheme::SignatureScheme::BearerToken { tokens, .. } => {
                tokens.clone()
            }
            other => panic!("the fixture endpoint is a bearer-token one, not {other:?}"),
        }
    }

    /// A first endpoint: installed in the table, its channel minted, its owner
    /// folded onto it, and named in the outcome. This is the deploy story the
    /// facility exists for — the endpoint block and the agent's subscription
    /// both arrive in one reload.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_added_endpoint_is_installed_with_its_owner_folded_on() {
        let tree = Tree::holding(&document_with_webhooks("", &[]));
        let mut booted = boot(&tree, vec![]).await;
        assert!(served(&booted, "inbox").is_none(), "nothing serves it yet");

        let secret = tree.secret("inbox.token", "s3cret");
        tree.write(&document_with_webhooks(
            &bearer_endpoint("inbox", "/webhooks/inbox", &secret),
            &["inbox"],
        ));
        booted.driver.reload(TriggerSource::Bus).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert_eq!(
            status.delta.webhook_endpoints_added,
            vec!["inbox".to_string()],
        );
        assert!(status.delta.webhook_endpoints_changed.is_empty());
        assert!(
            status
                .delta
                .subscriptions_added
                .iter()
                .any(|line| line == &format!("{READER} webhook:inbox")),
            "{:?}",
            status.delta.subscriptions_added,
        );

        let entry = served(&booted, "inbox").expect("the endpoint is serving");
        assert_eq!(entry.endpoint.mount, "/webhooks/inbox");
        assert_eq!(tokens(&entry)["phone"], b"s3cret".to_vec());
        // The mount index answers too: that is the lookup a request makes.
        assert_eq!(
            booted
                .webhook
                .endpoint_by_mount("/webhooks/inbox")
                .expect("the mount resolves")
                .slug(),
            "inbox",
        );
        assert!(
            subscribed_anywhere(
                &booted.messenger,
                &SubscriberEntryKind::App(READER.to_string())
            )
            .contains(&"webhook:inbox".to_string()),
            "the owning agent reads the endpoint's channel",
        );
    }

    /// The reverse: the block and the subscription leave together, and the
    /// endpoint is out of the table before the reload reports. From that
    /// instant its mount is an unrecognized URL.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_removed_endpoint_leaves_the_table() {
        let secret = tree_with_endpoint().await;
        let (tree, mut booted) = secret;
        tree.write(&document_with_webhooks("", &[]));
        booted.driver.reload(TriggerSource::Bus).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert_eq!(
            status.delta.webhook_endpoints_removed,
            vec!["inbox".to_string()],
        );
        assert!(served(&booted, "inbox").is_none());
        assert!(
            booted
                .webhook
                .endpoint_by_mount("/webhooks/inbox")
                .is_none()
        );
    }

    /// A rotated secret with no document edit at all: the resolved endpoint
    /// moved because its bytes did, so the reload is `applied` and names the
    /// endpoint. The comparison is over the resolved form, never the text.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_rotated_signing_secret_is_applied_without_a_document_edit() {
        let (tree, mut booted) = tree_with_endpoint().await;
        tree.secret("inbox.token", "rotated");

        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert_eq!(
            status.delta.webhook_endpoints_changed,
            vec!["inbox".to_string()],
        );
        let entry = served(&booted, "inbox").expect("the endpoint is serving");
        assert_eq!(tokens(&entry)["phone"], b"rotated".to_vec());
        // Nothing else moved: an endpoint's secret is not a channel edit.
        assert!(status.delta.channels_changed.is_empty());
        drop(tree);
    }

    /// An unreadable secret file refuses the whole reload in the environment
    /// grammar, and the running endpoint keeps verifying against the bytes it
    /// was serving. A fresh boot could not have produced that state either.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_missing_secret_file_refuses_the_reload_in_the_environment_grammar() {
        let (tree, mut booted) = tree_with_endpoint().await;
        std::fs::remove_file(tree.secret_path("inbox.token")).expect("the secret is removable");

        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Refused);
        assert!(
            status
                .refusals
                .iter()
                .any(|line| line.contains("inbox.token")),
            "the refusal names the file: {:?}",
            status.refusals,
        );
        let entry = served(&booted, "inbox").expect("nothing was touched");
        assert_eq!(tokens(&entry)["phone"], b"s3cret".to_vec());
    }

    /// Two endpoints swapping mounts: both are changed, and the table is
    /// derived in one swap, so no request can see the intermediate in which two
    /// entries claim one mount.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_mount_swap_between_two_endpoints_converges() {
        let tree = Tree::new();
        let first = tree.secret("first.token", "one");
        let second = tree.secret("second.token", "two");
        let document = |a: &str, b: &str| {
            format!(
                "{}{}",
                bearer_endpoint("first", a, &first),
                bearer_endpoint("second", b, &second),
            )
        };
        tree.write(&document_with_webhooks(
            &document("/webhooks/a", "/webhooks/b"),
            &["first", "second"],
        ));
        let mut booted = boot(&tree, vec![]).await;

        tree.write(&document_with_webhooks(
            &document("/webhooks/b", "/webhooks/a"),
            &["first", "second"],
        ));
        booted.driver.reload(TriggerSource::Bus).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert_eq!(
            status.delta.webhook_endpoints_changed,
            vec!["first".to_string(), "second".to_string()],
            "changed follows candidate document order",
        );
        assert_eq!(
            booted
                .webhook
                .endpoint_by_mount("/webhooks/a")
                .expect("the mount resolves")
                .slug(),
            "second",
        );
        assert_eq!(
            booted
                .webhook
                .endpoint_by_mount("/webhooks/b")
                .expect("the mount resolves")
                .slug(),
            "first",
        );
    }

    /// A components root holding one `brenn:replay` package, in the layout the
    /// resolver reads: the artifact, and a record binding it.
    fn install_replay_package(root: &std::path::Path, name: &str) {
        install_replay_package_from(root, name, "brenn_replay.wasm");
    }

    /// [`install_replay_package`] over a named artifact, for the case whose
    /// subject is the bytes under the package rather than the document over it.
    fn install_replay_package_from(root: &std::path::Path, name: &str, artifact: &str) {
        let dir = root.join(name);
        std::fs::create_dir_all(&dir).expect("a package directory");
        let bytes = crate::consumers::fixture_artifact(artifact);
        std::fs::write(dir.join(format!("{name}.wasm")), &bytes).expect("write the artifact");
        std::fs::write(
            dir.join("package.json"),
            format!(
                "{{\n  \"v\": 2,\n  \"name\": \"{name}\",\n  \"world\": \"brenn:replay\",\n  \
                 \"artifact\": \"{name}.wasm\",\n  \"artifact_sha256\": \"{}\"\n}}\n",
                brenn_lib::util::sha256_hex(&bytes),
            ),
        )
        .expect("write the record");
    }

    /// A replay-protected endpoint arriving at reload: the component is
    /// compiled at prepare, and its store — held by nobody, since this endpoint
    /// is new — is opened at commit, in that order. The proof that the store
    /// was really opened is that a request path can take the guard's lock and
    /// find a component whose `check` runs; an unopened one panics on the first
    /// read.
    ///
    /// A second reload that moves nothing about the replay block then carries
    /// the same guard forward, which is what keeps a store from being opened
    /// twice over one file.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_added_replay_protected_endpoint_opens_its_store_at_commit() {
        let components = tempfile::tempdir().expect("a components root");
        install_replay_package(components.path(), "replay-generic");
        let tree = Tree::new();
        let secret = tree.secret("inbox.token", "s3cret");
        let store = tree.secret_path("replay.sqlite");
        let endpoint = |ceiling: usize| {
            format!(
                r#"
webhook inbox {{
    mount = "/webhooks/inbox";
    transport_ceiling_bytes = {ceiling};
    signature {{
        scheme = bearer-token;
        header = "authorization";
    }}
    token phone {{ secret_file = "{}"; }}
    replay_protection {{
        component = "replay-generic";
        store_path = "{}";
    }}
}}
"#,
                secret.display(),
                store.display(),
            )
        };
        tree.write(&document_with_webhooks("", &[]));
        let mut booted = boot(&tree, vec![components.path().to_path_buf()]).await;

        tree.write(&document_with_webhooks(&endpoint(1024), &["inbox"]));
        booted.driver.reload(TriggerSource::Bus).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        let entry = served(&booted, "inbox").expect("the endpoint is serving");
        let guard = entry
            .replay
            .clone()
            .expect("the endpoint is replay-protected");
        {
            let slot = guard.slot.lock().await;
            let component = slot.as_ref().expect("the component is installed");
            // A `check` reads the store; it would panic if commit had not
            // opened it. The verdict itself is the component's business.
            let (verdict, _quota_hit) = component.check(&brenn_wasm::CheckInput {
                headers: Vec::new(),
                body: b"{}".to_vec(),
                received_at: 0,
                key_id: "phone".to_string(),
                endpoint_slug: "inbox".to_string(),
            });
            let _ = verdict;
        }

        // A ceiling edit is a changed endpoint whose replay block did not
        // move: the guard travels, component, store and lock together.
        tree.write(&document_with_webhooks(&endpoint(2048), &["inbox"]));
        booted.driver.reload(TriggerSource::Bus).await;
        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert_eq!(
            status.delta.webhook_endpoints_changed,
            vec!["inbox".to_string()],
        );
        let after = served(&booted, "inbox").expect("the endpoint is serving");
        assert_eq!(after.endpoint.transport_ceiling_bytes, 2048);
        assert!(
            Arc::ptr_eq(
                &guard,
                &after.replay.clone().expect("still replay-protected")
            ),
            "an unmoved replay block keeps the guard it is being served through",
        );
    }

    /// One replay-protected bearer endpoint over `store`, with an explicit
    /// store cap so a case can move the replay block without moving the path.
    fn replay_endpoint(
        slug: &str,
        mount: &str,
        secret: &std::path::Path,
        store: &std::path::Path,
        size_limit: &str,
    ) -> String {
        format!(
            r#"
webhook {slug} {{
    mount = "{mount}";
    signature {{
        scheme = bearer-token;
        header = "authorization";
    }}
    token phone {{ secret_file = "{}"; }}
    replay_protection {{
        component = "replay-generic";
        store_path = "{}";
        store_size_limit = "{size_limit}";
    }}
}}
"#,
            secret.display(),
            store.display(),
        )
    }

    /// One instant every replay case checks at, and its envelope spelling. The
    /// component's skew window is five minutes wide, so a nonce written before
    /// a reload is still live after it.
    const REPLAY_NOW_MS: u64 = 1_748_000_000_000;
    const REPLAY_NOW_RFC3339: &str = "2025-05-23T11:33:20.000Z";

    /// Run one `check` through the component in this entry's guard, and hand
    /// back the verdict.
    ///
    /// Two things are proven by calling this at all. A component whose store
    /// was never opened panics on the first read, so reaching past the call is
    /// the proof that commit opened one. And the verdict says *which* file: the
    /// component records the last `sent_at` it accepted per client, so a second
    /// envelope at the same instant is a `MonotonicityViolation` against a
    /// store that already holds the first one and an accept against a store
    /// that does not. That is how a case tells "the file at that path" from
    /// "some empty file".
    async fn replay_check_at(
        entry: &brenn_webhook::EndpointRuntime,
        nonce: &str,
    ) -> Result<(), brenn_wasm::ReplayError> {
        let guard = entry
            .replay
            .clone()
            .expect("the endpoint is replay-protected");
        let slot = guard.slot.lock().await;
        let component = slot.as_ref().expect("the component is installed");
        let (verdict, _quota_hit) = component.check(&replay_input(entry.slug(), nonce));
        verdict
    }

    /// One phonebuddy envelope at [`REPLAY_NOW_MS`], the shape the fixture's
    /// replay component parses.
    fn replay_input(endpoint_slug: &str, nonce: &str) -> brenn_wasm::CheckInput {
        let body = format!(
            r#"{{"client_id":"phone","sent_at":"{REPLAY_NOW_RFC3339}","nonce":"{nonce}"}}"#
        );
        brenn_wasm::CheckInput {
            headers: Vec::new(),
            body: body.into_bytes(),
            received_at: REPLAY_NOW_MS,
            key_id: "phone".to_string(),
            endpoint_slug: endpoint_slug.to_string(),
        }
    }

    /// One envelope no store has seen, asserted accepted — which also leaves
    /// the accepting store holding this instant for `phone`.
    async fn replay_check(entry: &brenn_webhook::EndpointRuntime) {
        let nonce = format!("first-sight-{}", entry.slug());
        let verdict = replay_check_at(entry, &nonce).await;
        assert!(
            verdict.is_ok(),
            "a first-sight envelope against a freshly opened store must be accepted: {verdict:?}",
        );
    }

    /// Whether the store this entry checks against already accepted an envelope
    /// at [`REPLAY_NOW_MS`] — the discriminator for "this component reads the
    /// bytes at that path".
    async fn replay_store_carries_history(entry: &brenn_webhook::EndpointRuntime) -> bool {
        let nonce = format!("probe-{}", entry.slug());
        match replay_check_at(entry, &nonce).await {
            Ok(()) => false,
            Err(brenn_wasm::ReplayError::MonotonicityViolation) => true,
            other => panic!("unexpected verdict from the history probe: {other:?}"),
        }
    }

    /// This entry's replay guard, by identity.
    fn guard_of(entry: &brenn_webhook::EndpointRuntime) -> Arc<brenn_webhook::ReplayGuard> {
        entry
            .replay
            .clone()
            .expect("the endpoint is replay-protected")
    }

    /// A replay block that moved over an unmoved store path: the arriving
    /// component is compiled at prepare, the retiring one is dropped at 21w,
    /// and only then is the same file opened again. Nothing in the planner
    /// forbids the pair — the path is used once in the candidate — so the
    /// commit order is the whole of what keeps two holders off one file.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_changed_replay_component_over_the_same_store_path_converges() {
        let components = tempfile::tempdir().expect("a components root");
        install_replay_package(components.path(), "replay-generic");
        let tree = Tree::new();
        let secret = tree.secret("inbox.token", "s3cret");
        let store = tree.secret_path("replay.sqlite");
        tree.write(&document_with_webhooks(
            &replay_endpoint("inbox", "/webhooks/inbox", &secret, &store, "64MiB"),
            &["inbox"],
        ));
        let mut booted = boot(&tree, vec![components.path().to_path_buf()]).await;
        let serving = served(&booted, "inbox").expect("serving");
        let before = guard_of(&serving);
        // Leaves this instant in the store, so the component installed by the
        // reload can be held to reading the same file rather than a new one.
        replay_check(&serving).await;

        tree.write(&document_with_webhooks(
            &replay_endpoint("inbox", "/webhooks/inbox", &secret, &store, "32MiB"),
            &["inbox"],
        ));
        booted.driver.reload(TriggerSource::Bus).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert_eq!(
            status.delta.webhook_endpoints_changed,
            vec!["inbox".to_string()],
        );
        let after = served(&booted, "inbox").expect("serving");
        assert!(
            !Arc::ptr_eq(&before, &guard_of(&after)),
            "a moved replay block is a fresh guard, not the running one"
        );
        assert!(
            replay_store_carries_history(&after).await,
            "the arriving component reads the store the retired one wrote to",
        );
    }

    /// An added endpoint taking a removed endpoint's store path. The planner's
    /// uniqueness check passes — one use in the candidate — and the retiring
    /// holder is dropped before the arriving one opens the file.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_added_endpoint_reusing_a_removed_endpoints_store_path_converges() {
        let components = tempfile::tempdir().expect("a components root");
        install_replay_package(components.path(), "replay-generic");
        let tree = Tree::new();
        let secret = tree.secret("inbox.token", "s3cret");
        let store = tree.secret_path("replay.sqlite");
        tree.write(&document_with_webhooks(
            &replay_endpoint("inbox", "/webhooks/inbox", &secret, &store, "64MiB"),
            &["inbox"],
        ));
        let mut booted = boot(&tree, vec![components.path().to_path_buf()]).await;
        replay_check(&served(&booted, "inbox").expect("serving")).await;

        tree.write(&document_with_webhooks(
            &replay_endpoint("mailbox", "/webhooks/mailbox", &secret, &store, "64MiB"),
            &["mailbox"],
        ));
        booted.driver.reload(TriggerSource::Bus).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert_eq!(
            status.delta.webhook_endpoints_removed,
            vec!["inbox".to_string()],
        );
        assert!(
            served(&booted, "inbox").is_none(),
            "the removed one is gone"
        );
        assert!(
            replay_store_carries_history(&served(&booted, "mailbox").expect("serving")).await,
            "the arriving endpoint inherited the file, not merely the path",
        );
    }

    /// Two endpoints swapping store paths in one reload: both guards are fresh,
    /// both old holders are dropped at 21w, and both files are opened at 22w.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_store_path_swap_between_two_endpoints_converges() {
        let components = tempfile::tempdir().expect("a components root");
        install_replay_package(components.path(), "replay-generic");
        let tree = Tree::new();
        let secret = tree.secret("inbox.token", "s3cret");
        let first = tree.secret_path("first.sqlite");
        let second = tree.secret_path("second.sqlite");
        let pair = |a: &std::path::Path, b: &std::path::Path| {
            format!(
                "{}{}",
                replay_endpoint("inbox", "/webhooks/inbox", &secret, a, "64MiB"),
                replay_endpoint("mailbox", "/webhooks/mailbox", &secret, b, "64MiB"),
            )
        };
        tree.write(&document_with_webhooks(
            &pair(&first, &second),
            &["inbox", "mailbox"],
        ));
        let mut booted = boot(&tree, vec![components.path().to_path_buf()]).await;
        // Only `first` is written to, so after the swap the endpoint holding
        // `first` must see the history and the one holding `second` must not.
        replay_check(&served(&booted, "inbox").expect("serving")).await;

        tree.write(&document_with_webhooks(
            &pair(&second, &first),
            &["inbox", "mailbox"],
        ));
        booted.driver.reload(TriggerSource::Bus).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert_eq!(
            status.delta.webhook_endpoints_changed,
            vec!["inbox".to_string(), "mailbox".to_string()],
        );
        let inbox = served(&booted, "inbox").expect("serving");
        let mailbox = served(&booted, "mailbox").expect("serving");
        assert_eq!(guard_of(&inbox).store_path, second);
        assert_eq!(guard_of(&mailbox).store_path, first);
        assert!(
            replay_store_carries_history(&mailbox).await,
            "mailbox inherited the file inbox had written to",
        );
        assert!(
            !replay_store_carries_history(&inbox).await,
            "inbox took the other file, which nothing had written to",
        );
    }

    /// The cross-subsystem handover, and the whole reason 21w sits before
    /// `start_consumers` rather than inside 22w: the store-path namespace is
    /// one namespace, the planner holds it unique over the candidate alone, and
    /// so a candidate may hand a retiring endpoint's store to an arriving
    /// consumer. Only the commit order keeps the two holders apart.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_consumer_taking_a_removed_endpoints_store_path_converges() {
        let components = tempfile::tempdir().expect("a components root");
        install_replay_package(components.path(), "replay-generic");
        let tree = Tree::new();
        let secret = tree.secret("inbox.token", "s3cret");
        let store = tree.secret_path("replay.sqlite");
        tree.write(&document_with_webhooks(
            &replay_endpoint("inbox", "/webhooks/inbox", &secret, &store, "64MiB"),
            &["inbox"],
        ));
        let mut booted = boot(&tree, vec![components.path().to_path_buf()]).await;
        replay_check(&served(&booted, "inbox").expect("serving")).await;

        tree.write(&document_with_webhooks(
            &format!(
                r#"{PACKAGED}component Sifter {{
    abi = processor;
    requires = [ports, store];
    in inbound;
    out digest;
}}
{PACKAGED}

new sifter: Sifter {{
    grants = [ports, store];
    store_path = "{}";
    in inbound <- work {{ push_depth = 4; }}
    out digest -> scratch;
}}
"#,
                store.display(),
            ),
            &[],
        ));
        install_package(components.path(), &staged_module(&tree));
        booted.driver.reload(TriggerSource::Bus).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert_eq!(
            status.delta.webhook_endpoints_removed,
            vec!["inbox".to_string()],
        );
        assert_eq!(status.delta.consumers_added, vec!["sifter".to_string()]);
        assert!(
            served(&booted, "inbox").is_none(),
            "the endpoint that held the store is gone"
        );
    }

    /// A bundle release that ships new bytes under the package an unmoved
    /// `replay_protection` block names. A fresh boot would compile the new
    /// artifact, so the reload must install it: the endpoint is `changed` with
    /// a fresh guard, and the store is handed from the old component to the new
    /// one. Nothing in the document moved.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_bumped_replay_package_is_applied_under_an_unmoved_document() {
        let components = tempfile::tempdir().expect("a components root");
        install_replay_package(components.path(), "replay-generic");
        let tree = Tree::new();
        let secret = tree.secret("inbox.token", "s3cret");
        let store = tree.secret_path("replay.sqlite");
        let document = document_with_webhooks(
            &replay_endpoint("inbox", "/webhooks/inbox", &secret, &store, "64MiB"),
            &["inbox"],
        );
        tree.write(&document);
        let mut booted = boot(&tree, vec![components.path().to_path_buf()]).await;
        let before = guard_of(&served(&booted, "inbox").expect("serving"));

        // Same package name, same document, different artifact bytes.
        install_replay_package_from(
            components.path(),
            "replay-generic",
            "brenn_replay_generic.wasm",
        );
        tree.write(&document);
        booted.driver.reload(TriggerSource::Bus).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert_eq!(
            status.delta.webhook_endpoints_changed,
            vec!["inbox".to_string()],
            "a bumped package is a changed endpoint, not an unchanged reload",
        );
        let after = guard_of(&served(&booted, "inbox").expect("serving"));
        assert!(
            !Arc::ptr_eq(&before, &after),
            "the endpoint serves the component compiled from the new bytes",
        );
        assert_eq!(
            after.verified.artifact_sha256,
            brenn_lib::util::sha256_hex(&crate::consumers::fixture_artifact(
                "brenn_replay_generic.wasm"
            )),
            "the installed guard carries the release it was compiled from",
        );
    }

    /// The same bytes under the same document are not a handover: nothing is
    /// reported and the running guard keeps serving, so a re-deploy or a
    /// rollback whose artifact is identical costs the endpoint nothing.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_reinstalled_replay_package_with_the_same_bytes_is_unchanged() {
        let components = tempfile::tempdir().expect("a components root");
        install_replay_package(components.path(), "replay-generic");
        let tree = Tree::new();
        let secret = tree.secret("inbox.token", "s3cret");
        let store = tree.secret_path("replay.sqlite");
        let document = document_with_webhooks(
            &replay_endpoint("inbox", "/webhooks/inbox", &secret, &store, "64MiB"),
            &["inbox"],
        );
        tree.write(&document);
        let mut booted = boot(&tree, vec![components.path().to_path_buf()]).await;
        let before = guard_of(&served(&booted, "inbox").expect("serving"));

        install_replay_package(components.path(), "replay-generic");
        tree.write(&document);
        booted.driver.reload(TriggerSource::Bus).await;

        let status = booted.last_status().await;
        assert!(
            status.delta.webhook_endpoints_changed.is_empty(),
            "{status:?}"
        );
        assert!(
            Arc::ptr_eq(
                &before,
                &guard_of(&served(&booted, "inbox").expect("serving"))
            ),
            "identical bytes leave the running component in place",
        );
    }

    /// A replay store whose directory is not on this host: refused at prepare,
    /// in the environment grammar, naming the path. The alternative is a commit
    /// that reaches `open_store` and aborts the process — every live session
    /// with it.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_replay_store_under_a_missing_directory_refuses_the_reload() {
        let components = tempfile::tempdir().expect("a components root");
        install_replay_package(components.path(), "replay-generic");
        let tree = Tree::new();
        let secret = tree.secret("inbox.token", "s3cret");
        tree.write(&document_with_webhooks("", &[]));
        let mut booted = boot(&tree, vec![components.path().to_path_buf()]).await;

        let missing = std::path::Path::new("/nonexistent-brenn-store-dir/replay.sqlite");
        tree.write(&document_with_webhooks(
            &replay_endpoint("inbox", "/webhooks/inbox", &secret, missing, "64MiB"),
            &["inbox"],
        ));
        booted.driver.reload(TriggerSource::Bus).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Refused);
        assert!(
            status
                .refusals
                .iter()
                .any(|line| line.contains("parent directory does not exist")
                    && line.contains("nonexistent-brenn-store-dir")),
            "the refusal names the directory: {:?}",
            status.refusals,
        );
        assert!(served(&booted, "inbox").is_none(), "nothing was installed");
    }

    /// A replay component whose package no declared mount holds: refused at
    /// prepare in the environment grammar, naming the package. The refusal is
    /// the whole point of compiling at 7w — past it, commit's `open_store` and
    /// its `expect` take the process down.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_missing_replay_package_refuses_the_reload() {
        let components = tempfile::tempdir().expect("a components root");
        install_replay_package(components.path(), "replay-generic");
        let tree = Tree::new();
        let secret = tree.secret("inbox.token", "s3cret");
        let store = tree.secret_path("replay.sqlite");
        tree.write(&document_with_webhooks("", &[]));
        let mut booted = boot(&tree, vec![components.path().to_path_buf()]).await;

        tree.write(&document_with_webhooks(
            &format!(
                r#"
webhook inbox {{
    mount = "/webhooks/inbox";
    signature {{
        scheme = bearer-token;
        header = "authorization";
    }}
    token phone {{ secret_file = "{}"; }}
    replay_protection {{
        component = "replay-nowhere";
        store_path = "{}";
    }}
}}
"#,
                secret.display(),
                store.display(),
            ),
            &["inbox"],
        ));
        booted.driver.reload(TriggerSource::Bus).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Refused);
        assert!(
            status
                .refusals
                .iter()
                .any(|line| line.contains("replay-nowhere")),
            "the refusal names the package: {:?}",
            status.refusals,
        );
        assert!(served(&booted, "inbox").is_none(), "nothing was installed");
    }

    /// A candidate the webhook *document* half refuses — two endpoints on one
    /// mount — is reported in the resolver's own words. Not through the planner
    /// classifier, which would frame an operator's duplicate mount as a
    /// possible host defect.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_duplicate_mount_is_refused_in_the_resolvers_own_words() {
        let tree = Tree::new();
        let secret = tree.secret("inbox.token", "s3cret");
        tree.write(&document_with_webhooks(
            &bearer_endpoint("inbox", "/webhooks/inbox", &secret),
            &["inbox"],
        ));
        let mut booted = boot(&tree, vec![]).await;

        tree.write(&document_with_webhooks(
            &format!(
                "{}{}",
                bearer_endpoint("inbox", "/webhooks/shared", &secret),
                bearer_endpoint("mailbox", "/webhooks/shared", &secret),
            ),
            &["inbox", "mailbox"],
        ));
        booted.driver.reload(TriggerSource::Bus).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Refused);
        assert!(
            status
                .refusals
                .iter()
                .any(|line| line.starts_with("[[webhook_endpoint]]")
                    && line.contains("/webhooks/shared")),
            "the refusal is the resolver's: {:?}",
            status.refusals,
        );
        assert!(
            !status
                .refusals
                .iter()
                .any(|line| line.contains("host defect")),
            "an operator's duplicate mount is not framed as a host defect: {:?}",
            status.refusals,
        );
        assert_eq!(
            served(&booted, "inbox")
                .expect("still serving")
                .endpoint
                .mount,
            "/webhooks/inbox",
            "nothing was touched",
        );
    }

    /// Replay protection gained by an endpoint that keeps its slug, then lost
    /// again. The losing transition is the only one where a live guard is
    /// emptied with no arriving store to open, and the proof that its holder
    /// was really released is that a third reload can protect the same path
    /// again — a file still held would panic on the second open.
    #[tokio::test(flavor = "multi_thread")]
    async fn replay_protection_gained_and_lost_on_a_surviving_endpoint_converges() {
        let components = tempfile::tempdir().expect("a components root");
        install_replay_package(components.path(), "replay-generic");
        let tree = Tree::new();
        let secret = tree.secret("inbox.token", "s3cret");
        let store = tree.secret_path("replay.sqlite");
        let bare = document_with_webhooks(
            &bearer_endpoint("inbox", "/webhooks/inbox", &secret),
            &["inbox"],
        );
        let protected = document_with_webhooks(
            &replay_endpoint("inbox", "/webhooks/inbox", &secret, &store, "64MiB"),
            &["inbox"],
        );
        tree.write(&bare);
        let mut booted = boot(&tree, vec![components.path().to_path_buf()]).await;
        assert!(
            served(&booted, "inbox").expect("serving").replay.is_none(),
            "the endpoint boots unprotected",
        );

        // Gained.
        tree.write(&protected);
        booted.driver.reload(TriggerSource::Bus).await;
        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert_eq!(
            status.delta.webhook_endpoints_changed,
            vec!["inbox".to_string()],
        );
        replay_check(&served(&booted, "inbox").expect("serving")).await;

        // Lost: the guard goes, and with it the holder of the file.
        tree.write(&bare);
        booted.driver.reload(TriggerSource::Bus).await;
        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        let entry = served(&booted, "inbox").expect("serving");
        assert!(
            entry.replay.is_none(),
            "an endpoint that lost its replay block serves without a guard",
        );

        // Gained again over the same path. This is what proves the release:
        // a component still holding that file makes commit's open panic.
        tree.write(&protected);
        booted.driver.reload(TriggerSource::Bus).await;
        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert!(
            replay_store_carries_history(&served(&booted, "inbox").expect("serving")).await,
            "the re-added protection opened the same file, which nothing was holding",
        );
    }

    /// An endpoint's owner moving from the agent that subscribes to it to a
    /// WASM consumer's `in` port. The endpoint block itself does not move: what
    /// moves is who reads the channel it mints, which the resolver stamps onto
    /// the endpoint — so this reaches the delta twice, as a changed endpoint and
    /// as a withdrawn agent subscription.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_owner_change_from_agent_to_consumer_converges() {
        let components = tempfile::tempdir().expect("a components root");
        let tree = Tree::new();
        let secret = tree.secret("inbox.token", "s3cret");
        let endpoint = bearer_endpoint("inbox", "/webhooks/inbox", &secret);
        tree.write(&document_with_webhooks(&endpoint, &["inbox"]));
        let mut booted = boot(&tree, vec![components.path().to_path_buf()]).await;
        assert!(
            matches!(
                served(&booted, "inbox").expect("serving").endpoint.owner,
                brenn_lib::webhook::config::WebhookOwner::App(_),
            ),
            "the agent owns it at boot",
        );

        tree.write(&document_with_webhooks(
            &format!(
                r#"{endpoint}{PACKAGED}component Sifter {{
    abi = processor;
    requires = [ports];
    in hooked;
    out digest;
}}
{PACKAGED}

new sifter: Sifter {{
    grants = [ports];
    in hooked <- "webhook:inbox" {{ push_depth = 4; retain_depth = 8; }}
    out digest -> scratch;
}}
"#
            ),
            &[],
        ));
        install_package(components.path(), &staged_module(&tree));
        booted.driver.reload(TriggerSource::Bus).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert_eq!(
            status.delta.webhook_endpoints_changed,
            vec!["inbox".to_string()],
            "the stamped owner moved",
        );
        assert!(
            status
                .delta
                .subscriptions_removed
                .iter()
                .any(|line| line == &format!("{READER} webhook:inbox")),
            "the agent's subscription is withdrawn: {:?}",
            status.delta.subscriptions_removed,
        );
        assert_eq!(status.delta.consumers_added, vec!["sifter".to_string()]);
        let entry = served(&booted, "inbox").expect("serving");
        assert_eq!(
            entry.endpoint.owner,
            brenn_lib::webhook::config::WebhookOwner::Wasm(Arc::from("sifter")),
        );
        let channel = booted
            .messenger
            .directory()
            .resolve("webhook:inbox")
            .expect("the channel stays at its address through the owner change");
        assert!(
            channel.subscribers.iter().any(|s| matches!(
                &s.kind,
                brenn_lib::messaging::SubscriberEntryKind::Wasm(slug) if &**slug == "sifter"
            )),
            "the new owner is folded onto the channel it now reads",
        );
    }

    /// The reverse edit, which is not an owner change but an orphan: the
    /// endpoint block stays and its only subscriber leaves. Refused offline and
    /// here, in rule 9's own words.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_endpoint_left_without_a_subscriber_refuses_the_reload() {
        let (tree, mut booted) = tree_with_endpoint().await;
        let secret = tree.secret_path("inbox.token");

        tree.write(&document_with_webhooks(
            &bearer_endpoint("inbox", "/webhooks/inbox", &secret),
            &[],
        ));
        booted.driver.reload(TriggerSource::Bus).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Refused);
        assert!(
            status
                .refusals
                .iter()
                .any(|line| line.contains("orphan endpoints are not permitted")),
            "{:?}",
            status.refusals,
        );
        assert!(
            served(&booted, "inbox").is_some(),
            "the running endpoint keeps serving"
        );
    }

    /// A request that took the guard's lock before commit reaches 21w checks
    /// against the component it holds, and commit waits behind it. The other
    /// half of the invariant — a request that arrives after 21w finds an empty
    /// slot and answers 503 — is
    /// `inbound.rs`'s `a_request_whose_guard_was_emptied_answers_503_and_publishes_nothing`.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_removed_endpoints_component_serves_a_request_holding_its_guard() {
        let components = tempfile::tempdir().expect("a components root");
        install_replay_package(components.path(), "replay-generic");
        let tree = Tree::new();
        let secret = tree.secret("inbox.token", "s3cret");
        let store = tree.secret_path("replay.sqlite");
        tree.write(&document_with_webhooks(
            &replay_endpoint("inbox", "/webhooks/inbox", &secret, &store, "64MiB"),
            &["inbox"],
        ));
        let mut booted = boot(&tree, vec![components.path().to_path_buf()]).await;
        let guard = guard_of(&served(&booted, "inbox").expect("serving"));

        // Stand in for a request that has verified its signature and is now in
        // the replay step: it holds the slot lock across the whole reload.
        let held = Arc::clone(&guard);
        let (locked_tx, locked_rx) = tokio::sync::oneshot::channel();
        let holder = tokio::spawn(async move {
            let slot = held.slot.lock().await;
            let component = Arc::clone(slot.as_ref().expect("the running component"));
            locked_tx.send(()).expect("the test is waiting");
            // Long enough that commit's 21w is certainly waiting on this lock.
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;
            let (verdict, _quota_hit) = component.check(&replay_input("inbox", "in-flight"));
            verdict
        });
        locked_rx.await.expect("the holder took the lock");

        tree.write(&document_with_webhooks("", &[]));
        booted.driver.reload(TriggerSource::Bus).await;

        let verdict = holder.await.expect("the holder task");
        assert!(
            verdict.is_ok(),
            "the in-flight request checked against the component it held: {verdict:?}",
        );
        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert!(served(&booted, "inbox").is_none(), "the endpoint is gone");
        assert!(
            guard.slot.lock().await.is_none(),
            "commit emptied the guard once the request let go of it",
        );
    }

    /// A booted process serving one bearer endpoint whose token is `s3cret`.
    async fn tree_with_endpoint() -> (Tree, Booted) {
        let tree = Tree::new();
        let secret = tree.secret("inbox.token", "s3cret");
        tree.write(&document_with_webhooks(
            &bearer_endpoint("inbox", "/webhooks/inbox", &secret),
            &["inbox"],
        ));
        let booted = boot(&tree, vec![]).await;
        (tree, booted)
    }

    // ---------------------------------------------------------------------
    // Config-carrying mounts: a mount's `config/` tree is part of the document
    // ---------------------------------------------------------------------

    /// The name every case below declares its config mount under, and so the
    /// namespace every handle its fragment writes hangs beneath.
    pub(crate) const CONFIG_MOUNT: &str = "automations";

    /// The ceiling the operator writes for that mount: a namespace of its own
    /// to build in, the work channel to read, and the one grant word its
    /// consumers need.
    pub(crate) const CEILING: &str = r#"
principal automator {
    grants = [ports];
    acl publish [prefix "brenn:automations.", exact work];
    acl subscribe [prefix "brenn:automations.", exact work];
}
"#;

    /// A second ceiling, identical in reach, for the case that moves a mount
    /// from one principal to another and expects nothing to move with it.
    const SPARE_CEILING: &str = r#"
principal spare-automator {
    grants = [ports];
    acl publish [prefix "brenn:automations.", exact work];
    acl subscribe [prefix "brenn:automations.", exact work];
}
"#;

    /// The deployment half of a config-mount case: the floor, `ceilings`, and
    /// the component class a fragment instantiates.
    ///
    /// The class is declared here and stamped nowhere: a fragment declares no
    /// component class (a mounted document instantiates and declares none), so
    /// the vocabulary has to reach it through the packaged module the way an
    /// installed package's does.
    ///
    /// `ceilings` is a parameter because a ceiling is live only while a mount
    /// is under it: a principal nothing delegates to is dead config and refused,
    /// so the operator's `principal` line and the mounts document's `under`
    /// line arrive together and leave together.
    pub(crate) fn document_with(ceilings: &str) -> String {
        document(&format!(
            r#"{ceilings}
{PACKAGED}component Demo {{
    abi = processor;
    requires = [ports];
    in inbound;
    out digest;
}}
{PACKAGED}
"#
        ))
    }

    /// A fragment declaring one channel in its ceiling's namespace and one
    /// consumer reading the deployment's work channel — which it can only spell
    /// by address, handles not crossing an authority boundary.
    ///
    /// `retain` sizes the declared channel, so a case can edit the fragment
    /// into a document that differs in exactly one convergible number.
    pub(crate) fn fragment(retain: u32) -> String {
        format!(
            r#"use @{PACKAGED_MODULE}::*;

channel digest at "brenn:automations.digest" {{
    push_depth = 1;
    retain_depth = {retain};
    standing_retain_depth = 64;
}}

new sifter: Demo {{
    grants = [ports];
    in inbound <- "brenn:work" {{ push_depth = 4; }}
    out digest -> digest;
}}
"#
        )
    }

    /// The address the fragment declares, and the slug its consumer takes —
    /// the mount's name leading the handle written inside it.
    pub(crate) const FRAGMENT_ADDRESS: &str = "brenn:automations.digest";
    pub(crate) const FRAGMENT_CONSUMER: &str = "automations.sifter";

    /// Boot a process over a document with no ceiling and no config mount
    /// declared, and hand back the tree, the components root and the process.
    ///
    /// The root is complete without its mounts, so this boots clean — which is
    /// the precondition every case here rests on, and the reason a fragment can
    /// be added by a reload at all.
    async fn boot_without_fragment() -> (Tree, tempfile::TempDir, Booted) {
        let tree = Tree::holding(&document_with(""));
        let components = tempfile::tempdir().expect("a components root");
        install_package(components.path(), &staged_module(&tree));
        let booted = boot_with(
            &tree,
            BootFixture {
                components_roots: vec![components.path().to_path_buf()],
                ..BootFixture::default()
            },
        )
        .await;
        (tree, components, booted)
    }

    /// Rewrite the root document and re-install the package its fenced half
    /// becomes.
    ///
    /// Both halves or neither: the packaged module is the file the fragment's
    /// class was declared in, so a root edit that moves the module's bytes moves
    /// the spec hash the consumer carries, and an install left behind is the
    /// spec-binding refusal rather than the case's own subject.
    pub(crate) fn restage(tree: &Tree, components: &tempfile::TempDir, text: &str) {
        tree.write(text);
        install_package(components.path(), &staged_module(tree));
    }

    /// [`boot_without_fragment`] with the ceiling written, the config mount
    /// declared under it and its fragment applied by one reload: where every
    /// case that edits, retires or re-ceilings a fragment starts.
    async fn boot_with_fragment() -> (Tree, tempfile::TempDir, Booted) {
        let (tree, components, mut booted) = boot_without_fragment().await;
        restage(&tree, &components, &document_with(CEILING));
        booted
            .mounts
            .config(CONFIG_MOUNT, "automator", &fragment(4));
        booted.driver.reload(TriggerSource::Signal).await;
        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        (tree, components, booted)
    }

    /// The whole shape this slice exists for: a directory the operator declared
    /// `under` a principal carries a document, and what that document declares
    /// arrives by reload, under the mount's name, with no restart.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_fragment_arrives_by_reload_under_its_mount_s_name() {
        let (tree, components, mut booted) = boot_without_fragment().await;
        assert!(
            booted
                .messenger
                .directory()
                .resolve(FRAGMENT_ADDRESS)
                .is_none(),
            "the fragment is not declared yet",
        );

        restage(&tree, &components, &document_with(CEILING));
        booted
            .mounts
            .config(CONFIG_MOUNT, "automator", &fragment(4));
        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert_eq!(
            status.delta.channels_added,
            vec![FRAGMENT_ADDRESS.to_string()]
        );
        assert_eq!(
            status.delta.consumers_added,
            vec![FRAGMENT_CONSUMER.to_string()],
            "the mount's name leads every handle its config writes",
        );
        assert!(
            booted
                .messenger
                .directory()
                .resolve(FRAGMENT_ADDRESS)
                .is_some(),
            "the fragment's channel is in the directory",
        );
        let mount = status
            .mounts
            .iter()
            .find(|mount| mount.name == CONFIG_MOUNT)
            .expect("the status body lists the config mount");
        assert_eq!(mount.under.as_deref(), Some("automator"));
        assert!(mount.trees.contains(&"config".to_string()), "{mount:?}");
    }

    /// A fragment edit is a level-2 delta by construction: everything a mounted
    /// document may declare lowers into the convergible blocks. The assertion
    /// is on the level-1 comparison being empty, not merely on the outcome —
    /// an `applied` reload would not distinguish "converged" from "there was
    /// nothing a restart would have been needed for".
    #[tokio::test(flavor = "multi_thread")]
    async fn a_fragment_edit_needs_no_restart() {
        let (_tree, _components, mut booted) = boot_with_fragment().await;

        booted.mounts.edit(CONFIG_MOUNT, &fragment(16));
        let baseline = booted.driver.baseline().document.config.clone();
        let candidate = check_config(&deployment_inputs(
            &booted.driver.env.config_path,
            &booted.mounts.load().roots,
        ))
        .expect("the edited fragment compiles");
        let level_one = non_convergible_differences(&baseline, &candidate.config);
        assert!(
            level_one.refusals.is_empty(),
            "a fragment edit is a level-2 delta: {:?}",
            level_one.refusals,
        );

        booted.driver.reload(TriggerSource::Signal).await;
        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert_eq!(
            status.delta.channels_changed,
            vec![FRAGMENT_ADDRESS.to_string()]
        );
    }

    /// A fragment that does not compile refuses the whole reload — the
    /// operator's own pending edits with it — with the old document still
    /// running and the refusal naming the mount's file, line and column.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_fragment_that_does_not_compile_refuses_the_reload() {
        let (_tree, _components, mut booted) = boot_with_fragment().await;
        let running = booted.driver.baseline().document.document_sha256.clone();

        booted.mounts.edit(CONFIG_MOUNT, "\nchannel broken at {\n");
        assert!(
            booted
                .driver
                .prepare_and_report(TriggerSource::Bus)
                .await
                .is_none()
        );

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Refused);
        assert_eq!(status.running_document_sha256, running);
        let report = status.refusals.join("\n");
        assert!(
            report.contains(&format!("{CONFIG_MOUNT}/config/main.brenn"))
                && report.contains("line 2"),
            "the refusal names the mount's own file on disk, with a line: {report}",
        );
        assert_eq!(
            booted.driver.baseline().document.document_sha256,
            running,
            "the old document is still running",
        );
    }

    /// A fragment reaching past its ceiling is refused on the same terms, which
    /// is the rule this slice exists for: the mount's `under` line, not the
    /// filesystem, is what bounds what its author can put on the bus.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_fragment_exceeding_its_ceiling_refuses_the_reload() {
        let (_tree, _components, mut booted) = boot_with_fragment().await;
        let running = booted.driver.baseline().document.document_sha256.clone();

        booted.mounts.edit(
            CONFIG_MOUNT,
            &format!(
                "{}\nchannel elsewhere at \"brenn:elsewhere\" {{ push_depth = 1; \
                 retain_depth = 4; standing_retain_depth = 4; }}\n",
                fragment(4),
            ),
        );
        assert!(
            booted
                .driver
                .prepare_and_report(TriggerSource::Bus)
                .await
                .is_none()
        );

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Refused);
        let report = status.refusals.join("\n");
        assert!(
            report.contains("brenn:elsewhere") && report.contains("automator"),
            "the refusal names the address and the ceiling: {report}",
        );
        assert!(
            report.contains(&format!("{CONFIG_MOUNT}/config/main.brenn:15:")),
            "positioned at the declaration, in the mount's own file: {report}",
        );
        assert_eq!(booted.driver.baseline().document.document_sha256, running);
    }

    /// The declared-channel rule above is the ceiling's second line of defence.
    /// The first is the fit rule, over what the fragment's own bodies *confer*:
    /// an `acl` reaching past the ceiling is reach the operator never wrote,
    /// whether or not any channel is declared for it.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_fragment_acl_past_its_ceiling_refuses_the_reload() {
        let (_tree, _components, mut booted) = boot_with_fragment().await;
        let running = booted.driver.baseline().document.document_sha256.clone();

        booted.mounts.edit(
            CONFIG_MOUNT,
            &fragment(4).replace(
                "    grants = [ports];\n",
                "    grants = [ports];\n    acl subscribe [prefix \"brenn:secrets.\"];\n",
            ),
        );
        assert!(
            booted
                .driver
                .prepare_and_report(TriggerSource::Bus)
                .await
                .is_none()
        );

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Refused);
        let report = status.refusals.join("\n");
        assert!(
            report.contains("brenn:secrets.")
                && report.contains(&format!("the config of mount `{CONFIG_MOUNT}`")),
            "the refusal names the reach and the stamp that would hold it: {report}",
        );
        assert_eq!(booted.driver.baseline().document.document_sha256, running);
    }

    /// The operator's lever: the mount line. Taking it out retires everything
    /// the fragment declared, the way a deleted module's entities are retired.
    /// The ceiling goes with it, because a principal nothing delegates to is
    /// dead config.
    #[tokio::test(flavor = "multi_thread")]
    async fn removing_a_config_mount_retires_what_its_fragment_declared() {
        let (tree, components, mut booted) = boot_with_fragment().await;

        restage(&tree, &components, &document_with(""));
        booted.mounts.uninstall(CONFIG_MOUNT);
        booted.mounts.write();
        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        assert_eq!(
            status.delta.consumers_removed,
            vec![FRAGMENT_CONSUMER.to_string()]
        );
        assert_eq!(
            status.delta.channels_removed,
            vec![FRAGMENT_ADDRESS.to_string()]
        );
        assert!(
            status.mounts.iter().all(|mount| mount.name != CONFIG_MOUNT),
            "the mount is gone from the status body too",
        );
    }

    /// Moving a mount's ceiling to a different principal of the same reach
    /// recompiles the fragment under it and lowers to the same projection: a
    /// ceiling is a compile-time bound and nothing that runs reads it, so there
    /// is nothing for a reload to promote.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_ceiling_moved_to_a_principal_that_fits_changes_nothing() {
        let (tree, components, mut booted) = boot_with_fragment().await;

        restage(&tree, &components, &document_with(SPARE_CEILING));
        booted.mounts.move_ceiling(CONFIG_MOUNT, "spare-automator");
        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Unchanged, "{:?}", status.refusals);
        let mount = status
            .mounts
            .iter()
            .find(|mount| mount.name == CONFIG_MOUNT)
            .expect("the status body lists the config mount");
        assert_eq!(mount.under.as_deref(), Some("spare-automator"));
    }

    /// A `config/` tree with no entry document is a *document* fault, not a
    /// mount-verification one: the file's absence is reported by the compiler
    /// at step 1, in the same report as every other document refusal, and
    /// positioned at the mounts document's own `mount` line.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_config_mount_with_no_entry_document_refuses_at_step_one() {
        let (tree, components, mut booted) = boot_without_fragment().await;
        restage(&tree, &components, &document_with(CEILING));
        let config = booted
            .mounts
            .config(CONFIG_MOUNT, "automator", &fragment(4));
        std::fs::remove_file(config.join("main.brenn")).expect("the entry is removable");

        assert!(
            booted
                .driver
                .prepare_and_report(TriggerSource::Bus)
                .await
                .is_none()
        );
        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Refused);
        let report = status.refusals.join("\n");
        assert!(
            report.contains(CONFIG_MOUNT) && report.contains("main.brenn"),
            "the refusal names the mount and the file it wanted: {report}",
        );
        // And it is positioned on the mount's own name, not on the `path` value
        // further along the line. Which of the two the operator is sent to is
        // the difference between "this mount is broken" and "this path is
        // wrong".
        let mounts = booted.mounts.path().display().to_string();
        let positioned = report
            .lines()
            .find(|line| line.starts_with(&mounts))
            .unwrap_or_else(|| panic!("the refusal is not in the mounts document: {report}"));
        let column = positioned
            .strip_prefix(&format!("{mounts}:"))
            .and_then(|rest| rest.split(':').nth(1))
            .unwrap_or_else(|| panic!("the refusal carries no position: {positioned}"));
        assert_eq!(column, (b"mount ".len() + 1).to_string(), "{positioned}");
    }
}
