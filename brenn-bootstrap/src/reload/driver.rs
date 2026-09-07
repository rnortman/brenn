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
    AppConfig, DocumentInputs, LoadedDocument, LoadedMounts, Roots, check_config, try_load_mounts,
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
use crate::reload::compare::non_convergible_differences;
use crate::reload::delta::{PlanDelta, PlanFacts, convergibility_refusals, plan_delta};
use crate::reload::mqtt::ReferencedClient;
use crate::reload::surfaces::{
    SurfaceDocInputs, SurfaceDocParams, SurfaceDocs, arriving, build_surface_docs,
    system_participant_refusals,
};
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
/// because level 1 refuses any candidate that would have moved one: an app map,
/// a client identity, a tool registry and a replay store path are all
/// projections of blocks a reload cannot converge. The roots are not among
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
    pub apps: Arc<IndexMap<String, AppConfig>>,
    pub mqtt_clients: IndexMap<String, MqttClientIdentity>,
    pub tool_registry: Arc<brenn_tool_registry::ToolRegistry>,
    pub replay_store_paths: Vec<PathBuf>,
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
    pub mqtt_service: Option<Arc<brenn_mqtt::MqttService>>,
    /// The concrete ingress router, whose route table the commit adds to and
    /// removes from. Present on exactly the terms `mqtt_service` is: both come
    /// off the one `MqttResult`.
    pub mqtt_event_router: Option<Arc<brenn_server::mqtt_router::MqttEventRouterImpl>>,
    pub max_payload_bytes: usize,
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
    pub fn prepare(&self, source: TriggerSource) -> Prepared {
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

        // 2. Level 1: everything a reload cannot converge must be equal.
        let differences =
            non_convergible_differences(&self.baseline.document.config, &candidate.config);
        if !differences.is_empty() {
            return refused(Some(sha), differences);
        }

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
        let plan = match self.plan_of(&candidate) {
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
        let baseline_records = self.baseline_records();
        let baseline_facts = PlanFacts {
            directory: &self.baseline.directory,
            consumers: &self.baseline.consumers,
            records: &baseline_records,
            mqtt_ingress: &self.baseline.mqtt_ingress,
            surfaces: &self.baseline.surfaces,
        };
        let candidate_facts = PlanFacts {
            directory: &plan.directory,
            consumers: &plan.wasm_consumers,
            records: &candidate_records,
            mqtt_ingress: &plan.mqtt_ingress_channels,
            surfaces: &plan.surfaces,
        };
        let delta = plan_delta(
            &baseline_facts,
            &candidate_facts,
            kind_differences.into_keys().collect(),
        );
        let mut refusals = convergibility_refusals(
            &baseline_facts,
            &candidate_facts,
            &delta,
            self.env.messenger.directory(),
        );
        // Rule 6, the one rule that reads the MQTT runtime rather than the two
        // plans: a broker session is a boot-time fact in both directions.
        refusals.extend(super::mqtt::session_refusals(
            &delta.mqtt,
            &self.sessions(),
            &self.referenced_clients(&plan),
        ));
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
            applied,
        }))
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
    /// the module roots this reload's mounts derive.
    fn inputs(&self, roots: &Roots) -> DocumentInputs {
        DocumentInputs {
            root: self.env.config_path.clone(),
            module_roots: roots.module_roots.clone(),
            role: brenn_lib::config::DocumentRole::Deployment,
        }
    }

    /// Lower a candidate document with the booted plan inputs.
    fn plan_of(&self, candidate: &LoadedDocument) -> Result<MessagingPlan, Vec<String>> {
        let planned = catch_quietly(AssertUnwindSafe(|| {
            plan_messaging(&PlanInputs {
                config: &candidate.config,
                apps: Some(&self.env.apps),
                mqtt_clients: &self.env.mqtt_clients,
                tool_registry: Some(&self.env.tool_registry),
                replay_store_paths: &self.env.replay_store_paths,
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

    /// Which clients have a live broker session right now.
    ///
    /// The registry is written at boot and never after, so this is the set a
    /// fresh boot of the *baseline* document produced — which is exactly what
    /// rule 6 compares the candidate's reference set against.
    fn sessions(&self) -> Vec<String> {
        self.env
            .mqtt_service
            .as_ref()
            .map(|service| service.client_slugs())
            .unwrap_or_default()
    }

    /// The clients a fresh boot of the candidate would spawn a supervisor for,
    /// each with what in the candidate names it.
    ///
    /// The same three sources boot's derivation reads, in the same order, over
    /// the candidate's ingress channels and consumer policies and the booted
    /// app policies — the apps are level-1 frozen, so theirs is the candidate's
    /// set too. The attribution lets rule 6's refusal name the line that asked
    /// for the session.
    fn referenced_clients(&self, plan: &MessagingPlan) -> Vec<ReferencedClient> {
        let mut out: Vec<ReferencedClient> = Vec::new();
        let mut push = |client: &str, named_by: String| {
            if !out.iter().any(|one| one.client == client) {
                out.push(ReferencedClient {
                    client: client.to_string(),
                    named_by,
                });
            }
        };
        for channel in &plan.mqtt_ingress_channels {
            push(&channel.client_slug, channel.channel_address.clone());
        }
        for (slug, app) in self.env.apps.iter() {
            for matcher in &app.policy.acls.mqtt_publish {
                push(
                    &matcher.client,
                    format!("app `{slug}`'s `mqtt_publish` matcher"),
                );
            }
            for matcher in &app.policy.acls.mqtt_subscribe {
                push(
                    &matcher.client,
                    format!("app `{slug}`'s `mqtt_subscribe` matcher"),
                );
            }
        }
        for consumer in &plan.wasm_consumers {
            for matcher in &consumer.policy.acls.mqtt_publish {
                push(
                    &matcher.client,
                    format!("consumer `{}`'s `mqtt_publish` matcher", consumer.slug),
                );
            }
        }
        debug_assert_eq!(
            out.iter()
                .map(|one| one.client.as_str())
                .collect::<Vec<_>>(),
            crate::mqtt::referenced_clients(
                &plan.mqtt_ingress_channels,
                self.env.apps.values().map(|app| &app.policy),
                plan.wasm_consumers.iter().map(|consumer| &consumer.policy),
            )
            .into_iter()
            .collect::<Vec<_>>(),
            "rule 6's reference set has drifted from the one boot spawns sessions for",
        );
        out
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
        let prepared = tokio::task::block_in_place(|| self.prepare(source));
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
            mut applied,
        } = ready;
        let sha = document.document_sha256.clone();
        // The walk is `async` throughout — it awaits a stopping consumer's last
        // drain step and the database — so unlike prepare it is not the
        // blocking pool's to run.
        let deferred = match super::commit::apply(
            &self.env,
            &mut self.registry,
            &plan,
            &delta,
            loaded,
            &records,
            &super::commit::SurfaceCommit {
                roots: &surface_roots,
                runtimes: &surface_runtimes,
                docs: &surface_docs,
                prefix: &document.config.surface_description.prefix,
            },
        )
        .await
        {
            Ok(deferred) => deferred,
            Err(refusals) => {
                self.report_refusal(source, Some(sha.clone()), refusals)
                    .await;
                return;
            }
        };
        // The one delta field prepare could not know: which filters the broker
        // took now and which it will take on the next connect. Prepare measured
        // the body with every moved filter listed here, so replacing that with
        // the ones that actually deferred only shrinks it.
        applied.delta.mqtt_deferred = deferred;
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
            "reload applied"
        );
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

fn refused(document_sha256: Option<String>, refusals: Vec<String>) -> Prepared {
    Prepared::Refused {
        document_sha256,
        refusals,
    }
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

    use brenn_lib::access::test_fixtures::delivery_policy_for_addresses;
    use brenn_lib::config::{BrennConfig, PACKAGED, PACKAGED_MODULE};
    use brenn_lib::messaging::SubscriberEntryKind;
    use brenn_messaging::config_reload::{RELOAD_ADDRESS, STATUS_ADDRESS};
    use brenn_messaging::query::MessageQuery;
    use brenn_obs::alerting::make_capturing_alerter_with_severity;
    use brenn_server::messaging_router::DeliveryBinding;
    use brenn_server::test_support::init_db_memory;
    use rusqlite::OptionalExtension;

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
    // Effectively unrated: the case that publishes continuously across a
    // reload asks what a publish meets while a subscriber is leaving, and a
    // throttle inside that window would answer a different question.
    send_rate = {{ burst = 1000000, refill_interval_secs = 1, refill = 1000000 }};
}}

channel scratch at "ephemeral:scratch" {{
    push_depth = 1;
    retain_depth = 4;
}}
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

        pub(crate) fn modules(&self) -> PathBuf {
            self.dir.path().join("modules")
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
                text.push_str(&format!(
                    "mount {name} {{ path = \"{}\"; }}\n",
                    path.display()
                ));
            }
            std::fs::write(self.path(), text).expect("the mounts document is writable");
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

    /// A booted process the driver decides against: the messaging layer the
    /// document brought up, the driver holding that document as its baseline,
    /// and the alerts anything raised.
    pub(crate) struct Booted {
        pub(crate) driver: ReloadDriver,
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
        /// The MQTT runtime the reload walks, when the fixture asked for one.
        pub(crate) mqtt: Option<(
            Arc<brenn_mqtt::MqttService>,
            Arc<brenn_server::mqtt_router::MqttEventRouterImpl>,
        )>,
        /// The stop signals of the live supervisors, when the fixture booted
        /// against a real broker.
        ///
        /// Held so a fixture can fire them. Dropping them stops nothing: each
        /// supervisor holds a sender of its own inside its
        /// `MqttClientHandle`, so the channel never closes while the
        /// supervisor runs. A fixture that does not call `stop_mqtt()` leaves
        /// a connected supervisor under the document's client id for the rest
        /// of the test binary.
        pub(crate) mqtt_stop_txs: Vec<tokio::sync::watch::Sender<bool>>,
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
            replay_store_paths: &[],
        })
        .expect("the fixture document configures messaging")
    }

    /// The `[[mqtt_client]]` identities a fixture document declares.
    fn client_identities(config: &BrennConfig) -> IndexMap<String, MqttClientIdentity> {
        brenn_lib::mqtt::config::resolve_client_identities(&config.mqtt_clients)
    }

    /// A live `MqttService` and ingress router over the plan's **referenced**
    /// clients, as boot builds them.
    ///
    /// Referenced and not declared, because that is the set boot spawns a
    /// session for and the set rule 6 is defined against: a client the document
    /// declares and nothing binds has no session on a real host, and a fixture
    /// that gave it one would be a fixture rule 6 cannot be tested on.
    ///
    /// The handles are registered and no supervisor is spawned, so every session
    /// exists and none has a connection: a SUBSCRIBE at commit comes back
    /// `DeferredDisconnected`, which is the outcome a broker-down reload has and
    /// the one that leaves the filter in the reconnect-survival set for a test to
    /// read.
    async fn mqtt_runtime(
        plan: &MessagingPlan,
        apps: &IndexMap<String, AppConfig>,
        db: &brenn_db::Db,
    ) -> (
        Arc<brenn_mqtt::MqttService>,
        Arc<brenn_server::mqtt_router::MqttEventRouterImpl>,
    ) {
        use brenn_server::mqtt_router::IngressRoute;

        let referenced = crate::mqtt::referenced_clients(
            &plan.mqtt_ingress_channels,
            apps.values().map(|app| &app.policy),
            plan.wasm_consumers.iter().map(|consumer| &consumer.policy),
        );
        let service = brenn_mqtt::MqttService::new();
        for slug in referenced {
            let (stop_tx, _stop_rx) = tokio::sync::watch::channel(false);
            service
                .add_client(brenn_mqtt::MqttClientHandle::new(
                    Arc::new(brenn_server::test_support::mqtt::test_client_config(slug)),
                    brenn_mqtt::union_subscriptions(slug, &plan.mqtt_ingress_channels),
                    stop_tx,
                ))
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
        apps: &Arc<IndexMap<String, AppConfig>>,
        plan: &MessagingPlan,
        db: &brenn_db::Db,
        messenger: &Arc<Messenger>,
    ) -> (
        (
            Arc<brenn_mqtt::MqttService>,
            Arc<brenn_server::mqtt_router::MqttEventRouterImpl>,
        ),
        Vec<tokio::sync::watch::Sender<bool>>,
    ) {
        let clients = brenn_lib::mqtt::config::resolve_clients(&config.mqtt_clients);
        let result = crate::mqtt::start_mqtt(
            config,
            apps,
            &plan.wasm_consumers,
            &plan.mqtt_ingress_channels,
            &clients,
        )
        .await;
        let service = result
            .service
            .expect("a live fixture declares and references a client");
        let router = result
            .event_router
            .expect("a live fixture declares and references a client");
        // The state the router delivers through must carry the messenger this
        // process publishes with: an inbound packet reaches the bus through it,
        // and a state without one panics the delivery path on the first
        // message.
        let mut state = brenn_server::test_support::state::test_state(db);
        state.messenger = Some(messenger.clone());
        let stop_txs = crate::mqtt::wire_mqtt_state(
            &service,
            &router,
            state,
            &plan.mqtt_ingress_channels,
            result.stop_txs,
        )
        .await;
        ((service, router), stop_txs)
    }

    /// The reader app's resolved subscriptions on `addresses`.
    ///
    /// Pull-only (`push_depth = 0`) on purpose: an `App` subscriber entry is
    /// what these cases want on the channel, and a push-enabled one would put
    /// the conversation-delivery path between the publish and the consumer this
    /// is about.
    fn static_subscriptions(
        config: &BrennConfig,
        addresses: &[&str],
    ) -> Vec<brenn_lib::messaging::config::ResolvedSubscription> {
        use brenn_lib::messaging::config::{Depth, NoiseLevel, ResolvedSubscription};
        use brenn_lib::messaging::directory::WakeMin;

        addresses
            .iter()
            .map(|address| {
                let raw = config
                    .channels
                    .iter()
                    .find(|channel| channel.address.as_deref() == Some(*address))
                    .unwrap_or_else(|| panic!("the fixture document declares {address}"));
                let uuid = raw
                    .uuid
                    .as_deref()
                    .expect("a durable channel carries the uuid its row is named by");
                // A subscriber may not retain more than the channel's standing
                // buffer holds, so the block's own number is the ceiling; four
                // is deep enough for anything these fixtures read.
                let retain_depth = match raw.standing_retain_depth {
                    Some(Depth::Bounded(standing)) => Depth::Bounded(standing.min(4)),
                    _ => Depth::Bounded(4),
                };
                ResolvedSubscription {
                    channel_uuid: uuid::Uuid::parse_str(uuid).expect("a lowered uuid parses"),
                    channel_address: (*address).to_string(),
                    push_depth: Depth::Bounded(0),
                    retain_depth,
                    noise: NoiseLevel::Silent,
                    wake_min: WakeMin::Never,
                }
            })
            .collect()
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
        /// `brenn:` addresses the reader app holds a **static subscription** on.
        ///
        /// An address here seats an `App` subscriber entry on the channel; a
        /// policy alone does not. The reader's policy is also extended to match
        /// each address.
        pub(crate) reader_subscriptions: Vec<&'static str>,
        /// Addresses the reader app may *read* without holding a subscriber
        /// entry on them.
        ///
        /// Separate from `reader_subscriptions` because seating an `App`
        /// subscriber on a substrate-owned channel — a surface's config
        /// channel, a description document — would change the topology the
        /// case is about; a policy alone is all a `query` needs.
        pub(crate) reader_reads: Vec<&'static str>,
        /// A deployed surface asset tree to declare as one more mount, in the
        /// shape a release installs: the kernel pair at its root and one
        /// `processor/<kind>/` directory per kind it serves.
        pub(crate) surface_assets: Option<PathBuf>,
        /// Stand up a live `MqttService` and ingress router over the document's
        /// `mqtt_client` declarations. Off by default: a process that never
        /// referenced a client has neither, which is the shape rule 6's first
        /// arm is about.
        pub(crate) mqtt: bool,
        /// Stand up the MQTT subsystem the way boot does, against whatever
        /// broker the document's `mqtt_client` names — one real supervisor per
        /// referenced client, dialing and staying connected.
        ///
        /// Distinct from `mqtt`, which registers handles and spawns nothing:
        /// that fixture is for the cases about the *plan*, where every
        /// SUBSCRIBE defers and no packet moves. This one is for the cases
        /// about the wire, and it needs a broker to be listening.
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
            reader_subscriptions,
            reader_reads,
            surface_assets,
            mqtt,
            mqtt_live,
            dispatcher,
        } = fixture;
        assert!(
            !(mqtt && mqtt_live),
            "a fixture asks for one MQTT runtime or the other, not both",
        );
        // The roots every load below reads are the mounts document's, exactly
        // as `run_server` derives them: a record read out of a path the reload
        // would not name is a baseline that disagrees with every candidate.
        let mounts = tree.mounts(&components_roots);
        if let Some(assets) = &surface_assets {
            mounts.install(SURFACE_MOUNT, &[("surface", assets)]);
        }
        let loaded_mounts = mounts.load();
        let components_roots = loaded_mounts.roots.components_roots.clone();
        let module_roots = loaded_mounts.roots.module_roots.clone();
        let db = db.unwrap_or_else(init_db_memory);
        let tool_registry = tool_registry
            .unwrap_or_else(|| Arc::new(brenn_tool_registry::ToolRegistry::new(vec![])));
        let reader_subscriptions = reader_subscriptions.as_slice();
        let document = check_config(&DocumentInputs {
            root: tree.root(),
            module_roots,
            role: brenn_lib::config::DocumentRole::Deployment,
        })
        .expect("the fixture document must load");
        let subscriptions = static_subscriptions(&document.config, reader_subscriptions);
        let messaging = (!subscriptions.is_empty()).then_some(
            brenn_lib::messaging::config::ResolvedMessagingConfig {
                send_budget: 1_000_000,
                subscriptions,
            },
        );
        let mut reader =
            brenn_server::test_support::app_config::minimal_app_config(READER, messaging, vec![]);
        // The conversation send budget these fixtures publish against. Raised
        // off the default of 100 for the case that publishes continuously
        // across a reload: the budget is not what any of these tests is about,
        // and a reload outlasting it would fail on the budget instead.
        reader.messaging_default_send_budget = 1_000_000;
        reader.policy = delivery_policy_for_addresses(
            std::iter::once(STATUS_ADDRESS)
                .chain(reader_subscriptions.iter().copied())
                .chain(reader_reads.iter().copied()),
        );
        reader
            .policy
            .grants
            .insert(brenn_envelope::grants::AppCapability::MessagingPublish);
        reader
            .policy
            .acls
            .brenn_publish
            .push(brenn_lib::access::acl::ChannelMatcher::Exact(
                RELOAD_ADDRESS
                    .strip_prefix("brenn:")
                    .expect("the request channel is a brenn: address")
                    .to_string(),
            ));
        // The work channel too: the cases that publish across a reload do it as
        // this app, which is the only principal these fixtures seat.
        reader
            .policy
            .acls
            .brenn_publish
            .push(brenn_lib::access::acl::ChannelMatcher::Exact(
                "work".to_string(),
            ));
        let mut map: IndexMap<String, AppConfig> = IndexMap::new();
        map.insert(READER.to_string(), reader);
        let apps = Arc::new(map);
        let (alert_dispatcher, captured, _drain) = make_capturing_alerter_with_severity();

        let result = brenn_messaging_boot::test_fixtures::boot_messaging_with_tools(
            &document.config,
            db.clone(),
            &apps,
            alert_dispatcher.clone(),
            "brenn://test",
            &tool_registry,
        )
        .await;
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
        let (mqtt, mqtt_stop_txs) = match (mqtt, mqtt_live) {
            (true, _) => (Some(mqtt_runtime(&plan, &apps, &db).await), Vec::new()),
            (_, true) => {
                let (runtime, stop_txs) =
                    live_mqtt_runtime(&document.config, &apps, &plan, &db, &messenger).await;
                (Some(runtime), stop_txs)
            }
            _ => (None, Vec::new()),
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
                    mqtt_service: None,
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
        let attach_registry = brenn_attach_server::registry::AttachRegistry::default();

        let surface_roots = brenn_surface_server::validate_surface_assets(
            &loaded_mounts.roots.surface_roots,
            &plan.surfaces,
        );

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
        let driver = ReloadDriver::new(
            ReloadEnv {
                config_path: tree.root(),
                root: Some(root),
                build_id: TEST_BUILD_ID,
                apps,
                mqtt_clients: client_identities(&document.config),
                tool_registry,
                replay_store_paths: Vec::new(),
                surface_roots: Arc::new(std::sync::RwLock::new(Arc::new(surface_roots))),
                surfaces: surfaces_cell,
                attach_registry,
                mqtt_service: mqtt.as_ref().map(|(service, _)| service.clone()),
                mqtt_event_router: mqtt.as_ref().map(|(_, router)| router.clone()),
                max_payload_bytes: document.config.messaging.max_body_bytes,
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
            mounts,
            mqtt,
            mqtt_stop_txs,
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
        let ready = match booted.driver.prepare(TriggerSource::Signal) {
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
        std::fs::read_to_string(tree.modules().join(format!("{PACKAGED_MODULE}.brenn")))
            .expect("the staged module is readable")
    }

    /// A document declaring one consumer of the `processor-config` fixture
    /// component: it reads the work channel, and each directive it takes off it
    /// is answered on the sink channel with what its `config` map holds.
    pub(crate) fn document_with_a_configured_consumer(value: &str) -> String {
        document(&format!(
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
        ))
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
        let ports: String = (0..topics.len())
            .map(|index| format!("    in inbound{index};\n"))
            .collect();
        let bindings: String = topics
            .iter()
            .enumerate()
            .map(|(index, topic)| {
                format!(
                    "    in inbound{index} <- \"mqtt:ha:{topic}\" {{ push_depth = 4; \
                     retain_depth = 4; }}\n"
                )
            })
            .collect();
        document(&format!(
            r#"mqtt_client ha {{
    url = "mqtts://127.0.0.1:8883";
    qos = 1;
}}

channel sink at "brenn:sink" {{
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
{bindings}    out digest -> sink;
}}
"#
        ))
    }

    /// Two declared brokers and a consumer bound to one `mqtt:` topic per
    /// `(client, topic)` pair. A client no pair names is declared and
    /// referenced by nothing, so boot spawns no supervisor for it.
    pub(crate) fn document_with_two_brokers(bindings: &[(&str, &str)]) -> String {
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
            r#"mqtt_client ha {{
    url = "mqtts://127.0.0.1:8883";
    qos = 1;
}}

mqtt_client spare {{
    url = "mqtts://127.0.0.1:8884";
    qos = 1;
}}

channel sink at "brenn:sink" {{
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

    /// Rule 6's first arm on the arrangement it is actually about: a client the
    /// document declares, with a live service beside it that holds no session
    /// for it because nothing referenced it at boot.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_mqtt_binding_on_a_declared_but_unreferenced_client_is_refused() {
        let tree = Tree::holding(&document_with_two_brokers(&[("ha", "home/state")]));
        let components = tempfile::tempdir().expect("a components root");
        install_package(components.path(), &staged_module(&tree));
        let mut booted = boot_with(
            &tree,
            BootFixture {
                components_roots: vec![components.path().to_path_buf()],
                mqtt: true,
                ..BootFixture::default()
            },
        )
        .await;

        tree.write(&document_with_two_brokers(&[
            ("ha", "home/state"),
            ("spare", "home/other"),
        ]));
        // The second binding adds a port, so the component's specification
        // moves with it; the installed package has to be the one the candidate
        // document names or the refusal would be the spec-binding one.
        install_package(components.path(), &staged_module(&tree));
        assert!(
            booted
                .driver
                .prepare_and_report(TriggerSource::Bus)
                .await
                .is_none()
        );
        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Refused);
        assert!(
            status.refusals.iter().any(|line| {
                line.contains("mqtt:spare:home/other")
                    && line.contains("has no broker session")
                    && line.ends_with(super::super::NEEDS_RESTART)
            }),
            "{:?}",
            status.refusals,
        );
    }

    /// The broker document with nothing referencing it, which is a client a
    /// fresh boot spawns no supervisor for.
    pub(crate) fn document_with_a_broker_only() -> String {
        document(
            r#"mqtt_client ha {
    url = "mqtts://127.0.0.1:8883";
    qos = 1;
}
"#,
        )
    }

    /// Rule 6's first arm. A client only ever named by a document is a client
    /// with no supervisor: nothing spawned one at boot because nothing
    /// referenced it, and a reload does not start one.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_mqtt_binding_on_a_client_with_no_session_is_refused() {
        const TOPIC: &str = "home/state";
        let tree = Tree::holding(&document_with_a_broker_only());
        let components = tempfile::tempdir().expect("a components root");
        let mut booted = boot(&tree, vec![components.path().to_path_buf()]).await;

        tree.write(&document_with_an_mqtt_consumer(&[TOPIC]));
        install_package(components.path(), &staged_module(&tree));

        assert!(
            booted
                .driver
                .prepare_and_report(TriggerSource::Bus)
                .await
                .is_none()
        );
        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Refused);
        assert!(
            status.refusals.iter().any(|line| {
                line.contains(&format!("mqtt:ha:{TOPIC}"))
                    && line.contains("has no broker session")
                    && line.ends_with(super::super::NEEDS_RESTART)
            }),
            "{:?}",
            status.refusals,
        );
    }

    /// Rule 6's second arm. Dropping the last thing that referenced a client
    /// would leave a supervisor a fresh boot would not have spawned, and
    /// stopping one at reload is not something this facility does.
    #[tokio::test(flavor = "multi_thread")]
    async fn dropping_a_clients_last_reference_is_refused() {
        let tree = Tree::holding(&document_with_an_mqtt_consumer(&["home/state"]));
        let components = tempfile::tempdir().expect("a components root");
        install_package(components.path(), &staged_module(&tree));
        let mut booted = boot_with(
            &tree,
            BootFixture {
                components_roots: vec![components.path().to_path_buf()],
                mqtt: true,
                ..BootFixture::default()
            },
        )
        .await;

        tree.write(&document_with_a_broker_only());
        assert!(
            booted
                .driver
                .prepare_and_report(TriggerSource::Bus)
                .await
                .is_none()
        );
        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Refused);
        assert_eq!(
            status.refusals,
            vec![
                "mqtt client \"ha\" would lose its last reference: this change needs a restart"
                    .to_string(),
            ],
        );
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
                mqtt: true,
                ..BootFixture::default()
            },
        )
        .await;
        let (service, router) = booted.mqtt.clone().expect("the fixture stood one up");
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

        // And back: the binding goes, and with it the filter and the route. The
        // client keeps its other reference, so rule 6 has nothing to say.
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
                mqtt: true,
                ..BootFixture::default()
            },
        )
        .await;
        let (service, router) = booted.mqtt.clone().expect("the fixture stood one up");

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

        let ready = match booted.driver.prepare(TriggerSource::Signal) {
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

    /// A candidate that configures no messaging at all. The planner answers
    /// `None` there, and a process running the reload facility has messaging by
    /// construction — so this is a document for some other process.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_candidate_that_configures_no_messaging_is_refused() {
        let tree = Tree::holding(&document(""));
        let mut booted = boot(&tree, Vec::new()).await;

        tree.write("// a document that configures nothing at all\n");
        assert!(
            booted
                .driver
                .prepare_and_report(TriggerSource::Signal)
                .await
                .is_none()
        );

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Refused);
        assert_eq!(status.refusals.len(), 1, "{:?}", status.refusals);
        assert!(
            status.refusals[0].contains("configures no messaging")
                && status.refusals[0].ends_with(super::super::NEEDS_RESTART),
            "{:?}",
            status.refusals
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
        /// read past.
        pub(crate) fn stop_mqtt(&self) {
            for stop in &self.mqtt_stop_txs {
                // A supervisor that already exited is a closed channel, which
                // is the state this asks for.
                let _ = stop.send(true);
            }
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
        // what its startup sweep drains — which is how the test sees that the
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
        let tree = Tree::holding(&document(""));
        let mut booted = boot_with(
            &tree,
            BootFixture {
                components_roots: vec![components.path().to_path_buf()],
                reader_subscriptions: vec!["brenn:work"],
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
        tree.write(&document_with_a_configured_consumer("v1"));
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
        tree.write(&document(""));
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
        let measured = match booted.driver.prepare(TriggerSource::Signal) {
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
    /// refused, and nothing moves.
    ///
    /// Rule 1's other half.  A retune must not re-create a subscriber
    /// entry that belongs to a live conversation.  The rule is
    /// unit-tested; this case covers the operator-facing verdict for the
    /// likeliest shape: retuning a channel an agent reads.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_retune_of_a_channel_an_agent_subscribes_to_is_refused() {
        let tree = Tree::holding(&document(""));
        let mut booted = boot_with(
            &tree,
            BootFixture {
                reader_subscriptions: vec!["brenn:work"],
                ..BootFixture::default()
            },
        )
        .await;
        let booted_sha = booted.driver.baseline().document.document_sha256.clone();
        let before = subscriber_lines(&booted.messenger, "brenn:work");
        assert_eq!(
            subscribed_anywhere(
                &booted.messenger,
                &SubscriberEntryKind::App(READER.to_string())
            ),
            ["brenn:work"],
            "the fixture seats the declared subscriber this case is about",
        );

        tree.write(&document("").replace(
            "    standing_retain_depth = 64;",
            "    standing_retain_depth = 32;",
        ));
        booted.driver.reload(TriggerSource::Signal).await;

        let status = booted.last_status().await;
        assert_eq!(status.outcome, Outcome::Refused);
        assert_eq!(status.refusals.len(), 1, "{:?}", status.refusals);
        assert!(
            status.refusals[0].contains("brenn:work")
                && status.refusals[0].contains(READER)
                && status.refusals[0].ends_with(super::super::NEEDS_RESTART),
            "{:?}",
            status.refusals
        );
        // Refused means untouched: the process still projects what it booted,
        // and the entry the retune would have re-created is where it was.
        assert_eq!(
            booted.driver.baseline().document.document_sha256,
            booted_sha
        );
        assert_eq!(subscriber_lines(&booted.messenger, "brenn:work"), before);
        assert_eq!(
            booted
                .messenger
                .directory()
                .resolve("brenn:work")
                .expect("the work channel is still declared")
                .resolved_channel
                .standing_retain_depth,
            brenn_lib::messaging::config::Depth::Bounded(64),
            "the live entry keeps the tuning it booted with",
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
        let ready = match booted.driver.prepare(TriggerSource::Signal) {
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
    /// The kind's schema document.
    const PANEL_SCHEMA: &str = "brenn:surface.kind.panel.schema";

    /// Every address a surface case reads back.
    pub(crate) const DESKBAR_READS: [&str; 5] = [
        DESKBAR_BINDINGS,
        SURFACE_INDEX,
        DESKBAR_HELP,
        PANEL_HELP,
        PANEL_SCHEMA,
    ];

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
                reader_reads: DESKBAR_READS.to_vec(),
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
                reader_reads: [
                    DESKBAR_READS.as_slice(),
                    ["brenn:surface.surface.sidebar.help"].as_slice(),
                ]
                .concat(),
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
                reader_reads: DESKBAR_READS.to_vec(),
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
}
