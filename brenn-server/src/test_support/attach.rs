//! Fixtures shared by the attachment-session suites: one configurable
//! [`AttachProfile`] stub and one [`AttachSessionCtx`] builder.
//!
//! The stub is what makes those suites transport tests — nothing it answers
//! names a component, a port, or an instance, only an attribution string and a
//! channel address. One copy is what keeps a new trait member from being stubbed
//! once per plane: a suite states the authority it means to exercise and takes
//! the defaults for the rest.

use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;

use brenn_attach_proto::ServerFrame;
use brenn_lib::access::AppPolicy;
use brenn_lib::db::Db;
use brenn_lib::messaging::config::{
    ChannelConfigRaw, Depth, MessagingGlobalConfig, build_channel_entries,
};
use brenn_lib::messaging::query::NoopWakeRouter;
use brenn_lib::messaging::{
    MessagingDirectory, Messenger, ParticipantId, SubscriberEntry, WakeRouter,
};
use brenn_lib::obs::alerting::AlertDispatcher;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::routes::attach::profile::{
    AttachProfile, DeferredTarget, PublishPosture, PublishRate, SubscriptionFacts,
};
use crate::routes::attach::registry::{AttachRegistry, SessionCaps};
use crate::routes::attach::session::AttachSessionCtx;

/// The attacher every attach fixture stands up.
pub(crate) const TEST_ATTACHER: &str = "deskbar";

/// The authenticated account every attach fixture attaches under.
pub(crate) const TEST_ACCOUNT: &str = "dev";

/// The body cap every attach fixture uses unless it is aiming at the cap.
pub(crate) const TEST_BODY_BYTES: usize = 64 * 1024;

/// Outbound queue depth for a fixture context — deep enough that no test blocks
/// on the writer it does not have.
const OUTBOUND_QUEUE: usize = 64;

/// The stub route seam: every authority question a plane asks is a field, so a
/// test writes the answers it means to exercise and nothing else.
///
/// Start from [`TestProfile::new`] and override with struct-update syntax.
pub(crate) struct TestProfile {
    /// The bare principal, minted from `slug`.
    pub attacher: ParticipantId,
    /// The send-budget scope half — the attacher's slug.
    pub slug: String,
    /// The channels this attacher may subscribe, at the fold boot resolved.
    pub subscribable: HashMap<String, SubscriptionFacts>,
    /// Attribution (`None` = the attacher itself) → the channels it may publish.
    pub publishable: HashMap<Option<String>, HashSet<String>>,
    /// The sub-identities this attacher declares. Anything else is undeclared and
    /// mints nothing.
    pub declared: HashSet<String>,
    /// The one channel, if any, carrying the diagnostics posture.
    pub diagnostic: Option<String>,
    pub deferred_targets: Vec<DeferredTarget>,
    pub subscribe_burst: u32,
    pub publish_rate: PublishRate,
    pub alert_granted: bool,
    pub session_caps: SessionCaps,
    /// Concurrent subscriptions one attachment of this attacher may hold.
    pub max_active_subscriptions: usize,
    /// The directory entry this stub mints on a successful subscribe, if any.
    /// `None` is the boot-declared answer a surface gives; a suite exercising
    /// the runtime-entry hook states the entry it means to see minted, whose
    /// depths the plane then clamps against the channel.
    pub runtime_entry: Option<SubscriberEntry>,
}

impl TestProfile {
    /// An attacher that declares no sub-identity, subscribes and publishes
    /// nothing, holds no alert grant, and is uncapped — every authority answer is
    /// the deny-by-default one, and a suite grants exactly what it exercises.
    pub(crate) fn new() -> Self {
        Self {
            attacher: ParticipantId::for_surface(TEST_ATTACHER),
            slug: TEST_ATTACHER.to_string(),
            subscribable: HashMap::new(),
            publishable: HashMap::new(),
            declared: HashSet::new(),
            diagnostic: None,
            deferred_targets: Vec::new(),
            subscribe_burst: 4,
            publish_rate: PublishRate {
                burst: 4,
                per_sec: 1,
            },
            alert_granted: false,
            session_caps: SessionCaps::UNCAPPED,
            max_active_subscriptions: usize::MAX,
            runtime_entry: None,
        }
    }
}

impl AttachProfile for TestProfile {
    fn attacher(&self) -> &ParticipantId {
        &self.attacher
    }

    fn subscribable(&self, channel: &str) -> Option<SubscriptionFacts> {
        self.subscribable.get(channel).copied()
    }

    fn publishable(&self, attribution: Option<&str>, channel: &str) -> bool {
        self.publishable
            .get(&attribution.map(str::to_string))
            .is_some_and(|channels| channels.contains(channel))
    }

    fn admit_attribution(&self, attribution: Option<&str>) -> Option<ParticipantId> {
        match attribution {
            None => Some(self.attacher.clone()),
            Some(name) if self.declared.contains(name) => {
                Some(ParticipantId::for_surface_component(&self.slug, name))
            }
            Some(_) => None,
        }
    }

    fn publish_posture(&self, channel: &str) -> PublishPosture {
        match &self.diagnostic {
            Some(diagnostic) if diagnostic == channel => PublishPosture::Diagnostic,
            _ => PublishPosture::Invariant,
        }
    }

    fn send_budget_scope(&self) -> &str {
        &self.slug
    }

    fn deferred_view_targets(&self) -> &[DeferredTarget] {
        &self.deferred_targets
    }

    fn subscribe_burst(&self) -> u32 {
        self.subscribe_burst
    }

    fn publish_rate(&self) -> PublishRate {
        self.publish_rate
    }

    fn alert_granted(&self) -> bool {
        self.alert_granted
    }

    fn session_caps(&self) -> SessionCaps {
        self.session_caps
    }

    fn max_active_subscriptions(&self) -> usize {
        self.max_active_subscriptions
    }

    fn runtime_entry(&self, _channel: &str) -> Option<SubscriberEntry> {
        self.runtime_entry.clone()
    }
}

/// A `Messenger` over one durable in-memory channel, and the uuid rows are
/// seeded against.
///
/// The transport suites need a real store to read positions and retained windows
/// out of; the channel's own sizing is never their subject, so it is as wide as
/// it can be and every clamp under test comes from the profile instead.
pub(crate) async fn one_channel_messenger(db: &Db, bare: &str) -> (Arc<Messenger>, Uuid) {
    one_channel_messenger_at_standing(db, bare, Depth::Unbounded).await
}

/// [`one_channel_messenger`] with the channel's standing retention stated.
///
/// The standing depth is the ceiling on every depth any subscriber may hold on
/// the channel, so a suite whose subject is that clamp names a bounded one here
/// and leaves the rest of the fixture alone.
pub(crate) async fn one_channel_messenger_at_standing(
    db: &Db,
    bare: &str,
    standing: Depth,
) -> (Arc<Messenger>, Uuid) {
    let raw = ChannelConfigRaw {
        send_rate: None,
        uuid: Some(Uuid::new_v4().to_string()),
        address: Some(bare.to_string()),
        address_prefix: None,
        description: None,
        push_depth: Some(standing),
        retain_depth: Some(standing),
        standing_retain_depth: Some(standing),
        noise: None,
        sink: None,
        wake_min: None,
    };
    let entry = build_channel_entries(&[raw], &MessagingGlobalConfig::default())
        .pop()
        .expect("one channel entry");
    let channel_uuid = entry.uuid;
    {
        let conn = db.lock().await;
        brenn_lib::messaging::db::upsert_channels(&conn, std::slice::from_ref(&entry));
    }
    let messenger = Messenger::new(
        db.clone(),
        Arc::new(MessagingDirectory::with_entries(vec![entry])),
        Arc::from("test-origin"),
        Arc::new(indexmap::IndexMap::new()),
        Arc::new(NoopWakeRouter) as Arc<dyn WakeRouter>,
        MessagingGlobalConfig::default(),
    );
    (messenger, channel_uuid)
}

/// [`one_channel_messenger`]'s ephemeral twin: one declared `ephemeral:`
/// channel, its retention a ring store rather than the database.
///
/// The retained window is `retain_depth`, so a suite that means to exercise the
/// clamp sizes it here; the durable twin's retention is the database's and is
/// bounded by the subscription's own depths instead.
pub(crate) fn one_ephemeral_channel_messenger(
    db: &Db,
    bare: &str,
    retain_depth: u64,
) -> (Arc<Messenger>, Uuid) {
    let entry = brenn_lib::messaging::testutils::ephemeral_channel_entry(bare, retain_depth);
    let channel_uuid = entry.uuid;
    let stores = Arc::new(brenn_lib::messaging::store::RingStores::build(
        std::slice::from_ref(&entry),
    ));
    let messenger = Messenger::new(
        db.clone(),
        Arc::new(MessagingDirectory::with_entries(vec![entry])),
        Arc::from("test-origin"),
        Arc::new(indexmap::IndexMap::new()),
        Arc::new(NoopWakeRouter) as Arc<dyn WakeRouter>,
        MessagingGlobalConfig::default(),
    )
    .with_ring_stores(stores);
    (messenger, channel_uuid)
}

/// Builds one attachment's context over a stub profile.
///
/// Defaults are the shape every suite shares — a directory-less messenger, an
/// empty policy, a no-op alert dispatcher, an empty registry, the nil session id
/// — so a suite names only the pieces its subject reads.
pub(crate) struct AttachCtxBuilder {
    profile: Arc<dyn AttachProfile>,
    messenger: Option<Arc<Messenger>>,
    policy: AppPolicy,
    alert_dispatcher: Option<AlertDispatcher>,
    max_body_bytes: usize,
    session_id: Uuid,
}

impl AttachCtxBuilder {
    pub(crate) fn new(profile: TestProfile) -> Self {
        Self {
            profile: Arc::new(profile),
            messenger: None,
            policy: AppPolicy::default(),
            alert_dispatcher: None,
            max_body_bytes: TEST_BODY_BYTES,
            session_id: Uuid::nil(),
        }
    }

    /// A real `Messenger`, for the planes that read the store or publish.
    pub(crate) fn messenger(mut self, messenger: Arc<Messenger>) -> Self {
        self.messenger = Some(messenger);
        self
    }

    /// The attacher's resolved policy — the delivery floor and the publish ACLs.
    pub(crate) fn policy(mut self, policy: AppPolicy) -> Self {
        self.policy = policy;
        self
    }

    /// A dispatcher whose alerts the test reads back.
    pub(crate) fn alert_dispatcher(mut self, dispatcher: AlertDispatcher) -> Self {
        self.alert_dispatcher = Some(dispatcher);
        self
    }

    pub(crate) fn max_body_bytes(mut self, max: usize) -> Self {
        self.max_body_bytes = max;
        self
    }

    pub(crate) fn session_id(mut self, session_id: Uuid) -> Self {
        self.session_id = session_id;
        self
    }

    /// The context and the receiver its outbound frames land in.
    pub(crate) fn build(self) -> (AttachSessionCtx, mpsc::Receiver<ServerFrame>) {
        let (tx, rx) = mpsc::channel::<ServerFrame>(OUTBOUND_QUEUE);
        let ctx = AttachSessionCtx {
            profile: self.profile,
            messenger: self
                .messenger
                .unwrap_or_else(super::surface::shape_only_messenger),
            policy: Arc::new(self.policy),
            alert_dispatcher: self
                .alert_dispatcher
                .unwrap_or_else(|| AlertDispatcher::noop().0),
            registry: AttachRegistry::default(),
            max_body_bytes: self.max_body_bytes,
            session_id: self.session_id,
            account: TEST_ACCOUNT.to_string(),
            ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
            tx,
        };
        (ctx, rx)
    }
}
