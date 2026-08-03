//! The rig both remote-route suites boot against: `ws_tests` for the HTTP edge,
//! `conformance_tests` for a whole attacher driven through it.
//!
//! Booted-shaped rather than hand-built: the `[[remote]]` block is real TOML
//! resolved off a real 0600 token file, lowered through the real runtime builder,
//! over a messenger carrying the registrations and delivery bindings boot would
//! install. What a test then asserts is a property of the wiring, not of a
//! fixture that agreed with it.

use std::sync::{Arc, Mutex};

use brenn_envelope::chat::chat_roster_bare_name;
use brenn_lib::db;
use brenn_lib::messaging::chat_roster::CHAT_ROSTER_COMPONENT;
use brenn_lib::messaging::config::{AttachSendBudget, MessagingGlobalConfig};
use brenn_lib::messaging::remote::{RemoteConfigRaw, resolve_remotes};
use brenn_lib::messaging::store::RingStores;
use brenn_lib::messaging::system::{SystemParticipantSpec, registrations_from_specs};
use brenn_lib::messaging::{
    AttachScope, ChannelEntry, MessagingDirectory, Messenger, SubscriberEntryKind,
    SubscriberRegistration, WakeEconomics, WakeRouter, attach_principal_budgets,
};
use brenn_lib::obs::alerting::AlertDispatcher;

use crate::state::AppState;
use crate::test_support::state::test_state_with_capturing_alerter;

/// The slug every fixture here configures.
pub(crate) const SLUG: &str = "pod-kitchen";

/// The app whose chat tree the fixture's `[[remote]]` blocks grant. Registered
/// with the messenger so the server-side roster publish — which answers `None`
/// for an app it does not know — is reachable from a test.
pub(crate) const APP: &str = "home";

/// The app's single allowed user. A singleton app resolves its conversation
/// through this name, so a test that mints one through the messenger creates the
/// matching user row.
pub(crate) const OWNER: &str = "operator";

/// The token the fixture writes into its 0600 file, after trimming.
pub(crate) const TOKEN: &str = "s3cret-token";

/// Body cap the fixture messenger runs under, small enough that the derived
/// frame cap is a distinctive number in the `Welcome`.
pub(crate) const TEST_MAX_BODY_BYTES: usize = 4096;

/// A fleet-driver `[[remote]]` block: a roster read at 1/1, outbound
/// conversation leaves under prefixes, publish rights on the inbound ones.
pub(crate) const FLEET: &str = r#"
slug = "pod-kitchen"
token_file = "TOKEN_FILE"
grants = ["subscribe", "publish", "ephemeral_subscribe", "ephemeral_publish", "alert"]
subscribe_acl = [
  { exact  = "chat.app.home.roster", push_depth = 1, retain_depth = 1 },
  { prefix = "chat.app.home.out.",   push_depth = 8, retain_depth = 64 },
]
ephemeral_subscribe_acl = [
  { prefix = "chat.app.home.stream.", push_depth = 32, retain_depth = 32 },
]
publish_acl           = [ { prefix = "chat.app.home.in." } ]
ephemeral_publish_acl = [ { prefix = "chat.app.home.wake." } ]
"#;

/// Everything one rig needs to assert against, past the `AppState` the server
/// runs on.
pub(crate) struct RemoteTestHarness {
    pub state: AppState,
    pub alerts: Arc<Mutex<Vec<(String, String)>>>,
    /// The live directory the messenger resolves through, so a test can
    /// provision or deprovision a channel under a running attachment and read
    /// back the subscriber entries a subscribe minted.
    pub directory: Arc<MessagingDirectory>,
    flusher: AlertDispatcher,
    /// Held open for the test's lifetime: dropping it unlinks the token file,
    /// and a later resolution in the same test would then fail to read it.
    _token_file: tempfile::NamedTempFile,
}

impl RemoteTestHarness {
    /// Drain the captured alerts, flushing the dispatcher first so the drainer
    /// task has run. Call after a happens-before edge — an observed response, an
    /// observed detach — or the answer is a race.
    pub async fn captured(&self) -> Vec<(String, String)> {
        self.flusher.flush().await;
        self.alerts.lock().unwrap().clone()
    }
}

/// Write a 0600 token file, so resolution exercises the real mode-checked load.
fn write_token() -> tempfile::NamedTempFile {
    use std::io::Write as _;
    let mut f = tempfile::NamedTempFile::new().unwrap();
    writeln!(f, "{TOKEN}").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(f.path(), std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    f
}

/// Build the rig around one `[[remote]]` body, with no channels provisioned.
pub(crate) async fn remote_harness(db: &db::Db, body: &str) -> RemoteTestHarness {
    remote_harness_with_channels(db, body, vec![]).await
}

/// [`remote_harness`] over a directory carrying `entries`.
///
/// Channels are declared the way boot declares them — a durable channel gets its
/// database row, a non-durable one its process ring — because a remote's
/// subscribe resolves the directory and then reads the store behind it, and a
/// channel present in one half and absent from the other is a panic rather than
/// a test failure.
pub(crate) async fn remote_harness_with_channels(
    db: &db::Db,
    body: &str,
    entries: Vec<ChannelEntry>,
) -> RemoteTestHarness {
    let token_file = write_token();
    let toml = body.replace("TOKEN_FILE", &token_file.path().display().to_string());
    let raw: RemoteConfigRaw = toml::from_str(&toml).expect("[[remote]] block must parse");
    let resolved = resolve_remotes(&[raw], &MessagingGlobalConfig::default());

    let (nondurable, durable): (Vec<ChannelEntry>, Vec<ChannelEntry>) = entries
        .iter()
        .cloned()
        .partition(|entry| !entry.capabilities().durable);
    {
        let conn = db.lock().await;
        brenn_lib::messaging::db::upsert_channels(&conn, &durable);
    }

    let router = Arc::new(crate::messaging_router::WakeRouterImpl::new(
        crate::active_bridge::ActiveBridges::new(),
    ));
    for remote in &resolved {
        router.register_remote_delivery_routes(remote);
    }
    let directory = Arc::new(MessagingDirectory::with_entries(entries));
    let mut apps = indexmap::IndexMap::new();
    // Singleton with one allowed user, the shape config validation enforces: it
    // is what lets the messenger resolve — and mint — the app's conversation.
    let mut app = crate::test_support::app_config::default_test_app_config(APP, APP);
    app.singleton = true;
    app.allowed_users = vec![OWNER.to_string()];
    apps.insert(APP.to_string(), app);
    let messenger = Messenger::new(
        db.clone(),
        Arc::clone(&directory),
        Arc::from("test"),
        Arc::new(apps),
        Arc::clone(&router) as Arc<dyn WakeRouter>,
        MessagingGlobalConfig {
            max_body_bytes: TEST_MAX_BODY_BYTES,
            ..Default::default()
        },
    )
    .with_subscriber_registrations(
        resolved
            .iter()
            .map(|remote| {
                (
                    SubscriberEntryKind::Remote(remote.slug.clone()),
                    SubscriberRegistration {
                        policy: Arc::new(remote.policy.clone()),
                        wake: WakeEconomics::Eager,
                    },
                )
            })
            .collect(),
    )
    // The roster writer, so a test can publish the app's snapshot the way the
    // server does: `system:chat-roster` holds the only authority on the address.
    .with_subscriber_registrations(registrations_from_specs(&[
        SystemParticipantSpec::publish_only(
            CHAT_ROSTER_COMPONENT,
            brenn_lib::messaging::ChannelScheme::Brenn,
            &[chat_roster_bare_name(
                &brenn_lib::config::LlmChatConfig::default().prefix,
                APP,
            )],
        ),
    ]))
    .with_attach_send_budgets(resolved.iter().flat_map(|remote| {
        attach_principal_budgets(
            AttachScope::remote(&remote.slug),
            vec![(None, AttachSendBudget::default())],
        )
    }))
    .with_ring_stores(Arc::new(RingStores::build(&nondurable)));

    let (mut state, alerts, _drainer) = test_state_with_capturing_alerter(db);
    let flusher = state.alert_dispatcher.clone();
    state.messenger = Some(Arc::clone(&messenger));
    state.remotes = Arc::new(super::build_remote_runtimes(
        &resolved,
        Some(&messenger),
        TEST_MAX_BODY_BYTES,
    ));
    router.set_state(state.clone());
    RemoteTestHarness {
        state,
        alerts,
        directory,
        flusher,
        _token_file: token_file,
    }
}
