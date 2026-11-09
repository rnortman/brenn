//! `api_key_provider` changes against a running process: every key file is
//! re-read at reload, the resolved table is compared against the one
//! `api-request` reads, and commit installs the candidate's table whole. The
//! first and the last provider are the exceptions, refused because the tool
//! is registered only while one exists.

use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;

use brenn_lib::config::{AccountLookup, ApiKeys, PACKAGED};
use brenn_messaging::config_reload::{Outcome, ReloadStatus};

use super::driver::TriggerSource;
use super::driver::tests::{
    BootFixture, Booted, Tree, boot_with, document, install_package, seat_user, staged_module,
};

/// The user every case registers before boot.
const ALICE: &str = "alice";

/// Write `text` as the key file `name` under the tree's secrets, at 0600.
fn key(tree: &Tree, name: &str, text: &str) -> PathBuf {
    let path = tree.secret(name, text);
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
        .expect("the key file is private");
    path
}

/// One `account` block: name, user, key file, default.
struct Account<'a>(&'a str, &'a str, &'a PathBuf, bool);

/// A per-user provider block named `name` holding `accounts`.
fn per_user(name: &str, accounts: &[Account<'_>]) -> String {
    let blocks: String = accounts
        .iter()
        .map(|Account(account, user, path, default)| {
            format!(
                "    account {account} {{\n        user = \"{user}\";\n        key_file = \
                 \"{}\";\n        default = {default};\n    }}\n",
                path.display()
            )
        })
        .collect();
    format!(
        "\napi_key_provider {name} {{\n    header = \"Authorization\";\n    api_hosts = \
         [\"https://api.example.com\"];\n{blocks}}}\n"
    )
}

/// A deployment-key provider block named `name`.
fn deployment(name: &str, path: &std::path::Path) -> String {
    format!(
        "\napi_key_provider {name} {{\n    key_file = \"{}\";\n    header = \"X-Api-Key\";\n    \
         api_hosts = [\"https://api.example.com\"];\n}}\n",
        path.display()
    )
}

/// A containerised agent, its container's home at `home`, holding
/// `api-request` under one clause naming `account` of alice's on `mail`.
fn mailer(home: &std::path::Path, account: &str) -> String {
    format!(
        r#"
container sandbox {{
    image = "example.com/cc:latest";
    home_dir = "{home}";
}}

agent Mailer() {{
    working_dir = ".";
    container = "sandbox";
    container_working_dir = "/work";
    tool api-request {{
        allow {{ provider = "mail"; user = "{ALICE}"; account = "{account}"; }}
    }}
}}

new mailer: Mailer();
"#,
        home = home.display(),
    )
}

/// Boot `tree` over a database in which `users` are registered.
async fn boot_registered(tree: &Tree, users: &[&str]) -> Booted {
    let db = brenn_server::test_support::init_db_memory();
    {
        let conn = db.lock().await;
        for user in users {
            brenn_db::auth::user::create_user(&conn, user, "$argon2id$fake");
        }
    }
    boot_with(
        tree,
        BootFixture {
            db: Some(db),
            ..BootFixture::default()
        },
    )
    .await
}

/// Reload, and the outcome it published.
async fn reload(booted: &mut Booted) -> ReloadStatus {
    booted.driver.reload(TriggerSource::Signal).await;
    booted.last_status().await
}

fn assert_applied(status: &ReloadStatus) {
    assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
}

/// The key the running table sends for `user` on `provider` when the call
/// names `account`, or `None` when the table resolves none.
fn sent(booted: &Booted, provider: &str, user: &str, account: Option<&str>) -> Option<String> {
    let table = booted.api_keys.load();
    match &table.get(provider)?.keys {
        ApiKeys::Deployment(key) => Some(key.expose().to_string()),
        ApiKeys::Accounts(accounts) => match accounts.lookup(user, account) {
            AccountLookup::Found { key, .. } => Some(key.expose().to_string()),
            _ => None,
        },
    }
}

/// Every account edit converges in one reload and names the provider as
/// changed: an account added, re-pathed to a file with another key, its
/// `default` moved, and removed.
#[tokio::test(flavor = "multi_thread")]
async fn account_edits_converge() {
    let tree = Tree::new();
    let home = key(&tree, "alice-home", "k3y-home");
    let work = key(&tree, "alice-work", "k3y-work");
    let other = key(&tree, "alice-other", "k3y-other");
    let doc = |accounts: &[Account<'_>]| document(&per_user("mail", accounts));
    tree.write(&doc(&[Account("home", ALICE, &home, false)]));
    let mut booted = boot_registered(&tree, &[ALICE]).await;
    assert_eq!(
        sent(&booted, "mail", ALICE, None).as_deref(),
        Some("k3y-home")
    );

    let steps: [(&[Account<'_>], Option<&str>, Option<&str>); 4] = [
        // Added: alice now has two, and `home` stays the default.
        (
            &[
                Account("home", ALICE, &home, true),
                Account("work", ALICE, &work, false),
            ],
            Some("work"),
            Some("k3y-work"),
        ),
        // Re-pathed to a file holding another key.
        (
            &[
                Account("home", ALICE, &home, true),
                Account("work", ALICE, &other, false),
            ],
            Some("work"),
            Some("k3y-other"),
        ),
        // The default moved: an omitted account now resolves to `work`.
        (
            &[
                Account("home", ALICE, &home, false),
                Account("work", ALICE, &other, true),
            ],
            None,
            Some("k3y-other"),
        ),
        // Removed.
        (&[Account("home", ALICE, &home, false)], Some("work"), None),
    ];
    for (accounts, account, expected) in steps {
        tree.write(&doc(accounts));
        let status = reload(&mut booted).await;
        assert_applied(&status);
        assert_eq!(status.delta.api_key_providers_changed, vec!["mail"]);
        assert!(status.delta.api_key_providers_added.is_empty());
        assert!(status.delta.api_key_providers_removed.is_empty());
        assert_eq!(sent(&booted, "mail", ALICE, account).as_deref(), expected);
    }
}

/// A key file rotated in place, with no document edit at all, is applied and
/// names the provider: the comparison is over the bytes, never the text.
#[tokio::test(flavor = "multi_thread")]
async fn a_key_rotated_in_place_is_applied_without_a_document_edit() {
    let tree = Tree::new();
    let path = key(&tree, "alice", "k3y-one");
    tree.write(&document(&per_user(
        "mail",
        &[Account("alice", ALICE, &path, false)],
    )));
    let mut booted = boot_registered(&tree, &[ALICE]).await;

    key(&tree, "alice", "k3y-two");
    let status = reload(&mut booted).await;
    assert_applied(&status);
    assert_eq!(status.delta.api_key_providers_changed, vec!["mail"]);
    assert!(status.delta.channels_changed.is_empty());
    assert_eq!(
        sent(&booted, "mail", ALICE, None).as_deref(),
        Some("k3y-two")
    );

    // Nothing moved since: the same bytes are no change.
    let status = reload(&mut booted).await;
    assert_eq!(status.outcome, Outcome::Unchanged, "{:?}", status.refusals);
}

/// A provider added beside another, and one removed beside another, each
/// converge and are named.
#[tokio::test(flavor = "multi_thread")]
async fn a_provider_beside_another_is_added_and_removed() {
    let tree = Tree::new();
    let maps = key(&tree, "maps", "k3y-maps");
    let alice = key(&tree, "alice", "k3y-alice");
    let mail = per_user("mail", &[Account("alice", ALICE, &alice, false)]);
    tree.write(&document(&deployment("maps", &maps)));
    let mut booted = boot_registered(&tree, &[ALICE]).await;

    tree.write(&document(&format!("{}{mail}", deployment("maps", &maps))));
    let status = reload(&mut booted).await;
    assert_applied(&status);
    assert_eq!(status.delta.api_key_providers_added, vec!["mail"]);
    assert_eq!(
        sent(&booted, "mail", ALICE, None).as_deref(),
        Some("k3y-alice")
    );

    tree.write(&document(&mail));
    let status = reload(&mut booted).await;
    assert_applied(&status);
    assert_eq!(status.delta.api_key_providers_removed, vec!["maps"]);
    assert_eq!(sent(&booted, "maps", ALICE, None), None);
    assert_eq!(
        sent(&booted, "mail", ALICE, None).as_deref(),
        Some("k3y-alice")
    );
}

/// With no provider of either kind at boot, the first is refused in the
/// restart grammar and the table stays empty.
#[tokio::test(flavor = "multi_thread")]
async fn the_first_provider_is_refused() {
    let tree = Tree::new();
    let maps = key(&tree, "maps", "k3y-maps");
    tree.write(&document(""));
    let mut booted = boot_registered(&tree, &[]).await;
    tree.write(&document(&deployment("maps", &maps)));
    let status = reload(&mut booted).await;
    assert_eq!(status.outcome, Outcome::Refused);
    assert_eq!(
        status.refusals,
        vec![
            "api_key_providers[maps] added: the first provider needs a restart: this change \
             needs a restart"
        ],
    );
    assert!(booted.api_keys.load().is_empty());
}

/// With one provider at boot and no `oauth_provider`, removing it is refused
/// in the restart grammar and the table keeps its key.
#[tokio::test(flavor = "multi_thread")]
async fn the_last_provider_is_refused() {
    let tree = Tree::new();
    let maps = key(&tree, "maps", "k3y-maps");
    tree.write(&document(&deployment("maps", &maps)));
    let mut booted = boot_registered(&tree, &[]).await;
    tree.write(&document(""));
    let status = reload(&mut booted).await;
    assert_eq!(status.outcome, Outcome::Refused);
    assert_eq!(
        status.refusals,
        vec![
            "api_key_providers[maps] removed: the last provider needs a restart: this change \
             needs a restart"
        ],
    );
    assert_eq!(
        sent(&booted, "maps", ALICE, None).as_deref(),
        Some("k3y-maps")
    );
}

/// An account naming a user who is not registered is refused in boot's
/// words, naming provider, account and user; once the user registers, the
/// same document applies with no restart.
#[tokio::test(flavor = "multi_thread")]
async fn an_account_for_an_unregistered_user_applies_once_the_user_registers() {
    let tree = Tree::new();
    let alice = key(&tree, "alice", "k3y-alice");
    let bob = key(&tree, "bob", "k3y-bob");
    tree.write(&document(&per_user(
        "mail",
        &[Account("alice", ALICE, &alice, false)],
    )));
    let mut booted = boot_registered(&tree, &[ALICE]).await;

    tree.write(&document(&per_user(
        "mail",
        &[
            Account("alice", ALICE, &alice, false),
            Account("bob", "bob", &bob, false),
        ],
    )));
    let status = reload(&mut booted).await;
    assert_eq!(status.outcome, Outcome::Refused);
    assert_eq!(status.refusals.len(), 1, "{:?}", status.refusals);
    let line = &status.refusals[0];
    for part in [
        "`mail`",
        "`bob`",
        "\"bob\"",
        "not registered",
        "register the user first",
    ] {
        assert!(line.contains(part), "{part} in {line}");
    }
    assert!(!line.contains("this change needs a restart"), "{line}");
    assert_eq!(sent(&booted, "mail", "bob", None), None);

    seat_user(&booted, "bob").await;
    let status = reload(&mut booted).await;
    assert_applied(&status);
    assert_eq!(
        sent(&booted, "mail", "bob", None).as_deref(),
        Some("k3y-bob")
    );
}

/// A clause naming an account the candidate does not configure is refused
/// with the registry's wording, and the table is untouched.
#[tokio::test(flavor = "multi_thread")]
async fn a_clause_naming_an_account_the_candidate_lacks_is_refused() {
    let tree = Tree::new();
    let home_dir = tempfile::tempdir().expect("a container home");
    let home = key(&tree, "alice-home", "k3y-home");
    let work = key(&tree, "alice-work", "k3y-work");
    let both = per_user(
        "mail",
        &[
            Account("home", ALICE, &home, true),
            Account("work", ALICE, &work, false),
        ],
    );
    tree.write(&document(&format!(
        "{both}{}",
        mailer(home_dir.path(), "work")
    )));
    let mut booted = boot_registered(&tree, &[ALICE]).await;

    let home_only = per_user("mail", &[Account("home", ALICE, &home, false)]);
    tree.write(&document(&format!(
        "{home_only}{}",
        mailer(home_dir.path(), "work")
    )));
    let status = reload(&mut booted).await;
    assert_eq!(status.outcome, Outcome::Refused);
    assert!(
        status.refusals.iter().any(|line| {
            line.contains("tool_grant for \"api-request\" has an invalid ACL clause")
                && line.contains("work")
        }),
        "{:?}",
        status.refusals,
    );
    assert_eq!(
        sent(&booted, "mail", ALICE, Some("work")).as_deref(),
        Some("k3y-work")
    );
}

/// A wasm consumer granted `api-request` under one clause naming `account` of
/// alice's on `mail`.
fn consumer(account: &str) -> String {
    format!(
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
    tool api-request {{
        allow {{ provider = "mail"; user = "{ALICE}"; account = "{account}"; }}
    }}
}}
"#
    )
}

/// A consumer's clause naming an account the candidate does not configure is
/// refused by the planner in the registry's wording, and the table is
/// untouched.
#[tokio::test(flavor = "multi_thread")]
async fn a_consumer_clause_naming_an_account_the_candidate_lacks_is_refused() {
    let components = tempfile::tempdir().expect("a components root");
    let tree = Tree::new();
    let home = key(&tree, "alice-home", "k3y-home");
    let work = key(&tree, "alice-work", "k3y-work");
    let both = per_user(
        "mail",
        &[
            Account("home", ALICE, &home, true),
            Account("work", ALICE, &work, false),
        ],
    );
    tree.write(&document(&format!("{both}{}", consumer("work"))));
    install_package(components.path(), &staged_module(&tree));
    let db = brenn_server::test_support::init_db_memory();
    {
        let conn = db.lock().await;
        brenn_db::auth::user::create_user(&conn, ALICE, "$argon2id$fake");
    }
    let mut booted = boot_with(
        &tree,
        BootFixture {
            db: Some(db),
            components_roots: vec![components.path().to_path_buf()],
            ..BootFixture::default()
        },
    )
    .await;
    assert_eq!(
        sent(&booted, "mail", ALICE, Some("work")).as_deref(),
        Some("k3y-work")
    );

    let home_only = per_user("mail", &[Account("home", ALICE, &home, false)]);
    tree.write(&document(&format!("{home_only}{}", consumer("work"))));
    install_package(components.path(), &staged_module(&tree));
    let status = reload(&mut booted).await;
    assert_eq!(status.outcome, Outcome::Refused);
    assert!(
        status.refusals.iter().any(|line| {
            line.contains("wasm consumer \"sifter\"")
                && line.contains("tool_grant for \"api-request\" has an invalid ACL clause")
                && line.contains("work")
        }),
        "{:?}",
        status.refusals,
    );
    assert_eq!(
        sent(&booted, "mail", ALICE, Some("work")).as_deref(),
        Some("k3y-work")
    );
}

/// A missing and a world-readable key file each refuse the reload in the
/// environment grammar, naming the file; the running table keeps its key.
#[tokio::test(flavor = "multi_thread")]
async fn an_unreadable_key_file_refuses_and_leaves_the_table() {
    let tree = Tree::new();
    let path = key(&tree, "alice", "k3y-alice");
    tree.write(&document(&per_user(
        "mail",
        &[Account("alice", ALICE, &path, false)],
    )));
    let mut booted = boot_registered(&tree, &[ALICE]).await;

    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644))
        .expect("the key file is chmod-able");
    let status = reload(&mut booted).await;
    assert_eq!(status.outcome, Outcome::Refused);
    assert!(
        status
            .refusals
            .iter()
            .any(|line| line.contains(&path.display().to_string())
                && !line.contains("this change needs a restart")),
        "{:?}",
        status.refusals,
    );
    assert_eq!(
        sent(&booted, "mail", ALICE, None).as_deref(),
        Some("k3y-alice")
    );

    std::fs::remove_file(&path).expect("the key file is removable");
    let status = reload(&mut booted).await;
    assert_eq!(status.outcome, Outcome::Refused);
    assert!(
        status
            .refusals
            .iter()
            .any(|line| line.contains(&path.display().to_string())),
        "{:?}",
        status.refusals,
    );
    assert_eq!(
        sent(&booted, "mail", ALICE, None).as_deref(),
        Some("k3y-alice")
    );
}
