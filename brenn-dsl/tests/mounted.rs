//! Config-carrying mounts: a mount's `config/` tree as part of the document.
//!
//! A mount the operator declared `under` a principal may carry a fourth tree
//! whose entry document is compiled with the deployment. What is under test
//! here is the three things that make it true: the tree is loaded under keys of
//! its own, everything it declares is namespaced under the mount's name and
//! attributed to a stamp the mount is, and its top level admits only what a
//! ceiling can bound.

mod support;

use brenn_dsl::diag::Diagnostic;
use brenn_dsl::resolved::{
    ChanId, HandlePath, RChanRef, RMatcherVal, ResolvedConfig, StampId, StampOrigin,
};
use brenn_dsl::{DocumentInputs, DocumentRole, compile};
use support::{
    compile_with_mount, compile_with_mounts, derive_with_mount, derive_with_mounts, durable,
    messages,
};

/// A deployment root that declares the ceiling and nothing else.
/// The ceiling every fixture's mount is declared under: reach over the mount's
/// own address namespace and no capability word.
///
/// A mount stamp holds no default reach, so a fragment that declares a channel
/// outside these two lines is refused; the lines themselves are not dead config
/// over an empty fragment, because the mount is what holds them.
const CEILING: &str = "principal automator {\n  \
                       acl publish [prefix \"brenn:automations.\"];\n  \
                       acl subscribe [prefix \"brenn:automations.\"];\n}\n";

/// The handles of every channel the document declares, in order.
fn channels(config: &ResolvedConfig) -> Vec<String> {
    config
        .channels
        .iter()
        .map(|channel| channel.handle.dotted())
        .collect()
}

/// Resolve a one-file fragment under `automator`, or panic with the refusals.
fn resolved(fragment: &str) -> ResolvedConfig {
    compile_with_mount(
        &[("", CEILING)],
        "automations",
        "automator",
        &[("", fragment)],
    )
    .unwrap_or_else(|errors| panic!("{:?}", messages(&errors)))
}

/// Resolve a one-file fragment under `automator`, expecting refusals.
fn refused(fragment: &str) -> Vec<Diagnostic> {
    compile_with_mount(
        &[("", CEILING)],
        "automations",
        "automator",
        &[("", fragment)],
    )
    .expect_err("the fragment was expected not to compile")
}

// ── the mount is a stamp ─────────────────────────────────────────────────────

/// The whole shape in one test: the mount is recorded as a stamp under the
/// principal the mounts document named, and it wrote no body of its own —
/// which is what makes its ceiling exactly that principal.
#[test]
fn a_config_carrying_mount_is_a_stamp_under_its_principal() {
    let config = resolved(&durable("digest", "brenn:automations.digest"));
    let stamp = &config.stamps[0];
    assert!(
        matches!(stamp.origin, StampOrigin::Mount),
        "the mount's stamp is not an assembly's"
    );
    assert_eq!(stamp.handle.dotted(), "automations");
    assert_eq!(
        stamp
            .under
            .as_ref()
            .map(brenn_dsl::resolved::HandlePath::dotted),
        Some("automator".to_string())
    );
    assert!(!stamp.wrote_body, "a mount declares no ceiling of its own");
    assert!(stamp.grants.is_none());
    assert!(stamp.acls.is_empty());
    assert_eq!(stamp.parent, None);
}

/// The mount's name prefixes every handle its config declares, at the top level
/// and inside anything it stamps, and every one of them is attributed to the
/// mount's stamp.
#[test]
fn a_fragments_handles_are_namespaced_under_the_mount() {
    let config = resolved(&format!(
        "{}\nassembly Pair() {{\n{}}}\nnew pair: Pair();\n",
        durable("digest", "brenn:automations.digest"),
        durable("inner", "brenn:automations.inner"),
    ));
    assert_eq!(
        channels(&config),
        ["automations.digest", "automations.pair.inner"]
    );
    for channel in &config.channels {
        assert_eq!(
            channel.stamp,
            Some(StampId(0)),
            "`{}` is not held by the mount",
            channel.handle.dotted()
        );
    }
}

/// A mount's name, whatever it is, leads its fragment's handles.
#[test]
fn two_mounts_declare_under_their_own_names() {
    let one = resolved(&durable("digest", "brenn:one.digest"));
    assert_eq!(channels(&one), ["automations.digest"]);
    let two = compile_with_mount(
        &[("", CEILING)],
        "other",
        "automator",
        &[("", &durable("digest", "brenn:two.digest"))],
    )
    .unwrap_or_else(|errors| panic!("{:?}", messages(&errors)));
    assert_eq!(channels(&two), ["other.digest"]);
}

// ── the ceiling names a principal of the deployment ──────────────────────────

/// Resolution's refusal, not derivation's assertion: derivation resolves a
/// stamp's `under` through a map it trusts resolution to have filled.
#[test]
fn a_mount_under_a_name_no_principal_carries_is_refused() {
    let errors = compile_with_mount(
        &[("", CEILING)],
        "automations",
        "nobody",
        &[("", &durable("digest", "brenn:automations.digest"))],
    )
    .expect_err("a ceiling that names nothing does not compile");
    assert_eq!(
        messages(&errors),
        ["`automations` is under `nobody`, which the deployment declares no `principal` for"]
    );
}

/// The refusal is positioned in the mounts document, at the `under` clause: the
/// line the operator has to edit is not in either compiled tree.
#[test]
fn the_unknown_ceiling_is_cited_at_the_mounts_document() {
    let errors = compile_with_mount(
        &[("", CEILING)],
        "automations",
        "nobody",
        &[("", &durable("digest", "brenn:automations.digest"))],
    )
    .expect_err("a ceiling that names nothing does not compile");
    let rendered = errors[0].render();
    // The column, not just the file: the two spans a mounted root carries are
    // the `mount` item and the `under` clause, and this refusal is the
    // ceiling's, so it belongs on the clause.
    let column = "mount automations under ".len() + 1;
    assert!(
        rendered.starts_with(&format!("prod.mounts.brenn:1:{column}:")),
        "the refusal is not at the `under` clause: {rendered}"
    );
}

/// A principal whose own body was refused is withheld, and that refusal is the
/// answer; a second one would send the operator to the wrong file.
#[test]
fn a_ceiling_withheld_where_it_was_written_draws_one_refusal() {
    let root = "principal automator {\n    grants = [publish];\n    nonsense = 3;\n}\n";
    let errors = compile_with_mount(
        &[("", root)],
        "automations",
        "automator",
        &[("", &durable("digest", "brenn:automations.digest"))],
    )
    .expect_err("a principal with a stray attr does not compile");
    assert!(
        !messages(&errors)
            .iter()
            .any(|message| message.contains("declares no `principal` for")),
        "the withheld principal drew a second refusal: {:?}",
        messages(&errors)
    );
}

// ── the mount's name is the fragment's namespace ─────────────────────────────

/// A mount named for something the deployment already declares would give one
/// name to two things, since the mount's name prefixes the fragment's handles.
#[test]
fn a_mount_name_that_a_deployment_handle_holds_is_refused() {
    let root = format!("{CEILING}{}", durable("automations", "brenn:root.thing"));
    let errors = compile_with_mount(
        &[("", &root)],
        "automations",
        "automator",
        &[("", &durable("digest", "brenn:automations.digest"))],
    )
    .expect_err("a mount named for a root handle does not compile");
    assert_eq!(
        messages(&errors),
        [
            "`automations` is a mount and a channel here; a mount's name prefixes \
             everything its config declares"
        ]
    );
}

/// The rule is about the deployment tree only: a fragment declaring its own
/// `automations` handle is `automations.automations`, which collides with
/// nothing.
#[test]
fn a_fragment_may_declare_a_handle_of_the_mounts_own_name() {
    let config = resolved(&durable("automations", "brenn:automations.self"));
    assert_eq!(channels(&config), ["automations.automations"]);
}

// ── the top-level discipline ─────────────────────────────────────────────────

/// What a fragment admits: channels, links, assemblies of its own, `new`,
/// constants and pins.
#[test]
fn a_fragment_declares_channels_links_and_its_own_arrangements() {
    let fragment = format!(
        "const window = 8;\n{}link fan;\nassembly Arr() {{\n    link inner;\n}}\n\
         new arr: Arr();\n",
        durable("digest", "brenn:automations.digest"),
    );
    let config = resolved(&fragment);
    assert_eq!(
        config
            .links
            .iter()
            .map(|link| link.handle.dotted())
            .collect::<Vec<_>>(),
        ["automations.fan", "automations.arr.inner"]
    );
}

/// A tuning names a system-minted family by raw address and carries no stamp,
/// so nothing would bound it.
#[test]
fn a_fragment_tunes_no_family() {
    assert_eq!(
        messages(&refused(&support::tuning("mqtt:broker."))),
        [
            "a mount's config sizes the channels it declares; a family the deployment \
             mints is the deployment's to tune"
        ]
    );
}

/// A class is declared by the package that ships it; a fragment reaches one the
/// way every deployment does.
#[test]
fn a_fragment_declares_no_component_class() {
    let errors = refused("component Thing {\n    abi = wasm;\n    in a;\n}\n");
    let messages = messages(&errors);
    assert!(
        messages[0]
            .starts_with("a mount's config instantiates component classes and declares none"),
        "{messages:?}"
    );
}

/// Each of these is either a host fact or an entity whose authority is not
/// spelled in ceiling words.
#[test]
fn a_fragment_declares_nothing_the_deployment_owns() {
    for (source, kindword) in [
        (
            "remote peer {\n    token_file = \"/x\";\n    grants = [publish];\n}\n",
            "a remote",
        ),
        (
            "repo life {\n    remote = \"https://x/y.git\";\n}\n",
            "a repo",
        ),
        (
            "mqtt_client broker {\n    url = \"mqtt://x:1883\";\n}\n",
            "an mqtt client",
        ),
        (
            "mcp_server tool {\n    command = \"x\";\n}\n",
            "an mcp server",
        ),
    ] {
        assert_eq!(
            messages(&refused(source)),
            [format!(
                "a mount's config places components and channels; {kindword} is the \
                 deployment's to declare"
            )],
            "{source}"
        );
    }
}

/// A top-level `grant` or `acl` aims authority at an entity outside the mount's
/// namespace; a fragment writes its reach in its own bodies.
///
/// One refusal each, and each names the item the author wrote: an author who
/// pasted an `acl` at top level — the likeliest of the two — is told about an
/// `acl`.
#[test]
fn a_fragment_writes_no_top_level_grant_or_acl() {
    for (source, opening) in [
        (
            "acl subscribe [prefix \"brenn:automations.\"];\n",
            "an acl statement needs an enclosing entity body",
        ),
        (
            "grant automator publish prefix \"brenn:automations.\";\n",
            "a top-level `grant` aims authority at an entity",
        ),
    ] {
        let errors = refused(source);
        let messages = messages(&errors);
        assert!(messages[0].starts_with(opening), "{source}: {messages:?}");
    }
}

/// The existing refusal, unchanged: a mount path is a host fact and a mounts
/// document is where one is written.
#[test]
fn a_fragment_declares_no_mount() {
    let errors = refused("mount other {\n    path = \"/x\";\n}\n");
    let messages = messages(&errors);
    assert!(
        messages[0].starts_with("a `mount` is declared in the mounts document"),
        "{messages:?}"
    );
}

// ── the loader ───────────────────────────────────────────────────────────────

/// A fragment's tree `use` resolves under the fragment's own directory, and its
/// files are placed under a key of the mount's own.
#[test]
fn a_fragments_tree_is_loaded_under_its_own_keys() {
    let dir = support::scratch("mounted-tree");
    std::fs::write(dir.join("main.brenn"), CEILING).expect("the root writes");
    let config_dir = dir.join("automations/config");
    std::fs::create_dir_all(&config_dir).expect("the config tree writes");
    std::fs::write(
        config_dir.join("main.brenn"),
        format!(
            "use helpers::*;\n{}",
            durable("digest", "brenn:automations.digest")
        ),
    )
    .expect("the entry writes");
    std::fs::write(
        config_dir.join("helpers.brenn"),
        durable("side", "brenn:automations.side"),
    )
    .expect("the tree module writes");
    let document = compile(&DocumentInputs {
        root: dir.join("main.brenn"),
        module_roots: Vec::new().into(),
        mounted: vec![support::mounted_root(
            "automations",
            "automator",
            &config_dir,
        )],
        role: DocumentRole::Deployment,
    })
    .unwrap_or_else(|errors| panic!("{:?}", messages(&errors)));
    let places: Vec<String> = document
        .files
        .iter()
        .map(|file| file.path.display().to_string())
        .collect();
    assert_eq!(
        places,
        [
            "main.brenn",
            "mount:automations/main.brenn",
            "mount:automations/helpers.brenn"
        ]
    );
}

/// A `config/` tree with no entry is a document fact, not a mount fact, so it
/// is reported with the document's refusals — at the mounts document's line.
#[test]
fn a_config_tree_with_no_entry_is_refused_at_the_mount_line() {
    let dir = support::scratch("mounted-no-entry");
    std::fs::write(dir.join("main.brenn"), CEILING).expect("the root writes");
    let config_dir = dir.join("automations/config");
    std::fs::create_dir_all(&config_dir).expect("the config tree writes");
    let errors = compile(&DocumentInputs {
        root: dir.join("main.brenn"),
        module_roots: Vec::new().into(),
        mounted: vec![support::mounted_root(
            "automations",
            "automator",
            &config_dir,
        )],
        role: DocumentRole::Deployment,
    })
    .expect_err("a config tree with no entry does not compile");
    assert_eq!(
        messages(&errors),
        [format!(
            "`automations` carries config and `{}` is not there",
            config_dir.join("main.brenn").display()
        )]
    );
    // An absent entry is a fault of the mount, not of its ceiling, so it is
    // positioned on the mount's own name rather than on its `under` clause.
    let column = "mount ".len() + 1;
    assert!(
        errors[0]
            .render()
            .starts_with(&format!("prod.mounts.brenn:1:{column}:")),
        "{}",
        errors[0].render()
    );
}

/// The identity covers the fragment: an edit inside a mount moves the hash the
/// host reports as `applied` and compares as `unchanged`.
#[test]
fn a_fragment_edit_moves_the_document_identity() {
    let hash = |body: &str| {
        let dir = support::scratch("mounted-identity");
        std::fs::write(dir.join("main.brenn"), CEILING).expect("the root writes");
        let config_dir = dir.join("automations/config");
        std::fs::create_dir_all(&config_dir).expect("the config tree writes");
        std::fs::write(config_dir.join("main.brenn"), body).expect("the entry writes");
        compile(&DocumentInputs {
            root: dir.join("main.brenn"),
            module_roots: Vec::new().into(),
            mounted: vec![support::mounted_root(
                "automations",
                "automator",
                &config_dir,
            )],
            role: DocumentRole::Deployment,
        })
        .unwrap_or_else(|errors| panic!("{:?}", messages(&errors)))
        .document_sha256()
    };
    let before = hash(&durable("digest", "brenn:automations.digest"));
    let after = hash(&format!(
        "{}\n",
        durable("digest", "brenn:automations.digest")
    ));
    assert_ne!(before, after, "the fragment is outside the identity");
}

/// A tree module of one mount is not reachable from another authority root: the
/// keys namespace it, so a deployment `use helpers;` reads the deployment's.
#[test]
fn a_tree_module_name_is_not_shared_across_authority_roots() {
    let dir = support::scratch("mounted-disjoint");
    std::fs::write(
        dir.join("main.brenn"),
        format!("use helpers::*;\n{CEILING}"),
    )
    .expect("the root writes");
    std::fs::write(
        dir.join("helpers.brenn"),
        durable("root_side", "brenn:root.side"),
    )
    .expect("the root's tree module writes");
    let config_dir = dir.join("automations/config");
    std::fs::create_dir_all(&config_dir).expect("the config tree writes");
    std::fs::write(config_dir.join("main.brenn"), "use helpers::*;\n").expect("the entry writes");
    std::fs::write(
        config_dir.join("helpers.brenn"),
        durable("mount_side", "brenn:automations.side"),
    )
    .expect("the fragment's tree module writes");
    let document = compile(&DocumentInputs {
        root: dir.join("main.brenn"),
        module_roots: Vec::new().into(),
        mounted: vec![support::mounted_root(
            "automations",
            "automator",
            &config_dir,
        )],
        role: DocumentRole::Deployment,
    })
    .unwrap_or_else(|errors| panic!("{:?}", messages(&errors)));
    assert_eq!(
        channels(&document.resolved),
        ["root_side", "automations.mount_side"]
    );
}

/// A fragment's `use` of a module that is not in its own tree names the absence
/// in the fragment's namespace, not the deployment's.
#[test]
fn a_fragment_reaching_outside_its_tree_names_no_module() {
    let dir = support::scratch("mounted-escape");
    std::fs::write(dir.join("main.brenn"), CEILING).expect("the root writes");
    std::fs::write(dir.join("helpers.brenn"), "const x = 1;\n").expect("the sibling writes");
    let config_dir = dir.join("automations/config");
    std::fs::create_dir_all(&config_dir).expect("the config tree writes");
    std::fs::write(config_dir.join("main.brenn"), "use helpers::*;\n").expect("the entry writes");
    let errors = compile(&DocumentInputs {
        root: dir.join("main.brenn"),
        module_roots: Vec::new().into(),
        mounted: vec![support::mounted_root(
            "automations",
            "automator",
            &config_dir,
        )],
        role: DocumentRole::Deployment,
    })
    .expect_err("a fragment cannot reach the deployment's tree");
    assert!(
        messages(&errors)[0].starts_with("no module `mount:automations::helpers`"),
        "{:?}",
        messages(&errors)
    );
}

// ── principals inside a fragment ─────────────────────────────────────────────

/// A `principal` the fragment declares is one segment under the mount's name,
/// carries the mount's stamp as its origin, and hangs beneath the ceiling with
/// no `under` written: the ceiling has no name inside the fragment, so the
/// resolver fills the parent in.
#[test]
fn a_fragment_principal_is_namespaced_and_rooted_at_the_ceiling() {
    let config = resolved("principal q {\n    grants = [publish];\n}\n");
    let principals: Vec<&str> = config
        .principals
        .iter()
        .map(|principal| {
            principal
                .handle
                .0
                .last()
                .expect("a segment")
                .value()
                .as_str()
        })
        .collect();
    assert_eq!(principals, ["automator", "q"]);
    let q = &config.principals[1];
    assert_eq!(q.handle.dotted(), "automations.q");
    assert_eq!(q.origin, Some(StampId(0)));
    assert_eq!(
        q.parent.as_ref().map(HandlePath::dotted),
        Some("automator".to_string())
    );
    assert_eq!(config.principals[0].origin, None);
}

/// `principal r under q` names the fragment's own `q`, at its full handle: two
/// fragments each declaring `q` are two principals, and a bare name here would
/// merge them in derivation's dotted-handle maps.
#[test]
fn a_fragment_principal_under_another_chains_inside_the_fragment() {
    let config = resolved(
        "principal q {\n    grants = [publish];\n}\n\
         principal r under q {\n    grants = [publish];\n}\n",
    );
    let r = &config.principals[2];
    assert_eq!(r.handle.dotted(), "automations.r");
    assert_eq!(
        r.parent.as_ref().map(HandlePath::dotted),
        Some("automations.q".to_string())
    );
}

/// A `new … under q` written in the body of an assembly the fragment declared
/// resolves through the same namespace at any depth. `prefix` is the wrong
/// lever for this: it deepens with every nested body, and the handle would come
/// out `automations.<inst>.q`.
#[test]
fn a_stamp_in_a_fragment_body_names_the_fragments_principal() {
    let config = resolved(
        "principal q {\n    grants = [publish];\n}\n\
         assembly Leaf() {\n    channel c at \"brenn:automations.c\" { push_depth = 1; }\n}\n\
         assembly Body() {\n    new leaf: Leaf() under q;\n}\n\
         new body: Body();\n",
    );
    let leaf = config
        .stamps
        .iter()
        .find(|stamp| stamp.handle.dotted() == "automations.body.leaf")
        .expect("the nested stamp is recorded");
    assert_eq!(
        leaf.under.as_ref().map(HandlePath::dotted),
        Some("automations.q".to_string())
    );
}

/// A fragment principal handed to an arrangement as a `Principal` argument is
/// bound to the same handle, so the arrangement's own `under <param>` resolves
/// to the principal the model holds and the handed-principal escape in the
/// dead-config pass finds it.
#[test]
fn a_fragment_principal_handed_as_an_argument_is_namespaced() {
    let config = resolved(
        "principal q {\n    grants = [publish];\n}\n\
         assembly Leaf() {\n    channel c at \"brenn:automations.c\" { push_depth = 1; }\n}\n\
         assembly Holder(who: Principal) {\n    new leaf: Leaf() under who;\n}\n\
         new holder: Holder(who = q);\n",
    );
    let handed: Vec<String> = config
        .handed_principals
        .iter()
        .map(HandlePath::dotted)
        .collect();
    assert_eq!(handed, ["automations.q"]);
    let leaf = config
        .stamps
        .iter()
        .find(|stamp| stamp.handle.dotted() == "automations.holder.leaf")
        .expect("the nested stamp is recorded");
    assert_eq!(
        leaf.under.as_ref().map(HandlePath::dotted),
        Some("automations.q".to_string())
    );
}

/// A mount's ceiling is the deployment's to write. One that names a principal
/// the mount's own config declares would be a ceiling inside the authority it
/// bounds, and the mounts document is where the refusal belongs.
#[test]
fn a_mount_under_its_own_configs_principal_is_refused() {
    let errors = compile_with_mount(
        &[("", CEILING)],
        "automations",
        "automations.q",
        &[("", "principal q {\n    grants = [publish];\n}\n")],
    )
    .expect_err("a ceiling inside the mount does not compile");
    // Two refusals, and the second is a consequence of the first: a fragment
    // principal with no `under` hangs beneath the ceiling, so a ceiling that
    // names one makes it its own parent. The mounts document is still the line
    // to edit.
    assert!(
        messages(&errors).contains(
            &"`automations` is under `automations.q`, which the mount `automations` declares; \
              a mount's ceiling is the deployment's to write"
        ),
        "{:?}",
        messages(&errors)
    );
}

/// A cycle among a fragment's own principals is refused where every cycle is,
/// and positioned on the `under` clause that closed it — the last segment of
/// the handle, not the mount's name, whose span is in the mounts document.
#[test]
fn a_cycle_among_fragment_principals_is_refused() {
    let fragment = "principal q under r {\n    grants = [publish];\n}\n\
                    principal r under q {\n    grants = [publish];\n}\n";
    let errors = refused(fragment);
    assert_eq!(
        messages(&errors),
        [
            "`automations.r` is under `automations.q`, which is under `automations.r`; \
             a chain of principals bottoms out at the operator"
        ]
    );
    let rendered = errors[0].render();
    assert!(
        rendered.starts_with("mount:automations/main.brenn:"),
        "the cycle is not cited in the fragment: {rendered}"
    );
}

// ── what a ceiling bounds ────────────────────────────────────────────────────

/// A mount stamp holds no default reach: declaring a mount is consent to
/// whatever its author writes next, which is consent to no address at all. So a
/// channel the fragment declares must sit where the ceiling's reach is written.
#[test]
fn a_fragment_channel_outside_the_ceilings_reach_is_refused() {
    let refusals = derive_with_mount(
        &[("", CEILING)],
        "automations",
        "automator",
        &[("", &durable("stray", "brenn:elsewhere.stray"))],
    )
    .expect_err("a channel nothing reaches does not compile");
    assert_eq!(
        messages(&refusals),
        [
            "`automations` declares `brenn:elsewhere.stray` and `automator` reaches it on no \
             plane; a mount's channels live where its principal's reach is written"
        ]
    );
}

/// The same channel inside the ceiling's prefix compiles, and the reach it is
/// authorized by is the operator's line rather than the fragment's declaration.
#[test]
fn a_fragment_channel_inside_the_ceilings_reach_compiles() {
    derive_with_mount(
        &[("", CEILING)],
        "automations",
        "automator",
        &[("", &durable("digest", "brenn:automations.digest"))],
    )
    .unwrap_or_else(|errors| panic!("{:?}", messages(&errors)));
}

/// A ceiling over a fragment that declares nothing keeps its words and its
/// lines. The dead-config rules read what is stamped beneath a principal *now*;
/// a mount's ceiling caps text its author has not written yet, and the mount is
/// the arrangement that holds every word of it.
#[test]
fn a_ceiling_over_an_empty_fragment_is_not_dead_config() {
    let ceiling = "principal automator {\n    grants = [publish, subscribe];\n    \
                   acl publish [prefix \"brenn:automations.\"];\n}\n";
    derive_with_mount(&[("", ceiling)], "automations", "automator", &[("", "")])
        .unwrap_or_else(|errors| panic!("{:?}", messages(&errors)));
}

/// A fragment principal gets no such exemption: it caps text in the same
/// fragment, so a word nothing under it holds is dead config exactly as it
/// would be in the root.
#[test]
fn a_fragment_principal_capping_nothing_is_dead_config() {
    let refusals = derive_with_mount(
        &[("", CEILING)],
        "automations",
        "automator",
        &[("", "principal q {\n    grants = [publish];\n}\n")],
    )
    .expect_err("a principal nothing is under does not compile");
    assert!(
        messages(&refusals)
            .iter()
            .any(|message| { message.starts_with("`automations.q` delegates to nothing") }),
        "{:?}",
        messages(&refusals)
    );
}

/// A fragment's chain bottoms out at the ceiling, so a principal it declares is
/// held to the ceiling's words — with `above` naming the deployment principal,
/// which is the name the author asks the operator to widen.
#[test]
fn a_fragment_principal_exceeding_its_ceiling_is_refused() {
    let refusals = derive_with_mount(
        &[("", CEILING)],
        "automations",
        "automator",
        &[(
            "",
            "principal q {\n    grants = [publish];\n}\n\
             assembly Leaf() {\n    channel c at \"brenn:automations.c\" { push_depth = 1; }\n}\n\
             new leaf: Leaf() under q;\n",
        )],
    )
    .expect_err("a principal wider than its ceiling does not compile");
    assert!(
        messages(&refusals)
            .iter()
            .any(|message| message.contains("`automations.q`") && message.contains("`automator`")),
        "{:?}",
        messages(&refusals)
    );
}

/// The discipline pass stops at the item level, so an assembly whose *body*
/// places a surface reaches expansion. The `new` is the consent to the whole
/// arrangement, so the `new` is where a fragment is refused one: the assembly
/// is fine for the deployment to stamp.
#[test]
fn a_fragment_stamping_an_assembly_that_places_a_surface_is_refused() {
    let errors = compile_with_mount(
        &[("", CEILING), ("@inner", INNER)],
        "automations",
        "automator",
        &[("", "use @inner::*;\n\nnew page: Inner(slug = \"demo\");\n")],
    )
    .expect_err("a fragment that places a surface does not compile");
    assert!(
        messages(&errors).iter().any(|message| {
            *message
                == "`automations.page`, stamping `Inner`, places a surface; a mount's config \
                    places components and channels"
        }),
        "{:?}",
        messages(&errors)
    );
}

/// A packaged arrangement that places a surface for the deployment is not
/// refused: what this rule is about is the authority root the `new` is written
/// in, not the assembly.
#[test]
fn the_same_assembly_stamped_by_the_deployment_is_fine() {
    compile_with_mount(
        &[
            (
                "",
                &format!("use @inner::*;\n\n{CEILING}new page: Inner(slug = \"demo\");\n"),
            ),
            ("@inner", INNER),
        ],
        "automations",
        "automator",
        &[("", &durable("digest", "brenn:automations.digest"))],
    )
    .unwrap_or_else(|errors| panic!("{:?}", messages(&errors)));
}

/// A fragment's `extend surface` block reaches the item-level discipline: a
/// contribution is not one of the entities a mount may not write, and it is the
/// authority pass that decides which surfaces this mount may place on.
///
/// Resolution alone is what this asserts — the ceiling in this suite's root
/// writes no `surfaces` line, so the block is refused at derivation, which the
/// `surfaces`-axis cases below assert.
#[test]
fn a_fragments_contribution_resolves_and_lands_on_the_merged_surface() {
    let config = compile_with_mount(
        &[
            (
                "",
                &format!("use @inner::*;\n\n{CEILING}new page: Inner(slug = \"demo\");\n"),
            ),
            ("@inner", INNER),
        ],
        "automations",
        "automator",
        &[(
            "",
            &format!(
                "use @inner::*;\n\n{}\nextend surface \"demo\" {{\n  \
                 new extra: Panel {{ grants = [dom]; in messages <- digest; }}\n}}\n",
                durable("digest", "brenn:automations.digest")
            ),
        )],
    )
    .unwrap_or_else(|errors| panic!("{:?}", messages(&errors)));
    let [surface] = &config.surfaces[..] else {
        panic!("one surface");
    };
    let names: Vec<&str> = surface
        .components
        .iter()
        .map(|component| component.instance.value().as_str())
        .collect();
    assert_eq!(names, ["panel", "extra"]);
    // The contributed component keeps the mount's stamp, not the surface's:
    // after the merge one surface holds components from two authority roots.
    assert_eq!(surface.components[1].stamp, Some(StampId(0)));
}

/// The same block written by the deployment lands: what this rule is about is
/// the authority root the block was expanded in.
#[test]
fn the_deployment_places_on_its_own_surface_from_anywhere() {
    let config = compile_with_mount(
        &[
            (
                "",
                &format!(
                    "use @inner::*;\n\n{CEILING}new page: Inner(slug = \"demo\");\n\n\
                     extend surface \"demo\" {{\n  \
                     new extra: Panel {{ grants = [dom]; in messages <- page.out; }}\n}}\n"
                ),
            ),
            ("@inner", INNER),
        ],
        "automations",
        "automator",
        &[("", &durable("digest", "brenn:automations.digest"))],
    )
    .unwrap_or_else(|errors| panic!("{:?}", messages(&errors)));
    let [surface] = &config.surfaces[..] else {
        panic!("one surface");
    };
    assert_eq!(surface.components.len(), 2);
    assert_eq!(
        surface.components[1].stamp, None,
        "the contribution is the deployment's own text"
    );
}

/// A packaged arrangement placing a surface, for the two tests above.
const INNER: &str = "\
component Panel { abi = processor; requires = [dom]; in messages; }

assembly Inner(slug: String) {
    channel out at f\"ephemeral:{slug}.out\" { push_depth = 4; retain_depth = 16; }
    surface page {
        slug = slug;
        grants = [subscribe];
        new panel: Panel { chrome = true; grants = [dom]; in messages <- out; }
    }
}
";

// ── across authority roots, the address is the contract ──────────────────────
//
// Handles do not cross an authority boundary: a fragment's `use` cannot reach
// the deployment tree and the deployment's cannot reach a fragment. So the
// address is the interface, and a literal naming a channel the *other*
// authority root declared is resolved to that declaration rather than refused.
// Inside one authority root the one-spelling rule is untouched.

/// A packaged class for the cross-root fixtures: one `in` port, nothing
/// required, no document type.
const SINK: &str = "component Sink { abi = processor; requires = []; in messages; }\n";

/// One top-level instance of `class` binding `messages` to what `chan` spells —
/// a handle, or an address in quotes.
fn sink(handle: &str, class: &str, chan: &str) -> String {
    format!(
        "new {handle}: {class} {{ slug = \"{handle}\"; grants = []; \
         in messages <- {chan}; }}\n"
    )
}

/// The position of the channel a handle names.
fn channel_at(config: &ResolvedConfig, handle: &str) -> ChanId {
    ChanId(
        config
            .channels
            .iter()
            .position(|channel| channel.handle.dotted() == handle)
            .unwrap_or_else(|| panic!("`{handle}` is declared")),
    )
}

/// The one spelling a fragment has for a channel the deployment declared is its
/// address, and the compiler resolves the binding to the declaration — so every
/// later pass sees one channel and not two.
#[test]
fn a_fragment_naming_a_root_channel_by_address_resolves_to_it() {
    let config = compile_with_mount(
        &[
            (
                "",
                &format!("{CEILING}{}", durable("inbox", "brenn:root.inbox")),
            ),
            ("@sink", SINK),
        ],
        "automations",
        "automator",
        &[(
            "",
            &format!(
                "use @sink::*;\n{}",
                sink("worker", "Sink", "\"brenn:root.inbox\"")
            ),
        )],
    )
    .unwrap_or_else(|errors| panic!("{:?}", messages(&errors)));
    assert_eq!(
        config.consumers[0].bindings[0].chan,
        Some(RChanRef::Decl(channel_at(&config, "inbox")))
    );
}

/// And the other direction: the operator may bind a root port to a fragment's
/// address, and owns the refusal if the fragment goes away.
#[test]
fn the_deployment_naming_a_fragment_channel_by_address_resolves_to_it() {
    let config = compile_with_mount(
        &[
            (
                "",
                &format!(
                    "use @sink::*;\n{CEILING}{}",
                    sink("watcher", "Sink", "\"brenn:automations.digest\"")
                ),
            ),
            ("@sink", SINK),
        ],
        "automations",
        "automator",
        &[("", &durable("digest", "brenn:automations.digest"))],
    )
    .unwrap_or_else(|errors| panic!("{:?}", messages(&errors)));
    assert_eq!(
        config.consumers[0].bindings[0].chan,
        Some(RChanRef::Decl(channel_at(&config, "automations.digest")))
    );
}

/// An `exact` matcher is the other position that names a channel, and it
/// resolves the same way in both directions — which is what makes the
/// operator's `acl subscribe [exact "…"]` over a fragment's channel one
/// identity downstream rather than a second spelling.
#[test]
fn an_exact_matcher_resolves_across_authority_roots() {
    let config = compile_with_mount(
        &[
            (
                "",
                &format!(
                    "principal automator {{\n  \
                     acl publish [prefix \"brenn:automations.\"];\n  \
                     acl subscribe [prefix \"brenn:automations.\", \
                     exact \"brenn:automations.digest\"];\n}}\n{}",
                    durable("inbox", "brenn:root.inbox")
                ),
            ),
            ("@sink", SINK),
        ],
        "automations",
        "automator",
        &[(
            "",
            &format!(
                "use @sink::*;\n{}new worker: Sink {{ slug = \"worker\"; grants = []; \
                 acl subscribe [exact \"brenn:root.inbox\"];\n  in messages <- digest; }}\n",
                durable("digest", "brenn:automations.digest"),
            ),
        )],
    )
    .unwrap_or_else(|errors| panic!("{:?}", messages(&errors)));
    // The deployment's ceiling names the fragment's channel.
    let principal = &config.principals[0];
    assert_eq!(
        principal.acls[1].matchers[1].val.value(),
        &RMatcherVal::Chan(channel_at(&config, "automations.digest"))
    );
    // The fragment's consumer names the deployment's.
    assert_eq!(
        config.consumers[0].acls[0].matchers[0].val.value(),
        &RMatcherVal::Chan(channel_at(&config, "inbox"))
    );
}

/// Inside one authority root the rule is what it was: where a channel exists,
/// it is named. Both positions are held to it.
#[test]
fn a_literal_naming_a_channel_of_the_same_authority_root_is_still_refused() {
    let bound = compile_with_mount(
        &[("", CEILING), ("@sink", SINK)],
        "automations",
        "automator",
        &[(
            "",
            &format!(
                "use @sink::*;\n{}{}",
                durable("digest", "brenn:automations.digest"),
                sink("worker", "Sink", "\"brenn:automations.digest\"")
            ),
        )],
    )
    .expect_err("a fragment naming its own channel by address does not compile");
    assert_eq!(
        messages(&bound),
        [
            "`brenn:automations.digest` is the address channel `automations.digest` declares; \
          name the channel, not its address"
        ]
    );
    let matched = compile_with_mount(
        &[("", CEILING), ("@sink", SINK)],
        "automations",
        "automator",
        &[(
            "",
            &format!(
                "use @sink::*;\n{}new worker: Sink {{ slug = \"worker\"; grants = []; \
                 acl subscribe [exact \"brenn:automations.digest\"];\n  \
                 in messages <- digest; }}\n",
                durable("digest", "brenn:automations.digest"),
            ),
        )],
    )
    .expect_err("a fragment matching its own channel by address does not compile");
    assert_eq!(
        messages(&matched),
        [
            "`brenn:automations.digest` is the address channel `automations.digest` declares; \
          name the channel, not its address"
        ]
    );
}

/// The rule is per authority root and not per file: a fragment's second file
/// declaring the channel is the same author's text, so naming it by address is
/// the second spelling the rule refuses.
#[test]
fn a_literal_across_two_files_of_one_tree_is_refused() {
    let errors = compile_with_mount(
        &[("", CEILING), ("@sink", SINK)],
        "automations",
        "automator",
        &[
            (
                "",
                &format!(
                    "use @sink::*;\nuse helpers::*;\n{}",
                    sink("worker", "Sink", "\"brenn:automations.digest\"")
                ),
            ),
            ("helpers", &durable("digest", "brenn:automations.digest")),
        ],
    )
    .expect_err("one authority root's second file is the same author");
    assert_eq!(
        messages(&errors),
        [
            "`brenn:automations.digest` is the address channel `automations.digest` declares; \
          name the channel, not its address"
        ]
    );
}

// ── a pin travels with the declaration ───────────────────────────────────────

/// The uuid pin of `address`, as a `uuid_pins` section.
fn pin(address: &str) -> String {
    format!("uuid_pins {{\n    \"{address}\" = \"3f2504e0-4f89-41d3-9a0c-0305e82c3301\";\n}}\n")
}

/// A fragment re-identifies the channels it declares, which is what a migration
/// of its own durable ring needs.
#[test]
fn a_fragment_pins_its_own_channel() {
    let config = compile_with_mount(
        &[("", CEILING)],
        "automations",
        "automator",
        &[(
            "",
            &format!(
                "{}{}",
                durable("digest", "brenn:automations.digest"),
                pin("brenn:automations.digest")
            ),
        )],
    )
    .unwrap_or_else(|errors| panic!("{:?}", messages(&errors)));
    assert_eq!(config.uuid_pins[0].origin, Some(StampId(0)));
}

/// Re-identifying a channel one did not declare is a migration done from the
/// wrong side, in both directions.
#[test]
fn a_pin_on_another_authority_roots_channel_is_refused() {
    let from_fragment = compile_with_mount(
        &[(
            "",
            &format!("{CEILING}{}", durable("inbox", "brenn:root.inbox")),
        )],
        "automations",
        "automator",
        &[("", &pin("brenn:root.inbox"))],
    )
    .expect_err("a fragment pinning the deployment's channel does not compile");
    assert_eq!(
        messages(&from_fragment),
        [
            "`brenn:root.inbox` is declared by the deployment document; a pin travels with \
          the declaration"
        ]
    );
    let from_deployment = compile_with_mount(
        &[("", &format!("{CEILING}{}", pin("brenn:automations.digest")))],
        "automations",
        "automator",
        &[("", &durable("digest", "brenn:automations.digest"))],
    )
    .expect_err("the deployment pinning a fragment's channel does not compile");
    assert_eq!(
        messages(&from_deployment),
        [
            "`brenn:automations.digest` is declared by the config of mount `automations`; \
          a pin travels with the declaration"
        ]
    );
}

// ── doctypes unify on the declaration, across the boundary ───────────────────

/// Two classes for the doctype fixtures: one tag each, on the same port name.
const TAGGED: &str = "\
component Feed { abi = processor; requires = []; in messages: \"digest@1\"; }
component Stale { abi = processor; requires = []; in messages: \"digest@2\"; }
";

/// A deployment root declaring a channel inside the mount's reach, with one
/// consumer of `class` bound to it, and a fragment binding the same channel by
/// address with one consumer of its own.
fn across_roots(root_class: &str, fragment_class: &str) -> Result<(), Vec<Diagnostic>> {
    derive_with_mount(
        &[
            (
                "",
                &format!(
                    "use @tagged::*;\n{CEILING}{}{}",
                    durable("shared", "brenn:automations.shared"),
                    sink("reader", root_class, "shared")
                ),
            ),
            ("@tagged", TAGGED),
        ],
        "automations",
        "automator",
        &[(
            "",
            &format!(
                "use @tagged::*;\n{}",
                sink("worker", fragment_class, "\"brenn:automations.shared\"")
            ),
        )],
    )
    .map(|_| ())
}

/// The rewrite is what makes propagation cross the boundary: the fragment's
/// literal and the root's handle are one channel, so two ports agreeing on a
/// document type agree.
#[test]
fn doctypes_agree_across_authority_roots() {
    across_roots("Feed", "Feed").unwrap_or_else(|errors| panic!("{:?}", messages(&errors)));
}

/// And disagreeing, they are refused: the two claims meet on one channel only
/// because the fragment's literal resolved to the deployment's declaration.
///
/// The sites a conflict cites are where each tag is *declared*, which for a
/// packaged class is the package's own file — a fragment declares no component
/// class, so a port's tag is never written in a fragment. What is written in a
/// fragment is a channel's own expectation, which the test below cites.
#[test]
fn doctypes_conflicting_across_authority_roots_are_refused() {
    let errors = across_roots("Feed", "Stale")
        .expect_err("one channel carries one document, whoever bound it");
    let conflict = errors
        .iter()
        .find(|error| error.message.contains("different document types"))
        .unwrap_or_else(|| panic!("{:?}", messages(&errors)));
    assert!(
        conflict
            .message
            .contains("`shared` (`brenn:automations.shared`)"),
        "{}",
        conflict.message
    );
    let notes: Vec<&str> = conflict
        .related
        .iter()
        .map(|(note, _)| note.as_str())
        .collect();
    assert_eq!(
        notes,
        [
            "port `messages` of `Feed` declares `digest@1` here",
            "port `messages` of `Stale` declares `digest@2` here"
        ]
    );
}

/// A channel's own `doctype` is the operator's — or the fragment author's —
/// expectation, and it arbitrates across the boundary too: the refusal has a
/// site in each authority root, the fragment's declaration and the deployment's
/// binding.
#[test]
fn a_fragment_channels_expectation_holds_against_a_root_port() {
    let errors = derive_with_mount(
        &[
            (
                "",
                &format!(
                    "use @tagged::*;\n{CEILING}{}",
                    sink("reader", "Stale", "\"brenn:automations.digest\"")
                ),
            ),
            ("@tagged", TAGGED),
        ],
        "automations",
        "automator",
        &[(
            "",
            "channel digest at \"brenn:automations.digest\" {\n    push_depth = 4;\n    \
             retain_depth = 16;\n    standing_retain_depth = 64;\n    \
             doctype = \"digest@1\";\n}\n",
        )],
    )
    .expect_err("a channel's expectation is not the port's tag");
    let conflict = errors
        .iter()
        .find(|error| error.message.contains("expects `digest@1`"))
        .unwrap_or_else(|| panic!("{:?}", messages(&errors)));
    assert_eq!(
        conflict.message,
        "port `messages` of `Stale` declares `digest@2`, and channel \
         `automations.digest` expects `digest@1`"
    );
    let (_, site) = &conflict.related[0];
    assert_eq!(
        site.filename_inner()
            .expect("a parsed span carries its filename"),
        "mount:automations/main.brenn"
    );
}

// ── the fit rule is what a ceiling is enforced by ────────────────────────────
//
// The rules above cover what a fragment *declares*. What it *binds* is the
// other half and the primary one: a binding or an `acl` confers reach on the
// entity that holds it, and `check_fit` holds every conferred entry against the
// mount stamp's ceiling. A regression that let mount stamps back into
// `default_reach`, or that skipped the fit rule for a mount stamp, would leave
// the rest of this suite green while a fragment silently gained the bus.

/// A fragment binding a port to an address its ceiling does not reach is
/// refused, and the refusal names the mount's config as the stamp and the
/// ceiling as the line to widen.
#[test]
fn a_fragment_binding_outside_its_ceiling_is_refused() {
    let refusals = derive_with_mount(
        &[
            (
                "",
                &format!("{CEILING}{}", durable("secrets", "brenn:secrets.inbox")),
            ),
            ("@sink", SINK),
        ],
        "automations",
        "automator",
        &[(
            "",
            &format!(
                "use @sink::*;\n{}",
                sink("worker", "Sink", "\"brenn:secrets.inbox\"")
            ),
        )],
    )
    .expect_err("a fragment does not reach what its ceiling was not written for");
    let refusal = refusals
        .iter()
        .find(|error| error.message.contains("the config of mount `automations`"))
        .unwrap_or_else(|| panic!("{:?}", messages(&refusals)));
    assert!(
        refusal.message.contains("brenn:secrets.inbox") && refusal.message.contains("automator"),
        "{}",
        refusal.message
    );
}

/// The same for an `acl` written in a fragment's own instance body: a prefix
/// wider than the ceiling's is reach the operator never wrote.
#[test]
fn a_fragment_acl_outside_its_ceiling_is_refused() {
    let refusals = derive_with_mount(
        &[("", CEILING), ("@sink", SINK)],
        "automations",
        "automator",
        &[(
            "",
            &format!(
                "use @sink::*;\n{}new worker: Sink {{ slug = \"worker\"; grants = []; \
                 acl subscribe [prefix \"brenn:secrets.\"];\n  in messages <- digest; }}\n",
                durable("digest", "brenn:automations.digest"),
            ),
        )],
    )
    .expect_err("a fragment's acl is held to its ceiling");
    let refusal = refusals
        .iter()
        .find(|error| error.message.contains("the config of mount `automations`"))
        .unwrap_or_else(|| panic!("{:?}", messages(&refusals)));
    assert!(
        refusal.message.contains("brenn:secrets.") && refusal.message.contains("automator"),
        "{}",
        refusal.message
    );
}

// ── the skips in the declared-channel rule ───────────────────────────────────

/// The declared-channel rule reads the ceiling's reach entries for the
/// channel's own family. A **confined** address is spelled over by no family,
/// so there are no entries to read and nothing to refuse: the serving host
/// authorizes it and no ceiling holds it.
#[test]
fn a_fragment_declares_a_confined_channel_under_a_brenn_only_ceiling() {
    derive_with_mount(
        &[("", CEILING)],
        "automations",
        "automator",
        &[(
            "",
            "channel scratch at \"local:automations.scratch\" {\n    push_depth = 4;\n    \
             retain_depth = 16;\n}\n",
        )],
    )
    .unwrap_or_else(|errors| panic!("{:?}", messages(&errors)));
}

/// `ephemeral:` is not confined, so it is held like any other: a ceiling
/// written entirely in `brenn:` lines reaches no ephemeral address, and the
/// declaration is refused.
#[test]
fn a_fragment_ephemeral_channel_is_held_to_the_ceiling_like_any_other() {
    let refusals = derive_with_mount(
        &[("", CEILING)],
        "automations",
        "automator",
        &[(
            "",
            "channel scratch at \"ephemeral:automations.scratch\" {\n    push_depth = 4;\n    \
             retain_depth = 16;\n}\n",
        )],
    )
    .expect_err("a `brenn:` ceiling reaches no ephemeral address");
    assert_eq!(
        messages(&refusals),
        [
            "`automations` declares `ephemeral:automations.scratch` and `automator` reaches \
             it on no plane; a mount's channels live where its principal's reach is written"
        ]
    );
}

/// The rule refuses only a channel the ceiling reaches on *neither* plane. A
/// one-way arrangement — the fragment publishes what the operator reads
/// elsewhere — is legitimate, so a publish-only ceiling admits the declaration.
#[test]
fn a_fragment_channel_the_ceiling_reaches_on_one_plane_compiles() {
    let ceiling = "principal automator {\n    \
                   acl publish [prefix \"brenn:automations.\"];\n}\n";
    derive_with_mount(
        &[("", ceiling)],
        "automations",
        "automator",
        &[("", &durable("digest", "brenn:automations.digest"))],
    )
    .unwrap_or_else(|errors| panic!("{:?}", messages(&errors)));
}

// ── two config-carrying mounts ───────────────────────────────────────────────
//
// One mount exercises none of the per-mount bookkeeping. Two are the steady
// state: brenn's own release mount beside one or more authors'.

/// The ceiling both of the two-mount fixtures' mounts are declared under, wide
/// enough for either namespace.
const WIDE: &str = "principal automator {\n  \
                    acl publish [prefix \"brenn:\"];\n  \
                    acl subscribe [prefix \"brenn:\"];\n}\n";

/// Two mounts in one document are two namespaces: the same handle name, and the
/// same tree-module name, declared in both, are four distinct things.
#[test]
fn two_mounts_in_one_document_are_two_namespaces() {
    let fragment_a: &[(&str, &str)] = &[
        ("", "use helpers::*;\n"),
        ("helpers", &durable("digest", "brenn:one.digest")),
    ];
    let fragment_b: &[(&str, &str)] = &[
        ("", "use helpers::*;\n"),
        ("helpers", &durable("digest", "brenn:two.digest")),
    ];
    let config = compile_with_mounts(
        &[("", WIDE)],
        &[
            ("one", "automator", fragment_a),
            ("two", "automator", fragment_b),
        ],
    )
    .unwrap_or_else(|errors| panic!("{:?}", messages(&errors)));
    assert_eq!(channels(&config), ["one.digest", "two.digest"]);
    assert_eq!(config.stamps.len(), 2);
    assert_eq!(config.channels[0].stamp, Some(StampId(0)));
    assert_eq!(config.channels[1].stamp, Some(StampId(1)));
}

/// The address namespace is one namespace, whoever declares in it: two mounts
/// declaring one address collide exactly as two files of one tree do.
#[test]
fn two_mounts_declaring_one_address_collide() {
    let one: &[(&str, &str)] = &[("", &durable("digest", "brenn:shared.digest"))];
    let two: &[(&str, &str)] = &[("", &durable("digest", "brenn:shared.digest"))];
    let errors = compile_with_mounts(
        &[("", WIDE)],
        &[("one", "automator", one), ("two", "automator", two)],
    )
    .expect_err("one address is declared once");
    assert!(
        messages(&errors)
            .iter()
            .any(|message| message.contains("two channels declare the address")),
        "{:?}",
        messages(&errors)
    );
}

/// One fragment naming another's channel by address is a cross-root reference
/// like any other: the literal resolves to the declaration, and the binding is
/// then held to the *binding* fragment's own ceiling. Neither author consents
/// to the other; the operator's two `under` lines do, one each.
#[test]
fn one_fragment_names_another_fragments_channel_by_address() {
    let producer: &[(&str, &str)] = &[("", &durable("digest", "brenn:one.digest"))];
    let consumer: &[(&str, &str)] = &[(
        "",
        &format!(
            "use @sink::*;\n{}",
            sink("worker", "Sink", "\"brenn:one.digest\"")
        ),
    )];
    let config = compile_with_mounts(
        &[("", WIDE), ("@sink", SINK)],
        &[
            ("one", "automator", producer),
            ("two", "automator", consumer),
        ],
    )
    .unwrap_or_else(|errors| panic!("{:?}", messages(&errors)));
    assert_eq!(
        config.consumers[0].bindings[0].chan,
        Some(RChanRef::Decl(channel_at(&config, "one.digest")))
    );

    // And the ceiling still decides: the same document with `two` under a
    // principal that does not reach `brenn:one.` is refused.
    let narrow = "principal automator {\n  \
                  acl publish [prefix \"brenn:\"];\n  \
                  acl subscribe [prefix \"brenn:\"];\n}\n\
                  principal narrow {\n  \
                  acl publish [prefix \"brenn:two.\"];\n  \
                  acl subscribe [prefix \"brenn:two.\"];\n}\n";
    let refusals = derive_with_mounts(
        &[("", narrow), ("@sink", SINK)],
        &[("one", "automator", producer), ("two", "narrow", consumer)],
    )
    .expect_err("a fragment reaches another's channel only where its own ceiling says so");
    assert!(
        messages(&refusals)
            .iter()
            .any(|message| message.contains("the config of mount `two`")),
        "{:?}",
        messages(&refusals)
    );
}

// ── an agent's subscription is a cross-root position too ─────────────────────

/// A minimal deployment agent class, for the subscription fixtures.
fn assistant(chan: &str) -> String {
    format!(
        "agent Assistant() {{\n    name = \"A\";\n    grants = [subscribe];\n    \
         subscribe {chan} {{ push_depth = 4; }}\n}}\nnew alice: Assistant();\n"
    )
}

/// The same class with no subscription, for the fixture that instantiates it
/// from the wrong authority root.
const ASSISTANT: &str = "agent Assistant() {\n    name = \"A\";\n    grants = [subscribe];\n}\n";

/// A deployment agent subscribing to a fragment's channel by address is the
/// headline use of the rule, and the agent subscription is one of the two
/// position kinds the rewrite covers: it resolves to the declaration, not to a
/// loose address, so ACL derivation and doctype propagation see one channel.
#[test]
fn a_deployment_agent_subscribes_to_a_fragment_channel_by_address() {
    let config = compile_with_mount(
        &[(
            "",
            &format!("{CEILING}{}", assistant("\"brenn:automations.digest\"")),
        )],
        "automations",
        "automator",
        &[("", &durable("digest", "brenn:automations.digest"))],
    )
    .unwrap_or_else(|errors| panic!("{:?}", messages(&errors)));
    assert_eq!(
        config.agents[0].subs[0].chan,
        RChanRef::Decl(channel_at(&config, "automations.digest"))
    );
}

/// And within one authority root the one-spelling rule still holds for that
/// same position: the deployment names its own channel by its handle.
#[test]
fn a_deployment_agent_naming_a_deployment_channel_by_address_is_refused() {
    let errors = compile_with_mount(
        &[(
            "",
            &format!(
                "{CEILING}{}{}",
                durable("inbox", "brenn:root.inbox"),
                assistant("\"brenn:root.inbox\"")
            ),
        )],
        "automations",
        "automator",
        &[("", &durable("digest", "brenn:automations.digest"))],
    )
    .expect_err("where a channel exists, it is named");
    assert_eq!(
        messages(&errors),
        [
            "`brenn:root.inbox` is the address channel `inbox` declares; name the channel, \
          not its address"
        ]
    );
}

/// A fragment writing a `new` against a deployment agent class reaches no
/// class: handles do not cross an authority root, and an agent class is
/// declarable only in a deployment document. So the agent arm of the
/// emitted-kind rule is a backstop with no path to it today, and this is what
/// holds the door it guards shut.
#[test]
fn a_fragment_reaches_no_agent_class_to_instantiate() {
    let errors = compile_with_mount(
        &[("", &format!("{CEILING}{ASSISTANT}"))],
        "automations",
        "automator",
        &[("", "new alice: Assistant();\n")],
    )
    .expect_err("a fragment cannot name the deployment's agent class");
    assert!(
        messages(&errors)
            .iter()
            .any(|message| message.contains("Assistant")),
        "{:?}",
        messages(&errors)
    );
}

// ── a fragment's files never leave its own tree ──────────────────────────────
//
// Every byte under `config/` is the mount author's, symbolic links included,
// and a clone lays them down as they were committed. The grammar bounds a tree
// *key* and says nothing about the inode a key resolves to, so following a link
// out of the tree would let an author aim the compiler at the operator's own
// document or at a secret file and read the answer off `brenn:config.status`.

/// One document with a mounted root, compiled.
fn compiled(root: &std::path::Path, config_dir: &std::path::Path) -> Vec<Diagnostic> {
    compile(&DocumentInputs {
        root: root.to_path_buf(),
        module_roots: Vec::new().into(),
        mounted: vec![support::mounted_root(
            "automations",
            "automator",
            config_dir,
        )],
        role: DocumentRole::Deployment,
    })
    .err()
    .unwrap_or_default()
}

/// An entry document that is a link to a file outside the tree is not a module:
/// the refusal names the path as written and never what it resolved to.
#[test]
#[cfg(unix)]
fn a_linked_entry_document_is_refused() {
    let dir = support::scratch("mounted-linked-entry");
    std::fs::write(dir.join("main.brenn"), CEILING).expect("the root writes");
    let secret = dir.join("secret.brenn");
    std::fs::write(&secret, "token = hunter2\n").expect("the secret writes");
    let config_dir = dir.join("automations/config");
    std::fs::create_dir_all(&config_dir).expect("the config tree writes");
    let entry = config_dir.join("main.brenn");
    std::os::unix::fs::symlink(&secret, &entry).expect("the link writes");

    let errors = compiled(&dir.join("main.brenn"), &config_dir);
    assert_eq!(
        messages(&errors),
        [format!(
            "`{}` leaves the config tree of mount `automations`: a mount's config is read \
             only from the directory the mounts document declares, so a link out of it is \
             not a module",
            entry.display()
        )]
    );
    let rendered = errors[0].render();
    assert!(
        !rendered.contains("hunter2") && !rendered.contains("secret.brenn"),
        "the refusal names what the link resolved to: {rendered}"
    );
}

/// And a tree module reached by `use`: the same rule, positioned at the `use`
/// that named it.
#[test]
#[cfg(unix)]
fn a_linked_tree_module_is_refused() {
    let dir = support::scratch("mounted-linked-module");
    std::fs::write(dir.join("main.brenn"), CEILING).expect("the root writes");
    let outside = dir.join("outside.brenn");
    std::fs::write(&outside, durable("stray", "brenn:automations.stray"))
        .expect("the outside module writes");
    let config_dir = dir.join("automations/config");
    std::fs::create_dir_all(&config_dir).expect("the config tree writes");
    std::fs::write(config_dir.join("main.brenn"), "use helpers::*;\n").expect("the entry writes");
    std::os::unix::fs::symlink(&outside, config_dir.join("helpers.brenn"))
        .expect("the link writes");

    let errors = compiled(&dir.join("main.brenn"), &config_dir);
    assert_eq!(
        messages(&errors),
        [format!(
            "`{}` leaves the config tree of mount `automations`: a mount's config is read \
             only from the directory the mounts document declares, so a link out of it is \
             not a module",
            config_dir.join("helpers.brenn").display()
        )]
    );
}

/// A `config/` tree that is itself a link is refused whole: with the root a
/// link, "inside the tree" would mean inside whatever it points at, and every
/// file under it would pass the containment rule while being the operator's.
#[test]
#[cfg(unix)]
fn a_linked_config_tree_is_refused() {
    let dir = support::scratch("mounted-linked-tree");
    std::fs::write(dir.join("main.brenn"), CEILING).expect("the root writes");
    let elsewhere = dir.join("elsewhere");
    std::fs::create_dir_all(&elsewhere).expect("the other tree writes");
    std::fs::write(elsewhere.join("main.brenn"), "").expect("its entry writes");
    let mount = dir.join("automations");
    std::fs::create_dir_all(&mount).expect("the mount writes");
    let config_dir = mount.join("config");
    std::os::unix::fs::symlink(&elsewhere, &config_dir).expect("the link writes");

    let errors = compiled(&dir.join("main.brenn"), &config_dir);
    assert_eq!(
        messages(&errors),
        [format!(
            "`automations`: `{}` is not a directory of its own; a mount's config tree is \
             read as the directory it is, so that nothing under it can name a file outside it",
            config_dir.display()
        )]
    );
}

/// A link *within* the tree is the author's own arrangement and is read: what
/// the rule is about is leaving, not linking.
#[test]
#[cfg(unix)]
fn a_link_inside_the_tree_is_read() {
    let dir = support::scratch("mounted-inside-link");
    std::fs::write(dir.join("main.brenn"), CEILING).expect("the root writes");
    let config_dir = dir.join("automations/config");
    std::fs::create_dir_all(&config_dir).expect("the config tree writes");
    std::fs::write(
        config_dir.join("real.brenn"),
        durable("digest", "brenn:automations.digest"),
    )
    .expect("the real module writes");
    std::fs::write(config_dir.join("main.brenn"), "use helpers::*;\n").expect("the entry writes");
    std::os::unix::fs::symlink(
        config_dir.join("real.brenn"),
        config_dir.join("helpers.brenn"),
    )
    .expect("the link writes");

    assert_eq!(
        messages(&compiled(&dir.join("main.brenn"), &config_dir)),
        Vec::<&str>::new()
    );
}

/// The refusal for a link out of the tree does not depend on whether the link's
/// target is there. If it did, one `use` per path would answer "does this file
/// exist on the host" for anything the server's user can traverse, on the
/// channel the mount's own author reads.
#[test]
#[cfg(unix)]
fn a_linked_entry_document_is_refused_whether_or_not_its_target_exists() {
    let dir = support::scratch("mounted-linked-entry-oracle");
    std::fs::write(dir.join("main.brenn"), CEILING).expect("the root writes");
    let secret = dir.join("secret.brenn");
    std::fs::write(&secret, "token = hunter2\n").expect("the secret writes");
    let config_dir = dir.join("automations/config");
    std::fs::create_dir_all(&config_dir).expect("the config tree writes");
    std::os::unix::fs::symlink(&secret, config_dir.join("main.brenn")).expect("the link writes");

    let present = rendered(&compiled(&dir.join("main.brenn"), &config_dir));
    std::fs::remove_file(&secret).expect("the secret is removed");
    let absent = rendered(&compiled(&dir.join("main.brenn"), &config_dir));

    assert_eq!(present, absent, "the refusal reports on the link's target");
}

/// Same property for the `use`-based lookup path, which a fragment can probe
/// one path at a time.
#[test]
#[cfg(unix)]
fn a_linked_tree_module_is_refused_whether_or_not_its_target_exists() {
    let dir = support::scratch("mounted-linked-module-oracle");
    std::fs::write(dir.join("main.brenn"), CEILING).expect("the root writes");
    let outside = dir.join("outside.brenn");
    std::fs::write(&outside, durable("stray", "brenn:automations.stray"))
        .expect("the outside module writes");
    let config_dir = dir.join("automations/config");
    std::fs::create_dir_all(&config_dir).expect("the config tree writes");
    std::fs::write(config_dir.join("main.brenn"), "use helpers::*;\n").expect("the entry writes");
    std::os::unix::fs::symlink(&outside, config_dir.join("helpers.brenn"))
        .expect("the link writes");

    let present = rendered(&compiled(&dir.join("main.brenn"), &config_dir));
    std::fs::remove_file(&outside).expect("the outside module is removed");
    let absent = rendered(&compiled(&dir.join("main.brenn"), &config_dir));

    assert_eq!(present, absent, "the refusal reports on the link's target");
}

/// Every diagnostic's full text, which is what reaches the mount author.
#[cfg(unix)]
fn rendered(errors: &[Diagnostic]) -> Vec<String> {
    errors.iter().map(|error| error.render()).collect()
}

// ── the `surfaces` axis over a mount ─────────────────────────────────────────
//
// A mount's contributions land where its principal's `surfaces` are written and
// nowhere else. The axis is the operator's line in the deployment's own text;
// the fragment names the surface by slug, which is the contract across the
// boundary.

/// A packaged module holding the one class the placement fixtures instantiate.
const PANEL: &str = "component Tile { abi = processor; requires = [dom]; in messages; }\n";

/// A packaged assembly whose body is a contribution placing a shell, for the
/// case where the block reaching a fragment is one the fragment stamps rather
/// than one it wrote.
const SHELL_PLACER: &str = "\
assembly PlaceShell(slug: String, feed: Channel) {
    extend surface f\"{slug}\" {
        new extra: Tile { chrome = true; grants = [dom]; in messages <- feed; }
    }
}
";

/// A deployment declaring one surface of its own and a ceiling with the
/// `surfaces` line a case writes.
///
/// The surface is written in the root's own text rather than stamped from an
/// assembly, so the only recorded stamp in the document is the mount's and the
/// sole fit refusal is the one a case is about.
fn placement_root(axis: &str) -> String {
    placement_root_with(axis, "chrome = true; ")
}

/// The same, with `demo`'s body chrome written by the caller: the one test that
/// puts the chrome in a contribution needs the body to hold none.
fn placement_root_with(axis: &str, demo_chrome: &str) -> String {
    format!(
        "use @panel::*;\n\n\
         channel base at \"ephemeral:base.msgs\" {{ push_depth = 1; retain_depth = 1; }}\n\n\
         surface demo {{\n  grants = [subscribe];\n  \
         new host: Tile {{ {demo_chrome}grants = [dom]; in messages <- base; }}\n}}\n\n\
         surface aux {{\n  grants = [subscribe];\n  \
         new host: Tile {{ chrome = true; grants = [dom]; in messages <- base; }}\n}}\n\n\
         principal automator {{\n  grants = [dom];\n{axis}  \
         acl subscribe [prefix \"brenn:automations.\"];\n}}\n"
    )
}

/// A fragment declaring its own channel and contributing one panel bound to it,
/// with any extra keys on the contributed instance.
fn placement_fragment(slug: &str, keys: &str) -> String {
    format!(
        "use @panel::*;\n\n{}\nextend surface \"{slug}\" {{\n  \
         new extra: Tile {{ grants = [dom]; {keys}in messages <- digest; }}\n}}\n",
        durable("digest", "brenn:automations.digest")
    )
}

/// Derive the placement root beside a fragment, expecting refusals.
fn placement_refusals(axis: &str, fragment: &str) -> Vec<Diagnostic> {
    derive_with_mount(
        &[("", &placement_root(axis)), ("@panel", PANEL)],
        "automations",
        "automator",
        &[("", fragment)],
    )
    .expect_err("the fragment was expected not to derive")
}

#[test]
fn a_mount_places_on_a_surface_its_principal_names() {
    derive_with_mount(
        &[
            ("", &placement_root("  surfaces = [\"demo\"];\n")),
            ("@panel", PANEL),
        ],
        "automations",
        "automator",
        &[("", &placement_fragment("demo", ""))],
    )
    .unwrap_or_else(|errors| panic!("{:?}", messages(&errors)));
}

/// A mount under a principal with no `surfaces` line places on no page, which
/// is exactly what a config-carrying mount could do before the axis existed.
#[test]
fn a_mount_under_a_principal_with_no_surfaces_line_places_on_nothing() {
    let refusals = placement_refusals("", &placement_fragment("demo", ""));
    assert_eq!(
        messages(&refusals),
        [
            "the config of mount `automations` places components on surface `demo`, and this \
             stamp's ceiling places on no such surface: a mount's contributions land where \
             its principal's `surfaces` are written, so add `demo` to `automator`"
        ]
    );
}

/// The axis is per slug: a principal that names one surface does not thereby
/// name the deployment's others.
#[test]
fn a_mount_places_on_no_surface_its_principal_does_not_name() {
    let refusals = placement_refusals("  surfaces = [\"demo\"];\n", &placement_fragment("aux", ""));
    let expected = "the config of mount `automations` places components on surface `aux`, and \
                    this stamp's ceiling places on no such surface";
    assert!(
        messages(&refusals)
            .iter()
            .any(|message| message.starts_with(expected)),
        "{:?}",
        messages(&refusals)
    );
}

/// The chrome is the surface's shell and stays in the deployment's text. The
/// refusal is at the attr, not the block: a contribution from a mount is legal
/// and only this key inside one is not.
#[test]
fn a_fragment_may_not_place_the_chrome() {
    let errors = compile_with_mount(
        &[
            ("", &placement_root("  surfaces = [\"demo\"];\n")),
            ("@panel", PANEL),
        ],
        "automations",
        "automator",
        &[("", &placement_fragment("demo", "chrome = true; "))],
    )
    .expect_err("a mount does not write the chrome");
    assert_eq!(
        messages(&errors),
        [
            "the config of mount `automations` places `extra` as chrome; the chrome is the \
             surface's shell, and a mount's contribution places panels",
            // Both are true of the one key: the mount may not write it, and the
            // surface it wrote it on already holds the shell the deployment gave
            // it. The second is about the merged surface, which is a shape the
            // authority refusal says nothing about.
            "surface `demo` holds a second chrome; every surface holds exactly one",
        ]
    );
}

/// The same rule through a packaged assembly the fragment stamps. The stamp
/// the refusal is attributed to is the assembly's, not the mount's — the
/// innermost recorded one — so the sentence names it as an arrangement rather
/// than telling the author a mount by that name exists.
#[test]
fn a_fragment_may_not_stamp_an_assembly_that_places_the_chrome() {
    let errors = compile_with_mount(
        &[
            ("", &placement_root("  surfaces = [\"demo\"];\n")),
            ("@panel", &format!("{PANEL}{SHELL_PLACER}")),
        ],
        "automations",
        "automator",
        &[(
            "",
            &format!(
                "use @panel::*;\n\n{}\nnew placer: PlaceShell(slug = \"demo\", feed = digest);\n",
                durable("digest", "brenn:automations.digest")
            ),
        )],
    )
    .expect_err("a mount does not write the chrome, however it is stamped");
    assert_eq!(
        messages(&errors),
        [
            "`automations.placer`, stamping `PlaceShell`, places `extra` as chrome; the \
             chrome is the surface's shell, and a mount's contribution places panels",
            "surface `demo` holds a second chrome; every surface holds exactly one",
        ]
    );
}

/// The same key written by the deployment is not refused: what the rule is
/// about is the authority root the block was expanded in.
#[test]
fn the_deployment_writes_the_chrome_in_a_contribution_of_its_own() {
    let root = format!(
        "{}\nextend surface \"demo\" {{\n  \
         new shell: Tile {{ grants = [dom]; chrome = true; in messages <- base; }}\n}}\n",
        placement_root_with("  surfaces = [\"demo\"];\n", "")
    );
    let config = compile_with_mount(
        &[("", &root), ("@panel", PANEL)],
        "automations",
        "automator",
        &[("", &placement_fragment("demo", ""))],
    )
    .unwrap_or_else(|errors| panic!("{:?}", messages(&errors)));
    assert_eq!(config.surfaces[0].components.len(), 3);
    assert_eq!(config.surfaces[0].slug.value(), "demo");
}

// ── what a contribution's bindings are held to ───────────────────────────────
//
// Two caps, both the operator's: the surface's own explicit ACL, which
// suppresses derivation and must
// cover every binding on the merged surface; and the ceiling on the stamp the
// surface itself was expanded inside, which a contributed binding confers on
// through the surface's subject.

/// A deployment whose one surface writes an explicit `acl subscribe` over its
/// own `in.` prefix — prod's `Deskbar` shape — beside a ceiling naming it.
///
/// The principal's reach carries the same prefix, so the fragment may declare a
/// channel there: what a case is about is the surface's ACL, and a reach
/// refusal would answer first.
const WALLED_ROOT: &str = "use @panel::*;\n\n\
                           channel home at \"brenn:demo.in.home\" { push_depth = 1; \
                           retain_depth = 1; standing_retain_depth = 1; }\n\n\
                           surface demo {\n  grants = [subscribe];\n  \
                           acl subscribe [prefix \"brenn:demo.in.\"];\n  \
                           new host: Tile { chrome = true; grants = [dom]; \
                           in messages <- home; }\n}\n\n\
                           principal automator {\n  grants = [dom];\n  \
                           surfaces = [\"demo\"];\n  \
                           acl subscribe [prefix \"brenn:automations.\", \
                           prefix \"brenn:demo.in.\"];\n}\n";

/// A fragment contributing one panel bound to a channel of its own at `address`.
fn walled_fragment(address: &str) -> String {
    format!(
        "use @panel::*;\n\n{}\nextend surface \"demo\" {{\n  \
         new extra: Tile {{ grants = [dom]; in messages <- digest; }}\n}}\n",
        durable("digest", address),
    )
}

/// The surface's explicit ACL is a second cap on top of the ceiling: a
/// contributed binding the operator's list does not cover is refused at the
/// binding, by the same coverage rule that holds for a body instance.
#[test]
fn an_explicit_surface_acl_caps_a_contributed_binding() {
    let errors = derive_with_mount(
        &[("", WALLED_ROOT), ("@panel", PANEL)],
        "automations",
        "automator",
        &[("", &walled_fragment("brenn:automations.digest"))],
    )
    .expect_err("the binding is outside the surface's own ACL");
    let messages = messages(&errors);
    assert_eq!(messages.len(), 1, "{messages:?}");
    assert!(
        messages[0].contains("automations.digest"),
        "the refusal names the binding's channel: {messages:?}"
    );
}

/// The same fragment with its channel under the surface's prefix compiles: the
/// operator's `acl` and the operator's `surfaces` line between them say where a
/// mount's panels may read, and the fragment writes inside both.
#[test]
fn a_contributed_binding_under_the_surfaces_acl_is_accepted() {
    derive_with_mount(
        &[("", WALLED_ROOT), ("@panel", PANEL)],
        "automations",
        "automator",
        &[("", &walled_fragment("brenn:demo.in.digest"))],
    )
    .unwrap_or_else(|errors| panic!("{:?}", messages(&errors)));
}

/// Where the surface derives its ACL instead, the derivation reads the merged
/// component set: the `surface:<slug>` principal's reach grows to cover the
/// fragment's binding. That widening is bounded by the ceiling, which every
/// contributed binding is fit-checked against.
#[test]
fn a_derived_surface_acl_widens_with_its_contributions() {
    let config = derive_with_mount(
        &[
            ("", &placement_root("  surfaces = [\"demo\"];\n")),
            ("@panel", PANEL),
        ],
        "automations",
        "automator",
        &[("", &placement_fragment("demo", ""))],
    )
    .unwrap_or_else(|errors| panic!("{:?}", messages(&errors)));
    // `demo` is the root's first surface; the derived list is positional with
    // the resolved one.
    let patterns: Vec<&str> = config.surfaces[0]
        .acl
        .brenn_subscribe
        .iter()
        .map(brenn_dsl::derived::DMatcher::pattern)
        .collect();
    assert_eq!(
        patterns,
        ["automations.digest"],
        "the contribution's channel is in the surface's derived reach"
    );
}

// ── the ceiling on the stamp the surface itself was declared in ──────────────

/// A deployment whose surface is stamped from an assembly `under p`, with the
/// mount's ceiling reaching `reach` and the operator's stamp ceiling reaching
/// `stamp_reach`.
///
/// The surface's own subject carries the merged bounds, so a contributed
/// binding is fit-checked against `p` as well as against the mount's principal.
fn stamped_surface_root(stamp_reach: &str) -> String {
    format!(
        "use @panel::*;\n\n\
         channel base at \"ephemeral:base.msgs\" {{ push_depth = 1; retain_depth = 1; }}\n\n\
         assembly Page(slug: String) {{\n  surface page {{\n    slug = slug;\n    \
         grants = [subscribe];\n    \
         new host: Tile {{ chrome = true; grants = [dom]; in messages <- base; }}\n  }}\n}}\n\n\
         principal holder {{\n  grants = [dom, subscribe];\n  \
         acl subscribe [prefix \"ephemeral:base.\", {stamp_reach}];\n}}\n\n\
         new demo: Page(slug = \"demo\") under holder;\n\n\
         principal automator {{\n  grants = [dom];\n  surfaces = [\"demo\"];\n  \
         acl subscribe [prefix \"brenn:automations.\"];\n}}\n"
    )
}

/// The operator ceilinged the assembly that declares the surface, and the
/// mount's ceiling promises more than that ceiling holds. Both are the
/// operator's lines and the two have to be reconciled; better refused than a
/// `surface:demo` principal running wider than the stamp that declared it.
#[test]
fn a_contribution_fits_under_the_stamp_the_surface_was_declared_in() {
    let errors = derive_with_mount(
        &[
            ("", &stamped_surface_root("prefix \"brenn:other.\"")),
            ("@panel", PANEL),
        ],
        "automations",
        "automator",
        &[("", &placement_fragment("demo", ""))],
    )
    .expect_err("the contributed binding is outside the surface stamp's ceiling");
    let messages = messages(&errors);
    assert_eq!(messages.len(), 1, "{messages:?}");
    assert!(
        messages[0].contains("automations.digest") && messages[0].contains("holder"),
        "the refusal names the binding and the operator's principal: {messages:?}"
    );
}

/// The same document with the stamp's ceiling covering the fragment's channel:
/// both ceilings hold, so the contribution lands.
#[test]
fn a_contribution_under_both_ceilings_is_accepted() {
    derive_with_mount(
        &[
            ("", &stamped_surface_root("prefix \"brenn:automations.\"")),
            ("@panel", PANEL),
        ],
        "automations",
        "automator",
        &[("", &placement_fragment("demo", ""))],
    )
    .unwrap_or_else(|errors| panic!("{:?}", messages(&errors)));
}

// ── a contributed binding is the contributor's, across the boundary ──────────

/// Two classes whose `messages` port carries a document type, so two bindings
/// of one channel from two authority roots can be made to agree or to disagree.
const TAGGED_PANEL: &str = "component Feeder { abi = processor; requires = [dom]; \
                            in messages: \"brenn.feed@1\"; }\n\
                            component Staler { abi = processor; requires = [dom]; \
                            in messages: \"brenn.stale@1\"; }\n";

/// A deployment whose surface's body binds `home` through a class carrying the
/// feed document type, with the ceiling naming the surface.
fn tagged_root() -> String {
    "use @tagged::*;\n\n\
     channel home at \"brenn:demo.in.home\" { push_depth = 1; retain_depth = 1; \
     standing_retain_depth = 1; }\n\n\
     surface demo {\n  grants = [subscribe];\n  \
     acl subscribe [prefix \"brenn:demo.in.\"];\n  \
     new host: Feeder { chrome = true; grants = [dom]; in messages <- home; }\n}\n\n\
     principal automator {\n  grants = [dom];\n  surfaces = [\"demo\"];\n  \
     acl subscribe [prefix \"brenn:demo.in.\"];\n}\n"
        .to_string()
}

/// A fragment contributing one instance of `class`, bound to the deployment's
/// channel by the one spelling it has across the boundary: the address.
fn tagged_fragment(class: &str) -> String {
    format!(
        "use @tagged::*;\n\nextend surface \"demo\" {{\n  \
         new extra: {class} {{ grants = [dom]; in messages <- \"brenn:demo.in.home\"; }}\n}}\n"
    )
}

/// A contributed binding's literal resolves to the deployment's declaration,
/// which is what makes the slug-and-address contract usable at all: a fragment
/// cannot spell the root's handle, so the address is the only spelling it has.
///
/// The rewrite reads the *component's* own stamp, not the surface's — after the
/// merge one surface holds components from more than one root.
#[test]
fn a_contributed_binding_names_a_root_channel_by_address() {
    let config = compile_with_mount(
        &[("", &tagged_root()), ("@tagged", TAGGED_PANEL)],
        "automations",
        "automator",
        &[("", &tagged_fragment("Feeder"))],
    )
    .unwrap_or_else(|errors| panic!("{:?}", messages(&errors)));
    let [host, extra] = &config.surfaces[0].components[..] else {
        panic!("the body's instance and the contribution's");
    };
    assert_eq!(host.bindings[0].chan, extra.bindings[0].chan);
    assert_eq!(
        extra.bindings[0].chan,
        Some(RChanRef::Decl(channel_at(&config, "home"))),
        "the contribution's literal is the root's declaration"
    );
}

/// One channel carries one document, whichever root's text bound it: the body's
/// instance and the contribution's agree here.
#[test]
fn a_contributed_binding_agrees_on_the_channels_doctype() {
    derive_with_mount(
        &[("", &tagged_root()), ("@tagged", TAGGED_PANEL)],
        "automations",
        "automator",
        &[("", &tagged_fragment("Feeder"))],
    )
    .unwrap_or_else(|errors| panic!("{:?}", messages(&errors)));
}

/// And disagreeing, they are refused. Both claims are named, each at the file
/// its class was declared in — which for a fragment's placement is the packaged
/// module, since a fragment declares no class of its own.
#[test]
fn a_contributed_binding_conflicting_on_the_doctype_is_refused() {
    let errors = derive_with_mount(
        &[("", &tagged_root()), ("@tagged", TAGGED_PANEL)],
        "automations",
        "automator",
        &[("", &tagged_fragment("Staler"))],
    )
    .expect_err("two document types on one channel");
    let conflict = errors
        .iter()
        .find(|error| error.message.contains("different document types"))
        .unwrap_or_else(|| panic!("{:?}", messages(&errors)));
    assert!(
        conflict.message.contains("`home` (`brenn:demo.in.home`)"),
        "{}",
        conflict.message
    );
    assert_eq!(conflict.related.len(), 2, "{}", conflict.message);
}

// ── a kind is described by the document that ships it ────────────────────────
//
// The two description channels a kind carries are an ordinary declaration, and
// nothing downstream cares which authority root wrote it: the validator
// resolves the derived address through the directory and stops there. So a
// bundle that ships a kind describes it in its own fragment, and the operator's
// part is one standing prefix in the mount's reach, written once.

/// The library module the description vocabulary lives in, as the deployment's
/// release mount ships it.
const DESCRIPTION: &str = "assembly KindDescription(kind: String) {\n  \
                           channel help at f\"brenn:surface.kind.{kind}.help\" {\n    \
                           push_depth = 1; retain_depth = 1; standing_retain_depth = 1;\n  }\n  \
                           channel schema at f\"brenn:surface.kind.{kind}.schema\" {\n    \
                           push_depth = 1; retain_depth = 1; standing_retain_depth = 1;\n  }\n}\n";

/// The deployment: the surface the fragment places on, and a ceiling whose
/// reach carries `reach` beside the surface's own prefix.
fn describing_root(reach: &str) -> String {
    format!(
        "use @panel::*;\n\n\
         channel home at \"brenn:demo.in.home\" {{ push_depth = 1; retain_depth = 1; \
         standing_retain_depth = 1; }}\n\n\
         surface demo {{\n  grants = [subscribe];\n  \
         new host: Tile {{ chrome = true; grants = [dom]; in messages <- home; }}\n}}\n\n\
         principal automator {{\n  grants = [dom];\n  surfaces = [\"demo\"];\n  \
         acl subscribe [prefix \"brenn:demo.in.\"{reach}];\n}}\n"
    )
}

/// The fragment: it places its bundle's kind and describes it in the same text.
const DESCRIBING_FRAGMENT: &str = "use @panel::*;\nuse @describe::*;\n\n\
                                   new tile_desc: KindDescription(kind = \"tile\");\n\n\
                                   extend surface \"demo\" {\n  \
                                   new extra: Tile { grants = [dom]; \
                                   in messages <- \"brenn:demo.in.home\"; }\n}\n";

/// The v5 identity an unpinned `brenn:` address derives to, which is what a
/// fragment's description channels get without writing a pin of their own.
fn address_uuid(address: &str) -> uuid::Uuid {
    let namespace = uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_DNS, b"brenn.dsl-channel");
    uuid::Uuid::new_v5(&namespace, address.as_bytes())
}

/// The whole rule in one document: the fragment declares its own kind's
/// two description channels, under a principal reaching the family, and the
/// channels resolve with the address-derived identity.
#[test]
fn a_fragment_describes_the_kind_its_bundle_ships() {
    let config = derive_with_mount(
        &[
            ("", &describing_root(", prefix \"brenn:surface.kind.\"")),
            ("@panel", PANEL),
            ("@describe", DESCRIPTION),
        ],
        "automations",
        "automator",
        &[("", DESCRIBING_FRAGMENT)],
    )
    .unwrap_or_else(|errors| panic!("{:?}", messages(&errors)));
    for (handle, address) in [
        ("automations.tile_desc.help", "brenn:surface.kind.tile.help"),
        (
            "automations.tile_desc.schema",
            "brenn:surface.kind.tile.schema",
        ),
    ] {
        let id = channel_at(&config.resolved, handle);
        assert_eq!(
            config.resolved.channels[id.0].address.value(),
            address,
            "`{handle}` is declared at its derived address"
        );
        let stamp = config.resolved.channels[id.0]
            .stamp
            .expect("a fragment's channel carries a stamp");
        assert_eq!(
            config.resolved.stamps[stamp.0].parent,
            Some(StampId(0)),
            "`{handle}` is stamped inside the mount's own stamp"
        );
        assert_eq!(
            config.channel_uuids[id.0],
            Some(address_uuid(address)),
            "`{handle}` takes the address-derived identity, so no pin is needed"
        );
    }
}

/// The one constraint is the ceiling: the addresses are ordinary channels, and
/// a fragment reaching neither of them is refused at its stamp, naming the
/// principal whose line the operator has to widen.
#[test]
fn a_fragment_describing_a_kind_its_ceiling_does_not_reach_is_refused() {
    let errors = derive_with_mount(
        &[
            ("", &describing_root("")),
            ("@panel", PANEL),
            ("@describe", DESCRIPTION),
        ],
        "automations",
        "automator",
        &[("", DESCRIBING_FRAGMENT)],
    )
    .expect_err("the description family is outside the mount's reach");
    let messages = messages(&errors);
    assert!(
        messages
            .iter()
            .any(|message| message.contains("brenn:surface.kind.tile.help")
                && message.contains("automator")),
        "{messages:?}"
    );
}

/// Both roots describing one kind is an address collision, refused at compile
/// with both sites named — which is the half of the rule that makes it
/// self-enforcing without anything knowing whose kind it is.
#[test]
fn the_root_and_a_fragment_describing_one_kind_collide() {
    let root = format!(
        "use @describe::*;\n{}\nnew tile_desc: KindDescription(kind = \"tile\");\n",
        describing_root(", prefix \"brenn:surface.kind.\""),
    );
    let errors = compile_with_mount(
        &[("", &root), ("@panel", PANEL), ("@describe", DESCRIPTION)],
        "automations",
        "automator",
        &[("", DESCRIBING_FRAGMENT)],
    )
    .expect_err("one address, two declarations");
    let collision = errors
        .iter()
        .find(|error| error.message.contains("brenn:surface.kind.tile.help"))
        .unwrap_or_else(|| panic!("{:?}", messages(&errors)));
    assert!(
        collision
            .message
            .contains("two channels declare the address"),
        "{}",
        collision.message
    );
    assert_eq!(collision.related.len(), 1, "the prior declaration is cited");
}
