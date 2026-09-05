//! Loading, starting and stopping one `[[wasm_consumer]]`.
//!
//! Boot walks the resolved consumers once, loading each and then starting each,
//! and holds what came back in a [`ConsumerRegistry`]. The two halves are
//! separate because they answer to different failures: a load reads the
//! components roots and the host's state directory and may refuse, while a
//! start is wiring that cannot fail once the component is in hand.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use brenn_lib::messaging::config::ResolvedWasmConsumer;
use brenn_lib::wasm_package::Verified;
use brenn_messaging::Messenger;
use brenn_obs::alerting::AlertDispatcher;
use tokio::sync::Notify;
use tracing::info;

/// The environment-free half of a `[[wasm_consumer]]`'s `ProcessorLoadSpec`:
/// everything derivable from the resolved consumer alone. The remaining fields
/// (alerter, store path, MQTT egress callback, tool host) depend on process-wide
/// services and stay at the call site.
pub(crate) struct ConsumerLoadParts {
    pub output_ports: std::collections::HashMap<String, brenn_wasm::OutputPortSpec>,
    pub declared_out_ports: std::collections::BTreeSet<String>,
    pub input_amplification_mt: std::collections::HashMap<String, u64>,
    pub mqtt_sinks: std::collections::HashMap<String, brenn_wasm::SinkBudget>,
    pub grants: std::collections::BTreeSet<brenn_wasm::ComponentGrant>,
    pub output_acl: brenn_wasm::OutputAclFn,
}

/// Lower a resolved consumer to the load-spec fields it fully determines.
pub(crate) fn lower_consumer_load_parts(
    consumer: &brenn_lib::messaging::config::ResolvedWasmConsumer,
) -> ConsumerLoadParts {
    use brenn_lib::messaging::ComponentGrant;
    use brenn_lib::messaging::Urgency;
    use brenn_wasm::ProcessorUrgency;
    use std::collections::{BTreeSet, HashMap};

    let output_ports: HashMap<String, brenn_wasm::OutputPortSpec> = consumer
        .outputs
        .iter()
        .map(|o| {
            let wu = match o.default_urgency {
                Urgency::VeryLow => ProcessorUrgency::VeryLow,
                Urgency::Low => ProcessorUrgency::Low,
                Urgency::Normal => ProcessorUrgency::Normal,
                Urgency::High => ProcessorUrgency::High,
            };
            (
                o.port.clone(),
                brenn_wasm::OutputPortSpec {
                    channel_address: o.channel_address.clone(),
                    default_urgency: wu,
                    budget: brenn_wasm::SinkBudget {
                        fill_mt: o.budget.fill_mt,
                        capacity_mt: o.budget.capacity_mt,
                    },
                },
            )
        })
        .collect();
    // Windows are built from the same `inputs`, so every driven window port
    // is present.
    let input_amplification_mt: HashMap<String, u64> = consumer
        .inputs
        .iter()
        .map(|i| (i.port.clone(), i.amplification_mt))
        .collect();
    let mqtt_sinks: HashMap<String, brenn_wasm::SinkBudget> = consumer
        .mqtt_sinks
        .iter()
        .map(|(client, b)| {
            (
                client.clone(),
                brenn_wasm::SinkBudget {
                    fill_mt: b.fill_mt,
                    capacity_mt: b.capacity_mt,
                },
            )
        })
        .collect();
    // `takeover` names no interface and cannot reach a top-level consumer.
    // Asserted here because a hand-built config reaches this loader without
    // passing through the config front end's refusal.
    let grants: BTreeSet<ComponentGrant> = consumer
        .grants
        .iter()
        .inspect(|g| {
            assert!(
                g.wit_import().is_some(),
                "consumer {:?}: granted `{}`, which names no WIT interface and belongs to a \
                 page — a top-level consumer has no page, and the config front end refuses \
                 the word",
                consumer.slug,
                g.word(),
            )
        })
        .copied()
        .collect();
    // The word and the statements it consents to are one configuration, refused
    // at derive in either direction. Asserted again here because a hand-built
    // config reaches this loader without passing through that refusal.
    assert_eq!(
        grants.contains(&ComponentGrant::Tools),
        !consumer.policy.tool_grants.is_empty(),
        "consumer {:?}: `tools` is granted iff the consumer names a tool — a grant with \
         no tool reaches nothing, and a tool with no grant is authority nobody gave",
        consumer.slug,
    );
    // `brenn-wasm` never sees a brenn-lib type; this closure bridges the two.
    let policy = consumer.policy.clone();
    let output_acl: brenn_wasm::OutputAclFn = std::sync::Arc::new(move |addr: &str| {
        match brenn_lib::messaging::ChannelScheme::split(addr) {
            Some((scheme, name)) => {
                brenn_lib::messaging::gates::publish_acl_allows(&policy, scheme, name)
            }
            None => false,
        }
    });

    ConsumerLoadParts {
        output_ports,
        declared_out_ports: consumer.declared_out_ports.clone(),
        input_amplification_mt,
        mqtt_sinks,
        grants,
        output_acl,
    }
}

/// Resolve a consumer's package, verify it, then load what it names.
///
/// Same ordering rule as [`crate::load_verified_replay`], and one function for the
/// same reason: the resolution is the only source of an artifact path, so a
/// component whose package does not bind it cannot reach the loader. The
/// caller assembles the dozen wired fields of the load spec but never its
/// `component_path` — whatever it staged there is discarded for the artifact
/// the package bound.
fn load_verified_consumer(
    components_roots: &[PathBuf],
    package: &str,
    slug: &str,
    config_spec_sha256: &str,
    record: Option<brenn_lib::wasm_package::Verified>,
    spec: brenn_wasm::ProcessorLoadSpec<'_>,
) -> (
    brenn_wasm::ProcessorComponent,
    brenn_lib::wasm_package::Verified,
) {
    // A caller that has already read the record hands it over rather than
    // letting a second reading answer: re-hashing the artifact would both cost
    // the hash twice and admit a bundle swapped between the two readings, so
    // that what a caller decided on and what it loads could differ.
    let verified = match record {
        Some(verified) => verified,
        None => {
            let roots = brenn_lib::wasm_package::require_components_root(
                components_roots,
                &format!("consumer {slug:?}"),
            );
            brenn_lib::wasm_package::verify_consumer(roots, package, slug, config_spec_sha256)
        }
    };
    // The spec's borrows outlive this call; narrowing them to the artifact's
    // scope is what lets the path the verification produced be the one loaded.
    let mut spec: brenn_wasm::ProcessorLoadSpec<'_> = spec;
    spec.component_path = &verified.artifact;
    let component = brenn_wasm::ProcessorComponent::load(spec);
    (component, verified)
}

/// Assert that the directory a consumer's KV store would be created in exists
/// on this host.
///
/// A fact about the deployment target, not about the document: the same
/// `store_path` is right on the host whose state directory holds it and wrong on
/// a workstation that has never had one. So it is asked here, beside the load
/// that opens the store, rather than in consumer resolution — which runs under
/// `config-check` on machines that are not the target.
///
/// # Panics
///
/// When the parent directory is absent. A path with no parent component is
/// checked against the current directory, which is what `std::path::absolute`
/// resolved it against.
pub(crate) fn assert_store_parent_exists(slug: &str, store_path: &std::path::Path) {
    let parent = store_path
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."));
    assert!(
        parent.exists(),
        "[[wasm_consumer]] {slug:?}: store_path {:?} — parent directory does not exist",
        store_path,
    );
}
/// The process-wide services a consumer's load spec is wired to.
///
/// One value for the whole walk: every field is a service the process built
/// before any consumer was loaded, and none of them is per-consumer.
pub(crate) struct ConsumerLoadContext<'a> {
    pub components_roots: &'a [PathBuf],
    pub alert_dispatcher: &'a AlertDispatcher,
    /// The MQTT service, when one was started. A consumer holding the `mqtt`
    /// grant gets an egress callback over it; one that does not gets no callback
    /// and no linked interface.
    pub mqtt_service: Option<Arc<brenn_mqtt::MqttService>>,
    pub tool_registry: &'a Arc<brenn_tool_registry::ToolRegistry>,
    /// `[messaging].max_body_bytes`, the ceiling on one published payload.
    pub max_payload_bytes: usize,
}

/// A loaded consumer: the instantiated component, what the package bound, and
/// the `Notify` its dispatch task will park on.
///
/// The `Notify` is minted here rather than at the start because the delivery
/// binding that carries wakes to it is registered before the task exists.
pub(crate) struct LoadedConsumer {
    pub component: Arc<brenn_wasm::ProcessorComponent>,
    pub verified: Verified,
    pub notify: Arc<Notify>,
}

/// Load one resolved consumer: check its store's parent, wire its load spec to
/// the process's services, verify its package, and instantiate it.
///
/// Nothing on disk is written and nothing exclusive is taken: the consumer's KV
/// store is named here and opened at the start ([`start_consumer`]), so a
/// replacement for a consumer that is still running can be loaded before the
/// old instance is retired.
///
/// `record` is what the package bound, for a caller that has already read it;
/// `None` reads it here. One reading either way — the artifact is hashed once
/// and the bytes loaded are the bytes that reading described.
///
/// Every refusal here is an environment fact, and at boot they are all asked
/// *after* the messaging layer has been committed: the channel rows are
/// upserted, the cursors reconciled and primed. So a boot refused by this
/// function has already written to the database — idempotently, and the next
/// boot of either document reconciles it, but a caller that needs the refusal
/// before anything is written has to call this before it commits, not after.
///
/// # Panics
///
/// On anything that makes the consumer unrunnable — an absent store parent, a
/// package the roots do not hold, an artifact its record does not bind, a
/// specification hash that is not the packaged one. A boot that cannot load a
/// declared component must not serve.
pub(crate) fn load_consumer(
    ctx: &ConsumerLoadContext<'_>,
    consumer: &ResolvedWasmConsumer,
    record: Option<brenn_lib::wasm_package::Verified>,
) -> LoadedConsumer {
    if let Some(store_path) = consumer.store_path.as_deref() {
        assert_store_parent_exists(&consumer.slug, store_path);
    }
    let notify = Arc::new(Notify::new());
    let ConsumerLoadParts {
        output_ports,
        declared_out_ports,
        input_amplification_mt,
        mqtt_sinks,
        grants,
        output_acl,
    } = lower_consumer_load_parts(consumer);
    let alerter = Arc::new(brenn_wasm_dispatch::DispatcherAlerter::new(
        ctx.alert_dispatcher
            .clone()
            .with_field("wasm_slug", &consumer.slug),
        consumer.slug.clone(),
    ));
    // Synchronous MQTT egress callback. Built iff the consumer holds the `Mqtt`
    // grant — the `mqtt` interface is linked iff this is `Some`, and
    // `ProcessorComponent::load` re-asserts that invariant.
    let mqtt_publish: Option<brenn_wasm::MqttPublishFn> = if consumer
        .grants
        .contains(&brenn_lib::messaging::ComponentGrant::Mqtt)
    {
        Some(crate::wasm_mqtt::make_wasm_mqtt_publish_fn(
            consumer.policy.clone(),
            consumer.slug.clone(),
            ctx.mqtt_service.clone(),
            ctx.alert_dispatcher.clone(),
        ))
    } else {
        None
    };
    // Real tool host over the shared registry, built iff the consumer holds ≥1
    // tool grant (so `tool_host.is_some()` tracks the `Tools` capability —
    // `ProcessorComponent::load` re-asserts that invariant).
    let tool_host: Option<brenn_wasm::ToolHostFn> = if consumer.policy.tool_grants.is_empty() {
        None
    } else {
        Some(Arc::new(brenn_tool_registry::WasmToolHost::new(
            ctx.tool_registry.clone(),
            consumer.policy.tool_grants.clone(),
            consumer.slug.clone(),
            ctx.alert_dispatcher.clone(),
        )))
    };
    let (component, verified) = load_verified_consumer(
        ctx.components_roots,
        &consumer.package,
        &consumer.slug,
        &consumer.spec_sha256,
        record,
        brenn_wasm::ProcessorLoadSpec {
            // Placeholder; the callee owns this field.
            component_path: std::path::Path::new(""),
            slug: &consumer.slug,
            output_ports,
            declared_out_ports,
            input_amplification_mt,
            mqtt_sinks,
            config: consumer.config.clone(),
            grants,
            store_path: consumer.store_path.as_deref(),
            max_page_count: consumer.max_page_count,
            max_payload_bytes: ctx.max_payload_bytes,
            alerter,
            output_acl,
            mqtt_publish,
            tool_host,
        },
    );
    let store_path_present = consumer.store_path.is_some();
    info!(
        slug = %consumer.slug,
        package = %consumer.package,
        component_path = %verified.artifact.display(),
        root = %verified.root.display(),
        world = %verified.world,
        artifact_sha256 = %verified.artifact_sha256,
        spec_sha256 = verified.spec_sha256.as_deref(),
        store_path_present,
        store_path = consumer.store_path.as_deref().map(|p| p.display().to_string()),
        "WASM processor component loaded"
    );
    LoadedConsumer {
        component: Arc::new(component),
        verified,
        notify,
    }
}

/// One consumer in service: what the package bound it to, the instantiated
/// component, and the handle that ends its task.
///
/// Every field is read by the converger — the binding record to decide whether
/// this consumer is still bound to the bytes the roots hold, the component and
/// the handle to take it out of service. The consumer's *resolved value* is not
/// here: the driver's baseline carries one copy of that, off the plan, and a
/// second copy here would be a second answer to the same question that nothing
/// refreshes. Holding the handle is load-bearing on its own: dropping its stop
/// sender stops the task.
pub(crate) struct RunningConsumer {
    pub verified: Verified,
    pub component: Arc<brenn_wasm::ProcessorComponent>,
    pub handle: brenn_wasm_dispatch::ConsumerHandle,
}

/// Every consumer in service, by slug.
///
/// Deliberately not on `AppState`: nothing outside the holder of this map needs
/// a consumer's component or its stop signal, and a shared registry would be a
/// second answer to "what is running" beside the directory's subscribers.
pub(crate) type ConsumerRegistry = HashMap<String, RunningConsumer>;

/// Start a loaded consumer's dispatch task.
///
/// The task runs the startup sweep before its first wait, so a backlog left by
/// a prior process is drained without waiting for a new wake.
///
/// The KV store is opened here rather than at the load, because the file admits
/// one holder: a replacement for a running consumer is loaded while the old
/// instance still holds its store, and only a start is late enough to be sure
/// the old one is gone.
///
/// # Panics
///
/// If the consumer's store cannot be opened, or is already open — the latter
/// meaning a caller started a second instance without retiring the first.
pub(crate) fn start_consumer(
    loaded: LoadedConsumer,
    consumer: &ResolvedWasmConsumer,
    messenger: &Arc<Messenger>,
    alert_dispatcher: &AlertDispatcher,
) -> RunningConsumer {
    loaded.component.open_store();
    let handle =
        brenn_wasm_dispatch::spawn_wasm_consumer_task(brenn_wasm_dispatch::WasmConsumerConfig {
            slug: consumer.slug.clone(),
            component: loaded.component.clone(),
            notify: loaded.notify,
            messenger: messenger.clone(),
            alert_dispatcher: alert_dispatcher.clone(),
            inputs: consumer.inputs.clone(),
            outputs: consumer.outputs.clone(),
            activation_pacing: consumer.activation_pacing,
        });
    info!(slug = %consumer.slug, "wasm_dispatch: consumer task spawned");
    RunningConsumer {
        verified: loaded.verified,
        component: loaded.component,
        handle,
    }
}

/// A built component fixture's bytes, by artifact file name.
///
/// The staging root is a build-system fact — the `//brenn-wasm:fixture_*`
/// targets wired through `BUILD.bazel` `data` — so it is spelled once, here,
/// and every test that wants a real component's bytes comes through this.
#[cfg(test)]
pub(crate) fn fixture_artifact(file_name: &str) -> Vec<u8> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../brenn-wasm/target/components")
        .join(file_name);
    std::fs::read(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The verify-then-load pair, over a package on disk.
    ///
    /// The unit checks live in `brenn-lib`; what is pinned here is that the boot
    /// path runs them, and runs them *first*. Each case hands the loader an
    /// artifact it would reject on its own terms, so a panic naming the package
    /// is proof the verification happened before the load rather than instead of
    /// it — and a refactor that drops or reorders the call fails here instead of
    /// shipping a host that loads unbound components.
    mod verified_load {
        use std::path::{Path, PathBuf};

        struct NoopAlerter;

        impl brenn_wasm::ProcessorAlerter for NoopAlerter {
            fn alert(&self, _severity: brenn_wasm::GuestAlertSeverity, _title: &str, _body: &str) {}
        }

        /// A components root holding one package directory, with a record
        /// binding whatever `artifact_bytes` and `spec` are given here. The
        /// record is written by hand — not by the emitter — because these cases
        /// need records the emitter refuses to write.
        fn package(root: &Path, name: &str, artifact_bytes: &[u8], spec: Option<&str>) -> PathBuf {
            let dir = root.join(name);
            std::fs::create_dir_all(&dir).expect("create package dir");
            let artifact = dir.join(format!("{name}.wasm"));
            std::fs::write(&artifact, artifact_bytes).expect("write artifact");
            let artifact_sha = brenn_lib::util::sha256_hex(artifact_bytes);
            let record = match spec {
                Some(text) => {
                    std::fs::write(dir.join(format!("{name}.brenn")), text).expect("write spec");
                    format!(
                        "{{\n  \"v\": 2,\n  \"name\": \"{name}\",\n  \"world\": \
                         \"brenn:processor\",\n  \"artifact\": \"{name}.wasm\",\n  \
                         \"artifact_sha256\": \"{artifact_sha}\",\n  \"spec\": \
                         \"{name}.brenn\",\n  \"spec_sha256\": \"{}\"\n}}\n",
                        brenn_lib::util::sha256_hex(text.as_bytes()),
                    )
                }
                None => format!(
                    "{{\n  \"v\": 2,\n  \"name\": \"{name}\",\n  \"world\": \
                     \"brenn:replay\",\n  \"artifact\": \"{name}.wasm\",\n  \
                     \"artifact_sha256\": \"{artifact_sha}\"\n}}\n",
                ),
            };
            std::fs::write(dir.join("package.json"), record).expect("write record");
            artifact
        }

        fn load_spec(slug: &str) -> brenn_wasm::ProcessorLoadSpec<'_> {
            brenn_wasm::ProcessorLoadSpec {
                // Placeholder; the callee owns this field.
                component_path: Path::new(""),
                slug,
                output_ports: Default::default(),
                declared_out_ports: Default::default(),
                input_amplification_mt: Default::default(),
                mqtt_sinks: Default::default(),
                config: Default::default(),
                grants: Default::default(),
                store_path: None,
                max_page_count: 1,
                max_payload_bytes: 1024,
                alerter: std::sync::Arc::new(NoopAlerter),
                output_acl: std::sync::Arc::new(|_| true),
                mqtt_publish: None,
                tool_host: None,
            }
        }

        /// Resolve, verify, and load a consumer's package by calling the boot
        /// path itself, so these cases cover the sequence `run_server` runs
        /// rather than a copy of it.
        ///
        /// `root` is an `Option` because a host started without `--components`
        /// has no root to resolve against, and the refusal that fact earns is
        /// part of the path under test.
        fn verify_then_load(
            root: Option<&Path>,
            package: &str,
            slug: &str,
            config_spec_sha256: &str,
            fill: impl FnOnce(&mut brenn_wasm::ProcessorLoadSpec<'_>),
        ) -> brenn_wasm::ProcessorComponent {
            let mut spec = load_spec(slug);
            fill(&mut spec);
            let roots: Vec<PathBuf> = root.map(Path::to_path_buf).into_iter().collect();
            super::load_verified_consumer(&roots, package, slug, config_spec_sha256, None, spec).0
        }

        const SPEC: &str = "component Demo {\n  abi = processor;\n}\n";

        /// A real built component's bytes, by artifact basename.
        ///
        /// The refusal cases below hand the loader bytes it rejects, which is
        /// what makes them proof of ordering; the two acceptance cases need the
        /// opposite — an artifact that loads — or a false refusal on a valid
        /// package would first be seen on a deploy target.
        fn fixture_bytes(basename: &str) -> Vec<u8> {
            crate::consumers::fixture_artifact(&format!("{basename}.wasm"))
        }

        #[test]
        fn a_consumer_whose_package_binds_it_loads() {
            let dir = tempfile::tempdir().expect("tempdir");
            package(
                dir.path(),
                "demo",
                &fixture_bytes("brenn_processor_demo"),
                Some(SPEC),
            );
            // Returning at all is the assertion: verification passed and the
            // loader instantiated the artifact behind it. Both would panic.
            let component = verify_then_load(
                Some(dir.path()),
                "demo",
                "demo",
                &brenn_lib::util::sha256_hex(SPEC.as_bytes()),
                // processor-demo imports `ports`; the load is a real one, so
                // the grant it needs is the real one too.
                |spec| spec.grants = [brenn_wasm::ComponentGrant::Ports].into_iter().collect(),
            );
            drop(component);
        }

        #[test]
        fn a_replay_endpoint_whose_package_binds_it_loads() {
            let dir = tempfile::tempdir().expect("tempdir");
            package(dir.path(), "replay", &fixture_bytes("brenn_replay"), None);
            // A real page budget, unlike the refusal cases below: this load
            // reaches the KV store, which cannot lay out its schema in one
            // page.
            let (component, _verified) = crate::load_verified_replay(
                "endpoint",
                &[dir.path().to_path_buf()],
                "replay",
                &dir.path().join("replay.sqlite"),
                brenn_wasm::store::DEFAULT_MAX_PAGE_COUNT,
                Default::default(),
            );
            drop(component);
        }

        #[test]
        #[should_panic(expected = "was configured against a specification that hashes to")]
        fn a_consumer_configured_against_no_specification_at_all_never_reaches_the_loader() {
            // The empty hash is what a lowering that stopped filling the field
            // would produce, and every hand-built fixture in the tree defaults
            // it to one. It must match nothing rather than match everything.
            let dir = tempfile::tempdir().expect("tempdir");
            package(dir.path(), "demo", b"not a component", Some(SPEC));
            verify_then_load(Some(dir.path()), "demo", "demo", "", |_| {});
        }

        #[test]
        #[should_panic(expected = "but its package record binds")]
        fn a_consumer_artifact_its_record_does_not_bind_never_reaches_the_loader() {
            let dir = tempfile::tempdir().expect("tempdir");
            let artifact = package(dir.path(), "demo", b"not a component", Some(SPEC));
            std::fs::write(&artifact, b"tampered").expect("tamper");
            verify_then_load(
                Some(dir.path()),
                "demo",
                "demo",
                &brenn_lib::util::sha256_hex(SPEC.as_bytes()),
                |_| {},
            );
        }

        #[test]
        #[should_panic(expected = "was configured against a specification that hashes to")]
        fn a_consumer_whose_config_spec_is_not_the_packaged_one_never_reaches_the_loader() {
            let dir = tempfile::tempdir().expect("tempdir");
            package(dir.path(), "demo", b"not a component", Some(SPEC));
            verify_then_load(
                Some(dir.path()),
                "demo",
                "demo",
                &brenn_lib::util::sha256_hex(b"component Demo {}\n"),
                |_| {},
            );
        }

        #[test]
        #[should_panic(expected = "has no readable record")]
        fn a_consumer_with_no_record_never_reaches_the_loader() {
            let dir = tempfile::tempdir().expect("tempdir");
            std::fs::create_dir(dir.path().join("demo")).expect("create package dir");
            std::fs::write(dir.path().join("demo/demo.wasm"), b"not a component")
                .expect("write artifact");
            verify_then_load(
                Some(dir.path()),
                "demo",
                "demo",
                &brenn_lib::util::sha256_hex(SPEC.as_bytes()),
                |_| {},
            );
        }

        #[test]
        #[should_panic(expected = "is not an installed package directory")]
        fn a_consumer_naming_a_package_that_is_not_installed_never_reaches_the_loader() {
            let dir = tempfile::tempdir().expect("tempdir");
            package(dir.path(), "demo", b"not a component", Some(SPEC));
            verify_then_load(
                Some(dir.path()),
                "panel",
                "demo",
                &brenn_lib::util::sha256_hex(SPEC.as_bytes()),
                |_| {},
            );
        }

        #[test]
        #[should_panic(expected = "without --components")]
        fn a_consumer_configured_on_a_host_started_without_the_flag_never_reaches_the_loader() {
            verify_then_load(
                None,
                "demo",
                "demo",
                &brenn_lib::util::sha256_hex(SPEC.as_bytes()),
                |_| {},
            );
        }

        #[test]
        #[should_panic(expected = "but its package record binds")]
        fn a_replay_artifact_its_record_does_not_bind_never_reaches_the_loader() {
            let dir = tempfile::tempdir().expect("tempdir");
            let artifact = package(dir.path(), "replay", b"not a component", None);
            std::fs::write(&artifact, b"tampered").expect("tamper");
            crate::load_verified_replay(
                "endpoint",
                &[dir.path().to_path_buf()],
                "replay",
                &dir.path().join("replay.sqlite"),
                1,
                Default::default(),
            );
        }

        #[test]
        #[should_panic(expected = "declares world")]
        fn a_replay_endpoint_handed_a_processor_package_never_reaches_the_loader() {
            let dir = tempfile::tempdir().expect("tempdir");
            package(dir.path(), "replay", b"not a component", Some(SPEC));
            crate::load_verified_replay(
                "endpoint",
                &[dir.path().to_path_buf()],
                "replay",
                &dir.path().join("replay.sqlite"),
                1,
                Default::default(),
            );
        }
    }

    /// A resolved consumer with the given grant words and tool names, all
    /// other fields defaulted.
    fn tool_consumer(
        grants: &[brenn_lib::messaging::ComponentGrant],
        tools: &[&str],
    ) -> brenn_lib::messaging::config::ResolvedWasmConsumer {
        let mut policy = brenn_lib::access::AppPolicy::default();
        for tool in tools {
            policy.tool_grants.insert(
                (*tool).to_string(),
                brenn_lib::tools::ResolvedToolGrant {
                    acl: Vec::new(),
                    rate_limit: None,
                },
            );
        }
        brenn_lib::messaging::config::ResolvedWasmConsumer {
            slug: "tooler".to_string(),
            package: "tooler".to_string(),
            spec_sha256: String::new(),
            declared_out_ports: std::collections::BTreeSet::new(),
            grants: grants.iter().copied().collect(),
            store_path: None,
            max_page_count: 1,
            inputs: Vec::new(),
            outputs: Vec::new(),
            config: std::collections::HashMap::new(),
            policy,
            activation_pacing: brenn_lib::messaging::config::ActivationPacing {
                burst: 1,
                min_period: std::time::Duration::from_millis(1),
            },
            mqtt_sinks: std::collections::HashMap::new(),
        }
    }

    #[test]
    fn a_granted_tools_word_with_a_named_tool_lowers_to_the_capability() {
        let parts = lower_consumer_load_parts(&tool_consumer(
            &[
                brenn_lib::messaging::ComponentGrant::Ports,
                brenn_lib::messaging::ComponentGrant::Tools,
            ],
            &["git-repo-pull"],
        ));
        assert!(parts.grants.contains(&brenn_wasm::ComponentGrant::Tools));
    }

    #[test]
    #[should_panic(expected = "granted iff the consumer names a tool")]
    fn a_tools_word_with_no_tool_statement_panics() {
        lower_consumer_load_parts(&tool_consumer(
            &[brenn_lib::messaging::ComponentGrant::Tools],
            &[],
        ));
    }

    #[test]
    #[should_panic(expected = "granted iff the consumer names a tool")]
    fn a_tool_statement_with_no_tools_word_panics() {
        lower_consumer_load_parts(&tool_consumer(
            &[brenn_lib::messaging::ComponentGrant::Ports],
            &["git-repo-pull"],
        ));
    }

    /// The host-side half of `store_path` resolution: an existing parent
    /// passes.
    #[test]
    fn a_store_path_under_an_existing_directory_passes() {
        let tmp = tempfile::TempDir::new().unwrap();
        assert_store_parent_exists("keeper", &tmp.path().join("store.sqlite"));
    }

    #[test]
    #[should_panic(expected = "parent directory does not exist")]
    fn a_store_path_under_a_missing_directory_panics() {
        assert_store_parent_exists(
            "keeper",
            std::path::Path::new("/nonexistent_dir_xyz_brenn_test/store.sqlite"),
        );
    }

    /// `load_consumer` checks the store parent before the package, so the
    /// operator sees the actionable refusal (missing state directory) first.
    #[tokio::test]
    #[should_panic(expected = "parent directory does not exist")]
    async fn a_load_of_a_consumer_whose_store_parent_is_missing_panics() {
        let (alert_dispatcher, _drainer) = brenn_obs::alerting::noop_alert_dispatcher();
        let tool_registry = Arc::new(brenn_tool_registry::ToolRegistry::new(vec![]));
        let ctx = ConsumerLoadContext {
            components_roots: &[],
            alert_dispatcher: &alert_dispatcher,
            mqtt_service: None,
            tool_registry: &tool_registry,
            max_payload_bytes: 1024,
        };
        let mut consumer = tool_consumer(&[brenn_lib::messaging::ComponentGrant::Ports], &[]);
        consumer.store_path = Some(std::path::PathBuf::from(
            "/nonexistent_dir_xyz_brenn_test/store.sqlite",
        ));
        load_consumer(&ctx, &consumer, None);
    }
}
