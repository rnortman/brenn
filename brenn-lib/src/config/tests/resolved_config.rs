use super::*;
use crate::config::ResolvedConfig;
use crate::integration::IntegrationRegistry;
use crate::messaging::Urgency;
use crate::pwa_push::config::AppPwaPushBlock;
use crate::pwa_push::config::PwaPushGlobalConfig;
use crate::webhook::config::WebhookSignatureConfigRaw;
use crate::webhook::{AppWebhookSubscriptionRaw, WebhookEndpointConfigRaw, WebhookKeyConfigRaw};

// -----------------------------------------------------------------------
// ResolvedConfig round-trip tests (AC-9 / design §Test plan)
// -----------------------------------------------------------------------

/// End-to-end test: `validate_and_resolve` → `ResolvedConfig.webhook_endpoints`
/// → `WebhookService` produces the same slug set.
///
/// Guards the refactored bootstrap consumption path that previously had no
/// coverage (design §"Test plan").
#[test]
fn resolved_config_webhook_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let app_dir = dir.path().join("myapp");
    std::fs::create_dir(&app_dir).unwrap();

    // Write a secret file for the HMAC key.
    let secret_path = write_secret(dir.path(), "hmac.secret", "my-super-secret");

    let config = BrennConfig {
        webhook_endpoints: vec![WebhookEndpointConfigRaw {
            slug: "test-hook".to_string(),
            mount: None,
            description: None,
            transport_ceiling_bytes: 1024 * 1024,
            content_type: "application/json".to_string(),
            signature: WebhookSignatureConfigRaw::HmacRawBody {
                algorithm: "hmac-sha256".to_string(),
                header: "X-Hub-Signature-256".to_string(),
                format: "hex".to_string(),
                key_id_header: None,
            },
            keys: vec![WebhookKeyConfigRaw {
                key_id: "k1".to_string(),
                secret_file: secret_path,
            }],
            tokens: vec![],
            replay_protection: None,
            urgency: None,
        }],
        apps: vec![AppConfigRaw {
            slug: "myapp".to_string(),
            working_dir: Some(app_dir),
            singleton: true,
            allowed_users: vec!["alice".to_string()],
            // singleton apps require at least compact_soft_pct.
            compact_soft_pct: Some(75),
            webhook_subscriptions: vec![AppWebhookSubscriptionRaw {
                endpoint: "test-hook".to_string(),
                wake_min: None,
            }],
            ..Default::default()
        }],
        ..Default::default()
    };

    let ResolvedConfig {
        apps,
        webhook_endpoints,
        pwa_push,
        ..
    } = validate_and_resolve(
        &config,
        &IntegrationRegistry::new(vec![]),
        Some(super::test_runtime_dir()),
    );

    // apps contains the app and its webhook_subscriptions is non-empty.
    let app = apps.get("myapp").expect("myapp must be in resolved apps");
    assert!(
        !app.webhook_subscriptions.is_empty(),
        "app's webhook_subscriptions must be populated"
    );
    assert_eq!(app.webhook_subscriptions[0].endpoint_slug, "test-hook");

    // webhook_endpoints carries the declared endpoint.
    assert_eq!(webhook_endpoints.len(), 1, "one endpoint expected");
    let ep = webhook_endpoints
        .get("test-hook")
        .expect("test-hook must be in webhook_endpoints");
    assert_eq!(ep.slug, "test-hook");
    assert_eq!(ep.owner.app_slug(), Some("myapp"));
    assert_eq!(ep.urgency, Urgency::Normal);
    assert_eq!(ep.mount, "/webhooks/test-hook");
    // Signature scheme resolved with a non-empty key set.
    match &ep.scheme {
        crate::webhook::SignatureScheme::HmacRawBody { keys, .. } => {
            assert!(!keys.is_empty(), "HMAC key must be loaded");
        }
        other => panic!("unexpected scheme: {:?}", other),
    }

    // pwa_push is None (no pwa_push configured).
    assert!(pwa_push.is_none());

    // Build a WebhookService from the endpoint table and verify slug coverage.
    let svc = crate::webhook::WebhookService::new(webhook_endpoints);
    let svc_slugs: std::collections::HashSet<&str> =
        svc.all_endpoints().map(|ep| ep.slug.as_str()).collect();
    assert!(
        svc_slugs.contains("test-hook"),
        "service must expose test-hook"
    );
    assert_eq!(svc_slugs.len(), 1);
}

/// App-owned endpoints stamp a resolved `wake_min` onto the owning app's
/// `webhook_subscriptions`: the sub-level override when present, else the
/// global `default_wake_min` fallback. Guards the Rule 9 `app_stamp` closure,
/// restructured in the WASM-owner refactor. The fixture also declares a
/// wasm-owned endpoint (`hook-wasm`, owned by `myconsumer` via a
/// `webhook:hook-wasm` subscription and subscribed to by no app): the
/// assertion that the app carries exactly the two app-subscribed endpoints
/// proves a wasm-owned endpoint stamps nothing onto any app, and the endpoint
/// resolves to `WebhookOwner::Wasm`.
#[test]
fn app_owned_endpoint_stamps_resolved_wake_min() {
    use crate::messaging::WakeMin;
    use crate::webhook::config::WebhookOwner;

    let dir = tempfile::tempdir().unwrap();
    let app_dir = dir.path().join("myapp");
    std::fs::create_dir(&app_dir).unwrap();

    let secret_override = write_secret(dir.path(), "override.secret", "s1");
    let secret_default = write_secret(dir.path(), "default.secret", "s2");

    let make_endpoint = |slug: &str, secret: std::path::PathBuf| WebhookEndpointConfigRaw {
        slug: slug.to_string(),
        mount: None,
        description: None,
        transport_ceiling_bytes: 1024 * 1024,
        content_type: "application/json".to_string(),
        signature: WebhookSignatureConfigRaw::HmacRawBody {
            algorithm: "hmac-sha256".to_string(),
            header: "X-Hub-Signature-256".to_string(),
            format: "hex".to_string(),
            key_id_header: None,
        },
        keys: vec![WebhookKeyConfigRaw {
            key_id: "k1".to_string(),
            secret_file: secret,
        }],
        tokens: vec![],
        replay_protection: None,
        urgency: None,
    };

    let secret_wasm = write_secret(dir.path(), "wasm.secret", "s3");

    let mut config = BrennConfig {
        webhook_endpoints: vec![
            make_endpoint("hook-override", secret_override),
            make_endpoint("hook-default", secret_default),
            // Wasm-owned: no app subscribes; `myconsumer` owns it via a
            // `webhook:hook-wasm` subscription. Must stamp nothing onto any app.
            make_endpoint("hook-wasm", secret_wasm),
        ],
        wasm_consumers: vec![crate::messaging::config::WasmConsumerConfigRaw::minimal(
            "myconsumer",
            dir.path().join("dummy.wasm"),
            &["webhook:hook-wasm"],
        )],
        apps: vec![AppConfigRaw {
            slug: "myapp".to_string(),
            working_dir: Some(app_dir),
            singleton: true,
            allowed_users: vec!["alice".to_string()],
            compact_soft_pct: Some(75),
            webhook_subscriptions: vec![
                AppWebhookSubscriptionRaw {
                    endpoint: "hook-override".to_string(),
                    wake_min: Some(WakeMin::High),
                },
                AppWebhookSubscriptionRaw {
                    endpoint: "hook-default".to_string(),
                    wake_min: None,
                },
            ],
            ..Default::default()
        }],
        ..Default::default()
    };
    // Set the global fallback to a value distinct from the override so the
    // "inherit global default" path is unambiguously exercised.
    config.messaging.default_wake_min = WakeMin::VeryLow;

    let ResolvedConfig {
        apps,
        webhook_endpoints,
        ..
    } = validate_and_resolve(
        &config,
        &IntegrationRegistry::new(vec![]),
        Some(super::test_runtime_dir()),
    );

    let app = apps.get("myapp").expect("myapp must be in resolved apps");
    assert_eq!(
        app.webhook_subscriptions.len(),
        2,
        "exactly the two app-subscribed endpoints must be stamped; the wasm-owned \
         endpoint must stamp nothing onto the app"
    );

    // The wasm-owned endpoint resolved to a Wasm owner and, by the count above,
    // contributed no app-side subscription stamp.
    let wasm_ep = webhook_endpoints
        .get("hook-wasm")
        .expect("hook-wasm must be in webhook_endpoints");
    match &wasm_ep.owner {
        WebhookOwner::Wasm(slug) => assert_eq!(slug.as_ref(), "myconsumer"),
        other => panic!("hook-wasm must be wasm-owned, got {other:?}"),
    }

    let find = |slug: &str| {
        app.webhook_subscriptions
            .iter()
            .find(|s| s.endpoint_slug == slug)
            .unwrap_or_else(|| panic!("subscription for {slug} must be stamped"))
    };
    assert_eq!(
        find("hook-override").wake_min,
        WakeMin::High,
        "sub-level wake_min override must be stamped"
    );
    assert_eq!(
        find("hook-default").wake_min,
        WakeMin::VeryLow,
        "absent sub-level wake_min must inherit the global default_wake_min"
    );
}

/// End-to-end test: `validate_and_resolve` → `ResolvedConfig.pwa_push` is
/// `Some(...)` when an app has `pwa_push.enabled = true` and a valid global
/// `[pwa_push]` block is present.
///
/// Guards the Phase 5 binding (`let pwa_push = resolve_pwa_push_layer(...)`)
/// against accidental short-circuiting in future `validate_and_resolve` edits
/// (design §"Test plan").
///
#[test]
fn resolved_config_pwa_push_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let app_dir = dir.path().join("pushapp");
    std::fs::create_dir(&app_dir).unwrap();

    // Point keypair_file at a non-existent path inside the tempdir so that
    // `load_or_generate` generates a fresh keypair and writes it there.
    let keypair_path = dir.path().join("vapid.json");
    let subject = "mailto:test@example.com";

    let config = BrennConfig {
        pwa_push: PwaPushGlobalConfig {
            keypair_file: Some(keypair_path),
            subject: Some(subject.to_string()),
            ..Default::default()
        },
        apps: vec![AppConfigRaw {
            slug: "pushapp".to_string(),
            working_dir: Some(app_dir),
            allowed_users: vec!["alice".to_string()],
            // Push capability is now authorized by the `pwa_push` *grant*, not by
            // `[app.pwa_push].enabled` section presence (access-control Phase 0).
            // The section is retained for its delivery settings; the grant is what
            // makes `resolve_pwa_push_layer` load the keypair.
            grants: vec![crate::access::AppCapability::PwaPush],
            pwa_push: Some(AppPwaPushBlock {
                default_title: None,
            }),
            ..Default::default()
        }],
        ..Default::default()
    };

    let ResolvedConfig { pwa_push, apps, .. } = validate_and_resolve(
        &config,
        &IntegrationRegistry::new(vec![]),
        Some(super::test_runtime_dir()),
    );

    // Phase 5 binding must produce Some(...).
    let resolved = pwa_push.expect("pwa_push must be Some when an app enables it");

    // Subject round-trips verbatim.
    assert_eq!(resolved.subject, subject);

    // VAPID keypair was generated (87-char base64url public key).
    assert_eq!(
        resolved.vapid.public_b64url.len(),
        87,
        "VAPID public key must be 87-char base64url-no-pad"
    );

    // The app is resolved and has push authorized — now via the `PwaPush`
    // grant (the legacy `[app.pwa_push].enabled` boolean was removed;
    // access-control §2.5.1), surfaced through `pwa_push_enabled()`.
    let app = apps
        .get("pushapp")
        .expect("pushapp must be in resolved apps");
    assert!(
        app.pwa_push_enabled(),
        "app must have the pwa_push grant (push authorized)"
    );
}

/// When no app enables `pwa_push`, `ResolvedConfig.pwa_push` must be `None`.
///
/// Guards against a future regression where the Phase 5 binding is accidentally
/// wired to a non-None default or short-circuited.
#[test]
fn resolved_config_pwa_push_absent_is_none() {
    let dir = tempfile::tempdir().unwrap();
    let config = BrennConfig {
        apps: vec![AppConfigRaw {
            slug: "myapp".to_string(),
            working_dir: Some(dir.path().to_path_buf()),
            ..Default::default()
        }],
        ..Default::default()
    };
    let ResolvedConfig { pwa_push, .. } = validate_and_resolve(
        &config,
        &IntegrationRegistry::new(vec![]),
        Some(super::test_runtime_dir()),
    );
    assert!(pwa_push.is_none());
}

/// The git webhook pipeline reference config resolves clean: two per-forge
/// endpoints, each owned by the sole WASM subscriber `git-forge-parser`, with the
/// 64 KiB transport ceiling and the two forge-specific hex formats carried
/// through. Guards that a single WASM consumer owning *multiple* endpoints
/// resolves (the parser owns both git endpoints), and that `sha256-hex` resolves.
#[test]
fn git_pipeline_reference_config_resolves() {
    use crate::webhook::config::WebhookOwner;

    let dir = tempfile::tempdir().unwrap();
    let secret_forgejo = write_secret(dir.path(), "forgejo.secret", "s-forgejo");
    let secret_github = write_secret(dir.path(), "github.secret", "s-github");

    let endpoint = |slug: &str, header: &str, format: &str, secret: std::path::PathBuf| {
        WebhookEndpointConfigRaw {
            slug: slug.to_string(),
            mount: None,
            description: None,
            transport_ceiling_bytes: 65536,
            content_type: "application/json".to_string(),
            signature: WebhookSignatureConfigRaw::HmacRawBody {
                algorithm: "hmac-sha256".to_string(),
                header: header.to_string(),
                format: format.to_string(),
                key_id_header: None,
            },
            keys: vec![WebhookKeyConfigRaw {
                key_id: slug.to_string(),
                secret_file: secret,
            }],
            tokens: vec![],
            replay_protection: None,
            urgency: None,
        }
    };

    let config = BrennConfig {
        webhook_endpoints: vec![
            endpoint("git-forgejo", "X-Gitea-Signature", "hex", secret_forgejo),
            endpoint(
                "git-github",
                "X-Hub-Signature-256",
                "sha256-hex",
                secret_github,
            ),
        ],
        // The parser is the sole subscriber of both webhook channels, so it owns
        // both endpoints (Rule 9 sole-wasm-owner).
        wasm_consumers: vec![crate::messaging::config::WasmConsumerConfigRaw::minimal(
            "git-forge-parser",
            dir.path().join("brenn_git_forge_parser.wasm"),
            &["webhook:git-forgejo", "webhook:git-github"],
        )],
        // The resolver requires at least one app; the pipeline itself is app-less
        // (both endpoints are wasm-owned), so a placeholder that touches neither
        // endpoint stands in for the rest of a real deployment.
        apps: vec![AppConfigRaw {
            slug: "placeholder".to_string(),
            working_dir: Some(dir.path().to_path_buf()),
            ..Default::default()
        }],
        ..Default::default()
    };

    let ResolvedConfig {
        webhook_endpoints, ..
    } = validate_and_resolve(
        &config,
        &IntegrationRegistry::new(vec![]),
        Some(super::test_runtime_dir()),
    );

    assert_eq!(webhook_endpoints.len(), 2, "both git endpoints resolve");
    for slug in ["git-forgejo", "git-github"] {
        let ep = webhook_endpoints
            .get(slug)
            .unwrap_or_else(|| panic!("{slug} must resolve"));
        assert_eq!(ep.transport_ceiling_bytes, 65536);
        assert_eq!(ep.mount, format!("/webhooks/{slug}"));
        match &ep.owner {
            WebhookOwner::Wasm(owner) => assert_eq!(owner.as_ref(), "git-forge-parser"),
            other => panic!("{slug} must be wasm-owned, got {other:?}"),
        }
    }
}
