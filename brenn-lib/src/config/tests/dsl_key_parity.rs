//! Parity gates between the DSL's resolver key tables and the config structs
//! they admit keys for.
//!
//! Every other vocabulary the language carries is tied to its struct by the
//! exhaustive struct literals in lowering: a field added to a config struct
//! fails to compile where the literal builds it. A key table is a list of
//! strings, which no literal reaches, so the tie is made here instead. A field
//! added, removed or renamed fails to compile in the destructure below; a field
//! the DSL neither spells nor deliberately omits fails the assertion, named.

use brenn_dsl::resolve::{CONSUMER_KEYS, SURFACE_COMPONENT_KEYS};

use crate::messaging::config::{SurfaceComponentRaw, WasmConsumerConfigRaw};

/// Every field is either a key the DSL admits or a deliberate omission with a
/// reason; every key and every omission names a field.
fn assert_parity(what: &str, fields: &[&str], keys: &[&str], omitted: &[(&str, &str)]) {
    for field in fields {
        let spelled = keys.contains(field);
        let excused = omitted.iter().any(|(name, _)| name == field);
        assert!(
            spelled != excused,
            "{what}: field `{field}` is {} — a config field is either a DSL key or a \
             listed omission with a reason, never both and never neither",
            if spelled { "both" } else { "neither" },
        );
    }
    for name in keys.iter().chain(omitted.iter().map(|(name, _)| name)) {
        assert!(
            fields.contains(name),
            "{what}: `{name}` names no field of the config struct",
        );
    }
}

#[test]
fn surface_component_keys_account_for_every_field() {
    let fields = field_names!(SurfaceComponentRaw {
        kind,
        instance,
        abi,
        send_burst,
        send_refill_secs,
        parked_batch_depth,
        chrome,
        config,
        grants,
    });
    let omitted = [
        ("kind", "folds from the class name an instance names"),
        ("instance", "is the `new` handle"),
        ("abi", "is the class's artifact shape"),
    ];
    assert_parity(
        "surface component",
        &fields,
        &SURFACE_COMPONENT_KEYS,
        &omitted,
    );
}

#[test]
fn consumer_keys_account_for_every_field() {
    let fields = field_names!(WasmConsumerConfigRaw {
        slug,
        component_path,
        grants,
        store_path,
        store_size_limit,
        subscriptions,
        outputs,
        io_ports,
        subscribe_acl,
        ephemeral_subscribe_acl,
        local_subscribe_acl,
        publish_acl,
        ephemeral_publish_acl,
        local_publish_acl,
        mqtt_publish_acl,
        mqtt_subscribe_acl,
        webhook_acl,
        config,
        activation_burst,
        activation_min_period_ms,
        mqtt_outputs,
        tool_grants,
    });
    let statement = "carried by a statement, not a key";
    let omitted = [
        ("subscriptions", statement),
        ("outputs", statement),
        ("io_ports", statement),
        ("subscribe_acl", statement),
        ("ephemeral_subscribe_acl", statement),
        ("local_subscribe_acl", statement),
        ("publish_acl", statement),
        ("ephemeral_publish_acl", statement),
        ("local_publish_acl", statement),
        ("mqtt_publish_acl", statement),
        ("mqtt_subscribe_acl", statement),
        ("webhook_acl", statement),
        ("mqtt_outputs", statement),
        ("tool_grants", statement),
    ];
    assert_parity("consumer", &fields, &CONSUMER_KEYS, &omitted);
}
