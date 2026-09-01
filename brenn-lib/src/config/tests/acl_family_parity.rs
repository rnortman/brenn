//! Parity gates between the DSL's ACL family table and the structs that hold
//! the lists it names.
//!
//! `Family` answers which list a plane and a scheme select and which entity
//! kinds keep it; the runtime counterpart of that answer is struct *shape* — the
//! fields of the raw ACL structs — and a struct definition cannot read a
//! predicate. So the equality is asserted here instead: each struct is
//! destructured exhaustively (a field added, removed or renamed fails to compile
//! in the destructure) and its fields are held equal to exactly the families
//! `Family::held_by` gives that kind, spelled through `Family::field_name`.
//!
//! Two structs per component, deliberately. `WasmAclsRaw` is a borrowed view
//! handed to policy building; `WasmConsumerConfigRaw` is what lowering fills.
//! Gating the view alone would leave a field added to one and not the other
//! uncaught, which is the drift this file exists to catch.

use std::collections::BTreeSet;

use brenn_dsl::derive::{AclShape, Family, MQTT_SINK_KEYS, REMOTE_CEILING_KEYS};
use brenn_envelope::grants::{ComponentHost, EntityKind};

use crate::access::raw::{
    AppAclRaw, AttachAclsRaw, ChannelMatcherRaw, MqttClientMatcherRaw, MqttSubMatcherRaw,
    WasmAclsRaw, WebhookMatcherRaw,
};
use crate::messaging::config::{WasmConsumerConfigRaw, WasmConsumerMqttOutputRaw};
use crate::messaging::remote::{RemoteConfigRaw, RemoteSubscribeAclRaw};

/// The field names this entity kind's lists are held under, in a struct of this
/// shape.
fn held(kind: EntityKind, shape: AclShape) -> BTreeSet<String> {
    Family::ALL
        .into_iter()
        .filter(|family| family.held_by(kind))
        .map(|family| family.field_name(shape))
        .collect()
}

fn named(fields: &[&str]) -> BTreeSet<String> {
    fields.iter().map(|field| (*field).to_string()).collect()
}

/// Every field of the struct is an ACL list, so the whole field set is the
/// family set.
fn assert_all_fields_are_lists(what: &str, fields: &[&str], kind: EntityKind, shape: AclShape) {
    assert_eq!(
        named(fields),
        held(kind, shape),
        "{what}: the struct's fields and the families a {} holds must be the same set",
        kind.label(),
    );
}

/// Only the `_acl`-suffixed fields are ACL lists; the rest are ports, budgets
/// and identity, and `rest` names them.
///
/// Both directions are asserted, because the suffix is a convention and not a
/// type: the suffixed fields must be exactly the families this kind holds, and
/// every remaining field must be one `rest` accounts for. Without the second
/// half an ACL list added under an unconventional name would be filtered out of
/// the comparison and pass unseen — which is the drift this file exists to
/// catch.
fn assert_acl_fields_are_lists(
    what: &str,
    fields: &[&str],
    kind: EntityKind,
    shape: AclShape,
    rest: &[&str],
) {
    let suffixed: Vec<&str> = fields
        .iter()
        .copied()
        .filter(|field| field.ends_with("_acl"))
        .collect();
    assert_eq!(
        named(&suffixed),
        held(kind, shape),
        "{what}: the `_acl` fields and the families a {} holds must be the same set",
        kind.label(),
    );
    let unsuffixed: Vec<&str> = fields
        .iter()
        .copied()
        .filter(|field| !field.ends_with("_acl"))
        .collect();
    assert_eq!(
        named(&unsuffixed),
        named(rest),
        "{what}: every field that is not an ACL list must be accounted for here — an ACL list \
         under a name without the `_acl` suffix would otherwise be filtered out of the \
         comparison above and gated by nothing",
    );
}

/// `Family::ALL` is what every gate in this file iterates, so a family missing
/// from it is a list nothing here holds anyone to. The arm list is exhaustive:
/// a tenth family stops this file compiling until it is written down, next to
/// the `ALL` row it also needs.
#[test]
fn the_family_table_is_walked_by_an_exhaustive_guard() {
    for family in Family::ALL {
        match family {
            Family::BrennSubscribe
            | Family::BrennPublish
            | Family::EphemeralSubscribe
            | Family::EphemeralPublish
            | Family::LocalSubscribe
            | Family::LocalPublish
            | Family::MqttSubscribe
            | Family::MqttPublish
            | Family::Webhook => {}
        }
        assert!(
            Family::ALL.contains(&family),
            "Family::ALL is missing {family:?}"
        );
    }
}

#[test]
fn an_agents_acl_block_holds_exactly_its_families() {
    let fields = field_names!(AppAclRaw {
        mqtt_subscribe,
        mqtt_publish,
        brenn_subscribe,
        brenn_publish,
        ephemeral_publish,
        ephemeral_subscribe,
        local_publish,
        webhook,
    });
    assert_all_fields_are_lists("app acl", &fields, EntityKind::Agent, AclShape::App);
}

#[test]
fn a_components_acl_view_holds_exactly_its_families() {
    let fields = field_names!(WasmAclsRaw {
        subscribe,
        ephemeral_subscribe,
        publish,
        ephemeral_publish,
        local_publish,
        local_subscribe,
        mqtt_publish,
        mqtt_subscribe,
        webhook,
    });
    assert_all_fields_are_lists(
        "wasm acl view",
        &fields,
        EntityKind::Component(ComponentHost::TopLevel),
        AclShape::View,
    );
}

#[test]
fn an_attachers_acl_view_holds_exactly_its_families() {
    let fields = field_names!(AttachAclsRaw {
        subscribe,
        publish,
        ephemeral_subscribe,
        ephemeral_publish,
    });
    assert_all_fields_are_lists(
        "attach acl view",
        &fields,
        EntityKind::Surface,
        AclShape::View,
    );
}

#[test]
fn a_consumers_config_holds_exactly_its_families() {
    let fields = field_names!(WasmConsumerConfigRaw {
        slug,
        package,
        spec_sha256,
        declared_out_ports,
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
    assert_acl_fields_are_lists(
        "consumer config",
        &fields,
        EntityKind::Component(ComponentHost::TopLevel),
        AclShape::ConsumerConfig,
        &[
            "slug",
            "package",
            "spec_sha256",
            "declared_out_ports",
            "grants",
            "store_path",
            "store_size_limit",
            "subscriptions",
            "outputs",
            "io_ports",
            "config",
            "activation_burst",
            "activation_min_period_ms",
            "mqtt_outputs",
            "tool_grants",
        ],
    );
}

#[test]
fn a_remotes_config_holds_exactly_its_families() {
    let fields = field_names!(RemoteConfigRaw {
        slug,
        token_file,
        grants,
        subscribe_acl,
        ephemeral_subscribe_acl,
        publish_acl,
        ephemeral_publish_acl,
        publish_burst,
        publish_per_sec,
        max_sessions,
        max_subscriptions,
    });
    assert_acl_fields_are_lists(
        "remote config",
        &fields,
        EntityKind::Remote,
        AclShape::ConsumerConfig,
        &[
            "slug",
            "token_file",
            "grants",
            "publish_burst",
            "publish_per_sec",
            "max_sessions",
            "max_subscriptions",
        ],
    );
}

/// Which of a remote's lists carry per-entry depth ceilings is a fact about
/// their element type, so the types are pinned in a destructure and the names
/// are held equal to `carries_ceilings`.
#[allow(dead_code)]
fn ceiling_lists_are_typed_as_ceilings(remote: RemoteConfigRaw) {
    let _ceilings: [Vec<RemoteSubscribeAclRaw>; 2] =
        [remote.subscribe_acl, remote.ephemeral_subscribe_acl];
    let _plain: [Vec<ChannelMatcherRaw>; 2] = [remote.publish_acl, remote.ephemeral_publish_acl];
}

/// The same, for the element types of every other list: a family that changed
/// its matcher kind would land here rather than in a lowering surprise.
#[allow(dead_code)]
fn each_list_carries_its_matcher_kind(app: AppAclRaw, wasm: WasmAclsRaw<'_>) {
    let _channels: [Vec<ChannelMatcherRaw>; 5] = [
        app.brenn_subscribe,
        app.brenn_publish,
        app.ephemeral_subscribe,
        app.ephemeral_publish,
        app.local_publish,
    ];
    let _mqtt_subs: [Vec<MqttSubMatcherRaw>; 1] = [app.mqtt_subscribe];
    let _mqtt_clients: [Vec<MqttClientMatcherRaw>; 1] = [app.mqtt_publish];
    let _webhooks: [Vec<WebhookMatcherRaw>; 1] = [app.webhook];
    let _local_subscribe: &[ChannelMatcherRaw] = wasm.local_subscribe;
}

#[test]
fn the_lists_carrying_ceilings_are_the_ones_typed_for_them() {
    let ceilings: BTreeSet<String> = Family::ALL
        .into_iter()
        .filter(|family| family.carries_ceilings())
        .map(|family| family.field_name(AclShape::ConsumerConfig))
        .collect();
    assert_eq!(
        ceilings,
        named(&["subscribe_acl", "ephemeral_subscribe_acl"]),
        "the families carrying ceilings must be exactly the remote lists typed \
         RemoteSubscribeAclRaw",
    );
    for family in Family::ALL.into_iter().filter(|f| f.carries_ceilings()) {
        assert!(
            family.held_by(EntityKind::Remote),
            "{}: a ceiling is a remote's to state, so only a family a remote holds carries one",
            family.name(),
        );
    }
}

#[test]
fn a_ceiling_entry_states_exactly_the_ceiling_keys() {
    let fields = field_names!(RemoteSubscribeAclRaw {
        exact,
        prefix,
        push_depth,
        retain_depth,
    });
    let matcher_kinds = named(&["exact", "prefix"]);
    let stated: BTreeSet<String> = fields
        .iter()
        .map(|field| (*field).to_string())
        .filter(|field| !matcher_kinds.contains(field))
        .collect();
    assert_eq!(
        stated,
        named(&REMOTE_CEILING_KEYS),
        "a remote's subscribe entry states the matcher kind and the ceiling keys the DSL \
         admits, and nothing else",
    );
}

/// The other entry that carries a tail: an outbound MQTT entry mints a sink, and
/// the block that overrides that sink's budget states exactly the tail's keys.
#[test]
fn an_mqtt_sink_override_states_exactly_the_sink_keys() {
    let fields = field_names!(WasmConsumerMqttOutputRaw {
        client,
        publish_per_activation,
        publish_capacity,
    });
    let stated: BTreeSet<String> = fields
        .iter()
        .map(|field| (*field).to_string())
        .filter(|field| field != "client")
        .collect();
    assert_eq!(
        stated,
        named(&MQTT_SINK_KEYS),
        "an mqtt sink override states the client it is about and the budget keys the DSL \
         admits, and nothing else",
    );
}
