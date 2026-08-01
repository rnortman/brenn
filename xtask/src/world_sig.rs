// Structural signatures for WIT worlds, used by the world-equivalence gate in
// `check_wit.rs` to compare a built artifact's embedded world against the WIT source
// the component was generated from.
//
// The signature is a canonical text form computed over decoded `wit_parser` data, never
// over printed WIT. That makes the three differences that are legitimate between a source
// world and its artifact invisible by construction rather than special-cased:
//
//   - doc comments (stripped from the encoded type section) are never read;
//   - item order (the encoder's, not the source file's) is normalized by sorting;
//   - arena ids (meaningless across two independent `Resolve`s) never appear — a named
//     interface type is referenced by its package-qualified path instead.
//
// Two worlds with equal signature maps accept and produce the same values under the
// canonical ABI. Two worlds with unequal maps differ in a way a guest or host can observe.
//
// An interface keeps its members as a map rather than one flattened string, because
// componentization elides at member granularity: an artifact's copy of an imported
// interface carries only the functions and types the guest actually reaches. Comparing
// member-by-member is what lets the caller apply subset rules to imports and equality
// rules to exports.

use std::collections::BTreeMap;
use wit_parser::{
    Function, FunctionKind, Handle, InterfaceId, Resolve, Type, TypeDefKind, TypeId, TypeOwner,
    WorldId, WorldItem, WorldKey,
};

/// A world reduced to two key→signature maps. Keys are the world's import/export names
/// (a kebab-name, or the package-qualified id of an interface); values are the canonical
/// structural signature of the item under that key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorldSignature {
    pub imports: BTreeMap<String, ItemSignature>,
    pub exports: BTreeMap<String, ItemSignature>,
}

/// The signature of one world item.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ItemSignature {
    /// An imported or exported interface, as member name → member signature. Member names
    /// are the interface's own export names (`publish`, `[method]transaction.get`,
    /// `store-error`), which are the guest-visible ones.
    Interface(BTreeMap<String, String>),
    /// A world-level function or type: one signature string.
    Item(String),
}

impl ItemSignature {
    /// A one-line rendering for diagnostics.
    pub fn describe(&self) -> String {
        match self {
            ItemSignature::Interface(members) => {
                let rendered = members
                    .iter()
                    .map(|(name, sig)| format!("{name}: {sig}"))
                    .collect::<Vec<_>>()
                    .join("; ");
                format!("interface {{{rendered}}}")
            }
            ItemSignature::Item(sig) => sig.clone(),
        }
    }
}

/// Reduce one world of `resolve` to its canonical signature maps.
pub fn world_signature(resolve: &Resolve, world: WorldId) -> WorldSignature {
    let world = &resolve.worlds[world];
    WorldSignature {
        imports: signature_map(resolve, world.imports.iter()),
        exports: signature_map(resolve, world.exports.iter()),
    }
}

fn signature_map<'a>(
    resolve: &Resolve,
    items: impl Iterator<Item = (&'a WorldKey, &'a WorldItem)>,
) -> BTreeMap<String, ItemSignature> {
    items
        .map(|(key, item)| (item_key(resolve, key), item_signature(resolve, item)))
        .collect()
}

/// The map key for one world item: the kebab-name, or the package-qualified interface id.
///
/// `WorldKey::Interface` is only used for interfaces that carry a package (an inline
/// interface in a world is keyed by name), so `id_of` resolving to `None` here would mean
/// a shape neither world in this repo can produce; it degrades to a fixed placeholder
/// rather than an arena index, which would differ between two independent `Resolve`s and
/// report a spurious mismatch.
fn item_key(resolve: &Resolve, key: &WorldKey) -> String {
    match key {
        WorldKey::Name(name) => name.clone(),
        WorldKey::Interface(id) => interface_key(resolve, *id),
    }
}

fn interface_key(resolve: &Resolve, id: InterfaceId) -> String {
    resolve
        .id_of(id)
        .unwrap_or_else(|| match &resolve.interfaces[id].name {
            Some(name) => format!("<unpackaged-interface {name}>"),
            None => "<anonymous-interface>".to_string(),
        })
}

fn item_signature(resolve: &Resolve, item: &WorldItem) -> ItemSignature {
    match item {
        WorldItem::Interface { id, .. } => {
            ItemSignature::Interface(interface_members(resolve, *id))
        }
        WorldItem::Function(f) => {
            ItemSignature::Item(format!("func {}", function_signature(resolve, f)))
        }
        WorldItem::Type(id) => {
            ItemSignature::Item(format!("type {}", type_body(resolve, *id, &mut Vec::new())))
        }
    }
}

/// An interface as its member map: type definitions and function signatures keyed by the
/// interface's own export names. A map, not a list, so ordering — the encoder's, not the
/// source file's — cannot register as a difference.
fn interface_members(resolve: &Resolve, id: InterfaceId) -> BTreeMap<String, String> {
    let iface = &resolve.interfaces[id];
    let mut members = BTreeMap::new();
    for (name, type_id) in &iface.types {
        members.insert(
            format!("type {name}"),
            type_body(resolve, *type_id, &mut Vec::new()),
        );
    }
    for (name, func) in &iface.functions {
        members.insert(format!("func {name}"), function_signature(resolve, func));
    }
    members
}

fn function_signature(resolve: &Resolve, func: &Function) -> String {
    let kind = match &func.kind {
        FunctionKind::Freestanding => "freestanding",
        FunctionKind::AsyncFreestanding => "async-freestanding",
        FunctionKind::Method(_) => "method",
        FunctionKind::AsyncMethod(_) => "async-method",
        FunctionKind::Static(_) => "static",
        FunctionKind::AsyncStatic(_) => "async-static",
        FunctionKind::Constructor(_) => "constructor",
    };
    // The owning resource of a method/static/constructor is not rendered: it is already
    // fixed by the function's key in the interface map (`[method]transaction.get`).
    let params = func
        .params
        .iter()
        .map(|(name, ty)| format!("{name}: {}", type_ref(resolve, ty, &mut Vec::new())))
        .collect::<Vec<_>>()
        .join(", ");
    let result = match &func.result {
        Some(ty) => type_ref(resolve, ty, &mut Vec::new()),
        None => "_".to_string(),
    };
    format!("{kind}({params}) -> {result}")
}

/// How a type is referred to from a use site.
///
/// A named type owned by an interface is referred to nominally, by its package-qualified
/// path. That is both stable across two independent `Resolve`s and the thing that stops
/// recursion: a resource's methods take handles to the resource itself.
///
/// Everything else — anonymous structural types, and named types owned by a world — is
/// expanded in place. World-owned names are the `use` aliases a world pulls in; which
/// world declares them is not part of the shape, and the artifact's synthesized world has
/// a different name from the source's.
fn type_ref(resolve: &Resolve, ty: &Type, seen: &mut Vec<TypeId>) -> String {
    match ty {
        Type::Bool => "bool".to_string(),
        Type::U8 => "u8".to_string(),
        Type::U16 => "u16".to_string(),
        Type::U32 => "u32".to_string(),
        Type::U64 => "u64".to_string(),
        Type::S8 => "s8".to_string(),
        Type::S16 => "s16".to_string(),
        Type::S32 => "s32".to_string(),
        Type::S64 => "s64".to_string(),
        Type::F32 => "f32".to_string(),
        Type::F64 => "f64".to_string(),
        Type::Char => "char".to_string(),
        Type::String => "string".to_string(),
        Type::ErrorContext => "error-context".to_string(),
        Type::Id(id) => type_id_ref(resolve, *id, seen),
    }
}

fn type_id_ref(resolve: &Resolve, id: TypeId, seen: &mut Vec<TypeId>) -> String {
    let def = &resolve.types[id];
    match (&def.name, def.owner) {
        (Some(name), TypeOwner::Interface(iface)) => {
            format!("{}/{name}", interface_key(resolve, iface))
        }
        _ => type_body(resolve, id, seen),
    }
}

/// The structure of one type definition, with use sites rendered by `type_ref`.
///
/// `seen` guards against a cycle through anonymous types. Nominal references break every
/// cycle the two in-tree worlds can express, so this is belt-and-braces: it keeps a
/// pathological input a reported mismatch instead of a stack overflow.
fn type_body(resolve: &Resolve, id: TypeId, seen: &mut Vec<TypeId>) -> String {
    if seen.contains(&id) {
        return "<recursive>".to_string();
    }
    seen.push(id);
    let body = kind_body(resolve, &resolve.types[id].kind, seen);
    seen.pop();
    body
}

fn kind_body(resolve: &Resolve, kind: &TypeDefKind, seen: &mut Vec<TypeId>) -> String {
    match kind {
        TypeDefKind::Record(record) => {
            let fields = record
                .fields
                .iter()
                .map(|f| format!("{}: {}", f.name, type_ref(resolve, &f.ty, seen)))
                .collect::<Vec<_>>()
                .join(", ");
            format!("record{{{fields}}}")
        }
        TypeDefKind::Resource => "resource".to_string(),
        TypeDefKind::Handle(Handle::Own(id)) => {
            format!("own<{}>", type_id_ref(resolve, *id, seen))
        }
        TypeDefKind::Handle(Handle::Borrow(id)) => {
            format!("borrow<{}>", type_id_ref(resolve, *id, seen))
        }
        TypeDefKind::Flags(flags) => {
            let names = flags
                .flags
                .iter()
                .map(|f| f.name.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            format!("flags{{{names}}}")
        }
        TypeDefKind::Tuple(tuple) => {
            let types = tuple
                .types
                .iter()
                .map(|t| type_ref(resolve, t, seen))
                .collect::<Vec<_>>()
                .join(", ");
            format!("tuple<{types}>")
        }
        TypeDefKind::Variant(variant) => {
            let cases = variant
                .cases
                .iter()
                .map(|c| match &c.ty {
                    Some(ty) => format!("{}({})", c.name, type_ref(resolve, ty, seen)),
                    None => c.name.clone(),
                })
                .collect::<Vec<_>>()
                .join(", ");
            format!("variant{{{cases}}}")
        }
        TypeDefKind::Enum(enum_) => {
            let cases = enum_
                .cases
                .iter()
                .map(|c| c.name.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            format!("enum{{{cases}}}")
        }
        TypeDefKind::Option(ty) => format!("option<{}>", type_ref(resolve, ty, seen)),
        TypeDefKind::Result(result) => format!(
            "result<{}, {}>",
            optional_type_ref(resolve, &result.ok, seen),
            optional_type_ref(resolve, &result.err, seen)
        ),
        TypeDefKind::List(ty) => format!("list<{}>", type_ref(resolve, ty, seen)),
        TypeDefKind::Map(key, value) => format!(
            "map<{}, {}>",
            type_ref(resolve, key, seen),
            type_ref(resolve, value, seen)
        ),
        TypeDefKind::FixedSizeList(ty, len) => {
            format!("list<{}, {len}>", type_ref(resolve, ty, seen))
        }
        TypeDefKind::Future(ty) => {
            format!("future<{}>", optional_type_ref(resolve, ty, seen))
        }
        TypeDefKind::Stream(ty) => {
            format!("stream<{}>", optional_type_ref(resolve, ty, seen))
        }
        // A named alias reaching here is world-owned or anonymous; either way its shape is
        // the shape of what it points at.
        TypeDefKind::Type(ty) => type_ref(resolve, ty, seen),
        // Only produced before a `Resolve` is built; reaching it means the input was not
        // fully resolved, so render it as its own distinct shape rather than eliding it.
        TypeDefKind::Unknown => "unknown".to_string(),
    }
}

fn optional_type_ref(resolve: &Resolve, ty: &Option<Type>, seen: &mut Vec<TypeId>) -> String {
    match ty {
        Some(ty) => type_ref(resolve, ty, seen),
        None => "_".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Parse `wit` as a single-file package and return the signature of the named world.
    fn signature_of(wit: &str, world: &str) -> WorldSignature {
        let mut resolve = Resolve::new();
        let pkg = resolve.push_str("test.wit", wit).expect("parse wit");
        let id = resolve
            .select_world(&[pkg], Some(world))
            .expect("select world");
        world_signature(&resolve, id)
    }

    /// The member map of an imported interface, by world-item key.
    fn members(sig: &WorldSignature, key: &str) -> BTreeMap<String, String> {
        match sig.imports.get(key).expect("import present") {
            ItemSignature::Interface(members) => members.clone(),
            other => panic!("expected an interface at `{key}`, got {other:?}"),
        }
    }

    const BASE: &str = r#"
package test:pkg@0.1.0;

interface types {
    /// A doc comment that must not reach the signature.
    type name = string;
    record point { x: u32, y: u32 }
    variant outcome { ok-ish(name), bad(string) }
    enum color { red, green }
    flags perms { read, write }
}

interface handles {
    use types.{point};
    resource conn {
        constructor(seed: u32);
        send: func(p: point) -> result<u32, string>;
        close: func();
    }
    open: func() -> result<conn, string>;
}

world thing {
    use types.{point, outcome};
    import handles;
    export run: func(p: point) -> result<_, outcome>;
}
"#;

    #[test]
    fn signature_covers_imports_and_exports() {
        let sig = signature_of(BASE, "thing");
        assert!(
            sig.imports.contains_key("test:pkg/handles@0.1.0"),
            "imported interface keyed by its package-qualified id: {:?}",
            sig.imports.keys().collect::<Vec<_>>()
        );
        assert!(
            sig.imports.contains_key("point") && sig.imports.contains_key("outcome"),
            "world-level `use` types appear as imports: {:?}",
            sig.imports.keys().collect::<Vec<_>>()
        );
        assert_eq!(
            sig.exports.keys().collect::<Vec<_>>(),
            vec!["run"],
            "one export"
        );
    }

    /// World-level `use` aliases are expanded through to the interface-qualified name of
    /// what they alias, so a world named differently on each side (the artifact's world is
    /// synthesized as `root`) does not perturb the signature.
    #[test]
    fn world_use_alias_expands_to_the_nominal_interface_type() {
        let sig = signature_of(BASE, "thing");
        assert_eq!(
            sig.imports.get("point"),
            Some(&ItemSignature::Item(
                "type test:pkg/types@0.1.0/point".to_string()
            )),
            "{:?}",
            sig.imports
        );
    }

    /// Renaming the world must not change any signature: only items and shapes count.
    #[test]
    fn world_name_is_not_part_of_the_signature() {
        let renamed = BASE.replace("world thing {", "world root {");
        assert_eq!(signature_of(BASE, "thing"), signature_of(&renamed, "root"));
    }

    /// Doc comments are never read, so stripping them (what the encoded type section does)
    /// is invisible.
    #[test]
    fn docs_do_not_affect_the_signature() {
        let stripped = BASE.replace("/// A doc comment that must not reach the signature.\n", "");
        assert_eq!(
            signature_of(BASE, "thing"),
            signature_of(&stripped, "thing")
        );
    }

    /// Reordering items within an interface is invisible: the interface signature is sorted.
    #[test]
    fn item_order_does_not_affect_the_signature() {
        let reordered = BASE.replace(
            "    record point { x: u32, y: u32 }\n    variant outcome { ok-ish(name), bad(string) }\n",
            "    variant outcome { ok-ish(name), bad(string) }\n    record point { x: u32, y: u32 }\n",
        );
        assert_ne!(reordered, BASE, "the fixture edit applied");
        assert_eq!(
            signature_of(BASE, "thing"),
            signature_of(&reordered, "thing")
        );
    }

    /// Record field order IS part of the shape: fields are positional in the canonical ABI.
    #[test]
    fn record_field_order_changes_the_signature() {
        let swapped = BASE.replace(
            "record point { x: u32, y: u32 }",
            "record point { y: u32, x: u32 }",
        );
        assert_ne!(signature_of(BASE, "thing"), signature_of(&swapped, "thing"));
    }

    /// A renamed function inside an imported interface changes that interface's signature —
    /// the drift class the gate exists to catch.
    #[test]
    fn renamed_interface_function_changes_the_signature() {
        let renamed = BASE.replace("open: func()", "open-conn: func()");
        let before = signature_of(BASE, "thing");
        let after = signature_of(&renamed, "thing");
        assert_ne!(
            before.imports.get("test:pkg/handles@0.1.0"),
            after.imports.get("test:pkg/handles@0.1.0")
        );
    }

    /// A changed parameter type changes the signature even though the name is untouched.
    #[test]
    fn changed_param_type_changes_the_signature() {
        let widened = BASE.replace("constructor(seed: u32)", "constructor(seed: u64)");
        assert_ne!(signature_of(BASE, "thing"), signature_of(&widened, "thing"));
    }

    /// Resource methods take handles to their own resource; rendering them nominally is
    /// what keeps that from recursing. The signature must still name the resource.
    #[test]
    fn resource_methods_render_nominally() {
        let handles = members(&signature_of(BASE, "thing"), "test:pkg/handles@0.1.0");
        assert_eq!(
            handles.get("type conn").map(String::as_str),
            Some("resource"),
            "{handles:?}"
        );
        let send = handles
            .get("func [method]conn.send")
            .expect("method present: {handles:?}");
        assert!(
            send.contains("test:pkg/handles@0.1.0/conn"),
            "the method's self parameter names the resource nominally: {send}"
        );
    }

    /// Interface members are individually addressable — the granularity componentization
    /// elides at, and therefore the granularity the gate must compare at.
    #[test]
    fn interface_members_are_keyed_individually() {
        let handles = members(&signature_of(BASE, "thing"), "test:pkg/handles@0.1.0");
        let mut keys = handles.keys().map(String::as_str).collect::<Vec<_>>();
        keys.sort_unstable();
        assert_eq!(
            keys,
            vec![
                "func [constructor]conn",
                "func [method]conn.close",
                "func [method]conn.send",
                "func open",
                "type conn",
                "type point",
            ],
            "{handles:?}"
        );
    }

    /// An added variant case changes the signature: the guest-visible shape moved.
    #[test]
    fn added_variant_case_changes_the_signature() {
        let extended = BASE.replace(
            "variant outcome { ok-ish(name), bad(string) }",
            "variant outcome { ok-ish(name), bad(string), worse }",
        );
        assert_ne!(
            signature_of(BASE, "thing"),
            signature_of(&extended, "thing")
        );
    }

    /// A type alias is compared by what it resolves to, not by the alias name: `type name =
    /// string` and a bare `string` are the same shape.
    #[test]
    fn aliases_compare_by_target_shape() {
        let inlined = BASE.replace("ok-ish(name)", "ok-ish(string)");
        let before = signature_of(BASE, "thing");
        let after = signature_of(&inlined, "thing");
        let key = "test:pkg/types@0.1.0";
        // `name` is interface-owned, so it is referenced nominally; inlining `string` in
        // its place is a visible change at the reference site.
        assert_ne!(before.imports.get(key), after.imports.get(key));
    }

    /// Nested anonymous structure is rendered in place and compared.
    #[test]
    fn nested_anonymous_types_are_structural() {
        let wit = r#"
package test:nest@0.1.0;
world w {
    export f: func(x: list<tuple<u32, option<string>>>) -> result<list<u8>, string>;
}
"#;
        let sig = signature_of(wit, "w");
        assert_eq!(
            sig.exports.get("f"),
            Some(&ItemSignature::Item(
                "func freestanding(x: list<tuple<u32, option<string>>>) \
                 -> result<list<u8>, string>"
                    .to_string()
            )),
            "{:?}",
            sig.exports
        );
    }

    /// Two worlds in different packages that declare the same shapes still differ: the
    /// nominal path of an interface type carries its package. A package rename is a
    /// guest-visible change (the import name a host must satisfy moves with it).
    #[test]
    fn package_identity_is_part_of_the_signature() {
        let renamed = BASE.replace("package test:pkg@0.1.0;", "package test:other@0.1.0;");
        assert_ne!(signature_of(BASE, "thing"), signature_of(&renamed, "thing"));
    }
}
