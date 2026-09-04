//! Guest scaffolding generated from a component specification.
//!
//! One `.brenn` specification names one component class, and that class states
//! the whole shape of the component's port surface: which ports it has, which
//! way each faces, and which capabilities it needs. Every one of those facts is
//! spelled as a free string on both sides of the ABI today, so a typo is caught
//! at first publish rather than at compile. This module turns the class into a
//! Rust module the guest crate compiles: an enum over the inbound ports, a
//! payload marker trait and a typed publish handle per outbound port, the raw
//! names for the string-taking parts of the SDK, and a re-export per capability
//! the spec declares.
//!
//! The specification is the source of truth, and the bytes generated from are
//! the bytes the package embeds and the host hash-binds at boot, so a guest
//! that compiles is a guest whose ports agree with what a deployment can wire.
//!
//! Layout is not this module's problem: what it writes is run through rustfmt
//! by the build rule that generates the file, so the emitter states the code
//! and the toolchain states how it looks. Predicting the layout here would be a
//! second copy of rustfmt's heuristics, correct only for the shapes a fixture
//! happens to cover.
//!
//! Validation here is deliberately minimal. The compiler — class resolution in
//! [`crate::resolve`] — remains the sole authority on what a specification may
//! say; this module refuses only what it cannot emit, so it never becomes a
//! second opinion on legality.

use std::fmt::Write as _;

use brenn_envelope::grants::ComponentGrant;

use crate::diag::Diagnostic;
use crate::model::{ComponentClass, DocComment, File, PortDecl, PortDir, StrLike};
use crate::resolve::decode_str;
use crate::resolved::Abi;

/// The one component class a scaffold is generated from.
///
/// A module holds exactly one class, or `class` names which of them to take.
/// Non-class items are ignored: a specification shipping an assembly beside its
/// class is ordinary, and an assembly is deployment vocabulary with no guest
/// surface.
pub fn select_class<'a>(
    file: &'a File,
    class: Option<&str>,
    filename: &str,
) -> Result<&'a ComponentClass, Diagnostic> {
    let classes: Vec<&ComponentClass> = file.components().collect();
    match class {
        Some(wanted) => classes
            .iter()
            .copied()
            .find(|candidate| candidate.name.value() == wanted)
            .ok_or_else(|| {
                Diagnostic::unpositioned(
                    format!(
                        "no component class named `{wanted}` in this module; it declares {}",
                        name_list(&classes)
                    ),
                    filename,
                )
            }),
        None => match classes.as_slice() {
            [only] => Ok(only),
            [] => Err(Diagnostic::unpositioned(
                "this module declares no component class, so there is no port surface to \
                 generate from"
                    .to_string(),
                filename,
            )),
            [first, second, ..] => {
                let mut error = Diagnostic::at(
                    format!(
                        "this module declares {} component classes; name the one to generate \
                         from with `--class <Name>`",
                        classes.len()
                    ),
                    first.name.span().clone(),
                );
                error.related.push((
                    "another class is declared here".to_string(),
                    second.name.span().clone(),
                ));
                Err(error)
            }
        },
    }
}

/// The classes a module declares, for a refusal that has to list them.
fn name_list(classes: &[&ComponentClass]) -> String {
    if classes.is_empty() {
        return "none".to_string();
    }
    classes
        .iter()
        .map(|class| format!("`{}`", class.name.value()))
        .collect::<Vec<_>>()
        .join(", ")
}

/// The Rust source of the guest module for one specification.
///
/// `spec_basename` is what the generated header names as its input; it is
/// carried rather than derived so a caller generating from text in memory says
/// what the text is. `filename` is what an unpositioned diagnostic names.
pub fn generate(
    file: &File,
    class: Option<&str>,
    spec_basename: &str,
    filename: &str,
) -> Result<String, Diagnostic> {
    let class = select_class(file, class, filename)?;
    let abi = abi_of(class)?;
    let ports = PortNames::of(class, abi)?;
    let grants = grant_modules(class)?;

    let mut out = String::new();
    write_header(&mut out, spec_basename, class.doc.as_ref());
    let publishes = abi == Abi::Processor && ports.outbound().next().is_some();
    if publishes {
        // Each port's payload marker trait has `Serialize` as its supertrait,
        // and the SDK re-exports that trait so a guest crate needs no serde
        // dependency of its own to compile the module.
        let _ = write!(out, "\n{GUEST_ONLY}use brenn_guest::serde;\n");
    }
    write_in_port(&mut out, &ports, abi);
    if abi == Abi::Processor {
        write_publish_handles(&mut out, &ports);
    }
    write_port_names(&mut out, &ports);
    if abi == Abi::Processor {
        write_grant_reexports(&mut out, &grants);
    }
    Ok(out)
}

/// The abi word the class states, refused where it names no abi.
///
/// The generator branches on the abi rather than taking it as a flag: which
/// SDK a guest is written against is a property of the component, not of the
/// invocation that generates its module.
fn abi_of(class: &ComponentClass) -> Result<Abi, Diagnostic> {
    let word = &class.attrs.abi.value.name;
    Abi::parse(word.value()).ok_or_else(|| {
        Diagnostic::at(
            format!(
                "`{}` is not an abi; expected one of {}",
                word.value(),
                Abi::ALL.map(|abi| abi.as_str()).join(", ")
            ),
            word.span().clone(),
        )
    })
}

// ── identifier mapping ───────────────────────────────────────────────────────
//
// A port name is kebab-case in the specification and reaches Rust in three
// spellings: a `CamelCase` enum variant, a `snake_case` function, and a
// `SCREAMING_SNAKE_CASE` constant. Every mapping is total or refused — there is
// no raw-identifier escape hatch, because a port whose name needs one is a port
// whose name should be boring.

/// One port, with the three identifiers it maps to.
struct PortName {
    /// As the specification spells it, which is what goes on the wire.
    raw: String,
    variant: String,
    handle: String,
    constant: String,
    /// The marker trait an outbound port's payload types implement. It is what
    /// binds a Rust type to this port: the publish handle takes only
    /// implementors, so the type cannot be chosen afresh at a call site.
    payload_trait: String,
    dir: PortDir,
    /// The doctype the specification annotates it with, decoded, where it
    /// annotates one with a plain string. An interpolated doctype names
    /// constants this pass has no scope to resolve, so it carries no note.
    doctype: Option<String>,
}

impl PortName {
    fn inbound(&self) -> bool {
        matches!(self.dir, PortDir::Into | PortDir::Both)
    }

    fn outbound(&self) -> bool {
        matches!(self.dir, PortDir::Outof | PortDir::Both)
    }
}

/// Every port of one class, mapped and checked for collisions.
struct PortNames {
    ports: Vec<PortName>,
}

impl PortNames {
    fn of(class: &ComponentClass, abi: Abi) -> Result<PortNames, Diagnostic> {
        let mut ports: Vec<PortName> = Vec::new();
        for decl in &class.ports {
            ports.push(map_port(decl, abi)?);
        }
        let names = PortNames { ports };
        names.check_collisions(class, abi)?;
        Ok(names)
    }

    fn inbound(&self) -> impl Iterator<Item = &PortName> {
        self.ports.iter().filter(|port| port.inbound())
    }

    fn outbound(&self) -> impl Iterator<Item = &PortName> {
        self.ports.iter().filter(|port| port.outbound())
    }

    /// Two ports whose names differ only in punctuation map to one identifier.
    /// Each namespace is checked on its own: the type, handle and constant
    /// spellings cannot collide with each other, only within themselves.
    ///
    /// Only the spellings the abi actually emits are checked, the same rule the
    /// keyword refusal follows: a dom module writes no publish handle and no
    /// payload trait, so a dom class is never refused for an identifier its
    /// module does not contain.
    fn check_collisions(&self, class: &ComponentClass, abi: Abi) -> Result<(), Diagnostic> {
        for namespace in Namespace::ALL {
            let mut seen: Vec<(&str, &str, Spelling)> = Vec::new();
            for port in &self.ports {
                for (identifier, kind) in namespace.spellings(port, abi) {
                    if let Some((prior, _, prior_kind)) = seen
                        .iter()
                        .find(|(_, other, _)| *other == identifier)
                        .copied()
                    {
                        // Where the two ports reach the same name by different
                        // spellings — one port's variant, another's payload
                        // trait — neither word names the clash, so the
                        // namespace does.
                        let what = match namespace.mixed_label() {
                            Some(label) if prior_kind != kind => label,
                            _ => kind.label(),
                        };
                        return Err(collision(class, &port.raw, prior, identifier, what));
                    }
                    seen.push((port.raw.as_str(), identifier, kind));
                }
            }
        }
        Ok(())
    }
}

/// One kind of identifier a port mints, and the word a refusal calls it by.
///
/// The kind is a value rather than its own label so that "did these two ports
/// reach one identifier by the same kind of spelling" is a comparison of kinds,
/// not of prose, and so each word is written once.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Spelling {
    Variant,
    PayloadTrait,
    Handle,
    Constant,
}

impl Spelling {
    fn label(self) -> &'static str {
        match self {
            Spelling::Variant => "enum variant",
            Spelling::PayloadTrait => "payload marker trait",
            Spelling::Handle => "publish handle",
            Spelling::Constant => "port-name constant",
        }
    }
}

/// One of the three Rust namespaces a port name reaches.
///
/// A collision is a collision within one of them; the three cannot collide with
/// each other, because a type, a function and a constant are three different
/// kinds of name. The type namespace holds two spellings — an inbound port's
/// enum variant and an outbound port's payload marker trait — kept together so
/// a reader is never asked to tell one `FooPayload` from another.
#[derive(Clone, Copy)]
enum Namespace {
    TypeName,
    Handle,
    Constant,
}

impl Namespace {
    // The handle is checked first: two outbound ports that differ only in
    // punctuation collide as handles and as payload traits both, and the handle
    // is the plainer of the two names to be told about.
    const ALL: [Namespace; 3] = [Namespace::Handle, Namespace::TypeName, Namespace::Constant];

    /// What a refusal calls this namespace where the two ports reach it by
    /// different kinds of spelling, for the one namespace that holds more than
    /// one kind. The others can only ever collide with themselves, so their
    /// refusal names the kind directly.
    fn mixed_label(self) -> Option<&'static str> {
        match self {
            Namespace::TypeName => Some("type name"),
            Namespace::Handle | Namespace::Constant => None,
        }
    }

    /// The identifiers a port takes here under this abi, each with its kind.
    /// Only an inbound port is a variant; only an outbound port of a processor
    /// class is a publish handle and a payload trait, because those are the
    /// only ones the emitter writes.
    fn spellings(self, port: &PortName, abi: Abi) -> Vec<(&str, Spelling)> {
        let publishes = port.outbound() && abi == Abi::Processor;
        match self {
            Namespace::TypeName => {
                let mut out = Vec::new();
                if port.inbound() {
                    out.push((port.variant.as_str(), Spelling::Variant));
                }
                if publishes {
                    out.push((port.payload_trait.as_str(), Spelling::PayloadTrait));
                }
                out
            }
            Namespace::Handle => publishes
                .then_some((port.handle.as_str(), Spelling::Handle))
                .into_iter()
                .collect(),
            Namespace::Constant => vec![(port.constant.as_str(), Spelling::Constant)],
        }
    }
}

/// Two ports of one class mapping to one Rust identifier.
fn collision(
    class: &ComponentClass,
    port: &str,
    prior: &str,
    identifier: &str,
    what: &str,
) -> Diagnostic {
    let span = class
        .ports
        .iter()
        .find(|decl| decl.name.value() == port)
        .map(|decl| decl.name.span().clone())
        .unwrap_or_else(|| class.name.span().clone());
    let mut error = Diagnostic::at(
        format!(
            "ports `{port}` and `{prior}` both map to the {what} `{identifier}`; port names \
             that differ only in punctuation are one name in Rust"
        ),
        span,
    );
    if let Some(decl) = class.ports.iter().find(|decl| decl.name.value() == prior) {
        error
            .related
            .push(("it also maps here".to_string(), decl.name.span().clone()));
    }
    error
}

/// One port declaration, mapped to its three spellings.
fn map_port(decl: &PortDecl, abi: Abi) -> Result<PortName, Diagnostic> {
    let raw = decl.name.value().clone();
    let Some(words) = split_words(&raw) else {
        return Err(Diagnostic::at(
            format!(
                "port `{raw}` does not map to a Rust identifier: a port name is \
                 punctuation-separated groups of letters and digits, starting with a letter"
            ),
            decl.name.span().clone(),
        ));
    };
    let variant = words
        .iter()
        .map(|word| capitalize(word))
        .collect::<String>();
    let handle = words.join("_");
    let constant = handle.to_uppercase();
    let payload_trait = format!("{variant}Payload");
    // Only the spellings this port actually emits are checked: an inbound port
    // named `in` emits the variant `In` and the constant `IN`, and no function,
    // so the keyword its publish handle would have been is never written. A dom
    // class writes no handle at all.
    let dir = decl.dir.value();
    let emitted: [(&str, bool); 2] = [
        (
            variant.as_str(),
            matches!(dir, PortDir::Into | PortDir::Both),
        ),
        (
            handle.as_str(),
            matches!(dir, PortDir::Outof | PortDir::Both) && abi == Abi::Processor,
        ),
    ];
    for (identifier, emitted) in emitted {
        if emitted && is_keyword(identifier) {
            return Err(Diagnostic::at(
                format!(
                    "port `{raw}` maps to `{identifier}`, which is a Rust keyword; rename the \
                     port — there is no raw-identifier escape here"
                ),
                decl.name.span().clone(),
            ));
        }
    }
    Ok(PortName {
        raw,
        variant,
        handle,
        constant,
        payload_trait,
        dir: dir.clone(),
        doctype: doctype_note(decl)?,
    })
}

/// A port's doctype as a single line of prose, where it has one that is
/// stateable as such.
fn doctype_note(decl: &PortDecl) -> Result<Option<String>, Diagnostic> {
    let Some(doctype) = decl.doctype.as_ref() else {
        return Ok(None);
    };
    let text = match doctype.value() {
        StrLike::Str(literal) => decode_str(literal)?,
        // Interpolation names constants, and resolving those is the compiler's
        // pass, not this one. A doctype is a nominal tag with no runtime
        // consumer, so the note is dropped rather than the module refused.
        StrLike::Fstr(_) => return Ok(None),
    };
    // Any control character, not just a newline: the note is written into a
    // `///` line comment, and a bare CR ends that comment as surely as a LF
    // does — as a lexer error inside a generated file, with no span to point
    // at the specification. The DSL's escapes decode `\r` and `\0` as well as
    // `\n`, so all three reach here.
    if text.contains(char::is_control) {
        return Ok(None);
    }
    Ok(Some(text))
}

/// The punctuation-separated groups of a port name, or nothing where the name
/// does not decompose into identifier material.
fn split_words(name: &str) -> Option<Vec<String>> {
    if !name.starts_with(|character: char| character.is_ascii_alphabetic()) {
        return None;
    }
    let mut words: Vec<String> = Vec::new();
    for word in name.split(['-', '_']) {
        if word.is_empty()
            || !word
                .chars()
                .all(|character| character.is_ascii_alphanumeric())
        {
            return None;
        }
        words.push(word.to_ascii_lowercase());
    }
    Some(words)
}

fn capitalize(word: &str) -> String {
    let mut characters = word.chars();
    match characters.next() {
        Some(first) => first.to_ascii_uppercase().to_string() + characters.as_str(),
        None => String::new(),
    }
}

/// Every word Rust reserves, strict and reserved alike, across editions.
const KEYWORDS: [&str; 51] = [
    "abstract", "as", "async", "await", "become", "box", "break", "const", "continue", "crate",
    "do", "dyn", "else", "enum", "extern", "false", "final", "fn", "for", "gen", "if", "impl",
    "in", "let", "loop", "macro", "match", "mod", "move", "mut", "override", "priv", "pub", "ref",
    "return", "self", "static", "struct", "super", "trait", "true", "try", "type", "typeof",
    "unsafe", "unsized", "use", "virtual", "where", "while", "yield",
];

fn is_keyword(identifier: &str) -> bool {
    identifier == "Self" || KEYWORDS.contains(&identifier)
}

// ── grants ───────────────────────────────────────────────────────────────────

/// The SDK module a grant word names, where it names one.
///
/// Exhaustive over the vocabulary rather than a lookup table with a default, so
/// a capability added to [`ComponentGrant`] is answered here or the crate does
/// not compile. `Ports` is embodied by the publish handles themselves and
/// `Takeover` is consent to a binding with no interface behind it, so neither
/// names a module.
///
/// One module per capability, never shared: the re-export is what holds a
/// class's declared words and the code it can write equal at compile time, so a
/// module two words could reach would hand the narrower word the wider
/// authority.
fn sdk_module(grant: ComponentGrant) -> Option<&'static str> {
    match grant {
        ComponentGrant::Store => Some("store"),
        ComponentGrant::Log => Some("log"),
        ComponentGrant::Alert => Some("alert"),
        ComponentGrant::Config => Some("config"),
        ComponentGrant::Mqtt => Some("mqtt"),
        ComponentGrant::Tools => Some("tools"),
        ComponentGrant::Dom => Some("dom"),
        ComponentGrant::PageDom => Some("page_dom"),
        ComponentGrant::Ports | ComponentGrant::Takeover => None,
    }
}

/// The SDK modules a class's declared grants name, in vocabulary order.
///
/// Both lists are read: a capability the component can run without is still a
/// capability it reaches for, and reaching it through this module is what makes
/// deleting the word from the specification break the guest compile.
fn grant_modules(class: &ComponentClass) -> Result<Vec<&'static str>, Diagnostic> {
    let mut declared: Vec<ComponentGrant> = Vec::new();
    for attr in [class.attrs.requires.as_ref(), class.attrs.optional.as_ref()]
        .into_iter()
        .flatten()
    {
        for word in &attr.value.words {
            let Some(grant) = ComponentGrant::parse(word.name.value()) else {
                return Err(Diagnostic::at(
                    format!(
                        "`{}` is not a capability a component holds; the words are {}",
                        word.name.value(),
                        ComponentGrant::ALL
                            .map(|grant| format!("`{}`", grant.word()))
                            .join(", ")
                    ),
                    word.name.span().clone(),
                ));
            };
            declared.push(grant);
        }
    }
    // Alphabetical rather than vocabulary order, which is the order a reader of
    // the emitted file finds them in.
    let mut modules: Vec<&'static str> = ComponentGrant::ALL
        .into_iter()
        .filter(|grant| declared.contains(grant))
        .filter_map(sdk_module)
        .collect();
    modules.sort_unstable();
    Ok(modules)
}

// ── emission ─────────────────────────────────────────────────────────────────

/// The attribute every item naming `brenn_guest` carries.
///
/// The SDK is a wasm32 crate, and a component whose DOM-free half is host-tested
/// compiles this same module for the host to reach its port names. Gating the
/// items that name the SDK — the payload traits, the publish handles, the window
/// classifier, the capability re-exports — is what lets one generated module
/// serve both builds; the port names and the inbound enum are plain Rust and
/// carry no gate.
const GUEST_ONLY: &str = "#[cfg(target_arch = \"wasm32\")]\n";

fn write_header(out: &mut String, spec_basename: &str, doc: Option<&DocComment>) {
    let _ = writeln!(out, "// Generated from {spec_basename} — do not edit.");
    out.push('\n');
    if let Some(doc) = doc {
        for line in &doc.lines {
            let text = line.content.value().trim_end();
            if text.is_empty() {
                out.push_str("//!\n");
            } else {
                let _ = writeln!(out, "//!{text}");
            }
        }
        out.push_str("//!\n");
    }
    out.push_str(
        "//! The whole port surface the specification states; a guest uses the part of it\n\
         //! that it needs.\n\
         \n\
         #![allow(dead_code, unused_imports)]\n",
    );
}

fn write_in_port(out: &mut String, ports: &PortNames, abi: Abi) {
    let inbound: Vec<&PortName> = ports.inbound().collect();
    out.push_str(
        "\n/// The ports the specification declares as inbound — `in` and `io`.\n\
         #[derive(Clone, Copy, Debug, PartialEq, Eq)]\n",
    );
    if inbound.is_empty() {
        // Uninhabited, and legal: such a component is activated only for its
        // own deferred views, which arrive on the activation rather than as a
        // port window.
        out.push_str("pub enum InPort {}\n");
    } else {
        out.push_str("pub enum InPort {\n");
        for port in &inbound {
            write_doctype_doc(out, port, "    ");
            let _ = writeln!(out, "    {},", port.variant);
        }
        out.push_str("}\n");
    }
    out.push_str("\nimpl InPort {\n");

    out.push_str("    /// Every inbound port, in the order the specification declares them.\n");
    let variants: Vec<String> = inbound
        .iter()
        .map(|port| format!("InPort::{}", port.variant))
        .collect();
    let _ = writeln!(
        out,
        "    pub const ALL: [InPort; {}] = [{}];",
        inbound.len(),
        variants.join(", ")
    );
    out.push('\n');

    out.push_str("    /// The name this port is published and bound under.\n");
    out.push_str("    pub const fn name(self) -> &'static str {\n");
    if inbound.is_empty() {
        out.push_str("        match self {}\n");
    } else {
        out.push_str("        match self {\n");
        for port in &inbound {
            let _ = writeln!(
                out,
                "            InPort::{} => \"{}\",",
                port.variant, port.raw
            );
        }
        out.push_str("        }\n");
    }
    out.push_str("    }\n\n");

    out.push_str("    /// The port a name spells, or nothing where it spells none.\n");
    if inbound.is_empty() {
        // A one-armed match over a name no variant can answer is what clippy
        // refuses, so a component with no inbound port says so directly.
        out.push_str(
            "    pub fn from_name(_name: &str) -> Option<InPort> {\n        None\n    }\n",
        );
    } else {
        out.push_str(
            "    pub fn from_name(name: &str) -> Option<InPort> {\n        match name {\n",
        );
        for port in &inbound {
            let _ = writeln!(
                out,
                "            \"{}\" => Some(InPort::{}),",
                port.raw, port.variant
            );
        }
        out.push_str("            _ => None,\n        }\n    }\n");
    }

    if abi == Abi::Processor {
        out.push_str(
            "\n    /// Classify an activation window.\n    ///\n\
             \x20   /// A port the specification does not declare is not bad input: the\n\
             \x20   /// artifact is hash-bound to the specification that generated this\n\
             \x20   /// module, so an undeclared port means the host handed over a window it\n\
             \x20   /// could not have been configured to produce. The activation fails.\n\
             \x20   #[cfg(target_arch = \"wasm32\")]\n\
             \x20   pub fn of(window: &brenn_guest::PortWindow) -> Result<InPort, brenn_guest::Error> {\n\
             \x20       InPort::from_name(window.port()).ok_or_else(|| {\n\
             \x20           brenn_guest::Error::failed(format!(\n\
             \x20               \"activation on port `{}`, which this component does not declare\",\n\
             \x20               window.port()\n\
             \x20           ))\n\
             \x20       })\n\
             \x20   }\n",
        );
    }
    out.push_str("}\n");
}

/// The payload marker trait and the publish handle for each outbound port.
///
/// The pair is what makes the port's payload type checkable rather than merely
/// named: the trait is the port's own, the guest binds a type to it once as an
/// impl, and the handle takes only implementors — so a publish of some other
/// type does not compile, wherever it is published through the handle. The
/// string-taking SDK surface stays unchecked; there the generated module gives
/// the name alone.
fn write_publish_handles(out: &mut String, ports: &PortNames) {
    for port in ports.outbound() {
        let _ = write!(
            out,
            "\n/// The payload types this guest publishes on the `{}` port. Bind a type to\n\
             /// the port once, as an impl:\n\
             /// `impl spec::{} for Body<'_> {{}}`\n\
             {}pub trait {}: serde::Serialize {{}}\n",
            port.raw, port.payload_trait, GUEST_ONLY, port.payload_trait
        );
        let _ = write!(
            out,
            "\n/// A typed publish handle for the `{}` port, over any payload bound to it\n\
             /// by `{}`. An owned payload binds through a `const`:\n\
             /// `const OUT: OutPort<Body> = spec::{}();`\n\
             /// A borrowed payload cannot be named in one, so publish it inline:\n\
             /// `spec::{}().publish(&body)?`.\n",
            port.raw, port.payload_trait, port.handle, port.handle
        );
        if let Some(doctype) = &port.doctype {
            let _ = write!(out, "///\n/// Doctype: `{doctype}`.\n");
        }
        let _ = write!(
            out,
            "{}pub const fn {}<T: {}>() -> brenn_guest::OutPort<T> {{\n\
             \x20   brenn_guest::OutPort::new(\"{}\")\n\
             }}\n",
            GUEST_ONLY, port.handle, port.payload_trait, port.raw
        );
    }
}

fn write_port_names(out: &mut String, ports: &PortNames) {
    out.push_str(
        "\n/// The port names as text, for the parts of the SDK that take one.\n\
         pub mod port {\n",
    );
    for port in &ports.ports {
        write_doctype_doc(out, port, "    ");
        let _ = writeln!(
            out,
            "    pub const {}: &str = \"{}\";",
            port.constant, port.raw
        );
    }
    out.push_str("}\n");
}

fn write_grant_reexports(out: &mut String, modules: &[&'static str]) {
    if modules.is_empty() {
        return;
    }
    out.push_str(
        "\n// One re-export per capability the specification declares. Reaching a\n\
         // capability through this module is what makes deleting its word from the\n\
         // specification break the guest compile.\n",
    );
    for module in modules {
        let _ = writeln!(out, "{GUEST_ONLY}pub use brenn_guest::{module};");
    }
}

fn write_doctype_doc(out: &mut String, port: &PortName, indent: &str) {
    if let Some(doctype) = &port.doctype {
        let _ = writeln!(out, "{indent}/// Doctype: `{doctype}`.");
    }
}
